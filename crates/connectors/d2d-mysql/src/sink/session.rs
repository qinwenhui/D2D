//! MySQL 写入会话：把一份 Arrow 数据切成若干条多行 INSERT

use async_trait::async_trait;
use d2d_connector::{MemMeter, SinkSession};
use d2d_core::{D2DError, D2DResult};
use mysql_async::prelude::*;
use mysql_async::{Pool, Value};
use std::sync::Arc;
use tracing::{debug, info};

use crate::sql::quote_ident;

/// MySQL 预处理语句的占位符硬上限
const MAX_PLACEHOLDERS: usize = 65_535;
/// 单条语句的字节软上限（客户端 max_allowed_packet 是 64 MB，留一半余量）
const MAX_STMT_BYTES: usize = 32 * 1024 * 1024;

/// 一条写入会话 = 一次真实写入（内核攒够一份才开一条）
pub struct MysqlSinkSession {
    pub(crate) pool: Pool,
    pub(crate) qualified_table: String,
    pub(crate) meter: Option<Arc<MemMeter>>,
    pub(crate) total_rows: u64,
}

#[async_trait]
impl SinkSession for MysqlSinkSession {
    async fn write_batch(&mut self, batch: arrow::record_batch::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let ncols = batch.num_columns();
        if ncols == 0 {
            return Err(D2DError::schema("写入批次没有任何列"));
        }

        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| quote_ident(f.name()))
            .collect();
        let col_list = columns.join(",");

        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| D2DError::external("mysql", format!("取连接失败: {}", e)))?;

        let rows_per_stmt = (MAX_PLACEHOLDERS / ncols).max(1);
        let mut start = 0usize;
        let mut stmts = 0usize;
        while start < rows {
            // 攒一段：行数、字节、占位符三个上限谁先到谁截断
            let mut params: Vec<Value> = Vec::new();
            let mut n = 0usize;
            let mut bytes = 0usize;
            while start + n < rows && n < rows_per_stmt {
                let row_vals: Vec<Value> = (0..ncols)
                    .map(|c| {
                        value_of(
                            batch.column(c).as_ref(),
                            start + n,
                            batch.schema().field(c).name(),
                            batch.schema().field(c).data_type(),
                        )
                    })
                    .collect::<D2DResult<Vec<_>>>()?;
                let row_bytes: usize = row_vals
                    .iter()
                    .map(|v| match v {
                        Value::Bytes(b) => b.len() + 2,
                        _ => 8,
                    })
                    .sum();
                // 至少装一行：单行超上限时让它自己撞 max_allowed_packet，而不是死循环
                if n > 0 && bytes + row_bytes > MAX_STMT_BYTES {
                    break;
                }
                bytes += row_bytes;
                params.extend(row_vals);
                n += 1;
            }

            let one_row = format!(
                "({})",
                vec!["?"; ncols].join(",")
            );
            let values = vec![one_row.as_str(); n].join(",");
            let sql = format!("INSERT INTO {} ({}) VALUES {}", self.qualified_table, col_list, values);

            // 如实申报这段绑定值（求值期间它们都在本进程里）
            let _lease = self.meter.as_ref().map(|m| m.claim(bytes));
            conn.exec_drop(&sql, params)
                .await
                .map_err(|e| D2DError::sink_write("mysql", e))?;
            stmts += 1;
            start += n;
        }

        self.total_rows += rows as u64;
        debug!(rows, stmts, "mysql 写入完成");
        Ok(rows)
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // 每次 write_batch 都已提交（autocommit），没有缓冲要落
        info!(total_rows = self.total_rows, "mysql session committed");
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        // 已经执行的 INSERT 是已提交事务，这里无事可做
        Ok(())
    }
}

/// 一个单元格 → 绑定值
///
/// 文本承载（让 MySQL 按列类型转换），两个例外见模块文档：NULL 与二进制。
fn value_of(
    arr: &dyn arrow::array::Array,
    row: usize,
    col: &str,
    dt: &arrow::datatypes::DataType,
) -> D2DResult<Value> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    if arr.is_null(row) {
        return Ok(Value::NULL);
    }
    Ok(match dt {
        DataType::Boolean => {
            let v = arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            Value::Bytes(if v { b"1".to_vec() } else { b"0".to_vec() })
        }
        DataType::Binary => {
            let v = arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            Value::Bytes(v.to_vec())
        }
        DataType::LargeBinary => {
            let v = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            Value::Bytes(v.to_vec())
        }
        DataType::Date32 => {
            let days = arr.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
            let d = chrono::DateTime::from_timestamp(days as i64 * 86400, 0)
                .ok_or_else(|| D2DError::schema(format!("列 `{}` 的日期 {} 超出范围", col, days)))?
                .naive_utc()
                .date();
            Value::Bytes(d.format("%Y-%m-%d").to_string().into_bytes())
        }
        DataType::Time64(u) => {
            let v = match u {
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .value(row),
                other => {
                    return Err(D2DError::schema(format!(
                        "列 `{}` 的类型 Time64({:?}) 没有对应的 Arrow 数组",
                        col, other
                    )))
                }
            };
            Value::Bytes(fmt_time(v, *u, col)?.into_bytes())
        }
        DataType::Timestamp(u, _) => {
            let v = match u {
                TimeUnit::Second => arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Millisecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(row),
            };
            Value::Bytes(fmt_timestamp(v, *u, col)?.into_bytes())
        }
        // 其余一律走 arrow 的标准文本渲染（整数/小数/字符串/JSON 都是对的）
        _ => Value::Bytes(
            arrow::util::display::array_value_to_string(arr, row)
                .map_err(|e| D2DError::schema(format!("列 `{}` 转文本失败: {}", col, e)))?
                .into_bytes(),
        ),
    })
}

fn fmt_time(v: i64, unit: arrow::datatypes::TimeUnit, col: &str) -> D2DResult<String> {
    use arrow::datatypes::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000_000) as u32),
        TimeUnit::Microsecond => (v.div_euclid(1_000_000), (v.rem_euclid(1_000_000) * 1_000) as u32),
        TimeUnit::Nanosecond => (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000) as u32),
    };
    let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos)
        .ok_or_else(|| D2DError::schema(format!("列 `{}` 的时间值 {} 超出范围", col, v)))?;
    Ok(match unit {
        TimeUnit::Second => t.format("%H:%M:%S").to_string(),
        TimeUnit::Millisecond => t.format("%H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => t.format("%H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => t.format("%H:%M:%S%.9f").to_string(),
    })
}

fn fmt_timestamp(v: i64, unit: arrow::datatypes::TimeUnit, col: &str) -> D2DResult<String> {
    use arrow::datatypes::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000_000) as u32),
        TimeUnit::Microsecond => (v.div_euclid(1_000_000), (v.rem_euclid(1_000_000) * 1_000) as u32),
        TimeUnit::Nanosecond => (v.div_euclid(1_000_000_000), v.rem_euclid(1_000_000_000) as u32),
    };
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| D2DError::schema(format!("列 `{}` 的时间戳 {} 超出范围", col, v)))?
        .naive_utc();
    Ok(match unit {
        TimeUnit::Second => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        TimeUnit::Millisecond => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_boolean_binds_as_digits_not_true_false() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        for (i, want) in [(0usize, "1"), (1, "0")] {
            match value_of(&a, i, "b", &DataType::Boolean).unwrap() {
                Value::Bytes(b) => assert_eq!(String::from_utf8_lossy(&b), want),
                other => panic!("{:?}", other),
            }
        }
        assert!(matches!(
            value_of(&a, 2, "b", &DataType::Boolean).unwrap(),
            Value::NULL
        ));
    }

    #[test]
    fn test_null_vs_empty_string() {
        let a = StringArray::from(vec![Some(""), None]);
        assert!(matches!(
            value_of(&a, 0, "s", &DataType::Utf8).unwrap(),
            Value::Bytes(_)
        ));
        assert!(matches!(value_of(&a, 1, "s", &DataType::Utf8).unwrap(), Value::NULL));
    }

    #[test]
    fn test_timestamp_uses_space_not_uppercase_t() {
        let ts = chrono::NaiveDate::from_ymd_opt(2022, 1, 1)
            .unwrap()
            .and_hms_micro_opt(2, 3, 4, 123456)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let a = TimestampMicrosecondArray::from(vec![ts]).with_timezone_opt(None::<&str>);
        let got = match value_of(&a, 0, "t", &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap()
        {
            Value::Bytes(b) => String::from_utf8(b).unwrap(),
            other => panic!("{:?}", other),
        };
        assert_eq!(got, "2022-01-01 02:03:04.123456");
        assert!(!got.contains('T'));
    }

    #[test]
    fn test_date_and_integers() {
        let days = (chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d = Date32Array::from(vec![days]);
        let got = match value_of(&d, 0, "d", &DataType::Date32).unwrap() {
            Value::Bytes(b) => String::from_utf8(b).unwrap(),
            _ => panic!(),
        };
        assert_eq!(got, "2022-01-01");

        let n = Int64Array::from(vec![i64::MIN]);
        let got = match value_of(&n, 0, "n", &DataType::Int64).unwrap() {
            Value::Bytes(b) => String::from_utf8(b).unwrap(),
            _ => panic!(),
        };
        assert_eq!(got, i64::MIN.to_string());
    }
}
