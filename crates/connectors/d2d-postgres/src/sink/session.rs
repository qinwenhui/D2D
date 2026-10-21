//! PostgreSQL 写入会话：把一份 Arrow 数据切成若干条多行 INSERT

use async_trait::async_trait;
use bytes::BytesMut;
use d2d_connector::{MemMeter, SinkSession};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use tokio_postgres::types::{to_sql_checked, IsNull, ToSql, Type};
use tokio_postgres::NoTls;
use tracing::{debug, info};

use crate::config::PgConnection;

/// 写入时一个参数怎么绑
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cast {
    /// 绑文本并显式转成这个类型：`$n::text::<ty>`
    Text(String),
    /// 直接绑字节（`bytea`）
    Bytea,
}

#[derive(Debug, Clone)]
pub struct TargetColumn {
    pub name: String,
    pub cast: Cast,
}

/// 一个绑定值
#[derive(Debug)]
pub enum PgParam {
    Text(Option<String>),
    Bytes(Option<Vec<u8>>),
}

impl ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgParam::Text(v) => v.to_sql(ty, out),
            PgParam::Bytes(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <Option<String> as ToSql>::accepts(ty) || <Option<Vec<u8>> as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

/// PostgreSQL 协议的参数计数上限（Int16）
const MAX_PARAMS: usize = 65_535;
/// 单条语句的字节软上限（只算绑定值本身）
const MAX_STMT_BYTES: usize = 32 * 1024 * 1024;

pub struct PgSinkSession {
    pub(crate) connection: PgConnection,
    pub(crate) database: String,
    pub(crate) qualified_table: String,
    pub(crate) columns: Vec<TargetColumn>,
    pub(crate) meter: Option<Arc<MemMeter>>,
    pub(crate) total_rows: u64,
}

#[async_trait]
impl SinkSession for PgSinkSession {
    async fn write_batch(&mut self, batch: arrow::record_batch::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let ncols = batch.num_columns();
        if ncols != self.columns.len() {
            return Err(D2DError::schema(format!(
                "写入批次列数 {} 与目标表列数 {} 不一致 —— 查询侧要先做列映射",
                ncols,
                self.columns.len()
            )));
        }

        let (client, conn) = self
            .connection
            .config(&self.database)
            .connect(NoTls)
            .await
            .map_err(|e| D2DError::external("postgres", format!("取连接失败: {}", e)))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!(error = %e, "postgres 写入连接结束");
            }
        });

        let col_list = self
            .columns
            .iter()
            .map(|c| d2d_connector::quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        let rows_per_stmt = (MAX_PARAMS / ncols).max(1);

        let mut start = 0usize;
        let mut stmts = 0usize;
        while start < rows {
            let mut params: Vec<PgParam> = Vec::new();
            let mut n = 0usize;
            let mut bytes = 0usize;
            while start + n < rows && n < rows_per_stmt {
                let row_vals: Vec<PgParam> = (0..ncols)
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
                        PgParam::Text(Some(s)) => s.len() + 2,
                        PgParam::Bytes(Some(b)) => b.len() + 2,
                        _ => 8,
                    })
                    .sum();
                if n > 0 && bytes + row_bytes > MAX_STMT_BYTES {
                    break;
                }
                bytes += row_bytes;
                params.extend(row_vals);
                n += 1;
            }
            let sql = self.build_insert(&col_list, n);
            let refs: Vec<&(dyn ToSql + Sync)> =
                params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
            let _lease = self.meter.as_ref().map(|m| m.claim(bytes));
            client
                .execute(&sql, &refs)
                .await
                .map_err(|e| D2DError::sink_write("postgres", e))?;
            stmts += 1;
            start += n;
        }

        self.total_rows += rows as u64;
        debug!(rows, stmts, "postgres 写入完成");
        Ok(rows)
    }

    async fn commit(&mut self) -> D2DResult<()> {
        info!(total_rows = self.total_rows, "postgres session committed");
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

impl PgSinkSession {
    /// `INSERT INTO t (c1,c2) VALUES ($1::text::bigint,$2::text),($3::text::bigint,$4::text)`
    fn build_insert(&self, col_list: &str, rows: usize) -> String {
        let ncols = self.columns.len();
        let mut values = Vec::with_capacity(rows);
        for r in 0..rows {
            let mut one = Vec::with_capacity(ncols);
            for (c, col) in self.columns.iter().enumerate() {
                let idx = r * ncols + c + 1;
                one.push(match &col.cast {
                    Cast::Text(ty) => format!("${}::text::{}", idx, ty),
                    Cast::Bytea => format!("${}", idx),
                });
            }
            values.push(format!("({})", one.join(",")));
        }
        format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.qualified_table,
            col_list,
            values.join(",")
        )
    }
}

/// 一个单元格 → 绑定值（文本承载，`bytea` 绑原始字节）
fn value_of(
    arr: &dyn arrow::array::Array,
    row: usize,
    col: &str,
    dt: &arrow::datatypes::DataType,
) -> D2DResult<PgParam> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    if arr.is_null(row) {
        return Ok(PgParam::Text(None));
    }
    Ok(match dt {
        DataType::Boolean => {
            let v = arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            PgParam::Text(Some(if v { "true" } else { "false" }.to_string()))
        }
        DataType::Binary => {
            let v = arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            PgParam::Bytes(Some(v.to_vec()))
        }
        DataType::LargeBinary => {
            let v = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            PgParam::Bytes(Some(v.to_vec()))
        }
        DataType::Date32 => {
            let days = arr.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
            let d = chrono::DateTime::from_timestamp(days as i64 * 86400, 0)
                .ok_or_else(|| D2DError::schema(format!("列 `{}` 的日期 {} 超出范围", col, days)))?
                .naive_utc()
                .date();
            PgParam::Text(Some(d.format("%Y-%m-%d").to_string()))
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
            PgParam::Text(Some(fmt_time(v, *u, col)?))
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
            PgParam::Text(Some(fmt_timestamp(v, *u, col)?))
        }
        _ => PgParam::Text(Some(
            arrow::util::display::array_value_to_string(arr, row)
                .map_err(|e| D2DError::schema(format!("列 `{}` 转文本失败: {}", col, e)))?,
        )),
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
    use arrow::array::{BooleanArray, Date32Array, StringArray};

    #[test]
    fn test_boolean_binds_as_true_false() {
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        for (i, want) in [(0usize, "true"), (1, "false")] {
            match value_of(&a, i, "b", &arrow::datatypes::DataType::Boolean).unwrap() {
                PgParam::Text(Some(s)) => assert_eq!(s, want),
                _ => panic!(),
            }
        }
        assert!(matches!(
            value_of(&a, 2, "b", &arrow::datatypes::DataType::Boolean).unwrap(),
            PgParam::Text(None)
        ));
    }

    #[test]
    fn test_null_vs_empty_string() {
        let a = StringArray::from(vec![Some(""), None]);
        assert!(matches!(
            value_of(&a, 0, "s", &arrow::datatypes::DataType::Utf8).unwrap(),
            PgParam::Text(Some(ref s)) if s.is_empty()
        ));
        assert!(matches!(
            value_of(&a, 1, "s", &arrow::datatypes::DataType::Utf8).unwrap(),
            PgParam::Text(None)
        ));
    }

    #[test]
    fn test_date_and_binary() {
        let days = (chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let d = Date32Array::from(vec![days]);
        match value_of(&d, 0, "d", &arrow::datatypes::DataType::Date32).unwrap() {
            PgParam::Text(Some(s)) => assert_eq!(s, "2022-01-01"),
            _ => panic!(),
        }
        use arrow::array::BinaryArray;
        let b = BinaryArray::from_vec(vec![&[0u8, 1, 255][..]]);
        match value_of(&b, 0, "b", &arrow::datatypes::DataType::Binary).unwrap() {
            PgParam::Bytes(Some(v)) => assert_eq!(v, vec![0, 1, 255]),
            _ => panic!(),
        }
    }

    fn session(columns: Vec<TargetColumn>) -> PgSinkSession {
        PgSinkSession {
            connection: PgConnection::from_json(&serde_json::json!({"host": "h"})).unwrap(),
            database: "db".into(),
            qualified_table: "\"public\".\"t\"".into(),
            columns,
            meter: None,
            total_rows: 0,
        }
    }

    /// 多行 INSERT 的占位符必须连续编号，且每列 cast 正确
    #[test]
    fn test_build_insert_placeholder_numbering() {
        let s = session(vec![
            TargetColumn { name: "id".into(), cast: Cast::Text("bigint".into()) },
            TargetColumn { name: "b".into(), cast: Cast::Bytea },
            TargetColumn { name: "s".into(), cast: Cast::Text("text".into()) },
        ]);
        let sql = s.build_insert("\"id\",\"b\",\"s\"", 2);
        assert_eq!(
            sql,
            "INSERT INTO \"public\".\"t\" (\"id\",\"b\",\"s\") VALUES \
             ($1::text::bigint,$2,$3::text::text),($4::text::bigint,$5,$6::text::text)"
        );
    }
}
