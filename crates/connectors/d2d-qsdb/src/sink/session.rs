//! QSDB 写入会话：一次 `write_batch` = 一次提交
//!
//! 通过 `qsdb-client::append_batches` 把 Arrow 批编码成 IPC 流 POST 到
//! `qsdb server`。一次调用内按分区归并、写完 manifest 才返回，所以：
//!
//! - [`SinkSession::commit`] 无事可做（数据已经落盘）
//! - [`SinkSession::abort`] **无法撤销已经写入的批** —— 这里如实记一条 warn，
//!   不假装回滚成功。这与其它走 autocommit 的目标端语义一致。
//!
//! # 自动建表
//!
//! QSDB 的 schema 来自表定义（不是"写入时推断"）。首批数据的 Arrow schema 就是
//! 最权威的来源，所以 `create_table: true`（缺省）时在第一次写入惰性建表：
//! 按 Arrow schema 渲染 `CREATE TABLE …`（含 `PARTITIONED BY` /
//! `WITH (compression='…')`），交给服务端执行。
//!
//! 多个会话可能同时写首批，用一把进程内锁 + 原子标志保证只建一次。

use arrow::datatypes::{DataType, IntervalUnit, Schema, TimeUnit};
use async_trait::async_trait;
use d2d_connector::SinkSession;
use d2d_core::{D2DError, D2DResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// 建表选项（`sink.properties`）
#[derive(Debug, Clone)]
pub struct CreateOptions {
    /// 表不存在时按首批 Arrow schema 自动建表（缺省 true）
    pub create_table: bool,
    /// 自动建表时的分区列（QSDB 原生分区）
    pub partition_by: Vec<String>,
    /// 自动建表时的压缩（`zstd` | `snappy`）
    pub compression: Option<String>,
}

impl CreateOptions {
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let create_table = match props.get("create_table") {
            None => true,
            Some(v) => v.as_bool().ok_or_else(|| {
                D2DError::config("sink.properties.create_table 需要布尔值（true/false）")
            })?,
        };

        let partition_by = match props.get("partition_by") {
            None => Vec::new(),
            Some(serde_json::Value::Array(a)) => a
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            D2DError::config(
                                "sink.properties.partition_by 需要列名字符串数组，如 [\"dt\"]",
                            )
                        })
                })
                .collect::<D2DResult<Vec<_>>>()?,
            Some(serde_json::Value::String(s)) => s
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            Some(_) => {
                return Err(D2DError::config(
                    "sink.properties.partition_by 需要列名字符串数组或逗号分隔字符串",
                ))
            }
        };

        let compression = match props.get("compression") {
            None => None,
            Some(serde_json::Value::String(s)) => {
                let s = s.trim().to_ascii_lowercase();
                if s != "zstd" && s != "snappy" {
                    return Err(D2DError::config(format!(
                        "QSDB 只支持 compression: zstd | snappy，收到 `{s}`"
                    )));
                }
                Some(s)
            }
            Some(_) => {
                return Err(D2DError::config(
                    "sink.properties.compression 需要字符串（zstd | snappy）",
                ))
            }
        };

        Ok(Self {
            create_table,
            partition_by,
            compression,
        })
    }
}

/// Arrow 类型 → QSDB DDL 类型
///
/// 与 QSDB 服务端的类型映射保持同一口径（详见 QSDB 的 `LogicalType::try_from_arrow`）。
/// 不在表内的类型**显式拒绝**，不做有损降级。
fn qsdb_sql_type(dt: &DataType) -> D2DResult<String> {
    let t = match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INT".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) => {
            if !(1..=38).contains(p) {
                return Err(D2DError::schema(format!(
                    "DECIMAL precision {p} 超出 QSDB 的 1..=38"
                )));
            }
            if *s < 0 || i16::from(*s) > i16::from(*p) {
                return Err(D2DError::schema(format!("DECIMAL scale {s} 超出 0..={p}")));
            }
            format!("DECIMAL({p}, {s})")
        }
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "STRING".to_string(),
        DataType::Binary => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time64(TimeUnit::Nanosecond) => "TIME".to_string(),
        DataType::Timestamp(_, tz) => match tz {
            Some(tz) => format!("TIMESTAMP WITH TIME ZONE ({tz})"),
            None => "TIMESTAMP".to_string(),
        },
        DataType::Interval(IntervalUnit::MonthDayNano) => "INTERVAL".to_string(),
        other => {
            return Err(D2DError::schema(format!(
                "列类型 {other:?} 无法写入 QSDB —— 支持 Boolean / 有符号整型 / 浮点 / \
                 Decimal128 / Utf8 / Binary / Date32 / Date64 / Time64(ns) / Timestamp / \
                 Interval(MonthDayNano)"
            )))
        }
    };
    Ok(t)
}

/// 按 Arrow schema 渲染 QSDB 建表 DDL
fn create_table_ddl(table: &str, schema: &Schema, opts: &CreateOptions) -> D2DResult<String> {
    let mut out = String::with_capacity(256);
    out.push_str("CREATE TABLE ");
    out.push_str(table);
    out.push_str(" (\n");
    for (i, f) in schema.fields().iter().enumerate() {
        if i > 0 {
            out.push_str(",\n");
        }
        out.push_str("  ");
        out.push_str(f.name());
        out.push(' ');
        out.push_str(&qsdb_sql_type(f.data_type())?);
        if !f.is_nullable() {
            out.push_str(" NOT NULL");
        }
    }
    out.push_str("\n)");

    if !opts.partition_by.is_empty() {
        for key in &opts.partition_by {
            if !schema.fields().iter().any(|f| f.name() == key) {
                let cols = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(D2DError::config(format!(
                    "sink.properties.partition_by 的列 `{key}` 不在输入 schema 里（可用列: {cols}）"
                )));
            }
        }
        out.push_str(" PARTITIONED BY (");
        out.push_str(&opts.partition_by.join(", "));
        out.push(')');
    }

    if let Some(c) = &opts.compression {
        // 压缩名已在 CreateOptions 里校验过；这里仍做一次转义，保持 DDL 可组合
        out.push_str(" WITH (compression = '");
        out.push_str(&c.replace('\'', "''"));
        out.push_str("')");
    }
    out.push(';');
    Ok(out)
}

fn ensure_table_http(
    client: &qsdb_client::Client,
    table: &str,
    schema: &Schema,
    opts: &CreateOptions,
) -> D2DResult<()> {
    let exists = client
        .tables()
        .map_err(|e| D2DError::external("qsdb-http", format!("列出表失败: {e}")))?
        .iter()
        .any(|t| t == table);
    if exists {
        return Ok(());
    }
    if !opts.create_table {
        return Err(D2DError::config(format!(
            "QSDB 表 `default.{table}` 不存在；请先建表，\
             或设置 sink.properties.create_table: true（按首批 Arrow schema 自动建表）"
        )));
    }
    let ddl = create_table_ddl(table, schema, opts)?;
    client.execute(&ddl).map_err(|e| {
        D2DError::external(
            "qsdb-http",
            format!("自动建表失败（DDL: {}）: {e}", ddl.trim()),
        )
    })?;
    info!(table = %table, "QSDB 表不存在，已按首批 Arrow schema 自动建表");
    Ok(())
}

pub struct QsdbSinkSession {
    client: Arc<qsdb_client::Client>,
    table: String,
    opts: CreateOptions,
    /// 表已确认存在（或 `create_table: false` 时 init 已校验过）
    table_ready: Arc<AtomicBool>,
    create_lock: Arc<Mutex<()>>,
    total_rows: u64,
}

impl QsdbSinkSession {
    pub fn new(
        client: Arc<qsdb_client::Client>,
        table: String,
        opts: CreateOptions,
        table_ready: Arc<AtomicBool>,
        create_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            client,
            table,
            opts,
            table_ready,
            create_lock,
            total_rows: 0,
        }
    }
}

#[async_trait]
impl SinkSession for QsdbSinkSession {
    async fn write_batch(&mut self, batch: arrow::record_batch::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }

        let client = Arc::clone(&self.client);
        let table = self.table.clone();
        let opts = self.opts.clone();
        let ready = Arc::clone(&self.table_ready);
        let lock = Arc::clone(&self.create_lock);
        let schema = batch.schema();

        let written = tokio::task::spawn_blocking(move || -> D2DResult<u64> {
            // 只有首批（建表前）需要拿锁；建好之后走原子快路径
            if !ready.load(Ordering::Acquire) {
                let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
                if !ready.load(Ordering::Acquire) {
                    ensure_table_http(&client, &table, &schema, &opts)?;
                    ready.store(true, Ordering::Release);
                }
            }
            let ack = client
                .append_batches(
                    &table,
                    std::slice::from_ref(&batch),
                    &qsdb_client::WriteOptions::append(),
                )
                .map_err(|e| {
                    D2DError::external("qsdb-http", format!("追加写入 default.{table} 失败: {e}"))
                })?;
            Ok(ack.rows)
        })
        .await
        .map_err(|e| D2DError::internal(format!("qsdb 写入任务失败: {e}")))??;

        self.total_rows += written;
        usize::try_from(written)
            .map_err(|_| D2DError::internal(format!("QSDB 返回的行数 {written} 超出 usize")))
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // append 已在 write_batch 内提交（一次调用一次提交）
        info!(table = %self.table, rows = self.total_rows, "qsdb 写入会话完成");
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        warn!(
            table = %self.table,
            rows = self.total_rows,
            "qsdb 会话中止：已 append 的批是自动提交的，无法回滚"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_create_options() {
        let props = serde_json::Map::new();
        let o = CreateOptions::from_props(&props).unwrap();
        assert!(o.create_table);
        assert!(o.partition_by.is_empty());
        assert_eq!(o.compression, None);

        let props = match json!({
            "create_table": false,
            "partition_by": ["dt"],
            "compression": "snappy"
        }) {
            serde_json::Value::Object(m) => m,
            _ => unreachable!(),
        };
        let o = CreateOptions::from_props(&props).unwrap();
        assert!(!o.create_table);
        assert_eq!(o.partition_by, vec!["dt"]);
        assert_eq!(o.compression.as_deref(), Some("snappy"));
    }

    #[test]
    fn test_bad_compression() {
        let props = match json!({"compression": "lz4"}) {
            serde_json::Value::Object(m) => m,
            _ => unreachable!(),
        };
        let e = CreateOptions::from_props(&props).unwrap_err().to_string();
        assert!(e.contains("zstd"), "{}", e);
    }

    #[test]
    fn test_create_table_ddl() {
        use arrow::datatypes::{DataType, Field};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("dt", DataType::Date32, false),
        ]);
        let opts = CreateOptions {
            create_table: true,
            partition_by: vec!["dt".into()],
            compression: Some("zstd".into()),
        };
        let ddl = create_table_ddl("events", &schema, &opts).unwrap();
        assert!(ddl.contains("CREATE TABLE events"), "{}", ddl);
        assert!(ddl.contains("id BIGINT NOT NULL"), "{}", ddl);
        assert!(ddl.contains("name STRING"), "{}", ddl);
        assert!(ddl.contains("PARTITIONED BY (dt)"), "{}", ddl);
        assert!(ddl.contains("compression = 'zstd'"), "{}", ddl);
    }

    #[test]
    fn test_unsigned_arrow_is_rejected() {
        use arrow::datatypes::{DataType, Field};
        let schema = Schema::new(vec![Field::new("u", DataType::UInt32, false)]);
        let opts = CreateOptions {
            create_table: true,
            partition_by: vec![],
            compression: None,
        };
        let e = create_table_ddl("t", &schema, &opts)
            .unwrap_err()
            .to_string();
        assert!(e.contains("UInt32"), "{}", e);
    }

    #[test]
    fn test_decimal_validation() {
        use arrow::datatypes::{DataType, Field};
        let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(0, 0), true)]);
        let opts = CreateOptions {
            create_table: true,
            partition_by: vec![],
            compression: None,
        };
        assert!(create_table_ddl("t", &schema, &opts).is_err());

        let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(10, 11), true)]);
        assert!(create_table_ddl("t", &schema, &opts).is_err());
    }

    #[test]
    fn test_partition_column_must_exist() {
        use arrow::datatypes::{DataType, Field};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let opts = CreateOptions {
            create_table: true,
            partition_by: vec!["dt".into()],
            compression: None,
        };
        let e = create_table_ddl("t", &schema, &opts)
            .unwrap_err()
            .to_string();
        assert!(e.contains("dt"), "{}", e);
    }
}
