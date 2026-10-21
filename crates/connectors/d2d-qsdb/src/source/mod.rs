//! QSDB **reader 方向**：连接 `qsdb server` 的 HTTP 流式查询
//!
//! 走 QSDB 官方 `qsdb-client` 的 `query_stream`（rows 通道，**客户端内存恒定**），
//! 再把一块块行式数据转成 Arrow 批。SQL 由本连接器拼：
//! `SELECT <columns> FROM <table> [WHERE …] [ORDER BY …] [LIMIT n]`。
//!
//! `where_clause` / `order_by` / 表达式投影都能下推（走完整 SQL）。
//!
//! ⚠️ QSDB 的 SQL 方言里**双引号是字符串字面量**，标识符用**反引号**
//! （或裸名）。连接器自动生成的列名/表名已用反引号；你在 `where_clause` /
//! `order_by` / 表达式投影里手写列名时也要用反引号，例如
//! `where_clause: "`flag` = 1"`。写成 `"flag"` 会变成常量字符串，筛选结果
//! 静默全真/全假 —— 这是实测踩到的坑。
//!
//! # 取数单元 = 一次查询
//!
//! 只切**一个**分片：一次查询就是服务端的一个执行单元，客户端再切只会把同一份
//! 结果查多遍。引擎的 `flow.source.concurrency` 对 QSDB 源不会放大并行度。
//!
//! # 阻塞与流
//!
//! `qsdb-client` 是同步的。HTTP 请求/解码全部放进 `spawn_blocking`，批次经有界
//! channel 交给异步流，下游处理与取数重叠、内存有上界。

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    IntervalMonthDayNanoBuilder, StringBuilder, Time64NanosecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNano, IntervalUnit, Schema, SchemaRef, TimeUnit,
};
use async_trait::async_trait;
use d2d_connector::{
    QueryColumn, QueryConfig, SourceConnector, SourceSpec, SourceSplit, UnitFootprint,
};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use qsdb_client::{Chunk, ColumnDesc, Tag, ValueRef};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::config::{http_db_name, QsdbConnection};

/// 连接器与取数线程之间的在手批次数（背压深度）
const CHANNEL_DEPTH: usize = 2;
/// HTTP 模式每次请求/解码的在手缓冲名义值（rows 块 + 转 Arrow 的批）
const NOMINAL_HTTP_BUFFER: usize = 16 * 1024 * 1024;

/// HTTP 模式在 init 时固定下来的查询计划
#[derive(Clone)]
struct HttpSource {
    conn: QsdbConnection,
    /// 取数 SQL（带用户 LIMIT）
    sql: String,
    /// rows 协议的列类型（与 schema 逐列对齐）
    tags: Vec<Tag>,
}

pub struct QsdbSourceConnector {
    name: String,
    http: Option<HttpSource>,
    schema: Option<SchemaRef>,
    rows_read: Arc<AtomicU64>,
}

impl QsdbSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "qsdb".to_string(),
            http: None,
            schema: None,
            rows_read: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for QsdbSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// HTTP：SQL 组装 / rows schema → Arrow / Chunk → RecordBatch
// ---------------------------------------------------------------------------

/// QSDB SQL 的标识符引号是**反引号**。
///
/// ⚠️ 这是实测出来的方言差异：QSDB 把双引号解析成**字符串字面量** ——
/// `SELECT "id"` 返回常量 `'id'`（schema 是 Utf8），不是列。通用连接器的
/// `quote_ident`（双引号）在 QSDB 上会静默返回错数据，所以这里必须用反引号。
fn qsdb_quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn qsdb_column_sql(c: &QueryColumn) -> String {
    match c {
        QueryColumn::Name(n) => qsdb_quote_ident(n),
        QueryColumn::Expr { expr, alias } => {
            format!("{expr} AS {}", qsdb_quote_ident(alias))
        }
    }
}

/// 拼 HTTP 模式的查询 SQL（`where/order` 是 QSDB SQL 方言，原样下推）
fn build_http_sql(table: &str, query: &QueryConfig, limit_zero: bool) -> D2DResult<String> {
    let cols = match &query.columns {
        None => "*".to_string(),
        Some(cols) if cols.is_empty() => {
            return Err(D2DError::config("qsdb query.columns 不能为空数组"))
        }
        Some(cols) => cols
            .iter()
            .map(qsdb_column_sql)
            .collect::<Vec<_>>()
            .join(", "),
    };
    let mut sql = format!("SELECT {cols} FROM {}", qsdb_quote_ident(table));
    if let Some(w) = query
        .where_clause
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }
    if let Some(o) = query
        .order_by
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sql.push_str(" ORDER BY ");
        sql.push_str(o);
    }
    if limit_zero {
        sql.push_str(" LIMIT 0");
    } else if let Some(n) = query.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

fn http_err(context: &str, e: qsdb_client::Error) -> D2DError {
    D2DError::external("qsdb-http", format!("{context}: {e}"))
}

/// rows 协议的列描述 → Arrow schema
fn arrow_schema_from_descs(descs: &[ColumnDesc]) -> D2DResult<SchemaRef> {
    let mut fields = Vec::with_capacity(descs.len());
    for d in descs {
        let dt = match d.tag {
            Tag::Bool => DataType::Boolean,
            Tag::I8 => DataType::Int8,
            Tag::I16 => DataType::Int16,
            Tag::I32 => DataType::Int32,
            Tag::I64 => DataType::Int64,
            Tag::F32 => DataType::Float32,
            Tag::F64 => DataType::Float64,
            Tag::Decimal128 => {
                let (p, s) = d.decimal.ok_or_else(|| {
                    D2DError::schema(format!(
                        "QSDB 返回 DECIMAL 列 `{}` 却没有 precision/scale",
                        d.name
                    ))
                })?;
                DataType::Decimal128(p, s)
            }
            Tag::Utf8 => DataType::Utf8,
            Tag::Binary => DataType::Binary,
            Tag::Date32 => DataType::Date32,
            Tag::Time64Ns => DataType::Time64(TimeUnit::Nanosecond),
            Tag::TimestampNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, d.tz.as_deref().map(Arc::from))
            }
            Tag::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        };
        fields.push(Field::new(&d.name, dt, d.nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn type_mismatch(col: &str, expected: &str, got: &ValueRef) -> D2DError {
    D2DError::external(
        "qsdb-http",
        format!("列 `{col}` 声明为 {expected}，服务端却返回 {got:?}（协议类型不一致）"),
    )
}

/// rows 协议的一块 → Arrow `RecordBatch`
fn chunk_to_batch(
    chunk: &Chunk,
    schema: &SchemaRef,
    tags: &[Tag],
) -> D2DResult<arrow::record_batch::RecordBatch> {
    let n = chunk.num_rows();
    if chunk.num_columns() != schema.fields().len() || tags.len() != schema.fields().len() {
        return Err(D2DError::external(
            "qsdb-http",
            format!(
                "rows 块列数 {} 与 schema 列数 {} 不一致",
                chunk.num_columns(),
                schema.fields().len()
            ),
        ));
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(tags.len());
    for (c, tag) in tags.iter().enumerate() {
        let field = schema.field(c);
        let col = field.name();
        let arr: ArrayRef = match tag {
            Tag::Bool => {
                let mut b = BooleanBuilder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Bool(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "BOOLEAN", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::I8 => {
                let mut b = Int8Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::I8(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "TINYINT", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::I16 => {
                let mut b = Int16Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::I16(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "SMALLINT", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::I32 => {
                let mut b = Int32Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::I32(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "INT", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::I64 => {
                let mut b = Int64Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::I64(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "BIGINT", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::F32 => {
                let mut b = Float32Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::F32(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "FLOAT", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::F64 => {
                let mut b = Float64Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::F64(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "DOUBLE", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Decimal128 => {
                let (p, s) = match field.data_type() {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(D2DError::internal(format!(
                            "列 `{col}` 的 schema 不是 Decimal128"
                        )))
                    }
                };
                let mut b = Decimal128Builder::new()
                    .with_precision_and_scale(p, s)
                    .map_err(|e| {
                        D2DError::schema(format!("列 `{col}` 的 DECIMAL({p},{s}) 非法: {e}"))
                    })?;
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Decimal128(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "DECIMAL", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Utf8 => {
                let mut b = StringBuilder::with_capacity(n, n * 16);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Utf8(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "STRING", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Binary => {
                let mut b = BinaryBuilder::with_capacity(n, n * 16);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Binary(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "BINARY", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Date32 => {
                let mut b = Date32Builder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Date32(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "DATE", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Time64Ns => {
                let mut b = Time64NanosecondBuilder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Time64Ns(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "TIME", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::TimestampNs => {
                let tz = match field.data_type() {
                    DataType::Timestamp(_, tz) => tz.clone(),
                    _ => {
                        return Err(D2DError::internal(format!(
                            "列 `{col}` 的 schema 不是 Timestamp"
                        )))
                    }
                };
                let mut b = TimestampNanosecondBuilder::with_capacity(n);
                if let Some(tz) = tz {
                    b = b.with_timezone(tz.as_ref());
                }
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::TimestampNs(v) => b.append_value(v),
                        v => return Err(type_mismatch(col, "TIMESTAMP", &v)),
                    }
                }
                Arc::new(b.finish())
            }
            Tag::Interval => {
                let mut b = IntervalMonthDayNanoBuilder::with_capacity(n);
                for r in 0..n {
                    match chunk.get(r, c).map_err(|e| http_err("读 rows 块", e))? {
                        ValueRef::Null => b.append_null(),
                        ValueRef::Interval {
                            months,
                            days,
                            nanos,
                        } => b.append_value(IntervalMonthDayNano::new(months, days, nanos)),
                        v => return Err(type_mismatch(col, "INTERVAL", &v)),
                    }
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(arr);
    }

    arrow::record_batch::RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|e| D2DError::schema(format!("组装 RecordBatch 失败: {e}")))
}

// ---------------------------------------------------------------------------
// trait 实现
// ---------------------------------------------------------------------------

#[async_trait]
impl SourceConnector for QsdbSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        QsdbConnection::validate_method(spec.method.as_deref())?;
        let connection = QsdbConnection::from_json(&spec.connection)?;
        http_db_name(spec.schema_name.as_deref())?;

        if spec.effective_partition()?.is_some() {
            return Err(D2DError::config(
                "qsdb source 不接受 source.partition —— 一次查询本身就是单个取数单元；\
                 请删掉 partition 配置",
            ));
        }

        let query = spec.query.clone().unwrap_or_default();
        let schema_sql = build_http_sql(&spec.table, &query, true)?;
        let sql = build_http_sql(&spec.table, &query, false)?;
        let conn_for_init = connection.clone();

        let (schema, tags) = tokio::task::spawn_blocking(move || -> D2DResult<_> {
            let client = conn_for_init.http_client()?;
            let opts = conn_for_init.http_query_options();
            let mut stream = client
                .query_stream(&schema_sql, &opts)
                .map_err(|e| http_err("获取 schema", e))?;
            let descs = stream.schema().to_vec();
            stream
                .finish()
                .map_err(|e| http_err("结束 schema 查询", e))?;
            let schema = arrow_schema_from_descs(&descs)?;
            let tags = descs.iter().map(|d| d.tag).collect();
            Ok((schema, tags))
        })
        .await
        .map_err(|e| D2DError::internal(format!("qsdb http 初始化任务失败: {e}")))??;

        info!(
            connector = %self.name,
            table = %spec.table,
            columns = schema.fields().len(),
            sql = %sql,
            "qsdb source 已连接"
        );

        self.http = Some(HttpSource {
            conn: connection,
            sql,
            tags,
        });
        self.schema = Some(schema);
        Ok(())
    }

    async fn schema(&self) -> D2DResult<SchemaRef> {
        self.schema
            .clone()
            .ok_or_else(|| D2DError::schema("schema not fetched, call init() first"))
    }

    async fn split(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        // 单分片：一次查询就是服务端的一个执行单元
        let split = SourceSplit {
            split_id: format!("qsdb-{}", spec.table),
            split_info: serde_json::json!({ "table": spec.table }),
        };
        debug!(table = %spec.table, "qsdb split generated (1)");
        Ok(vec![split])
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| D2DError::internal("qsdb source 未初始化：先调 init()"))?;
        let rows_read = Arc::clone(&self.rows_read);
        let split_id = split.split_id.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<arrow::record_batch::RecordBatch>>(
            CHANNEL_DEPTH,
        );

        let http = self
            .http
            .clone()
            .ok_or_else(|| D2DError::internal("qsdb source 未初始化：先调 init()"))?;
        tokio::task::spawn_blocking(move || {
            let client = match http.conn.http_client() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };
            let opts = http.conn.http_query_options();
            let mut stream = match client.query_stream(&http.sql, &opts) {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.blocking_send(Err(http_err("发起查询", e)));
                    return;
                }
            };

            let mut batches = 0u64;
            loop {
                match stream.next_chunk() {
                    Ok(Some(chunk)) => {
                        if chunk.num_rows() == 0 {
                            continue;
                        }
                        rows_read.fetch_add(chunk.num_rows() as u64, Ordering::Relaxed);
                        let batch = match chunk_to_batch(&chunk, &schema, &http.tags) {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.blocking_send(Err(e));
                                break;
                            }
                        };
                        batches += 1;
                        if tx.blocking_send(Ok(batch)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(http_err("读取查询流", e)));
                        break;
                    }
                }
            }
            debug!(split = %split_id, batches, "qsdb split 读完");
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
        UnitFootprint::connection(NOMINAL_HTTP_BUFFER)
    }

    fn exported_rows(&self) -> Option<u64> {
        Some(self.rows_read.load(Ordering::Relaxed))
    }

    async fn close(&mut self) -> D2DResult<()> {
        self.http = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::QueryConfig;

    #[test]
    fn test_build_http_sql_full_pushdown() {
        let q = QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("id".into()),
                QueryColumn::Expr {
                    expr: "`amount` * 1.1".into(),
                    alias: "scaled".into(),
                },
            ]),
            where_clause: Some("id > 10".into()),
            order_by: Some("id DESC".into()),
            limit: Some(100),
        };
        let sql = build_http_sql("events", &q, false).unwrap();
        assert_eq!(
            sql,
            "SELECT `id`, `amount` * 1.1 AS `scaled` FROM `events` \
             WHERE id > 10 ORDER BY id DESC LIMIT 100"
        );

        let schema_sql = build_http_sql("events", &q, true).unwrap();
        assert!(schema_sql.ends_with("LIMIT 0"), "{}", schema_sql);
    }

    #[test]
    fn test_arrow_schema_from_rows_descs() {
        let descs = vec![
            ColumnDesc {
                name: "id".into(),
                tag: Tag::I64,
                nullable: false,
                max_chars: None,
                tz: None,
                decimal: None,
            },
            ColumnDesc {
                name: "amount".into(),
                tag: Tag::Decimal128,
                nullable: true,
                max_chars: None,
                tz: None,
                decimal: Some((18, 4)),
            },
            ColumnDesc {
                name: "ts".into(),
                tag: Tag::TimestampNs,
                nullable: true,
                max_chars: None,
                tz: Some("UTC".into()),
                decimal: None,
            },
        ];
        let schema = arrow_schema_from_descs(&descs).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(18, 4));
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }
}
