//! PostgreSQL **reader 方向**
//!
//! 走 `tokio-postgres` 的**简单查询协议**（`simple_query`）：结果一律是文本，
//! 正好交给共享的 `d2d_types::text_batch::RowBuilder` 按逻辑类型解析。
//! 所以 SELECT 列表里每列都做了 `::text` 投影（见 [`crate::schema`]）。
//!
//! # 取数单元 = 一条连接
//!
//! 一次 [`SourceConnector::read`] 建一条连接、跑一句 SQL、读到 EOF 再断开。
//! （`tokio-postgres` 没有连接池；一个 `Client` 内部只有一条连接，并发查询会排队，
//! 所以每个取数单元各开一条。）

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_connector::{
    MemMeter, QueryConfig, SourceConnector, SourceSpec, SourceSplit, UnitFootprint,
};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use d2d_types::text_batch::{RowBuildOptions, RowBuilder};
use d2d_types::LogicalType;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::{Client, NoTls};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::config::PgConnection;
use crate::sql::PgSqlBuilder;

const BATCH_ROWS: usize = 8192;
const NOMINAL_BATCH_BYTES: usize = 4 * 1024 * 1024;
const NOMINAL_CONN_READ_BUFFER: usize = 4 * 1024 * 1024;
const CHANNEL_DEPTH: usize = 2;

#[derive(Debug, Clone)]
struct ResolvedQuery {
    schema: String,
    table: String,
    query: QueryConfig,
}

pub struct PgSourceConnector {
    name: String,
    connection: Option<PgConnection>,
    database: Option<String>,
    schema: Option<SchemaRef>,
    logical: Vec<LogicalType>,
    /// 已渲染好的 SELECT 列表（每列带 `::text` 投影）
    select_list: String,
    bytea_columns: Vec<bool>,
    resolved: Option<ResolvedQuery>,
    rows_read: Arc<AtomicU64>,
    started: Arc<AtomicBool>,
    meter: Option<Arc<MemMeter>>,
}

impl PgSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "postgres".to_string(),
            connection: None,
            database: None,
            schema: None,
            logical: Vec::new(),
            select_list: String::new(),
            bytea_columns: Vec::new(),
            resolved: None,
            rows_read: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            meter: None,
        }
    }

    /// 库名：`source.schema_name` 优先（引擎给命名空间留的通用位置），兼容 `connection.database`
    ///
    /// ⚠️ 注意 PostgreSQL 里 `schema`（public）与 `database` 是两层；本连接器的
    /// `schema_name` 装的是**数据库名**，表所在的 schema 固定用 `public`。
    /// （与达梦的 `schema_name` 语义不同，但那是"引擎的通用槽位 + 各库自己解释"的既定做法。）
    fn database_of(spec: &SourceSpec) -> D2DResult<String> {
        spec.schema_name
            .clone()
            .or_else(|| {
                spec.connection
                    .get("database")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                D2DError::config("postgres source 需要 `source.schema_name`（数据库名）")
            })
    }

    async fn connect(&self) -> D2DResult<Client> {
        let conn = self
            .connection
            .as_ref()
            .ok_or_else(|| D2DError::internal("postgres source 未初始化：先调 init()"))?;
        let db = self
            .database
            .as_ref()
            .ok_or_else(|| D2DError::internal("postgres source 未初始化：先调 init()"))?;
        let (client, connection) = conn
            .config(db)
            .connect(NoTls)
            .await
            .map_err(|e| D2DError::external("postgres", format!("连不上 PostgreSQL: {}", e)))?;
        // 连接必须有人驱动；Client 释放后它自己结束
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                debug!(error = %e, "postgres 连接关闭");
            }
        });
        Ok(client)
    }

    pub fn build_split_sql(&self, split: &SourceSplit) -> String {
        let Some(r) = &self.resolved else {
            return String::new();
        };
        PgSqlBuilder::new(Some(&r.schema), &r.table).build_query(
            &self.select_list,
            &r.query,
            &split_condition(split),
        )
    }

    async fn plan_splits(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        use d2d_connector::partition::{PartitionMode, ProbedBounds};
        let Some(p) = spec.effective_partition()? else {
            return Ok(vec![Self::single_split(&spec.table)]);
        };
        let probed = if p.mode == PartitionMode::Range && (p.lower.is_none() || p.upper.is_none())
        {
            let col = p.column.clone().unwrap_or_default();
            let (lo, hi) = self.probe_min_max(spec, &col).await?;
            let cuts = d2d_connector::partition::range_cuts(&lo, &hi, p.count)?;
            info!(column = %col, cuts = cuts.len(), "postgres range 分片：MIN/MAX 探测完成");
            Some(ProbedBounds::CutPoints(cuts))
        } else {
            None
        };
        let plan = d2d_connector::partition::plan(
            &p,
            &spec.table,
            spec.schema_name.as_deref(),
            probed,
        )?;
        Ok(plan
            .partitions
            .iter()
            .enumerate()
            .map(|(i, part)| SourceSplit {
                split_id: format!("{}_{}", spec.table, part.id),
                split_info: serde_json::json!({
                    "mode": p.mode.as_str(),
                    "index": i,
                    "describe": part.describe,
                    "where_condition": crate::sql::render_partition_predicate(&part.predicate),
                }),
            })
            .collect())
    }

    fn single_split(table: &str) -> SourceSplit {
        SourceSplit {
            split_id: format!("{}_single", table),
            split_info: serde_json::json!({"mode":"none","index":0,"where_condition":""}),
        }
    }

    async fn probe_min_max(
        &self,
        spec: &SourceSpec,
        column: &str,
    ) -> D2DResult<(d2d_connector::partition::Literal, d2d_connector::partition::Literal)> {
        if column.trim().is_empty() {
            return Err(D2DError::config(
                "partition.mode=range 需要 column（或显式给 lower+upper 以跳过探测）",
            ));
        }
        let client = self.connect().await?;
        let sql = PgSqlBuilder::new(Some("public"), &spec.table).build_min_max(column);
        let row = client
            .query_one(&sql, &[])
            .await
            .map_err(|e| D2DError::external("postgres", format!("MIN/MAX 探测失败: {}", e)))?;
        let min_s: Option<String> = row.get(0);
        let max_s: Option<String> = row.get(1);
        match (min_s, max_s) {
            (Some(a), Some(b)) => Ok(crate::sql::literal_pair(&a, &b)),
            _ => Err(D2DError::external(
                "postgres",
                format!(
                    "range 分片探测 MIN/MAX 返回空值（列 {} 全为 NULL 或表为空）—— \
                     请改用 partition.mode=list / none",
                    column
                ),
            )),
        }
    }
}

pub fn split_condition(split: &SourceSplit) -> String {
    split
        .split_info
        .get("where_condition")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

#[async_trait]
impl SourceConnector for PgSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        let database = Self::database_of(spec)?;
        let connection = PgConnection::from_json(&spec.connection)?;
        info!(
            connector = %self.name, table = %spec.table, database = %database,
            host = %connection.host, port = connection.port, "initializing postgres source"
        );
        if let Some(p) = spec.effective_partition()? {
            p.validate()?;
        }
        self.connection = Some(connection);
        self.database = Some(database.clone());

        let client = self.connect().await?;
        let query = spec.query.clone().unwrap_or_default();
        let query = match spec
            .effective_partition()?
            .and_then(|p| p.column.filter(|c| !c.trim().is_empty()))
        {
            Some(col) => {
                let q = query;
                // 分区列没有 ::text 投影就会让 SELECT 列表与 schema 对不上 ——
                // 这里不自动补：`select_list` 是按列生成的，补列要重跑 resolve。
                if !q
                    .columns
                    .as_ref()
                    .map(|cs| cs.iter().any(|c| c.output_name() == col))
                    .unwrap_or(true)
                {
                    return Err(D2DError::config(format!(
                        "partition.column `{}` 必须出现在 query.columns 里（PostgreSQL 源不自动补列）",
                        col
                    )));
                }
                q
            }
            None => query,
        };

        let resolved = crate::schema::resolve_schema(&client, "public", spec, &query).await?;
        drop(client);

        self.schema = Some(Arc::clone(&resolved.schema));
        self.logical = resolved.logical;
        self.select_list = resolved.select_list;
        self.bytea_columns = resolved.bytea_columns;
        self.resolved = Some(ResolvedQuery {
            schema: "public".to_string(),
            table: spec.table.clone(),
            query,
        });
        self.meter = Some(MemMeter::new(
            "postgres 读缓冲",
            format!("每条连接约 {} B 的结果缓冲（按需增长）", NOMINAL_CONN_READ_BUFFER),
        ));
        Ok(())
    }

    async fn schema(&self) -> D2DResult<SchemaRef> {
        self.schema
            .clone()
            .ok_or_else(|| D2DError::schema("schema not fetched, call init() first"))
    }

    async fn split(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        let splits = self.plan_splits(spec).await?;
        info!(connector = %self.name, table = %spec.table, splits = splits.len(), "postgres split generated");
        Ok(splits)
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        if self.resolved.is_none() {
            return Err(D2DError::internal("postgres source 未初始化：先调 init()"));
        }
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| D2DError::internal("postgres source 未初始化：先调 init()"))?;
        let logical = self.logical.clone();
        let bytea = self.bytea_columns.clone();
        let sql = self.build_split_sql(split);
        if sql.is_empty() {
            return Err(D2DError::internal("postgres source 未初始化：先调 init()"));
        }
        let connection = self
            .connection
            .clone()
            .ok_or_else(|| D2DError::internal("postgres source 未初始化：先调 init()"))?;
        let database = self
            .database
            .clone()
            .ok_or_else(|| D2DError::internal("postgres source 未初始化：先调 init()"))?;
        let rows_read = Arc::clone(&self.rows_read);
        let started = Arc::clone(&self.started);
        let meter = self.meter.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<arrow::record_batch::RecordBatch>>(
            CHANNEL_DEPTH,
        );
        tokio::spawn(async move {
            let res = stream_split(
                connection, database, sql, schema, logical, bytea, &tx, &rows_read, &started,
                meter,
            )
            .await;
            if let Err(e) = res {
                let _ = tx.send(Err(e)).await;
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
        UnitFootprint::connection(NOMINAL_CONN_READ_BUFFER + NOMINAL_BATCH_BYTES)
    }

    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    fn exported_rows(&self) -> Option<u64> {
        if self.started.load(Ordering::Relaxed) {
            Some(self.rows_read.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing postgres source");
        self.schema = None;
        self.logical.clear();
        self.resolved = None;
        self.connection = None;
        self.meter = None;
        Ok(())
    }
}

impl Default for PgSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_split(
    connection: PgConnection,
    database: String,
    sql: String,
    schema: SchemaRef,
    logical: Vec<LogicalType>,
    bytea_columns: Vec<bool>,
    tx: &tokio::sync::mpsc::Sender<D2DResult<arrow::record_batch::RecordBatch>>,
    rows_read: &AtomicU64,
    started: &AtomicBool,
    meter: Option<Arc<MemMeter>>,
) -> D2DResult<()> {
    let (client, conn) = connection
        .config(&database)
        .connect(NoTls)
        .await
        .map_err(|e| D2DError::external("postgres", format!("取连接失败: {}", e)))?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            debug!(error = %e, "postgres 连接结束");
        }
    });
    let _lease = meter.as_ref().map(|m| m.claim(NOMINAL_CONN_READ_BUFFER));

    let ncols = schema.fields().len();
    let stream = client
        .simple_query_raw(&sql)
        .await
        .map_err(|e| D2DError::external("postgres", format!("查询失败（{}）: {}", sql, e)))?;
    // SimpleQueryStream 是 !Unpin（内部 PhantomPinned），要 pin 住才能 next()
    let mut stream = Box::pin(stream);
    started.store(true, Ordering::Relaxed);

    let new_builder = |logical: &[LogicalType]| -> D2DResult<RowBuilder> {
        RowBuilder::new(
            Arc::clone(&schema),
            logical.to_vec(),
            // PostgreSQL：'' 是真值，只有 NULL 才是 NULL
            RowBuildOptions::mysql_protocol(),
            BATCH_ROWS,
        )
    };
    let mut rb = new_builder(&logical)?;
    let mut batch_bytes = 0usize;
    let mut row_no: u64 = 0;

    while let Some(msg) = stream.next().await {
        let msg = msg.map_err(|e| D2DError::external("postgres", format!("取行失败: {}", e)))?;
        let SimpleQueryMessage::Row(row) = msg else {
            continue; // CommandComplete / RowDescription
        };
        // 第一遍：把 bytea 的十六进制串解码出来（`decoded` 先只读，下面借用它）
        let raw: Vec<Option<&str>> = (0..ncols).map(|i| row.get(i)).collect();
        let decoded: Vec<Option<Box<[u8]>>> = (0..ncols)
            .map(|i| {
                if bytea_columns.get(i).copied().unwrap_or(false) {
                    raw[i].map(|s| decode_hex(s).map(Vec::into_boxed_slice)).transpose()
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>, D2DError>>()?;

        let cells: Vec<Option<&[u8]>> = (0..ncols)
            .map(|i| {
                if bytea_columns.get(i).copied().unwrap_or(false) {
                    decoded[i].as_deref()
                } else {
                    raw[i].map(|s| {
                        batch_bytes += s.len();
                        s.as_bytes()
                    })
                }
            })
            .collect();

        rb.append_row(&cells, row_no)?;
        row_no += 1;

        if rb.rows() as usize >= BATCH_ROWS || batch_bytes >= NOMINAL_BATCH_BYTES {
            let n = rb.rows();
            let batch = std::mem::replace(&mut rb, new_builder(&logical)?);
            if tx.send(Ok(batch.finish()?)).await.is_err() {
                return Ok(());
            }
            rows_read.fetch_add(n, Ordering::Relaxed);
            batch_bytes = 0;
        }
    }

    if !rb.is_empty() {
        let n = rb.rows();
        let _ = tx.send(Ok(rb.finish()?)).await;
        rows_read.fetch_add(n, Ordering::Relaxed);
    }
    debug!(rows = row_no, "postgres 分片读完");
    Ok(())
}

fn decode_hex(s: &str) -> Result<Vec<u8>, D2DError> {
    if !s.len().is_multiple_of(2) {
        return Err(D2DError::schema(format!("bytea 十六进制串长度为奇数: {:?}", s)));
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    for pair in b.chunks_exact(2) {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| D2DError::schema(format!("bytea 十六进制串非法: {:?}", s)))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| D2DError::schema(format!("bytea 十六进制串非法: {:?}", s)))?;
        out.push((hi * 16 + lo) as u8);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{PartitionMode, PartitionSpec};
    use serde_json::json;

    fn spec() -> SourceSpec {
        SourceSpec {
            connector: "postgres".into(),
            connection: json!({"host": "192.0.2.13", "user": "postgres", "password": "p"}),
            method: None,
            properties: None,
            table: "t_a".into(),
            schema_name: Some("db".into()),
            query: None,
            type_contract: None,
            split: None,
            partition: None,
        }
    }

    #[test]
    fn test_name_and_database() {
        assert_eq!(PgSourceConnector::new().name(), "postgres");
        let mut s = spec();
        assert_eq!(PgSourceConnector::database_of(&s).unwrap(), "db");
        s.schema_name = None;
        s.connection = json!({"host": "h", "database": "db2"});
        assert_eq!(PgSourceConnector::database_of(&s).unwrap(), "db2");
    }

    #[test]
    fn test_decode_hex() {
        assert_eq!(decode_hex("0001ff").unwrap(), vec![0, 1, 255]);
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
        assert!(decode_hex("abc").is_err());
        assert!(decode_hex("zz").is_err());
    }

    #[tokio::test]
    async fn test_read_before_init_errors() {
        let c = PgSourceConnector::new();
        let s = SourceSplit { split_id: "x".into(), split_info: json!({"where_condition": ""}) };
        assert!(c.read(&s).await.is_err());
        assert!(c.schema().await.is_err());
    }

    #[tokio::test]
    async fn test_plan_splits() {
        let c = PgSourceConnector::new();
        let mut s = spec();
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("user_id".into()),
            count: 2,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert!(split_condition(&splits[0]).contains("md5"));
        s.partition = None;
        assert_eq!(c.plan_splits(&s).await.unwrap()[0].split_id, "t_a_single");
    }
}
