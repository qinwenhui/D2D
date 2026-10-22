//! MySQL **reader 方向**
//!
//! 走 MySQL 协议（`mysql_async` 的文本协议，`COM_QUERY`）。与 Doris Source 同形：
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`crate::sql`] | MySQL 方言：反引号、分区谓词、查询组装 |
//! | [`crate::schema`] | 权威 schema：`information_schema.COLUMNS` |
//! | [`crate::wire`] | 协议类型码 → 逻辑类型（**只给表达式别名**回退） |
//!
//! # 取数单元 = 一条连接
//!
//! 一次 [`SourceConnector::read`] 取一条连接、跑一句 SQL、读到 EOF 再归还。
//! 并发由内核的闸门定，连接器看不见那个数字。

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_connector::{
    MemMeter, QueryConfig, SourceConnector, SourceSpec, SourceSplit, UnitFootprint,
};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use d2d_types::text_batch::{RowBuildOptions, RowBuilder};
use d2d_types::LogicalType;
use mysql_async::prelude::*;
use mysql_async::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::config::MysqlConnection;
use crate::sql::MysqlSqlBuilder;

/// 一批交多少行（上界；还会被字节数提前截断）
const BATCH_ROWS: usize = 8192;
/// 一批的标称字节上界 —— 长文本列靠它兜住
const NOMINAL_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// 一条连接的标称读缓冲（启动期账本用；真实占用由 memory_meter 申报）
const NOMINAL_CONN_READ_BUFFER: usize = 4 * 1024 * 1024;
const CHANNEL_DEPTH: usize = 2;

#[derive(Debug, Clone)]
struct ResolvedQuery {
    database: String,
    table: String,
    query: QueryConfig,
}

/// MySQL Source 连接器
pub struct MysqlSourceConnector {
    name: String,
    pool: Option<mysql_async::Pool>,
    schema: Option<SchemaRef>,
    logical: Vec<LogicalType>,
    resolved: Option<ResolvedQuery>,
    rows_read: Arc<AtomicU64>,
    started: Arc<AtomicBool>,
    meter: Option<Arc<MemMeter>>,
}

impl MysqlSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "mysql".to_string(),
            pool: None,
            schema: None,
            logical: Vec::new(),
            resolved: None,
            rows_read: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            meter: None,
        }
    }

    /// 库名：优先 `source.schema_name`（引擎给"源的命名空间"留的通用位置），
    /// 兼容 `connection.database`
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
                D2DError::config("mysql source 需要 `source.schema_name`（库名，用于限定表名）")
            })
    }

    pub fn build_split_sql(&self, split: &SourceSplit) -> String {
        let Some(r) = &self.resolved else {
            return String::new();
        };
        MysqlSqlBuilder::new(Some(&r.database), &r.table)
            .build_query(&r.query, &split_condition(split))
    }

    async fn plan_splits(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        use d2d_connector::partition::{PartitionMode, ProbedBounds};

        let Some(p) = spec.effective_partition()? else {
            return Ok(vec![Self::single_split(&spec.table)]);
        };
        let probed = if p.mode == PartitionMode::Range && (p.lower.is_none() || p.upper.is_none()) {
            let col = p.column.clone().unwrap_or_default();
            let (lo, hi) = self.probe_min_max(spec, &col).await?;
            let cuts = d2d_connector::partition::range_cuts(&lo, &hi, p.count)?;
            info!(column = %col, cuts = cuts.len(), "mysql range 分片：MIN/MAX 探测完成");
            Some(ProbedBounds::CutPoints(cuts))
        } else {
            None
        };
        let plan =
            d2d_connector::partition::plan(&p, &spec.table, spec.schema_name.as_deref(), probed)?;
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
            split_info: serde_json::json!({
                "mode": "none", "index": 0, "where_condition": "",
            }),
        }
    }

    async fn probe_min_max(
        &self,
        spec: &SourceSpec,
        column: &str,
    ) -> D2DResult<(
        d2d_connector::partition::Literal,
        d2d_connector::partition::Literal,
    )> {
        use d2d_connector::partition::Literal;
        if column.trim().is_empty() {
            return Err(D2DError::config(
                "partition.mode=range 需要 column（或显式给 lower+upper 以跳过探测）",
            ));
        }
        let pool = self.require_pool()?;
        let database = Self::database_of(spec)?;
        let sql = MysqlSqlBuilder::new(Some(&database), &spec.table).build_min_max(column);
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| D2DError::external("mysql", format!("取连接失败: {}", e)))?;
        let rows: Vec<mysql_async::Row> = conn
            .query(&sql)
            .await
            .map_err(|e| D2DError::external("mysql", format!("MIN/MAX 探测失败: {}", e)))?;
        drop(conn);
        let cell = |r: &mysql_async::Row, i: usize| match r.as_ref(i) {
            Some(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Some(Value::NULL) | None => None,
            Some(v) => Some(format!("{:?}", v)),
        };
        let (min_s, max_s) = rows
            .first()
            .map(|r| (cell(r, 0), cell(r, 1)))
            .unwrap_or((None, None));
        let (Some(min_s), Some(max_s)) = (min_s, max_s) else {
            return Err(D2DError::external(
                "mysql",
                format!(
                    "range 分片探测 MIN/MAX 返回空值（列 {} 全为 NULL 或表为空）—— \
                     请改用 partition.mode=list / none",
                    column
                ),
            ));
        };
        Ok((Literal::parse(&min_s), Literal::parse(&max_s)))
    }

    fn require_pool(&self) -> D2DResult<mysql_async::Pool> {
        self.pool
            .clone()
            .ok_or_else(|| D2DError::internal("mysql source 未初始化：先调 init()"))
    }
}

/// 从分片里取分区条件
pub fn split_condition(split: &SourceSplit) -> String {
    split
        .split_info
        .get("where_condition")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

#[async_trait]
impl SourceConnector for MysqlSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        let database = Self::database_of(spec)?;
        let connection = MysqlConnection::from_json(&spec.connection)?;
        info!(
            connector = %self.name, table = %spec.table, database = %database,
            host = %connection.host, port = connection.port, "initializing mysql source"
        );
        if let Some(p) = spec.effective_partition()? {
            p.validate()?;
        }
        let pool = connection.pool(Some(&database));

        // 开工前自检：连库 + 把 SQL 真跑一遍（表名/列名/表达式错了当场失败）
        let mut control = pool
            .get_conn()
            .await
            .map_err(|e| D2DError::external("mysql", format!("连不上 MySQL: {}", e)))?;

        let builder = MysqlSqlBuilder::new(Some(&database), &spec.table);
        let query = spec.query.clone().unwrap_or_default();
        let query = match spec
            .effective_partition()?
            .and_then(|p| p.column.filter(|c| !c.trim().is_empty()))
        {
            Some(col) => builder.ensure_partition_column(&query, &col)?,
            None => query,
        };
        let resolved = crate::schema::resolve_schema(&mut control, &database, spec, &query).await?;
        drop(control);

        self.pool = Some(pool);
        self.schema = Some(Arc::clone(&resolved.schema));
        self.logical = resolved.logical;
        self.resolved = Some(ResolvedQuery {
            database,
            table: spec.table.clone(),
            query,
        });
        self.meter = Some(MemMeter::new(
            "mysql 读缓冲",
            format!(
                "每条连接约 {} B 的协议读缓冲（按需增长，上限 {} B）",
                NOMINAL_CONN_READ_BUFFER,
                crate::config::MAX_ALLOWED_PACKET
            ),
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
        info!(connector = %self.name, table = %spec.table, splits = splits.len(), "mysql split generated");
        Ok(splits)
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        let pool = self.require_pool()?;
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| D2DError::internal("mysql source 未初始化：先调 init()"))?;
        let logical = self.logical.clone();
        let sql = self.build_split_sql(split);
        if sql.is_empty() {
            return Err(D2DError::internal("mysql source 未初始化：先调 init()"));
        }
        let rows_read = Arc::clone(&self.rows_read);
        let started = Arc::clone(&self.started);
        let meter = self.meter.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<arrow::record_batch::RecordBatch>>(
            CHANNEL_DEPTH,
        );
        tokio::spawn(async move {
            let res =
                stream_split(pool, sql, schema, logical, &tx, &rows_read, &started, meter).await;
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

    /// 源端实际交出去的行数（本连接器解码时自己数的）—— 与目标端确认数对账
    fn exported_rows(&self) -> Option<u64> {
        if self.started.load(Ordering::Relaxed) {
            Some(self.rows_read.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing mysql source");
        if let Some(pool) = self.pool.take() {
            pool.disconnect()
                .await
                .map_err(|e| D2DError::external("mysql", format!("关闭连接池失败: {}", e)))?;
        }
        self.schema = None;
        self.logical.clear();
        self.resolved = None;
        self.meter = None;
        Ok(())
    }
}

impl Default for MysqlSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::too_many_arguments)]
async fn stream_split(
    pool: mysql_async::Pool,
    sql: String,
    schema: SchemaRef,
    logical: Vec<LogicalType>,
    tx: &tokio::sync::mpsc::Sender<D2DResult<arrow::record_batch::RecordBatch>>,
    rows_read: &AtomicU64,
    started: &AtomicBool,
    meter: Option<Arc<MemMeter>>,
) -> D2DResult<()> {
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| D2DError::external("mysql", format!("取连接失败: {}", e)))?;
    let _lease = meter.as_ref().map(|m| m.claim(NOMINAL_CONN_READ_BUFFER));

    let ncols = schema.fields().len();
    let mut qr = conn
        .query_iter(&sql)
        .await
        .map_err(|e| D2DError::external("mysql", format!("查询失败（{}）: {}", sql, e)))?;
    if qr.columns_ref().len() != ncols {
        return Err(D2DError::schema(format!(
            "结果集列数 {} 与 schema 列数 {} 不一致 —— 查询与契约对不上了",
            qr.columns_ref().len(),
            ncols
        )));
    }
    started.store(true, Ordering::Relaxed);

    let new_builder = |logical: &[LogicalType]| -> D2DResult<RowBuilder> {
        RowBuilder::new(
            Arc::clone(&schema),
            logical.to_vec(),
            // MySQL：'' 是**真值**，只有 NULL 才是 NULL（与达梦/Oracle 相反）
            RowBuildOptions::mysql_protocol(),
            BATCH_ROWS,
        )
    };
    let mut rb = new_builder(&logical)?;
    let mut batch_bytes = 0usize;
    let mut row_no: u64 = 0;

    while let Some(row) = qr
        .next()
        .await
        .map_err(|e| D2DError::external("mysql", format!("取行失败: {}", e)))?
    {
        // 文本协议下所有值都是 Bytes，NULL 是 Value::NULL
        let mut cells: Vec<Option<&[u8]>> = Vec::with_capacity(ncols);
        for i in 0..ncols {
            match row.as_ref(i) {
                None | Some(Value::NULL) => cells.push(None),
                Some(Value::Bytes(b)) => {
                    batch_bytes += b.len();
                    cells.push(Some(&b[..]));
                }
                Some(other) => {
                    return Err(D2DError::external(
                        "mysql",
                        format!(
                            "第 {} 行列 {} 的返回值不是文本协议的字节串（{:?}）—— \
                             本连接器按 COM_QUERY 文本协议读，出现别的形态说明假设不成立",
                            row_no + 1,
                            schema.field(i).name(),
                            other
                        ),
                    ))
                }
            }
        }
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
    debug!(rows = row_no, "mysql 分片读完");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{PartitionMode, PartitionSpec};
    use serde_json::json;

    fn spec() -> SourceSpec {
        SourceSpec {
            connector: "mysql".into(),
            connection: json!({"host": "192.0.2.10", "user": "root", "password": "p"}),
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
        assert_eq!(MysqlSourceConnector::new().name(), "mysql");
        let mut s = spec();
        assert_eq!(MysqlSourceConnector::database_of(&s).unwrap(), "db");
        s.schema_name = None;
        s.connection = json!({"host": "h", "database": "db2"});
        assert_eq!(MysqlSourceConnector::database_of(&s).unwrap(), "db2");
        s.connection = json!({"host": "h"});
        assert!(MysqlSourceConnector::database_of(&s).is_err());
    }

    #[tokio::test]
    async fn test_read_before_init_errors() {
        let c = MysqlSourceConnector::new();
        let s = SourceSplit {
            split_id: "x".into(),
            split_info: json!({"where_condition": ""}),
        };
        assert!(c.read(&s).await.is_err());
        assert!(c.schema().await.is_err());
    }

    #[test]
    fn test_unit_footprint_and_exported_rows() {
        let c = MysqlSourceConnector::new();
        let f = c.unit_footprint(&spec());
        assert_eq!(f.kind, d2d_connector::UnitKind::Connection);
        assert_eq!(f.child_bytes_each, 0);
        assert_eq!(c.exported_rows(), None);
        c.rows_read.store(7, Ordering::Relaxed);
        c.started.store(true, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), Some(7));
    }

    #[tokio::test]
    async fn test_plan_splits() {
        let c = MysqlSourceConnector::new();
        let mut s = spec();
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["`id` < 10".into(), "`id` >= 10".into()],
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert_eq!(split_condition(&splits[0]), "`id` < 10");

        s.partition = Some(PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("user_id".into()),
            count: 4,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(
            split_condition(&splits[3]),
            "MOD(CRC32(COALESCE(CAST(`user_id` AS CHAR), '')), 4) = 3"
        );

        s.partition = None;
        assert_eq!(c.plan_splits(&s).await.unwrap()[0].split_id, "t_a_single");
    }
}
