//! qmdb **reader 方向**：Flight SQL 流式查询
//!
//! 走标准 Flight SQL 面：`GetFlightInfo(CommandStatementQuery)` 拿执行票据，
//! `DoGet(ticket)` 逐批收 Arrow `RecordBatch` —— **列式直传，没有行解析、
//! 没有类型重建**（与 doris 的 flight 读路径同一量级的开销模型）。
//! SQL 由本连接器拼：`SELECT <columns> FROM db.t [WHERE …] [ORDER BY …] [LIMIT n]`。
//!
//! # 取数单元 = 一次查询
//!
//! 分片切法走内核 [`d2d_connector::partition`] 那套（list / range / none；
//! hash 被 qmdb 方言拒绝，见 `sql` 模块文档）。每个分片一次
//! `GetFlightInfo + DoGet`；qmdb 的快照一致性由 ticket 内的 sn 保证
//! （同 ticket 重复 DoGet 读到同一数据视图），并发分片读各自独立。
//!
//! range 分片缺边界时做一次 `MIN/MAX` 探测 —— 聚合在服务端做，配合分区
//! 裁剪是一次便宜的往返（与 doris flight 的"必须显式给边界"不同：
//! qmdb 的聚合点查就是为这种用法设计的）。
//!
//! # 开工前自检
//!
//! init 用 `SELECT * FROM t LIMIT 0` 拿权威 schema：表名拼错、权限不足
//! 都在这里大声失败，而不是跑完 0 行、退出码 0、日志一路 INFO。

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_connector::{QueryConfig, SourceConnector, SourceSpec, SourceSplit, UnitFootprint};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use crate::config::QmdbConnection;
use crate::flight::{QmdbFlight, NOMINAL_FLIGHT_BUFFER};
use crate::sql::QmdbSqlBuilder;

/// 一批交多少行（**上界**，来自服务端，实际由服务端分批决定）
const NOMINAL_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// 通道深度：**有界**，这是进程内常驻有上界的原因
const CHANNEL_DEPTH: usize = 2;

/// init 后固化的查询上下文
#[derive(Clone)]
struct ResolvedQuery {
    database: Option<String>,
    table: String,
    /// 用户查询配置（已自动补上分区列）
    query: QueryConfig,
}

pub struct QmdbSourceConnector {
    name: String,
    flight: Option<QmdbFlight>,
    /// LIMIT 0 探出来的权威 schema
    schema: Option<SchemaRef>,
    resolved: Option<ResolvedQuery>,
    /// 本次运行**源端实际交出去的行数**（见 [`Self::exported_rows`]）
    rows_read: Arc<AtomicU64>,
    /// 是否真的开过工 —— 用来区分"读出 0 行"与"根本没读"
    started: Arc<AtomicBool>,
}

impl QmdbSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "qmdb".to_string(),
            flight: None,
            schema: None,
            resolved: None,
            rows_read: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 解析配置里的库名
    ///
    /// 优先 `source.schema_name`（引擎给"命名空间"留的通用位置）；
    /// `connection.database` 作为兼容写法也收。qmdb 是多库的，**表名一律
    /// 全限定**（qmdb 文档对连接池/同步工具的建议），两者都没有就报错。
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
                D2DError::config(
                    "qmdb source 需要 `source.schema_name`（qmdb 的库名，表名一律全限定 db.table）",
                )
            })
    }

    /// 生成某个分片的完整查询 SQL（测试与调试也用）
    pub fn build_split_sql(&self, split: &SourceSplit) -> D2DResult<String> {
        let Some(r) = &self.resolved else {
            return Err(D2DError::internal("qmdb source 未初始化：先调 init()"));
        };
        let db = r.database.as_deref();
        QmdbSqlBuilder::new(db, &r.table).build_query(&r.query, &split_condition(split))
    }

    /// 规划分片 —— 切法走**内核 `partition` 那套**，与 doris 两侧同源
    ///
    /// range 且用户没给边界时探一次 MIN/MAX（聚合在服务端做，一次往返）。
    async fn plan_splits(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        use d2d_connector::partition::{PartitionMode, ProbedBounds};

        let Some(p) = spec.effective_partition()? else {
            return Ok(vec![Self::single_split(&spec.table)]);
        };

        // 只有 range 缺边界才需要连库探测；list / none / 已给边界的 range 零探测
        let probed = if p.mode == PartitionMode::Range && (p.lower.is_none() || p.upper.is_none()) {
            let flight = self
                .flight
                .clone()
                .ok_or_else(|| D2DError::internal("qmdb source 未初始化：先调 init()"))?;
            let qualified = self.qualified_table()?;
            let col = p.column.clone().unwrap_or_default();
            let (lo, hi) = flight.probe_min_max(&qualified, &col).await?;
            let cuts = d2d_connector::partition::range_cuts(&lo, &hi, p.count)?;
            info!(column = %col, cuts = cuts.len(), "qmdb range 分片：MIN/MAX 探测完成");
            Some(ProbedBounds::CutPoints(cuts))
        } else {
            None
        };

        let plan =
            d2d_connector::partition::plan(&p, &spec.table, spec.schema_name.as_deref(), probed)?;

        let mut splits = Vec::with_capacity(plan.partitions.len());
        for (i, part) in plan.partitions.iter().enumerate() {
            // hash 在 qmdb 方言里没有正确实现的路 —— 在这里（规划期）报出来，
            // 不留到读取期才炸
            let cond = crate::sql::render_partition_predicate(&part.predicate)?;
            splits.push(SourceSplit {
                split_id: format!("{}_{}", spec.table, part.id),
                split_info: serde_json::json!({
                    "mode": p.mode.as_str(),
                    "index": i,
                    "describe": part.describe,
                    "where_condition": cond,
                }),
            });
        }
        Ok(splits)
    }

    fn qualified_table(&self) -> D2DResult<String> {
        let r = self
            .resolved
            .as_ref()
            .ok_or_else(|| D2DError::internal("qmdb source 未初始化：先调 init()"))?;
        QmdbSqlBuilder::new(r.database.as_deref(), &r.table).qualified_table()
    }

    fn single_split(table: &str) -> SourceSplit {
        SourceSplit {
            split_id: format!("{table}_single"),
            split_info: serde_json::json!({
                "mode": "none",
                "index": 0,
                "where_condition": "",
            }),
        }
    }
}

/// 从分片里取出分区条件（`WHERE` 片段，不含 `WHERE`）
pub fn split_condition(split: &SourceSplit) -> String {
    split
        .split_info
        .get("where_condition")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

#[async_trait]
impl SourceConnector for QmdbSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        QmdbConnection::validate_method(spec.method.as_deref())?;
        let connection = QmdbConnection::from_json(&spec.connection)?;
        let database = Self::database_of(spec)?;
        info!(
            connector = %self.name,
            table = %spec.table,
            database = %database,
            addr = %connection.addr,
            "initializing qmdb source"
        );
        // 分片配置在这里就校验（拼错/漏配 fail-fast，不留到 split 才发现）
        if let Some(p) = spec.effective_partition()? {
            p.validate()?;
        }

        // 分区列必须出现在 SELECT 列表里（否则切不了范围）；引擎侧的 Project 会剔回来
        let builder = QmdbSqlBuilder::new(Some(&database), &spec.table);
        let query = spec.query.clone().unwrap_or_default();
        let query = match spec
            .effective_partition()?
            .and_then(|p| p.column.filter(|c| !c.trim().is_empty()))
        {
            Some(col) => builder.ensure_partition_column(&query, &col)?,
            None => query,
        };

        // 连接 + LIMIT 0 自检：表名拼错/权限不足在这里大声失败
        let flight = QmdbFlight::connect(&connection).await?;
        let qualified = builder.qualified_table()?;
        let schema = flight.table_schema(&qualified).await?;
        info!(
            connector = %self.name,
            table = %qualified,
            columns = schema.fields().len(),
            "qmdb flight source 已连接（Arrow Flight SQL）"
        );

        self.flight = Some(flight);
        self.schema = Some(schema);
        self.resolved = Some(ResolvedQuery {
            database: Some(database),
            table: spec.table.clone(),
            query,
        });
        Ok(())
    }

    async fn schema(&self) -> D2DResult<SchemaRef> {
        self.schema
            .clone()
            .ok_or_else(|| D2DError::schema("schema not fetched, call init() first"))
    }

    async fn split(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        let splits = self.plan_splits(spec).await?;
        info!(
            connector = %self.name,
            table = %spec.table,
            splits = splits.len(),
            "qmdb split generated"
        );
        Ok(splits)
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        let flight = self
            .flight
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb source 未初始化：先调 init()"))?;
        let sql = self.build_split_sql(split)?;
        let rows_read = Arc::clone(&self.rows_read);
        let started = Arc::clone(&self.started);
        let split_id = split.split_id.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<arrow::record_batch::RecordBatch>>(
            CHANNEL_DEPTH,
        );

        // 读动作整个住在这一次调用里：一次 GetFlightInfo + DoGet，读到 EOF。
        // 通道有界，下游不取就自然背压到 gRPC 流控上。
        tokio::spawn(async move {
            match flight.stream_query(&sql).await {
                Ok(mut stream) => {
                    started.store(true, Ordering::Relaxed);
                    while let Some(item) = stream.next().await {
                        match item {
                            Ok(batch) => {
                                let n = batch.num_rows();
                                // 行数在**送出之后**才计：送不出去的那批不算"交出去了"
                                if tx.send(Ok(batch)).await.is_err() {
                                    return; // 下游已断开
                                }
                                rows_read.fetch_add(n as u64, Ordering::Relaxed);
                            }
                            Err(e) => {
                                // 错误也要送达：直接 return 会让下游把"断了"当成"读完了"
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        }
                    }
                    debug!(split = %split_id, "qmdb 分片读完");
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    /// 一次 `read` = **一条 gRPC 流**（取数单元的形态）
    fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
        UnitFootprint::connection(NOMINAL_FLIGHT_BUFFER + NOMINAL_BATCH_BYTES)
    }

    /// **源端实际交出去的行数**（与目标端确认载入数是对账的两端）
    fn exported_rows(&self) -> Option<u64> {
        if self.started.load(Ordering::Relaxed) {
            Some(self.rows_read.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing qmdb source");
        self.flight = None;
        self.schema = None;
        self.resolved = None;
        Ok(())
    }
}

impl Default for QmdbSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::PartitionMode;
    use serde_json::json;

    fn spec() -> SourceSpec {
        SourceSpec {
            connector: "qmdb".into(),
            connection: json!({"addr": "192.0.2.13:31337", "user": "root", "password": "x"}),
            method: None,
            properties: None,
            table: "t_a".into(),
            schema_name: Some("bench_lc".into()),
            query: None,
            type_contract: None,
            split: None,
            partition: None,
        }
    }

    #[test]
    fn test_name_is_qmdb() {
        // 与 register() 里登记的名字、TypeAdapter 的 db_name 必须逐字一致
        assert_eq!(QmdbSourceConnector::new().name(), "qmdb");
    }

    #[test]
    fn test_database_from_schema_name_then_connection() {
        let mut s = spec();
        assert_eq!(QmdbSourceConnector::database_of(&s).unwrap(), "bench_lc");
        // connection.database 作为兼容写法
        s.schema_name = None;
        s.connection = json!({"addr": "h:31337", "database": "db2", "user": "root"});
        assert_eq!(QmdbSourceConnector::database_of(&s).unwrap(), "db2");
        // 都没有 → 明确报错，不猜
        s.connection = json!({"addr": "h:31337", "user": "root"});
        let e = QmdbSourceConnector::database_of(&s)
            .unwrap_err()
            .to_string();
        assert!(e.contains("schema_name"), "{}", e);
        // 空串不当成"给了"
        s.schema_name = Some("   ".into());
        assert!(QmdbSourceConnector::database_of(&s).is_err());
    }

    /// 未初始化时 `read` 必须报错，不能悄悄返回空流（那会"迁 0 行也报成功"）
    #[tokio::test]
    async fn test_read_before_init_errors() {
        let c = QmdbSourceConnector::new();
        let s = SourceSplit {
            split_id: "x".into(),
            split_info: json!({"where_condition": ""}),
        };
        assert!(c.read(&s).await.is_err());
        assert!(c.schema().await.is_err());
    }

    /// 没开过工 → `None`（引擎如实跳过对账，而不是假装对过）
    #[test]
    fn test_exported_rows_is_none_before_any_read() {
        assert_eq!(QmdbSourceConnector::new().exported_rows(), None);
    }

    /// 开过工就是"源端交出去多少行" —— 与目标端确认载入数是对账的两端
    #[test]
    fn test_exported_rows_reports_counted_rows_once_started() {
        let c = QmdbSourceConnector::new();
        c.rows_read.store(1234, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), None, "还没开工，不能报数");
        c.started.store(true, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), Some(1234));
        // 真的读出 0 行（空表）是有效事实，不能吞成"没提供"
        c.rows_read.store(0, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), Some(0));
    }

    /// 取数单元的形态：一条连接（个数归内核闸门管）
    #[test]
    fn test_unit_footprint_is_one_connection() {
        let f = QmdbSourceConnector::new().unit_footprint(&spec());
        assert_eq!(f.kind, d2d_connector::UnitKind::Connection);
        assert!(f.bytes_each >= NOMINAL_BATCH_BYTES);
        assert_eq!(f.child_bytes_each, 0);
    }

    #[test]
    fn test_split_condition_extraction() {
        let s = SourceSplit {
            split_id: "x".into(),
            split_info: json!({"where_condition": "phid < 10"}),
        };
        assert_eq!(split_condition(&s), "phid < 10");
        let s2 = SourceSplit {
            split_id: "y".into(),
            split_info: json!({}),
        };
        assert_eq!(split_condition(&s2), "");
    }

    /// 分片规划走内核那套：list/none 直接可用，hash 在规划期就报错
    #[tokio::test]
    async fn test_plan_splits_uses_kernel_partition() {
        let mut c = QmdbSourceConnector::new();
        let mut s = spec();
        c.resolved = Some(ResolvedQuery {
            database: Some("bench_lc".into()),
            table: "t_a".into(),
            query: QueryConfig::default(),
        });

        // list
        s.partition = Some(d2d_connector::PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["phid < 10".into(), "phid >= 10".into()],
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert_eq!(split_condition(&splits[0]), "phid < 10");

        // hash → 规划期拒绝（qmdb 方言没有稳定的行哈希函数）
        s.partition = Some(d2d_connector::PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("user_id".into()),
            count: 4,
            ..Default::default()
        });
        let e = c.plan_splits(&s).await.unwrap_err().to_string();
        assert!(e.contains("hash"), "{}", e);

        // none：单片、无条件
        s.partition = Some(d2d_connector::PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(split_condition(&splits[0]), "");
        assert_eq!(splits[0].split_id, "t_a_p000");

        // 压根没配 partition：退化成单分片
        s.partition = None;
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id, "t_a_single");
    }

    /// range + 显式边界：零探测直接切
    #[tokio::test]
    async fn test_plan_splits_range_with_explicit_bounds() {
        let mut c = QmdbSourceConnector::new();
        let mut s = spec();
        c.resolved = Some(ResolvedQuery {
            database: Some("bench_lc".into()),
            table: "t_a".into(),
            query: QueryConfig::default(),
        });
        s.partition = Some(d2d_connector::PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("pdate".into()),
            lower: Some(d2d_connector::partition::Literal::parse("2026-10-01")),
            upper: Some(d2d_connector::partition::Literal::parse("2026-10-05")),
            count: 2,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert!(split_condition(&splits[0]).contains("pdate >="));
        assert!(split_condition(&splits[1]).contains("pdate <"));
    }
}
