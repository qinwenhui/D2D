//! Doris **reader 方向**：`DorisSourceConnector` 及它用到的一切
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`sql`] | Doris SQL 方言：反引号标识符、分区谓词渲染、查询组装 |
//! | [`schema`] | 权威 schema 从哪来：`DESCRIBE` + 结果集元数据 |
//! | [`wire`] | MySQL 协议类型码 → 逻辑类型（**只对表达式别名**的回退） |
//! | [`conn`] | FE 的 MySQL 协议口：建连选项与连接池 |
//!
//! 写方向（X → Doris）不在这里，见 crate 根文档「骨架」一节。
//!
//! # 只有一种读取方式，所以没有 `method/` 那一层
//!
//! 达梦有 `odbc` 与 `dmfldr` 两条路，于是有 `method/` 做路由。Doris 读只有一个口
//! —— FE 的 MySQL 协议 —— 硬造一层路由只会多一层"为什么这里有个 trait"。
//! 真出现第二条读法（比如走 BE 的 Arrow Flight）时再加。

pub mod conn;
pub mod schema;
pub mod sql;
pub mod wire;

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

use crate::config::DorisConnection;
use crate::source::conn::Reader;
use crate::source::sql::DorisSqlBuilder;

/// 一批交多少行（**上界**，实际还会被字节数提前截断）
///
/// 37 列宽表实测 422 字节/行，8192 行 ≈ 3.5 MB，与内核
/// `flow::NOMINAL_BATCH_BYTES` 同一个量级口径。
const BATCH_ROWS: usize = 8192;

/// 一批的标称字节上界 —— 行虽小但列是长文本时，光靠行数会把批撑爆
///
/// 与达梦侧 `NOMINAL_PARSE_BATCH_BYTES` 取同一个 4 MB：两处估的是同一件事
/// （"源端单批的典型大小"），口径不一致会让内存账在两台之间对不上。
const NOMINAL_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// 一条连接的标称读缓冲（启动期内存账用）
///
/// 这是**估计**不是实测：mysql_async 按需增长，不预分配，上限是
/// [`conn::MAX_ALLOWED_PACKET`]。这里取"单包典型大小"量级，够用来回答
/// "这份配置装不装得下"。真实占用由 [`Self::memory_meter`] 在运行期回答。
const NOMINAL_CONN_READ_BUFFER: usize = 4 * 1024 * 1024;

/// 通道深度：**有界**，这是进程内常驻有上界的原因
const CHANNEL_DEPTH: usize = 2;

/// init 后固化的查询上下文
#[derive(Debug, Clone)]
struct ResolvedQuery {
    database: String,
    table: String,
    /// 用户查询配置（已自动补上分区列）
    query: QueryConfig,
}

/// Doris Source 连接器 —— 经 FE 的 MySQL 协议口读
pub struct DorisSourceConnector {
    name: String,
    reader: Option<Arc<Reader>>,
    /// 契约推导的权威 schema
    schema: Option<SchemaRef>,
    /// 与 schema 逐列对齐的逻辑类型（行构建器的解析策略）
    logical: Vec<LogicalType>,
    resolved: Option<ResolvedQuery>,
    /// 本次运行**源端实际交出去的行数**（见 [`Self::exported_rows`]）
    rows_read: Arc<AtomicU64>,
    /// 是否真的开过工 —— 用来区分"读出 0 行"与"根本没读"
    started: Arc<AtomicBool>,
    /// 运行期占用申报尺子
    meter: Option<Arc<MemMeter>>,
}

impl DorisSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "doris".to_string(),
            reader: None,
            schema: None,
            logical: Vec::new(),
            resolved: None,
            rows_read: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicBool::new(false)),
            meter: None,
        }
    }

    /// 解析配置里的库名
    ///
    /// 优先 `source.schema_name` —— 那是引擎给"这个源的命名空间叫什么"留的通用位置
    /// （达梦用它装 schema）。Doris 没有 schema 这一层，database 就是命名空间，
    /// 落在同一个位置对用户最省心。`connection.database` 作为兼容写法也收。
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
                    "doris source 需要 `source.schema_name`（Doris 的库名，用于限定表名）",
                )
            })
    }

    /// 生成某个分片的完整查询 SQL
    pub fn build_split_sql(&self, split: &SourceSplit) -> String {
        let Some(r) = &self.resolved else {
            return String::new();
        };
        DorisSqlBuilder::new(Some(&r.database), &r.table)
            .build_query(&r.query, &split_condition(split))
    }

    /// 规划分片 —— 切法走**内核 `partition` 那套**，与达梦两侧同源
    ///
    /// 连接器只负责回答"边界在哪"（range 且用户没给边界时探一次 MIN/MAX），
    /// 以及把内核的结构化谓词渲染成 Doris 方言。
    async fn plan_splits(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        use d2d_connector::partition::{PartitionMode, ProbedBounds};

        let Some(p) = spec.effective_partition()? else {
            return Ok(vec![Self::single_split(&spec.table)]);
        };

        let probed = if p.mode == PartitionMode::Range && (p.lower.is_none() || p.upper.is_none()) {
            let col = p.column.clone().unwrap_or_default();
            let (lo, hi) = self.probe_min_max(spec, &col).await?;
            let cuts = d2d_connector::partition::range_cuts(&lo, &hi, p.count)?;
            info!(column = %col, cuts = cuts.len(), "doris range 分片：MIN/MAX 探测完成");
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
                    "where_condition": sql::render_partition_predicate(&part.predicate),
                }),
            })
            .collect())
    }

    fn single_split(table: &str) -> SourceSplit {
        SourceSplit {
            split_id: format!("{}_single", table),
            split_info: serde_json::json!({
                "mode": "none",
                "index": 0,
                "where_condition": "",
            }),
        }
    }

    /// 探 MIN/MAX（range 分片缺边界时用）
    ///
    /// 走的是同一个 MySQL 协议口，一次往返。代价与表大小相关，所以只在
    /// "用户没给边界"时才做 —— 给了 `lower`/`upper` 就是零探测（内核的契约）。
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
        let reader = self.require_reader()?;
        let database = Self::database_of(spec)?;
        let sql = DorisSqlBuilder::new(Some(&database), &spec.table).build_min_max(column);

        let mut conn = reader
            .get_conn()
            .await
            .map_err(|e| D2DError::external("doris", format!("取连接失败: {}", e)))?;
        let mut qr = conn.query_iter(&sql).await.map_err(|e| {
            D2DError::external("doris", format!("MIN/MAX 探测失败（{}）: {}", sql, e))
        })?;
        let row: Option<mysql_async::Row> = qr
            .next()
            .await
            .map_err(|e| D2DError::external("doris", format!("MIN/MAX 取行失败: {}", e)))?;
        drop(qr);
        drop(conn);

        let cell = |r: &mysql_async::Row, i: usize| -> Option<String> {
            match r.as_ref(i) {
                Some(Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            }
        };
        let (Some(min_s), Some(max_s)) = (
            row.as_ref().and_then(|r| cell(r, 0)),
            row.as_ref().and_then(|r| cell(r, 1)),
        ) else {
            return Err(D2DError::external(
                "doris",
                format!(
                    "range 分片探测 MIN/MAX 返回空值（列 {} 全为 NULL 或表为空）—— \
                     请改用 partition.mode=list / none",
                    column
                ),
            ));
        };
        Ok((Literal::parse(&min_s), Literal::parse(&max_s)))
    }

    fn require_reader(&self) -> D2DResult<Arc<Reader>> {
        self.reader
            .clone()
            .ok_or_else(|| D2DError::internal("doris source 未初始化：先调 init()"))
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
impl SourceConnector for DorisSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        let database = Self::database_of(spec)?;
        let connection = DorisConnection::from_json(&spec.connection, &database, &spec.table)?;
        info!(
            connector = %self.name,
            table = %spec.table,
            database = %database,
            query_port = connection.query_port,
            "initializing doris source"
        );
        // 分片配置在这里就校验（拼错/漏配 fail-fast，不留到 split 才发现）
        if let Some(p) = spec.effective_partition()? {
            p.validate()?;
        }

        let reader = Arc::new(Reader::new(&connection)?);

        // ── 开工前自检：连库 + 把 SQL 真的跑一遍 ──
        //
        // 这是"迁 0 行也报成功"那道口子的封堵点：表名拼错、列名不存在、表达式写错，
        // 全都在这里大声失败，而不是跑完 0 行、退出码 0、日志一路 INFO。
        // `LIMIT 0` 实测在 1915 万行的表上是 2 ms（FE 只规划不取数），所以这顿自检免费。
        let mut control = reader
            .get_conn()
            .await
            .map_err(|e| D2DError::external("doris", format!("连不上 Doris FE: {}", e)))?;

        // 分区列必须出现在 SELECT 列表里（否则切不了范围）；引擎侧的 Project 会剔回来
        let builder = DorisSqlBuilder::new(Some(&database), &spec.table);
        let query = spec.query.clone().unwrap_or_default();
        let query = match spec
            .effective_partition()?
            .and_then(|p| p.column.filter(|c| !c.trim().is_empty()))
        {
            Some(col) => builder.ensure_partition_column(&query, &col)?,
            None => query,
        };

        let resolved_schema = schema::resolve_schema(&mut control, &database, spec, &query).await?;
        drop(control);

        // ── 可空分区列 + range：NULL 行会被**静默漏掉** ──
        //
        // range 的谓词是 `col >= lo AND col < hi`，NULL 行对它求值为 NULL，
        // 于是哪个区间都不进。实测（`d2d_type_probe`：3 行，`c_dt` 可空，
        // range 分 4 片）只迁出去 **2 行**，退出码 0、行数对账还"通过"——
        // 因为两端数的都是那 2 行。
        //
        // hash 模式没有这个问题：它渲染的是 `COALESCE(..., '')`，NULL 落 0 号桶。
        // 所以这里只提醒，不改内核语义 —— `Range` 谓词长什么样是内核定的，
        // 连接器私自给它加 `OR col IS NULL` 会让同一个谓词在达梦那边是另一个意思。
        // 提醒而不是报错：DESCRIBE 说"可空"不等于真有 NULL，有 NULL 也不等于
        // 用户没打算把它们排除在外（边界是用户自己声明要取的范围）。
        if let Some(p) = spec.effective_partition()? {
            if p.mode == d2d_connector::PartitionMode::Range {
                if let Some(col) = p.column.as_deref() {
                    let nullable = resolved_schema
                        .schema
                        .fields()
                        .iter()
                        .find(|f| f.name().eq_ignore_ascii_case(col))
                        .map(|f| f.is_nullable());
                    if nullable == Some(true) {
                        tracing::warn!(
                            column = %col,
                            "range 分片的列在表上可空：NULL 行不满足任何区间谓词，会被**静默漏掉**\
                             （两端行数还会一致）。要么改用 partition.mode=hash（NULL 落 0 号桶），\
                             要么在 query.where_clause 里显式把 NULL 排除或纳入"
                        );
                    }
                }
            }
        }

        self.reader = Some(reader);
        self.schema = Some(Arc::clone(&resolved_schema.schema));
        self.logical = resolved_schema.logical;
        self.resolved = Some(ResolvedQuery {
            database,
            table: spec.table.clone(),
            query,
        });
        // 申报尺子：名字与说明由**本连接器**给（引擎不解释字节是什么）
        self.meter = Some(MemMeter::new(
            "doris/mysql 读缓冲",
            format!(
                "每条连接约 {} B 的协议读缓冲（按需增长，上限 {} B）",
                NOMINAL_CONN_READ_BUFFER,
                conn::MAX_ALLOWED_PACKET
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
        info!(connector = %self.name, table = %spec.table, splits = splits.len(), "doris split generated");
        Ok(splits)
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        let reader = self.require_reader()?;
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| D2DError::internal("doris source 未初始化：先调 init()"))?;
        let logical = self.logical.clone();
        let sql = self.build_split_sql(split);
        if sql.is_empty() {
            return Err(D2DError::internal("doris source 未初始化：先调 init()"));
        }

        let rows_read = Arc::clone(&self.rows_read);
        let started = Arc::clone(&self.started);
        let meter = self.meter.clone();
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<arrow::record_batch::RecordBatch>>(
            CHANNEL_DEPTH,
        );

        // 读动作整个住在这一次调用里：一条连接、一句 SQL、读到 EOF。
        // 通道是有界的，下游不取就自然背压到 MySQL 连接上（协议本身是拉式的）。
        tokio::spawn(async move {
            let res = stream_split(
                reader, sql, schema, logical, &tx, &rows_read, &started, meter,
            )
            .await;
            if let Err(e) = res {
                // 错误也要**送达**：直接 return 会让下游把"断了"当成"读完了"
                let _ = tx.send(Err(e)).await;
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    /// 一次 `read` = **一条 MySQL 连接**（取数单元的形态）
    fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
        UnitFootprint::connection(NOMINAL_CONN_READ_BUFFER + NOMINAL_BATCH_BYTES)
    }

    /// 运行期申报：协议读缓冲是**本连接器自己的**，不在任何池的账上
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    /// **源端实际交出去的行数**
    ///
    /// 这是本连接器在解码时自己数出来的，与 `write_batch` 的返回值是**对账的两端**：
    /// 它能发现"读出来了但没写进去"（转换/pipeline/目标端丢的）。
    ///
    /// ⚠️ 它**证明不了**"SQL 返回的行就是全部该返回的行"。分区条件写错导致少取数，
    /// 这个数会跟着一起少，两端照样相等。那类问题的防线在别处：分片覆盖性
    /// （[`d2d_connector::partition`] 的结构性保证）与逐列指纹对账。
    /// 与 dmfldr 的导出日志计数是**同一层意思**的东西，不要当成更强的保证。
    fn exported_rows(&self) -> Option<u64> {
        if self.started.load(Ordering::Relaxed) {
            Some(self.rows_read.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing doris source");
        if let Some(reader) = self.reader.take() {
            // `read` 里的任务各自持有 `Arc<Reader>`，所以这里只关得掉"确实没人还在用"的那份；
            // 池自己的 Drop 会收拾剩下的连接。
            if let Ok(r) = Arc::try_unwrap(reader) {
                r.disconnect().await?;
            }
        }
        self.schema = None;
        self.logical.clear();
        self.resolved = None;
        self.meter = None;
        Ok(())
    }
}

impl Default for DorisSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

/// 读一个分片：一条连接 → 一句 SQL → 一批批交给内核
#[allow(clippy::too_many_arguments)]
async fn stream_split(
    reader: Arc<Reader>,
    sql: String,
    schema: SchemaRef,
    logical: Vec<LogicalType>,
    tx: &tokio::sync::mpsc::Sender<D2DResult<arrow::record_batch::RecordBatch>>,
    rows_read: &AtomicU64,
    started: &AtomicBool,
    meter: Option<Arc<MemMeter>>,
) -> D2DResult<()> {
    let mut conn = reader
        .get_conn()
        .await
        .map_err(|e| D2DError::external("doris", format!("取连接失败: {}", e)))?;
    // 挂账：这条连接自己那份读缓冲。租约随函数返回（连接归还）自动销账。
    let _lease = meter.as_ref().map(|m| m.claim(NOMINAL_CONN_READ_BUFFER));

    let ncols = schema.fields().len();
    let mut qr = conn
        .query_iter(&sql)
        .await
        .map_err(|e| D2DError::external("doris", format!("查询失败（{}）: {}", sql, e)))?;

    // 列数校验：服务器返回的列与 schema 对不上，往下走就是整批错位
    let got = qr.columns_ref().len();
    if got != ncols {
        return Err(D2DError::schema(format!(
            "结果集列数 {} 与 schema 列数 {} 不一致 —— 查询与契约对不上了",
            got, ncols
        )));
    }

    started.store(true, Ordering::Relaxed);

    let new_builder = |logical: &[LogicalType]| -> D2DResult<RowBuilder> {
        RowBuilder::new(
            Arc::clone(&schema),
            logical.to_vec(),
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
        .map_err(|e| D2DError::external("doris", format!("取行失败: {}", e)))?
    {
        // 文本协议下所有值都是 `Bytes`，NULL 是 `Value::NULL`。
        // 出现别的形态说明协议假设不成立 —— **报错而不是当 NULL**：
        // 静默当 NULL 就是把整列悄悄抹掉。
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
                        "doris",
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
            // 行数在**送出之后**才计：送不出去的那批不算"交出去了"
            //
            // ⚠️ 但 send 失败（下游断开）时下面 return Ok 了，那一批也不会计 —— 对。
            if tx.send(Ok(batch.finish()?)).await.is_err() {
                return Ok(()); // 下游已断开
            }
            rows_read.fetch_add(n, Ordering::Relaxed);
            batch_bytes = 0;
        }
    }

    if !rb.is_empty() {
        let n = rb.rows();
        // 下游可能已经走了，这一批送不出去也不算错
        let _ = tx.send(Ok(rb.finish()?)).await;
        rows_read.fetch_add(n, Ordering::Relaxed);
    }
    debug!(rows = row_no, "doris 分片读完");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{PartitionMode, PartitionSpec};
    use serde_json::json;

    fn spec() -> SourceSpec {
        SourceSpec {
            connector: "doris".into(),
            connection: json!({"fe_hosts": ["192.0.2.10:19030"], "username": "root", "password": "x"}),
            method: None,
            properties: None,
            table: "t_a".into(),
            schema_name: Some("demo_db".into()),
            query: None,
            type_contract: None,
            split: None,
            partition: None,
        }
    }

    #[test]
    fn test_name_is_doris() {
        // 与 register() 里登记的名字、TypeAdapter 的 db_name 必须逐字一致
        assert_eq!(DorisSourceConnector::new().name(), "doris");
    }

    #[test]
    fn test_database_from_schema_name_then_connection() {
        let mut s = spec();
        assert_eq!(
            DorisSourceConnector::database_of(&s).unwrap(),
            "demo_db"
        );
        // connection.database 作为兼容写法
        s.schema_name = None;
        s.connection = json!({"fe_hosts": ["h"], "database": "db2"});
        assert_eq!(DorisSourceConnector::database_of(&s).unwrap(), "db2");
        // 都没有 → 明确报错，不猜
        s.connection = json!({"fe_hosts": ["h"]});
        let e = DorisSourceConnector::database_of(&s)
            .unwrap_err()
            .to_string();
        assert!(e.contains("schema_name"), "{}", e);
        // 空串不当成"给了"
        s.schema_name = Some("   ".into());
        assert!(DorisSourceConnector::database_of(&s).is_err());
    }

    /// 未初始化时 `read` 必须报错，不能悄悄返回空流（那会"迁 0 行也报成功"）
    #[tokio::test]
    async fn test_read_before_init_errors() {
        let c = DorisSourceConnector::new();
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
        assert_eq!(DorisSourceConnector::new().exported_rows(), None);
    }

    /// 开过工就是"源端交出去多少行" —— 与目标端确认载入数是对账的两端
    #[test]
    fn test_exported_rows_reports_counted_rows_once_started() {
        let c = DorisSourceConnector::new();
        c.rows_read.store(1234, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), None, "还没开工，不能报数");
        c.started.store(true, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), Some(1234));
        // 真的读出 0 行（空表）是有效事实，不能吞成"没提供"
        c.rows_read.store(0, Ordering::Relaxed);
        assert_eq!(c.exported_rows(), Some(0));
    }

    /// 取数单元的形态：一条连接（连接器答"我一次开几个"，个数归内核闸门管）
    #[test]
    fn test_unit_footprint_is_one_connection() {
        let f = DorisSourceConnector::new().unit_footprint(&spec());
        assert_eq!(f.kind, d2d_connector::UnitKind::Connection);
        assert!(f.bytes_each >= NOMINAL_BATCH_BYTES);
        // 全在进程内，没有子进程那部分
        assert_eq!(f.child_bytes_each, 0);
    }

    #[test]
    fn test_split_condition_extraction() {
        let s = SourceSplit {
            split_id: "x".into(),
            split_info: json!({"where_condition": "`id` < 10"}),
        };
        assert_eq!(split_condition(&s), "`id` < 10");
        // 缺键 = 无条件（单分片），不是错误
        let s2 = SourceSplit {
            split_id: "y".into(),
            split_info: json!({}),
        };
        assert_eq!(split_condition(&s2), "");
    }

    /// 分片规划走内核那套：谓词是真 SQL，不是占位符
    #[tokio::test]
    async fn test_plan_splits_uses_kernel_partition() {
        let c = DorisSourceConnector::new();
        let mut s = spec();
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["`id` < 10".into(), "`id` >= 10".into()],
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert_eq!(split_condition(&splits[0]), "`id` < 10");

        // hash：渲染成 Doris 的 CRC32 形态（内核不知道什么是 CRC32）
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("user_id".into()),
            count: 4,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 4);
        assert_eq!(
            split_condition(&splits[3]),
            "MOD(CRC32(COALESCE(CAST(`user_id` AS STRING), '')), 4) = 3"
        );

        // range + 显式边界：零探测
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("created_at".into()),
            lower: Some(d2d_connector::partition::Literal::parse("2022-01-01")),
            upper: Some(d2d_connector::partition::Literal::parse("2022-01-05")),
            count: 2,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 2);
        assert!(split_condition(&splits[0]).contains("`created_at` >="));

        // none：单片、无条件（走内核规划，所以 id 是 `p000`）
        s.partition = Some(PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        });
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(split_condition(&splits[0]), "");
        assert_eq!(splits[0].split_id, "t_a_p000");

        // 压根没配 partition：退化成分片器的单分片（与上面那个不是同一条路）
        s.partition = None;
        let splits = c.plan_splits(&s).await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].split_id, "t_a_single");
    }

    /// range 缺边界要 probe —— 这里只验证它**要求给 column**（真探测要连库）
    #[tokio::test]
    async fn test_probe_requires_column() {
        let c = DorisSourceConnector::new();
        let e = c
            .probe_min_max(&spec(), "  ")
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("column"), "{}", e);
    }
}
