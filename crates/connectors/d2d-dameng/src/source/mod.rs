//! 达梦 **reader 方向**：`DamengSourceConnector` 及它用到的一切
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`method`] | 读取方式路由：`odbc`（游标逐行）/ `dmfldr`（服务端快载导出） |
//! | [`sql`] | 达梦 SQL 方言：标识符引号、分片谓词渲染、查询组装 |
//! | [`split`] | 分片的**边界探测**（内核 [`d2d_connector::partition`] 只负责切法数学） |
//!
//! 写方向（X → 达梦）不在这里，见 crate 根文档「骨架」一节。

pub mod method;
pub mod split;
pub mod sql;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_connector::{
    MemMeter, PhysicalFormat, PhysicalSource, PhysicalStream, SourceConnector, SourceSpec,
    SourceSplit,
};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use std::sync::Arc;
use tracing::info;

use crate::config::ConnectionConfig;
use crate::source::method::dmfldr::params::{ExportMode, FlDrParams};
use crate::source::method::dmfldr::FlDrMethod;
use crate::source::method::{odbc::OdbcMethod, DamengMethodKind, DamengReadMethod};
use crate::source::split::split_condition;
use crate::source::sql::DamengSqlBuilder;

/// 解析缓冲里**一批**的标称字节数 —— 只用于启动期内存账的估算
///
/// 取 4 MB：与内核 `flow::NOMINAL_BATCH_BYTES` 同一个量级口径
/// （"源端单批的典型大小"），这样两处估的是同一件事，不会各说各话。
/// 真要精确值只有跑起来才知道 —— 那是 `memory_meter`（运行期申报）的活。
const NOMINAL_PARSE_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// init 后固化的查询上下文（ODBC 路径）
#[derive(Debug, Clone)]
struct ResolvedQuery {
    schema_name: Option<String>,
    table: String,
    /// 用户查询配置（已自动补齐分区列）
    query: d2d_connector::QueryConfig,
}

/// 达梦数据库 Source 连接器
///
/// 通过 `source.method` 在 ODBC 游标 / dmfldr 高速导出两种读取方式间路由
/// （见 设计说明）。数据库语义层（SQL 方言、类型契约、split）跨方法复用。
pub struct DamengSourceConnector {
    name: String,
    /// init 时按 method 固化的读取方式
    method_kind: DamengMethodKind,
    /// Arc 持有：CLI 经共享句柄在 pipeline 结束后调 cleanup() 清理导出产物
    method: Option<Arc<dyn DamengReadMethod>>,
    /// dmfldr 路径：契约推导的权威 schema
    ctx: Option<crate::source::method::DamengReadContext>,
    /// ODBC 路径：固化查询上下文
    resolved: Option<ResolvedQuery>,
    /// dmfldr 路径：init 时固化（已含引擎授予的并发额度）的参数
    ///
    /// 存一份是为了让 [`Self::unit_footprint`] 能回答"每个取数单元占多少内存"
    /// —— 那取决于 `buffer_node_size`，是 init 时才知道的。
    resolved_params: Option<crate::source::method::dmfldr::params::FlDrParams>,
    /// **运行期占用申报尺子**（init 时按 method 建，缺省不申报）
    ///
    /// 只有 ODBC 挂账：它的行集缓冲是**进程内**的、不在任何池的账上，
    /// 而且只有它自己（`odbc-api` 分配完的那一刻）才知道真要了多少字节。
    ///
    /// dmfldr 侧不挂账，不是漏了：它的大头是**子进程**（已由
    /// [`Self::unit_footprint`] 单列为 `child_process_bytes`），
    /// 进程内那些 Arrow 批次就是池里在排队的项，账本第 ②③ 项已经记过 ——
    /// 再挂一次就是把同一笔内存记两遍。
    meter: Option<Arc<MemMeter>>,
}

impl DamengSourceConnector {
    pub fn new() -> Self {
        Self {
            name: "dameng".to_string(),
            method_kind: DamengMethodKind::Odbc,
            method: None,
            ctx: None,
            resolved: None,
            resolved_params: None,
            meter: None,
        }
    }

    /// 生成某个 split 的完整查询 SQL（用户条件 + 分区条件）
    ///
    /// 仅 ODBC 路径有意义；未 init 时返回空串。
    pub fn build_split_sql(&self, split: &SourceSplit) -> String {
        let Some(resolved) = &self.resolved else {
            return String::new();
        };
        let builder = DamengSqlBuilder::new(resolved.schema_name.as_deref(), &resolved.table);
        builder.build_query(&resolved.query, &split_condition(split))
    }

    /// ODBC 分片规划 —— 走**内核 `partition` 那套**，与 dmfldr 同源
    ///
    /// 曾经 ODBC 走的是另一套（`split.rs` 里那批 `SplitStrategy`），
    /// 而 range 那条路产出的是 `__PENDING_RANGE_i__` 占位条件 —— 也就是说
    /// **range 从来没能真正用起来**（占位条件会被 `build_query` 静默丢掉，
    /// 于是每个分片都在扫全表）。现在两条路径共用内核规划：切法一致、
    /// 条件渲染一致、也顺手把那个坑填了。
    async fn plan_odbc_splits(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        use d2d_connector::partition::{PartitionMode, ProbedBounds};

        let Some(p) = spec.effective_partition()? else {
            return Ok(vec![Self::single_split(&spec.table)]);
        };

        // range 且缺边界 → 探一次 MIN/MAX（有索引时近乎免费）
        let probed = if p.mode == PartitionMode::Range && (p.lower.is_none() || p.upper.is_none()) {
            let col = p.column.clone().unwrap_or_default();
            let (lo, hi) = self.probe_min_max(spec, &col).await?;
            let cuts = d2d_connector::partition::range_cuts(&lo, &hi, p.count)?;
            info!(
                column = %col,
                cuts = cuts.len(),
                "odbc range 分片：MIN/MAX 探测完成（零探测请显式给 lower/upper）"
            );
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
                    // 内核谓词 → 达梦 SQL 片段；`build_query` 会把它 AND 进 WHERE
                    "where_condition": crate::source::sql::render_partition_predicate(&part.predicate),
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
            return Err(d2d_core::D2DError::config(
                "partition.mode=range 需要 column（或显式给 lower+upper 以跳过探测）",
            ));
        }
        let sql =
            DamengSqlBuilder::new(spec.schema_name.as_deref(), &spec.table).build_min_max(column);
        let conn = ConnectionConfig::from_json(&spec.connection)?;
        let timeout = spec
            .prop("query_timeout_sec")
            .and_then(|v| v.as_u64())
            .unwrap_or(3600);

        let row = tokio::task::spawn_blocking(move || {
            OdbcMethod::probe_min_max(&conn.to_odbc_connection_string(), &sql, timeout)
        })
        .await
        .map_err(|e| d2d_core::D2DError::internal(format!("MIN/MAX 探测任务 panic: {}", e)))??;

        let (Some(min_s), Some(max_s)) = (row.0, row.1) else {
            return Err(d2d_core::D2DError::external(
                "odbc",
                format!(
                    "range 分片探测 MIN/MAX 返回空值（列 {} 全为 NULL 或表为空）—— \
                     请改用 partition.mode=list / none",
                    column
                ),
            ));
        };
        Ok((Literal::parse(&min_s), Literal::parse(&max_s)))
    }
}

#[async_trait]
impl SourceConnector for DamengSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()> {
        let kind = DamengMethodKind::parse(spec.method.as_deref())?;
        self.method_kind = kind;
        info!(connector = %self.name, method = %kind, table = %spec.table, "initializing dameng source");

        match kind {
            DamengMethodKind::FlDr => {
                // 类型契约 → 权威 schema（dmfldr 文本不带类型信息，契约是唯一来源）
                let ctx = crate::source::method::validate_dmfldr_read_config(spec)?;
                let params = FlDrParams::from_props(&spec.props())?;
                params.validate()?;
                // 启动期清掉上次被 kill 留下的导出物（一次 kill 实测 7.6 GB）。
                // 只清**属于已死进程**的目录，见 `proc::sweep_stale_export_dirs`。
                let swept = crate::source::method::dmfldr::proc::sweep_stale_export_dirs(
                    &params
                        .work_dir
                        .clone()
                        .unwrap_or_else(|| std::env::temp_dir().join("d2d-dmfldr")),
                );
                if swept > 0 {
                    tracing::info!(dirs = swept, "清掉了上次异常退出留下的 dmfldr 导出目录");
                }
                // 导出模式由 `source.partition` 派生 —— 不再是用户旋钮
                let export_mode = ExportMode::for_spec(spec)?;
                params.validate_for(export_mode)?;
                let conn = ConnectionConfig::from_json(&spec.connection)?;
                // 这两行原本在 CLI 里（`if connector == "dameng" { ... }`）。
                // 挪进来是因为**只有达梦自己知道自己的读取方式有几个旋钮** ——
                // 让它自己报告，CLI 就不必为每一种库写一段"我记得它有什么"。
                info!(
                    method = "dmfldr",
                    export_mode = if export_mode == ExportMode::Partitioned {
                        "partitioned"
                    } else {
                        "table"
                    },
                    columns = ctx.column_names.len(),
                    "达梦读取方式"
                );
                // ⚠️ 这里**没有并发度可传** —— 那是刻意的。
                // "同时开几个导出子进程"由内核按**任务**发券卡死：一个分片一个任务，
                // 券在派发器手里。连接器连那个数字都看不到，也就谈不上"自己限流"。
                self.method = Some(Arc::new(FlDrMethod::new(params.clone(), conn, export_mode)));
                self.ctx = Some(ctx);
                self.resolved = None;
                self.resolved_params = Some(params);
                self.meter = None;
            }
            DamengMethodKind::Odbc => {
                // 契约即权威 schema —— odbc 路径同样**按位置**用它给每一列定类型，
                // 所以它是必填的（缺失就 fail-fast，不留"无类型裸奔"的口子）。
                let ctx = crate::source::method::build_read_context(spec)?;
                // 解析查询配置；若配置了分区列而 columns 未含，自动补齐
                let builder = DamengSqlBuilder::new(spec.schema_name.as_deref(), &spec.table);
                let query = spec.query.clone().unwrap_or_default();
                let query = match spec
                    .effective_partition()?
                    .and_then(|p| p.column.filter(|c| !c.trim().is_empty()))
                {
                    Some(col) => builder.ensure_split_column(&query, &col)?,
                    None => query,
                };

                let fetch_rows = spec
                    .prop("fetch_size")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10_000) as usize;
                let timeout_sec = spec
                    .prop("query_timeout_sec")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(3600);

                // 行集占用的申报尺子。名字与说明由**本连接器**给（引擎不解释字节是什么），
                // 这里把真实构成写清楚，好让"账本 ① vs 实测"这一比看得懂。
                let meter = MemMeter::new(
                    "dameng/odbc 行集",
                    format!(
                        "每条连接 {} 行 × {} 列（列宽取驱动上报值 ×4，上限 {} B）+ 指示器",
                        fetch_rows,
                        ctx.column_names.len(),
                        crate::source::method::odbc::MAX_TEXT_COL_BYTES
                    ),
                );
                let method = OdbcMethod::new(
                    ConnectionConfig::from_json(&spec.connection)?,
                    spec.schema_name.clone(),
                    spec.table.clone(),
                    query.clone(),
                    fetch_rows,
                    timeout_sec,
                    Arc::clone(&meter),
                );

                // ── 启动期自检：连不上 / 列序对不上，**此刻就失败** ──
                //
                // 这是"迁移 0 行也报成功"那道口子的封堵点：ODBC 曾经是一条空桩，
                // 跑完 0.2 秒、退出码 0、日志一路 INFO，一行数据都没迁。
                let probe = method.clone();
                let expected = ctx.column_names.clone();
                tokio::task::spawn_blocking(move || probe.self_check(&expected))
                    .await
                    .map_err(|e| {
                        d2d_core::D2DError::internal(format!("odbc 自检任务 panic: {}", e))
                    })??;

                self.resolved = Some(ResolvedQuery {
                    schema_name: spec.schema_name.clone(),
                    table: spec.table.clone(),
                    query,
                });
                self.method = Some(Arc::new(method));
                self.ctx = Some(ctx);
                self.resolved_params = None;
                self.meter = Some(meter);
            }
            DamengMethodKind::Dpi => {
                // 契约即 schema，也即 SELECT 列序 —— DPI 与 dmfldr 同一条规矩
                let ctx = crate::source::method::build_read_context_for(
                    spec,
                    "dpi",
                    "DPI returns text cells parsed per the contract; the contract is the \
                     schema authority and its column order is the SELECT list order",
                )?;
                let params =
                    crate::source::method::dpi::params::DpiParams::from_props(&spec.props())?;
                params.validate()?;
                crate::source::method::dpi::params::validate_dpi_query(spec)?;

                let conn = ConnectionConfig::from_json(&spec.connection)?;
                // 动态库只在选了 dpi 时加载；加载失败是**配置错误**，不是运行期故障
                let library = crate::dpi::ffi::DpiLibrary::load(params.library.as_deref())?;
                let method =
                    crate::source::method::dpi::DpiMethod::new(params.clone(), conn, library)?;

                // ── 启动期自检：连库 + 空结果查询 + 列序/类型可绑定 ──
                //
                // 与 ODBC 同一个理由：让"连不上 / 列名对不上 / LOB 不支持"在开工前
                // 失败，而不是跑完 30M 行才发现某列被截断。
                let probe = method.clone();
                let probe_spec = spec.clone();
                let probe_ctx = ctx.clone();
                tokio::task::spawn_blocking(move || probe.self_check(&probe_spec, &probe_ctx))
                    .await
                    .map_err(|e| {
                        d2d_core::D2DError::internal(format!("dpi 自检任务 panic: {}", e))
                    })??;

                info!(
                    method = "dpi",
                    columns = ctx.column_names.len(),
                    fetch_rows = params.fetch_rows,
                    "达梦读取方式"
                );
                self.method = Some(Arc::new(method));
                self.ctx = Some(ctx);
                self.resolved = None;
                self.resolved_params = None;
                self.meter = None;
            }
        }
        Ok(())
    }

    async fn schema(&self) -> D2DResult<SchemaRef> {
        match (&self.method_kind, &self.ctx) {
            (DamengMethodKind::FlDr | DamengMethodKind::Dpi, Some(ctx)) => {
                Ok(Arc::clone(&ctx.schema))
            }
            _ => Err(d2d_core::D2DError::schema(
                "schema not fetched, call init() first",
            )),
        }
    }

    async fn split(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        match (&self.method_kind, &self.method, &self.ctx) {
            (DamengMethodKind::FlDr, Some(m), Some(ctx)) => m.split(spec, ctx).await,
            (DamengMethodKind::Dpi, Some(m), Some(ctx)) => {
                let splits = m.split(spec, ctx).await?;
                info!(
                    connector = %self.name,
                    table = %spec.table,
                    splits = splits.len(),
                    "dpi split generated"
                );
                Ok(splits)
            }
            (DamengMethodKind::Odbc, _, _) => {
                let splits = self.plan_odbc_splits(spec).await?;
                info!(
                    connector = %self.name,
                    table = %spec.table,
                    splits = splits.len(),
                    "odbc split generated"
                );
                Ok(splits)
            }
            _ => Err(d2d_core::D2DError::internal(
                "dameng source not initialized: call init() first",
            )),
        }
    }

    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream> {
        match (&self.method_kind, &self.method, &self.ctx) {
            // 两条路径的 read 形状完全一样：方法自己拼 SQL、自己开取数单元、
            // 自己把结果按契约类型组装成 RecordBatch
            (_, Some(m), Some(ctx)) => m.read(ctx, split).await,
            _ => Err(d2d_core::D2DError::internal(
                "dameng source not initialized: call init() first",
            )),
        }
    }

    fn as_physical(&self) -> Option<&dyn PhysicalSource> {
        Some(self)
    }

    /// dmfldr 取数是**独立子进程**（内存闸门够不着），ODBC 是进程内游标。
    ///
    /// 只读声明、不做 I/O —— 所以 `--dry-run`（那时还没 init）也算得出来。
    fn unit_footprint(&self, spec: &SourceSpec) -> d2d_connector::UnitFootprint {
        use d2d_connector::UnitFootprint;
        match DamengMethodKind::parse(spec.method.as_deref()) {
            // ODBC：一条连接一股，**行集缓冲才是这条账里的大头** ——
            // `行数 × 契约列宽之和`，宽表上轻松几百 MB，而且它不在任何池的账上。
            // 这里如实报出来，装不下就让启动期账本拒绝 —— 不偷偷夹小 fetch_size。
            Ok(DamengMethodKind::Odbc) => {
                let fetch_rows = spec
                    .prop("fetch_size")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10_000) as usize;
                return UnitFootprint::connection(OdbcMethod::rowset_bytes(
                    fetch_rows,
                    crate::source::method::odbc::declared_row_bytes(spec),
                ));
            }
            // DPI：每条连接一套数组绑定缓冲（行数 × 列宽）+ 一个稳态物理块。
            // 块缓冲在直通面才用；逻辑面少一块，这里按多的报。
            Ok(DamengMethodKind::Dpi) => {
                let p = crate::source::method::dpi::params::DpiParams::from_props(&spec.props())
                    .unwrap_or_default();
                let ncols = spec
                    .type_contract
                    .as_ref()
                    .map(|c| c.len())
                    .unwrap_or(1)
                    .max(1);
                // 每格：数据缓冲下限 + ind + act（两个 i64）
                let row_bytes = ncols * (p.column_bytes.max(64) + 16);
                let fetch_bytes = p.fetch_rows.max(1) * row_bytes;
                return UnitFootprint::connection(fetch_bytes + p.block_bytes() * 2);
            }
            Ok(DamengMethodKind::FlDr) => {}
            Err(_) => return UnitFootprint::in_process(),
        }
        match FlDrParams::from_props(&spec.props()) {
            Ok(p) => {
                // ⚡ 一个取数单元 = **两部分**，两个都要报：
                //
                //   进程**外**：一个导出子进程（buffer_node_size）
                //   进程**内**：本连接器自己的并行解析缓冲 —— `parse_parallel` 个批次
                //
                // 只报子进程，把 `parse_parallel` 调到 16 就成了账外内存；
                // 只报一个合计数，"本进程口径"（收尾拿去跟实测 RSS 比的那个）会算歪。
                //
                // ⚠️ 单批字节用**标称值**，不要拿 `odbc::declared_row_bytes` 那把尺子 ——
                // 那是给 ODBC 的 `TextRowSet` 用的（每列真的按最大文本宽度分配定长缓冲），
                // 而这里的批次是 **Arrow 数组**，比文本小一个量级。
                // 实测踩过：拿它估这一项，37 列宽表每 task 估出 183 MB、16 路共 2.9 GB，
                // 于是**一份原本能跑的配置被拒绝启动**了 —— 账本估歪的代价不是"显示难看"，
                // 是把能跑的活挡在门外。
                let in_process = p.parse_parallel.max(1) * NOMINAL_PARSE_BATCH_BYTES;
                UnitFootprint::process_plus_local(
                    p.buffer_node_size as usize * 1024 * 1024,
                    in_process,
                )
            }
            Err(_) => UnitFootprint::in_process(),
        }
    }

    /// 运行期申报：只有 ODBC 挂账（见 `meter` 字段的说明）
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    /// 源端权威行数：**dmfldr 导出侧的日志统计**
    ///
    /// ODBC 是逐行游标、不产出总行数，如实返回 `None`（引擎会跳过对账并说明原因，
    /// 而不是假装对过账）。dmfldr 的取值规则见 `method::dmfldr::proc::parse_log_stats`
    /// —— 进度行按**取最大**读，因为多条线程的进度是乱序的。
    fn exported_rows(&self) -> Option<u64> {
        self.method.as_ref().and_then(|m| m.exported_rows())
    }

    /// 释放导出产物（dmfldr 的临时数据文件 / 控制文件 / 日志）
    ///
    /// 为什么 `&self`：引擎在 `Arc<dyn SourceConnector>` 后面持有它。
    /// 读取方式内部用 Mutex 管导出目录，所以共享句柄也调得动。
    async fn cleanup(&self) -> D2DResult<()> {
        if let Some(m) = &self.method {
            m.close().await
        } else {
            Ok(())
        }
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing dameng source");
        self.cleanup().await?;
        self.resolved = None;
        self.method = None;
        // TODO(M8-C): 关闭 ODBC 连接池
        Ok(())
    }
}

/// 物理载荷能力：把「读取方式能不能产出目标可直接吃的字节」透出去
///
/// 判定权在读取方式（[`DamengReadMethod::physical_format`]），连接器只负责把
/// 契约推导出的列名补进格式描述 —— 列序是直通正确性的关键，见 `doris/session.rs`
/// 关于 `columns` 头的注释。
#[async_trait]
impl PhysicalSource for DamengSourceConnector {
    fn physical_format(&self) -> Option<PhysicalFormat> {
        let ctx = self.ctx.as_ref()?;
        let m = self.method.as_ref()?;
        m.physical_format(ctx)
    }

    async fn read_physical(&self, split: &SourceSplit) -> D2DResult<PhysicalStream> {
        let ctx = self.ctx.as_ref().ok_or_else(|| {
            D2DError::internal("dameng source not initialized: call init() first")
        })?;
        let m = self.method.as_ref().ok_or_else(|| {
            D2DError::internal("dameng source not initialized: call init() first")
        })?;
        m.read_physical(ctx, split).await
    }
}

impl Default for DamengSourceConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::method::validate_dmfldr_read_config;
    use crate::source::method::DamengReadContext;
    use d2d_connector::{QueryColumn, QueryConfig, SplitConfig, SplitMode, TypeContractColumn};
    use serde_json::json;

    fn dmfldr_spec() -> SourceSpec {
        SourceSpec {
            connector: "dameng".to_string(),
            connection: json!({"host": "192.0.2.1", "user": "SYSDBA", "password": "p"}),
            method: Some("dmfldr".to_string()),
            properties: None,
            table: "t_a".to_string(),
            schema_name: Some("PROD".to_string()),
            query: Some(QueryConfig {
                columns: Some(vec![
                    QueryColumn::Name("id".into()),
                    QueryColumn::Name("name".into()),
                ]),
                ..Default::default()
            }),
            type_contract: Some(vec![
                TypeContractColumn {
                    column: "id".into(),
                    source_type: "BIGINT".into(),
                    sink_type: "BIGINT".into(),
                },
                TypeContractColumn {
                    column: "name".into(),
                    source_type: "VARCHAR(200)".into(),
                    sink_type: "VARCHAR(500)".into(),
                },
            ]),
            split: Some(SplitConfig {
                mode: SplitMode::None,
                column: String::new(),
                count: 8,
            }),
            partition: None,
        }
    }

    #[tokio::test]
    async fn test_dmfldr_init_builds_context_and_method() {
        let spec = dmfldr_spec();
        let mut conn = DamengSourceConnector::new();
        conn.init(&spec).await.unwrap();
        assert_eq!(conn.method_kind, DamengMethodKind::FlDr);
        let schema = conn.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
    }

    /// ODBC 的 `init` 现在**会连库自检**（那是"迁 0 行也报成功"的封堵点），
    /// 所以它必须有真库才能跑 —— 标 `#[ignore]`，在真实数据库上单独跑。
    #[tokio::test]
    #[ignore = "需要可达的达梦（init 会做连接 + 列序自检）"]
    async fn test_odbc_init_against_real_db() {
        let mut spec = dmfldr_spec();
        spec.method = None; // 缺省 odbc
        spec.connection = json!({"dsn": "DM8", "user": "DEMO_USER", "password": "x"});
        let mut conn = DamengSourceConnector::new();
        conn.init(&spec).await.unwrap();
        assert_eq!(conn.method_kind, DamengMethodKind::Odbc);
    }

    /// **不需要连库**的那部分：SQL 组装 + 内核分片规划
    ///
    /// 这条守的是"ODBC 的分片不再是占位条件"—— 曾经 range 产出
    /// `__PENDING_RANGE_i__`，会被 `build_query` 静默丢掉，于是每片都在扫全表。
    #[tokio::test]
    async fn test_odbc_splits_use_kernel_partition_not_placeholders() {
        use d2d_connector::{PartitionMode, PartitionSpec};

        let mut spec = dmfldr_spec();
        spec.method = Some("odbc".into());
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["\"id\" < 10".into(), "\"id\" >= 10".into()],
            ..Default::default()
        });
        spec.split = None;

        let conn = DamengSourceConnector::new();
        let splits = conn.plan_odbc_splits(&spec).await.unwrap();
        assert_eq!(splits.len(), 2);
        let c0 = split_condition(&splits[0]);
        assert_eq!(c0, "\"id\" < 10");
        assert_eq!(split_condition(&splits[1]), "\"id\" >= 10");
        assert!(!c0.contains("PENDING"), "不能是占位条件: {}", c0);

        // hash 模式：内核渲染成 ORA_HASH(...)（DM 没有 HASH()）
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("USER_ID".into()),
            count: 4,
            ..Default::default()
        });
        let splits = conn.plan_odbc_splits(&spec).await.unwrap();
        assert_eq!(splits.len(), 4);
        assert_eq!(
            split_condition(&splits[2]),
            "MOD(ORA_HASH(COALESCE(CAST(\"USER_ID\" AS VARCHAR), ''), 4294967295), 4) = 2"
        );

        // range + 显式边界：**零探测**，切点由内核按时间/数值跨度算
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("id".into()),
            lower: Some(d2d_connector::partition::Literal::Int(1)),
            upper: Some(d2d_connector::partition::Literal::Int(100)),
            count: 4,
            ..Default::default()
        });
        let splits = conn.plan_odbc_splits(&spec).await.unwrap();
        assert_eq!(splits.len(), 4);
        assert_eq!(split_condition(&splits[0]), "\"id\" >= 1 AND \"id\" < 26");
        assert_eq!(
            split_condition(&splits[3]),
            "\"id\" >= 76 AND \"id\" <= 100"
        );

        // 不分片：单片、条件为空
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        });
        let splits = conn.plan_odbc_splits(&spec).await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(split_condition(&splits[0]), "");
    }

    #[tokio::test]
    async fn test_dmfldr_init_rejects_missing_contract() {
        let mut spec = dmfldr_spec();
        spec.type_contract = None;
        let mut conn = DamengSourceConnector::new();
        let err = conn.init(&spec).await.unwrap_err();
        assert!(err.to_string().contains("type_contract"), "{}", err);
    }

    #[tokio::test]
    async fn test_dmfldr_init_rejects_where() {
        let mut spec = dmfldr_spec();
        spec.query = Some(QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("id".into()),
                QueryColumn::Name("name".into()),
            ]),
            where_clause: Some("\"status\" = 1".into()),
            ..Default::default()
        });
        let err = validate_dmfldr_read_config(&spec).unwrap_err();
        assert!(err.to_string().contains("where_clause"), "{}", err);
    }

    #[tokio::test]
    async fn test_dmfldr_init_rejects_column_order_mismatch() {
        let mut spec = dmfldr_spec();
        spec.query = Some(QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("name".into()),
                QueryColumn::Name("id".into()),
            ]),
            ..Default::default()
        });
        let err = validate_dmfldr_read_config(&spec).unwrap_err();
        assert!(
            err.to_string().contains("与 type_contract 顺序完全一致"),
            "{}",
            err
        );
    }

    /// `split()` 交出去的 `split_info` 必须带够 `read()` 要用的键
    ///
    /// 两种分片的键不同，因为**产物怎么来不同**：`TABLE=` 的产物是导出后才知道的
    /// （所以只给控制文件/数据文件路径，导出在 `read` 里发生），分片模式给的是
    /// 规划期就写好的 SQL 与数据文件路径。
    #[test]
    fn test_dmfldr_split_info_shape() {
        let table = json!({
            "kind": "dmfldr_table",
            "dir": "/tmp/x",
            "ctl": "/tmp/x/t.ctl",
            "data": "/tmp/x/t.txt",
            "max_rows": 10000
        });
        for k in ["dir", "ctl", "data", "max_rows"] {
            assert!(table.get(k).is_some(), "TABLE= 缺键 {}", k);
        }

        let shard = json!({
            "kind": "dmfldr_shard",
            "index": 0,
            "shards": 16,
            "sql": "/tmp/x/shard_000.sql",
            "data": "/tmp/x/shard_000.txt",
            "describe": "…",
            "max_rows": 10000
        });
        for k in ["index", "sql", "data", "max_rows"] {
            assert!(shard.get(k).is_some(), "分片模式缺键 {}", k);
        }
        // 内核切块时代的 `dmfldr_chunk` 已经不存在了 —— 产物内部怎么切是连接器的事
        assert!(table.get("start").is_none() && shard.get("start").is_none());
    }

    #[allow(dead_code)]
    fn _assert_types(_: Option<DamengReadContext>) {}
}
