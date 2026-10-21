//! dmfldr 读取方法：全表高速导出 + 连接器侧分块并行解析
//!
//! # 一个 source_task = 一份产物（导出 + 交给池）
//!
//! | 模式 | 有几片（几个任务） | 一片的产物 |
//! |---|---|---|
//! | `TABLE=`（`partition.mode: none`） | **1 片** | 整表导出成一个文件 |
//! | `SQLFILE=`（分片模式） | `partition` 切出几片就几片 | 一片一个文件 |
//!
//! `split()` **只规划**（写控制文件 / 写分片 SQL），**导出住在 `read()` 里** ——
//! 券是任务生命的起点（内核发券 → 取数 → 进池 → 还券），而导出是唯一碰库的动作。
//!
//! # 产物内部怎么切，是**本连接器**的事
//!
//! 一份产物要不要切成多块并行解析，由 `source.properties.dmfldr.parse_parallel` 决定
//! （**缺省 1 = 顺序解析**），内核只看到"一条流"，它不切块、不知道产物的形状。
//! 做得好坏是连接器的竞争力 —— 内核只按 `flow.source.concurrency` 发券。
//!
//! 由此"导出与加载重叠"也不再需要一条专门的流式接口（旧版有个 `split_stream`，已删）：
//! 一个任务一个产物，重叠是任务并发的自然结果。
//!
//! 限制（启动期 fail-fast）：不支持 where/order/limit/计算列（TABLE 模式）。

pub mod ctl;
pub mod fmt;
pub mod params;
pub mod parse;
pub mod partition;
pub mod physical;
pub mod plan;
pub mod proc;

use crate::config::ConnectionConfig;
use crate::source::method::{DamengReadContext, DamengReadMethod};
use arrow::array::RecordBatch;
use d2d_connector::{Charset, CsvDialect, PhysicalFormat, PhysicalStream, SourceSpec, SourceSplit};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
pub use params::{CharacterCode, ExportMode, FlDrParams};
use parse::{ChunkParser, TextParseOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// 分片级行数账本：各分片把自己那一片的导出结果记进来，**最后一个到齐的做交叉校验**
///
/// 为什么需要一个"到齐"的概念：`split_info` 里带着"本片是第几片、共几片"，
/// 而各分片是**并发**跑的 —— 谁的 `read()` 最后返回，谁就负责对账。
/// 对不上（各分片日志合计 ≠ 分区探测总数）说明分片区间没有完整覆盖源表
/// （或存在重叠），继续下去会静默丢数/重复，所以从那个 `read()` 里直接失败。
#[derive(Default)]
struct ShardLedger {
    /// 已完成的导出分片数
    done: usize,
    /// 各分片 dmfldr 日志行数之和
    rows: u64,
}

/// [`FlDrMethod::note_shard_done`] 的自由函数版本
///
/// 流式路径在 `tokio::spawn` 出来的收尾任务里记账 —— 那里拿不到 `&self`，
/// 只能持有几个 `Arc`。把同一段账本逻辑抽出来，两条路径才**不会各记各的账**。
fn note_shard_done_impl(
    ledger: &Mutex<ShardLedger>,
    total: usize,
    shard_expected_rows: &Mutex<Option<u64>>,
    exported_rows: &Mutex<Option<u64>>,
    rows_read: u64,
) -> D2DResult<()> {
    let mut ledger = ledger.lock().unwrap();
    ledger.done += 1;
    ledger.rows += rows_read;
    if total == 0 || ledger.done < total {
        return Ok(());
    }

    let sum = ledger.rows;
    let expected = *shard_expected_rows.lock().unwrap();
    match expected {
        // 日志一行都没解析出来 → 退回探测总数当对账基准（比没有基准好：
        // 它是独立来源，仍能挡住"某片漏导"）
        Some(pt) if sum == 0 => {
            warn!(
                probe_total = pt,
                "dmfldr 日志未解析出行数，改用探测总数作为对账基准"
            );
            *exported_rows.lock().unwrap() = Some(pt);
        }
        Some(pt) if sum != pt => {
            return Err(D2DError::external(
                "dmfldr",
                format!(
                    "分片导出行数交叉校验失败：各分片日志合计 {} 行，分区探测总数 {} 行。\
                     说明分片区间没有完整覆盖源表（或存在重复），继续会静默丢数/重复。",
                    sum, pt
                ),
            ));
        }
        _ if sum == 0 => {
            warn!("dmfldr 日志未解析出任何行数，本次跳过分片级行数对账");
        }
        _ => {
            *exported_rows.lock().unwrap() = Some(sum);
        }
    }
    info!(
        shards = total,
        total_rows = expected,
        "dmfldr: 全部分片导出完成"
    );
    Ok(())
}

/// 一个已启动、尚未等待结束的分片导出
struct ShardExport {
    proc: proc::SqlExportProcess,
    shard: partition::Shard,
}

/// dmfldr 读取方法实现
pub struct FlDrMethod {
    params: FlDrParams,
    conn: ConnectionConfig,
    /// **派生的**导出模式（由 `source.partition` 定，见 [`ExportMode::for_spec`]）
    ///
    /// 存在实例上而不是每处现算：`read()` 拿不到 `SourceSpec`，
    /// 而解析选项（有无控制文件 → 空值判定）取决于它。
    export_mode: ExportMode,
    /// 已产生的导出目录（close 时统一清理）
    export_dirs: Mutex<Vec<PathBuf>>,
    /// dmfldr 日志报出的导出行数（供收尾行数对账；日志缺失/解析失败时为 None）
    ///
    /// `Arc` 是必需的：分片模式下每个分片任务各记一笔，累加发生在 [`ShardLedger`] 里。
    exported_rows: Arc<Mutex<Option<u64>>>,
    /// 分片级行数账本（见 [`ShardLedger`]）
    ledger: Arc<Mutex<ShardLedger>>,
    /// 本次规划出的分片总数（分片模式下由 [`Self::split`] 写入）
    shard_total: Arc<AtomicUsize>,
    /// **值探测路径**给出的源端预期总行数 —— 唯一能与"各分片日志合计"对账的独立来源
    ///
    /// 元数据路径没有这个数，它的覆盖性由 `plan_ranges` 的"连续且首尾无界"结构性保证；
    /// 用户显式声明的分片则自负其责（同 DataX 的 `where` 契约）。
    shard_expected_rows: Arc<Mutex<Option<u64>>>,
}

impl FlDrMethod {
    pub fn new(params: FlDrParams, conn: ConnectionConfig, export_mode: ExportMode) -> Self {
        Self {
            params,
            conn,
            export_mode,
            export_dirs: Mutex::new(Vec::new()),
            exported_rows: Arc::new(Mutex::new(None)),
            ledger: Arc::new(Mutex::new(ShardLedger::default())),
            shard_total: Arc::new(AtomicUsize::new(0)),
            shard_expected_rows: Arc::new(Mutex::new(None)),
        }
    }

    fn work_dir(&self) -> PathBuf {
        self.params
            .work_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("d2d-dmfldr"))
    }

    /// 每批解析出多少行（**驱动取数粒度**，来自 `source.properties.fetch_size`）
    ///
    /// 它管的是"一次从库里拉多少行"，属于数据库连接行为 —— 所以住在 `properties`
    /// 里，而不是 `flow.source`（那里管的是并发与内存）。
    fn fetch_size(&self, spec: &SourceSpec) -> usize {
        crate::source::method::dmfldr::params::FlDrParams::fetch_size(&spec.props())
    }

    /// 本次导出产物的 CSV 方言 + 字符集；`None` = 产物无法被目标端直接解释
    ///
    /// 直通的前提是「源端产出的字节，目标端能按**同样的规则**解释」。以下任一成立
    /// 就不满足，必须回落逻辑面（由引擎解析后按目标语义重写）：
    ///
    /// - `null_mode = true`：空值被写成字面量（如 `NULL`）。目标端（Doris）的空值
    ///   字面量是**固定不可配**的 `\N`，收到字面量 `NULL` 会当成普通字符串写进列里。
    /// - `escape` 有值：源端把分隔符等字符转义了，目标端不会做同样的反转义，
    ///   直通会把 `\|` 原样写进去，值就变了。
    /// - 字符集不是 UTF-8/GBK：超出 [`Charset`] 的表达范围。
    /// - 分隔符不是单字节：方言描述是单字节粒度。
    fn export_dialect(&self) -> Option<(CsvDialect, Charset)> {
        let p = &self.params;
        if p.null_mode || p.escape.is_some() {
            return None;
        }
        let sep = p.field_seperator.as_bytes();
        if sep.len() != 1 {
            return None;
        }
        let charset = match p.character_code {
            CharacterCode::Utf8 => Charset::Utf8,
            CharacterCode::Gbk => Charset::Gbk,
            _ => return None,
        };
        Some((
            CsvDialect {
                column_separator: sep[0],
                // 控制文件固定 `STR X '0A'`；SQL 模式走 dmfldr 缺省，同样是换行
                line_delimiter: b'\n',
                quote: None,
                escape: None,
                null_literal: None,
            },
            charset,
        ))
    }

    /// 规划分片：**只写 SQL、不出数据** —— 导出挪进了 [`Self::read`]
    ///
    /// 每一片就是一个取数单元（一份 `SourceSplit`），`split_info` 里带着
    /// "SQL 文件 / 数据文件 / 第几片、共几片"。`read()` 拿到它就整片干活。
    ///
    /// ## 为什么导出不再住在这里
    ///
    /// 旧版本在这里 spawn 一批 dmfldr 进程、导完一片就往一条流里推一片 —— 那套
    /// **流式接口**存在的唯一理由是让"导出"与"下游加载"重叠。现在不需要了：
    /// **每个分片任务自带导出**，重叠是任务并发的自然结果，而且是同一件事的更简单说法。
    /// 顺带把"并发度给多少"这个问题从连接器手里彻底拿走了（本方法里没有任何限流）。
    ///
    /// ## 行数对账的两个来源
    ///
    /// 各分片导出完成后由 [`ShardLedger`] 汇总（这是实测来源）；
    /// 值探测路径另有一个独立的「探测总数」，两者**对不上就失败**，
    /// 避免"IN 列表没覆盖全 / 分片重叠"被静默吞掉。
    async fn plan_shard_splits(
        &self,
        spec: &SourceSpec,
        ctx: &DamengReadContext,
        dir: PathBuf,
    ) -> D2DResult<Vec<SourceSplit>> {
        self.export_dirs.lock().unwrap().push(dir.clone());

        // 分区意图与用户 WHERE 在规划开始前就固化下来 ——
        // 用户条件（若配了）要 AND 进**每一条**分片 SQL，不能只挂在某一片上
        let partition = spec.effective_partition()?;
        let user_where = spec
            .query
            .as_ref()
            .and_then(|q| q.where_clause.clone())
            .filter(|w| !w.trim().is_empty());
        let max_rows = self.fetch_size(spec);

        let (shards, plan_source, probe_total) = partition::plan_shards(
            &self.params,
            &self.conn,
            partition::ShardPlanInput {
                schema: spec.schema_name.as_deref(),
                table: &spec.table,
                columns: &ctx.column_names,
                partition: partition.as_ref(),
                user_where: user_where.as_deref(),
                dir: &dir,
            },
        )
        .await?;
        info!(
            plan_source = ?plan_source,
            shards = shards.len(),
            "dmfldr: 分片规划完成（任务数由数据决定：几片就是几个任务）"
        );

        let total = shards.len();
        self.shard_total.store(total, Ordering::Relaxed);
        *self.shard_expected_rows.lock().unwrap() = probe_total;

        Ok(shards
            .into_iter()
            .map(|s| SourceSplit {
                split_id: format!("{}_shard_{}", sanitize(&spec.table), s.index),
                split_info: serde_json::json!({
                    "kind": "dmfldr_shard",
                    "index": s.index,
                    "shards": total,
                    "sql": s.sql_path.to_string_lossy(),
                    "data": s.data_path.to_string_lossy(),
                    "describe": s.describe,
                    "max_rows": max_rows,
                }),
            })
            .collect())
    }

    /// 一片分片导完后的记账；**最后一片到齐时做交叉校验**
    ///
    /// 对不上直接返回 `Err` —— 那个错误会从最后一片的 `read()` 里冒出来，
    /// 走的就是正常的"源端任务失败 → 整条链路收工"那条路，不需要额外的上报口子。
    fn note_shard_done(&self, rows_read: u64) -> D2DResult<()> {
        note_shard_done_impl(
            &self.ledger,
            self.shard_total.load(Ordering::Relaxed),
            &self.shard_expected_rows,
            &self.exported_rows,
            rows_read,
        )
    }

    // ─────────────────────── 取数：一条路，两种分片 ───────────────────────
    //
    // 两种分片的差别只有**一处**：产物怎么来。
    //
    //   TABLE=   → 一个 dmfldr 进程导出整表，一个产物、一个任务
    //   分片模式 → 一个分片一个进程，各自一个产物、一个任务
    //
    // 之后的"解析产物 → 批次流"是同一件事，所以只有一份实现（[`Self::parse_export`]）。
    // 而"同时能跑几个"两边都不在这里 —— 内核按任务发券，连接器不参与限流。

    /// 整表导出（`TABLE=`）：起**一个** dmfldr 进程、产出**一个**文件
    ///
    /// ⚡ 这就是"一个任务 = 一份产物"里的那个任务。**导出必须在这里，不能挪到
    /// `split()`** —— 券是任务生命的起点（内核发券 → 取数 → 进池 → 还券），
    /// 而导出是唯一碰库的动作；放在 `split()` 里它就在券外，等于绕过了上限。
    ///
    /// 返回 `(数据文件, 列头结束偏移)`。列头校验是 schema drift 的防线，
    /// 必须在读之前做 —— 列名对不上不是"慢一点"，是整批数据错位。
    async fn export_table(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<(PathBuf, u64)> {
        let info = &split.split_info;
        let missing = |what: &str| {
            D2DError::schema(format!("dmfldr 分片 {} 缺少 {} 信息", split.split_id, what))
        };
        let dir = PathBuf::from(
            info.get("dir")
                .and_then(|v| v.as_str())
                .ok_or_else(|| missing("dir"))?,
        );
        let ctl_path = PathBuf::from(
            info.get("ctl")
                .and_then(|v| v.as_str())
                .ok_or_else(|| missing("ctl"))?,
        );
        let data_file = PathBuf::from(
            info.get("data")
                .and_then(|v| v.as_str())
                .ok_or_else(|| missing("data"))?,
        );
        let log_path = dir.join("d2d_fldr.log");

        info!(file = %data_file.display(), "dmfldr: 整表导出开始（TABLE=：一个进程 → 一个产物）");
        let outcome = proc::run_export(
            &self.params.executable,
            &self.conn.to_userid(),
            &ctl_path,
            &log_path,
            &dir,
        )
        .await?;

        // 源端权威行数（供收尾对账）。日志统计缺失时 rows_read 为 0 → 留 None，
        // 即按"拿不到源端行数"处理，而不是当成 0 行误报差异。
        if outcome.stats.rows_read > 0 {
            *self.exported_rows.lock().unwrap() = Some(outcome.stats.rows_read);
        }

        let header_end = if self.params.colname_heading {
            plan::read_header(
                &outcome.data_file,
                self.params.field_seperator.as_bytes(),
                &ctx.column_names,
            )?
            .0
        } else {
            0
        };

        info!(
            file = %outcome.data_file.display(),
            rows_exported = outcome.stats.rows_read,
            elapsed_ms = outcome.stats.elapsed_ms,
            header_end,
            "dmfldr: 整表导出完成（接下来解析的是**本连接器内部**的事）"
        );
        Ok((outcome.data_file, header_end))
    }

    /// 解析分片信息并**启动** dmfldr（不等它结束）
    ///
    /// ⚡ 这里就是"一个取数单元"落地的位置：一次调用 = 一个 dmfldr 导出子进程。
    /// 同时能跑几个由**内核按任务发券**卡死（券在派发器手里，任务结束才还），
    /// 本连接器既不取券、也不知道上限是多少 —— 这正是契约里说的"连接器不参与限流"。
    fn spawn_shard_export(&self, split: &SourceSplit) -> D2DResult<ShardExport> {
        let info = &split.split_info;
        let index = info.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let missing = |what: &str| {
            D2DError::schema(format!("dmfldr 分片 {} 缺少 {} 信息", split.split_id, what))
        };
        let sql_path = PathBuf::from(
            info.get("sql")
                .and_then(|v| v.as_str())
                .ok_or_else(|| missing("sql"))?,
        );
        let data_path = PathBuf::from(
            info.get("data")
                .and_then(|v| v.as_str())
                .ok_or_else(|| missing("data"))?,
        );
        let describe = info
            .get("describe")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let dir = data_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.work_dir());

        let shard = partition::Shard {
            index,
            sql_path,
            data_path,
            rows: 0,
            describe,
        };

        debug!(shard = index, dir = %dir.display(), "dmfldr: 启动本片导出（不等待，交由跟随读消费）");
        let proc = partition::spawn_shard(&self.params, &self.conn, &shard, &dir)?;
        Ok(ShardExport { proc, shard })
    }

    /// 等待一个分片导出结束，做产物校验与行数记账
    async fn finish_shard_export(&self, exp: ShardExport) -> D2DResult<()> {
        let stats = exp.proc.wait().await?;
        partition::warn_if_incomplete(&stats, exp.shard.index);
        partition::verify_shard_output(&exp.shard, &stats)?;
        self.note_shard_done(stats.rows_read)
    }

    /// **非流式**：等导出结束后返回产物路径
    ///
    /// 逻辑面 `parse_parallel > 1` 需要先知道文件总长才能切区间，只能走这条路。
    async fn export_shard(&self, split: &SourceSplit) -> D2DResult<PathBuf> {
        let exp = self.spawn_shard_export(split)?;
        let data_path = exp.shard.data_path.clone();
        self.finish_shard_export(exp).await?;
        Ok(data_path)
    }

    /// **流式**：返回产物路径 + 完成信号，读者可以立刻跟随读
    ///
    /// 等待/对账放在一条独立任务里：数据文件读完之前它可能还在跑，
    /// 但 [`physical::ExportDone`] 只会在记账**之后**置位 —— 于是流结束时，
    /// 行数账本一定是完整的，对账失败也能从这条流里冒出去。
    async fn export_shard_streaming(
        &self,
        split: &SourceSplit,
    ) -> D2DResult<(PathBuf, Arc<physical::ExportDone>)> {
        let exp = self.spawn_shard_export(split)?;
        let data_path = exp.shard.data_path.clone();
        let done = physical::ExportDone::new();
        let done_task = Arc::clone(&done);
        let ledger = Arc::clone(&self.ledger);
        let total = Arc::clone(&self.shard_total);
        let expected = Arc::clone(&self.shard_expected_rows);
        let exported = Arc::clone(&self.exported_rows);

        tokio::spawn(async move {
            let index = exp.shard.index;
            let result = match exp.proc.wait().await {
                Ok(stats) => {
                    partition::warn_if_incomplete(&stats, index);
                    match partition::verify_shard_output(&exp.shard, &stats) {
                        Ok(()) => note_shard_done_impl(
                            &ledger,
                            total.load(Ordering::Relaxed),
                            &expected,
                            &exported,
                            stats.rows_read,
                        ),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            };
            done_task.finish(result.err());
        });

        Ok((data_path, done))
    }

    /// `split_info.kind` 不认识时的错误 —— 说清"应该有哪几种"，而不是一句 unknown
    fn unknown_kind(split: &SourceSplit, got: Option<&str>) -> D2DError {
        D2DError::schema(format!(
            "dmfldr 分片 {} 的 kind {:?} 不认识（本连接器的 split() 只产出 \
             dmfldr_table / dmfldr_shard）",
            split.split_id, got
        ))
    }

    /// 一个分片要解析的批次行数（`split()` 时固化在 `split_info`）
    fn max_rows_of(&self, split: &SourceSplit) -> usize {
        split
            .split_info
            .get("max_rows")
            .and_then(|v| v.as_u64())
            .unwrap_or(10_000)
            .max(1) as usize
    }

    /// **解析一份导出产物 → 一条批次流**
    ///
    /// # 切几块、并行几路，在这里决定 —— 内核看到的只有"一条流"
    ///
    /// 这正是"连接器内部怎么干活是它自己的事"的落点：内核只按
    /// `flow.source.concurrency` 发券（同时几个任务在往池里放），
    /// 任务**内部**再并行几路由 `source.properties.dmfldr.parse_parallel` 说话。
    /// 内核不切块、不知道产物的形状，也就不会替连接器安排并行度。
    ///
    /// 缺省 `parse_parallel = 1` = 整份产物顺序解析：`TABLE=` 模式的特性就是
    /// "一个进程整表读"，与之一致。大表要快就显式调大 —— 代价进得了账本。
    fn parse_export(
        &self,
        ctx: &DamengReadContext,
        path: PathBuf,
        header_end: u64,
        opts: TextParseOptions,
        max_rows: usize,
    ) -> D2DResult<RecordBatchStream> {
        let parallel = self.params.parse_parallel.max(1);
        let chunks = plan::plan_chunks(&path, parallel, header_end)?;
        debug!(
            file = %path.display(),
            parse_parallel = parallel,
            chunks = chunks.len(),
            "dmfldr: 开始解析产物（并行度由连接器自己定，内核只看到一条流）"
        );

        // **有界**通道：满则解析停下 —— 内存上界 ≈ 通道深度 + parallel 个在手的批次
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<RecordBatch>>(4);
        for chunk in chunks {
            let tx = tx.clone();
            let path = path.clone();
            let opts = opts.clone();
            let schema = ctx.schema.clone();
            let logical = ctx.logical_types.clone();
            // ⚠️ `spawn_blocking`，不是 `tokio::spawn`：解析是**阻塞** I/O + CPU 密集的活。
            // 直接 spawn 会占住 runtime worker，而这里一开就是 parallel 个 ——
            // 那不是"慢一点"，是整条链路停摆（本项目在逻辑面踩过这类死锁，
            // 见 `dmfldr/physical.rs` 模块头的同一句提醒）。
            tokio::task::spawn_blocking(move || {
                if let Err(e) =
                    parse_chunk_into(&tx, &path, &schema, &logical, &opts, &chunk, max_rows)
                {
                    let _ = tx.blocking_send(Err(e));
                }
            });
        }
        // 手里那一份 `tx` 必须丢掉，否则通道永不关闭 —— 下游会一直等下去
        drop(tx);
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    /// **流式解析**一份还在导出的产物（`parse_parallel = 1` 的路径）
    ///
    /// 契约与 [`Self::parse_export`] 相同：有界通道 + `spawn_blocking` +
    /// 块边界行对齐。差别只在输入：`TailReader` 读到当前末尾会等待，
    /// 而不是把「还没写完」当成 EOF。
    fn parse_export_streaming(
        &self,
        ctx: &DamengReadContext,
        path: PathBuf,
        header_end: u64,
        opts: TextParseOptions,
        max_rows: usize,
        done: Arc<physical::ExportDone>,
    ) -> D2DResult<RecordBatchStream> {
        debug!(
            file = %path.display(),
            "dmfldr: 开始流式解析产物（导出未结束即开始消费）"
        );
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<RecordBatch>>(4);
        let schema = ctx.schema.clone();
        let logical = ctx.logical_types.clone();
        // 同 `parse_export`：阻塞 I/O + CPU 密集，必须挪出 runtime worker
        tokio::task::spawn_blocking(move || {
            if let Err(e) = parse_stream_into(
                &tx, &path, header_end, &done, &schema, &logical, &opts, max_rows,
            ) {
                let _ = tx.blocking_send(Err(e));
            }
        });
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

/// 阻塞地跟随解析一份**还在增长**的文件
///
/// 与 [`parse_chunk_into`] 的差别只在 Reader：`TailReader` 在数据还没写完时
/// 等待而不是 EOF；末行补齐、行数按 `\n` 计数等语义完全复用。
fn parse_stream_into(
    tx: &tokio::sync::mpsc::Sender<D2DResult<RecordBatch>>,
    path: &Path,
    header_end: u64,
    done: &Arc<physical::ExportDone>,
    schema: &arrow::datatypes::SchemaRef,
    logical: &[d2d_types::LogicalType],
    opts: &TextParseOptions,
    max_rows: usize,
) -> D2DResult<()> {
    let tail = physical::TailReader::open(path, header_end, Arc::clone(done))?;
    // 1MB：TailReader 每次 read 都要 seek，缓冲太小会把系统调用放进行级热路径
    let mut reader = std::io::BufReader::with_capacity(1024 * 1024, tail);
    let parser = ChunkParser::new(schema, logical, opts);
    loop {
        // `skip_header = false`：`header_end` 已经在列头之后（分片模式是 0）
        let (batch, rows) = parser.parse_rows(&mut reader, max_rows, false)?;
        if rows == 0 {
            break;
        }
        if tx.blocking_send(Ok(batch)).is_err() {
            break; // 下游断开
        }
    }
    Ok(())
}

/// 阻塞地解析**一块**，把批次塞进有界通道（通道满即阻塞 = 背压）
///
/// 与调用方约定：本函数跑在 `spawn_blocking` 里，所以可以用 `blocking_send`。
fn parse_chunk_into(
    tx: &tokio::sync::mpsc::Sender<D2DResult<RecordBatch>>,
    path: &Path,
    schema: &arrow::datatypes::SchemaRef,
    logical: &[d2d_types::LogicalType],
    opts: &TextParseOptions,
    chunk: &plan::FileChunk,
    max_rows: usize,
) -> D2DResult<()> {
    use std::io::{BufReader, Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(chunk.start))?;
    let mut reader = BufReader::new(f.take(chunk.end.saturating_sub(chunk.start)));
    let parser = ChunkParser::new(schema, logical, opts);
    loop {
        // `skip_header = false`：`chunk.start` **已经在列头之后**（见 `FileChunk` 的注释）。
        // 这里曾经传 true，于是列头被跳过两次、第一行数据被吃掉。
        let (batch, rows) = parser.parse_rows(&mut reader, max_rows, false)?;
        if rows == 0 {
            break;
        }
        if tx.blocking_send(Ok(batch)).is_err() {
            break; // 下游断开
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl DamengReadMethod for FlDrMethod {
    fn kind(&self) -> crate::source::method::DamengMethodKind {
        crate::source::method::DamengMethodKind::FlDr
    }

    fn exported_rows(&self) -> Option<u64> {
        *self.exported_rows.lock().unwrap()
    }

    fn physical_format(&self, ctx: &DamengReadContext) -> Option<PhysicalFormat> {
        let (dialect, charset) = self.export_dialect()?;
        Some(PhysicalFormat::csv(
            dialect,
            charset,
            ctx.column_names.clone(),
        ))
    }

    /// 直通面读一份产物
    ///
    /// 与逻辑面走的是同一个 `export_table` / `export_shard`（两条路径只会走一条），
    /// 差别只在产出：那边解析成批次，这边把产物**按行对齐切块**吐出去。
    async fn read_physical(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<PhysicalStream> {
        match split.split_info.get("kind").and_then(|v| v.as_str()) {
            Some("dmfldr_shard") => {
                // **不等导出结束**：dmfldr 边导边写普通文件（实测首个 4MB 约 0.3s
                // 就落盘），跟随读让首个 sink 块不必再等整片导完。
                let (data_path, done) = self.export_shard_streaming(split).await?;
                // 块大小与 sink 批次的关系见 `FlDrParams::physical_block_mb`：
                // 块 < 批次会把多个 shard 的块拼进同一条 Stream Load（跨多个日期分区），
                // 实测单请求慢约 50%。
                let mb = 1024 * 1024;
                let block = match self.params.physical_block_mb {
                    0 => physical::PHYSICAL_BLOCK_BYTES,
                    n => n as usize * mb,
                };
                let first = match self.params.first_block_mb {
                    0 => block,
                    n => n as usize * mb,
                };
                debug!(
                    split = %split.split_id,
                    block_mb = block / mb,
                    first_block_mb = first / mb,
                    "dmfldr: 流式物理块大小"
                );
                // 分片模式的产物不带列头（SQL 模式没有控制文件），整文件就是数据
                physical::open_streaming(data_path, 0, done, split.split_id.clone(), block, first)
            }
            Some("dmfldr_table") => {
                let (data_file, header_end) = self.export_table(ctx, split).await?;
                let len = std::fs::metadata(&data_file).map(|m| m.len()).unwrap_or(0);
                // 从**数据起点**开始（列头之后）—— 区间本身就是"数据区"，没人再跳列头
                physical::open_range(data_file, header_end, len, split.split_id.clone())
            }
            other => Err(Self::unknown_kind(split, other)),
        }
    }

    /// 规划分片 —— **只规划，一份产物都不出**
    ///
    /// | 模式 | 返回几片 |
    /// |---|---|
    /// | `TABLE=`（不分片） | **1 片**：整表一次导出、一个任务 |
    /// | 分片模式 | `partition` 切出几片就几片 |
    ///
    /// **导出不在这里**（曾经在）。券是任务生命的起点 —— 内核发券 → 取数 → 进池 → 还券，
    /// 而导出是唯一碰库的动作；留在 `split()` 里它就在券外，等于绕过了上限。
    async fn split(
        &self,
        spec: &SourceSpec,
        ctx: &DamengReadContext,
    ) -> D2DResult<Vec<SourceSplit>> {
        let table = &spec.table;
        let dir = proc::make_export_dir(&self.work_dir(), table)?;

        // ── 分片模式（partition.mode != none）──
        // 单进程 TABLE= 只能整表拉（实测 1915 万行 122s），按分区列切成 N 片并发实测 20s。
        if self.export_mode == ExportMode::Partitioned {
            return self.plan_shard_splits(spec, ctx, dir).await;
        }

        // ── TABLE=：**一个任务** ──
        // 这里只写控制文件、把路径交出去；导出本身住在 `read()` 里（见上）。
        let data_file = dir.join(format!("{}.txt", sanitize(table)));
        let ctl_path = ctl::write_control_file(
            ctx,
            spec.schema_name.as_deref(),
            table,
            &dir,
            &data_file,
            &self.params,
            self.params.effective_task_threads(),
        )?;
        self.export_dirs.lock().unwrap().push(dir.clone());

        info!(
            table = %table,
            columns = ctx.column_names.len(),
            export_dir = %dir.display(),
            parse_parallel = self.params.parse_parallel,
            "dmfldr: TABLE= 模式规划完成（1 片 = 1 个任务；产物内部的解析并行度是连接器自己的事）"
        );

        Ok(vec![SourceSplit {
            split_id: format!("{}_table", sanitize(table)),
            split_info: serde_json::json!({
                "kind": "dmfldr_table",
                "dir": dir.to_string_lossy(),
                "ctl": ctl_path.to_string_lossy(),
                "data": data_file.to_string_lossy(),
                "max_rows": self.fetch_size(spec),
            }),
        }])
    }

    /// 读一个分片 —— **两种分片的取数动作都在这里，因为券要包住它**
    ///
    /// | `kind` | 这一次 `read` 干什么 |
    /// |---|---|
    /// | `dmfldr_table` | 导出整表（一个进程、一个产物）→ 解析 |
    /// | `dmfldr_shard` | 导出本片（一个进程、一个产物）→ 解析 |
    ///
    /// 两条只差"产物怎么来"，之后是同一件事。
    async fn read(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<RecordBatchStream> {
        let max_rows = self.max_rows_of(split);
        match split.split_info.get("kind").and_then(|v| v.as_str()) {
            Some("dmfldr_shard") => {
                // SQL 模式不生成控制文件，走 dmfldr 缺省：分隔符 |、空值=空串、不转义。
                // 解析选项必须与导出侧实际行为一致，否则字段切分/空值判定全错。
                //
                // 缺省 `parse_parallel = 1` 走**流式**：导出还在写就开始解析，
                // 首块不必等整片导完。>1 需要预先知道文件总长来切区间，只能回退到
                // 「等导出完成」—— 这是显式调大并行度的代价，不是默认行为。
                if self.params.parse_parallel.max(1) == 1 {
                    let (data_path, done) = self.export_shard_streaming(split).await?;
                    let opts =
                        TextParseOptions::sql_mode_defaults(&data_path, self.params.character_code);
                    // 分片模式的产物不带列头 ⟹ 数据从 0 开始
                    self.parse_export_streaming(ctx, data_path, 0, opts, max_rows, done)
                } else {
                    let data_path = self.export_shard(split).await?;
                    let opts =
                        TextParseOptions::sql_mode_defaults(&data_path, self.params.character_code);
                    self.parse_export(ctx, data_path, 0, opts, max_rows)
                }
            }
            Some("dmfldr_table") => {
                let (data_file, header_end) = self.export_table(ctx, split).await?;
                // 控制文件在 TABLE= 模式下是生成了的，解析选项必须跟着它走
                let opts = TextParseOptions::from_params(&self.params, &data_file);
                self.parse_export(ctx, data_file, header_end, opts, max_rows)
            }
            other => Err(Self::unknown_kind(split, other)),
        }
    }

    async fn close(&self) -> D2DResult<()> {
        let dirs = std::mem::take(&mut *self.export_dirs.lock().unwrap());
        for d in dirs {
            proc::cleanup_dir(&d);
        }
        Ok(())
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use d2d_types::LogicalType;

    fn method_table() -> FlDrMethod {
        FlDrMethod::new(FlDrParams::default(), dummy_conn(), ExportMode::Table)
    }

    /// 不认识的 `kind` → 报错时要说清"应该有哪几种"
    ///
    /// 曾经这里认的是 `dmfldr_chunk`（内核切块时代的产物）。那个 kind 已经不存在了 ——
    /// 切块现在是**连接器内部**的事，`SourceSplit` 里不再出现它。
    #[tokio::test]
    async fn test_read_rejects_unknown_split_kind() {
        let m = method_table();
        let ctx = dummy_ctx();
        let split = SourceSplit {
            split_id: "s".into(),
            split_info: serde_json::json!({"kind": "dmfldr_chunk"}),
        };
        // `RecordBatchStream` 不是 Debug，`unwrap_err` 用不了
        let err = match m.read(&ctx, &split).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("不认识的 kind 应该报错"),
        };
        assert!(err.contains("dmfldr_table"), "要列出合法取值: {}", err);
        assert!(err.contains("dmfldr_shard"), "要列出合法取值: {}", err);
    }

    /// 造一份"像导出产物"的文本：`rows` 行，两列，行足够长以保证能被切成多块
    fn write_export(rows: usize) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        // 每行 ~155 字节：够大，5 千行就能跨过 `plan_chunks` 的多块门槛
        let padding = "x".repeat(150);
        let mut s = String::from("id|name\n");
        for i in 0..rows {
            s.push_str(&format!("{}|n{}{}\n", i, i, padding));
        }
        std::fs::write(&path, s).unwrap();
        (dir, path)
    }

    /// 解析一份产物，返回交出去的总行数
    async fn drain_export(parallel: usize, path: &Path, ctx: &DamengReadContext) -> usize {
        use futures::StreamExt;
        let params = FlDrParams {
            parse_parallel: parallel,
            ..Default::default()
        };
        let m = FlDrMethod::new(params.clone(), dummy_conn(), ExportMode::Table);
        let header_end = plan::read_header(path, b"|", &ctx.column_names).unwrap().0;
        let opts = TextParseOptions::from_params(&params, path);
        let mut stream = m
            .parse_export(ctx, path.to_path_buf(), header_end, opts, 500)
            .unwrap();
        let mut total = 0;
        while let Some(b) = stream.next().await {
            total += b.unwrap().num_rows();
        }
        total
    }

    /// **解析是连接器内部的事**：`parse_parallel` 只改内部怎么切，
    /// 不改交出去的东西 —— 内核看到的永远是**一条流**、同样的行数。
    ///
    /// 这条守的正是"别让内核去切块"：如果哪天有人把并行度又抬回拓扑里
    /// （`split()` 返回 N 片），这个测试仍然会过 —— 但它旁边那条
    /// `test_table_mode_is_one_split` 会挂。
    #[tokio::test]
    async fn test_parse_export_row_count_is_independent_of_parallelism() {
        let (_dir, path) = write_export(5000);
        let ctx = dummy_ctx();

        // 先确认这份产物**真的会被切成多块**（否则这条测试什么也没测）
        let header_end = plan::read_header(&path, b"|", &ctx.column_names).unwrap().0;
        let chunks = plan::plan_chunks(&path, 4, header_end).unwrap();
        assert!(
            chunks.len() > 1,
            "样本要跨多块才有意义，实际 {} 块",
            chunks.len()
        );

        assert_eq!(
            drain_export(1, &path, &ctx).await,
            5000,
            "顺序解析：一行不多一行不少"
        );
        assert_eq!(
            drain_export(4, &path, &ctx).await,
            5000,
            "并行解析：行数必须一致"
        );
    }

    /// **回归：列头只能跳过一次。**
    ///
    /// `plan_chunks` 切出来的块**起点已经在列头之后**，而解析侧又传了
    /// `skip_header: true` —— 两处各自表达同一件事，于是第一行数据被当成列头吃掉。
    /// 分片模式因为 `header_end = 0` 侥幸没事，这个坑只在 TABLE 模式露出。
    #[tokio::test]
    async fn test_header_is_skipped_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.txt");
        // 1 行列头 + 3 行数据：少一行一眼看得出来
        std::fs::write(&path, "id|name\n1|a\n2|b\n3|c\n").unwrap();
        let ctx = dummy_ctx();
        assert_eq!(drain_export(1, &path, &ctx).await, 3, "三行数据一行不能少");
    }

    /// `TABLE=` 模式**就是一片**：产物的大小不该再变成"内核派几个任务"
    ///
    /// 内核侧从此没有"chunk"这个概念 —— 一个任务、一份产物，
    /// 产物内部怎么切是连接器自己的事。
    #[tokio::test]
    async fn test_table_mode_is_one_split_whatever_its_size() {
        let mut spec = dmfldr_spec_for_split();
        spec.partition = Some(d2d_connector::PartitionSpec {
            mode: d2d_connector::PartitionMode::None,
            ..Default::default()
        });
        let ctx = dummy_ctx();
        let m = method_table();
        let splits = m.split(&spec, &ctx).await.unwrap();
        assert_eq!(splits.len(), 1, "TABLE= 模式的任务数恒为 1（数据不切）");
        assert_eq!(splits[0].split_info["kind"], "dmfldr_table");
        // 导出**不在这里发生**：产物路径由 split 交出，导出住在 read 里
        assert!(splits[0].split_info.get("data").is_some());
        assert!(
            !Path::new(splits[0].split_info["data"].as_str().unwrap()).exists(),
            "split() 只规划不导出 —— 文件此刻不该存在"
        );
    }

    fn dmfldr_spec_for_split() -> SourceSpec {
        serde_json::from_value(serde_json::json!({
            "connector": "dameng",
            "connection": {"host": "h", "user": "u", "password": "p"},
            "method": "dmfldr",
            "table": "T_A",
            "type_contract": [
                {"column": "id", "source_type": "BIGINT", "sink_type": "BIGINT"},
                {"column": "name", "source_type": "VARCHAR(20)", "sink_type": "VARCHAR(50)"}
            ],
            "query": {"columns": ["id", "name"]}
        }))
        .unwrap()
    }

    fn dummy_conn() -> ConnectionConfig {
        ConnectionConfig::from_json(&serde_json::json!({"host": "h", "user": "u"})).unwrap()
    }

    fn dummy_ctx() -> DamengReadContext {
        let (schema, logical_types) = {
            let lts = vec![LogicalType::Int64, LogicalType::Utf8];
            let names = ["id".to_string(), "name".to_string()];
            let fields: Vec<arrow::datatypes::Field> = names
                .iter()
                .zip(lts.iter())
                .map(|(n, lt)| {
                    arrow::datatypes::Field::new(
                        n,
                        d2d_types::LogicalArrowBridge::to_arrow(lt).unwrap(),
                        true,
                    )
                })
                .collect();
            (
                std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
                lts,
            )
        };
        DamengReadContext {
            schema,
            logical_types,
            column_names: vec!["id".into(), "name".into()],
        }
    }
}
