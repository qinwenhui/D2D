//! `flow`：执行拓扑与它的配置面
//!
//! # 拓扑
//!
//! ```text
//! splits ──► 派发器 ──► [source_pool] ──► 派发器 ──► [sink_pool] ──► 派发器 ──► 目标端
//!            ×N_s          容量 C_s         ×N_p          容量 C_k         ×N_k
//! ```
//!
//! 三段各有各的并发度，**互不推导**：
//!
//! | 段 | 并发度管什么 | 谁的能量决定它 |
//! |---|---|---|
//! | `source.concurrency` | 同时几个取数单元压在源库上 | **源库能接几个** ← 运维知道 |
//! | `pipeline.concurrency` | 同时几份数据在解析/转换 | 本机 CPU |
//! | `sink.concurrency` | 同时几个导入压在目标端 | **目标端能接几个** ← 运维知道 |
//!
//! # 三档是**同一个形状**：取一份数据 → 取一张券 → spawn 一个任务
//!
//! ```text
//! loop {
//!     let unit = 上游取一份数据单元;          // 数据决定"有几个任务"
//!     let _permit = slots.acquire().await;    // concurrency 决定"同时几个"
//!     spawn(async move { let _p = _permit; 干这一份 });
//! }
//! ```
//!
//! 这里有两句话，**每一句都有牙**：
//!
//! ```text
//! concurrency = 上限（闸门）     ← 卡"同一时刻最多几个任务在跑"
//! 任务数       = 数据决定         ← 动态，和上限没有任何关系
//! ```
//!
//! - **数据少就只产生少数任务**：池里 100 MB、`batch_bytes` 60 MB ⟹ 只开 2 个任务，
//!   而不是"配了 8 路就开 8 个"（8 路只是说**最多**同时 8 个）
//! - **数据多就产生成百上千个任务**，全被闸门卡住并发
//!
//! 配置项叫 `concurrency` 而不是 `tasks` —— 命名本身就把语义写死了。
//!
//! | 段 | 一个**任务**是什么 | 任务数由什么决定 |
//! |---|---|---|
//! | source | 一个分片（`read(split)` 整个住在里面：dmfldr 的导出也在） | `partition` 切出多少片 |
//! | pipeline | 从 `source_pool` 取**一份**（≤ `batch_bytes`）搬到 `sink_pool` | 池里数据量 ÷ `batch_bytes`（动态） |
//! | sink | 从 `sink_pool` 取**一份**（≤ `sink.batch_bytes`）写下去 | 池里数据量 ÷ 攒批量（动态） |
//!
//! **顺序是"先取券、再取数据"，不能反。** 反过来（先 pop 再 acquire）看着一样，
//! 实则差两件事：
//!
//! 1. 派发器会**攥着一份已经出池的数据等券**，而前一个任务攒批时（`try_pop`）
//!    会跳过它 —— 于是 `concurrency: 1` 这种"本该严格有序"的配置也能乱序
//!    （`topology.rs::test_direct_path_passes_bytes_verbatim_in_order` 抓的就是它）。
//! 2. 内存账上会多出一份（`concurrency × batch_bytes` 之外的那一份）。
//!
//! 先取券则两件事都没有：派发器从不持有数据，拿不到券就停在 `acquire()` 上，
//! 于是**也不再从上游取下一份** —— 内存上界就靠这一句。
//!
//! # 攒批**住在内核**，不在连接器里
//!
//! `batch_bytes` = 内核一次递给连接器的数据上界。连接器**内部不再攒批** ——
//! 这条分界解决的是一个很具体的问题：连接器内部攒着不发时，"攒着的那点内存"
//! 不在任何池的账上，而"源端停下来时最后一批一直吊着"要靠一个空闲超时去救。
//! 搬到池上之后，两个问题一起消失（那个 `flush_interval_ms` 就是这么没的）。
//!
//! **但"攒"的脾气两档不同**，因为两档的瓶颈不同：
//!
//! | 档 | 怎么攒 | 为什么 |
//! |---|---|---|
//! | pipeline | `gather`：**取到多少拿多少**，池里一空就走 | 吃 CPU，等它攒批的收益没有实测支持 |
//! | sink | `gather_until_full`：**接着等，攒够才发** | 每次写入贵在目标端的事务上、几乎与大小无关 ⟹ **单元数直接决定耗时** |
//!
//! 实测代价（同一份数据、`batch_bytes: 100MB`）：sink"有多少发多少"是 108 个单元、
//! "攒够才发"是 96 个，而 Stream Load 平均每次 ~5.1 s ⟹ 单元数多 12% 就是耗时多 12%。
//! 数据见 基准实测「并发模型重构 A/B」。
//!
//! sink 那条"等"的**收尾出口不是超时，是池关闭**（= 不会再有数据进来了，
//! 见下面那个 `gather_until_full`）—— 所以它不需要任何空闲超时旋钮。
//!
//! # 内存不是并发的因
//!
//! 曾经的实现是「内存额度 ÷ 每单元常驻 = 并发数」。那条路被删掉了，因为它把
//! **用户能感知的东西**（数据库能接几个连接）换成了**用户感知不到的黑盒**：
//!
//! > 内存不够可以溢出到磁盘。你把我并发砍了，我想达到的 CPU 密集取数就断开了。
//! > 我如果觉得内存不够，我会加内存 —— 但你给我的是"加了内存才能跑"，
//! > 那我永远不会再去加内存，而我以为那就是正常速度。
//!
//! 现在的关系是反的：
//!
//! - **并发由用户配置**（`*.concurrency`），填多少就是多少
//! - **内存是账本**（`engine.memory_mb`）：启动期把下面几项加起来给用户看，
//!   超了**拒绝启动并指明超在哪一项**；不够就让池溢出，而不是偷偷砍并发
//!
//! # 内存的六个来源（**互不重叠**）
//!
//! | # | 项 | 量 |
//! |---|---|---|
//! | ① | 源端取数单元 | `source.concurrency × 单元常驻`（dmfldr 是子进程，闸门够不着） |
//! | ② | source_pool | `source.pool.capacity` |
//! | ③ | pipeline 工作集 | `pipeline.concurrency × 单批字节` |
//! | ④ | 转换中间态 | 与 ③ 同量级（cast 变宽、投影新建 buffer） |
//! | ⑤ | sink_pool | `sink.pool.capacity` |
//! | ⑥ | sink 工作集 | `sink.concurrency × sink.batch_bytes` |
//!
//! ③④ 就是"pipeline 的内存参数" —— **它由并发数与批次大小共同决定，不需要第七个旋钮**。
//! pipeline 坐在两个池中间，手里最多只持**一批**，所以它没有自己的池。
//!
//! ③⑥ 的系数就是 `concurrency`，**不加一** —— 派发器先取券再取数据，
//! 手里从不持有数据（见上文）。这条性质值得写下来：它曾经是错的
//! （先 pop 再 acquire 会让实际峰值变成 `concurrency + 1`，且顺序也会乱）。
//!
//! # 直通路径也在同一张拓扑里
//!
//! 直通不是"另一条链路"，只是 `pipeline_task` 那一格**什么都不做**：
//! 源端产出的本来就是目标端能直接吃的字节，没有可转换的东西。
//! 于是"直通是一个 task 干完全程"的那种并行体系被删掉了 ——
//! 它绕开三段结构，等于每个分片独占一份三段资源。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{debug, error, info};

use d2d_connector::{
    PhysicalBlock, PhysicalSession, SinkConnector, SinkSession, SinkSpec, SourceConnector,
    SourceSpec, SourceSplit, UnitFootprint,
};
use d2d_core::{D2DError, D2DResult, RecordBatchExt};
use d2d_memory::{MemSnapshot, SlotPool};

use crate::audit::{RowAudit, Verdict};
use crate::plan::ExecutionPath;
use crate::pool::{DataPool, PoolConfig, PoolOverflow, PoolStats, SpillItem};

// ═══════════════════════════ 配置面 ═══════════════════════════

/// 字节数：接受 `"512MB"` / `"2GB"` / `"64M"` / 裸数字（字节）
///
/// **单位一律按 1024 进制**（与 `engine.memory_mb` 一致）。
/// 混用 1000/1024 会让"我明明给了 512MB 为什么超了"变成一笔糊涂账。
pub fn parse_bytesize(s: &str) -> Result<usize, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("字节数不能为空".into());
    }
    let split = t
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '_')
        .unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let num: f64 = num
        .replace('_', "")
        .parse()
        .map_err(|_| format!("'{}' 不是合法的数字", num))?;
    let mult: f64 = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1.0,
        "K" | "KB" | "KIB" => 1024.0,
        "M" | "MB" | "MIB" => 1024.0 * 1024.0,
        "G" | "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "T" | "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        other => {
            return Err(format!(
                "不识别的单位 '{}'（支持 B/K/KB/M/MB/G/GB/T/TB，按 1024 进制）",
                other
            ))
        }
    };
    Ok((num * mult) as usize)
}

fn de_bytesize<'de, D>(d: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Num(usize),
        Str(String),
    }
    match Raw::deserialize(d)? {
        Raw::Num(n) => Ok(n),
        Raw::Str(s) => parse_bytesize(&s).map_err(serde::de::Error::custom),
    }
}

/// 一个池的配置（`flow.*.pool`）
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSpec {
    /// 池里最多积压多少字节
    #[serde(deserialize_with = "de_bytesize")]
    pub capacity: usize,
    /// 池满时怎么办
    #[serde(default)]
    pub on_overflow: PoolOverflow,
    /// spill 文件上限；超过即退化为背压（`on_overflow: spill` 时才有意义）
    #[serde(default = "default_max_spill", deserialize_with = "de_bytesize")]
    pub max_spill: usize,
    /// spill 目录；缺省用系统临时目录
    #[serde(default)]
    pub spill_dir: Option<PathBuf>,
}

fn default_max_spill() -> usize {
    4 * 1024 * 1024 * 1024
}

impl PoolSpec {
    fn to_pool_config(&self, name: &'static str) -> PoolConfig {
        PoolConfig {
            name,
            capacity_bytes: self.capacity,
            overflow: self.on_overflow,
            max_spill_bytes: self.max_spill,
            spill_dir: self.spill_dir.clone(),
        }
    }
}

fn default_source_pool() -> PoolSpec {
    PoolSpec {
        capacity: 512 * 1024 * 1024,
        on_overflow: PoolOverflow::Backpressure,
        max_spill: default_max_spill(),
        spill_dir: None,
    }
}

fn default_sink_pool() -> PoolSpec {
    default_source_pool()
}

/// 源端一段
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceStage {
    /// 同时几个取数单元（= 源库那边能接几个连接 / 几个导出进程）。0 = 跟随 `pipeline.concurrency`
    #[serde(default)]
    pub concurrency: usize,
    #[serde(default = "default_source_pool")]
    pub pool: PoolSpec,
}

impl Default for SourceStage {
    fn default() -> Self {
        Self {
            concurrency: 0,
            pool: default_source_pool(),
        }
    }
}

/// 处理一段
///
/// **没有 pool** —— pipeline 坐在两个池中间，手里最多只持一批。
/// 它的内存由 `concurrency × batch_bytes` 表达，见模块文档。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct PipelineStage {
    /// 同时几个处理 task。0 = CPU 核数
    #[serde(default)]
    pub concurrency: usize,
    /// 单个处理单元的字节上限。
    ///
    /// task 从 `source_pool` 取到第一个元素后，**顺手把此刻已经排队的**其它元素
    /// 一并取走，累积到这个字节数就不再贪多（不会为凑满而空等）。
    ///
    /// **0（缺省）= 不累积**：源端给多大就是多大，一个不多取、零额外拷贝，
    /// 且内存账可以用"源端单批多大就多大"精确算出来。
    /// 设成非零值 = 用**一次 `concat_batches` 的拷贝**换更少的算子调用次数。
    /// 这是纯优化项，不是必需项 —— 所以缺省关着。
    #[serde(default, deserialize_with = "de_bytesize")]
    pub batch_bytes: usize,
    /// 单批行数上限（可选兜底；0 = 不限）。
    ///
    /// 窄表按字节攒批会攒出巨量行，有些算子逐行处理会退化 —— 设了它就能兜住。
    #[serde(default)]
    pub batch_rows: usize,
}

/// 不攒批时，**一个处理单元大概占多少字节** —— 只用于内存账的估算
///
/// 取 4 MB：这是 dmfldr 直通面的块大小（`PHYSICAL_BLOCK_BYTES`），
/// 也是逻辑面解析批次的典型量级。放在这里而不是引用具体连接器的常量，
/// 是因为 `d2d-execution` 不该依赖某个 source 实现。
///
/// ⚠️ 它**只影响账本估得准不准**，不影响任何运行时行为。
/// 估小了（比如源端单批其实有 50MB）会让账本偏乐观 —— 所以宁可保守。
pub const NOMINAL_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// 目标端一段
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkStage {
    /// 同时几个写入（= 目标端能接几个并发导入）。0 = 跟随 `pipeline.concurrency`
    #[serde(default)]
    pub concurrency: usize,
    /// **内核一次递给连接器的数据上界**（字节；与 `batch_rows` 谁先到谁触发）
    ///
    /// 注意它的名字说的是"内核**给**多少"，不是"连接器**攒**多少" ——
    /// 攒批住在内核（本模块文档「攒批住在内核」一节），连接器收到即写。
    ///
    /// # 调小它会亏，而且**不是线性的**
    ///
    /// 每次写入有一份**与大小无关的固定开销**（目标端开事务、提交、往返延迟）。
    /// 单元小到一定程度，固定开销就占了大头 —— 那时再多写几个单元也不会更快，
    /// 只是白付固定开销。实测（`.39`，19,151,537 行 / 37 列 / Doris Stream Load，
    /// 见 基准实测「机制：sink 是瓶颈时…」）：
    ///
    /// | 单元大小 | 单元数 | 墙钟 |
    /// |---|---|---|
    /// | ~70 MB | 108–113 | 86 s |
    /// | **~90–100 MB** | 76–96 | **74–78 s** |
    /// | ~102 MB（512 MB 档） | 29–64 | 74–75 s |
    ///
    /// 拐点在 **~90 MB** 上下：过了它，每次写入的成本从"事务开销主导"转成
    /// "字节吞吐主导"，再放大就不再有收益。**缺省 100 MB 就落在甜区**，
    /// 没有特别理由别往下调。
    ///
    /// ⚠️ 反过来说，**调大也不总是更快**：`batch_bytes` 只决定"一次递多少"，
    /// 真到瓶颈在别处（取数、解析）时，把 100 MB 调到 512 MB 一点用没有 ——
    /// 实测两档墙钟都是 ~74 s。
    #[serde(default = "default_sink_batch_bytes", deserialize_with = "de_bytesize")]
    pub batch_bytes: usize,
    /// 一次写入的行数上限（**与 `batch_bytes` 谁先到谁触发**）。
    ///
    /// 两个都要的理由是对称的：只按行，宽表会在内存里堆出几百 MB；
    /// 只按字节，窄表可能攒出上千万行才凑满，一次导入失败重传的代价极高。
    #[serde(default = "default_sink_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_sink_pool")]
    pub pool: PoolSpec,
}

fn default_sink_batch_bytes() -> usize {
    100 * 1024 * 1024
}
fn default_sink_batch_rows() -> usize {
    200_000
}

impl Default for SinkStage {
    fn default() -> Self {
        Self {
            concurrency: 0,
            batch_bytes: default_sink_batch_bytes(),
            batch_rows: default_sink_batch_rows(),
            pool: default_sink_pool(),
        }
    }
}

/// 整条链路的拓扑配置（YAML 的 `flow:` 段）
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowSpec {
    #[serde(default)]
    pub source: SourceStage,
    #[serde(default)]
    pub pipeline: PipelineStage,
    #[serde(default)]
    pub sink: SinkStage,
}

// ─────────────────── 内存账（启动期校验，不是运行期推导） ───────────────────

/// 账本里的一行
#[derive(Debug, Clone)]
pub struct MemoryLine {
    pub what: &'static str,
    pub bytes: usize,
    pub note: String,
}

/// 全链路内存账
#[derive(Debug, Clone)]
pub struct MemoryAccount {
    pub lines: Vec<MemoryLine>,
    pub total: usize,
    /// 其中**属于源端子进程**的那部分（dmfldr 的 N × buffer_node_size）
    ///
    /// 单列出来是为了能同口径比对：进程自报的 RSS **不含**子进程，
    /// 拿含子进程的总额去比会把差值算小，看着像账本很准 —— 那是假的。
    pub child_process_bytes: usize,
}

impl MemoryAccount {
    pub fn mb(&self) -> usize {
        self.total / 1048576
    }

    /// **本进程内**的那部分（总额减去源端子进程）—— 用来和进程自报 RSS 同口径比对
    pub fn in_process_mb(&self) -> usize {
        self.total.saturating_sub(self.child_process_bytes) / 1048576
    }

    pub fn child_process_mb(&self) -> usize {
        self.child_process_bytes / 1048576
    }

    /// 人类可读的多行明细（启动日志与报错都用它，**每一行都要能对上号**）
    pub fn render(&self) -> String {
        let mut s = String::new();
        for l in &self.lines {
            s.push_str(&format!(
                "\n    {:>6} MB  {:<16} {}",
                l.bytes / 1048576,
                l.what,
                l.note
            ));
        }
        s.push_str(&format!("\n    {:>6}     {:<16}", "------", "合计"));
        s.push_str(&format!("\n    {:>6} MB", self.mb()));
        s
    }
}

impl FlowSpec {
    /// 三个并发度**解析后的实际值**（`0` 的跟随语义在这里落地）
    ///
    /// 跟随规则：pipeline 缺省 = CPU 核数；source/sink 缺省 = 跟随 pipeline。
    /// 这是**缺省值互相参照**，不是"运行时推导"—— 填了就完全按填的来。
    pub fn resolve(&self) -> ResolvedFlow {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pipeline = if self.pipeline.concurrency > 0 {
            self.pipeline.concurrency
        } else {
            cpus
        };
        let source = if self.source.concurrency > 0 {
            self.source.concurrency
        } else {
            pipeline
        };
        let sink = if self.sink.concurrency > 0 {
            self.sink.concurrency
        } else {
            pipeline
        };
        ResolvedFlow {
            source,
            pipeline,
            sink,
            source_explicit: self.source.concurrency > 0,
            pipeline_explicit: self.pipeline.concurrency > 0,
            sink_explicit: self.sink.concurrency > 0,
        }
    }

    /// 把全链路常驻内存拆成互不重叠的六项
    ///
    /// `has_transforms` 决定要不要算"转换中间态"那一项 —— 直通没有转换，
    /// 算了就是虚报，会让用户以为额度不够而白白加内存。
    pub fn memory_account(
        &self,
        r: &ResolvedFlow,
        unit: UnitFootprint,
        has_transforms: bool,
    ) -> MemoryAccount {
        let mut lines = Vec::new();

        // ① 源端取数单元（dmfldr 是子进程，**内存闸门够不着**，必须单独记）
        if unit.bytes_each > 0 {
            // 一个取数单元可能**两部分都有**（dmfldr：外面的导出子进程 + 本进程里
            // 那份解析缓冲），所以分开报 —— 报一个数会让"本进程口径"算歪。
            let local = unit.bytes_each.saturating_sub(unit.child_bytes_each);
            let where_ = match (unit.child_bytes_each, local) {
                (c, l) if c > 0 && l > 0 => format!(
                    "，其中子进程 {} MB + 本进程内 {} MB",
                    c / 1048576,
                    l / 1048576
                ),
                (c, _) if c > 0 => "，独立于本进程".to_string(),
                _ => "，在本进程内".to_string(),
            };
            lines.push(MemoryLine {
                what: "源端取数单元",
                bytes: r.source * unit.bytes_each,
                note: format!(
                    "{} × {} MB（{}{}）",
                    r.source,
                    unit.bytes_each / 1048576,
                    unit.kind.describe(),
                    where_
                ),
            });
        } else {
            lines.push(MemoryLine {
                what: "源端取数单元",
                bytes: 0,
                note: format!(
                    "{} 个{}（进程内，不额外占用）",
                    r.source,
                    unit.kind.describe()
                ),
            });
        }

        // ② source_pool
        lines.push(MemoryLine {
            what: "source_pool",
            bytes: self.source.pool.capacity,
            note: format!("池满处置：{}", self.source.pool.on_overflow.describe()),
        });

        // ③ pipeline 工作集：并发 × 单批。**这就是 pipeline 的内存参数**
        //
        // `batch_bytes = 0`（不攒批）时没有显式上限，就按"源端单批典型大小"估。
        // 这一项是**估算**而非精确值 —— 但它在账本里的角色是"让用户看见这笔开销"，
        // 而不是拿去卡准入，所以估得保守（偏大）是对的。
        let per_batch = if self.pipeline.batch_bytes > 0 {
            self.pipeline.batch_bytes
        } else {
            NOMINAL_BATCH_BYTES
        };
        lines.push(MemoryLine {
            what: "pipeline 工作集",
            bytes: r.pipeline * per_batch,
            note: if self.pipeline.batch_bytes > 0 {
                format!(
                    "{} 个 task × {} MB（攒批上限）",
                    r.pipeline,
                    per_batch / 1048576
                )
            } else {
                format!(
                    "{} 个 task × {} MB（按源端单批估算，配 batch_bytes 可精确控制）",
                    r.pipeline,
                    per_batch / 1048576
                )
            },
        });

        // ④ 转换中间态（cast 变宽 / 投影新建 buffer）
        if has_transforms {
            lines.push(MemoryLine {
                what: "转换中间态",
                bytes: r.pipeline * per_batch,
                note: "算子链的中间结果，与工作集同量级".to_string(),
            });
        }

        // ⑤ sink_pool
        lines.push(MemoryLine {
            what: "sink_pool",
            bytes: self.sink.pool.capacity,
            note: format!("池满处置：{}", self.sink.pool.on_overflow.describe()),
        });

        // ⑥ sink 工作集：攒批缓冲还没落盘的那部分
        lines.push(MemoryLine {
            what: "sink 工作集",
            bytes: r.sink * self.sink.batch_bytes,
            note: format!(
                "{} 个写入 × {} MB 攒批",
                r.sink,
                self.sink.batch_bytes / 1048576
            ),
        });

        let total = lines.iter().map(|l| l.bytes).sum();
        // ① 里**属于子进程的那部分**才是本进程之外的；其余（含连接器的进程内缓冲、
        // 两个池、两段工作集、转换中间态）都在本进程里。按 `child_bytes_each` 算，
        // 而不是按"是不是 Process 形态"一刀切 —— 一个单元可以两部分都有。
        let child_process_bytes = r.source * unit.child_bytes_each.min(unit.bytes_each);
        MemoryAccount {
            lines,
            total,
            child_process_bytes,
        }
    }

    /// **启动期校验两个池的 spill 目录**（可写 + 清残骸）
    ///
    /// 与 [`Self::validate_memory`] 并列，都在 `--dry-run` 里跑得到 ——
    /// 配置错误必须在开工前说清楚，而不是等池第一次满的时候。
    /// 细节（为什么不能推迟、为什么只清"确定已死"的）见
    /// [`PoolConfig::validate_spill_dir`]。
    pub fn validate_spill_dirs(&self) -> D2DResult<()> {
        self.source
            .pool
            .to_pool_config("source")
            .validate_spill_dir()?;
        self.sink.pool.to_pool_config("sink").validate_spill_dir()?;
        Ok(())
    }

    /// **启动期账本校验**：六项加起来必须装进 `memory_mb`
    ///
    /// 超了**拒绝启动**并指明超在哪一项 —— 而不是偷偷把并发砍下来。
    /// 后者会让"我配了 32 路并发"变成"实际跑了 6 路而你永远不知道"。
    pub fn validate_memory(
        &self,
        r: &ResolvedFlow,
        unit: UnitFootprint,
        has_transforms: bool,
        memory_mb: usize,
    ) -> D2DResult<MemoryAccount> {
        let acct = self.memory_account(r, unit, has_transforms);
        let budget = memory_mb * 1048576;
        if acct.total > budget {
            let over = (acct.total - budget) / 1048576;
            return Err(D2DError::config(format!(
                "内存账超出：配置需要 {} MB，engine.memory_mb 只给了 {} MB（超 {} MB）。\
                 各项明细：{}\n    \n\
                 处置方式（**不是自动砍并发**）：\n    \
                 ① 调大 engine.memory_mb（并发是你要的，就该给够内存）；\n    \
                 ② 或调小某个池的 capacity（池只是缓冲，小一点不影响吞吐）；\n    \
                 ③ 或给池配 on_overflow: spill（把它溢到磁盘，并发一点不用降）。",
                acct.mb(),
                memory_mb,
                over,
                acct.render()
            )));
        }
        Ok(acct)
    }
}

/// 解析后的三档并发（把"0 = 跟随"落到具体数字）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedFlow {
    pub source: usize,
    pub pipeline: usize,
    pub sink: usize,
    pub source_explicit: bool,
    pub pipeline_explicit: bool,
    pub sink_explicit: bool,
}

impl ResolvedFlow {
    fn mark(explicit: bool) -> &'static str {
        if explicit {
            "显式配置"
        } else {
            "缺省跟随"
        }
    }

    pub fn log(&self, spec: &FlowSpec) {
        info!(
            source = format_args!("{} 路（{}）", self.source, Self::mark(self.source_explicit)),
            pipeline = format_args!(
                "{} 路（{}）",
                self.pipeline,
                Self::mark(self.pipeline_explicit)
            ),
            sink = format_args!("{} 路（{}）", self.sink, Self::mark(self.sink_explicit)),
            source_pool_mb = spec.source.pool.capacity / 1048576,
            source_pool_policy = spec.source.pool.on_overflow.describe(),
            sink_pool_mb = spec.sink.pool.capacity / 1048576,
            sink_pool_policy = spec.sink.pool.on_overflow.describe(),
            "执行拓扑（三段各自独立，互不推导）"
        );
    }
}

// ═══════════════════════════ 流动的元素 ═══════════════════════════

/// 在池子里流动的东西
///
/// 逻辑面流的是 Arrow 批次，直通面流的是原始字节块。**一个枚举覆盖两者**，
/// 是为了让两条路径共用同一张拓扑 —— 否则直通又会退化成"另一条链路"。
#[derive(Debug)]
pub enum FlowItem {
    Batch(arrow::array::RecordBatch),
    Block(d2d_connector::PhysicalBlock),
}

impl FlowItem {
    pub fn bytes(&self) -> usize {
        match self {
            Self::Batch(b) => b.estimated_memory_size(),
            Self::Block(b) => b.bytes.len(),
        }
    }

    pub fn rows(&self) -> usize {
        match self {
            Self::Batch(b) => b.num_rows(),
            Self::Block(b) => b.rows as usize,
        }
    }
}

impl SpillItem for FlowItem {
    fn heap_bytes(&self) -> usize {
        self.bytes()
    }

    fn spill_to(&self, w: &mut dyn std::io::Write) -> D2DResult<()> {
        // 一个字节的类型标记 + 载荷。有了它，spill 文件即使被人工翻出来
        // 也能一眼看出装的是哪一路的数据。
        match self {
            Self::Batch(b) => {
                w.write_all(&[0u8])
                    .map_err(|e| D2DError::internal(e.to_string()))?;
                <arrow::array::RecordBatch as SpillItem>::spill_to(b, w)
            }
            Self::Block(b) => {
                w.write_all(&[1u8])
                    .map_err(|e| D2DError::internal(e.to_string()))?;
                <d2d_connector::PhysicalBlock as SpillItem>::spill_to(b, w)
            }
        }
    }

    fn unspill_from(r: &mut dyn std::io::Read) -> D2DResult<Self> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)
            .map_err(|e| D2DError::internal(e.to_string()))?;
        match tag[0] {
            0 => Ok(Self::Batch(
                <arrow::array::RecordBatch as SpillItem>::unspill_from(r)?,
            )),
            1 => Ok(Self::Block(
                <d2d_connector::PhysicalBlock as SpillItem>::unspill_from(r)?,
            )),
            other => Err(D2DError::internal(format!(
                "spill 载荷标记 {} 不认识（文件损坏或版本不匹配）",
                other
            ))),
        }
    }
}

// ═══════════════════════════ 执行 ═══════════════════════════

/// 本次执行的读数
#[derive(Debug, Clone)]
pub struct FlowReport {
    /// 目标端确认写下的行数（直通面是 `NumberLoadedRows`；逻辑面是连接器确认收下的）
    pub rows_written: u64,
    /// **行数对账的三个数**（源端自报 / 我们读出来的 / 目标端确认的）+ 结论入口
    ///
    /// 判据只有一处（[`crate::audit::RowAudit::verdict`]）；这里只是把数带出来，
    /// 让上层能显示"对到头了 / 只对了一半 / 对不上"，而不是只知道一个行数。
    pub rows: RowAudit,
    pub splits: usize,
    pub source_pool: PoolStats,
    pub sink_pool: PoolStats,
    pub elapsed_ms: u64,
    /// **进程内存水位的实测峰值**（不是估算）
    ///
    /// 为什么要它：`engine.memory_mb` 那本账是**预测** —— 六项相加，
    /// 其中"源端取数单元""pipeline 工作集""转换中间态"都是按公式估的，
    /// 而连接器内部还有自己的缓冲（典型：ODBC 的行集是
    /// `行数 × 列数 × 每列字节`，宽表上几百 MB 一个连接）。
    ///
    /// 实测与预测**并排打出来**，差值就是"账本偏乐观多少"。没有这个读数，
    /// "配了 512 MB 却常驻 3 GB"就只能靠猜。
    pub peak_rss_bytes: Option<usize>,
    /// **连接器自报的运行期占用峰值**，一台尺子一行（见 [`MemSnapshot`]）
    ///
    /// 与 `peak_rss_bytes` 的分工：RSS 是**事实**但不告诉你钱花在哪；这一项是
    /// 连接器能**自己认领**的那部分。两者一减，剩下的就是谁也不认领的 ——
    /// Arrow 结构开销、分配器碎片、第三方库自己的缓存。那部分才是下一步要看的东西。
    ///
    /// ⚠️ 与账本**不可相加**：连接器报的缓冲可能与账本某一项重叠
    /// （ODBC 行集就同时是账本第 ①项），所以只做**并排展示**，不做求和。
    pub connector_memory: Vec<MemSnapshot>,
    /// **两道闸门的历史峰值占用** —— 判断"这个并发数是不是给大了"的依据
    ///
    /// 与数据池的峰值不同：池的峰值说的是"排队等待"（缓冲），
    /// 闸门的峰值说的是"**同时有几个任务在跑**"（源端那就是几个取数单元压在库上）。
    /// 峰值 ≪ 上限 = 这个数配大了；峰值 == 分片数 = 并发不是瓶颈（数据就那么点）。
    pub source_gate_peak: usize,
    pub sink_gate_peak: usize,
}

impl FlowReport {
    /// 连接器自报合计（跨多台尺子求和 —— 同一连接器内各尺子互不重叠）
    pub fn connector_reported_bytes(&self) -> usize {
        self.connector_memory.iter().map(|m| m.peak_bytes).sum()
    }
}

/// 进程常驻内存的**实测峰值**（内核维护的水位，不是采样，不会漏掉尖峰）
///
/// Linux 读 `/proc/self/status` 的 `VmHWM`；其它平台返回 `None`（不假装知道）。
pub fn process_peak_rss_bytes() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
                return Some(kb * 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// 全局控制：中止信号 + 把还卡在池上的任务唤醒
///
/// # 为什么必须带上"关池"这一步
///
/// 只置一个 `AtomicBool` 是不够的：任务可能正卡在 `pool.push()`（池满背压）
/// 或 `pool.pop()`（池空等待）里，它**走不到**循环顶部那句 `if abort { break }`。
/// 不关池就是"标志位置了，但没人看得到" —— 表现为整个进程挂死。
///
/// 关池能让两边的等待立刻返回（`push` 报错、`pop` 在排空后给 `None`），
/// 于是每个任务都能在有限步内退出。
struct Ctl {
    flag: AtomicBool,
    err: Mutex<Option<D2DError>>,
    source_pool: Arc<DataPool<FlowItem>>,
    sink_pool: Arc<DataPool<FlowItem>>,
}

impl Ctl {
    fn new(source_pool: Arc<DataPool<FlowItem>>, sink_pool: Arc<DataPool<FlowItem>>) -> Arc<Self> {
        Arc::new(Self {
            flag: AtomicBool::new(false),
            err: Mutex::new(None),
            source_pool,
            sink_pool,
        })
    }

    fn is_set(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }

    /// 记录**第一个**错误（后续的通常都是它的连锁反应，报第一个才有排查价值），
    /// 然后关掉两个池把卡在 push/pop 上的任务全部放行。
    fn fail(&self, e: D2DError) {
        {
            let mut g = self.err.lock().unwrap();
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.flag.store(true, Ordering::Relaxed);
        self.source_pool.close();
        self.sink_pool.close();
    }

    fn take(&self) -> Option<D2DError> {
        self.err.lock().unwrap().take()
    }
}

/// **同时在跑几个任务** —— 纯读数，用来判断"这个并发数是不是给大了"
///
/// # 为什么不用闸门自己的峰值
///
/// 派发器在"拿到券、还没 spawn 出任务"的那一小段里也握着券（[`SlotPool::peak`]
/// 是在 `acquire` 里记的）。池空时它会一直握着等下一份数据，于是峰值被多算一个 ——
/// 而"3 片数据配 16 路"这种情形恰好要能读出**3**（数据限制）而不是 4。
///
/// 读数要回答的问题是"最多有几个任务**真的**在跑"，所以由任务自己进出：
/// 进任务时 +1，任务结束（含 panic / 提前 return）时随守卫 -1。
#[derive(Default)]
struct TaskGauge {
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl TaskGauge {
    fn enter(self: &Arc<Self>) -> TaskGuard {
        let now = self.live.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        TaskGuard(Arc::clone(self))
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

/// 任务在跑期间持有它；drop 即出账
struct TaskGuard(Arc<TaskGauge>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 等一个派发器**派完**，再把它派出去的任务**全部等完**；返回每个任务的结果
///
/// 派发器自己不在返回值里报错 —— 出错时它把错误交给 [`Ctl`]，让整条链路一起收
/// （那样才能保证"派了但还没跑完的任务"也被等到）。所以这里只需要处理
/// **派发器 panic** 这一种 `JoinError`；真发生了就把它记进 `Ctl`，
/// 否则会变成"任务没跑完却报了成功"。
async fn drain_dispatched<T>(
    ctl: &Arc<Ctl>,
    dispatch: tokio::task::JoinHandle<Vec<tokio::task::JoinHandle<D2DResult<T>>>>,
) -> Vec<T> {
    let handles = match dispatch.await {
        Ok(h) => h,
        Err(e) => {
            error!(error = %e, "派发器 panic，无法等齐它派出的任务");
            ctl.fail(D2DError::internal(format!("派发器 panic: {}", e)));
            return Vec::new();
        }
    };
    let mut out = Vec::with_capacity(handles.len());
    for (i, h) in handles.into_iter().enumerate() {
        match h.await {
            Ok(Ok(v)) => out.push(v),
            // 任务自己的错误已经在失败那一刻交给 `Ctl` 了，这里不重复记 ——
            // `Ctl` 只保留**第一个**错误，重复记会让真正的首因被后来的连锁反应挤掉
            Ok(Err(_)) => {}
            Err(e) => {
                // panic 没有别的上报路径，必须在这里记下来：
                // 漏掉它就成了"任务没跑完却报了成功"
                error!(task = i, error = %e, "任务 panic");
                ctl.fail(D2DError::internal(format!("任务 {} panic: {}", i, e)));
            }
        }
    }
    out
}

/// 一个 `sink_task` 的收尾读数
///
/// 分成三项而不是一个总数，是因为**直通行数对账必须对全体求和**：
/// `sink.concurrency > 1` 时，任何单个 task 都只看到自己那一份源端行数
/// 与自己那条会话确认的行数，单看一个永远对不上。
#[derive(Debug, Clone, Copy, Default)]
struct SinkOutcome {
    /// 逻辑面：`write_batch` 报告已收下的行数；直通面：未确认，取 `confirmed`
    written: u64,
    /// 直通面：目标端确认落盘的行数（`NumberLoadedRows`）。逻辑面为 `None`
    confirmed: Option<u64>,
}

/// 逻辑面的写入会话 / 直通面的写入会话 —— 用一个枚举抹平
enum SinkHandle {
    Logical(Box<dyn SinkSession>),
    Physical(Box<dyn PhysicalSession>),
}

impl SinkHandle {
    async fn write(&mut self, item: FlowItem) -> D2DResult<u64> {
        match (self, item) {
            (Self::Logical(s), FlowItem::Batch(b)) => Ok(s.write_batch(b).await? as u64),
            // 字节块**按所有权**递下去：一次写入的载荷要一路交到 HTTP 层，
            // 而 `.body()` 要 `'static`。收所有权才谈得上零拷贝（见 `PhysicalSession`）。
            (Self::Physical(s), FlowItem::Block(b)) => s.write_block(b).await,
            (Self::Logical(_), FlowItem::Block(_)) => Err(D2DError::internal(
                "逻辑面会话收到字节块 —— 执行路径判定出错",
            )),
            (Self::Physical(_), FlowItem::Batch(_)) => Err(D2DError::internal(
                "直通会话收到 Arrow 批次 —— 执行路径判定出错",
            )),
        }
    }

    async fn commit(&mut self) -> D2DResult<u64> {
        match self {
            Self::Logical(s) => {
                s.commit().await?;
                Ok(0)
            }
            Self::Physical(s) => s.commit().await,
        }
    }

    async fn abort(&mut self) -> D2DResult<()> {
        match self {
            Self::Logical(s) => s.abort().await,
            Self::Physical(s) => s.abort().await,
        }
    }
}

/// 一次 flow 执行需要的外部输入
pub struct FlowRun<'a> {
    pub spec: &'a FlowSpec,
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn SinkConnector>,
    pub source_spec: SourceSpec,
    pub sink_spec: SinkSpec,
    pub transforms: d2d_transform::TransformChain,
    pub path: ExecutionPath,
    /// 直通的受限字节扫描开关（安全闸；逻辑面忽略）
    pub forbidden_scan: bool,
    /// 直通开工前整片预扫（多读一遍源文件，换"发之前就失败"）
    pub preflight_scan: bool,
    /// 行数对账失败是否中止（直通面自校验用）
    pub fail_on_row_mismatch: bool,
}

/// 按拓扑跑完整条链路
///
/// 这是**唯一**的执行入口：逻辑面与直通面走的是同一个函数，
/// 区别只在 `pipeline_task` 那一格做不做转换、以及写入会话是哪种。
pub async fn run_flow(run: FlowRun<'_>) -> D2DResult<FlowReport> {
    let start = Instant::now();
    let r = run.spec.resolve();
    let splits_seen = Arc::new(AtomicUsize::new(0));
    // 源端阶段真正读出来的行数 —— 对账里"② 搬运/写入侧"这一跳的左端
    let rows_read = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let direct = matches!(run.path, ExecutionPath::Direct { .. });
    let (format, forbidden) = match &run.path {
        ExecutionPath::Direct { format, forbidden } => (Some(format.clone()), forbidden.clone()),
        ExecutionPath::Logical { .. } => (None, Vec::new()),
    };

    // ── 池先建，控制块后建 ──
    //
    // 顺序不能反：控制块必须拿着两个池，才能在中止时 `close()` 它们 ——
    // 否则卡在 `push`/`pop` 上的任务收不到信号，整个进程挂死。
    let source_pool = DataPool::<FlowItem>::new(run.spec.source.pool.to_pool_config("source"));
    let sink_pool = DataPool::<FlowItem>::new(run.spec.sink.pool.to_pool_config("sink"));
    let ctl = Ctl::new(Arc::clone(&source_pool), Arc::clone(&sink_pool));

    // ── 三道闸门：每个派发器手里一道，容量 = 该档 `concurrency` ──
    //
    // 闸门卡的是**任务**，不是连接器内部的东西 —— 连接器看不见这个数字，
    // 它只要守"一次 `read` = 一个取数单元"的契约。
    //
    // （这里发过两次错牌：先是三个"券数 == 任务数"的池（纯空转），
    //   后来把闸门塞给连接器（内核伸手管连接器内部）。两次都删了，
    //   完整的错法与理由见 `d2d_memory::slot` 的模块文档。）
    let source_slots = SlotPool::new("source", r.source);
    let pipeline_slots = SlotPool::new("pipeline", r.pipeline);
    let sink_slots = SlotPool::new("sink", r.sink);

    // 三档"同时在跑几个任务"的读数（闸门管上限，尺子管读数，两件事分开）
    let source_gauge = Arc::new(TaskGauge::default());
    let pipeline_gauge = Arc::new(TaskGauge::default());
    let sink_gauge = Arc::new(TaskGauge::default());

    // ── ① 源端派发器：一个分片 = 一份数据单元 ──
    //
    // 分片清单一次拿到。**"导完一片交一片"不再需要一条专门的流式接口** ——
    // dmfldr 的导出已经住在连接器的 `read(split)` 里，于是每个分片任务
    // 自带"导出 + 解析 + 进池"，导出与下游写入的重叠就是任务之间的并发。
    let preflight = run.preflight_scan;
    let source_dispatch = {
        let source = Arc::clone(&run.source);
        let spec = run.source_spec.clone();
        let pool = Arc::clone(&source_pool);
        let ctl = Arc::clone(&ctl);
        let slots = Arc::clone(&source_slots);
        let gauge = Arc::clone(&source_gauge);
        let seen = Arc::clone(&splits_seen);
        let read_rows = Arc::clone(&rows_read);
        let forb = forbidden.clone();
        tokio::spawn(async move {
            let splits = match source.split(&spec).await {
                Ok(v) => v,
                Err(e) => {
                    error!(error = %e, "分片规划失败");
                    ctl.fail(e);
                    return Vec::new();
                }
            };
            debug!(splits = splits.len(), "分片规划完成（任务数由数据决定）");
            let mut handles = Vec::with_capacity(splits.len());
            for split in splits {
                if ctl.is_set() {
                    break;
                }
                // 取一张券 → 取一份数据单元 → spawn 一个任务。
                // 拿不到券时**停在 acquire 上**，于是不再往下取 —— 内存上界就靠这句。
                let permit = match slots.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        ctl.fail(e);
                        break;
                    }
                };
                seen.fetch_add(1, Ordering::Relaxed);
                let source = Arc::clone(&source);
                let pool = Arc::clone(&pool);
                let ctl = Arc::clone(&ctl);
                let forb = forb.clone();
                let gauge = Arc::clone(&gauge);
                let read_rows = Arc::clone(&read_rows);
                handles.push(tokio::spawn(async move {
                    // 券随任务走：任务结束（含提前 return）即归还
                    let _permit = permit;
                    let _running = gauge.enter();
                    // 派发器取到券之后、任务真正开跑之前，链路可能已经中止了。
                    // 这一句挡的是"中止之后还去开一个取数单元"（dmfldr 会真起一个子进程）。
                    if ctl.is_set() {
                        return Ok((0, 0));
                    }
                    debug!(split = %split.split_id, "source_task 取到分片");
                    let res = if direct {
                        read_split_physical(&source, &split, &pool, &ctl, preflight, &forb).await
                    } else {
                        read_split(&source, &split, &pool, &ctl).await
                    };
                    // 读出来多少行 —— 计数在**源端阶段**，与目标端确认数完全独立
                    if let Ok((_, r)) = &res {
                        read_rows.fetch_add(*r, Ordering::Relaxed);
                    }
                    match res {
                        Ok((n, rows)) => {
                            debug!(split = %split.split_id, items = n, rows, "source_task 完成");
                            Ok((n, rows))
                        }
                        Err(e) => {
                            error!(split = %split.split_id, error = %e, "source_task 失败");
                            ctl.fail(e.clone());
                            Err(e)
                        }
                    }
                }));
            }
            handles
        })
    };

    // ── ② 处理派发器：从 source_pool 取一份（≤ batch_bytes）搬到 sink_pool ──
    //
    // 直通面在这一格什么都不做（没有可转换的东西），但它仍然是**独立的一段** ——
    // 有自己的并发度、自己的池，而不是"一个 task 干完全程"。
    let pipeline_dispatch = {
        let in_pool = Arc::clone(&source_pool);
        let out_pool = Arc::clone(&sink_pool);
        let ctl = Arc::clone(&ctl);
        let slots = Arc::clone(&pipeline_slots);
        let gauge = Arc::clone(&pipeline_gauge);
        let chain = run.transforms.clone();
        let batch_bytes = run.spec.pipeline.batch_bytes;
        let batch_rows = run.spec.pipeline.batch_rows;
        tokio::spawn(async move {
            let mut handles = Vec::new();
            loop {
                if ctl.is_set() {
                    break;
                }
                // ⚠️ **先取券、再取数据**，顺序不能反。
                //
                // 反过来（先 pop 再 acquire）会让派发器**手里攥着一份数据**等券，
                // 而那份数据已经出池了 —— 于是前一个任务在攒批时（`try_pop`）
                // 会跳过它，在 `concurrency: 1` 这种"本该严格有序"的配置下
                // 也能把顺序打乱。先取券就没有这个空窗：派发器从不持有数据。
                //
                // # 那"券在手、却卡在 `pop()` 上"要不要紧？—— 不要紧
                //
                // 池空时这个循环确实会**握着一张券等数据**。曾经把它记成一处待收的
                // 口子（基准实测 里那句"有效并发是 7 而不是 8，折算约 1 s"）。
                // 现在看，那笔账算错了，两条理由：
                //
                // 1. **这张券没有别的竞争者。** 三道闸门各归各的派发器，`slots.acquire()`
                //    全仓只有三处调用，就是三个循环自己（`source` / `pipeline` / `sink`）。
                //    攥着不放**谁也挡不住** —— 它本来也只有这一个使用者。
                // 2. **卡在 `pop()` 上 = 池是空的。** 池空意味着此刻**没有数据等着被处理**，
                //    就算把券让出去，那条空出来的 lane 也无事可做。
                //
                // 顺带把那个"折算约 1 s"的算术也说清楚：`park_s` 是**派发器这一个实体**
                // 的等待秒数，除以 8 是拿单个实体的秒数当成了每条 lane 的秒数。
                //
                // 所以这里**不动**：顺序是 FIFO 正确性要求的，而它一分钱不花。
                // 真要收，"先等到有数据再取券"需要一个新的同步原语（只等不取的窥视），
                // 而收益是零 —— 那笔风险不值得冒。
                let permit = match slots.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        ctl.fail(e);
                        break;
                    }
                };
                let Some(first) = in_pool.pop().await else {
                    break;
                };
                let first = match first {
                    Ok(x) => x,
                    Err(e) => {
                        ctl.fail(e);
                        break;
                    }
                };
                let in_pool = Arc::clone(&in_pool);
                let out_pool = Arc::clone(&out_pool);
                let ctl = Arc::clone(&ctl);
                let chain = chain.clone();
                let gauge = Arc::clone(&gauge);
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    let _running = gauge.enter();
                    // 中止之后不再搬下一份（这一份已经出池了，丢掉即可 —— 中止路径不承诺数据）
                    if ctl.is_set() {
                        return Ok(());
                    }
                    let out = if direct {
                        Ok(first)
                    } else {
                        gather(first, &in_pool, batch_bytes, batch_rows)
                            .and_then(|items| apply_chain(&chain, items))
                    };
                    let out = match out {
                        Ok(x) => x,
                        Err(e) => {
                            error!(error = %e, "pipeline_task 处理失败");
                            ctl.fail(e.clone());
                            return Err(e);
                        }
                    };
                    if let Err(e) = out_pool.push(out).await {
                        // 池被关闭时的错误是"中止"的连锁反应，不该盖掉第一个错误
                        if !ctl.is_set() {
                            ctl.fail(e.clone());
                        }
                        return Err(e);
                    }
                    Ok(())
                }));
            }
            handles
            // 这里不关 sink_pool —— 收口交给"派发器派完 + 任务全退"的统一级联
        })
    };

    // ── ③ 目标端派发器：一份数据 = 一次写入 ──
    //
    // 攒批归**内核**：这里攒够 `sink.batch_bytes` 才开一条会话写出去。
    // 连接器内部不再攒批，于是它也就没有 `should_flush`、没有"空闲多久强制落盘" ——
    // 那个超时本来就是内部攒批的副产品（"没人来写"恰恰意味着"没人来查超时"），
    // 攒批搬到池上之后它结构性消失了。
    let sink_spec = run.sink_spec.clone();
    let sink_dispatch = {
        let sink = Arc::clone(&run.sink);
        let in_pool = Arc::clone(&sink_pool);
        let ctl = Arc::clone(&ctl);
        let slots = Arc::clone(&sink_slots);
        let gauge = Arc::clone(&sink_gauge);
        let fmt = format.clone();
        let forb = forbidden.clone();
        let batch_bytes = run.spec.sink.batch_bytes;
        let batch_rows = run.spec.sink.batch_rows;
        let scan = run.forbidden_scan;
        tokio::spawn(async move {
            let mut handles = Vec::new();
            let mut next_id = 0usize;
            loop {
                if ctl.is_set() {
                    break;
                }
                // 先取券、再取数据（理由同处理档：派发器手里不攥数据）
                let permit = match slots.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        ctl.fail(e);
                        break;
                    }
                };
                let Some(first) = in_pool.pop().await else {
                    break;
                };
                let first = match first {
                    Ok(x) => x,
                    Err(e) => {
                        ctl.fail(e);
                        break;
                    }
                };
                let id = next_id;
                next_id += 1;
                let sink = Arc::clone(&sink);
                let in_pool = Arc::clone(&in_pool);
                let ctl = Arc::clone(&ctl);
                let spec = sink_spec.clone();
                let fmt = fmt.clone();
                let forb = forb.clone();
                let gauge = Arc::clone(&gauge);
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    let _running = gauge.enter();
                    run_sink_task(
                        id,
                        sink,
                        spec,
                        first,
                        in_pool,
                        ctl,
                        fmt,
                        forb,
                        scan,
                        batch_bytes,
                        batch_rows,
                    )
                    .await
                }));
            }
            handles
        })
    };

    // ── 收口：级联关闭 ──
    //
    // 顺序是**级联**的：source 全退 → 关 source_pool → pipeline 取空后全退
    // → 关 sink_pool → sink 取空后全退。任何一处提前关闭都会截断数据。
    //
    // 每一档要等**两件事**才关下游的池：派发器**派完**（否则它还会 spawn 出新任务），
    // 以及已派出去的任务**全部退出**（否则池一关就截断数据）。
    let closer = {
        let source_pool = Arc::clone(&source_pool);
        let sink_pool = Arc::clone(&sink_pool);
        let ctl = Arc::clone(&ctl);
        tokio::spawn(async move {
            let src = drain_dispatched(&ctl, source_dispatch).await;
            debug!(
                tasks = src.len(),
                "全部 source_task 已退出，关闭 source_pool"
            );
            source_pool.close();

            let pipe = drain_dispatched(&ctl, pipeline_dispatch).await;
            debug!(
                tasks = pipe.len(),
                "全部 pipeline_task 已退出，关闭 sink_pool"
            );
            sink_pool.close();
        })
    };

    // sink 是链条末端：它的派发器要等 sink_pool 关闭排空才收工，
    // 而那是 closer 的级联一步步推过来的 —— 所以这里等它就是等整条链路跑完。
    let sink_results = drain_dispatched(&ctl, sink_dispatch).await;
    let _ = closer.await;

    // ── 汇总 ──
    //
    // 失败的 sink 任务（含 panic）都已经把错误交给 `Ctl` 了，所以这里先看它。
    if let Some(e) = ctl.take() {
        return Err(e);
    }
    let outcomes = sink_results;

    // ── 行数对账：**对全体求和**，判据只有一处（[`crate::audit`]）──
    //
    // 三个数各自独立（源端自报 / 我们读出来的 / 目标端确认的），
    // 两次比较各抓一类丢行。这里只负责**把数凑齐**并问那一处要结论 ——
    // 口径与成因文案都在 `audit.rs`，两边各写一遍正是以前那个 bug 的温床。
    let rows_written = if direct {
        // 直通面：目标端**自己**给的确认数（`NumberLoadedRows`），比写入侧自报更硬
        outcomes.iter().filter_map(|o| o.confirmed).sum()
    } else {
        outcomes.iter().map(|o| o.written).sum()
    };
    let rows = RowAudit {
        exported: run.source.exported_rows(),
        read: rows_read.load(Ordering::Relaxed),
        written: rows_written,
    };
    // 引擎只做**一件事**：开关开着就对不上就失败。显示与措辞归上层 ——
    // 两边都打一遍日志，就又回到"同一个承诺说两遍"的老路上去了。
    let verdict = rows.verdict();
    if matches!(verdict, Verdict::Mismatch { .. }) && run.fail_on_row_mismatch {
        return Err(rows.mismatch_error(&verdict));
    }

    // ── 连接器自报的运行期峰值 ──
    //
    // 必须在**连接器还活着的时候**读：快照一取，尺子随连接器一起走，
    // 峰值不会因为后面 close 掉而丢（读的是已经记下的水位，不是"当前占用"）。
    let connector_memory: Vec<MemSnapshot> = [run.source.memory_meter(), run.sink.memory_meter()]
        .into_iter()
        .flatten()
        .map(|m| m.snapshot())
        .collect();

    Ok(FlowReport {
        rows_written,
        rows,
        splits: splits_seen.load(Ordering::Relaxed),
        source_pool: source_pool.stats(),
        sink_pool: sink_pool.stats(),
        elapsed_ms: start.elapsed().as_millis() as u64,
        peak_rss_bytes: process_peak_rss_bytes(),
        connector_memory,
        // 两道闸门的峰值：判断"这个并发数是不是给大了"
        source_gate_peak: source_gauge.peak(),
        sink_gate_peak: sink_gauge.peak(),
    })
}

/// 逻辑面：读一个分片，把 Arrow 批次推进 source_pool
async fn read_split(
    source: &Arc<dyn SourceConnector>,
    split: &SourceSplit,
    pool: &Arc<DataPool<FlowItem>>,
    ctl: &Arc<Ctl>,
) -> D2DResult<(usize, u64)> {
    use futures::StreamExt;
    let mut stream = source.read(split).await?;
    let mut n = 0usize;
    let mut rows = 0u64;
    while let Some(item) = stream.next().await {
        if ctl.is_set() {
            return Ok((n, rows));
        }
        let batch = item?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows() as u64;
        pool.push(FlowItem::Batch(batch)).await?;
        n += 1;
    }
    Ok((n, rows))
}

/// 直通面：读一个分片，把**原样字节块**推进 source_pool
///
/// 与 [`read_split`] 并列而不是塞进同一个函数：两者的产出类型、错误语义、
/// 安全闸都不一样，混在一起会得到一个谁都不敢改的函数。
async fn read_split_physical(
    source: &Arc<dyn SourceConnector>,
    split: &SourceSplit,
    pool: &Arc<DataPool<FlowItem>>,
    ctl: &Arc<Ctl>,
    preflight: bool,
    forbidden: &[u8],
) -> D2DResult<(usize, u64)> {
    use futures::StreamExt;
    let psrc = source
        .as_physical()
        .ok_or_else(|| D2DError::internal("直通路径需要一个具备物理形态的源"))?;

    // 预扫描（可选）：先把整片读一遍找违规字节，避免"发到一半才失败"
    if preflight && !forbidden.is_empty() {
        let mut s = psrc.read_physical(split).await?;
        while let Some(block) = s.next().await {
            let block = block?;
            if let Some(bad) = crate::direct::find_forbidden(&block.bytes, forbidden) {
                return Err(D2DError::external(
                    "direct_path",
                    format!(
                        "预扫描发现违规字节 {:?}（分片 {}）—— 未发出任何数据即失败。\
                         改用逻辑面重跑，或让源端导出带转义。",
                        bad as char, split.split_id
                    ),
                ));
            }
        }
    }

    let mut stream = psrc.read_physical(split).await?;
    let mut n = 0usize;
    let mut rows = 0u64;
    while let Some(item) = stream.next().await {
        if ctl.is_set() {
            return Ok((n, rows));
        }
        let block = item?;
        if block.is_empty() {
            continue;
        }
        rows += block.rows;
        pool.push(FlowItem::Block(block)).await?;
        n += 1;
    }
    Ok((n, rows))
}

/// 从池里**接着攒**，攒到 `batch_bytes` / `batch_rows` 为止 —— **sink 档用这个**
///
/// 与 [`gather`] 只差一件事：池里暂时没货时是**等**（`pop().await`），而不是停手。
///
/// # "没数据了"的出口不需要另造判据
///
/// `pop()` 返回 `None` 只有一种情况：**池已关闭且已排空**。而 `sink_pool` 的关闭
/// 发生在级联收口里"**所有 pipeline 任务都退出之后**"—— 那正是"不会再有数据进来"。
/// 所以收尾就落在这个 `None` 上，不用发明什么超时（从前那个
/// `sink.flush_interval_ms`——"空闲多久强制落盘"——就是这么被删掉的）。
///
/// # 为什么 sink 要等、pipeline 不等
///
/// sink 的每一次"送"（一次 Stream Load）代价几乎与大小无关 —— 实测每次 ~5.1 s，
/// 成本在目标端的事务上，**单元数直接决定耗时**：
/// 同一份数据，"攒够才发"是 96 个单元、"有多少发多少"是 108 个，耗时差 15%
/// （见 基准实测「并发模型重构 A/B」）。
/// pipeline 那一段吃的是 CPU，等它攒批的收益没有实测支持，先不动（[`gather`] 仍是"不等"）。
///
/// ⚠️ 代价：数据在池外多留一份（每个任务手里 ≤ `batch_bytes`）—— 这正是内存账 ⑥
/// 那一条 `sink.concurrency × batch_bytes` 记的东西。
async fn gather_until_full(
    first: FlowItem,
    pool: &Arc<DataPool<FlowItem>>,
    batch_bytes: usize,
    batch_rows: usize,
) -> D2DResult<FlowItem> {
    let mut items = vec![first];
    let mut bytes: usize = items[0].bytes();
    let mut rows: usize = items[0].rows();

    let want_more = batch_bytes > 0 || batch_rows > 0;
    if want_more {
        while items.len() < MAX_GATHER_ITEMS {
            if batch_bytes > 0 && bytes >= batch_bytes {
                break;
            }
            if batch_rows > 0 && rows >= batch_rows {
                break;
            }
            match pool.pop().await {
                Some(Ok(x)) => {
                    bytes += x.bytes();
                    rows += x.rows();
                    items.push(x);
                }
                Some(Err(e)) => return Err(e),
                // 关闭且排空 = 不会再有数据了 → 把手里这份送出去
                None => break,
            }
        }
    }

    if items.len() == 1 {
        return Ok(items.pop().unwrap());
    }
    merge_items(items)
}

/// 从池里顺手多取几个，攒到 `batch_bytes` / `batch_rows` 为止 —— **pipeline 档用这个**
///
/// **只取"此刻已经在池里"的**（`try_pop`），绝不为凑满而空等 ——
/// 后者会把一个批次的处理延迟拉长到"等下一个源端批次到达"，
/// 在源端慢的时候表现为整条链路停摆。
///
/// （sink 档不用它：那里的每一次"送"很贵，值得等一等。见 [`gather_until_full`]。）
fn gather(
    first: FlowItem,
    pool: &Arc<DataPool<FlowItem>>,
    batch_bytes: usize,
    batch_rows: usize,
) -> D2DResult<FlowItem> {
    let mut items = vec![first];
    let mut bytes: usize = items[0].bytes();
    let mut rows: usize = items[0].rows();

    let want_more = batch_bytes > 0 || batch_rows > 0;
    if want_more {
        while items.len() < MAX_GATHER_ITEMS {
            if batch_bytes > 0 && bytes >= batch_bytes {
                break;
            }
            if batch_rows > 0 && rows >= batch_rows {
                break;
            }
            match pool.try_pop() {
                Some(Ok(x)) => {
                    bytes += x.bytes();
                    rows += x.rows();
                    items.push(x);
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
    }

    if items.len() == 1 {
        return Ok(items.pop().unwrap());
    }
    merge_items(items)
}

/// 一次攒批最多拼多少件
///
/// 没有它，窄表 + 大 `batch_bytes` 会把手里的件数拉到几十万，
/// 而每件一次 `try_pop` 都在抢同一把锁 —— 攒批本身成了瓶颈。
/// 攒到上限就发车：**上限是"次数"，不是"量"**，量的上限仍由 `batch_bytes` 管。
const MAX_GATHER_ITEMS: usize = 10_000;

/// 把若干件拼成一件
///
/// 两种载荷各自合并，且**都必须合并**：
/// - Arrow 批次走 `concat_batches`（一次拷贝，换更少的算子调用）
/// - 字节块**直接首尾相接** —— 块边界是行对齐的，接起来仍是一条条完整记录；
///   不接的话一次 4 MB 的块就是一次 Stream Load，请求数会翻十几倍
fn merge_items(items: Vec<FlowItem>) -> D2DResult<FlowItem> {
    match items.first() {
        Some(FlowItem::Batch(_)) => {
            let mut batches = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    FlowItem::Batch(b) => batches.push(b),
                    FlowItem::Block(_) => {
                        return Err(D2DError::internal(
                            "同一份数据里混了 Arrow 批次与字节块 —— 执行路径判定出错",
                        ))
                    }
                }
            }
            let schema = batches[0].schema();
            // `concat_batches` 的签名是 `impl IntoIterator<Item = &'a RecordBatch>`：
            // 传 `&Vec<RecordBatch>` 正好，传 `iter()` 反而会多套一层 `&`
            let merged = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| D2DError::internal(format!("攒批拼接失败: {}", e)))?;
            Ok(FlowItem::Batch(merged))
        }
        Some(FlowItem::Block(_)) => {
            let mut bytes = Vec::new();
            let mut rows = 0u64;
            for it in items {
                match it {
                    FlowItem::Block(b) => {
                        bytes.extend_from_slice(&b.bytes);
                        rows += b.rows;
                    }
                    FlowItem::Batch(_) => {
                        return Err(D2DError::internal(
                            "同一份数据里混了 Arrow 批次与字节块 —— 执行路径判定出错",
                        ))
                    }
                }
            }
            Ok(FlowItem::Block(PhysicalBlock { bytes, rows }))
        }
        None => Err(D2DError::internal("攒批收到空的一份数据")),
    }
}

/// 跑算子链
fn apply_chain(chain: &d2d_transform::TransformChain, item: FlowItem) -> D2DResult<FlowItem> {
    match item {
        FlowItem::Batch(b) => Ok(FlowItem::Batch(chain.transform(b)?)),
        // 直通面不该走到这里（规划器保证了"有算子必走逻辑面"）
        other => Ok(other),
    }
}

/// 开一条写入会话（逻辑面 / 直通面）
async fn open_session(
    sink: &Arc<dyn SinkConnector>,
    spec: &SinkSpec,
    format: Option<&d2d_connector::PhysicalFormat>,
) -> D2DResult<SinkHandle> {
    match format {
        Some(fmt) => {
            let psink = sink
                .as_physical()
                .ok_or_else(|| D2DError::internal("直通路径需要一个能收物理载荷的目标"))?;
            Ok(SinkHandle::Physical(psink.begin_physical(fmt).await?))
        }
        None => Ok(SinkHandle::Logical(sink.begin_session(spec).await?)),
    }
}

/// 一个 sink 任务 = **一份数据 = 一次写入**
///
/// 它不做循环：循环住在派发器里。这里只干三件事 —— 从池里攒够一份（≤ `batch_bytes`）、
/// 开一条会话写出去、提交。所以"写了几次"就是"池里攒出了几份"，
/// 与 `sink.concurrency` 没有任何推导关系。
#[allow(clippy::too_many_arguments)]
async fn run_sink_task(
    id: usize,
    sink: Arc<dyn SinkConnector>,
    spec: SinkSpec,
    first: FlowItem,
    in_pool: Arc<DataPool<FlowItem>>,
    ctl: Arc<Ctl>,
    format: Option<d2d_connector::PhysicalFormat>,
    forbidden: Vec<u8>,
    scan_forbidden: bool,
    batch_bytes: usize,
    batch_rows: usize,
) -> D2DResult<SinkOutcome> {
    // 派发器取到券之后、任务真正开跑之前，链路可能已经中止了 ——
    // 这一句挡的是"中止之后还往目标端写一次"。
    if ctl.is_set() {
        return Ok(SinkOutcome::default());
    }

    // 攒批：**内核**从池里攒够一份再交出去。连接器收到即写 ——
    // 它内部没有缓冲，也就没有"攒着不发"这个状态（见模块文档同名一节）。
    //
    // ⚠️ sink 用的是**会等**的那一版：一次写入贵在目标端的事务上、几乎与大小无关，
    //    所以"攒够才发"能把单元数压下来（实测 108 → 96，耗时差 15%）。
    //    池关闭（= 不会再有数据）就是收尾出口，见 `gather_until_full`。
    let item = match gather_until_full(first, &in_pool, batch_bytes, batch_rows).await {
        Ok(x) => x,
        Err(e) => {
            // 池被关闭时的错误是"中止"的连锁反应，不该盖掉第一个错误
            if !ctl.is_set() {
                ctl.fail(e.clone());
            }
            return Err(e);
        }
    };
    let mut handle = match open_session(&sink, &spec, format.as_ref()).await {
        Ok(h) => h,
        Err(e) => {
            error!(task = id, error = %e, "开写入会话失败");
            ctl.fail(e.clone());
            return Err(e);
        }
    };

    let is_physical = format.is_some();
    let mut written: u64 = 0;

    // 任何一条失败路径都要做同一件事：记错误 → 放行所有人 → 回滚会话。
    // 抽成宏是为了**不漏**——漏一处就是一次挂死。
    macro_rules! bail {
        ($e:expr) => {{
            let e: D2DError = $e;
            error!(task = id, error = %e, "sink_task 失败，回滚会话");
            ctl.fail(e.clone());
            let _ = handle.abort().await;
            return Err(e);
        }};
    }

    // ── 安全闸：直通载荷里出现受限字节即失败 ──
    //
    // 放在**入 body 之前**（而不是预扫描阶段）的理由见 `crate::direct` 模块文档：
    // 这些字节本来就要被读一遍，扫描是顺路的；命中即整任务失败，
    // 把"目标端会静默改值"换成"大声失败"。
    if let FlowItem::Block(b) = &item {
        if scan_forbidden {
            if let Some(bad) = crate::direct::find_forbidden(&b.bytes, &forbidden) {
                bail!(D2DError::external(
                    "direct_path",
                    format!(
                        "直通载荷中出现违规字节 {:?}（偏移 {}）。\
                         目标端会把该字节当转义符解释，而源端导出未做转义 —— \
                         继续直通会静默改值。已中止。\
                         处理方式：① 改用逻辑面（配置任意 transform 算子即可自动回落）；\
                         ② 或在源端导出改为带转义，使之与目标端一致。",
                        bad as char,
                        memchr::memchr(bad, &b.bytes).unwrap_or(0)
                    ),
                ));
            }
        }
    }

    match handle.write(item).await {
        Ok(n) => written += n,
        Err(e) => bail!(e),
    }

    let confirmed = match handle.commit().await {
        Ok(n) => n,
        Err(e) => bail!(e),
    };

    // ⚠️ **对账不能在这里做。**
    //
    // 每个 sink_task 只看到自己那一份源端行数与自己那条会话确认的行数 ——
    // 单看一个永远对不上账（一份数据可能横跨多个源端分片）。
    // 所以这里只**收集**，比对放到 [`run_flow`] 里对全体求和之后做。
    let outcome = SinkOutcome {
        written,
        confirmed: is_physical.then_some(confirmed),
    };
    debug!(task = id, written = written, "sink_task 退出");
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    const MB: usize = 1024 * 1024;

    #[test]
    fn test_parse_bytesize_units() {
        assert_eq!(parse_bytesize("512").unwrap(), 512);
        assert_eq!(parse_bytesize("512B").unwrap(), 512);
        assert_eq!(parse_bytesize("1K").unwrap(), 1024);
        assert_eq!(parse_bytesize("64MB").unwrap(), 64 * MB);
        assert_eq!(parse_bytesize("2GB").unwrap(), 2 * 1024 * MB);
        assert_eq!(
            parse_bytesize(" 1.5GB ").unwrap(),
            (1.5 * 1024.0 * MB as f64) as usize
        );
        // 下划线分隔是给人看的，不是给解析器掺沙子的
        assert_eq!(parse_bytesize("1_048_576").unwrap(), 1048576);
    }

    #[test]
    fn test_parse_bytesize_rejects_garbage() {
        assert!(parse_bytesize("").is_err());
        assert!(parse_bytesize("abc").is_err());
        let e = parse_bytesize("5XB").unwrap_err();
        assert!(e.contains("不识别的单位"), "{}", e);
        assert!(
            e.contains("1024"),
            "错误信息要说明进制，否则用户按 1000 算就永远对不上: {}",
            e
        );
    }

    #[test]
    fn test_flow_spec_yaml_shape() {
        let yaml = r#"
source:
  concurrency: 16
  pool:
    capacity: 512MB
    on_overflow: spill
pipeline:
  concurrency: 8
  batch_bytes: 64MB
sink:
  concurrency: 4
  batch_bytes: 100MB
  batch_rows: 200000
  pool:
    capacity: 256MB
"#;
        let s: FlowSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(s.source.concurrency, 16);
        assert_eq!(s.source.pool.capacity, 512 * MB);
        assert_eq!(s.source.pool.on_overflow, PoolOverflow::Spill);
        assert_eq!(s.pipeline.concurrency, 8);
        assert_eq!(s.pipeline.batch_bytes, 64 * MB);
        assert_eq!(s.sink.concurrency, 4);
        assert_eq!(s.sink.batch_rows, 200_000);
        assert_eq!(s.sink.pool.capacity, 256 * MB);
        // 没写的字段落到安全缺省，而不是 0（0 会让池变成永远满）
        assert!(s.source.pool.max_spill > 0);
    }

    /// 参数名拼错必须**在解析配置时**失败 —— 与算子名拼错同一条纪律
    #[test]
    fn test_misspelled_flow_field_is_rejected() {
        let e = serde_yaml::from_str::<FlowSpec>(
            "source:\n  concurrency: 4\n  poool:\n    capacity: 1GB\n",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("poool"), "{}", e);
    }

    #[test]
    fn test_empty_flow_spec_has_safe_defaults() {
        let s: FlowSpec = serde_yaml::from_str("{}").unwrap();
        let r = s.resolve();
        assert!(r.pipeline >= 1);
        assert_eq!(r.source, r.pipeline, "源端缺省跟随处理并发");
        assert_eq!(r.sink, r.pipeline, "目标端缺省跟随处理并发");
        assert!(s.source.pool.capacity > 0);
        assert!(s.sink.pool.capacity > 0);
    }

    /// **核心**：并发是用户填的数，**任何内存额度下都不许被改写**
    #[test]
    fn test_concurrency_is_never_derived_from_memory() {
        let yaml = "source: {concurrency: 64}\npipeline: {concurrency: 32}\nsink: {concurrency: 8}";
        let s: FlowSpec = serde_yaml::from_str(yaml).unwrap();
        let r = s.resolve();
        assert_eq!((r.source, r.pipeline, r.sink), (64, 32, 8));

        // 内存给得再小，三档并发也一动不动 —— 不够就报错，不偷偷砍
        let unit = UnitFootprint::in_process();
        let e = s
            .validate_memory(&r, unit, false, 1)
            .unwrap_err()
            .to_string();
        assert!(e.contains("内存账超出"), "{}", e);
        let r2 = s.resolve();
        assert_eq!(
            (r2.source, r2.pipeline, r2.sink),
            (64, 32, 8),
            "报错不等于改写"
        );
    }

    /// 内存账六项必须**逐项可见**，且明细之和等于总数
    #[test]
    fn test_memory_account_is_itemized_and_adds_up() {
        let yaml = r#"
source:
  concurrency: 16
  pool: {capacity: 512MB}
pipeline: {concurrency: 16, batch_bytes: 64MB}
sink:
  concurrency: 8
  batch_bytes: 100MB
  pool: {capacity: 512MB}
"#;
        let s: FlowSpec = serde_yaml::from_str(yaml).unwrap();
        let r = s.resolve();
        let unit = UnitFootprint::process(64 * MB);
        let a = s.memory_account(&r, unit, true);
        let sum: usize = a.lines.iter().map(|l| l.bytes).sum();
        assert_eq!(sum, a.total);
        // 16×64(子进程) + 512 + 16×64(工作集) + 16×64(中间态) + 512 + 8×100
        assert_eq!(a.total, (1024 + 512 + 1024 + 1024 + 512 + 800) * MB);
        assert_eq!(a.lines.len(), 6, "六项一个不能少");
        // 全是子进程时，本进程口径 = 总额 − 子进程那部分
        assert_eq!(a.child_process_bytes, 16 * 64 * MB);
        assert_eq!(a.in_process_mb(), a.total / MB - 1024);
        let rendered = a.render();
        for what in [
            "源端取数单元",
            "source_pool",
            "pipeline 工作集",
            "转换中间态",
            "sink_pool",
            "sink 工作集",
        ] {
            assert!(rendered.contains(what), "明细里缺 {}: {}", what, rendered);
        }
    }

    /// **一个取数单元可以两部分都有**（dmfldr：外面一个导出子进程 + 本进程里的解析缓冲）
    ///
    /// 这时"本进程口径"必须只减掉**子进程那一部分** —— 按形态一刀切（"是 Process
    /// 就全减掉"）会把本进程里的缓冲也减掉，于是账本看着比实测还准，那是假的。
    #[test]
    fn test_unit_with_child_and_local_parts_is_accounted_separately() {
        let s: FlowSpec = serde_yaml::from_str("source: {concurrency: 4}").unwrap();
        let r = s.resolve();
        // 每单元：子进程 64 MB + 本进程内 16 MB
        let unit = d2d_connector::UnitFootprint::process_plus_local(64 * MB, 16 * MB);
        let a = s.memory_account(&r, unit, false);

        let line = a.lines.iter().find(|l| l.what == "源端取数单元").unwrap();
        assert_eq!(line.bytes, 4 * 80 * MB, "总额按 进程内 + 进程外 记");
        assert!(
            line.note.contains("子进程 64 MB") && line.note.contains("本进程内 16 MB"),
            "两部分要分别打出来，不然读账的人分不清哪笔在进程里: {}",
            line.note
        );
        assert_eq!(
            a.child_process_bytes,
            4 * 64 * MB,
            "只有子进程那部分算'本进程之外'"
        );
        assert_eq!(
            a.in_process_mb(),
            a.total / MB - 4 * 64,
            "本进程口径 = 总额 − 子进程部分（那 4×16 是本进程里的，不能减）"
        );
    }

    /// 没有转换算子时不该虚报"转换中间态" —— 虚报会让用户白白加内存
    #[test]
    fn test_no_transforms_means_no_intermediate_line() {
        let s: FlowSpec = serde_yaml::from_str("{}").unwrap();
        let r = s.resolve();
        let unit = UnitFootprint::in_process();
        let a = s.memory_account(&r, unit, false);
        assert!(
            !a.lines.iter().any(|l| l.what == "转换中间态"),
            "直通路径没有转换，不该算这一项"
        );
        let b = s.memory_account(&r, unit, true);
        assert!(b.total > a.total);
    }

    #[test]
    fn test_validate_memory_passes_when_it_fits() {
        let s: FlowSpec = serde_yaml::from_str("{}").unwrap();
        let r = s.resolve();
        let unit = UnitFootprint::in_process();
        let a = s.validate_memory(&r, unit, true, 8192).unwrap();
        assert!(a.total <= 8192 * MB);
    }

    /// 超账时报错必须**说清超在哪一项**，而不是只说"内存不够"
    #[test]
    fn test_over_budget_error_names_the_offending_line() {
        let yaml = r#"
source:
  concurrency: 64
  pool: {capacity: 2GB}
pipeline: {concurrency: 16, batch_bytes: 64MB}
sink: {concurrency: 8, batch_bytes: 100MB, pool: {capacity: 2GB}}
"#;
        let s: FlowSpec = serde_yaml::from_str(yaml).unwrap();
        let r = s.resolve();
        let e = s
            .validate_memory(&r, UnitFootprint::in_process(), true, 2048)
            .unwrap_err()
            .to_string();
        assert!(e.contains("source_pool"), "要指出是哪一项: {}", e);
        assert!(e.contains("spill"), "要给出可照做的处置方式: {}", e);
        assert!(e.contains("超 "), "{}", e);
    }

    // ───────── 攒批 ─────────

    fn batch(rows: usize) -> FlowItem {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let arr = Int32Array::from(vec![1i32; rows]);
        FlowItem::Batch(arrow::array::RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap())
    }

    #[test]
    fn test_gather_respects_batch_bytes() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        for _ in 0..3 {
            futures::executor::block_on(p.push(batch(64))).unwrap();
        }
        let first = futures::executor::block_on(p.pop()).unwrap().unwrap();
        // 上限 1 字节 → 第一个元素本身就超了 → 一个都不多取
        let g = gather(first, &p, 1, 0).unwrap();
        assert!(matches!(g, FlowItem::Batch(_)));
        assert_eq!(p.queued(), 2, "攒够了就不该再取");
    }

    #[test]
    fn test_gather_merges_rows() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        for _ in 0..3 {
            futures::executor::block_on(p.push(batch(100))).unwrap();
        }
        let first = futures::executor::block_on(p.pop()).unwrap().unwrap();
        let g = gather(first, &p, 64 * MB, 0).unwrap();
        assert_eq!(g.rows(), 300, "攒批必须把行数合并，不能丢");
        assert_eq!(p.queued(), 0);
    }

    fn block(rows: u64, byte: u8) -> FlowItem {
        FlowItem::Block(d2d_connector::PhysicalBlock {
            bytes: vec![byte; rows as usize],
            rows,
        })
    }

    /// **直通面同样要攒批** —— 块边界是行对齐的，接起来仍是一条条完整记录。
    ///
    /// 不接的代价很具体：源端一块 4 MB，就是一次 Stream Load。
    /// `sink.batch_bytes` 说"内核一次递给连接器多少"，字节块当然也得守这句话。
    #[test]
    fn test_gather_merges_byte_blocks_up_to_batch_bytes() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        for i in 0..4u64 {
            futures::executor::block_on(p.push(block(10, b'a' + i as u8))).unwrap();
        }
        let first = futures::executor::block_on(p.pop()).unwrap().unwrap();
        // 上限 25 字节：10 → 20 → 30 就停（攒够即走，不会为凑满而空等）
        let g = gather(first, &p, 25, 0).unwrap();
        match g {
            FlowItem::Block(b) => {
                assert_eq!(b.rows, 30, "行数必须合并，不能丢");
                assert_eq!(b.bytes.len(), 30, "字节必须首尾相接");
                assert_eq!(b.bytes[0], b'a');
                assert_eq!(b.bytes[29], b'c');
            }
            other => panic!("应当是字节块，实际 {:?}", other),
        }
        assert_eq!(p.queued(), 1, "攒够了就不该再取");
    }

    /// **sink 的攒批会等**：池里暂时没货就等着，攒够 `batch_bytes` 才交出去
    ///
    /// 这条与 `test_gather_*` 那一组是**成对**的：同一个池、同一份数据，
    /// `gather`（pipeline）拿一件就走，`gather_until_full`（sink）等到够。
    #[tokio::test]
    async fn test_gather_until_full_waits_for_more() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        p.push(batch(64)).await.unwrap();
        let first = p.pop().await.unwrap().unwrap();
        assert_eq!(p.queued(), 0, "池里此刻是空的");

        // 20ms 之后才来第二批 —— 不等的那版这时候已经发车了
        let p2 = Arc::clone(&p);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            p2.push(batch(100)).await.unwrap();
        });

        // 阈值卡在"第一批不够、加上第二批刚好够"的位置：
        // 它**必须**等第二批，但等到之后要立刻停手（不然会一直等下去）
        let cap = first.bytes() + 1;
        let g = gather_until_full(first, &p, cap, 0).await.unwrap();
        assert_eq!(g.rows(), 164, "第二批应当被等到了");
        assert_eq!(p.queued(), 0);
    }

    /// **收尾**：池关闭且排空 ⟹ 把手里那份送出去，不能一直等
    ///
    /// 这是"没数据了"的唯一出口 —— 靠它，sink 就不需要"空闲多久强制落盘"那种超时旋钮。
    #[tokio::test]
    async fn test_gather_until_full_gives_up_when_pool_is_closed() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        p.push(batch(64)).await.unwrap();
        let first = p.pop().await.unwrap().unwrap();
        p.close(); // 上游收工

        // 上限远大于手里的量：不等的那版立刻返回，等的那版必须靠"关闭"这个出口
        let g = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            gather_until_full(first, &p, 64 * MB, 0),
        )
        .await
        .expect("池已关闭还死等 —— 收尾出口失效了")
        .unwrap();
        assert_eq!(g.rows(), 64, "手里那份必须原样交出去，不能丢");
    }

    /// `batch_bytes = 0` = 不攒批：一个不多取、也一秒不等
    #[tokio::test]
    async fn test_gather_until_full_with_zero_is_passthrough() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        p.push(batch(7)).await.unwrap();
        p.push(batch(9)).await.unwrap();
        let first = p.pop().await.unwrap().unwrap();
        let g = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            gather_until_full(first, &p, 0, 0),
        )
        .await
        .expect("batch_bytes = 0 时不该等")
        .unwrap();
        assert_eq!(g.rows(), 7);
        assert_eq!(p.queued(), 1, "池里那一件必须原封不动");
    }

    /// 两种载荷绝不混在一份里 —— 混了就说明执行路径判定出了问题，宁可报错
    #[test]
    fn test_merge_items_rejects_mixed_payloads() {
        let e = merge_items(vec![batch(3), block(1, b'x')]).unwrap_err();
        assert!(e.to_string().contains("混"), "{}", e);
        let e = merge_items(vec![block(1, b'x'), batch(3)]).unwrap_err();
        assert!(e.to_string().contains("混"), "{}", e);
    }

    /// `batch_bytes = 0` = 不累积：源端给多大就是多大，一个不多取
    #[test]
    fn test_gather_with_zero_means_passthrough() {
        let p = DataPool::<FlowItem>::new(PoolConfig::new("t", 64 * MB));
        futures::executor::block_on(p.push(batch(7))).unwrap();
        futures::executor::block_on(p.push(batch(9))).unwrap();
        let first = futures::executor::block_on(p.pop()).unwrap().unwrap();
        let g = gather(first, &p, 0, 0).unwrap();
        assert_eq!(g.rows(), 7, "零上限时绝不能顺手多取");
        assert_eq!(p.queued(), 1, "池里那一件必须原封不动");
    }

    // ───────── 溢出载荷 ─────────

    /// FlowItem 的 spill 必须在两种载荷下都逐位还原
    #[tokio::test]
    async fn test_flow_item_spill_round_trip_both_planes() {
        let dir = std::env::temp_dir().join(format!("d2d-flowitem-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // 容量正好放得下一个 Item → 第二个必然溢出
        let cap = batch(5).bytes();
        let p = DataPool::<FlowItem>::new(
            crate::pool::PoolConfig::new("t", cap)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        p.push(batch(5)).await.unwrap();
        p.push(FlowItem::Block(d2d_connector::PhysicalBlock {
            bytes: b"1|a\n".to_vec(),
            rows: 1,
        }))
        .await
        .unwrap();

        let a = p.pop().await.unwrap().unwrap();
        assert!(
            matches!(a, FlowItem::Batch(_)),
            "先进先出：第一个应当是批次"
        );
        assert_eq!(a.rows(), 5);

        let b = p.pop().await.unwrap().unwrap();
        match b {
            FlowItem::Block(blk) => {
                assert_eq!(blk.bytes, b"1|a\n");
                assert_eq!(blk.rows, 1);
            }
            other => panic!("第二个应当是字节块，实际 {:?}", other),
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
