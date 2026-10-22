//! 分区体系：内核契约层（与具体数据库无关）
//!
//! ## 为什么单独抽一层
//!
//! "分片"这个词在本项目里同时指三件事，早先被塞进同一个
//! `SplitConfig { mode, column, count }`，`count` 一个字段承担三种语义：
//!
//! | 层 | 决定什么 | 现在归谁 |
//! |---|---|---|
//! | **L1 取数范围** | 从库里取哪些行 | **本模块**（`partition` 配置） |
//! | **L2 并行单元** | 同时几个单元压在库上 | `flow.source.concurrency` —— **内核持闸门**，连接器看不见这个数 |
//! | **L3 解析分块** | 落地文件切几块并行解析 | **已不是旋钮**：按文件大小自动切 |
//!
//! 本模块只负责 **L1**：把"要取哪些行"切成 N 段互不重叠、并集完整的取数范围。
//! **它完全不知道也不关心并行度** —— L2 是引擎的事，而且不由连接器经手。
//! 三者解耦之后，"切得细"不再等于"开很多进程"。
//!
//! ⚠️ 这里曾经把 L2 写成 `split.count` / dmfldr 的 `export_processes` ——
//! 两个**连接器自己解读的数字**。现在 L2 只有一个出处（引擎的闸门），
//! 连接器连这个数字都看不到。
//!
//! ## 三条设计原则
//!
//! 1. **用户给了边界就不探测。** 显式边界（[`PartitionMode::List`] /
//!    [`PartitionMode::Range`] 带 `lower`+`upper` / [`PartitionMode::Hash`]）
//!    是纯函数，一个查询都不发。只有 `range` 不给边界时才回退探测，且结果落盘复用。
//! 2. **谓词结构化，不是 SQL 字符串。** [`PartitionPredicate::Range`] 是
//!    `(列, 下界, 上界)`，由各连接器渲染成自己的方言（达梦 `TIMESTAMP'...'`、
//!    MySQL 反引号）。唯一的逃生舱是 [`PartitionPredicate::Raw`]（`list` 模式），
//!    作者自负正确性 —— 与 DataX 的 `where` 语义一致。
//! 3. **日期是一等公民。** 业务里最常见的分区键就是日期（表没有自增 ID 时尤其如此）。
//!    区间"均分"必须按**时间跨度**算，不能按字典序；切点还要吸附到自然边界
//!    （天/时/分/秒），否则谓词落在分区边界之外，服务端无法裁剪 —— 见 [`range_cuts`]。
//!
//! ## 覆盖性保证
//!
//! 所有模式产出的分区**互不重叠、并集等于用户声明的取数范围**（由 [`range_cuts`]
//! 的严格递增切点构造保证）。但用户给的边界本身是否覆盖了全表，内核无从判断 ——
//! 那是任务作者的责任，与 `list` 模式同一契约。

use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// 计划文件格式版本（不兼容变更时递增）
pub const PLAN_VERSION: u32 = 1;

// ─────────────────────────────── 模式 ───────────────────────────────

/// 分区模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionMode {
    /// 不分片：单个分区读全表。**一天的数据量、小表，就该用这个。**
    #[default]
    None,
    /// 用户直接给条件数组，每条一个分区。零探测。
    List,
    /// 按列的区间均分。给了 `lower`+`upper` 就零探测；没给才回退探测（结果可落盘）。
    Range,
    /// `MOD(HASH(col), count) = i`。零探测，无边界可依时的兜底。
    Hash,
}

impl PartitionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::List => "list",
            Self::Range => "range",
            Self::Hash => "hash",
        }
    }
}

/// 规划来源（写日志用，也是"这次为什么这么慢"的第一线索）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedBy {
    /// 用户显式给出（list / range+边界 / hash / none）—— 零探测
    User,
    /// 复用已落盘的计划 —— 零探测
    Cache,
    /// 回退探测了列边界（range 不给边界）
    Probe,
    /// 连接器用元数据字典回答的（如达梦 `USER_TAB_PARTITIONS`，~0.1s，不扫数据）
    Metadata,
}

// ─────────────────────────────── 字面量 ───────────────────────────────

/// 类型化字面量 —— **日期是日期，不是字符串**
///
/// 切分数学按类型分派（见 [`range_cuts`]）：整数按数值均分、日期按时间跨度均分、
/// 文本不可切。若把日期当字符串，按字典序切出来的区间大小会差几个数量级。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Int(i128),
    Float(f64),
    Text(String),
    Date(chrono::NaiveDate),
    Timestamp(chrono::NaiveDateTime),
}

impl Literal {
    /// 按"最具体到最宽泛"的顺序猜测类型
    ///
    /// 顺序有讲究：`2026` 是整数不是文本；`2026-01-01` 要先试日期再试文本。
    pub fn parse(raw: &str) -> Self {
        let t = raw.trim();
        if let Ok(i) = t.parse::<i128>() {
            return Self::Int(i);
        }
        if let Some(dt) = parse_datetime(t) {
            return Self::Timestamp(dt);
        }
        if let Ok(d) = chrono::NaiveDate::parse_from_str(t, "%Y-%m-%d") {
            return Self::Date(d);
        }
        if let Ok(f) = t.parse::<f64>() {
            // NaN/Inf 不能作为区间端点
            if f.is_finite() {
                return Self::Float(f);
            }
        }
        Self::Text(t.to_string())
    }

    /// 时间语义的毫秒表示（非时间类型返回 `None`）
    ///
    /// `Date` 归一化到当日零点 —— 这样 `Date` 与 `Timestamp` 能落在同一根数轴上比较，
    /// 不必在切分数学里到处分支。
    pub fn as_millis(&self) -> Option<i64> {
        match self {
            Self::Date(d) => d
                .and_hms_opt(0, 0, 0)
                .map(|dt| dt.and_utc().timestamp_millis()),
            Self::Timestamp(dt) => Some(dt.and_utc().timestamp_millis()),
            _ => None,
        }
    }

    /// 类型名（错误信息用，中文便于运维读懂）
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Int(_) => "整数",
            Self::Float(_) => "浮点数",
            Self::Text(_) => "文本",
            Self::Date(_) => "日期",
            Self::Timestamp(_) => "时间戳",
        }
    }

    /// 是否同族可比（只有同族才能均分）
    fn same_family(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(_), Self::Int(_)) => true,
            (Self::Float(_), Self::Float(_)) => true,
            (Self::Text(_), Self::Text(_)) => true,
            _ => self.as_millis().is_some() && other.as_millis().is_some(),
        }
    }
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(i) => write!(f, "{}", i),
            Self::Float(x) => write!(f, "{}", x),
            Self::Text(s) => write!(f, "{}", s),
            Self::Date(d) => write!(f, "{}", d),
            Self::Timestamp(dt) => write!(f, "{}", dt),
        }
    }
}

fn parse_datetime(t: &str) -> Option<chrono::NaiveDateTime> {
    const FORMATS: [&str; 4] = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ];
    FORMATS
        .iter()
        .find_map(|f| chrono::NaiveDateTime::parse_from_str(t, f).ok())
}

// ─────────────────────────────── 谓词 ───────────────────────────────

/// 结构化分区谓词 —— 内核产出，连接器渲染成各自方言
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPredicate {
    /// 无约束（单分区读全表）
    All,
    /// 原样 SQL 片段（`list` 模式的唯一逃生舱，作者自负正确性）
    Raw(String),
    /// `col >= lower AND col </<= upper`；`None` 表示该侧无界
    Range {
        column: String,
        lower: Option<Literal>,
        upper: Option<Literal>,
        /// `true` → `<=`（闭区间）；`false` → `<`（半开）
        upper_inclusive: bool,
    },
    /// `MOD(HASH(col), modulo) = remainder`
    Hash {
        column: String,
        modulo: u64,
        remainder: u64,
    },
}

/// 一个分区 = 一段可独立执行的取数范围
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Partition {
    /// 稳定、可读、可作文件名：`p000`、`p001`…
    pub id: String,
    /// 人类可读的取值范围（日志/排查用）
    pub describe: String,
    pub predicate: PartitionPredicate,
}

// ─────────────────────────────── 规格 ───────────────────────────────

/// 用户声明的分区意图（YAML `source.partition`）
///
/// ```yaml
/// # 不分片
/// partition: { mode: none }
///
/// # 用户直接给条件，零探测
/// partition:
///   mode: list
///   conditions:
///     - "CREATED_AT >= TIMESTAMP'2026-01-01 00:00:00' AND CREATED_AT < TIMESTAMP'2026-04-01 00:00:00'"
///     - "CREATED_AT >= TIMESTAMP'2026-04-01 00:00:00'"
///
/// # 上下边界 + 分区数（Spark 风格），零探测
/// partition:
///   mode: range
///   column: "CREATED_AT"
///   lower: "2026-01-01 00:00:00"
///   upper: "2026-10-01 00:00:00"
///   count: 16
///
/// # 不给边界 → 回退探测，结果落盘复用
/// partition:
///   mode: range
///   column: "USER_ID"
///   count: 16
///   plan_file: "/var/lib/d2d/plans/t1_id.json"
///
/// # 无边界可依时
/// partition:
///   mode: hash
///   column: "USER_ID"
///   count: 16
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionSpec {
    #[serde(default)]
    pub mode: PartitionMode,

    /// 分区列（`range` / `hash` 必填；`list` / `none` 无意义）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,

    /// `range` 显式下界（闭）。与 `upper` **必须成对给出**
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_literal_opt"
    )]
    pub lower: Option<Literal>,

    /// `range` 显式上界（闭）
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_literal_opt"
    )]
    pub upper: Option<Literal>,

    /// 切几片（**与并发度无关**：切得细是为了均衡和短尾巴，不是为了开更多执行体）
    ///
    /// `range` / `hash` **必须显式给出**（缺省 0 = 未指定 → 启动期报错）。
    /// 之所以不给它一个"聪明"的默认值（如自动等于并发度）：那正是旧模型把
    /// 取数范围与执行并发揉在一起的病根，切法就该由用户明确声明。
    /// `none` / `list` 下本字段无意义（分区数分别恒为 1 和 conditions.len()）。
    #[serde(default)]
    pub count: usize,

    /// `list` 模式的条件列表，每条 = 一个分区
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

impl Default for PartitionSpec {
    fn default() -> Self {
        Self {
            mode: PartitionMode::None,
            column: None,
            lower: None,
            upper: None,
            count: 0,
            conditions: Vec::new(),
        }
    }
}

impl PartitionSpec {
    /// 启动期 fail-fast（拼错/漏配一律报错，不静默降级）
    pub fn validate(&self) -> D2DResult<()> {
        match self.mode {
            PartitionMode::None => Ok(()),
            PartitionMode::List => {
                if self.conditions.is_empty() {
                    return Err(D2DError::config(
                        "partition.mode=list 需要非空的 conditions（每条 = 一个分区）；\
                         若只想过滤而不分片，请用 query.where_clause + partition.mode=none",
                    ));
                }
                for (i, c) in self.conditions.iter().enumerate() {
                    if c.trim().is_empty() {
                        return Err(D2DError::config(format!(
                            "partition.conditions[{}] 是空条件 —— 空条件会让该分区退化成全表扫描",
                            i
                        )));
                    }
                }
                Ok(())
            }
            PartitionMode::Range => {
                self.require_column("range")?;
                self.require_count("range")?;
                match (&self.lower, &self.upper) {
                    (Some(_), Some(_)) | (None, None) => Ok(()),
                    _ => Err(D2DError::config(
                        "partition.mode=range 的 lower 与 upper 必须**成对**给出（同为闭区间）；\
                         只想约束一侧请用 partition.mode=list 写显式条件",
                    )),
                }
            }
            PartitionMode::Hash => {
                self.require_column("hash")?;
                self.require_count("hash")?;
                Ok(())
            }
        }
    }

    fn require_column(&self, mode: &str) -> D2DResult<()> {
        match self.column.as_deref() {
            Some(c) if !c.trim().is_empty() => Ok(()),
            _ => Err(D2DError::config(format!(
                "partition.mode={} 需要 column（用于切分的列）",
                mode
            ))),
        }
    }

    fn require_count(&self, mode: &str) -> D2DResult<()> {
        if self.count == 0 {
            return Err(D2DError::config(format!(
                "partition.mode={} 需要显式给出 count（切几片）—— \
                 刻意不给默认值：切法与执行并发度是两件事，取数范围怎么切应当由你声明",
                mode
            )));
        }
        Ok(())
    }

    /// 这份声明**将要产出几个分区**（不实际规划，纯看声明）
    ///
    /// 供资源总账在做规划之前记账用 —— 源端子进程配额、并发上限都按它推。
    ///
    /// ⚠️ `count` 字段**不是**通用答案：`list` 模式的分区数是 `conditions.len()`，
    /// `none` 恒为 1。早先这里有处代码一律取 `count`，于是 `list` 模式被记成
    /// 1 个分区（实测踩到：17 片的配置只申请到 1 个子进程配额）。
    pub fn planned_count(&self) -> usize {
        match self.mode {
            // 退化情形（如区间窄到切不出 count 段）由 `plan()` 实际收敛，
            // 这里给的是**上界**，记账用上界是安全的
            PartitionMode::None => 1,
            PartitionMode::List => self.conditions.len().max(1),
            PartitionMode::Range | PartitionMode::Hash => self.count.max(1),
        }
    }

    /// 是否零探测（不需要连接器回答任何边界问题）
    pub fn is_probe_free(&self) -> bool {
        match self.mode {
            PartitionMode::None | PartitionMode::List | PartitionMode::Hash => true,
            // 给了显式边界就不需要探测
            PartitionMode::Range => self.lower.is_some() && self.upper.is_some(),
        }
    }

    /// 计划指纹：表 + 列 + 切法。用于拒绝把 A 表的计划用到 B 表上
    pub fn fingerprint(&self, table: &str, schema: Option<&str>) -> String {
        let mut s = String::new();
        s.push_str(schema.unwrap_or(""));
        s.push('.');
        s.push_str(table);
        s.push('|');
        s.push_str(self.mode.as_str());
        s.push('|');
        s.push_str(self.column.as_deref().unwrap_or(""));
        s.push('|');
        s.push_str(&self.count.to_string());
        s.push('|');
        if let Some(l) = &self.lower {
            s.push_str(&format!("{};", l));
        }
        s.push('|');
        if let Some(u) = &self.upper {
            s.push_str(&format!("{};", u));
        }
        s.push('|');
        for c in &self.conditions {
            s.push_str(c);
            s.push('\u{1}');
        }
        format!("{:016x}", fnv1a64(s.as_bytes()))
    }
}

fn de_literal_opt<'de, D>(d: D) -> Result<Option<Literal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Str(String),
        Int(i64),
        Float(f64),
    }
    Ok(Option::<Raw>::deserialize(d)?.map(|r| match r {
        Raw::Str(s) => Literal::parse(&s),
        Raw::Int(n) => Literal::Int(n as i128),
        Raw::Float(f) => Literal::Float(f),
    }))
}

/// FNV-1a 64：只为指纹，不作安全用途（省一个依赖）
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ─────────────────────────────── 切分数学 ───────────────────────────────

const MS_SECOND: i64 = 1_000;
const MS_MINUTE: i64 = 60 * MS_SECOND;
const MS_HOUR: i64 = 60 * MS_MINUTE;
const MS_DAY: i64 = 24 * MS_HOUR;

/// 时间切点吸附到多大的自然单位
///
/// 吸附的意义：达梦（及多数库）的表分区边界落在自然时间点上，谓词只有**正好压在**
/// 分区边界上，服务端才能裁剪。`CREATED_AT >= TIMESTAMP'2026-01-18 01:32:00'` 这种切点
/// 会让每个分片都退化成扫描更多分区。
fn snap_granularity(step_ms: i64) -> i64 {
    [MS_DAY, MS_HOUR, MS_MINUTE, MS_SECOND]
        .into_iter()
        .find(|g| *g <= step_ms)
        .unwrap_or(1)
}

/// 把 `[lower, upper]` **闭区间**均分，返回切点（含首尾）
///
/// 返回 `k+1` 个**严格递增**的值，切出 `k` 段连续闭区间，`k <= count`。
/// `k < count` 只会在退化时发生（如整数区间 `[1,3]` 要切 10 段 —— 闭区间下至多 2 段），
/// 此时返回能达到的最大段数，而不是产出空区间。
///
/// ## 切分数学按类型分派
///
/// ```text
/// 整数      : 按数值跨度均分     [1, 100] 切 4 → 1..25, 26..50, 51..75, 76..100
/// 浮点      : 按数值跨度均分
/// 日期/时间戳: 按**时间跨度**均分，切点吸附到自然边界
/// 文本      : 不可切 → 报错引导用 list 或 hash
/// ```
///
/// ## 为什么日期不能按字符串切
///
/// `2026-01-01 ~ 2026-10-01` 按字典序均分会把 `2026-01-01`～`2026-04-0x` 挤在第一段，
/// 之后每段的**时间跨度**互不相同（2 月只有 28 天），且切点全落在月中的任意时刻，
/// 服务端一个分区都裁不掉。
pub fn range_cuts(lower: &Literal, upper: &Literal, count: usize) -> D2DResult<Vec<Literal>> {
    let count = count.max(1);
    if !lower.same_family(upper) {
        return Err(D2DError::config(format!(
            "分区边界的类型不一致：lower 是{}（{}），upper 是{}（{}）—— 无法均分",
            lower.type_name(),
            lower,
            upper.type_name(),
            upper
        )));
    }
    match (lower, upper) {
        (Literal::Int(a), Literal::Int(b)) => Ok(int_cuts(*a, *b, count, lower, upper)),
        (Literal::Float(a), Literal::Float(b)) => Ok(float_cuts(*a, *b, count, lower, upper)),
        (Literal::Text(_), _) => Err(D2DError::config(
            "partition.mode=range 不支持文本列按区间切分（字典序均分没有业务意义）—— \
             请改用 partition.mode=list 显式列出条件，或 partition.mode=hash",
        )),
        _ => Ok(time_cuts(lower, upper, count)),
    }
}

/// 闭区间整数均分
///
/// 用 `(span+1)*i/count` 而不是 `span*i/count`：前者在 `count <= span` 时保证切点
/// **严格递增**（步长下界 `floor((span+1)/count) >= 1`），且 `[1,100]` 切 4 段正好是
/// `1..25 / 26..50 / 51..75 / 76..100` —— 与既有 `RangeSplitStrategy::conditions`
/// 的切法一致。后者会切出 `1..25 / 26..50 / 51..75` + 边界重复。
fn int_cuts(lo: i128, hi: i128, count: usize, lo_lit: &Literal, hi_lit: &Literal) -> Vec<Literal> {
    if hi <= lo {
        return vec![lo_lit.clone(), hi_lit.clone()];
    }
    let span = hi - lo; // > 0
                        // 闭区间下最多 span 段（span+1 个值，切点必须严格递增）
    let count = count.min(span as usize).max(1);

    let mut cuts = Vec::with_capacity(count + 1);
    cuts.push(lo);
    for i in 1..count {
        let off = ((span as u128 + 1) * i as u128) / count as u128;
        cuts.push(lo + off as i128);
    }
    cuts.push(hi);
    cuts.into_iter().map(Literal::Int).collect()
}

fn float_cuts(lo: f64, hi: f64, count: usize, lo_lit: &Literal, hi_lit: &Literal) -> Vec<Literal> {
    // 刻意写成 `partial_cmp` 而不是 `lo >= hi`：NaN 参与比较时 `lo >= hi` 为 false，
    // 会一路走到下面的均分算子里产出 NaN 切点。这里要的是"**只有严格 lo < hi 才切**"，
    // 其余（相等 / 反序 / 任一为 NaN）一律退回两段式。
    if lo.partial_cmp(&hi) != Some(std::cmp::Ordering::Less) {
        return vec![lo_lit.clone(), hi_lit.clone()];
    }
    let span = hi - lo;
    let mut cuts = vec![lo];
    for i in 1..count {
        let c = lo + span * (i as f64 / count as f64);
        // 严格递增，否则丢点（宁可少一段，不产空段）
        if c > *cuts.last().unwrap() && c < hi {
            cuts.push(c);
        }
    }
    cuts.push(hi);
    cuts.into_iter().map(Literal::Float).collect()
}

/// 按时间跨度均分，切点吸附到自然边界
fn time_cuts(lower: &Literal, upper: &Literal, count: usize) -> Vec<Literal> {
    let (lo, hi) = (
        lower.as_millis().unwrap_or(0),
        upper.as_millis().unwrap_or(0),
    );
    if hi <= lo {
        return vec![lower.clone(), upper.clone()];
    }
    let span = hi - lo;
    let step = (span / count as i64).max(1);
    let mut g = snap_granularity(step);
    // 两端都是纯日期时，只要每段跨度 >= 1 天就对齐到自然日
    let date_only = matches!((lower, upper), (Literal::Date(_), Literal::Date(_)));
    if date_only && step >= MS_DAY {
        g = MS_DAY;
    }

    let mut ms = Vec::with_capacity(count + 1);
    ms.push(lo);
    for i in 1..count {
        let raw = lo + step * i as i64;
        let snapped = raw.div_euclid(g) * g;
        let prev = *ms.last().unwrap();
        // 吸附后必须仍严格大于前一个切点、且小于上界，否则退回未吸附值
        let c = if snapped > prev && snapped < hi {
            snapped
        } else {
            raw
        };
        if c > prev && c < hi {
            ms.push(c);
        }
    }
    ms.push(hi);

    // 输出类型：全是零点 → 保持日期粒度；只要有一个带时刻 → 全体用时间戳，
    // 否则截断到日期会把多个切点压成同一个值。
    let all_midnight = ms.iter().all(|m| m.rem_euclid(MS_DAY) == 0);
    ms.into_iter()
        .map(|m| {
            let dt = chrono::DateTime::from_timestamp_millis(m)
                .map(|d| d.naive_utc())
                .unwrap_or_default();
            if date_only && all_midnight {
                Literal::Date(dt.date())
            } else {
                Literal::Timestamp(dt)
            }
        })
        .collect()
}

// ─────────────────────────────── 规划 ───────────────────────────────

/// 连接器**回答**的边界信息（`range` 不给显式边界时用）
///
/// 两种形态对应两种探测手段，覆盖性保证的强度不同：
///
/// | 形态 | 手段（以达梦为例） | 耗时 | 覆盖性 |
/// |---|---|---|---|
/// | `MinMax` | `SELECT MIN(c), MAX(c)` | 扫数据 | 由 `[min,max]` 闭区间保证 |
/// | `CutPoints` | `USER_TAB_PARTITIONS.HIGH_VALUE` | **~0.1s**（字典表，不扫数据） | 由"首尾无界、连续"的结构保证 |
#[derive(Debug, Clone, PartialEq)]
pub enum ProbedBounds {
    /// 列的最小/最大值 → 按类型均分成闭区间
    MinMax { lower: Literal, upper: Literal },
    /// 现成的切点（必须严格递增）→ 产出
    /// `(-∞, c₁)`、`[c₁, c₂)`、…、`[c_{k-1}, +∞)`
    ///
    /// **这三条性质是关键**：连续、互不重叠、首尾无界 —— 并集恒等于整个值域。
    /// 切点选得准不准只影响各段是否均衡，**不可能造成空洞或重叠**。
    /// 这比 `MinMax` 的覆盖性更强：`MinMax` 依赖"探测到的 min/max 就是真实边界"，
    /// 而探测与导出之间存在写入间隙时，新写入的行会落在区间之外被静默漏掉。
    CutPoints(Vec<Literal>),
}

/// 规划结果
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionPlan {
    pub version: u32,
    pub mode: PartitionMode,
    pub resolved_by: ResolvedBy,
    /// 表 + 列 + 切法 的指纹，防计划张冠李戴
    pub fingerprint: String,
    /// 探测时已知的源端总行数（`plan()` 这个纯函数不填，由连接器补充）
    ///
    /// 存进计划文件是有意的：**它是对账基准**。若只存在内存里，复用计划的那次运行
    /// 就拿不到这个独立来源，行数对账会静默失效 —— 而"静默"正是最该避免的。
    #[serde(default)]
    pub expected_rows: Option<u64>,
    pub partitions: Vec<Partition>,
}

fn pid(i: usize) -> String {
    format!("p{:03}", i)
}

/// 用现成切点构造 `(-∞, c₁)`、`[c₁, c₂)`、…、`[c_{k-1}, +∞)`
///
/// **这是元数据路径不丢数的结构性保证**：区间连续、互不重叠、首尾无界，
/// 并集恒等于整个值域。切点选得准不准只影响是否均衡，不可能造成空洞。
///
/// 与 `MinMax` 路径的差别值得注意：`MinMax` 依赖"探测到的 min/max 就是真实边界"，
/// 探测与导出之间若有新数据写入，那部分行落在闭区间之外会被**静默漏掉**；
/// 而这里的首尾无界天然免疫。
fn cut_point_partitions(column: &str, cuts: &[Literal]) -> Vec<Partition> {
    let mut out: Vec<Partition> = Vec::with_capacity(cuts.len() + 1);
    let mut prev: Option<Literal> = None;
    for c in cuts {
        out.push(Partition {
            id: pid(out.len()),
            describe: format!(
                "[{}, {})",
                prev.as_ref()
                    .map(|x| x.to_string())
                    .unwrap_or_else(|| "-∞".into()),
                c
            ),
            predicate: PartitionPredicate::Range {
                column: column.to_string(),
                lower: prev.clone(),
                upper: Some(c.clone()),
                upper_inclusive: false,
            },
        });
        prev = Some(c.clone());
    }
    out.push(Partition {
        id: pid(out.len()),
        describe: format!(
            "[{}, +∞)",
            prev.as_ref()
                .map(|x| x.to_string())
                .unwrap_or_else(|| "-∞".into())
        ),
        predicate: PartitionPredicate::Range {
            column: column.to_string(),
            lower: prev,
            upper: None,
            upper_inclusive: false,
        },
    });
    out
}

/// 内核规划入口 —— **纯函数**，唯一的副作用是调用方自己在外面做的落盘
///
/// `probed` 只在 `range` 且未给显式边界时被使用；给了边界时传什么都忽略。
pub fn plan(
    spec: &PartitionSpec,
    table: &str,
    schema: Option<&str>,
    probed: Option<ProbedBounds>,
) -> D2DResult<PartitionPlan> {
    spec.validate()?;
    let fingerprint = spec.fingerprint(table, schema);

    let (partitions, resolved_by): (Vec<Partition>, ResolvedBy) = match spec.mode {
        PartitionMode::None => (
            vec![Partition {
                id: pid(0),
                describe: "全表".to_string(),
                predicate: PartitionPredicate::All,
            }],
            ResolvedBy::User,
        ),

        PartitionMode::List => (
            spec.conditions
                .iter()
                .enumerate()
                .map(|(i, c)| Partition {
                    id: pid(i),
                    describe: c.clone(),
                    predicate: PartitionPredicate::Raw(c.clone()),
                })
                .collect(),
            ResolvedBy::User,
        ),

        PartitionMode::Hash => {
            let column = spec.column.clone().unwrap_or_default();
            (
                (0..spec.count)
                    .map(|i| Partition {
                        id: pid(i),
                        describe: format!("MOD(HASH({}), {}) = {}", column, spec.count, i),
                        predicate: PartitionPredicate::Hash {
                            column: column.clone(),
                            modulo: spec.count as u64,
                            remainder: i as u64,
                        },
                    })
                    .collect(),
                ResolvedBy::User,
            )
        }

        PartitionMode::Range => {
            let column = spec.column.clone().unwrap_or_default();
            match (&spec.lower, &spec.upper) {
                // ① 用户给了闭区间上下界 —— 零探测，按类型均分
                (Some(l), Some(u)) => {
                    let cuts = range_cuts(l, u, spec.count)?;
                    let n = cuts.len().saturating_sub(1);
                    let parts = (0..n)
                        .map(|i| Partition {
                            id: pid(i),
                            describe: format!("[{}, {}]", cuts[i], cuts[i + 1]),
                            predicate: PartitionPredicate::Range {
                                column: column.clone(),
                                lower: Some(cuts[i].clone()),
                                upper: Some(cuts[i + 1].clone()),
                                // 只有最后一段闭区间，前面半开 —— 不重叠的关键
                                upper_inclusive: i == n - 1,
                            },
                        })
                        .collect();
                    (parts, ResolvedBy::User)
                }
                // ② 连接器给了现成切点（字典表，不扫数据）—— 首尾无界
                (None, None) => match probed {
                    Some(ProbedBounds::CutPoints(cuts)) => {
                        let parts = cut_point_partitions(&column, &cuts);
                        (parts, ResolvedBy::Metadata)
                    }
                    // ③ 连接器探测了列的真实 min/max —— 闭区间均分
                    Some(ProbedBounds::MinMax { lower, upper }) => {
                        let cuts = range_cuts(&lower, &upper, spec.count)?;
                        let n = cuts.len().saturating_sub(1);
                        let parts = (0..n)
                            .map(|i| Partition {
                                id: pid(i),
                                describe: format!("[{}, {}]", cuts[i], cuts[i + 1]),
                                predicate: PartitionPredicate::Range {
                                    column: column.clone(),
                                    lower: Some(cuts[i].clone()),
                                    upper: Some(cuts[i + 1].clone()),
                                    upper_inclusive: i == n - 1,
                                },
                            })
                            .collect();
                        (parts, ResolvedBy::Probe)
                    }
                    None => {
                        return Err(D2DError::config(format!(
                        "partition.mode=range 未给 lower/upper，且连接器未能提供列 {} 的边界 —— \
                             请显式给出上下界，或改用 partition.mode=list / hash",
                        spec.column.as_deref().unwrap_or("?")
                    )))
                    }
                },
                _ => unreachable!("validate() 已保证 lower/upper 成对"),
            }
        }
    };

    Ok(PartitionPlan {
        version: PLAN_VERSION,
        mode: spec.mode,
        resolved_by,
        fingerprint,
        expected_rows: None, // 纯函数不探测行数；由连接器按需补充
        partitions,
    })
}

// ─────────────────────────────── 计划文件 ───────────────────────────────

/// 读取计划文件；不存在返回 `None`；指纹不符**报错**而不是静默忽略
///
/// 为什么脏计划要报错：把 A 表（或旧分区布局）的计划默默用到 B 表上，
/// 结果可能是**看起来成功的少数据**。宁可启动期失败。
pub fn load_plan(path: &Path, fingerprint: &str) -> D2DResult<Option<PartitionPlan>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = std::fs::read(path)
        .map_err(|e| D2DError::config(format!("读分区计划失败 {}: {}", path.display(), e)))?;
    let plan: PartitionPlan = serde_json::from_slice(&text).map_err(|e| {
        D2DError::config(format!(
            "分区计划 {} 解析失败（{}）—— 删掉它重新生成，或设 partition.plan_refresh: true",
            path.display(),
            e
        ))
    })?;
    if plan.version != PLAN_VERSION {
        return Err(D2DError::config(format!(
            "分区计划 {} 版本为 {}，本程序期望 {} —— 请重新生成",
            path.display(),
            plan.version,
            PLAN_VERSION
        )));
    }
    if plan.fingerprint != fingerprint {
        return Err(D2DError::config(format!(
            "分区计划 {} 与当前配置不符（计划指纹 {}，当前 {}）—— \
             表/分区列/切法变了。删掉该文件或在配置里设 partition.plan_refresh: true 重新生成",
            path.display(),
            plan.fingerprint,
            fingerprint
        )));
    }
    if plan.partitions.is_empty() {
        return Err(D2DError::config(format!(
            "分区计划 {} 不含任何分区 —— 请重新生成",
            path.display()
        )));
    }
    Ok(Some(plan))
}

/// 落盘计划（目录不存在则创建）
pub fn save_plan(path: &Path, p: &PartitionPlan) -> D2DResult<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).map_err(|e| {
                D2DError::config(format!("创建计划目录失败 {}: {}", dir.display(), e))
            })?;
        }
    }
    let text = serde_json::to_string_pretty(p)
        .map_err(|e| D2DError::internal(format!("序列化分区计划失败: {}", e)))?;
    std::fs::write(path, text)
        .map_err(|e| D2DError::config(format!("写分区计划失败 {}: {}", path.display(), e)))
}

// ─────────────────────────────── 测试 ───────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    fn ts(s: &str) -> Literal {
        Literal::parse(s)
    }

    fn millis_all(cuts: &[Literal]) -> Vec<i64> {
        cuts.iter()
            .map(|c| {
                c.as_millis()
                    .unwrap_or_else(|| panic!("{:?} 不是时间类型", c))
            })
            .collect()
    }

    // ───────── 字面量解析 ─────────

    #[test]
    fn test_literal_parse_dispatches_by_shape() {
        assert_eq!(Literal::parse("2026"), Literal::Int(2026));
        assert_eq!(Literal::parse("  -7 "), Literal::Int(-7));
        assert_eq!(
            Literal::parse("2026-01-01"),
            Literal::Date(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
        );
        match Literal::parse("2026-01-01 08:30:00") {
            Literal::Timestamp(dt) => assert_eq!(dt.to_string(), "2026-01-01 08:30:00"),
            other => panic!("expected timestamp, got {:?}", other),
        }
        assert_eq!(Literal::parse("3.5"), Literal::Float(3.5));
        assert_eq!(Literal::parse("ABC"), Literal::Text("ABC".into()));
        // NaN 不是合法端点，退化为文本
        assert_eq!(Literal::parse("NaN"), Literal::Text("NaN".into()));
    }

    /// Date 与 Timestamp 必须能落在同一根数轴上比较
    #[test]
    fn test_date_normalizes_to_midnight() {
        let d = ts("2026-01-01");
        let t = ts("2026-01-01 00:00:00");
        assert_eq!(d.as_millis(), t.as_millis());
        assert!(d.same_family(&t));
    }

    // ───────── 切分数学：整数 ─────────

    #[test]
    fn test_int_cuts_even_split() {
        // 与既有 RangeSplitStrategy::conditions(1,100) 分 4 段完全一致
        let c = range_cuts(&Literal::Int(1), &Literal::Int(100), 4).unwrap();
        assert_eq!(
            c,
            vec![
                Literal::Int(1),
                Literal::Int(26),
                Literal::Int(51),
                Literal::Int(76),
                Literal::Int(100)
            ]
        );
    }

    /// 切点必须严格递增 —— 这是"分区互不重叠"的唯一保证
    #[test]
    fn test_int_cuts_never_degenerate() {
        // 3 个值要切 10 段：闭区间下至多 2 段
        let c = range_cuts(&Literal::Int(1), &Literal::Int(3), 10).unwrap();
        assert_eq!(c, vec![Literal::Int(1), Literal::Int(2), Literal::Int(3)]);
        // 单值区间
        let c = range_cuts(&Literal::Int(5), &Literal::Int(5), 4).unwrap();
        assert_eq!(c, vec![Literal::Int(5), Literal::Int(5)]);
        // 大跨度不溢出
        let c = range_cuts(
            &Literal::Int(i64::MIN as i128),
            &Literal::Int(i64::MAX as i128),
            7,
        )
        .unwrap();
        assert_eq!(c.len(), 8);
        for w in c.windows(2) {
            match (&w[0], &w[1]) {
                (Literal::Int(a), Literal::Int(b)) => assert!(a < b, "切点必须严格递增: {:?}", c),
                _ => panic!("类型错"),
            }
        }
    }

    #[test]
    fn test_range_cuts_rejects_mixed_types() {
        let e = range_cuts(&Literal::Int(1), &Literal::Text("x".into()), 4)
            .unwrap_err()
            .to_string();
        assert!(e.contains("类型不一致"), "{}", e);
    }

    #[test]
    fn test_range_cuts_rejects_text() {
        let e = range_cuts(&Literal::Text("a".into()), &Literal::Text("z".into()), 4)
            .unwrap_err()
            .to_string();
        assert!(e.contains("list"), "{}", e);
        assert!(e.contains("hash"), "{}", e);
    }

    // ───────── 切分数学：日期（关键）─────────

    /// 日期必须按**时间跨度**均分，不是按字符串
    #[test]
    fn test_date_cuts_split_by_time_span() {
        // 2026-01-01 ~ 2026-01-31，30 天切 3 段 → 每段 10 天
        let c = range_cuts(&ts("2026-01-01"), &ts("2026-01-31 00:00:00"), 3).unwrap();
        assert_eq!(c.len(), 4);
        let d: Vec<String> = c.iter().map(|x| x.to_string()).collect();
        assert_eq!(d[0], "2026-01-01 00:00:00");
        assert_eq!(d[1], "2026-01-11 00:00:00");
        assert_eq!(d[2], "2026-01-21 00:00:00");
        assert_eq!(d[3], "2026-01-31 00:00:00");
    }

    /// 切点必须吸附到自然边界，否则服务端一个分区都裁不掉
    #[test]
    fn test_date_cuts_snap_to_day_boundary() {
        // 273 天 / 16 ≈ 17.06 天 —— 未吸附会切出 01:30:00 这种时刻
        let c = range_cuts(&ts("2026-01-01 00:00:00"), &ts("2026-10-01 00:00:00"), 16).unwrap();
        assert_eq!(c.len(), 17);
        for x in &c {
            match x {
                Literal::Timestamp(dt) => assert_eq!(
                    (dt.hour(), dt.minute(), dt.second()),
                    (0, 0, 0),
                    "切点未吸附到自然日边界: {}",
                    dt
                ),
                other => panic!("expected timestamp, got {:?}", other),
            }
        }
        let m = millis_all(&c);
        for w in m.windows(2) {
            assert!(w[0] < w[1], "切点必须严格递增: {:?}", c);
        }
    }

    /// 纯日期（Date/Date）切点保持日粒度，不冒出 00:00:00 后缀
    #[test]
    fn test_date_only_bounds_keep_date_granularity() {
        let c = range_cuts(&ts("2026-01-01"), &ts("2026-01-31"), 3).unwrap();
        assert_eq!(c[1].to_string(), "2026-01-11");
        assert!(matches!(c[1], Literal::Date(_)), "{:?}", c[1]);
    }

    /// 两端是纯日期但跨度不足一天时，绝不能把切点压到同一天
    #[test]
    fn test_date_only_short_span_does_not_collapse() {
        // 同一天，切 4 段 → 退化 1 段
        let c = range_cuts(&ts("2026-01-01"), &ts("2026-01-01"), 4).unwrap();
        assert_eq!(c.len(), 2);
    }

    /// 月长不等也不能让某段跨度过大 —— 按时间切，2 月自然就短
    #[test]
    fn test_date_cuts_handles_uneven_months() {
        let c = range_cuts(&ts("2026-02-01"), &ts("2026-05-01"), 3).unwrap();
        assert_eq!(c[1].to_string(), "2026-03-02");
        assert_eq!(c[2].to_string(), "2026-04-01");
        assert_eq!(c[3].to_string(), "2026-05-01");
    }

    /// **覆盖性**：切出的区间首尾相接、并集等于 `[lower, upper]`
    #[test]
    fn test_cuts_form_continuous_cover() {
        let cases: [(&str, &str, usize); 3] = [
            ("2026-01-01 00:00:00", "2026-10-01 00:00:00", 16),
            ("1", "7", 3),
            ("2026-03-01 08:00:00", "2026-03-01 20:00:00", 5),
        ];
        for (lo, hi, n) in cases {
            let c = range_cuts(&ts(lo), &ts(hi), n).unwrap();
            assert_eq!(c.first().unwrap(), &ts(lo), "首切点必须是下界 ({})", lo);
            assert_eq!(c.last().unwrap(), &ts(hi), "末切点必须是上界 ({})", hi);
            assert!(c.len() >= 2);
            if c[0].as_millis().is_some() {
                let m = millis_all(&c);
                for w in m.windows(2) {
                    assert!(w[0] < w[1], "区间不连续/不递增: {:?}", c);
                }
            } else {
                for w in c.windows(2) {
                    match (&w[0], &w[1]) {
                        (Literal::Int(a), Literal::Int(b)) => assert!(a < b, "{:?}", c),
                        _ => panic!("类型错"),
                    }
                }
            }
        }
    }

    /// 小时级区间：吸附粒度应随跨度下降（不能一律吸到天）
    #[test]
    fn test_sub_day_span_snaps_to_hour() {
        let c = range_cuts(&ts("2026-03-01 00:00:00"), &ts("2026-03-01 12:00:00"), 3).unwrap();
        assert_eq!(c[1].to_string(), "2026-03-01 04:00:00");
        assert_eq!(c[2].to_string(), "2026-03-01 08:00:00");
    }

    // ───────── 规划 ─────────

    #[test]
    fn test_plan_none_is_single_all_partition() {
        let spec = PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        };
        let p = plan(&spec, "T", Some("S"), None).unwrap();
        assert_eq!(p.partitions.len(), 1);
        assert_eq!(p.partitions[0].predicate, PartitionPredicate::All);
        assert_eq!(p.resolved_by, ResolvedBy::User);
    }

    #[test]
    fn test_plan_list_is_zero_probe() {
        let spec = PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["A = 1".into(), "A = 2".into()],
            ..Default::default()
        };
        assert!(spec.is_probe_free());
        let p = plan(&spec, "T", None, None).unwrap();
        assert_eq!(p.partitions.len(), 2);
        assert_eq!(
            p.partitions[1].predicate,
            PartitionPredicate::Raw("A = 2".into())
        );
    }

    /// **回归**：`planned_count` 必须按模式取值，不能一律取 `count`
    ///
    /// 实测踩到：`list` 模式（17 条 conditions、count 字段为 0）被记成 1 个分区，
    /// 于是源端子进程配额只申请到 1 个。
    #[test]
    fn test_planned_count_is_mode_aware() {
        assert_eq!(
            PartitionSpec {
                mode: PartitionMode::None,
                ..Default::default()
            }
            .planned_count(),
            1
        );
        assert_eq!(
            PartitionSpec {
                mode: PartitionMode::List,
                conditions: vec!["a".into(); 17],
                ..Default::default()
            }
            .planned_count(),
            17,
            "list 的分区数是 conditions.len()，不是 count"
        );
        assert_eq!(
            PartitionSpec {
                mode: PartitionMode::Hash,
                column: Some("C".into()),
                count: 8,
                ..Default::default()
            }
            .planned_count(),
            8
        );
        assert_eq!(
            PartitionSpec {
                mode: PartitionMode::Range,
                column: Some("C".into()),
                count: 16,
                ..Default::default()
            }
            .planned_count(),
            16
        );
        // 病态输入不能返回 0（会让下游按 0 分配资源）
        assert_eq!(
            PartitionSpec {
                mode: PartitionMode::List,
                ..Default::default()
            }
            .planned_count(),
            1
        );
    }

    #[test]
    fn test_plan_list_rejects_empty_conditions() {
        let spec = PartitionSpec {
            mode: PartitionMode::List,
            ..Default::default()
        };
        assert!(spec
            .validate()
            .unwrap_err()
            .to_string()
            .contains("conditions"));

        let spec = PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["A = 1".into(), "  ".into()],
            ..Default::default()
        };
        assert!(spec.validate().unwrap_err().to_string().contains("空条件"));
    }

    #[test]
    fn test_plan_range_with_explicit_bounds_is_zero_probe() {
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("CREATED_AT".into()),
            lower: Some(ts("2026-01-01")),
            upper: Some(ts("2026-10-01")),
            count: 4,
            ..Default::default()
        };
        assert!(spec.is_probe_free());
        let p = plan(&spec, "T", None, None).unwrap();
        assert_eq!(p.resolved_by, ResolvedBy::User);
        assert_eq!(p.partitions.len(), 4);
        // 只有最后一段闭区间，前面半开 —— 不重叠的关键
        match &p.partitions[3].predicate {
            PartitionPredicate::Range {
                upper_inclusive,
                upper,
                ..
            } => {
                assert!(*upper_inclusive);
                assert_eq!(upper.as_ref().unwrap().to_string(), "2026-10-01");
            }
            other => panic!("{:?}", other),
        }
        match &p.partitions[0].predicate {
            PartitionPredicate::Range {
                upper_inclusive,
                lower,
                ..
            } => {
                assert!(!*upper_inclusive);
                assert_eq!(lower.as_ref().unwrap().to_string(), "2026-01-01");
            }
            other => panic!("{:?}", other),
        }
        // 相邻分区首尾相接
        for w in p.partitions.windows(2) {
            let (
                PartitionPredicate::Range { upper: a, .. },
                PartitionPredicate::Range { lower: b, .. },
            ) = (&w[0].predicate, &w[1].predicate)
            else {
                panic!("类型错");
            };
            assert_eq!(a, b, "相邻分区必须首尾相接");
        }
    }

    #[test]
    fn test_plan_range_without_bounds_needs_probe() {
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("USER_ID".into()),
            count: 4,
            ..Default::default()
        };
        assert!(!spec.is_probe_free());
        let e = plan(&spec, "T", None, None).unwrap_err().to_string();
        assert!(e.contains("边界"), "{}", e);

        let p = plan(
            &spec,
            "T",
            None,
            Some(ProbedBounds::MinMax {
                lower: Literal::Int(0),
                upper: Literal::Int(100),
            }),
        )
        .unwrap();
        assert_eq!(p.resolved_by, ResolvedBy::Probe);
        assert_eq!(p.partitions.len(), 4);
    }

    // ───────── 切点路径（元数据字典表）─────────

    /// **核心不变量**：切点路径必须连续、首尾无界 —— 并集恒等于整个值域
    #[test]
    fn test_cut_points_form_total_cover() {
        let cuts: Vec<Literal> = (1..=5).map(|i| ts(&format!("2026-01-0{}", i))).collect();
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("CREATED_AT".into()),
            count: 5,
            ..Default::default()
        };
        let p = plan(
            &spec,
            "T",
            None,
            Some(ProbedBounds::CutPoints(cuts.clone())),
        )
        .unwrap();
        assert_eq!(p.resolved_by, ResolvedBy::Metadata);
        // k 个切点 → k+1 段
        assert_eq!(p.partitions.len(), cuts.len() + 1);

        let preds: Vec<&PartitionPredicate> = p.partitions.iter().map(|x| &x.predicate).collect();
        // 首段无下界、末段无上界
        match preds[0] {
            PartitionPredicate::Range { lower, .. } => assert!(lower.is_none(), "首段必须无下界"),
            other => panic!("{:?}", other),
        }
        match preds[preds.len() - 1] {
            PartitionPredicate::Range { upper, .. } => assert!(upper.is_none(), "末段必须无上界"),
            other => panic!("{:?}", other),
        }
        // 连续性：前一段的上界 == 后一段的下界
        for w in preds.windows(2) {
            let (
                PartitionPredicate::Range { upper: a, .. },
                PartitionPredicate::Range { lower: b, .. },
            ) = (w[0], w[1])
            else {
                panic!("类型错");
            };
            assert_eq!(a, b, "区间不连续");
        }
        // 切点必须来自输入（否则服务端裁不掉分区）
        for (i, c) in cuts.iter().enumerate() {
            match &preds[i].clone() {
                PartitionPredicate::Range { upper, .. } => assert_eq!(upper.as_ref(), Some(c)),
                other => panic!("{:?}", other),
            }
        }
    }

    /// 空切点也要产出 1 段（全表），而不是 0 段
    #[test]
    fn test_cut_points_empty_yields_single_range() {
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("C".into()),
            count: 4,
            ..Default::default()
        };
        let p = plan(&spec, "T", None, Some(ProbedBounds::CutPoints(vec![]))).unwrap();
        assert_eq!(p.partitions.len(), 1);
        assert_eq!(p.partitions[0].describe, "[-∞, +∞)");
    }

    #[test]
    fn test_plan_range_rejects_half_bounds() {
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("C".into()),
            lower: Some(Literal::Int(1)),
            count: 4,
            ..Default::default()
        };
        let e = spec.validate().unwrap_err().to_string();
        assert!(e.contains("成对"), "{}", e);
    }

    #[test]
    fn test_plan_hash_is_zero_probe() {
        let spec = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("USER_ID".into()),
            count: 4,
            ..Default::default()
        };
        assert!(spec.is_probe_free());
        let p = plan(&spec, "T", None, None).unwrap();
        assert_eq!(p.partitions.len(), 4);
        assert_eq!(
            p.partitions[2].predicate,
            PartitionPredicate::Hash {
                column: "USER_ID".into(),
                modulo: 4,
                remainder: 2
            }
        );
    }

    #[test]
    fn test_plan_requires_column_for_range_and_hash() {
        for mode in [PartitionMode::Range, PartitionMode::Hash] {
            let spec = PartitionSpec {
                mode,
                count: 4,
                ..Default::default()
            };
            assert!(spec.validate().unwrap_err().to_string().contains("column"));
        }
    }

    /// count 必须显式给：不给默认值，是为了不让"切法"重新被并发度悄悄决定
    #[test]
    fn test_range_and_hash_require_explicit_count() {
        for mode in [PartitionMode::Range, PartitionMode::Hash] {
            let spec = PartitionSpec {
                mode,
                column: Some("C".into()),
                ..Default::default()
            };
            let e = spec.validate().unwrap_err().to_string();
            assert!(e.contains("count"), "{}", e);
        }
    }

    /// none / list 下 count 无意义，不该报错
    #[test]
    fn test_count_ignored_for_none_and_list() {
        assert!(PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        }
        .validate()
        .is_ok());
        assert!(PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec!["A = 1".into()],
            ..Default::default()
        }
        .validate()
        .is_ok());
    }

    #[test]
    fn test_plan_partition_ids_are_unique_and_ordered() {
        let spec = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("C".into()),
            count: 3,
            ..Default::default()
        };
        let p = plan(&spec, "T", None, None).unwrap();
        let ids: Vec<&str> = p.partitions.iter().map(|x| x.id.as_str()).collect();
        assert_eq!(ids, vec!["p000", "p001", "p002"]);
    }

    // ───────── 计划文件 ─────────

    #[test]
    fn test_plan_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let pf = dir.path().join("sub").join("plan.json");
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("CREATED_AT".into()),
            lower: Some(ts("2026-01-01")),
            upper: Some(ts("2026-10-01")),
            count: 4,
            ..Default::default()
        };
        let p = plan(&spec, "T_A", Some("S"), None).unwrap();
        save_plan(&pf, &p).unwrap();

        let fp = spec.fingerprint("T_A", Some("S"));
        let back = load_plan(&pf, &fp).unwrap().unwrap();
        // 复用的切法必须逐元素一致，压测数字才可比
        assert_eq!(back, p);
    }

    #[test]
    fn test_load_plan_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_plan(&dir.path().join("nope.json"), "fp")
            .unwrap()
            .is_none());
    }

    /// 指纹不符必须**报错**，不能静默用到别的表上
    #[test]
    fn test_load_plan_rejects_fingerprint_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let pf = dir.path().join("plan.json");
        let spec = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("USER_ID".into()),
            count: 4,
            ..Default::default()
        };
        let p = plan(&spec, "T_A", None, None).unwrap();
        save_plan(&pf, &p).unwrap();

        let good = spec.fingerprint("T_A", None);
        assert!(load_plan(&pf, &good).unwrap().is_some());
        // 同一份计划用到别的表 → 拒绝
        let bad = spec.fingerprint("T_B", None);
        assert_ne!(good, bad);
        let e = load_plan(&pf, &bad).unwrap_err().to_string();
        assert!(e.contains("指纹"), "{}", e);
    }

    #[test]
    fn test_load_plan_rejects_bad_version_and_junk() {
        let dir = tempfile::tempdir().unwrap();
        let pf = dir.path().join("plan.json");
        let spec = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("C".into()),
            count: 2,
            ..Default::default()
        };
        let fp = spec.fingerprint("T", None);

        let mut p = plan(&spec, "T", None, None).unwrap();
        p.version = 999;
        save_plan(&pf, &p).unwrap();
        assert!(load_plan(&pf, &fp)
            .unwrap_err()
            .to_string()
            .contains("版本"));

        std::fs::write(&pf, "{ not json").unwrap();
        assert!(load_plan(&pf, &fp)
            .unwrap_err()
            .to_string()
            .contains("解析失败"));

        let mut p2 = plan(&spec, "T", None, None).unwrap();
        p2.partitions.clear();
        save_plan(&pf, &p2).unwrap();
        assert!(load_plan(&pf, &fp).is_err());
    }

    /// 指纹必须对切法敏感（改了 count 就不该复用旧计划）
    #[test]
    fn test_fingerprint_is_sensitive_to_split_shape() {
        let base = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("C".into()),
            count: 4,
            ..Default::default()
        };
        let mut other = base.clone();
        other.count = 8;
        assert_ne!(base.fingerprint("T", None), other.fingerprint("T", None));

        let mut other2 = base.clone();
        other2.column = Some("D".into());
        assert_ne!(base.fingerprint("T", None), other2.fingerprint("T", None));

        assert_ne!(base.fingerprint("T", None), base.fingerprint("T2", None));
        assert_ne!(
            base.fingerprint("T", Some("A")),
            base.fingerprint("T", Some("B"))
        );
    }

    // ───────── YAML ─────────

    #[test]
    fn test_yaml_range_with_dates() {
        let yaml = r#"
mode: range
column: "CREATED_AT"
lower: "2026-01-01 00:00:00"
upper: "2026-10-01 00:00:00"
count: 16
"#;
        let spec: PartitionSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.mode, PartitionMode::Range);
        assert!(matches!(spec.lower, Some(Literal::Timestamp(_))));
        assert!(spec.is_probe_free());
        spec.validate().unwrap();
    }

    #[test]
    fn test_yaml_list() {
        let yaml = r#"
mode: list
conditions:
  - "CREATED_AT >= TIMESTAMP'2026-01-01 00:00:00'"
  - "CREATED_AT < TIMESTAMP'2026-01-01 00:00:00'"
"#;
        let spec: PartitionSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.conditions.len(), 2);
        spec.validate().unwrap();
    }

    #[test]
    fn test_yaml_none_and_unknown_field() {
        let spec: PartitionSpec = serde_yaml::from_str("mode: none").unwrap();
        assert_eq!(spec.mode, PartitionMode::None);
        // 默认值
        let spec: PartitionSpec = serde_yaml::from_str("{}").unwrap();
        assert_eq!(spec.mode, PartitionMode::None);

        let e = serde_yaml::from_str::<PartitionSpec>("mode: nope")
            .unwrap_err()
            .to_string();
        assert!(e.contains("unknown variant"), "{}", e);

        // 拼错的键必须报错而不是静默忽略
        let e = serde_yaml::from_str::<PartitionSpec>("mode: none\ncound: 4")
            .unwrap_err()
            .to_string();
        assert!(e.contains("unknown field"), "{}", e);
    }
}
