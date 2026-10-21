//! dmfldr 方法参数（`source.dmfldr` 配置块）
//!
//! 强类型 + deny_unknown_fields（拼写错误即报错）+ 手册取值范围校验。
//! 默认值与《DM8 快速装载工具使用手册》2.2 参数表一致；引擎侧参数为本项目新增。

use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 数据文件字符编码（dmfldr CHARACTER_CODE）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CharacterCode {
    #[default]
    Utf8,
    Gbk,
    Gb18030,
    SingleByte,
    EucKr,
}

impl From<CharacterCode> for d2d_types::TextEncoding {
    fn from(c: CharacterCode) -> Self {
        match c {
            CharacterCode::Utf8 => Self::Utf8,
            CharacterCode::Gbk => Self::Gbk,
            CharacterCode::Gb18030 => Self::Gb18030,
            CharacterCode::SingleByte => Self::SingleByte,
            CharacterCode::EucKr => Self::EucKr,
        }
    }
}

impl CharacterCode {
    /// dmfldr 参数字面量
    pub fn as_ctl(&self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Gbk => "GBK",
            Self::Gb18030 => "GB18030",
            Self::SingleByte => "SINGLE_BYTE",
            Self::EucKr => "EUC-KR",
        }
    }
}

/// dmfldr 读取参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlDrParams {
    // ── 进程 ──
    /// dmfldr 可执行文件（PATH 名或绝对路径）
    #[serde(default = "default_executable")]
    pub executable: String,
    /// 导出工作目录（控制文件/数据文件/日志/LOB）；缺省用系统临时目录。
    /// ⚠️ 需容纳全表文本导出，请确保容量充足
    #[serde(default)]
    pub work_dir: Option<PathBuf>,

    // ── 文本格式 ──
    #[serde(default)]
    pub character_code: CharacterCode,
    /// 列分隔符（默认 `|`）
    #[serde(default = "default_field_sep")]
    pub field_seperator: String,
    /// NULL_MODE：true = 空值导出为 NULL 标记字符串（推荐，空字段无歧义）
    #[serde(default = "default_true")]
    pub null_mode: bool,
    /// NULL 标记字符串（NULL_MODE=true 时导出的空值表示）
    #[serde(default = "default_null_str")]
    pub null_str: String,
    /// 转义符：数据中含分隔符/换行/转义符本身时按 ESCAPE 规则转义。
    /// 空 = 不启用转义（要求数据不含分隔符，否则错位）
    #[serde(default)]
    pub escape: Option<String>,
    /// 导出行首打印列名（用于列名校验，保持 true）
    #[serde(default = "default_true")]
    pub colname_heading: bool,

    // ── 大字段 ──
    /// LOB 数据文件目录（空 = 与数据文件同目录）。CLIENT_LOB 恒为 TRUE（本地读回）
    #[serde(default)]
    pub lob_directory: Option<PathBuf>,
    /// CLOB 是否内联为 VARCHAR 导出
    #[serde(default)]
    pub lob_as_varchar: bool,
    /// CLOB 内联最大 MB（1..=20）
    #[serde(default = "default_lob_varchar_size")]
    pub lob_as_varchar_size: u32,

    // ── dmfldr 性能参数（手册 2.3 / 3.16）──
    /// 读文件缓冲区页大小 MB（1..=2048）
    #[serde(default = "default_buffer_node_size")]
    pub buffer_node_size: u32,
    /// 单次处理最大行数（0..=2^26-10000）
    #[serde(default = "default_read_rows")]
    pub read_rows: u64,
    /// 导出线程数；0 = 自动（CPU 核数，OUT 模式上限 16）
    #[serde(default)]
    pub task_thread_number: u32,
    /// 发送节点数（16..=65535）
    #[serde(default = "default_send_node_number")]
    pub send_node_number: u32,
    /// BDTA 批量（100..=10000）
    #[serde(default = "default_bdta_size")]
    pub bdta_size: u32,
    /// 是否压缩 BDTA（省带宽耗 CPU）
    #[serde(default)]
    pub compress_flag: bool,
    /// MPP/DPC 单文件导出（本项目强制 true，简化分块）
    #[serde(default = "default_true")]
    pub single_file: bool,

    /// 错误信息记录级别（0..=4）
    #[serde(default = "default_log_level")]
    pub log_level: u8,

    // ── 产物内部的并行度：**住在这里，因为是连接器自己的事** ──
    /// 一份导出产物内部的解析并行度（**缺省 1 = 单文件顺序解析**）
    ///
    /// # 为什么它是连接器的旋钮，而不是拓扑的一部分
    ///
    /// 内核指派给连接器的只有一件事：**同时最多几个任务在往 `source_pool` 里放**
    /// （`flow.source.concurrency`）。一个任务**内部**怎么干活 —— 起几个进程、
    /// 把产物切几块、并行解析几个 —— 全是这个连接器自己的实现选择。
    /// 内核不看、不管，也不该替它安排：让内核去切块，等于把"dmfldr 的产物长什么样"
    /// 写进通用拓扑。
    ///
    /// 做得好坏是连接器的竞争力：别人写的连接器内部并行得更聪明，那是它赢。
    ///
    /// # 缺省为什么是 1，而不是"自动"
    ///
    /// **`TABLE=` 模式的特性就是"一个进程整表读"**，单文件顺序解析与之一致。
    /// 大表不分片读，是你选了这个模式（要么表本来就小，要么你该用 `partition`），
    /// 不是连接器该偷偷帮你拆成多份 —— 同"用一条 JDBC 连接读大表"，连接池不会替你
    /// 拆成多连接。真拆了，**CPU 压力谁管、内存压力谁管？**
    ///
    /// 要快就把这个数调大：代价是每个 source_task 多占 `parse_parallel × 单批` 内存，
    /// 而**这笔账会进启动期账本**（见 `unit_footprint`），装不下就拒绝启动。
    #[serde(default = "default_parse_parallel")]
    pub parse_parallel: usize,

    // ── 直通面的物理块大小（与 sink 批次的关系是**性能关键**）──
    /// 流式直通时每块的目标大小（MB）；0 = 缺省 4MB
    ///
    /// # 为什么它要和 `flow.sink.batch_bytes` 对齐
    ///
    /// sink 的攒批是从池里取块拼到 `batch_bytes`。如果源端块 **小于** 批大小，
    /// 一个批次就会由多个 shard 的块拼成 —— 一次 Stream Load 里出现多个日期分区，
    /// 实测单请求耗时增加约 50%（30M 两轮：4MB 块拼批 41s vs 单 shard 块 31s）。
    /// 所以直通场景应把本值设到 **≥ sink.batch_bytes 对应的块大小**，
    /// 并相应放大 `flow.source.pool.capacity`（每路在飞至少 1 个块）。
    #[serde(default)]
    pub physical_block_mb: u32,
    /// 每个分片的**首块**大小（MB）；0 = 与 [`Self::physical_block_mb`] 相同
    ///
    /// 首块决定第一条 Stream Load 何时发车：它必须等整块攒满。稳态用大块摊薄
    /// 每请求成本，首块用小一点的块换启动时间。仍须 ≥ sink 批次，否则会拼批。
    #[serde(default)]
    pub first_block_mb: u32,

    // ── 导出并行化：这里**没有**这个字段，而且不该有 ──
    //
    // 这里曾经有 `export_mode` / `partition_column` / `export_processes`，全删了：
    //
    //   export_mode      → 由 source.partition 派生（ExportMode::for_spec）
    //   partition_column → 并入 source.partition.column
    //   export_processes → **不再存在**：并发不归 dmfldr 管，也不归"引擎给个数"管
    //
    // 最后一个的演变值得记一笔，因为它走过三版：
    //
    //   ① 用户旋钮            → 逼用户理解"dmfldr 是进程"，换 JDBC 源时同一个数
    //                            又变成"连接数"，得重新学一遍
    //   ② `grant.slots: usize` → 引擎"告诉"连接器一个数，连接器自己守着。
    //                            **它完全可以不听**（本仓库的 odbc 就是完全不读 grant 的那个）
    //   ③ 一道闸门（`SourceGrant`）→ 每开一个导出子进程先去取一张券，拿不到就等。
    //                            绕不过去了，但**方向错了**：那是内核伸手管连接器内部
    //   ④ 现在：**连接器连闸门都没有**。一个分片 = 一个任务，券在**内核的派发器**手里，
    //            按任务发 —— 这里连"数字"都看不到，也压根不需要参与限流。
    //
    // 所以本结构里没有任何跟并发有关的字段，这是**结构性**的结论，不是遗漏。
    /// `partitioned` 模式的分片规划方式（缺省 `auto`）
    #[serde(default)]
    pub partition_plan: PartitionPlan,
    /// 分片计划文件（JSON）
    ///
    /// 设了它之后：**文件存在就复用**（连元数据查询都省掉），不存在才做一次发现并落盘。
    ///
    /// 为什么要它：
    /// - **可复现**。所有人跑同一份计划，分片切法完全一致，压测数字才可比
    ///   （否则每次探测出的切片不同，导出侧的均衡性也跟着抖）。
    /// - **生产上发现只做一次**。源表分区布局是稳定的，迁移脚本没必要每次重算；
    ///   分区布局变了用 `plan_refresh` 覆盖即可。
    #[serde(default)]
    pub plan_file: Option<PathBuf>,
    /// 强制重新发现并覆盖 `plan_file`（缺省 false = 存在即复用）
    #[serde(default)]
    pub plan_refresh: bool,
}

/// 已删除的字段 → 迁移指引
///
/// serde 的 `unknown field` 报错只说"不认识"，不说"该怎么办"。对**刚刚删掉**的
/// 那几个字段（存量配置里多半还留着）给一条能直接照做的提示，
/// 比让用户去翻文档强得多。
fn reject_removed_fields(v: &serde_json::Value) -> D2DResult<()> {
    const REMOVED: [(&str, &str); 3] = [
        (
            "export_mode",
            "已删除 —— 导出模式现在**由 source.partition 派生**，把这一行删掉即可：
               · 不配 partition（或 mode: none）→ 单进程整表导出
               · 配了 list / range / hash        → 分片并发导出",
        ),
        (
            "partition_column",
            "已删除 —— 分区列现在写在 source.partition.column 里。",
        ),
        (
            "parse_chunks",
            "已改名 —— 它就是现在的 `parse_parallel`（一份导出产物内部的解析并行度），\
             但**语义和缺省值都变了**：旧值是'分几块'，现在是'几个解析任务'，而且\
             缺省从'按文件大小自动切'改成 **1（单文件顺序解析）**。\
             想让大文件的解析快一点，就显式写 parse_parallel: <N>，\
             并把这一行删掉。",
        ),
    ];
    for (field, hint) in REMOVED {
        if v.get(field).is_some() {
            return Err(D2DError::config(format!(
                "source.dmfldr.{} {}",
                field, hint
            )));
        }
    }
    Ok(())
}

/// 分片规划方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PartitionPlan {
    /// 优先用分区元数据边界切范围（~0.1s，不扫数据）；表未分区时自动退化为 `probe`
    #[default]
    Auto,
    /// 只用分区元数据。**表必须是按 `partition_column` 分区的**，否则报错而不是退化
    Metadata,
    /// 只用全表 `GROUP BY` 探测 + LPT 装箱。要扫全表（实测 1915 万行 ~31s），
    /// 但切片按真实行数均衡，且探测总数可作为独立的行数对账基准
    Probe,
}

/// dmfldr 导出模式 —— **由分区配置派生，不是用户旋钮**
///
/// ```text
/// source.partition.mode = none           → Table       （TABLE=  单进程整表导出）
/// source.partition.mode = list|range|hash → Partitioned （SQLFILE= 分片并发导出）
/// ```
///
/// # 为什么它不该是用户配的
///
/// 它和 `source.partition` 说的是同一件事。曾经要求用户**同时**配
/// `export_mode: partitioned` 和 `partition`，于是：
///
/// - 只配 `partition` → 报"TABLE 模式不支持分片"
/// - 只配 `export_mode` → 报"partitioned 需要分区列"
/// - 两个都配但模式对不上 → 冲突
///
/// 用户得同时理解两个旋钮才知道怎么让它跑起来 —— 这正是"分片与并发的链路
/// 不清晰"的直接来源。而它本来就是个纯派生量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportMode {
    /// 单进程 `TABLE=` 整表导出。
    ///
    /// 这不是"慢路"——它是**小表与增量同步的正确选择**：
    /// 几万行的表分成 300 片，只会换来 300 个进程的启动开销。
    /// 1915 万行的整表导出实测 122s，切 16 片并发才降到 20s——
    /// **只有数据量足够大时，分片才划算。**
    #[default]
    Table,
    /// 按 `partition.column` 切分、多进程 `SQLFILE` + `WHERE` 并发导出。
    /// 快路：同一份数据实测 **20s / 16 进程**（6× 于单进程），且各分片文件独立，
    /// 天然支持「导出一片就解析一片」。
    ///
    /// 为什么这条路更快：dmfldr 是客户端工具，单进程吞吐受限于「取数→格式化→写文件」
    /// 的串行度；`TABLE=` 只能整表拉，`SQLFILE=` 带 WHERE 可让服务端按分区裁剪，
    /// 再靠**进程级并行**把吞吐叠上去。这与「SQL 比 TABLE 慢」的直觉相反 ——
    /// 慢的是单个进程，快的是并行度。
    Partitioned,
}

impl ExportMode {
    /// **派生**：由 `source.partition` 决定用哪种导出模式
    ///
    /// 这是整条链路的接缝处 —— 用户只声明"怎么切"，导出模式随之而定。
    pub fn for_spec(spec: &d2d_connector::SourceSpec) -> D2DResult<Self> {
        let partitioned = spec
            .effective_partition()?
            .map(|p| p.mode != d2d_connector::PartitionMode::None)
            .unwrap_or(false);
        Ok(if partitioned {
            Self::Partitioned
        } else {
            Self::Table
        })
    }
}

fn default_executable() -> String {
    "dmfldr".to_string()
}
fn default_field_sep() -> String {
    "|".to_string()
}
fn default_true() -> bool {
    true
}
fn default_null_str() -> String {
    "NULL".to_string()
}
fn default_lob_varchar_size() -> u32 {
    10
}
fn default_buffer_node_size() -> u32 {
    64
}
fn default_read_rows() -> u64 {
    200_000
}
fn default_send_node_number() -> u32 {
    20
}
fn default_bdta_size() -> u32 {
    5000
}
fn default_log_level() -> u8 {
    3
}
fn default_parse_parallel() -> usize {
    1
}

impl Default for FlDrParams {
    fn default() -> Self {
        Self {
            executable: default_executable(),
            work_dir: None,
            character_code: CharacterCode::default(),
            field_seperator: default_field_sep(),
            null_mode: true,
            null_str: default_null_str(),
            escape: None,
            colname_heading: true,
            lob_directory: None,
            lob_as_varchar: false,
            lob_as_varchar_size: default_lob_varchar_size(),
            buffer_node_size: default_buffer_node_size(),
            read_rows: default_read_rows(),
            task_thread_number: 0,
            send_node_number: default_send_node_number(),
            bdta_size: default_bdta_size(),
            compress_flag: false,
            single_file: true,
            log_level: default_log_level(),
            parse_parallel: default_parse_parallel(),
            physical_block_mb: 0,
            first_block_mb: 0,
            partition_plan: PartitionPlan::default(),
            plan_file: None,
            plan_refresh: false,
        }
    }
}

impl FlDrParams {
    /// 从 `source.properties.dmfldr` 解析（缺失 = 默认值）
    ///
    /// 落在 `properties` 而不是顶层：`dmfldr` 是**这个方法特有的**参数块，
    /// 引擎不认识它，也不该认识 —— 换个源（比如 JDBC）根本没有这个东西。
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let Some(v) = props.get("dmfldr") else {
            return Ok(Self::default());
        };
        if v.is_null() {
            return Ok(Self::default());
        }
        reject_removed_fields(v)?;
        serde_json::from_value(v.clone())
            .map_err(|e| D2DError::config(format!("invalid source.properties.dmfldr: {}", e)))
    }

    /// **驱动每批取多少行**（`source.properties.fetch_size`）
    ///
    /// 放在 `properties` 里而不是 `flow.source`：它管的是**数据库往返粒度**
    /// （一次网络来回拉多少行），不是内存也不是并发。
    /// dmfldr 这条路径不用它（导出是整文件产出），所以只有 ODBC 路径读它。
    pub fn fetch_size(props: &serde_json::Map<String, serde_json::Value>) -> usize {
        props
            .get("fetch_size")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(10_000)
            .max(1)
    }

    /// 导出模式相关的取值校验（启动期 fail-fast）
    ///
    /// 与 [`Self::validate`] 分开是因为这些检查**取决于模式**，而模式由
    /// `source.partition` 派生（[`ExportMode::for_spec`]），本结构看不到它。
    ///
    /// SQL 模式不生成控制文件，相关选项一律走 dmfldr 缺省。与其静默忽略用户配置
    /// （会造成切分/空值判定错位），不如启动期直接拒绝。
    pub fn validate_for(&self, mode: ExportMode) -> D2DResult<()> {
        if mode != ExportMode::Partitioned {
            return Ok(());
        }
        let mut errs: Vec<String> = Vec::new();
        if self.field_seperator != "|" {
            errs.push(format!(
                "partition 模式下不支持自定义 field_seperator（无控制文件，dmfldr 缺省为 '|'），当前为 '{}'",
                self.field_seperator
            ));
        }
        if self.null_mode {
            errs.push(
                "partition 模式下不支持 null_mode=true（无控制文件，走 dmfldr 缺省：空值 = 空串）；                 请设为 null_mode: false"
                    .into(),
            );
        }
        if self.escape.is_some() {
            errs.push(
                "partition 模式下不支持 escape（无控制文件，走 dmfldr 缺省：不启用转义）".into(),
            );
        }
        if self.colname_heading {
            errs.push(
                "partition 模式下不支持 colname_heading（SQL 模式不产出列头）；请设为 false".into(),
            );
        }
        if !errs.is_empty() {
            return Err(D2DError::config(format!(
                "source.dmfldr 参数与分片模式冲突：{}",
                errs.join("; ")
            )));
        }
        Ok(())
    }

    /// 手册取值范围校验（启动期 fail-fast，与模式无关的部分）
    pub fn validate(&self) -> D2DResult<()> {
        let mut errs: Vec<String> = Vec::new();
        if self.field_seperator.is_empty() || self.field_seperator.len() > 255 {
            errs.push(format!(
                "field_seperator length must be 1..=255 bytes, got {}",
                self.field_seperator.len()
            ));
        }
        if let Some(esc) = &self.escape {
            if esc.chars().count() != 1 {
                errs.push("escape must be exactly 1 character".to_string());
            }
        }
        if self.null_str.len() > 128 {
            errs.push("null_str length must be <= 128 bytes".to_string());
        }
        if self.lob_as_varchar && !(1..=20).contains(&self.lob_as_varchar_size) {
            errs.push(format!(
                "lob_as_varchar_size must be 1..=20, got {}",
                self.lob_as_varchar_size
            ));
        }
        if !(1..=2048).contains(&self.buffer_node_size) {
            errs.push(format!(
                "buffer_node_size must be 1..=2048 MB, got {}",
                self.buffer_node_size
            ));
        }
        if self.read_rows > ((1u64 << 26) - 10_000) {
            errs.push(format!(
                "read_rows must be <= 2^26-10000, got {}",
                self.read_rows
            ));
        }
        if self.task_thread_number > 128 {
            errs.push(format!(
                "task_thread_number must be 1..=128, got {}",
                self.task_thread_number
            ));
        }
        if !(16..=65535).contains(&self.send_node_number) {
            errs.push(format!(
                "send_node_number must be 16..=65535, got {}",
                self.send_node_number
            ));
        }
        if !(100..=10000).contains(&self.bdta_size) {
            errs.push(format!(
                "bdta_size must be 100..=10000, got {}",
                self.bdta_size
            ));
        }
        if self.log_level > 4 {
            errs.push(format!("log_level must be 0..=4, got {}", self.log_level));
        }
        if !(1..=128).contains(&self.parse_parallel) {
            errs.push(format!(
                "parse_parallel must be 1..=128, got {}（1 = 单文件顺序解析；\
                 调大要多占 parse_parallel × 单批 的内存）",
                self.parse_parallel
            ));
        }
        if !self.single_file {
            errs.push(
                "single_file must be true: MPP/DPC multi-file export is not supported yet \
                 (connector-side chunk planning requires a single data file)"
                    .to_string(),
            );
        }
        if !errs.is_empty() {
            return Err(D2DError::config(format!(
                "invalid source.dmfldr params: {}",
                errs.join("; ")
            )));
        }
        Ok(())
    }

    /// 生效的 dmfldr 导出线程数（OUT 模式上限 16，手册 2.3）
    pub fn effective_task_threads(&self) -> u32 {
        if self.task_thread_number == 0 {
            let cpus = std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(4);
            cpus.clamp(2, 16)
        } else {
            self.task_thread_number.min(16)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_params() {
        let p = FlDrParams::default();
        assert_eq!(p.field_seperator, "|");
        assert!(p.null_mode);
        assert_eq!(p.null_str, "NULL");
        assert!(p.colname_heading);
        assert!(p.validate().is_ok());
        assert!(p.effective_task_threads() >= 2);
    }

    #[test]
    fn test_from_extra_empty_and_missing() {
        let extra = serde_json::Map::new();
        let p = FlDrParams::from_props(&extra).unwrap();
        assert_eq!(p.executable, "dmfldr");

        let mut extra2 = serde_json::Map::new();
        extra2.insert("dmfldr".into(), serde_json::Value::Null);
        assert!(FlDrParams::from_props(&extra2).is_ok());
    }

    #[test]
    fn test_deny_unknown_fields() {
        let mut extra = serde_json::Map::new();
        extra.insert("dmfldr".into(), json!({"field_seperatorr": "|"}));
        let err = FlDrParams::from_props(&extra).unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{}", err);
    }

    #[test]
    fn test_range_validation() {
        let p = FlDrParams {
            buffer_node_size: 4096,
            read_rows: u64::MAX,
            task_thread_number: 200,
            ..Default::default()
        };
        let err = p.validate().unwrap_err().to_string();
        assert!(err.contains("buffer_node_size"), "{}", err);
        assert!(err.contains("read_rows"), "{}", err);
        assert!(err.contains("task_thread_number"), "{}", err);
    }

    #[test]
    fn test_escape_must_be_single_char() {
        let p = FlDrParams {
            escape: Some("**".into()),
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }

    /// 一个「改一个字段」的用例：名字 + 改法
    type Case = (&'static str, fn(&mut FlDrParams));

    /// 分片模式下**仍然**要拒绝的那些（无控制文件 → 取值必须与 dmfldr 缺省一致）
    #[test]
    fn test_validate_for_partitioned_rejects_control_file_options() {
        let base = || FlDrParams {
            null_mode: false,
            colname_heading: false,
            ..Default::default()
        };
        let cases: [Case; 4] = [
            ("null_mode", |p| p.null_mode = true),
            ("colname_heading", |p| p.colname_heading = true),
            ("escape", |p| p.escape = Some("\\".to_string())),
            ("field_seperator", |p| p.field_seperator = ",".to_string()),
        ];
        for (needle, mutate) in cases {
            let mut p = base();
            mutate(&mut p);
            let e = p
                .validate_for(ExportMode::Partitioned)
                .unwrap_err()
                .to_string();
            assert!(e.contains(needle), "期望报 {}，实际: {}", needle, e);
        }
        // 而 TABLE 模式下这些都合法 —— 有控制文件，转义/空值标记都能表达
        let p = FlDrParams {
            escape: Some("\\".to_string()),
            ..Default::default()
        };
        assert!(
            p.validate_for(ExportMode::Table).is_ok(),
            "TABLE 模式有控制文件，不该拒绝转义"
        );
    }

    /// **回归**：删掉的字段必须给出**可照做的**迁移提示，而不是 serde 的
    /// "unknown field"
    #[test]
    fn test_removed_fields_give_migration_hint() {
        let mut extra = serde_json::Map::new();
        extra.insert(
            "dmfldr".into(),
            serde_json::json!({"export_mode": "partitioned"}),
        );
        let e = FlDrParams::from_props(&extra).unwrap_err().to_string();
        assert!(e.contains("已删除"), "{}", e);
        assert!(e.contains("source.partition"), "{}", e);

        let mut extra2 = serde_json::Map::new();
        extra2.insert(
            "dmfldr".into(),
            serde_json::json!({"partition_column": "CREATED_AT"}),
        );
        let e2 = FlDrParams::from_props(&extra2).unwrap_err().to_string();
        assert!(e2.contains("partition.column"), "{}", e2);
    }

    /// **链路接缝**：导出模式完全由 `source.partition` 派生
    ///
    /// 这是"分片 → 导出方式"那条链的测试锚点：用户只声明怎么切，
    /// 导出模式（TABLE= 单进程 / SQLFILE= 分片并发）随之而定，
    /// 不再需要同时配两个旋钮。
    #[test]
    fn test_export_mode_is_derived_from_partition() {
        use d2d_connector::{PartitionMode, PartitionSpec, SourceSpec};

        let mk = |spec: Option<PartitionSpec>| {
            serde_json::from_value::<SourceSpec>(serde_json::json!({
                "connector": "dameng",
                "connection": {"host": "h", "user": "u"},
                "table": "T",
            }))
            .map(|mut s| {
                s.partition = spec;
                s
            })
            .unwrap()
        };

        // 不配 partition → 单进程整表导出（小表/增量的正确选择）
        assert_eq!(ExportMode::for_spec(&mk(None)).unwrap(), ExportMode::Table);
        // 显式 none → 同上
        assert_eq!(
            ExportMode::for_spec(&mk(Some(PartitionSpec {
                mode: PartitionMode::None,
                ..Default::default()
            })))
            .unwrap(),
            ExportMode::Table
        );
        // 任一切分模式 → 分片并发导出
        for mode in [PartitionMode::List, PartitionMode::Hash] {
            let spec = PartitionSpec {
                mode,
                column: Some("C".into()),
                count: 4,
                conditions: vec!["A = 1".into()],
                ..Default::default()
            };
            assert_eq!(
                ExportMode::for_spec(&mk(Some(spec))).unwrap(),
                ExportMode::Partitioned,
                "mode={:?} 应当走分片导出",
                mode
            );
        }
        assert_eq!(
            ExportMode::for_spec(&mk(Some(PartitionSpec {
                mode: PartitionMode::Range,
                column: Some("CREATED_AT".into()),
                count: 8,
                ..Default::default()
            })))
            .unwrap(),
            ExportMode::Partitioned
        );
    }
}
