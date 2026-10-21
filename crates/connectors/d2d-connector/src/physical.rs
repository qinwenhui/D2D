//! 物理载荷面（physical plane）：字节级直通能力
//!
//! ## 为什么要有第二个"面"
//!
//! 引擎原本只有一条链路：
//!
//! ```text
//! 源文本 --解析--> Arrow RecordBatch --逐格格式化--> 目标文本
//! ```
//!
//! 这条链路是**全保真**的：Arrow 承载类型语义，任何转换算子都能插进来。它是缺省路径，
//! 也是本项目的核心能力，**不因性能优化而削弱**。
//!
//! 但纯搬运场景（无转换算子）走这条链路是纯亏：37 列 × 1915 万行 = **7 亿次**
//! 「取 Arrow 值 → 格式化成字符串」，而字节内容与源端导出文件里的原文**一模一样**。
//! 实测这部分开销让加载段从 ~106s 涨到 ~200s。
//!
//! 因此引入第二条链路 —— 物理面：
//!
//! ```text
//! 源文件块 --(原样字节)--> HTTP body
//! ```
//!
//! ## 协商，而不是猜测
//!
//! 直通**不是**「优化开关」，而是一次显式协商：
//!
//! - 源用 [`PhysicalSource::physical_format`] 声明「我能产出什么样的字节」
//! - 目标用 [`PhysicalSink::accepts_physical`] 回答「这种字节我能不能直接吃」
//! - 只有双方一致、**且转换算子链为空**时，才走直通
//!
//! 任何一条不满足，[`crate`] 的调用方（执行计划层）自动回落逻辑面，并记录回落原因。
//! 也就是说：**加了转换算子，直通自动失效，一切照旧走 Arrow** —— 转换能力不会丢。
//!
//! ## 安全边界
//!
//! 直通跳过了引擎的类型校验，所以边界必须显式收窄：
//!
//! 1. 源声明的编码必须与目标实际配置逐项相等（分隔符/行分隔符/字符集）
//! 2. 目标可以回报 `forbidden` 字节集 —— 载荷中一旦出现即判定不安全
//!    （典型：目标 CSV 读入器把反斜杠当转义符，而源导出未做转义 → 会静默改值）
//! 3. 直通路径在**每个字节块入 body 前**做一次 `memchr` 扫描，命中即整任务失败。
//!    代价≈0（本来就要读这些字节），效果是**把静默污染转成大声失败**。

use async_trait::async_trait;
use d2d_core::D2DResult;
use serde::{Deserialize, Serialize};

use crate::source::SourceSplit;

/// 载荷字符编码
///
/// 直通要求源与目标字符集一致 —— 引擎不再代做转码（转码即解析）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Charset {
    Utf8,
    Gbk,
}

impl Charset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Utf8 => "utf-8",
            Self::Gbk => "gbk",
        }
    }
}

/// CSV 方言：描述字节流的结构约定
///
/// 各字段语义是「**产出侧保证**」而非「消费侧期望」：
/// `escape: None` 意为「产出侧保证不需要任何转义」，而不是「目标不必转义」。
/// 目标若自身仍会解释转义序列，必须通过 [`PhysicalCompat::Accept::forbidden`] 告知。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvDialect {
    /// 列分隔符（单字节；多字节分隔符不支持直通）
    pub column_separator: u8,
    /// 行分隔符（单字节）
    pub line_delimiter: u8,
    /// 引号符；`None` = 不做引号包裹
    pub quote: Option<u8>,
    /// 转义符；`None` = 产出侧未做任何转义
    pub escape: Option<u8>,
    /// 空值字面量
    ///
    /// - `None` = 空值就是空字段（目标自己决定空字段算什么）
    /// - `Some("\\N")` = 源端显式产出目标端的 NULL token（Doris 的固定空值字面量）。
    ///   声明它同时承诺：**值里不再出现别的反斜杠** —— 否则 `\N` 与数据无法区分。
    ///   目标端据此可以不再把 `\` 列为受限字节（见 `d2d-doris` 的 `judge`）。
    /// - `Some(其它)` = 自定义字面量，目标若不认识必须拒绝直通
    pub null_literal: Option<String>,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            column_separator: b'|',
            line_delimiter: b'\n',
            quote: None,
            escape: None,
            null_literal: None,
        }
    }
}

/// 物理载荷格式（自描述）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalFormat {
    Csv {
        dialect: CsvDialect,
        charset: Charset,
        /// 列名与列序 —— 目标据此做**按名映射**（源列序常与目标表列序不同）
        columns: Vec<String>,
    },
}

impl PhysicalFormat {
    pub fn csv(dialect: CsvDialect, charset: Charset, columns: Vec<String>) -> Self {
        Self::Csv {
            dialect,
            charset,
            columns,
        }
    }

    /// 列名（供目标生成映射头）
    pub fn columns(&self) -> &[String] {
        match self {
            Self::Csv { columns, .. } => columns,
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Csv {
                dialect,
                charset,
                columns,
            } => format!(
                "csv(sep={:?}, line={:?}, quote={:?}, escape={:?}, null={:?}, charset={}, cols={})",
                dialect.column_separator as char,
                dialect.line_delimiter as char,
                dialect.quote.map(|c| c as char),
                dialect.escape.map(|c| c as char),
                dialect.null_literal,
                charset.as_str(),
                columns.len()
            ),
        }
    }
}

/// 目标对物理格式的兼容裁决
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalCompat {
    /// 可直接消费。
    ///
    /// `forbidden` = 载荷中**不得出现**的字节。出现即说明「产出侧的保证不成立」，
    /// 继续直通会静默改值，因此执行层会直接失败而不是硬着头皮发。
    Accept { forbidden: Vec<u8> },
    /// 不能直接消费，必须回落逻辑面；字符串是给运维看的理由
    Reject(String),
}

/// 一个物理字节块
///
/// **不变量：块边界与行边界对齐** —— 每个块含整数条完整记录。
/// 有了它，消费者可以直接把块首尾相接拼成 body，而 `rows` 也总是精确值。
#[derive(Debug, Clone)]
pub struct PhysicalBlock {
    pub bytes: Vec<u8>,
    /// 本块包含的记录条数
    pub rows: u64,
}

impl PhysicalBlock {
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// 物理字节块流
pub type PhysicalStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = D2DResult<PhysicalBlock>> + Send>>;

/// 源的物理载荷能力（可选实现）
///
/// 只有「导出产物本身就是目标可吃的文本」的读取方式才该实现它。
/// dmfldr 是典型；ODBC 游标读出来的是行对象，没有物理形态，不实现。
#[async_trait]
pub trait PhysicalSource: Send + Sync {
    /// 本源的物理载荷格式；`None` = 不支持物理面（调用方回落逻辑面）
    fn physical_format(&self) -> Option<PhysicalFormat>;

    /// 打开一个分片的物理字节流
    ///
    /// 实现方负责跳过表头等非数据字节，产出的块必须行对齐。
    async fn read_physical(&self, split: &SourceSplit) -> D2DResult<PhysicalStream>;
}

/// 目标的物理载荷能力（可选实现）
#[async_trait]
pub trait PhysicalSink: Send + Sync {
    /// 裁决能否直接消费该物理格式
    fn accepts_physical(&self, fmt: &PhysicalFormat) -> PhysicalCompat;

    /// 开启物理写入会话（每份数据一条）
    async fn begin_physical(&self, fmt: &PhysicalFormat) -> D2DResult<Box<dyn PhysicalSession>>;
}

/// 物理写入会话
///
/// 语义与 [`crate::sink::SinkSession`] 对齐（不攒批、提交、回滚），但载体是字节而非 Arrow。
///
/// # 为什么 `write_block` 收**所有权**
///
/// 一次写入的载荷要一路交到 HTTP 层（`.body(...)` 要 `'static` 的所有权）。
/// 若签名是 `&[u8]`，实现方就只能拷一份 —— 而**借出去的那份在整个请求期间还活着**，
/// 于是同一批字节在内存里有两份（实测 8 路 × 100 MB 批 ≈ 800 MB 白占，见 基准实测）。
/// 收 [`PhysicalBlock`] 就能零拷贝移交（`Bytes::from(block.bytes)`）。
#[async_trait]
pub trait PhysicalSession: Send + Sync {
    /// 写入一个字节块，**立即发出去**。`block.rows` 由产出侧精确计数，供收尾行数对账。
    ///
    /// 返回**已确认落盘**的行数。
    async fn write_block(&mut self, block: PhysicalBlock) -> D2DResult<u64>;

    /// 提交会话
    async fn commit(&mut self) -> D2DResult<u64>;

    /// 回滚/取消
    async fn abort(&mut self) -> D2DResult<()>;
}
