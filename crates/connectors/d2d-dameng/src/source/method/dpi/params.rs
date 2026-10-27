//! DPI 方法参数（`source.properties.dpi`）
//!
//! 与 dmfldr 的参数块彼此独立：两套方法没有共享的旋钮，改一边不会影响另一边。

use crate::source::method::dmfldr::params::CharacterCode;
use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 每次 `dpi_fetch` 拉多少行（数组取数）
///
/// spike 实测 4000 是拐点：单连接 122k 行/s，8000 / 16000 没有提升。
const DEFAULT_FETCH_ROWS: usize = 4000;
/// 单列缓冲下限。实际宽度会按 `desc_column` 上报的列宽放大，见 `session::column_width`。
const DEFAULT_COLUMN_BYTES: usize = 256;
/// 物理直通载荷的字段分隔符（缺省 `|`，配 Doris Stream Load 的 `column_separator`）
const DEFAULT_COLUMN_SEPARATOR: &str = "|";
/// 物理直通载荷的 NULL 字面量（缺省 `\N`，Doris 的 null_value 口径）
///
/// 空串 = **NULL 输出为裸空字段** —— qmdb CSV 的约定（协议 §2.1：空字段即 NULL、
/// 没有 NULL 字面量机制）。
const DEFAULT_NULL_LITERAL: &str = "\\N";
/// 稳态物理块（MB）—— 与 `flow.sink.batch_bytes` 对齐是性能关键，理由同 dmfldr
const DEFAULT_BLOCK_MB: u32 = 64;
/// 首块（MB）：先发一车，别等 64MB 攒满
const DEFAULT_FIRST_BLOCK_MB: u32 = 33;

/// DPI 读取参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DpiParams {
    /// DPI 动态库路径；空 = 按平台缺省名在 `LD_LIBRARY_PATH` / 系统路径里找
    ///
    /// 例：`/opt/dmdbms/drivers_odbc/libdmdpi.so`
    #[serde(default)]
    pub library: Option<PathBuf>,
    /// 每次取数的行数（数组取数粒度）
    #[serde(default = "default_fetch_rows")]
    pub fetch_rows: usize,
    /// 单列缓冲**下限**（字节）；实际宽度取「本值」与「desc 列宽 × 4 + 1」的较大者
    ///
    /// 列宽按 UTF-8 最坏 4 字节/字符放大，宁可多占一点，也不要静默截断。
    #[serde(default = "default_column_bytes")]
    pub column_bytes: usize,
    /// 会话字符集声明（**不是设置**，是"达梦会话实际吐什么字节"的声明）
    ///
    /// 只有 `utf8` / `gbk` 能走物理直通（Doris 侧只接受 UTF-8；GBK 会被目标拒绝并
    /// 自动回落逻辑面）；其他编码只走逻辑面。
    #[serde(default)]
    pub character_code: CharacterCode,
    /// 稳态物理块大小（MB）；0 = 64
    ///
    /// 必须 **≥** `flow.sink.batch_bytes` 对应的大小 —— 否则一个 sink 批次会由多个
    /// shard 的块拼成，跨多个日期分区，实测单请求慢约 50%。
    #[serde(default = "default_block_mb")]
    pub physical_block_mb: u32,
    /// 每个分片的首块大小（MB）；0 = 与 `physical_block_mb` 相同
    #[serde(default = "default_first_block_mb")]
    pub first_block_mb: u32,
    /// 直通载荷的字段分隔符（**单字节**；缺省 `|`）
    ///
    /// 这是「源端产出什么」的声明，必须与目标端的解析口径一致：qmdb CSV 固定 `,`
    /// 且不可配 → 走 qmdb 直通时在这里配 `","`。分隔符一变，值内受限字节集随之
    /// 变（见 `format::RowBlockWriter`）。
    #[serde(default = "default_column_separator")]
    pub column_separator: String,
    /// 直通载荷的 NULL 字面量；缺省 `\N`（Doris null_value 口径）
    ///
    /// - `\N`：NULL → 字面 `\N`，同时承诺**值里没有别的反斜杠**（`\` 进受限集）
    /// - `""`（空串）：NULL → 裸空字段 —— qmdb CSV 的约定（目标按"空字段=NULL"解释）
    #[serde(default = "default_null_literal")]
    pub null_literal: String,
}

fn default_fetch_rows() -> usize {
    DEFAULT_FETCH_ROWS
}
fn default_column_bytes() -> usize {
    DEFAULT_COLUMN_BYTES
}
fn default_block_mb() -> u32 {
    DEFAULT_BLOCK_MB
}
fn default_first_block_mb() -> u32 {
    DEFAULT_FIRST_BLOCK_MB
}
fn default_column_separator() -> String {
    DEFAULT_COLUMN_SEPARATOR.to_string()
}
fn default_null_literal() -> String {
    DEFAULT_NULL_LITERAL.to_string()
}

impl Default for DpiParams {
    fn default() -> Self {
        Self {
            library: None,
            fetch_rows: DEFAULT_FETCH_ROWS,
            column_bytes: DEFAULT_COLUMN_BYTES,
            character_code: CharacterCode::Utf8,
            physical_block_mb: DEFAULT_BLOCK_MB,
            first_block_mb: DEFAULT_FIRST_BLOCK_MB,
            column_separator: default_column_separator(),
            null_literal: default_null_literal(),
        }
    }
}

impl DpiParams {
    /// 从 `source.properties.dpi` 解析（缺失 = 默认值）
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let Some(v) = props.get("dpi") else {
            return Ok(Self::default());
        };
        if v.is_null() {
            return Ok(Self::default());
        }
        serde_json::from_value(v.clone())
            .map_err(|e| D2DError::config(format!("invalid source.properties.dpi: {}", e)))
    }

    /// 稳态物理块字节数
    pub fn block_bytes(&self) -> usize {
        let mb = if self.physical_block_mb == 0 {
            DEFAULT_BLOCK_MB
        } else {
            self.physical_block_mb
        };
        mb as usize * 1024 * 1024
    }

    /// 首块字节数（0 = 与稳态块相同）
    pub fn first_block_bytes(&self) -> usize {
        match self.first_block_mb {
            0 => self.block_bytes(),
            n => n as usize * 1024 * 1024,
        }
    }

    /// 直通载荷的字段分隔符字节（validate 保证单字节）
    pub fn separator_byte(&self) -> u8 {
        self.column_separator.as_bytes()[0]
    }

    /// 直通载荷的 NULL 字面量；空 = NULL 输出裸空字段
    pub fn null_literal_bytes(&self) -> &[u8] {
        self.null_literal.as_bytes()
    }

    /// 启动期校验（取值 + 互相约束）
    pub fn validate(&self) -> D2DResult<()> {
        let mut errs: Vec<String> = Vec::new();
        if !(1..=65_535).contains(&self.fetch_rows) {
            errs.push(format!(
                "fetch_rows must be 1..=65535, got {}",
                self.fetch_rows
            ));
        }
        if !(64..=1_048_576).contains(&self.column_bytes) {
            errs.push(format!(
                "column_bytes must be 64..=1048576, got {}",
                self.column_bytes
            ));
        }
        if self.physical_block_mb > 1024 {
            errs.push(format!(
                "physical_block_mb must be <= 1024, got {}",
                self.physical_block_mb
            ));
        }
        let block_mb = (self.block_bytes() / (1024 * 1024)) as u32;
        if self.first_block_mb > 0 && self.first_block_mb > block_mb {
            errs.push(format!(
                "first_block_mb ({}) must not exceed physical_block_mb ({})",
                self.first_block_mb, block_mb
            ));
        }
        // 方言：分隔符必须单字节；NULL 字面量不得含分隔符/换行（否则与数据无法区分）
        if self.column_separator.len() != 1 {
            errs.push(format!(
                "column_separator must be a single byte, got {:?}",
                self.column_separator
            ));
        }
        let sep = self.column_separator.as_bytes().first().copied();
        if !self.null_literal.is_empty()
            && self
                .null_literal
                .as_bytes()
                .iter()
                .any(|b| Some(*b) == sep || *b == b'\n' || *b == b'\r' || *b == 0)
        {
            errs.push(format!(
                "null_literal {:?} must not contain the separator/newline/NUL",
                self.null_literal
            ));
        }
        if !errs.is_empty() {
            return Err(D2DError::config(format!(
                "invalid source.properties.dpi: {}",
                errs.join("; ")
            )));
        }
        Ok(())
    }
}

/// **查询侧限制**（与 dmfldr 的规则同源，措辞与出口不同）
///
/// DPI 跑的就是一条 SELECT，所以：
/// - `where_clause`：放行，AND 进每条分片 SQL
/// - 计算列：拒绝 —— 列清单来自 `type_contract`，它是 schema 与列序的唯一权威
/// - `order_by`：拒绝 —— 迁移是集合语义，分片并行下整体顺序无意义
/// - `limit`：拒绝 —— N 个分片各 LIMIT n 会取回 N×n 行
pub fn validate_dpi_query(spec: &d2d_connector::SourceSpec) -> D2DResult<()> {
    if let Some(query) = &spec.query {
        if let Some(cols) = &query.columns {
            for c in cols {
                if matches!(c, d2d_connector::QueryColumn::Expr { .. }) {
                    return Err(D2DError::config(
                        "dpi 不支持计算列：SELECT 列表来自 source.type_contract（权威 schema），\
                         query.columns 仅用于声明列序；需要计算列请改用 method: odbc",
                    ));
                }
            }
            let contract: Vec<&str> = spec
                .type_contract
                .as_ref()
                .map(|c| c.iter().map(|x| x.column.as_str()).collect())
                .unwrap_or_default();
            let wanted: Vec<&str> = cols
                .iter()
                .map(|c| match c {
                    d2d_connector::QueryColumn::Name(n) => n.as_str(),
                    _ => "",
                })
                .collect();
            if !wanted.is_empty() && wanted != contract {
                return Err(D2DError::config(format!(
                    "dpi 要求 query.columns 与 type_contract 顺序完全一致（契约即 SELECT 列序）：\
                     query={:?}, contract={:?}",
                    wanted, contract
                )));
            }
        }
        if let Some(o) = &query.order_by {
            if !o.trim().is_empty() {
                return Err(D2DError::config(
                    "dpi 不支持 order_by：迁移是集合语义，且分片并行下整体顺序无意义",
                ));
            }
        }
        if query.limit.is_some() {
            return Err(D2DError::config(
                "dpi 不支持 limit：N 个分片各自 LIMIT 会取回 N×limit 行。\
                 需要截断请用 query.where_clause 表达",
            ));
        }
    }

    let effective = spec.effective_partition()?;
    if let Some(p) = &effective {
        p.validate()?;
        // MVP 不做元数据探测：range 必须显式给上下界。
        // （dmfldr 的探测走的是它的 plan 文件与 GROUP BY，DPI 不该借那条路。）
        if p.mode == d2d_connector::PartitionMode::Range && (p.lower.is_none() || p.upper.is_none())
        {
            return Err(D2DError::config(
                "dpi 的 range 分片需要显式给出 lower + upper（MVP 不做 MIN/MAX 探测）—— \
                 或者改用 partition.mode: list / hash / none",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn props(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        match v {
            serde_json::Value::Object(m) => m,
            _ => panic!("object"),
        }
    }

    #[test]
    fn test_defaults_when_absent() {
        let p = DpiParams::from_props(&props(json!({}))).unwrap();
        assert_eq!(p.fetch_rows, 4000);
        assert_eq!(p.block_bytes(), 64 * 1024 * 1024);
        assert_eq!(p.first_block_bytes(), 33 * 1024 * 1024);
    }

    #[test]
    fn test_parse_block() {
        let p = DpiParams::from_props(&props(json!({
            "dpi": {"library": "/opt/dm/libdmdpi.so", "fetch_rows": 2000,
                    "physical_block_mb": 32, "first_block_mb": 16}
        })))
        .unwrap();
        assert_eq!(p.fetch_rows, 2000);
        assert_eq!(p.block_bytes(), 32 * 1024 * 1024);
        assert_eq!(p.first_block_bytes(), 16 * 1024 * 1024);
        assert!(p.validate().is_ok());
    }

    #[test]
    fn test_first_block_larger_than_steady_is_rejected() {
        let p = DpiParams {
            physical_block_mb: 16,
            first_block_mb: 32,
            ..Default::default()
        };
        let err = p.validate().unwrap_err().to_string();
        assert!(err.contains("first_block_mb"), "{}", err);
    }

    #[test]
    fn test_unknown_key_is_rejected() {
        let err = DpiParams::from_props(&props(json!({"dpi": {"fetch_row": 1}})))
            .unwrap_err()
            .to_string();
        assert!(err.contains("fetch_row"), "{}", err);
    }

    /// qmdb 直通方言：`,` 分隔 + 裸空字段 NULL（协议 §2.1 约定，均不可配）
    #[test]
    fn test_qmdb_dialect_params() {
        let p = DpiParams::from_props(&props(json!({
            "dpi": {"column_separator": ",", "null_literal": ""}
        })))
        .unwrap();
        p.validate().unwrap();
        assert_eq!(p.separator_byte(), b',');
        assert!(p.null_literal_bytes().is_empty());
        // 缺省仍是 Doris 方言
        let d = DpiParams::default();
        assert_eq!(d.separator_byte(), b'|');
        assert_eq!(d.null_literal_bytes(), b"\\N");
    }

    /// 方言校验：多字节分隔符 / NULL 字面量含分隔符都必须拒绝
    #[test]
    fn test_dialect_validation() {
        let p = DpiParams::from_props(&props(json!({"dpi": {"column_separator": "||"}}))).unwrap();
        let err = p.validate().unwrap_err().to_string();
        assert!(err.contains("column_separator"), "{}", err);

        let p = DpiParams::from_props(&props(json!({
            "dpi": {"column_separator": ",", "null_literal": "N,A"}
        })))
        .unwrap();
        let err = p.validate().unwrap_err().to_string();
        assert!(err.contains("null_literal"), "{}", err);
    }

    #[test]
    fn test_range_without_bounds_is_rejected() {
        let spec: d2d_connector::SourceSpec = serde_json::from_value(json!({
            "connector": "dameng",
            "connection": {"host": "h", "user": "u"},
            "method": "dpi",
            "table": "T",
            "type_contract": [{"column": "id", "source_type": "BIGINT", "sink_type": "BIGINT"}],
            "partition": {"mode": "range", "column": "id", "count": 4}
        }))
        .unwrap();
        let err = validate_dpi_query(&spec).unwrap_err().to_string();
        assert!(err.contains("lower + upper"), "{}", err);
    }
}
