//! qmdb **物理载荷写入会话**（直通路径的目标端半边，协议 §2.1）
//!
//! 与 [`crate::sink::session::QmdbSinkSession`] 是兄弟关系：同一条 Flight 通道、
//! 同一套三段式装载，区别只在载荷从哪来 —— 一个来自「Arrow 批次逐格编码 IPC」，
//! 一个来自「源端导出的原始字节」（DPI 直通的 CSV 块，客户端 CPU 趋零）。
//!
//! # 一个块 = 一次完整装载会话
//!
//! 协议 §2.1：**一次 `begin_load` 会话对应一个文件**（多文件开多会话）。
//! 所以这里每个 [`PhysicalBlock`] 都独立走 begin_load → do_put → commit_load，
//! 与 doris（一块一次 HTTP Stream Load）同形，只是 qmdb 的"一次请求"是三段式。
//!
//! # CSV 约定（协议 §2.1，均不可配）
//!
//! `,` 分隔、`\n` 行分隔、**空字段 = NULL**、首行为表头（列名大小写无关、
//! 顺序无关）、日期 `YYYY-MM-DD`、时间戳 ISO。编码裁决见 [`judge`]。

use async_trait::async_trait;
use d2d_connector::{MemMeter, PhysicalBlock, PhysicalCompat, PhysicalFormat, PhysicalSession};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::LoadOptions;
use crate::flight::QmdbFlight;

/// qmdb CSV 的字段分隔符（协议固定 `,`，不可配）
const QMDB_CSV_SEPARATOR: u8 = b',';

/// 裁决 qmdb 能否直接消费源端的物理编码
///
/// qmdb CSV 解析器的每条约定都**不可配**（不像 Doris 的 Stream Load 可以带
/// `column_separator` 头），所以逐项硬比对，任一项不符即回落逻辑面 ——
/// 宁可慢，也不能让「引擎以为在搬运、实际在篡改」的情况发生。
pub fn judge(fmt: &PhysicalFormat) -> PhysicalCompat {
    // PhysicalFormat 目前只有 Csv 一个变体；let-else 兜住未来新增的变体
    #[allow(irrefutable_let_patterns)]
    let PhysicalFormat::Csv {
        dialect,
        charset,
        columns,
    } = fmt
    else {
        return PhysicalCompat::Reject(
            "物理载荷只开了 csv（parquet 协议面已就绪，等有 parquet 物理源再启用）".to_string(),
        );
    };
    if columns.is_empty() {
        return PhysicalCompat::Reject("源端未声明列名，无法生成 CSV 表头".to_string());
    }
    if dialect.column_separator != QMDB_CSV_SEPARATOR {
        return PhysicalCompat::Reject(format!(
            "列分隔符不一致：源端 {:?}，qmdb CSV 固定 `,`、不可配",
            dialect.column_separator as char
        ));
    }
    if dialect.line_delimiter != b'\n' {
        return PhysicalCompat::Reject(format!(
            "行分隔符不一致：源端 {:?}，qmdb CSV 固定 `\\n`",
            dialect.line_delimiter as char
        ));
    }
    if dialect.quote.is_some() {
        return PhysicalCompat::Reject(
            "源端使用引号包裹，qmdb CSV 不会做同样的引号解析（值里的 `,` 请在源端清理或回落逻辑面）"
                .to_string(),
        );
    }
    if dialect.escape.is_some() {
        return PhysicalCompat::Reject(
            "源端做了转义，qmdb CSV 不会反向还原（会把转义序列当字面文本写入）".to_string(),
        );
    }
    if let Some(lit) = &dialect.null_literal {
        return PhysicalCompat::Reject(format!(
            "源端用 {:?} 表示空值，而 qmdb CSV 的 NULL 就是裸空字段（其它字面量会被当字面文本写入）",
            lit
        ));
    }
    if *charset != d2d_connector::Charset::Utf8 {
        return PhysicalCompat::Reject(format!(
            "源端编码 {} 与 qmdb 期望的 utf-8 不一致（直通不做转码）",
            charset.as_str()
        ));
    }
    // 受限字节：**无**。e2e 实测（2026-10-27，.13）：qmdb CSV 解析器不解释反斜杠
    // 转义（`back\slash` 原样入库），值里出现 `\` 无害 —— 不设受限字节，
    // 免得含反斜杠的合法数据白白失去直通资格。
    PhysicalCompat::Accept { forbidden: vec![] }
}

/// 物理写入会话（**直通面**）：每个块独立走一次三段式装载
pub struct QmdbPhysicalSession {
    flight: QmdbFlight,
    table: String,
    /// CSV 表头（`col1,col2,...\n`）：拼在每个块字节前。
    /// 列名原样使用源端声明 —— qmdb 按名列对齐且大小写无关。
    header: Vec<u8>,
    load: LoadOptions,
    /// 占用申报尺子（连接器给的那把，缺省不申报）
    meter: Option<Arc<MemMeter>>,

    loads: u64,
    total_rows: u64,
    total_bytes: u64,
}

/// 表头行：源端列序 `,` 分隔、`\n` 结尾（列名原样——qmdb 按名列对齐且大小写无关）
fn header_row(columns: &[String]) -> Vec<u8> {
    let mut header = columns.join(",");
    header.push('\n');
    header.into_bytes()
}

impl QmdbPhysicalSession {
    pub fn new(
        flight: QmdbFlight,
        table: String,
        columns: &[String],
        load: LoadOptions,
        meter: Option<Arc<MemMeter>>,
    ) -> Self {
        Self {
            flight,
            table,
            header: header_row(columns),
            load,
            meter,
            loads: 0,
            total_rows: 0,
            total_bytes: 0,
        }
    }
}

#[async_trait]
impl PhysicalSession for QmdbPhysicalSession {
    /// **写一块 = 一次完整三段式装载**（块即文件；表头拼在块前）
    ///
    /// 行数对账：`block.rows` 由源端精确计数，服务端 commit 回报的 `rows` 必须
    /// 与它相等 —— 两边独立计数，对不上即失败（防止 CSV 行结构被静默改写）。
    async fn write_block(&mut self, block: PhysicalBlock) -> D2DResult<u64> {
        if block.is_empty() {
            return Ok(0);
        }
        let rows = block.rows;
        let mut payload = Vec::with_capacity(self.header.len() + block.bytes.len());
        payload.extend_from_slice(&self.header);
        payload.extend_from_slice(&block.bytes);
        let bytes = payload.len();

        // 如实申报"在飞的那一份"：载荷由内核攒好、按所有权递进来，
        // 这里拼上表头（一次拷贝）后移交 gRPC 层（见 DorisPhysicalSession 同款注释）
        let _in_flight = self.meter.as_ref().map(|m| m.claim(bytes));

        let stats = self
            .flight
            .load_bytes(
                &self.table,
                "csv",
                payload,
                &self.load.mode,
                self.load.watermark.as_ref(),
            )
            .await?;
        if stats.rows != rows {
            return Err(D2DError::external(
                "qmdb",
                format!(
                    "物理装载行数不一致：源端块计数 {rows} 行，qmdb commit 确认 {} 行 —— \
                     常见原因：值内出现分隔符/换行导致 CSV 行结构被改写",
                    stats.rows
                ),
            ));
        }
        self.total_rows += stats.rows;
        self.total_bytes += bytes as u64;
        self.loads += 1;
        debug!(
            table = %self.table,
            rows,
            files = stats.files,
            bytes,
            loads = self.loads,
            "qmdb 物理装载提交完成"
        );
        Ok(stats.rows)
    }

    /// 每个块都已各自原子提交（失败在 `load_bytes` 内部已 abort），会话级 commit 只记账
    async fn commit(&mut self) -> D2DResult<u64> {
        info!(
            table = %self.table,
            loads = self.loads,
            total_rows = self.total_rows,
            total_bytes = self.total_bytes,
            "qmdb 直通会话已提交"
        );
        Ok(self.total_rows)
    }

    async fn abort(&mut self) -> D2DResult<()> {
        debug!(
            table = %self.table,
            loads = self.loads,
            "qmdb 直通会话中止（各块要么已提交、要么已在装载内 abort）"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{Charset, CsvDialect};

    fn fmt(d: CsvDialect, cs: Charset) -> PhysicalFormat {
        PhysicalFormat::csv(d, cs, vec!["phid".into(), "pdate".into()])
    }

    /// qmdb 期望的源端方言：`,` 分隔、空字段 NULL、无引号无转义、utf-8
    fn qmdb_dialect() -> CsvDialect {
        CsvDialect {
            column_separator: b',',
            line_delimiter: b'\n',
            quote: None,
            escape: None,
            null_literal: None,
        }
    }

    #[test]
    fn test_accepts_qmdb_dialect() {
        let v = judge(&fmt(qmdb_dialect(), Charset::Utf8));
        assert_eq!(v, PhysicalCompat::Accept { forbidden: vec![] });
    }

    /// DPI 缺省方言（`|` 分隔 + `\N` NULL，为 Doris 定制）必须被拒 ——
    /// 否则 NULL 会被 qmdb 存成字面文本 `\N`、`,` 值列会被静默切错
    #[test]
    fn test_rejects_doris_default_dialect() {
        let d = CsvDialect {
            column_separator: b'|',
            line_delimiter: b'\n',
            quote: None,
            escape: None,
            null_literal: Some("\\N".to_string()),
        };
        let v = judge(&fmt(d, Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("分隔符")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_rejects_null_literal_and_quote_and_escape() {
        let mut d = qmdb_dialect();
        d.null_literal = Some("\\N".into());
        let v = judge(&fmt(d, Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("空值")),
            "{:?}",
            v
        );

        let mut d = qmdb_dialect();
        d.quote = Some(b'"');
        assert!(matches!(
            judge(&fmt(d, Charset::Utf8)),
            PhysicalCompat::Reject(_)
        ));

        let mut d = qmdb_dialect();
        d.escape = Some(b'\\');
        let v = judge(&fmt(d, Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("转义")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_rejects_gbk_and_empty_columns() {
        let v = judge(&fmt(qmdb_dialect(), Charset::Gbk));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("编码")),
            "{:?}",
            v
        );
        let f = PhysicalFormat::csv(qmdb_dialect(), Charset::Utf8, vec![]);
        assert!(matches!(judge(&f), PhysicalCompat::Reject(_)));
    }

    /// 表头行按源端列序拼 `,` 分隔、`\n` 结尾（列名大小写交服务端按名对齐）
    #[test]
    fn test_header_row_shape() {
        assert_eq!(
            header_row(&["PHID".to_string(), "PDATE".to_string()]),
            b"PHID,PDATE\n"
        );
    }
}
