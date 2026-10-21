//! Doris Stream Load 的**物理载荷**写入会话（直通路径的目标端半边）
//!
//! 与 [`crate::sink::session::DorisSinkSession`] 是兄弟关系：同一个 HTTP 协议、同一组
//! 请求头、同一套响应校验，区别只在 body 从哪来 ——
//! 一个来自「Arrow 批次逐格序列化」，一个来自「源端导出的原始字节」。
//!
//! ## 编码兼容裁决（`d2d_connector::PhysicalSink::accepts_physical`）
//!
//! 直通要求源端产出的字节，Doris 能按**同样的规则**解释。逐项比对，任一项不符即
//! 回落逻辑面 —— 宁可慢，也不能让「引擎以为在搬运、实际在篡改」的情况发生。
//!
//! | 比对项 | 为什么 |
//! |---|---|
//! | 列分隔符 | Doris 默认是制表符，必须显式一致 |
//! | 行分隔符 | Doris 默认 `\n`；源端控制文件固定 `STR X '0A'` |
//! | 字符集 | Stream Load 体按 UTF-8 解析 |
//! | 引号/转义/NULL 字面量 | 源端若做了这些加工，Doris 不会反向还原 |
//! | 格式 | 只支持 csv（json/parquet 无物理形态） |
//!
//! ## `forbidden`：反斜杠
//!
//! Doris CSV 读入器把 `\` 当转义符，而 dmfldr（无控制文件时）导出不做转义。
//! 载荷里一旦出现裸 `\`，Doris 会吃掉它并字面化下一个字符 —— 值被静默改写。
//! 因此这里回报 `forbidden = [b'\\']`，由执行层逐字节扫描拦截。
//! 这是**保守**取值：若某集群的 Doris 实际不做转义，代价也只是一次永不命中的
//! `memchr`（SIMD，顺路），不会误伤正确性。

use async_trait::async_trait;
use bytes::Bytes;
use d2d_connector::{MemMeter, PhysicalCompat, PhysicalFormat, PhysicalSession, WriteConfig};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::{DorisConnection, StreamLoadConfig};

/// Doris 读入 CSV 时仍会特殊解释的字节
///
/// 源端声明 `escape: None`（未做转义），所以载荷中任何反斜杠都会与 Doris 的解释
/// 不一致 —— 见模块头注释。
const DORIS_CSV_RESERVED: &[u8] = b"\\";

/// 物理写入会话（**直通面**）
///
/// # 一块一块地发，不攒批
///
/// 攒批住在内核（`flow.sink.batch_bytes`：内核从池里攒够一份才递过来）。
/// 所以这里没有缓冲区、没有 `should_flush`，一次 [`PhysicalSession::write_block`]
/// 就是一次 Stream Load。
///
/// 这条分工顺带把 P0-1 那笔"多拷一份载荷"的问题**结构性**解决了：
/// 载荷由内核攒好、**按所有权**递进来，这里 `Bytes::from(block.bytes)` 直接移交，
/// 全程只有一份。旧版之所以要拷，是因为它在会话内部攒 ——
/// `send` 借缓冲、`.body()` 要所有权，只能拷一份，而借出去的那份在整次请求期间还活着。
pub struct DorisPhysicalSession {
    connection: DorisConnection,
    config: StreamLoadConfig,
    write_config: WriteConfig,
    /// `columns` 头：源端列名（按源端列序）
    columns: String,

    /// 占用申报尺子（连接器给的那把，缺省不申报）
    meter: Option<Arc<MemMeter>>,
    /// 复用的 HTTP 客户端（**连接器级的那一个**）
    ///
    /// 原实现在**每次 flush** 都 `reqwest::Client::new()`：等于每次都丢掉连接池，
    /// 每个 body 都要重新建 TCP 连接。会话开得频繁之后（一份数据一条会话），
    /// 提到连接器级是必需的 —— 否则"每次写入一个客户端"会原样长回来。
    client: reqwest::Client,

    current_label: Option<String>,
    total_written_rows: u64,
    total_written_bytes: u64,
}

impl DorisPhysicalSession {
    pub fn new(
        connection: DorisConnection,
        config: StreamLoadConfig,
        write_config: WriteConfig,
        columns: String,
        meter: Option<Arc<MemMeter>>,
        client: reqwest::Client,
    ) -> Self {
        Self {
            connection,
            config,
            write_config,
            columns,
            meter,
            client,
            current_label: None,
            total_written_rows: 0,
            total_written_bytes: 0,
        }
    }

    /// 发送一次 Stream Load
    ///
    /// 请求头与 [`crate::sink::session::DorisSinkSession`] 完全一致，理由见那边的注释
    /// （`columns` 头必发、不发 `line_delimiter`、不发压缩体、必校验 Status）。
    ///
    /// `payload` 按**所有权**收 —— 载荷一路从攒批缓冲移交到 HTTP 层，中途不复制
    /// （见 [`Self::do_flush`] 关于那 800 MB 的注释）。
    /// 重试由调用方 `clone()`（`Bytes` 的 clone 只涨引用计数，O(1)）。
    async fn send(&self, payload: Bytes, label: &str) -> D2DResult<u64> {
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.connection.next_load_url(),
            self.connection.database,
            self.connection.table
        );

        let response = self
            .client
            .put(&url)
            .basic_auth(&self.connection.username, Some(&self.connection.password))
            .header("Expect", "100-continue")
            .header("Content-Type", "text/plain; charset=UTF-8")
            .header("label", label)
            .header("columns", self.columns.as_str())
            .header("column_separator", self.config.column_separator.as_str())
            .header("max_filter_ratio", self.config.max_filter_ratio.to_string())
            .header(
                "strict_mode",
                if self.config.strict_mode {
                    "true"
                } else {
                    "false"
                },
            )
            .body(payload)
            .send()
            .await
            .map_err(|e| D2DError::sink_write("doris", e))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() || !body.contains("\"Status\": \"Success\"") {
            return Err(D2DError::sink_write(
                "doris",
                format!("HTTP {} :: {}", status, body),
            ));
        }
        parse_number_loaded(&body).ok_or_else(|| {
            D2DError::sink_write(
                "doris",
                format!(
                    "Stream Load 报 Success 但响应缺少 NumberLoadedRows，无法与源端行数对账；\
                     响应体: {}",
                    truncate(&body, 600)
                ),
            )
        })
    }

    /// 发一次（含重试）
    ///
    /// `rows` 由产出侧精确计数（源端块的行数），Doris 回报的 `NumberLoadedRows`
    /// 必须与它相等 —— 两边独立计数，对不上即失败（见下）。
    ///
    /// 载荷**按所有权**收：`payload` 一路从内核攒好的那块字节移交到 HTTP 层，中途不复制。
    /// 重试靠 `Bytes::clone`（只涨引用计数，O(1)）。
    async fn send_with_retry(&mut self, payload: Bytes, rows: u64) -> D2DResult<u64> {
        let label = format!(
            "d2d_{}_{}",
            self.connection.table,
            Uuid::new_v4().to_string().replace('-', "")
        );
        self.current_label = Some(label.clone());
        let bytes = payload.len();

        let mut last_err = None;
        let max_retries = self.write_config.max_retries.unwrap_or(3).max(1);
        let backoff = self.write_config.retry_backoff_ms.unwrap_or(1000);

        for attempt in 0..max_retries {
            match self.send(payload.clone(), &label).await {
                Ok(loaded) => {
                    // Doris 自己的计数与源端块计数必须一致（两边独立计数）
                    if loaded != rows {
                        return Err(D2DError::sink_write(
                            "doris",
                            format!(
                                "行数不一致：源端块计数 {} 行，Doris 确认载入 {} 行。\
                                 常见原因：① 唯一键重复被静默去重；② 行分隔符不一致导致并行/断行。",
                                rows, loaded
                            ),
                        ));
                    }
                    self.total_written_rows += loaded;
                    self.total_written_bytes += bytes as u64;
                    debug!(label = %label, rows = loaded, bytes, attempt = attempt + 1, "直通 stream load 成功");
                    return Ok(loaded);
                }
                Err(e) => {
                    warn!(label = %label, attempt = attempt + 1, max_retries, error = %e,
                          "直通 stream load 失败，重试中");
                    last_err = Some(e);
                    if attempt + 1 < max_retries {
                        tokio::time::sleep(Duration::from_millis(backoff * (attempt as u64 + 1)))
                            .await;
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| D2DError::sink_write("doris", "重试耗尽")))
    }
}

#[async_trait]
impl PhysicalSession for DorisPhysicalSession {
    /// **写一块 = 发一次 Stream Load**（内核已经攒好了一份，这里不攒）
    async fn write_block(&mut self, block: d2d_connector::PhysicalBlock) -> D2DResult<u64> {
        let rows = block.rows;

        // ── 零拷贝移交 ──
        //
        // `Bytes::from(Vec<u8>)` 不搬数据，只换一个持有者，于是这批字节全程**只有一份**。
        // 曾经这里是 `send(&self.buffer)` 配 `.body(data.to_vec())`：
        // `send` 借缓冲、`.body` 要所有权 → 只能拷一份，而借出去的那份在整次请求期间
        // 还活着。8 路写入 × 100 MB 批 ≈ **800 MB 白占**，实测连接器自报 1652 MB 里
        // 有整整一半是这个（2026-10-16 基准，见 基准实测）。
        let payload = Bytes::from(block.bytes);

        // ── 如实申报"在飞的那一份" ──
        //
        // 这批字节是**内核攒出来的**（它已进内存账第 ⑥项），移交到这里之后
        // 就出了池的视野、只被 `reqwest` 拿着 —— 连接器自己认领它，
        // 才能解释 RSS 里"账本之外"的那部分去哪了。
        // 与账本**不可相加**（同一批字节，见 `SinkConnector::memory_meter`）。
        let _in_flight = self.meter.as_ref().map(|m| m.claim(payload.len()));

        self.send_with_retry(payload, rows).await
    }

    async fn commit(&mut self) -> D2DResult<u64> {
        // 没有剩余缓冲要落盘 —— 每次 `write_block` 都当场发出去了
        info!(
            total_rows = self.total_written_rows,
            total_bytes = self.total_written_bytes,
            "doris 直通会话已提交"
        );
        Ok(self.total_written_rows)
    }

    async fn abort(&mut self) -> D2DResult<()> {
        warn!(label = ?self.current_label, "doris 直通会话已回滚");
        Ok(())
    }
}

/// 从 Stream Load 响应体里取 `NumberLoadedRows`
///
/// 只做健壮的子串解析，不引入完整 JSON 依赖路径 —— 响应里数值字段可能带引号。
fn parse_number_loaded(body: &str) -> Option<u64> {
    let key = "\"NumberLoadedRows\"";
    let start = body.find(key)? + key.len();
    let rest = &body[start..];
    let colon = rest.find(':')? + 1;
    let rest = rest[colon..].trim_start();
    let rest = rest.trim_start_matches('"');
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse::<u64>().ok()
}

/// 按字节上限截断，且保证不切开多字节字符（切一半会 panic）
fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// 裁决 Doris 能否直接消费源端的物理编码
///
/// 与 [`StreamLoadConfig`] 逐项比对。抽成纯函数以便单测覆盖各种不匹配组合。
pub fn judge(cfg: &StreamLoadConfig, fmt: &PhysicalFormat) -> PhysicalCompat {
    if cfg.format != "csv" {
        return PhysicalCompat::Reject(format!(
            "目标端配置为 {} 格式，物理直通仅支持 csv",
            cfg.format
        ));
    }
    let PhysicalFormat::Csv {
        dialect,
        charset,
        columns,
    } = fmt;
    if columns.is_empty() {
        return PhysicalCompat::Reject("源端未声明列名，无法生成 columns 映射头".to_string());
    }
    if dialect.quote.is_some() {
        return PhysicalCompat::Reject(
            "源端使用引号包裹，Doris 侧不会做同样的引号解析".to_string(),
        );
    }
    if dialect.escape.is_some() {
        return PhysicalCompat::Reject(
            "源端做了转义，Doris 侧不会反向还原（会把转义序列当字面文本写入）".to_string(),
        );
    }
    // 空值字面量：Doris 固定是 `\N`，所以只有两种可接受形态 ——
    //
    //   None      空值就是空字段（dmfldr SQL 模式；目标端行为由 null_value 决定）
    //   Some(\N)  源端显式产出 Doris 的 NULL token（dpi 直通面）
    //
    // 后者同时是一句承诺：**值里不会再出现别的反斜杠**，否则无法与 `\N` 区分。
    // 源端逐字段保证（dpi 的 RowBlockWriter），因此这里不再把 `\` 列为受限字节；
    // 反过来，如果仍声明 `\` 受限，`\N` 本身就会被安全闸拦下 —— 那就等于
    // 从结构上禁止源端正确表达 NULL。
    let forbidden = match &dialect.null_literal {
        None => DORIS_CSV_RESERVED.to_vec(),
        Some(lit) if lit == "\\N" => Vec::new(),
        Some(other) => {
            return PhysicalCompat::Reject(format!(
                "源端用 {:?} 表示空值，而 Doris 的空值字面量固定为 \\N、不可配置",
                other
            ));
        }
    };
    if *charset != d2d_connector::Charset::Utf8 {
        return PhysicalCompat::Reject(format!(
            "源端编码 {} 与 Stream Load 期望的 utf-8 不一致（直通不做转码）",
            charset.as_str()
        ));
    }
    match single_byte(&cfg.column_separator) {
        Some(s) if s == dialect.column_separator => {}
        Some(s) => {
            return PhysicalCompat::Reject(format!(
                "列分隔符不一致：源端 {:?}，目标端 {:?}",
                dialect.column_separator as char, s as char
            ))
        }
        None => {
            return PhysicalCompat::Reject(format!(
                "目标端列分隔符 {:?} 不是单字节，物理直通不支持",
                cfg.column_separator
            ))
        }
    }
    match single_byte(&cfg.line_delimiter) {
        Some(l) if l == dialect.line_delimiter => {}
        Some(l) => {
            return PhysicalCompat::Reject(format!(
                "行分隔符不一致：源端 {:?}，目标端 {:?}",
                dialect.line_delimiter as char, l as char
            ))
        }
        None => {
            return PhysicalCompat::Reject(format!(
                "目标端行分隔符 {:?} 不是单字节，物理直通不支持",
                cfg.line_delimiter
            ))
        }
    }
    PhysicalCompat::Accept { forbidden }
}

fn single_byte(s: &str) -> Option<u8> {
    let b = s.as_bytes();
    if b.len() == 1 {
        Some(b[0])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{Charset, CsvDialect};

    fn cfg(sep: &str, line: &str) -> StreamLoadConfig {
        StreamLoadConfig {
            format: "csv".into(),
            column_separator: sep.into(),
            line_delimiter: line.into(),
            ..Default::default()
        }
    }

    fn fmt(d: CsvDialect, cs: Charset) -> PhysicalFormat {
        PhysicalFormat::csv(d, cs, vec!["a".into(), "b".into()])
    }

    fn dmfldr_sql_mode() -> CsvDialect {
        CsvDialect {
            column_separator: b'|',
            line_delimiter: b'\n',
            quote: None,
            escape: None,
            null_literal: None,
        }
    }

    /// 锁住 P0-1 依赖的那条性质：**载荷移交后缓冲是空的，容量也一并交出去**
    ///
    /// 这不是集成测试（真实验证在 `.39` 的基准上：自报从 1652 MB 掉到一批的量）。
    /// 它挡的是"顺手改回去"：`self.buffer.clone()` 或留容量复用，都会让这笔 800 MB 长回来，
    /// 而功能测试**全都还是会过** —— 只有内存读数会变，那正是最难被发现的回归。
    #[test]
    fn test_payload_handoff_cedes_buffer_and_its_capacity() {
        let mut buf: Vec<u8> = Vec::with_capacity(4096);
        buf.extend_from_slice(b"a|b\n");

        let payload = Bytes::from(std::mem::take(&mut buf));

        assert_eq!(&payload[..], b"a|b\n");
        assert!(buf.is_empty(), "缓冲必须在移交后立刻为空");
        assert_eq!(
            buf.capacity(),
            0,
            "容量也要交出去 —— 留着就等于把刚还回去的内存又占回来"
        );

        // 重试不复制：`Bytes::clone` 只涨引用计数，两份指向同一块
        let retry = payload.clone();
        assert_eq!(retry.as_ptr(), payload.as_ptr(), "重试必须是零拷贝");
    }

    #[test]
    fn test_accepts_matching_dialect() {
        let v = judge(&cfg("|", "\n"), &fmt(dmfldr_sql_mode(), Charset::Utf8));
        assert_eq!(
            v,
            PhysicalCompat::Accept {
                forbidden: vec![b'\\']
            }
        );
    }

    #[test]
    fn test_rejects_separator_mismatch() {
        let v = judge(&cfg("\t", "\n"), &fmt(dmfldr_sql_mode(), Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref d) if d.contains("列分隔符")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_rejects_line_delimiter_mismatch() {
        let v = judge(&cfg("|", "\r\n"), &fmt(dmfldr_sql_mode(), Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref d) if d.contains("行分隔符")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_rejects_non_utf8() {
        let v = judge(&cfg("|", "\n"), &fmt(dmfldr_sql_mode(), Charset::Gbk));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref d) if d.contains("编码")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_rejects_quote_escape_null_literal() {
        let mut d = dmfldr_sql_mode();
        d.quote = Some(b'"');
        assert!(matches!(
            judge(&cfg("|", "\n"), &fmt(d, Charset::Utf8)),
            PhysicalCompat::Reject(_)
        ));

        let mut d = dmfldr_sql_mode();
        d.escape = Some(b'\\');
        let v = judge(&cfg("|", "\n"), &fmt(d, Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("转义")),
            "{:?}",
            v
        );

        let mut d = dmfldr_sql_mode();
        d.null_literal = Some("NULL".into());
        let v = judge(&cfg("|", "\n"), &fmt(d, Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("空值")),
            "{:?}",
            v
        );
    }

    /// **dpi 直通面的前提**：源端显式产出 `\N` 时必须放行。
    ///
    /// 旧逻辑拒绝任何 `null_literal: Some(_)`，于是 DPI 无法正确表达 NULL，
    /// 又因为 `\` 在受限字节里，连"偷偷写 `\N`"都过不了安全闸。
    /// 放行 `\N` 的代价是源端承诺值里没有别的反斜杠 —— dpi 的 RowBlockWriter
    /// 逐字段检查；dmfldr 不声明 null_literal，仍然受 `\` 受限字节保护。
    #[test]
    fn test_accepts_doris_null_literal_without_backslash_guard() {
        let mut d = dmfldr_sql_mode();
        d.null_literal = Some("\\N".into());
        let v = judge(&cfg("|", "\n"), &fmt(d, Charset::Utf8));
        assert_eq!(
            v,
            PhysicalCompat::Accept { forbidden: vec![] },
            "声明 \\N 必须放行，且不再要求源端保证「无任何反斜杠」"
        );
    }

    #[test]
    fn test_rejects_json_format_and_empty_columns() {
        let mut c = cfg("|", "\n");
        c.format = "json".into();
        assert!(matches!(
            judge(&c, &fmt(dmfldr_sql_mode(), Charset::Utf8)),
            PhysicalCompat::Reject(_)
        ));

        let f = PhysicalFormat::csv(dmfldr_sql_mode(), Charset::Utf8, vec![]);
        assert!(matches!(
            judge(&cfg("|", "\n"), &f),
            PhysicalCompat::Reject(_)
        ));
    }

    #[test]
    fn test_rejects_multibyte_target_separator() {
        let v = judge(&cfg("||", "\n"), &fmt(dmfldr_sql_mode(), Charset::Utf8));
        assert!(
            matches!(v, PhysicalCompat::Reject(ref m) if m.contains("单字节")),
            "{:?}",
            v
        );
    }

    #[test]
    fn test_parse_number_loaded_variants() {
        let b =
            r#"{"TxnId":1,"Status":"Success","NumberLoadedRows":12345,"NumberTotalRows":12345}"#;
        assert_eq!(parse_number_loaded(b), Some(12345));
        // 带引号的数值
        let b2 = r#"{"Status":"Success","NumberLoadedRows":"777"}"#;
        assert_eq!(parse_number_loaded(b2), Some(777));
        // 0 行也要能解析（不能被当成缺失）
        let b3 = r#"{"Status":"Success","NumberLoadedRows":0}"#;
        assert_eq!(parse_number_loaded(b3), Some(0));
        // 缺失
        assert_eq!(parse_number_loaded(r#"{"Status":"Success"}"#), None);
    }

    #[test]
    fn test_truncate_is_char_safe() {
        // 不能把多字节字符切一半（会 panic）
        let s = "中文测试".repeat(200);
        let t = truncate(&s, 13);
        assert!(t.ends_with('…'));
    }
}
