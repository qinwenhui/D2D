//! Doris **writer 方向**：`DorisSinkConnector` 及它用到的两个写入会话
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`session`] | **逻辑面**写入会话：Arrow 批次逐格序列化成 CSV 再 Stream Load |
//! | [`physical`] | **直通面**写入会话：源端导出的原始字节直接进 HTTP body |
//!
//! 两个会话是**兄弟**：同一个 HTTP 协议、同一组请求头、同一套响应校验，
//! 区别只在 body 从哪来。它们共用 [`crate::config`] 里的连接配置。
//!
//! 读方向（Doris → X）不在这里，见 crate 根文档「骨架」一节。

use async_trait::async_trait;
use d2d_connector::{
    MemMeter, PhysicalCompat, PhysicalFormat, PhysicalSession, PhysicalSink, SinkConnector,
    SinkSession, SinkSpec, WriteConfig,
};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::{DorisConnection, StreamLoadConfig};
use crate::sink::physical::{judge, DorisPhysicalSession};
use crate::sink::session::DorisSinkSession;

pub mod physical;
pub mod session;

/// Doris Sink 连接器
pub struct DorisSinkConnector {
    name: String,
    connection: Option<DorisConnection>,
    stream_load_config: StreamLoadConfig,
    /// **运行期占用申报尺子**：序列化后的请求体（每个在飞的写入一份）
    ///
    /// 直通面的请求体是内核攒出来的那块字节，本来就在账上；逻辑面那份是
    /// `serialize_batches_csv` 新建的 `Vec<u8>`（明文 CSV 拷贝），**不在任何池的账上**。
    /// 两者都在交给 `reqwest` 之后出了池的视野，所以由连接器自己认领。
    meter: Option<Arc<MemMeter>>,
    /// **连接器级的 HTTP 客户端**（会话 clone 它）
    ///
    /// 提到这一层是**这次改动的直接后果**：会话现在是"一份数据一条"，
    /// 开得非常频繁 —— 每个会话自建一个 client 就等于每次都丢掉连接池，
    /// 每个 body 都要重新握手。放这里，连接全程 Keep-Alive，会话只 clone 一个句柄。
    client: reqwest::Client,
}

impl DorisSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "doris".to_string(),
            connection: None,
            stream_load_config: StreamLoadConfig::default(),
            meter: None,
            client: build_http_client(),
        }
    }

    /// 从 SinkSpec 解析 Doris 连接配置
    ///
    /// 解析搬到 [`DorisConnection::from_json`]：读方向也要解析同一段配置，
    /// 两处各写一遍的话，"少填一个字段时报什么错"这种事会分岔。
    fn parse_connection(spec: &SinkSpec) -> D2DResult<DorisConnection> {
        DorisConnection::from_json(&spec.connection, &spec.database, &spec.table)
    }

    /// 解析 Stream Load 配置 —— **只读 Doris 私有的那几项**
    ///
    /// ⚠️ 这里**不读** `batch_max_rows` / `batch_max_bytes` / `flush_interval_ms`。
    /// 前两项说的是"一次发多少"，是**每个目标端都要回答的通用问题**，与是不是 Doris
    /// 无关 —— 它们现在由 `flow.sink.batch_bytes` 表达，并且**由内核落实**
    /// （内核从池里攒够一份才交给连接器，连接器收到即写）。
    /// 第三项已经**不存在**：它解决的是"连接器内部攒着不发、最后一批吊着"，
    /// 而攒批搬走之后那个状态没了，旋钮也就没了。
    ///
    /// 留在 `stream_load` 块里的是 Doris 真正私有的东西：格式、分隔符、严格模式、
    /// 过滤比 —— 换个目标端（MySQL）这些名字就不存在了。
    fn parse_stream_load(spec: &SinkSpec) -> D2DResult<StreamLoadConfig> {
        let mut config = StreamLoadConfig::default();

        if let Some(sl) = spec.prop("stream_load") {
            if let Some(fmt) = sl.get("format").and_then(|v| v.as_str()) {
                config.format = fmt.to_string();
            }
            if let Some(sep) = sl.get("column_separator").and_then(|v| v.as_str()) {
                config.column_separator = sep.to_string();
            }
            if let Some(del) = sl.get("line_delimiter").and_then(|v| v.as_str()) {
                config.line_delimiter = del.to_string();
            }
            if let Some(ratio) = sl.get("max_filter_ratio").and_then(|v| v.as_f64()) {
                config.max_filter_ratio = ratio;
            }
            if let Some(strict) = sl.get("strict_mode").and_then(|v| v.as_bool()) {
                config.strict_mode = strict;
            }
            // 单请求体上限：目标端的物理约束（不是攒批阈值，别和 batch_bytes 混）。
            // json 不给就用 Doris BE 的出厂值补上 —— 见 `with_format_defaults`。
            if let Some(n) = sl.get("max_request_bytes").and_then(|v| v.as_u64()) {
                config.max_request_bytes = Some(n as usize);
            }
            // 攒批相关的字段：存量配置里多半还留着，必须**明确报错**。
            // 静默忽略会让用户以为"我明明配了 200000 行，怎么不生效" —— 那比报错难查得多。
            for (old, new) in [
                ("batch_max_rows", "batch_rows"),
                ("batch_max_bytes", "batch_bytes"),
            ] {
                if sl.get(old).is_some() {
                    return Err(D2DError::config(format!(
                        "sink.properties.stream_load.{} 已上移：攒批阈值是**通用概念**\
                         （任何目标端都要回答'一次发多少'），与是不是 Doris 无关。\
                         请删掉这一行，改配 flow.sink.{}",
                        old, new
                    )));
                }
            }
            if sl.get("flush_interval_ms").is_some() {
                return Err(D2DError::config(
                    "sink.properties.stream_load.flush_interval_ms 已**删除**：\
                     它解决的是「连接器内部攒着不发、最后一批一直吊着」—— \
                     而攒批已经搬到内核（`flow.sink.batch_bytes`：内核攒够一份才交给连接器，\
                     连接器收到即写）。连接器内部不再有缓冲，也就没有需要超时去催的东西。\
                     请直接删掉这一行。",
                ));
            }
        }

        // 按格式补缺省（json 的单请求上限只有 100 MB —— 见 with_format_defaults）
        Ok(config.with_format_defaults())
    }
}

#[async_trait]
impl SinkConnector for DorisSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        info!(connector = %self.name, database = %spec.database, table = %spec.table, "initializing doris sink");

        let mut conn = Self::parse_connection(spec)?;
        // 解析 Stream Load 直连目标（显式 be_hosts → FE 自动发现 → 回退 FE 转发）
        conn.resolve_load_targets().await?;
        self.connection = Some(conn);
        self.stream_load_config = Self::parse_stream_load(spec)?;
        // 名字与说明由**本连接器**给 —— 引擎不解释这些字节是什么
        self.meter = Some(MemMeter::new(
            "doris 请求体在飞",
            // 别写死 "CSV" —— json 档也是走这一笔。实测同批数据 json 载荷约是
            // csv 的 2.6 倍（每行都要重复键名），所以这一笔在 json 档明显更大。
            format!(
                "一次写入的明文载荷（{}），已移交 reqwest（每个在飞的写入一份）",
                self.stream_load_config.format
            ),
        ));

        Ok(())
    }

    async fn begin_session(&self, spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| d2d_core::D2DError::config("doris connection not initialized"))?
            .clone();

        let session = DorisSinkSession::new(
            connection,
            self.stream_load_config.clone(),
            spec.write.clone().unwrap_or_default(),
            self.meter.clone(),
            self.client.clone(),
        )?;

        Ok(Box::new(session))
    }

    fn as_physical(&self) -> Option<&dyn PhysicalSink> {
        Some(self)
    }

    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing doris sink");
        self.connection = None;
        Ok(())
    }
}

/// 造连接器级的 HTTP 客户端
///
/// 建不出来就退回缺省客户端 —— 连这个都要失败的话，后面的写入本来也成不了，
/// 而在 `new()` 里 panic 会把"连不上库"报成一个完全无关的崩溃。
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        // 大 body 上传，多留几条空闲连接；keep-alive 正是把它提到连接器级的理由
        .pool_max_idle_per_host(8)
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// 物理载荷写入能力
///
/// 裁决逻辑全在纯函数 [`judge`] 里（可单测），这里只负责接线与开会话。
#[async_trait]
impl PhysicalSink for DorisSinkConnector {
    fn accepts_physical(&self, fmt: &PhysicalFormat) -> PhysicalCompat {
        judge(&self.stream_load_config, fmt)
    }

    async fn begin_physical(&self, fmt: &PhysicalFormat) -> D2DResult<Box<dyn PhysicalSession>> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| D2DError::config("doris connection not initialized"))?
            .clone();

        // `columns` 头必须按**源端列序**声明：Doris 建表会把 UNIQUE KEY 列提到最前，
        // 位置映射必然错位。详见 session.rs 中 do_stream_load 的注释第 1 条。
        let columns = fmt.columns().join(",");
        if columns.is_empty() {
            return Err(D2DError::config(
                "物理直通需要源端声明列名（用于生成 Doris columns 映射头）",
            ));
        }
        warn!(
            columns = %columns,
            format = %fmt.describe(),
            "doris: 开启物理直通写入会话（跳过引擎解析，字节原样入 Stream Load）"
        );

        Ok(Box::new(DorisPhysicalSession::new(
            connection,
            self.stream_load_config.clone(),
            WriteConfig::default(),
            columns,
            self.meter.clone(),
            self.client.clone(),
        )))
    }
}

impl Default for DorisSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}
