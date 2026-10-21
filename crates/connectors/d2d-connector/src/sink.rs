use crate::physical::PhysicalSink;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use d2d_core::D2DResult;
use d2d_memory::MemMeter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Sink 连接器配置
///
/// 与 [`crate::SourceSpec`] 同一条分界：**顶层 = 引擎认识的通用概念**
/// （写到哪里：`connection` / `database` / `table`），
/// **[`Self::properties`] = 这个目标特有的**（引擎不解释，原样交给连接器）。
///
/// ```yaml
/// sink:
///   connector: "doris"
///   connection: {...}
///   database: "db_b"
///   table: "t_b"
///   properties:                # ← Doris 特有的
///     stream_load:
///       format: "csv"
///       column_separator: "|"
///       strict_mode: true
/// ```
///
/// ⚠️ **攒批阈值不在这里**。`batch_max_rows` / `batch_max_bytes` 曾经长在
/// `stream_load` 块里（那是 Doris 私有的名字），但"一次写多少"是**每一个目标端
/// 都要回答的通用问题**，与是不是 Doris 无关 —— 它现在由 `flow.sink.batch_bytes`
/// 表达，而且**由内核落实**：内核从 `sink_pool` 攒够一份再交给连接器，
/// 连接器**内部不再攒批**（见 [`SinkSession`]）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkSpec {
    /// 连接器名称（如 "doris"）
    pub connector: String,
    /// 连接配置
    pub connection: serde_json::Value,
    /// 写入方式（连接器内部路由，缺省由连接器决定）。
    /// doris: "stream_load"（缺省）；dameng（目标端）: "dmfldr" | "odbc"
    #[serde(default)]
    pub method: Option<String>,
    /// **本目标特有的参数**，引擎不解释，原样交给连接器
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Map<String, serde_json::Value>>,
    /// 目标库名
    pub database: String,
    /// 目标表名
    pub table: String,
    /// 写入参数（重试策略 —— 这是连接器**行为**，不是攒批策略）
    pub write: Option<WriteConfig>,
}

impl SinkSpec {
    /// 连接器专属参数块（缺省为空）
    pub fn props(&self) -> serde_json::Map<String, serde_json::Value> {
        self.properties.clone().unwrap_or_default()
    }

    /// 取一个连接器专属参数
    pub fn prop(&self, key: &str) -> Option<&serde_json::Value> {
        self.properties.as_ref().and_then(|p| p.get(key))
    }
}

/// 写入参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteConfig {
    /// 最大重试次数
    pub max_retries: Option<u32>,
    /// 重试退避（毫秒）
    pub retry_backoff_ms: Option<u64>,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            max_retries: Some(3),
            retry_backoff_ms: Some(1000),
        }
    }
}

/// Sink 连接器 Trait
///
/// 负责初始化写入会话。
///
/// # 契约：一条 `SinkSession` 同时**至多一个在飞写入**
///
/// 内核按 `flow.sink.concurrency` **给每一份数据发一张券**，拿到券才 spawn 一个任务，
/// 任务里开一条 [`SinkSession`]、写一份、结束。于是与源端同一条分工：
///
/// - **任务数由数据决定** —— `sink_pool` 里攒够几份就写几次，数据少就只写少数几次
/// - **同时在写几个由内核决定** —— 上限在引擎手里，连接器**看不见那个数字**
///
/// 连接器只需要守两件事：**一次 [`SinkSession::write_batch`] 对应一次真实写入**
/// （内部不要再攒批 —— 攒批归内核，见 [`SinkSession`]），
/// 以及**一条会话同时不要开多个在飞写入**（内核按"一份数据一个任务"发券，
/// 一条会话开 N 个在飞写入就等于绕过了闸门）。
#[async_trait]
pub trait SinkConnector: Send + Sync {
    /// 连接器名称
    fn name(&self) -> &str;

    /// 初始化连接器
    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()>;

    /// 开启新的写入会话
    ///
    /// 每个 `sink_task` 拥有独立的 `SinkSession`（写几份就有几条会话）。
    async fn begin_session(&self, spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>>;

    /// 物理载荷写入能力（可选）。返回 `None` = 本连接器只能走逻辑面。
    fn as_physical(&self) -> Option<&dyn PhysicalSink> {
        None
    }

    /// **运行期占用申报端口**：目标端把它自己知道的常驻缓冲挂到这把尺子上
    ///
    /// 最典型的是"序列化后的请求体"和"在飞的 HTTP body" —— 引擎量不到
    /// （body 交给 `reqwest` 之后就出了池的账），目标端自己知道。
    /// 同 [`crate::SourceConnector::memory_meter`]：引擎给尺子、连接器挂账、
    /// 引擎读峰值，**引擎不解释字节是什么**。
    ///
    /// 缺省 = 不申报（引擎如实显示为"未认领"）。
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        None
    }

    /// 关闭连接器
    async fn close(&mut self) -> D2DResult<()>;
}

/// Sink 写入会话
///
/// 代表一次具体的数据写入过程（如一次 Stream Load）：写入、提交、回滚。
///
/// # 契约：**不攒批**，而且同时至多一个在飞写入
///
/// "攒多少再发一次"由**内核**落实 —— 内核从 `sink_pool` 里攒够 `flow.sink.batch_bytes`
/// 才交给连接器一次。所以一次 [`Self::write_batch`] 就是**一次真实写入**，
/// 实现里不要再攒着不发。
///
/// 这条分工不是风格问题：连接器内部攒批时，"攒着的那点内存"不在任何池的账上，
/// 而"源端停下来时最后一批一直吊着不发"要靠一个空闲超时去救 —— 那个超时本身就是
/// 攒批的副产品。攒批搬到池上之后，这两个问题一起结构性消失了。
#[async_trait]
pub trait SinkSession: Send + Sync {
    /// 写入一批数据，**立即发出去**
    ///
    /// 返回实际已确认写入的行数。
    async fn write_batch(&mut self, batch: RecordBatch) -> D2DResult<usize>;

    /// 提交本次会话（如 Stream Load 关闭 HTTP 连接并确认）
    async fn commit(&mut self) -> D2DResult<()>;

    /// 回滚/取消本次会话
    async fn abort(&mut self) -> D2DResult<()>;
}
