//! qmdb **writer 方向**：三段式装载（begin_load → do_put → commit_load）
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`session`] | 写入会话：一次 `write_batch` = 一次完整装载 |
//!
//! # 一次 `write_batch` = 一次装载
//!
//! 内核保证一次 `write_batch` 就是一份攒好的数据，所以连接器内部**不再攒批**：
//! 拿到批 → `begin_load` 领 load_id → `do_put` 直灌（出站按 8192 行零拷贝切片）
//! → `commit_load` 拿摘要。任意阶段失败自动 `abort_load`，**不留半截数据**。
//! 会话的 `commit()` 因此是 no-op（每次写入都已原子提交）。
//!
//! # 为什么没有自动重试
//!
//! qmdb 的装载协议**没有幂等令牌**：如果 `commit_load` 的响应在网络里丢了，
//! 客户端无从区分"没提交"与"提交了但没听到回音" —— 盲目重试就是重复数据。
//! 所以失败就大声失败，让内核中止整条流。恢复姿势是改
//! `load.mode: overwrite_partitions` + 谓词圈定范围重跑（先覆盖旧文件，幂等），
//! 水位断点见 qmdb 文档 §6。
//!
//! # 列对齐（首批校验，防静默丢列）
//!
//! 服务端按**列名**对齐（大小写无关），对不上的列静默忽略/兜底 —— 对迁移工具
//! 这是"悄悄丢数据"。所以首批写入前把批的列名与目标表 schema 对一遍：
//! - 批里有、目标没有的列 → **报错**（这列的数据会被服务端悄悄扔掉）；
//! - 目标有、批里没有的列 → **警告**（服务端按 DEFAULT/NULL 兜底，可能是故意的）；
//! - 类型不同 → **警告**（服务端按 Arrow 合法 cast 自动转，非法 cast 会在
//!   装载时报中文错误，含列名）。

pub mod physical;
pub mod session;

use async_trait::async_trait;
use d2d_connector::physical::{PhysicalCompat, PhysicalFormat, PhysicalSession, PhysicalSink};
use d2d_connector::{SinkConnector, SinkSession, SinkSpec};
use d2d_core::{D2DError, D2DResult};
use d2d_memory::MemMeter;
use std::sync::Arc;
use tracing::{info, warn};

use crate::config::{LoadOptions, QmdbConnection};
use crate::flight::QmdbFlight;
use crate::sql::QmdbSqlBuilder;
use session::QmdbSinkSession;

pub struct QmdbSinkConnector {
    name: String,
    flight: Option<QmdbFlight>,
    qualified_table: Option<String>,
    /// 目标表 schema（LIMIT 0 探的；会话拿它做首批列对齐）
    target_schema: Option<arrow::datatypes::SchemaRef>,
    load: LoadOptions,
    /// 物理直通的"在飞载荷"申报尺子（`memory_meter` 口径，见 doris 同款）
    meter: Option<Arc<MemMeter>>,
}

impl QmdbSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "qmdb".to_string(),
            flight: None,
            qualified_table: None,
            target_schema: None,
            load: LoadOptions::default(),
            meter: None,
        }
    }
}

impl Default for QmdbSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for QmdbSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        QmdbConnection::validate_method(spec.method.as_deref())?;
        let connection = QmdbConnection::from_json(&spec.connection)?;
        let load = LoadOptions::from_props(&spec.props())?;

        let qualified = QmdbSqlBuilder::new(Some(&spec.database), &spec.table).qualified_table()?;
        info!(
            connector = %self.name,
            table = %qualified,
            addr = %connection.addr,
            load_mode = load.mode.as_str(),
            "initializing qmdb sink"
        );

        // 连接 + LIMIT 0 自检：表不存在/权限不足在这里大声失败，
        // 而不是等第一批数据写进去才发现
        let flight = QmdbFlight::connect(&connection).await?;
        let schema = flight.table_schema(&qualified).await.map_err(|e| {
            D2DError::config(format!(
                "qmdb 目标表 {qualified} 打不开（{e}）—— 确认表已建、账号有写权限"
            ))
        })?;
        info!(
            connector = %self.name,
            table = %qualified,
            columns = schema.fields().len(),
            "qmdb sink 已连接（Arrow Flight SQL 三段式装载）"
        );

        self.flight = Some(flight);
        self.qualified_table = Some(qualified);
        self.target_schema = Some(schema);
        self.load = load;
        // 物理直通的载荷尺子：逻辑面（IPC 编码流）不经它，只有直通会话申报
        self.meter = Some(MemMeter::new(
            "qmdb 物理装载载荷在飞",
            "一次物理装载的 CSV 载荷（表头+块字节），已移交 gRPC 层",
        ));
        Ok(())
    }

    async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let flight = self
            .flight
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb sink 未初始化：先调 init()"))?;
        let table = self
            .qualified_table
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb sink 未初始化：先调 init()"))?;
        let schema = self
            .target_schema
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb sink 未初始化：先调 init()"))?;
        Ok(Box::new(QmdbSinkSession::new(
            flight,
            table,
            schema,
            self.load.clone(),
        )))
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing qmdb sink");
        self.flight = None;
        self.qualified_table = None;
        self.target_schema = None;
        self.meter = None;
        Ok(())
    }

    fn as_physical(&self) -> Option<&dyn d2d_connector::PhysicalSink> {
        Some(self)
    }

    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }
}

/// 物理载荷写入能力（协议 §2.1：`begin_load.format=csv`，文件字节直传）
///
/// 裁决逻辑全在纯函数 [`physical::judge`] 里（可单测），这里只负责接线与开会话。
/// 直通由执行层自动协商（源声明方言 + 本裁决 Accept + 无转换算子），任何一条
/// 不满足自动回落 Arrow 逻辑面 —— 逻辑面行为不受影响。
#[async_trait]
impl PhysicalSink for QmdbSinkConnector {
    fn accepts_physical(&self, fmt: &PhysicalFormat) -> PhysicalCompat {
        if self.flight.is_none() {
            return PhysicalCompat::Reject("qmdb sink 未初始化".to_string());
        }
        physical::judge(fmt)
    }

    async fn begin_physical(&self, fmt: &PhysicalFormat) -> D2DResult<Box<dyn PhysicalSession>> {
        let flight = self
            .flight
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb sink 未初始化：先调 init()"))?;
        let table = self
            .qualified_table
            .clone()
            .ok_or_else(|| D2DError::internal("qmdb sink 未初始化：先调 init()"))?;
        let columns = fmt.columns().to_vec();
        warn!(
            connector = %self.name,
            table = %table,
            columns = columns.len(),
            format = %fmt.describe(),
            "qmdb: 开启物理直通写入会话（CSV 字节原样入三段式装载，客户端不做 Arrow 转换）"
        );
        Ok(Box::new(physical::QmdbPhysicalSession::new(
            flight,
            table,
            &columns,
            self.load.clone(),
            self.meter.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        assert_eq!(QmdbSinkConnector::new().name(), "qmdb");
    }

    /// 未初始化时开会话必须报错（配置错了不能静默退化）
    #[tokio::test]
    async fn test_begin_session_before_init_errors() {
        let c = QmdbSinkConnector::new();
        let spec = SinkSpec {
            connector: "qmdb".into(),
            connection: serde_json::json!({"addr": "h:31337", "user": "root"}),
            method: None,
            properties: None,
            database: "db".into(),
            table: "t".into(),
            write: None,
        };
        assert!(c.begin_session(&spec).await.is_err());
    }
}
