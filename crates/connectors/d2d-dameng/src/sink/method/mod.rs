//! 达梦 **writer 方向**的写入方式（method）路由
//!
//! 与读取方向同一条判据（见 设计说明 §1）：**同一种数据访问语义的不同传输实现
//! 是 `method` 参数，不是新连接器**。三条写入路径：
//!
//! | method | 怎么写 | 定位 |
//! |---|---|---|
//! | `odbc`（缺省） | 多行 `INSERT ... VALUES (?,?),(?,?)` 绑定参数 | **覆盖路径**：小批量、增量、能表达计算列与任意条件 |
//! | `dmfldr` | 落成数据文件 + 控制文件，起 `dmfldr MODE=IN` 快载 | **快路**：大批量 |
//! | `dpi` | 数组参数绑定 + `dpi_exec`（PARAMSET_SIZE）一次执行一块 | **快路且值不受限**：NULL 精确、无临时文件 |
//!
//! 与源端对称（那边是 `odbc` 覆盖 / `dmfldr` 快路 / `dpi` 快路且 NULL 精确），
//! 这样"选哪条路"在两侧是同一句话。
//!
//! # 写入方式不做的事
//!
//! 它**不参与并发**（券在内核手里）、**不自己攒批**（`batch_bytes` 由内核落实）。
//! 一次 `write_batch` 就是一次真实写入 —— 见 `SinkSession` 的契约。

pub mod dmfldr;
pub mod dpi;
pub mod odbc;

use crate::config::ConnectionConfig;
use async_trait::async_trait;
use d2d_connector::{PhysicalFormat, PhysicalSession, SinkSession};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;

/// 写入方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamengWriteKind {
    /// 绑定参数的多行 INSERT（缺省）
    Odbc,
    /// `dmfldr MODE=IN` 快载
    FlDr,
    /// DPI 数组参数绑定（一次 `dpi_exec` 执行一整块）
    Dpi,
}

impl DamengWriteKind {
    /// 从 YAML `sink.method` 解析；缺省 odbc
    pub fn parse(method: Option<&str>) -> D2DResult<Self> {
        match method {
            None | Some("") | Some("odbc") => Ok(Self::Odbc),
            Some("dmfldr") | Some("dmldr") => Ok(Self::FlDr),
            Some("dpi") => Ok(Self::Dpi),
            Some(other) => Err(D2DError::config(format!(
                "unsupported dameng sink method '{}', expected: odbc | dmfldr | dpi",
                other
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Odbc => "odbc",
            Self::FlDr => "dmfldr",
            Self::Dpi => "dpi",
        }
    }
}

impl std::fmt::Display for DamengWriteKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 写入会话要用的一切 —— 由连接器在 `init` 时固化
///
/// `columns` **不在这里**：目标列要到第一批数据才看得见（内核递过来的批自带 schema），
/// 所以由会话在第一次 `write_batch` 时定下来。契约见 `d2d_connector::SinkSession`。
#[derive(Debug, Clone)]
pub struct WriteContext {
    /// 目标 schema（`sink.database`）—— 达梦里就是模式名
    pub schema: Option<String>,
    pub table: String,
    pub conn: ConnectionConfig,
    /// 单条语句最多绑多少个参数（dmfldr/odbc 各自的物理约束）
    pub max_params: usize,
}

impl WriteContext {
    /// 目标表的限定名（`"SCHEMA"."TABLE"`）
    pub fn qualified_table(&self) -> String {
        match &self.schema {
            Some(s) => format!(
                "{}.{}",
                d2d_connector::quote_ident(s),
                d2d_connector::quote_ident(&self.table)
            ),
            None => d2d_connector::quote_ident(&self.table),
        }
    }
}

/// 写入方法 trait：只负责"怎么把这个库写进去"
///
/// schema 与列的推导由调用方给（连接器从第一批数据里取）——
/// 方法实现只管把 `RecordBatch` / 字节块落到目标表。
#[async_trait]
pub trait DamengWriteMethod: Send + Sync {
    fn kind(&self) -> DamengWriteKind;

    /// 开一条写入会话
    ///
    /// 会话是**按份**开的（内核攒够一份就开一条，写完即结束）——
    /// 见 `d2d_connector::SinkConnector::begin_session`。
    async fn begin_session(&self, ctx: Arc<WriteContext>) -> D2DResult<Box<dyn SinkSession>>;

    /// 物理面：本方法能不能直接吃外来的字节
    ///
    /// 缺省不能（返回一个说清原因的错误，而不是静默回落）——
    /// 直通闸在**规划期**由 `accepts_physical` 判定，走不到这里。
    async fn begin_physical(
        &self,
        _ctx: Arc<WriteContext>,
        _fmt: &PhysicalFormat,
    ) -> D2DResult<Box<dyn PhysicalSession>> {
        Err(D2DError::not_supported(format!(
            "dameng sink method '{}' 不支持物理直通写入",
            self.kind().as_str()
        )))
    }

    /// 收尾（清理临时文件、等子进程收工）
    async fn close(&self) -> D2DResult<()> {
        Ok(())
    }
}
