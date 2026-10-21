//! **连接器契约层** —— 引擎与连接器之间的界面，**不是**一个连接器
//!
//! ⚠️ 站在 `crates/connectors/` 下容易误以为它和 `d2d-dameng` / `d2d-doris` 是一类东西。
//! 不是：那两个是"某一种数据库怎么读写"，这个是"**任何**数据库要能被引擎驱动，
//! 必须实现哪几个方法"。换连接器不需要改这里；改这里意味着契约变了，所有连接器都得跟。
//!
//! # 四个 trait
//!
//! | trait | 方向 | 说什么 |
//! |---|---|---|
//! | [`SourceConnector`] | 读 | 我能怎么切分、怎么读、我的类型契约是什么 |
//! | [`SinkConnector`] | 写 | 我能怎么攒批、怎么开会话、我怎么认行 |
//! | [`PhysicalSource`] | 读（直通） | 我能产出什么样的原始字节 |
//! | [`PhysicalSink`] | 写（直通） | 这种原始字节我能不能直接吃 |
//!
//! 后两个是**成对协议**（源出的格式 ↔ 目标认的格式），这也是 source 与 sink 放在
//! 同一个 crate 的原因：它们不是两个独立的东西，是一次协商的两端。
//!
//! # 引擎不认识任何具体数据库
//!
//! [`partition`] 是切法数学、[`registry`] 是名字 → 实现的表。二者都不含具体库的名字，
//! 唯一做路径协商的地方（`d2d_execution::plan_path`）也只问能力、不比名字。

pub mod factory;
pub mod partition;
pub mod physical;
pub mod query;
pub mod registry;
pub mod sink;
pub mod source;

pub use factory::{SinkFactory, SourceFactory, TypeAdapterFactory};
pub use partition::{
    Partition, PartitionMode, PartitionPlan, PartitionPredicate, PartitionSpec, ProbedBounds,
    ResolvedBy, PLAN_VERSION,
};
pub use physical::{
    Charset, CsvDialect, PhysicalBlock, PhysicalCompat, PhysicalFormat, PhysicalSession,
    PhysicalSink, PhysicalSource, PhysicalStream,
};
pub use query::{quote_ident, QueryColumn, QueryConfig};
pub use registry::ConnectorRegistry;
pub use sink::{SinkConnector, SinkSession, SinkSpec, WriteConfig};
pub use source::{
    SourceConnector, SourceSpec, SourceSplit, SplitConfig, SplitMode, TypeContractColumn,
    UnitFootprint, UnitKind,
};

/// 连接器**类型适配器**的界面（每个连接器 crate 用 `src/types.rs` 实现它）
pub use d2d_types::{DbTypeDecl, TypeAdapter};

/// 连接器**运行期占用申报**的尺子（连接器实现者用；引擎读峰值）
///
/// 从 `d2d-memory` 转发，让连接器不必为了申报而多依赖一个 crate。
pub use d2d_memory::{MemLease, MemMeter, MemSnapshot};
