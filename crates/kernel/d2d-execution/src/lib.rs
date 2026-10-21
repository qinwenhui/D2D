//! 执行内核：把一次迁移**跑起来**
//!
//! # 两个面，一张拓扑
//!
//! - [`plan`]：在「逻辑面」与「直通面」之间做**唯一一次**协商，理由必须可读
//! - [`flow`]：`source_task → source_pool → pipeline_task → sink_pool → sink_task`
//!   的执行入口（[`run_flow`]），**两个面走同一个函数**
//! - [`pool`]：段之间的有界缓冲，带溢出策略（背压 / 溢出到磁盘 / 失败）
//!
//! 直通与逻辑面的差别被压缩到两处：`pipeline_task` 那一格做不做转换、
//! 写入会话是 `SinkSession` 还是 `PhysicalSession`。曾经直通是一条独立的执行器
//! （"一个 task 干完全程"），那让两条路径的并发账与内存账各算各的 —— 已删除。
//!
//! # 资源的两条线，互不换算
//!
//! | | 谁决定 | 不够时怎么办 |
//! |---|---|---|
//! | **并发** | 用户在 `flow.*.concurrency` 直接配 | 等（池背压）—— 绝不自动改 |
//! | **内存** | 用户配 `flow.*.pool.capacity` | 溢出到磁盘，或启动期拒绝 |
//!
//! `engine.memory_mb` **只用于启动期账本校验**（六项相加 ≤ 额度，超了列明哪一项），
//! 不参与任何并发推导。理由见 [`flow`] 模块文档。

pub mod direct;
pub mod audit;
pub mod flow;
pub mod job;
pub mod plan;
pub mod pool;

pub use audit::{RowAudit, Verdict};
pub use direct::{find_forbidden, warn_if_no_forbidden_declared, DirectConfig};
pub use flow::{
    parse_bytesize, run_flow, FlowItem, FlowReport, FlowRun, FlowSpec, MemoryAccount, MemoryLine,
    PipelineStage, PoolSpec, ResolvedFlow, SinkStage, SourceStage, NOMINAL_BATCH_BYTES,
};
/// 连接器**运行期占用**的读数（`FlowReport::connector_memory` 的元素类型）
pub use d2d_memory::MemSnapshot;
pub use job::{Job, JobBuilder};
pub use plan::{plan_path, ExecutionPath, LogicalReason};
pub use pool::{DataPool, PoolConfig, PoolOverflow, PoolStats, SpillItem};
