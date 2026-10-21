//! 最小内核：错误体系 + `RecordBatch` 流别名 + 几个 `RecordBatch` 便利方法
//!
//! # 这个 crate 只放"所有 crate 都要用、且与连哪个库无关"的东西
//!
//! 1. [`D2DError`] / [`D2DResult`]：全仓唯一的错误类型。它区分**可重试**与**致命**，
//!    因为执行引擎要据此决定能不能重试一个分片。
//! 2. [`RecordBatchStream`]：异步 Arrow 流的类型别名，Source 产出、Transform 中转的公共形状。
//! 3. [`RecordBatchExt`]：几个读写批次的小工具（估内存、投影选列）。
//!
//! # 这里**不该**放什么（曾经放错了，已删）
//!
//! 这里曾经同时住着 `metrics.rs`（指标上下文）、`types.rs`（SQL 类型名 → Arrow）、
//! `schema.rs`（`ArrowType` / `SchemaMapping`）。它们的代价不是"多几行"，
//! 而是**同一个概念有两份权威**：
//!
//! ```text
//! d2d-core::schema::ArrowType   ←→  d2d-types::LogicalType        谁说了算？
//! d2d-core::types::db_type_to_arrow（带 unwrap_or(Utf8) 静默降级）
//!                               ←→  d2d-types::TypeAdapter        谁说了算？
//! ```
//!
//! 类型规则只能有一套 —— 达梦读取路径早期正是"两套规则各自演化"导致字段错位。
//! 所以权威统一在 `d2d-types`，`d2d-core` 不再碰类型映射。
//! 指标那条线随任务统计一起废弃（统计在 `d2d-execution` 的 `FlowReport` 上，
//! 再由一个全局 `MetricsContext` 记一遍没有意义）。

pub mod error;
pub mod record_batch;

pub use error::{D2DError, D2DResult};
pub use record_batch::{RecordBatchExt, RecordBatchStream};
