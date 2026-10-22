pub mod cast;
pub mod column_map;
pub mod const_column;
pub mod project;
pub mod spec;

pub use cast::Cast;
pub use column_map::ColumnMap;
pub use const_column::{ConstColumn, ConstValue};
pub use project::Project;
pub use spec::{build_chain, CastMapping, NameMapping, TransformSpec};

use arrow::array::RecordBatch;
use d2d_core::D2DResult;
use std::sync::Arc;

/// 转换算子 Trait
///
/// 所有数据转换操作均基于 Arrow RecordBatch，向量化执行。
pub trait Transform: Send + Sync {
    /// 转换一批数据
    ///
    /// 输入和输出的 Schema 可能不同。
    fn transform(&self, input: RecordBatch) -> D2DResult<RecordBatch>;

    /// 算子名称（用于指标和日志）
    fn name(&self) -> &str;
}

/// 算子链：按顺序执行多个算子
///
/// 内部使用 Arc 包装，支持跨任务克隆（共享同一算子集）。
#[derive(Clone, Default)]
pub struct TransformChain {
    operators: Arc<Vec<Box<dyn Transform>>>,
}

/// 手写而非 derive：`dyn Transform` 不是 `Debug`，而 `Job` 需要能打印。
/// 只打算子名足够 —— 排查时要看的是"走了哪几步"，不是每步的内部状态。
impl std::fmt::Debug for TransformChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.operator_names()).finish()
    }
}

impl TransformChain {
    pub fn new(operators: Vec<Box<dyn Transform>>) -> Self {
        Self {
            operators: Arc::new(operators),
        }
    }

    pub fn transform(&self, mut batch: RecordBatch) -> D2DResult<RecordBatch> {
        for op in self.operators.iter() {
            batch = op.transform(batch)?;
        }
        Ok(batch)
    }

    pub fn len(&self) -> usize {
        self.operators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// 算子名称列表（按执行顺序）
    ///
    /// 供执行路径规划器归因：回落逻辑面时必须能说清「是哪些算子挡了直通」。
    pub fn operator_names(&self) -> Vec<String> {
        self.operators
            .iter()
            .map(|op| op.name().to_string())
            .collect()
    }
}
