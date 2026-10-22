use arrow::array::RecordBatch;
use d2d_core::{D2DResult, RecordBatchExt};

use crate::Transform;

/// Project 算子：列选择 / 重排
///
/// 零拷贝实现：只重组 Schema 和 Array 引用，不复制数据。
pub struct Project {
    column_names: Vec<String>,
    column_indices: Vec<usize>,
}

impl Project {
    pub fn new(column_names: Vec<String>) -> Self {
        Self {
            column_names,
            column_indices: Vec::new(),
        }
    }

    /// 根据输入 Schema 解析列索引
    pub fn resolve(&mut self, schema: &arrow::datatypes::Schema) -> d2d_core::D2DResult<()> {
        let mut indices = Vec::with_capacity(self.column_names.len());
        for name in &self.column_names {
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                    d2d_core::D2DError::schema(format!("project column '{}' not found", name))
                })?;
            indices.push(idx);
        }
        self.column_indices = indices;
        Ok(())
    }
}

impl Transform for Project {
    fn transform(&self, input: RecordBatch) -> D2DResult<RecordBatch> {
        if self.column_indices.is_empty() {
            // 未 resolve，尝试动态解析
            return Ok(input);
        }
        input.project_columns(&self.column_indices)
    }

    fn name(&self) -> &str {
        "project"
    }
}
