use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use d2d_core::D2DResult;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Transform;

/// Cast 算子：类型转换
///
/// 使用 Arrow Compute Kernel 进行向量化批量转换。
pub struct Cast {
    /// 列名 → 目标类型
    mappings: HashMap<String, DataType>,
    /// 解析后的列索引 → 目标类型
    resolved: Vec<(usize, DataType)>,
}

impl Cast {
    pub fn new(mappings: HashMap<String, DataType>) -> Self {
        Self {
            mappings,
            resolved: Vec::new(),
        }
    }

    pub fn resolve(&mut self, schema: &Schema) -> d2d_core::D2DResult<()> {
        let mut resolved = Vec::with_capacity(self.mappings.len());
        for (name, target_type) in &self.mappings {
            let idx = schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| d2d_core::D2DError::schema(format!("cast column '{}' not found", name)))?;
            resolved.push((idx, target_type.clone()));
        }
        self.resolved = resolved;
        Ok(())
    }
}

impl Transform for Cast {
    fn transform(&self, input: RecordBatch) -> D2DResult<RecordBatch> {
        let schema = input.schema();
        let mut new_columns: Vec<Arc<dyn arrow::array::Array>> =
            input.columns().to_vec();
        let mut new_fields: Vec<Field> =
            schema.fields().iter().map(|f| (**f).clone()).collect();

        for &(idx, ref target_type) in &self.resolved {
            let array = input.column(idx);
            let source_type = array.data_type();

            if source_type == target_type {
                continue; // 无需转换
            }

            let casted = arrow::compute::cast(array, target_type).map_err(|e| {
                d2d_core::D2DError::transform(format!(
                    "cast failed for column '{}' from {:?} to {:?}: {}",
                    schema.field(idx).name(),
                    source_type,
                    target_type,
                    e
                ))
            })?;

            new_columns[idx] = casted;
            new_fields[idx] = Field::new(
                new_fields[idx].name(),
                target_type.clone(),
                new_fields[idx].is_nullable(),
            );
        }

        let new_schema = SchemaRef::new(Schema::new(new_fields).with_metadata(schema.metadata().clone()));
        Ok(RecordBatch::try_new(new_schema, new_columns)?)
    }

    fn name(&self) -> &str {
        "cast"
    }
}
