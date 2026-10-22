use arrow::array::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use d2d_core::D2DResult;
use std::collections::HashMap;

use crate::Transform;

/// ColumnMap 算子：列名重映射
///
/// 纯元数据操作，零数据拷贝。
pub struct ColumnMap {
    mappings: HashMap<String, String>,
}

impl ColumnMap {
    pub fn new(mappings: HashMap<String, String>) -> Self {
        Self { mappings }
    }
}

impl Transform for ColumnMap {
    fn transform(&self, input: RecordBatch) -> D2DResult<RecordBatch> {
        let schema = input.schema();
        let mut new_fields = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let new_name = self
                .mappings
                .get(field.name())
                .cloned()
                .unwrap_or_else(|| field.name().to_string());
            new_fields.push(Field::new(
                new_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
        }

        let new_schema =
            SchemaRef::new(Schema::new(new_fields).with_metadata(schema.metadata().clone()));
        Ok(RecordBatch::try_new(new_schema, input.columns().to_vec())?)
    }

    fn name(&self) -> &str {
        "column_map"
    }
}
