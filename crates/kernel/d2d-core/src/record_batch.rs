use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::pin::Pin;

use crate::D2DResult;
use crate::error::D2DError;

/// 异步 RecordBatch 流
///
/// 全链路数据传递的核心抽象，Source 产出，经过 Transform，最终 Sink 消费。
pub type RecordBatchStream = Pin<Box<dyn futures::Stream<Item = D2DResult<RecordBatch>> + Send>>;

/// RecordBatch 扩展工具
pub trait RecordBatchExt {
    /// 估算内存占用（字节数）
    ///
    /// 基于 Arrow Buffer 实际分配大小，用于内存预算。
    fn estimated_memory_size(&self) -> usize;

    /// 行数
    fn num_rows(&self) -> usize;

    /// 列数
    fn num_columns(&self) -> usize;

    /// 投影选列（零拷贝）
    fn project_columns(&self, indices: &[usize]) -> D2DResult<RecordBatch>;

    /// 空判断
    fn is_empty(&self) -> bool;
}

impl RecordBatchExt for RecordBatch {
    fn estimated_memory_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum()
    }

    fn num_rows(&self) -> usize {
        self.num_rows()
    }

    fn num_columns(&self) -> usize {
        self.num_columns()
    }

    fn project_columns(&self, indices: &[usize]) -> D2DResult<RecordBatch> {
        let schema = self.schema();
        let mut columns = Vec::with_capacity(indices.len());
        let mut fields = Vec::with_capacity(indices.len());

        for &idx in indices {
            if idx >= schema.fields().len() {
                return Err(D2DError::schema(format!(
                    "project index {} out of bounds (schema has {} fields)",
                    idx,
                    schema.fields().len()
                )));
            }
            columns.push(self.column(idx).clone());
            fields.push(schema.field(idx).clone());
        }

        let new_schema = SchemaRef::new(arrow::datatypes::Schema::new(fields).with_metadata(schema.metadata().clone()));
        Ok(RecordBatch::try_new(new_schema, columns)?)
    }

    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_estimated_memory_size() {
        let batch = make_test_batch();
        let size = batch.estimated_memory_size();
        assert!(size > 0);
    }

    #[test]
    fn test_project_columns() {
        let batch = make_test_batch();
        let projected = batch.project_columns(&[1]).unwrap();
        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), "name");
    }

}
