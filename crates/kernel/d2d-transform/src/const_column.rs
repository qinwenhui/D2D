use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use d2d_core::D2DResult;
use std::sync::Arc;

use crate::Transform;

/// 常量列算子：在引擎侧构造定值列，不占用数据库查询
///
/// 典型场景：
/// - 打标记列（`source_system = 'dm'`、`batch_id = '20261014'`）
/// - DB 侧不方便写的常量
///
/// V1 支持 Int64 / Float64 / Utf8 / Boolean / Null 类型。
#[derive(Debug, Clone)]
pub struct ConstColumn {
    /// 输出列名
    pub name: String,
    /// 常量值
    pub value: ConstValue,
}

/// 引擎侧常量值（V1 有限集）
///
/// `PartialEq` 是给声明式构造（`spec.rs`）的单测用的：要能断言
/// "YAML 里写的值"与"程序里构造的值"是同一个。
#[derive(Debug, Clone, PartialEq)]
pub enum ConstValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
}

impl ConstColumn {
    pub fn new(name: impl Into<String>, value: ConstValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }

    fn build_array(&self, len: usize) -> ArrayRef {
        use arrow::array::*;
        match &self.value {
            ConstValue::Null => new_null_array(&arrow::datatypes::DataType::Null, len),
            ConstValue::Bool(v) => Arc::new(BooleanArray::from(vec![*v; len])) as ArrayRef,
            ConstValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; len])) as ArrayRef,
            ConstValue::Float64(v) => Arc::new(Float64Array::from(vec![*v; len])) as ArrayRef,
            ConstValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str(); len])) as ArrayRef,
        }
    }

    /// 该常量列的 Arrow 类型
    pub fn data_type(&self) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        match &self.value {
            ConstValue::Null => DataType::Null,
            ConstValue::Bool(_) => DataType::Boolean,
            ConstValue::Int64(_) => DataType::Int64,
            ConstValue::Float64(_) => DataType::Float64,
            ConstValue::Utf8(_) => DataType::Utf8,
        }
    }
}

impl Transform for ConstColumn {
    fn transform(&self, input: RecordBatch) -> D2DResult<RecordBatch> {
        let len = input.num_rows();
        let array = self.build_array(len);

        // 追加到现有列之后
        let mut fields: Vec<Field> = input
            .schema()
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();
        let mut columns = Vec::with_capacity(fields.len() + 1);
        for c in input.columns() {
            columns.push(Arc::clone(c) as ArrayRef);
        }

        fields.push(Field::new(&self.name, self.data_type(), true));
        columns.push(array);

        let schema =
            SchemaRef::new(Schema::new(fields).with_metadata(input.schema().metadata().clone()));
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn name(&self) -> &str {
        "const_column"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_batch(rows: usize) -> RecordBatch {
        use arrow::array::Int32Array;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
        )
        .unwrap()
    }

    #[test]
    fn test_const_int64() {
        let op = ConstColumn::new("flag", ConstValue::Int64(1));
        let out = op.transform(input_batch(3)).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.schema().field(1).name(), "flag");
        let col = out.column(1);
        let arr = col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_const_utf8() {
        let op = ConstColumn::new("src", ConstValue::Utf8("dm".to_string()));
        let out = op.transform(input_batch(2)).unwrap();
        let arr = out
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "dm");
    }

    #[test]
    fn test_const_empty_batch() {
        let op = ConstColumn::new("flag", ConstValue::Bool(true));
        let out = op.transform(input_batch(0)).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.num_columns(), 2);
    }
}
