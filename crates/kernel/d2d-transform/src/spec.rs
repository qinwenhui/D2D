//! 算子的**声明式描述**：YAML 与代码之间的唯一桥梁
//!
//! ```yaml
//! transform:
//!   - op: "project"
//!     columns: ["id", "name"]
//!   - op: "cast"
//!     mappings:
//!       - {column: "amount", target_type: "float64"}
//! ```
//!
//! 程序化构造走同一条路：
//! ```ignore
//! TransformSpec::Project { columns: vec!["id".into()] }
//! ```
//!
//! # 为什么是一个**强类型枚举**，而不是 `{op: String, params: Value}`
//!
//! 旧的 `TransformOpConfig { op: String, params: Value }` 有两个静默降级口子：
//!
//! - **算子名拼错** → 只打一条 `warn!("unknown transform operator, skipping")`，
//!   任务照跑，转换**悄悄没做**。用户拿到的是一份"成功"的错误数据。
//! - **参数名拼错**（`column` 写成 `colums`）→ `params.get(..)` 返回 `None`，
//!   那个分支被跳过，同样是静默少做一步转换。
//!
//! 这两个口子用 `#[serde(tag = "op", deny_unknown_fields)]` 一次性堵死：
//! 拼错的名字在**解析配置时**就报错，根本跑不起来。
//! 这与本项目"宁可大声失败，不可静默降级"的一贯口径一致。

use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{Cast, ColumnMap, ConstColumn, ConstValue, Project, Transform};

/// 一个转换算子的声明
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransformSpec {
    /// 选列（也可用于重排、去列）
    Project { columns: Vec<String> },
    /// 类型转换
    Cast { mappings: Vec<CastMapping> },
    /// 列重命名
    ColumnMap { mappings: Vec<NameMapping> },
    /// 构造常量列（不读库）
    ConstColumn {
        name: String,
        value: serde_json::Value,
    },
}

/// `cast` 的一条映射
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CastMapping {
    pub column: String,
    /// 目标 Arrow 类型（`int64` / `float64` / `utf8` / `date32` / `timestamp` …）
    pub target_type: String,
}

/// `column_map` 的一条映射
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NameMapping {
    pub from: String,
    pub to: String,
}

impl TransformSpec {
    /// 算子名（日志与直通路径归因用）
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Project { .. } => "project",
            Self::Cast { .. } => "cast",
            Self::ColumnMap { .. } => "column_map",
            Self::ConstColumn { .. } => "const_column",
        }
    }

    /// 构造算子实例
    pub fn build(&self) -> D2DResult<Box<dyn Transform>> {
        match self {
            Self::Project { columns } => {
                if columns.is_empty() {
                    return Err(D2DError::config(
                        "project 算子的 columns 不能为空（空选列等于把所有列都丢掉）",
                    ));
                }
                Ok(Box::new(Project::new(columns.clone())))
            }
            Self::Cast { mappings } => {
                let mut map = HashMap::with_capacity(mappings.len());
                for m in mappings {
                    map.insert(m.column.clone(), arrow_type_of(&m.target_type)?);
                }
                if map.is_empty() {
                    return Err(D2DError::config("cast 算子的 mappings 不能为空"));
                }
                Ok(Box::new(Cast::new(map)))
            }
            Self::ColumnMap { mappings } => {
                let mut map = HashMap::with_capacity(mappings.len());
                for m in mappings {
                    map.insert(m.from.clone(), m.to.clone());
                }
                if map.is_empty() {
                    return Err(D2DError::config("column_map 算子的 mappings 不能为空"));
                }
                Ok(Box::new(ColumnMap::new(map)))
            }
            Self::ConstColumn { name, value } => {
                let v = const_value_of(value)?;
                Ok(Box::new(ConstColumn::new(name.clone(), v)))
            }
        }
    }
}

/// 把一串声明构造成算子链
pub fn build_chain(specs: &[TransformSpec]) -> D2DResult<crate::TransformChain> {
    let mut ops: Vec<Box<dyn Transform>> = Vec::with_capacity(specs.len());
    for s in specs {
        ops.push(s.build().map_err(|e| {
            D2DError::config(format!("构造 transform 算子 '{}' 失败：{}", s.op_name(), e))
        })?);
    }
    Ok(crate::TransformChain::new(ops))
}

/// 类型名 → Arrow 类型
///
/// 放在这里（而不是 CLI）是因为它是**算子域**的知识：`cast` 的目标类型集合
/// 由算子自己定义，不该由调用方解释。
pub fn arrow_type_of(s: &str) -> D2DResult<arrow::datatypes::DataType> {
    use arrow::datatypes::DataType;
    let t = match s.trim().to_lowercase().as_str() {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date32" | "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        other => {
            return Err(D2DError::config(format!(
                "不支持的 cast 目标类型 '{}'。支持：int8/16/32/64、uint8/16/32/64、\
                 float32/64、utf8、binary、date32、timestamp",
                other
            )))
        }
    };
    Ok(t)
}

/// JSON 字面量 → 常量值
pub fn const_value_of(v: &serde_json::Value) -> D2DResult<ConstValue> {
    use serde_json::Value;
    match v {
        Value::Null => Ok(ConstValue::Null),
        Value::Bool(b) => Ok(ConstValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ConstValue::Int64(i))
            } else if let Some(f) = n.as_f64() {
                Ok(ConstValue::Float64(f))
            } else {
                Err(D2DError::config(format!("不支持的数值常量: {}", n)))
            }
        }
        Value::String(s) => Ok(ConstValue::Utf8(s.clone())),
        other => Err(D2DError::config(format!(
            "不支持的常量类型（只支持 null/bool/number/string）: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<Vec<TransformSpec>, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn test_parse_all_ops() {
        let specs = parse(
            r#"
- op: "project"
  columns: ["a", "b"]
- op: "cast"
  mappings:
    - {column: "amount", target_type: "float64"}
- op: "column_map"
  mappings:
    - {from: "a", to: "b"}
- op: "const_column"
  name: "src"
  value: "dm"
"#,
        )
        .unwrap();
        assert_eq!(specs.len(), 4);
        assert_eq!(specs[0].op_name(), "project");
        assert_eq!(specs[3].op_name(), "const_column");
        for s in &specs {
            s.build().unwrap_or_else(|e| panic!("{}: {}", s.op_name(), e));
        }
    }

    /// **回归**：算子名拼错必须在**解析配置时**就失败
    ///
    /// 旧实现是 `{op: String, params: Value}` + `_ => warn!("unknown operator, skipping")`：
    /// 任务照跑，那一步转换**悄悄没做**，用户拿到一份"成功"的错误数据。
    #[test]
    fn test_unknown_operator_is_a_parse_error_not_a_warning() {
        let e = parse("- op: \"projct\"\n  columns: [\"a\"]\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("projct"), "{}", e);
    }

    /// **回归**：参数名拼错同样必须失败（旧实现是 `params.get(..)` 返回 None 静默跳过）
    #[test]
    fn test_misspelled_param_is_a_parse_error() {
        let e = parse("- op: \"project\"\n  colums: [\"a\"]\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("colums"), "{}", e);

        let e = parse("- op: \"cast\"\n  mappings:\n    - {column: a, target_typ: int64}\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("target_typ"), "{}", e);
    }

    #[test]
    fn test_build_rejects_empty_and_bad_values() {
        assert!(TransformSpec::Project { columns: vec![] }
            .build()
            .map(|_| ())
            .unwrap_err()
            .to_string()
            .contains("不能为空"));
        assert!(TransformSpec::Cast {
            mappings: vec![CastMapping {
                column: "a".into(),
                target_type: "int80".into()
            }]
        }
        .build()
        .map(|_| ())
        .unwrap_err()
        .to_string()
        .contains("不支持"));

        // 错误信息要能指导修正
        let e = arrow_type_of("int80").unwrap_err().to_string();
        // 错误信息必须**列出可选项**，否则用户只能猜下一个写什么
        assert!(e.contains("支持："), "应当列出支持的类型: {}", e);
        assert!(e.contains("utf8"), "应当列出支持的类型: {}", e);
    }

    #[test]
    fn test_build_chain_preserves_order() {
        let specs = parse(
            r#"
- op: "project"
  columns: ["a"]
- op: "const_column"
  name: "c"
  value: 1
"#,
        )
        .unwrap();
        let chain = build_chain(&specs).unwrap();
        assert_eq!(chain.operator_names(), vec!["project", "const_column"]);
    }

    #[test]
    fn test_const_value_types() {
        assert_eq!(const_value_of(&serde_json::json!(null)).unwrap(), ConstValue::Null);
        assert_eq!(const_value_of(&serde_json::json!(true)).unwrap(), ConstValue::Bool(true));
        assert_eq!(const_value_of(&serde_json::json!(7)).unwrap(), ConstValue::Int64(7));
        assert_eq!(const_value_of(&serde_json::json!(1.5)).unwrap(), ConstValue::Float64(1.5));
        assert_eq!(
            const_value_of(&serde_json::json!("x")).unwrap(),
            ConstValue::Utf8("x".into())
        );
        assert!(const_value_of(&serde_json::json!({"a": 1})).is_err());
    }

    /// 程序化构造与 YAML 落到同一条路
    #[test]
    fn test_programmatic_and_yaml_agree() {
        let from_yaml = parse("- op: \"project\"\n  columns: [\"a\", \"b\"]\n").unwrap();
        let programmatic = vec![TransformSpec::Project {
            columns: vec!["a".into(), "b".into()],
        }];
        assert_eq!(from_yaml, programmatic);
        assert_eq!(
            build_chain(&from_yaml).unwrap().operator_names(),
            build_chain(&programmatic).unwrap().operator_names()
        );
    }
}
