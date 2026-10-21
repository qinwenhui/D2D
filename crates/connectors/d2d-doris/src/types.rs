//! Doris 类型适配
//!
//! `DorisTypeAdapter`：Doris 类型声明 ↔ D2D 规范逻辑类型的双向翻译。
//! Doris 作为 OLAP 引擎，类型语义较简单，但 DECIMAL 上限 38/27 需注意。
//!
//! 它曾经是一个独立 crate（`crates/doris-types`）。并进来的理由同达梦：
//! 一个数据库只该占**一个** crate。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, LobPolicy, LogicalType, TemporalPrecision, TypeAdapter};

/// Doris 类型适配器
///
/// Doris 类型覆盖：TINYINT/SMALLINT/INT/BIGINT/LARGEINT、FLOAT/DOUBLE、
/// DECIMAL（精度 ≤38）、DATE/DATETIME（微秒）、CHAR/VARCHAR/STRING/TEXT、
/// 无 BLOB（大对象走 STRING+Base64 或 LARGEINT）
pub struct DorisTypeAdapter;

impl DorisTypeAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DorisTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for DorisTypeAdapter {
    fn db_name(&self) -> &str {
        "doris"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        let ty = match decl.base.as_str() {
            "TINYINT" => LogicalType::Int8,
            "SMALLINT" => LogicalType::Int16,
            "INT" | "INTEGER" => LogicalType::Int32,
            "BIGINT" => LogicalType::Int64,
            "LARGEINT" => return Err(D2DError::schema(
                "Doris LARGEINT（128 位整数）超出逻辑类型表达范围，需拆分为 BIGINT 对或转 DECIMAL",
            )),
            "BOOLEAN" | "BOOL" => LogicalType::Boolean,
            "FLOAT" => LogicalType::Float32,
            "DOUBLE" => LogicalType::Float64,

            // DECIMAL(p,s)：Doris 精度上限 38，V2 起支持 27,9 级别
            //
            // `DECIMALV3` 是 Doris 2.x 的内部名，读方向**一定会**遇到它：
            // `information_schema.COLUMNS` 报的就是 `decimalv3(30, 8)`。
            // `DECIMAL32/64/128` 是 2.1 起的新别名，同样归一到这里。
            // （本连接器取声明类型时走的是 `DESCRIBE`，它报 `decimal(30,8)` ——
            //   两条路都要能进得来，否则换个取法就"未知类型"。）
            "DECIMAL" | "DECIMALV3" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128" | "NUMERIC"
            | "DEC" => {
                let p = decl.precision.unwrap_or(38).clamp(1, 38) as u8;
                let s = decl.scale.unwrap_or(0).clamp(0, p as i32) as i8;
                LogicalType::Decimal {
                    precision: p,
                    scale: s,
                }
            }

            // 字符串族：Doris 的 STRING 即 VARCHAR(1048576)
            "CHAR" => LogicalType::Utf8,
            "VARCHAR" => LogicalType::Utf8,
            "STRING" => LogicalType::Utf8,
            "TEXT" => LogicalType::LargeUtf8,

            // 时间：DATETIME 微秒无时区；DATE 到天
            "DATE" | "DATEV2" => LogicalType::Date32,
            "DATETIME" | "DATETIMEV2" => LogicalType::Timestamp {
                precision: self.temporal_precision(decl),
                tz: None, // Doris 2.1 之前无 TIMESTAMPTZ，默认本地
            },

            // 聚合/半结构化
            "HLL" | "BITMAP" => {
                return Err(D2DError::schema(format!(
                    "Doris {} 是聚合类型，不能作为迁移目标列类型",
                    decl.base
                )))
            }
            "JSON" => LogicalType::Json,

            _ => {
                return Err(D2DError::schema(format!(
                    "未知 Doris 类型 '{}'：D2D 拒绝静默降级。请补充映射或在建表时改用标准类型",
                    decl.render()
                )))
            }
        };
        ty.validate()?;
        Ok(ty)
    }

    fn to_db_type(&self, ty: &LogicalType) -> D2DResult<String> {
        Ok(match ty {
            LogicalType::Null => "NULL".to_string(),
            LogicalType::Boolean => "BOOLEAN".to_string(),
            LogicalType::Int8 => "TINYINT".to_string(),
            LogicalType::Int16 => "SMALLINT".to_string(),
            LogicalType::Int32 => "INT".to_string(),
            LogicalType::Int64 => "BIGINT".to_string(),
            // UInt64 > INT64_MAX 的场景 Doris 用 LARGEINT 才安全，这里明确拒绝而非溢出
            LogicalType::UInt64 => {
                return Err(D2DError::schema(
                    "UInt64 超出 Doris BIGINT 值域，请转 DECIMAL(20,0) 或 LARGEINT 并补充适配",
                ))
            }
            LogicalType::UInt8 => "TINYINT".to_string(),
            LogicalType::UInt16 => "SMALLINT".to_string(),
            LogicalType::UInt32 => "INT".to_string(),
            LogicalType::Float32 => "FLOAT".to_string(),
            LogicalType::Float64 => "DOUBLE".to_string(),
            LogicalType::Decimal { precision, scale } => {
                if *precision > 38 {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({}, {}) 超出 Doris 精度上限 38",
                        precision, scale
                    )));
                }
                format!("DECIMAL({},{})", precision, scale)
            }
            LogicalType::Utf8 => "VARCHAR".to_string(),
            LogicalType::LargeUtf8 => "TEXT".to_string(),
            LogicalType::Binary => {
                return Err(D2DError::schema(
                    "Doris 无 BINARY 列；二进制数据请显式 Base64 转 VARCHAR 后写入",
                ))
            }
            LogicalType::LargeBinary => {
                return Err(D2DError::schema(
                    "Doris 无 BLOB 列；大二进制请显式 Base64 转 TEXT 后写入",
                ))
            }
            LogicalType::Date32 => "DATE".to_string(),
            LogicalType::Time64(p) => {
                return Err(D2DError::schema(format!(
                    "Doris 无 TIME 类型（精度 {}），请转 DATETIME 或 VARCHAR",
                    p.as_str()
                )))
            }
            LogicalType::Timestamp { precision, tz } => {
                if tz.is_some() {
                    return Err(D2DError::schema(
                        "Doris（2.1 前）无 TIMESTAMPTZ；带时区时间戳请显式转换为本地时间后写入",
                    ));
                }
                match precision {
                    TemporalPrecision::Second | TemporalPrecision::Milli => {
                        // Doris DATETIME 是微秒存储，低精度写入自动补零，安全
                        "DATETIME".to_string()
                    }
                    TemporalPrecision::Micro => "DATETIME".to_string(),
                    TemporalPrecision::Nano => {
                        return Err(D2DError::schema(
                            "Doris DATETIME 最高微秒，纳秒精度需显式截断为微秒",
                        ))
                    }
                }
            }
            LogicalType::Interval => {
                return Err(D2DError::schema(
                    "Doris 无 INTERVAL 类型，请转 BIGINT（如毫秒数）",
                ))
            }
            LogicalType::Duration => {
                return Err(D2DError::schema("Doris 无 DURATION 类型，请转 BIGINT"))
            }
            LogicalType::List(_) | LogicalType::Struct(_) | LogicalType::Map(_, _) => {
                return Err(D2DError::schema(
                    "Doris 2.x 复杂类型（ARRAY/STRUCT/MAP）适配未接入 V1，请展平为标量列",
                ))
            }
            LogicalType::Json => "JSON".to_string(),
            LogicalType::Enum(_) => {
                return Err(D2DError::schema("Doris 无 ENUM，请映射为 VARCHAR"))
            }
        })
    }

    fn lob_policy(&self) -> LobPolicy {
        LobPolicy::RequiresEncoding
    }
}

/// `DATETIME(n)` 的 n 是**小数位数**，不是"精度等级" —— 全 crate 唯一一份这个规则
///
/// | n | 实际精度 | 逻辑类型 |
/// |---|---|---|
/// | 0（也是 `DATETIME` 缺省） | 秒 | Micro（容器给细一点，值不变） |
/// | 1–3 | 毫秒 | Milli |
/// | 4–6 | 微秒 | Micro |
///
/// 从两条路进来的 `DATETIME` 都走它：`DESCRIBE` 的声明串（本模块）与结果集元数据的
/// 类型码（`source::wire`）。**必须是同一份** —— 同一个 `datetime(6)` 从两条路读出来
/// 得到不同的逻辑类型，写回 Doris 时就会一半成功一半失败。
///
/// ⚠️ 这里曾经把 6 映射成 `Nano`，注释还写着"微秒" —— 自相矛盾。后果不是显示难看：
/// `to_db_type` 对 Nano 是**直接拒绝**的（Doris 最高微秒），于是
/// "从 Doris 读一列 `DATETIME(6)` 再写回 Doris"这条最该能跑的路径会失败。
/// 实测 `DESCRIBE` 报 `datetime(6)`、线上文本是 `...02:03:04.123456`（6 位小数）。
pub fn datetime_precision(fractional_digits: u8) -> TemporalPrecision {
    match fractional_digits {
        1..=3 => TemporalPrecision::Milli,
        _ => TemporalPrecision::Micro,
    }
}

impl DorisTypeAdapter {
    fn temporal_precision(&self, decl: &DbTypeDecl) -> TemporalPrecision {
        datetime_precision(decl.precision.unwrap_or(0).min(255) as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> DorisTypeAdapter {
        DorisTypeAdapter::new()
    }

    #[test]
    fn test_int_types() {
        assert_eq!(
            a().to_logical_from_str("BIGINT").unwrap(),
            LogicalType::Int64
        );
        assert_eq!(a().to_logical_from_str("INT").unwrap(), LogicalType::Int32);
    }

    #[test]
    fn test_decimal() {
        let t = a().to_logical_from_str("DECIMAL(27,9)").unwrap();
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 27,
                scale: 9
            }
        );
    }

    #[test]
    fn test_datetime_micro_no_tz() {
        let t = a().to_logical_from_str("DATETIME").unwrap();
        assert_eq!(
            t,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None
            }
        );
    }

    /// `DESCRIBE` 报出来的原生类型必须全部认得（回归：漏了 `decimal` 的 v3 家族）
    #[test]
    fn test_describe_native_types_all_mapped() {
        // 实测 `DESCRIBE d2d_type_probe` 的 Type 列逐项
        for (decl, want) in [
            ("int", LogicalType::Int32),
            ("boolean", LogicalType::Boolean),
            ("tinyint", LogicalType::Int8),
            ("smallint", LogicalType::Int16),
            ("bigint", LogicalType::Int64),
            ("float", LogicalType::Float32),
            ("double", LogicalType::Float64),
            (
                "decimal(18,4)",
                LogicalType::Decimal {
                    precision: 18,
                    scale: 4,
                },
            ),
            (
                "decimal(38,10)",
                LogicalType::Decimal {
                    precision: 38,
                    scale: 10,
                },
            ),
            (
                "decimal(10,0)",
                LogicalType::Decimal {
                    precision: 10,
                    scale: 0,
                },
            ),
            ("date", LogicalType::Date32),
            ("char(10)", LogicalType::Utf8),
            ("varchar(200)", LogicalType::Utf8),
            ("text", LogicalType::LargeUtf8),
            ("json", LogicalType::Json),
        ] {
            assert_eq!(a().to_logical_from_str(decl).unwrap(), want, "{}", decl);
        }
    }

    /// `information_schema` 的 MySQL 兼容写法也要能吃（同一个字段两条取法）
    #[test]
    fn test_information_schema_spellings_also_mapped() {
        // 带空格、v3 后缀 —— 实测 information_schema 就是这么写的
        assert_eq!(
            a().to_logical_from_str("decimalv3(30, 8)").unwrap(),
            LogicalType::Decimal {
                precision: 30,
                scale: 8
            }
        );
        // MySQL 兼容视图会给长度后缀
        assert_eq!(
            a().to_logical_from_str("tinyint(4)").unwrap(),
            LogicalType::Int8
        );
        assert_eq!(
            a().to_logical_from_str("bigint(20)").unwrap(),
            LogicalType::Int64
        );
    }

    /// DATETIME(n) 的 n 是小数位数，**6 是微秒不是纳秒**
    ///
    /// 曾经映射成 Nano，而 `to_db_type(Nano)` 直接拒绝 —— 于是
    /// "Doris → Doris 迁一列 DATETIME(6)"这条最该能跑的路径会失败。
    #[test]
    fn test_datetime_fractional_digits_are_not_nanoseconds() {
        let micro = TemporalPrecision::Micro;
        assert_eq!(
            a().to_logical_from_str("datetime(6)").unwrap(),
            LogicalType::Timestamp {
                precision: micro,
                tz: None
            }
        );
        assert_eq!(
            a().to_logical_from_str("datetime(4)").unwrap(),
            LogicalType::Timestamp {
                precision: micro,
                tz: None
            }
        );
        assert_eq!(
            a().to_logical_from_str("datetime(0)").unwrap(),
            LogicalType::Timestamp {
                precision: micro,
                tz: None
            }
        );
        let milli = TemporalPrecision::Milli;
        assert_eq!(
            a().to_logical_from_str("datetime(3)").unwrap(),
            LogicalType::Timestamp {
                precision: milli,
                tz: None
            }
        );

        // 等价于回归断言：读回来的类型必须能写回 Doris
        for decl in [
            "datetime",
            "datetime(0)",
            "datetime(3)",
            "datetime(4)",
            "datetime(6)",
        ] {
            let lt = a().to_logical_from_str(decl).unwrap();
            assert!(a().to_db_type(&lt).is_ok(), "{} 读回来却写不回去", decl);
        }
    }

    #[test]
    fn test_unknown_rejected() {
        let err = a().to_logical_from_str("GEOMETRY").unwrap_err();
        assert!(err.to_string().contains("拒绝静默降级"));
    }

    #[test]
    fn test_binary_from_logical_rejected() {
        let err = a().to_db_type(&LogicalType::LargeBinary).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Base64"), "应引导 Base64: {}", msg);
    }

    #[test]
    fn test_dameng_to_doris_chain() {
        // 端到端：达梦 NUMBER(38,10) → 逻辑 Decimal → Doris DECIMAL(38,10)
        let dm = d2d_types::LogicalType::Decimal {
            precision: 38,
            scale: 10,
        };
        let arrow = d2d_types::LogicalArrowBridge::to_arrow(&dm).unwrap();
        let back = d2d_types::LogicalArrowBridge::from_arrow(&arrow).unwrap();
        assert_eq!(dm, back);

        let decl = a().to_db_type(&back).unwrap();
        assert_eq!(decl, "DECIMAL(38,10)");
    }

    #[test]
    fn test_dameng_blob_to_doris_blocked() {
        // 达梦 BLOB → LargeBinary，Doris 拒绝写入，必须 Base64
        let dm_blob = d2d_types::LogicalType::LargeBinary;
        let err = a().to_db_type(&dm_blob);
        assert!(err.is_err(), "Doris 必须拒绝裸 BLOB 写入");
    }
}
