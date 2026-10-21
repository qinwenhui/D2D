//! 达梦类型适配
//!
//! `DamengTypeAdapter`：达梦类型声明 ↔ D2D 规范逻辑类型的双向翻译。
//!
//! 它曾经是一个独立 crate（`crates/connectors/d2d-dameng/src/types.rs`）。并进来是因为**一个数据库占两个
//! 同级 crate** 会让"加一个连接器"变成"加两个 crate" —— 而类型适配器除了这个连接器
//! 没有第二个使用者，属于本库的实现细节。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, FixedOffset, LobPolicy, LogicalType, TemporalPrecision, TypeAdapter};

/// 达梦（DM8）类型适配器
///
/// 覆盖达梦特有语义：
/// - `NUMBER` 无参时默认 DECIMAL(38,10)；`NUMBER(p)` 省略标度 = DECIMAL(p,0)
/// - `TIMESTAMP` 可带精度参数 `(0..6)`；`WITH TIME ZONE` 显式携带时区
/// - `IMAGE`/`LONGVARBINARY` → Binary；`TEXT`/`LONGVARCHAR` → Utf8
/// - `BLOB`/`CLOB` 走 LobPolicy::RequiresEncoding（写入前必须显式编码）
/// - `INTERVAL` 系列 → Interval
pub struct DamengTypeAdapter;

impl DamengTypeAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DamengTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for DamengTypeAdapter {
    fn db_name(&self) -> &str {
        "dameng"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        let ty = match decl.base.as_str() {
            // ── 精确数值 ──
            "NUMBER" | "NUMERIC" | "DECIMAL" | "DEC" => {
                // 达梦 NUMBER(p[,s]) 精度可达 38；无参 NUMBER 默认 (38,10)。
                // 显式精度但省略标度 = NUMBER(p,0)（SQL 标准语义，DM 一致）
                let precision = decl.precision.unwrap_or(38).clamp(1, 38) as u8;
                let scale = match (decl.precision, decl.scale) {
                    (Some(_), None) => 0,
                    (_, Some(s)) => s.clamp(0, precision as i32) as i8,
                    (None, None) => 10,
                };
                LogicalType::Decimal { precision, scale }
            }

            // ── 整数 ──
            "TINYINT" | "BYTE" => LogicalType::Int8,
            "SMALLINT" | "SMALLIDENTITY" => LogicalType::Int16,
            "INT" | "INTEGER" | "IDENTITY" | "BIGINT IDENTITY" => LogicalType::Int32,
            "BIGINT" => LogicalType::Int64,
            "BIT" => LogicalType::Boolean,

            // ── 浮点 ──
            "FLOAT" => LogicalType::Float64,
            "DOUBLE" | "DOUBLE PRECISION" => LogicalType::Float64,
            "REAL" => LogicalType::Float32,

            // ── 字符串 ──
            "VARCHAR" | "VARCHAR2" | "CHAR" | "CHARACTER" => LogicalType::Utf8,
            "TEXT" | "LONGVARCHAR" | "CLOB" => LogicalType::LargeUtf8,

            // ── 二进制 ──
            "BINARY" | "VARBINARY" => LogicalType::Binary,
            "IMAGE" | "LONGVARBINARY" | "BLOB" => LogicalType::LargeBinary,

            // ── 时间（显式精度与时区） ──
            "DATE" => LogicalType::Date32,
            "TIME" => LogicalType::Time64(self.temporal_precision(decl)),
            "TIME WITH TIME ZONE" => LogicalType::Time64(self.temporal_precision(decl)),
            "TIMESTAMP" => LogicalType::Timestamp {
                precision: self.temporal_precision(decl),
                tz: if decl.with_time_zone {
                    Some(self.default_tz().unwrap_or(FixedOffset::cst()))
                } else {
                    None // 达梦 TIMESTAMP 无时区 = 本地时间
                },
            },
            "DATETIME" | "DATETIME WITH TIME ZONE" => LogicalType::Timestamp {
                precision: self.temporal_precision(decl),
                tz: if decl.with_time_zone {
                    Some(self.default_tz().unwrap_or(FixedOffset::cst()))
                } else {
                    None
                },
            },

            // ── 区间 ──
            "INTERVAL" => LogicalType::Interval,
            "INTERVAL YEAR" | "INTERVAL MONTH" => LogicalType::Interval,
            "INTERVAL DAY" | "INTERVAL HOUR" | "INTERVAL MINUTE" | "INTERVAL SECOND" => {
                LogicalType::Interval
            }

            // ── 其他 ──
            "BFILE" | "SYS.ANYDATA" => {
                return Err(D2DError::schema(format!(
                    "达梦类型 '{}' 无对应逻辑类型，需在 Connector 层显式处理（如 SYS.ANYDATA 请先转 XML/JSON 文本列）",
                    decl.render()
                )))
            }

            _ => {
                return Err(D2DError::schema(format!(
                    "未知达梦类型 '{}'：D2D 拒绝静默降级。请在 d2d-dameng/src/types.rs 中补充映射，或在 source.query 中显式 CAST 为已知类型",
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
            LogicalType::Boolean => "BIT".to_string(),
            LogicalType::Int8 => "TINYINT".to_string(),
            LogicalType::Int16 => "SMALLINT".to_string(),
            LogicalType::Int32 => "INT".to_string(),
            LogicalType::Int64 => "BIGINT".to_string(),
            LogicalType::UInt8 => "TINYINT".to_string(),
            LogicalType::UInt16 => "SMALLINT".to_string(),
            LogicalType::UInt32 => "INT".to_string(),
            LogicalType::UInt64 => "BIGINT".to_string(),
            LogicalType::Float32 => "REAL".to_string(),
            LogicalType::Float64 => "DOUBLE".to_string(),
            LogicalType::Decimal { precision, scale } => format!("NUMBER({},{})", precision, scale),
            LogicalType::Utf8 => "VARCHAR".to_string(),
            LogicalType::LargeUtf8 => "TEXT".to_string(),
            LogicalType::Binary => "VARBINARY".to_string(),
            LogicalType::LargeBinary => "BLOB".to_string(),
            LogicalType::Date32 => "DATE".to_string(),
            LogicalType::Time64(p) => format!("TIME({})", self.precision_digits(p)),
            LogicalType::Timestamp { precision, tz } => {
                let digits = self.precision_digits(precision);
                match tz {
                    Some(_) => format!("TIMESTAMP({}) WITH TIME ZONE", digits),
                    None => format!("TIMESTAMP({})", digits),
                }
            }
            LogicalType::Interval => "INTERVAL DAY TO SECOND".to_string(),
            LogicalType::Duration => "INTERVAL DAY TO SECOND".to_string(),
            LogicalType::List(_) => return Err(D2DError::schema("达梦不支持 LIST 类型写入")),
            LogicalType::Struct(_) => return Err(D2DError::schema("达梦不支持 STRUCT 类型写入")),
            LogicalType::Map(_, _) => return Err(D2DError::schema("达梦不支持 MAP 类型写入")),
            LogicalType::Json => return Err(D2DError::schema("达梦 JSON 需显式落为 TEXT 列")),
            LogicalType::Enum(_) => {
                return Err(D2DError::schema("达梦不支持 ENUM，请映射为 VARCHAR"))
            }
        })
    }

    fn lob_policy(&self) -> LobPolicy {
        // 达梦 BLOB/CLOB 可流式读，但写入文本类 sink 前必须显式编码
        LobPolicy::RequiresEncoding
    }

    fn default_temporal_precision(&self) -> TemporalPrecision {
        // 达梦 TIMESTAMP 默认微妙
        TemporalPrecision::Micro
    }

    fn default_tz(&self) -> Option<FixedOffset> {
        // 达梦 TIMESTAMP WITH TIME ZONE 默认库时区，V1 保守按 UTC+8 声明
        Some(FixedOffset::cst())
    }
}

impl DamengTypeAdapter {
    fn temporal_precision(&self, decl: &DbTypeDecl) -> TemporalPrecision {
        // 达梦 TIMESTAMP(n) 的 n 是"秒的小数位数"（0..=6）：
        // 0=秒, 1-3=毫秒级, 4-6=微秒级；达梦标准无纳秒（9 位按纳秒容纳）
        match decl.precision {
            Some(0) => TemporalPrecision::Second,
            Some(1) | Some(2) | Some(3) => TemporalPrecision::Milli,
            Some(4) | Some(5) | Some(6) => TemporalPrecision::Micro,
            Some(9) => TemporalPrecision::Nano,
            _ => self.default_temporal_precision(),
        }
    }

    fn precision_digits(&self, p: &TemporalPrecision) -> u8 {
        match p {
            TemporalPrecision::Second => 0,
            TemporalPrecision::Milli => 3,
            TemporalPrecision::Micro => 6,
            TemporalPrecision::Nano => 9,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> DamengTypeAdapter {
        DamengTypeAdapter::new()
    }

    #[test]
    fn test_number_with_params() {
        let t = a().to_logical_from_str("NUMBER(19,4)").unwrap();
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 19,
                scale: 4
            }
        );
    }

    #[test]
    fn test_number_no_params_defaults_38_10() {
        let t = a().to_logical_from_str("NUMBER").unwrap();
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 38,
                scale: 10
            }
        );
    }

    #[test]
    fn test_number_precision_clamped() {
        // 超出 38 位应钳制而非报错（达梦封顶 38）
        let t = a().to_logical_from_str("NUMBER(50,5)").unwrap();
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 38,
                scale: 5
            }
        );
    }

    #[test]
    fn test_number_precision_only_means_scale_zero() {
        // NUMBER(p) 省略标度 = NUMBER(p,0)（SQL 标准语义，DM 一致）
        let t = a().to_logical_from_str("NUMBER(1)").unwrap();
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 1,
                scale: 0
            }
        );
        let t2 = a().to_logical_from_str("NUMBER(10)").unwrap();
        assert_eq!(
            t2,
            LogicalType::Decimal {
                precision: 10,
                scale: 0
            }
        );
    }

    #[test]
    fn test_timestamp_precision_digits() {
        let t = a().to_logical_from_str("TIMESTAMP(3)").unwrap();
        assert_eq!(
            t,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Milli,
                tz: None
            }
        );
    }

    #[test]
    fn test_timestamp_no_tz_is_local() {
        let t = a().to_logical_from_str("TIMESTAMP").unwrap();
        assert_eq!(
            t,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None
            }
        );
    }

    #[test]
    fn test_timestamp_with_tz() {
        let t = a()
            .to_logical_from_str("TIMESTAMP(6) WITH TIME ZONE")
            .unwrap();
        assert_eq!(
            t,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: Some(FixedOffset::cst())
            }
        );
    }

    #[test]
    fn test_blob_is_large_binary_not_utf8() {
        let t = a().to_logical_from_str("BLOB").unwrap();
        assert_eq!(t, LogicalType::LargeBinary);
        assert!(t.is_binary());
    }

    #[test]
    fn test_unknown_type_rejected_not_fallback() {
        let err = a().to_logical_from_str("WEIRD_TYPE").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("WEIRD_TYPE"), "错误应含原始类型名: {}", msg);
        assert!(msg.contains("拒绝静默降级"), "应明确拒绝降级: {}", msg);
    }

    #[test]
    fn test_from_logical_decimal() {
        let s = a()
            .to_db_type(&LogicalType::Decimal {
                precision: 27,
                scale: 9,
            })
            .unwrap();
        assert_eq!(s, "NUMBER(27,9)");
    }

    #[test]
    fn test_from_logical_timestamp_roundtrip() {
        let ty = LogicalType::Timestamp {
            precision: TemporalPrecision::Micro,
            tz: None,
        };
        let decl = a().to_db_type(&ty).unwrap();
        let back = a().to_logical_from_str(&decl).unwrap();
        assert_eq!(ty, back);
    }

    #[test]
    fn test_lob_policy() {
        assert_eq!(a().lob_policy(), LobPolicy::RequiresEncoding);
    }

    #[test]
    fn test_from_logical_rejects_complex() {
        assert!(a()
            .to_db_type(&LogicalType::List(Box::new(LogicalType::Int32)))
            .is_err());
    }
}
