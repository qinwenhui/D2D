//! PostgreSQL 类型适配
//!
//! 类型词汇来自 `information_schema.columns`（实测 PG 18.4）：
//! `bigint` / `integer` / `smallint` / `numeric` / `real` / `double precision` /
//! `boolean` / `character varying` / `character` / `text` / `date` /
//! `timestamp without time zone` / `timestamp with time zone` / `time without time zone` /
//! `bytea` / `json` / `jsonb` / `uuid` / `ARRAY` / `USER-DEFINED`。
//!
//! # 两个刻意的决定
//!
//! - **`timestamp with time zone` 按 UTC 挂钟时间承载**（`tz: None`）。
//!   读取时 SQL 里做 `AT TIME ZONE 'UTC'`，会话时区也钉成 UTC，所以值是可复现的；
//!   写成"带时区的 Arrow 时间戳"反而会在下游引入一道必须对齐的时区换算。
//! - **无符号整数按更宽的有符号类型承载**（`UInt8/UInt16 → integer`、`UInt32 → bigint`、
//!   `UInt64 → numeric(20,0)`）：PostgreSQL 没有无符号整数，这是**值域不丢**的最宽写法。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, LogicalType, TemporalPrecision, TypeAdapter};

pub struct PgTypeAdapter;

impl PgTypeAdapter {
    pub fn new() -> Self {
        Self
    }

    fn precision(&self, decl: &DbTypeDecl) -> D2DResult<TemporalPrecision> {
        match decl.precision {
            None | Some(0) => Ok(TemporalPrecision::Second),
            Some(1..=3) => Ok(TemporalPrecision::Milli),
            Some(4..=6) => Ok(TemporalPrecision::Micro),
            Some(n) => Err(D2DError::schema(format!(
                "PostgreSQL 时间类型的小数位最多 6 位，声明里是 {}（{}）",
                n,
                decl.render()
            ))),
        }
    }

    fn fsp(p: &TemporalPrecision) -> D2DResult<u32> {
        match p {
            TemporalPrecision::Second => Ok(0),
            TemporalPrecision::Milli => Ok(3),
            TemporalPrecision::Micro => Ok(6),
            TemporalPrecision::Nano => Err(D2DError::schema(
                "PostgreSQL 时间类型最高微秒（6 位小数），纳秒精度需显式截断为微秒",
            )),
        }
    }
}

impl Default for PgTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for PgTypeAdapter {
    fn db_name(&self) -> &str {
        "postgres"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        let ty = match decl.base.as_str() {
            "SMALLINT" | "INT2" => LogicalType::Int16,
            "INTEGER" | "INT" | "INT4" => LogicalType::Int32,
            "BIGINT" | "INT8" => LogicalType::Int64,
            "REAL" | "FLOAT4" => LogicalType::Float32,
            "DOUBLE PRECISION" | "FLOAT8" => LogicalType::Float64,
            "NUMERIC" | "DECIMAL" => {
                let p = decl.precision.unwrap_or(38).clamp(1, 1000);
                let s = decl.scale.unwrap_or(0).clamp(0, p as i32);
                if p > 38 {
                    return Err(D2DError::schema(format!(
                        "PostgreSQL NUMERIC({},{}) 超出逻辑类型的 38 位精度 —— 请先缩小精度",
                        p, s
                    )));
                }
                LogicalType::Decimal { precision: p as u8, scale: s as i8 }
            }
            "BOOLEAN" | "BOOL" => LogicalType::Boolean,
            "CHARACTER VARYING" | "VARCHAR" | "CHARACTER" | "CHAR" | "BPCHAR" | "TEXT"
            | "NAME" | "UUID" | "USER-DEFINED" => LogicalType::Utf8,
            "DATE" => LogicalType::Date32,
            // 带时区的也按 UTC 挂钟承载（见模块文档）
            "TIMESTAMP" | "TIMESTAMPTZ" => LogicalType::Timestamp {
                precision: self.precision(decl)?,
                tz: None,
            },
            "TIME" => {
                if decl.with_time_zone {
                    return Err(D2DError::schema(
                        "PostgreSQL `time with time zone` 没有对应的逻辑类型 —— \
                         请先在查询侧转成 UTC 的 time 或文本",
                    ));
                }
                LogicalType::Time64(self.precision(decl)?)
            }
            "BYTEA" => LogicalType::LargeBinary,
            "JSON" | "JSONB" => LogicalType::Json,
            "ARRAY" => {
                return Err(D2DError::schema(format!(
                    "PostgreSQL 数组类型（{}）不在逻辑类型范围内 —— 请先在查询侧展平或用 `array_to_string`",
                    decl.render()
                )))
            }
            "INTERVAL" | "MONEY" | "XML" | "INET" | "CIDR" | "MACADDR" | "OID" | "REGCLASS" => {
                return Err(D2DError::schema(format!(
                    "PostgreSQL 类型 {} 没有对应的逻辑类型 —— 请在查询侧显式转成文本/数值",
                    decl.base
                )))
            }
            _ => {
                return Err(D2DError::schema(format!(
                    "未知 PostgreSQL 类型 '{}'：D2D 拒绝静默降级。请补充映射",
                    decl.render()
                )))
            }
        };
        ty.validate()?;
        Ok(ty)
    }

    fn to_db_type(&self, ty: &LogicalType) -> D2DResult<String> {
        Ok(match ty {
            LogicalType::Null => "text".to_string(),
            LogicalType::Boolean => "boolean".to_string(),
            LogicalType::Int8 | LogicalType::UInt8 | LogicalType::UInt16 => "smallint".to_string(),
            LogicalType::Int16 => "smallint".to_string(),
            LogicalType::Int32 => "integer".to_string(),
            LogicalType::UInt32 => "bigint".to_string(),
            LogicalType::Int64 => "bigint".to_string(),
            LogicalType::UInt64 => "numeric(20,0)".to_string(),
            LogicalType::Float32 => "real".to_string(),
            LogicalType::Float64 => "double precision".to_string(),
            LogicalType::Decimal { precision, scale } => {
                if *precision > 38 {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({},{}) 超出 PostgreSQL 适配器的 38 位上限",
                        precision, scale
                    )));
                }
                format!("numeric({},{})", precision, scale)
            }
            LogicalType::Utf8 | LogicalType::LargeUtf8 => "text".to_string(),
            LogicalType::Binary | LogicalType::LargeBinary => "bytea".to_string(),
            LogicalType::Date32 => "date".to_string(),
            LogicalType::Time64(p) => format!("time({})", Self::fsp(p)?),
            LogicalType::Timestamp { precision, tz } => {
                if tz.is_some() {
                    return Err(D2DError::schema(
                        "本适配器把时间戳按 UTC 挂钟承载，不接受带时区的逻辑类型",
                    ));
                }
                format!("timestamp({})", Self::fsp(precision)?)
            }
            LogicalType::Json => "jsonb".to_string(),
            LogicalType::Enum(_) => "text".to_string(),
            other => {
                return Err(D2DError::schema(format!(
                    "PostgreSQL 没有能承载 {} 的列类型",
                    other.describe()
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> PgTypeAdapter {
        PgTypeAdapter::new()
    }

    /// 实测 information_schema 的 data_type 逐条钉住
    #[test]
    fn test_real_data_types() {
        for (decl, want) in [
            ("bigint", LogicalType::Int64),
            ("integer", LogicalType::Int32),
            ("smallint", LogicalType::Int16),
            ("boolean", LogicalType::Boolean),
            ("real", LogicalType::Float32),
            ("double precision", LogicalType::Float64),
            ("character varying(200)", LogicalType::Utf8),
            ("character(10)", LogicalType::Utf8),
            ("text", LogicalType::Utf8),
            ("uuid", LogicalType::Utf8),
            ("date", LogicalType::Date32),
            ("bytea", LogicalType::LargeBinary),
            ("json", LogicalType::Json),
            ("jsonb", LogicalType::Json),
            ("user-defined", LogicalType::Utf8),
        ] {
            assert_eq!(a().to_logical_from_str(decl).unwrap(), want, "{}", decl);
        }
        assert_eq!(
            a().to_logical_from_str("numeric(38,10)").unwrap(),
            LogicalType::Decimal {
                precision: 38,
                scale: 10
            }
        );
    }

    #[test]
    fn test_timestamp_variants() {
        let ts = |p| LogicalType::Timestamp {
            precision: p,
            tz: None,
        };
        assert_eq!(
            a().to_logical_from_str("timestamp(6)").unwrap(),
            ts(TemporalPrecision::Micro)
        );
        assert_eq!(
            a().to_logical_from_str("timestamp(3) with time zone")
                .unwrap(),
            ts(TemporalPrecision::Milli),
            "带时区的也按 UTC 挂钟承载"
        );
        assert_eq!(
            a().to_logical_from_str("time(3)").unwrap(),
            LogicalType::Time64(TemporalPrecision::Milli)
        );
        let e = a()
            .to_logical_from_str("time(3) with time zone")
            .unwrap_err()
            .to_string();
        assert!(e.contains("time with time zone"), "{}", e);
    }

    #[test]
    fn test_unsupported_types_rejected() {
        for (decl, needle) in [
            ("array", "数组"),
            ("interval", "没有对应"),
            ("money", "没有对应"),
        ] {
            let e = a().to_logical_from_str(decl).unwrap_err().to_string();
            assert!(e.contains(needle), "{} 的报错: {}", decl, e);
        }
        let e = a()
            .to_logical_from_str("numeric(50,0)")
            .unwrap_err()
            .to_string();
        assert!(e.contains("38"), "{}", e);
    }

    /// 无符号整数按更宽的有符号类型承载，值域不丢
    #[test]
    fn test_unsigned_widening() {
        assert_eq!(a().to_db_type(&LogicalType::UInt8).unwrap(), "smallint");
        assert_eq!(a().to_db_type(&LogicalType::UInt32).unwrap(), "bigint");
        assert_eq!(
            a().to_db_type(&LogicalType::UInt64).unwrap(),
            "numeric(20,0)"
        );
    }

    #[test]
    fn test_round_trip() {
        for ty in [
            LogicalType::Int16,
            LogicalType::Int64,
            LogicalType::Decimal {
                precision: 38,
                scale: 10,
            },
            LogicalType::Boolean,
            LogicalType::Utf8,
            LogicalType::LargeBinary,
            LogicalType::Date32,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None,
            },
            LogicalType::Time64(TemporalPrecision::Milli),
            LogicalType::Json,
        ] {
            let decl = a().to_db_type(&ty).unwrap();
            assert_eq!(a().to_logical_from_str(&decl).unwrap(), ty, "{}", decl);
        }
    }
}
