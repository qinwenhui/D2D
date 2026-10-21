//! MySQL 类型适配
//!
//! 与 Doris 的适配器同一条纪律：**一处翻译，别处不再各写一套**。
//! MySQL 的声明类型从 `information_schema.COLUMNS.COLUMN_TYPE` 来（实测 8.0.32 给的是
//! 完整写法：`tinyint`, `tinyint(1)`, `tinyint unsigned`, `decimal(38,10)`,
//! `datetime(3)`, `timestamp(6)`, `time(3)`, `varbinary(100)`, `enum('a','b')` …）。
//!
//! # 两类"看起来像但语义不同"的写法
//!
//! - **`UNSIGNED`**：是类型的一部分，不是修饰。`INT UNSIGNED` 装 0..=4294967295，
//!   映射成有符号 `Int32` 会在值域上端溢出。所以每个整数都区分有无符号。
//! - **`tinyint(1)`**：MySQL 里 `BOOLEAN` 就是它的别名，但**没有任何东西保证值只有 0/1**。
//!   映射成 `Boolean` 的话，一个存了 `2` 的标记列会让整行解析失败。
//!   所以这里如实映射成 `Int8` —— 值不受损，代价是"这列是不是布尔"要用户自己知道。
//!
//! `TIMESTAMP` 在 MySQL 里是**带时区语义**的（按会话时区换算），但文本协议读回来的是
//! 已换算好的本地时间。这里映射成无时区时间戳并保留这一点：迁移工具不该替用户改时区。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, LogicalType, TemporalPrecision, TypeAdapter};

/// MySQL 类型适配器
pub struct MysqlTypeAdapter;

impl MysqlTypeAdapter {
    pub fn new() -> Self {
        Self
    }

    /// `DATETIME(n)` / `TIME(n)` 的 n 是**小数位数**（0..=6）
    fn temporal_precision(&self, decl: &DbTypeDecl) -> D2DResult<TemporalPrecision> {
        match decl.precision {
            None | Some(0) => Ok(TemporalPrecision::Second),
            Some(1..=3) => Ok(TemporalPrecision::Milli),
            Some(4..=6) => Ok(TemporalPrecision::Micro),
            Some(n) => Err(D2DError::schema(format!(
                "MySQL 时间类型的小数位最多 6 位，声明里是 {}（{}）",
                n,
                decl.render()
            ))),
        }
    }

    /// 小数位 → MySQL 的 `(n)` 写法
    fn fsp(p: &TemporalPrecision) -> D2DResult<u32> {
        match p {
            TemporalPrecision::Second => Ok(0),
            TemporalPrecision::Milli => Ok(3),
            TemporalPrecision::Micro => Ok(6),
            TemporalPrecision::Nano => Err(D2DError::schema(
                "MySQL 时间类型最高微秒（6 位小数），纳秒精度需显式截断为微秒",
            )),
        }
    }
}

impl Default for MysqlTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for MysqlTypeAdapter {
    fn db_name(&self) -> &str {
        "mysql"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        // `INT UNSIGNED` 会被解析成 base="INT UNSIGNED"；拆出来再查表
        let unsigned = decl.base.ends_with(" UNSIGNED");
        let base = decl.base.trim_end_matches(" UNSIGNED").trim();

        let ty = match (base, unsigned) {
            ("TINYINT", false) => LogicalType::Int8,
            ("TINYINT", true) => LogicalType::UInt8,
            ("SMALLINT", false) => LogicalType::Int16,
            ("SMALLINT", true) => LogicalType::UInt16,
            // MEDIUMINT 是 MySQL 特有的 3 字节整数
            ("MEDIUMINT", false) => LogicalType::Int32,
            ("MEDIUMINT", true) => LogicalType::UInt32,
            ("INT" | "INTEGER", false) => LogicalType::Int32,
            ("INT" | "INTEGER", true) => LogicalType::UInt32,
            ("BIGINT", false) => LogicalType::Int64,
            ("BIGINT", true) => LogicalType::UInt64,
            ("FLOAT", _) => LogicalType::Float32,
            ("DOUBLE" | "REAL", _) => LogicalType::Float64,
            ("DECIMAL" | "NUMERIC" | "DEC" | "FIXED", _) => {
                // MySQL 8.0 上限：precision 65、scale 30；逻辑类型只表达 38 位
                let p = decl.precision.unwrap_or(10).clamp(1, 65);
                let s = decl.scale.unwrap_or(0).clamp(0, p as i32);
                if p > 38 {
                    return Err(D2DError::schema(format!(
                        "MySQL DECIMAL({},{}) 超出逻辑类型的 38 位精度 —— 请先在库里缩小精度，\
                         或拆分列（D2D 拒绝静默截断）",
                        p, s
                    )));
                }
                LogicalType::Decimal { precision: p as u8, scale: s as i8 }
            }
            ("DATE", _) => LogicalType::Date32,
            ("DATETIME", _) => LogicalType::Timestamp {
                precision: self.temporal_precision(decl)?,
                tz: None,
            },
            // TIMESTAMP 的时区语义已在会话层换算过，这里按无时区时间戳承载
            ("TIMESTAMP", _) => LogicalType::Timestamp {
                precision: self.temporal_precision(decl)?,
                tz: None,
            },
            ("TIME", _) => LogicalType::Time64(self.temporal_precision(decl)?),
            ("YEAR", _) => LogicalType::Int16,
            ("CHAR" | "VARCHAR", _) => LogicalType::Utf8,
            ("TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT", _) => LogicalType::LargeUtf8,
            // BIT(n) 经文本协议回的是原始字节，按二进制承载而不是猜成整数
            ("BIT", _) => LogicalType::Binary,
            ("BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB", _) => {
                LogicalType::LargeBinary
            }
            ("JSON", _) => LogicalType::Json,
            ("ENUM" | "SET", _) => LogicalType::Utf8,
            ("GEOMETRY" | "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
            | "MULTIPOLYGON" | "GEOMETRYCOLLECTION", _) => {
                return Err(D2DError::schema(format!(
                    "MySQL 空间类型 {} 没有对应的逻辑类型 —— 请先在查询侧转成 WKT 文本",
                    decl.base
                )))
            }
            _ => {
                return Err(D2DError::schema(format!(
                    "未知 MySQL 类型 '{}'：D2D 拒绝静默降级。请补充映射，或建表时改用标准类型",
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
            LogicalType::Boolean => "TINYINT(1)".to_string(),
            LogicalType::Int8 => "TINYINT".to_string(),
            LogicalType::UInt8 => "TINYINT UNSIGNED".to_string(),
            LogicalType::Int16 => "SMALLINT".to_string(),
            LogicalType::UInt16 => "SMALLINT UNSIGNED".to_string(),
            LogicalType::Int32 => "INT".to_string(),
            LogicalType::UInt32 => "INT UNSIGNED".to_string(),
            LogicalType::Int64 => "BIGINT".to_string(),
            LogicalType::UInt64 => "BIGINT UNSIGNED".to_string(),
            LogicalType::Float32 => "FLOAT".to_string(),
            LogicalType::Float64 => "DOUBLE".to_string(),
            LogicalType::Decimal { precision, scale } => {
                if *precision > 65 || *scale > 30 || *scale > *precision as i8 {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({},{}) 超出 MySQL 范围（精度 ≤65、标度 ≤30 且 ≤精度）",
                        precision, scale
                    )));
                }
                format!("DECIMAL({},{})", precision, scale)
            }
            LogicalType::Utf8 => "VARCHAR(255)".to_string(),
            LogicalType::LargeUtf8 => "LONGTEXT".to_string(),
            LogicalType::Binary => "VARBINARY(65535)".to_string(),
            LogicalType::LargeBinary => "LONGBLOB".to_string(),
            LogicalType::Date32 => "DATE".to_string(),
            LogicalType::Time64(p) => format!("TIME({})", Self::fsp(p)?),
            LogicalType::Timestamp { precision, tz } => {
                if tz.is_some() {
                    return Err(D2DError::schema(
                        "MySQL TIMESTAMP/DATETIME 不带时区；带时区的时间戳请先换算成本地时间",
                    ));
                }
                format!("DATETIME({})", Self::fsp(precision)?)
            }
            LogicalType::Json => "JSON".to_string(),
            LogicalType::Enum(_) => "VARCHAR(255)".to_string(),
            other => {
                return Err(D2DError::schema(format!(
                    "MySQL 没有能承载 {} 的列类型",
                    other.describe()
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> MysqlTypeAdapter {
        MysqlTypeAdapter::new()
    }

    /// 实测 `information_schema.COLUMN_TYPE` 的输出逐条钉住（8.0.32）
    #[test]
    fn test_real_column_types() {
        for (decl, want) in [
            ("int", LogicalType::Int32),
            ("tinyint(1)", LogicalType::Int8),
            ("tinyint", LogicalType::Int8),
            ("tinyint unsigned", LogicalType::UInt8),
            ("smallint", LogicalType::Int16),
            ("smallint unsigned", LogicalType::UInt16),
            ("int unsigned", LogicalType::UInt32),
            ("bigint", LogicalType::Int64),
            ("bigint unsigned", LogicalType::UInt64),
            ("float", LogicalType::Float32),
            ("double", LogicalType::Float64),
            ("date", LogicalType::Date32),
            ("char(10)", LogicalType::Utf8),
            ("varchar(200)", LogicalType::Utf8),
            ("text", LogicalType::LargeUtf8),
            ("blob", LogicalType::LargeBinary),
            ("varbinary(100)", LogicalType::LargeBinary),
            ("binary(8)", LogicalType::LargeBinary),
            ("json", LogicalType::Json),
            ("enum('a','b')", LogicalType::Utf8),
            ("set('x','y')", LogicalType::Utf8),
            ("year", LogicalType::Int16),
        ] {
            assert_eq!(a().to_logical_from_str(decl).unwrap(), want, "{}", decl);
        }
        assert_eq!(
            a().to_logical_from_str("decimal(38,10)").unwrap(),
            LogicalType::Decimal { precision: 38, scale: 10 }
        );
    }

    /// 小数位映射：0→秒、1-3→毫秒、4-6→微秒；7+ 报错
    #[test]
    fn test_temporal_fractional_digits() {
        let ts = |p| LogicalType::Timestamp { precision: p, tz: None };
        assert_eq!(a().to_logical_from_str("datetime").unwrap(), ts(TemporalPrecision::Second));
        assert_eq!(a().to_logical_from_str("datetime(3)").unwrap(), ts(TemporalPrecision::Milli));
        assert_eq!(a().to_logical_from_str("datetime(6)").unwrap(), ts(TemporalPrecision::Micro));
        assert_eq!(a().to_logical_from_str("timestamp(6)").unwrap(), ts(TemporalPrecision::Micro));
        assert_eq!(
            a().to_logical_from_str("time(3)").unwrap(),
            LogicalType::Time64(TemporalPrecision::Milli)
        );
        let e = a().to_logical_from_str("datetime(7)").unwrap_err().to_string();
        assert!(e.contains("6 位"), "{}", e);
    }

    /// `UNSIGNED` 是值域的一部分，不能当修饰忽略
    #[test]
    fn test_unsigned_changes_type() {
        assert_eq!(a().to_logical_from_str("int").unwrap(), LogicalType::Int32);
        assert_eq!(a().to_logical_from_str("int unsigned").unwrap(), LogicalType::UInt32);
        assert_ne!(
            a().to_logical_from_str("bigint").unwrap(),
            a().to_logical_from_str("bigint unsigned").unwrap()
        );
        // 写回时的写法也要带 UNSIGNED
        assert_eq!(a().to_db_type(&LogicalType::UInt64).unwrap(), "BIGINT UNSIGNED");
    }

    /// 超过 38 位精度的 DECIMAL 必须拒绝，不能截断
    #[test]
    fn test_decimal_over_38_rejected() {
        let e = a().to_logical_from_str("decimal(65,0)").unwrap_err().to_string();
        assert!(e.contains("38"), "{}", e);
    }

    #[test]
    fn test_spatial_types_rejected() {
        let e = a().to_logical_from_str("geometry").unwrap_err().to_string();
        assert!(e.contains("WKT"), "{}", e);
    }

    #[test]
    fn test_round_trip_scalar_types() {
        for ty in [
            LogicalType::Int8,
            LogicalType::UInt32,
            LogicalType::Decimal { precision: 38, scale: 10 },
            LogicalType::Utf8,
            LogicalType::LargeBinary,
            LogicalType::Date32,
            LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None },
            LogicalType::Time64(TemporalPrecision::Milli),
            LogicalType::Json,
        ] {
            let decl = a().to_db_type(&ty).unwrap();
            assert_eq!(a().to_logical_from_str(&decl).unwrap(), ty, "{}", decl);
        }
    }

    #[test]
    fn test_nano_and_tz_rejected() {
        let e = a()
            .to_db_type(&LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: None,
            })
            .unwrap_err()
            .to_string();
        assert!(e.contains("微秒"), "{}", e);
        let e = a()
            .to_db_type(&LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: Some(d2d_types::FixedOffset::cst()),
            })
            .unwrap_err()
            .to_string();
        assert!(e.contains("时区"), "{}", e);
    }
}
