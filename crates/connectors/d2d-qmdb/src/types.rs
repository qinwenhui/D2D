//! qmdb 类型适配
//!
//! 读路径本身是 Arrow-native 的（Flight SQL 直接给 `RecordBatch`，无类型重建），
//! 这个适配器的用途只有一个 —— 启动期的 `source.type_contract` 校验：
//! 把配置里写的 qmdb 类型名翻成 D2D 逻辑类型，以及反向给出 DDL 建议。
//!
//! 类型词汇表与 qmdb 官方文档 §9 对齐（`BOOLEAN` / `TINYINT`~`BIGINT` /
//! `FLOAT` / `DOUBLE` / `DECIMAL(p,s)` / `VARCHAR(n)` / `BINARY` / `DATE` /
//! `TIMESTAMP`）。未知类型拒绝映射，**绝不 fallback 到 Utf8**。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, LogicalType, TemporalPrecision, TypeAdapter};

/// qmdb 的逻辑类型（只覆盖 DDL 能表达的那一套）
#[derive(Debug, Clone, PartialEq, Eq)]
enum QmdbType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Varchar,
    Binary,
    Date,
    /// 时间戳；服务端统一归一到微秒
    Timestamp {
        precision: TemporalPrecision,
    },
}

impl QmdbType {
    /// 解析 DDL 类型名（大小写不敏感）
    fn from_sql(sql: &str) -> D2DResult<Self> {
        let raw = sql.trim();
        let upper = raw.to_ascii_uppercase();

        // WITH TIME ZONE / WITHOUT TIME ZONE 都不在 qmdb 的词汇表里：
        // 它的时间戳就是微秒整数，没有时区语义
        if upper.contains("WITH TIME ZONE") || upper.contains("WITHOUT TIME ZONE") {
            return Err(D2DError::schema(format!(
                "qmdb 的 TIMESTAMP 没有时区语义（服务端统一存微秒），\
                 不接受 `{raw}` —— 带时区的时间请用绝对时刻写入 TIMESTAMP"
            )));
        }

        let (name, precision, scale) = match raw.find('(') {
            Some(start) => {
                let end = raw.find(')').ok_or_else(|| {
                    D2DError::schema(format!("无法解析 qmdb 类型 `{raw}`：括号未闭合"))
                })?;
                let base = raw[..start].trim().to_ascii_uppercase();
                let params: Vec<&str> = raw[start + 1..end]
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect();
                match params.as_slice() {
                    [p, s] => (
                        base,
                        Some(p.parse::<u32>().map_err(|_| {
                            D2DError::schema(format!("qmdb 类型 `{raw}` 的精度 `{p}` 不是整数"))
                        })?),
                        Some(s.parse::<i32>().map_err(|_| {
                            D2DError::schema(format!("qmdb 类型 `{raw}` 的标度 `{s}` 不是整数"))
                        })?),
                    ),
                    [p] => (
                        base,
                        Some(p.parse::<u32>().map_err(|_| {
                            D2DError::schema(format!("qmdb 类型 `{raw}` 的精度 `{p}` 不是整数"))
                        })?),
                        None,
                    ),
                    _ => {
                        return Err(D2DError::schema(format!(
                            "无法解析 qmdb 类型 `{raw}`：参数最多两个 (p, s)"
                        )))
                    }
                }
            }
            None => (upper, None, None),
        };

        match name.as_str() {
            "BOOLEAN" | "BOOL" => Ok(Self::Boolean),
            "TINYINT" | "INT8" => Ok(Self::Int8),
            "SMALLINT" | "INT16" => Ok(Self::Int16),
            "INT" | "INTEGER" | "INT32" => Ok(Self::Int32),
            "BIGINT" | "INT64" => Ok(Self::Int64),
            "FLOAT" | "REAL" | "FLOAT32" => Ok(Self::Float32),
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT64" => Ok(Self::Float64),
            "DECIMAL" | "NUMERIC" => {
                let (p, s) = match (precision, scale) {
                    (Some(p), Some(s)) => (p, s),
                    (Some(p), None) => (p, 0),
                    (None, _) => (38, 0),
                };
                let p = u8::try_from(p)
                    .map_err(|_| D2DError::schema(format!("qmdb DECIMAL 精度 {p} 超出 u8")))?;
                let s = i8::try_from(s)
                    .map_err(|_| D2DError::schema(format!("qmdb DECIMAL 标度 {s} 超出 i8")))?;
                Ok(Self::Decimal {
                    precision: p,
                    scale: s,
                })
            }
            "VARCHAR" | "STRING" | "TEXT" => Ok(Self::Varchar),
            "BINARY" | "VARBINARY" | "BYTEA" => Ok(Self::Binary),
            "DATE" => Ok(Self::Date),
            "TIMESTAMP" | "DATETIME" => Ok(Self::Timestamp {
                precision: match precision {
                    None => TemporalPrecision::Micro,
                    // qmdb 统一归一到微秒：声明别的精度也按微秒承载
                    Some(_) => TemporalPrecision::Micro,
                },
            }),
            other => Err(D2DError::schema(format!(
                "qmdb 类型 `{other}` 无法映射（不认识这个类型名，词汇表见 qmdb 文档 §9）"
            ))),
        }
    }

    fn to_sql(&self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Int8 => "TINYINT".to_string(),
            Self::Int16 => "SMALLINT".to_string(),
            Self::Int32 => "INT".to_string(),
            Self::Int64 => "BIGINT".to_string(),
            Self::Float32 => "FLOAT".to_string(),
            Self::Float64 => "DOUBLE".to_string(),
            Self::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
            Self::Varchar => "VARCHAR".to_string(),
            Self::Binary => "BINARY".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Timestamp { .. } => "TIMESTAMP".to_string(),
        }
    }
}

pub struct QmdbTypeAdapter;

impl QmdbTypeAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QmdbTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for QmdbTypeAdapter {
    fn db_name(&self) -> &str {
        "qmdb"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        let rendered = decl.render();
        let q = QmdbType::from_sql(&rendered)?;
        let ty = match q {
            QmdbType::Boolean => LogicalType::Boolean,
            QmdbType::Int8 => LogicalType::Int8,
            QmdbType::Int16 => LogicalType::Int16,
            QmdbType::Int32 => LogicalType::Int32,
            QmdbType::Int64 => LogicalType::Int64,
            QmdbType::Float32 => LogicalType::Float32,
            QmdbType::Float64 => LogicalType::Float64,
            QmdbType::Decimal { precision, scale } => LogicalType::Decimal { precision, scale },
            QmdbType::Varchar => LogicalType::Utf8,
            QmdbType::Binary => LogicalType::Binary,
            QmdbType::Date => LogicalType::Date32,
            // qmdb 服务端统一归一到微秒（秒×1e6、纳秒÷1000）
            QmdbType::Timestamp { precision } => LogicalType::Timestamp {
                precision,
                tz: None,
            },
        };
        ty.validate()?;
        Ok(ty)
    }

    fn to_db_type(&self, ty: &LogicalType) -> D2DResult<String> {
        let q = match ty {
            LogicalType::Boolean => QmdbType::Boolean,
            LogicalType::Int8 => QmdbType::Int8,
            LogicalType::Int16 => QmdbType::Int16,
            LogicalType::Int32 => QmdbType::Int32,
            LogicalType::Int64 => QmdbType::Int64,
            // qmdb 没有无符号类型：按值域不丢的最宽有符号类型承载
            LogicalType::UInt8 => QmdbType::Int16,
            LogicalType::UInt16 => QmdbType::Int32,
            LogicalType::UInt32 => QmdbType::Int64,
            LogicalType::UInt64 => QmdbType::Decimal {
                precision: 20,
                scale: 0,
            },
            LogicalType::Float32 => QmdbType::Float32,
            LogicalType::Float64 => QmdbType::Float64,
            LogicalType::Decimal { precision, scale } => {
                if !(1..=38).contains(precision) {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({precision},{scale}) 超出 qmdb 的 1..=38"
                    )));
                }
                if *scale < 0 || i16::from(*scale) > i16::from(*precision) {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({precision},{scale}) 的标度超出 0..={precision}"
                    )));
                }
                QmdbType::Decimal {
                    precision: *precision,
                    scale: *scale,
                }
            }
            LogicalType::Utf8 | LogicalType::LargeUtf8 => QmdbType::Varchar,
            LogicalType::Binary | LogicalType::LargeBinary => QmdbType::Binary,
            LogicalType::Date32 => QmdbType::Date,
            // qmdb 服务端统一归一到微秒，任何精度声明都无损承载
            LogicalType::Timestamp { tz, .. } => {
                // qmdb 的 TIMESTAMP 无时区语义；UTC 偏移按"存 UTC 墙钟时间"约定承载
                if let Some(off) = tz {
                    if off.minutes != 0 {
                        return Err(D2DError::schema(format!(
                            "qmdb 的 TIMESTAMP 没有时区语义，收到偏移 {off}；\
                             请先转换成 UTC（或本地时间）再写入"
                        )));
                    }
                }
                QmdbType::Timestamp {
                    precision: TemporalPrecision::Micro,
                }
            }
            other => {
                return Err(D2DError::schema(format!(
                    "qmdb 没有与 `{}` 对应的类型（拒绝静默降级）",
                    other.describe()
                )))
            }
        };
        Ok(q.to_sql())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_types::FixedOffset;

    fn to_logical(s: &str) -> D2DResult<LogicalType> {
        QmdbTypeAdapter::new().to_logical(&DbTypeDecl::parse(s))
    }

    #[test]
    fn test_to_logical_full_vocabulary() {
        assert_eq!(to_logical("BOOLEAN").unwrap(), LogicalType::Boolean);
        assert_eq!(to_logical("TINYINT").unwrap(), LogicalType::Int8);
        assert_eq!(to_logical("SMALLINT").unwrap(), LogicalType::Int16);
        assert_eq!(to_logical("INT").unwrap(), LogicalType::Int32);
        assert_eq!(to_logical("BIGINT").unwrap(), LogicalType::Int64);
        assert_eq!(to_logical("FLOAT").unwrap(), LogicalType::Float32);
        assert_eq!(to_logical("DOUBLE").unwrap(), LogicalType::Float64);
        assert_eq!(
            to_logical("DECIMAL(18,4)").unwrap(),
            LogicalType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(to_logical("VARCHAR(64)").unwrap(), LogicalType::Utf8);
        assert_eq!(to_logical("VARCHAR").unwrap(), LogicalType::Utf8);
        assert_eq!(to_logical("BINARY").unwrap(), LogicalType::Binary);
        assert_eq!(to_logical("DATE").unwrap(), LogicalType::Date32);
        assert_eq!(
            to_logical("TIMESTAMP").unwrap(),
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None
            }
        );
        // 大小写不敏感
        assert_eq!(to_logical("bigint").unwrap(), LogicalType::Int64);
    }

    #[test]
    fn test_unknown_type_is_error() {
        let e = to_logical("JSONB").unwrap_err().to_string();
        assert!(e.contains("JSONB"), "{}", e);
        let e = to_logical("INTERVAL DAY").unwrap_err().to_string();
        assert!(e.contains("无法映射"), "{}", e);
    }

    /// qmdb 的 TIMESTAMP 没有时区语义 —— WITH TIME ZONE 直接拒绝
    #[test]
    fn test_timestamp_with_time_zone_is_rejected() {
        let e = to_logical("TIMESTAMP WITH TIME ZONE")
            .unwrap_err()
            .to_string();
        assert!(e.contains("时区"), "{}", e);
    }

    #[test]
    fn test_to_db_type() {
        let a = QmdbTypeAdapter::new();
        assert_eq!(a.to_db_type(&LogicalType::Int32).unwrap(), "INT");
        assert_eq!(a.to_db_type(&LogicalType::Int8).unwrap(), "TINYINT");
        // 无符号按值域不丢的最宽有符号承载
        assert_eq!(a.to_db_type(&LogicalType::UInt8).unwrap(), "SMALLINT");
        assert_eq!(a.to_db_type(&LogicalType::UInt32).unwrap(), "BIGINT");
        assert_eq!(a.to_db_type(&LogicalType::UInt64).unwrap(), "DECIMAL(20,0)");
        assert_eq!(
            a.to_db_type(&LogicalType::Decimal {
                precision: 9,
                scale: 2
            })
            .unwrap(),
            "DECIMAL(9,2)"
        );
        assert_eq!(a.to_db_type(&LogicalType::Utf8).unwrap(), "VARCHAR");
        assert_eq!(a.to_db_type(&LogicalType::Date32).unwrap(), "DATE");
    }

    #[test]
    fn test_to_db_type_rejects_out_of_range_decimal() {
        let a = QmdbTypeAdapter::new();
        let e = a
            .to_db_type(&LogicalType::Decimal {
                precision: 39,
                scale: 0,
            })
            .unwrap_err()
            .to_string();
        assert!(e.contains("1..=38"), "{}", e);
    }

    /// 带时区（非 UTC）的时间戳拒绝：qmdb 存的是无时区微秒
    #[test]
    fn test_non_utc_timestamp_is_error() {
        let a = QmdbTypeAdapter::new();
        let off = FixedOffset::from_hours(8);
        assert!(a
            .to_db_type(&LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: Some(off)
            })
            .is_err());
    }

    /// JSON 等没有对应物的类型拒绝降级
    #[test]
    fn test_json_is_rejected() {
        let a = QmdbTypeAdapter::new();
        assert!(a.to_db_type(&LogicalType::Json).is_err());
    }
}
