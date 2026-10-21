//! QSDB 类型适配
//!
//! QSDB 是 Arrow-native 的：读路径拿到的就是 `RecordBatch`，**不做文本解析、不做值转换**。
//! 这个适配器的用途只有一个 —— 启动期的 `source.type_contract` 校验
//! （把配置里写的 QSDB 类型名翻成 D2D 逻辑类型）。
//!
//! 类型词汇表与 QSDB 的 DDL 解析器保持同一套（`BOOLEAN` / `TINYINT` / `BIGINT` /
//! `DECIMAL(p,s)` / `STRING` / `VARCHAR(n)` / `CHAR(n)` / `TIMESTAMP [WITH TIME ZONE]`
//! / `INTERVAL` 等）。这里自带一份最小解析/渲染，不依赖未发布的 `qsdb` crate。

use d2d_core::{D2DError, D2DResult};
use d2d_types::{DbTypeDecl, FixedOffset, LogicalType, TemporalPrecision, TypeAdapter};

/// 解析 `+08:00` / `-05:30` 形式的固定偏移
fn parse_offset(s: &str) -> Option<FixedOffset> {
    let (sign, rest) = match s.as_bytes().first()? {
        b'+' => (1i16, &s[1..]),
        b'-' => (-1i16, &s[1..]),
        _ => return None,
    };
    let (h, m) = rest.split_once(':')?;
    let hours: i16 = h.parse().ok()?;
    let minutes: i16 = m.parse().ok()?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let offset = FixedOffset {
        minutes: sign * (hours * 60 + minutes),
    };
    offset.is_valid().then_some(offset)
}

/// QSDB 的逻辑类型（只覆盖 DDL 能表达的那一套）
#[derive(Debug, Clone, PartialEq, Eq)]
enum QsdbType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: i8 },
    Varchar { max_chars: Option<u32> },
    Char { length: u32 },
    Binary,
    Date,
    Time,
    Timestamp { timezone: Option<String> },
    Interval,
}

impl QsdbType {
    /// 解析 DDL 类型名（大小写不敏感）
    fn from_sql(sql: &str) -> D2DResult<Self> {
        let raw = sql.trim();
        let upper = raw.to_ascii_uppercase();

        // TIMESTAMP [WITH TIME ZONE [(tz)]]
        if upper.starts_with("TIMESTAMP") {
            let rest = raw["TIMESTAMP".len()..].trim();
            if rest.is_empty() {
                return Ok(Self::Timestamp { timezone: None });
            }
            let rest_upper = rest.to_ascii_uppercase();
            if !rest_upper.starts_with("WITH TIME ZONE") {
                return Err(D2DError::schema(format!("无法解析 QSDB 类型 `{raw}`")));
            }
            let tail = rest["WITH TIME ZONE".len()..].trim();
            // QSDB v1 的 `TIMESTAMP WITH TIME ZONE`（不写具体时区）= UTC
            let tz = if tail.is_empty() {
                Some("UTC".to_string())
            } else {
                let inner = tail
                    .strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| D2DError::schema(format!("无法解析 QSDB 类型的时区 `{raw}`")))?;
                Some(inner.trim().to_string())
            };
            return Ok(Self::Timestamp { timezone: tz });
        }

        let (name, params) = match raw.split_once('(') {
            Some((n, rest)) => {
                let p = rest.strip_suffix(')').ok_or_else(|| {
                    D2DError::schema(format!("无法解析 QSDB 类型 `{raw}`：括号未闭合"))
                })?;
                (n.trim().to_ascii_uppercase(), Some(p.trim()))
            }
            None => (upper.clone(), None),
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
                let (p, s) = match params {
                    None => (38, 0),
                    Some(spec) => {
                        let (a, b) = spec.split_once(',').ok_or_else(|| {
                            D2DError::schema(format!(
                                "无法解析 QSDB DECIMAL 精度 `{spec}`，期望 DECIMAL(p, s)"
                            ))
                        })?;
                        let p = a.trim().parse::<u8>().map_err(|_| {
                            D2DError::schema(format!("QSDB DECIMAL 精度 `{a}` 不是整数"))
                        })?;
                        let s = b.trim().parse::<i8>().map_err(|_| {
                            D2DError::schema(format!("QSDB DECIMAL 标度 `{b}` 不是整数"))
                        })?;
                        (p, s)
                    }
                };
                if !(1..=38).contains(&p) {
                    return Err(D2DError::schema(format!(
                        "QSDB DECIMAL 精度 {p} 超出 1..=38"
                    )));
                }
                if s < 0 || i16::from(s) > i16::from(p) {
                    return Err(D2DError::schema(format!(
                        "QSDB DECIMAL 标度 {s} 超出 0..={p}"
                    )));
                }
                Ok(Self::Decimal {
                    precision: p,
                    scale: s,
                })
            }
            "STRING" | "TEXT" => Ok(Self::Varchar { max_chars: None }),
            "VARCHAR" => {
                let n = params
                    .and_then(|p| p.parse::<u32>().ok())
                    .ok_or_else(|| D2DError::schema(format!("QSDB VARCHAR 需要长度：`{raw}`")))?;
                Ok(Self::Varchar { max_chars: Some(n) })
            }
            "CHAR" => {
                let n = params
                    .and_then(|p| p.parse::<u32>().ok())
                    .ok_or_else(|| D2DError::schema(format!("QSDB CHAR 需要长度：`{raw}`")))?;
                Ok(Self::Char { length: n })
            }
            "BINARY" => Ok(Self::Binary),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            "INTERVAL" => Ok(Self::Interval),
            _ => Err(D2DError::schema(format!(
                "QSDB 类型 `{raw}` 无法映射（不认识这个类型名）"
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
            Self::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
            Self::Varchar { max_chars: None } => "STRING".to_string(),
            Self::Varchar { max_chars: Some(n) } => format!("VARCHAR({n})"),
            Self::Char { length } => format!("CHAR({length})"),
            Self::Binary => "BINARY".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Time => "TIME".to_string(),
            Self::Timestamp { timezone: None } => "TIMESTAMP".to_string(),
            Self::Timestamp { timezone: Some(tz) } => format!("TIMESTAMP WITH TIME ZONE ({tz})"),
            Self::Interval => "INTERVAL".to_string(),
        }
    }
}

pub struct QsdbTypeAdapter;

impl QsdbTypeAdapter {
    pub fn new() -> Self {
        Self
    }

    /// QSDB 的时区语义：v1 只认 UTC
    fn tz_of(timezone: &Option<String>) -> D2DResult<Option<FixedOffset>> {
        match timezone {
            None => Ok(None),
            Some(s) if s.eq_ignore_ascii_case("utc") || s == "Z" => Ok(Some(FixedOffset::utc())),
            Some(s) => parse_offset(s).map(Some).ok_or_else(|| {
                D2DError::schema(format!(
                    "QSDB 时间戳时区 `{s}` 无法映射成固定偏移（QSDB v1 只支持 UTC）"
                ))
            }),
        }
    }
}

impl Default for QsdbTypeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeAdapter for QsdbTypeAdapter {
    fn db_name(&self) -> &str {
        "qsdb"
    }

    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType> {
        let rendered = decl.render();
        let q = QsdbType::from_sql(&rendered)?;
        let ty = match q {
            QsdbType::Boolean => LogicalType::Boolean,
            QsdbType::Int8 => LogicalType::Int8,
            QsdbType::Int16 => LogicalType::Int16,
            QsdbType::Int32 => LogicalType::Int32,
            QsdbType::Int64 => LogicalType::Int64,
            QsdbType::Float32 => LogicalType::Float32,
            QsdbType::Float64 => LogicalType::Float64,
            QsdbType::Decimal { precision, scale } => LogicalType::Decimal { precision, scale },
            // QSDB 的 VARCHAR(n)/CHAR(n) 长度语义只存在于逻辑层，物理都是 Utf8
            QsdbType::Varchar { .. } | QsdbType::Char { .. } => LogicalType::Utf8,
            QsdbType::Binary => LogicalType::Binary,
            QsdbType::Date => LogicalType::Date32,
            // QSDB 内部固定 i64 纳秒
            QsdbType::Time => LogicalType::Time64(TemporalPrecision::Nano),
            QsdbType::Timestamp { timezone } => LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: Self::tz_of(&timezone)?,
            },
            QsdbType::Interval => LogicalType::Interval,
        };
        ty.validate()?;
        Ok(ty)
    }

    fn to_db_type(&self, ty: &LogicalType) -> D2DResult<String> {
        let q = match ty {
            LogicalType::Boolean => QsdbType::Boolean,
            LogicalType::Int8 => QsdbType::Int8,
            LogicalType::Int16 => QsdbType::Int16,
            LogicalType::Int32 => QsdbType::Int32,
            LogicalType::Int64 => QsdbType::Int64,
            // QSDB 没有无符号类型：按值域不丢的最宽有符号类型承载
            LogicalType::UInt8 => QsdbType::Int16,
            LogicalType::UInt16 => QsdbType::Int32,
            LogicalType::UInt32 => QsdbType::Int64,
            LogicalType::UInt64 => QsdbType::Decimal {
                precision: 20,
                scale: 0,
            },
            LogicalType::Float32 => QsdbType::Float32,
            LogicalType::Float64 => QsdbType::Float64,
            LogicalType::Decimal { precision, scale } => {
                if !(1..=38).contains(precision) {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({precision},{scale}) 超出 QSDB 的 1..=38"
                    )));
                }
                if *scale < 0 || i16::from(*scale) > i16::from(*precision) {
                    return Err(D2DError::schema(format!(
                        "DECIMAL({precision},{scale}) 的标度超出 0..={precision}"
                    )));
                }
                QsdbType::Decimal {
                    precision: *precision,
                    scale: *scale,
                }
            }
            LogicalType::Utf8 | LogicalType::LargeUtf8 => QsdbType::Varchar { max_chars: None },
            LogicalType::Binary | LogicalType::LargeBinary => QsdbType::Binary,
            LogicalType::Date32 => QsdbType::Date,
            LogicalType::Time64(_) => QsdbType::Time,
            LogicalType::Timestamp { tz, .. } => match tz {
                None => QsdbType::Timestamp { timezone: None },
                Some(off) if off.minutes == 0 => QsdbType::Timestamp {
                    timezone: Some("UTC".to_string()),
                },
                Some(off) => {
                    return Err(D2DError::schema(format!(
                        "QSDB v1 的时间戳只支持 UTC，收到偏移 {off}"
                    )))
                }
            },
            LogicalType::Interval => QsdbType::Interval,
            other => {
                return Err(D2DError::schema(format!(
                    "QSDB 没有与 `{}` 对应的类型（拒绝静默降级）",
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

    fn to_logical(s: &str) -> D2DResult<LogicalType> {
        QsdbTypeAdapter::new().to_logical(&DbTypeDecl::parse(s))
    }

    #[test]
    fn test_to_logical() {
        assert_eq!(to_logical("BIGINT").unwrap(), LogicalType::Int64);
        assert_eq!(to_logical("STRING").unwrap(), LogicalType::Utf8);
        assert_eq!(
            to_logical("DECIMAL(18,4)").unwrap(),
            LogicalType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(
            to_logical("TIMESTAMP").unwrap(),
            LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: None
            }
        );
        assert_eq!(
            to_logical("TIMESTAMP WITH TIME ZONE").unwrap(),
            LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: Some(FixedOffset::utc())
            }
        );
        assert_eq!(
            to_logical("TIME").unwrap(),
            LogicalType::Time64(TemporalPrecision::Nano)
        );
    }

    #[test]
    fn test_unknown_type_is_error() {
        let e = to_logical("JSONB").unwrap_err().to_string();
        assert!(e.contains("JSONB"), "{}", e);
    }

    #[test]
    fn test_to_db_type() {
        let a = QsdbTypeAdapter::new();
        assert_eq!(a.to_db_type(&LogicalType::Int32).unwrap(), "INT");
        assert_eq!(
            a.to_db_type(&LogicalType::Decimal {
                precision: 9,
                scale: 2
            })
            .unwrap(),
            "DECIMAL(9, 2)"
        );
        assert_eq!(a.to_db_type(&LogicalType::Utf8).unwrap(), "STRING");
        assert!(a.to_db_type(&LogicalType::Json).is_err());
    }

    #[test]
    fn test_non_utc_timestamp_is_error() {
        let a = QsdbTypeAdapter::new();
        let off = FixedOffset::from_hours(8);
        assert!(a
            .to_db_type(&LogicalType::Timestamp {
                precision: TemporalPrecision::Nano,
                tz: Some(off)
            })
            .is_err());
    }
}
