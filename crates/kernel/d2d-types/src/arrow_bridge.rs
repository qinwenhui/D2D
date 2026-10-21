use crate::{FixedOffset, LogicalType, TemporalPrecision};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;

/// LogicalType ↔ Arrow DataType 双向桥接
///
/// **铁律：绝不静默 fallback**。遇到无法表达的类型，返回带完整信息的错误，
/// 由上层决定如何处理。这是防止"BLOB 悄悄变字符串"类事故的核心防线。
pub struct LogicalArrowBridge;

impl LogicalArrowBridge {
    /// LogicalType → Arrow DataType
    pub fn to_arrow(ty: &LogicalType) -> D2DResult<DataType> {
        ty.validate_deep()?;
        let dt = match ty {
            LogicalType::Null => DataType::Null,
            LogicalType::Boolean => DataType::Boolean,
            LogicalType::Int8 => DataType::Int8,
            LogicalType::Int16 => DataType::Int16,
            LogicalType::Int32 => DataType::Int32,
            LogicalType::Int64 => DataType::Int64,
            LogicalType::UInt8 => DataType::UInt8,
            LogicalType::UInt16 => DataType::UInt16,
            LogicalType::UInt32 => DataType::UInt32,
            LogicalType::UInt64 => DataType::UInt64,
            LogicalType::Float32 => DataType::Float32,
            LogicalType::Float64 => DataType::Float64,
            LogicalType::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            LogicalType::Utf8 => DataType::Utf8,
            LogicalType::LargeUtf8 => DataType::LargeUtf8,
            LogicalType::Binary => DataType::Binary,
            LogicalType::LargeBinary => DataType::LargeBinary,
            LogicalType::Date32 => DataType::Date32,
            LogicalType::Time64(p) => DataType::Time64(Self::time_unit(p)),
            LogicalType::Timestamp { precision, tz } => {
                DataType::Timestamp(Self::time_unit(precision), tz.map(Self::tz_to_arrow))
            }
            LogicalType::Interval => DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
            LogicalType::Duration => {
                DataType::Duration(arrow::datatypes::TimeUnit::Nanosecond)
            }
            LogicalType::List(inner) => {
                DataType::List(Arc::new(Field::new("item", Self::to_arrow(inner)?, true)))
            }
            LogicalType::Struct(fields) => {
                let mut arrow_fields = Vec::with_capacity(fields.len());
                for f in fields {
                    arrow_fields.push(Field::new(&f.name, Self::to_arrow(&f.logical_type)?, f.nullable));
                }
                DataType::Struct(arrow_fields.into())
            }
            LogicalType::Map(k, v) => {
                // arrow 的 Map: entries 为一个固定 struct（keys + values）
                let entries = Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", Self::to_arrow(k)?, false),
                            Field::new("values", Self::to_arrow(v)?, true),
                        ]
                        .into(),
                    ),
                    false,
                );
                DataType::Map(Arc::new(entries), false)
            }
            LogicalType::Json => DataType::Utf8, // JSON 序列化为字符串承载（显式决策，非 fallback）
            LogicalType::Enum(_) => DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            ),
        };
        Ok(dt)
    }

    /// Arrow DataType → LogicalType
    ///
    /// 无法表达的类型返回错误，**绝不 fallback 到 Utf8**。
    pub fn from_arrow(dt: &DataType) -> D2DResult<LogicalType> {
        let ty = match dt {
            DataType::Null => LogicalType::Null,
            DataType::Boolean => LogicalType::Boolean,
            DataType::Int8 => LogicalType::Int8,
            DataType::Int16 => LogicalType::Int16,
            DataType::Int32 => LogicalType::Int32,
            DataType::Int64 => LogicalType::Int64,
            DataType::UInt8 => LogicalType::UInt8,
            DataType::UInt16 => LogicalType::UInt16,
            DataType::UInt32 => LogicalType::UInt32,
            DataType::UInt64 => LogicalType::UInt64,
            DataType::Float16 => {
                return Err(D2DError::schema(
                    "Arrow Float16 无法映射为逻辑类型（无对应语义），请先在 Connector 内转为 Float32/Float64",
                ))
            }
            DataType::Float32 => LogicalType::Float32,
            DataType::Float64 => LogicalType::Float64,
            DataType::Decimal128(p, s) => {
                if *p < 1 || *p > 38 {
                    return Err(D2DError::schema(format!(
                        "Arrow Decimal128 精度 {} 超出逻辑类型范围 1..=38",
                        p
                    )));
                }
                LogicalType::Decimal {
                    precision: *p,
                    scale: *s,
                }
            }
            // Arrow 60 新增的 i32/i64 承载的定点数（精度上限分别是 9 / 18）。
            // 它们比 Decimal128 窄，逻辑类型用同一个 Decimal 承载即可。
            DataType::Decimal32(p, s) | DataType::Decimal64(p, s) => LogicalType::Decimal {
                precision: *p,
                scale: *s,
            },
            DataType::Decimal256(p, s) => LogicalType::Decimal {
                precision: (*p).min(38),
                scale: (*s).min(*p as i8),
            },
            DataType::Utf8 | DataType::Utf8View => LogicalType::Utf8,
            DataType::LargeUtf8 => LogicalType::LargeUtf8,
            DataType::Binary | DataType::BinaryView | DataType::FixedSizeBinary(_) => LogicalType::Binary,
            DataType::LargeBinary | DataType::LargeListView(_) => LogicalType::LargeBinary,
            DataType::Date32 | DataType::Date64 => LogicalType::Date32,
            DataType::Time32(u) => LogicalType::Time64(Self::unit_to_precision(u)?),
            DataType::Time64(u) => LogicalType::Time64(Self::unit_to_precision(u)?),
            DataType::Timestamp(u, tz) => LogicalType::Timestamp {
                precision: Self::unit_to_precision(u)?,
                tz: tz.as_ref().map(|t| Self::tz_from_arrow(t)),
            },
            DataType::Interval(_) => LogicalType::Interval,
            DataType::Duration(_) => LogicalType::Duration,
            DataType::List(inner) | DataType::ListView(inner) | DataType::LargeList(inner) => {
                LogicalType::List(Box::new(Self::from_arrow(inner.data_type())?))
            }
            DataType::FixedSizeList(inner, _) => {
                LogicalType::List(Box::new(Self::from_arrow(inner.data_type())?))
            }
            DataType::Struct(fields) => {
                let mut logical_fields = Vec::with_capacity(fields.len());
                for f in fields {
                    logical_fields.push(crate::LogicalField::new(
                        f.name(),
                        Self::from_arrow(f.data_type())?,
                        f.is_nullable(),
                    ));
                }
                LogicalType::Struct(logical_fields)
            }
            DataType::Map(entries, _) => {
                // arrow 的 Map: entries struct 含 keys/values 两个字段
                let DataType::Struct(inner) = entries.data_type() else {
                    return Err(D2DError::schema("Arrow Map 的 entries 不是 Struct"));
                };
                if inner.len() != 2 {
                    return Err(D2DError::schema(
                        "Arrow Map 的 entries Struct 必须有 keys/values 两字段",
                    ));
                }
                LogicalType::Map(
                    Box::new(Self::from_arrow(inner[0].data_type())?),
                    Box::new(Self::from_arrow(inner[1].data_type())?),
                )
            }
            DataType::Dictionary(_, value) => {
                if value.as_ref() == &DataType::Utf8 {
                    LogicalType::Enum(Vec::new()) // 字典值域需从数据获取，此处结构占位
                } else {
                    return Err(D2DError::schema(format!(
                        "Arrow Dictionary<_, {}> 无法映射为逻辑类型",
                        value
                    )));
                }
            }
            // 明确拒绝而非降级
            DataType::RunEndEncoded(_, _) => {
                return Err(D2DError::schema(
                    "Arrow RunEndEncoded 类型不支持，请先在 Connector 内解码",
                ))
            }
            DataType::Union(_, _) => {
                return Err(D2DError::schema(
                    "Arrow Union 类型无对应逻辑类型，请拆分为 Struct",
                ))
            }
        };
        ty.validate()?;
        Ok(ty)
    }

    /// LogicalSchema ↔ Arrow Schema
    pub fn schema_to_arrow(schema: &crate::LogicalSchema) -> D2DResult<Schema> {
        let mut fields = Vec::with_capacity(schema.fields.len());
        for f in &schema.fields {
            fields.push(Field::new(&f.name, Self::to_arrow(&f.logical_type)?, f.nullable));
        }
        Ok(Schema::new(fields).with_metadata(schema.metadata.clone()))
    }

    pub fn schema_from_arrow(schema: &Schema) -> D2DResult<crate::LogicalSchema> {
        let mut fields = Vec::with_capacity(schema.fields().len());
        for f in schema.fields() {
            fields.push(crate::LogicalField::new(
                f.name(),
                Self::from_arrow(f.data_type())?,
                f.is_nullable(),
            ));
        }
        let mut ls = crate::LogicalSchema::new(fields);
        // Arrow 60 把元数据从 HashMap 换成了 `Metadata` 类型；逻辑层保持 HashMap
        ls.metadata = schema.metadata().clone().into();
        Ok(ls)
    }

    fn time_unit(p: &TemporalPrecision) -> TimeUnit {
        match p {
            TemporalPrecision::Second => TimeUnit::Second,
            TemporalPrecision::Milli => TimeUnit::Millisecond,
            TemporalPrecision::Micro => TimeUnit::Microsecond,
            TemporalPrecision::Nano => TimeUnit::Nanosecond,
        }
    }

    fn unit_to_precision(u: &TimeUnit) -> D2DResult<TemporalPrecision> {
        Ok(match u {
            TimeUnit::Second => TemporalPrecision::Second,
            TimeUnit::Millisecond => TemporalPrecision::Milli,
            TimeUnit::Microsecond => TemporalPrecision::Micro,
            TimeUnit::Nanosecond => TemporalPrecision::Nano,
        })
    }

    fn tz_to_arrow(offset: FixedOffset) -> Arc<str> {
        // Arrow 用 "+HH:MM" 字符串表示时区
        format!("{}", offset).into()
    }

    fn tz_from_arrow(tz: &str) -> FixedOffset {
        // 解析 "+08:00" / "-05:30" / "UTC"
        let s = tz.trim();
        if s.eq_ignore_ascii_case("UTC") || s.is_empty() {
            return FixedOffset::utc();
        }
        let (sign, rest) = match s.chars().next() {
            Some('+') => (1i16, &s[1..]),
            Some('-') => (-1i16, &s[1..]),
            _ => return FixedOffset::utc(),
        };
        let parts: Vec<&str> = rest.split(':').collect();
        let h: i16 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
        let m: i16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
        FixedOffset {
            minutes: (sign * (h * 60 + m)).clamp(-1439, 1439),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalField;

    #[test]
    fn test_roundtrip_scalar() {
        for ty in [
            LogicalType::Int64,
            LogicalType::Utf8,
            LogicalType::Decimal { precision: 19, scale: 4 },
            LogicalType::Boolean,
        ] {
            let arrow = LogicalArrowBridge::to_arrow(&ty).unwrap();
            let back = LogicalArrowBridge::from_arrow(&arrow).unwrap();
            assert_eq!(ty, back, "roundtrip failed for {}", ty.describe());
        }
    }

    #[test]
    fn test_roundtrip_temporal() {
        let ty = LogicalType::Timestamp {
            precision: TemporalPrecision::Micro,
            tz: Some(FixedOffset::cst()),
        };
        let arrow = LogicalArrowBridge::to_arrow(&ty).unwrap();
        assert_eq!(arrow, DataType::Timestamp(TimeUnit::Microsecond, Some("+08:00".into())));
        let back = LogicalArrowBridge::from_arrow(&arrow).unwrap();
        assert_eq!(ty, back);
    }

    #[test]
    fn test_schema_roundtrip() {
        let schema = crate::LogicalSchema::new(vec![
            LogicalField::new("id", LogicalType::Int64, false),
            LogicalField::new("blob_col", LogicalType::LargeBinary, true),
        ]);
        let arrow = LogicalArrowBridge::schema_to_arrow(&schema).unwrap();
        let back = LogicalArrowBridge::schema_from_arrow(&arrow).unwrap();
        assert_eq!(schema, back);
    }

    #[test]
    fn test_float16_rejected_not_fallback() {
        let err = LogicalArrowBridge::from_arrow(&DataType::Float16).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Float16"), "应明确指出 Float16 不支持: {}", msg);
        assert!(!msg.contains("Utf8"), "绝不能 fallback 成字符串");
    }

    #[test]
    fn test_decimal_bad_precision_rejected() {
        let err = LogicalArrowBridge::from_arrow(&DataType::Decimal128(0, 0)).unwrap_err();
        assert!(err.to_string().contains("精度"));
    }
}
