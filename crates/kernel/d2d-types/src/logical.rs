use d2d_core::D2DResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// D2D 规范逻辑类型（内部唯一权威类型）
///
/// 设计原则：
/// 1. **与 Arrow / 各数据库类型解耦** —— 所有 Connector 通过适配层转换到本类型
/// 2. **语义完整** —— 时间类型必须带显式精度与时区语义，禁止隐式约定
/// 3. **可溯源** —— 字段携带原始数据库类型声明，出问题可追溯
/// 4. **无静默降级** —— 遇到无法表达的类型，适配层必须报错而非 fallback
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalType {
    /// 全空列（占位列）
    Null,
    Boolean,
    /// 定长有符号整数
    Int8,
    Int16,
    Int32,
    Int64,
    /// 定长无符号整数
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// IEEE 浮点
    Float32,
    Float64,
    /// 十进制定点数。precision 1..=38，scale 0..=precision
    Decimal { precision: u8, scale: i8 },
    /// 字符串（UTF-8）
    Utf8,
    /// 大字符串（>2GB 场景）
    LargeUtf8,
    /// 二进制
    Binary,
    /// 大二进制（BLOB 映射目标）
    LargeBinary,
    /// 日历日（无时刻、无时区）
    Date32,
    /// 日内的时刻（无日期、无时区）
    Time64(TemporalPrecision),
    /// 时间戳。`tz = None` 表示"本地时间无时区语义"（达梦 TIMESTAMP / Doris DATETIME）
    /// `tz = Some(offset)` 表示带固定偏移的绝对时刻
    Timestamp {
        precision: TemporalPrecision,
        tz: Option<FixedOffset>,
    },
    /// 时间区间（如 INTERVAL DAY TO SECOND）
    Interval,
    /// 时长（纳秒计）
    Duration,
    /// 列表
    List(Box<LogicalType>),
    /// 结构体
    Struct(Vec<LogicalField>),
    /// 键值映射
    Map(Box<LogicalType>, Box<LogicalType>),
    /// 半结构化 JSON
    Json,
    /// 枚举
    Enum(Vec<String>),
}

/// 时间精度（显式化，杜绝"默认微妙"这类隐式约定）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemporalPrecision {
    Second,
    Milli,
    Micro,
    Nano,
}

impl TemporalPrecision {
    pub fn as_str(&self) -> &'static str {
        match self {
            TemporalPrecision::Second => "second",
            TemporalPrecision::Milli => "milli",
            TemporalPrecision::Micro => "micro",
            TemporalPrecision::Nano => "nano",
        }
    }
}

/// 固定时区偏移（分钟）。+480 = UTC+8（北京时间）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedOffset {
    /// 相对 UTC 的分钟偏移，范围 -1439..=1439
    pub minutes: i16,
}

impl FixedOffset {
    pub fn utc() -> Self {
        Self { minutes: 0 }
    }

    pub fn from_hours(hours: i8) -> Self {
        Self {
            minutes: (hours as i16) * 60,
        }
    }

    /// 中国标准时间 UTC+8
    pub fn cst() -> Self {
        Self::from_hours(8)
    }

    pub fn is_valid(&self) -> bool {
        self.minutes >= -1439 && self.minutes <= 1439
    }
}

impl std::fmt::Display for FixedOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sign = if self.minutes < 0 { '-' } else { '+' };
        let abs = self.minutes.abs();
        write!(f, "{}{:02}:{:02}", sign, abs / 60, abs % 60)
    }
}

/// 类型溯源：记录 field 的原始数据库类型声明
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeOrigin {
    /// 来源数据库标识，如 "dameng" / "doris"
    pub db: String,
    /// 原始类型声明，如 "NUMBER(38,10)" / "TIMESTAMP"
    pub db_type: String,
}

impl TypeOrigin {
    pub fn new(db: impl Into<String>, db_type: impl Into<String>) -> Self {
        Self {
            db: db.into(),
            db_type: db_type.into(),
        }
    }
}

/// 逻辑字段
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    pub name: String,
    pub logical_type: LogicalType,
    /// 是否可空
    pub nullable: bool,
    /// 原始数据库类型溯源（适配层填充；引擎派生的字段为 None）
    pub origin: Option<TypeOrigin>,
}

impl LogicalField {
    pub fn new(name: impl Into<String>, logical_type: LogicalType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullable,
            origin: None,
        }
    }

    /// 带溯源构造
    pub fn with_origin(
        name: impl Into<String>,
        logical_type: LogicalType,
        nullable: bool,
        origin: TypeOrigin,
    ) -> Self {
        Self {
            name: name.into(),
            logical_type,
            nullable,
            origin: Some(origin),
        }
    }
}

/// 逻辑 Schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LogicalSchema {
    pub fields: Vec<LogicalField>,
    /// 元数据：主键、表名等
    pub metadata: HashMap<String, String>,
}

impl LogicalSchema {
    pub fn new(fields: Vec<LogicalField>) -> Self {
        Self {
            fields,
            metadata: HashMap::new(),
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn field(&self, name: &str) -> Option<&LogicalField> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// 找出 other 中缺失的字段名（用于契约校验）
    pub fn missing_columns<'a>(&self, required: &'a [String]) -> Vec<&'a str> {
        required
            .iter()
            .filter(|c| self.field(c).is_none())
            .map(|c| c.as_str())
            .collect()
    }

    pub fn primary_keys(&self) -> Vec<String> {
        self.metadata
            .get("primary_keys")
            .map(|s| s.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default()
    }
}

impl LogicalType {
    /// 是否为大对象（BLOB/CLOB）——sink 需要特殊处理策略
    pub fn is_lob(&self) -> bool {
        matches!(
            self,
            LogicalType::Binary | LogicalType::LargeBinary | LogicalType::Utf8 | LogicalType::LargeUtf8
        )
    }

    /// 是否为二进制类型
    pub fn is_binary(&self) -> bool {
        matches!(self, LogicalType::Binary | LogicalType::LargeBinary)
    }

    /// 是否为时间类型
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            LogicalType::Date32
                | LogicalType::Time64(_)
                | LogicalType::Timestamp { .. }
                | LogicalType::Interval
                | LogicalType::Duration
        )
    }

    /// 是否为数值类型
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Int8
                | LogicalType::Int16
                | LogicalType::Int32
                | LogicalType::Int64
                | LogicalType::UInt8
                | LogicalType::UInt16
                | LogicalType::UInt32
                | LogicalType::UInt64
                | LogicalType::Float32
                | LogicalType::Float64
                | LogicalType::Decimal { .. }
        )
    }

    /// 校验 Decimal 精度标度是否合法
    pub fn validate(&self) -> D2DResult<()> {
        if let LogicalType::Decimal { precision, scale } = self {
            if *precision < 1 || *precision > 38 {
                return Err(d2d_core::D2DError::schema(format!(
                    "Decimal precision must be 1..=38, got {}",
                    precision
                )));
            }
            if *scale < 0 || *scale as u8 > *precision {
                return Err(d2d_core::D2DError::schema(format!(
                    "Decimal scale must be 0..=precision({}), got {}",
                    precision, scale
                )));
            }
        }
        if let LogicalType::Timestamp { tz: Some(off), .. } = self {
            if !off.is_valid() {
                return Err(d2d_core::D2DError::schema(format!(
                    "invalid timezone offset: {}",
                    off
                )));
            }
        }
        Ok(())
    }

    /// 递归校验整个类型（含嵌套）
    pub fn validate_deep(&self) -> D2DResult<()> {
        self.validate()?;
        match self {
            LogicalType::List(inner) => inner.validate_deep(),
            LogicalType::Struct(fields) => {
                for f in fields {
                    f.logical_type.validate_deep()?;
                }
                Ok(())
            }
            LogicalType::Map(k, v) => {
                k.validate_deep()?;
                v.validate_deep()
            }
            _ => Ok(()),
        }
    }

    /// 人类可读描述（用于错误信息）
    pub fn describe(&self) -> String {
        match self {
            LogicalType::Null => "NULL".to_string(),
            LogicalType::Boolean => "BOOLEAN".to_string(),
            LogicalType::Int8 => "TINYINT".to_string(),
            LogicalType::Int16 => "SMALLINT".to_string(),
            LogicalType::Int32 => "INT".to_string(),
            LogicalType::Int64 => "BIGINT".to_string(),
            LogicalType::UInt8 => "TINYINT UNSIGNED".to_string(),
            LogicalType::UInt16 => "SMALLINT UNSIGNED".to_string(),
            LogicalType::UInt32 => "INT UNSIGNED".to_string(),
            LogicalType::UInt64 => "BIGINT UNSIGNED".to_string(),
            LogicalType::Float32 => "FLOAT".to_string(),
            LogicalType::Float64 => "DOUBLE".to_string(),
            LogicalType::Decimal { precision, scale } => format!("DECIMAL({},{})", precision, scale),
            LogicalType::Utf8 => "VARCHAR".to_string(),
            LogicalType::LargeUtf8 => "TEXT".to_string(),
            LogicalType::Binary => "BINARY".to_string(),
            LogicalType::LargeBinary => "BLOB".to_string(),
            LogicalType::Date32 => "DATE".to_string(),
            LogicalType::Time64(p) => format!("TIME({})", p.as_str()),
            LogicalType::Timestamp { precision, tz } => match tz {
                Some(off) => format!("TIMESTAMP({}) WITH TIME ZONE {}", precision.as_str(), off),
                None => format!("TIMESTAMP({})", precision.as_str()),
            },
            LogicalType::Interval => "INTERVAL".to_string(),
            LogicalType::Duration => "DURATION".to_string(),
            LogicalType::List(inner) => format!("LIST<{}>", inner.describe()),
            LogicalType::Struct(fields) => {
                let inner: Vec<String> = fields
                    .iter()
                    .map(|f| format!("{}: {}", f.name, f.logical_type.describe()))
                    .collect();
                format!("STRUCT<{}>", inner.join(", "))
            }
            LogicalType::Map(k, v) => format!("MAP<{}, {}>", k.describe(), v.describe()),
            LogicalType::Json => "JSON".to_string(),
            LogicalType::Enum(vals) => format!("ENUM({})", vals.join("|")),
        }
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.describe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_validation() {
        assert!(LogicalType::Decimal { precision: 38, scale: 10 }.validate().is_ok());
        assert!(LogicalType::Decimal { precision: 0, scale: 0 }.validate().is_err());
        assert!(LogicalType::Decimal { precision: 10, scale: 11 }.validate().is_err());
    }

    #[test]
    fn test_tz_validation() {
        let ok = LogicalType::Timestamp {
            precision: TemporalPrecision::Micro,
            tz: Some(FixedOffset::cst()),
        };
        assert!(ok.validate().is_ok());

        let bad = LogicalType::Timestamp {
            precision: TemporalPrecision::Micro,
            tz: Some(FixedOffset { minutes: 9999 }),
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_describe() {
        assert_eq!(LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None }.describe(), "TIMESTAMP(micro)");
        assert_eq!(FixedOffset::cst().to_string(), "+08:00");
        assert_eq!(LogicalType::Decimal{precision:19,scale:4}.describe(), "DECIMAL(19,4)");
    }

    #[test]
    fn test_nested_validation() {
        let bad = LogicalType::List(Box::new(LogicalType::Decimal { precision: 99, scale: 0 }));
        assert!(bad.validate_deep().is_err());
    }

    #[test]
    fn test_schema_lookup() {
        let schema = LogicalSchema::new(vec![
            LogicalField::new("id", LogicalType::Int64, false),
            LogicalField::new("amount", LogicalType::Decimal { precision: 19, scale: 4 }, true),
        ]);
        assert_eq!(schema.field_index("amount"), Some(1));
        assert!(schema.field("nope").is_none());
        let required = vec!["id".to_string(), "nope".to_string()];
        let missing = schema.missing_columns(&required);
        assert_eq!(missing, vec!["nope"]);
    }

    #[test]
    fn test_origin_provenance() {
        let f = LogicalField::with_origin(
            "amt",
            LogicalType::Decimal { precision: 38, scale: 10 },
            true,
            TypeOrigin::new("dameng", "NUMBER(38,10)"),
        );
        assert_eq!(f.origin.unwrap().db_type, "NUMBER(38,10)");
    }
}
