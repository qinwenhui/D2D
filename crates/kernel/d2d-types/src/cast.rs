use crate::{LogicalType, TemporalPrecision};
use d2d_core::{D2DError, D2DResult};

/// 类型转换策略
///
/// 引擎/Sink 在执行转换前必须查询本矩阵，禁止凭感觉转。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastPolicy {
    /// 完全等价（同一类型的参数差异，如 Int32 → Int32）
    Exact,
    /// 安全拓宽，值域严格包含且无精度损失（Int32 → Int64, Float32 → Float64）
    Widen,
    /// 有损转换，需显式确认（Decimal → Float64 丢精度, Int64 → Float64 大数丢整）
    Lossy {
        /// 风险描述（中文，直接展示给任务作者）
        risk: &'static str,
    },
    /// 语义不通，禁止转换（Utf8 → Timestamp 无格式声明, Binary → Utf8 乱码）
    Forbidden {
        /// 拒绝原因（中文，直接展示给任务作者）
        reason: &'static str,
    },
}

impl CastPolicy {
    pub fn is_allowed(&self) -> bool {
        !matches!(self, CastPolicy::Forbidden { .. })
    }

    pub fn is_lossless(&self) -> bool {
        matches!(self, CastPolicy::Exact | CastPolicy::Widen)
    }

    /// 供 CLI/日志展示
    pub fn describe(&self) -> String {
        match self {
            CastPolicy::Exact => "精确转换".to_string(),
            CastPolicy::Widen => "安全拓宽".to_string(),
            CastPolicy::Lossy { risk } => format!("有损转换: {}", risk),
            CastPolicy::Forbidden { reason } => format!("禁止转换: {}", reason),
        }
    }
}

/// 类型兼容矩阵
///
/// 回答一个问题：`from` 类型的数据能不能安全地当作 `to` 类型用？
pub struct CastTable;

impl CastTable {
    /// 查询转换策略
    pub fn policy(from: &LogicalType, to: &LogicalType) -> CastPolicy {
        use LogicalType as L;

        if from == to {
            return CastPolicy::Exact;
        }

        match (from, to) {
            // ── 整型 → 整型：按位宽拓宽安全 ──
            (L::Int8, L::Int16 | L::Int32 | L::Int64) => CastPolicy::Widen,
            (L::Int16, L::Int32 | L::Int64) => CastPolicy::Widen,
            (L::Int32, L::Int64) => CastPolicy::Widen,
            (L::Int8 | L::Int16 | L::Int32 | L::Int64, L::UInt8 | L::UInt16 | L::UInt32 | L::UInt64) => {
                if Self::uint_covers(from, to) {
                    CastPolicy::Widen
                } else {
                    CastPolicy::Lossy { risk: "有符号转无符号：负值将回绕" }
                }
            }
            (L::UInt8, L::UInt16 | L::UInt32 | L::UInt64) => CastPolicy::Widen,
            (L::UInt16, L::UInt32 | L::UInt64) => CastPolicy::Widen,
            (L::UInt32, L::UInt64) => CastPolicy::Widen,

            // ── 整型收窄：有损（截断） ──
            (L::Int64, L::Int32 | L::Int16 | L::Int8)
            | (L::Int32, L::Int16 | L::Int8)
            | (L::Int16, L::Int8) => CastPolicy::Lossy { risk: "整型收窄：超出目标范围将被截断" },
            (L::UInt64, L::UInt32 | L::UInt16 | L::UInt8)
            | (L::UInt32, L::UInt16 | L::UInt8)
            | (L::UInt16, L::UInt8) => {
                CastPolicy::Lossy { risk: "无符号整型收窄：超出目标范围将被截断" }
            }

            // ── 整数 → 浮点 ──
            (L::Int8 | L::Int16 | L::UInt8 | L::UInt16, L::Float32 | L::Float64) => CastPolicy::Widen,
            (L::Int32 | L::UInt32, L::Float64) => CastPolicy::Widen,
            (L::Int32 | L::UInt32, L::Float32) => {
                CastPolicy::Lossy { risk: "32 位整数转 Float32：超过 2^24 精度丢失" }
            }
            (L::Int64 | L::UInt64, L::Float32 | L::Float64) => {
                CastPolicy::Lossy { risk: "64 位整数转浮点：大整数精度丢失" }
            }

            // ── 浮点 → 浮点 ──
            (L::Float32, L::Float64) => CastPolicy::Widen,
            (L::Float64, L::Float32) => CastPolicy::Lossy { risk: "双精度转单精度：精度与范围下降" },

            // ── 整数 → Decimal ──
            (L::Int8 | L::Int16 | L::Int32, L::Decimal { .. }) => CastPolicy::Widen,
            (L::Int64 | L::UInt32 | L::UInt64, L::Decimal { .. }) => {
                CastPolicy::Lossy { risk: "宽整数转 Decimal：需确认目标精度容得下极值" }
            }

            // ── 浮点 → Decimal ──
            (L::Float32 | L::Float64, L::Decimal { .. }) => {
                CastPolicy::Lossy { risk: "浮点转定点：二进制近似值落入十进制标度，结果取决于具体值" }
            }

            // ── Decimal → 数值 ──
            (L::Decimal { .. }, L::Int8 | L::Int16 | L::Int32 | L::Int64 | L::UInt8 | L::UInt16 | L::UInt32 | L::UInt64) => {
                CastPolicy::Lossy { risk: "定点转整数：可能溢出或小数截断" }
            }
            (L::Decimal { .. }, L::Float32 | L::Float64) => {
                CastPolicy::Lossy { risk: "定点转浮点：高精度十进制值可能无法精确表示" }
            }
            (L::Decimal { precision: p1, scale: s1 }, L::Decimal { precision: p2, scale: s2 }) => {
                if p2 >= p1 && s2 >= s1 {
                    CastPolicy::Widen
                } else if p2 >= p1 && s2 < s1 {
                    CastPolicy::Lossy { risk: "DECIMAL 标度收窄：小数部分被截断" }
                } else {
                    CastPolicy::Lossy { risk: "DECIMAL 精度/标度不足：可能溢出或截断" }
                }
            }

            // ── 字符串 → 字符串 ──
            (L::Utf8, L::LargeUtf8) => CastPolicy::Widen,
            (L::LargeUtf8, L::Utf8) => CastPolicy::Lossy { risk: "大文本转普通文本：长度可能超限" },

            // ── 二进制 ↔ 字符串：显式禁止（乱码风险） ──
            (L::Binary | L::LargeBinary, L::Utf8 | L::LargeUtf8) => CastPolicy::Forbidden {
                reason: "二进制转字符串必然乱码；如需文本请显式声明解码方式（如 Base64）",
            },
            (L::Utf8 | L::LargeUtf8, L::Binary | L::LargeBinary) => CastPolicy::Forbidden {
                reason: "字符串转二进制需显式声明编码方式（如 Base64）",
            },
            (L::Binary, L::LargeBinary) => CastPolicy::Widen,
            (L::LargeBinary, L::Binary) => CastPolicy::Lossy { risk: "大二进制转普通二进制：长度可能超限" },

            // ── 时间戳精度/时区调整 ──
            (
                L::Timestamp { precision: p1, tz: t1 },
                L::Timestamp { precision: p2, tz: t2 },
            ) => Self::temporal_precision_policy(p1, p2, t1.is_some(), t2.is_some()),

            // ── 时间类型之间：一律禁止隐式转换 ──
            (L::Date32, L::Time64(_) | L::Timestamp { .. }) => CastPolicy::Forbidden {
                reason: "DATE 无时刻信息，转时刻类型需显式补默认时刻并在配置中声明",
            },
            (L::Time64(_), L::Date32 | L::Timestamp { .. }) => CastPolicy::Forbidden {
                reason: "TIME 无日期信息，转时间戳需显式补默认日期并在配置中声明",
            },
            (L::Timestamp { .. }, L::Date32 | L::Time64(_)) => CastPolicy::Forbidden {
                reason: "TIMESTAMP 截断为 DATE/TIME 会丢失语义，请显式执行并确认",
            },

            // ── 布尔 / 枚举 / JSON / 复杂类型：仅等价或显式禁止 ──
            (L::Boolean, _) => CastPolicy::Forbidden { reason: "BOOLEAN 不支持隐式转为其他类型" },
            (_, L::Boolean) => CastPolicy::Forbidden { reason: "其他类型不支持隐式转为 BOOLEAN" },
            (L::Enum(_), _) => CastPolicy::Forbidden { reason: "ENUM 需先显式映射为字符串" },
            (L::Json, _) | (_, L::Json) => CastPolicy::Forbidden {
                reason: "JSON 与标量间的转换需显式声明序列化格式",
            },
            (L::Null, _) => CastPolicy::Widen, // 空列可充当任何类型
            (_, L::Null) => CastPolicy::Lossy { risk: "非空列转空列：所有数据将丢失" },

            (L::List(a), L::List(b)) => Self::policy(a, b),
            (L::Struct(fa), L::Struct(fb)) if fa.len() == fb.len() => {
                let mut worst = CastPolicy::Exact;
                for (a, b) in fa.iter().zip(fb.iter()) {
                    let p = Self::policy(&a.logical_type, &b.logical_type);
                    if !p.is_lossless() {
                        return p;
                    }
                    if matches!(p, CastPolicy::Widen) {
                        worst = CastPolicy::Widen;
                    }
                }
                worst
            }
            (L::Map(ka, va), L::Map(kb, vb)) => {
                let kp = Self::policy(ka, kb);
                if !kp.is_lossless() {
                    return kp;
                }
                Self::policy(va, vb)
            }

            _ => CastPolicy::Forbidden {
                reason: "类型族不通（如标量↔复杂类型），需显式拆解/构造",
            },
        }
    }

    /// uint 是否能无风险容纳有符号 from 的值域
    fn uint_covers(from: &LogicalType, to: &LogicalType) -> bool {
        use LogicalType as L;
        matches!(
            (from, to),
            (L::Int8, L::UInt8 | L::UInt16 | L::UInt32 | L::UInt64)
                | (L::Int16, L::UInt16 | L::UInt32 | L::UInt64)
                | (L::Int32, L::UInt32 | L::UInt64)
        )
    }

    fn temporal_precision_policy(
        from: &TemporalPrecision,
        to: &TemporalPrecision,
        from_tz: bool,
        to_tz: bool,
    ) -> CastPolicy {
        use TemporalPrecision as P;
        let rank = |p: &TemporalPrecision| match p {
            P::Second => 0,
            P::Milli => 1,
            P::Micro => 2,
            P::Nano => 3,
        };
        if from_tz != to_tz {
            return CastPolicy::Lossy {
                risk: "时区语义变化：需确认偏移基准，否则绝对时刻会偏移",
            };
        }
        if rank(to) >= rank(from) {
            CastPolicy::Widen
        } else {
            CastPolicy::Lossy { risk: "时间精度收窄：亚秒信息被截断" }
        }
    }

    /// 断言可转换，否则返回带完整类型信息的错误（fail-fast 用）
    pub fn ensure_allowed(from: &LogicalType, to: &LogicalType, context: &str) -> D2DResult<()> {
        let policy = Self::policy(from, to);
        if !policy.is_allowed() {
            return Err(D2DError::transform(format!(
                "[{}] 类型转换被禁止: {} → {}（{}）。请调整 transform 配置或 sink 表结构。",
                context,
                from.describe(),
                to.describe(),
                match policy {
                    CastPolicy::Forbidden { reason } => reason,
                    _ => "",
                }
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TemporalPrecision;

    #[test]
    fn test_exact() {
        assert_eq!(
            CastTable::policy(&LogicalType::Int32, &LogicalType::Int64),
            CastPolicy::Widen
        );
    }

    #[test]
    fn test_binary_to_utf8_forbidden() {
        let p = CastTable::policy(&LogicalType::LargeBinary, &LogicalType::Utf8);
        assert!(!p.is_allowed(), "BLOB→VARCHAR 必须禁止");
    }

    #[test]
    fn test_temporal_cross_forbidden() {
        // 时间类型之间禁止隐式互转
        let p = CastTable::policy(&LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None }, &LogicalType::Date32);
        assert!(!p.is_allowed());
        let p2 = CastTable::policy(&LogicalType::Date32, &LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None });
        assert!(!p2.is_allowed());
    }

    #[test]
    fn test_decimal_scale_narrow_lossy() {
        let p = CastTable::policy(
            &LogicalType::Decimal { precision: 38, scale: 10 },
            &LogicalType::Decimal { precision: 19, scale: 4 },
        );
        assert!(matches!(p, CastPolicy::Lossy { .. }));
    }

    #[test]
    fn test_tz_change_lossy() {
        let p = CastTable::policy(
            &LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None },
            &LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: Some(crate::FixedOffset::cst()) },
        );
        assert!(matches!(p, CastPolicy::Lossy { .. }));
    }

    #[test]
    fn test_precision_widen_ok() {
        let p = CastTable::policy(
            &LogicalType::Timestamp { precision: TemporalPrecision::Second, tz: None },
            &LogicalType::Timestamp { precision: TemporalPrecision::Nano, tz: None },
        );
        assert!(p.is_lossless());
    }

    #[test]
    fn test_boolean_forbidden() {
        assert!(!CastTable::policy(&LogicalType::Boolean, &LogicalType::Int32).is_allowed());
    }
}
