//! **MySQL 协议类型码** → 逻辑类型
//!
//! # 这只是**回退**，不是权威
//!
//! 表上真实列的类型来自 `DESCRIBE`（Doris 原生写法，经 [`crate::types::DorisTypeAdapter`]），
//! 那才是权威。这里处理的是**另一种列**：`query.columns` 里的表达式别名
//! （`"amount" * 1.1 AS scaled`）—— 它不在表结构里，`DESCRIBE` 也就问不出来，
//! 只能看服务器在结果集元数据里报的类型码。
//!
//! 为什么不干脆一律用类型码：它是**有损**的，而且丢的正是要紧的信息。实测
//! （192.0.2.10:19030，`d2d_type_probe`，21 列）：
//!
//! | 真实 Doris 类型 | 协议码 | 丢了什么 |
//! |---|---|---|
//! | `boolean` | `MYSQL_TYPE_TINY` | 与 `tinyint` 分不开 |
//! | `largeint` | `MYSQL_TYPE_STRING` | 128 位整数被当成字符串 |
//! | `text` / `string` | `MYSQL_TYPE_BLOB` | 与二进制列分不开 |
//! | `varchar(200)` | `MYSQL_TYPE_STRING` 且 `column_length=255` | **长度一律 255**，与声明无关 |
//! | `decimal(18,4)` | `MYSQL_TYPE_NEWDECIMAL` `len=20 dec=4` | 精度得反推 |
//!
//! 所以：**能用 `DESCRIBE` 就必须用 `DESCRIBE`**；走这里说明这列的声明类型本来就不存在。

use crate::types::datetime_precision;
use d2d_types::LogicalType;
use mysql_async::consts::ColumnType;
use mysql_async::Column;

/// 结果集列元数据 → 逻辑类型
///
/// 无法确定的返回 `Err`（带上列名）—— 不猜。猜错的代价是"写进去了、值不对"，
/// 而这里猜不出来时用户是有出路的（改用 `type_contract` 显式声明）。
pub fn logical_type_of(col: &Column) -> Result<LogicalType, String> {
    let name = col.name_str().to_string();
    let decimals = col.decimals();
    let len = col.column_length();

    let ty = match col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => LogicalType::Int8,
        ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => LogicalType::Int16,
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => LogicalType::Int32,
        ColumnType::MYSQL_TYPE_LONGLONG => LogicalType::Int64,
        ColumnType::MYSQL_TYPE_FLOAT => LogicalType::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => LogicalType::Float64,

        // 精度反推：MySQL 协议里 DECIMAL 的 column_length 是**显示宽度**，
        // 实测 `decimal(18,4)` → 20、`decimal(38,10)` → 40、`decimal(10,0)` → 11。
        // 也就是 `precision + (有小数点 ? 2 : 1)`（符号位 + 小数点）。
        ColumnType::MYSQL_TYPE_NEWDECIMAL | ColumnType::MYSQL_TYPE_DECIMAL => {
            let scale = decimals.min(38) as i8;
            let overhead = u32::from(scale > 0) + 1;
            let precision = len.saturating_sub(overhead).clamp(1, 38) as u8;
            LogicalType::Decimal { precision, scale }
        }

        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => LogicalType::Date32,
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => LogicalType::Timestamp {
            precision: datetime_precision(decimals),
            tz: None,
        },
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            LogicalType::Time64(datetime_precision(decimals))
        }

        // 实测 `text`/`string` 走的就是 BLOB 码 —— 所以这里落 Utf8 而不是二进制。
        // Doris 没有二进制列类型，落到这个码上的实际都是文本。
        // 长度一律是 255，区分不出 STRING(1048576) 与 varchar(80)，故统一 Utf8。
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET => LogicalType::Utf8,

        ColumnType::MYSQL_TYPE_JSON => LogicalType::Json,
        ColumnType::MYSQL_TYPE_NULL => LogicalType::Null,

        other => {
            return Err(format!(
                "列 `{}`：Doris 结果集报的类型码 {:?} 无法确定为逻辑类型。\
                 若这是 `query.columns` 里的表达式，请用 `type_contract` 显式声明它的类型",
                name, other
            ))
        }
    };
    Ok(ty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_types::TemporalPrecision;
    use mysql_async::consts::ColumnFlags;

    fn col(ty: ColumnType, len: u32, dec: u8) -> Column {
        Column::new(ty)
            .with_name(b"c")
            .with_column_length(len)
            .with_decimals(dec)
            .with_flags(ColumnFlags::empty())
    }

    // ───────── 实测形状逐条钉住 ─────────

    #[test]
    fn test_integer_family_as_observed() {
        // 23 个实测类型码里挑的整数族（len/dec 取自真实输出）
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_TINY, 4, 0)).unwrap(),
            LogicalType::Int8
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_SHORT, 6, 0)).unwrap(),
            LogicalType::Int16
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_LONG, 11, 0)).unwrap(),
            LogicalType::Int32
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_LONGLONG, 20, 0)).unwrap(),
            LogicalType::Int64
        );
    }

    #[test]
    fn test_decimal_precision_recovered_from_display_width() {
        // 实测：decimal(18,4) → len=20 dec=4；decimal(38,10) → len=40 dec=10；decimal(10,0) → len=11 dec=0
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 20, 4)).unwrap(),
            LogicalType::Decimal {
                precision: 18,
                scale: 4
            }
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 40, 10)).unwrap(),
            LogicalType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        // scale=0 时没有小数点，只多一个符号位
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 11, 0)).unwrap(),
            LogicalType::Decimal {
                precision: 10,
                scale: 0
            }
        );
    }

    #[test]
    fn test_datetime_fractional_digits() {
        let ts = |p| LogicalType::Timestamp {
            precision: p,
            tz: None,
        };
        // 实测：datetime → dec=0；datetime(3) → dec=3；datetime(6) → dec=6
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 19, 0)).unwrap(),
            ts(TemporalPrecision::Micro)
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 19, 3)).unwrap(),
            ts(TemporalPrecision::Milli)
        );
        // 6 是微秒，不是纳秒（与 types.rs 同一规则）
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 19, 6)).unwrap(),
            ts(TemporalPrecision::Micro)
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATE, 10, 0)).unwrap(),
            LogicalType::Date32
        );
    }

    /// 实测 `text` / `string` 走 BLOB 码 —— 必须落 Utf8，落 Binary 会让写回 Doris 失败
    #[test]
    fn test_blob_code_is_text_because_doris_has_no_binary() {
        for t in [
            ColumnType::MYSQL_TYPE_BLOB,
            ColumnType::MYSQL_TYPE_TINY_BLOB,
            ColumnType::MYSQL_TYPE_MEDIUM_BLOB,
            ColumnType::MYSQL_TYPE_LONG_BLOB,
        ] {
            assert_eq!(
                logical_type_of(&col(t, 255, 0)).unwrap(),
                LogicalType::Utf8,
                "{:?}",
                t
            );
        }
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_JSON, 255, 0)).unwrap(),
            LogicalType::Json
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_STRING, 255, 0)).unwrap(),
            LogicalType::Utf8
        );
    }

    /// 认不出来就报错并**指出出路**，不猜
    #[test]
    fn test_unknown_code_errors_with_a_way_out() {
        let e = logical_type_of(&col(ColumnType::MYSQL_TYPE_GEOMETRY, 0, 0)).unwrap_err();
        assert!(e.contains("type_contract"), "要指出出路: {}", e);
        assert!(e.contains('c'), "要点出是哪一列: {}", e);
    }

    /// `MYSQL_TYPE_NULL` 是合法的（全 NULL 的常量表达式），不是错误
    #[test]
    fn test_null_type_is_not_an_error() {
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NULL, 0, 0)).unwrap(),
            LogicalType::Null
        );
    }
}
