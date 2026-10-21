//! **MySQL 协议类型码** → 逻辑类型
//!
//! 只给**表达式别名**用（`SELECT MAX(created_at) AS m ...` 这种列不在
//! `information_schema` 里，问不出声明类型）。表上的真实列一律走
//! `information_schema.COLUMN_TYPE`（权威，见 `schema.rs`）。
//!
//! 协议码比 Doris 那条路信息多一些：它带 `UNSIGNED_FLAG` 与字符集，所以能区分
//! `INT` / `INT UNSIGNED`、以及 `TEXT`（字符集 33）/ `BLOB`（字符集 63）。
//! 但仍然丢精度宽度（`column_length` 是显示宽度，`varchar(200)` 也报 255 级别），
//! 所以它只是回退。

use d2d_types::{LogicalType, TemporalPrecision};
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::Column;

/// 结果集列元数据 → 逻辑类型
pub fn logical_type_of(col: &Column) -> Result<LogicalType, String> {
    let name = col.name_str().to_string();
    let unsigned = col.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let binary = col.character_set() == 63;
    let decimals = col.decimals();
    let len = col.column_length();

    let ty = match col.column_type() {
        ColumnType::MYSQL_TYPE_TINY => pick(unsigned, LogicalType::UInt8, LogicalType::Int8),
        ColumnType::MYSQL_TYPE_SHORT => pick(unsigned, LogicalType::UInt16, LogicalType::Int16),
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            pick(unsigned, LogicalType::UInt32, LogicalType::Int32)
        }
        ColumnType::MYSQL_TYPE_LONGLONG => pick(unsigned, LogicalType::UInt64, LogicalType::Int64),
        ColumnType::MYSQL_TYPE_YEAR => LogicalType::Int16,
        ColumnType::MYSQL_TYPE_FLOAT => LogicalType::Float32,
        ColumnType::MYSQL_TYPE_DOUBLE => LogicalType::Float64,

        ColumnType::MYSQL_TYPE_NEWDECIMAL | ColumnType::MYSQL_TYPE_DECIMAL => {
            let scale = decimals.min(38) as i8;
            // 显示宽度 ≈ 精度 + 符号位 +（有标度时）小数点
            let overhead = u32::from(scale > 0) + 1;
            let precision = len.saturating_sub(overhead).clamp(1, 38) as u8;
            LogicalType::Decimal { precision, scale }
        }

        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => LogicalType::Date32,
        ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2
        | ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2 => LogicalType::Timestamp {
            precision: fractional_precision(decimals),
            tz: None,
        },
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            LogicalType::Time64(fractional_precision(decimals))
        }

        // 字符集是这里唯一能区分 TEXT 与 BLOB 的东西：
        // 两者都走 MYSQL_TYPE_BLOB，字符集 63（binary）才是真二进制。
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB => {
            if binary {
                LogicalType::LargeBinary
            } else {
                LogicalType::LargeUtf8
            }
        }
        ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET => {
            if binary {
                LogicalType::LargeBinary
            } else {
                LogicalType::Utf8
            }
        }
        ColumnType::MYSQL_TYPE_BIT => LogicalType::Binary,
        ColumnType::MYSQL_TYPE_JSON => LogicalType::Json,
        ColumnType::MYSQL_TYPE_NULL => LogicalType::Null,

        other => {
            return Err(format!(
                "列 `{}`：MySQL 结果集报的类型码 {:?} 无法确定为逻辑类型。\
                 若这是 `query.columns` 里的表达式，请用 `type_contract` 显式声明它的类型",
                name, other
            ))
        }
    };
    Ok(ty)
}

fn pick(unsigned: bool, u: LogicalType, s: LogicalType) -> LogicalType {
    if unsigned {
        u
    } else {
        s
    }
}

/// `DATETIME(n)` 的 n 是小数位数（0..=6）
fn fractional_precision(decimals: u8) -> TemporalPrecision {
    match decimals {
        1..=3 => TemporalPrecision::Milli,
        4..=6 => TemporalPrecision::Micro,
        _ => TemporalPrecision::Second,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(ty: ColumnType, len: u32, dec: u8, flags: ColumnFlags, charset: u16) -> Column {
        Column::new(ty)
            .with_name(b"c")
            .with_column_length(len)
            .with_decimals(dec)
            .with_flags(flags)
            .with_character_set(charset)
    }

    #[test]
    fn test_unsigned_flag_changes_type() {
        let signed = col(ColumnType::MYSQL_TYPE_LONG, 11, 0, ColumnFlags::empty(), 63);
        let unsigned = col(ColumnType::MYSQL_TYPE_LONG, 11, 0, ColumnFlags::UNSIGNED_FLAG, 63);
        assert_eq!(logical_type_of(&signed).unwrap(), LogicalType::Int32);
        assert_eq!(logical_type_of(&unsigned).unwrap(), LogicalType::UInt32);
    }

    /// TEXT 与 BLOB 都走 BLOB 码，靠字符集区分
    #[test]
    fn test_blob_vs_text_by_charset() {
        let text = col(ColumnType::MYSQL_TYPE_BLOB, 65535, 0, ColumnFlags::empty(), 45);
        let blob = col(ColumnType::MYSQL_TYPE_BLOB, 65535, 0, ColumnFlags::empty(), 63);
        assert_eq!(logical_type_of(&text).unwrap(), LogicalType::LargeUtf8);
        assert_eq!(logical_type_of(&blob).unwrap(), LogicalType::LargeBinary);
    }

    #[test]
    fn test_temporal_fractional_digits() {
        let ts = |p| LogicalType::Timestamp { precision: p, tz: None };
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 19, 0, ColumnFlags::empty(), 63))
                .unwrap(),
            ts(TemporalPrecision::Second)
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 23, 3, ColumnFlags::empty(), 63))
                .unwrap(),
            ts(TemporalPrecision::Milli)
        );
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_DATETIME, 26, 6, ColumnFlags::empty(), 63))
                .unwrap(),
            ts(TemporalPrecision::Micro)
        );
    }

    #[test]
    fn test_decimal_precision_from_display_width() {
        // decimal(10,2) → 显示宽度 12
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 12, 2, ColumnFlags::empty(), 63))
                .unwrap(),
            LogicalType::Decimal { precision: 10, scale: 2 }
        );
        // decimal(10,0) → 11
        assert_eq!(
            logical_type_of(&col(ColumnType::MYSQL_TYPE_NEWDECIMAL, 11, 0, ColumnFlags::empty(), 63))
                .unwrap(),
            LogicalType::Decimal { precision: 10, scale: 0 }
        );
    }

    #[test]
    fn test_unknown_errors_with_a_way_out() {
        let e = logical_type_of(&col(
            ColumnType::MYSQL_TYPE_GEOMETRY,
            0,
            0,
            ColumnFlags::empty(),
            63,
        ))
        .unwrap_err();
        assert!(e.contains("type_contract"), "{}", e);
    }
}
