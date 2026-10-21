//! LogicalType → dmfldr 控制文件 FORMAT 串 / 解析端 chrono 格式串
//!
//! **单向真相原则**：控制文件声明的导出格式与解析端使用的格式必须同源，
//! 否则会出现"导出秒级、解析按微秒"这类静默错位。本模块同时产出两侧格式。

use d2d_types::{LogicalType, TemporalPrecision};

/// dmfldr 控制文件的 DATE FORMAT 串（None = 无需 FORMAT，按原样导出）
///
/// 手册 3.3：目标列为日期类型时默认按 `yyyy-mm-dd hh:mi:ss` 解析——
/// 秒级以下精度默认全丢，因此时间列必须显式声明 FORMAT。
pub fn dm_export_format(lt: &LogicalType) -> Option<String> {
    let frac = |p: &TemporalPrecision| match p {
        TemporalPrecision::Second => "",
        TemporalPrecision::Milli => ".FF3",
        TemporalPrecision::Micro => ".FF6",
        TemporalPrecision::Nano => ".FF9",
    };
    match lt {
        LogicalType::Date32 => Some("YYYY-MM-DD".to_string()),
        LogicalType::Time64(p) => Some(format!("HH24:MI:SS{}", frac(p))),
        LogicalType::Timestamp { precision, tz } => {
            let base = format!("YYYY-MM-DD HH24:MI:SS{}", frac(precision));
            match tz {
                Some(_) => Some(format!("{} TZH:TZM", base)),
                None => Some(base),
            }
        }
        _ => None,
    }
}

/// 解析端 chrono 格式串（与 [`dm_export_format`] 严格对应）
///
/// **实现不在这里** —— 它是 [`d2d_types::text_batch::temporal_format`]。
/// 格式串是三条读取路径共用的（dmfldr / ODBC / Doris MySQL 协议口），
/// 放在这里就等于给 dmfldr 留了一份可以各自演化的副本。
///
/// 这里保留转发，是因为"控制文件 FORMAT 与解析格式同源"这条约束**属于 dmfldr**：
/// 下面那组测试钉住的正是 [`dm_export_format`] 与它的逐项对应关系。
pub use d2d_types::text_batch::temporal_format as parse_format;

/// 是否需要 chrono 解析（时间列）
pub fn is_temporal(lt: &LogicalType) -> bool {
    lt.is_temporal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_types::FixedOffset;

    fn ts(p: TemporalPrecision, tz: Option<FixedOffset>) -> LogicalType {
        LogicalType::Timestamp { precision: p, tz }
    }

    #[test]
    fn test_date_formats() {
        assert_eq!(
            dm_export_format(&LogicalType::Date32).unwrap(),
            "YYYY-MM-DD"
        );
        assert_eq!(parse_format(&LogicalType::Date32).unwrap(), "%Y-%m-%d");
    }

    #[test]
    fn test_timestamp_precision_mapping() {
        let cases = [
            (
                TemporalPrecision::Second,
                "YYYY-MM-DD HH24:MI:SS",
                "%Y-%m-%d %H:%M:%S",
            ),
            (
                TemporalPrecision::Milli,
                "YYYY-MM-DD HH24:MI:SS.FF3",
                "%Y-%m-%d %H:%M:%S%.f",
            ),
            (
                TemporalPrecision::Micro,
                "YYYY-MM-DD HH24:MI:SS.FF6",
                "%Y-%m-%d %H:%M:%S%.f",
            ),
            (
                TemporalPrecision::Nano,
                "YYYY-MM-DD HH24:MI:SS.FF9",
                "%Y-%m-%d %H:%M:%S%.f",
            ),
        ];
        for (p, dm, chrono) in cases {
            let lt = ts(p, None);
            assert_eq!(dm_export_format(&lt).unwrap(), dm, "{:?}", p);
            assert_eq!(parse_format(&lt).unwrap(), chrono, "{:?}", p);
        }
    }

    #[test]
    fn test_timestamp_with_tz() {
        let lt = ts(TemporalPrecision::Micro, Some(FixedOffset::cst()));
        let dm = dm_export_format(&lt).unwrap();
        assert!(dm.ends_with("TZH:TZM"), "{}", dm);
        assert_eq!(parse_format(&lt).unwrap(), "%Y-%m-%d %H:%M:%S%.f%z");
    }

    #[test]
    fn test_time64_formats() {
        let lt = LogicalType::Time64(TemporalPrecision::Micro);
        assert_eq!(dm_export_format(&lt).unwrap(), "HH24:MI:SS.FF6");
        assert_eq!(parse_format(&lt).unwrap(), "%H:%M:%S%.f");
    }

    #[test]
    fn test_non_temporal_has_no_format() {
        assert!(dm_export_format(&LogicalType::Int64).is_none());
        assert!(parse_format(&LogicalType::Utf8).is_none());
        assert!(dm_export_format(&LogicalType::Decimal {
            precision: 19,
            scale: 4
        })
        .is_none());
    }
}
