//! 分片契约的**读取端**
//!
//! 分片怎么**切**已经全部搬去内核（`d2d_connector::partition`）——
//! 两条读取路径（odbc / dmfldr）共用同一套切法与条件渲染。
//! 这里只剩一件事：**怎么把分片里那条 WHERE 片段读出来**。
//!
//! # 删掉的那些
//!
//! 这里曾经有一整套 `SplitStrategy`（Range / Hash / None）+ `build_strategy`，
//! 是 ODBC 分片的早期实现。接内核 `partition` 之后它们**零调用点**，
//! 却还躺在仓库里 —— 而且 `RangeSplitStrategy` 产出的
//! `__PENDING_RANGE_i__` 占位条件会被 `build_query` 静默丢掉，
//! **等于 range 模式从来没真正生效过，每片都在扫全表**。
//!
//! 一个没人调的模块能藏住一个这么大的坑，是因为当时只有一个连接器在用分片。

use d2d_connector::SourceSplit;

/// 从 `split_info` 取出该分片的 WHERE 条件（不含 `WHERE` 关键字）
///
/// **约定**：分片由各读取路径产出时，把条件写进 `split_info["where_condition"]`；
/// 未写（或写成空）表示"全表"。
pub fn split_condition(split: &SourceSplit) -> String {
    split
        .split_info
        .get("where_condition")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_condition_roundtrip() {
        let split = SourceSplit {
            split_id: "t_0".into(),
            split_info: serde_json::json!({ "where_condition": "\"id\" >= 1" }),
        };
        assert_eq!(split_condition(&split), "\"id\" >= 1");
    }

    #[test]
    fn test_missing_condition_means_full_table() {
        let split = SourceSplit {
            split_id: "t_0".into(),
            split_info: serde_json::json!({ "mode": "none" }),
        };
        assert_eq!(split_condition(&split), "");
    }
}
