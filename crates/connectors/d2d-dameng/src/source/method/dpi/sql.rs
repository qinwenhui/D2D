//! DPI 分片规划：内核切法 + 达梦 SQL 渲染
//!
//! 与 dmfldr 的 `partition.rs` 刻意分开：这里不写 SQL 文件、不做元数据探测、
//! 不碰 `FlDrParams`。切法本身仍走内核同一套 [`d2d_connector::partition::plan`]，
//! 于是两种方法对同一个 `source.partition` 给出**同一组区间**。

use crate::source::method::DamengReadContext;
use crate::source::sql::{render_partition_predicate, DamengSqlBuilder};
use d2d_connector::{quote_ident, PartitionMode, PartitionSpec, SourceSpec, SourceSplit};
use d2d_core::{D2DError, D2DResult};

/// 组装一条 SELECT（列清单 = 契约列序）
///
/// `extra` 是分片谓词（已渲染成达梦 SQL）或启动自检用的 `1 = 0`。
fn build_select(spec: &SourceSpec, ctx: &DamengReadContext, extra: Option<&str>) -> String {
    let cols = ctx
        .column_names
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {} FROM {}",
        cols,
        DamengSqlBuilder::new(spec.schema_name.as_deref(), &spec.table).qualified_table()
    );

    let mut conditions: Vec<String> = Vec::new();
    if let Some(q) = &spec.query {
        if let Some(w) = &q.where_clause {
            let w = w.trim();
            if !w.is_empty() {
                conditions.push(format!("({})", w));
            }
        }
    }
    if let Some(p) = extra.map(str::trim).filter(|p| !p.is_empty()) {
        conditions.push(format!("({})", p));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql
}

/// 启动自检 SQL：跑一次完整查询但取 0 行
///
/// 它验证的是：库连得上、表/列名对、SELECT 列表能执行、列序能描述。
pub fn probe_sql(spec: &SourceSpec, ctx: &DamengReadContext) -> String {
    build_select(spec, ctx, Some("1 = 0"))
}

/// 规划分片（每片 = 一条带分区谓词的 SELECT）
pub fn plan_splits(
    spec: &SourceSpec,
    ctx: &DamengReadContext,
    max_rows: usize,
) -> D2DResult<Vec<SourceSplit>> {
    let partition = spec.effective_partition()?;
    let p = match partition {
        Some(p) => {
            p.validate()?;
            p
        }
        None => PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        },
    };

    let plan = d2d_connector::partition::plan(
        &p,
        &spec.table,
        spec.schema_name.as_deref(),
        None, // DPI MVP 不做 MIN/MAX / 元数据探测（range 必须显式给边界）
    )?;

    let total = plan.partitions.len();
    if total == 0 {
        return Err(D2DError::config(
            "dpi 分片规划结果为空（partition.conditions 为空？）",
        ));
    }

    let splits = plan
        .partitions
        .iter()
        .enumerate()
        .map(|(i, part)| {
            let pred = render_partition_predicate(&part.predicate);
            SourceSplit {
                split_id: format!("{}_dpi_{}", sanitize(&spec.table), i),
                split_info: serde_json::json!({
                    "kind": "dpi_shard",
                    "index": i,
                    "shards": total,
                    "sql": build_select(spec, ctx, Some(&pred)),
                    "describe": part.describe,
                    "max_rows": max_rows,
                }),
            }
        })
        .collect();
    Ok(splits)
}

/// `split_info` → SQL（`read` / `read_physical` 共用）
pub fn split_sql(split: &SourceSplit) -> D2DResult<&str> {
    match split.split_info.get("kind").and_then(|v| v.as_str()) {
        Some("dpi_shard") => split
            .split_info
            .get("sql")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| D2DError::schema(format!("dpi 分片 {} 缺少 sql 信息", split.split_id))),
        other => Err(D2DError::schema(format!(
            "dpi 分片 {} 的 kind {:?} 不认识（本连接器的 split() 只产出 dpi_shard）",
            split.split_id, other
        ))),
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_types::{LogicalArrowBridge, LogicalType};

    fn ctx() -> DamengReadContext {
        let lts = vec![LogicalType::Int64, LogicalType::Utf8];
        let names = ["id".to_string(), "name".to_string()];
        let fields: Vec<arrow::datatypes::Field> = names
            .iter()
            .zip(lts.iter())
            .map(|(n, lt)| {
                arrow::datatypes::Field::new(n, LogicalArrowBridge::to_arrow(lt).unwrap(), true)
            })
            .collect();
        DamengReadContext {
            schema: std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            logical_types: lts,
            column_names: names.to_vec(),
        }
    }

    fn spec(partition: serde_json::Value) -> SourceSpec {
        serde_json::from_value(serde_json::json!({
            "connector": "dameng",
            "connection": {"host": "h", "user": "u", "password": "p"},
            "method": "dpi",
            "schema_name": "APP",
            "table": "T_A",
            "query": {"where_clause": "\"status\" = 1"},
            "type_contract": [
                {"column": "id", "source_type": "BIGINT", "sink_type": "BIGINT"},
                {"column": "name", "source_type": "VARCHAR(20)", "sink_type": "VARCHAR(50)"}
            ],
            "partition": partition
        }))
        .unwrap()
    }

    #[test]
    fn test_none_mode_is_one_split_with_user_where() {
        let s = spec(serde_json::json!({"mode": "none"}));
        let splits = plan_splits(&s, &ctx(), 4000).unwrap();
        assert_eq!(splits.len(), 1);
        let sql = split_sql(&splits[0]).unwrap();
        assert!(
            sql.contains("SELECT \"id\", \"name\" FROM \"APP\".\"T_A\""),
            "{}",
            sql
        );
        assert!(sql.contains("WHERE (\"status\" = 1)"), "{}", sql);
        assert_eq!(splits[0].split_info["kind"], "dpi_shard");
        assert_eq!(splits[0].split_info["max_rows"], 4000);
    }

    #[test]
    fn test_list_mode_ands_each_condition() {
        let s = spec(serde_json::json!({
            "mode": "list",
            "conditions": ["\"id\" < 10", "\"id\" >= 10"]
        }));
        let splits = plan_splits(&s, &ctx(), 100).unwrap();
        assert_eq!(splits.len(), 2);
        let sql0 = split_sql(&splits[0]).unwrap();
        let sql1 = split_sql(&splits[1]).unwrap();
        assert!(
            sql0.contains("(\"status\" = 1) AND (\"id\" < 10)"),
            "{}",
            sql0
        );
        assert!(
            sql1.contains("(\"status\" = 1) AND (\"id\" >= 10)"),
            "{}",
            sql1
        );
    }

    #[test]
    fn test_hash_mode_renders_mod_hash() {
        let s = spec(serde_json::json!({"mode": "hash", "column": "id", "count": 4}));
        let splits = plan_splits(&s, &ctx(), 100).unwrap();
        assert_eq!(splits.len(), 4);
        assert!(split_sql(&splits[2])
            .unwrap()
            .contains("(MOD(ORA_HASH(COALESCE(CAST(\"id\" AS VARCHAR), ''), 4294967295), 4) = 2)"));
    }

    #[test]
    fn test_range_with_bounds_is_half_open() {
        let s = spec(serde_json::json!({
            "mode": "range", "column": "id", "count": 4, "lower": 1, "upper": 100
        }));
        let splits = plan_splits(&s, &ctx(), 100).unwrap();
        assert_eq!(splits.len(), 4);
        let sql0 = split_sql(&splits[0]).unwrap();
        assert!(sql0.contains("\"id\" >= 1 AND \"id\" < 26"), "{}", sql0);
    }

    #[test]
    fn test_probe_sql_has_no_rows() {
        let s = spec(serde_json::json!({"mode": "none"}));
        let sql = probe_sql(&s, &ctx());
        assert!(
            sql.contains("WHERE (\"status\" = 1) AND (1 = 0)"),
            "{}",
            sql
        );
    }

    #[test]
    fn test_unknown_split_kind_is_reported() {
        let split = SourceSplit {
            split_id: "s".into(),
            split_info: serde_json::json!({"kind": "dmfldr_shard"}),
        };
        let err = split_sql(&split).unwrap_err().to_string();
        assert!(err.contains("dpi_shard"), "{}", err);
    }
}
