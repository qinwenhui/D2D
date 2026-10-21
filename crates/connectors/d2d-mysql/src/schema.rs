//! 权威 schema 从哪来：`information_schema.COLUMNS`
//!
//! MySQL 这里**不用猜** —— `COLUMN_TYPE` 给的就是声明原文（实测 8.0.32）：
//! `tinyint(1)` / `tinyint unsigned` / `decimal(38,10)` / `datetime(3)` /
//! `varbinary(100)` / `enum('a','b')`。`UNSIGNED` 与精度都在里面，直接喂给
//! [`MysqlTypeAdapter`](crate::types::MysqlTypeAdapter)。
//!
//! 表达式别名（`SELECT MAX(created_at) AS m`）不在 `information_schema` 里，
//! 退回结果集元数据的协议类型码（[`crate::wire`]），并明确告诉用户那条路有损。

use crate::wire;
use d2d_connector::{QueryConfig, SourceSpec, TypeContractColumn};
use d2d_core::{D2DError, D2DResult};
use d2d_types::{LogicalArrowBridge, LogicalType, TypeAdapter};
use arrow::datatypes::{Field, SchemaRef};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// `information_schema` 的一行
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedColumn {
    pub name: String,
    /// 声明原文，如 `decimal(18,4)` / `tinyint unsigned`
    pub type_str: String,
    pub nullable: bool,
}

pub struct ResolvedSchema {
    pub schema: SchemaRef,
    pub logical: Vec<LogicalType>,
}

/// 取一个表的列声明
pub async fn describe_table(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> D2DResult<Vec<DescribedColumn>> {
    let sql = "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE \
               FROM information_schema.COLUMNS \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               ORDER BY ORDINAL_POSITION";
    let rows: Vec<Row> = conn
        .exec(sql, (database, table))
        .await
        .map_err(|e| D2DError::external("mysql", format!("读 information_schema 失败: {}", e)))?;
    let out: Vec<DescribedColumn> = rows
        .iter()
        .filter_map(|r| {
            let name = text(r, 0);
            let ty = text(r, 1);
            if name.is_empty() || ty.is_empty() {
                return None;
            }
            Some(DescribedColumn {
                name,
                type_str: ty,
                nullable: !text(r, 2).eq_ignore_ascii_case("no"),
            })
        })
        .collect();
    if out.is_empty() {
        return Err(D2DError::external(
            "mysql",
            format!(
                "表 `{}`.`{}` 没有任何列 —— 表不存在或当前用户没权限",
                database, table
            ),
        ));
    }
    Ok(out)
}

fn text(r: &Row, i: usize) -> String {
    match r.as_ref(i) {
        Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(mysql_async::Value::NULL) | None => String::new(),
        Some(other) => format!("{:?}", other),
    }
}

fn index_by_name(cols: &[DescribedColumn]) -> HashMap<String, &DescribedColumn> {
    // 用小写键：MySQL 的列名匹配在多数平台上不区分大小写
    cols.iter().map(|c| (c.name.to_lowercase(), c)).collect()
}

fn contract_index(contract: Option<&[TypeContractColumn]>) -> HashMap<String, String> {
    contract
        .unwrap_or(&[])
        .iter()
        .map(|c| (c.column.to_lowercase(), c.source_type.clone()))
        .collect()
}

/// 解析本次查询的 schema
pub async fn resolve_schema(
    conn: &mut Conn,
    database: &str,
    spec: &SourceSpec,
    query: &QueryConfig,
) -> D2DResult<ResolvedSchema> {
    let described = describe_table(conn, database, &spec.table).await?;
    let by_name = index_by_name(&described);
    debug!(table = %spec.table, columns = described.len(), "MySQL 表结构已取到");

    let builder = super::sql::MysqlSqlBuilder::new(Some(database), &spec.table);
    let probe_sql = builder.build_query(query, "");
    let qr = conn
        .query_iter(&probe_sql)
        .await
        .map_err(|e| D2DError::external("mysql", format!("schema 探测失败（{}）: {}", probe_sql, e)))?;
    let cols = qr.columns_ref().to_vec();
    drop(qr);
    if cols.is_empty() {
        return Err(D2DError::schema(format!(
            "查询没有返回任何列（{}）—— 请检查 source.query.columns",
            probe_sql
        )));
    }

    let adapter = crate::types::MysqlTypeAdapter::new();
    let contract = contract_index(spec.type_contract.as_deref());
    let mut fields = Vec::with_capacity(cols.len());
    let mut logical = Vec::with_capacity(cols.len());

    for c in &cols {
        let name = c.name_str().to_string();
        let key = name.to_lowercase();
        let (decl, source) = match (contract.get(&key), by_name.get(&key)) {
            (Some(t), _) => (t.clone(), "type_contract"),
            (None, Some(d)) => (d.type_str.clone(), "information_schema"),
            (None, None) => (String::new(), "wire"),
        };
        let (lt, nullable) = if source == "wire" {
            (
                wire::logical_type_of(c).map_err(D2DError::schema)?,
                true, // 协议不告诉可空性，表达式一律按可空处理
            )
        } else {
            (
                adapter.to_logical_from_str(&decl)?,
                by_name.get(&key).map(|d| d.nullable).unwrap_or(true),
            )
        };
        debug!(column = %name, declared = %decl, source, logical = %lt.describe(), "列类型已定");
        fields.push(Field::new(&name, LogicalArrowBridge::to_arrow(&lt)?, nullable));
        logical.push(lt);
    }

    info!(table = %spec.table, columns = fields.len(), "mysql source schema 已确定");
    Ok(ResolvedSchema {
        schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        logical,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real() -> Vec<DescribedColumn> {
        [
            ("id", "int", false),
            ("c_bool", "tinyint(1)", true),
            ("c_utiny", "tinyint unsigned", true),
            ("c_dec", "decimal(38,10)", true),
            ("c_dt6", "datetime(6)", true),
            ("c_ts", "timestamp(6)", true),
            ("c_time", "time(3)", true),
            ("c_vc", "varchar(200)", true),
            ("c_txt", "text", true),
            ("c_blob", "blob", true),
            ("c_json", "json", true),
            ("c_enum", "enum('a','b')", true),
        ]
        .iter()
        .map(|(n, t, nullable)| DescribedColumn {
            name: n.to_string(),
            type_str: t.to_string(),
            nullable: *nullable,
        })
        .collect()
    }

    #[test]
    fn test_index_is_case_insensitive() {
        let d = real();
        let idx = index_by_name(&d);
        assert!(idx.contains_key("c_utiny"));
        assert_eq!(idx["c_utiny"].type_str, "tinyint unsigned");
    }

    /// information_schema 给的类型必须能一条不落地喂进适配器
    #[test]
    fn test_every_described_type_maps() {
        let a = crate::types::MysqlTypeAdapter::new();
        for c in real() {
            let lt = a
                .to_logical_from_str(&c.type_str)
                .unwrap_or_else(|e| panic!("{} ({}) 映射失败: {}", c.name, c.type_str, e));
            assert!(LogicalArrowBridge::to_arrow(&lt).is_ok(), "{}", c.name);
        }
    }

    #[test]
    fn test_contract_overrides_by_name() {
        let idx = contract_index(Some(&[TypeContractColumn {
            column: "Scaled".into(),
            source_type: "DECIMAL(18,4)".into(),
            sink_type: "DECIMAL(18,4)".into(),
        }]));
        assert_eq!(idx.get("scaled").map(String::as_str), Some("DECIMAL(18,4)"));
        assert!(contract_index(None).is_empty());
    }
}
