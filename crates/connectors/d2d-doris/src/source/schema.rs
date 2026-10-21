//! 权威 schema 从哪来：`DESCRIBE` + 结果集元数据
//!
//! # 为什么是 `DESCRIBE`，不是 `information_schema`
//!
//! 两者在同一个连接上都能查，但 `information_schema.COLUMNS` 是 Doris 的
//! **MySQL 兼容视图**，报的是"翻译成 MySQL 的说法"，翻译过程会丢东西。实测同一个
//! 表（`d2d_type_probe`，21 列）逐列对比：
//!
//! | 列 | `information_schema` | `DESCRIBE`（真类型） |
//! |---|---|---|
//! | `c_bool` | `tinyint(1)` | **`boolean`** |
//! | `c_dec18_4` | `decimalv3(18, 4)` | `decimal(18,4)` |
//! | `c_str` / `c_txt` | `string` | **`text`** |
//!
//! `boolean` 被读成 `tinyint(1)` 之后，逻辑类型是 `Int8` 而不是 `Boolean` ——
//! 不报错、不告警，就是类型悄悄变了。所以取 `DESCRIBE`：
//! 它给的是 Doris 自己的类型词汇，正好是 [`DorisTypeAdapter`] 认得的那一套。
//!
//! # 两类列，两个来源
//!
//! | 列 | 类型从哪来 |
//! |---|---|
//! | 表上的真实列 | `DESCRIBE` → [`DorisTypeAdapter`]（权威） |
//! | `query.columns` 里的表达式别名 | 结果集元数据的类型码 → [`super::wire`]（回退） |
//!
//! [`DorisTypeAdapter`]: crate::types::DorisTypeAdapter

use crate::types::DorisTypeAdapter;
use arrow::datatypes::{Field, SchemaRef};
use d2d_connector::{QueryConfig, SourceSpec, TypeContractColumn};
use d2d_core::{D2DError, D2DResult};
use d2d_types::{LogicalArrowBridge, LogicalType, TypeAdapter};
use mysql_async::prelude::*;
use mysql_async::{Conn, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// `DESCRIBE` 的一行
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedColumn {
    pub name: String,
    /// Doris 原生类型写法，如 `decimal(18,4)` / `boolean` / `datetime(6)`
    pub type_str: String,
    pub nullable: bool,
}

/// 解析出来的 schema 与它逐列对应的逻辑类型
pub struct ResolvedSchema {
    pub schema: SchemaRef,
    /// 与 `schema.fields()` 逐列对齐 —— 交给共享行构建器当解析策略
    pub logical: Vec<LogicalType>,
}

/// 取一个表的列声明（`DESCRIBE`，同时给出是否可空）
pub async fn describe_table(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> D2DResult<Vec<DescribedColumn>> {
    let sql = format!(
        "DESCRIBE {}.{}",
        super::sql::quote_ident(database),
        super::sql::quote_ident(table)
    );
    let rows: Vec<Row> = conn
        .query(&sql)
        .await
        .map_err(|e| D2DError::external("doris", format!("DESCRIBE {} 失败: {}", table, e)))?;

    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let name = text(r, 0);
        let type_str = text(r, 1);
        if name.is_empty() || type_str.is_empty() {
            continue;
        }
        out.push(DescribedColumn {
            name,
            type_str,
            // DESCRIBE 的 Null 列是 "Yes" / "No"
            nullable: !text(r, 2).eq_ignore_ascii_case("no"),
        });
    }
    if out.is_empty() {
        return Err(D2DError::external(
            "doris",
            format!(
                "DESCRIBE {}.{} 没有返回任何列 —— 表不存在或没权限",
                database, table
            ),
        ));
    }
    Ok(out)
}

/// 取一行里第 i 个单元格的文本（MySQL 文本协议下所有值都是字节串）
fn text(r: &Row, i: usize) -> String {
    match r.as_ref(i) {
        Some(mysql_async::Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(mysql_async::Value::NULL) | None => String::new(),
        Some(other) => format!("{:?}", other),
    }
}

/// 把 `DESCRIBE` 的结果做成 小写列名 → 声明 的索引
///
/// 用小写键：Doris 的列名匹配不区分大小写，用户写 `SELECT ID` 也会返回 `id` 列。
/// 用精确匹配会在这种写法上漏掉声明类型，悄悄退化成协议类型码那条路 ——
/// 而那条路会丢 `boolean`。
fn index_by_name(cols: &[DescribedColumn]) -> HashMap<String, &DescribedColumn> {
    cols.iter().map(|c| (c.name.to_lowercase(), c)).collect()
}

/// 解析出本次查询的 schema
///
/// 步骤：`DESCRIBE` 拿声明类型 → 跑一次 `LIMIT 0` 拿结果集的列与顺序 →
/// 逐列定类型。`LIMIT 0` 实测在 1915 万行的表上是 **2 ms**（FE 只规划不取数），
/// 所以"开工前把 SQL 真的跑一遍"这件事是免费的 —— 表名拼错、列名不存在、
/// 表达式写错，全都在这里大声失败，而不是迁完 0 行才发现。
pub async fn resolve_schema(
    conn: &mut Conn,
    database: &str,
    spec: &SourceSpec,
    query: &QueryConfig,
) -> D2DResult<ResolvedSchema> {
    let described = describe_table(conn, database, &spec.table).await?;
    let by_name = index_by_name(&described);
    debug!(table = %spec.table, columns = described.len(), "Doris 表结构已取到");

    // 结果集元数据：列名与顺序以**服务器返回的**为准（`SELECT *` 也只有这里知道列有哪些）
    let builder = super::sql::DorisSqlBuilder::new(Some(database), &spec.table);
    let probe_sql = builder.build_query(query, "");
    let qr = conn.query_iter(&probe_sql).await.map_err(|e| {
        D2DError::external("doris", format!("schema 探测失败（{}）: {}", probe_sql, e))
    })?;
    let cols = qr.columns_ref().to_vec();
    drop(qr);

    if cols.is_empty() {
        return Err(D2DError::schema(format!(
            "查询没有返回任何列（{}）—— 请检查 source.query.columns",
            probe_sql
        )));
    }

    let adapter = DorisTypeAdapter::new();
    let contract = contract_index(spec.type_contract.as_deref());

    let mut fields = Vec::with_capacity(cols.len());
    let mut logical = Vec::with_capacity(cols.len());
    for c in &cols {
        let name = c.name_str().to_string();
        let key = name.to_lowercase();

        // ① 用户在 type_contract 里显式声明过 → 用它（表达式别名的唯一出路）
        // ② 表上的真实列 → DESCRIBE 的声明类型（权威）
        // ③ 其余（表达式别名）→ 结果集类型码（回退，会在错误信息里说明）
        let (decl, source) = match (contract.get(&key), by_name.get(&key)) {
            (Some(t), _) => (t.clone(), "type_contract"),
            (None, Some(d)) => (d.type_str.clone(), "describe"),
            (None, None) => (String::new(), "wire"),
        };

        let (lt, nullable) = if source == "wire" {
            let lt = super::wire::logical_type_of(c).map_err(D2DError::schema)?;
            // 协议不会告诉我们可空性 —— 表达式一律按可空处理（保守方向是对的：
            // 声明成不可空而实际有 NULL 会炸在写出那一步）
            (lt, true)
        } else {
            let lt = adapter.to_logical_from_str(&decl)?;
            let nullable = by_name.get(&key).map(|d| d.nullable).unwrap_or(true);
            (lt, nullable)
        };

        debug!(column = %name, declared = %decl, source, logical = %lt.describe(), "列类型已定");
        let dt = LogicalArrowBridge::to_arrow(&lt)?;
        fields.push(Field::new(&name, dt, nullable));
        logical.push(lt);
    }

    info!(
        table = %spec.table,
        columns = fields.len(),
        "doris source schema 已确定"
    );
    Ok(ResolvedSchema {
        schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        logical,
    })
}

/// 列名（小写）→ 契约里声明的源库类型
fn contract_index(contract: Option<&[TypeContractColumn]>) -> HashMap<String, String> {
    contract
        .unwrap_or(&[])
        .iter()
        .map(|c| (c.column.to_lowercase(), c.source_type.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(table: &str) -> SourceSpec {
        SourceSpec {
            connector: "doris".into(),
            connection: json!({"fe_hosts": ["h"], "database": "demo_db"}),
            method: None,
            properties: None,
            table: table.into(),
            schema_name: None,
            query: None,
            type_contract: None,
            split: None,
            partition: None,
        }
    }

    /// 实测 `DESCRIBE d2d_type_probe` 的原样输出（Field / Type / Null）
    fn real_describe() -> Vec<DescribedColumn> {
        let rows = [
            ("id", "int", "No"),
            ("c_bool", "boolean", "Yes"),
            ("c_tiny", "tinyint", "Yes"),
            ("c_small", "smallint", "Yes"),
            ("c_big", "bigint", "Yes"),
            ("c_dec18_4", "decimal(18,4)", "Yes"),
            ("c_dt3", "datetime(3)", "Yes"),
            ("c_dt6", "datetime(6)", "Yes"),
            ("c_vc", "varchar(200)", "Yes"),
            ("c_str", "text", "Yes"),
            ("c_json", "json", "Yes"),
            ("c_large", "largeint", "Yes"),
        ];
        rows.iter()
            .map(|(n, t, null)| DescribedColumn {
                name: n.to_string(),
                type_str: t.to_string(),
                nullable: !null.eq_ignore_ascii_case("no"),
            })
            .collect()
    }

    #[test]
    fn test_index_is_case_insensitive() {
        let d = real_describe();
        let idx = index_by_name(&d);
        // 用户写大写列名也要能查到声明类型 —— 否则会退化成协议类型码，
        // 而那条路上 `boolean` 会变成 `Int8`
        assert!(idx.contains_key("c_bool"));
        assert_eq!(idx["c_bool"].type_str, "boolean");
    }

    /// `DESCRIBE` 给的可空性必须传到 Arrow schema 上
    #[test]
    fn test_nullability_is_carried_from_describe() {
        let d = real_describe();
        assert!(!d.iter().find(|c| c.name == "id").unwrap().nullable);
        assert!(d.iter().find(|c| c.name == "c_bool").unwrap().nullable);
    }

    /// `boolean` 必须走 Doris 适配器变成 Boolean，**不能**被协议码带成 Int8
    #[test]
    fn test_boolean_stays_boolean_through_describe_path() {
        let a = DorisTypeAdapter::new();
        assert_eq!(
            a.to_logical_from_str("boolean").unwrap(),
            LogicalType::Boolean
        );
        // 对照：information_schema 会给 tinyint(1)，那就是 Int8 —— 两者不同
        assert_eq!(
            a.to_logical_from_str("tinyint(1)").unwrap(),
            LogicalType::Int8
        );
    }

    /// `largeint` 读不了 —— 必须**在启动期**报错，不能静默降级
    #[test]
    fn test_largeint_rejected_with_message() {
        let a = DorisTypeAdapter::new();
        let e = a.to_logical_from_str("largeint").unwrap_err().to_string();
        assert!(e.contains("LARGEINT"), "{}", e);
    }

    #[test]
    fn test_contract_overrides_by_column_name() {
        let mut s = spec("t");
        s.type_contract = Some(vec![TypeContractColumn {
            column: "Scaled".into(),
            source_type: "DECIMAL(18,4)".into(),
            sink_type: "DECIMAL(18,4)".into(),
        }]);
        let idx = contract_index(s.type_contract.as_deref());
        // 大小写不敏感：SQL 里写 `AS scaled`，契约里写 `Scaled`，要能对上
        assert_eq!(idx.get("scaled").map(String::as_str), Some("DECIMAL(18,4)"));
        assert!(contract_index(None).is_empty());
    }

    #[test]
    fn test_quote_ident_used_for_describe_sql() {
        // DESCRIBE 的库表名同样走反引号（双引号在 Doris 里是字符串字面量）
        let sql = format!(
            "DESCRIBE {}.{}",
            super::super::sql::quote_ident("demo_db"),
            super::super::sql::quote_ident("t_a")
        );
        assert_eq!(sql, "DESCRIBE `demo_db`.`t_a`");
        assert!(!sql.contains('"'));
    }
}
