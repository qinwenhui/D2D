//! 权威 schema：`information_schema.columns` + 一层 `::text` 投影
//!
//! # 为什么读取时把每一列都 cast 成文本
//!
//! PostgreSQL 的扩展查询协议返回**二进制**结果，`tokio-postgres` 会按 Rust 类型做
//! `FromSql` 校验；要接住所有类型就得为每种类型写一份转换（NUMERIC 还要额外 feature）。
//! 而本项目已经有一份**按逻辑类型解析文本**的实现（`d2d_types::text_batch`，三条读取路径共用）。
//! 所以这里统一 `::text`，把值交给那份实现。
//!
//! 两个例外必须在 SQL 里做，因为文本表示不是我们要的形态：
//!
//! | 类型 | SQL 里怎么写 | 为什么 |
//! |---|---|---|
//! | `bytea` | `encode(col,'hex')` | `::text` 会带 `\x` 前缀 |
//! | `timestamptz` | `(col AT TIME ZONE 'UTC')::text` | 直接 `::text` 会带 `+00` 偏移 |
//!
//! `timestamp without time zone` 的文本是 `YYYY-MM-DD HH:MM:SS[.ffffff]`，
//! 与共享解析器的格式一致。

use crate::types::PgTypeAdapter;
use d2d_connector::{QueryColumn, QueryConfig, SourceSpec, TypeContractColumn};
use d2d_core::{D2DError, D2DResult};
use d2d_types::{LogicalArrowBridge, LogicalType, TypeAdapter};
use arrow::datatypes::{Field, SchemaRef};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Client;
use tracing::{debug, info};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedColumn {
    pub name: String,
    /// `data_type`，如 `character varying` / `timestamp without time zone`
    pub data_type: String,
    /// `udt_name`，如 `int4` / `_int4`（数组）/ 枚举类型名
    pub udt_name: String,
    pub char_len: Option<i32>,
    pub num_precision: Option<i32>,
    pub num_scale: Option<i32>,
    pub datetime_precision: Option<i32>,
    pub nullable: bool,
}

pub struct ResolvedSchema {
    pub schema: SchemaRef,
    pub logical: Vec<LogicalType>,
    /// 渲染好的 SELECT 列表（每列已带 `::text` 投影），供每次取数复用
    pub select_list: String,
    /// 与 `schema.fields()` 逐列对齐：这一列是不是 `bytea`（取回来是十六进制串）
    pub bytea_columns: Vec<bool>,
}

/// 查询 information_schema
pub async fn describe_table(
    client: &Client,
    schema: &str,
    table: &str,
) -> D2DResult<Vec<DescribedColumn>> {
    let rows = client
        .query(
            "SELECT column_name, data_type, udt_name, character_maximum_length,
                    numeric_precision, numeric_scale, datetime_precision, is_nullable
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .map_err(|e| D2DError::external("postgres", format!("读 information_schema 失败: {}", e)))?;
    let out: Vec<DescribedColumn> = rows
        .iter()
        .map(|r| DescribedColumn {
            name: r.get::<_, String>(0),
            data_type: r.get::<_, String>(1),
            udt_name: r.get::<_, String>(2),
            char_len: r.get::<_, Option<i32>>(3),
            num_precision: r.get::<_, Option<i32>>(4),
            num_scale: r.get::<_, Option<i32>>(5),
            datetime_precision: r.get::<_, Option<i32>>(6),
            nullable: r.get::<_, String>(7).eq_ignore_ascii_case("yes"),
        })
        .collect();
    if out.is_empty() {
        return Err(D2DError::external(
            "postgres",
            format!(
                "表 \"{}\".\"{}\" 没有任何列 —— 表不存在或当前用户没权限",
                schema, table
            ),
        ));
    }
    Ok(out)
}

/// 声明原文 → 适配器认得的那种写法
fn canonical_type(c: &DescribedColumn) -> String {
    match c.data_type.as_str() {
        "numeric" | "decimal" => match (c.num_precision, c.num_scale) {
            (Some(p), Some(s)) => format!("numeric({},{})", p, s),
            _ => "numeric".to_string(),
        },
        "timestamp without time zone" => match c.datetime_precision {
            Some(p) => format!("timestamp({})", p),
            None => "timestamp".to_string(),
        },
        "timestamp with time zone" => match c.datetime_precision {
            Some(p) => format!("timestamp({}) with time zone", p),
            None => "timestamp with time zone".to_string(),
        },
        "time without time zone" => match c.datetime_precision {
            Some(p) => format!("time({})", p),
            None => "time".to_string(),
        },
        "character varying" => match c.char_len {
            Some(n) => format!("character varying({})", n),
            None => "character varying".to_string(),
        },
        "character" => match c.char_len {
            Some(n) => format!("character({})", n),
            None => "character".to_string(),
        },
        // 数组在 information_schema 里报 data_type=ARRAY，细节在 udt_name（_int4 之类）
        other => other.to_string(),
    }
}

/// 一列在 SELECT 里怎么取
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectKind {
    Plain,
    Bytea,
    TimestampTz,
}

fn select_kind(c: &DescribedColumn) -> SelectKind {
    match c.data_type.as_str() {
        "bytea" => SelectKind::Bytea,
        "timestamp with time zone" => SelectKind::TimestampTz,
        _ => SelectKind::Plain,
    }
}

fn select_item(expr: &str, kind: SelectKind, alias: &str) -> String {
    let e = format!("({})", expr);
    let rendered = match kind {
        SelectKind::Plain => format!("{}::text", e),
        SelectKind::Bytea => format!("encode({}, 'hex')", e),
        SelectKind::TimestampTz => format!("({} AT TIME ZONE 'UTC')::text", e),
    };
    format!("{} AS {}", rendered, d2d_connector::quote_ident(alias))
}

fn contract_index(contract: Option<&[TypeContractColumn]>) -> HashMap<String, String> {
    contract
        .unwrap_or(&[])
        .iter()
        .map(|c| (c.column.to_lowercase(), c.source_type.clone()))
        .collect()
}

/// 解析 schema，并产出可复用的 SELECT 列表
pub async fn resolve_schema(
    client: &Client,
    schema: &str,
    spec: &SourceSpec,
    query: &QueryConfig,
) -> D2DResult<ResolvedSchema> {
    let described = describe_table(client, schema, &spec.table).await?;
    let by_name: HashMap<String, &DescribedColumn> =
        described.iter().map(|c| (c.name.to_lowercase(), c)).collect();
    debug!(table = %spec.table, columns = described.len(), "PostgreSQL 表结构已取到");

    struct Out<'a> {
        name: String,
        item: String,
        /// 表上的真实列（表达式为 None）
        described: Option<&'a DescribedColumn>,
    }
    let mut outs: Vec<Out> = Vec::new();
    match &query.columns {
        Some(cols) if !cols.is_empty() => {
            for c in cols {
                match c {
                    QueryColumn::Name(n) => {
                        let Some(d) = by_name.get(&n.to_lowercase()) else {
                            return Err(D2DError::schema(format!(
                                "查询列 `{}` 不在表 `{}` 里",
                                n, spec.table
                            )));
                        };
                        outs.push(Out {
                            name: n.clone(),
                            item: select_item(&d2d_connector::quote_ident(&d.name), select_kind(d), n),
                            described: Some(d),
                        });
                    }
                    QueryColumn::Expr { expr, alias } => {
                        // 表达式按普通文本投影；返回 bytea/timestamptz 的表达式不支持（见 type_from_oid）
                        outs.push(Out {
                            name: alias.clone(),
                            item: select_item(expr, SelectKind::Plain, alias),
                            described: None,
                        });
                    }
                }
            }
        }
        _ => {
            for d in &described {
                outs.push(Out {
                    name: d.name.clone(),
                    item: select_item(&d2d_connector::quote_ident(&d.name), select_kind(d), &d.name),
                    described: Some(d),
                });
            }
        }
    }

    let select_list = outs.iter().map(|o| o.item.clone()).collect::<Vec<_>>().join(",\n  ");
    let probe_sql = crate::sql::PgSqlBuilder::new(Some(schema), &spec.table)
        .build_query(&select_list, query, "");

    // `prepare` 只解析/规划，不取数 —— 拿结果集类型足够
    let stmt = client.prepare(&probe_sql).await.map_err(|e| {
        D2DError::external("postgres", format!("schema 探测失败（{}）: {}", probe_sql, e))
    })?;
    let cols = stmt.columns();
    if cols.len() != outs.len() {
        return Err(D2DError::schema(format!(
            "结果集列数 {} 与预期 {} 不一致",
            cols.len(),
            outs.len()
        )));
    }

    let adapter = PgTypeAdapter::new();
    let contract = contract_index(spec.type_contract.as_deref());
    let mut fields = Vec::with_capacity(outs.len());
    let mut logical = Vec::with_capacity(outs.len());
    let mut bytea_columns = Vec::with_capacity(outs.len());

    for (i, out) in outs.iter().enumerate() {
        let key = out.name.to_lowercase();
        let (lt, nullable) = if let Some(t) = contract.get(&key) {
            (
                adapter.to_logical_from_str(t)?,
                out.described.map(|d| d.nullable).unwrap_or(true),
            )
        } else if let Some(d) = out.described {
            (adapter.to_logical_from_str(&canonical_type(d))?, d.nullable)
        } else {
            // 表达式别名：只能看协议给的类型 OID
            (type_from_oid(cols[i].type_(), &out.name)?, true)
        };
        debug!(column = %out.name, logical = %lt.describe(), "列类型已定");
        fields.push(Field::new(&out.name, LogicalArrowBridge::to_arrow(&lt)?, nullable));
        logical.push(lt);
        bytea_columns.push(
            out.described
                .map(|d| select_kind(d) == SelectKind::Bytea)
                .unwrap_or(false),
        );
    }

    info!(table = %spec.table, columns = fields.len(), "postgres source schema 已确定");
    Ok(ResolvedSchema {
        schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        logical,
        select_list,
        bytea_columns,
    })
}

/// 表达式列用：PG 类型 OID → 逻辑类型
///
/// 只覆盖能**无损确定**的；`NUMERIC` 因为拿不到精度/标度而拒绝（让用户用 `type_contract`）。
fn type_from_oid(t: &tokio_postgres::types::Type, col: &str) -> D2DResult<LogicalType> {
    use tokio_postgres::types::Type;
    Ok(match *t {
        Type::BOOL => LogicalType::Boolean,
        Type::INT2 => LogicalType::Int16,
        Type::INT4 => LogicalType::Int32,
        Type::INT8 => LogicalType::Int64,
        Type::FLOAT4 => LogicalType::Float32,
        Type::FLOAT8 => LogicalType::Float64,
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UUID | Type::CHAR => {
            LogicalType::Utf8
        }
        Type::DATE => LogicalType::Date32,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => LogicalType::Timestamp {
            precision: d2d_types::TemporalPrecision::Micro,
            tz: None,
        },
        Type::TIME => LogicalType::Time64(d2d_types::TemporalPrecision::Micro),
        Type::BYTEA => LogicalType::LargeBinary,
        Type::JSON | Type::JSONB => LogicalType::Json,
        Type::NUMERIC => {
            return Err(D2DError::schema(format!(
                "表达式列 `{}` 的类型是 NUMERIC，但协议不给精度/标度 —— \
                 请用 `type_contract` 显式声明它的类型（如 DECIMAL(18,4)）",
                col
            )))
        }
        ref other => {
            return Err(D2DError::schema(format!(
                "表达式列 `{}` 的类型 {:?} 无法确定为逻辑类型 —— \
                 请用 `type_contract` 显式声明",
                col, other
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(dt: &str, len: Option<i32>, np: Option<i32>, ns: Option<i32>, dp: Option<i32>) -> DescribedColumn {
        DescribedColumn {
            name: "c".into(),
            data_type: dt.into(),
            udt_name: "x".into(),
            char_len: len,
            num_precision: np,
            num_scale: ns,
            datetime_precision: dp,
            nullable: true,
        }
    }

    /// 实测 information_schema 的写法 → 适配器认得的写法
    #[test]
    fn test_canonical_spellings() {
        assert_eq!(canonical_type(&c("numeric", None, Some(38), Some(10), None)), "numeric(38,10)");
        assert_eq!(
            canonical_type(&c("timestamp without time zone", None, None, None, Some(6))),
            "timestamp(6)"
        );
        assert_eq!(
            canonical_type(&c("timestamp with time zone", None, None, None, Some(3))),
            "timestamp(3) with time zone"
        );
        assert_eq!(
            canonical_type(&c("time without time zone", None, None, None, Some(3))),
            "time(3)"
        );
        assert_eq!(
            canonical_type(&c("character varying", Some(200), None, None, None)),
            "character varying(200)"
        );
        assert_eq!(canonical_type(&c("character", Some(10), None, None, None)), "character(10)");
        assert_eq!(canonical_type(&c("bigint", None, Some(64), Some(0), None)), "bigint");
        assert_eq!(canonical_type(&c("ARRAY", None, None, None, None)), "ARRAY");
    }

    /// 每种 canonical 写法都要能喂进适配器
    #[test]
    fn test_every_canonical_maps() {
        let a = PgTypeAdapter::new();
        for d in [
            c("bigint", None, Some(64), Some(0), None),
            c("integer", None, Some(32), Some(0), None),
            c("smallint", None, Some(16), Some(0), None),
            c("numeric", None, Some(18), Some(4), None),
            c("real", None, Some(24), None, None),
            c("double precision", None, Some(53), None, None),
            c("boolean", None, None, None, None),
            c("character varying", Some(200), None, None, None),
            c("character", Some(10), None, None, None),
            c("text", None, None, None, None),
            c("date", None, None, None, Some(0)),
            c("timestamp without time zone", None, None, None, Some(6)),
            c("timestamp with time zone", None, None, None, Some(6)),
            c("time without time zone", None, None, None, Some(3)),
            c("bytea", None, None, None, None),
            c("json", None, None, None, None),
            c("jsonb", None, None, None, None),
            c("uuid", None, None, None, None),
        ] {
            let canon = canonical_type(&d);
            let lt = a
                .to_logical_from_str(&canon)
                .unwrap_or_else(|e| panic!("{} 映射失败: {}", canon, e));
            assert!(LogicalArrowBridge::to_arrow(&lt).is_ok(), "{}", canon);
        }
    }

    /// `select_item` 收的是**已渲染好的 SQL 表达式**（标识符由调用方加引号）——
    /// 列名不加引号会被 PostgreSQL 折叠成小写，混合大小写的列就会找不到。
    #[test]
    fn test_select_items() {
        assert_eq!(
            select_item("\"id\"", SelectKind::Plain, "id"),
            "(\"id\")::text AS \"id\""
        );
        assert_eq!(
            select_item("\"payload\"", SelectKind::Bytea, "payload"),
            "encode((\"payload\"), 'hex') AS \"payload\""
        );
        assert_eq!(
            select_item("\"created_at\"", SelectKind::TimestampTz, "created_at"),
            "((\"created_at\") AT TIME ZONE 'UTC')::text AS \"created_at\""
        );
        // 用户表达式原样进入
        assert_eq!(
            select_item("MAX(x)", SelectKind::Plain, "m"),
            "(MAX(x))::text AS \"m\""
        );
    }

    #[test]
    fn test_contract_index() {
        let idx = contract_index(Some(&[TypeContractColumn {
            column: "Scaled".into(),
            source_type: "numeric(18,4)".into(),
            sink_type: "numeric(18,4)".into(),
        }]));
        assert_eq!(idx.get("scaled").map(String::as_str), Some("numeric(18,4)"));
    }
}
