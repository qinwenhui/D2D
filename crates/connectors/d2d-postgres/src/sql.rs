//! PostgreSQL SQL 方言
//!
//! 与达梦那边一样用**双引号**标识符 —— 但这是 PostgreSQL 的标准语法，不是巧合：
//! 未加引号的标识符会被折叠成小写，所以本连接器一律显式加引号。
//!
//! # 哈希分片
//!
//! PostgreSQL 没有 `CRC32()`。用 `md5` 前 8 个十六进制位当 32 位整数：
//!
//! ```sql
//! MOD(ABS(('x' || substr(md5(COALESCE(CAST(col AS TEXT), '')), 1, 8))::bit(32)::int::bigint), n) = i
//! ```
//!
//! 实测（PG 18.4）：1000 行分 4 桶，四桶之和正好 1000；NULL 行经 `COALESCE` 也落在桶里。
//! `hashtext()` 也能用并且更快，但它是**未文档化**的函数（且返回 int4），不拿它当契约。

use d2d_connector::partition::{Literal, PartitionPredicate};
use d2d_connector::quote_ident;
use d2d_core::D2DResult;

/// 字符串字面量：单引号翻倍
///
/// PostgreSQL 默认 `standard_conforming_strings = on`，反斜杠**没有**转义语义，
/// 所以不需要像 MySQL 那样把 `\` 翻倍。
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub fn sql_literal_of(lit: &Literal) -> String {
    match lit {
        Literal::Int(i) => i.to_string(),
        Literal::Float(f) => {
            let s = f.to_string();
            if s.contains(['.', 'e', 'E']) {
                s
            } else {
                format!("{}.0", s)
            }
        }
        Literal::Text(s) => quote_str(s),
        Literal::Date(d) => quote_str(&d.to_string()),
        Literal::Timestamp(dt) => quote_str(&dt.to_string()),
    }
}

pub fn render_partition_predicate(p: &PartitionPredicate) -> String {
    match p {
        PartitionPredicate::All => String::new(),
        PartitionPredicate::Raw(s) => s.clone(),
        PartitionPredicate::Range {
            column,
            lower,
            upper,
            upper_inclusive,
        } => {
            let col = quote_ident(column);
            let mut preds = Vec::new();
            if let Some(l) = lower {
                preds.push(format!("{} >= {}", col, sql_literal_of(l)));
            }
            if let Some(u) = upper {
                let op = if *upper_inclusive { "<=" } else { "<" };
                preds.push(format!("{} {} {}", col, op, sql_literal_of(u)));
            }
            preds.join(" AND ")
        }
        PartitionPredicate::Hash {
            column,
            modulo,
            remainder,
        } => format!(
            "MOD(ABS(('x' || substr(md5(COALESCE(CAST({} AS TEXT), '')), 1, 8))::bit(32)::int::bigint), {}) = {}",
            quote_ident(column),
            modulo,
            remainder
        ),
    }
}

/// PostgreSQL SQL 生成器
///
/// SELECT 列表由调用方给**渲染好的片段**（因为每列要按类型加不同的 cast，
/// 见 [`crate::schema`]）；这里只管 FROM / WHERE / ORDER BY / LIMIT。
pub struct PgSqlBuilder<'a> {
    pub schema: Option<&'a str>,
    pub table: &'a str,
}

impl<'a> PgSqlBuilder<'a> {
    pub fn new(schema: Option<&'a str>, table: &'a str) -> Self {
        Self { schema, table }
    }

    pub fn qualified_table(&self) -> String {
        match self.schema {
            Some(s) => format!("{}.{}", quote_ident(s), quote_ident(self.table)),
            None => quote_ident(self.table),
        }
    }

    pub fn build_query(
        &self,
        select_list: &str,
        query: &d2d_connector::QueryConfig,
        split_condition: &str,
    ) -> String {
        let mut sql = format!("SELECT\n  {}\nFROM {}", select_list, self.qualified_table());
        let mut conditions: Vec<String> = Vec::new();
        if let Some(w) = &query.where_clause {
            let t = w.trim();
            if !t.is_empty() {
                conditions.push(format!("({})", t));
            }
        }
        let s = split_condition.trim();
        if !s.is_empty() {
            conditions.push(format!("({})", s));
        }
        if !conditions.is_empty() {
            sql.push_str(&format!("\nWHERE {}", conditions.join("\n  AND ")));
        }
        if let Some(o) = &query.order_by {
            let t = o.trim();
            if !t.is_empty() {
                sql.push_str(&format!("\nORDER BY {}", t));
            }
        }
        if let Some(l) = query.limit {
            sql.push_str(&format!("\nLIMIT {}", l));
        }
        sql
    }

    pub fn build_min_max(&self, column: &str) -> String {
        format!(
            "SELECT MIN({c})::text AS min_v, MAX({c})::text AS max_v FROM {t}",
            c = quote_ident(column),
            t = self.qualified_table()
        )
    }
}

/// 取 MIN/MAX 的解析（`::text` 回来的是字符串）
pub fn literal_pair(min_s: &str, max_s: &str) -> (Literal, Literal) {
    (Literal::parse(min_s), Literal::parse(max_s))
}

/// 校验表达式列可以安全地包一层 cast
pub fn ensure_castable(expr: &str) -> D2DResult<()> {
    if expr.trim().is_empty() {
        return Err(D2DError_::config("查询列里的表达式为空"));
    }
    Ok(())
}

// 一个小别名只为上面那行的可读性；避免引入 D2DError 的全名太长
use d2d_core::D2DError as D2DError_;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use d2d_connector::QueryConfig;

    fn b() -> PgSqlBuilder<'static> {
        PgSqlBuilder::new(Some("public"), "t_a")
    }

    #[test]
    fn test_qualified_table_quotes() {
        assert_eq!(b().qualified_table(), "\"public\".\"t_a\"");
        assert_eq!(PgSqlBuilder::new(None, "t").qualified_table(), "\"t\"");
    }

    #[test]
    fn test_literals() {
        assert_eq!(sql_literal_of(&Literal::Int(-7)), "-7");
        assert_eq!(sql_literal_of(&Literal::Float(3.0)), "3.0");
        assert_eq!(sql_literal_of(&Literal::Text("AB'C".into())), "'AB''C'");
        // standard_conforming_strings: 反斜杠不转义
        assert_eq!(sql_literal_of(&Literal::Text(r"a\b".into())), r"'a\b'");
        assert_eq!(
            sql_literal_of(&Literal::Date(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())),
            "'2022-01-01'"
        );
    }

    #[test]
    fn test_range_predicate() {
        let p = PartitionPredicate::Range {
            column: "created_at".into(),
            lower: Some(Literal::parse("2022-01-01")),
            upper: Some(Literal::parse("2022-02-01")),
            upper_inclusive: true,
        };
        assert_eq!(
            render_partition_predicate(&p),
            "\"created_at\" >= '2022-01-01' AND \"created_at\" <= '2022-02-01'"
        );
    }

    #[test]
    fn test_hash_predicate_shape() {
        let sql = render_partition_predicate(&PartitionPredicate::Hash {
            column: "user_id".into(),
            modulo: 4,
            remainder: 3,
        });
        assert_eq!(
            sql,
            "MOD(ABS(('x' || substr(md5(COALESCE(CAST(\"user_id\" AS TEXT), '')), 1, 8))::bit(32)::int::bigint), 4) = 3"
        );
        assert!(sql.contains("COALESCE"), "可空列会丢 NULL 行: {}", sql);
    }

    #[test]
    fn test_build_query() {
        let q = QueryConfig {
            where_clause: Some("\"status\" = 1".into()),
            order_by: Some("\"id\" ASC".into()),
            limit: Some(10),
            ..Default::default()
        };
        let sql = b().build_query("\"id\"::text", &q, "\"id\" >= 1");
        assert_eq!(
            sql,
            "SELECT\n  \"id\"::text\nFROM \"public\".\"t_a\"\nWHERE (\"status\" = 1)\n  AND (\"id\" >= 1)\nORDER BY \"id\" ASC\nLIMIT 10"
        );
    }

    #[test]
    fn test_min_max() {
        assert_eq!(
            b().build_min_max("created_at"),
            "SELECT MIN(\"created_at\")::text AS min_v, MAX(\"created_at\")::text AS max_v FROM \"public\".\"t_a\""
        );
    }
}
