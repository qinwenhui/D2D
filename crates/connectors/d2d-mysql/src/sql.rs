//! MySQL SQL 方言：反引号、分区谓词渲染、查询组装
//!
//! 与 Doris 那份同源（都走 MySQL 语义），差别在**哈希分片的函数**：
//! MySQL 有 `CRC32()`，没有 Doris 那种 `HASH()`；而 `CAST(... AS STRING)` 是 Doris 的写法，
//! MySQL 要用 `CAST(... AS CHAR)`。
//!
//! # 为什么不能复用 `d2d_connector::quote_ident`
//!
//! 那个是双引号。MySQL 默认 `sql_mode` 里双引号是**字符串字面量**（`ANSI_QUOTES`
//! 打开才是标识符），`SELECT "id"` 返回的是常量 `'id'`：行数对、列数对、数据全错。

use d2d_connector::partition::{Literal, PartitionPredicate};
use d2d_connector::{QueryColumn, QueryConfig};
use d2d_core::D2DResult;

/// MySQL 标识符引号（内嵌反引号翻倍）
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// 字符串字面量
///
/// 单引号翻倍是标准写法；反斜杠也翻倍 —— MySQL 的默认 `sql_mode` 把 `\` 当转义符，
/// 不翻倍的话值里的 `\n` 会变成真换行。代价是在 `NO_BACKSLASH_ESCAPES` 模式下
/// 反斜杠会被多留一个 —— 那种模式对本项目要写的分区边界（数字/日期）没有影响。
fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// 内核类型化字面量 → MySQL SQL 字面量
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

/// SELECT 列表里的一列 → MySQL SQL 片段
pub fn column_sql(c: &QueryColumn) -> String {
    match c {
        QueryColumn::Name(n) => quote_ident(n),
        QueryColumn::Expr { expr, alias } => format!("{} AS {}", expr, quote_ident(alias)),
    }
}

/// 内核结构化谓词 → MySQL SQL 片段（不含 `WHERE`）
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
        // MySQL 有 `CRC32()`。`COALESCE` 不能省：NULL 参与 `MOD(...) = i` 求值为 NULL，
    // 四个桶都不收 —— 可空列上的 NULL 行会被静默漏掉（Doris 那边实测过）。
        PartitionPredicate::Hash {
            column,
            modulo,
            remainder,
        } => format!(
            "MOD(CRC32(COALESCE(CAST({} AS CHAR), '')), {}) = {}",
            quote_ident(column),
            modulo,
            remainder
        ),
    }
}

/// MySQL SQL 生成器
pub struct MysqlSqlBuilder<'a> {
    pub database: Option<&'a str>,
    pub table: &'a str,
}

impl<'a> MysqlSqlBuilder<'a> {
    pub fn new(database: Option<&'a str>, table: &'a str) -> Self {
        Self { database, table }
    }

    pub fn qualified_table(&self) -> String {
        match self.database {
            Some(d) => format!("{}.{}", quote_ident(d), quote_ident(self.table)),
            None => quote_ident(self.table),
        }
    }

    fn select_list(&self, query: &QueryConfig) -> String {
        match &query.columns {
            Some(cols) if !cols.is_empty() => {
                cols.iter().map(column_sql).collect::<Vec<_>>().join(",\n  ")
            }
            _ => "*".to_string(),
        }
    }

    pub fn build_query(&self, query: &QueryConfig, split_condition: &str) -> String {
        let mut sql = format!(
            "SELECT\n  {}\nFROM {}",
            self.select_list(query),
            self.qualified_table()
        );
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
            "SELECT MIN({c}) AS min_v, MAX({c}) AS max_v FROM {t}",
            c = quote_ident(column),
            t = self.qualified_table()
        )
    }

    /// 分区列必须出现在 SELECT 列表里
    pub fn ensure_partition_column(
        &self,
        query: &QueryConfig,
        column: &str,
    ) -> D2DResult<QueryConfig> {
        let Some(columns) = &query.columns else {
            return Ok(query.clone());
        };
        let has = columns.iter().any(|c| {
            c.output_name() == column || matches!(c, QueryColumn::Name(n) if n == column)
        });
        if has {
            return Ok(query.clone());
        }
        let mut cols = columns.clone();
        cols.push(QueryColumn::Name(column.to_string()));
        let mut q = query.clone();
        q.columns = Some(cols);
        Ok(q)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn b() -> MysqlSqlBuilder<'static> {
        MysqlSqlBuilder::new(Some("db"), "t_a")
    }

    #[test]
    fn test_identifiers_use_backticks() {
        assert_eq!(quote_ident("id"), "`id`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
        assert!(!quote_ident("id").contains('"'));
        assert_eq!(b().qualified_table(), "`db`.`t_a`");
    }

    #[test]
    fn test_column_sql_never_emits_double_quotes() {
        assert_eq!(column_sql(&QueryColumn::Name("id".into())), "`id`");
        let e = QueryColumn::Expr { expr: "`amt` * 1.1".into(), alias: "scaled".into() };
        assert_eq!(column_sql(&e), "`amt` * 1.1 AS `scaled`");
    }

    #[test]
    fn test_literals() {
        assert_eq!(sql_literal_of(&Literal::Int(-7)), "-7");
        assert_eq!(sql_literal_of(&Literal::Float(3.5)), "3.5");
        assert_eq!(sql_literal_of(&Literal::Float(3.0)), "3.0");
        assert_eq!(sql_literal_of(&Literal::Text("AB'C".into())), "'AB''C'");
        assert_eq!(sql_literal_of(&Literal::Text(r"a\b".into())), r"'a\\b'");
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
            upper_inclusive: false,
        };
        assert_eq!(
            render_partition_predicate(&p),
            "`created_at` >= '2022-01-01' AND `created_at` < '2022-02-01'"
        );
        assert_eq!(render_partition_predicate(&PartitionPredicate::All), "");
    }

    /// 哈希分片必须用 MySQL 的 `CRC32` + `AS CHAR`，并且带 COALESCE
    #[test]
    fn test_hash_predicate() {
        let sql = render_partition_predicate(&PartitionPredicate::Hash {
            column: "user_id".into(),
            modulo: 4,
            remainder: 3,
        });
        assert_eq!(sql, "MOD(CRC32(COALESCE(CAST(`user_id` AS CHAR), '')), 4) = 3");
        assert!(sql.contains("COALESCE"), "可空列会丢 NULL 行：{}", sql);
        // Doris 的 CAST AS STRING 在 MySQL 里不成立
        assert!(!sql.contains("AS STRING"), "{}", sql);
    }

    #[test]
    fn test_build_query() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("id".into())]),
            where_clause: Some("`status` = 1".into()),
            order_by: Some("`id` ASC".into()),
            limit: Some(10),
        };
        let sql = b().build_query(&q, "`id` >= 1");
        assert_eq!(
            sql,
            "SELECT\n  `id`\nFROM `db`.`t_a`\nWHERE (`status` = 1)\n  AND (`id` >= 1)\nORDER BY `id` ASC\nLIMIT 10"
        );
        assert!(!sql.contains('"'));
    }

    #[test]
    fn test_min_max_and_partition_column() {
        assert_eq!(
            b().build_min_max("created_at"),
            "SELECT MIN(`created_at`) AS min_v, MAX(`created_at`) AS max_v FROM `db`.`t_a`"
        );
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("name".into())]),
            ..Default::default()
        };
        let fixed = b().ensure_partition_column(&q, "id").unwrap();
        assert_eq!(fixed.columns.unwrap().len(), 2);
        assert!(b().ensure_partition_column(&QueryConfig::default(), "id").unwrap().columns.is_none());
    }
}
