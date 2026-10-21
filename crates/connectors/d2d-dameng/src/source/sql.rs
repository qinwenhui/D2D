use d2d_connector::partition::{Literal, PartitionPredicate};
use d2d_connector::{quote_ident, QueryConfig};
use d2d_core::D2DResult;

/// 内核类型化字面量 → 达梦 SQL 字面量
///
/// 与 [`crate::source::method::dmfldr::partition::sql_literal`] 的分工：那个吃的是**字符串**
/// （来自 dmfldr 的文本输出，只能靠形态猜类型），这个吃的是内核已经解析好的
/// [`Literal`]，类型是确定的。
///
/// 时间一律渲染成 `TIMESTAMP'...'`：达梦的 `DATE` 是 `TIMESTAMP` 的子类型，
/// 用 TIMESTAMP 字面量与时序列比较总是安全的（这条形态已在分片 SQL 上实测）。
pub fn sql_literal_of(lit: &Literal) -> String {
    match lit {
        Literal::Int(i) => i.to_string(),
        Literal::Float(f) => {
            // 保证渲染出小数点，免得 `1.0` 变成 `1` 被当成整数参与运算
            let s = f.to_string();
            if s.contains(['.', 'e', 'E']) {
                s
            } else {
                format!("{}.0", s)
            }
        }
        Literal::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::Date(d) => format!("TIMESTAMP'{} 00:00:00'", d),
        Literal::Timestamp(dt) => format!("TIMESTAMP'{}'", dt),
    }
}

/// 内核结构化谓词 → 达梦 SQL 片段（不含 `WHERE`）
///
/// 谓词是结构化的、渲染是方言相关的，这条边界就在这里：内核不知道双引号，
/// 达梦不知道什么是 `PartitionPlan`。
pub fn render_partition_predicate(p: &PartitionPredicate) -> String {
    match p {
        PartitionPredicate::All => String::new(),
        // list 模式的逃生舱：原样 SQL，作者自负正确性
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
        // DM 没有 `HASH()` 这个函数（实测：`无法解析的成员访问表达式[HASH]`），
        // 但有 Oracle 兼容的 `ORA_HASH(expr, max)`。两参数形态返回 0..=max，
        // 再 MOD 到分片数。
        //
        // `COALESCE(CAST(... AS VARCHAR), '')` 不是装饰：哈希函数对 NULL 的
        // 返回值不可依赖，裸用会让 NULL 行"哪个桶都不进"（Doris 侧踩过同一个坑）。
        // 统一先转字符串再补空串，保证确定、互斥、覆盖。
        PartitionPredicate::Hash {
            column,
            modulo,
            remainder,
        } => format!(
            "MOD(ORA_HASH(COALESCE(CAST({} AS VARCHAR), ''), 4294967295), {}) = {}",
            quote_ident(column),
            modulo,
            remainder
        ),
    }
}

/// 达梦 SQL 生成器
///
/// 只做字符串级组装，不理解表达式语义。
/// 表名/列名自动加双引号；用户 WHERE / 表达式原样拼接（任务作者负责正确性）。
pub struct DamengSqlBuilder<'a> {
    /// 数据库 schema 名（可选）
    pub schema: Option<&'a str>,
    /// 表名
    pub table: &'a str,
}

impl<'a> DamengSqlBuilder<'a> {
    pub fn new(schema: Option<&'a str>, table: &'a str) -> Self {
        Self { schema, table }
    }

    /// 带限定名的表：`"SCHEMA"."TABLE"`
    pub fn qualified_table(&self) -> String {
        match self.schema {
            Some(s) => format!("{}.{}", quote_ident(s), quote_ident(self.table)),
            None => quote_ident(self.table),
        }
    }

    /// SELECT 列表
    fn select_list(&self, query: &QueryConfig) -> String {
        match &query.columns {
            Some(cols) if !cols.is_empty() => cols
                .iter()
                .map(|c| c.to_sql())
                .collect::<Vec<_>>()
                .join(",\n  "),
            _ => "*".to_string(),
        }
    }

    /// 组装完整查询 SQL
    ///
    /// 组合顺序：`SELECT <columns> FROM <table> WHERE (用户条件) AND (分区条件) ORDER BY ... LIMIT ...`
    pub fn build_query(&self, query: &QueryConfig, split_condition: &str) -> String {
        let mut sql = format!(
            "SELECT\n  {}\nFROM {}",
            self.select_list(query),
            self.qualified_table()
        );

        // WHERE：用户条件与分区条件 AND 组合
        let mut conditions: Vec<String> = Vec::new();
        if let Some(w) = &query.where_clause {
            let trimmed = w.trim();
            if !trimmed.is_empty() {
                conditions.push(format!("({})", trimmed));
            }
        }
        let trimmed_split = split_condition.trim();
        if !trimmed_split.is_empty() && !trimmed_split.starts_with("__PENDING_") {
            conditions.push(format!("({})", trimmed_split));
        }

        if !conditions.is_empty() {
            sql.push_str(&format!("\nWHERE {}", conditions.join("\n  AND ")));
        }

        if let Some(order) = &query.order_by {
            let trimmed = order.trim();
            if !trimmed.is_empty() {
                sql.push_str(&format!("\nORDER BY {}", trimmed));
            }
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!("\nLIMIT {}", limit));
        }

        sql
    }

    /// 取 MIN/MAX（range 分片用）
    pub fn build_min_max(&self, column: &str) -> String {
        format!(
            "SELECT MIN({c}) AS min_v, MAX({c}) AS max_v FROM {t}",
            c = quote_ident(column),
            t = self.qualified_table()
        )
    }

    /// 分片列必须可读：若用户指定了 columns 且未含分区列，需要自动补上
    ///
    /// 返回修正后的 columns 列表（缺省 None 表示 SELECT *，无需处理）
    pub fn ensure_split_column(
        &self,
        query: &QueryConfig,
        split_column: &str,
    ) -> D2DResult<QueryConfig> {
        let Some(columns) = &query.columns else {
            return Ok(query.clone()); // SELECT *，天然包含所有列
        };

        let has_split = columns.iter().any(|c| {
            c.output_name() == split_column
                || matches!(c, d2d_connector::QueryColumn::Name(n) if n == split_column)
        });

        if has_split {
            return Ok(query.clone());
        }

        // 自动补充分区列到 SELECT 列表（引擎侧 Project 会把它剔掉）
        let mut new_columns = columns.clone();
        new_columns.push(d2d_connector::QueryColumn::Name(split_column.to_string()));
        let mut new_query = query.clone();
        new_query.columns = Some(new_columns);
        Ok(new_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::QueryColumn;

    fn builder() -> DamengSqlBuilder<'static> {
        DamengSqlBuilder::new(Some("PROD"), "t_a")
    }

    #[test]
    fn test_qualified_table() {
        assert_eq!(builder().qualified_table(), "\"PROD\".\"t_a\"");
        let b = DamengSqlBuilder::new(None, "t_a");
        assert_eq!(b.qualified_table(), "\"t_a\"");
    }

    #[test]
    fn test_build_query_select_star_with_split_only() {
        let q = QueryConfig::default();
        let sql = builder().build_query(&q, "\"id\" >= 1 AND \"id\" < 101");
        assert_eq!(
            sql,
            "SELECT\n  *\nFROM \"PROD\".\"t_a\"\nWHERE (\"id\" >= 1 AND \"id\" < 101)"
        );
    }

    #[test]
    fn test_build_query_user_where_plus_split() {
        let q = QueryConfig {
            where_clause: Some("\"status\" = 1".to_string()),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "\"id\" >= 1");
        assert!(sql.contains("\nWHERE (\"status\" = 1)\n  AND (\"id\" >= 1)"));
    }

    #[test]
    fn test_build_query_columns_with_expr() {
        let q = QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("id".to_string()),
                QueryColumn::Expr {
                    expr: "\"amount\" * 1.1".to_string(),
                    alias: "scaled".to_string(),
                },
            ]),
            order_by: Some("\"id\" ASC".to_string()),
            limit: Some(1000),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "\"id\" < 10");
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"amount\" * 1.1 AS \"scaled\""));
        assert!(sql.contains("ORDER BY \"id\" ASC"));
        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn test_pending_split_condition_skipped() {
        let q = QueryConfig::default();
        let sql = builder().build_query(&q, "__PENDING_RANGE_0__");
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("PENDING"));
    }

    #[test]
    fn test_min_max() {
        assert_eq!(
            builder().build_min_max("id"),
            "SELECT MIN(\"id\") AS min_v, MAX(\"id\") AS max_v FROM \"PROD\".\"t_a\""
        );
    }

    #[test]
    fn test_ensure_split_column_appends_missing() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("name".to_string())]),
            ..Default::default()
        };
        let fixed = builder().ensure_split_column(&q, "id").unwrap();
        let cols = fixed.columns.unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].output_name(), "id");
    }

    #[test]
    fn test_ensure_split_column_no_change_when_present() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("id".to_string())]),
            ..Default::default()
        };
        let fixed = builder().ensure_split_column(&q, "id").unwrap();
        assert_eq!(fixed.columns.unwrap().len(), 1);
    }

    #[test]
    fn test_ensure_split_column_select_star_untouched() {
        let q = QueryConfig::default();
        let fixed = builder().ensure_split_column(&q, "id").unwrap();
        assert!(fixed.columns.is_none());
    }

    // ───────── 内核谓词渲染 ─────────

    #[test]
    fn test_sql_literal_of_rendering() {
        use chrono::NaiveDate;
        assert_eq!(sql_literal_of(&Literal::Int(-7)), "-7");
        assert_eq!(sql_literal_of(&Literal::Float(3.5)), "3.5");
        // 整数形态的浮点必须补上小数点，否则会被当整数比对
        assert_eq!(sql_literal_of(&Literal::Float(3.0)), "3.0");
        assert_eq!(sql_literal_of(&Literal::Text("AB'C".into())), "'AB''C'");
        assert_eq!(
            sql_literal_of(&Literal::Date(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())),
            "TIMESTAMP'2026-01-01 00:00:00'"
        );
        let ts = match Literal::parse("2026-01-01 08:30:00") {
            Literal::Timestamp(dt) => dt,
            other => panic!("{:?}", other),
        };
        assert_eq!(
            sql_literal_of(&Literal::Timestamp(ts)),
            "TIMESTAMP'2026-01-01 08:30:00'"
        );
    }

    #[test]
    fn test_render_predicate_all_is_empty() {
        assert_eq!(render_partition_predicate(&PartitionPredicate::All), "");
    }

    #[test]
    fn test_render_predicate_raw_passthrough() {
        assert_eq!(
            render_partition_predicate(&PartitionPredicate::Raw("A = 1".into())),
            "A = 1"
        );
    }

    fn range_pred(
        lower: Option<Literal>,
        upper: Option<Literal>,
        incl: bool,
    ) -> PartitionPredicate {
        PartitionPredicate::Range {
            column: "CREATED_AT".into(),
            lower,
            upper,
            upper_inclusive: incl,
        }
    }

    #[test]
    fn test_render_predicate_range_half_open_vs_closed() {
        let (lo, up) = (
            Some(Literal::parse("2026-01-01")),
            Some(Literal::parse("2026-02-01")),
        );
        // 半开：段内用 <，保证相邻分片不重叠
        assert_eq!(
            render_partition_predicate(&range_pred(lo.clone(), up.clone(), false)),
            "\"CREATED_AT\" >= TIMESTAMP'2026-01-01 00:00:00' AND \"CREATED_AT\" < TIMESTAMP'2026-02-01 00:00:00'"
        );
        // 闭：只有最后一段用 <=，兜住上界本身
        assert!(render_partition_predicate(&range_pred(lo, up, true))
            .contains("<= TIMESTAMP'2026-02-01"));

        // 单侧无界
        assert_eq!(
            render_partition_predicate(&range_pred(None, Some(Literal::Int(10)), false)),
            "\"CREATED_AT\" < 10"
        );
        assert_eq!(
            render_partition_predicate(&range_pred(Some(Literal::Int(1)), None, false)),
            "\"CREATED_AT\" >= 1"
        );
        // 两侧都无界 = 全表
        assert_eq!(
            render_partition_predicate(&range_pred(None, None, false)),
            ""
        );
    }

    #[test]
    fn test_render_predicate_hash() {
        assert_eq!(
            render_partition_predicate(&PartitionPredicate::Hash {
                column: "USER_ID".into(),
                modulo: 16,
                remainder: 3,
            }),
            "MOD(ORA_HASH(COALESCE(CAST(\"USER_ID\" AS VARCHAR), ''), 4294967295), 16) = 3"
        );
    }

    /// 列名必须走 quote_ident —— 否则分区列名里带引号就是一个注入口子
    #[test]
    fn test_render_predicate_quotes_column_names() {
        let p = PartitionPredicate::Hash {
            column: "a\"b".into(),
            modulo: 2,
            remainder: 0,
        };
        assert_eq!(
            render_partition_predicate(&p),
            "MOD(ORA_HASH(COALESCE(CAST(\"a\"\"b\" AS VARCHAR), ''), 4294967295), 2) = 0"
        );
    }
}
