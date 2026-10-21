//! Doris SQL 方言：标识符引法、分区谓词渲染、查询组装
//!
//! 内核 [`d2d_connector::partition`] 只负责"切法数学"与结构化谓词，
//! 渲染成哪家方言是连接器的事 —— 这个文件就是 Doris 的那一份。
//!
//! # 为什么不能复用 `d2d_connector::quote_ident`
//!
//! 那个是**双引号**（达梦 / PostgreSQL / Oracle 兼容）。Doris 走 MySQL 语义，
//! 双引号是**字符串字面量**，不是标识符：
//!
//! ```sql
//! SELECT "id" FROM t;   -- 返回的是字符串 'id'，每行都是，不是列
//! SELECT `id` FROM t;   -- 才是列
//! ```
//!
//! 实测（192.0.2.10:19030，`d2d_type_probe`）：`SELECT "id"` 三行都是 `id`。
//! 而 `QueryColumn::to_sql()` 用的正是双引号 —— 直接拿它组装 SELECT 列表，
//! 结果是**每列都变成同一个常量字符串、列名还对得上**：行数对、列数对、
//! 退出码 0、数据全错。所以这里自己渲染，用反引号。

use d2d_connector::partition::{Literal, PartitionPredicate};
use d2d_connector::{QueryColumn, QueryConfig};
use d2d_core::D2DResult;

/// Doris 标识符引号：反引号（内嵌反引号靠翻倍转义）
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// 内核类型化字面量 → Doris SQL 字面量
///
/// 时间一律渲染成**带引号的字符串**，靠 Doris 的隐式转换与时间列比较。
/// 实测（同一集群）：`WHERE c_dt >= '2022-01-01 00:00:00'` 与
/// `WHERE c_date >= '2022-01-01'` 都按时间语义比较，不是字符串比较。
/// 不写成 `TIMESTAMP'...'` 是因为那在 Doris 里得写成函数形式 `TIMESTAMP('...')`，
/// 而字符串形态对本项目已有的两种时间列（`DATE` / `DATETIME`）都成立。
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
        // 单引号内的单引号翻倍
        Literal::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Literal::Date(d) => format!("'{}'", d),
        Literal::Timestamp(dt) => format!("'{}'", dt),
    }
}

/// SELECT 列表里的一列 → Doris SQL 片段
///
/// 与 [`QueryColumn::to_sql`] 的唯一差别是**引号**，但那正是上面说的那个坑。
pub fn column_sql(c: &QueryColumn) -> String {
    match c {
        QueryColumn::Name(n) => quote_ident(n),
        // 表达式是用户写的原样 SQL，作者自负正确性（与 `where_clause` 同一契约）；
        // 别名是我们接过来的标识符，必须自己引
        QueryColumn::Expr { expr, alias } => format!("{} AS {}", expr, quote_ident(alias)),
    }
}

/// 内核结构化谓词 → Doris SQL 片段（不含 `WHERE`）
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
        // Doris 没有 `HASH()` 函数（实测：`Can not found function 'HASH'`），
        // 所以用 CRC32 代替 —— 分片只需要"确定 + 互斥 + 覆盖"，不需要是存储哈希。
        //
        // ⚠️ `COALESCE(..., '')` 不是装饰。裸 `MOD(CRC32(CAST(col AS STRING)), n)` 在
        // **可空列**上会把 NULL 行丢进"哪个桶都不是"：`NULL = i` 求值为 NULL，四个桶
        // 全不收。实测（`d2d_type_probe` 3 行、`c_tiny` 可空）：四个桶加起来只有 2 行
        // —— 静默丢 1 行，而且每片自己看都"正常完成"。补上 COALESCE 后 3 行齐。
        PartitionPredicate::Hash {
            column,
            modulo,
            remainder,
        } => format!(
            "MOD(CRC32(COALESCE(CAST({} AS STRING), '')), {}) = {}",
            quote_ident(column),
            modulo,
            remainder
        ),
    }
}

/// Doris SQL 生成器
///
/// 只做字符串级组装，不理解表达式语义。表名/列名自动加反引号；
/// 用户 WHERE / 表达式原样拼接（任务作者负责正确性）。
pub struct DorisSqlBuilder<'a> {
    /// 数据库名（可选）
    pub database: Option<&'a str>,
    /// 表名
    pub table: &'a str,
}

impl<'a> DorisSqlBuilder<'a> {
    pub fn new(database: Option<&'a str>, table: &'a str) -> Self {
        Self { database, table }
    }

    /// 带限定名的表：`` `db`.`table` ``
    pub fn qualified_table(&self) -> String {
        match self.database {
            Some(d) => format!("{}.{}", quote_ident(d), quote_ident(self.table)),
            None => quote_ident(self.table),
        }
    }

    /// SELECT 列表
    fn select_list(&self, query: &QueryConfig) -> String {
        match &query.columns {
            Some(cols) if !cols.is_empty() => cols
                .iter()
                .map(column_sql)
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

        let mut conditions: Vec<String> = Vec::new();
        if let Some(w) = &query.where_clause {
            let trimmed = w.trim();
            if !trimmed.is_empty() {
                conditions.push(format!("({})", trimmed));
            }
        }
        let trimmed_split = split_condition.trim();
        if !trimmed_split.is_empty() {
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

    /// 取 MIN/MAX（range 分片缺边界时探测用）
    pub fn build_min_max(&self, column: &str) -> String {
        format!(
            "SELECT MIN({c}) AS min_v, MAX({c}) AS max_v FROM {t}",
            c = quote_ident(column),
            t = self.qualified_table()
        )
    }

    /// 分区列必须出现在查询输出里（否则无法按它切范围）
    ///
    /// 返回修正后的 columns 列表（缺省 None 表示 `SELECT *`，天然包含所有列）。
    /// 引擎侧的 Project 会把这个自动补上的列再剔掉。
    pub fn ensure_partition_column(
        &self,
        query: &QueryConfig,
        column: &str,
    ) -> D2DResult<QueryConfig> {
        let Some(columns) = &query.columns else {
            return Ok(query.clone()); // SELECT *，天然包含所有列
        };
        // Doris 的列名不区分大小写（MySQL 语义）；这里必须按同一口径比较。
        // 否则 `columns: ["USER_ID"]` + `partition.column: "user_id"` 会被判成"没有
        // 分区列"，再补一个 `user_id` —— 结果集里出现两个同名列，目标端报一个
        // 与根因毫不相干的「个数不匹配」。
        let has = columns.iter().any(|c| {
            c.output_name().eq_ignore_ascii_case(column)
                || matches!(c, QueryColumn::Name(n) if n.eq_ignore_ascii_case(column))
        });
        if has {
            return Ok(query.clone());
        }
        let mut new_columns = columns.clone();
        new_columns.push(QueryColumn::Name(column.to_string()));
        let mut new_query = query.clone();
        new_query.columns = Some(new_columns);
        Ok(new_query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn builder() -> DorisSqlBuilder<'static> {
        DorisSqlBuilder::new(Some("demo_db"), "t_a")
    }

    // ───────── 引号：这一节守的是"每列都变成常量字符串"那个坑 ─────────

    #[test]
    fn test_identifiers_use_backticks_not_double_quotes() {
        assert_eq!(quote_ident("id"), "`id`");
        // 内嵌反引号翻倍
        assert_eq!(quote_ident("a`b"), "`a``b`");
        // **绝不能**出现双引号：Doris 会把它当字符串字面量
        assert!(!quote_ident("id").contains('"'));
    }

    #[test]
    fn test_column_sql_never_emits_double_quotes() {
        let c = QueryColumn::Name("id".into());
        assert_eq!(column_sql(&c), "`id`");
        let e = QueryColumn::Expr {
            expr: "`amt` * 1.1".into(),
            alias: "scaled".into(),
        };
        assert_eq!(column_sql(&e), "`amt` * 1.1 AS `scaled`");
        // 与内核那份（双引号）确实不同 —— 这条差异是有意的
        assert_ne!(column_sql(&c), c.to_sql());
    }

    #[test]
    fn test_qualified_table() {
        assert_eq!(builder().qualified_table(), "`demo_db`.`t_a`");
        assert_eq!(DorisSqlBuilder::new(None, "t_a").qualified_table(), "`t_a`");
    }

    // ───────── 字面量 ─────────

    #[test]
    fn test_sql_literal_of_rendering() {
        assert_eq!(sql_literal_of(&Literal::Int(-7)), "-7");
        assert_eq!(sql_literal_of(&Literal::Float(3.5)), "3.5");
        // 整数形态的浮点必须补小数点
        assert_eq!(sql_literal_of(&Literal::Float(3.0)), "3.0");
        assert_eq!(sql_literal_of(&Literal::Text("AB'C".into())), "'AB''C'");
        assert_eq!(
            sql_literal_of(&Literal::Date(NaiveDate::from_ymd_opt(2022, 1, 1).unwrap())),
            "'2022-01-01'"
        );
        let ts = match Literal::parse("2022-01-01 08:30:00") {
            Literal::Timestamp(dt) => dt,
            other => panic!("{:?}", other),
        };
        assert_eq!(
            sql_literal_of(&Literal::Timestamp(ts)),
            "'2022-01-01 08:30:00'"
        );
    }

    // ───────── 谓词渲染 ─────────

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
            Some(Literal::parse("2022-01-01")),
            Some(Literal::parse("2022-02-01")),
        );
        // 纯日期边界渲染成日期字面量（`Literal::parse("2022-01-01")` 是 Date 不是 Timestamp）。
        // 与 DATETIME 列比较时 Doris 会把它当当日零点 —— 实测成立。
        assert_eq!(
            render_partition_predicate(&range_pred(lo.clone(), up.clone(), false)),
            "`CREATED_AT` >= '2022-01-01' AND `CREATED_AT` < '2022-02-01'"
        );
        // 边界带时刻就渲染成完整时间戳
        let ts = |s: &str| Some(Literal::parse(s));
        assert_eq!(
            render_partition_predicate(&range_pred(
                ts("2022-01-01 08:00:00"),
                ts("2022-02-01 08:00:00"),
                false
            )),
            "`CREATED_AT` >= '2022-01-01 08:00:00' AND `CREATED_AT` < '2022-02-01 08:00:00'"
        );
        assert!(render_partition_predicate(&range_pred(lo, up, true)).contains("<= '2022-02-01"));
        // 单侧无界
        assert_eq!(
            render_partition_predicate(&range_pred(None, Some(Literal::Int(10)), false)),
            "`CREATED_AT` < 10"
        );
        // 两侧都无界 = 全表
        assert_eq!(
            render_partition_predicate(&range_pred(None, None, false)),
            ""
        );
    }

    /// 哈希谓词：CRC32 而不是 HASH，且**必须带 COALESCE**
    ///
    /// 去掉 COALESCE 就是"可空列上静默丢 NULL 行"，实测在 `d2d_type_probe` 上
    /// 3 行只能覆盖到 2 行。
    #[test]
    fn test_render_predicate_hash_uses_crc32_and_covers_nulls() {
        let sql = render_partition_predicate(&PartitionPredicate::Hash {
            column: "USER_ID".into(),
            modulo: 4,
            remainder: 3,
        });
        assert_eq!(
            sql,
            "MOD(CRC32(COALESCE(CAST(`USER_ID` AS STRING), '')), 4) = 3"
        );
        // Doris 没有 HASH() 函数，出现它就是错的
        assert!(!sql.contains("HASH("), "Doris 无 HASH() 函数: {}", sql);
        assert!(
            sql.contains("COALESCE"),
            "缺 COALESCE 会在可空列上丢 NULL 行: {}",
            sql
        );
    }

    /// 列名必须走 quote_ident —— 否则分区列名里带引号就是一个注入口子
    #[test]
    fn test_render_predicate_quotes_column_names() {
        let p = PartitionPredicate::Hash {
            column: "a`b".into(),
            modulo: 2,
            remainder: 0,
        };
        assert!(render_partition_predicate(&p).contains("`a``b`"));
    }

    // ───────── 查询组装 ─────────

    #[test]
    fn test_build_query_select_star_with_split_only() {
        let q = QueryConfig::default();
        let sql = builder().build_query(&q, "`id` >= 1 AND `id` < 101");
        assert_eq!(
            sql,
            "SELECT\n  *\nFROM `demo_db`.`t_a`\nWHERE (`id` >= 1 AND `id` < 101)"
        );
    }

    #[test]
    fn test_build_query_user_where_plus_split() {
        let q = QueryConfig {
            where_clause: Some("`status` = 1".to_string()),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "`id` >= 1");
        assert!(
            sql.contains("\nWHERE (`status` = 1)\n  AND (`id` >= 1)"),
            "{}",
            sql
        );
    }

    /// 完整的查询必须**一个双引号都没有** —— 有就是列会变成常量字符串
    #[test]
    fn test_build_query_is_free_of_double_quotes() {
        let q = QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("id".to_string()),
                QueryColumn::Expr {
                    expr: "`amount` * 1.1".to_string(),
                    alias: "scaled".to_string(),
                },
            ]),
            order_by: Some("`id` ASC".to_string()),
            limit: Some(1000),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "`id` < 10");
        assert!(
            !sql.contains('"'),
            "Doris 会把双引号当字符串字面量: {}",
            sql
        );
        assert!(sql.contains("`id`"));
        assert!(sql.contains("`amount` * 1.1 AS `scaled`"));
        assert!(sql.contains("ORDER BY `id` ASC"));
        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn test_min_max() {
        assert_eq!(
            builder().build_min_max("created_at"),
            "SELECT MIN(`created_at`) AS min_v, MAX(`created_at`) AS max_v FROM `demo_db`.`t_a`"
        );
    }

    // ───────── 分区列补齐 ─────────

    #[test]
    fn test_ensure_partition_column_appends_missing() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("name".to_string())]),
            ..Default::default()
        };
        let fixed = builder().ensure_partition_column(&q, "id").unwrap();
        let cols = fixed.columns.unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].output_name(), "id");
    }

    #[test]
    fn test_ensure_partition_column_no_change_when_present() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("id".to_string())]),
            ..Default::default()
        };
        assert_eq!(
            builder()
                .ensure_partition_column(&q, "id")
                .unwrap()
                .columns
                .unwrap()
                .len(),
            1
        );
    }

    /// **回归**：大小写不同也算同一列 —— 不能再补一个同名列
    ///
    /// 复现：`columns: ["USER_ID"]` + `partition.column: "user_id"` 曾被判定为缺失，
    /// 结果集里出现 `USER_ID` 和 `user_id` 两列；目标端只报「个数不匹配」。
    #[test]
    fn test_ensure_partition_column_is_case_insensitive() {
        let q = QueryConfig {
            columns: Some(vec![QueryColumn::Name("USER_ID".to_string())]),
            ..Default::default()
        };
        let fixed = builder().ensure_partition_column(&q, "user_id").unwrap();
        assert_eq!(fixed.columns.unwrap().len(), 1);
    }

    #[test]
    fn test_ensure_partition_column_select_star_untouched() {
        let q = QueryConfig::default();
        assert!(builder()
            .ensure_partition_column(&q, "id")
            .unwrap()
            .columns
            .is_none());
    }
}
