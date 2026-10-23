//! qmdb SQL 方言：标识符、字面量、分区谓词渲染、查询组装
//!
//! 内核 [`d2d_connector::partition`] 只负责"切法数学"与结构化谓词，
//! 渲染成哪家方言是连接器的事 —— 这个文件就是 qmdb 的那一份。
//!
//! # 方言要点（DataFusion 系）
//!
//! - **标识符折叠小写**：不带引号的 `PHID` 与 `phid` 等价（服务端开了折叠）。
//!   带引号就成了另一回事 —— DataFusion 的双引号是"精确大小写标识符"，
//!   `"PHID"` 不会折叠、对着全小写的 schema 十有八九解析失败。所以本连接器
//!   **自己生成的标识符一律小写裸名**（并校验字符集），绝不生成带引号的标识符。
//! - **字符串字面量用单引号**，内部单引号翻倍；日期/时间戳按服务端文档口径
//!   渲染成带引号的字符串（`WHERE pdate = '2026-10-21'` 是官方推荐的分区裁剪姿势）。
//! - **哈希切分没有出路**：qmdb 的 SQL 面没有 CRC32/HASH 这类稳定行哈希函数
//!   （DataFusion 不暴露它内部给 GROUP BY 用的哈希器）。哈希分片要的是
//!   "确定 + 互斥 + 覆盖"，随便挑一个近似函数就是在赌"行数对但数据错" ——
//!   这里直接拒绝 `partition.mode: hash`，让用户改 list / range。

use d2d_connector::partition::{Literal, PartitionPredicate};
use d2d_connector::{QueryColumn, QueryConfig};
use d2d_core::{D2DError, D2DResult};

/// qmdb 生成的标识符：**小写裸名**（不引号 —— 引号会绕过服务端的大小写折叠）
///
/// 字符集超出 `[A-Za-z0-9_]` 的标识符不生成（拒绝而不是猜引法）；
/// 真有怪名字，让用户走 `query.columns` 的表达式形式自己写。
pub fn quote_ident(name: &str) -> D2DResult<String> {
    let name = name.trim();
    let lowered = name.to_ascii_lowercase();
    let valid = !lowered.is_empty()
        && lowered.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
        && lowered
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(D2DError::config(format!(
            "标识符 `{name}` 不能作为 qmdb 的裸标识符生成（只支持字母/数字/下划线，\
             且不引号——qmdb 服务端把不带引号的标识符折叠成小写）。\
             怪名字请用 query.columns 的表达式形式自己写引法"
        )));
    }
    Ok(lowered)
}

/// 内核类型化字面量 → qmdb SQL 字面量
///
/// 日期/时间戳渲染成**带引号的字符串**：`WHERE pdate = '2026-10-21'` 是
/// qmdb 官方文档给的分区裁剪姿势，服务端按时间语义比较。
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

/// SELECT 列表里的一列 → qmdb SQL 片段
///
/// 列名走小写裸名；表达式是用户写的原样 SQL，作者自负正确性
/// （与 `where_clause` 同一契约）；别名是我们接过来的标识符，必须自己渲染。
pub fn column_sql(c: &QueryColumn) -> D2DResult<String> {
    match c {
        QueryColumn::Name(n) => Ok(quote_ident(n)?),
        QueryColumn::Expr { expr, alias } => {
            Ok(format!("{} AS {}", expr.trim(), quote_ident(alias)?))
        }
    }
}

/// 内核结构化谓词 → qmdb SQL 片段（不含 `WHERE`）
///
/// hash 在 qmdb 上没有正确实现的路（见模块文档），直接报配置错误。
pub fn render_partition_predicate(p: &PartitionPredicate) -> D2DResult<String> {
    match p {
        PartitionPredicate::All => Ok(String::new()),
        // list 模式的逃生舱：原样 SQL，作者自负正确性
        PartitionPredicate::Raw(s) => Ok(s.clone()),
        PartitionPredicate::Range {
            column,
            lower,
            upper,
            upper_inclusive,
        } => {
            let col = quote_ident(column)?;
            let mut preds = Vec::new();
            if let Some(l) = lower {
                preds.push(format!("{} >= {}", col, sql_literal_of(l)));
            }
            if let Some(u) = upper {
                let op = if *upper_inclusive { "<=" } else { "<" };
                preds.push(format!("{} {} {}", col, op, sql_literal_of(u)));
            }
            Ok(preds.join(" AND "))
        }
        PartitionPredicate::Hash { column, .. } => Err(D2DError::config(format!(
            "qmdb 不支持 partition.mode: hash（SQL 面没有 CRC32/HASH 这类稳定的行哈希函数，\
             硬凑出来的哈希谓词会「行数对、数据错」）。请改用 partition.mode: list / range；\
             range 按分区列（如 pdate）切与 qmdb 的分区裁剪天然配合。column: {}",
            column
        ))),
    }
}

/// qmdb SQL 生成器
///
/// 只做字符串级组装，不理解表达式语义。表名/列名走小写裸名；
/// 用户 WHERE / 表达式原样拼接（任务作者负责正确性）。
pub struct QmdbSqlBuilder<'a> {
    /// 数据库名（可选；缺省时不限定，依赖该连接自己的默认库 —— 不推荐）
    pub database: Option<&'a str>,
    /// 表名
    pub table: &'a str,
}

impl<'a> QmdbSqlBuilder<'a> {
    pub fn new(database: Option<&'a str>, table: &'a str) -> Self {
        Self { database, table }
    }

    /// 带限定名的表：`db.table`（qmdb 多库，同步工具一律全限定 —— 协议文档的建议）
    pub fn qualified_table(&self) -> D2DResult<String> {
        match self.database {
            Some(d) => Ok(format!("{}.{}", quote_ident(d)?, quote_ident(self.table)?)),
            None => quote_ident(self.table),
        }
    }

    /// SELECT 列表
    ///
    /// 显式给出**空**列数组是配置错误，不是 `SELECT *` —— 静默退化会让人以为
    /// 列裁剪生效了，实际迁的是整表。
    fn select_list(&self, query: &QueryConfig) -> D2DResult<String> {
        match &query.columns {
            Some(cols) if cols.is_empty() => Err(D2DError::config(
                "qmdb query.columns 不能为空数组：删掉 columns（= SELECT *）或至少写一列",
            )),
            Some(cols) => {
                let mut parts = Vec::with_capacity(cols.len());
                for c in cols {
                    parts.push(column_sql(c)?);
                }
                Ok(parts.join(", "))
            }
            None => Ok("*".to_string()),
        }
    }

    /// 组装完整查询 SQL
    ///
    /// 组合顺序：`SELECT <columns> FROM <table> WHERE (用户条件) AND (分区条件) ORDER BY ... LIMIT ...`
    pub fn build_query(&self, query: &QueryConfig, split_condition: &str) -> D2DResult<String> {
        let mut sql = format!(
            "SELECT {} FROM {}",
            self.select_list(query)?,
            self.qualified_table()?
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
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }

        if let Some(order) = &query.order_by {
            let trimmed = order.trim();
            if !trimmed.is_empty() {
                sql.push_str(&format!(" ORDER BY {}", trimmed));
            }
        }

        if let Some(limit) = query.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        Ok(sql)
    }

    /// 取 MIN/MAX（range 分片缺边界时探测用）
    pub fn build_min_max(&self, column: &str) -> D2DResult<String> {
        Ok(format!(
            "SELECT MIN({c}) AS min_v, MAX({c}) AS max_v FROM {t}",
            c = quote_ident(column)?,
            t = self.qualified_table()?
        ))
    }

    /// 分区列必须出现在查询输出里（否则无法按它切范围）
    ///
    /// 返回修正后的 columns 列表（缺省 None 表示 `SELECT *`，天然包含所有列）。
    /// 引擎侧的 Project 会把这个自动补上的列再剔掉。
    ///
    /// qmdb 的标识符折叠小写，所以按 ASCII 不区分大小写比较（与服务端口径一致）。
    pub fn ensure_partition_column(
        &self,
        query: &QueryConfig,
        column: &str,
    ) -> D2DResult<QueryConfig> {
        let Some(columns) = &query.columns else {
            return Ok(query.clone()); // SELECT *，天然包含所有列
        };
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

    fn builder() -> QmdbSqlBuilder<'static> {
        QmdbSqlBuilder::new(Some("bench_lc"), "t_a")
    }

    // ───────── 标识符：小写裸名，绝不带引号 ─────────

    #[test]
    fn test_identifiers_are_bare_lowercase() {
        assert_eq!(quote_ident("PHID").unwrap(), "phid");
        assert_eq!(quote_ident("pdate").unwrap(), "pdate");
        assert_eq!(quote_ident("  User_ID ").unwrap(), "user_id");
        // 绝不出现引号 —— 双引号会绕过折叠、反引号不是这个方言的引法
        assert!(quote_ident("A`B").is_err());
        assert!(quote_ident("a\"b").is_err());
    }

    #[test]
    fn test_weird_identifier_is_rejected() {
        // 空格/点号/内嵌引号：拒绝而不是猜
        assert!(quote_ident("a b").is_err());
        assert!(quote_ident("db.t").is_err());
        assert!(quote_ident("").is_err());
        assert!(quote_ident("1abc").is_err());
    }

    #[test]
    fn test_column_sql_lowers_names_keeps_expr() {
        let c = QueryColumn::Name("OrderID".into());
        assert_eq!(column_sql(&c).unwrap(), "orderid");
        let e = QueryColumn::Expr {
            expr: "amount * 1.1".into(),
            alias: "Scaled".into(),
        };
        assert_eq!(column_sql(&e).unwrap(), "amount * 1.1 AS scaled");
    }

    // ───────── 字面量 ─────────

    #[test]
    fn test_sql_literal_of_rendering() {
        assert_eq!(sql_literal_of(&Literal::Int(-7)), "-7");
        assert_eq!(sql_literal_of(&Literal::Float(3.5)), "3.5");
        assert_eq!(sql_literal_of(&Literal::Float(3.0)), "3.0");
        assert_eq!(sql_literal_of(&Literal::Text("AB'C".into())), "'AB''C'");
        assert_eq!(
            sql_literal_of(&Literal::Date(
                NaiveDate::from_ymd_opt(2026, 10, 21).unwrap()
            )),
            "'2026-10-21'"
        );
        let ts = match Literal::parse("2026-10-21 08:30:00") {
            Literal::Timestamp(dt) => dt,
            other => panic!("{:?}", other),
        };
        assert_eq!(
            sql_literal_of(&Literal::Timestamp(ts)),
            "'2026-10-21 08:30:00'"
        );
    }

    // ───────── 谓词渲染 ─────────

    #[test]
    fn test_render_predicate_all_is_empty() {
        assert_eq!(
            render_partition_predicate(&PartitionPredicate::All).unwrap(),
            ""
        );
    }

    #[test]
    fn test_render_predicate_raw_passthrough() {
        assert_eq!(
            render_partition_predicate(&PartitionPredicate::Raw("phid = 1".into())).unwrap(),
            "phid = 1"
        );
    }

    #[test]
    fn test_render_predicate_range() {
        let p = |lower, upper, incl| PartitionPredicate::Range {
            column: "PDATE".into(),
            lower,
            upper,
            upper_inclusive: incl,
        };
        let lo = Some(Literal::parse("2026-10-01"));
        let hi = Some(Literal::parse("2026-10-02"));
        assert_eq!(
            render_partition_predicate(&p(lo.clone(), hi.clone(), false)).unwrap(),
            "pdate >= '2026-10-01' AND pdate < '2026-10-02'"
        );
        assert!(render_partition_predicate(&p(lo, hi, true))
            .unwrap()
            .contains("<= '2026-10-02"));
        // 单侧无界
        assert_eq!(
            render_partition_predicate(&p(None, Some(Literal::Int(10)), false)).unwrap(),
            "pdate < 10"
        );
        // 两侧都无界 = 全表
        assert_eq!(
            render_partition_predicate(&p(None, None, false)).unwrap(),
            ""
        );
    }

    /// 哈希切分必须被拒绝：错误的哈希谓词是"行数对、数据错"
    #[test]
    fn test_render_predicate_hash_is_rejected() {
        let p = PartitionPredicate::Hash {
            column: "user_id".into(),
            modulo: 4,
            remainder: 3,
        };
        let e = render_partition_predicate(&p).unwrap_err().to_string();
        assert!(e.contains("hash"), "{}", e);
        assert!(e.contains("list / range"), "{}", e);
    }

    #[test]
    fn test_render_predicate_quotes_column_names_via_validation() {
        let p = PartitionPredicate::Range {
            column: "a b".into(),
            lower: None,
            upper: Some(Literal::Int(1)),
            upper_inclusive: false,
        };
        assert!(render_partition_predicate(&p).is_err());
    }

    // ───────── 查询组装 ─────────

    #[test]
    fn test_build_query_select_star_with_split_only() {
        let q = QueryConfig::default();
        let sql = builder()
            .build_query(&q, "phid >= 1 AND phid < 101")
            .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM bench_lc.t_a WHERE (phid >= 1 AND phid < 101)"
        );
    }

    #[test]
    fn test_build_query_user_where_plus_split() {
        let q = QueryConfig {
            where_clause: Some("status = 1".to_string()),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "phid >= 1").unwrap();
        assert!(
            sql.contains("WHERE (status = 1) AND (phid >= 1)"),
            "{}",
            sql
        );
    }

    /// 完整的生成 SQL **绝不含双引号**：qmdb 的双引号是精确大小写标识符，
    /// 对着全小写的 schema 会解析失败
    #[test]
    fn test_build_query_is_free_of_quotes() {
        let q = QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("Phid".to_string()),
                QueryColumn::Expr {
                    expr: "amount * 1.1".to_string(),
                    alias: "Scaled".to_string(),
                },
            ]),
            order_by: Some("phid ASC".to_string()),
            limit: Some(1000),
            ..Default::default()
        };
        let sql = builder().build_query(&q, "phid < 10").unwrap();
        assert!(!sql.contains('"'), "qmdb 双引号是精确标识符: {}", sql);
        assert!(!sql.contains('`'), "反引号不是 qmdb 的引法: {}", sql);
        assert!(sql.contains("phid"));
        assert!(sql.contains("amount * 1.1 AS scaled"));
        assert!(sql.contains("ORDER BY phid ASC"));
        assert!(sql.contains("LIMIT 1000"));
    }

    /// 空列数组是配置错误，不是 `SELECT *`
    #[test]
    fn test_empty_columns_is_error() {
        let q = QueryConfig {
            columns: Some(vec![]),
            ..Default::default()
        };
        assert!(builder().build_query(&q, "").is_err());
    }

    #[test]
    fn test_min_max() {
        assert_eq!(
            builder().build_min_max("PDate").unwrap(),
            "SELECT MIN(pdate) AS min_v, MAX(pdate) AS max_v FROM bench_lc.t_a"
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

    /// **回归**：大小写不同也算同一列（qmdb 折叠小写）—— 不能再补一个同名列
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

    #[test]
    fn test_qualified_table_requires_valid_names() {
        assert_eq!(builder().qualified_table().unwrap(), "bench_lc.t_a");
        let b = QmdbSqlBuilder::new(Some("bench-lc"), "t");
        assert!(b.qualified_table().is_err());
    }
}
