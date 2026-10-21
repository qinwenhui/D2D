use serde::{Deserialize, Serialize};

/// 查询配置（Source 查询侧，显式下推声明）
///
/// 引擎只做字符串级 SQL 组装，不理解表达式语义。
/// `where` / 表达式内容是"原样 SQL 片段"，由任务作者负责正确性（与 DataX `where` 参数语义一致）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryConfig {
    /// SELECT 列表；缺省 = `SELECT *`
    ///
    /// 支持纯列名字符串或带表达式的结构化形式：
    /// ```yaml
    /// columns:
    ///   - "id"
    ///   - expr: "\"amount\" * 1.1"
    ///     alias: "scaled_amount"
    /// ```
    pub columns: Option<Vec<QueryColumn>>,

    /// WHERE 片段（原样 SQL，不含 `WHERE` 关键字）
    ///
    /// 引擎会将其与 split 分区条件用 `AND` 组合：
    /// `WHERE (用户where) AND (分区条件)`
    pub where_clause: Option<String>,

    /// ORDER BY 片段（原样 SQL，不含 `ORDER BY` 关键字）
    pub order_by: Option<String>,

    /// LIMIT 行数
    pub limit: Option<u64>,
}

impl QueryConfig {
    /// 是否配置了任何查询约束
    pub fn is_empty(&self) -> bool {
        self.columns.is_none()
            && self.where_clause.is_none()
            && self.order_by.is_none()
            && self.limit.is_none()
    }

    /// 提取所有引用的裸列名（用于校验：这些列必须在查询输出中）
    pub fn referenced_columns(&self) -> Vec<String> {
        let mut cols = Vec::new();
        if let Some(columns) = &self.columns {
            for c in columns {
                match c {
                    QueryColumn::Name(n) => cols.push(n.clone()),
                    QueryColumn::Expr { alias, .. } => cols.push(alias.clone()),
                }
            }
        }
        cols
    }
}

/// SELECT 列表中的一列
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryColumn {
    /// 纯列名
    Name(String),
    /// 数据库侧表达式 + 输出别名
    Expr {
        /// 原样 SQL 表达式（标识符建议自带引号）
        expr: String,
        /// 输出列名（必填，作为读回后的列名）
        alias: String,
    },
}

impl QueryColumn {
    /// 该列读回后的输出列名
    pub fn output_name(&self) -> &str {
        match self {
            QueryColumn::Name(n) => n,
            QueryColumn::Expr { alias, .. } => alias,
        }
    }

    /// 渲染为 SQL SELECT 片段
    pub fn to_sql(&self) -> String {
        match self {
            QueryColumn::Name(n) => quote_ident(n),
            QueryColumn::Expr { expr, alias } => {
                format!("{} AS {}", expr, quote_ident(alias))
            }
        }
    }
}

/// 默认标识符引号风格：双引号（达梦 / PostgreSQL / Oracle 兼容）
pub fn quote_ident(name: &str) -> String {
    // 防止标识符内嵌双引号导致注入
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_column_name() {
        let col = QueryColumn::Name("id".to_string());
        assert_eq!(col.output_name(), "id");
        assert_eq!(col.to_sql(), "\"id\"");
    }

    #[test]
    fn test_query_column_expr() {
        let col = QueryColumn::Expr {
            expr: "\"amount\" * 1.1".to_string(),
            alias: "scaled".to_string(),
        };
        assert_eq!(col.output_name(), "scaled");
        assert_eq!(col.to_sql(), "\"amount\" * 1.1 AS \"scaled\"");
    }

    #[test]
    fn test_quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn test_referenced_columns() {
        let q = QueryConfig {
            columns: Some(vec![
                QueryColumn::Name("id".to_string()),
                QueryColumn::Expr {
                    expr: "'x'".to_string(),
                    alias: "const_col".to_string(),
                },
            ]),
            ..Default::default()
        };
        assert_eq!(q.referenced_columns(), vec!["id", "const_col"]);
        assert!(!q.is_empty());
    }

    #[test]
    fn test_yaml_deserialize_mixed_columns() {
        let yaml = r#"
columns:
  - "id"
  - expr: "\"amount\" * 2"
    alias: "double_amount"
where_clause: "\"status\" = 1"
limit: 100
"#;
        let q: QueryConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(q.columns.as_ref().unwrap().len(), 2);
        assert_eq!(q.where_clause.as_deref(), Some("\"status\" = 1"));
        assert_eq!(q.limit, Some(100));
    }
}
