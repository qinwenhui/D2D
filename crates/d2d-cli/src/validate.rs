use d2d_connector::ConnectorRegistry;
use d2d_core::{D2DError, D2DResult};
use d2d_types::{CastPolicy, CastTable, DbTypeDecl, LogicalType};
use tracing::{info, warn};

/// 列级类型契约检查结果
#[derive(Debug, Clone)]
pub struct ColumnCheck {
    pub column: String,
    pub from: LogicalType,
    pub to: LogicalType,
    pub policy: CastPolicy,
}

/// 启动期类型契约校验
///
/// 在没有运行时数据的情况下，用声明的类型做静态推演：
/// - `Forbidden` → 直接失败（fail-fast，防止数据错乱）
/// - `Lossy` → 警告列出（任务作者知情）
/// - `Exact` / `Widen` → 静默通过
pub struct SchemaContractChecker;

impl SchemaContractChecker {
    /// 校验 source 类型声明 → sink 类型声明 的全链路兼容性
    ///
    /// `columns` 为 (列名, 源库类型声明, 目标库类型声明) 列表。
    ///
    /// 适配器从 `registry` **按数据库名查** —— 这里曾经是一个
    /// `match db { "dameng" => ..., "doris" => ... }`，意味着加一个连接器
    /// 要在 CLI 里加一个分支。而"我的类型适配器叫什么"只有连接器自己知道。
    pub fn check(
        registry: &ConnectorRegistry,
        source_db: &str,
        sink_db: &str,
        columns: &[(String, String, String)],
    ) -> D2DResult<Vec<ColumnCheck>> {
        let source_adapter = registry.type_adapter(source_db)?;
        let sink_adapter = registry.type_adapter(sink_db)?;

        let mut checks = Vec::with_capacity(columns.len());
        let mut forbidden = Vec::new();
        let mut lossy = Vec::new();

        for (name, src_decl, dst_decl) in columns {
            let from = source_adapter
                .to_logical(&DbTypeDecl::parse(src_decl))
                .map_err(|e| {
                    D2DError::schema(format!(
                        "列 '{}' 的源类型 '{}' 无法映射为逻辑类型: {}",
                        name, src_decl, e
                    ))
                })?;
            let to = sink_adapter
                .to_logical(&DbTypeDecl::parse(dst_decl))
                .map_err(|e| {
                    D2DError::schema(format!(
                        "列 '{}' 的目标类型 '{}' 无法映射为逻辑类型: {}",
                        name, dst_decl, e
                    ))
                })?;

            let policy = CastTable::policy(&from, &to);
            checks.push(ColumnCheck {
                column: name.clone(),
                from: from.clone(),
                to: to.clone(),
                policy,
            });

            if !policy.is_allowed() {
                forbidden.push(format!(
                    "  - {}: {} → {}（{}）",
                    name,
                    from.describe(),
                    to.describe(),
                    policy.describe()
                ));
            } else if !policy.is_lossless() {
                lossy.push(format!(
                    "  - {}: {} → {}（{}）",
                    name,
                    from.describe(),
                    to.describe(),
                    policy.describe()
                ));
            }
        }

        if !lossy.is_empty() {
            warn!(
                count = lossy.len(),
                "以下列存在有损类型转换（数据可能劣化）:\n{}",
                lossy.join("\n")
            );
        }

        if !forbidden.is_empty() {
            return Err(D2DError::schema(format!(
                "类型契约校验失败，以下列的类型转换被禁止（继续执行将导致数据错乱）:\n{}\n请修正 sink 表结构或在配置中显式处理。",
                forbidden.join("\n")
            )));
        }

        info!(
            columns = checks.len(),
            forbidden = 0,
            lossy = lossy.len(),
            "schema contract check passed"
        );
        Ok(checks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::register_all;

    fn col(name: &str, from: &str, to: &str) -> (String, String, String) {
        (name.to_string(), from.to_string(), to.to_string())
    }

    /// 走**真实注册路径**建注册表，而不是手工 `Box::new(DamengTypeAdapter)`
    /// —— 这样"适配器注册对了没有"也在测试覆盖里（写错名字这里就会红）
    fn reg() -> ConnectorRegistry {
        let r = ConnectorRegistry::new();
        register_all(&r);
        r
    }

    #[test]
    fn test_clean_contract_passes() {
        let cols = vec![
            col("id", "BIGINT", "BIGINT"),
            col("name", "VARCHAR(255)", "VARCHAR(255)"),
            col("amount", "NUMBER(19,4)", "DECIMAL(19,4)"),
        ];
        let checks = SchemaContractChecker::check(&reg(), "dameng", "doris", &cols).unwrap();
        assert_eq!(checks.len(), 3);
        assert!(checks.iter().all(|c| c.policy.is_lossless()));
    }

    #[test]
    fn test_blob_to_doris_forbidden() {
        // 达梦 BLOB → Doris STRING：必须被禁止（这是典型的静默错乱场景）
        let cols = vec![col("payload", "BLOB", "STRING")];
        let err = SchemaContractChecker::check(&reg(), "dameng", "doris", &cols).unwrap_err();
        assert!(err.to_string().contains("禁止"), "应明确禁止: {}", err);
    }

    #[test]
    fn test_decimal_scale_lossy_warns_but_passes() {
        // NUMBER(38,10) → DECIMAL(19,4)：有损但允许（会警告）
        let cols = vec![col("amount", "NUMBER(38,10)", "DECIMAL(19,4)")];
        let checks = SchemaContractChecker::check(&reg(), "dameng", "doris", &cols).unwrap();
        assert!(!checks[0].policy.is_lossless());
    }

    #[test]
    fn test_timestamp_chain() {
        // 达梦 TIMESTAMP → Doris DATETIME：同为"无时区微妙"，无损
        let cols = vec![col("create_time", "TIMESTAMP", "DATETIME")];
        let checks = SchemaContractChecker::check(&reg(), "dameng", "doris", &cols).unwrap();
        assert!(checks[0].policy.is_lossless());
    }

    #[test]
    fn test_unknown_source_type_fails() {
        let cols = vec![col("x", "WEIRD_TYPE", "INT")];
        let err = SchemaContractChecker::check(&reg(), "dameng", "doris", &cols).unwrap_err();
        assert!(err.to_string().contains("WEIRD_TYPE"));
    }

    /// 没注册的库要在**启动期**报错并列出可选项，而不是跑到一半才发现
    #[test]
    fn test_unregistered_db_is_rejected_with_a_list() {
        let cols = vec![col("x", "INT", "INT")];
        // ⚠️ 用**确实没注册**的名字。这条以前写过 "mysql"、后来又写过 "postgres" ——
        // 每接入一个连接器，它就从"验证报错"变成"验证不报错"，当场失败。这就是测试在起作用。
        let err = SchemaContractChecker::check(&reg(), "oracle", "doris", &cols).unwrap_err();
        let s = err.to_string();
        assert!(s.contains("oracle"), "{}", s);
        assert!(s.contains("dameng"), "报错应列出已注册的库: {}", s);
        assert!(s.contains("mysql"), "报错应列出已注册的库: {}", s);
        assert!(s.contains("postgres"), "报错应列出已注册的库: {}", s);
    }
}
