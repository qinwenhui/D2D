//! **接线处：这个文件是全仓库唯一"引擎知道有哪些连接器"的地方**
//!
//! # 新增一个连接器的完整代价
//!
//! 1. 新建 `crates/connectors/<库名>/`（照 `d2d-dameng` / `d2d-doris` 的骨架摆，
//!    详见 `docs` 里那份「照做清单」）
//! 2. 在那个 crate 里写 `pub fn register(reg: &ConnectorRegistry)`
//! 3. **本文件加一行**调用它 + `Cargo.toml` 加一条依赖
//!
//! `main.rs` 不需要改 —— 它只调用 [`register_all`]。
//!
//! # 为什么不是"零改动"
//!
//! Rust 是静态链接，没有稳定 ABI。要么显式写这一行，要么引 `inventory` 之类的
//! 链接期注册库做隐式登记。选显式，因为：
//!
//! - 名字与实现**在同一个地方**能看全（`inventory` 的登记散落在各 crate 里，
//!   想知道"这台机器上到底编进了哪几个连接器"得去读链接结果）
//! - 少一个依赖，少一层"为什么没生效"的可能性
//!
//! DataFusion / Polars 也是这个路子：编译期插件 + 一处显式注册。

use d2d_connector::ConnectorRegistry;
use tracing::info;

/// 把本二进制**编进去的**所有连接器登记进注册表
///
/// 加一行 = 多支持一个库。删一行 = 这个库不再参与（配置里写了它会在启动期
/// 报"未注册"并列出已注册的名单，不会静默退化）。
pub fn register_all(reg: &ConnectorRegistry) {
    d2d_dameng::register(reg);
    d2d_doris::register(reg);
    d2d_mysql::register(reg);
    d2d_postgres::register(reg);
    info!("已注册的连接器：{}", reg.describe());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 二进制里编进去的连接器必须真的能查到 —— 这条挡住"加了 register 却写错名字"
    #[test]
    fn test_all_builtin_connectors_are_registered() {
        let reg = ConnectorRegistry::new();
        register_all(&reg);
        // 达梦两个方向都在：reader（source/）与 writer（sink/）
        // Doris 现在也两个方向都在：读走 FE 的 MySQL 协议口（source/），写走 Stream Load（sink/）
        assert_eq!(
            reg.list_sources(),
            vec!["dameng", "doris", "mysql", "postgres"]
        );
        assert_eq!(
            reg.list_sinks(),
            vec!["dameng", "doris", "mysql", "postgres"]
        );
        assert_eq!(
            reg.list_type_adapters(),
            vec!["dameng", "doris", "mysql", "postgres"]
        );
    }

    /// **机械检查："加连接器不改 CLI"这条承诺有没有被破坏**
    ///
    /// `main.rs` 里不许出现任何连接器 crate 的**代码**引用（注释里提到名字没关系，
    /// 那是在解释历史）。曾经它有两处硬伤：为了算内存账 `DamengSourceConnector::new()`
    /// 造探针、收尾时调具体类型的 `cleanup()` / `exported_rows()` —— 两处都让
    /// "换个源"变成"改 CLI"。
    ///
    /// 这条测试不检查"能不能跑通"，只检查**那条依赖边还在不在**。人一不留神就会
    /// 在 main.rs 里手滑 import 一个具体类型，测试比评审可靠。
    #[test]
    fn test_main_rs_does_not_reference_any_connector_crate() {
        // 只留非注释行：`//`、`///`、`//!` 开头的都排除
        let code: String = include_str!("main.rs")
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        for forbidden in [
            "d2d_dameng",
            "d2d_doris",
            "DamengSourceConnector",
            "DorisSinkConnector",
            "DamengTypeAdapter",
            "DorisTypeAdapter",
        ] {
            assert!(
                !code.contains(forbidden),
                "main.rs 里出现了连接器专有名字 `{}` —— 这会让新增连接器必须改 CLI。请挪到 connectors.rs 或连接器自己的 register() 里。",
                forbidden
            );
        }
    }
}
