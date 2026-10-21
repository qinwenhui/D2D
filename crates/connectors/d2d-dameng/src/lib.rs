//! 达梦（DM8）连接器
//!
//! # 这个 crate 的骨架 —— 每个连接器都照这个摆
//!
//! ```text
//! src/
//! ├── lib.rs       只做模块声明与对外导出（不放实现）
//! ├── config.rs    本库私有的连接配置 + 连接串渲染
//! ├── types.rs     本库类型 ↔ 内核逻辑类型的适配器
//! ├── dpi/         达梦 DPI 的共享 FFI（读、写两个方向共用，无共享状态）
//! ├── source/      **reader 方向**：读这个库
//! │   ├── mod.rs   SourceConnector 实现
//! │   ├── sql.rs   本库 SQL 方言
//! │   └── split.rs 分片边界探测
//! └── sink/        **writer 方向**：往这个库写
//!     └── method/  写入方式路由：odbc（绑定插入）/ dmfldr（IN 快载）
//! ```
//!
//! 加一个新的连接器 = **新建一个这样的目录**。类型适配器并进 `types.rs`
//! （它曾经是本仓库里的独立 crate `dameng-types`），因为一个数据库只该占一个 crate。
//!
//! # 两个方向都在这里
//!
//! 达梦作为**源**有三条读取路径（`odbc` / `dmfldr` / `dpi`）；
//! 作为**目标**有三条写入路径（`odbc` 绑定插入 / `dmfldr` IN 快载 / `dpi` 数组参数绑定）。
//! 两侧共用同一份 `config` / `types` —— 一个数据库只该有一套说法；
//! `dpi` 的底层 FFI 也共用（`src/dpi/`），但读/写会话各管各的句柄与缓冲。
//!
//! # 两条读取路径的公共面
//!
//! 数据库语义层（SQL 方言、类型契约、分片探测）跨方法复用：`sql.rs` 与 `split.rs`
//! 在 `source/` 下与 `method/` 平级，两种方法都直接用它们，而不是各写一份。

//! # 怎么被引擎发现
//!
//! [`register`] —— 引擎只调它，不认识本 crate 里的任何具体类型。
//! 详见 `d2d_connector::factory`。

pub mod config;
/// 达梦 DPI 的共享底层（读、写共用同一份 FFI 声明）
mod dpi;
pub mod sink;
pub mod source;
pub mod types;

pub use config::ConnectionConfig;
pub use sink::DamengSinkConnector;
pub use source::method::validate_dmfldr_read_config;
pub use source::sql::DamengSqlBuilder;
pub use source::DamengSourceConnector;
pub use types::DamengTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

/// 把本 crate 提供的东西**一次登记完**
///
/// 这是引擎与本 crate 之间的**唯一**接线点：新增/删除能力都只改这里，
/// 引擎侧（`d2d-cli`）不认识 `DamengSourceConnector` 这个名字。
///
/// 一个连接器 crate 应该把**它提供的全部东西**都在这里登记 ——
/// source、sink、类型适配器。分散到多处会让"这个库支持什么"没有单一答案。
pub fn register(reg: &ConnectorRegistry) {
    reg.register_source_factory(SourceFactory::new("dameng", || {
        Box::new(DamengSourceConnector::new())
    }));
    reg.register_sink_factory(SinkFactory::new("dameng", || {
        Box::new(DamengSinkConnector::new())
    }));
    reg.register_type_adapter(TypeAdapterFactory::new("dameng", || {
        Box::new(DamengTypeAdapter::new())
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 注册用的名字必须与连接器自报的名字一致 —— 否则引擎按 `source.connector: dameng`
    /// 查表会查不到，而错误只会在跑任务时才出现。
    #[test]
    fn test_register_names_match_connector_names() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert!(reg.source_factory("dameng").is_ok());
        assert!(reg.type_adapter("dameng").is_ok());
        assert_eq!(reg.list_sources(), vec!["dameng"]);
    }
}
