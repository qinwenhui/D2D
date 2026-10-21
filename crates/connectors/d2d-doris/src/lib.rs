//! Doris 连接器
//!
//! # 这个 crate 的骨架 —— 每个连接器都照这个摆
//!
//! ```text
//! src/
//! ├── lib.rs       只做模块声明与对外导出（不放实现）
//! ├── config.rs    本库私有的连接配置（FE/BE 地址、库表、格式参数）+ BE 自动发现
//! ├── types.rs     本库类型 ↔ 内核逻辑类型的适配器
//! ├── sink/        **writer 方向**：往这个库写
//! │   ├── mod.rs        SinkConnector 实现
//! │   ├── session.rs    逻辑面写入会话（Arrow → CSV）
//! │   └── physical.rs   直通面写入会话（原始字节 → HTTP body）
//! └── source/      **reader 方向**：读这个库
//!     ├── mod.rs        SourceConnector 实现
//!     ├── conn.rs       连 FE 的 MySQL 协议口（建连选项 + 连接池）
//!     ├── schema.rs     权威 schema：`DESCRIBE` + 结果集元数据
//!     ├── wire.rs       MySQL 协议类型码 → 逻辑类型（只对表达式别名的回退）
//!     └── sql.rs        Doris SQL 方言：反引号、分区谓词渲染、查询组装
//! ```
//!
//! 加一个新的连接器 = **新建一个这样的目录**。类型适配器并进 `types.rs`
//! （它曾经是本仓库里的独立 crate `doris-types`），因为一个数据库只该占一个 crate。
//!
//! # 两个方向都在，但用的是**两个口**
//!
//! | 方向 | 走哪个口 | 缺省端口 |
//! |---|---|---|
//! | 写（Stream Load） | FE 的 **HTTP** 口（`http_port`），可直连 BE（`be_http_port`） | 8030 / 8040 |
//! | 读（查询） | FE 的 **MySQL 协议**口（`query_port`） | 9030 |
//!
//! 读不走 HTTP：Doris 的 REST 查询口在 2.x 已不保证存在，且拉不了大结果集、没有游标。
//!
//! # 连接配置为什么单独一个文件
//!
//! 它曾经和 [`config::StreamLoadConfig`] 一起塞在 `stream_load.rs` 里 ——
//! 那个文件名说的是协议，装的却是配置，找东西的人得先猜。现在读写共用
//! [`config::DorisConnection::from_json`]：两个口在同一段配置里，各解析一遍
//! 必然会在"少填一个字段时报什么错"这种地方分岔。

pub mod config;
pub mod sink;
pub mod source;
pub mod types;

pub use config::{DorisConnection, StreamLoadConfig};
pub use sink::physical::DorisPhysicalSession;
pub use sink::DorisSinkConnector;
pub use source::DorisSourceConnector;
pub use types::DorisTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

/// 把本 crate 提供的东西**一次登记完**
///
/// 这是引擎与本 crate 之间的**唯一**接线点：新增/删除能力都只改这里，
/// 引擎侧（`d2d-cli`）不认识 `DorisSinkConnector` 这个名字。
pub fn register(reg: &ConnectorRegistry) {
    reg.register_sink_factory(SinkFactory::new("doris", || {
        Box::new(DorisSinkConnector::new())
    }));
    // 读方向：经 FE 的 MySQL 协议口（`source/`），见 crate 文档
    reg.register_source_factory(SourceFactory::new("doris", || {
        Box::new(DorisSourceConnector::new())
    }));
    reg.register_type_adapter(TypeAdapterFactory::new("doris", || {
        Box::new(DorisTypeAdapter::new())
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 注册用的名字必须与连接器自报的名字一致 —— 否则引擎按 `sink.connector: doris`
    /// 查表会查不到，而错误只会在跑任务时才出现。
    #[test]
    fn test_register_names_match_connector_names() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert!(reg.sink_factory("doris").is_ok());
        assert!(reg.source_factory("doris").is_ok());
        assert!(reg.type_adapter("doris").is_ok());
        assert_eq!(reg.list_sinks(), vec!["doris"]);
    }

    /// 登记出来的实例自报的名字也得是 `doris` —— 查得到工厂、名字对不上，
    /// 一样要到跑任务时才发现
    #[test]
    fn test_registered_source_reports_same_name() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        let f = reg.source_factory("doris").unwrap();
        let c = f.build();
        assert_eq!(c.name(), "doris");
        assert_eq!(reg.list_sources(), vec!["doris"]);
    }
}
