//! QSDB 连接器：通过官方 `qsdb-client` 连接 **server 模式**的 QSDB
//!
//! QSDB 是 **Arrow-native** 的列式库。本连接器只走 HTTP：
//!
//! - 读：`query_stream`（rows 通道，客户端内存恒定）→ Arrow 批
//! - 写：`append_batches`（Arrow IPC）→ 服务端 `Qsdb::append`
//!
//! ```text
//! src/
//! ├── config.rs        连接配置（url / 认证 / 超时）
//! ├── types.rs         启动期 type_contract 用的类型适配器
//! ├── source/mod.rs    SourceConnector（HTTP 查询 → 异步流）
//! └── sink/
//!     ├── mod.rs       SinkConnector（连接 + 建表策略）
//!     └── session.rs   SinkSession（Arrow IPC append，一次调用一次提交）
//! ```
//!
//! # 部署前提
//!
//! 先在目标机器上以 server 模式启动 QSDB：
//!
//! ```text
//! qsdb server --path /var/lib/qsdb --addr 0.0.0.0:9430
//! ```
//!
//! 然后在 `connection.url` 里填 `http://host:9430`。服务是**单库**的，
//! `database` / `schema_name` 只能是 `default`。
//!
//! > 进程内的 embedded 模式依赖未发布的 `qsdb` crate，公开分支不引入；
//! > QSDB 统一以 server 模式部署。

pub mod config;
pub mod sink;
pub mod source;
pub mod types;

pub use config::QsdbConnection;
pub use sink::QsdbSinkConnector;
pub use source::QsdbSourceConnector;
pub use types::QsdbTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

pub fn register(reg: &ConnectorRegistry) {
    reg.register_source_factory(SourceFactory::new("qsdb", || {
        Box::new(QsdbSourceConnector::new())
    }));
    reg.register_sink_factory(SinkFactory::new("qsdb", || {
        Box::new(QsdbSinkConnector::new())
    }));
    reg.register_type_adapter(TypeAdapterFactory::new("qsdb", || {
        Box::new(QsdbTypeAdapter::new())
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_names_match() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert_eq!(reg.list_sources(), vec!["qsdb"]);
        assert_eq!(reg.list_sinks(), vec!["qsdb"]);
        assert_eq!(reg.list_type_adapters(), vec!["qsdb"]);
        assert_eq!(reg.source_factory("qsdb").unwrap().build().name(), "qsdb");
        assert_eq!(reg.sink_factory("qsdb").unwrap().build().name(), "qsdb");
        assert_eq!(reg.type_adapter("qsdb").unwrap().db_name(), "qsdb");
    }
}
