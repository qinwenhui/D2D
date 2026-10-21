//! MySQL 连接器：读（MySQL 协议）与写（多行 INSERT）
//!
//! ```text
//! src/
//! ├── config.rs        连接参数与连接池（读写共用）
//! ├── types.rs         声明类型 ↔ 逻辑类型（`information_schema.COLUMN_TYPE` 喂它）
//! ├── sql.rs           方言：反引号、分区谓词、查询组装
//! ├── wire.rs          协议类型码 → 逻辑类型（只给表达式别名回退）
//! ├── schema.rs        权威 schema：information_schema + LIMIT 0 探测
//! ├── source/mod.rs    SourceConnector 实现（mysql_async 流式读）
//! └── sink/
//!     ├── mod.rs       SinkConnector 实现
//!     └── session.rs   多行 INSERT 写入会话
//! ```
//!
//! # 一个连 MySQL 的位置
//!
//! 源与目标**共用** [`config::MysqlConnection`]：读和写只是方向不同，
//! "连哪个库"没有第二种说法。

pub mod config;
pub mod schema;
pub mod sink;
pub mod source;
pub mod sql;
pub mod types;
pub mod wire;

pub use config::MysqlConnection;
pub use sink::MysqlSinkConnector;
pub use source::MysqlSourceConnector;
pub use types::MysqlTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

/// 把本 crate 提供的东西一次登记完
pub fn register(reg: &ConnectorRegistry) {
    reg.register_source_factory(SourceFactory::new("mysql", || {
        Box::new(MysqlSourceConnector::new())
    }));
    reg.register_sink_factory(SinkFactory::new("mysql", || Box::new(MysqlSinkConnector::new())));
    reg.register_type_adapter(TypeAdapterFactory::new("mysql", || Box::new(MysqlTypeAdapter::new())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_names_match() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert!(reg.source_factory("mysql").is_ok());
        assert!(reg.sink_factory("mysql").is_ok());
        assert!(reg.type_adapter("mysql").is_ok());
        assert_eq!(reg.list_sources(), vec!["mysql"]);
        assert_eq!(reg.list_sinks(), vec!["mysql"]);
        assert_eq!(reg.source_factory("mysql").unwrap().build().name(), "mysql");
        assert_eq!(reg.sink_factory("mysql").unwrap().build().name(), "mysql");
        assert_eq!(reg.type_adapter("mysql").unwrap().db_name(), "mysql");
    }
}
