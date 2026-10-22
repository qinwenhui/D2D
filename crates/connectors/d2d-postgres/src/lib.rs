//! PostgreSQL 连接器：读（简单查询协议，文本投影）与写（多行 INSERT）
//!
//! ```text
//! src/
//! ├── config.rs        连接参数（会话时区钉成 UTC）
//! ├── types.rs         声明类型 ↔ 逻辑类型
//! ├── sql.rs           方言：双引号、md5 分片、查询组装
//! ├── schema.rs        权威 schema + `::text` 投影（bytea/timestamptz 特殊处理）
//! ├── source/mod.rs    SourceConnector（simple_query 流式读）
//! └── sink/
//!     ├── mod.rs       SinkConnector
//!     └── session.rs   多行 INSERT（`$n::text::目标类型`）
//! ```

pub mod config;
pub mod schema;
pub mod sink;
pub mod source;
pub mod sql;
pub mod types;

pub use config::PgConnection;
pub use sink::PgSinkConnector;
pub use source::PgSourceConnector;
pub use types::PgTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

pub fn register(reg: &ConnectorRegistry) {
    reg.register_source_factory(SourceFactory::new("postgres", || {
        Box::new(PgSourceConnector::new())
    }));
    reg.register_sink_factory(SinkFactory::new("postgres", || {
        Box::new(PgSinkConnector::new())
    }));
    reg.register_type_adapter(TypeAdapterFactory::new("postgres", || {
        Box::new(PgTypeAdapter::new())
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_names_match() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert_eq!(reg.list_sources(), vec!["postgres"]);
        assert_eq!(reg.list_sinks(), vec!["postgres"]);
        assert_eq!(reg.list_type_adapters(), vec!["postgres"]);
        assert_eq!(
            reg.source_factory("postgres").unwrap().build().name(),
            "postgres"
        );
        assert_eq!(
            reg.sink_factory("postgres").unwrap().build().name(),
            "postgres"
        );
        assert_eq!(reg.type_adapter("postgres").unwrap().db_name(), "postgres");
    }
}
