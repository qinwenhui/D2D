//! qmdb 连接器：**Arrow Flight SQL 数据面直连**（arrow 60 原生）
//!
//! qmdb 是 Arrow-native 的列数仓，数据面就是 Flight SQL。本连接器不引
//! `qmdb-client`（它锁 arrow 59，随服务端 DataFusion 的版本走），而是用
//! 内核同款 arrow 60 直说协议 —— 线格式（gRPC + Arrow IPC）跨 arrow
//! crate 版本稳定，qmdb 方协议文档 §0 明确了这一点：
//!
//! - 读：`GetFlightInfo(CommandStatementQuery)` → `DoGet` 逐批收 Arrow 批，
//!   **没有行解析、没有类型重建**（列式直传）
//! - 写：服务端三段式装载 `begin_load → do_put(load_id) → commit_load`，
//!   任意阶段失败自动 abort，不留半截数据
//!
//! ```text
//! src/
//! ├── config.rs        连接配置（addr / 认证 / 超时）+ 装载选项（mode / watermark）
//! ├── sql.rs           qmdb 方言：小写裸标识符、字面量、分区谓词渲染、查询组装
//! ├── flight.rs        Flight SQL 客户端：查询流 + 三段式装载
//! ├── types.rs         启动期 type_contract 用的类型适配器
//! ├── source/mod.rs    SourceConnector（分片切法走内核 partition 那套）
//! └── sink/
//!     ├── mod.rs       SinkConnector（连接 + 目标表自检）
//!     └── session.rs   SinkSession（一次 write_batch = 一次原子装载）
//! ```
//!
//! # 部署前提
//!
//! 目标机器上 qmdb server 在跑（Flight SQL 端点缺省 `:31337`）：
//!
//! ```yaml
//! source:
//!   connector: "qmdb"
//!   connection: { addr: "192.0.2.13:31337", user: "root", password: "${QMDB_PASS}" }
//!   schema_name: "bench_lc"
//!   table: "dwd_wide_gen"
//! ```
//!
//! # 设计取舍
//!
//! - **不自动重试写入**：装载协议没有幂等令牌，commit 响应丢失后盲目重试就是
//!   重复数据。失败大声失败；恢复靠 `load.mode: overwrite_partitions` 圈范围重跑。
//! - **hash 分片不可用**：qmdb 的 SQL 面没有稳定的行哈希函数，硬凑的哈希谓词
//!   会"行数对、数据错"—— 规划期直接拒绝（见 `sql` 模块）。
//! - **首批列对齐校验**：服务端按列名（大小写无关）对齐，对不上的列静默丢弃；
//!   会话首批先和目标表 schema 对一遍，孤儿列大声失败。

pub mod config;
pub mod flight;
pub mod sink;
pub mod source;
pub mod sql;
pub mod types;

pub use config::{LoadMode, LoadOptions, QmdbConnection};
pub use flight::QmdbFlight;
pub use sink::QmdbSinkConnector;
pub use source::QmdbSourceConnector;
pub use types::QmdbTypeAdapter;

use d2d_connector::{ConnectorRegistry, SinkFactory, SourceFactory, TypeAdapterFactory};

pub fn register(reg: &ConnectorRegistry) {
    reg.register_source_factory(SourceFactory::new("qmdb", || {
        Box::new(QmdbSourceConnector::new())
    }));
    reg.register_sink_factory(SinkFactory::new("qmdb", || {
        Box::new(QmdbSinkConnector::new())
    }));
    reg.register_type_adapter(TypeAdapterFactory::new("qmdb", || {
        Box::new(QmdbTypeAdapter::new())
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_names_match() {
        let reg = ConnectorRegistry::new();
        register(&reg);
        assert_eq!(reg.list_sources(), vec!["qmdb"]);
        assert_eq!(reg.list_sinks(), vec!["qmdb"]);
        assert_eq!(reg.list_type_adapters(), vec!["qmdb"]);
        assert_eq!(reg.source_factory("qmdb").unwrap().build().name(), "qmdb");
        assert_eq!(reg.sink_factory("qmdb").unwrap().build().name(), "qmdb");
        assert_eq!(reg.type_adapter("qmdb").unwrap().db_name(), "qmdb");
    }
}
