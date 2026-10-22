//! 连接器注册中心：**名字 → 实现**的唯一一张表
//!
//! 引擎不认识任何具体数据库。它需要连接器时，拿配置里的名字（`source.connector`
//! / `sink.connector`）来这张表查。
//!
//! 表里存的是**工厂**而不是造好的实例 —— 为什么，以及它怎么让"加连接器不改引擎"
//! 成立，见 [`crate::factory`] 的模块文档。
//!
//! # 三张子表
//!
//! | 名字空间 | 键 | 谁填 |
//! |---|---|---|
//! | source | `source.connector` | 每个连接器 crate 的 `register()` |
//! | sink | `sink.connector` | 同上 |
//! | 类型适配器 | 数据库名（与上面同一个名字空间） | 同上 |
//!
//! 一个连接器 crate 的 `register()` 会把它提供的**全部**东西一次登记完 ——
//! 所以"这个库支持什么"只有一个地方要看。

use crate::factory::{SinkFactory, SourceFactory, TypeAdapterFactory};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

/// 连接器注册中心
///
/// 内部用 `RwLock` 而不是 `&mut self`：`register()` 发生在启动期（需要可变），
/// 查表发生在运行期（只需要 `&`），而且引擎在 `Arc` 后面持有它。
#[derive(Default)]
pub struct ConnectorRegistry {
    sources: RwLock<HashMap<String, Arc<SourceFactory>>>,
    sinks: RwLock<HashMap<String, Arc<SinkFactory>>>,
    type_adapters: RwLock<HashMap<String, Arc<TypeAdapterFactory>>>,
}

impl std::fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorRegistry")
            .field("sources", &self.list_sources())
            .field("sinks", &self.list_sinks())
            .field("type_adapters", &self.list_type_adapters())
            .finish()
    }
}

impl ConnectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 已注册的 Source 名字（排序，日志与报错里读着稳）
    pub fn list_sources(&self) -> Vec<String> {
        sorted_names(&self.sources)
    }

    /// 已注册的 Sink 名字
    pub fn list_sinks(&self) -> Vec<String> {
        sorted_names(&self.sinks)
    }

    /// 已注册类型适配器的数据库名
    pub fn list_type_adapters(&self) -> Vec<String> {
        sorted_names(&self.type_adapters)
    }
}

fn sorted_names<T>(map: &RwLock<HashMap<String, Arc<T>>>) -> Vec<String> {
    let mut v: Vec<String> = map.read().unwrap().keys().cloned().collect();
    v.sort();
    v
}

/// 三张表的插入与读取
///
/// 拆成一组泛型小函数，是因为三张表的逻辑逐字相同（同名覆盖警告 + 读锁克隆），
/// 复制三遍只会让"改一处忘两处"。
impl ConnectorRegistry {
    pub(crate) fn insert_source_factory(&self, factory: SourceFactory) {
        let name = factory.name().to_string();
        info!(name = %name, "注册 source 连接器");
        let mut m = self.sources.write().unwrap();
        if m.contains_key(&name) {
            warn!(name = %name, "source 连接器重复注册，后者覆盖前者");
        }
        m.insert(name, Arc::new(factory));
    }

    pub(crate) fn insert_sink_factory(&self, factory: SinkFactory) {
        let name = factory.name().to_string();
        info!(name = %name, "注册 sink 连接器");
        let mut m = self.sinks.write().unwrap();
        if m.contains_key(&name) {
            warn!(name = %name, "sink 连接器重复注册，后者覆盖前者");
        }
        m.insert(name, Arc::new(factory));
    }

    pub(crate) fn insert_type_adapter(&self, factory: TypeAdapterFactory) {
        let db = factory.db().to_string();
        info!(db = %db, "注册类型适配器");
        let mut m = self.type_adapters.write().unwrap();
        if m.contains_key(&db) {
            warn!(db = %db, "类型适配器重复注册，后者覆盖前者");
        }
        m.insert(db, Arc::new(factory));
    }

    pub(crate) fn read_source_factories(&self) -> HashMap<String, Arc<SourceFactory>> {
        self.sources.read().unwrap().clone()
    }

    pub(crate) fn read_sink_factories(&self) -> HashMap<String, Arc<SinkFactory>> {
        self.sinks.read().unwrap().clone()
    }

    pub(crate) fn read_type_adapters(&self) -> HashMap<String, Arc<TypeAdapterFactory>> {
        self.type_adapters.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SinkConnector, SinkSession, SinkSpec, SourceConnector, SourceSpec, SourceSplit,
        UnitFootprint,
    };
    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;
    use d2d_core::{D2DError, D2DResult, RecordBatchStream};
    use d2d_types::{DbTypeDecl, LogicalType, TypeAdapter};

    struct DummySource;
    struct DummySink;
    struct DummyAdapter;

    #[async_trait]
    impl SourceConnector for DummySource {
        fn name(&self) -> &str {
            "dummy_source"
        }
        async fn init(&mut self, _spec: &SourceSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn schema(&self) -> D2DResult<SchemaRef> {
            Ok(SchemaRef::new(arrow::datatypes::Schema::empty()))
        }
        async fn split(&self, _spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
            Ok(vec![])
        }
        async fn read(&self, _split: &SourceSplit) -> D2DResult<RecordBatchStream> {
            Err(D2DError::not_supported("dummy"))
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }

        // 覆写只为验证"引擎能在 init 之前、经工厂问到它"
        fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
            UnitFootprint::process(4096)
        }
    }

    #[async_trait]
    impl SinkConnector for DummySink {
        fn name(&self) -> &str {
            "dummy_sink"
        }
        async fn init(&mut self, _spec: &SinkSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
            Err(D2DError::not_supported("dummy"))
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    impl TypeAdapter for DummyAdapter {
        fn db_name(&self) -> &str {
            "dummy"
        }
        fn to_logical(&self, _decl: &DbTypeDecl) -> D2DResult<LogicalType> {
            Ok(LogicalType::Utf8)
        }
        fn to_db_type(&self, _ty: &LogicalType) -> D2DResult<String> {
            Ok("TEXT".into())
        }
    }

    fn registered() -> ConnectorRegistry {
        let reg = ConnectorRegistry::new();
        reg.register_source_factory(SourceFactory::new("dummy_source", || Box::new(DummySource)));
        reg.register_sink_factory(SinkFactory::new("dummy_sink", || Box::new(DummySink)));
        reg.register_type_adapter(TypeAdapterFactory::new("dummy", || Box::new(DummyAdapter)));
        reg
    }

    #[test]
    fn test_registry_lookup() {
        let registry = registered();

        assert!(registry.source_factory("dummy_source").is_ok());
        assert!(registry.sink_factory("dummy_sink").is_ok());
        assert!(registry.source_factory("not_exist").is_err());

        assert_eq!(registry.list_sources(), vec!["dummy_source"]);
        assert_eq!(registry.list_sinks(), vec!["dummy_sink"]);
        assert_eq!(registry.list_type_adapters(), vec!["dummy"]);
    }

    /// **加连接器不改引擎**的机制性保证：引擎拿名字查表就能拿到可用实例，
    /// 全程不出现具体类型。
    #[test]
    fn test_factory_builds_without_knowing_the_concrete_type() {
        let registry = registered();
        let f = registry.source_factory("dummy_source").unwrap();
        assert_eq!(f.build().name(), "dummy_source");
    }

    /// 内存账预测必须能在 init **之前**、经工厂问到（`--dry-run` 依赖这条）
    #[test]
    fn test_unit_footprint_is_askable_before_init() {
        let registry = registered();
        let f = registry.source_factory("dummy_source").unwrap();
        let spec: SourceSpec = serde_json::from_value(serde_json::json!({
            "connector": "dummy_source",
            "connection": {},
            "table": "t",
        }))
        .unwrap();
        assert_eq!(f.unit_footprint(&spec).bytes_each, 4096);
    }

    #[test]
    fn test_type_adapter_lookup_by_db_name() {
        let registry = registered();
        let a = registry.type_adapter("dummy").unwrap();
        assert_eq!(a.db_name(), "dummy");
        assert!(registry.type_adapter("nope").is_err());
    }

    /// 查不到时要把**已注册的名单**报出来 —— 否则用户只能猜自己拼错了没有
    #[test]
    fn test_missing_lookup_lists_what_is_registered() {
        let registry = registered();
        let e = registry.source_factory("damneg").unwrap_err().to_string();
        assert!(e.contains("dummy_source"), "报错里应列出已注册项: {}", e);
    }

    /// 新连接器的两个名字（工厂键 / 连接器自报）必须一致，否则查表查不到。
    /// 这条只有真跑一遍才发现得了 —— 所以在这里钉死。
    #[test]
    fn test_register_name_matches_connector_name() {
        let registry = registered();
        for name in registry.list_sources() {
            let f = registry.source_factory(&name).unwrap();
            assert_eq!(f.build().name(), name, "工厂键与连接器 name() 不一致");
        }
        for name in registry.list_sinks() {
            let f = registry.sink_factory(&name).unwrap();
            assert_eq!(f.build().name(), name, "工厂键与连接器 name() 不一致");
        }
    }
}
