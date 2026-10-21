//! 连接器**工厂**：注册表里存的是"怎么造一个"，不是"造好的那个"
//!
//! # 为什么必须是工厂
//!
//! 引擎要在**三个不同时刻**碰连接器，而它们的先后顺序是刚性的：
//!
//! ```text
//! ① 启动期账本   unit_footprint(&spec)   —— 还没 init、**不能连库**（--dry-run 也要算得出）
//! ② init         init(&spec, grant)      —— 需要引擎算好并发额度之后
//! ③ 收尾         exported_rows / cleanup —— 执行完了，手上是 Arc<dyn _>
//! ```
//!
//! 如果注册表存的是构造好的实例，① 就没法做（实例必须先 init 才知道连哪里）。
//! 所以注册表存**构造函数**，由引擎按正确顺序调用。
//!
//! # 这也是"加连接器不改 CLI"的落点
//!
//! 引擎侧只认识 [`SourceFactory`] / [`SinkFactory`] / [`TypeAdapterFactory`] 三个界面，
//! 不认识 `DamengSourceConnector` 这类具体类型。新增一个连接器只需要：
//!
//! 1. 新建 `crates/connectors/<库名>/`
//! 2. 在那个 crate 里写一个 `pub fn register(reg: &ConnectorRegistry)`
//! 3. 在 `d2d-cli/src/connectors.rs` 里加**一行** `调用它`
//!
//! # 为什么不做运行时动态加载
//!
//! Rust 没有稳定 ABI，dylib 插件会让每个连接器绑死内核版本 —— 比"编译期插件"
//! 更脆。DataX 能做 jar 插件是 JVM 的红利，不是架构的功劳。
//! 编译期注册 + 一处置接线是 Rust 生态里等价且可靠的做法。

use crate::registry::ConnectorRegistry;
use crate::sink::SinkConnector;
use crate::source::{SourceConnector, SourceSpec, UnitFootprint};
use d2d_core::D2DResult;
use d2d_types::TypeAdapter;
use std::sync::Arc;

/// `Source` 连接器的工厂
pub struct SourceFactory {
    name: String,
    build: Arc<dyn Fn() -> Box<dyn SourceConnector> + Send + Sync>,
}

impl std::fmt::Debug for SourceFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceFactory").field("name", &self.name).finish()
    }
}

impl SourceFactory {
    /// `name` 必须与产出的连接器 `name()` 一致 —— 注册表拿它当键
    pub fn new<N, F>(name: N, build: F) -> Self
    where
        N: Into<String>,
        F: Fn() -> Box<dyn SourceConnector> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            build: Arc::new(build),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// 造一个**尚未 init** 的实例
    pub fn build(&self) -> Box<dyn SourceConnector> {
        (self.build)()
    }

    /// 只读声明地问一句"每个取数单元常驻多少内存" —— **不连库**
    ///
    /// 走的是一个刚造出来、还没 init 的实例：`unit_footprint` 按契约只读 `spec`，
    /// 必须能在 `--dry-run`（那时连不上库）时算得出来。
    pub fn unit_footprint(&self, spec: &SourceSpec) -> UnitFootprint {
        self.build().unit_footprint(spec)
    }
}

/// `Sink` 连接器的工厂
pub struct SinkFactory {
    name: String,
    build: Arc<dyn Fn() -> Box<dyn SinkConnector> + Send + Sync>,
}

impl std::fmt::Debug for SinkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkFactory").field("name", &self.name).finish()
    }
}

impl SinkFactory {
    pub fn new<N, F>(name: N, build: F) -> Self
    where
        N: Into<String>,
        F: Fn() -> Box<dyn SinkConnector> + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            build: Arc::new(build),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn build(&self) -> Box<dyn SinkConnector> {
        (self.build)()
    }
}

/// 类型适配器的工厂
///
/// 键是**数据库名**，与 `source.connector` / `sink.connector` 的取值同一个名字空间 ——
/// 启动期类型契约校验拿 `source.connector` 直接查这张表。
///
/// 为什么是查表而不是 `match db { "dameng" => ..., "doris" => ... }`：
/// match 意味着**加一个连接器要在 CLI 里加一个分支**，而这个分支的内容
/// （"我的类型适配器叫什么"）只有连接器自己知道。注册表把这个问题交回给它的主人。
pub struct TypeAdapterFactory {
    db: String,
    build: Arc<dyn Fn() -> Box<dyn TypeAdapter> + Send + Sync>,
}

impl std::fmt::Debug for TypeAdapterFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeAdapterFactory").field("db", &self.db).finish()
    }
}

impl TypeAdapterFactory {
    pub fn new<D, F>(db: D, build: F) -> Self
    where
        D: Into<String>,
        F: Fn() -> Box<dyn TypeAdapter> + Send + Sync + 'static,
    {
        Self {
            db: db.into(),
            build: Arc::new(build),
        }
    }

    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn build(&self) -> Box<dyn TypeAdapter> {
        (self.build)()
    }
}

impl ConnectorRegistry {
    /// 注册一个 Source 工厂
    pub fn register_source_factory(&self, factory: SourceFactory) {
        self.insert_source_factory(factory);
    }

    /// 注册一个 Sink 工厂
    pub fn register_sink_factory(&self, factory: SinkFactory) {
        self.insert_sink_factory(factory);
    }

    /// 注册一个类型适配器
    pub fn register_type_adapter(&self, factory: TypeAdapterFactory) {
        self.insert_type_adapter(factory);
    }

    /// 按名字取 Source 工厂；查不到时**列出有哪些**，省得人猜
    pub fn source_factory(&self, name: &str) -> D2DResult<Arc<SourceFactory>> {
        let factories = self.read_source_factories();
        factories.get(name).cloned().ok_or_else(|| {
            d2d_core::D2DError::config(format!(
                "source connector '{}' 未注册。已注册的有：[{}]。\
                 若是新写的连接器，检查它有没有在自己的 `register()` 里登记",
                name,
                factories.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })
    }

    /// 按名字取 Sink 工厂
    pub fn sink_factory(&self, name: &str) -> D2DResult<Arc<SinkFactory>> {
        let factories = self.read_sink_factories();
        factories.get(name).cloned().ok_or_else(|| {
            d2d_core::D2DError::config(format!(
                "sink connector '{}' 未注册。已注册的有：[{}]。\
                 若是新写的连接器，检查它有没有在自己的 `register()` 里登记",
                name,
                factories.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })
    }

    /// 按数据库名取类型适配器
    pub fn type_adapter(&self, db: &str) -> D2DResult<Box<dyn TypeAdapter>> {
        let factories = self.read_type_adapters();
        factories.get(db).map(|f| f.build()).ok_or_else(|| {
            d2d_core::D2DError::config(format!(
                "数据库 '{}' 没有注册类型适配器，无法做启动期类型校验。已注册的有：[{}]",
                db,
                factories.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })
    }

    /// 已注册的连接器名单（启动日志用）
    pub fn describe(&self) -> String {
        format!(
            "source=[{}] sink=[{}] 类型适配器=[{}]",
            self.read_source_factories().keys().cloned().collect::<Vec<_>>().join(", "),
            self.read_sink_factories().keys().cloned().collect::<Vec<_>>().join(", "),
            self.read_type_adapters().keys().cloned().collect::<Vec<_>>().join(", "),
        )
    }
}
