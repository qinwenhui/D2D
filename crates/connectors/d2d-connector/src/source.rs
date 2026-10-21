use crate::partition::{PartitionMode, PartitionSpec};
use crate::physical::PhysicalSource;
use crate::query::QueryConfig;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use d2d_memory::MemMeter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Source 连接器配置
///
/// # 哪些字段属于这里，哪些属于 [`Self::properties`]
///
/// **这里的 = 引擎认识的通用概念**：连哪个库（`connection`）、读什么（`table` /
/// `query` / `type_contract`）、怎么切（`partition`）。
///
/// **`properties` 里的 = 这个源特有的**：引擎不解释，原样交给连接器。
/// 典型是"驱动怎么取数"这类参数 —— 比如 `fetch_size`（JDBC/ODBC 每批拉多少行）。
/// 它跟 `host` 一样是连接行为，只是更细；放在通用区会让人误以为换个源也管用。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// 连接器名称（如 "dameng"）
    pub connector: String,
    /// 连接配置（连接器特定）
    pub connection: serde_json::Value,
    /// 读取方式（连接器内部路由，缺省由连接器决定）。
    /// dameng: "odbc"（缺省） | "dmfldr"（全表高速导出，见 设计说明）
    #[serde(default)]
    pub method: Option<String>,
    /// **本源特有的参数**，引擎不解释，原样交给连接器。
    ///
    /// ```yaml
    /// properties:
    ///   fetch_size: 10000        # 驱动每批取多少行（dmfldr 忽略它）
    ///   dmfldr:                  # 与方法同名的块，由该方法强类型解析
    ///     executable: "dmfldr"
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<serde_json::Map<String, serde_json::Value>>,
    /// 源表名
    pub table: String,
    /// 数据库 schema 名（可选，用于限定表名，如 "PROD"）
    #[serde(default)]
    pub schema_name: Option<String>,
    /// 查询配置（SELECT 列 / WHERE / ORDER BY / LIMIT 的显式下推）
    pub query: Option<QueryConfig>,
    /// 类型契约声明（启动期校验：源列类型 → 目标列类型的兼容性，防数据错乱）
    #[serde(default)]
    pub type_contract: Option<Vec<TypeContractColumn>>,
    /// 分片配置
    ///
    /// **已废弃**：语义被三件事共用（取数范围 / 并发度 / 解析块数），
    /// 请改用 [`SourceSpec::partition`]。这里保留只为兼容存量配置，
    /// 经 [`SourceSpec::effective_partition`] 转换时会打废弃告警。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub split: Option<SplitConfig>,
    /// 分区配置（取数范围的切法；与并发度、解析块数无关）
    ///
    /// 见 [`crate::partition`] 模块文档。与 `split` **互斥**，同时出现即报错。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionSpec>,
}

impl SourceSpec {
    /// 连接器专属参数块（缺省为空）
    pub fn props(&self) -> serde_json::Map<String, serde_json::Value> {
        self.properties.clone().unwrap_or_default()
    }

    /// 取一个连接器专属参数
    pub fn prop(&self, key: &str) -> Option<&serde_json::Value> {
        self.properties.as_ref().and_then(|p| p.get(key))
    }

    /// 生效的分区规格：`partition` 优先；只有废弃的 `split` 时做一次显式转换
    ///
    /// 两者同时出现**报错**而不是取其一：静默择一会让"我明明配了 range+上下界，
    /// 为什么还在跑 range 探测"这类问题查无可查。
    pub fn effective_partition(&self) -> D2DResult<Option<PartitionSpec>> {
        match (&self.partition, &self.split) {
            (Some(p), None) => Ok(Some(p.clone())),
            (None, None) => Ok(None),
            (Some(_), Some(_)) => Err(D2DError::config(
                "source.partition 与 source.split 不能同时配置 —— \
                 source.split 已废弃（它把取数范围/并发度/解析块数混在一个 count 字段里），\
                 请删掉 source.split，改用 source.partition",
            )),
            (None, Some(s)) => {
                tracing::warn!(
                    table = %self.table,
                    "source.split 已废弃，请改用 source.partition（语义更准：只描述取数范围的切法）"
                );
                Ok(Some(s.to_partition_spec()))
            }
        }
    }
}

/// 列级类型契约：源库类型声明 → 目标库类型声明
///
/// CLI 启动期用适配层转换为逻辑类型，通过兼容矩阵校验。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeContractColumn {
    /// 列名
    pub column: String,
    /// 源库类型声明（如 "NUMBER(19,4)" / "BLOB" / "TIMESTAMP"）
    pub source_type: String,
    /// 目标库类型声明（如 "DECIMAL(19,4)" / "DATETIME"）
    pub sink_type: String,
}

/// 分片配置（**已废弃**，见 [`SourceSpec::partition`]）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitConfig {
    pub mode: SplitMode,
    /// 分片列名（主键或索引列；mode = none 时无语义，可省略）
    #[serde(default)]
    pub column: String,
    /// 分片数量
    pub count: usize,
}

impl SplitConfig {
    /// 显式转换到新模型：旧 `count` 在这条路上确实是"切几片"，语义无损
    pub fn to_partition_spec(&self) -> PartitionSpec {
        let column = if self.column.trim().is_empty() {
            None
        } else {
            Some(self.column.clone())
        };
        PartitionSpec {
            mode: match self.mode {
                SplitMode::Range => PartitionMode::Range,
                SplitMode::Hash => PartitionMode::Hash,
                SplitMode::None => PartitionMode::None,
            },
            column,
            // 旧配置没有边界概念 → range 走回退探测（这正是它原来的行为）
            lower: None,
            upper: None,
            count: self.count.max(1),
            conditions: Vec::new(),
        }
    }
}

/// 分片模式
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitMode {
    /// 按范围分片（如主键范围）
    Range,
    /// 按哈希分片
    Hash,
    /// 不分片（单线程）
    None,
}

/// 数据分片描述
#[derive(Debug, Clone)]
pub struct SourceSplit {
    /// 分片 ID（唯一标识）
    pub split_id: String,
    /// 分片信息（JSON 格式，连接器特定）
    pub split_info: serde_json::Value,
}

/// 取数单元的**形态**（连接器回答"我是怎么取数的"）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnitKind {
    /// 独立子进程（如 dmfldr）—— **内存闸门够不着**，必须单独记账
    Process,
    /// 数据库连接（如 JDBC / ODBC）
    Connection,
    /// 进程内读，无额外单元
    #[default]
    InProcess,
}

impl UnitKind {
    pub fn describe(&self) -> &'static str {
        match self {
            Self::Process => "进程",
            Self::Connection => "连接",
            Self::InProcess => "进程内",
        }
    }
    /// 是否占用内存闸门**管不着**的内存
    pub fn is_outside_gate(&self) -> bool {
        matches!(self, Self::Process)
    }
}

/// 单个取数单元的形态与开销 —— 连接器回答，引擎据此记账
///
/// **注意这里没有"几个"。** 个数由**引擎的闸门**卡住（内核按 `concurrency`
/// 给每个分片任务发券），不由连接器自己定 —— 那正是"资源总账"存在的意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UnitFootprint {
    pub kind: UnitKind,
    /// 每个单元的常驻字节（**进程内 + 进程外**之和）
    pub bytes_each: usize,
    /// 其中**在本进程之外**的那部分（子进程的常驻）。
    ///
    /// 单列出来是因为收尾要拿"本进程口径"去跟实测 RSS 比：RSS **不含**子进程，
    /// 拿含子进程的总额去比会把差值算小、看着像账本很准 —— 那是假的。
    /// 连接器在进程内时（ODBC 行集、并行解析缓冲）这一项为 0。
    pub child_bytes_each: usize,
}

impl UnitFootprint {
    pub fn in_process() -> Self {
        Self::default()
    }
    /// 全是子进程的（如 dmfldr 的导出进程）
    pub fn process(bytes_each: usize) -> Self {
        Self { kind: UnitKind::Process, bytes_each, child_bytes_each: bytes_each }
    }
    /// 全在进程内的（如 ODBC 的行集缓冲）
    pub fn connection(bytes_each: usize) -> Self {
        Self { kind: UnitKind::Connection, bytes_each, child_bytes_each: 0 }
    }
    /// **两部分都有**：子进程 + 本进程内自己开的缓冲
    ///
    /// dmfldr 就是这种：一个取数单元 = 一个导出子进程（在外面）+ 本进程里
    /// 那份并行解析的缓冲（在里面）。两个数都得报 —— 报一个，账本要么漏算，
    /// 要么把"本进程口径"算歪。
    pub fn process_plus_local(child_bytes_each: usize, in_process_each: usize) -> Self {
        Self {
            kind: UnitKind::Process,
            bytes_each: child_bytes_each + in_process_each,
            child_bytes_each,
        }
    }
}

/// Source 连接器 Trait
///
/// 每个 Source 实现负责：
/// 1. 解析连接配置并建立连接
/// 2. 根据分片策略生成 SourceSplit 列表
/// 3. 读取每个分片的数据，产出 RecordBatch Stream
///
/// # 契约：一次 `read` = **一个取数单元**
///
/// 内核按 `flow.source.concurrency` **给每个分片发一张券**，拿到券才 spawn 一个任务，
/// 任务里做一次 [`Self::read`]。于是两件事各自成立、互不推导：
///
/// - **任务数由数据决定** —— [`Self::split`] 切出几片就是几个任务，
///   数据少就只产生少数任务（1 个分片配 16 路并发，仍然只有 1 个任务）
/// - **同时在跑几个由内核决定** —— 上限在引擎手里，连接器**看不见那个数字**
///
/// **连接器不参与限流。** 它不需要回答"我能开几个"，只要保证"一次 `read` 确实只开
/// 一个取数单元"（一个 dmfldr 导出子进程 / 一条 ODBC 游标 / 一条连接）。
/// 这条分工是刻意的：`concurrency` 是**资源总账**的额度，账只有一本，
/// 不能这个连接器解读一遍、那个连接器再解读一遍。
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// 连接器名称
    fn name(&self) -> &str;

    /// 初始化连接器（解析配置、建立连接池等）
    async fn init(&mut self, spec: &SourceSpec) -> D2DResult<()>;

    /// 获取源端 Schema
    async fn schema(&self) -> D2DResult<SchemaRef>;

    /// 规划分片：回答"这次数据切成几份"，**一份就是一个取数单元**（见 [`Self::read`]）
    ///
    /// 这个函数可以很贵 —— 它是**规划**，不是取数。若某个读取方式的分片粒度本身
    /// 取决于一次昂贵动作的产物（dmfldr 的 `TABLE=` 整表导出：产物多大决定切几块），
    /// 那个动作就发生在这里 —— 它产出的是"有几片"，不是"片里的数据"。
    async fn split(&self, spec: &SourceSpec) -> D2DResult<Vec<SourceSplit>>;

    /// 读取一个分片，产出 RecordBatch Stream
    ///
    /// 每个分片独立读取，互不干扰。**分片所需的取数动作整个住在这一次调用里** ——
    /// 比如 dmfldr 的分片模式：一次 `read` = 导出这一片的文件 + 解析 + 把批次交给内核。
    /// 于是"导出"与"下游解析/写入"天然重叠（同一批任务在跑），不需要额外的流式接口。
    async fn read(&self, split: &SourceSplit) -> D2DResult<RecordBatchStream>;

    /// 物理载荷能力（可选）。返回 `None` = 本连接器只能走逻辑面。
    ///
    /// 用「能力访问器」而不是向下转型：调用方只需一个 `Option` 判断，
    /// 不必关心具体连接器类型，也不引入 `Any`。
    fn as_physical(&self) -> Option<&dyn PhysicalSource> {
        None
    }

    /// 本连接器的取数单元**是什么形态、单份占多少内存**
    ///
    /// 资源总账据此记「源端」那一笔账（子进程的内存闸门够不着，必须单独记）。
    /// 缺省 = 进程内读，不占额外资源。
    ///
    /// 这是**启动期预测**：`--dry-run` 时（还没连库）就得算得出来，因为它的用途是
    /// "装不下就拒绝启动"。跑起来之后**实际**占了多少，走 [`Self::memory_meter`]。
    fn unit_footprint(&self, _spec: &SourceSpec) -> UnitFootprint {
        UnitFootprint::in_process()
    }

    /// **运行期占用申报端口**：连接器把**它自己知道的**常驻缓冲挂到这把尺子上
    ///
    /// 引擎量不到连接器内部的东西 —— ODBC 行集是 `odbc-api` 按驱动上报的列宽分配的、
    /// dmfldr 的字节在子进程里 —— 都在第三方库或别的进程里。而连接器知道自己要了多少。
    /// 所以引擎给尺子、连接器挂账、引擎读峰值（见 [`d2d_memory::MemMeter`]）。
    ///
    /// 与 [`Self::unit_footprint`] 的分工是**预测 vs 事实**：那一项决定"要不要拒绝启动"，
    /// 这一项回答"那本账准不准"。
    ///
    /// **引擎不解释这些字节是什么**（名字与说明都由连接器给）：把"doris 会拷一份 body"
    /// 之类写进通用账本，等于把连接器私有行为漏进引擎，换个目标端就成了假账。
    ///
    /// ⚠️ 报上来的量与账本**不可相加** —— 它可能与账本某一项重叠（ODBC 行集就同时是
    /// 账本第 ①项）。它的用途是**解释** `VmHWM` 里哪些是连接器认领的。
    ///
    /// 缺省 = 不申报（引擎如实显示为"未认领"）。
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        None
    }

    /// **源端权威行数** —— 收尾行数对账要比的那个数
    ///
    /// 只看写入侧永远发现不了「静默丢数」：目标端的 UNIQUE 模型对重复键是静默
    /// upsert，目标表唯一键漏掉真正的唯一列时整表会悄悄少行而流程全程"成功"。
    /// 所以要有一个**独立于写入侧**的数字来比。
    ///
    /// - `Some(n)`：源端能给出权威计数（如 dmfldr 的导出日志）
    /// - `None`：给不出（如 ODBC 游标逐行拉）—— 引擎如实跳过并说明，不假装对过账
    ///
    /// **为什么必须在 trait 上**：它与目标端的 `SinkSession::write_batch` 返回值是
    /// 对账的两端。源侧只有达梦有这个方法时，换个源就得改引擎。
    fn exported_rows(&self) -> Option<u64> {
        None
    }

    /// 释放导出产物（临时数据文件、控制文件、日志）
    ///
    /// 为什么是 `&self` 而不是 `&mut self`：引擎在 `Arc<dyn SourceConnector>` 后面
    /// 持有它（执行期需要共享），而 `close(&mut self)` 无法经共享句柄调用。
    /// 连接器内部要可变状态时自己加锁（dmfldr 的导出目录就是这么做的）。
    ///
    /// 缺省 = 没有需要释放的东西。
    async fn cleanup(&self) -> D2DResult<()> {
        Ok(())
    }

    /// 关闭连接器，释放资源
    async fn close(&mut self) -> D2DResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with(yaml_extra: &str) -> serde_json::Value {
        let yaml = format!(
            r#"
connector: dameng
connection: {{host: h, user: u}}
table: T_A
{}
"#,
            yaml_extra
        );
        serde_yaml::from_str::<serde_json::Value>(&yaml).unwrap()
    }

    #[test]
    fn test_partition_binds_to_its_own_field() {
        let v = spec_with(
            "partition:\n  mode: range\n  column: CREATED_AT\n  lower: \"2026-01-01\"\n  upper: \"2026-10-01\"\n  count: 16",
        );
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        let p = spec.partition.as_ref().unwrap();
        assert_eq!(p.mode, PartitionMode::Range);
        assert_eq!(p.count, 16);
        assert!(p.is_probe_free());
        // 引擎认识的通用字段绝不落进 properties —— 那会被连接器当成自己的参数解析
        assert!(spec.properties.is_none(), "{:?}", spec.properties);
    }

    /// 连接器专属参数走 `properties`，通用区看不到它们
    #[test]
    fn test_connector_specific_params_go_into_properties() {
        let v = spec_with("properties:\n  fetch_size: 20000\n  dmfldr:\n    work_dir: /tmp/x");
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.prop("fetch_size").and_then(|x| x.as_u64()), Some(20000));
        assert!(spec.prop("dmfldr").is_some());
        assert_eq!(spec.props().len(), 2);
    }

    /// 顶层写错字段名必须**在解析配置时**失败（不然会被静默忽略，参数看起来"没生效"）
    #[test]
    fn test_unknown_top_level_field_is_rejected() {
        let e = serde_json::from_value::<SourceSpec>(spec_with("fetch_size: 10000"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("fetch_size"), "{}", e);
        assert!(e.contains("properties"), "错误要指出该放哪儿: {}", e);
    }

    /// 旧 split 配置必须继续可用（存量 YAML 不能因为改名就跑不起来）
    #[test]
    fn test_legacy_split_still_works_and_converts() {
        let v = spec_with("split:\n  mode: range\n  column: USER_ID\n  count: 8");
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        assert!(spec.partition.is_none());
        let p = spec.effective_partition().unwrap().unwrap();
        assert_eq!(p.mode, PartitionMode::Range);
        assert_eq!(p.column.as_deref(), Some("USER_ID"));
        assert_eq!(p.count, 8);
        // 旧配置没有边界 → 保留"回退探测"原语义
        assert!(!p.is_probe_free());
    }

    #[test]
    fn test_legacy_split_none_mode() {
        let v = spec_with("split:\n  mode: none\n  count: 16");
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        let p = spec.effective_partition().unwrap().unwrap();
        assert_eq!(p.mode, PartitionMode::None);
        // 空列名归一化成 None，而不是留一个 ""（否则 range/hash 的校验会误判通过）
        assert_eq!(p.column, None);
    }

    /// 两个都配 → 必须报错，不能静默择一
    #[test]
    fn test_both_partition_and_split_is_rejected() {
        let v = spec_with(
            "partition:\n  mode: hash\n  column: C\n  count: 4\nsplit:\n  mode: none\n  count: 8",
        );
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        let e = spec.effective_partition().unwrap_err().to_string();
        assert!(e.contains("不能同时配置"), "{}", e);
    }

    #[test]
    fn test_neither_configured_is_fine() {
        let v = spec_with("");
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        assert!(spec.effective_partition().unwrap().is_none());
    }

    #[test]
    fn test_partition_yaml_round_trip() {
        let v = spec_with("partition:\n  mode: list\n  conditions:\n    - \"A = 1\"");
        let spec: SourceSpec = serde_json::from_value(v).unwrap();
        let back = serde_json::to_value(&spec).unwrap();
        assert_eq!(back["partition"]["conditions"][0], "A = 1");
        // 未配置的字段不该出现在序列化结果里（保持配置文件干净）
        assert!(back.get("split").is_none());
    }
}
