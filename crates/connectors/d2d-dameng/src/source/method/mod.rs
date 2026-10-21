//! 达梦 Source 的读取方式（method）路由
//!
//! 设计要点（见 设计说明）：
//! - method 是**传输方式**参数，不是独立连接器：SQL 方言 / 类型契约 / 列语义跨方法复用
//! - `odbc`：进程内游标读取（条件/计算列/排序下推全支持，odbc-api 接线为待办）
//! - `dmfldr`：全表高速导出（dmfldr OUT + TABLE），引擎侧文本分块并行解析
//! - `dpi`：达梦原生 DPI 游标 + 数组取数（NULL 语义正确、无导出文件）
//!
//! 方法实现（[`DamengReadMethod`]）只负责"怎么读"，共享的 schema 契约
//! （[`DamengReadContext`]）由连接器在 init 时构建后注入。

pub mod dmfldr;
pub mod dpi;
pub mod odbc;

use crate::config::ConnectionConfig;
use crate::source::method::dmfldr::params::ExportMode;
use crate::types::DamengTypeAdapter;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use d2d_connector::{SourceSpec, SourceSplit};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use d2d_types::{LogicalArrowBridge, LogicalType, TypeAdapter};

/// 读取方式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamengMethodKind {
    /// ODBC 游标（缺省）
    Odbc,
    /// dmfldr 全表高速导出
    FlDr,
    /// 达梦原生 DPI（数组取数；NULL 由指示器给出）
    Dpi,
}

impl DamengMethodKind {
    /// 从 YAML `source.method` 解析；缺省 odbc
    pub fn parse(method: Option<&str>) -> D2DResult<Self> {
        match method {
            None | Some("") | Some("odbc") => Ok(Self::Odbc),
            Some("dmfldr") | Some("dmldr") => Ok(Self::FlDr),
            Some("dpi") => Ok(Self::Dpi),
            Some(other) => Err(D2DError::config(format!(
                "unsupported dameng source method '{}', expected: odbc | dmfldr | dpi",
                other
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Odbc => "odbc",
            Self::FlDr => "dmfldr",
            Self::Dpi => "dpi",
        }
    }
}

impl std::fmt::Display for DamengMethodKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 方法共享的读取上下文：由 type_contract 派生的权威 schema
///
/// dmfldr 模式下这是**唯一** schema 来源（文本导出不带类型信息）。
#[derive(Debug, Clone)]
pub struct DamengReadContext {
    /// 契约推导的 Arrow schema（列序 = 契约声明序）
    pub schema: SchemaRef,
    /// 与 schema.fields 逐列对齐的逻辑类型（驱动解析策略）
    pub logical_types: Vec<LogicalType>,
    /// 列名（与契约声明序一致）
    pub column_names: Vec<String>,
}

/// 从 source.type_contract 构建读取上下文
///
/// 逐列：达梦类型声明 → DamengTypeAdapter → LogicalType → Arrow Field。
/// 无可映射类型时 fail-fast（无静默降级）。
pub fn build_read_context(spec: &SourceSpec) -> D2DResult<DamengReadContext> {
    build_read_context_for(
        spec,
        "dmfldr",
        "dmfldr exports plain text without type information, the contract is the only schema source",
    )
}

/// 与 [`build_read_context`] 相同，只是错误信息带上方法名与理由
///
/// 三种方法都要求 `type_contract`（它是 schema 与列序的唯一权威），但各自的
/// "为什么"不同 —— 报错时把话说到点子上，用户才知道该补什么。
pub fn build_read_context_for(
    spec: &SourceSpec,
    method: &str,
    reason: &str,
) -> D2DResult<DamengReadContext> {
    let contract = spec.type_contract.as_ref().ok_or_else(|| {
        D2DError::config(format!(
            "source.type_contract is required in {} mode: {}",
            method, reason
        ))
    })?;
    if contract.is_empty() {
        return Err(D2DError::config("source.type_contract must not be empty"));
    }

    let adapter = DamengTypeAdapter::new();
    let mut fields = Vec::with_capacity(contract.len());
    let mut logical_types = Vec::with_capacity(contract.len());
    let mut column_names = Vec::with_capacity(contract.len());

    for c in contract {
        let lt = adapter.to_logical_from_str(&c.source_type)?;
        lt.validate()?;
        let arrow_ty = LogicalArrowBridge::to_arrow(&lt)?;
        fields.push(arrow::datatypes::Field::new(
            &c.column, arrow_ty, true, // dmfldr 文本无法判定非空，统一可空
        ));
        logical_types.push(lt);
        column_names.push(c.column.clone());
    }

    let schema = SchemaRef::new(arrow::datatypes::Schema::new(fields));
    Ok(DamengReadContext {
        schema,
        logical_types,
        column_names,
    })
}

/// dmfldr 模式对查询侧配置的限制（fail-fast，拒绝静默降级）
///
/// ⚠️ **限制随导出模式而变**。早先这里一律按 `TABLE=` 模式拒绝，把 `SQLFILE=` 本来
/// 就支持的能力（`WHERE` 下推）也一并挡掉了；而 `SQLFILE=` 跑的就是一条完整 SELECT。
///
/// | 配置 | `partition.mode: none`（`TABLE=`） | 分片模式（`SQLFILE=`） |
/// |---|---|---|
/// | `query.where_clause` | ❌ 整表导出，没有 where 的位置 | ✅ 拼进每条分片 SQL |
/// | `query.columns` 计算列 | ❌ | ❌ 列清单来自 `type_contract`（权威 schema） |
/// | `query.order_by` | ❌ | ❌ 分片并行导出下顺序无意义 |
/// | `query.limit` | ❌ | ❌ **N 个分片各 LIMIT n 会取回 N×n 行** |
///
/// 两种模式的切换**不需要用户配**——由 `source.partition` 派生（[`ExportMode::for_spec`]）。
///
/// `columns` 非空时列序必须与 `type_contract` 完全一致（契约即导出列序，
/// 也是文本解析的 schema —— 不一致会逐列错位）。
pub fn validate_dmfldr_query(spec: &SourceSpec) -> D2DResult<()> {
    let partitioned = ExportMode::for_spec(spec)? == ExportMode::Partitioned;
    let mode = if partitioned {
        "dmfldr partitioned (SQLFILE)"
    } else {
        "dmfldr TABLE"
    };

    if let Some(query) = &spec.query {
        if let Some(cols) = &query.columns {
            for c in cols {
                if matches!(c, d2d_connector::QueryColumn::Expr { .. }) {
                    return Err(D2DError::config(format!(
                        "{} 不支持计算列：导出/分片的列清单来自 source.type_contract（权威 schema），\
                         query.columns 仅用于声明列序；需要计算列请改用 method: odbc",
                        mode
                    )));
                }
            }
            // 列序必须与契约一致（dmfldr 按控制文件 coldef 顺序导出，解析以契约为 schema）
            let contract: Vec<&str> = spec
                .type_contract
                .as_ref()
                .map(|c| c.iter().map(|x| x.column.as_str()).collect())
                .unwrap_or_default();
            let wanted: Vec<&str> = cols
                .iter()
                .map(|c| match c {
                    d2d_connector::QueryColumn::Name(n) => n.as_str(),
                    _ => "",
                })
                .collect();
            if !wanted.is_empty() && wanted != contract {
                return Err(D2DError::config(format!(
                    "{} 要求 query.columns 与 type_contract 顺序完全一致（契约即导出 schema）：\
                     query={:?}, contract={:?}",
                    mode, wanted, contract
                )));
            }
        }
        // ── WHERE：SQL 模式放行（这正是本次修掉的过宽限制）──
        if let Some(w) = &query.where_clause {
            if !w.trim().is_empty() && !partitioned {
                return Err(D2DError::config(
                    "dmfldr TABLE 模式不支持 where_clause 下推（TABLE= 是整表导出，没有 where 的位置）—— \
                     请给 source.partition 声明一种切分方式（如 mode: range / list / hash）——                     分片模式下 where 会拼进每条分片 SQL",
                ));
            }
        }
        if let Some(o) = &query.order_by {
            if !o.trim().is_empty() {
                return Err(D2DError::config(format!(
                    "{} 不支持 order_by：迁移是集合语义，且分片并行导出下整体顺序无意义",
                    mode
                )));
            }
        }
        if query.limit.is_some() {
            return Err(D2DError::config(format!(
                "{} 不支持 limit：{}分片各自 LIMIT 会取回 N×limit 行，与「限制总行数」的意图相反。\
                 需要截断请用 query.where_clause 表达",
                mode,
                if partitioned { "N 个" } else { "" }
            )));
        }
    }

    // ── 分区：table 模式一律不支持；partitioned 模式支持（由 connector 规划）──
    let effective = spec.effective_partition()?;
    if let Some(p) = &effective {
        p.validate()?;
        if !partitioned && p.mode != d2d_connector::PartitionMode::None {
            return Err(D2DError::config(
                "dmfldr TABLE 模式不支持 DB 侧分片（TABLE= 不能下推范围）—— \
                 请改用 export_mode: partitioned（SQLFILE 模式，支持 where 与分片），\
                 或设 partition.mode: none（此时导出文件按大小自动分块并行解析）",
            ));
        }
    }

    Ok(())
}

/// 启动期 dmfldr 配置校验（CLI dry-run / init 双保险）
pub fn validate_dmfldr_read_config(spec: &SourceSpec) -> D2DResult<DamengReadContext> {
    // 连接配置可解析
    ConnectionConfig::from_json(&spec.connection)?;
    // 参数块也要在这里校验：拼错的键、**已经删掉的旋钮**、越界的取值，
    // 都必须在 dry-run 能走到的那条路上拦下来。
    //
    // ⚠️ 曾经这块只在 `DamengSourceConnector::init` 里校验 —— 而 `--dry-run`
    // 不调 init，于是"dry-run 说没问题、真跑起来才报错"。fail-fast 的价值
    // 有一半在这里：让用户在**改配置的时候**就知道，而不是等开工。
    let params = crate::source::method::dmfldr::params::FlDrParams::from_props(&spec.props())?;
    params.validate()?;
    params.validate_for(crate::source::method::dmfldr::params::ExportMode::for_spec(
        spec,
    )?)?;
    validate_dmfldr_query(spec)?;
    build_read_context(spec)
}

/// 读取方法 trait：只负责"怎么读"，schema 契约由 context 注入
#[async_trait]
pub trait DamengReadMethod: Send + Sync {
    fn kind(&self) -> DamengMethodKind;

    /// 规划分片（dmfldr `TABLE=`：触发整表导出并按产物大小分块）
    async fn split(
        &self,
        spec: &SourceSpec,
        ctx: &DamengReadContext,
    ) -> D2DResult<Vec<SourceSplit>>;

    /// 读取一个分片，产出 RecordBatch Stream
    ///
    /// **分片所需的取数动作整个住在这一次调用里**：dmfldr 的分片模式下，
    /// 一次 `read` = 导出这一片的文件 + 解析 + 把批次交出去。
    /// 于是"导出"与"下游解析/写入"的重叠不再需要一条专门的流式接口 ——
    /// 它就是任务之间的并发（见 [`d2d_connector::SourceConnector`] 的契约一节）。
    async fn read(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<RecordBatchStream>;

    /// 物理载荷格式：本方法产出的文本能否被目标端**直接**消费
    ///
    /// 返回 `Some` 是「源端声明」，能不能真走直通由目标端裁决
    /// （见 `d2d_connector::PhysicalSink::accepts_physical`）与执行计划层协商。
    /// 缺省 `None` = 不支持物理面（如 ODBC 游标：读出来是行对象，没有物理形态）。
    fn physical_format(&self, _ctx: &DamengReadContext) -> Option<d2d_connector::PhysicalFormat> {
        None
    }

    /// 打开一个分片的物理字节流（仅当 [`Self::physical_format`] 返回 `Some` 时有意义）
    async fn read_physical(
        &self,
        _ctx: &DamengReadContext,
        _split: &SourceSplit,
    ) -> D2DResult<d2d_connector::PhysicalStream> {
        Err(D2DError::not_supported(
            "this dameng read method does not support physical passthrough",
        ))
    }

    /// 释放导出产物（数据文件/控制文件/日志）
    async fn close(&self) -> D2DResult<()> {
        Ok(())
    }

    /// 源端权威行数（导出/读出侧统计），供迁移收尾做行数对账
    ///
    /// dmfldr 路径取 dmfldr 日志中的「读取的逻辑记录总数」——是导出侧的真实行数；
    /// ODBC 路径暂无统计，返回 `None`。
    ///
    /// **为什么必须有这个对账**：目标端静默减少行数是真实事故——
    /// Doris UNIQUE 模型对重复键是**静默 upsert**（不报错、不计数），
    /// 若目标表唯一键漏了真正的唯一列，整表会悄悄少几千行而流程「成功」。
    /// 只在写入侧看行数永远发现不了，必须与源端总数比对。
    fn exported_rows(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_connector::{
        PartitionMode, PartitionSpec, QueryColumn, QueryConfig, SplitConfig, SplitMode,
        TypeContractColumn,
    };
    use serde_json::json;

    fn base_spec() -> SourceSpec {
        SourceSpec {
            connector: "dameng".to_string(),
            connection: json!({"host": "h", "user": "u", "password": "p"}),
            method: Some("dmfldr".to_string()),
            properties: None,
            table: "t_a".to_string(),
            schema_name: None,
            query: None,
            type_contract: Some(vec![TypeContractColumn {
                column: "id".to_string(),
                source_type: "BIGINT".to_string(),
                sink_type: "BIGINT".to_string(),
            }]),
            split: None,
            partition: None,
        }
    }

    /// 分片导出模式的规格 —— **只靠 `partition` 声明**，不再有 `export_mode`
    ///
    /// SQL 模式不生成控制文件，所以 null_mode / colname_heading 必须走 dmfldr 缺省。
    fn partitioned_spec() -> SourceSpec {
        let mut spec = base_spec();
        spec.properties = Some(serde_json::Map::from_iter([(
            "dmfldr".to_string(),
            json!({"null_mode": false, "colname_heading": false}),
        )]));
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("id".into()),
            count: 4,
            ..Default::default()
        });
        spec
    }

    #[test]
    fn test_method_kind_parse() {
        assert_eq!(
            DamengMethodKind::parse(None).unwrap(),
            DamengMethodKind::Odbc
        );
        assert_eq!(
            DamengMethodKind::parse(Some("odbc")).unwrap(),
            DamengMethodKind::Odbc
        );
        assert_eq!(
            DamengMethodKind::parse(Some("dmfldr")).unwrap(),
            DamengMethodKind::FlDr
        );
        assert_eq!(
            DamengMethodKind::parse(Some("dpi")).unwrap(),
            DamengMethodKind::Dpi
        );
        assert!(DamengMethodKind::parse(Some("binlog")).is_err());
    }

    #[test]
    fn test_build_read_context_from_contract() {
        let mut spec = base_spec();
        spec.type_contract = Some(vec![
            TypeContractColumn {
                column: "id".into(),
                source_type: "BIGINT".into(),
                sink_type: "BIGINT".into(),
            },
            TypeContractColumn {
                column: "ts".into(),
                source_type: "TIMESTAMP(6)".into(),
                sink_type: "DATETIME(6)".into(),
            },
        ]);
        let ctx = build_read_context(&spec).unwrap();
        assert_eq!(ctx.logical_types[0], LogicalType::Int64);
        assert_eq!(
            ctx.logical_types[1],
            LogicalType::Timestamp {
                precision: d2d_types::TemporalPrecision::Micro,
                tz: None
            }
        );
        assert_eq!(ctx.column_names, vec!["id", "ts"]);
    }

    #[test]
    fn test_contract_required_in_dmfldr() {
        let mut spec = base_spec();
        spec.type_contract = None;
        let err = build_read_context(&spec).unwrap_err();
        assert!(err.to_string().contains("type_contract"), "{}", err);
    }

    #[test]
    fn test_dmfldr_rejects_expr_column() {
        let mut spec = base_spec();
        spec.query = Some(QueryConfig {
            columns: Some(vec![QueryColumn::Expr {
                expr: "\"a\"*2".into(),
                alias: "b".into(),
            }]),
            ..Default::default()
        });
        let err = validate_dmfldr_query(&spec).unwrap_err();
        assert!(err.to_string().contains("不支持计算列"), "{}", err);
    }

    #[test]
    fn test_dmfldr_accepts_plain_columns() {
        let mut spec = base_spec();
        spec.query = Some(QueryConfig {
            columns: Some(vec![QueryColumn::Name("id".into())]),
            ..Default::default()
        });
        spec.split = Some(SplitConfig {
            mode: SplitMode::None,
            column: String::new(),
            count: 8,
        });
        assert!(validate_dmfldr_query(&spec).is_ok());
    }

    // ───────── where 限制的修复（回归）─────────

    /// **回归**：SQLFILE 模式跑的就是一条完整 SELECT，where 本来就该放行。
    /// 旧代码无条件按 TABLE 模式拒绝，把这条能力一起挡掉了。
    #[test]
    fn test_where_allowed_in_partitioned_mode() {
        let mut spec = partitioned_spec();
        spec.query = Some(QueryConfig {
            where_clause: Some("\"CREATED_AT\" >= TIMESTAMP'2026-01-01 00:00:00'".into()),
            ..Default::default()
        });
        assert!(
            validate_dmfldr_query(&spec).is_ok(),
            "partitioned 模式必须放行 where_clause"
        );
    }

    /// where 在 TABLE 模式下仍然要拒绝（`TABLE=` 确实是整表导出）
    #[test]
    fn test_where_still_rejected_in_table_mode() {
        let mut spec = base_spec();
        spec.query = Some(QueryConfig {
            where_clause: Some("\"status\" = 1".into()),
            ..Default::default()
        });
        let err = validate_dmfldr_query(&spec).unwrap_err();
        assert!(
            err.to_string().contains("TABLE 模式不支持 where_clause"),
            "{}",
            err
        );
    }

    /// 空 where 不算配置
    #[test]
    fn test_blank_where_is_not_a_conflict() {
        let mut spec = base_spec();
        spec.query = Some(QueryConfig {
            where_clause: Some("   ".into()),
            ..Default::default()
        });
        assert!(validate_dmfldr_query(&spec).is_ok());
    }

    /// limit 在两种模式下都必须拒绝：N 个分片各 LIMIT n = 取回 N×n 行
    #[test]
    fn test_limit_rejected_in_both_modes() {
        for mut spec in [base_spec(), partitioned_spec()] {
            spec.query = Some(QueryConfig {
                limit: Some(1000),
                ..Default::default()
            });
            let err = validate_dmfldr_query(&spec).unwrap_err();
            assert!(err.to_string().contains("不支持 limit"), "{}", err);
        }
    }

    #[test]
    fn test_order_by_rejected_in_both_modes() {
        for mut spec in [base_spec(), partitioned_spec()] {
            spec.query = Some(QueryConfig {
                order_by: Some("\"id\"".into()),
                ..Default::default()
            });
            assert!(validate_dmfldr_query(&spec).is_err());
        }
    }

    // ───────── 内核 PartitionSpec 在 dmfldr 下的校验 ─────────

    #[test]
    fn test_partition_allowed_in_partitioned_mode() {
        let mut spec = partitioned_spec();
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("CREATED_AT".into()),
            lower: Some(d2d_connector::partition::Literal::parse("2026-01-01")),
            upper: Some(d2d_connector::partition::Literal::parse("2026-10-01")),
            count: 16,
            ..Default::default()
        });
        assert!(validate_dmfldr_query(&spec).is_ok());
    }

    /// partitioned 模式两个来源都没有分区列 → 启动期就要报错（fail-fast 没丢）
    /// 非法 PartitionSpec 在启动期就要失败，而不是跑到一半才发现
    #[test]
    fn test_invalid_partition_fails_at_validation() {
        let mut spec = partitioned_spec();
        spec.partition = Some(PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("C".into()),
            // 只给下界 → 非法
            lower: Some(d2d_connector::partition::Literal::Int(1)),
            count: 4,
            ..Default::default()
        });
        assert!(validate_dmfldr_query(&spec)
            .unwrap_err()
            .to_string()
            .contains("成对"));
    }
}
