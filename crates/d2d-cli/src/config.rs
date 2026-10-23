//! YAML 配置面 —— **只是 [`d2d_execution::Job`] 的一个构造函数**
//!
//! 内核不认识 YAML：`Job` 是纯数据结构，本模块只负责"从文件读出来"。
//! 两条入口（YAML / 程序化）在 [`JobConfig::into_job`] 汇合到同一条执行路径。
//!
//! # 配置面长什么样
//!
//! ```yaml
//! job:      { name: "..." }
//! flow:                                   # ← 执行拓扑：并发 + 池 + 攒批
//!   source:   { concurrency: 16, pool: { capacity: 512MB, on_overflow: spill } }
//!   pipeline: { concurrency: 16, batch_bytes: 64MB }
//!   sink:     { concurrency: 8, batch_bytes: 100MB, batch_rows: 200000,
//!               pool: { capacity: 512MB, on_overflow: spill } }
//! source:   { connector, connection, table, query, type_contract, partition,
//!             properties: {...} }          # ← properties = 这个源特有的
//! transform: [ ... ]
//! sink:     { connector, connection, database, table,
//!             properties: {...} }          # ← properties = 这个目标特有的
//! engine:   { memory_mb: 2048, ... }        # ← 只用于启动期账本校验
//! ```
//!
//! # `flow` 与 `engine` 的分工（**这是全篇最重要的一条**）
//!
//! - `flow` 里的三个 `concurrency` 是**用户说了算**的：他清楚源库能接几个连接、
//!   目标端能接几个导入。填多少就是多少，引擎**不做任何改写**。
//! - `engine.memory_mb` 是**账本上限**：启动期把六项常驻相加，超了**拒绝启动**
//!   并指明超在哪一项。它不推导任何并发。
//!
//! 旧版本反着来（内存 ÷ 每单元常驻 = 并发），代价是"配了 32 路实际跑 6 路
//! 而你永远不会知道"。详见 `d2d_execution::flow` 的模块文档。

use d2d_connector::{SinkSpec, SourceSpec};
use d2d_core::{D2DError, D2DResult};
use d2d_execution::{DirectConfig, FlowSpec, Job};
use d2d_transform::TransformSpec;
use serde::{Deserialize, Serialize};

/// Job 的 YAML 形态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub job: JobMeta,
    /// 执行拓扑（三档并发 / 两个池 / 攒批）。整段可省略，缺省值互相参照
    #[serde(default)]
    pub flow: Option<FlowSpec>,
    pub source: SourceSpec,
    /// 算子声明。**强类型枚举**：算子名或参数名拼错，解析配置时就报错 ——
    /// 旧实现（`{op: String, params: Value}` + 未知即跳过）会让转换**悄悄没做**。
    #[serde(rename = "transform")]
    pub transform_ops: Option<Vec<TransformSpec>>,
    pub sink: SinkSpec,
    pub engine: Option<EngineConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMeta {
    pub name: String,
}

/// 引擎侧：**内存账本 + 核验开关**（不再有任何并发推导）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    // ══════════════════════════════════════════════════════════════
    //  内存账本 —— 唯一一个"资源"字段
    // ══════════════════════════════════════════════════════════════
    /// 全链路内存预算（MB）
    ///
    /// **它不是并发的因。** 启动期把六项常驻加起来（源端取数单元 / source_pool /
    /// pipeline 工作集 / 转换中间态 / sink_pool / sink 工作集）与它比对：
    ///
    /// - 装得下 → 打印逐项明细，正常开跑
    /// - 装不下 → **拒绝启动**，并告诉你超在哪一项、有哪三条处置方式
    ///
    /// 给多少都不会改变任何并发度。不够就让池溢出到磁盘，或你来调大它。
    pub memory_mb: Option<usize>,

    // ── 核验开关（**都缺省开，建议除非有明确理由否则别关**）──
    /// 行数核验：源端导出行数 vs 目标端确认载入行数，不一致即整个任务失败
    ///
    /// **关掉只影响比对，不影响计数本身**（计数是免费的）。
    /// 这是目前**唯一**能抓住「静默丢数」的机制：Doris 的 UNIQUE 模型对重复键是
    /// 静默 upsert，目标表唯一键选错会悄悄少几千行而流程全程「成功」。
    pub verify_row_count: Option<bool>,
    /// 直通路径的受限字节扫描（Doris 把 `\` 当转义符而源端未转义 → 静默改值）
    ///
    /// 实测代价 ~100 ms/GiB（SIMD 顺路扫描），占总耗时 ~0.1%。
    pub verify_forbidden_scan: Option<bool>,
    /// 直通路径开工前整片预扫（false = 只做在途扫描，零额外 I/O 且一定不会静默污染）
    pub direct_preflight_scan: Option<bool>,

    /// tokio 运行时线程数（0 = CPU 核数）
    pub worker_threads: Option<usize>,

    // ── 已删除的字段：收进这里，好给出"该改成什么"而不是"不认识" ──
    #[serde(flatten)]
    pub removed: std::collections::BTreeMap<String, serde_json::Value>,
}

/// 已删除的 `engine.*` 字段 → 新位置
///
/// serde 的 `unknown field` 只说"不认识"，不说"该改成什么"。对**刚刚搬走**的
/// 那几个字段（存量配置里多半还留着）给一条能直接照做的提示，比让人翻文档强。
const MOVED_ENGINE_FIELDS: [(&str, &str); 5] = [
    (
        "source_slots",
        "已改名 → flow.source.concurrency（语义不变：同时几个取数单元）",
    ),
    (
        "pipeline_slots",
        "已改名 → flow.pipeline.concurrency（语义不变：同时几个处理 task）",
    ),
    (
        "sink_slots",
        "已改名 → flow.sink.concurrency（语义不变：同时几个并发写入）",
    ),
    (
        "channel_capacity",
        "已删除 → 换成 flow.<段>.pool.capacity（**单位从「块数」变成「字节」**：\
         块数说不出到底占多少内存，字节数可以）",
    ),
    (
        "policy",
        "已删除 → 换成 flow.<段>.pool.on_overflow（从全局面的一档，变成**每个池各自一档**：\
         源端的池和目标端的池本来就该能独立决定溢出怎么办）",
    ),
];

impl EngineConfig {
    /// 拒绝已搬走/已删除的字段，并给出新位置
    pub fn reject_removed(&self) -> D2DResult<()> {
        for key in self.removed.keys() {
            for (old, hint) in MOVED_ENGINE_FIELDS {
                if key == old {
                    return Err(D2DError::config(format!("engine.{} {}", old, hint)));
                }
            }
        }
        if let Some(first) = self.removed.keys().next() {
            return Err(D2DError::config(format!(
                "engine.{} 不是已知字段。engine 段现在只有 memory_mb / worker_threads \
                 与三个核验开关；并发与池容量在 flow 段，连接器专属参数在 \
                 source.properties / sink.properties",
                first
            )));
        }
        Ok(())
    }

    pub fn memory_mb(&self) -> usize {
        self.memory_mb.unwrap_or(2048)
    }

    pub fn direct_config(&self) -> DirectConfig {
        DirectConfig {
            preflight_scan: self.direct_preflight_scan.unwrap_or(false),
            forbidden_scan: self.verify_forbidden_scan.unwrap_or(true),
            fail_on_row_mismatch: self.verify_row_count.unwrap_or(true),
        }
    }
}

impl JobConfig {
    /// **YAML → `Job`**：两条入口在这里汇合
    ///
    /// 程序化调用方直接 `Job::builder(..).build()`，走的是一模一样的执行路径 ——
    /// 这是"别人能拿它当核心依赖"的最小条件。
    pub fn into_job(&self) -> D2DResult<Job> {
        let direct = match &self.engine {
            Some(e) => {
                e.reject_removed()?;
                e.direct_config()
            }
            None => DirectConfig::default(),
        };
        Job::builder(
            self.job.name.clone(),
            self.source.clone(),
            self.sink.clone(),
        )
        .transforms(self.transform_ops.clone().unwrap_or_default())
        .flow(self.flow.clone().unwrap_or_default())
        .direct_config(direct)
        .build()
    }

    pub fn from_yaml(path: &std::path::Path) -> D2DResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| D2DError::config(format!("failed to read config file: {}", e)))?;
        // 示例配置里写的是 `${DM_USER}` 这类占位符。**必须真的展开**：
        // 曾经没有这一步，占位符会原样传给驱动，报出来是
        // `Access denied for user '${MYSQL_USER}'` —— 看着像口令错，其实是没展开。
        let content = expand_env(&content)?;
        let config: JobConfig = serde_yaml::from_str(&content)
            .map_err(|e| D2DError::config(format!("failed to parse YAML: {}", e)))?;
        Ok(config)
    }
}

/// 把 YAML 文本里的 `${VAR}` 换成环境变量的值
///
/// 规则刻意简单、刻意严格：
/// - 只认 `${NAME}` 这一种写法（`$NAME` 不展开 —— 那是别的系统的事）；
/// - **变量不存在就报错**并点名。留空会让连接用一个空口令，错误被推迟到登录那一刻，
///   而那时的报错说的是"认证失败"，指不到配置上；
/// - `$$` 转义成字面的 `$`（值里真的要写 `${` 时用 `$${`）。
fn expand_env(input: &str) -> D2DResult<String> {
    let mut out = String::with_capacity(input.len());
    // 按**字符串切片**推进，不碰字节：配置里有中文注释与中文值，
    // 逐字节 `push(b as char)` 会把多字节字符打碎。
    let mut rest = input;
    loop {
        let Some(idx) = rest.find('$') else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..idx]);
        let after = &rest[idx..];

        if let Some(r) = after.strip_prefix("$$") {
            out.push('$');
            rest = r;
        } else if let Some(r) = after.strip_prefix("${") {
            let Some(end) = r.find('}') else {
                return Err(D2DError::config(
                    "配置里的 `${` 没有闭合 —— 写成 `${NAME}`，或 `$${` 表示字面的 ${",
                ));
            };
            let name = &r[..end];
            if name.is_empty() {
                return Err(D2DError::config("配置里出现了空的 `${}` 占位符"));
            }
            let value = std::env::var(name).map_err(|_| {
                D2DError::config(format!(
                    "配置引用了环境变量 `{}`，但环境里没有它 —— \
                     请先 export {}，或把配置里的占位符改成字面值",
                    name, name
                ))
            })?;
            out.push_str(&value);
            rest = &r[end + 1..];
        } else {
            // 单独的 `$`（不跟 `$` 或 `{`）原样保留
            out.push('$');
            rest = &after[1..];
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────── 环境变量展开 ─────────

    #[test]
    fn test_expand_env_basic() {
        std::env::set_var("D2D_TEST_USER", "alice");
        assert_eq!(expand_env("user: ${D2D_TEST_USER}").unwrap(), "user: alice");
        // 多个占位符 + 中文原样保留
        std::env::set_var("D2D_TEST_PASS", "p@ss");
        assert_eq!(
            expand_env("用户: ${D2D_TEST_USER} 口令: ${D2D_TEST_PASS}（中文注释）").unwrap(),
            "用户: alice 口令: p@ss（中文注释）"
        );
    }

    /// 变量不存在**必须报错并点名**，不能留空 —— 留空会变成一个空口令，
    /// 报错推迟到登录那一刻，指不到配置上
    #[test]
    fn test_expand_env_missing_var_is_named() {
        let e = expand_env("password: ${D2D_TEST_DEFINITELY_MISSING}")
            .unwrap_err()
            .to_string();
        assert!(e.contains("D2D_TEST_DEFINITELY_MISSING"), "{}", e);
        assert!(e.contains("export"), "要指出出路: {}", e);
    }

    #[test]
    fn test_expand_env_escape_and_plain_dollar() {
        // `$$` → 字面 `$`（值里真要写 `${` 时用 `$${`）
        assert_eq!(expand_env("a $$ b").unwrap(), "a $ b");
        assert_eq!(expand_env("a $${NOT_A_VAR} b").unwrap(), "a ${NOT_A_VAR} b");
        // 单独的 `$` 原样保留（比如正则/口令里的它）
        assert_eq!(expand_env("pa$word").unwrap(), "pa$word");
        // 未闭合
        assert!(expand_env("${OOPS")
            .unwrap_err()
            .to_string()
            .contains("闭合"));
    }

    /// 示例配置里的占位符——测试里给它们一个假值
    ///
    /// 示例是**给人抄的**，里面的口令一律写成 `${DM_USER}` 这种占位符；
    /// 而 `from_yaml` 现在会真展开（缺变量就报错），所以这两条测试要先铺好环境。
    fn set_example_env() {
        for k in [
            "DM_DSN",
            "DM_USER",
            "DM_PASS",
            "DORIS_USER",
            "DORIS_PASS",
            "DORIS_FE",
            "MYSQL_USER",
            "MYSQL_PASS",
            "PG_USER",
            "PG_PASS",
            "QMDB_PASS",
        ] {
            std::env::set_var(k, "dummy");
        }
    }

    /// 随包发布的示例配置必须能被本版本的代码解析
    ///
    /// 防的是"配置漂移"：改了字段名却忘了改示例，用户照抄 `examples/` 直接启动失败。
    /// 这个项目已经在"配置与代码不一致"上吃过亏，而这条测试的成本只有一次 YAML 解析。
    #[test]
    fn test_shipped_examples_parse() {
        set_example_env();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let mut seen = 0;
        for entry in std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("读不到示例目录 {}: {}", dir.display(), e))
        {
            let p = entry.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            seen += 1;
            let job = JobConfig::from_yaml(&p)
                .unwrap_or_else(|e| panic!("示例配置 {} 解析失败: {}", p.display(), e));
            // 分区配置本身也要能通过校验（拼错/漏配在启动期就该失败）
            if let Some(spec) = job
                .source
                .effective_partition()
                .unwrap_or_else(|e| panic!("示例 {} 的 partition/split 冲突: {}", p.display(), e))
            {
                spec.validate()
                    .unwrap_or_else(|e| panic!("示例 {} 的 partition 非法: {}", p.display(), e));
            }
        }
        assert!(seen > 0, "示例目录里一个 YAML 都没有？{}", dir.display());
    }

    /// 随包示例还要能**构造出 Job** —— 只解析不算过，算子名字/参数名/类型名
    /// 都在这步校验。`--dry-run` 现在也会走到这里，所以这个是同一条防线。
    #[test]
    fn test_shipped_examples_build_into_jobs() {
        set_example_env();
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        for entry in std::fs::read_dir(&dir).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            let job = JobConfig::from_yaml(&p)
                .and_then(|c| c.into_job())
                .unwrap_or_else(|e| panic!("示例 {} 构造 Job 失败: {}", p.display(), e));
            assert!(!job.name.is_empty());
        }
    }

    /// **回归**：算子名拼错必须在解析配置时就失败
    ///
    /// 旧实现是 `{op: String, params: Value}` + `_ => warn!("unknown operator, skipping")`，
    /// 任务照跑，那一步转换**悄悄没做** —— 用户拿到一份"成功"的错误数据。
    #[test]
    fn test_misspelled_operator_fails_at_config_parse() {
        let yaml = r#"
job:
  name: "t"
source:
  connector: "dameng"
  connection: {host: h, user: u}
  table: "T"
transform:
  - op: "projct"
    columns: ["a"]
sink:
  connector: "doris"
  connection: {}
  database: "d"
  table: "t"
"#;
        let e = serde_yaml::from_str::<JobConfig>(yaml)
            .unwrap_err()
            .to_string();
        assert!(e.contains("projct"), "{}", e);
        // 错误信息要列出**可选项**，否则用户只能猜下一个写什么
        assert!(e.contains("project"), "{}", e);
        assert!(e.contains("column_map"), "{}", e);
    }

    /// **回归**：参数名拼错同样必须失败（旧实现是 `params.get(..)` 返回 None 静默跳过）
    #[test]
    fn test_misspelled_param_fails_at_config_parse() {
        let yaml = r#"
job: {name: "t"}
source: {connector: "dameng", connection: {host: h}, table: "T"}
transform:
  - op: "project"
    colums: ["a"]
sink: {connector: "doris", connection: {}, database: "d", table: "t"}
"#;
        let e = serde_yaml::from_str::<JobConfig>(yaml)
            .unwrap_err()
            .to_string();
        assert!(e.contains("colums"), "{}", e);
    }

    /// 程序化构造的 `Job` 与 YAML 来的落到同一条路
    #[test]
    fn test_job_from_yaml_and_from_builder_agree() {
        let yaml = r#"
job: {name: "same"}
source: {connector: "dameng", connection: {host: h}, table: "T"}
transform:
  - op: "project"
    columns: ["a", "b"]
sink: {connector: "doris", connection: {}, database: "d", table: "t"}
"#;
        let cfg: JobConfig = serde_yaml::from_str(yaml).unwrap();
        let from_yaml = cfg.into_job().unwrap();

        let programmatic =
            d2d_execution::Job::builder("same", cfg.source.clone(), cfg.sink.clone())
                .transforms(vec![TransformSpec::Project {
                    columns: vec!["a".into(), "b".into()],
                }])
                .build()
                .unwrap();

        assert_eq!(from_yaml.name, programmatic.name);
        assert_eq!(
            from_yaml.transforms.operator_names(),
            programmatic.transforms.operator_names()
        );
        assert_eq!(
            from_yaml.flow, programmatic.flow,
            "两条入口的 flow 缺省必须一致"
        );
    }

    // ───────── 已删字段的迁移提示 ─────────

    /// **回归**：`engine.source_slots` 等搬走的字段必须给出**新位置**，不能只说"不认识"
    #[test]
    fn test_moved_engine_fields_get_actionable_hints() {
        for (old, expect) in [
            ("source_slots", "flow.source.concurrency"),
            ("pipeline_slots", "flow.pipeline.concurrency"),
            ("sink_slots", "flow.sink.concurrency"),
            ("channel_capacity", "flow.<段>.pool.capacity"),
            ("policy", "flow.<段>.pool.on_overflow"),
        ] {
            let yaml = format!(
                "job: {{name: t}}\n\
                 source: {{connector: dameng, connection: {{host: h}}, table: T}}\n\
                 sink: {{connector: doris, connection: {{}}, database: d, table: t}}\n\
                 engine: {{memory_mb: 2048, {}}}\n",
                match old {
                    "policy" => "policy: backpressure".to_string(),
                    _ => format!("{}: 4", old),
                }
            );
            let cfg: JobConfig = serde_yaml::from_str(&yaml).unwrap();
            let e = cfg.into_job().unwrap_err().to_string();
            assert!(e.contains(old), "{}", e);
            assert!(
                e.contains(expect),
                "提示里要写清新位置 {}=>{}, 实际: {}",
                old,
                expect,
                e
            );
        }
    }

    #[test]
    fn test_engine_config_defaults() {
        let cfg: EngineConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.memory_mb(), 2048);
        let d = cfg.direct_config();
        assert!(d.forbidden_scan && d.fail_on_row_mismatch && !d.preflight_scan);
    }

    /// flow 段整段可省略（缺省值必须能跑）
    #[test]
    fn test_flow_section_is_optional() {
        let yaml = r#"
job: {name: "t"}
source: {connector: "dameng", connection: {host: h}, table: "T"}
sink: {connector: "doris", connection: {}, database: "d", table: "t"}
"#;
        let cfg: JobConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.flow.is_none());
        let job = cfg.into_job().unwrap();
        let r = job.flow.resolve();
        assert!(r.source >= 1 && r.pipeline >= 1 && r.sink >= 1);
    }
}
