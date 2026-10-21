//! `Job`：一次迁移的**纯数据描述**
//!
//! # 为什么要有它
//!
//! 在这之前，"一次迁移"只能从 YAML 构造 —— 执行逻辑长在 `main()` 里、
//! 算子声明是 `{op: String, params: Value}` 这种字符串形状的东西。
//! 结果是**只有 CLI 能用 D2D**：想从代码里发起一次迁移，得先写个 YAML 落盘。
//!
//! 现在 `Job` 是普通数据结构，构造方式有两条（且**落到同一条执行路径**）：
//!
//! ```ignore
//! // ① 程序化构造
//! let job = Job::builder("nightly", source, sink)
//!     .transforms(vec![TransformSpec::Project { columns: vec!["id".into()] }])?
//!     .build()?;
//!
//! // ② 从 YAML 来（`d2d-cli` 负责这一层，内核不认识 YAML）
//! let job = JobConfig::from_yaml(path)?.into_job()?;
//! ```
//!
//! # 边界
//!
//! `Job` 描述**做什么**（读哪个源、怎么转、写到哪里、安全闸开不开）
//! 与**按什么拓扑跑**（[`FlowSpec`]：三档并发 + 两个池 + 攒批）。
//! 它**不描述这台机器有多少内存** —— 那是 `engine.memory_mb` 的事，
//! 而且只用于启动期校验，不参与任何推导。

use d2d_connector::{SinkSpec, SourceSpec};
use d2d_core::D2DResult;
use d2d_transform::{build_chain, TransformChain, TransformSpec};

use crate::flow::FlowSpec;
use crate::DirectConfig;

/// 一次迁移的完整描述
#[derive(Debug, Clone)]
pub struct Job {
    /// 任务名（日志与指标用；也是 pipeline id 的前缀）
    pub name: String,
    pub source: SourceSpec,
    /// 转换算子链（**直通路径的前提是它为空**，见 `plan_path`）
    pub transforms: TransformChain,
    pub sink: SinkSpec,
    /// 执行拓扑与它的资源面（三档并发 / 两个池 / 攒批）
    pub flow: FlowSpec,
    /// 直通路径的安全闸开关
    pub direct: DirectConfig,
}

impl Job {
    /// 起一个 builder。源与目标是必填，其余有合理缺省。
    pub fn builder(
        name: impl Into<String>,
        source: SourceSpec,
        sink: SinkSpec,
    ) -> JobBuilder {
        JobBuilder {
            name: name.into(),
            source,
            sink,
            specs: Vec::new(),
            flow: FlowSpec::default(),
            direct: DirectConfig::default(),
        }
    }

    /// 是否含转换算子（决定执行路径会走逻辑面还是直通）
    pub fn has_transforms(&self) -> bool {
        !self.transforms.is_empty()
    }
}

/// `Job` 的构造器
///
/// 用 builder 而不是 `Job { .. }` 字面量：算子声明需要**在构造期就解析成算子实例**
/// （类型名拼错、算子名拼错都当场报错），这个失败点必须有地方安放。
#[derive(Debug, Clone)]
pub struct JobBuilder {
    name: String,
    source: SourceSpec,
    sink: SinkSpec,
    specs: Vec<TransformSpec>,
    flow: FlowSpec,
    direct: DirectConfig,
}

impl JobBuilder {
    /// 追加一个算子（按调用顺序执行）
    pub fn transform(mut self, spec: TransformSpec) -> Self {
        self.specs.push(spec);
        self
    }

    /// 一次设置整条算子链
    pub fn transforms(mut self, specs: Vec<TransformSpec>) -> Self {
        self.specs = specs;
        self
    }

    /// 覆盖执行拓扑（三档并发 / 池容量 / 攒批）
    pub fn flow(mut self, flow: FlowSpec) -> Self {
        self.flow = flow;
        self
    }

    /// 覆盖直通路径的安全闸策略
    pub fn direct_config(mut self, direct: DirectConfig) -> Self {
        self.direct = direct;
        self
    }

    /// 定稿。算子声明在这里被解析 —— 拼错的名字此刻失败，而不是运行到一半。
    pub fn build(self) -> D2DResult<Job> {
        let transforms = build_chain(&self.specs)?;
        Ok(Job {
            name: self.name,
            source: self.source,
            transforms,
            sink: self.sink,
            flow: self.flow,
            direct: self.direct,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn src() -> SourceSpec {
        serde_json::from_value(json!({
            "connector": "dameng",
            "connection": {"host": "h", "user": "u"},
            "table": "T"
        }))
        .unwrap()
    }

    fn snk() -> SinkSpec {
        serde_json::from_value(json!({
            "connector": "doris",
            "connection": {"fe_hosts": ["x"]},
            "database": "db",
            "table": "t"
        }))
        .unwrap()
    }

    /// 程序化构造一条完整的 Job —— 这就是"别人能拿去当依赖"的最小验证
    #[test]
    fn test_build_job_programmatically_without_yaml() {
        let job = Job::builder("nightly", src(), snk())
            .transform(TransformSpec::Project {
                columns: vec!["id".into(), "name".into()],
            })
            .transform(TransformSpec::ConstColumn {
                name: "src_sys".into(),
                value: json!("dm"),
            })
            .build()
            .unwrap();

        assert_eq!(job.name, "nightly");
        assert!(job.has_transforms());
        assert_eq!(
            job.transforms.operator_names(),
            vec!["project", "const_column"]
        );
    }

    /// 空算子链 = 直通路径的前提，必须能表达
    #[test]
    fn test_job_without_transforms_is_direct_eligible() {
        let job = Job::builder("pure_copy", src(), snk()).build().unwrap();
        assert!(!job.has_transforms());
        assert!(job.transforms.is_empty());
    }

    /// **构造期**就拒绝非法算子，而不是运行到一半
    #[test]
    fn test_builder_rejects_invalid_operator_at_build_time() {
        let e = Job::builder("x", src(), snk())
            .transform(TransformSpec::Cast {
                mappings: vec![d2d_transform::CastMapping {
                    column: "a".into(),
                    target_type: "int80".into(),
                }],
            })
            .build()
            .unwrap_err()
            .to_string();
        assert!(e.contains("cast"), "错误要指出是哪个算子: {}", e);
        assert!(e.contains("int80"), "{}", e);

        // 空 project 同样在构造期失败
        assert!(Job::builder("x", src(), snk())
            .transform(TransformSpec::Project { columns: vec![] })
            .build()
            .is_err());
    }

    #[test]
    fn test_direct_config_override() {
        let job = Job::builder("x", src(), snk())
            .direct_config(DirectConfig {
                preflight_scan: true,
                ..Default::default()
            })
            .build()
            .unwrap();
        assert!(job.direct.preflight_scan);
        // 缺省仍然是保守的那一档
        assert!(!Job::builder("y", src(), snk()).build().unwrap().direct.preflight_scan);
    }

}
