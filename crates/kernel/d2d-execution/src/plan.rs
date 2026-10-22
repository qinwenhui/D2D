//! 执行路径规划：在「逻辑面」与「物理面」之间做显式协商
//!
//! 这是整个直通优化的**唯一决策点**。放在一处、可单独测试、回落原因必须写清楚 ——
//! 而不是散落在各处的 `if 没有算子 && 源支持 && 目标支持`。
//!
//! ## 决策规则（按序判定，先到先得）
//!
//! 1. **有转换算子 → 逻辑面**。这是硬约束：转换算子定义在 Arrow RecordBatch 上，
//!    跳过 Arrow 就等于丢掉转换能力。本项目的核心能力不能被性能优化绕过。
//! 2. 源不提供物理载荷 → 逻辑面
//! 3. 目标不提供物理载荷 → 逻辑面
//! 4. 目标裁决为 [`PhysicalCompat::Reject`] → 逻辑面
//! 5. 以上都通过 → 物理面直通
//!
//! 判定结果与理由会写进日志。运维看到的应该是一句明确的
//! 「本次走 X 路径，因为 Y」，而不是靠猜。

use d2d_connector::{PhysicalCompat, PhysicalFormat, SinkConnector, SourceConnector};
use d2d_transform::TransformChain;

/// 本次执行选择的路径
#[derive(Debug, Clone)]
pub enum ExecutionPath {
    /// 物理面直通：源字节 → 目标 body，零解析零重编码
    Direct {
        format: PhysicalFormat,
        /// 载荷中不得出现的字节（出现即失败，见 [`crate::direct`]）
        forbidden: Vec<u8>,
    },
    /// 逻辑面：Source → Arrow → Transform → Sink（全保真，转换能力完整）
    Logical { reason: LogicalReason },
}

/// 回落逻辑面的原因（必须可读、可归因）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalReason {
    /// 配了转换算子 —— 正常且预期的回落
    TransformsPresent { operators: Vec<String> },
    /// 源端读取方式没有物理形态（如 ODBC 游标）
    SourceNoPhysical,
    /// 目标端不接受物理写入
    SinkNoPhysical,
    /// 目标端明确拒绝该物理编码（如字符集/分隔符不匹配）
    SinkRejected { detail: String },
}

impl LogicalReason {
    pub fn describe(&self) -> String {
        match self {
            Self::TransformsPresent { operators } => format!(
                "配了 {} 个转换算子 [{}]，转换算子定义在 Arrow 上，必须走逻辑面",
                operators.len(),
                operators.join(", ")
            ),
            Self::SourceNoPhysical => "源端读取方式不产出物理载荷（如 ODBC 游标）".to_string(),
            Self::SinkNoPhysical => "目标端不支持物理写入".to_string(),
            Self::SinkRejected { detail } => format!("目标端拒绝了源端编码：{}", detail),
        }
    }
}

impl ExecutionPath {
    /// 一行日志用的人类可读描述
    pub fn describe(&self) -> String {
        match self {
            Self::Direct { format, .. } => format!("物理面直通（{}）", format.describe()),
            Self::Logical { reason } => format!("逻辑面（{}）", reason.describe()),
        }
    }
}

/// 协商执行路径
///
/// 三个入参都是「能力视图」而非具体类型：换连接器、换读取方式都不需要改这个函数。
pub fn plan_path(
    source: &dyn SourceConnector,
    sink: &dyn SinkConnector,
    transforms: &TransformChain,
) -> ExecutionPath {
    // 1. 有转换算子 —— 直通免谈。这一条必须在最前面，且不可绕过。
    if !transforms.is_empty() {
        return ExecutionPath::Logical {
            reason: LogicalReason::TransformsPresent {
                operators: transforms.operator_names(),
            },
        };
    }

    // 2. 源端有没有物理形态
    let Some(psrc) = source.as_physical() else {
        return ExecutionPath::Logical {
            reason: LogicalReason::SourceNoPhysical,
        };
    };
    let Some(format) = psrc.physical_format() else {
        return ExecutionPath::Logical {
            reason: LogicalReason::SourceNoPhysical,
        };
    };

    // 3. 目标端能不能收
    let Some(psink) = sink.as_physical() else {
        return ExecutionPath::Logical {
            reason: LogicalReason::SinkNoPhysical,
        };
    };

    // 4. 目标端逐项比对编码
    match psink.accepts_physical(&format) {
        PhysicalCompat::Accept { forbidden } => ExecutionPath::Direct { format, forbidden },
        PhysicalCompat::Reject(detail) => ExecutionPath::Logical {
            reason: LogicalReason::SinkRejected { detail },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;
    use d2d_connector::{
        Charset, CsvDialect, PhysicalSession, PhysicalSink, PhysicalSource, PhysicalStream,
        SinkSession, SinkSpec, SourceSpec, SourceSplit,
    };
    use d2d_core::{D2DResult, RecordBatchStream};

    // ── 测试替身：能力可任意组合 ──

    struct TestSource {
        physical: Option<PhysicalFormat>,
    }

    #[async_trait]
    impl SourceConnector for TestSource {
        fn name(&self) -> &str {
            "test"
        }
        async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn schema(&self) -> D2DResult<SchemaRef> {
            Ok(std::sync::Arc::new(arrow::datatypes::Schema::empty()))
        }
        async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
            Ok(vec![])
        }
        async fn read(&self, _: &SourceSplit) -> D2DResult<RecordBatchStream> {
            unimplemented!()
        }
        fn as_physical(&self) -> Option<&dyn PhysicalSource> {
            self.physical.as_ref().map(|_| self as &dyn PhysicalSource)
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl PhysicalSource for TestSource {
        fn physical_format(&self) -> Option<PhysicalFormat> {
            self.physical.clone()
        }
        async fn read_physical(&self, _: &SourceSplit) -> D2DResult<PhysicalStream> {
            unimplemented!()
        }
    }

    struct TestSink {
        verdict: Option<PhysicalCompat>,
    }

    #[async_trait]
    impl SinkConnector for TestSink {
        fn name(&self) -> &str {
            "test"
        }
        async fn init(&mut self, _: &SinkSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn begin_session(&self, _: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
            unimplemented!()
        }
        fn as_physical(&self) -> Option<&dyn PhysicalSink> {
            self.verdict.as_ref().map(|_| self as &dyn PhysicalSink)
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl PhysicalSink for TestSink {
        fn accepts_physical(&self, _: &PhysicalFormat) -> PhysicalCompat {
            self.verdict.clone().unwrap()
        }
        async fn begin_physical(&self, _: &PhysicalFormat) -> D2DResult<Box<dyn PhysicalSession>> {
            unimplemented!()
        }
    }

    fn fmt() -> PhysicalFormat {
        PhysicalFormat::csv(CsvDialect::default(), Charset::Utf8, vec!["a".into()])
    }

    fn src(physical: bool) -> TestSource {
        TestSource {
            physical: if physical { Some(fmt()) } else { None },
        }
    }

    fn sink(verdict: Option<PhysicalCompat>) -> TestSink {
        TestSink { verdict }
    }

    /// 空算子链 + 双方均支持 → 直通
    #[test]
    fn test_direct_when_everything_aligns() {
        let p = plan_path(
            &src(true),
            &sink(Some(PhysicalCompat::Accept {
                forbidden: vec![b'\\'],
            })),
            &TransformChain::default(),
        );
        match p {
            ExecutionPath::Direct { forbidden, .. } => assert_eq!(forbidden, vec![b'\\']),
            other => panic!("期望直通, 实际 {:?}", other),
        }
    }

    /// **核心保护**：只要有一个转换算子，就必须回落逻辑面
    #[test]
    fn test_transforms_force_logical_path() {
        use d2d_transform::Transform;
        struct Noop;
        impl Transform for Noop {
            fn transform(
                &self,
                b: arrow::array::RecordBatch,
            ) -> D2DResult<arrow::array::RecordBatch> {
                Ok(b)
            }
            fn name(&self) -> &str {
                "noop"
            }
        }
        let chain = TransformChain::new(vec![Box::new(Noop)]);
        let p = plan_path(
            &src(true),
            &sink(Some(PhysicalCompat::Accept { forbidden: vec![] })),
            &chain,
        );
        match p {
            ExecutionPath::Logical {
                reason: LogicalReason::TransformsPresent { operators },
            } => assert_eq!(operators, vec!["noop"]),
            other => panic!("有转换算子时必须回落逻辑面, 实际 {:?}", other),
        }
    }

    /// 源不支持物理 → 逻辑面
    #[test]
    fn test_source_without_physical_falls_back() {
        let p = plan_path(
            &src(false),
            &sink(Some(PhysicalCompat::Accept { forbidden: vec![] })),
            &TransformChain::default(),
        );
        assert!(matches!(
            p,
            ExecutionPath::Logical {
                reason: LogicalReason::SourceNoPhysical
            }
        ));
    }

    /// 目标不支持物理 → 逻辑面
    #[test]
    fn test_sink_without_physical_falls_back() {
        let p = plan_path(&src(true), &sink(None), &TransformChain::default());
        assert!(matches!(
            p,
            ExecutionPath::Logical {
                reason: LogicalReason::SinkNoPhysical
            }
        ));
    }

    /// 目标明确拒绝（分隔符/字符集不匹配）→ 逻辑面，且理由要带原文
    #[test]
    fn test_sink_rejection_falls_back_with_detail() {
        let p = plan_path(
            &src(true),
            &sink(Some(PhysicalCompat::Reject("分隔符不一致".into()))),
            &TransformChain::default(),
        );
        match p {
            ExecutionPath::Logical {
                reason: LogicalReason::SinkRejected { detail },
            } => assert!(detail.contains("分隔符")),
            other => panic!("实际 {:?}", other),
        }
    }

    #[test]
    fn test_describe_is_readable() {
        let p = plan_path(
            &src(true),
            &sink(Some(PhysicalCompat::Reject("字符集".into()))),
            &TransformChain::default(),
        );
        let d = p.describe();
        assert!(d.contains("逻辑面") && d.contains("字符集"), "{}", d);
    }
}
