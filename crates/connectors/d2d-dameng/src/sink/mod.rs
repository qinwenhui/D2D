//! 达梦 **writer 方向**：`DamengSinkConnector` 及它用到的一切
//!
//! | 子模块 | 是什么 |
//! |---|---|
//! | [`method`] | 写入方式路由：`odbc`（绑定参数的多行 INSERT）/ `dmfldr`（IN 快载）/ `dpi`（数组参数绑定） |
//!
//! 读方向（达梦 → X）在 `source/`，见 crate 根文档「骨架」一节。
//!
//! # 与源端共用什么
//!
//! `ConnectionConfig`（连接串渲染）、`DamengTypeAdapter`（类型词汇）、`quote_ident` ——
//! 两侧同库，没有第二套说法。写入方式**不外溢**：它只回答"怎么把这一份数据落进达梦"。

pub mod method;

use async_trait::async_trait;
use d2d_connector::{SinkConnector, SinkSession, SinkSpec};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use tracing::info;

use crate::config::ConnectionConfig;
use crate::dpi::ffi::DpiLibrary;
use crate::sink::method::dmfldr::{DamengFastLoadMethod, FlDrInParams};
use crate::sink::method::dpi::{DamengDpiWriteMethod, DpiSinkParams};
use crate::sink::method::odbc::DamengInsertMethod;
use crate::sink::method::{DamengWriteKind, DamengWriteMethod, WriteContext};

/// 达梦数据库 Sink 连接器
///
/// 通过 `sink.method` 在 ODBC 绑定插入 / dmfldr 快载之间路由
/// （判据见 设计说明 §1：传输方式差异 = 参数，不是新连接器）。
pub struct DamengSinkConnector {
    name: String,
    kind: DamengWriteKind,
    ctx: Option<Arc<WriteContext>>,
    method: Option<Arc<dyn DamengWriteMethod>>,
}

impl DamengSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "dameng".to_string(),
            kind: DamengWriteKind::Odbc,
            ctx: None,
            method: None,
        }
    }
}

#[async_trait]
impl SinkConnector for DamengSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    /// 解析配置、建写入方式。**不连库** —— 连不上会在第一次写入时大声失败，
    /// 而"配置本身对不对"（方式名、参数块、连接串）在这里就定死。
    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        let kind = DamengWriteKind::parse(spec.method.as_deref())?;
        self.kind = kind;
        let conn = ConnectionConfig::from_json(&spec.connection)?;
        let props = spec.props();

        let ctx = Arc::new(WriteContext {
            schema: Some(spec.database.clone()).filter(|s| !s.trim().is_empty()),
            table: spec.table.clone(),
            // 连接配置除了给会话，dmfldr 那条路组装 USERID 也要用 —— 两边各持一份 clone
            conn: conn.clone(),
            max_params: DamengInsertMethod::max_params_of(&props),
        });

        info!(
            connector = %self.name,
            method = %kind,
            target = %ctx.qualified_table(),
            direction = "write",
            "initializing dameng sink"
        );

        self.method = Some(match kind {
            DamengWriteKind::Odbc => Arc::new(DamengInsertMethod),
            DamengWriteKind::FlDr => {
                // 旋钮在**启动期**就校验掉（拼错的键、越界的取值），别等第一批数据才报
                let p = FlDrInParams::from_props(&props)?;
                p.validate()?;
                // 与读侧同一件事：清掉上次被 kill 留下的导出物
                let swept = crate::sink::method::dmfldr::sweep_stale_in_dirs(&p);
                if swept > 0 {
                    tracing::info!(dirs = swept, "清掉了上次异常退出留下的 dmfldr 导出目录");
                }
                info!(
                    method = "dmfldr",
                    separator = %p.field_seperator,
                    null_str = %p.null_str,
                    executable = %p.executable,
                    "达梦写入方式"
                );
                Arc::new(DamengFastLoadMethod::new(p, conn.clone()))
            }
            DamengWriteKind::Dpi => {
                // 旋钮在启动期校验；动态库也在启动期加载 —— 缺库/缺符号
                // 不该等到第一批数据才暴露。
                let p = DpiSinkParams::from_props(&props)?;
                p.validate()?;
                let library = DpiLibrary::load(p.library.as_deref())?;
                info!(
                    method = "dpi",
                    param_rows = p.param_rows,
                    max_buffer_mb = p.max_buffer_mb,
                    library = %p
                        .library
                        .as_ref()
                        .map(|x| x.display().to_string())
                        .unwrap_or_else(|| "<系统路径>".to_string()),
                    "达梦写入方式"
                );
                Arc::new(DamengDpiWriteMethod::new(p, conn.clone(), library)?)
            }
        });
        self.ctx = Some(ctx);
        Ok(())
    }

    async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let ctx = self
            .ctx
            .clone()
            .ok_or_else(|| D2DError::config("dameng sink not initialized: call init() first"))?;
        let method = self
            .method
            .clone()
            .ok_or_else(|| D2DError::config("dameng sink not initialized: call init() first"))?;
        method.begin_session(ctx).await
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing dameng sink");
        if let Some(m) = &self.method {
            m.close().await?;
        }
        self.method = None;
        Ok(())
    }
}

impl Default for DamengSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink_spec(method: Option<&str>) -> SinkSpec {
        serde_json::from_value(serde_json::json!({
            "connector": "dameng",
            "connection": {"host": "h", "user": "u", "password": "p"},
            "database": "PROD",
            "table": "t_b",
            "method": method,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn test_init_resolves_method_and_target() {
        let mut c = DamengSinkConnector::new();
        c.init(&sink_spec(None)).await.unwrap();
        assert_eq!(c.kind, DamengWriteKind::Odbc, "缺省 odbc");
        assert_eq!(
            c.ctx.as_ref().unwrap().qualified_table(),
            "\"PROD\".\"t_b\""
        );
        assert!(c.method.is_some());
    }

    #[tokio::test]
    async fn test_unknown_method_is_rejected_with_options() {
        let mut c = DamengSinkConnector::new();
        let e = c
            .init(&sink_spec(Some("jdbc")))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("odbc"), "要列出可选值: {}", e);
        assert!(e.contains("dmfldr"), "要列出可选值: {}", e);
        assert!(e.contains("dpi"), "要列出可选值: {}", e);
    }

    #[test]
    fn test_dpi_method_parses() {
        assert_eq!(
            DamengWriteKind::parse(Some("dpi")).unwrap(),
            DamengWriteKind::Dpi
        );
        assert_eq!(DamengWriteKind::Dpi.as_str(), "dpi");
    }

    /// 没 init 就开会话 → 明确报错（而不是造出一个连不上库的会话）
    #[tokio::test]
    async fn test_begin_session_before_init_fails() {
        let c = DamengSinkConnector::new();
        // `Box<dyn SinkSession>` 不是 Debug，`unwrap_err` 用不了
        let e = match c.begin_session(&sink_spec(None)).await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("没 init 不该造得出会话"),
        };
        assert!(e.contains("not initialized"), "{}", e);
    }

    /// `max_params` 是连接器的物理约束，走 properties；写错类型要能看出来
    #[tokio::test]
    async fn test_max_params_comes_from_properties() {
        let mut spec = sink_spec(None);
        spec.properties = Some(
            serde_json::json!({"max_params": 500})
                .as_object()
                .unwrap()
                .clone(),
        );
        assert_eq!(DamengInsertMethod::max_params_of(&spec.props()), 500);
        // 缺省是安全值；0/非法值不能传下去（0 会让切段除零）
        assert_eq!(
            DamengInsertMethod::max_params_of(&serde_json::Map::new()),
            super::method::odbc::DEFAULT_MAX_PARAMS
        );
        let zero = serde_json::json!({"max_params": 0});
        assert_eq!(
            DamengInsertMethod::max_params_of(zero.as_object().unwrap()),
            super::method::odbc::DEFAULT_MAX_PARAMS
        );
    }
}
