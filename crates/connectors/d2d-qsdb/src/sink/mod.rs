//! QSDB **writer 方向**：HTTP Arrow IPC
//!
//! 通过 `qsdb-client::append_batches` 把 Arrow 批编码成 IPC 流，POST 到
//! `qsdb server` 的 `/api/v1/write?format=ipc&mode=append`。
//!
//! # 一次 `write_batch` = 一次提交
//!
//! 内核保证一次 `write_batch` 就是一份攒好的数据，所以连接器内部**不再攒批**，
//! 直接把这份数据交给 `append_batches`。QSDB 的 append 语义是"一次调用按分区
//! 归并后提交"。
//!
//! # 库名
//!
//! `qsdb server` 是**单库**服务，库由服务端启动参数决定。`sink.database`
//! 必须是 `default`，否则启动期直接报配置错误（不静默改写）。

pub mod session;

use async_trait::async_trait;
use d2d_connector::{SinkConnector, SinkSession, SinkSpec};
use d2d_core::{D2DError, D2DResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::config::{http_db_name, QsdbConnection};
use session::{CreateOptions, QsdbSinkSession};

pub struct QsdbSinkConnector {
    name: String,
    /// HTTP 连接配置（每个会话/每次请求自己建 client，连接可并行）
    http: Option<QsdbConnection>,
    table: Option<String>,
    opts: Option<CreateOptions>,
    /// 表已确认存在（跨会话共享，建一次即可）
    table_ready: Arc<AtomicBool>,
    /// 首批写入的建表互斥（跨会话共享）
    create_lock: Arc<Mutex<()>>,
}

impl QsdbSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "qsdb".to_string(),
            http: None,
            table: None,
            opts: None,
            table_ready: Arc::new(AtomicBool::new(false)),
            create_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl Default for QsdbSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for QsdbSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        QsdbConnection::validate_method(spec.method.as_deref())?;
        let connection = QsdbConnection::from_json(&spec.connection)?;
        http_db_name(Some(&spec.database))?;
        let opts = CreateOptions::from_props(&spec.props())?;
        let table = spec.table.clone();
        let create_table = opts.create_table;
        let conn = connection.clone();

        let ready = tokio::task::spawn_blocking(move || -> D2DResult<bool> {
            let client = conn.http_client()?;
            let exists = client
                .tables()
                .map_err(|e| D2DError::external("qsdb-http", format!("列出表失败: {e}")))?
                .iter()
                .any(|t| t == &table);
            if !exists && !create_table {
                return Err(D2DError::config(format!(
                    "QSDB 表 `default.{table}` 不存在；请先建表，\
                     或设置 sink.properties.create_table: true（按首批 Arrow schema 自动建表）"
                )));
            }
            Ok(exists || !create_table)
        })
        .await
        .map_err(|e| D2DError::internal(format!("qsdb http 初始化任务失败: {e}")))??;

        info!(
            connector = %self.name,
            table = %spec.table,
            url = connection.url.as_deref().unwrap_or(""),
            create_table,
            partition_by = ?opts.partition_by,
            compression = ?opts.compression,
            "qsdb sink 已连接"
        );

        self.http = Some(connection);
        self.table_ready.store(ready, Ordering::Release);
        self.table = Some(spec.table.clone());
        self.opts = Some(opts);
        Ok(())
    }

    async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let table = self
            .table
            .clone()
            .ok_or_else(|| D2DError::internal("qsdb sink 未初始化：先调 init()"))?;
        let opts = self
            .opts
            .clone()
            .ok_or_else(|| D2DError::internal("qsdb sink 未初始化：先调 init()"))?;
        let conn = self
            .http
            .as_ref()
            .ok_or_else(|| D2DError::internal("qsdb sink 未初始化：先调 init()"))?;
        // 每个会话一条 client：批间复用 keep-alive，会话之间各自并行
        let client = Arc::new(conn.http_client()?);

        Ok(Box::new(QsdbSinkSession::new(
            client,
            table,
            opts,
            Arc::clone(&self.table_ready),
            Arc::clone(&self.create_lock),
        )))
    }

    async fn close(&mut self) -> D2DResult<()> {
        self.http = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        assert_eq!(QsdbSinkConnector::new().name(), "qsdb");
    }
}
