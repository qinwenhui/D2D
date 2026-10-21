//! Doris **Arrow Flight SQL** 读路径（`source.method: flight`）
//!
//! # 为什么有它
//!
//! MySQL 协议那条路要逐行解码再重建 Arrow。30M × 28 列的实测里，这占了
//! 整个任务 ~60/62s，是明确的 CPU 瓶颈。Flight SQL 直接返回 Arrow 批：
//! 没有行解析、没有类型重建，列式数据从 BE 直接流到本进程。
//!
//! # 协议形状
//!
//! ```text
//! FE:8070  ── execute(SQL) ──► FlightInfo{schema, endpoints[]}
//!                                      │ 每个 endpoint 带 ticket + location
//!                                      ▼
//! BE:8050  ── do_get(ticket) ──► FlightRecordBatchStream（Arrow 批）
//! ```
//!
//! FE 返回的 `location` 通常是 **BE** 的 Flight 端口（`grpc+tcp://be:8050`），
//! 不是 FE 自己。必须按 location 连到 BE 取 ticket；直接拿 FE 的 client
//! `do_get` 会得到服务端的 Internal error。
//!
//! # 认证
//!
//! FE 与 BE 都接受 Arrow Flight SQL 的 `Handshake`（Basic user/password）。
//! 每个 endpoint 的连接都会握手一次；ticket 本身不当作认证凭据使用。
//!
//! # 分片
//!
//! 一条 SQL 仍由一个分片执行；分片谓词、列裁剪、LIMIT 全部走 SQL 下推。
//! `FlightInfo` 可以返回多个 endpoint（多个 scan range），本模块会逐个取完。

use crate::config::DorisConnection;
use crate::source::sql::DorisSqlBuilder;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::sql::client::FlightSqlServiceClient;
use d2d_connector::QueryConfig;
use d2d_core::{D2DError, D2DResult};
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::Channel;

/// 一条 Flight 连接/流的标称缓冲（内存账用）
pub const NOMINAL_FLIGHT_BUFFER: usize = 16 * 1024 * 1024;

/// 初始化成功的 Flight 源：FE 地址、认证信息与 Arrow schema
#[derive(Clone)]
pub struct FlightSource {
    user: String,
    password: String,
    /// FE 的 Flight SQL URL（初始化时至少一个可用）
    fe_urls: Vec<String>,
    schema: SchemaRef,
}

impl FlightSource {
    /// 连 FE、跑一次 `LIMIT 0` 拿 Arrow schema
    ///
    /// `query` 已经由调用方补过分区列；这里只把 LIMIT 换成 0，不取数据。
    pub async fn init(
        conn: &DorisConnection,
        database: &str,
        table: &str,
        query: &QueryConfig,
    ) -> D2DResult<Self> {
        let builder = DorisSqlBuilder::new(Some(database), table);
        let mut probe = query.clone();
        probe.limit = Some(0);
        let sql = builder.build_query(&probe, "");

        let fe_urls: Vec<String> = conn.fe_hosts.iter().map(|h| fe_url(h, conn)).collect();
        let mut errors = Vec::new();
        let mut found = None;
        for url in &fe_urls {
            match Self::probe(url, &conn.username, &conn.password, &sql).await {
                Ok(schema) => {
                    found = Some(schema);
                    break;
                }
                Err(e) => errors.push(format!("{url}: {e}")),
            }
        }
        let Some(schema) = found else {
            return Err(D2DError::external(
                "doris-flight",
                format!(
                    "所有 FE 的 Flight SQL 都不可用（端口 {}）：{}",
                    conn.flight_port,
                    errors.join("; ")
                ),
            ));
        };
        Ok(Self {
            user: conn.username.clone(),
            password: conn.password.clone(),
            fe_urls,
            schema,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn probe(url: &str, user: &str, password: &str, sql: &str) -> D2DResult<SchemaRef> {
        let mut client = connect(url, user, password).await?;
        let info = client
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| flight_err(format!("执行 schema 探测（{url}）"), e))?;
        let schema = Schema::try_from(info).map_err(|e| {
            D2DError::schema(format!("Doris Flight SQL 返回的 schema 无法解析: {e}"))
        })?;
        Ok(Arc::new(schema))
    }

    /// 执行一个分片的 SQL，把 Arrow 批送进有界通道
    ///
    /// 通道满时 `send` 会等待下游，背压自然传回 Flight 流。
    pub async fn read(
        &self,
        sql: &str,
        tx: &mpsc::Sender<D2DResult<arrow::record_batch::RecordBatch>>,
        rows_read: &AtomicU64,
        started: &AtomicBool,
    ) -> D2DResult<()> {
        let (mut fe, fe_url) = self.connect_any().await?;
        let info = fe
            .execute(sql.to_string(), None)
            .await
            .map_err(|e| flight_err(format!("执行查询（{fe_url}）"), e))?;
        if info.endpoint.is_empty() {
            return Err(D2DError::external(
                "doris-flight",
                "FlightInfo 没有返回任何 endpoint（查询没有可取的 scan range）",
            ));
        }
        started.store(true, Ordering::Relaxed);

        for (i, ep) in info.endpoint.iter().enumerate() {
            let ticket = ep.ticket.clone().ok_or_else(|| {
                D2DError::external(
                    "doris-flight",
                    format!("endpoint {i} 没有 ticket，无法取数"),
                )
            })?;
            let url = ep
                .location
                .first()
                .map(|l| normalize_uri(&l.uri))
                .unwrap_or_else(|| fe_url.clone());

            // endpoint 的 location 通常是 BE 的 Flight 端口；按它建立新连接
            let mut client = connect(&url, &self.user, &self.password).await?;
            let mut stream = client
                .do_get(ticket)
                .await
                .map_err(|e| flight_err(format!("do_get（{url}）"), e))?;

            while let Some(item) = stream.next().await {
                let batch = item.map_err(|e| {
                    D2DError::external("doris-flight", format!("读取 Flight 流失败: {e}"))
                })?;
                let n = batch.num_rows();
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(()); // 下游断开
                }
                rows_read.fetch_add(n as u64, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    async fn connect_any(&self) -> D2DResult<(FlightSqlServiceClient<Channel>, String)> {
        let mut errors = Vec::new();
        for url in &self.fe_urls {
            match connect(url, &self.user, &self.password).await {
                Ok(c) => return Ok((c, url.clone())),
                Err(e) => errors.push(format!("{url}: {e}")),
            }
        }
        Err(D2DError::external(
            "doris-flight",
            format!("所有 FE 的 Flight SQL 都连不上: {}", errors.join("; ")),
        ))
    }
}

async fn connect(
    url: &str,
    user: &str,
    password: &str,
) -> D2DResult<FlightSqlServiceClient<Channel>> {
    let channel = Channel::from_shared(url.to_string())
        .map_err(|e| D2DError::config(format!("Doris Flight SQL 地址 {url} 无效: {e}")))?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| D2DError::external("doris-flight", format!("连接 {url} 失败: {e}")))?;
    let mut client = FlightSqlServiceClient::new(channel);
    if !user.trim().is_empty() {
        client.handshake(user, password).await.map_err(|e| {
            D2DError::connection("doris-flight", format!("Flight SQL 认证失败（{url}）: {e}"))
        })?;
    }
    Ok(client)
}

/// FE 的 Flight URL：`host[:query_port]` → `http(s)://host:flight_port`
pub fn fe_url(host: &str, conn: &DorisConnection) -> String {
    let host = host.trim();
    let hostname = match host.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    };
    let scheme = if conn.flight_tls { "https" } else { "http" };
    format!("{scheme}://{hostname}:{}", conn.flight_port)
}

/// Flight endpoint 的 URI → tonic 能吃的 http(s) URI
pub fn normalize_uri(uri: &str) -> String {
    let uri = uri.trim();
    if let Some(rest) = uri.strip_prefix("grpc+tcp://") {
        format!("http://{rest}")
    } else if let Some(rest) = uri.strip_prefix("grpc+tls://") {
        format!("https://{rest}")
    } else if let Some(rest) = uri.strip_prefix("grpc://") {
        format!("http://{rest}")
    } else {
        uri.to_string()
    }
}

fn flight_err(what: impl std::fmt::Display, e: arrow_flight::error::FlightError) -> D2DError {
    D2DError::external("doris-flight", format!("{what} 失败: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> DorisConnection {
        DorisConnection {
            fe_hosts: vec!["fe1".into()],
            be_hosts: vec![],
            database: "db".into(),
            table: "t".into(),
            username: "u".into(),
            password: "p".into(),
            http_port: 8030,
            be_http_port: 8040,
            query_port: 9030,
            flight_port: 8070,
            flight_tls: false,
            load_targets: vec![],
        }
    }

    #[test]
    fn test_fe_url_strips_query_port() {
        assert_eq!(fe_url("fe1", &conn()), "http://fe1:8070");
        assert_eq!(fe_url("fe1:19030", &conn()), "http://fe1:8070");
        assert_eq!(fe_url(" fe1 ", &conn()), "http://fe1:8070");
    }

    #[test]
    fn test_fe_url_tls() {
        let c = DorisConnection {
            flight_tls: true,
            ..conn()
        };
        assert_eq!(fe_url("fe1", &c), "https://fe1:8070");
    }

    #[test]
    fn test_normalize_location() {
        assert_eq!(normalize_uri("grpc+tcp://be1:8050"), "http://be1:8050");
        assert_eq!(normalize_uri("grpc://be1:8050"), "http://be1:8050");
        assert_eq!(normalize_uri("grpc+tls://be1:8050"), "https://be1:8050");
        assert_eq!(normalize_uri("http://be1:8050"), "http://be1:8050");
    }
}
