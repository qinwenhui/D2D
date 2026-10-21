//! QSDB 连接配置：**只支持 `qsdb server`（HTTP）模式**
//!
//! ```yaml
//! source:
//!   connector: "qsdb"
//!   method: "http"                       # 可省略；只给 url 时自动选 http
//!   connection:
//!     url: "http://192.0.2.10:9430"
//!     user: "${QSDB_USER}"               # Basic；或 token: "${QSDB_TOKEN}"
//!     password: "${QSDB_PASS}"
//!     connect_timeout_ms: 10000
//!     query_timeout_ms: 0                # 0/缺省 = 不设服务端超时
//!   table: "events"
//! ```
//!
//! # 为什么没有 embedded
//!
//! 进程内直接打开数据目录（`connection.path`）依赖 QSDB 的 `qsdb` crate，
//! 而它没有发布到 crates.io —— 公开分支无法自包含。QSDB 统一以 server 模式
//! 部署时，官方 `qsdb-client` 足够覆盖读、写两个方向：
//!
//! - 读：`query_stream`（rows 通道，**客户端内存恒定**）→ 转成 Arrow 批
//! - 写：`append_batches`（Arrow IPC）→ 服务端 `Qsdb::append`
//!
//! ⚠️ `qsdb-client` **不做 TLS**：只接受 `http://host:port`。要加密请在 QSDB
//! 前面挂 TLS 终止代理（这是 QSDB 自己的既定立场，不是本连接器的限制）。
//! 另外 HTTP 服务是**单库**的（服务端启动时指定的那个库），`database` /
//! `schema_name` 只能是 `default`。

use d2d_core::{D2DError, D2DResult};
use std::time::Duration;

/// QSDB HTTP 服务启动时指定的库名
pub const DEFAULT_DB: &str = "default";

#[derive(Debug, Clone)]
pub struct QsdbConnection {
    /// `qsdb server` 地址，如 `http://192.0.2.10:9430`
    pub url: Option<String>,
    /// Basic 认证用户名
    pub user: Option<String>,
    pub password: Option<String>,
    /// Bearer token（与 user/password 二选一）
    pub token: Option<String>,
    /// TCP 建连超时（毫秒；缺省 10s）
    pub connect_timeout_ms: Option<u64>,
    /// 读写 I/O 超时（毫秒；缺省不设）
    pub io_timeout_ms: Option<u64>,
    /// 服务端查询超时（毫秒 → `X-QSDB-Timeout-Ms`；缺省不设）
    pub query_timeout_ms: Option<u64>,
}

impl QsdbConnection {
    pub fn from_json(conn: &serde_json::Value) -> D2DResult<Self> {
        let get_str = |keys: &[&str]| -> Option<String> {
            keys.iter().find_map(|k| {
                conn.get(*k)
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
            })
        };

        // embedded 专属配置：直接拒绝，而不是"忽略后报缺 url"。
        // 用户从老配置迁移过来时，需要看到"为什么 path 不再生效"。
        if get_str(&["path"]).is_some() {
            return Err(D2DError::config(
                "qsdb 现在只支持 server 模式：请先启动 `qsdb server`，\
                 并用 connection.url 连接；embedded（connection.path）已移除",
            ));
        }
        for k in ["cache_mb", "read_only"] {
            if conn.get(k).is_some() {
                return Err(D2DError::config(format!(
                    "qsdb connection.{k} 属于已移除的 embedded 模式；\
                     server 模式的缓存/只读在启动 `qsdb server` 时配置"
                )));
            }
        }
        if let Some(db) = get_str(&["database", "db"]) {
            if db != DEFAULT_DB {
                return Err(D2DError::config(format!(
                    "qsdb server 是单库服务，connection.database 只能是 `{DEFAULT_DB}`，收到 `{db}`；\
                     库由 `qsdb server` 的启动参数决定"
                )));
            }
        }

        Ok(Self {
            url: get_str(&["url", "endpoint", "addr"]),
            user: get_str(&["user", "username"]),
            password: get_str(&["password", "pass"]),
            token: get_str(&["token"]),
            connect_timeout_ms: conn.get("connect_timeout_ms").and_then(|v| v.as_u64()),
            io_timeout_ms: conn.get("io_timeout_ms").and_then(|v| v.as_u64()),
            query_timeout_ms: conn.get("query_timeout_ms").and_then(|v| v.as_u64()),
        })
    }

    /// `method` 只接受 http/server/client（或省略）
    pub fn validate_method(method: Option<&str>) -> D2DResult<()> {
        match method
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("http") | Some("server") | Some("client") => Ok(()),
            Some("embedded") | Some("local") | Some("file") => Err(D2DError::config(
                "qsdb 现在只支持 server 模式（method: http）；\
                 embedded 已移除 —— 请先启动 `qsdb server`，并用 connection.url 连接",
            )),
            Some(other) => Err(D2DError::config(format!(
                "未知 qsdb method `{other}`：只支持 http（qsdb server）"
            ))),
        }
    }

    fn require_url(&self) -> D2DResult<&str> {
        self.url.as_deref().ok_or_else(|| {
            D2DError::config(
                "qsdb server 模式需要 connection.url（如 http://192.0.2.10:9430）—— \
                 端口必填，不猜默认端口",
            )
        })
    }

    /// 构造一个 `qsdb-client`（惰性建连，构造本身不发请求）
    pub fn http_client(&self) -> D2DResult<qsdb_client::Client> {
        let url = self.require_url()?;
        if self.token.is_some() && self.user.is_some() {
            return Err(D2DError::config(
                "qsdb connection 的 token 与 user/password 只能二选一",
            ));
        }
        let auth = match (&self.token, &self.user) {
            (Some(t), None) => qsdb_client::Auth::Bearer(t.clone()),
            (None, Some(u)) => qsdb_client::Auth::Basic {
                user: u.clone(),
                password: self.password.clone().unwrap_or_default(),
            },
            _ => qsdb_client::Auth::None,
        };
        let opts = qsdb_client::ConnectOptions {
            connect_timeout: Some(
                self.connect_timeout_ms
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_secs(10)),
            ),
            io_timeout: self.io_timeout_ms.map(Duration::from_millis),
        };
        qsdb_client::Client::with_options(url, auth, opts)
            .map_err(|e| D2DError::external("qsdb-http", format!("连接配置 {url} 无效: {e}")))
    }

    /// 查询选项（格式由调用方改；这里只带服务端超时）
    pub fn http_query_options(&self) -> qsdb_client::QueryOptions {
        qsdb_client::QueryOptions {
            timeout_ms: self.query_timeout_ms.filter(|ms| *ms > 0),
            ..Default::default()
        }
    }
}

/// HTTP 模式的库名：`qsdb server` 是单库服务，库名由服务端启动参数决定
pub fn http_db_name(spec_db: Option<&str>) -> D2DResult<()> {
    if let Some(db) = spec_db.map(str::trim).filter(|s| !s.is_empty()) {
        if db != DEFAULT_DB {
            return Err(D2DError::config(format!(
                "qsdb server 是**单库**服务，不支持指定 database/schema_name=`{db}`；\
                 请删掉该字段（或设为 default）"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_http() {
        let c = QsdbConnection::from_json(&json!({
            "url": "http://h:9430", "user": "u", "password": "p"
        }))
        .unwrap();
        assert_eq!(c.url.as_deref(), Some("http://h:9430"));
        assert_eq!(c.user.as_deref(), Some("u"));
        assert!(c.http_client().is_ok());
        assert_eq!(c.http_query_options().timeout_ms, None);
    }

    #[test]
    fn test_embedded_config_is_rejected() {
        let e = QsdbConnection::from_json(&json!({"path": "/data/qsdb"}))
            .unwrap_err()
            .to_string();
        assert!(e.contains("server"), "{}", e);
        assert!(e.contains("url"), "{}", e);

        let e = QsdbConnection::from_json(&json!({
            "url": "http://h:9430", "cache_mb": 1024
        }))
        .unwrap_err()
        .to_string();
        assert!(e.contains("cache_mb"), "{}", e);
    }

    #[test]
    fn test_missing_url() {
        let c = QsdbConnection::from_json(&json!({"user": "u"})).unwrap();
        let e = c.http_client().unwrap_err().to_string();
        assert!(e.contains("url"), "{}", e);
    }

    #[test]
    fn test_method_validation() {
        assert!(QsdbConnection::validate_method(None).is_ok());
        assert!(QsdbConnection::validate_method(Some("http")).is_ok());
        assert!(QsdbConnection::validate_method(Some("server")).is_ok());
        let e = QsdbConnection::validate_method(Some("embedded"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("server"), "{}", e);
        assert!(QsdbConnection::validate_method(Some("jdbc")).is_err());
    }

    #[test]
    fn test_http_options_and_auth_conflict() {
        let c = QsdbConnection::from_json(&json!({
            "url": "http://h:9430", "token": "t", "user": "u"
        }))
        .unwrap();
        let e = c.http_client().unwrap_err().to_string();
        assert!(e.contains("二选一"), "{}", e);

        let c = QsdbConnection::from_json(&json!({
            "url": "http://h:9430", "query_timeout_ms": 5000
        }))
        .unwrap();
        assert!(c.http_client().is_ok());
        assert_eq!(c.http_query_options().timeout_ms, Some(5000));
    }

    #[test]
    fn test_http_db_name() {
        assert!(http_db_name(None).is_ok());
        assert!(http_db_name(Some("default")).is_ok());
        let e = http_db_name(Some("app")).unwrap_err().to_string();
        assert!(e.contains("单库"), "{}", e);

        let e = QsdbConnection::from_json(&json!({
            "url": "http://h:9430", "database": "app"
        }))
        .unwrap_err()
        .to_string();
        assert!(e.contains("单库"), "{}", e);
    }
}
