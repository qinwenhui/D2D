//! PostgreSQL 连接配置
//!
//! ```yaml
//! source:
//!   connector: "postgres"
//!   connection: { host: "192.0.2.13", port: 5432, user: "postgres", password: "..." }
//!   schema_name: "public"
//!   table: "t_a"
//! ```
//!
//! # 会话时区固定 UTC
//!
//! `timestamp with time zone` 经 `::text` 出来带 `+00` 偏移；`timestamp without time zone`
//! 本身没有偏移。两边的"挂钟时间"只有在**会话时区确定**时才是确定的，所以建连时
//! 显式 `timezone=UTC` —— 不靠服务器/用户的默认设置（那会让同一份数据在不同机器上
//! 迁出不同的值）。

use d2d_core::{D2DError, D2DResult};
use tokio_postgres::Config;

#[derive(Debug, Clone)]
pub struct PgConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// 库名（`schema_name`/`database` 会覆盖它）
    pub database: Option<String>,
}

impl PgConnection {
    pub fn from_json(conn: &serde_json::Value) -> D2DResult<Self> {
        let host = conn
            .get("host")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| D2DError::config("postgres connection 需要 host（如 192.0.2.13）"))?
            .to_string();
        let port = conn.get("port").and_then(|v| v.as_u64()).unwrap_or(5432) as u16;
        let user = conn
            .get("user")
            .or_else(|| conn.get("username"))
            .and_then(|v| v.as_str())
            .unwrap_or("postgres")
            .to_string();
        let password = conn
            .get("password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let database = conn
            .get("database")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
        })
    }

    pub fn config(&self, database: &str) -> Config {
        let mut c = Config::new();
        c.host(self.host.clone())
            .port(self.port)
            .user(self.user.clone())
            .password(self.password.clone())
            .dbname(database)
            .options("-c timezone=UTC");
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_defaults_and_aliases() {
        let c = PgConnection::from_json(&json!({"host": "h", "password": "p"})).unwrap();
        assert_eq!((c.port, c.user.as_str()), (5432, "postgres"));
        let c = PgConnection::from_json(
            &json!({"host": "h", "port": 5433, "username": "u", "database": "db"}),
        )
        .unwrap();
        assert_eq!(c.port, 5433);
        assert_eq!(c.user, "u");
        assert_eq!(c.database.as_deref(), Some("db"));
    }

    #[test]
    fn test_host_required() {
        assert!(PgConnection::from_json(&json!({})).is_err());
        assert!(PgConnection::from_json(&json!({"host": "  "})).is_err());
    }

    /// 会话时区必须显式钉成 UTC
    #[test]
    fn test_session_timezone_is_utc() {
        let c = PgConnection::from_json(&json!({"host": "h"})).unwrap();
        let cfg = c.config("db");
        assert_eq!(cfg.get_options(), Some("-c timezone=UTC"));
        assert_eq!(cfg.get_dbname(), Some("db"));
    }
}
