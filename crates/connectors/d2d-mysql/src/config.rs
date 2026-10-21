//! MySQL 连接配置
//!
//! 读写共用一份：源和目标都只需要"连哪个库"。
//!
//! ```yaml
//! source:
//!   connector: "mysql"
//!   connection: { host: "192.0.2.10", port: 3306, user: "root", password: "..." }
//!   schema_name: "db_a"
//!   table: "t_a"
//! ```

use d2d_core::{D2DError, D2DResult};
use mysql_async::{Opts, OptsBuilder, Pool};

/// MySQL 连接参数
#[derive(Debug, Clone)]
pub struct MysqlConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// 库名。源/目标的 `source.schema_name` / `sink.database` 会覆盖它 ——
    /// 这里只作为 `connection.database` 的兼容写法保留。
    pub database: Option<String>,
}

impl MysqlConnection {
    /// 从 `source.connection` / `sink.connection` 那段 JSON 解析
    pub fn from_json(conn: &serde_json::Value) -> D2DResult<Self> {
        let host = conn
            .get("host")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| D2DError::config("mysql connection 需要 host（如 192.0.2.10）"))?
            .to_string();
        let port = conn
            .get("port")
            .and_then(|v| v.as_u64())
            .unwrap_or(3306) as u16;
        let user = conn
            .get("user")
            .or_else(|| conn.get("username"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
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
        Ok(Self { host, port, user, password, database })
    }

    /// 建连选项
    ///
    /// ⚠️ `prefer_socket(false)` + 显式 `max_allowed_packet` / `wait_timeout`：
    /// mysql_async 缺省会在握手后发一句
    /// `SELECT @@max_allowed_packet, @@wait_timeout, @@socket` 向服务端要设置。
    /// MySQL 本身支持这句，但显式给值有两个好处：少一次往返；以及
    /// **和 Doris 那条路共用同一套建连姿势**（Doris 的 FE 不实现 `@@socket`，
    /// 不显式给就会连不上）。
    pub fn opts(&self, database: Option<&str>) -> Opts {
        OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.clone()))
            .db_name(database.map(|s| s.to_string()))
            .prefer_socket(false)
            .max_allowed_packet(Some(MAX_ALLOWED_PACKET))
            .wait_timeout(Some(WAIT_TIMEOUT_SECS))
            .into()
    }

    /// 建一个单主机连接池
    pub fn pool(&self, database: Option<&str>) -> Pool {
        Pool::new(self.opts(database))
    }
}

/// 客户端读包上限（与 Doris Source 同一个量级口径）
pub const MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;
const WAIT_TIMEOUT_SECS: usize = 28800;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_defaults_and_aliases() {
        let c = MysqlConnection::from_json(&json!({
            "host": "192.0.2.10", "user": "root", "password": "p"
        }))
        .unwrap();
        assert_eq!((c.port, c.user.as_str(), c.password.as_str()), (3306, "root", "p"));
        assert_eq!(c.database, None);

        // username / database 也收
        let c = MysqlConnection::from_json(&json!({
            "host": "h", "port": 3307, "username": "u", "password": "p", "database": "db"
        }))
        .unwrap();
        assert_eq!(c.port, 3307);
        assert_eq!(c.user, "u");
        assert_eq!(c.database.as_deref(), Some("db"));
    }

    #[test]
    fn test_host_is_required() {
        let e = MysqlConnection::from_json(&json!({"port": 3306})).unwrap_err().to_string();
        assert!(e.contains("host"), "{}", e);
        let e = MysqlConnection::from_json(&json!({"host": "   "})).unwrap_err().to_string();
        assert!(e.contains("host"), "{}", e);
    }

    /// 建连选项必须显式给那三项（与 Doris 同一条纪律：别让驱动去问服务端）
    #[test]
    fn test_opts_are_explicit() {
        let c = MysqlConnection::from_json(&json!({"host": "h", "user": "u", "password": "p"})).unwrap();
        let o = c.opts(Some("db"));
        assert!(!o.prefer_socket());
        assert!(o.max_allowed_packet().is_some());
        assert!(o.wait_timeout().is_some());
        assert_eq!(o.ip_or_hostname(), "h");
        assert_eq!(o.tcp_port(), 3306);
        assert_eq!(o.db_name(), Some("db"));
    }
}
