//! 达梦连接配置 → dmfldr / ODBC 通用的 USERID 串
//!
//! USERID 语法（手册 2.3）：
//! `{{<user>[/<password>]} | /}[@<connect_identifier>][<option>]`
//! `<connect_identifier> ::= <svc_name> | {<host>[:<port>]}`

use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};

/// 连接配置（source.connection / sink.connection 的强类型视图）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// dm_svc.conf 服务名（与 host:port 二选一，优先）
    #[serde(default)]
    pub svc: Option<String>,
    /// 登录身份：SYSDBA / SYSSSO / SYSAUDITOR / USERS / AUTO
    #[serde(default)]
    pub auth_as: Option<String>,

    // ── 以下两项只被 ODBC 路径使用 ──
    /// ODBC 数据源名（`/etc/odbc.ini` 里那一条，如 `DM8`）
    ///
    /// **推荐用它**：驱动名与主机解析交给 odbc.ini / dm_svc.conf 管，
    /// 配置里就不必把主机端口再写一遍。给了它就不看 `host`/`port`。
    #[serde(default)]
    pub dsn: Option<String>,
    /// DSN-less 连接时用的 ODBC 驱动名（缺省 `DM8 ODBC DRIVER`，即 odbcinst.ini 里的节名）
    #[serde(default)]
    pub odbc_driver: Option<String>,
}

fn default_port() -> u16 {
    5236
}

/// ODBC 连接串里的值：含 `;`、`{`、`}` 时必须用大括号包起来
///
/// 达梦密码里常见的 `#` 在 ODBC 里不是特殊字符（那是 **dmfldr USERID 串**的坑，
/// 见 [`ConnectionConfig::to_userid`]），但分号会截断属性列表 —— 这一层要单独转义。
fn odbc_value(v: &str) -> String {
    if v.contains(';') || v.contains('{') || v.contains('}') {
        format!("{{{}}}", v)
    } else {
        v.to_string()
    }
}

impl ConnectionConfig {
    /// 从 spec.connection JSON 解析
    pub fn from_json(v: &serde_json::Value) -> D2DResult<Self> {
        serde_json::from_value(v.clone())
            .map_err(|e| D2DError::config(format!("invalid dameng connection config: {}", e)))
    }

    /// 组装 dmfldr USERID 值：`user/"password"@host:port`
    ///
    /// 密码用双引号封闭（非空时）。原因：达梦把 `#` 当作扩展选项引导符
    /// （`...#{k=v,...}`），密码里含 `#` 时裸写会被解析成选项起始，报
    /// 「无效的USERID串」。实测：
    /// - `DEMO_USER/s3cret@h:port` → 无效的USERID串（失败）
    /// - `DEMO_USER/"s3cret"@h:port` → 正常
    /// - 给普通密码也加引号不影响解析（错误密码在两种写法下都报「用户名或密码错误」）
    pub fn to_userid(&self) -> String {
        let ident = match &self.svc {
            Some(s) if !s.trim().is_empty() => s.trim().to_string(),
            _ => format!("{}:{}", self.host, self.port),
        };
        let auth = match &self.auth_as {
            Some(a) if !a.trim().is_empty() => format!(" AS {}", a.trim().to_uppercase()),
            _ => String::new(),
        };
        if self.password.is_empty() {
            format!("{}@{}{}", self.user, ident, auth)
        } else {
            format!("{}/\"{}\"@{}{}", self.user, self.password, ident, auth)
        }
    }

    /// 组装 ODBC 连接串
    ///
    /// 两种形态，**优先 DSN**：
    ///
    /// ```text
    /// 给了 dsn:   DSN=DM8;UID=APP;PWD=***
    /// 没给 dsn:   DRIVER={DM8 ODBC DRIVER};SERVER=192.0.2.13;PORT=5238;UID=APP;PWD=***
    /// ```
    ///
    /// 为什么优先 DSN：DSN 把驱动路径与主机解析交给 `odbc.ini` / `dm_svc.conf`，
    /// 而 DSN-less 串要自己把属性名拼对 —— 达梦驱动的属性名与 ODBC 标准并不完全一致，
    /// 实测 `SERVER=` 直连形态会被驱动拒（`Invalid string or buffer length`）。
    /// 有 DSN 就用 DSN，不跟属性名较劲。
    pub fn to_odbc_connection_string(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        match self.dsn.as_ref().filter(|s| !s.trim().is_empty()) {
            Some(dsn) => parts.push(format!("DSN={}", odbc_value(dsn.trim()))),
            None => {
                let driver = self
                    .odbc_driver
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("DM8 ODBC DRIVER");
                parts.push(format!("DRIVER={{{}}}", driver));
                let host = self
                    .svc
                    .as_deref()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or(self.host.as_str());
                parts.push(format!("SERVER={}", odbc_value(host.trim())));
                parts.push(format!("PORT={}", self.port));
            }
        }
        parts.push(format!("UID={}", odbc_value(&self.user)));
        if !self.password.is_empty() {
            parts.push(format!("PWD={}", odbc_value(&self.password)));
        }
        parts.join(";")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_userid_host_port() {
        let c = ConnectionConfig::from_json(&json!({
            "host": "192.0.2.1", "port": 5236, "user": "SYSDBA", "password": "p@ss"
        }))
        .unwrap();
        assert_eq!(c.to_userid(), "SYSDBA/\"p@ss\"@192.0.2.1:5236");
    }

    /// 回归：密码含 `#`（达梦用它引导扩展选项 `#{k=v}`）——
    /// 裸写实测报「无效的USERID串」，必须用双引号包住密码
    #[test]
    fn test_userid_password_with_hash_is_quoted() {
        let c = ConnectionConfig::from_json(&json!({
            "host": "db.example", "port": 5236, "user": "APPUSER", "password": "p#ss"
        }))
        .unwrap();
        assert_eq!(c.to_userid(), "APPUSER/\"p#ss\"@db.example:5236");
    }

    #[test]
    fn test_userid_svc_priority() {
        let c = ConnectionConfig::from_json(&json!({
            "host": "ignored", "user": "U", "password": "P", "svc": "DMSERVER"
        }))
        .unwrap();
        assert_eq!(c.to_userid(), "U/\"P\"@DMSERVER");
    }

    #[test]
    fn test_userid_auth_as() {
        let c = ConnectionConfig::from_json(&json!({
            "host": "h", "user": "U", "password": "P", "auth_as": "sysdba"
        }))
        .unwrap();
        assert_eq!(c.to_userid(), "U/\"P\"@h:5236 AS SYSDBA");
    }

    #[test]
    fn test_userid_no_password() {
        let c = ConnectionConfig::from_json(&json!({"host": "h", "user": "U"})).unwrap();
        assert_eq!(c.to_userid(), "U@h:5236");
    }

    // ───────── ODBC 连接串 ─────────

    #[test]
    fn test_odbc_conn_string_prefers_dsn() {
        let c = ConnectionConfig::from_json(&json!({
            "dsn": "DM8", "host": "ignored", "port": 1, "user": "APP", "password": "pw"
        }))
        .unwrap();
        assert_eq!(c.to_odbc_connection_string(), "DSN=DM8;UID=APP;PWD=pw");
    }

    #[test]
    fn test_odbc_conn_string_dsn_less_uses_driver_and_host() {
        let c = ConnectionConfig::from_json(&json!({
            "host": "192.0.2.13", "port": 5238, "user": "APP", "password": "pw"
        }))
        .unwrap();
        assert_eq!(
            c.to_odbc_connection_string(),
            "DRIVER={DM8 ODBC DRIVER};SERVER=192.0.2.13;PORT=5238;UID=APP;PWD=pw"
        );
    }

    /// 密码里出现 `;` 会把属性列表截断 —— 必须用大括号包起来
    #[test]
    fn test_odbc_conn_string_escapes_semicolon_in_password() {
        let c = ConnectionConfig::from_json(&json!({
            "dsn": "D", "user": "U", "password": "a;b"
        }))
        .unwrap();
        assert_eq!(c.to_odbc_connection_string(), "DSN=D;UID=U;PWD={a;b}");
    }

    /// 达梦密码里的 `#` 在 **ODBC** 里不是特殊字符（那是 dmfldr USERID 串的坑），
    /// 不该被转义 —— 两个函数对同一个密码的处理刻意不同。
    #[test]
    fn test_odbc_conn_string_leaves_hash_alone() {
        let c = ConnectionConfig::from_json(&json!({
            "dsn": "D", "user": "U", "password": "p#ss"
        }))
        .unwrap();
        assert_eq!(c.to_odbc_connection_string(), "DSN=D;UID=U;PWD=p#ss");
        assert_eq!(c.to_userid(), "U/\"p#ss\"@:5236");
    }

    #[test]
    fn test_odbc_conn_string_no_password() {
        let c = ConnectionConfig::from_json(&json!({"dsn": "D", "user": "U"})).unwrap();
        assert_eq!(c.to_odbc_connection_string(), "DSN=D;UID=U");
    }
}
