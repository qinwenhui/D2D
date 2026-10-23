//! qmdb 连接配置：**只走 Arrow Flight SQL 数据面**
//!
//! ```yaml
//! source:
//!   connector: "qmdb"
//!   method: "flight"                     # 可省略；只有这一条路
//!   connection:
//!     addr: "192.0.2.13:31337"           # Flight SQL 端点，host:port 必填
//!     user: "root"
//!     password: "${QMDB_PASS}"
//!     connect_timeout_ms: 5000
//!   schema_name: "bench_lc"              # 库名（qmdb 是多库的，表名一律全限定）
//!   table: "dwd_wide_gen"
//! ```
//!
//! # 为什么不引 `qmdb-client`
//!
//! 它与服务端同仓配套发布，arrow 锁在服务端 DataFusion 的 59.x 上；而内核全线
//! arrow 60（已发布的 qsdb-client 也要求 arrow "60"，降级没有出路）。qmdb 的
//! 线格式 = gRPC(protobuf) + Arrow IPC(flatbuffers)，**跨 arrow crate 版本稳定**
//! （qmdb 方自己的协议文档 §0 明确了这一点），所以这里用 arrow 60 直说 Flight SQL：
//! 查询走标准 `CommandStatementQuery` + DoGet，装载走服务端三段式私有 action
//! （begin_load → do_put(load_id) → commit_load），与 qmdb 的 Rust/Python/JDBC
//! 三个官方客户端同一条服务端路径。
//!
//! # 认证
//!
//! 每个 RPC 带 metadata 头 `authorization: Bearer user:password`（无需握手）。
//! 服务端连续认证失败 10 次会锁来源 IP 300s —— 密码没改对之前不要反复重试。

use d2d_core::{D2DError, D2DResult};
use std::time::Duration;

/// qmdb Flight SQL 的缺省端口（协议文档 §0）
pub const DEFAULT_PORT: u16 = 31337;

/// TCP 建连超时缺省值（与 qmdb-client 同一口径：5s，连不上立刻报错）
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;

#[derive(Debug, Clone)]
pub struct QmdbConnection {
    /// Flight SQL 端点，`host:port`（如 `192.0.2.13:31337`）
    pub addr: String,
    pub user: String,
    pub password: String,
    /// TCP 建连超时（毫秒；缺省 5s）
    pub connect_timeout_ms: u64,
}

impl QmdbConnection {
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

        let addr = get_str(&["addr", "address", "endpoint", "url"]).ok_or_else(|| {
            D2DError::config(
                "qmdb 需要 connection.addr（Flight SQL 端点，host:port，如 192.0.2.13:31337）",
            )
        })?;
        // 容忍 `http://host:port` 写法（用户从别的工具复制来的）；剥掉 scheme
        let addr = addr
            .strip_prefix("grpc+tcp://")
            .or_else(|| addr.strip_prefix("grpc://"))
            .or_else(|| addr.strip_prefix("http://"))
            .unwrap_or(&addr)
            .trim_end_matches('/')
            .to_string();
        // 端口必填：不猜默认端口（猜错了报"连不上"比报"地址缺端口"难查得多）
        let has_port = match addr.rsplit_once(':') {
            Some((_, p)) => !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()),
            None => false,
        };
        if !has_port {
            return Err(D2DError::config(format!(
                "qmdb connection.addr 必须带端口（host:port），收到 `{addr}`；\
                 Flight SQL 缺省端口是 {DEFAULT_PORT}"
            )));
        }

        // 已知不认识的键直接拒绝：静默忽略会让"我配了怎么没生效"查无可查
        let known = [
            "addr",
            "address",
            "endpoint",
            "url",
            "user",
            "username",
            "password",
            "pass",
            "connect_timeout_ms",
            "database",
            "db",
        ];
        if let Some(k) = conn
            .as_object()
            .and_then(|m| m.keys().find(|k| !known.contains(&k.as_str())))
        {
            if k == "token" {
                return Err(D2DError::config(
                    "qmdb 不支持 connection.token：认证就是 CREATE USER 管理的账号+密码，\
                     请改用 connection.user / connection.password",
                ));
            }
            if k == "path" {
                return Err(D2DError::config(
                    "qmdb 没有 embedded/文件模式：它是常驻服务，请用 connection.addr 连接",
                ));
            }
            if k == "host" || k == "hosts" {
                return Err(D2DError::config(
                    "qmdb 是单端点服务：请用 connection.addr（host:port），没有 host/hosts 列表",
                ));
            }
            return Err(D2DError::config(format!(
                "qmdb connection 不认识字段 `{k}`（认识的：{}）",
                known.join(", ")
            )));
        }

        Ok(Self {
            addr,
            user: get_str(&["user", "username"])
                .ok_or_else(|| D2DError::config("qmdb 需要 connection.user"))?,
            password: get_str(&["password", "pass"]).unwrap_or_default(),
            connect_timeout_ms: conn
                .get("connect_timeout_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_MS),
        })
    }

    /// `method` 只接受 flight（或省略）—— qmdb 的数据面只有这一条
    pub fn validate_method(method: Option<&str>) -> D2DResult<()> {
        match method
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            None | Some("flight") | Some("flight_sql") | Some("arrow_flight") => Ok(()),
            Some("jdbc") | Some("mysql") | Some("odbc") => Err(D2DError::config(format!(
                "qmdb 没有 `{}` 这样的数据面：它只说 Arrow Flight SQL（method 可省略或写 flight）",
                method.unwrap().trim()
            ))),
            Some(other) => Err(D2DError::config(format!(
                "未知 qmdb method `{other}`：只支持 flight（Arrow Flight SQL）"
            ))),
        }
    }

    /// 建一条 gRPC 通道（真拨号，失败立刻报错）
    ///
    /// 通道是**可克隆复用**的：HTTP/2 多路复用，多个分片读/多个写入会话共享同一条
    /// TCP 连接即可，不必每个取数单元重新握手。
    pub async fn channel(&self) -> D2DResult<tonic::transport::Channel> {
        let url = format!("http://{}", self.addr);
        tonic::transport::Endpoint::from_shared(url.clone())
            .map_err(|e| D2DError::config(format!("qmdb 地址 {url} 无效: {e}")))?
            .connect_timeout(Duration::from_millis(self.connect_timeout_ms))
            .connect()
            .await
            .map_err(|e| {
                D2DError::connection(
                    "qmdb",
                    format!("无法连接 qmdb {url}（{e}）—— 确认服务在跑、地址端口对"),
                )
            })
    }
}

/// 装载模式（与服务端 begin_load 的 mode 一一对应）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadMode {
    /// 追加（缺省）
    Append,
    /// 覆盖命中的分区（谓词串列表；未命中分区服务端报错）
    OverwritePartitions(Vec<String>),
    /// 整表替换
    OverwriteTable,
}

impl LoadMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::OverwritePartitions(_) => "overwrite_partitions",
            Self::OverwriteTable => "overwrite_table",
        }
    }

    /// begin_load 的 preds 参数（仅 overwrite_partitions 需要）
    pub fn preds(&self) -> &[String] {
        match self {
            Self::OverwritePartitions(p) => p,
            _ => &[],
        }
    }
}

/// commit_load 可登记的水位（断点锚点；`SHOW WATERMARKS ON db.t` 可查）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    pub name: String,
    pub value: String,
}

/// sink 装载选项（`sink.properties.load` 块）
///
/// ```yaml
/// sink:
///   properties:
///     load:
///       mode: append                  # append（缺省）| overwrite_partitions | overwrite_table
///       partitions:                   # mode=overwrite_partitions 时的分区谓词
///         - "pdate = '2026-10-21'"
///       watermark:                    # 可选：commit 时登记水位
///         name: daily
///         value: "2026-10-21"
/// ```
#[derive(Debug, Clone)]
pub struct LoadOptions {
    pub mode: LoadMode,
    pub watermark: Option<Watermark>,
}

impl Default for LoadOptions {
    fn default() -> Self {
        Self {
            mode: LoadMode::Append,
            watermark: None,
        }
    }
}

impl LoadOptions {
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let mut opts = Self::default();
        let Some(load) = props.get("load") else {
            return Ok(opts);
        };
        let obj = load.as_object().ok_or_else(|| {
            D2DError::config("qmdb sink.properties.load 必须是映射（mode/partitions/watermark）")
        })?;

        for k in obj.keys() {
            if !matches!(k.as_str(), "mode" | "partitions" | "watermark") {
                return Err(D2DError::config(format!(
                    "qmdb sink.properties.load 不认识字段 `{k}`（认识的：mode / partitions / watermark）；\
                     攒批阈值请配 flow.sink.batch_bytes，不在这里"
                )));
            }
        }

        let mode = obj
            .get("mode")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_lowercase);
        match mode.as_deref() {
            None => {}
            Some("append") => opts.mode = LoadMode::Append,
            Some("overwrite_table") => opts.mode = LoadMode::OverwriteTable,
            Some("overwrite_partitions") => {
                let empty = Vec::new();
                let parts = obj
                    .get("partitions")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| {
                        D2DError::config(
                            "load.mode=overwrite_partitions 需要 load.partitions（分区谓词列表）",
                        )
                    })?;
                let preds: Vec<String> = parts
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(String::from)
                            .ok_or_else(|| {
                                D2DError::config(
                                    "load.partitions 每一项都要是非空字符串谓词，如 `pdate = '2026-10-21'`",
                                )
                            })
                    })
                    .collect::<D2DResult<_>>()?;
                let preds = if preds.is_empty() { empty } else { preds };
                if preds.is_empty() {
                    return Err(D2DError::config(
                        "load.partitions 不能为空：overwrite_partitions 靠谓词圈定覆盖范围，\
                         圈不出范围就什么都覆盖不了（要整表覆盖请用 mode: overwrite_table）",
                    ));
                }
                opts.mode = LoadMode::OverwritePartitions(preds);
            }
            Some(other) => {
                return Err(D2DError::config(format!(
                "未知 load.mode `{other}`：只支持 append | overwrite_partitions | overwrite_table"
            )))
            }
        }

        if let Some(w) = obj.get("watermark") {
            let wobj = w
                .as_object()
                .ok_or_else(|| D2DError::config("load.watermark 必须是映射（name / value）"))?;
            let name = wobj
                .get("name")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| D2DError::config("load.watermark.name 不能为空"))?;
            let value = wobj
                .get("value")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| D2DError::config("load.watermark.value 不能为空"))?;
            opts.watermark = Some(Watermark {
                name: name.to_string(),
                value: value.to_string(),
            });
        }

        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_connection() {
        let c = QmdbConnection::from_json(&json!({
            "addr": "192.0.2.13:31337", "user": "root", "password": "x"
        }))
        .unwrap();
        assert_eq!(c.addr, "192.0.2.13:31337");
        assert_eq!(c.user, "root");
        assert_eq!(c.connect_timeout_ms, DEFAULT_CONNECT_TIMEOUT_MS);
    }

    #[test]
    fn test_addr_scheme_and_slash_tolerated() {
        let c = QmdbConnection::from_json(&json!({
            "addr": "http://192.0.2.13:31337/", "user": "root"
        }))
        .unwrap();
        assert_eq!(c.addr, "192.0.2.13:31337");
    }

    #[test]
    fn test_addr_requires_port() {
        // 裸 host（哪怕长得像 IP）在解析期就拒绝，不等到拨号才报"连不上"
        let e = QmdbConnection::from_json(&json!({ "addr": "192.0.2.13", "user": "root" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("端口"), "{}", e);
    }

    #[test]
    fn test_addr_without_port_is_config_error() {
        let e = QmdbConnection::from_json(&json!({ "addr": "just-host", "user": "root" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("端口"), "{}", e);
        assert!(e.contains(&DEFAULT_PORT.to_string()), "{}", e);
    }

    #[test]
    fn test_missing_addr_or_user() {
        let e = QmdbConnection::from_json(&json!({ "user": "root" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("addr"), "{}", e);

        let e = QmdbConnection::from_json(&json!({ "addr": "h:31337" }))
            .unwrap_err()
            .to_string();
        assert!(e.contains("user"), "{}", e);
    }

    #[test]
    fn test_unknown_and_removed_keys() {
        let e = QmdbConnection::from_json(&json!({
            "addr": "h:31337", "user": "u", "token": "t"
        }))
        .unwrap_err()
        .to_string();
        assert!(e.contains("token"), "{}", e);
        assert!(e.contains("user"), "{}", e);

        let e = QmdbConnection::from_json(&json!({
            "addr": "h:31337", "user": "u", "path": "/data"
        }))
        .unwrap_err()
        .to_string();
        assert!(e.contains("embedded"), "{}", e);

        let e = QmdbConnection::from_json(&json!({
            "addr": "h:31337", "user": "u", "fetch_size": 100
        }))
        .unwrap_err()
        .to_string();
        assert!(e.contains("fetch_size"), "{}", e);
    }

    #[test]
    fn test_method_validation() {
        assert!(QmdbConnection::validate_method(None).is_ok());
        assert!(QmdbConnection::validate_method(Some("flight")).is_ok());
        assert!(QmdbConnection::validate_method(Some("FLIGHT_SQL")).is_ok());
        let e = QmdbConnection::validate_method(Some("jdbc"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("flight"), "{}", e);
        let e = QmdbConnection::validate_method(Some("odbc"))
            .unwrap_err()
            .to_string();
        assert!(e.contains("flight"), "{}", e);
    }

    #[test]
    fn test_load_options_default_is_append() {
        let props = serde_json::Map::new();
        let o = LoadOptions::from_props(&props).unwrap();
        assert_eq!(o.mode, LoadMode::Append);
        assert!(o.watermark.is_none());
    }

    #[test]
    fn test_load_options_full() {
        let props = serde_json::json!({
            "load": {
                "mode": "overwrite_partitions",
                "partitions": ["pdate = '2026-10-21'", "pdate = '2026-10-22'"],
                "watermark": { "name": "daily", "value": "2026-10-22" }
            }
        });
        let o = LoadOptions::from_props(props.as_object().unwrap()).unwrap();
        assert_eq!(
            o.mode,
            LoadMode::OverwritePartitions(vec![
                "pdate = '2026-10-21'".into(),
                "pdate = '2026-10-22'".into()
            ])
        );
        assert_eq!(
            o.watermark,
            Some(Watermark {
                name: "daily".into(),
                value: "2026-10-22".into()
            })
        );
    }

    #[test]
    fn test_load_options_rejects_bad_shapes() {
        // overwrite_partitions 没给谓词
        let props = serde_json::json!({ "load": { "mode": "overwrite_partitions" } });
        let e = LoadOptions::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("partitions"), "{}", e);

        // 空谓词列表
        let props =
            serde_json::json!({ "load": { "mode": "overwrite_partitions", "partitions": [] } });
        let e = LoadOptions::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("不能为空"), "{}", e);

        // 未知字段
        let props = serde_json::json!({ "load": { "mode": "append", "batch_rows": 100 } });
        let e = LoadOptions::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("batch_rows"), "{}", e);
        assert!(e.contains("flow.sink"), "{}", e);

        // 未知模式
        let props = serde_json::json!({ "load": { "mode": "upsert" } });
        let e = LoadOptions::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("upsert"), "{}", e);

        // 水位缺 value
        let props = serde_json::json!({ "load": { "watermark": { "name": "d" } } });
        let e = LoadOptions::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("value"), "{}", e);
    }
}
