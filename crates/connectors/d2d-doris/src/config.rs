use d2d_core::{D2DError, D2DResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, warn};

/// Doris Stream Load 配置 —— **只有 Doris 私有的东西**
///
/// ⚠️ 攒批阈值（一次发多少行 / 多少字节）**不在这里**。
/// 那是"一次发多少"的通用问题，由 `flow.sink.batch_bytes` 表达，
/// 而且**由内核落实**：内核从池里攒够一份才交给连接器，连接器收到即写。
/// 曾经它长在这张结构里叫 `batch_threshold`，于是这些阈值就带上了 Doris 的颜色，
/// 换个目标端（MySQL）用户得重新学一遍同样的事。
#[derive(Debug, Clone)]
pub struct StreamLoadConfig {
    /// 格式：csv / json / parquet
    ///
    /// 目前已实现 **csv** 与 **json**；parquet 未实现（会明确报错，不静默降级）。
    /// 与另外两项的关系见下面各自的注释 —— 它们**只管 csv**。
    pub format: String,
    /// 列分隔符（**仅 `csv`**）
    ///
    /// JSON 有自己的语法，分隔符不是"一个字符串"能表达的东西，配了也不生效。
    pub column_separator: String,
    /// 行分隔符（**仅 `csv`**）
    ///
    /// JSON 恒用 `\n`：Doris 的行分隔符得靠请求头传，而 HTTP 头值不允许含控制字符
    /// （见 `session.rs::do_stream_load` 第 2 条），所以那边只能是缺省的 `\n`。
    pub line_delimiter: String,
    /// 最大错误过滤比例
    pub max_filter_ratio: f64,
    /// 严格模式
    pub strict_mode: bool,
    /// **单次请求体的字节上限**（连接器内部据此把一批切成几次 Stream Load）
    ///
    /// 这是**目标端的物理约束**，与 `flow.sink.batch_bytes` 不是一回事：
    /// 那个是"内核一次递给连接器多少"（上界，进内存账），这个是"一个 HTTP 请求
    /// 最多能带多少字节"。内核给的上界之内，连接器想拆几个请求是它的事
    /// （见设计说明 §3.2）。
    ///
    /// 为什么非有不可：Doris 的 BE 对 **JSON 请求体**有独立的、小得多的上限
    /// `streaming_load_json_max_mb`（缺省 **100 MB**），而 CSV 那条是
    /// `streaming_load_max_mb`（缺省 10 GB）。同一个 `batch_bytes: 100MB`，
    /// CSV 一路顺畅、JSON 直接整批被拒（实测：
    /// `json body size 142058920 exceed BE's conf streaming_load_json_max_mb 104857600`）
    /// —— 因为 JSON 每行都重复一遍键名，同样数据体积约是 CSV 的 1.4 倍。
    ///
    /// 缺省见 [`StreamLoadConfig::default`]：csv 不设（内核的上界已经够用），
    /// json 取 Doris BE 的缺省 100 MB。BE 上调过这个参数的话，这里跟着调。
    pub max_request_bytes: Option<usize>,
}

/// JSON 的缺省单请求上限 —— Doris BE `streaming_load_json_max_mb` 的出厂值
pub const JSON_DEFAULT_MAX_REQUEST_BYTES: usize = 100 * 1024 * 1024;

impl Default for StreamLoadConfig {
    fn default() -> Self {
        Self {
            format: "csv".to_string(),
            column_separator: "\t".to_string(),
            line_delimiter: "\n".to_string(),
            max_filter_ratio: 0.0,
            strict_mode: true,
            // csv 不设：内核的 `flow.sink.batch_bytes` 已经是上界，
            // 而 Doris 对 CSV 请求体的上限（10 GB）远高于任何合理的攒批量
            max_request_bytes: None,
        }
    }
}

impl StreamLoadConfig {
    /// 按格式补上缺省的单请求上限
    ///
    /// `json` 必须先补：Doris BE 对它的上限只有 100 MB，不补的话
    /// "同一份 `batch_bytes` 配 csv 能跑、配 json 就整批被拒"。
    /// 用户显式给了就听用户的（BE 上调过就不该再按出厂值切）。
    pub fn with_format_defaults(mut self) -> Self {
        if self.max_request_bytes.is_none() && self.format == "json" {
            self.max_request_bytes = Some(JSON_DEFAULT_MAX_REQUEST_BYTES);
        }
        self
    }
}

/// Doris HTTP 连接配置
///
/// **两个口**：`http_port` 是 Stream Load 用的（写方向），`query_port` 是 FE 的
/// MySQL 协议口（读方向）。它们不是一个端口 —— 常见部署为 18030 / 19030。
#[derive(Debug, Clone)]
pub struct DorisConnection {
    pub fe_hosts: Vec<String>,
    /// Stream Load 直连的 BE 地址（可选，但**强烈建议配置**）
    ///
    /// 实测：经 FE 转发 Stream Load 会丢失 `Authorization` 头，Doris 返回
    /// `[NOT_AUTHORIZED]no valid Basic authorization`；直连任一 BE 则正常。
    /// 条目可自带端口（`host:18040`），否则用 `be_http_port`。
    pub be_hosts: Vec<String>,
    pub database: String,
    pub table: String,
    pub username: String,
    pub password: String,
    pub http_port: u16,
    /// BE 的 HTTP 端口（Stream Load 端口），默认 8040；常见部署为 18040
    pub be_http_port: u16,
    /// **FE 的 MySQL 协议口**（读方向），Doris 默认 9030；常见部署为 19030
    ///
    /// 读走它而不是 HTTP：Doris 的 REST 查询口在 2.x 已不保证存在，且拉不了大结果集、
    /// 没有游标。MySQL 协议是业界读 Doris 的标准做法。
    pub query_port: u16,
    /// 初始化时解析出的实际 Stream Load 目标（host:port）。
    /// 由 `resolve_load_targets` 填充：显式 be_hosts 优先，否则向 FE 自动发现。
    pub load_targets: Vec<String>,
}

/// FE/BE 选择游标（进程级轮询）
static FE_ROUND_ROBIN: AtomicUsize = AtomicUsize::new(0);

/// 向 FE 查询存活 BE 列表（`host:http_port`）
///
/// 依赖 FE 的 `/api/show_proc?path=/backends` 接口。返回行的列序（Doris 4.x）：
/// `[id, Host, HeartbeatPort, BePort, HttpPort, BrpcPort, ..., Alive, ...]`
/// 即下标 1=Host、4=HttpPort、9=Alive。字段可能是字符串也可能是数字，两者都兼容。
async fn discover_be_hosts(fe_base: &str, user: &str, pass: &str) -> D2DResult<Vec<String>> {
    let url = format!("{}/api/show_proc?path=/backends", fe_base);
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .basic_auth(user, Some(pass))
        .send()
        .await
        .map_err(|e| D2DError::external("doris", format!("BE 发现请求失败: {}", e)))?;
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| D2DError::external("doris", format!("BE 发现返回非 JSON: {}", e)))?;
    Ok(parse_backends(&body))
}

/// 从 `/api/show_proc?path=/backends` 响应里解析存活 BE 的 `host:http_port`
///
/// 拆成纯函数以便单测（真实响应字段既有字符串也有数字，两者都要兼容）。
fn parse_backends(body: &serde_json::Value) -> Vec<String> {
    let Some(rows) = body.get("data").and_then(|d| d.as_array()) else {
        return Vec::new();
    };
    let cell = |v: Option<&serde_json::Value>| -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(other) => other.to_string().trim_matches('"').to_string(),
            None => String::new(),
        }
    };
    let mut hosts = Vec::new();
    for r in rows {
        let Some(cols) = r.as_array() else { continue };
        let host = cell(cols.get(1));
        let http_port = cell(cols.get(4));
        let alive = cell(cols.get(9));
        if host.is_empty() || http_port.is_empty() {
            continue;
        }
        if !alive.eq_ignore_ascii_case("true") {
            continue; // 跳过已下线/未心跳的 BE
        }
        hosts.push(format!("{}:{}", host, http_port));
    }
    hosts
}

impl DorisConnection {
    /// 从 `source.connection` / `sink.connection` 那段 JSON 解析
    ///
    /// 读方向与写方向**共用这一份**：两个口在同一个 `connection` 块里，
    /// 各解析一遍必然会在"少填一个字段时报什么错"这种地方分岔。
    ///
    /// `database` / `table` 由调用方给 —— 它们在 `source`/`sink` 的顶层，
    /// 不在 `connection` 块里。
    pub fn from_json(
        conn: &serde_json::Value,
        database: &str,
        table: &str,
    ) -> D2DResult<DorisConnection> {
        let fe_hosts = conn
            .get("fe_hosts")
            .and_then(|v| v.as_array())
            .ok_or_else(|| D2DError::config("doris fe_hosts is required"))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        if fe_hosts.is_empty() {
            return Err(D2DError::config("doris fe_hosts cannot be empty"));
        }

        let str_of = |k: &str| {
            conn.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let port_of =
            |k: &str, dflt: u16| conn.get(k).and_then(|v| v.as_u64()).unwrap_or(dflt as u64) as u16;
        let list_of = |k: &str| {
            conn.get(k)
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        Ok(DorisConnection {
            fe_hosts,
            be_hosts: list_of("be_hosts"),
            database: database.to_string(),
            table: table.to_string(),
            username: str_of("username"),
            password: str_of("password"),
            http_port: port_of("http_port", 8030),
            be_http_port: port_of("be_http_port", 8040),
            query_port: port_of("query_port", 9030),
            // 由 init 里的 resolve_load_targets 填充（只有写方向需要）
            load_targets: Vec::new(),
        })
    }

    /// 轮询选择 FE 节点
    ///
    /// 修正两处 V1 问题：
    /// - 原先恒取 `fe_hosts[0]`，配多个 FE 时流量全压第一个，轮询形同虚设；
    /// - 原先无条件拼 `:{http_port}`，而 `fe_hosts` 里本来就允许写 `host:port`
    ///   （见 examples 配置），会拼出 `http://h:8030:8030`。现在带端口的条目原样用。
    pub fn next_fe_url(&self) -> String {
        Self::pick(&self.fe_hosts, self.http_port)
    }

    /// Stream Load 的目标地址：**优先直连 BE**（见 `be_hosts` 字段注释），
    /// 未解析出 BE 时才退回 FE。
    pub fn next_load_url(&self) -> String {
        if !self.load_targets.is_empty() {
            return Self::pick(&self.load_targets, self.be_http_port);
        }
        self.next_fe_url()
    }

    /// 解析 Stream Load 实际目标列表（在 sink init 时调用一次）
    ///
    /// 优先级：① 显式 `be_hosts` ② 向 FE 自动发现 ③ 回退 FE 转发。
    ///
    /// 为什么要直连 BE 而不是让 FE 转发：
    /// - 实测经 FE 转发 Stream Load **会丢失 `Authorization` 头**，Doris 回
    ///   `[NOT_AUTHORIZED]no valid Basic authorization`，直连 BE 正常；
    /// - 直连 BE 也是 Doris 官方推荐做法：写入压力按 BE 轮询分散，不经 FE 单点。
    ///
    /// 为什么做自动发现：生产环境 BE 扩缩容/迁移很常见，写死列表必然过期；
    /// `/api/show_proc?path=/backends` 同时给出存活状态，可跳过已下线的 BE。
    pub async fn resolve_load_targets(&mut self) -> D2DResult<()> {
        if !self.be_hosts.is_empty() {
            self.load_targets = self.be_hosts.clone();
            info!(targets = ?self.load_targets, "doris: 使用显式配置的 BE 直连列表");
            return Ok(());
        }
        for fe in &self.fe_hosts.clone() {
            let base = if fe.contains(':') {
                format!("http://{}", fe.trim())
            } else {
                format!("http://{}:{}", fe.trim(), self.http_port)
            };
            match discover_be_hosts(&base, &self.username, &self.password).await {
                Ok(hosts) if !hosts.is_empty() => {
                    info!(fe = %base, be_count = hosts.len(), targets = ?hosts,
                          "doris: 已从 FE 自动发现存活 BE 列表");
                    self.load_targets = hosts;
                    return Ok(());
                }
                Ok(_) => warn!(fe = %base, "doris: 自动发现未返回存活 BE"),
                Err(e) => warn!(fe = %base, error = %e, "doris: 自动发现失败"),
            }
        }
        warn!(
            "doris: 未能发现 BE，Stream Load 回退到 FE 转发 —— 注意部分集群转发时会丢失 \
             Authorization 头（Doris 报 [NOT_AUTHORIZED]），此时请显式配置 be_hosts"
        );
        Ok(())
    }

    /// 轮询取一个地址，条目自带端口则原样使用
    fn pick(hosts: &[String], default_port: u16) -> String {
        if hosts.is_empty() {
            return String::new();
        }
        let idx = FE_ROUND_ROBIN.fetch_add(1, Ordering::Relaxed) % hosts.len();
        let host = hosts[idx].trim();
        if host.contains(':') {
            format!("http://{}", host)
        } else {
            format!("http://{}:{}", host, default_port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conn(fe: Vec<&str>, be: Vec<&str>) -> DorisConnection {
        DorisConnection {
            fe_hosts: fe.into_iter().map(String::from).collect(),
            be_hosts: be.into_iter().map(String::from).collect(),
            database: "db".into(),
            table: "t".into(),
            username: "u".into(),
            password: "p".into(),
            http_port: 18030,
            be_http_port: 18040,
            query_port: 19030,
            load_targets: Vec::new(),
        }
    }

    /// 真实响应形状（取自 Doris 4.0.8 实测输出，字段是字符串）
    #[test]
    fn test_parse_backends_real_shape() {
        let body = json!({
            "msg": "success",
            "code": 0,
            "data": [
                ["1791787536812","192.0.2.10","19050","19060","18040","18060","8050",
                 "2026-10-12 15:33:15","2026-10-14 19:05:47","true","false"],
                ["1791790397393","192.0.2.11","19050","19060","18040","18060","8050",
                 "2026-10-12 16:31:21","2026-10-14 19:05:47","true","false"],
                // 已下线：Alive=false 必须被跳过
                ["1791790999999","192.0.2.12","19050","19060","18040","18060","8050",
                 "2026-10-12 16:31:21","2026-10-14 19:05:47","false","false"]
            ]
        });
        assert_eq!(
            parse_backends(&body),
            vec![
                "192.0.2.10:18040".to_string(),
                "192.0.2.11:18040".to_string()
            ]
        );
    }

    /// 数字型字段也要能解析；缺列/畸形行直接跳过而不是 panic
    #[test]
    fn test_parse_backends_tolerates_numbers_and_junk() {
        let body = json!({
            "data": [
                [1, "h1", 19050, 19060, 18040, 18060, 8050, "", "", "true", "false"],
                ["row-with-too-few-cols"],
                ["x", "", "19050", "19060", "18040", "", "", "", "", "true", "false"],
            ]
        });
        assert_eq!(parse_backends(&body), vec!["h1:18040".to_string()]);
        // 非预期结构 → 空列表，不 panic
        assert!(parse_backends(&json!({"code": 0})).is_empty());
        assert!(parse_backends(&json!(null)).is_empty());
    }

    // ───────── 连接配置解析（读写共用那一份）─────────

    #[test]
    fn test_from_json_ports_default_and_override() {
        // 只给 fe_hosts：三个端口各取自己的缺省（8030 / 8040 / 9030），
        // 不是"共用一个"—— 它们本来就不是一个口
        let c = DorisConnection::from_json(
            &json!({"fe_hosts": ["h1"], "username": "u", "password": "p"}),
            "db",
            "t",
        )
        .unwrap();
        assert_eq!(
            (c.http_port, c.be_http_port, c.query_port),
            (8030, 8040, 9030)
        );
        assert_eq!(c.database, "db");
        assert_eq!(c.table, "t");

        let c = DorisConnection::from_json(
            &json!({"fe_hosts": ["h1"], "http_port": 18030, "be_http_port": 18040, "query_port": 19030}),
            "db",
            "t",
        )
        .unwrap();
        assert_eq!(
            (c.http_port, c.be_http_port, c.query_port),
            (18030, 18040, 19030)
        );
    }

    #[test]
    fn test_from_json_rejects_missing_or_empty_fe_hosts() {
        assert!(DorisConnection::from_json(&json!({}), "db", "t").is_err());
        let e = DorisConnection::from_json(&json!({"fe_hosts": []}), "db", "t")
            .unwrap_err()
            .to_string();
        assert!(e.contains("empty"), "{}", e);
    }

    #[test]
    fn test_load_url_prefers_be() {
        let mut c = conn(vec!["fe1"], vec!["be1:18040", "be2:18040"]);
        c.load_targets = c.be_hosts.clone();
        let urls: Vec<String> = (0..10).map(|_| c.next_load_url()).collect();
        assert!(urls.iter().all(|u| u.contains("be")), "{:?}", urls);
        // 轮询游标是进程级的（`FE_ROUND_ROBIN`），并行测试会插进来推进它；
        // 不看相邻两次，只看一轮调用里两个 BE 都出现过。
        assert!(urls.iter().any(|u| u.contains("be1")), "{:?}", urls);
        assert!(urls.iter().any(|u| u.contains("be2")), "{:?}", urls);
    }

    /// 未解析出 BE 时必须回退 FE（不能返回空串导致 URL 畸形）
    #[test]
    fn test_load_url_falls_back_to_fe() {
        let c = conn(vec!["fe1"], vec![]);
        assert_eq!(c.next_load_url(), "http://fe1:18030");
    }

    /// 条目自带端口时不得再拼一次端口（曾产生 http://h:8030:8030）
    #[test]
    fn test_pick_respects_inline_port() {
        assert_eq!(
            DorisConnection::pick(&["h:9999".to_string()], 18030),
            "http://h:9999"
        );
        assert_eq!(
            DorisConnection::pick(&["h".to_string()], 18030),
            "http://h:18030"
        );
        assert_eq!(DorisConnection::pick(&[], 18030), "");
    }
}
