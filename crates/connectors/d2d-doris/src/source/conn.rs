//! 连 FE 的 MySQL 协议口：建连选项、连接池、多 FE 轮询
//!
//! # 取数单元 = 一条连接
//!
//! 内核按 `flow.source.concurrency` **给每个分片发一张券**，一次
//! [`SourceConnector::read`] 对应一个取数单元。这里的一个单元就是**一条 MySQL 连接**：
//! 读时取一条、读完归还。连接器不参与限流（那是内核闸门的事），它只保证
//! "一次 read 只占一条"。
//!
//! [`SourceConnector::read`]: d2d_connector::SourceConnector::read

use crate::config::DorisConnection;
use d2d_core::{D2DError, D2DResult};
use mysql_async::{Conn, Opts, OptsBuilder, Pool};
use std::sync::atomic::{AtomicUsize, Ordering};

/// 客户端读包上限
///
/// Doris 把结果集切成有界大小的包发过来，单行不会接近这个数。取 64 MB：
/// 够任何单行用，又不至于让连接各自留一个巨大的空缓冲。
/// （别拿它当"内存账"里的一项 —— mysql_async 按需增长，不预分配。）
pub const MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;

/// 连接空闲超时（秒），与 Doris FE 的 `wait_timeout` 缺省同量级
const WAIT_TIMEOUT_SECS: usize = 28800;

/// 建一条到某个 FE 的 MySQL 协议连接选项
///
/// # ⚠️ 三个显式参数一个都不能省：Doris 的 FE 不实现 `@@socket`
///
/// 出厂 `Opts::from_url` 出来的选项，在握手之后会发一句
/// `SELECT @@max_allowed_packet, @@wait_timeout, @@socket` 去问服务端设置。
/// Doris 直接回：
///
/// ```text
/// ERROR 1105: errCode = 2, detailMessage = Unsupported system variable: socket
/// ```
///
/// **连接根本建不起来** —— 不是"少个设置"，是连不上。三个都显式给值之后
/// `loads` 就空了，那句探测 SQL 不会发。
///
/// 这三项本来也该由客户端定：`wait_timeout` 影响的是**本进程**的连接复用，
/// 不是服务端的行为。
fn opts_for(host: &str, conn: &DorisConnection) -> Opts {
    let host = host.trim();
    // `fe_hosts` 里允许写 `host:port`（与 HTTP 那边同一约定），带端口的条目原样用
    let (h, port) = match host.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            (h.to_string(), p.parse::<u16>().unwrap_or(conn.query_port))
        }
        _ => (host.to_string(), conn.query_port),
    };

    OptsBuilder::default()
        .ip_or_hostname(h)
        .tcp_port(port)
        .user(Some(conn.username.clone()))
        .pass(Some(conn.password.clone()))
        .db_name(Some(conn.database.clone()))
        .prefer_socket(false)
        .max_allowed_packet(Some(MAX_ALLOWED_PACKET))
        .wait_timeout(Some(WAIT_TIMEOUT_SECS))
        .into()
}

/// 读侧的连接来源：**每个 FE 一个池**，取连接时轮询
///
/// 为什么不是一个池：mysql_async 的 `Opts` 只装一个主机（0.34 没有
/// `TryFrom<Vec<Opts>>`），多 FE 只能靠"多个池 + 自己轮询"。
/// 这与 HTTP 那边的做法一致（`config.rs` 的 `FE_ROUND_ROBIN`），
/// 好处也一样：配了三个 FE 就真的会用三个，而不是把流量全压第一个。
pub struct Reader {
    pools: Vec<Pool>,
    cursor: AtomicUsize,
}

impl Reader {
    pub fn new(conn: &DorisConnection) -> D2DResult<Self> {
        if conn.fe_hosts.is_empty() {
            return Err(D2DError::config("doris fe_hosts 为空，无法建立读连接"));
        }
        let pools = conn
            .fe_hosts
            .iter()
            .map(|h| Pool::new(opts_for(h, conn)))
            .collect();
        Ok(Self {
            pools,
            cursor: AtomicUsize::new(0),
        })
    }

    /// 取一条连接（轮询 FE）
    pub async fn get_conn(&self) -> D2DResult<Conn> {
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % self.pools.len();
        self.pools[idx]
            .get_conn()
            .await
            .map_err(|e| D2DError::external("doris", format!("从 FE 取连接失败: {}", e)))
    }

    /// 关掉所有池
    pub async fn disconnect(self) -> D2DResult<()> {
        for p in self.pools {
            p.disconnect()
                .await
                .map_err(|e| D2DError::external("doris", format!("关闭连接池失败: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(fe: Vec<&str>) -> DorisConnection {
        DorisConnection {
            fe_hosts: fe.into_iter().map(String::from).collect(),
            be_hosts: vec![],
            database: "db".into(),
            table: "t".into(),
            username: "u".into(),
            password: "p".into(),
            http_port: 18030,
            be_http_port: 18040,
            query_port: 19030,
            load_targets: vec![],
        }
    }

    /// `@@socket` 那一课：三个参数缺一个，连都建不起来
    #[test]
    fn test_opts_explicitly_disable_the_socket_probe() {
        let o = opts_for("192.0.2.10", &conn(vec!["192.0.2.10"]));
        assert!(
            !o.prefer_socket(),
            "Doris 不实现 @@socket，探测它会让建连直接失败"
        );
        assert!(o.max_allowed_packet().is_some(), "不给值就会去问服务端");
        assert!(o.wait_timeout().is_some(), "不给值就会去问服务端");
    }

    #[test]
    fn test_opts_use_query_port_and_credentials() {
        let o = opts_for("192.0.2.10", &conn(vec!["192.0.2.10"]));
        assert_eq!(o.ip_or_hostname(), "192.0.2.10");
        assert_eq!(o.tcp_port(), 19030);
        assert_eq!(o.user(), Some("u"));
        assert_eq!(o.pass(), Some("p"));
        assert_eq!(o.db_name(), Some("db"));
    }

    /// `fe_hosts` 条目自带端口时用它（与 HTTP 那边同一约定）
    #[test]
    fn test_opts_respect_inline_port() {
        let o = opts_for("192.0.2.10:9999", &conn(vec!["192.0.2.10:9999"]));
        assert_eq!(o.ip_or_hostname(), "192.0.2.10");
        assert_eq!(o.tcp_port(), 9999);
        let o = opts_for("192.0.2.11", &conn(vec!["192.0.2.11"]));
        assert_eq!(o.tcp_port(), 19030);
    }

    #[test]
    fn test_reader_rejects_empty_fe_hosts() {
        let e = Reader::new(&conn(vec![]))
            .err()
            .expect("空 fe_hosts 必须报错")
            .to_string();
        assert!(e.contains("fe_hosts"), "{}", e);
    }

    /// 配了几个 FE 就有几个池 —— 否则配了三个 FE 全压第一个
    #[test]
    fn test_reader_builds_one_pool_per_fe() {
        let r = Reader::new(&conn(vec!["192.0.2.10:19030", "192.0.2.11:19030"])).unwrap();
        assert_eq!(r.pools.len(), 2);
        // 轮询确实在换池（取模后的下标必须落在合法范围内且会走遍）
        let idx: Vec<usize> = (0..4)
            .map(|_| r.cursor.fetch_add(1, Ordering::Relaxed) % r.pools.len())
            .collect();
        assert_eq!(idx, vec![0, 1, 0, 1]);
    }
}
