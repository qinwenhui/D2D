//! MySQL **writer 方向**
//!
//! 形态：**绑定参数的多行 INSERT**（与达梦的 `odbc` 写入方式是同一种做法）。
//!
//! ```text
//! write_batch(一份数据)：
//!   从池里取一条连接
//!   按"占位符上限"和"字节上限"把这一份切成若干段
//!   每段一条 `INSERT INTO db.t (c1,c2) VALUES (?,?),(?,?),...`
//!   绑参数执行（autocommit，一次 write_batch 可能就是多条语句）
//! ```
//!
//! # 两个上限都不是拍脑袋
//!
//! - **占位符 65535**：MySQL 协议里预处理语句的硬上限，超过直接报错。
//!   每段行数 = `65535 / 列数`（37 列 → 1771 行/语句）。
//! - **字节 ~32 MB**：客户端 `max_allowed_packet` 我们设了 64 MB，服务端一般也是
//!   64 MB；留一半余量。宽行/长文本时先撞到的是这个，不是行数。
//!
//! # 值怎么绑
//!
//! 一律绑**文本**（`Value::Bytes`），让 MySQL 按目标列声明类型转换 —— 与达梦
//! `odbc` 那条路同一个立场：只有一条类型转换路径要维护。两个例外：
//!
//! - `NULL` 绑 `Value::NULL`（不能绑空串：MySQL 里 `''` 和 NULL 是两回事）；
//! - 二进制列绑**原始字节**（文本会有编码问题）。
//!
//! 布尔必须显式转成 `1/0`：MySQL 会把字符串 `'true'` 当数字 0 存下去并只给一条警告。
//!
//! # 为什么不自动重试
//!
//! 一条多行 INSERT 在服务端是原子的，但**"超时"不等于"没写进去"** —— 提交可能已经
//! 完成而响应丢了。自动重试会再插一遍：没有唯一键就是重复行，有唯一键则变成一次
//! 莫名其妙的报错。所以这里不猜，错误原样上抛；要不要重跑由用户决定（也由
//! 目标端有没有唯一键决定安全性）。

pub mod session;

use async_trait::async_trait;
use d2d_connector::{MemMeter, SinkConnector, SinkSession, SinkSpec, WriteConfig};
use d2d_core::{D2DError, D2DResult};
use mysql_async::prelude::*;
use std::sync::Arc;
use tracing::info;

use crate::config::MysqlConnection;
use crate::sql::quote_ident;
use session::MysqlSinkSession;

/// MySQL Sink 连接器
pub struct MysqlSinkConnector {
    name: String,
    pool: Option<mysql_async::Pool>,
    database: Option<String>,
    table: Option<String>,
    write_config: WriteConfig,
    meter: Option<Arc<MemMeter>>,
}

impl MysqlSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "mysql".to_string(),
            pool: None,
            database: None,
            table: None,
            write_config: WriteConfig::default(),
            meter: None,
        }
    }
}

impl Default for MysqlSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SinkConnector for MysqlSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        let connection = MysqlConnection::from_json(&spec.connection)?;
        info!(
            connector = %self.name, database = %spec.database, table = %spec.table,
            host = %connection.host, port = connection.port, "initializing mysql sink"
        );
        let pool = connection.pool(Some(&spec.database));

        // 开工前自检：连得上、目标表在、列对得上（"迁 0 行也报成功"的封堵点）
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| D2DError::external("mysql", format!("连不上 MySQL: {}", e)))?;
        let sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
                   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        let rows: Vec<mysql_async::Row> = conn
            .exec(sql, (&spec.database, &spec.table))
            .await
            .map_err(|e| D2DError::external("mysql", format!("读目标表结构失败: {}", e)))?;
        let target_cols: Vec<String> = rows
            .iter()
            .filter_map(|r| match r.as_ref(0) {
                Some(mysql_async::Value::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
            .collect();
        drop(conn);
        if target_cols.is_empty() {
            return Err(D2DError::external(
                "mysql",
                format!("目标表 `{}`.`{}` 不存在或不可见", spec.database, spec.table),
            ));
        }
        info!(columns = target_cols.len(), "目标表结构已取到");

        self.pool = Some(pool);
        self.database = Some(spec.database.clone());
        self.table = Some(spec.table.clone());
        self.write_config = spec.write.clone().unwrap_or_default();
        self.meter = Some(MemMeter::new(
            "mysql 写入绑定",
            "一次写入的绑定值（每段一条多行 INSERT，执行完即释放）",
        ));
        Ok(())
    }

    async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let pool = self
            .pool
            .clone()
            .ok_or_else(|| D2DError::config("mysql sink not initialized"))?;
        let table = self
            .table
            .clone()
            .ok_or_else(|| D2DError::config("mysql sink not initialized"))?;
        let database = self
            .database
            .clone()
            .ok_or_else(|| D2DError::config("mysql sink not initialized"))?;
        Ok(Box::new(MysqlSinkSession {
            pool,
            qualified_table: format!("{}.{}", quote_ident(&database), quote_ident(&table)),
            meter: self.meter.clone(),
            total_rows: 0,
        }))
    }

    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing mysql sink");
        if let Some(pool) = self.pool.take() {
            pool.disconnect()
                .await
                .map_err(|e| D2DError::external("mysql", format!("关闭连接池失败: {}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(conn: serde_json::Value) -> SinkSpec {
        SinkSpec {
            connector: "mysql".into(),
            connection: conn,
            method: None,
            properties: None,
            database: "db".into(),
            table: "t".into(),
            write: None,
        }
    }

    #[test]
    fn test_name() {
        assert_eq!(MysqlSinkConnector::new().name(), "mysql");
    }

    #[tokio::test]
    async fn test_session_before_init_errors() {
        let c = MysqlSinkConnector::new();
        assert!(c.begin_session(&spec(json!({"host": "h"}))).await.is_err());
    }
}
