//! PostgreSQL **writer 方向**
//!
//! 形态与 MySQL / 达梦 `odbc` 一致：**绑定参数的多行 INSERT**。
//!
//! ```text
//! write_batch(一份数据)：
//!   建一条连接
//!   按占位符上限与字节上限把这一份切段
//!   每段：INSERT INTO "public"."t" (c1,c2) VALUES ($1::text::bigint,$2::text,...), (...)
//! ```
//!
//! # 为什么参数要写 `$n::text::目标类型`
//!
//! PostgreSQL 的参数是**强类型**的：`$1::bigint` 会让协议把参数声明成 `int8`，
//! 而我们想绑文本 —— `tokio-postgres` 会当场拒绝
//! （`WrongType { postgres: Int8, rust: "&str" }`，实测）。
//! 先转 `text` 再显式转目标类型，参数类型就是 TEXT，值的转换由服务端做：
//! 这正是本项目"**一种值转换路径**"的立场。
//!
//! `bytea` 例外：直接绑 `Vec<u8>`（文本会有编码问题）。
//!
//! # 目标列类型从哪来
//!
//! init 时读 `information_schema.columns` —— 写入用的 cast 是**目标表的声明类型**，
//! 不是源端的类型。这一条不能省：目标列是 `numeric(18,4)` 而源端给的是文本，
//! 不带 cast 就是类型不匹配。

pub mod session;

use async_trait::async_trait;
use d2d_connector::{MemMeter, SinkConnector, SinkSession, SinkSpec};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tracing::info;

use crate::config::PgConnection;
use session::{Cast, PgSinkSession, TargetColumn};

pub struct PgSinkConnector {
    name: String,
    connection: Option<PgConnection>,
    database: Option<String>,
    table: Option<String>,
    columns: Vec<TargetColumn>,
    meter: Option<Arc<MemMeter>>,
}

impl PgSinkConnector {
    pub fn new() -> Self {
        Self {
            name: "postgres".to_string(),
            connection: None,
            database: None,
            table: None,
            columns: Vec::new(),
            meter: None,
        }
    }
}

impl Default for PgSinkConnector {
    fn default() -> Self {
        Self::new()
    }
}

/// 目标列类型 → 写入 cast
fn cast_for(data_type: &str, udt_name: &str) -> D2DResult<Cast> {
    Ok(match data_type {
        "smallint" | "integer" | "bigint" | "real" | "double precision" | "boolean"
        | "numeric" | "decimal" | "date" | "time without time zone"
        | "timestamp without time zone" | "timestamp with time zone" | "json" | "jsonb"
        | "uuid" => Cast::Text(data_type.to_string()),
        // text 系：参数本身就是 text，不需要再 cast
        "text" | "character varying" | "character" => Cast::Text("text".to_string()),
        "bytea" => Cast::Bytea,
        "ARRAY" => {
            return Err(D2DError::schema(format!(
                "目标列是数组类型（{}），本连接器不支持写入 —— 请先展平",
                udt_name
            )))
        }
        other => {
            return Err(D2DError::schema(format!(
                "目标列类型 {} 不支持写入（PostgreSQL 枚举等自定义类型请在查询侧转成文本）",
                other
            )))
        }
    })
}

#[async_trait]
impl SinkConnector for PgSinkConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn init(&mut self, spec: &SinkSpec) -> D2DResult<()> {
        let connection = PgConnection::from_json(&spec.connection)?;
        info!(
            connector = %self.name, database = %spec.database, table = %spec.table,
            host = %connection.host, port = connection.port, "initializing postgres sink"
        );
        let (client, conn) = connection
            .config(&spec.database)
            .connect(NoTls)
            .await
            .map_err(|e| D2DError::external("postgres", format!("连不上 PostgreSQL: {}", e)))?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "postgres 目标连接关闭");
            }
        });

        let rows = client
            .query(
                "SELECT column_name, data_type, udt_name
                 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = $1
                 ORDER BY ordinal_position",
                &[&spec.table],
            )
            .await
            .map_err(|e| D2DError::external("postgres", format!("读目标表结构失败: {}", e)))?;
        let mut columns = Vec::with_capacity(rows.len());
        for r in &rows {
            let name: String = r.get(0);
            let data_type: String = r.get(1);
            let udt: String = r.get(2);
            columns.push(TargetColumn { name: name.clone(), cast: cast_for(&data_type, &udt)? });
        }
        drop(client);
        if columns.is_empty() {
            return Err(D2DError::external(
                "postgres",
                format!(
                    "目标表 \"public\".\"{}\" 不存在或不可见",
                    spec.table
                ),
            ));
        }
        info!(columns = columns.len(), "目标表结构已取到");

        self.connection = Some(connection);
        self.database = Some(spec.database.clone());
        self.table = Some(spec.table.clone());
        self.columns = columns;
        self.meter = Some(MemMeter::new(
            "postgres 写入绑定",
            "一次写入的绑定值（每段一条多行 INSERT，执行完即释放）",
        ));
        Ok(())
    }

    async fn begin_session(&self, _spec: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        let connection = self
            .connection
            .clone()
            .ok_or_else(|| D2DError::config("postgres sink not initialized"))?;
        let database = self
            .database
            .clone()
            .ok_or_else(|| D2DError::config("postgres sink not initialized"))?;
        let table = self
            .table
            .clone()
            .ok_or_else(|| D2DError::config("postgres sink not initialized"))?;
        Ok(Box::new(PgSinkSession {
            connection,
            database,
            qualified_table: format!(
                "\"public\".{}",
                d2d_connector::quote_ident(&table)
            ),
            columns: self.columns.clone(),
            meter: self.meter.clone(),
            total_rows: 0,
        }))
    }

    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        self.meter.clone()
    }

    async fn close(&mut self) -> D2DResult<()> {
        info!(connector = %self.name, "closing postgres sink");
        self.connection = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast_for_real_types() {
        assert!(matches!(cast_for("bigint", "int8").unwrap(), Cast::Text(t) if t == "bigint"));
        assert!(matches!(cast_for("numeric", "numeric").unwrap(), Cast::Text(t) if t == "numeric"));
        assert!(matches!(cast_for("bytea", "bytea").unwrap(), Cast::Bytea));
        assert!(matches!(cast_for("character varying", "varchar").unwrap(), Cast::Text(t) if t == "text"));
        assert!(cast_for("ARRAY", "_int4").is_err());
        assert!(cast_for("USER-DEFINED", "my_enum").is_err());
    }

    #[test]
    fn test_name() {
        assert_eq!(PgSinkConnector::new().name(), "postgres");
    }
}
