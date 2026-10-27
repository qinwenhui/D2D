//! 达梦 DPI 读取方法（`method: dpi`）
//!
//! # 为什么有它
//!
//! - **NULL 语义正确**：DPI 的指示器天然区分 SQL NULL 与空串；dmfldr 的 SQL 导出
//!   把 NULL 写成空字段， Doris 会存成空字符串（见 `physical-direct-null-unsafe`）。
//! - **没有导出文件/子进程**：一次 `read` = 一条连接 + 一条游标。
//! - **通用查询**：跑的就是一条 SELECT，WHERE/分片谓词与其它方法同源。
//!
//! # 与其它方法的关系
//!
//! 完全独立：不读写 `FlDrParams`、不碰 dmfldr 的导出目录、不改 ODBC 的任何行为。
//! 共享的只有两件事 —— `type_contract` 推导的 schema（[`DamengReadContext`]）和
//! 内核的分片切法（[`d2d_connector::partition::plan`]）。
//!
//! # 线程模型
//!
//! 所有 DPI 调用（连接、执行、取数、释放）都住在**一个** `spawn_blocking` 任务里；
//! 异步侧只通过有界通道收 `PhysicalBlock` / `RecordBatch`。句柄不跨线程、不跨 await。
//!
//! # 两条产出路径
//!
//! | 路径 | 产出 | NULL | 用途 |
//! |---|---|---|---|
//! | 直通面 | `|` 分隔的 UTF-8 文本块 | `\N` | 无转换算子的纯搬运（快） |
//! | 逻辑面 | `RecordBatch`（RowBuilder 按契约类型解析） | `None` | 有转换 / 非 UTF-8 / LOB 之外的任意数据 |

pub mod format;
pub mod params;
pub mod session;
pub mod sql;

use crate::config::ConnectionConfig;
use crate::source::method::dmfldr::params::CharacterCode;
use crate::source::method::{DamengMethodKind, DamengReadContext, DamengReadMethod};
use d2d_connector::{Charset, CsvDialect, PhysicalFormat, PhysicalStream, SourceSpec, SourceSplit};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use d2d_types::text_batch::NullRule;
use d2d_types::{LobSource, RowBuildOptions, RowBuilder};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::dpi::ffi::DpiLibrary;
use format::RowBlockWriter;
use params::DpiParams;
use session::{DpiSession, SessionConfig};

/// DPI 读取方法
///
/// `Clone` 只为启动自检：`init` 里要把同一个方法（的共享句柄）递进
/// `spawn_blocking`。
#[derive(Clone)]
pub struct DpiMethod {
    params: DpiParams,
    conn: ConnectionConfig,
    library: Arc<DpiLibrary>,
    /// 各分片读出的行数之和（收尾对账用）
    exported_rows: Arc<AtomicU64>,
}

impl DpiMethod {
    pub fn new(
        params: DpiParams,
        conn: ConnectionConfig,
        library: Arc<DpiLibrary>,
    ) -> D2DResult<Self> {
        validate_connection(&conn)?;
        Ok(Self {
            params,
            conn,
            library,
            exported_rows: Arc::new(AtomicU64::new(0)),
        })
    }

    fn session_config(&self) -> SessionConfig {
        let server = match self.conn.svc.as_deref().map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => format!("{}:{}", self.conn.host.trim(), self.conn.port),
        };
        SessionConfig {
            server,
            user: self.conn.user.clone(),
            password: self.conn.password.clone(),
            fetch_rows: self.params.fetch_rows.max(1),
            column_bytes: self.params.column_bytes,
        }
    }

    /// 启动期自检：连库 + 跑一次 `... WHERE (user) AND 1 = 0` + 描述列
    ///
    /// 阻塞调用，调用方负责 `spawn_blocking`。
    pub fn self_check(&self, spec: &SourceSpec, ctx: &DamengReadContext) -> D2DResult<()> {
        let sql = sql::probe_sql(spec, ctx);
        let mut s = DpiSession::connect(Arc::clone(&self.library), &self.session_config())?;
        s.execute(&sql, &ctx.column_names, self.params.column_bytes)?;
        info!(
            columns = ctx.column_names.len(),
            fetch_rows = self.params.fetch_rows,
            buffer_bytes = s.buffer_bytes(),
            "dpi: 启动自检通过（连接 / SQL / 列序 / 类型可绑定）"
        );
        Ok(())
    }

    fn charset(&self) -> Option<Charset> {
        charset_of(&self.params.character_code)
    }
}

/// 会话语义上的字符集 → 直通声明；不在表内的编码只能走逻辑面
fn charset_of(code: &CharacterCode) -> Option<Charset> {
    match code {
        CharacterCode::Utf8 => Some(Charset::Utf8),
        CharacterCode::Gbk => Some(Charset::Gbk),
        _ => None,
    }
}

/// 直通载荷声明（不碰动态库，便于单测）
///
/// 方言由 `DpiParams` 声明（缺省 Doris 口径 `|` + `\N`；qmdb 直通配 `","` + `""`），
/// 且**声明必须与 `pump_physical` 的实际产出一致** —— 两边同源自 `params`。
fn physical_format_of(params: &DpiParams, columns: &[String]) -> Option<PhysicalFormat> {
    let charset = charset_of(&params.character_code)?;
    let null = params.null_literal_bytes();
    Some(PhysicalFormat::csv(
        CsvDialect {
            column_separator: params.separator_byte(),
            line_delimiter: format::LINE_DELIMITER,
            quote: None,
            escape: None,
            // 声明与产出同源：`\N` 档同时承诺「值里不再有别的反斜杠」（逐字段检查）；
            // 裸空字段档（qmdb）声明 None —— 目标按「空字段 = NULL」解释
            null_literal: if null.is_empty() {
                None
            } else {
                Some(String::from_utf8_lossy(null).into_owned())
            },
        },
        charset,
        columns.to_vec(),
    ))
}

/// 连接配置校验（不碰动态库，便于单测）
fn validate_connection(conn: &ConnectionConfig) -> D2DResult<()> {
    if conn
        .auth_as
        .as_deref()
        .map(|a| !a.trim().is_empty())
        .unwrap_or(false)
    {
        return Err(D2DError::config(
            "dpi 方法暂不支持 connection.auth_as（OS 认证方式）—— \
             请用普通用户名/密码连接，或改用 method: odbc",
        ));
    }
    if conn.host.trim().is_empty()
        && conn
            .svc
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    {
        return Err(D2DError::config(
            "dpi 连接需要 connection.host（或 dm_svc.conf 服务名 connection.svc）",
        ));
    }
    Ok(())
}

/// 直通面：取一批 → 拼文本块 → 交出去（阻塞，跑在 `spawn_blocking` 里）
#[allow(clippy::too_many_arguments)]
fn pump_physical(
    lib: &Arc<DpiLibrary>,
    cfg: &SessionConfig,
    sql: &str,
    names: &[String],
    params: &DpiParams,
    exported: &AtomicU64,
    tx: &mpsc::Sender<D2DResult<d2d_connector::PhysicalBlock>>,
    split_id: &str,
) -> D2DResult<()> {
    let mut s = DpiSession::connect(Arc::clone(lib), cfg)?;
    s.execute(sql, names, params.column_bytes)?;

    let mut w = RowBlockWriter::with_dialect(
        names.len(),
        params.block_bytes(),
        params.first_block_bytes(),
        params.separator_byte(),
        params.null_literal_bytes(),
    );
    let mut row_no = 0u64;
    loop {
        let n = s.fetch()?;
        if n == 0 {
            break;
        }
        for r in 0..n {
            row_no += 1;
            w.begin_row(row_no);
            for (c, name) in names.iter().enumerate() {
                w.push_cell(s.cell(c, r)?, name)?;
            }
            w.end_row()?;
            if w.is_ready() {
                if let Some(block) = w.take_block() {
                    if tx.blocking_send(Ok(block)).is_err() {
                        // 下游已断开（中止/失败），正常收工
                        return Ok(());
                    }
                }
            }
        }
    }
    if let Some(block) = w.finish() {
        let _ = tx.blocking_send(Ok(block));
    }
    exported.fetch_add(row_no, Ordering::Relaxed);
    info!(split = %split_id, rows = row_no, "dpi: 分片取数完成（直通面）");
    Ok(())
}

/// 逻辑面：取一批 → RowBuilder → RecordBatch（阻塞，跑在 `spawn_blocking` 里）
#[allow(clippy::too_many_arguments)]
fn pump_logical(
    lib: &Arc<DpiLibrary>,
    cfg: &SessionConfig,
    sql: &str,
    ctx: &DamengReadContext,
    params: &DpiParams,
    exported: &AtomicU64,
    tx: &mpsc::Sender<D2DResult<arrow::array::RecordBatch>>,
    split_id: &str,
) -> D2DResult<()> {
    let mut s = DpiSession::connect(Arc::clone(lib), cfg)?;
    s.execute(sql, &ctx.column_names, params.column_bytes)?;

    // 达梦口径的 NULL 规则在这里**不适用**：NULL 由指示器给出（`None`），
    // 空串就是空串。用 ExplicitOnly 才能保住这个区别。
    let opts = RowBuildOptions {
        encoding: params.character_code.into(),
        null: NullRule::ExplicitOnly,
        lob: LobSource::Unsupported,
    };
    let ncols = ctx.column_names.len();
    let mut row_no = 0u64;

    loop {
        let n = s.fetch()?;
        if n == 0 {
            break;
        }
        let mut rb = RowBuilder::new(
            ctx.schema.clone(),
            ctx.logical_types.clone(),
            opts.clone(),
            n,
        )?;
        // 借用 `s` 的缓冲；放在本批作用域内，下一批 fetch 前就释放
        let mut cells: Vec<Option<&[u8]>> = Vec::with_capacity(ncols);
        for r in 0..n {
            cells.clear();
            for c in 0..ncols {
                cells.push(s.cell(c, r)?);
            }
            rb.append_row(&cells, row_no)?;
            row_no += 1;
        }
        let batch = rb.finish()?;
        if batch.num_rows() > 0 && tx.blocking_send(Ok(batch)).is_err() {
            return Ok(());
        }
    }
    exported.fetch_add(row_no, Ordering::Relaxed);
    info!(split = %split_id, rows = row_no, "dpi: 分片取数完成（逻辑面）");
    Ok(())
}

#[async_trait::async_trait]
impl DamengReadMethod for DpiMethod {
    fn kind(&self) -> DamengMethodKind {
        DamengMethodKind::Dpi
    }

    fn exported_rows(&self) -> Option<u64> {
        match self.exported_rows.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    async fn split(
        &self,
        spec: &SourceSpec,
        ctx: &DamengReadContext,
    ) -> D2DResult<Vec<SourceSplit>> {
        let splits = sql::plan_splits(spec, ctx, self.params.fetch_rows)?;
        info!(
            table = %spec.table,
            splits = splits.len(),
            "dpi: 分片规划完成（一片 = 一条 SELECT + 一条连接）"
        );
        Ok(splits)
    }

    async fn read(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<RecordBatchStream> {
        let sql = sql::split_sql(split)?.to_string();
        let cfg = self.session_config();
        let lib = Arc::clone(&self.library);
        let ctx = ctx.clone();
        let params = self.params.clone();
        let exported = Arc::clone(&self.exported_rows);
        let split_id = split.split_id.clone();

        // 有界通道：满则阻塞取数 = 背压；内存上界 ≈ 4 个批次
        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            if let Err(e) = pump_logical(&lib, &cfg, &sql, &ctx, &params, &exported, &tx, &split_id)
            {
                let _ = tx.blocking_send(Err(e));
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn physical_format(&self, ctx: &DamengReadContext) -> Option<PhysicalFormat> {
        physical_format_of(&self.params, &ctx.column_names)
    }

    async fn read_physical(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<PhysicalStream> {
        if self.charset().is_none() {
            // 直通面只声明 utf8/gbk，其余编码只能走逻辑面
            return Err(D2DError::not_supported(format!(
                "dpi 直通面只支持 utf8 / gbk 声明，当前为 {:?}；请改用逻辑面",
                self.params.character_code
            )));
        }
        let sql = sql::split_sql(split)?.to_string();
        let cfg = self.session_config();
        let lib = Arc::clone(&self.library);
        let names = ctx.column_names.clone();
        let params = self.params.clone();
        let exported = Arc::clone(&self.exported_rows);
        let split_id = split.split_id.clone();

        let (tx, rx) = mpsc::channel(4);
        tokio::task::spawn_blocking(move || {
            if let Err(e) =
                pump_physical(&lib, &cfg, &sql, &names, &params, &exported, &tx, &split_id)
            {
                let _ = tx.blocking_send(Err(e));
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2d_types::{LogicalArrowBridge, LogicalType};

    fn ctx() -> DamengReadContext {
        let lts = vec![LogicalType::Int64, LogicalType::Utf8];
        let names = ["id".to_string(), "name".to_string()];
        let fields: Vec<arrow::datatypes::Field> = names
            .iter()
            .zip(lts.iter())
            .map(|(n, lt)| {
                arrow::datatypes::Field::new(n, LogicalArrowBridge::to_arrow(lt).unwrap(), true)
            })
            .collect();
        DamengReadContext {
            schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            logical_types: lts,
            column_names: names.to_vec(),
        }
    }

    /// `physical_format` 的声明必须与直通产出一致：`|`、`\n`、NULL=`\N`
    #[test]
    fn test_physical_format_declares_pipe_and_null_literal() {
        let fmt = physical_format_of(&DpiParams::default(), &ctx().column_names).unwrap();
        let PhysicalFormat::Csv {
            dialect,
            charset,
            columns,
        } = fmt;
        assert_eq!(dialect.column_separator, b'|');
        assert_eq!(dialect.line_delimiter, b'\n');
        assert_eq!(dialect.null_literal.as_deref(), Some("\\N"));
        assert_eq!(charset, Charset::Utf8);
        assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_auth_as_is_rejected() {
        let conn = ConnectionConfig::from_json(
            &serde_json::json!({"host": "h", "user": "u", "password": "p", "auth_as": "SYSDBA"}),
        )
        .unwrap();
        let err = validate_connection(&conn).unwrap_err().to_string();
        assert!(err.contains("auth_as"), "{}", err);
    }

    #[test]
    fn test_unknown_charset_disables_physical() {
        let params = DpiParams {
            character_code: CharacterCode::Gb18030,
            ..Default::default()
        };
        assert!(physical_format_of(&params, &ctx().column_names).is_none());
    }

    #[test]
    fn test_host_or_svc_required() {
        let conn = ConnectionConfig::from_json(&serde_json::json!({"user": "u", "password": "p"}))
            .unwrap();
        assert!(validate_connection(&conn).is_err());
    }
}
