//! ODBC 读取方法：进程内游标
//!
//! 达梦 Source 的**覆盖路径**（`method: odbc`），不是快路径：
//!
//! | | dmfldr | odbc（本模块） |
//! |---|---|---|
//! | 取数方式 | 服务端快载工具整表导出到文件，引擎按字节分块并行解析 | 逐游标拉取，一行一行过来 |
//! | 查询能力 | TABLE 模式整表；SQLFILE 模式可带 WHERE，**不支持**计算列/ORDER BY/LIMIT | **全部下推**：WHERE / ORDER BY / LIMIT / 计算列 |
//! | 并发来源 | N 个导出子进程（CPU 密集） | N 条连接（**数据库侧**密集） |
//! | 内存 | 导出物落盘，进程内只有解析缓冲 | 游标批次直接进进程 |
//!
//! 所以它的价值在"dmfldr 做不了的事它能做"，速度上**预期慢于 dmfldr** ——
//! 实测数字见 基准实测，不美化。
//!
//! # 为什么整块挪进 `spawn_blocking`
//!
//! `odbc-api` 是**阻塞 API**。直接在 async 任务里调会占住 tokio worker ——
//! 本项目在逻辑面与 dmfldr 物理面各踩过一次同类问题（见
//! `dmfldr/physical.rs` 的模块注释）。这里照抄那套：**阻塞 I/O 全在
//! `spawn_blocking` 里，用有界 async 通道回传**，通道满则 `blocking_send` 阻塞 →
//! 读停下 → 进程内常驻有上界。
//!
//! # 一连接一分片
//!
//! 每个 `read()` 调用自己开一条连接、读完就关。这与拓扑是**对齐的**：
//! `flow.source.concurrency` 就是"同时几条连接压在达梦上"，
//! 而每个 `source_task` 一次处理一个分片。不需要连接池 ——
//! 连接数天然等于并发度，不会多也不会少。

use crate::config::ConnectionConfig;
use crate::source::method::dmfldr::parse::{RowBuildOptions, RowBuilder};
use crate::source::method::{DamengMethodKind, DamengReadContext, DamengReadMethod};
use crate::source::split::split_condition;
use crate::source::sql::DamengSqlBuilder;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use d2d_connector::{MemMeter, QueryConfig, SourceSpec, SourceSplit};
use d2d_core::{D2DError, D2DResult, RecordBatchStream};
use std::sync::Arc;
use tracing::{debug, info};

/// 通道深度：**有界**，这是进程内常驻有上界的原因
const CHANNEL_DEPTH: usize = 2;

/// 文本列的最大长度上限（`TextRowSet::for_cursor` 用它给每列缓冲定长）
pub(crate) const MAX_TEXT_COL_BYTES: usize = 4096;

/// 按 `type_contract` 的声明估算**一行的文本上界**（字节）
///
/// 只用于反推批次大小，不进任何正确性路径 —— 估大一点更安全（批次更小、更省内存）。
/// 拿不到声明长度的一律按 `MAX_TEXT_COL_BYTES`（4096）算。
pub fn declared_row_bytes(spec: &SourceSpec) -> usize {
    let Some(contract) = &spec.type_contract else {
        return 64;
    };
    let mut total = 0usize;
    for c in contract {
        total += declared_col_bytes(&c.source_type);
    }
    total.max(1)
}

fn declared_col_bytes(source_type: &str) -> usize {
    let t = source_type.trim().to_ascii_uppercase();
    // VARCHAR(n) / CHAR(n) / CHARCTER(n) …：n 是**字符数**，UTF-8 最坏 4 字节
    if let Some(open) = t.find('(') {
        let inside = &t[open + 1..];
        let n: usize = inside
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if n > 0 {
            if t.starts_with("VARCHAR") || t.starts_with("CHAR") {
                return (n * 4).min(MAX_TEXT_COL_BYTES);
            }
            // NUMBER(p,s) / DECIMAL(p,s)：p 位数字 + 符号小数点
            if t.starts_with("NUMBER") || t.starts_with("DECIMAL") || t.starts_with("NUMERIC") {
                return n + 8;
            }
        }
    }
    if t.starts_with("TIMESTAMP") || t.starts_with("DATETIME") {
        return 32;
    }
    if t.starts_with("DATE") || t.starts_with("TIME") {
        return 32;
    }
    if t.starts_with("BIGINT") || t.starts_with("INT") || t.starts_with("SMALLINT") {
        return 24;
    }
    if t.starts_with("FLOAT") || t.starts_with("DOUBLE") || t.starts_with("REAL") {
        return 32;
    }
    if t.starts_with("CLOB") || t.starts_with("BLOB") || t.starts_with("TEXT") {
        // ODBC 游标拉大字段是本末倒置（走 dmfldr），这里只保证不把预算算爆
        return MAX_TEXT_COL_BYTES;
    }
    64
}

#[derive(Clone)]
pub struct OdbcMethod {
    conn: ConnectionConfig,
    schema_name: Option<String>,
    table: String,
    /// 查询侧配置（列/条件下推），分区列已由连接器补齐
    query: QueryConfig,
    /// 一次网络来回拉多少行（`source.properties.fetch_size`）
    ///
    /// **这就是 ODBC 路径唯一的内存旋钮**，而且它的代价是可算的：
    ///
    /// ```text
    /// 每个连接的常驻 ≈ fetch_size × 一行的列缓冲之和
    /// 全进程        ≈ flow.source.concurrency × 上式
    /// ```
    ///
    /// 启动期账本会把这一项**如实**列出来（见 `unit_footprint`），装不下就拒绝启动 ——
    /// 而不是偷偷把它夹小（试过：夹到 8 MB 后内存从 6.2 GB 降到 3.6 GB，
    /// 但 1915 万行那一档从 172 s 掉到 247 s，用户却完全看不到发生了什么）。
    fetch_rows: usize,
    /// 查询超时（秒）
    query_timeout_sec: u64,
    /// **行集占用的申报尺子**（运行期读数，见 [`rowset_allocated_bytes`]）
    meter: Arc<MemMeter>,
}

impl OdbcMethod {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        conn: ConnectionConfig,
        schema_name: Option<String>,
        table: String,
        query: QueryConfig,
        fetch_rows: usize,
        query_timeout_sec: u64,
        meter: Arc<MemMeter>,
    ) -> Self {
        Self {
            conn,
            schema_name,
            table,
            query,
            fetch_rows: fetch_rows.max(1),
            query_timeout_sec,
            meter,
        }
    }

    /// 每个连接的行集缓冲常驻（字节）—— **启动期预测**（账本第 ①项用）
    ///
    /// `TextRowSet` 按 `行数 × 每列最大字节` 给**每一列**分配定长缓冲，
    /// 所以这个数就是 `fetch_size × 契约声明的一行列宽之和`。
    ///
    /// ⚠️ 这是**用契约推的**，真正分配多少要看 ODBC 驱动上报的列宽（还要 ×4 留给
    /// UTF-8，上限 `MAX_TEXT_COL_BYTES`）。跑起来之后的**实测**走
    /// `rowset_allocated_bytes`，两者并排打出来才看得出这个估计准不准
    /// （2026-10-16 实测：估 183 MB/连接、实报 185.6 MB/连接，差 1.4%）。
    pub fn rowset_bytes(fetch_rows: usize, per_row_bytes: usize) -> usize {
        fetch_rows.max(1).saturating_mul(per_row_bytes.max(1))
    }

    /// 本分片的完整 SQL：用户查询条件 AND 分区条件
    pub fn build_sql(&self, split: &SourceSplit) -> String {
        let builder = DamengSqlBuilder::new(self.schema_name.as_deref(), &self.table);
        builder.build_query(&self.query, &split_condition(split))
    }

    /// 探一个 `SELECT MIN(c), MAX(c)` —— range 分片缺边界时用（**阻塞**，调用方 spawn_blocking）
    ///
    /// 返回 `(min, max)` 的**文本形态**，由调用方按内核 `Literal::parse` 解释成类型。
    /// 走文本而不是按类型绑定：达梦的列类型五花八门，而这里只需要一个能比较的边界值，
    /// 文本 + 内核那套形态推断已经够用，也和 dmfldr 的探测路径一致。
    pub fn probe_min_max(
        cs: &str,
        sql: &str,
        timeout_sec: u64,
    ) -> D2DResult<(Option<String>, Option<String>)> {
        use odbc_api::{buffers::TextRowSet, ConnectionOptions, Cursor, Environment};

        let env = Environment::new().map_err(|e| odbc_err("创建 ODBC 环境失败", e))?;
        let conn = env
            .connect_with_connection_string(cs, ConnectionOptions::default())
            .map_err(|e| odbc_err(&format!("ODBC 连接失败（{}）", redact(cs)), e))?;
        let Some(mut cursor) = conn
            .execute(sql, (), Some(timeout_sec as usize))
            .map_err(|e| odbc_err(&format!("MIN/MAX 查询失败: {}", sql), e))?
        else {
            return Ok((None, None));
        };
        let mut buffers = TextRowSet::for_cursor(1, &mut cursor, Some(MAX_TEXT_COL_BYTES))
            .map_err(|e| odbc_err("分配 MIN/MAX 缓冲失败", e))?;
        let mut cur = cursor
            .bind_buffer(&mut buffers)
            .map_err(|e| odbc_err("绑定 MIN/MAX 缓冲失败", e))?;
        let Some(batch) = cur.fetch().map_err(|e| odbc_err("MIN/MAX 取数失败", e))? else {
            return Ok((None, None));
        };
        if batch.num_rows() == 0 {
            return Ok((None, None));
        }
        let to_s = |v: Option<&[u8]>| v.map(|b| String::from_utf8_lossy(b).into_owned());
        Ok((to_s(batch.at(0, 0)), to_s(batch.at(1, 0))))
    }

    /// **启动期自检** —— 这是"迁移 0 行也报成功"那道口子的封堵点
    ///
    /// 连不上、DSN 没配、驱动缺失，都必须**在开工前**大声失败，
    /// 而不是等到 `read()` 静静返回空流。
    ///
    /// 顺带把**列序**也验了：解析是按位置用 `type_contract` 的类型去打每一列的，
    /// 所以"契约声明序"必须与"查询返回序"逐列同名。名字对不上就报错并列出两边，
    /// 否则错误会以"某列类型对不上"的形式在几百万行之后炸出来。
    pub fn self_check(&self, expected_columns: &[String]) -> D2DResult<()> {
        let cs = self.conn.to_odbc_connection_string();
        // 只验通道与列序，不取数据。
        //
        // 把 `1 = 0` 当作**分区条件**传进去 —— `build_query` 会把它 AND 进 WHERE，
        // 于是原查询一个字都不用改（ORDER BY / LIMIT 也照常保留），
        // 只是必然返回 0 行。比套一层子查询干净，也不会撞上"子查询里能不能带 LIMIT"。
        let builder = DamengSqlBuilder::new(self.schema_name.as_deref(), &self.table);
        let sql = builder.build_query(&self.query, "1 = 0");

        let got = probe_columns(&cs, &sql, self.query_timeout_sec)?;
        if !got.is_empty() && got.len() == expected_columns.len() {
            let mismatch: Vec<String> = got
                .iter()
                .zip(expected_columns.iter())
                .enumerate()
                .filter(|(_, (a, b))| !a.eq_ignore_ascii_case(b))
                .map(|(i, (a, b))| format!("第 {} 列：查询给 '{}'，契约要 '{}'", i + 1, a, b))
                .collect();
            if !mismatch.is_empty() {
                return Err(D2DError::config(format!(
                    "odbc 列序与 type_contract 不一致：\n  {}\n\
                     解析是**按位置**用契约的类型去打每一列的，两者错位就会把 \
                     VARCHAR 当 DATETIME 解析。请让 source.query.columns 的顺序与 \
                     type_contract 逐列一致（名字大小写不敏感）。",
                    mismatch.join("\n  ")
                )));
            }
        } else if got.len() != expected_columns.len() {
            return Err(D2DError::config(format!(
                "odbc 查询返回 {} 列，type_contract 声明 {} 列 —— 必须逐列对齐",
                got.len(),
                expected_columns.len()
            )));
        }

        info!(
            dsn = %self.conn.dsn.as_deref().unwrap_or("<DSN-less>"),
            table = %self.table,
            columns = expected_columns.len(),
            "odbc 自检通过（连接 + 目标表可读 + 列序与契约一致）"
        );
        Ok(())
    }
}

/// 跑一条"不取数据、只验证通道"的查询，回读结果列名
fn probe_columns(cs: &str, sql: &str, timeout_sec: u64) -> D2DResult<Vec<String>> {
    use odbc_api::{ConnectionOptions, Environment, ResultSetMetadata};

    let env =
        Environment::new().map_err(|e| odbc_err("创建 ODBC 环境失败（unixODBC 装了没？）", e))?;
    let conn = env
        .connect_with_connection_string(cs, ConnectionOptions::default())
        .map_err(|e| {
            odbc_err(
                &format!(
                    "ODBC 连不上（连接串 {}）。检查 /etc/odbcinst.ini 的驱动注册、\
                     /etc/odbc.ini 的 DSN、以及 libdmdpi.so 是否在驱动目录里",
                    redact(cs)
                ),
                e,
            )
        })?;
    let Some(mut cursor) = conn
        .execute(sql, (), Some(timeout_sec as usize))
        .map_err(|e| odbc_err(&format!("ODBC 自检查询失败: {}", sql), e))?
    else {
        return Ok(Vec::new());
    };
    let names: Vec<String> = cursor
        .column_names()
        .map_err(|e| odbc_err("取结果列名失败", e))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(names)
}

/// 连接串里带密码，报错时不能原样打出去
fn redact(cs: &str) -> String {
    cs.split(';')
        .map(|kv| {
            if kv.to_ascii_uppercase().starts_with("PWD=") {
                "PWD=***"
            } else {
                kv
            }
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn odbc_err(what: &str, e: odbc_api::Error) -> D2DError {
    D2DError::external("odbc", format!("{}: {}", what, e))
}

#[async_trait]
impl DamengReadMethod for OdbcMethod {
    fn kind(&self) -> DamengMethodKind {
        DamengMethodKind::Odbc
    }

    async fn split(
        &self,
        _spec: &SourceSpec,
        _ctx: &DamengReadContext,
    ) -> D2DResult<Vec<SourceSplit>> {
        // 分工：**由连接器出分片**（`DamengSourceConnector::split`，走内核 partition 那套），
        // 方法只负责"给定一个分片怎么读"。这里报错防误用，而不是静默返回空。
        Err(D2DError::internal(
            "OdbcMethod::split 不该被调用 —— 分片由 DamengSourceConnector::split 产出",
        ))
    }

    async fn read(
        &self,
        ctx: &DamengReadContext,
        split: &SourceSplit,
    ) -> D2DResult<RecordBatchStream> {
        let sql = self.build_sql(split);
        debug!(split_id = %split.split_id, %sql, "odbc 游标读取开始");

        // ⚡ **一条游标 = 一个取数单元** —— 但这里不取任何券。
        //
        // 它曾经是两条读取路径里唯一**完全不看 grant** 的那条，于是"odbc 同时在库上
        // 几个连接"实际由 source_task 的个数决定。现在的答案更直接：**一次 `read`
        // 只开一条连接**，而"同时几次 `read`"由内核按任务发券卡死。
        // 连接器不需要知道上限是多少 —— 它只要守这条契约。
        let cs = self.conn.to_odbc_connection_string();
        let fetch_rows = self.fetch_rows;
        let timeout_sec = self.query_timeout_sec;
        let schema = ctx.schema.clone();
        let logical = ctx.logical_types.clone();
        let split_id = split.split_id.clone();
        let meter = Arc::clone(&self.meter);

        // 有界通道：满则读侧阻塞，形成背压 —— 与 dmfldr 物理面同一套
        let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<RecordBatch>>(CHANNEL_DEPTH);

        tokio::task::spawn_blocking(move || {
            if let Err(e) = pump(
                &cs,
                &sql,
                schema,
                logical,
                fetch_rows,
                timeout_sec,
                &tx,
                &meter,
            ) {
                let _ = tx.blocking_send(Err(e));
            }
            debug!(split_id = %split_id, "odbc 游标读取结束");
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

/// `odbc-api` **实际分配**的行集字节数 —— 读它自己刚建好的缓冲，不再估一遍
///
/// 与 [`OdbcMethod::rowset_bytes`]（按契约推的启动期预测）是**同一件事的两个来源**：
/// 那个用类型契约的声明宽度，这个问 `odbc-api` 要真实值。两者的差就是"静态估计准不准"。
///
/// 真实构成（见 odbc-api `TextColumn::try_new`）：
///
/// ```text
/// 每列    行数 × (max_str_len + 1)   ← max_str_len 由**驱动上报**的列宽得来，×4 留给 UTF-8，上限 4096
/// 每列    行数 × size_of::<isize>()  ← 指示器（NULL / 截断长度）
/// ```
///
/// 这正是"契约里写 VARCHAR(50)、驱动报 200、实际分配 200"那类差异的来源 ——
/// 所以它只能是**运行期读数**。
fn rowset_allocated_bytes(buffers: &odbc_api::buffers::TextRowSet) -> usize {
    use odbc_api::RowSetBuffer;
    let rows = buffers.row_array_size();
    let cols = buffers.num_cols();
    if rows == 0 || cols == 0 {
        return 0;
    }
    let text: usize = (0..cols)
        .map(|i| rows.saturating_mul(buffers.max_len(i) + 1))
        .sum();
    let indicators = rows
        .saturating_mul(cols)
        .saturating_mul(std::mem::size_of::<isize>());
    text + indicators
}

/// 阻塞读循环：游标 → `TextRowSet` 批次 → 契约类型 → `RecordBatch`
///
/// 类型解析走 [`RowBuilder`] —— **与 dmfldr 同一份实现**，不是同一套约定各写一遍。
#[allow(clippy::too_many_arguments)]
fn pump(
    cs: &str,
    sql: &str,
    schema: arrow::datatypes::SchemaRef,
    logical: Vec<d2d_types::LogicalType>,
    fetch_rows: usize,
    timeout_sec: u64,
    tx: &tokio::sync::mpsc::Sender<D2DResult<RecordBatch>>,
    meter: &Arc<MemMeter>,
) -> D2DResult<()> {
    // `num_result_cols` / `column_names` 来自 ResultSetMetadata，必须显式引入
    use odbc_api::{
        buffers::TextRowSet, ConnectionOptions, Cursor, Environment, ResultSetMetadata,
    };

    let env = Environment::new().map_err(|e| odbc_err("创建 ODBC 环境失败", e))?;
    let conn = env
        .connect_with_connection_string(cs, ConnectionOptions::default())
        .map_err(|e| odbc_err(&format!("ODBC 连接失败（{}）", redact(cs)), e))?;

    let Some(mut cursor) = conn
        .execute(sql, (), Some(timeout_sec as usize))
        .map_err(|e| odbc_err(&format!("ODBC 执行查询失败: {}", sql), e))?
    else {
        return Ok(());
    };

    // 列数必须与契约一致 —— 不一致就是拿错了契约（或 SQL 列序变了），
    // 继续往下走只会逐列错位地写进目标表。
    let ncols_expected = schema.fields().len();
    let ncols_actual = cursor
        .num_result_cols()
        .map_err(|e| odbc_err("取结果列数失败", e))? as usize;
    if ncols_actual != ncols_expected {
        let names: Vec<String> = cursor
            .column_names()
            .map_err(|e| odbc_err("取结果列名失败", e))?
            .filter_map(|r| r.ok())
            .collect();
        return Err(D2DError::schema(format!(
            "odbc 结果列数 {} != type_contract 列数 {}。查询返回的列是 [{}]，\
             契约声明的列是 [{}] —— odbc 模式下契约即权威 schema，两者必须逐列对齐",
            ncols_actual,
            ncols_expected,
            names.join(", "),
            schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let mut buffers = TextRowSet::for_cursor(fetch_rows, &mut cursor, Some(MAX_TEXT_COL_BYTES))
        .map_err(|e| {
            odbc_err(
                "分配行集缓冲失败（某列文本太长？可调大 MAX_TEXT_COL_BYTES）",
                e,
            )
        })?;

    // ── 如实申报这块行集 ──
    //
    // 挂账点就在分配点旁边，销账靠租约 drop（`?` 提前返回、panic 展开都会销）——
    // 手写 release 必然漏，漏了就是**越跑越高的假账**，比没有读数更坏。
    // 这块内存不在任何池的账上（池里是 Arrow 批次），所以它曾经只能以
    // "账本说 2144 MB、实测 6205 MB"的形式被发现。
    let _rowset_lease = meter.claim(rowset_allocated_bytes(&buffers));

    let mut row_set_cursor = cursor
        .bind_buffer(&mut buffers)
        .map_err(|e| odbc_err("绑定行集缓冲失败", e))?;

    let mut row_base: u64 = 0;
    while let Some(batch) = row_set_cursor
        .fetch()
        .map_err(|e| odbc_err("游标取数失败", e))?
    {
        let nrows = batch.num_rows();
        if nrows == 0 {
            continue;
        }
        let mut rb = RowBuilder::new(
            schema.clone(),
            logical.clone(),
            // 达梦/Oracle 口径：空串即 NULL；LOB 走 Unsupported（文本行集承载不了）
            RowBuildOptions::dameng_default(),
            nrows,
        )?;
        for r in 0..nrows {
            // `None` = SQL NULL；`Some(b"")` = 空串。两者语义不同，不能混。
            let cells: Vec<Option<&[u8]>> = (0..ncols_actual).map(|c| batch.at(c, r)).collect();
            rb.append_row(&cells, row_base + r as u64)?;
        }
        row_base += nrows as u64;
        let out = rb.finish()?;
        if out.num_rows() == 0 {
            continue;
        }
        if tx.blocking_send(Ok(out)).is_err() {
            return Ok(()); // 下游已断开
        }
    }
    Ok(())
}
