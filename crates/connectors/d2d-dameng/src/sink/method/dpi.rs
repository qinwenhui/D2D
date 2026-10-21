//! 达梦写入方式：**DPI 数组参数绑定**（`sink.method: dpi`）
//!
//! # 形状
//!
//! ```text
//! write_batch(一份数据)：
//!   开一条 DPI 连接（spawn_blocking 里，与 odbc 那条路同一个形状）
//!   dpi_prepare(`INSERT INTO t (c1,c2) VALUES (?,?)`)
//!   dpi_desc_param 拿每列的目标类型/精度/标度
//!   把这一份按 param_rows 切块，每块：
//!     值全部格式化成文本 → 列式数组缓冲（每列 rows × width 连续存放）
//!     dpi_bind_param2 绑定每个参数的数组（NULL 用指示器 -1）
//!     dpi_exec 一次执行整块（dsql_attr_paramset_size = 行数）
//!     用 PARAM_STATUS_PTR / dpi_row_count 核对目标端确认的行数
//!   连接随作用域关掉
//! ```
//!
//! # 与 odbc / dmfldr 的分工
//!
//! | | `dpi`（本条） | `odbc` | `dmfldr` |
//! |---|---|---|---|
//! | 速度 | 快（一次往返执行一整块） | 慢（多行 VALUES 文本拼接） | 最快（`direct: true` 时） |
//! | 值里的分隔符/换行 | **无限制**（绑定参数，不经文本） | 无限制 | **受限**（撞车即失败） |
//! | NULL | 指示器精确 | `Option<&str>` 精确 | 空值字面量（撞车即失败） |
//! | 约束检查 | **总是**由数据库检查 | 总是 | 仅 `direct: false` |
//! | 临时文件/子进程 | 无 | 无 | 有 |
//! | 运行机依赖 | `libdmdpi.so` | unixODBC | dmfldr 可执行 |
//!
//! # 事务
//!
//! 与 odbc 一致：取决于 DPI 连接的自动提交（缺省打开）。**不做跨批事务**，
//! 失败重跑可能产生重复行 —— 幂等靠目标端唯一键。一块参数集里若有行失败，
//! 状态数组会把它抓出来并让整任务失败；但已经落库的行不会自动回滚。
//!
//! # 字符集
//!
//! Arrow 字符串是 UTF-8，DPI 的 `DSQL_C_NCHAR` 按**客户端会话字符集**解释字节。
//! 所以本方法只接受 `character_code: utf8`（客户端也必须是 UTF-8），
//! 其它编码宁可启动期失败，也不要往库里写乱码。

use super::odbc::collect_texts;
use super::{DamengWriteKind, DamengWriteMethod, WriteContext};
use crate::config::ConnectionConfig;
use crate::dpi::ffi::{
    DpiHandle, DpiLibrary, DSQL_C_NCHAR, DSQL_NULL_DATA, DSQL_PARAM_ERROR, DSQL_PARAM_SUCCESS,
    DSQL_PARAM_SUCCESS_WITH_INFO, DSQL_PARAM_UNUSED, DSQL_ROW_COUNT_UNKNOWN,
};
use crate::source::method::dmfldr::params::CharacterCode;
use async_trait::async_trait;
use d2d_connector::SinkSession;
use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::os::raw::c_void;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// 一块参数集默认装多少行
///
/// 数组 DML 的往返次数 = 行数 / 本值。30M 行 × 28 列实测（.39，Doris → 达梦，
/// sink 并发 8）：2000 → 125.5s、5000 → 110.4s、10000 → 93.6s、20000 → 79.7s、
/// 50000 → 89.2s（过大反而回退）。取 20000；单块内存仍由
/// `max_buffer_mb` 封顶，遇到超长值会自动缩块。
const DEFAULT_PARAM_ROWS: usize = 20_000;
/// 单块绑定缓冲上限（MB）
///
/// 值长度不均时（一行特别长）会按行数折半重算，保证这一块的缓冲不超上限。
const DEFAULT_MAX_BUFFER_MB: u32 = 64;

/// DPI 写入参数（`sink.properties.dpi`）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DpiSinkParams {
    /// DPI 动态库路径；空 = 按平台缺省名在 `LD_LIBRARY_PATH` / 系统路径里找
    #[serde(default)]
    pub library: Option<PathBuf>,
    /// 一次 `dpi_exec` 执行多少行（数组 DML 的行数，1..=65535）
    #[serde(default = "default_param_rows")]
    pub param_rows: usize,
    /// 单块参数缓冲上限（MB，1..=1024）
    #[serde(default = "default_max_buffer_mb")]
    pub max_buffer_mb: u32,
    /// 会话字符集声明（**不是设置**）—— 目前只接受 `utf8`
    ///
    /// Arrow 字符串是 UTF-8；DPI 按客户端会话字符集解释 `DSQL_C_NCHAR` 的字节。
    /// 声明成 gbk 只会让"我们发 UTF-8、驱动按 GBK 读"变成静默乱码，所以直接拒绝。
    #[serde(default)]
    pub character_code: CharacterCode,
}

fn default_param_rows() -> usize {
    DEFAULT_PARAM_ROWS
}
fn default_max_buffer_mb() -> u32 {
    DEFAULT_MAX_BUFFER_MB
}

impl Default for DpiSinkParams {
    fn default() -> Self {
        Self {
            library: None,
            param_rows: DEFAULT_PARAM_ROWS,
            max_buffer_mb: DEFAULT_MAX_BUFFER_MB,
            character_code: CharacterCode::Utf8,
        }
    }
}

impl DpiSinkParams {
    /// 从 `sink.properties.dpi` 解析（缺失 = 默认值）
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let Some(v) = props.get("dpi") else {
            return Ok(Self::default());
        };
        if v.is_null() {
            return Ok(Self::default());
        }
        serde_json::from_value(v.clone())
            .map_err(|e| D2DError::config(format!("invalid sink.properties.dpi: {}", e)))
    }

    pub fn max_buffer_bytes(&self) -> usize {
        self.max_buffer_mb as usize * 1024 * 1024
    }

    /// 启动期校验（拼错的键由 `deny_unknown_fields` 在解析时拦掉）
    pub fn validate(&self) -> D2DResult<()> {
        let mut errs: Vec<String> = Vec::new();
        if !(1..=65_535).contains(&self.param_rows) {
            errs.push(format!(
                "param_rows must be 1..=65535, got {}",
                self.param_rows
            ));
        }
        if !(1..=1024).contains(&self.max_buffer_mb) {
            errs.push(format!(
                "max_buffer_mb must be 1..=1024, got {}",
                self.max_buffer_mb
            ));
        }
        if self.character_code != CharacterCode::Utf8 {
            errs.push(format!(
                "character_code 目前只支持 utf8（Arrow 字符串是 UTF-8，DPI 按会话字符集解释字节），\
                 当前为 {:?}；需要其它编码请在上游转换，或改用 sink.method: dmfldr",
                self.character_code
            ));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(D2DError::config(format!(
                "invalid sink.properties.dpi: {}",
                errs.join("; ")
            )))
        }
    }
}

/// DPI 数组 DML 写入方法
pub struct DamengDpiWriteMethod {
    params: DpiSinkParams,
    conn: ConnectionConfig,
    library: Arc<DpiLibrary>,
}

impl DamengDpiWriteMethod {
    pub fn new(
        params: DpiSinkParams,
        conn: ConnectionConfig,
        library: Arc<DpiLibrary>,
    ) -> D2DResult<Self> {
        validate_connection(&conn)?;
        Ok(Self {
            params,
            conn,
            library,
        })
    }
}

#[async_trait]
impl DamengWriteMethod for DamengDpiWriteMethod {
    fn kind(&self) -> DamengWriteKind {
        DamengWriteKind::Dpi
    }

    async fn begin_session(&self, ctx: Arc<WriteContext>) -> D2DResult<Box<dyn SinkSession>> {
        Ok(Box::new(DpiWriteSession {
            ctx,
            params: self.params.clone(),
            conn: self.conn.clone(),
            library: Arc::clone(&self.library),
        }))
    }
}

/// 一条会话 = 一次真实写入（内核攒够一份才开一条）
struct DpiWriteSession {
    ctx: Arc<WriteContext>,
    params: DpiSinkParams,
    conn: ConnectionConfig,
    library: Arc<DpiLibrary>,
}

#[async_trait]
impl SinkSession for DpiWriteSession {
    async fn write_batch(&mut self, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let ctx = Arc::clone(&self.ctx);
        let params = self.params.clone();
        let conn = self.conn.clone();
        let lib = Arc::clone(&self.library);
        tokio::task::spawn_blocking(move || insert_batch(&lib, &params, &ctx, &conn, batch))
            .await
            .map_err(|e| D2DError::internal(format!("达梦 DPI 写入任务 panic: {}", e)))?
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // 每块都已由 DPI 连接的自动提交落库（与 odbc 路径同一语义）
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        // 没有缓冲可清；已提交的块不回滚（幂等靠目标端唯一键）
        Ok(())
    }
}

/// 一条 DPI 写连接（env/con/stmt）
///
/// 与读侧一样：句柄只在创建它的 `spawn_blocking` 线程里用，不跨 await。
struct WriteConn {
    lib: Arc<DpiLibrary>,
    env: DpiHandle,
    con: DpiHandle,
    stmt: DpiHandle,
    logged_in: bool,
}

impl WriteConn {
    fn open(lib: &Arc<DpiLibrary>, conn: &ConnectionConfig) -> D2DResult<Self> {
        let env = lib.alloc_env()?;
        let con = match lib.alloc_con(env) {
            Ok(c) => c,
            Err(e) => {
                lib.free_env(env);
                return Err(e);
            }
        };
        if let Err(e) = lib.login(con, &server_of(conn), &conn.user, &conn.password) {
            lib.free_con(con);
            lib.free_env(env);
            return Err(e);
        }
        let stmt = match lib.alloc_stmt(con) {
            Ok(s) => s,
            Err(e) => {
                lib.logout(con);
                lib.free_con(con);
                lib.free_env(env);
                return Err(e);
            }
        };
        Ok(Self {
            lib: Arc::clone(lib),
            env,
            con,
            stmt,
            logged_in: true,
        })
    }
}

impl Drop for WriteConn {
    fn drop(&mut self) {
        self.lib.free_stmt(self.stmt);
        self.stmt = std::ptr::null_mut();
        if self.logged_in {
            self.lib.logout(self.con);
            self.logged_in = false;
        }
        self.lib.free_con(self.con);
        self.lib.free_env(self.env);
        self.con = std::ptr::null_mut();
        self.env = std::ptr::null_mut();
    }
}

/// 真正的写入：开连接 → prepare → 分块绑定 → 数组执行 → 对账
fn insert_batch(
    lib: &Arc<DpiLibrary>,
    params: &DpiSinkParams,
    ctx: &WriteContext,
    conn_cfg: &ConnectionConfig,
    batch: arrow::array::RecordBatch,
) -> D2DResult<usize> {
    let ncols = batch.num_columns();
    if ncols == 0 {
        return Err(D2DError::schema(
            "达梦 DPI 写入收到 0 列的数据 —— 上游 schema 不对",
        ));
    }
    let rows = batch.num_rows();
    if rows == 0 {
        return Ok(0);
    }

    let columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| d2d_connector::quote_ident(f.name()))
        .collect();
    let sql = build_insert(&ctx.qualified_table(), &columns);

    let conn = WriteConn::open(lib, conn_cfg)?;
    // prepare 失败的诊断信息经常太抽象（例如「个数不匹配」），把 SQL 带上
    // 才能一眼看出是列不存在、重名，还是占位符数量不对。
    conn.lib.prepare(conn.stmt, &sql).map_err(|e| {
        D2DError::external(
            "dameng/dpi",
            format!("{}（SQL: {}）", e, truncate_sql(&sql)),
        )
    })?;

    let nparams = conn.lib.number_params(conn.stmt)?;
    if nparams != ncols {
        return Err(D2DError::schema(format!(
            "达梦 DPI 写入：SQL 有 {} 个占位符，数据有 {} 列。\
             INSERT 列表按数据 schema 生成，出现差异说明上游 schema 不对。SQL: {}",
            nparams, ncols, sql
        )));
    }

    let mut descs = Vec::with_capacity(ncols);
    for i in 1..=ncols {
        descs.push(conn.lib.desc_param(conn.stmt, i)?);
    }

    // 状态/处理行数按最大块预分配：指针在整批期间保持有效
    let max_rows = params.param_rows;
    let mut status = vec![DSQL_PARAM_UNUSED; max_rows];
    let mut processed: u64 = 0;
    conn.lib
        .set_param_status_ptr(conn.stmt, status.as_mut_ptr())?;
    conn.lib
        .set_params_processed_ptr(conn.stmt, &mut processed)?;

    let cap = params.max_buffer_bytes();
    let mut done = 0usize;
    while done < rows {
        let want = max_rows.min(rows - done);
        let texts = collect_texts(&batch, done, want)?;
        let (n, widths) = plan_chunk(&texts, ncols, want, cap)?;
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let mut buffers = build_param_buffers(&texts, ncols, n, &widths, done, &names)?;

        // **必须先设 PARAMSET_SIZE 再绑定**：驱动在绑定参数时要按数组行数
        // 解释缓冲（dmPython 的 `Cursor_BindParamVariable` 也是这个顺序）。
        // 反过来（先绑后设）单参数碰巧能跑，多参数会把列数据错位解释。
        conn.lib.set_paramset_size(conn.stmt, n)?;
        status[..n].fill(DSQL_PARAM_UNUSED);
        processed = 0;

        for c in 0..ncols {
            let val = unsafe { buffers.data.as_mut_ptr().add(buffers.offsets[c]) as *mut c_void };
            let ind_ptr = unsafe { buffers.ind.as_mut_ptr().add(c * n) };
            let act_ptr = unsafe { buffers.act.as_mut_ptr().add(c * n) };
            conn.lib.bind_param2(
                conn.stmt,
                c + 1,
                DSQL_C_NCHAR,
                &descs[c],
                val,
                widths[c],
                ind_ptr,
                act_ptr,
            )?;
        }

        match conn.lib.exec(conn.stmt) {
            Ok(_) => {
                // 先对账、再解绑：`dpi_row_count` 在解绑之后可能就没了
                // （官方 dmPython 也是 exec → 取 rowcount → unbind 的顺序）。
                let verified = verify_chunk(&conn.lib, conn.stmt, &status, processed, n, done, ctx);
                let unbind = conn.lib.unbind_params(conn.stmt);
                verified?;
                unbind?;
            }
            Err(e) => {
                // 缓冲马上要释放，必须解绑；解绑失败不覆盖真正的执行错误
                let _ = conn.lib.unbind_params(conn.stmt);
                return Err(e);
            }
        }
        debug!(rows = n, "达梦 DPI：一块参数集已写入");
        done += n;
    }

    info!(
        rows,
        columns = ncols,
        param_rows = params.param_rows,
        "达梦写入完成（DPI 数组参数绑定）"
    );
    Ok(rows)
}

/// 一块参数集的列式绑定缓冲
#[derive(Debug)]
struct ParamBuffers {
    /// 每列 `rows × width` 字节连续存放
    data: Vec<u8>,
    /// 长度指示器，**列优先**：第 c 列第 r 行在 `c * rows + r`
    ind: Vec<i64>,
    /// 实际长度（与 `ind` 同形；输入参数照 dmPython 一并填写）
    act: Vec<i64>,
    /// 第 c 列在 `data` 里的起始偏移
    offsets: Vec<usize>,
}

/// 把行优先的文本摊成**列优先**的参数数组
///
/// `texts` 是 [`collect_texts`] 的产物（`r * ncols + c`），而 DPI 的每个参数
/// 要一块连续数组（`c * rows + r`）。两个下标**不能混用**：混了以后单列碰巧
/// 正确，多列会把长度指示器串到别的列上 —— 实测症状是时间戳被按 ID 的长度
/// 截断，驱动只报一句「无效的日期字符串」。
fn build_param_buffers(
    texts: &[Option<String>],
    ncols: usize,
    rows: usize,
    widths: &[usize],
    first_row: usize,
    names: &[&str],
) -> D2DResult<ParamBuffers> {
    let row_bytes: usize = widths.iter().sum();
    let mut data = vec![0u8; rows * row_bytes];
    let mut ind = vec![0i64; ncols * rows];
    let mut act = vec![0i64; ncols * rows];
    let mut offsets = Vec::with_capacity(ncols);
    let mut off = 0usize;
    for w in widths {
        offsets.push(off);
        off += *w * rows;
    }

    for r in 0..rows {
        for c in 0..ncols {
            let src = r * ncols + c;
            let dst_i = c * rows + r;
            match &texts[src] {
                None => {
                    ind[dst_i] = DSQL_NULL_DATA;
                    act[dst_i] = 0;
                }
                Some(v) => {
                    let bytes = v.as_bytes();
                    if bytes.contains(&0) {
                        return Err(D2DError::external(
                            "dameng/dpi",
                            format!(
                                "列 '{}' 的第 {} 行值含 NUL 字节：DPI 的 DSQL_C_NCHAR \
                                 以 NUL 结尾，写进去会被静默截断。请把它转成十六进制文本，\
                                 或改用 sink.method: odbc。",
                                names[c],
                                first_row + r + 1
                            ),
                        ));
                    }
                    let dst = offsets[c] + r * widths[c];
                    data[dst..dst + bytes.len()].copy_from_slice(bytes);
                    ind[dst_i] = bytes.len() as i64;
                    act[dst_i] = bytes.len() as i64;
                }
            }
        }
    }
    Ok(ParamBuffers {
        data,
        ind,
        act,
        offsets,
    })
}

/// 一块参数集的目标端对账
///
/// 两路证据：`PARAM_STATUS_PTR`（逐行）与 `dpi_row_count`（整块）。
/// 任一显示"没全进去"都大声失败 —— 数组 DML 允许"整体成功、个别行失败"。
fn verify_chunk(
    lib: &DpiLibrary,
    stmt: DpiHandle,
    status: &[u16],
    processed: u64,
    n: usize,
    done: usize,
    ctx: &WriteContext,
) -> D2DResult<()> {
    let reported = process_status(status, processed, n, done, &ctx.qualified_table())?;

    let count = lib.row_count(stmt)?;
    if count != DSQL_ROW_COUNT_UNKNOWN && count != n as i64 {
        return Err(D2DError::external(
            "dameng/dpi",
            format!(
                "达梦 DPI 数组写入：本块提交 {} 行，目标端 dpi_row_count 只确认 {} 行。",
                n, count
            ),
        ));
    }
    if reported == 0 && count == DSQL_ROW_COUNT_UNKNOWN {
        warn!(
            rows = n,
            "达梦 DPI：驱动既没给 PARAM_STATUS 也没给 row_count，只能以 dpi_exec 成功为准"
        );
    }
    Ok(())
}

/// 逐行状态校验（纯函数，便于单测）；返回驱动报告的处理行数
///
/// 数组 DML 允许「整体 rc 成功、个别行失败」，所以状态数组是必须看的。
/// `processed == 0` 表示驱动没写处理行数（老驱动），此时交给 `dpi_row_count`。
fn process_status(
    status: &[u16],
    processed: u64,
    n: usize,
    done: usize,
    table: &str,
) -> D2DResult<usize> {
    let reported = processed as usize;
    if reported > n {
        return Err(D2DError::internal(format!(
            "DPI 报告处理了 {} 组参数，超过本块的 {} 行",
            reported, n
        )));
    }
    if reported == 0 {
        return Ok(0);
    }
    let failed: Vec<usize> = status[..reported]
        .iter()
        .enumerate()
        .filter(|(_, s)| **s == DSQL_PARAM_ERROR)
        .map(|(i, _)| i + 1)
        .collect();
    if !failed.is_empty() {
        return Err(D2DError::external(
            "dameng/dpi",
            format!(
                "达梦 DPI 数组写入：第 {}..{} 行中有 {} 行失败（块内首次失败在第 {} 行）。\
                 已成功的行不会自动回滚；表 '{}' 可能处于部分写入状态。",
                done + 1,
                done + reported,
                failed.len(),
                failed[0],
                table
            ),
        ));
    }
    let ok = status[..reported]
        .iter()
        .filter(|s| matches!(**s, DSQL_PARAM_SUCCESS | DSQL_PARAM_SUCCESS_WITH_INFO))
        .count();
    if ok != n {
        return Err(D2DError::external(
            "dameng/dpi",
            format!(
                "达梦 DPI 数组写入：本块 {} 行，目标端只确认 {} 行成功\
                 （{} 行状态未知或未处理）。",
                n,
                ok,
                n - ok
            ),
        ));
    }
    Ok(reported)
}

/// 一段 `rows × ncols` 文本的列宽与块大小规划
///
/// 宽度 = 本块该列最大字节长度 + 1（NUL），下限 1（空串也要留结尾）。
/// 总缓冲超过 `cap` 时按行数折半重算 —— 避免一行超长值把整块撑爆。
fn plan_chunk(
    texts: &[Option<String>],
    ncols: usize,
    mut rows: usize,
    cap: usize,
) -> D2DResult<(usize, Vec<usize>)> {
    debug_assert!(rows > 0);
    loop {
        let mut widths = vec![1usize; ncols];
        for r in 0..rows {
            for c in 0..ncols {
                if let Some(v) = &texts[r * ncols + c] {
                    let need = v.len() + 1;
                    if need > widths[c] {
                        widths[c] = need;
                    }
                }
            }
        }
        let total = rows * widths.iter().sum::<usize>();
        if total <= cap {
            return Ok((rows, widths));
        }
        if rows == 1 {
            return Err(D2DError::external(
                "dameng/dpi",
                format!(
                    "单行 {} 列需要 {} 字节参数缓冲，超过 sink.properties.dpi.max_buffer_mb \
                     对应的 {} 字节。LOB/超长值请改用 sink.method: dmfldr，或调大 max_buffer_mb。",
                    ncols, total, cap
                ),
            ));
        }
        rows = (rows / 2).max(1);
    }
}

/// 错误信息里附带的 SQL（太长就截断，避免一行日志刷屏）
fn truncate_sql(sql: &str) -> String {
    const MAX: usize = 2048;
    if sql.len() <= MAX {
        return sql.to_string();
    }
    let mut end = MAX;
    while end > 0 && !sql.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…（共 {} 字节）", &sql[..end], sql.len())
}

/// `INSERT INTO t (c1,c2) VALUES (?,?)`
fn build_insert(table: &str, cols: &[String]) -> String {
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        cols.join(","),
        vec!["?"; cols.len()].join(",")
    )
}

/// `host:port` 或 dm_svc.conf 服务名（与读侧同一口径）
fn server_of(conn: &ConnectionConfig) -> String {
    match conn.svc.as_deref().map(str::trim) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => format!("{}:{}", conn.host.trim(), conn.port),
    }
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
            "dpi 写入暂不支持 connection.auth_as（OS 认证方式）—— \
             请用普通用户名/密码连接，或改用 sink.method: odbc",
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
            "dpi 写入需要 connection.host（或 dm_svc.conf 服务名 connection.svc）—— \
             dpi 的登录串不吃 ODBC 的 dsn",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};

    fn cfg(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn test_defaults_and_unknown_key() {
        let p = DpiSinkParams::from_props(&cfg(serde_json::json!({}))).unwrap();
        assert_eq!(p.param_rows, 20_000);
        assert_eq!(p.max_buffer_mb, 64);
        assert!(p.validate().is_ok());

        let e = DpiSinkParams::from_props(&cfg(serde_json::json!({
            "dpi": {"param_row": 1}
        })))
        .unwrap_err()
        .to_string();
        assert!(e.contains("param_row"), "{}", e);
    }

    #[test]
    fn test_non_utf8_is_rejected() {
        let p = DpiSinkParams {
            character_code: CharacterCode::Gbk,
            ..Default::default()
        };
        let e = p.validate().unwrap_err().to_string();
        assert!(e.contains("utf8"), "{}", e);
    }

    #[test]
    fn test_build_insert_shape() {
        let cols = vec!["\"A\"".to_string(), "\"B\"".to_string()];
        assert_eq!(
            build_insert("\"S\".\"T\"", &cols),
            "INSERT INTO \"S\".\"T\" (\"A\",\"B\") VALUES (?,?)"
        );
    }

    #[test]
    fn test_plan_chunk_widths_include_nul() {
        let texts = vec![
            Some("abc".to_string()),
            None,
            Some("".to_string()),
            Some("de".to_string()),
        ];
        let (rows, widths) = plan_chunk(&texts, 2, 2, 1 << 20).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(widths, vec![4, 3]); // "abc"+NUL / "de"+NUL
    }

    /// 一行超长值不能把整块撑爆：折半后把它挤出本块，宽度重新算
    #[test]
    fn test_plan_chunk_shrinks_for_huge_values() {
        let mut texts: Vec<Option<String>> = (0..100).map(|_| Some("y".into())).collect();
        texts[50] = Some("x".repeat(10_000));
        let (rows, widths) = plan_chunk(&texts, 1, 100, 4096).unwrap();
        assert_eq!(rows, 50, "折半到 50 行时超长值已被挤出");
        assert_eq!(widths, vec![2]);
        assert!(rows * widths[0] <= 4096);
    }

    #[test]
    fn test_plan_chunk_single_huge_row_fails() {
        let texts = vec![Some("x".repeat(10_000))];
        let e = plan_chunk(&texts, 1, 1, 4096).unwrap_err().to_string();
        assert!(e.contains("max_buffer_mb"), "{}", e);
    }

    /// **回归**：文本行优先、绑定缓冲列优先 —— 指示器不能串列
    ///
    /// 曾经的 bug：`ind` 按 `r * ncols + c` 填，而驱动按 `c * rows + r` 读。
    /// 单列碰巧正确；两列时第二列拿到第一列的长度（时间戳被截成 3 字节），
    /// 驱动只报「无效的日期字符串」。
    #[test]
    fn test_param_buffers_are_column_major() {
        let texts = vec![
            Some("1".to_string()),
            Some("2026-10-16 18:23:57.000000".to_string()),
            Some("22".to_string()),
            None,
        ];
        let widths = vec![3usize, 27];
        let b = build_param_buffers(&texts, 2, 2, &widths, 0, &["ID", "TS"]).unwrap();

        // 第 0 列两行、第 1 列两行 —— 列优先，不是 [1, 26, 2, -1]
        assert_eq!(b.ind, vec![1, 2, 26, DSQL_NULL_DATA]);
        assert_eq!(b.act, vec![1, 2, 26, 0]);
        assert_eq!(b.offsets, vec![0, 6]);

        assert_eq!(&b.data[0..3], b"1\0\0");
        assert_eq!(&b.data[3..6], b"22\0");
        assert_eq!(&b.data[6..32], b"2026-10-16 18:23:57.000000");
        assert_eq!(b.data[32], 0, "NUL 结尾");
    }

    #[test]
    fn test_param_buffers_reject_nul() {
        let texts = vec![Some("a\0b".to_string())];
        let e = build_param_buffers(&texts, 1, 1, &[4], 10, &["V"])
            .unwrap_err()
            .to_string();
        assert!(e.contains("NUL"), "{}", e);
        assert!(e.contains("11"), "要给出绝对行号: {}", e);
    }

    /// 逐行状态：全部成功（含 WITH_INFO）放行；有 ERROR / 数量不符必须失败
    #[test]
    fn test_process_status() {
        let n = 3;
        assert_eq!(
            process_status(
                &[
                    DSQL_PARAM_SUCCESS,
                    DSQL_PARAM_SUCCESS_WITH_INFO,
                    DSQL_PARAM_SUCCESS
                ],
                3,
                n,
                0,
                "\"T\""
            )
            .unwrap(),
            3
        );

        // 个别行失败
        let e = process_status(
            &[DSQL_PARAM_SUCCESS, DSQL_PARAM_ERROR, DSQL_PARAM_SUCCESS],
            3,
            n,
            100,
            "\"T\"",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("1 行失败"), "{}", e);
        assert!(e.contains("101"), "要给出绝对行号: {}", e);
        assert!(e.contains("部分写入"), "{}", e);

        // 驱动只处理了 2 行（第 3 行未处理）
        let e = process_status(
            &[DSQL_PARAM_SUCCESS, DSQL_PARAM_SUCCESS, DSQL_PARAM_UNUSED],
            2,
            n,
            0,
            "\"T\"",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("只确认 2 行"), "{}", e);

        // 老驱动不写处理行数 → 交由 row_count 判定
        assert_eq!(process_status(&[], 0, n, 0, "\"T\"").unwrap(), 0);
    }

    fn batch_of(cols: Vec<(&str, ArrayRef)>) -> arrow::array::RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, c)| Field::new(*n, c.data_type().clone(), true))
            .collect();
        let data: Vec<ArrayRef> = cols.into_iter().map(|(_, c)| c).collect();
        arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), data).unwrap()
    }

    /// 文本必须**行优先**（参数按行依次填进各列的数组）
    #[test]
    fn test_texts_are_row_major_for_param_arrays() {
        let b = batch_of(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2]))),
            ("name", Arc::new(StringArray::from(vec![Some("a"), None]))),
        ]);
        let t = collect_texts(&b, 0, 2).unwrap();
        assert_eq!(
            t,
            vec![Some("1".into()), Some("a".into()), Some("2".into()), None]
        );
    }
}
