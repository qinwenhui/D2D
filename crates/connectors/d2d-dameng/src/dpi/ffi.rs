//! 达梦 DPI（DM Performance Interface）的最小 FFI 子集
//!
//! 读（数组取数）与写（数组参数绑定）共用这一份声明。两边各用各的函数子集，
//! 会话状态不共享 —— 见 [`crate::dpi`] 的模块说明。
//!
//! # 为什么是手写，不是 bindgen
//!
//! 只需要 ~15 个函数；整份 `DPI.h` 有 118 个 API、大量平台宏。手写让
//! **构建期没有任何达梦依赖**，符号缺失也只在 `method: dpi` 的配置上暴露。
//!
//! # 为什么是运行期 `libloading`
//!
//! `libdmdpi.so` / `dmdpi.dll` 由达梦客户端提供，路径随安装而变。
//! 编译期链接会让整个连接器硬依赖它 —— 而 odbc / dmfldr 两条路根本不需要。
//!
//! # 类型宽度的硬约束
//!
//! 达梦头文件在 64 位下用 `-DDM64` 把 `slength` / `ulength` 定义成 8 字节。
//! Rust 侧固定用 `i64` / `u64`，与 x86_64 的 C ABI 一致；32 位构建直接拒绝
//! （`compile_error!`），免得把 4 字节的指示器塞给 8 字节的库。
//!
//! 声明来源：达梦 DM8 SDK `include/DPI.h`、`DPItypes.h`（见 `NOTICE-dpi.md`）。

// 句柄只可能来自本模块的 alloc / login；调用方（session）在同一个阻塞线程里
// 按序使用。把每个包装都标成 `unsafe fn` 只会让调用处遍布 unsafe 块，
// 拦不住任何真实错误 —— 真正的安全边界是「不把裸句柄交给本模块以外的人」。
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use d2d_core::{D2DError, D2DResult};
use libloading::Library;
use std::ffi::CString;
use std::os::raw::c_void;
use std::path::Path;
use std::sync::Arc;

#[cfg(not(target_pointer_width = "64"))]
compile_error!(
    "dpi 方法只支持 64 位目标：达梦 DPI 的 slength/ulength 在 DM64 下是 8 字节，\
     32 位构建的 ABI 不兼容"
);

/// `DPItypes.h`：`typedef void* dhandle;`
pub type DpiHandle = *mut c_void;
/// `DPItypes.h`：`typedef sdint2 DPIRETURN;`
pub type DpiReturn = i16;

// ── DPI.h 常量 ──
pub const DSQL_NULL_DATA: i64 = -1;
pub const DSQL_NO_DATA: DpiReturn = 100;
/// `DSQL_SUCCEEDED(rc) (((rc)&(~1))==0)` —— 0 = SUCCESS，1 = SUCCESS_WITH_INFO
pub fn succeeded(rc: DpiReturn) -> bool {
    (rc & !1) == 0
}
pub const DSQL_HANDLE_ENV: i16 = 1;
pub const DSQL_HANDLE_DBC: i16 = 2;
pub const DSQL_HANDLE_STMT: i16 = 3;

/// 绑定成字符串（NUL 结尾）；所有列都用它，类型解析交给逻辑面的 RowBuilder
pub const DSQL_C_NCHAR: i16 = 0;

/// DPIext.h：数组取数行数
pub const DSQL_ATTR_ROWSET_SIZE: i32 = 9;

// ── 写方向：参数绑定 / 数组 DML ──

/// DPIext.h：`DSQL_PARAM_INPUT` —— 只进不出的绑定参数
pub const DSQL_PARAM_INPUT: i16 = 1;
/// DPIext.h：一次 `dpi_exec` 处理多少组参数（数组 DML 的行数）
pub const DSQL_ATTR_PARAMSET_SIZE: i32 = 22;
/// DPIext.h：每行执行状态数组（u16，`DSQL_PARAM_*`）
pub const DSQL_ATTR_PARAM_STATUS_PTR: i32 = 20;
/// DPIext.h：驱动实际处理了多少行（u64）
pub const DSQL_ATTR_PARAMS_PROCESSED_PTR: i32 = 21;

// DSQL_ATTR_PARAM_STATUS_PTR 的取值
pub const DSQL_PARAM_SUCCESS: u16 = 0;
pub const DSQL_PARAM_SUCCESS_WITH_INFO: u16 = 6;
pub const DSQL_PARAM_ERROR: u16 = 5;
pub const DSQL_PARAM_UNUSED: u16 = 7;

/// DPI.h：`dpi_row_count` 拿不到精确行数时的哨兵值
pub const DSQL_ROW_COUNT_UNKNOWN: i64 = -99;

// ── 函数指针类型（逐字对应头文件）──
type FnAllocEnv = unsafe extern "C" fn(*mut DpiHandle) -> DpiReturn;
type FnAllocCon = unsafe extern "C" fn(DpiHandle, *mut DpiHandle) -> DpiReturn;
type FnAllocStmt = unsafe extern "C" fn(DpiHandle, *mut DpiHandle) -> DpiReturn;
type FnFreeEnv = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnFreeCon = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnFreeStmt = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnLogin = unsafe extern "C" fn(
    DpiHandle,
    *const i8, // sdbyte* svr
    *const i8, // sdbyte* user
    *const i8, // sdbyte* pwd
) -> DpiReturn;
type FnLogout = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnCancel = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnSetStmtAttr = unsafe extern "C" fn(DpiHandle, i32, *mut c_void, i32) -> DpiReturn;
type FnExecDirect = unsafe extern "C" fn(DpiHandle, *const i8) -> DpiReturn;
type FnNumberColumns = unsafe extern "C" fn(DpiHandle, *mut i16) -> DpiReturn;
type FnDescColumn = unsafe extern "C" fn(
    DpiHandle,
    i16,      // icol
    *mut i8,  // name
    i16,      // buf_len
    *mut i16, // name_len
    *mut i16, // sqltype
    *mut u64, // col_sz (ulength)
    *mut i16, // dec_digits
    *mut i16, // nullable
) -> DpiReturn;
type FnBindCol2 = unsafe extern "C" fn(
    DpiHandle,
    u16,         // icol
    i16,         // ctype
    *mut c_void, // val
    i64,         // buf_len
    *mut i64,    // ind
    *mut i64,    // act_len
) -> DpiReturn;
type FnFetch = unsafe extern "C" fn(DpiHandle, *mut u64) -> DpiReturn;
type FnPrepare = unsafe extern "C" fn(DpiHandle, *const i8) -> DpiReturn;
type FnNumberParams = unsafe extern "C" fn(DpiHandle, *mut i16) -> DpiReturn;
type FnDescParam = unsafe extern "C" fn(
    DpiHandle,
    u16,      // iparam (1-based)
    *mut i16, // sql_type
    *mut u64, // prec (ulength)
    *mut i16, // scale
    *mut i16, // nullable
) -> DpiReturn;
type FnBindParam2 = unsafe extern "C" fn(
    DpiHandle,
    u16,         // iparam (1-based)
    i16,         // param_type (DSQL_PARAM_INPUT)
    i16,         // ctype
    i16,         // dtype（服务端类型）
    u64,         // precision
    i16,         // scale
    *mut c_void, // buf（数组：每行 `buf_len` 字节）
    i64,         // buf_len
    *mut i64,    // ind（长度或 NULL_DATA）
    *mut i64,    // act_len
) -> DpiReturn;
type FnExec = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnRowCount = unsafe extern "C" fn(DpiHandle, *mut i64) -> DpiReturn;
type FnUnbindParams = unsafe extern "C" fn(DpiHandle) -> DpiReturn;
type FnGetDiagRec = unsafe extern "C" fn(
    i16, // hndl_type
    DpiHandle,
    i16,      // rec_num
    *mut i32, // err_code
    *mut i8,  // err_msg
    i16,      // buf_sz
    *mut i16, // msg_len
) -> DpiReturn;

/// 已加载的 DPI 动态库
///
/// 函数指针是 `Copy`；`Library` 必须活得比所有指针长 —— 所以两者放在同一个
/// `Arc` 里，调用方只拿 `Arc<DpiLibrary>`。
pub struct DpiLibrary {
    _lib: Library,
    alloc_env: FnAllocEnv,
    alloc_con: FnAllocCon,
    alloc_stmt: FnAllocStmt,
    free_env: FnFreeEnv,
    free_con: FnFreeCon,
    free_stmt: FnFreeStmt,
    login: FnLogin,
    logout: FnLogout,
    cancel: FnCancel,
    set_stmt_attr: FnSetStmtAttr,
    exec_direct: FnExecDirect,
    number_columns: FnNumberColumns,
    desc_column: FnDescColumn,
    bind_col2: FnBindCol2,
    fetch: FnFetch,
    prepare: FnPrepare,
    number_params: FnNumberParams,
    desc_param: FnDescParam,
    bind_param2: FnBindParam2,
    exec: FnExec,
    row_count: FnRowCount,
    unbind_params: FnUnbindParams,
    get_diag_rec: FnGetDiagRec,
}

impl std::fmt::Debug for DpiLibrary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DpiLibrary")
    }
}

impl DpiLibrary {
    /// 加载并解析全部所需符号
    ///
    /// `path = None` 时按平台缺省名在动态库搜索路径里找（Linux: `libdmdpi.so`）。
    pub fn load(path: Option<&Path>) -> D2DResult<Arc<Self>> {
        let lib = match path {
            Some(p) => unsafe { Library::new(p) }.map_err(|e| {
                D2DError::config(format!(
                    "加载 DPI 动态库失败: {} ({})。请检查 properties.dpi.library\
                     （读侧 source.properties.dpi / 写侧 sink.properties.dpi），\
                     或把达梦客户端目录加入 LD_LIBRARY_PATH",
                    p.display(),
                    e
                ))
            })?,
            None => {
                let mut last = String::new();
                let mut loaded = None;
                for name in [
                    "libdmdpi.so",
                    "libdmdpi.so.1",
                    "dmdpi.dll",
                    "libdmdpi.dylib",
                ] {
                    match unsafe { Library::new(name) } {
                        Ok(l) => {
                            loaded = Some(l);
                            break;
                        }
                        Err(e) => last = format!("{}: {}", name, e),
                    }
                }
                loaded.ok_or_else(|| {
                    D2DError::config(format!(
                        "找不到 DPI 动态库（尝试过 libdmdpi.so / libdmdpi.so.1 / dmdpi.dll / \
                         libdmdpi.dylib，最后一次错误 {}）。请显式配置 \
                         source.properties.dpi.library",
                        last
                    ))
                })?
            }
        };

        macro_rules! sym {
            ($name:literal, $ty:ty) => {{
                let s: libloading::Symbol<$ty> = unsafe { lib.get($name) }.map_err(|e| {
                    D2DError::config(format!(
                        "DPI 动态库缺少符号 {}: {}（库版本过旧或不是达梦 DPI？）",
                        String::from_utf8_lossy(&$name[..$name.len() - 1]),
                        e
                    ))
                })?;
                *s
            }};
        }

        let this = Self {
            alloc_env: sym!(b"dpi_alloc_env\0", FnAllocEnv),
            alloc_con: sym!(b"dpi_alloc_con\0", FnAllocCon),
            alloc_stmt: sym!(b"dpi_alloc_stmt\0", FnAllocStmt),
            free_env: sym!(b"dpi_free_env\0", FnFreeEnv),
            free_con: sym!(b"dpi_free_con\0", FnFreeCon),
            free_stmt: sym!(b"dpi_free_stmt\0", FnFreeStmt),
            login: sym!(b"dpi_login\0", FnLogin),
            logout: sym!(b"dpi_logout\0", FnLogout),
            cancel: sym!(b"dpi_cancel\0", FnCancel),
            set_stmt_attr: sym!(b"dpi_set_stmt_attr\0", FnSetStmtAttr),
            exec_direct: sym!(b"dpi_exec_direct\0", FnExecDirect),
            number_columns: sym!(b"dpi_number_columns\0", FnNumberColumns),
            desc_column: sym!(b"dpi_desc_column\0", FnDescColumn),
            // bind_col2 多给一个「实际长度」数组 —— 截断检测必须是精确的，
            // 不能靠"缓冲里有没有 NUL"猜（值正好占满缓冲时两者无法区分）
            bind_col2: sym!(b"dpi_bind_col2\0", FnBindCol2),
            fetch: sym!(b"dpi_fetch\0", FnFetch),
            // 写方向的符号与读方向在同一份库里（spike 已在 .39 上逐个确认过）
            prepare: sym!(b"dpi_prepare\0", FnPrepare),
            number_params: sym!(b"dpi_number_params\0", FnNumberParams),
            desc_param: sym!(b"dpi_desc_param\0", FnDescParam),
            bind_param2: sym!(b"dpi_bind_param2\0", FnBindParam2),
            exec: sym!(b"dpi_exec\0", FnExec),
            row_count: sym!(b"dpi_row_count\0", FnRowCount),
            unbind_params: sym!(b"dpi_unbind_params\0", FnUnbindParams),
            get_diag_rec: sym!(b"dpi_get_diag_rec\0", FnGetDiagRec),
            _lib: lib,
        };
        Ok(Arc::new(this))
    }

    // ── 下面全是薄封装：把原始句柄的裸指针留在这一个文件里 ──

    pub fn alloc_env(&self) -> D2DResult<DpiHandle> {
        let mut h: DpiHandle = std::ptr::null_mut();
        let rc = unsafe { (self.alloc_env)(&mut h) };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_ENV, h, "dpi_alloc_env", rc));
        }
        Ok(h)
    }

    pub fn alloc_con(&self, env: DpiHandle) -> D2DResult<DpiHandle> {
        let mut h: DpiHandle = std::ptr::null_mut();
        let rc = unsafe { (self.alloc_con)(env, &mut h) };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_DBC, h, "dpi_alloc_con", rc));
        }
        Ok(h)
    }

    pub fn alloc_stmt(&self, con: DpiHandle) -> D2DResult<DpiHandle> {
        let mut h: DpiHandle = std::ptr::null_mut();
        let rc = unsafe { (self.alloc_stmt)(con, &mut h) };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_STMT, h, "dpi_alloc_stmt", rc));
        }
        Ok(h)
    }

    /// `dpi_login` —— 连接串是 `host:port`（或 dm_svc.conf 服务名）
    ///
    /// # 字符集行为（中文乱码的根源，读此段后再排障）
    ///
    /// DPI 缺省按**进程 locale** 推断客户端编码（中文 Windows=GBK、部分 Linux=GB18030），
    /// 而本引擎发出的字节永远是 UTF-8 —— 推断错了就是静默乱码。dmfldr 不受影响
    /// （控制文件显式 `CHARACTER_CODE=UTF-8`）。
    ///
    /// 显式声明的接口在当前驱动上**不可用**（2026-10-28 黑盒验证：`dpi_set_con_attr`/
    /// `dpi_set_env_attr` 设 `DSQL_ATTR_LOCAL_CODE` 一律返回 -1，与达梦社区公开帖一致；
    /// `dpi_login2`+`dpi_login_t.local_code` 是正路，但结构体布局需要 DPI.h）。所以：
    /// - 运行环境必须 `LANG=xx.UTF-8`（启动自检见 `ensure_utf8_locale`）；
    /// - 拿到 DPI.h 后改走 `dpi_login2` 显式声明（todo）。
    pub fn login(&self, con: DpiHandle, server: &str, user: &str, pwd: &str) -> D2DResult<()> {
        ensure_utf8_locale_logged();

        let svr = cstr(server, "DPI 服务器地址")?;
        let usr = cstr(user, "DPI 用户名")?;
        let p = cstr(pwd, "DPI 密码")?;
        // 部分目标的 `c_char` 是 u8（如 aarch64 Linux），统一 cast 到达梦的 sdbyte*
        let rc = unsafe {
            (self.login)(
                con,
                svr.as_ptr().cast(),
                usr.as_ptr().cast(),
                p.as_ptr().cast(),
            )
        };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_DBC, con, "dpi_login", rc));
        }
        Ok(())
    }

    pub fn logout(&self, con: DpiHandle) {
        unsafe {
            (self.logout)(con);
        }
    }

    pub fn free_stmt(&self, stmt: DpiHandle) {
        if !stmt.is_null() {
            unsafe {
                (self.free_stmt)(stmt);
            }
        }
    }

    pub fn free_con(&self, con: DpiHandle) {
        if !con.is_null() {
            unsafe {
                (self.free_con)(con);
            }
        }
    }

    pub fn free_env(&self, env: DpiHandle) {
        if !env.is_null() {
            unsafe {
                (self.free_env)(env);
            }
        }
    }

    pub fn cancel(&self, stmt: DpiHandle) {
        if !stmt.is_null() {
            unsafe {
                (self.cancel)(stmt);
            }
        }
    }

    /// 数组取数行数。
    ///
    /// 取值**按值转指针**、长度传 `sizeof(u64)` —— 这是 spike 里验证过的调用形态
    /// （达梦 Python 驱动也用同一种写法）。
    pub fn set_rowset_size(&self, stmt: DpiHandle, rows: usize) -> D2DResult<()> {
        let value = rows as u64;
        let rc = unsafe {
            (self.set_stmt_attr)(
                stmt,
                DSQL_ATTR_ROWSET_SIZE,
                value as usize as *mut c_void,
                std::mem::size_of::<u64>() as i32,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_STMT, stmt, "dpi_set_stmt_attr(ROWSET_SIZE)", rc));
        }
        Ok(())
    }

    pub fn exec_direct(&self, stmt: DpiHandle, sql: &str) -> D2DResult<()> {
        let s = cstr(sql, "DPI SQL")?;
        let rc = unsafe { (self.exec_direct)(stmt, s.as_ptr().cast()) };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_STMT, stmt, "dpi_exec_direct", rc));
        }
        Ok(())
    }

    pub fn number_columns(&self, stmt: DpiHandle) -> D2DResult<usize> {
        let mut n: i16 = 0;
        let rc = unsafe { (self.number_columns)(stmt, &mut n) };
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_STMT, stmt, "dpi_number_columns", rc));
        }
        Ok(n.max(0) as usize)
    }

    /// 描述一列：返回 `(name, sqltype, col_sz, scale, nullable)`
    pub fn desc_column(&self, stmt: DpiHandle, icol: usize) -> D2DResult<ColumnDesc> {
        let mut name = [0i8; 256];
        let mut name_len: i16 = 0;
        let mut sqltype: i16 = 0;
        let mut col_sz: u64 = 0;
        let mut scale: i16 = 0;
        let mut nullable: i16 = 0;
        let rc = unsafe {
            (self.desc_column)(
                stmt,
                icol as i16,
                name.as_mut_ptr(),
                name.len() as i16,
                &mut name_len,
                &mut sqltype,
                &mut col_sz,
                &mut scale,
                &mut nullable,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag(
                DSQL_HANDLE_STMT,
                stmt,
                &format!("dpi_desc_column({})", icol),
                rc,
            ));
        }
        let len = (name_len.max(0) as usize).min(name.len());
        let bytes: Vec<u8> = name[..len].iter().map(|&b| b as u8).collect();
        Ok(ColumnDesc {
            name: String::from_utf8_lossy(&bytes).to_string(),
            sqltype,
            size: col_sz,
            scale,
            nullable: nullable != 0,
        })
    }

    /// 绑定一列到调用方给的缓冲区（`DSQL_C_NCHAR`，NUL 结尾）
    ///
    /// `ind` 给出 NULL（-1）；`act` 给出实际长度（字节，不含结尾 NUL）。
    pub fn bind_col2(
        &self,
        stmt: DpiHandle,
        icol: usize,
        val: *mut c_void,
        buf_len: usize,
        ind: *mut i64,
        act: *mut i64,
    ) -> D2DResult<()> {
        let rc = unsafe {
            (self.bind_col2)(
                stmt,
                icol as u16,
                DSQL_C_NCHAR,
                val,
                buf_len as i64,
                ind,
                act,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag(
                DSQL_HANDLE_STMT,
                stmt,
                &format!("dpi_bind_col2({})", icol),
                rc,
            ));
        }
        Ok(())
    }

    /// 取一批；返回本批行数（`DSQL_NO_DATA` → 0）
    pub fn fetch(&self, stmt: DpiHandle) -> D2DResult<usize> {
        let mut rows: u64 = 0;
        let rc = unsafe { (self.fetch)(stmt, &mut rows) };
        if rc == DSQL_NO_DATA {
            return Ok(0);
        }
        if !succeeded(rc) {
            return Err(self.diag(DSQL_HANDLE_STMT, stmt, "dpi_fetch", rc));
        }
        Ok(rows as usize)
    }

    /// 取诊断信息并组装成**读方向**的 `D2DError`
    pub fn diag(&self, htype: i16, handle: DpiHandle, what: &str, rc: DpiReturn) -> D2DError {
        let detail = self.diag_message(htype, handle, what, rc);
        match htype {
            DSQL_HANDLE_DBC | DSQL_HANDLE_ENV => D2DError::connection("dameng/dpi", detail),
            _ => D2DError::source_read("dameng/dpi", detail),
        }
    }

    /// 取诊断信息并组装成**写方向**的 `D2DError`
    ///
    /// 与 [`Self::diag`] 只差错误类别：写失败不该挂成 `source_read`。
    pub fn diag_write(&self, htype: i16, handle: DpiHandle, what: &str, rc: DpiReturn) -> D2DError {
        let detail = self.diag_message(htype, handle, what, rc);
        match htype {
            DSQL_HANDLE_DBC | DSQL_HANDLE_ENV => D2DError::connection("dameng/dpi", detail),
            _ => D2DError::external("dameng/dpi", detail),
        }
    }

    fn diag_message(&self, htype: i16, handle: DpiHandle, what: &str, rc: DpiReturn) -> String {
        let mut code: i32 = 0;
        let mut msg = [0i8; 1024];
        let mut len: i16 = 0;
        let got = unsafe {
            (self.get_diag_rec)(
                htype,
                handle,
                1,
                &mut code,
                msg.as_mut_ptr(),
                msg.len() as i16,
                &mut len,
            )
        };
        let text = if succeeded(got) {
            let n = (len.max(0) as usize).min(msg.len());
            let bytes: Vec<u8> = msg[..n].iter().map(|&b| b as u8).collect();
            String::from_utf8_lossy(&bytes).to_string()
        } else {
            "（驱动未返回诊断信息）".to_string()
        };
        format!("{} 失败（rc={}, code={}）：{}", what, rc, code, text)
    }

    // ── 写方向：prepare → desc_param → bind_param2(数组) → exec ──

    /// `dpi_prepare`：绑定参数前必须先准备语句
    pub fn prepare(&self, stmt: DpiHandle, sql: &str) -> D2DResult<()> {
        let s = cstr(sql, "DPI SQL")?;
        let rc = unsafe { (self.prepare)(stmt, s.as_ptr().cast()) };
        if !succeeded(rc) {
            return Err(self.diag_write(DSQL_HANDLE_STMT, stmt, "dpi_prepare", rc));
        }
        Ok(())
    }

    /// `dpi_number_params`：`INSERT ... VALUES (?,?)` 的占位符个数
    pub fn number_params(&self, stmt: DpiHandle) -> D2DResult<usize> {
        let mut n: i16 = 0;
        let rc = unsafe { (self.number_params)(stmt, &mut n) };
        if !succeeded(rc) {
            return Err(self.diag_write(DSQL_HANDLE_STMT, stmt, "dpi_number_params", rc));
        }
        Ok(n.max(0) as usize)
    }

    /// `dpi_desc_param`：目标列的类型/精度/标度 —— 绑定时原样回传，驱动负责转换
    pub fn desc_param(&self, stmt: DpiHandle, iparam: usize) -> D2DResult<ParamDesc> {
        let mut sql_type: i16 = 0;
        let mut precision: u64 = 0;
        let mut scale: i16 = 0;
        let mut nullable: i16 = 0;
        let rc = unsafe {
            (self.desc_param)(
                stmt,
                iparam as u16,
                &mut sql_type,
                &mut precision,
                &mut scale,
                &mut nullable,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag_write(
                DSQL_HANDLE_STMT,
                stmt,
                &format!("dpi_desc_param({})", iparam),
                rc,
            ));
        }
        Ok(ParamDesc {
            sql_type,
            precision,
            scale,
            nullable: nullable != 0,
        })
    }

    /// 绑定一列**参数数组**
    ///
    /// `buf` 是 `rows × buf_len` 的连续缓冲（第 r 行在 `buf + r*buf_len`）；
    /// `ind` 每行给长度（字节）或 [`DSQL_NULL_DATA`]；`act` 同行放实际长度。
    /// 这是 dmPython `executemany` 用的同一形态（列式数组绑定）。
    #[allow(clippy::too_many_arguments)]
    pub fn bind_param2(
        &self,
        stmt: DpiHandle,
        iparam: usize,
        ctype: i16,
        desc: &ParamDesc,
        buf: *mut c_void,
        buf_len: usize,
        ind: *mut i64,
        act: *mut i64,
    ) -> D2DResult<()> {
        let rc = unsafe {
            (self.bind_param2)(
                stmt,
                iparam as u16,
                DSQL_PARAM_INPUT,
                ctype,
                desc.sql_type,
                desc.precision,
                desc.scale,
                buf,
                buf_len as i64,
                ind,
                act,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag_write(
                DSQL_HANDLE_STMT,
                stmt,
                &format!("dpi_bind_param2({})", iparam),
                rc,
            ));
        }
        Ok(())
    }

    /// 数组 DML 的行数（`DSQL_ATTR_PARAMSET_SIZE`）
    ///
    /// 调用形态与 dmPython 一致：**值按指针传**、`val_len = 0`。
    /// （`ROWSET_SIZE` 用的是"指针指向值 + 长度 8"，两种形态都实测可用，
    /// 但参数集大小这条以官方 Python 驱动为准。）
    pub fn set_paramset_size(&self, stmt: DpiHandle, rows: usize) -> D2DResult<()> {
        let rc =
            unsafe { (self.set_stmt_attr)(stmt, DSQL_ATTR_PARAMSET_SIZE, rows as *mut c_void, 0) };
        if !succeeded(rc) {
            return Err(self.diag_write(
                DSQL_HANDLE_STMT,
                stmt,
                "dpi_set_stmt_attr(PARAMSET_SIZE)",
                rc,
            ));
        }
        Ok(())
    }

    /// 每行状态数组（`DSQL_ATTR_PARAM_STATUS_PTR`）
    ///
    /// 数组 DML 可能「整体 rc 成功、个别行失败」，状态数组是唯一精确的逐行证据。
    pub fn set_param_status_ptr(&self, stmt: DpiHandle, status: *mut u16) -> D2DResult<()> {
        let rc = unsafe {
            (self.set_stmt_attr)(stmt, DSQL_ATTR_PARAM_STATUS_PTR, status as *mut c_void, 0)
        };
        if !succeeded(rc) {
            return Err(self.diag_write(
                DSQL_HANDLE_STMT,
                stmt,
                "dpi_set_stmt_attr(PARAM_STATUS_PTR)",
                rc,
            ));
        }
        Ok(())
    }

    /// 驱动实际处理的行数输出位置（`DSQL_ATTR_PARAMS_PROCESSED_PTR`）
    pub fn set_params_processed_ptr(&self, stmt: DpiHandle, processed: *mut u64) -> D2DResult<()> {
        let rc = unsafe {
            (self.set_stmt_attr)(
                stmt,
                DSQL_ATTR_PARAMS_PROCESSED_PTR,
                processed as *mut c_void,
                0,
            )
        };
        if !succeeded(rc) {
            return Err(self.diag_write(
                DSQL_HANDLE_STMT,
                stmt,
                "dpi_set_stmt_attr(PARAMS_PROCESSED_PTR)",
                rc,
            ));
        }
        Ok(())
    }

    /// 执行已绑定的参数集；返回原始 rc（用于区分 SUCCESS / SUCCESS_WITH_INFO）
    pub fn exec(&self, stmt: DpiHandle) -> D2DResult<DpiReturn> {
        let rc = unsafe { (self.exec)(stmt) };
        if !succeeded(rc) {
            return Err(self.diag_write(DSQL_HANDLE_STMT, stmt, "dpi_exec", rc));
        }
        Ok(rc)
    }

    /// `dpi_row_count`：目标端确认影响的行数；[`DSQL_ROW_COUNT_UNKNOWN`] = 拿不到
    pub fn row_count(&self, stmt: DpiHandle) -> D2DResult<i64> {
        let mut n: i64 = 0;
        let rc = unsafe { (self.row_count)(stmt, &mut n) };
        if !succeeded(rc) {
            return Err(self.diag_write(DSQL_HANDLE_STMT, stmt, "dpi_row_count", rc));
        }
        Ok(n)
    }

    /// 解绑参数；下一块要换缓冲，必须先解绑
    pub fn unbind_params(&self, stmt: DpiHandle) -> D2DResult<()> {
        let rc = unsafe { (self.unbind_params)(stmt) };
        if !succeeded(rc) {
            return Err(self.diag_write(DSQL_HANDLE_STMT, stmt, "dpi_unbind_params", rc));
        }
        Ok(())
    }
}

/// `dpi_desc_param` 的结果
#[derive(Debug, Clone, Copy)]
pub struct ParamDesc {
    /// 服务端类型码（绑定时作为 `dtype` 原样回传）
    pub sql_type: i16,
    pub precision: u64,
    pub scale: i16,
    pub nullable: bool,
}

/// `dpi_desc_column` 的结果
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    /// 达梦服务端类型码（`DSQL_CHAR=1`、`DSQL_VARCHAR=2`、`DSQL_DEC=9` …）
    pub sqltype: i16,
    /// 列声明长度
    pub size: u64,
    pub scale: i16,
    pub nullable: bool,
}

/// UTF-8 locale 自检（每进程只警告一次）：DPI 按**进程 locale** 推断客户端编码，
/// locale 不是 UTF-8 时，引擎发出的 UTF-8 字节会被驱动按错误编码解释/转码 ——
/// 中文静默乱码（dmfldr 不受影响，它的控制文件显式声明 CHARACTER_CODE）。
/// 显式声明接口（DSQL_ATTR_LOCAL_CODE / dpi_login2）在当前驱动上不可用，见
/// [`DpiLibrary::login`] 的字符集说明；所以这里只能把环境问题**提前喊出来**。
fn ensure_utf8_locale_logged() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // LANG 优先、LC_ALL 兜底；显式空串视为未设置（继续看下一个）
        let lang = ["LANG", "LC_ALL"]
            .iter()
            .find_map(|k| std::env::var(k).ok().filter(|v| !v.is_empty()))
            .unwrap_or_default();
        let ok = lang.is_empty()
            || lang.to_ascii_lowercase().contains("utf-8")
            || lang.to_ascii_lowercase().contains("utf8");
        if !ok {
            tracing::warn!(
                lang = %lang,
                "dpi: 运行环境 LANG/LC_ALL 不是 UTF-8 —— DPI 会按该 locale 解释客户端字节，\
                 中文很可能乱码（dmfldr 正常、dpi 乱即此症状）。\
                 处理：export LANG=zh_CN.UTF-8 后重跑；根治需 DPI 显式声明编码（待达梦 DPI.h）"
            );
        }
    });
}

fn cstr(s: &str, what: &str) -> D2DResult<CString> {
    CString::new(s).map_err(|_| D2DError::config(format!("{} 含 NUL 字节，无法传给 DPI", what)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_succeeded_matches_header_macro() {
        assert!(succeeded(0));
        assert!(succeeded(1));
        assert!(!succeeded(-1));
        assert!(!succeeded(2));
        // 头文件：DSQL_SUCCESS=0, DSQL_SUCCESS_WITH_INFO=1
    }

    #[test]
    fn test_cstr_rejects_nul() {
        assert!(cstr("a\0b", "x").is_err());
        assert!(cstr("abc", "x").is_ok());
    }
}
