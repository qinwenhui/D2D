//! 一条 DPI 会话：连接 → 执行 → 描述 → 绑定 → 数组取数
//!
//! # 线程约束
//!
//! 整个会话（env/con/stmt 三个裸句柄 + 绑定缓冲）**只在创建它的那个
//! `spawn_blocking` 线程里使用**，不实现 `Send`/`Sync`，也不跨 await 持有。
//! 这是刻意的最小暴露面：DPI 句柄的线程亲和性没有文档保证，不赌。

use crate::dpi::ffi::{DpiHandle, DpiLibrary, DSQL_NULL_DATA};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;

/// 连接参数（由 `DpiMethod` 从 `ConnectionConfig` + `DpiParams` 固化）
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// `host:port` 或 dm_svc.conf 服务名
    pub server: String,
    pub user: String,
    pub password: String,
    pub fetch_rows: usize,
    /// 字符串列缓冲下限（字节）
    pub column_bytes: usize,
}

/// 一列的运行期元数据
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub sqltype: i16,
    /// 绑定缓冲宽度（含 NUL 结尾）
    pub width: usize,
    /// 在绑定大缓冲里的起始偏移
    offset: usize,
}

/// DPI 会话
pub struct DpiSession {
    lib: Arc<DpiLibrary>,
    fetch_rows: usize,
    env: DpiHandle,
    con: DpiHandle,
    stmt: DpiHandle,
    logged_in: bool,
    columns: Vec<ColumnMeta>,
    /// `ncols × fetch_rows × width` 的列式缓冲
    data: Vec<u8>,
    /// `ncols × fetch_rows` 的指示器（NULL = -1）
    ind: Vec<i64>,
    /// `ncols × fetch_rows` 的实际长度（不含 NUL）；`dpi_bind_col2` 填
    act: Vec<i64>,
    total_bytes: usize,
}

impl DpiSession {
    /// 建立连接并分配语句句柄（不执行 SQL）
    pub fn connect(lib: Arc<DpiLibrary>, cfg: &SessionConfig) -> D2DResult<Self> {
        let env = lib.alloc_env()?;
        let con = match lib.alloc_con(env) {
            Ok(c) => c,
            Err(e) => {
                lib.free_env(env);
                return Err(e);
            }
        };
        if let Err(e) = lib.login(con, &cfg.server, &cfg.user, &cfg.password) {
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
            lib,
            fetch_rows: cfg.fetch_rows.max(1),
            env,
            con,
            stmt,
            logged_in: true,
            columns: Vec::new(),
            data: Vec::new(),
            ind: Vec::new(),
            act: Vec::new(),
            total_bytes: 0,
        })
    }

    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    /// 本会话绑定缓冲的字节数（启动期账本 / 日志用）
    pub fn buffer_bytes(&self) -> usize {
        self.total_bytes
    }

    /// 执行 SQL、描述列、分配并绑定数组缓冲
    ///
    /// 列序与 `expected` 不一致直接失败：SELECT 列表来自契约，列序错了就是
    /// **整批数据逐列错位**，不是"慢一点"。
    pub fn execute(
        &mut self,
        sql: &str,
        expected: &[String],
        column_bytes: usize,
    ) -> D2DResult<()> {
        self.lib.set_rowset_size(self.stmt, self.fetch_rows)?;
        self.lib.exec_direct(self.stmt, sql)?;

        let ncols = self.lib.number_columns(self.stmt)?;
        if ncols == 0 {
            return Err(D2DError::schema(format!(
                "dpi: SQL 没有返回任何列（split 的 SELECT 列表为空？）: {}",
                sql
            )));
        }
        if ncols != expected.len() {
            // 描述一遍列名，错误里带上实际列，便于定位
            let got: Vec<String> = (1..=ncols)
                .filter_map(|i| self.lib.desc_column(self.stmt, i).ok().map(|d| d.name))
                .collect();
            return Err(D2DError::schema(format!(
                "dpi 列数不一致：SQL 返回 {} 列，type_contract 声明 {} 列。实际列={:?}，契约={:?}",
                ncols,
                expected.len(),
                got,
                expected
            )));
        }

        let mut columns = Vec::with_capacity(ncols);
        let mut total = 0usize;
        for i in 1..=ncols {
            let d = self.lib.desc_column(self.stmt, i)?;
            if d.name != expected[i - 1] {
                return Err(D2DError::schema(format!(
                    "dpi 第 {} 列名不一致：SQL/库返回 '{}'，type_contract 声明 '{}'。\
                     SELECT 列表按契约生成，出现差异说明库端列名的大小写/别名与契约不符",
                    i,
                    d.name,
                    expected[i - 1]
                )));
            }
            let width = column_width(&d, column_bytes)?;
            columns.push(ColumnMeta {
                name: d.name,
                sqltype: d.sqltype,
                width,
                offset: total,
            });
            total += width * self.fetch_rows;
        }

        let mut data = vec![0u8; total];
        let mut ind = vec![0i64; ncols * self.fetch_rows];
        let mut act = vec![0i64; ncols * self.fetch_rows];
        for (i, c) in columns.iter().enumerate() {
            let col_off = c.offset;
            let ind_off = i * self.fetch_rows;
            self.lib.bind_col2(
                self.stmt,
                i + 1,
                unsafe { data.as_mut_ptr().add(col_off) as *mut std::os::raw::c_void },
                c.width,
                unsafe { ind.as_mut_ptr().add(ind_off) },
                unsafe { act.as_mut_ptr().add(ind_off) },
            )?;
        }

        self.total_bytes = total + (ind.len() + act.len()) * std::mem::size_of::<i64>();
        self.columns = columns;
        self.data = data;
        self.ind = ind;
        self.act = act;
        tracing::debug!(
            columns = ncols,
            fetch_rows = self.fetch_rows,
            buffer_bytes = self.total_bytes,
            "dpi: 列描述/数组绑定完成"
        );
        Ok(())
    }

    /// 取一批；返回行数（0 = 没有更多数据）
    pub fn fetch(&mut self) -> D2DResult<usize> {
        let n = self.lib.fetch(self.stmt)?;
        if n > self.fetch_rows {
            return Err(D2DError::internal(format!(
                "dpi_fetch 报告 {} 行，超过绑定的数组长度 {}",
                n, self.fetch_rows
            )));
        }
        Ok(n)
    }

    /// 取第 `col` 列第 `row` 个值；`None` = SQL NULL
    ///
    /// 长度来自 `dpi_bind_col2` 的 actual_length 数组 —— **不靠找 NUL 猜**：
    /// 值正好占满缓冲时，"截断"和"刚好放得下"在字节层面无法区分。
    /// `act >= width` 即截断（还要留 NUL 的位置），大声失败，不返回半截值。
    pub fn cell(&self, col: usize, row: usize) -> D2DResult<Option<&[u8]>> {
        let c = &self.columns[col];
        if row >= self.fetch_rows {
            return Err(D2DError::internal(format!(
                "dpi 取值越界：row={}, fetch_rows={}",
                row, self.fetch_rows
            )));
        }
        let idx = col * self.fetch_rows + row;
        if self.ind[idx] == DSQL_NULL_DATA {
            return Ok(None);
        }
        let act = self.act[idx];
        if act < 0 || act as usize >= c.width {
            return Err(D2DError::config(format!(
                "dpi 列 '{}' 第 {} 行的值实际 {} 字节，绑定缓冲只有 {} 字节（已截断或驱动异常）。\
                 请调大 source.properties.dpi.column_bytes",
                c.name,
                row + 1,
                act,
                c.width
            )));
        }
        let off = c.offset + row * c.width;
        Ok(Some(&self.data[off..off + act as usize]))
    }
}

impl Drop for DpiSession {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            // 下游断开时查询可能还在跑，先取消再释放句柄
            self.lib.cancel(self.stmt);
            self.lib.free_stmt(self.stmt);
            self.stmt = std::ptr::null_mut();
        }
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

/// 单列绑定宽度
///
/// 文本列按 **UTF-8 最坏 4 字节/字符** 放大，并至少取 `column_bytes`；
/// 其余类型按服务端声明长度估一个上界。LOB / 二进制列 MVP 明确拒绝 ——
/// `DSQL_C_NCHAR` 承载不了任意二进制（里面的 NUL 会被当成字符串结尾）。
fn column_width(d: &crate::dpi::ffi::ColumnDesc, column_bytes: usize) -> D2DResult<usize> {
    let floor = column_bytes.max(64);
    let w = match d.sqltype {
        // CHAR / VARCHAR
        1 | 2 => {
            let by_len = (d.size as usize).saturating_mul(4).saturating_add(1);
            by_len.max(floor + 1)
        }
        // BLOB / CLOB / BFILE / BINARY / VARBINARY
        12 | 19 | 1000 | 17 | 18 => {
            return Err(D2DError::not_supported(format!(
                "dpi 方法 MVP 不支持 LOB / 二进制列 '{}'（类型码 {}）。\
                 请改用 method: odbc，或在源端用十六进制字符串列承载",
                d.name, d.sqltype
            )));
        }
        // DECIMAL / NUMERIC
        9 => ((d.size as usize).saturating_add(4)).max(32),
        // FLOAT / DOUBLE
        10 | 11 => 64,
        // BIT
        3 => ((d.size as usize).saturating_mul(2)).max(64),
        // DATE / TIME
        14 | 15 => 32,
        // TIMESTAMP / TIME_TZ / TIMESTAMP_TZ
        16 | 22 | 23 => 64,
        // TINYINT / SMALLINT / INT / BIGINT
        5..=8 => 32,
        // INTERVAL_*
        100..=112 => 64,
        // 未知类型：保守放大（宁可多占，不可截断）
        _ => {
            let by_len = (d.size as usize).saturating_mul(4).saturating_add(1);
            by_len.max(floor + 1).min(1 << 20)
        }
    };
    Ok(w.max(16))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dpi::ffi::ColumnDesc;

    fn desc(sqltype: i16, size: u64) -> ColumnDesc {
        ColumnDesc {
            name: "c".into(),
            sqltype,
            size,
            scale: 0,
            nullable: true,
        }
    }

    #[test]
    fn test_text_width_covers_multibyte() {
        // VARCHAR(64)：64 个 4 字节字符 + NUL
        assert_eq!(column_width(&desc(2, 64), 256).unwrap(), 257);
        // 下限生效
        assert_eq!(column_width(&desc(2, 10), 512).unwrap(), 513);
    }

    #[test]
    fn test_numeric_width_is_small() {
        assert_eq!(column_width(&desc(9, 30), 256).unwrap(), 34);
        assert_eq!(column_width(&desc(8, 8), 256).unwrap(), 32);
    }

    #[test]
    fn test_lob_and_binary_are_rejected() {
        for t in [12, 19, 1000, 17, 18] {
            let err = column_width(&desc(t, 100), 256).unwrap_err().to_string();
            assert!(err.contains("不支持"), "type {}: {}", t, err);
        }
    }
}
