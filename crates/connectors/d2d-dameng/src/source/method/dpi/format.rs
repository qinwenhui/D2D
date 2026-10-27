//! 直通面：DPI 取到的行 → 目标可直接吃的分隔文本块
//!
//! # 方言由 `DpiParams` 声明
//!
//! - **Doris（缺省）**：`|` 分隔、NULL → `\N`（Stream Load 的 null_value 口径）
//! - **qmdb**：`,` 分隔、NULL → 裸空字段（协议 §2.1：空字段即 NULL、无字面量机制）——
//!   `source.properties.dpi` 里配 `column_separator: ","`、`null_literal: ""`
//!
//! # NULL 的表示（Doris 档）
//!
//! **NULL → `\N`，空串保持空字段**。这是 DPI 相对 dmfldr 的核心正确性收益：
//! dmfldr 的 SQL 模式把 NULL 导出成空字段，Doris 会存成空字符串
//! （见 `physical-direct-null-unsafe`）。
//!
//! # 值内受限字节（按方言推导）
//!
//! 结构字节 = 分隔符、CR、LF、NUL —— 出现任何一个，文本结构就无法表达该值，
//! 一并大声失败（逻辑面没有这个限制）。
//! 另外 NULL 字面量为 `\N` 时 `\` 也是受限字节：`\N` 以反斜杠开头，值里再出现
//! 别的反斜杠就无法与 NULL 字面量区分（本模块逐字段检查）。NULL 为裸空字段时
//! 反斜杠不再受限。

use d2d_connector::PhysicalBlock;
use d2d_core::{D2DError, D2DResult};

/// Doris 缺省方言的空值字面量（`null_value` 缺省值，会话里显式声明的是同一个常量）
pub const NULL_LITERAL: &[u8] = b"\\N";
pub const COLUMN_SEPARATOR: u8 = b'|';
pub const LINE_DELIMITER: u8 = b'\n';

/// 受限字节集上限：sep、LF、CR、NUL +（`\N` 档的）反斜杠
const MAX_FORBIDDEN: usize = 5;

/// 值里出现即失败的结构字节（SIMD 扫描）
///
/// 分两遍扫（memchr 的 memchr2/memchr3），而不是逐字节 `iter().find`：
/// 直通面每行都要过一遍，30M 行 × ~175 B ≈ 5 GB —— 逐字节扫描实测要多吃几秒。
#[inline]
fn forbidden_in(v: &[u8], set: &[u8]) -> Option<u8> {
    debug_assert!(set.len() <= MAX_FORBIDDEN);
    match set.len() {
        0 => None,
        1 => memchr::memchr(set[0], v).map(|i| v[i]),
        2 => memchr::memchr2(set[0], set[1], v).map(|i| v[i]),
        3 => memchr::memchr3(set[0], set[1], set[2], v).map(|i| v[i]),
        // 4 或 5 字节：两遍扫（3+1 / 3+2）。**4 字节档是 qmdb 方言的常态路径**
        // （sep/LF/CR/NUL，裸空字段 NULL 不受限 `\`）——别当 5 字节处理
        4 => match (
            memchr::memchr3(set[0], set[1], set[2], v),
            memchr::memchr(set[3], v),
        ) {
            (Some(i), Some(j)) => Some(v[i.min(j)]),
            (Some(i), None) => Some(v[i]),
            (None, Some(j)) => Some(v[j]),
            (None, None) => None,
        },
        _ => match (
            memchr::memchr3(set[0], set[1], set[2], v),
            memchr::memchr2(set[3], set[4], v),
        ) {
            (Some(i), Some(j)) => Some(v[i.min(j)]),
            (Some(i), None) => Some(v[i]),
            (None, Some(j)) => Some(v[j]),
            (None, None) => None,
        },
    }
}

/// 一行的块缓冲：攒够目标字节数就交出一块
pub struct RowBlockWriter {
    buf: Vec<u8>,
    block_bytes: usize,
    first_bytes: usize,
    first_sent: bool,
    rows: u64,
    row_no: u64,
    cells: usize,
    expected_cells: usize,
    /// 字段分隔符（方言）
    separator: u8,
    /// NULL 字面量（空 = NULL 输出裸空字段）
    null_literal: Vec<u8>,
    /// 值里出现即失败的字节集（按方言推导，见模块注释）
    forbidden: [u8; MAX_FORBIDDEN],
    forbidden_len: usize,
}

impl RowBlockWriter {
    /// Doris 缺省方言（`|` 分隔 + `\N` NULL）—— 现有 dm→doris 直通链路的行为
    pub fn new(expected_cells: usize, block_bytes: usize, first_bytes: usize) -> Self {
        Self::with_dialect(expected_cells, block_bytes, first_bytes, b'|', NULL_LITERAL)
    }

    /// 自定义方言：`separator` 字段分隔符；`null_literal` 空切片 = NULL 输出裸空字段
    ///
    /// 受限集 = {separator, LF, CR, NUL}；NULL 字面量为 `\N` 时再加 `\`
    /// （否则 `\N` 与数据无法区分）。裸空字段档不加 —— 目标端不再有 `\N` 要区分。
    pub fn with_dialect(
        expected_cells: usize,
        block_bytes: usize,
        first_bytes: usize,
        separator: u8,
        null_literal: &[u8],
    ) -> Self {
        let first = if first_bytes == 0 {
            block_bytes
        } else {
            first_bytes
        };
        let forbidden = [separator, b'\n', b'\r', 0, b'\\'];
        let forbidden_len = if null_literal == NULL_LITERAL { 5 } else { 4 };
        Self {
            // 直接按稳态块大小预分配：只增一次，避免逐个翻倍到 128MB
            buf: Vec::with_capacity(block_bytes.max(first)),
            block_bytes: block_bytes.max(1),
            first_bytes: first.max(1),
            first_sent: false,
            rows: 0,
            row_no: 0,
            cells: 0,
            expected_cells,
            separator,
            null_literal: null_literal.to_vec(),
            forbidden,
            forbidden_len,
        }
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// 开始一行（`row_no` 从 1 开始，只用于报错定位）
    pub fn begin_row(&mut self, row_no: u64) {
        self.row_no = row_no;
        self.cells = 0;
    }

    /// 追加一个字段；`None` = SQL NULL
    pub fn push_cell(&mut self, value: Option<&[u8]>, column: &str) -> D2DResult<()> {
        if self.cells > 0 {
            self.buf.push(self.separator);
        }
        match value {
            None => self.buf.extend_from_slice(&self.null_literal),
            Some(v) => {
                if let Some(bad) = forbidden_in(v, &self.forbidden[..self.forbidden_len]) {
                    return Err(D2DError::external(
                        "direct_path",
                        format!(
                            "dpi 直通：列 '{}' 第 {} 行含受限字节 {:?}。\
                             分隔符/换行/NUL 会破坏文本结构，`\\` 会与 NULL 字面量 `\\N` 混淆。\
                             处理方式：① 给 flow 加任意 transform 算子即可自动回落逻辑面；\
                             ② 或在源端清洗该列",
                            column, self.row_no, bad as char
                        ),
                    ));
                }
                self.buf.extend_from_slice(v);
            }
        }
        self.cells += 1;
        Ok(())
    }

    /// 结束一行：校验列数、写行分隔符、行数 +1
    pub fn end_row(&mut self) -> D2DResult<()> {
        if self.cells != self.expected_cells {
            return Err(D2DError::schema(format!(
                "dpi 直通：第 {} 行有 {} 个字段，契约声明 {} 列",
                self.row_no, self.cells, self.expected_cells
            )));
        }
        self.buf.push(LINE_DELIMITER);
        self.rows += 1;
        Ok(())
    }

    /// 当前块是否已经攒够（首块 / 稳态块各自的目标）
    pub fn is_ready(&self) -> bool {
        let target = if self.first_sent {
            self.block_bytes
        } else {
            self.first_bytes
        };
        self.buf.len() >= target
    }

    /// 交出一块（行对齐）；空块返回 `None`
    pub fn take_block(&mut self) -> Option<PhysicalBlock> {
        if self.buf.is_empty() {
            return None;
        }
        let block = PhysicalBlock {
            bytes: std::mem::take(&mut self.buf),
            rows: self.rows,
        };
        // 下一块用稳态大小预分配
        self.buf = Vec::with_capacity(self.block_bytes);
        self.first_sent = true;
        self.rows = 0;
        Some(block)
    }

    /// 收尾：交出最后一块（可能不足目标大小）
    pub fn finish(mut self) -> Option<PhysicalBlock> {
        self.take_block()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer() -> RowBlockWriter {
        RowBlockWriter::new(2, 1024, 16)
    }

    #[test]
    fn test_null_is_literal_and_empty_is_empty() {
        let mut w = writer();
        w.begin_row(1);
        w.push_cell(None, "a").unwrap();
        w.push_cell(Some(b""), "b").unwrap();
        w.end_row().unwrap();
        let b = w.finish().unwrap();
        assert_eq!(b.bytes, b"\\N|\n");
        assert_eq!(b.rows, 1);
    }

    #[test]
    fn test_rows_are_pipe_separated_and_newline_terminated() {
        let mut w = writer();
        w.begin_row(1);
        w.push_cell(Some(b"x"), "a").unwrap();
        w.push_cell(Some(b"y"), "b").unwrap();
        w.end_row().unwrap();
        w.begin_row(2);
        w.push_cell(Some(b"z"), "a").unwrap();
        w.push_cell(Some(b"w"), "b").unwrap();
        w.end_row().unwrap();
        let b = w.finish().unwrap();
        assert_eq!(b.bytes, b"x|y\nz|w\n");
        assert_eq!(b.rows, 2);
    }

    #[test]
    fn test_forbidden_bytes_fail_loudly() {
        for bad in [b"a|b", b"a\nb", b"a\rb", b"a\0b", b"a\\b"] {
            let mut w = writer();
            w.begin_row(7);
            let err = w.push_cell(Some(bad), "c").unwrap_err().to_string();
            assert!(err.contains("受限字节"), "{}", err);
            assert!(err.contains("第 7 行"), "{}", err);
        }
    }

    /// **qmdb 方言（4 字节受限集）**：回归 2026-10-27 基准事故 —— 4 字节 set 走了
    /// 5 字节的扫法，`set[4]` 越界 panic 被 tokio 吞掉，整任务静默变 0 行"成功"。
    /// NULL → 空字段、反斜杠不受限、分隔符 `,` 与换行/NUL 受限，都得走一遍。
    #[test]
    fn test_qmdb_dialect_writer() {
        let mut w = RowBlockWriter::with_dialect(3, 1024, 16, b',', b"");
        w.begin_row(1);
        w.push_cell(Some(b"back\\slash"), "a").unwrap(); // `\` 不再受限
        w.push_cell(None, "b").unwrap(); // NULL → 裸空字段
        w.push_cell(Some(b"x"), "c").unwrap();
        w.end_row().unwrap();
        let b = w.finish().unwrap();
        assert_eq!(b.bytes, b"back\\slash,,x\n");
        assert_eq!(b.rows, 1);

        for bad in [b"a,b", b"a\nb", b"a\rb", b"a\0b"] {
            let mut w = RowBlockWriter::with_dialect(1, 1024, 16, b',', b"");
            w.begin_row(7);
            let err = w.push_cell(Some(bad), "c").unwrap_err().to_string();
            assert!(err.contains("受限字节"), "{:?} -> {}", bad, err);
        }
    }

    #[test]
    fn test_cell_count_mismatch_fails() {
        let mut w = writer();
        w.begin_row(1);
        w.push_cell(Some(b"x"), "a").unwrap();
        let err = w.end_row().unwrap_err().to_string();
        assert!(err.contains("契约声明 2 列"), "{}", err);
    }

    #[test]
    fn test_block_boundary_is_row_aligned() {
        // 每行 4 字节（a|b\n），块目标 2 → 每行结束后都 >= 目标，但只按整行切
        let mut w = RowBlockWriter::new(2, 4, 4);
        let mut blocks = Vec::new();
        for i in 0..3 {
            w.begin_row(i + 1);
            w.push_cell(Some(b"a"), "a").unwrap();
            w.push_cell(Some(b"b"), "b").unwrap();
            w.end_row().unwrap();
            if w.is_ready() {
                blocks.push(w.take_block().unwrap());
            }
        }
        blocks.extend(w.finish());
        assert_eq!(blocks.len(), 3, "每行都攒够一块 → 三块");
        for b in blocks {
            assert_eq!(b.bytes, b"a|b\n");
            assert_eq!(b.rows, 1, "块边界必须与行边界对齐");
        }
    }
}
