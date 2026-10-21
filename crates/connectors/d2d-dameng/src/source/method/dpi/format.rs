//! 直通面：DPI 取到的行 → 目标可直接吃的 `|` 分隔文本块
//!
//! # NULL 的表示
//!
//! **NULL → `\N`，空串保持空字段**。这是 DPI 相对 dmfldr 的核心正确性收益：
//! dmfldr 的 SQL 模式把 NULL 导出成空字段，Doris 会存成空字符串
//! （见 `physical-direct-null-unsafe`）。
//!
//! # 为什么值里不能出现反斜杠
//!
//! `\N` 本身以反斜杠开头，而 Doris 的 CSV 读入器把反斜杠当转义引导符。
//! 目标端因此仍把 `\` 列为受限字节；源端声明 `null_literal = "\N"` 的同时
//! 必须保证**值里不再有别的反斜杠**，否则 `\N` 与数据无法区分。
//! 本模块逐字段检查，命中即失败（逻辑面没有这个限制）。
//!
//! 同理，值里出现 `|`、CR、LF、NUL 也会让文本结构无法表达 —— 一并大声失败。

use d2d_connector::PhysicalBlock;
use d2d_core::{D2DError, D2DResult};

/// Doris 的空值字面量（`null_value` 缺省值，会话里显式声明的是同一个常量）
pub const NULL_LITERAL: &[u8] = b"\\N";
pub const COLUMN_SEPARATOR: u8 = b'|';
pub const LINE_DELIMITER: u8 = b'\n';

/// 值里出现即失败的结构字节
///
/// 分两遍 SIMD 扫（memchr 的 memchr2/memchr3），而不是逐字节 `iter().find`：
/// 直通面每行都要过一遍，30M 行 × ~175 B ≈ 5 GB —— 逐字节扫描实测要多吃几秒。
#[inline]
fn forbidden_in(v: &[u8]) -> Option<u8> {
    if let Some(i) = memchr::memchr3(b'|', b'\n', 0, v) {
        return Some(v[i]);
    }
    memchr::memchr2(b'\r', b'\\', v).map(|i| v[i])
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
}

impl RowBlockWriter {
    pub fn new(expected_cells: usize, block_bytes: usize, first_bytes: usize) -> Self {
        let first = if first_bytes == 0 {
            block_bytes
        } else {
            first_bytes
        };
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
            self.buf.push(COLUMN_SEPARATOR);
        }
        match value {
            None => self.buf.extend_from_slice(NULL_LITERAL),
            Some(v) => {
                if let Some(bad) = forbidden_in(v) {
                    return Err(D2DError::external(
                        "direct_path",
                        format!(
                            "dpi 直通：列 '{}' 第 {} 行含受限字节 {:?}。\
                             `|`/换行/NUL 会破坏文本结构，`\\` 会与 NULL 字面量 `\\N` 混淆。\
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
