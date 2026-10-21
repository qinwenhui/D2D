//! 数据文件 → 字节分块（行边界对齐）
//!
//! dmfldr 导出的产物是一份（或每分片一份）文本文件，**由本连接器自己按块并行解析**：
//! - 按近似字节边界切 N 块，边界向后扫描到**下一个行分隔符之后**，保证不切断行
//! - 无需全量行索引（避免 GB 级 offset 数组），每边界只读一个小窗口
//! - 切出来的是**数据区**的段：`start` 已经在列头之后，谁也不用再跳列头
//!
//! ⚠️ 并行度（切几块）来自 `source.properties.dmfldr.parse_parallel`，
//! **不是内核的安排** —— 内核只按任务发券，任务内部怎么切是连接器的事。

use d2d_core::{D2DError, D2DResult};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// 行分隔符（控制文件固定 STR X '0A'）
pub const ROW_SEP: u8 = b'\n';

/// 边界对齐扫描窗口
const ALIGN_WINDOW: usize = 64 * 1024;

/// 一个可独立解析的文件分块 —— **描述的是数据区的一段**
///
/// 不变量：`start` 已经**在列头之后**（无列头时就是 0）。所以没有人需要再跳列头 ——
/// 这里曾经有个 `skip_header: bool`，而分块 0 又是从 `header_end` 开始的，
/// 于是列头被跳过两次：**第一行数据被当成列头吃掉**。
/// TABLE 模式（`partition.mode: none`）因此每次少一行，分片模式因为
/// `header_end = 0` 侥幸没事 —— 这个坑的教训是"两个地方各自表达同一件事"。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunk {
    pub index: usize,
    /// 起始偏移（含）—— 列头之后
    pub start: u64,
    /// 结束偏移（不含）
    pub end: u64,
}

/// **一个解析块的目标大小**
///
/// 这是**取块粒度**，不是并发度 —— 块越小，各 source_task 之间负载越匀，
/// 但每块的启动开销越大；块太大则退化成"一个 task 从头解析到尾"。
///
/// 64 MB 的取法：本表的导出分片每个约 100 MB，于是它**恰好不触发额外切块**
/// （分片模式本身已经给出足够的并行单元）；而在 `partition.mode: none`
/// 单文件导出（动辄几个 GB）时，它会切出足够多的块把 16 个 source_task 铺满。
///
/// 与它取代的 `dmfldr.parse_chunks` 的区别：那个是**用户旋钮**，
/// 而且算法是 `per_file = parse_chunks / 分片数`，63 片时恒等于 1（空转）；
/// 这个是**按文件大小推**，有默认值、不需要解释、也不会空转。
pub const CHUNK_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// 按文件大小算这个文件该切几块（下限 1；`plan_chunks` 内部还会按 256KB 再收敛一次）
pub fn chunk_count_for(path: &Path) -> usize {
    match std::fs::metadata(path) {
        Ok(m) => ((m.len() / CHUNK_TARGET_BYTES).max(1)) as usize,
        // stat 失败不该在这里报错 —— 真正的打开失败会由 plan_chunks 带着路径报出来
        Err(_) => 1,
    }
}

/// 分块规划
///
/// - `header_end`：列头行结束偏移（无列头时为 0）
/// - `chunks`：期望分块数（内部会按文件大小收敛，小文件自动降为 1 块）
pub fn plan_chunks(path: &Path, chunks: usize, header_end: u64) -> D2DResult<Vec<FileChunk>> {
    let meta = std::fs::metadata(path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("failed to stat dmfldr data file {}: {}", path.display(), e),
        )
    })?;
    let file_len = meta.len();
    if file_len <= header_end {
        // 空文件（空表导出）—— 单空块
        return Ok(vec![FileChunk {
            index: 0,
            start: header_end,
            end: header_end,
        }]);
    }

    let data_len = file_len - header_end;
    // 数据区太小不值得并行
    let effective = chunks.max(1).min((data_len / (256 * 1024)).max(1) as usize);
    if effective <= 1 {
        return Ok(vec![FileChunk {
            index: 0,
            start: header_end,
            end: file_len,
        }]);
    }

    let mut file = File::open(path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("failed to open data file {}: {}", path.display(), e),
        )
    })?;

    let mut boundaries: Vec<u64> = Vec::with_capacity(effective + 1);
    boundaries.push(header_end);
    for i in 1..effective {
        let approx = header_end + data_len * i as u64 / effective as u64;
        let aligned = align_to_row_boundary(&mut file, approx, file_len)?;
        boundaries.push(aligned.max(header_end).min(file_len));
    }
    boundaries.push(file_len);

    // 去重 + 排序（边界可能重合）
    boundaries.sort_unstable();
    boundaries.dedup();

    let mut chunks_out = Vec::with_capacity(boundaries.len().saturating_sub(1));
    for (i, w) in boundaries.windows(2).enumerate() {
        chunks_out.push(FileChunk {
            index: i,
            start: w[0],
            end: w[1],
        });
    }
    Ok(chunks_out)
}

/// 从 approx 起向后找到第一个行分隔符，返回其**之后**的偏移；
/// 若到文件尾都没有（理论不应发生），返回 file_len
fn align_to_row_boundary(file: &mut File, approx: u64, file_len: u64) -> D2DResult<u64> {
    let mut pos = approx;
    let mut buf = vec![0u8; ALIGN_WINDOW];
    while pos < file_len {
        file.seek(SeekFrom::Start(pos)).map_err(|e| {
            D2DError::external("dmfldr", format!("seek failed while chunk planning: {}", e))
        })?;
        let want = ((file_len - pos) as usize).min(ALIGN_WINDOW);
        let read = file.read(&mut buf[..want]).map_err(|e| {
            D2DError::external("dmfldr", format!("read failed while chunk planning: {}", e))
        })?;
        if read == 0 {
            break;
        }
        if let Some(off) = memchr::memchr(ROW_SEP, &buf[..read]) {
            return Ok(pos + off as u64 + 1);
        }
        pos += read as u64;
    }
    Ok(file_len)
}

/// 读取并解析列头行（COLNAME_HEADING=TRUE），返回 (首数据行偏移, 列名列表)
///
/// 列头按 field_sep 切分后与期望列名比对，不符即 schema 错误。
pub fn read_header(
    path: &Path,
    field_sep: &[u8],
    expected: &[String],
) -> D2DResult<(u64, Vec<String>)> {
    let mut file = File::open(path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("failed to open data file {}: {}", path.display(), e),
        )
    })?;
    let mut buf = Vec::new();
    // 列头行受字段数约束，不会巨大；给 1MB 上限足够
    let mut limited = file.by_ref().take(1024 * 1024);
    limited
        .read_to_end(&mut buf)
        .map_err(|e| D2DError::external("dmfldr", format!("failed to read header line: {}", e)))?;
    let nl = memchr::memchr(ROW_SEP, &buf)
        .ok_or_else(|| D2DError::schema("dmfldr data file has no row separator (empty?)"))?;
    let line = &buf[..nl];
    // 容忍 CRLF
    let line = if line.ends_with(b"\r") {
        &line[..line.len() - 1]
    } else {
        line
    };
    let names: Vec<String> =
        crate::source::method::dmfldr::parse::fields::split_fields(line, field_sep, None)
            .iter()
            .map(|f| String::from_utf8_lossy(f).trim().to_string())
            .collect();
    if names.len() != expected.len() {
        return Err(D2DError::schema(format!(
            "dmfldr header column count mismatch: file has {}, contract has {}",
            names.len(),
            expected.len()
        )));
    }
    for (i, (got, want)) in names.iter().zip(expected.iter()).enumerate() {
        if !got.eq_ignore_ascii_case(want) {
            return Err(D2DError::schema(format!(
                "dmfldr header column #{} mismatch: file has '{}', contract has '{}' \
                 (schema drift — update type_contract or the table)",
                i, got, want
            )));
        }
    }
    Ok((nl as u64 + 1, names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_plan_chunks_basic() {
        // 数据区需 > 256KB 才会触发多分块（小文件保护见 plan_chunks）
        let mut s = String::from("id|name\n");
        for i in 0..8000 {
            let mut line = format!("{}|row{}\n", i, i);
            while line.len() < 160 {
                line.push('x');
            }
            line.push('\n');
            s.push_str(&line);
        }
        let f = write_file(&s);
        let chunks = plan_chunks(f.path(), 4, 8).unwrap();
        assert!(chunks.len() >= 2 && chunks.len() <= 4, "{:?}", chunks);
        // 覆盖完整数据区且不重叠。**每块都从列头之后开始** —— 数据区第一行
        // 就在 chunks[0].start 处，没有任何人再跳一次列头（见 `FileChunk` 的注释：
        // 这里曾经跳过两次，TABLE 模式每次少一行）
        assert_eq!(chunks.first().unwrap().start, 8);
        assert_eq!(
            chunks.last().unwrap().end,
            std::fs::metadata(f.path()).unwrap().len()
        );
        for w in chunks.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }
        // 每个块的 start（除首块）前一个字节必须是行分隔符
        let bytes = std::fs::read(f.path()).unwrap();
        for c in &chunks[1..] {
            assert_eq!(
                bytes[(c.start - 1) as usize],
                ROW_SEP,
                "chunk {} not aligned",
                c.index
            );
        }
    }

    #[test]
    fn test_plan_chunks_no_header() {
        let mut s = String::new();
        for i in 0..20 {
            s.push_str(&format!("line{}\n", i));
        }
        let f = write_file(&s);
        let chunks = plan_chunks(f.path(), 4, 0).unwrap();
        let total: u64 = chunks.iter().map(|c| c.end - c.start).sum();
        assert_eq!(total, s.len() as u64);
    }

    #[test]
    fn test_plan_chunks_tiny_file_single() {
        let f = write_file("a|b\nc|d\n");
        let chunks = plan_chunks(f.path(), 8, 4).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 4, "起点在列头之后");
    }

    #[test]
    fn test_read_header_ok() {
        let f = write_file("id|name|amt\n1|a|2.5\n");
        let (data_start, names) =
            read_header(f.path(), b"|", &["id".into(), "name".into(), "amt".into()]).unwrap();
        assert_eq!(names, vec!["id", "name", "amt"]);
        assert_eq!(data_start, 12); // "id|name|amt\n" = 12 bytes
    }

    #[test]
    fn test_read_header_crlf_tolerance() {
        let f = write_file("id|name\r\n1|a\r\n");
        let (data_start, names) =
            read_header(f.path(), b"|", &["id".into(), "name".into()]).unwrap();
        assert_eq!(names, vec!["id", "name"]);
        assert_eq!(data_start, 9); // "id|name\r\n" = 7 + CRLF
    }

    #[test]
    fn test_read_header_mismatch_errors() {
        let f = write_file("id|nm\n1|a\n");
        let err = read_header(f.path(), b"|", &["id".into(), "name".into()]).unwrap_err();
        assert!(err.to_string().contains("mismatch"), "{}", err);

        let f2 = write_file("id\n1\n");
        let err2 = read_header(f2.path(), b"|", &["id".into(), "name".into()]).unwrap_err();
        assert!(err2.to_string().contains("column count"), "{}", err2);
    }
}
