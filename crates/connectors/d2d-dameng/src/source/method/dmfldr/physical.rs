//! dmfldr 导出产物的**物理载荷**读取（直通路径的源端半边）
//!
//! dmfldr 导出的就是目标端要吃的 CSV 文本，中间不存在任何"必须经过引擎"的变换。
//! 所以这里做的事只有一件：把文件的一段字节**按行对齐**切块吐出来。
//!
//! ## 为什么块边界必须行对齐
//!
//! 1. 消费端（直通面上的 `sink_task`）会把块首尾相接拼成 HTTP body，
//!    跨块的半截行会变成一条被腰斩的记录；
//! 2. 行数按块内的 `\n` 计数 —— 行对齐才保证「块行数之和 = 文件行数」这个等式，
//!    而它是收尾行数对账的**唯一数据来源**。
//!
//! ## 为什么用 `spawn_blocking`
//!
//! 文件读是阻塞系统调用。若直接在 async 任务里做，会占住 tokio worker ——
//! 本项目在逻辑面就踩过一次这类死锁（见 `pipeline.rs` 通道注释），不再重蹈。
//! 这里把 I/O 整体挪进 `spawn_blocking`，用**有界** async 通道回传：
//! 通道满 → `blocking_send` 阻塞 → 读停下 → 内存有上界。

use d2d_connector::{PhysicalBlock, PhysicalStream};
use d2d_core::{D2DError, D2DResult};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, warn};

/// 单个物理块的目标大小
///
/// 4MB 的取法：足够大以摊薄 HTTP 分块与 `memchr` 调用开销，又足够小以让
/// 「通道容量 × 块大小 × 并发分片」保持在做内存预算时心算得出（4MB × 4 × 16 ≈ 256MB）。
pub const PHYSICAL_BLOCK_BYTES: usize = 4 * 1024 * 1024;

/// 读缓冲（比块略大，保证一次读基本能填满一块）
const READ_BUF_BYTES: usize = 1024 * 1024;

/// 打开一份产物的 `[start, end)` 区间，按行对齐切块吐出去
///
/// ⚠️ `start` **指向数据区起点**（列头之后），所以这里**没有人再跳列头**。
/// 这里曾经有个 `skip_header` 参数，而调用方给的 `start` 又已经在列头之后 ——
/// 两处各自表达同一件事，于是列头被跳过两次、**第一行数据被吃掉**
/// （与逻辑面同一个坑，见 `plan::FileChunk` 的注释）。
///
/// 区间与逻辑面 `FlDrMethod::read` 用的是同一份规划结果 —— 两条路径看到的是
/// **完全相同的数据切分**，这是「直通与逻辑面结果应逐位一致」的结构性保证。
pub fn open_range(
    file: PathBuf,
    start: u64,
    end: u64,
    split_id: String,
) -> D2DResult<PhysicalStream> {
    // 有界通道：满则读侧阻塞，形成背压
    let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<PhysicalBlock>>(4);

    tokio::task::spawn_blocking(move || {
        if let Err(e) = pump(&file, start, end, &tx) {
            let _ = tx.blocking_send(Err(e));
        }
        debug!(split_id = %split_id, "dmfldr 物理块读取结束");
    });

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

/// 阻塞读循环
fn pump(
    path: &Path,
    start: u64,
    end: u64,
    tx: &tokio::sync::mpsc::Sender<D2DResult<PhysicalBlock>>,
) -> D2DResult<()> {
    let len = end.saturating_sub(start);
    if len == 0 {
        return Ok(());
    }
    let f = std::fs::File::open(path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("打开物理载荷文件失败 {}: {}", path.display(), e),
        )
    })?;
    let mut f = f;
    f.seek(SeekFrom::Start(start)).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("物理载荷定位失败 {}: {}", path.display(), e),
        )
    })?;
    // `start` 已经在列头之后，所以这里直接从数据第一行开始读（见 `open_range` 的注释）
    let mut r = BufReader::with_capacity(READ_BUF_BYTES, f.take(len));

    let mut acc: Vec<u8> = Vec::with_capacity(PHYSICAL_BLOCK_BYTES + READ_BUF_BYTES);
    let mut read_buf = vec![0u8; READ_BUF_BYTES];

    loop {
        let got = loop {
            match r.read(&mut read_buf) {
                Ok(n) => break n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(D2DError::external(
                        "dmfldr",
                        format!("读物理载荷失败 {}: {}", path.display(), e),
                    ))
                }
            }
        };
        if got > 0 {
            acc.extend_from_slice(&read_buf[..got]);
        }

        let eof = got == 0;
        if eof || acc.len() >= PHYSICAL_BLOCK_BYTES {
            // 先切到最后一个换行之后：块 = 整数条记录
            if let Some(i) = memchr::memrchr(b'\n', &acc) {
                let tail = acc.split_off(i + 1);
                let out = std::mem::replace(&mut acc, tail);
                if tx.blocking_send(Ok(make_block(out))).is_err() {
                    return Ok(()); // 下游已断开
                }
            }
            // ⚠️ 按「最后一个换行」切分后，`acc` 里可能还留着一截**没有换行符**的
            // 尾巴 —— 那就是文件的最后一条记录，必须单独发出。
            //
            // 历史 bug：这一段曾被写成 match 的 `None if eof =>` 分支，于是
            // 「acc 里含换行、但尾部还有残行」时**永远进不去**，末行被静默丢弃。
            // 目标端与源端的行数对账能事后抓到它，但那是失败重跑；
            // 这里直接发对，不留这个坑。
            if eof && !acc.is_empty() {
                let mut out = std::mem::take(&mut acc);
                if out.last() != Some(&b'\n') {
                    warn!(
                        file = %path.display(),
                        "物理载荷末行缺少换行符，已补 —— 否则目标端会丢掉这条记录"
                    );
                    out.push(b'\n');
                }
                if tx.blocking_send(Ok(make_block(out))).is_err() {
                    return Ok(());
                }
            }
        }

        if eof {
            return Ok(());
        }
    }
}

// ─────────────────────────── 流式跟随读（导出未结束就开始交付）───────────────────────────

/// 跟随读的轮询间隔
///
/// 10ms：dmfldr 的写入粒度约 16MB / 0.5s，再密只会空转；再疏则会增加端到端尾延迟。
const TAIL_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// 导出子进程的完成状态：阻塞读线程轮询它，异步等待任务写入它
///
/// # 为什么需要一层共享状态
///
/// 跟随读在 `spawn_blocking` 线程里做，而「dmfldr 是否退出、退出码多少、
/// 行数对账是否失败」由另一条 async 任务等出来。两条线程之间只交换两件事：
/// **结束了吗**、**失败原因是什么**。`done` 用 Release/Acquire 发布，
/// 保证读到 `true` 时错误槽已经写好。
#[derive(Debug, Default)]
pub struct ExportDone {
    done: AtomicBool,
    error: Mutex<Option<D2DError>>,
}

impl ExportDone {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 标记结束；`error` 非空表示本次导出失败（退出码非 0 / 行数对账失败）
    pub fn finish(&self, error: Option<D2DError>) {
        if let Some(e) = error {
            *self.error.lock().unwrap() = Some(e);
        }
        self.done.store(true, Ordering::Release);
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub fn error(&self) -> Option<D2DError> {
        self.error.lock().unwrap().clone()
    }
}

/// 跟随读一个**正在增长**的数据文件
///
/// # 为什么不能直接读到 `metadata.len()`
///
/// 实测 dmfldr 会按 16MB 预扩 DATA 文件，未写区域是 NUL 字节 —— 文件大小
/// **不等于**有效数据长度。所以每次读到的字节都要在**首个 NUL** 处截断；
/// 遇到 NUL（或当前 EOF）就睡眠等待，直到 [`ExportDone`] 被置位。
///
/// # 为什么用 seek + read
///
/// 跨平台（本仓库在 Windows 上开发、Linux 上跑批），且只有一个读者；
/// 每次从 `pos` 重新定位的代价可以忽略，换来的是「半行不会丢」的简单语义。
pub(crate) struct TailReader {
    file: std::fs::File,
    pos: u64,
    done: Arc<ExportDone>,
    /// 结束时的「尾部是否全是零」检查只做一次
    verified: bool,
}

impl TailReader {
    /// 打开数据文件；文件可能还没被 dmfldr 创建，等它出现（或等导出失败）
    pub(crate) fn open(path: &Path, start: u64, done: Arc<ExportDone>) -> D2DResult<Self> {
        let file = loop {
            match std::fs::File::open(path) {
                Ok(f) => break f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    if done.is_done() {
                        return Err(done.error().unwrap_or_else(|| {
                            D2DError::external(
                                "dmfldr",
                                format!("导出已结束但数据文件不存在: {}", path.display()),
                            )
                        }));
                    }
                    std::thread::sleep(TAIL_POLL_INTERVAL);
                }
                Err(e) => {
                    return Err(D2DError::external(
                        "dmfldr",
                        format!("打开数据文件失败 {}: {}", path.display(), e),
                    ))
                }
            }
        };
        Ok(Self {
            file,
            pos: start,
            done,
            verified: false,
        })
    }

    /// 结束时复核：`pos` 之后若还有**非零**字节，说明有效数据里的 NUL 被误判成了
    /// 预扩零区 —— 必须显式失败，不能静默截断。
    ///
    /// 正常形态是「结束即截到真实长度」或「零区直到文件尾」，两种都放行。
    fn verify_no_hidden_data(&mut self) -> std::io::Result<()> {
        if self.verified {
            return Ok(());
        }
        self.verified = true;
        let sz = self.file.metadata()?.len();
        if self.pos >= sz {
            return Ok(());
        }
        let mut buf = vec![0u8; READ_BUF_BYTES];
        let mut p = self.pos;
        self.file.seek(SeekFrom::Start(p))?;
        loop {
            let n = self.file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            if buf[..n].iter().any(|&b| b != 0) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "数据文件在偏移 {} 之后仍有非零字节：有效数据中的 NUL 被误判为预扩零区，\
                         继续读会静默丢数",
                        p
                    ),
                ));
            }
            p += n as u64;
        }
        Ok(())
    }
}

impl Read for TailReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            self.file.seek(SeekFrom::Start(self.pos))?;
            let n = self.file.read(buf)?;
            if n > 0 {
                // 预扩零区：首个 NUL 之后都不是已写数据
                let valid = memchr::memchr(0, &buf[..n]).unwrap_or(n);
                if valid > 0 {
                    self.pos += valid as u64;
                    return Ok(valid);
                }
            }

            // 当前可读位置是 EOF 或零区：等 dmfldr 继续写
            if self.done.is_done() {
                if let Some(e) = self.done.error() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ));
                }
                self.verify_no_hidden_data()?;
                return Ok(0);
            }
            std::thread::sleep(TAIL_POLL_INTERVAL);
        }
    }
}

/// 流式打开一份**正在导出**的产物：按行对齐切块，导出结束前就能持续交付
///
/// 与 [`open_range`] 的契约完全相同（块边界行对齐、`rows` 精确），差别只有：
/// `end` 不预先知道，由 [`ExportDone`] 的置位来宣告结束。
pub fn open_streaming(
    file: PathBuf,
    start: u64,
    done: Arc<ExportDone>,
    split_id: String,
    block_bytes: usize,
    first_block_bytes: usize,
) -> D2DResult<PhysicalStream> {
    // 与 `open_range` 相同的有界通道：满则读侧阻塞，形成背压
    let (tx, rx) = tokio::sync::mpsc::channel::<D2DResult<PhysicalBlock>>(4);

    tokio::task::spawn_blocking(move || {
        if let Err(e) = pump_streaming(&file, start, done, &tx, block_bytes, first_block_bytes) {
            let _ = tx.blocking_send(Err(e));
        }
        debug!(split_id = %split_id, "dmfldr 流式物理块读取结束");
    });

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

/// 跟随读的阻塞循环：攒到目标块大小就在最后一个换行处切块发出去
///
/// `first_block_bytes` 只作用于本分片的**第一块**；后续用 `block_bytes`。
fn pump_streaming(
    path: &Path,
    start: u64,
    done: Arc<ExportDone>,
    tx: &tokio::sync::mpsc::Sender<D2DResult<PhysicalBlock>>,
    block_bytes: usize,
    first_block_bytes: usize,
) -> D2DResult<()> {
    let block_bytes = block_bytes.max(1);
    let first_block_bytes = first_block_bytes.clamp(1, block_bytes);
    let tail = TailReader::open(path, start, Arc::clone(&done))?;
    let mut r = BufReader::with_capacity(READ_BUF_BYTES, tail);
    let mut emitted_any = false;

    let mut acc: Vec<u8> = Vec::with_capacity(block_bytes + READ_BUF_BYTES);
    let mut read_buf = vec![0u8; READ_BUF_BYTES];

    loop {
        let got = loop {
            match r.read(&mut read_buf) {
                Ok(n) => break n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    // TailReader 把导出失败塞进了 io::Error；优先还原本来的 D2DError
                    // （保留 SourceRead/External 的类别与原始文案）
                    return Err(done.error().unwrap_or_else(|| {
                        D2DError::external(
                            "dmfldr",
                            format!("读流式产物失败 {}: {}", path.display(), e),
                        )
                    }));
                }
            }
        };
        if got > 0 {
            acc.extend_from_slice(&read_buf[..got]);
        }

        let eof = got == 0;
        let target = if emitted_any {
            block_bytes
        } else {
            first_block_bytes
        };
        if eof || acc.len() >= target {
            if let Some(i) = memchr::memrchr(b'\n', &acc) {
                let tail = acc.split_off(i + 1);
                let out = std::mem::replace(&mut acc, tail);
                if tx.blocking_send(Ok(make_block(out))).is_err() {
                    return Ok(()); // 下游已断开
                }
                emitted_any = true;
            }
            // 导出结束：把最后没带换行的一截补出来（与 `open_range` 的收尾一致）
            if eof && !acc.is_empty() {
                let mut out = std::mem::take(&mut acc);
                if out.last() != Some(&b'\n') {
                    warn!(
                        file = %path.display(),
                        "流式产物末行缺少换行符，已补 —— 否则目标端会丢掉这条记录"
                    );
                    out.push(b'\n');
                }
                if tx.blocking_send(Ok(make_block(out))).is_err() {
                    return Ok(());
                }
            }
        }

        if eof {
            return Ok(());
        }
    }
}

/// 统计行数并封块
///
/// 行数 = 块内 `\n` 个数。因为块的末尾一定落在换行符之后，且块首是行首，
/// 所以这个计数与「块覆盖的完整记录条数」严格相等。
fn make_block(bytes: Vec<u8>) -> PhysicalBlock {
    let rows = memchr::memchr_iter(b'\n', &bytes).count() as u64;
    PhysicalBlock { bytes, rows }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Write;

    async fn drain(path: &Path, start: u64, end: u64) -> (Vec<u8>, u64, usize) {
        let mut st = open_range(path.to_path_buf(), start, end, "t".into()).unwrap();
        let mut out = Vec::new();
        let mut rows = 0u64;
        let mut blocks = 0usize;
        while let Some(b) = st.next().await {
            let b = b.unwrap();
            rows += b.rows;
            blocks += 1;
            out.extend_from_slice(&b.bytes);
        }
        (out, rows, blocks)
    }

    fn tmp(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        f
    }

    #[tokio::test]
    async fn test_basic_rows_and_bytes() {
        let f = tmp(b"a|b\nc|d\ne|f\n");
        let (out, rows, _) = drain(f.path(), 0, 12).await;
        assert_eq!(rows, 3);
        assert_eq!(out, b"a|b\nc|d\ne|f\n");
    }

    /// **回归：列头只能跳过一次。**
    ///
    /// 调用方给的 `start` 已经在列头之后，这里若再跳一次，第一行数据就没了。
    /// 分片模式（`start = 0` 且产物无列头）看不出这个错，TABLE 模式每次少一行。
    #[tokio::test]
    async fn test_range_starts_after_header_without_skipping_again() {
        let f = tmp(b"id|name\n1|a\n2|b\n");
        let (out, rows, _) = drain(f.path(), 8, 16).await; // 8 = "id|name\n" 之后
        assert_eq!(rows, 2, "两行数据一行不能少");
        assert_eq!(out, b"1|a\n2|b\n");
    }

    /// 造一个**远超块大小**的文件（否则只会有 1 块，测不出边界对齐）
    fn big_file(lines: u32, trailing_newline: bool) -> (tempfile::NamedTempFile, Vec<u8>) {
        let mut s = Vec::new();
        for i in 0..lines {
            s.extend_from_slice(format!("{:08}\t{:012}\n", i, i * 7).as_bytes());
        }
        if !trailing_newline {
            s.pop();
        }
        assert!(
            s.len() > PHYSICAL_BLOCK_BYTES * 2,
            "测试数据必须跨多块才能验证边界: {} 字节",
            s.len()
        );
        let f = tmp(&s);
        (f, s)
    }

    /// **关键不变量**：无论怎么切块，块边界必须落在行边界上，
    /// 且「各行拼接」= 原文件对应的连续区间（逐位一致）
    #[tokio::test]
    async fn test_block_boundaries_are_row_aligned() {
        let (f, s) = big_file(700_000, true);
        let len = s.len() as u64;
        let (out, rows, blocks) = drain(f.path(), 0, len).await;
        assert_eq!(rows, 700_000, "行数必须精确");
        assert_eq!(out, s, "拼接结果必须与原文件逐位一致");
        assert!(blocks > 1, "应当切成多块, 实际 {}", blocks);
    }

    /// 多块 + **末行无换行**：残行跨越块边界时也不能丢
    ///
    /// 这是最容易漏的一条：按「最后一个换行」切块后，尾部残行若不单独发出，
    /// 就会静默少一行 —— 且只有跨块时才会暴露。
    #[tokio::test]
    async fn test_multiblock_without_trailing_newline() {
        let (f, s) = big_file(700_000, false);
        let len = s.len() as u64;
        let (out, rows, blocks) = drain(f.path(), 0, len).await;
        assert!(blocks > 1, "应当切成多块, 实际 {}", blocks);
        assert_eq!(rows, 700_000, "末行无换行也必须计入行数");
        // 内容上只应多出补的那个换行符
        assert_eq!(out.len(), s.len() + 1);
        assert_eq!(&out[..s.len()], &s[..]);
        assert_eq!(*out.last().unwrap(), b'\n');
    }

    /// 与 `plan_chunks` 联合：分块区间各自读出来的内容，拼接后 = 原文件
    #[tokio::test]
    async fn test_chunks_concatenate_to_whole_file() {
        let mut s = Vec::new();
        for i in 0..80_000u32 {
            s.extend_from_slice(format!("row-{}|{}\n", i, i).as_bytes());
        }
        let f = tmp(&s);
        let chunks = super::super::plan::plan_chunks(f.path(), 8, 0).unwrap();
        assert!(chunks.len() > 1);

        let mut whole = Vec::new();
        let mut total_rows = 0u64;
        for c in &chunks {
            let (out, rows, _) = drain(f.path(), c.start, c.end).await;
            whole.extend_from_slice(&out);
            total_rows += rows;
        }
        assert_eq!(whole, s, "各分块拼接必须还原原文件");
        assert_eq!(total_rows, 80_000, "分块行数之和必须等于总行数");
    }

    /// 末行无换行：必须补一个，否则目标端会静默丢掉最后一条记录
    #[tokio::test]
    async fn test_missing_trailing_newline_is_fixed() {
        let f = tmp(b"a|b\nc|d");
        let (out, rows, _) = drain(f.path(), 0, 7).await;
        assert_eq!(rows, 2, "末行无换行也必须计入行数");
        assert_eq!(out, b"a|b\nc|d\n");
    }

    #[tokio::test]
    async fn test_empty_range_yields_nothing() {
        let f = tmp(b"a|b\n");
        let (out, rows, blocks) = drain(f.path(), 0, 0).await;
        assert_eq!(rows, 0);
        assert_eq!(blocks, 0);
        assert!(out.is_empty());
    }

    /// 文件不存在 → 错误里要带路径（而不是一句 "not found"）
    #[tokio::test]
    async fn test_missing_file_reports_its_path() {
        use futures::StreamExt;
        let missing = std::env::temp_dir().join("d2d-physical-does-not-exist.txt");
        let mut st = open_range(missing.clone(), 0, 1024, "x".into()).unwrap();
        let e = st.next().await.unwrap().unwrap_err().to_string();
        assert!(e.contains("does-not-exist"), "错误要带上是哪个文件: {}", e);
    }

    // ─────────────── 流式跟随读 ───────────────

    /// **核心场景**：导出进程还在写，读者已经拿到块。
    ///
    /// 同时模拟 dmfldr 的两个实测行为：文件按 16MB 预扩（尾部是 NUL）、
    /// 数据分多次写入（中间有停顿）。读者必须只在「首个 NUL 之前」取数，
    /// 且最终拼接结果与原数据逐位一致、行数精确。
    #[tokio::test]
    async fn test_streaming_follows_growing_file_with_zero_tail() {
        use std::io::{Seek, SeekFrom};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grow.txt");

        let line_count = 40_000usize;
        let mut data = Vec::new();
        for i in 0..line_count {
            data.extend_from_slice(format!("{:08}|{}\n", i, "x".repeat(100)).as_bytes());
        }
        assert!(data.len() > PHYSICAL_BLOCK_BYTES, "样本必须跨块");
        // 在换行处切开，模拟「写到一半」
        let cut = data[..data.len() / 2]
            .iter()
            .rposition(|&b| b == b'\n')
            .map(|i| i + 1)
            .unwrap();
        let (first, second) = (data[..cut].to_vec(), data[cut..].to_vec());

        let done = ExportDone::new();
        let p = path.clone();
        let d = Arc::clone(&done);
        let total_len = data.len() as u64;
        let writer = std::thread::spawn(move || {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&p)
                .unwrap();
            // dmfldr 实测：先按 16MB 预扩，未写区域是零
            f.set_len(16 * 1024 * 1024).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&first).unwrap();
            f.flush().unwrap();
            std::thread::sleep(Duration::from_millis(150));
            f.seek(SeekFrom::Start(first.len() as u64)).unwrap();
            f.write_all(&second).unwrap();
            f.flush().unwrap();
            // 结束时截到真实长度（dmfldr 实测行为）
            f.set_len(total_len).unwrap();
            d.finish(None);
        });

        let mut st = open_streaming(
            path,
            0,
            done,
            "grow".into(),
            PHYSICAL_BLOCK_BYTES,
            PHYSICAL_BLOCK_BYTES,
        )
        .unwrap();
        let mut out = Vec::new();
        let mut rows = 0u64;
        while let Some(b) = st.next().await {
            let b = b.unwrap();
            rows += b.rows;
            out.extend_from_slice(&b.bytes);
        }
        writer.join().unwrap();

        assert_eq!(rows, line_count as u64, "行数必须精确");
        assert_eq!(
            out, data,
            "拼接结果必须与原数据逐位一致（零区不能被当成数据）"
        );
    }

    /// 导出失败：已经交付过的块不受影响，但错误必须出现在流里
    /// （不能把「dmfldr 中途挂了」伪装成正常 EOF）
    #[tokio::test]
    async fn test_streaming_surfaces_export_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.txt");
        let mut data = Vec::new();
        for i in 0..50_000 {
            data.extend_from_slice(format!("{:08}|{}\n", i, "x".repeat(100)).as_bytes());
        }
        std::fs::write(&path, &data).unwrap();

        let done = ExportDone::new();
        let d = Arc::clone(&done);
        let boom = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            d.finish(Some(D2DError::external(
                "dmfldr",
                "boom: dmfldr exited with code 1",
            )));
        });

        let mut st = open_streaming(
            path,
            0,
            done,
            "bad".into(),
            PHYSICAL_BLOCK_BYTES,
            PHYSICAL_BLOCK_BYTES,
        )
        .unwrap();
        let mut err = None;
        while let Some(item) = st.next().await {
            if let Err(e) = item {
                err = Some(e.to_string());
                break;
            }
        }
        boom.join().unwrap();

        assert!(
            err.expect("导出失败必须在流里冒出来").contains("boom"),
            "错误文案要保留原始原因"
        );
    }

    /// 末行没有换行：导出结束时必须补一个，否则目标端会静默丢掉最后一条
    #[tokio::test]
    async fn test_streaming_flushes_last_line_without_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nolf.txt");
        std::fs::write(&path, b"a|b\nc|d").unwrap();

        let done = ExportDone::new();
        done.finish(None);
        let mut st = open_streaming(
            path,
            0,
            done,
            "nolf".into(),
            PHYSICAL_BLOCK_BYTES,
            PHYSICAL_BLOCK_BYTES,
        )
        .unwrap();
        let mut out = Vec::new();
        let mut rows = 0u64;
        while let Some(b) = st.next().await {
            let b = b.unwrap();
            rows += b.rows;
            out.extend_from_slice(&b.bytes);
        }
        assert_eq!(rows, 2);
        assert_eq!(out, b"a|b\nc|d\n");
    }

    /// 文件比读者先出现（导出已结束、done 已置位）也能正常读完
    #[tokio::test]
    async fn test_streaming_reads_already_finished_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("done.txt");
        std::fs::write(&path, b"1|a\n2|b\n").unwrap();
        let done = ExportDone::new();
        done.finish(None);
        let mut st = open_streaming(
            path,
            0,
            done,
            "done".into(),
            PHYSICAL_BLOCK_BYTES,
            PHYSICAL_BLOCK_BYTES,
        )
        .unwrap();
        let mut rows = 0;
        while let Some(b) = st.next().await {
            rows += b.unwrap().rows;
        }
        assert_eq!(rows, 2);
    }
}

#[cfg(test)]
mod benches {

    /// 计量开销微基准：把「数行」与「受限字节扫描」的实际代价量出来
    ///
    /// 存在的意义：这两个动作都在**每个字节上**跑一遍，直觉上"肯定很贵"。
    /// 真到了要决定「默认开还是关」的时候，需要的是数字而不是直觉。
    ///
    /// ```text
    /// cargo test -p d2d-dameng --release --lib bench_scan_overhead -- --ignored --nocapture
    /// ```
    #[test]
    #[ignore]
    fn bench_scan_overhead() {
        use std::time::Instant;

        // 造一段形态接近真实的数据：37 列、每行约 460 字节
        let mut unit = String::with_capacity(512);
        unit.push_str("USER_ID-abcd-0123456789");
        while unit.len() < 456 {
            unit.push('x');
        }
        unit.push_str("|||||\n");
        let line = unit.into_bytes();

        let target_bytes: usize = 1 << 30; // 1 GiB
        let reps = target_bytes / line.len();
        let mut buf = Vec::with_capacity(reps * line.len());
        for _ in 0..reps {
            buf.extend_from_slice(&line);
        }
        let gib = buf.len() as f64 / (1u64 << 30) as f64;
        println!("\n样本: {:.2} GiB / {} 行", gib, reps);

        // ── 1. 数行：memchr_iter(b'\n').count() ──
        let t = Instant::now();
        let rows = memchr::memchr_iter(b'\n', &buf).count();
        let d1 = t.elapsed();
        println!(
            "数行        : {:>8.1} ms  → {:>6.2} GiB/s   ({} 行)",
            d1.as_secs_f64() * 1e3,
            gib / d1.as_secs_f64(),
            rows
        );

        // ── 2. 受限字节扫描：memchr(b'\\', ..) ──
        let t = Instant::now();
        let hit = memchr::memchr(b'\\', &buf).is_some();
        let d2 = t.elapsed();
        println!(
            "受限字节扫描: {:>8.1} ms  → {:>6.2} GiB/s   (命中={})",
            d2.as_secs_f64() * 1e3,
            gib / d2.as_secs_f64(),
            hit
        );

        // ── 3. 找块切点：memrchr(b'\n', ..)（搬运本来就要做的）──
        let t = Instant::now();
        let _ = memchr::memrchr(b'\n', &buf);
        let d3 = t.elapsed();
        println!(
            "找块切点    : {:>8.1} ms  → {:>6.2} GiB/s   （本来就要做，非额外开销）",
            d3.as_secs_f64() * 1e3,
            gib / d3.as_secs_f64()
        );

        let extra = d1 + d2;
        println!(
            "\n两个额外扫描合计 {:.1} ms/GiB；按 8.9 GiB 数据、16 条流水线并行摊薄：\
             \n  串行等价 {:.2} s CPU，墙钟约 {:.2} s（≈ 总耗时的 {:.2}%）",
            extra.as_secs_f64() * 1e3,
            extra.as_secs_f64() * 8.9,
            extra.as_secs_f64() * 8.9 / 16.0,
            extra.as_secs_f64() * 8.9 / 16.0 / 110.0 * 100.0
        );
    }
}
