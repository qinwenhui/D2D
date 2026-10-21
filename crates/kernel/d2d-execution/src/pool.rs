//! 数据池：两个 stage 之间的**有界缓冲**，带溢出策略
//!
//! # 它在拓扑里的位置
//!
//! ```text
//! source_task ──► [source_pool] ──► pipeline_task ──► [sink_pool] ──► sink_task
//! ```
//!
//! 池是**生产者与消费者之间的解耦点**。没有它，上游快下游慢就只能靠"上游自己变慢"
//! （即上游被下游的节奏拖着走）；有了它，上游可以先行一步，超出缓冲的那部分由
//! [`PoolOverflow`] 处置。
//!
//! # 池容量管的是"排队量"，不是"总量"
//!
//! `capacity_bytes` 只约束**池里排队的那部分**。已被消费者取走、正在手上处理的那份
//! 不在池的账上 —— 它由 `并发数 × 批次大小` 静态决定，见 `flow` 模块的内存账。
//! 两笔加起来才是全链路常驻。
//!
//! # 三种溢出策略
//!
//! | 策略 | 上游 | 内存 | 磁盘 | 数据 |
//! |---|---|---|---|---|
//! | [`PoolOverflow::Spill`] | 不减速 | 封顶在容量 | 吃（可配上限） | 全部保留 |
//! | [`PoolOverflow::Backpressure`] | 等待 | 封顶在容量 | 不吃 | 全部保留 |
//! | [`PoolOverflow::Fail`] | 报错 | 封顶在容量 | 不吃 | 任务失败 |
//!
//! `Spill` 的存在理由：**内存不够不该变成"并发被砍"**。用户要的是若干个并发连接
//! 稳定地压在源库上（那是他能感知、能预料的），不是"我加了内存才会变快"。
//! 上游不该因为下游一时跟不上就停下来 —— 溢到磁盘去，下游慢慢读。
//!
//! # 溢出的顺序：溢"新来的"，不溢"即将被取走的"
//!
//! 池满时溢出的是**当前要入池的这个元素**，而不是队首那个老的。两个好处：
//!
//! 1. 不会把马上就要被消费的元素写下去又立刻读回来（白付一次 I/O）
//! 2. 磁盘区里的记录天然按入池顺序排列，读回时是**顺序读**
//!
//! 队列布局因此恒为 `[内存(老) …, 磁盘(新)]`，弹出从队首开始 —— 语义仍是 FIFO。

use d2d_core::{D2DError, D2DResult, RecordBatchExt};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// ─────────────────────────── 载荷契约 ───────────────────────────

/// 能进出数据池的元素
///
/// 实现它 = 回答三件事：占多少内存、怎么写到磁盘、怎么从磁盘读回来。
pub trait SpillItem: Send + 'static {
    /// 常驻内存估算（字节）—— 池的容量按它记账
    fn heap_bytes(&self) -> usize;

    /// 序列化到 spill 文件
    fn spill_to(&self, w: &mut dyn Write) -> D2DResult<()>;

    /// 从 spill 文件反序列化
    fn unspill_from(r: &mut dyn Read) -> D2DResult<Self>
    where
        Self: Sized;
}

impl SpillItem for arrow::array::RecordBatch {
    fn heap_bytes(&self) -> usize {
        self.estimated_memory_size()
    }

    /// Arrow IPC 流格式。
    ///
    /// 选 IPC 而不是 CSV/JSON：它**自带 schema**，读回来是逐位相同的批次
    /// （类型、null 位图、精度全部保留）。文本格式要重新推导类型，那正是
    /// 「两套类型解析规则各自演化」的起点，本项目在 dmfldr 早期为此吃过字段错位的亏。
    fn spill_to(&self, w: &mut dyn Write) -> D2DResult<()> {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(w, &self.schema())
            .map_err(|e| D2DError::internal(format!("spill: 建 IPC writer 失败: {}", e)))?;
        writer
            .write(self)
            .map_err(|e| D2DError::internal(format!("spill: 写批次失败: {}", e)))?;
        writer
            .finish()
            .map_err(|e| D2DError::internal(format!("spill: 收尾失败: {}", e)))?;
        Ok(())
    }

    fn unspill_from(r: &mut dyn Read) -> D2DResult<Self> {
        let mut reader = arrow::ipc::reader::StreamReader::try_new(r, None)
            .map_err(|e| D2DError::internal(format!("unspill: 建 IPC reader 失败: {}", e)))?;
        reader
            .next()
            .ok_or_else(|| D2DError::internal("unspill: IPC 流里没有批次"))?
            .map_err(|e| D2DError::internal(format!("unspill: 读批次失败: {}", e)))
    }
}

impl SpillItem for d2d_connector::PhysicalBlock {
    fn heap_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// 裸字节直接落盘：直通载荷本来就是目标端可直接吃的文本，
    /// 任何封装（压缩/编码）都只会给它加上一层要拆的壳。
    fn spill_to(&self, w: &mut dyn Write) -> D2DResult<()> {
        write_u64(w, self.rows)?;
        write_u64(w, self.bytes.len() as u64)?;
        w.write_all(&self.bytes)
            .map_err(|e| D2DError::internal(format!("spill: 写字节块失败: {}", e)))
    }

    fn unspill_from(r: &mut dyn Read) -> D2DResult<Self> {
        let rows = read_u64(r)?;
        let len = read_u64(r)? as usize;
        let mut bytes = vec![0u8; len];
        r.read_exact(&mut bytes)
            .map_err(|e| D2DError::internal(format!("unspill: 读字节块失败: {}", e)))?;
        Ok(d2d_connector::PhysicalBlock { bytes, rows })
    }
}

fn write_u64(w: &mut dyn Write, v: u64) -> D2DResult<()> {
    w.write_all(&v.to_le_bytes())
        .map_err(|e| D2DError::internal(format!("spill: 写长度失败: {}", e)))
}

fn read_u64(r: &mut dyn Read) -> D2DResult<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)
        .map_err(|e| D2DError::internal(format!("unspill: 读长度失败: {}", e)))?;
    Ok(u64::from_le_bytes(b))
}

// ─────────────────────────── 策略 ───────────────────────────

/// 池满时的处置方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolOverflow {
    /// 溢出到磁盘：上游**不减速**，多出来的落到 spill 文件，下游按需读回。
    ///
    /// 代价是磁盘 I/O 与磁盘空间；换的是"上游并发不被下游拖慢"。
    Spill,
    /// 背压：上游等待，直到下游取走一些。
    ///
    /// 缺省。不碰磁盘、不丢数据，代价是上游被下游的节奏拖着走。
    #[default]
    Backpressure,
    /// 直接失败：池满即判定为"这份配置撑不住这个负载"。
    ///
    /// 适合"宁可快速失败也不要悄悄变慢"的场景。
    Fail,
}

impl PoolOverflow {
    pub fn describe(&self) -> &'static str {
        match self {
            Self::Spill => "溢出到磁盘",
            Self::Backpressure => "背压等待",
            Self::Fail => "直接失败",
        }
    }
}

// ─────────────────────────── 配置与统计 ───────────────────────────

/// 一个池的配置
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// 池名（日志用，如 "source" / "sink"）
    pub name: &'static str,
    /// 内存里最多积压多少字节
    pub capacity_bytes: usize,
    pub overflow: PoolOverflow,
    /// spill 文件上限；超过即退化为背压。
    ///
    /// **必须有这一项**：没有它，`Spill` 只是把 OOM 从内存搬到磁盘 ——
    /// 磁盘写满的表现（进程报错、机器告警）比内存慢一步，反而更难归因。
    pub max_spill_bytes: usize,
    /// spill 目录；`None` → 临时目录
    pub spill_dir: Option<PathBuf>,
}

impl PoolConfig {
    pub fn new(name: &'static str, capacity_bytes: usize) -> Self {
        Self {
            name,
            capacity_bytes,
            overflow: PoolOverflow::default(),
            max_spill_bytes: 4 * 1024 * 1024 * 1024,
            spill_dir: None,
        }
    }

    pub fn with_overflow(mut self, o: PoolOverflow) -> Self {
        self.overflow = o;
        self
    }

    pub fn with_spill_limit(mut self, bytes: usize) -> Self {
        self.max_spill_bytes = bytes;
        self
    }

    pub fn with_spill_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.spill_dir = Some(dir.into());
        self
    }

    /// **启动期**校验 spill 目录可用，并顺手清掉上次被 kill 留下的残骸
    ///
    /// # 为什么不能推迟到"第一次池满"
    ///
    /// 原先这件事发生在 `SpillFile::open` 里 —— 那是池**第一次满**的时候。
    /// 目录写错、没权限、盘满，全都要等任务跑到一半、池第一次溢不出去才炸，
    /// 而且此时错误会伪装成"背压生效了、任务变慢"（`try_spill` 返回 false 就退背压）。
    /// 配置错误必须在**开工前**说清楚。
    ///
    /// 只在 `on_overflow: spill` 时校验：别的策略压根不碰这个目录，
    /// 为一条不会走到的路拒绝启动是另一种错。
    pub fn validate_spill_dir(&self) -> D2DResult<()> {
        if self.overflow != PoolOverflow::Spill {
            return Ok(());
        }
        let dir = match &self.spill_dir {
            Some(d) => d.clone(),
            // 缺省用系统临时目录 —— 那是每个进程都能写的，不必校验
            None => return Ok(()),
        };

        std::fs::create_dir_all(&dir).map_err(|e| {
            D2DError::config(format!(
                "{}池的 spill_dir 建不出来：{}（{}）—— \
                 请确认路径存在、权限够，或去掉 spill_dir 改用系统临时目录",
                self.name,
                dir.display(),
                e
            ))
        })?;

        // 真写一个探针文件再删掉：`create_dir_all` 成功不代表**目录可写**
        // （只读挂载、ACL 拒绝都会在这一步才现形）。
        let probe = dir.join(format!(".d2d-spill-probe-{}", std::process::id()));
        let write_ok = std::fs::write(&probe, b"d2d").and_then(|_| std::fs::remove_file(&probe));
        if let Err(e) = write_ok {
            let _ = std::fs::remove_file(&probe);
            return Err(D2DError::config(format!(
                "{}池的 spill_dir 不可写：{}（{}）—— \
                 配了 on_overflow: spill 就必须有个能写的目录",
                self.name,
                dir.display(),
                e
            )));
        }

        let swept = sweep_stale_spill_files(&dir, self.name);
        if swept > 0 {
            tracing::info!(
                dir = %dir.display(),
                files = swept,
                "清掉了上次异常退出留下的 spill 文件"
            );
        }
        Ok(())
    }
}

/// 扫掉这个目录里**属于已死进程**的陈旧 spill 文件，返回清掉几个
///
/// # 为什么要它
///
/// spill 文件的正常清理有两条路：磁盘区排空时的 `recycle()`，和持有时
/// [`DataPool`] 析构时的删除。两条都走 **Rust 的 Drop** —— 进程被 `kill -9`
/// （或宿主崩溃、容器被拉掉）时一个都不执行，文件就留在盘上了。
/// 文件名里带 pid，正是为了在这里认回来。
///
/// # 只清"确定不是活的"那些
///
/// 判活用 `/proc/{pid}`：这个项目真正跑批的是 Linux（见 基准实测）。
/// 别的平台没有可靠且免依赖的判活手段，**就一个都不清** —— 错删别人正在用的
/// spill 文件会把数据搞坏，而残留只是占盘。宁可留着。
fn sweep_stale_spill_files(dir: &Path, name: &str) -> usize {
    if !cfg!(target_os = "linux") {
        return 0;
    }
    let prefix = format!("d2d-spill-{}-", name);
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut n = 0;
    for e in entries.flatten() {
        let fname = e.file_name();
        let Some(fname) = fname.to_str() else {
            continue;
        };
        let Some(rest) = fname.strip_prefix(&prefix) else {
            continue;
        };
        let Some(pid) = rest
            .strip_suffix(".bin")
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == std::process::id() || pid_is_alive(pid) {
            continue;
        }
        if std::fs::remove_file(e.path()).is_ok() {
            n += 1;
        }
    }
    n
}

/// 这个 pid 还活着吗（Linux：看 `/proc`）
fn pid_is_alive(pid: u32) -> bool {
    Path::new(&format!("/proc/{}", pid)).exists()
}

/// 池的全程读数
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PoolStats {
    pub name: String,
    /// 排队占用的内存峰值
    pub peak_bytes: usize,
    pub capacity_bytes: usize,
    /// 溢出到磁盘的元素数与字节数
    pub spilled_items: u64,
    pub spilled_bytes: u64,
    /// 上游因池满而等待的次数（背压真实生效的证据）
    pub waits: u64,
    /// 仍在内存里排队的元素数
    pub queued_mem: usize,
    /// 仍在磁盘上等待读回的元素数
    pub queued_disk: usize,
}

impl std::fmt::Display for PoolStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}池 峰值 {}/{} MB · 排队 {} 内存 + {} 磁盘 · 溢出 {} 项/{} MB · 背压 {} 次",
            self.name,
            self.peak_bytes / 1048576,
            self.capacity_bytes / 1048576,
            self.queued_mem,
            self.queued_disk,
            self.spilled_items,
            self.spilled_bytes / 1048576,
            self.waits,
        )
    }
}

// ─────────────────────────── 池本体 ───────────────────────────

enum Slot<T> {
    /// 还在内存里
    Mem(T),
    /// 已落到 spill 文件的 `[offset, offset+len)`
    Disk { offset: u64, len: u64 },
}

struct SpillFile {
    path: PathBuf,
    /// 追加写
    wr: File,
    /// 按偏移读
    rd: File,
    /// 已写入的总长度（下一个记录从这里开始）
    len: u64,
}

impl SpillFile {
    fn open(dir: Option<&Path>, name: &str) -> D2DResult<Self> {
        let dir = match dir {
            Some(d) => d.to_path_buf(),
            None => std::env::temp_dir(),
        };
        std::fs::create_dir_all(&dir)
            .map_err(|e| D2DError::config(format!("spill 目录 {} 不可用: {}", dir.display(), e)))?;
        let path = dir.join(format!("d2d-spill-{}-{}.bin", name, std::process::id()));
        let wr = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| {
                D2DError::config(format!("建 spill 文件 {} 失败: {}", path.display(), e))
            })?;
        let rd = OpenOptions::new().read(true).open(&path).map_err(|e| {
            D2DError::config(format!("打开 spill 文件 {} 失败: {}", path.display(), e))
        })?;
        tracing::debug!(path = %path.display(), "spill 文件已建立");
        Ok(Self {
            path,
            wr,
            rd,
            len: 0,
        })
    }

    /// 追加一条记录，返回 `(offset, len)`
    fn append(&mut self, item: &dyn Fn(&mut dyn Write) -> D2DResult<()>) -> D2DResult<(u64, u64)> {
        let offset = self.len;
        let mut counter = ByteCounter {
            inner: &mut self.wr,
            n: 0,
        };
        item(&mut counter)?;
        let n = counter.n;
        self.len += n;
        Ok((offset, n))
    }

    fn read_at(&mut self, offset: u64, len: u64) -> D2DResult<Box<dyn Read + '_>> {
        self.rd
            .seek(SeekFrom::Start(offset))
            .map_err(|e| D2DError::internal(format!("spill: seek 失败: {}", e)))?;
        Ok(Box::new((&mut self.rd).take(len)))
    }

    /// 把文件截回 `to` —— 撤销一次刚写完的追加
    ///
    /// 给"写之前估不准、写之后按真实长度核对"那条路用（见 [`DataPool::try_spill`] 的
    /// 上限核对）。`set_len` 之后必须把写指针也挪回去，否则下一次追加会从
    /// 旧的末尾开始写，中间留下一段空洞。
    fn rollback_to(&mut self, to: u64) -> D2DResult<()> {
        self.wr
            .set_len(to)
            .map_err(|e| D2DError::internal(format!("spill: 回滚截断失败: {}", e)))?;
        self.wr
            .seek(SeekFrom::Start(to))
            .map_err(|e| D2DError::internal(format!("spill: 回滚 seek 失败: {}", e)))?;
        self.len = to;
        Ok(())
    }

    /// 磁盘区已排空 → 从头复用（否则 spill 文件会一路长到总数据量）
    fn recycle(&mut self) -> D2DResult<()> {
        self.wr
            .set_len(0)
            .map_err(|e| D2DError::internal(format!("spill: 截断失败: {}", e)))?;
        self.wr
            .seek(SeekFrom::Start(0))
            .map_err(|e| D2DError::internal(format!("spill: 回绕失败: {}", e)))?;
        self.len = 0;
        tracing::debug!(path = %self.path.display(), "spill 文件已回收");
        Ok(())
    }
}

/// 数着写了多少字节的 Write 包装（spill 记录长度要记进队列索引）
struct ByteCounter<'a> {
    inner: &'a mut File,
    n: u64,
}

impl Write for ByteCounter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.n += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// 从 spill 文件读回一个元素，并在磁盘区排空时回收文件
fn take_disk<T: SpillItem>(g: &mut Inner<T>, offset: u64, len: u64, name: &str) -> D2DResult<T> {
    g.disk_bytes = g.disk_bytes.saturating_sub(len);
    g.disk_slots -= 1;
    let out = {
        let sf = g
            .spill
            .as_mut()
            .expect("队列里有磁盘元素却没有 spill 文件 —— 状态机出错");
        match sf.read_at(offset, len) {
            Ok(mut r) => T::unspill_from(&mut r),
            Err(e) => Err(e),
        }
    };
    if g.disk_slots == 0 {
        if let Some(sf) = g.spill.as_mut() {
            if let Err(e) = sf.recycle() {
                tracing::warn!(pool = name, error = %e, "spill 文件回收失败");
            }
        }
    }
    out
}

struct Inner<T> {
    slots: VecDeque<Slot<T>>,
    mem_bytes: usize,
    /// 磁盘上未消费的字节数（含记录头）
    disk_bytes: u64,
    disk_slots: usize,
    closed: bool,
    spill: Option<SpillFile>,
}

/// 有界缓冲池。`Arc<Self>` 共享，生产者 [`Self::push`]，消费者 [`Self::pop`]。
///
/// # 两个信号量，一条不变量
///
/// ```text
/// items 的可用凭证数  ==  队列长度
/// mem   的可用额度    ==  容量 - 队列里占的内存
/// ```
///
/// 容量本身是 usize，可以超过 4GiB；只有**单个元素**的 acquire 受 u32 限制。
///
/// 两者都在 `inner` 那把锁**里面**一起增减，所以不变量永远成立 --
/// “拿到一个 items 凭证”就等于“队里一定有一件货”。
///
/// # 为什么是 `Semaphore` 而不是 `Notify`
///
/// `Notify` 的许可会被 `Notified::enable()` 吃掉。而消费者那边
/// 「取数据」要和「超时落盘」用 `tokio::select!` 赛跑，输掉的分支 future 会被
/// **drop** -- 已经 enable 的许可跟着蒸发，于是队里有货却没人被叫醒，整条链路挂死。
/// `Semaphore::acquire` / `acquire_many` **是取消安全的**：取消即归还，不会丢。
///
/// 顺带解决了惊群：放进去/取走**一件**货就只唤醒**一个**等待者。
/// 用 `notify_waiters` 时一个位置要叫醒池边上所有等待者（实测 16 个），
/// 那一项在 1915 万行那一档把背压策略从 83 s 拖到 175 s。
pub struct DataPool<T> {
    cfg: PoolConfig,
    inner: Mutex<Inner<T>>,
    /// 队列长度（入队 `add_permits(1)`，出队消耗一个）
    items: Arc<Semaphore>,
    /// 剩余内存额度（按字节计；容量是 usize，可以超过 4GiB）
    mem: Arc<Semaphore>,
    peak: AtomicUsize,
    spilled_items: AtomicU64,
    spilled_bytes: AtomicU64,
    waits: AtomicU64,
    /// 写完才发现真实长度越限、被撤掉的次数（见 [`Self::try_spill`]）
    spill_rollbacks: AtomicU64,
}

/// 单个元素大于整个池容量 —— 等多久都放不下，必须显式失败而不是挂死
fn too_big_err(name: &str, bytes: usize, cap: usize) -> D2DError {
    D2DError::internal(format!(
        "{}池：单个元素 {} MB 就超过了池容量 {} MB。\
         这不是「暂时拥挤」，等多久都放不下 —— 请调大 flow.*.pool.capacity。",
        name,
        bytes / 1048576,
        cap / 1048576
    ))
}

impl<T: SpillItem> DataPool<T> {
    pub fn new(cfg: PoolConfig) -> Arc<Self> {
        // `Semaphore::new` 的容量是 usize —— 池容量本身可以超过 4GiB。
        // 只有单次 `acquire_many` 是 u32，所以限制的是**单个元素**（见 push），
        // 不是池容量。旧版在 4GiB 处硬 assert 是过度保守。
        Arc::new(Self {
            inner: Mutex::new(Inner {
                slots: VecDeque::new(),
                mem_bytes: 0,
                disk_bytes: 0,
                disk_slots: 0,
                closed: false,
                spill: None,
            }),
            items: Arc::new(Semaphore::new(0)),
            mem: Arc::new(Semaphore::new(cfg.capacity_bytes)),
            cfg,
            peak: AtomicUsize::new(0),
            spilled_items: AtomicU64::new(0),
            spilled_bytes: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            spill_rollbacks: AtomicU64::new(0),
        })
    }

    pub fn name(&self) -> &'static str {
        self.cfg.name
    }

    pub fn capacity_bytes(&self) -> usize {
        self.cfg.capacity_bytes
    }

    pub fn overflow(&self) -> PoolOverflow {
        self.cfg.overflow
    }

    /// 当前排队占用的内存
    pub fn used_bytes(&self) -> usize {
        self.inner.lock().unwrap().mem_bytes
    }

    /// 关闭：不再接受 `push`；已排队的数据仍可被取走，取完 `pop` 返回 `None`
    pub fn close(&self) {
        self.inner.lock().unwrap().closed = true;
        // 关闭信号量 -> 所有等在 acquire 上的任务立刻拿到 Err，回去把剩下的取完
        self.items.close();
        self.mem.close();
    }

    /// 入池。池满时按 [`PoolOverflow`] 处置。
    pub async fn push(&self, item: T) -> D2DResult<()> {
        let bytes = item.heap_bytes();
        // 单个元素比整个池还大 -> 等多久都放不下，必须显式失败而不是挂死
        if bytes > self.cfg.capacity_bytes {
            return Err(too_big_err(self.cfg.name, bytes, self.cfg.capacity_bytes));
        }
        // 单次 acquire_many 是 u32；单个元素超过 4GiB 必须拆批，不能静默截断
        let n = u32::try_from(bytes).map_err(|_| {
            D2DError::internal(format!(
                "{}池：单个元素 {} MB 超过单次信号量 acquire 的 u32 上限（4 GiB）；\
                 请把该批拆小（池容量本身可以超过 4GiB）",
                self.cfg.name,
                bytes / 1048576
            ))
        })?;

        // ① 争取内存额度
        let permit = match self.cfg.overflow {
            PoolOverflow::Fail => match self.mem.clone().try_acquire_many_owned(n) {
                Ok(p) => p,
                Err(_) => {
                    return Err(D2DError::internal(format!(
                        "{}池已满（{} MB / {} MB）且策略为 fail",
                        self.cfg.name,
                        self.used_bytes() / 1048576,
                        self.cfg.capacity_bytes / 1048576
                    )))
                }
            },
            PoolOverflow::Spill => match self.mem.clone().try_acquire_many_owned(n) {
                Ok(p) => p,
                Err(_) => {
                    // ② 溢到磁盘；盘也到顶了才回来背压
                    if self.try_spill(&item)? {
                        return Ok(());
                    }
                    self.acquire_mem(n).await?
                }
            },
            PoolOverflow::Backpressure => self.acquire_mem(n).await?,
        };

        // ③ 入队
        //
        // `permit.forget()` 把额度**移交给队列**：这批字节从此算在池账上，
        // 直到某个消费者取走才由 [`Self::take_front`] 还回去。
        // 队列长度与额度在同一把锁里一起变 -- 这是 items 信号量能当凭证用的前提。
        let mut g = self.inner.lock().unwrap();
        if g.closed {
            return Err(D2DError::internal(format!(
                "{}池已关闭，不能再写入",
                self.cfg.name
            )));
        }
        g.mem_bytes += bytes;
        g.slots.push_back(Slot::Mem(item));
        self.note_peak(g.mem_bytes);
        permit.forget();
        self.items.add_permits(1);
        Ok(())
    }

    /// 把一个元素写进 spill 文件并挂到队列尾。返回 `false` = 磁盘额度不够，没写。
    ///
    /// # 上限是**按落盘后的真实长度**算的，不是按 `heap_bytes()` 估的
    ///
    /// 曾经这里只做 `disk_bytes + heap_bytes() + 16 > max_spill` 这一个估算判定，
    /// 而 `heap_bytes()` 是**内存**里的估算（Arrow 的 `estimated_memory_size()`），
    /// 落盘要额外加 schema 与消息帧 —— 那个 `+16` 差得不是一点：
    /// 实测一张 200 列的批次 `heap 122552 B → spill 136456 B`，**多出 13904 B**
    /// （约 70 B/列，见 `test_spill_length_can_exceed_heap_estimate`）。
    /// 常数写小了，于是"单次溢出可以越过上限"，`max_spill` 不再是硬的。
    ///
    /// 现在两道：
    ///
    /// 1. **写之前**用 `heap_bytes()` 挡一道：连"落盘至少这么大"都放不下就没必要写。
    ///    它只是省掉明显没戏的写盘，**不是**保证 —— `heap_bytes()` 估的是内存占用，
    ///    落盘还要加 schema 与消息帧，真实长度可以**大于**它。
    /// 2. **写之后**用 `append` 返回的**真实长度**核对，超了就 `rollback_to` 撤掉这次追加。
    ///    这一条才是保证：`disk_bytes <= max_spill_bytes` 由它兜住。
    ///
    /// ⚠️ 第 1 条**不能**改成"估算 + 一个大余量"（曾经试过 +64 KB）：那等于给每次
    /// 溢出都预留 64 KB，`max_spill` 配得比 64 KB 小时**永远溢不出去** ——
    /// 于是每一件都退化成背压，表现是"任务卡住不动"，而配置看着完全正常。
    /// 余量估大了不会更安全，只会把功能关掉。
    fn try_spill(&self, item: &T) -> D2DResult<bool> {
        let bytes = item.heap_bytes() as u64;
        let max = self.cfg.max_spill_bytes as u64;
        let mut g = self.inner.lock().unwrap();
        if g.disk_bytes + bytes > max {
            return Ok(false);
        }
        // ⚠️ **写入与入队必须在同一把锁里**，不能拆成「锁外写盘、再锁内入队」。
        //
        // 拆开的话有个真实的竞态：两次加锁之间，消费者可能把磁盘区读空并触发
        // `recycle()`（截断文件 + `len` 归零）；我们手上那个 `offset` 随即作废，
        // 下次 append 会从 0 开始覆盖它 -- 于是这条记录被读成垃圾。
        if g.spill.is_none() {
            g.spill = Some(SpillFile::open(
                self.cfg.spill_dir.as_deref(),
                self.cfg.name,
            )?);
        }
        // 借用收在一对花括号里：下面要读 `g.disk_bytes`，而它会被 `sf` 的可变借用挡住
        let (before, offset, len) = {
            let sf = g.spill.as_mut().unwrap();
            let before = sf.len;
            let (offset, len) = sf.append(&|w| item.spill_to(w))?;
            (before, offset, len)
        };
        if g.disk_bytes + len > max {
            // 估算放行了、真实长度放不下 —— 撤掉这次追加，交回背压
            g.spill.as_mut().unwrap().rollback_to(before)?;
            self.spill_rollbacks.fetch_add(1, Ordering::Relaxed);
            return Ok(false);
        }
        g.disk_bytes += len;
        g.disk_slots += 1;
        g.slots.push_back(Slot::Disk { offset, len });
        self.spilled_items.fetch_add(1, Ordering::Relaxed);
        self.spilled_bytes.fetch_add(len, Ordering::Relaxed);
        self.items.add_permits(1);
        Ok(true)
    }

    /// 等一块内存额度（**取消安全**：future 被 drop 时已拿到的额度会还回去）
    async fn acquire_mem(&self, n: u32) -> D2DResult<OwnedSemaphorePermit> {
        if let Ok(p) = self.mem.clone().try_acquire_many_owned(n) {
            return Ok(p);
        }
        let nth = self.waits.fetch_add(1, Ordering::Relaxed) + 1;
        if nth == 1 {
            tracing::debug!(
                pool = self.cfg.name,
                capacity_mb = self.cfg.capacity_bytes / 1048576,
                "池已满，上游开始背压"
            );
        }
        self.mem.clone().acquire_many_owned(n).await.map_err(|_| {
            D2DError::internal(format!("{}池已关闭，等待额度的写入被中止", self.cfg.name))
        })
    }

    /// 从队首取一件。
    ///
    /// `permit_held`：调用方**已经**消耗了这件货的 `items` 凭证
    /// （信号量路径走这条），于是这里不再重复取；
    /// `false` 表示由本函数自己取凭证。
    /// 关闭之后凭证机制不再可用，排空走 `permit_held = true` 且不经信号量。
    fn take_front(&self, permit_held: bool) -> Option<D2DResult<T>> {
        if !permit_held {
            match self.items.clone().try_acquire() {
                Ok(p) => {
                    p.forget();
                }
                Err(_) => return None, // 没凭证 = 没货（或池已关闭）
            }
        }
        let mut g = self.inner.lock().unwrap();
        match g.slots.pop_front() {
            Some(Slot::Mem(item)) => {
                let n = item.heap_bytes();
                g.mem_bytes = g.mem_bytes.saturating_sub(n);
                drop(g);
                self.mem.add_permits(n); // 额度还给池，别的生产者立刻能写
                Some(Ok(item))
            }
            Some(Slot::Disk { offset, len }) => {
                // 磁盘元素不占内存额度，取走它不必归还任何东西
                Some(take_disk(&mut g, offset, len, self.cfg.name))
            }
            None => {
                // 凭证多于货 -- 只可能出现在关闭竞态里，补回去别把状态搞坏
                if !permit_held {
                    self.items.add_permits(1);
                }
                None
            }
        }
    }

    /// 出池。返回 `None` = 池已关闭且**已排空**。
    pub async fn pop(&self) -> Option<D2DResult<T>> {
        loop {
            // ① 先试着直接取（绝大多数情况走这条，零等待）
            if let Some(out) = self.take_front(false) {
                return Some(out);
            }
            // ② 空：池关了吗？
            //
            // ⚠️ guard 必须在调 `take_front` **之前**释放，
            // 否则 std 的 Mutex 不可重入 -> 自己把自己锁死。
            let closed = {
                let g = self.inner.lock().unwrap();
                g.closed
            };
            if closed {
                return self.take_front(true); // 关闭后排空
            }
            // ③ 等“有货”。acquire 取消安全；拿到凭证后这件货就一定在。
            match self.items.acquire().await {
                Ok(p) => p.forget(),
                Err(_) => continue, // 池关了 -> 回顶部走排空
            }
            if let Some(out) = self.take_front(true) {
                return Some(out);
            }
            // 理论上到不了这里（凭证与队列长度恒等）；补回凭证再来一轮
            self.items.add_permits(1);
        }
    }

    /// 非阻塞出池：池空时立刻返回 `None`
    ///
    /// ⚠️ 关闭之后 `items` 信号量的凭证取不出来（`try_acquire` 一律返回 `Err`），
    /// 所以这里必须像 `pop` 一样补一条排空分支 —— 否则**源端一读完就把池关了**，
    /// 下游的`try_pop`（攒批用）从此一件都拿不到，攒批静默失效。
    pub fn try_pop(&self) -> Option<D2DResult<T>> {
        match self.take_front(false) {
            Some(out) => Some(out),
            None => {
                let closed = {
                    let g = self.inner.lock().unwrap();
                    g.closed
                };
                if closed {
                    self.take_front(true)
                } else {
                    None
                }
            }
        }
    }

    /// 池是否已关闭且排空（消费者据此收工）
    pub fn is_closed_empty(&self) -> bool {
        let g = self.inner.lock().unwrap();
        g.closed && g.slots.is_empty()
    }

    /// 仍排在池里的元素数（内存 + 磁盘）
    pub fn queued(&self) -> usize {
        self.inner.lock().unwrap().slots.len()
    }

    pub fn stats(&self) -> PoolStats {
        let g = self.inner.lock().unwrap();
        let mem = g.slots.iter().filter(|s| matches!(s, Slot::Mem(_))).count();
        PoolStats {
            name: self.cfg.name.to_string(),
            peak_bytes: self.peak.load(Ordering::Relaxed),
            capacity_bytes: self.cfg.capacity_bytes,
            spilled_items: self.spilled_items.load(Ordering::Relaxed),
            spilled_bytes: self.spilled_bytes.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            queued_mem: mem,
            queued_disk: g.disk_slots,
        }
    }

    fn note_peak(&self, used: usize) {
        self.peak.fetch_max(used, Ordering::Relaxed);
    }
}

impl<T> Drop for DataPool<T> {
    fn drop(&mut self) {
        if let Ok(g) = self.inner.lock() {
            if let Some(sf) = &g.spill {
                let _ = std::fs::remove_file(&sf.path);
                tracing::debug!(path = %sf.path.display(), "spill 文件已删除");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    const KB: usize = 1024;

    /// 测试载荷：一块可校验的字节
    #[derive(Debug, PartialEq, Eq)]
    struct Blob {
        id: u32,
        bytes: Vec<u8>,
    }

    impl Blob {
        fn new(id: u32, size: usize) -> Self {
            Self {
                id,
                bytes: vec![id as u8; size],
            }
        }
    }

    impl SpillItem for Blob {
        fn heap_bytes(&self) -> usize {
            self.bytes.len()
        }
        fn spill_to(&self, w: &mut dyn Write) -> D2DResult<()> {
            write_u64(w, self.id as u64)?;
            write_u64(w, self.bytes.len() as u64)?;
            w.write_all(&self.bytes)
                .map_err(|e| D2DError::internal(e.to_string()))
        }
        fn unspill_from(r: &mut dyn Read) -> D2DResult<Self> {
            let id = read_u64(r)? as u32;
            let len = read_u64(r)? as usize;
            let mut bytes = vec![0u8; len];
            r.read_exact(&mut bytes)
                .map_err(|e| D2DError::internal(e.to_string()))?;
            Ok(Self { id, bytes })
        }
    }

    /// 容量超过 4GiB 不再是硬上限：额度单位自动放大，小容量仍逐字节
    #[cfg(target_pointer_width = "64")]
    #[tokio::test]
    async fn test_pool_capacity_above_4gib_is_allowed() {
        let cap = 5usize * 1024 * 1024 * 1024;
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", cap).with_overflow(PoolOverflow::Backpressure),
        );
        assert_eq!(p.capacity_bytes(), cap);
        p.push(Blob::new(1, 3 * KB)).await.unwrap();
        let got = p.pop().await.unwrap().unwrap();
        assert_eq!(got.id, 1);
        assert_eq!(p.used_bytes(), 0);
    }

    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let d = std::env::temp_dir().join(format!(
            "d2d-pool-test-{}-{}-{}",
            tag,
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    // ───────── 基本语义 ─────────

    #[tokio::test]
    async fn test_fifo_order_is_preserved() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", 1024 * KB));
        for i in 0..10 {
            p.push(Blob::new(i, 10)).await.unwrap();
        }
        for i in 0..10 {
            assert_eq!(p.pop().await.unwrap().unwrap().id, i, "必须先进先出");
        }
    }

    /// **核心不变量**：无论溢出与否，元素一个不丢、顺序不乱
    #[tokio::test]
    async fn test_spill_preserves_every_item_and_order() {
        let dir = tmpdir("order");
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", 4 * KB)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        // 30 个 1KB 元素，容量只有 4KB → 必然溢出
        for i in 0..30 {
            p.push(Blob::new(i, KB)).await.unwrap();
        }
        assert!(p.stats().spilled_items > 0, "这个配置必须触发溢出");
        for i in 0..30 {
            let got = p.pop().await.unwrap().unwrap();
            assert_eq!(got.id, i, "溢出不得打乱顺序");
            assert_eq!(got.bytes.len(), KB);
            assert!(got.bytes.iter().all(|b| *b == i as u8), "载荷不得被篡改");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 内存占用**任何时刻**都不超过容量 —— 否则池就没有意义了
    #[tokio::test]
    async fn test_memory_never_exceeds_capacity_under_spill() {
        let dir = tmpdir("cap");
        let cap = 8 * KB;
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", cap)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        for i in 0..100 {
            p.push(Blob::new(i, KB)).await.unwrap();
            assert!(p.used_bytes() <= cap, "第 {} 次入池后内存超限", i);
        }
        assert_eq!(p.stats().peak_bytes, cap, "峰值应当正好顶到容量");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── 背压 ─────────

    #[tokio::test]
    async fn test_backpressure_blocks_producer_until_consumer_takes() {
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", 4 * KB).with_overflow(PoolOverflow::Backpressure),
        );
        for _ in 0..4 {
            p.push(Blob::new(0, KB)).await.unwrap();
        }
        assert_eq!(p.used_bytes(), 4 * KB);

        // 第 5 个必须卡住
        let p2 = Arc::clone(&p);
        let blocked = tokio::spawn(async move { p2.push(Blob::new(99, KB)).await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(!blocked.is_finished(), "池满时上游必须等待，不能撑破容量");

        // 取走一个 → 立刻放行
        let _ = p.pop().await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), blocked)
            .await
            .expect("下游释放后上游应当立刻被唤醒")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_fail_policy_errors_instead_of_waiting() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", KB).with_overflow(PoolOverflow::Fail));
        p.push(Blob::new(0, KB)).await.unwrap();
        let e = p.push(Blob::new(1, KB)).await.unwrap_err().to_string();
        assert!(e.contains("已满"), "{}", e);
    }

    /// **回归**：spill 有上限，写满后退化为背压而不是把磁盘吃光
    ///
    /// 这条守的是"OOM 不要只是被搬到磁盘上"：没有上限的话，
    /// 下游一慢就一路写盘，直到磁盘满 —— 而磁盘满的表现比内存慢一步，更难归因。
    #[tokio::test]
    async fn test_spill_capacity_is_bounded_then_falls_back_to_backpressure() {
        let dir = tmpdir("limit");
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", KB)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_limit(4 * KB)
                .with_spill_dir(&dir),
        );
        // 每条记录落在 spill 文件里是 1040 字节（id 8 + 长度 8 + 载荷 1024）。
        // 容量 1KB 只装得下 1 条，spill 上限 4KB 只放得下 3 条 → 一共 4 条入池。
        for i in 0..4 {
            p.push(Blob::new(i, KB)).await.unwrap();
        }
        assert_eq!(p.stats().spilled_items, 3);

        // 第 5 条：内存满 + 磁盘到顶 → 应当**等**，而不是无限制写盘
        let p2 = Arc::clone(&p);
        let blocked = tokio::spawn(async move { p2.push(Blob::new(100, KB)).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!blocked.is_finished(), "spill 到顶后必须退化为背压");

        // 取走一条 → 腾出内存 → 立刻放行（落进内存，不再写盘）
        drop(p.pop().await);
        tokio::time::timeout(std::time::Duration::from_secs(2), blocked)
            .await
            .expect("腾出空间后应当放行")
            .unwrap()
            .unwrap();
        assert_eq!(
            p.stats().spilled_items,
            3,
            "放行的这一条该进内存，不该再写盘"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── 收工语义 ─────────

    /// 关闭后：已排队的还能取完，取完 `pop` 返回 None
    #[tokio::test]
    async fn test_close_drains_then_returns_none() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", 64 * KB));
        for i in 0..3 {
            p.push(Blob::new(i, KB)).await.unwrap();
        }
        p.close();
        for i in 0..3 {
            assert_eq!(
                p.pop().await.unwrap().unwrap().id,
                i,
                "关闭不得丢掉已排队的数据"
            );
        }
        assert!(p.pop().await.is_none(), "排空且关闭后必须返回 None");
    }

    /// **回归**：关闭之后 `try_pop` 仍要把排队的取完
    ///
    /// 信号量一旦 `close()`，`try_acquire` 一律返回 `Err` —— 凭证取不出来了，
    /// 但队列里剩下的货还得让消费者拿走。少了这条排空分支，**攒批会静默失效**：
    /// 源头一读完就关池，下游的 `try_pop`（攒批用）从此一件都拿不到，
    /// 表现是"配了 batch_bytes 却一点没攒起来"，而且不报任何错。
    #[tokio::test]
    async fn test_try_pop_still_drains_after_close() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", 64 * KB));
        for i in 0..3 {
            p.push(Blob::new(i, KB)).await.unwrap();
        }
        p.close();
        for i in 0..3 {
            let got = p.try_pop().expect("关闭后排队的数据仍要能被取走").unwrap();
            assert_eq!(got.id, i);
        }
        assert!(p.try_pop().is_none(), "取完了就该是 None");
    }

    #[tokio::test]
    async fn test_push_after_close_is_rejected() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", 64 * KB));
        p.close();
        assert!(p.push(Blob::new(0, KB)).await.is_err());
    }

    /// 关闭时**正在等待**的消费者必须被唤醒（否则收工时会挂住）
    #[tokio::test]
    async fn test_close_wakes_blocked_consumer() {
        let p = DataPool::<Blob>::new(PoolConfig::new("t", 64 * KB));
        let p2 = Arc::clone(&p);
        let waiter = tokio::spawn(async move { p2.pop().await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        p.close();
        let r = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .expect("关闭必须唤醒等待中的消费者");
        assert!(r.unwrap().is_none());
    }

    // ───────── spill 文件回收 ─────────

    /// 磁盘区排空后文件必须回收，否则 spill 文件会一路长到总数据量
    #[tokio::test]
    async fn test_spill_file_is_recycled_when_drained() {
        let dir = tmpdir("recycle");
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", KB)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        for i in 0..20 {
            p.push(Blob::new(i, KB)).await.unwrap();
        }
        let spilled = p.stats().spilled_bytes;
        assert!(spilled > 10 * KB as u64, "应当溢出不少: {}", spilled);

        // 全部取走
        for i in 0..20 {
            assert_eq!(p.pop().await.unwrap().unwrap().id, i);
        }
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1, "spill 文件应当还在（池还活着）");
        let size = files[0].metadata().unwrap().len();
        assert_eq!(
            size, 0,
            "磁盘区排空后文件应当被截断回收，实际 {} 字节",
            size
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 池析构时 spill 文件必须删掉（不然每跑一次任务就在磁盘上留一坨）
    #[tokio::test]
    async fn test_spill_file_removed_on_drop() {
        let dir = tmpdir("drop");
        {
            let p = DataPool::<Blob>::new(
                PoolConfig::new("t", KB)
                    .with_overflow(PoolOverflow::Spill)
                    .with_spill_dir(&dir),
            );
            p.push(Blob::new(0, KB)).await.unwrap();
            p.push(Blob::new(1, KB)).await.unwrap(); // 触发 spill
            assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        }
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "池析构后 spill 文件必须被清理"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── 并发 ─────────

    /// **回归**：多生产者多消费者在紧张容量下不得死锁，且一个不丢
    ///
    /// 这个形状正是线上那次"内存额度被等待者自己占满"的死锁的缩小版。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_push_pop_does_not_deadlock() {
        const PRODUCERS: u32 = 8;
        const PER: u32 = 50;
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", 4 * KB).with_overflow(PoolOverflow::Backpressure),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));

        let mut prods = Vec::new();
        for pid in 0..PRODUCERS {
            let p = Arc::clone(&p);
            prods.push(tokio::spawn(async move {
                for i in 0..PER {
                    p.push(Blob::new(pid * PER + i, KB)).await.unwrap();
                }
            }));
        }
        let consumer = {
            let p = Arc::clone(&p);
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                while let Some(item) = p.pop().await {
                    seen.lock().unwrap().push(item.unwrap().id);
                }
            })
        };
        let closer = {
            let p = Arc::clone(&p);
            tokio::spawn(async move {
                futures::future::join_all(prods).await;
                p.close();
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            closer.await.unwrap();
            consumer.await.unwrap();
        })
        .await
        .expect("并发读写死锁了");

        let mut got = seen.lock().unwrap().clone();
        got.sort_unstable();
        let mut want: Vec<u32> = (0..PRODUCERS * PER).collect();
        want.sort_unstable();
        assert_eq!(got, want, "并发下不得丢元素");
        assert_eq!(p.used_bytes(), 0, "收工后内存账必须归零");
    }

    /// **回归**：溢出模式下并发读写同样一个不丢
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_spill_does_not_lose_items() {
        let dir = tmpdir("conc-spill");
        let p = DataPool::<Blob>::new(
            PoolConfig::new("t", 4 * KB)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));

        let prod = {
            let p = Arc::clone(&p);
            tokio::spawn(async move {
                for i in 0..200u32 {
                    p.push(Blob::new(i, KB)).await.unwrap();
                }
                p.close();
            })
        };
        // 两个消费者同时读，验证磁盘区的偏移索引在多消费者下依然正确
        let mut cons = Vec::new();
        for _ in 0..2 {
            let p = Arc::clone(&p);
            let seen = Arc::clone(&seen);
            cons.push(tokio::spawn(async move {
                while let Some(item) = p.pop().await {
                    seen.lock().unwrap().push(item.unwrap().id);
                }
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            prod.await.unwrap();
            futures::future::join_all(cons).await;
        })
        .await
        .expect("溢出模式下并发读写死锁了");

        let mut got = seen.lock().unwrap().clone();
        got.sort_unstable();
        assert_eq!(got, (0..200u32).collect::<Vec<_>>(), "溢出模式下不得丢元素");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── 载荷实现 ─────────

    /// RecordBatch 溢出后必须**逐位相同**（schema、null、精度全保留）
    #[tokio::test]
    async fn test_record_batch_round_trip_is_lossless() {
        use arrow::array::{Float64Array, Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;

        let schema = StdArc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
                StdArc::new(StringArray::from(vec![Some("a"), None, Some("ccc")])),
                StdArc::new(Float64Array::from(vec![Some(1.5), Some(-0.25), None])),
            ],
        )
        .unwrap();

        let dir = tmpdir("ipc");
        // 容量正好放得下**一个**批次 → 第二个必然溢出。
        // （单个元素比整个池还大现在会直接报错，那是刻意的：等多久都放不下。）
        let cap = batch.estimated_memory_size();
        let p = DataPool::<arrow::array::RecordBatch>::new(
            PoolConfig::new("t", cap)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        p.push(batch.clone()).await.unwrap();
        p.push(batch.clone()).await.unwrap();
        assert_eq!(p.stats().spilled_items, 1, "第二个批次应当溢出到磁盘");
        let back = p.pop().await.unwrap().unwrap();
        assert_eq!(back, batch, "Arrow IPC 往返必须逐位一致");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 直通载荷溢出后同样逐位一致（含行数）
    #[tokio::test]
    async fn test_physical_block_round_trip_is_lossless() {
        let dir = tmpdir("blk");
        let blk = d2d_connector::PhysicalBlock {
            bytes: b"1|a\n2|b\n".to_vec(),
            rows: 2,
        };
        // 容量正好放得下一个块 —— 第二个必然溢出，且是**字节原样**落盘（不是 Arrow IPC）
        let p = DataPool::<d2d_connector::PhysicalBlock>::new(
            PoolConfig::new("t", blk.bytes.len())
                .with_overflow(PoolOverflow::Spill)
                .with_spill_dir(&dir),
        );
        p.push(blk.clone()).await.unwrap();
        p.push(blk.clone()).await.unwrap();
        assert_eq!(p.stats().spilled_items, 1);
        assert_eq!(p.pop().await.unwrap().unwrap().bytes, blk.bytes);
        let back = p.pop().await.unwrap().unwrap();
        assert_eq!(back.bytes, blk.bytes);
        assert_eq!(back.rows, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── max_spill 是硬上限（P4-1 第 1 条）─────────

    /// 一个**落盘长度远大于 `heap_bytes()`** 的载荷
    ///
    /// 专门用来打"估算放行了、真实长度放不下"那条路：`heap_bytes` 只报 1 KB，
    /// 实际却写 `real` 字节。真实的 `RecordBatch` 也有这个性质（Arrow IPC 的
    /// schema 与消息帧），只是没这么夸张。
    struct Bulky {
        real: usize,
    }

    impl SpillItem for Bulky {
        fn heap_bytes(&self) -> usize {
            1024
        }
        fn spill_to(&self, w: &mut dyn Write) -> D2DResult<()> {
            w.write_all(&vec![7u8; self.real])
                .map_err(|e| D2DError::internal(e.to_string()))
        }
        fn unspill_from(r: &mut dyn Read) -> D2DResult<Self> {
            let mut b = Vec::new();
            r.read_to_end(&mut b)
                .map_err(|e| D2DError::internal(e.to_string()))?;
            Ok(Self { real: b.len() })
        }
    }

    /// 池容量恰好只装得下一件（`Bulky` 报 1 KB）—— 第二件必然走 spill 分支
    fn bulky_pool(dir: &Path, max_spill: usize) -> Arc<DataPool<Bulky>> {
        DataPool::<Bulky>::new(
            PoolConfig::new("t", 1024)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_limit(max_spill)
                .with_spill_dir(dir),
        )
    }

    /// **契约：`disk_bytes <= max_spill_bytes`，一次都不许越。**
    ///
    /// 修之前这里必挂：上限只按 `heap_bytes() + 16` 预估，而 `Bulky` 真实落盘
    /// 128 KB（`heap_bytes` 只报 1 KB）——一次溢出就能越过上限。
    ///
    /// 越限之后**不能**溢出成功，于是退回背压；池里没有消费者，所以那次 push
    /// 会一直等着 —— 用超时把"它在等"这件事变成可观测量，而不是让测试挂死。
    #[tokio::test]
    async fn test_spill_never_exceeds_max_spill_bytes() {
        let dir = tmpdir("maxspill");
        // max = 100 KB，真实落盘 128 KB → 预检用 heap(1 KB) 放行，写后核对必须拦下
        let p = bulky_pool(&dir, 100 * KB);
        p.push(Bulky { real: 128 * KB }).await.unwrap(); // 进内存
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            p.push(Bulky { real: 128 * KB }),
        )
        .await;
        assert!(
            blocked.is_err(),
            "越限的那件**不该**溢出成功，应当退回背压（等不到就该超时）"
        );

        let st = p.stats();
        assert_eq!(st.spilled_items, 0, "越限的不该算作溢出成功");
        assert!(
            st.spilled_bytes <= 100 * KB as u64,
            "溢出字节 {} 越过了上限 100 KB",
            st.spilled_bytes
        );

        // 回滚过的 spill 文件必须**真的缩回去**：留着被丢弃的字节，下次 append
        // 的起始位置就错了，读回来是垃圾
        if let Some(f) = spill_file_in(&dir) {
            assert_eq!(
                std::fs::metadata(&f).unwrap().len(),
                0,
                "回滚后文件应当回到追加前的长度（这里是 0）"
            );
        }
        // 数据没丢：内存里那件还在
        assert_eq!(p.pop().await.unwrap().unwrap().real, 128 * KB);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 回滚**只撤这一次追加**，已经写进去的记录要原样读得回来
    #[tokio::test]
    async fn test_rollback_keeps_earlier_records_intact() {
        let dir = tmpdir("rollback");
        // max = 200 KB：32 KB 能进，再加 150 KB 是 182 KB 也能进，再加就超
        let p = bulky_pool(&dir, 200 * KB);
        p.push(Bulky { real: 32 * KB }).await.unwrap(); // 进内存
        p.push(Bulky { real: 32 * KB }).await.unwrap(); // 池满 → 落盘 32 KB
        assert_eq!(p.stats().spilled_items, 1, "第一条应当溢出成功");
        p.push(Bulky { real: 150 * KB }).await.unwrap(); // 32+150 = 182 KB ≤ 200 KB
        assert_eq!(p.stats().spilled_items, 2);

        // 再来一件 150 KB：32+150+150 = 332 KB > 200 KB → 写后核对拦下并回滚
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            p.push(Bulky { real: 150 * KB }),
        )
        .await;
        assert!(blocked.is_err(), "越限的那件应当退回背压");
        let st = p.stats();
        assert_eq!(st.spilled_items, 2, "越限那条不该算溢出成功");
        assert!(
            st.spilled_bytes <= 200 * KB as u64,
            "越限了：{}",
            st.spilled_bytes
        );

        // 前面两件**读回来还是原样** —— 回滚没有把它们的字节一起截掉
        assert_eq!(p.pop().await.unwrap().unwrap().real, 32 * KB);
        assert_eq!(p.pop().await.unwrap().unwrap().real, 32 * KB);
        assert_eq!(p.pop().await.unwrap().unwrap().real, 150 * KB);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `heap_bytes()` **不是**落盘长度的上界 —— 这正是"写后必须核对"的理由
    ///
    /// 拿真 `RecordBatch` 量：Arrow IPC 要多写 schema 与消息帧，列越多差得越多。
    /// 这条测试记录的既是事实、也是那个设计决定的依据。
    #[tokio::test]
    async fn test_spill_length_can_exceed_heap_estimate() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        // 刻意做宽：200 列，让 schema 与消息帧的固定开销显出来
        let mut fields = Vec::new();
        let mut cols: Vec<arrow::array::ArrayRef> = Vec::new();
        for i in 0..200 {
            fields.push(Field::new(format!("c{:03}", i), DataType::Int64, true));
            cols.push(Arc::new(Int64Array::from((0..64).collect::<Vec<i64>>())));
        }
        fields.push(Field::new("s", DataType::Utf8, true));
        cols.push(Arc::new(StringArray::from(
            (0..64).map(|i| format!("value-{}", i)).collect::<Vec<_>>(),
        )));
        let batch =
            arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();

        let heap = batch.estimated_memory_size();
        let dir = tmpdir("overhead");
        // 容量按**实际批次大小**定：装得下一件、装不下两件。
        // 比 pool 容量还大的元素会被 `too_big_err` 直接拒（那是另一条已知行为，
        // 见 `test_element_larger_than_pool_fails_immediately`），这里要测的是溢出。
        let p = DataPool::<arrow::record_batch::RecordBatch>::new(
            PoolConfig::new("t", heap + heap / 2)
                .with_overflow(PoolOverflow::Spill)
                .with_spill_limit(64 * 1024 * KB)
                .with_spill_dir(&dir),
        );
        p.push(batch.clone()).await.unwrap();
        p.push(batch.clone()).await.unwrap();
        assert_eq!(p.stats().spilled_items, 1, "第二件应当溢到磁盘");

        let spilled = p.stats().spilled_bytes as usize;
        assert!(
            spilled > heap,
            "IPC 落盘（{}）居然不大于内存估算（{}）？那条估算-not-上界的前提要重看",
            spilled,
            heap
        );
        eprintln!(
            "200 列批次的落盘开销：heap {} B → spill {} B（多 {} B）",
            heap,
            spilled,
            spilled - heap
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn spill_file_in(dir: &Path) -> Option<PathBuf> {
        std::fs::read_dir(dir)
            .ok()?
            .flatten()
            .find(|e| e.file_name().to_string_lossy().starts_with("d2d-spill-"))
            .map(|e| e.path())
    }

    // ───────── spill_dir 启动期校验（P4-1 第 2 条）─────────

    #[test]
    fn test_validate_spill_dir_probes_writability() {
        let dir = tmpdir("validate").join("sub");
        let cfg = PoolConfig::new("source", KB)
            .with_overflow(PoolOverflow::Spill)
            .with_spill_dir(&dir);
        cfg.validate_spill_dir().unwrap();
        assert!(dir.exists(), "目录应当被建出来");
        // 探针文件不能留在目录里
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(leftovers.is_empty(), "探针文件没删干净: {:?}", leftovers);
    }

    /// 路径根本建不出来 → **启动期**报错，而不是等池第一次满
    #[test]
    fn test_validate_spill_dir_fails_when_path_is_a_file() {
        let dir = tmpdir("validate_file");
        let blocker = dir.join("iam-a-file");
        std::fs::write(&blocker, b"x").unwrap();
        // 把一个**文件**当目录用 → create_dir_all 失败
        let cfg = PoolConfig::new("source", KB)
            .with_overflow(PoolOverflow::Spill)
            .with_spill_dir(blocker.join("under-a-file"));
        let e = cfg.validate_spill_dir().unwrap_err().to_string();
        assert!(e.contains("spill_dir"), "{}", e);
        assert!(e.contains("建不出来"), "要说是哪一步失败: {}", e);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 不溢出的池不校验 —— 为一条**不会走到**的路拒绝启动是另一种错
    #[test]
    fn test_validate_spill_dir_skipped_when_not_spilling() {
        let dir = tmpdir("validate_skip");
        let blocker = dir.join("iam-a-file");
        std::fs::write(&blocker, b"x").unwrap();
        for ov in [PoolOverflow::Backpressure, PoolOverflow::Fail] {
            let cfg = PoolConfig::new("source", KB)
                .with_overflow(ov)
                .with_spill_dir(blocker.join("under-a-file"));
            assert!(
                cfg.validate_spill_dir().is_ok(),
                "{:?} 不该校验 spill_dir",
                ov
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    // ───────── kill 残留清理（P4-1 第 3 条）─────────

    /// 只清**确定已死**的进程留下的文件；同名前缀、别的池名、活进程的都不动
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sweep_removes_only_dead_process_files() {
        let dir = tmpdir("sweep");
        // 一个**保证不存在**的 pid（Linux 的 pid 上限远小于 u32::MAX）
        let dead = u32::MAX;
        let ours = std::process::id();

        let dead_file = dir.join(format!("d2d-spill-sink-{}.bin", dead));
        let live_file = dir.join(format!("d2d-spill-sink-{}.bin", ours));
        let other_pool = dir.join(format!("d2d-spill-source-{}.bin", dead));
        let unrelated = dir.join("not-a-spill-file.txt");
        for f in [&dead_file, &live_file, &other_pool, &unrelated] {
            std::fs::write(f, b"x").unwrap();
        }

        let n = sweep_stale_spill_files(&dir, "sink");
        assert_eq!(n, 1, "只该清掉 sink 池那个已死 pid 的文件");
        assert!(!dead_file.exists(), "已死进程的残骸应当被清掉");
        assert!(live_file.exists(), "自己进程的文件不能动");
        assert!(other_pool.exists(), "别的池名的文件不能动");
        assert!(unrelated.exists(), "不认识的文件不能动");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// 校验 spill_dir 时顺手清残骸 —— 一次启动一次，不是每次开文件
    #[cfg(target_os = "linux")]
    #[test]
    fn test_validate_spill_dir_sweeps_stale_files() {
        let dir = tmpdir("validate_sweep");
        let stale = dir.join(format!("d2d-spill-source-{}.bin", u32::MAX));
        std::fs::write(&stale, b"leftover").unwrap();

        PoolConfig::new("source", KB)
            .with_overflow(PoolOverflow::Spill)
            .with_spill_dir(&dir)
            .validate_spill_dir()
            .unwrap();
        assert!(!stale.exists(), "启动期应当把上次 kill 留下的残骸清掉");
        std::fs::remove_dir_all(&dir).ok();
    }
}
