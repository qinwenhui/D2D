//! **运行期占用申报端口** —— 连接器往这儿挂账，引擎从这儿读峰值
//!
//! # 为什么必须由连接器自己报
//!
//! 引擎量不到连接器内部：ODBC 的行集是 `odbc-api` 按**驱动上报的列宽**分配的、
//! Doris 的请求体被 `reqwest` 拿走了 —— 都在第三方库里，从外面没有通用办法看见。
//! 而**连接器自己知道它要了多少**（它就是要那块内存的人）。所以分工只能是：
//!
//! ```text
//! 引擎给尺子（本模块）      连接器往尺子上挂账            引擎读峰值
//! ```
//!
//! 这是 `d2d_connector::SourceConnector::unit_footprint` 之外的第二条路，
//! 两者分工是 **预测 vs 事实**：
//!
//! | | 何时有数 | 干什么用 |
//! |---|---|---|
//! | `unit_footprint`（静态） | `--dry-run` 就有（还没连库） | 装不下**拒绝启动** |
//! | 本模块（运行期） | 跑起来才有 | 回答那本账**准不准** |
//!
//! # 引擎不解释这些字节是什么
//!
//! 引擎只做加法与并排展示。差额于是能被拆成两半：
//! **连接器已认领的**，和**谁也不认领的**（Arrow 结构开销 / 分配器碎片 / 第三方库）。
//!
//! 若改成引擎硬编码"doris 会拷贝一份 body"，那是把连接器私有行为漏进通用账本 ——
//! 换个目标端（MySQL）那句话就不成立了，而账本里会永远躺着一条假账。
//!
//! # 用法
//!
//! ```no_run
//! use d2d_memory::MemMeter;
//! # fn f(size: usize) {
//! let meter = MemMeter::new("例子/缓冲", "一块就要 size 字节");
//! {
//!     let _lease = meter.claim(size);   // 挂账
//!     // …这块内存活着的时候…
//! }                                     // ← 租约 drop，自动销账
//! assert_eq!(meter.peak_bytes(), size);
//! # }
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 一把尺子：一个**有名字的**占用项（名字与说明由连接器给，引擎不解释）
pub struct MemMeter {
    label: &'static str,
    note: String,
    /// 当前在账的字节（挂账累加、销账累减）
    current: AtomicUsize,
    /// 曾经到过的最高水位
    ///
    /// 为什么要峰值而不是末尾读数：比对的对象是 `VmHWM`（内核水位），
    /// 而跑到结束时连接器手里早就空了 —— 只记 current 的话永远比出个 0。
    peak: AtomicUsize,
}

/// 一次申报的**租约**：drop 即销账
///
/// 用 RAII 而不是"记得调 release"：连接器里那块内存的释放点常常是
/// 中途 `return`、`?`、panic 展开 —— 手写销账必然漏，漏了就是**越跑越高的假账**。
pub struct MemLease {
    meter: Arc<MemMeter>,
    bytes: usize,
}

/// 一次申报的读数快照（日志/对账用）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemSnapshot {
    /// 连接器给的名字（如 `dameng/odbc 行集`）
    pub label: String,
    /// 这块内存**是什么**（连接器给的说明，引擎不理解也不解释）
    pub note: String,
    /// 峰值（与 `VmHWM` 同口径）
    pub peak_bytes: usize,
    /// 快照那一刻在账的字节
    pub current_bytes: usize,
}

impl MemMeter {
    pub fn new(label: &'static str, note: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            label,
            note: note.into(),
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    /// 挂账 `bytes`，返回的租约 drop 时销账
    pub fn claim(self: &Arc<Self>, bytes: usize) -> MemLease {
        self.raise(bytes);
        MemLease {
            meter: Arc::clone(self),
            bytes,
        }
    }

    pub fn label(&self) -> &'static str {
        self.label
    }

    pub fn note(&self) -> &str {
        &self.note
    }

    pub fn current_bytes(&self) -> usize {
        self.current.load(Ordering::Acquire)
    }

    pub fn peak_bytes(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> MemSnapshot {
        MemSnapshot {
            label: self.label.to_string(),
            note: self.note.clone(),
            peak_bytes: self.peak_bytes(),
            current_bytes: self.current_bytes(),
        }
    }

    /// 挂账：先加再抬水位
    ///
    /// 水位这样算是**准的**而不是"大概"：`current` 只被挂账加、被销账减，
    /// 于是每一瞬间的最大值必然紧跟在某一次挂账之后 —— 每次挂账后抬一次水位，
    /// 取到就是真峰值（并发下各线程看到的是各自的 `current`，`fetch_max` 收敛到最大那个）。
    fn raise(&self, bytes: usize) {
        let after = self.current.fetch_add(bytes, Ordering::AcqRel) + bytes;
        self.peak.fetch_max(after, Ordering::AcqRel);
    }

    fn lower(&self, bytes: usize) {
        self.current.fetch_sub(bytes, Ordering::AcqRel);
    }
}

impl Drop for MemLease {
    fn drop(&mut self) {
        self.meter.lower(self.bytes);
    }
}

impl MemSnapshot {
    pub fn mb(&self) -> usize {
        self.peak_bytes / 1048576
    }
}

impl std::fmt::Debug for MemMeter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemMeter")
            .field("label", &self.label)
            .field("current_bytes", &self.current_bytes())
            .field("peak_bytes", &self.peak_bytes())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peak_tracks_high_water_not_last_value() {
        let m = MemMeter::new("t", "test");
        let a = m.claim(100);
        let b = m.claim(50);
        assert_eq!(m.current_bytes(), 150);
        assert_eq!(m.peak_bytes(), 150);
        drop(a);
        drop(b);
        // 全销账后 current 归零，但峰值必须留着 —— 否则末态读数永远是 0
        assert_eq!(m.current_bytes(), 0);
        assert_eq!(m.peak_bytes(), 150);
    }

    #[test]
    fn peak_is_max_over_all_overlapping_claims() {
        let m = MemMeter::new("t", "test");
        let _a = m.claim(100);
        drop(_a);
        let _b = m.claim(30);
        assert_eq!(m.peak_bytes(), 100);
    }

    #[test]
    fn lease_drop_is_the_only_way_to_release() {
        let m = MemMeter::new("t", "test");
        {
            let _lease = m.claim(4096);
            assert_eq!(m.current_bytes(), 4096);
        }
        assert_eq!(m.current_bytes(), 0);
    }

    #[test]
    fn snapshot_carries_label_and_note() {
        let m = MemMeter::new("dameng/odbc 行集", "8 × 10000 行");
        let _l = m.claim(1024);
        let s = m.snapshot();
        assert_eq!(s.label, "dameng/odbc 行集");
        assert_eq!(s.note, "8 × 10000 行");
        assert_eq!(s.peak_bytes, 1024);
        assert_eq!(s.current_bytes, 1024);
    }

    #[test]
    fn concurrent_claims_do_not_lose_bytes() {
        const N: usize = 8;
        // 栅栏保证 8 份租约**同时**在账 —— 否则先跑完的线程先销账，峰值就不是 8 份
        let m = MemMeter::new("t", "test");
        let barrier = Arc::new(std::sync::Barrier::new(N));
        let mut handles = Vec::new();
        for _ in 0..N {
            let m = Arc::clone(&m);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                let _l = m.claim(1_000_000);
                barrier.wait();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.current_bytes(), 0);
        assert_eq!(m.peak_bytes(), N * 1_000_000);
    }
}
