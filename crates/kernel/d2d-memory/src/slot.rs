//! 槽位池：**可计数资源**的额度 —— 并发闸门的实现
//!
//! 这个模块只做一件事：把"同时只能有 N 个"这件事变成一个能借、能还、
//! 能看峰值的对象。
//!
//! # 它装在哪：三档派发器的手里
//!
//! 这里走过两段弯路，值得记下来，因为两条错法长得完全不一样：
//!
//! 1. **券数 == 任务数**：引擎给三档各发一个池、容量等于任务数、每个任务每轮取一张券。
//!    券永远够用，`acquire()` 从不阻塞 —— 那三个池是纯装饰。
//! 2. **把闸门塞给连接器**：既然引擎自己不用，那就给连接器吧 —— 连接器每开一个
//!    取数单元取一张券。方向错了：那是"内核伸手管连接器内部"，而连接器内部本来
//!    就不该由内核解释（dmfldr 的导出子进程与 ODBC 的游标根本不是一回事）。
//!
//! 现在它在**派发器**手里，而派发器是内核自己的循环：
//!
//! ```text
//! 用户配 flow.source.concurrency: 16
//!      └─► SlotPool::new("source", 16)          ← 内核手里的闸门
//!             └─► 派发器：取一份数据 → acquire() → spawn(任务，券随任务走)
//! ```
//!
//! 于是两件事各自成立、互不推导：
//!
//! - **任务数由数据决定** —— 上游有 100 份就派 100 个任务，只有 1 份就派 1 个
//!   （池里 100 MB、`batch_bytes` 60 MB 就是 2 个任务，不是"并发数"个）
//! - **同时几个由闸门决定** —— 拿不到券的派发器停在 `acquire()` 上，
//!   于是它**不再从上游取下一份**，内存也就不会被抽干
//!
//! 连接器全程不知道这个数字。它只要守"一次 `read` = 一个取数单元"的契约，
//! 而"同时在跑几个"由内核按任务发券卡死 —— 这正是"个数不由连接器定"的落实方式。
//!
//! # 为什么不用内存池那一套
//!
//! 并发管的是**个数**，不是字节。把它塞进字节账里，就会出现"我调大内存，
//! 并发才涨上去"这种用户完全无法预料的因果 —— 而那正是被删掉的那版设计最大的问题。
//!
//! 字节那边由 `d2d_execution::pool::DataPool` 负责，两者互不换算。
//!
//! # 峰值为什么重要
//!
//! 峰值回答"这个额度是不是设小了"。没有它就只剩两个读数（当前占用/上限），
//! 而当前占用在任务结束时总是 0，什么也说明不了。
//! 收尾时打出的 `source_units = 2/16` 就是它 —— 左边远小于右边说明配大了。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use d2d_core::{D2DError, D2DResult};

/// 可计数资源的池
#[derive(Debug)]
pub struct SlotPool {
    name: &'static str,
    total: usize,
    sem: Arc<tokio::sync::Semaphore>,
    peak: AtomicUsize,
}

impl SlotPool {
    pub fn new(name: &'static str, total: usize) -> Arc<Self> {
        let total = total.max(1); // 0 会让信号量永久阻塞，病态输入不能传下去
        Arc::new(Self {
            name,
            total,
            sem: Arc::new(tokio::sync::Semaphore::new(total)),
            peak: AtomicUsize::new(0),
        })
    }

    /// 上限（"这个额度给了多少"）
    pub fn total(&self) -> usize {
        self.total
    }

    /// 历史峰值占用（"这个额度用满了没有" —— 峰值 ≪ 上限说明给大了）
    ///
    /// ⚠️ 这里**故意只有"上限"和"峰值"两个读数**，没有 `used()` / `available()`：
    /// 后两者在任务结束时恒为 `0` / `total`，什么也说明不了。
    /// 而且它们等于"信号量还能拿几个"，那是**实现细节的镜子**，不是使用者该看的东西。
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    fn note_peak(&self, used: usize) {
        self.peak.fetch_max(used, Ordering::Relaxed);
    }

    /// 取一个槽位
    ///
    /// ⚠️ **只在调用方不同时持有别的槽位时使用**。"持有 A 等 B"是死锁的经典配方。
    ///
    /// 本仓库靠**三道互不相干的闸门**把这条约束结构化掉了（源端 / 处理 / 目标端各一道）：
    /// 每个任务**只从自己那一档取一张券**，绝不跨档去拿 —— 成环的前提不存在。
    /// 同一档里每个任务也是各自独立地 `acquire()`（拿不到就排队），不互相等待。
    ///
    /// ⚠️ 另外：**别在拿着别的锁的时候 await 这个** —— 那会把 `Mutex` 跨 await 持有，
    /// 是另一个经典配方。
    pub async fn acquire(self: &Arc<Self>) -> D2DResult<SlotLease> {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| D2DError::internal(format!("槽位池 '{}' 已关闭", self.name)))?;
        // 峰值 = "拿在手里的最多有几张券"。`acquire` 返回时这张券一定在手上，
        // 所以此刻的占用就是 `total - 剩余`。
        self.note_peak(self.total.saturating_sub(self.sem.available_permits()));
        Ok(SlotLease { _permit: permit })
    }
}

/// 槽位租约，Drop 即归还
#[derive(Debug)]
pub struct SlotLease {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 峰值用于判断"这个额度是不是给大了" —— 必须单调不减
    #[tokio::test]
    async fn test_peak_never_goes_down() {
        let p = SlotPool::new("t", 4);
        let a = p.acquire().await.unwrap();
        let b = p.acquire().await.unwrap();
        let c = p.acquire().await.unwrap();
        assert_eq!(p.peak(), 3);
        drop((a, b, c));
        assert_eq!(p.peak(), 3, "峰值不该随释放回落");
        let _d = p.acquire().await.unwrap();
        assert_eq!(p.peak(), 3, "没超过历史峰值就不该变");
    }

    /// 0 是病态输入：信号量会永久阻塞，必须在构造时归一到 1
    #[tokio::test]
    async fn test_zero_total_is_normalized() {
        let p = SlotPool::new("t", 0);
        assert_eq!(p.total(), 1);
        assert!(p.acquire().await.is_ok(), "0 张券会让所有人永久等待");
    }

    /// `acquire()` 真的会阻塞到有人归还 —— 闸门"会咬人"的最小证据
    #[tokio::test]
    async fn test_acquire_blocks_until_released() {
        let p = SlotPool::new("t", 1);
        let held = p.acquire().await.unwrap();
        let p2 = Arc::clone(&p);
        let waiter = tokio::spawn(async move { p2.acquire().await.unwrap() });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "槽位没释放就不该拿到");
        drop(held);
        let lease = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .expect("释放后应当立刻拿到")
            .unwrap();
        drop(lease);
        assert_eq!(p.peak(), 1, "峰值 = 最多同时握着几张券");
    }

    /// 额度**只由租约归还**：忘掉 lease 会泄漏，drop 才还
    #[tokio::test]
    async fn test_capacity_is_returned_only_by_dropping_the_lease() {
        let p = SlotPool::new("t", 2);
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let p = Arc::clone(&p);
            let live = Arc::clone(&live);
            let peak = Arc::clone(&peak);
            tasks.push(tokio::spawn(async move {
                let _l = p.acquire().await.unwrap();
                let n = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                live.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2, "同时在手的券不得超过上限");
        assert_eq!(p.peak(), 2);
    }
}
