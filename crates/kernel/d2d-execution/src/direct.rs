//! 直通面的**安全闸**与它的开关
//!
//! # 直通不是另一条链路
//!
//! 直通的执行已经并入统一拓扑（见 [`crate::flow`]）：
//! `source_task` 产出原样字节块 → `source_pool` → `pipeline_task`（**什么都不做**）
//! → `sink_pool` → `sink_task`。本模块只留下直通**独有**的那些东西：
//!
//! 1. 受限字节扫描（[`find_forbidden`]）
//! 2. 三个安全开关（[`DirectConfig`]）
//!
//! 曾经这里有一个 `DirectExecutor`，它"一个 task 干完全程"，绕开了三段结构 ——
//! 等于每个分片独占一份三段资源，而且直通与逻辑面的并发账各算各的。
//! 那一整套被删掉了：**同一张拓扑、同一个入口（[`crate::flow::run_flow`]）**，
//! 区别只在 `pipeline_task` 那一格做不做转换。
//!
//! # 为什么必须有受限字节扫描
//!
//! 直通跳过了引擎的类型校验。目标端会回报它自己**仍会特殊解释**的字节
//! （例如 Doris CSV 读入器把 `\` 当转义符，而 dmfldr 导出侧未做转义）。
//! 一旦载荷里出现这类字节，目标端的解释就与源端的产出不一致 —— 这是**静默改值**，
//! 比报错危险得多。
//!
//! 因此每个字节块在**进入 body 之前**都过一遍 `memchr`。命中即整任务失败：
//!
//! - 代价≈0：这些字节本来就要拷贝/发送，扫描是顺路的（SIMD，>1GB/s）
//! - 收益确定：把「悄悄写错」换成「大声失败」，运维改用逻辑面重跑即可
//!
//! 这是**精确扫描**而非抽样，不存在"刚好没抽到"的漏网。

/// 直通面的安全开关
///
/// 三项都**缺省开**（除了预扫描 —— 它要多读一遍源文件）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectConfig {
    /// 是否在开工前对每个分片做一次预扫描
    ///
    /// `false`（缺省）：只做在途扫描 —— 零额外 I/O，发现违规时可能已发出部分数据
    /// （但一定是**失败退出**，不会有静默污染）
    /// `true`：提前失败，代价是多读一遍源文件（页缓存热时几乎免费）
    pub preflight_scan: bool,
    /// 受限字节扫描开关（缺省开）
    ///
    /// 关掉它 = 放弃「目标端会误解载荷」的防护。实测代价仅 **~100 ms/GiB**
    /// （SIMD `memchr`，与搬运本身同量级的顺路扫描），见 `benches::bench_scan_overhead`。
    /// **只有在已确认源数据不含受限字节时才建议关**
    pub forbidden_scan: bool,
    /// 行数比对失败是否中止任务（缺省 true）
    ///
    /// 关掉只影响**比对**，不影响计数本身（计数免费：Doris 响应自带
    /// `NumberLoadedRows`，源端行数来自 dmfldr 自己的进度输出）。
    pub fail_on_row_mismatch: bool,
}

impl Default for DirectConfig {
    fn default() -> Self {
        Self {
            preflight_scan: false,
            forbidden_scan: true,
            fail_on_row_mismatch: true,
        }
    }
}

/// 在 `hay` 中查找任一 `forbidden` 字节
///
/// `forbidden` 规模是个位数（转义符/引号符），逐个 `memchr` 即可吃到 SIMD；
/// 没必要上 Aho-Corasick。
pub fn find_forbidden(hay: &[u8], forbidden: &[u8]) -> Option<u8> {
    forbidden
        .iter()
        .copied()
        .find(|b| memchr::memchr(*b, hay).is_some())
}

/// 汇总直通路径的会话级告警（供 CLI 使用）
pub fn warn_if_no_forbidden_declared(forbidden: &[u8]) {
    if forbidden.is_empty() {
        tracing::warn!(
            "目标端未声明任何受限字节 —— 直通将不做转义安全性检查。\
             若源端导出未转义而目标端会解释转义序列，存在静默改值风险"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_forbidden_hits_and_misses() {
        assert_eq!(find_forbidden(b"abc|def", b"\\"), None);
        assert_eq!(find_forbidden(b"a\\b", b"\\"), Some(b'\\'));
        assert_eq!(find_forbidden(b"a\"b", b"\\\""), Some(b'"'));
        assert_eq!(find_forbidden(b"", b"\\"), None);
        // 空禁用集 = 不做检查
        assert_eq!(find_forbidden(b"a\\b", &[]), None);
    }

    #[test]
    fn test_find_forbidden_multibyte_scan() {
        let hay = vec![b'a'; 1024 * 1024];
        assert_eq!(find_forbidden(&hay, b"\\"), None);
        let mut hay2 = hay.clone();
        hay2[1024 * 1024 - 3] = b'\\';
        assert_eq!(find_forbidden(&hay2, b"\\"), Some(b'\\'));
    }

    /// 缺省必须是保守的那一档：安全闸开着、预扫描关着（不白付一遍 I/O）
    #[test]
    fn test_defaults_are_conservative() {
        let c = DirectConfig::default();
        assert!(c.forbidden_scan, "受限字节扫描缺省必须开");
        assert!(c.fail_on_row_mismatch, "行数对账缺省必须开");
        assert!(!c.preflight_scan, "预扫描缺省关 —— 它要多读一遍源文件");
    }
}
