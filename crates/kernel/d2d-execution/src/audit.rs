//! 收尾行数对账 —— **全仓唯一一处**
//!
//! # 为什么要有它
//!
//! 在这之前，"这次迁移到底对不对"有两处说法：`flow.rs`（直通面）比一次
//! 块计数与目标端确认数，`main.rs`（CLI 侧）又比一次 dmfldr 日志与写入行数。
//! 口径不同、触发条件不同、错误文案不同 —— **同一个承诺写了两遍**。
//! 而 CLI 那一份根本没法测（它是 `main()` 里的一段），于是"丢了行也报成功"
//! 只能靠人去读日志发现。
//!
//! 现在只有一处：本模块。引擎负责**出结论**，上层只负责**显示与退出码**。
//!
//! # 三个数，各自独立
//!
//! | 数 | 谁给的 | 回答什么 |
//! |---|---|---|
//! | [`RowAudit::exported`] | 连接器（`SourceConnector::exported_rows()`） | 源端**自己**说导出了多少行（dmfldr 的导出日志；给不出时为 `None`） |
//! | [`RowAudit::read`] | 引擎 | 我们**从源端读出来**多少行（源端阶段真正产出的） |
//! | [`RowAudit::written`] | 目标端 | 目标端**确认写下**多少行（Doris 的 `NumberLoadedRows`） |
//!
//! 两次比较各抓一类丢行，**缺一不可**：
//!
//! ```text
//! exported ──①── read ──②── written
//!           ① 导出侧丢行（源库→产物）
//!                      ② 搬运/写入侧丢行（产物→目标端）
//! ```
//!
//! 少了 ①，dmfldr 导出时自己丢几行没人知道（源端读到的和目标端确认的会一起变小，两边仍然相等）；
//! 少了 ②，源端给不出权威数的那些连接器（如 ODBC 游标）就**完全没有对账**。

use d2d_core::D2DError;

/// 一次迁移的**行数三个数**（口径见模块文档）
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowAudit {
    /// 源端自报的权威行数；连接器给不出时为 `None`（**不是 0** —— 0 是"确实零行"）
    pub exported: Option<u64>,
    /// 引擎从源端读出来的行数
    pub read: u64,
    /// 目标端确认写下的行数
    pub written: u64,
}

/// 对账结论
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// 两次比较都过了，而且源端权威数也在（对账做到头了）
    Pass,
    /// **只对了一半**：源端给不出独立行数，② 成立、① 做不了。
    /// 如实说"对了一半"，而不是假装对过账。
    Partial { reason: &'static str },
    /// 对不上
    Mismatch {
        /// 是哪一次比较（"导出侧" / "搬运/写入侧"）
        stage: &'static str,
        expect: u64,
        got: u64,
    },
}

impl RowAudit {
    /// 出结论 —— **全仓"什么算对账通过"的唯一判据**
    pub fn verdict(&self) -> Verdict {
        // ① 源端自报 vs 我们读出来的：抓**导出侧**丢行
        if let Some(exported) = self.exported {
            if exported != self.read {
                return Verdict::Mismatch {
                    stage: "导出侧",
                    expect: exported,
                    got: self.read,
                };
            }
        }
        // ② 我们读出来的 vs 目标端确认的：抓**搬运/写入侧**丢行
        if self.read != self.written {
            return Verdict::Mismatch {
                stage: "搬运/写入侧",
                expect: self.read,
                got: self.written,
            };
        }
        match self.exported {
            Some(_) => Verdict::Pass,
            None => Verdict::Partial {
                reason: "源端给不出独立行数（该连接器不产出导出统计）",
            },
        }
    }

    /// 结论的**唯一**错误构造 —— 两个数都报出来，并给出可排查的原因
    ///
    /// 只说"对账失败"没法排查，所以每种情形都点名最常见的成因。
    pub fn mismatch_error(&self, v: &Verdict) -> D2DError {
        let Verdict::Mismatch { stage, expect, got } = *v else {
            return D2DError::internal("mismatch_error 只该用于 Mismatch 结论");
        };
        let hint = match stage {
            "导出侧" => {
                "① 源端导出本身漏了行（分片区间没覆盖全 / 分片重叠）；\
                 ② 解析分块时末行被截断"
            }
            _ => {
                "① 目标表唯一键不含真正的唯一列（重复键被静默去重）；\
                 ② 某分片解析产物没搬完就被丢掉；\
                 ③ 目标端 strict_mode / max_filter_ratio 过滤了脏数据"
            }
        };
        D2DError::external(
            "d2d",
            format!(
                "行数对账失败（{}）：应当 {} 行，实际 {} 行，相差 {} 行。常见原因：{}。\
                 （三个数的口径见 `d2d_execution::audit` 模块文档：\
                 源端自报 {} / 读出 {} / 目标端确认 {}）",
                stage,
                expect,
                got,
                expect as i64 - got as i64,
                hint,
                self.exported
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "—".into()),
                self.read,
                self.written,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audit(exported: Option<u64>, read: u64, written: u64) -> RowAudit {
        RowAudit {
            exported,
            read,
            written,
        }
    }

    /// 两次比较都过 + 源端权威数在 ⟹ 对到头了
    #[test]
    fn test_all_three_agree_is_pass() {
        assert_eq!(audit(Some(100), 100, 100).verdict(), Verdict::Pass);
    }

    /// 源端给不出权威数 ⟹ **如实说"只对了一半"**，不是 Pass、也不是 Mismatch
    #[test]
    fn test_missing_source_count_is_partial_not_pass() {
        match audit(None, 100, 100).verdict() {
            Verdict::Partial { reason } => assert!(!reason.is_empty()),
            other => panic!("应当如实说只对了一半，得到 {:?}", other),
        }
    }

    /// **① 抓导出侧丢行**：源端自报 100，我们只读到 99（而目标端也确认 99）
    ///
    /// 这一条是"只看写入侧永远发现不了"的那类 ——
    /// 少了 ①，两个数一起变小，② 照样相等。
    #[test]
    fn test_catches_export_side_loss() {
        match audit(Some(100), 99, 99).verdict() {
            Verdict::Mismatch { stage, expect, got } => {
                assert_eq!(stage, "导出侧");
                assert_eq!((expect, got), (100, 99));
            }
            other => panic!("导出侧丢行必须报出来，得到 {:?}", other),
        }
    }

    /// **② 抓搬运/写入侧丢行**：读出来 100、目标端只确认 99
    #[test]
    fn test_catches_transfer_side_loss() {
        match audit(Some(100), 100, 99).verdict() {
            Verdict::Mismatch { stage, expect, got } => {
                assert_eq!(stage, "搬运/写入侧");
                assert_eq!((expect, got), (100, 99));
            }
            other => panic!("搬运侧丢行必须报出来，得到 {:?}", other),
        }
    }

    /// **源端给不出权威数时，② 仍然要查** —— 否则 ODBC 这类源就完全没有对账
    #[test]
    fn test_transfer_side_still_checked_without_exported() {
        assert!(matches!(
            audit(None, 100, 99).verdict(),
            Verdict::Mismatch {
                stage: "搬运/写入侧",
                ..
            }
        ));
    }

    /// 源端权威数**大于**读出来的、与**小于**它，都要报（方向不敏感）
    #[test]
    fn test_mismatch_is_direction_agnostic() {
        assert!(matches!(
            audit(Some(100), 101, 101).verdict(),
            Verdict::Mismatch { .. }
        ));
        assert!(matches!(
            audit(Some(100), 99, 99).verdict(),
            Verdict::Mismatch { .. }
        ));
    }

    /// 零行是合法结果，不是"算不出来"
    #[test]
    fn test_zero_rows_is_a_real_verdict() {
        assert_eq!(audit(Some(0), 0, 0).verdict(), Verdict::Pass);
        assert!(matches!(
            audit(Some(0), 0, 1).verdict(),
            Verdict::Mismatch { .. }
        ));
    }

    #[test]
    fn test_mismatch_error_names_stage_numbers_and_causes() {
        let a = audit(Some(100), 100, 99);
        let e = a.mismatch_error(&a.verdict()).to_string();
        assert!(e.contains("搬运/写入侧"), "{}", e);
        assert!(e.contains("100") && e.contains("99"), "{}", e);
        assert!(e.contains("相差 1"), "{}", e);
        assert!(e.contains("唯一键"), "要给出可排查的原因: {}", e);

        let b = audit(Some(100), 99, 99);
        let e2 = b.mismatch_error(&b.verdict()).to_string();
        assert!(e2.contains("导出侧"), "{}", e2);
        assert!(e2.contains("分片"), "导出侧的原因要对得上: {}", e2);
    }
}
