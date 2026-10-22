//! dmfldr 子进程管控
//!
//! 职责：拼装 argv → spawn → await 退出码 → 从日志提取统计信息。
//! 退出码非 0 = 失败（错误分类见 map_exit）；日志统计缺失只告警不失败
//! （各版本日志措辞可能有差异，退出码是硬门禁）。

use d2d_core::{D2DError, D2DResult};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

/// dmfldr 导出的统计信息（从日志提取，尽力而为）
#[derive(Debug, Clone, Default)]
pub struct ExportStats {
    /// 读取的逻辑记录总数（导出行数，权威行数）
    pub rows_read: u64,
    /// 拒绝的逻辑记录总数
    pub rows_rejected: u64,
    /// 跳过的逻辑记录总数
    pub rows_skipped: u64,
    /// 耗时（毫秒）
    pub elapsed_ms: u64,
}

/// 导出结果
#[derive(Debug)]
pub struct ExportOutcome {
    pub data_file: PathBuf,
    pub stats: ExportStats,
}

/// 执行 dmfldr OUT + TABLE 导出
///
/// - `userid`：USERID 值（连接配置组装）
/// - `ctl_path`：控制文件路径
/// - `log_path`：日志输出路径
/// - `work_dir`：子进程工作目录
pub async fn run_export(
    executable: &str,
    userid: &str,
    ctl_path: &Path,
    log_path: &Path,
    work_dir: &Path,
) -> D2DResult<ExportOutcome> {
    let data_file = infer_data_file(ctl_path)?;

    let started = Instant::now();
    // argv 逐项传递（无 shell）；字符串参数值按手册要求用单引号封闭
    let output = tokio::process::Command::new(executable)
        .current_dir(work_dir)
        .arg(format!("USERID={}", userid))
        .arg("MODE='OUT'")
        .arg(format!("CONTROL='{}'", ctl_path.display()))
        .arg(format!("LOG='{}'", log_path.display()))
        .output()
        .await
        .map_err(|e| {
            D2DError::external(
                "dmfldr",
                format!(
                    "failed to spawn dmfldr '{}' (is it installed and on PATH?): {}",
                    executable, e
                ),
            )
        })?;

    let elapsed_ms = started.elapsed().as_millis() as u64;
    let status = output.status;
    // dmfldr 控制台输出为 GBK（见 decode_dm_text），按 UTF-8 lossy 取会让中文标签变乱码
    let stdout = decode_dm_text(&output.stdout);
    let stderr = decode_dm_text(&output.stderr);

    if !status.success() {
        let log_tail = tail_file(log_path, 4000);
        return Err(map_exit(
            status.code(),
            &stdout,
            &stderr,
            &log_tail,
            executable,
        ));
    }

    if !data_file.exists() {
        return Err(D2DError::external(
            "dmfldr",
            format!(
                "dmfldr reported success but data file {} was not produced",
                data_file.display()
            ),
        ));
    }

    let stats = parse_log_stats(&push_str_lines(&stdout, &stderr), log_path).unwrap_or_default();
    let mut stats = stats;
    stats.elapsed_ms = elapsed_ms;

    tracing::info!(
        data_file = %data_file.display(),
        rows_read = stats.rows_read,
        rows_rejected = stats.rows_rejected,
        elapsed_ms = stats.elapsed_ms,
        "dmfldr export completed"
    );
    Ok(ExportOutcome { data_file, stats })
}

/// 执行 dmfldr **IN** 快载（目标端方向）
///
/// 与 [`run_export`] 是同一件事的两面：**argv 形式只该有一处知道**，
/// 所以 IN 的拼装也放在本模块，而不是让 sink 那边自己 `Command::new`。
///
/// argv 三条实测约定（抄自能跑通的脚本，别照着直觉改）：
/// - `USERID` 的值**不带**外层单引号，但密码要用**双引号**包住
///   （`#` 是达梦的扩展选项引导符，不包会报「无效的USERID串」）
/// - `MODE='IN'` / `CONTROL='…'` / `LOG='…'` 的值要带**字面单引号**
///   （shell 会把 `MODE='IN'` 的引号吃掉，dmfldr 就报「参数分析出错」）
///
/// ⚠️ 退出码非 0 即失败：实测「拒绝 1 行」会给出退出码 2 ——
/// 也就是说**丢行是有退出码的**，不会静默过去。
pub async fn run_load(
    executable: &str,
    userid: &str,
    ctl_path: &Path,
    log_path: &Path,
    work_dir: &Path,
) -> D2DResult<ExportStats> {
    let started = Instant::now();
    let output = tokio::process::Command::new(executable)
        .current_dir(work_dir)
        .arg(format!("USERID={}", userid))
        .arg("MODE='IN'")
        .arg(format!("CONTROL='{}'", ctl_path.display()))
        .arg(format!("LOG='{}'", log_path.display()))
        .output()
        .await
        .map_err(|e| {
            D2DError::external(
                "dmfldr",
                format!(
                    "failed to spawn dmfldr '{}' (is it installed and on PATH?): {}",
                    executable, e
                ),
            )
        })?;

    let elapsed_ms = started.elapsed().as_millis() as u64;
    let stdout = decode_dm_text(&output.stdout);
    let stderr = decode_dm_text(&output.stderr);

    if !output.status.success() {
        let log_tail = tail_file(log_path, 4000);
        return Err(map_exit(
            output.status.code(),
            &stdout,
            &stderr,
            &log_tail,
            executable,
        ));
    }

    // IN 的统计块走 **stderr**（实测），LOG 文件里也有同一份 —— 两处都给上，谁有算谁
    let mut stats =
        parse_log_stats(&push_str_lines(&stdout, &stderr), log_path).ok_or_else(|| {
            D2DError::external(
                "dmfldr",
                "IN 模式没能从日志里解析出行数统计 —— 无法与源端对账，不敢当成功",
            )
        })?;
    stats.elapsed_ms = elapsed_ms;
    tracing::info!(
        rows_loaded = stats.rows_read,
        rows_rejected = stats.rows_rejected,
        rows_skipped = stats.rows_skipped,
        elapsed_ms,
        "dmfldr load completed"
    );
    Ok(stats)
}

/// 把 stdout/stderr 拼成待扫描文本
mod stdout_push {
    pub fn push_str_lines(a: &str, b: &str) -> String {
        let mut s = String::with_capacity(a.len() + b.len() + 1);
        s.push_str(a);
        s.push('\n');
        s.push_str(b);
        s
    }
}
use stdout_push::push_str_lines;

/// 从控制文件解析 INFILE 路径（数据文件在哪）
fn infer_data_file(ctl_path: &Path) -> D2DResult<PathBuf> {
    let content = std::fs::read_to_string(ctl_path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("failed to read control file {}: {}", ctl_path.display(), e),
        )
    })?;
    for line in content.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("INFILE") {
            let rest = rest.trim();
            let rest = rest.trim_start_matches('*').trim(); // '*' = 用 DATA 参数（我们不生成这种）
            let unquoted = rest
                .trim_start_matches('\'')
                .split('\'')
                .next()
                .unwrap_or("")
                .to_string();
            if !unquoted.is_empty() {
                let p = PathBuf::from(unquoted);
                // has_root() 同时覆盖 Windows 盘符绝对路径与 Unix 风格 / 根路径
                if p.has_root() {
                    return Ok(p);
                }
                // 相对路径基于控制文件所在目录
                return Ok(ctl_path.parent().unwrap_or(Path::new(".")).join(p));
            }
        }
    }
    Err(D2DError::config(
        "control file has no INFILE clause (cannot locate data file)",
    ))
}

/// 退出码 → 错误分类（含日志尾部，便于诊断）
///
/// 连接/网络/超时类失败 → `SourceRead`（可重试）；其余 → `External`（致命）。
fn map_exit(
    code: Option<i32>,
    stdout: &str,
    stderr: &str,
    log_tail: &str,
    executable: &str,
) -> D2DError {
    let blob = format!("{}\n{}\n{}", stdout, stderr, log_tail).to_lowercase();
    let retryable = blob.contains("连接")
        || blob.contains("网络")
        || blob.contains("超时")
        || blob.contains("登录")
        || blob.contains("timeout")
        || blob.contains("connect");
    let msg = format!(
        "dmfldr exited with code {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}\n--- log tail ---\n{}",
        code,
        tail_str(stdout, 2000),
        tail_str(stderr, 2000),
        log_tail
    );
    if retryable {
        D2DError::source_read(executable, msg)
    } else {
        D2DError::external(executable, msg)
    }
}

fn tail_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("...{}", &s[s.len() - max..])
    }
}

fn tail_file(path: &Path, max: usize) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) => tail_str(&s, max),
        Err(_) => String::new(),
    }
}

/// 解析 dmfldr 日志/输出中的统计行（手册 3.6 示例格式）
///
/// 形如：
/// ```text
/// 3 行加载成功。
/// 跳过的逻辑记录总数:0
/// 读取的逻辑记录总数:3
/// 拒绝的逻辑记录总数:0
/// 用时:20.522(ms)
/// ```
/// OUT 模式的 succeeded 行措辞可能不同，**读取的逻辑记录总数**是权威导出行数。
/// 执行 dmfldr OUT + **SQLFILE** 导出（分片并发路线）
///
/// 与 `run_export`（`TABLE=` 整表路线）的区别：
/// - 走 `SQLFILE=`，查询里可带 WHERE，服务端据此**按分区裁剪**（实测 28 行的分区 0.01s
///   vs 全表扫 14s）；这是分片并发快 6× 的前提。
/// - **不生成控制文件**：列分隔符 / NULL / 转义全部走 dmfldr 缺省，
///   因此分片路线的解析选项必须与缺省对齐（见 `dmfldr::mod` 的 `read()`）。
///
/// 参数值按手册要求用单引号封闭（SQLFILE/DATA/LOG 均为字符串参数）。
/// 返回该次导出的日志统计（`rows_read` = 该分片的权威行数）。
///
/// **为什么必须把行数带回来**：分片并发导出后，源端权威总行数不再来自一次全表
/// 统计，而是「各分片日志行数之和」。少了它，收尾的行数对账就失去源端基准，
/// 静默丢数（唯一键去重、分片区间重叠）将无法发现。
pub async fn run_sql_export(
    executable: &str,
    userid: &str,
    sql_path: &Path,
    data_path: &Path,
    log_path: &Path,
    work_dir: &Path,
    task_threads: u32,
) -> D2DResult<ExportStats> {
    spawn_sql_export(
        executable,
        userid,
        sql_path,
        data_path,
        log_path,
        work_dir,
        task_threads,
    )?
    .wait()
    .await
}

/// 已启动、**尚未等待结束**的 dmfldr SQL 导出子进程
///
/// [`run_sql_export`] 是「spawn + wait」的合体。流式 source 需要进程还在写时
/// 这边就开始读，所以把等待拆了出来：调用方拿到句柄 → 跟随读数据文件 →
/// 读完后 [`Self::wait`] 收退出码与行数统计。
pub struct SqlExportProcess {
    data_path: PathBuf,
    log_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    executable: String,
    child: tokio::process::Child,
    started: Instant,
}

/// 启动 dmfldr OUT + SQLFILE 导出，**不等待**它结束
///
/// stdout/stderr 重定向到日志旁边的文件：流式期间数据文件与日志同时在写，
/// 用管道就得再起两个 drain 任务，不如让 dmfldr 自己写文件、退出后一次读取。
pub fn spawn_sql_export(
    executable: &str,
    userid: &str,
    sql_path: &Path,
    data_path: &Path,
    log_path: &Path,
    work_dir: &Path,
    task_threads: u32,
) -> D2DResult<SqlExportProcess> {
    let started = Instant::now();
    let threads = if task_threads == 0 { 4 } else { task_threads };
    let stdout_path = log_path.with_extension("stdout");
    let stderr_path = log_path.with_extension("stderr");
    let stdout = std::fs::File::create(&stdout_path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("创建 {} 失败: {}", stdout_path.display(), e),
        )
    })?;
    let stderr = std::fs::File::create(&stderr_path).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("创建 {} 失败: {}", stderr_path.display(), e),
        )
    })?;

    let child = tokio::process::Command::new(executable)
        .current_dir(work_dir)
        .arg(format!("USERID={}", userid))
        .arg("MODE='OUT'")
        .arg(format!("SQLFILE='{}'", sql_path.display()))
        .arg(format!("DATA='{}'", data_path.display()))
        .arg(format!("LOG='{}'", log_path.display()))
        .arg(format!("TASK_THREAD_NUMBER={}", threads))
        .arg("LOG_LEVEL=1")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|e| {
            D2DError::external(
                "dmfldr",
                format!(
                    "failed to spawn dmfldr '{}' (is it installed and on PATH?): {}",
                    executable, e
                ),
            )
        })?;

    Ok(SqlExportProcess {
        data_path: data_path.to_path_buf(),
        log_path: log_path.to_path_buf(),
        stdout_path,
        stderr_path,
        executable: executable.to_string(),
        child,
        started,
    })
}

impl SqlExportProcess {
    /// 等待导出结束并解析统计；退出码非 0 / 产物缺失都是错误
    pub async fn wait(mut self) -> D2DResult<ExportStats> {
        let status =
            self.child.wait().await.map_err(|e| {
                D2DError::external("dmfldr", format!("等待 dmfldr 退出失败: {}", e))
            })?;
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        let stdout = std::fs::read(&self.stdout_path)
            .map(|b| decode_dm_text(&b))
            .unwrap_or_default();
        let stderr = std::fs::read(&self.stderr_path)
            .map(|b| decode_dm_text(&b))
            .unwrap_or_default();

        if !status.success() {
            let log_tail = tail_file(&self.log_path, 4000);
            return Err(map_exit(
                status.code(),
                &stdout,
                &stderr,
                &log_tail,
                &self.executable,
            ));
        }
        if !self.data_path.exists() {
            return Err(D2DError::external(
                "dmfldr",
                format!(
                    "dmfldr reported success but data file {} was not produced",
                    self.data_path.display()
                ),
            ));
        }
        // ⚠️ 统计行（`读取的逻辑记录总数:`）dmfldr 是打到 **stdout** 上的，LOG 文件里不一定有。
        // 曾经这里传的是空串、只读日志文件 → 分片行数恒为 0 → 「各分片日志行数之和」
        // 恒为 0 → 分片级行数对账被静默跳过（不是在报错，是根本没生效）。
        // 与 TABLE 路径的 `run_export` 保持一致：stdout + stderr + 日志文件三处都看。
        let mut stats =
            parse_log_stats(&push_str_lines(&stdout, &stderr), &self.log_path).unwrap_or_default();
        stats.elapsed_ms = elapsed_ms;
        if stats.rows_read == 0 {
            // 「日志没报行数」与「这一片本来就是空的」是两件事，必须分开报。
            //
            // dmfldr 对**导出 0 行**的分片不打印 `N rows is load out` 进度行，
            // 所以空分片必然走到这里。此时 0 就是正确答案，报警只会制造噪音 ——
            // 实测日期区间切分下后几片常常是空的（数据分布不均），
            // 一路 WARN 刷屏会把真正的问题（非空分片日志解析失败）淹掉。
            //
            // 判据用数据文件大小：文件为空 → 确属空分片；文件非空却没报数 → 真问题。
            let data_bytes = std::fs::metadata(&self.data_path)
                .map(|m| m.len())
                .unwrap_or(0);
            if data_bytes == 0 {
                tracing::debug!(
                    data = %self.data_path.display(),
                    "本分片导出 0 行（数据文件为空），不参与行数对账是正常的"
                );
            } else {
                tracing::warn!(
                    data = %self.data_path.display(),
                    data_bytes,
                    "dmfldr 未报出导出行数，但数据文件非空 —— 本分片按 0 行参与对账，\
                     这会表现为「行数对账失败」而非静默丢数（检查 dmfldr 版本日志措辞）"
                );
            }
        }
        tracing::debug!(
            data = %self.data_path.display(),
            elapsed_ms,
            rows = stats.rows_read,
            "dmfldr: sql export ok"
        );
        Ok(stats)
    }
}

/// 解码达梦 dmfldr 的文本输出/日志
///
/// 中文 Linux 上 dmfldr 的控制台输出与 `LOG=` 文件都是 **GBK** 编码。直接按 UTF-8 读
/// 会得到替换字符（`read_to_string` 更是直接返回 Err、整个日志都读不到），
/// 于是「读取的逻辑记录总数」等标签永远匹配不上 —— 实测 `rows_exported` 恒为 0，
/// 连带行数对账被静默跳过。先试 UTF-8，失败再按 GBK 解。
pub(crate) fn decode_dm_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            let (cow, _, _) = encoding_rs::GBK.decode(bytes);
            cow.into_owned()
        }
    }
}

pub fn parse_log_stats(texts: &str, log_path: &Path) -> Option<ExportStats> {
    let mut content = texts.to_string();
    if let Ok(bytes) = std::fs::read(log_path) {
        content.push('\n');
        content.push_str(&decode_dm_text(&bytes));
    }
    let mut stats = ExportStats::default();
    let mut matched = false;
    for line in content.lines() {
        let line = line.trim();
        if let Some(v) = extract_u64(line, "读取的逻辑记录总数:") {
            stats.rows_read = v;
            matched = true;
        } else if let Some(v) = extract_u64(line, "拒绝的逻辑记录总数:") {
            stats.rows_rejected = v;
        } else if let Some(v) = extract_u64(line, "跳过的逻辑记录总数:") {
            stats.rows_skipped = v;
        } else if let Some(v) = extract_u64(line, "用时:") {
            stats.elapsed_ms = v;
        } else if let Some(v) = u64_before(line, "rows is load out") {
            // SQLFILE 模式（见 extract_rows_loaded 注释）。
            // 取**最大值**而不是最后一行：进度行是累进的，取 max 既得到总数，
            // 又对「多线程输出乱序」免疫。
            stats.rows_read = stats.rows_read.max(v);
            matched = true;
        } else if let Some(v) = u64_before(line, "行数据已导出") {
            // **控制文件（`TABLE=`）模式**的行数来源。
            //
            // 实测日志（dmfldr V8，`MODE=OUT CONTROL=`，`character_code: utf8`，逐字节抄的）：
            // ```text
            // dmfldr: 2026-10-16 17:27:59  export success.
            // dmfldr: 2026-10-16 17:27:59  19151537 行数据已导出
            // 用时: 158785.644(ms)
            // ```
            // 数字在标记**前面**，所以用 `u64_before` 而不是 `extract_u64`。
            stats.rows_read = stats.rows_read.max(v);
            matched = true;
        }
    }
    if matched {
        Some(stats)
    } else {
        None
    }
}

/// 从**标记前面**取那个数字（标记与数字之间的空白会被跳过）
///
/// 两种导出模式的行数都是"数字在前、标记在后"，只是标记不同：
///
/// ```text
/// SQLFILE 模式：  `1773 rows is load out`        ← 英文进度行，累进，取最大
/// TABLE=  模式：  `19151537 行数据已导出`         ← 控制文件模式的收尾统计
/// ```
///
/// ⚠️ SQLFILE 模式的实测细节（dmfldr V8）：**不打印**控制文件那套中文统计块
/// （`读取的逻辑记录总数:` 一行都没有），无论 `LOG_LEVEL` 传 1 / 3 / 不传，
/// stdout、stderr、LOG 文件三处都找不到；只有英文进度行，
/// 且**前面的值是中间进度、最后一行才是总数**（实测 1773 行的导出打出 1500、1773）。
/// 所以调用方一律**取最大** —— 既是总数，又对多线程交错输出的乱序免疫。
fn u64_before(line: &str, marker: &str) -> Option<u64> {
    let idx = line.find(marker)?;
    let digits: String = line[..idx]
        .trim_end()
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// 从 "标签:数值" 行提取数值（容忍毫秒小数，如 用时:20.522(ms)）
///
/// 注意：dmfldr 实际输出是 `读取的逻辑记录总数: 19151537` —— **冒号后有空格**。
/// 旧实现不跳空格，`take_while(is_ascii_digit)` 遇到首个空格就停，永远返回 None，
/// 于是 rows_read 恒为 0、行数对账被静默跳过。
fn extract_u64(line: &str, label: &str) -> Option<u64> {
    let idx = line.find(label)?;
    let rest = line[idx + label.len()..].trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// 清理导出目录（契约：close 时调用；失败只告警）
pub fn cleanup_dir(dir: &Path) {
    if dir.exists() {
        if let Err(e) = std::fs::remove_dir_all(dir) {
            tracing::warn!(dir = %dir.display(), error = %e, "failed to cleanup dmfldr export dir");
        }
    }
}

/// 创建唯一导出目录：`{work_dir}/{table}_{uuid}`
pub fn make_export_dir(work_dir: &Path, table: &str) -> D2DResult<PathBuf> {
    // 目录名带 **pid**：进程被 `kill -9` 之后导出物没人清，靠它在下一次启动时认回来
    // （见 [`sweep_stale_export_dirs`]）。实测一次 kill 会留下 ~7.6 GB。
    let dir = work_dir.join(format!(
        "{}_{}_{}",
        sanitize(table),
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).map_err(|e| {
        D2DError::external(
            "dmfldr",
            format!("failed to create dmfldr work dir {}: {}", dir.display(), e),
        )
    })?;
    Ok(dir)
}

/// 清掉 `work_dir` 里**属于已死进程**的导出目录，返回清掉几个
///
/// # 为什么需要它
///
/// 导出物（数据文件 + 控制文件，1915 万行的表约 7.6 GB）在正常路径上由
/// `cleanup()` 删掉。而 `kill -9` 不给任何清理机会 —— 实测一次中途 kill 之后
/// `fldr_work` 里躺着 7.6 GB，且**没有任何东西会再来清它**（路径里是随机 uuid，
/// 下一次运行只会新建一个）。反复被杀就会把盘填满。
///
/// # 只清"确定已死"的
///
/// 目录名形如 `{表}_{pid}_{uuid}`。判活看 `/proc/{pid}`（这个项目跑批在 Linux）；
/// 别的平台一个都不清 —— 错删**另一个正在跑的任务**的导出物会把那次迁移搞坏，
/// 而残留只是占盘。旧格式（不带 pid）的目录无法判断归属，也一律留着。
pub fn sweep_stale_export_dirs(work_dir: &Path) -> usize {
    if !cfg!(target_os = "linux") {
        return 0;
    }
    let Ok(entries) = std::fs::read_dir(work_dir) else {
        return 0;
    };
    let me = std::process::id();
    let mut n = 0;
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        // 从右往左拆：`{table}_{pid}_{uuid}`
        let mut parts = name.rsplitn(3, '_');
        let (_uuid, pid_s, table) = (parts.next(), parts.next(), parts.next());
        let (Some(pid_s), Some(table)) = (pid_s, table) else {
            continue;
        };
        if table.is_empty() {
            continue;
        }
        let Ok(pid) = pid_s.parse::<u32>() else {
            continue;
        };
        if pid == me || Path::new(&format!("/proc/{}", pid)).exists() {
            continue;
        }
        if std::fs::remove_dir_all(e.path()).is_ok() {
            n += 1;
        }
    }
    n
}

/// 表名安全化（目录名）
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_stats() {
        let text = "dmfldr: header noise\n\
                    3 行加载成功。\n\
                    跳过的逻辑记录总数:0\n\
                    读取的逻辑记录总数:3\n\
                    拒绝的逻辑记录总数:0\n\
                    用时:20.522(ms)\n";
        let stats = parse_log_stats(text, Path::new("/nonexistent.log")).unwrap();
        assert_eq!(stats.rows_read, 3);
        assert_eq!(stats.rows_rejected, 0);
        assert_eq!(stats.rows_skipped, 0);
        assert_eq!(stats.elapsed_ms, 20);
    }

    #[test]
    fn test_extract_stats_missing() {
        assert!(parse_log_stats("no stats here", Path::new("/nonexistent.log")).is_none());
    }

    #[test]
    fn test_infer_data_file_from_ctl() {
        let dir = tempfile::tempdir().unwrap();
        let ctl = dir.path().join("t.ctl");
        std::fs::write(
            &ctl,
            "LOAD DATA\nINFILE '/data/out.txt' STR X '0A'\nINTO TABLE t\n",
        )
        .unwrap();
        assert_eq!(
            infer_data_file(&ctl).unwrap(),
            PathBuf::from("/data/out.txt")
        );
    }

    #[test]
    fn test_infer_data_file_relative() {
        let dir = tempfile::tempdir().unwrap();
        let ctl = dir.path().join("t.ctl");
        std::fs::write(&ctl, "LOAD DATA\nINFILE 'out.txt'\nINTO TABLE t\n").unwrap();
        assert_eq!(infer_data_file(&ctl).unwrap(), dir.path().join("out.txt"));
    }

    #[test]
    fn test_sanitize_table_name() {
        assert_eq!(sanitize("t/a:b"), "t_a_b");
        assert_eq!(sanitize("t_a-1"), "t_a-1");
    }

    #[test]
    fn test_map_exit_retryable_on_connection_error() {
        let e = map_exit(Some(1), "", "连接数据库失败", "", "dmfldr");
        assert!(
            e.is_retryable(),
            "connection failure should be retryable: {}",
            e
        );
        let e2 = map_exit(Some(1), "", "invalid parameter", "", "dmfldr");
        assert!(!e2.is_retryable());
    }

    /// 回归：dmfldr 的日志与控制台输出是 **GBK**。
    /// 旧实现按 UTF-8 读（`read_to_string` 直接 Err / `from_utf8_lossy` 得到乱码），
    /// 中文标签永远匹配不上 → `rows_exported` 恒为 0 → 行数对账被静默跳过。
    /// SQLFILE 模式的行数来源：英文进度行，且必须取**最大**值
    ///
    /// 实测输出（同一份 1773 行的导出，LOG_LEVEL=3 时）：
    /// `1500 rows is load out` → `1773 rows is load out`
    /// 取最后一行是 1773（对），取第一行是 1500（错，会低估 → 对账误报丢数）。
    #[test]
    fn test_parse_rows_loaded_sqlfile_mode() {
        let text = "dmfldr V8\n1500 rows is load out\n1773 rows is load out\n";
        let s = parse_log_stats(text, Path::new("/nonexistent.log")).unwrap();
        assert_eq!(s.rows_read, 1773, "必须取最大进度值（= 最终总数）");

        // 单行形态（LOG_LEVEL=1 / 缺省）
        let s2 = parse_log_stats(
            "dmfldr V8\n1773 rows is load out\n",
            Path::new("/nonexistent.log"),
        );
        assert_eq!(s2.unwrap().rows_read, 1773);

        // 乱序也要取最大
        let s3 = parse_log_stats(
            "1773 rows is load out\n1500 rows is load out\n",
            Path::new("/nonexistent.log"),
        );
        assert_eq!(s3.unwrap().rows_read, 1773);

        // 0 行导出也必须被识别为「解析到了」，而不是「没解析到」
        let s4 = parse_log_stats("0 rows is load out\n", Path::new("/nonexistent.log")).unwrap();
        assert_eq!(s4.rows_read, 0);
    }

    #[test]
    fn test_u64_before_shapes() {
        // SQLFILE 模式
        assert_eq!(
            u64_before("1773 rows is load out", "rows is load out"),
            Some(1773)
        );
        assert_eq!(
            u64_before("  1500 rows is load out  ", "rows is load out"),
            Some(1500)
        );
        assert_eq!(
            u64_before("0 rows is load out", "rows is load out"),
            Some(0)
        );
        // TABLE= 模式：数字与标记之间**有一个空格**，必须跳过
        assert_eq!(
            u64_before(
                "dmfldr: 2026-10-16 17:27:59  19151537 行数据已导出",
                "行数据已导出"
            ),
            Some(19_151_537)
        );
        assert_eq!(
            u64_before("19151537 行数据已导出", "行数据已导出"),
            Some(19_151_537)
        );
        // 无数字前缀 / 无该串 → None（不能把别的东西误认成行数）
        assert_eq!(u64_before("rows is load out", "rows is load out"), None);
        assert_eq!(u64_before("行数据已导出", "行数据已导出"), None);
        assert_eq!(
            u64_before("读取的逻辑记录总数: 19151537", "行数据已导出"),
            None,
            "标记必须对得上，不能拿别处的数字凑"
        );
    }

    /// **回归**：`TABLE=`（控制文件）模式的行数**曾经根本解析不出来**。
    ///
    /// 下面三行是 2026-10-16 在 `.39` 上跑 `MODE=OUT CONTROL=` 真实产出的日志
    /// （`character_code: utf8`，**逐字节抄下来**的，含那两处双空格与行尾的 `(ms)`）。
    ///
    /// 修之前解析器只认 `读取的逻辑记录总数:`（控制文件模式其实不打这个标签）
    /// 与 `rows is load out`（SQLFILE 模式才有）—— 两条都不匹配 ⟹ `rows_read` 恒为 0
    /// ⟹ 收尾行数对账被**静默跳过**。实测后果：那一档丢了 1 行、**退出码还是 0**。
    #[test]
    fn test_parse_log_stats_table_mode() {
        let text = "dmfldr: 2026-10-16 17:27:59  export success.\n\
                    dmfldr: 2026-10-16 17:27:59  19151537 行数据已导出\n\
                    用时: 158785.644(ms)\n";
        let s = parse_log_stats(text, Path::new("/nonexistent.log")).unwrap();
        assert_eq!(s.rows_read, 19_151_537);
        assert_eq!(s.elapsed_ms, 158_785, "行尾的 (ms) 与小数不能把数字截错");
    }

    /// **回归**：dmfldr 的统计行走 **stdout**，不是 LOG 文件。
    ///
    /// 历史 bug：`run_sql_export` 给 `parse_log_stats` 传了空串、只读日志文件 →
    /// 分片行数恒为 0 → 「各分片日志行数之和」恒为 0 → 分片级行数对账**静默失效**
    /// （不报错，是根本没生效）。用假 dmfldr 脚本把这条链路整条跑一遍。
    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_sql_export_reads_stats_from_stdout() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        // 假 dmfldr：统计只打到 stdout（与真实行为一致），LOG 文件留空
        let exe = dir.path().join("fake_dmfldr");
        std::fs::write(
            &exe,
            r#"#!/bin/bash
for a in "$@"; do
  case "$a" in
    DATA=*) d=$(echo "$a" | cut -d= -f2- | tr -d "'"); echo "x|y" > "$d" ;;
    LOG=*)  l=$(echo "$a" | cut -d= -f2- | tr -d "'"); : > "$l" ;;
  esac
done
echo "424242 rows is load out"
"#,
        )
        .unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sql_path = dir.path().join("q.sql");
        let data_path = dir.path().join("q.txt");
        let log_path = dir.path().join("q.log");
        std::fs::write(&sql_path, "SELECT 1;\n").unwrap();

        let stats = run_sql_export(
            exe.to_str().unwrap(),
            "u/\"p\"@h:1",
            &sql_path,
            &data_path,
            &log_path,
            dir.path(),
            2,
        )
        .await
        .unwrap();

        assert_eq!(
            stats.rows_read, 424242,
            "统计必须能从 stdout 解析到（LOG 文件里没有这一行）"
        );
    }

    #[test]
    fn test_parse_stats_from_gbk_log() {
        let text = "跳过的逻辑记录总数: 0\n读取的逻辑记录总数: 19151537\n拒绝的逻辑记录总数: 7\n用时:122553.4(ms)\n";
        let (gbk, _, had_err) = encoding_rs::GBK.encode(text);
        assert!(!had_err);

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d2d_fldr.log");
        std::fs::write(&p, &gbk).unwrap();

        let s = parse_log_stats("", &p).expect("应能从 GBK 日志中解析出统计");
        assert_eq!(s.rows_read, 19_151_537);
        assert_eq!(s.rows_rejected, 7);
        assert_eq!(s.rows_skipped, 0);
    }

    #[test]
    fn test_decode_dm_text_passes_utf8_through() {
        assert_eq!(
            decode_dm_text("读取的逻辑记录总数: 3".as_bytes()),
            "读取的逻辑记录总数: 3"
        );
        let (gbk, _, _) = encoding_rs::GBK.encode("中文字段");
        assert_eq!(decode_dm_text(&gbk), "中文字段");
    }

    // ───────── kill -9 残留的清理 ─────────

    /// 目录名里必须带 pid —— 下次启动就是靠它认出"这是死进程留下的"
    #[test]
    fn test_export_dir_name_carries_pid() {
        let d = tempfile::tempdir().unwrap();
        let dir = make_export_dir(d.path(), "T_DEMO").unwrap();
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with(&format!("T_DEMO_{}_", std::process::id())),
            "目录名应当是 {{表}}_{{pid}}_{{uuid}}：{}",
            name
        );
        assert!(dir.is_dir());
    }

    /// 只清**确定已死**的：自己 pid 的、非本格式的、以及同名文件都不动
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sweep_removes_only_dead_process_dirs() {
        let d = tempfile::tempdir().unwrap();
        let dead = u32::MAX; // Linux 的 pid 上限远小于 u32::MAX
        let me = std::process::id();

        let dead_dir = d.path().join(format!("T_A_{}_abc123", dead));
        let live_dir = d.path().join(format!("T_A_{}_abc123", me));
        let other_table_dead = d.path().join(format!("T_B_{}_def456", dead));
        let old_format = d.path().join("T_A_notauuid");
        let weird_file = d.path().join(format!("T_A_{}_file", dead));
        for x in [&dead_dir, &live_dir, &other_table_dead, &old_format] {
            std::fs::create_dir_all(x).unwrap();
            std::fs::write(x.join("data.txt"), b"x").unwrap();
        }
        std::fs::write(&weird_file, b"x").unwrap();

        let n = sweep_stale_export_dirs(d.path());
        assert_eq!(n, 2, "只该清掉两个已死进程的目录");
        assert!(!dead_dir.exists(), "死进程的目录应当被清掉");
        assert!(!other_table_dead.exists(), "别的表名、同样死进程的也该清");
        assert!(live_dir.exists(), "自己进程的目录不能动");
        assert!(
            old_format.exists(),
            "旧格式（不带 pid）无法判断归属，留着手工处理"
        );
        assert!(weird_file.exists(), "同名文件不是目录，不动");
    }

    /// 工作目录不存在时安静返回 0（还没开始跑就校验，不该报错）
    #[test]
    fn test_sweep_tolerates_missing_dir() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(sweep_stale_export_dirs(&d.path().join("nope")), 0);
    }
}
