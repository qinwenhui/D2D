//! dmfldr 分片并发导出（`export_mode: partitioned`）的**分片规划**
//!
//! ## 职责边界
//!
//! 切法（怎么切、切几片）由内核 [`d2d_connector::partition`] 决定；
//! 本模块只做两件连接器该做的事：
//!
//! 1. **回答边界问题** —— 表的分区边界（`USER_TAB_PARTITIONS.HIGH_VALUE`，不扫数据）
//!    或分区列的真实行数分布（全表 `GROUP BY`，慢）
//! 2. **把结构化谓词渲染成达梦 SQL** —— 见 [`crate::source::sql::render_partition_predicate`]
//!
//! ## 四条规划路径
//!
//! | 来源 | 触发 | 探测 | 覆盖性保证 |
//! |---|---|---|---|
//! | **用户声明** | `partition.mode: list / range(带上下界) / hash / none` | **零** | 用户自负（同 DataX `where` 契约） |
//! | **复用计划** | `dmfldr.plan_file` 存在 | **零** | 同上次 |
//! | **分区元数据** | `mode: range` 不给边界（缺省） | **~0.1s**，不扫数据 | 切点**首尾无界**，并集恒等于值域 |
//! | **值探测** | 元数据不可用时的退化路径 | **~31s**（1915 万行） | 弱：靠 `IN (值列表)`，另有行数交叉校验兜底 |
//!
//! 为什么缺省选「不均衡」的元数据路径：**均衡性在这个架构里是免费的**。
//! 分片导出与下游写入是重叠的（导完一片立即交付），只要导出总时长 < 加载总时长，
//! 导出侧的倾斜就被完全吸收。实测：导出 17s vs 加载 88s，探测省下的 31s 是净赚。
//!
//! ## 值探测路径为什么要额外守卫
//!
//! 它用 `IN (值列表)` 表达分片 —— 漏一个值就静默少一批数据。
//! 两道防线：NULL 直接拒绝（`IN` 永远匹配不上 NULL），
//! 以及「各分片 dmfldr 日志行数之和 == 探测总数」的交叉校验。

use super::params::PartitionPlan;
use super::proc;
use crate::config::ConnectionConfig;
use crate::source::sql::{render_partition_predicate, sql_literal_of};
use d2d_connector::partition::{
    self as kpart, Literal, PartitionMode, PartitionPlan as KernelPlan, PartitionPredicate,
    PartitionSpec, ProbedBounds,
};
use d2d_core::{D2DError, D2DResult};

use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// 一个分区的 (值, 行数)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStat {
    pub value: String,
    pub rows: u64,
}

/// 一个导出分片
#[derive(Debug, Clone)]
pub struct Shard {
    pub index: usize,
    pub sql_path: PathBuf,
    pub data_path: PathBuf,
    /// 预期行数；元数据路径下未知（= 0），仅用于「文件空但预期非空」的告警
    pub rows: u64,
    /// 人类可读的分片取值范围（日志/排查用）
    pub describe: String,
}

/// 分片规划的输入（把散落的参数收成一个，免得 `plan_shards` 有八个位置参数）
pub struct ShardPlanInput<'a> {
    pub schema: Option<&'a str>,
    pub table: &'a str,
    /// 导出列清单（来自 `type_contract`，也是解析时的权威 schema）
    pub columns: &'a [String],
    /// 用户分区意图（`source.partition`，或兼容旧 `source.split` 转换而来）
    pub partition: Option<&'a PartitionSpec>,
    /// 用户 WHERE 片段（partitioned 模式支持；与分区谓词 AND 组合）
    pub user_where: Option<&'a str>,
    pub dir: &'a Path,
}

/// 把分区按行数 LPT 装箱成 `groups` 组
///
/// 只按组数均分（轮询）在分区大小悬殊时会让某一片成为长尾 —— 实测该表 257 个分区里
/// 大分区 30 万行、小分区 28 行，轮询分片会让总耗时由最大的那片决定。
pub fn pack_partitions(mut parts: Vec<PartitionStat>, groups: usize) -> Vec<Vec<PartitionStat>> {
    let groups = groups.max(1);
    parts.sort_by_key(|p| std::cmp::Reverse(p.rows)); // 大者优先
    let mut buckets: Vec<(u64, Vec<PartitionStat>)> =
        (0..groups).map(|_| (0, Vec::new())).collect();
    for p in parts {
        // 找到当前累计行数最小的组
        let idx = buckets
            .iter()
            .enumerate()
            .min_by_key(|(_, (sum, _))| *sum)
            .map(|(i, _)| i)
            .unwrap_or(0);
        buckets[idx].0 += p.rows;
        buckets[idx].1.push(p);
    }
    buckets
        .into_iter()
        .map(|(_, v)| v)
        .filter(|v| !v.is_empty())
        .collect()
}

/// SQL 字面量渲染：数值原样、时间用 `TIMESTAMP'...'`、其余按字符串加引号
///
/// 分区值来自 dmfldr 的文本输出（时间列形如 `2022-01-01 00:00:00.000000`），
/// 只能靠形态猜类型。已经解析成 [`Literal`] 的走 [`crate::source::sql::sql_literal_of`]。
pub fn sql_literal(raw: &str) -> String {
    let t = raw.trim();
    if !t.is_empty()
        && t.chars().all(|c| c.is_ascii_digit() || c == '-')
        && t.chars().any(|c| c.is_ascii_digit())
    {
        return t.to_string();
    }
    let looks_dt = {
        let b = t.as_bytes();
        b.len() >= 10
            && b[0..4].iter().all(|c| c.is_ascii_digit())
            && b[4] == b'-'
            && b[5..7].iter().all(|c| c.is_ascii_digit())
            && b[7] == b'-'
            && b[8..10].iter().all(|c| c.is_ascii_digit())
    };
    if looks_dt {
        format!("TIMESTAMP'{}'", t)
    } else {
        format!("'{}'", t.replace('\'', "''"))
    }
}

/// 解析探测输出（每行 `值|行数`）
///
/// **空值行会被保留**（`value == ""`）而不是就地丢弃：那代表分区列上存在 NULL，
/// 而分片 SQL 用的是 `IN (值列表)` —— NULL 永远匹配不上，这些行会被静默漏掉。
/// 把判断权交给调用方，才能显式失败而不是悄悄少数据。
pub fn parse_probe(content: &str, field_sep: char) -> Vec<PartitionStat> {
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            continue;
        }
        let mut it = line.rsplitn(2, field_sep);
        let rows_s = it.next().unwrap_or("").trim();
        let value = it.next().unwrap_or("").trim();
        if let Ok(rows) = rows_s.parse::<u64>() {
            out.push(PartitionStat {
                value: value.to_string(),
                rows,
            });
        }
    }
    out
}

/// 探测分区值清单（退化路径：要扫全表）
pub async fn probe_partitions(
    params: &super::params::FlDrParams,
    conn: &ConnectionConfig,
    schema: Option<&str>,
    table: &str,
    column: &str,
    dir: &Path,
) -> D2DResult<Vec<PartitionStat>> {
    let tbl = table_sql(schema, table);
    let sql_path = dir.join("probe_partitions.sql");
    let data_path = dir.join("probe_partitions.txt");
    std::fs::write(
        &sql_path,
        format!(
            "SELECT {}, COUNT(*) FROM {} GROUP BY {};\n",
            column, tbl, column
        ),
    )
    .map_err(|e| D2DError::external("dmfldr", format!("写探测 SQL 失败: {}", e)))?;

    proc::run_sql_export(
        &params.executable,
        &conn.to_userid(),
        &sql_path,
        &data_path,
        &dir.join("probe_partitions.log"),
        dir,
        params.task_thread_number,
    )
    .await?;

    let bytes = std::fs::read(&data_path)
        .map_err(|e| D2DError::external("dmfldr", format!("读探测结果失败: {}", e)))?;
    let sep = params.field_seperator.chars().next().unwrap_or('|');
    let parts = parse_probe(&proc::decode_dm_text(&bytes), sep);
    let total: u64 = parts.iter().map(|p| p.rows).sum();
    info!(
        partitions = parts.len(),
        total_rows = total,
        column = %column,
        "dmfldr: 分区探测完成"
    );
    Ok(parts)
}

// ─────────────────────────── 元数据路径：按分区边界取切点 ───────────────────────────

/// 从分区元数据取边界值（`USER_TAB_PARTITIONS.HIGH_VALUE`）
///
/// **不用扫任何数据**：实测 0.13s，而全表 `GROUP BY` 探测稳定 31s（且加大线程数无效 ——
/// 瓶颈在服务端扫描，不在客户端取数）。
pub async fn fetch_partition_bounds(
    params: &super::params::FlDrParams,
    conn: &ConnectionConfig,
    schema: Option<&str>,
    table: &str,
    dir: &Path,
) -> D2DResult<Vec<String>> {
    let esc = |s: &str| s.replace('\'', "''").to_uppercase();

    // 用哪个数据字典视图，取决于「登录用户 == 表属主」是否成立：
    //
    // - 相等 → `USER_TAB_PARTITIONS`（实测 dmfldr 下正常，0.13s）
    // - 不等 → `ALL_TAB_PARTITIONS` 并按 OWNER 过滤
    //
    // ⚠️ 实测：dmfldr 的 OUT 模式对 `ALL_TAB_PARTITIONS` 会报「获取表信息失败」
    // （退出码 1），而 `USER_TAB_PARTITIONS` 正常。所以**只有确实跨 schema 时才走
    // ALL_**，其余一律走 USER_ —— 否则会平白丢掉元数据快路，退化到 31s 的全表探测。
    let owner_is_login = match schema {
        None => true,
        Some(s) => s.eq_ignore_ascii_case(&conn.user),
    };
    let sql = if owner_is_login {
        format!(
            "SELECT HIGH_VALUE FROM USER_TAB_PARTITIONS WHERE TABLE_NAME='{}' \
             ORDER BY PARTITION_POSITION;\n",
            esc(table)
        )
    } else {
        format!(
            "SELECT HIGH_VALUE FROM ALL_TAB_PARTITIONS WHERE OWNER='{}' AND TABLE_NAME='{}' \
             ORDER BY PARTITION_POSITION;\n",
            esc(schema.unwrap_or_default()),
            esc(table)
        )
    };
    let sql_path = dir.join("partition_bounds.sql");
    let data_path = dir.join("partition_bounds.txt");
    std::fs::write(&sql_path, sql)
        .map_err(|e| D2DError::external("dmfldr", format!("写分区元数据 SQL 失败: {}", e)))?;

    proc::run_sql_export(
        &params.executable,
        &conn.to_userid(),
        &sql_path,
        &data_path,
        &dir.join("partition_bounds.log"),
        dir,
        params.task_thread_number,
    )
    .await?;

    let bytes = std::fs::read(&data_path)
        .map_err(|e| D2DError::external("dmfldr", format!("读分区元数据失败: {}", e)))?;
    let bounds = parse_high_values(&proc::decode_dm_text(&bytes));
    info!(
        table = %table,
        source_view = if owner_is_login { "USER_TAB_PARTITIONS" } else { "ALL_TAB_PARTITIONS" },
        bounds = bounds.len(),
        first = bounds.first().map(|s| s.as_str()).unwrap_or("-"),
        last = bounds.last().map(|s| s.as_str()).unwrap_or("-"),
        "dmfldr: 分区边界已取得"
    );
    Ok(bounds)
}

/// 解析 `HIGH_VALUE` 文本，还原成可用于 SQL 比较的字面量内容
///
/// 达梦把 HIGH_VALUE 渲染成带类型前缀的形式：`DATETIME'2022-01-01 00:00:00'`、
/// `'ABC'`、`12345`。这里剥掉外层包装只留内容，由渲染层负责重新加引号。
///
/// `MAXVALUE` 及其之后的分区（默认收容分区）没有有限上界，遇到即停止 —— 它不能当切点。
pub fn parse_high_values(content: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.eq_ignore_ascii_case("MAXVALUE") {
            break;
        }
        // 形如 TYPE'内容' 或 '内容'
        let inner = match line.find('\'') {
            Some(i) => {
                let rest = &line[i + 1..];
                match rest.rfind('\'') {
                    Some(j) if j > 0 || !rest.is_empty() => rest[..rest.len().min(j)].to_string(),
                    _ => continue,
                }
            }
            None => line.to_string(), // 裸数字
        };
        let inner = inner.trim().to_string();
        if !inner.is_empty() {
            out.push(inner);
        }
    }
    out
}

/// 边界文本 → 类型化切点（内核要的是 [`Literal`]，不是字符串）
///
/// 用 `Literal::parse` 而非 [`sql_literal`] 的形态猜测：切点需要参与**时间跨度**运算
/// （见内核 `range_cuts` 的文档），只有真正解析成日期才能按时间均分。
fn parse_cut_points(bounds: &[String]) -> Vec<Literal> {
    bounds.iter().map(|b| Literal::parse(b)).collect()
}

/// 从全部边界里**等距取样**出至多 `groups-1` 个内部切点（→ `groups` 段）
///
/// 为什么取样而不是全用：表有 257 个分区就切 257 段，等于要起 257 个 dmfldr 进程。
/// 实测 64 片相对 16 片只快 5%，内存却涨 3.5× —— 均衡性收益被进程开销吃掉。
/// 想更细的均衡，显式把 `count` 调大即可（**显式选择**，不是默认行为）。
///
/// 取样只影响均衡，不影响覆盖性：内核拿到的仍是"首尾无界、连续"的切点，
/// 并集恒等于整个值域 —— 少取几个切点只会让某段更长，不会造成空洞。
fn pick_cuts(cuts: &[Literal], groups: usize) -> Vec<Literal> {
    let n = cuts.len();
    if n < 2 {
        return Vec::new();
    }
    let groups = groups.max(1).min(n - 1);
    let mut out: Vec<Literal> = Vec::with_capacity(groups.saturating_sub(1));
    for g in 1..groups {
        let idx = g * n / groups;
        if idx > 0 && idx < n {
            let c = cuts[idx].clone();
            if out.last() != Some(&c) {
                out.push(c);
            }
        }
    }
    out
}

fn table_sql(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => {
            let q = |x: &str| format!("\"{}\"", x.replace('"', "\"\""));
            format!("{}.{}", q(s), q(table))
        }
        None => format!("\"{}\"", table.replace('"', "\"\"")),
    }
}

fn write_shard(dir: &Path, i: usize, sql: String, rows: u64, describe: String) -> D2DResult<Shard> {
    let sql_path = dir.join(format!("shard_{:03}.sql", i));
    let data_path = dir.join(format!("shard_{:03}.txt", i));
    std::fs::write(&sql_path, sql)
        .map_err(|e| D2DError::external("dmfldr", format!("写分片 SQL 失败: {}", e)))?;
    Ok(Shard {
        index: i,
        sql_path,
        data_path,
        rows,
        describe,
    })
}

/// 组装分片 SQL：`SELECT <列> FROM <表> [WHERE (用户条件) AND (分区谓词)]`
fn shard_sql_predicated(
    tbl: &str,
    columns: &[String],
    user_where: Option<&str>,
    predicate: &str,
) -> String {
    let mut conds: Vec<String> = Vec::new();
    // 用户条件在前，分区谓词在后 —— 与 ODBC 路径 `build_query` 的组合顺序一致
    if let Some(w) = user_where {
        conds.push(format!("({})", w));
    }
    let p = predicate.trim();
    if !p.is_empty() {
        conds.push(format!("({})", p));
    }
    let w = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join("\n  AND "))
    };
    format!("SELECT {} FROM {}{};\n", columns.join(", "), tbl, w)
}

/// 把一个内核分区计划物化成本轮的分片（SQL 落回导出目录）
fn materialize_kernel(
    plan: &KernelPlan,
    tbl: &str,
    columns: &[String],
    user_where: Option<&str>,
    dir: &Path,
) -> D2DResult<Vec<Shard>> {
    let mut out = Vec::with_capacity(plan.partitions.len());
    for (i, p) in plan.partitions.iter().enumerate() {
        let pred = render_partition_predicate(&p.predicate);
        let sql = shard_sql_predicated(tbl, columns, user_where, &pred);
        out.push(write_shard(dir, i, sql, 0, p.describe.clone())?);
    }
    Ok(out)
}

/// 分片规划的来源（写日志用，也是排查「这次为什么这么慢」的第一线索）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardPlanSource {
    /// 用户显式声明（list / range+上下界 / hash / none）—— **零探测**
    User {
        mode: PartitionMode,
        partitions: usize,
    },
    /// 复用已落盘的计划 —— **零探测**
    Recorded { path: PathBuf, partitions: usize },
    /// 分区元数据切点：不扫数据（实测 ~0.1s）
    PartitionMetadata { partitions: usize },
    /// 退化路径：全表 GROUP BY 探测 + LPT 装箱（实测 1915 万行 ~31s）
    ValueProbe { partitions: usize, total_rows: u64 },
}

/// 规划分片
///
/// 决策顺序（**能零探测就零探测**）：
///
/// 1. 用户声明了 `partition` 且该模式不需要边界 → 内核纯函数直接出分片
/// 2. `range` 不给边界 → 先看 `plan_file` 缓存，再走分区元数据，最后退化到全表探测
/// 3. `range` 给了边界 → 同 1（内核按类型均分）
///
/// 返回 `(分片列表, 规划来源, 源端预期总行数)`；元数据路径下总行数未知（`None`），
/// 由各分片 dmfldr 日志求和得出。
pub async fn plan_shards(
    params: &super::params::FlDrParams,
    conn: &ConnectionConfig,
    input: ShardPlanInput<'_>,
) -> D2DResult<(Vec<Shard>, ShardPlanSource, Option<u64>)> {
    let ShardPlanInput {
        schema,
        table,
        columns,
        partition,
        user_where,
        dir,
    } = input;
    let user_where = user_where.map(str::trim).filter(|s| !s.is_empty());
    let tbl = table_sql(schema, table);

    if let Some(ps) = partition {
        ps.validate()?;
    }

    // ── A) 零探测：用户显式声明了切法 ──
    if let Some(ps) = partition {
        if ps.is_probe_free() {
            let plan = kpart::plan(ps, table, schema, None)?;
            let shards = materialize_kernel(&plan, &tbl, columns, user_where, dir)?;
            info!(
                mode = ps.mode.as_str(),
                shards = shards.len(),
                "dmfldr: 分片规划完成（用户显式声明，未做任何探测）"
            );
            return Ok((
                shards,
                ShardPlanSource::User {
                    mode: ps.mode,
                    partitions: plan.partitions.len(),
                },
                None,
            ));
        }
    }

    // ── B) range 不给边界：回退探测 ──
    //
    // 分区列只来自 `partition.column` —— 旧的 `dmfldr.partition_column` 已并入
    let column = partition
        .and_then(|p| p.column.clone())
        .ok_or_else(|| {
            D2DError::config(
                "分片模式需要 source.partition.column（用于切分的列）——                  如 `partition: {mode: range, column: CREATED_AT, count: 16}`",
            )
        })?;

    // 切几片 —— 只由 `partition.count` 决定。
    // 并发上限**不在这里**：它由引擎的闸门管，`FlDrParams` 里已经没有那个字段了。
    let target_shards = partition.map(|p| p.count).filter(|c| *c > 0).unwrap_or(1);

    let spec_for_plan; // 保住临时 spec 的生命周期
    let spec: &PartitionSpec = match partition {
        Some(p) => p,
        None => {
            spec_for_plan = PartitionSpec {
                mode: PartitionMode::Range,
                column: Some(column.clone()),
                count: target_shards,
                ..Default::default()
            };
            &spec_for_plan
        }
    };
    let fingerprint = spec.fingerprint(table, schema);

    // ── B0) 复用已落盘的计划：一次发现都不做 ──
    if let Some(pf) = &params.plan_file {
        if !params.plan_refresh {
            if let Some(plan) = kpart::load_plan(pf, &fingerprint)? {
                let shards = materialize_kernel(&plan, &tbl, columns, user_where, dir)?;
                let n = shards.len();
                let resolved = plan.resolved_by;
                info!(
                    plan_file = %pf.display(),
                    shards = n,
                    resolved_by = ?resolved,
                    "dmfldr: 复用已记录的分片计划（未做发现）"
                );
                return Ok((
                    shards,
                    ShardPlanSource::Recorded {
                        path: pf.clone(),
                        partitions: n,
                    },
                    plan.expected_rows,
                ));
            }
        }
    }

    // ── B1) 快路：分区元数据切点（不扫数据）──
    if params.partition_plan != PartitionPlan::Probe {
        match fetch_partition_bounds(params, conn, schema, table, dir).await {
            Ok(bounds) if bounds.len() >= 2 => {
                // 257 个分区边界只取 15 个当切点 → 16 段（与"一个分区一个进程"区分开）
                let cuts = pick_cuts(&parse_cut_points(&bounds), target_shards);
                let plan = kpart::plan(spec, table, schema, Some(ProbedBounds::CutPoints(cuts)))?;
                if plan.partitions.len() >= 2 {
                    let shards = materialize_kernel(&plan, &tbl, columns, user_where, dir)?;
                    info!(
                        shards = shards.len(),
                        partitions = bounds.len(),
                        "dmfldr: 分片规划完成（分区元数据切点，未扫数据）"
                    );
                    save_cached(params, pf_of(params), &plan)?;
                    return Ok((
                        shards,
                        ShardPlanSource::PartitionMetadata {
                            partitions: bounds.len(),
                        },
                        None,
                    ));
                }
                if params.partition_plan == PartitionPlan::Metadata {
                    return Err(D2DError::config(format!(
                        "partition_plan=metadata 但分区边界不足以切分（仅取到 {} 条）——\
                         表 {} 需按 {} 做 RANGE 分区，或改用 partition_plan: probe / auto",
                        bounds.len(),
                        table,
                        column
                    )));
                }
                info!(bounds = bounds.len(), "分区边界不足以切分，退化为全表探测");
            }
            Ok(bounds) => {
                if params.partition_plan == PartitionPlan::Metadata {
                    return Err(D2DError::config(format!(
                        "partition_plan=metadata 但分区边界不足以切分（仅取到 {} 条）：\
                         表 {} 需按 {} 做 RANGE 分区，或改用 partition_plan: probe / auto",
                        bounds.len(),
                        table,
                        column
                    )));
                }
                info!(bounds = bounds.len(), "分区边界不足以切分，退化为全表探测");
            }
            Err(e) => {
                if params.partition_plan == PartitionPlan::Metadata {
                    return Err(D2DError::config(format!(
                        "partition_plan=metadata 但分区元数据查询失败：{}",
                        e
                    )));
                }
                info!(error = %e, "分区元数据不可用，退化为全表探测");
            }
        }
    }

    // ── B2) 退化路径：值探测 + LPT 装箱 ──
    let parts = probe_partitions(params, conn, schema, table, &column, dir).await?;
    if parts.is_empty() {
        return Err(D2DError::external(
            "dmfldr",
            "分区探测结果为空，无法分片导出",
        ));
    }

    // NULL 守卫：分片 SQL 用 `IN (值列表)`，NULL 永远匹配不上 —— 这些行会被静默漏掉。
    let null_rows: u64 = parts
        .iter()
        .filter(|p| p.value.trim().is_empty())
        .map(|p| p.rows)
        .sum();
    if null_rows > 0 {
        return Err(D2DError::config(format!(
            "分区列 {} 存在 {} 行 NULL：分片导出基于 `IN (值列表)`，NULL 无法被覆盖，\
             继续执行会静默丢数。请改用 method: odbc，或先处理这些 NULL 行。",
            column, null_rows
        )));
    }
    let parts: Vec<PartitionStat> = parts
        .into_iter()
        .filter(|p| !p.value.trim().is_empty())
        .collect();
    let partitions = parts.len();
    let total_rows: u64 = parts.iter().map(|p| p.rows).sum();

    let groups = pack_partitions(parts, target_shards.min(partitions).max(1));
    let kernel_partitions: Vec<kpart::Partition> = groups
        .into_iter()
        .enumerate()
        .map(|(i, g)| {
            let values: Vec<Literal> = g.iter().map(|p| Literal::parse(&p.value)).collect();
            // 值列表路径只能表达成 `IN (...)`，正是内核 `Raw` 逃生舱的用途
            let pred = format!(
                "{} IN ({})",
                column,
                values
                    .iter()
                    .map(sql_literal_of)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            kpart::Partition {
                id: format!("p{:03}", i),
                describe: format!("IN[{} 个分区值]", g.len()),
                predicate: PartitionPredicate::Raw(pred),
            }
        })
        .collect();

    let plan = KernelPlan {
        version: kpart::PLAN_VERSION,
        mode: PartitionMode::Range,
        resolved_by: kpart::ResolvedBy::Probe,
        fingerprint,
        expected_rows: Some(total_rows),
        partitions: kernel_partitions,
    };
    let shards = materialize_kernel(&plan, &tbl, columns, user_where, dir)?;
    info!(
        shards = shards.len(),
        total_rows, "dmfldr: 分片规划完成（探测 + LPT 装箱）"
    );
    save_cached(params, pf_of(params), &plan)?;
    Ok((
        shards,
        ShardPlanSource::ValueProbe {
            partitions,
            total_rows,
        },
        Some(total_rows),
    ))
}

fn pf_of(params: &super::params::FlDrParams) -> Option<&Path> {
    params.plan_file.as_deref()
}

fn save_cached(
    _params: &super::params::FlDrParams,
    path: Option<&Path>,
    plan: &KernelPlan,
) -> D2DResult<()> {
    let Some(pf) = path else {
        return Ok(());
    };
    kpart::save_plan(pf, plan)?;
    info!(
        plan_file = %pf.display(),
        partitions = plan.partitions.len(),
        "dmfldr: 分片计划已落盘（后续运行可直接复用）"
    );
    Ok(())
}

/// **启动**一个分片的导出，立即返回句柄（不等它结束）
///
/// 流式 source 用这个：进程还在写，读者就可以开始跟随读数据文件。
pub fn spawn_shard(
    params: &super::params::FlDrParams,
    conn: &ConnectionConfig,
    shard: &Shard,
    dir: &Path,
) -> D2DResult<proc::SqlExportProcess> {
    let log = dir.join(format!("shard_{:03}.log", shard.index));
    proc::spawn_sql_export(
        &params.executable,
        &conn.to_userid(),
        &shard.sql_path,
        &shard.data_path,
        &log,
        dir,
        params.task_thread_number,
    )
}

/// 退出码成功之后的产物校验：文件缺失 / 日志有行但文件为空都是静默丢数
pub fn verify_shard_output(shard: &Shard, stats: &proc::ExportStats) -> D2DResult<()> {
    // 退出码 0 但文件缺失 = 静默丢数据，必须显式失败
    if !shard.data_path.exists() {
        return Err(D2DError::external(
            "dmfldr",
            format!(
                "分片 {} 导出成功但数据文件缺失: {}",
                shard.index,
                shard.data_path.display()
            ),
        ));
    }
    let sz = std::fs::metadata(&shard.data_path)
        .map(|m| m.len())
        .unwrap_or(0);
    if sz == 0 && stats.rows_read > 0 {
        return Err(D2DError::external(
            "dmfldr",
            format!(
                "分片 {} 的日志报 {} 行，但数据文件为空 —— 导出静默丢数",
                shard.index, stats.rows_read
            ),
        ));
    }
    Ok(())
}

/// 导出单个分片（**等待结束**），返回其日志统计（`rows_read` 即该分片权威行数）
///
/// 非流式路径（逻辑面 `parse_parallel > 1`）仍走这里；物理直通面走
/// [`spawn_shard`] + 跟随读。
pub async fn run_shard(
    params: &super::params::FlDrParams,
    conn: &ConnectionConfig,
    shard: &Shard,
    dir: &Path,
) -> D2DResult<proc::ExportStats> {
    let stats = spawn_shard(params, conn, shard, dir)?.wait().await?;
    verify_shard_output(shard, &stats)?;
    Ok(stats)
}

/// 单分片统计里的拒绝/跳过行数不为 0 时告警
///
/// 它们会让「源端行数」对不上，是排查丢数的第一线索，不能静默。
pub fn warn_if_incomplete(stats: &proc::ExportStats, shard: usize) {
    if stats.rows_rejected > 0 || stats.rows_skipped > 0 {
        warn!(
            shard,
            rejected = stats.rows_rejected,
            skipped = stats.rows_skipped,
            "分片导出存在被拒绝/跳过的记录，行数对账需要一并考虑"
        );
    }
}

/// 生成 `IN (值列表)` 分片 SQL 的辅助（供测试）
pub fn shard_sql(table_sql: &str, columns: &[String], column: &str, values: &[String]) -> String {
    let lits: Vec<String> = values.iter().map(|v| sql_literal(v)).collect();
    format!(
        "SELECT {} FROM {} WHERE {} IN ({});\n",
        columns.join(", "),
        table_sql,
        column,
        lits.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(v: &str, r: u64) -> PartitionStat {
        PartitionStat {
            value: v.into(),
            rows: r,
        }
    }

    #[test]
    fn test_sql_literal_rendering() {
        assert_eq!(sql_literal("12345"), "12345");
        assert_eq!(sql_literal("-7"), "-7");
        assert_eq!(
            sql_literal("2022-01-01 00:00:00.000000"),
            "TIMESTAMP'2022-01-01 00:00:00.000000'"
        );
        assert_eq!(sql_literal("AB'C"), "'AB''C'");
        assert_eq!(sql_literal("  X1  "), "'X1'");
    }

    #[test]
    fn test_parse_probe() {
        let c = "2022-01-01 00:00:00.000000|304812\n2022-02-01 00:00:00.000000|28\n";
        let v = parse_probe(c, '|');
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].rows, 304812);
        assert_eq!(v[1].value, "2022-02-01 00:00:00.000000");
    }

    /// LPT 装箱的目标是**最小化最大组**（完工时间），不是让各组大小相等
    ///
    /// 注意这里早先写过一条 `max - min <= 100_000` 的断言，那是**数学上不可能成立**的：
    /// 存在一个 300_000 的不可分割分区，任何装箱的最大组都 ≥ 300_000，
    /// 而其余两组的和只有 200_003，故 `max - min >= 199_997` 恒成立。
    /// 该断言与算法无关，只会恒红。现在改为断言**真正的不变量**：
    /// 完工时间达到理论下界，且行数不丢。
    #[test]
    fn test_pack_partitions_minimizes_makespan() {
        let parts = vec![
            p("a", 300_000),
            p("b", 100_000),
            p("c", 100_000),
            p("d", 1),
            p("e", 1),
            p("f", 1),
        ];
        let total: u64 = parts.iter().map(|x| x.rows).sum();
        let g = pack_partitions(parts, 3);
        assert_eq!(g.len(), 3);
        let sums: Vec<u64> = g.iter().map(|v| v.iter().map(|x| x.rows).sum()).collect();

        // 行数不丢（装箱只是重新分组）
        assert_eq!(sums.iter().sum::<u64>(), total, "装箱不得丢行: {:?}", sums);
        // 理论下界 = max(最大单项, 总量/组数)。轮询装箱会把 1 行的小分区
        // 堆到 300000 上得到 300001，LPT 不会 —— 这条断言正好把两者区分开。
        let lower_bound = 300_000u64.max(total.div_ceil(3));
        assert_eq!(
            *sums.iter().max().unwrap(),
            lower_bound,
            "完工时间未达下界: {:?}",
            sums
        );
    }

    /// 没有超大单项时，LPT 应把各组压到接近总量/组数
    #[test]
    fn test_pack_partitions_balances_when_divisible() {
        let parts: Vec<PartitionStat> = (1..=20).map(|i| p(&format!("v{}", i), i * 1000)).collect();
        let total: u64 = parts.iter().map(|x| x.rows).sum();
        let g = pack_partitions(parts, 4);
        assert_eq!(g.len(), 4);
        let sums: Vec<u64> = g.iter().map(|v| v.iter().map(|x| x.rows).sum()).collect();
        assert_eq!(sums.iter().sum::<u64>(), total);
        let max = *sums.iter().max().unwrap();
        // 允许一个分区的粒度误差
        assert!(max <= total / 4 + 20_000, "分组偏离均衡上限: {:?}", sums);
    }

    #[test]
    fn test_pack_more_groups_than_partitions() {
        let g = pack_partitions(vec![p("a", 1), p("b", 2)], 8);
        assert_eq!(g.len(), 2, "组数不应超过分区数");
    }

    #[test]
    fn test_shard_sql_shape() {
        let cols = vec!["USER_ID".to_string(), "CREATED_AT".to_string()];
        let s = shard_sql(
            "\"S\".\"T\"",
            &cols,
            "CREATED_AT",
            &["2022-01-01 00:00:00.000000".to_string()],
        );
        assert!(
            s.contains("WHERE CREATED_AT IN (TIMESTAMP'2022-01-01 00:00:00.000000')"),
            "{}",
            s
        );
    }

    // ─────────────── 分区元数据路径 ───────────────

    #[test]
    fn test_parse_high_values_real_shape() {
        // 实测的达梦输出形态
        let c = "DATETIME'2022-01-01 00:00:00'\nDATETIME'2022-01-02 00:00:00'\nDATETIME'2027-01-01 00:00:00'\n";
        assert_eq!(
            parse_high_values(c),
            vec![
                "2022-01-01 00:00:00".to_string(),
                "2022-01-02 00:00:00".to_string(),
                "2027-01-01 00:00:00".to_string()
            ]
        );
    }

    #[test]
    fn test_parse_high_values_string_and_numeric_keys() {
        // 字符串分区键：'ABC'
        assert_eq!(parse_high_values("'ABC'\n'DEF'\n"), vec!["ABC", "DEF"]);
        // 数值分区键：裸数字
        assert_eq!(parse_high_values("100\n200\n"), vec!["100", "200"]);
        // 空行/空白行忽略
        assert_eq!(parse_high_values("\n  100  \n\n"), vec!["100"]);
    }

    /// MAXVALUE 及其之后都不能当切点（它没有有限上界）
    #[test]
    fn test_parse_high_values_stops_at_maxvalue() {
        let c = "DATETIME'2022-01-01 00:00:00'\nMAXVALUE\nDATETIME'2099-01-01 00:00:00'\n";
        assert_eq!(
            parse_high_values(c),
            vec!["2022-01-01 00:00:00".to_string()]
        );
    }

    /// 边界文本必须**解析成时间语义**，否则内核会按字符串切
    #[test]
    fn test_parse_cut_points_is_temporal() {
        let cuts = parse_cut_points(&[
            "2022-01-01 00:00:00".to_string(),
            "2022-02-01 00:00:00".to_string(),
        ]);
        assert_eq!(cuts.len(), 2);
        assert!(cuts[0].as_millis().is_some(), "{:?}", cuts[0]);
        assert!(cuts[1].as_millis() > cuts[0].as_millis());
        // 数值分区键仍按整数
        let n = parse_cut_points(&["100".to_string(), "200".to_string()]);
        assert!(matches!(n[0], Literal::Int(100)), "{:?}", n[0]);
    }

    // ─────────────── 切点取样 ───────────────

    fn day_cuts(n: usize) -> Vec<Literal> {
        (1..=n)
            .map(|i| Literal::parse(&format!("2026-01-{:02} 00:00:00", i)))
            .collect()
    }

    /// 28 个分区边界切 16 段，而不是切 28 段
    #[test]
    fn test_pick_cuts_limits_shard_count() {
        let all = day_cuts(28);
        assert_eq!(pick_cuts(&all, 4).len(), 3, "4 段需要 3 个内部切点");
        assert_eq!(pick_cuts(&all, 16).len(), 15);
        // 想要的段数超过可用切点数 → 收敛到 min(groups, n-1) - 1 个切点
        assert_eq!(pick_cuts(&all, 999).len(), 27 - 1);
        assert_eq!(pick_cuts(&all, 28).len(), 26);
        assert_eq!(pick_cuts(&all, 27).len(), 26);
    }

    /// 取样出的切点必须严格递增、且都来自原始边界（否则服务端裁不掉分区）
    #[test]
    fn test_pick_cuts_are_real_and_ordered() {
        let all = day_cuts(28);
        for groups in [1usize, 2, 3, 7, 16, 27, 28, 50] {
            let picked = pick_cuts(&all, groups);
            for c in &picked {
                assert!(all.contains(c), "切点 {} 不是分区边界", c);
            }
            for w in picked.windows(2) {
                assert!(
                    w[0].as_millis() < w[1].as_millis(),
                    "切点必须严格递增 (groups={}): {:?}",
                    groups,
                    picked
                );
            }
            assert!(picked.len() <= groups.saturating_sub(1));
        }
    }

    #[test]
    fn test_pick_cuts_edges() {
        assert!(pick_cuts(&[], 4).is_empty());
        assert!(pick_cuts(&day_cuts(1), 4).is_empty(), "单个边界无内部切点");
        // ⚠️ 两个边界也取不出内部切点（`min(groups, n-1) = 1` → 循环为空）。
        //    这是沿用已久的语义：调用方据此判定"边界不足以切分"，
        //    退回到值探测路径，而不是硬切出两片。改动它等于改这条退化判据。
        assert!(pick_cuts(&day_cuts(2), 4).is_empty());
        assert_eq!(pick_cuts(&day_cuts(3), 4).len(), 1, "三个边界可切两段");
    }

    // ─────────────── 分片 SQL 组装 ───────────────

    #[test]
    fn test_shard_sql_predicated_combines_user_where() {
        let cols = vec!["A".to_string()];
        // 只有分区谓词
        let s = shard_sql_predicated("\"T\"", &cols, None, "\"P\" >= 1");
        assert_eq!(s, "SELECT A FROM \"T\" WHERE (\"P\" >= 1);\n");
        // 只有用户条件
        let s = shard_sql_predicated("\"T\"", &cols, Some("S = 1"), "");
        assert_eq!(s, "SELECT A FROM \"T\" WHERE (S = 1);\n");
        // 两者 AND 组合 —— 这是 partitioned 模式放行 where 之后的实际形态
        let s = shard_sql_predicated("\"T\"", &cols, Some("S = 1"), "\"P\" >= 1 AND \"P\" < 2");
        assert_eq!(
            s,
            "SELECT A FROM \"T\" WHERE (S = 1)\n  AND (\"P\" >= 1 AND \"P\" < 2);\n"
        );
        // 都不给 = 全表（合法 SQL，不是残句）
        let s = shard_sql_predicated("\"T\"", &cols, None, "");
        assert_eq!(s, "SELECT A FROM \"T\";\n");
    }

    // ─────────────── 端到端：零探测规划 ───────────────

    fn cols() -> Vec<String> {
        vec!["USER_ID".to_string(), "CREATED_AT".to_string()]
    }

    /// `list` 模式：零探测，每条条件一个分片，且 where 被 AND 进去
    #[tokio::test]
    async fn test_plan_shards_list_is_zero_probe() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let spec = PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec![
                "CREATED_AT >= TIMESTAMP'2026-01-01 00:00:00'".into(),
                "CREATED_AT < TIMESTAMP'2026-01-01 00:00:00'".into(),
            ],
            ..Default::default()
        };
        let (shards, source, total) = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: Some("S"),
                table: "T",
                columns: &cols(),
                partition: Some(&spec),
                user_where: Some("STATUS = 1"),
                dir: dir.path(),
            },
        )
        .await
        .unwrap();

        assert_eq!(shards.len(), 2);
        assert_eq!(total, None);
        assert!(matches!(source, ShardPlanSource::User { .. }));
        let sql0 = std::fs::read_to_string(&shards[0].sql_path).unwrap();
        assert!(sql0.contains("(STATUS = 1)"), "{}", sql0);
        assert!(
            sql0.contains("(CREATED_AT >= TIMESTAMP'2026-01-01 00:00:00')"),
            "{}",
            sql0
        );
        assert!(sql0.contains("FROM \"S\".\"T\""), "{}", sql0);
    }

    /// `range` + 显式上下界：零探测，按时间均分
    #[tokio::test]
    async fn test_plan_shards_range_with_bounds_is_zero_probe() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let spec = PartitionSpec {
            mode: PartitionMode::Range,
            column: Some("CREATED_AT".into()),
            lower: Some(Literal::parse("2026-01-01 00:00:00")),
            upper: Some(Literal::parse("2026-01-05 00:00:00")),
            count: 4,
            ..Default::default()
        };
        let (shards, source, _) = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: Some(&spec),
                user_where: None,
                dir: dir.path(),
            },
        )
        .await
        .unwrap();

        assert_eq!(shards.len(), 4);
        assert!(matches!(source, ShardPlanSource::User { .. }));
        let sql1 = std::fs::read_to_string(&shards[1].sql_path).unwrap();
        // 第 2 段 [01-02, 01-03) —— 段内半开（列名走 quote_ident）
        assert!(
            sql1.contains("\"CREATED_AT\" >= TIMESTAMP'2026-01-02 00:00:00'")
                && sql1.contains("\"CREATED_AT\" < TIMESTAMP'2026-01-03 00:00:00'"),
            "{}",
            sql1
        );
        // 末段闭区间，兜住上界
        let sql3 = std::fs::read_to_string(&shards[3].sql_path).unwrap();
        assert!(
            sql3.contains("\"CREATED_AT\" <= TIMESTAMP'2026-01-05 00:00:00'"),
            "{}",
            sql3
        );
        // 相邻两段的边界值必须严丝合缝：前段上界 == 后段下界
        let sql2 = std::fs::read_to_string(&shards[2].sql_path).unwrap();
        assert!(
            sql1.contains("\"CREATED_AT\" < TIMESTAMP'2026-01-03 00:00:00'")
                && sql2.contains("\"CREATED_AT\" >= TIMESTAMP'2026-01-03 00:00:00'"),
            "区间不连续:\n{}\n{}",
            sql1,
            sql2
        );
    }

    /// `hash` 模式：零探测
    #[tokio::test]
    async fn test_plan_shards_hash_is_zero_probe() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let spec = PartitionSpec {
            mode: PartitionMode::Hash,
            column: Some("USER_ID".into()),
            count: 3,
            ..Default::default()
        };
        let (shards, _, _) = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: Some(&spec),
                user_where: None,
                dir: dir.path(),
            },
        )
        .await
        .unwrap();
        assert_eq!(shards.len(), 3);
        let sql2 = std::fs::read_to_string(&shards[2].sql_path).unwrap();
        assert!(
            sql2.contains(
                "MOD(ORA_HASH(COALESCE(CAST(\"USER_ID\" AS VARCHAR), ''), 4294967295), 3) = 2"
            ),
            "{}",
            sql2
        );
    }

    /// `none` 模式：单分片读全表（含用户 where），"就迁一天数据"该用这个
    #[tokio::test]
    async fn test_plan_shards_none_is_single_shard_with_where() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let spec = PartitionSpec {
            mode: PartitionMode::None,
            ..Default::default()
        };
        let (shards, _, _) = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: Some(&spec),
                user_where: Some("CREATED_AT = TIMESTAMP'2026-03-01 00:00:00'"),
                dir: dir.path(),
            },
        )
        .await
        .unwrap();
        assert_eq!(shards.len(), 1);
        let sql = std::fs::read_to_string(&shards[0].sql_path).unwrap();
        assert!(
            sql.contains("WHERE (CREATED_AT = TIMESTAMP'2026-03-01 00:00:00')"),
            "{}",
            sql
        );
        assert!(!sql.contains(" AND "), "单分片不该出现分片条件: {}", sql);
    }

    /// 非法的 PartitionSpec 必须在规划期就失败
    #[tokio::test]
    async fn test_plan_shards_rejects_invalid_spec() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let spec = PartitionSpec {
            mode: PartitionMode::List,
            conditions: vec![],
            ..Default::default()
        };
        let e = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: Some(&spec),
                user_where: None,
                dir: dir.path(),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains("conditions"), "{}", e);
    }

    /// 既没有 `partition` 也没有 `partition_column` → 明确报错，而不是静默走全表
    #[tokio::test]
    async fn test_plan_shards_requires_a_partition_column() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams {
            ..Default::default()
        };
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        let e = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: None,
                user_where: None,
                dir: dir.path(),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains("source.partition.column"), "{}", e);
        // 错误信息要给出可直接照抄的写法，而不是只说"缺列"
        assert!(e.contains("mode: range"), "{}", e);
    }

    /// 旧配置（只给 partition_column，不给 partition）继续可用：落到 range-auto 路径
    #[tokio::test]
    async fn test_legacy_partition_column_is_still_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let params = super::super::params::FlDrParams::default();
        let conn =
            ConnectionConfig::from_json(&serde_json::json!({"host":"h","user":"u"})).unwrap();
        // 只验证参数装配阶段不报错（真正探测要连库；错误信息应来自连接，而非配置）
        let e = plan_shards(
            &params,
            &conn,
            ShardPlanInput {
                schema: None,
                table: "T",
                columns: &cols(),
                partition: None,
                user_where: None,
                dir: dir.path(),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            !e.contains("partition_column"),
            "不该再要求 partition_column: {}",
            e
        );
    }
}
