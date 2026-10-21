//! D2D 命令行入口
//!
//! **这个文件不认识任何具体数据库** —— 连接器由 [`connectors::register_all`] 登记，
//! 之后一路只经 `dyn` 接口使用（工厂造实例、trait 调能力）。
//! 想知道"支持哪些库"，看 `connectors.rs`，那里是唯一的接线处。

pub mod args;
pub mod config;
pub mod connectors;
pub mod validate;

use args::Cli;
use clap::Parser;
use config::JobConfig;
use d2d_connector::ConnectorRegistry;
use d2d_execution::direct::warn_if_no_forbidden_declared;
use d2d_execution::{plan_path, run_flow, ExecutionPath, FlowRun, Verdict};
use std::sync::Arc;
use tracing::{error, info, warn};
use validate::SchemaContractChecker;

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(&cli.log_level)
        .init();

    info!(config = ?cli.config, "D2D starting");

    // ── 加载配置 ──
    //
    // 在**建运行时之前**读，因为 `engine.worker_threads` 要交给运行时构造器 ——
    // 这曾经是个空转的旋钮：配置里写了、代码里没人读，`#[tokio::main]` 早就
    // 按 CPU 核数把线程池建好了。旋钮要么接线、要么删掉，不能挂着好看。
    let job_config = JobConfig::from_yaml(&cli.config)?;
    info!(job_name = %job_config.job.name, "config loaded");

    let want_threads = job_config
        .engine
        .as_ref()
        .and_then(|e| e.worker_threads)
        .unwrap_or(0);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if want_threads > 0 {
        builder.worker_threads(want_threads);
    }
    let rt = builder
        .build()
        .map_err(|e| anyhow::anyhow!("建 tokio 运行时失败: {}", e))?;
    rt.block_on(run(cli, job_config))
}

async fn run(cli: Cli, job_config: JobConfig) -> anyhow::Result<()> {
    // 查询侧摘要（Source 侧下推配置的人类可读预览）
    if let Some(query) = &job_config.source.query {
        info!(
            columns = ?query.columns.as_ref().map(|c| c.iter().map(|x| x.output_name().to_string()).collect::<Vec<_>>()),
            where_clause = ?query.where_clause,
            order_by = ?query.order_by,
            limit = ?query.limit,
            "source query pushdown config"
        );
    }
    // 分区配置摘要。**把「是否零探测」显式打出来**：这是排查
    // 「这次迁移为什么比上次慢 30 秒」的第一线索 —— 零探测与回退探测差一个数量级。
    // `?` 会在 partition 与已废弃的 split 同时配置时于启动期直接失败
    if let Some(p) = job_config.source.effective_partition()? {
        info!(
            mode = p.mode.as_str(),
            column = p.column.as_deref().unwrap_or("-"),
            count = p.count,
            conditions = p.conditions.len(),
            zero_probe = p.is_probe_free(),
            "source partition config"
        );
    }
    // 连接器专属参数（引擎不解释，原样交给连接器）—— 打出来是为了排查
    // "我明明配了 fetch_size 怎么不生效"这类问题时的第一手信息
    if let Some(props) = &job_config.source.properties {
        info!(keys = ?props.keys().collect::<Vec<_>>(), "source.properties");
    }
    if let Some(props) = &job_config.sink.properties {
        info!(keys = ?props.keys().collect::<Vec<_>>(), "sink.properties");
    }

    // ── 连接器登记：**唯一的接线处**（见 `connectors.rs` 的模块文档） ──
    //
    // 放在最前面：后面每一步（类型校验要类型适配器、账本要 unit_footprint、
    // 执行要造实例）都得先有表可查。查不到时的报错会**列出已注册的名单**，
    // 省得用户对着一个拼错的库名猜。
    let registry = ConnectorRegistry::new();
    connectors::register_all(&registry);

    // ── 启动期类型契约校验（fail-fast：禁止的转换直接拒绝，防数据错乱） ──
    // dry-run 同样执行完整校验（dry-run 的意义就是验证配置可用性）
    if let Some(type_contract) = &job_config.source.type_contract {
        let cols: Vec<(String, String, String)> = type_contract
            .iter()
            .map(|c| (c.column.clone(), c.source_type.clone(), c.sink_type.clone()))
            .collect();
        let checks = SchemaContractChecker::check(
            &registry,
            &job_config.source.connector,
            &job_config.sink.connector,
            &cols,
        )?;
        for c in &checks {
            if !c.policy.is_lossless() {
                warn!(
                    column = %c.column,
                    from = %c.from.describe(),
                    to = %c.to.describe(),
                    policy = %c.policy.describe(),
                    "lossy type conversion"
                );
            }
        }
        info!(checked = checks.len(), "schema contract check completed");
    } else {
        warn!("未配置 source.type_contract，跳过启动期类型校验（建议配置以阻断类型错乱）");
    }

    // ⚠️ 这里曾经有一段 `if job_config.source.connector == "dameng" { ... }`：
    // 从 CLI 里点名 dameng、解析它的 method、跑一遍 dmfldr 的启动期校验、再打日志。
    // 已删除，原因有三：
    //   ① 校验**在 `init()` 里又跑了一遍**（那是权威位置，因为只有连接器自己知道
    //      自己的读取方式有哪些约束），CLI 这份是重复的；
    //   ② 那段日志该由连接器自己打（旋钮叫什么，只有它知道）；
    //   ③ 它让"换个源"从"写个连接器"变成"改 CLI" —— 正是本次收敛要拿掉的东西。
    // 顺序上也无妨：校验失败发生在 `init()`，同样在**建连之前**（dry-run 里也会跑到）。

    // ── YAML → Job：两条入口在这里汇合 ──
    //
    // 放在 dry-run **之前**：dry-run 的意义就是验证配置可用性，
    // 而"算子名/参数名/类型名对不对"正是配置可用性的一部分。
    // 已搬走的 `engine.*` 字段（source_slots / channel_capacity / policy ...）
    // 也在这步被拒绝，并给出新位置 —— 静默忽略会让用户以为参数生效了。
    let job = job_config.into_job()?;
    info!(
        job = %job.name,
        operators = ?job.transforms.operator_names(),
        "job 已构造"
    );

    // ── ① 三档并发：**用户配了什么就是什么**，这里只把"0 = 跟随"解析成数字 ──
    let resolved = job.flow.resolve();
    resolved.log(&job.flow);

    // 源端取数单元的形态（进程/连接/进程内）+ 每单元常驻字节 —— 只读声明、不连库。
    //
    // 经**工厂**问，而不是 `DamengSourceConnector::new()`：这一句曾经是
    // "换个源就得改 CLI" 的两处硬伤之一（另一处是收尾时的 exported_rows/cleanup）。
    // 工厂里存的就是"怎么造一个"，所以引擎能在不认识具体类型的前提下问出这个数。
    let source_factory = registry.source_factory(&job_config.source.connector)?;
    let unit = source_factory.unit_footprint(&job_config.source);

    // ── ② 内存账本校验：**装不下就拒绝启动，并指明超在哪一项** ──
    //
    // 重点在"拒绝"而不是"改写"：绝不因为内存不够就把用户的并发砍掉。
    // 用户能感知的是"源库能接几个连接"，感知不到"内存 ÷ 每单元常驻"。
    let memory_mb = job_config
        .engine
        .as_ref()
        .map(|e| e.memory_mb())
        .unwrap_or(2048);
    // ── ②' spill 目录：可写在**启动期**就要说清楚 ──
    //
    // 放在账本校验旁边，同样在 dry-run 之前 —— 原来这件事推迟到"池第一次满"，
    // 那时目录不可写的表现是"池退化成背压、任务变慢"，看不出是配置写错了。
    job.flow.validate_spill_dirs()?;

    let account = job
        .flow
        .validate_memory(&resolved, unit, job.has_transforms(), memory_mb)?;
    info!(
        budget_mb = memory_mb,
        accounted_mb = account.mb(),
        "内存账（六项，互不重叠）{}",
        account.render()
    );

    if cli.dry_run {
        info!("dry run mode: config validated successfully");
        return Ok(());
    }

    // ── 连接器生命周期：工厂造实例 → init（&mut 独占期）→ 共享句柄 ──
    //
    // 三步都是**与具体类型无关**的：引擎只经工厂建、经 trait 调。
    // 收尾要用的 exported_rows / cleanup 也已在 trait 上（`&self` 语义），
    // 所以可以直接在 `Arc<dyn SourceConnector>` 上调用。
    //
    // ⚠️ **这里不造闸门，也不把并发数交给连接器。** `flow.*.concurrency` 只进
    // `run_flow` —— 它在**内核的派发器**手里变成三道闸门（一个任务一张券）。
    // 连接器全程看不到那个数字：它只要守"一次 `read` = 一个取数单元、
    // 一条会话同时至多一个在飞写入"的契约，剩下的由内核按任务卡死。
    // （走过两版弯路：先传一个 `slots: usize` 让连接器"自己守规矩"——odbc 压根不读；
    //   再传一道必须 `acquire()` 的闸门 —— 那是内核伸手管连接器内部。都删了。）
    let mut source = source_factory.build();
    source.init(&job_config.source).await?;
    let source: Arc<dyn d2d_connector::SourceConnector> = Arc::from(source);

    let sink_factory = registry.sink_factory(&job_config.sink.connector)?;
    let mut sink = sink_factory.build();
    sink.init(&job_config.sink).await?;
    let sink: Arc<dyn d2d_connector::SinkConnector> = Arc::from(sink);

    info!(
        source = %source.name(),
        sink = %sink.name(),
        "connectors resolved"
    );

    // ── 执行路径协商（决策只有这一处，理由必须可读） ──
    let exec_path = plan_path(source.as_ref(), sink.as_ref(), &job.transforms);
    info!(path = %exec_path.describe(), "执行路径已确定");
    if !job.direct.forbidden_scan {
        warn!("已关闭直通受限字节扫描（verify_forbidden_scan=false）—— 载荷被目标端误解将不被发现");
    }
    if let ExecutionPath::Direct { forbidden, .. } = &exec_path {
        warn_if_no_forbidden_declared(forbidden);
    }

    // ── 跑 ──
    //
    // 一个入口、一张拓扑：逻辑面与直通面都走 `run_flow`。
    // 曾经这里是"每个分片 spawn 一个 task"，外加直通另起一路 —— 于是
    // 三段（取数/处理/写入）只有一档并发，直通与逻辑面的资源账还各算各的。
    let started = std::time::Instant::now();
    let result = run_flow(FlowRun {
        spec: &job.flow,
        source: Arc::clone(&source),
        sink: Arc::clone(&sink),
        source_spec: job_config.source.clone(),
        sink_spec: job_config.sink.clone(),
        transforms: job.transforms.clone(),
        path: exec_path,
        forbidden_scan: job.direct.forbidden_scan,
        preflight_scan: job.direct.preflight_scan,
        fail_on_row_mismatch: job.direct.fail_on_row_mismatch,
    })
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    if let Ok(r) = &result {
        info!(
            source_pool = %r.source_pool,
            sink_pool = %r.sink_pool,
            "池读数（全程峰值）—— 峰值 ≪ 容量说明池给大了；溢出次数非零说明 upstream 比 downstream 快"
        );
        // 闸门峰值：**判断"并发数是不是给大了"的唯一读数**。
        // 与池不同的是它的语义：池量的是"排队"，闸门量的是"**同时有几个任务在跑**"。
        // 左值远小于右值有两种成因，读数本身分不开：并发配大了，或者**数据就这么点**
        // （任务数由数据决定 —— 1 个分片配 16 路，峰值恒为 1）。
        info!(
            source_units = format!("{}/{}", r.source_gate_peak, resolved.source),
            sink_writes = format!("{}/{}", r.sink_gate_peak, resolved.sink),
            "并发闸门峰值（simultaneously running tasks）—— \
             左值远小于右值：并发配大了，或者数据量本来就凑不出那么多任务"
        );

        // ── 账本（预测）vs 连接器自报 vs 实测，三者并排 ──
        //
        // 这是"账本准不准"的**唯一**读数，也是"钱花在哪"的唯一读数。
        // 账本六项里，源端取数单元 / 两段工作集 / 转换中间态都是按公式估的；
        // 而连接器内部还有自己的缓冲（ODBC 行集、Doris 请求体），那部分
        // **在池的账上根本不存在** —— 曾经它只能以"配了 512 MB 却常驻 3 GB"的形式被发现。
        //
        // 三个数的口径与用途都不一样，**不可相加**：
        //   账本    预测（启动期算的，所以 --dry-run 也有）
        //   自报    连接器认领的那部分（运行期事实，可能与账本某一项重叠）
        //   实测    内核水位 VmHWM（事实，且**不含子进程**）
        // ⚠️ 必须**同口径**比：进程自报的 RSS 不含子进程，而账本含。
        // 拿账本总额去比会把差值算小、看着像账本很准 —— 那是假的。
        let inproc_mb = account.in_process_mb();
        let reported_mb = r.connector_reported_bytes() / 1048576;
        match r.peak_rss_bytes {
            Some(rss) => {
                let rss_mb = rss / 1048576;
                let gap = rss_mb as i64 - inproc_mb as i64;
                let pct = if inproc_mb > 0 {
                    gap * 100 / inproc_mb as i64
                } else {
                    0
                };
                let fields = format!(
                    "账本（预测，本进程口径） {} MB\n连接器自报（运行期峰值）  {} MB\n实测（VmHWM，事实）      {} MB",
                    inproc_mb, reported_mb, rss_mb
                );
                // 差额拆成两半：连接器认领的，和谁也不认领的。
                // 后者才是下一步要看的东西（Arrow 结构开销 / 分配器碎片 / 第三方库）。
                let unclaimed_mb = (rss_mb as i64 - reported_mb as i64).max(0);
                if pct >= 20 {
                    warn!(
                        accounted_inproc_mb = inproc_mb,
                        accounted_children_mb = account.child_process_mb(),
                        connector_reported_mb = reported_mb,
                        measured_self_mb = rss_mb,
                        unclaimed_mb = unclaimed_mb,
                        gap_pct = pct,
                        "内存账 vs 实测（本进程口径）：账本偏低 {}%。\n{}\n\
                         差额 = 实测 − 自报 = {} MB 未由连接器认领（含账本已列的池与工作集、\
                         Arrow 结构开销、分配器碎片）",
                        pct,
                        fields,
                        unclaimed_mb
                    );
                } else {
                    info!(
                        accounted_inproc_mb = inproc_mb,
                        accounted_children_mb = account.child_process_mb(),
                        connector_reported_mb = reported_mb,
                        measured_self_mb = rss_mb,
                        unclaimed_mb = unclaimed_mb,
                        gap_pct = pct,
                        "内存对账（本进程口径）\n{}",
                        fields
                    );
                }
            }
            None => info!(
                accounted_inproc_mb = inproc_mb,
                accounted_children_mb = account.child_process_mb(),
                "内存账（本平台取不到实测 RSS，只有预测值）"
            ),
        }

        // 连接器自报的明细：**每一行都由连接器自己命名**（引擎不解释字节是什么）
        if !r.connector_memory.is_empty() {
            let detail = r
                .connector_memory
                .iter()
                .map(|m| {
                    format!(
                        "\n    {:>6} MB  {:<20} {}",
                        m.peak_bytes / 1048576,
                        m.label,
                        m.note
                    )
                })
                .collect::<String>();
            info!("连接器自报明细（运行期峰值）{}", detail);
        }
    }

    // 释放连接器资源（dmfldr：清理导出目录）。
    // 失败时**保留**导出目录：全表导出动辄几 GB、耗时几分钟，删掉就没了排查依据。
    // 需要无条件保留可设环境变量 D2D_KEEP_EXPORT=1。
    let keep_export = result.is_err() || std::env::var("D2D_KEEP_EXPORT").is_ok();
    if keep_export {
        warn!("导出目录已保留（work_dir 下），排查完可手动删除");
    } else if let Err(e) = source.cleanup().await {
        warn!(error = %e, "source cleanup failed, export files may remain in work_dir");
    }

    // 失败时把错误原样抛出 —— 不要用 `?` 提前返回，那样会跳过上面的导出目录保留逻辑
    let report = match result {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, elapsed_ms = elapsed_ms, "迁移失败");
            return Err(anyhow::anyhow!(e.to_string()));
        }
    };

    info!(
        splits = report.splits,
        rows_written = report.rows_written,
        elapsed_ms = report.elapsed_ms,
        "D2D completed"
    );

    // ── 行数对账：**结论在引擎里出**，这里只负责显示与退出码 ──
    //
    // 判据只有一处（`d2d_execution::audit`）—— 曾经这里和 `flow.rs` 各写了一遍，
    // 口径与触发条件不同，而这一份根本没法测（它是 `main()` 里的一段）。
    // 对不上**且**校验开着时，引擎已经返回 Err 了，走不到这里；
    // 所以下面的 Mismatch 分支只可能是"关掉了校验"那一档 —— 仍然要说出来：
    // 关掉校验不等于关掉知情权。
    let rows = &report.rows;
    match rows.verdict() {
        Verdict::Pass => info!(
            source_rows = rows.exported,
            read_rows = rows.read,
            written_rows = rows.written,
            "行数对账通过：源端导出 = 搬运 = 目标端确认（三个数各自独立）"
        ),
        Verdict::Partial { reason } => warn!(
            read_rows = rows.read,
            written_rows = rows.written,
            reason,
            "行数对账只核了一半：源端给不出独立行数，\
             '读出来的 = 目标端确认的' 这条仍然成立"
        ),
        Verdict::Mismatch { .. } => {
            // 到得了这里 ⟹ `flow.direct.fail_on_row_mismatch: false`
            let e = rows.mismatch_error(&rows.verdict());
            warn!("{}  （verify_row_count=false，仅告警不中止）", e);
        }
    }

    Ok(())
}
