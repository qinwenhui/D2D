//! **拓扑级的行为契约**
//!
//! 这些用例保护的是「三段拓扑本身」的承诺：数据一条不丢、直通字节一字不改、
//! 任何一段失败整条链路一起停、池配到最紧也不死锁。
//!
//! 还有这一轮新立起来的两条，它们各自需要一条**观测型**测试（功能测试全绿也说明不了）：
//!
//! - **并发只当闸门**：配 N 路就最多 N 个任务在跑（`read()` 数得出来）
//! - **任务数由数据决定**：数据只有几份就只派几个任务，哪怕并发配了 16

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use d2d_connector::{
    Charset, CsvDialect, MemMeter, PhysicalBlock, PhysicalCompat, PhysicalFormat, PhysicalSession,
    PhysicalSink, PhysicalSource, PhysicalStream, SinkConnector, SinkSession, SinkSpec,
    SourceConnector, SourceSpec, SourceSplit,
};
use d2d_core::{D2DError, D2DResult};
use d2d_execution::{
    run_flow, ExecutionPath, FlowReport, FlowRun, FlowSpec, LogicalReason, Verdict,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn schema() -> arrow::datatypes::SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]))
}

// ─────────────────────────── 测试替身 ───────────────────────────

/// 取数时每片挂账的字节数（测试用固定值，好断言）
const SRC_CLAIM_BYTES: usize = 1 << 20;

/// 每片产 `rows_per_split` 行；逻辑面按 `batch_rows` 切批，物理面按 `batch_rows` 切块
struct FakeSource {
    splits: usize,
    rows_per_split: usize,
    batch_rows: usize,
    physical: bool,
    /// 替身也走一遍**运行期申报**：真连接器（ODBC 行集 / Doris 请求体）
    /// 报的就是这类"不在任何池的账上"的缓冲
    meter: Arc<MemMeter>,
}

impl FakeSource {
    fn logical(splits: usize, rows_per_split: usize, batch_rows: usize) -> Arc<Self> {
        Arc::new(Self {
            splits,
            rows_per_split,
            batch_rows,
            physical: false,
            meter: MemMeter::new("fake 源端缓冲", "每片一块取数缓冲"),
        })
    }
    fn physical(splits: usize, rows_per_split: usize, block_rows: usize) -> Arc<Self> {
        Arc::new(Self {
            splits,
            rows_per_split,
            batch_rows: block_rows,
            physical: true,
            meter: MemMeter::new("fake 源端缓冲", "每片一块取数缓冲"),
        })
    }
}

#[async_trait]
impl SourceConnector for FakeSource {
    fn name(&self) -> &str {
        "fake"
    }
    async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
        Ok(())
    }
    async fn schema(&self) -> D2DResult<arrow::datatypes::SchemaRef> {
        Ok(schema())
    }
    async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        Ok((0..self.splits)
            .map(|i| SourceSplit {
                split_id: format!("s{}", i),
                split_info: serde_json::json!({}),
            })
            .collect())
    }
    async fn read(&self, split: &SourceSplit) -> D2DResult<d2d_core::RecordBatchStream> {
        let _lease = self.meter.claim(SRC_CLAIM_BYTES);
        let idx: usize = split.split_id[1..].parse().unwrap();
        let base = idx * self.rows_per_split;
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < self.rows_per_split {
            let n = self.batch_rows.min(self.rows_per_split - off);
            let ids: Vec<i64> = (0..n).map(|k| (base + off + k) as i64).collect();
            let vs: Vec<String> = ids.iter().map(|i| format!("v{}", i)).collect();
            out.push(Ok(arrow::array::RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(vs)),
                ],
            )
            .unwrap()));
            off += n;
        }
        Ok(Box::pin(futures::stream::iter(out)))
    }
    fn as_physical(&self) -> Option<&dyn PhysicalSource> {
        if self.physical {
            Some(self)
        } else {
            None
        }
    }
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        Some(Arc::clone(&self.meter))
    }
    async fn close(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

#[async_trait]
impl PhysicalSource for FakeSource {
    fn physical_format(&self) -> Option<PhysicalFormat> {
        Some(PhysicalFormat::csv(
            CsvDialect::default(),
            Charset::Utf8,
            vec!["id".into(), "v".into()],
        ))
    }
    async fn read_physical(&self, split: &SourceSplit) -> D2DResult<PhysicalStream> {
        let idx: usize = split.split_id[1..].parse().unwrap();
        let base = idx * self.rows_per_split;
        let mut out = Vec::new();
        let mut off = 0usize;
        while off < self.rows_per_split {
            let n = self.batch_rows.min(self.rows_per_split - off);
            // **块边界必须与行边界对齐**（物理面的不变量）
            let mut bytes = Vec::new();
            for k in 0..n {
                bytes.extend_from_slice(
                    format!("{}|v{}\n", base + off + k, base + off + k).as_bytes(),
                );
            }
            out.push(Ok(PhysicalBlock {
                bytes,
                rows: n as u64,
            }));
            off += n;
        }
        Ok(Box::pin(futures::stream::iter(out)))
    }
}

/// 每次写入挂账的字节数（测试用固定值，好断言）
const SINK_CLAIM_BYTES: usize = 2 << 20;

#[derive(Default)]
struct Rec {
    rows: u64,
    batches: usize,
    sessions: usize,
    body: Vec<u8>,
    aborted: bool,
    /// 模拟目标端"静默去重"：确认行数比实际少这么多
    drop_rows: u64,
}

#[derive(Clone)]
struct FakeSink {
    rec: Arc<Mutex<Rec>>,
    /// 替身也走一遍**运行期申报**（见 [`FakeSource::meter`]）
    meter: Arc<MemMeter>,
}

impl Default for FakeSink {
    fn default() -> Self {
        Self {
            rec: Arc::new(Mutex::new(Rec::default())),
            meter: MemMeter::new("fake 汇端缓冲", "每次写入一份攒批缓冲"),
        }
    }
}

#[async_trait]
impl SinkConnector for FakeSink {
    fn name(&self) -> &str {
        "fake"
    }
    async fn init(&mut self, _: &SinkSpec) -> D2DResult<()> {
        Ok(())
    }
    async fn begin_session(&self, _: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        self.rec.lock().unwrap().sessions += 1;
        Ok(Box::new(FakeSession {
            rec: Arc::clone(&self.rec),
            meter: Arc::clone(&self.meter),
        }))
    }
    fn as_physical(&self) -> Option<&dyn PhysicalSink> {
        Some(self)
    }
    fn memory_meter(&self) -> Option<Arc<MemMeter>> {
        Some(Arc::clone(&self.meter))
    }
    async fn close(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

#[async_trait]
impl PhysicalSink for FakeSink {
    fn accepts_physical(&self, _: &PhysicalFormat) -> PhysicalCompat {
        PhysicalCompat::Accept {
            forbidden: vec![b'\\'],
        }
    }
    async fn begin_physical(&self, _: &PhysicalFormat) -> D2DResult<Box<dyn PhysicalSession>> {
        let mut r = self.rec.lock().unwrap();
        r.sessions += 1;
        // 每会话各丢这么多：模拟"目标表唯一键选错导致静默去重"。
        // 用每会话一份（而不是全局一份）是为了在 sink 并发 > 1 时行为可预期。
        let drop = r.drop_rows;
        Ok(Box::new(FakePhysicalSession {
            rec: Arc::clone(&self.rec),
            rows: 0,
            drop,
        }))
    }
}

struct FakeSession {
    rec: Arc<Mutex<Rec>>,
    meter: Arc<MemMeter>,
}

#[async_trait]
impl SinkSession for FakeSession {
    async fn write_batch(&mut self, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
        let _lease = self.meter.claim(SINK_CLAIM_BYTES);
        let mut r = self.rec.lock().unwrap();
        r.rows += batch.num_rows() as u64;
        r.batches += 1;
        Ok(batch.num_rows())
    }
    async fn commit(&mut self) -> D2DResult<()> {
        Ok(())
    }
    async fn abort(&mut self) -> D2DResult<()> {
        self.rec.lock().unwrap().aborted = true;
        Ok(())
    }
}

struct FakePhysicalSession {
    rec: Arc<Mutex<Rec>>,
    /// **本会话**写进去的行数（真实目标端也只回报自己这条会话的
    /// `NumberLoadedRows`，不是全局计数）
    rows: u64,
    drop: u64,
}

#[async_trait]
impl PhysicalSession for FakePhysicalSession {
    async fn write_block(&mut self, block: PhysicalBlock) -> D2DResult<u64> {
        let rows = block.rows;
        let mut r = self.rec.lock().unwrap();
        r.body.extend_from_slice(&block.bytes);
        r.rows += rows;
        r.batches += 1;
        drop(r);
        self.rows += rows;
        Ok(rows)
    }
    async fn commit(&mut self) -> D2DResult<u64> {
        Ok(self.rows.saturating_sub(self.drop))
    }
    async fn abort(&mut self) -> D2DResult<()> {
        self.rec.lock().unwrap().aborted = true;
        Ok(())
    }
}

// ─────────────────────────── 脚手架 ───────────────────────────

fn spec(yaml: &str) -> FlowSpec {
    serde_yaml::from_str(yaml).unwrap()
}

fn source_spec() -> SourceSpec {
    serde_json::from_value(serde_json::json!({
        "connector": "fake",
        "connection": {},
        "table": "t"
    }))
    .unwrap()
}

fn sink_spec() -> SinkSpec {
    serde_json::from_value(serde_json::json!({
        "connector": "fake",
        "connection": {},
        "database": "d",
        "table": "t"
    }))
    .unwrap()
}

fn logical_path() -> ExecutionPath {
    ExecutionPath::Logical {
        reason: LogicalReason::SourceNoPhysical,
    }
}

fn direct_path() -> ExecutionPath {
    ExecutionPath::Direct {
        format: PhysicalFormat::csv(
            CsvDialect::default(),
            Charset::Utf8,
            vec!["id".into(), "v".into()],
        ),
        forbidden: vec![b'\\'],
    }
}

async fn go(
    f: &FlowSpec,
    src: Arc<dyn SourceConnector>,
    snk: &FakeSink,
    path: ExecutionPath,
) -> D2DResult<FlowReport> {
    go_with_sink(f, src, Arc::new(snk.clone()), path, true).await
}

/// 同上，但换一个替身目标端 / 关掉行数校验（有些用例要专有的行为）
async fn go_with_sink(
    f: &FlowSpec,
    src: Arc<dyn SourceConnector>,
    sink: Arc<dyn SinkConnector>,
    path: ExecutionPath,
    fail_on_row_mismatch: bool,
) -> D2DResult<FlowReport> {
    run_flow(FlowRun {
        spec: f,
        source: src,
        sink,
        source_spec: source_spec(),
        sink_spec: sink_spec(),
        transforms: d2d_transform::TransformChain::default(),
        path,
        forbidden_scan: true,
        preflight_scan: false,
        fail_on_row_mismatch,
    })
    .await
}

// ─────────────────────────── 用例 ───────────────────────────

/// **一条不丢**：三段拓扑下源端产出的行数与目标端收到的行数必须相等
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_three_stage_topology_loses_no_rows() {
    let f = spec(
        "source: {concurrency: 4}\npipeline: {concurrency: 3, batch_bytes: 0}\nsink: {concurrency: 2}",
    );
    let src = FakeSource::logical(10, 250, 40);
    let snk = FakeSink::default();
    let r = go(&f, src, &snk, logical_path()).await.unwrap();
    assert_eq!(r.rows_written, 2500);
    assert_eq!(r.splits, 10);
    let rec = snk.rec.lock().unwrap();
    assert_eq!(rec.rows, 2500);
    assert!(!rec.aborted);
    // 会话数是"攒出了几份数据"，**不是** `sink.concurrency`：
    // 后者只是上限。确切的份数取决于到达时序（这份用例不钉它，
    // 钉它的是 `test_sink_sessions_follow_data_not_concurrency`），
    // 这里只断言它落在合法区间里：至少 1 份，至多一份一件（70 件）。
    assert!(
        (1..=70).contains(&rec.sessions),
        "会话数应当由数据决定（1..=70），实际 {}",
        rec.sessions
    );
}

/// **连接器自报的运行期占用，必须原样出现在报告里**
///
/// 这条保护的是"引擎不解释字节是什么"那条分工：连接器给名字与说明、往尺子上挂账，
/// 引擎只负责**如实带出来**。丢了这个读数，"账本说 2 GB、实测 6 GB"的差额
/// 就又回到"只能靠猜"的状态 —— 而那正是它存在的理由。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_report_carries_connector_reported_memory() {
    // 源端/汇端各并发 1，峰值才是确定的（并发 >1 时各份租约重叠多少不确定）
    let f = spec("source: {concurrency: 1}\npipeline: {concurrency: 1}\nsink: {concurrency: 1}");
    let src = FakeSource::logical(1, 10, 10);
    let snk = FakeSink::default();
    let r = go(&f, src, &snk, logical_path()).await.unwrap();

    let by_label = |label: &str| {
        r.connector_memory
            .iter()
            .find(|m| m.label == label)
            .unwrap_or_else(|| panic!("报告里没有 {} 的自报", label))
    };
    let src_m = by_label("fake 源端缓冲");
    assert_eq!(src_m.peak_bytes, SRC_CLAIM_BYTES);
    assert!(!src_m.note.is_empty(), "说明由连接器给，不能是空串");

    let snk_m = by_label("fake 汇端缓冲");
    assert_eq!(snk_m.peak_bytes, SINK_CLAIM_BYTES);

    // 全部销账后不该留着在账的字节 —— 留着的就是越跑越高的假账
    assert_eq!(src_m.current_bytes, 0);
    assert_eq!(snk_m.current_bytes, 0);

    assert_eq!(
        r.connector_reported_bytes(),
        SRC_CLAIM_BYTES + SINK_CLAIM_BYTES
    );
}

/// **核心**：源端并发**不会被分片数或内存改写** —— 配 3 路就最多 3 个在跑
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_source_concurrency_is_honored_verbatim() {
    static PEAK: AtomicU64 = AtomicU64::new(0);
    static LIVE: AtomicU64 = AtomicU64::new(0);

    struct CountingSource;

    #[async_trait]
    impl SourceConnector for CountingSource {
        fn name(&self) -> &str {
            "counting"
        }
        async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn schema(&self) -> D2DResult<arrow::datatypes::SchemaRef> {
            Ok(schema())
        }
        async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
            Ok((0..12)
                .map(|i| SourceSplit {
                    split_id: format!("s{}", i),
                    split_info: serde_json::json!({}),
                })
                .collect())
        }
        async fn read(&self, _: &SourceSplit) -> D2DResult<d2d_core::RecordBatchStream> {
            let live = LIVE.fetch_add(1, Ordering::SeqCst) + 1;
            PEAK.fetch_max(live, Ordering::SeqCst);
            let ids: Vec<i64> = (0..10).collect();
            let vs: Vec<String> = ids.iter().map(|i| format!("v{}", i)).collect();
            let b = arrow::array::RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(vs)),
                ],
            )
            .unwrap();
            LIVE.fetch_sub(1, Ordering::SeqCst);
            Ok(Box::pin(futures::stream::iter(vec![Ok(b)])))
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    PEAK.store(0, Ordering::SeqCst);
    let f = spec(
        "source: {concurrency: 3}\npipeline: {concurrency: 8, batch_bytes: 0}\nsink: {concurrency: 2}",
    );
    let snk = FakeSink::default();
    let r = go(&f, Arc::new(CountingSource), &snk, logical_path())
        .await
        .unwrap();
    assert_eq!(r.rows_written, 120);
    assert!(PEAK.load(Ordering::SeqCst) <= 3, "源端并发不得被放开");
}

fn expected_direct_body() -> Vec<u8> {
    let mut want = Vec::new();
    for i in 0..6usize {
        for k in 0..100usize {
            want.extend_from_slice(format!("{}|v{}\n", i * 100 + k, i * 100 + k).as_bytes());
        }
    }
    want
}

/// **直通承诺**：字节一字不改地穿过整条三段链路
///
/// 单路取数 + 单路处理 + 单路写入时，连**顺序**都与源端一致 ——
/// 这条用例把"逐位保真"钉死在最容易验证的形状上。
///
/// ⚠️ 顺序能保住，靠的是派发器**先取券、再取数据**（见 `flow.rs` 里那段注释）：
/// 若反过来，派发器会攥着一份数据等券，而那份数据已经出池了 ——
/// 前一个任务攒批时会跳过它，`concurrency: 1` 下也能把顺序打乱。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_direct_path_passes_bytes_verbatim_in_order() {
    let f = spec("source: {concurrency: 1}\npipeline: {concurrency: 1}\nsink: {concurrency: 1}");
    let src = FakeSource::physical(6, 100, 37);
    let snk = FakeSink::default();
    let r = go(&f, src, &snk, direct_path()).await.unwrap();
    assert_eq!(r.rows_written, 600);
    let rec = snk.rec.lock().unwrap();
    assert_eq!(
        rec.body,
        expected_direct_body(),
        "直通必须逐位保真，一块不多一块不少"
    );
    // 会话数 = "内核攒出了几份"，上限是源端块数（6 片 × ⌈100/37⌉ = 18 块）
    assert!(
        (1..=18).contains(&rec.sessions),
        "会话数应当由数据决定（1..=18），实际 {}",
        rec.sessions
    );
}

/// 多路并发下：**内容仍然一字不改**，但分片之间会交错
///
/// 这是拓扑的固有性质，不是缺陷：三段各自并发，就必然没有跨分片的全局顺序。
/// 目标端收到的是若干次独立的写入，各自内部仍按行边界对齐。
/// 对 Doris Stream Load 这类"每次 flush 一次独立导入"的目标端，交错完全无碍；
/// 若某个目标端依赖全局有序，那就该把三档并发都设成 1 —— 而那是个明确的选择，
/// 不是被引擎偷偷决定的。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_direct_path_bytes_intact_under_concurrency() {
    let f = spec("source: {concurrency: 3}\npipeline: {concurrency: 2}\nsink: {concurrency: 2}");
    let src = FakeSource::physical(6, 100, 37);
    let snk = FakeSink::default();
    let r = go(&f, src, &snk, direct_path()).await.unwrap();
    assert_eq!(r.rows_written, 600);
    let rec = snk.rec.lock().unwrap();

    // 逐行比对（**内容**一致，顺序可交错）
    let mut got: Vec<&[u8]> = rec.body.split(|b| *b == b'\n').collect();
    got.pop(); // 末尾空段
    let want_bytes = expected_direct_body();
    let mut want: Vec<&[u8]> = want_bytes.split(|b| *b == b'\n').collect();
    want.pop();
    let mut got_s: Vec<Vec<u8>> = got.iter().map(|l| l.to_vec()).collect();
    let mut want_s: Vec<Vec<u8>> = want.iter().map(|l| l.to_vec()).collect();
    got_s.sort();
    want_s.sort();
    assert_eq!(got_s, want_s, "并发下字节内容仍必须一条不多一条不少");
    assert_eq!(
        rec.body.len(),
        want_bytes.len(),
        "总字节数也必须一致（没有多余的拼接痕迹）"
    );
    // 每一行都以换行结尾 —— 块边界与行边界对齐的不变量
    assert_eq!(rec.body.last(), Some(&b'\n'));
}

/// 安全闸：直通载荷含受限字节 → 整任务失败并回滚（**绝不静默写出被篡改的数据**）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_forbidden_byte_fails_the_whole_job() {
    struct BadSource;

    #[async_trait]
    impl SourceConnector for BadSource {
        fn name(&self) -> &str {
            "bad"
        }
        async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn schema(&self) -> D2DResult<arrow::datatypes::SchemaRef> {
            Ok(schema())
        }
        async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
            Ok(vec![SourceSplit {
                split_id: "s0".into(),
                split_info: serde_json::json!({}),
            }])
        }
        async fn read(&self, _: &SourceSplit) -> D2DResult<d2d_core::RecordBatchStream> {
            unimplemented!()
        }
        fn as_physical(&self) -> Option<&dyn PhysicalSource> {
            Some(self)
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl PhysicalSource for BadSource {
        fn physical_format(&self) -> Option<PhysicalFormat> {
            Some(PhysicalFormat::csv(
                CsvDialect::default(),
                Charset::Utf8,
                vec!["a".into()],
            ))
        }
        async fn read_physical(&self, _: &SourceSplit) -> D2DResult<PhysicalStream> {
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(PhysicalBlock {
                    bytes: b"1|ok\n2|fine\n".to_vec(),
                    rows: 2,
                }),
                Ok(PhysicalBlock {
                    bytes: b"3|has\\backslash\n".to_vec(),
                    rows: 1,
                }),
            ])))
        }
    }

    let f = spec("source: {concurrency: 2}\npipeline: {concurrency: 2}\nsink: {concurrency: 2}");
    let snk = FakeSink::default();
    let e = go(&f, Arc::new(BadSource), &snk, direct_path())
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("违规字节"), "{}", e);
    assert!(snk.rec.lock().unwrap().aborted, "失败必须回滚会话");
}

/// 对账：目标端确认行数少于源端块计数 → 失败
/// （对应真实场景：Doris UNIQUE 模型把重复键静默去重）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_direct_row_mismatch_fails() {
    let f = spec("source: {concurrency: 2}\npipeline: {concurrency: 2}\nsink: {concurrency: 2}");
    let src = FakeSource::physical(4, 50, 25);
    let snk = FakeSink::default();
    snk.rec.lock().unwrap().drop_rows = 7;
    let e = go(&f, src, &snk, direct_path())
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("对账失败"), "{}", e);
}

/// **关掉校验 ≠ 关掉知情权**：不中止，但结论必须**照样带出来**
///
/// 这是把对账收成一处（`d2d_execution::audit`）换来的东西：以前 CLI 里那段
/// 一比 `None` 就直接 `info!("跳过行数对账")`，**关掉校验就等于什么都不知道**；
/// 现在三个数一直在，判据是一个纯函数，报告里带得出来、也测得出来。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mismatch_is_reported_even_when_verification_is_off() {
    let f = spec(
        "source: {concurrency: 2}
pipeline: {concurrency: 2}
sink: {concurrency: 2}",
    );
    let src = FakeSource::physical(4, 50, 25);
    let snk = FakeSink::default();
    snk.rec.lock().unwrap().drop_rows = 7; // 目标端每会话"静默"少收 7 行

    let r = go_with_sink(&f, src, Arc::new(snk.clone()), direct_path(), false)
        .await
        .expect("关掉校验时不该中止");

    assert!(
        matches!(r.rows.verdict(), Verdict::Mismatch { .. }),
        "报告里必须带出'对不上'的结论，实际 {:?}",
        r.rows.verdict()
    );
    assert_eq!(
        r.rows.written + r.rows.exported.unwrap_or(0),
        r.rows.written,
        "假连接器给不出源端权威数时 exported 应为 None"
    );
    assert!(r.rows.read > r.rows.written, "读出来的应当多于目标端确认的");
}

/// **回归**：三个池都配到最紧（每段 1 路、池只装一件）也不得死锁
///
/// 旧拓扑在这种配置下会死：它的死锁来自"持有内存租约去等更多内存"。
/// 新拓扑里每个任务**只持有一个槽位**，环从结构上不存在 —— 这条用例把
/// 那个结论钉在测试里。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tight_pools_do_not_deadlock() {
    let f = spec(
        "source: {concurrency: 1, pool: {capacity: 4096}}\n\
         pipeline: {concurrency: 1, batch_bytes: 0}\n\
         sink: {concurrency: 1, pool: {capacity: 4096}}",
    );
    let src = FakeSource::logical(20, 300, 64);
    let snk = FakeSink::default();
    let r = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        go(&f, src, &snk, logical_path()),
    )
    .await
    .expect("最紧的池配死锁了")
    .unwrap();
    assert_eq!(r.rows_written, 6000, "背压下也必须一条不丢");
}

/// 溢出到磁盘同样一条不丢（池容量只有 8KB，数据远大于它）
///
/// 目标端故意用 [`SlowSink`]：快消费者会把队列随手排空，池峰值只剩一两件，
/// 8192 的容量恰好装得下 —— 测试于是变成"看调度运气"（CI 上抖过一次）。
/// 慢消费者保证上游真的把池压满，溢出是确定发生的。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_spill_path_loses_no_rows() {
    let dir = std::env::temp_dir().join(format!("d2d-flow-spill-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.display().to_string().replace('\\', "/");
    let f = spec(&format!(
        "source:\n  concurrency: 2\n  pool: {{capacity: 8192, on_overflow: spill, spill_dir: \"{d}\"}}\n\
         pipeline: {{concurrency: 2, batch_bytes: 0}}\n\
         sink: {{concurrency: 1, batch_bytes: 0, batch_rows: 0, pool: {{capacity: 8192, on_overflow: spill, spill_dir: \"{d}\"}}}}"
    ));
    let src = FakeSource::logical(8, 200, 32);
    let snk = Arc::new(SlowSink {
        rec: Arc::new(Mutex::new(Rec::default())),
    });
    let r = go_with_sink(&f, src, snk, logical_path(), true)
        .await
        .unwrap();
    assert_eq!(r.rows_written, 1600);
    assert!(
        r.source_pool.spilled_items > 0 || r.sink_pool.spilled_items > 0,
        "这个配置本该触发溢出：source={:?} sink={:?}",
        r.source_pool,
        r.sink_pool
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// 源端读失败 → 整条链路失败并**唤醒所有等待者**（不得挂死）
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_source_failure_aborts_everything() {
    struct FailingSource;

    #[async_trait]
    impl SourceConnector for FailingSource {
        fn name(&self) -> &str {
            "failing"
        }
        async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
            Ok(())
        }
        async fn schema(&self) -> D2DResult<arrow::datatypes::SchemaRef> {
            Ok(schema())
        }
        async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
            Ok((0..4)
                .map(|i| SourceSplit {
                    split_id: format!("s{}", i),
                    split_info: serde_json::json!({}),
                })
                .collect())
        }
        async fn read(&self, split: &SourceSplit) -> D2DResult<d2d_core::RecordBatchStream> {
            if split.split_id == "s1" {
                return Err(D2DError::external("fake", "读库炸了"));
            }
            Ok(Box::pin(futures::stream::iter(vec![Ok(
                arrow::array::RecordBatch::try_new(
                    schema(),
                    vec![
                        Arc::new(Int64Array::from(vec![1i64])),
                        Arc::new(StringArray::from(vec!["v"])),
                    ],
                )
                .unwrap(),
            )])))
        }
        async fn close(&mut self) -> D2DResult<()> {
            Ok(())
        }
    }

    let f = spec("source: {concurrency: 2}\npipeline: {concurrency: 2}\nsink: {concurrency: 2}");
    let snk = FakeSink::default();
    let e = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        go(&f, Arc::new(FailingSource), &snk, logical_path()),
    )
    .await
    .expect("源端失败后整条链路挂死了（中止信号没传到等池的任务）")
    .unwrap_err()
    .to_string();
    assert!(e.contains("读库炸了"), "错误要原样带出来: {}", e);
}

/// 一个**完全不参与限流**的源连接器：它只是数"同时在跑几个 `read`"
///
/// 这正是新契约的形状 —— 连接器不取券、不知道上限，甚至没法"不守规矩"：
/// 一次 `read` 就是一个取数单元，而每次 `read` 由内核按任务发券才被调起来。
#[derive(Clone)]
struct CountingUnitsSource {
    splits: usize,
    /// 被调起过几次（= 派了几个任务）
    reads: Arc<AtomicUsize>,
    /// 同时在跑几个 stream / 观测到的峰值
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl CountingUnitsSource {
    fn new(splits: usize) -> Arc<Self> {
        Arc::new(Self {
            splits,
            reads: Arc::new(AtomicUsize::new(0)),
            live: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        })
    }
}

/// 流被取完（或中途随流一起被丢弃）时归还计数 —— 两条路都只还一次
struct LiveGuard(Arc<AtomicUsize>);
impl Drop for LiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl SourceConnector for CountingUnitsSource {
    fn name(&self) -> &str {
        "counting-units"
    }
    async fn init(&mut self, _: &SourceSpec) -> D2DResult<()> {
        Ok(())
    }
    async fn schema(&self) -> D2DResult<arrow::datatypes::SchemaRef> {
        Ok(schema())
    }
    async fn split(&self, _: &SourceSpec) -> D2DResult<Vec<SourceSplit>> {
        Ok((0..self.splits)
            .map(|i| SourceSplit {
                split_id: format!("s{}", i),
                split_info: serde_json::json!({}),
            })
            .collect())
    }
    async fn read(&self, split: &SourceSplit) -> D2DResult<d2d_core::RecordBatchStream> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);

        let idx: usize = split.split_id[1..].parse().unwrap();
        let rows: Vec<i64> = (0..8).map(|k| (idx * 8 + k) as i64).collect();
        let vs: Vec<String> = rows.iter().map(|i| format!("v{}", i)).collect();
        let batch = arrow::array::RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(rows)),
                Arc::new(StringArray::from(vs)),
            ],
        )
        .unwrap();

        // 这一个取数单元**活到流被取完为止**（真实连接器也是如此：游标开着就是占着一个），
        // 20ms 的延迟保证多个单元真的能重叠 —— 否则"峰值"永远是 1，什么也测不出来。
        let guard = LiveGuard(Arc::clone(&self.live));
        let stream = futures::stream::unfold((Some(batch), Some(guard)), |(b, g)| async move {
            match b {
                Some(b) => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    Some((Ok::<_, D2DError>(b), (None, g)))
                }
                // 批次已交出，`g` 随本轮的 future 一起 drop（归还计数）
                None => None,
            }
        });
        Ok(Box::pin(stream))
    }
    async fn close(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

// ══════════════ 并发：**上限是闸门，任务数由数据决定**（两句话都有牙） ══════════════

/// **上半句**：`concurrency` 真的卡住"同一时刻最多几个任务在跑"
///
/// 为什么必须有这一条：上限退化成"一个谁都可以不理的数字"时，**其他测试全都会照样绿**
/// （旧版就是这么过来的 —— `odbc` 那条路压根不读它）。只有"观测并发"的测试能钉住它。
///
/// ⚠️ 这条测试的形状变了：源连接器**从头到尾不参与限流**，连券都没见过。
/// 它只负责"一次 `read` = 一个取数单元"，卡并发完全是内核的事。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrency_caps_running_tasks() {
    const LIMIT: usize = 2;
    let f = spec(&format!(
        "source: {{concurrency: {LIMIT}, pool: {{capacity: 64MB}}}}\n\
         pipeline: {{concurrency: 4}}\n\
         sink: {{concurrency: 2, pool: {{capacity: 64MB}}}}"
    ));
    let src = CountingUnitsSource::new(16);
    let snk = FakeSink::default();

    let report = go(
        &f,
        Arc::clone(&src) as Arc<dyn SourceConnector>,
        &snk,
        logical_path(),
    )
    .await
    .unwrap();

    assert_eq!(report.rows_written, 16 * 8, "数据不能因为限流而少");
    assert_eq!(
        src.reads.load(Ordering::SeqCst),
        16,
        "16 个分片 = 16 个任务（任务数由数据决定）"
    );
    let peak = src.peak.load(Ordering::SeqCst);
    assert!(
        peak <= LIMIT,
        "同时在跑的任务 {} 个，超过了引擎设的上限 {} —— 闸门没卡住",
        peak,
        LIMIT
    );
    assert_eq!(
        src.live.load(Ordering::SeqCst),
        0,
        "任务退出必须归还额度，否则额度会漏光"
    );
    assert_eq!(
        report.source_gate_peak, LIMIT,
        "16 个分片足以打满 2 路，闸门峰值应当等于上限"
    );
}

/// **下半句**：数据少就只产生少数任务 —— 并发只是**上限**，不是任务数
///
/// 3 个分片配 16 路并发：任务是 3 个，不是 16 个。
/// 这一条把"配了 16 路"与"开了 16 个任务"彻底分开：
/// 前者是闸门的宽度，后者由数据决定。峰值读数同时也就有了第二个含义 ——
/// 它可能小，**不是因为数配小了，而是因为数据就这么点**。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_task_count_follows_data_not_concurrency() {
    let f = spec("source: {concurrency: 16}\npipeline: {concurrency: 16}\nsink: {concurrency: 16}");
    let src = CountingUnitsSource::new(3);
    let snk = FakeSink::default();

    let report = go(
        &f,
        Arc::clone(&src) as Arc<dyn SourceConnector>,
        &snk,
        logical_path(),
    )
    .await
    .unwrap();

    assert_eq!(report.rows_written, 3 * 8);
    assert_eq!(
        src.reads.load(Ordering::SeqCst),
        3,
        "3 片数据就是 3 个任务 —— 并发是上限，不是任务数"
    );
    assert!(
        src.peak.load(Ordering::SeqCst) <= 3,
        "3 份数据不可能跑满 16 路"
    );
    assert_eq!(
        report.source_gate_peak, 3,
        "峰值如实把'并发配得比数据还大'暴露出来"
    );
}

/// **目标端同样成立**：一份数据就写一次，哪怕配了 16 路写入并发
///
/// 会话是**按份**开的 —— 一份数据一条。这条守的是"会话数 ≠ 并发数"：
/// 顺着旧的心智模型很容易写成"开 N 条会话、各自循环取活干"，
/// 那样任务数就又被并发数绑死了。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sink_sessions_follow_data_not_concurrency() {
    // 1 个分片、10 行、单批 —— 池里从头到尾只有一份
    let f = spec("source: {concurrency: 1}\npipeline: {concurrency: 1}\nsink: {concurrency: 16}");
    let src = FakeSource::logical(1, 10, 10);
    let snk = FakeSink::default();

    let r = go(&f, src, &snk, logical_path()).await.unwrap();
    assert_eq!(r.rows_written, 10);
    let rec = snk.rec.lock().unwrap();
    assert_eq!(
        rec.sessions, 1,
        "只有一份数据，就只该开一条会话（16 路是上限，不是任务数）"
    );
    assert_eq!(rec.batches, 1, "一份数据 = 一次写入");
}

/// 一份写得很慢的会话 —— 用来把时序钉住，好观测"内核攒批"
struct SlowSink {
    rec: Arc<Mutex<Rec>>,
}

#[async_trait]
impl SinkConnector for SlowSink {
    fn name(&self) -> &str {
        "slow"
    }
    async fn init(&mut self, _: &SinkSpec) -> D2DResult<()> {
        Ok(())
    }
    async fn begin_session(&self, _: &SinkSpec) -> D2DResult<Box<dyn SinkSession>> {
        Ok(Box::new(SlowSession {
            rec: Arc::clone(&self.rec),
        }))
    }
    async fn close(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

struct SlowSession {
    rec: Arc<Mutex<Rec>>,
}

#[async_trait]
impl SinkSession for SlowSession {
    async fn write_batch(&mut self, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
        // 一次写入期间上游把池填满 —— 下一次 `gather` 就能一口气攒走一大批
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut r = self.rec.lock().unwrap();
        r.rows += batch.num_rows() as u64;
        r.batches += 1;
        Ok(batch.num_rows())
    }
    async fn commit(&mut self) -> D2DResult<()> {
        Ok(())
    }
    async fn abort(&mut self) -> D2DResult<()> {
        Ok(())
    }
}

/// **攒批住在内核**：连接器收到的是内核攒好的一份，而不是源端的一个小批次
///
/// 对照组是 `sink.batch_bytes: 0`（不攒）—— 那时"一份数据 = 一个源端批次"，
/// 100 个小批次就是 100 次写入、100 条会话。
/// 开了攒批之后，内核从池里攒够才交出去，写入次数掉到个位数。
///
/// 这一条同时说明了"会话按份开"的代价为什么可控：会话开得频繁，
/// 所以 HTTP 客户端必须提到连接器级（`DorisSinkConnector::client`）—— 否则
/// "每次写入建一个客户端"会原样长回来。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sink_batching_happens_in_the_kernel() {
    let base = "source: {concurrency: 1, pool: {capacity: 64MB}}\n\
                pipeline: {concurrency: 1, batch_bytes: 0}";
    let src: Arc<dyn SourceConnector> = FakeSource::logical(1, 1000, 10); // 100 个 10 行的小批

    let rec0 = Arc::new(Mutex::new(Rec::default()));
    let f0 = spec(&format!(
        "{base}\nsink: {{concurrency: 1, batch_bytes: 0, batch_rows: 0, pool: {{capacity: 64MB}}}}"
    ));
    go_with_sink(
        &f0,
        Arc::clone(&src),
        Arc::new(SlowSink {
            rec: Arc::clone(&rec0),
        }),
        logical_path(),
        true,
    )
    .await
    .unwrap();
    let no_batch = rec0.lock().unwrap().batches;

    let rec1 = Arc::new(Mutex::new(Rec::default()));
    let f1 = spec(&format!(
        "{base}\nsink: {{concurrency: 1, batch_bytes: 64MB, pool: {{capacity: 64MB}}}}"
    ));
    go_with_sink(
        &f1,
        src,
        Arc::new(SlowSink {
            rec: Arc::clone(&rec1),
        }),
        logical_path(),
        true,
    )
    .await
    .unwrap();
    let batched = rec1.lock().unwrap().batches;

    assert_eq!(
        rec0.lock().unwrap().rows,
        rec1.lock().unwrap().rows,
        "攒批不得改变行数"
    );
    assert_eq!(no_batch, 100, "不攒批时：一个源端批次 = 一次写入");
    assert!(
        batched < no_batch,
        "内核攒批应当大幅减少写入次数：不攒 {} 次 vs 攒批 {} 次",
        no_batch,
        batched
    );
}
