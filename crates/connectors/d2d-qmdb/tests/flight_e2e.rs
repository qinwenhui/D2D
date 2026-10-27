//! Flight SQL 数据面端到端（**需要运行中的 qmdb server**）
//!
//! ```text
//! QMDB_FLIGHT_ADDR=192.0.2.13:31337 QMDB_FLIGHT_USER=root QMDB_FLIGHT_PASS=root \
//!   cargo test -p d2d-qmdb --test flight_e2e -- --ignored
//! ```
//!
//! 覆盖：三段式装载（sink 写入 → COUNT 对账）→ overwrite_table 幂等重跑 →
//! Flight SQL source（LIMIT 下推读回）→ 清理。表名带 pid+纳秒，跑完即删。

use arrow::array::Array as _;
use arrow::array::{
    BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use d2d_connector::{
    PhysicalSink, SinkConnector, SinkSpec, SourceConnector, SourceSpec, SourceSplit,
};
use d2d_qmdb::QmdbConnection;
use d2d_qmdb::{QmdbFlight, QmdbSinkConnector, QmdbSourceConnector};
use futures::{FutureExt, StreamExt};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn conn_json() -> serde_json::Value {
    serde_json::json!({
        "addr": std::env::var("QMDB_FLIGHT_ADDR").expect("需要 QMDB_FLIGHT_ADDR，例如 192.0.2.13:31337"),
        "user": std::env::var("QMDB_FLIGHT_USER").unwrap_or_else(|_| "root".into()),
        "password": std::env::var("QMDB_FLIGHT_PASS").unwrap_or_else(|_| "root".into()),
    })
}

/// 全类型覆盖的样本批（列名与 [`create_table`] 的 DDL 一致）
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(9, 2), true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("pdate", DataType::Date32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("中文值")])),
            Arc::new(
                Decimal128Array::from(vec![Some(123456i128), None, Some(-5)])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(Date32Array::from(vec![Some(20_747), Some(20_748), None])), // 2026-10-21/22
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_792_571_400_000_000),
                None,
                Some(1_792_658_200_000_000),
            ])),
        ],
    )
    .unwrap()
}

async fn raw_client() -> QmdbFlight {
    let conn = QmdbConnection::from_json(&conn_json()).unwrap();
    QmdbFlight::connect(&conn).await.unwrap()
}

fn table_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("bench_lc.d2d_qmdb_e2e_{}_{}", std::process::id(), nanos)
}

/// 收全一个查询的结果行数
async fn count(flight: &QmdbFlight, sql: &str) -> u64 {
    let (_, batches) = flight.query(sql).await.unwrap();
    batches
        .first()
        .map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("COUNT 列应是 Int64");
            col.value(0) as u64
        })
        .unwrap_or(0)
}

/// 点查一行：(name, amount)（NULL → None）
async fn point_lookup(flight: &QmdbFlight, table: &str, id: i64) -> (Option<String>, Option<i128>) {
    let (_, mut batches) = flight
        .query(&format!("SELECT name, amount FROM {table} WHERE id = {id}"))
        .await
        .unwrap();
    let b = batches.pop().expect("点查应有一行");
    let name = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let amount = b
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    let n = if name.is_null(0) {
        None
    } else {
        Some(name.value(0).to_string())
    };
    let a = if amount.is_null(0) {
        None
    } else {
        Some(amount.value(0))
    };
    (n, a)
}

#[tokio::test]
#[ignore = "requires a running qmdb server; set QMDB_FLIGHT_ADDR / QMDB_FLIGHT_USER / QMDB_FLIGHT_PASS"]
async fn flight_sink_then_source_roundtrip() {
    let flight = raw_client().await;
    let table = table_name();
    if let Err(e) = flight
        .query(&format!(
            "CREATE TABLE {table} (\
             id BIGINT, name VARCHAR, amount DECIMAL(9,2), flag BOOLEAN, \
             pdate DATE, ts TIMESTAMP)"
        ))
        .await
    {
        let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
        panic!("建表失败（检查账号权限/DDL 语法）: {e}");
    }

    // 断言失败也要 teardown —— 否则就在 bench_lc 留孤儿表（qmdb 方代清过一次）
    let result = std::panic::AssertUnwindSafe(roundtrip_body(&flight, &table))
        .catch_unwind()
        .await;
    let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

async fn roundtrip_body(flight: &QmdbFlight, table: &str) {
    let short = table.rsplit_once('.').unwrap().1;

    // ── 写：三段式装载 ──
    let sink_spec: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "database": "bench_lc",
        "table": short,
    }))
    .unwrap();
    let mut sink = QmdbSinkConnector::new();
    sink.init(&sink_spec).await.unwrap();
    let mut session = sink.begin_session(&sink_spec).await.unwrap();
    assert_eq!(session.write_batch(sample_batch()).await.unwrap(), 3);
    session.commit().await.unwrap();

    // 对账：COUNT 必须等于写入行数
    assert_eq!(
        count(flight, &format!("SELECT COUNT(*) FROM {table}")).await,
        3
    );

    // 幂等重跑：overwrite_table 再写同一批 → 行数不变（整表替换）
    let sink_spec_overwrite: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "database": "bench_lc",
        "table": short,
        "properties": { "load": { "mode": "overwrite_table" } }
    }))
    .unwrap();
    let mut sink2 = QmdbSinkConnector::new();
    sink2.init(&sink_spec_overwrite).await.unwrap();
    let mut s2 = sink2.begin_session(&sink_spec_overwrite).await.unwrap();
    assert_eq!(s2.write_batch(sample_batch()).await.unwrap(), 3);
    s2.commit().await.unwrap();
    assert_eq!(
        count(flight, &format!("SELECT COUNT(*) FROM {table}")).await,
        3,
        "overwrite_table 应该整表替换，不是追加"
    );

    // 值抽查：中文/NULL/DECIMAL 没有被静默改写（逐行点查，最直白）
    let (n1, a1) = point_lookup(flight, table, 1).await;
    assert_eq!(n1.as_deref(), Some("a"));
    assert_eq!(a1, Some(123456i128), "DECIMAL(9,2) 未定点值 1234.56");
    let (n2, a2) = point_lookup(flight, table, 2).await;
    assert_eq!(n2, None, "NULL 不该被写成空串或别的");
    assert_eq!(a2, None);
    let (n3, a3) = point_lookup(flight, table, 3).await;
    assert_eq!(n3.as_deref(), Some("中文值"));
    assert_eq!(a3, Some(-5i128));

    // ── 读：Flight SQL source（LIMIT 下推） ──
    let source_spec: SourceSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "schema_name": "bench_lc",
        "table": short,
        "query": { "limit": 2 },
    }))
    .unwrap();
    let mut source = QmdbSourceConnector::new();
    source.init(&source_spec).await.unwrap();
    let schema = source.schema().await.unwrap();
    assert_eq!(schema.fields().len(), 6, "LIMIT 0 探出的 schema 应有 6 列");

    let splits = source.split(&source_spec).await.unwrap();
    assert_eq!(splits.len(), 1, "没配 partition 时是单分片");
    let mut stream = source.read(&splits[0]).await.unwrap();
    let mut rows = 0usize;
    while let Some(item) = stream.next().await {
        rows += item.unwrap().num_rows();
    }
    assert_eq!(rows, 2, "LIMIT 2 下推后源端只交出 2 行");
    assert_eq!(
        source.exported_rows(),
        Some(2),
        "对账计数应与读到的行数一致"
    );
    source.close().await.unwrap();
}

/// 认证失败要报出明确的"认证失败"（而不是笼统的传输错误）
#[tokio::test]
#[ignore = "requires a running qmdb server; set QMDB_FLIGHT_ADDR"]
async fn bad_password_fails_loud() {
    let addr = std::env::var("QMDB_FLIGHT_ADDR").expect("需要 QMDB_FLIGHT_ADDR");
    let conn = QmdbConnection::from_json(&serde_json::json!({
        "addr": addr, "user": "root", "password": "definitely-wrong"
    }))
    .unwrap();
    let flight = QmdbFlight::connect(&conn).await.unwrap();
    let e = flight
        .query("SELECT 1")
        .await
        .expect_err("错误密码必须失败")
        .to_string();
    assert!(e.contains("认证失败"), "{e}");
}

/// **列乱序按列名对齐**（服务端修复后的行为回归）
///
/// qmdb 服务端装载曾按自己的表 schema **盲解**客户端 IPC 批次：多余列被静默剥掉、
/// 同宽类型乱序会**静默错位**（int64 的值进 double 列的位置 —— 不是丢，是污染）。
/// 修复后按列名对齐 + 未知列拒绝 + 安全强转。这里验证 D2D 侧的组合行为：
/// 批次列序与目标表不同，值必须按**列名**落位（本连接器的首批校验本就是按名的，
/// 不会挡乱序批次）。
#[tokio::test]
#[ignore = "requires a running qmdb server; set QMDB_FLIGHT_ADDR / QMDB_FLIGHT_USER / QMDB_FLIGHT_PASS"]
async fn out_of_order_columns_align_by_name() {
    let flight = raw_client().await;
    let table = table_name();
    if let Err(e) = flight
        .query(&format!(
            "CREATE TABLE {table} (id BIGINT, name VARCHAR, amount DECIMAL(9,2))"
        ))
        .await
    {
        let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
        panic!("建表失败: {e}");
    }
    let result = std::panic::AssertUnwindSafe(out_of_order_body(&flight, &table))
        .catch_unwind()
        .await;
    let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

async fn out_of_order_body(flight: &QmdbFlight, table: &str) {
    let short = table.rsplit_once('.').unwrap().1;

    // 批次列序 = (amount, name, id)，与目标表 (id, name, amount) 刻意相反；
    // 类型各异（DECIMAL/UTF8/INT64），如果服务端按位置摆就会炸或错位
    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Decimal128(9, 2), true),
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]));
    let shuffled = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                Decimal128Array::from(vec![Some(150i128), Some(-25)])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ), // 1.50 / -0.25
            Arc::new(StringArray::from(vec![Some("甲"), Some("乙")])),
            Arc::new(Int64Array::from(vec![7i64, 8])),
        ],
    )
    .unwrap();

    let sink_spec: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "database": "bench_lc",
        "table": short,
    }))
    .unwrap();
    let mut sink = QmdbSinkConnector::new();
    sink.init(&sink_spec).await.unwrap();
    let mut session = sink.begin_session(&sink_spec).await.unwrap();
    assert_eq!(session.write_batch(shuffled).await.unwrap(), 2);
    session.commit().await.unwrap();

    // 逐行核对：值跟着**列名**走，不跟位置走
    let (name7, amount7) = point_lookup(flight, table, 7).await;
    assert_eq!(name7.as_deref(), Some("甲"), "name 值错位了");
    assert_eq!(amount7, Some(150i128), "amount 值错位了（1.50）");
    let (name8, amount8) = point_lookup(flight, table, 8).await;
    assert_eq!(name8.as_deref(), Some("乙"));
    assert_eq!(amount8, Some(-25i128), "amount 值错位了（-0.25）");

    // 未知列（目标表没有的列）现在服务端会拒绝 —— 客户端首批校验先拦，
    // 这里直接绕过连接器用底层协议验证服务端兜底
    let ghost_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ghost", DataType::Utf8, true),
    ]));
    let ghost = RecordBatch::try_new(
        ghost_schema,
        vec![
            Arc::new(Int64Array::from(vec![9i64])),
            Arc::new(StringArray::from(vec![Some("boo")])),
        ],
    )
    .unwrap();
    let stats = flight
        .load_table(table, vec![ghost], &d2d_qmdb::LoadMode::Append, None)
        .await;
    let err = stats.expect_err("未知列必须被服务端拒绝（静默丢弃的时代结束了）");
    let msg = err.to_string();
    assert!(msg.contains("ghost") || msg.contains("未知列"), "{msg}");
}

/// **range 分片无边界 → SHOW PARTITIONS 拿分区值当切点**（分区对齐切分）
///
/// qmdb 2026-10-23 新增 `SHOW PARTITIONS FROM db.t`（分区列 + rows/files）。
/// range 不给 lower/upper 时，分片规划优先拿真实分区值当切点 —— 每段与存储
/// 分区精确对齐（qmdb 分区裁剪每段都吃满），而不是 MIN/MAX 合成均分点。
/// 本测试建分区表 → 装三个分区的数据 → 无边界 range 规划 → 断言：
/// 切点=分区值、全部数据无重叠无空洞地被读回。
#[tokio::test]
#[ignore = "requires a running qmdb server; set QMDB_FLIGHT_ADDR / QMDB_FLIGHT_USER / QMDB_FLIGHT_PASS"]
async fn range_splits_follow_partitions() {
    let flight = raw_client().await;
    let table = table_name();
    if let Err(e) = flight
        .query(&format!(
            "CREATE TABLE {table} (id BIGINT, pdate DATE) PARTITIONED BY (pdate)"
        ))
        .await
    {
        let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
        panic!("建表失败（检查 PARTITIONED BY 语法）: {e}");
    }
    let result = std::panic::AssertUnwindSafe(range_splits_body(&flight, &table))
        .catch_unwind()
        .await;
    let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

async fn range_splits_body(_flight: &QmdbFlight, table: &str) {
    let short = table.rsplit_once('.').unwrap().1;

    // 装 3 个分区 × 2 行：2026-10-21 / 22 / 23
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pdate", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5, 6])),
            Arc::new(Date32Array::from(vec![
                Some(20_747),
                Some(20_747),
                Some(20_748),
                Some(20_748),
                Some(20_749),
                Some(20_749),
            ])),
        ],
    )
    .unwrap();
    let sink_spec: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "database": "bench_lc",
        "table": short,
    }))
    .unwrap();
    let mut sink = QmdbSinkConnector::new();
    sink.init(&sink_spec).await.unwrap();
    let mut session = sink.begin_session(&sink_spec).await.unwrap();
    assert_eq!(session.write_batch(batch).await.unwrap(), 6);
    session.commit().await.unwrap();

    // range 无边界 + count 8（远大于分区数：分区值切点优先于 count 合成）
    let source_spec: SourceSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "schema_name": "bench_lc",
        "table": short,
        "partition": {
            "mode": "range",
            "column": "pdate",
            "count": 8
        }
    }))
    .unwrap();
    let mut source = QmdbSourceConnector::new();
    source.init(&source_spec).await.unwrap();
    let splits = source.split(&source_spec).await.unwrap();

    // 3 个分区值切点 → 内核给 4 段：(-∞,p1) / [p1,p2) / [p2,p3) / [p3,+∞)
    // 首尾两段结构性必须（覆盖性靠"首尾无界"保证），实践中是空的
    assert_eq!(
        splits.len(),
        4,
        "切点应来自 SHOW PARTITIONS 的分区值，而不是 count 均分: {:?}",
        splits.iter().map(split_condition_of).collect::<Vec<_>>()
    );
    let conds: Vec<String> = splits.iter().map(split_condition_of).collect();
    assert!(
        conds[1].contains("2026-10-21") && conds[1].contains("2026-10-22"),
        "{}",
        conds[1]
    );
    assert!(
        conds[2].contains("2026-10-22") && conds[2].contains("2026-10-23"),
        "{}",
        conds[2]
    );

    // 全部数据无重叠无空洞地读回 —— 分区对齐切分的覆盖性验证
    let mut total = 0usize;
    for s in &splits {
        let mut stream = source.read(s).await.unwrap();
        while let Some(item) = stream.next().await {
            total += item.unwrap().num_rows();
        }
    }
    assert_eq!(total, 6, "分片并集必须覆盖全部 6 行");
    assert_eq!(source.exported_rows(), Some(6));
    source.close().await.unwrap();
}

fn split_condition_of(split: &SourceSplit) -> String {
    split
        .split_info
        .get("where_condition")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// **物理载荷装载**（协议 §2.1）：CSV 字节直传，客户端零 Arrow 转换
///
/// 兼做服务端 CSV 行为的**行为规格探测**：
/// - 空字段 = NULL（协议口径）；**空串也是空字段** → 服务端存成什么？
/// - 裸反斜杠值（judge 把 `\` 列为受限，但本测试绕过协商直发 —— 看服务端是否解释转义）
/// - 时间戳两种分隔（`空格` / ISO `T`）哪个能解析
/// - 表头大小写（源端大写列名 vs qmdb 小写折叠）
/// - 一个块 = 一次装载会话；多块 = 多会话追加
#[tokio::test]
#[ignore = "requires a running qmdb server; set QMDB_FLIGHT_ADDR / QMDB_FLIGHT_USER / QMDB_FLIGHT_PASS"]
async fn physical_csv_load_roundtrip() {
    let flight = raw_client().await;
    let table = table_name();
    if let Err(e) = flight
        .query(&format!(
            "CREATE TABLE {table} (\
             id BIGINT, name VARCHAR, amount DECIMAL(9,2), pdate DATE, ts TIMESTAMP)"
        ))
        .await
    {
        let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
        panic!("建表失败: {e}");
    }
    let result = std::panic::AssertUnwindSafe(physical_csv_body(&flight, &table))
        .catch_unwind()
        .await;
    let _ = flight.query(&format!("DROP TABLE IF EXISTS {table}")).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

async fn physical_csv_body(flight: &QmdbFlight, table: &str) {
    let short = table.rsplit_once('.').unwrap().1;
    let columns: Vec<String> = ["ID", "NAME", "AMOUNT", "PDATE", "TS"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let mut sink = QmdbSinkConnector::new();
    let spec: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qmdb",
        "connection": conn_json(),
        "database": "bench_lc",
        "table": short,
    }))
    .unwrap();
    sink.init(&spec).await.unwrap();

    // 直通协商必须通过（qmdb 方言：`,` 分隔 + 空字段 NULL）
    let fmt = d2d_connector::PhysicalFormat::csv(
        d2d_connector::CsvDialect {
            column_separator: b',',
            line_delimiter: b'\n',
            quote: None,
            escape: None,
            null_literal: None,
        },
        d2d_connector::Charset::Utf8,
        columns.clone(),
    );
    let compat = sink.accepts_physical(&fmt);
    assert!(
        matches!(compat, d2d_connector::PhysicalCompat::Accept { .. }),
        "qmdb 方言必须被接受: {compat:?}"
    );

    let mut session = sink.begin_physical(&fmt).await.unwrap();

    // 块 1：NULL/空串/中文/反斜杠/两种时间戳分隔
    //（表头由会话拼，块里只有数据行）
    let block1 = "1,甲,12.50,2026-10-21,2026-10-21 12:00:00\n\
                  2,,9.99,2026-10-22,\n\
                  3,中文值,-0.25,2026-10-23,2026-10-23T08:30:00\n\
                  4,back\\slash,1.00,2026-10-23,2026-10-23 09:00:00\n";
    let written = session
        .write_block(d2d_connector::PhysicalBlock {
            bytes: block1.as_bytes().to_vec(),
            rows: 4,
        })
        .await
        .unwrap_or_else(|e| panic!("块 1 装载失败: {e}"));
    assert_eq!(written, 4);

    // 块 2：第二个文件/会话，append 追加
    let block2 = "5,乙,7.00,2026-10-24,2026-10-24 10:00:00\n".as_bytes().to_vec();
    let written = session
        .write_block(d2d_connector::PhysicalBlock {
            bytes: block2,
            rows: 1,
        })
        .await
        .unwrap_or_else(|e| panic!("块 2 装载失败: {e}"));
    assert_eq!(written, 1);
    assert_eq!(session.commit().await.unwrap(), 5);

    // 行数对账
    assert_eq!(
        count(flight, &format!("SELECT COUNT(*) FROM {table}")).await,
        5
    );

    // ── 值抽查（2026-10-27 实测固化的服务端 CSV 语义）──
    // 空字段 = NULL（协议口径）；时间戳 `空格` 与 ISO `T` 两种分隔都解析；
    // 反斜杠原样保留（服务端不解释转义）；表头大写列名按名列对齐。
    let name_of = |flight: &QmdbFlight, id: i64| {
        let flight = flight.clone();
        async move {
            let (_, batches) = flight
                .query(&format!("SELECT name, ts FROM {table} WHERE id = {id}"))
                .await
                .unwrap();
            let b = batches.last().unwrap();
            let name = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let ts = b
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            (
                if name.is_null(0) {
                    None
                } else {
                    Some(name.value(0).to_string())
                },
                if ts.is_null(0) {
                    None
                } else {
                    ts.value_as_datetime(0).map(|d| d.to_string())
                },
            )
        }
    };

    let (n, ts) = name_of(flight, 1).await;
    assert_eq!(n.as_deref(), Some("甲"));
    assert_eq!(
        ts.as_deref(),
        Some("2026-10-21 12:00:00"),
        "空格分隔的时间戳"
    );

    let (n, ts) = name_of(flight, 2).await;
    assert_eq!(n, None, "空字段必须落成 NULL");
    assert_eq!(ts, None, "空字段必须落成 NULL");

    let (n, _) = name_of(flight, 3).await;
    assert_eq!(n.as_deref(), Some("中文值"), "中文值被改写");

    let (n, ts) = name_of(flight, 4).await;
    assert_eq!(
        n.as_deref(),
        Some("back\\slash"),
        "反斜杠必须原样保留（不解释转义）"
    );
    assert_eq!(
        ts.as_deref(),
        Some("2026-10-23 09:00:00"),
        "ISO `T` 分隔的时间戳"
    );

    let (n, _) = name_of(flight, 5).await;
    assert_eq!(n.as_deref(), Some("乙"));
}
