//! HTTP 模式端到端（**需要外部运行中的 `qsdb server`**）
//!
//! ```text
//! qsdb server --path /tmp/qsdb-e2e --addr 127.0.0.1:19430
//! QSDB_HTTP_URL=http://127.0.0.1:19430 \
//!   cargo test -p d2d-qsdb --test http_e2e -- --ignored
//! ```
//!
//! 覆盖：HTTP sink 自动建表 + Arrow IPC 写入 → HTTP source SQL 查询（含 WHERE /
//! ORDER BY / LIMIT 下推）→ rows 流还原成 Arrow 批。

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use d2d_connector::{SinkConnector, SinkSpec, SourceConnector, SourceSpec};
use d2d_qsdb::{QsdbSinkConnector, QsdbSourceConnector};
use futures::StreamExt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
        ],
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires a running qsdb server; set QSDB_HTTP_URL=http://127.0.0.1:9430"]
async fn http_sink_then_source_roundtrip() {
    let url =
        std::env::var("QSDB_HTTP_URL").expect("需要 QSDB_HTTP_URL，例如 http://127.0.0.1:19430");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("d2d_http_e2e_{}_{}", std::process::id(), nanos);

    // ── 写：HTTP sink（自动建表 + IPC append） ──
    let sink_spec: SinkSpec = serde_json::from_value(serde_json::json!({
        "connector": "qsdb",
        "method": "http",
        "connection": { "url": url },
        "database": "default",
        "table": table,
        "properties": { "create_table": true, "compression": "zstd" }
    }))
    .unwrap();
    let mut sink = QsdbSinkConnector::new();
    sink.init(&sink_spec).await.unwrap();
    let mut session = sink.begin_session(&sink_spec).await.unwrap();
    assert_eq!(session.write_batch(sample_batch()).await.unwrap(), 3);
    session.commit().await.unwrap();

    // ── 读：HTTP source（SQL 下推 WHERE / ORDER BY / LIMIT） ──
    let source_spec: SourceSpec = serde_json::from_value(serde_json::json!({
        "connector": "qsdb",
        "method": "http",
        "connection": { "url": url },
        "table": table,
        "query": {
            "columns": ["id"],
            "where_clause": "id >= 2",
            "order_by": "id DESC",
            "limit": 1
        }
    }))
    .unwrap();
    let mut src = QsdbSourceConnector::new();
    src.init(&source_spec).await.unwrap();
    let schema = src.schema().await.unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);

    let splits = src.split(&source_spec).await.unwrap();
    assert_eq!(splits.len(), 1);
    let mut stream = src.read(&splits[0]).await.unwrap();
    let mut ids = Vec::new();
    while let Some(b) = stream.next().await {
        let b = b.unwrap();
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id 应该是 Int64");
        ids.extend(col.iter().flatten());
    }
    assert_eq!(ids, vec![3]);
    assert_eq!(src.exported_rows(), Some(1));
}

#[tokio::test]
#[ignore = "requires a running qsdb server; set QSDB_HTTP_URL=http://127.0.0.1:9430"]
async fn http_source_rejects_non_default_db() {
    let url =
        std::env::var("QSDB_HTTP_URL").expect("需要 QSDB_HTTP_URL，例如 http://127.0.0.1:19430");
    let spec: SourceSpec = serde_json::from_value(serde_json::json!({
        "connector": "qsdb",
        "method": "http",
        "connection": { "url": url },
        "schema_name": "app",
        "table": "whatever"
    }))
    .unwrap();
    let mut src = QsdbSourceConnector::new();
    let e = src.init(&spec).await.unwrap_err().to_string();
    assert!(e.contains("单库"), "{}", e);
}
