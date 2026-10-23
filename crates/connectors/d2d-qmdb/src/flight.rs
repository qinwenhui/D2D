//! qmdb **Flight SQL 数据面客户端**（arrow 60 原生实现）
//!
//! 协议（qmdb 方协议文档为准，参考实现 `tests/qmdb_flight.py` / `crates/qmdb-client`）：
//!
//! ```text
//! 查询： GetFlightInfo(Any(CommandStatementQuery)) → endpoints[0].ticket
//!        DoGet(ticket) → FlightData 流（首条 IPC Schema，后续 RecordBatch）
//! 装载： DoAction("begin_load", {table, mode, preds})   → {load_id}
//!        DoPut(desc{cmd: {"load_id"}}, <FlightData 流>)
//!        DoAction("commit_load", {load_id})             → {sn, rows, files, bytes}
//!        失败/放弃： DoAction("abort_load", {load_id})  → staging 自动清理
//! ```
//!
//! # 为什么不引 qmdb-client
//!
//! 它锁 arrow 59（服务端 DataFusion 的锁链），内核全线 arrow 60。线格式
//! （gRPC + Arrow IPC）跨 arrow crate 版本稳定 —— qmdb 方协议文档 §0 明确
//! arrow 59/60 客户端均可对接。这里零转换：服务端来的就是 arrow 60 的
//! `RecordBatch`，进内核不需要任何桥接。
//!
//! # 会话与并发
//!
//! 认证 = 每个 RPC 的 metadata 头（无需握手）；会话按"凭证+通道"聚合。
//! [`tonic::transport::Channel`] 是 HTTP/2 多路复用的：克隆它给多个分片读 /
//! 写入会话共享，不会多建 TCP 连接。

use crate::config::{LoadMode, QmdbConnection, Watermark};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{Any, CommandStatementQuery};
use arrow_flight::{Action, FlightClient, FlightDescriptor};
use d2d_core::{D2DError, D2DResult};
use futures::StreamExt;
use prost::Message as _;
use std::sync::Arc;

/// DoGet 流的标称在手缓冲（内存账口径，与 doris flight 同一个 16MB）
pub const NOMINAL_FLIGHT_BUFFER: usize = 16 * 1024 * 1024;

/// 装载出站的切片行数 —— 服务端建议 8192 行/批左右
///
/// 内核攒出来的批动辄几十万行，一整批编成一条巨大的 IPC 消息既占两端内存，
/// 也让 gRPC 流控失去粒度。切片是零拷贝视图（`RecordBatch::slice`），不搬数据。
pub const LOAD_CHUNK_ROWS: usize = 8192;

/// 装载摘要（服务端 commit_load 返回体；`bytes` 兼容旧服务端缺省为 0）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadStats {
    pub sn: u64,
    pub rows: u64,
    pub files: u64,
    pub bytes: u64,
}

/// qmdb Flight 客户端：一条已拨号的 gRPC 通道 + 认证参数
#[derive(Clone)]
pub struct QmdbFlight {
    channel: tonic::transport::Channel,
    user: String,
    password: String,
    addr: String,
}

impl QmdbFlight {
    /// 连接服务端（真拨号，失败立刻报错 —— 与 qmdb-client 同一口径）
    pub async fn connect(conn: &QmdbConnection) -> D2DResult<Self> {
        let channel = conn.channel().await?;
        Ok(Self {
            channel,
            user: conn.user.clone(),
            password: conn.password.clone(),
            addr: conn.addr.clone(),
        })
    }

    /// 造一个带认证头的 FlightClient（每次 RPC 自己用一个；通道共享）
    fn client(&self) -> D2DResult<FlightClient> {
        let mut client = FlightClient::new(self.channel.clone());
        client
            .add_header(
                "authorization",
                &format!("Bearer {}:{}", self.user, self.password),
            )
            .map_err(|e| D2DError::external("qmdb", format!("设置认证头失败: {e}")))?;
        Ok(client)
    }

    fn err(&self, what: &str, status: tonic::Status) -> D2DError {
        match status.code() {
            tonic::Code::Unauthenticated => D2DError::connection(
                "qmdb",
                format!(
                    "{what}：认证失败（{}）—— {}。注意：服务端连续失败 10 次会锁来源 IP 300s",
                    self.addr,
                    status.message()
                ),
            ),
            tonic::Code::Unavailable => D2DError::connection(
                "qmdb",
                format!("{what}：服务 {} 不可达 —— {}", self.addr, status.message()),
            ),
            _ => D2DError::external("qmdb", format!("{what}: {}", status.message())),
        }
    }

    fn flight_err(&self, what: &str, e: FlightError) -> D2DError {
        match e {
            FlightError::Tonic(status) => self.err(what, *status),
            other => D2DError::external("qmdb", format!("{what}: {other}")),
        }
    }

    // ---------- 查询 ----------

    /// 执行查询，逐批消费（**流式**：内存与结果总量无关）
    ///
    /// gRPC 流控把背压自然传回服务端：下游消费多快，服务端就推多快。
    pub async fn stream_query(
        &self,
        sql: &str,
    ) -> D2DResult<futures::stream::BoxStream<'static, D2DResult<RecordBatch>>> {
        let ticket = self.statement_ticket(sql).await?;
        let mut client = self.client()?;
        let stream = client
            .do_get(ticket)
            .await
            .map_err(|e| self.flight_err(&format!("DoGet（{sql}）"), e))?;
        let addr = self.addr.clone();
        Ok(stream
            .map(move |item| match item {
                Ok(b) => Ok(b),
                Err(e) => Err(D2DError::external(
                    "qmdb",
                    format!("读查询流失败（{addr}）: {e}"),
                )),
            })
            .boxed())
    }

    /// 执行查询并收全结果（探测/元数据用；**不要**拿它读大结果）
    pub async fn query(&self, sql: &str) -> D2DResult<(SchemaRef, Vec<RecordBatch>)> {
        let mut schema: Option<SchemaRef> = None;
        let mut batches = Vec::new();
        let mut stream = self.stream_query(sql).await?;
        while let Some(item) = stream.next().await {
            let b = item?;
            schema.get_or_insert_with(|| b.schema());
            batches.push(b);
        }
        let schema = schema.ok_or_else(|| {
            D2DError::schema(format!("查询 `{sql}` 没有返回 schema（空流）—— 服务端异常"))
        })?;
        Ok((schema, batches))
    }

    /// 目标表 schema：`SELECT * FROM t LIMIT 0` 的 FlightInfo 直接解 schema
    ///
    /// 为什么不从 DoGet 流里拿：`FlightRecordBatchStream::schema()` 要等**首次 poll
    /// 之后**才从 None 变 Some（schema 消息在流内部被消费；qmdb 方用 pyarrow 探针
    /// 确认服务端确实发了，是客户端观察时机的差异）。而 `FlightInfo.schema` 本身
    /// 就是 IPC 编码的 Schema —— 一次往返、零批数据、不依赖 poll 时序。
    /// 这也是 qmdb 协议文档 §1 的推荐做法（schema 两来源之一）。
    pub async fn table_schema(&self, qualified_table: &str) -> D2DResult<SchemaRef> {
        let sql = format!("SELECT * FROM {qualified_table} LIMIT 0");
        let info = self.flight_info(&sql).await?;
        let schema = Schema::try_from(info)
            .map_err(|e| D2DError::schema(format!("FlightInfo 的 schema 无法解析: {e}")))?;
        Ok(Arc::new(schema))
    }

    /// MIN/MAX 探测（range 分片缺边界时用；聚合在服务端做，一次往返）
    pub async fn probe_min_max(
        &self,
        qualified_table: &str,
        column: &str,
    ) -> D2DResult<(
        d2d_connector::partition::Literal,
        d2d_connector::partition::Literal,
    )> {
        use d2d_connector::partition::Literal;

        let col = crate::sql::quote_ident(column)?;
        let sql = format!("SELECT MIN({col}) AS min_v, MAX({col}) AS max_v FROM {qualified_table}");
        let (schema, batches) = self.query(&sql).await?;
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| {
                D2DError::external(
                    "qmdb",
                    format!(
                        "range 分片探测 MIN/MAX 没有返回行（表 {qualified_table} 为空）—— \
                     请显式给 partition.lower/upper，或改用 partition.mode: list"
                    ),
                )
            })?;
        if batch.num_columns() < 2 {
            return Err(D2DError::schema(format!(
                "MIN/MAX 探测返回了 {} 列（应为 2）—— 查询与契约对不上了",
                batch.num_columns()
            )));
        }
        let cell = |i: usize| -> D2DResult<Option<String>> {
            scalar_to_string(batch.column(i).as_ref(), schema.field(i).data_type())
        };
        let (Some(min_s), Some(max_s)) = (cell(0)?, cell(1)?) else {
            return Err(D2DError::external(
                "qmdb",
                format!(
                    "range 分片探测 MIN/MAX 返回空值（列 {column} 全为 NULL）—— \
                     请改用 partition.mode: list，或显式给 partition.lower/upper"
                ),
            ));
        };
        Ok((Literal::parse(&min_s), Literal::parse(&max_s)))
    }

    // ---------- 装载（三段式） ----------

    /// 三段式装载一次调用：begin_load → do_put → commit_load。
    ///
    /// 任意阶段失败都会先 abort 再返回错误 —— **不留半截数据**
    /// （staging 由服务端清理，导入会话不悬挂）。
    pub async fn load_table(
        &self,
        qualified_table: &str,
        batches: Vec<RecordBatch>,
        mode: &LoadMode,
        watermark: Option<&Watermark>,
    ) -> D2DResult<LoadStats> {
        // ① begin_load 领 load_id
        let body = serde_json::json!({
            "table": qualified_table,
            "mode": mode.as_str(),
            "preds": mode.preds(),
        });
        let load_id = {
            let mut client = self.client()?;
            let mut results = client
                .do_action(Action::new(
                    "begin_load",
                    serde_json::to_vec(&body).map_err(|e| {
                        D2DError::external("qmdb", format!("begin_load 编码失败: {e}"))
                    })?,
                ))
                .await
                .map_err(|e| self.flight_err("begin_load", e))?;
            let first = results
                .next()
                .await
                .transpose()
                .map_err(|e| self.flight_err("begin_load", e))?
                .ok_or_else(|| D2DError::external("qmdb", "begin_load 无响应"))?;
            let resp: serde_json::Value = serde_json::from_slice(&first)
                .map_err(|e| D2DError::external("qmdb", format!("begin_load 响应解析失败: {e}")))?;
            resp.get("load_id")
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| {
                    D2DError::external("qmdb", format!("begin_load 响应里没有 load_id: {resp}"))
                })?
        };

        // ② do_put 直灌批流；失败务必 abort（否则等 30 分钟空闲回收）
        if let Err(e) = self.put_batches(&load_id, batches).await {
            let _ = self.abort_load(&load_id).await;
            return Err(e);
        }

        // ③ commit_load 拿摘要（可带水位）
        let mut commit_body = serde_json::json!({ "load_id": load_id });
        if let Some(w) = watermark {
            commit_body["watermark"] = serde_json::json!({ "name": w.name, "value": w.value });
        }
        match self.action_json("commit_load", commit_body).await {
            Ok(bytes) => {
                let stats: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                    D2DError::external("qmdb", format!("commit_load 响应解析失败: {e}"))
                })?;
                let get = |k: &str| stats.get(k).and_then(|v| v.as_u64());
                Ok(LoadStats {
                    sn: get("sn").ok_or_else(|| missing(&stats, "sn"))?,
                    rows: get("rows").ok_or_else(|| missing(&stats, "rows"))?,
                    files: get("files").ok_or_else(|| missing(&stats, "files"))?,
                    bytes: get("bytes").unwrap_or(0),
                })
            }
            Err(e) => {
                let _ = self.abort_load(&load_id).await;
                Err(e)
            }
        }
    }

    /// 放弃一次装载（staging 由服务端清理）
    pub async fn abort_load(&self, load_id: &str) -> D2DResult<()> {
        self.action_json("abort_load", serde_json::json!({ "load_id": load_id }))
            .await
            .map(|_| ())
    }

    // ---------- 内部 ----------

    async fn statement_ticket(&self, sql: &str) -> D2DResult<arrow_flight::Ticket> {
        let info = self.flight_info(sql).await?;
        info.endpoint
            .first()
            .and_then(|e| e.ticket.clone())
            .ok_or_else(|| D2DError::external("qmdb", format!("查询 `{sql}` 无执行票据（空端点）")))
    }

    /// GetFlightInfo：标准 FlightSQL 的"执行并拿执行计划"一步
    async fn flight_info(&self, sql: &str) -> D2DResult<arrow_flight::FlightInfo> {
        let cmd = CommandStatementQuery {
            query: sql.to_string(),
            transaction_id: None,
        };
        let any = Any::pack(&cmd)
            .map_err(|e| D2DError::external("qmdb", format!("命令编码失败: {e}")))?;
        let desc = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
            cmd: any.encode_to_vec().into(),
            path: vec![],
        };
        let mut client = self.client()?;
        client
            .get_flight_info(desc)
            .await
            .map_err(|e| self.flight_err(&format!("GetFlightInfo（{sql}）"), e))
    }

    /// 以 load_id 为描述符上传批流（出站按 [`LOAD_CHUNK_ROWS`] 零拷贝切片）
    async fn put_batches(&self, load_id: &str, batches: Vec<RecordBatch>) -> D2DResult<()> {
        let desc = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
            cmd: serde_json::to_vec(&serde_json::json!({ "load_id": load_id }))
                .map_err(|e| D2DError::external("qmdb", format!("load_id 编码失败: {e}")))?
                .into(),
            path: vec![],
        };
        let schema = batches.first().map(|b| b.schema()).unwrap_or_else(|| {
            // 零批装载（服务端容忍）：编码器仍要一个 schema 才能发首条 Schema 消息
            std::sync::Arc::new(arrow::datatypes::Schema::empty())
        });
        let mut chunks = Vec::with_capacity(batches.len());
        for b in batches {
            let rows = b.num_rows();
            let mut offset = 0;
            while offset < rows {
                let n = LOAD_CHUNK_ROWS.min(rows - offset);
                chunks.push(b.slice(offset, n));
                offset += n;
            }
        }
        let stream = futures::stream::iter(chunks.into_iter().map(Ok::<_, FlightError>));
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_flight_descriptor(Some(desc))
            .build(stream);
        let mut client = self.client()?;
        let mut results = client
            .do_put(encoder)
            .await
            .map_err(|e| self.flight_err("do_put（装载上传）", e))?;
        while let Some(r) = results.next().await {
            r.map_err(|e| self.flight_err("do_put（装载上传）", e))?;
        }
        Ok(())
    }

    async fn action_json(&self, action: &str, body: serde_json::Value) -> D2DResult<Vec<u8>> {
        let bytes = serde_json::to_vec(&body)
            .map_err(|e| D2DError::external("qmdb", format!("{action} 编码失败: {e}")))?;
        let mut client = self.client()?;
        let mut results = client
            .do_action(Action::new(action, bytes))
            .await
            .map_err(|e| self.flight_err(action, e))?;
        let first = results
            .next()
            .await
            .transpose()
            .map_err(|e| self.flight_err(action, e))?
            .ok_or_else(|| D2DError::external("qmdb", format!("{action} 无响应")))?;
        Ok(first.to_vec())
    }
}

fn missing(stats: &serde_json::Value, key: &str) -> D2DError {
    D2DError::external("qmdb", format!("commit_load 响应里没有 {key}: {stats}"))
}

/// Arrow 标量 → 字符串（MIN/MAX 探测的取数口；之后统一 `Literal::parse`）
///
/// 只为探测服务：覆盖分区列的现实类型（整数/浮点/字符串/日期/时间戳/布尔/十进制）。
/// 别的类型在探测场景不出现，出现即报错（不静默转字符串）。
fn scalar_to_string(
    array: &dyn arrow::array::Array,
    dt: &arrow::datatypes::DataType,
) -> D2DResult<Option<String>> {
    use arrow::array::{AsArray, Decimal128Array};
    use arrow::datatypes::{DataType::*, Int16Type, Int32Type, Int64Type, Int8Type};
    use arrow::datatypes::{Float32Type, Float64Type};
    use arrow::datatypes::{UInt16Type, UInt32Type, UInt64Type, UInt8Type};
    use chrono::NaiveDate;

    if array.is_null(0) {
        return Ok(None);
    }
    let s = match dt {
        Int8 => array.as_primitive::<Int8Type>().value(0).to_string(),
        Int16 => array.as_primitive::<Int16Type>().value(0).to_string(),
        Int32 => array.as_primitive::<Int32Type>().value(0).to_string(),
        Int64 => array.as_primitive::<Int64Type>().value(0).to_string(),
        UInt8 => array.as_primitive::<UInt8Type>().value(0).to_string(),
        UInt16 => array.as_primitive::<UInt16Type>().value(0).to_string(),
        UInt32 => array.as_primitive::<UInt32Type>().value(0).to_string(),
        UInt64 => array.as_primitive::<UInt64Type>().value(0).to_string(),
        Float32 => array.as_primitive::<Float32Type>().value(0).to_string(),
        Float64 => array.as_primitive::<Float64Type>().value(0).to_string(),
        Utf8 => array.as_string::<i32>().value(0).to_string(),
        LargeUtf8 => array.as_string::<i64>().value(0).to_string(),
        Boolean => array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| D2DError::schema("BOOLEAN 探测列的物理类型不是 BooleanArray"))?
            .value(0)
            .to_string(),
        Date32 => {
            let days = array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(0);
            let d = NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| D2DError::schema(format!("Date32 值 {days} 超出日历范围")))?;
            d.format("%Y-%m-%d").to_string()
        }
        Timestamp(unit, _) => {
            let us: i64 = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampSecondType>()
                        .value(0)
                        * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                        .value(0)
                        * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => array
                    .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                    .value(0),
                arrow::datatypes::TimeUnit::Nanosecond => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                        .value(0)
                        / 1_000
                }
            };
            // 与 `Literal::parse` 的时间戳口径对齐：`YYYY-MM-DD HH:MM:SS`
            let secs = us.div_euclid(1_000_000);
            let d =
                NaiveDate::from_num_days_from_ce_opt((secs.div_euclid(86_400) + 719_163) as i32)
                    .ok_or_else(|| D2DError::schema(format!("时间戳 {us}us 超出日历范围")))?;
            let sod = secs.rem_euclid(86_400);
            format!(
                "{} {:02}:{:02}:{:02}",
                d.format("%Y-%m-%d"),
                sod / 3600,
                (sod % 3600) / 60,
                sod % 60
            )
        }
        Decimal128(_, scale) => {
            // 与 qmdb 文档口径一致：未定点值 ÷10^s 的十进制展示
            let v = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| D2DError::schema("DECIMAL 探测列的物理类型不是 Decimal128Array"))?
                .value(0);
            let s = *scale as i32;
            if s == 0 {
                v.to_string()
            } else {
                let neg = v < 0;
                let digits = v.unsigned_abs().to_string();
                let scale = s.unsigned_abs() as usize;
                let padded = format!("{digits:0>width$}", width = scale + 1);
                let (int_part, frac_part) = padded.split_at(padded.len() - scale);
                format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac_part)
            }
        }
        other => {
            return Err(D2DError::schema(format!(
                "range 探测不支持对 {other:?} 列做 MIN/MAX —— 请显式给 partition.lower/upper"
            )))
        }
    };
    Ok(Some(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use std::sync::Arc;

    #[test]
    fn test_scalar_to_string_int_and_text() {
        let a: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![Some(42i64), None]));
        assert_eq!(
            scalar_to_string(a.as_ref(), &DataType::Int64).unwrap(),
            Some("42".to_string())
        );
        let t: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec![Some("x"), None]));
        assert_eq!(
            scalar_to_string(t.as_ref(), &DataType::Utf8).unwrap(),
            Some("x".to_string())
        );
    }

    #[test]
    fn test_scalar_to_string_first_null_is_none() {
        let a = Int64Array::from(vec![None, Some(1i64)]);
        assert_eq!(
            scalar_to_string(&a, &DataType::Int64).unwrap(),
            None,
            "探测只看第一行；第一行为 NULL 就是空值"
        );
    }

    #[test]
    fn test_scalar_to_string_date() {
        let a = Date32Array::from(vec![20_747]); // 2026-10-21
        assert_eq!(
            scalar_to_string(&a, &DataType::Date32).unwrap(),
            Some("2026-10-21".to_string())
        );
    }

    #[test]
    fn test_scalar_to_string_timestamp() {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::TimeUnit;
        // 2026-10-21 08:30:00 UTC = 20747 天 * 86400 + 30600 秒
        let a = TimestampMicrosecondArray::from(vec![1_792_571_400_000_000i64]);
        assert_eq!(
            scalar_to_string(&a, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            Some("2026-10-21 08:30:00".to_string())
        );
    }

    #[test]
    fn test_scalar_to_string_decimal() {
        // 123456 / scale 2 → 1234.56
        let a = Decimal128Array::from(vec![123_456i128])
            .with_precision_and_scale(9, 2)
            .unwrap();
        assert_eq!(
            scalar_to_string(&a, &DataType::Decimal128(9, 2)).unwrap(),
            Some("1234.56".to_string())
        );
        // 负数与补零：-5 / scale 2 → -0.05
        let b = Decimal128Array::from(vec![-5i128])
            .with_precision_and_scale(9, 2)
            .unwrap();
        assert_eq!(
            scalar_to_string(&b, &DataType::Decimal128(9, 2)).unwrap(),
            Some("-0.05".to_string())
        );
    }

    #[test]
    fn test_unsupported_probe_type_is_error() {
        let a = arrow::array::BinaryArray::from(vec![b"x".as_ref()]);
        let e = scalar_to_string(&a, &DataType::Binary)
            .unwrap_err()
            .to_string();
        assert!(e.contains("MIN/MAX"), "{}", e);
    }
}
