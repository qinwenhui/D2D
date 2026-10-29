//! qmdb **Flight SQL 数据面客户端**（arrow 60 原生实现）
//!
//! 协议（qmdb 方协议文档为准，参考实现 `tests/qmdb_flight.py` / `crates/qmdb-client`）：
//!
//! ```text
//! 查询： GetFlightInfo(Any(CommandStatementQuery)) → endpoints[0].ticket
//!        DoGet(ticket) → FlightData 流（首条 IPC Schema，后续 RecordBatch）
//! 装载： DoAction("begin_load", {table, mode, preds, format?}) → {load_id}
//!        DoPut(desc{cmd: {"load_id"}}, <FlightData 流>)
//!        DoAction("commit_load", {load_id})             → {sn, rows, files, bytes}
//!        失败/放弃： DoAction("abort_load", {load_id})  → staging 自动清理
//! ```
//!
//! 装载有两种载荷（协议 §2）：缺省 `arrow_ipc`（批流，[`Self::load_table`]）与
//! **物理载荷** `csv`/`parquet`（[`Self::load_bytes`]，协议 §2.1）——后者把文件字节
//! 原样切块塞进 `FlightData.data_body`，服务端 staging 落盘后整文件解码，
//! 客户端 CPU 趋零；一次 `begin_load` 会话对应**一个文件**（多文件开多会话）。
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
    ///
    /// 连上后顺手探一次服务端版本（`SELECT version()`，best-effort）：
    /// qmdb 的线协议与服务端配套演进，报障时"客户端协议面 × 服务端版本"
    /// 是第一个要对的口径。探测失败只 debug，不影响连接可用性。
    pub async fn connect(conn: &QmdbConnection) -> D2DResult<Self> {
        let channel = conn.channel().await?;
        let me = Self {
            channel,
            user: conn.user.clone(),
            password: conn.password.clone(),
            addr: conn.addr.clone(),
        };
        if let Ok((_, batches)) = me.query("SELECT version()").await {
            let version = batches
                .first()
                .filter(|b| b.num_rows() > 0)
                .and_then(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                })
                .map(|a| a.value(0).to_string());
            match version {
                Some(v) => {
                    tracing::info!(target: "d2d_qmdb", server = %me.addr, "qmdb 服务端版本：{v}")
                }
                None => tracing::debug!(target: "d2d_qmdb", "SELECT version() 没有返回行"),
            }
        }
        Ok(me)
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
            scalar_to_string(batch.column(i).as_ref(), schema.field(i).data_type(), 0)
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

    /// 拉取表的分区值清单（`SHOW PARTITIONS FROM db.t`，qmdb 2026-10-23 新增）
    ///
    /// 返回 `Ok(None)` 表示"这条路不通，走 MIN/MAX 回退"：语句在老服务端不存在、
    /// 表没有分区、或请求的列不是分区列。真错误（网络/认证）原样上抛。
    pub async fn probe_partition_values(
        &self,
        qualified_table: &str,
        column: &str,
    ) -> D2DResult<Option<Vec<d2d_connector::partition::Literal>>> {
        let sql = format!("SHOW PARTITIONS FROM {qualified_table}");
        let (schema, batches) = match self.query(&sql).await {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(
                    target: "d2d_qmdb",
                    table = %qualified_table,
                    error = %e,
                    "SHOW PARTITIONS 不可用，range 分片回退 MIN/MAX 探测"
                );
                return Ok(None);
            }
        };
        parse_partition_values(&schema, &batches, column)
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
        let load_id = self.begin_load(qualified_table, mode, None).await?;

        // ② do_put 直灌批流；失败务必 abort（否则等 30 分钟空闲回收）
        if let Err(e) = self.put_batches(&load_id, batches).await {
            let _ = self.abort_load(&load_id).await;
            return Err(e);
        }

        self.commit_load(&load_id, watermark).await
    }

    /// 物理载荷装载（协议 §2.1）：begin_load(format) → do_put(文件字节) → commit_load。
    ///
    /// `wire` 是 `begin_load.format`（`"csv"` / `"parquet"`）；`payload` 是**一个完整
    /// 文件**的字节 —— 协议约定一次会话对应一个文件，多文件开多会话（本仓的
    /// 物理会话在 `sink/physical.rs` 里按块开会话）。
    ///
    /// 服务端 staging 落盘后整文件解码，再走与 arrow_ipc 完全相同的 coerce 通道
    /// （按列名对齐 + 安全强转 + 动态分区路由），客户端 CPU 趋零。
    pub async fn load_bytes(
        &self,
        qualified_table: &str,
        wire: &str,
        payload: Vec<u8>,
        mode: &LoadMode,
        watermark: Option<&Watermark>,
    ) -> D2DResult<LoadStats> {
        let load_id = self.begin_load(qualified_table, mode, Some(wire)).await?;
        if let Err(e) = self.put_raw(&load_id, payload).await {
            let _ = self.abort_load(&load_id).await;
            return Err(e);
        }
        self.commit_load(&load_id, watermark).await
    }

    /// begin_load 领 `load_id`；`format` 为 `None` = 缺省 arrow_ipc
    async fn begin_load(
        &self,
        qualified_table: &str,
        mode: &LoadMode,
        format: Option<&str>,
    ) -> D2DResult<String> {
        let mut body = serde_json::json!({
            "table": qualified_table,
            "mode": mode.as_str(),
            "preds": mode.preds(),
        });
        if let Some(f) = format {
            body["format"] = serde_json::json!(f);
        }
        let mut client = self.client()?;
        let mut results = client
            .do_action(Action::new(
                "begin_load",
                serde_json::to_vec(&body)
                    .map_err(|e| D2DError::external("qmdb", format!("begin_load 编码失败: {e}")))?,
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
            })
    }

    /// commit_load 拿摘要（可带水位）；失败先 abort
    async fn commit_load(
        &self,
        load_id: &str,
        watermark: Option<&Watermark>,
    ) -> D2DResult<LoadStats> {
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
                let _ = self.abort_load(load_id).await;
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
        // 服务端并发查询上限（缺省 32）现在是"info 即咬合"：第 33 路 GetFlightInfo
        // 直接被拒（p50 2.2ms 的干净拒绝）。读是幂等的 —— 指数退避后重新排队，
        // 突发并发下自动等位而不是把任务打失败。
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.flight_info(sql).await {
                Ok(info) => {
                    return info
                        .endpoint
                        .first()
                        .and_then(|e| e.ticket.clone())
                        .ok_or_else(|| {
                            D2DError::external("qmdb", format!("查询 `{sql}` 无执行票据（空端点）"))
                        });
                }
                Err(e) if attempt < RATE_LIMIT_MAX_ATTEMPTS && is_rate_limited(&e) => {
                    let wait = RATE_LIMIT_BACKOFF_BASE_MS * (1 << (attempt - 1));
                    tracing::warn!(
                        attempt,
                        wait_ms = wait,
                        sql = %sql,
                        error = %e,
                        "qmdb: 触发服务端并发查询上限，退避重试"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                }
                Err(e) => return Err(e),
            }
        }
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

    /// 以 load_id 为描述符上传**原始文件字节**（物理载荷，协议 §2.1）
    ///
    /// 线形态：首条 `FlightData` 带 descriptor + **空 Schema 的 IPC 消息**占位
    /// （服务端流解析强制要求"首批前有 Schema 消息"，csv 模式不用它对列 ——
    /// 列对齐走文件自带表头；实测不带会被拒"导入流缺少前置 schema 消息"，
    /// 协议文档说的"IPC Schema 消息若有会被跳过"应更正为"必须有"）。
    /// 后续每条 `data_header` 空、`data_body` = 文件字节切块（建议 4~8MB）。
    ///
    /// 切块用 [`bytes::Bytes::slice`] —— 零拷贝视图，`payload` 整体在内存里**只有一份**。
    async fn put_raw(&self, load_id: &str, payload: Vec<u8>) -> D2DResult<()> {
        // 协议建议 4~8MB/块，但服务端 tonic 解码上限还是缺省 4MB：4MB 块 + protobuf
        // 帧开销（~6B）= 4,194,310 > 4,194,304，实测被拒"decoded message length too
        // large"。降到 3.5MB 留足余量（qmdb 方调大服务端上限后再回 4~8MB）。
        const CHUNK_BYTES: usize = 3584 * 1024;
        let desc = FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
            cmd: serde_json::to_vec(&serde_json::json!({ "load_id": load_id }))
                .map_err(|e| D2DError::external("qmdb", format!("load_id 编码失败: {e}")))?
                .into(),
            path: vec![],
        };
        // 空 Schema 的 IPC Schema 消息（占位，见模块注释）
        let placeholder_schema: arrow_flight::FlightData = {
            use arrow::ipc::writer::IpcWriteOptions;
            use arrow_flight::SchemaAsIpc;
            SchemaAsIpc::new(&Schema::empty(), &IpcWriteOptions::default()).into()
        };
        let placeholder_header = placeholder_schema.data_header;
        let payload = bytes::Bytes::from(payload);
        let mut chunks = vec![arrow_flight::FlightData {
            flight_descriptor: Some(desc),
            data_header: placeholder_header,
            data_body: bytes::Bytes::new(),
            app_metadata: bytes::Bytes::new(),
        }];
        let mut offset = 0usize;
        while offset < payload.len() {
            let end = CHUNK_BYTES.min(payload.len() - offset) + offset;
            chunks.push(arrow_flight::FlightData {
                flight_descriptor: None,
                data_header: bytes::Bytes::new(),
                data_body: payload.slice(offset..end),
                app_metadata: bytes::Bytes::new(),
            });
            offset = end;
        }
        let stream = futures::stream::iter(chunks.into_iter().map(Ok::<_, FlightError>));
        let mut client = self.client()?;
        let mut results = client
            .do_put(stream)
            .await
            .map_err(|e| self.flight_err("do_put（物理载荷上传）", e))?;
        while let Some(r) = results.next().await {
            r.map_err(|e| self.flight_err("do_put（物理载荷上传）", e))?;
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

/// 读侧限流退避：最多 5 次重试，200ms 起指数退避（0.2/0.4/0.8/1.6/3.2s，累计 ~6.2s）
const RATE_LIMIT_MAX_ATTEMPTS: u32 = 5;
const RATE_LIMIT_BACKOFF_BASE_MS: u64 = 200;

/// 服务端并发护栏（`ResourceExhausted` 或中文原文"并发查询上限…"）—— 读幂等，可退避重试
fn is_rate_limited(e: &D2DError) -> bool {
    let msg = e.to_string();
    msg.contains("并发查询上限")
        || msg.contains("ResourceExhausted")
        || msg.contains("Resource exhausted")
}

/// Arrow 标量 → 字符串（探测的取数口：MIN/MAX、SHOW PARTITIONS；之后统一 `Literal::parse`）
///
/// 只为探测服务：覆盖分区列的现实类型（整数/浮点/字符串/日期/时间戳/布尔/十进制）。
/// 别的类型在探测场景不出现，出现即报错（不静默转字符串）。
pub(crate) fn scalar_to_string(
    array: &dyn arrow::array::Array,
    dt: &arrow::datatypes::DataType,
    row: usize,
) -> D2DResult<Option<String>> {
    use arrow::array::{AsArray, Decimal128Array};
    use arrow::datatypes::{DataType::*, Int16Type, Int32Type, Int64Type, Int8Type};
    use arrow::datatypes::{Float32Type, Float64Type};
    use arrow::datatypes::{UInt16Type, UInt32Type, UInt64Type, UInt8Type};
    use chrono::NaiveDate;

    if array.is_null(row) {
        return Ok(None);
    }
    let s = match dt {
        Int8 => array.as_primitive::<Int8Type>().value(row).to_string(),
        Int16 => array.as_primitive::<Int16Type>().value(row).to_string(),
        Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        UInt8 => array.as_primitive::<UInt8Type>().value(row).to_string(),
        UInt16 => array.as_primitive::<UInt16Type>().value(row).to_string(),
        UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
        UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
        Float32 => array.as_primitive::<Float32Type>().value(row).to_string(),
        Float64 => array.as_primitive::<Float64Type>().value(row).to_string(),
        Utf8 => array.as_string::<i32>().value(row).to_string(),
        LargeUtf8 => array.as_string::<i64>().value(row).to_string(),
        Boolean => array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| D2DError::schema("BOOLEAN 探测列的物理类型不是 BooleanArray"))?
            .value(row)
            .to_string(),
        Date32 => {
            let days = array
                .as_primitive::<arrow::datatypes::Date32Type>()
                .value(row);
            let d = NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| D2DError::schema(format!("Date32 值 {days} 超出日历范围")))?;
            d.format("%Y-%m-%d").to_string()
        }
        Timestamp(unit, _) => {
            let us: i64 = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampSecondType>()
                        .value(row)
                        * 1_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                        .value(row)
                        * 1_000
                }
                arrow::datatypes::TimeUnit::Microsecond => array
                    .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                    .value(row),
                arrow::datatypes::TimeUnit::Nanosecond => {
                    array
                        .as_primitive::<arrow::datatypes::TimestampNanosecondType>()
                        .value(row)
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
                .value(row);
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

/// `SHOW PARTITIONS` 结果 → 分区值切点（严格递增；`Literal` 没有 `PartialOrd`，自比）
///
/// 结果形状：分区列逐列 + `rows`/`files`（分区值已排序）。请求的列不在分区列里
/// （或没有分区）→ `Ok(None)`，调用方走 MIN/MAX 回退。
pub(crate) fn parse_partition_values(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    column: &str,
) -> D2DResult<Option<Vec<d2d_connector::partition::Literal>>> {
    use d2d_connector::partition::Literal;

    // rows/files 是统计列，不是分区列
    let is_stat = |name: &str| matches!(name.to_ascii_lowercase().as_str(), "rows" | "files");
    let col_idx = schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| !is_stat(f.name()) && f.name().eq_ignore_ascii_case(column));
    let Some((ci, _)) = col_idx else {
        return Ok(None); // 请求的列不是分区列（或表没有分区）→ 回退
    };

    let mut values: Vec<Literal> = Vec::new();
    for b in batches {
        let arr = b.column(ci).as_ref();
        let dt = schema.field(ci).data_type();
        for row in 0..b.num_rows() {
            let Some(s) = scalar_to_string(arr, dt, row)? else {
                // 分区值为 NULL 极不寻常（NULL 分区），跳过并留痕，不让它炸整个规划
                tracing::warn!(target: "d2d_qmdb", column = %column, "SHOW PARTITIONS 出现 NULL 分区值，已跳过");
                continue;
            };
            values.push(Literal::parse(&s));
        }
    }
    if values.len() < 2 {
        // 0 或 1 个分区：切点切不出区间结构，交给 MIN/MAX 或单分片
        return Ok(None);
    }

    // **去重**：分区可以是**多级**的（实测 `bench_full.t_dnl_day_24result_unit` 是
    // `(pdate, version_id)` 两级）——按 pdate 切时，同一个 pdate 会随 version_id
    // 重复出现。切点要的是**不同的值**，重复的直接去掉。
    let mut cuts: Vec<Literal> = Vec::with_capacity(values.len());
    for v in values {
        let dup = cuts
            .last()
            .map(|last| literal_equal(last, &v))
            .unwrap_or(false);
        if !dup {
            cuts.push(v);
        }
    }
    if cuts.len() < 2 {
        return Ok(None);
    }
    for w in cuts.windows(2) {
        if !literal_strictly_less(&w[0], &w[1]) {
            return Err(D2DError::schema(format!(
                "SHOW PARTITIONS 返回的分区值不是严格递增（{w:?}）—— \
                 切点乱序会造成分片重叠/空洞，拒绝规划"
            )));
        }
    }
    Ok(Some(cuts))
}

/// `Literal` 的同族相等比较（去重用）
fn literal_equal(
    a: &d2d_connector::partition::Literal,
    b: &d2d_connector::partition::Literal,
) -> bool {
    use d2d_connector::partition::Literal;
    match (a, b) {
        (Literal::Int(x), Literal::Int(y)) => x == y,
        (Literal::Float(x), Literal::Float(y)) => x == y,
        (Literal::Date(x), Literal::Date(y)) => x == y,
        (Literal::Timestamp(x), Literal::Timestamp(y)) => x == y,
        (Literal::Text(x), Literal::Text(y)) => x == y,
        _ => false,
    }
}

/// `Literal` 的同族严格小于比较（内核 `Literal` 没有 `PartialOrd`；切点校验专用）
fn literal_strictly_less(
    a: &d2d_connector::partition::Literal,
    b: &d2d_connector::partition::Literal,
) -> bool {
    use d2d_connector::partition::Literal;
    match (a, b) {
        (Literal::Int(x), Literal::Int(y)) => x < y,
        (Literal::Float(x), Literal::Float(y)) => x < y,
        (Literal::Date(x), Literal::Date(y)) => x < y,
        (Literal::Timestamp(x), Literal::Timestamp(y)) => x < y,
        (Literal::Text(x), Literal::Text(y)) => x < y,
        _ => false, // 异族不可比：交给上层"非严格递增"报错
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    /// 服务端并发咬合（"info 即咬合"，第 33 路被拒）的两种错误形态都要认；
    /// 其他错误（认证/SQL/断流）不能触发读侧重试 —— 断流重试是执行层的职责
    #[test]
    fn test_rate_limited_detection() {
        let e1 = D2DError::external(
            "qmdb",
            "GetFlightInfo（SELECT ...）: 并发查询上限(32)，已满，请稍后重试",
        );
        assert!(is_rate_limited(&e1), "{e1}");
        let e2 = D2DError::external(
            "qmdb",
            "GetFlightInfo: ResourceExhausted: concurrent query limit",
        );
        assert!(is_rate_limited(&e2), "{e2}");
        let e3 = D2DError::connection(
            "qmdb",
            "GetFlightInfo：认证失败（h:31337）—— 用户名或密码错误",
        );
        assert!(!is_rate_limited(&e3), "{e3}");
        let e4 = D2DError::external("qmdb", "读查询流失败: ConnectionReset");
        assert!(!is_rate_limited(&e4), "{e4}");
    }

    #[test]
    fn test_scalar_to_string_int_and_text() {
        let a: Arc<dyn arrow::array::Array> = Arc::new(Int64Array::from(vec![Some(42i64), None]));
        assert_eq!(
            scalar_to_string(a.as_ref(), &DataType::Int64, 0).unwrap(),
            Some("42".to_string())
        );
        let t: Arc<dyn arrow::array::Array> = Arc::new(StringArray::from(vec![Some("x"), None]));
        assert_eq!(
            scalar_to_string(t.as_ref(), &DataType::Utf8, 0).unwrap(),
            Some("x".to_string())
        );
    }

    #[test]
    fn test_scalar_to_string_first_null_is_none() {
        let a = Int64Array::from(vec![None, Some(1i64)]);
        assert_eq!(
            scalar_to_string(&a, &DataType::Int64, 0).unwrap(),
            None,
            "探测只看第一行；第一行为 NULL 就是空值"
        );
    }

    #[test]
    fn test_scalar_to_string_date() {
        let a = Date32Array::from(vec![20_747]); // 2026-10-21
        assert_eq!(
            scalar_to_string(&a, &DataType::Date32, 0).unwrap(),
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
            scalar_to_string(&a, &DataType::Timestamp(TimeUnit::Microsecond, None), 0).unwrap(),
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
            scalar_to_string(&a, &DataType::Decimal128(9, 2), 0).unwrap(),
            Some("1234.56".to_string())
        );
        // 负数与补零：-5 / scale 2 → -0.05
        let b = Decimal128Array::from(vec![-5i128])
            .with_precision_and_scale(9, 2)
            .unwrap();
        assert_eq!(
            scalar_to_string(&b, &DataType::Decimal128(9, 2), 0).unwrap(),
            Some("-0.05".to_string())
        );
    }

    // ───────── SHOW PARTITIONS 解析 ─────────

    use d2d_connector::partition::Literal;

    /// 构造 SHOW PARTITIONS 结果：分区列 + rows/files（值已排序，与服务端一致）
    fn partitions_batch(dates: &[Option<i32>]) -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pdate", DataType::Date32, true),
            Field::new("rows", DataType::Int64, true),
            Field::new("files", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Date32Array::from(dates.to_vec())),
                Arc::new(Int64Array::from(vec![1i64; dates.len()])),
                Arc::new(Int64Array::from(vec![1i64; dates.len()])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    #[test]
    fn test_parse_partition_values_date_cuts() {
        let (schema, batches) = partitions_batch(&[Some(20_747), Some(20_748), Some(20_749)]);
        let cuts = parse_partition_values(&schema, &batches, "pdate")
            .unwrap()
            .unwrap();
        assert_eq!(
            cuts,
            vec![
                Literal::parse("2026-10-21"),
                Literal::parse("2026-10-22"),
                Literal::parse("2026-10-23"),
            ],
            "分区值渲染成日期字面量当切点"
        );
    }

    #[test]
    fn test_parse_partition_values_requested_column_not_partition() {
        let (schema, batches) = partitions_batch(&[Some(20_747), Some(20_748)]);
        // 请求的列不是分区列 → None，调用方走 MIN/MAX 回退
        assert!(parse_partition_values(&schema, &batches, "id")
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_parse_partition_values_single_partition_falls_back() {
        let (schema, batches) = partitions_batch(&[Some(20_747)]);
        // 1 个分区切不出区间结构 → None 回退
        assert!(parse_partition_values(&schema, &batches, "pdate")
            .unwrap()
            .is_none());
        // 0 个分区同理
        let (schema, batches) = partitions_batch(&[]);
        assert!(parse_partition_values(&schema, &batches, "pdate")
            .unwrap()
            .is_none());
    }

    /// **回归：两级分区（pdate, version_id）按 pdate 切** —— pdate 会重复出现
    ///
    /// 实测 `bench_full.t_dnl_day_24result_unit` 是 `(pdate, version_id)` 两级分区：
    /// SHOW PARTITIONS 对每个 (pdate, version_id) 组合返一行，于是 pdate 列的值
    /// **重复**。不去重的话，严格递增校验会误报"分区值乱序"→ 整个 range 规划失败。
    #[test]
    fn test_parse_partition_values_dedupes_composite_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pdate", DataType::Utf8, true),
            Field::new("version_id", DataType::Utf8, true),
            Field::new("rows", DataType::Int64, true),
            Field::new("files", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "2025-10-01",
                    "2025-10-01",
                    "2025-10-01",
                    "2025-10-02",
                    "2025-10-02",
                ])),
                Arc::new(StringArray::from(vec![
                    "10_20251001_1",
                    "11_20251001_1",
                    "12_20251001_1",
                    "1_20251002_1",
                    "2_20251002_1",
                ])),
                Arc::new(Int64Array::from(vec![1i64; 5])),
                Arc::new(Int64Array::from(vec![1i64; 5])),
            ],
        )
        .unwrap();
        let cuts = parse_partition_values(&schema, &[batch], "pdate")
            .unwrap()
            .unwrap();
        assert_eq!(
            cuts,
            vec![Literal::parse("2025-10-01"), Literal::parse("2025-10-02")],
            "重复的 pdate 必须去重，只留不同的分区值当切点"
        );
    }

    #[test]
    fn test_parse_partition_values_unsorted_is_error() {
        // 服务端承诺分区值排序；乱序说明假设破了，必须拒绝（切点乱序 = 分片重叠）
        let (schema, batches) = partitions_batch(&[Some(20_749), Some(20_747)]);
        let e = parse_partition_values(&schema, &batches, "pdate")
            .unwrap_err()
            .to_string();
        assert!(e.contains("严格递增"), "{}", e);
    }

    #[test]
    fn test_unsupported_probe_type_is_error() {
        let a = arrow::array::BinaryArray::from(vec![b"x".as_ref()]);
        let e = scalar_to_string(&a, &DataType::Binary, 0)
            .unwrap_err()
            .to_string();
        assert!(e.contains("MIN/MAX"), "{}", e);
    }
}
