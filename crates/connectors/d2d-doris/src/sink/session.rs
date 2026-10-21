use arrow::array::{Array, RecordBatch};
use async_trait::async_trait;
use d2d_connector::{MemMeter, SinkSession, WriteConfig};
use d2d_core::{D2DError, D2DResult};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::{DorisConnection, StreamLoadConfig};

/// CSV 载荷里代表 NULL 的字面量
///
/// Doris Stream Load 的 `null_value` 缺省值就是它。载荷里写它、
/// 请求头里也显式声明它（见 `do_stream_load`），两处引用这**同一个常量**。
const NULL_LITERAL: &str = "\\N";

/// Doris Stream Load 写入会话（**逻辑面**）
///
/// # 一条会话写一份，不攒批
///
/// 攒批住在内核（`flow.sink.batch_bytes`：内核从 `sink_pool` 攒够一份才交给连接器）。
/// 所以这里没有缓冲区、没有 `should_flush`、也没有"空闲多久强制落盘" ——
/// 一次 [`SinkSession::write_batch`] 就是一次 Stream Load。
///
/// 这条分工的实际收益很具体：内部攒批时，"攒着的那点内存"不在任何池的账上；
/// 而"源端停下来时最后一批一直吊着"要靠一个空闲超时去救，那个超时又必须和
/// `pop()` 赛跑才有效（曾经就是这么写的）。攒批搬到池上，两件事一起没了。
pub struct DorisSinkSession {
    connection: DorisConnection,
    config: StreamLoadConfig,
    write_config: WriteConfig,
    /// 占用申报尺子（连接器给的那把，缺省不申报）
    meter: Option<Arc<MemMeter>>,
    /// 复用的 HTTP 客户端（**连接器级的那一个**，见 `DorisSinkConnector::client`）
    client: reqwest::Client,

    // 当前 Stream Load Label（幂等标识）
    current_label: Option<String>,

    // 累计统计
    total_written_rows: u64,
    total_written_bytes: u64,
}

impl DorisSinkSession {
    pub fn new(
        connection: DorisConnection,
        config: StreamLoadConfig,
        write_config: WriteConfig,
        meter: Option<Arc<MemMeter>>,
        client: reqwest::Client,
    ) -> D2DResult<Self> {
        Ok(Self {
            connection,
            config,
            write_config,
            meter,
            client,
            current_label: None,
            total_written_rows: 0,
            total_written_bytes: 0,
        })
    }

    /// 从 `start` 行起攒**一个**请求体，返回 (载荷, 本块行数)
    ///
    /// # 两条不变量
    ///
    /// 1. **切点落在行边界**：一行要么整行在块里，要么整行留给下一块 —— 劈成两半的话
    ///    两个块各自看都是"半行"，Doris 报的会是脏数据，看不出真正的原因。
    /// 2. **`载荷.len() <= 上限`**（单行自身超限那一例除外，见下）。
    ///
    /// ⚠️ 第 2 条第一版**写错了**：当时是"先追加、再看 `out.len() >= lim` 就停"，
    /// 于是每块都可能**超出上限一点点**（最后一行的长度）。实测被 Doris 挡下：
    /// `json body size 104858428 exceed BE's conf streaming_load_json_max_mb 104857600`
    /// —— 超了 828 字节，就一行的事。
    /// 现在的写法是"追加后若越界就**回退这一行**并收尾"，所以上限是硬的。
    ///
    /// 单行自身就超过上限时**报错**而不是硬发：那一行无论怎么切都塞不进一个请求，
    /// 发出去只会换回一个更难懂的服务端错误。出路是调大 BE 的上限或调大这里的上限。
    fn build_chunk(
        &self,
        batch: &RecordBatch,
        start: usize,
        keys: &[String],
    ) -> D2DResult<(Vec<u8>, usize)> {
        let total = batch.num_rows();
        let limit = self.config.max_request_bytes;
        let is_csv = self.config.format == "csv";
        let (sep, line) = (
            self.config.column_separator.as_bytes(),
            self.config.line_delimiter.as_bytes(),
        );

        let mut out = Vec::new();
        let mut n = 0usize;
        while start + n < total {
            let before = out.len();
            if is_csv {
                append_csv_row(&mut out, batch, start + n, sep, line)?;
            } else {
                append_json_row(&mut out, batch, start + n, keys)?;
            }
            n += 1;

            if let Some(lim) = limit {
                if out.len() > lim {
                    if n == 1 {
                        return Err(D2DError::sink_write(
                            "doris",
                            format!(
                                "第 {} 行单独一行就有 {} 字节，超过单请求上限 {} 字节（{} 格式）—— \
                                 切不动了。请调大 BE 的 streaming_load_json_max_mb / \
                                 streaming_load_max_mb，或调大 \
                                 sink.properties.stream_load.max_request_bytes",
                                start + 1,
                                out.len(),
                                lim,
                                self.config.format
                            ),
                        ));
                    }
                    // 这一行放不下 —— 退回去，留给下一块
                    out.truncate(before);
                    n -= 1;
                    break;
                }
            }
        }
        Ok((out, n))
    }

    /// 执行 Stream Load HTTP 请求
    ///
    /// 实现要点（均为实测踩出来的坑，勿想当然改回去）：
    ///
    /// 1. **必须显式发 `columns` 头**。源端 Arrow schema 的列序是「契约声明序」，
    ///    而 Doris 建表 DDL 会把 UNIQUE KEY 列提到最前、其余列按建表语句序，
    ///    两者通常不一致。不发 columns 头时 Doris 按**位置**映射 → 整体错位
    ///    （varchar 灌进 datetime 列），实测直接 load 失败或写入错列。
    /// 2. **不能发 `line_delimiter` 头**。值是裸换行（`\n`），HTTP 头值不允许含
    ///    控制字符 → 客户端直接报错。Doris 侧默认行分隔符就是 `\n`，不设即可。
    /// 3. **不支持 gzip 压缩体**。`Content-Encoding` / `compress_type` /
    ///    `compression` 三种写法实测全部失败（Doris 把 gzip 二进制按明文解析，
    ///    报 "Invalid file encoding"），只能发明文。
    /// 4. **必须校验响应 Status**。HTTP 200 也可能是 `Status: Fail`（例如 strict_mode
    ///    过滤、列数不匹配），原实现无条件 `Ok(())` 会让失败被当成成功。
    /// 5. `column_separator` 必须显式设 —— Doris 默认是制表符，不是 `|`。
    async fn do_stream_load(
        &self,
        data: Vec<u8>,
        label: &str,
        columns: Option<&str>,
    ) -> D2DResult<()> {
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            self.connection.next_load_url(),
            self.connection.database,
            self.connection.table
        );

        // 先把长度取出来：`body` 要拿走这块内存的所有权（`.body(data)`），
        // 之后 `data` 就不在了。**不要**为了留着 `data` 而写 `.body(data.clone())` ——
        // 那是把整份载荷再拷一遍（64 MB 的批就是白占 64 MB），
        // 而调用方在重试循环里已经拷过一份了，两处一起就是三份在账。
        let n_bytes = data.len();

        debug!(
            url = %url,
            label = %label,
            bytes = n_bytes,
            "sending stream load request"
        );

        // 连接器级的客户端：连接全程 Keep-Alive。曾经这里是 `Client::new()` ——
        // 每次写入都丢掉连接池，每次 body 都要重新握手。
        // 与格式相关的头：**CSV 与 JSON 各发各的**。
        //
        // 不能图省事一律发出去：`column_separator` / `null_value` 对 JSON 是无意义的
        // （JSON 有自己的语法，分隔符与空值都不是"一个字符串"能表达的东西），
        // 发了至少是误导 —— 以后有人看到"JSON 也能配 null_value"就会去调它。
        let is_csv = self.config.format == "csv";
        let mut req = self
            .client
            .put(&url)
            .basic_auth(&self.connection.username, Some(&self.connection.password))
            .header("Expect", "100-continue")
            .header("Content-Type", "text/plain; charset=UTF-8")
            .header("label", label)
            .header("max_filter_ratio", self.config.max_filter_ratio.to_string())
            .header(
                "strict_mode",
                if self.config.strict_mode {
                    "true"
                } else {
                    "false"
                },
            );
        if is_csv {
            if let Some(c) = columns {
                req = req.header("columns", c);
            }
            req = req
                .header("column_separator", self.config.column_separator.as_str())
                // 显式声明 NULL 字面量，别靠"Doris 默认就是 \N"这条隐含约定 ——
                // 载荷里写的就是它（见 serialize_batch_csv），两处必须同源
                .header("null_value", NULL_LITERAL);
        } else {
            // JSON 形态由 `format: json` 声明。**一行一个对象**，不用 `strip_outer_array`
            // （那是数组形态才要的；实测裸发一行一个对象即可）。
            req = req.header("format", "json");
        }
        let response = req
            .body(data)
            .send()
            .await
            .map_err(|e| D2DError::sink_write("doris", e))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() || !body.contains("\"Status\": \"Success\"") {
            return Err(D2DError::sink_write(
                "doris",
                format!("HTTP {} :: {}", status, body),
            ));
        }

        info!(label = %label, bytes = n_bytes, status = %status, "stream load success");
        Ok(())
    }

    /// 生成唯一的 Stream Load Label
    fn generate_label(&self) -> String {
        format!(
            "d2d_{}_{}",
            self.connection.table,
            Uuid::new_v4().to_string().replace('-', "")
        )
    }
}

#[async_trait]
impl SinkSession for DorisSinkSession {
    /// **写一批 = 发一次 Stream Load**（内核已经攒好了一份，这里不攒）
    async fn write_batch(&mut self, batch: RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }

        // Doris `columns` 头：按 Arrow schema 实际列序声明，让 Doris 按列名映射。
        // 缺了它 Doris 按位置映射，而源列序与 Doris 表列序（UNIQUE KEY 列在最前）
        // 通常不同 → 写入错列。详见 do_stream_load 注释第 1 条。
        //
        // **`json` 格式不发这个头**：JSON 按**键名**对应列，位置压根不参与，
        // 这个头在那边是空转（实测把列序写反了也照样对得上）。少一个头就少一处能配错。
        let columns: Option<String> = if self.config.format == "csv" {
            Some(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        } else {
            None
        };

        // 键名整批复用（json 用；csv 那边不用，cost 可忽略）
        let keys = json_keys(&batch);
        let max_retries = self.write_config.max_retries.unwrap_or(3).max(1);
        let backoff_ms = self.write_config.retry_backoff_ms.unwrap_or(1000);

        // ── 一批可能拆成几次 Stream Load ──
        //
        // 拆的是**目标端的物理约束**，不是攒批语义：内核给的 `batch_bytes` 是上界，
        // 连接器在上界之内按"单请求能带多少"再细分（设计说明）。
        // 逐块构造、当场发出去，**不把整批的载荷先攒在内存里** —— 那正是
        // "一次发多少"搬回内核要消掉的那种账外占用。
        let mut sent_rows = 0usize;
        while sent_rows < rows {
            let (data, chunk_rows) = self.build_chunk(&batch, sent_rows, keys.as_slice())?;

            // ── 如实申报这份请求体 ──
            //
            // 它是一份**新分配的明文载荷**（不是池里那些 Arrow 批次），发送时所有权要交给
            // `reqwest` —— 于是它在任何池的账上都不存在。
            // 租约覆盖整个重试循环：重试期间重传的那份也是活的，报少了就还是假账。
            let _body_lease = self.meter.as_ref().map(|m| m.claim(data.len()));

            let label = self.generate_label();
            self.current_label = Some(label.clone());
            let mut last_error = None;

            for attempt in 0..max_retries {
                match self
                    .do_stream_load(data.clone(), &label, columns.as_deref())
                    .await
                {
                    Ok(()) => {
                        self.total_written_rows += chunk_rows as u64;
                        self.total_written_bytes += data.len() as u64;
                        debug!(
                            label = %label,
                            rows = chunk_rows,
                            bytes = data.len(),
                            attempt = attempt + 1,
                            "stream load success"
                        );
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            label = %label,
                            attempt = attempt + 1,
                            max_retries = max_retries,
                            error = %e,
                            "stream load failed, retrying"
                        );
                        last_error = Some(e);
                        if attempt + 1 < max_retries {
                            tokio::time::sleep(Duration::from_millis(
                                backoff_ms * (attempt as u64 + 1),
                            ))
                            .await;
                        }
                    }
                }
            }
            if let Some(e) = last_error {
                // 已经发出去的块是**已提交的事务**，回不去 —— 如实说明，
                // 别让调用方以为"整批都没写"
                return Err(D2DError::sink_write(
                    "doris",
                    format!(
                        "第 {} 批（本批共 {} 行）写入失败，此前已成功写入 {} 行：{}",
                        sent_rows / chunk_rows.max(1) + 1,
                        rows,
                        sent_rows,
                        e
                    ),
                ));
            }
            sent_rows += chunk_rows;
        }

        if sent_rows != rows {
            return Err(D2DError::internal(format!(
                "doris 写入行数对不上：批里 {} 行，只送出去 {} 行",
                rows, sent_rows
            )));
        }
        Ok(rows)
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // 没有剩余缓冲要落盘 —— 每次 `write_batch` 都当场发出去了。
        // 保留这个方法是因为"会话结束"这个语义仍然存在（日后要发 cancel、
        // 要确认事务状态都挂在这里），而不是因为在等一个 flush。
        info!(
            total_rows = self.total_written_rows,
            total_bytes = self.total_written_bytes,
            "doris session committed"
        );
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        // 没有缓冲要清 —— 已经发出去的 Stream Load 是一个已经提交的事务，
        // 这里无事可做（"向 Doris 发 cancel"见 后续工作）
        warn!(label = ?self.current_label, "doris session aborted");
        Ok(())
    }
}

/// 将 Arrow Array 的某个元素转为字符串（CSV 序列化用）
fn array_value_to_string(col: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::util::display::array_value_to_string as arrow_to_string;
    arrow_to_string(col, row).unwrap_or_default()
}

/// 批次 → **JSON** 载荷（**一行一个对象**，不是数组）
///
/// # 为什么一行一个对象而不是 `JSON_ARRAY`
///
/// Doris 两种都能吃（数组形态要配 `strip_outer_array: true`）。选行式是因为它
/// **天然可流式**：攒批交进来多大都不用在内存里再拼一个巨大的数组括号；
/// 数组形态还得额外开一个 `strip_outer_array` 头，多一处约定就多一处能配错的地方。
/// 实测：裸发一行一个对象、不带任何额外头（`format: json` 除外）即可。
///
/// # 与 CSV 的两处结构性差别（都是 JSON 更省事的地方）
///
/// | | CSV | JSON |
/// |---|---|---|
/// | NULL | 靠 `\N` 这个**字面量**，值本身就等于 `\N` 时撞车 | 原生 `null`，**不可能撞车** |
/// | 列对应 | 按**位置**（所以必须发 `columns` 头纠偏） | 按**键名**（见下） |
///
/// 所以这里**不需要** CSV 那套"值等于 `\N` 就大声失败"的守卫 —— 那是 CSV 的
/// 表达能力缺陷，不是数据的缺陷。
///
/// # 列名即键名，且**大小写敏感**
///
/// 实测（192.0.2.10:18040）：`{"ID":12}` 这样的键**匹配不上**列 `id`，
/// 那一行报 `There is no column matching jsonpaths` 被拒。
/// 缺键 → 该列落 NULL；多余键 → 忽略。
/// 需要改名请在**引擎侧**用 `transform.op: column_map`，别指望协议层能对上。
#[cfg(test)]
fn serialize_batch_json(batch: &RecordBatch) -> D2DResult<Vec<u8>> {
    let keys = json_keys(batch);
    // 粗估：一行约 32 字节打底 + 内容，内容按 Arrow 里的实际大小估
    let mut out: Vec<u8> =
        Vec::with_capacity(batch.get_array_memory_size() + batch.num_rows() * 16);
    for row in 0..batch.num_rows() {
        append_json_row(&mut out, batch, row, &keys)?;
    }
    Ok(out)
}

/// 键名（转义后、含 `"名":`）—— 整批复用，别每行每列重算
fn json_keys(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let mut k = Vec::with_capacity(f.name().len() + 3);
            k.push(b'"');
            escape_json_str_into(&mut k, f.name());
            k.extend_from_slice(b"\":");
            // 全部来自合法 &str 的 UTF-8 字节，转义不引入非法序列
            String::from_utf8(k).expect("键名转义后仍是 UTF-8")
        })
        .collect()
}

/// 把**一行** JSON 对象追加到 `out`（含末尾换行）
fn append_json_row(
    out: &mut Vec<u8>,
    batch: &RecordBatch,
    row: usize,
    keys: &[String],
) -> D2DResult<()> {
    let schema = batch.schema();
    out.push(b'{');
    for (col, key) in keys.iter().enumerate() {
        if col > 0 {
            out.push(b',');
        }
        out.extend_from_slice(key.as_bytes());
        let arr = batch.column(col);
        if arr.is_null(row) {
            out.extend_from_slice(b"null");
        } else {
            let f = schema.field(col);
            write_json_value(out, arr, row, f.name(), f.data_type())?;
        }
    }
    out.extend_from_slice(b"}\n");
    Ok(())
}

/// 一个单元格 → JSON 值
fn write_json_value(
    out: &mut Vec<u8>,
    arr: &arrow::array::ArrayRef,
    row: usize,
    col: &str,
    dt: &arrow::datatypes::DataType,
) -> D2DResult<()> {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use std::io::Write;

    macro_rules! prim {
        ($t:ty) => {
            write!(
                out,
                "{}",
                arr.as_any().downcast_ref::<$t>().unwrap().value(row)
            )
            .map_err(|e| D2DError::internal(format!("写 JSON 失败: {}", e)))?
        };
    }

    match dt {
        DataType::Null => out.extend_from_slice(b"null"),
        DataType::Boolean => {
            let v = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            out.extend_from_slice(if v { b"true" } else { b"false" });
        }
        DataType::Int8 => prim!(Int8Array),
        DataType::Int16 => prim!(Int16Array),
        DataType::Int32 => prim!(Int32Array),
        DataType::Int64 => prim!(Int64Array),
        DataType::UInt8 => prim!(UInt8Array),
        DataType::UInt16 => prim!(UInt16Array),
        DataType::UInt32 => prim!(UInt32Array),
        DataType::UInt64 => prim!(UInt64Array),
        DataType::Float32 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row);
            // NaN / ±Inf 不是合法 JSON。**报错，不写成 null** ——
            // 悄悄变 null 就是"跑完了、值没了"
            if !v.is_finite() {
                return Err(non_finite_err(col, v.to_string()));
            }
            write!(out, "{:?}", v)
                .map_err(|e| D2DError::internal(format!("写 JSON 失败: {}", e)))?
        }
        DataType::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            if !v.is_finite() {
                return Err(non_finite_err(col, v.to_string()));
            }
            // `{:?}` 而不是 `{}`：前者给**能往返的最短表示**（大数走 `1e308` 这种指数形态），
            // 后者会把 1e308 展开成 309 个字符的十进制
            write!(out, "{:?}", v)
                .map_err(|e| D2DError::internal(format!("写 JSON 失败: {}", e)))?
        }
        DataType::Decimal128(p, s) => {
            let v = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(row);
            // 按定点十进制原样写（不写成浮点：38 位精度走 f64 就没了）。
            // 实测 Doris 能原样收下 38 位的 JSON 数字。
            out.extend_from_slice(scaled_i128_to_decimal_string(v, *s).as_bytes());
            let _ = p;
        }
        // 字符串一律当 **JSON 字符串**（转义后）发。
        //
        // 这一条同时覆盖了两种目标列，实测两头都对：
        //   - 目标列是 `varchar` → 原样存下这段文本；
        //   - 目标列是 **`json`** → Doris 会把**字符串内容当 JSON 解析**后存成对象
        //     （发 `"{\"k\":1}"`，读回来是 `{"k":1}`，不是带引号的字符串）。
        // 所以不需要区分"这列是不是 json 类型"，也就不需要把 LogicalType 传下来。
        DataType::Utf8 => {
            let v = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            push_json_string(out, v);
        }
        DataType::LargeUtf8 => {
            let v = arr
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row);
            push_json_string(out, v);
        }
        DataType::Date32 => {
            let days = arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row);
            let d = chrono::DateTime::from_timestamp(days as i64 * 86400, 0)
                .ok_or_else(|| {
                    D2DError::schema(format!("列 `{}` 的日期 {} 超出可表示范围", col, days))
                })?
                .naive_utc()
                .date();
            push_json_string(out, &d.format("%Y-%m-%d").to_string());
        }
        // Arrow 的 Time64 只有 Micro/Nano 两种具体数组（Second/Milli 是 Time32 的事）。
        // `d2d_types` 的桥接会把 `Time64(Second)` 也映射成 `DataType::Time64(Second)`，
        // 那种列**建不出数组**（`RowBuilder` 里那次 cast 就会失败），所以这里给一句
        // 说得清的报错，而不是 `unwrap` 崩掉。
        DataType::Time64(u) => {
            let v = match u {
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .value(row),
                other => {
                    return Err(D2DError::schema(format!(
                        "列 `{}` 的类型 Time64({:?}) 没有对应的 Arrow 数组，无法序列化",
                        col, other
                    )))
                }
            };
            push_json_string(out, &fmt_time(v, *u, col)?);
        }
        DataType::Timestamp(u, _tz) => {
            let v = match u {
                TimeUnit::Second => arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Millisecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Microsecond => arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(row),
                TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(row),
            };
            push_json_string(out, &fmt_timestamp(v, *u, col)?);
        }
        other => {
            return Err(D2DError::schema(format!(
                "列 `{}` 的类型 {:?} 无法写成 JSON —— Doris 没有二进制/复杂列类型，\
                 请确认是不是有 BLOB 列没先转成文本",
                col, other
            )))
        }
    }
    Ok(())
}

fn non_finite_err(col: &str, v: String) -> D2DError {
    D2DError::schema(format!(
        "列 `{}` 的值是 {} —— 不是合法 JSON 数字。CSV 里能写（当文本），JSON 里不能，\
         转换时会把整行判成脏数据。请在查询侧把它过滤或替换掉",
        col, v
    ))
}

/// i128 定点数 → `-123.4560` 这样的十进制串（按 scale 补零，不丢尾零）
fn scaled_i128_to_decimal_string(v: i128, scale: i8) -> String {
    if scale <= 0 {
        // scale=0：整数，负 scale 语义上还要乘 10^|s|（本仓未产生，按整数处理）
        return v.to_string();
    }
    let neg = v < 0;
    // i128::MIN 取绝对值会溢出，用无符号拆分
    let mag = v.unsigned_abs();
    let s = mag.to_string();
    let sc = scale as usize;
    let (int_part, frac_part) = if s.len() > sc {
        (s[..s.len() - sc].to_string(), s[s.len() - sc..].to_string())
    } else {
        ("0".to_string(), format!("{:0>width$}", s, width = sc))
    };
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac_part)
}

fn fmt_time(v: i64, unit: arrow::datatypes::TimeUnit, col: &str) -> D2DResult<String> {
    use arrow::datatypes::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (
            v.div_euclid(1_000),
            (v.rem_euclid(1_000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            v.div_euclid(1_000_000),
            (v.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            v.div_euclid(1_000_000_000),
            v.rem_euclid(1_000_000_000) as u32,
        ),
    };
    let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs as u32, nanos)
        .ok_or_else(|| D2DError::schema(format!("列 `{}` 的时间值 {} 超出范围", col, v)))?;
    Ok(match unit {
        TimeUnit::Second => t.format("%H:%M:%S").to_string(),
        TimeUnit::Millisecond => t.format("%H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => t.format("%H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => t.format("%H:%M:%S%.9f").to_string(),
    })
}

fn fmt_timestamp(v: i64, unit: arrow::datatypes::TimeUnit, col: &str) -> D2DResult<String> {
    use arrow::datatypes::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (v, 0),
        TimeUnit::Millisecond => (
            v.div_euclid(1_000),
            (v.rem_euclid(1_000) * 1_000_000) as u32,
        ),
        TimeUnit::Microsecond => (
            v.div_euclid(1_000_000),
            (v.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        TimeUnit::Nanosecond => (
            v.div_euclid(1_000_000_000),
            v.rem_euclid(1_000_000_000) as u32,
        ),
    };
    let dt = chrono::DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| D2DError::schema(format!("列 `{}` 的时间戳 {} 超出范围", col, v)))?
        .naive_utc();
    Ok(match unit {
        // 无小数位时**不补** `.000000`：实测 Doris 两者都收，少几个字节
        TimeUnit::Second => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        TimeUnit::Millisecond => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        TimeUnit::Microsecond => dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        TimeUnit::Nanosecond => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
    })
}

/// JSON 字符串字面量（含两侧引号）
fn push_json_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    escape_json_str_into(out, s);
    out.push(b'"');
}

/// JSON 字符串转义（RFC 8259：`"`、`\`、以及所有 < 0x20 的控制字符必须转义）
///
/// 非 ASCII **原样输出 UTF-8**，不转 `\uXXXX` —— 实测中文与 emoji 都能直接过，
/// 转成 `\u` 只是把字节数翻几倍。合法 UTF-8 的 JSON 不需要转义非 ASCII。
fn escape_json_str_into(out: &mut Vec<u8>, s: &str) {
    for &b in s.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x0C => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0x00..=0x1F => out.extend_from_slice(format!("\\u{:04x}", b).as_bytes()),
            _ => out.push(b),
        }
    }
}

/// 批次 → CSV 载荷。**NULL 写成 [`NULL_LITERAL`]，不写空字段。**
///
/// 提到自由函数是为了让它可单测 —— 它是"NULL 写成什么"这条约定的唯一出处，
/// 而这条约定错了的代价是整批被拒（数值列）或 NULL 被悄悄改成空串（字符串列），
/// 两种都不会在别处报错。
#[cfg(test)]
fn serialize_batch_csv_with(batch: &RecordBatch, sep: &[u8], line: &[u8]) -> D2DResult<Vec<u8>> {
    let mut output = Vec::with_capacity(batch.get_array_memory_size());
    for row_idx in 0..batch.num_rows() {
        append_csv_row(&mut output, batch, row_idx, sep, line)?;
    }
    Ok(output)
}

/// 把**一行** CSV 追加到 `output`
///
/// 拆成"一行一次"是为了让请求体按上限切分时切点落在**行边界**上
/// （见 [`DorisSinkSession::build_chunk`]）—— 序列化的粒度必须细到行，
/// 调用方才可能有行级的切分点。
///
/// # NULL 必须写成 [`NULL_LITERAL`]，不能写成空字段
///
/// `arrow::util::display::array_value_to_string` 对 NULL 返回**空串**，
/// 而空字段在 Doris 的 CSV Stream Load 里**不是 NULL**：
///
/// | 目标列类型 | 写空字段 | 写 `\N` |
/// |---|---|---|
/// | `BOOLEAN` / 数值 / 日期 | strict_mode 下**整批被拒** `value is incorrect` | 正常，落成 NULL |
/// | `VARCHAR` | 落成**空串**（NULL 被悄悄改成 `''`） | 正常，落成 NULL |
///
/// 实测（192.0.2.10:18040，`d2d_null_probe` 三列 `s/b/d`）：空字段那行
/// `Status: Fail`、`NumberFilteredRows: 1`、`FirstErrorMsg: column(b) value is
/// incorrect`；`\N` 那行 `Status: Success`，查回来三列 `IS NULL` 全是 1。
///
/// 这个 bug 活了很久没被发现，因为**之前跑过的都是 37 列宽表、那些列恰好一个
/// NULL 都没有** —— 迁移工具里"没有 NULL 的样本"是最容易漏掉的一类样本。
/// （`json` 格式没有这个问题：它用原生 `null`，见 [`append_json_row`]。）
///
/// # 值本身等于 `\N` 的字符串
///
/// 那会和 NULL 撞车（`null_value` 的默认值就是 `\N`）。Doris 的 CSV 没有转义
/// 可用 —— 与 dmfldr IN 路径"分隔符无法转义"是同一类问题，处理方式也一致：
/// **大声失败**，指出出路，而不是让它悄悄变成 NULL。
fn append_csv_row(
    output: &mut Vec<u8>,
    batch: &RecordBatch,
    row_idx: usize,
    sep: &[u8],
    line: &[u8],
) -> D2DResult<()> {
    let num_cols = batch.num_columns();
    for col_idx in 0..num_cols {
        let col = batch.column(col_idx);
        if col.is_null(row_idx) {
            output.extend_from_slice(NULL_LITERAL.as_bytes());
        } else {
            let value = array_value_to_string(col, row_idx);
            if value == NULL_LITERAL {
                return Err(D2DError::external(
                    "doris",
                    format!(
                        "第 {} 行 列 `{}` 的值就是字符串 \"\\N\" —— 这是 Doris 的 NULL 字面量，\
                         写下去会被当成 NULL，而 CSV 没有转义可用。\
                         请在查询侧把它换掉（如 REPLACE(col, '\\N', ...)），或改用 json 格式",
                        row_idx + 1,
                        batch.schema().field(col_idx).name()
                    ),
                ));
            }
            output.extend_from_slice(value.as_bytes());
        }
        if col_idx < num_cols - 1 {
            output.extend_from_slice(sep);
        }
    }
    output.extend_from_slice(line);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc as StdArc;

    fn batch_of(cols: Vec<(&str, arrow::array::ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            StdArc::new(Schema::new(fields)),
            cols.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    /// **NULL 必须写成 `\N`** —— 写空字段会让数值列整批被拒、字符串列的 NULL 变空串
    #[test]
    fn test_null_becomes_backslash_n_not_empty() {
        let b = batch_of(vec![
            ("id", StdArc::new(Int32Array::from(vec![Some(1), Some(2)]))),
            ("s", StdArc::new(StringArray::from(vec![Some("x"), None]))),
            (
                "flag",
                StdArc::new(BooleanArray::from(vec![None, Some(true)])),
            ),
        ]);
        let out = String::from_utf8(serialize_batch_csv_with(&b, b"|", b"\n").unwrap()).unwrap();
        assert_eq!(out, "1|x|\\N\n2|\\N|true\n");
        // 绝不能出现"两个分隔符贴在一起"那种空字段
        assert!(!out.contains("||"), "NULL 被写成了空字段: {}", out);
    }

    /// 空串与 NULL 在载荷里必须长得**不一样**
    #[test]
    fn test_empty_string_differs_from_null() {
        let b = batch_of(vec![(
            "s",
            StdArc::new(StringArray::from(vec![Some(""), None])),
        )]);
        let out = String::from_utf8(serialize_batch_csv_with(&b, b"|", b"\n").unwrap()).unwrap();
        assert_eq!(out, "\n\\N\n");
    }

    /// 值本身等于 `\N` 的字符串会与 NULL 撞车 —— 大声失败，不静默改语义
    #[test]
    fn test_literal_backslash_n_value_fails_loudly() {
        let b = batch_of(vec![(
            "s",
            StdArc::new(StringArray::from(vec![Some("\\N")])),
        )]);
        let e = match serialize_batch_csv_with(&b, b"|", b"\n") {
            Err(e) => e.to_string(),
            Ok(payload) => panic!("值等于 NULL 字面量时必须报错，实际产出: {:?}", payload),
        };
        assert!(e.contains("\\N"), "{}", e);
        assert!(e.contains("`s`"), "要点出是哪一列: {}", e);
        assert!(
            e.contains("REPLACE") || e.contains("json"),
            "要指出出路: {}",
            e
        );
    }

    #[test]
    fn test_multi_column_separator_and_line_delimiter() {
        let b = batch_of(vec![
            ("a", StdArc::new(Int32Array::from(vec![Some(1)]))),
            ("b", StdArc::new(Int32Array::from(vec![Some(2)]))),
        ]);
        let out = serialize_batch_csv_with(&b, b"\t", b"\r\n").unwrap();
        assert_eq!(out, b"1\t2\r\n");
    }

    #[test]
    fn test_empty_batch_yields_empty_payload() {
        let b = batch_of(vec![(
            "a",
            StdArc::new(Int32Array::from(Vec::<i32>::new())),
        )]);
        assert!(serialize_batch_csv_with(&b, b"|", b"\n")
            .unwrap()
            .is_empty());
    }

    /// NULL 字面量与发送的请求头必须同源（改了常量两处一起改）
    #[test]
    fn test_null_literal_is_doris_default() {
        assert_eq!(NULL_LITERAL, "\\N");
    }

    // ───────────────────────── JSON ─────────────────────────

    use arrow::array::{
        Date32Array, Decimal128Array, Float64Array, Int64Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::TimeUnit;

    fn json_of(cols: Vec<(&str, arrow::array::ArrayRef)>) -> String {
        String::from_utf8(serialize_batch_json(&batch_of(cols)).unwrap()).unwrap()
    }

    /// **一行一个对象** —— 不是 JSON 数组（数组形态要 `strip_outer_array`，多一处约定）
    #[test]
    fn test_one_object_per_line_not_an_array() {
        let out = json_of(vec![("id", StdArc::new(Int64Array::from(vec![1, 2])))]);
        assert_eq!(out, "{\"id\":1}\n{\"id\":2}\n");
        assert!(!out.trim_start().starts_with('['), "不该是数组形态");
    }

    /// NULL 走**原生 `null`**，不可能像 CSV 那样跟某个字面量撞车
    #[test]
    fn test_null_is_native_and_cannot_collide() {
        let out = json_of(vec![(
            "s",
            StdArc::new(StringArray::from(vec![Some(""), None])),
        )]);
        assert_eq!(out, "{\"s\":\"\"}\n{\"s\":null}\n");
        // CSV 那边值等于 `\N` 要报错；JSON 里 `\N` 只是个普通字符串
        let out = json_of(vec![(
            r#"s"#,
            StdArc::new(StringArray::from(vec![Some("\\N")])),
        )]);
        assert_eq!(out, "{\"s\":\"\\\\N\"}\n");
    }

    /// 字符串转义：引号、反斜杠、控制字符；**非 ASCII 原样输出**（不转 `\u`）
    #[test]
    fn test_string_escaping() {
        let out = json_of(vec![(
            "s",
            StdArc::new(StringArray::from(vec![Some("a\"b\\c\td\ne\u{1}f中文🌏")])),
        )]);
        assert_eq!(out, "{\"s\":\"a\\\"b\\\\c\\td\\ne\\u0001f中文🌏\"}\n");
    }

    /// 列名（键名）本身也要转义 —— 列名里带引号会把 JSON 结构撑破
    #[test]
    fn test_key_names_are_escaped() {
        let out = json_of(vec![("we\"ird", StdArc::new(Int64Array::from(vec![1])))]);
        assert_eq!(out, "{\"we\\\"ird\":1}\n");
    }

    /// 数值：整数原样、布尔 `true/false`、浮点**能往返**且大数走指数形态
    #[test]
    fn test_numbers_and_bools() {
        let out = json_of(vec![
            ("i", StdArc::new(Int64Array::from(vec![i64::MIN, i64::MAX]))),
            ("f", StdArc::new(Float64Array::from(vec![0.1, 1e308]))),
            ("b", StdArc::new(BooleanArray::from(vec![true, false]))),
        ]);
        assert_eq!(
            out,
            "{\"i\":-9223372036854775808,\"f\":0.1,\"b\":true}\n\
             {\"i\":9223372036854775807,\"f\":1e308,\"b\":false}\n"
        );
        // `{}` 会把 1e308 展开成 309 个字符；`{:?}` 才是短形态
        assert!(!out.contains("1000000000000000000000"));
    }

    /// NaN / ±Inf 不是合法 JSON —— **报错，不悄悄写成 null**
    #[test]
    fn test_non_finite_float_is_rejected() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let b = batch_of(vec![("f", StdArc::new(Float64Array::from(vec![v])))]);
            let e = match serialize_batch_json(&b) {
                Err(e) => e.to_string(),
                Ok(p) => panic!(
                    "非有限浮点必须报错，实际产出: {}",
                    String::from_utf8_lossy(&p)
                ),
            };
            assert!(e.contains("合法 JSON"), "{}", e);
            assert!(e.contains("`f`"), "要点出是哪一列: {}", e);
        }
        // 有限值照常
        let b = batch_of(vec![("f", StdArc::new(Float64Array::from(vec![1.5])))]);
        assert!(serialize_batch_json(&b).is_ok());
    }

    /// DECIMAL **不写成浮点**：38 位精度走 f64 就没了。实测 Doris 能原样收下这种写法。
    #[test]
    fn test_decimal_keeps_full_precision() {
        let out = json_of(vec![(
            "d",
            StdArc::new(
                Decimal128Array::from(vec![
                    12345678901234567890i128 * 10_000_000_000 + 1234567890,
                    -1,
                    0,
                ])
                .with_precision_and_scale(38, 10)
                .unwrap(),
            ),
        )]);
        assert_eq!(
            out,
            "{\"d\":12345678901234567890.1234567890}\n\
             {\"d\":-0.0000000001}\n\
             {\"d\":0.0000000000}\n"
        );
        // 绝不能掉进科学计数法（那是 f64 的写法）
        assert!(!out.contains('e'), "{}", out);
    }

    /// 定点渲染的边界：i128::MIN 取绝对值会溢出，必须走 unsigned_abs
    #[test]
    fn test_decimal_i128_min_does_not_overflow() {
        // i128::MIN 的绝对值是 2^127，`i128::abs` 在这里会 panic/溢出
        assert_eq!(
            scaled_i128_to_decimal_string(i128::MIN, 2),
            "-1701411834604692317316873037158841057.28"
        );
        // 末两位为 0 也要保留（scale=2 的 100 是 "1.00"，不是 "1"）
        assert_eq!(scaled_i128_to_decimal_string(100, 2), "1.00");
        assert_eq!(scaled_i128_to_decimal_string(5, 0), "5");
        assert_eq!(scaled_i128_to_decimal_string(-5, 3), "-0.005");
        assert_eq!(scaled_i128_to_decimal_string(12345, 2), "123.45");
    }

    /// 时间渲染成 Doris 认的形态（**空格**分隔，不是 RFC3339 的 `T`）
    #[test]
    fn test_temporal_rendering() {
        let out = json_of(vec![
            (
                "d",
                StdArc::new(Date32Array::from(vec![
                    (chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()
                        - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                    .num_days() as i32,
                ])),
            ),
            (
                "ts",
                StdArc::new(
                    TimestampMicrosecondArray::from(vec![chrono::NaiveDate::from_ymd_opt(
                        2022, 1, 1,
                    )
                    .unwrap()
                    .and_hms_micro_opt(2, 3, 4, 123456)
                    .unwrap()
                    .and_utc()
                    .timestamp_micros()])
                    .with_timezone_opt(None::<&str>),
                ),
            ),
        ]);
        assert_eq!(
            out,
            "{\"d\":\"2022-01-01\",\"ts\":\"2022-01-01 02:03:04.123456\"}\n"
        );
        assert!(
            !out.contains('T'),
            "RFC3339 的 T 分隔符 Doris 不认: {}",
            out
        );
    }

    #[test]
    fn test_time_unit_rendering() {
        assert_eq!(
            fmt_timestamp(0, TimeUnit::Second, "c").unwrap(),
            "1970-01-01 00:00:00"
        );
        assert_eq!(
            fmt_timestamp(1, TimeUnit::Millisecond, "c").unwrap(),
            "1970-01-01 00:00:00.001"
        );
        assert_eq!(
            fmt_timestamp(1_000_000, TimeUnit::Microsecond, "c").unwrap(),
            "1970-01-01 00:00:01.000000"
        );
        // 负值不能走 div_euclid 之外的算法（会算成前一天同一时刻的正数）
        assert_eq!(
            fmt_timestamp(-1, TimeUnit::Second, "c").unwrap(),
            "1969-12-31 23:59:59"
        );
        assert_eq!(
            fmt_time(3_723_000_004, TimeUnit::Microsecond, "c").unwrap(),
            "01:02:03.000004"
        );
    }

    /// 二进制列写不了 JSON（Doris 也没有二进制列类型）—— 报错并指出出路
    #[test]
    fn test_binary_column_rejected_with_hint() {
        use arrow::array::BinaryArray;
        let b = batch_of(vec![(
            "blob",
            StdArc::new(BinaryArray::from_vec(vec![&b"\x00\x01"[..]])),
        )]);
        let e = match serialize_batch_json(&b) {
            Err(e) => e.to_string(),
            Ok(p) => panic!(
                "二进制列必须报错，实际产出: {}",
                String::from_utf8_lossy(&p)
            ),
        };
        assert!(e.contains("`blob`"), "{}", e);
        assert!(
            e.contains("文本") || e.contains("BLOB"),
            "要指出出路: {}",
            e
        );
    }

    #[test]
    fn test_empty_batch_yields_empty_json_payload() {
        let b = batch_of(vec![(
            "a",
            StdArc::new(Int64Array::from(Vec::<i64>::new())),
        )]);
        assert!(serialize_batch_json(&b).unwrap().is_empty());
    }

    // ───────────────── 按单请求上限切分 ─────────────────

    fn session(fmt: &str, limit: Option<usize>) -> DorisSinkSession {
        let cfg = crate::config::StreamLoadConfig {
            format: fmt.to_string(),
            max_request_bytes: limit,
            ..Default::default()
        };
        DorisSinkSession::new(
            crate::config::DorisConnection::from_json(
                &serde_json::json!({"fe_hosts": ["h"]}),
                "db",
                "t",
            )
            .unwrap(),
            cfg,
            WriteConfig::default(),
            None,
            reqwest::Client::new(),
        )
        .unwrap()
    }

    /// 上限之下**不切**：整批一个块，与切分前行为一致
    #[test]
    fn test_no_limit_keeps_whole_batch_in_one_chunk() {
        let b = batch_of(vec![("id", StdArc::new(Int64Array::from(vec![1, 2, 3])))]);
        let s = session("csv", None);
        let (data, n) = s.build_chunk(&b, 0, &[]).unwrap();
        assert_eq!(n, 3);
        assert_eq!(String::from_utf8(data).unwrap(), "1\n2\n3\n");
    }

    /// 切点必须落在**行边界**，且每块**不超过上限**
    ///
    /// 两个断言缺一不可。第一版只有"行边界"那条，于是漏掉了"每块都超上限一行"
    /// 这个 bug —— 直到线上被 Doris 挡下（`json body size 104858428 exceed ... 104857600`，
    /// 超 828 字节，正好一行）。**测试要断言的契约是 `<= 上限`，不是"看起来切开了"。**
    #[test]
    fn test_chunking_respects_limit_and_row_boundaries() {
        let b = batch_of(vec![(
            "id",
            StdArc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        )]);
        // {"id":N}\n 是 9 或 10 字节 → 上限 13 意味着一行一块
        let limit = 13usize;
        let s = session("json", Some(limit));
        let mut sent = 0;
        let mut blocks = Vec::new();
        while sent < 5 {
            let (data, n) = s.build_chunk(&b, sent, &json_keys(&b)).unwrap();
            assert!(
                data.len() <= limit,
                "块 {} 字节超了上限 {} —— 上限必须是硬的",
                data.len(),
                limit
            );
            let text = String::from_utf8(data).unwrap();
            assert!(text.ends_with('\n'), "块尾必须是完整行: {:?}", text);
            assert_eq!(text.matches('\n').count(), n, "块里行数与报告的行数要一致");
            blocks.push(text);
            sent += n;
        }
        assert!(blocks.len() > 1, "上限压这么低应当切出多块");
        // 拼起来 = 整批（一行不少、一行不重）
        let all: String = blocks.concat();
        assert_eq!(
            all,
            "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n{\"id\":5}\n"
        );
    }

    /// 上限足够大时**不切**；上限恰好等于整批大小也仍然不切（边界是闭的）
    #[test]
    fn test_chunking_boundary_is_inclusive() {
        let b = batch_of(vec![("id", StdArc::new(Int64Array::from(vec![1, 2, 3])))]);
        let whole = "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n".len();
        // 正好等于：整批一块
        let s = session("json", Some(whole));
        let (data, n) = s.build_chunk(&b, 0, &json_keys(&b)).unwrap();
        assert_eq!(n, 3);
        assert_eq!(data.len(), whole);
        // 少一个字节：最后一行放不下，切成两块
        let s = session("json", Some(whole - 1));
        let (d1, n1) = s.build_chunk(&b, 0, &json_keys(&b)).unwrap();
        assert_eq!((n1, d1.len()), (2, "{\"id\":1}\n{\"id\":2}\n".len()));
        let (d2, n2) = s.build_chunk(&b, n1, &json_keys(&b)).unwrap();
        assert_eq!(
            (n2, String::from_utf8(d2).unwrap().as_str()),
            (1, "{\"id\":3}\n")
        );
    }

    /// 单行就超过上限：**报错**，不硬发也不静默丢
    #[test]
    fn test_single_oversized_row_errors() {
        let b = batch_of(vec![(
            "s",
            StdArc::new(StringArray::from(vec!["x".repeat(200)])),
        )]);
        let s = session("json", Some(16));
        let e = match s.build_chunk(&b, 0, &json_keys(&b)) {
            Err(e) => e.to_string(),
            Ok((d, _)) => panic!("单行超限必须报错，实际产出 {} 字节", d.len()),
        };
        assert!(e.contains("单独一行"), "{}", e);
        assert!(
            e.contains("streaming_load_json_max_mb"),
            "要指出出路: {}",
            e
        );
    }

    /// `format: json` 不给 `max_request_bytes` 时**自动补** Doris BE 的出厂上限
    ///
    /// 不补的话，"同一份 batch_bytes 配 csv 能跑、配 json 整批被拒"——
    /// 实测撞过：`json body size 142058920 exceed BE's conf streaming_load_json_max_mb 104857600`。
    #[test]
    fn test_json_gets_default_request_limit_but_csv_does_not() {
        let json = StreamLoadConfig {
            format: "json".into(),
            ..Default::default()
        }
        .with_format_defaults();
        assert_eq!(
            json.max_request_bytes,
            Some(crate::config::JSON_DEFAULT_MAX_REQUEST_BYTES)
        );

        let csv = StreamLoadConfig::default().with_format_defaults();
        assert_eq!(
            csv.max_request_bytes, None,
            "csv 的上限远高于任何合理攒批量，不必切"
        );

        // 用户显式给了就听用户的（BE 上调过就不该再按出厂值切）
        let custom = StreamLoadConfig {
            format: "json".into(),
            max_request_bytes: Some(4096),
            ..Default::default()
        }
        .with_format_defaults();
        assert_eq!(custom.max_request_bytes, Some(4096));
    }
}
