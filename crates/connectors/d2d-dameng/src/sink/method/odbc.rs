//! 达梦写入方式：**绑定参数的多行 INSERT**（覆盖路径）
//!
//! # 形状
//!
//! ```text
//! write_batch(一份数据)：
//!   开一条连接（spawn_blocking 里，与读取路径同一个形状）
//!   按 max_params 把这一份切成若干段
//!   每段拼一条 `INSERT INTO t (c1,c2) VALUES (?,?),(?,?),(?,?)`
//!   绑参数执行
//!   连接随作用域关掉
//! ```
//!
//! # 为什么每次写入开一条连接
//!
//! `odbc_api::Connection<'_>` 借着 `Environment` 活 —— 想把它存进会话结构就是个
//! 自引用结构。而内核一次递过来的是**一份**数据（默认 ~100 MB），
//! 一次连接握手摊在这上面可以忽略。读取路径也是同一个形状（`OdbcMethod::read`）。
//!
//! # 为什么是"多行 VALUES"而不是"一行一条"
//!
//! 一行一条 = 每行一次往返。多行把 `max_params / 列数` 行并进一条语句，
//! 往返次数按列数分之一降下来。参数上限是**连接器的物理约束**
//! （`properties.max_params`），不是内核的事 —— 内核给的是 `batch_bytes` 那个上界。
//!
//! # 值一律按**文本**绑
//!
//! 达梦驱动会把文本按目标列的声明类型转换。选它而不是"逐类型挑 native 绑定"，
//! 是因为只有一条路径要维护（与 dmfldr 那条"文本进、库自己解释"也是同一个立场）。
//! 代价是时间/日期要**显式格式化**（见 [`arrow_value_to_text`]）——
//! 交给 Arrow 的默认 Display 会打出 `2026-10-16T18:19:13`，那个 `T` 达梦不认。

use super::{DamengWriteKind, DamengWriteMethod, WriteContext};
use async_trait::async_trait;
use d2d_connector::SinkSession;
use d2d_core::{D2DError, D2DResult};
use odbc_api::parameter::InputParameter;
use odbc_api::{ConnectionOptions, Environment, IntoParameter};
use std::sync::Arc;
use tracing::{debug, info};

/// 缺省单语句参数上限
///
/// 取 2000：达梦对一条语句里的参数个数有上限（与 Oracle 同源的实现），
/// 2000 在安全区内，同时足以让往返次数降一个数量级。超了要让用户能调（`properties.max_params`）。
pub const DEFAULT_MAX_PARAMS: usize = 2000;

/// 绑定参数的多行 INSERT 写入方法
/// 无状态：写入要用的一切由 `begin_session` 时的 [`WriteContext`] 给
/// （连接器持有它，方法自己不必再存一份）
#[derive(Debug, Default)]
pub struct DamengInsertMethod;

#[async_trait]
impl DamengWriteMethod for DamengInsertMethod {
    fn kind(&self) -> DamengWriteKind {
        DamengWriteKind::Odbc
    }

    async fn begin_session(&self, ctx: Arc<WriteContext>) -> D2DResult<Box<dyn SinkSession>> {
        Ok(Box::new(DamengInsertSession { ctx }))
    }
}

impl DamengInsertMethod {
    /// `properties.max_params`（可选覆写）
    pub fn max_params_of(props: &serde_json::Map<String, serde_json::Value>) -> usize {
        props
            .get("max_params")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .filter(|v| *v >= 1)
            .unwrap_or(DEFAULT_MAX_PARAMS)
    }
}

/// 一条写入会话 = 一次真实写入（内核攒够一份才开一条）
struct DamengInsertSession {
    ctx: Arc<WriteContext>,
}

#[async_trait]
impl SinkSession for DamengInsertSession {
    async fn write_batch(&mut self, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let ctx = Arc::clone(&self.ctx);
        // 阻塞 I/O：整段挪进 spawn_blocking（与读取路径同一条纪律）
        tokio::task::spawn_blocking(move || insert_batch(&ctx, batch))
            .await
            .map_err(|e| D2DError::internal(format!("达梦写入任务 panic: {}", e)))?
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // 每次 `write_batch` 都已落库（没有缓冲），这里无事可做。
        // 保留它是为了"会话结束"这个语义（日后要发 cancel、要确认事务都挂这儿）。
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        // 没有缓冲可清；已经写进去的行是**逐条自动提交**的，不回滚 ——
        // 这条要说清楚（见模块文档"事务"一节之外的使用者须知）：
        // ODBC 路径不做跨批事务，失败重跑可能产生重复行（幂等靠目标端唯一键）。
        Ok(())
    }
}

/// 真正的插入：开连接 → 切段 → 多行 VALUES
fn insert_batch(ctx: &WriteContext, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
    let ncols = batch.num_columns();
    if ncols == 0 {
        return Err(D2DError::schema(
            "达梦写入收到 0 列的数据 —— 上游 schema 不对",
        ));
    }
    let columns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| d2d_connector::quote_ident(f.name()))
        .collect();
    let rows = batch.num_rows();
    // 一段塞多少行：参数上限 ÷ 列数（至少 1 行）
    let rows_per_stmt = (ctx.max_params / ncols).max(1);

    let cs = ctx.conn.to_odbc_connection_string();
    let env =
        Environment::new().map_err(|e| odbc_err("创建 ODBC 环境失败（unixODBC 装了没？）", e))?;
    let conn = env
        .connect_with_connection_string(&cs, ConnectionOptions::default())
        .map_err(|e| odbc_err("达梦连接失败（写向）", e))?;

    let mut done = 0usize;
    while done < rows {
        let n = rows_per_stmt.min(rows - done);
        let sql = build_multi_row_insert(&ctx.qualified_table(), &columns, n);
        // 值一律先落成字符串：`&[Box<dyn InputParameter>]` 按序绑到 `?` 1..N，
        // 而字符串要在 execute 返回前一直活着 —— 所以 params 借 local 的 `texts`。
        let texts = collect_texts(&batch, done, n);
        // `Option<&str>` 一次到位：`Some` 绑文本、`None` 绑 SQL NULL。
        // 借的是 local 的 `texts`，在 `execute` 返回前一直活着 ✓
        let params: Vec<Box<dyn InputParameter>> = texts
            .iter()
            .map(|t| Box::new(t.as_deref().into_parameter()) as Box<dyn InputParameter>)
            .collect();
        conn.execute(&sql, params.as_slice(), None)
            .map_err(|e| odbc_err("达梦 INSERT 失败", e))?;
        debug!(rows = n, "达梦：一段已写入");
        done += n;
    }

    info!(
        rows,
        columns = ncols,
        rows_per_stmt,
        "达梦写入完成（绑定参数的多行 INSERT）"
    );
    Ok(rows)
}

/// `INSERT INTO t (c1,c2) VALUES (?,?),(?,?),(?,?)`
fn build_multi_row_insert(table: &str, cols: &[String], rows: usize) -> String {
    let one = format!("({})", vec!["?"; cols.len()].join(","));
    let values = vec![one.as_str(); rows].join(",");
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table,
        cols.join(","),
        values
    )
}

/// 把一段数据摊平成长度 `rows × ncols` 的文本（行优先），`None` = SQL NULL
///
/// 顺序必须是**行优先**：参数按 `?` 从左到右绑，而 SQL 里是 `(c1,c2),(c1,c2)`。
pub(crate) fn collect_texts(
    batch: &arrow::array::RecordBatch,
    start: usize,
    rows: usize,
) -> Vec<Option<String>> {
    let ncols = batch.num_columns();
    let mut out = Vec::with_capacity(rows * ncols);
    for r in start..start + rows {
        for c in 0..ncols {
            out.push(arrow_value_to_text(batch.column(c).as_ref(), r));
        }
    }
    out
}

/// 一个 Arrow 值 → 达梦能按目标列类型解释的文本；`None` = NULL
///
/// **时间/日期必须显式格式化**：Arrow 的 Display 对 `Timestamp` 打出
/// `2026-10-16T18:19:13`（带 `T`），达梦不认这个形态。
pub fn arrow_value_to_text(col: &dyn arrow::array::Array, row: usize) -> Option<String> {
    use arrow::datatypes::{DataType, TimeUnit};

    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Boolean => {
            let a = col.as_any().downcast_ref::<arrow::array::BooleanArray>()?;
            Some(if a.value(row) { "1".into() } else { "0".into() })
        }
        // 时间戳：达梦认 `YYYY-MM-DD HH:MM:SS[.ffffff]`
        DataType::Timestamp(unit, _) => {
            use arrow::array::TimestampMicrosecondArray as Ts;
            let a = col.as_any().downcast_ref::<Ts>()?;
            let micros = match unit {
                TimeUnit::Second => a.value(row) * 1_000_000,
                TimeUnit::Millisecond => a.value(row) * 1_000,
                TimeUnit::Microsecond => a.value(row),
                TimeUnit::Nanosecond => a.value(row) / 1_000,
            };
            Some(micros_to_dm_timestamp(micros))
        }
        DataType::Date32 => {
            let a = col.as_any().downcast_ref::<arrow::array::Date32Array>()?;
            Some(days_to_dm_date(a.value(row) as i64))
        }
        // 其余交给 Arrow 的规范文本形态：DECIMAL 是精确十进制、整数不走科学计数法
        _ => {
            use arrow::util::display::array_value_to_string;
            Some(array_value_to_string(col, row).unwrap_or_default())
        }
    }
}

/// 微秒时间戳 → `YYYY-MM-DD HH:MM:SS.ffffff`（UTC 存储，不做时区换算）
fn micros_to_dm_timestamp(micros: i64) -> String {
    use chrono::{DateTime, Utc};
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(secs, frac * 1000) {
        Some(dt) => dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        None => chrono::DateTime::UNIX_EPOCH
            .naive_utc()
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string(),
    }
}

/// 自 epoch 起的天数 → `YYYY-MM-DD`
fn days_to_dm_date(days: i64) -> String {
    use chrono::{DateTime, Utc};
    let secs = days * 86_400;
    match DateTime::<Utc>::from_timestamp(secs, 0) {
        Some(dt) => dt.naive_utc().format("%Y-%m-%d").to_string(),
        None => chrono::DateTime::UNIX_EPOCH
            .naive_utc()
            .format("%Y-%m-%d")
            .to_string(),
    }
}

/// ODBC 错误 → 带上下文的内核错误（与读取路径同一套措辞口径）
fn odbc_err(what: &str, e: odbc_api::Error) -> D2DError {
    D2DError::external("odbc", format!("{}: {}", what, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch_of(cols: Vec<ArrayRef>) -> arrow::array::RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{}", i), c.data_type().clone(), true))
            .collect();
        arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    /// 参数顺序必须与 SQL 里的 `(c1,c2),(c1,c2)` 一致 —— **行优先**
    #[test]
    fn test_texts_are_row_major() {
        let b = batch_of(vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ]);
        let t = collect_texts(&b, 0, 2);
        assert_eq!(
            t,
            vec![
                Some("1".into()),
                Some("a".into()),
                Some("2".into()),
                Some("b".into())
            ]
        );
    }

    /// NULL 必须是 `None`（绑成 SQL NULL），**不能**是一串空字符串 ——
    /// 那会把空串写进 NOT NULL 列或当成 0。
    #[test]
    fn test_null_is_none_not_empty_string() {
        let b = batch_of(vec![Arc::new(StringArray::from(vec![Some("x"), None]))]);
        let t = collect_texts(&b, 0, 2);
        assert_eq!(t, vec![Some("x".into()), None]);
    }

    /// 从中间一段取（分批写入时用）—— 偏移不能错位
    #[test]
    fn test_collect_texts_honours_offset() {
        let b = batch_of(vec![Arc::new(Int32Array::from(vec![10, 20, 30]))]);
        assert_eq!(
            collect_texts(&b, 1, 2),
            vec![Some("20".into()), Some("30".into())]
        );
    }

    /// **回归**：时间戳不能带 `T` —— 达梦不认 Arrow 的默认 Display
    #[test]
    fn test_timestamp_uses_dm_shape() {
        let arr = TimestampMicrosecondArray::from(vec![0i64]);
        let s = arrow_value_to_text(&arr, 0).unwrap();
        assert_eq!(s, "1970-01-01 00:00:00.000000");
        assert!(!s.contains('T'), "达梦不认带 T 的时间戳: {}", s);
    }

    #[test]
    fn test_multi_row_insert_shape() {
        let cols = vec!["\"A\"".to_string(), "\"B\"".to_string()];
        assert_eq!(
            build_multi_row_insert("\"S\".\"T\"", &cols, 2),
            "INSERT INTO \"S\".\"T\" (\"A\",\"B\") VALUES (?,?),(?,?)"
        );
        assert_eq!(
            build_multi_row_insert("\"T\"", &cols, 1),
            "INSERT INTO \"T\" (\"A\",\"B\") VALUES (?,?)"
        );
    }

    /// 列名要**加引号**（达梦默认大小写敏感；不加引号会变成全大写）
    #[test]
    fn test_column_names_are_quoted() {
        let ctx = WriteContext {
            schema: Some("PROD".into()),
            table: "t_a".into(),
            conn: crate::config::ConnectionConfig::from_json(
                &serde_json::json!({"host":"h","user":"u"}),
            )
            .unwrap(),
            max_params: 10,
        };
        assert_eq!(ctx.qualified_table(), "\"PROD\".\"t_a\"");
    }
}
