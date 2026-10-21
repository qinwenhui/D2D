//! 达梦写入方式：**`dmfldr MODE=IN` 快载**（快路）
//!
//! # 形状
//!
//! ```text
//! write_batch(一份数据)：
//!   建一个临时目录（每份数据一个，互不干扰）
//!   把这一份按分隔符写成 data.txt（值**原样**，不转义）
//!   写 d2d_fldr.ctl（列序 + 时间列的 DATE FORMAT + 空值字面量）
//!   spawn dmfldr MODE='IN' CONTROL=…（`proc::run_load`）
//!   从日志读「读取的逻辑记录总数」= 目标端**自己确认**载入的行数
//!   返回那个数（→ 收尾对账拿它当"目标端确认"）
//! ```
//!
//! # 为什么值不转义，而是**含分隔符就失败**
//!
//! 实测（2026-10-16，dmfldr V8）：控制文件声明 `ESCAPE='\\'` 之后，
//! 值里的 `\\` 能正确还原，但**值里的分隔符写成 `\|` 会被直接拒绝**（退出码 2、记一行拒绝）
//! —— dmfldr 先按分隔符切行、再谈转义，所以分隔符根本轮不到转义出手。
//!
//! 结论：**ESCAPE 唯一能救的那件事它救不了**，声明它只会让写值这一侧多一条
//! 必须记住的转义规则。于是这里不设 ESCAPE、值原样写，而把"救不了"变成**大声失败**：
//!
//! ```text
//! 值里含 分隔符 / 换行 / 恰好等于空值字面量  ⟹  报错，并指出出路
//! ```
//!
//! 出路是真的存在：把 `field_seperator` 换成数据里不出现的字符
//! （控制文件的 `FIELDS X '..'` 支持十六进制，非可打印字符也能当分隔符）。
//! 这比"写进去赌一把"诚实 —— 本项目最不能接受的就是静默改值。
//!
//! # 与 ODBC 那条路的分工
//!
//! | | `dmfldr`（本条） | `odbc` |
//! |---|---|---|
//! | 速度 | 快路（`direct: true` 时最快） | 慢（逐段多行 INSERT） |
//! | 值里的分隔符 | **受限**（见上） | 无限制（绑定参数，不经文本） |
//! | 约束检查 | 缺省**会查**（`DIRECT=FALSE`）；`direct: true` 时**不查** | 一直由数据库检查 |
//! | 行数对账 | 目标端自报「读取的逻辑记录总数」 | 只知道自己提交了多少 |
//!
//! 选型口径与源端一致：**大批量走 dmfldr，值脏 / 要精确控制走 odbc**。
//!
//! # `DIRECT`：一个"缺省不安全"的旋钮
//!
//! dmfldr 自己的 `DIRECT` 缺省是 **TRUE** = 直接写 B 树、**不检查约束**（手册：
//! 「约束的检查等由用户保证」）。实测（2026-10-17，dmfldr V8）：往主键表灌两行
//! `ID=1`，`DIRECT=TRUE` 下两行都进去了，**之后整张表的 UPDATE / DELETE / INSERT
//! 全部失败**，只剩 SELECT —— 表被写成了不可更新的状态。`DIRECT=FALSE` 则整批被拒
//! （`违反唯一性约束条件`），表干净。
//!
//! 代价是装载慢 4–5.4×（2M 行：有主键 1.55 → 8.43 s，无主键 0.70 → 2.88 s），
//! 但仍远快于 ODBC。所以本连接器的缺省是 **`direct: false`**，
//! 要快路的用户显式开，并自己承担"这批数据不会违反任何约束"的保证。

use super::{DamengWriteKind, DamengWriteMethod, WriteContext};
use crate::config::ConnectionConfig;
use crate::source::method::dmfldr::ctl::{render_in_control_file, InControlSpec};
use crate::source::method::dmfldr::params::CharacterCode;
use crate::source::method::dmfldr::proc;
use async_trait::async_trait;
use d2d_connector::SinkSession;
use d2d_core::{D2DError, D2DResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

/// IN 快载的参数（`sink.properties.dmfldr`）
///
/// **比读侧那份小得多**：读侧的 `colname_heading` / `single_file` / `send_node_number`
/// 这些是"怎么把库导出来"的旋钮，灌进去时没有对应物。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlDrInParams {
    /// dmfldr 可执行文件（PATH 名或绝对路径）
    #[serde(default = "default_executable")]
    pub executable: String,
    /// 临时目录（数据文件 / 控制文件 / 日志都放这儿）；缺省用系统临时目录
    #[serde(default)]
    pub work_dir: Option<PathBuf>,
    #[serde(default)]
    pub character_code: CharacterCode,
    /// 列分隔符（默认 `|`）
    ///
    /// ⚠️ 值里出现它就会让这一份失败（见模块文档）—— 数据脏就把这个换掉。
    #[serde(default = "default_field_sep")]
    pub field_seperator: String,
    /// 空值字面量（`NULL_MODE=TRUE`）。值恰好等于它也会失败 —— 否则那个值会被读成 NULL。
    #[serde(default = "default_null_str")]
    pub null_str: String,
    /// 读文件缓冲区页大小 MB（1..=2048）
    #[serde(default = "default_buffer_node_size")]
    pub buffer_node_size: u32,
    /// 加载线程数；0 = 自动
    #[serde(default)]
    pub task_thread_number: u32,
    /// 错误信息记录级别（0..=4）
    #[serde(default = "default_log_level")]
    pub log_level: u8,
    /// **走不走 dmfldr 的 `DIRECT` 快路**（缺省 **false**）
    ///
    /// `DIRECT=TRUE` 直接写 B 树，省掉普通插入的运算符跳转 —— 也就**不检查约束**
    /// （手册：「约束的检查等由用户保证，dmfldr 将不处理有约束冲突的数据」）。
    ///
    /// 实测（2026-10-17，dmfldr V8）：往主键表灌两行 `ID=1`，`DIRECT=TRUE`（dmfldr 自己的
    /// 缺省值）下两行都进去了，**之后整张表的 UPDATE / DELETE / INSERT 全部失败**，
    /// 只剩 SELECT —— 表被写成了不可更新的状态。`DIRECT=FALSE` 则整批被拒
    /// （`违反唯一性约束条件`），表干净。
    ///
    /// 代价是装载慢 4–5.4×（2M 行：有主键 1.55 → 8.43 s，无主键 0.70 → 2.88 s），
    /// 但仍远快于 ODBC 的多行 INSERT。**要做快路就显式写 `direct: true`**，
    /// 并且自己保证目标表没有会被这批数据违反的主键/唯一索引/外键/CHECK。
    #[serde(default)]
    pub direct: bool,
}

fn default_executable() -> String {
    "dmfldr".to_string()
}
fn default_field_sep() -> String {
    "|".to_string()
}
fn default_null_str() -> String {
    "NULL".to_string()
}
fn default_buffer_node_size() -> u32 {
    64
}
fn default_log_level() -> u8 {
    3
}

impl Default for FlDrInParams {
    fn default() -> Self {
        Self {
            executable: default_executable(),
            work_dir: None,
            character_code: CharacterCode::default(),
            field_seperator: default_field_sep(),
            null_str: default_null_str(),
            buffer_node_size: default_buffer_node_size(),
            task_thread_number: 0,
            log_level: default_log_level(),
            direct: false, // 安全优先，见字段文档；快路要显式开
        }
    }
}

impl FlDrInParams {
    /// 从 `sink.properties.dmfldr` 解析（缺失 = 默认值）
    pub fn from_props(props: &serde_json::Map<String, serde_json::Value>) -> D2DResult<Self> {
        let Some(v) = props.get("dmfldr") else {
            return Ok(Self::default());
        };
        if v.is_null() {
            return Ok(Self::default());
        }
        serde_json::from_value(v.clone())
            .map_err(|e| D2DError::config(format!("invalid sink.properties.dmfldr: {}", e)))
    }

    /// 启动期取值校验（fail-fast，与读侧同一条纪律）
    pub fn validate(&self) -> D2DResult<()> {
        let mut errs: Vec<String> = Vec::new();
        if self.field_seperator.is_empty() || self.field_seperator.len() > 4 {
            errs.push(format!(
                "field_seperator 得是 1..=4 字节（超过 4 字节的控制文件 FIELDS 子句表达不了），当前 {}",
                self.field_seperator.len()
            ));
        }
        if self.null_str.is_empty() || self.null_str.len() > 128 {
            errs.push("null_str 得是 1..=128 字节".to_string());
        }
        if !(1..=2048).contains(&self.buffer_node_size) {
            errs.push(format!(
                "buffer_node_size must be 1..=2048 MB, got {}",
                self.buffer_node_size
            ));
        }
        if self.log_level > 4 {
            errs.push(format!("log_level must be 0..=4, got {}", self.log_level));
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(D2DError::config(format!(
                "invalid sink.properties.dmfldr: {}",
                errs.join("; ")
            )))
        }
    }

    fn work_dir(&self) -> PathBuf {
        self.work_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("d2d-dmfldr-in"))
    }
}

/// 写侧的启动期清扫：清掉上次被 `kill -9` 留下的 IN 导出物
///
/// 与读侧 [`crate::source::method::dmfldr::proc::sweep_stale_export_dirs`] 同一个道理
/// —— 目录名里带 pid，只清"确定已死"的那些（判活看 `/proc`，非 Linux 一个都不清）。
pub fn sweep_stale_in_dirs(p: &FlDrInParams) -> usize {
    let dir = p
        .work_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("d2d-dmfldr-in"));
    crate::source::method::dmfldr::proc::sweep_stale_export_dirs(&dir)
}

/// `dmfldr MODE=IN` 快载写入方法
pub struct DamengFastLoadMethod {
    params: FlDrInParams,
    conn: ConnectionConfig,
    /// 已产生的临时目录（close 时统一清理）
    dirs: Mutex<Vec<PathBuf>>,
}

impl DamengFastLoadMethod {
    pub fn new(params: FlDrInParams, conn: ConnectionConfig) -> Self {
        Self {
            params,
            conn,
            dirs: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl DamengWriteMethod for DamengFastLoadMethod {
    fn kind(&self) -> DamengWriteKind {
        DamengWriteKind::FlDr
    }

    async fn begin_session(&self, ctx: Arc<WriteContext>) -> D2DResult<Box<dyn SinkSession>> {
        Ok(Box::new(DamengFastLoadSession {
            ctx,
            params: self.params.clone(),
            conn: self.conn.clone(),
        }))
    }

    async fn close(&self) -> D2DResult<()> {
        let dirs = std::mem::take(&mut *self.dirs.lock().unwrap());
        for d in dirs {
            proc::cleanup_dir(&d);
        }
        Ok(())
    }
}

/// 一条会话 = 一次真实写入（内核攒够一份才开一条）
struct DamengFastLoadSession {
    ctx: Arc<WriteContext>,
    params: FlDrInParams,
    conn: ConnectionConfig,
}

#[async_trait]
impl SinkSession for DamengFastLoadSession {
    async fn write_batch(&mut self, batch: arrow::array::RecordBatch) -> D2DResult<usize> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(0);
        }
        let loaded = self.load(batch).await?;
        // ⚠️ 返回的是**目标端自己确认载入的行数**（dmfldr 日志的「读取的逻辑记录总数」），
        //    不是"我提交了多少行"。两者不等就是丢数，收尾对账会拿它抓出来。
        if loaded != rows as u64 {
            return Err(D2DError::external(
                "dmfldr",
                format!(
                    "达梦 IN 快载：提交 {} 行，目标端只确认载入 {} 行（相差 {}）。\
                     本批已被拒绝，不是「部分成功」。",
                    rows,
                    loaded,
                    rows as i64 - loaded as i64
                ),
            ));
        }
        Ok(loaded as usize)
    }

    async fn commit(&mut self) -> D2DResult<()> {
        // 每次 `write_batch` 都当场载完了（没有缓冲）
        Ok(())
    }

    async fn abort(&mut self) -> D2DResult<()> {
        // 已载入的批是已提交事务，不回滚（幂等靠目标端唯一键）
        Ok(())
    }
}

impl DamengFastLoadSession {
    async fn load(&self, batch: arrow::array::RecordBatch) -> D2DResult<u64> {
        let dir = proc::make_export_dir(&self.params.work_dir(), &self.ctx.table)?;
        let data_file = dir.join("data.txt");
        let ctl_file = dir.join("d2d_fldr.ctl");
        let log_file = dir.join("d2d_fldr.log");

        let p = &self.params;
        let text = render_data(&batch, &p.field_seperator, &p.null_str, p.character_code)?;
        std::fs::write(&data_file, text.as_bytes()).map_err(|e| {
            D2DError::external(
                "dmfldr",
                format!("写数据文件 {} 失败: {}", data_file.display(), e),
            )
        })?;

        let (columns, date_formats) = columns_of(&batch);
        let ctl = render_in_control_file(&InControlSpec {
            table_sql: &self.ctx.qualified_table(),
            data_file: &data_file,
            columns: &columns,
            date_formats: &date_formats,
            field_sep: &p.field_seperator,
            null_str: &p.null_str,
            character_code: p.character_code,
            buffer_node_size: p.buffer_node_size,
            task_threads: effective_threads(p.task_thread_number),
            log_level: p.log_level,
            direct: p.direct,
        });
        std::fs::write(&ctl_file, ctl).map_err(|e| {
            D2DError::external(
                "dmfldr",
                format!("写控制文件 {} 失败: {}", ctl_file.display(), e),
            )
        })?;

        debug!(
            dir = %dir.display(),
            rows = batch.num_rows(),
            bytes = text.len(),
            "dmfldr IN：数据与控制文件已就绪"
        );

        let stats = proc::run_load(
            &p.executable,
            &self.conn.to_userid(),
            &ctl_file,
            &log_file,
            &dir,
        )
        .await?;

        // 被拒绝/跳过的记录数不为 0 → 这一批**没有全进去**，必须失败
        // （退出码可能仍是 0：dmfldr 允许 max_filter_ratio 范围内丢行）
        if stats.rows_rejected > 0 || stats.rows_skipped > 0 {
            return Err(D2DError::external(
                "dmfldr",
                format!(
                    "达梦 IN 快载有记录没能进去：拒绝 {} 行、跳过 {} 行。\
                     常见原因：① 值里含分隔符 '{}'（dmfldr 无法转义分隔符，\
                     请把 sink.properties.dmfldr.field_seperator 换成数据里不出现的字符）；\
                     ② 值恰好等于空值字面量 '{}'；③ 与目标列类型不匹配。",
                    stats.rows_rejected, stats.rows_skipped, p.field_seperator, p.null_str
                ),
            ));
        }

        info!(rows = stats.rows_read, dir = %dir.display(), "达梦 IN 快载完成");
        // 成功即清理：数据文件里可能是整表的原文，留着没有意义
        proc::cleanup_dir(&dir);
        Ok(stats.rows_read)
    }
}

/// `task_thread_number` 的缺省语义（与读侧一致：0 = 自动 → 按 CPU 核数，上限 16）
fn effective_threads(n: u32) -> u32 {
    if n > 0 {
        return n;
    }
    std::thread::available_parallelism()
        .map(|v| v.get() as u32)
        .unwrap_or(4)
        .min(16)
}

/// 把一批数据渲染成 dmfldr IN 吃得下的文本
///
/// **值原样写**（不转义，理由见模块文档），并在这里守住那条不可协商的边界：
/// 值与分隔符/换行/空值字面量撞车时**失败**，而不是写进去赌一把。
fn render_data(
    batch: &arrow::array::RecordBatch,
    sep: &str,
    null_str: &str,
    charset: CharacterCode,
) -> D2DResult<String> {
    use crate::sink::method::odbc::arrow_value_to_text;

    let ncols = batch.num_columns();
    let mut out = String::with_capacity(batch.num_rows() * ncols * 8);
    for r in 0..batch.num_rows() {
        for c in 0..ncols {
            if c > 0 {
                out.push_str(sep);
            }
            match arrow_value_to_text(batch.column(c).as_ref(), r) {
                None => out.push_str(null_str),
                Some(v) => {
                    if v.contains(sep) {
                        return Err(collision_err(
                            &batch.schema().field(c).name().clone(),
                            &v,
                            sep,
                            "列分隔符",
                            "sink.properties.dmfldr.field_seperator",
                        ));
                    }
                    if v.contains('\n') || v.contains('\r') {
                        return Err(collision_err(
                            &batch.schema().field(c).name().clone(),
                            &v,
                            "换行",
                            "换行符",
                            "（dmfldr 按 \\n 切行，值里不能带换行）",
                        ));
                    }
                    if v == null_str {
                        return Err(collision_err(
                            &batch.schema().field(c).name().clone(),
                            &v,
                            null_str,
                            "空值字面量",
                            "sink.properties.dmfldr.null_str",
                        ));
                    }
                    out.push_str(&v);
                }
            }
        }
        out.push('\n');
    }
    let _ = charset; // 编码由控制文件声明，字节原样落盘
    Ok(out)
}

fn collision_err(col: &str, value: &str, hit: &str, what: &str, fix: &str) -> D2DError {
    D2DError::external(
        "dmfldr",
        format!(
            "达梦 IN 快载拒绝这一批：列 '{}' 的值里出现了{} {:?}（值片段: {:?}）。\
             dmfldr **先按分隔符切行、再谈转义**，所以这类值无法表达 —— \
             写进去只会静默错位。出路：改 {}；\
             或改用 sink.method: odbc（绑定参数，不经文本）。",
            col,
            what,
            hit,
            truncate(value, 40),
            fix
        ),
    )
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut end = n;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// 列名（加引号）+ 每列的时间格式
///
/// 时间列**必须**声明 `DATE FORMAT`：否则 dmfldr 用默认格式解释，
/// 而我们写的是 `YYYY-MM-DD HH24:MI:SS.FF6`（微秒），默认格式下微秒会被丢掉。
fn columns_of(batch: &arrow::array::RecordBatch) -> (Vec<String>, Vec<Option<&'static str>>) {
    let mut names = Vec::with_capacity(batch.num_columns());
    let mut fmts = Vec::with_capacity(batch.num_columns());
    for f in batch.schema().fields() {
        names.push(d2d_connector::quote_ident(f.name()));
        fmts.push(date_format_for(f.data_type()));
    }
    (names, fmts)
}

/// Arrow 类型 → 控制文件的 DATE FORMAT 元素
///
/// 只给"必须声明否则会丢精度"的那些：时间戳（微秒）与日期。
/// 其余（含 `Time32/64`）交给 dmfldr 默认解释 —— 我们的文本形态就是它的常规形态。
fn date_format_for(dt: &arrow::datatypes::DataType) -> Option<&'static str> {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Timestamp(_, _) => Some("YYYY-MM-DD HH24:MI:SS.FF6"),
        DataType::Date32 | DataType::Date64 => Some("YYYY-MM-DD"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch_of(cols: Vec<(&str, ArrayRef)>) -> arrow::array::RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, c)| Field::new(*n, c.data_type().clone(), true))
            .collect();
        let data: Vec<ArrayRef> = cols.into_iter().map(|(_, c)| c).collect();
        arrow::array::RecordBatch::try_new(Arc::new(Schema::new(fields)), data).unwrap()
    }

    #[test]
    fn test_render_data_shape_and_null_literal() {
        let b = batch_of(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2]))),
            ("name", Arc::new(StringArray::from(vec![Some("a"), None]))),
        ]);
        let t = render_data(&b, "|", "NULL", CharacterCode::Utf8).unwrap();
        assert_eq!(
            t, "1|a\n2|NULL\n",
            "NULL 要写成空值字面量（NULL_MODE=TRUE 才认得）"
        );
    }

    /// **这条是这一版的全部意义**：值里含分隔符必须**失败**，不能写进去。
    ///
    /// dmfldr 先按分隔符切行、再谈转义，所以 `a|b` 会变成两列 ——
    /// 写进去就是静默错位。实测过：声明 ESCAPE 也救不了（那行会被拒绝）。
    #[test]
    fn test_value_with_separator_is_refused_not_written() {
        let b = batch_of(vec![("name", Arc::new(StringArray::from(vec!["a|b"])))]);
        let e = render_data(&b, "|", "NULL", CharacterCode::Utf8)
            .unwrap_err()
            .to_string();
        assert!(e.contains("分隔符"), "{}", e);
        assert!(e.contains("field_seperator"), "要给出出路: {}", e);
        assert!(e.contains("odbc"), "要提到另一条路: {}", e);
    }

    /// 值里含换行同样无法表达（dmfldr 按 `\n` 切行）
    #[test]
    fn test_value_with_newline_is_refused() {
        let b = batch_of(vec![("v", Arc::new(StringArray::from(vec!["a\nb"])))]);
        let e = render_data(&b, "|", "NULL", CharacterCode::Utf8)
            .unwrap_err()
            .to_string();
        assert!(e.contains("换行"), "{}", e);
    }

    /// 值**恰好等于**空值字面量：写进去会被读成 NULL（静默改成空值），必须拦
    #[test]
    fn test_value_equal_to_null_literal_is_refused() {
        let b = batch_of(vec![("v", Arc::new(StringArray::from(vec!["NULL"])))]);
        let e = render_data(&b, "|", "NULL", CharacterCode::Utf8)
            .unwrap_err()
            .to_string();
        assert!(e.contains("空值字面量"), "{}", e);
    }

    /// 反斜杠是**正常字符**（不设 ESCAPE 就没有转义规则）—— 不能误伤
    #[test]
    fn test_backslash_is_ordinary() {
        let b = batch_of(vec![("v", Arc::new(StringArray::from(vec!["a\\b"])))]);
        let t = render_data(&b, "|", "NULL", CharacterCode::Utf8).unwrap();
        assert_eq!(t, "a\\b\n");
    }

    /// 换了分隔符之后，原来会撞车的值就合法了 —— 出路是真的
    #[test]
    fn test_changing_separator_is_a_real_way_out() {
        let b = batch_of(vec![("v", Arc::new(StringArray::from(vec!["a|b"])))]);
        let t = render_data(&b, "\u{1}", "NULL", CharacterCode::Utf8).unwrap();
        assert_eq!(t, "a|b\n");
    }

    /// 时间列必须带 DATE FORMAT，否则微秒会被默认格式吃掉
    #[test]
    fn test_timestamp_column_declares_date_format() {
        let arr = TimestampMicrosecondArray::from(vec![0i64]);
        let b = batch_of(vec![("ts", Arc::new(arr))]);
        let (names, fmts) = columns_of(&b);
        assert_eq!(names, vec!["\"ts\""]);
        assert_eq!(fmts, vec![Some("YYYY-MM-DD HH24:MI:SS.FF6")]);
    }

    #[test]
    fn test_validate_rejects_bad_params() {
        let mut p = FlDrInParams::default();
        assert!(p.validate().is_ok());
        p.field_seperator = String::new();
        assert!(p.validate().is_err(), "空分隔符会让整份数据挤成一列");
        p = FlDrInParams {
            buffer_node_size: 0,
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }

    /// 未知字段（拼错的旋钮）要在**解析配置时**失败，而不是被静默忽略
    #[test]
    fn test_unknown_param_is_rejected() {
        let props = serde_json::json!({"dmfldr": {"field_sep": "|"}});
        let e = FlDrInParams::from_props(props.as_object().unwrap())
            .unwrap_err()
            .to_string();
        assert!(e.contains("field_sep"), "{}", e);
    }

    /// `direct` 缺省必须是 **false** —— dmfldr 自己的缺省是 TRUE，而 TRUE 不检查约束
    #[test]
    fn test_direct_is_opt_in() {
        assert!(!FlDrInParams::default().direct, "缺省必须是安全路径");
        let empty = serde_json::Map::new();
        assert!(!FlDrInParams::from_props(&empty).unwrap().direct);
        // 只有显式写 true 才走快路
        let mut props = serde_json::Map::new();
        props.insert("dmfldr".into(), serde_json::json!({"direct": true}));
        assert!(FlDrInParams::from_props(&props).unwrap().direct);
        // 拼错字段名要报错（deny_unknown_fields），不能静默当成 false
        props.insert("dmfldr".into(), serde_json::json!({"direkt": true}));
        assert!(FlDrInParams::from_props(&props).is_err());
    }
}
