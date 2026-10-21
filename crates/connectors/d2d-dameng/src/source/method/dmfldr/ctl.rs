//! dmfldr 控制文件生成（**OUT / IN 共用一套语法**）
//!
//! 产出语法（手册 3.1）：
//! ```text
//! OPTIONS ( <key> = <value> ... )
//! LOAD DATA
//! INFILE '<path>' STR X '0A'
//! INTO TABLE "SCHEMA"."TABLE"
//! FIELDS '<sep>'
//! ( col1, col2 DATE FORMAT '...', ... )
//! ```
//!
//! 手册明载「OUT 模式控制文件与 IN 格式相同」，实测也成立 ——
//! 所以**两个方向共用这里**：`render_control_file` 是源端（读库落盘），
//! [`render_in_control_file`] 是目标端（把文本灌进库）。分开写两份的话，
//! 那些用血换来的转义规则（`quote`）迟早会各自演化。
//!
//! 说明：
//! - 行分隔符固定 `X '0A'`（LF），跨平台确定；解析端按 LF 切行（兼容尾部 CR）
//! - 时间列由契约类型生成 `DATE FORMAT`（见 `fmt.rs`），杜绝默认格式丢精度
//! - 永不生成 `WIDTH`（定宽导出会截断数据，设计禁用项）

use super::params::{CharacterCode, FlDrParams};
use crate::source::method::dmfldr::fmt::dm_export_format;
use crate::source::method::DamengReadContext;
use d2d_core::D2DResult;
use std::path::Path;

/// 生成 dmfldr OUT 控制文件内容
pub fn render_control_file(
    ctx: &DamengReadContext,
    schema_name: Option<&str>,
    table: &str,
    data_file: &Path,
    params: &FlDrParams,
    task_threads: u32,
) -> String {
    let mut opts: Vec<(String, String)> = Vec::new();

    // ── 文本格式 ──
    opts.push((
        "CHARACTER_CODE".into(),
        quote(params.character_code.as_ctl()),
    ));
    opts.push(("NULL_MODE".into(), bool_lit(params.null_mode)));
    if params.null_mode {
        opts.push(("NULL_STR".into(), quote(&params.null_str)));
    }
    if let Some(esc) = &params.escape {
        if !esc.is_empty() {
            opts.push(("ESCAPE".into(), quote(esc)));
        }
    }
    opts.push(("COLNAME_HEADING".into(), bool_lit(params.colname_heading)));

    // ── 大字段 ──
    if let Some(dir) = &params.lob_directory {
        opts.push(("LOB_DIRECTORY".into(), quote(&dir.to_string_lossy())));
    }
    if params.lob_as_varchar {
        opts.push(("LOB_AS_VARCHAR".into(), bool_lit(true)));
        opts.push((
            "LOB_AS_VARCHAR_SIZE".into(),
            params.lob_as_varchar_size.to_string(),
        ));
    }

    // ── 性能参数 ──
    opts.push((
        "BUFFER_NODE_SIZE".into(),
        params.buffer_node_size.to_string(),
    ));
    opts.push(("READ_ROWS".into(), params.read_rows.to_string()));
    opts.push(("TASK_THREAD_NUMBER".into(), task_threads.to_string()));
    opts.push((
        "SEND_NODE_NUMBER".into(),
        params.send_node_number.to_string(),
    ));
    opts.push(("BDTA_SIZE".into(), params.bdta_size.to_string()));
    opts.push(("COMPRESS_FLAG".into(), bool_lit(params.compress_flag)));
    opts.push(("SINGLE_FILE".into(), bool_lit(params.single_file)));
    opts.push(("LOG_LEVEL".into(), params.log_level.to_string()));
    // 静默客户端进度输出（日志仍完整记录）
    opts.push(("SILENT".into(), bool_lit(true)));

    let opts_block = opts
        .iter()
        .map(|(k, v)| format!("  {} = {}", k, v))
        .collect::<Vec<_>>()
        .join("\n");

    // ── 列定义（时间列附加 DATE FORMAT）──
    let coldefs = ctx
        .column_names
        .iter()
        .zip(ctx.logical_types.iter())
        .map(|(name, lt)| match dm_export_format(lt) {
            Some(fmt) => format!("  {} DATE FORMAT '{}'", name, fmt),
            None => format!("  {}", name),
        })
        .collect::<Vec<_>>()
        .join(",\n");

    let into_table = match schema_name {
        Some(s) => format!(
            "\"{}\".\"{}\"",
            s.replace('"', "\"\""),
            table.replace('"', "\"\"")
        ),
        None => format!("\"{}\"", table.replace('"', "\"\"")),
    };

    format!(
        "-- D2D generated dmfldr control file (OUT + TABLE mode)\n\
         OPTIONS (\n{opts}\n)\n\
         LOAD DATA\n\
         INFILE {data} STR X '0A'\n\
         INTO TABLE {tbl}\n\
         {sep_clause}\n\
         (\n{cols}\n)\n",
        opts = opts_block,
        data = quote(&data_file.to_string_lossy()),
        tbl = into_table,
        sep_clause = field_separator_clause(&params.field_seperator),
        cols = coldefs,
    )
}

fn bool_lit(b: bool) -> String {
    if b {
        "TRUE".to_string()
    } else {
        "FALSE".to_string()
    }
}

/// dmfldr 字符串值：单引号封闭，内部**反斜杠与单引号都要转义**
///
/// 达梦控制文件的字符串字面量用反斜杠作转义符（实测，非文档明载）：
/// - `ESCAPE = '\'`     → 「控制文件语法分析出错」（`\'` 把结尾引号吃掉，串不闭合）
/// - `ESCAPE = '\\'`    → 通过
/// - `NULL_STR = 'N\ULL'` → 语法通过，但值被**静默改写成 `NULL`**（`\U` 被当转义）
/// - `INFILE '/p\x\'`   → 同样报语法错，说明文件路径走同一套词法
///
/// 因此含反斜杠的取值（最典型是 Windows 路径 `C:\data\out.txt`）必须写成 `\\`，
/// 否则轻则语法错、重则值被悄悄改写。反斜杠必须先于单引号处理。
fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// FIELDS 子句：单字节可打印字符按普通串；其余用十六进制 `X'..'`（手册 `<row_term_option>` 同构语法）
fn field_separator_clause(sep: &str) -> String {
    let bytes = sep.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii_graphic() {
        format!("FIELDS {}", quote(sep))
    } else {
        let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
        format!("FIELDS X '{}'", hex)
    }
}

/// 控制文件落盘
pub fn write_control_file(
    ctx: &DamengReadContext,
    schema_name: Option<&str>,
    table: &str,
    dir: &Path,
    data_file: &Path,
    params: &FlDrParams,
    task_threads: u32,
) -> D2DResult<std::path::PathBuf> {
    let content = render_control_file(ctx, schema_name, table, data_file, params, task_threads);
    let ctl_path = dir.join("d2d_fldr.ctl");
    std::fs::write(&ctl_path, content).map_err(|e| {
        d2d_core::D2DError::external(
            "dmfldr",
            format!("failed to write control file {}: {}", ctl_path.display(), e),
        )
    })?;
    Ok(ctl_path)
}

/// 目标端（IN）控制文件的输入
///
/// 与源端那份的差别**只有一处**：列宽/行数这些"怎么读文件"的选项换成"怎么灌进表"，
/// 而词法（`quote` / `field_separator_clause`）完全共用。
#[derive(Debug, Clone)]
pub struct InControlSpec<'a> {
    /// 目标表（**已经限定并加引号**，如 `"SCHEMA"."T"`）
    pub table_sql: &'a str,
    pub data_file: &'a Path,
    /// 列名（与 `date_formats` 逐列对齐）
    pub columns: &'a [String],
    /// 每列的时间格式；`None` = 该列不声明 DATE FORMAT（非时间列）
    pub date_formats: &'a [Option<&'a str>],
    pub field_sep: &'a str,
    /// `NULL_MODE=TRUE` 时的空值字面量
    pub null_str: &'a str,
    pub character_code: CharacterCode,
    pub buffer_node_size: u32,
    pub task_threads: u32,
    pub log_level: u8,
    /// `DIRECT`：走不走"直接写 B 树"的快路
    pub direct: bool,
}

/// 生成 dmfldr **IN** 控制文件内容（把文本灌进目标表）
///
/// ⚠️ **不设 `ESCAPE`** —— 实测（2026-10-16，dmfldr V8）：`ESCAPE='\'` 声明之后
/// `a\b` 能正确还原成 `a`，但**值里的分隔符写成 `\|` 会被直接拒绝**
/// （dmfldr 先按分隔符切行、再谈转义）。也就是说 ESCAPE 唯一能救的那件事它救不了，
/// 声明它只会让"写值"这一侧多一条必须记住的转义规则。
///
/// 所以本函数**不声明转义**，值由调用方**原样**写；值里含分隔符/换行时，
/// 调用方必须**大声失败**（见 `sink::method::dmfldr`），而不是写进去赌一把。
///
/// # `DIRECT` 必须显式写出来，缺省值**不安全**
///
/// dmfldr 的 `DIRECT` 缺省是 **TRUE**，手册原文：
/// 「通过数据的转换和数据的封装直接对 B 树进行操作……**但对于约束的检查等由用户保证，
/// dmfldr 将不处理有约束冲突的数据**」。
///
/// 实测（2026-10-17）：往主键表灌两行 `ID=1` ——
///
/// | `DIRECT` | 结果 |
/// |---|---|
/// | `TRUE`（缺省） | 两行都进去；随后 **UPDATE / DELETE / INSERT 全部失败**，只剩 SELECT |
/// | `FALSE` | 整批被拒：`违反表[…]唯一性约束条件`，表干净 |
///
/// 也就是说违反主键不会"多几行"，而是**把表写成不可更新的状态**。代价是装载慢
/// 4–5.4×（2M 行：有主键 1.55 → 8.43 s，无主键 0.70 → 2.88 s），但仍远快于 ODBC。
/// 所以缺省走 `FALSE`，要快路由用户显式开（`sink.properties.dmfldr.direct: true`）。
///
/// 另一件容易误会的事：**索引不是"不建"** —— `INDEX_OPTION` 缺省 1 会在装载完成后
/// 把数据按序插进二级索引。真正不检查的是约束。
pub fn render_in_control_file(spec: &InControlSpec<'_>) -> String {
    let opts: Vec<(String, String)> = vec![
        ("CHARACTER_CODE".into(), quote(spec.character_code.as_ctl())),
        ("NULL_MODE".into(), bool_lit(true)),
        ("NULL_STR".into(), quote(spec.null_str)),
        // 我方产出的数据文件**不写列头**（列序由控制文件的 coldef 声明）
        ("COLNAME_HEADING".into(), bool_lit(false)),
        ("BUFFER_NODE_SIZE".into(), spec.buffer_node_size.to_string()),
        ("TASK_THREAD_NUMBER".into(), spec.task_threads.to_string()),
        ("LOG_LEVEL".into(), spec.log_level.to_string()),
        ("SILENT".into(), bool_lit(true)),
        ("DIRECT".into(), bool_lit(spec.direct)),
    ];

    let opts_block = opts
        .iter()
        .map(|(k, v)| format!("  {} = {}", k, v))
        .collect::<Vec<_>>()
        .join(
            "
",
        );

    let coldefs = spec
        .columns
        .iter()
        .zip(spec.date_formats.iter())
        .map(|(name, fmt)| match fmt {
            Some(f) => format!("  {} DATE FORMAT '{}'", name, f),
            None => format!("  {}", name),
        })
        .collect::<Vec<_>>()
        .join(
            ",
",
        );

    format!(
        "-- D2D generated dmfldr control file (IN mode)
         OPTIONS (
{opts}
)
         LOAD DATA
         INFILE {data} STR X '0A'
         INTO TABLE {tbl}
         {sep_clause}
         (
{cols}
)
",
        opts = opts_block,
        data = quote(&spec.data_file.to_string_lossy()),
        tbl = spec.table_sql,
        sep_clause = field_separator_clause(spec.field_sep),
        cols = coldefs,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::method::DamengReadContext;
    use arrow::datatypes::{Field, Schema};
    use d2d_types::{FixedOffset, LogicalType, TemporalPrecision};
    use std::sync::Arc;

    fn ctx() -> DamengReadContext {
        let logical = vec![
            LogicalType::Int64,
            LogicalType::Utf8,
            LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None,
            },
            LogicalType::Decimal {
                precision: 19,
                scale: 4,
            },
            LogicalType::Timestamp {
                precision: TemporalPrecision::Milli,
                tz: Some(FixedOffset::cst()),
            },
        ];
        let names = ["id", "name", "cts", "amt", "cts_tz"];
        let fields: Vec<Field> = names
            .iter()
            .zip(logical.iter())
            .map(|(n, lt)| {
                Field::new(
                    *n,
                    d2d_types::LogicalArrowBridge::to_arrow(lt).unwrap(),
                    true,
                )
            })
            .collect();
        DamengReadContext {
            schema: Arc::new(Schema::new(fields)),
            logical_types: logical,
            column_names: names.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn test_render_control_file_structure() {
        let params = FlDrParams::default();
        let ctl = render_control_file(
            &ctx(),
            Some("PROD"),
            "t_a",
            Path::new("/data/out.txt"),
            &params,
            8,
        );
        assert!(ctl.contains("OPTIONS ("), "{}", ctl);
        assert!(ctl.contains("CHARACTER_CODE = 'UTF-8'"), "{}", ctl);
        assert!(ctl.contains("NULL_MODE = TRUE"), "{}", ctl);
        assert!(ctl.contains("NULL_STR = 'NULL'"), "{}", ctl);
        assert!(ctl.contains("COLNAME_HEADING = TRUE"), "{}", ctl);
        assert!(ctl.contains("BUFFER_NODE_SIZE = 64"), "{}", ctl);
        assert!(ctl.contains("TASK_THREAD_NUMBER = 8"), "{}", ctl);
        assert!(ctl.contains("SINGLE_FILE = TRUE"), "{}", ctl);
        assert!(ctl.contains("INFILE '/data/out.txt' STR X '0A'"), "{}", ctl);
        assert!(ctl.contains("INTO TABLE \"PROD\".\"t_a\""), "{}", ctl);
        assert!(ctl.contains("FIELDS '|'"), "{}", ctl);
    }

    #[test]
    fn test_render_date_formats_for_temporal_columns() {
        let ctl = render_control_file(
            &ctx(),
            None,
            "t",
            Path::new("/o.txt"),
            &FlDrParams::default(),
            4,
        );
        assert!(
            ctl.contains("cts DATE FORMAT 'YYYY-MM-DD HH24:MI:SS.FF6'"),
            "{}",
            ctl
        );
        assert!(
            ctl.contains("cts_tz DATE FORMAT 'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM'"),
            "{}",
            ctl
        );
        // 非时间列不带 FORMAT
        assert!(ctl.contains("  id,\n"), "{}", ctl);
        assert!(ctl.contains("  amt,\n"), "{}", ctl);
        // 永不生成 WIDTH
        assert!(!ctl.contains("WIDTH"), "{}", ctl);
    }

    #[test]
    fn test_hex_separator_for_non_printable() {
        let params = FlDrParams {
            field_seperator: "\u{1}".to_string(),
            ..Default::default()
        };
        let ctl = render_control_file(&ctx(), None, "t", Path::new("/o"), &params, 2);
        assert!(ctl.contains("FIELDS X '01'"), "{}", ctl);
    }

    #[test]
    fn test_escape_param_emitted() {
        let params = FlDrParams {
            escape: Some("*".into()),
            ..Default::default()
        };
        let ctl = render_control_file(&ctx(), None, "t", Path::new("/o"), &params, 2);
        assert!(ctl.contains("ESCAPE = '*'"), "{}", ctl);
    }

    /// 回归：默认转义符 `\` 必须写成 `'\\'`，否则达梦把 `\'` 当转义序列吃掉结尾引号
    /// → 「控制文件语法分析出错」（实测原样 `ESCAPE = '\'` 必然失败）
    #[test]
    fn test_backslash_escape_is_doubled() {
        let params = FlDrParams {
            escape: Some("\\".into()),
            ..Default::default()
        };
        let ctl = render_control_file(&ctx(), None, "t", Path::new("/o"), &params, 2);
        assert!(ctl.contains("ESCAPE = '\\\\'"), "{}", ctl);
        // 不得出现未转义的裸反斜杠引号形式
        assert!(!ctl.contains("ESCAPE = '\\'"), "{}", ctl);
    }

    /// 回归：Windows 路径的反斜杠同样在控制文件词法里被当转义符，必须翻倍
    #[test]
    fn test_windows_path_backslash_is_doubled() {
        let ctl = render_control_file(
            &ctx(),
            None,
            "t",
            Path::new(r"C:\d2d\work\out.txt"),
            &FlDrParams::default(),
            2,
        );
        assert!(
            ctl.contains(r"INFILE 'C:\\d2d\\work\\out.txt' STR X '0A'"),
            "{}",
            ctl
        );
    }

    // ───────── IN 控制文件：DIRECT 必须显式写出来 ─────────

    fn in_spec(direct: bool) -> String {
        let cols = vec!["ID".to_string(), "S".to_string()];
        let fmts: Vec<Option<&str>> = vec![None, None];
        render_in_control_file(&InControlSpec {
            table_sql: "\"APP\".\"T\"",
            data_file: std::path::Path::new("/tmp/data.txt"),
            columns: &cols,
            date_formats: &fmts,
            field_sep: "|",
            null_str: "NULL",
            character_code: CharacterCode::Utf8,
            buffer_node_size: 8,
            task_threads: 1,
            log_level: 3,
            direct,
        })
    }

    /// **缺省必须写 `DIRECT = FALSE`**，不能靠 dmfldr 自己的缺省
    ///
    /// dmfldr 的缺省是 TRUE，而 TRUE 不检查约束 —— 实测能把主键表写成不可更新的状态。
    /// 这条测试挂在控制文件这一层：只要有人把这一行删掉，dmfldr 就会悄悄退回
    /// 不检查约束的行为，而所有别的测试都还是会过。
    #[test]
    fn test_in_control_declares_direct_false() {
        let ctl = in_spec(false);
        assert!(
            ctl.contains("DIRECT = FALSE"),
            "必须显式声明，不能省：{}",
            ctl
        );
        // 顺手钉住"没有 ESCAPE"那条实测结论
        assert!(!ctl.contains("ESCAPE"), "{}", ctl);
    }

    #[test]
    fn test_in_control_declares_direct_true_when_opted_in() {
        assert!(in_spec(true).contains("DIRECT = TRUE"));
    }
}
