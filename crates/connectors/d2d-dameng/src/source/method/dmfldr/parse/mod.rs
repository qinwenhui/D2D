//! dmfldr 文本数据文件 → RecordBatch 解析器
//!
//! 输入：导出的分隔符文本（行分隔 LF，列分隔可配，escape 可选）
//! 输出：按契约 schema 组装的 RecordBatch 流（解析策略由 LogicalType 驱动）
//!
//! 核心语义（防数据错乱）：
//! - DECIMAL 按契约 precision/scale 校验，超出**报错**（不静默舍入）
//! - 时间列按契约精度解析（秒级以下由控制文件 FORMAT 保证导出，见 fmt.rs）
//! - BLOB/CLOB 字段是 LOB 文件引用 `文件名:偏移:长度`，按引用 seek 读回
//! - 每行字段数必须与契约列数一致，否则报错（列序漂移防护）
//! - UTF-8 严格校验，非法字节报错（不替换）

pub mod fields;

use self::fields::{read_logical_line, split_fields};
use super::params::{CharacterCode, FlDrParams};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use d2d_core::D2DResult;
use d2d_types::text_batch::{LobSource, NullRule};
use d2d_types::{LogicalType, TextEncoding};
use std::path::PathBuf;

// 单元格解析由 d2d_types::text_batch 提供（见 fields.rs 的模块文档）。
// 这里转出，调用方（`odbc.rs` / 本模块）不必改引用路径。
pub use d2d_types::text_batch::{parse_temporal_to_i64, RowBuildOptions, RowBuilder};

/// 解析选项（由 FlDrParams + 契约推导）
#[derive(Debug, Clone)]
pub struct TextParseOptions {
    pub field_sep: Vec<u8>,
    pub escape: Option<Vec<u8>>,
    pub null_mode: bool,
    pub null_str: String,
    pub encoding: CharacterCode,
    /// LOB 引用文件的目录（lob_directory 或数据文件所在目录）
    pub lob_dir: PathBuf,
}

impl TextParseOptions {
    pub fn from_params(params: &FlDrParams, data_file: &std::path::Path) -> Self {
        let lob_dir = params.lob_directory.clone().unwrap_or_else(|| {
            data_file
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."))
        });
        Self {
            field_sep: params.field_seperator.as_bytes().to_vec(),
            escape: params
                .escape
                .as_ref()
                .filter(|s| !s.is_empty())
                .map(|s| s.as_bytes().to_vec()),
            null_mode: params.null_mode,
            null_str: params.null_str.clone(),
            encoding: params.character_code,
            lob_dir,
        }
    }

    /// SQL 模式（`export_mode: partitioned`，不生成控制文件）的解析选项。
    ///
    /// 没有控制文件时 dmfldr 一律走缺省：列分隔符 `|`、空值导出为空串（不等价于
    /// NULL_MODE 的标记串）、不启用转义。解析端必须按同一套缺省来，否则字段切分与
    /// 空值判定都会错位。这些约束在 `FlDrParams::validate` 里做前置校验。
    pub fn sql_mode_defaults(data_file: &std::path::Path, encoding: CharacterCode) -> Self {
        let lob_dir = data_file
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            field_sep: b"|".to_vec(),
            escape: None,
            null_mode: false,
            null_str: String::new(),
            encoding,
            lob_dir,
        }
    }
}

/// [`TextParseOptions`] → 共享行构建器的选项
///
/// 这一层转换就是 dmfldr 与 D2S 其余读取路径的**全部差异**：
/// 编码怎么解、什么算 NULL、LOB 是文件引用还是内联字节。
/// 再往下（"这列是 DECIMAL(19,4) 该怎么解析"）就是同一份代码了。
impl From<&TextParseOptions> for RowBuildOptions {
    fn from(o: &TextParseOptions) -> Self {
        Self {
            encoding: TextEncoding::from(o.encoding),
            // NULL_MODE=true → 只有标记串算空值；false → 空串即空值（Oracle 语义）
            null: if o.null_mode {
                NullRule::Marker(o.null_str.clone())
            } else {
                NullRule::EmptyIsNull
            },
            // dmfldr 的 LOB 列是文件引用，按 `文件名:偏移:长度` 从 lob_dir 读回
            lob: LobSource::FileRef(o.lob_dir.clone()),
        }
    }
}

/// 分块解析器：对一段文件字节范围做流式行解析
pub struct ChunkParser<'a> {
    schema: &'a SchemaRef,
    /// 与 schema.fields 逐列对齐的逻辑类型（解析策略）
    logical: &'a [LogicalType],
    opts: &'a TextParseOptions,
    /// 自本分块起点起已消费的字节数（跨多次 parse_rows 调用累计），仅用于报错定位
    consumed_total: std::sync::atomic::AtomicU64,
}

impl<'a> ChunkParser<'a> {
    pub fn new(
        schema: &'a SchemaRef,
        logical: &'a [LogicalType],
        opts: &'a TextParseOptions,
    ) -> Self {
        Self {
            schema,
            logical,
            opts,
            consumed_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 解析最多 max_rows 行，返回 (RecordBatch, 行数)
    ///
    /// - `skip_header`：首行是列头（导出时已校验），直接跳过
    /// - 空行（纯换行）跳过；返回行数 0 表示 EOF
    pub fn parse_rows<R: std::io::BufRead>(
        &self,
        reader: &mut R,
        max_rows: usize,
        skip_header: bool,
    ) -> D2DResult<(RecordBatch, u64)> {
        let ncols = self.schema.fields().len();
        let mut rb = RowBuilder::new(
            self.schema.clone(),
            self.logical.to_vec(),
            RowBuildOptions::from(self.opts),
            max_rows.max(1),
        )?;

        let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
        let mut line_no: u64 = 0;
        let mut rows: u64 = 0;
        let mut first = true;

        loop {
            // 上限检查在读行**之前**：避免为了凑满 max_rows 而预消费下一行
            if rows as usize >= max_rows {
                break;
            }
            line_buf.clear();
            let n = read_logical_line(reader, self.opts.escape.as_deref(), &mut line_buf).map_err(
                |e| d2d_core::D2DError::external("dmfldr", format!("read line failed: {}", e)),
            )?;
            if n == 0 {
                break; // EOF
            }
            line_no += 1;
            // 本行在分块内的起始偏移（用于报错定位到文件绝对偏移）
            let chunk_off = self
                .consumed_total
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);

            // 去行尾换行（容忍 CRLF）
            let raw_len = line_buf.len();
            let had_lf = line_buf.last() == Some(&b'\n');
            let mut line: &[u8] = &line_buf;
            if line.last() == Some(&b'\n') {
                line = &line[..line.len() - 1];
            }
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }

            if first && skip_header {
                first = false;
                continue; // 列头已在导出期校验
            }
            first = false;

            if line.is_empty() {
                continue; // 空行（文件尾换行）跳过
            }

            let fields = split_fields(line, &self.opts.field_sep, self.opts.escape.as_deref());
            if fields.len() != ncols {
                // 带上「行长」与「分隔符个数」：这两项能立刻区分两种失败模式——
                // 行长偏小 ⇒ 读到的是残行（分块/EOF 问题）；行长正常但分隔符少 ⇒ 切分逻辑问题。
                let sep_cnt = match self.opts.field_sep.first() {
                    Some(&b) => line.iter().filter(|&&x| x == b).count(),
                    None => 0,
                };
                return Err(d2d_core::D2DError::schema(format!(
                    "解析失败：第 {} 行 列 '<row>'：字段数 {} != 契约列数 {}                      [line_bytes={}, sep_count={}, raw_len={}, had_lf={}, chunk_off={}]",
                    line_no, fields.len(), ncols, line.len(), sep_cnt, raw_len, had_lf, chunk_off
                )));
            }

            // 类型规则、空值判定、DECIMAL 校验全部交给共享的 RowBuilder ——
            // 与 ODBC、Doris MySQL 协议口用的是**同一份实现**，不是同一套约定各写一遍。
            let cells: Vec<Option<&[u8]>> = fields.iter().map(|f| Some(&f[..])).collect();
            rb.append_row_with_line(&cells, line_no as usize, line)?;
            rows += 1;
        }

        Ok((rb.finish()?, rows))
    }
}

#[cfg(test)]
mod tests {
    use super::super::params::FlDrParams;
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use d2d_types::TemporalPrecision;
    use std::sync::Arc;

    fn ctx_of(types: Vec<(&str, LogicalType)>) -> (SchemaRef, Vec<LogicalType>) {
        let logical: Vec<LogicalType> = types.iter().map(|(_, t)| t.clone()).collect();
        let names: Vec<&str> = types.iter().map(|(n, _)| *n).collect();
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
        (Arc::new(Schema::new(fields)), logical)
    }

    fn opts(params: &FlDrParams) -> TextParseOptions {
        TextParseOptions::from_params(params, std::path::Path::new("/data/out.txt"))
    }

    #[test]
    fn test_parse_basic_types() {
        let (schema, logical) = ctx_of(vec![
            ("id", LogicalType::Int64),
            ("name", LogicalType::Utf8),
            (
                "amt",
                LogicalType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            ),
            (
                "cts",
                LogicalType::Timestamp {
                    precision: TemporalPrecision::Micro,
                    tz: None,
                },
            ),
        ]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "id|name|amt|cts\n1|alice|12.34|2024-01-02 03:04:05.123456\n2|bob|NULL|NULL\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let (batch, rows) = parser.parse_rows(&mut r, 100, true).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.null_count(), 0);
        let amts = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(amts.value(0), 1234);
        assert!(amts.is_null(1));
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        let ts = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(
            ts.value(0),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_micro_opt(3, 4, 5, 123456)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        );
    }

    #[test]
    fn test_parse_header_skipped() {
        let (schema, logical) = ctx_of(vec![("id", LogicalType::Int64)]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "id\n7\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let (batch, rows) = parser.parse_rows(&mut r, 100, true).unwrap();
        assert_eq!(rows, 1);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
    }

    #[test]
    fn test_parse_field_count_mismatch() {
        let (schema, logical) = ctx_of(vec![("a", LogicalType::Int64), ("b", LogicalType::Int64)]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "1|2|3\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let err = parser
            .parse_rows(&mut r, 100, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("字段数"), "{}", err);
        // 切分失败是 dmfldr **这一层**的问题，所以诊断信息也得由这一层给：
        // 行长 / 分隔符个数能区分"读到残行"与"切分逻辑错"两种失败模式
        assert!(err.contains("sep_count"), "要带上切分诊断: {}", err);
    }

    #[test]
    fn test_parse_decimal_overflow_rejected() {
        let (schema, logical) = ctx_of(vec![(
            "amt",
            LogicalType::Decimal {
                precision: 5,
                scale: 2,
            },
        )]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        // 小数位超过 scale → 拒绝（不静默舍入）
        let data = "1.234\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let err = parser.parse_rows(&mut r, 100, false).unwrap_err();
        assert!(err.to_string().contains("scale"), "{}", err);
    }

    #[test]
    fn test_parse_int_overflow_rejected() {
        let (schema, logical) = ctx_of(vec![("id", LogicalType::Int8)]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "999\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let err = parser
            .parse_rows(&mut r, 100, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("越界"), "{}", err);
        // 类型规则在 d2d_types::text_batch，但**原始行文本**只有这一层拿得到 ——
        // 报错里必须带上它，否则用户不知道去 8.9GB 文件的哪一行看
        assert!(err.contains("999"), "报错要带原文: {}", err);
    }

    #[test]
    fn test_parse_date_and_tz_timestamp() {
        let (schema, logical) = ctx_of(vec![
            ("d", LogicalType::Date32),
            (
                "t",
                LogicalType::Timestamp {
                    precision: TemporalPrecision::Milli,
                    tz: Some(d2d_types::FixedOffset::cst()),
                },
            ),
        ]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "2024-03-05|2024-03-05 06:07:08.123+08:00\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let (batch, rows) = parser.parse_rows(&mut r, 100, false).unwrap();
        assert_eq!(rows, 1);
        let d = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .unwrap();
        assert_eq!(
            d.value(0),
            (chrono::NaiveDate::from_ymd_opt(2024, 3, 5).unwrap()
                - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32
        );
        let t = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .unwrap();
        // +08:00 06:07:08.123 = UTC 22:07:08.123（前一天）
        assert_eq!(
            t.value(0),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 4)
                .unwrap()
                .and_hms_milli_opt(22, 7, 8, 123)
                .unwrap()
                .and_utc()
                .timestamp_millis()
        );
    }

    #[test]
    fn test_parse_lob_reference() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dmfldr.lob"), b"HELLO-WORLD").unwrap();
        let (schema, logical) = ctx_of(vec![("b", LogicalType::LargeBinary)]);
        let params = FlDrParams {
            lob_directory: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let o = TextParseOptions::from_params(&params, std::path::Path::new("/data/out.txt"));
        let parser = ChunkParser::new(&schema, &logical, &o);
        let data = "dmfldr.lob:6:5\n";
        let mut r = std::io::BufReader::new(data.as_bytes());
        let (batch, rows) = parser.parse_rows(&mut r, 100, false).unwrap();
        assert_eq!(rows, 1);
        let b = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(b.value(0), b"WORLD");
    }

    #[test]
    fn test_parse_escape_and_crlf() {
        let (schema, logical) = ctx_of(vec![("s", LogicalType::Utf8)]);
        let params = FlDrParams {
            escape: Some("\\".into()),
            field_seperator: "|".into(),
            ..Default::default()
        };
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        // "a\|b" → 字段值 "a|b"；行尾 CRLF
        let data = b"a\\|b\r\n";
        let mut r = std::io::BufReader::new(&data[..]);
        let (batch, rows) = parser.parse_rows(&mut r, 100, false).unwrap();
        assert_eq!(rows, 1);
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "a|b");
    }

    #[test]
    fn test_parse_batching_respects_max_rows() {
        let (schema, logical) = ctx_of(vec![("id", LogicalType::Int64)]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let mut data = String::new();
        for i in 0..25 {
            data.push_str(&format!("{}\n", i));
        }
        let mut r = std::io::BufReader::new(data.as_bytes());
        let (b1, n1) = parser.parse_rows(&mut r, 10, false).unwrap();
        assert_eq!(n1, 10);
        let (b2, n2) = parser.parse_rows(&mut r, 10, false).unwrap();
        assert_eq!(n2, 10);
        let (b3, n3) = parser.parse_rows(&mut r, 10, false).unwrap();
        assert_eq!(n3, 5);
        assert_eq!(b1.num_rows() + b2.num_rows() + b3.num_rows(), 25);
    }

    #[test]
    fn test_parse_empty_file() {
        let (schema, logical) = ctx_of(vec![("id", LogicalType::Int64)]);
        let params = FlDrParams::default();
        let o = opts(&params);
        let parser = ChunkParser::new(&schema, &logical, &o);
        let mut r = std::io::BufReader::new("".as_bytes());
        let (batch, rows) = parser.parse_rows(&mut r, 100, false).unwrap();
        assert_eq!(rows, 0);
        assert_eq!(batch.num_rows(), 0);
    }
}
