//! **文本单元格 → Arrow 批次** —— 全仓唯一一份"按逻辑类型解析文本"的规则
//!
//! # 为什么它在这里，而不是某个连接器里
//!
//! 它原本长在 `d2d-dameng` 的 dmfldr 解析模块里。ODBC 那条路要用时，
//! 当时的做法是把它提出来在连接器内共用（`RowBuildOptions` 的注释记了这件事）。
//! 到 Doris Source 要用第三次时，"换个连接器就再抄一遍"已经不成立了 ——
//! 于是它上移到类型层：**"这列是 DECIMAL(19,4)，什么样的文本算合法"是类型问题，
//! 不是某个库的问题。**
//!
//! 三次踩过的代价：dmfldr 早期"导出按秒级、解析按微秒"两套格式各自演化，
//! 出现过字段错位（见 `dmfldr/fmt.rs` 的「单向真相原则」）。规则可以有一套，
//! 就不要再有第二套。
//!
//! # 谁在用
//!
//! | 调用方 | 单元格从哪来 | LOB |
//! |---|---|---|
//! | dmfldr 文本解析 | 导出文件的行，按分隔符切开 | [`LobSource::FileRef`] |
//! | 达梦 ODBC 游标 | `TextRowSet` 批次 | [`LobSource::Unsupported`] |
//! | Doris MySQL 协议口 | 结果行（文本协议，全为字节串） | [`LobSource::Inline`] |
//!
//! # 契约
//!
//! 对外只有三件事：建、追加一行、收尾。一行的形态是 `&[Option<&[u8]>]`，
//! `None` = SQL NULL、`Some(b"")` = 空串 —— **这两者语义不同，调用方不能混**
//! （ODBC 的游标天然区分它们，别在适配时抹平）。
//!
//! 解析一律**严格**：DECIMAL 小数位超 scale、整数越界、UTF-8 非法字节、
//! 时间不匹配格式，全部报错而不是静默截断或替换。理由同 [`crate::LogicalArrowBridge`]：
//! 迁移工具最贵的 bug 是"跑完了、数字不对、没人知道"。

use crate::{LogicalType, TemporalPrecision};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Int64Builder, LargeBinaryBuilder, LargeStringBuilder,
    NullBuilder, PrimitiveBuilder, StringBuilder,
};
use arrow::datatypes::{
    ArrowTimestampType, DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use d2d_core::{D2DError, D2DResult};
use std::path::PathBuf;
use std::sync::Arc;

// ─────────────────────────────── 选项 ───────────────────────────────

/// 文本字节的编码
///
/// 来自 dmfldr 的 `CHARACTER_CODE` 选项（它也是达梦库本身的字符集）。
/// 数值/时间字段恒为 ASCII，只有字符串列会真正走到这里。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TextEncoding {
    /// UTF-8（**严格校验**：非法字节报错，不替换）
    #[default]
    Utf8,
    /// 单字节：按 Latin-1 逐字节映射（可逆、无替换歧义）
    SingleByte,
    Gbk,
    Gb18030,
    EucKr,
}

/// LOB 列的单元格**装的是什么** —— 三种形态，不是三种"支持程度"
///
/// 这个枚举存在的原因是：同一个 `LogicalType::LargeBinary`，在三条读取路径上
/// 单元格的含义完全不同。把它们都塞进一个 `Option<PathBuf>` 会得到"用空路径
/// 去 open 文件"这种把**设计缺陷**伪装成**运行期故障**的错误。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LobSource {
    /// 单元格是 LOB 文件引用 `文件名:偏移:长度`，按引用从该目录 seek 读回
    ///
    /// dmfldr 的导出物形态（手册 2.3）。
    FileRef(PathBuf),
    /// 单元格字节**就是**值本身，不需要旁路读文件
    ///
    /// Doris 的 MySQL 协议口给的就是字节。
    Inline,
    /// 本读取路径**无法忠实承载 LOB** —— 遇到即报错
    ///
    /// 达梦 ODBC 的 `TextRowSet` 会把 BLOB 截断到列宽（驱动上报的宽度），
    /// 读回来的字节是残的。静默写坏数据比大声失败糟得多，所以这里显式拒绝：
    /// 要迁 BLOB，请换 dmfldr 路径，或先在库里转成 Base64 的字符串列。
    #[default]
    Unsupported,
}

/// **什么算 NULL** —— 这不是格式偏好，是两个数据库的语义差异
///
/// 曾经这里只有一对 `(null_mode: bool, null_str: String)`，于是"空串算不算 NULL"
/// 被悄悄绑死在 dmfldr 的 `NULL_MODE` 上。加 Doris 时才发现这个绑法是**会写坏数据的**：
///
/// | 库 | `''` 是什么 | 依据 |
/// |---|---|---|
/// | 达梦 / Oracle | **就是 NULL** | Oracle 语义，达梦兼容之 |
/// | Doris / MySQL | 一个长度 0 的**真值** | 与 NULL 严格区分 |
///
/// 拿达梦那套去读 Doris，每一个空串都会变成 NULL —— 行数对得上、列数对得上、
/// 跑完退出码 0，只有值错了。所以判定规则必须在**连接器建选项时**说清楚，
/// 不能靠一个布尔位的默认值。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NullRule {
    /// 空串即 NULL（Oracle 语义；也是 dmfldr `NULL_MODE=false` 的约定）
    #[default]
    EmptyIsNull,
    /// 只有这个标记串算 NULL，空串是**真的空串**（dmfldr `NULL_MODE=true`）
    Marker(String),
    /// 空值只由协议自己表达（`None`），**空串就是空串**（MySQL / Doris 语义）
    ExplicitOnly,
}

impl NullRule {
    /// 这个字段是不是 NULL
    fn matches(&self, field: &[u8]) -> bool {
        match self {
            Self::EmptyIsNull => field.is_empty(),
            Self::Marker(s) => !s.is_empty() && field.eq_ignore_ascii_case(s.as_bytes()),
            Self::ExplicitOnly => false,
        }
    }
}

/// 单元格解析选项
#[derive(Debug, Clone, Default)]
pub struct RowBuildOptions {
    pub encoding: TextEncoding,
    pub null: NullRule,
    pub lob: LobSource,
}

impl RowBuildOptions {
    /// **达梦口径**：空串即 NULL（Oracle 语义），LOB 走文件引用，UTF-8
    ///
    /// 达梦 ODBC 游标用这个。BLOB 走 [`LobSource::Unsupported`] ——
    /// 它的文本行集承载不了 LOB，见该枚举的说明。
    pub fn dameng_default() -> Self {
        Self {
            encoding: TextEncoding::Utf8,
            null: NullRule::EmptyIsNull,
            lob: LobSource::Unsupported,
        }
    }

    /// **MySQL/Doris 口径**：空串是空串，NULL 只由协议表达，LOB 就是字节本身
    pub fn mysql_protocol() -> Self {
        Self {
            encoding: TextEncoding::Utf8,
            null: NullRule::ExplicitOnly,
            lob: LobSource::Inline,
        }
    }
}

// ─────────────────────────────── 行构建器 ───────────────────────────────

/// **逐行**按逻辑类型构建 RecordBatch
///
/// `logical` 与 `schema.fields()` **逐列对齐**，且长度必须一致 ——
/// 调用方拼错列序时在这里就失败，而不是产出一批列名对、值错位的批次。
pub struct RowBuilder {
    schema: SchemaRef,
    logical: Vec<LogicalType>,
    cols: Vec<ColBuilder>,
    opts: RowBuildOptions,
    rows: u64,
}

impl RowBuilder {
    pub fn new(
        schema: SchemaRef,
        logical: Vec<LogicalType>,
        opts: RowBuildOptions,
        capacity: usize,
    ) -> D2DResult<Self> {
        let ncols = schema.fields().len();
        if logical.len() != ncols {
            return Err(D2DError::schema(format!(
                "行构建器：schema 列数 {} 与逻辑类型数 {} 不一致",
                ncols,
                logical.len()
            )));
        }
        let cols = schema
            .fields()
            .iter()
            .map(|f| ColBuilder::new(f.data_type(), capacity.max(1)))
            .collect::<D2DResult<Vec<_>>>()?;
        Ok(Self {
            schema,
            logical,
            cols,
            opts,
            rows: 0,
        })
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// 追加一行。`fields[i] == None` 表示 SQL NULL。
    ///
    /// `row_no` 只用于报错定位（dmfldr 传行号、ODBC 传"批内行号 + 累计偏移"）。
    /// 想在报错里带上原始行文本，用 [`Self::append_row_with_line`]。
    pub fn append_row(&mut self, fields: &[Option<&[u8]>], row_no: u64) -> D2DResult<()> {
        self.append_row_ctx(fields, row_no as usize, b"")
    }

    /// 追加一行，并把**原始行文本**留在报错上下文里（dmfldr 用，ODBC/Doris 给不出整行）
    pub fn append_row_with_line(
        &mut self,
        fields: &[Option<&[u8]>],
        line_no: usize,
        raw_line: &[u8],
    ) -> D2DResult<()> {
        self.append_row_ctx(fields, line_no, raw_line)
    }

    fn append_row_ctx(
        &mut self,
        fields: &[Option<&[u8]>],
        line_no: usize,
        raw_line: &[u8],
    ) -> D2DResult<()> {
        let ncols = self.schema.fields().len();
        if fields.len() != ncols {
            return Err(D2DError::schema(format!(
                "行构建器：本行列数 {} != 目标列数 {}（第 {} 行）",
                fields.len(),
                ncols,
                line_no
            )));
        }
        for (i, field) in fields.iter().enumerate() {
            let col_name = self.schema.field(i).name().clone();
            match field {
                None => self.cols[i].append_null(),
                // dmfldr 的 NULL 是**字面量**（NULL_MODE），ODBC/Doris 的 NULL 是 None
                // —— 两条路都收，判定规则在 is_null_field 一处
                Some(b) if self.opts.null.matches(b) => {
                    self.cols[i].append_null()
                }
                Some(b) => {
                    append_cell(
                        &self.opts,
                        &mut self.cols[i],
                        b,
                        &col_name,
                        &self.logical[i],
                        line_no,
                        raw_line,
                    )?;
                }
            }
        }
        self.rows += 1;
        Ok(())
    }

    pub fn finish(self) -> D2DResult<RecordBatch> {
        if self.rows == 0 {
            return Ok(RecordBatch::new_empty(self.schema));
        }
        let arrays: Vec<ArrayRef> = self
            .cols
            .into_iter()
            .zip(self.schema.fields().iter())
            .map(|(b, f)| b.finish(f.data_type()))
            .collect::<D2DResult<_>>()?;
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| D2DError::schema(format!("RecordBatch 组装失败: {}", e)))
    }
}

/// 逐列 Arrow builder（按 schema DataType 构建；Time64 Second/Milli 走 Int64+cast）
enum ColBuilder {
    Int8(PrimitiveBuilder<Int8Type>),
    Int16(PrimitiveBuilder<Int16Type>),
    Int32(PrimitiveBuilder<Int32Type>),
    Int64(PrimitiveBuilder<Int64Type>),
    UInt8(PrimitiveBuilder<UInt8Type>),
    UInt16(PrimitiveBuilder<UInt16Type>),
    UInt32(PrimitiveBuilder<UInt32Type>),
    UInt64(PrimitiveBuilder<UInt64Type>),
    Float32(PrimitiveBuilder<Float32Type>),
    Float64(PrimitiveBuilder<Float64Type>),
    Decimal128(PrimitiveBuilder<Decimal128Type>),
    Boolean(BooleanBuilder),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Binary(BinaryBuilder),
    LargeBinary(LargeBinaryBuilder),
    Date32(PrimitiveBuilder<Date32Type>),
    /// Time64 任何单位都先以 Int64 承载目标单位值，finish 时 cast
    Time64I64(Int64Builder),
    TsSecond(PrimitiveBuilder<TimestampSecondType>),
    TsMilli(PrimitiveBuilder<TimestampMillisecondType>),
    TsMicro(PrimitiveBuilder<TimestampMicrosecondType>),
    TsNano(PrimitiveBuilder<TimestampNanosecondType>),
    Null(NullBuilder),
}

/// 时间戳 builder：带时区语义（tz=None → 无时区数组，与 DataType 严格一致）
fn ts_builder<P: ArrowTimestampType>(
    capacity: usize,
    tz: &Option<Arc<str>>,
) -> PrimitiveBuilder<P> {
    PrimitiveBuilder::<P>::with_capacity(capacity).with_timezone_opt(tz.as_deref())
}

impl ColBuilder {
    fn new(dt: &DataType, capacity: usize) -> D2DResult<Self> {
        Ok(match dt {
            DataType::Int8 => Self::Int8(PrimitiveBuilder::<Int8Type>::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(PrimitiveBuilder::<Int16Type>::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(PrimitiveBuilder::<Int32Type>::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(PrimitiveBuilder::<Int64Type>::with_capacity(capacity)),
            DataType::UInt8 => Self::UInt8(PrimitiveBuilder::<UInt8Type>::with_capacity(capacity)),
            DataType::UInt16 => Self::UInt16(PrimitiveBuilder::<UInt16Type>::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(PrimitiveBuilder::<UInt32Type>::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(PrimitiveBuilder::<UInt64Type>::with_capacity(capacity)),
            DataType::Float32 => {
                Self::Float32(PrimitiveBuilder::<Float32Type>::with_capacity(capacity))
            }
            DataType::Float64 => {
                Self::Float64(PrimitiveBuilder::<Float64Type>::with_capacity(capacity))
            }
            DataType::Decimal128(p, s) => Self::Decimal128(
                PrimitiveBuilder::<Decimal128Type>::with_capacity(capacity)
                    .with_data_type(DataType::Decimal128(*p, *s)),
            ),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            DataType::LargeUtf8 => {
                Self::LargeUtf8(LargeStringBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16)),
            DataType::LargeBinary => {
                Self::LargeBinary(LargeBinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            DataType::Date32 => Self::Date32(PrimitiveBuilder::<Date32Type>::with_capacity(capacity)),
            DataType::Time64(_) => Self::Time64I64(Int64Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                Self::TsSecond(ts_builder::<TimestampSecondType>(capacity, tz))
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                Self::TsMilli(ts_builder::<TimestampMillisecondType>(capacity, tz))
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                Self::TsMicro(ts_builder::<TimestampMicrosecondType>(capacity, tz))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                Self::TsNano(ts_builder::<TimestampNanosecondType>(capacity, tz))
            }
            DataType::Null => Self::Null(NullBuilder::new()),
            other => {
                return Err(D2DError::schema(format!(
                    "文本行构建器不支持列类型 {:?} —— 请在连接器内先转换，不要落到这里",
                    other
                )))
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Decimal128(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::LargeUtf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::LargeBinary(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64I64(b) => b.append_null(),
            Self::TsSecond(b) => b.append_null(),
            Self::TsMilli(b) => b.append_null(),
            Self::TsMicro(b) => b.append_null(),
            Self::TsNano(b) => b.append_null(),
            Self::Null(b) => b.append_null(),
        }
    }

    fn finish(self, dt: &DataType) -> D2DResult<ArrayRef> {
        Ok(match self {
            Self::Int8(mut b) => Arc::new(b.finish()),
            Self::Int16(mut b) => Arc::new(b.finish()),
            Self::Int32(mut b) => Arc::new(b.finish()),
            Self::Int64(mut b) => Arc::new(b.finish()),
            Self::UInt8(mut b) => Arc::new(b.finish()),
            Self::UInt16(mut b) => Arc::new(b.finish()),
            Self::UInt32(mut b) => Arc::new(b.finish()),
            Self::UInt64(mut b) => Arc::new(b.finish()),
            Self::Float32(mut b) => Arc::new(b.finish()),
            Self::Float64(mut b) => Arc::new(b.finish()),
            Self::Decimal128(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::LargeUtf8(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::LargeBinary(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Time64I64(mut b) => {
                let arr = Arc::new(b.finish()) as ArrayRef;
                // Int64 位模式 → Time64(单位)；值已在目标单位预计算
                arrow::compute::cast(&arr, dt).map_err(|e| {
                    D2DError::schema(format!("cast Int64 -> {:?} failed: {}", dt, e))
                })?
            }
            Self::TsSecond(mut b) => Arc::new(b.finish()),
            Self::TsMilli(mut b) => Arc::new(b.finish()),
            Self::TsMicro(mut b) => Arc::new(b.finish()),
            Self::TsNano(mut b) => Arc::new(b.finish()),
            Self::Null(mut b) => Arc::new(b.finish()),
        })
    }
}

// ─────────────────────────────── 单元格解析 ───────────────────────────────

/// **单元格解析** —— 三条读取路径走的是同一份规则
fn append_cell(
    opts: &RowBuildOptions,
    builder: &mut ColBuilder,
    field: &[u8],
    col_name: &str,
    lt: &LogicalType,
    line_no: usize,
    raw_line: &[u8],
) -> D2DResult<()> {
    macro_rules! bail {
        ($msg:expr) => {
            return Err(ctx_err(line_no, col_name, raw_line, &$msg))
        };
    }
    // 时间列：builder 与逻辑类型需同时决定，先解析值再按 builder 追加
    if lt.is_temporal() {
        let s = trimmed_str(field).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
        let v = parse_temporal_to_i64(s, lt).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
        match builder {
            ColBuilder::Date32(b) => b.append_value(v as i32),
            ColBuilder::Time64I64(b) => b.append_value(v),
            ColBuilder::TsSecond(b) => b.append_value(v),
            ColBuilder::TsMilli(b) => b.append_value(v),
            ColBuilder::TsMicro(b) => b.append_value(v),
            ColBuilder::TsNano(b) => b.append_value(v),
            _ => bail!(format!("时间列 {} 落在了非时间的 builder 上", col_name)),
        }
        return Ok(());
    }
    match (builder, lt) {
        (ColBuilder::Int8(b), _) => b.append_value(
            parse_int::<i8>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Int16(b), _) => b.append_value(
            parse_int::<i16>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Int32(b), _) => b.append_value(
            parse_int::<i32>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Int64(b), _) => b.append_value(
            parse_int::<i64>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::UInt8(b), _) => b.append_value(
            parse_int::<u8>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::UInt16(b), _) => b.append_value(
            parse_int::<u16>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::UInt32(b), _) => b.append_value(
            parse_int::<u32>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::UInt64(b), _) => b.append_value(
            parse_int::<u64>(field, col_name).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Float32(b), _) => b.append_value(
            parse_float::<f32>(field, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Float64(b), _) => b.append_value(
            parse_float::<f64>(field, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?,
        ),
        (ColBuilder::Decimal128(b), LogicalType::Decimal { precision, scale }) => {
            let s = trimmed_str(field).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            let v = parse_decimal_i128(s, *precision, *scale)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(v);
        }
        (ColBuilder::Boolean(b), _) => {
            let s = trimmed_str(field).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(parse_bool(s).map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?);
        }
        (ColBuilder::Utf8(b), _) => {
            let s = decode_text(field, opts.encoding, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(&s);
        }
        (ColBuilder::LargeUtf8(b), _) => {
            let s = decode_text(field, opts.encoding, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(&s);
        }
        (ColBuilder::Binary(b), _) => {
            let bytes = lob_cell_bytes(opts, field, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(&bytes);
        }
        (ColBuilder::LargeBinary(b), _) => {
            let bytes = lob_cell_bytes(opts, field, col_name)
                .map_err(|m| ctx_err(line_no, col_name, raw_line, &m))?;
            b.append_value(&bytes);
        }
        (ColBuilder::Null(_), _) => { /* 全空列 */ }
        (b, t) => bail!(format!(
            "builder/逻辑类型不匹配：列 {} 的 builder 是 {}，逻辑类型是 {}",
            col_name,
            b.kind_name(),
            t.describe()
        )),
    }
    Ok(())
}

impl ColBuilder {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Int8(_) => "int8",
            Self::Int16(_) => "int16",
            Self::Int32(_) => "int32",
            Self::Int64(_) => "int64",
            Self::UInt8(_) => "uint8",
            Self::UInt16(_) => "uint16",
            Self::UInt32(_) => "uint32",
            Self::UInt64(_) => "uint64",
            Self::Float32(_) => "float32",
            Self::Float64(_) => "float64",
            Self::Decimal128(_) => "decimal128",
            Self::Boolean(_) => "boolean",
            Self::Utf8(_) => "utf8",
            Self::LargeUtf8(_) => "large_utf8",
            Self::Binary(_) => "binary",
            Self::LargeBinary(_) => "large_binary",
            Self::Date32(_) => "date32",
            Self::Time64I64(_) => "time64",
            Self::TsSecond(_) => "ts_second",
            Self::TsMilli(_) => "ts_milli",
            Self::TsMicro(_) => "ts_micro",
            Self::TsNano(_) => "ts_nano",
            Self::Null(_) => "null",
        }
    }
}

/// 错误上下文：有原始行文本就带上（dmfldr），没有就只报行列（ODBC / Doris 协议）
fn ctx_err(line_no: usize, col: &str, raw: &[u8], msg: &str) -> D2DError {
    if raw.is_empty() {
        return D2DError::schema(format!(
            "解析失败：第 {} 行 列 '{}'：{}",
            line_no, col, msg
        ));
    }
    let preview: String = String::from_utf8_lossy(&raw[..raw.len().min(64)]).replace('\n', "\\n");
    D2DError::schema(format!(
        "解析失败：第 {} 行 列 '{}'：{}（原文: {}）",
        line_no, col, msg, preview
    ))
}

// ─────────────────────────────── 标量解析 ───────────────────────────────

fn parse_int<T: std::str::FromStr>(field: &[u8], col: &str) -> Result<T, String> {
    let s = trimmed_str(field)?;
    s.parse::<T>()
        .map_err(|_| format!("列 '{}'：'{}' 越界或不是整数", col, s))
}

fn parse_float<T: std::str::FromStr>(field: &[u8], col: &str) -> Result<T, String> {
    let s = trimmed_str(field)?;
    s.parse::<T>()
        .map_err(|_| format!("列 '{}'：'{}' 不是合法浮点数", col, s))
}

/// 去掉首尾 ASCII 空白后按 UTF-8 解析（数值/时间字段均为 ASCII）
pub fn trimmed_str(bytes: &[u8]) -> Result<&str, String> {
    let t: &[u8] = {
        let mut s = 0;
        let mut e = bytes.len();
        while s < e && bytes[s].is_ascii_whitespace() {
            s += 1;
        }
        while e > s && bytes[e - 1].is_ascii_whitespace() {
            e -= 1;
        }
        &bytes[s..e]
    };
    std::str::from_utf8(t).map_err(|e| format!("字段不是合法 UTF-8 文本: {}", e))
}

/// 按 `encoding` 把字段字节解码为 UTF-8 String
///
/// UTF-8 **严格校验**（不丢字符）；GBK/GB18030/EUC-KR 走 encoding_rs；
/// SINGLE_BYTE 按 Latin-1 逐字节映射（可逆、无替换歧义）。
pub fn decode_text(bytes: &[u8], enc: TextEncoding, col: &str) -> Result<String, String> {
    match enc {
        TextEncoding::Utf8 => String::from_utf8(bytes.to_vec())
            .map_err(|e| format!("列 '{}'：字段不是合法 UTF-8: {}", col, e)),
        TextEncoding::SingleByte => Ok(bytes.iter().map(|&b| b as char).collect()),
        TextEncoding::Gbk => Ok(encoding_rs::GBK.decode(bytes).0.into_owned()),
        TextEncoding::Gb18030 => Ok(encoding_rs::GB18030.decode(bytes).0.into_owned()),
        TextEncoding::EucKr => Ok(encoding_rs::EUC_KR.decode(bytes).0.into_owned()),
    }
}

/// 定点数字符串 → i128（按契约精度/标度校验，超出即报错，绝不静默舍入）
pub fn parse_decimal_i128(field: &str, precision: u8, scale: i8) -> Result<i128, String> {
    let s = field.trim();
    if s.is_empty() {
        return Err("DECIMAL 字段为空".to_string());
    }
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((a, b)) => (a, b),
        None => (rest, ""),
    };
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("不是合法的定点数字面量 '{}'", field));
    }
    if frac_part.len() > scale as usize {
        return Err(format!(
            "小数位数 {} 超过声明的 scale {}（拒绝静默舍入）",
            frac_part.len(),
            scale
        ));
    }
    let int_significant = int_part.trim_start_matches('0').len();
    // 有效位数 = 整数有效位 + scale（小数部分补齐到 scale）
    if int_significant + scale as usize > precision as usize {
        return Err(format!(
            "总有效位数超过声明的 precision {}（值 '{}'）",
            precision, field
        ));
    }
    let mut digits = String::with_capacity(int_part.len() + frac_part.len() + 1);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..(scale as usize) {
        digits.push('0');
    }
    let value: i128 = digits
        .parse()
        .map_err(|_| format!("定点数 '{}' 超出 128 位可表示范围", field))?;
    Ok(if neg { -value } else { value })
}

/// 布尔字段文本
///
/// 达梦 BIT 导出为 `0/1`；Doris 的 BOOLEAN 经 MySQL 协议口回 `0/1`；
/// 契约里也可能写成 `TRUE/FALSE`。三种写法都收。
pub fn parse_bool(field: &str) -> Result<bool, String> {
    match field.trim().to_ascii_uppercase().as_str() {
        "1" | "TRUE" | "T" | "YES" => Ok(true),
        "0" | "FALSE" | "F" | "NO" => Ok(false),
        _ => Err(format!("不是合法的布尔字面量 '{}'", field)),
    }
}

/// LOB 列的单元格 → 字节（三种形态，见 [`LobSource`]）
fn lob_cell_bytes(opts: &RowBuildOptions, field: &[u8], col: &str) -> Result<Vec<u8>, String> {
    match &opts.lob {
        LobSource::Inline => Ok(field.to_vec()),
        LobSource::FileRef(dir) => {
            let s = trimmed_str(field)?;
            read_lob_bytes(dir, s)
        }
        LobSource::Unsupported => Err(format!(
            "列 '{}' 是 LOB 类型，但本读取路径无法忠实承载它 —— \
             达梦 ODBC 的文本行集会把 LOB 截断到驱动上报的列宽（读回来是残的）。\
             请改用 dmfldr 路径，或先在库里把它转成 Base64 的字符串列",
            col
        )),
    }
}

/// BLOB/CLOB 的 LOB 引用解析："文件名:起始偏移:长度"（rsplitn 容忍路径中的冒号）
pub fn parse_lob_ref(field: &str) -> Result<(String, u64, u64), String> {
    let mut parts = field.rsplitn(3, ':');
    let len_s = parts
        .next()
        .ok_or_else(|| format!("不是合法的 LOB 引用 '{}'", field))?;
    let off_s = parts
        .next()
        .ok_or_else(|| format!("不是合法的 LOB 引用 '{}'", field))?;
    let name = parts
        .next()
        .ok_or_else(|| format!("不是合法的 LOB 引用 '{}'", field))?;
    let off: u64 = off_s
        .trim()
        .parse()
        .map_err(|_| format!("LOB 引用 '{}' 的偏移不是数字", field))?;
    let len: u64 = len_s
        .trim()
        .parse()
        .map_err(|_| format!("LOB 引用 '{}' 的长度不是数字", field))?;
    if name.trim().is_empty() {
        return Err(format!("不是合法的 LOB 引用 '{}'", field));
    }
    Ok((name.trim().to_string(), off, len))
}

/// 从 LOB 文件按引用读回字节
pub fn read_lob_bytes(lob_dir: &std::path::Path, reference: &str) -> Result<Vec<u8>, String> {
    let (name, off, len) = parse_lob_ref(reference)?;
    let path = if std::path::Path::new(&name).has_root() {
        PathBuf::from(&name)
    } else {
        lob_dir.join(&name)
    };
    use std::io::{Seek, SeekFrom};
    let mut f = std::fs::File::open(&path)
        .map_err(|e| format!("LOB 文件 {} 打开失败: {}", path.display(), e))?;
    f.seek(SeekFrom::Start(off))
        .map_err(|e| format!("LOB 文件 {} seek({}) 失败: {}", path.display(), off, e))?;
    let mut buf = vec![0u8; len as usize];
    std::io::Read::read_exact(&mut f, &mut buf)
        .map_err(|e| format!("LOB 文件 {} read({}@{}) 失败: {}", path.display(), len, off, e))?;
    Ok(buf)
}

/// NULL 判定（规则见 [`NullRule`]）
pub fn is_null_field(field: &[u8], rule: &NullRule) -> bool {
    rule.matches(field)
}

// ─────────────────────────────── 时间解析 ───────────────────────────────

/// 逻辑类型 → chrono 解析格式串
///
/// ⚠️ 这一份与 dmfldr 控制文件的 `FORMAT` 串**必须同源**：导出侧声明秒级、
/// 解析侧按微秒读，就是字段错位。dmfldr 侧的对应物是
/// `d2d_dameng::…::fmt::dm_export_format`，它有测试钉住两者的对应关系。
pub fn temporal_format(lt: &LogicalType) -> Option<&'static str> {
    let has_frac = |p: &TemporalPrecision| !matches!(p, TemporalPrecision::Second);
    match lt {
        LogicalType::Date32 => Some("%Y-%m-%d"),
        LogicalType::Time64(p) => match has_frac(p) {
            true => Some("%H:%M:%S%.f"),
            false => Some("%H:%M:%S"),
        },
        LogicalType::Timestamp { precision, tz } => match (has_frac(precision), tz.is_some()) {
            (true, false) => Some("%Y-%m-%d %H:%M:%S%.f"),
            (false, false) => Some("%Y-%m-%d %H:%M:%S"),
            (true, true) => Some("%Y-%m-%d %H:%M:%S%.f%z"),
            (false, true) => Some("%Y-%m-%d %H:%M:%S%z"),
        },
        _ => None,
    }
}

/// 解析时间字段时**依次尝试**的格式（先精确，后放宽小数位）
///
/// 为什么要放宽：`LogicalType::Timestamp` 的精度说的是**Arrow 里存多细**，
/// 不是"文本里一定有几位小数"。同一个 `DATETIME` 列，达梦导出带 `.000000`，
/// 而 Doris 的 MySQL 协议口回的是 `2022-01-01 00:00:00` —— 两者都是合法的
/// 该列取值。只认一种写法会把正常数据判成脏数据。
///
/// ⚠️ 顺序有意义：先按声明精度精确匹配，匹配不上才退到"无小数位"这一种写法。
/// 放宽的是**小数部分的有无**，不是类型本身 —— 缺位补零与多余位截断都不允许
/// （后者已由 [`parse_decimal_i128`] 的同类规则在数值侧挡住）。
pub fn temporal_formats(lt: &LogicalType) -> Vec<&'static str> {
    // 写死成字面量表而不是字符串替换：这条路径**每个单元格都要走一次**，
    // 替换出来的 String 没有地方放（返回 &'static str 就意味着泄漏）。
    match temporal_format(lt) {
        None => Vec::new(),
        Some("%H:%M:%S%.f") => vec!["%H:%M:%S%.f", "%H:%M:%S"],
        Some("%Y-%m-%d %H:%M:%S%.f") => vec!["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"],
        Some("%Y-%m-%d %H:%M:%S%.f%z") => {
            vec!["%Y-%m-%d %H:%M:%S%.f%z", "%Y-%m-%d %H:%M:%S%z"]
        }
        Some(only) => vec![only],
    }
}

/// 按逻辑类型解析时间字段，产出目标 Arrow 单位的 i64 值
pub fn parse_temporal_to_i64(field: &str, lt: &LogicalType) -> Result<i64, String> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    let formats = temporal_formats(lt);
    if formats.is_empty() {
        return Err(format!("逻辑类型 {} 不是可解析的时间类型", lt.describe()));
    }
    let s = field.trim();

    match lt {
        LogicalType::Date32 => {
            let d = try_parse(&formats, |f| NaiveDate::parse_from_str(s, f))?;
            Ok((d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days())
        }
        LogicalType::Time64(p) => {
            let t = try_parse(&formats, |f| NaiveTime::parse_from_str(s, f))?;
            let nanos = t
                .signed_duration_since(NaiveTime::from_hms_opt(0, 0, 0).unwrap())
                .num_nanoseconds()
                .unwrap_or(0);
            Ok(match p {
                TemporalPrecision::Second => nanos / 1_000_000_000,
                TemporalPrecision::Milli => nanos / 1_000_000,
                TemporalPrecision::Micro => nanos / 1_000,
                TemporalPrecision::Nano => nanos,
            })
        }
        LogicalType::Timestamp { precision, tz } => {
            if tz.is_some() {
                // 带时区语义：文本里有偏移量，按它归一到 UTC 再取 epoch
                let dt = try_parse(&formats, |f| chrono::DateTime::parse_from_str(s, f))?;
                match precision {
                    TemporalPrecision::Second => Ok(dt.timestamp()),
                    TemporalPrecision::Milli => Ok(dt.timestamp_millis()),
                    TemporalPrecision::Micro => Ok(dt.timestamp_micros()),
                    TemporalPrecision::Nano => dt
                        .timestamp_nanos_opt()
                        .ok_or_else(|| format!("时间戳 '{}' 超出纳秒可表示范围", field)),
                }
            } else {
                // 无时区语义：文本是"本地挂钟时间"，按 UTC 轴取 epoch
                // （与 Arrow 无时区时间戳的存储约定一致）
                let ndt = try_parse(&formats, |f| NaiveDateTime::parse_from_str(s, f))?;
                match precision {
                    TemporalPrecision::Second => Ok(ndt.and_utc().timestamp()),
                    TemporalPrecision::Milli => Ok(ndt.and_utc().timestamp_millis()),
                    TemporalPrecision::Micro => Ok(ndt.and_utc().timestamp_micros()),
                    TemporalPrecision::Nano => ndt
                        .and_utc()
                        .timestamp_nanos_opt()
                        .ok_or_else(|| format!("时间戳 '{}' 超出纳秒可表示范围", field)),
                }
            }
        }
        other => Err(format!("逻辑类型 {} 不是时间类型", other.describe())),
    }
}

fn try_parse<T, E, F>(formats: &[&str], mut f: F) -> Result<T, String>
where
    F: FnMut(&str) -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut last = String::new();
    for fmt in formats {
        match f(fmt) {
            Ok(v) => return Ok(v),
            Err(e) => last = format!("{}（尝试格式 {}）", e, fmt),
        }
    }
    Err(last)
}

// ─────────────────────────────── 测试 ───────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalArrowBridge;
    use arrow::array::{Array, Decimal128Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};

    fn schema_of(types: &[(&str, LogicalType)]) -> (SchemaRef, Vec<LogicalType>) {
        let logical: Vec<LogicalType> = types.iter().map(|(_, t)| t.clone()).collect();
        let fields: Vec<Field> = types
            .iter()
            .map(|(n, lt)| Field::new(*n, LogicalArrowBridge::to_arrow(lt).unwrap(), true))
            .collect();
        (Arc::new(Schema::new(fields)), logical)
    }

    fn build(types: &[(&str, LogicalType)], rows: &[&[Option<&[u8]>]]) -> RecordBatch {
        let (schema, logical) = schema_of(types);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), rows.len()).unwrap();
        for (i, r) in rows.iter().enumerate() {
            rb.append_row(r, i as u64).unwrap();
        }
        rb.finish().unwrap()
    }

    // ───────── 基本类型 ─────────

    #[test]
    fn test_basic_types_and_null() {
        let batch = build(
            &[
                ("id", LogicalType::Int64),
                ("name", LogicalType::Utf8),
                ("amt", LogicalType::Decimal { precision: 10, scale: 2 }),
                ("flag", LogicalType::Boolean),
            ],
            &[
                &[Some(b"1"), Some(b"alice"), Some(b"12.34"), Some(b"1")],
                // None = SQL NULL；空串 = 空串（两个都要走到 append_null 之外的路径）
                &[Some(b"2"), None, Some(b"-0.5"), Some(b"false")],
            ],
        );
        assert_eq!(batch.num_rows(), 2);
        let ids = batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1));
        let amts = batch.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(amts.value(0), 1234);
        assert_eq!(amts.value(1), -50);
        let flags = batch.column(3).as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
        assert!(flags.value(0));
        assert!(!flags.value(1));
    }

    /// **两个库对空串的语义不同** —— 这条是"跑完退出码 0、只有值错了"的防线
    #[test]
    fn test_null_rule_distinguishes_empty_string_from_null() {
        // MySQL/Doris：'' 是真值，只有协议给的 None 才是 NULL
        let batch = build(&[("s", LogicalType::Utf8)], &[&[Some(b"")], &[None]]);
        let s = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(s.value(0), "");
        assert!(!s.is_null(0), "Doris 口径下空串不是 NULL");
        assert!(s.is_null(1));

        // 达梦/Oracle：空串就是 NULL
        let opts = RowBuildOptions::dameng_default();
        let (schema, logical) = schema_of(&[("s", LogicalType::Utf8)]);
        let mut rb = RowBuilder::new(schema, logical, opts, 1).unwrap();
        rb.append_row(&[Some(b"")], 0).unwrap();
        let b = rb.finish().unwrap();
        assert!(
            b.column(0).as_any().downcast_ref::<StringArray>().unwrap().is_null(0),
            "达梦口径下空串必须落成 NULL"
        );
    }

    /// dmfldr `NULL_MODE=true`：只有标记串算 NULL，空串是真空串
    #[test]
    fn test_null_rule_marker() {
        let (schema, logical) = schema_of(&[("s", LogicalType::Utf8)]);
        let opts = RowBuildOptions {
            null: NullRule::Marker("NULL".into()),
            ..RowBuildOptions::mysql_protocol()
        };
        let mut rb = RowBuilder::new(schema, logical, opts, 3).unwrap();
        rb.append_row(&[Some(b"NULL")], 0).unwrap();
        rb.append_row(&[Some(b"")], 1).unwrap();
        rb.append_row(&[Some(b"x")], 2).unwrap();
        let s = rb.finish().unwrap();
        let s = s.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(s.is_null(0));
        assert_eq!(s.value(1), "");
        assert_eq!(s.value(2), "x");
        // 标记串判定不分大小写（dmfldr 导出的写法不保证）
        assert!(NullRule::Marker("NULL".into()).matches(b"null"));
    }

    // ───────── 严格性：这三条是"跑完了数字不对"的防线 ─────────

    #[test]
    fn test_decimal_scale_overflow_rejected() {
        let (schema, logical) = schema_of(&[("amt", LogicalType::Decimal { precision: 5, scale: 2 })]);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 1).unwrap();
        let e = rb.append_row(&[Some(b"1.234")], 0).unwrap_err().to_string();
        assert!(e.contains("scale"), "{}", e);
    }

    #[test]
    fn test_decimal_precision_overflow_rejected() {
        let (schema, logical) = schema_of(&[("amt", LogicalType::Decimal { precision: 4, scale: 2 })]);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 1).unwrap();
        // 整数 3 位 + scale 2 = 5 > precision 4
        let e = rb.append_row(&[Some(b"123.45")], 0).unwrap_err().to_string();
        assert!(e.contains("precision"), "{}", e);
        // 前导零不算有效位
        assert!(build(&[("amt", LogicalType::Decimal { precision: 4, scale: 2 })], &[&[Some(b"001.23")]]).num_rows() == 1);
    }

    #[test]
    fn test_int_overflow_rejected() {
        let (schema, logical) = schema_of(&[("id", LogicalType::Int8)]);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 1).unwrap();
        let e = rb.append_row(&[Some(b"999")], 0).unwrap_err().to_string();
        assert!(e.contains("越界"), "{}", e);
    }

    #[test]
    fn test_invalid_utf8_rejected() {
        let (schema, logical) = schema_of(&[("s", LogicalType::Utf8)]);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 1).unwrap();
        let e = rb
            .append_row(&[Some(&[0xff, 0xfe])], 0)
            .unwrap_err()
            .to_string();
        assert!(e.contains("UTF-8"), "{}", e);
    }

    #[test]
    fn test_column_count_mismatch_rejected() {
        let (schema, logical) = schema_of(&[("a", LogicalType::Int64), ("b", LogicalType::Int64)]);
        let mut rb =
            RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 1).unwrap();
        let e = rb.append_row(&[Some(b"1")], 0).unwrap_err().to_string();
        assert!(e.contains("列数"), "{}", e);
    }

    #[test]
    fn test_schema_logical_length_mismatch_rejected() {
        let (schema, _) = schema_of(&[("a", LogicalType::Int64)]);
        let e = RowBuilder::new(schema, vec![], RowBuildOptions::mysql_protocol(), 1)
            .err()
            .expect("schema 与逻辑类型数不一致时必须报错")
            .to_string();
        assert!(e.contains("不一致"), "{}", e);
    }

    // ───────── 时间 ─────────

    fn ts_micro(field: &str) -> i64 {
        parse_temporal_to_i64(
            field,
            &LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_timestamp_with_and_without_fraction() {
        // 带小数（dmfldr 导出形态）
        assert_eq!(
            ts_micro("2024-01-02 03:04:05.123456"),
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_micro_opt(3, 4, 5, 123456)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        );
        // 不带小数（Doris MySQL 协议口回的就是这个形态）—— **必须同样接受**
        assert_eq!(
            ts_micro("2023-10-08 16:43:57"),
            chrono::NaiveDate::from_ymd_opt(2023, 10, 8)
                .unwrap()
                .and_hms_opt(16, 43, 57)
                .unwrap()
                .and_utc()
                .timestamp_micros()
        );
    }

    #[test]
    fn test_date_and_tz_timestamp() {
        assert_eq!(
            parse_temporal_to_i64("2022-01-01", &LogicalType::Date32).unwrap(),
            (chrono::NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()
                - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days()
        );
        let tz_lt = LogicalType::Timestamp {
            precision: TemporalPrecision::Milli,
            tz: Some(crate::FixedOffset::cst()),
        };
        // +08:00 06:07:08.123 = UTC 前一天 22:07:08.123
        assert_eq!(
            parse_temporal_to_i64("2024-03-05 06:07:08.123+08:00", &tz_lt).unwrap(),
            chrono::NaiveDate::from_ymd_opt(2024, 3, 4)
                .unwrap()
                .and_hms_milli_opt(22, 7, 8, 123)
                .unwrap()
                .and_utc()
                .timestamp_millis()
        );
    }

    #[test]
    fn test_bad_temporal_rejected_with_both_formats() {
        let e = parse_temporal_to_i64(
            "not-a-date",
            &LogicalType::Timestamp {
                precision: TemporalPrecision::Micro,
                tz: None,
            },
        )
        .unwrap_err();
        // 报错要说清"试过哪些格式"，否则用户不知道该改成什么样
        assert!(e.contains("尝试格式"), "{}", e);
    }

    #[test]
    fn test_temporal_formats_shape() {
        let micro = LogicalType::Timestamp {
            precision: TemporalPrecision::Micro,
            tz: None,
        };
        assert_eq!(
            temporal_formats(&micro),
            vec!["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"]
        );
        // 秒级只有一种写法，不该凭空多出一个兜底
        let sec = LogicalType::Timestamp {
            precision: TemporalPrecision::Second,
            tz: None,
        };
        assert_eq!(temporal_formats(&sec), vec!["%Y-%m-%d %H:%M:%S"]);
        assert!(temporal_formats(&LogicalType::Utf8).is_empty());
    }

    #[test]
    fn test_time64_units() {
        use arrow::array::Time64MicrosecondArray;
        let batch = build(
            &[("t", LogicalType::Time64(TemporalPrecision::Micro))],
            &[&[Some(b"01:02:03.000004")]],
        );
        let t = batch.column(0).as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
        assert_eq!(t.value(0), 3_723_000_004);
    }

    // ───────── LOB 三种形态 ─────────

    #[test]
    fn test_lob_file_ref() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("x.lob"), b"HELLO-WORLD").unwrap();
        let (schema, logical) = schema_of(&[("b", LogicalType::LargeBinary)]);
        let opts = RowBuildOptions {
            lob: LobSource::FileRef(dir.path().to_path_buf()),
            ..RowBuildOptions::mysql_protocol()
        };
        let mut rb = RowBuilder::new(schema, logical, opts, 1).unwrap();
        rb.append_row(&[Some(b"x.lob:6:5")], 0).unwrap();
        let batch = rb.finish().unwrap();
        let b = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(b.value(0), b"WORLD");
    }

    #[test]
    fn test_lob_inline_takes_bytes_verbatim() {
        let batch = build(
            &[("b", LogicalType::Binary)],
            &[&[Some(b"\x00\x01\xff")]],
        );
        let b = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(b.value(0), b"\x00\x01\xff");
    }

    /// 默认是 `Unsupported`：**静默把截断的 LOB 当值写下去**比报错糟得多
    #[test]
    fn test_lob_unsupported_by_default() {
        let (schema, logical) = schema_of(&[("b", LogicalType::LargeBinary)]);
        let mut rb = RowBuilder::new(schema, logical, RowBuildOptions::dameng_default(), 1).unwrap();
        let e = rb.append_row(&[Some(b"whatever")], 0).unwrap_err().to_string();
        assert!(e.contains("LOB"), "{}", e);
        assert!(e.contains("dmfldr"), "要指出出路: {}", e);
    }

    // ───────── 编码 ─────────

    #[test]
    fn test_decode_text_encodings() {
        assert_eq!(decode_text("中文".as_bytes(), TextEncoding::Utf8, "c").unwrap(), "中文");
        assert!(decode_text(&[0xff, 0xfe], TextEncoding::Utf8, "c").is_err());
        // Latin-1 逐字节可逆
        assert_eq!(decode_text(&[0x41, 0xE9], TextEncoding::SingleByte, "c").unwrap(), "Aé");
        let (gbk, _, _) = encoding_rs::GBK.encode("中文");
        assert_eq!(decode_text(&gbk, TextEncoding::Gbk, "c").unwrap(), "中文");
    }

    // ───────── 标量解析的边角 ─────────

    #[test]
    fn test_parse_decimal_edge_cases() {
        assert_eq!(parse_decimal_i128("0", 10, 2).unwrap(), 0);
        assert_eq!(parse_decimal_i128("-1.5", 10, 2).unwrap(), -150);
        assert_eq!(parse_decimal_i128("+1.5", 10, 2).unwrap(), 150);
        // 无小数点时补零到 scale
        assert_eq!(parse_decimal_i128("7", 10, 4).unwrap(), 70000);
        assert!(parse_decimal_i128("", 10, 2).is_err());
        assert!(parse_decimal_i128("1e5", 10, 2).is_err(), "科学计数法不是定点数写法");
        assert!(parse_decimal_i128("1.2.3", 10, 2).is_err());
    }

    #[test]
    fn test_parse_bool_variants() {
        for t in ["1", "TRUE", "t", "yes"] {
            assert!(parse_bool(t).unwrap(), "{}", t);
        }
        for f in ["0", "FALSE", "f", "NO"] {
            assert!(!parse_bool(f).unwrap(), "{}", f);
        }
        assert!(parse_bool("2").is_err());
    }

    #[test]
    fn test_parse_lob_ref_tolerates_colons_in_path() {
        assert_eq!(
            parse_lob_ref("/data/a:b.lob:10:20").unwrap(),
            ("/data/a:b.lob".to_string(), 10, 20)
        );
        assert!(parse_lob_ref("nocolon").is_err());
        assert!(parse_lob_ref(":1:2").is_err());
    }

    // ───────── 空批与容量 ─────────

    #[test]
    fn test_empty_batch_keeps_schema() {
        let (schema, logical) = schema_of(&[("id", LogicalType::Int64)]);
        let rb = RowBuilder::new(schema, logical, RowBuildOptions::mysql_protocol(), 4).unwrap();
        assert!(rb.is_empty());
        let batch = rb.finish().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
    }

    #[test]
    fn test_unsupported_arrow_type_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new("l", DataType::Float16, true)]));
        let e = RowBuilder::new(
            schema,
            vec![LogicalType::Float32],
            RowBuildOptions::mysql_protocol(),
            1,
        )
        .err()
        .expect("Arrow Float16 没有对应 builder，必须报错")
        .to_string();
        assert!(e.contains("不支持列类型"), "{}", e);
    }

    #[test]
    fn test_timestamp_array_materialized_correctly() {
        let batch = build(
            &[("t", LogicalType::Timestamp { precision: TemporalPrecision::Micro, tz: None })],
            &[&[Some(b"2023-10-08 16:43:57")]],
        );
        let t = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(t.value(0), ts_micro("2023-10-08 16:43:57"));
        assert_eq!(t.null_count(), 0);
    }
}
