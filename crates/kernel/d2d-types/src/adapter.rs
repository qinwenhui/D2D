use crate::{FixedOffset, LogicalType, TemporalPrecision};
use d2d_core::D2DResult;

/// 数据库类型声明（从元数据/配置解析而来）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbTypeDecl {
    /// 基础类型名（大写），如 "NUMBER" / "VARCHAR2" / "TIMESTAMP"
    pub base: String,
    /// 第一个参数：精度/长度
    pub precision: Option<u32>,
    /// 第二个参数：标度
    pub scale: Option<i32>,
    /// 是否显式声明 WITH TIME ZONE
    pub with_time_zone: bool,
}

impl DbTypeDecl {
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into().to_uppercase(),
            precision: None,
            scale: None,
            with_time_zone: false,
        }
    }

    pub fn with_params(mut self, precision: Option<u32>, scale: Option<i32>) -> Self {
        self.precision = precision;
        self.scale = scale;
        self
    }

    pub fn with_tz(mut self, tz: bool) -> Self {
        self.with_time_zone = tz;
        self
    }

    /// 从 "NUMBER(38,10)" / "TIMESTAMP WITH TIME ZONE" 字符串解析
    pub fn parse(s: &str) -> Self {
        let raw = s.trim();
        let upper = raw.to_uppercase();

        // WITH TIME ZONE / WITH LOCAL TIME ZONE 检测
        let (base_part, with_time_zone) = if let Some(idx) = upper.find(" WITH TIME ZONE") {
            (&raw[..idx], true)
        } else if let Some(idx) = upper.find(" WITH LOCAL TIME ZONE") {
            (&raw[..idx], true)
        } else {
            (raw, false)
        };

        // WITH [LOCAL] TIME ZONE 去掉后可能残留类型尾缀（如 "TIMESTAMP(6)"），下面解析参数
        let (base, precision, scale) = if let Some(start) = base_part.find('(') {
            let end = base_part.find(')').unwrap_or(base_part.len());
            let base = base_part[..start].trim().to_uppercase();
            let params: Vec<i64> = base_part[start + 1..end]
                .split(',')
                .filter_map(|p| p.trim().parse().ok())
                .collect();
            match params.as_slice() {
                [p1, p2] => (base, Some(*p1 as u32), Some(*p2 as i32)),
                [p1] => (base, Some(*p1 as u32), None),
                _ => (base, None, None),
            }
        } else {
            // 无括号参数；去掉可能的 "WITHOUT TIME ZONE" 尾缀
            let base = base_part
                .to_uppercase()
                .replace(" WITHOUT TIME ZONE", "")
                .trim()
                .to_string();
            (base, None, None)
        };

        Self {
            base,
            precision,
            scale,
            with_time_zone,
        }
    }

    /// 渲染回字符串（用于溯源记录）
    pub fn render(&self) -> String {
        let mut s = self.base.clone();
        if let Some(p) = self.precision {
            match self.scale {
                Some(sc) => s.push_str(&format!("({},{})", p, sc)),
                None => s.push_str(&format!("({})", p)),
            }
        }
        if self.with_time_zone {
            s.push_str(" WITH TIME ZONE");
        }
        s
    }
}

/// 大对象（LOB）处理策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LobPolicy {
    /// 不支持，遇到即报错
    Unsupported,
    /// 支持，作为字节流/文本流处理
    Streaming,
    /// 需要 Base64 等显式编码转换后才可写入
    RequiresEncoding,
}

/// 类型适配器 Trait
///
/// **职责**：把数据库的类型声明翻译为 D2D 规范逻辑类型（读方向），
/// 以及反向翻译（写方向）。这是 Source/Sink 与内部类型体系之间的唯一通道。
///
/// **实现铁律**：
/// 1. 无法映射的类型返回错误，**绝不 fallback 到 Utf8**
/// 2. 读方向必须保留原始类型声明的精度信息（NUMBER(38,10) ≠ 泛泛的 Decimal）
/// 3. 时间类型必须显式化精度与时区语义
pub trait TypeAdapter: Send + Sync {
    /// 数据库标识（与 ConnectorRegistry 的 name 对应，如 "dameng"）
    fn db_name(&self) -> &str;

    /// 读方向：数据库类型声明 → 逻辑类型
    ///
    /// 无法映射时返回明确错误（含原始声明），由上层 fail-fast。
    fn to_logical(&self, decl: &DbTypeDecl) -> D2DResult<LogicalType>;

    /// 写方向：逻辑类型 → 数据库类型声明字符串
    fn to_db_type(&self, ty: &LogicalType) -> D2DResult<String>;

    /// 大对象策略
    fn lob_policy(&self) -> LobPolicy {
        LobPolicy::Unsupported
    }

    /// 默认时间精度（该数据库的常规 TIMESTAMP 精度），未显式声明时使用
    fn default_temporal_precision(&self) -> TemporalPrecision {
        TemporalPrecision::Micro
    }

    /// 默认时区语义（None = 本地时间无时区，最保守）
    fn default_tz(&self) -> Option<FixedOffset> {
        None
    }

    /// 便捷方法：从字符串声明读转换（自动 parse）
    fn to_logical_from_str(&self, s: &str) -> D2DResult<LogicalType> {
        self.to_logical(&DbTypeDecl::parse(s))
    }
}
