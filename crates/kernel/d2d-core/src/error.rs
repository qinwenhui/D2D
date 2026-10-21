use std::fmt;
use thiserror::Error;

/// 全局结果类型别名
pub type D2DResult<T> = Result<T, D2DError>;

/// D2D 统一错误体系
///
/// 区分可重试错误 (Retryable) 与致命错误 (Fatal)，便于执行引擎决策。
#[derive(Error, Debug, Clone)]
pub enum D2DError {
    /// 配置解析或校验错误 —— 致命
    #[error("Config error: {0}")]
    Config(String),

    /// Schema 不匹配或元数据错误 —— 致命
    #[error("Schema error: {0}")]
    Schema(String),

    /// 数据源读取错误 —— 可重试
    #[error("Source read error (retryable): {origin} | detail: {detail}")]
    SourceRead { origin: String, detail: String },

    /// 数据写入错误 —— 可重试
    #[error("Sink write error (retryable): {sink} | detail: {detail}")]
    SinkWrite { sink: String, detail: String },

    /// 数据转换错误 —— 通常致命（数据质量问题）
    #[error("Transform error: {0}")]
    Transform(String),

    /// 内存超限错误 —— 可重试（触发 spill 后重试）
    #[error("Memory limit exceeded: requested={requested} available={available}")]
    MemoryExceeded { requested: usize, available: usize },

    /// Spill IO 错误 —— 可重试
    #[error("Spill error: {0}")]
    Spill(String),

    /// 连接错误 —— 可重试
    #[error("Connection error (retryable): {connector} | {detail}")]
    Connection { connector: String, detail: String },

    /// 超时错误 —— 可重试
    #[error("Timeout: {operation} after {duration_ms}ms")]
    Timeout { operation: String, duration_ms: u64 },

    /// 不支持的特性或类型 —— 致命
    #[error("Not supported: {0}")]
    NotSupported(String),

    /// 内部/未知错误
    #[error("Internal error: {0}")]
    Internal(String),

    /// 外部库错误封装
    #[error("External error [{origin}]: {detail}")]
    External { origin: String, detail: String },
}

impl D2DError {
    /// 判断错误是否可重试
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            D2DError::SourceRead { .. }
                | D2DError::SinkWrite { .. }
                | D2DError::MemoryExceeded { .. }
                | D2DError::Spill { .. }
                | D2DError::Connection { .. }
                | D2DError::Timeout { .. }
        )
    }

    /// 判断错误是否为致命错误
    pub fn is_fatal(&self) -> bool {
        !self.is_retryable()
    }

    /// 快速创建 SourceRead 错误
    pub fn source_read<O: fmt::Display, D: fmt::Display>(origin: O, detail: D) -> Self {
        D2DError::SourceRead {
            origin: origin.to_string(),
            detail: detail.to_string(),
        }
    }

    /// 快速创建 SinkWrite 错误
    pub fn sink_write<S: fmt::Display, D: fmt::Display>(sink: S, detail: D) -> Self {
        D2DError::SinkWrite {
            sink: sink.to_string(),
            detail: detail.to_string(),
        }
    }

    /// 快速创建 Connection 错误
    pub fn connection<C: fmt::Display, D: fmt::Display>(connector: C, detail: D) -> Self {
        D2DError::Connection {
            connector: connector.to_string(),
            detail: detail.to_string(),
        }
    }

    /// 快速创建 NotSupported 错误
    pub fn not_supported<D: fmt::Display>(detail: D) -> Self {
        D2DError::NotSupported(detail.to_string())
    }

    /// 快速创建 Config 错误
    pub fn config<D: fmt::Display>(detail: D) -> Self {
        D2DError::Config(detail.to_string())
    }

    /// 快速创建 Transform 错误
    pub fn transform<D: fmt::Display>(detail: D) -> Self {
        D2DError::Transform(detail.to_string())
    }

    /// 快速创建 Schema 错误
    pub fn schema<D: fmt::Display>(detail: D) -> Self {
        D2DError::Schema(detail.to_string())
    }

    /// 快速创建 Internal 错误
    pub fn internal<D: fmt::Display>(detail: D) -> Self {
        D2DError::Internal(detail.to_string())
    }

    /// 快速创建 External 错误
    pub fn external<O: fmt::Display, D: fmt::Display>(origin: O, detail: D) -> Self {
        D2DError::External {
            origin: origin.to_string(),
            detail: detail.to_string(),
        }
    }
}

/// 从 arrow 错误转换
impl From<arrow::error::ArrowError> for D2DError {
    fn from(e: arrow::error::ArrowError) -> Self {
        D2DError::External {
            origin: "arrow".to_string(),
            detail: e.to_string(),
        }
    }
}

/// 从 serde_json 错误转换
impl From<serde_json::Error> for D2DError {
    fn from(e: serde_json::Error) -> Self {
        D2DError::External {
            origin: "serde_json".to_string(),
            detail: e.to_string(),
        }
    }
}

/// 从 std::io 错误转换
impl From<std::io::Error> for D2DError {
    fn from(e: std::io::Error) -> Self {
        D2DError::External {
            origin: "io".to_string(),
            detail: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable() {
        let e = D2DError::source_read("dameng", "connection reset");
        assert!(e.is_retryable());
        assert!(!e.is_fatal());

        let e2 = D2DError::config("bad yaml");
        assert!(!e2.is_retryable());
        assert!(e2.is_fatal());
    }
}
