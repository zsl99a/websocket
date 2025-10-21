use std::fmt;
use tokio_tungstenite::tungstenite;

pub type WebSocketResult<T> = Result<T, WebSocketError>;

#[derive(Debug, Clone)]
pub enum WebSocketError {
    /// 连接错误
    ConnectionFailed(String),
    /// 连接超时
    ConnectionTimeout,
    /// 连接已关闭
    ConnectionClosed,
    /// 消息发送失败
    SendFailed(String),
    /// 消息解析失败
    ParseError(String),
    /// 心跳超时
    HeartbeatTimeout,
    /// 重连失败
    ReconnectFailed(String),
    /// 配置错误
    ConfigError(String),
    /// 网络错误
    NetworkError(String),
    /// 内部错误
    InternalError(String),
}

impl fmt::Display for WebSocketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebSocketError::ConnectionFailed(msg) => write!(f, "连接失败: {}", msg),
            WebSocketError::ConnectionTimeout => write!(f, "连接超时"),
            WebSocketError::ConnectionClosed => write!(f, "连接已关闭"),
            WebSocketError::SendFailed(msg) => write!(f, "消息发送失败: {}", msg),
            WebSocketError::ParseError(msg) => write!(f, "消息解析失败: {}", msg),
            WebSocketError::HeartbeatTimeout => write!(f, "心跳超时"),
            WebSocketError::ReconnectFailed(msg) => write!(f, "重连失败: {}", msg),
            WebSocketError::ConfigError(msg) => write!(f, "配置错误: {}", msg),
            WebSocketError::NetworkError(msg) => write!(f, "网络错误: {}", msg),
            WebSocketError::InternalError(msg) => write!(f, "内部错误: {}", msg),
        }
    }
}

impl std::error::Error for WebSocketError {}

impl From<tungstenite::Error> for WebSocketError {
    fn from(err: tungstenite::Error) -> Self {
        match err {
            tungstenite::Error::ConnectionClosed => WebSocketError::ConnectionClosed,
            tungstenite::Error::AlreadyClosed => WebSocketError::ConnectionClosed,
            _ => WebSocketError::NetworkError(err.to_string()),
        }
    }
}

impl From<serde_json::Error> for WebSocketError {
    fn from(err: serde_json::Error) -> Self {
        WebSocketError::ParseError(err.to_string())
    }
}

/// 错误分类器 - 判断错误是否可恢复
pub struct ErrorClassifier;

impl ErrorClassifier {
    /// 检查错误是否可以通过重连恢复
    pub fn is_recoverable(error: &WebSocketError) -> bool {
        matches!(
            error,
            WebSocketError::ConnectionFailed(_)
                | WebSocketError::ConnectionTimeout
                | WebSocketError::ConnectionClosed
                | WebSocketError::NetworkError(_)
                | WebSocketError::HeartbeatTimeout
        )
    }

    /// 检查错误是否需要立即重连
    pub fn requires_immediate_reconnect(error: &WebSocketError) -> bool {
        matches!(
            error,
            WebSocketError::ConnectionClosed | WebSocketError::HeartbeatTimeout
        )
    }
} 