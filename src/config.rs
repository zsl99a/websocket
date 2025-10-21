use std::time::Duration;
use crate::error::{WebSocketError, WebSocketResult};

/// WebSocket 客户端配置
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// WebSocket 服务器 URL
    pub url: String,
    /// 连接超时时间
    pub connect_timeout: Duration,
    /// 重连配置
    pub reconnect: ReconnectConfig,
    /// 心跳配置
    pub heartbeat: HeartbeatConfig,
    /// 消息配置
    pub message: MessageConfig,
}

/// 重连配置
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// 是否启用自动重连
    pub enabled: bool,
    /// 最大重连次数 (None 表示无限重连)
    pub max_retries: Option<u32>,
    /// 初始重连间隔
    pub initial_interval: Duration,
    /// 最大重连间隔
    pub max_interval: Duration,
    /// 退避倍数
    pub backoff_multiplier: f64,
}

/// 心跳配置
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// 是否启用心跳
    pub enabled: bool,
    /// 心跳间隔
    pub interval: Duration,
    /// 心跳超时时间
    pub timeout: Duration,
}

/// 消息配置
#[derive(Debug, Clone)]
pub struct MessageConfig {
    /// 最大消息大小 (字节)
    pub max_size: usize,
    /// 消息队列大小
    pub queue_size: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            connect_timeout: Duration::from_secs(10),
            reconnect: ReconnectConfig::default(),
            heartbeat: HeartbeatConfig::default(),
            message: MessageConfig::default(),
        }
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: Some(5),
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(60),
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
        }
    }
}

impl Default for MessageConfig {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024, // 64MB
            queue_size: 1000,
        }
    }
}

impl ClientConfig {
    /// 创建新的配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// 设置连接超时
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// 设置重连配置
    pub fn with_reconnect(mut self, config: ReconnectConfig) -> Self {
        self.reconnect = config;
        self
    }

    /// 禁用自动重连
    pub fn without_reconnect(mut self) -> Self {
        self.reconnect.enabled = false;
        self
    }

    /// 设置心跳配置
    pub fn with_heartbeat(mut self, config: HeartbeatConfig) -> Self {
        self.heartbeat = config;
        self
    }

    /// 禁用心跳
    pub fn without_heartbeat(mut self) -> Self {
        self.heartbeat.enabled = false;
        self
    }

    /// 设置消息配置
    pub fn with_message_config(mut self, config: MessageConfig) -> Self {
        self.message = config;
        self
    }

    /// 验证配置
    pub fn validate(&self) -> WebSocketResult<()> {
        if self.url.is_empty() {
            return Err(WebSocketError::ConfigError("URL 不能为空".to_string()));
        }

        if !self.url.starts_with("ws://") && !self.url.starts_with("wss://") {
            return Err(WebSocketError::ConfigError(
                "URL 必须以 ws:// 或 wss:// 开头".to_string(),
            ));
        }

        if self.connect_timeout.is_zero() {
            return Err(WebSocketError::ConfigError(
                "连接超时时间必须大于0".to_string(),
            ));
        }

        if self.heartbeat.enabled && self.heartbeat.interval.is_zero() {
            return Err(WebSocketError::ConfigError(
                "心跳间隔必须大于0".to_string(),
            ));
        }

        if self.reconnect.enabled && self.reconnect.initial_interval.is_zero() {
            return Err(WebSocketError::ConfigError(
                "重连间隔必须大于0".to_string(),
            ));
        }

        Ok(())
    }
} 