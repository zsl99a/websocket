use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;

use crate::config::ClientConfig;
use crate::error::{WebSocketError, WebSocketResult, ErrorClassifier};
use crate::event::{EventBus, EventBuilder};

/// 连接状态
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    /// 断开连接
    Disconnected,
    /// 正在连接
    Connecting,
    /// 已连接
    Connected,
    /// 正在重连
    Reconnecting,
    /// 连接失败
    Failed(String),
}

/// WebSocket 流类型别名
pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// 连接管理器
pub struct ConnectionManager {
    config: ClientConfig,
    event_bus: EventBus,
    state: Arc<std::sync::RwLock<ConnectionState>>,
    retry_count: Arc<AtomicU32>,
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new(config: ClientConfig, event_bus: EventBus) -> Self {
        Self {
            config,
            event_bus,
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            retry_count: Arc::new(AtomicU32::new(0)),
        }
    }

    /// 建立连接
    pub async fn connect(&self) -> WebSocketResult<WsStream> {
        self.set_state(ConnectionState::Connecting).await;
        
        tracing::info!("正在连接到 WebSocket 服务器: {}", self.config.url);

        // 使用超时机制连接
        let connect_future = connect_async(&self.config.url);
        let result = timeout(self.config.connect_timeout, connect_future).await;

        match result {
            Ok(Ok((ws_stream, response))) => {
                tracing::info!("WebSocket 连接成功，响应状态: {}", response.status());
                self.set_state(ConnectionState::Connected).await;
                self.retry_count.store(0, Ordering::Relaxed);
                
                // 发布连接成功事件
                self.event_bus.publish(EventBuilder::connected()).await?;
                
                Ok(ws_stream)
            }
            Ok(Err(e)) => {
                let error_msg = format!("WebSocket 连接失败: {}", e);
                tracing::error!("{}", error_msg);
                self.set_state(ConnectionState::Failed(error_msg.clone())).await;
                
                let ws_error = WebSocketError::ConnectionFailed(error_msg);
                self.event_bus.publish(EventBuilder::error(ws_error.clone())).await?;
                
                Err(ws_error)
            }
            Err(_) => {
                let error_msg = format!("连接超时，超时时间: {:?}", self.config.connect_timeout);
                tracing::error!("{}", error_msg);
                self.set_state(ConnectionState::Failed(error_msg.clone())).await;
                
                let ws_error = WebSocketError::ConnectionTimeout;
                self.event_bus.publish(EventBuilder::error(ws_error.clone())).await?;
                
                Err(ws_error)
            }
        }
    }

    /// 带重连的连接方法
    pub async fn connect_with_retry(&self) -> WebSocketResult<WsStream> {
        if !self.config.reconnect.enabled {
            return self.connect().await;
        }

        let mut current_retry = 0;
        let max_retries = self.config.reconnect.max_retries.unwrap_or(u32::MAX);

        loop {
            match self.connect().await {
                Ok(stream) => {
                    if current_retry > 0 {
                        // 这是重连成功
                        self.event_bus.publish(EventBuilder::reconnected()).await?;
                    }
                    return Ok(stream);
                }
                Err(e) if current_retry >= max_retries => {
                    let error_msg = format!("达到最大重连次数 {}, 最后错误: {}", max_retries, e);
                    tracing::error!("{}", error_msg);
                    
                    let final_error = WebSocketError::ReconnectFailed(error_msg);
                    self.event_bus.publish(EventBuilder::reconnect_failed(final_error.to_string())).await?;
                    self.event_bus.publish(EventBuilder::error(final_error.clone())).await?;
                    
                    return Err(final_error);
                }
                Err(e) if ErrorClassifier::is_recoverable(&e) => {
                    current_retry += 1;
                    self.retry_count.store(current_retry, Ordering::Relaxed);
                    
                    // 发布重连开始事件
                    self.set_state(ConnectionState::Reconnecting).await;
                    self.event_bus.publish(EventBuilder::reconnecting(current_retry)).await?;
                    
                    // 计算退避延迟
                    let delay = self.calculate_backoff_delay(current_retry);
                    tracing::info!("第 {} 次重连失败，{:?} 后重试: {}", current_retry, delay, e);
                    
                    sleep(delay).await;
                }
                Err(e) => {
                    // 不可恢复的错误，直接返回
                    tracing::error!("遇到不可恢复的错误，停止重连: {}", e);
                    self.event_bus.publish(EventBuilder::error(e.clone())).await?;
                    return Err(e);
                }
            }
        }
    }

    /// 计算退避延迟
    fn calculate_backoff_delay(&self, retry_count: u32) -> Duration {
        let base_delay = self.config.reconnect.initial_interval;
        let multiplier = self.config.reconnect.backoff_multiplier;
        let max_delay = self.config.reconnect.max_interval;

        let calculated_delay = base_delay.as_millis() as f64 * multiplier.powi(retry_count as i32 - 1);
        let delay_ms = calculated_delay.min(max_delay.as_millis() as f64) as u64;

        Duration::from_millis(delay_ms)
    }

    /// 断开连接
    pub async fn disconnect(&self, reason: Option<String>) {
        tracing::info!("断开 WebSocket 连接，原因: {:?}", reason);
        self.set_state(ConnectionState::Disconnected).await;
        self.event_bus.publish(EventBuilder::disconnected(reason)).await.ok();
    }

    /// 获取当前连接状态
    pub fn get_state(&self) -> ConnectionState {
        self.state.read().unwrap().clone()
    }

    /// 检查是否已连接
    pub fn is_connected(&self) -> bool {
        matches!(self.get_state(), ConnectionState::Connected)
    }

    /// 设置连接状态（内部方法）
    async fn set_state(&self, new_state: ConnectionState) {
        let mut state = self.state.write().unwrap();
        if *state != new_state {
            tracing::debug!("连接状态变更: {:?} -> {:?}", *state, new_state);
            *state = new_state;
        }
    }
} 