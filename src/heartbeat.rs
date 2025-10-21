use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::config::HeartbeatConfig;
use crate::error::{WebSocketError, WebSocketResult};
use crate::event::{EventBus, EventBuilder};
use crate::message::Message;

/// 心跳管理器
pub struct HeartbeatManager {
    config: HeartbeatConfig,
    event_bus: EventBus,
    is_running: Arc<AtomicBool>,
    last_pong: Arc<AtomicU64>,
    pending_ping: Arc<AtomicBool>,
}

impl HeartbeatManager {
    /// 创建新的心跳管理器
    pub fn new(config: HeartbeatConfig, event_bus: EventBus) -> Self {
        Self {
            config,
            event_bus,
            is_running: Arc::new(AtomicBool::new(false)),
            last_pong: Arc::new(AtomicU64::new(0)),
            pending_ping: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 启动心跳管理器
    pub async fn start(&self, message_sender: mpsc::UnboundedSender<WsMessage>) -> WebSocketResult<()> {
        if !self.config.enabled {
            tracing::info!("心跳功能已禁用");
            return Ok(());
        }

        if self.is_running.load(Ordering::Relaxed) {
            tracing::warn!("心跳管理器已经在运行");
            return Ok(());
        }

        self.is_running.store(true, Ordering::Relaxed);
        self.update_last_pong();

        let is_running = Arc::clone(&self.is_running);
        let last_pong = Arc::clone(&self.last_pong);
        let pending_ping = Arc::clone(&self.pending_ping);
        let config = self.config.clone();
        let event_bus = self.event_bus.clone();

        tokio::spawn(async move {
            let mut heartbeat_interval = interval(config.interval);
            
            tracing::info!("心跳管理器已启动，间隔: {:?}", config.interval);

            while is_running.load(Ordering::Relaxed) {
                heartbeat_interval.tick().await;

                // 检查是否有待处理的 ping
                if pending_ping.load(Ordering::Relaxed) {
                    let elapsed = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64 - last_pong.load(Ordering::Relaxed);

                    if elapsed > config.timeout.as_millis() as u64 {
                        tracing::warn!("心跳超时，时长: {}ms", elapsed);
                        
                        // 发布心跳超时事件
                        if let Err(e) = event_bus.publish(EventBuilder::heartbeat_timeout()).await {
                            tracing::error!("发布心跳超时事件失败: {}", e);
                        }

                        // 发布错误事件
                        if let Err(e) = event_bus.publish(EventBuilder::error(WebSocketError::HeartbeatTimeout)).await {
                            tracing::error!("发布心跳超时错误事件失败: {}", e);
                        }

                        break;
                    }
                }

                // 发送 Ping 消息
                if let Err(e) = message_sender.send(WsMessage::Ping(vec![].into())) {
                    tracing::error!("发送心跳消息失败: {}", e);
                    break;
                }

                pending_ping.store(true, Ordering::Relaxed);
                
                // 发布心跳发送事件
                if let Err(e) = event_bus.publish(EventBuilder::heartbeat_sent()).await {
                    tracing::error!("发布心跳发送事件失败: {}", e);
                }

                tracing::debug!("发送心跳 Ping");
            }

            tracing::info!("心跳管理器已停止");
        });

        Ok(())
    }

    /// 停止心跳管理器
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        tracing::info!("心跳管理器停止信号已发送");
    }

    /// 处理接收到的 Pong 消息
    pub async fn handle_pong(&self) -> WebSocketResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.pending_ping.store(false, Ordering::Relaxed);
        self.update_last_pong();

        // 发布心跳响应事件
        self.event_bus.publish(EventBuilder::heartbeat_received()).await?;

        tracing::debug!("收到心跳 Pong 响应");
        Ok(())
    }

    /// 处理接收到的 Ping 消息（需要回复 Pong）
    pub async fn handle_ping(&self, message_sender: &mpsc::UnboundedSender<WsMessage>) -> WebSocketResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // 回复 Pong 消息
        let pong_message = Message::pong();
        match serde_json::to_string(&pong_message) {
            Ok(pong_json) => {
                if let Err(e) = message_sender.send(WsMessage::Text(pong_json.into())) {
                    tracing::error!("发送 Pong 响应失败: {}", e);
                    return Err(WebSocketError::SendFailed(format!("Pong 发送失败: {}", e)));
                }
                tracing::debug!("发送心跳 Pong 响应");
            }
            Err(e) => {
                tracing::error!("序列化 Pong 消息失败: {}", e);
                return Err(WebSocketError::ParseError(format!("Pong 序列化失败: {}", e)));
            }
        }

        Ok(())
    }

    /// 更新最后收到 Pong 的时间
    fn update_last_pong(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_pong.store(now, Ordering::Relaxed);
    }

    /// 检查心跳是否健康
    pub fn is_healthy(&self) -> bool {
        if !self.config.enabled {
            return true; // 如果禁用了心跳，认为是健康的
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let elapsed = now - self.last_pong.load(Ordering::Relaxed);
        let timeout_ms = self.config.timeout.as_millis() as u64;

        elapsed <= timeout_ms || !self.pending_ping.load(Ordering::Relaxed)
    }

    /// 获取心跳统计信息
    pub fn get_stats(&self) -> HeartbeatStats {
        let last_pong_time = self.last_pong.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        HeartbeatStats {
            enabled: self.config.enabled,
            is_running: self.is_running.load(Ordering::Relaxed),
            interval: self.config.interval,
            timeout: self.config.timeout,
            last_pong_time,
            time_since_last_pong: Duration::from_millis(now.saturating_sub(last_pong_time)),
            pending_ping: self.pending_ping.load(Ordering::Relaxed),
            is_healthy: self.is_healthy(),
        }
    }

    /// 重置心跳状态
    pub fn reset(&self) {
        self.pending_ping.store(false, Ordering::Relaxed);
        self.update_last_pong();
        tracing::debug!("心跳状态已重置");
    }
}

/// 心跳统计信息
#[derive(Debug, Clone)]
pub struct HeartbeatStats {
    /// 是否启用心跳
    pub enabled: bool,
    /// 是否正在运行
    pub is_running: bool,
    /// 心跳间隔
    pub interval: Duration,
    /// 超时时间
    pub timeout: Duration,
    /// 最后一次收到 Pong 的时间戳（毫秒）
    pub last_pong_time: u64,
    /// 距离最后一次 Pong 的时间
    pub time_since_last_pong: Duration,
    /// 是否有待处理的 Ping
    pub pending_ping: bool,
    /// 心跳是否健康
    pub is_healthy: bool,
}

/// 健康检查器
pub struct HealthChecker {
    heartbeat_manager: Arc<HeartbeatManager>,
    check_interval: Duration,
}

impl HealthChecker {
    /// 创建新的健康检查器
    pub fn new(heartbeat_manager: Arc<HeartbeatManager>, check_interval: Duration) -> Self {
        Self {
            heartbeat_manager,
            check_interval,
        }
    }

    /// 启动健康检查
    pub async fn start(&self) -> WebSocketResult<()> {
        let heartbeat_manager = Arc::clone(&self.heartbeat_manager);
        let check_interval = self.check_interval;

        tokio::spawn(async move {
            let mut interval = interval(check_interval);
            
            loop {
                interval.tick().await;
                
                let stats = heartbeat_manager.get_stats();
                if !stats.is_healthy {
                    tracing::warn!("健康检查失败: {:?}", stats);
                    // 这里可以触发重连或其他恢复操作
                }
            }
        });

        Ok(())
    }

    /// 执行一次健康检查
    pub async fn check_once(&self) -> HeartbeatStats {
        self.heartbeat_manager.get_stats()
    }
} 