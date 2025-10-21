use crate::error::{WebSocketError, WebSocketResult};
use crate::message::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};

/// WebSocket 事件类型
#[derive(Debug, Clone)]
pub enum WebSocketEvent {
    /// 连接建立
    Connected,
    /// 连接断开
    Disconnected(Option<String>),
    /// 重连开始
    Reconnecting(u32), // 重连次数
    /// 重连成功
    Reconnected,
    /// 重连失败
    ReconnectFailed(String),
    /// 收到消息
    MessageReceived(Message),
    /// 消息发送成功
    MessageSent(Message),
    /// 消息发送失败
    MessageSendFailed(Message, String),
    /// 心跳发送
    HeartbeatSent,
    /// 心跳响应
    HeartbeatReceived,
    /// 心跳超时
    HeartbeatTimeout,
    /// 发生错误
    Error(WebSocketError),
}

/// 事件处理器类型
pub type EventHandler = Arc<dyn Fn(WebSocketEvent) -> WebSocketResult<()> + Send + Sync>;

/// 事件总线
pub struct EventBus {
    /// 广播发送器
    sender: broadcast::Sender<WebSocketEvent>,
    /// 事件处理器映射
    handlers: Arc<RwLock<HashMap<String, EventHandler>>>,
}

impl EventBus {
    /// 创建新的事件总线
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);

        Self {
            sender,
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 创建默认容量的事件总线
    pub fn default() -> Self {
        Self::new(1000)
    }

    /// 发布事件
    pub async fn publish(&self, event: WebSocketEvent) -> WebSocketResult<()> {
        tracing::debug!("发布事件: {:?}", event);

        // 发送到广播通道
        if let Err(e) = self.sender.send(event.clone()) {
            tracing::warn!("广播事件失败: {}", e);
        }

        // 调用注册的处理器
        let handlers = self.handlers.read().await;
        for (name, handler) in handlers.iter() {
            if let Err(e) = handler(event.clone()) {
                tracing::error!("事件处理器 '{}' 执行失败: {}", name, e);
            }
        }

        Ok(())
    }

    /// 注册事件处理器
    pub async fn register_handler<F>(&self, name: impl Into<String>, handler: F)
    where
        F: Fn(WebSocketEvent) -> WebSocketResult<()> + Send + Sync + 'static,
    {
        let mut handlers = self.handlers.write().await;
        handlers.insert(name.into(), Arc::new(handler));
    }

    /// 移除事件处理器
    pub async fn unregister_handler(&self, name: &str) {
        let mut handlers = self.handlers.write().await;
        handlers.remove(name);
    }

    /// 订阅事件流
    pub fn subscribe(&self) -> broadcast::Receiver<WebSocketEvent> {
        self.sender.subscribe()
    }

    /// 获取活跃订阅者数量
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Clone for EventBus {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            handlers: Arc::clone(&self.handlers),
        }
    }
}

/// 事件监听器
pub struct EventListener {
    receiver: broadcast::Receiver<WebSocketEvent>,
    handlers: HashMap<String, EventHandler>,
}

impl EventListener {
    /// 创建新的事件监听器
    pub fn new(event_bus: &EventBus) -> Self {
        Self {
            receiver: event_bus.subscribe(),
            handlers: HashMap::new(),
        }
    }

    /// 注册本地事件处理器
    pub fn register_handler<F>(&mut self, name: impl Into<String>, handler: F)
    where
        F: Fn(WebSocketEvent) -> WebSocketResult<()> + Send + Sync + 'static,
    {
        self.handlers.insert(name.into(), Arc::new(handler));
    }

    /// 开始监听事件
    pub async fn listen(&mut self) -> WebSocketResult<()> {
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    tracing::debug!("收到事件: {:?}", event);

                    // 调用本地处理器
                    for (name, handler) in &self.handlers {
                        if let Err(e) = handler(event.clone()) {
                            tracing::error!("本地事件处理器 '{}' 执行失败: {}", name, e);
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!("事件监听器滞后，跳过了 {} 个事件", skipped);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("事件总线已关闭，停止监听");
                    break;
                }
            }
        }
        Ok(())
    }
}

/// 事件构建器 - 便于创建常用事件
pub struct EventBuilder;

impl EventBuilder {
    /// 创建连接事件
    pub fn connected() -> WebSocketEvent {
        WebSocketEvent::Connected
    }

    /// 创建断开连接事件
    pub fn disconnected(reason: Option<impl Into<String>>) -> WebSocketEvent {
        WebSocketEvent::Disconnected(reason.map(|r| r.into()))
    }

    /// 创建重连事件
    pub fn reconnecting(attempt: u32) -> WebSocketEvent {
        WebSocketEvent::Reconnecting(attempt)
    }

    /// 创建重连成功事件
    pub fn reconnected() -> WebSocketEvent {
        WebSocketEvent::Reconnected
    }

    /// 创建重连失败事件
    pub fn reconnect_failed(reason: impl Into<String>) -> WebSocketEvent {
        WebSocketEvent::ReconnectFailed(reason.into())
    }

    /// 创建消息接收事件
    pub fn message_received(message: Message) -> WebSocketEvent {
        WebSocketEvent::MessageReceived(message)
    }

    /// 创建消息发送成功事件
    pub fn message_sent(message: Message) -> WebSocketEvent {
        WebSocketEvent::MessageSent(message)
    }

    /// 创建消息发送失败事件
    pub fn message_send_failed(message: Message, reason: impl Into<String>) -> WebSocketEvent {
        WebSocketEvent::MessageSendFailed(message, reason.into())
    }

    /// 创建心跳发送事件
    pub fn heartbeat_sent() -> WebSocketEvent {
        WebSocketEvent::HeartbeatSent
    }

    /// 创建心跳响应事件
    pub fn heartbeat_received() -> WebSocketEvent {
        WebSocketEvent::HeartbeatReceived
    }

    /// 创建心跳超时事件
    pub fn heartbeat_timeout() -> WebSocketEvent {
        WebSocketEvent::HeartbeatTimeout
    }

    /// 创建错误事件
    pub fn error(error: WebSocketError) -> WebSocketEvent {
        WebSocketEvent::Error(error)
    }
}
