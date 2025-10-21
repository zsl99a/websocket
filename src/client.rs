use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::config::ClientConfig;
use crate::connection::{ConnectionManager, WsStream};
use crate::error::{WebSocketError, WebSocketResult};
use crate::event::{EventBus, EventBuilder, WebSocketEvent};
use crate::heartbeat::HeartbeatManager;
use crate::message::{Message, MessageProcessor, MessageType};

/// WebSocket 客户端状态
#[derive(Debug, Clone, PartialEq)]
pub enum ClientState {
    /// 初始化状态
    Initialized,
    /// 正在启动
    Starting,
    /// 运行中
    Running,
    /// 正在停止
    Stopping,
    /// 已停止
    Stopped,
    /// 错误状态
    Error(String),
}

/// WebSocket 客户端
pub struct WebSocketClient {
    config: ClientConfig,
    state: Arc<RwLock<ClientState>>,
    
    // 核心组件
    connection_manager: Arc<ConnectionManager>,
    message_processor: Arc<RwLock<MessageProcessor>>,
    heartbeat_manager: Arc<HeartbeatManager>,
    event_bus: EventBus,
    
    // 任务句柄
    message_loop_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    
    // 消息发送通道
    message_sender: Arc<RwLock<Option<mpsc::UnboundedSender<WsMessage>>>>,
}

impl WebSocketClient {
    /// 创建新的 WebSocket 客户端
    pub async fn new(config: ClientConfig) -> WebSocketResult<Self> {
        // 验证配置
        config.validate()?;

        // 创建事件总线
        let event_bus = EventBus::new(config.message.queue_size);

        // 创建组件
        let connection_manager = Arc::new(ConnectionManager::new(config.clone(), event_bus.clone()));
        let message_processor = Arc::new(RwLock::new(MessageProcessor::new()));
        let heartbeat_manager = Arc::new(HeartbeatManager::new(config.heartbeat.clone(), event_bus.clone()));

        Ok(Self {
            config,
            state: Arc::new(RwLock::new(ClientState::Initialized)),
            connection_manager,
            message_processor,
            heartbeat_manager,
            event_bus,
            message_loop_handle: Arc::new(RwLock::new(None)),
            message_sender: Arc::new(RwLock::new(None)),
        })
    }

    /// 启动客户端
    pub async fn start(&self) -> WebSocketResult<()> {
        {
            let mut state = self.state.write().await;
            if *state != ClientState::Initialized && *state != ClientState::Stopped {
                return Err(WebSocketError::InternalError(
                    format!("客户端状态无效，当前状态: {:?}", *state)
                ));
            }
            *state = ClientState::Starting;
        }

        tracing::info!("启动 WebSocket 客户端");

        // 建立连接
        let ws_stream = self.connection_manager.connect_with_retry().await?;

        // 启动消息循环
        self.start_message_loop(ws_stream).await?;

        // 更新状态
        {
            let mut state = self.state.write().await;
            *state = ClientState::Running;
        }

        tracing::info!("WebSocket 客户端启动成功");
        Ok(())
    }

    /// 停止客户端
    pub async fn stop(&self) -> WebSocketResult<()> {
        {
            let mut state = self.state.write().await;
            *state = ClientState::Stopping;
        }

        tracing::info!("正在停止 WebSocket 客户端");

        // 停止心跳管理器
        self.heartbeat_manager.stop();

        // 关闭消息发送通道
        {
            let mut sender = self.message_sender.write().await;
            *sender = None;
        }

        // 等待消息循环结束
        {
            let mut handle = self.message_loop_handle.write().await;
            if let Some(handle) = handle.take() {
                handle.abort();
                let _ = handle.await;
            }
        }

        // 断开连接
        self.connection_manager.disconnect(Some("客户端主动停止".to_string())).await;

        // 更新状态
        {
            let mut state = self.state.write().await;
            *state = ClientState::Stopped;
        }

        tracing::info!("WebSocket 客户端已停止");
        Ok(())
    }

    /// 发送消息
    pub async fn send_message(&self, message: Message) -> WebSocketResult<()> {
        let sender = self.message_sender.read().await;
        if let Some(sender) = sender.as_ref() {
            let processor = self.message_processor.read().await;
            let ws_message = processor.prepare_send(message.clone())?;
            
            if let Err(_) = sender.send(ws_message) {
                let error = WebSocketError::SendFailed("消息发送通道已关闭".to_string());
                self.event_bus.publish(EventBuilder::message_send_failed(message, error.to_string())).await?;
                return Err(error);
            }

            // 发布消息发送事件
            self.event_bus.publish(EventBuilder::message_sent(message)).await?;
            Ok(())
        } else {
            Err(WebSocketError::ConnectionClosed)
        }
    }

    /// 发送数据消息（便捷方法）
    pub async fn send_data(&self, data: serde_json::Value) -> WebSocketResult<()> {
        let message = Message::data(data);
        self.send_message(message).await
    }

    /// 注册消息处理器
    pub async fn register_message_handler<F>(&self, message_type: MessageType, handler: F) 
    where
        F: Fn(&Message) -> WebSocketResult<()> + Send + Sync + 'static,
    {
        let mut processor = self.message_processor.write().await;
        processor.router_mut().register_handler(message_type, handler);
    }

    /// 注册事件处理器
    pub async fn register_event_handler<F>(&self, name: impl Into<String>, handler: F)
    where
        F: Fn(WebSocketEvent) -> WebSocketResult<()> + Send + Sync + 'static,
    {
        self.event_bus.register_handler(name, handler).await;
    }

    /// 获取客户端状态
    pub async fn get_state(&self) -> ClientState {
        self.state.read().await.clone()
    }

    /// 检查是否正在运行
    pub async fn is_running(&self) -> bool {
        matches!(*self.state.read().await, ClientState::Running)
    }

    /// 获取事件总线（用于订阅事件）
    pub fn event_bus(&self) -> &EventBus {
        &self.event_bus
    }

    /// 启动消息循环
    async fn start_message_loop(&self, ws_stream: WsStream) -> WebSocketResult<()> {
        let (mut ws_sink, mut ws_stream) = ws_stream.split();

        // 创建消息发送通道
        let (msg_sender, mut msg_receiver) = mpsc::unbounded_channel::<WsMessage>();
        
        // 保存发送器引用
        {
            let mut sender = self.message_sender.write().await;
            *sender = Some(msg_sender.clone());
        }

        // 克隆必要的组件引用
        let message_processor = Arc::clone(&self.message_processor);
        let heartbeat_manager = Arc::clone(&self.heartbeat_manager);
        let event_bus = self.event_bus.clone();
        let connection_manager = Arc::clone(&self.connection_manager);

        // 启动消息处理任务
        let handle = tokio::spawn(async move {
            tracing::info!("消息循环已启动");

            loop {
                tokio::select! {
                    // 处理接收到的 WebSocket 消息
                    msg = ws_stream.next() => {
                        match msg {
                            Some(Ok(ws_msg)) => {
                                if let Err(e) = Self::handle_received_message(
                                    &ws_msg,
                                    &message_processor,
                                    &heartbeat_manager,
                                    &event_bus,
                                    &msg_sender,
                                ).await {
                                    tracing::error!("处理接收消息失败: {}", e);
                                    let _ = event_bus.publish(EventBuilder::error(e)).await;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::error!("WebSocket 接收错误: {}", e);
                                let ws_error = WebSocketError::from(e);
                                let _ = event_bus.publish(EventBuilder::error(ws_error)).await;
                                break;
                            }
                            None => {
                                tracing::info!("WebSocket 连接已关闭");
                                let _ = connection_manager.disconnect(Some("连接被远程关闭".to_string())).await;
                                break;
                            }
                        }
                    }
                    
                    // 处理要发送的消息
                    msg = msg_receiver.recv() => {
                        match msg {
                            Some(ws_msg) => {
                                if let Err(e) = ws_sink.send(ws_msg).await {
                                    tracing::error!("发送消息失败: {}", e);
                                    let ws_error = WebSocketError::SendFailed(e.to_string());
                                    let _ = event_bus.publish(EventBuilder::error(ws_error)).await;
                                    break;
                                }
                            }
                            None => {
                                tracing::info!("消息发送通道已关闭");
                                break;
                            }
                        }
                    }
                }
            }

            tracing::info!("消息循环已结束");
        });

        // 保存任务句柄
        {
            let mut handle_guard = self.message_loop_handle.write().await;
            *handle_guard = Some(handle);
        }

        Ok(())
    }

    /// 处理接收到的 WebSocket 消息
    async fn handle_received_message(
        ws_msg: &WsMessage,
        message_processor: &Arc<RwLock<MessageProcessor>>,
        heartbeat_manager: &Arc<HeartbeatManager>,
        event_bus: &EventBus,
        msg_sender: &mpsc::UnboundedSender<WsMessage>,
    ) -> WebSocketResult<()> {
        match ws_msg {
            WsMessage::Text(_) | WsMessage::Binary(_) => {
                // 尝试解析为我们的消息格式
                let processor = message_processor.read().await;
                match processor.process_received(ws_msg.clone()).await {
                    Ok(_) => {
                        // 消息处理成功，检查是否是心跳消息
                        if let Ok(message) = crate::message::MessageSerializer::deserialize(ws_msg) {
                            if message.is_pong() {
                                heartbeat_manager.handle_pong().await?;
                            } else if message.is_ping() {
                                heartbeat_manager.handle_ping(msg_sender).await?;
                            } else {
                                // 发布消息接收事件
                                event_bus.publish(EventBuilder::message_received(message)).await?;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("消息处理失败: {}", e);
                        event_bus.publish(EventBuilder::error(e)).await?;
                    }
                }
            }
            WsMessage::Close(frame) => {
                let reason = frame.as_ref().map(|f| f.reason.to_string());
                tracing::info!("收到关闭帧: {:?}", reason);
                event_bus.publish(EventBuilder::disconnected(reason)).await?;
            }
            _ => {
                tracing::debug!("收到未处理的消息类型: {:?}", ws_msg);
            }
        }

        Ok(())
    }
} 