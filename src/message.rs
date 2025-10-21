use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use crate::error::{WebSocketError, WebSocketResult};

/// 消息类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageType {
    /// 普通数据消息
    Data,
    /// 心跳 Ping
    Ping,
    /// 心跳 Pong
    Pong,
    /// 系统控制消息
    Control,
    /// 自定义消息类型
    Custom(String),
}

/// 通用消息结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// 消息类型
    pub message_type: MessageType,
    /// 消息 ID
    pub id: Option<String>,
    /// 消息内容
    pub payload: serde_json::Value,
    /// 时间戳
    pub timestamp: i64,
    /// 额外元数据
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Message {
    /// 创建新消息
    pub fn new(message_type: MessageType, payload: serde_json::Value) -> Self {
        Self {
            message_type,
            id: None,
            payload,
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
        }
    }

    /// 创建数据消息
    pub fn data(payload: serde_json::Value) -> Self {
        Self::new(MessageType::Data, payload)
    }

    /// 创建 Ping 消息
    pub fn ping() -> Self {
        Self::new(MessageType::Ping, serde_json::json!("ping"))
    }

    /// 创建 Pong 消息
    pub fn pong() -> Self {
        Self::new(MessageType::Pong, serde_json::json!("pong"))
    }

    /// 设置消息 ID
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// 添加元数据
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// 检查是否为 Ping 消息
    pub fn is_ping(&self) -> bool {
        self.message_type == MessageType::Ping
    }

    /// 检查是否为 Pong 消息
    pub fn is_pong(&self) -> bool {
        self.message_type == MessageType::Pong
    }
}

/// 消息序列化器
pub struct MessageSerializer;

impl MessageSerializer {
    /// 将消息序列化为 WebSocket 消息
    pub fn serialize(message: &Message) -> WebSocketResult<WsMessage> {
        match serde_json::to_string(message) {
            Ok(json) => Ok(WsMessage::Text(json.into())),
            Err(e) => Err(WebSocketError::ParseError(format!("序列化失败: {}", e))),
        }
    }

    /// 将 WebSocket 消息反序列化为消息
    pub fn deserialize(ws_message: &WsMessage) -> WebSocketResult<Message> {
        match ws_message {
            WsMessage::Text(text) => {
                serde_json::from_str::<Message>(text)
                    .map_err(|e| WebSocketError::ParseError(format!("反序列化失败: {}", e)))
            }
            WsMessage::Binary(data) => {
                serde_json::from_slice::<Message>(data)
                    .map_err(|e| WebSocketError::ParseError(format!("二进制反序列化失败: {}", e)))
            }
            WsMessage::Ping(data) => {
                let payload = serde_json::Value::String(String::from_utf8_lossy(data).to_string());
                Ok(Message::new(MessageType::Ping, payload))
            }
            WsMessage::Pong(data) => {
                let payload = serde_json::Value::String(String::from_utf8_lossy(data).to_string());
                Ok(Message::new(MessageType::Pong, payload))
            }
            WsMessage::Close(_) => {
                Err(WebSocketError::ConnectionClosed)
            }
            _ => Err(WebSocketError::ParseError("不支持的消息类型".to_string())),
        }
    }
}

/// 消息处理器类型定义
pub type MessageHandler = Box<dyn Fn(&Message) -> WebSocketResult<()> + Send + Sync>;

/// 消息路由器
pub struct MessageRouter {
    handlers: HashMap<MessageType, Vec<MessageHandler>>,
}

impl MessageRouter {
    /// 创建新的消息路由器
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    /// 注册消息处理器
    pub fn register_handler<F>(&mut self, message_type: MessageType, handler: F)
    where
        F: Fn(&Message) -> WebSocketResult<()> + Send + Sync + 'static,
    {
        self.handlers
            .entry(message_type)
            .or_insert_with(Vec::new)
            .push(Box::new(handler));
    }

    /// 路由消息到相应的处理器
    pub async fn route_message(&self, message: &Message) -> WebSocketResult<()> {
        if let Some(handlers) = self.handlers.get(&message.message_type) {
            for handler in handlers {
                if let Err(e) = handler(message) {
                    tracing::error!("消息处理器执行失败: {}", e);
                    return Err(e);
                }
            }
        } else {
            tracing::debug!("没有找到消息类型 {:?} 的处理器", message.message_type);
        }
        Ok(())
    }
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// 消息处理器
pub struct MessageProcessor {
    router: MessageRouter,
}

impl MessageProcessor {
    /// 创建新的消息处理器
    pub fn new() -> Self {
        Self {
            router: MessageRouter::new(),
        }
    }

    /// 获取消息路由器的可变引用
    pub fn router_mut(&mut self) -> &mut MessageRouter {
        &mut self.router
    }

    /// 处理接收到的 WebSocket 消息
    pub async fn process_received(&self, ws_message: WsMessage) -> WebSocketResult<()> {
        let message = MessageSerializer::deserialize(&ws_message)?;
        tracing::debug!("收到消息: {:?}", message);
        
        self.router.route_message(&message).await?;
        Ok(())
    }

    /// 准备发送的消息
    pub fn prepare_send(&self, message: Message) -> WebSocketResult<WsMessage> {
        tracing::debug!("准备发送消息: {:?}", message);
        MessageSerializer::serialize(&message)
    }
}

impl Default for MessageProcessor {
    fn default() -> Self {
        Self::new()
    }
} 