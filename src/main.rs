use anyhow::Result;
use tracing::Level;
use std::time::Duration;
use tokio::time::sleep;

use websocket::{
    ClientConfig, WebSocketClient, WebSocketEvent, MessageType, Message,
    config::{ReconnectConfig, HeartbeatConfig, MessageConfig}
};

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_thread_ids(true)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    tracing::info!("启动 WebSocket 客户端示例");

    // 创建配置
    let config = ClientConfig::new("wss://echo.websocket.org")
        .with_connect_timeout(Duration::from_secs(10))
        .with_reconnect(ReconnectConfig {
            enabled: true,
            max_retries: Some(3),
            initial_interval: Duration::from_secs(1),
            max_interval: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        })
        .with_heartbeat(HeartbeatConfig {
            enabled: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(10),
        })
        .with_message_config(MessageConfig {
            max_size: 1024 * 1024, // 1MB
            queue_size: 1000,
        });

    // 创建客户端
    let client = WebSocketClient::new(config).await?;

    // 注册事件处理器
    client.register_event_handler("logger", |event: WebSocketEvent| {
        match event {
            WebSocketEvent::Connected => {
                tracing::info!("✅ 客户端已连接");
            }
            WebSocketEvent::Disconnected(reason) => {
                tracing::warn!("❌ 客户端已断开连接: {:?}", reason);
            }
            WebSocketEvent::Reconnecting(attempt) => {
                tracing::info!("🔄 正在进行第 {} 次重连...", attempt);
            }
            WebSocketEvent::Reconnected => {
                tracing::info!("✅ 重连成功");
            }
            WebSocketEvent::ReconnectFailed(reason) => {
                tracing::error!("❌ 重连失败: {}", reason);
            }
            WebSocketEvent::MessageReceived(message) => {
                tracing::info!("📨 收到消息: {:?}", message.payload);
            }
            WebSocketEvent::MessageSent(message) => {
                tracing::info!("📤 发送消息: {:?}", message.payload);
            }
            WebSocketEvent::MessageSendFailed(message, reason) => {
                tracing::error!("❌ 消息发送失败: {:?}, 原因: {}", message.payload, reason);
            }
            WebSocketEvent::HeartbeatSent => {
                tracing::debug!("💓 发送心跳");
            }
            WebSocketEvent::HeartbeatReceived => {
                tracing::debug!("💓 收到心跳响应");
            }
            WebSocketEvent::HeartbeatTimeout => {
                tracing::warn!("💓 心跳超时");
            }
            WebSocketEvent::Error(error) => {
                tracing::error!("❌ 发生错误: {}", error);
            }
        }
        Ok(())
    }).await;

    // 注册数据消息处理器
    client.register_message_handler(MessageType::Data, |message| {
        tracing::info!("处理数据消息: {}", message.payload);
        Ok(())
    }).await;

    // 启动客户端
    if let Err(e) = client.start().await {
        tracing::error!("启动客户端失败: {}", e);
        return Err(e.into());
    }

    // 等待一段时间让连接建立
    sleep(Duration::from_secs(2)).await;

    // 发送一些测试消息
    for i in 1..=5 {
        let message_data = serde_json::json!({
            "type": "test",
            "message": format!("这是第 {} 条测试消息", i),
            "timestamp": chrono::Utc::now().timestamp()
        });

        if let Err(e) = client.send_data(message_data).await {
            tracing::error!("发送消息失败: {}", e);
        }

        sleep(Duration::from_secs(2)).await;
    }

    // 发送自定义消息
    let custom_message = Message::new(
        MessageType::Custom("custom_type".to_string()),
        serde_json::json!({
            "custom_field": "custom_value",
            "number": 42
        })
    ).with_id("msg_001");

    if let Err(e) = client.send_message(custom_message).await {
        tracing::error!("发送自定义消息失败: {}", e);
    }

    // 运行一段时间
    tracing::info!("客户端将运行 30 秒...");
    sleep(Duration::from_secs(30)).await;

    // 停止客户端
    tracing::info!("正在停止客户端...");
    client.stop().await?;

    tracing::info!("WebSocket 客户端示例结束");
    Ok(())
}
