use grafana_plugin_sdk::backend::{self, async_trait};

use super::error::StreamError;
use super::frame::build_frame_from_ws_records;
use super::QuotesPlugin;

// ═══════════════════════════════════════════════════════════════
//  StreamService — real-time streaming via Grafana Live
// ═══════════════════════════════════════════════════════════════

#[async_trait]
impl backend::StreamService for QuotesPlugin {
    type JsonValue = ();
    type Error = StreamError;
    type Stream = backend::BoxRunStream<Self::Error>;

    async fn subscribe_stream(
        &self,
        request: backend::SubscribeStreamRequest<Self>,
    ) -> Result<backend::SubscribeStreamResponse, Self::Error> {
        let path = request.path.as_str();
        let exists = self.queries.read().unwrap().contains_key(path);
        Ok(if exists {
            backend::SubscribeStreamResponse::ok(None)
        } else {
            backend::SubscribeStreamResponse::not_found()
        })
    }

    async fn run_stream(
        &self,
        request: backend::RunStreamRequest<Self>,
    ) -> Result<Self::Stream, Self::Error> {
        let path = request.path.as_str().to_string();
        let sq = self
            .queries
            .read()
            .unwrap()
            .get(&path)
            .cloned()
            .ok_or_else(|| StreamError::QueryNotFound(path))?;

        // Build WebSocket URL from base HTTP URL
        let ws_url = sq
            .base_url
            .replace("http://", "ws://")
            .replace("https://", "wss://");
        let ws_url = format!("{ws_url}/ws");

        let field_configs = sq.field_configs;
        let topic = sq.topic;
        let key = sq.key;

        // Connect to WebSocket
        let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
            .await
            .map_err(|e| StreamError::WebSocket(e.to_string()))?;

        let (mut write, mut read) = futures_util::StreamExt::split(ws_stream);

        // Send subscribe message
        let sub_msg = serde_json::json!({
            "action": "subscribe",
            "topic": &topic,
            "key": &key,
            "history": 100
        });
        use futures_util::SinkExt;
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(
                sub_msg.to_string().into(),
            ))
            .await
            .map_err(|e| StreamError::WebSocket(e.to_string()))?;

        // Return async stream that yields StreamPackets
        Ok(Box::pin(async_stream::try_stream! {
            use futures_util::StreamExt as _;
            while let Some(msg) = read.next().await {
                let msg = msg.map_err(|e| StreamError::WebSocket(e.to_string()))?;
                let text: &str = match &msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => t,
                    _ => continue,
                };

                let value: serde_json::Value = serde_json::from_str(text)
                    .map_err(|e| StreamError::Json(e.to_string()))?;

                let msg_type = value.get("type").and_then(|t| t.as_str()).unwrap_or("");

                match msg_type {
                    "snapshot" => {
                        if let Some(records) = value.get("records").and_then(|r| r.as_array()) {
                            if records.is_empty() {
                                continue;
                            }
                            let frame = build_frame_from_ws_records(
                                &topic, &key, records, &field_configs,
                            ).map_err(StreamError::Frame)?;
                            let packet = backend::StreamPacket::from_frame(
                                frame.check().map_err(|e| StreamError::Frame(e.to_string()))?
                            ).map_err(|e| StreamError::Frame(e.to_string()))?;
                            yield packet;
                        }
                    }
                    "record" => {
                        if let Some(record) = value.get("record") {
                            let records = [record.clone()];
                            let frame = build_frame_from_ws_records(
                                &topic, &key, &records, &field_configs,
                            ).map_err(StreamError::Frame)?;
                            let packet = backend::StreamPacket::from_frame(
                                frame.check().map_err(|e| StreamError::Frame(e.to_string()))?
                            ).map_err(|e| StreamError::Frame(e.to_string()))?;
                            yield packet;
                        }
                    }
                    _ => continue,
                }
            }
        }))
    }

    async fn publish_stream(
        &self,
        _request: backend::PublishStreamRequest<Self>,
    ) -> Result<backend::PublishStreamResponse, Self::Error> {
        Ok(backend::PublishStreamResponse::ok(
            serde_json::Value::Null,
        ))
    }
}
