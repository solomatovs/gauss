mod http;
mod ws;

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use tokio_util::sync::CancellationToken;

use server_api::{OverflowPolicy, TopicInspector, TopicSubscriber};

#[derive(Clone)]
struct AppState {
    inspector: Arc<dyn TopicInspector>,
    subscriber: Arc<dyn TopicSubscriber>,
    ws_buffer: usize,
    ws_overflow: OverflowPolicy,
}

/// Topic-based HTTP + WebSocket API сервер.
pub async fn run(
    port: u16,
    inspector: Arc<dyn TopicInspector>,
    subscriber: Arc<dyn TopicSubscriber>,
    ws_buffer: usize,
    ws_overflow: OverflowPolicy,
    shutdown: CancellationToken,
) -> Result<(), String> {
    let state = AppState {
        inspector,
        subscriber,
        ws_buffer,
        ws_overflow,
    };

    let app = Router::new()
        .route("/api/topics", get(http::handle_list_topics))
        .route("/api/topics/{name}", get(http::handle_query_topic))
        .route("/ws", get(ws::handle_ws))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|e| format!("bind api :{port}: {e}"))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|e| format!("axum serve: {e}"))?;

    Ok(())
}
