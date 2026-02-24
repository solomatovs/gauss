use std::io::Read;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use server_api::{
    OverflowPolicy, ErrorKind, Middleware,
    TransportStream, TopicRecord, TopicSource,
    TopicPublisher,
};

use crate::endpoint::{Endpoint, decode_frame, blocking_send_with_overflow};

// ═══════════════════════════════════════════════════════════════
//  Source task — transport → framing → codec → ctx.publish()
// ═══════════════════════════════════════════════════════════════

/// Запустить source: transport acceptor → connection readers → ctx.publish().
#[allow(clippy::too_many_arguments)]
pub fn spawn_source(
    mut endpoint: Endpoint,
    target_topic: String,
    ctx: Arc<dyn TopicPublisher>,
    source_buffer: usize,
    source_overflow: OverflowPolicy,
    conn_buffer: usize,
    conn_overflow: OverflowPolicy,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let name = endpoint.name.clone();
        let framing = endpoint.framing.clone();
        let middleware = endpoint.middleware.clone();
        let codec = endpoint.codec.clone();
        let key_field = endpoint.key_field.clone();
        let ts_field = endpoint.ts_field.clone();

        // Канал: per-connection tasks → processing loop
        let (record_tx, mut record_rx) = mpsc::channel::<TopicRecord>(source_buffer);

        // Blocking acceptor thread
        let (conn_tx, mut conn_rx) = mpsc::channel::<Box<dyn TransportStream>>(conn_buffer);

        let accept_name = name.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = endpoint.transport.start() {
                tracing::error!(source = %accept_name, error = ?e, "transport start error");
                return;
            }
            tracing::info!(source = %accept_name, "transport started");

            loop {
                match endpoint.transport.next_connection() {
                    Ok(Some(stream)) => {
                        if blocking_send_with_overflow(
                            &conn_tx,
                            stream,
                            conn_overflow,
                            &format!("source [{accept_name}] conn"),
                        )
                        .is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::info!(source = %accept_name, "transport closed");
                        break;
                    }
                    Err(e) => {
                        tracing::error!(source = %accept_name, error = ?e, "transport error");
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        continue;
                    }
                }
            }
            let _ = endpoint.transport.stop();
        });

        // Connection receiver: получает TransportStream, спавнит blocking reader
        let conn_name = name.clone();
        let conn_framing = framing.clone();
        let conn_middleware = middleware.clone();
        let conn_codec = codec.clone();
        let conn_key_field = key_field.clone();
        let conn_ts_field = ts_field.clone();
        let conn_token = token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    stream = conn_rx.recv() => {
                        match stream {
                            Some(stream) => {
                                let peer = stream.peer_info();
                                tracing::info!(source = %conn_name, %peer, "new connection");

                                let task_name = conn_name.clone();
                                let task_framing = conn_framing.clone();
                                let task_middleware = conn_middleware.clone();
                                let task_codec = conn_codec.clone();
                                let task_key_field = conn_key_field.clone();
                                let task_ts_field = conn_ts_field.clone();
                                let task_tx = record_tx.clone();

                                tokio::task::spawn_blocking(move || {
                                    handle_source_connection(
                                        &task_name,
                                        stream,
                                        &*task_framing,
                                        &task_middleware,
                                        &*task_codec,
                                        &task_key_field,
                                        &task_ts_field,
                                        &task_tx,
                                        source_overflow,
                                    );
                                    tracing::info!(source = %task_name, %peer, "connection closed");
                                });
                            }
                            None => break,
                        }
                    }
                    _ = conn_token.cancelled() => break,
                }
            }
        });

        // Async loop: получает TopicRecord → publish to topic
        loop {
            tokio::select! {
                record = record_rx.recv() => {
                    match record {
                        Some(record) => {
                            if let Err(e) = ctx.publish(&target_topic, record).await {
                                tracing::error!(source = %name, error = ?e, "publish error");
                            }
                        }
                        None => break,
                    }
                }
                _ = token.cancelled() => break,
            }
        }

        tracing::info!(source = %name, "finished");
    })
}

/// Блокирующее чтение из одного соединения.
#[allow(clippy::too_many_arguments)]
fn handle_source_connection(
    name: &str,
    mut stream: Box<dyn TransportStream>,
    framing: &dyn server_api::Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn server_api::Codec,
    key_field: &str,
    ts_field: &str,
    tx: &mpsc::Sender<TopicRecord>,
    overflow: OverflowPolicy,
) {
    let mut buf = Vec::with_capacity(8192);
    let mut tmp = [0u8; 4096];

    loop {
        loop {
            match framing.decode(&buf) {
                Ok(Some((frame, consumed))) => {
                    buf.drain(..consumed);

                    match decode_frame(frame, middleware, codec, key_field, ts_field) {
                        Ok(record) => {
                            if blocking_send_with_overflow(
                                tx,
                                record,
                                overflow,
                                &format!("source [{name}]"),
                            )
                            .is_err()
                            {
                                return;
                            }
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::Format => {
                                tracing::warn!(source = %name, error = ?e, "bad record, skipping");
                            }
                            _ => {
                                tracing::error!(source = %name, error = ?e, "decode error, disconnecting");
                                return;
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    tracing::error!(source = %name, error = ?e, "framing error");
                    return;
                }
            }
        }

        match stream.read(&mut tmp) {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) => {
                tracing::error!(source = %name, error = %e, "read error");
                return;
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  Plugin Source task — монолитный source плагин
// ═══════════════════════════════════════════════════════════════

/// Запустить plugin-based source: плагин самостоятельно получает данные
/// и публикует их через TopicPublisher.
pub fn spawn_source_plugin_task(
    source: Arc<dyn TopicSource>,
    source_name: String,
    target_topic: String,
    ctx: Arc<dyn TopicPublisher>,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        tracing::info!(source = %source_name, "plugin starting");
        tokio::select! {
            result = source.start(ctx, &target_topic) => {
                if let Err(e) = result {
                    tracing::error!(source = %source_name, error = ?e, "plugin error");
                }
            }
            _ = token.cancelled() => {
                tracing::info!(source = %source_name, "plugin cancellation requested");
                if let Err(e) = source.stop().await {
                    tracing::error!(source = %source_name, error = ?e, "plugin stop error");
                }
            }
        }
        tracing::info!(source = %source_name, "plugin stopped");
    })
}
