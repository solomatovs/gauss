use std::io::Write;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use server_api::{
    OverflowPolicy, ErrorKind, Middleware,
    TransportStream, TopicRecord, TopicSink,
    TopicSubscription, TopicSubscriber,
};

use crate::endpoint::{Endpoint, encode_to_wire, blocking_send_with_overflow};
use crate::PipelineError;

// ═══════════════════════════════════════════════════════════════
//  Sink task — subscription → sink.send()
// ═══════════════════════════════════════════════════════════════

/// Запустить sink task: подписывается на topics через TopicSubscriber,
/// вызывает sink.send() для каждой записи.
pub async fn spawn_sink_task(
    sink: Arc<dyn TopicSink>,
    sink_name: String,
    topic_names: Vec<String>,
    ctx: &Arc<dyn TopicSubscriber>,
    buffer: usize,
    overflow: OverflowPolicy,
    token: CancellationToken,
) -> Result<JoinHandle<()>, PipelineError> {
    // Подписаться на все topics через TopicSubscriber
    let mut subscriptions: Vec<(String, Box<dyn TopicSubscription>)> = Vec::new();
    for topic_name in &topic_names {
        let sub = ctx.subscribe(topic_name, buffer, overflow).await
            .map_err(|e| PipelineError::Subscription { topic: topic_name.clone(), source: e })?;
        subscriptions.push((topic_name.clone(), sub));
    }

    Ok(tokio::spawn(async move {
        if subscriptions.is_empty() {
            tracing::warn!(sink = %sink_name, "no topics subscribed, stopping");
            return;
        }

        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for (topic_name, mut sub) in subscriptions {
            let sink = sink.clone();
            let name = sink_name.clone();
            let t = token.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        record = sub.recv() => {
                            match record {
                                Some(record) => {
                                    if let Err(e) = sink.send(&topic_name, &record).await {
                                        tracing::error!(sink = %name, topic = %topic_name, error = ?e, "send error");
                                    }
                                }
                                None => break,
                            }
                        }
                        _ = t.cancelled() => break,
                    }
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
        tracing::info!(sink = %sink_name, "stopped");
    }))
}

// ═══════════════════════════════════════════════════════════════
//  Pipeline Sink — ctx.subscribe() → codec → framing → transport
// ═══════════════════════════════════════════════════════════════

/// Запустить pipeline sink: subscribe topics → codec.encode → middleware.encode
/// (reverse) → framing.encode → transport.write_all.
///
/// Supports fan-out: если transport = tcp-server, все принятые соединения
/// получают все записи. Если tcp-client — одно исходящее соединение.
///
/// Зеркало `spawn_source`.
#[allow(clippy::too_many_arguments)]
pub async fn spawn_pipeline_sink(
    mut endpoint: Endpoint,
    topic_names: Vec<String>,
    ctx: &Arc<dyn TopicSubscriber>,
    sink_buffer: usize,
    sink_overflow: OverflowPolicy,
    conn_buffer: usize,
    conn_overflow: OverflowPolicy,
    token: CancellationToken,
) -> Result<JoinHandle<()>, PipelineError> {
    // Подписаться на все topics заранее
    let mut subscriptions: Vec<(String, Box<dyn TopicSubscription>)> = Vec::new();
    for topic_name in &topic_names {
        let sub = ctx.subscribe(topic_name, sink_buffer, sink_overflow).await
            .map_err(|e| PipelineError::Subscription { topic: topic_name.clone(), source: e })?;
        subscriptions.push((topic_name.clone(), sub));
    }

    Ok(tokio::spawn(async move {
        let name = endpoint.name.clone();
        let framing = endpoint.framing.clone();
        let middleware = endpoint.middleware.clone();
        let codec = endpoint.codec.clone();

        // Broadcast channel: main loop → all per-connection writer tasks (fan-out).
        let (bcast_tx, _) = broadcast::channel::<TopicRecord>(sink_buffer);

        // ── Tier 2: Transport acceptor (blocking) ──
        let accept_name = name.clone();
        let accept_framing = framing.clone();
        let accept_middleware = middleware.clone();
        let accept_codec = codec.clone();
        let accept_bcast_tx = bcast_tx.clone();

        let (conn_tx, mut conn_rx) = mpsc::channel::<Box<dyn TransportStream>>(conn_buffer);

        tokio::task::spawn_blocking(move || {
            if let Err(e) = endpoint.transport.start() {
                tracing::error!(sink = %accept_name, error = ?e, "transport start error");
                return;
            }
            tracing::info!(sink = %accept_name, "transport started");

            loop {
                match endpoint.transport.next_connection() {
                    Ok(Some(stream)) => {
                        if blocking_send_with_overflow(
                            &conn_tx,
                            stream,
                            conn_overflow,
                            &format!("sink [{accept_name}] conn"),
                        )
                        .is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::info!(sink = %accept_name, "transport closed");
                        break;
                    }
                    Err(e) => {
                        tracing::error!(sink = %accept_name, error = ?e, "transport error");
                        std::thread::sleep(std::time::Duration::from_secs(1));
                        continue;
                    }
                }
            }
            let _ = endpoint.transport.stop();
        });

        // ── Connection receiver: spawn per-connection blocking writer ──
        let conn_name = name.clone();
        let conn_token = token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    stream = conn_rx.recv() => {
                        match stream {
                            Some(stream) => {
                                let peer = stream.peer_info();
                                tracing::info!(sink = %conn_name, %peer, "new connection");

                                let task_name = conn_name.clone();
                                let task_framing = accept_framing.clone();
                                let task_middleware = accept_middleware.clone();
                                let task_codec = accept_codec.clone();
                                let mut task_rx = accept_bcast_tx.subscribe();

                                tokio::task::spawn_blocking(move || {
                                    handle_sink_connection(
                                        &task_name,
                                        stream,
                                        &*task_framing,
                                        &task_middleware,
                                        &*task_codec,
                                        &mut task_rx,
                                    );
                                    tracing::info!(sink = %task_name, %peer, "connection closed");
                                });
                            }
                            None => break,
                        }
                    }
                    _ = conn_token.cancelled() => break,
                }
            }
        });

        // ── Tier 1: Topic subscription → broadcast to all connections ──
        if subscriptions.is_empty() {
            tracing::warn!(sink = %name, "no topics subscribed, stopping");
            return;
        }

        if subscriptions.len() == 1 {
            // Single topic — no merge needed
            let (_topic_name, mut sub) = subscriptions.into_iter().next().unwrap();
            loop {
                tokio::select! {
                    record = sub.recv() => {
                        match record {
                            Some(record) => { let _ = bcast_tx.send(record); }
                            None => break,
                        }
                    }
                    _ = token.cancelled() => break,
                }
            }
        } else {
            // Multiple topics: merge into single mpsc, then broadcast
            let (merge_tx, mut merge_rx) = mpsc::channel::<TopicRecord>(sink_buffer);

            for (topic_name, mut sub) in subscriptions {
                let tx = merge_tx.clone();
                let sink_name = name.clone();
                let t = token.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            record = sub.recv() => {
                                match record {
                                    Some(record) => {
                                        if tx.send(record).await.is_err() {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                            _ = t.cancelled() => break,
                        }
                    }
                    tracing::info!(sink = %sink_name, %topic_name, "topic subscription ended");
                });
            }
            drop(merge_tx);

            loop {
                tokio::select! {
                    record = merge_rx.recv() => {
                        match record {
                            Some(record) => { let _ = bcast_tx.send(record); }
                            None => break,
                        }
                    }
                    _ = token.cancelled() => break,
                }
            }
        }

        tracing::info!(sink = %name, "finished");
    }))
}

/// Блокирующая запись в одно соединение (зеркало handle_source_connection).
///
/// Pipeline: TopicRecord → codec.encode() → middleware.encode() (REVERSE order)
///           → framing.encode() → stream.write_all()
fn handle_sink_connection(
    name: &str,
    mut stream: Box<dyn TransportStream>,
    framing: &dyn server_api::Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn server_api::Codec,
    rx: &mut broadcast::Receiver<TopicRecord>,
) {
    let mut buf = Vec::with_capacity(8192);

    loop {
        let record = match rx.blocking_recv() {
            Ok(record) => record,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(sink = %name, skipped = n, "lagged, skipped messages");
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };

        buf.clear();
        match encode_to_wire(&record, framing, middleware, codec, &mut buf) {
            Ok(()) => {}
            Err(e) => match e.kind() {
                ErrorKind::Format => {
                    tracing::warn!(sink = %name, error = ?e, "bad record, skipping");
                    continue;
                }
                _ => {
                    tracing::error!(sink = %name, error = ?e, "encode error, disconnecting");
                    return;
                }
            }
        }

        if let Err(e) = stream.write_all(&buf) {
            tracing::error!(sink = %name, error = %e, "write error");
            return;
        }
    }
}
