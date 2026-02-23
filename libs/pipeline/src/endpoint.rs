use std::io::{Read, Write};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use server_api::{
    OverflowPolicy, ProcessContext, PluginError, ErrorKind, Codec, Framing, Middleware,
    TransportStream, Transport, TopicProcessor, TopicRecord, TopicSink,
    TopicSource, TopicSubscription, resolve_path, now_ms,
};

use plugin_host::{PluginTransport, PluginFraming, PluginMiddleware, PluginCodec};

use crate::config::{PluginRef, config_json_or_empty};
use crate::PipelineError;

// ═══════════════════════════════════════════════════════════════
//  Overflow-aware send utilities (for source internal channels)
// ═══════════════════════════════════════════════════════════════

/// Блокирующий вариант send с overflow policy (для blocking-контекстов source).
fn blocking_send_with_overflow<T>(
    tx: &mpsc::Sender<T>,
    val: T,
    overflow: OverflowPolicy,
    label: &str,
) -> Result<(), ()> {
    match overflow {
        OverflowPolicy::Drop => match tx.try_send(val) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(%label, "channel full, dropping");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
        },
        OverflowPolicy::BackPressure => tx.blocking_send(val).map_err(|_| ()),
    }
}

// ═══════════════════════════════════════════════════════════════
//  Source Endpoint
// ═══════════════════════════════════════════════════════════════

/// Унифицированный endpoint: transport + framing + [middleware...] + codec.
pub struct Endpoint {
    pub name: String,
    pub transport: Box<dyn Transport>,
    pub framing: Arc<dyn Framing>,
    pub middleware: Vec<Arc<dyn Middleware>>,
    pub codec: Arc<dyn Codec>,
    pub key_field: String,
    pub ts_field: String,
}

impl Endpoint {
    /// Load a pipeline endpoint from plugin paths and config values.
    ///
    /// Загружает transport, framing, middleware chain и codec из .so плагинов.
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        name: &str,
        transport_path: &str,
        transport_config: &Option<toml::Value>,
        framing_path: &str,
        framing_config: &Option<toml::Value>,
        middleware_refs: &[PluginRef],
        codec_path: &str,
        codec_config: &Option<toml::Value>,
    ) -> Result<Self, PipelineError> {
        let transport_json = config_json_or_empty(transport_config)?;
        let framing_json = config_json_or_empty(framing_config)?;
        let codec_json = config_json_or_empty(codec_config)?;

        let transport = PluginTransport::load(transport_path, &transport_json)
            .map_err(|e| PipelineError::PluginLoad { plugin: transport_path.to_string(), source: e })?;
        let framing = PluginFraming::load(framing_path, &framing_json)
            .map_err(|e| PipelineError::PluginLoad { plugin: framing_path.to_string(), source: e })?;
        let codec = PluginCodec::load(codec_path, &codec_json)
            .map_err(|e| PipelineError::PluginLoad { plugin: codec_path.to_string(), source: e })?;

        let mut middleware: Vec<Arc<dyn Middleware>> = Vec::new();
        for (i, mw_ref) in middleware_refs.iter().enumerate() {
            let mw_json = config_json_or_empty(&mw_ref.config)?;
            let mw = PluginMiddleware::load(&mw_ref.plugin, &mw_json)
                .map_err(|e| PipelineError::PluginLoad { plugin: mw_ref.plugin.clone(), source: e })?;
            tracing::info!(
                endpoint = %name,
                index = i,
                plugin = %mw_ref.plugin,
                "loaded middleware"
            );
            middleware.push(Arc::new(mw));
        }

        tracing::info!(
            endpoint = %name,
            transport = %transport_path,
            framing = %framing_path,
            middleware_count = middleware.len(),
            codec = %codec_path,
            "loaded endpoint"
        );

        Ok(Self {
            name: name.to_string(),
            transport: Box::new(transport),
            framing: Arc::new(framing),
            middleware,
            codec: Arc::new(codec),
            key_field: "symbol".to_string(),
            ts_field: "ts_ms".to_string(),
        })
    }

    /// Decode: raw frame bytes → TopicRecord.
    ///
    /// Pipeline: middleware.decode() (forward order) → codec.decode()
    pub fn decode_frame(&self, frame: Vec<u8>) -> Result<TopicRecord, PluginError> {
        decode_frame(frame, &self.middleware, &*self.codec, &self.key_field, &self.ts_field)
    }

    /// Encode: TopicRecord → wire bytes appended to `out`.
    ///
    /// Pipeline: codec.encode() → middleware.encode() (REVERSE order — onion model)
    ///           → framing.encode()
    pub fn encode_to_wire(&self, record: &TopicRecord, out: &mut Vec<u8>) -> Result<(), PluginError> {
        encode_to_wire(record, &*self.framing, &self.middleware, &*self.codec, out)
    }
}

// ═══════════════════════════════════════════════════════════════
//  Source task — transport → framing → codec → ctx.publish()
// ═══════════════════════════════════════════════════════════════

/// Запустить source: transport acceptor → connection readers → ctx.publish().
#[allow(clippy::too_many_arguments)]
pub fn spawn_source(
    mut endpoint: Endpoint,
    target_topic: String,
    ctx: Arc<dyn ProcessContext>,
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

/// Decode raw frame → TopicRecord using middleware (forward) + codec + key extraction.
fn decode_frame(
    frame: Vec<u8>,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
    key_field: &str,
    ts_field: &str,
) -> Result<TopicRecord, PluginError> {
    let data = middleware.iter().try_fold(frame, |d, mw| mw.decode(d))?;
    let (value, record_data) = codec.decode(&data)?;
    let key = resolve_path(&value, key_field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let ts_ms = resolve_path(&value, ts_field)
        .and_then(|v| v.as_i64())
        .unwrap_or_else(now_ms);
    Ok(TopicRecord { ts_ms, key, data: record_data })
}

/// Encode TopicRecord → wire bytes: codec → middleware (reverse) → framing.
fn encode_to_wire(
    record: &TopicRecord,
    framing: &dyn Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
    out: &mut Vec<u8>,
) -> Result<(), PluginError> {
    let data = codec.encode(&record.data)?;
    let data = middleware.iter().rev().try_fold(data, |d, mw| mw.encode(d))?;
    framing.encode(&data, out)
}

/// Блокирующее чтение из одного соединения.
#[allow(clippy::too_many_arguments)]
fn handle_source_connection(
    name: &str,
    mut stream: Box<dyn TransportStream>,
    framing: &dyn Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
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
//  Processor task — subscription → processor.process(ctx)
// ═══════════════════════════════════════════════════════════════

/// Запустить processor task: получает записи через TopicSubscription,
/// вызывает processor.process() с доступом к ProcessContext.
pub fn spawn_processor_task(
    processor: Arc<dyn TopicProcessor>,
    source_topic: String,
    mut subscription: Box<dyn TopicSubscription>,
    ctx: Arc<dyn ProcessContext>,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                record = subscription.recv() => {
                    match record {
                        Some(record) => {
                            if let Err(e) = processor.process(&*ctx, &source_topic, &record).await {
                                tracing::error!(trigger = %source_topic, error = ?e, "processor error");
                            }
                        }
                        None => break,
                    }
                }
                _ = token.cancelled() => break,
            }
        }
        tracing::info!(trigger = %source_topic, "processor stopped");
    })
}

// ═══════════════════════════════════════════════════════════════
//  Sink task — subscription → sink.send()
// ═══════════════════════════════════════════════════════════════

/// Запустить sink task: подписывается на topics через ProcessContext,
/// вызывает sink.send() для каждой записи.
pub async fn spawn_sink_task(
    sink: Arc<dyn TopicSink>,
    sink_name: String,
    topic_names: Vec<String>,
    ctx: &Arc<dyn ProcessContext>,
    buffer: usize,
    overflow: OverflowPolicy,
    token: CancellationToken,
) -> Result<JoinHandle<()>, PipelineError> {
    // Подписаться на все topics через ProcessContext
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
    ctx: &Arc<dyn ProcessContext>,
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
    framing: &dyn Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
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

// ═══════════════════════════════════════════════════════════════
//  Plugin Source task — монолитный source плагин
// ═══════════════════════════════════════════════════════════════

/// Запустить plugin-based source: плагин самостоятельно получает данные
/// и публикует их через ProcessContext.
pub fn spawn_source_plugin_task(
    source: Arc<dyn TopicSource>,
    source_name: String,
    target_topic: String,
    ctx: Arc<dyn ProcessContext>,
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
