use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use server_api::{
    OverflowPolicy, PluginError, Codec, Framing, Middleware,
    Transport, TopicProcessor, TopicRecord,
    TopicPublisher, TopicSubscription,
    TopicCodec, TopicInspector, TopicContext,
    resolve_path, now_ms,
};

use plugin_host::{PluginTransport, PluginFraming, PluginMiddleware, PluginCodec};

use crate::config::{PluginRef, config_json_or_empty};
use crate::PipelineError;

// ═══════════════════════════════════════════════════════════════
//  Overflow-aware send utilities
// ═══════════════════════════════════════════════════════════════

/// Блокирующий вариант send с overflow policy (для blocking-контекстов).
pub(crate) fn blocking_send_with_overflow<T>(
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
//  Endpoint — transport + framing + middleware + codec composition
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
//  Shared encode/decode helpers
// ═══════════════════════════════════════════════════════════════

/// Decode raw frame → TopicRecord using middleware (forward) + codec + key extraction.
///
/// Создаёт TopicRecord с `value` (структурированные данные) и `raw`
/// (оригинальные байты после middleware для zero-copy transport).
pub(crate) fn decode_frame(
    frame: Vec<u8>,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
    key_field: &str,
    ts_field: &str,
) -> Result<TopicRecord, PluginError> {
    let data = middleware.iter().try_fold(frame, |d, mw| mw.decode(d))?;
    let value = codec.decode(&data)?;
    let key = resolve_path(&value, key_field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let ts_ms = resolve_path(&value, ts_field)
        .and_then(|v| v.as_i64())
        .unwrap_or_else(now_ms);
    Ok(TopicRecord {
        ts_ms,
        key,
        value,
        raw: Some(server_api::RawPayload::new(data, codec.data_format())),
    })
}

/// Encode TopicRecord → wire bytes: codec → middleware (reverse) → framing.
///
/// Zero-copy: если raw bytes есть и формат совпадает с codec,
/// используем напрямую без повторной сериализации.
pub(crate) fn encode_to_wire(
    record: &TopicRecord,
    framing: &dyn Framing,
    middleware: &[Arc<dyn Middleware>],
    codec: &dyn Codec,
    out: &mut Vec<u8>,
) -> Result<(), PluginError> {
    let data = match &record.raw {
        Some(raw) if raw.format == codec.data_format() => raw.bytes.clone(),
        _ => codec.encode(&record.value)?,
    };
    let data = middleware.iter().rev().try_fold(data, |d, mw| mw.encode(d))?;
    framing.encode(&data, out)
}

// ═══════════════════════════════════════════════════════════════
//  Processor task — subscription → processor.process(ctx)
// ═══════════════════════════════════════════════════════════════

/// Запустить processor task: получает записи через TopicSubscription,
/// вызывает processor.process() с доступом к TopicContext.
pub fn spawn_processor_task(
    processor: Arc<dyn TopicProcessor>,
    source_topic: String,
    mut subscription: Box<dyn TopicSubscription>,
    publisher: Arc<dyn TopicPublisher>,
    codec: Arc<dyn TopicCodec>,
    inspector: Arc<dyn TopicInspector>,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                record = subscription.recv() => {
                    match record {
                        Some(record) => {
                            let ctx = TopicContext {
                                publisher: &*publisher,
                                codec: &*codec,
                                inspector: &*inspector,
                            };
                            if let Err(e) = processor.process(ctx, &source_topic, &record).await {
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
