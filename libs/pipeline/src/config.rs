use serde::Deserialize;

use server_api::OverflowPolicy;

// ═══════════════════════════════════════════════════════════════
//  Format Config
// ═══════════════════════════════════════════════════════════════

/// Именованный формат сериализации. Центральный реестр форматов,
/// на которые ссылаются topic'ы по имени.
#[derive(Debug, Deserialize)]
pub struct FormatConfig {
    /// Уникальное имя формата (e.g., "json", "proto-quote", "csv-quotes").
    pub name: String,
    /// Путь к .so плагину FormatSerializer.
    pub plugin: String,
    /// Конфигурация плагина (schema, delimiter, etc).
    #[serde(default)]
    pub config: Option<toml::Value>,
}

// ═══════════════════════════════════════════════════════════════
//  Topic Config
// ═══════════════════════════════════════════════════════════════

/// Конфигурация topic'а: storage backend.
#[derive(Debug, Deserialize)]
pub struct TopicConfig {
    /// Имя topic'а (e.g., "quotes.raw", "ohlc.1m").
    pub name: String,
    /// Storage backend: "memory" (built-in) или путь к .so плагину.
    #[serde(default = "default_topic_storage")]
    pub storage: String,
    /// Конфигурация storage плагина.
    #[serde(default)]
    pub storage_config: Option<toml::Value>,
    /// Ссылка на именованный формат из [[formats]] (e.g., "json", "proto-quote").
    pub format: String,
    /// Размер буфера по умолчанию для подписчиков этого topic'а.
    #[serde(default = "default_topic_buffer")]
    pub buffer: usize,
    /// Стратегия переполнения по умолчанию для подписчиков.
    #[serde(default = "default_topic_overflow")]
    pub overflow: OverflowPolicy,
}

fn default_topic_storage() -> String {
    "memory".into()
}
fn default_topic_buffer() -> usize {
    4096
}
fn default_topic_overflow() -> OverflowPolicy {
    OverflowPolicy::BackPressure
}

// ═══════════════════════════════════════════════════════════════
//  Processor Config
// ═══════════════════════════════════════════════════════════════

/// Конфигурация processor'а: trigger topic + plugin.
#[derive(Debug, Deserialize)]
pub struct ProcessorConfig {
    /// Путь к .so плагину processor'а.
    pub plugin: String,
    /// Topic-trigger: processor вызывается после каждого save в этот topic.
    pub trigger: String,
    /// Конфигурация processor'а.
    pub config: Option<toml::Value>,
    /// Размер буфера подписки на trigger topic.
    #[serde(default = "default_processor_buffer")]
    pub buffer: usize,
    /// Стратегия переполнения подписки.
    #[serde(default = "default_processor_overflow")]
    pub overflow: OverflowPolicy,
}

fn default_processor_buffer() -> usize {
    4096
}
fn default_processor_overflow() -> OverflowPolicy {
    OverflowPolicy::BackPressure
}

// ═══════════════════════════════════════════════════════════════
//  Source Config
// ═══════════════════════════════════════════════════════════════

/// Конфигурация source: либо pipeline (transport + framing + codec),
/// либо plugin (монолитный .so) — симметрично с SinkConfig.
///
/// Pipeline mode: задаётся transport + framing + codec.
/// Plugin mode:   задаётся plugin (путь к .so монолитного source'а).
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    #[serde(default = "default_source_name")]
    pub name: String,
    /// Topic, в который source публикует записи.
    pub topic: String,

    // --- Plugin mode (монолитный source плагин) ---
    /// Путь к .so плагину source'а. Взаимоисключающе с transport/framing/codec.
    pub plugin: Option<String>,
    /// Конфигурация source плагина (plugin mode only).
    pub config: Option<toml::Value>,

    // --- Pipeline mode (transport + framing + codec) ---
    pub transport: Option<String>,
    pub transport_config: Option<toml::Value>,
    pub framing: Option<String>,
    pub framing_config: Option<toml::Value>,
    #[serde(default)]
    pub middleware: Vec<PluginRef>,
    pub codec: Option<String>,
    pub codec_config: Option<toml::Value>,

    // --- Key extraction (pipeline mode) ---
    /// Field path to extract as TopicRecord.key (default: "symbol").
    #[serde(default = "default_key_field")]
    pub key_field: String,
    /// Field path to extract as TopicRecord.ts_ms (default: "ts_ms").
    #[serde(default = "default_ts_field")]
    pub ts_field: String,

    // --- Common ---
    /// Размер буфера канала source → processing loop.
    #[serde(default = "default_source_buffer", alias = "source_buffer")]
    pub buffer: usize,
    /// Стратегия переполнения канала source → processing.
    #[serde(default = "default_source_overflow", alias = "source_overflow")]
    pub overflow: OverflowPolicy,
    /// Размер буфера канала передачи TCP соединений (pipeline mode).
    #[serde(default = "default_conn_buffer")]
    pub conn_buffer: usize,
    /// Стратегия переполнения канала передачи соединений.
    #[serde(default = "default_conn_overflow")]
    pub conn_overflow: OverflowPolicy,
}

impl SourceConfig {
    /// Pipeline source (transport + framing + codec)?
    pub fn is_pipeline(&self) -> bool {
        self.transport.is_some()
    }

    /// Validate: либо plugin, либо pipeline, не оба.
    pub fn validate(&self) -> Result<(), String> {
        let has_plugin = self.plugin.is_some();
        let has_pipeline = self.transport.is_some()
            || self.framing.is_some()
            || self.codec.is_some();

        if has_plugin && has_pipeline {
            return Err(format!(
                "source [{}]: cannot specify both 'plugin' and 'transport/framing/codec'",
                self.name
            ));
        }
        if !has_plugin && !has_pipeline {
            return Err(format!(
                "source [{}]: must specify either 'plugin' or 'transport + framing + codec'",
                self.name
            ));
        }
        if has_pipeline
            && (self.framing.is_none() || self.codec.is_none()) {
                return Err(format!(
                    "source [{}]: pipeline mode requires all of: transport, framing, codec",
                    self.name
                ));
            }
        Ok(())
    }
}

fn default_source_name() -> String {
    "unnamed".into()
}
fn default_source_buffer() -> usize {
    8192
}
fn default_source_overflow() -> OverflowPolicy {
    OverflowPolicy::BackPressure
}
fn default_conn_buffer() -> usize {
    4
}
fn default_conn_overflow() -> OverflowPolicy {
    OverflowPolicy::BackPressure
}
fn default_key_field() -> String {
    "symbol".into()
}
fn default_ts_field() -> String {
    "ts_ms".into()
}

/// Ссылка на плагин с опциональным конфигом.
#[derive(Debug, Deserialize)]
pub struct PluginRef {
    pub plugin: String,
    pub config: Option<toml::Value>,
}

// ═══════════════════════════════════════════════════════════════
//  Sink Config
// ═══════════════════════════════════════════════════════════════

/// Конфигурация sink: либо plugin-based (монолитный), либо pipeline-based
/// (transport + framing + middleware + codec) — симметрично с Source.
///
/// Pipeline mode: задаётся transport + framing + codec (зеркало Source).
/// Plugin mode:   задаётся plugin (путь к .so монолитного sink'а).
#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(default = "default_sink_name")]
    pub name: String,

    // --- Plugin mode (монолитный sink плагин) ---
    /// Путь к .so плагину sink'а. Взаимоисключающе с transport/framing/codec.
    pub plugin: Option<String>,
    /// Конфигурация sink плагина (plugin mode only).
    pub config: Option<toml::Value>,

    // --- Pipeline mode (symmetric with Source) ---
    /// Transport plugin path (tcp-server, tcp-client .so).
    pub transport: Option<String>,
    pub transport_config: Option<toml::Value>,
    /// Framing plugin path (lines, length-prefixed .so).
    pub framing: Option<String>,
    pub framing_config: Option<toml::Value>,
    /// Middleware chain (encode применяется в ОБРАТНОМ порядке — onion model).
    #[serde(default)]
    pub middleware: Vec<PluginRef>,
    /// Codec plugin path (json, csv .so).
    pub codec: Option<String>,
    pub codec_config: Option<toml::Value>,

    // --- Common ---
    /// Topics, на которые подписан sink (broadcast push).
    #[serde(default)]
    pub topics: Vec<String>,
    /// Размер буфера канала для sink'а.
    #[serde(default = "default_sink_buffer")]
    pub buffer: usize,
    /// Стратегия переполнения.
    #[serde(default = "default_sink_overflow")]
    pub overflow: OverflowPolicy,

    // --- Pipeline mode: connection management ---
    /// Размер буфера канала передачи TCP соединений (pipeline mode).
    #[serde(default = "default_conn_buffer")]
    pub conn_buffer: usize,
    /// Стратегия переполнения канала передачи соединений.
    #[serde(default = "default_conn_overflow")]
    pub conn_overflow: OverflowPolicy,
}

impl SinkConfig {
    /// Pipeline sink (transport + framing + codec)?
    pub fn is_pipeline(&self) -> bool {
        self.transport.is_some()
    }

    /// Validate: либо plugin, либо pipeline, не оба.
    pub fn validate(&self) -> Result<(), String> {
        let has_plugin = self.plugin.is_some();
        let has_pipeline = self.transport.is_some()
            || self.framing.is_some()
            || self.codec.is_some();

        if has_plugin && has_pipeline {
            return Err(format!(
                "sink [{}]: cannot specify both 'plugin' and 'transport/framing/codec'",
                self.name
            ));
        }
        if !has_plugin && !has_pipeline {
            return Err(format!(
                "sink [{}]: must specify either 'plugin' or 'transport + framing + codec'",
                self.name
            ));
        }
        if has_pipeline
            && (self.framing.is_none() || self.codec.is_none()) {
                return Err(format!(
                    "sink [{}]: pipeline mode requires all of: transport, framing, codec",
                    self.name
                ));
            }
        Ok(())
    }
}

fn default_sink_name() -> String {
    "unnamed".into()
}
fn default_sink_buffer() -> usize {
    8192
}
fn default_sink_overflow() -> OverflowPolicy {
    OverflowPolicy::Drop
}

// ═══════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════

/// Сериализовать Option<toml::Value> в JSON-строку (или "{}").
pub fn config_json_or_empty(val: &Option<toml::Value>) -> Result<String, crate::PipelineError> {
    match val {
        Some(v) => Ok(serde_json::to_string(v)?),
        None => Ok("{}".to_string()),
    }
}
