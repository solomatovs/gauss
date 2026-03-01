use std::sync::Arc;

use crate::config::ConfigValues;
use crate::error::PluginError;
use crate::format::FormatSerializer;
use crate::mapping::MapSchema;
use crate::record::TopicRecord;

/// Read mode — how a consumer reads from a topic.
/// Determined by the storage, not the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Sequential by cursor (Kafka semantics).
    /// Supported by: ring buffer, file.
    Offset,
    /// Only the latest value (missed ones not needed).
    /// Supported by: ring buffer, file.
    Latest,
    /// Filter by ts_ms range.
    /// Supported by: all.
    Query,
    /// Entire table / all data at once.
    /// Supported by: table, clickhouse, postgres.
    Snapshot,
    /// Snapshot on every change.
    /// Supported by: table.
    Subscribe,
}

/// Parameters for a read operation.
pub struct ReadParams {
    pub mode: ReadMode,
    /// Cursor position for offset reads.
    pub offset: Option<u64>,
    /// Time range filter for query reads.
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub limit: Option<usize>,
}

/// Result of a read operation.
pub struct ReadResult {
    pub records: Vec<TopicRecord>,
    /// Next offset (for offset reads).
    pub next_offset: Option<u64>,
}

/// Context provided to storage at init time.
///
/// - Without deserialization (`format` not in `storage_config`):
///   `serializer = None`, `mapping = None` — writes `record.data` as-is.
///
/// - With deserialization (`format` + `schema_map` in `storage_config`):
///   `serializer` resolved by engine, `mapping` built via Schema Mapping Pipeline.
pub struct StorageContext {
    pub serializer: Option<Arc<dyn FormatSerializer>>,
    pub mapping: Option<MapSchema>,
}

/// Storage plugin trait.
///
/// The engine doesn't enumerate or know concrete implementations.
/// For the engine, storage is just this trait.
pub trait TopicStorage: Send + Sync {
    /// Initialize with context. Called once before data processing starts.
    fn init(&mut self, ctx: StorageContext) -> Result<(), PluginError>;

    /// Save a record.
    fn save(&self, record: TopicRecord) -> Result<(), PluginError>;

    /// Read records according to mode and parameters.
    fn read(&self, mode: &ReadMode, params: &ReadParams) -> Result<ReadResult, PluginError>;

    /// Which read modes this storage supports.
    /// Engine calls this at startup for configuration validation.
    fn supported_read_modes(&self) -> &[ReadMode];

    /// Hot-reload Sighup-context parameters at runtime.
    ///
    /// Called by the engine on SIGHUP after validating that only Sighup-context
    /// params have changed. `config` contains the full new ConfigValues (including
    /// unchanged Postmaster params).
    ///
    /// Default: returns error (plugin does not support runtime reconfiguration).
    fn reconfigure(&self, _config: &ConfigValues) -> Result<(), PluginError> {
        Err(PluginError::logic("reconfigure not supported"))
    }
}
