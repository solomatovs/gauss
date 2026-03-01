use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::PluginError;
use crate::record::TopicRecord;
use crate::storage::{ReadParams, ReadResult};

/// Read TopicRecords from a source topic.
pub trait TopicReader: Send + Sync {
    fn recv(&self) -> Pin<Box<dyn Future<Output = Option<TopicRecord>> + Send + '_>>;
}

/// Write TopicRecords to a target topic.
pub trait TopicWriter: Send + Sync {
    fn send(
        &self,
        record: TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Query any topic (for lookups, joins, etc.).
pub trait TopicInspector: Send + Sync {
    fn query(
        &self,
        topic: &str,
        params: &ReadParams,
    ) -> Pin<Box<dyn Future<Output = Result<ReadResult, PluginError>> + Send + '_>>;

    fn topics(&self) -> Vec<String>;
}

/// Context provided to processors at init time.
///
/// - Source processor: `reader = None`, `writer = Some`
/// - Transform processor: `reader = Some`, `writer = Some`
/// - Sink processor: `reader = Some`, `writer = None`
pub struct ProcessorContext {
    /// Read from source topic (None for source processors).
    pub reader: Option<Arc<dyn TopicReader>>,
    /// Write to target topic (None for sink processors).
    pub writer: Option<Arc<dyn TopicWriter>>,
    /// Query any topic (for lookups, joins).
    pub inspector: Arc<dyn TopicInspector>,
}

/// Processor — the only active entity in the system.
///
/// Three variants:
/// - Source: Transport → framing → TopicRecord → Topic
/// - Transform: Topic → process → Topic
/// - Sink: Topic → process → Transport
///
/// The processor owns its event loop. The engine spawns it as a dedicated task.
pub trait Processor: Send + Sync {
    /// Initialize with context. Called once before `run()`.
    fn init(
        &mut self,
        ctx: ProcessorContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Run the processor. Should block (async) until shutdown.
    fn run(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Signal graceful shutdown.
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}
