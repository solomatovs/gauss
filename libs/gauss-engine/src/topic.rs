use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::broadcast;

use gauss_api::config::ConfigValues;
use gauss_api::error::PluginError;
use gauss_api::processor::{TopicInspector, TopicReader, TopicWriter};
use gauss_api::record::TopicRecord;
use gauss_api::storage::{ReadMode, ReadParams, ReadResult, TopicStorage};

/// A named topic backed by a storage plugin.
pub struct Topic {
    name: String,
    storage: Box<dyn TopicStorage>,
    /// Notification channel: broadcast unit signal on every save.
    notify_tx: broadcast::Sender<()>,
}

impl std::fmt::Debug for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Topic").field("name", &self.name).finish()
    }
}

impl Topic {
    pub fn new(name: String, storage: Box<dyn TopicStorage>) -> Self {
        let (notify_tx, _) = broadcast::channel(64);
        Self {
            name,
            storage,
            notify_tx,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn save(&self, record: TopicRecord) -> Result<(), PluginError> {
        self.storage.save(record)?;
        // Notify subscribers (ignore if no receivers).
        let _ = self.notify_tx.send(());
        Ok(())
    }

    pub fn read(&self, mode: &ReadMode, params: &ReadParams) -> Result<ReadResult, PluginError> {
        self.storage.read(mode, params)
    }

    pub fn supported_read_modes(&self) -> &[ReadMode] {
        self.storage.supported_read_modes()
    }

    pub fn subscribe_notify(&self) -> broadcast::Receiver<()> {
        self.notify_tx.subscribe()
    }

    /// Hot-reload Sighup-context parameters on the underlying storage.
    pub fn reconfigure(&self, config: &ConfigValues) -> Result<(), PluginError> {
        self.storage.reconfigure(config)
    }
}

/// Registry of all topics in the engine.
///
/// Uses interior mutability so that new topics can be added at runtime (SIGHUP reload).
#[derive(Debug)]
pub struct TopicRegistry {
    topics: std::sync::RwLock<HashMap<String, Arc<Topic>>>,
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self {
            topics: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, topic: Topic) {
        let name = topic.name.clone();
        let mut guard = match self.topics.write() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::warn!("topic registry write lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        guard.insert(name, Arc::new(topic));
    }

    pub fn get(&self, name: &str) -> Option<Arc<Topic>> {
        let guard = match self.topics.read() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::warn!("topic registry read lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        guard.get(name).cloned()
    }

    pub fn topic_names(&self) -> Vec<String> {
        let guard = match self.topics.read() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::warn!("topic registry read lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        guard.keys().cloned().collect()
    }

    pub fn contains(&self, name: &str) -> bool {
        let guard = match self.topics.read() {
            Ok(g) => g,
            Err(poisoned) => {
                tracing::warn!("topic registry read lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        guard.contains_key(name)
    }
}

// ---------------------------------------------------------------------------
// TopicWriter implementation — writes to a specific topic
// ---------------------------------------------------------------------------

pub struct RegistryTopicWriter {
    topic: Arc<Topic>,
}

impl RegistryTopicWriter {
    pub fn new(topic: Arc<Topic>) -> Self {
        Self { topic }
    }
}

impl TopicWriter for RegistryTopicWriter {
    fn send(
        &self,
        record: TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async move { self.topic.save(record) })
    }
}

// ---------------------------------------------------------------------------
// TopicReader implementation — reads from a specific topic with a read mode
// ---------------------------------------------------------------------------

pub struct RegistryTopicReader {
    topic: Arc<Topic>,
    mode: ReadMode,
    offset: AtomicU64,
    notify_rx: tokio::sync::Mutex<broadcast::Receiver<()>>,
}

impl RegistryTopicReader {
    pub fn new(topic: Arc<Topic>, mode: ReadMode) -> Self {
        let notify_rx = topic.subscribe_notify();
        Self {
            topic,
            mode,
            offset: AtomicU64::new(0),
            notify_rx: tokio::sync::Mutex::new(notify_rx),
        }
    }
}

impl TopicReader for RegistryTopicReader {
    fn recv(&self) -> Pin<Box<dyn Future<Output = Option<TopicRecord>> + Send + '_>> {
        Box::pin(async move {
            loop {
                let params = ReadParams {
                    mode: self.mode,
                    offset: Some(self.offset.load(Ordering::Relaxed)),
                    from_ms: None,
                    to_ms: None,
                    limit: Some(1),
                };

                match self.topic.read(&self.mode, &params) {
                    Ok(result) => {
                        if let Some(record) = result.records.into_iter().next() {
                            if let Some(next) = result.next_offset {
                                self.offset.store(next, Ordering::Relaxed);
                            }
                            return Some(record);
                        }
                        // No data yet — wait for notification.
                        let mut rx = self.notify_rx.lock().await;
                        // Ignore lag errors (we'll just re-read).
                        let _ = rx.recv().await;
                    }
                    Err(_) => return None,
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// TopicInspector implementation — query any topic by name
// ---------------------------------------------------------------------------

pub struct RegistryTopicInspector {
    registry: Arc<TopicRegistry>,
}

impl RegistryTopicInspector {
    pub fn new(registry: Arc<TopicRegistry>) -> Self {
        Self { registry }
    }
}

impl TopicInspector for RegistryTopicInspector {
    fn query(
        &self,
        topic: &str,
        params: &ReadParams,
    ) -> Pin<Box<dyn Future<Output = Result<ReadResult, PluginError>> + Send + '_>> {
        let topic = self.registry.get(topic).map(Ok).unwrap_or_else(|| {
            Err(PluginError::logic(format!("topic not found: {topic}")))
        });
        // Copy params to avoid lifetime conflict between &self and &params.
        let owned_params = ReadParams {
            mode: params.mode,
            offset: params.offset,
            from_ms: params.from_ms,
            to_ms: params.to_ms,
            limit: params.limit,
        };
        Box::pin(async move {
            let topic = topic?;
            topic.read(&owned_params.mode, &owned_params)
        })
    }

    fn topics(&self) -> Vec<String> {
        self.registry.topic_names()
    }
}
