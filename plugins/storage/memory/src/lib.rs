use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::RwLock;

use server_api::{PluginError, RecordSchema, StorageFactory, TopicQuery, TopicRecord, TopicStorage, SortOrder};

// ═══════════════════════════════════════════════════════════════
//  MemoryStorageConfig
// ═══════════════════════════════════════════════════════════════

fn default_max_records() -> usize {
    100_000
}

#[derive(Debug, serde::Deserialize)]
pub struct MemoryStorageConfig {
    #[serde(default = "default_max_records")]
    pub max_records: usize,
}

impl Default for MemoryStorageConfig {
    fn default() -> Self {
        Self {
            max_records: default_max_records(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  MemoryStorage
// ═══════════════════════════════════════════════════════════════

/// In-memory ring-buffer storage. Для topic'ов, не требующих
/// дисковой persistence (e.g., "quotes.raw" для real-time).
pub struct MemoryStorage {
    records: RwLock<VecDeque<TopicRecord>>,
    max_records: usize,
}

impl MemoryStorage {
    pub fn new(max_records: usize) -> Self {
        Self {
            records: RwLock::new(VecDeque::with_capacity(max_records.min(65536))),
            max_records,
        }
    }
}

impl TopicStorage for MemoryStorage {
    fn init(&self, _schema: Option<RecordSchema>) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn save(
        &self,
        records: &[TopicRecord],
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let records = records.to_vec();
        Box::pin(async move {
            let mut buf = self.records.write().await;
            for record in records {
                if buf.len() >= self.max_records {
                    buf.pop_front();
                }
                buf.push_back(record);
            }
            Ok(())
        })
    }

    fn query(
        &self,
        query: &TopicQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        let query = query.clone();
        Box::pin(async move {
            let buf = self.records.read().await;
            let mut result: Vec<TopicRecord> = buf
                .iter()
                .filter(|r| {
                    if let Some(ref key) = query.key {
                        if r.key != *key {
                            return false;
                        }
                    }
                    if let Some(from) = query.from_ms {
                        if r.ts_ms < from {
                            return false;
                        }
                    }
                    if let Some(to) = query.to_ms {
                        if r.ts_ms >= to {
                            return false;
                        }
                    }
                    true
                })
                .cloned()
                .collect();

            match query.order {
                SortOrder::Desc => result.reverse(),
                SortOrder::Asc => {}
            }

            let offset = query.offset.unwrap_or(0);
            if offset > 0 && offset < result.len() {
                result = result.split_off(offset);
            } else if offset >= result.len() {
                result.clear();
            }

            if let Some(limit) = query.limit {
                result.truncate(limit);
            }

            Ok(result)
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ═══════════════════════════════════════════════════════════════
//  MemoryStorageFactory
// ═══════════════════════════════════════════════════════════════

pub struct MemoryStorageFactory;

impl StorageFactory for MemoryStorageFactory {
    fn create(&self, config_json: &str) -> Result<Arc<dyn TopicStorage>, PluginError> {
        let config: MemoryStorageConfig = if config_json == "{}" {
            MemoryStorageConfig::default()
        } else {
            serde_json::from_str(config_json)?
        };
        Ok(Arc::new(MemoryStorage::new(config.max_records)))
    }
}
