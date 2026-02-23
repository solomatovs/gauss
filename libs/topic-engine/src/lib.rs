pub mod error;

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use server_api::{
    FormatSerializer, OverflowPolicy, PluginError, ProcessContext, RecordData, TopicQuery,
    TopicRecord, TopicStorage, TopicSubscription,
};

pub use error::TopicError;

// ═══════════════════════════════════════════════════════════════
//  Subscriber
// ═══════════════════════════════════════════════════════════════

struct Subscriber {
    tx: mpsc::Sender<TopicRecord>,
    overflow: OverflowPolicy,
}

// ═══════════════════════════════════════════════════════════════
//  MpscSubscription — server-side TopicSubscription impl
// ═══════════════════════════════════════════════════════════════

pub struct MpscSubscription {
    rx: mpsc::Receiver<TopicRecord>,
}

impl TopicSubscription for MpscSubscription {
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<TopicRecord>> + Send + '_>> {
        Box::pin(async { self.rx.recv().await })
    }
}

// ═══════════════════════════════════════════════════════════════
//  Topic
// ═══════════════════════════════════════════════════════════════

/// Именованный канал данных: storage + unified subscribers.
///
/// Все подписчики (processors, sinks, WS) получают данные
/// через единый механизм mpsc с overflow policy.
pub struct Topic {
    pub name: String,
    storage: Arc<dyn TopicStorage>,
    subscribers: RwLock<Vec<Subscriber>>,
    format: Arc<dyn FormatSerializer>,
}

impl Topic {
    pub fn new(
        name: String,
        storage: Arc<dyn TopicStorage>,
        format: Arc<dyn FormatSerializer>,
    ) -> Self {
        Self {
            name,
            storage,
            subscribers: RwLock::new(Vec::new()),
            format,
        }
    }

    /// Подписаться на topic. Возвращает MpscSubscription.
    pub async fn subscribe(
        &self,
        buffer: usize,
        overflow: OverflowPolicy,
    ) -> MpscSubscription {
        let (tx, rx) = mpsc::channel(buffer);
        let mut subs = self.subscribers.write().await;
        subs.push(Subscriber { tx, overflow });
        MpscSubscription { rx }
    }

    /// Опубликовать запись: save → notify all subscribers.
    pub async fn publish(&self, record: TopicRecord) -> Result<(), TopicError> {
        // 1. Persist to storage
        self.storage.save(&[record.clone()]).await
            .map_err(TopicError::Storage)?;

        // 2. Send to all subscribers
        let mut subs = self.subscribers.write().await;
        let mut i = 0;
        while i < subs.len() {
            let sub = &subs[i];
            if sub.tx.is_closed() {
                subs.swap_remove(i);
                continue;
            }
            match sub.overflow {
                OverflowPolicy::Drop => match sub.tx.try_send(record.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(topic = %self.name, "subscriber channel full, dropping");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        subs.swap_remove(i);
                        continue;
                    }
                },
                OverflowPolicy::BackPressure => {
                    let tx = sub.tx.clone();
                    let rec = record.clone();
                    let name = self.name.clone();
                    tokio::spawn(async move {
                        if tx.send(rec).await.is_err() {
                            tracing::warn!(topic = %name, "subscriber closed during backpressure send");
                        }
                    });
                }
            }
            i += 1;
        }

        Ok(())
    }

    /// Запросить данные из storage.
    pub async fn query(&self, params: &TopicQuery) -> Result<Vec<TopicRecord>, TopicError> {
        self.storage.query(params).await.map_err(TopicError::Storage)
    }

    /// Flush storage.
    pub async fn flush(&self) -> Result<(), TopicError> {
        self.storage.flush().await.map_err(TopicError::Storage)
    }
}

// ═══════════════════════════════════════════════════════════════
//  TopicRegistry
// ═══════════════════════════════════════════════════════════════

/// Реестр всех topic'ов. Реализует ProcessContext для processor'ов и sink'ов.
pub struct TopicRegistry {
    topics: HashMap<String, Arc<Topic>>,
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    pub fn register(&mut self, topic: Topic) {
        let name = topic.name.clone();
        self.topics.insert(name, Arc::new(topic));
    }
}

impl ProcessContext for TopicRegistry {
    fn query(
        &self,
        topic: &str,
        query: &TopicQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        let topic_arc = self.topics.get(topic).cloned();
        let topic_name = topic.to_string();
        let query = query.clone();
        Box::pin(async move {
            match topic_arc {
                Some(t) => t.query(&query).await.map_err(TopicError::into_plugin_error),
                None => Err(TopicError::NotFound(topic_name).into_plugin_error()),
            }
        })
    }

    fn publish(
        &self,
        topic: &str,
        record: TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let topic_arc = self.topics.get(topic).cloned();
        let topic_name = topic.to_string();
        Box::pin(async move {
            match topic_arc {
                Some(t) => t.publish(record).await.map_err(TopicError::into_plugin_error),
                None => Err(TopicError::NotFound(topic_name).into_plugin_error()),
            }
        })
    }

    fn subscribe(
        &self,
        topic: &str,
        buffer: usize,
        overflow: OverflowPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn TopicSubscription>, PluginError>> + Send + '_>> {
        let topic_arc = self.topics.get(topic).cloned();
        let topic_name = topic.to_string();
        Box::pin(async move {
            let t = topic_arc
                .ok_or_else(|| TopicError::NotFound(topic_name).into_plugin_error())?;
            let sub = t.subscribe(buffer, overflow).await;
            Ok(Box::new(sub) as Box<dyn TopicSubscription>)
        })
    }

    fn flush_topic(
        &self,
        topic: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let topic_arc = self.topics.get(topic).cloned();
        let topic_name = topic.to_string();
        Box::pin(async move {
            match topic_arc {
                Some(t) => t.flush().await.map_err(TopicError::into_plugin_error),
                None => Err(TopicError::NotFound(topic_name).into_plugin_error()),
            }
        })
    }

    fn topics(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }

    fn deserialize_data(&self, topic: &str, data: &RecordData)
        -> Result<serde_json::Value, PluginError>
    {
        let t = self.topics.get(topic)
            .ok_or_else(|| TopicError::NotFound(topic.to_string()).into_plugin_error())?;
        t.format.deserialize(data.as_bytes())
    }

    fn serialize_data(&self, topic: &str, value: &serde_json::Value)
        -> Result<RecordData, PluginError>
    {
        let t = self.topics.get(topic)
            .ok_or_else(|| TopicError::NotFound(topic.to_string()).into_plugin_error())?;
        let bytes = t.format.serialize(value)?;
        Ok(RecordData::new(bytes, t.format.format()))
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
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
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

            if let Some(limit) = query.limit {
                if result.len() > limit {
                    result = result.split_off(result.len() - limit);
                }
            }

            Ok(result)
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}
