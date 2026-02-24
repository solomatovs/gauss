pub mod error;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use server_api::{
    OverflowPolicy, PluginError, TopicQuery,
    TopicRecord, TopicStorage, TopicSubscription,
    TopicPublisher, TopicSubscriber, TopicCodec, TopicInspector,
    now_ms,
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
}

impl Topic {
    pub fn new(
        name: String,
        storage: Arc<dyn TopicStorage>,
    ) -> Self {
        Self {
            name,
            storage,
            subscribers: RwLock::new(Vec::new()),
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

/// Реестр всех topic'ов. Реализует TopicPublisher, TopicSubscriber,
/// TopicCodec и TopicInspector.
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

impl TopicPublisher for TopicRegistry {
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
}

impl TopicSubscriber for TopicRegistry {
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
}

impl TopicCodec for TopicRegistry {
    fn deserialize_data(&self, _topic: &str, record: &TopicRecord)
        -> Result<serde_json::Value, PluginError>
    {
        // Value уже в record — тривиально
        Ok(record.value.clone())
    }

    fn serialize_data(&self, _topic: &str, value: &serde_json::Value)
        -> Result<TopicRecord, PluginError>
    {
        // Создаём record с value, без raw (не было ingestion)
        Ok(TopicRecord {
            ts_ms: now_ms(),
            key: String::new(),
            value: value.clone(),
            raw: None,
        })
    }
}

impl TopicInspector for TopicRegistry {
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

    fn topics(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
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
}

