use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::{
    OverflowPolicy, PluginError, RecordSchema, TopicQuery, TopicRecord,
};

// ════════════════════════════════════════════════════════════════
//  Topic Plugin Traits
// ════════════════════════════════════════════════════════════════

/// Фабрика для создания TopicStorage по JSON-конфигу.
///
/// Реализации: MemoryStorageFactory (built-in), FFI-плагины (fallback).
/// Используется StorageRegistry для единообразного резолвинга storage по имени.
pub trait StorageFactory: Send + Sync {
    fn create(&self, config_json: &str) -> Result<Arc<dyn TopicStorage>, PluginError>;
}

/// Storage backend для topic. Каждый topic имеет свой экземпляр.
/// Поддерживает save (persist) и query (read historical data).
///
/// Плагины: file storage, memory storage, clickhouse, etc.
pub trait TopicStorage: Send + Sync {
    /// Инициализация (создание директорий, таблиц и т.д.)
    /// Получает схему данных topic'а (если формат предоставляет).
    /// Storage использует схему для создания типизированных таблиц/структур.
    /// None = бессхемный формат (JSON), storage хранит как есть.
    fn init(&self, schema: Option<RecordSchema>) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Сохранить записи. Семантика зависит от реализации:
    /// файловый storage может делать append или upsert по key+ts.
    fn save(&self, records: &[TopicRecord]) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Запросить записи по параметрам.
    fn query(&self, query: &TopicQuery) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>>;

    /// Flush буферов на диск.
    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Подписка на topic — асинхронный поток записей.
///
/// Реализуется server-side (MpscSubscription). API crate определяет
/// только трейт, без зависимости от tokio.
pub trait TopicSubscription: Send {
    /// Получить следующую запись. None = topic закрыт / подписка отменена.
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = Option<TopicRecord>> + Send + '_>>;
}

/// Публикация записей в topic (save → notify subscribers).
pub trait TopicPublisher: Send + Sync {
    /// Опубликовать запись в topic (save → notify subscribers).
    fn publish(&self, topic: &str, record: TopicRecord)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Подписка на real-time поток записей из topic'а.
pub trait TopicSubscriber: Send + Sync {
    /// Подписаться на topic. Возвращает поток записей (mpsc receiver).
    ///
    /// - `buffer`: размер буфера канала
    /// - `overflow`: стратегия при переполнении (Drop или BackPressure)
    #[allow(clippy::type_complexity)]
    fn subscribe(
        &self,
        topic: &str,
        buffer: usize,
        overflow: OverflowPolicy,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn TopicSubscription>, PluginError>> + Send + '_>>;
}

/// Формат-зависимая сериализация/десериализация данных topic'а.
pub trait TopicCodec: Send + Sync {
    /// Десериализовать данные используя формат topic'а.
    /// С новой архитектурой value уже в record — метод может быть тривиальным.
    fn deserialize_data(&self, topic: &str, record: &TopicRecord)
        -> Result<serde_json::Value, PluginError>;

    /// Сериализовать Value в TopicRecord используя формат topic'а.
    fn serialize_data(&self, topic: &str, value: &serde_json::Value)
        -> Result<TopicRecord, PluginError>;
}

/// Чтение, метаданные и управление topic'ами.
pub trait TopicInspector: Send + Sync {
    /// Запросить записи из storage topic'а.
    fn query(&self, topic: &str, query: &TopicQuery)
        -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>>;

    /// Список всех доступных topic'ов.
    fn topics(&self) -> Vec<String>;

    /// Flush storage конкретного topic'а.
    fn flush_topic(&self, topic: &str)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Контекст, доступный процессору при обработке записи.
///
/// Явно предоставляет доступ к нужным возможностям topic system
/// через отдельные суб-трейты.
pub struct TopicContext<'a> {
    /// Публикация записей в topics.
    pub publisher: &'a dyn TopicPublisher,
    /// Формат-зависимая сериализация/десериализация.
    pub codec: &'a dyn TopicCodec,
    /// Чтение, метаданные и управление topics.
    pub inspector: &'a dyn TopicInspector,
}

/// Processor — обработчик, вызываемый триггером при сохранении в topic.
///
/// Получает TopicContext для чтения/записи в любые topics.
/// Processor может: запрашивать текущие данные, трансформировать их,
/// публиковать результат в другие topics (что вызывает дальнейшие triggers).
pub trait TopicProcessor: Send + Sync {
    /// Обработать запись, поступившую из source_topic.
    fn process<'a>(
        &'a self,
        ctx: TopicContext<'a>,
        source_topic: &'a str,
        record: &'a TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + 'a>>;
}

/// Sink — компонент, общающийся с клиентом по специфичному протоколу
/// (HTTP, WS, TCP, Kafka, ...) и имеющий доступ к topics для запроса данных.
///
/// **Push path (real-time)**: server подписывает sink на topic broadcasts,
/// вызывает send() для каждой новой записи.
///
/// **Pull path (on-demand)**: sink использует TopicInspector
/// для запроса исторических данных из topic'ов (по запросу клиента).
pub trait TopicSink: Send + Sync {
    /// Инициализация с доступом к topic inspector (query, topics, flush).
    fn init(&self, ctx: Arc<dyn TopicInspector>)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Получить запись из подписанного topic'а (real-time push).
    fn send(&self, topic: &str, record: &TopicRecord)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Flush буферов (при graceful shutdown).
    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Source — монолитный плагин, самостоятельно получающий данные
/// (подключение к внешнему сервису, чтение файлов, etc.)
/// и публикующий их в target topic через TopicPublisher.
///
/// Зеркало TopicSink для inbound направления.
/// Pipeline source (transport + framing + codec) — альтернатива для
/// стандартных протоколов; TopicSource — для кастомных.
pub trait TopicSource: Send + Sync {
    /// Запустить source. Должен блокировать (async) пока source активен.
    fn start(
        &self,
        ctx: Arc<dyn TopicPublisher>,
        target_topic: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Остановить source (graceful shutdown).
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}
