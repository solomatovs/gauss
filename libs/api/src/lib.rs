use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use serde::{Deserialize, Serialize};

// ════════════════════════════════════════════════════════════════
//  Overflow Policy
// ════════════════════════════════════════════════════════════════

/// Стратегия поведения при переполнении bounded канала.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// try_send(): если канал полон — дропнуть сообщение, залогировать.
    Drop,
    /// .send().await: ждать пока появится место (back-pressure).
    #[serde(alias = "backpressure")]
    BackPressure,
}

// ════════════════════════════════════════════════════════════════
//  Topic System Types
// ════════════════════════════════════════════════════════════════

/// Формат бинарных данных в RecordData.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum DataFormat {
    #[default]
    Json,
    Csv,
    Protobuf,
    Avro,
    Raw,
}


impl std::fmt::Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::Json => write!(f, "json"),
            DataFormat::Csv => write!(f, "csv"),
            DataFormat::Protobuf => write!(f, "protobuf"),
            DataFormat::Avro => write!(f, "avro"),
            DataFormat::Raw => write!(f, "raw"),
        }
    }
}

/// Непрозрачный бинарный payload с метаданными формата.
///
/// Основной носитель данных в topic system. Байты интерпретируются
/// согласно объявленному формату. Topic-engine, pipeline и transport
/// слои работают с ним как с opaque — только процессоры и кодеки
/// десериализуют по необходимости.
#[derive(Clone, Debug)]
pub struct RecordData {
    bytes: Vec<u8>,
    format: DataFormat,
}

impl RecordData {
    /// Создать RecordData с явно указанным форматом.
    pub fn new(bytes: Vec<u8>, format: DataFormat) -> Self {
        Self { bytes, format }
    }

    /// Создать JSON-формат RecordData из serde_json::Value.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, PluginError> {
        let bytes = serde_json::to_vec(value)?;
        Ok(Self {
            bytes,
            format: DataFormat::Json,
        })
    }

    /// Получить сырые байты.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Потребить и вернуть внутренний Vec<u8>.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Получить объявленный формат.
    pub fn format(&self) -> DataFormat {
        self.format
    }

    /// Распарсить как JSON. Ошибка если формат не JSON или парсинг неуспешен.
    pub fn as_json(&self) -> Result<serde_json::Value, PluginError> {
        if self.format != DataFormat::Json {
            return Err(PluginError::new(format!(
                "RecordData format is {:?}, not JSON",
                self.format
            )));
        }
        Ok(serde_json::from_slice(&self.bytes)?)
    }

    /// Попытка распарсить как JSON независимо от объявленного формата.
    pub fn try_as_json(&self) -> Result<serde_json::Value, PluginError> {
        Ok(serde_json::from_slice(&self.bytes)?)
    }

    /// Длина payload в байтах.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Serialize for RecordData {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("RecordData", 2)?;
        s.serialize_field("format", &self.format)?;
        match self.format {
            DataFormat::Json => {
                // Inline JSON для читаемости
                let value: serde_json::Value = serde_json::from_slice(&self.bytes)
                    .unwrap_or(serde_json::Value::Null);
                s.serialize_field("data", &value)?;
            }
            _ => {
                // Base64 для бинарных форматов
                let encoded = base64::engine::general_purpose::STANDARD.encode(&self.bytes);
                s.serialize_field("data", &encoded)?;
            }
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for RecordData {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Поддержка трёх вариантов:
        // 1. {"format":"json","data":{...}} — inline JSON
        // 2. {"format":"protobuf","data":"base64..."} — base64 бинарный
        // 3. <любой JSON value> — legacy, трактуется как JSON
        let raw = serde_json::Value::deserialize(deserializer)?;

        if let serde_json::Value::Object(ref map) = raw {
            if let (Some(fmt_val), Some(data_val)) = (map.get("format"), map.get("data")) {
                if let Some(fmt_str) = fmt_val.as_str() {
                    let format: DataFormat = serde_json::from_value(
                        serde_json::Value::String(fmt_str.to_string())
                    ).unwrap_or(DataFormat::Json);

                    return match format {
                        DataFormat::Json => {
                            let bytes = serde_json::to_vec(data_val)
                                .map_err(serde::de::Error::custom)?;
                            Ok(RecordData::new(bytes, DataFormat::Json))
                        }
                        _ => {
                            // base64 decode
                            let b64 = data_val.as_str().ok_or_else(|| {
                                serde::de::Error::custom("expected base64 string for binary format")
                            })?;
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(b64)
                                .map_err(serde::de::Error::custom)?;
                            Ok(RecordData::new(bytes, format))
                        }
                    };
                }
            }
        }

        // Legacy fallback: любой JSON value → JSON format
        let bytes = serde_json::to_vec(&raw)
            .map_err(serde::de::Error::custom)?;
        Ok(RecordData::new(bytes, DataFormat::Json))
    }
}

/// Универсальная запись в topic. Payload — RecordData (бинарные данные
/// с метаданными формата) для поддержки произвольных форматов.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicRecord {
    /// Timestamp в миллисекундах (Unix epoch).
    pub ts_ms: i64,
    /// Partition key (например, symbol).
    pub key: String,
    /// Данные записи (бинарные, с указанием формата).
    pub data: RecordData,
}

/// Параметры запроса к topic storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicQuery {
    /// Фильтр по ключу (например, symbol).
    pub key: Option<String>,
    /// Начало диапазона (inclusive, Unix ms).
    pub from_ms: Option<i64>,
    /// Конец диапазона (exclusive, Unix ms).
    pub to_ms: Option<i64>,
    /// Максимальное количество записей. При наличии — возвращает последние N.
    pub limit: Option<usize>,
}

// ════════════════════════════════════════════════════════════════
//  Topic Plugin Traits
// ════════════════════════════════════════════════════════════════

/// Storage backend для topic. Каждый topic имеет свой экземпляр.
/// Поддерживает save (persist) и query (read historical data).
///
/// Плагины: file storage, memory storage, clickhouse, etc.
pub trait TopicStorage: Send + Sync {
    /// Инициализация (создание директорий, таблиц и т.д.)
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

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

/// Контекст для processor'ов и sink'ов — полный доступ к topic system.
///
/// Позволяет запрашивать данные, публиковать записи, подписываться
/// на real-time поток и управлять topic'ами.
///
/// Server-side TopicRegistry реализует этот трейт.
pub trait ProcessContext: Send + Sync {
    /// Запросить записи из storage topic'а.
    fn query(&self, topic: &str, query: &TopicQuery)
        -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>>;

    /// Опубликовать запись в topic (save → notify subscribers).
    fn publish(&self, topic: &str, record: TopicRecord)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

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

    /// Flush storage конкретного topic'а.
    fn flush_topic(&self, topic: &str)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Список всех доступных topic'ов.
    fn topics(&self) -> Vec<String>;

    /// Десериализовать данные используя формат topic'а.
    /// Ошибка если topic не найден или формат не сконфигурирован.
    fn deserialize_data(&self, topic: &str, data: &RecordData)
        -> Result<serde_json::Value, PluginError>;

    /// Сериализовать Value в RecordData используя формат topic'а.
    /// Ошибка если topic не найден или формат не сконфигурирован.
    fn serialize_data(&self, topic: &str, value: &serde_json::Value)
        -> Result<RecordData, PluginError>;
}

/// Processor — обработчик, вызываемый триггером при сохранении в topic.
///
/// Получает ProcessContext для чтения/записи в любые topics.
/// Processor может: запрашивать текущие данные, трансформировать их,
/// публиковать результат в другие topics (что вызывает дальнейшие triggers).
pub trait TopicProcessor: Send + Sync {
    /// Обработать запись, поступившую из source_topic.
    fn process<'a>(
        &'a self,
        ctx: &'a dyn ProcessContext,
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
/// **Pull path (on-demand)**: sink использует ctx (ProcessContext)
/// для запроса исторических данных из topic'ов (по запросу клиента).
pub trait TopicSink: Send + Sync {
    /// Инициализация с доступом к topic system.
    fn init(&self, ctx: Arc<dyn ProcessContext>)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Получить запись из подписанного topic'а (real-time push).
    fn send(&self, topic: &str, record: &TopicRecord)
        -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Flush буферов (при graceful shutdown).
    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

/// Source — монолитный плагин, самостоятельно получающий данные
/// (подключение к внешнему сервису, чтение файлов, etc.)
/// и публикующий их в target topic через ProcessContext.
///
/// Зеркало TopicSink для inbound направления.
/// Pipeline source (transport + framing + codec) — альтернатива для
/// стандартных протоколов; TopicSource — для кастомных.
pub trait TopicSource: Send + Sync {
    /// Запустить source. Должен блокировать (async) пока source активен.
    fn start(
        &self,
        ctx: Arc<dyn ProcessContext>,
        target_topic: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;

    /// Остановить source (graceful shutdown).
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>>;
}

// ════════════════════════════════════════════════════════════════
//  Source Plugin Traits
// ════════════════════════════════════════════════════════════════

/// Абстрактный двунаправленный байтовый поток соединения.
/// Реализации должны поддерживать блокирующие Read и Write.
pub trait TransportStream: std::io::Read + std::io::Write + Send {
    /// Описание удалённой стороны (для логирования).
    fn peer_info(&self) -> String {
        "unknown".into()
    }
}

impl TransportStream for std::net::TcpStream {
    fn peer_info(&self) -> String {
        self.peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "?".into())
    }
}

/// Transport — унифицированный трейт для входящих и исходящих соединений.
///
/// Все методы **блокирующие** — хост вызывает их из отдельного потока.
pub trait Transport: Send {
    /// Инициализировать транспорт (bind, listen, validate config и т.д.).
    fn start(&mut self) -> Result<(), PluginError>;

    /// Получить следующее соединение. Блокирует до готовности.
    /// None = graceful shutdown.
    fn next_connection(&mut self) -> Result<Option<Box<dyn TransportStream>>, PluginError>;

    /// Остановить транспорт.
    fn stop(&mut self) -> Result<(), PluginError>;
}

/// Framing — определение границ сообщений в потоке байтов.
///
/// Реализации должны быть stateless — всё состояние буферизации хранится
/// у вызывающего. Это позволяет безопасно использовать один экземпляр
/// framing для множества соединений.
pub trait Framing: Send + Sync {
    /// Извлечь один фрейм из буфера.
    /// Возвращает (frame_data, bytes_consumed) или None если фрейм неполный.
    fn decode(&self, buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>, PluginError>;

    /// Добавить framed данные в выходной буфер.
    fn encode(&self, data: &[u8], buf: &mut Vec<u8>) -> Result<(), PluginError>;
}

/// Codec — парсер формата: bytes ↔ (Value + RecordData).
///
/// Реализации: JSON codec, CSV codec, Protobuf codec, etc.
/// Codec отвечает ТОЛЬКО за парсинг/сериализацию формата.
/// Извлечение key/ts_ms — ответственность pipeline (Endpoint).
pub trait Codec: Send + Sync {
    /// Парсинг: сырые байты → (структурированное значение, RecordData).
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError>;

    /// Сериализация: RecordData → сырые байты.
    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError>;
}

/// Конвертация bytes ↔ serde_json::Value для конкретного формата + schema.
///
/// Каждый экземпляр сконфигурирован с нужной schema (descriptor, avsc, columns).
/// Используется ProcessContext'ом для format-agnostic доступа к данным topic'ов.
///
/// Отличие от Codec:
/// - Codec: wire format (bytes ↔ TopicRecord, extraction key/ts)
/// - FormatSerializer: data format (bytes ↔ serde_json::Value, schema-aware)
pub trait FormatSerializer: Send + Sync {
    /// Десериализовать сырые байты в структурированное значение.
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError>;

    /// Сериализовать структурированное значение в сырые байты.
    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError>;

    /// Формат данных, производимых этим сериализатором.
    fn format(&self) -> DataFormat;
}

/// Middleware — преобразование байтов между framing и codec.
///
/// На decode (inbound): вызывается после framing.decode(), до codec.decode().
/// На encode (outbound): вызывается после codec.encode(), до framing.encode().
///
/// Middleware в цепочке применяются в порядке массива при decode,
/// и в обратном порядке при encode (onion model).
///
/// Реализации должны быть stateless.
pub trait Middleware: Send + Sync {
    /// Преобразование декодированного фрейма (inbound: framing → codec).
    fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError>;

    /// Преобразование сериализованных данных (outbound: codec → framing).
    fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError>;
}

// ════════════════════════════════════════════════════════════════
//  Unified Plugin FFI
// ════════════════════════════════════════════════════════════════

/// Результат создания плагина (unified для всех типов).
/// Host и plugin компилируются одним компилятором в одном workspace,
/// поэтому совместимость Rust ABI гарантирована.
#[repr(C)]
pub struct PluginCreateResult {
    /// При успехе: указатель на `Box<Box<dyn Trait>>` (double-boxed для thin ptr).
    /// При ошибке: null.
    pub plugin_ptr: *mut (),
    /// При ошибке: указатель на `Box<String>` с сообщением об ошибке.
    /// При успехе: null.
    pub error_ptr: *mut (),
}

// Safety: PluginCreateResult передаётся только между host и plugin при создании.
unsafe impl Send for PluginCreateResult {}

/// Сигнатура символа `qs_create_*`, экспортируемого плагинами.
pub type CreatePluginFn =
    unsafe extern "C" fn(config_json_ptr: *const u8, config_json_len: usize) -> PluginCreateResult;

/// Сигнатура символа `qs_destroy_*`, экспортируемого плагинами.
pub type DestroyPluginFn = unsafe extern "C" fn(plugin_ptr: *mut ());

/// Helper для плагинов: вернуть успешный результат.
/// T — трейт-объект плагина (dyn TopicStorage, dyn Transport, ...).
pub fn plugin_ok<T: ?Sized>(val: Box<T>) -> PluginCreateResult {
    // Double-box: Box<dyn Trait> — fat pointer (2 words).
    // Box<Box<dyn Trait>> — thin pointer (1 word), safe для *mut ().
    let boxed: Box<Box<T>> = Box::new(val);
    PluginCreateResult {
        plugin_ptr: Box::into_raw(boxed) as *mut (),
        error_ptr: std::ptr::null_mut(),
    }
}

/// Helper для плагинов: вернуть ошибку.
pub fn plugin_err(error: String) -> PluginCreateResult {
    let boxed: Box<String> = Box::new(error);
    PluginCreateResult {
        plugin_ptr: std::ptr::null_mut(),
        error_ptr: Box::into_raw(boxed) as *mut (),
    }
}

/// Макрос для генерации `qs_destroy_*` функций в плагинах.
///
/// Пример:
/// ```ignore
/// qs_destroy_fn!(qs_destroy_codec, Codec);
/// ```
#[macro_export]
macro_rules! qs_destroy_fn {
    ($fn_name:ident, $trait_ty:path) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $fn_name(ptr: *mut ()) {
            if !ptr.is_null() {
                let _ = unsafe { Box::from_raw(ptr as *mut Box<dyn $trait_ty>) };
            }
        }
    };
}

// ════════════════════════════════════════════════════════════════
//  ABI Version
// ════════════════════════════════════════════════════════════════

/// ABI version of the plugin interface.
///
/// Bump this whenever any plugin trait changes in a binary-incompatible way:
/// - Adding/removing/reordering trait methods
/// - Changing method signatures (parameter types, return types)
/// - Changing `#[repr(C)]` struct layouts (`PluginCreateResult`)
/// - Changing FFI function signatures (`CreatePluginFn`, `DestroyPluginFn`)
pub const QS_ABI_VERSION: u32 = 3;

/// Signature of the `qs_abi_version` symbol exported by plugins.
pub type AbiVersionFn = unsafe extern "C" fn() -> u32;

/// Macro to export the `qs_abi_version` symbol from a plugin.
///
/// Every plugin crate must call this exactly once at crate root:
/// ```ignore
/// server_api::qs_abi_version_fn!();
/// ```
#[macro_export]
macro_rules! qs_abi_version_fn {
    () => {
        #[unsafe(no_mangle)]
        pub extern "C" fn qs_abi_version() -> u32 {
            $crate::QS_ABI_VERSION
        }
    };
}

// ════════════════════════════════════════════════════════════════
//  Plugin Error
// ════════════════════════════════════════════════════════════════

/// Category of a plugin error. Allows the host to make intelligent
/// decisions about error handling (skip, retry, fail fast).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Invalid configuration — permanent, fail at startup.
    Config,
    /// I/O or network error — transient, may retry/reconnect.
    Io,
    /// Data format/parse error — bad input, skip record.
    Format,
    /// Logical error (not found, invalid state, generic).
    Logic,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Config => f.write_str("config"),
            ErrorKind::Io => f.write_str("io"),
            ErrorKind::Format => f.write_str("format"),
            ErrorKind::Logic => f.write_str("logic"),
        }
    }
}

/// Unified error type for all plugin trait methods.
///
/// Carries an `ErrorKind` for categorization and a human-readable message.
/// `From` impls assign the appropriate kind automatically and allow
/// ergonomic `?` in plugin implementations.
#[derive(Clone)]
pub struct PluginError {
    kind: ErrorKind,
    message: String,
}

impl PluginError {
    /// Generic logic error (default kind).
    pub fn new(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Logic, message: msg.into() }
    }

    /// Configuration error — permanent, fail at startup.
    pub fn config(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Config, message: msg.into() }
    }

    /// I/O error — transient, may retry/reconnect.
    pub fn io(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Io, message: msg.into() }
    }

    /// Format/parse error — bad input, skip record.
    pub fn format_err(msg: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Format, message: msg.into() }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Debug for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.kind, self.message)
    }
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PluginError {}

impl From<String> for PluginError {
    fn from(s: String) -> Self { Self { kind: ErrorKind::Logic, message: s } }
}

impl From<&str> for PluginError {
    fn from(s: &str) -> Self { Self { kind: ErrorKind::Logic, message: s.to_string() } }
}

impl From<std::io::Error> for PluginError {
    fn from(e: std::io::Error) -> Self { Self { kind: ErrorKind::Io, message: e.to_string() } }
}

impl From<serde_json::Error> for PluginError {
    fn from(e: serde_json::Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}

impl From<std::str::Utf8Error> for PluginError {
    fn from(e: std::str::Utf8Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}

impl From<std::string::FromUtf8Error> for PluginError {
    fn from(e: std::string::FromUtf8Error) -> Self { Self { kind: ErrorKind::Format, message: e.to_string() } }
}

// ════════════════════════════════════════════════════════════════
//  Utilities
// ════════════════════════════════════════════════════════════════

/// Resolve a dot-notation path in a `serde_json::Value`.
///
/// Supports nested field access via dot separation:
/// - `"symbol"` → `value["symbol"]`
/// - `"quote.symbol"` → `value["quote"]["symbol"]`
/// - `"data.prices.bid"` → `value["data"]["prices"]["bid"]`
///
/// Returns `None` if any segment is missing.
pub fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Текущее Unix-время в миллисекундах.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Конвертировать unix ms в строку даты `YYYY-MM-DD`.
/// Использует алгоритм Howard Hinnant (civil_from_days).
pub fn date_from_ms(ms: i64) -> String {
    let secs = ms / 1000;
    let days = secs.div_euclid(86400) + 719468;
    let era = days.div_euclid(146097);
    let doe = days.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

// ════════════════════════════════════════════════════════════════
//  FFI Config Parsing
// ════════════════════════════════════════════════════════════════

/// Десериализовать конфиг плагина из FFI raw pointer + length.
///
/// # Safety
/// `config_json_ptr` должен указывать на `config_json_len` валидных байт.
pub unsafe fn parse_plugin_config<T: serde::de::DeserializeOwned>(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> Result<T, PluginError> {
    let json_bytes = unsafe { std::slice::from_raw_parts(config_json_ptr, config_json_len) };
    let json_str = std::str::from_utf8(json_bytes)
        .map_err(|e| PluginError::config(format!("invalid UTF-8 config: {e}")))?;
    serde_json::from_str(json_str)
        .map_err(|e| PluginError::config(format!("invalid config JSON: {e}")))
}

/// Десериализовать опциональный конфиг плагина.
/// Возвращает `T::default()` если указатель null или длина 0.
///
/// # Safety
/// Если `config_json_ptr` не null, он должен указывать на `config_json_len` валидных байт.
pub unsafe fn parse_plugin_config_opt<T: Default + serde::de::DeserializeOwned>(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> Result<T, PluginError> {
    if config_json_ptr.is_null() || config_json_len == 0 {
        return Ok(T::default());
    }
    unsafe { parse_plugin_config(config_json_ptr, config_json_len) }
}
