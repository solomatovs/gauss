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
//  Data Format
// ════════════════════════════════════════════════════════════════

/// Формат бинарных данных в RawPayload.
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

// ════════════════════════════════════════════════════════════════
//  RawPayload
// ════════════════════════════════════════════════════════════════

/// Оригинальные байты записи в wire-формате.
///
/// Заполняется при ingestion (Codec.decode), используется для
/// zero-copy transport в sink'ах когда формат совпадает.
#[derive(Clone, Debug)]
pub struct RawPayload {
    pub bytes: Vec<u8>,
    pub format: DataFormat,
}

impl RawPayload {
    pub fn new(bytes: Vec<u8>, format: DataFormat) -> Self {
        Self { bytes, format }
    }
}

// ════════════════════════════════════════════════════════════════
//  TopicRecord
// ════════════════════════════════════════════════════════════════

/// Универсальная запись в topic.
///
/// Несёт структурированные данные (`value`) для storage/processing
/// и опциональные оригинальные байты (`raw`) для zero-copy transport.
#[derive(Clone, Debug)]
pub struct TopicRecord {
    /// Timestamp в миллисекундах (Unix epoch).
    pub ts_ms: i64,
    /// Partition key (например, symbol).
    pub key: String,
    /// Структурированные данные записи.
    pub value: serde_json::Value,
    /// Опциональные оригинальные байты для zero-copy transport.
    /// Заполняется при ingestion (Codec.decode), отсутствует при
    /// создании из processing/query.
    pub raw: Option<RawPayload>,
}

impl Serialize for TopicRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("TopicRecord", 3)?;
        s.serialize_field("ts_ms", &self.ts_ms)?;
        s.serialize_field("key", &self.key)?;
        s.serialize_field("value", &self.value)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for TopicRecord {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Raw {
            ts_ms: i64,
            key: String,
            value: serde_json::Value,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(TopicRecord {
            ts_ms: raw.ts_ms,
            key: raw.key,
            value: raw.value,
            raw: None,
        })
    }
}

// ════════════════════════════════════════════════════════════════
//  TopicQuery
// ════════════════════════════════════════════════════════════════

/// Направление сортировки результатов.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
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
    /// Максимальное количество записей.
    pub limit: Option<usize>,
    /// Смещение (для пагинации).
    pub offset: Option<usize>,
    /// Направление сортировки по ts_ms.
    #[serde(default)]
    pub order: SortOrder,
}
