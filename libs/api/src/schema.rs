use serde::{Deserialize, Serialize};

// ════════════════════════════════════════════════════════════════
//  Scalar Type
// ════════════════════════════════════════════════════════════════

/// Скалярные типы — покрывают нативные типы всех storage backend'ов.
///
/// Каждый backend маппит в свой нативный тип:
/// - ClickHouse: `Int64`, `DateTime64(6)`, `Decimal(p,s)`, `UUID`, ...
/// - PostgreSQL: `BIGINT`, `TIMESTAMPTZ`, `NUMERIC(p,s)`, `UUID`, `JSONB`, ...
/// - Kafka: Avro/Protobuf logical types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarType {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    /// Фиксированная точность. PG: `NUMERIC(p,s)`, CH: `Decimal(p,s)`.
    #[serde(rename = "decimal")]
    Decimal { precision: u8, scale: u8 },
    String,
    Bytes,
    /// Микросекунды от epoch. PG: `TIMESTAMPTZ`, CH: `DateTime64(6)`.
    Timestamp,
    /// Дни от epoch. PG: `DATE`, CH: `Date`.
    Date,
    /// 128-bit UUID. PG: `UUID`, CH: `UUID`.
    Uuid,
    /// Полуструктурированные данные. PG: `JSONB`, CH: `String`/`JSON`.
    Json,
}

impl std::fmt::Display for ScalarType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScalarType::Bool => write!(f, "bool"),
            ScalarType::Int32 => write!(f, "int32"),
            ScalarType::Int64 => write!(f, "int64"),
            ScalarType::Float32 => write!(f, "float32"),
            ScalarType::Float64 => write!(f, "float64"),
            ScalarType::Decimal { precision, scale } => {
                write!(f, "decimal({precision},{scale})")
            }
            ScalarType::String => write!(f, "string"),
            ScalarType::Bytes => write!(f, "bytes"),
            ScalarType::Timestamp => write!(f, "timestamp"),
            ScalarType::Date => write!(f, "date"),
            ScalarType::Uuid => write!(f, "uuid"),
            ScalarType::Json => write!(f, "json"),
        }
    }
}

// ════════════════════════════════════════════════════════════════
//  Field Type
// ════════════════════════════════════════════════════════════════

/// Тип поля — скаляр или массив скаляров.
///
/// PG: `type[]`, CH: `Array(type)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Scalar(ScalarType),
    Array(ScalarType),
}

impl std::fmt::Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::Scalar(s) => write!(f, "{s}"),
            FieldType::Array(s) => write!(f, "array<{s}>"),
        }
    }
}

// ════════════════════════════════════════════════════════════════
//  Field & RecordSchema
// ════════════════════════════════════════════════════════════════

/// Одно поле в схеме записи.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: std::string::String,
    pub field_type: FieldType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<std::string::String>, field_type: FieldType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            nullable,
        }
    }

    /// Shortcut: non-nullable scalar field.
    pub fn scalar(name: impl Into<std::string::String>, scalar: ScalarType) -> Self {
        Self::new(name, FieldType::Scalar(scalar), false)
    }

    /// Shortcut: nullable scalar field.
    pub fn scalar_nullable(name: impl Into<std::string::String>, scalar: ScalarType) -> Self {
        Self::new(name, FieldType::Scalar(scalar), true)
    }
}

/// Полная схема записей topic'а.
///
/// Storage backend использует для создания таблиц/структур.
/// `key` и `ts_ms` — системные поля TopicRecord, не входят в RecordSchema.
/// Storage сам решает как их маппить (CH: `LowCardinality(String)` + `Int64`,
/// PG: `TEXT` + `BIGINT`, Kafka: message key + timestamp).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSchema {
    pub fields: Vec<Field>,
}

impl RecordSchema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }
}
