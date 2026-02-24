use crate::{DataFormat, PluginError, RecordSchema};

// ════════════════════════════════════════════════════════════════
//  Pipeline Plugin Traits
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

/// Codec — парсер формата: bytes ↔ serde_json::Value.
///
/// Реализации: JSON codec, CSV codec, Protobuf codec, etc.
/// Codec отвечает ТОЛЬКО за парсинг/сериализацию формата.
/// Извлечение key/ts_ms — ответственность pipeline (Endpoint).
pub trait Codec: Send + Sync {
    /// Парсинг: сырые байты → структурированное значение.
    fn decode(&self, data: &[u8]) -> Result<serde_json::Value, PluginError>;

    /// Сериализация: структурированное значение → сырые байты.
    fn encode(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError>;

    /// Формат данных этого кодека (для RawPayload matching).
    fn data_format(&self) -> DataFormat;
}

// ════════════════════════════════════════════════════════════════
//  FormatSerializer
// ════════════════════════════════════════════════════════════════

/// Конвертация bytes ↔ serde_json::Value для конкретного формата + schema.
///
/// Каждый экземпляр сконфигурирован с нужной schema (descriptor, avsc, columns).
///
/// Отличие от Codec:
/// - Codec: wire format (bytes ↔ Value, pipeline decode/encode)
/// - FormatSerializer: data format (bytes ↔ Value, schema-aware, storage init)
pub trait FormatSerializer: Send + Sync {
    /// Десериализовать сырые байты в структурированное значение.
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError>;

    /// Сериализовать структурированное значение в сырые байты.
    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError>;

    /// Формат данных, производимых этим сериализатором.
    fn format(&self) -> DataFormat;

    /// Схема данных этого формата.
    ///
    /// Форматы с сильной типизацией (Avro, Protobuf) возвращают полную схему
    /// с rich-типами (Decimal, Timestamp, UUID, Array и т.д.).
    /// Форматы без надёжной типовой информации (CSV, JSON) возвращают `None`.
    ///
    /// Storage backends используют схему для создания типизированных
    /// таблиц/структур вместо хранения данных как JSON-строки.
    fn schema(&self) -> Option<RecordSchema> {
        None
    }
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
