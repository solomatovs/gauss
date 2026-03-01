/// Universal data record. The engine only knows `ts_ms`.
/// `data` is opaque bytes — the engine never interprets them.
pub struct TopicRecord {
    /// Timestamp in milliseconds — index for temporal queries, sorting, retention.
    pub ts_ms: i64,
    /// Opaque bytes — neither the engine nor the topic interpret their contents.
    pub data: Vec<u8>,
}
