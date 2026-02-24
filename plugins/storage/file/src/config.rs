// ════════════════════════════════════════════════════════════════
//  Configuration
// ════════════════════════════════════════════════════════════════

/// Стратегия партиционирования файлов.
#[derive(Clone, Copy, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PartitionBy {
    /// Один файл на ключ: `{data_dir}/{KEY}.jsonl`
    #[default]
    Key,
    /// Один файл на день: `{data_dir}/{YYYY-MM-DD}.jsonl`
    Date,
}

/// Режим записи.
#[derive(Clone, Copy, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WriteMode {
    /// Если последняя строка имеет тот же ts_ms — перезаписать (upsert).
    #[default]
    Upsert,
    /// Всегда дописывать в конец файла.
    Append,
}

#[derive(serde::Deserialize)]
pub(crate) struct FilePluginConfig {
    pub data_dir: String,
    #[serde(default)]
    pub partition_by: PartitionBy,
    #[serde(default)]
    pub write_mode: WriteMode,
}

// ════════════════════════════════════════════════════════════════
//  On-disk record formats
// ════════════════════════════════════════════════════════════════

/// Запись на диске для partition_by=key (key хранится в имени файла).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct DiskRecord {
    pub ts_ms: i64,
    pub value: serde_json::Value,
}

/// Запись на диске для partition_by=date (key внутри строки).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct DiskRecordWithKey {
    pub ts_ms: i64,
    pub key: String,
    pub value: serde_json::Value,
}
