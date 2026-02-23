use std::future::Future;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::pin::Pin;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TopicRecord, TopicQuery, TopicStorage, PluginCreateResult, date_from_ms,
    PluginError,
};

/// Файловый TopicStorage с append-only семантикой.
///
/// Структура на диске:
/// ```text
/// {data_dir}/{YYYY-MM-DD}.jsonl
/// ```
/// Каждая строка — JSON TopicRecord. Один файл на день.
///
/// Подходит для append-only данных (e.g., сырые котировки):
/// записи только добавляются, не обновляются.
#[derive(Clone)]
pub struct RawFileStorage {
    data_dir: PathBuf,
}

impl RawFileStorage {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: PathBuf::from(data_dir),
        }
    }

    fn do_save(&self, records: &[TopicRecord]) -> Result<(), PluginError> {
        for record in records {
            let date = date_from_ms(record.ts_ms);
            let line = serde_json::to_string(record)
                .map_err(|e| PluginError::format_err(format!("json serialize: {e}")))?;
            self.append_line(&line, &date)?;
        }
        Ok(())
    }

    fn append_line(&self, line: &str, date: &str) -> Result<(), PluginError> {
        let path = self.data_dir.join(format!("{date}.jsonl"));
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| PluginError::io(format!("mkdir raw: {e}")))?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| PluginError::io(format!("open raw {}: {e}", path.display())))?;
        writeln!(f, "{line}").map_err(|e| PluginError::io(format!("write raw: {e}")))?;
        Ok(())
    }

    fn do_query(&self, query: &TopicQuery) -> Result<Vec<TopicRecord>, PluginError> {
        let files = self.date_files(query.from_ms, query.to_ms)?;
        let mut result = Vec::new();

        for path in files {
            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let reader = std::io::BufReader::new(file);

            for line in reader.lines() {
                let line = match line {
                    Ok(l) => l,
                    Err(_) => continue,
                };
                if line.is_empty() {
                    continue;
                }

                let record: TopicRecord = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // Фильтр по ключу
                if let Some(ref key) = query.key {
                    if record.key != *key {
                        continue;
                    }
                }

                // Фильтр по времени
                if let Some(from) = query.from_ms {
                    if record.ts_ms < from {
                        continue;
                    }
                }
                if let Some(to) = query.to_ms {
                    if record.ts_ms >= to {
                        continue;
                    }
                }

                result.push(record);

                if let Some(limit) = query.limit {
                    if result.len() >= limit {
                        return Ok(result);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Определить список файлов {date}.jsonl за нужный диапазон.
    fn date_files(&self, from_ms: Option<i64>, to_ms: Option<i64>) -> Result<Vec<PathBuf>, PluginError> {
        let mut entries: Vec<PathBuf> = Vec::new();

        let dir = match std::fs::read_dir(&self.data_dir) {
            Ok(d) => d,
            Err(_) => return Ok(entries), // директория не существует — пустой результат
        };

        let from_date = from_ms.map(date_from_ms);
        let to_date = to_ms.map(date_from_ms);

        for entry in dir {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            let name = name.to_string_lossy();

            if !name.ends_with(".jsonl") {
                continue;
            }

            let date = &name[..name.len() - 6]; // remove ".jsonl"

            // Фильтр по датам
            if let Some(ref from) = from_date {
                if date < from.as_str() {
                    continue;
                }
            }
            if let Some(ref to) = to_date {
                if date > to.as_str() {
                    continue;
                }
            }

            entries.push(entry.path());
        }

        entries.sort();
        Ok(entries)
    }
}

impl TopicStorage for RawFileStorage {
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let dir = self.data_dir.clone();
        Box::pin(async move {
            std::fs::create_dir_all(dir).map_err(|e| PluginError::io(format!("mkdir raw: {e}")))
        })
    }

    fn save(
        &self,
        records: &[TopicRecord],
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let this = self.clone();
        let records = records.to_vec();
        Box::pin(async move { this.do_save(&records) })
    }

    fn query(
        &self,
        query: &TopicQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        let this = self.clone();
        let query = query.clone();
        Box::pin(async move { this.do_query(&query) })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---- Plugin FFI entry points ----

#[derive(serde::Deserialize)]
struct RawFilePluginConfig {
    data_dir: String,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_topic_storage(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: RawFilePluginConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let storage = RawFileStorage::new(&cfg.data_dir);
    plugin_ok(Box::new(storage) as Box<dyn TopicStorage>)
}

server_api::qs_destroy_fn!(qs_destroy_topic_storage, TopicStorage);
server_api::qs_abi_version_fn!();
