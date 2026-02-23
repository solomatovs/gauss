use std::future::Future;
use std::io::{BufRead, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    TopicRecord, TopicQuery, TopicStorage, PluginCreateResult,
    RecordData, DataFormat, PluginError,
};
use base64::Engine;

/// Файловый TopicStorage с upsert-семантикой.
///
/// Структура на диске:
/// ```text
/// {data_dir}/{KEY}.jsonl
/// ```
/// Каждая строка — JSON `FileRecord { ts_ms, data }`. Key хранится в имени файла.
/// Файл отсортирован по ts_ms. Upsert: если последняя строка имеет тот же ts_ms — перезаписать.
///
/// Подходит для данных, которые обновляются (e.g., OHLC свечи): один
/// файл на символ, последняя свеча перезаписывается при каждом тике.
#[derive(Clone)]
pub struct FileStorage {
    data_dir: PathBuf,
}

/// Компактная структура для записи в файл (без key — он в имени файла).
/// Поддерживает формат-aware хранение: JSON inline, бинарные — base64.
#[derive(serde::Serialize, serde::Deserialize)]
struct FileRecord {
    ts_ms: i64,
    #[serde(default)]
    format: Option<String>,
    data: serde_json::Value,
}

impl FileStorage {
    pub fn new(data_dir: &str) -> Self {
        Self {
            data_dir: PathBuf::from(data_dir),
        }
    }

    fn record_path(&self, key: &str) -> PathBuf {
        self.data_dir.join(format!("{key}.jsonl"))
    }

    fn do_save(&self, records: &[TopicRecord]) -> Result<(), PluginError> {
        for record in records {
            self.do_save_record(record)?;
        }
        Ok(())
    }

    fn do_save_record(&self, record: &TopicRecord) -> Result<(), PluginError> {
        let path = self.record_path(&record.key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| PluginError::io(format!("mkdir: {e}")))?;
        }

        let fr = match record.data.format() {
            DataFormat::Json => FileRecord {
                ts_ms: record.ts_ms,
                format: Some("json".to_string()),
                data: record.data.as_json().unwrap_or(serde_json::Value::Null),
            },
            _ => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(record.data.as_bytes());
                FileRecord {
                    ts_ms: record.ts_ms,
                    format: Some(record.data.format().to_string()),
                    data: serde_json::Value::String(encoded),
                }
            }
        };
        let new_line = serde_json::to_string(&fr).map_err(|e| PluginError::format_err(format!("json: {e}")))?;

        if path.exists() {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| PluginError::io(format!("open {}: {e}", path.display())))?;

            if let Some((last_line, last_pos)) = read_last_line(&mut f)? {
                if let Ok(last_fr) = serde_json::from_str::<FileRecord>(&last_line) {
                    if last_fr.ts_ms == record.ts_ms {
                        // Тот же ts — перезаписать последнюю строку (upsert)
                        f.seek(SeekFrom::Start(last_pos))
                            .map_err(|e| PluginError::io(format!("seek: {e}")))?;
                        f.set_len(last_pos)
                            .map_err(|e| PluginError::io(format!("truncate: {e}")))?;
                        writeln!(f, "{new_line}").map_err(|e| PluginError::io(format!("write: {e}")))?;
                        return Ok(());
                    }
                }
            }

            // Другой ts — append
            f.seek(SeekFrom::End(0))
                .map_err(|e| PluginError::io(format!("seek end: {e}")))?;
            writeln!(f, "{new_line}").map_err(|e| PluginError::io(format!("write: {e}")))?;
        } else {
            let mut f = std::fs::File::create(&path)
                .map_err(|e| PluginError::io(format!("create {}: {e}", path.display())))?;
            writeln!(f, "{new_line}").map_err(|e| PluginError::io(format!("write: {e}")))?;
        }

        Ok(())
    }

    fn do_query(&self, query: &TopicQuery) -> Result<Vec<TopicRecord>, PluginError> {
        // Если указан ключ — читаем один файл, иначе все файлы
        let paths: Vec<(String, PathBuf)> = if let Some(ref key) = query.key {
            let p = self.record_path(key);
            if p.exists() {
                vec![(key.clone(), p)]
            } else {
                return Ok(Vec::new());
            }
        } else {
            self.all_key_files()?
        };

        let mut result = Vec::new();
        for (key, path) in paths {
            let f = std::fs::File::open(&path)
                .map_err(|e| PluginError::io(format!("open {}: {e}", path.display())))?;
            let reader = std::io::BufReader::new(f);

            for line in reader.lines() {
                let line = line.map_err(|e| PluginError::io(format!("read line: {e}")))?;
                if line.is_empty() {
                    continue;
                }
                let fr: FileRecord =
                    serde_json::from_str(&line).map_err(|e| PluginError::format_err(format!("parse json: {e}")))?;

                if let Some(from) = query.from_ms {
                    if fr.ts_ms < from {
                        continue;
                    }
                }
                if let Some(to) = query.to_ms {
                    if fr.ts_ms >= to {
                        continue;
                    }
                }

                let record_data = file_record_to_record_data(&fr)?;
                result.push(TopicRecord {
                    ts_ms: fr.ts_ms,
                    key: key.clone(),
                    data: record_data,
                });
            }
        }

        // Сортировать по ts_ms (при чтении из нескольких файлов)
        if query.key.is_none() {
            result.sort_by_key(|r| r.ts_ms);
        }

        if let Some(limit) = query.limit {
            if result.len() > limit {
                result = result.split_off(result.len() - limit);
            }
        }

        Ok(result)
    }

    /// Список всех (key, path) файлов в data_dir.
    fn all_key_files(&self) -> Result<Vec<(String, PathBuf)>, PluginError> {
        let dir = match std::fs::read_dir(&self.data_dir) {
            Ok(d) => d,
            Err(_) => return Ok(Vec::new()),
        };

        let mut files = Vec::new();
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
            let key = name[..name.len() - 6].to_string();
            files.push((key, entry.path()));
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }
}

impl TopicStorage for FileStorage {
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        let dir = self.data_dir.clone();
        Box::pin(async move {
            std::fs::create_dir_all(dir).map_err(|e| PluginError::io(format!("mkdir: {e}")))
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

/// Прочитать последнюю строку файла и позицию её начала.
fn read_last_line(f: &mut std::fs::File) -> Result<Option<(String, u64)>, PluginError> {
    let len = f.seek(SeekFrom::End(0)).map_err(|e| PluginError::io(format!("seek: {e}")))?;
    if len == 0 {
        return Ok(None);
    }

    let mut pos = len;
    let mut found_content = false;

    loop {
        if pos == 0 {
            break;
        }
        pos -= 1;
        f.seek(SeekFrom::Start(pos))
            .map_err(|e| PluginError::io(format!("seek: {e}")))?;
        let mut buf = [0u8; 1];
        std::io::Read::read_exact(f, &mut buf).map_err(|e| PluginError::io(format!("read: {e}")))?;

        if buf[0] == b'\n' {
            if found_content {
                pos += 1;
                break;
            }
        } else {
            found_content = true;
        }
    }

    f.seek(SeekFrom::Start(pos))
        .map_err(|e| PluginError::io(format!("seek: {e}")))?;
    let mut line = String::new();
    std::io::BufReader::new(&*f)
        .read_line(&mut line)
        .map_err(|e| PluginError::io(format!("read_line: {e}")))?;

    let line = line.trim_end().to_string();
    if line.is_empty() {
        return Ok(None);
    }

    Ok(Some((line, pos)))
}

/// Восстановить RecordData из FileRecord.
/// Поддерживает legacy (без format) и новый формат.
fn file_record_to_record_data(fr: &FileRecord) -> Result<RecordData, PluginError> {
    let format_str = fr.format.as_deref().unwrap_or("json");
    let format: DataFormat = serde_json::from_value(
        serde_json::Value::String(format_str.to_string())
    ).unwrap_or(DataFormat::Json);

    match format {
        DataFormat::Json => {
            let bytes = serde_json::to_vec(&fr.data)
                .map_err(|e| PluginError::format_err(format!("serialize json data: {e}")))?;
            Ok(RecordData::new(bytes, DataFormat::Json))
        }
        _ => {
            let b64 = fr.data.as_str()
                .ok_or_else(|| PluginError::format_err("expected base64 string for binary format"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| PluginError::format_err(format!("base64 decode: {e}")))?;
            Ok(RecordData::new(bytes, format))
        }
    }
}

// ---- Plugin FFI entry points ----

#[derive(serde::Deserialize)]
struct FilePluginConfig {
    data_dir: String,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_topic_storage(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: FilePluginConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let storage = FileStorage::new(&cfg.data_dir);
    plugin_ok(Box::new(storage) as Box<dyn TopicStorage>)
}

server_api::qs_destroy_fn!(qs_destroy_topic_storage, TopicStorage);
server_api::qs_abi_version_fn!();
