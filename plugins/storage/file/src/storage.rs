use std::io::{BufRead, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::future::Future;
use std::pin::Pin;

use server_api::{date_from_ms, PluginError, RecordSchema, TopicQuery, TopicRecord, TopicStorage};

use super::config::{DiskRecord, DiskRecordWithKey, PartitionBy, WriteMode};

// ════════════════════════════════════════════════════════════════
//  FileStorage
// ════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct FileStorage {
    data_dir: PathBuf,
    partition_by: PartitionBy,
    write_mode: WriteMode,
}

impl FileStorage {
    pub(crate) fn new(data_dir: &str, partition_by: PartitionBy, write_mode: WriteMode) -> Self {
        Self {
            data_dir: PathBuf::from(data_dir),
            partition_by,
            write_mode,
        }
    }

    /// Путь к файлу для записи/чтения одной записи.
    fn record_path(&self, key: &str, ts_ms: i64) -> PathBuf {
        match self.partition_by {
            PartitionBy::Key => self.data_dir.join(format!("{key}.jsonl")),
            PartitionBy::Date => {
                let date = date_from_ms(ts_ms);
                self.data_dir.join(format!("{date}.jsonl"))
            }
        }
    }

    /// Сериализовать запись в JSON-строку для записи на диск.
    fn serialize_line(&self, record: &TopicRecord) -> Result<String, PluginError> {
        let line = match self.partition_by {
            PartitionBy::Key => serde_json::to_string(&DiskRecord {
                ts_ms: record.ts_ms,
                value: record.value.clone(),
            }),
            PartitionBy::Date => serde_json::to_string(&DiskRecordWithKey {
                ts_ms: record.ts_ms,
                key: record.key.clone(),
                value: record.value.clone(),
            }),
        };
        line.map_err(|e| PluginError::format_err(format!("json serialize: {e}")))
    }

    // ── Save ──

    fn do_save(&self, records: &[TopicRecord]) -> Result<(), PluginError> {
        for record in records {
            self.do_save_record(record)?;
        }
        Ok(())
    }

    fn do_save_record(&self, record: &TopicRecord) -> Result<(), PluginError> {
        let path = self.record_path(&record.key, record.ts_ms);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PluginError::io(format!("mkdir: {e}")))?;
        }

        let new_line = self.serialize_line(record)?;

        match self.write_mode {
            WriteMode::Append => self.append_line(&path, &new_line),
            WriteMode::Upsert => self.upsert_line(&path, &new_line, record.ts_ms),
        }
    }

    fn append_line(&self, path: &PathBuf, line: &str) -> Result<(), PluginError> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| PluginError::io(format!("open {}: {e}", path.display())))?;
        writeln!(f, "{line}").map_err(|e| PluginError::io(format!("write: {e}")))
    }

    fn upsert_line(&self, path: &PathBuf, new_line: &str, ts_ms: i64) -> Result<(), PluginError> {
        if !path.exists() {
            let mut f = std::fs::File::create(path)
                .map_err(|e| PluginError::io(format!("create {}: {e}", path.display())))?;
            writeln!(f, "{new_line}").map_err(|e| PluginError::io(format!("write: {e}")))?;
            return Ok(());
        }

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| PluginError::io(format!("open {}: {e}", path.display())))?;

        if let Some((last_line, last_pos)) = read_last_line(&mut f)? {
            if let Ok(last_ts) = parse_ts_ms(&last_line) {
                if last_ts == ts_ms {
                    f.seek(SeekFrom::Start(last_pos))
                        .map_err(|e| PluginError::io(format!("seek: {e}")))?;
                    f.set_len(last_pos)
                        .map_err(|e| PluginError::io(format!("truncate: {e}")))?;
                    writeln!(f, "{new_line}")
                        .map_err(|e| PluginError::io(format!("write: {e}")))?;
                    return Ok(());
                }
            }
        }

        f.seek(SeekFrom::End(0))
            .map_err(|e| PluginError::io(format!("seek end: {e}")))?;
        writeln!(f, "{new_line}").map_err(|e| PluginError::io(format!("write: {e}")))
    }

    // ── Query ──

    fn do_query(&self, query: &TopicQuery) -> Result<Vec<TopicRecord>, PluginError> {
        let paths = self.resolve_query_files(query)?;

        let mut result = Vec::new();
        for (implicit_key, path) in &paths {
            let f = std::fs::File::open(path)
                .map_err(|e| PluginError::io(format!("open {}: {e}", path.display())))?;
            let reader = std::io::BufReader::new(f);

            for line in reader.lines() {
                let line = line.map_err(|e| PluginError::io(format!("read line: {e}")))?;
                if line.is_empty() {
                    continue;
                }

                let record = self.parse_line(&line, implicit_key.as_deref())?;

                if let Some(ref key) = query.key {
                    if record.key != *key {
                        continue;
                    }
                }
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
            }
        }

        if paths.len() > 1 {
            result.sort_by_key(|r| r.ts_ms);
        }

        if let Some(limit) = query.limit {
            if result.len() > limit {
                result = result.split_off(result.len() - limit);
            }
        }

        Ok(result)
    }

    /// Распарсить строку JSONL в TopicRecord.
    fn parse_line(&self, line: &str, implicit_key: Option<&str>) -> Result<TopicRecord, PluginError> {
        match self.partition_by {
            PartitionBy::Key => {
                let dr: DiskRecord = serde_json::from_str(line)
                    .map_err(|e| PluginError::format_err(format!("parse json: {e}")))?;
                Ok(TopicRecord {
                    ts_ms: dr.ts_ms,
                    key: implicit_key.unwrap_or("").to_string(),
                    value: dr.value,
                    raw: None,
                })
            }
            PartitionBy::Date => {
                let dr: DiskRecordWithKey = serde_json::from_str(line)
                    .map_err(|e| PluginError::format_err(format!("parse json: {e}")))?;
                Ok(TopicRecord {
                    ts_ms: dr.ts_ms,
                    key: dr.key,
                    value: dr.value,
                    raw: None,
                })
            }
        }
    }

    /// Определить список файлов для чтения.
    fn resolve_query_files(
        &self,
        query: &TopicQuery,
    ) -> Result<Vec<(Option<String>, PathBuf)>, PluginError> {
        match self.partition_by {
            PartitionBy::Key => {
                if let Some(ref key) = query.key {
                    let p = self.data_dir.join(format!("{key}.jsonl"));
                    if p.exists() {
                        Ok(vec![(Some(key.clone()), p)])
                    } else {
                        Ok(Vec::new())
                    }
                } else {
                    self.all_key_files()
                }
            }
            PartitionBy::Date => self.date_files(query.from_ms, query.to_ms),
        }
    }

    fn all_key_files(&self) -> Result<Vec<(Option<String>, PathBuf)>, PluginError> {
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
            files.push((Some(key), entry.path()));
        }
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(files)
    }

    fn date_files(
        &self,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
    ) -> Result<Vec<(Option<String>, PathBuf)>, PluginError> {
        let dir = match std::fs::read_dir(&self.data_dir) {
            Ok(d) => d,
            Err(_) => return Ok(Vec::new()),
        };

        let from_date = from_ms.map(date_from_ms);
        let to_date = to_ms.map(date_from_ms);

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
            let date = &name[..name.len() - 6];
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
            files.push((None, entry.path()));
        }
        files.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(files)
    }
}

// ════════════════════════════════════════════════════════════════
//  TopicStorage impl
// ════════════════════════════════════════════════════════════════

impl TopicStorage for FileStorage {
    fn init(&self, _schema: Option<RecordSchema>) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
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

// ════════════════════════════════════════════════════════════════
//  Helpers
// ════════════════════════════════════════════════════════════════

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
        std::io::Read::read_exact(f, &mut buf)
            .map_err(|e| PluginError::io(format!("read: {e}")))?;

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

/// Быстро извлечь ts_ms из JSON-строки без полной десериализации.
fn parse_ts_ms(line: &str) -> Result<i64, PluginError> {
    #[derive(serde::Deserialize)]
    struct TsOnly {
        ts_ms: i64,
    }
    let ts: TsOnly = serde_json::from_str(line)
        .map_err(|e| PluginError::format_err(format!("parse ts_ms: {e}")))?;
    Ok(ts.ts_ms)
}
