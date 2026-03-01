use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use gauss_api::error::PluginError;
use gauss_api::record::TopicRecord;
use gauss_api::storage::{ReadMode, ReadParams, ReadResult, StorageContext, TopicStorage};

/// What to do when ring buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFull {
    /// Drop the incoming record silently.
    Drop,
    /// Overwrite the oldest record (default ring buffer behavior).
    Overwrite,
}

impl WriteFull {
    fn parse(s: &str) -> Result<Self, PluginError> {
        match s {
            "drop" => Ok(Self::Drop),
            "overwrite" => Ok(Self::Overwrite),
            other => Err(PluginError::config(format!(
                "unknown write_full: {other} (expected 'drop' or 'overwrite')"
            ))),
        }
    }
}

/// Configuration for memory ring buffer storage.
#[derive(Debug, gauss_api::ConfigParams)]
pub struct MemoryStorageConfig {
    #[param(context = "postmaster", description = "Maximum number of records in the ring buffer")]
    pub storage_size: u64,

    #[param(context = "sighup", description = "Behavior when buffer is full: 'drop' or 'overwrite'")]
    pub write_full: String,
}

impl Default for MemoryStorageConfig {
    fn default() -> Self {
        Self {
            storage_size: 4096,
            write_full: "overwrite".to_string(),
        }
    }
}

/// Internal record with a monotonic offset for cursor tracking.
struct OffsetRecord {
    offset: u64,
    record: TopicRecord,
}

/// In-memory ring buffer storage.
///
/// Stores `TopicRecord` as-is (opaque bytes). No deserialization needed.
/// Supports read modes: Offset, Latest, Query.
pub struct MemoryRingBuffer {
    storage_size: usize,
    write_full: RwLock<WriteFull>,
    buffer: RwLock<VecDeque<OffsetRecord>>,
    next_offset: AtomicU64,
}

impl MemoryRingBuffer {
    pub fn new(config: MemoryStorageConfig) -> Result<Self, PluginError> {
        if config.storage_size == 0 {
            return Err(PluginError::config("storage_size must be > 0"));
        }

        let storage_size = config.storage_size as usize;
        let write_full = WriteFull::parse(&config.write_full)?;

        Ok(Self {
            buffer: RwLock::new(VecDeque::with_capacity(storage_size)),
            storage_size,
            write_full: RwLock::new(write_full),
            next_offset: AtomicU64::new(0),
        })
    }
}

impl TopicStorage for MemoryRingBuffer {
    fn init(&mut self, _ctx: StorageContext) -> Result<(), PluginError> {
        // Ring buffer mode doesn't need serializer or mapping.
        Ok(())
    }

    fn save(&self, record: TopicRecord) -> Result<(), PluginError> {
        let mut buf = self.buffer.write().map_err(|e| PluginError::logic(e.to_string()))?;
        let write_full = *self.write_full.read().map_err(|e| PluginError::logic(e.to_string()))?;

        if buf.len() >= self.storage_size {
            match write_full {
                WriteFull::Drop => return Ok(()),
                WriteFull::Overwrite => {
                    buf.pop_front();
                }
            }
        }

        let offset = self.next_offset.fetch_add(1, Ordering::Relaxed);
        buf.push_back(OffsetRecord { offset, record });
        Ok(())
    }

    fn read(&self, mode: &ReadMode, params: &ReadParams) -> Result<ReadResult, PluginError> {
        let buf = self.buffer.read().map_err(|e| PluginError::logic(e.to_string()))?;

        match mode {
            ReadMode::Offset => {
                let start_offset = params.offset.unwrap_or(0);
                let limit = params.limit.unwrap_or(100);

                let mut records = Vec::new();
                let mut last_offset = start_offset;

                for entry in buf.iter() {
                    if entry.offset < start_offset {
                        continue;
                    }
                    if records.len() >= limit {
                        break;
                    }
                    records.push(TopicRecord {
                        ts_ms: entry.record.ts_ms,
                        data: entry.record.data.clone(),
                    });
                    last_offset = entry.offset + 1;
                }

                Ok(ReadResult {
                    next_offset: Some(last_offset),
                    records,
                })
            }
            ReadMode::Latest => {
                let limit = params.limit.unwrap_or(1);
                let records: Vec<TopicRecord> = buf
                    .iter()
                    .rev()
                    .take(limit)
                    .map(|e| TopicRecord {
                        ts_ms: e.record.ts_ms,
                        data: e.record.data.clone(),
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();

                let next_offset = buf.back().map(|e| e.offset + 1);

                Ok(ReadResult {
                    records,
                    next_offset,
                })
            }
            ReadMode::Query => {
                let from_ms = params.from_ms.unwrap_or(i64::MIN);
                let to_ms = params.to_ms.unwrap_or(i64::MAX);
                let limit = params.limit.unwrap_or(1000);

                let records: Vec<TopicRecord> = buf
                    .iter()
                    .filter(|e| e.record.ts_ms >= from_ms && e.record.ts_ms <= to_ms)
                    .take(limit)
                    .map(|e| TopicRecord {
                        ts_ms: e.record.ts_ms,
                        data: e.record.data.clone(),
                    })
                    .collect();

                Ok(ReadResult {
                    records,
                    next_offset: None,
                })
            }
            other => Err(PluginError::logic(format!(
                "read mode {other:?} not supported by memory ring buffer"
            ))),
        }
    }

    fn supported_read_modes(&self) -> &[ReadMode] {
        &[ReadMode::Offset, ReadMode::Latest, ReadMode::Query]
    }

    fn reconfigure(&self, config: &gauss_api::config::ConfigValues) -> Result<(), PluginError> {
        // Only write_full is Sighup — storage_size is Postmaster (engine already checked).
        if let Some(val) = config.get_str("write_full") {
            let new_wf = WriteFull::parse(val)?;
            let mut wf = self.write_full.write().map_err(|e| PluginError::logic(e.to_string()))?;
            *wf = new_wf;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// FFI exports for dynamic (.so) loading
// ---------------------------------------------------------------------------

gauss_api::qs_abi_version_fn!();
gauss_api::qs_config_params_fn!(MemoryStorageConfig);
gauss_api::qs_destroy_fn!(qs_destroy_storage, gauss_api::storage::TopicStorage);

/// # Safety
///
/// `config_ptr` must point to a valid `ConfigValues` owned by the engine.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_storage(
    config_ptr: *const (),
) -> gauss_api::ffi::PluginCreateResult {
    let config = unsafe { gauss_api::ffi::config_from_ptr(config_ptr) };
    match MemoryStorageConfig::from_config(config).and_then(MemoryRingBuffer::new) {
        Ok(storage) => gauss_api::ffi::plugin_ok(Box::new(
            Box::new(storage) as Box<dyn TopicStorage>,
        )),
        Err(e) => gauss_api::ffi::plugin_err(&e.to_string()),
    }
}
