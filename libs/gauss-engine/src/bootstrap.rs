use std::path::Path;
use std::sync::Arc;

use tokio::sync::watch;

use gauss_api::processor::{ProcessorContext, TopicReader, TopicWriter};
use gauss_api::storage::{ReadMode, StorageContext};

use crate::config::{GaussConfig, ProcessorConfig, TopicConfig};
use crate::error::EngineError;
use crate::plugin_host;
use crate::topic::{
    RegistryTopicInspector, RegistryTopicReader, RegistryTopicWriter, Topic, TopicRegistry,
};

/// Per-processor shutdown + join handle.
struct ProcessorSlot {
    name: String,
    handle: tokio::task::JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

/// The running engine — holds all topics and processor tasks.
pub struct Engine {
    registry: Arc<TopicRegistry>,
    processors: Vec<ProcessorSlot>,
    config: GaussConfig,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("registry", &self.registry)
            .field("config", &self.config)
            .finish()
    }
}

impl Engine {
    /// Bootstrap the engine from a parsed configuration.
    ///
    /// Creates topics, spawns processors as tokio tasks.
    pub async fn bootstrap(config: GaussConfig) -> Result<Self, EngineError> {
        // --- 1. Create topics ---
        let registry = Arc::new(TopicRegistry::new());
        for topic_cfg in &config.topics {
            let topic_ctx = format!("topic '{}'", topic_cfg.name);

            let mut storage = create_storage(topic_cfg)
                .map_err(|e| e.with_context(&topic_ctx))?;
            storage
                .init(StorageContext {
                    serializer: None,
                    mapping: None,
                })
                .map_err(|e| e.with_context(&topic_ctx))?;

            tracing::info!(topic = %topic_cfg.name, storage = %topic_cfg.storage, "created topic");
            registry.register(Topic::new(topic_cfg.name.clone(), storage));
        }

        // --- 2. Spawn processors ---
        let mut processors = Vec::new();
        for proc_cfg in &config.processors {
            let slot = spawn_processor(proc_cfg, &registry).await?;
            processors.push(slot);
        }

        Ok(Engine {
            registry,
            processors,
            config,
        })
    }

    /// Get the topic registry (e.g., for the API server).
    pub fn registry(&self) -> &Arc<TopicRegistry> {
        &self.registry
    }

    /// Reload configuration (SIGHUP).
    ///
    /// 1. New topics → create storage → init → register.
    /// 2. Existing topics with changed storage_config → check ParamContext,
    ///    validate, reconfigure (only Sighup params allowed to change).
    /// 3. Deleted topics → error (forbidden).
    /// 4. Changed processors → stop → recreate → init → spawn.
    /// 5. Deleted processors → stop.
    /// 6. New processors → create → init → spawn.
    pub async fn reload(&mut self, new_config: GaussConfig) -> Result<(), EngineError> {
        let old_config = &self.config;

        // --- Topics ---

        // Check for deleted topics (forbidden).
        for old_topic in &old_config.topics {
            let still_exists = new_config.topics.iter().any(|t| t.name == old_topic.name);
            if !still_exists {
                return Err(EngineError::Config(format!(
                    "topic '{}' cannot be deleted at runtime (requires restart)",
                    old_topic.name
                )));
            }
        }

        // New topics: create → init → register.
        for new_topic in &new_config.topics {
            let existed = old_config.topics.iter().any(|t| t.name == new_topic.name);
            if !existed {
                let topic_ctx = format!("topic '{}'", new_topic.name);
                let mut storage =
                    create_storage(new_topic).map_err(|e| e.with_context(&topic_ctx))?;
                storage
                    .init(StorageContext {
                        serializer: None,
                        mapping: None,
                    })
                    .map_err(|e| e.with_context(&topic_ctx))?;

                tracing::info!(topic = %new_topic.name, storage = %new_topic.storage, "created new topic (reload)");
                self.registry
                    .register(Topic::new(new_topic.name.clone(), storage));
            }
        }

        // Existing topics: check for config changes, reconfigure if needed.
        for new_topic in &new_config.topics {
            let old_topic = match old_config.topics.iter().find(|t| t.name == new_topic.name) {
                Some(t) => t,
                None => continue, // new topic, already handled above
            };

            // Check if storage_config changed.
            if old_topic.storage_config == new_topic.storage_config {
                continue; // no change
            }

            let topic_ctx = format!("topic '{}'", new_topic.name);

            // Plugin path must not change.
            if old_topic.storage != new_topic.storage {
                return Err(EngineError::Config(format!(
                    "{topic_ctx}: storage plugin path cannot be changed at runtime (requires restart)"
                )));
            }

            // Load plugin to get config params for validation.
            let path = Path::new(&new_topic.storage);
            let lib = plugin_host::PluginLib::load(
                path,
                b"qs_create_storage",
                b"qs_destroy_storage",
            )
            .map_err(|e| e.with_context(&topic_ctx))?;
            let params = lib.config_params();

            // Parse TOML → format-independent values.
            let old_raw =
                plugin_host::parse_toml_config(old_topic.storage_config.as_ref(), &params)
                    .map_err(|e| e.with_context(&topic_ctx))?;
            let new_raw =
                plugin_host::parse_toml_config(new_topic.storage_config.as_ref(), &params)
                    .map_err(|e| e.with_context(&topic_ctx))?;

            let old_values = plugin_host::validate_and_build(&old_raw, &params)
                .map_err(|e| e.with_context(&topic_ctx))?;
            let new_values = plugin_host::validate_and_build(&new_raw, &params)
                .map_err(|e| e.with_context(&topic_ctx))?;

            // Check that only Sighup-context params changed.
            plugin_host::check_sighup_changes(&old_values, &new_values, &params)
                .map_err(|e| e.with_context(&topic_ctx))?;

            // Reconfigure the live storage.
            let topic = self.registry.get(&new_topic.name).ok_or_else(|| {
                EngineError::TopicNotFound(new_topic.name.clone())
            })?;
            topic
                .reconfigure(&new_values)
                .map_err(|e| e.with_context(&topic_ctx))?;

            tracing::info!(topic = %new_topic.name, "reconfigured topic storage (reload)");
        }

        // --- Processors ---

        // Stop deleted processors.
        let mut kept = Vec::new();
        for slot in self.processors.drain(..) {
            let still_exists = new_config
                .processors
                .iter()
                .any(|p| p.name == slot.name);
            if still_exists {
                kept.push(slot);
            } else {
                tracing::info!(processor = %slot.name, "stopping removed processor (reload)");
                let _ = slot.shutdown_tx.send(true);
                let _ = slot.handle.await;
            }
        }

        // For each processor in new config: recreate if changed, keep if same.
        let mut new_processors = Vec::new();
        for proc_cfg in &new_config.processors {
            let old_proc = old_config
                .processors
                .iter()
                .find(|p| p.name == proc_cfg.name);

            let changed = match old_proc {
                None => true, // new processor
                Some(old) => processor_config_changed(old, proc_cfg),
            };

            if changed {
                // Stop old if it existed.
                if let Some(idx) = kept.iter().position(|s| s.name == proc_cfg.name) {
                    let slot = kept.remove(idx);
                    tracing::info!(processor = %slot.name, "stopping processor for reconfiguration (reload)");
                    let _ = slot.shutdown_tx.send(true);
                    let _ = slot.handle.await;
                }

                // Create new.
                let slot = spawn_processor(proc_cfg, &self.registry).await?;
                tracing::info!(processor = %proc_cfg.name, "spawned processor (reload)");
                new_processors.push(slot);
            } else {
                // Unchanged — keep existing slot.
                if let Some(idx) = kept.iter().position(|s| s.name == proc_cfg.name) {
                    new_processors.push(kept.remove(idx));
                }
            }
        }

        self.processors = new_processors;
        self.config = new_config;

        tracing::info!("config reload complete");
        Ok(())
    }

    /// Reload configuration from a file path.
    pub async fn reload_from_file(&mut self, path: &str) -> Result<(), EngineError> {
        let new_config = GaussConfig::load(path)?;
        self.reload(new_config).await
    }

    /// Graceful shutdown: signal all processors and wait for them.
    pub async fn shutdown(self) {
        for slot in &self.processors {
            let _ = slot.shutdown_tx.send(true);
        }
        for slot in self.processors {
            let _ = slot.handle.await;
        }
        tracing::info!("engine shut down");
    }
}

// ---------------------------------------------------------------------------
// Spawn a single processor from config
// ---------------------------------------------------------------------------

async fn spawn_processor(
    proc_cfg: &ProcessorConfig,
    registry: &Arc<TopicRegistry>,
) -> Result<ProcessorSlot, EngineError> {
    let reader: Option<Arc<dyn TopicReader>> = if let Some(ref source) = proc_cfg.source {
        let topic = registry.get(&source.topic).ok_or_else(|| {
            EngineError::TopicNotFound(format!(
                "processor '{}' source topic '{}'",
                proc_cfg.name, source.topic
            ))
        })?;

        let mode = parse_read_mode(&source.read)?;

        if !topic.supported_read_modes().contains(&mode) {
            return Err(EngineError::UnsupportedReadMode {
                topic: source.topic.clone(),
                mode,
            });
        }

        Some(Arc::new(RegistryTopicReader::new(topic.clone(), mode)))
    } else {
        None
    };

    let writer: Option<Arc<dyn TopicWriter>> = if let Some(ref target) = proc_cfg.target {
        let topic = registry.get(&target.topic).ok_or_else(|| {
            EngineError::TopicNotFound(format!(
                "processor '{}' target topic '{}'",
                proc_cfg.name, target.topic
            ))
        })?;
        Some(Arc::new(RegistryTopicWriter::new(topic.clone())))
    } else {
        None
    };

    let inspector = Arc::new(RegistryTopicInspector::new(registry.clone()));
    let ctx = ProcessorContext {
        reader,
        writer,
        inspector,
    };

    let proc_ctx = format!("processor '{}'", proc_cfg.name);

    let mut processor =
        create_processor(proc_cfg).map_err(|e| e.with_context(&proc_ctx))?;
    processor
        .init(ctx)
        .await
        .map_err(|e| e.with_context(&proc_ctx))?;

    let proc_name = proc_cfg.name.clone();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        tokio::select! {
            result = processor.run() => {
                match result {
                    Ok(()) => tracing::info!(processor = %proc_name, "processor stopped"),
                    Err(e) => tracing::error!(processor = %proc_name, error = %e, "processor error"),
                }
            }
            _ = shutdown_rx.changed() => {
                tracing::info!(processor = %proc_name, "processor shutting down");
                if let Err(e) = processor.stop().await {
                    tracing::error!(processor = %proc_name, error = %e, "processor stop error");
                }
            }
        }
    });

    tracing::info!(processor = %proc_cfg.name, plugin = %proc_cfg.plugin, "spawned processor");

    Ok(ProcessorSlot {
        name: proc_cfg.name.clone(),
        handle,
        shutdown_tx,
    })
}

// ---------------------------------------------------------------------------
// Factory functions: all plugins loaded via .so through plugin_host
// ---------------------------------------------------------------------------

/// Create storage from .so plugin path.
fn create_storage(cfg: &TopicConfig) -> Result<Box<dyn gauss_api::storage::TopicStorage>, EngineError> {
    let path = Path::new(&cfg.storage);
    if path.extension().is_none_or(|ext| ext != "so") {
        return Err(EngineError::Config(format!(
            "storage '{}': expected path to .so plugin",
            cfg.storage
        )));
    }
    plugin_host::load_storage(path, cfg.storage_config.as_ref())
}

/// Create processor from .so plugin path.
fn create_processor(cfg: &ProcessorConfig) -> Result<Box<dyn gauss_api::processor::Processor>, EngineError> {
    let path = Path::new(&cfg.plugin);
    if path.extension().is_none_or(|ext| ext != "so") {
        return Err(EngineError::Config(format!(
            "processor '{}': expected path to .so plugin",
            cfg.plugin
        )));
    }
    plugin_host::load_processor(path, cfg.config.as_ref())
}

/// Parse read mode string → ReadMode enum.
fn parse_read_mode(s: &str) -> Result<ReadMode, EngineError> {
    match s {
        "offset" => Ok(ReadMode::Offset),
        "latest" => Ok(ReadMode::Latest),
        "query" => Ok(ReadMode::Query),
        "snapshot" => Ok(ReadMode::Snapshot),
        "subscribe" => Ok(ReadMode::Subscribe),
        other => Err(EngineError::Config(format!("unknown read mode: '{other}'"))),
    }
}

/// Check if processor config changed between reloads.
fn processor_config_changed(old: &ProcessorConfig, new: &ProcessorConfig) -> bool {
    old.plugin != new.plugin
        || old.config != new.config
        || old.source.as_ref().map(|s| (&s.topic, &s.read))
            != new.source.as_ref().map(|s| (&s.topic, &s.read))
        || old.target.as_ref().map(|t| &t.topic)
            != new.target.as_ref().map(|t| &t.topic)
}
