use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{ServeArgs, ServerConfig};
use crate::error::ServerError;
use pipeline::{Endpoint, spawn_source, spawn_source_plugin_task, spawn_processor_task, spawn_sink_task, spawn_pipeline_sink};
use pipeline::config::config_json_or_empty;
use plugin_host::{
    PluginFormatSerializer, PluginTopicProcessor,
    PluginTopicSink, PluginTopicSource, StorageRegistry,
};
use storage_memory::MemoryStorageFactory;
use topic_engine::{Topic, TopicRegistry};
use server_api::{
    FormatSerializer, TopicProcessor, TopicSource, TopicSink,
    TopicPublisher, TopicSubscriber, TopicInspector, TopicCodec,
};

// ═══════════════════════════════════════════════════════════════
//  App — bootstrapped server ready to run
// ═══════════════════════════════════════════════════════════════

struct App {
    handles: Vec<JoinHandle<()>>,
    plugin_sinks: Vec<Arc<dyn TopicSink>>,
    inspector: Arc<dyn TopicInspector>,
    api_port: u16,
    ws_buffer: usize,
    ws_overflow: server_api::OverflowPolicy,
    subscriber: Arc<dyn TopicSubscriber>,
}

/// Создать и сконфигурировать все компоненты.
async fn bootstrap(config: ServerConfig, token: &CancellationToken) -> Result<App, ServerError> {
    // --- Load formats ---
    let mut formats: HashMap<String, Arc<dyn FormatSerializer>> = HashMap::new();
    for fmt_cfg in &config.formats {
        let config_json = config_json_or_empty(&fmt_cfg.config)?;
        let fmt = PluginFormatSerializer::load(&fmt_cfg.plugin, &config_json)?;
        tracing::info!(name = %fmt_cfg.name, plugin = %fmt_cfg.plugin, "loaded format");
        formats.insert(fmt_cfg.name.clone(), Arc::new(fmt));
    }

    // --- Create topics ---
    if config.topics.is_empty() {
        return Err(ServerError::NoComponents("[[topics]]"));
    }

    let mut storage_registry = StorageRegistry::new();
    storage_registry.register("memory", Box::new(MemoryStorageFactory));

    let mut registry = TopicRegistry::new();

    for topic_cfg in &config.topics {
        let fmt = formats.get(&topic_cfg.format)
            .ok_or_else(|| ServerError::FormatNotFound(topic_cfg.format.clone()))?;

        let config_json = config_json_or_empty(&topic_cfg.storage_config)?;
        let storage = storage_registry.create(&topic_cfg.storage, &config_json)?;
        storage.init(fmt.schema()).await?;

        let topic = Topic::new(topic_cfg.name.clone(), storage);
        registry.register(topic);
        tracing::info!(
            topic = %topic_cfg.name,
            storage = %topic_cfg.storage,
            format = %topic_cfg.format,
            buffer = topic_cfg.buffer,
            overflow = ?topic_cfg.overflow,
            "registered topic"
        );
    }

    // Freeze registry → typed Arc'ы для каждого суб-трейта
    let registry = Arc::new(registry);
    let publisher: Arc<dyn TopicPublisher> = registry.clone();
    let subscriber: Arc<dyn TopicSubscriber> = registry.clone();
    let codec: Arc<dyn TopicCodec> = registry.clone();
    let inspector: Arc<dyn TopicInspector> = registry.clone();

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    // --- Spawn processors (all plugin-based) ---
    for (i, proc_cfg) in config.processors.iter().enumerate() {
        let config_json = config_json_or_empty(&proc_cfg.config)?;
        let plugin_proc = PluginTopicProcessor::load(&proc_cfg.plugin, &config_json)?;
        tracing::info!(
            index = i,
            plugin = %proc_cfg.plugin,
            trigger = %proc_cfg.trigger,
            "loaded processor"
        );
        let processor: Arc<dyn TopicProcessor> = Arc::new(plugin_proc);

        let subscription = subscriber
            .subscribe(&proc_cfg.trigger, proc_cfg.buffer, proc_cfg.overflow)
            .await?;

        handles.push(spawn_processor_task(
            processor,
            proc_cfg.trigger.clone(),
            subscription,
            publisher.clone(),
            codec.clone(),
            inspector.clone(),
            token.clone(),
        ));
    }

    // --- Spawn sinks ---
    let mut plugin_sinks: Vec<Arc<dyn TopicSink>> = Vec::new();

    for sink_cfg in &config.sinks {
        sink_cfg.validate()?;

        if sink_cfg.is_pipeline() {
            // Pipeline sink: transport + framing + middleware + codec
            let endpoint = Endpoint::load(
                &sink_cfg.name,
                sink_cfg.transport.as_ref().unwrap(),
                &sink_cfg.transport_config,
                sink_cfg.framing.as_ref().unwrap(),
                &sink_cfg.framing_config,
                &sink_cfg.middleware,
                sink_cfg.codec.as_ref().unwrap(),
                &sink_cfg.codec_config,
            )?;
            handles.push(spawn_pipeline_sink(
                endpoint,
                sink_cfg.topics.clone(),
                &subscriber,
                sink_cfg.buffer,
                sink_cfg.overflow,
                sink_cfg.conn_buffer,
                sink_cfg.conn_overflow,
                token.clone(),
            ).await?);
            tracing::info!(
                sink = %sink_cfg.name,
                transport = %sink_cfg.transport.as_deref().unwrap_or("?"),
                codec = %sink_cfg.codec.as_deref().unwrap_or("?"),
                topics = ?sink_cfg.topics,
                "spawned pipeline sink"
            );
        } else {
            // Plugin sink (legacy monolithic)
            let plugin_path = sink_cfg.plugin.as_ref().unwrap();
            let config_json = config_json_or_empty(&sink_cfg.config)?;
            let ps = PluginTopicSink::load(plugin_path, &config_json)?;
            ps.init(inspector.clone()).await?;

            let arc_sink: Arc<dyn TopicSink> = Arc::new(ps);
            plugin_sinks.push(arc_sink.clone());

            handles.push(spawn_sink_task(
                arc_sink,
                sink_cfg.name.clone(),
                sink_cfg.topics.clone(),
                &subscriber,
                sink_cfg.buffer,
                sink_cfg.overflow,
                token.clone(),
            ).await?);
            tracing::info!(
                sink = %sink_cfg.name,
                plugin = %plugin_path,
                topics = ?sink_cfg.topics,
                "spawned plugin sink"
            );
        }
    }

    // --- Load sources ---
    if config.sources.is_empty() {
        return Err(ServerError::NoComponents("[[sources]]"));
    }

    for source_cfg in &config.sources {
        source_cfg.validate()?;

        if source_cfg.is_pipeline() {
            // Pipeline source: transport + framing + middleware + codec
            let mut endpoint = Endpoint::load(
                &source_cfg.name,
                source_cfg.transport.as_ref().unwrap(),
                &source_cfg.transport_config,
                source_cfg.framing.as_ref().unwrap(),
                &source_cfg.framing_config,
                &source_cfg.middleware,
                source_cfg.codec.as_ref().unwrap(),
                &source_cfg.codec_config,
            )?;
            endpoint.key_field = source_cfg.key_field.clone();
            endpoint.ts_field = source_cfg.ts_field.clone();
            handles.push(spawn_source(
                endpoint,
                source_cfg.topic.clone(),
                publisher.clone(),
                source_cfg.buffer,
                source_cfg.overflow,
                source_cfg.conn_buffer,
                source_cfg.conn_overflow,
                token.clone(),
            ));
            tracing::info!(
                source = %source_cfg.name,
                transport = %source_cfg.transport.as_deref().unwrap_or("?"),
                codec = %source_cfg.codec.as_deref().unwrap_or("?"),
                topic = %source_cfg.topic,
                "spawned pipeline source"
            );
        } else {
            // Plugin source (monolithic)
            let plugin_path = source_cfg.plugin.as_ref().unwrap();
            let config_json = config_json_or_empty(&source_cfg.config)?;
            let ps = PluginTopicSource::load(plugin_path, &config_json)?;
            let arc_source: Arc<dyn TopicSource> = Arc::new(ps);

            handles.push(spawn_source_plugin_task(
                arc_source,
                source_cfg.name.clone(),
                source_cfg.topic.clone(),
                publisher.clone(),
                token.clone(),
            ));
            tracing::info!(
                source = %source_cfg.name,
                plugin = %plugin_path,
                topic = %source_cfg.topic,
                "spawned plugin source"
            );
        }
    }

    Ok(App {
        handles,
        plugin_sinks,
        inspector,
        api_port: config.api_port,
        ws_buffer: config.ws_buffer,
        ws_overflow: config.ws_overflow,
        subscriber,
    })
}

// ═══════════════════════════════════════════════════════════════
//  Lifecycle — run API + wait for shutdown + drain + flush
// ═══════════════════════════════════════════════════════════════

impl App {
    async fn serve(self, token: CancellationToken) -> Result<(), ServerError> {
        // --- API server (HTTP + WS) ---
        let api_handle = tokio::spawn({
            let inspector = self.inspector.clone();
            let subscriber = self.subscriber.clone();
            let api_token = token.clone();
            let port = self.api_port;
            let ws_buffer = self.ws_buffer;
            let ws_overflow = self.ws_overflow;
            async move {
                if let Err(e) = topic_api_server::run(port, inspector, subscriber, ws_buffer, ws_overflow, api_token).await {
                    tracing::error!(error = %e, "api server error");
                }
            }
        });

        tracing::info!(port = self.api_port, "api server (http+ws) listening");
        tracing::info!("server ready");

        // --- Ожидание Ctrl+C ---
        tokio::signal::ctrl_c().await?;
        tracing::info!("shutting down...");

        // Signal all tasks to stop cooperatively
        token.cancel();

        // Drain: wait up to 5s for tasks to finish gracefully
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Abort anything still running
        for h in &self.handles {
            if !h.is_finished() {
                h.abort();
            }
        }
        if !api_handle.is_finished() {
            api_handle.abort();
        }

        // Wait for all tasks to complete
        for h in self.handles {
            let _ = h.await;
        }
        let _ = api_handle.await;

        // Flush all topic storages via TopicInspector
        for topic_name in self.inspector.topics() {
            if let Err(e) = self.inspector.flush_topic(&topic_name).await {
                tracing::error!(topic = %topic_name, error = ?e, "flush error");
            }
        }

        // Flush plugin sinks
        for ps in &self.plugin_sinks {
            if let Err(e) = ps.flush().await {
                tracing::error!(error = ?e, "plugin sink flush error");
            }
        }

        tracing::info!("shutdown complete");
        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════
//  Entry point
// ═══════════════════════════════════════════════════════════════

pub async fn run(args: ServeArgs) -> Result<(), ServerError> {
    tracing::info!("gauss-server starting");

    let config = ServerConfig::load(&args.config)?;
    tracing::info!(config = %args.config, "loaded config");

    let token = CancellationToken::new();
    let app = bootstrap(config, &token).await?;
    app.serve(token).await
}
