#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("config serialization: {0}")]
    ConfigSerialization(#[from] serde_json::Error),

    #[error("plugin load ({plugin}): {source}")]
    PluginLoad { plugin: String, source: server_api::PluginError },

    #[error("subscription ({topic}): {source}")]
    Subscription { topic: String, source: server_api::PluginError },
}
