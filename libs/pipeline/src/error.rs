#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("config serialization: {0}")]
    ConfigSerialization(#[from] serde_json::Error),

    #[error("plugin load ({plugin}): {source}")]
    PluginLoad { plugin: String, source: server_api::PluginError },

    #[error("subscription ({topic}): {source}")]
    Subscription { topic: String, source: server_api::PluginError },

    #[error("validation: {0}")]
    Validation(String),
}

impl PipelineError {
    /// Access the inner PluginError (if any) to inspect ErrorKind.
    pub fn plugin_error(&self) -> Option<&server_api::PluginError> {
        match self {
            PipelineError::PluginLoad { source, .. } => Some(source),
            PipelineError::Subscription { source, .. } => Some(source),
            _ => None,
        }
    }
}
