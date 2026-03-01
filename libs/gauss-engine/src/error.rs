use gauss_api::error::PluginError;
use gauss_api::storage::ReadMode;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("config error: {0}")]
    Config(String),

    #[error("plugin error: {0}")]
    Plugin(#[from] PluginError),

    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("read mode {mode:?} not supported by storage of topic '{topic}'")]
    UnsupportedReadMode { topic: String, mode: ReadMode },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl EngineError {
    /// Add context to the error.
    ///
    /// For `Plugin` variant, context is added to the inner `PluginError`.
    /// For other variants, context is prepended to the message.
    pub fn with_context(self, ctx: impl std::fmt::Display) -> Self {
        match self {
            EngineError::Plugin(e) => EngineError::Plugin(e.with_context(ctx)),
            EngineError::Config(msg) => EngineError::Config(format!("{ctx}: {msg}")),
            EngineError::TopicNotFound(msg) => EngineError::TopicNotFound(format!("{ctx}: {msg}")),
            other => other,
        }
    }
}
