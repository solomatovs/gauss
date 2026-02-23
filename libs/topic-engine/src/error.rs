#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    #[error("topic '{0}' not found")]
    NotFound(String),

    #[error("storage: {0}")]
    Storage(server_api::PluginError),
}

impl TopicError {
    /// Convert to PluginError preserving ErrorKind.
    ///
    /// `Storage(PluginError)` → inner PluginError as-is (kind preserved).
    /// `NotFound` → Logic kind.
    pub fn into_plugin_error(self) -> server_api::PluginError {
        match self {
            TopicError::NotFound(name) => {
                server_api::PluginError::new(format!("topic '{name}' not found"))
            }
            TopicError::Storage(e) => e,
        }
    }
}
