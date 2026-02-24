#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("config ({context}): {detail}")]
    Config { context: &'static str, detail: String },

    #[error("no {0} configured")]
    NoComponents(&'static str),

    #[error("format '{0}' not found")]
    FormatNotFound(String),

    #[error("plugin: {0}")]
    Plugin(#[from] server_api::PluginError),

    #[error("{0}")]
    Pipeline(#[from] pipeline::PipelineError),

    #[error("signal: {0}")]
    Signal(#[from] std::io::Error),
}
