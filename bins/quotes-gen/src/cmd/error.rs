use server_api::PluginError;

#[derive(Debug, thiserror::Error)]
pub enum QuotesGenError {
    #[error("{0}")]
    Config(String),

    #[error("{0}")]
    Plugin(#[from] PluginError),

    #[error("{0}")]
    Pipeline(#[from] pipeline::PipelineError),
}
