pub mod config;
pub mod error;
mod endpoint;
mod source;
mod sink;

pub use error::PipelineError;
pub use endpoint::{Endpoint, spawn_processor_task};
pub use source::{spawn_source, spawn_source_plugin_task};
pub use sink::{spawn_sink_task, spawn_pipeline_sink};
