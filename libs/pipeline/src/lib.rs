pub mod config;
pub mod error;
mod endpoint;

pub use error::PipelineError;
pub use endpoint::{
    Endpoint,
    spawn_source,
    spawn_source_plugin_task,
    spawn_processor_task,
    spawn_sink_task,
    spawn_pipeline_sink,
};
