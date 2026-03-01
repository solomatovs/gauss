use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use gauss_api::error::PluginError;
use gauss_api::processor::{Processor, ProcessorContext, TopicReader, TopicWriter};

/// Passthrough processor: reads from source topic, writes to target topic as-is.
pub struct PassthroughProcessor {
    reader: Option<Arc<dyn TopicReader>>,
    writer: Option<Arc<dyn TopicWriter>>,
}

impl PassthroughProcessor {
    pub fn new() -> Self {
        Self {
            reader: None,
            writer: None,
        }
    }
}

impl Default for PassthroughProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl Processor for PassthroughProcessor {
    fn init(
        &mut self,
        ctx: ProcessorContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async move {
            self.reader = ctx.reader;
            self.writer = ctx.writer;

            if self.reader.is_none() {
                return Err(PluginError::config(
                    "passthrough processor requires a source topic",
                ));
            }
            if self.writer.is_none() {
                return Err(PluginError::config(
                    "passthrough processor requires a target topic",
                ));
            }
            Ok(())
        })
    }

    fn run(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async move {
            let reader = self
                .reader
                .as_ref()
                .ok_or_else(|| PluginError::logic("reader not initialized"))?;
            let writer = self
                .writer
                .as_ref()
                .ok_or_else(|| PluginError::logic("writer not initialized"))?;

            while let Some(record) = reader.recv().await {
                writer.send(record).await?;
            }
            Ok(())
        })
    }

    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// FFI exports for dynamic (.so) loading
// ---------------------------------------------------------------------------

gauss_api::qs_abi_version_fn!();
gauss_api::qs_config_params_fn!([]);
gauss_api::qs_destroy_fn!(qs_destroy_processor, gauss_api::processor::Processor);

/// # Safety
///
/// `_config_ptr` must point to a valid `ConfigValues` owned by the engine.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_processor(
    _config_ptr: *const (),
) -> gauss_api::ffi::PluginCreateResult {
    gauss_api::ffi::plugin_ok(Box::new(
        Box::new(PassthroughProcessor::new()) as Box<dyn Processor>,
    ))
}
