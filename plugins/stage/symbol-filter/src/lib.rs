use std::future::Future;
use std::pin::Pin;

use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok,
    ProcessContext, TopicRecord, TopicProcessor, PluginCreateResult,
    PluginError,
};
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
struct Config {
    /// Список разрешённых символов (по record.key). Пустой = пропускать все.
    #[serde(default)]
    symbols: Vec<String>,
    /// Topic для публикации отфильтрованных записей.
    /// Если не указан — запись не переиздаётся (фильтрация без пересылки).
    #[serde(default)]
    target_topic: Option<String>,
}

struct SymbolFilter {
    symbols: Vec<String>,
    target_topic: Option<String>,
}

impl TopicProcessor for SymbolFilter {
    fn process<'a>(
        &'a self,
        ctx: &'a dyn ProcessContext,
        _source_topic: &'a str,
        record: &'a TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + 'a>> {
        Box::pin(async move {
            // Фильтр: если symbols пуст — пропускать всё; иначе проверять key
            if !self.symbols.is_empty() && !self.symbols.iter().any(|s| s == &record.key) {
                return Ok(()); // filtered out
            }

            // Если есть target_topic — переиздать запись
            if let Some(ref topic) = self.target_topic {
                ctx.publish(topic, record.clone()).await?;
            }

            Ok(())
        })
    }
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_processor(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let config: Config = match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) }
    {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };
    plugin_ok(Box::new(SymbolFilter {
        symbols: config.symbols,
        target_topic: config.target_topic,
    }) as Box<dyn TopicProcessor>)
}

server_api::qs_destroy_fn!(qs_destroy_processor, TopicProcessor);
server_api::qs_abi_version_fn!();
