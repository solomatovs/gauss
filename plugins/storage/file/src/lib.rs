mod config;
mod storage;

use server_api::{
    parse_plugin_config, plugin_err, plugin_ok,
    PluginCreateResult, TopicStorage,
};

use config::FilePluginConfig;
pub use storage::FileStorage;

// ════════════════════════════════════════════════════════════════
//  Plugin FFI entry points
// ════════════════════════════════════════════════════════════════

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_topic_storage(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: FilePluginConfig = match unsafe { parse_plugin_config(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let storage = FileStorage::new(&cfg.data_dir, cfg.partition_by, cfg.write_mode);
    plugin_ok(Box::new(storage) as Box<dyn TopicStorage>)
}

server_api::qs_destroy_fn!(qs_destroy_topic_storage, TopicStorage);
server_api::qs_abi_version_fn!();
