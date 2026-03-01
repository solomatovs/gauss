use std::path::Path;

use libloading::{Library, Symbol};

use gauss_api::config::{ConfigParam, ConfigValues, ParamType, ParamValue};
use gauss_api::ffi::{
    AbiVersionFn, ConfigParamsFn, CreatePluginFn, DestroyPluginFn, PluginCreateResult,
    QS_ABI_VERSION,
};
use crate::error::EngineError;

/// A loaded .so plugin library with ABI version already verified.
pub struct PluginLib {
    _lib: Library,
    config_params_fn: ConfigParamsFn,
    create_fn: CreatePluginFn,
    destroy_fn: DestroyPluginFn,
}

impl PluginLib {
    /// Load a plugin .so from `path`, verify ABI version, resolve symbols.
    ///
    /// `create_symbol` / `destroy_symbol` — FFI symbol names to look up
    /// (e.g. `"qs_create_storage"`, `"qs_destroy_storage"`).
    pub fn load(
        path: &Path,
        create_symbol: &[u8],
        destroy_symbol: &[u8],
    ) -> Result<Self, EngineError> {
        let lib = unsafe { Library::new(path) }.map_err(|e| {
            EngineError::Config(format!("failed to load plugin '{}': {e}", path.display()))
        })?;

        // Check ABI version.
        let abi_fn: Symbol<AbiVersionFn> =
            unsafe { lib.get(b"qs_abi_version") }.map_err(|e| {
                EngineError::Config(format!(
                    "plugin '{}' missing qs_abi_version symbol: {e}",
                    path.display()
                ))
            })?;

        let plugin_abi = unsafe { abi_fn() };
        if plugin_abi != QS_ABI_VERSION {
            return Err(EngineError::Config(format!(
                "plugin '{}' ABI version mismatch: plugin={plugin_abi}, host={QS_ABI_VERSION}",
                path.display()
            )));
        }

        let config_params_fn: ConfigParamsFn =
            *unsafe { lib.get::<ConfigParamsFn>(b"qs_config_params") }.map_err(|e| {
                EngineError::Config(format!(
                    "plugin '{}' missing qs_config_params symbol: {e}",
                    path.display()
                ))
            })?;

        let create_fn: CreatePluginFn = *unsafe { lib.get::<CreatePluginFn>(create_symbol) }
            .map_err(|e| {
                EngineError::Config(format!(
                    "plugin '{}' missing create symbol: {e}",
                    path.display()
                ))
            })?;

        let destroy_fn: DestroyPluginFn = *unsafe { lib.get::<DestroyPluginFn>(destroy_symbol) }
            .map_err(|e| {
                EngineError::Config(format!(
                    "plugin '{}' missing destroy symbol: {e}",
                    path.display()
                ))
            })?;

        Ok(Self {
            _lib: lib,
            config_params_fn,
            create_fn,
            destroy_fn,
        })
    }

    /// Get plugin's declared config parameters.
    pub fn config_params(&self) -> Vec<ConfigParam> {
        let ptr = unsafe { (self.config_params_fn)() };
        if ptr.is_null() {
            return Vec::new();
        }
        unsafe { *Box::from_raw(ptr as *mut Vec<ConfigParam>) }
    }

    /// Call the plugin's create function with validated ConfigValues.
    pub fn create(&self, config: &ConfigValues) -> Result<*mut (), EngineError> {
        let result: PluginCreateResult =
            unsafe { (self.create_fn)(config as *const ConfigValues as *const ()) };

        if result.plugin_ptr.is_null() {
            let msg = if !result.error_ptr.is_null() && result.error_len > 0 {
                let error_msg = unsafe {
                    String::from_utf8_lossy(std::slice::from_raw_parts(
                        result.error_ptr,
                        result.error_len,
                    ))
                    .into_owned()
                };
                // Free the error string allocated by the plugin.
                unsafe {
                    let _ = Box::from_raw(core::ptr::slice_from_raw_parts_mut(
                        result.error_ptr,
                        result.error_len,
                    ));
                };
                error_msg
            } else {
                "unknown error".to_string()
            };
            return Err(EngineError::Config(format!("plugin create failed: {msg}")));
        }

        Ok(result.plugin_ptr)
    }

    /// Get the destroy function pointer (for cleanup on drop).
    pub fn destroy_fn(&self) -> DestroyPluginFn {
        self.destroy_fn
    }
}

// ---------------------------------------------------------------------------
// Config parsing & validation (format-independent)
// ---------------------------------------------------------------------------

/// Parse plugin config into format-independent key-value pairs.
///
/// `config` is a format-independent `serde_json::Value` (already deserialized
/// from TOML, YAML, or HCL by the config loader).
///
/// - Rejects unknown keys (not declared in `params`).
/// - Converts `serde_json::Value` → `ParamValue` based on declared `ParamType`.
///
/// Returns only the keys that are present in the config source.
/// Defaults and required-checks are handled by `validate_and_build`.
pub fn parse_plugin_config(
    config: Option<&serde_json::Value>,
    params: &[ConfigParam],
) -> Result<std::collections::HashMap<String, ParamValue>, EngineError> {
    let obj = match config {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => {
            return Err(EngineError::Config(
                "plugin config must be a table/object".into(),
            ))
        }
        None => return Ok(std::collections::HashMap::new()),
    };

    // Reject unknown keys — any key not declared via qs_config_params().
    let known: std::collections::HashSet<&str> =
        params.iter().map(|p| p.name.as_str()).collect();
    for key in obj.keys() {
        if !known.contains(key.as_str()) {
            return Err(EngineError::Config(format!(
                "unknown parameter '{key}'"
            )));
        }
    }

    let mut result = std::collections::HashMap::new();
    for param in params {
        if let Some(v) = obj.get(&param.name) {
            let pv = value_to_param_value(v, param)?;
            result.insert(param.name.clone(), pv);
        }
    }

    Ok(result)
}

/// Build `ConfigValues` from parsed key-value pairs (format-independent).
///
/// For each declared param:
/// - If present in `parsed`: use the value.
/// - If absent with default: use default value.
/// - If absent and required: return error.
pub fn validate_and_build(
    parsed: &std::collections::HashMap<String, ParamValue>,
    params: &[ConfigParam],
) -> Result<ConfigValues, EngineError> {
    let mut values = ConfigValues::new();

    for param in params {
        match parsed.get(&param.name) {
            Some(v) => {
                values.set(&param.name, v.clone());
            }
            None => {
                if let Some(ref default) = param.default {
                    values.set(&param.name, default.clone());
                } else if param.required {
                    return Err(EngineError::Config(format!(
                        "missing required parameter '{}'",
                        param.name
                    )));
                }
            }
        }
    }

    Ok(values)
}

/// Convert a single value to a ParamValue according to the declared type.
fn value_to_param_value(
    val: &serde_json::Value,
    param: &ConfigParam,
) -> Result<ParamValue, EngineError> {
    match param.param_type {
        ParamType::Bool => {
            let b = val.as_bool().ok_or_else(|| {
                EngineError::Config(format!("parameter '{}': expected bool", param.name))
            })?;
            Ok(ParamValue::Bool(b))
        }
        ParamType::I64 => {
            let i = val.as_i64().ok_or_else(|| {
                EngineError::Config(format!("parameter '{}': expected integer", param.name))
            })?;
            Ok(ParamValue::I64(i))
        }
        ParamType::U64 => {
            // Try u64 first (covers positive integers from any source).
            if let Some(u) = val.as_u64() {
                return Ok(ParamValue::U64(u));
            }
            // Fall back to i64 for formats that only have signed integers.
            let i = val.as_i64().ok_or_else(|| {
                EngineError::Config(format!("parameter '{}': expected integer", param.name))
            })?;
            if i < 0 {
                return Err(EngineError::Config(format!(
                    "parameter '{}': expected non-negative integer, got {i}",
                    param.name
                )));
            }
            Ok(ParamValue::U64(i as u64))
        }
        ParamType::F64 => {
            let f = val.as_f64().ok_or_else(|| {
                EngineError::Config(format!("parameter '{}': expected float", param.name))
            })?;
            Ok(ParamValue::F64(f))
        }
        ParamType::Str => Ok(ParamValue::Str(flatten_value(val))),
    }
}

/// Flatten a value into a string for flat config transport.
///
/// Scalars are converted directly (no quoting).
/// Arrays and objects are serialized as JSON strings.
fn flatten_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(val).unwrap_or_default()
        }
    }
}

// ---------------------------------------------------------------------------
// Type-safe wrappers for loading specific plugin types
// ---------------------------------------------------------------------------

use gauss_api::processor::Processor;
use gauss_api::storage::TopicStorage;

/// Load a `TopicStorage` plugin from a .so file.
///
/// 1. Load .so, verify ABI version.
/// 2. Call `qs_config_params()` to get declared params.
/// 3. Parse config → raw values, validate, build ConfigValues.
/// 4. Call `qs_create_storage(&config_values)`.
pub fn load_storage(
    path: &Path,
    config: Option<&serde_json::Value>,
) -> Result<Box<dyn TopicStorage>, EngineError> {
    let lib = PluginLib::load(path, b"qs_create_storage", b"qs_destroy_storage")?;
    let params = lib.config_params();
    let raw = parse_plugin_config(config, &params)?;
    let config_values = validate_and_build(&raw, &params)?;
    let ptr = lib.create(&config_values)?;
    // Safety: the plugin returned a Box<Box<dyn TopicStorage>>, we reconstruct it.
    let storage = unsafe { *Box::from_raw(ptr as *mut Box<dyn TopicStorage>) };
    // Leak the library to keep the .so loaded.
    std::mem::forget(lib);
    Ok(storage)
}

/// Load a `Processor` plugin from a .so file.
pub fn load_processor(
    path: &Path,
    config: Option<&serde_json::Value>,
) -> Result<Box<dyn Processor>, EngineError> {
    let lib = PluginLib::load(path, b"qs_create_processor", b"qs_destroy_processor")?;
    let params = lib.config_params();
    let raw = parse_plugin_config(config, &params)?;
    let config_values = validate_and_build(&raw, &params)?;
    let ptr = lib.create(&config_values)?;
    let processor = unsafe { *Box::from_raw(ptr as *mut Box<dyn Processor>) };
    std::mem::forget(lib);
    Ok(processor)
}

/// Filter ConfigParams to only those with Sighup context.
pub fn sighup_params(params: &[ConfigParam]) -> Vec<&ConfigParam> {
    params
        .iter()
        .filter(|p| p.context == gauss_api::config::ParamContext::Sighup)
        .collect()
}

/// Check that only Sighup-context parameters changed between old and new config.
///
/// Compares format-independent `ConfigValues`.
/// Returns an error if any Postmaster parameter changed — those require a full restart.
pub fn check_sighup_changes(
    old_values: &ConfigValues,
    new_values: &ConfigValues,
    params: &[ConfigParam],
) -> Result<(), EngineError> {
    use gauss_api::config::ParamContext;

    for param in params {
        if param.context != ParamContext::Postmaster {
            continue;
        }
        if old_values.get(&param.name) != new_values.get(&param.name) {
            return Err(EngineError::Config(format!(
                "parameter '{}' has context 'postmaster' and cannot be changed at runtime \
                 (requires restart)",
                param.name
            )));
        }
    }

    Ok(())
}
