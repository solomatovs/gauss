use crate::PluginError;

// ════════════════════════════════════════════════════════════════
//  Unified Plugin FFI
// ════════════════════════════════════════════════════════════════

/// Результат создания плагина (unified для всех типов).
/// Host и plugin компилируются одним компилятором в одном workspace,
/// поэтому совместимость Rust ABI гарантирована.
#[repr(C)]
pub struct PluginCreateResult {
    /// При успехе: указатель на `Box<Box<dyn Trait>>` (double-boxed для thin ptr).
    /// При ошибке: null.
    pub plugin_ptr: *mut (),
    /// При ошибке: указатель на `Box<String>` с сообщением об ошибке.
    /// При успехе: null.
    pub error_ptr: *mut (),
}

// Safety: PluginCreateResult передаётся только между host и plugin при создании.
unsafe impl Send for PluginCreateResult {}

/// Сигнатура символа `qs_create_*`, экспортируемого плагинами.
pub type CreatePluginFn =
    unsafe extern "C" fn(config_json_ptr: *const u8, config_json_len: usize) -> PluginCreateResult;

/// Сигнатура символа `qs_destroy_*`, экспортируемого плагинами.
pub type DestroyPluginFn = unsafe extern "C" fn(plugin_ptr: *mut ());

/// Helper для плагинов: вернуть успешный результат.
/// T — трейт-объект плагина (dyn TopicStorage, dyn Transport, ...).
pub fn plugin_ok<T: ?Sized>(val: Box<T>) -> PluginCreateResult {
    // Double-box: Box<dyn Trait> — fat pointer (2 words).
    // Box<Box<dyn Trait>> — thin pointer (1 word), safe для *mut ().
    let boxed: Box<Box<T>> = Box::new(val);
    PluginCreateResult {
        plugin_ptr: Box::into_raw(boxed) as *mut (),
        error_ptr: std::ptr::null_mut(),
    }
}

/// Helper для плагинов: вернуть ошибку.
pub fn plugin_err(error: String) -> PluginCreateResult {
    let boxed: Box<String> = Box::new(error);
    PluginCreateResult {
        plugin_ptr: std::ptr::null_mut(),
        error_ptr: Box::into_raw(boxed) as *mut (),
    }
}

/// Макрос для генерации `qs_destroy_*` функций в плагинах.
///
/// Пример:
/// ```ignore
/// qs_destroy_fn!(qs_destroy_codec, Codec);
/// ```
#[macro_export]
macro_rules! qs_destroy_fn {
    ($fn_name:ident, $trait_ty:path) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $fn_name(ptr: *mut ()) {
            if !ptr.is_null() {
                let _ = unsafe { Box::from_raw(ptr as *mut Box<dyn $trait_ty>) };
            }
        }
    };
}

// ════════════════════════════════════════════════════════════════
//  ABI Version
// ════════════════════════════════════════════════════════════════

/// ABI version of the plugin interface.
///
/// Bump this whenever any plugin trait changes in a binary-incompatible way:
/// - Adding/removing/reordering trait methods
/// - Changing method signatures (parameter types, return types)
/// - Changing `#[repr(C)]` struct layouts (`PluginCreateResult`)
/// - Changing FFI function signatures (`CreatePluginFn`, `DestroyPluginFn`)
pub const QS_ABI_VERSION: u32 = 7;

/// Signature of the `qs_abi_version` symbol exported by plugins.
pub type AbiVersionFn = unsafe extern "C" fn() -> u32;

/// Macro to export the `qs_abi_version` symbol from a plugin.
///
/// Every plugin crate must call this exactly once at crate root:
/// ```ignore
/// server_api::qs_abi_version_fn!();
/// ```
#[macro_export]
macro_rules! qs_abi_version_fn {
    () => {
        #[unsafe(no_mangle)]
        pub extern "C" fn qs_abi_version() -> u32 {
            $crate::QS_ABI_VERSION
        }
    };
}

// ════════════════════════════════════════════════════════════════
//  FFI Config Parsing
// ════════════════════════════════════════════════════════════════

/// Десериализовать конфиг плагина из FFI raw pointer + length.
///
/// # Safety
/// `config_json_ptr` должен указывать на `config_json_len` валидных байт.
pub unsafe fn parse_plugin_config<T: serde::de::DeserializeOwned>(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> Result<T, PluginError> {
    let json_bytes = unsafe { std::slice::from_raw_parts(config_json_ptr, config_json_len) };
    let json_str = std::str::from_utf8(json_bytes)
        .map_err(|e| PluginError::config(format!("invalid UTF-8 config: {e}")))?;
    serde_json::from_str(json_str)
        .map_err(|e| PluginError::config(format!("invalid config JSON: {e}")))
}

/// Десериализовать опциональный конфиг плагина.
/// Возвращает `T::default()` если указатель null или длина 0.
///
/// # Safety
/// Если `config_json_ptr` не null, он должен указывать на `config_json_len` валидных байт.
pub unsafe fn parse_plugin_config_opt<T: Default + serde::de::DeserializeOwned>(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> Result<T, PluginError> {
    if config_json_ptr.is_null() || config_json_len == 0 {
        return Ok(T::default());
    }
    unsafe { parse_plugin_config(config_json_ptr, config_json_len) }
}
