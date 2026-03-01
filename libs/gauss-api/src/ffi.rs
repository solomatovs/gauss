use crate::config::{ConfigParam, ConfigValues};

/// Current ABI version. Host checks this against plugin's `qs_abi_version()`.
pub const QS_ABI_VERSION: u32 = 2;

/// FFI return struct from `qs_create_*` functions.
#[repr(C)]
pub struct PluginCreateResult {
    /// Pointer to the created plugin object (Box<Box<dyn Trait>>).
    /// Null if creation failed.
    pub plugin_ptr: *mut (),
    /// Pointer to a heap-allocated error string.
    /// Null if creation succeeded.
    pub error_ptr: *mut u8,
    /// Length of the error string.
    pub error_len: usize,
}

/// Type signature for `qs_abi_version` symbol.
pub type AbiVersionFn = unsafe extern "C" fn() -> u32;

/// Type signature for `qs_config_params` symbol.
/// Returns a pointer to a boxed `Vec<ConfigParam>`. Caller takes ownership.
pub type ConfigParamsFn = unsafe extern "C" fn() -> *mut ();

/// Type signature for `qs_create_*` symbols.
/// Takes a pointer to engine-owned `ConfigValues` (plugin borrows, does not own).
pub type CreatePluginFn = unsafe extern "C" fn(*const ()) -> PluginCreateResult;

/// Type signature for `qs_destroy_*` symbols.
pub type DestroyPluginFn = unsafe extern "C" fn(*mut ());

/// Helper: create a successful `PluginCreateResult` from a trait object.
pub fn plugin_ok<T: ?Sized>(plugin: Box<Box<T>>) -> PluginCreateResult {
    PluginCreateResult {
        plugin_ptr: Box::into_raw(plugin) as *mut (),
        error_ptr: std::ptr::null_mut(),
        error_len: 0,
    }
}

/// Helper: create a failed `PluginCreateResult` from an error message.
pub fn plugin_err(msg: &str) -> PluginCreateResult {
    let bytes = msg.as_bytes().to_vec();
    let len = bytes.len();
    let ptr = Box::into_raw(bytes.into_boxed_slice()) as *mut u8;
    PluginCreateResult {
        plugin_ptr: std::ptr::null_mut(),
        error_ptr: ptr,
        error_len: len,
    }
}

/// Helper: return config params from plugin to engine.
/// Engine will reconstruct as `Box<Vec<ConfigParam>>` and take ownership.
pub fn config_params_ok(params: Vec<ConfigParam>) -> *mut () {
    Box::into_raw(Box::new(params)) as *mut ()
}

/// Cast an FFI config pointer to a `&ConfigValues` reference.
///
/// # Safety
///
/// `ptr` must be a valid pointer to a `ConfigValues` value owned by the engine.
pub unsafe fn config_from_ptr<'a>(ptr: *const ()) -> &'a ConfigValues {
    unsafe { &*(ptr as *const ConfigValues) }
}

/// Macro: export `qs_abi_version` function.
#[macro_export]
macro_rules! qs_abi_version_fn {
    () => {
        #[unsafe(no_mangle)]
        pub extern "C" fn qs_abi_version() -> u32 {
            $crate::ffi::QS_ABI_VERSION
        }
    };
}

/// Macro: export `qs_config_params` function.
///
/// Two forms:
/// - `qs_config_params_fn!(MyConfigType)` — uses `MyConfigType::config_params()` from derive.
/// - `qs_config_params_fn!([])` — manual list (e.g. empty for plugins with no config).
#[macro_export]
macro_rules! qs_config_params_fn {
    ([$($param:expr),* $(,)?]) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn qs_config_params() -> *mut () {
            $crate::ffi::config_params_ok(vec![$($param),*])
        }
    };
    ($config_type:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn qs_config_params() -> *mut () {
            $crate::ffi::config_params_ok(<$config_type>::config_params())
        }
    };
}

/// Macro: export `qs_destroy_*` function for a trait object.
#[macro_export]
macro_rules! qs_destroy_fn {
    ($name:ident, $trait_ty:path) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $name(ptr: *mut ()) {
            if !ptr.is_null() {
                let _ = unsafe { Box::from_raw(ptr as *mut Box<dyn $trait_ty>) };
            }
        }
    };
}
