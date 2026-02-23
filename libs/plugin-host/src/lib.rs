use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use libloading::{Library, Symbol};

use server_api::{
    AbiVersionFn, CreatePluginFn, DataFormat, PluginCreateResult, PluginError,
    QS_ABI_VERSION, RecordData, TopicRecord, TopicQuery,
    Codec, FormatSerializer, Framing, Middleware, TransportStream,
    Transport, TopicStorage, TopicProcessor, TopicSink, TopicSource, ProcessContext,
};

// ════════════════════════════════════════════════════════════════
//  Generic plugin loading
// ════════════════════════════════════════════════════════════════

/// Загрузить .so плагин: найти символ `create_symbol`, вызвать его,
/// десериализовать результат как `Box<dyn T>`.
///
/// Возвращает (inner, library). Порядок drop важен: inner первым.
///
/// # Safety
/// `create_symbol` должен соответствовать ожидаемому типу трейта.
unsafe fn load_plugin<T: ?Sized>(
    plugin_path: &str,
    create_symbol: &[u8],
    config_json: &str,
) -> Result<(Box<T>, Library), PluginError> {
    let lib = unsafe { Library::new(plugin_path) }
        .map_err(|e| PluginError::config(format!("failed to load plugin '{plugin_path}': {e}")))?;

    // ── ABI version check ──
    let version_fn: Symbol<AbiVersionFn> = unsafe { lib.get(b"qs_abi_version") }
        .map_err(|_| PluginError::config(format!(
            "plugin '{plugin_path}' does not export 'qs_abi_version' — \
             likely compiled against an older server-api. Rebuild the plugin."
        )))?;
    let plugin_version = unsafe { version_fn() };
    if plugin_version != QS_ABI_VERSION {
        return Err(PluginError::config(format!(
            "ABI version mismatch for plugin '{plugin_path}': \
             plugin has version {plugin_version}, host expects {QS_ABI_VERSION}. \
             Rebuild the plugin."
        )));
    }

    let create_fn: Symbol<CreatePluginFn> = unsafe { lib.get(create_symbol) }
        .map_err(|e| {
            let sym = String::from_utf8_lossy(create_symbol);
            PluginError::config(format!("symbol '{sym}' not found in '{plugin_path}': {e}"))
        })?;

    let result: PluginCreateResult = unsafe { create_fn(config_json.as_ptr(), config_json.len()) };

    if !result.error_ptr.is_null() {
        let error = unsafe { *Box::from_raw(result.error_ptr as *mut String) };
        return Err(PluginError::config(format!("plugin '{plugin_path}' error: {error}")));
    }

    if result.plugin_ptr.is_null() {
        return Err(PluginError::config(format!("plugin '{plugin_path}' returned null")));
    }

    let inner: Box<T> = unsafe { *Box::from_raw(result.plugin_ptr as *mut Box<T>) };
    Ok((inner, lib))
}

// ════════════════════════════════════════════════════════════════
//  Plugin wrapper macro
// ════════════════════════════════════════════════════════════════

/// Генерирует struct + load() + get()/get_mut() + Drop для plugin wrapper'а.
macro_rules! define_plugin_wrapper {
    ($name:ident, $trait_ty:path, $symbol:literal $(, unsafe $marker:ident)*) => {
        pub struct $name {
            inner: Option<Box<dyn $trait_ty>>,
            _lib: Library,
        }

        $(unsafe impl $marker for $name {})*

        impl $name {
            pub fn load(plugin_path: &str, config_json: &str) -> Result<Self, PluginError> {
                let (inner, lib) = unsafe {
                    load_plugin::<dyn $trait_ty>(plugin_path, $symbol, config_json)?
                };
                Ok(Self { inner: Some(inner), _lib: lib })
            }

            #[inline]
            #[allow(dead_code)]
            fn get(&self) -> Result<&(dyn $trait_ty + '_), PluginError> {
                self.inner.as_ref()
                    .map(|b| &**b)
                    .ok_or_else(|| PluginError::new(concat!(stringify!($name), " already dropped")))
            }

            #[inline]
            #[allow(dead_code)]
            fn get_mut(&mut self) -> Result<&mut (dyn $trait_ty + '_), PluginError> {
                match self.inner.as_mut() {
                    Some(b) => Ok(&mut **b),
                    None => Err(PluginError::new(concat!(stringify!($name), " already dropped"))),
                }
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                // Дропаем inner первым, пока _lib (и vtable) ещё живы.
                self.inner.take();
            }
        }
    };
}

// ════════════════════════════════════════════════════════════════
//  Plugin wrapper definitions
// ════════════════════════════════════════════════════════════════

define_plugin_wrapper!(PluginTopicStorage, TopicStorage, b"qs_create_topic_storage", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginTopicProcessor, TopicProcessor, b"qs_create_processor", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginTopicSink, TopicSink, b"qs_create_sink", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginTopicSource, TopicSource, b"qs_create_topic_source", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginTransport, Transport, b"qs_create_transport", unsafe Send);
define_plugin_wrapper!(PluginFraming, Framing, b"qs_create_framing", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginCodec, Codec, b"qs_create_codec", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginMiddleware, Middleware, b"qs_create_middleware", unsafe Send, unsafe Sync);
define_plugin_wrapper!(PluginFormatSerializer, FormatSerializer, b"qs_create_format_serializer", unsafe Send, unsafe Sync);

// ════════════════════════════════════════════════════════════════
//  Trait implementations (unique per plugin type)
// ════════════════════════════════════════════════════════════════

impl TopicStorage for PluginTopicStorage {
    fn init(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.init(),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn save(&self, records: &[TopicRecord]) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.save(records),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn query(&self, query: &TopicQuery) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRecord>, PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.query(query),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.flush(),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

impl TopicProcessor for PluginTopicProcessor {
    fn process<'a>(
        &'a self,
        ctx: &'a dyn ProcessContext,
        source_topic: &'a str,
        record: &'a TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + 'a>> {
        match self.get() {
            Ok(inner) => inner.process(ctx, source_topic, record),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

impl TopicSink for PluginTopicSink {
    fn init(&self, ctx: Arc<dyn ProcessContext>) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.init(ctx),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn send(&self, topic: &str, record: &TopicRecord) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.send(topic, record),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.flush(),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

impl TopicSource for PluginTopicSource {
    fn start(
        &self,
        ctx: Arc<dyn ProcessContext>,
        target_topic: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.start(ctx, target_topic),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }

    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + '_>> {
        match self.get() {
            Ok(inner) => inner.stop(),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

impl Transport for PluginTransport {
    fn start(&mut self) -> Result<(), PluginError> {
        self.get_mut()?.start()
    }

    fn next_connection(&mut self) -> Result<Option<Box<dyn TransportStream>>, PluginError> {
        self.get_mut()?.next_connection()
    }

    fn stop(&mut self) -> Result<(), PluginError> {
        self.get_mut()?.stop()
    }
}

impl Framing for PluginFraming {
    fn decode(&self, buf: &[u8]) -> Result<Option<(Vec<u8>, usize)>, PluginError> {
        self.get()?.decode(buf)
    }

    fn encode(&self, data: &[u8], buf: &mut Vec<u8>) -> Result<(), PluginError> {
        self.get()?.encode(data, buf)
    }
}

impl Codec for PluginCodec {
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError> {
        self.get()?.decode(data)
    }

    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError> {
        self.get()?.encode(data)
    }
}

impl Middleware for PluginMiddleware {
    fn decode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError> {
        self.get()?.decode(data)
    }

    fn encode(&self, data: Vec<u8>) -> Result<Vec<u8>, PluginError> {
        self.get()?.encode(data)
    }
}

impl FormatSerializer for PluginFormatSerializer {
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        self.get()?.deserialize(data)
    }

    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        self.get()?.serialize(value)
    }

    fn format(&self) -> DataFormat {
        self.get().map(|inner| inner.format()).unwrap_or(DataFormat::Raw)
    }
}
