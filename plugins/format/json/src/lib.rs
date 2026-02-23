use server_api::{
    plugin_ok, PluginCreateResult, PluginError, Codec, FormatSerializer, RecordData, DataFormat,
};

pub struct JsonCodec;

impl Codec for JsonCodec {
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError> {
        let s = std::str::from_utf8(data)?;
        let value: serde_json::Value = serde_json::from_str(s)?;
        Ok((value, RecordData::new(data.to_vec(), DataFormat::Json)))
    }

    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError> {
        match data.format() {
            DataFormat::Json => Ok(data.as_bytes().to_vec()),
            other => Err(PluginError::format_err(format!("JSON codec: cannot encode {other:?} format data"))),
        }
    }
}

// ---- FFI ----

/// # Safety
/// `_config_json_ptr` must point to `_config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_codec(
    _config_json_ptr: *const u8,
    _config_json_len: usize,
) -> PluginCreateResult {
    plugin_ok(Box::new(JsonCodec) as Box<dyn Codec>)
}

server_api::qs_destroy_fn!(qs_destroy_codec, Codec);

// ---- FormatSerializer ----

struct JsonFormatSerializer;

impl FormatSerializer for JsonFormatSerializer {
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        Ok(serde_json::from_slice(data)?)
    }

    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        Ok(serde_json::to_vec(value)?)
    }

    fn format(&self) -> DataFormat {
        DataFormat::Json
    }
}

/// # Safety
/// `_config_json_ptr` must point to `_config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_format_serializer(
    _config_json_ptr: *const u8,
    _config_json_len: usize,
) -> PluginCreateResult {
    plugin_ok(Box::new(JsonFormatSerializer) as Box<dyn FormatSerializer>)
}

server_api::qs_destroy_fn!(qs_destroy_format_serializer, FormatSerializer);
server_api::qs_abi_version_fn!();
