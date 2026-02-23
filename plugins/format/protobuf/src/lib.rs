use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost::Message;
use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok, Codec, FormatSerializer, PluginCreateResult,
    PluginError, RecordData, DataFormat,
};

// ---- Config ----

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct ProtobufCodecConfig {
    /// Path to FileDescriptorSet (.bin from `protoc --descriptor_set_out`).
    descriptor_path: String,
    /// Fully-qualified message type name (e.g. "market.Quote").
    message_type: String,
}

// ---- Codec ----

pub struct ProtobufCodec {
    descriptor: MessageDescriptor,
}

impl Codec for ProtobufCodec {
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError> {
        let message = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| PluginError::format_err(format!("protobuf decode: {e}")))?;
        let value: serde_json::Value = serde_json::to_value(&message)?;
        Ok((value, RecordData::new(data.to_vec(), DataFormat::Protobuf)))
    }

    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError> {
        match data.format() {
            DataFormat::Protobuf => Ok(data.as_bytes().to_vec()),
            other => Err(PluginError::format_err(format!("Protobuf codec: cannot encode {other:?} format data"))),
        }
    }
}

// ---- FFI ----

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_codec(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: ProtobufCodecConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    if cfg.descriptor_path.is_empty() {
        return plugin_err("protobuf: descriptor_path is required".to_string());
    }
    if cfg.message_type.is_empty() {
        return plugin_err("protobuf: message_type is required".to_string());
    }

    // Load FileDescriptorSet from file.
    let descriptor_bytes = match std::fs::read(&cfg.descriptor_path) {
        Ok(b) => b,
        Err(e) => {
            return plugin_err(format!(
                "protobuf: failed to read descriptor file '{}': {e}",
                cfg.descriptor_path
            ))
        }
    };

    let pool = match DescriptorPool::decode(descriptor_bytes.as_slice()) {
        Ok(p) => p,
        Err(e) => {
            return plugin_err(format!("protobuf: failed to parse descriptor set: {e}"))
        }
    };

    let descriptor = match pool.get_message_by_name(&cfg.message_type) {
        Some(d) => d,
        None => {
            return plugin_err(format!(
                "protobuf: message type '{}' not found in descriptor",
                cfg.message_type
            ))
        }
    };

    plugin_ok(Box::new(ProtobufCodec {
        descriptor,
    }) as Box<dyn Codec>)
}

server_api::qs_destroy_fn!(qs_destroy_codec, Codec);

// ---- FormatSerializer ----

struct ProtobufFormatSerializer {
    descriptor: MessageDescriptor,
}

impl FormatSerializer for ProtobufFormatSerializer {
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        let message = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| PluginError::format_err(format!("protobuf deserialize: {e}")))?;
        Ok(serde_json::to_value(&message)?)
    }

    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        let message: DynamicMessage =
            DynamicMessage::deserialize(self.descriptor.clone(), value)
                .map_err(|e| PluginError::format_err(format!("value→protobuf: {e}")))?;
        Ok(message.encode_to_vec())
    }

    fn format(&self) -> DataFormat {
        DataFormat::Protobuf
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct ProtobufFormatConfig {
    descriptor_path: String,
    message_type: String,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_format_serializer(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: ProtobufFormatConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    if cfg.descriptor_path.is_empty() {
        return plugin_err("protobuf format: descriptor_path is required".to_string());
    }
    if cfg.message_type.is_empty() {
        return plugin_err("protobuf format: message_type is required".to_string());
    }

    let descriptor_bytes = match std::fs::read(&cfg.descriptor_path) {
        Ok(b) => b,
        Err(e) => {
            return plugin_err(format!(
                "protobuf format: failed to read descriptor file '{}': {e}",
                cfg.descriptor_path
            ))
        }
    };

    let pool = match DescriptorPool::decode(descriptor_bytes.as_slice()) {
        Ok(p) => p,
        Err(e) => return plugin_err(format!("protobuf format: failed to parse descriptor set: {e}")),
    };

    let descriptor = match pool.get_message_by_name(&cfg.message_type) {
        Some(d) => d,
        None => {
            return plugin_err(format!(
                "protobuf format: message type '{}' not found in descriptor",
                cfg.message_type
            ))
        }
    };

    plugin_ok(Box::new(ProtobufFormatSerializer { descriptor }) as Box<dyn FormatSerializer>)
}

server_api::qs_destroy_fn!(qs_destroy_format_serializer, FormatSerializer);
server_api::qs_abi_version_fn!();
