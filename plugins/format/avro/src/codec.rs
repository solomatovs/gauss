use apache_avro::Schema;
use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok, Codec, PluginCreateResult,
    PluginError, DataFormat,
};

use super::convert::{avro_to_value, value_to_avro};

// ═══════════════════════════════════════════════════════════════
//  AvroCodec
// ═══════════════════════════════════════════════════════════════

pub struct AvroCodec {
    schema: Schema,
}

impl Codec for AvroCodec {
    fn decode(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        let mut reader = data;
        let avro_value = apache_avro::from_avro_datum(&self.schema, &mut reader, None)
            .map_err(|e| PluginError::format_err(format!("avro decode: {e}")))?;
        Ok(avro_to_value(&avro_value))
    }

    fn encode(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        let avro_value = value_to_avro(value, &self.schema)?;
        apache_avro::to_avro_datum(&self.schema, avro_value)
            .map_err(|e| PluginError::format_err(format!("avro encode: {e}")))
    }

    fn data_format(&self) -> DataFormat {
        DataFormat::Avro
    }
}

// ═══════════════════════════════════════════════════════════════
//  FFI
// ═══════════════════════════════════════════════════════════════

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct AvroCodecConfig {
    schema_path: String,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_codec(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: AvroCodecConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    if cfg.schema_path.is_empty() {
        return plugin_err("avro: schema_path is required".to_string());
    }

    let schema_str = match std::fs::read_to_string(&cfg.schema_path) {
        Ok(s) => s,
        Err(e) => {
            return plugin_err(format!(
                "avro: failed to read schema file '{}': {e}",
                cfg.schema_path
            ))
        }
    };

    let schema = match Schema::parse_str(&schema_str) {
        Ok(s) => s,
        Err(e) => return plugin_err(format!("avro: failed to parse schema: {e}")),
    };

    plugin_ok(Box::new(AvroCodec {
        schema,
    }) as Box<dyn Codec>)
}

server_api::qs_destroy_fn!(qs_destroy_codec, Codec);
