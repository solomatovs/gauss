use apache_avro::types::Value;
use apache_avro::Schema;
use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok, Codec, FormatSerializer, PluginCreateResult,
    PluginError, RecordData, DataFormat,
};

// ---- Config ----

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct AvroCodecConfig {
    /// Path to Avro schema file (.avsc).
    schema_path: String,
}

// ---- Avro ↔ Value conversion ----

fn avro_to_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::json!(i),
        Value::Long(l) => serde_json::json!(l),
        Value::Float(f) => serde_json::json!(f),
        Value::Double(d) => serde_json::json!(d),
        Value::Bytes(b) | Value::Fixed(_, b) => {
            serde_json::Value::String(base64_encode(b))
        }
        Value::String(s) | Value::Enum(_, s) => {
            serde_json::Value::String(s.clone())
        }
        Value::Union(_, inner) => avro_to_value(inner),
        Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(avro_to_value).collect())
        }
        Value::Map(entries) => {
            let map: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), avro_to_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Value::Record(fields) => {
            let map: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_to_value(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Value::Date(d) => serde_json::json!(d),
        Value::TimeMillis(t) => serde_json::json!(t),
        Value::TimeMicros(t) => serde_json::json!(t),
        Value::TimestampMillis(t) => serde_json::json!(t),
        Value::TimestampMicros(t) => serde_json::json!(t),
        Value::TimestampNanos(t) => serde_json::json!(t),
        Value::Decimal(d) => {
            // Decimal as raw bytes in base64
            let bytes: Vec<u8> = d.try_into().unwrap_or_default();
            serde_json::Value::String(base64_encode(&bytes))
        }
        Value::BigDecimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(u) => serde_json::Value::String(u.to_string()),
        Value::Duration(_) => serde_json::Value::Null,
        Value::LocalTimestampMillis(t) => serde_json::json!(t),
        Value::LocalTimestampMicros(t) => serde_json::json!(t),
        Value::LocalTimestampNanos(t) => serde_json::json!(t),
    }
}

/// Simple base64 encode (no padding, standard alphabet).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[(n >> 18 & 0x3F) as usize] as char);
        result.push(CHARS[(n >> 12 & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[(n >> 6 & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

// ---- Codec impl ----

pub struct AvroCodec {
    schema: Schema,
}

impl Codec for AvroCodec {
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError> {
        let mut reader = data;
        let avro_value = apache_avro::from_avro_datum(&self.schema, &mut reader, None)
            .map_err(|e| PluginError::format_err(format!("avro decode: {e}")))?;
        let parsed = avro_to_value(&avro_value);
        Ok((parsed, RecordData::new(data.to_vec(), DataFormat::Avro)))
    }

    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError> {
        match data.format() {
            DataFormat::Avro => Ok(data.as_bytes().to_vec()),
            other => Err(PluginError::format_err(format!("Avro codec: cannot encode {other:?} format data"))),
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
    let cfg: AvroCodecConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    if cfg.schema_path.is_empty() {
        return plugin_err("avro: schema_path is required".to_string());
    }

    // Load schema from file.
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

// ---- FormatSerializer ----

/// Конвертация serde_json::Value → apache_avro::types::Value.
fn value_to_avro(val: &serde_json::Value, schema: &Schema) -> Result<Value, PluginError> {
    match (val, schema) {
        (serde_json::Value::Null, _) => Ok(Value::Null),
        (serde_json::Value::Bool(b), _) => Ok(Value::Boolean(*b)),
        (serde_json::Value::Number(n), Schema::Int) => {
            Ok(Value::Int(n.as_i64().unwrap_or(0) as i32))
        }
        (serde_json::Value::Number(n), Schema::Long) => {
            Ok(Value::Long(n.as_i64().unwrap_or(0)))
        }
        (serde_json::Value::Number(n), Schema::Float) => {
            Ok(Value::Float(n.as_f64().unwrap_or(0.0) as f32))
        }
        (serde_json::Value::Number(n), Schema::Double) => {
            Ok(Value::Double(n.as_f64().unwrap_or(0.0)))
        }
        (serde_json::Value::Number(n), _) => {
            // Fallback: try long, then double
            if let Some(i) = n.as_i64() {
                Ok(Value::Long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Double(f))
            } else {
                Ok(Value::Null)
            }
        }
        (serde_json::Value::String(s), _) => Ok(Value::String(s.clone())),
        (serde_json::Value::Array(items), Schema::Array(inner)) => {
            let avro_items: Result<Vec<Value>, PluginError> = items
                .iter()
                .map(|item| value_to_avro(item, &inner.items))
                .collect();
            Ok(Value::Array(avro_items?))
        }
        (serde_json::Value::Object(map), Schema::Record(record_schema)) => {
            let mut fields = Vec::new();
            for field in &record_schema.fields {
                let field_val = map.get(&field.name).unwrap_or(&serde_json::Value::Null);
                let avro_val = value_to_avro(field_val, &field.schema)?;
                fields.push((field.name.clone(), avro_val));
            }
            Ok(Value::Record(fields))
        }
        (val, Schema::Union(union_schema)) => {
            // Try each variant, use the first that succeeds
            for (idx, variant) in union_schema.variants().iter().enumerate() {
                if let Ok(v) = value_to_avro(val, variant) {
                    return Ok(Value::Union(idx as u32, Box::new(v)));
                }
            }
            Err(PluginError::format_err(format!("avro: cannot convert Value to union: {val}")))
        }
        _ => Ok(Value::Null),
    }
}

struct AvroFormatSerializer {
    schema: Schema,
}

impl FormatSerializer for AvroFormatSerializer {
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        let mut reader = data;
        let avro_value = apache_avro::from_avro_datum(&self.schema, &mut reader, None)
            .map_err(|e| PluginError::format_err(format!("avro deserialize: {e}")))?;
        Ok(avro_to_value(&avro_value))
    }

    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        let avro_value = value_to_avro(value, &self.schema)?;
        apache_avro::to_avro_datum(&self.schema, avro_value)
            .map_err(|e| PluginError::format_err(format!("avro serialize: {e}")))
    }

    fn format(&self) -> DataFormat {
        DataFormat::Avro
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct AvroFormatConfig {
    schema_path: String,
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_format_serializer(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: AvroFormatConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    if cfg.schema_path.is_empty() {
        return plugin_err("avro format: schema_path is required".to_string());
    }

    let schema_str = match std::fs::read_to_string(&cfg.schema_path) {
        Ok(s) => s,
        Err(e) => {
            return plugin_err(format!(
                "avro format: failed to read schema file '{}': {e}",
                cfg.schema_path
            ))
        }
    };

    let schema = match Schema::parse_str(&schema_str) {
        Ok(s) => s,
        Err(e) => return plugin_err(format!("avro format: failed to parse schema: {e}")),
    };

    plugin_ok(Box::new(AvroFormatSerializer { schema }) as Box<dyn FormatSerializer>)
}

server_api::qs_destroy_fn!(qs_destroy_format_serializer, FormatSerializer);
server_api::qs_abi_version_fn!();
