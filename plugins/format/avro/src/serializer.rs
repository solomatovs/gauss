use apache_avro::Schema;
use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok, FormatSerializer,
    PluginCreateResult, PluginError, DataFormat, Field, FieldType, RecordSchema, ScalarType,
};

use super::convert::{avro_to_value, value_to_avro};

// ═══════════════════════════════════════════════════════════════
//  AvroFormatSerializer
// ═══════════════════════════════════════════════════════════════

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

    fn schema(&self) -> Option<RecordSchema> {
        match &self.schema {
            Schema::Record(record_schema) => {
                let fields: Vec<Field> = record_schema.fields.iter()
                    .filter_map(|f| {
                        avro_schema_to_field(&f.name, &f.schema)
                    })
                    .collect();
                if fields.is_empty() { None } else { Some(RecordSchema::new(fields)) }
            }
            _ => None,
        }
    }
}

/// Отобразить Avro-поле в Field с rich типизацией.
fn avro_schema_to_field(name: &str, schema: &Schema) -> Option<Field> {
    match schema {
        // Nullable union: ["null", T] → nullable field
        Schema::Union(union_schema) => {
            let non_null: Vec<&Schema> = union_schema.variants()
                .iter()
                .filter(|v| !matches!(v, Schema::Null))
                .collect();
            if non_null.len() == 1 {
                let scalar = avro_schema_to_scalar(non_null[0])?;
                Some(Field::new(name, FieldType::Scalar(scalar), true))
            } else {
                Some(Field::new(name, FieldType::Scalar(ScalarType::Json), true))
            }
        }
        Schema::Array(inner) => {
            let scalar = avro_schema_to_scalar(&inner.items)?;
            Some(Field::new(name, FieldType::Array(scalar), false))
        }
        other => {
            let scalar = avro_schema_to_scalar(other)?;
            Some(Field::new(name, FieldType::Scalar(scalar), false))
        }
    }
}

/// Отобразить тип Avro-схемы в ScalarType.
fn avro_schema_to_scalar(schema: &Schema) -> Option<ScalarType> {
    match schema {
        Schema::Boolean => Some(ScalarType::Bool),
        Schema::Int => Some(ScalarType::Int32),
        Schema::Long => Some(ScalarType::Int64),
        Schema::Float => Some(ScalarType::Float32),
        Schema::Double => Some(ScalarType::Float64),
        Schema::String | Schema::Enum(_) => Some(ScalarType::String),
        Schema::Bytes | Schema::Fixed(_) => Some(ScalarType::Bytes),
        Schema::Date => Some(ScalarType::Date),
        Schema::TimeMillis | Schema::TimeMicros => Some(ScalarType::Int64),
        Schema::TimestampMillis | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => Some(ScalarType::Timestamp),
        Schema::Uuid => Some(ScalarType::Uuid),
        Schema::Decimal(d) => Some(ScalarType::Decimal {
            precision: d.precision as u8,
            scale: d.scale as u8,
        }),
        Schema::BigDecimal => Some(ScalarType::String),
        // Nested Record, Map → Json blob
        Schema::Record(_) | Schema::Map(_) => Some(ScalarType::Json),
        _ => None,
    }
}

// ═══════════════════════════════════════════════════════════════
//  FFI
// ═══════════════════════════════════════════════════════════════

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
