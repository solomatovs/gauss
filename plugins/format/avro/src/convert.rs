use apache_avro::types::Value;
use apache_avro::Schema;
use server_api::PluginError;

// ═══════════════════════════════════════════════════════════════
//  Avro → JSON conversion
// ═══════════════════════════════════════════════════════════════

pub(crate) fn avro_to_value(value: &Value) -> serde_json::Value {
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

// ═══════════════════════════════════════════════════════════════
//  JSON → Avro conversion
// ═══════════════════════════════════════════════════════════════

pub(crate) fn value_to_avro(val: &serde_json::Value, schema: &Schema) -> Result<Value, PluginError> {
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

// ═══════════════════════════════════════════════════════════════
//  Base64
// ═══════════════════════════════════════════════════════════════

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
