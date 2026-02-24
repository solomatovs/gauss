use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use grafana_plugin_sdk::data::Frame;
use grafana_plugin_sdk::prelude::*;

use super::query::{FieldConfig, FieldValueType, TopicRecord};

// ═══════════════════════════════════════════════════════════════
//  Frame builders
// ═══════════════════════════════════════════════════════════════

/// Build a Grafana Frame from TopicRecord structs (HTTP API response).
pub(crate) fn build_frame_from_topic_records(
    topic: &str,
    key: &str,
    records: &[TopicRecord],
    field_configs: &[FieldConfig],
) -> Frame {
    let len = records.len();
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(len);

    let mut num_cols: Vec<(usize, String, Vec<f64>)> = Vec::new();
    let mut str_cols: Vec<(usize, String, Vec<String>)> = Vec::new();

    for (i, fc) in field_configs.iter().enumerate() {
        let name = fc.display_name().to_string();
        match fc.r#type {
            FieldValueType::Number | FieldValueType::Time => {
                num_cols.push((i, name, Vec::with_capacity(len)));
            }
            FieldValueType::String | FieldValueType::Boolean => {
                str_cols.push((i, name, Vec::with_capacity(len)));
            }
        }
    }

    for record in records {
        let dt = match DateTime::from_timestamp_millis(record.ts_ms) {
            Some(dt) => dt,
            None => continue,
        };
        timestamps.push(dt);

        let data_value = &record.value;
        let full = serde_json::json!({
            "ts_ms": record.ts_ms,
            "key": &record.key,
            "value": data_value,
        });

        for (idx, _, col) in &mut num_cols {
            let fc = &field_configs[*idx];
            col.push(
                extract_number(&full, &fc.path)
                    .or_else(|| extract_number(data_value, &fc.path))
                    .unwrap_or(f64::NAN),
            );
        }

        for (idx, _, col) in &mut str_cols {
            let fc = &field_configs[*idx];
            let val = match fc.r#type {
                FieldValueType::Boolean => extract_bool(&full, &fc.path)
                    .or_else(|| extract_bool(data_value, &fc.path))
                    .map(|b| b.to_string())
                    .unwrap_or_default(),
                _ => extract_string(&full, &fc.path)
                    .or_else(|| extract_string(data_value, &fc.path))
                    .unwrap_or_default(),
            };
            col.push(val);
        }
    }

    let frame_name = if key.is_empty() {
        topic.to_string()
    } else {
        format!("{key} {topic}")
    };

    let mut frame = Frame::new(frame_name).with_field(timestamps.into_field("time"));

    for (_, name, col) in num_cols {
        frame = frame.with_field(col.into_field(name));
    }

    for (_, name, col) in str_cols {
        frame = frame.with_field(col.into_field(name));
    }

    frame
}

/// Build a Grafana Frame from WebSocket JSON records.
pub(crate) fn build_frame_from_ws_records(
    topic: &str,
    key: &str,
    records: &[serde_json::Value],
    field_configs: &[FieldConfig],
) -> Result<Frame, String> {
    let len = records.len();
    let mut timestamps: Vec<DateTime<Utc>> = Vec::with_capacity(len);

    let mut num_cols: Vec<(usize, String, Vec<f64>)> = Vec::new();
    let mut str_cols: Vec<(usize, String, Vec<String>)> = Vec::new();

    for (i, fc) in field_configs.iter().enumerate() {
        let name = fc.display_name().to_string();
        match fc.r#type {
            FieldValueType::Number | FieldValueType::Time => {
                num_cols.push((i, name, Vec::with_capacity(len)));
            }
            FieldValueType::String | FieldValueType::Boolean => {
                str_cols.push((i, name, Vec::with_capacity(len)));
            }
        }
    }

    for record in records {
        let ts_ms = record
            .get("ts_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let dt = match DateTime::from_timestamp_millis(ts_ms) {
            Some(dt) => dt,
            None => continue,
        };
        timestamps.push(dt);

        let data = record.get("value").unwrap_or(record);

        for (idx, _, col) in &mut num_cols {
            let fc = &field_configs[*idx];
            col.push(
                extract_number(record, &fc.path)
                    .or_else(|| extract_number(data, &fc.path))
                    .unwrap_or(f64::NAN),
            );
        }

        for (idx, _, col) in &mut str_cols {
            let fc = &field_configs[*idx];
            let val = match fc.r#type {
                FieldValueType::Boolean => extract_bool(record, &fc.path)
                    .or_else(|| extract_bool(data, &fc.path))
                    .map(|b| b.to_string())
                    .unwrap_or_default(),
                _ => extract_string(record, &fc.path)
                    .or_else(|| extract_string(data, &fc.path))
                    .unwrap_or_default(),
            };
            col.push(val);
        }
    }

    let frame_name = if key.is_empty() {
        topic.to_string()
    } else {
        format!("{key} {topic}")
    };

    let mut frame = Frame::new(frame_name).with_field(timestamps.into_field("time"));

    for (_, name, col) in num_cols {
        frame = frame.with_field(col.into_field(name));
    }

    for (_, name, col) in str_cols {
        frame = frame.with_field(col.into_field(name));
    }

    Ok(frame)
}

// ═══════════════════════════════════════════════════════════════
//  Channel path — deterministic hash of query parameters
// ═══════════════════════════════════════════════════════════════

pub(crate) fn channel_path(topic: &str, key: &str, field_configs: &[FieldConfig]) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    topic.hash(&mut h);
    key.hash(&mut h);
    for fc in field_configs {
        fc.path.hash(&mut h);
        fc.alias.hash(&mut h);
        let type_tag = match fc.r#type {
            FieldValueType::Number => "n",
            FieldValueType::String => "s",
            FieldValueType::Boolean => "b",
            FieldValueType::Time => "t",
        };
        type_tag.hash(&mut h);
    }
    format!("{:016x}", h.finish())
}

// ═══════════════════════════════════════════════════════════════
//  JSON helpers — flatten & dot-path extraction
// ═══════════════════════════════════════════════════════════════

/// Flatten a JSON value into dot-notation paths with inferred types.
pub(crate) fn flatten_json_paths(value: &serde_json::Value, prefix: &str) -> Vec<serde_json::Value> {
    let mut result = Vec::new();
    if let serde_json::Value::Object(map) = value {
        for (key, val) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match val {
                serde_json::Value::Object(_) => {
                    result.extend(flatten_json_paths(val, &path));
                }
                serde_json::Value::Number(_) => {
                    result.push(serde_json::json!({"path": path, "type": "number"}));
                }
                serde_json::Value::String(_) => {
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                }
                serde_json::Value::Bool(_) => {
                    result.push(serde_json::json!({"path": path, "type": "boolean"}));
                }
                serde_json::Value::Array(arr) => {
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                    if let Some(first) = arr.first() {
                        if first.is_object() {
                            result.extend(flatten_json_paths(first, &format!("{path}.0")));
                        }
                    }
                }
                serde_json::Value::Null => {
                    result.push(serde_json::json!({"path": path, "type": "string"}));
                }
            }
        }
    }
    result
}

/// Resolve a dot-notation path against a JSON value.
fn resolve_path<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for segment in path.split('.') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        match current.get(segment) {
            Some(next) => current = next,
            None => {
                if let Ok(idx) = segment.parse::<usize>() {
                    current = current.get(idx)?;
                } else {
                    return None;
                }
            }
        }
    }
    Some(current)
}

fn extract_number(data: &serde_json::Value, path: &str) -> Option<f64> {
    let v = resolve_path(data, path)?;
    v.as_f64()
        .or_else(|| v.as_i64().map(|i| i as f64))
        .or_else(|| v.as_u64().map(|u| u as f64))
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn extract_string(data: &serde_json::Value, path: &str) -> Option<String> {
    let v = resolve_path(data, path)?;
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn extract_bool(data: &serde_json::Value, path: &str) -> Option<bool> {
    let v = resolve_path(data, path)?;
    v.as_bool()
        .or_else(|| v.as_str().and_then(|s| s.parse::<bool>().ok()))
}
