use std::sync::OnceLock;

use server_api::{
    plugin_ok, plugin_err, parse_plugin_config_opt,
    PluginCreateResult, PluginError, Codec, DataFormat,
};

use super::parser::{ColumnMap, parse_fields, parse_delimiter};

// ═══════════════════════════════════════════════════════════════
//  CsvCodec
// ═══════════════════════════════════════════════════════════════

pub struct CsvCodec {
    delimiter: char,
    quoting: bool,
    header: HeaderMode,
    columns: OnceLock<ColumnMap>,
}

#[derive(Default)]
enum HeaderMode {
    #[default]
    Absent,
    Present,
}

impl Codec for CsvCodec {
    fn decode(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        let s = std::str::from_utf8(data)?;
        let s = s.trim_end_matches(['\r', '\n']);
        if s.is_empty() {
            return Err(PluginError::format_err("CSV: empty line"));
        }

        let fields = parse_fields(s, self.delimiter, self.quoting);

        // Обработка заголовка (RFC 4180: optional header line)
        if matches!(self.header, HeaderMode::Present)
            && self.columns.get().is_none() {
                let map = ColumnMap::from_header(&fields);
                let _ = self.columns.set(map);
                return Err(PluginError::format_err("CSV: header line parsed, skipping"));
            }

        let cols = self.columns.get_or_init(|| {
            ColumnMap::default_for(fields.len())
        });

        let mut map = serde_json::Map::new();
        for (i, value) in fields.iter().enumerate() {
            let col_name = if i < cols.names.len() {
                cols.names[i].clone()
            } else {
                format!("col{i}")
            };

            if let Ok(n) = value.parse::<i64>() {
                map.insert(col_name, serde_json::Value::Number(n.into()));
            } else if let Ok(n) = value.parse::<f64>() {
                if let Some(num) = serde_json::Number::from_f64(n) {
                    map.insert(col_name, serde_json::Value::Number(num));
                } else {
                    map.insert(col_name, serde_json::Value::String(value.clone()));
                }
            } else {
                map.insert(col_name, serde_json::Value::String(value.clone()));
            }
        }

        Ok(serde_json::Value::Object(map))
    }

    fn encode(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        let obj = value.as_object()
            .ok_or_else(|| PluginError::format_err("CSV codec encode: expected object Value"))?;

        let cols = self.columns.get();

        let mut result = String::new();
        let keys: Vec<&String> = if let Some(c) = cols {
            c.names.iter().collect()
        } else {
            obj.keys().collect()
        };

        for (i, key) in keys.iter().enumerate() {
            if i > 0 {
                result.push(self.delimiter);
            }
            if let Some(val) = obj.get(*key) {
                match val {
                    serde_json::Value::String(s) => {
                        if self.quoting && (s.contains(self.delimiter) || s.contains('"') || s.contains('\n')) {
                            result.push('"');
                            result.push_str(&s.replace('"', "\"\""));
                            result.push('"');
                        } else {
                            result.push_str(s);
                        }
                    }
                    serde_json::Value::Null => {}
                    other => {
                        result.push_str(&other.to_string());
                    }
                }
            }
        }

        Ok(result.into_bytes())
    }

    fn data_format(&self) -> DataFormat {
        DataFormat::Csv
    }
}

// ═══════════════════════════════════════════════════════════════
//  FFI
// ═══════════════════════════════════════════════════════════════

#[derive(serde::Deserialize)]
#[serde(default)]
struct CsvPluginConfig {
    delimiter: String,
    quoting: bool,
    header: String,
}

impl Default for CsvPluginConfig {
    fn default() -> Self {
        Self {
            delimiter: ",".to_string(),
            quoting: true,
            header: String::new(),
        }
    }
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_codec(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: CsvPluginConfig = match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
        Ok(c) => c,
        Err(e) => return plugin_err(e.to_string()),
    };

    let delimiter = match parse_delimiter(&cfg.delimiter) {
        Ok(d) => d,
        Err(e) => return plugin_err(e.to_string()),
    };

    let header = match cfg.header.as_str() {
        "" | "absent" => HeaderMode::Absent,
        "present" => HeaderMode::Present,
        other => {
            return plugin_err(format!(
                "CSV: header must be \"absent\" or \"present\", got {other:?}"
            ));
        }
    };

    plugin_ok(Box::new(CsvCodec {
        delimiter,
        quoting: cfg.quoting,
        header,
        columns: OnceLock::new(),
    }) as Box<dyn Codec>)
}

server_api::qs_destroy_fn!(qs_destroy_codec, Codec);
