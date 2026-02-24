use server_api::{
    plugin_ok, plugin_err, parse_plugin_config_opt,
    PluginCreateResult, PluginError, FormatSerializer, DataFormat,
};

use super::parser::{parse_fields, parse_delimiter};

// ═══════════════════════════════════════════════════════════════
//  CsvFormatSerializer
// ═══════════════════════════════════════════════════════════════

struct CsvFormatSerializer {
    delimiter: char,
    quoting: bool,
    /// Порядок колонок для serialize. Если пуст — порядок ключей из Value object.
    columns: Vec<String>,
}

impl CsvFormatSerializer {
    fn csv_to_value(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        let s = std::str::from_utf8(data)?;
        let s = s.trim_end_matches(['\r', '\n']);
        if s.is_empty() {
            return Err(PluginError::format_err("CSV: empty line"));
        }

        let fields = parse_fields(s, self.delimiter, self.quoting);

        let mut map = serde_json::Map::new();
        for (i, value) in fields.iter().enumerate() {
            let col_name = if i < self.columns.len() {
                self.columns[i].clone()
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

    fn value_to_csv(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        let obj = value.as_object()
            .ok_or_else(|| PluginError::format_err("CSV serialize: expected object Value"))?;

        let keys: Vec<&String> = if self.columns.is_empty() {
            obj.keys().collect()
        } else {
            self.columns.iter().collect()
        };

        let mut result = String::new();
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
}

impl FormatSerializer for CsvFormatSerializer {
    fn deserialize(&self, data: &[u8]) -> Result<serde_json::Value, PluginError> {
        self.csv_to_value(data)
    }

    fn serialize(&self, value: &serde_json::Value) -> Result<Vec<u8>, PluginError> {
        self.value_to_csv(value)
    }

    fn format(&self) -> DataFormat {
        DataFormat::Csv
    }
}

// ═══════════════════════════════════════════════════════════════
//  FFI
// ═══════════════════════════════════════════════════════════════

#[derive(serde::Deserialize)]
#[serde(default)]
struct CsvFormatConfig {
    delimiter: String,
    quoting: bool,
    columns: Vec<String>,
}

impl Default for CsvFormatConfig {
    fn default() -> Self {
        Self {
            delimiter: ",".to_string(),
            quoting: true,
            columns: Vec::new(),
        }
    }
}

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_format_serializer(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let cfg: CsvFormatConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    let delimiter = match parse_delimiter(&cfg.delimiter) {
        Ok(d) => d,
        Err(e) => return plugin_err(e.to_string()),
    };

    plugin_ok(Box::new(CsvFormatSerializer {
        delimiter,
        quoting: cfg.quoting,
        columns: cfg.columns,
    }) as Box<dyn FormatSerializer>)
}

server_api::qs_destroy_fn!(qs_destroy_format_serializer, FormatSerializer);
