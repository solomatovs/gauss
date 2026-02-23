use std::sync::OnceLock;

use server_api::{
    plugin_ok, plugin_err, parse_plugin_config_opt,
    PluginCreateResult, PluginError, Codec, FormatSerializer, RecordData, DataFormat,
};

// ---- Column mapping ----

struct ColumnMap {
    names: Vec<String>,
}

impl ColumnMap {
    /// Определить маппинг из строки заголовка.
    fn from_header(fields: &[String]) -> Self {
        let names: Vec<String> = fields.iter().map(|f| f.trim().to_string()).collect();
        Self { names }
    }

    /// Маппинг по умолчанию: col0, col1, col2, ...
    fn default_for(num_fields: usize) -> Self {
        let names: Vec<String> = (0..num_fields).map(|i| format!("col{i}")).collect();
        Self { names }
    }
}

// ---- RFC 4180 field parser ----

/// Разбирает одну RFC 4180 строку на поля с учётом quoting.
fn parse_fields(line: &str, delimiter: char, quoting: bool) -> Vec<String> {
    if !quoting {
        return line.split(delimiter).map(|s| s.to_string()).collect();
    }

    let mut fields = Vec::new();
    let mut chars = line.chars().peekable();
    let mut field = String::new();

    loop {
        if chars.peek() == Some(&'"') {
            // Quoted field (RFC 4180 rule 5-7)
            chars.next(); // consume opening quote
            loop {
                match chars.next() {
                    Some('"') => {
                        if chars.peek() == Some(&'"') {
                            // Escaped quote: "" → "
                            chars.next();
                            field.push('"');
                        } else {
                            // End of quoted field
                            break;
                        }
                    }
                    Some(c) => field.push(c),
                    None => break, // EOF inside quote — best effort
                }
            }
            // Consume until delimiter or end
            loop {
                match chars.peek() {
                    Some(&c) if c == delimiter => {
                        chars.next();
                        break;
                    }
                    Some(_) => { chars.next(); } // skip trailing chars after closing quote
                    None => break,
                }
            }
        } else {
            // Unquoted field
            loop {
                match chars.peek() {
                    Some(&c) if c == delimiter => {
                        chars.next();
                        break;
                    }
                    Some(_) => field.push(chars.next().unwrap()),
                    None => break,
                }
            }
        }

        fields.push(std::mem::take(&mut field));

        if chars.peek().is_none() && !line.ends_with(delimiter) {
            break;
        }
        if chars.peek().is_none() {
            // Trailing delimiter → one more empty field
            fields.push(String::new());
            break;
        }
    }

    fields
}

// ---- CsvCodec ----

pub struct CsvCodec {
    delimiter: char,
    quoting: bool,
    header: HeaderMode,
    /// Маппинг колонок, инициализируется из заголовка или по умолчанию.
    columns: OnceLock<ColumnMap>,
}

#[derive(Default)]
enum HeaderMode {
    #[default]
    Absent,
    Present,
}

impl Codec for CsvCodec {
    fn decode(&self, data: &[u8]) -> Result<(serde_json::Value, RecordData), PluginError> {
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

        // Build Value object from all columns
        let mut map = serde_json::Map::new();
        for (i, value) in fields.iter().enumerate() {
            let col_name = if i < cols.names.len() {
                cols.names[i].clone()
            } else {
                format!("col{i}")
            };

            // Try to parse as number, fallback to string
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

        let value = serde_json::Value::Object(map);
        Ok((value, RecordData::new(data.to_vec(), DataFormat::Csv)))
    }

    fn encode(&self, data: &RecordData) -> Result<Vec<u8>, PluginError> {
        match data.format() {
            DataFormat::Csv => Ok(data.as_bytes().to_vec()),
            other => Err(PluginError::format_err(format!("CSV codec: cannot encode {other:?} format data"))),
        }
    }
}

// ---- FFI ----

#[derive(serde::Deserialize)]
#[serde(default)]
struct CsvPluginConfig {
    /// Разделитель полей (по умолчанию ",").
    delimiter: String,

    /// Обработка двойных кавычек по RFC 4180 (по умолчанию true).
    quoting: bool,

    /// Наличие строки заголовка: "absent" (по умолчанию) или "present".
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

fn parse_delimiter(s: &str) -> Result<char, PluginError> {
    match s {
        "\\t" | "\t" => Ok('\t'),
        "\\n" => Ok('\n'),
        s if s.len() == 1 => Ok(s.chars().next().unwrap()),
        other => Err(PluginError::config(format!(
            "CSV: delimiter must be a single character, got {other:?}"
        ))),
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

// ---- FormatSerializer ----

/// CSV FormatSerializer: bytes ↔ Value.
/// Конфиг columns определяет порядок полей при serialize.
struct CsvFormatSerializer {
    delimiter: char,
    quoting: bool,
    /// Порядок колонок для serialize. Если пуст — порядок ключей из Value object.
    columns: Vec<String>,
}

impl CsvFormatSerializer {
    /// Разобрать CSV строку в Value.
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

    /// Сериализовать Value в CSV строку.
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

#[derive(serde::Deserialize)]
#[serde(default)]
struct CsvFormatConfig {
    delimiter: String,
    quoting: bool,
    /// Порядок колонок для serialize.
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
server_api::qs_abi_version_fn!();
