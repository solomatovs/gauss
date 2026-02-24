use serde::Deserialize;

// ═══════════════════════════════════════════════════════════════
//  TopicRecord — matches the server's API response format
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub(crate) struct TopicRecord {
    pub ts_ms: i64,
    #[allow(dead_code)]
    pub key: String,
    /// Structured data value (serde_json::Value).
    pub value: serde_json::Value,
}

// ═══════════════════════════════════════════════════════════════
//  Query model — format-aware with per-field configuration
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    #[default]
    Json,
    Csv,
    Protobuf,
    Avro,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionMode {
    #[default]
    Http,
    Tcp,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FieldValueType {
    #[default]
    Number,
    String,
    Boolean,
    Time,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldConfig {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub r#type: FieldValueType,
}

impl FieldConfig {
    /// Display name: alias if set, otherwise path.
    pub fn display_name(&self) -> &str {
        if self.alias.is_empty() {
            &self.path
        } else {
            &self.alias
        }
    }
}

/// The query model sent from Grafana frontend.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotesQuery {
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub format: DataFormat,
    #[serde(default)]
    pub connection_mode: ConnectionMode,
    #[serde(default)]
    pub field_configs: Vec<FieldConfig>,
    /// @deprecated — legacy comma-separated field names.
    #[serde(default)]
    pub fields: Option<String>,
}

// ═══════════════════════════════════════════════════════════════
//  StreamQuery — saved query for a streaming channel
// ═══════════════════════════════════════════════════════════════

#[derive(Clone, Debug)]
pub(crate) struct StreamQuery {
    pub base_url: String,
    pub topic: String,
    pub key: String,
    pub field_configs: Vec<FieldConfig>,
}

// ═══════════════════════════════════════════════════════════════
//  Field config resolution (with legacy migration)
// ═══════════════════════════════════════════════════════════════

pub(crate) fn resolve_field_configs(query: &QuotesQuery) -> Vec<FieldConfig> {
    if !query.field_configs.is_empty() {
        return query.field_configs.clone();
    }
    // Legacy migration
    if let Some(ref fields) = query.fields {
        return fields
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| FieldConfig {
                path: s.to_string(),
                alias: String::new(),
                r#type: FieldValueType::Number,
            })
            .collect();
    }
    Vec::new()
}
