use serde::Deserialize;

use crate::error::EngineError;

/// Root configuration — parsed from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct GaussConfig {
    /// HTTP API port.
    #[serde(default = "default_api_port")]
    pub api_port: u16,

    /// Format plugin definitions.
    #[serde(default)]
    pub formats: Vec<FormatConfig>,

    /// Converter plugin definitions.
    #[serde(default)]
    pub converters: Vec<ConverterConfig>,

    /// Schema mapping definitions (Rhai scripts).
    #[serde(default)]
    pub schema_maps: Vec<SchemaMapConfig>,

    /// Topic definitions.
    #[serde(default)]
    pub topics: Vec<TopicConfig>,

    /// Processor definitions.
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
}

fn default_api_port() -> u16 {
    9200
}

#[derive(Debug, Clone, Deserialize)]
pub struct FormatConfig {
    pub name: String,
    pub plugin: String,
    #[serde(default)]
    pub config: Option<toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConverterConfig {
    pub name: String,
    pub plugin: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchemaMapConfig {
    pub name: String,
    pub script: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    /// Path to storage .so plugin.
    pub storage: String,
    #[serde(default)]
    pub storage_config: Option<toml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessorConfig {
    pub name: String,
    /// Path to processor .so plugin.
    pub plugin: String,
    #[serde(default)]
    pub source: Option<ProcessorSourceConfig>,
    #[serde(default)]
    pub target: Option<ProcessorTargetConfig>,
    #[serde(default)]
    pub config: Option<toml::Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProcessorSourceConfig {
    pub topic: String,
    pub read: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProcessorTargetConfig {
    pub topic: String,
}

impl GaussConfig {
    /// Load configuration from a TOML file.
    pub fn load(path: &str) -> Result<Self, EngineError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| EngineError::Config(format!("{path}: {e}")))?;
        Self::parse(&content)
    }

    /// Parse configuration from a TOML string.
    pub fn parse(toml_str: &str) -> Result<Self, EngineError> {
        toml::from_str(toml_str).map_err(|e| EngineError::Config(e.to_string()))
    }
}
