use std::path::Path;

use serde::Deserialize;
use serde_json::Value;

use crate::error::EngineError;

// ---------------------------------------------------------------------------
// Config parser abstraction
// ---------------------------------------------------------------------------

/// Trait for configuration format parsers.
///
/// Each format (HCL, TOML, YAML, ...) implements this trait
/// in its own crate (e.g. `gauss-config-hcl`).
pub trait ConfigParser: Send + Sync {
    /// File extensions this parser handles (without dot), e.g. `["toml"]`.
    fn extensions(&self) -> &[&str];

    /// Parse a configuration string into `GaussConfig`.
    fn parse(&self, content: &str) -> Result<GaussConfig, EngineError>;
}

/// Registry of config parsers — resolves file extension to the right parser.
pub struct ConfigRegistry {
    parsers: Vec<Box<dyn ConfigParser>>,
}

impl ConfigRegistry {
    pub fn new() -> Self {
        Self {
            parsers: Vec::new(),
        }
    }

    /// Register a config parser.
    pub fn register(mut self, parser: impl ConfigParser + 'static) -> Self {
        self.parsers.push(Box::new(parser));
        self
    }

    /// Load configuration from a file, selecting parser by extension.
    pub fn load(&self, path: &str) -> Result<GaussConfig, EngineError> {
        let ext = Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        let parser = self
            .parsers
            .iter()
            .find(|p| p.extensions().contains(&ext))
            .ok_or_else(|| {
                let supported: Vec<&str> = self
                    .parsers
                    .iter()
                    .flat_map(|p| p.extensions().iter().copied())
                    .collect();
                EngineError::Config(format!(
                    "{path}: unsupported config format '.{ext}' (supported: {})",
                    supported.join(", ")
                ))
            })?;

        let content = std::fs::read_to_string(path)
            .map_err(|e| EngineError::Config(format!("{path}: {e}")))?;

        parser.parse(&content)
    }
}

// ---------------------------------------------------------------------------
// Configuration structs
// ---------------------------------------------------------------------------

/// Root configuration.
///
/// Format-independent: deserialized from HCL (or other formats)
/// by the appropriate `gauss-config-*` crate.
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
    pub config: Option<Value>,
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
    pub storage_config: Option<Value>,
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
    pub config: Option<Value>,
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
