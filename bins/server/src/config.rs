use clap::{Args, Parser, Subcommand};
use serde::Deserialize;

pub use server_api::OverflowPolicy;
pub use pipeline::config::{
    FormatConfig, TopicConfig, ProcessorConfig, SourceConfig, SinkConfig,
};

#[derive(Parser)]
#[command(name = "gauss-server", about = "Сервис обмена данными")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Запустить сервер
    Serve(ServeArgs),
}

#[derive(Args, Clone, Debug)]
pub struct ServeArgs {
    /// Путь к TOML конфиг файлу
    #[arg(long, default_value = "config.toml", env = "CONFIG_PATH")]
    pub config: String,
}

// ---- TOML Config ----

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_api_port")]
    pub api_port: u16,
    #[serde(default)]
    pub formats: Vec<FormatConfig>,
    #[serde(default)]
    pub topics: Vec<TopicConfig>,
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    /// Размер буфера подписки WS клиентов на topic'ы.
    #[serde(default = "default_ws_buffer")]
    pub ws_buffer: usize,
    /// Стратегия переполнения WS подписок.
    #[serde(default = "default_ws_overflow")]
    pub ws_overflow: OverflowPolicy,
}

fn default_api_port() -> u16 {
    9200
}
fn default_ws_buffer() -> usize {
    4096
}
fn default_ws_overflow() -> OverflowPolicy {
    OverflowPolicy::Drop
}

impl ServerConfig {
    pub fn load(path: &str) -> Result<Self, crate::error::ServerError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::ServerError::Config { context: "read", detail: format!("'{path}': {e}") })?;
        toml::from_str(&content)
            .map_err(|e| crate::error::ServerError::Config { context: "parse", detail: format!("'{path}': {e}") })
    }
}
