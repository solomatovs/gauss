use clap::Args;
use serde::Deserialize;

use pipeline::config::PluginRef;

use super::error::QuotesGenError;

// ═══════════════════════════════════════════════════════════════
//  Config file (TOML)
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    pub symbol: Option<String>,
    pub rate: Option<f64>,
    pub history: Option<bool>,
    pub file: Option<String>,
    pub generate: Option<bool>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub interval: Option<u64>,
    pub price: Option<f64>,
    pub seed: Option<i64>,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(default = "default_sink_name")]
    pub name: String,
    pub transport: String,
    pub transport_config: Option<toml::Value>,
    pub framing: String,
    pub framing_config: Option<toml::Value>,
    #[serde(default)]
    pub middleware: Vec<PluginRef>,
    pub codec: String,
    pub codec_config: Option<toml::Value>,
}

fn default_sink_name() -> String {
    "unnamed".into()
}

pub fn load_config(path: &str) -> Result<Config, QuotesGenError> {
    let content =
        std::fs::read_to_string(path).map_err(|e| QuotesGenError::Config(format!("cannot read config {path}: {e}")))?;
    toml::from_str(&content).map_err(|e| QuotesGenError::Config(format!("bad config {path}: {e}")))
}

// ═══════════════════════════════════════════════════════════════
//  CLI args
// ═══════════════════════════════════════════════════════════════

#[derive(Args, Clone, Debug)]
pub struct GenArgs {
    /// Путь к config.toml
    #[arg(long, default_value = "config.toml", env = "QUOTES_GEN_CONFIG")]
    pub config: String,

    /// Фильтр по символу (напр. EURUSD). Без указания — все 9 символов
    #[arg(long)]
    pub symbol: Option<String>,

    /// Сообщений в секунду (0 = интерактивный режим)
    #[arg(long)]
    pub rate: Option<f64>,

    /// Добавить метку времени ts_ms
    #[arg(long)]
    pub history: bool,

    /// Файл для отправки (CSV/пробелы)
    #[arg(long)]
    pub file: Option<String>,

    /// Генерация исторических данных (в stdout + sinks)
    #[arg(long)]
    pub generate: bool,

    /// Начало периода (UTC), напр. "2026-02-15 20:00:00"
    #[arg(long)]
    pub from: Option<String>,

    /// Конец периода (UTC), напр. "2026-02-15 21:00:00"
    #[arg(long)]
    pub to: Option<String>,

    /// Интервал между тиками в мс для --generate
    #[arg(long)]
    pub interval: Option<u64>,

    /// Начальная bid-цена (0 = по умолчанию для символа)
    #[arg(long)]
    pub price: Option<f64>,

    /// Seed для PRNG (0 = текущее время)
    #[arg(long)]
    pub seed: Option<i64>,
}

// ═══════════════════════════════════════════════════════════════
//  Effective — merged config
// ═══════════════════════════════════════════════════════════════

/// Итоговая конфигурация после мержа: config.toml < env/CLI
pub struct Effective {
    pub symbol: Option<String>,
    pub rate: f64,
    pub history: bool,
    pub file: Option<String>,
    pub generate: bool,
    pub from: Option<String>,
    pub to: Option<String>,
    pub interval: u64,
    pub price: f64,
    pub seed: i64,
    pub sinks: Vec<SinkConfig>,
}

impl Effective {
    pub fn new(args: &GenArgs) -> Result<Self, QuotesGenError> {
        let cfg = match load_config(&args.config) {
            Ok(c) => c,
            Err(e) => {
                if std::path::Path::new(&args.config).exists() {
                    return Err(e);
                }
                Config::default()
            }
        };

        if cfg.sinks.is_empty() {
            return Err(QuotesGenError::Config("no [[sinks]] configured in config".into()));
        }

        Ok(Self {
            symbol: args.symbol.clone().or(cfg.symbol),
            rate: args.rate.or(cfg.rate).unwrap_or(0.0),
            history: args.history || cfg.history.unwrap_or(false),
            file: args.file.clone().or(cfg.file),
            generate: args.generate || cfg.generate.unwrap_or(false),
            from: args.from.clone().or(cfg.from),
            to: args.to.clone().or(cfg.to),
            interval: args.interval.or(cfg.interval).unwrap_or(1000),
            price: args.price.or(cfg.price).unwrap_or(0.0),
            seed: args.seed.or(cfg.seed).unwrap_or(0),
            sinks: cfg.sinks,
        })
    }
}
