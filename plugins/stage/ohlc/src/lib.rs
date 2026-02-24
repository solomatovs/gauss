use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

use server_api::{
    parse_plugin_config_opt, plugin_err, plugin_ok,
    TopicContext, TopicRecord, TopicProcessor,
    PluginCreateResult, now_ms, PluginError,
};

// ═══════════════════════════════════════════════════════════════
//  Local domain types (previously in core API)
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
struct Quote {
    symbol: String,
    bid: f64,
    #[allow(dead_code)]
    ask: f64,
    ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct Candle {
    symbol: String,
    tf: String,
    ts_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
}

// ═══════════════════════════════════════════════════════════════
//  Timeframes
// ═══════════════════════════════════════════════════════════════

const ALL_TIMEFRAMES: &[(&str, i64)] = &[
    ("1s", 1_000),
    ("5s", 5_000),
    ("15s", 15_000),
    ("30s", 30_000),
    ("1m", 60_000),
    ("5m", 300_000),
    ("15m", 900_000),
    ("30m", 1_800_000),
    ("1h", 3_600_000),
    ("4h", 14_400_000),
    ("1d", 86_400_000),
    ("1w", 604_800_000),
    ("1y", 31_536_000_000),
];

fn resolve_timeframes(names: &[String]) -> Result<Vec<(&'static str, i64)>, PluginError> {
    let mut result = Vec::new();
    for name in names {
        let name = name.trim();
        match ALL_TIMEFRAMES.iter().find(|(n, _)| *n == name) {
            Some(&tf) => result.push(tf),
            None => {
                let known: Vec<&str> = ALL_TIMEFRAMES.iter().map(|(n, _)| *n).collect();
                return Err(PluginError::config(format!(
                    "неизвестный таймфрейм: '{name}'. Допустимые: {}",
                    known.join(",")
                )));
            }
        }
    }
    if result.is_empty() {
        return Err(PluginError::config("список таймфреймов пуст"));
    }
    Ok(result)
}

// ═══════════════════════════════════════════════════════════════
//  Plugin config
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
struct OhlcConfig {
    #[serde(default = "default_timeframes")]
    timeframes: Vec<String>,
}

impl Default for OhlcConfig {
    fn default() -> Self {
        Self {
            timeframes: default_timeframes(),
        }
    }
}

fn default_timeframes() -> Vec<String> {
    vec![
        "1s", "1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1y",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

// ═══════════════════════════════════════════════════════════════
//  OhlcEngine — in-memory OHLC state machine
// ═══════════════════════════════════════════════════════════════

/// In-memory OHLC state machine.
/// Хранит текущую свечу для каждой пары (symbol, tf).
struct OhlcEngine {
    timeframes: Vec<(&'static str, i64)>,
    candles: HashMap<(String, String), Candle>,
}

impl OhlcEngine {
    fn new(timeframes: Vec<(&'static str, i64)>) -> Self {
        Self {
            timeframes,
            candles: HashMap::new(),
        }
    }

    /// Обработать тик. Возвращает обновлённые свечи по всем таймфреймам.
    fn process_tick(&mut self, quote: &Quote) -> Vec<Candle> {
        let ts_ms = quote.ts_ms.unwrap_or_else(now_ms);
        let bid = quote.bid;

        let mut updated = Vec::with_capacity(self.timeframes.len());

        for &(tf_name, interval_ms) in &self.timeframes {
            let window_start = ts_ms - (ts_ms % interval_ms);
            let key = (quote.symbol.clone(), tf_name.to_string());

            let candle = self.candles.entry(key).or_insert_with(|| Candle {
                symbol: quote.symbol.clone(),
                tf: tf_name.to_string(),
                ts_ms: window_start,
                open: bid,
                high: bid,
                low: bid,
                close: bid,
                volume: 0,
            });

            if candle.ts_ms != window_start {
                // Окно сменилось — начать новую свечу
                candle.ts_ms = window_start;
                candle.open = bid;
                candle.high = bid;
                candle.low = bid;
                candle.close = bid;
                candle.volume = 1;
            } else {
                // Обновить текущую свечу
                if bid > candle.high {
                    candle.high = bid;
                }
                if bid < candle.low {
                    candle.low = bid;
                }
                candle.close = bid;
                candle.volume += 1;
            }

            updated.push(candle.clone());
        }

        updated
    }
}

// ═══════════════════════════════════════════════════════════════
//  OhlcProcessor — TopicProcessor impl
// ═══════════════════════════════════════════════════════════════

/// TopicProcessor: получает котировки из trigger'а, вычисляет OHLC свечи,
/// публикует обновлённые свечи в topic'и ohlc.{tf} через TopicPublisher.
struct OhlcProcessor {
    engine: Mutex<OhlcEngine>,
}

impl OhlcProcessor {
    fn new(timeframes: Vec<(&'static str, i64)>) -> Self {
        Self {
            engine: Mutex::new(OhlcEngine::new(timeframes)),
        }
    }
}

impl TopicProcessor for OhlcProcessor {
    fn process<'a>(
        &'a self,
        ctx: TopicContext<'a>,
        source_topic: &'a str,
        record: &'a TopicRecord,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send + 'a>> {
        Box::pin(async move {
            // Value уже в record — десериализация тривиальна
            let json_value = ctx.codec.deserialize_data(source_topic, record)?;
            let quote: Quote = serde_json::from_value(json_value)
                .map_err(|e| PluginError::format_err(format!("OHLC: failed to parse Quote from record: {e}")))?;
            let candles = self.engine.lock().await.process_tick(&quote);

            for candle in candles {
                let topic = format!("ohlc.{}", candle.tf);
                let candle_value = serde_json::to_value(&candle)
                    .map_err(|e| PluginError::format_err(format!("OHLC: failed to serialize Candle: {e}")))?;

                // serialize_data теперь возвращает TopicRecord
                let candle_record = ctx.codec.serialize_data(&topic, &candle_value)?;
                let candle_record = TopicRecord {
                    ts_ms: candle.ts_ms,
                    key: candle.symbol.clone(),
                    ..candle_record
                };
                ctx.publisher.publish(&topic, candle_record).await?;
            }

            Ok(())
        })
    }
}

// ═══════════════════════════════════════════════════════════════
//  FFI entry points
// ═══════════════════════════════════════════════════════════════

/// # Safety
/// `config_json_ptr` must point to `config_json_len` valid UTF-8 bytes (or be null).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn qs_create_processor(
    config_json_ptr: *const u8,
    config_json_len: usize,
) -> PluginCreateResult {
    let config: OhlcConfig =
        match unsafe { parse_plugin_config_opt(config_json_ptr, config_json_len) } {
            Ok(c) => c,
            Err(e) => return plugin_err(e.to_string()),
        };

    let timeframes = match resolve_timeframes(&config.timeframes) {
        Ok(tf) => tf,
        Err(e) => return plugin_err(e.to_string()),
    };

    plugin_ok(Box::new(OhlcProcessor::new(timeframes)) as Box<dyn TopicProcessor>)
}

server_api::qs_destroy_fn!(qs_destroy_processor, TopicProcessor);
server_api::qs_abi_version_fn!();
