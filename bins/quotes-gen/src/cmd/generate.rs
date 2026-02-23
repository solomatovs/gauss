use std::io::Write;

use clap::Args;
use serde::{Deserialize, Serialize};
use server_api::{PluginError, TopicRecord, RecordData, now_ms};

use pipeline::Endpoint;
use pipeline::config::PluginRef;

// ═══════════════════════════════════════════════════════════════
//  QuotesGenError — CLI-specific error type
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, thiserror::Error)]
pub enum QuotesGenError {
    #[error("{0}")]
    Config(String),

    #[error("{0}")]
    Plugin(#[from] PluginError),

    #[error("{0}")]
    Pipeline(#[from] pipeline::PipelineError),
}

// ═══════════════════════════════════════════════════════════════
//  Local Quote type (for generation only)
// ═══════════════════════════════════════════════════════════════

#[derive(Debug, Serialize, Deserialize)]
struct Quote {
    symbol: String,
    bid: f64,
    ask: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    ts_ms: Option<i64>,
}

fn quote_to_record(quote: &Quote) -> Result<TopicRecord, PluginError> {
    let data = RecordData::from_json(&serde_json::to_value(quote)?)?;
    Ok(TopicRecord {
        ts_ms: quote.ts_ms.unwrap_or_else(now_ms),
        key: quote.symbol.clone(),
        data,
    })
}

// ═══════════════════════════════════════════════════════════════
//  Sink pipeline (Endpoint wrapper with connection management)
// ═══════════════════════════════════════════════════════════════

pub struct SinkPipeline {
    pub endpoint: Endpoint,
    pub stream: Option<Box<dyn server_api::TransportStream>>,
    pub buf: Vec<u8>,
}

impl SinkPipeline {
    fn name(&self) -> &str {
        &self.endpoint.name
    }

    fn ensure_connected(&mut self) -> Result<(), PluginError> {
        if self.stream.is_none() {
            let stream = self
                .endpoint
                .transport
                .next_connection()?
                .ok_or_else(|| PluginError::io(format!("[{}] transport returned None", self.endpoint.name)))?;
            self.stream = Some(stream);
        }
        Ok(())
    }

    fn send_quote(&mut self, quote: &Quote) -> Result<(), PluginError> {
        let record = quote_to_record(quote)?;
        self.buf.clear();
        self.endpoint.encode_to_wire(&record, &mut self.buf)?;
        self.ensure_connected()?;
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.buf)?;
        Ok(())
    }

    fn send_raw(&mut self, raw: &[u8]) -> Result<(), PluginError> {
        self.buf.clear();
        self.endpoint.framing.encode(raw, &mut self.buf)?;
        self.ensure_connected()?;
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.buf)?;
        Ok(())
    }

    fn send_quote_reconnect(&mut self, quote: &Quote) -> Result<(), PluginError> {
        match self.send_quote(quote) {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(sink = %self.endpoint.name, error = %e, "send error, reconnecting");
                self.stream = None;
                self.send_quote(quote)
            }
        }
    }

    fn send_raw_reconnect(&mut self, raw: &[u8]) -> Result<(), PluginError> {
        match self.send_raw(raw) {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(sink = %self.endpoint.name, error = %e, "send error, reconnecting");
                self.stream = None;
                self.send_raw(raw)
            }
        }
    }
}

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

// ═══════════════════════════════════════════════════════════════
//  Symbol
// ═══════════════════════════════════════════════════════════════

struct Symbol {
    name: &'static str,
    price: f64,
    spread: f64,
    step: f64,
}

impl Symbol {
    fn tick(&mut self, rng: &mut Rng) {
        let delta = (rng.next_f64() * 2.0 - 1.0) * self.step;
        self.price += delta;
        if self.price < self.step {
            self.price = self.step;
        }
    }

    fn to_quote(&self, history: bool) -> Quote {
        Quote {
            symbol: self.name.to_string(),
            bid: self.price,
            ask: self.price + self.spread,
            ts_ms: if history { Some(now_ms()) } else { None },
        }
    }

    fn to_quote_ts(&self, ts_ms: i64) -> Quote {
        Quote {
            symbol: self.name.to_string(),
            bid: self.price,
            ask: self.price + self.spread,
            ts_ms: Some(ts_ms),
        }
    }
}

fn new_symbols() -> Vec<Symbol> {
    vec![
        Symbol { name: "XAUUSD", price: 2650.00, spread: 0.70, step: 2.00 },
        Symbol { name: "XAGUSD", price: 31.50, spread: 0.03, step: 0.05 },
        Symbol { name: "EURUSD", price: 1.08500, spread: 0.00020, step: 0.00050 },
        Symbol { name: "GBPUSD", price: 1.26500, spread: 0.00020, step: 0.00050 },
        Symbol { name: "USDJPY", price: 150.000, spread: 0.020, step: 0.050 },
        Symbol { name: "AUDUSD", price: 0.65500, spread: 0.00020, step: 0.00050 },
        Symbol { name: "USDCHF", price: 0.88000, spread: 0.00020, step: 0.00050 },
        Symbol { name: "USDCAD", price: 1.36000, spread: 0.00020, step: 0.00050 },
        Symbol { name: "NZDUSD", price: 0.61500, spread: 0.00020, step: 0.00050 },
    ]
}

// ═══════════════════════════════════════════════════════════════
//  RNG (xorshift64)
// ═══════════════════════════════════════════════════════════════

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: i64) -> Self {
        let state = if seed == 0 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
                | 1 // ensure non-zero
        } else {
            seed as u64
        };
        Self { state }
    }

    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }

    /// Returns f64 in [0, 1)
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    fn next_intn(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ═══════════════════════════════════════════════════════════════
//  File reader
// ═══════════════════════════════════════════════════════════════

struct FileReader {
    lines: Vec<String>,
    pos: usize,
    path: String,
}

impl FileReader {
    fn open(path: &str) -> Result<Self, QuotesGenError> {
        let content =
            std::fs::read_to_string(path).map_err(|e| QuotesGenError::Config(format!("cannot open {path}: {e}")))?;
        let lines: Vec<String> = content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .map(|l| {
                let l = l.replace([',', ';'], " ");
                l.split_whitespace().collect::<Vec<_>>().join(" ")
            })
            .collect();
        if lines.is_empty() {
            return Err(QuotesGenError::Config(format!("file is empty: {path}")));
        }
        Ok(Self {
            lines,
            pos: 0,
            path: path.to_string(),
        })
    }

    fn total(&self) -> usize {
        self.lines.len()
    }
    fn remaining(&self) -> usize {
        self.lines.len() - self.pos
    }
    fn done(&self) -> bool {
        self.pos >= self.lines.len()
    }

    fn next_line(&mut self) -> Option<&str> {
        if self.done() {
            return None;
        }
        let line = &self.lines[self.pos];
        self.pos += 1;
        Some(line)
    }

    fn next_n(&mut self, n: usize) -> &[String] {
        if self.done() {
            return &[];
        }
        let end = (self.pos + n).min(self.lines.len());
        let slice = &self.lines[self.pos..end];
        self.pos = end;
        slice
    }
}

// ═══════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════
//  Time parsing (multiple formats)
// ═══════════════════════════════════════════════════════════════

fn parse_time(s: &str) -> Result<i64, QuotesGenError> {
    let s = s.trim();
    let (date_part, time_part) = if s.len() >= 11 && (s.as_bytes()[10] == b'T' || s.as_bytes()[10] == b' ')
    {
        (&s[..10], Some(&s[11..]))
    } else {
        (s, None)
    };

    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return Err(QuotesGenError::Config(format!("bad date: {s}")));
    }
    let year: i64 = parts[0].parse().map_err(|_| QuotesGenError::Config(format!("bad year: {s}")))?;
    let month: u32 = parts[1].parse().map_err(|_| QuotesGenError::Config(format!("bad month: {s}")))?;
    let day: u32 = parts[2].parse().map_err(|_| QuotesGenError::Config(format!("bad day: {s}")))?;

    let (hour, min, sec) = if let Some(t) = time_part {
        let tp: Vec<&str> = t.split(':').collect();
        let h: u32 = tp.first().and_then(|v| v.parse().ok()).unwrap_or(0);
        let m: u32 = tp.get(1).and_then(|v| v.parse().ok()).unwrap_or(0);
        let sc: u32 = tp.get(2).and_then(|v| v.parse().ok()).unwrap_or(0);
        (h, m, sc)
    } else {
        (0, 0, 0)
    };

    // civil_from_days (Howard Hinnant algorithm)
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (m as u64) + 2) / 5 + (day as u64) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;

    Ok(days * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64)
}

// ═══════════════════════════════════════════════════════════════
//  Load sink pipelines from config
// ═══════════════════════════════════════════════════════════════

fn load_pipelines(sinks: &[SinkConfig]) -> Result<Vec<SinkPipeline>, QuotesGenError> {
    let mut pipelines = Vec::new();

    for sink_cfg in sinks {
        let endpoint = Endpoint::load(
            &sink_cfg.name,
            &sink_cfg.transport,
            &sink_cfg.transport_config,
            &sink_cfg.framing,
            &sink_cfg.framing_config,
            &sink_cfg.middleware,
            &sink_cfg.codec,
            &sink_cfg.codec_config,
        )?;

        pipelines.push(SinkPipeline {
            endpoint,
            stream: None,
            buf: Vec::with_capacity(8192),
        });
    }

    Ok(pipelines)
}

// ═══════════════════════════════════════════════════════════════
//  Main dispatch
// ═══════════════════════════════════════════════════════════════

pub async fn run(args: &Effective) -> Result<(), QuotesGenError> {
    let mut rng = Rng::new(args.seed);
    let mut symbols = new_symbols();

    if let Some(ref sym) = args.symbol {
        symbols.retain(|s| s.name.eq_ignore_ascii_case(sym));
        if symbols.is_empty() {
            let names: Vec<_> = new_symbols().iter().map(|s| s.name).collect();
            return Err(QuotesGenError::Config(format!(
                "unknown symbol: {sym}\navailable: {}",
                names.join(" ")
            )));
        }
    }

    if args.price > 0.0 && symbols.len() == 1 {
        symbols[0].price = args.price;
    }

    // Load sink pipelines
    let mut pipelines = load_pipelines(&args.sinks)?;

    // Start transports and connect
    for p in &mut pipelines {
        p.endpoint.transport.start()?;
        p.ensure_connected()?;
        tracing::info!(sink = %p.name(), "connected");
    }

    if args.generate {
        let from = args
            .from
            .as_deref()
            .ok_or_else(|| QuotesGenError::Config("--from is required with --generate".into()))?;
        let to = args
            .to
            .as_deref()
            .ok_or_else(|| QuotesGenError::Config("--to is required with --generate".into()))?;
        let from_sec = parse_time(from)?;
        let to_sec = parse_time(to)?;
        if to_sec <= from_sec {
            return Err(QuotesGenError::Config("--to must be after --from".into()));
        }
        run_generate(args, &mut pipelines, &mut symbols, &mut rng, from_sec, to_sec).await
    } else if let Some(ref path) = args.file {
        let path = path.clone();
        run_file(args, &mut pipelines, &path).await
    } else {
        run_stream(args, &mut pipelines, &mut symbols, &mut rng).await
    }
}

// ═══════════════════════════════════════════════════════════════
//  Generate mode: stdout + sinks
// ═══════════════════════════════════════════════════════════════

async fn run_generate(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
    from_sec: i64,
    to_sec: i64,
) -> Result<(), QuotesGenError> {
    let interval_ms = args.interval as i64;
    let from_ms = from_sec * 1000;
    let to_ms = to_sec * 1000;

    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    let mut quotes: Vec<Quote> = Vec::new();

    let mut t_ms = from_ms;
    while t_ms < to_ms {
        for sym in symbols.iter_mut() {
            sym.tick(rng);
            let jitter = rng.next_intn(interval_ms as usize) as i64;
            let ts = t_ms + jitter;
            let quote = sym.to_quote_ts(ts);
            // Serialize for stdout using first pipeline's codec
            if let Some(p) = pipelines.first() {
                let record = quote_to_record(&quote)?;
                if let Ok(data) = p.endpoint.codec.encode(&record.data) {
                    let _ = out.write_all(&data);
                    let _ = out.write_all(b"\n");
                }
            }
            quotes.push(quote);
        }
        t_ms += interval_ms;
    }
    out.flush().ok();

    let duration = to_sec - from_sec;
    tracing::info!(
        ticks = quotes.len(),
        symbols = symbols.len(),
        duration_s = duration,
        interval_ms,
        "generated ticks"
    );

    // Send to sinks
    tracing::info!(count = quotes.len(), "sending to sinks");
    let start = std::time::Instant::now();

    if args.rate <= 0.0 {
        // Batch send
        for (i, quote) in quotes.iter().enumerate() {
            for p in pipelines.iter_mut() {
                p.send_quote(quote)?;
            }
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                eprint!(
                    "\r  {}/{} sent ({:.1} msg/s)",
                    i + 1,
                    quotes.len(),
                    (i + 1) as f64 / elapsed.as_secs_f64()
                );
            }
        }
    } else {
        // Rate-limited send
        let interval = std::time::Duration::from_secs_f64(1.0 / args.rate);
        for (i, quote) in quotes.iter().enumerate() {
            for p in pipelines.iter_mut() {
                p.send_quote(quote)?;
            }
            if (i + 1) % 500 == 0 {
                let elapsed = start.elapsed();
                eprint!(
                    "\r  {}/{} sent ({:.1} msg/s)",
                    i + 1,
                    quotes.len(),
                    (i + 1) as f64 / elapsed.as_secs_f64()
                );
            }
            if i < quotes.len() - 1 {
                tokio::time::sleep(interval).await;
            }
        }
    }

    let elapsed = start.elapsed();
    tracing::info!(
        sent = quotes.len(),
        elapsed_s = format_args!("{:.1}", elapsed.as_secs_f64()),
        rate = format_args!("{:.1}", quotes.len() as f64 / elapsed.as_secs_f64()),
        "send complete"
    );

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  Stream mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_stream(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    println!("Quote Generator (plugin-based)");
    println!("  sinks   : {}", pipelines.iter().map(|p| p.name()).collect::<Vec<_>>().join(", "));
    if let Some(ref sym) = args.symbol {
        println!("  symbol  : {sym}");
    }
    if args.history {
        println!("  ts_ms   : yes");
    }

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_random_auto(args, pipelines, symbols, rng).await
    } else {
        println!();
        println!("Commands: Enter -- send 1 random quote | N -- send N quotes | q -- quit");
        println!();
        run_random_interactive(args, pipelines, symbols, rng).await
    }
}

async fn run_random_interactive(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    let stdin = std::io::stdin();

    loop {
        print!("> ");
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            break;
        }

        if input.is_empty() {
            let idx = rng.next_intn(symbols.len());
            symbols[idx].tick(rng);
            let quote = symbols[idx].to_quote(args.history);
            for p in pipelines.iter_mut() {
                p.send_quote_reconnect(&quote)?;
            }
            // Print serialized form
            if let Some(p) = pipelines.first() {
                let record = quote_to_record(&quote)?;
                if let Ok(data) = p.endpoint.codec.encode(&record.data) {
                    println!("  -> {}", String::from_utf8_lossy(&data));
                }
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    for _ in 0..n {
                        let idx = rng.next_intn(symbols.len());
                        symbols[idx].tick(rng);
                        let quote = symbols[idx].to_quote(args.history);
                        if let Some(p) = pipelines.first() {
                            let record = quote_to_record(&quote)?;
                            if let Ok(data) = p.endpoint.codec.encode(&record.data) {
                                println!("  -> {}", String::from_utf8_lossy(&data));
                            }
                        }
                        for p in pipelines.iter_mut() {
                            p.send_quote_reconnect(&quote)?;
                        }
                    }
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    Ok(())
}

async fn run_random_auto(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    symbols: &mut [Symbol],
    rng: &mut Rng,
) -> Result<(), QuotesGenError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / args.rate));
    let mut sent = 0u64;
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                break;
            }
            _ = interval.tick() => {
                let idx = rng.next_intn(symbols.len());
                symbols[idx].tick(rng);
                let quote = symbols[idx].to_quote(args.history);
                for p in pipelines.iter_mut() {
                    if let Err(e) = p.send_quote_reconnect(&quote) {
                        tracing::error!(sink = %p.name(), error = %e, "send error");
                    }
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════
//  File mode: interactive / auto
// ═══════════════════════════════════════════════════════════════

async fn run_file(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    path: &str,
) -> Result<(), QuotesGenError> {
    let mut fr = FileReader::open(path)?;

    println!("Quote Generator (plugin-based)");
    println!("  sinks   : {}", pipelines.iter().map(|p| p.name()).collect::<Vec<_>>().join(", "));
    println!("  mode    : file ({}, {} lines)", fr.path, fr.total());

    if args.rate > 0.0 {
        println!("  rate    : {:.1} msg/s", args.rate);
        println!();
        run_file_auto(args, pipelines, &mut fr).await
    } else {
        println!();
        println!("Commands: Enter -- send next | N -- send N lines | q -- quit");
        println!();
        run_file_interactive(pipelines, &mut fr).await
    }
}

async fn run_file_interactive(
    pipelines: &mut [SinkPipeline],
    fr: &mut FileReader,
) -> Result<(), QuotesGenError> {
    let stdin = std::io::stdin();

    while !fr.done() {
        print!("[{}/{}] > ", fr.total() - fr.remaining(), fr.total());
        std::io::stdout().flush().ok();

        let mut input = String::new();
        if stdin.read_line(&mut input).unwrap_or(0) == 0 {
            break;
        }
        let input = input.trim();

        if input == "q" || input == "quit" {
            println!("Bye!");
            return Ok(());
        }

        if input.is_empty() {
            if let Some(line) = fr.next_line() {
                let line = line.to_string();
                println!("  -> {line}");
                for p in pipelines.iter_mut() {
                    p.send_raw_reconnect(line.as_bytes())?;
                }
            }
        } else {
            match input.parse::<usize>() {
                Ok(n) if n > 0 => {
                    let lines: Vec<String> = fr.next_n(n).to_vec();
                    if lines.is_empty() {
                        println!("  EOF");
                        continue;
                    }
                    for l in &lines {
                        println!("  -> {l}");
                        for p in pipelines.iter_mut() {
                            p.send_raw_reconnect(l.as_bytes())?;
                        }
                    }
                    println!("  sent {} lines", lines.len());
                }
                _ => {
                    println!("  unknown command (Enter, N, q)");
                }
            }
        }
    }

    println!("  EOF -- all lines sent");
    Ok(())
}

async fn run_file_auto(
    args: &Effective,
    pipelines: &mut [SinkPipeline],
    fr: &mut FileReader,
) -> Result<(), QuotesGenError> {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs_f64(1.0 / args.rate));
    let mut sent = 0u64;
    let total = fr.total();
    let start = std::time::Instant::now();

    println!("Sending... (Ctrl+C to stop)");

    while !fr.done() {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let elapsed = start.elapsed();
                println!(
                    "\n  stopped: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
                    elapsed.as_secs_f64(),
                    sent as f64 / elapsed.as_secs_f64()
                );
                return Ok(());
            }
            _ = interval.tick() => {
                let line = match fr.next_line() {
                    Some(l) => l.to_string(),
                    None => break,
                };
                for p in pipelines.iter_mut() {
                    if let Err(e) = p.send_raw_reconnect(line.as_bytes()) {
                        tracing::error!(sink = %p.name(), error = %e, "send error");
                    }
                }
                sent += 1;
                if sent % 100 == 0 {
                    let elapsed = start.elapsed();
                    eprint!("\r  {sent}/{total} sent ({:.1} msg/s)", sent as f64 / elapsed.as_secs_f64());
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\r  done: {sent}/{total} sent in {:.1}s ({:.1} msg/s)",
        elapsed.as_secs_f64(),
        sent as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
