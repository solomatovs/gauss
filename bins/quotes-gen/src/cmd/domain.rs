use server_api::now_ms;

use super::error::QuotesGenError;
use super::sink_pipeline::Quote;

// ═══════════════════════════════════════════════════════════════
//  Symbol
// ═══════════════════════════════════════════════════════════════

pub struct Symbol {
    pub name: &'static str,
    pub price: f64,
    spread: f64,
    step: f64,
}

impl Symbol {
    pub fn tick(&mut self, rng: &mut Rng) {
        let delta = (rng.next_f64() * 2.0 - 1.0) * self.step;
        self.price += delta;
        if self.price < self.step {
            self.price = self.step;
        }
    }

    pub fn to_quote(&self, history: bool) -> Quote {
        Quote {
            symbol: self.name.to_string(),
            bid: self.price,
            ask: self.price + self.spread,
            ts_ms: if history { Some(now_ms()) } else { None },
        }
    }

    pub fn to_quote_ts(&self, ts_ms: i64) -> Quote {
        Quote {
            symbol: self.name.to_string(),
            bid: self.price,
            ask: self.price + self.spread,
            ts_ms: Some(ts_ms),
        }
    }
}

pub fn new_symbols() -> Vec<Symbol> {
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

pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: i64) -> Self {
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
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }

    pub fn next_intn(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ═══════════════════════════════════════════════════════════════
//  File reader
// ═══════════════════════════════════════════════════════════════

pub struct FileReader {
    lines: Vec<String>,
    pos: usize,
    pub path: String,
}

impl FileReader {
    pub fn open(path: &str) -> Result<Self, QuotesGenError> {
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

    pub fn total(&self) -> usize {
        self.lines.len()
    }
    pub fn remaining(&self) -> usize {
        self.lines.len() - self.pos
    }
    pub fn done(&self) -> bool {
        self.pos >= self.lines.len()
    }

    pub fn next_line(&mut self) -> Option<&str> {
        if self.done() {
            return None;
        }
        let line = &self.lines[self.pos];
        self.pos += 1;
        Some(line)
    }

    pub fn next_n(&mut self, n: usize) -> &[String] {
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
//  Time parsing (multiple formats)
// ═══════════════════════════════════════════════════════════════

pub fn parse_time(s: &str) -> Result<i64, QuotesGenError> {
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
