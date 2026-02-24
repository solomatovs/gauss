/// Resolve a dot-notation path in a `serde_json::Value`.
///
/// Supports nested field access via dot separation:
/// - `"symbol"` → `value["symbol"]`
/// - `"quote.symbol"` → `value["quote"]["symbol"]`
/// - `"data.prices.bid"` → `value["data"]["prices"]["bid"]`
///
/// Returns `None` if any segment is missing.
pub fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// Текущее Unix-время в миллисекундах.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Конвертировать unix ms в строку даты `YYYY-MM-DD`.
/// Использует алгоритм Howard Hinnant (civil_from_days).
pub fn date_from_ms(ms: i64) -> String {
    let secs = ms / 1000;
    let days = secs.div_euclid(86400) + 719468;
    let era = days.div_euclid(146097);
    let doe = days.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}
