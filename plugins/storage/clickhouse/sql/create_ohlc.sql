CREATE TABLE IF NOT EXISTS {table} (
    key    LowCardinality(String),
    ts_ms  Int64,
    format LowCardinality(String) DEFAULT 'json',
    data   String
) ENGINE = ReplacingMergeTree()
ORDER BY (key, ts_ms)
