CREATE TABLE IF NOT EXISTS {database}.mt5_ohlc_m1_raw
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    timeframe LowCardinality(String),
    mt5_server_ts_raw Int64,
    ts_utc Int64,
    server_utc_offset_seconds Int32,
    offset_source LowCardinality(String),
    offset_confidence LowCardinality(String),
    open_scaled Int64,
    high_scaled Int64,
    low_scaled Int64,
    close_scaled Int64,
    digits UInt8,
    batch_id String,
    bar_hash String,
    source_status LowCardinality(String),
    ingested_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(ts_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, ts_utc, batch_id);

CREATE TABLE IF NOT EXISTS {database}.ohlc_m1_canonical
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    timeframe LowCardinality(String),
    mt5_server_ts_raw Int64,
    ts_utc Int64,
    server_utc_offset_seconds Int32,
    offset_source LowCardinality(String),
    offset_confidence LowCardinality(String),
    open_scaled Int64,
    high_scaled Int64,
    low_scaled Int64,
    close_scaled Int64,
    digits UInt8,
    batch_id String,
    bar_hash String,
    source_status LowCardinality(String),
    ingested_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(ts_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, ts_utc);

CREATE TABLE IF NOT EXISTS {database}.ohlc_m1_conflicts
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    ts_utc Int64,
    existing_bar_hash String,
    incoming_bar_hash String,
    existing_open_scaled Int64,
    existing_high_scaled Int64,
    existing_low_scaled Int64,
    existing_close_scaled Int64,
    incoming_open_scaled Int64,
    incoming_high_scaled Int64,
    incoming_low_scaled Int64,
    incoming_close_scaled Int64,
    detected_at_utc Int64,
    batch_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(ts_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, ts_utc, detected_at_utc);
