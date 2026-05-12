CREATE TABLE IF NOT EXISTS {database}.ingest_operations
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    timeframe LowCardinality(String),
    operation_type LowCardinality(String),
    batch_id String,
    mt5_range_start Int64,
    mt5_range_end_exclusive Int64,
    status LowCardinality(String),
    status_rank UInt8,
    stage LowCardinality(String),
    source_bar_count Nullable(UInt32),
    canonical_row_count Nullable(UInt32),
    source_hash Nullable(String),
    error_message Nullable(String),
    event_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(event_at_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, batch_id, event_at_utc, status_rank);

CREATE TABLE IF NOT EXISTS {database}.ohlc_m1_verified_coverage
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    timeframe LowCardinality(String),
    mt5_range_start Int64,
    mt5_range_end_exclusive Int64,
    utc_range_start Int64,
    utc_range_end_exclusive Int64,
    source_bar_count UInt32,
    canonical_row_count UInt32,
    source_hash String,
    verification_method LowCardinality(String),
    batch_id String,
    verified_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(utc_range_start, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, utc_range_start, utc_range_end_exclusive, verified_at_utc);
