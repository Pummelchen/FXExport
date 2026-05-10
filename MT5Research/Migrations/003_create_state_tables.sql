CREATE TABLE IF NOT EXISTS {database}.broker_time_offsets
(
    broker_source_id String,
    valid_from_mt5_server_ts Int64,
    valid_to_mt5_server_ts Int64,
    offset_seconds Int32,
    source LowCardinality(String),
    confidence LowCardinality(String),
    created_at_utc Int64
)
ENGINE = MergeTree
ORDER BY (broker_source_id, valid_from_mt5_server_ts);

CREATE TABLE IF NOT EXISTS {database}.ingest_state
(
    broker_source_id String,
    logical_symbol String,
    mt5_symbol String,
    oldest_mt5_server_ts_raw Int64,
    latest_ingested_closed_mt5_server_ts_raw Int64,
    latest_ingested_closed_ts_utc Int64,
    status LowCardinality(String),
    last_batch_id String,
    updated_at_utc Int64
)
ENGINE = ReplacingMergeTree(updated_at_utc)
ORDER BY (broker_source_id, logical_symbol);

CREATE TABLE IF NOT EXISTS {database}.verification_results
(
    broker_source_id String,
    logical_symbol String,
    range_start_mt5_server_ts Int64,
    range_end_mt5_server_ts Int64,
    result LowCardinality(String),
    mismatch_count UInt32,
    details String,
    checked_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(checked_at_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, checked_at_utc);

CREATE TABLE IF NOT EXISTS {database}.repair_log
(
    broker_source_id String,
    logical_symbol String,
    range_start_mt5_server_ts Int64,
    range_end_mt5_server_ts Int64,
    decision LowCardinality(String),
    outcome LowCardinality(String),
    details String,
    batch_id String,
    created_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(created_at_utc, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, created_at_utc);
