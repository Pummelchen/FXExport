CREATE TABLE IF NOT EXISTS {database}.broker_sources
(
    broker_source_id String,
    mt5_company String,
    mt5_server String,
    mt5_account_login Int64,
    discovery_source LowCardinality(String),
    status LowCardinality(String),
    is_active UInt8,
    first_seen_utc Int64,
    last_seen_utc Int64
)
ENGINE = ReplacingMergeTree(last_seen_utc)
ORDER BY (broker_source_id, mt5_company, mt5_server, mt5_account_login);

CREATE TABLE IF NOT EXISTS {database}.data_certificates
(
    broker_source_id String,
    logical_symbol String,
    timeframe LowCardinality(String),
    utc_range_start Int64,
    utc_range_end_exclusive Int64,
    certificate_sha256 String,
    hash_schema_version LowCardinality(String),
    coverage_row_count UInt32,
    coverage_source_bar_count UInt64,
    coverage_canonical_row_count UInt64,
    first_covered_utc Int64,
    last_covered_utc Int64,
    mt5_source_sha256_aggregate String,
    canonical_readback_sha256_aggregate String,
    offset_authority_sha256_aggregate String,
    certificate_status LowCardinality(String),
    created_at_utc Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(utc_range_start, 'UTC'))
ORDER BY (broker_source_id, logical_symbol, utc_range_start, utc_range_end_exclusive, created_at_utc);
