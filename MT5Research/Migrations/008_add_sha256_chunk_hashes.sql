ALTER TABLE {database}.ingest_operations
ADD COLUMN IF NOT EXISTS hash_schema_version Nullable(String) AFTER source_hash;

ALTER TABLE {database}.ingest_operations
ADD COLUMN IF NOT EXISTS mt5_source_sha256 Nullable(String) AFTER hash_schema_version;

ALTER TABLE {database}.ingest_operations
ADD COLUMN IF NOT EXISTS canonical_readback_sha256 Nullable(String) AFTER mt5_source_sha256;

ALTER TABLE {database}.ingest_operations
ADD COLUMN IF NOT EXISTS offset_authority_sha256 Nullable(String) AFTER canonical_readback_sha256;

ALTER TABLE {database}.ohlc_m1_verified_coverage
ADD COLUMN IF NOT EXISTS hash_schema_version LowCardinality(String) DEFAULT '' AFTER source_hash;

ALTER TABLE {database}.ohlc_m1_verified_coverage
ADD COLUMN IF NOT EXISTS mt5_source_sha256 String DEFAULT '' AFTER hash_schema_version;

ALTER TABLE {database}.ohlc_m1_verified_coverage
ADD COLUMN IF NOT EXISTS canonical_readback_sha256 String DEFAULT '' AFTER mt5_source_sha256;

ALTER TABLE {database}.ohlc_m1_verified_coverage
ADD COLUMN IF NOT EXISTS offset_authority_sha256 String DEFAULT '' AFTER canonical_readback_sha256;
