ALTER TABLE {database}.broker_time_offsets
ADD COLUMN IF NOT EXISTS mt5_company String DEFAULT '' AFTER broker_source_id;

ALTER TABLE {database}.broker_time_offsets
ADD COLUMN IF NOT EXISTS mt5_server String DEFAULT '' AFTER mt5_company;

ALTER TABLE {database}.broker_time_offsets
ADD COLUMN IF NOT EXISTS mt5_account_login Int64 DEFAULT 0 AFTER mt5_server;

ALTER TABLE {database}.broker_time_offsets
ADD COLUMN IF NOT EXISTS verification_evidence String DEFAULT '' AFTER confidence;

ALTER TABLE {database}.broker_time_offsets
ADD COLUMN IF NOT EXISTS is_active UInt8 DEFAULT 1 AFTER verification_evidence;
