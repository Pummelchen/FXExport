ALTER TABLE {database}.ingest_operations
ADD COLUMN IF NOT EXISTS status_rank UInt8 AFTER status;
