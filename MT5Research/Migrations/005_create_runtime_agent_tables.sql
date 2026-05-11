CREATE TABLE IF NOT EXISTS {database}.runtime_agent_events
(
    broker_source_id String,
    agent_name LowCardinality(String),
    status LowCardinality(String),
    severity LowCardinality(String),
    message String,
    details String,
    started_at_utc Int64,
    finished_at_utc Int64,
    duration_ms UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(finished_at_utc, 'UTC'))
ORDER BY (broker_source_id, agent_name, finished_at_utc);

CREATE TABLE IF NOT EXISTS {database}.runtime_agent_state
(
    broker_source_id String,
    agent_name LowCardinality(String),
    status LowCardinality(String),
    last_message String,
    last_ok_at_utc Int64,
    last_error_at_utc Int64,
    updated_at_utc Int64
)
ENGINE = ReplacingMergeTree(updated_at_utc)
ORDER BY (broker_source_id, agent_name);
