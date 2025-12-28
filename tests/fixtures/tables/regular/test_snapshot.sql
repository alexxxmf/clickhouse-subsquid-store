CREATE TABLE IF NOT EXISTS ${network}_test_snapshot (
  id UInt64,
  value String,
  updated_at DateTime64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;
