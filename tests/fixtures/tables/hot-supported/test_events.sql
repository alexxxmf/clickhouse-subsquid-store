CREATE TABLE IF NOT EXISTS ${network}_test_events (
  id UInt64,
  height UInt64,
  data String,
  timestamp DateTime64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (height, id)
SETTINGS index_granularity = 8192;
