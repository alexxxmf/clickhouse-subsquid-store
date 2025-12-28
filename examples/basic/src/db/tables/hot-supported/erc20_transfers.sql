CREATE TABLE IF NOT EXISTS ${network}_erc20_transfers (
  token FixedString(42),       -- Ethereum address with 0x prefix (42 chars)
  from FixedString(42),        -- Ethereum address with 0x prefix (42 chars)
  to FixedString(42),          -- Ethereum address with 0x prefix (42 chars)
  value UInt256,
  height UInt64,
  block_timestamp DateTime64(3),
  tx_hash FixedString(66),     -- Transaction hash with 0x prefix (66 chars)
  log_index UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY (token, height, log_index)
PARTITION BY toYYYYMM(block_timestamp)
SETTINGS index_granularity = 8192;