CREATE TABLE IF NOT EXISTS ${network}_erc20_balances (
  token FixedString(42),       -- Ethereum address with 0x prefix (42 chars)
  address FixedString(42),     -- Ethereum address with 0x prefix (42 chars)
  balance UInt256,
  last_updated_height UInt64,
  last_updated_timestamp DateTime64(3)
) ENGINE = ReplacingMergeTree(last_updated_height)
ORDER BY (token, address)
SETTINGS index_granularity = 8192;
