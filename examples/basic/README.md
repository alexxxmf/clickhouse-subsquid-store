# Basic Example: ERC20 Transfer Indexer

This example demonstrates how to use `@subsquid/clickhouse-store` to index ERC20 transfers with automatic hot/cold table management.

## Features Demonstrated

- ✅ Hot/cold table architecture
- ✅ Automatic migration every 30 blocks
- ✅ Reorg handling with ValidBlocksManager
- ✅ Migration hooks for monitoring
- ✅ Proper table organization (hot-supported vs regular)

## Setup

1. **Start ClickHouse**:
   ```bash
   docker run -d --name clickhouse \
     -p 8123:8123 \
     clickhouse/clickhouse-server:latest
   ```

2. **Install dependencies**:
   ```bash
   npm install
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

4. **Run the indexer**:
   ```bash
   npm start
   ```

## Table Structure

### Hot-Supported Tables (src/db/tables/hot-supported/)

Tables that require hot/cold variants for reorg handling:

- **erc20_transfers.sql** - Event table with `height` column
  - Creates: `ethereum_hot_erc20_transfers` and `ethereum_cold_erc20_transfers`
  - Participates in auto-migration
  - Used for indexing transfer events

### Regular Tables (src/db/tables/regular/)

Tables that don't need hot/cold variants:

- **erc20_balances.sql** - Snapshot table
  - Creates: `ethereum_erc20_balances` (single table)
  - Not auto-migrated
  - Updated by separate scripts

## How It Works

1. **Catchup Phase**:
   - Processor fetches historical blocks from archive
   - Data inserted directly to **cold tables** (immutable)
   - No migration needed (historical data is already finalized)

2. **Chain Tip Phase**:
   - Processor switches to RPC for latest blocks
   - Data inserted to **hot tables** (mutable, can reorg)
   - Auto-migration runs every 30 blocks

3. **Auto-Migration**:
   - Triggered after 30 blocks at chain tip
   - Moves finalized data (older than 50 blocks) to cold tables
   - Hot tables stay small and fast
   - Cold tables grow with append-only inserts

4. **Reorg Handling**:
   - If blockchain reorganizes, ValidBlocksManager updates valid block hashes
   - No expensive DELETE operations needed
   - Queries automatically filter by valid blocks

## Migration Logs

You'll see migration logs like this:

```
📊 Migration check: 30 blocks
📦 Hot→Cold migration: maxHeight=10000080, cutoff=10000030, keeping last 50 blocks hot
   ✓ erc20_transfers: 1,542 rows migrated to cold
✅ Migrated 1,542 rows in 125ms
```

## Querying Data

Since cold tables are append-only, queries are fast:

```sql
-- Query cold table (historical data, no FINAL needed)
SELECT
  token,
  count() as transfer_count,
  sum(value) as total_value
FROM ethereum_cold_erc20_transfers
WHERE block_timestamp >= '2024-01-01'
GROUP BY token
ORDER BY transfer_count DESC
LIMIT 10

-- Query hot table (recent data)
SELECT *
FROM ethereum_hot_erc20_transfers
WHERE height > 10000000
ORDER BY height DESC
LIMIT 100

-- Combined view (if needed)
SELECT * FROM ethereum_cold_erc20_transfers
UNION ALL
SELECT * FROM ethereum_hot_erc20_transfers
```

## Next Steps

- Modify the processor to index additional events
- Add custom migration hooks for monitoring
- Implement materialized views for analytics
- Deploy to production with monitoring
