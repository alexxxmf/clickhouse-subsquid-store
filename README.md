# @subsquid/clickhouse-store

Production-ready ClickHouse adapter for [Subsquid](https://subsquid.io) EVM processors with automatic hot/cold table architecture and blockchain reorg handling.

**Created by Alex Moro Fernandez**

## Why Hot/Cold Architecture?

Traditional ClickHouse DELETE operations are expensive:
- `DELETE` marks rows for deletion, requiring `FINAL` keyword in queries
- `FINAL` forces full table scans and prevents index usage
- Queries become slow as data grows

This library solves it by **isolating mutable from immutable data**:
- **Hot tables**: Last ~50 blocks (subject to reorgs) - small, fast deletes
- **Cold tables**: Historical data (finalized) - append-only, no `FINAL` needed
- **Result**: Fast queries on TB+ of data while handling blockchain reorganizations correctly

## Features

- 🔥 **Hot/Cold Table Architecture** - Automatic data lifecycle management
- 🔄 **Auto-Migration** - Configurable migration strategies (block count, finality, time-based)
- 🛡️ **Reorg Handling** - Efficient blockchain reorganization support using registry pattern
- 📁 **Folder-based Config** - Explicit separation of event vs snapshot tables
- ✅ **Schema Validation** - Early detection of configuration errors
- 📊 **Built-in Metrics** - Performance tracking out of the box
- 🔧 **Flexible Hooks** - Customize migration behavior for your use case

## Installation

```bash
npm install @subsquid/clickhouse-store @clickhouse/client
```

## Quick Start

```typescript
import { ClickhouseDatabase } from '@subsquid/clickhouse-store'
import { createClient } from '@clickhouse/client'

// 1. Create ClickHouse client
const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'my_indexer',
})

// 2. Create database adapter
const database = new ClickhouseDatabase({
  client,
  processorId: 'my-indexer',
  network: 'ethereum',
  supportHotBlocks: true,
  hotBlocksDepth: 50,

  // Auto-migration every 30 blocks
  autoMigrate: true,
  migrationInterval: 30,
})

// 3. Use with Subsquid processor
processor.run(database, async (ctx) => {
  // Update routing (hot vs cold tables)
  database.setIsAtChainTip(ctx.isHead)

  // Your data processing logic
  const transfers = processTransfers(ctx.blocks)
  await ctx.store.insert(transfers)

  // Migration happens automatically!
})
```

## Table Organization

Organize your SQL schemas by table type:

```
src/db/tables/
├── hot-supported/          # Tables that need hot/cold variants
│   └── transfers.sql       # Creates hot_ and cold_ tables
└── regular/                # Tables without hot/cold
    └── balances.sql        # Creates single table
```

**Hot-supported tables** (event tables):
- Must have a `height` column for migration
- Automatically split into hot/cold variants
- Participate in auto-migration
- Example: transfers, swaps, mints, burns

**Regular tables** (snapshot tables):
- No height column required
- Single table, no hot/cold variants
- Not migrated automatically
- Example: balances, positions, aggregates

## How It Works

### Phase 1: Catchup (Historical Sync)

```
Archive → Processor → Cold Tables (append-only)
```

- Fetches historical blocks from Subsquid archive
- Inserts directly to **cold tables** (data is already finalized)
- Fast bulk inserts, no migration needed
- Cold tables grow but stay append-only

### Phase 2: Chain Tip (Real-time)

```
RPC → Processor → Hot Tables → (migration) → Cold Tables
```

- Switches to RPC for latest blocks
- Inserts to **hot tables** (recent blocks can reorg)
- Auto-migration runs every N blocks
- Moves finalized data to cold tables
- Hot tables stay small (~50 blocks worth)

### Phase 3: Reorg Handling

```
Reorg detected → Update ValidBlocks → No expensive DELETEs!
```

- ValidBlocksManager tracks valid block hashes
- On reorg: update registry, not data
- Queries filter by valid blocks automatically
- No slow DELETE operations needed

## Advanced Usage

### Custom Migration Hooks

```typescript
const database = new ClickhouseDatabase({
  client,
  processorId: 'my-indexer',
  network: 'ethereum',
  autoMigrate: true,
  migrationInterval: 30,

  migrationHooks: {
    beforeMigration: async (context) => {
      console.log(`Migration check: ${context.blocksSinceLastMigration} blocks`)

      // Conditional migration
      if (context.hotTableRows < 1000) {
        return false // Skip migration
      }

      // Send metrics to monitoring
      metrics.recordMigrationCheck(context)

      return true // Proceed
    },

    afterMigration: async (result) => {
      console.log(`Migrated ${result.migrated} rows in ${result.durationMs}ms`)

      // Alert if migration is slow
      if (result.durationMs > 5000) {
        alerts.send('Migration took >5s')
      }

      // Send metrics
      metrics.recordMigration(result)
    },
  },
})
```

### Custom Height Column

If your tables use a different column name for block height:

```typescript
const database = new ClickhouseDatabase({
  client,
  processorId: 'my-indexer',
  network: 'ethereum',

  // Use different column name
  heightColumnName: 'block_number',

  // Custom paths
  tablesPath: 'database/schemas',
  hotSupportedTablesPath: 'database/schemas/events',
})
```

### Manual Migration

Trigger migration manually when needed:

```typescript
// Trigger migration manually
const result = await database.migrateHotToCold()

console.log(`Migrated ${result.migrated} rows`)
console.log(`Cutoff height: ${result.cutoffHeight}`)
console.log(`Duration: ${result.durationMs}ms`)
console.log(`Tables:`, result.tables)
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `client` | `ClickHouseClient` | required | ClickHouse client instance |
| `processorId` | `string` | `'default'` | Unique processor ID |
| `network` | `string` | `'ethereum'` | Network name for table prefixing |
| `supportHotBlocks` | `boolean` | `true` | Enable hot/cold architecture |
| `hotBlocksDepth` | `number` | `10` | Number of blocks to keep in hot tables |
| `tablesPath` | `string` | `'src/db/tables'` | Base path for table definitions |
| `hotSupportedTablesPath` | `string` | `'src/db/tables/hot-supported'` | Path to hot-supported tables |
| `heightColumnName` | `string` | `'height'` | Column name for block height |
| `autoMigrate` | `boolean` | `true` | Enable automatic migration |
| `migrationInterval` | `number` | `30` | Migrate every N blocks |
| `migrationOnFinality` | `boolean` | `false` | Migrate when finality advances |
| `migrationHooks` | `MigrationHooks` | `undefined` | Custom migration hooks |

## Architecture Deep Dive

### Delete Efficiency

**Without Hot/Cold**:
```sql
-- Slow: requires FINAL, full table scan
SELECT * FROM transfers FINAL
WHERE block_timestamp > '2024-01-01'
ORDER BY value DESC
LIMIT 100
```

**With Hot/Cold**:
```sql
-- Fast: append-only cold table, no FINAL needed
SELECT * FROM cold_transfers
WHERE block_timestamp > '2024-01-01'
ORDER BY value DESC
LIMIT 100
```

### Registry Pattern for Reorgs

Instead of deleting orphaned data:

1. **ValidBlocksManager** maintains a registry of valid block hashes
2. On reorg: update registry, don't touch data
3. Queries automatically filter by valid blocks
4. Data cleanup happens during migration (bulk operation)

This avoids ClickHouse's DELETE performance pitfalls:
- No `FINAL` keyword needed
- No full partition scans
- Indexes work properly
- Queries stay fast

## Performance

Benchmarks on Ethereum mainnet (M1 MacBook Pro, 4 cores):

| Metric | Value |
|--------|-------|
| **Catchup speed** | ~500 blocks/sec |
| **Migration** | ~5,000 rows in <200ms |
| **Reorg handling** | <50ms for 10-block reorg |
| **Memory usage** | ~150MB stable at chain tip |
| **Query speed (cold)** | Full index support, no FINAL |

## Examples

- [Basic ERC20 Transfers](examples/basic/) - Simple transfer indexer
- More examples coming soon!

## Troubleshooting

### Tables not created

Make sure your table SQL files are in the correct directories:
- Event tables → `src/db/tables/hot-supported/`
- Snapshot tables → `src/db/tables/regular/`

### Migration not triggering

Check that:
1. `autoMigrate` is `true`
2. You're at chain tip (`ctx.isHead` is `true`)
3. `migrationInterval` blocks have passed

### Validation errors

If you see "Table missing required column 'height'":
- Move the table to `regular/` if it doesn't need hot/cold
- Or add a `height` column to the table schema

## Development

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Run tests with coverage
npm run test:coverage

# Lint
npm run lint

# Format
npm run format
```

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT © Alex Moro Fernandez

## Acknowledgments

- Built for [Subsquid](https://subsquid.io)
- Inspired by the need for efficient blockchain indexing at scale
- Thanks to the ClickHouse and Subsquid communities

## Support

- GitHub Issues: [Report a bug](https://github.com/alexmf1990/clickhouse-subsquid-store/issues)
- Documentation: [Full API docs](docs/API.md)
- Examples: [See examples/](examples/)
