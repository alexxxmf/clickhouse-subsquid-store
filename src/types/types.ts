/**
 * Type definitions for ClickHouse Database adapter
 * Compatible with @subsquid/evm-processor
 */

/**
 * Block reference containing height and hash information
 */
export interface BlockRef {
  height: number
  hash: string
  parent?: string
}

/**
 * Hash and height tuple for hot block processing
 */
export interface HashAndHeight {
  height: number
  hash: string
}

/**
 * Database state returned by connect()
 * Represents the current indexing position
 */
export interface DatabaseState {
  height: number      // Last indexed block height
  hash: string        // Last indexed block hash (for hot blocks)
  top: HashAndHeight[] // Hot block chain (unfinalized blocks)
  finalizedHeight?: number // Last finalized block height (for proper recovery)
}

/**
 * Transaction info for finalized blocks
 * Matches @subsquid/util-internal-processor-tools interface
 */
export interface FinalTxInfo {
  prevHead: HashAndHeight
  nextHead: HashAndHeight
  isOnTop: boolean
}

/**
 * Transaction info for hot (unfinalized) blocks
 * Matches @subsquid/util-internal-processor-tools interface
 */
export interface HotTxInfo {
  finalizedHead: HashAndHeight
  baseHead: HashAndHeight
  newBlocks: HashAndHeight[]
}

/**
 * Store interface compatible with @subsquid/typeorm-store
 */
export interface Store {
  insert<T extends object>(entities: T | T[]): Promise<void>
  save<T extends object>(entities: T | T[]): Promise<void>
  remove<E extends object>(entity: E | E[]): Promise<void>
  // Additional methods can be added as needed
}

/**
 * Status table row structure
 */
export interface StatusRow {
  id: string
  height: number
  hash: string
  parent_hash: string
  hot_blocks: string        // JSON serialized BlockRef[]
  finalized_height: number
  finalized_hash: string    // Hash of the finalized block (for proper recovery)
  timestamp: number
  last_run: number          // Timestamp of last processor run (for staleness detection)
}

/**
 * Migration result returned by migrateHotToCold()
 */
export interface MigrationResult {
  migrated: number           // Total number of rows migrated
  cutoffHeight: number       // Block height cutoff for this migration
  durationMs: number         // Time taken in milliseconds
  tables: Array<{            // Per-table migration stats
    name: string
    rows: number
  }>
}

/**
 * Context provided to migration hooks
 */
export interface MigrationContext {
  currentHeight: number      // Latest block height in hot tables
  finalizedHeight: number    // Latest finalized block height
  hotTableRows: number       // Estimated rows in hot tables
  blocksSinceLastMigration: number
  isAtChainTip: boolean      // Whether we're at the chain tip
}

/**
 * Context provided to transformRows hook during migration
 */
export interface TransformRowsContext {
  client: any                // ClickHouseClient for querying (e.g., price lookups)
  table: string              // Base table name (e.g., 'swaps')
  hotTable: string           // Full hot table name (e.g., 'ethereum_hot_swaps')
  coldTable: string          // Full cold table name (e.g., 'ethereum_cold_swaps')
  cutoffHeight: number       // Block height cutoff for this migration
  network: string            // Network name (e.g., 'ethereum', 'base')
}

/**
 * Hooks for customizing migration behavior
 */
export interface MigrationHooks {
  /**
   * Called before migration starts
   * Return false to cancel migration
   */
  beforeMigration?: (context: MigrationContext) => Promise<boolean>

  /**
   * Called after successful migration
   */
  afterMigration?: (result: MigrationResult) => Promise<void>

  /**
   * Custom migration logic (replaces default migrateHotToCold)
   */
  customMigration?: (database: any) => Promise<MigrationResult>

  /**
   * Transform rows in-flight during migration (before INSERT to cold)
   *
   * When provided, rows are loaded into memory, transformed, then inserted.
   * When not provided, uses efficient direct INSERT INTO ... SELECT ...
   *
   * Use cases:
   * - Enrich data with additional lookups (e.g., token prices)
   * - Filter out unwanted rows
   * - Normalize or clean data before cold storage
   *
   * @example
   * transformRows: async (rows, ctx) => {
   *   if (ctx.table !== 'swaps') return rows; // Only transform swaps
   *   const exoticSwaps = rows.filter(r => r.eth_price_usd === 0);
   *   // Lookup prices and enrich...
   *   return rows;
   * }
   */
  transformRows?: <T extends Record<string, any>>(
    rows: T[],
    context: TransformRowsContext
  ) => Promise<T[]>
}

/**
 * Configuration options for ClickhouseDatabase
 */
export interface ClickhouseDatabaseOptions {
  client: any  // ClickHouseClient from @clickhouse/client
  processorId?: string       // Unique ID for this processor (e.g., 'amm-v2', 'amm-v3')
                             // Required when running multiple processors to avoid conflicts
  stateTable?: string        // Default: 'squid_processor_status'
  supportHotBlocks?: boolean // Default: true
  hotBlocksDepth?: number    // Default: 10
  network?: string           // Network name for table prefixing (e.g., 'ethereum', 'base')
                             // Default: 'ethereum'
  tables?: string[]          // Tables managed by this processor (for scoped rollbacks)
                             // If not provided, rollback affects ALL tables in database
  tablesPath?: string        // Path to SQL table definitions directory
                             // Default: 'src/db/tables' (relative to project root)
  hotSupportedTablesPath?: string // Path to SQL files for tables that need hot/cold support
                             // Default: 'src/db/tables/hot-supported' (relative to project root)
                             // Only tables in this directory will participate in auto-migration
  heightColumnName?: string  // Name of the column containing block height
                             // Default: 'height'
                             // Used for migration queries and validation

  // Auto-migration configuration
  autoMigrate?: boolean      // Enable automatic hot→cold migration
                             // Default: true
  migrationInterval?: number // Migrate every N blocks (when at chain tip)
                             // Default: 30
  migrationOnFinality?: boolean // Migrate when finality advances (instead of block count)
                             // Default: false (use block count)
  migrationHooks?: MigrationHooks // Optional hooks for custom migration behavior

  // Hot blocks staleness configuration
  staleHotBlocksThresholdMs?: number // How long indexer can be down before hot blocks are considered stale
                             // Default: 600000 (10 minutes)
                             // Set to 0 to always clear hot blocks on restart
                             // Set to Infinity to never clear hot blocks (trust Subsquid's reorg detection)
  trustHotBlocksOnQuickRestart?: boolean // If true, trust hot blocks if downtime < threshold
                             // Default: true
                             // If false, always clear hot blocks on restart (old behavior)
}
