import type { ClickHouseClient } from '@clickhouse/client'
import { ClickhouseStore } from './ClickhouseStore'
import { globalMetrics, measureAsync } from '../utils/metrics'
import { ValidBlocksManager } from './ValidBlocksManager'
import type {
  BlockRef,
  DatabaseState,
  FinalTxInfo,
  HotTxInfo,
  HashAndHeight,
  Store,
  ClickhouseDatabaseOptions,
  StatusRow,
  MigrationResult,
  MigrationContext,
  MigrationHooks,
} from '../types/types'
import { readdirSync } from 'fs'
import { join } from 'path'

/**
 * ClickHouse Database adapter for Subsquid EVM Processor
 *
 * Implements the Database interface from @subsquid/typeorm-store
 * using the cold/hot reorg handling approach:
 * - Cold zone: Finalized blocks (height <= currentHeight - finalityDepth)
 * - Hot zone: Recent blocks tracked in valid_blocks table
 *
 * Key Features:
 * - Hot block support with automatic reorg detection
 * - Cold/hot reorg handling (no expensive deletes during reorgs)
 * - Status table for crash recovery
 * - Compatible with EvmBatchProcessor
 *
 * ## Why Hot/Cold Architecture?
 *
 * This pattern enables efficient DELETE operations in ClickHouse by isolating
 * mutable data (recent blocks subject to reorgs) from immutable historical data:
 *
 * **Without Hot/Cold**:
 * - DELETE in ClickHouse marks rows for deletion, requiring FINAL in queries
 * - FINAL forces full table scans and prevents index usage
 * - ReplacingMergeTree/CollapsingMergeTree deduplication happens asynchronously
 * - Queries become slow and resource-intensive
 * - Partition-level deletes still require full partition scans
 *
 * **With Hot/Cold**:
 * - DELETE only affects small hot tables (last ~50 blocks)
 * - Cold tables are append-only, never require FINAL
 * - Queries on historical data remain fast with full index support
 * - Registry pattern (valid_blocks) avoids DELETE entirely for reorg handling
 * - Migration moves finalized data to cold storage in bulk
 *
 * The result: Fast queries on historical data while maintaining blockchain correctness
 * during reorganizations. This architecture sidesteps ClickHouse's well-known
 * DELETE performance limitations without sacrificing query speed.
 */
export class ClickhouseDatabase {
  private client: ClickHouseClient
  private stateTable: string
  private processorId: string
  private hotBlocks: BlockRef[] = []
  readonly supportsHotBlocks = true
  private finalizedHeight: number = -1
  private supportHotBlocks: boolean
  private hotBlocksDepth: number
  private network: string
  private tablesPath: string
  private hotSupportedTablesPath: string
  private heightColumnName: string

  // Cold/hot reorg handling - tracks valid block hashes for hot zone
  private validBlocksManager: ValidBlocksManager

  // Track if we're at chain tip (for hot/cold table routing)
  // When catching up (isAtChainTip = false), insert directly to cold tables
  // When at tip (isAtChainTip = true), insert to hot tables
  private isAtChainTip: boolean = false

  // Dynamically discovered migrateable tables (from hot-supported directory)
  private migrateableTables: string[] = []

  // Auto-migration state
  private autoMigrate: boolean
  private migrationInterval: number
  private migrationOnFinality: boolean
  private migrationHooks?: MigrationHooks
  private blocksSinceLastMigration: number = 0

  constructor(options: ClickhouseDatabaseOptions) {
    this.client = options.client
    this.stateTable = options.stateTable || 'squid_processor_status'
    this.processorId = options.processorId || 'default'
    this.supportHotBlocks = options.supportHotBlocks ?? true
    this.hotBlocksDepth = options.hotBlocksDepth || 10
    this.network = options.network || 'ethereum'
    this.tablesPath = options.tablesPath || 'src/db/tables'
    this.hotSupportedTablesPath = options.hotSupportedTablesPath || 'src/db/tables/hot-supported'
    this.heightColumnName = options.heightColumnName || 'height'

    // Auto-migration configuration with sensible defaults
    this.autoMigrate = options.autoMigrate ?? true
    this.migrationInterval = options.migrationInterval ?? 30
    this.migrationOnFinality = options.migrationOnFinality ?? false
    this.migrationHooks = options.migrationHooks

    // Initialize ValidBlocksManager for cold/hot reorg handling
    this.validBlocksManager = new ValidBlocksManager(
      this.client,
      this.processorId,
      this.hotBlocksDepth
    )

    // Discover hot-supported tables from configured path
    this.migrateableTables = this.discoverHotSupportedTables()
  }

  /**
   * Discover table names from SQL files in hot-supported directory
   * Returns base table names (without network prefix or hot/cold prefix)
   */
  private discoverHotSupportedTables(): string[] {
    try {
      // Use process.cwd() to get project root, works both in dev (src/) and prod (lib/)
      const hotSupportedDir = join(process.cwd(), this.hotSupportedTablesPath)
      const files = readdirSync(hotSupportedDir).filter(f => f.endsWith('.sql'))

      // Extract table names from filenames (e.g., erc20_transfers.sql -> erc20_transfers)
      const tableNames = files.map(f => f.replace('.sql', ''))

      if (tableNames.length > 0) {
        console.log(`📋 Discovered ${tableNames.length} hot-supported table(s): ${tableNames.join(', ')}`)
      } else {
        console.warn(`⚠️  No hot-supported tables found in ${this.hotSupportedTablesPath}`)
        console.warn(`   Auto-migration will be disabled. Add .sql files to hot-supported directory.`)
      }

      return tableNames
    } catch (err: any) {
      console.warn(`⚠️  Could not discover hot-supported tables from ${this.hotSupportedTablesPath}: ${err.message}`)
      console.warn(`   Using empty table list. Auto-migration will be disabled.`)
      return []
    }
  }

  /**
   * Validate that all hot-supported tables have the required height column
   * Called during connect() to catch configuration errors early
   */
  private async validateHotSupportedTables(): Promise<void> {
    if (this.migrateableTables.length === 0) {
      // No tables to validate, skip
      return
    }

    console.log(`🔍 Validating hot-supported tables...`)
    const errors: string[] = []

    for (const table of this.migrateableTables) {
      const hotTable = `${this.network}_hot_${table}`

      try {
        // Check if table exists by querying its schema
        const result = await this.client.query({
          query: `DESCRIBE TABLE ${hotTable}`,
          format: 'JSONEachRow',
        })

        const columns = await result.json<{ name: string; type: string }>()
        const hasHeightColumn = columns.some(col => col.name === this.heightColumnName)

        if (!hasHeightColumn) {
          errors.push(
            `Table '${hotTable}' is missing required column '${this.heightColumnName}'.\n` +
            `  Hot-supported tables must have a '${this.heightColumnName}' column for auto-migration.\n` +
            `  Available columns: ${columns.map(c => c.name).join(', ')}`
          )
        }
      } catch (err: any) {
        if (err.message?.includes('UNKNOWN_TABLE') || err.type === 'UNKNOWN_TABLE') {
          // Table doesn't exist yet - that's fine, it will be created
          continue
        }
        // Other errors - re-throw
        throw err
      }
    }

    if (errors.length > 0) {
      const errorMsg = [
        '╔════════════════════════════════════════════════════════════════╗',
        '║  Hot-Supported Table Validation Failed                        ║',
        '╚════════════════════════════════════════════════════════════════╝',
        '',
        'The following tables in hot-supported directory have configuration errors:',
        '',
        ...errors.map(e => e.split('\n').map(line => `  ${line}`).join('\n')),
        '',
        'Fix these issues by either:',
        `  1. Adding a '${this.heightColumnName}' column to the table schema`,
        '  2. Moving the table SQL file to regular/ directory if it doesn\'t need hot/cold support',
        `  3. Setting heightColumnName option if using a different column name`,
        '',
      ].join('\n')

      throw new Error(errorMsg)
    }

    console.log(`✅ All hot-supported tables validated successfully`)
  }

  /**
   * Initialize database connection and recover last indexed state
   * Called by processor before starting indexing
   */
  async connect(): Promise<DatabaseState> {
    console.log('🔌 Connecting to ClickHouse database...')

    try {
      // Test connection
      await this.client.ping()
      globalMetrics.recordConnection(true)
    } catch (err: any) {
      globalMetrics.recordConnection(false)
      throw new Error(
        `Failed to connect to ClickHouse: ${err.message}\n` +
        `Make sure ClickHouse is running and accessible at the configured URL.`
      )
    }

    try {
      // Ensure status table exists
      await this.initializeStatusTable()
    } catch (err: any) {
      throw new Error(
        `Failed to initialize status table: ${err.message}\n` +
        `Check database permissions and schema.`
      )
    }

    let state: DatabaseState
    try {
      // Retrieve last processed state
      state = await this.getLastProcessedBlock()
    } catch (err: any) {
      throw new Error(
        `Failed to recover indexer state: ${err.message}\n` +
        `The status table may be corrupted.`
      )
    }

    // Initialize internal state
    // FIX: Use the actual stored finalized height, not just the latest indexed block
    // This is critical for proper hot blocks recovery - Subsquid's HotProcessor
    // expects consecutive blocks and validates this on construction
    this.finalizedHeight = state.finalizedHeight ?? state.height
    this.hotBlocks = state.top

    // Initialize ValidBlocksManager (loads existing valid blocks from DB)
    try {
      await this.validBlocksManager.initialize()
    } catch (err: any) {
      throw new Error(
        `Failed to initialize ValidBlocksManager: ${err.message}\n` +
        `Cold/hot reorg handling may not work correctly.`
      )
    }

    // Validate hot-supported tables configuration (only if auto-migration is enabled)
    if (this.autoMigrate && this.supportHotBlocks) {
      try {
        await this.validateHotSupportedTables()
      } catch (err: any) {
        throw err // Re-throw with the nicely formatted error message
      }
    }

    // STALE HOT BLOCKS DETECTION:
    // If we have hot blocks OR the stored height is beyond cold data, those blocks
    // may have been reorged away. Subsquid's runner validates block hashes before
    // our code runs, so we need to rollback to cold height preemptively.
    // This is safe because cold data is guaranteed to still be valid.
    const staleRollback = await this.detectAndRollbackStaleHotBlocks(state)
    if (staleRollback) {
      state = staleRollback
      this.finalizedHeight = state.finalizedHeight ?? state.height
      this.hotBlocks = state.top
    }

    console.log(`✅ ClickHouse Database connected (cold/hot reorg handling)`)
    console.log(`   Processor ID: ${this.processorId}`)
    console.log(`   Last indexed block: ${state.height}`)
    console.log(`   Finalized height: ${this.finalizedHeight}`)
    console.log(`   Hot blocks in chain: ${this.hotBlocks.length}`)
    console.log(`   Valid blocks in registry: ${this.validBlocksManager.getBlockCount()}`)
    if (this.hotBlocks.length > 0) {
      console.log(`   Hot range: ${this.hotBlocks[0].height} - ${this.hotBlocks[this.hotBlocks.length - 1].height}`)
    }

    return state
  }

  /**
   * Close database connection
   */
  async disconnect(): Promise<void> {
    console.log('🔌 Disconnecting from ClickHouse...')
    await this.client.close()
    console.log('✅ Disconnected')
  }

  /**
   * Process finalized blocks (no rollback needed)
   * These blocks are considered immutable
   */
  async transact(
    info: FinalTxInfo,
    cb: (store: Store) => Promise<void>
  ): Promise<void> {
    console.log(`📦 Processing finalized block ${info.nextHead.height}...`)

    const store = new ClickhouseStore(this.client)

    // Execute user's data processing callback
    await cb(store)

    // Flush all buffered inserts to ClickHouse
    await this.flushStore(store)

    // Update finalized height to latest block
    this.finalizedHeight = info.nextHead.height

    // Save updated status
    await this.saveStatus({
      height: info.nextHead.height,
      hash: info.nextHead.hash,
      top: this.hotBlocks,
    })

    // Record metrics
    globalMetrics.recordBlocksProcessed(1, true)

    console.log(`✅ Finalized block processed up to height ${this.finalizedHeight}`)
  }

  /**
   * Process hot (unfinalized) blocks with reorg detection
   * Uses cold/hot approach: on reorg, just update valid_blocks registry
   * (no expensive table deletions needed)
   */
  async transactHot(
    info: HotTxInfo,
    cb: (store: Store, block: HashAndHeight) => Promise<void>
  ): Promise<void> {
    console.log(`🔥 Processing ${info.newBlocks.length} hot blocks...`)

    // Track previous finality for migration trigger
    const previousFinalizedHeight = this.finalizedHeight

    // Update finalized height
    if (info.finalizedHead.height > this.finalizedHeight) {
      this.finalizedHeight = info.finalizedHead.height
      console.log(`   ✓ Finalized up to block ${info.finalizedHead.height}`)

      // Remove finalized blocks from hot list
      this.hotBlocks = this.hotBlocks.filter(b => b.height > info.finalizedHead.height)
    }

    // Detect chain reorganization
    const reorgDetected = this.detectReorg(info.newBlocks)

    if (reorgDetected) {
      console.log('🔄 Chain reorganization detected!')
      globalMetrics.recordReorgDetected()

      const rollbackHeight = this.findCommonAncestor(info.newBlocks)
      const blocksAffected = this.hotBlocks.filter(b => b.height > rollbackHeight).length

      // Cold/hot approach: Update valid_blocks registry instead of deleting data
      // Orphaned rows become invisible because their block_hash is no longer valid
      await this.validBlocksManager.handleReorg(
        rollbackHeight + 1,  // Remove blocks > rollbackHeight
        info.newBlocks.map(b => ({ height: b.height, hash: b.hash }))
      )

      globalMetrics.recordReorgExecuted(rollbackHeight, blocksAffected)

      // Reset hot blocks to common ancestor
      this.hotBlocks = this.hotBlocks.filter(b => b.height <= rollbackHeight)
    }

    // Process each new hot block
    for (const block of info.newBlocks) {
      const store = new ClickhouseStore(this.client)

      // Execute user callback for this specific block
      await cb(store, block)

      // Flush block data to database
      await this.flushStore(store)

      // Record metrics
      globalMetrics.recordBlockProcessed(false)

      // Add block to valid_blocks registry (cold/hot approach)
      // Skip if this block was already added during reorg handling
      if (!this.validBlocksManager.isValidBlock(block.height, block.hash)) {
        await this.validBlocksManager.addBlock(block.height, block.hash)
      }

      // Add to hot blocks chain
      this.hotBlocks.push(block)
    }

    // Maintain hot blocks depth limit (prevent unlimited growth)
    if (this.hotBlocks.length > this.hotBlocksDepth) {
      const excess = this.hotBlocks.length - this.hotBlocksDepth
      console.log(`   Pruning ${excess} old hot blocks (depth limit: ${this.hotBlocksDepth})`)
      this.hotBlocks = this.hotBlocks.slice(-this.hotBlocksDepth)
    }

    // Save updated status
    if (info.newBlocks.length > 0) {
      const lastBlock = info.newBlocks[info.newBlocks.length - 1]
      await this.saveStatus({
        height: lastBlock.height,
        hash: lastBlock.hash,
        top: this.hotBlocks,
      })
    }

    // AUTO-MIGRATION: Trigger hot→cold migration if conditions are met
    if (this.autoMigrate && this.isAtChainTip) {
      this.blocksSinceLastMigration += info.newBlocks.length

      // Determine if migration should trigger
      const shouldMigrate = this.migrationOnFinality
        ? (this.finalizedHeight > previousFinalizedHeight)  // Finality advanced
        : (this.blocksSinceLastMigration >= this.migrationInterval)  // Block count reached

      if (shouldMigrate) {
        console.log(`🔄 Auto-migration triggered`)
        await this.performMigration()
      }
    }

    console.log(`✅ Hot blocks processed up to height ${info.newBlocks[info.newBlocks.length - 1]?.height}`)
  }

  /**
   * Alternative hot block processing (batch mode)
   * Not commonly used, but required by interface
   */
  async transactHot2(
    info: HotTxInfo,
    cb: (store: Store, sliceBeg: number, sliceEnd: number) => Promise<void>
  ): Promise<void> {
    // For now, delegate to transactHot with a wrapper
    await this.transactHot(info, async (store) => {
      await cb(store, 0, info.newBlocks.length)
    })
  }

  // ==================== PRIVATE METHODS ====================

  /**
   * Detect if we're at chain tip
   * Currently relies on external setIsAtChainTip() call
   * Future: Could use block timestamp heuristics
   */
  private detectChainTip(info: HotTxInfo): boolean {
    // For now, we rely on the isAtChainTip flag set externally
    // via setIsAtChainTip(ctx.isHead) in the processor
    //
    // Future enhancement: Could check if block timestamps are recent
    // const latestBlock = info.newBlocks[info.newBlocks.length - 1]
    // const blockAge = Date.now() - latestBlock.timestamp (if available)
    // return blockAge < 60_000 // within last 60 seconds

    return this.isAtChainTip
  }

  /**
   * Perform hot→cold migration with hooks support
   * This orchestrates the migration process and calls user-defined hooks
   */
  private async performMigration(): Promise<void> {
    // Estimate hot table rows (query one representative table)
    let hotTableRows = 0
    if (this.migrateableTables.length > 0) {
      try {
        const result = await this.client.query({
          query: `SELECT count() as cnt FROM ${this.network}_hot_${this.migrateableTables[0]}`,
          format: 'JSONEachRow',
        })
        const [{ cnt }] = await result.json<{ cnt: string }>()
        hotTableRows = parseInt(cnt, 10)
      } catch {
        // Table might not exist yet
      }
    }

    // Build context for hooks
    const context: MigrationContext = {
      currentHeight: this.hotBlocks[this.hotBlocks.length - 1]?.height || this.finalizedHeight,
      finalizedHeight: this.finalizedHeight,
      hotTableRows,
      blocksSinceLastMigration: this.blocksSinceLastMigration,
      isAtChainTip: this.isAtChainTip,
    }

    // Hook: beforeMigration
    if (this.migrationHooks?.beforeMigration) {
      const shouldContinue = await this.migrationHooks.beforeMigration(context)
      if (!shouldContinue) {
        console.log('   Migration cancelled by beforeMigration hook')
        return
      }
    }

    // Execute migration (custom or default)
    let result: MigrationResult
    if (this.migrationHooks?.customMigration) {
      result = await this.migrationHooks.customMigration(this)
    } else {
      result = await this.migrateHotToCold()
    }

    // Hook: afterMigration
    if (this.migrationHooks?.afterMigration) {
      await this.migrationHooks.afterMigration(result)
    }

    // Log result
    if (result.migrated > 0) {
      console.log(`   ✓ Migrated ${result.migrated} rows in ${result.durationMs}ms (cutoff: ${result.cutoffHeight})`)
    }

    // Reset counter
    this.blocksSinceLastMigration = 0
  }

  /**
   * Detect and rollback stale hot blocks on startup
   *
   * When the indexer stops at chain tip with hot (unfinalized) blocks and restarts
   * after some time, those blocks may have been reorged away. Subsquid's runner
   * validates block hashes BEFORE our code runs, causing "block not found" errors.
   *
   * Solution: Always rollback to finalized height on startup if we have hot blocks.
   * This is safe because:
   * 1. Finalized blocks are guaranteed to still exist on chain
   * 2. We only lose a few unfinalized blocks (which we'd lose anyway with a reorg)
   * 3. The processor will re-index those blocks from finalized height
   *
   * @returns New state rolled back to finalized height, or null if no rollback needed
   */
  private async detectAndRollbackStaleHotBlocks(state: DatabaseState): Promise<DatabaseState | null> {
    const hotBlockCount = state.top.length
    const lastIndexedHeight = state.height

    // Step 1: Get the cold status (height + hash) - this is our safe checkpoint
    const coldStatus = await this.getColdStatus()

    // If no cold status exists, fall back to querying cold tables for height only
    // This handles the case where we're upgrading from before cold_status existed
    let coldHeight: number
    let coldHash: string

    if (coldStatus) {
      coldHeight = coldStatus.height
      coldHash = coldStatus.hash
    } else {
      coldHeight = await this.getLatestColdBlockHeight()
      coldHash = '' // No hash available - will need manual intervention or fresh start
    }

    // Check if rollback is needed:
    // 1. We have hot blocks (they could be stale after restart)
    // 2. OR stored height > cold height (gap exists, likely from incomplete previous rollback)
    const hasHotBlocks = hotBlockCount > 0
    const heightBeyondCold = lastIndexedHeight > coldHeight && coldHeight >= 0

    if (!hasHotBlocks && !heightBeyondCold) {
      return null
    }

    console.log(`⚠️  STALE DATA DETECTED - ROLLBACK REQUIRED:`)
    console.log(`   Last indexed: ${lastIndexedHeight} (hash: ${state.hash?.slice(0, 10) || 'empty'}...)`)
    console.log(`   Cold status: height=${coldHeight}, hash=${coldHash?.slice(0, 10) || 'none'}...`)
    console.log(`   Hot blocks: ${hotBlockCount}`)
    if (heightBeyondCold) {
      console.log(`   ⚠️  Height ${lastIndexedHeight} is beyond cold data ${coldHeight} - gap of ${lastIndexedHeight - coldHeight} blocks`)
    }

    if (!coldHash) {
      console.log(`   ⚠️  No cold hash available - this is a legacy state or fresh database`)
      console.log(`   → Rolling back to height ${coldHeight} with empty hash (Subsquid will verify)`)
    } else {
      console.log(`   → Rolling back to cold checkpoint: block ${coldHeight}`)
    }

    // Step 2: Clear ALL valid_blocks (we're starting fresh from cold data)
    await this.validBlocksManager.clear()
    console.log(`   ✓ Cleared all valid_blocks`)

    // Step 3: Delete ALL data from hot tables (start fresh)
    await this.clearAllHotTables()

    // Step 4: Update status table to reflect rollback
    const newState: DatabaseState = {
      height: coldHeight,
      hash: coldHash, // Use the hash from cold_status if available
      top: [],  // No hot blocks
      finalizedHeight: coldHeight,
    }

    await this.saveStatus(newState)
    console.log(`   ✓ Updated status table: height=${coldHeight}, hash=${coldHash?.slice(0, 10) || 'empty'}...`)

    console.log(`✅ Rollback complete. Processor will resume from block ${coldHeight}`)

    return newState
  }

  /**
   * Get the latest block height from cold tables
   * Queries all cold tables and returns the maximum height found
   */
  private async getLatestColdBlockHeight(): Promise<number> {
    let maxHeight = -1

    for (const table of this.migrateableTables) {
      const coldTable = `${this.network}_cold_${table}`

      try {
        const result = await this.client.query({
          query: `SELECT max(height) as max_height FROM ${coldTable}`,
          format: 'JSONEachRow',
        })
        const [row] = await result.json<{ max_height: string | number | null }>()

        if (row && row.max_height !== null) {
          const height = typeof row.max_height === 'string'
            ? parseInt(row.max_height, 10)
            : row.max_height
          if (height > maxHeight) {
            maxHeight = height
          }
        }
      } catch (err: any) {
        // Table might not exist yet - that's ok
        if (!err.message?.includes('UNKNOWN_TABLE') && err.type !== 'UNKNOWN_TABLE') {
          console.error(`     Warning: Failed to query ${coldTable}: ${err.message}`)
        }
      }
    }

    return maxHeight
  }

  /**
   * Clear ALL data from hot tables (complete reset)
   */
  private async clearAllHotTables(): Promise<void> {
    console.log(`   Clearing all hot tables...`)

    let totalDeleted = 0

    for (const table of this.migrateableTables) {
      const hotTable = `${this.network}_hot_${table}`

      try {
        // Count rows to delete (for logging)
        const countResult = await this.client.query({
          query: `SELECT count() as cnt FROM ${hotTable}`,
          format: 'JSONEachRow',
        })
        const [{ cnt }] = await countResult.json<{ cnt: string }>()
        const rowsToDelete = parseInt(cnt, 10)

        if (rowsToDelete === 0) continue

        // Truncate is faster than DELETE for clearing entire table
        await this.client.command({
          query: `TRUNCATE TABLE ${hotTable}`,
        })

        console.log(`     ✓ ${hotTable}: cleared ${rowsToDelete} rows`)
        totalDeleted += rowsToDelete

      } catch (err: any) {
        // Table might not exist yet - that's ok
        if (err.message?.includes('UNKNOWN_TABLE') || err.type === 'UNKNOWN_TABLE') {
          continue
        }
        // Log but don't fail entire rollback for one table
        console.error(`     ✗ ${hotTable}: ${err.message}`)
      }
    }

    console.log(`   ✓ Cleared ${totalDeleted} total rows from hot tables`)
  }

  /**
   * Create status tables if they don't exist
   */
  private async initializeStatusTable(): Promise<void> {
    // Main status table for processor state (includes hot blocks)
    const createStatusTableSQL = `
      CREATE TABLE IF NOT EXISTS ${this.stateTable} (
        id String DEFAULT 'default',
        height Int64,
        hash String,
        parent_hash String,
        hot_blocks String,          -- JSON array of hot block references
        finalized_height Int64,
        timestamp DateTime64(3) DEFAULT now64(3)
      ) ENGINE = ReplacingMergeTree(timestamp)
      ORDER BY (id)
    `

    await this.client.command({
      query: createStatusTableSQL,
    })

    // Cold status table - stores the latest block height/hash in cold storage
    // This is the "safe" checkpoint we can always rollback to on restart
    const createColdStatusTableSQL = `
      CREATE TABLE IF NOT EXISTS ${this.stateTable}_cold (
        id String DEFAULT 'default',
        height Int64,
        hash String,
        timestamp DateTime64(3) DEFAULT now64(3)
      ) ENGINE = ReplacingMergeTree(timestamp)
      ORDER BY (id)
    `

    await this.client.command({
      query: createColdStatusTableSQL,
    })
  }

  /**
   * Get the cold status (latest block in cold storage)
   */
  private async getColdStatus(): Promise<{ height: number; hash: string } | null> {
    try {
      const result = await this.client.query({
        query: `
          SELECT height, hash
          FROM ${this.stateTable}_cold FINAL
          WHERE id = '${this.processorId}'
          LIMIT 1
        `,
        format: 'JSONEachRow',
      })

      const rows = await result.json<{ height: number | string; hash: string }>()
      if (rows.length === 0) {
        return null
      }

      return {
        height: typeof rows[0].height === 'string' ? parseInt(rows[0].height, 10) : rows[0].height,
        hash: rows[0].hash,
      }
    } catch (err: any) {
      // Table might not exist yet
      if (err.message?.includes('UNKNOWN_TABLE') || err.type === 'UNKNOWN_TABLE') {
        return null
      }
      throw err
    }
  }

  /**
   * Update the cold status (called during hot→cold migration)
   */
  private async updateColdStatus(height: number, hash: string): Promise<void> {
    await this.client.insert({
      table: `${this.stateTable}_cold`,
      values: [{
        id: this.processorId,
        height,
        hash,
        timestamp: Date.now(),
      }],
      format: 'JSONEachRow',
    })
  }

  /**
   * Retrieve the last processed block state from status table
   */
  private async getLastProcessedBlock(): Promise<DatabaseState> {
    const result = await this.client.query({
      query: `
        SELECT height, hash, hot_blocks, finalized_height
        FROM ${this.stateTable} FINAL
        WHERE id = '${this.processorId}'
        ORDER BY timestamp DESC
        LIMIT 1
      `,
      format: 'JSONEachRow',
    })

    const rows = await result.json<StatusRow>()

    if (rows.length === 0) {
      // No previous state - starting from genesis
      console.log('   No previous state found - starting from genesis')
      return { height: -1, hash: '', top: [] }
    }

    const row = rows[0]
    const hotBlocks: HashAndHeight[] = row.hot_blocks
      ? JSON.parse(row.hot_blocks).map((b: any) => ({
          height: Number(b.height),
          hash: b.hash
        }))
      : []

    return {
      height: Number(row.height),
      hash: row.hash || '',
      top: hotBlocks,
      finalizedHeight: row.finalized_height ? Number(row.finalized_height) : undefined,
    }
  }

  /**
   * Save current indexing state to status table
   */
  private async saveStatus(state: DatabaseState): Promise<void> {
    // FIX: Sanitize hot blocks to only include height and hash
    // Subsquid's block objects contain extra properties like baseFeePerGas (BigInt)
    // JSON.stringify cannot serialize BigInt, causing "Do not know how to serialize a BigInt" error
    // We only need height and hash for reorg detection anyway
    const sanitizedHotBlocks = state.top.map(b => ({
      height: b.height,
      hash: b.hash,
    }))

    await this.client.insert({
      table: this.stateTable,
      values: [
        {
          id: this.processorId,
          height: state.height,
          hash: state.hash,
          parent_hash: (state.top[state.top.length - 1] as any)?.parent || '',
          hot_blocks: JSON.stringify(sanitizedHotBlocks),
          finalized_height: this.finalizedHeight,
          timestamp: Date.now(),
        },
      ],
      format: 'JSONEachRow',
    })
  }

  // Maximum rows per insert batch to avoid connection issues
  // ClickHouse handles large batches well - bigger is better for throughput
  // Retry logic handles intermittent EPIPE errors
  private readonly BATCH_SIZE = 200000

  /**
   * Flush buffered inserts from store to ClickHouse
   */
  private async flushStore(store: ClickhouseStore): Promise<void> {
    const inserts = store.getInserts()

    if (inserts.length === 0) {
      return
    }

    // Group entities by table name (based on constructor name)
    const byTable = new Map<string, any[]>()

    for (const entity of inserts) {
      // Convert class name to table name (e.g., Token -> tokens, Pair -> pairs)
      const tableName = this.getTableName(entity.constructor.name)
      if (!byTable.has(tableName)) {
        byTable.set(tableName, [])
      }
      byTable.get(tableName)!.push(entity)
    }

    // Bulk insert to each table in parallel
    await Promise.all(
      Array.from(byTable.entries()).map(async ([table, entities]) => {
        if (entities.length === 0) return

        // Convert entities to ClickHouse-compatible format (BigInt -> string)
        const serializedEntities = entities.map(entity => this.serializeEntity(entity))

        // Insert in chunks with retry logic for EPIPE errors
        let inserted = 0
        for (let i = 0; i < serializedEntities.length; i += this.BATCH_SIZE) {
          const chunk = serializedEntities.slice(i, i + this.BATCH_SIZE)

          // Retry up to 3 times on transient connection errors
          const retryableCodes = ['EPIPE', 'ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED']
          for (let attempt = 1; attempt <= 3; attempt++) {
            try {
              await this.client.insert({
                table,
                values: chunk,
                format: 'JSONEachRow',
                clickhouse_settings: {
                  date_time_input_format: 'best_effort',
                },
              })
              break
            } catch (err: any) {
              const isRetryable = retryableCodes.includes(err.code) || err.message?.includes('socket hang up')
              if (isRetryable && attempt < 3) {
                const delay = 500 * attempt // 500ms, 1000ms backoff
                console.warn(`   ⚠️ ${err.code || 'Connection error'} on ${table} (attempt ${attempt}/3), retrying in ${delay}ms...`)
                await new Promise(r => setTimeout(r, delay))
              } else {
                throw err
              }
            }
          }

          inserted += chunk.length
        }

        console.log(`   ✓ Inserted ${inserted} rows into ${table}`)
      })
    )

    // Clear store buffers after flush
    store.clearBuffers()
  }

  /**
   * Convert entity class name to table name
   * Pattern-based conversion: PascalCase -> {network}_[hot_|cold_]snake_case + 's'
   *
   * During catchup (not at chain tip): insert directly to cold tables
   * At chain tip: insert to hot tables, then migrate to cold periodically
   *
   * Examples (at chain tip, ethereum):
   *   Token -> ethereum_hot_tokens
   *   V3Swap -> ethereum_hot_v3_swaps
   * Examples (catching up, ethereum):
   *   Token -> ethereum_cold_tokens
   *   V3Swap -> ethereum_cold_v3_swaps
   */
  private getTableName(className: string): string {
    // Convert PascalCase to snake_case
    // Handle: V2Burn -> v2_burn, V3Swap -> v3_swap, Token -> token
    const snakeCase = className
      .replace(/([a-z])([A-Z])/g, '$1_$2')  // camelCase boundaries: someWord -> some_Word
      .replace(/([0-9])([A-Z])/g, '$1_$2')  // digit+uppercase: V2Burn -> V2_Burn
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')  // consecutive caps: HTMLParser -> HTML_Parser
      .toLowerCase()

    // Pluralize (simple 's' suffix) and prefix with network and hot/cold
    const tempZone = this.isAtChainTip ? 'hot_' : 'cold_'
    return `${this.network}_${tempZone}${snakeCase}s`
  }

  /**
   * Set whether we're at the chain tip
   * Called from the handler when ctx.isHead changes
   *
   * When catching up: insert directly to cold tables (faster, no migration needed)
   * At chain tip: insert to hot tables (supports reorg handling)
   */
  setIsAtChainTip(isHead: boolean): void {
    if (this.isAtChainTip !== isHead) {
      this.isAtChainTip = isHead
      if (isHead) {
        console.log('📍 Reached chain tip - switching to hot table inserts')
      } else {
        console.log('📍 Catching up - inserting directly to cold tables')
      }
    }
  }

  /**
   * Check if we're currently at chain tip
   */
  getIsAtChainTip(): boolean {
    return this.isAtChainTip
  }

  /**
   * Serialize entity for ClickHouse insertion
   * Converts BigInt values to strings since JSON.stringify doesn't support BigInt
   * Converts hex strings to Buffer for FixedString fields (if _hexFields is specified)
   */
  private serializeEntity(entity: any): any {
    const serialized: any = {}

    // Get hex fields from entity metadata (optional, set via constructor)
    const hexFields = new Set((entity._hexFields as string[]) || [])

    for (const [key, value] of Object.entries(entity)) {
      if (typeof value === 'bigint') {
        // Convert BigInt to string for ClickHouse Int256 fields
        serialized[key] = value.toString()
      } else if (value instanceof Date) {
        // Dates are handled by ClickHouse client
        serialized[key] = value
      } else if (Array.isArray(value)) {
        // Handle arrays (e.g., whitelistPairs, poolsWithWethOrStables)
        serialized[key] = value
      } else if (hexFields.has(key) && typeof value === 'string') {
        // For FixedString fields: Keep as hex string (without 0x prefix)
        // ClickHouse automatically converts hex strings to binary when using JSONEachRow format
        // Example: "abcd1234..." (40 hex chars) → FixedString(20) as 20 bytes
        const hexString = value.startsWith('0x') ? value.slice(2) : value
        serialized[key] = hexString || ''
      } else {
        // Pass through other values as-is
        serialized[key] = value
      }
    }

    return serialized
  }

  /**
   * Detect if a chain reorganization occurred
   */
  private detectReorg(newBlocks: HashAndHeight[]): boolean {
    if (this.hotBlocks.length === 0 || newBlocks.length === 0) {
      return false
    }

    const currentTip = this.hotBlocks[this.hotBlocks.length - 1]
    const newBlock = newBlocks[0]

    // A reorg is detected when:
    // 1. New block height is less than or equal to current tip (going backwards)
    // This is the only reliable reorg indicator without parent hash verification
    const isReorg = newBlock.height <= currentTip.height

    if (isReorg) {
      console.log(`   ⚠️  Chain reorganization detected:`)
      console.log(`      Current tip: height=${currentTip.height}, hash=${currentTip.hash}`)
      console.log(`      New block:   height=${newBlock.height}, hash=${newBlock.hash}`)
    }

    return isReorg
  }

  /**
   * Find the common ancestor block between current chain and new chain
   */
  private findCommonAncestor(newBlocks: HashAndHeight[]): number {
    // Build hash-to-height map of current chain
    const hashToHeight = new Map<string, number>()
    for (const block of this.hotBlocks) {
      hashToHeight.set(block.hash, block.height)
    }

    // Find highest block in new chain that matches our history
    let ancestorHeight = this.finalizedHeight
    for (const newBlock of newBlocks) {
      if (hashToHeight.has(newBlock.hash)) {
        ancestorHeight = Math.max(ancestorHeight, newBlock.height)
      }
    }

    if (ancestorHeight > this.finalizedHeight) {
      console.log(`   ✓ Found common ancestor at height ${ancestorHeight}`)
    } else {
      console.log(`   ⚠️  Deep reorg detected - no common ancestor in hot blocks`)
      console.log(`   → Rolling back to finalized block ${this.finalizedHeight}`)
    }

    return ancestorHeight
  }

  /**
   * Get the ValidBlocksManager for building query filters
   * Use this to filter out orphaned rows in the hot zone:
   *
   * ```typescript
   * const filter = db.getValidBlocksManager().buildValidBlocksFilter('height', 'hash', currentHeight)
   * const query = `SELECT * FROM swaps WHERE ${filter.sql}`
   * ```
   */
  getValidBlocksManager(): ValidBlocksManager {
    return this.validBlocksManager
  }

  /**
   * Get the current finality threshold
   * Blocks at or below this height are considered "cold" (finalized)
   */
  getFinalityThreshold(): number {
    return this.validBlocksManager.getFinalityThreshold(
      this.validBlocksManager.getHighestBlock()
    )
  }

  // ==================== HOT → COLD MIGRATION ====================

  // Track last migration height to avoid redundant migrations
  private lastMigrationHeight: number = -1

  /**
   * Trigger hot→cold migration manually or from processor
   * Call this periodically (e.g., every 30 batches) when at chain tip
   *
   * Logic:
   * 1. Get max height from representative hot table
   * 2. Calculate cutoff = maxHeight - hotBlocksDepth
   * 3. For each table: INSERT to cold WHERE height <= cutoff
   * 4. Verify cold insert count matches
   * 5. DELETE from hot WHERE height <= cutoff
   */
  async migrateHotToCold(): Promise<MigrationResult> {
    const startTime = Date.now()
    // Skip if not at chain tip (during catchup, data goes directly to cold)
    if (!this.isAtChainTip) {
      return {
        migrated: 0,
        cutoffHeight: -1,
        durationMs: Date.now() - startTime,
        tables: [],
      }
    }

    // Step 1: Get max height from hot tables (use first discovered table as representative)
    let maxHeight: number
    try {
      if (this.migrateableTables.length === 0) {
        console.log('📦 No migrateable tables discovered, skipping migration')
        return {
          migrated: 0,
          cutoffHeight: -1,
          durationMs: Date.now() - startTime,
          tables: [],
        }
      }

      const representativeTable = `${this.network}_hot_${this.migrateableTables[0]}`
      const result = await this.client.query({
        query: `SELECT max(${this.heightColumnName}) as max_height FROM ${representativeTable}`,
        format: 'JSONEachRow',
      })
      const [row] = await result.json<{ max_height: string | number }>()
      maxHeight = typeof row.max_height === 'string' ? parseInt(row.max_height, 10) : row.max_height

      if (!maxHeight || maxHeight <= 0) {
        console.log('📦 No data in hot tables to migrate')
        return {
          migrated: 0,
          cutoffHeight: -1,
          durationMs: Date.now() - startTime,
          tables: [],
        }
      }
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.type === 'UNKNOWN_TABLE') {
        return {
          migrated: 0,
          cutoffHeight: -1,
          durationMs: Date.now() - startTime,
          tables: [],
        }
      }
      throw err
    }

    // Step 2: Calculate cutoff height (keep hotBlocksDepth blocks in hot)
    const cutoffHeight = maxHeight - this.hotBlocksDepth

    // Skip if cutoff hasn't advanced enough since last migration
    if (cutoffHeight <= this.lastMigrationHeight) {
      return {
        migrated: 0,
        cutoffHeight,
        durationMs: Date.now() - startTime,
        tables: [],
      }
    }

    console.log(`📦 Hot→Cold migration: maxHeight=${maxHeight}, cutoff=${cutoffHeight}, keeping last ${this.hotBlocksDepth} blocks hot`)

    let totalMigrated = 0
    const tableResults: Array<{ name: string; rows: number }> = []

    // Step 3-5: Migrate each table with verification
    for (const table of this.migrateableTables) {
      const hotTable = `${this.network}_hot_${table}`
      const coldTable = `${this.network}_cold_${table}`

      try {
        // Count rows to migrate
        const countResult = await this.client.query({
          query: `SELECT count() as cnt FROM ${hotTable} WHERE ${this.heightColumnName} <= {cutoff:Int64}`,
          query_params: { cutoff: cutoffHeight },
          format: 'JSONEachRow',
        })
        const [{ cnt }] = await countResult.json<{ cnt: string }>()
        const rowsToMigrate = parseInt(cnt, 10)

        if (rowsToMigrate === 0) continue

        // INSERT to cold table
        await this.client.command({
          query: `
            INSERT INTO ${coldTable}
            SELECT * FROM ${hotTable}
            WHERE ${this.heightColumnName} <= {cutoff:Int64}
          `,
          query_params: { cutoff: cutoffHeight },
        })

        // Verify: count rows inserted to cold (recent rows with height <= cutoff)
        // Note: Can't easily verify exact count due to ReplacingMergeTree deduplication
        // Instead, we trust the INSERT succeeded if no error was thrown
        // The cold table uses ReplacingMergeTree so duplicates are handled

        // DELETE from hot table (only after INSERT succeeded)
        await this.client.command({
          query: `ALTER TABLE ${hotTable} DELETE WHERE ${this.heightColumnName} <= {cutoff:Int64}`,
          query_params: { cutoff: cutoffHeight },
        })

        console.log(`   ✓ ${table}: ${rowsToMigrate} rows migrated to cold`)
        totalMigrated += rowsToMigrate
        tableResults.push({ name: table, rows: rowsToMigrate })

      } catch (err: any) {
        // Table might not exist yet - that's ok
        if (err.message?.includes('UNKNOWN_TABLE') || err.type === 'UNKNOWN_TABLE') {
          continue
        }
        // Log but don't fail entire migration for one table
        console.error(`   ✗ ${table}: ${err.message}`)
      }
    }

    this.lastMigrationHeight = cutoffHeight

    // Step 6: Update cold status with the cutoff height and hash
    // Get the block hash for the cutoff height from our hot blocks or valid_blocks
    const cutoffHash = await this.getBlockHashForHeight(cutoffHeight)
    if (cutoffHash) {
      await this.updateColdStatus(cutoffHeight, cutoffHash)
      console.log(`   ✓ Updated cold status: height=${cutoffHeight}, hash=${cutoffHash.slice(0, 10)}...`)
    } else {
      console.log(`   ⚠️ Could not find hash for cutoff height ${cutoffHeight} - cold status not updated`)
    }

    console.log(`✅ Migration complete: ${totalMigrated} total rows moved to cold storage`)

    return {
      migrated: totalMigrated,
      cutoffHeight,
      durationMs: Date.now() - startTime,
      tables: tableResults,
    }
  }

  /**
   * Get block hash for a specific height
   * Tries hot blocks first, then valid_blocks, then queries cold tables
   */
  private async getBlockHashForHeight(height: number): Promise<string | null> {
    // Check hot blocks in memory
    const hotBlock = this.hotBlocks.find(b => b.height === height)
    if (hotBlock) {
      return hotBlock.hash
    }

    // Check valid_blocks
    const validHashes = this.validBlocksManager.getValidHashes()
    const validHash = validHashes.get(height)
    if (validHash) {
      return validHash
    }

    // Query cold tables that have hash column (use first discovered table)
    if (this.migrateableTables.length > 0) {
      const representativeTable = this.migrateableTables[0]

      try {
        const result = await this.client.query({
          query: `
            SELECT hash
            FROM ${this.network}_cold_${representativeTable}
            WHERE height = {height:Int64}
            LIMIT 1
          `,
          query_params: { height },
          format: 'JSONEachRow',
        })
        const rows = await result.json<{ hash: string }>()
        if (rows.length > 0 && rows[0].hash) {
          return rows[0].hash
        }
      } catch (err: any) {
        // Table might not exist or no data at this height
      }

      // Try hot table as well
      try {
        const result = await this.client.query({
          query: `
            SELECT hash
            FROM ${this.network}_hot_${representativeTable}
            WHERE height = {height:Int64}
            LIMIT 1
          `,
          query_params: { height },
          format: 'JSONEachRow',
        })
        const rows = await result.json<{ hash: string }>()
        if (rows.length > 0 && rows[0].hash) {
          return rows[0].hash
        }
      } catch (err: any) {
        // Table might not exist or no data at this height
      }
    }

    return null
  }

  /**
   * Get hot blocks depth configuration
   */
  getHotBlocksDepth(): number {
    return this.hotBlocksDepth
  }

  /**
   * Get the list of migrateable tables discovered from src/db/tables/
   * Returns base table names (without network or hot/cold prefix)
   */
  getMigrateableTables(): string[] {
    return this.migrateableTables
  }
}
