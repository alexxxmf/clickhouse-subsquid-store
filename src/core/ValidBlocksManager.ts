/**
 * ValidBlocksManager - Manages valid block hashes for cold/hot reorg handling
 *
 * This class tracks which block hashes are valid in the "hot zone" (recent blocks
 * that can be reorganized). Instead of deleting orphaned rows during reorgs,
 * we update this registry and filter queries by valid hashes.
 *
 * Architecture:
 * - Cold zone (finalized): Blocks older than finality threshold - always valid
 * - Hot zone (mutable): Recent blocks - filtered by valid_blocks table
 *
 * Usage:
 * ```typescript
 * const manager = new ValidBlocksManager(client, 'amm-v2', 30)
 *
 * // On new block
 * await manager.addBlock(block.height, block.hash, block.timestamp)
 *
 * // On reorg
 * await manager.handleReorg(reorgHeight, newBlocks)
 *
 * // For queries (get valid hashes for filtering)
 * const validHashes = await manager.getValidHashes()
 * ```
 */

import type { ClickHouseClient } from '@clickhouse/client'

/**
 * Convert a Date to ClickHouse DateTime64(3) compatible format
 * ClickHouse expects: '2023-05-01 12:00:00.000' (no T separator, no Z suffix)
 */
function toClickHouseTimestamp(date: Date): string {
  return date.toISOString().slice(0, 23).replace('T', ' ')
}

export interface BlockRef {
  height: number
  hash: string
  timestamp?: Date
}

export class ValidBlocksManager {
  private client: ClickHouseClient
  private processorId: string
  private finalityDepth: number
  private tableName: string

  // In-memory cache of valid blocks (synced with DB)
  private validBlocks: Map<number, string> = new Map()

  constructor(
    client: ClickHouseClient,
    processorId: string = 'default',
    finalityDepth: number = 30,
    tableName: string = 'valid_blocks'
  ) {
    this.client = client
    this.processorId = processorId
    this.finalityDepth = finalityDepth
    this.tableName = tableName
  }

  /**
   * Initialize the manager by loading existing valid blocks from DB
   */
  async initialize(): Promise<void> {
    // Ensure table exists
    await this.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          height UInt64,
          hash String,
          timestamp DateTime64(3),
          processor_id String DEFAULT 'default'
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (processor_id, height)
        SETTINGS index_granularity = 8192
      `,
    })

    // Load existing valid blocks into memory
    const result = await this.client.query({
      query: `
        SELECT height, hash
        FROM ${this.tableName} FINAL
        WHERE processor_id = {processor_id:String}
        ORDER BY height
      `,
      query_params: { processor_id: this.processorId },
      format: 'JSONEachRow',
    })

    const rows = await result.json<{ height: string | number; hash: string }>()

    this.validBlocks.clear()
    for (const row of rows) {
      this.validBlocks.set(Number(row.height), row.hash)
    }

    console.log(`ValidBlocksManager: Loaded ${this.validBlocks.size} valid blocks for processor '${this.processorId}'`)
  }

  /**
   * Add a new block to the valid blocks registry
   * Called when processing a new block
   */
  async addBlock(height: number, hash: string, timestamp?: Date): Promise<void> {
    // Add to in-memory cache
    this.validBlocks.set(height, hash)

    // Insert into DB
    await this.client.insert({
      table: this.tableName,
      values: [{
        height,
        hash,
        timestamp: toClickHouseTimestamp(timestamp || new Date()),
        processor_id: this.processorId,
      }],
      format: 'JSONEachRow',
    })

    // Prune old blocks beyond finality depth
    await this.pruneOldBlocks(height)
  }

  /**
   * Add multiple blocks at once (batch insert)
   */
  async addBlocks(blocks: BlockRef[]): Promise<void> {
    if (blocks.length === 0) return

    // Add to in-memory cache
    for (const block of blocks) {
      this.validBlocks.set(block.height, block.hash)
    }

    // Batch insert into DB
    await this.client.insert({
      table: this.tableName,
      values: blocks.map(b => ({
        height: b.height,
        hash: b.hash,
        timestamp: toClickHouseTimestamp(b.timestamp || new Date()),
        processor_id: this.processorId,
      })),
      format: 'JSONEachRow',
    })

    // Prune old blocks
    const maxHeight = Math.max(...blocks.map(b => b.height))
    await this.pruneOldBlocks(maxHeight)
  }

  /**
   * Handle a chain reorganization
   * Removes blocks at and above reorgHeight, then adds new blocks
   */
  async handleReorg(reorgHeight: number, newBlocks: BlockRef[]): Promise<void> {
    console.log(`ValidBlocksManager: Handling reorg at height ${reorgHeight}`)
    console.log(`  Removing blocks >= ${reorgHeight}`)
    console.log(`  Adding ${newBlocks.length} new blocks`)

    // Remove invalidated blocks from memory
    for (const [height] of this.validBlocks) {
      if (height >= reorgHeight) {
        this.validBlocks.delete(height)
      }
    }

    // Delete invalidated blocks from DB
    // Note: Using ALTER TABLE DELETE for immediate deletion
    await this.client.command({
      query: `
        ALTER TABLE ${this.tableName}
        DELETE WHERE processor_id = {processor_id:String} AND height >= {height:UInt64}
      `,
      query_params: {
        processor_id: this.processorId,
        height: reorgHeight,
      },
    })

    // Add new blocks
    if (newBlocks.length > 0) {
      await this.addBlocks(newBlocks)
    }

    console.log(`ValidBlocksManager: Reorg handled, ${this.validBlocks.size} valid blocks remaining`)
  }

  /**
   * Get all valid block hashes (for query filtering)
   * Returns a Map of height -> hash
   */
  getValidHashes(): Map<number, string> {
    return new Map(this.validBlocks)
  }

  /**
   * Get valid hashes as an array of tuples (for ClickHouse IN clause)
   */
  getValidHashesArray(): Array<[number, string]> {
    return Array.from(this.validBlocks.entries())
  }

  /**
   * Get the current finality threshold height
   */
  getFinalityThreshold(currentHeight: number): number {
    return currentHeight - this.finalityDepth
  }

  /**
   * Check if a block hash is valid
   */
  isValidBlock(height: number, hash: string): boolean {
    const validHash = this.validBlocks.get(height)
    return validHash === hash
  }

  /**
   * Get the highest valid block height
   */
  getHighestBlock(): number {
    if (this.validBlocks.size === 0) return -1
    return Math.max(...this.validBlocks.keys())
  }

  /**
   * Get the lowest valid block height
   */
  getLowestBlock(): number {
    if (this.validBlocks.size === 0) return -1
    return Math.min(...this.validBlocks.keys())
  }

  /**
   * Get count of valid blocks
   */
  getBlockCount(): number {
    return this.validBlocks.size
  }

  /**
   * Remove blocks older than finality threshold
   * These blocks are now "cold" and don't need to be in the registry
   */
  private async pruneOldBlocks(currentHeight: number): Promise<void> {
    const threshold = currentHeight - this.finalityDepth

    // Remove from memory
    let pruned = 0
    for (const [height] of this.validBlocks) {
      if (height < threshold) {
        this.validBlocks.delete(height)
        pruned++
      }
    }

    if (pruned > 0) {
      // Delete from DB
      await this.client.command({
        query: `
          ALTER TABLE ${this.tableName}
          DELETE WHERE processor_id = {processor_id:String} AND height < {threshold:UInt64}
        `,
        query_params: {
          processor_id: this.processorId,
          threshold,
        },
      })

      console.log(`ValidBlocksManager: Pruned ${pruned} finalized blocks (threshold: ${threshold})`)
    }
  }

  /**
   * Build a WHERE clause fragment for filtering hot zone data
   * Use this in queries to filter out orphaned rows
   *
   * @param heightColumn - Name of the height column in the table
   * @param hashColumn - Name of the block hash column in the table
   * @param currentHeight - Current blockchain height
   * @returns SQL WHERE clause fragment
   */
  buildValidBlocksFilter(
    heightColumn: string = 'height',
    hashColumn: string = 'hash',
    currentHeight: number
  ): { sql: string; params: Record<string, any> } {
    const threshold = this.getFinalityThreshold(currentHeight)
    const validHashes = this.getValidHashesArray()

    // If no valid hashes, only return cold zone data
    if (validHashes.length === 0) {
      return {
        sql: `${heightColumn} <= {finality_threshold:Int64}`,
        params: { finality_threshold: threshold },
      }
    }

    // Build tuple array for IN clause
    const tuples = validHashes.map(([h, hash]) => `(${h}, '${hash}')`).join(', ')

    return {
      sql: `(
        ${heightColumn} <= {finality_threshold:Int64}
        OR
        (${heightColumn}, ${hashColumn}) IN (${tuples})
      )`,
      params: { finality_threshold: threshold },
    }
  }

  /**
   * Clear all valid blocks (for testing or reset)
   */
  async clear(): Promise<void> {
    this.validBlocks.clear()

    await this.client.command({
      query: `
        ALTER TABLE ${this.tableName}
        DELETE WHERE processor_id = {processor_id:String}
      `,
      query_params: { processor_id: this.processorId },
    })

    console.log(`ValidBlocksManager: Cleared all valid blocks for processor '${this.processorId}'`)
  }
}
