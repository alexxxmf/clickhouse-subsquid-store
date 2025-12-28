/**
 * Basic Example: ERC20 Transfer Indexer
 *
 * This example demonstrates how to use @subsquid/clickhouse-store
 * to index ERC20 transfers with automatic hot/cold table management.
 */

import 'dotenv/config'
import { ClickhouseDatabase } from '@subsquid/clickhouse-store'
import { createClient } from '@clickhouse/client'
import { EvmBatchProcessor } from '@subsquid/evm-processor'
import * as erc20Abi from './abi/erc20'

// Create ClickHouse client
const client = createClient({
  url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
  database: process.env.CLICKHOUSE_DATABASE || 'transfers',
})

// Create database adapter with hot/cold support
const database = new ClickhouseDatabase({
  client,
  processorId: 'erc20-transfers',
  network: 'ethereum',
  supportHotBlocks: true,
  hotBlocksDepth: 50, // Keep last 50 blocks in hot tables

  // Auto-migrate every 30 blocks at chain tip
  autoMigrate: true,
  migrationInterval: 30,

  // Optional: verbose logging
  migrationHooks: {
    beforeMigration: async (ctx) => {
      console.log(`📊 Migration check: ${ctx.blocksSinceLastMigration} blocks`)
      return true // Proceed with migration
    },
    afterMigration: async (result) => {
      console.log(`✅ Migrated ${result.migrated} rows in ${result.durationMs}ms`)
    },
  },
})

// Create processor
const processor = new EvmBatchProcessor()
  .setGateway('https://v2.archive.subsquid.io/network/ethereum-mainnet')
  .setRpcEndpoint({
    url: process.env.ETH_RPC_URL || 'https://rpc.ankr.com/eth',
    rateLimit: 10,
  })
  .setFinalityConfirmation(75)
  .setBlockRange({ from: 10_000_000 })
  .addLog({
    topic0: [erc20Abi.events.Transfer.topic],
    transaction: true,
  })

// Define Transfer entity
interface Transfer {
  token: string
  from: string
  to: string
  value: bigint
  height: number
  block_timestamp: Date
  tx_hash: string
  log_index: number
}

// Run processor
processor.run(database, async (ctx) => {
  // Update hot/cold routing based on chain position
  database.setIsAtChainTip(ctx.isHead)

  // Extract transfers from blocks
  const transfers: Transfer[] = []

  for (const block of ctx.blocks) {
    for (const log of block.logs) {
      if (log.topics[0] === erc20Abi.events.Transfer.topic) {
        const decoded = erc20Abi.events.Transfer.decode(log)

        transfers.push({
          token: log.address.toLowerCase(),
          from: decoded.from.toLowerCase(),
          to: decoded.to.toLowerCase(),
          value: decoded.value,
          height: block.header.height,
          block_timestamp: new Date(block.header.timestamp),
          tx_hash: log.transactionHash,
          log_index: log.logIndex,
        })
      }
    }
  }

  // Save to ClickHouse (automatically routed to hot or cold tables)
  if (transfers.length > 0) {
    await ctx.store.insert(transfers)
    console.log(`Processed ${transfers.length} transfers at block ${ctx.blocks[ctx.blocks.length - 1].header.height}`)
  }

  // Migration happens automatically based on migrationInterval
})
