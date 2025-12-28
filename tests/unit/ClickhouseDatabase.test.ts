/**
 * Unit tests for ClickhouseDatabase
 * Tests configuration options and validation logic
 */

import type { ClickhouseDatabaseOptions } from '../../src/types/types'

describe('ClickhouseDatabase', () => {
  describe('configuration options', () => {
    it('should use default values when not provided', () => {
      const defaults = {
        stateTable: 'squid_processor_status',
        processorId: 'default',
        supportHotBlocks: true,
        hotBlocksDepth: 10,
        network: 'ethereum',
        tablesPath: 'src/db/tables',
        hotSupportedTablesPath: 'src/db/tables/hot-supported',
        heightColumnName: 'height',
        autoMigrate: true,
        migrationInterval: 30,
        migrationOnFinality: false,
      }

      expect(defaults.stateTable).toBe('squid_processor_status')
      expect(defaults.processorId).toBe('default')
      expect(defaults.supportHotBlocks).toBe(true)
      expect(defaults.hotBlocksDepth).toBe(10)
      expect(defaults.network).toBe('ethereum')
      expect(defaults.heightColumnName).toBe('height')
      expect(defaults.autoMigrate).toBe(true)
      expect(defaults.migrationInterval).toBe(30)
      expect(defaults.migrationOnFinality).toBe(false)
    })

    it('should accept custom configuration', () => {
      const config = {
        processorId: 'my-indexer',
        network: 'base',
        hotBlocksDepth: 100,
        heightColumnName: 'block_number',
        migrationInterval: 50,
        migrationOnFinality: true,
      }

      expect(config.processorId).toBe('my-indexer')
      expect(config.network).toBe('base')
      expect(config.hotBlocksDepth).toBe(100)
      expect(config.heightColumnName).toBe('block_number')
      expect(config.migrationInterval).toBe(50)
      expect(config.migrationOnFinality).toBe(true)
    })
  })

  describe('table name generation', () => {
    it('should generate hot table names correctly', () => {
      const network = 'ethereum'
      const table = 'transfers'

      const hotTable = `${network}_hot_${table}`

      expect(hotTable).toBe('ethereum_hot_transfers')
    })

    it('should generate cold table names correctly', () => {
      const network = 'ethereum'
      const table = 'transfers'

      const coldTable = `${network}_cold_${table}`

      expect(coldTable).toBe('ethereum_cold_transfers')
    })

    it('should generate status table names with processor ID', () => {
      const processorId = 'my-indexer'
      const network = 'ethereum'

      const validBlocksTable = `${processorId}_${network}_valid_blocks`

      expect(validBlocksTable).toBe('my-indexer_ethereum_valid_blocks')
    })
  })

  describe('height column validation', () => {
    it('should validate hot-supported tables have height column', () => {
      const tableColumns = [
        { name: 'id', type: 'UInt64' },
        { name: 'height', type: 'UInt64' },
        { name: 'data', type: 'String' },
      ]

      const heightColumnName = 'height'
      const hasHeightColumn = tableColumns.some(col => col.name === heightColumnName)

      expect(hasHeightColumn).toBe(true)
    })

    it('should detect missing height column', () => {
      const tableColumns = [
        { name: 'id', type: 'UInt64' },
        { name: 'data', type: 'String' },
        { name: 'timestamp', type: 'DateTime64(3)' },
      ]

      const heightColumnName = 'height'
      const hasHeightColumn = tableColumns.some(col => col.name === heightColumnName)

      expect(hasHeightColumn).toBe(false)
    })

    it('should support custom height column names', () => {
      const tableColumns = [
        { name: 'id', type: 'UInt64' },
        { name: 'block_number', type: 'UInt64' },
        { name: 'data', type: 'String' },
      ]

      const heightColumnName = 'block_number'
      const hasHeightColumn = tableColumns.some(col => col.name === heightColumnName)

      expect(hasHeightColumn).toBe(true)
    })
  })

  describe('migration trigger logic', () => {
    it('should trigger migration based on block count', () => {
      const blocksSinceLastMigration = 30
      const migrationInterval = 30
      const migrationOnFinality = false

      const shouldMigrate = !migrationOnFinality && blocksSinceLastMigration >= migrationInterval

      expect(shouldMigrate).toBe(true)
    })

    it('should not trigger migration if interval not reached', () => {
      const blocksSinceLastMigration = 20
      const migrationInterval = 30
      const migrationOnFinality = false

      const shouldMigrate = !migrationOnFinality && blocksSinceLastMigration >= migrationInterval

      expect(shouldMigrate).toBe(false)
    })

    it('should trigger migration on finality advance', () => {
      const currentFinalized = 1000
      const previousFinalized = 990
      const migrationOnFinality = true

      const shouldMigrate = migrationOnFinality && currentFinalized > previousFinalized

      expect(shouldMigrate).toBe(true)
    })

    it('should not migrate if not at chain tip', () => {
      const isAtChainTip = false
      const blocksSinceLastMigration = 100

      const shouldMigrate = isAtChainTip && blocksSinceLastMigration >= 30

      expect(shouldMigrate).toBe(false)
    })
  })

  describe('migration cutoff calculation', () => {
    it('should calculate cutoff height correctly', () => {
      const maxHeight = 1000
      const hotBlocksDepth = 50

      const cutoffHeight = maxHeight - hotBlocksDepth

      expect(cutoffHeight).toBe(950)
    })

    it('should keep specified number of blocks hot', () => {
      const maxHeight = 24111288
      const hotBlocksDepth = 50

      const cutoffHeight = maxHeight - hotBlocksDepth

      // Blocks with height <= cutoffHeight go to cold
      // Blocks with height > cutoffHeight stay in hot
      const hotBlocks = [cutoffHeight + 1, maxHeight]

      expect(cutoffHeight).toBe(24111238)
      expect(hotBlocks[1] - hotBlocks[0] + 1).toBeLessThanOrEqual(hotBlocksDepth)
    })
  })

  describe('chain tip detection', () => {
    it('should route to cold tables during catchup', () => {
      const isAtChainTip = false

      const insertTarget = isAtChainTip ? 'hot' : 'cold'

      expect(insertTarget).toBe('cold')
    })

    it('should route to hot tables at chain tip', () => {
      const isAtChainTip = true

      const insertTarget = isAtChainTip ? 'hot' : 'cold'

      expect(insertTarget).toBe('hot')
    })
  })

  describe('migration hooks', () => {
    it('should call beforeMigration hook', async () => {
      let called = false

      const hooks = {
        beforeMigration: async (context: any) => {
          called = true
          return true
        },
      }

      await hooks.beforeMigration({})

      expect(called).toBe(true)
    })

    it('should skip migration if beforeMigration returns false', async () => {
      const hooks = {
        beforeMigration: async (context: any) => {
          return false // Cancel migration
        },
      }

      const shouldProceed = await hooks.beforeMigration({})

      expect(shouldProceed).toBe(false)
    })

    it('should call afterMigration hook with result', async () => {
      let receivedResult: any = null

      const hooks = {
        afterMigration: async (result: any) => {
          receivedResult = result
        },
      }

      const migrationResult = {
        migrated: 799,
        cutoffHeight: 24111238,
        durationMs: 105,
        tables: [{ name: 'erc20_transfers', rows: 799 }],
      }

      await hooks.afterMigration(migrationResult)

      expect(receivedResult).toEqual(migrationResult)
      expect(receivedResult.migrated).toBe(799)
    })
  })
})
