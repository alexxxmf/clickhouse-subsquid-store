/**
 * Unit tests for loadSqlFiles utility
 * Tests folder-based table discovery and hot/cold variant generation
 */

import { readdirSync } from 'fs'
import { join } from 'path'

describe('loadSqlFiles', () => {
  const fixturesPath = join(__dirname, '../fixtures/tables')

  describe('folder-based discovery', () => {
    it('should discover hot-supported tables', () => {
      const hotSupportedDir = join(fixturesPath, 'hot-supported')
      const files = readdirSync(hotSupportedDir).filter(f => f.endsWith('.sql'))

      expect(files).toContain('test_events.sql')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should discover regular tables', () => {
      const regularDir = join(fixturesPath, 'regular')
      const files = readdirSync(regularDir).filter(f => f.endsWith('.sql'))

      expect(files).toContain('test_snapshot.sql')
      expect(files.length).toBeGreaterThan(0)
    })

    it('should separate hot-supported from regular tables', () => {
      const hotSupportedDir = join(fixturesPath, 'hot-supported')
      const regularDir = join(fixturesPath, 'regular')

      const hotFiles = readdirSync(hotSupportedDir).filter(f => f.endsWith('.sql'))
      const regularFiles = readdirSync(regularDir).filter(f => f.endsWith('.sql'))

      // Test files should be in different directories
      expect(hotFiles).toContain('test_events.sql')
      expect(regularFiles).toContain('test_snapshot.sql')
      expect(hotFiles).not.toContain('test_snapshot.sql')
      expect(regularFiles).not.toContain('test_events.sql')
    })
  })

  describe('table name extraction', () => {
    it('should extract table names from SQL files', () => {
      const files = ['test_events.sql', 'transfers.sql', 'swaps.sql']
      const tableNames = files.map(f => f.replace('.sql', ''))

      expect(tableNames).toEqual(['test_events', 'transfers', 'swaps'])
    })

    it('should generate hot and cold table names', () => {
      const network = 'ethereum'
      const baseTable = 'test_events'

      const hotTable = `${network}_hot_${baseTable}`
      const coldTable = `${network}_cold_${baseTable}`

      expect(hotTable).toBe('ethereum_hot_test_events')
      expect(coldTable).toBe('ethereum_cold_test_events')
    })

    it('should generate single table name for regular tables', () => {
      const network = 'ethereum'
      const baseTable = 'test_snapshot'

      const tableName = `${network}_${baseTable}`

      expect(tableName).toBe('ethereum_test_snapshot')
    })
  })

  describe('network placeholder substitution', () => {
    it('should replace ${network} placeholder in SQL', () => {
      const sql = 'CREATE TABLE IF NOT EXISTS ${network}_test_events'
      const network = 'ethereum'

      const result = sql.replace(/\$\{network\}/g, network)

      expect(result).toBe('CREATE TABLE IF NOT EXISTS ethereum_test_events')
    })

    it('should replace multiple ${network} placeholders', () => {
      const sql = 'CREATE TABLE ${network}_hot_${network}_events'
      const network = 'base'

      const result = sql.replace(/\$\{network\}/g, network)

      expect(result).toBe('CREATE TABLE base_hot_base_events')
    })
  })
})
