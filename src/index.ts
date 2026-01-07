/**
 * @subsquid/clickhouse-store
 *
 * Production-ready ClickHouse adapter for Subsquid EVM processors
 * with hot/cold table architecture and automatic reorg handling.
 *
 * Created by Alex Moro Fernandez
 *
 * @packageDocumentation
 */

// Core classes
export { ClickhouseDatabase } from './core/ClickhouseDatabase'
export { ClickhouseStore } from './core/ClickhouseStore'
export { ValidBlocksManager } from './core/ValidBlocksManager'

// Utilities
export { loadSqlFiles, verifyTables } from './utils/loadSqlFiles'
export { globalMetrics, measureAsync } from './utils/metrics'

// Type exports
export type {
  // Database types
  BlockRef,
  HashAndHeight,
  DatabaseState,
  StatusRow,
  ClickhouseDatabaseOptions,

  // Transaction types
  FinalTxInfo,
  HotTxInfo,

  // Migration types
  MigrationResult,
  MigrationContext,
  MigrationHooks,
  TransformRowsContext,

  // Store types
  Store,
} from './types'
