/**
 * Metrics Collection for ClickHouse Database
 *
 * Tracks performance metrics and can export to Prometheus or logs
 */

export interface DatabaseMetrics {
  // Connection metrics
  connections: number
  connectionErrors: number
  lastConnectionTime: number

  // Block processing metrics
  blocksProcessed: number
  blocksFinalized: number
  hotBlocksProcessed: number

  // Reorg metrics
  reorgsDetected: number
  reorgsExecuted: number
  lastReorgHeight: number
  lastReorgTime: number
  totalBlocksRolledBack: number

  // Insert metrics
  totalInserts: number
  insertErrors: number
  lastInsertDuration: number
  avgInsertDuration: number

  // Query metrics
  totalQueries: number
  queryErrors: number
  lastQueryDuration: number
  avgQueryDuration: number

  // Status updates
  statusUpdates: number
  lastStatusUpdate: number
}

export class MetricsCollector {
  private metrics: DatabaseMetrics = {
    connections: 0,
    connectionErrors: 0,
    lastConnectionTime: 0,
    blocksProcessed: 0,
    blocksFinalized: 0,
    hotBlocksProcessed: 0,
    reorgsDetected: 0,
    reorgsExecuted: 0,
    lastReorgHeight: 0,
    lastReorgTime: 0,
    totalBlocksRolledBack: 0,
    totalInserts: 0,
    insertErrors: 0,
    lastInsertDuration: 0,
    avgInsertDuration: 0,
    totalQueries: 0,
    queryErrors: 0,
    lastQueryDuration: 0,
    avgQueryDuration: 0,
    statusUpdates: 0,
    lastStatusUpdate: 0,
  }

  private insertDurations: number[] = []
  private queryDurations: number[] = []
  private maxSampleSize = 100 // Keep last 100 samples for averaging

  // Connection metrics
  recordConnection(success: boolean) {
    if (success) {
      this.metrics.connections++
      this.metrics.lastConnectionTime = Date.now()
    } else {
      this.metrics.connectionErrors++
    }
  }

  // Block processing metrics
  recordBlockProcessed(isFinalized: boolean) {
    this.metrics.blocksProcessed++
    if (isFinalized) {
      this.metrics.blocksFinalized++
    } else {
      this.metrics.hotBlocksProcessed++
    }
  }

  recordBlocksProcessed(count: number, isFinalized: boolean) {
    this.metrics.blocksProcessed += count
    if (isFinalized) {
      this.metrics.blocksFinalized += count
    } else {
      this.metrics.hotBlocksProcessed += count
    }
  }

  // Reorg metrics
  recordReorgDetected() {
    this.metrics.reorgsDetected++
  }

  recordReorgExecuted(rollbackHeight: number, blocksAffected: number) {
    this.metrics.reorgsExecuted++
    this.metrics.lastReorgHeight = rollbackHeight
    this.metrics.lastReorgTime = Date.now()
    this.metrics.totalBlocksRolledBack += blocksAffected
  }

  // Insert metrics
  recordInsert(durationMs: number, success: boolean) {
    this.metrics.totalInserts++

    if (success) {
      this.metrics.lastInsertDuration = durationMs
      this.insertDurations.push(durationMs)

      if (this.insertDurations.length > this.maxSampleSize) {
        this.insertDurations.shift()
      }

      this.metrics.avgInsertDuration =
        this.insertDurations.reduce((a, b) => a + b, 0) / this.insertDurations.length
    } else {
      this.metrics.insertErrors++
    }
  }

  // Query metrics
  recordQuery(durationMs: number, success: boolean) {
    this.metrics.totalQueries++

    if (success) {
      this.metrics.lastQueryDuration = durationMs
      this.queryDurations.push(durationMs)

      if (this.queryDurations.length > this.maxSampleSize) {
        this.queryDurations.shift()
      }

      this.metrics.avgQueryDuration =
        this.queryDurations.reduce((a, b) => a + b, 0) / this.queryDurations.length
    } else {
      this.metrics.queryErrors++
    }
  }

  // Status update metrics
  recordStatusUpdate() {
    this.metrics.statusUpdates++
    this.metrics.lastStatusUpdate = Date.now()
  }

  // Get current metrics
  getMetrics(): DatabaseMetrics {
    return { ...this.metrics }
  }

  // Get metrics as Prometheus format
  toPrometheusFormat(): string {
    const m = this.metrics
    const lines: string[] = []

    // Helper to add metric
    const addMetric = (name: string, value: number, help: string, type: string = 'counter') => {
      lines.push(`# HELP clickhouse_${name} ${help}`)
      lines.push(`# TYPE clickhouse_${name} ${type}`)
      lines.push(`clickhouse_${name} ${value}`)
      lines.push('')
    }

    // Connection metrics
    addMetric('connections_total', m.connections, 'Total successful connections')
    addMetric('connection_errors_total', m.connectionErrors, 'Total connection errors')
    addMetric('last_connection_timestamp', m.lastConnectionTime / 1000, 'Last connection timestamp', 'gauge')

    // Block processing metrics
    addMetric('blocks_processed_total', m.blocksProcessed, 'Total blocks processed')
    addMetric('blocks_finalized_total', m.blocksFinalized, 'Total finalized blocks')
    addMetric('hot_blocks_processed_total', m.hotBlocksProcessed, 'Total hot blocks processed')

    // Reorg metrics
    addMetric('reorgs_detected_total', m.reorgsDetected, 'Total reorgs detected')
    addMetric('reorgs_executed_total', m.reorgsExecuted, 'Total reorgs executed')
    addMetric('last_reorg_height', m.lastReorgHeight, 'Height of last reorg', 'gauge')
    addMetric('last_reorg_timestamp', m.lastReorgTime / 1000, 'Timestamp of last reorg', 'gauge')
    addMetric('blocks_rolled_back_total', m.totalBlocksRolledBack, 'Total blocks rolled back')

    // Insert metrics
    addMetric('inserts_total', m.totalInserts, 'Total insert operations')
    addMetric('insert_errors_total', m.insertErrors, 'Total insert errors')
    addMetric('last_insert_duration_ms', m.lastInsertDuration, 'Last insert duration in ms', 'gauge')
    addMetric('avg_insert_duration_ms', m.avgInsertDuration, 'Average insert duration in ms', 'gauge')

    // Query metrics
    addMetric('queries_total', m.totalQueries, 'Total query operations')
    addMetric('query_errors_total', m.queryErrors, 'Total query errors')
    addMetric('last_query_duration_ms', m.lastQueryDuration, 'Last query duration in ms', 'gauge')
    addMetric('avg_query_duration_ms', m.avgQueryDuration, 'Average query duration in ms', 'gauge')

    // Status metrics
    addMetric('status_updates_total', m.statusUpdates, 'Total status table updates')
    addMetric('last_status_update_timestamp', m.lastStatusUpdate / 1000, 'Last status update timestamp', 'gauge')

    return lines.join('\n')
  }

  // Get metrics as JSON
  toJSON(): string {
    return JSON.stringify(this.metrics, null, 2)
  }

  // Print metrics summary to console
  printSummary() {
    const m = this.metrics

    console.log('\n📊 ClickHouse Database Metrics Summary')
    console.log('═'.repeat(70))

    console.log('\n🔌 Connection Metrics:')
    console.log(`   Total Connections: ${m.connections}`)
    console.log(`   Connection Errors: ${m.connectionErrors}`)
    console.log(`   Last Connection: ${new Date(m.lastConnectionTime).toISOString()}`)

    console.log('\n📦 Block Processing:')
    console.log(`   Total Blocks: ${m.blocksProcessed}`)
    console.log(`   Finalized: ${m.blocksFinalized}`)
    console.log(`   Hot Blocks: ${m.hotBlocksProcessed}`)

    console.log('\n🔄 Reorg Metrics:')
    console.log(`   Reorgs Detected: ${m.reorgsDetected}`)
    console.log(`   Reorgs Executed: ${m.reorgsExecuted}`)
    console.log(`   Last Reorg Height: ${m.lastReorgHeight}`)
    if (m.lastReorgTime > 0) {
      console.log(`   Last Reorg Time: ${new Date(m.lastReorgTime).toISOString()}`)
    }
    console.log(`   Total Blocks Rolled Back: ${m.totalBlocksRolledBack}`)

    console.log('\n💾 Insert Performance:')
    console.log(`   Total Inserts: ${m.totalInserts}`)
    console.log(`   Insert Errors: ${m.insertErrors}`)
    console.log(`   Avg Duration: ${m.avgInsertDuration.toFixed(2)} ms`)
    console.log(`   Last Duration: ${m.lastInsertDuration.toFixed(2)} ms`)

    console.log('\n🔍 Query Performance:')
    console.log(`   Total Queries: ${m.totalQueries}`)
    console.log(`   Query Errors: ${m.queryErrors}`)
    console.log(`   Avg Duration: ${m.avgQueryDuration.toFixed(2)} ms`)
    console.log(`   Last Duration: ${m.lastQueryDuration.toFixed(2)} ms`)

    console.log('\n📝 Status Updates: ' + m.statusUpdates)

    console.log('═'.repeat(70) + '\n')
  }

  // Reset all metrics
  reset() {
    this.metrics = {
      connections: 0,
      connectionErrors: 0,
      lastConnectionTime: 0,
      blocksProcessed: 0,
      blocksFinalized: 0,
      hotBlocksProcessed: 0,
      reorgsDetected: 0,
      reorgsExecuted: 0,
      lastReorgHeight: 0,
      lastReorgTime: 0,
      totalBlocksRolledBack: 0,
      totalInserts: 0,
      insertErrors: 0,
      lastInsertDuration: 0,
      avgInsertDuration: 0,
      totalQueries: 0,
      queryErrors: 0,
      lastQueryDuration: 0,
      avgQueryDuration: 0,
      statusUpdates: 0,
      lastStatusUpdate: 0,
    }
    this.insertDurations = []
    this.queryDurations = []
  }
}

// Global metrics collector instance
export const globalMetrics = new MetricsCollector()

// Utility function to measure operation duration
export async function measureAsync<T>(
  operation: () => Promise<T>,
  onComplete: (durationMs: number, success: boolean) => void
): Promise<T> {
  const start = Date.now()
  try {
    const result = await operation()
    const duration = Date.now() - start
    onComplete(duration, true)
    return result
  } catch (err) {
    const duration = Date.now() - start
    onComplete(duration, false)
    throw err
  }
}
