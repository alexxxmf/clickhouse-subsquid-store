import { ClickHouseClient } from '@clickhouse/client'
import { readFileSync, readdirSync } from 'fs'
import { join } from 'path'

/**
 * Load and execute SQL schema files from hot-supported and regular directories
 * This creates all necessary tables, indexes, etc.
 *
 * @param client - ClickHouse client instance
 * @param network - Network name (e.g., 'ethereum', 'base') for template substitution
 * @param tablesPath - Path to SQL table definitions directory (relative to project root)
 */
export async function loadSqlFiles(
  client: ClickHouseClient,
  network: string = 'ethereum',
  tablesPath: string = 'src/db/tables'
): Promise<void> {
  // Use process.cwd() to get project root, works both in dev (src/) and prod (lib/)
  const baseDir = join(process.cwd(), tablesPath)
  const hotSupportedDir = join(baseDir, 'hot-supported')
  const regularDir = join(baseDir, 'regular')

  console.log('📁 Loading SQL schema files from:', baseDir)
  console.log(`   Network: ${network}`)

  // Get all .sql files from both directories
  type SqlFile = { path: string; name: string; type: 'hot-supported' | 'regular' }
  let sqlFiles: SqlFile[] = []

  // Load hot-supported tables (will create hot+cold variants)
  try {
    const files = readdirSync(hotSupportedDir)
      .filter(f => f.endsWith('.sql'))
      .map(f => ({ path: join(hotSupportedDir, f), name: f, type: 'hot-supported' as const }))
      .sort((a, b) => a.name.localeCompare(b.name))

    sqlFiles.push(...files)
    if (files.length > 0) {
      console.log(`   📋 Hot-supported tables (${files.length}): ${files.map(f => f.name).join(', ')}`)
    }
  } catch (err) {
    console.log(`   ⚠️  No hot-supported directory found, skipping`)
  }

  // Load regular tables (will create single tables, no hot/cold)
  try {
    const files = readdirSync(regularDir)
      .filter(f => f.endsWith('.sql'))
      .map(f => ({ path: join(regularDir, f), name: f, type: 'regular' as const }))
      .sort((a, b) => a.name.localeCompare(b.name))

    sqlFiles.push(...files)
    if (files.length > 0) {
      console.log(`   📋 Regular tables (${files.length}): ${files.map(f => f.name).join(', ')}`)
    }
  } catch (err) {
    console.log(`   ⚠️  No regular directory found, skipping`)
  }

  if (sqlFiles.length === 0) {
    console.warn(`⚠️  No .sql files found in ${baseDir}`)
    return
  }

  console.log(`📄 Found ${sqlFiles.length} SQL file(s):`, sqlFiles.map(f => `\n   - ${f.name}`).join(''))
  console.log()

  // Ensure we're using the correct database
  const database = process.env.CLICKHOUSE_DATABASE || 'default'
  console.log(`🎯 Target database: ${database}`)

  try {
    await client.exec({ query: `USE ${database}` })
    console.log(`✅ Switched to database: ${database}`)
  } catch (err: any) {
    console.error(`❌ Failed to switch to database ${database}:`, err.message)
    throw err
  }
  console.log()

  // Execute each SQL file
  for (const file of sqlFiles) {
    console.log(`🔨 Processing ${file.name} (${file.type})...`)

    try {
      let baseSql = readFileSync(file.path, 'utf-8')

      // Replace ${network} placeholder in ALL SQL files
      if (baseSql.includes('${network}')) {
        console.log(`   📋 Substituting \${network} → ${network}`)
        baseSql = baseSql.replace(/\$\{network\}/g, network)
      }

      // Extract table name from base SQL (e.g., ethereum_erc20_transfers)
      const tableNameMatch = baseSql.match(/CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)/i)
      if (!tableNameMatch) {
        console.warn(`   ⚠️  Could not extract table name from ${file.name}, skipping`)
        continue
      }

      const baseTableName = tableNameMatch[1]
      console.log(`   📊 Base table: ${baseTableName}`)

      if (file.type === 'hot-supported') {
        // Create HOT and COLD variants
        const hotSql = baseSql.replace(
          new RegExp(`\\b${baseTableName}\\b`, 'g'),
          `${baseTableName.replace(`${network}_`, `${network}_hot_`)}`
        )

        const coldSql = baseSql.replace(
          new RegExp(`\\b${baseTableName}\\b`, 'g'),
          `${baseTableName.replace(`${network}_`, `${network}_cold_`)}`
        )

        // Execute hot table
        console.log(`   🔥 Creating HOT table...`)
        await executeSQL(client, hotSql, database)

        // Execute cold table
        console.log(`   ❄️  Creating COLD table...`)
        await executeSQL(client, coldSql, database)

        console.log(`   ✅ Successfully created hot/cold variants for ${file.name}`)
      } else {
        // Regular table - create single table (no hot/cold variants)
        console.log(`   📊 Creating regular table...`)
        await executeSQL(client, baseSql, database)

        console.log(`   ✅ Successfully created table ${file.name}`)
      }
    } catch (err: any) {
      console.error(`   ❌ Failed to process ${file.name}:`, err.message)
      throw err
    }
  }

  console.log()
  console.log('✨ All SQL schema files executed successfully!')
  console.log()
}

/**
 * Execute a SQL statement with proper error handling
 */
async function executeSQL(client: ClickHouseClient, sql: string, database: string): Promise<void> {
  // Split by semicolons to handle multiple statements
  const statements = sql
    .split(';')
    .map(s => {
      // IMPORTANT: Remove SQL comments (lines starting with --)
      // ClickHouse's command() method does NOT handle SQL comments properly.
      return s
        .split('\n')
        .filter(line => !line.trim().startsWith('--'))
        .join('\n')
        .trim()
    })
    .filter(s => s.length > 0)

  for (let i = 0; i < statements.length; i++) {
    let statement = statements[i]

    // Fully qualify table names with database name if it's a CREATE TABLE statement
    if (statement.toUpperCase().includes('CREATE TABLE')) {
      statement = statement.replace(
        /CREATE TABLE IF NOT EXISTS (\w+)/i,
        `CREATE TABLE IF NOT EXISTS ${database}.$1`
      )
    }

    try {
      // Use command() instead of exec() - it's the right method for DDL statements
      await client.command({
        query: statement,
      })
    } catch (err: any) {
      // Check for specific error types
      if (err.type === 'SYNTAX_ERROR' && err.message?.includes('Empty query')) {
        continue
      }
      if (err.message?.includes('already exists') || err.message?.includes('duplicate')) {
        console.log(`      ⚠️  Table already exists, skipping`)
        continue
      }

      console.error(`      ❌ Failed statement ${i + 1}:`)
      console.error(`      Query (first 200 chars):`, statement.substring(0, 200))
      console.error(`      Error:`, err.message)
      throw err
    }
  }
}

/**
 * Verify that all required tables exist
 */
export async function verifyTables(client: ClickHouseClient, expectedTables: string[]): Promise<void> {
  console.log('🔍 Verifying tables exist...')

  const result = await client.query({
    query: 'SHOW TABLES',
    format: 'JSONEachRow',
  })

  const tables = await result.json<{ name: string }>()
  const tableNames = tables.map(t => t.name)

  console.log('   Available tables:', tableNames)

  for (const expected of expectedTables) {
    if (tableNames.includes(expected)) {
      console.log(`   ✅ Table '${expected}' exists`)
    } else {
      console.error(`   ❌ Table '${expected}' is missing!`)
      throw new Error(`Required table '${expected}' does not exist`)
    }
  }

  console.log()
  console.log(`✅ All ${expectedTables.length} required tables verified!`)
  console.log()
}
