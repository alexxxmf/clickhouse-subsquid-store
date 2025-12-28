import type {
  ClickHouseClient,
  CommandParams,
  CommandResult,
  DataFormat,
  InsertParams,
  InsertResult,
  QueryParams,
  QueryResult,
} from '@clickhouse/client'


export type QueryParamsWithFormat<Format extends DataFormat> = Omit<QueryParams, 'format'> & {
  format?: Format
}

export class ClickhouseStore {
    private deletes = new Set<string>()
    private inserts: any[] = []

    constructor(public client: ClickHouseClient) {}

    /**
     * Get buffered inserts (used by ClickhouseDatabase to flush data)
     */
    getInserts(): any[] {
      return this.inserts
    }

    /**
     * Clear all internal buffers
     */
    clearBuffers(): void {
      this.inserts = []
      this.deletes.clear()
    }

    async insert<T extends object>(entities: T | T[]): Promise<void> {
      const arr = Array.isArray(entities) ? entities : [entities]
      // Use concat or loop instead of spread to avoid stack overflow with large arrays
      for (const entity of arr) {
        this.inserts.push(entity)
      }
    }

    async query<Format extends DataFormat = 'JSON'>(params: QueryParamsWithFormat<Format>): Promise<QueryResult<Format>> {
      return this.client.query(params)
    }

    async command(params: CommandParams): Promise<CommandResult> {
      return this.client.command(params)
    }

    async close() {
      return this.client.close()
    }

    async save<T extends object>(entities: T | T[]): Promise<void> {
      return this.insert(entities)
    }

    async remove<E extends object>(
      entityOrClass: any,
      id?: string | string[]
    ): Promise<void> {
      // Case 1 & 2: entity instance(s) → extract .id
      if (id == null) {
        const entities = Array.isArray(entityOrClass) ? entityOrClass : [entityOrClass]
        for (const e of entities) {
          if (e && typeof e === 'object' && 'id' in e) {
            this.deletes.add(e.id as string)
          }
        }
        return
      }

      // Case 3: class + id(s)
      const ids = Array.isArray(id) ? id : [id]
      for (const i of ids) {
        this.deletes.add(i)
      }
    }

    private async getTableEngine(table: string): Promise<string> {
      const result = await this.client.query({
        query: `
          SELECT engine
          FROM system.tables
          WHERE database = currentDatabase()
            AND name = {table:String}
        `,
        query_params: { table },
        format: 'JSONEachRow',
      })

      const rows = await result.json<{ engine: string }>()

      if (rows.length === 0) {
        throw new Error(`Table "${table}" does not exist in the current database`)
      }
      
      return rows[0].engine
    }

    async removeAllRowsByQuery({
      table,
      query,
      params,
    }: {
      table: string
      query: string
      params?: Record<string, unknown>
    }) {
      let count = 0
      const res = await this.client.query({
        query,
        format: 'JSONEachRow',
        clickhouse_settings: {
          date_time_output_format: 'iso',
          output_format_json_quote_64bit_floats: 1,
          output_format_json_quote_64bit_integers: 1,
        },
        query_params: params,
      })

      for await (const rows of res.stream()) {
        await this.client.insert({
          table,
          values: rows.map((row: any) => {
            const data = row.json()

            data.sign = -1

            return data
          }),
          format: 'JSONEachRow',
          clickhouse_settings: {
            date_time_input_format: 'best_effort',
          },
        })

        count += rows.length
      }

      return count
    }

    async removeAllRows({
      tables,
      where,
      params = {},
    }: {
      tables: string | string[]
      where: string
      params?: Record<string, any>
    }) {
      tables = typeof tables === 'string' ? [tables] : tables

      const results = []

      for (const table of tables) {
        const engine = await this.getTableEngine(table)

        let count: number

        if (engine.includes('Collapsing') || engine.includes('VersionedCollapsing')) {
          // Collapsing → insert tombstones and count how many we cancelled
          const result = await this.client.query({
            query: `SELECT *, -1 AS sign FROM ${table} FINAL WHERE ${where}`,
            query_params: params,
            format: 'JSONEachRow',
          })

          await this.client.insert({
            table,
            values: result.stream(),
            format: 'JSONEachRow',
          })

          const [{ count: c }] = await this.client.query({
            query: `SELECT count() AS count FROM ${table} FINAL WHERE ${where}`,
            query_params: params,
            format: 'JSONEachRow',
          }).then(r => r.json<{ count: number | string }>())

          count = Number(c)
          console.log(`Reorg cleanup: ${table} (Collapsing) — cancelled ${count} rows`)
        } else {
          // All other engines
          // TODO: maybe we can cover specific cases for some of the others....
          // in any case reorgs don t happen frequently plus not many rows affected on average
          // shouldn't be super computationally expensive
          count = await this.removeAllRowsByQuery({
            table,
            query: `SELECT * FROM ${table} FINAL WHERE ${where}`,
            params,
          })

          console.log(`Reorg cleanup: ${table} (${engine}) — logically removed ${count} rows (will dedup on merge)`)
        }

        results.push({ table, engine, removed: count })
      }

      console.log('Reorg cleanup complete:', results)
      return results
    }

}