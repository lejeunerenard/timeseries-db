declare module 'timeseries-db'

// hyperdb ships no types, this stub covers exactly what the wrapper calls
interface Hyperdb {
  insert(name: string, record: Record<string, unknown>): Promise<void>
  delete(name: string, record: Record<string, unknown>): Promise<void>
  get(name: string, record: Record<string, unknown>): Promise<unknown>
  find(name: string, query: Record<string, unknown>, options?: Record<string, unknown>): unknown
  findOne(
    name: string,
    query: Record<string, unknown>,
    options?: Record<string, unknown>
  ): Promise<unknown>
  flush(): Promise<void>
  ready(): Promise<void>
  close(): Promise<void>
  snapshot(options?: Record<string, unknown>): Hyperdb
  transaction(options?: Record<string, unknown>): Hyperdb
  exclusiveTransaction(options?: Record<string, unknown>): Promise<Hyperdb>
  closed: boolean
  writable: boolean
  definition: {
    resolveCollection(name: string): unknown
  }
}

type Tag = { key: string; value: string }

type Bound = {
  measurement?: string
  tags?: Tag[]
  field?: string
  timestamp?: Date | number
}

type Query = {
  gt?: Bound
  gte?: Bound
  lt?: Bound
  lte?: Bound
  limit?: number
  reverse?: boolean
}

class Timeseries {
  constructor(db: Hyperdb, options?: Record<string, unknown>)

  db: Hyperdb

  ready(): Promise<void>
  close(): Promise<void>
  flush(): Promise<Timeseries>

  closed: boolean
  writable: boolean
  readable: boolean

  snapshot(options?: Record<string, unknown>): Timeseries
  transaction(options?: Record<string, unknown>): Timeseries
  exclusiveTransaction(options?: Record<string, unknown>): Promise<Timeseries>

  insert(
    measurement: string,
    tags: Tag[],
    field: string,
    timestamp: Date | number,
    value: number
  ): Promise<number>

  find(query: Query, options?: Query): Promise<unknown>
  findOne(query: Query, options?: Query): Promise<unknown>
  findSeries(
    query: Query,
    options?: Query
  ): Promise<Array<{ measurement: string; tags: Tag[]; field: string; 'series-id': number }>>
  get(measurement: string, tags: Tag[], field: string, timestamp: Date | number): Promise<unknown>
  delete(query: Query | Record<string, unknown>): Promise<void>

  changes(range?: Record<string, unknown>): unknown
  diff(collection: string, query?: Query): unknown
}
