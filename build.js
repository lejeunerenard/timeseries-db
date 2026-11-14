const Hyperdb = require('hyperdb/builder')
const Hyperschema = require('hyperschema')

const SCHEMA_DIR = './spec/hyperschema'
const DB_DIR = './spec/hyperdb'

const NS = 'timeseries-db'

const schema = Hyperschema.from(SCHEMA_DIR)
const ns = schema.namespace(NS)

ns.register({
  name: 'tag',
  fields: [
    {
      name: 'key',
      type: 'string',
      required: true
    },
    {
      name: 'value',
      type: 'string',
      required: true
    }
  ]
})

const SeriesIdFields = [
  {
    name: 'measurement',
    type: 'string',
    required: true
  },
  {
    // an encoding of the 'tag' struct as an ordered array at library runtime
    name: 'tags',
    type: 'buffer',
    required: true
  },
  {
    name: 'field',
    type: 'string',
    required: true
  }
]

ns.register({
  name: 'series-id',
  alias: 'uint'
})

ns.register({
  name: 'series-normalized-key-inverted-lookup',
  fields: [
    ...SeriesIdFields,
    {
      name: 'series-id',
      type: `@${NS}/series-id`,
      required: true
    }
  ]
})

ns.register({
  name: 'series-datapoint',
  fields: [
    {
      name: 'series-id',
      type: `@${NS}/series-id`,
      required: true
    },
    {
      name: 'timestamp',
      type: 'date',
      required: true
    },
    {
      name: 'value',
      type: 'int', // Only numbers atm
      required: true
    }
  ]
})

// High mark of the series-id counter: one committed row per counter partition,
// the only one used in v1 is { name: 'latest' }. A `name` field so the
// collection key is never empty, which keeps the generated keyEncoding a real
// IndexEncoder. Room for per-namespace/per-measurement counters later.
ns.register({
  name: 'series-id-latest',
  fields: [
    {
      name: 'name',
      type: 'string',
      required: true
    },
    {
      name: 'id',
      type: `@${NS}/series-id`,
      required: true
    }
  ]
})

Hyperschema.toDisk(schema)

// Hyper DB

const db = Hyperdb.from(SCHEMA_DIR, DB_DIR)
const dbNs = db.namespace(NS)

dbNs.require('builder/inverted-indexes.js')

dbNs.collections.register({
  name: 'series-ids',
  schema: `@${NS}/series-normalized-key-inverted-lookup`,
  key: ['measurement', 'tags', 'field']
})

dbNs.indexes.register({
  name: 'series-by-tag',
  collection: `@${NS}/series-ids`,
  key: {
    type: {
      fields: [
        { name: 'key', type: 'string' },
        { name: 'value', type: 'string' }
      ]
    },
    map: 'tagIndex'
  }
})

dbNs.indexes.register({
  name: 'series-by-field',
  collection: `@${NS}/series-ids`,
  key: {
    type: 'string',
    map: 'fieldIndex'
  }
})

dbNs.collections.register({
  name: 'series-data',
  schema: `@${NS}/series-datapoint`,
  key: ['series-id', 'timestamp']
})

dbNs.collections.register({
  name: 'series-id-latest',
  schema: `@${NS}/series-id-latest`,
  key: ['name']
})

Hyperdb.toDisk(db, { esm: false })
