const test = require('brittle')
const HyperDB = require('hyperdb')
const Timeseries = require('..')
const {
  openBee2,
  openRocks,
  seriesOf,
  seriesOfWindow,
  drainOf,
  insertBatch
} = require('./helpers/index.js')

const NS = '@timeseries-db'
const seriesData = NS + '/series-data'
const seriesIdLatest = NS + '/series-id-latest'

const t0 = new Date(0)
const t1 = new Date(1000)
const t2 = new Date(2000)
const t3 = new Date(3000)
const preEpoch = new Date(-5000)

test('staged writes are invisible to reads until flush, nothing auto-flushes', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  const query = seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load')

  const inserted = await insertBatch(
    batch,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t0, 11],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t1, 12],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t2, 13]
  )
  t.is(new Set(inserted).size, 1)

  const staged = await drainOf(() => ts.find(query))
  t.is(staged.length, 0)

  await batch.flush()

  const committed = await drainOf(() => ts.find(query))
  t.is(committed.length, 3)
  t.alike(
    committed.map((p) => p.value),
    [11, 12, 13]
  )

  await ts.close()
})

test('tag order does not matter, one series-ids row per identity', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  const a = await batch.insert(
    'cpu',
    [
      { key: 'host', value: 'a' },
      { key: 'region', value: 'west' }
    ],
    'load',
    t0,
    1
  )
  const b = await batch.insert(
    'cpu',
    [
      { key: 'region', value: 'west' },
      { key: 'host', value: 'a' }
    ],
    'load',
    t0,
    2
  )
  t.is(a, b)
  await batch.flush()
  const rows = await drainOf(() => ts.findSeries({ gte: { measurement: 'cpu' }, lte: { measurement: 'cpu' } }))
  t.is(rows.length, 1)
  t.alike(rows[0].tags, [
    { key: 'host', value: 'a' },
    { key: 'region', value: 'west' }
  ])
  await ts.close()
})

test('tag value boundaries cannot collide', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  const a = await batch.insert('cpu', [{ key: 'a', value: '1|b' }], 'load', t0, 1)
  const b = await batch.insert(
    'cpu',
    [
      { key: 'a', value: '1' },
      { key: 'b', value: '2' }
    ],
    'load',
    t0,
    2
  )
  t.not(a, b)
  await batch.flush()
  const rows = await drainOf(() => ts.findSeries({ gte: { measurement: 'cpu' }, lte: { measurement: 'cpu' } }))
  t.is(rows.length, 2)
  await ts.close()
})

test('counter: N new series get N distinct ids, committed with the batch', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  const ids = await insertBatch(
    batch,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1],
    ['cpu', [{ key: 'host', value: 'b' }], 'load', t0, 2],
    ['mem', [{ key: 'host', value: 'a' }], 'used', t0, 3]
  )
  t.alike(ids, [1, 2, 3])

  const stagedCounter = await db.get(seriesIdLatest, { name: 'latest' })
  t.is(stagedCounter, null)

  await batch.flush()
  const counter = await db.get(seriesIdLatest, { name: 'latest' })
  t.alike(counter, { name: 'latest', id: 3 })
  await ts.close()
})

test('second batch that reuses a committed identity does not bump the counter', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)

  const first = await ts.exclusiveTransaction()
  const idA = await first.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  await first.flush()

  const second = await ts.exclusiveTransaction()
  const idB = await second.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2)
  t.is(idA, idB)
  await second.flush()

  const counter = await db.get(seriesIdLatest, { name: 'latest' })
  t.is(counter.id, 1)
  await ts.close()
})

test('delete: exact, whole series and time window, staged until flush', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const seed = await ts.exclusiveTransaction()
  await insertBatch(
    seed,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t2, 3],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t3, 4]
  )
  await seed.flush()

  const batch = await ts.exclusiveTransaction()

  // exact key: identity + timestamp
  await batch.delete({
    measurement: 'cpu',
    tags: [{ key: 'host', value: 'a' }],
    field: 'load',
    timestamp: t0
  })

  // whole series identity without timestamp
  await batch.delete({ measurement: 'mem', tags: [{ key: 'host', value: 'z' }], field: 'none' })

  // time window via range query
  await batch.delete(seriesOfWindow('cpu', [{ key: 'host', value: 'a' }], 'load', t1, t2))

  // nothing took effect on the committed view yet
  const stillThere = await drainOf(() =>
    ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load'))
  )
  t.is(stillThere.length, 4)

  await batch.flush()

  const left = await drainOf(() => ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load')))
  t.alike(
    left.map((p) => p.timestamp.getTime()),
    [t3.getTime()]
  )
  await ts.close()
})

test('delete is query-selected, points inserted after the stream survive', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2)
  await batch.flush()

  const batch2 = await ts.exclusiveTransaction()
  await batch2.delete({ measurement: 'cpu', tags: [{ key: 'host', value: 'a' }], field: 'load' })
  await batch2.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t2, 3)
  await batch2.flush()

  const left = await drainOf(() => ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load')))
  t.not(left.length, 0)
  t.alike(
    left.map((p) => p.value),
    [3]
  )
  await ts.close()
})

test('find range semantics: gte/lt bounds, limit, reverse', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const seed = await ts.exclusiveTransaction()
  await insertBatch(
    seed,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t2, 3],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t3, 4]
  )
  await seed.flush()

  const window = seriesOfWindow('cpu', [{ key: 'host', value: 'a' }], 'load', t1, t2)
  const hits = await drainOf(() => ts.find(window))
  t.alike(
    hits.map((p) => p.value),
    [2, 3]
  )

  const limited = await drainOf(() => ts.find(window, { limit: 1 }))
  t.alike(
    limited.map((p) => p.value),
    [2]
  )

  const reversed = await drainOf(() => ts.find(window, { reverse: true }))
  t.alike(
    reversed.map((p) => p.value),
    [3, 2]
  )

  const only = await ts.findOne(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load'))
  t.is(only.value, 1)

  await ts.close()
})

test('v1 rejects queries that do not carry leading identity', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)

  await t.exception(
    () => ts.find({ gte: { field: 'load' } }),
    /a datapoint bound must carry the full identity/
  )

  await t.exception(
    () => ts.findSeries({ gte: { tags: [{ key: 'host', value: 'a' }] } }),
    /query bound constrains tags while an earlier PK component is unbounded/
  )

  await t.exception(
    () => ts.findSeries({ gte: {}, lte: {} }),
    /a series query must constrain at least one leading PK component/
  )

  await ts.close()
})

test('a cancelled transaction leaves the db unchanged and must be closed', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  await t.exception(async () => {
    const batch = await ts.exclusiveTransaction()
    try {
      await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
      throw new Error('aborted batch')
    } finally {
      // transactions must always be closed, even when cancelled
      await batch.close()
    }
  }, /^Error: aborted batch$/)
  const rows = await drainOf(() => ts.findSeries({ gte: { measurement: 'cpu' }, lte: { measurement: 'cpu' } }))
  t.is(rows.length, 0)
  const batch = await ts.exclusiveTransaction()
  await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  await batch.flush()
  const after = await drainOf(() => ts.findSeries({ gte: { measurement: 'cpu' }, lte: { measurement: 'cpu' } }))
  t.is(after.length, 1)
  await ts.close()
})

test('lifecycle: ready/close, insert after close throws synchronously', async (t) => {
  const db = await openRocks(t)
  await db.ready()
  const ts = new Timeseries(db)
  await ts.ready()
  t.is(ts.closed, false)
  t.is(ts.writable, true)
  await ts.close()
  t.is(ts.closed, true)
  t.exception(
    () => ts.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1),
    /^Error: Hyperdb is closed$/
  )
})

test('constructor rejects a foreign definition by name', async (t) => {
  const other = {
    versions: { schema: 1, db: 1 },
    collections: [],
    indexes: [],
    resolveCollection: () => null,
    resolveIndex: () => null
  }
  const db = HyperDB.rocks(await t.tmp(), other)
  try {
    t.exception(
      () => new Timeseries(db),
      /missing the collections: .*@timeseries-db\/series-id-latest/
    )
  } finally {
    await db.close()
  }
})

test('insert argument validation errors', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  t.exception.all(() => ts.insert('cpu', [{ key: 'host', value: 'a' }], 'load'), TypeError)

  t.exception(() => ts.insert('cpu', [], 'load', t0, 1.5), /value must be an integer/)

  t.exception.all(() => ts.insert('cpu', [], 'load', 'yesterday', 1), TypeError)

  t.exception.all(
    () => ts.insert('cpu', { host: 'a' }, 'load', t0, 1),
    /tags must be an array of \[\{ key, value \}\] objects/
  )

  const batch = await ts.exclusiveTransaction()
  const readOnly = ts.snapshot()
  t.is(readOnly.writable, false)
  t.exception(
    () => readOnly.insert('cpu', [], 'load', t0, 1),
    /the wrapped hyperdb instance is not writable/
  )
  await batch.close()
  await ts.close()
})

test('get() miss returns null, re-insert after delete-all reuses the series-id', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)

  t.is(await ts.get('cpu', [{ key: 'host', value: 'a' }], 'load', t0), null)

  const seed = await ts.exclusiveTransaction()
  const id = await seed.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  await seed.flush()

  const missing = await ts.get('cpu', [{ key: 'host', value: 'a' }], 'load', t2)
  t.is(missing, null)

  const rm = await ts.exclusiveTransaction()
  await rm.delete({ measurement: 'cpu', tags: [{ key: 'host', value: 'a' }], field: 'load' })
  await rm.flush()

  const again = await ts.exclusiveTransaction()
  const id2 = await again.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2)
  t.is(id, id2)
  await again.flush()

  const counter = await db.get(seriesIdLatest, { name: 'latest' })
  t.is(counter.id, 1)
  await ts.close()
})

test('prefix scan on series-ids: every series under a measurement, no more', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const seed = await ts.exclusiveTransaction()
  await insertBatch(
    seed,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1],
    ['cpu', [{ key: 'host', value: 'b' }], 'load', t0, 2],
    ['cpu', [], 'idle', t0, 3],
    ['mem', [{ key: 'host', value: 'a' }], 'used', t0, 4]
  )
  await seed.flush()

  const rows = await drainOf(() => ts.findSeries({ gte: { measurement: 'cpu' }, lte: { measurement: 'cpu' } }))
  t.is(rows.length, 3)
  const measurements = rows.map((row) => row.measurement)
  t.alike(measurements, ['cpu', 'cpu', 'cpu'])

  await ts.close()
})

test('identity round-trip: tags decode back to normalized pairs', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)
  const seed = await ts.exclusiveTransaction()
  const id = await seed.insert(
    'cpu',
    [
      { key: 'region', value: 'west' },
      { key: 'host', value: 'a' }
    ],
    'load',
    t0,
    1
  )
  await seed.flush()

  const rows = await drainOf(() => ts.findSeries(
    seriesOf(
      'cpu',
      [
        { key: 'host', value: 'a' },
        { key: 'region', value: 'west' }
      ],
      'load'
    )
  ))
  t.is(rows.length, 1)
  t.alike(rows[0].tags, [
    { key: 'host', value: 'a' },
    { key: 'region', value: 'west' }
  ])
  t.is(rows[0]['series-id'], id)
  t.is(rows[0].field, 'load')

  await ts.close()
})

test('key ordering is chronological across INT width classes', async (t) => {
  const db = await openRocks(t)
  const ts = new Timeseries(db)

  const seed = await ts.exclusiveTransaction()
  await insertBatch(
    seed,
    ['cpu', [{ key: 'host', value: 'a' }], 'load', preEpoch, 1],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', 0, 2],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', 0xf6, 3],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', 0xffff, 4],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', 0xffffffff, 5],
    ['cpu', [{ key: 'host', value: 'a' }], 'load', 0x100000000, 6]
  )
  await seed.flush()

  const all = await drainOf(() => ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load')))
  t.is(all.length, 6)
  t.is(all[0].value, 1)
  t.is(all[5].value, 6)
  const stamps = all.map((p) => p.timestamp.getTime())
  let sorted = true
  for (let i = 1; i < stamps.length; i++) if (stamps[i] < stamps[i - 1]) sorted = false
  t.is(sorted, true)

  const window = seriesOfWindow(
    'cpu',
    [{ key: 'host', value: 'a' }],
    'load',
    new Date(0xf6),
    new Date(0xffffffff)
  )
  const hits = await drainOf(() => ts.find(window))
  t.alike(
    hits.map((p) => p.value),
    [3, 4, 5]
  )

  await ts.close()
})

test('bee2 - diff works', async (t) => {
  const db = await openBee2(t)
  const ts = new Timeseries(db)
  const seed = await ts.exclusiveTransaction()
  await seed.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  await seed.flush()

  const second = await ts.exclusiveTransaction()
  await second.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t1, 2)
  await second.flush()

  const diffs = await drainOf(() => ts.diff(seriesData))
  t.is(diffs.length, 2)
  t.is(
    diffs.every((d) => d.left === null),
    true
  )
  t.alike(
    diffs.map((d) => d.right.value),
    [1, 2]
  )

  await ts.close()
})

test('lifecycle and staging work on bee2 as well', async (t) => {
  const db = await openBee2(t)
  const ts = new Timeseries(db)
  const batch = await ts.exclusiveTransaction()
  const id = await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', t0, 1)
  const staged = await drainOf(() =>
    ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load'))
  )
  t.is(staged.length, 0)
  await batch.flush()
  const after = await drainOf(() => ts.find(seriesOf('cpu', [{ key: 'host', value: 'a' }], 'load')))
  t.is(after.length, 1)
  t.is(after[0].value, 1)
  const counter = await db.get(seriesIdLatest, { name: 'latest' })
  t.is(counter.id, id)
  await ts.close()
})
