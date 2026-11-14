const HyperDB = require('hyperdb')
const Hyperbee2 = require('hyperbee2')
const Hypercore = require('hypercore')
const def = require('../../spec/hyperdb/index.js')

// hyperbee2 normally takes a corestore which this package does not depend on.
// A fresh single-core tree only touches the store for `get` and the lifecycle
// getters, so a plain object stands in for one
async function openBee2(t) {
  const core = new Hypercore(await t.tmp())
  const store = {
    get: () => core,
    opened: false,
    opening: false,
    closed: false,
    closing: null,
    close: () => Promise.resolve(),
    replicate: () => null
  }
  const bee = new Hyperbee2(store)
  const db = HyperDB.bee2(bee, def)
  t.teardown(() => db.close())
  await db.ready()
  return db
}

// hyperdb's rocks engine opens a real rocksdb-native store, so each test gets
// its own temporary db directory, which brittle removes after the test. The
// close is registered as a brittle teardown so no test body needs a try/finally
async function openRocks(t) {
  const db = HyperDB.rocks(await t.tmp(), def)
  t.teardown(() => db.close())
  await db.ready()
  return db
}

function seriesOf(measurement, tags, field) {
  return { gte: { measurement, tags, field }, lte: { measurement, tags, field } }
}

function seriesOfWindow(measurement, tags, field, from, to) {
  return {
    gte: { measurement, tags, field, timestamp: from },
    lte: { measurement, tags, field, timestamp: to }
  }
}

// streamx readables are async-iterable. `toArray()` only exists on hyperdb's
// own query streams, not on the engine diff stream, so drain generically
async function drainOf(fn) {
  const stream = await fn()
  const rows = []
  for await (const row of stream) rows.push(row)
  return rows
}

async function insertBatch(ts, ...rows) {
  const ids = []
  for (const row of rows) ids.push(await ts.insert(...row))
  return ids
}

module.exports = { openBee2, openRocks, seriesOf, seriesOfWindow, drainOf, insertBatch }
