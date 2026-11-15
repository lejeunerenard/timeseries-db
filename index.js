// A timeseries database on top of hyperdb. `Timeseries` is a thin wrapper
// around an *already booted* hyperdb instance carrying the definition that
// `build.js` generates (`spec/hyperdb`): the class never boots and never owns
// the engine.
//
// Batch contract (mirrors hyperdb):
// - every write stages into the engine's in-memory update map. Staged writes
//   are invisible to reads until `flush()` commits them; nothing flushes
//   automatically, including `find()`.
// - a batch is "N writes, then one `flush()`". The wrapper's `flush()`
//   enqueues the new `series-id-latest` high mark immediately before
//   `db.flush()`, so the counter row is committed in the same atomic batch as
//   the datapoints it was used for. A batch that is never flushed leaves the
//   counter untouched.
//
// Concurrency caveat: `series-id`s are allocated from the committed high mark,
// loaded once per batch and incremented in memory. Two live sessions
// allocated from the same high mark share ids, and different identities then
// interleave in `series-data`, so insert batches must go through an explicit
// `exclusiveTransaction()` (hyperdb's built-in lock), flushed by the caller.
// A writer inserting `series-ids` rows without this wrapper never advances the
// counter and collides guaranteed.
//
// Tags are arrays of `{ key, value }` objects throughout the API: `insert()`
// takes `[{ key, value }, ...]` (a `[]` means an untagged series), and
// `findSeries()` rows and query bounds use the identical shape, so a row can
// be fed back to `insert()` or into a query unchanged. The one `normalizeTags`
// in `builder/inverted-indexes.js` covers writes and bounds alike.
//
// Queries keep hyperdb's own shape: `{ gt, gte, lt, lte }` of `{ measurement,
// tags, field, timestamp }` plus `{ limit, reverse }`, the same for `find()`,
// `findOne()`, `findSeries()` and `delete()`. Rules for the query translation
// in `_encodeQueryRange`: data scans need exact series bounds (every identity
// component) and become one range scan on `series-data`; `series-id`s are
// internal only, a series is always referred to by measurement, tags and
// field; `findSeries()` also accepts leading-prefix bounds (`measurement`,
// then `tags`, then `field`) as a prefix scan on `series-ids`. Anything else
// throws `QueryRequiresIdentityError`.

const b4a = require('b4a')
const { Transform } = require('streamx')
const { normalizeTags, denormalizeTags } = require('./builder/inverted-indexes.js')

const requiredCollections = [
  '@timeseries-db/series-data',
  '@timeseries-db/series-ids',
  '@timeseries-db/series-id-latest'
]

class TimeseriesDefinitionError extends Error {}
class NotWritableError extends Error {}
class ValueNotSupportedError extends Error {}
class QueryRequiresIdentityError extends Error {}
class UnsupportedWithEngineError extends Error {}

// Class helpers

function toTimestamp(timestamp) {
  if (timestamp instanceof Date) return timestamp
  if (Number.isSafeInteger(timestamp) && timestamp >= 0) return new Date(timestamp)
  return null
}

// Map key for one identity: the normalized tags buffer joined to the strings
function identityKey(measurement, tagsBuf, field) {
  return measurement + '\u0000' + field + '\u0000' + b4a.toString(tagsBuf, 'hex')
}

class Timeseries {
  constructor(db, options = {}) {
    const definition = db === null || db === undefined ? null : db.definition
    const resolveCollection = definition === null ? null : definition.resolveCollection

    const missing = []
    for (const name of requiredCollections) {
      if (resolveCollection === null || resolveCollection === undefined) missing.push(name)
      else if (resolveCollection(name) === null) missing.push(name)
    }

    if (missing.length > 0) {
      throw new TimeseriesDefinitionError(
        'The passed hyperdb definition is missing the collections: ' +
          missing.join(', ') +
          '. Pass a booted hyperdb instance with the ' +
          'generated timeseries-db definition, re-run build.js if it is stale'
      )
    }

    this.db = db
    this.context = options === undefined ? null : (options.context ?? null)

    // batch state, see the module docstring for the allocation algorithm
    this._seriesCache = new Map()
    this._latestId = null
    this._counterDirty = false
  }

  get closed() {
    return this.db.closed
  }

  get writable() {
    return this.db.writable
  }

  get readable() {
    return this.db.readable
  }

  ready() {
    return this.db.ready()
  }

  close() {
    return this.db.close()
  }

  async flush() {
    if (this._counterDirty) {
      // commit the new high mark in the same batch as the datapoints
      await this.db.insert('@timeseries-db/series-id-latest', {
        name: 'latest',
        id: this._latestId
      })
    }
    await this.db.flush()
    this._seriesCache.clear()
    this._latestId = null
    this._counterDirty = false
    return this
  }

  _sessionOptions(options = {}) {
    return { ...options, context: options.context ?? this.context }
  }

  snapshot(options = {}) {
    return new Timeseries(this.db.snapshot(this._sessionOptions(options)), {
      context: this.context
    })
  }

  transaction(options = {}) {
    return new Timeseries(this.db.transaction(this._sessionOptions(options)), {
      context: this.context
    })
  }

  async exclusiveTransaction(options = {}) {
    return new Timeseries(await this.db.exclusiveTransaction(this._sessionOptions(options)), {
      context: this.context
    })
  }

  insert(measurement, tags, field, timestamp, value) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (!this.db.writable) {
      throw new NotWritableError(
        'the wrapped hyperdb instance is not writable, use transaction() or exclusiveTransaction() to insert'
      )
    }

    if (typeof measurement !== 'string' || measurement === '') {
      throw new TypeError('measurement must be a non-empty string')
    }
    if (typeof field !== 'string' || field === '') {
      throw new TypeError('field must be a non-empty string')
    }
    if (!Array.isArray(tags)) {
      throw new TypeError(
        'tags must be an array of [{ key, value }] objects, pass [] for an untagged series'
      )
    }
    const date = toTimestamp(timestamp)
    if (date === null) {
      throw new TypeError('timestamp must be a Date or an unsigned integer of milliseconds')
    }
    if (!Number.isInteger(value)) {
      throw new ValueNotSupportedError(
        'value must be an integer, only int values are supported atm'
      )
    }

    return this._insertPoint(measurement, normalizeTags(tags), field, date, value)
  }

  async _ensureSeriesId(measurement, tags, field) {
    const cacheKey = identityKey(measurement, tags, field)
    if (this._seriesCache.has(cacheKey)) return this._seriesCache.get(cacheKey)

    let seriesId
    const existing = await this.db.get('@timeseries-db/series-ids', { measurement, tags, field })
    if (existing === null) {
      if (this._latestId === null) {
        const counter = await this.db.get('@timeseries-db/series-id-latest', { name: 'latest' })
        this._latestId = counter === null ? 0 : counter.id
      }
      seriesId = ++this._latestId
      this._counterDirty = true
      await this.db.insert('@timeseries-db/series-ids', {
        measurement,
        tags,
        field,
        'series-id': seriesId
      })
    } else {
      seriesId = existing['series-id']
    }
    this._seriesCache.set(cacheKey, seriesId)

    return seriesId
  }

  async _insertPoint(measurement, tags, field, timestamp, value) {
    const seriesId = await this._ensureSeriesId(measurement, tags, field)

    await this.db.insert('@timeseries-db/series-data', { 'series-id': seriesId, timestamp, value })
    return seriesId
  }

  find(query, options = {}) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (typeof query !== 'object' || query === null) {
      throw new TypeError(
        'query must be { gt, gte, lt, lte } of { measurement, tags, field, timestamp }'
      )
    }
    return this._find(query, options)
  }

  findOne(query, options = {}) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (typeof query !== 'object' || query === null) {
      throw new TypeError(
        'query must be { gt, gte, lt, lte } of { measurement, tags, field, timestamp }'
      )
    }
    return this._findOne(query, options)
  }

  // series metadata rows come back as an array with the `tags` buffer decoded
  // to pairs, users round-trip plain values and never handle buffers
  findSeries(query, options = {}) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (typeof query !== 'object' || query === null) {
      throw new TypeError('query must be { gt, gte, lt, lte } of { measurement, tags, field }')
    }
    return this._findSeries(query, options)
  }

  get(measurement, tags, field, timestamp) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (typeof measurement !== 'string' || measurement === '') {
      throw new TypeError('measurement must be a non-empty string')
    }
    if (typeof field !== 'string' || field === '') {
      throw new TypeError('field must be a non-empty string')
    }
    if (!Array.isArray(tags)) {
      throw new TypeError('tags must be an array of [{ key, value }] objects')
    }
    const date = toTimestamp(timestamp)
    if (date === null) {
      throw new TypeError('timestamp must be a Date or an unsigned integer of milliseconds')
    }
    return this._get(measurement, normalizeTags(tags), field, date)
  }

  async _find(query, options) {
    const { name, range } = await this._encodeQueryRange(query, 'data')
    return this.db.find(name, range, this._streamOptions(query, options))
  }

  async _findOne(query, options) {
    const { name, range } = await this._encodeQueryRange(query, 'data')
    return this.db.findOne(name, range, { ...this._streamOptions(query, options), limit: 1 })
  }

  async _findSeries(query, options) {
    const { name, range } = await this._encodeQueryRange(query, 'series')
    return this.db.find(name, range, this._streamOptions(query, options)).pipe(new Transform({
      transform (row, cb) {
        row.tags = denormalizeTags(row.tags)
        this.push(row)
        cb()
      }
    }))
  }

  async _get(measurement, tags, field, timestamp) {
    const series = await this.db.get('@timeseries-db/series-ids', { measurement, tags, field })
    if (series === null) return null
    return await this.db.get('@timeseries-db/series-data', {
      'series-id': series['series-id'],
      timestamp
    })
  }

  _streamOptions(query, options) {
    return { limit: query.limit, reverse: query.reverse, ...options }
  }

  _isRangeQuery(query) {
    for (const name of ['gt', 'gte', 'lt', 'lte']) {
      if (query[name] !== undefined && query[name] !== null) return true
    }
    return false
  }

  // Deletion is query-selected, not a range delete: exact key (timestamp
  // present) -> one `db.delete`, anything coarser streams `find(query)` and
  // enqueues `db.delete` per hit, so it is O(matched points) and points
  // inserted after the stream starts survive. `_seriesCache` entries for
  // removed series are dropped, otherwise a later `insert()` in the same
  // batch would resurrect a deleted id
  delete(query) {
    if (this.db.closed) throw new Error('Hyperdb is closed')
    if (typeof query !== 'object' || query === null) {
      throw new TypeError(
        'query must be { gt, gte, lt, lte } or an exact key { measurement, tags, field, timestamp }'
      )
    }
    return this._delete(query)
  }

  async _delete(query) {
    let ids
    let range

    if (this._isRangeQuery(query)) {
      const encoded = await this._encodeQueryRange(query, 'data')
      ids = encoded.ids
      range = encoded.range
    } else {
      // exact-key form: { measurement, tags, field, timestamp }
      const seriesId = await this._resolveExactSeriesId(query)
      if (seriesId === null) return
      ids = [seriesId]
      const timestamp = query.timestamp
      if (timestamp !== undefined && timestamp !== null) {
        const date = toTimestamp(timestamp)
        if (date === null) {
          throw new TypeError('timestamp must be a Date or an unsigned integer of milliseconds')
        }
        await this.db.delete('@timeseries-db/series-data', {
          'series-id': seriesId,
          timestamp: date
        })
        this._dropCachedSeries(ids)
        return
      }
      // identity without timestamp = whole series: an open-ended scan, the
      // short key plus the 0xff terminator spans every point of the series
      range = { gte: { 'series-id': seriesId }, lte: { 'series-id': seriesId } }
    }

    for await (const point of this.db.find('@timeseries-db/series-data', range)) {
      await this.db.delete('@timeseries-db/series-data', point)
    }
    this._dropCachedSeries(ids)
  }

  _dropCachedSeries(ids) {
    for (const [key, seriesId] of this._seriesCache) {
      if (ids.includes(seriesId)) this._seriesCache.delete(key)
    }
  }

  // resolve an exact delete key from the full identity; null when the series
  // was never allocated (nothing can be staged against it)
  async _resolveExactSeriesId(query) {
    const identity = this._exactIdentity(query)
    if (identity === null) {
      throw new QueryRequiresIdentityError(
        'delete exact key must carry the full identity (measurement, tags and field)'
      )
    }
    return await this._resolveSeriesId(identity)
  }

  _exactIdentity(query) {
    const measurement = query.measurement
    const tags = query.tags
    const field = query.field
    if (measurement === undefined || tags === undefined || field === undefined) return null
    if (typeof measurement !== 'string' || measurement === '') {
      throw new TypeError('measurement must be a non-empty string')
    }
    if (typeof field !== 'string' || field === '') {
      throw new TypeError('field must be a non-empty string')
    }
    return { measurement, tags: normalizeTags(tags), field }
  }

  async _resolveSeriesId(identity) {
    const cacheKey = identityKey(identity.measurement, identity.tags, identity.field)
    const seriesId = this._seriesCache.get(cacheKey)
    if (seriesId !== undefined) return seriesId
    const row = await this.db.get('@timeseries-db/series-ids', identity)
    return row === null ? null : row['series-id']
  }

  // query translation, see the module docstring for the rules
  async _encodeQueryRange(query, mode) {
    const range = {}
    const ids = []
    let constraints = 0
    let unknown = false

    for (const name of ['gt', 'gte', 'lt', 'lte']) {
      const bound = query[name]
      if (bound === undefined || bound === null) continue
      if (mode === 'series') {
        // an empty bound is unbounded, skipping it keeps the auto range
        const encoded = this._encodeSeriesBound(bound)
        if (Object.keys(encoded).length > 0) {
          range[name] = encoded
          constraints++
        }
      } else {
        constraints++
        const encoded = await this._encodeDataBound(bound)
        if (encoded === null) unknown = true
        else {
          range[name] = encoded
          ids.push(encoded['series-id'])
        }
      }
    }

    if (mode === 'series') {
      if (constraints === 0) {
        throw new QueryRequiresIdentityError(
          'a series query must constrain at least one leading PK component on a bound: measurement, then tags, then field'
        )
      }
      return { name: '@timeseries-db/series-ids', range, ids }
    }

    if (constraints === 0) {
      throw new QueryRequiresIdentityError(
        'a datapoint query must constrain every bound to an exact series: measurement + tags + field'
      )
    }

    if (unknown) {
      // a bound referenced a series that was never allocated: collapse the
      // query to an empty scan. ids allocated by the counter start at 1, so
      // id 0 never has rows or points
      return {
        name: '@timeseries-db/series-data',
        range: { gte: { 'series-id': 0 }, lte: { 'series-id': 0 } },
        ids
      }
    }

    return { name: '@timeseries-db/series-data', range, ids }
  }

  // bounds must be leading PK components only: measurement, then tags,
  // then field. constraining a later PK component while an earlier one is
  // unbounded has no scan shape on series-ids
  _encodeSeriesBound(bound) {
    if (typeof bound !== 'object' || bound === null) {
      throw new TypeError('query bound must be an object')
    }

    const out = {}
    let unbounded = false

    for (const key of ['measurement', 'tags', 'field']) {
      if (bound[key] === undefined || bound[key] === null) {
        unbounded = true
        continue
      }
      if (unbounded) {
        throw new QueryRequiresIdentityError(
          'query bound constrains ' +
            key +
            ' while an earlier PK component is unbounded, that query shape belongs to series-by-tag/series-by-field, which v1 does not probe'
        )
      }
      out[key] = key === 'tags' ? normalizeTags(bound[key]) : bound[key]
      if (key !== 'tags' && typeof out[key] !== 'string') {
        throw new TypeError('query bound ' + key + ' must be a non-empty string')
      }
    }

    for (const key in bound) {
      if (key !== 'measurement' && key !== 'tags' && key !== 'field') {
        throw new QueryRequiresIdentityError(
          'series queries constrain identity only, ' + key + ' is not a series-ids PK component'
        )
      }
    }

    return out
  }

  // data bounds must carry the full series identity. returns null when the
  // series does not exist, which collapses the whole query to an empty scan
  async _encodeDataBound(bound) {
    if (typeof bound !== 'object' || bound === null) {
      throw new TypeError('query bound must be an object')
    }

    const out = {}

    const identity = this._exactIdentity(bound)
    if (identity === null) {
      throw new QueryRequiresIdentityError(
        'a datapoint bound must carry the full identity (measurement, tags and field), constraining a later PK component while an earlier one is unbounded belongs to series-by-tag/series-by-field, which v1 does not probe'
      )
    }
    out['series-id'] = await this._resolveSeriesId(identity)
    if (out['series-id'] === null) return null

    if (bound.timestamp !== undefined) {
      const timestamp = toTimestamp(bound.timestamp)
      if (timestamp === null) {
        throw new TypeError(
          'query bound timestamp must be a Date or an unsigned integer of milliseconds'
        )
      }
      out.timestamp = timestamp
    }

    return out
  }

  // pass-through to db
  changes(range = {}) {
    return this.db.changes(range)
  }

  // pass-through to db
  diff(collectionName, query = {}) {
    return this.db.diff(collectionName, query)
  }
}

module.exports = Timeseries
