# Timeseries DB

A P2P timeseries database built on hyperdb.

> [!IMPORTANT]
> This is library is experimental. The API is subject to change, and everything may break.

## Usage

Merge the library's schema structs and collections into your build script, the
generators register under the `timeseries-db` namespace alongside your own
definitions:

```js
const Hyperschema = require('hyperschema')
const Hyperdb = require('hyperdb/builder')
const { generateSchema, generateDb } = require('timeseries-db/generate-schema')

// applies the tag, series-id, series-datapoint and counter structs
const schema = Hyperschema.from('./spec/hyperschema')
generateSchema(schema)
Hyperschema.toDisk(schema)

// registers the series-ids, series-data and series-id-latest collections
// and the series-by-tag/series-by-field indexes
const db = Hyperdb.from('./spec/hyperschema', './spec/hyperdb')
generateDb(db)
Hyperdb.toDisk(db, { esm: false })
```

Run it as part of your build, then boot hyperdb with the generated definition
(`./spec/hyperdb`) and wrap it:

```js
const HyperDB = require('hyperdb')
const Timeseries = require('timeseries-db')
const def = require('./spec/hyperdb')

const db = HyperDB.rocks('./my-timeseries.db', def)
const ts = new Timeseries(db)

const batch = await ts.exclusiveTransaction()
await batch.insert('cpu', [{ key: 'host', value: 'a' }], 'load', new Date(), 42)
await batch.flush()
```

## License

Apache-2.0
