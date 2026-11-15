const Hyperdb = require('hyperdb/builder')
const Hyperschema = require('hyperschema')
const { generateSchema, generateDb } = require('./generate-schema.js')

const SCHEMA_DIR = './spec/hyperschema'
const DB_DIR = './spec/hyperdb'

const schema = Hyperschema.from(SCHEMA_DIR)
generateSchema(schema)
Hyperschema.toDisk(schema)

const db = Hyperdb.from(SCHEMA_DIR, DB_DIR)
generateDb(db)
Hyperdb.toDisk(db, { esm: false })
