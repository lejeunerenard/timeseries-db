const b4a = require('b4a')
const { getEncoding } = require('../spec/hyperschema/index.js')

const tagEncoding = getEncoding('@timeseries-db/tag')

function tagIndex (record) {
  return record.tags
}

function fieldIndex (record) {
  return [record.field]
}

module.exports = { tagIndex, fieldIndex }
