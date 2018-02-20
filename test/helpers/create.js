var hyperdb = require('hyperdb')
var ram = require('random-access-memory')

var uniondb = require('../..')

var dbs = {}
function factory (key, opts, cb) {
  if (typeof opts === 'function') return factory(key, null, opts)
  opts = opts || {}

  if (key && dbs[key]) {
    var db = dbs[key]
    if (opts.checkout) {
      return cb(null, db.checkout(opts.checkout))
    }
    return cb(null, dbs[key])
  }

  db = hyperdb(ram, key, opts)
  db.ready(function (err) {
    if (err) return cb(err)
    dbs[db.key] = db
    return cb(null, db)
  })
}

function fromLayers (layerBatches, cb) {
  var currentDb = null
  var currentIdx = 0

  makeNextLayer()

  function makeNextLayer (err) {
    if (err) return cb(err)

    if (currentDb) {
      currentDb.version(function (err, version) {
        if (err) return cb(err)
        return makeUnionDB({
          parent: {
            key: currentDb.key,
            version: version
          },
          valueEncoding: 'json'
        })
      })
    } else {
      return makeUnionDB({
        valueEncoding: 'json'
      })
    }
  }

  function makeUnionDB (opts) {
    var batch = layerBatches[currentIdx++]
    var db = uniondb(factory, null, opts)
    currentDb = db
    db.batch(batch, function (err) {
      if (err) return cb(err)
      if (currentIdx === layerBatches.length) return cb(null, currentDb)
      return makeNextLayer()
    })
  }
}

module.exports = {
  fromLayers: fromLayers,
  factory: factory
}
