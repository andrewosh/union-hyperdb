var hyperdb = require('hyperdb')
var ram = require('random-access-memory')
var each = require('async-each')

var uniondb = require('../..')

var dbs = {}
function factory (key, opts, cb) {
  if (typeof opts === 'function') return factory(key, null, opts)
  if (key && dbs[key]) return cb(null, dbs[key])

  var db = hyperdb(ram, key, opts)
  db.ready(function (err) {
    if (err) return cb(err)
    dbs[db.key] = db
    return cb(null, db)
  })
}

function fromLayers (layerBatches, cb) {
  var currentDb = null

  each(layerBatches, function (batch, next) {
    if (currentDb) {
      currentDb.version(function (err, version) {
        if (err) return next(err)
        return makeNextLayer({
          parent: {
            key: currentDb.key,
            version: version
          },
          valueEncoding: 'json'
        })
      })
    } else {
      return makeNextLayer({
        valueEncoding: 'json'
      })
    }

    function makeNextLayer (opts) {
      var db = uniondb(factory, null, opts)
      currentDb = db
      db.batch(batch, next)
    }
  }, function (err) {
    if (err) return cb(err)
    return cb(null, currentDb)
  })
}

module.exports = {
  fromLayers: fromLayers
}
