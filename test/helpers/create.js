var hyperdb = require('hyperdb')
var ram = require('random-access-memory')
var map = require('async-each')

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
  map(layerBatches, function (batch, next) {
    factory(null, { valueEncoding: 'json' }, function (err, db) {
      if (err) return next(err)
      db.batch(batch, function (err) {
        if (err) return next(err)
        db.version(function (err, version) {
          if (err) return next(err)
          return next(null, {
            key: db.key,
            version: version
          })
        })
      })
    })
  }, function (err, layers) {
    if (err) return cb(err)
    console.log('layers:', layers)
    var db = uniondb(factory, null, {
      valueEncoding: 'json',
      layers: layers
    })
    db.ready(function (err) {
      if (err) return cb(err)
      return cb(null, db)
    })
  })
}

module.exports = {
  fromLayers: fromLayers
}
