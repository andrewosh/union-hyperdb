var hyperdb = require('hyperdb')
var ram = require('random-access-memory')
var map = require('async-each')

var dbs = {}
function factory (key, opts, cb) {
  if (key && dbs[key]) return dbs[key]

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
        return next(null, {
          key: db.key
        })
      })
    })
  }, function (err, layers) {
    if (err) return cb(err)
    factory(null, { valueEncoding: 'json' }, {
      layers: layers
    }, cb)
  })
}

module.exports = {
  fromLayers: fromLayers
}
