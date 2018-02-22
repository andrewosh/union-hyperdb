var hyperdb = require('hyperdb')
var ram = require('random-access-memory')

var uniondb = require('../..')

function makeFactory () {
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

    console.log('CREATING WITH KEY:', key)
    db = hyperdb(ram, key, opts)
    db.ready(function (err) {
      if (err) return cb(err)
      dbs[db.key] = db
      return cb(null, db)
    })
  }
  return factory
}

function fromLayers (layerBatches, cb) {
  var currentDb = null
  var currentIdx = 0

  // Share hyperdbs between layers.
  var factory = makeFactory()

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
          valueEncoding: 'utf8'
        })
      })
    } else {
      return makeUnionDB({
        valueEncoding: 'utf8'
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

function twoFromLayers (layerFiles, cb) {
  fromLayers(layerFiles, function (err, db1) {
    if (err) return cb(err)
    db1.ready(function (err) {
      if (err) return cb(err)
      var db2 = uniondb(makeFactory(), db1.key, { valueEncoding: 'utf8' })
      db2.ready(function (err) {
        if (err) return cb(err)
        return cb(null, db1, db2)
      })
    })
  })
}

function two (cb) {
  var db1 = uniondb(makeFactory(), { valueEncoding: 'utf8' })
  db1.ready(function (err) {
    if (err) return cb(err)
    var db2 = uniondb(makeFactory(), db1.key, { valueEncoding: 'utf8' })
    db2.ready(function (err) {
      if (err) return cb(err)
      return cb(null, db1, db2)
    })
  })
}

module.exports = {
  fromLayers: fromLayers,
  twoFromLayers: twoFromLayers,
  two: two,
  makeFactory: makeFactory
}
