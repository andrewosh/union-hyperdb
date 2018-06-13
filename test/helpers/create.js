const p = require('path')

const hyperdb = require('hyperdb')
const each = require('async-each')
const corestore = require('corestore')

const uniondb = require('../..')

const STORAGE_DIR = p.join(__dirname, '..', 'test-storage')
var idx = 0

async function makeFactory (cb) {
  var store = corestore(p.join(STORAGE_DIR, '' + idx++), {
    network: {
      disable: true
    }
  })
  store.ready(err => {
    if (err) return cb(err)

    function coreFactory (key, opts) {
      console.log('GETTING CORE:', key, 'opts:', opts)
      return store.get(key, opts)
    }

    function dbFactory (key, opts) {
      console.log('GETTING DB:', key, 'opts:', opts)
      return hyperdb(coreFactory, key, opts)
    }

    return cb(null, dbFactory)
  })
}

async function fromLayers (layerBatches, cb) {
  var dbs = []
  var currentDb = null
  var currentIdx = 0

  var factory
  makeFactory((err, f) => {
    if (err) return cb(err)
    factory = f
    makeNextLayer()
  })

  function makeNextLayer (err) {
    if (err) return cb(err)
    if (currentIdx === layerBatches.length) return cb(null, currentDb, dbs)
    if (currentDb) {
      currentDb.version(function (err, version) {
        if (err) return cb(err)
        return makeUnionDB({
          parent: {
            key: currentDb.key,
            version: version
          },
          valueEncoding: 'utf-8'
        })
      })
    } else {
      return makeUnionDB({
        valueEncoding: 'utf-8'
      })
    }
  }

  function makeUnionDB (opts) {
    var batch = layerBatches[currentIdx++]
    var db = uniondb(factory, null, opts)
    db.ready(err => {
      if (err) throw err
      each(batch, function (cmd, next) {
        switch (cmd.type) {
          case 'put':
            db.put(cmd.key, cmd.value, next)
            break
          case 'mount':
            if (cmd.versioned) {
              currentDb.version(function (err, version) {
                if (err) throw err
                db.mount(currentDb.key, cmd.key, {
                  version: version,
                  remotePath: cmd.remotePath
                }, next)
              })
            } else {
              db.mount(currentDb.key, cmd.key, {
                remotePath: cmd.remotePath
              }, next)
            }
            break
          case 'delete':
            db.delete(cmd.key, next)
            break
        }
      }, function (err) {
        if (err) throw err
        currentDb = db
        dbs.push(db)
        return makeNextLayer()
      })
    })
  }
}

function twoFromLayers (layerFiles, cb) {
  makeFactory((err, f1) => {
    if (err) return cb(err)
    finish(f1)
  })

  function finish (f1) {
    fromLayers(layerFiles, function (err, db1) {
      if (err) return cb(err)
      db1.ready().then(function () {
        var db2 = uniondb(f1, db1.key, { valueEncoding: 'utf8' })
        db2.ready().then(function () {
          return cb(null, db1, db2)
        }).catch(function (err) {
          return cb(err)
        })
      }).catch(function (err) {
        return cb(err)
      })
    })
  })
}

function two (cb) {
  makeFactory((err, f1) => {
    if (err) return cb(err)
    makeFactory((err, f2) => {
      if (err) return cb(err)
      finish(f1, f2)
    })
  })

  function finish (f1, f2) {
    var db1 = uniondb(f1, { valueEncoding: 'utf8' })
    db1.ready().then(function () {
      var db2 = uniondb(f2, db1.key, { valueEncoding: 'utf8' })
      db2.ready().then(function () {
        return cb(null, db1, db2)
      }).catch(function (err) {
        return cb(err)
      })
    }).catch(function (err) {
      return cb(err)
    })
  }
}

module.exports = {
  fromLayers,
  twoFromLayers,
  two
}
