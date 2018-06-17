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
      return store.get(key, opts)
    }

    function dbFactory (key, opts) {
      opts.lex = true
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
        process.nextTick(makeNextLayer)
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
      db1.ready(err => {
        if (err) return cb(err)
        var db2 = uniondb(f1, db1.key, { valueEncoding: 'utf8' })
        return cb(null, db1, db2)
      })
    })
  }
}

function one (opts, cb) {
  makeFactory((err, f1) => {
    if (err) return cb(err)
    let db1 = uniondb(f1, Object.assign(opts, { valueEncoding: 'utf-8' }))
    db1.ready(err => {
      if (err) return cb(err)
      return cb(null, db1)
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
    db1.ready(err => {
      if (err) return cb(err)
      var db2 = uniondb(f2, db1.key, { valueEncoding: 'utf8' })
      // db2 will not be ready until the first remote-update.
      return cb(null, db1, db2)
    })
  }
}

module.exports = {
  fromLayers,
  twoFromLayers,
  two,
  one
}
