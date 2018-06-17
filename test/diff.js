var test = require('tape')
var equals = require('shallow-equals')

var create = require('./helpers/create')

test('simple diff, no links', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    validate(db)
  })

  var expected = [
    { left: 'a', right: null },
    { left: 'b', right: null }
  ]
  var seen = 0

  function validate (db) {
    var diff = db.createDiffStream(null, '/')
    diff.on('data', (diff) => {
      for (var i = 0; i < expected.length; i++) {
        if (equals(toKeys(diff), expected[i])) {
          seen++
          return
        }
      }
      t.fail('unexpected diff:', diff)
    })
    diff.on('end', () => {
      t.equals(expected.length, seen)
      t.end()
    })
    diff.on('error', (err) => {
      t.fail(err.message)
      t.end()
    })
  }
})

test('diff between versions, no links', t => {
  var version
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.version((err, v) => {
      t.error(err)
      version = v
      db.put('a', 'something', err => {
        t.error(err)
        validate(db)
      })
    })
  })

  var expected = [
    { left: 'a', right: 'a' }
  ]
  var seen = 0

  function validate (db) {
    var diff = db.createDiffStream(version, '/')
    diff.on('data', (diff) => {
      for (var i = 0; i < expected.length; i++) {
        if (equals(toKeys(diff), expected[i])) {
          seen++
          return
        }
      }
      t.fail('unexpected diff:', diff)
    })
    diff.on('end', () => {
      t.equals(expected.length, seen)
      t.end()
    })
    diff.on('error', (err) => {
      t.fail(err.message)
      t.end()
    })
  }

})


function toKeys (diff) {
  return {
    left: diff.left ? diff.left[0].key : null,
    right: diff.right ? diff.right[0].key : null
  }
}

