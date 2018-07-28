var test = require('tape')

var create = require('./helpers/create')
var verify = require('./helpers/verify')

test('put/get with an index and one layer', function (t) {
  t.plan(2 + 3 * 2)

  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    console.log('calling index')
    db.index(function (err) {
      t.error(err)
      console.log('called index')
      verify.values(t, db, { 'a': 'hello', 'b': 'goodbye' })
    })
  })
})

test('put/get with an index and two layers', function (t) {
  t.plan(2 + 3 * 3)

  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ],
    [
     { type: 'put', key: 'a', value: 'dog' },
     { type: 'put', key: 'c', value: 'human' }
    ]
  ], function (err, db) {
    t.error(err)
    db.index(function (err) {
      t.error(err)
      verify.values(t, db, { 'a': 'dog', 'b': 'goodbye', 'c': 'human' })
    })
  })
})

test('index entries are correctly added', function (t) {
  t.plan(2 + 3 * 4 + 3 * 4)
  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ],
    [
     { type: 'put', key: 'a', value: 'dog' },
     { type: 'put', key: 'c', value: 'human' }
    ],
    [
     { type: 'put', key: 'c', value: 'somewhere' },
     { type: 'put', key: 'd', value: 'rainbow' }
    ]
  ], function (err, db) {
    t.error(err)
    db.index(function (err) {
      t.error(err)
      verify.indices(t, db, { 'a': 1, 'b': 2, 'c': 0, 'd': 0 })
      verify.values(t, db, { 'a': 'dog', 'b': 'goodbye', 'c': 'somewhere', 'd': 'rainbow' })
    })
  })
})
