var test = require('tape')
var create = require('./helpers/create')

test('put/list with a single layer', function (t) {
  t.plan(3)

  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.list('/', function (err, l) {
      t.error(err)
      t.same(l, ['b', 'a'])
    })
  })
})

test('put/get with two layers', function (t) {
  t.plan(3)

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
    db.list('/', function (err, l) {
      t.error(err)
      t.same(l, ['b', 'a', 'c'])
    })
  })
})

test('put/get with two layers and a deletion', function (t) {
  t.plan(4)

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
    db.delete('a', function (err) {
      t.error(err)
      db.list('/', function (err, l) {
        t.error(err)
        t.same(l, ['b', 'c'])
      })
    })
  })
})
