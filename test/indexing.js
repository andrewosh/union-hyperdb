var p = require('path')
var test = require('tape')

var messages = require('../lib/messages')
var create = require('./helpers/create')

function verifyIndices (t, db, indicesByKey) {
  Object.keys(indicesByKey).forEach(function (key) {
    db._db.get(p.join('/INDEX', key), function (err, nodes) {
      t.error(err)
      t.same(nodes.length, 1)
      var decoded = messages.Entry.decode(nodes[0].value)
      t.same(decoded.layerIndex, indicesByKey[key])
    })
  })
}

function verifyValues (t, db, valuesByKey) {
  Object.keys(valuesByKey).forEach(function (key) {
    db.get(key, function (err, nodes) {
      t.error(err)
      t.same(nodes.length, 1)
      t.same(nodes[0].value, valuesByKey[key])
    })
  })
}

test('put/get with an index and one layer', function (t) {
  t.plan(2 + 3 * 2)

  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.index(function (err) {
      t.error(err)
      verifyValues(t, db, { 'a': 'hello', 'b': 'goodbye' })
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
      verifyValues(t, db, { 'a': 'dog', 'b': 'goodbye', 'c': 'human' })
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
      verifyIndices(t, db, { 'a': 1, 'b': 2, 'c': 0, 'd': 0 })
      verifyValues(t, db, { 'a': 'dog', 'b': 'goodbye', 'c': 'somewhere', 'd': 'rainbow' })
    })
  })
})
