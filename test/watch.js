var test = require('tape')
var create = require('./helpers/create')

test('should emit changes to watch functions', t => {
  create.fromLayers([
    [
      { type: 'put', key: '/a', value: 'hello' },
      { type: 'put', key: '/b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.watch('/', (nodes) => {
      t.true(nodes.length)
      t.same(nodes[0].key, 'a')
      t.end()
    })
    db.put('a', 'hello', function (err) {
      t.error(err)
    })
  })
})

test('should emit changes in symlinks to watch functions', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' }
    ],
    [
      { type: 'mount', key: 'b', remotePath: '/' },
      { type: 'put', key: 'c', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.watch('b', (nodes) => {
      t.true(nodes.length)
      t.same(nodes[0].key, 'b/d')
      t.end()
    })
    db.put('b/d', 'hello', err => {
      t.error(err)
    })
  })
})

test('should stop watching', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' }
    ],
    [
      { type: 'mount', key: 'b', remotePath: '/' },
      { type: 'put', key: 'c', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    var calls = 0
    var unwatch = db.watch('b', (nodes) => {
      t.true(nodes.length)
      t.same(nodes[0].key, 'b/d')
      calls++
    })
    db.put('b/d', 'hello', err => {
      t.error(err)
      unwatch()
      db.put('b/e', 'goodbye', err => {
        t.error(err)
        t.same(calls, 1)
        t.end()
      })
    })
  })
})
