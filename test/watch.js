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
    db.watch('/', () => {
      t.end()
    })
    db.put('a', 'hello', function (err) {
      t.error(err)
    })
  })
})

test('should emit deletions', t => {
  create.fromLayers([
    [
      { type: 'put', key: '/a', value: 'hello' },
      { type: 'put', key: '/b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.watch('/', () => {
      t.end()
    })
    db.del('a', function (err) {
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
    db.watch('b', () => {
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
    var unwatch = db.watch('b', () => {
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
