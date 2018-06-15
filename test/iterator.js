var test = require('tape')
var create = require('./helpers/create')

test('can iterate over local values without layers', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'z', value: 'world' },
      { type: 'put', key: 'b', value: 'goodbye' },
      { type: 'put', key: 'd', value: 'dog' },
      { type: 'put', key: 'f', value: 'yes' }
    ]
  ], (err, db) => {
    t.error(err)
    db.index(err => {
      t.error(err)
      let keys = ['a', 'b', 'd', 'f', 'z']
      testIteratorOrder(t, false, db.lexIterator(), keys, err => {
        t.error(err)
        t.end()
      })
    })
  })
})

test('multiple layer iteration fails without indexing', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'z', value: 'world' },
      { type: 'put', key: 'b', value: 'goodbye' },
      { type: 'put', key: 'd', value: 'dog' },
      { type: 'put', key: 'f', value: 'yes' }
    ]
  ], (err, db) => {
    t.error(err)
    let keys = ['a', 'b', 'd', 'f', 'z']
    let ite = db.lexIterator()
    ite.next((err, value) => {
      t.true(err)
      t.end()
    })
  })
})

test('can iterate over local values in multiple layers', t => {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'z', value: 'world' },
    ],
    [
      { type: 'put', key: 'b', value: 'goodbye' },
      { type: 'put', key: 'd', value: 'dog' },
      { type: 'put', key: 'f', value: 'yes' }
    ]
  ], (err, db) => {
    t.error(err)
    db.index(err => {
      t.error(err)
      let keys = ['a', 'b', 'd', 'f', 'z']
      testIteratorOrder(t, false, db.lexIterator(), keys, err => {
        t.error(err)
        t.end()
      })
    })
  })
})

test('can iterate within a single symlink')

// Copied from hyperdb

function testIteratorOrder (t, reverse, iterator, expected, done) {
  var sorted = expected.slice().sort()
  if (reverse) sorted.reverse()
  each(iterator, onEach, onDone)
  function onEach (err, node) {
    t.error(err, 'no error')
    console.log('NODE:', node)
    var key = node.key || node[0].key
    t.same(key, sorted.shift())
  }
  function onDone () {
    t.same(sorted.length, 0)
    if (done === undefined) t.end()
    else done()
  }
}

function each (ite, cb, done) {
  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return done()
    cb(null, node)
    ite.next(loop)
  })
}
