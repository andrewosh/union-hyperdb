var test = require('tape')

var create = require('./helpers/create')
var verify = require('./helpers/verify')

test('a link can be created and read', function (t) {
  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' },
      { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.sub('link', function (err, sub) {
      t.error(err)
      db.get('link', function (err, nodes) {
        t.error(err)
        t.same(nodes.length, 1)
        t.same(nodes[0].value.localPath, '/link')
        t.end()
      })
    })
  })
})

test('simple read from a mount works', function (t) {
  t.plan(13)

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
    verify.values(t, db, {
      '/b/a': 'hello',
      'a': 'hello',
      'c': 'goodbye'
    })
  })
})

test('read from a mount with a remote link works', function (t) {
  t.plan(9)

  create.fromLayers([
    [
      { type: 'put', key: 'a/b/c', value: 'hello' }
    ],
    [
      { type: 'mount', key: 'b', remotePath: '/a/b' },
      { type: 'put', key: 'd', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    verify.values(t, db, {
      '/b/c': 'hello',
      'd': 'goodbye'
    })
  })
})

test('parent layers have frozen links', function (t) {
  t.plan(16)

  create.fromLayers([
    [
      { type: 'put', key: 'a', value: 'hello' }
    ],
    [
      { type: 'mount', key: 'b', remotePath: '/' },
      { type: 'put', key: 'c', value: 'goodbye' }
    ],
    [
      { type: 'put', key: 'd', value: 'something' }
    ]
  ], async function (err, topLayer, layers) {
    t.error(err)

    var firstLayer = layers[0]
    var secondLayer = layers[1]

    await topLayer.index()

    firstLayer.put('a', 'world', function (err) {
      t.error(err)
      secondLayer.get('b/a', function (err, nodes) {
        t.error(err, 'right here ya are')
        t.same(nodes[0].value, 'world', 'wait not here?')
        verify.values(t, topLayer, {
          '/b/a': 'hello',
          'c': 'goodbye',
          'd': 'something'
        })
      })
    })
  })
})

test('symlinks replicate')
test('symlinks are automatically frozen in layers')
