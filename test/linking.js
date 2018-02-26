var test = require('tape')

var create = require('./helpers/create')
var verify = require('./helpers/verify')

test('simple read from a mount works', function (t) {
  t.plan(1 + 3 * 3)

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
      'b/a': 'hello',
      'a': 'hello',
      'c': 'goodbye'
    })
  })
})

test('parent layers have frozen links', function (t) {
  t.plan(4 + 3 * 3)

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
  ], function (err, topLayer, layers) {
    t.error(err)

    var firstLayer = layers[0]
    var secondLayer = layers[1]

    firstLayer.put('a', 'world', function (err) {
      t.error(err)
      secondLayer.get('b/a', function (err, nodes) {
        t.error(err)
        t.same(nodes[0].value, 'world')
        verify.values(t, topLayer, {
          'b/a': 'hello',
          'c': 'goodbye',
          'd': 'something'
        })
      })
    })
  })
})

test('symlinks replicate')
test('symlinks are automatically frozen in layers')
