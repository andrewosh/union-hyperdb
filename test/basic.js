var test = require('tape')
var create = require('./helpers/create')

test('put/get with a single layer', function (t) {
  t.plan(6)

  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.get('a', function (err, nodes) {
      t.error(err)
      t.same(nodes.length, 1)
      t.same(nodes[0].value, 'hello')
      db.get('b', function (err, value) {
        t.error(err)
        t.same(nodes.length, 1)
        t.same(nodes[0].value, 'goodbye')
      })
    })
  })
})
