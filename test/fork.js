var test = require('tape')
var create = require('./helpers/create')

test('can create a static fork', function (t) {
  create.fromLayers([
    [
     { type: 'put', key: 'a', value: 'hello' },
     { type: 'put', key: 'b', value: 'goodbye' }
    ]
  ], function (err, db) {
    t.error(err)
    db.fork(function (err, fork) {
      t.error(err)
      fork.get('a', function (err, nodes) {
        t.error(err)
        t.same(nodes[0].value, 'hello')
        t.end()
      })
    })
  })
})
