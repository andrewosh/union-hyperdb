var test = require('tape')

var replicate = require('./helpers/replicate')
var create = require('./helpers/create')
var verify = require('./helpers/verify')

test('should replicate without any layers', function (t) {
  t.plan(3 + 3 * 1)

  create.two(function (err, db1, db2) {
    t.error(err)
    console.log('HERERR')
    db1.put('cat', 'dog', function (err) {
      t.error(err)
      replicate(db1, db2, function (err) {
        t.error(err)
        verify.values(t, db2, {
          'cat': 'dog'
        })
      })
    })
  })
})

test('should replicate between two databases with no local changes', function (t) {
  t.plan(2 + 3 * 2)

  create.twoFromLayers([
    [
      { type: 'put', key: 'hello', value: 'goodbye' },
      { type: 'put', key: 'cat', value: 'dog' }
    ]
  ], function (err, db1, db2) {
    t.error(err)
    replicate(db1, db2, function (err) {
      t.error(err)
      verify.values(t, db2, {
        'cat': 'dog',
        'hello': 'goodbye'
      })
    })
  })
})
