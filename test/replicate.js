var test = require('tape')

var replicate = require('./helpers/replicate')
var create = require('./helpers/create')
var verify = require('./helpers/verify')

test('blah', function (t) {
  t.plan(5 + 3 * 1)

  create.two(function (err, db1, db2) {
    t.error(err)
    db1.put('cat', 'dog', function (err) {
      t.error(err)
      db2.get('cat', function (err, contents) {
        t.error(err)
        t.same(contents, null)
        replicate(db1, db2, function (err) {
          t.error(err)
          verify.values(t, db2, {
            'cat': 'dog'
          })
        })
      })
    })
  })
})

test('should replicate between two databases with no local changes', function (t) {
  t.plan(4 + 3 * 2)

  create.twoFromLayers([
    [
      { type: 'put', key: 'hello', value: 'goodbye' },
      { type: 'put', key: 'cat', value: 'dog' }
    ]
  ], function (err, db1, db2) {
    t.error(err)
    db2.get('cat', function (err, contents) {
      t.error(err)
      t.same(contents, null)
      replicate(db1, db2, function (err) {
        t.error(err)
        verify.values(t, db2, {
          'cat': 'dog',
          'hello': 'goodbye'
        })
      })
    })
  })
})
