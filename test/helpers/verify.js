var p = require('path')

var messages = require('../../lib/messages')

module.exports.indices = verifyIndices
module.exports.values = verifyValues

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
      console.log('OH THE VALUE IS:', nodes[0].value)
      t.same(nodes[0].value, valuesByKey[key])
    })
  })
}
