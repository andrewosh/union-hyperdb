var test = require('tape')

var Trie = require('../lib/trie')

test('simple gets', function (t) {
  var trie = new Trie()
  trie.add('a', 'hello')
  trie.add('b', 'goodbye')
  var a = trie.get('a')
  t.equal(a, 'hello')
  t.end()
})

test('exact subpaths', function (t) {
  var trie = new Trie()
  trie.add('/a', 'hello')
  trie.add('/a/b', 'goodbye')
  var a = trie.get('/a')
  var b = trie.get('/a/b')
  t.equal(a, 'hello')
  t.equal(b, 'goodbye')
  t.end()
})

test('enclosing path', function (t) {
  var trie = new Trie()
  trie.add('/a', 'hello')
  trie.add('/a/b', 'goodbye')
  var b = trie.get('/a/b')
  var c = trie.get('/a/c', { enclosing: true })
  var d = trie.get('/a/d')
  t.equal(b, 'goodbye')
  t.equal(c, 'hello')
  t.equal(d, null)
  t.end()
})

test('multiple values at path', function (t) {
  var trie = new Trie()
  trie.add('/a', 'hello')
  trie.add('/a', 'goodbye', { push: true })
  var results = trie.get('/a')
  t.deepEqual(results, ['hello', 'goodbye'])
  t.end()
})
