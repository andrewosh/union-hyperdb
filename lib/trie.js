function normalize (path) {
  if (path.startsWith('/')) return path.slice(1)
  return path
}

function Trie () {
  this.trie = { nodes: {} }
}

Trie.prototype.add = function (path, data, opts) {
  opts = opts || {}
  path = normalize(path)
  var list = path.split('/')
  var trie = this.trie
  for (var i = 0; i < list.length; i++) {
    var next = trie.nodes[list[i]] || { nodes: {} }
    trie.nodes[list[i]] = next
    trie = next
  }

  trie.path = path

  if (opts.push) {
    if (!trie.value) trie.value = []
    if (!(trie.value instanceof Array)) trie.value = [trie.value]
    trie.value.push(data)
  } else {
    trie.value = data
  }
}

Trie.prototype.get = function (path, opts) {
  opts = opts || {}
  path = normalize(path)

  var list = path.split('/')
  var trie = this.trie
  for (var i = 0; i < list.length; i++) {
    var next = trie.nodes[list[i]]
    if (next) trie = next
  }

  if (trie.path !== path && !opts.enclosing) return null
  return trie.value
}

module.exports = Trie
