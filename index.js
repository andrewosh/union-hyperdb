var p = require('path')
var thunky = require('thunky')
var codecs = require('codecs')
var map = require('async-each')
var reduce = require('async-reduce')

var Trie = require('./lib/trie')
var messages = require('./lib/messages')

const META_PATH = '/META/'
const INDEX_PATH = '/INDEX/'
const DATA_PATH = '/DATA/'

module.exports = UnionDB

function UnionDB (factory, key, opts) {
  if (!(this instanceof UnionDB)) return new UnionDB(factory, key, opts)
  if (key && !(key instanceof Buffer) && !(typeof key === 'string')) {
    return new UnionDB(factory, null, opts)
  }
  this.opts = opts || {}
  this.parent = this.opts.parent
  this.key = key

  console.log('IN CONSTRUCTOR, key:', key)

  this._codec = (this.opts.valueEncoding) ? codecs(this.opts.valueEncoding) : null
  this._linkTrie = new Trie()
  this._factory = factory

  // Indexing the layers will record which layer last modified each entry (for faster lookups).
  // If a UnionDB is not indexed, each `get` will have to traverse the list of layers.
  this.indexed = false

  // Set in open.
  this.db = null

  this.ready = thunky(open)

  var self = this
  function open (cb) {
    self._factory(self.key, null, function (err, db) {
      if (err) return cb(err)
      if (self.key && self.parent) {
        var error = new Error('Cannot specify both a parent key and layers.')
        return cb(error)
      }
      self.db = db
      self.db.ready(function (err) {
        if (err) return cb(err)
        if (self.key) return self._load(cb)
        self.key = db.key
        return self._save(cb)
      })
    })
  }
}

UnionDB.prototype._loadParent = function (cb) {
  var self = this

  this._createUnionDB(this.parent.key, {
    checkout: this.parent.version,
    valueEncoding: this.opts.valueEncoding
  }, function (err, db) {
    if (err) return cb(err)
    console.log('after uniondb creation in _loadParent')
    self.parent.db = db
    return cb()
  }, function (err) {
    return cb(err)
  })
}

UnionDB.prototype._load = function (cb) {
  var self = this

  this.db.get(META_PATH, function (err, nodes) {
    if (err) return cb(err)
    if (nodes.length > 1) return cb(new Error('Read conflict in metadata file.'))
    var metadata = messages.Metadata.decode(nodes[0].value)
    // TODO: handle the case of multipe parents? (i.e. a merge)
    self.parent = metadata.parents[0]
    if (self.parent) return self._loadParent(cb)
    return cb()
  })
}

UnionDB.prototype._save = function (cb) {
  var self = this

  this.db.put(META_PATH, messages.Metadata.encode({
    parents: [this.parent]
  }), function (err) {
    if (err) return cb(err)
    if (self.parent) return self._loadParent(cb)
    return cb()
  })
}

UnionDB.prototype._createUnionDB = function (key, opts, cb) {
  console.log('in createUnionDB')
  return cb(null, new UnionDB(this._factory, key, opts))
}

/**
 * TODO: Linking is still WIP.
 */
UnionDB.prototype.mount = function (key, path, opts, cb) {
  var self = this

  this._saveLink(key, path, opts, function (err) {
    if (err) return cb(err)
    self._createUnionDB(key, opts, function (err, db) {
      if (err) return cb(err)
      self.linkTrie.add(path, db)
      return cb(null, db)
    })
  })
}

UnionDB.prototype._get = function (idx, key, cb) {
  var self = this

  if (!this.db) return cb(new Error('Attempting to get from an uninitialized database.'))
  if (idx === 0) {
    console.log('GETTING:', p.join(DATA_PATH, key))
    return this.db.get(p.join(DATA_PATH, key), function (err, nodes) {
      if (err) return cb(err)
      if (self._codec) {
        console.log('theres a codec')
        nodes.forEach(function (node) {
          node.value = self._codec.decode(node.value)
          console.log('updated node value to:', node.value)
        })
      }
      return cb(null, nodes)
    })
  }
  return this.parent._get(idx - 1, key, cb)
}

UnionDB.prototype._makeIndex = function (key, idx, deleted) {
  var dataPath = p.join(DATA_PATH, key)

  return messages.Entry.encode({
    deleted: deleted,
    layerIndex: idx,
    path: dataPath
  })
}

UnionDB.prototype._putIndex = function (key, idx, deleted, cb) {
  var indexPath = p.join(INDEX_PATH, key)

  var index = this._makeIndex(key, idx, deleted)

  console.log('PUTTING VALUE AT:', indexPath)
  this.db.put(indexPath, index, function (err) {
    return cb(err)
  })
}

UnionDB.prototype._putIndices = function (indices, cb) {
  var self = this

  this.db.batch(Object.keys(indices).map(function (key) {
    var value = indices[key]
    return {
      type: 'put',
      key: key,
      value: self._makeIndex(key, value.idx, value.deleted)
    }
  }), function (err) {
    return cb(err)
  })
}

UnionDB.prototype._listLayerStats = function (layer, cb) {
  if (!layer.db) return cb(new Error('Attempting to list from an uninitialized database.'))

  var entries = {}
  var diffStream = layer.db.createDiffStream(INDEX_PATH)
  diffStream.on('data', function (data) {
    if (data.type === 'put') {
      // We only care about the last put for each key in the layer.
      entries[data.name.slice(INDEX_PATH.length)] = data.nodes
    }
  })
  diffStream.once('error', cb)
  diffStream.once('end', function () {
    return cb(null, entries)
  })
}

// BEGIN Public API

UnionDB.prototype.get = function (key, cb) {
  var self = this

  var existingNodes = []
  var nextIndices = []
  var deleted = false

  this.ready(function (err) {
    if (err) return cb(err)
    self.db.get(p.join(INDEX_PATH, key), function (err, nodes) {
      if (err) return cb(err)
      console.log('getting key:', key, 'self.key:', self.key, 'self.parent is:', self.parent)
      if (!nodes && self.parent) {
        console.log('LOOKING UP IN PARENT')
        return self.parent.db.get(key, cb)
      }
      if (!nodes) return cb(null, null)

      nodes.forEach(function (node) {
        var entry = messages.Entry.decode(node.value)
        console.log('entry:', entry)
        if (entry.deleted) {
          deleted = true
        } else if (entry.layerIndex === 0) {
          existingNodes.push(entry.layerIndex)
        } else {
          nextIndices.push(entry.layerIndex)
        }
      })

      // If any nodes directly provide values, then stop the recursion and return those values.
      // Nodes that provide index references to other layers will be ignored -- values take precedence.
      if (existingNodes.length > 0) {
        return map(existingNodes, function (node, next) {
          console.log('doing a _get for key:', key)
          return self._get(0, key, next)
        }, function (err, nodes) {
          nodes = nodes.reduce(function (l, n) { return l.concat(n) })
          if (err) return cb(err)
          return cb(null, nodes)
        })
      }

      // If any nodes indicate that the entry has been deleted, then that also takes precedence over indices.
      // TODO: Ensure that the logic here is correct.
      if (deleted) return cb(null, null)

      // If multiple nodes provide indices, then the smallest (most recent) one wins.
      var nextIndex = nextIndices.reduce(function min (x, y) { return Math.min(x, y) })
      return self._get(nextIndex, key, cb)
    })
  })
}

UnionDB.prototype.put = function (key, value, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    var encoded = (self._codec) ? self._codec.encode(value) : value

    var dataPath = p.join(DATA_PATH, key)
    console.log('ADDING VALUE TO:', p.join(DATA_PATH, key))

    // TODO: this operation should be transactional.
    this.db.put(dataPath, encoded, function (err) {
      if (err) return cb(err)
      this._putIndex(key, self.layers.length - 1, false, function (err) {
        return cb(err)
      })
    })
  })
}

UnionDB.prototype.batch = function (records, cb) {
  console.log('in batch')
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    // Warning: records is mutated in this map to save an iteration.
    var stats = records.map(function (record) {
      var stat = {
        type: 'put',
        key: p.join(DATA_PATH, record.key),
        value: (self._codec) ? self._codec.encode(record.value) : record.value
      }
      record.key = p.join(INDEX_PATH, record.key)
      record.value = self._makeIndex(record.key, 0, false)
      return stat
    })
    var toBatch = records.concat(stats)
    console.log('in batch, toBatch:', toBatch)
    return self.db.batch(toBatch, cb)
  })
}

UnionDB.prototype.delete = function (key, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    this._putIndex(key, self.layers.length - 1, true, function (err) {
      return cb(err)
    })
  })
}

/**
 * Indexing with many layers is a very expensive operation unless an index has already been constructed
 * (either in this layer, or in a previous one -- the more recent the better).
 *
 * Diff streams have to be created for each layer, starting with the base layer. Entries must be
 * created one layer at a time.
 *
 * Any future forks of this database will be able to take advantage of precomputed indices,
 * reducing the cost of indexing subsequent layers.
 */
UnionDB.prototype.index = function (opts, cb) {
  if (typeof opts === 'function') return this.index(null, opts)
  opts = opts || {}
  var self = this

  // Updated in `reducer`.
  var currentIndex = 0

  this.ready(function (err) {
    if (err) return cb(err)
    reduce(self.layers, reducer, {}, writeIndex)
  })

  function reducer (existingEntries, nextLayer, cb) {
    self._listLayerStats(nextLayer, function (err, nextEntries) {
      if (err) return cb(err)

      for (var path in nextEntries) {
        var next = nextEntries[path]

        // If the existing layer does not contain any values for this key, then the subsequent
        // layer's nodes MUST not contain any layerIndex values.

        // If any nodes in `next` reflect modifications done to this key in the next layer
        // (either a deletion or a value update), then this layer should overwrite the previous.
        var deleted = false
        for (var i = 0; i < next.length; i++) {
          var decoded = messages.Entry.decode(next[i].value)
          if ((decoded.deleted !== undefined) || decoded.value) {
            deleted = deleted || decoded.deleted
            existingEntries[path] = {
              idx: currentIndex,
              deleted: deleted
            }
            break
          }
        }
      }

      currentIndex++
      return cb(null, existingEntries)
    })
  }

  function writeIndex (err, entries) {
    if (err) return cb(err)
    self._putIndices(entries, function (err) {
      if (err) return cb(err)
      self.indexed = true
      return cb(null)
    })
  }
}

UnionDB.prototype.replicate = function (opts) {

}

UnionDB.prototype.share = function (dir, cb) {

}

UnionDB.prototype.authorize = function (key) {
  return this.db.authorize(key)
}

UnionDB.prototype.version = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    return self.db.version(cb)
  })
}
