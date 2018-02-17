var p = require('path')

var thunky = require('thunky')
var codecs = require('codecs')
var reduce = require('async-reduce')

var Trie = require('./lib/trie')
var messages = require('./lib/messages')

const META_PATH = '/META/'
const INDEX_PATH = '/INDEX/'
const DATA_PATH = '/DATA/'

module.exports = UnionDB

// This behavior is extracted towards the top so that it's super visible.
function resolveEntryConflicts (entries) {
  var winningEntry = {
    deleted: false,
    // Hopefully there are never more than 1e12 layers...
    layerIndex: 1e12
  }
  entries.forEach(function (entry) {
    if (entry.deleted) {
      // If any entry declares that this value has been deleted in this layer, then assume the
      // value has been deleted.
      winningEntry.deleted = true
    } else if (entry.layerIndex === 0) {
      // If any entry declares that the latest value is in the current layer, then use that value.
      winningEntry.layerIndex = 0
    } else {
      // Otherwise, the entry with the most recent layer index should win.
      winningEntry.layerIndex = Math.min(winningEntry.layerIndex, entry.layerIndex)
    }
  })
  return winningEntry
}

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

UnionDB.prototype._getMetadata = function (cb) {
  var self = this

  this.db.ready(function (err) {
    if (err) return cb(err)
    self.db.get(META_PATH, function (err, nodes) {
      if (err) return cb(err)
      if (nodes.length > 1) console.error('Conflict in metadata file -- using first node\'s value.')
      return cb(null, messages.Metadata.decode(nodes[0].value))
    })
  })
}

UnionDB.prototype._saveMetadata = function (cb) {
  var self = this

  this.db.ready(function (err) {
    if (err) return cb(err)
    self.db.put(META_PATH, messages.Metadata.encode({
      parents: [self.parent],
      indexed: self.indexed
    }), function (err) {
      if (err) return cb(err)
      return cb(null)
    })
  })
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

UnionDB.prototype._isIndexed = function (cb) {
  this._getMetadata(function (err, metadata) {
    if (err) return cb(err)
    return cb(null, metadata.indexed)
  })
}

UnionDB.prototype._load = function (cb) {
  var self = this

  this._getMetadata(function (err, metadata) {
    if (err) return cb(err)
    // TODO: handle the case of multiple parents? (i.e. a merge)
    self.parent = metadata.parents[0]
    if (self.parent) return self._loadParent(cb)
    return cb()
  })
}

UnionDB.prototype._save = function (cb) {
  var self = this

  this._saveMetadata(function (err) {
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

UnionDB.prototype._getIndex = function (cb) {
  if (!this.db) return cb(new Error('Attempting to list from an uninitialized database.'))
  var self = this

  this._isIndexed(function (err, indexed) {
    if (err) return cb(err)
    if (indexed || !self.parent) {
      // If already indexed, or there's no parent, just spit out the contents of the INDEX subtree.
      getLocalIndex(function (err, localIndex) {
        if (err) return cb(err)
        return cb(null, localIndex)
      })
    } else {
      // Otherwise, recursively get the index of the parent and merge with our INDEX subtree.
      self.parent._getIndex(function (err, parentIndex) {
        if (err) return cb(err)
        getLocalIndex(function (err, localIndex) {
          if (err) return cb(err)
          Object.keys(parentIndex).forEach(function (pkey) {
            if (!localIndex[pkey]) {
              var parentEntry = parentIndex[pkey]
              parentEntry.layerIndex++
              localIndex[pkey] = parentEntry
            }
          })
          return cb(null, localIndex)
        })
      })
    }
  })

  function getLocalIndex (cb) {
    var allEntries = {}
    var diffStream = this.db.createDiffStream(INDEX_PATH)
    diffStream.on('data', function (data) {
      if (data.type === 'put') {
        // We only care about the last put for each key in the layer.
        allEntries[data.name] = data.nodes.map(function (node) {
          return messages.Entry.decode(node)
        })
      }
    })
    diffStream.once('error', cb)
    diffStream.once('end', function () {
      Object.keys(allEntries).forEach(function (key) {
        var entries = allEntries[key]
        allEntries[key] = resolveEntryConflicts(entries)
      })
      return cb(null, allEntries)
    })
  }
}

// BEGIN Public API

UnionDB.prototype.get = function (key, cb) {
  var self = this

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

      var entries = nodes.map(function (node) {
        return messages.Entry.decode(node.value)
      })

      var resolvedEntry = resolveEntryConflicts(entries)

      if (resolvedEntry.deleted) {
        return cb(null, null)
      } else if (resolvedEntry.layerIndex === 0) {
        return self._get(0, key, cb)
      } else {
        return self._get(resolvedEntry.layerIndex, key, cb)
      }
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
      this._putIndex(key, 0, false, function (err) {
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

    self._putIndex(key, 0, true, function (err) {
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

  this.getIndex(function (err, index) {
    if (err) return cb(err)
    writeIndex(index)
  })

  function writeIndex (index) {
    self._putIndices(index, function (err) {
      if (err) return cb(err)
      self.indexed = true
      self._saveMetadata(function (err) {
        if (err) return cb(err)
        return cb()
      })
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
