var p = require('path')
var EventEmitter = require('events').EventEmitter

var duplexify = require('duplexify')
var map = require('async-each')
var each = map
// var multiplex = require('multiplex')
// var pump = require('pump')
var inherits = require('inherits')
var bulk = require('bulk-write-stream')
var thunky = require('thunky')
var codecs = require('codecs')

var Trie = require('./lib/trie')
var messages = require('./lib/messages')

const META_PATH = '/META/'
const INDEX_PATH = '/INDEX/'
const DATA_PATH = '/DATA/'
const LINKS_PATH = '/LINKS'

module.exports = UnionDB

// This behavior is extracted to the top so that it's super visible.
function resolveEntryConflicts (entries) {
  var winningEntry = {
    deleted: false,
    // Hopefully there are never more than 1e9 layers...
    layerIndex: 1e9
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
    if (entry.link) winningEntry.link = entry.link
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

  if (this.opts.version) {
    this.versions = messages.Version.decode(this.opts.version)
    this.localVersion = this.versions.localVersion
    this.linkVersions = this.versions.linkVersions
  }

  this.localKey = null

  this._codec = (this.opts.valueEncoding) ? codecs(this.opts.valueEncoding) : null
  this._links = {}
  this._linkTrie = new Trie()
  this._factory = factory

  // Indexing the layers will record which layer last modified each entry (for faster lookups).
  // If a UnionDB is not indexed, each `get` will have to traverse the list of layers.
  this.indexed = false

  // Set in open.
  this._db = null

  var self = this

  this.ready = thunky(open)
  this.ready(function (err) {
    if (err) throw err
  })

  function open (cb) {
    self._factory(self.key, { checkout: self.localVersion }, function (err, db) {
      if (err) return cb(err)
      if (self.key && self.parent) {
        var error = new Error('Cannot specify both a parent key and layers.')
        return cb(error)
      }
      self._db = db
      self._db.ready(function (err) {
        if (err) return cb(err)

        self.localKey = self._db.local.key

        self._db.watch(LINKS_PATH, function () {
          // Refresh the links whenver any change.
          return self._loadLinks()
        })

        if (self.key) return self._load(cb)
        self.key = db.key
        return self._save(cb)
      })
    })
  }
}
inherits(UnionDB, EventEmitter)

UnionDB.prototype._getMetadata = function (cb) {
  var self = this

  this._db.ready(function (err) {
    if (err) return cb(err)
    self._db.get(META_PATH, function (err, nodes) {
      if (err) return cb(err)
      if (!nodes) return cb(null, null)
      if (nodes.length > 1) console.error('Conflict in metadata file -- using first node\'s value.')
      return cb(null, messages.Metadata.decode(nodes[0].value))
    })
  })
}

UnionDB.prototype._saveMetadata = function (cb) {
  var self = this

  this._db.ready(function (err) {
    if (err) return cb(err)
    self._db.put(META_PATH, messages.Metadata.encode({
      parents: [self.parent],
      indexed: self.indexed
    }), function (err) {
      if (err) return cb(err)
      return cb(null)
    })
  })
}

UnionDB.prototype._loadDatabase = function (record, cb) {
  var dbMetadata = (record.db) ? record.db : record
  this._createUnionDB(dbMetadata.key, {
    version: dbMetadata.version,
    valueEncoding: this.opts.valueEncoding
  }, function (err, db) {
    if (err) return cb(err)
    record.db = db
    return cb()
  }, function (err) {
    return cb(err)
  })
}

UnionDB.prototype._loadParent = function (cb) {
  if (this.parent) {
    return this._loadDatabase(this.parent, cb)
  }
  return cb()
}

UnionDB.prototype._isIndexed = function (cb) {
  this._getMetadata(function (err, metadata) {
    if (err) return cb(err)
    return cb(null, metadata.indexed)
  })
}

UnionDB.prototype._loadLinks = function (cb) {
  var self = this

  this._findEntries(LINKS_PATH, function (err, entries) {
    if (err) return cb(err)

    each(Object.values(entries), function (entry, next) {
      var linkRecord = entry.link
      // TODO: might need a better link identifier.
      var linkId = linkRecord.localPath

      if (self._links[linkId]) return next()

      if (self.linkVersions && self.linkVersions[linkRecord.localPath]) {
        linkRecord.db.version = self.linkVersions[linkRecord.localPath]
      }

      self._linkTrie.add(linkRecord.localPath, linkRecord)

      self._loadDatabase(linkRecord, function (err) {
        if (err) return cb(err)
        self._links[linkId] = linkRecord
        return next()
      })
    }, function (err) {
      if (!cb && err) throw err
      if (cb) return cb(err)
    })
  })
}

UnionDB.prototype._load = function (cb) {
  var self = this

  this._getMetadata(function (err, metadata) {
    if (err) return cb(err)
    if (!metadata) return cb()
    // TODO: handle the case of multiple parents? (i.e. a merge)
    self.parent = metadata.parents[0]
    self._loadParent(function (err) {
      if (err) return cb(err)
      return self._loadLinks(cb)
    })
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
  return cb(null, new UnionDB(this._factory, key, opts))
}

UnionDB.prototype._get = function (idx, key, cb) {
  var self = this

  if (!this._db) return process.nextTick(cb, new Error('Attempting to get from an uninitialized database.'))
  if (idx === 0) {
    return this._db.get(p.join(DATA_PATH, key), function (err, nodes) {
      if (err) return cb(err)
      if (!nodes) return cb(null, null)
      if (self._codec) {
        nodes.forEach(function (node) {
          node.value = self._codec.decode(node.value)
        })
      }
      return cb(null, nodes)
    })
  }
  return this.parent.db._get(idx - 1, key, cb)
}

UnionDB.prototype._makeIndex = function (key, idx, deleted, link) {
  var entry = {
    deleted: deleted,
    layerIndex: idx
  }
  if (link) entry.link = link
  return messages.Entry.encode(entry)
}

UnionDB.prototype._putIndex = function (key, idx, deleted, cb) {
  var indexPath = p.join(INDEX_PATH, key)

  var index = this._makeIndex(key, idx, deleted)

  this._db.put(indexPath, index, function (err) {
    return cb(err)
  })
}

UnionDB.prototype._putIndices = function (indices, cb) {
  var self = this

  this._db.batch(Object.keys(indices).map(function (key) {
    var value = indices[key]
    return {
      type: 'put',
      key: key,
      value: self._makeIndex(key, value.layerIndex, value.deleted)
    }
  }), function (err) {
    return cb(err)
  })
}

UnionDB.prototype._putLink = function (key, path, opts, cb) {
  if (typeof opts === 'function') return this._putLink(key, path, null, cb)
  opts = opts || {}
  var self = this

  var linkPath = p.join(LINKS_PATH, path)

  this._db.put(linkPath, this._makeIndex(linkPath, 0, false, {
    db: {
      key: key,
      version: opts.version
    },
    localPath: path,
    remotePath: opts.remotePath
  }), function (err) {
    if (err) return cb(err)
    return self._loadLinks(cb)
  })
}

UnionDB.prototype._getIndex = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    self._isIndexed(function (err, indexed) {
      if (err) return cb(err)
      if (indexed || !self.parent) {
        // If already indexed, or there's no parent, just spit out the contents of the INDEX subtree.
        getLocalIndex(function (err, localIndex) {
          if (err) return cb(err)
          return cb(null, localIndex)
        })
      } else {
        // Otherwise, recursively get the index of the parent and merge with our INDEX subtree.
        self.parent.db._getIndex(function (err, parentIndex) {
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
  })

  function getLocalIndex (cb) {
    var allEntries = {}
    var diffStream = self._db.createDiffStream(INDEX_PATH)
    diffStream.on('data', function (data) {
      if (data.type === 'put') {
        // We only care about the last put for each key in the layer.
        allEntries[data.name] = data.nodes.map(function (node) {
          return messages.Entry.decode(node.value)
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

UnionDB.prototype.mount = function (key, path, opts, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    return self._putLink(key, path, opts, cb)
  })
}

UnionDB.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, {}, opts)

  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    // If there's a link, recurse into the linked db.
    var link = self._linkTrie.get(key, { enclosing: true })
    if (link) {
      var remotePath = p.resolve(key.slice(link.localPath.length))
      if (link.remotePath) remotePath = p.resolve(p.join(link.remotePath, remotePath))
      return link.db.get(remotePath, opts, cb)
    }

    // Else, first check this database, then recurse into parents.
    self._db.get(p.join(INDEX_PATH, key), function (err, nodes) {
      if (err) return cb(err)
      if (!nodes && self.parent) {
        return self.parent.db.get(key, opts, cb)
      }

      if (!nodes) {
        return cb(null, null)
      }

      var entries = nodes.map(function (node) {
        return messages.Entry.decode(node.value)
      })

      // If there are any conflicting entries, resolve them according to the the strategy above.
      if (entries.length > 1) {
        self.emit('conflict', key, entries)
      }
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

    // If there's a link, recurse into the linked db.
    var link = self._linkTrie.get(key, { enclosing: true })
    if (link) {
      return link.db.put(key.slice(link.localPath), value, cb)
    }

    var encoded = (self._codec) ? self._codec.encode(value) : value

    var dataPath = p.join(DATA_PATH, key)

    // TODO: This operation should be transactional.
    self._db.put(dataPath, encoded, function (err) {
      if (err) return cb(err)
      self._putIndex(key, 0, false, function (err) {
        return cb(err)
      })
    })
  })
}

UnionDB.prototype.batch = function (records, cb) {
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
    return self._db.batch(toBatch, cb)
  })
}

UnionDB.prototype.createWriteStream = function () {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    self.batch(batch, cb)
  }
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

  this._getIndex(function (err, index) {
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

UnionDB.prototype._findEntries = function (dir, cb) {
  var self = this

  var entries = {}

  var stream = self._db.createDiffStream(dir)

  stream.on('data', function (data) {
    if (data.type === 'put') {
      var newEntries = data.nodes.map(function (node) {
        return messages.Entry.decode(node.value)
      })
      entries[data.name] = resolveEntryConflicts(newEntries)
    }
  })
  stream.on('end', function () {
    return cb(null, entries)
  })
  stream.on('error', function (err) {
    return cb(err)
  })
}

UnionDB.prototype._list = function (prefix, dir, cb) {
  var self = this

  self._isIndexed(function (err, indexed) {
    if (err) return cb(err)
    if (!indexed && self.parent) {
      self.parent.db.ready(function (err) {
        if (err) return cb(err)
        self.parent.db._findEntries(dir, function (err, parentEntries) {
          if (err) return cb(err)
          self._findEntries(dir, function (err, entries) {
            if (err) return cb(err)
            Object.assign(parentEntries, entries)
            return processEntries(null, parentEntries)
          })
        })
      })
    } else {
      self._findEntries(dir, processEntries)
    }
  })

  function processEntries (err, entries) {
    if (err) return cb(err)
    var list = []
    for (var name in entries) {
      if (!entries[name].deleted) {
        list.push(name.slice(prefix.length))
      }
    }
    return cb(null, list)
  }
}

UnionDB.prototype.list = function (dir, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    return self._list(INDEX_PATH, p.join(INDEX_PATH, dir), cb)
  })
}

UnionDB.prototype.watch = function (key, onchange) {
  var self = this

  // Check to see if there are any links that are prefixed by `key`. If so, watch each
  // one and propagate any changes through `onchange`
  var watchFuncs = {}

  // TODO: remove a watch if its corresponding link is deleted.
  // TODO: add a watch if a link is added to a watched directory (currently will have to re-watch).
  Object.keys(self._links).forEach(watchLink)

  watchFuncs.push(this._db.watch(p.join(DATA_PATH, key), function change (nodes) {
    return onchange(nodes.map(function (node) {
      node.key = node.key.slice(DATA_PATH.length)
      return node
    }))
  }))

  return unwatch

  function unwatch () {
    watchFuncs.forEach(function (func) {
      func()
    })
  }

  function watchLink (linkKey) {
    if (linkKey.startsWith(key) || key.startsWith(linkKey)) {
      var prefix = linkKey.slice(key.length)
      var suffix = key.slice(linkKey.length)
      var linkRecord = self._links[linkKey]

      watchFuncs.push(linkRecord.db.watch((suffix === '') ? '/' : suffix, function change (nodes) {
        return onchange(nodes.map(function (node) {
          node.key = p.join(prefix, node.key)
          return node
        }))
      }))
    }
  }
}

UnionDB.prototype.replicate = function (opts) {
  var self = this

  var proxy = duplexify()

  this.ready(function (err) {
    if (err) proxy.destroy(err)
    var stream = self._db.replicate(opts)
    proxy.setReadable(stream)
    proxy.setWritable(stream)
  })

    /*
  this.ready(function (err) {
    if (err) proxy.destroy(err)

    var parentStream = (self.parent) ? self.parent.db.replicate(opts) : null
    var myStream = self._db.replicate(opts)
    var stream = null

    if (parentStream) {
      stream = multiplex()
      var s1 = stream.createStream()
      var s2 = stream.createStream()
      pump(parentStream, s1, parentStream, function (err) {
        if (err) proxy.destroy(err)
      })
      pump(myStream, s2, myStream, function (err) {
        if (err) proxy.destroy(err)
      })
    } else {
      stream = myStream
    }
    proxy.setReadable(stream)
    proxy.setWritable(stream)
  })
  */

  return proxy
}

UnionDB.prototype.share = function (dir, cb) {

}

UnionDB.prototype.authorize = function (key) {
  return this._db.authorize(key)
}

UnionDB.prototype.version = function (cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    self._db.version(function (err, dbVersion) {
      if (err) return cb(err)

      var sortedLinks = Object.values(self._links).sort(function (l1, l2) {
        return l1.db.key < l2.db.key
      })

      map(sortedLinks, function (link, next) {
        link.db.version(function (err, version) {
          if (err) return next(err)
          return next(null, { path: link.localPath, version: version })
        })
      }, function (err, linkAndVersions) {
        if (err) return cb(err)

        var linkVersions = {}
        linkAndVersions.forEach(function (lak) {
          linkVersions[lak.path] = lak.version
        })

        return cb(null, messages.Version.encode({
          localVersion: dbVersion,
          linkVersions: linkVersions
        }))
      })
    })
  })
}
