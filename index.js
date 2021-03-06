var p = require('path')
var EventEmitter = require('events').EventEmitter

var map = require('async-each')
var each = map
var inherits = require('inherits')
var bulk = require('bulk-write-stream')
var codecs = require('codecs')
var nanoiterator = require('nanoiterator')
var duplexify = require('duplexify')
var pumpify = require('pumpify')
var through = require('through2')
var merge = require('merge-stream')
var maybe = require('call-me-maybe')

var Trie = require('./lib/trie')
var messages = require('./lib/messages')

const META_PATH = 'META/'
const INDEX_PATH = 'INDEX/'
const DATA_PATH = 'DATA/'
const LINKS_PATH = 'LINKS/'

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

  this.opts = Object.assign({}, opts || {})
  this.parent = this.opts.parent
  this.key = key
  if (this.opts.map) {
    this.map = this.opts.map
    delete this.opts.map
  }
  if (this.opts.reduce) {
    this.reduce = this.opts.reduce
    delete this.opts.reduce
  }

  if (this.opts.version) {
    this.versions = messages.Version.decode(this.opts.version)
    this.localVersion = this.versions.localVersion
    this.linkVersions = this.versions.linkVersions
  }

  this.localKey = null

  this._codec = (this.opts.valueEncoding) ? codecs(this.opts.valueEncoding) : null
  this._links = {}
  this._linkTrie = new Trie()
  this._changeHandlers = []
  this._factory = factory

  // Indexing the layers will record which layer last modified each entry (for faster lookups).
  // If a UnionDB is not indexed, each `get` will have to traverse the list of layers.
  this.indexed = false

  // Set in open.
  this._db = null
  this._feeds = null

  var self = this

  this._ready = open()

  this.ready = function (cb) {
    if (!cb) return self._ready
    self._ready.then(function () {
      return cb(null)
    }).catch(function (err) {
      return cb(err)
    })
  }

  async function open () {
    if (self.key && self.parent) {
      var error = new Error('Cannot specify both a parent key and layers.')
      throw error
    }
    let opts = Object.assign({}, self.opts, { valueEncoding: 'binary', version: undefined })
    let db = factory(self.key, opts)
    if (self.localVersion) db = db.checkout(self.localVersion, opts)
    self._db = db
    return new Promise((resolve, reject) => {
      self._db.ready(function (err) {
        if (err) return reject(err)
        self.localKey = self._db.local.key
        self._feeds = self._db.feeds
        self._db.watch(LINKS_PATH, function () {
          // Refresh the links whenever any change.
          return self._loadLinks()
        })
        if (self.key) {
          return self._load(err => {
            if (err) return reject(err)
            return resolve()
          })
        }
        self.key = db.key
        return self._save(err => {
          if (err) return reject(err)
          return resolve()
        })
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
      if (!nodes || !nodes.length) return cb(null, null)
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

UnionDB.prototype._loadDatabase = function (record, opts, cb) {
  if (typeof opts === 'function') return this._loadDatabase(record, null, opts)
  opts = opts || {}

  var dbMetadata = (record.db) ? record.db : record

  this._createUnionDB(dbMetadata.key, {
    version: dbMetadata.version,
    valueEncoding: opts.valueEncoding || this.opts.valueEncoding,
    sparse: opts.sparse || false
  }, function (err, db) {
    if (err) return cb(err)
    record.db = db
    return cb()
  })
}

UnionDB.prototype._loadParent = function (cb) {
  if (this.parent) {
    return this._loadDatabase(this.parent, cb)
  }
  process.nextTick(cb, null)
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

      self._loadDatabase(linkRecord, {
        valueEncoding: linkRecord.valueEncoding,
        sparse: true
      }, function (err) {
        if (err) return next(err)
        self._links[linkId] = linkRecord
        self._linkTrie.add(linkRecord.localPath, linkRecord, {
          push: !!linkRecord.flat
        })
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
  let db = new UnionDB(this._factory, key, opts)
  db.ready(err => {
    if (err) return cb(err)
    return cb(null, db)
  })
}

UnionDB.prototype._prereturn = function (result) {
  if (!result) return null
  result = result.map(n => {
    return Object.assign({}, {...n}, {
      key: n.key.slice(DATA_PATH.length),
      value: (this._codec) ? this._codec.decode(n.value) : n.value
    })
  })
  if (this.map) result = result.map(n => this.map(n))
  if (this.reduce) result = result.reduce(this.reduce)
  return result
}

UnionDB.prototype._get = function (idx, key, cb) {
  var self = this

  if (!this._db) return process.nextTick(cb, new Error('Attempting to get from an uninitialized database.'))
  if (idx === 0) {
    return this._db.get(p.join(DATA_PATH, key), function (err, nodes) {
      if (err) return cb(err)
      if (!nodes) return cb(null, null)
      return cb(null, self._prereturn(nodes))
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

UnionDB.prototype._putIndex = async function (key, idx, deleted, cb) {
  var indexPath = p.join(INDEX_PATH, key)

  var index = this._makeIndex(key, idx, deleted)

  return maybe(cb, new Promise(async (resolve, reject) => {
    this._db.put(indexPath, index, err => {
      if (err) return reject(err)
      return resolve()
    })
  }))
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
    remotePath: opts.remotePath,
    valueEncoding: opts.valueEncoding,
    flat: !!opts.flat
  }), function (err) {
    if (err) return cb(err)
    return self._loadLinks(cb)
  })
}

UnionDB.prototype._delLink = function (path, cb) {
  var linkPath = p.join(LINKS_PATH, path)
  this._db.put(linkPath, this._makeIndex(linkPath, 0, true, cb))
}

UnionDB.prototype._getIndex = function (cb) {
  var self = this

  this._ready.then(function () {
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
  }).catch(function (err) {
    return cb(err)
  })

  function getLocalIndex (cb) {
    var allEntries = {}
    self._db.list(INDEX_PATH, function (err, nodes) {
      if (err) return cb(err)
      if (!nodes || !nodes.length) return cb(null, allEntries)
      // We only care about the last put for each key in the layer.
      nodes.forEach(function (nodes) {
        allEntries[nodes[0].key] = nodes.map(function (node) {
          return messages.Entry.decode(node.value)
        })
      })
      Object.keys(allEntries).forEach(function (key) {
        var entries = allEntries[key]
        allEntries[key] = resolveEntryConflicts(entries)
      })
      return cb(null, allEntries)
    })
  }
}

UnionDB.prototype._getEntryValues = function (key, nodes, cb) {
  var entries = nodes.map(function (node) {
    return messages.Entry.decode(node.value)
  })

  // If there are any conflicting entries, resolve them according to the the strategy above.
  if (entries.length > 1) {
    this.emit('conflict', key, entries)
  }
  var resolvedEntry = resolveEntryConflicts(entries)

  if (resolvedEntry.deleted) {
    return cb(null, null)
  }
  return this._get(resolvedEntry.layerIndex, key, cb)
}

// BEGIN Public API

UnionDB.prototype.mount = async function (key, path, opts, cb) {
  if (typeof opts === 'function') return this.mount(key, path, null, opts)
  return maybe(cb, new Promise(async (resolve, reject) => {
    await this.ready()
    this._putLink(key, path, opts, err => {
      if (err) return reject(err)
      return resolve()
    })
  }))
}

UnionDB.prototype.get = async function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  opts = opts || {}

  var self = this

  return maybe(cb, new Promise((resolve, reject) => {
    this._ready.then(function () {
      // If there's a link, recurse into the linked db.
      var link = self._linkTrie.get(key, { enclosing: true })
      if (link && link.localPath === key) {
        // TODO: hacky way of getting link records.
        // This shows that conflict handling for links needs to be fixed.
        var linkMeta = Object.assign({}, link, { db: null })
        return resolve([{
          key,
          value: linkMeta
        }])
      } else if (link) {
        var remotePath = p.resolve(key.slice(link.localPath.length))
        if (link.remotePath) remotePath = p.resolve(p.join(link.remotePath, remotePath))
        return link.db.get(remotePath, opts, (err, nodes) => {
          if (err) return reject(err)
          return resolve(nodes)
        })
      }

      // Else, first check this database, then recurse into parents.
      self._db.get(p.join(INDEX_PATH, key), function (err, nodes) {
        if (err) return cb(err)
        if ((!nodes || !nodes.length) && self.parent) {
          return self.parent.db.get(key, opts, (err, values) => {
            if (err) return reject(err)
            return resolve(values)
          })
        }

        if (!nodes || !nodes.length) return resolve(null)

        return self._getEntryValues(key, nodes, (err, values) => {
          if (err) return reject(err)
          return resolve(values)
        })
      })
    }).catch(function (err) {
      return reject(err)
    })
  }))
}

UnionDB.prototype.put = async function (key, value, cb) {
  var self = this

  return maybe(cb, new Promise((resolve, reject) => {
    this._ready.then(function () {
      // If there's a link, recurse into the linked db.
      if (self._links[key]) return cb(new Error('Cannot overwrite a symlink. Please delete first.'))
      var link = self._linkTrie.get(key, { enclosing: true })
      if (link) {
        return link.db.put(key.slice(link.localPath), value, err => {
          if (err) return reject(err)
          return resolve()
        })
      }

      var encoded = (self._codec) ? self._codec.encode(value) : value

      var dataPath = p.join(DATA_PATH, key)

      // TODO: This operation should be transactional.
      self._db.put(dataPath, encoded, function (err) {
        if (err) return cb(err)
        self._putIndex(key, 0, false, function (err) {
          if (err) return reject(err)
          return resolve()
        })
      })
    }).catch(function (err) {
      return reject(err)
    })
  }))
}

UnionDB.prototype.batch = async function (records, cb) {
  var self = this

  return maybe(cb, new Promise((resolve, reject) => {
    this._ready.then(function () {
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
      return self._db.batch(toBatch, (err, nodes) => {
        if (err) return reject(err)
        return resolve(nodes)
      })
    }).catch(function (err) {
      return reject(err)
    })
  }))
}

UnionDB.prototype.createWriteStream = function () {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    self.batch(batch, cb)
  }
}

UnionDB.prototype.del = async function (key, cb) {
  var self = this

  return maybe(cb, new Promise(async (resolve, reject) => {
    await this._ready
    await self._putIndex(key, 0, true)
    self._db.del(p.join(DATA_PATH, key), err => {
      if (err) return reject(err)
      if (self._links[key]) {
        self._delLink(key, err => {
          if (err) return reject(err)
          delete self._links[key]
          return resolve(null)
        })
      } else {
        return resolve(null)
      }
    })
  }))
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

  self._db.list(dir, function (err, nodes) {
    if (err) return cb(err)
    if (!nodes || !nodes.length) return cb(null, {})
    var entries = {}
    nodes.forEach(function (nodes) {
      var newEntries = nodes.map(function (node) {
        return messages.Entry.decode(node.value)
      })
      entries[nodes[0].key] = resolveEntryConflicts(newEntries)
    })
    return cb(null, entries)
  })
}

UnionDB.prototype._list = function (prefix, dir, cb) {
  var self = this

  this._isIndexed(function (err, indexed) {
    if (err) return cb(err)
    if (!indexed && self.parent) {
      self.parent.db.ready().then(function () {
        self.parent.db._findEntries(dir, function (err, parentEntries) {
          if (err) return cb(err)
          self._findEntries(dir, function (err, entries) {
            if (err) return cb(err)
            Object.assign(parentEntries, entries)
            return processEntries(null, parentEntries)
          })
        })
      }).catch(function (err) {
        return cb(err)
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

UnionDB.prototype.list = async function (dir, cb) {
  var self = this
  return maybe(cb, new Promise((resolve, reject) => {
    this._ready.then(function () {
      return self._list(INDEX_PATH, p.join(INDEX_PATH, dir), (err, values) => {
        if (err) return reject(err)
        return resolve(values)
      })
    }).catch(function (err) {
      return reject(err)
    })
  }))
}

/**
 * TODO: this will currently not traverse into symlinks *during* iteration. If the entire range
 * is contained within a symlink, then that will still work.
 */
UnionDB.prototype.lexIterator = function (opts) {
  // TODO: Support for non-indexed dbs.
  var self = this
  opts = opts || {}

  var ite = null
  var link = null

  return nanoiterator({ next, open })

  function next (cb) {
    ite.next((err, nodes) => {
      if (err) return cb(err)
      if (!nodes) return cb(null, null)
      if (link) {
        nodes.forEach(function (n) {
          n.key = p.join(link.localPath, n.key.slice(link.remotePath.length - 1))
        })
        return cb(null, nodes)
      }
      self._getEntryValues(nodes[0].key.slice(INDEX_PATH.length), nodes, (err, values) => {
        if (err) return cb(err)
        // If this is a deletion, get the next value.
        if (values.length) {
          // Remove deletions
          values = values.filter(v => !!v.value)
        } else {
          values.key = values.key.slice(DATA_PATH.length)
          if (!values.value) values = null
        }
        if (!values) return next(cb)
        return cb(null, values)
      })
    })
  }

  function open (cb) {
    self._isIndexed((err, indexed) => {
      if (err) return cb(err)
      if (!indexed && self.parent) return cb(new Error('Can only iterate over an indexed database'))

      // Check if this iteration is contained within a symlink.
      let end = opts.lt || opts.lte
      let start = opts.gt || opts.gte
      let startLink = start && self._linkTrie.get(start, { enclosing: true })
      let endLink = end && self._linkTrie.get(end, { enclosing: true })
      if (startLink && endLink) {
        if (!startLink.db.key.equals(endLink.db.key)) {
          return cb(new Error('Cannot iterate across multiple different symlinks'))
        }
        link = startLink
        ite = startLink.db.lexIterator(fixPaths(startLink.remotePath, startLink.localPath, opts))
        return cb(null)
      } else if (startLink || endLink) {
        return cb(new Error('Cannot currently iterate both locally and through symlinks'))
      }

      fixPaths(INDEX_PATH, '', opts)
      ite = self._db.lexIterator(opts)
      return cb(null)
    })
  }

  function fixPaths (base, local, opts) {
    if (opts.lt) opts.lt = p.join(base, opts.lt.slice(local.length))
    if (opts.gt) opts.gt = p.join(base, opts.gt.slice(local.length))
    if (opts.gte) opts.gte = p.join(base, opts.gte.slice(local.length))
    if (opts.lte) opts.lte = p.join(base, opts.lte.slice(local.length))
    if (!opts.gt && !opts.gte) opts.gt = base

    // If no lt or lte is specified, ensure that the iterator doesn't leave the 'base' subtree.
    if (!opts.lt && !opts.lte) {
      let lastChar = base.charCodeAt[base.length - 1]
      let lt = base
      lt[lt.length - 1] = lastChar + 1
      opts.lt = lt
    }

    return opts
  }
}

/**
 * Note: The diff stream currently *only* operates on the top layer.
 */
UnionDB.prototype.createDiffStream = function (other, prefix) {
  var self = this

  if (other) {
    var version = messages.Version.decode(other)
    var localCheckout = version.localVersion
    var linkCheckouts = version.linkVersions
  }

  var proxy = duplexify.obj()
  proxy.pause()
  proxy.setWritable(null)

  this.ready(err => {
    if (err) proxy.destroy(err)
    var localDiff = localCheckout ? this._db.checkout(localCheckout) : null
    var localStream = pumpify.obj(
      this._db.createDiffStream(localDiff, p.join(DATA_PATH, prefix)),
      through.obj(({ left, right }, enc, cb) => {
        return cb(null, {
          left: self._prereturn(left),
          right: self._prereturn(right)
        })
      })
    )

    var links = getLinks(prefix)
    if (links) {
      map(links, (link, next) => {
        return createLinkStream(link, next)
      }, (err, linkStreams) => {
        if (err) proxy.destroy(err)
        proxy.setReadable(merge.apply(merge, [localStream, ...linkStreams]))
        proxy.resume()
      })
    } else {
      proxy.setReadable(localStream)
      proxy.resume()
    }
  })

  return proxy

  function createLinkStream (link, cb) {
    var version = linkCheckouts[link.localPath]
    return cb(null, link.db.createDiffStream(version, linkPath(link, prefix)))
  }

  function getLinks (prefix) {
    return Object.keys(self._links)
      .filter(path => path.startsWith(prefix))
      .map(path => self._links[path])
  }
}

UnionDB.prototype.watch = function (key, onchange) {
  var self = this

  // Check to see if there are any links that are prefixed by `key`. If so, watch each
  // one and propagate any changes through `onchange`
  var watchers = []

  // TODO: remove a watch if its corresponding link is deleted.
  // TODO: add a watch if a link is added to a watched directory (currently will have to re-watch).
  Object.keys(self._links).forEach(watchLink)

  watchers.push(this._db.watch(p.join(DATA_PATH, key), onchange))

  self._changeHandlers.push(onchange)

  return unwatch

  function unwatch () {
    self._changeHandlers.splice(self._changeHandlers.indexOf(onchange), 1)
    watchers.forEach(function (watcher) {
      if (watcher.destroy) watcher.destroy()
      else watcher()
    })
  }

  function watchLink (linkKey) {
    if (linkKey.startsWith(key) || key.startsWith(linkKey)) {
      var suffix = key.slice(linkKey.length)
      var linkRecord = self._links[linkKey]

      watchers.push(linkRecord.db.watch((suffix === '') ? '/' : suffix, onchange))
    }
  }
}

UnionDB.prototype.replicate = function (opts) {
  var self = this
  return self._db.replicate(opts)
}

UnionDB.prototype.fork = function (opts, cb) {
  if (typeof opts === 'function') return this.fork(null, opts)
  opts = opts || {}
  var self = this

  if (opts.version) return process.nextTick(finish, opts.version)
  this.version(function (err, version) {
    if (err) return cb(err)
    finish(version)
  })

  function finish (version) {
    self._createUnionDB(null, Object.assign({}, self.opts, {
      parent: {
        key: self.key,
        version: version
      }
    }), cb)
  }
}

UnionDB.prototype.sub = async function (path, opts, cb) {
  if (typeof opts === 'function') return this.sub(path, null, opts)
  opts = opts || {}

  return maybe(cb, new Promise((resolve, reject) => {
    this._createUnionDB(null, opts, (err, db) => {
      if (err) return reject(err)
      this.mount(db.key, path, opts, err => {
        if (err) return reject(err)
        return resolve(db)
      })
    })
  }))
}

/**
 * TODO: should snapshot just fork?
 */
UnionDB.prototype.snapshot = function (opts, cb) {
  return this.fork(opts, cb)
}

UnionDB.prototype.authorize = function (key) {
  return this._db.authorize(key)
}

UnionDB.prototype.version = async function (cb) {
  var self = this

  return maybe(cb, new Promise((resolve, reject) => {
    this._ready.then(function () {
      self._db.version(function (err, dbVersion) {
        if (err) return reject(err)

        var sortedLinks = Object.values(self._links).sort(function (l1, l2) {
          return l1.db.key < l2.db.key
        })

        map(sortedLinks, function (link, next) {
          link.db.version(function (err, version) {
            if (err) return next(err)
            return next(null, { path: link.localPath, version: version })
          })
        }, function (err, linkAndVersions) {
          if (err) return reject(err)

          var linkVersions = {}
          linkAndVersions.forEach(function (lak) {
            linkVersions[lak.path] = lak.version
          })

          return resolve(messages.Version.encode({
            localVersion: dbVersion,
            linkVersions: linkVersions
          }))
        })
      })
    }).catch(function (err) {
      return reject(err)
    })
  }))
}

function linkPath (link, prefix) {
  return p.join(link.remotePath, prefix.slice(link.localPath.length))
}
