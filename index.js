var p = require('path')
var EventEmitter = require('events').EventEmitter

const LRU = require('lru')
var map = require('async-each')
var inherits = require('inherits')
var bulk = require('bulk-write-stream')
var codecs = require('codecs')
var nanoiterator = require('nanoiterator')
var duplexify = require('duplexify')
var pumpify = require('pumpify')
var pump = require('pump')
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
  this._codec = (this.opts.valueEncoding) ? codecs(this.opts.valueEncoding) : null
  this._dbs = new LRU(this.opts.dbCacheSize || 5)

  this._linkRecords = new Map()
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

  this.ready = async function (cb) {
    await this._ready
    if (cb) return cb()
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

    await ready(self._db)
    self._feeds = self._db.feeds
    self._db.watch(LINKS_PATH, function () {
      // Refresh the links whenever any change.
      return self._loadLinks()
    })
    if (self.key) {
      await self._load()
      self.emit('ready')
      return
    }
    self.key = db.key
    await self._save()
    self.emit('ready')
  }
}
inherits(UnionDB, EventEmitter)

UnionDB.prototype._getMetadata = async function () {
  var self = this

  return new Promise((resolve, reject) => {
    this._db.ready(function (err) {
      if (err) return reject(err)
      self._db.get(META_PATH, function (err, nodes) {
        if (err) return reject(err)
        if (!nodes || !nodes.length) return resolve(null)
        if (nodes.length > 1) console.error('Conflict in metadata file -- using first node\'s value.')
        return resolve(messages.Metadata.decode(nodes[0].value))
      })
    })
  })
}

UnionDB.prototype._saveMetadata = async function () {
  await ready(this._db)
  await put(this._db, META_PATH, messages.Metadata.encode({
    parents: [this.parent],
    indexed: this.indexed
  }))
}

UnionDB.prototype._loadDatabase = async function (key, opts) {
  if (typeof key === 'object' && !opts) {
    key = key.key
    opts = key
  }
  opts = opts || {}

  let db = this._dbs.get(key)

  db = await this._createUnionDB(key, {
    valueEncoding: opts.valueEncoding || this.opts.valueEncoding,
    sparse: opts.sparse || false
  })

  if (db) {
    if (opts.version) return db.checkout(opts.version)
    return db
  }

  this._dbs.set(db.key, db)
  return db
}

UnionDB.prototype._loadLinkDB = async function (linkRecord) {
  let version = this.linkVersions && this.linkVersions[linkRecord.localPath]
  return this._loadDatabase(linkRecord.db.key, {
    version: version || linkRecord.db.version,
    valueEncoding: linkRecord.valueEncoding || this.opts.valueEncoding
  })
}

UnionDB.prototype._loadParent = async function () {
  if (this.parent) {
    return this._loadDatabase(this.parent)
  }
}

UnionDB.prototype._isIndexed = async function () {
  let metadata = await this._getMetadata()
  return metadata.indexed
}

/**
 * TODO: Currently link management is pretty poor -- the in-memory index is refreshed frequently.
 *       This should be acceptable for small #s of links, but it will not scale effectively.
 */
UnionDB.prototype._loadLinks = async function (cb) {
  return maybe(cb, new Promise(async (resolve, reject) => {
    let entries = await this._findEntries('/', { onlyLinks: true })
    this._linkRecords = new Map()
    this._linkTrie = new Trie()
    for (let entry of entries) {
      let linkRecord = entry.link
      // TODO: might need a better link identifier.
      this._linkTrie.add(linkRecord.localPath, linkRecord, {
        push: false
      })
      this._linkRecords.set(linkRecord.localPath, linkRecord)
    }
    return resolve()
  }))
}

UnionDB.prototype._load = async function () {
  var self = this

  let metadata = await this._getMetadata()
  if (!metadata) return

    // TODO: handle the case of multiple parents? (i.e. a merge)
  self.parent = metadata.parents[0]
  await self._loadParent()
  await self._loadLinks()
}

UnionDB.prototype._save = async function () {
  await this._saveMetadata()
  if (this.parent) await this._loadParent()
}

UnionDB.prototype._createUnionDB = async function (key, opts) {
  let db = new UnionDB(this._factory, key, opts)
  await db.ready()
  return db
}

UnionDB.prototype._prereturn = function (result) {
  if (!result) return null
  result = result.map(n => {
    return {
      key: n.key.slice(DATA_PATH.length),
      value: (this._codec) ? this._codec.decode(n.value) : n.value,
      feed: n.feed,
      seq: n.seq
    }
  })
  if (this.map) result = result.map(n => this.map(n))
  if (this.reduce) result = result.reduce(this.reduce)
  return result
}

UnionDB.prototype._get = async function (idx, key) {
  if (!this._db) throw new Error('Attempting to get from an uninitialized database.')
  if (idx === 0) {
    let nodes = await get(this._db, p.join(DATA_PATH, key))
    if (!nodes || !nodes.length) return null
    return this._prereturn(nodes)
  }
  let parentDb = await this._loadDatabase(this.parent)
  return parentDb._get(idx - 1, key)
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
  path = makeAbsolute(path)
  if (typeof opts === 'function') return this._putLink(key, path, null, cb)
  opts = opts || {}
  var self = this

  var linkPath = p.join(LINKS_PATH, path)
  var entryPath = p.join(INDEX_PATH, path)

  let index = this._makeIndex(linkPath, 0, false, {
    db: {
      key: key,
      version: opts.version
    },
    localPath: path,
    remotePath: makeAbsolute(opts.remotePath),
    valueEncoding: opts.valueEncoding,
    flat: !!opts.flat
  })

  this._db.batch([
    {
      key: linkPath,
      value: index
    },
    {
      key: entryPath,
      value: index
    }
  ], function (err) {
    if (err) return cb(err)
    return self._loadLinks(cb)
  })
}

UnionDB.prototype._delLink = function (path, cb) {
  var linkPath = p.join(LINKS_PATH, path)
  this._db.put(linkPath, this._makeIndex(linkPath, 0, true, cb))
}

UnionDB.prototype._getEntryValues = async function (key, entry) {
  if (!entry) {
    let [indexed, nodes] = await Promise.all([
      this._isIndexed(),
      get(this._db, p.join(INDEX_PATH, key))
    ])
    if (!nodes || !nodes.length) {
      if (!indexed && this.parent) {
        let parentDb = await this._loadDatabase(this.parent)
        return parentDb._getEntryValues(key, entry)
      }
      return null
    }
    let entries = nodes.map(n => messages.Entry.decode(n.value))

    // If there are any conflicting entries, resolve them according to the the strategy above.
    if (entries.length > 1) {
      this.emit('conflict', key, entries)
    }
    entry = resolveEntryConflicts(entries)
  }
  if (entry.deleted) return { deleted: true }
  if (entry.link) return entry.link
  return this._get(entry.layerIndex, key)
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
  key = makeAbsolute(key)
  if (typeof opts === 'function') return this.get(key, null, opts)
  opts = opts || {}

  var self = this

  return maybe(cb, new Promise(async (resolve, reject) => {
    await this.ready()
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
      var remotePath = makeAbsolute(key.slice(link.localPath.length))
      if (link.remotePath) remotePath = p.join(link.remotePath, remotePath)
      let db = await this._loadLinkDB(link)
      let nodes = await db.get(remotePath, opts)
      return resolve(fixKeys(link, nodes))
    }

    let nodes = await this._getEntryValues(key)
    if (nodes && nodes.deleted) return resolve(null)

    if ((!nodes || !nodes.length) && self.parent) {
      let parentDb = await this._loadDatabase(self.parent)
      return resolve(parentDb.get(key, opts))
    }

    return resolve(nodes)
  }))
}

UnionDB.prototype.put = async function (key, value, cb) {
  key = makeAbsolute(key)
  return maybe(cb, new Promise(async (resolve, reject) => {
    await this.ready()
    // If there's a link, recurse into the linked db.
    var link = this._linkTrie.get(key, { enclosing: true })
    if (link) {
      let db = await this._loadLinkDB(link)
      return db.put(key.slice(link.localPath), value, err => {
        if (err) return reject(err)
        return resolve()
      })
    }

    var encoded = (this._codec) ? this._codec.encode(value) : value

    var dataPath = p.join(DATA_PATH, key)

    // TODO: This operation should be transactional.
    this._db.put(dataPath, encoded, err => {
      if (err) return cb(err)
      this._putIndex(key, 0, false, err => {
        if (err) return reject(err)
        return resolve()
      })
    })
  }))
}

UnionDB.prototype.batch = async function (records, cb) {
  var self = this

  return maybe(cb, new Promise(async (resolve, reject) => {
    await this.ready()
    // Warning: records is mutated in this map to save an iteration.
    var stats = records.map(function (record) {
      record.key = makeAbsolute(record.key)
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
    await this.ready()
    await self._putIndex(key, 0, true)
    self._db.del(p.join(DATA_PATH, key), err => {
      if (err) return reject(err)
      if (self._linkRecords.get(key)) {
        self._delLink(key, err => {
          if (err) return reject(err)
          self._linkRecords.delete(key)
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
UnionDB.prototype.index = async function (opts, cb) {
  if (typeof opts === 'function') return this.index(null, opts)
  opts = opts || {}
  var self = this

  return maybe(cb, new Promise(async (resolve, reject) => {
    let indexed = await this._isIndexed()

    // If we're already indexed, or there's nothing to index, return.
    if (indexed || !this.parent) {
      this.indexed = true
      if (!this.versions) await this._saveMetadata()
      return resolve()
    }

    // Else, apply the index streams for each parent (until we reach one that's indexed) in reverse order.
    // This can be *very* expensive -- one should index often.
    let entryStreams = await this._getIndexStreams()
    for (let i = entryStreams.length - 1; i >= 0; i--) {
      await indexLayer(entryStreams[i], i)
    }
    this.indexed = true
    await this._saveMetadata()
    await this._loadLinks()
    return resolve()

    async function indexLayer (entryStream, idx) {
      let entryTransformStream = through.obj(function (entry, enc, cb) {
        entry.layerIndex += idx
        let encoded = messages.Entry.encode(entry)
        if (entry.link) {
          this.push({ key: p.join(LINKS_PATH, entry.key), value: encoded })
        }
        return cb(null, { key: p.join(INDEX_PATH, entry.key), value: encoded })
      })
      return new Promise((resolve, reject) => {
        pump(entryStream, entryTransformStream, self._db.createWriteStream(), async err => {
          if (err) return reject(err)
          return resolve()
        })
      })
    }
  }))
}

UnionDB.prototype._getIndexStreams = async function () {
  let entryStream = this._createEntryStream('/', { recursive: true })
  if (await this._isIndexed()) return entryStream
  var parentStreams = []
  if (this.parent) {
    let parentDb = await this._loadDatabase(this.parent)
    parentStreams = await parentDb._getIndexStreams()
  }
  return [entryStream, ...parentStreams]
}

UnionDB.prototype._createEntryStream = function (prefix, opts) {
  prefix = prefix || '/'
  opts = opts || {}

  let root = opts.onlyLinks ? p.join(LINKS_PATH, prefix) : p.join(INDEX_PATH, prefix)
  let entryStream = this._db.createPrefixReadStream(root, opts)

  return pumpify.obj(entryStream, through.obj((nodes, enc, cb) => {
    if (!nodes || !nodes.length) return cb(null, {})
    var newEntries = nodes.map(function (node) {
      return messages.Entry.decode(node.value)
    })
    let resolved = resolveEntryConflicts(newEntries)
    // TODO: this could be better.
    resolved.key = nodes[0].key.split('/').slice(1).join('/')
    return cb(null, resolved)
  }))
}

UnionDB.prototype._findEntries = async function (prefix, opts) {
  let collected = await collect(this._createEntryStream(prefix, opts))
  return collected
}

UnionDB.prototype.createPrefixReadStream = function (prefix, opts) {
  let self = this

  var proxy = duplexify.obj()
  proxy.pause()
  proxy.setWritable(null)

  const createStream = async () => {
    await self.ready()

    let indexed = await this._isIndexed()
    if (!indexed) proxy.destroy(new Error('Can only create a read stream on an indexed database.'))

    var link = self._linkTrie.get(prefix, { enclosing: true })
    if (link) {
      let db = await this._loadLinkDB(link)
      var remotePath = p.resolve(prefix.slice(link.localPath.length))
      if (link.remotePath) remotePath = p.resolve(p.join(link.remotePath, remotePath))
      let linkStream = db.createPrefixReadStream(remotePath, opts)
      proxy.setReadable(pumpify.obj(linkStream, through.obj((nodes, enc, cb) => {
        return cb(null, fixKeys(link, nodes))
      })))
      proxy.resume()
      return
    }

    let entryStream = self._createEntryStream(prefix, opts)
    let resolvedStream = through.obj(async (entry, enc, cb) => {
      let resolved = await this._getEntryValues(entry.key, entry)
      if (resolved && resolved.deleted) return cb(null, null)
      return cb(null, resolved)
    })
    proxy.setReadable(pumpify.obj(entryStream, resolvedStream))
    proxy.resume()
  }
  createStream()

  return proxy
}

UnionDB.prototype.list = async function (prefix, cb) {
  return maybe(cb, new Promise(async (resolve, reject) => {
    return resolve(await collect(this.createPrefixReadStream(prefix, { recursive: false })))
  }))
}

/**
 * TODO: this will currently not traverse into symlinks *during* iteration. If the entire range
 * is contained within a symlink, then that will still work.
 * TODO: THIS IS CURRENTLY BROKEN
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
      map(links, ([path, link], next) => {
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
    self._loadLinkDB(link).then(db => {
      return cb(null, db.createDiffStream(version, linkPath(link, prefix)))
    }).catch(err => {
      return cb(err)
    })
  }

  function getLinks (prefix) {
    return [...self._linkRecords].filter(([k, v]) => k.startsWith(prefix))
  }
}

UnionDB.prototype.watch = function (key, onchange) {
  key = makeAbsolute(key)
  var self = this

  // Check to see if there are any links that are prefixed by `key`. If so, watch each
  // one and propagate any changes through `onchange`
  var watchers = []

  // TODO: remove a watch if its corresponding link is deleted.
  // TODO: add a watch if a link is added to a watched directory (currently will have to re-watch).
  self._linkRecords.forEach(watchLink)

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

  function watchLink (linkRecord, linkKey) {
    if (linkKey.startsWith(key) || key.startsWith(linkKey)) {
      var suffix = key.slice(linkKey.length)
      self._loadLinkDB(linkRecord).then((db) => {
        watchers.push(db.watch((suffix === '') ? '/' : suffix, onchange))
      })
    }
  }
}

UnionDB.prototype.replicate = function (opts) {
  var self = this
  return self._db.replicate(opts)
}

UnionDB.prototype.fork = async function (opts, cb) {
  if (typeof opts === 'function') return this.fork(null, opts)
  opts = opts || {}
  var self = this

  return maybe(cb, new Promise(async (resolve, reject) => {
    let version = opts.version
    if (!version) version = await this.version()
    let db = await self._createUnionDB(null, Object.assign({}, self.opts, {
      parent: {
        key: self.key,
        version: version
      }
    }))
    return resolve(db)
  }))
}

UnionDB.prototype.sub = async function (path, opts, cb) {
  if (typeof opts === 'function') return this.sub(path, null, opts)
  opts = opts || {}
  await this.ready()

  return maybe(cb, new Promise(async (resolve, reject) => {
    let link = this._linkTrie.get(path)
    if (link) {
      let db = await this._loadLinkDB(link)
      await db.ready()
      return resolve(db)
    }
    let db = await this._createUnionDB(null, opts)
    await this.mount(db.key, path, opts)
    return resolve(db)
  }))
}

/**
 * TODO: should snapshot just fork?
 */
UnionDB.prototype.snapshot = function (opts, cb) {
  return this.fork(opts, cb)
}

UnionDB.prototype.checkout = async function (version) {
  await this.ready()
  return this._createUnionDB(this.key, { version })
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

        // TODO: this is a very expensive thing to do if you have many links. Better approach to link versioning?
        var sortedLinks = [...self._linkRecords].map(([k, v]) => v).sort(function (l1, l2) {
          return l1.db.key < l2.db.key
        })

        map(sortedLinks, function (link, next) {
          self._loadLinkDB(link).then(db => {
            db.version(function (err, version) {
              if (err) return next(err)
              return next(null, { path: link.localPath, version: version })
            })
          }).catch(err => {
            return next(err)
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

function makeAbsolute (key) {
  if (!key) return '/'
  if (!key.startsWith('/')) return '/' + key
  return key
}

function fixKeys (link, nodes) {
  if (!nodes) return []
  return nodes.map(n => {
    return {
      key: p.join(link.localPath, p.relative(link.remotePath, '/' + n.key)),
      value: n.value,
      feed: n.feed,
      seq: n.seq
    }
  })
}

async function collect (stream) {
  return new Promise((resolve, reject) => {
    let result = []
    stream.on('data', d => result.push(d))
    stream.once('end', () => resolve(result))
    stream.once('error', err => reject(err))
  })
}

async function get (db, key) {
  return new Promise((resolve, reject) => {
    db.get(key, (err, nodes) => {
      if (err) return reject(err)
      return resolve(nodes)
    })
  })
}

async function put (db, key, value) {
  return new Promise((resolve, reject) => {
    db.put(key, value, err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}

async function ready (db) {
  return new Promise((resolve, reject) => {
    db.ready(err => {
      if (err) return reject(err)
      return resolve()
    })
  })
}
