# union-hyperdb
[![CircleCI](https://circleci.com/gh/andrewosh/union-hyperdb.svg?style=svg)](https://circleci.com/gh/andrewosh/union-hyperdb)
[![JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)

union-hyperdb is a wrapper around multiple [hyperdbs](https://github.com/mafintosh/hyperdb) that supports nesting and/or layering of hyperdbs within a parent database:

1. Lightweight __forking__ and __layering__ operations are supported through a `parents` relationship. If an entry can't be found in the main tree, the search will traverse into all parents. This creates a union filesystem-like view over the underlying databases.
2. Cross-db __symlinking__ (of both live and versioned dbs) is supported through special `Link` entries. 

No external indexing is required, and sub-dbs are instantiated dynamically when first needed.

### Installation
`npm i multi-append-tree`

### Usage

```js
var hypercore = require('hypercore')
var uniondb = require('union-hyperdb')
var ram = require('random-access-memory')

// A hypercore factory function is required, because union-hyperdb constructs hyperdbs dynamically.
var factory = function (key, version, cb) {
  return hypercore(ram, key, { version })
}

var baseTree = factory()
var db = uniondb(baseTree, factory)
```

### API
union-hyperdb implements the hyperdb API, with the addition of the methods described below.

#### `var uniondb = uniondb(factory, opts)`

Since union-hyperdbs need to dynamically create sub-hyperdbs, it needs to be provided with a hypercore factory function.

`factory` is a function that takes a key and an optional version, and returns a hypercore:

`opts` are hyperdb options, but with the important addition of:
```js
{ 
  parents: [] // A list of { key: key, version: version} objects specifying parent append-trees.
}
```

#### `mount(name, target, opts cb)`

Create a link record that references another hyperdb (specified by `target`)
`name` is the symlink's absolute path
`target` can be either:
1. An object of the form `{ key: <key>, path: <path> }`
2. A string of the form `dat://<key>/<path>` or `<key>/<path>` (assuming the latter form is a dat key + a path).
`opts` is an optional object of the form:
```js
{
  version: <unspecified, as in latest> // The desired db version, if the link should be versioned (static).
}
```

#### `lexIterator(opts)`

Create a lexicographic iterator over the database. 
`opts` are LevelDB-style options (`gt`, `gte`, `lt`, `lte`, and `reverse`).

#### `index(cb)`

Compute a layer index over the entire database, such that lookups will require constant time (in the number of layers). 

__Note__: If one does *not* call `index` before calling read-intensive methods, then those methods might need to sequentially search each layer for content.
