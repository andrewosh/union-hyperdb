var p = require('path')
var fs = require('fs')
var protobuf = require('protocol-buffers')

module.exports = protobuf(fs.readFileSync(p.join(__dirname, 'schema.proto')))
