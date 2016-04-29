#!/usr/bin/env node

// / check args
if (process.argv.length !== 3) {
  console.error('use: ' + process.argv[0] + ' [PATH_TO_CONFIG]')
  process.exit(1)
}

var fs = require('fs')
var Compare = require('../lib/Compare')

// / retrieve json file from arguments
var jsonFile = process.argv[2]

// / read config file
fs.readFile(jsonFile, function (err, data) {
  if (err) {
    // / error reading config file
    console.error('error reading config file:', err)
    process.exit(1)
  }
  var _compare = new Compare()
  // / compare dbs
  _compare.run(data, function (err) {
    if (err) {
      process.exit(1)
    }
  })
})
