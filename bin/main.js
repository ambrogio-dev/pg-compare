#!/usr/bin/env node

/**
 * TODO DDL
 *
 * table
 *   all constraints (pkeys DONE, unique DONE, check etc)
 *   comment DONE
 * column
 *   comment DONE
 *   compare type DONE, nullable DONE, pkey DONE IN TABLE, defaul DONE
 *
 * TODO DATA
 *   sync (NB config select tables)
 *   sequences (set current value) DONE
 *
 * DONE
 * tables / columns
 * fkeys
 * functions
 * indexes
 * types (enum)
 * views
 *
 */

// / check args
if (process.argv.length !== 3) {
  console.error('use: ' + process.argv[0] + ' [configFile]')
  process.exit(1)
}

var fs = require('fs')

var DbCompare = require('../lib/DbCompare')

var jsonFile = process.argv[2]

// / read config file
fs.readFile(jsonFile, function (err, data) {
  if (err) {
    console.error('error reading config file:', err)
    process.exit(1)
  }
  var _dbCompare = new DbCompare()
  var _check = _dbCompare.run(data, function (err) {
    if (err) {
      process.exit(1)
    }
  })
})
