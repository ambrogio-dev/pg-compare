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
var pg = require('pg')
var tools = require('a-toolbox')

var Delta = require('../lib/Delta')
var Schema = require('../lib/Schema')
var log = require('../lib/log')

var jsonFile = process.argv[2]

/**
* Check if config json given in input has all mandatory fields
* @public
* @function
* @param {object} config Configuration parameters
*/
var checkConfig = function (config) {
  /**
  * Check if connection parameters are ok
  * @private
  * @function
  * @param {object} Connection parameters
  * @param {string} Connection to check (connection1|connection2)
  */
  var _checkConnectionParams = function (conn, key) {
    if (!conn) {
      console.error('error in config file, missing connection1 param')
      process.exit(1)
    } else {
      if (!conn.host) {
        console.error('error in config file, missing host param for ' + key)
        process.exit(1)
      }
      if (!conn.user) {
        console.error('error in config file, missing user param for ' + key)
        process.exit(1)
      }
      if (!conn.password) {
        console.error('error in config file, missing password param for ' + key)
        process.exit(1)
      }
      if (!conn.database) {
        console.error('error in config file, missing database param for ' + key)
        process.exit(1)
      }
    }
  }
  // / check both connection parameters
  _checkConnectionParams(config.connection1, 'connection1')
  _checkConnectionParams(config.connection2, 'connection2')
  // / check if schema parameters are present
  if (!config.compare || !config.compare.schema || Object.keys(config.compare.schema).length === 0) {
    console.error('error in config file, missing schema param')
    process.exit(1)
  }
}

// / read config file
fs.readFile(jsonFile, function (err, data) {
  if (err) {
    console.error('error reading config file:', err)
    process.exit(1)
  }
  var _config
  // / try parsing the json file in input
  try {
    _config = JSON.parse(data)
  } catch (ex) {
    console.error('error parsing config file:', ex)
    process.exit(1)
  }
  // / check if config parameters are ok
  checkConfig(_config)

  log.setVerbose(_config.verbose)
  var _conn1 = _config.connection1
  var _conn2 = _config.connection2
  var _db1 = new pg.Client(_conn1)
  var _db2 = new pg.Client(_conn2)

  var _schema = _config.compare.schema
  var _options = _config.compare.options

  // / default owner is user of connection 2
  if (!_options.owner) {
    _options.owner = _conn2.connection.user
  }

  /**
  * Compare database 1 and database 2
  * @private
  * @function
  */
  var _compare = function () {
    // / init both schema
    var _schema1 = new Schema(_db1, _schema)
    // / don't load table rows for schema 2, only sync from schema 1
    var _schemaClone = JSON.parse(JSON.stringify(_schema))
    _schemaClone.rows = null
    var _schema2 = new Schema(_db2, _schemaClone)

    var _job = new tools.tasks(function () {
      // / close both connections
      _db1.end()
      _db2.end()

      // / retrieve sql to execute
      var _delta = new Delta(_schema1.data(), _schema2.data(), _options)

      // / if there is output file in config, write there else write everything in console
      if (_config.output) {
        fs.writeFile(_config.output, _delta.sql(), function (err) {
          if (err) {
            console.error('error writing sql file:', err)
            process.exit(1)
          }
          console.log('Compare completed')
        })
      } else {
        console.log(_delta.sql())
        console.log('Compare completed')
      }
    })

    _job.todo('db1.schema')
    _job.todo('db2.schema')
    // / load data from both schema
    _schema1.load(function () {
      _job.done('db1.schema')
    })

    _schema2.load(function () {
      _job.done('db2.schema')
    })
  }

  // / try to connect to db 1
  _db1.connect(function (err) {
    if (err) {
      console.error('error connecting to database 1:', err)
      process.exit(1)
    }
    // / try to connect to db 1
    _db2.connect(function (err) {
      if (err) {
        console.error('error connecting to database 2:', err)
        process.exit(1)
      }
      // / compare both dbs
      _compare()
    })
  })
})
