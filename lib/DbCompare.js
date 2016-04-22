var fs = require('fs')
var pg = require('pg')
var tools = require('a-toolbox')

var Delta = require('./Delta')
var Schema = require('./Schema')
var log = require('./log')

var DbCompare = function () {
  var delta
  /**
  * Check if config json given in input has all mandatory fields
  * @public
  * @function
  * @param {object} config Configuration parameters
  */
  var __checkConfig = function (config) {
    /**
    * Check if connection parameters are ok
    * @private
    * @function
    * @param {object} Connection parameters
    * @param {string} Connection to check (connection1|connection2)
    */
    var _checkConnectionParams = function (conn, key) {
      if (!conn) {
        console.error('error in config file, missing param', key)
        return false
      } else {
        if (!conn.host) {
          console.error('error in config file, missing host param for ' + key)
          return false
        }
        if (!conn.user) {
          console.error('error in config file, missing user param for ' + key)
          return false
        }
        if (!conn.password) {
          console.error('error in config file, missing password param for ' + key)
          return false
        }
        if (!conn.database) {
          console.error('error in config file, missing database param for ' + key)
          return false
        }
      }
      return true
    }
    // / check both connection parameters
    if (!_checkConnectionParams(config.connection1, 'connection1')) {
      return false
    }
    if (!_checkConnectionParams(config.connection2, 'connection2')) {
      return false
    }
    // / check if schema parameters are present
    if (!config.compare || !config.compare.schema || Object.keys(config.compare.schema).length === 0) {
      console.error('error in config file, missing schema param')
      return false
    }
    return true
  }

  var run = function (config, callback) {
    var _config
    // / try parsing the json in input
    try {
      _config = JSON.parse(config)
    } catch (ex) {
      console.error('error parsing config file:', ex)
      callback(true)
      return
    }
    // / check if config parameters are ok
    if (!__checkConfig(_config)) {
      callback(true)
      return
    }

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
      var _schemaClone = tools.object.clone(_schema)
      _schemaClone.rows = null
      var _schema2 = new Schema(_db2, _schemaClone)

      var _job = new tools.tasks(function () {
        // / close both connections
        _db1.end()
        _db2.end()

        // / retrieve sql to execute
        delta = new Delta(_schema1.data(), _schema2.data(), _options)

        // / if there is output file in config, write there else write everything in console
        if (_config.output) {
          fs.writeFile(_config.output, delta.sql(), function (err) {
            if (err) {
              console.error('error writing sql file:', err)
              callback(true)
              return
            }
            callback(false)
          })
        } else {
          console.log(_delta.sql())
          callback(false)
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
        callback(true)
        return
      }
      // / try to connect to db 1
      _db2.connect(function (err) {
        if (err) {
          console.error('error connecting to database 2:', err)
          callback(true)
          return
        }
        // / compare both dbs
        _compare()
      })
    })
  }

  var getDeltaDifference = function (key) {
    if (!delta) {
      return null
    }
    return delta.getDeltaDifference(key)
  }
  return {
    run: run,
    getDeltaDifference: getDeltaDifference
  }
}

module.exports = DbCompare
