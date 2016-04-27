var fs = require('fs')
var pg = require('pg')
var tools = require('a-toolbox')

var Delta = require('./Delta')
var Schema = require('./Schema')
var log = require('./log')

var DbCompare = function () {
  // / keep a reference about delta differences between dbs
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

  /**
  * Run the db compare
  * @public
  * @function
  * @param {object} config Configuration parameters
  * @param {function} callback Callback function
  */
  var run = function (config, callback) {
    // / keep a reference to "jsonparsed" configuration
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

    // / set the verbose attribute to log copying the configuration value
    log.setVerbose(_config.verbose)

    // / create 2 postgresql connections
    var _conn1 = _config.connection1
    var _conn2 = _config.connection2
    var _db1 = new pg.Client(_conn1)
    var _db2 = new pg.Client(_conn2)

    var _schema = _config.compare.schema
    var _options = _config.compare.options

    // / default owner is user of connection 2
    if (!_options.owner) {
      _options.owner = _conn2.user
    }

    /**
    * Compare database 1 and database 2
    * @private
    * @function
    */
    var _compare = function () {
      var _errorFlag = false
      // / init both schema
      var _schema1 = new Schema(_db1, _schema)
      var _schema2 = new Schema(_db2, _schema)

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
              log.verbose('error', 'error writing sql file:', err)
              callback(true)
              return
            }
            if (_errorFlag) {
              log.verbose('error', '/*** Comparing terminated with errors ***/')
            } else {
              log.verbose('log', '/*** Comparing terminated succesfully ***/')
            }
            callback(false)
          })
        } else {
          console.log(delta.sql())
          if (_errorFlag) {
            log.verbose('error', '/*** Comparing terminated with errors ***/')
          } else {
            log.verbose('log', '/*** Comparing terminated succesfully ***/')
          }
          callback(false)
        }
      })

      _job.todo('db1.schema')
      _job.todo('db2.schema')
      // / load data from both schema
      _schema1.load(function (err) {
        _errorFlag = _errorFlag || err
        _job.done('db1.schema')
      })

      _schema2.load(function (err) {
        _errorFlag = _errorFlag || err
        _job.done('db2.schema')
      })
    }

    // / try to connect to db 1
    _db1.connect(function (err) {
      if (err) {
        log.verbose('error', 'error connecting to database 1:', err)
        callback(true)
        return
      }
      // / try to connect to db 2
      _db2.connect(function (err) {
        if (err) {
          log.verbose('error', 'error connecting to database 2:', err)
          callback(true)
          return
        }
        // / compare both dbs
        _compare()
      })
    })
  }

  /**
  * Retrieve delta sql for the category in input
  * @public
  * @function
  * @param {string} key Category to retrieve delta sql
  */
  var getDeltaDifference = function (key) {
    if (!delta || !key) {
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
