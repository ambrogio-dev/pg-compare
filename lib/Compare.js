var fs = require('fs')
var pg = require('pg')
var Orchestrator = require('orchestrator')

var Delta = require('./Delta')
var Schema = require('./Schema')
var log = require('./log')

var Compare = function () {
  /**
   * keep a reference about delta differences between dbs
   * @type {Delta}
   * @private
   */
  var __delta

  /**
   * keep a reference about db connection
   * @type {pg.Client}
   * @private
   */
  var __db1, __db2

  /**
   * keep a reference about config parameters
   * @type {object}
   * @private
   */
  var __config

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
        log.verbose('error', 'error in config file, missing param', key)
        return false
      } else {
        if (!conn.host) {
          log.verbose('error', 'error in config file, missing host param for ' + key)
          return false
        }
        if (!conn.user) {
          log.verbose('error', 'error in config file, missing user param for ' + key)
          return false
        }
        if (!conn.password) {
          log.verbose('error', 'error in config file, missing password param for ' + key)
          return false
        }
        if (!conn.database) {
          log.verbose('error', 'error in config file, missing database param for ' + key)
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
      log.verbose('error', 'error in config file, missing schema param')
      return false
    }

    return true
  }

  /**
    * Compare database 1 and database 2
    * @private
    * @function
    * @param {function} callback Callback function
    */
  var __compare = function (callback) {
    var _schema = __config.compare.schema
    var _options = __config.compare.options

    // / init both schema
    var _schema1 = new Schema(__db1, _schema)
    var _schema2 = new Schema(__db2, _schema)

    var _orchestrator = new Orchestrator()

    // / load both schemas and then compare them
    _orchestrator.add('schema1', function (next) {
      // / load data from schema 1
      _schema1.load(function (err) {
        next(err)
      })
    })
    _orchestrator.add('schema2', function (next) {
      // / load data from schema 2
      _schema2.load(function (err) {
        next(err)
      })
    })

    _orchestrator.start('schema1', 'schema2', function (err) {
      // / close both connections
      __db1.end()
      __db2.end()

      // / if there was an error during schema loading, return
      if (err) {
        log.verbose('error', '/*** Comparing terminated with errors ***/')
        callback(true)
        return
      }

      // / retrieve sql to execute
      __delta = new Delta(_schema1.data(), _schema2.data(), _options)

      // / if there is output file in config, write there else write everything in console
      if (__config.output) {
        fs.writeFile(__config.output, __delta.sql(), function (err) {
          if (err) {
            log.verbose('error', 'error writing sql file:', err)
            callback(true)
            return
          }
          console.log('/*** Comparing terminated succesfully ***/')
          callback(false)
        })
      } else {
        console.log(__delta.sql())
        console.log('/*** Comparing terminated succesfully ***/')
        callback(false)
      }
    })
  }

  /**
  * Run the db compare
  * @public
  * @function
  * @param {object} config Configuration parameters
  * @param {function} callback Callback function
  */
  var run = function (config, callback) {
    // / try parsing the json in input
    try {
      __config = JSON.parse(config)
    } catch (ex) {
      console.error('error parsing config file:', ex)
      callback(true)
      return
    }

    // / set the verbose attribute to log copying the configuration value
    log.setVerbose(__config.verbose)

    // / check if config parameters are ok
    if (!__checkConfig(__config)) {
      callback(true)
      return
    }

    // / create 2 postgresql connections
    var _conn1 = __config.connection1
    var _conn2 = __config.connection2
    __db1 = new pg.Client(_conn1)
    __db2 = new pg.Client(_conn2)

    // / default owner is user of connection 2
    if (!__config.compare.options.owner) {
      __config.compare.options.owner = _conn2.user
    }

    var _orchestrator = new Orchestrator()

    // / try to connect to db 1
    _orchestrator.add('db1', function (next) {
      __db1.connect(function (err) {
        if (err) {
          log.verbose('error', 'error connecting to database 1:', err)
        }
        next(err)
      })
    })

    // / try to connect to db 2
    _orchestrator.add('db2', function (next) {
      __db2.connect(function (err) {
        if (err) {
          log.verbose('error', 'error connecting to database 2:', err)
        }
        next(err)
      })
    })

    _orchestrator.start('db1', 'db2', function (err) {
      if (err) {
        callback(true)
        return
      }
      // / compare both dbs
      __compare(callback)
    })
  }

  /**
  * Retrieve delta sql for the category in input
  * @public
  * @function
  * @param {string} key Category to retrieve delta sql
  */
  var getDeltaDifference = function (key) {
    if (!__delta || !key) {
      return null
    }
    return __delta.getDeltaDifference(key)
  }

  return {
    run: run,
    getDeltaDifference: getDeltaDifference
  }
}

module.exports = Compare
