var tools = require('a-toolbox')
var pg = require('pg')

var DbCompare = require('./lib/DbCompare')

// / template of configuration parameters
var configTemplate = {
  'connection1': {
    'host': '10.20.6.22',
    'user': 'alatini',
    'password': 'devdevdev',
    'database': 'test-compare1'
  },
  'connection2': {
    'host': '10.20.6.22',
    'user': 'alatini',
    'password': 'devdevdev',
    'database': 'test-compare2'
  },
  'compare': {
    'schema': {
      'tables': true,
      'fkeys': true,
      'functions': true,
      'indexes': true,
      'types': true,
      'views': true,
      'sequences': true
    },
    'options': {
      'mode': 'full',
      'owner': 'alatini'
    }
  },
  'output': '/tmp/delta.sql',
  'verbose': false
}

// / keep a reference to count success or error tests
var _successCount = 0
var _errorCount = 0

var dbCompare = new DbCompare()

/**
 * Try to pass a config file with a wrong json format
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testWrongJson = function (next) {
  console.log('testWrongJson executing')
  dbCompare.run({a: 1}, function (err) {
    if (err) {
      console.log('testWrongJson success')
      _successCount++
    } else {
      console.log('testWrongJson error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without connection to db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingConnection1 = function (next) {
  console.log('testMissingConnection1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1 = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingConnection1 success')
      _successCount++
    } else {
      console.log('testMissingConnection1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without connection to db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingConnection2 = function (next) {
  console.log('testMissingConnection2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2 = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingConnection2 success')
      _successCount++
    } else {
      console.log('testMissingConnection2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without host of db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingHost1 = function (next) {
  console.log('testMissingHost1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1.host = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingHost1 success')
      _successCount++
    } else {
      console.log('testMissingHost1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without host of db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingHost2 = function (next) {
  console.log('testMissingHost2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2.host = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingHost2 success')
      _successCount++
    } else {
      console.log('testMissingHost2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without username to connect to db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingUser1 = function (next) {
  console.log('testMissingUser1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1.user = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingUser1 success')
      _successCount++
    } else {
      console.log('testMissingUser1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without username to connect to db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingUser2 = function (next) {
  console.log('testMissingUser2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2.user = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingUser2 success')
      _successCount++
    } else {
      console.log('testMissingUser2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without password to connect to db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingPassword1 = function (next) {
  console.log('testMissingPassword1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1.password = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingPassword1 success')
      _successCount++
    } else {
      console.log('testMissingPassword1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without password to connect to db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingPassword2 = function (next) {
  console.log('testMissingPassword2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2.password = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingPassword2 success')
      _successCount++
    } else {
      console.log('testMissingPassword2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without database name of db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingDatabase1 = function (next) {
  console.log('testMissingDatabase1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1.database = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingDatabase1 success')
      _successCount++
    } else {
      console.log('testMissingDatabase1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without database name of db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingDatabase2 = function (next) {
  console.log('testMissingDatabase2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2.database = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingDatabase2 success')
      _successCount++
    } else {
      console.log('testMissingDatabase2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file without schema parameters
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testMissingSchema = function (next) {
  console.log('testMissingSchema executing')
  var _config = tools.object.clone(configTemplate)
  _config.compare.schema = null
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testMissingSchema success')
      _successCount++
    } else {
      console.log('testMissingSchema error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file with wrong connection parameters for db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testWrongConnection1 = function (next) {
  console.log('testWrongConnection1 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection1.password = 'xxx'
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testWrongConnection1 success')
      _successCount++
    } else {
      console.log('testWrongConnection1 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Try to pass a config file with wrong connection parameters for db2
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testWrongConnection2 = function (next) {
  console.log('testWrongConnection2 executing')
  var _config = tools.object.clone(configTemplate)
  _config.connection2.password = 'xxx'
  dbCompare.run(JSON.stringify(_config), function (err) {
    if (err) {
      console.log('testWrongConnection2 success')
      _successCount++
    } else {
      console.log('testWrongConnection2 error')
      _errorCount++
    }
    next && next()
  })
}

/**
 * Connect to 2 dbs, execute scripts in input and then compare them
 * @function
 * @param {object} prm
 * @param {string} prm.sql1 Sql script to execute to db1
 * @param {string} prm.sql2 Sql script to execute to db2
 * @param {string} prm.sqlDifference Sql delta that is expected to be generated
 * @param {object} prm.schema Schema object with items to compare
 * @param {string} prm.deltaKey Db item that has to be compared
 * @param {function} callback Callback function
 */
var _deltaCompare = function (prm, callback) {
  // / use configuration template and adjust it with input parameters
  var _config = tools.object.clone(configTemplate)
  _config.compare.schema = prm.schema
  if (!prm.full) {
    _config.compare.options.mode = null
  }

  // / create two connections
  var _conn1 = _config.connection1
  var _conn2 = _config.connection2
  var _db1 = new pg.Client(_conn1)
  var _db2 = new pg.Client(_conn2)

  // / sql to clear all items created by test functions
  var _clearSql = 'DROP TABLE IF EXISTS factories CASCADE; \n' +
    'DROP TABLE IF EXISTS users CASCADE; \n' +
    'DROP TABLE IF EXISTS cities CASCADE; \n' +
    'DROP FUNCTION IF EXISTS func(text); \n' +
    'DROP FUNCTION IF EXISTS func(text, text); \n' +
    'DROP FUNCTION IF EXISTS func2(text, text); \n' +
    'DROP INDEX IF EXISTS in_users; \n' +
    'DROP INDEX IF EXISTS in_users2; \n' +
    'DROP TYPE IF EXISTS fruit; \n' +
    'DROP TYPE IF EXISTS sport; \n' +
    'DROP VIEW IF EXISTS vw; \n' +
    'DROP VIEW IF EXISTS vw2; \n' +
    'DROP SEQUENCE IF EXISTS seq_users; \n'
  prm.sql1 = _clearSql + prm.sql1
  prm.sql2 = _clearSql + prm.sql2

  // / try to connect to db1
  _db1.connect(function (err) {
    if (err) {
      console.error('error connecting to database 1:', err)
      callback && callback(true)
      return
    }
    // / execute query for db1
    _db1.query(
      prm.sql1,
      function (err, result) {
        if (err) {
          console.error('error inserting in database 1:', err)
          callback && callback(true)
          return
        }
        // / close db1 connection
        _db1.end()
        // / try to connect to db 2
        _db2.connect(function (err) {
          if (err) {
            console.error('error connecting to database 2:', err)
            callback && callback(true)
            return
          }
          // / execute query for db2
          _db2.query(
            prm.sql2,
            function (err, result) {
              if (err) {
                console.error('error inserting database 2:', err)
                callback && callback(true)
                return
              }
              // / close db2 connection
              _db2.end()
              // / compare 2 databases
              dbCompare.run(JSON.stringify(_config), function (err) {
                if (!err) {
                  // / retrieve delta sql
                  var _differences = dbCompare.getDeltaDifference(prm.deltaKey)
                  // / compare delta sql with the expected one
                  if (_differences.join('') === prm.sqlDifference) {
                    callback && callback(false)
                  } else {
                    callback && callback(true)
                  }
                } else {
                  callback && callback(true)
                }
              })
            })
        })
      })
  })
}

/**
 * Compare two identical tables
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareSuccess = function (next) {
  console.log('testTableCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL, ' +
      "surname VARCHAR NOT NULL, age INTEGER DEFAULT 10, phone VARCHAR, gender CHAR NOT NULL DEFAULT 'M');\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL, ' +
      "surname VARCHAR NOT NULL, age INTEGER DEFAULT 10, phone VARCHAR, gender CHAR NOT NULL DEFAULT 'M');\n",
    sqlDifference: '',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareSuccess error')
      _errorCount++
    } else {
      console.log('testTableCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a table that db2 hasn't
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareMissingTable = function (next) {
  console.log('testTableCompareMissingTable executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON table users IS 'users';\n" +
      "COMMENT ON column users.name IS 'name'",
    sql2: 'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sqlDifference: 'CREATE TABLE users (\nid serial NOT NULL,name varchar NOT NULL\n);\n' +
      'ALTER TABLE users OWNER TO ' + configTemplate.compare.options.owner + ';\n' +
      "COMMENT ON TABLE users IS 'users';\n" +
      "COMMENT ON COLUMN users.name IS 'name';\n",
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareMissingTable error')
      _errorCount++
    } else {
      console.log('testTableCompareMissingTable success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a table comment that db2 hasn't
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareMissingTableComment = function (next) {
  console.log('testTableCompareMissingTableComment executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON TABLE users IS 'users';\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sqlDifference: "COMMENT ON TABLE users IS 'users';\n",
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareMissingTableComment error')
      _errorCount++
    } else {
      console.log('testTableCompareMissingTableComment success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db1 have different table comment
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareWrongTableComment = function (next) {
  console.log('testTableCompareWrongTableComment executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON TABLE users is 'users';\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON TABLE users is 'cities';\n",
    sqlDifference: "COMMENT ON TABLE users IS 'users';\n",
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareWrongTableComment error')
      _errorCount++
    } else {
      console.log('testTableCompareWrongTableComment success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a column not nullable but in db2 is nullable
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareSetNotNull = function (next) {
  console.log('testTableCompareSetNotNull executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n',
    sqlDifference: '/* WARNING: column name of table users is not null but without default value*/\n' +
      'ALTER TABLE users ALTER COLUMN name SET NOT NULL;\n',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareSetNotNull error')
      _errorCount++
    } else {
      console.log('testTableCompareSetNotNull success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a column that db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareMissingColumn = function (next) {
  console.log('testTableCompareMissingColumn executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL);\n',
    sqlDifference: 'ALTER TABLE users ADD COLUMN name varchar NOT NULL;\n',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareMissingColumn error')
      _errorCount++
    } else {
      console.log('testTableCompareMissingColumn success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a comment on a column that db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareMissingColumnComment = function (next) {
  console.log('testTableCompareMissingColumnComment executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON COLUMN users.name IS 'nome';\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sqlDifference: "COMMENT ON COLUMN users.name IS 'nome';\n",
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareMissingColumnComment error')
      _errorCount++
    } else {
      console.log('testTableCompareMissingColumnComment success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different comment on same column
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareWrongColumnComment = function (next) {
  console.log('testTableCompareMissingColumnComment executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON COLUMN users.name IS 'nome';\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      "COMMENT ON COLUMN users.name IS 'cognome';\n",
    sqlDifference: "COMMENT ON COLUMN users.name IS 'nome';\n",
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareWrongColumnComment error')
      _errorCount++
    } else {
      console.log('testTableCompareWrongColumnComment success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a column with default value but in db2 there isn't default value for the same column
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareMissingDefaultValue = function (next) {
  console.log('testTableCompareMissingDefaultValue executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, age INTEGER NOT NULL DEFAULT 10);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, age INTEGER NOT NULL);\n',
    sqlDifference: 'ALTER TABLE users ALTER COLUMN age SET DEFAULT 10;\n',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareMissingDefaultValue error')
      _errorCount++
    } else {
      console.log('testTableCompareMissingDefaultValue success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different default values for the same column
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareWrongDefaultValue = function (next) {
  console.log('testTableCompareWrongDefaultValue executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, age INTEGER NOT NULL DEFAULT 10);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, age INTEGER NOT NULL DEFAULT 20);\n',
    sqlDifference: 'ALTER TABLE users ALTER COLUMN age SET DEFAULT 10;\n',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareWrongDefaultValue error')
      _errorCount++
    } else {
      console.log('testTableCompareWrongDefaultValue success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different data type for the same column
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareWrongDataType = function (next) {
  console.log('testTableCompareWrongDataType executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name integer);\n',
    sqlDifference: 'ALTER TABLE users ALTER COLUMN name SET DATA TYPE varchar;\n',
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareWrongDataType error')
      _errorCount++
    } else {
      console.log('testTableCompareWrongDataType success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a column on a table but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareDropColumn = function (next) {
  console.log('testTableCompareDropColumn executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n',
    sqlDifference: 'ALTER TABLE users DROP COLUMN name;\n',
    full: true,
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareDropColumn error')
      _errorCount++
    } else {
      console.log('testTableCompareDropColumn success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a table that db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTableCompareDropTable = function (next) {
  console.log('testTableCompareDropTable executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n',
    sqlDifference: 'DROP TABLE factories;',
    full: true,
    schema: {
      tables: true
    },
    deltaKey: 'tables'
  }, function (err) {
    if (err) {
      console.log('testTableCompareDropTable error')
      _errorCount++
    } else {
      console.log('testTableCompareDropTable success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: they have same primary keys
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testPkeysCompareSuccess = function (next) {
  console.log('testPkeysCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT pk_factories PRIMARY KEY (id);\n',
    sql2: 'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT pk_factories PRIMARY KEY (id);\n',
    sqlDifference: '--primary keys match',
    schema: {
      tables: true
    },
    deltaKey: 'pkeys'
  }, function (err) {
    if (err) {
      console.log('testPkeysCompareSuccess error')
      _errorCount++
    } else {
      console.log('testPkeysCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a primary key but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testPkeysCompareMissing = function (next) {
  console.log('testPkeysCompareMissing executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (name, surname);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n',
    sqlDifference: 'ALTER TABLE users\n  ADD CONSTRAINT pk_users PRIMARY KEY (name,surname);',
    schema: {
      tables: true
    },
    deltaKey: 'pkeys'
  }, function (err) {
    if (err) {
      console.log('testPkeysCompareMissing error')
      _errorCount++
    } else {
      console.log('testPkeysCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different definitions for the same primary key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testPkeysCompareWrongFields = function (next) {
  console.log('testPkeysCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (name, surname);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (name);\n',
    sqlDifference: 'ALTER TABLE users\n  DROP CONSTRAINT pk_users;\n' +
      'ALTER TABLE users\n  ADD CONSTRAINT pk_users PRIMARY KEY (name,surname);',
    schema: {
      tables: true
    },
    deltaKey: 'pkeys'
  }, function (err) {
    if (err) {
      console.log('testPkeysCompareSuccess error')
      _errorCount++
    } else {
      console.log('testPkeysCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a primary key but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testPkeysCompareDrop = function (next) {
  console.log('testPkeysCompareDrop executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT pk_factories PRIMARY KEY (id);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (name, surname);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT pk_factories PRIMARY KEY (id);\n',
    sqlDifference: 'ALTER TABLE users\n  DROP CONSTRAINT pk_users;',
    full: true,
    schema: {
      tables: true
    },
    deltaKey: 'pkeys'
  }, function (err) {
    if (err) {
      console.log('testPkeysCompareDrop error')
      _errorCount++
    } else {
      console.log('testPkeysCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have the same unique constraints
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testUniqueCompareSuccess = function (next) {
  console.log('testUniqueCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT un_factories UNIQUE (id);\n',
    sql2: 'CREATE TABLE factories (id SERIAL NOT NULL, name VARCHAR NOT NULL);\n' +
      'ALTER TABLE factories ADD CONSTRAINT un_factories UNIQUE (id);\n',
    sqlDifference: '--unique constraints match',
    schema: {
      tables: true
    },
    deltaKey: 'unique'
  }, function (err) {
    if (err) {
      console.log('testUniqueCompareSuccess error')
      _errorCount++
    } else {
      console.log('testUniqueCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has an unique constraint but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testUniqueCompareMissing = function (next) {
  console.log('testUniqueCompareMissing executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n',
    sqlDifference: 'ALTER TABLE users\n  ADD CONSTRAINT un_users UNIQUE (name,surname);',
    schema: {
      tables: true
    },
    deltaKey: 'unique'
  }, function (err) {
    if (err) {
      console.log('testUniqueCompareMissing error')
      _errorCount++
    } else {
      console.log('testUniqueCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different definitions for the same unique constraint
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testUniqueCompareWrongFields = function (next) {
  console.log('testUniqueCompareWrongFields executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name);\n',
    sqlDifference: 'ALTER TABLE users\n  DROP CONSTRAINT un_users;\n' +
      'ALTER TABLE users\n  ADD CONSTRAINT un_users UNIQUE (name,surname);',
    schema: {
      tables: true
    },
    deltaKey: 'unique'
  }, function (err) {
    if (err) {
      console.log('testUniqueCompareWrongFields error')
      _errorCount++
    } else {
      console.log('testUniqueCompareWrongFields success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has an unique constraint but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testUniqueCompareDrop = function (next) {
  console.log('testUniqueCompareDrop executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users2 UNIQUE (age);\n',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users2 UNIQUE (age);\n',
    sqlDifference: 'ALTER TABLE users\n  DROP CONSTRAINT un_users;',
    full: true,
    schema: {
      tables: true
    },
    deltaKey: 'unique'
  }, function (err) {
    if (err) {
      console.log('testUniqueCompareDrop error')
      _errorCount++
    } else {
      console.log('testUniqueCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have the same foreign keys
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareSuccess = function (next) {
  console.log('testFkCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: '--foreign keys match',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareSuccess error')
      _errorCount++
    } else {
      console.log('testFkCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a foreign key but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareMissing = function (next) {
  console.log('testFkCompareMissing executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n',
    sqlDifference: 'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareMissing error')
      _errorCount++
    } else {
      console.log('testFkCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different target table for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareTargetWrongTable = function (next) {
  console.log('testFkCompareTargetWrongTable executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE cities (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES cities(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareTargetWrongTable error')
      _errorCount++
    } else {
      console.log('testFkCompareTargetWrongTable success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different target columns for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareTargetWrongColumns = function (next) {
  console.log('testFkCompareTargetWrongColumns executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR, surname VARCHAR);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_name VARCHAR, employee_surname VARCHAR);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_name, employee_surname) REFERENCES users(name, surname) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (name VARCHAR, surname VARCHAR);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_name VARCHAR, employee_surname VARCHAR);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_name, employee_surname) REFERENCES users(surname, name) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_name,employee_surname)\n  REFERENCES users (name,surname) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareTargetWrongColumns error')
      _errorCount++
    } else {
      console.log('testFkCompareTargetWrongColumns success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different source table for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareSourceWrongTable = function (next) {
  console.log('testFkCompareSourceWrongTable executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE cities (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE cities ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE cities\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareSourceWrongTable error')
      _errorCount++
    } else {
      console.log('testFkCompareSourceWrongTable success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different source columns for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareSourceWrongColumns = function (next) {
  console.log('testFkCompareSourceWrongColumns executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR, surname VARCHAR);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_name VARCHAR, employee_surname VARCHAR);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_name, employee_surname) REFERENCES users(name, surname) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (name VARCHAR, surname VARCHAR);\n' +
      'ALTER TABLE users ADD CONSTRAINT un_users UNIQUE (name, surname);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_name VARCHAR, employee_surname VARCHAR);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_surname, employee_name) REFERENCES users(name, surname) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_name,employee_surname)\n  REFERENCES users (name,surname) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareSourceWrongColumns error')
      _errorCount++
    } else {
      console.log('testFkCompareSourceWrongColumns success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different match type for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareWrongMatch = function (next) {
  console.log('testFkCompareWrongMatch executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareWrongMatch error')
      _errorCount++
    } else {
      console.log('testFkCompareWrongMatch success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different update behaviour for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareWrongUpdate = function (next) {
  console.log('testFkCompareWrongUpdate executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE SET NULL ON DELETE CASCADE;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareWrongUpdate error')
      _errorCount++
    } else {
      console.log('testFkCompareWrongUpdate success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different delete behaviour for the same foreign key
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareWrongDelete = function (next) {
  console.log('testFkCompareWrongDelete executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users(id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;\n' +
      'ALTER TABLE factories\n  ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id)\n  REFERENCES users (id) MATCH SIMPLE\n  ON UPDATE CASCADE ON DELETE CASCADE;',
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareWrongDelete error')
      _errorCount++
    } else {
      console.log('testFkCompareWrongDelete success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a foreign key but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFkCompareDrop = function (next) {
  console.log('testFkCompareDrop executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE cities (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT, city_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES cities (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE cities (id SERIAL NOT NULL PRIMARY KEY);\n' +
      'CREATE TABLE factories (id SERIAL NOT NULL, employee_id BIGINT, city_id BIGINT);\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES cities (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL;\n' +
      'ALTER TABLE factories ADD CONSTRAINT fk_employee FOREIGN KEY (employee_id) REFERENCES users (id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE SET NULL;\n',
    sqlDifference: 'ALTER TABLE factories\n  DROP CONSTRAINT fk_employee;',
    full: true,
    schema: {
      fkeys: true
    },
    deltaKey: 'fkeys'
  }, function (err) {
    if (err) {
      console.log('testFkCompareDrop error')
      _errorCount++
    } else {
      console.log('testFkCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have same functions
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFunctionCompareSuccess = function (next) {
  console.log('testFunctionCompareSuccess executing')
  _deltaCompare({
    sql1: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sql2: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sqlDifference: '--functions match',
    schema: {
      functions: true
    },
    deltaKey: 'functions'
  }, function (err) {
    if (err) {
      console.log('testFunctionCompareSuccess error')
      _errorCount++
    } else {
      console.log('testFunctionCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a function but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFunctionCompareMissing = function (next) {
  console.log('testFunctionCompareSuccess executing')
  _deltaCompare({
    sql1: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sql2: '',
    sqlDifference: "CREATE OR REPLACE FUNCTION public.func(message text)\n RETURNS character varying\n LANGUAGE plv8\nAS $function$ return 'Hello world'; $function$\n;\nALTER FUNCTION func(message text) OWNER TO " + configTemplate.compare.options.owner + ';',
    schema: {
      functions: true
    },
    deltaKey: 'functions'
  }, function (err) {
    if (err) {
      console.log('testFunctionCompareMissing error')
      _errorCount++
    } else {
      console.log('testFunctionCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different sources for the same function
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFunctionCompareWrongSource = function (next) {
  console.log('testFunctionCompareWrongSource executing')
  _deltaCompare({
    sql1: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sql2: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ var a =1;return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sqlDifference: "CREATE OR REPLACE FUNCTION public.func(message text)\n RETURNS character varying\n LANGUAGE plv8\nAS $function$ return 'Hello world'; $function$\n;",
    schema: {
      functions: true
    },
    deltaKey: 'functions'
  }, function (err) {
    if (err) {
      console.log('testFunctionCompareWrongSource error')
      _errorCount++
    } else {
      console.log('testFunctionCompareWrongSource success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different arguments for the same function
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFunctionCompareWrongArgs = function (next) {
  console.log('testFunctionCompareWrongArgs executing')
  _deltaCompare({
    sql1: "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sql2: "CREATE FUNCTION func(message text, sms text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sqlDifference: "CREATE OR REPLACE FUNCTION public.func(message text)\n RETURNS character varying\n LANGUAGE plv8\nAS $function$ return 'Hello world'; $function$\n;",
    schema: {
      functions: true
    },
    deltaKey: 'functions'
  }, function (err) {
    if (err) {
      console.log('testFunctionCompareWrongArgs error')
      _errorCount++
    } else {
      console.log('testFunctionCompareWrongArgs success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a function but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testFunctionCompareDrop = function (next) {
  console.log('testFunctionCompareDrop executing')
  _deltaCompare({
    sql1: "CREATE FUNCTION func2(message text, sms text) RETURNS varchar AS $BODY$ return 'Hello world2'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sql2: "CREATE FUNCTION func2(message text, sms text) RETURNS varchar AS $BODY$ return 'Hello world2'; $BODY$ LANGUAGE plv8 VOLATILE;" +
      "CREATE FUNCTION func(message text) RETURNS varchar AS $BODY$ return 'Hello world'; $BODY$ LANGUAGE plv8 VOLATILE;",
    sqlDifference: 'DROP FUNCTION func(message text);',
    full: true,
    schema: {
      functions: true
    },
    deltaKey: 'functions'
  }, function (err) {
    if (err) {
      console.log('testFunctionCompareDrop error')
      _errorCount++
    } else {
      console.log('testFunctionCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have same indexes
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testIndexesCompareSuccess = function (next) {
  console.log('testIndexesCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users ON users USING hash (name);',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users ON users USING hash (name);',
    sqlDifference: '--indexes match',
    schema: {
      indexes: true
    },
    deltaKey: 'indexes'
  }, function (err) {
    if (err) {
      console.log('testIndexesCompareSuccess error')
      _errorCount++
    } else {
      console.log('testIndexesCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has an index but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testIndexesCompareMissing = function (next) {
  console.log('testIndexesCompareMissing executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users ON users USING hash (name);',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n',
    sqlDifference: 'CREATE INDEX in_users ON users USING hash (name);',
    schema: {
      indexes: true
    },
    deltaKey: 'indexes'
  }, function (err) {
    if (err) {
      console.log('testIndexesCompareMissing error')
      _errorCount++
    } else {
      console.log('testIndexesCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different sources for the same index
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testIndexesCompareWrongSource = function (next) {
  console.log('testIndexesCompareWrongSource executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users ON users USING hash (name);',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users ON users USING btree (age);',
    sqlDifference: 'DROP INDEX in_users;\nCREATE INDEX in_users ON users USING hash (name);',
    schema: {
      indexes: true
    },
    deltaKey: 'indexes'
  }, function (err) {
    if (err) {
      console.log('testIndexesCompareWrongSource error')
      _errorCount++
    } else {
      console.log('testIndexesCompareWrongSource success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has an index but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testIndexesCompareDrop = function (next) {
  console.log('testIndexesCompareDrop executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users2 ON users USING hash (surname);',
    sql2: 'CREATE TABLE users (name VARCHAR NOT NULL, surname VARCHAR NOT NULL, age INTEGER);\n' +
      'CREATE INDEX in_users2 ON users USING hash (surname);' +
      'CREATE INDEX in_users ON users USING hash (name);',
    sqlDifference: 'DROP INDEX in_users;',
    full: true,
    schema: {
      indexes: true
    },
    deltaKey: 'indexes'
  }, function (err) {
    if (err) {
      console.log('testIndexesCompareDrop error')
      _errorCount++
    } else {
      console.log('testIndexesCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have same enum types
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTypeCompareSuccess = function (next) {
  console.log('testTypeCompareSuccess executing')
  _deltaCompare({
    sql1: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n",
    sql2: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n",
    sqlDifference: '--types match',
    schema: {
      types: true
    },
    deltaKey: 'types'
  }, function (err) {
    if (err) {
      console.log('testTypeCompareSuccess error')
      _errorCount++
    } else {
      console.log('testTypeCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has an enum type but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTypeCompareMissing = function (next) {
  console.log('testTypeCompareMissing executing')
  _deltaCompare({
    sql1: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n",
    sql2: '',
    sqlDifference: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE');\nALTER TYPE fruit OWNER TO " + configTemplate.compare.options.owner + ';',
    schema: {
      types: true
    },
    deltaKey: 'types'
  }, function (err) {
    if (err) {
      console.log('testTypeCompareMissing error')
      _errorCount++
    } else {
      console.log('testTypeCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 hasn't an enum value comparing to db1
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTypeCompareMissingValueAfter = function (next) {
  console.log('testTypeCompareMissingValueAfter executing')
  _deltaCompare({
    sql1: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n",
    sql2: "CREATE TYPE fruit AS ENUM ('BANANA','PEACH','PINEAPPLE');",
    sqlDifference: "ALTER TYPE fruit ADD VALUE 'APPLE' AFTER 'BANANA';",
    schema: {
      types: true
    },
    deltaKey: 'types'
  }, function (err) {
    if (err) {
      console.log('testTypeCompareMissingValueAfter error')
      _errorCount++
    } else {
      console.log('testTypeCompareMissingValueAfter success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 hasn't an enum value comparing to db1 (as frist enum value)
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTypeCompareMissingValueBefore = function (next) {
  console.log('testTypeCompareMissingValueBefore executing')
  _deltaCompare({
    sql1: "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n",
    sql2: "CREATE TYPE fruit AS ENUM ('APPLE','PEACH','PINEAPPLE');",
    sqlDifference: "ALTER TYPE fruit ADD VALUE 'BANANA' BEFORE 'APPLE';",
    schema: {
      types: true
    },
    deltaKey: 'types'
  }, function (err) {
    if (err) {
      console.log('testTypeCompareMissingValueBefore error')
      _errorCount++
    } else {
      console.log('testTypeCompareMissingValueBefore success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has an enum type but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testTypeCompareDrop = function (next) {
  console.log('testTypeCompareDrop executing')
  _deltaCompare({
    sql1: "CREATE TYPE sport AS ENUM ('FOOTBALL','VOLLEYBALL'); \n",
    sql2: "CREATE TYPE sport AS ENUM ('FOOTBALL','VOLLEYBALL'); \n" +
      "CREATE TYPE fruit AS ENUM ('BANANA','PEACH','PINEAPPLE');",
    sqlDifference: 'DROP TYPE fruit;',
    full: true,
    schema: {
      types: true
    },
    deltaKey: 'types'
  }, function (err) {
    if (err) {
      console.log('testTypeCompareDrop error')
      _errorCount++
    } else {
      console.log('testTypeCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have same views
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testViewsCompareSuccess = function (next) {
  console.log('testViewsCompareSuccess executing')
  _deltaCompare({
    sql1: "CREATE VIEW vw AS SELECT 'pippo' AS name, 1 AS id \n",
    sql2: "CREATE VIEW vw AS SELECT 'pippo' AS name, 1 AS id \n",
    sqlDifference: '--views match',
    schema: {
      views: true
    },
    deltaKey: 'views'
  }, function (err) {
    if (err) {
      console.log('testViewsCompareSuccess error')
      _errorCount++
    } else {
      console.log('testViewsCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 has a view but db2 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testViewsCompareMissing = function (next) {
  console.log('testViewsCompareMissing executing')
  _deltaCompare({
    sql1: "CREATE VIEW vw AS SELECT 'pippo' AS name, 1 AS id \n",
    sql2: '',
    sqlDifference: "CREATE VIEW vw AS\n SELECT 'pippo' AS name,\n    1 AS id;;\nALTER TABLE vw OWNER TO " + configTemplate.compare.options.owner + ';',
    schema: {
      views: true
    },
    deltaKey: 'views'
  }, function (err) {
    if (err) {
      console.log('testViewsCompareMissing error')
      _errorCount++
    } else {
      console.log('testViewsCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different sources for the same view
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testViewsCompareWrongSource = function (next) {
  console.log('testViewsCompareWrongSource executing')
  _deltaCompare({
    sql1: "CREATE VIEW vw AS SELECT 'pippo' AS name, 1 AS id \n",
    sql2: "CREATE VIEW vw AS SELECT 'pippo' AS name, 2 AS id \n",
    sqlDifference: "DROP VIEW vw;\nCREATE VIEW vw AS\n SELECT 'pippo' AS name,\n    1 AS id;;",
    schema: {
      views: true
    },
    deltaKey: 'views'
  }, function (err) {
    if (err) {
      console.log('testViewsCompareMissing error')
      _errorCount++
    } else {
      console.log('testViewsCompareMissing success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db2 has a view but db1 hasn't it
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testViewsCompareDrop = function (next) {
  console.log('testViewsCompareDrop executing')
  _deltaCompare({
    sql1: "CREATE VIEW vw2 AS SELECT 'hello world' AS name, 1 AS id \n",
    sql2: "CREATE VIEW vw2 AS SELECT 'hello world' AS name, 1 AS id; \n" +
      "CREATE VIEW vw AS SELECT 'pippo' AS name, 1 AS id \n",
    sqlDifference: 'DROP VIEW vw;',
    full: true,
    schema: {
      views: true
    },
    deltaKey: 'views'
  }, function (err) {
    if (err) {
      console.log('testViewsCompareDrop error')
      _errorCount++
    } else {
      console.log('testViewsCompareDrop success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have same current value for the same sequence
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testSequencesCompareSuccess = function (next) {
  console.log('testSequencesCompareSuccess executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n' +
      'CREATE SEQUENCE seq_users INCREMENT 1 MINVALUE 1 MAXVALUE 1000 START 1 NO CYCLE;\n',
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL, name VARCHAR);\n' +
      'CREATE SEQUENCE seq_users INCREMENT 1 MINVALUE 1 MAXVALUE 1000 START 1 NO CYCLE;\n',
    sqlDifference: '--sequences match',
    schema: {
      sequences: true
    },
    deltaKey: 'sequences'
  }, function (err) {
    if (err) {
      console.log('testSequencesCompareSuccess error')
      _errorCount++
    } else {
      console.log('testSequencesCompareSuccess success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Compare dbs: db1 and db2 have different current value for the same sequence
 * @function
 * @param {function} next Function to execute after finishing this test function
 */
var testSequencesCompareWrongValue = function (next) {
  console.log('testSequencesCompareWrongValue executing')
  _deltaCompare({
    sql1: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR);\n' +
      "INSERT INTO users(name) VALUES ('pippo');\n" +
      "INSERT INTO users(name) VALUES ('pluto');\n",
    sql2: 'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR);\n' +
      "INSERT INTO users(name) VALUES ('pippo');\n",
    sqlDifference: "SELECT setval('users_id_seq', 2);",
    schema: {
      sequences: true
    },
    deltaKey: 'sequences'
  }, function (err) {
    if (err) {
      console.log('testSequencesCompareWrongValue error')
      _errorCount++
    } else {
      console.log('testSequencesCompareWrongValue success')
      _successCount++
    }
    next && next()
  })
}

/**
 * Cycle test functions array and execute each function
 * @function
 * @param {Array.<function>} testFunctions Array of test functions to execute
 * @param {function} callback Callback function
 */
var executeTest = function (testFunctions, callback) {
  var _exe = function (i) {
    testFunctions[i](function () {
      if (testFunctions[i + 1]) {
        _exe(i + 1)
      } else {
        callback && callback()
      }
    })
  }
  _exe(0)
}

var runTests = (function () {
  // / array with all test functions to execute
  var _tests = [
    testWrongJson,
    testMissingConnection1,
    testMissingConnection2,
    testMissingHost1,
    testMissingHost2,
    testMissingUser1,
    testMissingUser2,
    testMissingPassword1,
    testMissingPassword2,
    testMissingDatabase1,
    testMissingDatabase2,
    testMissingSchema,
    testWrongConnection1,
    testWrongConnection2,
    testTableCompareSuccess,
    testTableCompareMissingTable,
    testTableCompareMissingTableComment,
    testTableCompareWrongTableComment,
    testTableCompareSetNotNull,
    testTableCompareMissingColumn,
    testTableCompareMissingColumnComment,
    testTableCompareWrongColumnComment,
    testTableCompareMissingDefaultValue,
    testTableCompareWrongDefaultValue,
    testTableCompareWrongDataType,
    testTableCompareDropColumn,
    testTableCompareDropTable,
    testPkeysCompareSuccess,
    testPkeysCompareMissing,
    testPkeysCompareWrongFields,
    testPkeysCompareDrop,
    testUniqueCompareSuccess,
    testUniqueCompareMissing,
    testUniqueCompareWrongFields,
    testUniqueCompareDrop,
    testFkCompareSuccess,
    testFkCompareMissing,
    testFkCompareTargetWrongTable,
    testFkCompareTargetWrongColumns,
    testFkCompareSourceWrongTable,
    testFkCompareSourceWrongColumns,
    testFkCompareWrongMatch,
    testFkCompareWrongUpdate,
    testFkCompareWrongDelete,
    testFkCompareDrop,
    testFunctionCompareSuccess,
    testFunctionCompareMissing,
    testFunctionCompareWrongSource,
    testFunctionCompareWrongArgs,
    testFunctionCompareDrop,
    testIndexesCompareSuccess,
    testIndexesCompareMissing,
    testIndexesCompareWrongSource,
    testIndexesCompareDrop,
    testTypeCompareSuccess,
    testTypeCompareMissing,
    testTypeCompareMissingValueAfter,
    testTypeCompareMissingValueBefore,
    testTypeCompareDrop,
    testViewsCompareSuccess,
    testViewsCompareMissing,
    testViewsCompareWrongSource,
    testViewsCompareDrop,
    testSequencesCompareSuccess,
    testSequencesCompareWrongValue
  ]
  // / execute all tests
  executeTest(_tests, function () {
    console.log('Test eseguiti: ', _tests.length, '\n')
    console.log('Test con successo: ', _successCount, '\n')
    console.log('Test con errore: ', _errorCount, '\n')
    process.exit(0)
  })
}())
