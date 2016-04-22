var tools = require('a-toolbox')
var pg = require('pg')

var DbCompare = require('./lib/DbCompare')

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

var _successCount = 0
var _errorCount = 0

var dbCompare = new DbCompare()

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

var _tableCompare = function (full, callback) {
  var _typeSql1 = 'DROP TABLE IF EXISTS factories CASCADE; \n' +
    'DROP TABLE IF EXISTS users CASCADE; \n' +
    'CREATE TABLE factories (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR NOT NULL);\n' +
    'ALTER TABLE factories OWNER TO alatini;\n' +
    'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR NOT NULL, ' +
      "surname VARCHAR NOT NULL, age INTEGER DEFAULT 10, phone VARCHAR, gender CHAR NOT NULL DEFAULT 'M');\n" +
    'ALTER TABLE users OWNER TO alatini;\n' +
    "COMMENT ON TABLE users IS 'prova';\n" +
    "COMMENT ON COLUMN users.name IS 'nome';\n" +
    "COMMENT ON COLUMN users.surname IS 'cognome';" +
    "COMMENT ON COLUMN users.phone IS 'telefono';"
  // 'select * from pg_tables'
  var _typeSql2 = 'DROP TABLE IF EXISTS cities CASCADE; \n' +
    'DROP TABLE IF EXISTS users CASCADE; \n' +
    'CREATE TABLE cities (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR NOT NULL);\n' +
    'ALTER TABLE cities OWNER TO alatini;\n' +
    'CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY, name VARCHAR, ' +
      "age INTEGER DEFAULT 20, phone INTEGER, gender CHAR NOT NULL DEFAULT 'M');\n" +
    'ALTER TABLE users OWNER TO alatini;\n' +
    "COMMENT ON COLUMN users.name IS 'cognome';" +
    "COMMENT ON COLUMN users.phone IS 'telefono';"
  var _typeDifference = 'CREATE TABLE factories (\nid serial NOT NULL,name varchar NOT NULL\n);\n' + 
    'ALTER TABLE factories OWNER TO alatini;\n' +
    '/* WARNING: column name of table users is not null but without default value*/\n' +
    'ALTER TABLE users ALTER COLUMN name SET NOT NULL;\n' +
    "COMMENT ON COLUMN users.name IS 'nome';\n" +
    'ALTER TABLE users ADD COLUMN surname varchar NOT NULL;\n' +
    "COMMENT ON COLUMN users.surname IS 'cognome';\n" +
    'ALTER TABLE users ALTER COLUMN age SET DEFAULT 10;\n' +
    'ALTER TABLE users ALTER COLUMN phone SET DATA TYPE varchar;\n'
  if (full) {
    _typeDifference += 'DROP TABLE cities;'
  }
  var _config = tools.object.clone(configTemplate)
  _config.compare.schema = {
    tables: true
  }
  if (!full) {
    _config.compare.options.mode = null
  }
  var _conn1 = _config.connection1
  var _conn2 = _config.connection2
  var _db1 = new pg.Client(_conn1)
  var _db2 = new pg.Client(_conn2)
  _db1.connect(function (err) {
    if (err) {
      console.error('error connecting to database 1:', err)
      callback && callback(true)
      return
    }
    _db1.query(
      _typeSql1,
      function (err, result) {
        if (err) {
          console.error('error inserting in database 1:', err)
          callback && callback(true)
          return
        }
        _db1.end()
        // / try to connect to db 1
        _db2.connect(function (err) {
          if (err) {
            console.error('error connecting to database 2:', err)
            callback && callback(true)
            return
          }
          _db2.query(
            _typeSql2,
            function (err, result) {
              if (err) {
                console.error('error inserting database 2:', err)
                callback && callback(true)
                return
              }
              _db2.end()
              dbCompare.run(JSON.stringify(_config), function (err) {
                if (!err) {
                  var _differences = dbCompare.getDeltaDifference('tables')
                  if (_differences.join('') == _typeDifference) {
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

var testTableComparing = function (next) {
  console.log('testTableComparing executing')
  _tableCompare(false, function (err) {
    if (err) {
      console.log('testTableComparing error')
      _errorCount++
    } else {
      console.log('testTableComparing success')
      _successCount++
    }
    next && next()
  })
}

var testTableComparingFull = function (next) {
  console.log('testTableComparingFull executing')
  _tableCompare(true, function (err) {
    if (err) {
      console.log('testTableComparingFull error')
      _errorCount++
    } else {
      console.log('testTableComparingFull success')
      _successCount++
    }
    next && next()
  })
}

var _typeCompare = function (full, callback) {
  var _typeSql1 = 'DROP TYPE IF EXISTS fruit; \n' +
    'DROP TYPE IF EXISTS sport; \n' +
    "CREATE TYPE fruit AS ENUM ('BANANA','APPLE','PEACH','PINEAPPLE'); \n" +
    "CREATE TYPE sport AS ENUM ('FOOTBALL','BASKETBALL','VOLLEY');"
  // 'select * from pg_tables'
  var _typeSql2 = 'DROP TYPE IF EXISTS fruit; \n' +
    'DROP TYPE IF EXISTS os; \n' +
    "CREATE TYPE fruit AS ENUM ('BANANA','PEACH','PINEAPPLE'); \n" +
    "CREATE TYPE os AS ENUM ('WINDOWS','LINUX','MAC');"
  var _typeDifference = "ALTER TYPE fruit ADD VALUE 'APPLE' AFTER 'BANANA';\n" +
    "CREATE TYPE sport AS ENUM ('FOOTBALL','BASKETBALL','VOLLEY');"
  if (full) {
    _typeDifference += '\nDROP TYPE os;'
  }
  var _config = tools.object.clone(configTemplate)
  _config.compare.schema = {
    types: true
  }
  if (!full) {
    _config.compare.options.mode = null
  }
  var _conn1 = _config.connection1
  var _conn2 = _config.connection2
  var _db1 = new pg.Client(_conn1)
  var _db2 = new pg.Client(_conn2)
  _db1.connect(function (err) {
    if (err) {
      console.error('error connecting to database 1:', err)
      callback && callback(true)
      return
    }
    _db1.query(
      _typeSql1,
      function (err, result) {
        if (err) {
          console.error('error inserting in database 1:', err)
          callback && callback(true)
          return
        }
        _db1.end()
        // / try to connect to db 1
        _db2.connect(function (err) {
          if (err) {
            console.error('error connecting to database 2:', err)
            callback && callback(true)
            return
          }
          _db2.query(
            _typeSql2,
            function (err, result) {
              if (err) {
                console.error('error inserting database 2:', err)
                callback && callback(true)
                return
              }
              _db2.end()
              dbCompare.run(JSON.stringify(_config), function (err) {
                if (!err) {
                  var _differences = dbCompare.getDeltaDifference('types')
                  if (_differences.join('') == _typeDifference) {
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

var testTypeComparing = function (next) {
  console.log('testTypeComparing executing')
  _typeCompare(false, function (err) {
    if (err) {
      console.log('testTypeComparing error')
      _errorCount++
    } else {
      console.log('testTypeComparing success')
      _successCount++
    }
    next && next()
  })
}

var testTypeComparingFull = function (next) {
  console.log('testTypeComparingFull executing')
  _typeCompare(true, function (err) {
    if (err) {
      console.log('testTypeComparingFull error')
      _errorCount++
    } else {
      console.log('testTypeComparingFull success')
      _successCount++
    }
    next && next()
  })
}

var executeTest = function (testFunctions, callback) {
  var _exe = function (i) {
    testFunctions[i](function () {
      if (testFunctions[i + 1]) {
        testFunctions[i + 1]
      } else {
        callback && callback()
      }
    })
  }
  for (var i = 0; i < testFunctions.length; i++) {
    _exe(i)
  }
}

var runTests = (function () {
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
    testTableComparing,
    testTableComparingFull,
    //testTypeComparing,
    //testTypeComparingFull
  ]
  executeTest(_tests, function () {
    console.log('Test eseguiti: ', _tests.length, '\n')
    console.log('Test con successo: ', _successCount, '\n')
    console.log('Test con errore: ', _errorCount, '\n')
    process.exit(0)
  })
}())
