var tools = require('a-toolbox')

var log = require('./log')

/**
 * @param {DB} db
 * @param {object} options
 * @param {boolean} options.tables Flag to indicates if checking tables (include columns, primary keys and unique constraints)
 * @param {boolean} options.fkeys Flag to indicates if checking foreign keys
 * @param {boolean} options.functions Flag to indicates if checking functions
 * @param {boolean} options.indexes Flag to indicates if checking indexes
 * @param {boolean} options.types Flag to indicates if checking types
 * @param {boolean} options.views Flag to indicates if checking views
 * @param {boolean} options.sequences Flag to indicates if checking sequences next value
 */
var Schema = function (db, options) {
  var __data = {
    tables: {},
    fkeys: {},
    indexes: {},
    functions: {},
    types: {},
    views: {},
    sequences: {}
  }

  /**
   * Retrieve all requested items from the schema
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var load = function (callback) {
    var _errorFlag = false
    var _tasks = new tools.tasks(function () {
      log.verbose('log', 'load done')
      callback(_errorFlag)
    })
    // / declare tasks
    if (options.tables) {
      _tasks.todo('tables')
      _tasks.todo('pkeys')
      _tasks.todo('unique')
    }
    if (options.fkeys) {
      _tasks.todo('fkeys')
    }
    if (options.indexes) {
      _tasks.todo('indexes')
    }
    if (options.functions) {
      _tasks.todo('functions')
    }
    if (options.types) {
      _tasks.todo('types')
    }
    if (options.views) {
      _tasks.todo('views')
    }
    if (options.sequences) {
      _tasks.todo('sequences')
    }
    if (options.tables) {
      // / load tables data
      var _jobColumns = new tools.tasks(function () {
        log.verbose('log', 'done tables')
        _tasks.done('tables')
      })
      // / retrieve all db tables
      getTables(function (err, tables) {
        _errorFlag = _errorFlag || err
        if (!err) {
          // declare columns tasks to retrieve data for columns of each table
          _jobColumns.todo('table#columns')
          for (var i in tables) {
            var _tableName = tables[i].name
            _jobColumns.todo('table#columns#' + _tableName)
          }
          _jobColumns.done('table#columns')
          for (var i in tables) {
            var _table = tables[i]
            __data.tables[_table.name] = {
              name: _table.name,
              comment: _table.comment,
              fkeys: []
            }
            var _f = (function (tableName) {
              // / retrieve columns data
              getColumns(tableName, function (err, columns) {
                _errorFlag = _errorFlag || err
                if (!err) {
                  __data.tables[tableName].columns = columns
                }
                _jobColumns.done('table#columns#' + tableName)
              })
            }(_table.name))
          }
        }
      })

      // / retrieve primary keys data
      getPkeys(function (err, pkeys) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.pkeys = pkeys
        }
        _tasks.done('pkeys')
      })

      // / retrieve unique constraints data
      getUniqueConstraints(function (err, unique) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.unique = unique
        }
        _tasks.done('unique')
      })
    }

    if (options.fkeys) {
      // / retrieve foreign keys data
      getFkeys(function (err, fkeys) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.fkeys = fkeys
        }
        _tasks.done('fkeys')
      })
    }

    if (options.indexes) {
      // / retrieve indexes data
      getIndexes(function (err, indexes) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.indexes = indexes
        }
        _tasks.done('indexes')
      })
    }

    if (options.functions) {
      // / retrieve functions data
      getFunctions(function (err, functions) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.functions = functions
        }
        _tasks.done('functions')
      })
    }

    if (options.types) {
      // / retrieve types data
      getTypes(function (err, types) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.types = types
        }
        _tasks.done('types')
      })
    }

    if (options.views) {
      // / retrieve views data
      getViews(function (err, views) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.views = views
        }
        _tasks.done('views')
      })
    }

    if (options.sequences) {
      // / retrieve sequences data
      getSequences(function (err, sequences) {
        _errorFlag = _errorFlag || err
        if (!err) {
          __data.sequences = sequences
        }
        _tasks.done('sequences')
      })
    }
  }

  /**
   * Retrieve table names
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getTables = function (callback) {
    log.verbose('log', 'loading tables ...')
    db.query(
      'SELECT ' +
      't.tablename    AS name, ' +
      'd.description  AS comment ' +
      'FROM pg_tables t ' +
      "LEFT JOIN pg_attribute a ON a.attrelid=t.tablename::regclass AND a.attname='tableoid' " +
      'LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = 0' +
      'WHERE schemaname = $1',
      ['public'],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading tables error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading tables success')
        callback(false, result.rows)
      })
  }

  /**
   * Retrieve all columns data for a given table
   * @public
   * @function
   * @param {number} table Table id
   * @param {function} callback Callback function
   */
  var getColumns = function (table, callback) {
    log.verbose('log', 'loading colums', table, ' ...')
    db.query('SELECT DISTINCT ' +
    'a.attname                            AS name, ' +
    'format_type(a.atttypid, a.atttypmod) AS type, ' +
    'NOT a.attnotnull                     AS nullable, ' +
    'COALESCE(p.indisprimary, FALSE)      AS pkey, ' +
    'f.adsrc                              AS default, ' +
    'd.description                        AS comment, ' +
    'a.attnum                             AS attnum ' +
    'FROM pg_attribute a  ' +
    'LEFT JOIN pg_index p ON p.indrelid = a.attrelid AND a.attnum = ANY(p.indkey) ' +
    'LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum ' +
    'LEFT JOIN pg_attrdef f ON f.adrelid = a.attrelid  AND f.adnum = a.attnum ' +
    'WHERE a.attnum > 0 ' +
    'AND NOT a.attisdropped ' +
    'AND a.attrelid = $1::regclass ' +
    'ORDER BY a.attnum',
      [table],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading colums', table, err)
          callback(err)
          return
        }
        for (var i in result.rows) {
          // / format the right type for varchar columns
          if (result.rows[i].type.indexOf('character varying') != -1) {
            result.rows[i].type = result.rows[i].type.replace('character varying', 'varchar')
          }
          // format the right type for serial columns
          if (result.rows[i].default && result.rows[i].default.indexOf('nextval') != -1) {
            result.rows[i].default = '$sequence'
            result.rows[i].type = result.rows[i].type == 'bigint' ? 'bigserial' : 'serial'
            result.rows[i].sequence = result.rows[i].default
          }
        }
        log.verbose('log', 'loading colums', table, ' success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all primary keys data
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getPkeys = function (callback) {
    log.verbose('log', 'loading pkeys ...')
    db.query('SELECT ' +
    'c.constraint_name AS name, ' +
    'c.table_name AS tablename, ' +
    'ARRAY_AGG(u.column_name::text ORDER BY u.column_name) AS columns ' +
    'FROM information_schema.table_constraints c ' +
    'INNER JOIN information_schema.constraint_column_usage u on c.constraint_name = u.constraint_name and c.table_name = u.table_name ' +
    "WHERE constraint_type = 'PRIMARY KEY' " +
    'GROUP BY name, tablename ' +
    'ORDER BY c.constraint_name, tablename',
      [],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading pkeys error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading pkeys success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all unique constraints data
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getUniqueConstraints = function (callback) {
    log.verbose('log', 'loading unique constraints ...')
    db.query('SELECT ' +
    'c.constraint_name AS name, ' +
    'c.table_name AS tablename, ' +
    'ARRAY_AGG(u.column_name::text ORDER BY u.column_name) AS columns ' +
    'FROM information_schema.table_constraints c ' +
    'INNER JOIN information_schema.constraint_column_usage u on c.constraint_name = u.constraint_name and c.table_name = u.table_name ' +
    "WHERE constraint_type = 'UNIQUE' " +
    'GROUP BY name, tablename ' +
    'ORDER BY c.constraint_name, tablename',
      [],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading unique constraints error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading unique constraints success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all foreign keys data
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getFkeys = function (callback) {
    log.verbose('log', 'loading fkeys ...')
    db.query('SELECT ' +
    "CASE c.match_option WHEN 'FULL' THEN 'FULL' ELSE 'SIMPLE' END AS match, " +
    'c.constraint_name AS name, ' +
    // 'x.table_schema AS schema_name, ' +
    'x.table_name AS target_table,' +
    'ARRAY_AGG(x.column_name::text ORDER BY x.ordinal_position) AS target_columns, ' +
    // 'y.table_schema AS foreign_schema_name, ' +
    'y.table_name AS source_table, ' +
    'ARRAY_AGG(y.column_name::text ORDER BY x.ordinal_position) AS source_columns, ' +
    'c.update_rule AS on_update, ' +
    'c.delete_rule AS on_delete ' +
    'FROM information_schema.referential_constraints c ' +
    'INNER JOIN information_schema.key_column_usage x ON x.constraint_name = c.constraint_name ' +
    'INNER JOIN information_schema.key_column_usage y ON y.ordinal_position = x.position_in_unique_constraint AND y.constraint_name = c.unique_constraint_name ' +
    'GROUP BY match, name, target_table, source_table, on_update, on_delete ' +
    'ORDER BY c.constraint_name',
      [],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading fkeys error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading fkeys success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all functions
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getFunctions = function (callback) {
    log.verbose('log', 'loading functions ...')
    db.query('SELECT routine_name AS name, ' +
    // 'proargnames AS args, ' +
    'pg_get_functiondef(routine_name::regproc) AS source ' +
    'FROM information_schema.routines ' +
    // 'INNER JOIN pg_proc ON routine_name = proname ' +
    'WHERE specific_schema = $1',
      ['public'],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading functions error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading functions success')
        // / for each function retrieve the function arguments
        for (var i in result.rows) {
          var _args = result.rows[i].source.match(/FUNCTION.+\(([\w\s\,]*)\)/)
          result.rows[i].args = _args ? _args[1] : ''
        }
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all indexes
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getIndexes = function (callback) {
    log.verbose('log', 'loading indexes ...')
    db.query('SELECT ' +
    // n.nspname AS schema,
    't.relname  AS table, ' +
    'c.relname  AS name, ' +
    'pg_get_indexdef(indexrelid) AS source ' +
    'FROM pg_catalog.pg_class c ' +
    'JOIN pg_catalog.pg_namespace n ON n.oid    = c.relnamespace ' +
    'JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ' +
    'JOIN pg_catalog.pg_class t ON i.indrelid   = t.oid ' +
    "WHERE c.relkind = 'i' " +
    "AND n.nspname NOT IN ('pg_catalog', 'pg_toast') " +
    'AND pg_catalog.pg_table_is_visible(c.oid) ' +
    'AND i.indisprimary IS FALSE ' +
    'ORDER BY n.nspname, t.relname, c.relname',
      [],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading indexes error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading indexes success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all types
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getTypes = function (callback) {
    log.verbose('log', 'loading types ...')
    db.query('SELECT ' +
    // 'n.nspname AS schema, ' +
    't.typname AS name, ' +
    "string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) AS values " +
    'FROM pg_type t ' +
    'JOIN pg_enum e ON t.oid = e.enumtypid  ' +
    'JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace ' +
    'WHERE nspname = $1 ' +
    'GROUP BY n.nspname, name ' +
    'ORDER BY name',
      ['public'],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading types error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading types success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all views
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getViews = function (callback) {
    log.verbose('log', 'loading views ...')
    db.query('SELECT viewname AS name, ' +
    'definition AS source ' +
    // schemaname, viewowner
    'FROM pg_views ' +
    'WHERE schemaname = $1',
      ['public'],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading views error', err)
          callback(err)
          return
        }
        log.verbose('log', 'loading views success')
        callback(false, result.rows)
      }
    )
  }

  /**
   * Retrieve all sequences with current value
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var getSequences = function (callback) {
    log.verbose('log', 'loading sequences ...')
    db.query('SELECT sequence_name AS name ' +
    'FROM information_schema.sequences ' +
    'WHERE sequence_schema = $1',
      ['public'],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading sequences error', err)
          callback(err)
          return
        }
        var _sequences = []
        var _jobDone = new tools.tasks(function () {
          log.verbose('log', 'loading sequences success')
          callback(false, _sequences)
        })
        _jobDone.todo('sequences')
        for (var i in result.rows) {
          _jobDone.todo('seq#' + result.rows[i].name)
        }
        _jobDone.done('sequences')
        // / retrieve current value for each found sequence
        var _retrieveCurrVal = function (seqName) {
          db.query('SELECT last_value FROM ' + seqName,
            function (err, result) {
              if (err) {
                log.verbose('error', 'loading sequences error', err)
              } else if (result.rows && result.rows[0]) {
                _sequences.push({
                  name: seqName,
                  currval: result.rows[0].last_value
                })
              }
              _jobDone.done('seq#' + seqName)
            }
          )
        }
        for (var i in result.rows) {
          _retrieveCurrVal(result.rows[i].name)
        }
      }
    )
  }

  return {
    getTables: getTables,
    getColumns: getColumns,
    getPkeys: getPkeys,
    getUniqueConstraints: getUniqueConstraints,
    getFkeys: getFkeys,
    getIndexes: getIndexes,
    getFunctions: getFunctions,
    getTypes: getTypes,
    getViews: getViews,
    getSequences: getSequences,
    load: load,
    data: function () {
      return __data
    }
  }
}

module.exports = Schema
