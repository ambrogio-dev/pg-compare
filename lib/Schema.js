var Orchestrator = require('orchestrator')

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
    sequences: {},
    tabledata: {}
  }

  /**
   * Retrieve all requested items from the schema
   * @public
   * @function
   * @param {function} callback Callback function
   */
  var load = function (callback) {
    var _orchestrator = new Orchestrator()

    // / keep a reference of orchestrator tasks to execute
    var _tasks = []

    // / for each selected option, add its function to the orchestrator

    // / table option
    if (options.tables) {
      // / add tasks to execute
      _tasks.push('tables')
      _tasks.push('columns')
      _tasks.push('pkeys')
      _tasks.push('unique')

      // / keep a reference of loaded table
      var _tables

      // / load tables data
      _orchestrator.add('tables', function (next) {
        // / retrieve all db tables
        getTables(function (err, tables) {
          if (!err) {
            _tables = tables
            for (var i in tables) {
              var _table = tables[i]
              __data.tables[_table.name] = {
                name: _table.name,
                comment: _table.comment,
                fkeys: []
              }
            }
          }
          next(err)
        })
      })

      // / load columns data for each found table (execute it after table loading)
      _orchestrator.add('columns', ['tables'], function (next) {
        // / create an orchestrator to execute functions on each table
        var _orchestratorColumns = new Orchestrator()

        // / keep a reference of task to execute ('columns_' + tableName)
        var _columnTasks = []

        // / retrieve columns data for a table
        var _retrieveColumns = function (tableName) {
          _orchestratorColumns.add('columns_' + tableName, function (next) {
            getColumns(tableName, function (err, columns) {
              if (!err) {
                __data.tables[tableName].columns = columns
              }
              next(err)
            })
          })
        }

        // / add a task for each found table
        for (var i in _tables) {
          var _tableName = _tables[i].name
          _columnTasks.push('columns_' + _tableName)
          _retrieveColumns(_tableName)
        }
        _orchestratorColumns.start(_columnTasks, function (err) {
          next(err)
        })
      })

      // / load primary keys data
      _orchestrator.add('pkeys', function (next) {
        // / retrieve primary keys data
        getPkeys(function (err, pkeys) {
          if (!err) {
            __data.pkeys = pkeys
          }
          next(err)
        })
      })

      // / load unique constraints data
      _orchestrator.add('unique', function (next) {
        // / retrieve unique constraints data
        getUniqueConstraints(function (err, unique) {
          if (!err) {
            __data.unique = unique
          }
          next(err)
        })
      })
    }

    // / foreign keys option
    if (options.fkeys) {
      _tasks.push('fkeys')

      _orchestrator.add('fkeys', function (next) {
        // / retrieve foreign keys data
        getFkeys(function (err, fkeys) {
          if (!err) {
            __data.fkeys = fkeys
          }
          next(err)
        })
      })
    }

    // / indexes option
    if (options.indexes) {
      _tasks.push('indexes')

      _orchestrator.add('indexes', function (next) {
        // / retrieve indexes data
        getIndexes(function (err, indexes) {
          if (!err) {
            __data.indexes = indexes
          }
          next(err)
        })
      })
    }

    // / functions option
    if (options.functions) {
      _tasks.push('functions')

      _orchestrator.add('functions', function (next) {
        // / retrieve functions data
        getFunctions(function (err, functions) {
          if (!err) {
            __data.functions = functions
          }
          next(err)
        })
      })
    }

    // / types option
    if (options.types) {
      _tasks.push('types')

      _orchestrator.add('types', function (next) {
        // / retrieve types data
        getTypes(function (err, types) {
          if (!err) {
            __data.types = types
          }
          next(err)
        })
      })
    }

    // / views option
    if (options.views) {
      _tasks.push('views')

      _orchestrator.add('views', function (next) {
        // / retrieve views data
        getViews(function (err, views) {
          if (!err) {
            __data.views = views
          }
          next(err)
        })
      })
    }

    // / sequences option
    if (options.sequences) {
      _tasks.push('sequences')

      _orchestrator.add('sequences', function (next) {
        // / retrieve sequences data
        getSequences(function (err, sequences) {
          if (!err) {
            __data.sequences = sequences
          }
          next(err)
        })
      })
    }

    // / rows option
    if (options.rows && options.rows.length > 0) {
      _tasks.push('rows')

      _orchestrator.add('rows', function (next) {
        var _rowsOrchestrator = new Orchestrator()
        var _rowsTasks = []
        // / retrieve table pkeys and rows for each table in option.rows
        var _executeRows = function (i) {
          if (!options.rows[i]) {
            // / no more tables
            _rowsOrchestrator.start(_rowsTasks, function (err) {
              next(err)
            })
          } else {
            var _tablename = options.rows[i]
            _rowsTasks.push(_tablename)
            _rowsOrchestrator.add(_tablename, function (next) {
              getTableRows(_tablename, (function (err, tabledata) {
                if (!err) {
                  __data.tabledata[_tablename] = tabledata
                }
                next(err)
              }))
            })
            _executeRows(++i)
          }
        }
        _executeRows(0)
      })
    }

    _orchestrator.start(_tasks, function (err) {
      log.verbose('log', 'load done')
      callback(err)
    })
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
      "quote_ident(t.schemaname)||'.'||quote_ident(t.tablename) AS name, " +
      'd.description  AS comment ' +
      'FROM pg_tables t ' +
      "LEFT JOIN pg_attribute a ON a.attrelid=(quote_ident(t.schemaname)||'.'||quote_ident(t.tablename))::regclass AND a.attname='tableoid' " +
      'LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = 0' +
      'WHERE schemaname != ALL($1)',
      ['{information_schema,pg_catalog}'],
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
    log.verbose('log', 'loading columns', table, ' ...')
    db.query('SELECT DISTINCT ' +
    'a.attname                            AS name, ' +
    'format_type(a.atttypid, a.atttypmod) AS type, ' +
    'NOT a.attnotnull                     AS nullable, ' +
    'f.adsrc                              AS default, ' +
    'd.description                        AS comment, ' +
    'a.attnum                             AS attnum ' +
    'FROM pg_attribute a  ' +
    'LEFT JOIN pg_index p ON p.indrelid = a.attrelid AND a.attnum = ANY(p.indkey) ' +
    'LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum ' +
    'LEFT JOIN pg_attrdef f ON f.adrelid = a.attrelid  AND f.adnum = a.attnum ' +
    'WHERE a.attnum > 0 ' +
    'AND NOT a.attisdropped ' +
    'AND a.attrelid = ($1)::regclass ' +
    'ORDER BY a.attnum',
      [table],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading columns', table, err)
          callback(err)
          return
        }
        for (var i in result.rows) {
          // / format the right type for varchar columns
          if (result.rows[i].type.indexOf('character varying') !== -1) {
            result.rows[i].type = result.rows[i].type.replace('character varying', 'varchar')
          }
          // format the right type for serial columns
          if (result.rows[i].default && result.rows[i].default.indexOf('nextval') !== -1) {
            result.rows[i].default = '$sequence'
            result.rows[i].type = result.rows[i].type === 'bigint' ? 'bigserial' : 'serial'
            result.rows[i].sequence = result.rows[i].default
          }
        }
        log.verbose('log', 'loading columns', table, ' success')
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
    'x.table_name AS target_table,' +
    'ARRAY_AGG(x.column_name::text ORDER BY x.ordinal_position) AS target_columns, ' +
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
    db.query("SELECT 	quote_ident(n.nspname)||'.'||quote_ident(p.proname)||'('||pg_catalog.pg_get_function_arguments(p.oid)||')' as name, " +
	'pg_get_functiondef(p.oid) AS source ' +
  'FROM pg_proc p ' +
  'JOIN pg_namespace n on p.pronamespace = n.oid ' +
  'LEFT join pg_user u on u.usesysid = p.proowner ' +
  'WHERE pg_catalog.pg_function_is_visible(p.oid) ' +
    'AND NOT p.proisagg ' +
    'AND n.nspname <> $1 ' +
    'AND n.nspname <> $2 ' +
	  'AND u.usename = CURRENT_USER ' +
  'ORDER BY 1',
      ['pg_catalog','information_schema'],
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
        // / keep a reference of found sequences
        var _sequences = []

        var _orchestratorSequences = new Orchestrator()
        var _sequenceTasks = []

        // / retrieve current value for each found sequence
        var _retrieveCurrVal = function (seqName) {
          // / add this operation to sequences orchestrator
          _orchestratorSequences.add('seq_' + result.rows[i].name, function (next) {
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
                next(err)
              }
            )
          })
        }
        // / for each sequence, add it to tasks and retrieve current value
        for (var i in result.rows) {
          _sequenceTasks.push('seq_' + result.rows[i].name)
          _retrieveCurrVal(result.rows[i].name)
        }
        _orchestratorSequences.start(_sequenceTasks, function (err) {
          if (!err) {
            log.verbose('log', 'loading sequences success')
          }
          callback(err, _sequences)
        })
      }
    )
  }

  /**
   * Retrieve all primary keys and all rows for the given table
   * @public
   * @function
   * @param {string} tablename Table to analyze
   * @param {function} callback Callback function
   */
  var getTableRows = function (tablename, callback) {
    log.verbose('log', 'loading table...')
    db.query('SELECT ' +
    'c.constraint_name AS name, ' +
    'c.table_name AS tablename, ' +
    'ARRAY_AGG(u.column_name::text ORDER BY u.column_name) AS columns ' +
    'FROM information_schema.table_constraints c ' +
    'INNER JOIN information_schema.constraint_column_usage u on c.constraint_name = u.constraint_name and c.table_name = u.table_name ' +
    "WHERE constraint_type = 'PRIMARY KEY' AND c.table_name = $1" +
    'GROUP BY name, tablename ' +
    'ORDER BY c.constraint_name, tablename',
      [tablename],
      function (err, result) {
        if (err) {
          log.verbose('error', 'loading table primary key error', err)
          callback(err)
          return
        }
        var _pkeyColumns = (result.rows && result.rows.length > 0) ? result.rows[0].columns : []
        // / retrieve all rows
        db.query('SELECT * ' +
        'FROM ' + tablename,
          function (err, result) {
            if (err) {
              if (err.code === '42P01') {
                // / table not exists, do not notify error
                callback(false, {
                  pkeys: [],
                  rows: [],
                  exists: false,
                  valid: false
                })
                return
              }
              log.verbose('error', 'loading table rows error', err)
              callback(err)
              return
            }
            // / it works only with rows < 256
            if (result.rows && result.rows.length > 256) {
              callback(false, {
                pkeys: [],
                rows: [],
                exists: true,
                valid: false
              })
            } else {
              callback(false, {
                pkeys: _pkeyColumns,
                rows: result.rows,
                exists: true,
                valid: true
              })
            }
          }
        )
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
    getTableRows: getTableRows,
    load: load,
    data: function () {
      return __data
    }
  }
}

module.exports = Schema
