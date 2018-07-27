var tools = require('a-toolbox')

var log = require('./log')

/**
 * produce sql delta from schema1 to schema2
 * means schema1 = schema2 + delta
 * @param {object} schema1
 * @param {object} schema2
 * @param {object} options
 * @param {string} [options.mode=preserve] full or preserve (do not drop if something missing from schema1 to schema2)
 * @param {string} [options.owner=postgres]
 */
var Delta = function (schema1, schema2, options) {
  // / load default values if some options don't have value (mode = 'preserve', owner = 'postgres')
  if (!options) {
    options = {}
  }
  if (!options.mode || options.mode !== 'full') {
    options.mode = 'preserve'
  }
  if (!options.owner) {
    options.owner = 'postgres'
  }

  var __delta = {
    tables: [],
    pkeys: [],
    unique: [],
    fkeys: [],
    functions: [],
    indexes: [],
    types: [],
    views: [],
    sequences: [],
    rows: []
  }

  var __sql = ''
  /**
   * Generate related sql from a given column
   * @private
   * @function
   * @param {object} column Column to analyze
   * @param {string} column.name Name of the column
   * @param {string} column.type Type of the column
   * @param {?boolean} column.nullable Flag that indicates if column could have null values
   * @param {?string} column.default Default value
   * @param {?boolean} column.pkey Flag that indicates if this column is a primary key
   * @returns {string} sql
   */
  var __sqlColumn = function (column) {
    var _sql = column.name + ' ' + column.type +
    (column.nullable ? '' : ' NOT NULL')
    // / check if column has a default value
    if (column.default && column.default !== '$sequence') {
      _sql += ' DEFAULT ' + column.default
    }
    return _sql
  }

  /**
   * Generate right format for enum values
   * Es. [1, 2, 3] -> ['1', '2', '3']
   * @private
   * @function
   * @param {Array} values Enum values
   * @returns {Array} values
   */
  var __sqlEnumValues = function (values) {
    return values.split(',').map(function (v) {
      return "'" + v + "'"
    }).join(',')
  }

  /**
   * Generate right format to use a value in sql query
   * @private
   * @function
   * @param {*} value Value to transform
   * @returns {string}
   */  
  var _formatSqlValue = function (value) {
    if (value == null || value==='' || (value instanceof Array && value.length == 0)) {
      return 'null'
    }
    if (value instanceof Date) {
      return "'" + (new Date(value)).toISOString() + "'"
    }
    if (value instanceof Object) {
      return "'" + JSON.stringify(value) + "'"
    }
    if (typeof value === 'string') {
      value = value.replace(/'/g, "''")
    }
    return "'" + value + "'"
  }

  /**
   * Compare each table of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareTables = function () {
    // / retrieve tables for both schema
    var _tables1 = Object.keys(schema1.tables)
    var _tables2 = Object.keys(schema2.tables)

    var t, c1, c2, _columnName1, _columnName2, _exists
    // / cycle each table of schema 1 to compare its data to schema 2
    for (t in _tables1) {
      var _sqlTable = []
      var _sqlColumns = []
      // / retrieve table columns
      var _columns1 = schema1.tables[_tables1[t]].columns
      // / check if schema 2 contains this table
      if (tools.array.contains(_tables2, _tables1[t])) {
        // / schema 2 has this table

        // / retrieve table 2 columns
        var _columns2 = schema2.tables[_tables1[t]].columns

        // / cycle all columns of table 1 and compare them to columns of table 2
        for (c1 in _columns1) {
          _columnName1 = _columns1[c1].name
          // / check if column exists on table 2
          _exists = false
          for (c2 in _columns2) {
            _columnName2 = _columns2[c2].name
            // / column exists
            if (_columnName1 === _columnName2) {
              _exists = true
              break
            }
          }
          // / if column1 is not in table2, add it
          if (!_exists) {
            _sqlColumns.push('ALTER TABLE ' + _tables1[t] +
              ' ADD COLUMN ' + __sqlColumn(_columns1[c1]) + ';')
          } else {
            var _sqlEdit = 'ALTER TABLE ' + _tables1[t] +
            ' ALTER COLUMN ' + _columns1[c1].name
            // / check column type
            if (_columns1[c1].type !== _columns2[c2].type) {
              _sqlColumns.push(_sqlEdit + ' SET DATA TYPE ' + _columns1[c1].type + ';')
            }
            // / check column default value
            if (_columns1[c1].default !== _columns2[c2].default) {
              _sqlColumns.push(_sqlEdit + (_columns1[c1].default ? ' SET DEFAULT ' + _columns1[c1].default : ' DROP DEFAULT ') + ';')
            }
            // / check column nullable
            if (_columns1[c1].nullable !== _columns2[c2].nullable) {
              if (!_columns1[c1].nullable && !_columns1[c1].default) {
                log.verbose('error', 'column ' + _columns1[c1].name + ' of table ' + _tables1[t] + ' is not null but without default value')
                _sqlColumns.push('/* WARNING: column ' + _columns1[c1].name + ' of table ' + _tables1[t] + ' is not null but without default value*/')
              }
              _sqlColumns.push(_sqlEdit + (_columns1[c1].nullable ? ' DROP' : ' SET') + ' NOT NULL' + ';')
            }
          }
          // / check column comment
          if ((!_exists && _columns1[c1].comment) || _columns1[c1].comment !== _columns2[c2].comment) {
            _sqlColumns.push('COMMENT ON COLUMN ' + _tables1[t] + '.' + _columns1[c1].name + ' IS ' +
              (_columns1[c1].comment ? _formatSqlValue(_columns1[c1].comment) : 'NULL') + ';')
          }
        }
        // / if comments on the table are different, modify it
        if (schema1.tables[_tables1[t]].comment !== schema2.tables[_tables1[t]].comment) {
          _sqlColumns.push('COMMENT ON TABLE ' + _tables1[t] + ' IS ' +
            (schema1.tables[_tables1[t]].comment ? _formatSqlValue(schema1.tables[_tables1[t]].comment) : 'NULL') + ';')
        }
        // / if option mode is full, check if column from table 2 missing from column table 1 -> drop
        if (options.mode === 'full') {
          for (c2 in _columns2) {
            _columnName2 = _columns2[c2].name
            // / check if column exists
            _exists = false
            for (c1 in _columns1) {
              _columnName1 = _columns1[c1].name
              // / column exists
              if (_columnName1 === _columnName2) {
                _exists = true
                break
              }
            }
            // / if column2 is not in table1, drop it
            if (!_exists) {
              _sqlColumns.push('ALTER TABLE ' + _tables1[t] +
                ' DROP COLUMN ' + _columnName2 + ';')
            }
          }
        }
        _sqlTable.push(_sqlColumns.join('\n'))
      } else {
        // / table is not present in schema 2, create it
        _sqlTable.push('CREATE TABLE ' + _tables1[t] + ' (')
        var _sqlColumnComment = []
        // / cycle columns to add their sql declaration to create script
        for (var c in _columns1) {
          _sqlColumns.push(__sqlColumn(_columns1[c]))
          // / check if adding column comments
          if (_columns1[c].comment) {
            _sqlColumnComment.push('COMMENT ON COLUMN ' + _tables1[t] + '.' + _columns1[c].name + ' IS ' +
              _formatSqlValue(_columns1[c].comment))
          }
        }
        _sqlTable.push(_sqlColumns.join(','), ');')
        // / add table owner
        _sqlTable.push('ALTER TABLE ' + _tables1[t] + ' OWNER TO ' + options.owner + ';')
        // / add comment if present
        if (schema1.tables[_tables1[t]].comment) {
          _sqlTable.push('COMMENT ON TABLE ' + _tables1[t] + " IS " + _formatSqlValue(schema1.tables[_tables1[t]].comment) + ";")
        }
        // / add comment on columns if present
        if (_sqlColumnComment.length > 0) {
          _sqlTable.push(_sqlColumnComment.join(';\n') + ';')
        }
      }
      if (_sqlColumns.length > 0) {
        __delta.tables.push(_sqlTable.join('\n'), '\n')
      }
    }
    // / if option mode is full, check if there are tables in schema 2 not present in schema 1
    // / drop this tables
    if (options.mode === 'full') {
      for (t in _tables2) {
        if (!tools.array.contains(_tables1, _tables2[t])) {
          __delta.tables.push('DROP TABLE ' + _tables2[t] + ';')
        }
      }
    }
  }

  /**
   * Compare each primary key of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __comparePkeys = function () {
    var _pkeys1 = schema1.pkeys
    var _pkeys2 = schema2.pkeys
    var _sqls = []

    var i, j, _pk1, _pk2, _pkName1, _pkName2, _exists
    // / cycle each primary key of schema 1 to check if exists in schema 2
    for (i in _pkeys1) {
      _pk1 = _pkeys1[i]
      _pkName1 = _pk1.name
      // / check if pkey exists in schema 2
      _exists = false
      for (j in _pkeys2) {
        _pkName2 = _pkeys2[j].name
        // / pkey exists
        if (_pkName1 === _pkName2) {
          _exists = true
          break
        }
      }
      // / if primary key miss on schema 2, add it
      if (!_exists) {
        _sqls.push('ALTER TABLE ' + _pk1.tablename + '\n' +
          '  ADD CONSTRAINT ' + _pkName1 + ' PRIMARY KEY (' + _pk1.columns.join(',') + ')')
      } else {
        // / check if primary key is the same
        if (_pk1.tablename !== _pkeys2[j].tablename || _pk1.columns.sort().join(',') !== _pkeys2[j].columns.sort().join(',')) {
          // / drop old constraint and add the new one
          _sqls.push('ALTER TABLE ' + _pk1.tablename + '\n' +
            '  DROP CONSTRAINT ' + _pkName1)
          _sqls.push('ALTER TABLE ' + _pk1.tablename + '\n' +
            '  ADD CONSTRAINT ' + _pkName1 + ' PRIMARY KEY (' + _pk1.columns.join(',') + ')')
        }
      }
    }
    // / if options mode is full, check if there are primary keys in schema 2 not present in schema 1
    // / drop this pkeys
    if (options.mode === 'full') {
      for (i in _pkeys2) {
        _pk2 = _pkeys2[i]
        _pkName2 = _pk2.name
        // / check if pkey exists
        _exists = false
        for (j in _pkeys1) {
          _pk1 = _pkeys1[j]
          _pkName1 = _pk1.name
          // / pkey exists
          if (_pkName1 === _pkName2) {
            _exists = true
            break
          }
        }
        // / if foreign key is not in schema 1, drop it
        if (!_exists) {
          _sqls.push('ALTER TABLE ' + _pk2.tablename + '\n' +
            '  DROP CONSTRAINT ' + _pkName2)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.pkeys.push(_sqls.join(';\n') + ';')
    } else {
      __delta.pkeys.push('--primary keys match')
    }
  }

  /**
   * Compare each unique constraint of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareUniqueConstraints = function () {
    var _unique1 = schema1.unique
    var _unique2 = schema2.unique
    var _sqls = []

    var i, j, _un1, _un2, _unName1, _unName2, _exists
    // / cycle each unique constraint of schema 1 to check if exists in schema 2
    for (i in _unique1) {
      _un1 = _unique1[i]
      _unName1 = _un1.name
      // / check if unique constraint exists in schema 2
      _exists = false
      for (j in _unique2) {
        _unName2 = _unique2[j].name
        // / unique constraint exists
        if (_unName1 === _unName2) {
          _exists = true
          break
        }
      }
      // / if unique constraint miss on schema 2, add it
      if (!_exists) {
        _sqls.push('ALTER TABLE ' + _un1.tablename + '\n' +
          '  ADD CONSTRAINT ' + _unName1 + ' UNIQUE (' + _un1.columns.join(',') + ')')
      } else {
        // / check if unique constraint is the same
        if (_un1.tablename !== _unique2[j].tablename || _un1.columns.sort().join(',') !== _unique2[j].columns.sort().join(',')) {
          // / drop old constraint and add the new one
          _sqls.push('ALTER TABLE ' + _un1.tablename + '\n' +
            '  DROP CONSTRAINT ' + _unName1)
          _sqls.push('ALTER TABLE ' + _un1.tablename + '\n' +
            '  ADD CONSTRAINT ' + _unName1 + ' UNIQUE (' + _un1.columns.join(',') + ')')
        }
      }
    }
    // / if options mode is full, check if there are unique constraints in schema 2 not present in schema 1
    // / drop this unique
    if (options.mode === 'full') {
      for (i in _unique2) {
        _un2 = _unique2[i]
        _unName2 = _un2.name
        // / check if unique exists
        _exists = false
        for (j in _unique1) {
          _un1 = _unique1[j]
          _unName1 = _un1.name
          // / unique exists
          if (_unName1 === _unName2) {
            _exists = true
            break
          }
        }
        // / if unique constraint is not in schema 1, drop it
        if (!_exists) {
          _sqls.push('ALTER TABLE ' + _un2.tablename + '\n' +
            '  DROP CONSTRAINT ' + _unName2)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.unique.push(_sqls.join(';\n') + ';')
    } else {
      __delta.unique.push('--unique constraints match')
    }
  }

  /**
   * Compare each foreing key of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareFkeys = function () {
    var _fkeys1 = schema1.fkeys
    var _fkeys2 = schema2.fkeys
    var _sqls = []

    var i, j, _fk1, _fk2, _fkName1, _fkName2, _exists
    // / cycle each foreign key of schema 1 to check if exists in schema 2
    for (i in _fkeys1) {
      _fk1 = _fkeys1[i]
      _fkName1 = _fk1.name
      // / check if fkey exists in schema 2
      _exists = false
      for (j in _fkeys2) {
        _fkName2 = _fkeys2[j].name
        // / fkey exists
        if (_fkName1 === _fkName2) {
          _exists = true
          break
        }
      }
      // / if foreign key miss on schema 2, add it
      if (!_exists) {
        _sqls.push('ALTER TABLE ' + _fk1.target_table + '\n' +
          '  ADD CONSTRAINT ' + _fkName1 + ' FOREIGN KEY (' + _fk1.target_columns.join(',') + ')\n' +
          '  REFERENCES ' + _fk1.source_table + ' (' + _fk1.source_columns.join(',') + ')' + ' MATCH ' + _fk1.match + '\n' +
          '  ON UPDATE ' + _fk1.on_update + ' ON DELETE ' + _fk1.on_delete)
      } else {
        // / check if foreign key is the same
        if (_fk1.target_table !== _fkeys2[j].target_table ||
          _fk1.target_columns.join(',') !== _fkeys2[j].target_columns.join(',') ||
          _fk1.source_table !== _fkeys2[j].source_table ||
          _fk1.source_columns.join(',') !== _fkeys2[j].source_columns.join(',') ||
          _fk1.match !== _fkeys2[j].match ||
          _fk1.on_update !== _fkeys2[j].on_update ||
          _fk1.on_delete !== _fkeys2[j].on_delete) {
          // / drop old constraint and add the new one
          _sqls.push('ALTER TABLE ' + _fkeys2[j].target_table + '\n' +
            '  DROP CONSTRAINT ' + _fkName2)
          _sqls.push('ALTER TABLE ' + _fk1.target_table + '\n' +
            '  ADD CONSTRAINT ' + _fkName1 + ' FOREIGN KEY (' + _fk1.target_columns.join(',') + ')\n' +
            '  REFERENCES ' + _fk1.source_table + ' (' + _fk1.source_columns.join(',') + ')' + ' MATCH ' + _fk1.match + '\n' +
            '  ON UPDATE ' + _fk1.on_update + ' ON DELETE ' + _fk1.on_delete)
        }
      }
    }
    // / if options mode is full, check if there are foreign keys in schema 2 not present in schema 1
    // / drop this fkeys
    if (options.mode === 'full') {
      for (i in _fkeys2) {
        _fk2 = _fkeys2[i]
        _fkName2 = _fk2.name
        // / check if fkey exists
        _exists = false
        for (j in _fkeys1) {
          _fk1 = _fkeys1[j]
          _fkName1 = _fk1.name
          // / fkey exists
          if (_fkName1 === _fkName2) {
            _exists = true
            break
          }
        }
        // / if foreign key is not in schema 1, drop it
        if (!_exists) {
          _sqls.push('ALTER TABLE ' + _fk2.target_table + '\n' +
            '  DROP CONSTRAINT ' + _fkName2)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.fkeys.push(_sqls.join(';\n') + ';')
    } else {
      __delta.fkeys.push('--foreign keys match')
    }
  }

  /**
   * Compare each function of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareFunctions = function () {
    var _functions1 = schema1.functions
    var _functions2 = schema2.functions
    var _sqls = []

    var i, j, _f1, _f2, _exists
    // / cycle each function of schema 1 to check if exists in schema 2
    for (i in _functions1) {
      _f1 = _functions1[i]
      // / check if function exists
      _exists = false
      for (j in _functions2) {
        _f2 = _functions2[j]
        // / if function exists
        if (_f1.name === _f2.name) {
          // / if function source is different, replace it
          if (_f1.source !== _f2.source || _f1.args !== _f2.args) {
            _sqls.push(_f1.source)
          }
          _exists = true
          break
        }
      }
      // / if function miss in schema2, add it
      if (!_exists) {
        _sqls.push(_f1.source)
        // / add function owner
        _sqls.push('ALTER FUNCTION ' + _f1.name + '(' + _f1.args + ') OWNER TO ' + options.owner)
      }
    }
    // / if options mode is full, check if there are functions in schema 2 not present in schema 1
    // / drop this functions
    if (options.mode === 'full') {
      for (i in _functions2) {
        _f2 = _functions2[i]
        // / check if function exists
        _exists = false
        for (j in _functions1) {
          _f1 = _functions1[j]
          // / function exists
          if (_f1.name === _f2.name) {
            _exists = true
            break
          }
        }
        // / if function is not in schema1, drop it
        if (!_exists) {
          _sqls.push('DROP FUNCTION ' + _f2.name + '(' + _f2.args + ')')
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.functions.push(_sqls.join(';\n') + ';')
    } else {
      __delta.functions.push('--functions match')
    }
  }

  /**
   * Compare each index of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareIndexes = function () {
    var _indexes1 = schema1.indexes
    var _indexes2 = schema2.indexes
    var _sqls = []

    var i, j, _index1, _index2, _exists

    // / cycle each index of schema 1 to check if exists in schema 2
    for (i in _indexes1) {
      _index1 = _indexes1[i]
      // / check if index exists
      _exists = false
      for (j in _indexes2) {
        _index2 = _indexes2[j]
        // / if index exists
        if (_index1.name === _index2.name) {
          // / if index source is different, replace it
          if (_index1.source !== _index2.source) {
            _sqls.push('DROP INDEX ' + _index1.name + ';\n' + _index1.source)
          }
          _exists = true
          break
        }
      }
      // / if index1 miss in schema2, add it
      if (!_exists) {
        _sqls.push(_index1.source)
      }
    }
    // / if options mode is full, check if there are indexes in schema 2 not present in schema 1
    // / drop this indexes
    if (options.mode === 'full') {
      for (i in _indexes2) {
        _index2 = _indexes2[i]
        // / check if index exists
        _exists = false
        for (j in _indexes1) {
          _index1 = _indexes1[j]
          // / index exists
          if (_index1.name === _index2.name) {
            _exists = true
            break
          }
        }
        // / if index is not in schema1, drop it
        if (!_exists) {
          _sqls.push('DROP INDEX ' + _index2.name)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.indexes.push(_sqls.join(';\n') + ';')
    } else {
      __delta.indexes.push('--indexes match')
    }
  }

  /**
   * Compare each type of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareTypes = function () {
    var _types1 = schema1.types
    var _types2 = schema2.types
    var _sqls = []

    var i, j, _type1, _type2, _exists
    // / cycle each type of schema 1 to check if exists in schema 2
    for (i in _types1) {
      _type1 = _types1[i]
      var _values1 = _type1.values.split(',')
      // / check if type exists
      _exists = false
      for (j in _types2) {
        _type2 = _types2[j]
        // / if type exists
        if (_type1.name === _type2.name) {
          // / if type source is different, replace it

          if (_type1.values !== _type2.values) {
            var _values2 = _type2.values.split(',')
            for (var k = 0; k < _values1.length; k++) {
              if (!tools.array.contains(_values2, _values1[k])) {
                // / insert it after previous value or if the first before next value or if empty without position
                var _sqlPosition = ''
                if (_values1[k - 1]) {
                  _sqlPosition = " AFTER '" + _values1[k - 1] + "'"
                } else if (_values1[k + 1]) {
                  _sqlPosition = " BEFORE '" + _values1[k + 1] + "'"
                }
                _sqls.push('ALTER TYPE ' + _type1.name + " ADD VALUE '" + _values1[k] + "'" + _sqlPosition)
              }
            }
          }
          _exists = true
          break
        }
      }
      // / if type miss in schema2, add it
      if (!_exists) {
        _sqls.push('CREATE TYPE ' + _type1.name + ' AS ENUM (' + __sqlEnumValues(_type1.values) + ')')
        // / add type owner
        _sqls.push('ALTER TYPE ' + _type1.name + ' OWNER TO ' + options.owner)
      }
    }
    // / if options mode is full, check if there are types in schema 2 not present in schema 1
    // / drop this types
    if (options.mode === 'full') {
      for (i in _types2) {
        _type2 = _types2[i]
        // / check if type exists
        _exists = false
        for (j in _types1) {
          _type1 = _types1[j]
          // / type exists
          if (_type1.name === _type2.name) {
            _exists = true
            break
          }
        }
        // / if type is not in schema1, drop it
        if (!_exists) {
          _sqls.push('DROP TYPE ' + _type2.name)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.types.push(_sqls.join(';\n') + ';')
    } else {
      __delta.types.push('--types match')
    }
  }

  /**
   * Compare each view of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __compareViews = function () {
    var _views1 = schema1.views
    var _views2 = schema2.views
    var _sqls = []

    var i, j, _view1, _view2, _exists
    // / cycle each view of schema 1 to check if exists in schema 2
    for (i in _views1) {
      _view1 = _views1[i]
      // / check if view exists
      _exists = false
      for (j in _views2) {
        _view2 = _views2[j]
        // / if view exists
        if (_view1.name === _view2.name) {
          // / if view source is different, replace it
          if (_view1.source !== _view2.source) {
            _sqls.push('DROP VIEW ' + _view1.name + ';\nCREATE VIEW ' + _view1.name + ' AS\n' + _view1.source)
          }
          _exists = true
          break
        }
      }
      // / if view miss in schema2, add it
      if (!_exists) {
        _sqls.push('CREATE VIEW ' + _view1.name + ' AS\n' + _view1.source)
        // / add view owner
        _sqls.push('ALTER TABLE ' + _view1.name + ' OWNER TO ' + options.owner)
      }
    }
    // / if options mode is full, check if there are views in schema 2 not present in schema 1
    // / drop this views
    if (options.mode === 'full') {
      for (i in _views2) {
        _view2 = _views2[i]
        // / check if view exists
        _exists = false
        for (j in _views1) {
          _view1 = _views1[j]
          // / view exists
          if (_view1.name === _view2.name) {
            _exists = true
            break
          }
        }
        // / if view is not in schema, drop it
        if (!_exists) {
          _sqls.push('DROP VIEW ' + _view2.name)
        }
      }
    }

    if (_sqls.length > 0) {
      __delta.views.push(_sqls.join(';\n') + ';')
    } else {
      __delta.views.push('--views match')
    }
  }

  /**
   * Compare next value of each sequence of both schema and retrieve the sql code to execute
   * @private
   * @function
   */
  var __setSequencesValues = function () {
    var _sequences1 = schema1.sequences
    var _sequences2 = schema2.sequences
    var _sqls = []
    // / cycle each sequence of schema 1 to check if exists in schema 2
    for (var i in _sequences1) {
      var _seq1 = _sequences1[i]
      // / check if view exists
      var _exists = false
      for (var j in _sequences2) {
        var _seq2 = _sequences2[j]
        // / if view exists
        if (_seq1.name === _seq2.name) {
          _exists = true
          break
        }
      }
      // / check if both sequences have same current value
      if (_exists && _seq1.currval !== _seq2.currval) {
        _sqls.push("SELECT setval('" + _seq1.name + "', " + _seq1.currval + ')')
      }
    }

    if (_sqls.length > 0) {
      __delta.sequences.push(_sqls.join(';\n') + ';')
    } else {
      __delta.sequences.push('--sequences match')
    }
  }

  /**
   * Compare rows for each given table and generate insert/delete queries to generate same row
   * @private
   * @function
   */
  var __compareRows = function () {
    // / retrieve both table data
    var _tabledata1 = schema1.tabledata
    var _tabledata2 = schema2.tabledata
    var _sqls = []
    
    /**
     * Generate an insert query from a given row
     * @private
     * @function
     * @param {string} tablename Name of the table where inserting
     * @param {object} row Query result row
     * @returns {string}
     */ 
    var _generateInsertQuery = function (tablename, row) {
      var _sql = 'INSERT INTO ' + tablename + '(' + Object.keys(row).join(', ') + ') VALUES ('
      var _values = []
      for (var j in row) {
        _values.push(_formatSqlValue(row[j]))
      }
      _sql += _values.join(', ')
      _sql += ')'
      return _sql
    }
    
    /**
     * Check rows of both tables and generate necessary queries (insert or delete+insert)
     * @private
     * @function
     * @param {string} tablename Name of the table where inserting
     * @param {rows: {object}, pkeys: {Array.<string>}} td1 Data (primary keys and rows) for table of schema 1
     * @param {rows: {object}, pkeys: {Array.<string>}} td2 Data (primary keys and rows) for table of schema 2
     * @returns {inserting:{Array.<string>}, deleting:{Array.<string>}}
     */
    var _retrieveInsertRows = function (tablename, td1, td2) {
      var _rows1 = td1.rows
      var _rows2 = td2.rows

      var _insertQueries = []
      var _deleteQueries = []
      // / check every single row, analyze differences with other schema rows on pkeys columns
      // / or, if no pkeys are present, analyze all columns
      // / if this columns values doesn't match from schema 1 to schema 2, make a simple insert
      // / if it matches check each remaining column value and, if rows are not the same, delete row and insert a new one
      for (var i = 0; i < _rows1.length; i++) {
        // / keep a reference if there are 2 rows with same pkeys values or, if there aren't pkeys, same column values
        var _matchRows = false
        var j
        // / columns values to check (primary keys or all columns)
        var _columns = td1.pkeys.length > 0 ? td1.pkeys : Object.keys(_rows1[i])
        // / cycle all rows of second table
        for (j = 0; j < _rows2.length; j++) {
          // / check if there are at least a different column value
          var _sameColumnValues = true
          for (var k = 0; k < _columns.length; k++) {
            var _columnName = _columns[k]
            if (_rows1[i][_columnName] !== _rows2[j][_columnName]) {
              _sameColumnValues = false
              break
            }
          }
          if (_sameColumnValues) {
            _matchRows = true
            break
          }
        }
        if (_matchRows) {
          // / if checked columns are less than total columns, check other columns value
          // / if they are all the same do nothing, else delete old row and insert a new one
          if (_columns.length < Object.keys(_rows1[i]).length) {
            // / need to check each column value to search if there is a different column value
            var _isChanged = false
            for (var k in _rows1[i]) {
              // / compare jsonstringify to include each type comparation
              if (JSON.stringify(_rows1[i][k])!==JSON.stringify(_rows2[i][k])) {
                _isChanged = true
                break
              }
            }
            if (_isChanged) {
              // / there are some different column values
              // / make a delete+insert query
              var _whereSql = []
              for (var k = 0; k < td1.pkeys.length; k++) {
                var _keyColumn = td1.pkeys[k]
                _whereSql.push(_keyColumn + ' = ' + _formatSqlValue(_rows1[i][_keyColumn]))
              }
              var _sql = 'DELETE FROM ' + tablename +
              ' WHERE ' + _whereSql.join(' AND ')
              _deleteQueries.push(_sql)
              
              _insertQueries.push(_generateInsertQuery(tablename, _rows1[i]))
            }
          }
        } else {
          // / generate a new insert query
          _insertQueries.push(_generateInsertQuery(tablename, _rows1[i]))
        }
      }
      return {
        inserting: _insertQueries,
        deleting: _deleteQueries
      }
    }
    
    /**
     * Check rows of both tables and generate necessary delete queries for table 2
     * @private
     * @function
     * @param {string} tablename Name of the table where deleting
     * @param {rows: {object}, pkeys: {Array.<string>}} td1 Data (primary keys and rows) for table of schema 1
     * @param {rows: {object}, pkeys: {Array.<string>}} td2 Data (primary keys and rows) for table of schema 2
     * @returns {Array.<string>}
     */
    var _retrieveDeleteRows = function (tablename, td1, td2) {
      var _rows1 = td1.rows
      var _rows2 = td2.rows

      var _deleteQueries = []
      // / check every single row in schema 1, analyze differences with other schema rows on pkeys columns
      // / or, if no pkeys are present, analyze all columns
      // / if this columns values doesn't match in schema 1, make a delete
      // / if it matches leave it (it could be involved by a previous query)
      for (var i = 0; i < _rows2.length; i++) {
        // / keep a reference if there are 2 rows with same pkeys values or, if there aren't pkeys, same column values
        var _matchRows = false
        var j
        // / columns values to check (primary keys or all columns)
        var _columns = td1.pkeys.length > 0 ? td1.pkeys : Object.keys(_rows2[i])
        // / cycle all rows of second table
        for (j = 0; j < _rows1.length; j++) {
          // / check if there are at least a different column value
          var _sameColumnValues = true
          for (var k = 0; k < _columns.length; k++) {
            var _columnName = _columns[k]
            if (_rows2[i][_columnName] !== _rows1[j][_columnName]) {
              _sameColumnValues = false
              break
            }
          }
          if (_sameColumnValues) {
            _matchRows = true
            break
          }
        }
        // / columns values are not all the same -> delete this row
        if (!_matchRows) {
          // / generate a new delete query
          var _sql = 'DELETE FROM ' + tablename + ' WHERE '
          var _whereSql = []
          for (var k = 0; k < _columns.length; k++) {
            var _columnName = _columns[k]
            _whereSql.push(_columnName + ' = ' + _formatSqlValue(_rows2[i][_columnName]))
          }
          _sql += _whereSql.join(' AND ')
          _deleteQueries.push(_sql)
        }
      }
      return _deleteQueries
    }
    
    // / keep a reference about queries to execute on each table to disable/enable constraints
    var _sqlDisableConstraints = []
    var _sqlEnableConstraints = []
    // / cycle each given table in schema 1
    for (var i in _tabledata1) {
      if (!_tabledata1[i].exists) {
        log.verbose('error', '/* WARNING: table ' + i + " doesn't exist in schema 1*/")
        _sqls.push('/* WARNING: table ' + i + " doesn't exist in schema 1*/")
        continue
      }
      if (!_tabledata1[i].valid) {
        log.verbose('error', '/* WARNING: table ' + i + " in schema 1 has too many rows*/")
        _sqls.push('/* WARNING: table ' + i + " in schema 1 has too many rows*/")
        continue
      }
      // / disable constraints at the beginning of queries insert/update
      _sqlDisableConstraints.push('ALTER TABLE ' + i + ' DISABLE trigger ALL')
      // / enable constraints at the end of queries insert/update
      _sqlEnableConstraints.push('ALTER TABLE ' + i + ' ENABLE trigger ALL')
      var _queries = _retrieveInsertRows(i, _tabledata1[i], _tabledata2[i])
      // / make delete queries first
      _sqls = _sqls.concat(_queries.deleting)
      _sqls = _sqls.concat(_queries.inserting)
    }
    // / if options mode is full, check if there are rows in schema 2 not present in schema 1
    // / drop this rows
    if (options.mode === 'full') {
      // / cycle each given table
      for (var i in _tabledata1) {
        if (_tabledata1[i].exists && _tabledata1[i].valid) {
          _sqls = _retrieveDeleteRows(i, _tabledata1[i], _tabledata2[i]).concat(_sqls)
        }
      }
    }
    if (_sqls.length > 0) {
      // / generate final sql in right order -> disable constraints, insert/delete queries, enable constraints
      _sqls = _sqlDisableConstraints.concat(_sqls)
      _sqls = _sqls.concat(_sqlEnableConstraints)
      __delta.rows.push(_sqls.join(';\n') + ';')
    } else {
      __delta.rows.push('--rows match')
    }
  }

  var __main = function () {
    log.verbose('log', '... delta')
    // / compare all schema data
    __compareTables()
    __comparePkeys()
    __compareUniqueConstraints()
    __compareFkeys()
    __compareFunctions()
    __compareIndexes()
    __compareTypes()
    __compareViews()
    __setSequencesValues()
    __compareRows()

    // / retrieve delta sql
    __sql =
      'BEGIN;' + 
      '\n\n-- TYPES \n\n' +
      __delta.types.join('\n') +
      '\n\n-- FUNCTIONS \n\n' +
      __delta.functions.join('\n') +
      '\n\n-- TABLES AND COLUMNS \n\n' +
      __delta.tables.join('\n') +
      '\n\n-- PRIMARY KEYS \n\n' +
      __delta.pkeys.join('\n') +
      '\n\n-- FOREIGN KEYS \n\n' +
      __delta.fkeys.join('\n') +
      '\n\n-- INDEXES \n\n' +
      __delta.indexes.join('\n') +
      '\n\n-- VIEWS \n\n' +
      __delta.views.join('\n') +
      '\n\n-- SEQUENCES \n\n' +
      __delta.sequences.join('\n') +
      '\n\n-- ROWS \n\n' +
      __delta.rows.join('\n') + 
      '\n\n\nCOMMIT;'
  }

  __main()

  return {
    sql: function () {
      return __sql
    },
    getDeltaDifference: function (key) {
      if (!__delta) {
        return null
      }
      return __delta[key]
    }
  }
}

module.exports = Delta
