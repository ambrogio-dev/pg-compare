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
  if (!options.mode || options.mode != 'full') {
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
    sequences: []
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
   * @retuns {string} sql
   */
  var __sqlColumn = function (column) {
    var _sql = column.name + ' ' + column.type +
    (column.nullable ? '' : ' NOT NULL')
    // / check if column has a default value
    if (column.default && column.default != '$sequence') {
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
   * @retuns {Array} values
   */
  var __sqlEnumValues = function (values) {
    return values.split(',').map(function (v) {
      return "'" + v + "'"
    }).join(',')
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
    // / cycle each table of schema 1 to compare its data to schema 2
    for (var t in _tables1) {
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
        for (var c1 in _columns1) {
          var _columnName1 = _columns1[c1].name
          // / check if column exists on table 2
          var _exists = false
          for (var c2 in _columns2) {
            var _columnName2 = _columns2[c2].name
            // / column exists
            if (_columnName1 == _columnName2) {
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
            if (_columns1[c1].type != _columns2[c2].type) {
              _sqlColumns.push(_sqlEdit + ' SET DATA TYPE ' + _columns1[c1].type + ';')
            }
            // / check column default value
            if (_columns1[c1].default != _columns2[c2].default) {
              _sqlColumns.push(_sqlEdit + (_columns1[c1].default ? ' SET DEFAULT ' + _columns1[c1].default : ' DROP DEFAULT ') + ';')
            }
            // / check column nullable
            if (_columns1[c1].nullable != _columns2[c2].nullable) {
              if (!_columns1[c1].nullable && !_columns1[c1].default) {
                log.verbose('error', 'column ' + _columns1[c1].name + ' of table ' + _tables1[t] + ' is not null but without default value')
                _sqlColumns.push('/* WARNING: column ' + _columns1[c1].name + ' of table ' + _tables1[t] + ' is not null but without default value*/')
              }
              _sqlColumns.push(_sqlEdit + (_columns1[c1].nullable ? ' DROP' : ' SET') + ' NOT NULL' + ';')
            }
          }
          // / check column comment
          if ((!_exists && _columns1[c1].comment) || _columns1[c1].comment != _columns2[c2].comment) {
            _sqlColumns.push('COMMENT ON COLUMN ' + _tables1[t] + '.' + _columns1[c1].name + ' IS ' +
              (_columns1[c1].comment ? "'" + _columns1[c1].comment + "'" : 'NULL') + ';')
          }
        }
        // / if comments on the table are different, modify it
        if (schema1.tables[_tables1[t]].comment != schema2.tables[_tables1[t]].comment) {
          _sqlColumns.push('COMMENT ON TABLE ' + _tables1[t] + ' IS ' +
            (schema1.tables[_tables1[t]].comment ? "'" + schema1.tables[_tables1[t]].comment + "'" : 'NULL') + ';')
        }
        // / if option mode is full, check if column from table 2 missing from column table 1 -> drop
        if (options.mode == 'full') {
          for (var c2 in _columns2) {
            var _columnName2 = _columns2[c2].name
            // / check if column exists
            var _exists = false
            for (var c1 in _columns1) {
              var _columnName1 = _columns1[c1].name
              // / column exists
              if (_columnName1 == _columnName2) {
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
              "'" + _columns1[c].comment + "'")
          }
        }
        _sqlTable.push(_sqlColumns.join(','), ');')
        // / add table owner
        _sqlTable.push('ALTER TABLE ' + _tables1[t] + ' OWNER TO ' + options.owner + ';')
        // / add comment if present
        if (schema1.tables[_tables1[t]].comment) {
          _sqlTable.push('COMMENT ON TABLE ' + _tables1[t] + " IS '" + schema1.tables[_tables1[t]].comment + "';")
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
    if (options.mode == 'full') {
      for (var t in _tables2) {
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
    // / cycle each primary key of schema 1 to check if exists in schema 2
    for (var i in _pkeys1) {
      var _pk1 = _pkeys1[i]
      var _pkName1 = _pk1.name
      // / check if pkey exists in schema 2
      var _exists = false
      for (var j in _pkeys2) {
        var _pkName2 = _pkeys2[j].name
        // / pkey exists
        if (_pkName1 == _pkName2) {
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
        if (_pk1.tablename != _pkeys2[j].tablename || _pk1.columns.sort().join(',') != _pkeys2[j].columns.sort().join(',')) {
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
    if (options.mode == 'full') {
      for (var i in _pkeys2) {
        var _pkey2 = _pkeys2[i]
        var _pkeyName2 = _pkey2.name
        // / check if pkey exists
        var _exists = false
        for (var j in _pkeys1) {
          var _pkey1 = _pkeys1[j]
          var _pkeyName1 = _pkey1.name
          // / pkey exists
          if (_pkeyName1 == _pkeyName2) {
            _exists = true
            break
          }
        }
        // / if foreign key is not in schema 1, drop it
        if (!_exists) {
          _sqls.push('ALTER TABLE ' + _pkey2.tablename + '\n' +
            '  DROP CONSTRAINT ' + _pkeyName2)
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
    // / cycle each unique constraint of schema 1 to check if exists in schema 2
    for (var i in _unique1) {
      var _un1 = _unique1[i]
      var _unName1 = _un1.name
      // / check if unique constraint exists in schema 2
      var _exists = false
      for (var j in _unique2) {
        var _unName2 = _unique2[j].name
        // / unique constraint exists
        if (_unName1 == _unName2) {
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
        if (_un1.tablename != _unique2[j].tablename || _un1.columns.sort().join(',') != _unique2[j].columns.sort().join(',')) {
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
    if (options.mode == 'full') {
      for (var i in _unique2) {
        var _un2 = _unique2[i]
        var _unName2 = _un2.name
        // / check if unique exists
        var _exists = false
        for (var j in _unique1) {
          var _un1 = _unique1[j]
          var _unName1 = _un1.name
          // / unique exists
          if (_unName1 == _unName2) {
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
    // / cycle each foreign key of schema 1 to check if exists in schema 2
    for (var i in _fkeys1) {
      var _fk1 = _fkeys1[i]
      var _fkName1 = _fk1.name
      // / check if fkey exists in schema 2
      var _exists = false
      for (var j in _fkeys2) {
        var _fkName2 = _fkeys2[j].name
        // / fkey exists
        if (_fkName1 == _fkName2) {
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
        if (_fk1.target_table != _fkeys2[j].target_table ||
          _fk1.target_columns.join(',') != _fkeys2[j].target_columns.join(',') ||
          _fk1.source_table != _fkeys2[j].source_table ||
          _fk1.source_columns.join(',') != _fkeys2[j].source_columns.join(',') ||
          _fk1.match != _fkeys2[j].match ||
          _fk1.on_update != _fkeys2[j].on_update ||
          _fk1.on_delete != _fkeys2[j].on_delete) {
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
    if (options.mode == 'full') {
      for (var i in _fkeys2) {
        var _fkey2 = _fkeys2[i]
        var _fkeyName2 = _fkey2.name
        // / check if fkey exists
        var _exists = false
        for (var j in _fkeys1) {
          var _fkey1 = _fkeys1[j]
          var _fkeyName1 = _fkey1.name
          // / fkey exists
          if (_fkeyName1 == _fkeyName2) {
            _exists = true
            break
          }
        }
        // / if foreign key is not in schema 1, drop it
        if (!_exists) {
          _sqls.push('ALTER TABLE ' + _fkey2.target_table + '\n' +
            '  DROP CONSTRAINT ' + _fkeyName2)
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
    // / cycle each function of schema 1 to check if exists in schema 2
    for (var i in _functions1) {
      var _f1 = _functions1[i]
      // / check if function exists
      var _exists = false
      for (var j in _functions2) {
        var _f2 = _functions2[j]
        // / if function exists
        if (_f1.name == _f2.name) {
          // / if function source is different, replace it
          if (_f1.source != _f2.source || _f1.args != _f2.args) {
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
    if (options.mode == 'full') {
      for (var i in _functions2) {
        var _f2 = _functions2[i]
        // / check if function exists
        var _exists = false
        for (var j in _functions1) {
          var _f1 = _functions1[j]
          // / function exists
          if (_f1.name == _f2.name) {
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
    // / cycle each index of schema 1 to check if exists in schema 2
    for (var i in _indexes1) {
      var _index1 = _indexes1[i]
      // / check if index exists
      var _exists = false
      for (var j in _indexes2) {
        var _index2 = _indexes2[j]
        // / if index exists
        if (_index1.name == _index2.name) {
          // / if index source is different, replace it
          if (_index1.source != _index2.source) {
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
    if (options.mode == 'full') {
      for (var i in _indexes2) {
        var _index2 = _indexes2[i]
        // / check if index exists
        var _exists = false
        for (var j in _indexes1) {
          var _index1 = _indexes1[j]
          // / index exists
          if (_index1.name == _index2.name) {
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
    // / cycle each type of schema 1 to check if exists in schema 2
    for (var i in _types1) {
      var _type1 = _types1[i]
      var _values1 = _type1.values.split(',')
      // / check if type exists
      var _exists = false
      for (var j in _types2) {
        var _type2 = _types2[j]
        // / if type exists
        if (_type1.name == _type2.name) {
          // / if type source is different, replace it

          if (_type1.values != _type2.values) {
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
    if (options.mode == 'full') {
      for (var i in _types2) {
        var _type2 = _types2[i]
        // / check if type exists
        var _exists = false
        for (var j in _types1) {
          var _type1 = _types1[j]
          // / type exists
          if (_type1.name == _type2.name) {
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
    // / cycle each view of schema 1 to check if exists in schema 2
    for (var i in _views1) {
      var _view1 = _views1[i]
      // / check if view exists
      var _exists = false
      for (var j in _views2) {
        var _view2 = _views2[j]
        // / if view exists
        if (_view1.name == _view2.name) {
          // / if view source is different, replace it
          if (_view1.source != _view2.source) {
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
    if (options.mode == 'full') {
      for (var i in _views2) {
        var _view2 = _views2[i]
        // / check if view exists
        var _exists = false
        for (var j in _views1) {
          var _view1 = _views1[j]
          // / view exists
          if (_view1.name == _view2.name) {
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
        if (_seq1.name == _seq2.name) {
          _exists = true
          break
        }
      }
      // / check if both sequences have same current value
      if (_exists && _seq1.currval != _seq2.currval) {
        _sqls.push("SELECT setval('" + _seq1.name + "', " + _seq1.currval + ')')
      }
    }

    if (_sqls.length > 0) {
      __delta.sequences.push(_sqls.join(';\n') + ';')
    } else {
      __delta.sequences.push('--sequences match')
    }
  }

  var __main = (function () {
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

    // / retrieve delta sql
    __sql =
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
      __delta.sequences.join('\n')
  }())
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
