#!/usr/bin/env node

/**
 * TODO DDL
 * 
 * table
 *   all constraints (pkeys, unique, check etc)
 *   comment?
 * column
 *   comment
 *   compare type, nullable, pkey, default
 *   
 * TODO DATA
 *   sync (NB config select tables)
 *   sequences (set current value)
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

// @todo args.indexOf(-v)
// @todo mode full|preserve
// @todo file output

/// *** get args
//var _source = process.argv[2];
//var _output = process.argv[3];
var _verbose = process.argv.indexOf('-v') != -1;
/// check args
if (false) {
	console.error('use: ' + process.argv[0] + ' ... [-v]');
	process.end(-1);
}

var fs = require('fs');
var pg = require('pg');
var tools = require('a-toolbox');
/**
 * output management
 * @param {...} args any arguments 
 */
var verbose = function () {
	if (!_verbose)
		return;
	var _args = Array.prototype.slice.call(arguments);
	var _mode = _args.shift();
	if (_mode == 'error')
		console.error.apply(console, _args);
	else
		console.log.apply(console, _args);
};
/**
 * 
 * @param {DB} db
 * @param {object} options
 *   tables: true, 
 *   fkeys: true 
 *   @todo
 *   functions: true, 
 *   indexes: true, 
 *   types: true
 *   views: true
 */
var Schema = function (db, options) {

	var __data = {
		tables: {},
		fkeys: {},
		indexes: {},
		functions: {},
		types: {},
		views: {}
	};
	var load = function (callback) {
		var _tasks = new tools.tasks(function () {
			verbose('log', 'load done');
			callback();
		});
		/// declare tasks
		if (options.tables)
			_tasks.todo('tables');
		if (options.fkeys)
			_tasks.todo('fkeys');
		if (options.indexes)
			_tasks.todo('indexes');
		if (options.functions)
			_tasks.todo('functions');
		if (options.types)
			_tasks.todo('types');
		if (options.views)
			_tasks.todo('views');
		if (options.tables) {
			var _jobColumns = new tools.tasks(function () {
				console.log('done tables');
				_tasks.done('tables');
			});
			getTables(function (tables) {
				// declare columns tasks
				for (var i in tables) {
					var _table = tables[i];
					_jobColumns.todo('table#columns#' + _table);
					//_tasks.todo('table#count#' + _table);
				}

				for (var i in tables) {
					var _table = tables[i];
					__data.tables[_table] = {
						name: _table,
						fkeys: []
					};
					var _f = function (table) {
						getColumns(table, function (columns) {
							__data.tables[table].columns = columns;
							_jobColumns.done('table#columns#' + table);
						}, function (err) {
							// getColumns error
							_jobColumns.done('table#columns#' + table);
						});
						/*
						 countRows(table, function (count) {
						 __data.tables[table].count = count;
						 _tasks.done('table#count#' + table);
						 verbose('log','loading count', table, ' done');
						 }, function (err) {
						 // __count error
						 verbose('error','Error loading data, table', table, err);
						 });
						 */
					}(_table);
				}
			}, function (err) {
				// getTables error
			});
		}

		if (options.fkeys) {
			getFkeys(function (fkeys) {
				__data.fkeys = fkeys;
				/* links collected for each tables
				 (var i in fkeys) {
				 __data.tables[fkeys[i].source_table].fkeys.push({
				 name: fkeys[i].name,
				 from: fkeys[i].target_table,
				 match: fkeys[i].match == 'FULL' ? 'FULL' : 'SIMPLE'
				 });
				 }*/

				_tasks.done('fkeys');
			}, function (err) {
				_tasks.done('fkeys');
			});
		}

		if (options.indexes) {
			getIndexes(function (indexes) {
				__data.indexes = indexes;
				_tasks.done('indexes');
			}, function (err) {
				_tasks.done('indexes');
			});
		}

		if (options.functions) {
			getFunctions(function (functions) {
				__data.functions = functions;
				_tasks.done('functions');
			}, function (err) {
				_tasks.done('functions');
			});
		}

		if (options.types) {
			getTypes(function (types) {
				__data.types = types;
				_tasks.done('types');
			}, function (err) {
				_tasks.done('types');
			});
		}

		if (options.views) {
			getViews(function (views) {
				__data.views = views;
				_tasks.done('views');
			}, function (err) {
				_tasks.done('views');
			});
		}

	};
	var getTables = function (success, error) {
		verbose('log', 'loading tables ...');
		db.query(
			'SELECT tablename AS name ' +
			'FROM pg_tables ' +
			'WHERE schemaname = $1',
			['public'],
			function (err, result) {
				if (err) {
					verbose('log', 'loading tables error', err);
					error();
					return;
				}
				var _tables = [];
				for (var i in result.rows)
					_tables.push(result.rows[i].name);
				verbose('log', 'loading tables success');
				success(_tables);
			});
	};
	var getColumns = function (table, success, error) {
		verbose('log', 'loading colums', table, ' ...');
		db.query('SELECT ' +
			'a.attname                            AS name, ' +
			'format_type(a.atttypid, a.atttypmod) AS type, ' +
			'NOT a.attnotnull                     AS nullable, ' +
			'COALESCE(p.indisprimary, FALSE)      AS pkey, ' +
			'f.adsrc                              AS default, ' +
			'd.description                        AS comment ' +
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
					verbose('error', 'loading colums', table, ' success');
					error();
					return;
				}
				for (var i in result.rows) {
					if (result.rows[i].type.indexOf('character varying') != -1) {
						result.rows[i].type = result.rows[i].type.replace('character varying', 'varchar');
					}
					// serial
					if (result.rows[i].default && result.rows[i].default.indexOf('nextval') != -1) {
						result.rows[i].default = '$sequence';
						result.rows[i].type = result.rows[i].type == 'bigint' ? 'bigserial' : 'serial';
						result.rows[i].sequence = result.rows[i].default;
					}
				}
				verbose('log', 'loading colums', table, ' success');
				success(result.rows);
			}
		);
	};
	var getFkeys = function (success, error) {
		verbose('log', 'loading fkeys ...');
		db.query('SELECT ' +
			'CASE c.match_option WHEN \'FULL\' THEN \'FULL\' ELSE \'SIMPLE\' END AS match, ' +
			'c.constraint_name AS name, ' +
			//'x.table_schema AS schema_name, ' +
			'x.table_name AS target_table,' +
			'x.column_name AS target_column, ' +
			//'y.table_schema AS foreign_schema_name, ' +
			'y.table_name AS source_table, ' +
			'y.column_name AS source_column, ' +
			'c.update_rule AS on_update, ' +
			'c.delete_rule AS on_delete ' +
			'FROM information_schema.referential_constraints c ' +
			'INNER JOIN information_schema.key_column_usage x ON x.constraint_name = c.constraint_name ' +
			'INNER JOIN information_schema.key_column_usage y ON y.ordinal_position = x.position_in_unique_constraint AND y.constraint_name = c.unique_constraint_name ' +
			'ORDER BY c.constraint_name, x.ordinal_position',
			[],
			function (err, result) {
				if (err) {
					verbose('log', 'loading fkeys error', err);
					error();
					return;
				}
				verbose('log', 'loading fkeys success');
				success(result.rows);
			}
		);
	};
	var getFunctions = function (success, error) {
		verbose('log', 'loading functions ...');
		db.query('SELECT routine_name AS name, ' +
			//'proargnames AS args, ' +
			'pg_get_functiondef(routine_name::regproc) AS source ' +
			'FROM information_schema.routines ' +
			//'INNER JOIN pg_proc ON routine_name = proname ' +
			'WHERE specific_schema = $1',
			['public'],
			function (err, result) {
				if (err) {
					verbose('log', 'loading functions error', err);
					error();
					return;
				}
				verbose('log', 'loading functions success');
				for (var i in result.rows) {
					var _args = result.rows[i].source.match(/FUNCTION.+\(([\w\s\,]*)\)/);
					result.rows[i].args = _args ? _args[1] : '';
				}
				success(result.rows);
			}
		);
	};
	var getIndexes = function (success, error) {
		verbose('log', 'loading indexes ...');
		db.query('SELECT ' +
			// n.nspname AS schema, 
			't.relname  AS table, ' +
			'c.relname  AS name, ' +
			'pg_get_indexdef(indexrelid) AS source ' +
			'FROM pg_catalog.pg_class c ' +
			'JOIN pg_catalog.pg_namespace n ON n.oid    = c.relnamespace ' +
			'JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid ' +
			'JOIN pg_catalog.pg_class t ON i.indrelid   = t.oid ' +
			'WHERE c.relkind = \'i\' ' +
			'AND n.nspname NOT IN (\'pg_catalog\', \'pg_toast\') ' +
			'AND pg_catalog.pg_table_is_visible(c.oid) ' +
			'ORDER BY n.nspname, t.relname, c.relname',
			[],
			function (err, result) {
				if (err) {
					verbose('log', 'loading indexes error', err);
					error();
					return;
				}
				verbose('log', 'loading indexes success');
				success(result.rows);
			}
		);
	};
	var getTypes = function (success, error) {
		verbose('log', 'loading types ...');
		db.query('SELECT ' +
			//'n.nspname AS schema, ' +
			't.typname AS name, ' +
			'string_agg(e.enumlabel, \',\') AS values ' +
			'FROM pg_type t ' +
			'JOIN pg_enum e ON t.oid = e.enumtypid  ' +
			'JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace ' +
			'WHERE nspname = $1 ' +
			'GROUP BY n.nspname, name',
			['public'],
			function (err, result) {
				if (err) {
					verbose('log', 'loading types error', err);
					error();
					return;
				}
				verbose('log', 'loading types success');
				success(result.rows);
			}
		);
	};
	var getViews = function (success, error) {
		verbose('log', 'loading views ...');
		db.query('SELECT viewname AS name, ' +
			'definition AS source ' +
			// schemaname, viewowner
			'FROM pg_views ' +
			'WHERE schemaname = $1',
			['public'],
			function (err, result) {
				if (err) {
					verbose('log', 'loading views error', err);
					error();
					return;
				}
				verbose('log', 'loading views success');
				success(result.rows);
			}
		);
	};
	/* TODO
	 var countRow = function (table, success, error) {
	 db.query({
	 sql: 'SELECT COUNT(*) AS n FROM ' + table,
	 success: function (result) {
	 success(parseInt(result.rows[0].n));
	 },
	 error: error
	 });
	 };
	 */
	return  {
		getTables: getTables,
		getColumns: getColumns,
		getFkeys: getFkeys,
		getIndexes: getIndexes,
		getFunctions: getFunctions,
		getTypes: getTypes,
		getViews: getViews,
		load: load,
		data: function () {
			return __data;
		}
	};
};
/**
 * produce sql delta from schema1 to schema2
 * means schema1 = scherma2 + delta
 * @param {object} schema1
 * @param {object} schema2
 * @param {object} options
 * @param {string} [options.mode=preserve] full or preserve (do not drop if somthing missing from schema1 to schema2)
 * @param {string} [options.owner=postgres]
 */
var Delta = function (schema1, schema2, options) {
	if (!options)
		options = {};
	if (!options.mode || options.mode != 'full')
		options.mode = 'preserve';
	if (!options.owner)
		options.owner = 'postgres';
	var __delta = {
		tables: [],
		fkeys: [],
		functions: [],
		indexes: [],
		types: [],
		views: []
	};
	var __sql = '';
	var __sqlColumn = function (column) {
		var _sql = column.name + ' ' + column.type +
			(column.nullable ? '' : ' NOT NULL');
		if (column.default && column.default != '$sequence')
			_sql += ' DEFAULT ' + column.default;
		if (column.pkey)
			_sql += ' PRIMARY KEY';
		return _sql;
		// @todo (column.comment ? ' -- ' + column.comment : '')
	};
	var __sqlEnumValues = function (values) {
		return values.split(',').map(function (v) {
			return "'" + v + "'";
		}).join(',');
	};
	var __compareTables = function () {
		var _tables1 = amb.object.getKeys(schema1.tables);
		var _tables2 = amb.object.getKeys(schema2.tables);
		for (var t in _tables1) {
			var _sqlTable = [];
			var _sqlColumns = [];
			var _columns1 = schema1.tables[_tables1[t]].columns;
			if (amb.array.contains(_tables2, _tables1[t])) {
				var _columns2 = schema2.tables[_tables1[t]].columns;
				// alter table

				// column from table 1 missing or different to column table 2 -> add
				for (var c1 in _columns1) {
					//verbose('log',_columns[c]);
					var _columnName1 = _columns1[c1].name;
					// check if column exists
					var _exists = false;
					for (var c2 in _columns2) {
						var _columnName2 = _columns2[c2].name;
						// column exists
						if (_columnName1 == _columnName2) {
							_exists = true;
							break;
						}
					}
					// if column1 is not in table2, add
					if (!_exists) {
						_sqlColumns.push('ALTER TABLE ' + _tables1[t] +
							' ADD COLUMN ' + __sqlColumn(_columns1[c1]) + ';');
					} else {
						// @todo check column type default nullable etc for do ALTER COLUMN
					}
				}
				// column from table 2 missing from column table 1 -> drop
				if (options.mode == 'full') {
					for (var c2 in _columns2) {
						//verbose('log',_columns[c]);
						var _columnName2 = _columns2[c2].name;
						// check if column exists
						var _exists = false;
						for (var c1 in _columns1) {
							var _columnName1 = _columns1[c1].name;
							// column exists
							if (_columnName1 == _columnName2) {
								_exists = true;
								break;
							}
						}
						// if column2 is not in table1, drop
						if (!_exists) {
							_sqlColumns.push('ALTER TABLE ' + _tables1[t] +
								' DROP COLUMN ' + _columnName2 + ';');
						}
					}
				}
				_sqlTable.push(_sqlColumns.join('\n'));
			} else {
				// create table
				_sqlTable.push('CREATE TABLE ' + _tables1[t] + ' (');
				for (var c in _columns1) {
					//verbose('log',_columns[c]);
					_sqlColumns.push('  ' + __sqlColumn(_columns1[c]));
				}
				_sqlTable.push(_sqlColumns.join(',\n'), ');');
				// owner
				_sqlTable.push('ALTER TABLE ' + _tables1[t] + ' OWNER TO ' + options.owner + ';');
			}
			if (_sqlColumns.length > 0) {
				__delta.tables.push(_sqlTable.join('\n'), '\n');
			}
		}
		if (options.mode == 'full') {
			/// drop tables
			for (var t in _tables2) {
				if (!amb.array.contains(_tables1, _tables2[t]))
					__delta.tables.push('DROP TABLE ' + _tables2[t] + ';');
			}
		}
	};
	var __compareFkeys = function () {
		var _fkeys1 = schema1.fkeys;
		var _fkeys2 = schema2.fkeys;
		var _sqls = [];
		/// add missing fkeys from schema1 to schema2
		for (var i in _fkeys1) {
			var _fk1 = _fkeys1[i];
			var _fkName1 = _fk1.name;
			/// check if fkey exists
			var _exists = false;
			for (var j in _fkeys2) {
				var _fkName2 = _fkeys2[j].name;
				/// fkey exists
				if (_fkName1 == _fkName2) {
					_exists = true;
					break;
				}
			}
			/// if fkey1 miss, add
			if (!_exists) {
				_sqls.push('ALTER TABLE ' + _fk1.target_table + '\n' +
					'  ADD CONSTRAINT ' + _fkName1 + ' FOREIGN KEY (' + _fk1.target_column + ')\n' +
					'  REFERENCES ' + _fk1.source_table + ' (' + _fk1.source_column + ')' + ' MATCH ' + _fk1.match + '\n' +
					'  ON UPDATE ' + _fk1.on_update + ' ON DELETE ' + _fk1.on_delete);
			} else {
				/// @todo check fkey attrs: cascade etc
			}
		}
		/// fkey from table 2 missing from fkey table 1 -> drop
		if (options.mode == 'full') {
			for (var i in _fkeys2) {
				var _fkey2 = _fkeys2[i];
				var _fkeyName2 = _fkey2.name;
				/// check if fkey exists
				var _exists = false;
				for (var j in _fkeys1) {
					var _fkey1 = _fkeys1[j];
					var _fkeyName1 = _fkey1.name;
					/// fkey exists
					if (_fkeyName1 == _fkeyName2) {
						_exists = true;
						break;
					}
				}
				/// if fkey2 is not in fkeys1, drop
				if (!_exists) {
					_sqls.push('ALTER TABLE ' + _fkey2.target_table + '\n' +
						'  DROP CONSTRAINT ' + _fkeyName2);
				}
			}
		}

		if (_sqls.length > 0)
			__delta.fkeys.push(_sqls.join(';\n') + ';');
		else
			__delta.fkeys.push('--foreign keys match');
	};
	var __compareFunctions = function () {
		var _functions1 = schema1.functions;
		var _functions2 = schema2.functions;
		var _sqls = [];
		/// add missing functions from schema1 to schema2
		for (var i in _functions1) {
			var _f1 = _functions1[i];
			/// check if function exists
			var _exists = false;
			for (var j in _functions2) {
				var _f2 = _functions2[j];
				/// if function exists
				if (_f1.name == _f2.name) {
					/// if function source is different, replace
					if (_f1.source != _f2.source || _f1.args != _f2.args)
						_sqls.push(_f1.source);
					_exists = true;
					break;
				}
			}
			/// if function1 miss in schema2, add
			if (!_exists) {
				_sqls.push(_f1.source);
			}
		}
		/// function from schema 2 missing from function table 1 -> drop
		if (options.mode == 'full') {
			for (var i in _functions2) {
				var _f2 = _functions2[i];
				/// check if function exists
				var _exists = false;
				for (var j in _functions1) {
					var _f1 = _functions1[j];
					/// function exists
					if (_f1.name == _f2.name) {
						_exists = true;
						break;
					}
				}
				/// if function2 is not in schema1, drop
				if (!_exists)
					_sqls.push('DROP FUNCTION ' + _f2.name + '(' + _f2.args + ')');
			}
		}

		if (_sqls.length > 0)
			__delta.functions.push(_sqls.join(';\n') + ';');
		else
			__delta.functions.push('--functions match');
	};
	var __compareIndexes = function () {
		var _indexes1 = schema1.indexes;
		var _indexes2 = schema2.indexes;
		var _sqls = [];
		/// add missing indexes from schema1 to schema2
		for (var i in _indexes1) {
			var _index1 = _indexes1[i];
			/// check if index exists
			var _exists = false;
			for (var j in _indexes2) {
				var _index2 = _indexes2[j];
				/// if index exists
				if (_index1.name == _index2.name) {
					/// if index source is different, replace
					if (_index1.source != _index2.source)
						_sqls.push('DROP INDEX ' + _index1.name + '; ' + _index1.source);
					_exists = true;
					break;
				}
			}
			/// if index1 miss in schema2, add
			if (!_exists) {
				_sqls.push(_index1.source);
			}
		}
		/// index from schema 2 missing from index table 1 -> drop
		if (options.mode == 'full') {
			for (var i in _indexes2) {
				var _index2 = _indexes2[i];
				/// check if index exists
				var _exists = false;
				for (var j in _indexes1) {
					var _index1 = _indexes1[j];
					/// index exists
					if (_index1.name == _index2.name) {
						_exists = true;
						break;
					}
				}
				/// if index2 is not in schema1, drop
				if (!_exists)
					_sqls.push('DROP INDEX ' + _index2.name);
			}
		}

		if (_sqls.length > 0)
			__delta.indexes.push(_sqls.join(';\n') + ';');
		else
			__delta.indexes.push('--indexes match');
	};
	var __compareTypes = function () {
		var _types1 = schema1.types;
		var _types2 = schema2.types;
		var _sqls = [];
		/// add missing types from schema1 to schema2
		for (var i in _types1) {
			var _type1 = _types1[i];
			var _values1 = _type1.values.split(',');
			/// check if type exists
			var _exists = false;
			for (var j in _types2) {
				var _type2 = _types2[j];
				/// if type exists
				if (_type1.name == _type2.name) {
					/// if type source is different, replace

					// ALTER TYPE grantrole ADD VALUE 'AAA' BEFORE 'ADMIN';
					if (_type1.values != _type2.values) {
						var _values2 = _type2.values.split(',');
						var _last = amb.array.last(_values2);
						// @todo detect right "afeter", not just the last one
						for (var k in _values1) {
							if (!amb.array.contains(_values2, _values1[k]))
								_sqls.push('ALTER TYPE ' + _type1.name + ' ADD VALUE \'' + _values1[k] + '\' AFTER \'' + _last + '\'');
						}
					}
					_exists = true;
					break;
				}
			}
			/// if type1 miss in schema2, add
			if (!_exists) {
				_sqls.push('CREATE TYPE ' + _type1.name + ' AS ENUM (' + __sqlEnumValues(_type1.values) + ')');
			}
		}
		/// type from schema 2 missing from type table 1 -> drop
		if (options.mode == 'full') {
			for (var i in _types2) {
				var _type2 = _types2[i];
				/// check if type exists
				var _exists = false;
				for (var j in _types1) {
					var _type1 = _types1[j];
					/// type exists
					if (_type1.name == _type2.name) {
						_exists = true;
						break;
					}
				}
				/// if type2 is not in schema1, drop
				if (!_exists)
					_sqls.push('DROP TYPE ' + _type2.name);
			}
		}

		if (_sqls.length > 0)
			__delta.types.push(_sqls.join(';\n') + ';');
		else
			__delta.types.push('--types match');
	};
	var __compareViews = function () {
		var _views1 = schema1.views;
		var _views2 = schema2.views;
		var _sqls = [];
		/// add missing views from schema1 to schema2
		for (var i in _views1) {
			var _view1 = _views1[i];
			/// check if view exists
			var _exists = false;
			for (var j in _views2) {
				var _view2 = _views2[j];
				/// if view exists
				if (_view1.name == _view2.name) {
					/// if view source is different, replace
					if (_view1.source != _view2.source)
						_sqls.push('DROP VIEW ' + _view1.name + ';CREATE VIEW ' + _view1.name + ' AS\n' + _view1.source);
					_exists = true;
					break;
				}
			}
			/// if view1 miss in schema2, add
			if (!_exists) {
				_sqls.push('CREATE VIEW ' + _view1.name + ' AS\n' + _view1.source);
			}
		}
		/// view from schema 2 missing from view table 1 -> drop
		if (options.mode == 'full') {
			for (var i in _views2) {
				var _view2 = _views2[i];
				/// check if view exists
				var _exists = false;
				for (var j in _views1) {
					var _view1 = _views1[j];
					/// view exists
					if (_view1.name == _view2.name) {
						_exists = true;
						break;
					}
				}
				/// if view2 is not in schema1, drop
				if (!_exists)
					_sqls.push('DROP VIEW ' + _view2.name + ';');
			}
		}

		if (_sqls.length > 0)
			__delta.views.push(_sqls.join('\n'));
		else
			__delta.views.push('--views match');
	};
	var __main = function () {
		verbose('log', '... delta');
		__compareTables();
		__compareFkeys();
		__compareFunctions();
		__compareIndexes();
		__compareTypes();
		__compareViews();
		__sql =
			'\n\n-- TYPES \n\n' +
			__delta.types.join('\n') +
			'\n\n-- FUNCTIONS \n\n' +
			__delta.functions.join('\n') +
			'\n\n-- TABLES AND COLUMNS \n\n' +
			__delta.tables.join('\n') +
			'\n\n-- FOREIGN KEYS \n\n' +
			__delta.fkeys.join('\n') +
			'\n\n-- INDEXES \n\n' +
			__delta.indexes.join('\n') +
			'\n\n-- VIEWS \n\n' +
			__delta.views.join('\n');
	}();
	return {
		sql: function () {
			return __sql;
		}
	};
};
var _conn1 = {
	host: 'homer',
	user: 'ucc',
	password: 'xx',
	database: 'ucc'
};
var _conn2 = {
	host: 'homer',
	user: 'ucc',
	password: 'xx',
	database: 'yn-dev1'
};
var _db1 = new pg.Client(_conn1);
var _db2 = new pg.Client(_conn2);
// @todo connection error

var _schema = {
	tables: true,
	fkeys: true,
	functions: true,
	indexes: true,
	types: true,
	views: true
};
var _options = {
	mode: 'full', // full
	owner: _conn2.connection.user
};
var _schema1 = new Schema(_db1, _schema);
var _schema2 = new Schema(_db2, _schema);
var _job = new tools.tasks(function () {
	_db1.end();
	_db2.end();
	var _delta = new Delta(_schema1.data(), _schema2.data(), _options);
	console.log(_delta.sql());
	fs.writeFile('/tmp/delta.sql', _delta.sql());
});
_job.todo('db1.schema');
_job.todo('db2.schema');
_schema1.load(function () {
	_job.done('db1.schema');
});
_schema2.load(function () {
	_job.done('db2.schema');
});
