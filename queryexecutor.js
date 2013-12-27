var parser = require('./sqlparser').parser;

/*

Interface Cursor:
 Constructor - has to initialize the first walkthrough
 function reset() - restart walking through the data.
 function fetch() - return object containing the data of the next row. return undefined or null if no rows left
 function close() - shut down all observers and close all sub-cursors
 function getSchema() - return an array of fields of the form [identifier, type]
A storage backend has to implement that interface. Also all relational operations like selections, projections, cross-joins, index-joins, groups are cursors.


*/

function SQLinMemory() {
	var datatypes = {
		'INTEGER': 'NUMBER',
		'NUMBER': 'NUMBER',
		'FLOAT': 'NUMBER',
		'DOUBLE': 'NUMBER',
		'TEXT': 'TEXT',
		'STRING': 'TEXT',
		'DATE': 'DATE'
	};
	function validateDatatype(type) {
		type = type.toUpperCase();
		if(!datatypes.hasOwnProperty(type)) {
			throw "unknown data type: " + type;
		}
		return datatypes[type];
	}

	/*
	Data structure holding all tables
	*/
	var tables = {};

	/*
	Iterator that iterates over all tables (SHOW TABLES)
	*/
	function tablesIterator() {
		var keys, cursor;
		this.reset = function() {
			keys = ['TABLES'];
			for(tab in tables) {
				keys.push(tab);
			}
			cursor = 0;
		};
		this.reset();
		this.close = function() {
		}
		this.fetch = function() {
			if(cursor < keys.length) {
				// fetch one row
				var tuple = {IDENTIFIER: keys[cursor]};
				// move cursor one further
				cursor++;
				// skip all broken tables
				/*while(!tables[keys[cursor]]) {
					cursor++;
				}*/
				return tuple;
			}
		}
		this.getSchema = function() {
			return [['IDENTIFIER', 'TEXT']];
		}
	}
	/*
	Iterator that iterates over all tuples of one table
	*/
	function tableIterator(table) {
		var cursor;
		this.reset = function() {
			cursor = 0;
		};
		this.reset();
		// TODO: register INSERT/DELETE-observer in table
		// in order to keep cursor stability
		this.close = function() {
			// TODO: unregister observer in table
		}
		this.fetch = function() {
			if(cursor < table.data.length) {
				// fetch one row
				var tuple = table.data[cursor];
				// move cursor one further
				cursor++;
				return tuple;
			}
		}
		this.getSchema = function() {
			var schema = [];
			for(var i in table.schema) {
				schema.push([table.schema[i].id, table.schema[i].type]);
			}
			return schema;
		}
	}
	/*
	Find element of object and return attribute name with correct case
	*/
	function convertStringForAttribute(str, obj) {
		if(obj.hasOwnProperty(str)) return str;
		str = str.toUpperCase();
		for(var i in obj) {
			if(i.toUpperCase() == str)
				return i;
		}
	}
	/*
	Get the iterator for a table name
	*/
	function getTableIterator(identifier) {
		if(identifier.toUpperCase() == 'TABLES')
			return new tablesIterator();
		var tablename = convertStringForAttribute(identifier, tables);
		if(tablename) {
			return new tableIterator(tables[tablename]);
		}
	}
	/*
	Create condition out of expression
	@param id identifier of the row
	@param code parsed JSON values of the condition
	@param schema schema of the input tuple for that expression
	@param args wildcard arguments
	@return {id: string, type: string, fn: function(tuples: JSON)=>value} compiled function
	*/
	function createCondition(id, code, schema, args) {
		if(code.op) {
			var a = code.a ? createCondition('', code.a, schema, args) : undefined;
			var b = code.b ? createCondition('', code.b, schema, args) : undefined;
			switch(code.op) {
				case 'and':
				return function(tuples) {
						return a(tuples) && b(tuples);
					}
				break;
				case 'or':
				return function(tuples) {
						return a(tuples) || b(tuples);
					}
				break;
				case 'not':
				return function(tuples) {
						return !a(tuples);
					}
				break;

				default:
				throw "Unknown opcode " + code.op;
			}
		}
		if(code.cmp) {
			var a = code.a ? createFunction('', code.a, schema, args).fn : undefined;
			var b = code.b ? createFunction('', code.b, schema, args).fn : undefined;
			switch(code.cmp) {
				case '=':
				return function(tuples) {
						return a(tuples) == b(tuples);
					}
				break;
				case '<>':
				return function(tuples) {
						return a(tuples) != b(tuples);
					}
				break;
				case '<':
				return function(tuples) {
						return a(tuples) < b(tuples);
					}
				break;
				case '<=':
				return function(tuples) {
						return a(tuples) <= b(tuples);
					}
				break;
				case '>':
				return function(tuples) {
						return a(tuples) > b(tuples);
					}
				break;
				case '>=':
				return function(tuples) {
						return a(tuples) >= b(tuples);
					}
				break;

				default:
				throw "Unknown opcode " + code.cmp;
			}
		}
		// Default
		return function(t){return true;}
	}
	/*
	Create function out of expression
	@param id identifier of the row
	@param code parsed JSON values of the expression
	@param schema schema of the input tuple for that expression
	@param args wildcard arguments
	@return {id: string, type: string, fn: function(tuples: JSON)=>value} compiled function
	*/
	function createFunction(id, code, schema, args) {
		if(typeof code == 'object') {
			if(code.wildcard) {
				// ? element (pop one argument)
				var value = args.pop();
				return {
					id: id,
					type: (typeof value === 'number') ? 'NUMBER' : 'TEXT',
					fn: function(tuples) { return value; }
				}
			} else if(code.id) {
				// element fetch
				for(var i in schema) {
					var x = schema[i][0];
					if(code.id.toUpperCase() == x.toUpperCase()) {
						return {
							id: id,
							type: schema[i][1],
							fn: function(tuples){return tuples[x];}
						};
					}
				}
				throw "Unknown identifier: " + code.id;
			} else if(code.op) {
				var a = code.a ? createFunction('', code.a, schema, args).fn : undefined;
				var b = code.b ? createFunction('', code.b, schema, args).fn : undefined;
				switch(code.op) {
					case 'add':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return a(tuples) + b(tuples);
						}
					}
					break;
					// TODO: other arithmetic operations

					default:
					throw "Unknown opcode " + code.op;
				}
			}
		}
		if(typeof code == 'number') {
			return {
				id: id,
				type: 'NUMBER',
				fn: function(){return code;}
			};
		}
		if(typeof code == 'string') {
			return {
				id: id,
				type: 'TEXT',
				fn: function(){return code;}
			};
		}
		// Default
		return {
			id: id,
			type: 'INTEGER',
			fn: function(t){return 1;}
		};
	}
	/*
	Single value select (1 row, 1 col)
	@param value value to return
	@param type type of that one value
	*/
	function singleValue(value, type) {
		var count = 0;
		this.reset = function() {
			count = 0;
		}
		this.close = function() {
		}
		this.fetch = function() {
			if(count == 0) {
				count++;
				return {VALUE: value};
			}
		}
		this.getSchema = function() {
			return [['VALUE', type]];
		}
	}
	/*
	Traditional cross join
	*/
	function crossJoin(a, b) {
		var t1 = a, t2 = b;
		var leftTuple = t1.fetch();
		this.reset = function() {
			t1.reset();
			leftTuple = t1.fetch();
			t2.reset();
		}
		this.close = function() {
			t1.close();
			t2.close();
		}
		this.getSchema = function() {
			var r = [];
			var s = t1.getSchema();
			for(var i in s) r.push(s[i]);
			s = t2.getSchema();
			for(var i in s) r.push(s[i]);
			return r;
		}
		this.fetch = function() {
			if(!leftTuple) return undefined;
			var rightTuple = t2.fetch();
			if(!rightTuple) {
				leftTuple = t1.fetch();
				if(!leftTuple) // end of cross join
					return undefined;
				t2.reset();
				rightTuple = t2.fetch();
				if(!rightTuple) // t2 is empty
					return undefined;
			}
			var tuple = {};
			for(var i in leftTuple) {
				tuple[i] = leftTuple[i];
			}
			for(var i in rightTuple) {
				tuple[i] = rightTuple[i];
			}
			return tuple;
		}
	}
	// add name to a tables identifiers
	function renameSchema(table, prefix) {
		var t = table, p = prefix;
		this.reset = function() {
			t.reset();
		}
		this.close = function() {
			t.close();
		}
		this.getSchema = function() {
			var r = [];
			var s = t.getSchema();
			for(var i in s) r.push(s[i]);
			for(var i in s) r.push([p+'.'+s[i][0], s[i][1]]);
			return r;
		}
		this.fetch = function() {
			var tuple = t.fetch();
			if(!tuple) return tuple;
			for(var i in tuple) {
				tuple[p+'.'+i] = tuple[i];
			}
			return tuple;
		}
	}
	/*
	Map: convert a tuple with a function
	@param table table to iterate over
	@param schema resulting schema
	@param fn function that converts input tuple into output tuple
	*/
	function Map(table, schema, fn) {
		this.reset = function() {
			table.reset();
		}
		this.close = function() {
			table.close();
		}
		this.getSchema = function() {
			return schema;
		}
		this.fetch = function() {
			var tuple = table.fetch();
			if(tuple) return fn(tuple);
		}
	}
	/*
	Filter: only pass accepting tuples
	@param table table to filter
	@param fn function that should return wether a tuple is accepted
	*/
	function Filter(table, fn) {
		this.reset = function() {
			table.reset();
		}
		this.close = function() {
			table.close();
		}
		this.getSchema = function() {
			return table.getSchema;
		}
		this.fetch = function() {
			while(true) {
				var tuple = table.fetch();
				if(!tuple) {
					// reached end of tuples
					return undefined;
				}
				if(fn(tuple)) {
					// return accepted tuple
					return tuple;
				}
				// fetch next element
			}
		}
	}
	/*
	Prepare statement (this saves parsing time. maybe in future prepare clonable iterators)
	*/
	this.prepare = function(sql) {
		return parser.parse(sql);
	}
	/*
	Main query method
	*/
	this.query = function(sql) {
		var args = []; // wildcard arguments
		for(var i = arguments.length-1; i > 0; i--) {
			args.push(arguments[i]);
		}
		// parse the query
		var query = (typeof sql === 'string') ? this.prepare(sql) : sql;
		console.log(sql + ' => ' + JSON.stringify(query));
		// process queries
		if(query.type == 'select') return (function(){
			var from = null;
			if(query.from) {
				var tables = query.from;
				for(var t in tables) {
					var iterator = getTableIterator(tables[t]);
					if(!iterator) {
						throw "Table does not exist: " + tables[t];
					}
					iterator = new renameSchema(iterator, t);
					tables[t] = iterator;
					// cross-join all FROMs
					if(from) {
						from = new crossJoin(from, iterator);
					} else {
						from = iterator;
					}
				}
			} else {
				// Single Select
				from = new singleValue(1, 'INTEGER');
			}
			// Adjunction/Projection: walk through values to select
			var cols = [];
			for(var i in query.expr) {
				if(query.expr[i] === '') {
					// select *
					var schema = from.getSchema();
					for(var j in schema) {
						var x = schema[j][0];
						if(x.indexOf('.') != -1)
							cols.push([x, {id: x}]);
					}
				} else if((typeof query.expr[i]) == 'string') {
					// select table.*
					var table = getTableIterator(query.expr[i]);
					if(!table)
						throw "Table " + query.expr[i] + " does not exist";
					var schema = table.getSchema();
					table.close();
					for(var j in schema) {
						var x = query.expr[i] + '.' + schema[j][0];
						if(x.indexOf('.') != -1)
							cols.push([x, {id: x}]);
					}
				} else {
					// ... as ... or ...
					cols.push(query.expr[i]);
				}
			}
			var newtuple = {}, schema = [];
			// compile calculations first (wildcard order)
			for(var i in cols) {
				var f = createFunction(cols[i][0], cols[i][1], from.getSchema(), args);
				newtuple[f.id] = f.fn;
				schema.push([f.id, f.type]);
			}
			// WHERE-Filter (and find index checks) (wildcard order)
			if(query.where) {
				from = new Filter(from, createCondition(from, query.where, from.getSchema(), args));
			}
			// SELECT XYZ-Mapping
			var table = new Map(from, schema, function(inp) {
				var outp = {};
				// iterate over all cols
				for(var i in newtuple) {
					outp[i] = newtuple[i](inp);
				}
				return outp;
			});
			// TODO: Group by
			// TODO: Having
			// TODO: Order
			return table;
		})(); else if(query.type == 'createtable') return (function(){
			// CREATE TABLE: check if table already exists
			var table = getTableIterator(query.id);
			if(table) {
				table.close();
				// flag IF NOT EXISTS
				if(query.erroronexists) {
					throw "Table " + query.id + " already exists";
				}
			} else {
				var primary = undefined;
				var cols = {};
				// create table: verify col types
				for(var i in query.cols) {
					var typ = validateDatatype(query.cols[i].type);
					if(!typ) {
						throw "unknown data type: " + query.cols[i].type;
					}
					query.cols[i].type = typ;
					if(query.cols[i].default) {
						// DEFAULT-Value: evaluate and check
						var code = createFunction('default', query.cols[i].default, [], args);
						if(validateDatatype(code.type) != typ) {
							throw "incompatible data type for default value";
						}
						query.cols[i].default = code.fn({});
					}
					if(query.cols[i].primary) {
						if(primary) {
							throw "Two columns are marked primary: " + primary + " and " + query.cols[i].id;
						}
						primary = query.cols[i].id;
					}
					cols[query.cols[i].id] = query.cols[i];
				}
				// create data structure for table
				table = {id: query.id, schema: query.cols, cols: cols, data: [], primary: primary};
				tables[query.id] = table;
			}
			return new singleValue(query.id, 'STRING');
		})(); else if(query.type == 'insert') return (function(){
			// INSERT ...
			var tablename = convertStringForAttribute(query.table, tables);
			if(!tablename) {
				throw "Table " + query.table + " does not exist";
			}
			var table = tables[tablename];
			var cols = Array(query.cols.length);
			for(var i in query.cols) {
				cols[i] = convertStringForAttribute(query.cols[i], table.cols);
				if(!cols[i]) {
					throw "Table " + tablename + " has no column called " + query.cols[i];
				}
			}
			var last_insert = 0;
			for(var i in query.rows) {
				var row = query.rows[i];
				if(row.length != cols.length) {
					throw "INSERT row has wrong number of elements";
				}
				var tuple = {};
				for(var j in row) {
					// compile code of the insert query
					var code = createFunction(cols[j], row[j], [], args);
					tuple[code.id] = code.fn({}); // fill the tuples
				}
				// fill default values and auto_increment
				for(var j in table.schema) {
					var col = table.schema[j];
					if(!tuple[col.id]) {
						// col default
						if(col.auto_increment) {
							// AUTO_INCREMENT
							tuple[col.id] = col.auto_increment;
							col.auto_increment++;
						} else if(col.default) {
							// DEFAULT-Value
							tuple[col.id] = col.default;
						} else {
							// zero value
							if(col.type === 'TEXT') {
								tuple[col.id] = '';
							} else if(col.type === 'NUMBER') {
								tuple[col.id] = 0;
							}
						}
					}
					if(col.primary) {
						// update insert_id
						last_insert = tuple[col.id];
					}
				}
				table.data.push(tuple);
				// TODO: update all cursors to accept new item
			}
			var result;
			if(table.primary) {
				result = new singleValue(last_insert, table.cols[table.primary].type)
				result.insert_id = last_insert;
			} else {
				result = new singleValue(0, 'INTEGER');
			}
			return result;
		})();
		throw "unknown command: " + JSON.stringify(query);
	}
}

function printTable(table, print) {
	print = print || console.log;
	var schema = table.getSchema();
	var line = '';
	for(var i in schema) {
		line += schema[i][0] + '; ';
	}
	print(line);
	print(line.replace(/./g, '-'));
	var tuple;
	while(tuple = table.fetch()) {
		line = '';
		for(var i in schema) {
			line += tuple[schema[i][0]] + '; ';
		}
		print(line);
	}
	print('');
}

if(typeof exports) {
	exports.SQLinMemory = SQLinMemory;
	exports.printTable = printTable;
}
