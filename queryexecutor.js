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
	var self = this;

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
	};

	/*
	Data structure holding all tables
	*/
	var tables = {};

	/*
	A Table consists of a schema, data and indices
	*/
	function Table(id, schema) {
		tables[id] = this;
		this.id = id;
		this.schema = schema;
		this.primary = undefined;
		this.cols = {};
		// create table: verify col types
		for(var i in schema) {
			var typ = validateDatatype(schema[i].type);
			if(!typ) {
				throw "unknown data type: " + schema[i].type;
			}
			this.schema[i].type = typ;
			if(schema[i].primary) {
				if(this.primary) {
					throw "Two columns are marked primary: " + primary + " and " + schema[i].id;
				}
				this.primary = schema[i].id;
			}
			this.cols[schema[i].id] = schema[i];
		}
		this.data = [];
		this.cursors = [];


		/*
		INSERT a tuple into the table
		*/
		this.insertTuple = function(tuple) {
			var last_insert;
			// fill default values and auto_increment
			for(var j in this.schema) {
				var col = this.schema[j];
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
			this.data.push(tuple);
			// update all cursors to accept new item
			for(var j = 0; j < this.cursors.length; j++) {
				this.cursors[j].insert(this.data.length-1);
			}
			// TODO: update indices
			return last_insert;
		};

		// TODO: also move update and delete here in order to maintain transactions
	};

	/*
	Template for all cursors
	*/
	function Cursor() {
		this.reset =
		this.fetch =
		this.close =
		function() {};
		this.getSchema = function() {return [];}
		
		this.toArray = function() {
			var result = [];
			var tuple;
			while(tuple = this.fetch()) {
				result.push(tuple);
			}
			return result;
		};

		this.printTable = function(print) {
			print = print || console.log;
			var schema = this.getSchema();
			var line = '';
			for(var i in schema) {
				line += schema[i][0] + '; ';
			}
			print(line);
			print(line.replace(/./g, '-'));
			var tuple;
			while(tuple = this.fetch()) {
				line = '';
				for(var i in schema) {
					line += tuple[schema[i][0]] + '; ';
				}
				print(line);
			}
			print('');
		};

		this.assert = function(target) {
			var source = this.toArray();
			function print() {
				console.log("assertion fail!");
				console.log('IS:     ' + JSON.stringify(source));
				console.log('SHOULD: ' + JSON.stringify(target));
			};
			if(source.length != target.length) {
				print();
				return false;
			}
			for(var i = 0; i < source.length; i++) {
				for(var t in source[i]) {
					if(source[i][t] !== target[i][t]) {
						print();
						return false;
					}
				}
			}
			this.reset();
			this.printTable();
			return true;
		};
	};
	/*
	Iterator that iterates over all tables (SHOW TABLES)
	*/
	function tablesIterator() {
		var keys, cursor;
		this.reset = function() {
			keys = ['TABLES'];
			for(var tab in tables) {
				keys.push(tab);
			}
			cursor = 0;
		};
		this.reset();
		this.close = function() {
		};
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
		};
		this.getSchema = function() {
			return [['IDENTIFIER', 'TEXT']];
		};
	};
	tablesIterator.prototype = new Cursor();
	/*
	Iterator that iterates over all tuples of one table
	*/
	function tableIterator(table) {
		this.cursor = 0;
		var self = this;
		// observer that moves the cursor with deletions
		var observer = {
			active: false,
			insert: function(idx) {
			},
			remove: function(idx) {
				// a row is deleted so we have to step back
				if(idx < self.cursor) {
					self.cursor--;
				}
			},
			setActive: function(active) {
				if(active && !this.active) {
					// register
					table.cursors.push(observer);
					this.active = active;
				}
				if(!active && this.active) {
					// unregister
					table.cursors.splice(table.cursors.indexOf(observer), 1);
					this.active = active;
				}
			}
		};
		this.reset = function() {
			this.cursor = 0;
		};
		this.close = function() {
			// unregister observer in table
			observer.setActive(false);
			// but also keep in mind that users might restart with reset
		};
		this.fetch = function() {
			// TODO: prevent reading data that is too new (eternal loop with insert select)
			if(this.cursor < table.data.length) {
				// fetch one row
				var tuple = table.data[this.cursor];
				// move cursor one further
				this.cursor++;
				// observe changes as we move forward
				observer.setActive(true);
				return tuple;
			} else {
				observer.setActive(false);
			}
		};
		this.getSchema = function() {
			var schema = [];
			for(var i in table.schema) {
				schema.push([table.schema[i].id, table.schema[i].type]);
			}
			return schema;
		};
	};
	tableIterator.prototype = new Cursor();
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
	};
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
	};
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
			var a = code.a !== undefined ? createCondition('', code.a, schema, args) : undefined;
			var b = code.b !== undefined ? createCondition('', code.b, schema, args) : undefined;
			switch(code.op) {
				case 'and':
				return function(tuples) {
						return a(tuples) && b(tuples);
					};
				break;
				case 'or':
				return function(tuples) {
						return a(tuples) || b(tuples);
					};
				break;
				case 'not':
				return function(tuples) {
						return !a(tuples);
					};
				break;

				default:
				throw "Unknown opcode " + code.op;
			}
		}
		if(code.cmp) {
			var a = code.a !== undefined ? createFunction('', code.a, schema, args).fn : undefined;
			var b = code.b !== undefined ? createFunction('', code.b, schema, args).fn : undefined;
			var c = code.c !== undefined ? createFunction('', code.c, schema, args).fn : undefined;
			switch(code.cmp) {
				case '=':
				return function(tuples) {
						return a(tuples) == b(tuples);
					};
				break;
				case '<>':
				return function(tuples) {
						return a(tuples) != b(tuples);
					};
				break;
				case '<':
				return function(tuples) {
						return a(tuples) < b(tuples);
					};
				break;
				case '<=':
				return function(tuples) {
						return a(tuples) <= b(tuples);
					};
				break;
				case '>':
				return function(tuples) {
						return a(tuples) > b(tuples);
					};
				break;
				case '>=':
				return function(tuples) {
						return a(tuples) >= b(tuples);
					};
				break;
				case 'between':
				return function(tuples) {
						var v = a(tuples);
						return v >= b(tuples) && v <= c(tuples);
					};
				break;

				default:
				throw "Unknown opcode " + code.cmp;
			}
		}
		// Default
		return function(t){return true;};
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
				if(code.wildcard === true) {
					throw "unrecognized ? - this is a bug, please report";
				}
				var value = args[code.wildcard];
				return {
					id: id,
					type: (typeof value === 'number') ? 'NUMBER' : 'TEXT',
					fn: function(tuples) { return value; }
				};
			} else if(code.id) {
				// element fetch
				for(var i = 0; i < schema.length; i++) {
					var x = schema[i][0]; // schema relevant.relevant
					var y = /\.([a-zA-Z][a-zA-Z_0-9]*?)$/.exec(x); // schema something.relevant
					if(code.id.toUpperCase() == x.toUpperCase() || y && code.id.toUpperCase() == y[1].toUpperCase()) {
						return {
							id: id,
							type: schema[i][1],
							fn: function(tuples){return tuples[x];}
						};
					}
				}
				throw "Unknown identifier: " + code.id;
			} else if(code.op) {
				var a = code.a !== undefined ? createFunction('', code.a, schema, args).fn : undefined;
				var b = code.b !== undefined ? createFunction('', code.b, schema, args).fn : undefined;
				switch(code.op) {
					case 'add':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return a(tuples) + b(tuples);
						}
					};
					break;
					case 'sub':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return a(tuples) - b(tuples);
						}
					};
					break;
					case 'mul':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return a(tuples) * b(tuples);
						}
					};
					break;
					case 'div':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return a(tuples) / b(tuples);
						}
					};
					break;
					case 'neg':
					return {
						id: id,
						type: "NUMBER",
						fn: function(tuples) {
							return -a(tuples);
						}
					};
					break;
					// TODO: other arithmetic operations

					default:
					throw "Unknown opcode " + code.op;
				}
			} else if(code.nest) {
				// nested SELECT
				var froms = [];
				// add the scope of the tuple to the nested select
				// we do that by adding a FROM clause which returns one tuple
				function findFrom(q) {
					if(typeof q === 'object') {
						if(q.type === 'select') {
							if(!q.from) q.from = {};
							var id = '_outer_tuple';
							while(q.from[id]) id = '_'+id;
							var f = new singleTuple({}, schema);
							q.from[id] = f;
							froms.push(f);
						}
						for(var i in q) {
							// find all from clauses
							findFrom(q[i]);
						}
					}
				};
				findFrom(code.nest);
				// create the iterator (we will reset the iterator for each value)
				var iterator = self.query(code.nest, args);
				var schema = iterator.getSchema();
				if(schema.length !== 1) {
					throw "Nested select needs exactly 1 column";
				}
				return {
					id: id,
					type: schema[0][1],
					fn: function(tuples) {
						// update tuple information to nested selects
						for(var i in froms) {
							froms[i].reset(tuples);
						}
						// execute the inner select
						iterator.reset();
						var val = iterator.fetch();
						iterator.close();
						return val[schema[0][0]];
					}
				};
			} else if(code.call) {
				// functions
				var f = code.call.toUpperCase();
				function assertLength(n) {
					// TODO: also assert types
					if(code.args.length != n) {
						throw f + " expects " + n + " arguments";
					}
				};
				for(var i = 0; i < code.args.length; i++) {
					code.args[i] = createFunction('', code.args[i], schema, args);
				}
				switch(f) {
					case 'SQRT':
						assertLength(1);
						return {
							id: id,
							type: 'NUMBER',
							fn: function(tuples) {
								return Math.sqrt(code.args[0].fn(tuples));
							}
						};
					break;
					// TODO: more functions
					default:
					throw "unknown function " + f;
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
		};
		this.close = function() {
		};
		this.fetch = function() {
			if(count == 0) {
				count++;
				return {VALUE: value};
			}
		};
		this.getSchema = function() {
			return [['VALUE', type]];
		};
	};
	singleValue.prototype = new Cursor();
	/*
	Single tuple select (1 row, n cols)
	@param value tuple
	@param schema schema of the tuple
	*/
	function singleTuple(value, schema) {
		var count = 0;
		this.reset = function(newval) {
			count = 0;
			if(newval) {
				value = newval;
			}
		};
		this.close = function() {
		};
		this.fetch = function() {
			if(count == 0) {
				count++;
				return value;
			}
		};
		this.getSchema = function() {
			return schema;
		};
	};
	singleTuple.prototype = new Cursor();
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
		};
		this.close = function() {
			t1.close();
			t2.close();
		};
		this.getSchema = function() {
			var r = [];
			var s = t1.getSchema();
			for(var i in s) r.push(s[i]);
			s = t2.getSchema();
			for(var i in s) r.push(s[i]);
			return r;
		};
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
		};
	};
	crossJoin.prototype = new Cursor();
	/*
	Union of two iterators
	*/
	function Union(a, b) {
		(function(){
			// validate schema
			var sa = a.getSchema();
			var sb = b.getSchema();
			if(sa.length != sb.length) {
				throw "Incompatible count of columns for UNION";
			}
			for(var i = 0; i < sa.length; i++) {
				if(validateDatatype(sa[i][1]) != validateDatatype(sb[i][1])) {
					throw "Incompatible column " + sb[i][0] + " with type " + sb[i][1];
				}
			}
		})();
		this.reset = function() {
			a.reset();
			b.reset();
		};
		this.close = function() {
			a.close();
			b.close();
		};
		this.getSchema = function() {
			return a.getSchema();
		};
		this.fetch = function() {
			return a.fetch() || b.fetch();
		};
	};
	Union.prototype = new Cursor();
	// add name to a tables identifiers
	function renameSchema(table, prefix) {
		var t = table, p = prefix;
		this.reset = function() {
			t.reset();
		};
		this.close = function() {
			t.close();
		};
		this.getSchema = function() {
			var r = [];
			var s = t.getSchema();
			for(var i in s) r.push([p+'.'+s[i][0], s[i][1]]);
			return r;
		};
		this.fetch = function() {
			var tuple = t.fetch();
			if(!tuple) return tuple;
			var ntuple = {};
			for(var i in tuple) {
				ntuple[p+'.'+i] = tuple[i];
			}
			return ntuple;
		};
	};
	renameSchema.prototype = new Cursor();
	/*
	Map: convert a tuple with a function
	@param table table to iterate over
	@param schema resulting schema
	@param fn function that converts input tuple into output tuple
	*/
	function Map(table, schema, fn) {
		this.reset = function() {
			table.reset();
		};
		this.close = function() {
			table.close();
		};
		this.getSchema = function() {
			return schema;
		};
		this.fetch = function() {
			var tuple = table.fetch();
			if(tuple) return fn(tuple);
		};
	};
	Map.prototype = new Cursor();
	/*
	Filter: only pass accepting tuples
	@param table table to filter
	@param fn function that should return wether a tuple is accepted
	*/
	function Filter(table, fn) {
		this.reset = function() {
			table.reset();
		};
		this.close = function() {
			table.close();
		};
		this.getSchema = function() {
			return table.getSchema();
		};
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
		};
	};
	Filter.prototype = new Cursor();
	/*
	Limiter: do not allow more than n elements
	@param table table to filter
	@param n max number of rows to fetch
	*/
	function Limiter(table, n) {
		var nleft = n;
		this.reset = function() {
			table.reset();
			nleft = n;
		};
		this.close = function() {
			table.close();
		};
		this.getSchema = function() {
			return table.getSchema();
		};
		this.fetch = function() {
			if(nleft > 0) {
				var result = table.fetch();
				nleft--;
				if(nleft <= 0) {
					// close input after fetching everything (but it is resettable)
					table.close();
				}
				return result;
			}
		};
	};
	Limiter.prototype = new Cursor();
	/*
	Skipper: Skip n entries before returning anything
	@param table table to filter
	@param n number of rows to skip
	*/
	function Skipper(table, n) {
		for(var i = 0; i < n; i++) {
			// throw away n rows
			table.fetch();
		}
		this.reset = function() {
			table.reset();
			for(var i = 0; i < n; i++) {
				// throw away n rows
				table.fetch();
			}
		};
		this.close = function() {
			table.close();
		};
		this.getSchema = function() {
			return table.getSchema();
		};
		this.fetch = function() {
			return table.fetch();
		};
	};
	Skipper.prototype = new Cursor();
	/*
	Sorter: Sort all entries. For sorting, all entries have to be fetched.
	@param table table to sort
	@param sortfn sort criteria
	*/
	function Sorter(table, sortfn) {
		var data = [], tuple, cursor = 0;
		// at first, fetch all data
		while(tuple = table.fetch()) {
			data.push(tuple);
		}
		// sort everything
		data.sort(sortfn);
		this.reset = function() {
			// no reset; always stay with old data
			cursor = 0;
		};
		this.close = function() {
			// no need to close since we fetched everything
		};
		this.getSchema = function() {
			return table.getSchema();
		};
		this.fetch = function() {
			// just fetch the next prepared row
			if(cursor < data.length) {
				return data[cursor++];
			}
		};
	};
	Sorter.prototype = new Cursor();
	/*
	Prepare statement (this saves parsing time. maybe in future prepare clonable iterators)
	*/
	this.prepare = function(sql, scope) {
		scope = scope || {index: 1};
		// parse the query
		var query = parser.parse(sql);
		// enumerate all ?'s
		function walkThrough(query) {
			if(typeof query !== 'object') return;
			if(query.wildcard === true) {
				// give index to ?
				query.wildcard = scope.index++;
				return;
			}
			if(query.op || query.cmp || query.type === 'union') {
				walkThrough(query.a);
				walkThrough(query.b);
				walkThrough(query.c);
			}
			if(query.call && query.args) {
				for(var i = 0; i < query.args.length; i++) {
					walkThrough(query.args[i]);
				}
			}
			if(query.nest) {
				walkThrough(query.nest);
			}
			if(query.type === 'select') {
				for(var i = 0; i < query.expr.length; i++) {
					walkThrough(query.expr[i][1]);
				}
				walkThrough(query.from);
				walkThrough(query.where);
				if(query.order) {
					for(var i = 0; i < query.order.length; i++) {
						walkThrough(query.order[i].e);
					}
				}
				walkThrough(query.maxcount);
				walkThrough(query.startcount);
			}
			if(query.type === 'insert') {
				if(query.rows) {
					for(var i = 0; i < query.rows.length; i++) {
						for(var j = 0; j < query.rows[i].length; j++) {
							walkThrough(query.rows[i][j]);
						}
					}
				}
				walkThrough(query.select);
			}
			if(query.type === 'update') {
				for(var i in query.set) {
					walkThrough(query.set[i]);
				}
				walkThrough(query.where);
			}
			if(query.type === 'delete') {
				walkThrough(query.where);
			}
		}
		walkThrough(query);
		return query;
	};
	/*
	Main query method
	*/
	this.query = function(sql, schema) {
		var args;
		if(typeof schema === 'object' && schema.index) {
			// known scope
			args = schema;
		} else {
			// create new scope from function arguments
			args = {index: 1, length: arguments.length};
			for(var i = 1; i < arguments.length; i++) {
				args[i] = arguments[i];
			}
		}
		// parse the query
		var query = (typeof sql === 'string') ? this.prepare(sql, args) : sql;
		console.log(sql + ' => ' + JSON.stringify(query));
		// process queries
		if(query.type == 'select') return (function(){
			var from = null;
			if(query.from) {
				var tables = query.from;
				for(var t in tables) {
					var iterator;
					if(typeof tables[t] === 'string') {
						// named table
						iterator = getTableIterator(tables[t]);
						if(!iterator) {
							throw "Table does not exist: " + tables[t];
						}
					} else {
						// nested select
						if(tables[t].fetch) {
							// raw iterator passed via from
							iterator = tables[t];
						} else {
							// SELECT statement
							iterator = self.query(tables[t]);
						}
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
			// compile calculations
			for(var i = 0; i < cols.length; i++) {
				if(cols[i][0] == '-') {
					// unnamed column
					if(typeof cols[i][1] == 'object' && cols[i][1].id) {
						cols[i][0] = cols[i][1].id;
					} else {
						cols[i][0] = 'col'+String(i+1);
					}
				}
				var f = createFunction(cols[i][0], cols[i][1], from.getSchema(), args);
				newtuple[f.id] = f.fn;
				schema.push([f.id, f.type]);
			}
			// WHERE-Filter (and find index checks)
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
			// ORDER BY
			if(query.order) {
				var schema = table.getSchema();
				// prepare all cols
				for(var i = 0; i < query.order.length; i++) {
					query.order[i].e = createFunction('', query.order[i].e, schema, args);
				}
				var sortfn = function(a, b) {
					for(var i = 0; i < query.order.length; i++) {
						var xa = query.order[i].e.fn(a);
						var xb = query.order[i].e.fn(b);
						// evaluate with this criteria and compare
						if(query.order[i].desc) {
							if(xa > xb) return -1;
							if(xa < xb) return 1;
						} else {
							if(xa > xb) return 1;
							if(xa < xb) return -1;
						}
					}
					return 0; // the rest is equal
				};
				table = new Sorter(table, sortfn);
			}
			// LIMIT
			if(query.startcount !== undefined) {
				table = new Skipper(table, createFunction('', query.startcount, [], args).fn({}));
			}
			if(query.maxcount !== undefined) {
				table = new Limiter(table, createFunction('', query.maxcount, [], args).fn({}));
			}
			return table;
		})(); else if(query.type == 'union') return (function(){
			// TODO: handle arguments
			var a = self.query(query.a);
			var b = self.query(query.b);
			return new Union(a, b);
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
				for(var i = 0; i < query.cols.length; i++) {
					if(query.cols[i].default) {
						// DEFAULT-Value: evaluate and check
						var code = createFunction('default', query.cols[i].default, [], args);
						if(validateDatatype(code.type) != validateDatatype(query.cols[i].type)) {
							throw "incompatible data type for default value";
						}
						query.cols[i].default = code.fn({});
					}
				}
				// create the table
				table = new Table(query.id, query.cols);
				// the constructor has the side effect to register itself
			}
			return new singleValue(query.id, 'STRING');
			// TODO: ALTER
		})(); else if(query.type == 'droptable') return (function(){
			// DROP TABLE
			var tablename = convertStringForAttribute(query.id, tables);
			if(!tablename) {
				if(query.noerror) {
					return new singleValue(0, 'NUMBER');
				}
				throw "Table " + query.id + " does not exist";
			}
			// just drop the reference; GC will do the rest
			delete tables[tablename];
			// TODO: foreign keys
			return new singleValue(1, 'NUMBER');
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
			if(query.rows) {
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
					last_insert = table.insertTuple(tuple);
				}
			} else if(query.select) {
				var rows = self.query(query.select, args);
				var schema = rows.getSchema();
				if(schema.length != cols.length) {
					throw "Incompatible col count in INSERT SELECT";
				}
				var ituple;
				while(ituple = rows.fetch()) {
					var tuple = {};
					for(var j = 0; j < schema.length; j++) {
						tuple[cols[j]] = ituple[schema[j][0]];
					}
					last_insert = table.insertTuple(tuple);
				}
			} else {
				throw "unknown insert - this should not happen";
			}
			var result;
			if(table.primary) {
				result = new singleValue(last_insert, table.cols[table.primary].type);
				result.insert_id = last_insert;
			} else {
				result = new singleValue(0, 'INTEGER');
			}
			return result;
		})(); else if(query.type == 'update') return (function(){
			// UPDATE
			var tablename = convertStringForAttribute(query.table, tables);
			if(!tablename) {
				throw "Table " + query.table + " does not exist";
			}
			var table = tables[tablename];
			var iterator = new tableIterator(table);
			var newset = {};
			for(var i in query.set) {
				var colname = convertStringForAttribute(i, table.cols);
				if(!colname) {
					throw "Table " + tablename + " does not have a column called " + i;
				}
				var code = createFunction(colname, query.set[i], iterator.getSchema(), args);
				if(code.type != table.cols[colname].type) {
					throw "Column " + colname + " has incompatible type";
				}
				newset[colname] = code.fn;
			}
			if(query.where) {
				// Filter tuples by WHERE-Condition
				iterator = new Filter(iterator, createCondition(iterator, query.where, iterator.getSchema(), args));
			}
			var tuple, count = 0;
			// now update all tuples
			while(tuple = iterator.fetch()) {
				var newfields = {};
				for(var i in newset) {
					newfields[i] = newset[i](tuple);
				}
				for(var i in newfields) {
					// TODO: update index
					tuple[i] = newfields[i];
				}
				count++;
			}
			var result = new singleValue(count, 'NUMBER');
			result.num_rows = count;
			return result;
		})(); else if(query.type == 'delete') return (function(){
			// DELETE
			var tablename = convertStringForAttribute(query.table, tables);
			if(!tablename) {
				throw "Table " + query.table + " does not exist";
			}
			var table = tables[tablename];
			var tablei = new tableIterator(table);
			var iterator = tablei;
			if(query.where) {
				// Filter tuples by WHERE-Condition
				iterator = new Filter(iterator, createCondition(iterator, query.where, iterator.getSchema(), args));
			}
			var tuple, count = 0;
			// now remove all tuples that we find
			while(tuple = iterator.fetch()) {
				// take the cursor of the underlyign table iterator
				var index = tablei.cursor-1;
				// remove the data item
				table.data.splice(index, 1);
				// notify all cursors
				for(var i = 0; i < table.cursors.length; i++) {
					table.cursors[i].remove(index);
				}
				count++;
			}
			var result = new singleValue(count, 'NUMBER');
			result.num_rows = count;
			return result;
		})();
		throw "unknown command: " + JSON.stringify(query);
	};
	this.exportJSON = function() {
		var result = {};
		for(var t in tables) {
			var table = {};
			result[t] = table;
			table.schema = tables[t].schema;
			table.data = tables[t].data;
		}
		return result;
	};
	this.importJSON = function(json) {
		for(var t in json) {
			var table = new Table(t, json[t].schema);
			table.data = json[t].data;
		}
	};
}

if(typeof exports) {
	exports.SQLinMemory = SQLinMemory;
}
