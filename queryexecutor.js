var parser = require('./sqlparser').parser;

/**
SQLinMemory is a in memory SQL compatible database. It allows executing SQL queries on
relational data.
@constructor
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

	/**
	Data structure holding all tables
	@private
	*/
	var tables = {};

	/**
	A Table consists of a schema, data and indices
	@private
	@constructor
	@param {string} id identifier of the table
	@param schema schema of the table
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
			this.schema[i].type = typ; // TODO: remove this as the type diviersity gives us meta information for rendering
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


		/**
		INSERT a tuple into the table
		@private
		@param {Object} tuple JS object that contains the data
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

	/**
	Template for all cursors.
	A cursor may be a table cursor, a indexed table cursor or
	the result of a query.
	A cursor has to initialize itself to be ready for the first walkthrough.
	Providing a new storage backend to SQLinMemory.js is easy as you just have
	to provide cursors to walk through tables (or walk through indices)
	and all queries can be executed by composing walkthroughs on the data.
	@constructor
	*/
	function Cursor() {
		/**
		Restart walking through the data.
		This function should reinizialize the cursor as if you were
		creating the cursor again. This should also work after closing
		the cursor and should not leak memory when not all data was fetched.
		*/
		this.reset = function() {};
		/**
		Fetch one row of the table. After the last row is fetched,
		<tt>undefined</tt> should be returned. All observers should be
		closed after <tt>undefined</tt> was returned. No call to close()
		is necessary then.
		@return JS object containing the values. Keys are named like described by getSchema().
		*/
		this.fetch = function() {};
		/**
		Closes the cursor. Calling close() ensures that no memory is leaked.
		The only valid operation after close() is reset() and getSchema().
		*/
		this.close = function() {};
		/**
		Returns the data schema of the table contained in the cursor.
		@return JS array containing values of the form [identifier, type]
		*/
		this.getSchema = function() {return [];}
		
		/**
		Fetches all tuples and returns an array of tuples.
		This function walks through the table and closes it.
		@return JS array of JS objects
		*/
		this.toArray = function() {
			var result = [];
			var tuple;
			while(tuple = this.fetch()) {
				result.push(tuple);
			}
			return result;
		};

		/**
		Prints the table in a human readable form.
		This function walks through the table and closes it.
		@param print Printline function, defaults to <tt>console.log</tt>
		*/
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

		/**
		Asserts that the table behind has the asserted data.
		If that is not the case, a error message is printed.
		Use this function to provide test cases.
		This function walks through the table and closes it.
		@param target expected value that is compared against the result of toArray()
		*/
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
	/**
	Iterator that iterates over all tuples of one table
	@private
	@constructor
	@param {Table} table table to iterate over
	*/
	function tableIterator(table) {
		Cursor.call(this);

		/**
		Cursor that points to the next line to fetch.
		The observer moves this cursor around as the
		data changes. This guarantees cursor stability.
		*/
		this.cursor = 0;
		var self = this;
		/** observer that moves the cursor with deletions */
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
	/**
	Find element of object and return attribute name with correct case.
	@private
	@param {string} str string to find in obj, case insensitive
	@param {Object} obj associative array
	*/
	function convertStringForAttribute(str, obj) {
		if(obj.hasOwnProperty(str)) return str;
		str = str.toUpperCase();
		for(var i in obj) {
			if(i.toUpperCase() == str)
				return i;
		}
	};
	/**
	Get the iterator for a table name
	@private
	@param {string} identifier case insensitive identifier of the table
	*/
	function getTableIterator(identifier) {
		var data;
		// TODO: DRY
		if(identifier.toUpperCase() == 'TABLES') {
			data = [];
			data.push({IDENTIFIER: 'TABLES'});
			data.push({IDENTIFIER: 'COLUMNS'});
			for(var t in tables) {
				data.push({IDENTIFIER: t});
			}
			return new multiTuple(data, [['IDENTIFIER', 'TEXT']]);
		}
		if(identifier.toUpperCase() == 'COLUMNS') {
			data = [];
			data.push({
				TABLE: 'TABLES',
				COLUMN_NAME: 'IDENTIFIER',
				COLUMN_POSITION: 1,
				DATATYPE: 'TEXT',
				COMMENT: 'Identifier of the table'
			});
			data.push({
				TABLE: 'COLUMNS',
				COLUMN_NAME: 'TABLE',
				COLUMN_POSITION: 1,
				DATATYPE: 'TEXT',
				COMMENT: 'Name of the table'
			});
			data.push({
				TABLE: 'COLUMNS',
				COLUMN_NAME: 'COLUMN_NAME',
				COLUMN_POSITION: 2,
				DATATYPE: 'TEXT',
				COMMENT: 'Name of the column'
			});
			data.push({
				TABLE: 'COLUMNS',
				COLUMN_NAME: 'COLUMN_POSITION',
				COLUMN_POSITION: 3,
				DATATYPE: 'INTEGER',
				COMMENT: 'Position inside the table'
			});
			data.push({
				TABLE: 'COLUMNS',
				COLUMN_NAME: 'DATATYPE',
				COLUMN_POSITION: 4,
				DATATYPE: 'TEXT',
				COMMENT: 'Data type of the column'
			});
			data.push({
				TABLE: 'COLUMNS',
				COLUMN_NAME: 'COMMENT',
				COLUMN_POSITION: 5,
				DATATYPE: 'TEXT',
				COMMENT: 'Comment that describes the column'
			});
			for(var t in tables) {
				for(var i = 0; i < tables[t].schema.length; i++) {
					data.push({
						TABLE: t,
						COLUMN_NAME: tables[t].schema[i].id,
						COLUMN_POSITION: i+1,
						DATATYPE: tables[t].schema[i].type,
						COMMENT: tables[t].schema[i].comment
					});
				}
			}
			return new multiTuple(data, [['TABLE', 'TEXT'], ['COLUMN_NAME', 'TEXT'], ['COLUMN_POSITION', 'INTEGER'], ['DATATYPE', 'TEXT'], ['COMMENT', 'TEXT']]);
		}
		// a normal table
		var tablename = convertStringForAttribute(identifier, tables);
		if(tablename) {
			return new tableIterator(tables[tablename]);
		}
	};
	/**
	Create condition out of expression
	@private
	@param code parsed JSON values of the condition
	@param schema schema of the input tuple for that expression
	@param args wildcard arguments
	@return <tt>function(tuple: JSON)=>boolean</tt> compiled function
	*/
	function createCondition(code, schema, args) {
		if(code.op) {
			var a = code.a !== undefined ? createCondition(code.a, schema, args) : undefined;
			var b = code.b !== undefined ? createCondition(code.b, schema, args) : undefined;
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
	/**
	Create function out of expression
	@private
	@param {string} id identifier of the row
	@param code parsed JSON values of the expression
	@param schema schema of the input tuple for that expression
	@param args wildcard arguments
	@return object <tt>id: string, type: string, fn: function(tuple: JSON)=>value</tt> compiled function
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
	};
	/**
	 * Map of all aggregations. Create a aggregation by calling the function.
	 * The function returns a function which you can call for additional values.
	 * The function also has a method getValue which returns the result.
	 * */
	var aggregates = {
		SUM: function(value) {
			var sum = value;
			var x = function(value) {
				sum += value;
			}
			x.getValue = function() {
				return sum;
			}
			return x;
		},
		COUNT: function(value) {
			var count = 1;
			var x = function(value) {
				count++;
			}
			x.getValue = function() {
				return count;
			}
			return x;
		},
		AVG: function(value) {
			var count = 1, sum = value;
			var x = function(value) {
				count++;
				sum += value;
			}
			x.getValue = function() {
				return sum/count;
			}
			return x;
		},
		MAX: function(value) {
			var val = value;
			var x = function(value) {
				if(value > val) {
					val = value;
				}
			}
			x.getValue = function() {
				return val;
			}
			return x;
		},
		MIN: function(value) {
			var val = value;
			var x = function(value) {
				if(value < val) {
					val = value;
				}
			}
			x.getValue = function() {
				return val;
			}
			return x;
		},
		FIRST: function(value) {
			var val = value;
			var x = function(value) {
			}
			x.getValue = function() {
				return val;
			}
			return x;
		},
		LAST: function(value) {
			var val = value;
			var x = function(value) {
				val = value;
			}
			x.getValue = function() {
				return val;
			}
			return x;
		}
	};
	/**
	Single value select (1 row, 1 col)
	@private
	@constructor
	@param value value to return
	@param {string} type type of that one value
	*/
	function singleValue(value, type) {
		Cursor.call(this);

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
	/**
	Single tuple select (1 row, n cols)
	@private
	@constructor
	@param value tuple
	@param schema schema of the tuple
	*/
	function singleTuple(value, schema) {
		Cursor.call(this);

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
	/**
	Multiple tuple select (m row, n cols)
	@private
	@constructor
	@param values tuples
	@param schema schema of the tuple
	*/
	function multiTuple(values, schema) {
		Cursor.call(this);

		var count = 0;
		this.reset = function(newvals) {
			count = 0;
			if(newvals) {
				values = newvals;
			}
		};
		this.close = function() {
		};
		this.fetch = function() {
			if(count < values.length) {
				return values[count++];
			}
		};
		this.getSchema = function() {
			return schema;
		};
	};
	/**
	Traditional cross join
	@private
	@constructor
	@param {Cursor} a first table to cross join
	@param {Cursor} b second table to cross join
	*/
	function crossJoin(a, b) {
		Cursor.call(this);

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
	/**
	Union of two iterators
	@private
	@constructor
	@param {Cursor} a first table to output
	@param {Cursor} b second table to output
	*/
	function Union(a, b) {
		Cursor.call(this);

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
	/**
	Add name to a tables identifiers
	@private
	@constructor
	@param {Cursor} table table to rename
	@param {string} prefix prefix to give to all tables attributes (a turns to prefix.a)
	*/
	function renameSchema(table, prefix) {
		Cursor.call(this);

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
	/**
	Map: convert a tuple with a function
	@private
	@constructor
	@param {Cursor} table table to iterate over
	@param schema resulting schema
	@param fn function that converts input tuple into output tuple
	*/
	function Map(table, schema, fn) {
		Cursor.call(this);

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
	/**
	Filter: only pass accepting tuples
	@private
	@constructor
	@param {Cursor} table table to filter
	@param fn function that should return wether a tuple is accepted
	*/
	function Filter(table, fn) {
		Cursor.call(this);

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
	/**
	Group: group items according to a aggregate function
	@private
	@constructor
	@param {Cursor} table table to filter
	@param aggr aggregates (map from column name to aggregate function)
	*/
	function Group(table, aggr, getKey) {
		Cursor.call(this);

		// criteria for grouping
		getKey = getKey || function (inp) {
			return 'x';
		}

		var data, cursor;
		this.reset = function() {
			cursor = 0;
			data = [];
			// at first, fetch all data
			table.reset();

			// prepare data
			var tuple, result = {};
			// walk through data
			while(tuple = table.fetch()) {
				var key = getKey(tuple), fns;
				if(!result[key]) {
					// prepare first run element of aggregation
					fns = {};
					for(var i in aggr) {
						fns[i] = aggregates[aggr[i]](tuple[i]);
					}
					result[key] = fns;
				} else {
					// second run
					fns = result[key];
					for(var i in fns) {
						fns[i](tuple[i]);
					}
				}
			}
			// extract data from aggregate functions
			for(var i in result) {
				var x = result[i];
				for(var j in x) {
					x[j] = x[j].getValue();
				}
				data.push(x);
			}
		};
		this.reset();
		this.close = function() {
			// free the data (setting to null is faster than deleting)
			data = null;
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
	/**
	Limiter: do not allow more than n elements
	@private
	@constructor
	@param {Cursor} table table to filter
	@param {number} n max number of rows to fetch
	*/
	function Limiter(table, n) {
		Cursor.call(this);

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
	/**
	Skipper: Skip n entries before returning anything
	@private
	@constructor
	@param {Cursor} table table to filter
	@param {number} n number of rows to skip
	*/
	function Skipper(table, n) {
		Cursor.call(this);

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
	/**
	Sorter: Sort all entries. For sorting, all entries have to be fetched.
	@private
	@constructor
	@param {cursor} table table to sort
	@param sortfn sort criteria
	*/
	function Sorter(table, sortfn) {
		Cursor.call(this);

		var data, cursor;
		this.reset = function() {
			cursor = 0;
			data = [];
			// at first, fetch all data
			table.reset();
			var tuple;
			while(tuple = table.fetch()) {
				data.push(tuple);
			}
			// sort everything
			data.sort(sortfn);
		};
		this.reset();
		this.close = function() {
			// free the data (setting to null is faster than deleting)
			data = null;
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
	/**
	Prepare statement (this saves parsing time. maybe in future prepare clonable iterators)
	@param {string} sql SQL string to parse
	@return {Object} data structure that can be passed to query()
	@see query
	*/
	this.prepare = function(sql, scope) {
		scope = scope || {index: 1};
		// parse the query
		var query = parser.parse(sql);
		/**
		enumerate all ?'s in a query starting with 1
		@private
		@param query query object to walk through
		*/
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
				if(query.group) {
					for(var i = 0; i < query.group.length; i++) {
						walkThrough(query.group[i]);
					}
				}
				walkThrough(query.having);
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
	/**
	Main query method
	@param {string|Object} sql SQL string or prepared statement
	@param schema arguments for the <tt>?</tt> values. Use as many arguments as you want.
	@return {Cursor} Cursor that contains the results. Additionaly on insert last_insert is set and on update and delete num_rows is set.
	@see Cursor
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
					if(t != 'inner_table') {
						iterator = new renameSchema(iterator, t);
					}
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

			// helper functions: groupby-prepare (split expression into collection and after-aggregate part)
			var tmpcounter = 0, hasAnyAggregates = query.group?true:false;
			var inner = {}, innerAggregate = {}, innerschema = [];
			function splitFunction(fn, name) {
				var hasAggr = false;
				function findAggregates(fn) {
					if(typeof fn !== 'object') {
						return fn;
					}
					if(fn.id) {
						// export identifiers
						if(!inner[fn.id]) {
							inner[fn.id] = fn;
							innerAggregate[fn.id] = 'FIRST';
						}
					}
					if(fn.call && aggregates.hasOwnProperty(fn.call.toUpperCase()) && fn.args.length === 1) {
						// contains aggregate
						hasAggr = true;
						hasAnyAggregates = true;
						var newname = 'tmp'+(tmpcounter++); // new temporary name of the value to collect
						inner[newname] = fn.args[0];
						innerAggregate[newname] =fn.call.toUpperCase();
						return {id: newname}; // outer: read value from aggregates
					} else {
						// copy attributes
						var result = {};
						for(var f in fn) {
							// copy all attributes transformed
							result[f] = findAggregates(fn[f]);
						}
						return result;
					}
				}
				var outer = findAggregates(fn);
				if(!hasAggr) {
					// insert the FIRST aggregate
					inner[name] = outer;
					innerAggregate[name] = 'FIRST';
					return {id: name};
				} else {
					return outer;
				}
			}

			var newtuple = {}, schema = [];
			// compile calculations
			for(var i = 0; i < cols.length; i++) {
				// give names to all columns
				if(cols[i][0] === '-') {
					// unnamed column
					if(typeof cols[i][1] == 'object' && cols[i][1].id) {
						cols[i][0] = cols[i][1].id;
					} else {
						cols[i][0] = 'col'+String(i+1);
					}
				}
				// try to split the function from their aggregates
				newtuple[cols[i][0]] = splitFunction(cols[i][1], cols[i][0]);
			}
			// compile inner functions of aggregate
			if(hasAnyAggregates) {
				for(var i in inner) {
					var f = createFunction(i, inner[i], from.getSchema(), args);
					inner[f.id] = f.fn;
					innerschema.push([f.id, f.type]);
				}
			}
			// compile outer functions of aggregate
			for(var i = 0; i < cols.length; i++) {
				var f;
				if(hasAnyAggregates) {
					// expression split into two parts
					f = createFunction(cols[i][0], newtuple[cols[i][0]], innerschema, args);
				} else {
					// single expression
					f = createFunction(cols[i][0], cols[i][1], from.getSchema(), args);
				}
				newtuple[f.id] = f.fn;
				schema.push([f.id, f.type]);
			}
			// compile group function
			var groupFn;
			if(query.group) {
				groupFn = [];
				for(var i = 0; i < query.group.length; i++) {
					groupFn.push(createFunction('', query.group[i], innerschema, args).fn);
				}
			}

			// WHERE-Filter (evaluating the input)
			if(query.where) {
				from = new Filter(from, createCondition(query.where, from.getSchema(), args));
			}

			// do we have aggregates?
			if(hasAnyAggregates) {
				// GROUP BY / Aggregates
				// first calculate inner part of the aggregates and the passthrough IDs
				from = new Map(from, innerschema, function(inp) {
					var outp = {};
					// iterate over all cols
					for(var i in inner) {
						outp[i] = inner[i](inp);
					}
					return outp;
				});
				// then group the items and calculate the aggregates
				from = new Group(from, innerAggregate, function(inp) {
					// key for the grouping
					if(groupFn) {
						// build a key to group
						var result = '';
						for(var i = 0; i < groupFn.length; i++) {
							// hacky but no one would use that string in a database
							result += '||;|<-' + groupFn[i](inp);
						}
						return result;
					} else {
						// group all together
						return 'x';
					}
				});
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

			// HAVING-Filter (evaluating the output)
			if(query.having) {
				table = new Filter(table, createCondition(query.having, table.getSchema(), args));
			}
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
				iterator = new Filter(iterator, createCondition(query.where, iterator.getSchema(), args));
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
				iterator = new Filter(iterator, createCondition(query.where, iterator.getSchema(), args));
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
	/**
	Exports all tables as a JSON object
	@return JSON object containing all tables.
	@see importJSON
	*/
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
	/**
	Imports a exported JSON object into the table.
	Existing tables are overwritten.
	@param json JSON object generated by exportJSON()
	@see exportJSON
	*/
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
