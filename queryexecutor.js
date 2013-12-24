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
	var tables = {};

	function tableIterator() {
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
	function convertStringForAttribute(str, obj) {
		if(obj.hasOwnProperty(str)) return str;
		str = str.toUpperCase();
		for(var i in obj) {
			if(i.toUpperCase() == str)
				return i;
		}
	}
	/*
	Single value select
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
	function getTableIterator(identifier) {
		if(identifier.toUpperCase() == 'TABLES')
			return new tableIterator();
		// TODO: also return tables
	}
	this.query = function(sql) {
		var query = parser.parse(sql);
		console.log(JSON.stringify(query));
		if(query.type == 'select') {
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
			// TODO: WHERE-Filter, Projection
			return from;
		}
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
}

if(typeof exports) {
	exports.SQLinMemory = SQLinMemory;
	exports.printTable = printTable;
}
