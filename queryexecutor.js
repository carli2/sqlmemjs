var parser = require('./sqlparser').parser;

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
			while(cursor < keys.length && !tables[keys[cursor]]) {
				cursor++;
			}
			if(cursor < keys.length) {
				return {IDENTIFIER: keys[cursor]};
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
	function getTableIterator(identifier) {
		if(identifier.toUpperCase() == 'TABLES')
			return new tableIterator();
		// TODO: also return tables
	}
	this.query = function(sql) {
		var query = parser.parse(sql);
		console.log(JSON.stringify(query));
		if(query.type == 'select') {
			if(query.from) {
				var from = getTableIterator(query.from);
				return from;
				// TODO: select etc.
			}
		}
	}
}

if(typeof exports) {
	exports.SQLinMemory = SQLinMemory;
}
