var parser = require('./sqlparser').parser;

function SQLinMemory() {
	this.query = function(sql) {
		return JSON.stringify(parser.parse(sql));
	}
}

if(typeof exports) {
	exports.SQLinMemory = SQLinMemory;
}
