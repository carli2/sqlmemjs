var parser = require('./sqlparser').parser;

/**
 * list of observed queries
 */
var registeredQueries = [];

/**
 * definition of all tables (to assign columns to tables)
 */
var tableDef = {
	a: {
		x: true,
		y: true
	},
	b: {
		z: true
	}
};

/**
 * decompose a SQL query into table- and column related clauses
 */
function analyzeQuery(query) {
	query = parser.parse(query);
	/**
	 * type | interesting parts
	 *
	 * insert | table, cols, rows
	 * insert | table, cols, select
	 * update | table, set, where
	 * delete | table, where
	 *
	 * select | from, expr, where, having
	 */
	if (query.type == 'insert') {
		var conditions = {};
		conditions[query.table] = {}; // TODO: column values
		return conditions;
	} else if (query.type == 'update') {
		var conditions = {};
		conditions[query.table] = {}; // TODO: where
		return conditions;
	} else if (query.type == 'delete') {
		var conditions = {};
		conditions[query.table] = {}; // TODO: condition
		return conditions;
	} else if (query.type == 'select') {
		var conditions = {};
		for (var id in query.from) {
			var tableName = query.from[id];
			conditions[tableName] = conditions[tableName] || {};
		}
		// TODO: cols, where
		// TODO: nested
		return conditions;
	}
	throw "unknown query type " + query.type;
}

function queryEffectsSelect(query, select) {
	for (var table in query) {
		if (select[table]) {
			// table interception
			// TODO: analyze on tuple level
			return true;
		}
	}
	return false;
}

function observeQuery(query) {
	var clauses = analyzeQuery(query);
	registeredQueries.push({
		query: query,
		clauses: clauses
	});
	console.log('execute ' + query);
	console.log('');
}

function performQuery(query) {
	var clauses = analyzeQuery(query);
	console.log('execute ' + query);
	for (var i = 0; i < registeredQueries.length; i++) {
		if (queryEffectsSelect(clauses, registeredQueries[i].clauses)) {
			console.log('execute ' + registeredQueries[i].query);
		}
	}
	console.log('');
}

observeQuery('select * from a where x=2');
performQuery('insert into a(x) values (3)');
performQuery('delete from b');
performQuery('update a set y=1 where x=4');
performQuery('update a set y=y + 1 where x=2');


// TODO: nested select, multiple tables
