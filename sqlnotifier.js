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

	function isLiteral(v) {
		return typeof v === 'number' || typeof v === 'string';
	}

	function isVariable(v) {
		return typeof v === 'object' && v.id;
	}

	function insertWhereClauses(conditions, expr) {
		if (!expr) return;
		if (expr.cmp) {
			var exprs = {
				'=': 0,
				'<>': 1,
				'>=': 4,
				'<': 5,
				'<=': 6,
				'>': 7
				// genious operator list:
				// op&1: 0=equality assured, 1=unequality assured
				// op^1: opposite operator
			};
			var lit, v, x;
			if (isLiteral(expr.a) && isVariable(expr.b)) {
				lit = expr.a;
				v = expr.b.id;
				x = exprs[expr.cmp];
				if (x >= 4) {
					// flip < and > comparisons
					x ^= 1;
				}
			} else if (isLiteral(expr.b) && isVariable(expr.a)) {
				lit = expr.b;
				v = expr.a.id;
				x = exprs[expr.cmp];
			}
			if (v) {
				// v(variable) x(comparison operator) lit(literal9
				if (v.indexOf('.') != -1) {
					// table.identifier
					var table = v.substr(0, v.indexOf('.'));
					v = v.substr(v.indexOf('.'));
					conditions[table].push([v, x, lit]);
				} else {
					// find identifier in table list
					for (var table in tableDef) {
						if (tableDef[table].hasOwnProperty(v)) {
							// insert clause into table's restrictions
							conditions[table].push([v, x, lit]);
						}
					}
				}
				return;
			}
		}
		if (expr.op === 'and') {
			insertWhereClauses(conditions, expr.a);
			insertWhereClauses(conditions, expr.b);
			return;
		}
		console.log('TODO: ' + JSON.stringify(expr));
	}

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
		conditions[query.table] = []; // TODO: column values
		return conditions;
	} else if (query.type == 'update') {
		var conditions = {};
		conditions[query.table] = [];
		insertWhereClauses(conditions, query.where);
		// TODO: or-clause with after-values
		return conditions;
	} else if (query.type == 'delete') {
		var conditions = {};
		conditions[query.table] = [];
		insertWhereClauses(conditions, query.where);
		return conditions;
	} else if (query.type == 'select') {
		var conditions = {};
		for (var id in query.from) {
			var tableName = query.from[id];
			conditions[tableName] = conditions[tableName] || [];
		}
		// TODO: cols, where
		// TODO: nested
		insertWhereClauses(conditions, query.where);
		insertWhereClauses(conditions, query.having);
		return conditions;
	}
	throw "unknown query type " + query.type;
}

function queryEffectsSelect(query, select) {
	function clausesIntercept(a, b) {
		var unequalOperators = {
			1: true, // !=
			4: true, // <
			6: true  // >
		};
		for (var i = 0; i < a.length; i++) {
			for (var j = 0; j < b.length; j++) {
				var l = a[i], r = b[j];
				if (l[0] == r[0]) {
					// both clauses refer to the same attribute
					if (l[1] == 0 && r[1] == 0 && l[2] != r[2]) {
						// interception a=1 && a=2
						return false;
					}
					if (((l[1] == 0 && (r[1]&1)) || ((l[1]&1) && r[1] == 0)) && l[2] == r[2]) {
						// interception a=1 && a!=1
						return false;
					}
					// TODO: < >
				}
			}
		}
		// otherwise: interception
		return true;
	}

	for (var table in query) {
		if (select[table]) {
			// table interception
			if (clausesIntercept(query[table], select[table])) {
				return true;
			}
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
	console.log('observe ' + JSON.stringify(clauses));
	console.log('');
}

function performQuery(query) {
	var clauses = analyzeQuery(query);
	console.log('execute ' + query);
	console.log('trigger ' + JSON.stringify(clauses));
	for (var i = 0; i < registeredQueries.length; i++) {
		if (queryEffectsSelect(clauses, registeredQueries[i].clauses)) {
			console.log('execute ' + registeredQueries[i].query);
		}
	}
	console.log('');
}

observeQuery('select * from a where x=2');
observeQuery('select * from a where x between 6 and 9');
performQuery('insert into a(x) values (3)');
performQuery('delete from b');
performQuery('update a set y=1 where x=4');
performQuery('update a set y=y + 1 where x=2');
performQuery('update a set y=5 where x < 7');


// TODO: nested select, multiple tables
