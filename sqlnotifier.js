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
		// op^2: turn around sign
};
var testLiterals = {
	0: function(a, b) { return a == b; },
	1: function(a, b) { return a != b; },
	4: function(a, b) { return a >= b; },
	5: function(a, b) { return a < b; },
	6: function(a, b) { return a <= b; },
	7: function(a, b) { return a > b; },
};
var equalityCombinations = [
	// scheme: x is attribute, a, b are literals
	// if x ? a && x ? b && a ? b then unsat
	[5, 4, 6], // < >= <=
	[6, 4, 5], // <= >= <
	[6, 7, 6], // <= > <=
	[0, 0, 1], // = = <>
	[0, 1, 0], // = <> =
	[0, 5, 0], // = < =
	[0, 7, 0], // = > =
	[0, 4, 5], // = >= <
	[0, 6, 7], // = <= >
];

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
			var lit, v, x;
			if (isLiteral(expr.a) && isVariable(expr.b)) {
				lit = expr.a;
				v = expr.b.id;
				x = exprs[expr.cmp];
				if (x >= 4) {
					// flip < and > comparisons
					x ^= 2;
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
		var colvals = [];
		if (query.rows.length == 1) {
			// insert only one tuple: add clauses
			for (var i = 0; i < query.cols.length; i++) {
				if (isLiteral(query.rows[0][i])) {
					// add clause col = value
					colvals.push([query.cols[i], 0, query.rows[0][i]]);
				}
			}
		}
		conditions[query.table] = colvals; //  column values
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
		for (var i = 0; i < a.length; i++) {
			for (var j = 0; j < b.length; j++) {
				var l = a[i], r = b[j];
				if (l[0] == r[0]) {
					// both clauses refer to the same attribute
					/*var y = [];
					for (var x in testLiterals) {
						if (testLiterals[x](l[2], r[2])) {
							y.push(x);
						}
					}
					console.log('test clause ' + l[1] + ' ' + r[1] + ' ' + JSON.stringify(y));
					y = [];
					for (var x in testLiterals) {
						if (testLiterals[x](r[2], l[2])) {
							y.push(x);
						}
					}
					console.log('test clause ' + r[1] + ' ' + l[1] + ' ' + JSON.stringify(y));*/

					// walk through all unsat rules for relations
					for (var k = 0; k < equalityCombinations.length; k++) {
						if (l[1] == equalityCombinations[k][0] && r[1] == equalityCombinations[k][1] && testLiterals[equalityCombinations[k][2]](l[2], r[2])) {
							// x ? a && x ? b && a ? b => unsatisfiable
							return false;
						}
						if (r[1] == equalityCombinations[k][0] && l[1] == equalityCombinations[k][1] && testLiterals[equalityCombinations[k][2]](r[2], l[2])) {
							// x ? b && x ? a && b ? a => unsatisfiable
							return false;
						}
					}
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
performQuery('delete from a where x > 2');
performQuery('update a set y=1 where x=4');
performQuery('update a set y=y + 1 where x=2');
performQuery('update a set y=5 where x < 7');
performQuery('update a set y=1 where x < 4');


// TODO: nested select, multiple tables
