var Parser = require('jison').Parser;

var grammar = {
	lex: {
		"rules": [
		["\\s+", "" /* skop whitespace */],
		// literals
		["[0-9]+", "return 'NUMBER';"],
		["'(\\\\'|.)*?'", "return 'STRINGX';"],
		// reserved words: this is since JS Regex does not support (?i)
		["[Ss][Ee][Ll][Ee][Cc][Tt]\\b", "return 'SELECT';"],
		["[Uu][Nn][Ii][Oo][Nn]\\b", "return 'UNION';"],
		["[Aa][Ss]\\b", "return 'AS';"],
		["[Ss][Hh][Oo][Ww]\\s+[Tt][Aa][Bb][Ll][Ee][Ss]\\b", "return 'SHOWTABLES';"],
		["[Cc][Rr][Ee][Aa][Tt][Ee]\\b", "return 'CREATE';"],
		["[Dd][Rr][Oo][Pp]\\b", "return 'DROP';"],
		["[Tt][Aa][Bb][Ll][Ee]\\b", "return 'TABLE';"],
		// TODO: view
		// TODO: index
		["[Ii][Ff]\\s+[Nn][Oo][Tt]\\s+[Ee][Xx][Ii][Ss][Tt][Ss]\\b", "return 'IFNOTEXISTS';"],
		["[Ii][Ff]\\s+[Ee][Xx][Ii][Ss][Tt][Ss]\\b", "return 'IFEXISTS';"],
		["[Ff][Rr][Oo][Mm]\\b", "return 'FROM';"],
		// TODO: {inner, left, right, full} join
		["[Ii][Nn][Ss][Ee][Rr][Tt]\\s+[Ii][Nn][Tt][Oo]\\b", "return 'INSERTINTO';"],
		["[Vv][Aa][Ll][Uu][Ee][Ss]\\b", "return 'VALUES';"],
		["[Dd][Ee][Ff][Aa][Uu][Ll][Tt]\\b", "return 'DEFAULT';"],
		["[Pp][Rr][Ii][Mm][Aa][Rr][Yy]\\s+[Kk][Ee][Yy]\\b", "return 'PRIMARYKEY';"],
		["[Aa][Uu][Tt][Oo]_[Ii][Nn][Cc][Rr][Ee][Mm][Ee][Nn][Tt]\\b", "return 'AUTO_INCREMENT';"],
		["[Cc][Oo][Mm][Mm][Ee][Nn][Tt]\\b", "return 'COMMENT';"],
		["[Uu][Pp][Dd][Aa][Tt][Ee]\\b", "return 'UPDATE';"],
		["[Ss][Ee][Tt]\\b", "return 'SET';"],
		["[Dd][Ee][Ll][Ee][Tt][Ee]\\b", "return 'DELETE';"],
		["[Ww][Hh][Ee][Rr][Ee]\\b", "return 'WHERE';"],
		// TODO: GROUP BY
		// TODO: HAVING
		// TODO: ORDER BY
		["[Nn][Oo][Tt]\\b", "return 'NOT';"],
		["[Aa][Nn][Dd]\\b", "return 'AND';"],
		// TODO: Between
		// TODO: like
		["[Oo][Rr]\\b", "return 'OR';"],
		// identifiers
		["[a-zA-Z][a-zA-Z_0-9]*", "return 'IDENTIFIER1';"],
		["`.+?`", "return 'IDENTIFIER2';"],
		// symbols
		[",", "return ',';"],
		["\\.", "return '.';"],
		["\\*", "return '*';"],
		["\\+", "return '+';"],
		["-", "return '-';"],
		["=", "return '=';"],
		["<>", "return '<>';"],
		[">", "return '>';"],
		[">=", "return '>=';"],
		["<", "return '<';"],
		["<=", "return '<=';"],
		["\\/", "return '/';"],
		["\\(", "return '(';"],
		["\\)", "return ')';"],
		["\\?", "return '?';"],
		["$", "return 'EOF';"],
		]
	},
	operators: [
		["left", "UNION"],
		["left", "AND"],
		["left", "OR"],
		["left", "NOT"],
		["left", "=", "<>", "<", "<=", ">", ">="],
		["left", "+", "-"],
		["left", "*", "/"],
		["left", "^"],
		["left", "UMINUS"]
	],
	bnf: {
		// detecting the type of command
		"expressions":  [["cmd EOF", "return $1;"]],
		"cmd": [
			["select", "$$ = $1;"],
			["SHOWTABLES", "$$ = {type: 'select', from: {'table': 'tables'}, expr: ['']};"],
			["createtable", "$$ = $1;"],
			["droptable", "$$ = $1;"],
			["insert", "$$ = $1;"],
			["delete", "$$ = $1;"],
			["update", "$$ = $1;"]
		],

		// table creation syntax
		"createtable": [["CREATE TABLE IDENTIFIER ( tabrowdefs )", "$$ = {type: 'createtable', id: $3, cols: $5, erroronexists: true};"], ["CREATE TABLE IFNOTEXISTS IDENTIFIER ( tabrowdefs )", "$$ = {type: 'createtable', id: $4, cols: $6};"]],
		"tabrowdefs": [["", "$$ = [];"], ["tabrowdef", "$$ = [$1];"], ["tabrowdefs , tabrowdef", "$$ = $1; $$.push($3);"]],
		"tabrowdef": [
				["IDENTIFIER IDENTIFIER", "$$ = {id: $1, type: $2};"],
				["tabrowdef DEFAULT e", "$$ = $1; $$.default = $3;"],
				["tabrowdef PRIMARYKEY", "$$ = $1; $$.primary = true;"],
				["tabrowdef AUTO_INCREMENT", "$$ = $1; $$.auto_increment = 1;"],
				["tabrowdef COMMENT STRING", "$$ = $1; $$.comment = $3;"]
		],
		"droptable": [["DROP TABLE IDENTIFIER", "$$ = {type: 'droptable', id: $3};"], ["DROP TABLE IFEXISTS IDENTIFIER", "$$ = {type: 'droptable', id: $4, noerror: true};"]],

		// insert syntax
		"insert": [
			["INSERTINTO IDENTIFIER ( idlist ) VALUES insertrows", "$$ = {type: 'insert', table: $2, cols: $4, rows: $7};"],
			["INSERTINTO IDENTIFIER ( idlist ) select", "$$ = {type: 'insert', table: $2, cols: $4, select: $6};"],
		],
		"idlist": [["", "$$ = [];"], ["IDENTIFIER", "$$ = [$1];"], ["idlist , IDENTIFIER", "$$ = $1; $$.push($3);"]],
		"insertrows": [["insertrow", "$$ = [$1];"], ["insertrows , insertrow", "$$ = $1; $$.push($3);"]],
		"insertrow": [["( valuelist )", "$$ = $2;"]],
		"valuelist": [["e", "$$ = [$1];"], ["valuelist , e", "$$ = $1; $$.push($3);"]],

		// update syntax
		"update": [
			["UPDATE IDENTIFIER updateset", "$$ = {type: 'update', table: $2, set: $3};"],
			["update WHERE c", "$$ = $1; $$.where = $3;"]
			],
		"updateset": [["SET IDENTIFIER = e", "$$ = {}; $$[$2] = $4;"], ["updateset , IDENTIFIER = e", "$$ = $1; $$[$3] = $5;"]],

		// delete syntax
		"delete": [
			["DELETE FROM IDENTIFIER", "$$ = {type: 'delete', table: $3};"],
			["DELETE * FROM IDENTIFIER", "$$ = {type: 'delete', table: $4};"],
			["delete WHERE c", "$$ = $1; $$.where = $3;"]
		],

		// syntax of select
		"select1": [["SELECT cols", "$$ = {type: 'select', expr: $2};"]],
		"select2": [["select1", "$$ = $1;"], ["select1 FROM tables", "$$ = $1; $$.from = $3;"]],
		"select3": [["select2", "$$ = $1;"], ["select2 WHERE c", "$$ = $1; $$.where = $3;"]],
		// TODO: GROUP BY
		// TODO: HAVING
		// TODO: ORDER BY
		// TODO: LIMIT
		"select": [["select3", "$$ = $1;"], ["select UNION select", "$$ = {type: 'union', a: $1, b: $3};"]],

		"col": [["e AS IDENTIFIER", "$$ = [$3, $1];"], ["e", "$$ = ['', $1];"],
			["*", "$$ = '';"], ["IDENTIFIER . *", "$$ = $1;"]],
		"cols": [["col", "if($1[0] === '') $1[0] = 'col0'; $$ = [$1];"], ["cols , col", "$$ = $1; if($3[0] === '') $3[0] = 'col'+$$.length; $$.push($3);"]],

		"table": [["IDENTIFIER AS IDENTIFIER", "$$ = {id: $3, tab: $1};"], ["IDENTIFIER", "$$ = {id: $1, tab: $1}; $$[$1] = $1;"], ["( select )", "$$ = {id: 'inner_table', tab: $2};"], ["( select ) AS IDENTIFIER", "$$ = {id: $5, tab: $2};"]],
		"tables": [["table", "$$ = {}; $$[$1.id] = $1.tab;"], ["tables , table", "$$ = $1; $$[$3.id] = $3.tab;"]],

		// expressions and conditions
		"e": [
			["e + e", "$$ = {op: 'add', a: $1, b: $3};"],
			["e - e", "$$ = {op: 'sub', a: $1, b: $3};"],
			["e * e", "$$ = {op: 'mul', a: $1, b: $3};"],
			["e / e", "$$ = {op: 'div', a: $1, b: $3};"],
			["- e", "$$ = {op: 'neg', a: $2};",  {prec: "UMINUS"}],
			["( e )", "$$ = $2;"],
			["NUMBER", "$$ = Number(yytext);"],
			["STRING", "$$ = $1;"],
			["IDENTIFIER", "$$ = {id: $1};"],
			["IDENTIFIER . IDENTIFIER", "$$ = {id: $1+'.'+$3};"],
			["( select )", "$$ = {nest: $2};"],
			["?", "$$ = {wildcard: true};"]
		],
		"c": [
			["e = e", "$$ = {cmp: '=', a: $1, b: $3};"],
			["e <> e", "$$ = {cmp: '<>', a: $1, b: $3};"],
			["e < e", "$$ = {cmp: '<', a: $1, b: $3};"],
			["e <= e", "$$ = {cmp: '<=', a: $1, b: $3};"],
			["e > e", "$$ = {cmp: '>', a: $1, b: $3};"],
			["e >= e", "$$ = {cmp: '>=', a: $1, b: $3};"],
			["c AND c", "$$ = {op: 'and', a: $1, b: $3};"],
			["c OR c", "$$ = {op: 'or', a: $1, b: $3};"],
			["NOT c", "$$ = {op: 'not', a: $2};"]
		],
		"IDENTIFIER": [
			["IDENTIFIER1", "$$ = $1;"],
			["IDENTIFIER2", "$$ = $1.substring(1, $1.length-1);"]
		],
		"STRING": [["STRINGX", "$$ = eval(yytext)"]]
	}
};

var parser = new Parser(grammar);

require('fs').writeFile('sqlparser.js', parser.generate());
