var Parser = require('jison').Parser;

var grammar = {
	lex: {
		"rules": [
		["\\s+", "" /* skop whitespace */],
		// literals
		["[0-9]+", "return 'NUMBER';"],
		["'(\\\\'|.)*?'", "return 'STRING';"],
		// reserved words: this is since JS Regex does not support (?i)
		["[Ss][Ee][Ll][Ee][Cc][Tt]\\b", "return 'SELECT';"],
		["[Aa][Ss]\\b", "return 'AS';"],
		["[Ss][Hh][Oo][Ww]\\s+[Tt][Aa][Bb][Ll][Ee][Ss]\\b", "return 'SHOWTABLES';"],
		["[Cc][Rr][Ee][Aa][Tt][Ee]\\b", "return 'CREATE';"],
		["[Tt][Aa][Bb][Ll][Ee]\\b", "return 'TABLE';"],
		["[Ii][Ff]\\s+[Nn][Oo][Tt]\\s+[Ee][Xx][Ii][Ss][Tt][Ss]\\b", "return 'IFNOTEXISTS';"],
		["[Ff][Rr][Oo][Mm]\\b", "return 'FROM';"],
		// identifiers
		["[a-zA-Z][a-zA-Z_0-9]*", "return 'IDENTIFIER1';"],
		["`.+?`", "return 'IDENTIFIER2';"],
		// symbols
		[",", "return ',';"],
		["\\.", "return '.';"],
		["\\*", "return '*';"],
		["\\+", "return '+';"],
		["-", "return '-';"],
		["\\/", "return '/';"],
		["\\(", "return '(';"],
		["\\)", "return ')';"],
		["$", "return 'EOF';"],
		]
	},
	operators: [
		["left", "+", "-"],
		["left", "*", "/"],
		["left", "^"],
		["left", "UMINUS"]
	],
	bnf: {
		// detecting the type of command
		"expressions":  [["cmd EOF", "return $1;"]],
		"cmd": [["select", "$$ = $1"], ["SHOWTABLES", "$$ = {type: 'select', from: {'table': 'tables'}, expr: ['']}"], ["createtable", "$$ = $1"]],

		// table creation syntax
		"createtable": [["CREATE TABLE IDENTIFIER ( tabrowdefs )", "$$ = {type: 'createtable', id: $3, cols: $5, erroronexists: true};"], ["CREATE TABLE IFNOTEXISTS IDENTIFIER ( tabrowdefs )", "$$ = {type: 'createtable', id: $4, cols: $6};"]],
		"tabrowdefs": [["", "$$ = [];"], ["tabrowdef", "$$ = [$1];"], ["tabrowdefs , tabrowdef", "$$ = $1; $$.push($3);"]],
		"tabrowdef": [["IDENTIFIER IDENTIFIER", "$$ = [$1, $2];"]],

		// syntax of select
		"select1": [["SELECT cols", "$$ = {type: 'select', expr: $2};"]],
		"select2": [["select1", "$$ = $1"], ["select1 FROM tables", "$$ = $1; $$.from = $3;"]],
		"select": [["select2", "$$ = $1;"]],

		"col": [["e AS IDENTIFIER", "$$ = [$3, $1];"], ["e", "$$ = [yytext, $1];"],
			["*", "$$ = '';"], ["IDENTIFIER . *", "$$ = $1;"]],
		"cols": [["col", "$$ = [$1];"], ["cols , col", "$$ = $1; $$.push($3);"]],

		"table": [["IDENTIFIER AS IDENTIFIER", "$$ = {}; $$[$3] = $1;"], ["IDENTIFIER", "$$ = {}; $$[$1] = $1;"]],
		"tables": [["table", "$$ = $1;"], ["tables , IDENTIFIER AS IDENTIFIER", "$$ = $1; $$[$5] = $3;"], ["tables , IDENTIFIER", "$$ = $1; $$[$3] = $3;"]],

		// expressions and conditions
		"e": [
			["e + e", "$$ = {op: 'add', a: $1, b: $3};"],
			["e - e", "$$ = {op: 'sub', a: $1, b: $3};"],
			["e * e", "$$ = {op: 'mul', a: $1, b: $3};"],
			["e / e", "$$ = {op: 'div', a: $1, b: $3};"],
			["- e", "$$ = {op: 'neg', a: $2};",  {prec: "UMINUS"}],
			["( e )", "$$ = $2;"],
			["NUMBER", "$$ = Number(yytext);"],
			["STRING", "$$ = eval(yytext);"],
			["IDENTIFIER", "$$ = {id: $1};"],
			["IDENTIFIER . IDENTIFIER", "$$ = {id: $1+'.'+$3};"]
		],
		"IDENTIFIER": [
			["IDENTIFIER1", "$$ = $1;"],
			["IDENTIFIER2", "$$ = $1.substring(1, $1.length-1);"]
		]
	}
};

var parser = new Parser(grammar);

require('fs').writeFile('sqlparser.js', parser.generate());
