var Parser = require('jison').Parser;

var grammar = {
	lex: {
		"rules": [
		["\\s+", "" /* skop whitespace */],
		["[0-9]+", "return 'NUMBER';"],
		["'(\\\\'|.)*?'", "return 'STRING';"],
		// this is since JS Regex does not support (?i)
		["[Ss][Ee][Ll][Ee][Cc][Tt]", "return 'SELECT';"],
		["[Aa][Ss]", "return 'AS';"],
		["[Ff][Rr][Oo][Mm]", "return 'FROM';"],
		["[a-zA-Z][a-zA-Z_0-9]*", "return 'IDENTIFIER';"],
		[",", "return ',';"],
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
		"expressions":  [["cmd EOF", "return $1;"]],
		"cmd": [["select", "$$ = $1"]],
		"select1": [["SELECT cols", "$$ = {type: 'select', expr: $2};"]],
		"select2": [["select1", "$$ = $1"], ["select1 FROM IDENTIFIER", "$$ = $1; $$.from = $3;"]],
		"select": [["select2", "$$ = $1;"]],
		"col": [["e AS IDENTIFIER", "$$ = [$3, $1];"], ["e", "$$ = [yytext, $1];"]],
		"cols": [["col", "$$ = [$1];"], ["*", "$$ = ['all'];"], ["cols , col", "$$ = $1; $$.push($3);"], ["cols , *", "$$ = $1; $$.push('all');"]],
		"e": [
			["e + e", "$$ = {op: 'add', a: $1, b: $3};"],
			["e - e", "$$ = {op: 'sub', a: $1, b: $3};"],
			["e * e", "$$ = {op: 'mul', a: $1, b: $3};"],
			["e / e", "$$ = {op: 'div', a: $1, b: $3};"],
			["- e", "$$ = {op: 'neg', a: $2};",  {prec: "UMINUS"}],
			["( e )", "$$ = $2;"],
			["NUMBER", "$$ = Number(yytext);"],
			["STRING", "$$ = eval(yytext);"]
		]
	}
};

var parser = new Parser(grammar);

require('fs').writeFile('sqlparser.js', parser.generate());
