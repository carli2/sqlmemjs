var parser = require('./sqlparser').parser;

console.log(JSON.stringify(parser.parse('SELECT 1+2')));
console.log(JSON.stringify(parser.parse('SELECT 1+2 AS sum')));
console.log(JSON.stringify(parser.parse('SELECT 1+2 as sum')));
console.log(JSON.stringify(parser.parse('SELECT 1 as a, 2 as b')));
console.log(JSON.stringify(parser.parse("SELECT 'Monikas Imbiss'")));
console.log(JSON.stringify(parser.parse("SELECT 'Monika\\'s Imbiss'")));
console.log(JSON.stringify(parser.parse("SELECT *, 12")));
