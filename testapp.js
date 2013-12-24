var SQLinMemory = require('./queryexecutor').SQLinMemory;
var printTable = require('./queryexecutor').printTable;

var db = new SQLinMemory();

console.log(db.query('SELECT 1+2'));
console.log(db.query('SELECT 1+2 AS sum'));
console.log(db.query('SELECT 1+2 as sum'));
console.log(db.query('SELECT 1 as a, 2 as b'));
console.log(db.query("SELECT 'Monikas Imbiss'"));
console.log(db.query("SELECT 'Monika\\'s Imbiss'"));
console.log(db.query("SELECT *, 12"));
printTable(db.query("SELECT * FROM tables"));
