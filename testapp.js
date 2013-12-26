var sql = require('./queryexecutor');
var SQLinMemory = sql.SQLinMemory;
var printTable = sql.printTable;

var db = new SQLinMemory();

printTable(db.query('SELECT 1+2'));
printTable(db.query('SELECT 1+2 AS sum'));
printTable(db.query('SELECT 1+2 as sum'));
printTable(db.query('SELECT 1 as a, 2 as b'));
printTable(db.query("SELECT 'Monikas Imbiss'"));
printTable(db.query("SELECT 'Monika\\'s Imbiss'"));
printTable(db.query("CREATE TABLE person(ID integer, Name string)"));
printTable(db.query("SELECT * FROM tables"));
printTable(db.query("SELECT tables.* FROM tables"));
printTable(db.query("SHOW TABLES"));
printTable(db.query("SELECT * FROM tables as t1, tables as `t2`"));
