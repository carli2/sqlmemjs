var sql = require('./queryexecutor');
var SQLinMemory = sql.SQLinMemory;

var db = new SQLinMemory();

db.query('SELECT 1+2').assert([{col1: 3}]);
db.query('SELECT 1+2 -- this is a comment').assert([{col1: 3}]);
db.query('SELECT 1--this is a comment\n+2 -- this is a comment').assert([{col1: 3}]);
db.query('SELECT 1+2; something').assert([{col1: 3}]);
db.query('SELECT 1+2, 3').assert([{col1: 3, col2: 3}]);
db.query('SELECT 1+2 AS sum').assert([{sum: 3}]);
db.query('SELECT 2+2*2 as sum').assert([{sum: 6}]);
db.query('SELECT -1*3 as a, 3/4 as b').assert([{a: -1*3, b: 3/4}]);
db.query('SELECT a+b FROM (SELECT -1*3 as a, 3/4 as b) as t').assert([{col1: -1*3+3/4}]);
db.query('SELECT t.a+t.b FROM (SELECT -1*3 as a, 3/4 as b) AS t').assert([{col1: -1*3+3/4}]);
db.query('SELECT t.g.a+t.g.b FROM (SELECT * FROM (SELECT -1*3 as a, 3/4 as b) as g) AS t').assert([{col1: -1*3+3/4}]);
db.query('SELECT a+b FROM (SELECT * FROM (SELECT -1*3 as a, 3/4 as b) as t) as g').assert([{col1: -1*3+3/4}]);
db.query("SELECT 1+(select 2+3)").assert([{col1: 6}]);
db.query('SELECT sqrt(?)', 9).assert([{col1: 3}]);
db.query('select 1+2 where 0 between 1 and 3').assert([]);
db.query("SELECT 'Monikas Imbiss' UNION SELECT 'abc123'").assert([{col1: 'Monikas Imbiss'}, {col1: 'abc123'}]);
db.query("SELECT 'Monika\\'s Imbiss'").assert([{col1: 'Monika\'s Imbiss'}]);
db.query("CREATE TABLE IF NOT EXISTS person(ID integer PRIMARY KEY AUTO_INCREMENT, Name string COMMENT 'Name of the Person', Age NUMBER DEFAULT 18)").assert([{VALUE: 'person'}]);
db.query("CREATE TABLE IF NOT EXISTS person(ID integer PRIMARY KEY AUTO_INCREMENT, Name string COMMENT 'Name of the Person', Age NUMBER DEFAULT 18)").printTable();
db.query("DESC person").printTable();
db.query("SELECT * FROM tables").printTable();
db.query("SELECT tables.* FROM tables").printTable();
db.query("SHOW TABLES").printTable();
db.query("DESCRIBE TABLES").printTable();
db.query("SELECT * FROM tables as t1, tables as `t2`").printTable();
db.query("INSERT INTO person(Name, age) VALUES (?, 15), (?, 88)", "Hans", "Anton").printTable();
db.query("INSERT INTO person(Name, AGE) VALUES (?, ?)", 'Paul', 55).printTable();
var hanna = db.query("INSERT INTO person(Name) VALUES ('Hanna')").insert_id;
var exported = db.exportJSON();

console.log('');
console.log(' ---- exporting/importing ---- ');
console.log(JSON.stringify(exported));
console.log('');

db = new SQLinMemory();
db.importJSON(exported);
var getPerson = db.prepare("SELECT * FROM `person` WHERE ID=?");
db.query(getPerson, hanna).printTable();
db.query("UPDATE person SET Name='Eva', Age = Age+1 WHERE id=?", hanna).printTable();
db.query("SELECT * FROM `person`").printTable();
db.query("SELECT *, ? FROM `person` WHERE age > ?", 12, 30).printTable();
db.query("SELECT Name, (SELECT age+?) as nextage FROM `person`", 2).printTable();
db.query("DELETE * FROM `person` WHERE id=?", hanna).printTable();
db.query("INSERT INTO person(Name, Age) SELECT Name, Age+? FROM person WHERE Age < 100", 20);
db.query("SELECT * FROM `person`").printTable();
db.query("SELECT * FROM `person` LIMIT 300").printTable();
db.query("SELECT * FROM `person` LIMIT 3").printTable();
db.query("SELECT * FROM `person` LIMIT ?, 1", 3).printTable();
db.query("SELECT * FROM `person` LIMIT 3, 100").printTable();
db.query("SELECT * FROM `person` ORDER BY Age").printTable();
db.query("SELECT * FROM `person` ORDER BY Age DESC").printTable();
db.query("SELECT * FROM `person` ORDER BY Name, Age").printTable();

db.query("CREATE TABLE sums(category integer, value number)");
db.query("INSERT INTO SUMS(category, value) VALUES(1, 1), (2,5),(1,9),(3,10)");
db.query("SELECT * FROM `sums`").printTable();
db.query("SELECT SUM(value), MIN(value), MAX(value) FROM `sums`").printTable();
db.query("SELECT category, SUM(value), COUNT(value), AVG(value) FROM `sums` GROUP BY category").printTable();
db.query("SELECT category, SUM(value) AS SUMME FROM `sums` GROUP BY category HAVING SUMME > 7").printTable();

// interactive SQL shell
console.log('testing interactive SQL shell');
var shell = db.openShell(function(cursor) {
	cursor.printTable();
});
shell.write("-- starting\n");
shell.write("SEL");
shell.write("ECT 1;");
shell.write("/* such things occur in mysql dumps */;\n");
shell.write("SELECT 2;SELECT 3");
shell.close();
