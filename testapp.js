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
printTable(db.query("CREATE TABLE IF NOT EXISTS person(ID integer PRIMARY KEY AUTO_INCREMENT, Name string COMMENT 'Name of the Person', Age NUMBER DEFAULT 18)"));
printTable(db.query("CREATE TABLE IF NOT EXISTS person(ID integer PRIMARY KEY AUTO_INCREMENT, Name string COMMENT 'Name of the Person', Age NUMBER DEFAULT 18)"));
printTable(db.query("SELECT * FROM tables"));
printTable(db.query("SELECT tables.* FROM tables"));
printTable(db.query("SHOW TABLES"));
printTable(db.query("SELECT * FROM tables as t1, tables as `t2`"));
printTable(db.query("INSERT INTO person(Name, age) VALUES (?, 15), (?, 88)", "Hans", "Anton"));
printTable(db.query("INSERT INTO person(Name, AGE) VALUES (?, ?)", 'Paul', 55));
var hanna = db.query("INSERT INTO person(Name) VALUES ('Hanna')").insert_id;
var getPerson = db.prepare("SELECT * FROM `person` WHERE ID=?");
printTable(db.query(getPerson, hanna));
//printTable(db.query("UPDATE person SET Name='Eva' WHERE id=?", hanna));
printTable(db.query("SELECT * FROM `person`"));
printTable(db.query("SELECT *, ? FROM `person` WHERE age > ?", 12, 30));
