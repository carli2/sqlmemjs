SQL inMemory.js
===============

SQL inMemory.js is a full SQL engine written in JavaScript.

You can test it live on http://launix.de/sqlmemjs/

Download:

- http://launix.de/sqlmemjs/sqlinmem.js
- http://launix.de/sqlmemjs/sqlinmem.min.js

Funding it
----------

Please help this project by either funding http://www.indiegogo.com/projects/sql-inmemory-js/ or use the donate button under http://launix.de/donate.html

Planned features
----------------

- provide a full SQL engine
- prepared statements or queries
- full support for SELECT FROM INNER OUTER JOIN WHERE GROUP BY HAVING SORT BY and subqueries
- index joins
- runs in browser and on node.js
- safe way to provide SQL shell to web users

Build instructions
------------------

1. Install dependencies (<tt>make deps</tt>)
2. Compile the parser and the minified library (<tt>make</tt>)
3. Run the test cases (<tt>make run</tt>)

How to use
----------

```
var db = new SQLinMemory();
db.query("CREATE TABLE x(id integer PRIMARY KEY AUTO_INCREMENT, name string DEFAULT 'New Item')");
var id = db.query("INSERT INTO x(name) VALUES ('Carl')").insert_id;
db.query("INSERT INTO x(name) VALUES (?), (?)", "Peter", "Paul");

var rows = db.query("SELECT * FROM x");
var row;
console.log(JSON.stringify(rows.getSchema()));
while(row = rows.fetch()) {
  console.log(JSON.stringify(row));
}

var getX = db.prepare("SELECT * FROM x where id=?");
var carl = db.query(getX, id);
console.log(JSON.stringify(carl.fetch()));
carl.close();
```

The API allows the following operations:

- <tt>var db = new SQLinMemory();</tt> - creates the database
- <tt>db.prepare(sql)</tt> - compiles the sql string into a easier to process representation
- <tt>var table = db.query(sql\_or\_prepared, param1, param2)</tt> - executes the query and returns a table iterator
- <tt>table.getSchema()</tt> - returns an array of all columns where a column has the form ['IDENTIFIER', 'TYPE']
- <tt>table.fetch()</tt> - returns the next tuple or undefined when the table end is reached
- <tt>table.reset()</tt> - start reading again (you can do that at any point of time)
- <tt>table.close()</tt> - release all cursors (do not fetch after close; but you can reset; close is necessary when not fetching all data and unnecessary when fetch() once returned undefined)
- <tt>table.toArray()</tt> - fetches all tuples and returns them as a JavaScript Array
- <tt>table.printTable(printLine)</tt> - prints the table in human readable form (leave printLine blank to use console.log)
- <tt>db.exportJSON()</tt> - returns a compressed JSON object of all data stored
- <tt>db.importJSON(json)</tt> - imports the previously exported tables and overrides existing tables

Supported Commands
------------------

- SHOW TABLES
- CREATE TABLE table(col1 type1 PRIMARY KEY AUTO\_INCREMENT, col2 type2 DEFAULT value)
- DROP TABLE [IF EXISTS] table
- INSERT INTO table(col1, col2) VALUES (val11, val12), (val21, val22)
- UPDATE table SET col1=val1, col2=val2
- UPDATE table SET col1=val1, col2=val2 WHERE condition
- DELETE FROM table WHERE condition
- SELECT \* FROM table
- SELECT \* FROM table1, table2
- SELECT table1.\*, table2.col FROM table1, table2
- SELECT col1, col2+col3 FROM table
- SELECT * FROM table WHERE ID=?
- SELECT 1/2 UNION SELECT 1+2
- SELECT a+b FROM (SELECT 1 as a, 2 as b)
- SELECT 1+(SELECT 2+3)

Supported Data Types
--------------------

- INTEGER
- FLOAT
- DOUBLE
- NUMBER
- TEXT
- STRING


