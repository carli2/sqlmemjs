SQL inMemory.js
===============

SQL inMemory.js is a full SQL engine written in JavaScript.

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

Supported Commands
------------------

- SHOW TABLES
- CREATE TABLE table(col1 type1, col2 type2)
- INSERT INTO table(col1, col2) VALUES (val11, val12), (val21, val22)
- SELECT \* FROM table
- SELECT \* FROM table1, table2
- SELECT table1.\*, table2.col FROM table1, table2
- SELECT col1, col2+col3 FROM table

Supported Data Types
--------------------

- INTEGER
- FLOAT
- DOUBLE
- NUMBER
- TEXT
- STRING


