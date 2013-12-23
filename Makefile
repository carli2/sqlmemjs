all: sqlparser.js

sqlparser.js: sqlspec.js
	node sqlspec.js

run: sqlparser.js
	node testapp.js
