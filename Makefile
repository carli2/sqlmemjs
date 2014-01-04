all: dist doc

dist: sqlinmem.js sqlinmem.min.js dist/doc
	cat license.txt sqlinmem.js > dist/sqlinmem.js
	cat license.txt sqlinmem.min.js > dist/sqlinmem.min.js

dist/doc: queryexecutor.js
	`npm bin`/jsdoc -d dist/doc queryexecutor.js

doc: queryexecutor.js
	`npm bin`/jsdoc -p -d doc queryexecutor.js

sqlinmem.js: sqlparser.js queryexecutor.js
	cat sqlparser.js queryexecutor.js |sed "s|var parser = require('./sqlparser').parser;||" > sqlinmem.js

sqlinmem.min.js: sqlinmem.js
	`npm bin`/minify sqlinmem.js sqlinmem.min.js

sqlparser.js: sqlspec.js
	node sqlspec.js

deps:
	npm install jison minify jsdoc

run: sqlparser.js
	node testapp.js
