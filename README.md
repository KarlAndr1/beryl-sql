# Building & Installing
Make sure that lib-sqlite3 is installed!
Build the project:
```
make
```

Then install it into the Beryl home directory (requires Beryl to be installed!):
```
make install
```

# Using
	let sql = require "~/sql"
	
	let db = sql :open "./my-database.sqlite"
	db "INSERT INTO my_table (a, b, c) VALUES (?1, ?2, ?3)" 1 2 3
	
	sql :close db
