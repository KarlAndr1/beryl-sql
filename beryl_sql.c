#include <beryl.h>
#include <sqlite3.h>

#include <assert.h>
#include <string.h>
#include <limits.h>

// https://www.sqlite.org/quickstart.html

// NOTE: Uses beryl_talloc for allocations; use beryl_tfree to free
static char *beryl_str_to_cstr(struct i_val val) {
	assert(BERYL_TYPEOF(val) == TYPE_STR);
	const char *str = beryl_get_raw_str(&val);
	i_size len = BERYL_LENOF(val);
	
	char *cstr = beryl_talloc(len + 1);
	if(cstr == NULL)
		return NULL;
	
	memcpy(cstr, str, len);
	cstr[len] = '\0';
	
	return cstr;
}

struct beryl_sqldb_object {
	struct beryl_object header;
	sqlite3 *db;
};


static void beryl_sqldb_object_free(struct beryl_object *obj) {
	struct beryl_sqldb_object *db_obj = (struct beryl_sqldb_object*) obj;
	sqlite3_close_v2(db_obj->db); // https://www.sqlite.org/c3ref/close.html
}

static void blame_sql_error(int err) {
	const char *msg = sqlite3_errstr(err);
	struct i_val err_str = beryl_new_string(strlen(msg), msg);
	if(BERYL_TYPEOF(err_str) == TYPE_NULL) {
		beryl_blame_arg(BERYL_CONST_STR("Unable to show error message (out of memory, unable to allocate string)"));
		return;
	}
	beryl_blame_arg(err_str);
	beryl_release(err_str);
}

static int bind_i_val_as_sql_param(sqlite3_stmt *stmt, int i, const struct i_val *val) {
	switch(BERYL_TYPEOF(*val)) {
		case TYPE_STR:
			if(BERYL_LENOF(*val) > INT_MAX)
				return SQLITE_TOOBIG;
			return sqlite3_bind_text(stmt, i, beryl_get_raw_str(val), BERYL_LENOF(*val), SQLITE_STATIC);
		
		case TYPE_NULL:
			return sqlite3_bind_null(stmt, i);
		
		case TYPE_NUMBER:
			if(beryl_is_integer(*val))
				return sqlite3_bind_int(stmt, i, beryl_as_num(*val));
			else
				return sqlite3_bind_double(stmt, i, beryl_as_num(*val));
		
		default:
			return sqlite3_bind_text(stmt, i, "Unkown", -1, SQLITE_STATIC);
	}
}

static struct i_val cstr_to_beryl_str(const char *cstr) {
	if(cstr == NULL)
		return BERYL_NULL;

	size_t len = strlen(cstr);
	if(len > I_SIZE_MAX)
		return BERYL_NULL;
	
	return beryl_new_string(len, cstr);
}

static struct i_val create_table_from_row(sqlite3_stmt *stmt, int n_columns, const struct i_val *column_names) {
	if((unsigned int) n_columns > I_SIZE_MAX)
		return BERYL_ERR("Too many columns");
	
	struct i_val table = beryl_new_table(n_columns, true);
	if(BERYL_TYPEOF(table) == TYPE_NULL)
		return BERYL_ERR("Out of memory");
	
	for(int i = 0; i < n_columns; i++) {
		struct i_val column_val;
		switch(sqlite3_column_type(stmt, i)) {
		
			case SQLITE_NULL:
				column_val = BERYL_NULL;
				break;
				
			case SQLITE_INTEGER:
			case SQLITE_FLOAT:
				column_val = BERYL_NUMBER(sqlite3_column_double(stmt, i));
				break;
			
			case SQLITE_TEXT:
			case SQLITE_BLOB: {
				int len = sqlite3_column_bytes(stmt, i);
				if((unsigned) len > I_SIZE_MAX) {
					beryl_release(table);
					return BERYL_ERR("Text/blob too large");
				}
				column_val = beryl_new_string(len, sqlite3_column_blob(stmt, i));
				if(BERYL_TYPEOF(column_val) == TYPE_NULL) {
					beryl_release(table);
					return BERYL_ERR("Out of memory");
				}
			} break;
			
			
			default:
				assert(false);
		}
		
		beryl_table_insert(&table, column_names[i], column_val, false);
	}
	
	return table;
}

static struct i_val beryl_sqldb_object_call(struct beryl_object *obj, const struct i_val *args, i_size n_args) {
	struct beryl_sqldb_object *db_obj = (struct beryl_sqldb_object *) obj;
	if(db_obj->db == NULL)
		return BERYL_ERR("Database has been closed");
	
	if(BERYL_TYPEOF(args[0]) != TYPE_STR) {
		beryl_blame_arg(args[0]);
		return BERYL_ERR("Expected SQL query (a string) as first argument");
	}
	
	i_size n_params = n_args - 1;
	if(n_params > SQLITE_LIMIT_VARIABLE_NUMBER)
		return BERYL_ERR("Too many parameters");
	
	const char *expr = beryl_get_raw_str(&args[0]);
	i_size expr_len = BERYL_LENOF(args[0]);
	const char *expr_end = expr + expr_len;
	
	struct i_val rows = beryl_new_array(0, NULL, 4, false);
	if(BERYL_TYPEOF(rows) == TYPE_NULL)
		return BERYL_ERR("Out of memory");
	
	while(expr != expr_end) {
		sqlite3_stmt *stmt;
		int err = sqlite3_prepare_v2(db_obj->db, expr, expr_end - expr, &stmt, &expr);
		if(err != SQLITE_OK) {
			blame_sql_error(err);
			return BERYL_ERR("SQL compiler error");
		}
		
		for(i_size i = 1; i < n_args; i++) {
			int err = bind_i_val_as_sql_param(stmt, i, &args[i]);
			if(err) {
				sqlite3_finalize(stmt);
				beryl_release(rows);
				blame_sql_error(err);
				return BERYL_ERR("SQL parameter error");
			}
		}
		
		int res;
		int n_columns = sqlite3_column_count(stmt);
		struct i_val *column_names = beryl_talloc(sizeof(struct i_val) * n_columns);
		if(column_names == NULL) {
			sqlite3_finalize(stmt);
			beryl_release(rows);
			return BERYL_ERR("Out of memory");
		}
		for(int i = 0; i < n_columns; i++) {
			const char *column_name_str = sqlite3_column_name(stmt, i);
			column_names[i] = cstr_to_beryl_str(column_name_str);
			
			if(BERYL_TYPEOF(column_names[i]) == TYPE_NULL) {
				for(int j = i - 1; j >= 0; j--)
					beryl_release(column_names[i]);
				beryl_tfree(column_names);
				sqlite3_finalize(stmt);
				beryl_release(rows);
				return BERYL_ERR("Out of memory");
			}
		}
		
		while( (res = sqlite3_step(stmt)) != SQLITE_DONE ) { //Fetch a row
			if(res == SQLITE_BUSY) {
				sqlite3_finalize(stmt);
				beryl_release(rows);
				for(int i = 0; i < n_columns; i++)
					beryl_release(column_names[i]);
				beryl_tfree(column_names);
				return BERYL_ERR("Database is busy (timeout)");
			} else if(res != SQLITE_ROW) {
				sqlite3_finalize(stmt);
				beryl_release(rows);
				for(int i = 0; i < n_columns; i++)
					beryl_release(column_names[i]);
				beryl_tfree(column_names);
				blame_sql_error(res);
				return BERYL_ERR("SQL error");
			}
			
			struct i_val row = create_table_from_row(stmt, n_columns, column_names);
			struct i_val err = BERYL_NULL;
			if(BERYL_TYPEOF(row) == TYPE_ERR)
				err = row;
			else {
				if(!beryl_array_push(&rows, row))
					err = BERYL_ERR("Out of memory");
			}
			
			if(BERYL_TYPEOF(err) == TYPE_ERR) {
				sqlite3_finalize(stmt);
				beryl_release(rows);
				for(int i = 0; i < n_columns; i++)
					beryl_release(column_names[i]);
				return err;
			}
		}
		
		for(int i = 0; i < n_columns; i++)
			beryl_release(column_names[i]);
		
		sqlite3_finalize(stmt);
	}
	return rows;
}

struct beryl_object_class beryl_sqldb_object_class = {
	beryl_sqldb_object_free,
	beryl_sqldb_object_call,
	sizeof(struct beryl_sqldb_object),
	"sqldb",
	sizeof("sqldb") - 1
};

static struct i_val close_callback(const struct i_val *args, i_size n_args) {
	(void) n_args;
	
	if(beryl_object_class_type(args[0]) != &beryl_sqldb_object_class) {
		beryl_blame_arg(args[0]);
		return BERYL_ERR("Expected database object as argument for 'close'");
	}
	
	struct beryl_sqldb_object *obj = (struct beryl_sqldb_object*) beryl_as_object(args[0]);
	
	int err = sqlite3_close(obj->db);
	if(err != SQLITE_OK) {
		return BERYL_ERR("Unable to close database");
	}
	obj->db = NULL;
	
	return BERYL_NULL;
}

static struct i_val open_callback(const struct i_val *args, i_size n_args) {
	(void) n_args;
	
	if(BERYL_TYPEOF(args[0]) != TYPE_STR) {
		beryl_blame_arg(args[0]);
		return BERYL_ERR("Expected string path as first argument for 'sql.open'");
	}
	
	char *path = beryl_str_to_cstr(args[0]);
	if(path == NULL)
		return BERYL_ERR("Out of memory");
	
	sqlite3 *db;
	int err = sqlite3_open(path, &db);
	beryl_tfree(path);
	
	if(err) {
		sqlite3_close(db);
		beryl_blame_arg(args[0]);
		blame_sql_error(err);
		return BERYL_ERR("Unable to open database");
	}
	
	sqlite3_busy_timeout(db, 1000); //1 second is the default timeout
	
	struct i_val db_obj = beryl_new_object(&beryl_sqldb_object_class);
	if(BERYL_TYPEOF(db_obj) == TYPE_NULL) {
		sqlite3_close(db);
		return BERYL_ERR("Out of memory");
	}
	struct beryl_sqldb_object *db_obj_val = (struct beryl_sqldb_object *) beryl_as_object(db_obj);
	db_obj_val->db = db;
	
	return db_obj;
}

static struct i_val get_last_insert_rowid_callback(const struct i_val *args, i_size n_args) {
	(void) n_args;

	if(beryl_object_class_type(args[0]) != &beryl_sqldb_object_class) {
		beryl_blame_arg(args[0]);
		return BERYL_ERR("Expected database object as argument for 'close'");
	}
	
	struct beryl_sqldb_object *obj = (struct beryl_sqldb_object *) beryl_as_object(args[0]);
	
	sqlite3_int64 id = sqlite3_last_insert_rowid(obj->db);
	if(id > BERYL_NUM_MAX_INT)
		return BERYL_ERR("Id out of range");
	
	return BERYL_NUMBER(id);
}

static bool loaded = false;

static struct i_val lib_val;

#define LENOF(a) (sizeof(a)/sizeof(a[0]))


static void init_lib() {
	#define FN(name, arity, fn) { arity, false, name, sizeof(name) - 1, fn }
	static struct beryl_external_fn fns[] = {
		FN("open", 1, open_callback),
		FN("close", 1, close_callback),
		FN("get-last-insert-rowid", 1, get_last_insert_rowid_callback)
		//FN("format", 1, format_callback)
	};
	
	
	struct i_val table = beryl_new_table(LENOF(fns), true);
	if(BERYL_TYPEOF(table) == TYPE_NULL) {
		lib_val = BERYL_ERR("Out of memory");
		return;
	}
	
	for(size_t i = 0; i < LENOF(fns); i++) {
		beryl_table_insert(&table, BERYL_STATIC_STR(fns[i].name, fns[i].name_len), BERYL_EXT_FN(&fns[i]), false);
	}
	
	
	lib_val = table;
}

struct i_val beryl_lib_load() {
	bool ok_version = BERYL_LIB_CHECK_VERSION("0", "0");
	if(!ok_version) {
		return BERYL_ERR("Library `BerylSQL` only works for version 0:0:x");
	}
	
	if(!loaded) {
		init_lib();
		loaded = true;
	}
	return beryl_retain(lib_val);
}
