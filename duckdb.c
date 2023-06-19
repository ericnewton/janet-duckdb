#include <duckdb.h>
#include <janet.h>

typedef struct {
    duckdb_database handle;
} Database;

typedef struct {
    duckdb_connection handle;
} Connection;

static int database_gc(void *p, size_t s) {
    (void)s;
    Database * duckDb = (Database *)p;
    duckdb_close(&duckDb->handle);
    return 0;
}

static int database_get(void *p, Janet key, Janet *out);
static const JanetAbstractType database_type = {
    "duckdb.database",
    database_gc,
    NULL,
    database_get
};

static int connection_gc(void *p, size_t s) {
    (void)s;
    Connection * conn = (Connection *)p;
    duckdb_disconnect(&conn->handle);
    return 0;
}

static int connection_get(void *p, Janet key, Janet *out);
static const JanetAbstractType connection_type = {
    "duckdb.connection",
    connection_gc,
    NULL,
    connection_get
};

static Janet database_open(int32_t argc, Janet *argv) {
    janet_arity(argc, 0, 1);
    const char * path = NULL;
    if (argc == 1) {
	path = janet_getcstring(argv, 0);
    }
    Database * duckDb = (Database*)janet_abstract(&database_type, sizeof(Database));
    char * err;
    if (duckdb_open_ext(path, &duckDb->handle, NULL, &err) == DuckDBError) {
	janet_panicf("unable to open database %s: %s", path ? path : "<empty>", err);
    }
    return janet_wrap_abstract(duckDb);
}

static Janet database_close(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    Database * db = (Database*) janet_abstract(&database_type, sizeof(Database));
    duckdb_close(&db->handle);
    return janet_wrap_nil();
}

static Janet database_connect(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    Database * db = (Database*) janet_getabstract(argv, 0, &database_type);
    Connection * duckConn = (Connection*)janet_abstract(&connection_type, sizeof(Connection));
    if (duckdb_connect(db->handle, &duckConn->handle) == DuckDBError) {
	janet_panic("unable to connect to database");
    }
    return janet_wrap_abstract(duckConn);
}

static JanetMethod database_methods[] = {
    {"close", database_close},
    {"connect", database_connect},
    {NULL, NULL}
};

static int database_get(void *p, Janet key, Janet *out) {
    (void)p;
    if (!janet_checktype(key, JANET_KEYWORD)) {
	janet_panicf("expected keyword, got %v", key);
    }
    return janet_getmethod(janet_unwrap_keyword(key), database_methods, out);
}

static Janet connection_disconnect(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 1);
    Connection * c = (Connection*) janet_getabstract(argv, 0, &connection_type);
    duckdb_disconnect(&c->handle);
    return janet_wrap_nil();
}

static void bind1(duckdb_prepared_statement stmt, int32_t index, Janet arg) {
    duckdb_state res;
    switch (janet_type(arg)) {
    default:
	janet_panicf("error binding %v at index %d: invalid type (%t) for bind, needs %T",
		     arg,
		     index,
		     arg,
		     duckdb_param_type(stmt, index));
	return;
    case JANET_NIL:
	res = duckdb_bind_null(stmt, index);
	break;
    case JANET_NUMBER:
	res = duckdb_bind_double(stmt, index, janet_unwrap_number(arg));
	break;
    case JANET_BOOLEAN:
	res = duckdb_bind_boolean(stmt, index, janet_unwrap_boolean(arg) != 0);
	break;
    case JANET_STRING:
    case JANET_SYMBOL:
    case JANET_KEYWORD:
	{
	    const uint8_t *str = janet_unwrap_string(arg);
	    int32_t len = janet_string_length(str);
	    res = duckdb_bind_varchar_length(stmt, index, (const char*)str, len);
	}
	break;
    case JANET_BUFFER:
	{
	    JanetBuffer *buffer = janet_unwrap_buffer(arg);
	    res = duckdb_bind_blob(stmt, index, buffer->data, buffer->count);
	}
	break;
    }
    if (res == DuckDBError) {
	if (duckdb_param_type(stmt, index) == DUCKDB_TYPE_INVALID) {
	    janet_panicf("error binding %v (type %t) at index %d: "
			 "bind has no known type, statement is probably invalid",
			 arg, arg, index);
	}
	duckdb_type needed = duckdb_param_type(stmt, index);
	duckdb_destroy_prepare(&stmt);
	janet_panicf("error binding %v at index %d: needs %T was given a %t",
		     arg,
		     index,
		     needed,
		     arg);
    }
}

static int bind(duckdb_prepared_statement stmt, int32_t argc, Janet *argv, int offset) {
    int n = duckdb_nparams(stmt);
    if (offset + n > argc) {
	janet_panicf("too few bind parameters: given %d, need %d", argc - offset, n);
	return offset;
    }
    for (int32_t i = 0; offset + i < argc && i < n; i++) {
	/* important: bind indexes are 1 based */
	bind1(stmt, i + 1, argv[offset + i]);
    }
    return offset + n;
}

static int execute_extracted_statement(duckdb_connection c,
				       duckdb_extracted_statements statements,
				       idx_t statement_count,
				       idx_t index,
				       int32_t argc,
				       Janet *argv,
				       int32_t bind_offset,
				       duckdb_result *result_out) {
    duckdb_prepared_statement prepared_statement;
    if (duckdb_prepare_extracted_statement(c, statements, index, &prepared_statement) == DuckDBError) {
	JanetString err = janet_cstring(duckdb_prepare_error(prepared_statement));
	duckdb_destroy_prepare(&prepared_statement);
	janet_panicf("unable to prepare statement %d: %V", index + 1, janet_wrap_string(err));
    }
    bind_offset = bind(prepared_statement, argc, argv, bind_offset);
    if (duckdb_execute_prepared(prepared_statement, result_out) == DuckDBError) {
	JanetString err = janet_cstring(duckdb_result_error(result_out));
	duckdb_destroy_result(result_out);
	janet_panicf("unable to execute statement %d: %v", index + 1, janet_wrap_string(err));
    }
    duckdb_destroy_prepare(&prepared_statement);
    return bind_offset;
}

static Janet connection_eval(int32_t argc, Janet *argv) {
    janet_arity(argc, 2, -1);
    Connection * c = (Connection*) janet_getabstract(argv, 0, &connection_type);
    const char * query = janet_getcstring(argv, 1);
    duckdb_extracted_statements statements;
    idx_t statement_count = duckdb_extract_statements(c->handle, query, &statements);
    if (statement_count == 0) {
	JanetString err = janet_cstring(duckdb_extract_statements_error(statements));
	duckdb_destroy_extracted(&statements);
	janet_panicv(janet_wrap_string(err));
    }
    int bind_offset = 2;
    idx_t last = statement_count - 1;
    for (idx_t i = 0; i < last; i++) {
	duckdb_result result;
	bind_offset = execute_extracted_statement(c->handle, statements, statement_count, i, argc, argv, bind_offset, &result);
	duckdb_destroy_result(&result);
    }
    duckdb_result result;
    bind_offset = execute_extracted_statement(c->handle, statements, statement_count, last, argc, argv, bind_offset, &result);
    
    if (bind_offset != argc) {
	janet_panicf("not all arguments were bound to statements: bound %d of %d",
		    bind_offset - 2,
		    argc - 2);
    }

    const idx_t column_count = duckdb_column_count(&result);
    Janet *tupstart = janet_tuple_begin(column_count);
    for (idx_t i = 0; i < column_count; i++) {
        tupstart[i] = janet_ckeywordv(duckdb_column_name(&result, i));
    }
    const Janet *colnames = janet_tuple_end(tupstart);
    
    const idx_t row_count = duckdb_row_count(&result);
    JanetArray *rows = janet_array(row_count);
    for(idx_t row = 0; row < row_count; row++) {
	JanetKV *row_values = janet_struct_begin(column_count);
	for(idx_t col = 0; col < column_count; col++) {
	    Janet value;
	    if (duckdb_value_is_null(&result, col, row)) {
		value = janet_wrap_nil();
	    } else {
		duckdb_type type = duckdb_column_type(&result, col);
		switch (type) {
		default:
		    {
			janet_panicf("unknown conversion for data type (col %d, row %d)",
				     col, row);
			return janet_wrap_nil();
		    }
		case DUCKDB_TYPE_TINYINT:
		    value = janet_wrap_integer(duckdb_value_int8(&result, col, row));
		    break;
		case DUCKDB_TYPE_SMALLINT:
		    value = janet_wrap_integer(duckdb_value_int16(&result, col, row));
		    break;
		case DUCKDB_TYPE_INTEGER:
		    value = janet_wrap_integer(duckdb_value_int32(&result, col, row));
		    break;
		case DUCKDB_TYPE_BIGINT:
		    value = janet_wrap_number(duckdb_value_int64(&result, col, row));
		    break;
		case DUCKDB_TYPE_UTINYINT:
		    value = janet_wrap_integer(duckdb_value_uint8(&result, col, row));
		    break;
		case DUCKDB_TYPE_USMALLINT:
		    value = janet_wrap_integer(duckdb_value_uint16(&result, col, row));
		    break;
		case DUCKDB_TYPE_UINTEGER:
		    value = janet_wrap_integer(duckdb_value_uint32(&result, col, row));
		    break;
		case DUCKDB_TYPE_UBIGINT:
		    value = janet_wrap_number(duckdb_value_uint64(&result, col, row));
		    break;
		case DUCKDB_TYPE_FLOAT:
		    value = janet_wrap_number(duckdb_value_float(&result, col, row));
		    break;
		case DUCKDB_TYPE_DOUBLE:
		    value = janet_wrap_number(duckdb_value_double(&result, col, row));
		    break;
		case DUCKDB_TYPE_VARCHAR:
		    {
			duckdb_string dbstr = duckdb_value_string_internal(&result, col, row);
			uint8_t *str = janet_string_begin(dbstr.size);
			memcpy(str, dbstr.data, dbstr.size);
			value = janet_wrap_string(janet_string_end(str));
		    }
		    break;
		case DUCKDB_TYPE_BLOB:
		    {
			duckdb_blob blob = duckdb_value_blob(&result, col, row);
			JanetBuffer *b = janet_buffer(blob.size);
			memcpy(b->data, blob.data, blob.size);
			b->count = blob.size;
			value = janet_wrap_buffer(b);
			duckdb_free(blob.data);
		    }
		    break;
		}
                janet_struct_put(row_values, colnames[col], value);
            }
	}
	janet_array_push(rows, janet_wrap_struct(janet_struct_end(row_values)));
    }
    duckdb_destroy_result(&result);
    duckdb_destroy_extracted(&statements);
    return janet_wrap_array(rows);
}

static JanetMethod connection_methods[] = {
    {"eval", connection_eval},
    {"disconnect", connection_disconnect},
    {"close", connection_disconnect},
    {NULL, NULL}
};

static int connection_get(void *p, Janet key, Janet *out) {
    (void)p;
    if (!janet_checktype(key, JANET_KEYWORD)) {
	janet_panicf("expected keyword, got %v", key);
    }
    return janet_getmethod(janet_unwrap_keyword(key), connection_methods, out);
}

static Janet library_version(int32_t argc, Janet *argv) {
    janet_fixarity(argc, 0);
    return janet_cstringv(duckdb_library_version());
}

static const JanetReg cfuns[] = {
    {"open", database_open, "(duckdb/open path)\n\nopen a duckdb database. Path is optional."},
    {"library_version", library_version, "(duckdb/library)\n\nget the version of the duckdb library"},
    {"close", database_close, "(duckdb/close db)\n\n"
     "Closes the database."},
    {"connect", database_connect, "(duckdb/connect db)\n\n"
     "Returns an open connection to the database."},
    {"eval", connection_eval,
     "(duckdb/eval conn sql [,params])\n\n"
     "Evaluate sql statements with an open connection."
     "Optional parameters will be bound the statements."},
    {"disconnect", connection_disconnect, "(duckdb/disconnect conn)\n\n"
     "Closes an an open connection."},
    {NULL, NULL, NULL}
};

JANET_MODULE_ENTRY(JanetTable *env) {
    janet_cfuns(env, "duckdb", cfuns);
}
