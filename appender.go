package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

// Appender holds the duckdb appender. It allows to load bulk data into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool
}

// NewAppenderFromConn returns a new Appender from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema string, table string) (*Appender, error) {
	dbConn, ok := driverConn.(*conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}

	if dbConn.closed {
		panic("database/sql/driver: misuse of duckdb driver: Appender after Close")
	}

	var schemastr *(C.char)
	if schema != "" {
		schemastr = C.CString(schema)
		defer C.free(unsafe.Pointer(schemastr))
	}

	tablestr := C.CString(table)
	defer C.free(unsafe.Pointer(tablestr))

	var a C.duckdb_appender
	if state := C.duckdb_appender_create(*dbConn.con, schemastr, tablestr, &a); state == C.DuckDBError {
		return nil, fmt.Errorf("can't create appender")
	}

	return &Appender{c: dbConn, schema: schema, table: table, appender: &a}, nil
}

// Error returns the last duckdb appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
func (a *Appender) Flush() error {
	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}
	return nil
}

// Closes closes the appender.
func (a *Appender) Close() error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Appender")
	}

	a.closed = true

	if state := C.duckdb_appender_destroy(a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}
	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	return a.AppendRowArray(args)
}

// AppendRowArray loads a row of values into the appender. The values are provided as an array.
func (a *Appender) AppendRowArray(args []driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	for i, v := range args {
		if v == nil {
			if rv := C.duckdb_append_null(*a.appender); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d", i)
			}
			continue
		}

		var rv C.duckdb_state
		switch v := v.(type) {
		case uint8:
			rv = C.duckdb_append_uint8(*a.appender, C.uint8_t(v))
		case int8:
			rv = C.duckdb_append_int8(*a.appender, C.int8_t(v))
		case uint16:
			rv = C.duckdb_append_uint16(*a.appender, C.uint16_t(v))
		case int16:
			rv = C.duckdb_append_int16(*a.appender, C.int16_t(v))
		case uint32:
			rv = C.duckdb_append_uint32(*a.appender, C.uint32_t(v))
		case int32:
			rv = C.duckdb_append_int32(*a.appender, C.int32_t(v))
		case uint64:
			rv = C.duckdb_append_uint64(*a.appender, C.uint64_t(v))
		case int64:
			rv = C.duckdb_append_int64(*a.appender, C.int64_t(v))
		case uint:
			rv = C.duckdb_append_uint64(*a.appender, C.uint64_t(v))
		case int:
			rv = C.duckdb_append_int64(*a.appender, C.int64_t(v))
		case float32:
			rv = C.duckdb_append_float(*a.appender, C.float(v))
		case float64:
			rv = C.duckdb_append_double(*a.appender, C.double(v))
		case bool:
			rv = C.duckdb_append_bool(*a.appender, C.bool(v))
		case []byte:
			rv = C.duckdb_append_blob(*a.appender, unsafe.Pointer(&v[0]), C.uint64_t(len(v)))
		case string:
			str := C.CString(v)
			rv = C.duckdb_append_varchar(*a.appender, str)
			C.free(unsafe.Pointer(str))
		case time.Time:
			var dt C.duckdb_timestamp
			dt.micros = C.int64_t(v.UTC().UnixMicro())
			rv = C.duckdb_append_timestamp(*a.appender, dt)

		default:
			return fmt.Errorf("couldn't append unsupported parameter %d (type %T)", i, v)
		}
		if rv == C.DuckDBError {
			dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
			return fmt.Errorf("couldn't append parameter %d (type %T): %s", i, v, dbErr)
		}
	}

	if state := C.duckdb_appender_end_row(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}

	return nil
}

// Create an array of duckdb types from a list of go types
func createDuckDBTypes(args []driver.Value) *C.duckdb_logical_type {
	defaultLogicalType := C.duckdb_create_logical_type(0)
	argsTypes := C.malloc(C.size_t(len(args)) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	a := (*[1<<30 - 1]C.duckdb_logical_type)(argsTypes)

	for i, v := range args {
		switch v := v.(type) {
		case uint8:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT)
		case int8:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT)
		case uint16:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT)
		case int16:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT)
		case uint32:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER)
		case int32:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER)
		case uint64, uint:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT)
		case int64, int:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT)
		case float32:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT)
		case float64:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE)
		case bool:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN)
		case []byte:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB)
		case string:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
		case time.Time:
			a[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP)
		default:
			panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
		}
	}

	return (*C.duckdb_logical_type)(argsTypes)
}

// AppendChunk loads a column of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendChunk(args ...driver.Value) error {
	return a.AppendChunkArray(args)
}

func (a *Appender) AppendChunkArray(inputColumns []driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	var dataChunk C.duckdb_data_chunk
	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count
	dataChunk = C.duckdb_create_data_chunk(createDuckDBTypes(inputColumns), C.uint64_t(len(inputColumns)))
	C.duckdb_data_chunk_set_size(dataChunk, C.uint64_t(len(inputColumns)))

	for i, v := range inputColumns {
		var vector = C.duckdb_data_chunk_get_vector(dataChunk, C.uint64_t(i))
		var colData = C.duckdb_vector_get_data(vector)

		if colData == nil {
			return fmt.Errorf("couldn't append parameter %d", 0)
		}

		switch v := v.(type) {
		case uint8:
			writeableCol := (*[1<<30 - 1]C.uint8_t)(colData)
			writeableCol[0] = C.uint8_t(v)
		case int8:
			writeableCol := (*[1<<30 - 1]C.int8_t)(colData)
			writeableCol[0] = C.int8_t(v)
		case int32:
			writeableCol := (*[1<<30 - 1]C.int32_t)(colData)
			writeableCol[0] = C.int32_t(v)
		case int:
			writeableCol := (*[1<<30 - 1]C.int64_t)(colData)
			writeableCol[0] = C.int64_t(v)
		case int64:
			writeableCol := (*[1<<30 - 1]C.int64_t)(colData)
			writeableCol[0] = C.int64_t(v)
		}

	}

	var rv C.duckdb_state
	//rv = C.duckdb_append_uint8(*a.appender, C.uint8_t(v))
	rv = C.duckdb_append_data_chunk(*a.appender, dataChunk)

	if rv == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return fmt.Errorf("couldn't append parameter: %s", dbErr)
	}

	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
