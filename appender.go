package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
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

	appendCount     int
	chunks          []C.duckdb_data_chunk
	currentChunkIdx int
	chunkTypes      *C.duckdb_logical_type

	//appendcount
	//sliceofchunks
	//currentchunkidx
	//chunktypes
	//isinitialized
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
func createDuckDBTypes(row interface{}) *C.duckdb_logical_type {
	val := reflect.ValueOf(row)

	defaultLogicalType := C.duckdb_create_logical_type(0)
	rowTypes := C.malloc(C.size_t(val.NumField()) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	a := (*[1<<30 - 1]C.duckdb_logical_type)(rowTypes)

	for i := 0; i < val.NumField(); i++ {
		switch v := val.Field(i).Interface().(type) {
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

	return (*C.duckdb_logical_type)(rowTypes)
}

func CreateWritableCols(dataChunk C.duckdb_data_chunk, row interface{}) (map[int]C.duckdb_vector, []interface{}, error) {
	var chunkVectors = make(map[int]C.duckdb_vector)
	var writableCols []interface{}
	val := reflect.ValueOf(row)

	for i := 0; i < val.NumField(); i++ {
		var vector = C.duckdb_data_chunk_get_vector(dataChunk, C.uint64_t(i))
		var colData = C.duckdb_vector_get_data(vector)
		if colData == nil {
			return nil, nil, fmt.Errorf("couldn't append parameter %d", 0)
		}

		switch v := val.Field(i).Interface().(type) {
		case uint8:
			writeableCol := (*[1<<30 - 1]C.uint8_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case int8:
			writeableCol := (*[1<<30 - 1]C.int8_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case uint16:
			writeableCol := (*[1<<30 - 1]C.uint16_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case int16:
			writeableCol := (*[1<<30 - 1]C.int16_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case uint32:
			writeableCol := (*[1<<30 - 1]C.uint32_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case int32:
			writeableCol := (*[1<<30 - 1]C.int32_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case uint64, uint:
			writeableCol := (*[1<<30 - 1]C.uint64_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case int64, int:
			writeableCol := (*[1<<30 - 1]C.int64_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case float32:
			writeableCol := (*[1<<30 - 1]C.float)(colData)
			writableCols = append(writableCols, &writeableCol)
		case float64:
			writeableCol := (*[1<<30 - 1]C.double)(colData)
			writableCols = append(writableCols, &writeableCol)
		case bool:
			writeableCol := (*[1<<30 - 1]C.bool)(colData)
			writableCols = append(writableCols, &writeableCol)
		case []byte:
			writeableCol := (*[1<<30 - 1]C.uint8_t)(colData)
			writableCols = append(writableCols, &writeableCol)
		case string:
			chunkVectors[i] = vector
		case time.Time:
			writeableCol := (*[1<<30 - 1]C.duckdb_timestamp)(colData)
			writableCols = append(writableCols, &writeableCol)
		default:
			panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
		}
	}

	return chunkVectors, writableCols, nil
}

// AppendRows loads a column of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRows(args driver.Value) error {
	return a.AppendRowsArray(args)
}

func (a *Appender) AppendRowsArray(inputColumns driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	rows := reflect.ValueOf(inputColumns)
	if rows.Kind() != reflect.Slice {
		fmt.Println("Invalid argument type")
		return nil
	}

	firstRow := rows.Index(0).Interface()
	firstRowVal := reflect.ValueOf(firstRow)
	if firstRowVal.Kind() != reflect.Struct {
		fmt.Println("Invalid argument type")
		return nil
	}

	var dataChunk C.duckdb_data_chunk
	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count
	dataChunk = C.duckdb_create_data_chunk(createDuckDBTypes(firstRow), C.uint64_t(firstRowVal.NumField()))
	C.duckdb_data_chunk_set_size(dataChunk, C.uint64_t(rows.Len()))

	chunkVectors, colsWritable, err := CreateWritableCols(dataChunk, firstRow)
	if err != nil {
		return err
	}

	for i := 0; i < rows.Len(); i++ {
		row := rows.Index(i).Interface()
		for j := 0; j < firstRowVal.NumField(); j++ {
			switch firstRowVal.Field(i).Interface().(type) {
			case uint8:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.uint8_t)
				writeableCol[i] = C.uint8_t(reflect.ValueOf(row).Field(j).Interface().(uint8))
			case int8:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.int8_t)
				writeableCol[i] = C.int8_t(reflect.ValueOf(row).Field(j).Interface().(int8))
			case uint16:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.uint16_t)
				writeableCol[i] = C.uint16_t(reflect.ValueOf(row).Field(j).Interface().(uint16))
			case int16:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.int16_t)
				writeableCol[i] = C.int16_t(reflect.ValueOf(row).Field(j).Interface().(int16))
			case uint32:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.uint32_t)
				writeableCol[i] = C.uint32_t(reflect.ValueOf(row).Field(j).Interface().(uint32))
			case int32:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.int32_t)
				writeableCol[i] = C.int32_t(reflect.ValueOf(row).Field(j).Interface().(int32))
			case int:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.int64_t)
				writeableCol[i] = C.int64_t(reflect.ValueOf(row).Field(j).Interface().(int))
			case uint64:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.uint64_t)
				writeableCol[i] = C.uint64_t(reflect.ValueOf(row).Field(j).Interface().(uint64))
			case int64:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.int64_t)
				writeableCol[i] = C.int64_t(reflect.ValueOf(row).Field(j).Interface().(int64))
			case float32:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.float)
				writeableCol[i] = C.float(reflect.ValueOf(row).Field(j).Interface().(float32))
			case float64:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.double)
				writeableCol[i] = C.double(reflect.ValueOf(row).Field(j).Interface().(float64))
			case bool:
				writeableCol := colsWritable[i].(*[1<<30 - 1]C.bool)
				writeableCol[i] = C.bool(reflect.ValueOf(row).Field(j).Interface().(bool))
			case string:
				str := C.CString(reflect.ValueOf(row).Field(j).Interface().(string))
				C.duckdb_vector_assign_string_element(chunkVectors[j], C.uint64_t(i), str)
				C.free(unsafe.Pointer(str))
			case time.Time:
				writableCol := colsWritable[i].(*[1<<30 - 1]C.duckdb_timestamp)
				var dt C.duckdb_timestamp
				dt.micros = C.int64_t(reflect.ValueOf(row).Field(j).Interface().(time.Time).UTC().UnixMicro())
				writableCol[i] = dt
			}
		}

	}

	var rv C.duckdb_state
	rv = C.duckdb_append_data_chunk(*a.appender, dataChunk)

	if rv == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return fmt.Errorf("couldn't append parameter: %s", dbErr)
	}

	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
