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

const (
	PRIMITIVE = iota
	LIST
	STRUCT
)

type SetFunction func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{})

type CallbackColumn struct {
	vector   C.duckdb_vector
	function SetFunction

	colType int

	// Only for nested types
	fields          int // only for structs
	callbackColumns []CallbackColumn
}

// Appender holds the DuckDB appender. It allows data bulk loading into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	chunks           []C.duckdb_data_chunk
	currentChunkIdx  int
	currentChunkSize C.idx_t
	chunkTypes       *C.duckdb_logical_type

	callbackColumns []CallbackColumn
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

	return &Appender{c: dbConn, schema: schema, table: table, appender: &a, currentChunkIdx: 0, currentChunkSize: 0}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
func (a *Appender) Flush() error {
	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(a.chunks[a.currentChunkIdx], C.idx_t(a.currentChunkSize))

	// append all chunks to the appender and destroy them
	var state C.duckdb_state
	for i, chunk := range a.chunks {
		state = C.duckdb_append_data_chunk(*a.appender, chunk)
		if state == C.DuckDBError {
			dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
			return fmt.Errorf("duckdb error appending chunk %d of %d: %s", i+1, a.currentChunkIdx+1, dbErr)
		}
		C.duckdb_destroy_data_chunk(&chunk)
	}

	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}

	return nil
}

// Close closes the appender.
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
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	var err error
	// Initialize the chunk on the first call
	if len(a.chunks) == 0 {
		a.callbackColumns = make([]CallbackColumn, len(args))
		a.initializeChunkTypes(args)
		err = a.addChunk(len(args))
		// If the current chunk is full, create a new one
	} else if a.currentChunkSize == C.duckdb_vector_size() {
		a.currentChunkIdx++
		err = a.addChunk(len(args))
	}

	if err != nil {
		return err
	}

	return a.appendRowArray(args)
}

func (a *Appender) CreateDuckDBLogicalType(val driver.Value, col int) (C.duckdb_logical_type, CallbackColumn) {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Uint8:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[uint8](a, callbackColumn, row, val.(uint8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Int8:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[int8](a, callbackColumn, row, val.(int8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Uint16:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[uint16](a, callbackColumn, row, val.(uint16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Int16:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[int16](a, callbackColumn, row, val.(int16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Uint32:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[uint32](a, callbackColumn, row, val.(uint32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Int32:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[int32](a, callbackColumn, row, val.(int32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Uint64:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[uint64](a, callbackColumn, row, val.(uint64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Int64:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[int64](a, callbackColumn, row, val.(int64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Uint:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[uint](a, callbackColumn, row, val.(uint))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Int:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[int](a, callbackColumn, row, val.(int))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Float32:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[float32](a, callbackColumn, row, val.(float32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Float64:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[float64](a, callbackColumn, row, val.(float64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Bool:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setPrimitive[bool](a, callbackColumn, row, val.(bool))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.String:
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setVarchar(a, callbackColumn, row, val.(string))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), CallbackColumn{function: f, colType: PRIMITIVE}
	case reflect.Slice:
		// Check if it's []byte
		if v.Type().Elem().Kind() == reflect.Uint8 {
			f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
				setPrimitive[[]byte](a, callbackColumn, row, val.([]byte))
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB), CallbackColumn{function: f, colType: PRIMITIVE}
		}

		// Otherwise, it's a list
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setSlice(a, callbackColumn, row, val)
		}
		callbackColumn := CallbackColumn{function: f, colType: LIST, callbackColumns: make([]CallbackColumn, 1)}
		innerType, innerCallbackColumn := a.CreateDuckDBLogicalType(v.Index(0).Interface(), col)
		callbackColumn.callbackColumns[0] = innerCallbackColumn
		return C.duckdb_create_list_type(innerType), callbackColumn
	case reflect.Struct:
		// Check if it's time.Time
		if (v.Type() == reflect.TypeOf(time.Time{})) {
			f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
				setTime(a, callbackColumn, row, val)
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP), CallbackColumn{function: f, colType: PRIMITIVE}
		}

		// Otherwise, it's a struct
		f := func(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, val interface{}) {
			setStruct(a, callbackColumn, row, val)
		}

		structType := v.Type()
		structValue := v

		callbackColumn := CallbackColumn{function: f, colType: STRUCT, callbackColumns: make([]CallbackColumn, v.NumField()), fields: v.NumField()}

		// Create an array of the struct fields TYPES
		// and an array of the struct fields NAMES
		fieldTypes := make([]C.duckdb_logical_type, structType.NumField())
		fieldNames := make([]*C.char, structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			fieldTypes[i], callbackColumn.callbackColumns[i] = a.CreateDuckDBLogicalType(structValue.Field(i).Interface(), i)
			fieldNames[i] = C.CString(structType.Field(i).Name)
		}

		fieldTypesPtr := (*C.duckdb_logical_type)(unsafe.Pointer(&fieldTypes[0]))
		fieldNamesPtr := (**C.char)(unsafe.Pointer(&fieldNames[0]))

		structLogicalType := C.duckdb_create_struct_type(fieldTypesPtr, fieldNamesPtr, C.idx_t(structType.NumField()))

		for i := 0; i < structType.NumField(); i++ {
			C.free(unsafe.Pointer(fieldNames[i]))
			C.free(unsafe.Pointer(fieldTypes[i]))
		}

		return structLogicalType, callbackColumn
	default:
		panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
	}

	return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), CallbackColumn{}
}

// Create an array of duckdb types from a list of go types
func (a *Appender) initializeChunkTypes(args []driver.Value) {
	defaultLogicalType := C.duckdb_create_logical_type(0)
	rowTypes := C.malloc(C.size_t(len(args)) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	tmpChunkTypes := (*[1<<30 - 1]C.duckdb_logical_type)(rowTypes)

	for i, val := range args {
		tmpChunkTypes[i], a.callbackColumns[i] = a.CreateDuckDBLogicalType(val, i)
	}

	a.chunkTypes = (*C.duckdb_logical_type)(rowTypes)
}

func (c *CallbackColumn) getChildVectors(vector C.duckdb_vector, col int) error {
	switch c.colType {
	case LIST:
		childVector := C.duckdb_list_vector_get_child(vector)
		c.callbackColumns[0].vector = childVector
		err := c.callbackColumns[0].getChildVectors(childVector, col)
		if err != nil {
			return err
		}
	case STRUCT:
		for i := 0; i < c.fields; i++ {
			childVector := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))
			c.callbackColumns[i].vector = childVector
			err := c.callbackColumns[i].getChildVectors(childVector, col)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Appender) addChunk(colCount int) error {
	a.currentChunkSize = 0
	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count
	dataChunk := C.duckdb_create_data_chunk(a.chunkTypes, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		vector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		if vector == nil {
			return fmt.Errorf("error while appending column %d", i)
		}
		c := a.callbackColumns[i]
		c.vector = vector
		err := c.getChildVectors(vector, i)
		if err != nil {
			return err
		}
		a.callbackColumns[i] = c
	}

	a.chunks = append(a.chunks, dataChunk)
	return nil
}

func setPrimitive[T any](a *Appender, callbackColumn *CallbackColumn, row C.idx_t, value T) {
	ptr := C.duckdb_vector_get_data(callbackColumn.vector)
	xs := (*[1 << 31]T)(ptr)
	xs[row] = value
}

func setVarchar(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, value string) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element(callbackColumn.vector, row, str)
	C.free(unsafe.Pointer(str))
}

func setTime(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, value driver.Value) {
	var dt C.duckdb_timestamp
	dt.micros = C.int64_t(value.(time.Time).UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](a, callbackColumn, row, dt)
}

func setSlice(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, value driver.Value) {
	// Convert the refVal to []interface{} to be able to iterate over it
	refVal := reflect.ValueOf(value)
	interfaceSlice := make([]interface{}, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		interfaceSlice[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(callbackColumn.vector)

	// Set the offset of the list vector using the current size of the child vector
	setPrimitive[uint64](a, callbackColumn, row*2, uint64(childVectorSize))

	// Set the length of the list vector
	setPrimitive[uint64](a, callbackColumn, row*2+1, uint64(refVal.Len()))

	C.duckdb_list_vector_set_size(callbackColumn.vector, C.idx_t(refVal.Len())+childVectorSize)

	innerCallbackColumn := callbackColumn.callbackColumns[0]

	// Insert the values into the child vector
	for i, e := range interfaceSlice {
		childVectorRow := C.idx_t(i) + childVectorSize
		// Increase the child vector capacity if over standard vector size by standard vector size
		if childVectorRow%C.duckdb_vector_size() == 0 {
			C.duckdb_list_vector_reserve(callbackColumn.vector, childVectorRow+C.duckdb_vector_size())
		}
		innerCallbackColumn.function(a, &innerCallbackColumn, childVectorRow, e)
	}
}

func setStruct(a *Appender, callbackColumn *CallbackColumn, row C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	structType := refVal.Type()

	for i := 0; i < structType.NumField(); i++ {
		innerCallbackColumn := callbackColumn.callbackColumns[i]
		innerCallbackColumn.function(a, &innerCallbackColumn, row, refVal.Field(i).Interface())
	}
}

// appendRowArray loads a row of values into the appender. The values are provided as an array.
func (a *Appender) appendRowArray(args []driver.Value) error {
	for i, v := range args {
		if v == nil {
			if state := C.duckdb_append_null(*a.appender); state == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d", v)
			}
			continue
		}

		callbackCol := a.callbackColumns[i]
		callbackCol.function(a, &callbackCol, a.currentChunkSize, v)
	}
	a.currentChunkSize++

	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
