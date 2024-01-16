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

type LogicalTypeEnum int

const (
	PRIMITIVE LogicalTypeEnum = iota
	LIST
	STRUCT
)

type SetColumnValue func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{})

type ColumnInfo struct {
	vector   C.duckdb_vector
	function SetColumnValue

	colType LogicalTypeEnum

	// Only for nested types so that we can recursively store the child column info
	fields      int // the number of fields in the struct
	columnInfos []ColumnInfo
}

// Appender holds the DuckDB appender. It allows data bulk loading into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	chunks           []C.duckdb_data_chunk
	currentChunkSize C.idx_t
	columnTypes      *C.duckdb_logical_type

	columnInfos []ColumnInfo
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

	return &Appender{c: dbConn, schema: schema, table: table, appender: &a, currentChunkSize: 0}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
func (a *Appender) Flush() error {
	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(a.chunks[len(a.chunks)-1], C.idx_t(a.currentChunkSize))

	// append all chunks to the appender and destroy them
	var state C.duckdb_state
	for i, chunk := range a.chunks {
		state = C.duckdb_append_data_chunk(*a.appender, chunk)
		if state == C.DuckDBError {
			dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
			return fmt.Errorf("duckdb error appending chunk %d of %d: %s", i+1, len(a.chunks), dbErr)
		}
		C.duckdb_destroy_data_chunk(&chunk)
	}

	// free the column types
	C.free(unsafe.Pointer(a.columnTypes))

	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}

	return nil
}

// Close the appender.
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
		a.initializeColumnTypes(args)
		err = a.addChunk(len(args))
		// If the current chunk is full, create a new one
	} else if a.currentChunkSize == C.duckdb_vector_size() {
		err = a.addChunk(len(args))
	}

	if err != nil {
		return err
	}

	return a.appendRowArray(args)
}

func (a *Appender) InitializeColumnTypesAndInfos(val driver.Value, colIdx int) (C.duckdb_logical_type, ColumnInfo) {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Uint8:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint8](a, callbackColumn, rowIdx, val.(uint8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Int8:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int8](a, callbackColumn, rowIdx, val.(int8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Uint16:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint16](a, callbackColumn, rowIdx, val.(uint16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Int16:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int16](a, callbackColumn, rowIdx, val.(int16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Uint32:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint32](a, callbackColumn, rowIdx, val.(uint32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Int32:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int32](a, callbackColumn, rowIdx, val.(int32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Uint64:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint64](a, callbackColumn, rowIdx, val.(uint64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Int64:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int64](a, callbackColumn, rowIdx, val.(int64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Uint:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint](a, callbackColumn, rowIdx, val.(uint))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Int:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int](a, callbackColumn, rowIdx, val.(int))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Float32:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[float32](a, callbackColumn, rowIdx, val.(float32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Float64:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[float64](a, callbackColumn, rowIdx, val.(float64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Bool:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[bool](a, callbackColumn, rowIdx, val.(bool))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.String:
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setVarchar(a, callbackColumn, rowIdx, val.(string))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), ColumnInfo{function: f, colType: PRIMITIVE}
	case reflect.Slice:
		// Check if it's []byte since that is equivalent to the DuckDB BLOB type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if v.Type().Elem().Kind() == reflect.Uint8 {
			f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
				setPrimitive[[]byte](a, callbackColumn, rowIdx, val.([]byte))
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB), ColumnInfo{function: f, colType: PRIMITIVE}
		}

		// Otherwise, it's a list
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setList(a, callbackColumn, rowIdx, val)
		}
		callbackColumn := ColumnInfo{function: f, colType: LIST, columnInfos: make([]ColumnInfo, 1)}
		childType, childCallbackColumn := a.InitializeColumnTypesAndInfos(v.Index(0).Interface(), colIdx)
		callbackColumn.columnInfos[0] = childCallbackColumn
		return C.duckdb_create_list_type(childType), callbackColumn
	case reflect.Struct:
		// Check if it's time.Time since that is equivalent to the DuckDB TIMESTAMP type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if (v.Type() == reflect.TypeOf(time.Time{})) {
			f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
				setTime(a, callbackColumn, rowIdx, val)
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP), ColumnInfo{function: f, colType: PRIMITIVE}
		}

		// Otherwise, it's a struct
		f := func(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setStruct(a, callbackColumn, rowIdx, val)
		}

		structType := v.Type()
		structValue := v

		callbackColumn := ColumnInfo{function: f, colType: STRUCT, columnInfos: make([]ColumnInfo, v.NumField()), fields: v.NumField()}

		// Create an array of the struct fields TYPES
		// and an array of the struct fields NAMES
		fieldTypes := make([]C.duckdb_logical_type, structType.NumField())
		fieldNames := make([]*C.char, structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			fieldTypes[i], callbackColumn.columnInfos[i] = a.InitializeColumnTypesAndInfos(structValue.Field(i).Interface(), i)
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

	return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), ColumnInfo{}
}

// Create an array of duckdb types from a list of go types
func (a *Appender) initializeColumnTypes(args []driver.Value) {
	a.columnInfos = make([]ColumnInfo, len(args))
	defaultLogicalType := C.duckdb_create_logical_type(0)
	columnTypes := C.malloc(C.size_t(len(args)) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	columnTypesPtr := (*[1<<30 - 1]C.duckdb_logical_type)(columnTypes)

	for i, val := range args {
		columnTypesPtr[i], a.columnInfos[i] = a.InitializeColumnTypesAndInfos(val, i)
	}

	a.columnTypes = (*C.duckdb_logical_type)(columnTypes)
}

func (c *ColumnInfo) getChildVectors(vector C.duckdb_vector) error {
	switch c.colType {
	case LIST:
		childVector := C.duckdb_list_vector_get_child(vector)
		c.columnInfos[0].vector = childVector
		err := c.columnInfos[0].getChildVectors(childVector)
		if err != nil {
			return err
		}
	case STRUCT:
		for i := 0; i < c.fields; i++ {
			childVector := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))
			c.columnInfos[i].vector = childVector
			err := c.columnInfos[i].getChildVectors(childVector)
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
	dataChunk := C.duckdb_create_data_chunk(a.columnTypes, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		vector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		if vector == nil {
			return fmt.Errorf("error while appending column %d", i)
		}
		c := &a.columnInfos[i]
		c.vector = vector
		err := c.getChildVectors(vector)
		if err != nil {
			return err
		}
	}

	a.chunks = append(a.chunks, dataChunk)
	return nil
}

func setPrimitive[T any](a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, value T) {
	ptr := C.duckdb_vector_get_data(callbackColumn.vector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = value
}

func setVarchar(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, value string) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element(callbackColumn.vector, rowIdx, str)
	C.free(unsafe.Pointer(str))
}

func setTime(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	var dt C.duckdb_timestamp
	dt.micros = C.int64_t(value.(time.Time).UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](a, callbackColumn, rowIdx, dt)
}

func setList(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	// Convert the refVal to []interface{} to be able to iterate over it
	refVal := reflect.ValueOf(value)
	interfaceSlice := make([]interface{}, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		interfaceSlice[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(callbackColumn.vector)

	// Set the offset of the list vector using the current size of the child vector.
	// The duckdb list vector contains for each row two values, the offset and the length,
	// which are located at rowIdx*2 and rowIdx*2+1 respectively.
	setPrimitive[uint64](a, callbackColumn, rowIdx*2, uint64(childVectorSize))

	// Set the length of the list vector
	setPrimitive[uint64](a, callbackColumn, rowIdx*2+1, uint64(refVal.Len()))

	C.duckdb_list_vector_set_size(callbackColumn.vector, C.idx_t(refVal.Len())+childVectorSize)
	C.duckdb_list_vector_reserve(callbackColumn.vector, C.idx_t(refVal.Len())+childVectorSize)

	childColumnInfo := callbackColumn.columnInfos[0]

	// Insert the values into the child vector
	for i, e := range interfaceSlice {
		childVectorRow := C.idx_t(i) + childVectorSize
		childColumnInfo.function(a, &childColumnInfo, childVectorRow, e)
	}
}

func setStruct(a *Appender, callbackColumn *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	structType := refVal.Type()

	for i := 0; i < structType.NumField(); i++ {
		childColumnInfo := callbackColumn.columnInfos[i]
		childColumnInfo.function(a, &childColumnInfo, rowIdx, refVal.Field(i).Interface())
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

		columnInfo := a.columnInfos[i]
		columnInfo.function(a, &columnInfo, a.currentChunkSize, v)
	}
	a.currentChunkSize++

	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
