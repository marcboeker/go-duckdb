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

var typesMap = map[string]C.duckdb_type{
	"bool":      C.DUCKDB_TYPE_BOOLEAN,
	"int8":      C.DUCKDB_TYPE_TINYINT,
	"int16":     C.DUCKDB_TYPE_SMALLINT,
	"int32":     C.DUCKDB_TYPE_INTEGER,
	"int64":     C.DUCKDB_TYPE_BIGINT,
	"uint8":     C.DUCKDB_TYPE_UTINYINT,
	"uint16":    C.DUCKDB_TYPE_USMALLINT,
	"uint32":    C.DUCKDB_TYPE_UINTEGER,
	"uint64":    C.DUCKDB_TYPE_UBIGINT,
	"float32":   C.DUCKDB_TYPE_FLOAT,
	"float64":   C.DUCKDB_TYPE_DOUBLE,
	"string":    C.DUCKDB_TYPE_VARCHAR,
	"[]byte{}":  C.DUCKDB_TYPE_BLOB,
	"time.Time": C.DUCKDB_TYPE_TIMESTAMP,
	"uuid.UUID": C.DUCKDB_TYPE_UUID,
	"slice":     C.DUCKDB_TYPE_LIST,
	"struct":    C.DUCKDB_TYPE_STRUCT,
}

// SetColumnValue is the type definition for all column callback functions
type SetColumnValue func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{})

// ColumnInfo holds the logical column type, a callback function to write column values, and additional helper fields.
type ColumnInfo struct {
	vector   C.duckdb_vector
	function SetColumnValue

	colType C.duckdb_type

	// Only for nested types so that we can recursively store the child column info
	fields      int // the number of fields in the struct
	columnInfos []ColumnInfo
}

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	chunks           []C.duckdb_data_chunk
	currentChunkSize C.idx_t
	columnTypes      []C.duckdb_logical_type
	columnTypesPtr   unsafe.Pointer

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

	var schemaStr *C.char
	if schema != "" {
		schemaStr = C.CString(schema)
		defer C.free(unsafe.Pointer(schemaStr))
	}

	tableStr := C.CString(table)
	defer C.free(unsafe.Pointer(tableStr))

	var appender C.duckdb_appender
	state := C.duckdb_appender_create(*dbConn.con, schemaStr, tableStr, &appender)

	if state == C.DuckDBError {
		// we'll destroy the error message when destroying the appender
		err := errors.New(C.GoString(C.duckdb_appender_error(appender)))
		C.duckdb_appender_destroy(&appender)
		return nil, err
	}

	return &Appender{c: dbConn, schema: schema, table: table, appender: &appender, currentChunkSize: 0}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
// Unless you have a good reason to call this, call Close instead when you are done with the appender.
func (a *Appender) Flush() error {
	if len(a.chunks) == 0 && a.currentChunkSize == 0 {
		return nil
	}

	err := a.appendChunks()
	if err != nil {
		return err
	}

	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		return a.Error()
	}

	a.currentChunkSize = 0
	a.chunks = a.chunks[:0]
	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Appender")
	}

	a.closed = true
	var err error
	// append chunks if not already done via flush
	if len(a.chunks) != 0 || a.currentChunkSize != 0 {
		err = a.appendChunks()
	}

	// free the column types
	for i := range a.columnInfos {
		C.duckdb_destroy_logical_type(&a.columnTypes[i])
	}

	C.free(a.columnTypesPtr)

	if state := C.duckdb_appender_destroy(a.appender); state == C.DuckDBError {
		return a.Error()
	}
	return err
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	var err error
	// Initialize the chunk on the first call
	if len(a.columnTypes) == 0 {
		if err = a.initializeColumnTypes(args); err != nil {
			return err
		}
		err = a.addChunk(len(args))
		// If the current chunk is full, create a new one
	} else if a.currentChunkSize == C.duckdb_vector_size() || len(a.chunks) == 0 {
		err = a.addChunk(len(args))
	}

	if err != nil {
		return err
	}

	return a.appendRowArray(args)
}

func (a *Appender) InitializeColumnTypesAndInfos(v reflect.Type, colIdx int) (C.duckdb_logical_type, ColumnInfo) {
	switch v.Kind() {
	case reflect.Uint8:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint8](a, columnInfo, rowIdx, val.(uint8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_UTINYINT}
	case reflect.Int8:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int8](a, columnInfo, rowIdx, val.(int8))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_TINYINT}
	case reflect.Uint16:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint16](a, columnInfo, rowIdx, val.(uint16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_USMALLINT}
	case reflect.Int16:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int16](a, columnInfo, rowIdx, val.(int16))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_SMALLINT}
	case reflect.Uint32:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint32](a, columnInfo, rowIdx, val.(uint32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_UINTEGER}
	case reflect.Int32:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int32](a, columnInfo, rowIdx, val.(int32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_INTEGER}
	case reflect.Uint64:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint64](a, columnInfo, rowIdx, val.(uint64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_UBIGINT}
	case reflect.Int64:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int64](a, columnInfo, rowIdx, val.(int64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_BIGINT}
	case reflect.Uint:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[uint](a, columnInfo, rowIdx, val.(uint))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_UBIGINT}
	case reflect.Int:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[int](a, columnInfo, rowIdx, val.(int))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_BIGINT}
	case reflect.Float32:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[float32](a, columnInfo, rowIdx, val.(float32))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_FLOAT}
	case reflect.Float64:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[float64](a, columnInfo, rowIdx, val.(float64))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_DOUBLE}
	case reflect.Bool:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setPrimitive[bool](a, columnInfo, rowIdx, val.(bool))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_BOOLEAN}
	case reflect.String:
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setVarchar(a, columnInfo, rowIdx, val.(string))
		}
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_VARCHAR}
	case reflect.Slice:
		// Check if it's []byte since that is equivalent to the DuckDB BLOB type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if v.Elem().Kind() == reflect.Uint8 {
			f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
				setPrimitive[[]byte](a, columnInfo, rowIdx, val.([]byte))
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_BLOB}
		}

		// Otherwise, it's a list
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setList(a, columnInfo, rowIdx, val)
		}
		columnInfo := ColumnInfo{function: f, colType: C.DUCKDB_TYPE_LIST, columnInfos: make([]ColumnInfo, 1)}
		childType, childColumnInfo := a.InitializeColumnTypesAndInfos(v.Elem(), colIdx)
		columnInfo.columnInfos[0] = childColumnInfo
		listType := C.duckdb_create_list_type(childType)
		C.duckdb_destroy_logical_type(&childType)
		return listType, columnInfo
	case reflect.Array:
		if v.Elem().Kind() == reflect.Uint8 && v.Len() == 16 {
			f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
				setPrimitive[C.duckdb_hugeint](a, columnInfo, rowIdx, uuidToHugeInt(val.(UUID)))
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_UUID}
		}
	case reflect.Struct:
		// Check if it's time.Time since that is equivalent to the DuckDB TIMESTAMP type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if (v == reflect.TypeOf(time.Time{})) {
			f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
				setTime(a, columnInfo, rowIdx, val)
			}
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP), ColumnInfo{function: f, colType: C.DUCKDB_TYPE_TIMESTAMP}
		}

		// Otherwise, it's a struct
		f := func(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, val interface{}) {
			setStruct(a, columnInfo, rowIdx, val)
		}

		structType := v

		columnInfo := ColumnInfo{function: f, colType: C.DUCKDB_TYPE_STRUCT, columnInfos: make([]ColumnInfo, v.NumField()), fields: v.NumField()}

		// Create an array of the struct fields TYPES
		// and an array of the struct fields NAMES
		fieldTypesPtr, fieldTypes := mallocLogicalTypeSlice(structType.NumField())
		fieldNamesPtr, fieldNames := mallocCStringSlice(structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			fieldTypes[i], columnInfo.columnInfos[i] = a.InitializeColumnTypesAndInfos(structType.Field(i).Type, i)
			fieldNames[i] = C.CString(structType.Field(i).Name)
		}

		structLogicalType := C.duckdb_create_struct_type((*C.duckdb_logical_type)(fieldTypesPtr), (**C.char)(fieldNamesPtr), C.idx_t(structType.NumField()))

		for i := 0; i < structType.NumField(); i++ {
			C.duckdb_destroy_logical_type(&fieldTypes[i])
			C.free(unsafe.Pointer(fieldNames[i]))
		}
		C.free(fieldTypesPtr)
		C.free(fieldNamesPtr)

		return structLogicalType, columnInfo
	case reflect.Map:
		panic(fmt.Sprintf("%T: the appender currently doesn't support maps", v))
	default:
		panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
	}

	return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), ColumnInfo{}
}

func mallocCStringSlice(count int) (unsafe.Pointer, []*C.char) {
	var dummyExpr *C.char
	size := C.size_t(unsafe.Sizeof(dummyExpr))

	cStringsPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	cStrings := (*[1 << 30]*C.char)(cStringsPtr)[:count:count]
	return cStringsPtr, cStrings
}

func mallocLogicalTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummyType C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummyType))

	columnTypesPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	columnTypes := (*[1 << 30]C.duckdb_logical_type)(columnTypesPtr)[:count:count]
	return columnTypesPtr, columnTypes
}

// Create an array of duckdb types from a list of go types
func (a *Appender) initializeColumnTypes(args []driver.Value) error {
	a.columnInfos = make([]ColumnInfo, len(args))

	a.columnTypesPtr, a.columnTypes = mallocLogicalTypeSlice(len(args))

	for i, val := range args {
		if val == nil {
			return fmt.Errorf("The first row cannot contain null values (column %d)", i)
		}
		v := reflect.ValueOf(val)
		a.columnTypes[i], a.columnInfos[i] = a.InitializeColumnTypesAndInfos(v.Type(), i)
	}
	return nil
}

func (c *ColumnInfo) getChildVectors(vector C.duckdb_vector) {
	switch c.colType {
	case C.DUCKDB_TYPE_LIST:
		childVector := C.duckdb_list_vector_get_child(vector)
		c.columnInfos[0].vector = childVector
		c.columnInfos[0].getChildVectors(childVector)
	case C.DUCKDB_TYPE_STRUCT:
		for i := 0; i < c.fields; i++ {
			childVector := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))
			c.columnInfos[i].vector = childVector
			c.columnInfos[i].getChildVectors(childVector)
		}
	}
}

func (a *Appender) addChunk(colCount int) error {
	a.currentChunkSize = 0
	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count
	columnTypesPtr := (*C.duckdb_logical_type)(a.columnTypesPtr)
	dataChunk := C.duckdb_create_data_chunk(columnTypesPtr, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		vector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		if vector == nil {
			panic(fmt.Sprintf("error while appending column %d", i))
		}
		c := &a.columnInfos[i]
		c.vector = vector
		c.getChildVectors(vector)
	}

	a.chunks = append(a.chunks, dataChunk)
	return nil
}

func setNull(columnInfo *ColumnInfo, rowIdx C.idx_t) {
	C.duckdb_vector_ensure_validity_writable(columnInfo.vector)
	mask := C.duckdb_vector_get_validity(columnInfo.vector)
	C.duckdb_validity_set_row_invalid(mask, rowIdx)
}

func setPrimitive[T any](a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, value T) {
	ptr := C.duckdb_vector_get_data(columnInfo.vector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = value
}

func setVarchar(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, value string) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element(columnInfo.vector, rowIdx, str)
	C.free(unsafe.Pointer(str))
}

func setTime(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	var dt C.duckdb_timestamp
	dt.micros = C.int64_t(value.(time.Time).UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](a, columnInfo, rowIdx, dt)
}

func setList(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)

	childColumnInfo := columnInfo.columnInfos[0]

	if refVal.IsNil() {
		setNull(columnInfo, rowIdx)
	}

	// Convert the refVal to []interface{} to be able to iterate over it
	interfaceSlice := make([]interface{}, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		interfaceSlice[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(columnInfo.vector)

	// Set the offset of the list vector using the current size of the child vector.
	// The duckdb list vector contains for each row two values, the offset and the length,
	// which are located at rowIdx*2 and rowIdx*2+1 respectively.
	setPrimitive[uint64](a, columnInfo, rowIdx*2, uint64(childVectorSize))

	// Set the length of the list vector
	setPrimitive[uint64](a, columnInfo, rowIdx*2+1, uint64(refVal.Len()))

	C.duckdb_list_vector_set_size(columnInfo.vector, C.idx_t(refVal.Len())+childVectorSize)
	C.duckdb_list_vector_reserve(columnInfo.vector, C.idx_t(refVal.Len())+childVectorSize)

	// Insert the values into the child vector
	for i, e := range interfaceSlice {
		childVectorRow := C.idx_t(i) + childVectorSize
		childColumnInfo.function(a, &childColumnInfo, childVectorRow, e)
	}
}

func setStruct(a *Appender, columnInfo *ColumnInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	structType := refVal.Type()

	if value == nil {
		setNull(columnInfo, rowIdx)
	}

	for i := 0; i < structType.NumField(); i++ {
		childColumnInfo := columnInfo.columnInfos[i]
		childColumnInfo.function(a, &childColumnInfo, rowIdx, refVal.Field(i).Interface())
	}
}

// appendRowArray loads a row of values into the appender. The values are provided as an array.
func (a *Appender) appendRowArray(args []driver.Value) error {
	for i, v := range args {
		columnInfo := a.columnInfos[i]
		if v == nil {
			setNull(&columnInfo, a.currentChunkSize)
			continue
		}

		//vType := reflect.TypeOf(v).String()
		//duckdbType := typesMap[vType]
		//if duckdbType != a.columnInfos[i].colType {
		//	return fmt.Errorf("type mismatch for column %d, type %s", i, vType)
		//}

		columnInfo.function(a, &columnInfo, a.currentChunkSize, v)
	}
	a.currentChunkSize++

	return nil
}

func (a *Appender) appendChunks() error {
	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(a.chunks[len(a.chunks)-1], C.idx_t(a.currentChunkSize))

	// append all chunks to the appender and destroy them
	var state C.duckdb_state
	for _, chunk := range a.chunks {
		state = C.duckdb_append_data_chunk(*a.appender, chunk)
		if state == C.DuckDBError {
			dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
			return fmt.Errorf("Duckdb has returned an error while appending, all data has been invalidated."+
				"\n Check that the data being appended matches the schema."+
				"\n Error from duckdb: %s", dbErr)
		}
		C.duckdb_destroy_data_chunk(&chunk)
	}
	return nil
}
