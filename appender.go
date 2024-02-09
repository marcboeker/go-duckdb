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

const errNote = "NOTE: this is based on types initialized from the first row of appended data, please confirm this matches the schema."

var duckdbTypeMap = map[C.duckdb_type]string{
	C.DUCKDB_TYPE_BOOLEAN:   "bool",
	C.DUCKDB_TYPE_TINYINT:   "int8",
	C.DUCKDB_TYPE_SMALLINT:  "int16",
	C.DUCKDB_TYPE_INTEGER:   "int32",
	C.DUCKDB_TYPE_BIGINT:    "int64",
	C.DUCKDB_TYPE_UTINYINT:  "uint8",
	C.DUCKDB_TYPE_USMALLINT: "uint16",
	C.DUCKDB_TYPE_UINTEGER:  "uint32",
	C.DUCKDB_TYPE_UBIGINT:   "uint64",
	C.DUCKDB_TYPE_FLOAT:     "float32",
	C.DUCKDB_TYPE_DOUBLE:    "float64",
	C.DUCKDB_TYPE_VARCHAR:   "string",
	C.DUCKDB_TYPE_BLOB:      "[]uint8",
	C.DUCKDB_TYPE_TIMESTAMP: "time.Time",
	C.DUCKDB_TYPE_UUID:      "duckdb.UUID",
	C.DUCKDB_TYPE_LIST:      "slice",
	C.DUCKDB_TYPE_STRUCT:    "struct",
}

// SetColumnValue is the type definition for all column callback functions
type SetColumnValue func(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, val any)

// columnInfo holds the logical column type, a callback function to write column values, and additional helper fields.
type columnInfo struct {
	vector C.duckdb_vector
	fn     SetColumnValue

	logicalType C.duckdb_logical_type
	colType     C.duckdb_type

	// Only for nested types so that we can recursively store the child column info
	fields      int // the number of fields in the struct
	columnInfos []columnInfo
}

func (c *columnInfo) duckDBTypeToString() (string, C.duckdb_type) {
	if c.colType == C.DUCKDB_TYPE_LIST {
		s, t := c.columnInfos[0].duckDBTypeToString()
		return "[]" + s, t
	}

	if c.colType == C.DUCKDB_TYPE_STRUCT {
		s := "{"
		for i := 0; i < c.fields; i++ {
			if i > 0 {
				s += ", "
			}
			tmp, _ := c.columnInfos[i].duckDBTypeToString()
			s += tmp
		}
		s += "}"
		return s, c.colType
	}

	return duckdbTypeMap[c.colType], c.colType
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

	columnInfos []columnInfo
}

// NewAppenderFromConn returns a new Appender from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	dbConn, ok := driverConn.(*conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}

	if dbConn.closed {
		panic("database/sql/driver: misuse of duckdb driver: Appender after Close")
	}

	var cSchema *C.char
	if schema != "" {
		cSchema = C.CString(schema)
		defer C.free(unsafe.Pointer(cSchema))
	}

	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	var appender C.duckdb_appender
	state := C.duckdb_appender_create(*dbConn.con, cSchema, cTable, &appender)

	if state == C.DuckDBError {
		// We'll destroy the error message when destroying the appender.
		err := errors.New(C.GoString(C.duckdb_appender_error(appender)))
		C.duckdb_appender_destroy(&appender)
		return nil, err
	}

	return &Appender{
		c:                dbConn,
		schema:           schema,
		table:            table,
		appender:         &appender,
		currentChunkSize: 0,
	}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
// Unless you have a good reason to call this, call Close instead when you
// are done with the appender.
func (a *Appender) Flush() error {
	if len(a.chunks) == 0 && a.currentChunkSize == 0 {
		return nil
	}

	if err := a.appendChunks(); err != nil {
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

	// Append chunks if not already done via flush.
	var err error
	if len(a.chunks) != 0 || a.currentChunkSize != 0 {
		err = a.appendChunks()
	}

	// Free the column types.
	for i := range a.columnInfos {
		C.duckdb_destroy_logical_type(&a.columnTypes[i])
	}

	C.free(a.columnTypesPtr)

	if state := C.duckdb_appender_destroy(a.appender); state == C.DuckDBError {
		return a.Error()
	}

	return err
}

// AppendRow loads a row of values into the appender.
// The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	// Initialize the chunk on the first call.
	var err error
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

// Create an array of DuckDB types from a list of go types.
func (a *Appender) initializeColumnTypes(args []driver.Value) error {
	a.columnInfos = make([]columnInfo, len(args))
	a.columnTypesPtr, a.columnTypes = mallocLogicalTypeSlice(len(args))

	for i, val := range args {
		if val == nil {
			return fmt.Errorf("the first row cannot contain null values (column %d)", i)
		}

		v := reflect.ValueOf(val)
		a.columnInfos[i] = a.initializeColumnTypesAndInfos(v.Type(), i)
		a.columnTypes[i] = a.columnInfos[i].logicalType
	}

	return nil
}

func mallocLogicalTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummyType C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummyType))

	ctPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	ct := (*[1 << 30]C.duckdb_logical_type)(ctPtr)[:count:count]

	return ctPtr, ct
}

func mallocCStringSlice(count int) (unsafe.Pointer, []*C.char) {
	var dummyExpr *C.char
	size := C.size_t(unsafe.Sizeof(dummyExpr))

	cStringsPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	cStrings := (*[1 << 30]*C.char)(cStringsPtr)[:count:count]
	return cStringsPtr, cStrings
}

func (a *Appender) initializeColumnTypesAndInfos(v reflect.Type, colIdx int) columnInfo {
	switch v.Kind() {
	case reflect.Uint8:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[uint8](colInfo, rowIdx, val.(uint8))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT),
			colType:     C.DUCKDB_TYPE_UTINYINT,
		}
	case reflect.Int8:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[int8](colInfo, rowIdx, val.(int8))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT),
			colType:     C.DUCKDB_TYPE_TINYINT,
		}
	case reflect.Uint16:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[uint16](colInfo, rowIdx, val.(uint16))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT),
			colType:     C.DUCKDB_TYPE_USMALLINT,
		}
	case reflect.Int16:
		f := func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
			setPrimitive[int16](colInfo, rowIdx, val.(int16))
		}
		return columnInfo{
			fn:          f,
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT),
			colType:     C.DUCKDB_TYPE_SMALLINT,
		}
	case reflect.Uint32:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[uint32](colInfo, rowIdx, val.(uint32))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER),
			colType:     C.DUCKDB_TYPE_UINTEGER,
		}
	case reflect.Int32:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[int32](colInfo, rowIdx, val.(int32))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER),
			colType:     C.DUCKDB_TYPE_INTEGER,
		}
	case reflect.Uint64:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[uint64](colInfo, rowIdx, val.(uint64))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT),
			colType:     C.DUCKDB_TYPE_UBIGINT,
		}
	case reflect.Int64:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[int64](colInfo, rowIdx, val.(int64))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT),
			colType:     C.DUCKDB_TYPE_BIGINT,
		}
	case reflect.Uint:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[uint32](colInfo, rowIdx, val.(uint32))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER),
			colType:     C.DUCKDB_TYPE_UINTEGER,
		}
	case reflect.Int:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[int32](colInfo, rowIdx, val.(int32))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER),
			colType:     C.DUCKDB_TYPE_INTEGER,
		}
	case reflect.Float32:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[float32](colInfo, rowIdx, val.(float32))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT),
			colType:     C.DUCKDB_TYPE_FLOAT,
		}
	case reflect.Float64:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[float64](colInfo, rowIdx, val.(float64))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE),
			colType:     C.DUCKDB_TYPE_DOUBLE,
		}
	case reflect.Bool:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[bool](colInfo, rowIdx, val.(bool))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN),
			colType:     C.DUCKDB_TYPE_BOOLEAN,
		}
	case reflect.String:
		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setVarchar(colInfo, rowIdx, val.(string))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR),
			colType:     C.DUCKDB_TYPE_VARCHAR,
		}
	case reflect.Slice:
		// Check if it's []byte since that is equivalent to the DuckDB BLOB type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if v.Elem().Kind() == reflect.Uint8 {
			return columnInfo{
				fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
					setPrimitive[[]byte](colInfo, rowIdx, val.([]byte))
				},
				logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB),
				colType:     C.DUCKDB_TYPE_BLOB,
			}
		}

		childColInfo := a.initializeColumnTypesAndInfos(v.Elem(), colIdx)
		defer C.duckdb_destroy_logical_type(&childColInfo.logicalType)

		return columnInfo{
			fn: func(a *Appender, colInfo *columnInfo, rowIdx C.idx_t, val any) {
				setList(a, colInfo, rowIdx, val)
			},
			logicalType: C.duckdb_create_list_type(childColInfo.logicalType),
			colType:     C.DUCKDB_TYPE_LIST,
			columnInfos: []columnInfo{childColInfo},
		}
	case reflect.TypeOf(UUID{}).Kind():
		return columnInfo{
			fn: func(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, val any) {
				setPrimitive[C.duckdb_hugeint](columnInfo, rowIdx, uuidToHugeInt(val.(UUID)))
			},
			logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID),
			colType:     C.DUCKDB_TYPE_UUID,
		}
	case reflect.Struct:
		// Check if it's time.Time since that is equivalent to the DuckDB TIMESTAMP type.
		// If so, we can just use the primitive setter; otherwise it will not match the table set up by the user.
		if (v == reflect.TypeOf(time.Time{})) {
			return columnInfo{
				fn: func(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, val any) {
					setTime(columnInfo, rowIdx, val)
				},
				logicalType: C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP),
				colType:     C.DUCKDB_TYPE_TIMESTAMP,
			}
		}

		// Otherwise, it's a struct
		colInfo := columnInfo{
			fn: func(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, val any) {
				setStruct(a, columnInfo, rowIdx, val)
			},
			colType:     C.DUCKDB_TYPE_STRUCT,
			columnInfos: make([]columnInfo, v.NumField()),
			fields:      v.NumField(),
		}

		// Create an array of the struct fields TYPES
		// and an array of the struct fields NAMES
		structType := v
		fieldTypesPtr, fieldTypes := mallocLogicalTypeSlice(structType.NumField())
		fieldNamesPtr, fieldNames := mallocCStringSlice(structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			colInfo.columnInfos[i] = a.initializeColumnTypesAndInfos(structType.Field(i).Type, i)
			fieldTypes[i] = colInfo.columnInfos[i].logicalType
			fieldNames[i] = C.CString(structType.Field(i).Name)
		}

		colInfo.logicalType = C.duckdb_create_struct_type(
			(*C.duckdb_logical_type)(fieldTypesPtr),
			(**C.char)(fieldNamesPtr),
			C.idx_t(structType.NumField()),
		)

		for i := 0; i < structType.NumField(); i++ {
			C.duckdb_destroy_logical_type(&fieldTypes[i])
			C.free(unsafe.Pointer(fieldNames[i]))
		}
		C.free(fieldTypesPtr)
		C.free(fieldNamesPtr)

		return colInfo
	case reflect.Map:
		panic(fmt.Sprintf("%T: the appender currently doesn't support maps", v))
	default:
		panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
	}
}

func (c *columnInfo) getChildVectors(vector C.duckdb_vector) {
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

func setNull(columnInfo *columnInfo, rowIdx C.idx_t) {
	C.duckdb_vector_ensure_validity_writable(columnInfo.vector)
	mask := C.duckdb_vector_get_validity(columnInfo.vector)
	C.duckdb_validity_set_row_invalid(mask, rowIdx)
}

func setPrimitive[T any](columnInfo *columnInfo, rowIdx C.idx_t, value T) {
	ptr := C.duckdb_vector_get_data(columnInfo.vector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = value
}

func setVarchar(columnInfo *columnInfo, rowIdx C.idx_t, value string) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element(columnInfo.vector, rowIdx, str)
	C.free(unsafe.Pointer(str))
}

func setTime(columnInfo *columnInfo, rowIdx C.idx_t, value driver.Value) {
	var dt C.duckdb_timestamp
	dt.micros = C.int64_t(value.(time.Time).UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](columnInfo, rowIdx, dt)
}

func setList(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	childcolumnInfo := columnInfo.columnInfos[0]

	if refVal.IsNil() {
		setNull(columnInfo, rowIdx)
	}

	// Convert the refVal to []any to be able to iterate over it
	values := make([]any, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		values[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(columnInfo.vector)

	// Set the offset of the list vector using the current size of the child vector.
	// The duckdb list vector contains for each row two values, the offset and the length
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(refVal.Len()),
	}

	setPrimitive[C.duckdb_list_entry](columnInfo, rowIdx, listEntry)

	C.duckdb_list_vector_set_size(columnInfo.vector, C.idx_t(refVal.Len())+childVectorSize)
	C.duckdb_list_vector_reserve(columnInfo.vector, C.idx_t(refVal.Len())+childVectorSize)

	// Insert the values into the child vector
	for i, e := range values {
		childVectorRow := C.idx_t(i) + childVectorSize
		childcolumnInfo.fn(a, &childcolumnInfo, childVectorRow, e)
	}
}

func setStruct(a *Appender, columnInfo *columnInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	structType := refVal.Type()

	if value == nil {
		setNull(columnInfo, rowIdx)
	}

	for i := 0; i < structType.NumField(); i++ {
		childcolumnInfo := columnInfo.columnInfos[i]
		childcolumnInfo.fn(a, &childcolumnInfo, rowIdx, refVal.Field(i).Interface())
	}
}

func goTypeToString(v reflect.Type) string {
	valueType := v.String()
	switch valueType {
	case "int":
		return "int64"
	case "uint":
		return "uint64"
	case "time.Time":
		return "time.Time"
	}

	if v.Kind() == reflect.Slice {
		return "[]" + goTypeToString(v.Elem())
	}

	if v.Kind() == reflect.Struct {
		s := "{"
		for i := 0; i < v.NumField(); i++ {
			if i > 0 {
				s += ", "
			}
			s += goTypeToString(v.Field(i).Type)
		}
		s += "}"
		return s
	}
	return valueType
}

func (c *columnInfo) checkMatchingType(v reflect.Type) error {
	valueType := goTypeToString(v)
	expectedType, _ := c.duckDBTypeToString()

	if valueType != expectedType {
		return fmt.Errorf("expected: %s, actual: %s", expectedType, valueType)
	}
	return nil
}

// appendRowArray loads a row of values into the appender. The values are provided as an array.
func (a *Appender) appendRowArray(args []driver.Value) error {
	for i, v := range args {
		columnInfo := a.columnInfos[i]
		if v == nil {
			setNull(&columnInfo, a.currentChunkSize)
			continue
		}

		if err := columnInfo.checkMatchingType(reflect.TypeOf(v)); err != nil {
			return fmt.Errorf("type mismatch for column %d: \n%s \n%s", i, err.Error(), errNote)
		}

		columnInfo.fn(a, &columnInfo, a.currentChunkSize, v)
	}
	a.currentChunkSize++

	return nil
}

func (a *Appender) appendChunks() error {
	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(a.chunks[len(a.chunks)-1], C.idx_t(a.currentChunkSize))

	// Append all chunks to the appender and destroy them.
	var state C.duckdb_state
	var dbErr string
	for _, chunk := range a.chunks {
		if len(dbErr) == 0 {
			state = C.duckdb_append_data_chunk(*a.appender, chunk)
			if state == C.DuckDBError {
				dbErr = C.GoString(C.duckdb_appender_error(*a.appender))
			}
		}
		C.duckdb_destroy_data_chunk(&chunk)
	}

	if len(dbErr) > 0 {
		return fmt.Errorf(`duckdb has returned an error while appending, all data has been invalidated.
Check that the data being appended matches the schema.
Struct field names must match, and are case sensitive.
Error from duckdb: %s`, dbErr)
	}

	return nil
}
