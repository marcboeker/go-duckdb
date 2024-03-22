package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"reflect"
	"time"
	"unsafe"
)

var typeIdMap = map[C.duckdb_type]string{
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

var unsupportedAppenderTypeMap = map[C.duckdb_type]string{
	C.DUCKDB_TYPE_INVALID:      "INVALID",
	C.DUCKDB_TYPE_DATE:         "DATE",
	C.DUCKDB_TYPE_TIME:         "TIME",
	C.DUCKDB_TYPE_INTERVAL:     "INTERVAL",
	C.DUCKDB_TYPE_HUGEINT:      "HUGEINT",
	C.DUCKDB_TYPE_UHUGEINT:     "UHUGEINT",
	C.DUCKDB_TYPE_DECIMAL:      "DECIMAL",
	C.DUCKDB_TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	C.DUCKDB_TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	C.DUCKDB_TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	C.DUCKDB_TYPE_ENUM:         "ENUM",
	C.DUCKDB_TYPE_MAP:          "MAP",
	C.DUCKDB_TYPE_UNION:        "UNION",
	C.DUCKDB_TYPE_BIT:          "BIT",
	C.DUCKDB_TYPE_TIME_TZ:      "TIME_TZ",
	C.DUCKDB_TYPE_TIMESTAMP_TZ: "TIMESTAMP_TZ",
}

// fnSetVectorValue is the setter callback function for any (nested) vectors.
type fnSetVectorValue func(a *Appender, info *colInfo, rowIdx C.idx_t, val any)

type colInfo struct {
	vector C.duckdb_vector
	fn     fnSetVectorValue

	// The type of the column.
	duckdbType C.duckdb_type
	// The number of fields in a STRUCT column.
	numFields int
	// Recursively stores the child colInfos for nested types.
	colInfos []colInfo
}

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	con            *conn
	schema         string
	table          string
	duckdbAppender C.duckdb_appender
	closed         bool

	chunks      []C.duckdb_data_chunk
	currSize    C.idx_t
	colTypes    []C.duckdb_logical_type
	colTypesPtr unsafe.Pointer

	colInfos []colInfo
}

// NewAppenderFromConn returns a new Appender from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	con, ok := driverConn.(*conn)
	if !ok {
		return nil, getError(errAppenderInvalidCon, nil)
	}
	if con.closed {
		return nil, getError(errAppenderClosedCon, nil)
	}

	var cSchema *C.char
	if schema != "" {
		cSchema = C.CString(schema)
		defer C.free(unsafe.Pointer(cSchema))
	}

	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	var duckdbAppender C.duckdb_appender
	state := C.duckdb_appender_create(con.duckdbCon, cSchema, cTable, &duckdbAppender)

	if state == C.DuckDBError {
		// We destroy the error message when destroying the appender.
		err := duckdbError(C.duckdb_appender_error(duckdbAppender))
		C.duckdb_appender_destroy(&duckdbAppender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		con:            con,
		schema:         schema,
		table:          table,
		duckdbAppender: duckdbAppender,
		currSize:       0,
	}

	columnCount := int(C.duckdb_appender_column_count(duckdbAppender))
	a.colTypesPtr, a.colTypes = a.mallocTypeSlice(columnCount)

	// Get the column types.
	for i := 0; i < columnCount; i++ {
		a.colTypes[i] = C.duckdb_appender_column_type(duckdbAppender, C.idx_t(i))
	}

	// Get the column infos.
	a.colInfos = make([]colInfo, columnCount)
	for i := 0; i < columnCount; i++ {
		info, err := a.initColInfos(a.colTypes[i], i)
		if err != nil {
			a.destroyColumnTypes()
			C.duckdb_appender_destroy(&duckdbAppender)
			return nil, getError(errAppenderCreation, err)
		}
		a.colInfos[i] = info
	}

	return a, nil
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	// Nothing to flush.
	if len(a.chunks) == 0 && a.currSize == 0 {
		return nil
	}

	if err := a.appendDataChunks(); err != nil {
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	if state := C.duckdb_appender_flush(a.duckdbAppender); state == C.DuckDBError {
		return getError(errAppenderFlush, invalidatedAppenderError(nil))
	}

	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	var err error
	if len(a.chunks) != 0 || a.currSize != 0 {
		err = a.appendDataChunks()
	}

	a.destroyColumnTypes()
	state := C.duckdb_appender_destroy(&a.duckdbAppender)

	if err != nil || state == C.DuckDBError {
		return getError(errAppenderClose, invalidatedAppenderError(err))
	}
	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	// Create a new data chunk if the current chunk is full, or if this is the first row.
	if a.currSize == C.duckdb_vector_size() || len(a.chunks) == 0 {
		a.newDataChunk(len(args))
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}
	return nil
}

func (a *Appender) destroyColumnTypes() {
	for i := range a.colTypes {
		C.duckdb_destroy_logical_type(&a.colTypes[i])
	}
	C.free(a.colTypesPtr)
}

func (*Appender) mallocTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummy C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummy))

	ctPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	slice := (*[1 << 30]C.duckdb_logical_type)(ctPtr)[:count:count]

	return ctPtr, slice
}

func initPrimitive[T any](duckdbType C.duckdb_type) colInfo {
	info := colInfo{
		fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
			setPrimitive[T](info, rowIdx, val.(T))
		},
		duckdbType: duckdbType,
	}
	return info
}

func (a *Appender) initColInfos(logicalType C.duckdb_logical_type, colIdx int) (colInfo, error) {
	duckdbType := C.duckdb_get_type_id(logicalType)

	switch duckdbType {
	case C.DUCKDB_TYPE_UTINYINT:
		return initPrimitive[uint8](C.DUCKDB_TYPE_UTINYINT), nil
	case C.DUCKDB_TYPE_TINYINT:
		return initPrimitive[int8](C.DUCKDB_TYPE_TINYINT), nil
	case C.DUCKDB_TYPE_USMALLINT:
		return initPrimitive[uint16](C.DUCKDB_TYPE_USMALLINT), nil
	case C.DUCKDB_TYPE_SMALLINT:
		return initPrimitive[int16](C.DUCKDB_TYPE_SMALLINT), nil
	case C.DUCKDB_TYPE_UINTEGER:
		return initPrimitive[uint32](C.DUCKDB_TYPE_UINTEGER), nil
	case C.DUCKDB_TYPE_INTEGER:
		return initPrimitive[int32](C.DUCKDB_TYPE_INTEGER), nil
	case C.DUCKDB_TYPE_UBIGINT:
		return initPrimitive[uint64](C.DUCKDB_TYPE_UBIGINT), nil
	case C.DUCKDB_TYPE_BIGINT:
		return initPrimitive[int64](C.DUCKDB_TYPE_BIGINT), nil
	case C.DUCKDB_TYPE_FLOAT:
		return initPrimitive[float32](C.DUCKDB_TYPE_FLOAT), nil
	case C.DUCKDB_TYPE_DOUBLE:
		return initPrimitive[float64](C.DUCKDB_TYPE_DOUBLE), nil
	case C.DUCKDB_TYPE_BOOLEAN:
		return initPrimitive[bool](C.DUCKDB_TYPE_BOOLEAN), nil
	case C.DUCKDB_TYPE_VARCHAR:
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setCString(info, rowIdx, val.(string), len(val.(string)))
			},
			duckdbType: C.DUCKDB_TYPE_VARCHAR,
		}
		return info, nil
	case C.DUCKDB_TYPE_BLOB:
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				blob := val.([]byte)
				setCString(info, rowIdx, string(blob[:]), len(blob))
			},
			duckdbType: C.DUCKDB_TYPE_BLOB,
		}
		return info, nil
	case C.DUCKDB_TYPE_TIMESTAMP:
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setTime(info, rowIdx, val.(time.Time))
			},
			duckdbType: C.DUCKDB_TYPE_TIMESTAMP,
		}
		return info, nil
	case C.DUCKDB_TYPE_UUID:
		// The callback function casts the value via uuidToHugeInt. Thus, we do not
		// use initPrimitive here.
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setPrimitive[C.duckdb_hugeint](info, rowIdx, uuidToHugeInt(val.(UUID)))
			},
			duckdbType: C.DUCKDB_TYPE_UUID,
		}
		return info, nil

	case C.DUCKDB_TYPE_LIST:
		// We recurse into the child.
		childType := C.duckdb_list_type_child_type(logicalType)
		childInfo, err := a.initColInfos(childType, colIdx)
		C.duckdb_destroy_logical_type(&childType)
		if err != nil {
			return colInfo{}, err
		}

		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setList(a, info, rowIdx, val)
			},
			duckdbType: C.DUCKDB_TYPE_LIST,
			colInfos:   []colInfo{childInfo},
		}
		return info, nil

	case C.DUCKDB_TYPE_STRUCT:
		numFields := int(C.duckdb_struct_type_child_count(logicalType))

		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setStruct(a, info, rowIdx, val)
			},
			duckdbType: C.DUCKDB_TYPE_STRUCT,
			colInfos:   make([]colInfo, numFields),
			numFields:  numFields,
		}

		// Recurse into the children.
		for i := 0; i < numFields; i++ {
			childType := C.duckdb_struct_type_child_type(logicalType, C.idx_t(i))
			childInfo, err := a.initColInfos(childType, i)
			C.duckdb_destroy_logical_type(&childType)

			if err != nil {
				return colInfo{}, err
			}

			info.colInfos[i] = childInfo
		}

		return info, nil

	default:
		name, found := unsupportedAppenderTypeMap[duckdbType]
		if !found {
			name = "unknown type"
		}
		err := columnError(unsupportedTypeError(name), colIdx+1)
		return colInfo{}, err
	}
}

func (c *colInfo) getChildVectors(vector C.duckdb_vector) {
	switch c.duckdbType {
	case C.DUCKDB_TYPE_LIST:
		childVector := C.duckdb_list_vector_get_child(vector)
		c.colInfos[0].vector = childVector
		c.colInfos[0].getChildVectors(childVector)
	case C.DUCKDB_TYPE_STRUCT:
		for i := 0; i < c.numFields; i++ {
			childVector := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))
			c.colInfos[i].vector = childVector
			c.colInfos[i].getChildVectors(childVector)
		}
	}
}

func (a *Appender) newDataChunk(colCount int) {
	a.currSize = 0

	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count.
	colTypesPtr := (*C.duckdb_logical_type)(a.colTypesPtr)
	dataChunk := C.duckdb_create_data_chunk(colTypesPtr, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		c := &a.colInfos[i]
		c.vector = duckdbVector
		c.getChildVectors(duckdbVector)
	}

	a.chunks = append(a.chunks, dataChunk)
}

func setNull(info *colInfo, rowIdx C.idx_t) {
	C.duckdb_vector_ensure_validity_writable(info.vector)
	mask := C.duckdb_vector_get_validity(info.vector)
	C.duckdb_validity_set_row_invalid(mask, rowIdx)

	// Set the validity for all child vectors of a STRUCT.
	if typeIdMap[info.duckdbType] == "struct" {
		for i := 0; i < info.numFields; i++ {
			setNull(&info.colInfos[i], rowIdx)
		}
	}
}

func setPrimitive[T any](info *colInfo, rowIdx C.idx_t, value T) {
	ptr := C.duckdb_vector_get_data(info.vector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = value
}

func setCString(info *colInfo, rowIdx C.idx_t, value string, len int) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element_len(info.vector, rowIdx, str, C.idx_t(len))
	C.free(unsafe.Pointer(str))
}

func setTime(info *colInfo, rowIdx C.idx_t, value time.Time) {
	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(value.UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](info, rowIdx, ts)
}

func setList(a *Appender, info *colInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	childInfo := info.colInfos[0]

	if refVal.IsNil() {
		setNull(info, rowIdx)
	}

	// Convert the refVal to []any to iterate over it.
	values := make([]any, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		values[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(info.vector)

	// Set the offset and length of the list vector using the current size of the child vector.
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(refVal.Len()),
	}

	setPrimitive[C.duckdb_list_entry](info, rowIdx, listEntry)

	newLength := C.idx_t(refVal.Len()) + childVectorSize
	C.duckdb_list_vector_set_size(info.vector, newLength)
	C.duckdb_list_vector_reserve(info.vector, newLength)

	// Insert the values into the child vector.
	for i, e := range values {
		childVectorRow := C.idx_t(i) + childVectorSize
		childInfo.fn(a, &childInfo, childVectorRow, e)
	}
}

func setStruct(a *Appender, info *colInfo, rowIdx C.idx_t, value driver.Value) {
	refVal := reflect.ValueOf(value)
	structType := refVal.Type()

	if value == nil {
		setNull(info, rowIdx)
	}

	for i := 0; i < structType.NumField(); i++ {
		childInfo := info.colInfos[i]
		childInfo.fn(a, &childInfo, rowIdx, refVal.Field(i).Interface())
	}
}

func (c *colInfo) duckDBTypeToString() string {
	if c.duckdbType == C.DUCKDB_TYPE_LIST {
		s := c.colInfos[0].duckDBTypeToString()
		return "[]" + s
	}

	if c.duckdbType == C.DUCKDB_TYPE_STRUCT {
		s := "{"
		for i := 0; i < c.numFields; i++ {
			if i > 0 {
				s += ", "
			}
			tmp := c.colInfos[i].duckDBTypeToString()
			s += tmp
		}
		s += "}"
		return s
	}

	return typeIdMap[c.duckdbType]
}

func goTypeToString(v reflect.Type) string {
	switch v.String() {
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

	return v.String()
}

func (c *colInfo) typeMatch(v reflect.Type) error {
	actual := goTypeToString(v)
	expected := c.duckDBTypeToString()

	if actual != expected {
		return castError(actual, expected)
	}
	return nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	for i, v := range args {
		info := a.colInfos[i]
		if v == nil {
			setNull(&info, a.currSize)
			continue
		}

		if err := info.typeMatch(reflect.TypeOf(v)); err != nil {
			// Use 1-based indexing for readability, as we're talking about columns.
			return columnError(err, i+1)
		}
		info.fn(a, &info, a.currSize, v)
	}

	a.currSize++
	return nil
}

func (a *Appender) appendDataChunks() error {
	// Set the size of the current chunk to the current row count.
	C.duckdb_data_chunk_set_size(a.chunks[len(a.chunks)-1], C.idx_t(a.currSize))

	// Append all chunks to the appender and destroy them.
	var state C.duckdb_state
	var err error

	for _, chunk := range a.chunks {
		state = C.duckdb_append_data_chunk(a.duckdbAppender, chunk)
		if state == C.DuckDBError {
			err = duckdbError(C.duckdb_appender_error(a.duckdbAppender))
			break
		}
	}
	a.destroyDataChunks()
	return err
}

func (a *Appender) destroyDataChunks() {
	for _, chunk := range a.chunks {
		C.duckdb_destroy_data_chunk(&chunk)
	}
	a.currSize = 0
	a.chunks = a.chunks[:0]
}
