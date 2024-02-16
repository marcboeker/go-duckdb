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

const errNote = "NOTE: this is based on types initialized from the first row of appended data. Please confirm this matches the schema."

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

// SetColValue is the type definition for all column callback functions.
type SetColValue func(a *Appender, info *colInfo, rowIdx C.idx_t, val any)

// colInfo holds a column's underlying vector, a callback function to write column values to this vector, and additional helper fields.
type colInfo struct {
	vector C.duckdb_vector
	fn     SetColValue

	// The type of the column.
	ddbType C.duckdb_type
	// The number of fields in a STRUCT column.
	numFields int
	// Recursively stores the child colInfos for nested types.
	colInfos []colInfo
}

func (c *colInfo) duckDBTypeToString() string {
	if c.ddbType == C.DUCKDB_TYPE_LIST {
		s := c.colInfos[0].duckDBTypeToString()
		return "[]" + s
	}

	if c.ddbType == C.DUCKDB_TYPE_STRUCT {
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

	return typeIdMap[c.ddbType]
}

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	chunks      []C.duckdb_data_chunk
	currSize    C.idx_t
	colTypes    []C.duckdb_logical_type
	colTypesPtr unsafe.Pointer

	colInfos []colInfo
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
		c:        dbConn,
		schema:   schema,
		table:    table,
		appender: &appender,
		currSize: 0,
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
	if len(a.chunks) == 0 && a.currSize == 0 {
		return nil
	}

	if err := a.appendChunks(); err != nil {
		return err
	}

	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		return a.Error()
	}

	a.currSize = 0
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

	// Append chunks if not already done via Flush.
	var err error
	if len(a.chunks) != 0 || a.currSize != 0 {
		err = a.appendChunks()
	}

	// Free the column types.
	for i := range a.colInfos {
		C.duckdb_destroy_logical_type(&a.colTypes[i])
	}
	C.free(a.colTypesPtr)

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

	var err error
	if len(a.colTypes) == 0 {
		// Initialize the chunk on the first call.
		if err = a.initColTypes(args); err != nil {
			return err
		}
		err = a.appendChunk(len(args))

	} else if a.currSize == C.duckdb_vector_size() || len(a.chunks) == 0 {
		// The current chunk is full, create a new one.
		err = a.appendChunk(len(args))
	}

	if err != nil {
		return err
	}
	return a.appendRowArray(args)
}

// Create an array of DuckDB types from a list of Go types.
func (a *Appender) initColTypes(args []driver.Value) error {
	a.colInfos = make([]colInfo, len(args))
	a.colTypesPtr, a.colTypes = mallocLogicalTypeSlice(len(args))

	for i, val := range args {
		if val == nil {
			return fmt.Errorf("the first row cannot contain null values (column %d)", i)
		}

		v := reflect.ValueOf(val)
		a.colInfos[i], a.colTypes[i] = a.initColInfos(v.Type(), i)
	}

	return nil
}

func mallocLogicalTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummy C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummy))

	ctPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	slice := (*[1 << 30]C.duckdb_logical_type)(ctPtr)[:count:count]

	return ctPtr, slice
}

func mallocCStringSlice(count int) (unsafe.Pointer, []*C.char) {
	var dummy *C.char
	size := C.size_t(unsafe.Sizeof(dummy))

	csPtr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	slice := (*[1 << 30]*C.char)(csPtr)[:count:count]
	return csPtr, slice
}

func initPrimitive[T any](ddbType C.duckdb_type) (colInfo, C.duckdb_logical_type) {
	t := C.duckdb_create_logical_type(ddbType)
	info := colInfo{
		fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
			setPrimitive[T](info, rowIdx, val.(T))
		},
		ddbType: ddbType,
	}
	return info, t
}

func (a *Appender) initColInfos(v reflect.Type, colIdx int) (colInfo, C.duckdb_logical_type) {
	switch v.Kind() {
	case reflect.Uint8:
		return initPrimitive[uint8](C.DUCKDB_TYPE_UTINYINT)
	case reflect.Int8:
		return initPrimitive[int8](C.DUCKDB_TYPE_TINYINT)
	case reflect.Uint16:
		return initPrimitive[uint16](C.DUCKDB_TYPE_USMALLINT)
	case reflect.Int16:
		return initPrimitive[int16](C.DUCKDB_TYPE_SMALLINT)
	case reflect.Uint32:
		return initPrimitive[uint32](C.DUCKDB_TYPE_UINTEGER)
	case reflect.Int32:
		return initPrimitive[int32](C.DUCKDB_TYPE_INTEGER)
	case reflect.Uint64:
		return initPrimitive[uint64](C.DUCKDB_TYPE_UBIGINT)
	case reflect.Int64:
		return initPrimitive[int64](C.DUCKDB_TYPE_BIGINT)
	case reflect.Uint:
		return initPrimitive[uint32](C.DUCKDB_TYPE_UINTEGER)
	case reflect.Int:
		return initPrimitive[int32](C.DUCKDB_TYPE_INTEGER)
	case reflect.Float32:
		return initPrimitive[float32](C.DUCKDB_TYPE_FLOAT)
	case reflect.Float64:
		return initPrimitive[float64](C.DUCKDB_TYPE_DOUBLE)
	case reflect.Bool:
		return initPrimitive[bool](C.DUCKDB_TYPE_BOOLEAN)
	case reflect.String:
		t := C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setCString(info, rowIdx, val.(string))
			},
			ddbType: C.DUCKDB_TYPE_VARCHAR,
		}
		return info, t

	case reflect.Slice:
		// Check if it's []byte since that is equivalent to the DuckDB BLOB type.
		// If so, we can use the same setter as for VARCHAR;
		// otherwise it will not match the table set up by the user.
		if v.Elem().Kind() == reflect.Uint8 {
			t := C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB)
			info := colInfo{
				fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
					blob := val.([]byte)
					setCString(info, rowIdx, string(blob[:]))
				},
				ddbType: C.DUCKDB_TYPE_BLOB,
			}
			return info, t
		}

		// Otherwise, it's a LIST. We recurse into the child element type.
		childInfo, childType := a.initColInfos(v.Elem(), colIdx)
		defer C.duckdb_destroy_logical_type(&childType)

		t := C.duckdb_create_list_type(childType)
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setList(a, info, rowIdx, val)
			},
			ddbType:  C.DUCKDB_TYPE_LIST,
			colInfos: []colInfo{childInfo},
		}
		return info, t

	case reflect.TypeOf(UUID{}).Kind():
		// The callback function casts the value via uuidToHugeInt. Thus, we do not
		// use initPrimitive here.
		t := C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID)
		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setPrimitive[C.duckdb_hugeint](info, rowIdx, uuidToHugeInt(val.(UUID)))
			},
			ddbType: C.DUCKDB_TYPE_UUID,
		}
		return info, t

	case reflect.Struct:
		// Check if it's time.Time since that is equivalent to the DuckDB TIMESTAMP type.
		// If so, we can use setTime; otherwise it will not match the table set up by the user.
		if (v == reflect.TypeOf(time.Time{})) {
			t := C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP)
			info := colInfo{
				fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
					setTime(info, rowIdx, val.(time.Time))
				},
				ddbType: C.DUCKDB_TYPE_TIMESTAMP,
			}
			return info, t
		}

		// Otherwise, it's a STRUCT.
		numFields := v.NumField()

		info := colInfo{
			fn: func(a *Appender, info *colInfo, rowIdx C.idx_t, val any) {
				setStruct(a, info, rowIdx, val)
			},
			ddbType:   C.DUCKDB_TYPE_STRUCT,
			colInfos:  make([]colInfo, numFields),
			numFields: numFields,
		}

		// We recurse into the child numFields. To create the resulting duckdb_logical_type,
		// we create an array of the field's types, and an array of their names.
		typesPtr, types := mallocLogicalTypeSlice(numFields)
		namesPtr, names := mallocCStringSlice(numFields)

		for i := 0; i < numFields; i++ {
			childInfo, t := a.initColInfos(v.Field(i).Type, i)
			info.colInfos[i] = childInfo

			types[i] = t
			names[i] = C.CString(v.Field(i).Name)
		}

		t := C.duckdb_create_struct_type(
			(*C.duckdb_logical_type)(typesPtr), // array of child types
			(**C.char)(namesPtr),               // array of child names
			C.idx_t(numFields),                 // number of children
		)

		for i := 0; i < numFields; i++ {
			C.duckdb_destroy_logical_type(&types[i])
			C.free(unsafe.Pointer(names[i]))
		}
		C.free(typesPtr)
		C.free(namesPtr)

		return info, t

	case reflect.Map:
		panic(fmt.Sprintf("%T: the appender does not support maps", v))
	default:
		panic(fmt.Sprintf("could not append unsupported parameter %T", v))
	}
}

func (c *colInfo) getChildVectors(vector C.duckdb_vector) {
	switch c.ddbType {
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

func (a *Appender) appendChunk(colCount int) error {
	a.currSize = 0
	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count.
	colTypesPtr := (*C.duckdb_logical_type)(a.colTypesPtr)
	dataChunk := C.duckdb_create_data_chunk(colTypesPtr, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		vector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		if vector == nil {
			panic(fmt.Sprintf("error while appending column %d", i))
		}
		c := &a.colInfos[i]
		c.vector = vector
		c.getChildVectors(vector)
	}

	a.chunks = append(a.chunks, dataChunk)
	return nil
}

func setNull(info *colInfo, rowIdx C.idx_t) {
	C.duckdb_vector_ensure_validity_writable(info.vector)
	mask := C.duckdb_vector_get_validity(info.vector)
	C.duckdb_validity_set_row_invalid(mask, rowIdx)

	// Set the validity for all child vectors of a STRUCT.
	if typeIdMap[info.ddbType] == "struct" {
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

func setCString(info *colInfo, rowIdx C.idx_t, value string) {
	str := C.CString(value)
	C.duckdb_vector_assign_string_element(info.vector, rowIdx, str)
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
		return fmt.Errorf("expected: %s, actual: %s", expected, actual)
	}
	return nil
}

// appendRowArray loads a row of values into the appender. The values are provided as an array.
func (a *Appender) appendRowArray(args []driver.Value) error {
	for i, v := range args {
		info := a.colInfos[i]
		if v == nil {
			setNull(&info, a.currSize)
			continue
		}

		if err := info.typeMatch(reflect.TypeOf(v)); err != nil {
			return fmt.Errorf("type mismatch for column %d: \n%s \n%s", i, err.Error(), errNote)
		}
		info.fn(a, &info, a.currSize, v)
	}

	a.currSize++
	return nil
}

func (a *Appender) appendChunks() error {
	// Set the size of the current chunk to the current row.
	C.duckdb_data_chunk_set_size(a.chunks[len(a.chunks)-1], C.idx_t(a.currSize))

	// Append all chunks to the appender and destroy them.
	var state C.duckdb_state
	var dbErr string

	for _, chunk := range a.chunks {
		if dbErr == "" {
			state = C.duckdb_append_data_chunk(*a.appender, chunk)
			if state == C.DuckDBError {
				dbErr = C.GoString(C.duckdb_appender_error(*a.appender))
			}
		}
		// To avoid memory leaks, we have to destroy the chunks even if the appender returns an error.
		C.duckdb_destroy_data_chunk(&chunk)
	}

	if dbErr != "" {
		return fmt.Errorf(`duckdb has returned an error while appending, all data has been invalidated.
Check that the data being appended matches the schema.
Struct field names must match, and are case sensitive.
DuckDB error: %s`, dbErr)
	}

	return nil
}
