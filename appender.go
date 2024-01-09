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

// Appender holds the DuckDB appender. It allows to load bulk data into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	parent Parent
}

type Parent struct {
	chunkInfo  *ChunkInfo
	structInfo map[int]NestedInfo
}

type ChunkInfo struct {
	currentRow       C.idx_t
	chunks           []C.duckdb_data_chunk
	chunkVectors     []C.duckdb_vector
	currentChunkIdx  int
	currentChunkSize C.idx_t
	chunkTypes       *C.duckdb_logical_type

	parent *Parent
}

type NestedInfo struct {
	childVectors []C.duckdb_vector

	// only used for structs
	fields int

	parent *Parent
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

	c := ChunkInfo{currentRow: 0, currentChunkIdx: 0, currentChunkSize: C.duckdb_vector_size()}
	p := Parent{chunkInfo: &c, structInfo: make(map[int]NestedInfo)}
	return &Appender{c: dbConn, schema: schema, table: table, appender: &a, parent: p}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
func (a *Appender) Flush() error {
	// retrieve chunk info from parent
	chunkInfo := a.parent.chunkInfo

	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(chunkInfo.chunks[chunkInfo.currentChunkIdx], C.uint64_t(chunkInfo.currentRow))

	// append all chunks to the appender and destroy them
	var state C.duckdb_state
	for i, chunk := range chunkInfo.chunks {
		state = C.duckdb_append_data_chunk(*a.appender, chunk)
		if state == C.DuckDBError {
			dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
			return fmt.Errorf("duckdb error appending chunk %d of %d: %s", i+1, chunkInfo.currentChunkIdx+1, dbErr)
		}
		C.duckdb_destroy_data_chunk(&chunk)
	}

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
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	// retrieve chunk info from parent
	chunkInfo := a.parent.chunkInfo

	var err error
	// Initialize the chunk on the first call
	if len(chunkInfo.chunks) == 0 {
		a.initializeChunkTypes(args)
		err = a.addChunk(len(args))
		// If the current chunk is full, create a new one
	} else if chunkInfo.currentRow == C.duckdb_vector_size() {
		chunkInfo.currentChunkIdx++
		err = a.addChunk(len(args))
	}

	if err != nil {
		return err
	}

	return a.appendRowArray(args)
}

func (a Appender) CreateDuckDBLogicalType(val driver.Value, col int) C.duckdb_logical_type {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Uint8:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT)
	case reflect.Int8:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT)
	case reflect.Uint16:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT)
	case reflect.Int16:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT)
	case reflect.Uint32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER)
	case reflect.Int32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER)
	case reflect.Uint64, reflect.Uint:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT)
	case reflect.Int64, reflect.Int:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT)
	case reflect.Float32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT)
	case reflect.Float64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE)
	case reflect.Bool:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN)
	case reflect.String:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
	case reflect.Slice:
		// Check if it's []byte
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB)
		}

		// Otherwise, it's a list
		return C.duckdb_create_list_type(a.CreateDuckDBLogicalType(v.Index(0).Interface(), col))
	case reflect.Struct:
		// Check if it's time.Time
		if (v.Type() == reflect.TypeOf(time.Time{})) {
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP)
		}

		// Otherwise, it's a struct
		structType := v.Type()
		structValue := v

		// Create an array of the struct fields TYPES
		// and an array of the struct fields NAMES
		fieldTypes := make([]C.duckdb_logical_type, structType.NumField())
		fieldNames := make([]*C.char, structType.NumField())
		for i := 0; i < structType.NumField(); i++ {
			fieldTypes[i] = a.CreateDuckDBLogicalType(structValue.Field(i).Interface(), i)
			fieldNames[i] = C.CString(structType.Field(i).Name)
		}

		fieldTypesPtr := (*C.duckdb_logical_type)(unsafe.Pointer(&fieldTypes[0]))
		fieldNamesPtr := (**C.char)(unsafe.Pointer(&fieldNames[0]))

		structLogicalType := C.duckdb_create_struct_type(fieldTypesPtr, fieldNamesPtr, C.uint64_t(structType.NumField()))

		for i := 0; i < structType.NumField(); i++ {
			C.free(unsafe.Pointer(fieldNames[i]))
			C.free(unsafe.Pointer(fieldTypes[i]))
		}
		a.structInfo[col] = NestedInfo{fields: structType.NumField(), structInfo: make(map[int]NestedInfo)}

		return structLogicalType
	default:
		panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
	}

	return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID)
}

// Create an array of duckdb types from a list of go types
func (a *Appender) initializeChunkTypes(args []driver.Value) {
	defaultLogicalType := C.duckdb_create_logical_type(0)
	rowTypes := C.malloc(C.size_t(len(args)) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	tmpChunkTypes := (*[1<<30 - 1]C.duckdb_logical_type)(rowTypes)

	for i, val := range args {
		tmpChunkTypes[i] = a.CreateDuckDBLogicalType(val, i)
	}

	a.chunkTypes = (*C.duckdb_logical_type)(rowTypes)
}

func (a *Appender) addChunk(colCount int) error {
	a.currentRow = 0

	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count
	dataChunk := C.duckdb_create_data_chunk(a.chunkTypes, C.uint64_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.uint64_t(a.currentChunkSize))

	// reset the chunkVectors array if they've been previously set
	if a.chunkVectors != nil {
		a.chunkVectors = nil
	}

	for i := 0; i < colCount; i++ {
		vector := C.duckdb_data_chunk_get_vector(dataChunk, C.uint64_t(i))
		if vector == nil {
			return fmt.Errorf("error while appending column %d", i)
		}
		// check if its a struct and create a child vector for each field
		if _, ok := a.structInfo[i]; ok {
			s := a.structInfo[i]
			for j := 0; j < a.structInfo[i].fields; j++ {
				childVector := C.duckdb_struct_vector_get_child(vector, C.idx_t(j))
				s.childVectors = append(s.childVectors, childVector)
			}
			a.structInfo[i] = s
		}
		a.chunkVectors = append(a.chunkVectors, vector)
	}

	a.chunks = append(a.chunks, dataChunk)
	return nil
}

func (a *Appender) setSlice(refVal reflect.Value, chunkVector C.duckdb_vector, row C.idx_t, col int) error {
	// Convert the refVal to []interface{} to be able to iterate over it
	interfaceSlice := make([]interface{}, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		interfaceSlice[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(chunkVector)

	// Set the offset of the list vector using the current size of the child vector
	set[uint64](chunkVector, row*2, uint64(childVectorSize))

	// Set the length of the list vector
	set[uint64](chunkVector, row*2+1, uint64(refVal.Len()))

	C.duckdb_list_vector_set_size(chunkVector, C.uint64_t(refVal.Len())+childVectorSize)

	childVector := C.duckdb_list_vector_get_child(chunkVector)
	// Insert the values into the child vector
	for i, e := range interfaceSlice {
		childVectorRow := C.idx_t(i) + childVectorSize
		// Increase the child vector capacity if over standard vector size by standard vector size
		if childVectorRow%C.duckdb_vector_size() == 0 {
			C.duckdb_list_vector_reserve(chunkVector, childVectorRow+C.duckdb_vector_size())
		}
		err := a.setRow(e, childVector, childVectorRow, col)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *Appender) setRow(val driver.Value, chunkVector C.duckdb_vector, row C.idx_t, col int) error {
	refVal := reflect.ValueOf(val)
	switch refVal.Kind() {
	case reflect.Uint8:
		set[uint8](chunkVector, row, val.(uint8))
	case reflect.Int8:
		set[int8](chunkVector, row, val.(int8))
	case reflect.Uint16:
		set[uint16](chunkVector, row, val.(uint16))
	case reflect.Int16:
		set[int16](chunkVector, row, val.(int16))
	case reflect.Uint32:
		set[uint32](chunkVector, row, val.(uint32))
	case reflect.Int32:
		set[int32](chunkVector, row, val.(int32))
	case reflect.Uint64:
		set[uint64](chunkVector, row, val.(uint64))
	case reflect.Int64:
		set[int64](chunkVector, row, val.(int64))
	case reflect.Uint:
		set[uint](chunkVector, row, val.(uint))
	case reflect.Int:
		set[int](chunkVector, row, val.(int))
	case reflect.Float32:
		set[float32](chunkVector, row, val.(float32))
	case reflect.Float64:
		set[float64](chunkVector, row, val.(float64))
	case reflect.Bool:
		set[bool](chunkVector, row, val.(bool))
	case reflect.String:
		str := C.CString(val.(string))
		C.duckdb_vector_assign_string_element(chunkVector, C.uint64_t(row), str)
		C.free(unsafe.Pointer(str))
	case reflect.Slice:
		// Check if it's []byte
		if refVal.Type().Elem().Kind() == reflect.Uint8 {
			set[[]byte](chunkVector, row, val.([]byte))
			return nil
		}

		// Otherwise, it's a list
		if refVal.Type().Elem().Kind() != reflect.Interface {
			return a.setSlice(refVal, chunkVector, row, col)
		}
	case reflect.Struct:
		// Check if it's time.Time
		structType := refVal.Type()
		if (structType == reflect.TypeOf(time.Time{})) {
			var dt C.duckdb_timestamp
			dt.micros = C.int64_t(val.(time.Time).UTC().UnixMicro())
			set[C.duckdb_timestamp](chunkVector, row, dt)
			return nil
		}

		// Otherwise, it's a struct
		for i := 0; i < structType.NumField(); i++ {
			err := a.setRow(refVal.Field(i).Interface(), a.structInfo[col].childVectors[i], a.currentRow, i)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("couldn't append unsupported parameter %d (type %T)", row, val)
	}

	return nil
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

		err := a.setRow(v, a.chunkVectors[i], a.currentRow, i)
		if err != nil {
			return err
		}
	}

	a.currentRow++
	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
