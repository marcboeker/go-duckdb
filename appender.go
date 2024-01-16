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

// Appender holds the DuckDB appender. It allows to load bulk data into a DuckDB database.
type Appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool

	currentRow       C.idx_t
	chunks           []C.duckdb_data_chunk
	chunkVectors     []C.duckdb_vector
	currentChunkIdx  int
	currentChunkSize C.idx_t
	chunkTypes       *C.duckdb_logical_type
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

	var chunkSize = C.duckdb_vector_size()

	return &Appender{c: dbConn, schema: schema, table: table, appender: &a, currentRow: 0, currentChunkIdx: 0, currentChunkSize: chunkSize}, nil
}

// Error returns the last DuckDB appender error.
func (a *Appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

// Flush the appender to the underlying table and clear the internal cache.
func (a *Appender) Flush() error {
	if a.currentChunkIdx == 0 && a.currentRow == 0 {
		return nil
	}

	err := a.appendChunks()
	if err != nil {
		return err
	}

	if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}

	a.currentRow = 0
	a.currentChunkIdx = 0
	a.chunks = a.chunks[:0]
	return nil
}

// Closes closes the appender.
func (a *Appender) Close() error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Appender")
	}

	a.closed = true
	// append chunks if not already done via flush
	if a.currentChunkIdx != 0 || a.currentRow != 0 {
		if err := a.appendChunks(); err != nil {
			return err
		}
	}

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
		a.initializeChunkTypes(args)
		err = a.addChunk(len(args))
		// If the current chunk is full, create a new one
	} else if a.currentRow == C.duckdb_vector_size() {
		a.currentChunkIdx++
		err = a.addChunk(len(args))
	}

	if err != nil {
		return err
	}

	return a.appendRowArray(args)
}

// Create an array of duckdb types from a list of go types
func (a *Appender) initializeChunkTypes(args []driver.Value) {
	defaultLogicalType := C.duckdb_create_logical_type(0)
	rowTypes := C.malloc(C.size_t(len(args)) * C.size_t(unsafe.Sizeof(defaultLogicalType)))

	tmpChunkTypes := (*[1<<30 - 1]C.duckdb_logical_type)(rowTypes)

	for i, val := range args {
		switch v := val.(type) {
		case uint8:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT)
		case int8:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT)
		case uint16:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT)
		case int16:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT)
		case uint32:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER)
		case int32:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER)
		case uint64, uint:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT)
		case int64, int:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT)
		case float32:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT)
		case float64:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE)
		case bool:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN)
		case []byte:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_BLOB)
		case [16]byte:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID)
		case string:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR)
		case time.Time:
			tmpChunkTypes[i] = C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP)
		default:
			panic(fmt.Sprintf("couldn't append unsupported parameter %T", v))
		}
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
		a.chunkVectors = append(a.chunkVectors, vector)
	}

	a.chunks = append(a.chunks, dataChunk)
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

		switch v := v.(type) {
		case uint8:
			set[uint8](a.chunkVectors[i], a.currentRow, v)
		case int8:
			set[int8](a.chunkVectors[i], a.currentRow, v)
		case uint16:
			set[uint16](a.chunkVectors[i], a.currentRow, v)
		case int16:
			set[int16](a.chunkVectors[i], a.currentRow, v)
		case uint32:
			set[uint32](a.chunkVectors[i], a.currentRow, v)
		case int32:
			set[int32](a.chunkVectors[i], a.currentRow, v)
		case uint64:
			set[uint64](a.chunkVectors[i], a.currentRow, v)
		case int64:
			set[int64](a.chunkVectors[i], a.currentRow, v)
		case uint:
			set[uint](a.chunkVectors[i], a.currentRow, v)
		case int:
			set[int](a.chunkVectors[i], a.currentRow, v)
		case float32:
			set[float32](a.chunkVectors[i], a.currentRow, v)
		case float64:
			set[float64](a.chunkVectors[i], a.currentRow, v)
		case bool:
			set[bool](a.chunkVectors[i], a.currentRow, v)
		case []byte:
			set[[]byte](a.chunkVectors[i], a.currentRow, v)
		case [16]byte:
			set[C.duckdb_hugeint](a.chunkVectors[i], a.currentRow, uuidToHugeInt(v))
		case string:
			str := C.CString(v)
			C.duckdb_vector_assign_string_element(a.chunkVectors[i], C.uint64_t(a.currentRow), str)
			C.free(unsafe.Pointer(str))
		case time.Time:
			var dt C.duckdb_timestamp
			dt.micros = C.int64_t(v.UTC().UnixMicro())
			set[C.duckdb_timestamp](a.chunkVectors[i], a.currentRow, dt)
		default:
			return fmt.Errorf("couldn't append unsupported parameter %d (type %T)", i, v)
		}
	}

	a.currentRow++
	return nil
}

func (a *Appender) appendChunks() error {
	// set the size of the current chunk to the current row
	C.duckdb_data_chunk_set_size(a.chunks[a.currentChunkIdx], C.uint64_t(a.currentRow))

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
	return nil
}

var errCouldNotAppend = errors.New("could not append parameter")
