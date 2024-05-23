package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"unsafe"
)

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

	// The vector storage of each column in the data chunk.
	vectors []vector
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

	// Get the vector storage of each column.
	a.vectors = make([]vector, columnCount)
	var err error
	for i := 0; i < columnCount; i++ {
		if err = a.vectors[i].init(a.colTypes[i], i); err != nil {
			break
		}
	}
	if err != nil {
		a.destroyColumnTypes()
		C.duckdb_appender_destroy(&duckdbAppender)
		return nil, getError(errAppenderCreation, err)
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

	state := C.duckdb_appender_flush(a.duckdbAppender)
	if state == C.DuckDBError {
		err := duckdbError(C.duckdb_appender_error(a.duckdbAppender))
		return getError(errAppenderFlush, invalidatedAppenderError(err))
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
		// We destroyed the appender, so we cannot retrieve the duckdb error.
		return getError(errAppenderClose, invalidatedAppenderError(err))
	}
	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
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

func (a *Appender) newDataChunk(colCount int) {
	a.currSize = 0

	// duckdb_create_data_chunk takes an array of duckdb_logical_type and a column count.
	colTypesPtr := (*C.duckdb_logical_type)(a.colTypesPtr)
	dataChunk := C.duckdb_create_data_chunk(colTypesPtr, C.idx_t(colCount))
	C.duckdb_data_chunk_set_size(dataChunk, C.duckdb_vector_size())

	for i := 0; i < colCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(dataChunk, C.idx_t(i))
		a.vectors[i].duckdbVector = duckdbVector
		a.vectors[i].getChildVectors(duckdbVector)
	}

	a.chunks = append(a.chunks, dataChunk)
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// early-out, if the number of args does not match the column count
	if len(args) != len(a.vectors) {
		return columnCountError(len(args), len(a.vectors))
	}

	// Create a new data chunk if the current chunk is full, or if this is the first row.
	if a.currSize == C.duckdb_vector_size() || len(a.chunks) == 0 {
		a.newDataChunk(len(args))
	}

	for i, val := range args {
		vec := a.vectors[i]

		// Ensure that the types match before attempting to append anything.
		v, err := vec.tryCast(val)
		if err != nil {
			// Use 1-based indexing for readability, as we're talking about columns.
			return columnError(err, i+1)
		}

		// Append the row to the data chunk.
		vec.fn(&vec, a.currSize, v)
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
