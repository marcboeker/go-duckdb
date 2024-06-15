package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"unsafe"
)

// DataChunk storage of a DuckDB table.
type DataChunk struct {
	// data holds the underlying duckdb data chunk.
	data C.duckdb_data_chunk
	// columns is a helper slice providing direct access to all columns.
	columns []vector
}

// SetValue writes a single value to a column in a data chunk. Note that this requires casting the type for each invocation.
func (chunk *DataChunk) SetValue(colIdx int, rowIdx int, val any) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]

	// Ensure that the types match before attempting to set anything.
	v, err := column.tryCast(val)
	if err != nil {
		return columnError(err, colIdx)
	}

	// Set the value.
	column.setFn(column, C.idx_t(rowIdx), v)
	return nil
}

func (chunk *DataChunk) initFromTypes(ptr unsafe.Pointer, types []C.duckdb_logical_type) error {
	columnCount := len(types)

	// Initialize the callback functions to read and write values.
	chunk.columns = make([]vector, columnCount)
	var err error
	for i := 0; i < columnCount; i++ {
		if err = chunk.columns[i].init(types[i], i); err != nil {
			break
		}
	}
	if err != nil {
		return err
	}

	logicalTypesPtr := (*C.duckdb_logical_type)(ptr)
	chunk.data = C.duckdb_create_data_chunk(logicalTypesPtr, C.idx_t(columnCount))
	C.duckdb_data_chunk_set_size(chunk.data, C.duckdb_vector_size())

	// Initialize the vectors and their child vectors.
	for i := 0; i < columnCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(chunk.data, C.idx_t(i))
		chunk.columns[i].duckdbVector = duckdbVector
		chunk.columns[i].getChildVectors(duckdbVector)
	}
	return nil
}

func (chunk *DataChunk) close() {
	C.duckdb_destroy_data_chunk(&chunk.data)
}

func (chunk *DataChunk) setSize() error {
	if len(chunk.columns) == 0 {
		C.duckdb_data_chunk_set_size(chunk.data, C.idx_t(0))
		return nil
	}

	allEqual := true
	maxSize := C.idx_t(chunk.columns[0].size)
	for i := 0; i < len(chunk.columns); i++ {
		if chunk.columns[i].size != maxSize {
			allEqual = false
		}
		if chunk.columns[i].size > maxSize {
			maxSize = chunk.columns[i].size
		}
	}

	if !allEqual {
		return errDriver
	}
	C.duckdb_data_chunk_set_size(chunk.data, maxSize)
	return nil
}
