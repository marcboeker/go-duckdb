package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

// dataChunk storage of a DuckDB table.
type dataChunk struct {
	// The underlying duckdb data chunk.
	data C.duckdb_data_chunk
	// A helper slice providing direct access to all columns.
	columns []vector
}

func (chunk *dataChunk) init(ptr unsafe.Pointer, types []C.duckdb_logical_type) error {
	columnCount := len(types)

	// Get the vector storage of each column.
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

	for i := 0; i < columnCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(chunk.data, C.idx_t(i))
		chunk.columns[i].duckdbVector = duckdbVector
		chunk.columns[i].getChildVectors(duckdbVector)
	}
	return nil
}

func (chunk *dataChunk) destroy() {
	C.duckdb_destroy_data_chunk(&chunk.data)
}

func (chunk *dataChunk) setSize() error {
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
		return errors.New("this is not valid oh no!")
	}
	C.duckdb_data_chunk_set_size(chunk.data, maxSize)
	return nil
}
