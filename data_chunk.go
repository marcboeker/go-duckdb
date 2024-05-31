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
	// The underlying duckdb data chunk.
	data C.duckdb_data_chunk
	// A helper slice providing direct access to all columns.
	columns []vector
}

// InitFromTypes initializes a data chunk by providing its column types.
func (chunk *DataChunk) InitFromTypes(ptr unsafe.Pointer, types []C.duckdb_logical_type) error {
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

// Destroy the memory of a data chunk. This is crucial to avoid leaks.
func (chunk *DataChunk) Destroy() {
	C.duckdb_destroy_data_chunk(&chunk.data)
}

// SetSize sets the internal size of the data chunk. This fails if columns have different sizes.
func (chunk *DataChunk) SetSize() error {
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

// SetValue writes a single value to a column. Note that this requires casting the type for
// each invocation. Try to use the columnar function SetColumn for performance.
func (chunk *DataChunk) SetValue(columnIdx int, rowIdx int, val any) error {
	if columnIdx >= len(chunk.columns) {
		return errDriver
	}

	column := &chunk.columns[columnIdx]

	// Ensure that the types match before attempting to set anything.
	v, err := column.tryCast(val)
	if err != nil {
		return columnError(err, columnIdx)
	}

	// Set the value.
	column.setFn(column, C.idx_t(rowIdx), v)
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(columnIdx int, rowIdx int) (any, error) {
	// TODO
	return nil, errNotImplemented
}

// SetColumn sets the column to val, where val is a slice []T. T is the type of the column.
func (chunk *DataChunk) SetColumn(columnIdx int, val any) error {
	// TODO
	return errNotImplemented
}

// GetColumn returns a slice []T containing the column values. T is the type of the column.
func (chunk *DataChunk) GetColumn(columnIdx int) (any, error) {
	// TODO
	return nil, errNotImplemented
}

// GetColumnData returns a pointer to the underlying data of a column.
func (chunk *DataChunk) GetColumnData(columnIdx int) (unsafe.Pointer, error) {
	// TODO
	return nil, errNotImplemented
}

// TODO: GetMetaData, see table UDF PR.
// TODO: Add all templated functions.
// TODO: Projection pushdown.
