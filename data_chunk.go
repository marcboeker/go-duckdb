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
	// columnNames holds the column names, if known.
	columnNames []string
	// size caches the size after initialization.
	size int
}

// GetDataChunkCapacity returns the capacity of a data chunk.
func GetDataChunkCapacity() int {
	return int(C.duckdb_vector_size())
}

// GetSize returns the internal size of the data chunk.
func (chunk *DataChunk) GetSize() int {
	chunk.size = int(C.duckdb_data_chunk_get_size(chunk.data))
	return chunk.size
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	C.duckdb_data_chunk_set_size(chunk.data, C.idx_t(size))
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(colIdx int, rowIdx int) (any, error) {
	if colIdx >= len(chunk.columns) {
		return nil, getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]
	return column.getFn(column, C.idx_t(rowIdx)), nil
}

// SetValue writes a single value to a column in a data chunk. Note that this requires casting the type for each invocation.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx int, rowIdx int, val any) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]

	// Ensure that the types match before attempting to set anything.
	// This is done to prevent failures 'halfway through' writing column values,
	// potentially corrupting data in that column.
	// FIXME: Can we improve efficiency here? We are casting back-and-forth to any A LOT.
	// FIXME: Maybe we can make columnar insertions unsafe, i.e., we always assume a correct type.
	v, err := column.tryCast(val)
	if err != nil {
		return columnError(err, colIdx)
	}

	// Set the value.
	column.setFn(column, C.idx_t(rowIdx), v)
	return nil
}

func (chunk *DataChunk) initFromTypes(ptr unsafe.Pointer, types []C.duckdb_logical_type, writable bool) error {
	// NOTE: initFromTypes does not initialize the column names.
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
		v := C.duckdb_data_chunk_get_vector(chunk.data, C.idx_t(i))
		chunk.columns[i].initVectors(v, writable)
	}
	return nil
}

func (chunk *DataChunk) initFromDuckDataChunk(data C.duckdb_data_chunk, writable bool) error {
	columnCount := int(C.duckdb_data_chunk_get_column_count(data))
	chunk.columns = make([]vector, columnCount)
	chunk.data = data

	var err error
	for i := 0; i < columnCount; i++ {
		duckdbVector := C.duckdb_data_chunk_get_vector(data, C.idx_t(i))

		// Initialize the callback functions to read and write values.
		logicalType := C.duckdb_vector_get_column_type(duckdbVector)
		err = chunk.columns[i].init(logicalType, i)
		C.duckdb_destroy_logical_type(&logicalType)
		if err != nil {
			break
		}

		// Initialize the vector and its child vectors.
		chunk.columns[i].initVectors(duckdbVector, writable)
	}

	chunk.GetSize()
	return err
}

func (chunk *DataChunk) close() {
	C.duckdb_destroy_data_chunk(&chunk.data)
}
