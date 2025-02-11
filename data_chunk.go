package duckdb

import "C"
import (
	"sync"
)

// DataChunk storage of a DuckDB table.
type DataChunk struct {
	// data holds the underlying duckdb data chunk.
	apiChunk apiDataChunk
	// columns is a helper slice providing direct access to all columns.
	columns []vector
	// columnNames holds the column names, if known.
	columnNames []string
	// size caches the size after initialization.
	size int
}

var GetDataChunkCapacity = sync.OnceValue[int](func() int { return int(apiVectorSize()) })

// GetSize returns the internal size of the data chunk.
func (chunk *DataChunk) GetSize() int {
	chunk.size = int(apiDataChunkGetSize(chunk.apiChunk))
	return chunk.size
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	apiDataChunkSetSize(chunk.apiChunk, uint64(size))
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(colIdx int, rowIdx int) (any, error) {
	if colIdx >= len(chunk.columns) {
		return nil, getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}

	column := &chunk.columns[colIdx]
	return column.getFn(column, uint64(rowIdx)), nil
}

// SetValue writes a single value to a column in a data chunk.
// Note that this requires casting the type for each invocation.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx int, rowIdx int, val any) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}

	column := &chunk.columns[colIdx]
	return column.setFn(column, uint64(rowIdx), val)
}

// SetChunkValue writes a single value to a column in a data chunk.
// The difference with `chunk.SetValue` is that `SetChunkValue` does not
// require casting the value to `any` (implicitly).
// NOTE: Custom ENUM types must be passed as string.
func SetChunkValue[T any](chunk DataChunk, colIdx int, rowIdx int, val T) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	return setVectorVal(&chunk.columns[colIdx], uint64(rowIdx), val)
}

func (chunk *DataChunk) initFromTypes(types []apiLogicalType, writable bool) error {
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

	chunk.apiChunk = apiCreateDataChunk(types)
	apiDataChunkSetSize(chunk.apiChunk, uint64(GetDataChunkCapacity()))

	// Initialize the vectors and their child vectors.
	for i := 0; i < columnCount; i++ {
		v := apiDataChunkGetVector(chunk.apiChunk, uint64(i))
		chunk.columns[i].initVectors(v, writable)
	}
	return nil
}

func (chunk *DataChunk) initFromDuckDataChunk(apiChunk apiDataChunk, writable bool) error {
	columnCount := int(apiDataChunkGetColumnCount(apiChunk))
	chunk.columns = make([]vector, columnCount)
	chunk.apiChunk = apiChunk

	var err error
	for i := 0; i < columnCount; i++ {
		vec := apiDataChunkGetVector(apiChunk, uint64(i))

		// Initialize the callback functions to read and write values.
		logicalType := apiVectorGetColumnType(vec)
		err = chunk.columns[i].init(logicalType, i)
		apiDestroyLogicalType(&logicalType)
		if err != nil {
			break
		}

		// Initialize the vector and its child vectors.
		chunk.columns[i].initVectors(vec, writable)
	}

	chunk.GetSize()
	return err
}

func (chunk *DataChunk) initFromDuckVector(vec apiVector, writable bool) error {
	columnCount := 1
	chunk.columns = make([]vector, columnCount)

	// Initialize the callback functions to read and write values.
	logicalType := apiVectorGetColumnType(vec)
	err := chunk.columns[0].init(logicalType, 0)
	apiDestroyLogicalType(&logicalType)
	if err != nil {
		return err
	}

	// Initialize the vector and its child vectors.
	chunk.columns[0].initVectors(vec, writable)
	return nil
}

func (chunk *DataChunk) close() {
	apiDestroyDataChunk(&chunk.apiChunk)
}
