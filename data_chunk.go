package duckdb

import "C"
import (
	"github.com/marcboeker/go-duckdb/mapping"
)

// DataChunk storage of a DuckDB table.
type DataChunk struct {
	// data holds the underlying duckdb data chunk.
	chunk mapping.DataChunk
	// columns is a helper slice providing direct access to all columns.
	columns []vector
	// columnNames holds the column names, if known.
	columnNames []string
	// size caches the size after initialization.
	size int
}

// GetDataChunkCapacity returns the capacity of a data chunk.
func GetDataChunkCapacity() int {
	return int(mapping.VectorSize())
}

// GetSize returns the internal size of the data chunk.
func (chunk *DataChunk) GetSize() int {
	chunk.size = int(mapping.DataChunkGetSize(chunk.chunk))
	return chunk.size
}

// SetSize sets the internal size of the data chunk. Cannot exceed GetCapacity().
func (chunk *DataChunk) SetSize(size int) error {
	if size > GetDataChunkCapacity() {
		return getError(errAPI, errVectorSize)
	}
	mapping.DataChunkSetSize(chunk.chunk, mapping.IdxT(size))
	return nil
}

// GetValue returns a single value of a column.
func (chunk *DataChunk) GetValue(colIdx int, rowIdx int) (any, error) {
	if colIdx >= len(chunk.columns) {
		return nil, getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]

	return column.getFn(column, mapping.IdxT(rowIdx)), nil
}

// SetValue writes a single value to a column in a data chunk.
// Note that this requires casting the type for each invocation.
// NOTE: Custom ENUM types must be passed as string.
func (chunk *DataChunk) SetValue(colIdx int, rowIdx int, val any) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	column := &chunk.columns[colIdx]

	return column.setFn(column, mapping.IdxT(rowIdx), val)
}

// SetChunkValue writes a single value to a column in a data chunk.
// The difference with `chunk.SetValue` is that `SetChunkValue` does not
// require casting the value to `any` (implicitly).
// NOTE: Custom ENUM types must be passed as string.
func SetChunkValue[T any](chunk DataChunk, colIdx int, rowIdx int, val T) error {
	if colIdx >= len(chunk.columns) {
		return getError(errAPI, columnCountError(colIdx, len(chunk.columns)))
	}
	return setVectorVal(&chunk.columns[colIdx], mapping.IdxT(rowIdx), val)
}

func (chunk *DataChunk) initFromTypes(types []mapping.LogicalType, writable bool) error {
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

	chunk.chunk = mapping.CreateDataChunk(types)
	mapping.DataChunkSetSize(chunk.chunk, mapping.IdxT(GetDataChunkCapacity()))

	// Initialize the vectors and their child vectors.
	for i := 0; i < columnCount; i++ {
		v := mapping.DataChunkGetVector(chunk.chunk, mapping.IdxT(i))
		chunk.columns[i].initVectors(v, writable)
	}

	return nil
}

func (chunk *DataChunk) initFromDuckDataChunk(inputChunk mapping.DataChunk, writable bool) error {
	columnCount := mapping.DataChunkGetColumnCount(inputChunk)
	chunk.columns = make([]vector, columnCount)
	chunk.chunk = inputChunk

	var err error
	for i := mapping.IdxT(0); i < columnCount; i++ {
		vec := mapping.DataChunkGetVector(inputChunk, i)

		// Initialize the callback functions to read and write values.
		logicalType := mapping.VectorGetColumnType(vec)
		err = chunk.columns[i].init(logicalType, int(i))
		mapping.DestroyLogicalType(&logicalType)
		if err != nil {
			break
		}

		// Initialize the vector and its child vectors.
		chunk.columns[i].initVectors(vec, writable)
	}
	chunk.GetSize()

	return err
}

func (chunk *DataChunk) initFromDuckVector(vec mapping.Vector, writable bool) error {
	columnCount := 1
	chunk.columns = make([]vector, columnCount)

	// Initialize the callback functions to read and write values.
	logicalType := mapping.VectorGetColumnType(vec)
	err := chunk.columns[0].init(logicalType, 0)
	mapping.DestroyLogicalType(&logicalType)
	if err != nil {
		return err
	}

	// Initialize the vector and its child vectors.
	chunk.columns[0].initVectors(vec, writable)

	return nil
}

func (chunk *DataChunk) close() {
	mapping.DestroyDataChunk(&chunk.chunk)
}
