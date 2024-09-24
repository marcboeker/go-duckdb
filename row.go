package duckdb

/*
#include <duckdb.h>
*/
import "C"

type (
	// Row represents one row in duckdb. It references the vectors underneeth.
	Row struct {
		chunk      *DataChunk
		r          C.idx_t
		projection []int
	}
)

// Returns whether or now the column is projected
func (r Row) IsProjected(colIdx int) bool {
	return r.projection[colIdx] != -1
}

// SetRowValue sets the value at column c to value val.
// Returns an error when the setting the value failled.
// If the row is not projected, nil will be returned, no matter the type.
func SetRowValue[T any](row Row, colIdx int, val T) error {
	projectedRowIdx := row.projection[colIdx]
	if projectedRowIdx < 0 || projectedRowIdx >= len(row.chunk.columns) {
		// we want to allow setting to columns that are not projected,
		// it should just be a nop.
		return nil
	}
	vec := row.chunk.columns[projectedRowIdx]
	return setVectorVal(&vec, row.r, val)
}

// SetRowValue sets the column c to value val, if possible. If this operation
// fails an error is returned.
func (row Row) SetRowValue(colIdx int, val any) error {
	if !row.IsProjected(colIdx) {
		// we want to allow setting to columns that are not projected,
		// it should just be a nop.
		return nil
	}
	return row.chunk.SetValue(colIdx, int(row.r), val)
}
