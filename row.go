package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

type (
	// Row represents one row in duckdb. It references the vectors underneeth.
	Row struct {
		vectors    []vector
		r          C.idx_t
		projection []int
	}
)

// Returns whether or now the column is projected
func (r Row) IsProjected(c int) bool {
	return r.projection[c] != -1
}

// SetRowValue sets the value at column c to value val.
// Returns an error when the setting the value failled.
// If the row is not projected, nil will be returned, no matter the type.
func SetRowValue[T any](row Row, c int, val T) error {
	if !row.IsProjected(c) {
		// we want to allow setting to columns that are not projected,
		// it should just be a nop.
		return nil
	}
	vec := row.vectors[row.projection[c]]
	return setVectorVal(&vec, row.r, val)
}

// SetRowValue sets the column c to value val, if possible. If this operation
// fails an error is returned.
func (row Row) SetRowValue(c int, val any) error {
	if !row.IsProjected(c) {
		// we want to allow setting to columns that are not projected,
		// it should just be a nop.
		return nil
	}
	vec := row.vectors[c]

	// Ensure the types match before adding to the vector
	v, err := vec.tryCast(val)
	if err != nil {
		return columnError(err, c+1)
	}

	vec.fn(&vec, row.r, v)
	return nil
}

func (row *Row) initColumn(i C.idx_t, duckdbVector C.duckdb_vector) error {
	t := C.duckdb_vector_get_column_type(duckdbVector)
	if err := row.vectors[i].init(t, int(i)); err != nil {
		return err
	}
	row.vectors[i].duckdbVector = duckdbVector
	row.vectors[i].getChildVectors(duckdbVector)
	return nil
}
