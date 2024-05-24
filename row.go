package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"
import "unsafe"

type (
	Row struct {
		vectors    []vector
		r          C.idx_t
		info       C.duckdb_function_info
		projection []int
	}
)

// Returns whether or now the column is projected
func (r Row) IsProjected(c int) bool {
	return r.projection[c] != -1
}

func SetRowValue[T any](row Row, c int, val T) error {
	if !row.IsProjected(c) {
		// we want to allow setting to columns that are not projected,
		// it should just be a nop.
		return nil
	}
	vec := row.vectors[row.projection[c]]
	return setVectorVal(&vec, row.r, val)
}

func (row Row) SetRowValue(c int, val any) {
	vec := row.vectors[c]

	// Ensure the types match before adding to the vector
	v, err := vec.tryCast(val)
	if err != nil {
		cerr := columnError(err, c+1)
		errstr := C.CString(cerr.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_function_set_error(row.info, errstr)
	}

	vec.fn(&vec, row.r, v)
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
