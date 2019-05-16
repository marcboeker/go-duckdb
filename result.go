package duckdb

/*
#include <duckdb.h>
*/
import "C"

type result struct {
	r *C.duckdb_result
}

func (r result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r result) RowsAffected() (int64, error) {
	ra := int64(C.duckdb_value_int64(r.r, 0, 0))
	return ra, nil
}
