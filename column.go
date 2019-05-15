package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"unsafe"
)

type column struct {
	name    string
	dataType int
}

func parseColumns(result *C.duckdb_result) ([]*column, error) {
	cols := make([]*column, int(result.column_count))

	length := int(result.column_count)
	tmpslice := (*[1 << 30]C.duckdb_column)(unsafe.Pointer(result.columns))[:length:length]

	for i, s := range tmpslice {
		cols[i] = &column{name: C.GoString(s.name), dataType: int(s._type)}
	}

	return cols, nil
}
