package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"io"
)

type rows struct {
	r      *C.duckdb_result
	cols   []*column
	cursor int64
}

func (r *rows) Columns() []string {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Columns of closed rows")
	}

	cols := make([]string, len(r.cols))
	for i, c := range r.cols {
		cols[i] = c.name
	}

	return cols
}

func (r *rows) Next(dst []driver.Value) error {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Next of closed rows")
	}

	if r.cursor >= int64(r.r.row_count) {
		return io.EOF
	}

	for i := 0; i < int(r.r.column_count); i++ {
		ct := r.cols[i].dataType
		switch ct {
		case C.DUCKDB_TYPE_INVALID:
			return errInvalidType
		case C.DUCKDB_TYPE_BOOLEAN:
			val := C.duckdb_value_boolean(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = bool(val)
		case C.DUCKDB_TYPE_TINYINT:
			val := C.duckdb_value_int8(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int8(val)
		case C.DUCKDB_TYPE_SMALLINT:
			val := C.duckdb_value_int16(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int16(val)
		case C.DUCKDB_TYPE_INTEGER:
			val := C.duckdb_value_int32(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int32(val)
		case C.DUCKDB_TYPE_BIGINT:
			val := C.duckdb_value_int64(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int64(val)
		case C.DUCKDB_TYPE_FLOAT:
			val := C.duckdb_value_float(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = float32(val)
		case C.DUCKDB_TYPE_DOUBLE:
			val := C.duckdb_value_double(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = float64(val)
		case C.DUCKDB_TYPE_VARCHAR:
			val := C.duckdb_value_varchar(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = C.GoString(val)
		}
	}

	r.cursor++

	return nil
}

func (r *rows) Close() error {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Close of already closed rows")
	}

	C.duckdb_destroy_result(r.r)

	r.r = nil
	return nil
}

var (
	errInvalidType = errors.New("invalid data type")
)
