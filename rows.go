package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"io"
	"time"
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
			val := C.duckdb_value_int32(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			if val == 0 {
				dst[i] = false
			} else {
				dst[i] = true
			}
		case C.DUCKDB_TYPE_TINYINT:
			val := C.duckdb_value_int32(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int8(val)
		case C.DUCKDB_TYPE_SMALLINT:
			val := C.duckdb_value_int32(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int16(val)
		case C.DUCKDB_TYPE_INTEGER:
			val := C.duckdb_value_int32(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int32(val)
		case C.DUCKDB_TYPE_BIGINT:
			val := C.duckdb_value_int64(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = int64(val)
		case C.DUCKDB_TYPE_FLOAT:
			continue
		case C.DUCKDB_TYPE_DOUBLE:
			continue
		case C.DUCKDB_TYPE_TIMESTAMP:
			val := C.duckdb_value_int64(r.r, C.ulonglong(i), C.ulonglong(r.cursor))
			dst[i] = time.Unix(int64(val), 0)
		case C.DUCKDB_TYPE_DATE:
			continue
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