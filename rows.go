package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"time"
	"unsafe"
)

type rows struct {
	r      *C.duckdb_result
	s      *stmt
	cursor int64
}

func (r *rows) Columns() []string {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Columns of closed rows")
	}

	cols := make([]string, int64(r.r.column_count))
	for i, c := range r.columns() {
		cols[i] = C.GoString(c.name)
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

	cols := r.columns()

	for i := 0; i < int(r.r.column_count); i++ {
		col := cols[i]

		switch col._type {
		case C.DUCKDB_TYPE_INVALID:
			return errInvalidType
		case C.DUCKDB_TYPE_BOOLEAN:
			dst[i] = (*[1 << 31]bool)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_TINYINT:
			dst[i] = (*[1 << 31]int8)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_SMALLINT:
			dst[i] = (*[1 << 31]int16)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_INTEGER:
			dst[i] = (*[1 << 31]int32)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_BIGINT:
			dst[i] = (*[1 << 31]int64)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_FLOAT:
			dst[i] = (*[1 << 31]float32)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_DOUBLE:
			dst[i] = (*[1 << 31]float64)(unsafe.Pointer(col.data))[r.cursor]
		case C.DUCKDB_TYPE_DATE:
			val := (*[1 << 31]C.duckdb_date)(unsafe.Pointer(col.data))[r.cursor]
			dst[i] = time.Date(
				int(val.year),
				time.Month(val.month),
				int(val.day),
				0, 0, 0, 0,
				time.UTC,
			)
		case C.DUCKDB_TYPE_VARCHAR:
			dst[i] = C.GoString((*[1 << 31]*C.char)(unsafe.Pointer(col.data))[r.cursor])
		case C.DUCKDB_TYPE_TIMESTAMP:
			// TODO: Implement when availabe in DuckDB
		}
	}

	r.cursor++

	return nil
}

// implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	col := r.columns()[index]
	switch col._type{
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf("")
	}
	return nil
}

// implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	col := r.columns()[index]
	switch col._type{
	case C.DUCKDB_TYPE_BOOLEAN:
		return "BOOLEAN"
	case C.DUCKDB_TYPE_TINYINT:
		return "TINYINT"
	case C.DUCKDB_TYPE_SMALLINT:
		return "SMALLINT"
	case C.DUCKDB_TYPE_INTEGER:
		return "INT"
	case C.DUCKDB_TYPE_BIGINT:
		return "BIGINT"
	case C.DUCKDB_TYPE_FLOAT:
		return "FLOAT"
	case C.DUCKDB_TYPE_DOUBLE:
		return "DOUBLE"
	case C.DUCKDB_TYPE_DATE:
		return "DATE"
	case C.DUCKDB_TYPE_VARCHAR:
		return "VARCHAR"
	case C.DUCKDB_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	}
	return ""
}

func (r *rows) Close() error {
	if r.r == nil {
		panic("database/sql/driver: misuse of duckdb driver: Close of already closed rows")
	}

	C.duckdb_destroy_result(r.r)

	r.r = nil
	if r.s != nil {
		r.s.rows = false
		r.s = nil
	}

	return nil
}

func (r rows) columns() []C.duckdb_column {
	cols := make([]C.duckdb_column, int(r.r.column_count))

	for i := 0; i < int(r.r.column_count); i++ {
		cols[i] = (*[1 << 30]C.duckdb_column)(unsafe.Pointer(r.r.columns))[i]
	}

	return cols
}

var (
	errInvalidType = errors.New("invalid data type")
)
