package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

type appender struct {
	c        *conn
	schema   string
	table    string
	appender *C.duckdb_appender
	closed   bool
}

func (a *appender) Error() error {
	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	return errors.New(dbErr)
}

func (a *appender) Close() error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Appender")
	}

	a.closed = true

	// if state := C.duckdb_appender_flush(*a.appender); state == C.DuckDBError {
	// 	dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
	// 	return errors.New(dbErr)
	// }

	if state := C.duckdb_appender_destroy(a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}
	return nil
}

func (a *appender) AppendRow(args ...driver.Value) error {
	return a.AppendRowArray(args)
}

func (a *appender) AppendRowArray(args []driver.Value) error {
	if a.closed {
		panic("database/sql/driver: misuse of duckdb driver: use of closed Appender")
	}

	for i, v := range args {
		if v == nil {
			if rv := C.duckdb_append_null(*a.appender); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d", i)
			}
			continue
		}

		switch v := v.(type) {
		case uint8:
			if rv := C.duckdb_append_uint8(*a.appender, C.uint8_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case int8:
			if rv := C.duckdb_append_int8(*a.appender, C.int8_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case uint16:
			if rv := C.duckdb_append_uint16(*a.appender, C.uint16_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case int16:
			if rv := C.duckdb_append_int16(*a.appender, C.int16_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case uint32:
			if rv := C.duckdb_append_uint32(*a.appender, C.uint32_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case int32:
			if rv := C.duckdb_append_int32(*a.appender, C.int32_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case uint64:
			if rv := C.duckdb_append_uint64(*a.appender, C.uint64_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case int64:
			if rv := C.duckdb_append_int64(*a.appender, C.int64_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case uint:
			if rv := C.duckdb_append_uint64(*a.appender, C.uint64_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case int:
			if rv := C.duckdb_append_int64(*a.appender, C.int64_t(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case float32:
			if rv := C.duckdb_append_float(*a.appender, C.float(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case float64:
			if rv := C.duckdb_append_double(*a.appender, C.double(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case bool:
			if rv := C.duckdb_append_bool(*a.appender, C.bool(v)); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
		case []byte:
			if v == nil {
				if rv := C.duckdb_append_null(*a.appender); rv == C.DuckDBError {
					return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
				}
			} else {
				if rv := C.duckdb_append_blob(*a.appender, unsafe.Pointer(&v[0]), C.uint64_t(len(v))); rv == C.DuckDBError {
					return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
				}
			}
		case string:
			str := C.CString(v)
			if rv := C.duckdb_append_varchar(*a.appender, str); rv == C.DuckDBError {
				C.free(unsafe.Pointer(str))
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}
			C.free(unsafe.Pointer(str))
		case time.Time:
			var dt C.duckdb_timestamp
			dt.micros = C.int64_t(v.UTC().UnixMicro())
			if rv := C.duckdb_append_timestamp(*a.appender, dt); rv == C.DuckDBError {
				return fmt.Errorf("couldn't append parameter %d (type %T)", i, v)
			}

		default:
			return fmt.Errorf("couldn't append unsupported parameter %d (type %T)", i, v)
		}
	}

	if state := C.duckdb_appender_end_row(*a.appender); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_appender_error(*a.appender))
		return errors.New(dbErr)
	}

	return nil
}

var (
	errCouldNotAppend = errors.New("could not append parameter")
)
