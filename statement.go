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

type stmt struct {
	c      *conn
	stmt   *C.duckdb_prepared_statement
	closed bool
	rows   bool
}

func (s *stmt) Close() error {
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	C.duckdb_destroy_prepare(s.stmt)
	return nil
}

func (s *stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	paramCount := C.duckdb_nparams(*s.stmt)
	return int(paramCount)
}

func (s *stmt) start(args []driver.Value) error {
	if s.NumInput() != len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	for i, v := range args {
		switch v := v.(type) {
		case bool:
			if rv := C.duckdb_bind_boolean(*s.stmt, C.idx_t(i+1), true); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int8:
			if rv := C.duckdb_bind_int8(*s.stmt, C.idx_t(i+1), C.int8_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int16:
			if rv := C.duckdb_bind_int16(*s.stmt, C.idx_t(i+1), C.int16_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int32:
			if rv := C.duckdb_bind_int32(*s.stmt, C.idx_t(i+1), C.int32_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int64:
			if rv := C.duckdb_bind_int64(*s.stmt, C.idx_t(i+1), C.int64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int:
			if rv := C.duckdb_bind_int64(*s.stmt, C.idx_t(i+1), C.int64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case HugeInt:
			val := C.duckdb_hugeint{
				upper: C.int64_t(v.upper),
				lower: C.uint64_t(v.lower),
			}
			if rv := C.duckdb_bind_hugeint(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint8:
			if rv := C.duckdb_bind_uint8(*s.stmt, C.idx_t(i+1), C.uchar(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint16:
			if rv := C.duckdb_bind_uint16(*s.stmt, C.idx_t(i+1), C.uint16_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint32:
			if rv := C.duckdb_bind_uint32(*s.stmt, C.idx_t(i+1), C.uint32_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint64:
			if rv := C.duckdb_bind_uint64(*s.stmt, C.idx_t(i+1), C.uint64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case float32:
			if rv := C.duckdb_bind_float(*s.stmt, C.idx_t(i+1), C.float(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case float64:
			if rv := C.duckdb_bind_double(*s.stmt, C.idx_t(i+1), C.double(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case string:
			val := C.CString(v)
			if rv := C.duckdb_bind_varchar(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				C.free(unsafe.Pointer(val))
				return errCouldNotBind
			}
			C.free(unsafe.Pointer(val))
		case []byte:
			val := C.CBytes(v)
			l := len(v)
			if rv := C.duckdb_bind_blob(*s.stmt, C.idx_t(i+1), val, C.uint64_t(l)); rv == C.DuckDBError {
				C.free(unsafe.Pointer(val))
				return errCouldNotBind
			}
			C.free(unsafe.Pointer(val))
		case time.Time:
			val := C.duckdb_timestamp{
				micros: C.int64_t(v.UTC().UnixMicro()),
			}
			if rv := C.duckdb_bind_timestamp(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case Interval:
			val := C.duckdb_interval{
				months: C.int32_t(v.Months),
				days:   C.int32_t(v.Days),
				micros: C.int64_t(v.Micros),
			}
			if rv := C.duckdb_bind_interval(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case nil:
			if rv := C.duckdb_bind_null(*s.stmt, C.idx_t(i+1)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		default:
			return driver.ErrSkip
		}
	}

	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: Exec after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Exec with active Rows")
	}

	err := s.start(args)
	if err != nil {
		return nil, err
	}

	var res C.duckdb_result
	if state := C.duckdb_execute_prepared(*s.stmt, &res); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_result_error(&res))
		C.duckdb_destroy_result(&res)
		C.duckdb_destroy_prepare(s.stmt)
		return nil, errors.New(dbErr)
	}
	defer C.duckdb_destroy_result(&res)

	ra := int64(C.duckdb_value_int64(&res, 0, 0))

	return &result{ra}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: Query after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Query with active Rows")
	}

	err := s.start(args)
	if err != nil {
		return nil, err
	}

	var res C.duckdb_result
	if state := C.duckdb_execute_prepared(*s.stmt, &res); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_result_error(&res))
		C.duckdb_destroy_result(&res)
		C.duckdb_destroy_prepare(s.stmt)
		return nil, errors.New(dbErr)
	}
	s.rows = true

	return newRowsWithStmt(res, s), nil
}

var (
	errCouldNotBind = errors.New("could not bind parameter")
)
