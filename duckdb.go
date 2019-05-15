// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a databse/sql driver for the DuckDB database.
package duckdb

/*
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"unsafe"
)

func init() {
	sql.Register("duckdb", impl{})
}

type impl struct{}

func (impl) Open(name string) (driver.Conn, error) {
	var db C.duckdb_database
	var con C.duckdb_connection

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	if err := C.duckdb_open(cname, &db); err == C.DuckDBError {
		return nil, errError
	}
	if err := C.duckdb_connect(db, &con); err == C.DuckDBError {
		return nil, errError
	}

	return &conn{db: &db, con: &con}, nil
}

var (
	errError   = errors.New("could not open database")
	errNotImpl = errors.New("feature not implemented")
)
