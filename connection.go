package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"unsafe"

	"github.com/google/uuid"
)

type conn struct {
	db     *C.duckdb_database
	con    *C.duckdb_connection
	closed bool
	tx     bool
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch v := nv.Value.(type) {
	case HugeInt:
		nv.Value = HugeInt(v)
	case uuid.UUID:
		nv.Value = v.String()
	}
	return nil
}

func (c *conn) Exec(cmd string, args []driver.Value) (driver.Result, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Exec after Close")
	}
	stmt, err := c.prepareStmt(cmd)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args)
}

func (c *conn) Query(cmd string, args []driver.Value) (driver.Rows, error) {
	stmt, err := c.prepareStmt(cmd)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args)
}

func (c *conn) Prepare(cmd string) (driver.Stmt, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Prepare after Close")
	}
	return c.prepareStmt(cmd)
}

func (c *conn) Begin() (driver.Tx, error) {
	if c.tx {
		panic("database/sql/driver: misuse of duckdb driver: multiple Tx")
	}

	if _, err := c.Exec("BEGIN TRANSACTION", nil); err != nil {
		return nil, err
	}

	c.tx = true
	return &tx{c}, nil
}

func (c *conn) Close() error {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Close of already closed connection")
	}
	c.closed = true

	C.duckdb_disconnect(c.con)
	C.duckdb_close(c.db)
	c.db = nil

	return nil
}

func (c *conn) prepareStmt(cmd string) (driver.Stmt, error) {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))

	var s C.duckdb_prepared_statement
	if state := C.duckdb_prepare(*c.con, cmdstr, &s); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_prepare_error(s))
		C.duckdb_destroy_prepare(&s)

		return nil, errors.New(dbErr)
	}

	return &stmt{c: c, stmt: &s}, nil
}
