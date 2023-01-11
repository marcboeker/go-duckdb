package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"unsafe"
)

type conn struct {
	con    *C.duckdb_connection
	closed bool
	tx     bool
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
		return nil
	}
	return driver.ErrSkip
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext after Close")
	}

	if len(args) == 0 {
		// This should be removed once duckdb_extract_statements and related APIs are available to parse multiple statements
		// so query cancellation works for this case as well
		return c.execUnprepared(query)
	}

	stmt, err := c.prepareStmt(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ExecContext(ctx, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: QueryContext after Close")
	}

	if len(args) == 0 {
		// This should be removed once duckdb_extract_statements and related APIs are available to parse multiple statements
		// so query cancellation works for this case as well
		return c.queryUnprepared(query)
	}

	stmt, err := c.prepareStmt(query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryContext(ctx, args)
}

func (c *conn) Prepare(cmd string) (driver.Stmt, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Prepare after Close")
	}
	return c.prepareStmt(cmd)
}

// Deprecated: Use BeginTx instead.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx {
		panic("database/sql/driver: misuse of duckdb driver: multiple Tx")
	}

	if opts.ReadOnly {
		return nil, errors.New("read-only transactions are not supported")
	}

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	default:
		return nil, errors.New("isolation levels other than default are not supported")
	}

	if _, err := c.ExecContext(ctx, "BEGIN TRANSACTION", nil); err != nil {
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

	return nil
}

func (c *conn) prepareStmt(cmd string) (*stmt, error) {
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

func (c *conn) execUnprepared(cmd string) (driver.Result, error) {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))

	var res C.duckdb_result
	err := C.duckdb_query(*c.con, cmdstr, &res)
	defer C.duckdb_destroy_result(&res)

	if err == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_result_error(&res))
		return nil, errors.New(dbErr)
	}

	ra := int64(C.duckdb_value_int64(&res, 0, 0))
	return &result{ra}, nil
}

func (c *conn) queryUnprepared(cmd string) (driver.Rows, error) {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))

	var res C.duckdb_result
	if err := C.duckdb_query(*c.con, cmdstr, &res); err == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_result_error(&res))
		C.duckdb_destroy_result(&res)
		return nil, errors.New(dbErr)
	}

	return newRows(res), nil
}
