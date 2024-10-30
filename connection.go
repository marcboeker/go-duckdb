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
	duckdbCon C.duckdb_connection
	closed    bool
	tx        bool
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
		return nil
	}
	return driver.ErrSkip
}

func (c *conn) prepareStmts(ctx context.Context, query string) (*stmt, error) {
	if c.closed {
		return nil, getError(errClosedCon, nil)
	}

	stmts, count, errExtract := c.extractStmts(query)
	defer C.duckdb_destroy_extracted(&stmts)
	if errExtract != nil {
		return nil, errExtract
	}

	for i := C.idx_t(0); i < count-1; i++ {
		prepared, err := c.prepareExtractedStmt(stmts, i)
		if err != nil {
			return nil, err
		}

		// Execute the statement without any arguments and ignore the result.
		if _, err = prepared.ExecContext(ctx, nil); err != nil {
			return nil, err
		}
		if err = prepared.Close(); err != nil {
			return nil, err
		}
	}

	prepared, err := c.prepareExtractedStmt(stmts, count-1)
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	prepared, err := c.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	res, err := prepared.ExecContext(ctx, args)
	errClose := prepared.Close()
	if err != nil {
		err = errors.Join(err, errClose)
		return nil, err
	}
	return res, errClose
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	prepared, err := c.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	r, err := prepared.QueryContext(ctx, args)
	if err != nil {
		errClose := prepared.Close()
		err = errors.Join(err, errClose)
		return nil, err
	}

	// We must close the prepared statement after closing the rows r.
	prepared.closeOnRowsClose = true
	return r, err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, getError(errClosedCon, nil)
	}
	return c.prepareStmt(query)
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
		return errClosedCon
	}
	c.closed = true

	C.duckdb_disconnect(&c.duckdbCon)

	return nil
}

func (c *conn) prepareStmt(cmd string) (*stmt, error) {
	cmdStr := C.CString(cmd)
	defer C.duckdb_free(unsafe.Pointer(cmdStr))

	var s C.duckdb_prepared_statement
	if state := C.duckdb_prepare(c.duckdbCon, cmdStr, &s); state == C.DuckDBError {
		dbErr := getDuckDBError(C.GoString(C.duckdb_prepare_error(s)))
		C.duckdb_destroy_prepare(&s)
		return nil, dbErr
	}

	return &stmt{c: c, stmt: &s}, nil
}

func (c *conn) extractStmts(query string) (C.duckdb_extracted_statements, C.idx_t, error) {
	cQuery := C.CString(query)
	defer C.duckdb_free(unsafe.Pointer(cQuery))

	var stmts C.duckdb_extracted_statements
	stmtsCount := C.duckdb_extract_statements(c.duckdbCon, cQuery, &stmts)
	if stmtsCount == 0 {
		err := C.GoString(C.duckdb_extract_statements_error(stmts))
		C.duckdb_destroy_extracted(&stmts)
		if err != "" {
			return nil, 0, getDuckDBError(err)
		}
		return nil, 0, errors.New("no statements found")
	}

	return stmts, stmtsCount, nil
}

func (c *conn) prepareExtractedStmt(extractedStmts C.duckdb_extracted_statements, index C.idx_t) (*stmt, error) {
	var s C.duckdb_prepared_statement
	if state := C.duckdb_prepare_extracted_statement(c.duckdbCon, extractedStmts, index, &s); state == C.DuckDBError {
		dbErr := getDuckDBError(C.GoString(C.duckdb_prepare_error(s)))
		C.duckdb_destroy_prepare(&s)
		return nil, dbErr
	}

	return &stmt{c: c, stmt: &s}, nil
}
