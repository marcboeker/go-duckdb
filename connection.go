package duckdb

/*
#include <duckdb.h>
#include <arrow.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/big"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/cdata"
)

type Conn struct {
	con    *C.duckdb_connection
	closed bool
	tx     bool
}

func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
		return nil
	}
	return driver.ErrSkip
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext after Close")
	}

	stmts, size, err := c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_extracted(&stmts)

	// execute all statements without args, except the last one
	for i := C.idx_t(0); i < size-1; i++ {
		stmt, err := c.prepareExtractedStmt(stmts, i)
		if err != nil {
			return nil, err
		}
		// send nil args to execute statement and ignore result
		_, err = stmt.ExecContext(ctx, nil)
		stmt.Close()
		if err != nil {
			return nil, err
		}
	}

	// prepare and execute last statement with args and return result
	stmt, err := c.prepareExtractedStmt(stmts, size-1)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.ExecContext(ctx, args)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: QueryContext after Close")
	}

	stmts, size, err := c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_extracted(&stmts)

	// execute all statements without args, except the last one
	for i := C.idx_t(0); i < size-1; i++ {
		stmt, err := c.prepareExtractedStmt(stmts, i)
		if err != nil {
			return nil, err
		}
		// send nil args to execute statement and ignore result (using ExecContext since we're ignoring the result anyway)
		_, err = stmt.ExecContext(ctx, nil)
		stmt.Close()
		if err != nil {
			return nil, err
		}
	}

	// prepare and execute last statement with args and return result
	stmt, err := c.prepareExtractedStmt(stmts, size-1)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		stmt.Close()
		return nil, err
	}

	// we can't close the statement before the query result rows are closed
	stmt.closeOnRowsClose = true
	return rows, err
}

// QueryArrowContext prepares statements, executes them, returns Apache Arrow array.RecordReader as a result of the last
// executed statement. Arguments are bound to the last statement.
// https://duckdb.org/docs/api/c/api#arrow-interface
// NOTE: Experimental interface.
func (c *Conn) QueryArrowContext(ctx context.Context, query string, args ...any) (array.RecordReader, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: QueryArrowContext after Close")
	}

	stmts, size, err := c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_extracted(&stmts)

	// execute all statements without args, except the last one
	for i := C.idx_t(0); i < size-1; i++ {
		stmt, err := c.prepareExtractedStmt(stmts, i)
		if err != nil {
			return nil, err
		}
		// send nil args to execute statement and ignore result (using ExecContext since we're ignoring the result anyway)
		_, err = stmt.ExecContext(ctx, nil)
		stmt.Close()
		if err != nil {
			return nil, err
		}
	}

	// prepare and execute last statement with args and return result
	stmt, err := c.prepareExtractedStmt(stmts, size-1)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	res, err := stmt.executeArrow(args...)
	if err != nil {
		return nil, err
	}

	defer C.duckdb_destroy_arrow(res)

	sc, err := queryArrowSchema(res)
	if err != nil {
		return nil, err
	}

	var recs []arrow.Record
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rec, err := queryArrowArray(res, sc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		recs = append(recs, rec)
	}

	return array.NewRecordReader(sc, recs)
}

// queryArrowSchema fetches the internal arrow schema from the arrow result.
func queryArrowSchema(res *C.duckdb_arrow) (*arrow.Schema, error) {
	cdSchema := (*cdata.CArrowSchema)(unsafe.Pointer(C.malloc(C.sizeof_struct_ArrowSchema)))
	defer func() {
		cdata.ReleaseCArrowSchema(cdSchema)
		C.free(unsafe.Pointer(cdSchema))
	}()

	if state := C.duckdb_query_arrow_schema(
		*res,
		(*C.duckdb_arrow_schema)(unsafe.Pointer(&cdSchema)),
	); state == C.DuckDBError {
		return nil, errors.New("duckdb_query_arrow_schema")
	}

	sc, err := cdata.ImportCArrowSchema(cdSchema)
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCArrowSchema", err)
	}

	return sc, nil
}

// queryArrowArray fetches an internal arrow array from the arrow result.
//
// This function can be called multiple time to get next chunks,
// which will free the previous out_array.
// It will return io.EOF when the end of the result is reached.
func queryArrowArray(res *C.duckdb_arrow, sc *arrow.Schema) (arrow.Record, error) {
	cdArr := (*cdata.CArrowArray)(unsafe.Pointer(C.malloc(C.sizeof_struct_ArrowArray)))
	defer func() {
		cdata.ReleaseCArrowArray(cdArr)
		C.free(unsafe.Pointer(cdArr))
	}()

	if state := C.duckdb_query_arrow_array(
		*res,
		(*C.duckdb_arrow_array)(unsafe.Pointer(&cdArr)),
	); state == C.DuckDBError {
		return nil, errors.New("duckdb_query_arrow_array")
	}

	if (*C.struct_ArrowArray)(unsafe.Pointer(cdArr)).length == 0 {
		return nil, io.EOF
	}

	rec, err := cdata.ImportCRecordBatchWithSchema(cdArr, sc)
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCRecordBatchWithSchema", err)
	}

	return rec, nil
}

func (c *Conn) Prepare(cmd string) (driver.Stmt, error) {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Prepare after Close")
	}
	return c.prepareStmt(cmd)
}

// Deprecated: Use BeginTx instead.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
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

func (c *Conn) Close() error {
	if c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Close of already closed connection")
	}
	c.closed = true

	C.duckdb_disconnect(c.con)

	return nil
}

func (c *Conn) prepareStmt(cmd string) (*stmt, error) {
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

func (c *Conn) extractStmts(query string) (C.duckdb_extracted_statements, C.idx_t, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var stmts C.duckdb_extracted_statements
	stmtsCount := C.duckdb_extract_statements(*c.con, cquery, &stmts)
	if stmtsCount == 0 {
		err := C.GoString(C.duckdb_extract_statements_error(stmts))
		C.duckdb_destroy_extracted(&stmts)
		if err != "" {
			return nil, 0, errors.New(err)
		}
		return nil, 0, errors.New("no statements found")
	}

	return stmts, stmtsCount, nil
}

func (c *Conn) prepareExtractedStmt(extractedStmts C.duckdb_extracted_statements, index C.idx_t) (*stmt, error) {
	var s C.duckdb_prepared_statement
	if state := C.duckdb_prepare_extracted_statement(*c.con, extractedStmts, index, &s); state == C.DuckDBError {
		dbErr := C.GoString(C.duckdb_prepare_error(s))
		C.duckdb_destroy_prepare(&s)
		return nil, errors.New(dbErr)
	}

	return &stmt{c: c, stmt: &s}, nil
}
