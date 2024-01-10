package duckdb

/*
#include <duckdb.h>
#include <arrow.h>
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/cdata"
)

// Arrow exposes DuckDB Apache Arrow interface.
// https://duckdb.org/docs/api/c/api#arrow-interface
type Arrow struct {
	c *conn
}

// NewArrowFromConn returns a new Arrow from a DuckDB driver connection.
func NewArrowFromConn(driverConn any) (*Arrow, error) {
	dbConn, ok := driverConn.(*conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}

	if dbConn.closed {
		panic("database/sql/driver: misuse of duckdb driver: Arrow after Close")
	}

	return &Arrow{c: dbConn}, nil
}

// Query prepares statements, executes them, returns Apache Arrow array.RecordReader as a result of the last
// executed statement. Arguments are bound to the last statement.
func (a *Arrow) Query(ctx context.Context, query string, args ...any) (array.RecordReader, error) {
	if a.c.closed {
		panic("database/sql/driver: misuse of duckdb driver: Arrow.Query after Close")
	}

	stmts, size, err := a.c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_extracted(&stmts)

	// execute all statements without args, except the last one
	for i := C.idx_t(0); i < size-1; i++ {
		stmt, err := a.c.prepareExtractedStmt(stmts, i)
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
	stmt, err := a.c.prepareExtractedStmt(stmts, size-1)
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

	rowCount := uint64(C.duckdb_arrow_row_count(*res))

	var retrievedRows uint64

	for retrievedRows < rowCount {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		rec, err := queryArrowArray(res, sc)
		if err != nil {
			return nil, err
		}

		recs = append(recs, rec)

		retrievedRows += uint64(rec.NumRows())
	}

	return array.NewRecordReader(sc, recs)
}

// queryArrowSchema fetches the internal arrow schema from the arrow result.
func queryArrowSchema(res *C.duckdb_arrow) (*arrow.Schema, error) {
	cdSchema := (*cdata.CArrowSchema)(unsafe.Pointer(C.calloc(1, C.sizeof_struct_ArrowSchema)))
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
func queryArrowArray(res *C.duckdb_arrow, sc *arrow.Schema) (arrow.Record, error) {
	cdArr := (*cdata.CArrowArray)(unsafe.Pointer(C.calloc(1, C.sizeof_struct_ArrowArray)))
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

	rec, err := cdata.ImportCRecordBatchWithSchema(cdArr, sc)
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCRecordBatchWithSchema", err)
	}

	return rec, nil
}
