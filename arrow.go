//go:build duckdb_arrow

package duckdb

/*
#include <stdlib.h>
#include <stdint.h>

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArrayStream {
	void (*get_schema)(struct ArrowArrayStream*);
	void (*get_next)(struct ArrowArrayStream*);
	void (*get_last_error)(struct ArrowArrayStream*);
	void (*release)(struct ArrowArrayStream*);
	void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"

	"github.com/marcboeker/go-duckdb/arrowmapping"
	"github.com/marcboeker/go-duckdb/mapping"
)

// Arrow exposes DuckDB Apache Arrow interface.
// https://duckdb.org/docs/api/c/api#arrow-interface
type Arrow struct {
	conn *Conn
}

// NewArrowFromConn returns a new Arrow from a DuckDB driver connection.
func NewArrowFromConn(driverConn driver.Conn) (*Arrow, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}
	if conn.closed {
		return nil, errClosedCon
	}

	return &Arrow{conn: conn}, nil
}

// arrowStreamReader implements array.RecordReader for streaming DuckDB results.
type arrowStreamReader struct {
	ctx      context.Context
	res      *arrowmapping.Arrow
	schema   *arrow.Schema
	rowCount uint64

	mu         sync.Mutex // protects readCount and currentRec
	refCount   int64
	readCount  uint64
	currentRec arrow.Record
	closed     bool // tracks if the reader has been closed/released
	err        error
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (r *arrowStreamReader) Retain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil || r.closed {
		return // Do not increase refCount if there is an error or if closed.
	}
	r.refCount++
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (r *arrowStreamReader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.refCount <= 0 {
		return // Do not release if refCount is already zero.
	}
	r.refCount--
	if r.refCount != 0 {
		return // Do not release if there are still references.
	}

	// If this is the last reference, we need to clean up.
	r.closed = true
	if r.res != nil {
		arrowmapping.DestroyArrow(r.res)
		r.res = nil
	}
	if r.currentRec != nil {
		r.currentRec.Release()
	}
}

func (r *arrowStreamReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *arrowStreamReader) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readCount >= r.rowCount || r.err != nil {
		return false
	}

	if r.closed {
		r.err = errors.New("arrow reader has been closed")
		return false
	}

	if r.res == nil {
		r.err = errors.New("arrow result has already been released")
		return false
	}

	select {
	case <-r.ctx.Done():
		r.err = r.ctx.Err()
		return false
	default:
		if r.currentRec != nil {
			r.currentRec.Release() // Release the previous record.
		}
		rec, err := queryArrowArray(r.res, r.schema)
		if err != nil {
			r.err = err
			r.currentRec = nil
			return false
		}
		r.currentRec = rec
		r.readCount += uint64(rec.NumRows())
		return true
	}
}

func (r *arrowStreamReader) Record() arrow.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentRec
}

func (r *arrowStreamReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// QueryContext prepares statements, executes them, returns Apache Arrow array.RecordReader as a result of the last
// executed statement. Arguments are bound to the last statement.
func (a *Arrow) QueryContext(ctx context.Context, query string, args ...any) (array.RecordReader, error) {
	if a.conn.closed {
		return nil, errClosedCon
	}

	cleanupCtx := a.conn.setContext(ctx)
	defer cleanupCtx()

	stmts, size, errExtract := a.conn.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer mapping.DestroyExtracted(stmts)

	// Execute all statements without args, except the last one.
	for i := mapping.IdxT(0); i < size-mapping.IdxT(1); i++ {
		extractedStmt, err := a.conn.prepareExtractedStmt(*stmts, i)
		if err != nil {
			return nil, err
		}

		// Send nil args to execute the statement and ignore the result.
		_, err = extractedStmt.ExecContext(ctx, nil)
		errClose := extractedStmt.Close()
		if err != nil || errClose != nil {
			return nil, errors.Join(err, errClose)
		}
	}

	// Prepare and execute the last statement with args.
	stmt, err := a.conn.prepareExtractedStmt(*stmts, size-mapping.IdxT(1))
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	res, err := a.execute(stmt, a.anyArgsToNamedArgs(args))
	if err != nil {
		return nil, err
	}

	sc, err := a.queryArrowSchema(res)
	if err != nil {
		arrowmapping.DestroyArrow(res)
		return nil, err
	}

	return &arrowStreamReader{
		refCount: 1,
		ctx:      ctx,
		res:      res,
		schema:   sc,
		rowCount: uint64(arrowmapping.ArrowRowCount(*res)),
	}, nil
}

// queryArrowSchema fetches the internal arrow schema from the arrow result.
func (a *Arrow) queryArrowSchema(res *arrowmapping.Arrow) (*arrow.Schema, error) {
	schema := C.calloc(1, C.sizeof_struct_ArrowSchema)
	defer func() {
		cdata.ReleaseCArrowSchema((*cdata.CArrowSchema)(schema))
		C.free(schema)
	}()

	arrowSchema := arrowmapping.ArrowSchema{
		Ptr: unsafe.Pointer(&schema),
	}
	if arrowmapping.QueryArrowSchema(*res, &arrowSchema) == mapping.StateError {
		return nil, errors.New("duckdb_query_arrow_schema")
	}

	sc, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(schema))
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCArrowSchema", err)
	}

	return sc, nil
}

// queryArrowArray fetches an internal arrow array from the arrow result.
//
// This function can be called multiple time to get next chunks,
// which will free the previous out_array.
func queryArrowArray(res *arrowmapping.Arrow, sc *arrow.Schema) (arrow.Record, error) {
	arr := C.calloc(1, C.sizeof_struct_ArrowArray)
	defer func() {
		cdata.ReleaseCArrowArray((*cdata.CArrowArray)(arr))
		C.free(arr)
	}()

	arrowArray := arrowmapping.ArrowArray{
		Ptr: unsafe.Pointer(&arr),
	}
	if arrowmapping.QueryArrowArray(*res, &arrowArray) == mapping.StateError {
		return nil, errors.New("duckdb_query_arrow_array")
	}

	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(arr), sc)
	if err != nil {
		return nil, fmt.Errorf("%w: ImportCRecordBatchWithSchema", err)
	}

	return rec, nil
}

func (a *Arrow) execute(s *Stmt, args []driver.NamedValue) (*arrowmapping.Arrow, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}

	var res arrowmapping.Arrow
	if arrowmapping.ExecutePreparedArrow(*s.preparedStmt, &res) == mapping.StateError {
		errMsg := arrowmapping.QueryArrowError(res)
		arrowmapping.DestroyArrow(&res)
		return nil, fmt.Errorf("failed to execute the prepared arrow: %v", errMsg)
	}

	return &res, nil
}

func (a *Arrow) anyArgsToNamedArgs(args []any) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}

	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg
	}

	return argsToNamedArgs(values)
}

// RegisterView registers an Arrow record reader as a view with the given name in DuckDB.
// The returned release function must be called to release the memory once the view is no longer needed.
func (a *Arrow) RegisterView(reader array.RecordReader, name string) (release func(), err error) {
	if a.conn.closed {
		return nil, errClosedCon
	}

	stream := C.calloc(1, C.sizeof_struct_ArrowArrayStream)
	release = func() {
		cdata.ReleaseCArrowArrayStream((*cdata.CArrowArrayStream)(stream))
		C.free(stream)
	}
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(stream))

	arrowStream := arrowmapping.ArrowStream{
		Ptr: unsafe.Pointer(stream),
	}
	if arrowmapping.ArrowScan(a.conn.conn, name, arrowStream) == mapping.StateError {
		release()
		return nil, errors.New("duckdb_arrow_scan")
	}

	return release, nil
}
