package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"reflect"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Conn holds a connection to a DuckDB database.
// It implements the driver.Conn interface.
type Conn struct {
	// The internal DuckDB connection.
	conn mapping.Connection
	// The internal DuckDB connection id.
	id uint64
	// The context store of the connector of this connection.
	ctxStore *contextStore
	// True, if the connection has been closed, else false.
	closed bool
	// True, if the connection has an open transaction.
	tx bool
}

func newConn(conn mapping.Connection, ctxStore *contextStore) *Conn {
	return &Conn{
		conn:     conn,
		id:       extractConnId(conn),
		ctxStore: ctxStore,
	}
}

// CheckNamedValue implements the driver.NamedValueChecker interface.
func (conn *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval, []any, []bool, []int8, []int16, []int32, []int64, []int, []uint8, []uint16,
		[]uint32, []uint64, []uint, []float32, []float64, []string, map[string]any:
		return nil
	}

	vo := reflect.ValueOf(nv.Value)
	switch vo.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Array:
		return nil
	}
	return driver.ErrSkip
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.ExecerContext interface.
func (conn *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	prepared, err := conn.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	res, err := prepared.ExecContext(ctx, args)
	errClose := prepared.Close()
	if err != nil {
		if errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}
	if errClose != nil {
		return nil, errClose
	}

	return res, nil
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.QueryerContext interface.
func (conn *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	prepared, err := conn.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	r, err := prepared.QueryContext(ctx, args)
	if err != nil {
		errClose := prepared.Close()
		if errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}
	// We must close the prepared statement after closing the rows r.
	prepared.closeOnRowsClose = true

	return r, nil
}

// PrepareContext returns a prepared statement, bound to this connection.
// It implements the driver.ConnPrepareContext interface.
func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	return conn.prepareStmts(ctx, query)
}

// Prepare returns a prepared statement, bound to this connection.
// It implements the driver.Conn interface.
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	if conn.closed {
		return nil, errors.Join(errPrepare, errClosedCon)
	}

	stmts, count, err := conn.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer mapping.DestroyExtracted(stmts)
	if count != 1 {
		return nil, errors.Join(errPrepare, errMissingPrepareContext)
	}

	return conn.prepareExtractedStmt(*stmts, 0)
}

// Begin is deprecated: Use BeginTx instead.
func (conn *Conn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// It implements the driver.ConnBeginTx interface.
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if conn.tx {
		return nil, errors.Join(errBeginTx, errMultipleTx)
	}

	if opts.ReadOnly {
		return nil, errors.Join(errBeginTx, errReadOnlyTxNotSupported)
	}

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	default:
		return nil, errors.Join(errBeginTx, errIsolationLevelNotSupported)
	}

	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION`, nil); err != nil {
		return nil, err
	}
	conn.tx = true

	return &tx{conn}, nil
}

// Close closes the connection to the database.
// It implements the driver.Conn interface.
func (conn *Conn) Close() error {
	if conn.closed {
		return errClosedCon
	}
	conn.closed = true
	mapping.Disconnect(&conn.conn)
	conn.ctxStore.delete(conn.id)

	return nil
}

func (conn *Conn) extractStmts(query string) (*mapping.ExtractedStatements, mapping.IdxT, error) {
	var stmts mapping.ExtractedStatements

	count := mapping.ExtractStatements(conn.conn, query, &stmts)
	if count == 0 {
		errMsg := mapping.ExtractStatementsError(stmts)
		mapping.DestroyExtracted(&stmts)
		if errMsg != "" {
			return nil, 0, getDuckDBError(errMsg)
		}
		return nil, 0, errEmptyQuery
	}

	return &stmts, count, nil
}

func (conn *Conn) prepareExtractedStmt(extractedStmts mapping.ExtractedStatements, i mapping.IdxT) (*Stmt, error) {
	var stmt mapping.PreparedStatement
	state := mapping.PrepareExtractedStatement(conn.conn, extractedStmts, i, &stmt)
	if state == mapping.StateError {
		err := getDuckDBError(mapping.PrepareError(stmt))
		mapping.DestroyPrepare(&stmt)
		return nil, err
	}

	return &Stmt{conn: conn, preparedStmt: &stmt}, nil
}

func (conn *Conn) prepareStmts(ctx context.Context, query string) (*Stmt, error) {
	if conn.closed {
		return nil, errClosedCon
	}

	cleanupCtx := conn.setContext(ctx)
	defer cleanupCtx()

	stmts, count, errExtract := conn.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer mapping.DestroyExtracted(stmts)

	for i := mapping.IdxT(0); i < count-1; i++ {
		preparedStmt, err := conn.prepareExtractedStmt(*stmts, i)
		if err != nil {
			return nil, err
		}

		// Execute the statement without any arguments and ignore the result.
		_, execErr := preparedStmt.ExecContext(ctx, nil)
		closeErr := preparedStmt.Close()
		if execErr != nil {
			return nil, execErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}

	return conn.prepareExtractedStmt(*stmts, count-1)
}

// GetTableNames returns the tables names of a query.
// It expects a *sql.Conn connection, and a query for which to extract the table names.
// If qualified is true, then it returns the fully qualified table names,
// else it returns only the table names.
func GetTableNames(c *sql.Conn, query string, qualified bool) ([]string, error) {
	var v mapping.Value
	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		v = mapping.GetTableNames(conn.conn, query, qualified)
		return nil
	})
	defer mapping.DestroyValue(&v)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	size := mapping.GetListSize(v)
	for i := mapping.IdxT(0); i < size; i++ {
		func() {
			child := mapping.GetListChild(v, i)
			defer mapping.DestroyValue(&child)
			tableNames = append(tableNames, mapping.GetVarchar(child))
		}()
	}

	return tableNames, nil
}

// ConnId returns the connection id of the internal DuckDB connection.
// It expects a *sql.Conn connection.
func ConnId(c *sql.Conn) (uint64, error) {
	var id uint64

	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		if conn.closed {
			return errClosedCon
		}
		id = conn.id
		return nil
	})

	return id, err
}

// extractConnId extracts the connection id of a DuckDB connection.
func extractConnId(conn mapping.Connection) uint64 {
	var ctx mapping.ClientContext
	mapping.ConnectionGetClientContext(conn, &ctx)
	defer mapping.DestroyClientContext(&ctx)

	return uint64(mapping.ClientContextGetConnectionId(ctx))
}

// setContext sets the current context for the connection.
func (conn *Conn) setContext(ctx context.Context) func() {
	return conn.ctxStore.store(conn.id, ctx)
}

// contextStoreFromConn extracts the context store of a *sql.Conn connection.
func contextStoreFromConn(c *sql.Conn) (*contextStore, error) {
	var ctxStore *contextStore

	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		if conn.closed {
			return errClosedCon
		}
		ctxStore = conn.ctxStore

		return nil
	})

	return ctxStore, err
}
