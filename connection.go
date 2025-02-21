package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
)

// Conn holds a connection to a DuckDB database.
// It implements the driver.Conn interface.
type Conn struct {
	conn   apiConnection
	closed bool
	tx     bool
}

// CheckNamedValue implements the driver.NamedValueChecker interface.
func (conn *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
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
	return conn.prepareStmts(ctx, query)
}

// Prepare returns a prepared statement, bound to this connection.
// It implements the driver.Conn interface.
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	if conn.closed {
		return nil, errors.Join(errPrepare, errClosedCon)
	}

	extractedStmts, count, err := conn.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer apiDestroyExtracted(&extractedStmts)

	if count != 1 {
		return nil, errors.Join(errPrepare, errMissingPrepareContext)
	}
	return conn.prepareExtractedStmt(extractedStmts, 0)
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
	apiDisconnect(&conn.conn)
	return nil
}

func (conn *Conn) extractStmts(query string) (apiExtractedStatements, uint64, error) {
	var extractedStmts apiExtractedStatements

	count := apiExtractStatements(conn.conn, query, &extractedStmts)
	if count == 0 {
		errMsg := apiExtractStatementsError(extractedStmts)
		apiDestroyExtracted(&extractedStmts)
		if errMsg != "" {
			return extractedStmts, 0, getDuckDBError(errMsg)
		}
		return extractedStmts, 0, errEmptyQuery
	}
	return extractedStmts, count, nil
}

func (conn *Conn) prepareExtractedStmt(extractedStmts apiExtractedStatements, i uint64) (*Stmt, error) {
	var preparedStmt apiPreparedStatement
	state := apiPrepareExtractedStatement(conn.conn, extractedStmts, i, &preparedStmt)

	if apiState(state) == apiStateError {
		err := getDuckDBError(apiPrepareError(preparedStmt))
		apiDestroyPrepare(&preparedStmt)
		return nil, err
	}

	return &Stmt{conn: conn, preparedStmt: &preparedStmt}, nil
}

func (conn *Conn) prepareStmts(ctx context.Context, query string) (*Stmt, error) {
	if conn.closed {
		return nil, errClosedCon
	}

	extractedStmts, count, errExtract := conn.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer apiDestroyExtracted(&extractedStmts)

	for i := uint64(0); i < count-1; i++ {
		preparedStmt, err := conn.prepareExtractedStmt(extractedStmts, i)
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
	return conn.prepareExtractedStmt(extractedStmts, count-1)
}
