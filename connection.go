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
	apiConn apiConnection
	closed  bool
	tx      bool
}

// CheckNamedValue implements the driver.NamedValueChecker interface.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
		return nil
	}
	return driver.ErrSkip
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	prepared, err := c.prepareStmts(ctx, query)
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
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	prepared, err := c.prepareStmts(ctx, query)
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
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepareStmts(ctx, query)
}

// Prepare returns a prepared statement, bound to this connection.
// It implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, errors.Join(errPrepare, errClosedCon)
	}

	extractedStmts, count, err := c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer apiDestroyExtracted(&extractedStmts)

	if count != 1 {
		return nil, errors.Join(errPrepare, errMissingPrepareContext)
	}
	return c.prepareExtractedStmt(extractedStmts, 0)
}

// Begin is deprecated: Use BeginTx instead.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// It implements the driver.ConnBeginTx interface.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx {
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

	if _, err := c.ExecContext(ctx, `BEGIN TRANSACTION`, nil); err != nil {
		return nil, err
	}

	c.tx = true
	return &tx{c}, nil
}

// Close closes the connection to the database.
// It implements the driver.Conn interface.
func (c *Conn) Close() error {
	if c.closed {
		return errClosedCon
	}
	c.closed = true
	apiDisconnect(&c.apiConn)
	return nil
}

func (c *Conn) extractStmts(query string) (apiExtractedStatements, uint64, error) {
	var extractedStmts apiExtractedStatements

	count := apiExtractStatements(c.apiConn, query, &extractedStmts)
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

func (c *Conn) prepareExtractedStmt(extractedStmts apiExtractedStatements, i uint64) (*Stmt, error) {
	var preparedStmt apiPreparedStatement
	state := apiPrepareExtractedStatement(c.apiConn, extractedStmts, i, &preparedStmt)

	if apiState(state) == apiStateError {
		err := getDuckDBError(apiPrepareError(preparedStmt))
		apiDestroyPrepare(&preparedStmt)
		return nil, err
	}

	return &Stmt{conn: c, preparedStmt: &preparedStmt}, nil
}

func (c *Conn) prepareStmts(ctx context.Context, query string) (*Stmt, error) {
	if c.closed {
		return nil, errClosedCon
	}

	extractedStmts, count, errExtract := c.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer apiDestroyExtracted(&extractedStmts)

	for i := uint64(0); i < count-1; i++ {
		preparedStmt, err := c.prepareExtractedStmt(extractedStmts, i)
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
	return c.prepareExtractedStmt(extractedStmts, count-1)
}
