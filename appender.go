package duckdb

import (
	"database/sql/driver"
	"errors"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	conn     *Conn
	appender mapping.Appender
	closed   bool

	// The chunk to append to.
	chunk DataChunk
	// The column types of the table to append to.
	types []mapping.LogicalType
	// The number of appended rows.
	rowCount int
}

// NewAppenderFromConn returns a new Appender for the default catalog from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender from a DuckDB driver connection.
func NewAppender(driverConn driver.Conn, catalog, schema, table string) (*Appender, error) {
	conn, err := appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	var appender mapping.Appender
	state := mapping.AppenderCreateExt(conn.conn, catalog, schema, table, &appender)
	if state == mapping.StateError {
		err = errorDataError(mapping.AppenderErrorData(appender))
		mapping.AppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		conn:     conn,
		appender: appender,
	}

	// Get the column types.
	columnCount := mapping.AppenderColumnCount(appender)
	for i := mapping.IdxT(0); i < columnCount; i++ {
		colType := mapping.AppenderColumnType(appender, i)
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err = addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyTypeSlice(a.types)
			mapping.AppenderDestroy(&appender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	return a.initAppenderChunk()
}

// NewQueryAppender returns a new query Appender from a DuckDB driver connection.
// driverConn: the raw connection of sql.Conn.
// query: the query to execute, can be a INSERT, DELETE, UPDATE, or MERGE INTO statement.
// columnTypes: the column types of the appended data.
// go-duckdb tries to cast the values received via `AppendRow` to these types.
// tableName: (optional) table name to refer to the appended data, defaults to `appended_data`.
// columnNames: (optional) the column names of the table, defaults to `col1`, `col2`, ...
func NewQueryAppender(driverConn driver.Conn, query string, columnTypes []TypeInfo,
	tableName string, columnNames []string,
) (*Appender, error) {

	conn, err := appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	if query == "" {
		return nil, getError(errAppenderEmptyQuery, nil)
	}
	if len(columnNames) != 0 && len(columnTypes) != 0 {
		if len(columnNames) != len(columnTypes) {
			return nil, getError(errAppenderColumnMismatch, nil)
		}
	}

	if len(columnTypes) == 0 {
		// TODO: infer from first row instead.
	}

	a := &Appender{
		conn: conn,
	}

	// Get the logical types via the type infos.
	for _, ct := range columnTypes {
		a.types = append(a.types, ct.logicalType())
	}

	var appender mapping.Appender
	state := mapping.AppenderCreateQuery(conn.conn, query, a.types, tableName, columnNames, &appender)
	if state == mapping.StateError {
		err = errorDataError(mapping.AppenderErrorData(appender))
		mapping.AppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}
	a.appender = appender

	return a.initAppenderChunk()
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if err := a.appendDataChunk(); err != nil {
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		err := getDuckDBError(mapping.AppenderError(a.appender))
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunk()
	a.chunk.close()

	// We flush before closing to get a meaningful error message.
	var errFlush error
	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		errFlush = getDuckDBError(mapping.AppenderError(a.appender))
	}

	// Destroy all appender data and the appender.
	destroyTypeSlice(a.types)
	var errClose error
	if mapping.AppenderDestroy(&a.appender) == mapping.StateError {
		errClose = errAppenderClose
	}

	err := errors.Join(errAppend, errFlush, errClose)
	if err != nil {
		return getError(invalidatedAppenderError(err), nil)
	}

	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}

	return nil
}

func appenderConn(driverConn driver.Conn) (*Conn, error) {
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, getError(errInvalidCon, nil)
	}
	if conn.closed {
		return nil, getError(errClosedCon, nil)
	}
	return conn, nil
}

func (a *Appender) initAppenderChunk() (*Appender, error) {
	if err := a.chunk.initFromTypes(a.types, true); err != nil {
		a.chunk.close()
		destroyTypeSlice(a.types)
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a, nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return columnCountError(len(args), len(a.types))
	}

	// Create a new data chunk if the current chunk is full.
	if a.rowCount == GetDataChunkCapacity() {
		if err := a.appendDataChunk(); err != nil {
			return err
		}
	}

	// Set all values.
	for i, val := range args {
		err := a.chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}
	a.rowCount++

	return nil
}

func (a *Appender) appendDataChunk() error {
	if a.rowCount == 0 {
		// Nothing to append.
		return nil
	}
	if err := a.chunk.SetSize(a.rowCount); err != nil {
		return err
	}
	if mapping.AppendDataChunk(a.appender, a.chunk.chunk) == mapping.StateError {
		return getDuckDBError(mapping.AppenderError(a.appender))
	}

	a.chunk.reset(true)
	a.rowCount = 0

	return nil
}

func destroyTypeSlice(slice []mapping.LogicalType) {
	for _, t := range slice {
		mapping.DestroyLogicalType(&t)
	}
}
