package duckdb

import (
	"database/sql/driver"
	"errors"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	conn     *Conn
	schema   string
	table    string
	appender mapping.Appender
	closed   bool

	// The current chunk to append to.
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
	conn, ok := driverConn.(*Conn)
	if !ok {
		return nil, getError(errInvalidCon, nil)
	}
	if conn.closed {
		return nil, getError(errClosedCon, nil)
	}

	var appender mapping.Appender
	state := mapping.AppenderCreateExt(conn.conn, catalog, schema, table, &appender)
	if state == mapping.StateError {
		err := getDuckDBError(mapping.AppenderError(appender))
		mapping.AppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		conn:     conn,
		schema:   schema,
		table:    table,
		appender: appender,
		rowCount: 0,
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
			err := addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyTypeSlice(a.types)
			mapping.AppenderDestroy(&appender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	// Initialize the data chunk.
	if err := a.chunk.initFromTypes(a.types, true); err != nil {
		a.chunk.close()
		destroyTypeSlice(a.types)
		mapping.AppenderDestroy(&appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a, nil
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

	mapping.DataChunkReset(a.chunk.chunk)
	a.rowCount = 0

	return nil
}

func destroyTypeSlice(slice []mapping.LogicalType) {
	for _, t := range slice {
		mapping.DestroyLogicalType(&t)
	}
}
