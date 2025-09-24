package duckdb

import (
	"database/sql/driver"
	"errors"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Appender wraps functionality around the DuckDB appender.
// It enables efficient bulk transformations.
type Appender struct {
	// The raw sql.Conn's driver connection.
	conn *Conn
	// The DuckDB appender.
	appender mapping.Appender
	// True, if the appender has been closed.
	closed bool

	// The chunk to append to.
	chunk DataChunk
	// The column types of the table to append to.
	types []mapping.LogicalType
	// The number of appended rows.
	rowCount int

	// True, if the appender has been initialized.
	initialized bool
	// (Optional) query appender initialization information.
	initInfo appenderInitInfo
}

// appenderInitInfo is necessary, if we postpone appender creation to the first
// call to AppendRow. This can happen when creating a query Appender without
// specifying its column types.
type appenderInitInfo struct {
	query    string
	table    string
	colNames []string
}

// NewAppenderFromConn returns a new Appender for the default catalog.
// The Appender batches rows via AppendRow. Upon reading the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `schema` and `table` specify the table (`schema.table`) to append to.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender.
// The Appender batches rows via AppendRow. Upon reading the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `catalog`, `schema` and `table` specify the table (`catalog.schema.table`) to append to.
func NewAppender(driverConn driver.Conn, catalog, schema, table string) (*Appender, error) {
	var a Appender
	var err error

	err = a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	state := mapping.AppenderCreateExt(a.conn.conn, catalog, schema, table, &a.appender)
	if state == mapping.StateError {
		err = errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	// Get the column types.
	columnCount := mapping.AppenderColumnCount(a.appender)
	for i := mapping.IdxT(0); i < columnCount; i++ {
		colType := mapping.AppenderColumnType(a.appender, i)
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err = addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyTypeSlice(a.types)
			mapping.AppenderDestroy(&a.appender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	a.initialized = true
	return a.initAppenderChunk()
}

// NewQueryAppender returns a new query Appender.
// The Appender batches rows via AppendRow. Upon reading the auto-flush threshold or
// upon calling Flush or Close, it executes the query, treating the batched rows as a temporary table.
// `driverConn` is the raw sql.Conn's driver connection.
// `query` is the query to execute. It can be a INSERT, DELETE, UPDATE or MERGE INTO statement.
// `table` is the (optional) table name of the temporary table containing the batched rows.
// It defaults to `appended_data`.
// `colTypes` are the (optional) column types of the temporary table.
// (ATTENTION) If left empty, go-duckdb initializes the Appender on the first call to `AppendRow` using reflection.
// Meaning that first call might result in 'unexpected' Appender creation errors.
// `colNames` are the (optional) names of the columns of the temporary table containing the batched rows.
// They default to `col1`, `col2`, ...
func NewQueryAppender(driverConn driver.Conn, query, table string, colTypes []TypeInfo, colNames []string) (*Appender, error) {
	var a Appender
	var err error

	err = a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	if query == "" {
		return nil, getError(errAppenderEmptyQuery, nil)
	}
	if len(colNames) != 0 && len(colTypes) != 0 {
		if len(colNames) != len(colTypes) {
			return nil, getError(errAppenderColumnMismatch, nil)
		}
	}

	a.initInfo = appenderInitInfo{
		query:    query,
		table:    table,
		colNames: colNames,
	}

	// Infer the types during the first call to AppendRow.
	if len(colTypes) == 0 {
		return &a, nil
	}

	return a.initQueryAppender(colTypes)
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if !a.initialized {
		return nil
	}
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

	if !a.initialized {
		return nil
	}

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

	if !a.initialized {
		// TODO: set types via reflection
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}

	return nil
}

func (a *Appender) appenderConn(driverConn driver.Conn) error {
	var ok bool
	a.conn, ok = driverConn.(*Conn)
	if !ok {
		return getError(errInvalidCon, nil)
	}
	if a.conn.closed {
		return getError(errClosedCon, nil)
	}

	return nil
}

func (a *Appender) initQueryAppender(colTypes []TypeInfo) (*Appender, error) {
	// Get the logical types via the type infos.
	for _, ct := range colTypes {
		a.types = append(a.types, ct.logicalType())
	}

	state := mapping.AppenderCreateQuery(a.conn.conn, a.initInfo.query, a.types, a.initInfo.table, a.initInfo.colNames, &a.appender)
	if state == mapping.StateError {
		err := errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	a.initialized = true
	return a.initAppenderChunk()
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
