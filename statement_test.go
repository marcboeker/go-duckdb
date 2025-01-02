package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareQuery(t *testing.T) {
	db := openDB(t)
	createFooTable(db, t)

	prepared, err := db.Prepare(`SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err := prepared.Query(0)
	require.NoError(t, err)

	require.NoError(t, res.Close())
	require.NoError(t, prepared.Close())

	// Prepare on a connection.
	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	prepared, err = c.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err = prepared.Query(0)
	require.NoError(t, err)

	require.NoError(t, res.Close())
	require.NoError(t, prepared.Close())

	// Access the raw connection & statement.
	err = c.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*Conn)
		s, err := conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
		require.NoError(t, err)
		stmt := s.(*Stmt)

		stmtType, err := stmt.StatementType()
		require.NoError(t, err)
		require.Equal(t, STATEMENT_TYPE_SELECT, stmtType)

		paramType, err := stmt.ParamType(0)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, err = stmt.ParamType(1)
		require.NoError(t, err)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, err = stmt.ParamType(2)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		rows, err := stmt.QueryBound(context.Background())
		require.Nil(t, rows)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.NoError(t, err)

		rows, err = stmt.QueryBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, rows)

		badRows, err := stmt.QueryBound(context.Background())
		require.ErrorIs(t, err, errActiveRows)
		require.Nil(t, badRows)

		badResults, err := stmt.ExecBound(context.Background())
		require.ErrorIs(t, err, errActiveRows)
		require.Nil(t, badResults)

		require.NoError(t, rows.Close())

		require.NoError(t, stmt.Close())

		stmtType, err = stmt.StatementType()
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramType, err = stmt.ParamType(1)
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.ErrorIs(t, err, errCouldNotBind)
		require.ErrorIs(t, err, errClosedStmt)

		return nil
	})
	require.NoError(t, err)

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

func TestPrepareQueryPositional(t *testing.T) {
	db := openDB(t)
	createFooTable(db, t)

	prepared, err := db.Prepare(`SELECT $1, $2 AS foo WHERE foo = $2`)
	require.NoError(t, err)

	var foo, bar int
	row := prepared.QueryRow(1, 2)
	require.NoError(t, err)

	err = row.Scan(&foo, &bar)
	require.NoError(t, err)
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)

	require.NoError(t, prepared.Close())

	// Prepare on a connection.
	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	prepared, err = c.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $2 AND baz = $1`)
	require.NoError(t, err)
	res, err := prepared.Query(0, "hello")
	require.NoError(t, err)
	require.NoError(t, res.Close())
	require.NoError(t, prepared.Close())

	// Access the raw connection & statement.
	err = c.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*Conn)
		s, err := conn.PrepareContext(context.Background(), `UPDATE foo SET bar = $2 WHERE baz = $1`)
		require.NoError(t, err)
		stmt := s.(*Stmt)

		stmtType, err := stmt.StatementType()
		require.NoError(t, err)
		require.Equal(t, STATEMENT_TYPE_UPDATE, stmtType)

		paramName, err := stmt.ParamName(0)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramName, err = stmt.ParamName(1)
		require.NoError(t, err)
		require.Equal(t, "1", paramName)

		paramName, err = stmt.ParamName(2)
		require.NoError(t, err)
		require.Equal(t, "2", paramName)

		paramName, err = stmt.ParamName(3)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramType, err := stmt.ParamType(0)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, err = stmt.ParamType(1)
		require.NoError(t, err)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, err = stmt.ParamType(2)
		require.NoError(t, err)
		require.Equal(t, TYPE_VARCHAR, paramType)

		paramType, err = stmt.ParamType(3)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		result, err := stmt.ExecBound(context.Background())
		require.Nil(t, result)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.NoError(t, err)

		result, err = stmt.ExecBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result)

		require.NoError(t, stmt.Close())

		stmtType, err = stmt.StatementType()
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, err = stmt.ParamName(1)
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, "", paramName)

		paramType, err = stmt.ParamType(1)
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.ErrorIs(t, err, errCouldNotBind)
		require.ErrorIs(t, err, errClosedStmt)

		return nil
	})
	require.NoError(t, err)

	require.NoError(t, db.Close())
}

func TestPrepareQueryNamed(t *testing.T) {
	db := openDB(t)
	createFooTable(db, t)

	prepared, err := db.PrepareContext(context.Background(), `SELECT $foo, $bar, $baz, $foo`)
	require.NoError(t, err)

	var foo, bar, foo2 int
	var baz string
	row := prepared.QueryRow(sql.Named("baz", "x"), sql.Named("foo", 1), sql.Named("bar", 2))
	require.NoError(t, row.Scan(&foo, &bar, &baz, &foo2))
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	require.Equal(t, "x", baz)
	require.Equal(t, 1, foo2)

	require.NoError(t, prepared.Close())

	// Prepare on a connection.
	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	prepared, err = c.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $bar AND baz = $baz`)
	require.NoError(t, err)
	res, err := prepared.Query(sql.Named("bar", "hello"), sql.Named("baz", 0))
	require.NoError(t, err)
	require.NoError(t, res.Close())
	require.NoError(t, prepared.Close())

	// Access the raw connection & statement.
	err = c.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*Conn)
		s, err := conn.PrepareContext(context.Background(), `INSERT INTO foo VALUES ($bar, $baz)`)
		require.NoError(t, err)
		stmt := s.(*Stmt)

		stmtType, err := stmt.StatementType()
		require.NoError(t, err)
		require.Equal(t, STATEMENT_TYPE_INSERT, stmtType)

		paramName, err := stmt.ParamName(0)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramName, err = stmt.ParamName(1)
		require.NoError(t, err)
		require.Equal(t, "bar", paramName)

		paramName, err = stmt.ParamName(2)
		require.NoError(t, err)
		require.Equal(t, "baz", paramName)

		paramName, err = stmt.ParamName(3)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramType, err := stmt.ParamType(0)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, err = stmt.ParamType(1)
		// Will be fixed in the next release.
		// https://github.com/duckdb/duckdb/pull/14952
		// require.Equal(t, TYPE_VARCHAR, paramType)
		require.Equal(t, TYPE_INVALID, paramType)
		require.NoError(t, err)

		paramType, err = stmt.ParamType(2)
		// Will be fixed in the next release.
		// https://github.com/duckdb/duckdb/pull/14952
		// require.Equal(t, TYPE_INTEGER, paramType)
		require.Equal(t, TYPE_INVALID, paramType)
		require.NoError(t, err)

		paramType, err = stmt.ParamType(3)
		require.ErrorContains(t, err, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		result, err := stmt.ExecBound(context.Background())
		require.Nil(t, result)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.NoError(t, err)

		result, err = stmt.ExecBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result)

		require.NoError(t, stmt.Close())

		stmtType, err = stmt.StatementType()
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, err = stmt.ParamName(1)
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, "", paramName)

		paramType, err = stmt.ParamType(1)
		require.ErrorIs(t, err, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		err = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.ErrorIs(t, err, errCouldNotBind)
		require.ErrorIs(t, err, errClosedStmt)

		return nil
	})
	require.NoError(t, err)

	require.NoError(t, db.Close())
}

func TestUninitializedStmt(t *testing.T) {
	stmt := &Stmt{}

	stmtType, err := stmt.StatementType()
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

	paramType, err := stmt.ParamType(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, TYPE_INVALID, paramType)

	paramName, err := stmt.ParamName(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, "", paramName)

	err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
	require.ErrorIs(t, err, errCouldNotBind)
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ExecBound(context.Background())
	require.ErrorIs(t, err, errNotBound)
}

func TestPrepareWithError(t *testing.T) {
	db := openDB(t)
	createFooTable(db, t)

	testCases := []struct {
		sql string
		err string
	}{
		{
			sql: `SELECT * FROM tbl WHERE baz = ?`,
			err: `Table with name tbl does not exist`,
		},
		{
			sql: `SELECT * FROM foo WHERE col = ?`,
			err: `Referenced column "col" not found in FROM clause`,
		},
		{
			sql: `SELECT * FROM foo col = ?`,
			err: `syntax error at or near "="`,
		},
	}
	for _, tc := range testCases {
		prepared, err := db.Prepare(tc.sql)
		if err != nil {
			var dbErr *Error
			if !errors.As(err, &dbErr) {
				require.Fail(t, "error type is not (*duckdb.Error)")
			}
			require.ErrorContains(t, err, tc.err)
			continue
		}
		require.NoError(t, prepared.Close())
	}
	require.NoError(t, db.Close())
}

func TestPreparePivot(t *testing.T) {
	db := openDB(t)
	ctx := context.Background()
	createTable(db, t, `CREATE OR REPLACE TABLE cities(country VARCHAR, name VARCHAR, year INT, population INT)`)
	_, err := db.ExecContext(ctx, `INSERT INTO cities VALUES ('NL', 'Netherlands', '2020', '42')`)
	require.NoError(t, err)

	prepared, err := db.Prepare(`PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	var country, name string
	var population int
	row := prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	require.NoError(t, prepared.Close())

	prepared, err = db.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	require.NoError(t, prepared.Close())

	// Prepare on a connection.
	c, err := db.Conn(ctx)
	require.NoError(t, err)

	prepared, err = c.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	require.NoError(t, prepared.Close())

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}
