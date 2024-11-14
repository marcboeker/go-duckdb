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

	// Access the raw connection & statement.
	err = c.Raw(func(driverConn interface{}) error {
		conn := driverConn.(*Conn)
		s, err := conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
		require.NoError(t, err)
		stmt := s.(*Stmt)
		require.Equal(t, DUCKDB_STATEMENT_TYPE_SELECT, stmt.StatementType())
		require.Equal(t, TYPE_INTEGER, stmt.ParamType(1))

		rows, err := stmt.QueryBound(context.Background())
		require.Nil(t, rows)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.NoError(t, err)

		rows, err = stmt.QueryBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, rows)
		require.NoError(t, rows.Close())

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, res.Close())
	require.NoError(t, prepared.Close())
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
		require.Equal(t, DUCKDB_STATEMENT_TYPE_UPDATE, stmt.StatementType())
		require.Equal(t, "1", stmt.ParamName(1))
		require.Equal(t, TYPE_INTEGER, stmt.ParamType(1))
		require.Equal(t, "2", stmt.ParamName(2))
		require.Equal(t, TYPE_VARCHAR, stmt.ParamType(2))

		result, err := stmt.ExecBound(context.Background())
		require.Nil(t, result)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.NoError(t, err)

		result, err = stmt.ExecBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result)

		require.NoError(t, stmt.Close())
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
		require.Equal(t, DUCKDB_STATEMENT_TYPE_INSERT, stmt.StatementType())
		require.Equal(t, "bar", stmt.ParamName(1))
		require.Equal(t, TYPE_INVALID, stmt.ParamType(1)) // Not sure why this is invalid.
		require.Equal(t, "baz", stmt.ParamName(2))
		require.Equal(t, TYPE_INVALID, stmt.ParamType(2))

		result, err := stmt.ExecBound(context.Background())
		require.Nil(t, result)
		require.ErrorIs(t, err, errNotBound)

		err = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.NoError(t, err)

		result, err = stmt.ExecBound(context.Background())
		require.NoError(t, err)
		require.NotNil(t, result)

		require.NoError(t, stmt.Close())
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, db.Close())
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
