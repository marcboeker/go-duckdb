package duckdb

import (
	"context"
	"database/sql"
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
	err = row.Scan(&foo, &bar, &baz, &foo2)
	require.NoError(t, err)
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	require.Equal(t, "x", baz)
	require.Equal(t, 1, foo2)

	require.NoError(t, prepared.Close())
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
