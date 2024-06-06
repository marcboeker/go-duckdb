package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareQueryAutoIncrement(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	createFooTable(db, t)

	stmt, err := db.Prepare("SELECT * FROM foo WHERE baz=?")
	require.NoError(t, err)
	defer stmt.Close()

	rows, err := stmt.Query(0)
	require.NoError(t, err)
	defer rows.Close()
}

func TestPrepareQueryPositional(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	createFooTable(db, t)

	stmt, err := db.Prepare("SELECT $1, $2 as foo WHERE foo=$2")
	require.NoError(t, err)
	defer stmt.Close()

	var foo, bar int
	row := stmt.QueryRow(1, 2)
	require.NoError(t, err)

	row.Scan(&foo, &bar)
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
}

func TestPrepareQueryNamed(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	createFooTable(db, t)

	stmt, err := db.PrepareContext(context.Background(), "SELECT $foo, $bar, $baz, $foo")
	require.NoError(t, err)
	defer stmt.Close()
	var foo, bar, foo2 int
	var baz string
	err = stmt.QueryRow(sql.Named("baz", "x"), sql.Named("foo", 1), sql.Named("bar", 2)).Scan(&foo, &bar, &baz, &foo2)
	require.NoError(t, err)
	if foo != 1 || bar != 2 || baz != "x" || foo2 != 1 {
		require.Fail(t, "bad values: %d %d %s %d", foo, bar, baz, foo2)
	}
}

func TestPrepareWithError(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	createFooTable(db, t)

	testCases := []struct {
		tpl string
		err string
	}{
		{
			tpl: "SELECT * FROM tbl WHERE baz=?",
			err: "Table with name tbl does not exist",
		},
		{
			tpl: "SELECT * FROM foo WHERE col=?",
			err: `Referenced column "col" not found in FROM clause`,
		},
		{
			tpl: "SELECT * FROM foo col=?",
			err: `syntax error at or near "="`,
		},
	}
	for _, tc := range testCases {
		stmt, err := db.Prepare(tc.tpl)
		if err != nil {
			require.ErrorContains(t, err, tc.err)
			continue
		}
		defer stmt.Close()
	}
}
