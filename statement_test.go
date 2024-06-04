package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareQuery(t *testing.T) {
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
