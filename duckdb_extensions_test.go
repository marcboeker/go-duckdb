//go:build !duckdb_use_lib && !windows

package duckdb

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenSQLite(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "sqlite:testdata/pets.sqlite")
	require.NoError(t, err)

	var species string
	res := db.QueryRow("SELECT species FROM pets WHERE id=1")
	require.NoError(t, res.Scan(&species))
	require.Equal(t, "Gopher", species)
	require.NoError(t, db.Close())
}
