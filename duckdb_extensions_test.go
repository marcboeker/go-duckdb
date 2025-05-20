//go:build !duckdb_use_lib && !duckdb_use_static_lib

package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenSQLite(t *testing.T) {
	db := openDbWrapper(t, `sqlite:testdata/pets.sqlite`)
	defer closeDbWrapper(t, db)

	var species string
	res := db.QueryRow(`SELECT species FROM pets WHERE id = 1`)
	require.NoError(t, res.Scan(&species))
	require.Equal(t, "Gopher", species)
}

func TestLoadHTTPFS(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`INSTALL httpfs`)
	require.NoError(t, err)
	_, err = db.Exec(`LOAD httpfs`)
	require.NoError(t, err)
}

func TestLoadExcel(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`INSTALL excel`)
	require.NoError(t, err)
	_, err = db.Exec(`LOAD excel`)
	require.NoError(t, err)
}
