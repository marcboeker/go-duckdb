//go:build !duckdb_use_lib && !windows

package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenSQLite(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	db, _ := openDbWrapper(t, counters, false, `sqlite:testdata/pets.sqlite`)
	defer closeDbWrapper(t, counters, db)

	var species string
	res := db.QueryRow(`SELECT species FROM pets WHERE id = 1`)
	require.NoError(t, res.Scan(&species))
	require.Equal(t, "Gopher", species)
}
