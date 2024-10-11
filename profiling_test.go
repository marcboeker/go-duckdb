package duckdb

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfiling(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	con, err := db.Conn(context.Background())
	require.NoError(t, err)

	_, err = con.ExecContext(context.Background(), `PRAGMA enable_profiling = 'no_output'`)
	require.NoError(t, err)
	_, err = con.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)
	require.NoError(t, err)
	res, err := con.QueryContext(context.Background(), `SELECT range AS i FROM range(100) ORDER BY i`)
	require.NoError(t, err)

	info, err := GetProfilingInfo(con)
	require.NoError(t, err)

	_, err = con.ExecContext(context.Background(), `PRAGMA disable_profiling`)
	require.NoError(t, err)
	require.NoError(t, res.Close())
	require.NoError(t, con.Close())
	require.NoError(t, db.Close())

	// Verify the metrics.
	require.NotEmpty(t, info.Metrics, "metrics must not be empty")
	require.NotEmpty(t, info.Children, "children must not be empty")
	require.NotEmpty(t, info.Children[0].Metrics, "child metrics must not be empty")
}

func TestErrProfiling(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	con, err := db.Conn(context.Background())
	require.NoError(t, err)

	_, err = GetProfilingInfo(con)
	testError(t, err, errProfilingInfoEmpty.Error())
	require.NoError(t, con.Close())
	require.NoError(t, db.Close())
}
