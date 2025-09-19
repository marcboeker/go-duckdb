package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := GetProfilingInfo(conn)
	require.ErrorContains(t, err, errProfilingInfoEmpty.Error())

	_, err = conn.ExecContext(context.Background(), `PRAGMA enable_profiling = 'no_output'`)
	require.NoError(t, err)
	_, err = conn.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)
	require.NoError(t, err)

	res, err := conn.QueryContext(context.Background(), `SELECT range AS i FROM range(100) ORDER BY i`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	info, err := GetProfilingInfo(conn)
	require.NoError(t, err)

	// Verify the metrics.
	require.NotEmpty(t, info.Metrics, "metrics must not be empty")
	require.NotEmpty(t, info.Children, "children must not be empty")
	require.NotEmpty(t, info.Children[0].Metrics, "child metrics must not be empty")

	_, err = conn.ExecContext(context.Background(), `PRAGMA disable_profiling`)
	require.NoError(t, err)

	info, err = GetProfilingInfo(conn)
	require.ErrorContains(t, err, errProfilingInfoEmpty.Error())
}

func TestErrProfiling(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	_, err := GetProfilingInfo(conn)
	testError(t, err, errProfilingInfoEmpty.Error())
}
