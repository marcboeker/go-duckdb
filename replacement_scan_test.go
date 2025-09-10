package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

// FIXME: More replacement scan tests, also failure paths.
func TestReplacementScan(t *testing.T) {
	c := newConnectorWrapper(t, ``, func(execer driver.ExecerContext) error {
		return nil
	})
	defer closeConnectorWrapper(t, c)

	rangeRows := 100
	RegisterReplacementScan(c, func(tableName string) (string, []any, error) {
		return "range", []any{int64(rangeRows)}, nil
	})

	db := sql.OpenDB(c)
	defer closeDbWrapper(t, db)

	res, err := db.Query("SELECT * FROM any_table")
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	for i := 0; res.Next(); i++ {
		var val int
		require.NoError(t, res.Scan(&val))
		require.Equal(t, i, val)
		rangeRows--
	}
	require.NoError(t, res.Err())
	require.Equal(t, 0, rangeRows)
}

func TestReplacementScanList(t *testing.T) {
	c := newConnectorWrapper(t, ``, func(execer driver.ExecerContext) error {
		return nil
	})
	defer closeConnectorWrapper(t, c)

	RegisterReplacementScan(c, func(tableName string) (string, []any, error) {
		return "read_text", []any{[]string{"testdata/testA.txt", "testdata/testB.txt"}}, nil
	})

	db := sql.OpenDB(c)
	defer closeDbWrapper(t, db)

	var length int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM any_table").Scan(&length))
	require.Equal(t, 2, length)
}
