package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplacementScan(t *testing.T) {
	connector, err := NewConnector("", func(execer driver.ExecerContext) error {
		return nil
	})

	require.NoError(t, err)
	defer connector.Close()

	rangeRows := 100
	RegisterReplacementScan(connector, func(tableName string) (string, []any, error) {
		return "range", []any{int64(rangeRows)}, nil
	})

	db := sql.OpenDB(connector)
	rows, err := db.Query("select * from any_table")
	require.NoError(t, err)
	defer rows.Close()

	for i := 0; rows.Next(); i++ {
		var val int
		require.NoError(t, rows.Scan(&val))
		if val != i {
			require.Fail(t, "expected %d, got %d", i, val)
		}
		rangeRows--
	}
	if rows.Err() != nil {
		require.NoError(t, rows.Err())
	}
	if rangeRows != 0 {
		require.Fail(t, "expected 0, got %d", rangeRows)
	}
}
