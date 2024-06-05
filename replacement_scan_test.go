package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReplacementScan(t *testing.T) {

	connector, err := NewConnector("", func(execer driver.ExecerContext) error {
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	var rangeRows = 100
	RegisterReplacementScan(connector, func(tableName string) (string, []any, error) {
		return "range", []any{int64(rangeRows)}, nil
	})

	db := sql.OpenDB(connector)

	rows, err := db.Query("select * from any_table")
	if err != nil {
		t.Fatal(err)
	}
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
