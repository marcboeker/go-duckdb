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

	var rangeRows = 3
	_ = RegisterReplacementScan(connector, func(tableName string) (string, []any, error) {
		return "range", []any{int64(rangeRows)}, nil
	})

	db := sql.OpenDB(connector)

	rows, err := db.Query("select * from any_table")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		rangeRows--
	}
	if rows.Err() != nil {
		require.NoError(t, rows.Err())
	}

	if rangeRows != 0 {
		t.Fatalf("expected 0 rows, got %d", rangeRows)
	}

}
