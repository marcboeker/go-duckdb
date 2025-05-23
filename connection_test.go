package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTableNames(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	query := `SELECT * FROM schema1.table1, catalog3."schema.2"."table.2"`
	tableNames, err := GetTableNames(conn, query, true)
	require.NoError(t, err)

	require.Len(t, tableNames, 2)
	require.Contains(t, tableNames, `schema1.table1`)
	require.Contains(t, tableNames, `catalog3."schema.2"."table.2"`)
}
