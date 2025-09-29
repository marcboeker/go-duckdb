package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTableNames(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	tests := []struct {
		name           string
		query          string
		qualified      bool
		expectedTables []string
		expectedError  string
	}{
		{
			name:           "valid query with multiple tables, qualified",
			query:          `SELECT * FROM schema1.table1, catalog3."schema.2"."table.2"`,
			qualified:      true,
			expectedTables: []string{"schema1.table1", `catalog3."schema.2"."table.2"`},
		},
		{
			name:           "valid query with multiple tables, unqualified",
			query:          `SELECT * FROM schema1.table1, catalog3."schema.2"."table.2"`,
			qualified:      false,
			expectedTables: []string{"table1", "table.2"},
		},
		{
			name:           "valid query with no tables",
			query:          "SELECT 1 as num",
			expectedTables: nil,
		},
		{
			name:          "invalid query syntax",
			query:         "SELECT * FROM WHERE",
			expectedError: "Parser Error: syntax error at or near \"WHERE\"",
		},
		{
			name:          "empty query",
			query:         "",
			expectedError: "empty query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableNames, err := GetTableNames(conn, tt.query, tt.qualified)
			if tt.expectedError != "" {
				require.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, tableNames)
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tt.expectedTables, tableNames)
			}
		})
	}
}
