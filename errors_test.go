package duckdb

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

var testErrOpenMap = map[string]string{
	errParseDSN.Error():  ":mem ory:",
	errOpen.Error():      "?readonly",
	errSetConfig.Error(): "?threads=NaN",
}

func TestErrOpen(t *testing.T) {

	for errMsg, dsn := range testErrOpenMap {
		t.Run(errMsg, func(t *testing.T) {
			_, err := sql.Open("duckdb", dsn)
			require.Contains(t, err.Error(), errMsg)
		})
	}

	t.Run("local config option", func(t *testing.T) {
		_, err := sql.Open("duckdb", "?schema=main")
		require.Contains(t, err.Error(), errSetConfig.Error())
	})
}
