package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

const (
	testAppenderTableNestedMix = `
		CREATE OR REPLACE TABLE nested_mix (s STRUCT(A INT[], B STRUCT(C VARCHAR[])[])[]);;
`
)

func createAppenderNestedMix(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNestedMix)
	require.NoError(t, err)
	return &res
}

const mixRows = 100

func TestNestedMix(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedMix(db, t)
	defer db.Close()

	type dataRow struct {
		mix []mix_s
	}
	randRow := func(i int) dataRow {
		return dataRow{
			mix: createMixList(rand.Int31n(100)),
		}
	}
	rows := []dataRow{}
	for i := 0; i < mixRows; i++ {
		rows = append(rows, randRow(i))
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "nested_mix")
	require.NoError(t, err)
	defer appender.Close()

	for _, row := range rows {
		err := appender.AppendRow(
			row.mix,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  
				s
      FROM nested_mix
      `)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		var mix []interface{}
		err := res.Scan(
			&mix,
		)
		require.NoError(t, err)
		r.mix = convertDuckDBMixListToMixList(mix)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, mixRows)
}
