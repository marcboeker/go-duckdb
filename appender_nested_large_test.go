package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

type largeDataRowInterface struct {
	stringList []interface{}
	intList    []interface{}
	base       interface{}
}

type largeDataRow struct {
	ID         int
	stringList ListString
	intList    Int32List
	base       Base
}

func TestNestedAppenderLargeTable(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
				id BIGINT,
				stringList VARCHAR[],
				intList INT[],
				base STRUCT(I INT, V VARCHAR),
		)`,
	)
	require.NoError(t, err)
	defer db.Close()

	initLargeRow := func(i int) largeDataRow {
		dR := largeDataRow{ID: i}
		dR.stringList.Fill()
		dR.intList.Fill()
		dR.base.Fill(i)
		return dR
	}

	rows := make([]largeDataRow, 0, numAppenderTestRows)
	for i := 0; i < numAppenderTestRows; i++ {
		rows = append(rows, initLargeRow(i))
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)
	defer appender.Close()

	for _, row := range rows {
		err := appender.AppendRow(
			row.ID,
			row.stringList.L,
			row.intList,
			row.base,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test ORDER BY id`,
	)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := largeDataRow{}
		interfaces := largeDataRowInterface{}
		err := res.Scan(
			&r.ID,
			&interfaces.stringList,
			&interfaces.intList,
			&interfaces.base,
		)
		require.NoError(t, err)
		r.stringList.FillFromInterface(interfaces.stringList)
		r.intList.FillInnerFromInterface(interfaces.intList)
		r.base.FillFromInterface(interfaces.base)
		require.Equal(t, rows[i], r)
		i++
	}

	// Ensure that the number of rows returned is correct
	require.Equal(t, numAppenderTestRows, i)
}
