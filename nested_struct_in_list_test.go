package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	testAppenderTableNestedStructInList = `
  CREATE TABLE test(
	structList STRUCT(I INT, V INT)[],
  )`
)

func createAppenderNestedStructInList(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNestedStructInList)
	require.NoError(t, err)
	return &res
}

type two_ints struct {
	I int32
	V int32
}

func createTwoIntsStructSlice(i int) []two_ints {
	if i <= 0 {
		i = 1
	}

	var l []two_ints
	var j int
	for j = 0; j < i; j++ {
		l = append(l, two_ints{
			I: int32(j),
			V: int32(j + 100),
		})
	}
	return l
}

const totalRows = 2

func TestNestedStructInListAppender(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedStructInList(db, t)
	defer db.Close()

	type dataRow struct {
		structList []two_ints
	}
	randRow := func(i int) dataRow {

		return dataRow{
			structList: createTwoIntsStructSlice(5),
		}
	}
	rows := []dataRow{}
	for i := 0; i < totalRows; i++ {
		rows = append(rows, randRow(i))
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)
	defer appender.Close()

	for _, row := range rows {
		err := appender.AppendRow(
			row.structList,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  
				structList
      FROM test
      `)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		var structList []interface{}
		err := res.Scan(
			&structList,
		)
		require.NoError(t, err)
		r.structList = convertDuckDBStructToTwoIntsSlice(structList)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, totalRows)
}

func convertDuckDBStructToTwoIntsSlice(s []interface{}) []two_ints {
	var l []two_ints
	for _, v := range s {
		m := v.(map[string]interface{})
		l = append(l, two_ints{
			I: int32(m["I"].(int32)),
			V: int32(m["V"].(int32)),
		})
	}
	return l
}
