package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

const (
	testAppenderTableNestedListInStruct = `
  CREATE TABLE test(
	listStruct STRUCT(L INT[])
  )`
)

func createAppenderNestedListInStruct(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNestedListInStruct)
	require.NoError(t, err)
	return &res
}

type list_struct struct {
	L []int32
}

func TestNestedListInStructAppender(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedListInStruct(db, t)
	defer db.Close()

	type dataRow struct {
		listStruct list_struct
	}
	randRow := func(i int) dataRow {

		return dataRow{
			listStruct: list_struct{createIntSlice(rand.Int31n(3000))},
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
			row.listStruct,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  
				listStruct
      FROM test
      `)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		var listStruct interface{}
		err := res.Scan(
			&listStruct,
		)
		require.NoError(t, err)
		r.listStruct = convertDuckDBListStructToListStruct(listStruct)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, totalRows)
}

func convertDuckDBListStructToListStruct(s interface{}) list_struct {
	var listStruct list_struct
	innerList := s.(map[string]interface{})["L"]

	listStruct.L = convertInterfaceToIntSlice(innerList.([]interface{}))
	return listStruct
}
