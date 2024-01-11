package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	testAppenderTableNestedListInStructInList = `
  CREATE TABLE test(
	listStructList STRUCT(L STRUCT(I INT, L_I INT[])[])
  )`
)

func createAppenderNestedListInStructInList(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNestedListInStructInList)
	require.NoError(t, err)
	return &res
}

type listInStructList struct {
	L []struct {
		I   int32
		L_I []int32
	}
}

func TestNestedListInStructInListAppender(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedListInStructInList(db, t)
	defer db.Close()

	type dataRow struct {
		listStruct listInStructList
	}
	randRow := func(i int) dataRow {

		return dataRow{}
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

	//var resTbl []listInStructList
	//err := scan.Rows(&resTbl, res)

	//i := 0
	//for res.Next() {
	//	r := dataRow{}
	//	var listStruct []interface{}
	//	err := res.Scan(
	//		&listStruct,
	//	)
	//	require.NoError(t, err)
	//	r.listStruct = convertDuckDB
	//	require.Equal(t, rows[i], r)
	//	i++
	//}
	//// Ensure that the number of fetched rows equals the number of inserted rows.
	//require.Equal(t, i, totalRows)
}

//func convertDuckDBStructInListInStructToListInStructList(s []interface{}) listInStructList {
//	var listStruct listInStructList
//	for _, v := range s {
//		listStruct.L = append(listStruct.L, convertDuckDBStructInListInStructToListInStruct(v))
//	}
//}
//
//func convertDuckDBStructInListInStructToListInStruct(v interface{}) struct {
//	I   int32
//	L_I []int32
//} {
//	m := v.(map[string]interface{})
//
//}
