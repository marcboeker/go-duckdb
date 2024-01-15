package duckdb

//
//import (
//	"context"
//	"database/sql"
//	"github.com/stretchr/testify/require"
//	"testing"
//	//"github.com/jmoiron/sqlx"
//)
//
//const (
//	testAppenderTableNestedListInStructInList = `
//  CREATE TABLE test(
//	listStructList STRUCT(L STRUCT(I INT, L_I INT[])[])
//  )`
//)
//
//func createAppenderNestedListInStructInList(db *sql.DB, t *testing.T) *sql.Result {
//	res, err := db.Exec(testAppenderTableNestedListInStructInList)
//	require.NoError(t, err)
//	return &res
//}
//
//type listInStruct struct {
//	I   int32
//	L_I []int32
//}
//
//type listInStructList struct {
//	L []listInStruct
//}
//
//func createListInStruct(i int) listInStruct {
//	if i <= 0 {
//		i = 1
//	}
//
//	return listInStruct{
//		I:   int32(i),
//		L_I: createIntSlice(int32(i)),
//	}
//}
//
//func createListInStructList(i int) listInStructList {
//	if i <= 0 {
//		i = 1
//	}
//
//	var l []listInStruct
//	var j int
//	for j = 0; j < i; j++ {
//		l = append(l, createListInStruct(j))
//	}
//	return listInStructList{
//		L: l,
//	}
//}
//
//func TestNestedListInStructInListAppender(t *testing.T) {
//
//	c, err := NewConnector("", nil)
//	require.NoError(t, err)
//
//	db := sql.OpenDB(c)
//	createAppenderNestedListInStructInList(db, t)
//	defer db.Close()
//
//	type dataRow struct {
//		listStruct listInStructList
//	}
//	randRow := func(i int) dataRow {
//		return dataRow{
//			listStruct: createListInStructList(5),
//		}
//	}
//	rows := []dataRow{}
//	for i := 0; i < totalRows; i++ {
//		rows = append(rows, randRow(i))
//	}
//
//	conn, err := c.Connect(context.Background())
//	require.NoError(t, err)
//	defer conn.Close()
//
//	appender, err := NewAppenderFromConn(conn, "", "test")
//	require.NoError(t, err)
//	defer appender.Close()
//
//	for _, row := range rows {
//		err := appender.AppendRow(
//			row.listStruct,
//		)
//		require.NoError(t, err)
//	}
//	err = appender.Flush()
//	require.NoError(t, err)
//
//	res, err := db.QueryContext(
//		context.Background(), `
//			SELECT
//				listStructList
//      FROM test
//      `)
//	require.NoError(t, err)
//	defer res.Close()
//
//	//var resTbl []listInStructList
//	//err := scan.Rows(&resTbl, res)
//
//	i := 0
//	for res.Next() {
//		r := dataRow{}
//		var listStruct interface{}
//		err := res.Scan(
//			&listStruct,
//		)
//		require.NoError(t, err)
//		r.listStruct = convertDuckDBListStructInListToListStructInList(listStruct)
//		require.Equal(t, rows[i], r)
//		i++
//	}
//	// Ensure that the number of fetched rows equals the number of inserted rows.
//	require.Equal(t, i, totalRows)
//}
//
//func convertDuckDBListStructToListInStruct(s interface{}) listInStruct {
//	var listStruct listInStruct
//	vals := s.(map[string]interface{})
//	listStruct.I = vals["I"].(int32)
//	listStruct.L_I = convertInterfaceToIntSlice(vals["L_I"].([]interface{}))
//	return listStruct
//}
//
//func convertDuckDBListStructInListToListStructInList(s interface{}) listInStructList {
//	var listStructList listInStructList
//	innerList := s.(map[string]interface{})["L"]
//
//	for _, v := range innerList.([]interface{}) {
//		listStructList.L = append(listStructList.L, convertDuckDBListStructToListInStruct(v))
//	}
//	return listStructList
//}
