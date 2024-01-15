package duckdb

//
//import (
//	"context"
//	"database/sql"
//	"github.com/stretchr/testify/require"
//	"testing"
//)
//
//const (
//	testAppenderSMOL = `
//  CREATE TABLE test(
//    smolList STRUCT(I INT)[]
//  )`
//)
//
//func createAppenderSMOL(db *sql.DB, t *testing.T) *sql.Result {
//	res, err := db.Exec(testAppenderSMOL)
//	require.NoError(t, err)
//	return &res
//}
//
//func TestNestedAppenderSMOL(t *testing.T) {
//
//	c, err := NewConnector("", nil)
//	require.NoError(t, err)
//
//	db := sql.OpenDB(c)
//	createAppenderSMOL(db, t)
//	defer db.Close()
//
//	type dataRow struct {
//		smolList []smol
//	}
//	randRow := func(i int) dataRow {
//
//		return dataRow{
//			smolList: fillSMOL(100),
//		}
//	}
//	rows := []dataRow{}
//	for i := 0; i < 100; i++ {
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
//			row.smolList,
//		)
//		require.NoError(t, err)
//	}
//	err = appender.Flush()
//	require.NoError(t, err)
//
//	res, err := db.QueryContext(
//		context.Background(), `
//			SELECT
//				smolList,
//      FROM test
//      `)
//	require.NoError(t, err)
//	defer res.Close()
//
//	i := 0
//	for res.Next() {
//		r := dataRow{}
//		var sml []interface{}
//		err := res.Scan(
//			&sml,
//		)
//		require.NoError(t, err)
//		r.smolList = convertToSmol(sml)
//		require.Equal(t, rows[i], r)
//		i++
//	}
//	// Ensure that the number of fetched rows equals the number of inserted rows.
//	require.Equal(t, i, 100)
//}
