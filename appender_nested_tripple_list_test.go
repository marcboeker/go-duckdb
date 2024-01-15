package duckdb

//
//import (
//	"context"
//	"database/sql"
//	"github.com/stretchr/testify/require"
//	"math/rand"
//	"testing"
//)
//
//const (
//	testAppenderTableNestedLLL = `
//		CREATE OR REPLACE TABLE nested_list (LLL INT[][][]);
//`
//)
//
//func createAppenderNestedLLL(db *sql.DB, t *testing.T) *sql.Result {
//	res, err := db.Exec(testAppenderTableNestedLLL)
//	require.NoError(t, err)
//	return &res
//}
//
//const LLLRows = 100
//
//func TestNestedLLL(t *testing.T) {
//
//	c, err := NewConnector("", nil)
//	require.NoError(t, err)
//
//	db := sql.OpenDB(c)
//	createAppenderNestedLLL(db, t)
//	defer db.Close()
//
//	type dataRow struct {
//		LLL [][][]int32
//	}
//
//	randRow := func(i int) dataRow {
//		return dataRow{
//			LLL: createNestedLLL(rand.Int31n(100)),
//		}
//	}
//	rows := []dataRow{}
//	for i := 0; i < LLLRows; i++ {
//		rows = append(rows, randRow(i))
//	}
//
//	conn, err := c.Connect(context.Background())
//	require.NoError(t, err)
//	defer conn.Close()
//
//	appender, err := NewAppenderFromConn(conn, "", "nested_list")
//	require.NoError(t, err)
//	defer appender.Close()
//
//	for _, row := range rows {
//		err := appender.AppendRow(
//			row.LLL,
//		)
//		require.NoError(t, err)
//	}
//	err = appender.Flush()
//	require.NoError(t, err)
//
//	res, err := db.QueryContext(
//		context.Background(), `
//			SELECT
//				LLL
//      FROM nested_list
//      `)
//	require.NoError(t, err)
//	defer res.Close()
//
//	i := 0
//	for res.Next() {
//		r := dataRow{}
//		var LLL []interface{}
//		err := res.Scan(
//			&LLL,
//		)
//		require.NoError(t, err)
//		r.LLL = convertDuckDBListListListToListListList(LLL)
//		require.Equal(t, rows[i], r)
//		i++
//	}
//	// Ensure that the number of fetched rows equals the number of inserted rows.
//	require.Equal(t, i, LLLRows)
//}
