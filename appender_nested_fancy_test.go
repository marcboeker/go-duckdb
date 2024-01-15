package duckdb

//
//import (
//	"context"
//	"database/sql"
//	"fmt"
//	"github.com/jmoiron/sqlx"
//	"github.com/stretchr/testify/require"
//	"testing"
//)
//
//const (
//	testAppenderTableN = `
//		CREATE OR REPLACE TABLE test (
//		id BIGINT,
//		intList STRUCT(I INT, V VARCHAR)
//   );
//`
//)
//
//func createAppenderN(db *sql.DB, t *testing.T) *sql.Result {
//	res, err := db.Exec(testAppenderTableN)
//	require.NoError(t, err)
//	return &res
//}
//
//const alskdj = 100
//
//func TestNested(t *testing.T) {
//
//	c, err := NewConnector("", nil)
//	require.NoError(t, err)
//
//	db := sql.OpenDB(c)
//	createAppenderN(db, t)
//	defer db.Close()
//
//	type DataRow struct {
//		ID      int     `db:"id"`
//		IntList struct1 `db:"intList"`
//	}
//	randRow := func(i int) DataRow {
//		return DataRow{
//			ID:      i,
//			IntList: createStruct1(100),
//		}
//	}
//	rows := []DataRow{}
//	for i := 0; i < alskdj; i++ {
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
//			row.ID,
//			row.IntList,
//		)
//		require.NoError(t, err)
//	}
//	err = appender.Flush()
//	require.NoError(t, err)
//
//	res_db := sqlx.NewDb(db, "duckdb")
//	require.NoError(t, err)
//
//	dataR := []DataRow{}
//	err = res_db.Select(&dataR, "SELECT * FROM test")
//	require.NoError(t, err)
//	fmt.Println(dataR)
//}
