package duckdb

//
//import (
//	"context"
//	"database/sql"
//	"fmt"
//	"github.com/jmoiron/sqlx"
//	"github.com/stretchr/testify/require"
//	"math/rand"
//	"testing"
//)
//
//const (
//	testAppenderTableNestedMix = `
//		CREATE OR REPLACE TABLE nested_mix (S STRUCT(A INT[], B STRUCT(C VARCHAR[])[])[]);
//`
//)
//
//func createAppenderNestedMix(db *sql.DB, t *testing.T) *sql.Result {
//	res, err := db.Exec(testAppenderTableNestedMix)
//	require.NoError(t, err)
//	return &res
//}
//
//const mixRows = 100
//
//func TestNestedMix(t *testing.T) {
//
//	c, err := NewConnector("db", nil)
//	require.NoError(t, err)
//
//	db := sql.OpenDB(c)
//	createAppenderNestedMix(db, t)
//	defer db.Close()
//
//	type dataRow struct {
//		mix []S
//	}
//	randRow := func(i int) dataRow {
//		return dataRow{
//			mix: createMixList(rand.Int31n(100)),
//		}
//	}
//	rows := []dataRow{}
//	for i := 0; i < mixRows; i++ {
//		rows = append(rows, randRow(i))
//	}
//
//	conn, err := c.Connect(context.Background())
//	require.NoError(t, err)
//	defer conn.Close()
//
//	appender, err := NewAppenderFromConn(conn, "", "nested_mix")
//	require.NoError(t, err)
//	defer appender.Close()
//
//	for _, row := range rows {
//		err := appender.AppendRow(
//			row.mix,
//		)
//		require.NoError(t, err)
//	}
//	err = appender.Flush()
//	require.NoError(t, err)
//
//	var res_db *sqlx.DB
//
//	res_db, err = sqlx.Open("duckdb", "db")
//	require.NoError(t, err)
//
//	res, err := res_db.Queryx("SELECT S FROM nested_mix")
//	require.NoError(t, err)
//
//	type rowRes struct {
//		NS []S `db:"S"`
//	}
//
//	for res.Next() {
//		var r rowRes
//		err := res.StructScan(&r)
//		if err != nil {
//			panic(err.Error())
//		}
//
//		// Print the scanned data for debugging
//		fmt.Printf("%+v\n", r.NS)
//	}
//
//}
//
////res, err := db.QueryContext(
////	context.Background(), `
////		SELECT
////			s
////  FROM nested_mix
////  `)
////require.NoError(t, err)
////defer res.Close()
//
////i := 0
////for res.Next() {
////	r := DataRow{}
////	var mix []interface{}
////	err := res.Scan(
////		&mix,
////	)
////	require.NoError(t, err)
////	r.mix = convertDuckDBMixListToMixList(mix)
////	require.Equal(t, rows[i], r)
////	i++
////}
//// Ensure that the number of fetched rows equals the number of inserted rows.
////require.Equal(t, i, mixRows)
