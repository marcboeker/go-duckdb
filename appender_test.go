package duckdb

import (
	"context"
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testAppenderTableDDL = `
  CREATE TABLE test(
    id BIGINT,
    uint8 UTINYINT,
    int8 TINYINT,
    uint16 USMALLINT,
    int16 SMALLINT,
		uint32 UINTEGER,
    int32 INTEGER,
    uint64 UBIGINT,
    int64 BIGINT,
		timestamp TIMESTAMP,
    float REAL,
    double DOUBLE,
    string VARCHAR,
    bool BOOLEAN
  )`
)

func createAppenderTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableDDL)
	require.NoError(t, err)
	return &res
}

const numAppenderTestRows = 10000

func randInt(lo int64, hi int64) int64 {
	return rand.Int63n(hi-lo+1) + lo
}

func randBool() bool {
	return (rand.Int()%2 == 0)
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestAppender(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderTable(db, t)
	defer db.Close()

	type dataRow struct {
		ID        int
		UInt8     uint8
		Int8      int8
		UInt16    uint16
		Int16     int16
		UInt32    uint32
		Int32     int32
		UInt64    uint64
		Int64     int64
		Timestamp time.Time
		Float     float32
		Double    float64
		String    string
		Bool      bool
	}
	randRow := func(i int) dataRow {
		u64 := rand.Uint64()
		// go sql doesn't support uint64 values with high bit setPrimitive (see for example https://github.com/lib/pq/issues/72)
		if u64 > 9223372036854775807 {
			u64 = 9223372036854775807
		}
		return dataRow{
			ID:        i,
			UInt8:     uint8(randInt(0, 255)),
			Int8:      int8(randInt(-128, 127)),
			UInt16:    uint16(randInt(0, 65535)),
			Int16:     int16(randInt(-32768, 32767)),
			UInt32:    uint32(randInt(0, 4294967295)),
			Int32:     int32(randInt(-2147483648, 2147483647)),
			UInt64:    u64,
			Int64:     rand.Int63(),
			Timestamp: time.UnixMilli(randInt(0, time.Now().UnixMilli())).UTC(),
			Float:     rand.Float32(),
			Double:    rand.Float64(),
			String:    randString(int(randInt(0, 128))),
			Bool:      randBool(),
		}
	}
	rows := []dataRow{}
	for i := 0; i < numAppenderTestRows; i++ {
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
			row.ID,
			row.UInt8,
			row.Int8,
			row.UInt16,
			row.Int16,
			row.UInt32,
			row.Int32,
			row.UInt64,
			row.Int64,
			row.Timestamp,
			row.Float,
			row.Double,
			row.String,
			row.Bool,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  id,
					uint8,
					int8,
					uint16,
					int16,
					uint32,
					int32,
					uint64,
					int64,
					timestamp,
					float,
					double,
					string,
					bool
      FROM test
      ORDER BY id`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		err := res.Scan(
			&r.ID,
			&r.UInt8,
			&r.Int8,
			&r.UInt16,
			&r.Int16,
			&r.UInt32,
			&r.Int32,
			&r.UInt64,
			&r.Int64,
			&r.Timestamp,
			&r.Float,
			&r.Double,
			&r.String,
			&r.Bool,
		)
		require.NoError(t, err)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, numAppenderTestRows)
}
