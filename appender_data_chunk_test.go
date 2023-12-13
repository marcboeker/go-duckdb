package duckdb

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

const (
	testDataChunkTableDDL = `
	create table test(
		id BIGINT,
		uint8 UTINYINT,
		int8 TINYINT,
		uint16 USMALLINT,
		int16 SMALLINT,
		uint32 UINTEGER,
		int32 INTEGER,
		uint64 UBIGINT,
		int64 BIGINT,
		float REAL,
		double DOUBLE,
		bool BOOLEAN,
		string VARCHAR,
		timestamp TIMESTAMP,
	);`
)

func createDataChunkTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testDataChunkTableDDL)
	require.NoError(t, err)
	return &res
}

func TestDataChunkAppender(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createDataChunkTable(db, t)
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
		Float     float32
		Double    float64
		Bool      bool
		String    string
		Timestamp time.Time
	}

	randRow := func(i int) dataRow {
		u64 := rand.Uint64()
		// go sql doesn't support uint64 values with high bit set (see for example https://github.com/lib/pq/issues/72)
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
			Float:     rand.Float32(),
			Double:    rand.Float64(),
			Bool:      randBool(),
			String:    randString(int(randInt(0, 128))),
			Timestamp: time.UnixMilli(randInt(0, time.Now().UnixMilli())).UTC(),
		}
	}

	rows := []dataRow{}
	for i := 0; i < 2048; i++ {
		rows = append(rows, randRow(i))
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)
	defer appender.Close()

	err = appender.AppendRows(rows)
	require.NoError(t, err)
	err = appender.Flush()
	require.NoError(t, err)

	// FOR 1 ROW
	//var (
	//	id        int
	//	small_int uint8
	//)
	//row := db.QueryRow(`SELECT * FROM test`)
	//err = row.Scan(&id, &small_int)
	//if errors.Is(err, sql.ErrNoRows) {
	//	log.Println("no rows")
	//} else if err != nil {
	//	log.Fatal(err)
	//}
	//require.Equal(t, rows[0].ID, id)
	//require.Equal(t, rows[0].UInt8, small_int)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  *
	 FROM test,
	 --ORDER BY id`,
	)
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
			&r.Float,
			&r.Double,
			&r.Bool,
			&r.String,
			&r.Timestamp,
		)
		require.NoError(t, err)
		require.Equal(t, rows[i], r)
		i++
	}
}
