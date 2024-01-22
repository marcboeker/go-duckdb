package duckdb

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

const (
	testAppenderTableDDL = `
  CREATE TABLE test(
		id BIGINT,
		uuid UUID,
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
		bool BOOLEAN,
-- 	    blob BLOB
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
		UUID      UUID
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
		//Blob      []byte `duckdb:"blob"`
	}
	randRow := func(i int) dataRow {
		u64 := rand.Uint64()
		// go sql doesn't support uint64 values with high bit set (see for example https://github.com/lib/pq/issues/72)
		if u64 > 9223372036854775807 {
			u64 = 9223372036854775807
		}

		b, err := uuid.New().MarshalBinary()
		require.NoError(t, err)
		var uuidBytes [16]byte
		copy(uuidBytes[:], b)

		return dataRow{
			ID:        i,
			UUID:      uuidBytes,
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
			//Blob:      []byte{1},
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

	for i, row := range rows {
		if i%3000 == 0 {
			err = appender.Flush()
			require.NoError(t, err)
		}
		err := appender.AppendRow(
			row.ID,
			row.UUID,
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
			//row.Blob,
		)
		require.NoError(t, err)
	}
	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(), `
			SELECT  id,
					uuid,
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
					bool,
-- 					blob
      FROM test
      ORDER BY id`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	var scannedUUID []byte
	for res.Next() {
		r := dataRow{}
		err := res.Scan(
			&r.ID,
			&scannedUUID,
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
			//&r.Blob,
		)
		require.NoError(t, err)
		copy(r.UUID[:], scannedUUID)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, i, numAppenderTestRows)
}

func TestAppenderNullList(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`CREATE TABLE test(intSlice INT[][])`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Int32ListList{{1, 2, 3}, {4, 5, 6}},
	)

	err = appender.AppendRow(
		nil,
	)

	err = appender.AppendRow(
		Int32ListList{nil, {4, 5, 6}},
	)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT intSlice FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var intS []interface{}
		err := res.Scan(
			&intS,
		)
		if i == 0 {
			require.NoError(t, err)
		} else if i == 1 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if intS != nil {
				if intS[0] != nil {
					panic("expected nil")
				}
				// cast to []int32
				c := intS[1].([]interface{})
				if c[0].(int32) != 4 || c[1].(int32) != 5 || c[2].(int32) != 6 {
					panic("expected [4, 5, 6]")
				}
			} else {
				panic("expected non-nil")
			}
		}
		i++
	}
}

func TestAppenderNullStruct(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
			base STRUCT(I INT, V VARCHAR),
    	)
    `)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Base{1, "hello"},
	)

	err = appender.AppendRow(
		nil,
	)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var intS interface{}
		err := res.Scan(
			&intS,
		)
		if i == 0 {
			require.NoError(t, err)
		} else if i == 1 {
			require.Equal(t, nil, intS)
		}
		i++
	}
}

func TestAppenderNullIntAndString(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
				ID BIGINT, 
				str VARCHAR
    	)
    `)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		32,
		"hello",
	)
	err = appender.AppendRow(
		nil,
		nil,
	)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var id int
		var str string
		err = res.Scan(
			&id,
			&str,
		)
		if i == 0 {
			require.NoError(t, err)
			require.Equal(t, id, 32)
			require.Equal(t, str, "hello")
		} else {
			require.Error(t, err)
		}
		i++
	}

}
