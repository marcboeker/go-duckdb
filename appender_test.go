package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/stretchr/testify/require"
)

const (
	testAppenderTableDDL = `CREATE TABLE appdata(id BIGINT, u8 UTINYINT, i8 TINYINT, u16 USMALLINT, i16 SMALLINT,
		u32 UINTEGER, i32 INTEGER, u64 UBIGINT, i64 BIGINT,
		tm TIMESTAMP, f REAL, d DOUBLE, s VARCHAR, b BOOLEAN)`
)

func openAppenderDB(t *testing.T, pathname string) *sql.DB {
	db, err := sql.Open("duckdb", pathname)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func createAppenderTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableDDL)
	require.NoError(t, err)
	return &res
}

const (
	numAppenderTestRows = 10000
)

func TestRandomizedAppender(t *testing.T) {
	// t.Parallel()
	tmpFile, err := ioutil.TempFile(os.TempDir(), "go-duckdb-*.db")
	require.NoError(t, err)
	pathname := tmpFile.Name()
	tmpFile.Close()
	os.Remove(pathname)
	defer os.Remove(pathname)

	db := openAppenderDB(t, pathname)
	defer db.Close()
	createAppenderTable(db, t)

	type dataRow struct {
		ID  int       `db:"id"`
		U8  uint8     `db:"u8"`
		I8  int8      `db:"i8"`
		U16 uint16    `db:"u16"`
		I16 int16     `db:"i16"`
		U32 uint32    `db:"u32"`
		I32 int32     `db:"i32"`
		U64 uint64    `db:"u64"`
		I64 int64     `db:"i64"`
		TM  time.Time `db:"tm"`
		F   float32   `db:"f"`
		D   float64   `db:"d"`
		S   string    `db:"s"`
		B   bool      `db:"b"`
	}
	nextId := 1
	randRow := func() dataRow {
		id := nextId
		nextId++
		u64 := rand.Uint64()
		// go sql doesn't support uint64 values with high bit set (see for example https://github.com/lib/pq/issues/72)
		if u64 > 9223372036854775807 {
			u64 = 9223372036854775807
		}
		return dataRow{
			ID:  id,
			U8:  uint8(randInt(0, 255)),
			I8:  int8(randInt(-128, 127)),
			U16: uint16(randInt(0, 65535)),
			I16: int16(randInt(-32768, 32767)),
			U32: uint32(randInt(0, 4294967295)),
			I32: int32(randInt(-2147483648, 2147483647)),
			U64: u64,
			I64: rand.Int63(),
			TM:  time.UnixMilli(randInt(0, time.Now().UnixMilli())).UTC(),
			F:   rand.Float32(),
			D:   rand.Float64(),
			S:   randString(int(randInt(0, 128))),
			B:   randBool(),
		}
	}
	rows := []dataRow{}
	for i := 0; i < numAppenderTestRows; i++ {
		row := randRow()
		rows = append(rows, row)
	}
	dbconn, err := db.Conn(context.Background())
	require.NoError(t, err)
	err = dbconn.Raw(func(driverConn any) error {
		drvConn, ok := driverConn.(driver.Conn)
		require.True(t, ok)

		appender, err := NewAppenderFromConn(drvConn, "", "appdata")
		require.NoError(t, err)
		// NOTE: we don't defer the call to appender.Close(), but call it explicitly because appended data
		// may be cached and not inserted into the db immediately. The Close() call flushes the cache.

		for _, row := range rows {
			err := appender.AppendRow(row.ID, row.U8, row.I8, row.U16, row.I16, row.U32, row.I32, row.U64, row.I64,
				row.TM, row.F, row.D, row.S, row.B)
			require.NoError(t, err)
		}
		appender.Close()

		var readRows []dataRow
		require.NoError(t, sqlscan.Select(context.Background(), db, &readRows,
			"SELECT id, u8, i8, u16, i16, u32, i32, u64, i64, tm, f, d, s, b FROM appdata ORDER BY id"))

		require.Len(t, readRows, len(rows))
		for i := 0; i < len(rows); i++ {
			require.Equal(t, rows[i], readRows[i])
		}

		return nil
	})
	require.NoError(t, err)
}
