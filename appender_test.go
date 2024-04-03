package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
)

const numAppenderTestRows = 10000

type simpleStruct struct {
	A int32
	B string
}

type wrappedSimpleStruct struct {
	A string
	B simpleStruct
}

type wrappedStruct struct {
	N string
	M simpleStruct
}

type doubleWrappedStruct struct {
	X string
	Y wrappedStruct
}

type structWithList struct {
	L []int32
}

type mixedStruct struct {
	A struct {
		L []string
	}
	B []struct {
		L []int32
	}
}

type nestedDataRow struct {
	ID                  int64
	stringList          []string
	intList             []int32
	nestedIntList       [][]int32
	tripleNestedIntList [][][]int32
	simpleStruct        simpleStruct
	wrappedStruct       wrappedStruct
	doubleWrappedStruct doubleWrappedStruct
	structList          []simpleStruct
	structWithList      structWithList
	mix                 mixedStruct
	mixList             []mixedStruct
}

type resultRow struct {
	ID                  int64
	stringList          []any
	intList             []any
	nestedIntList       []any
	tripleNestedIntList []any
	simpleStruct        any
	wrappedStruct       any
	doubleWrappedStruct any
	structList          []any
	structWithList      any
	mix                 any
	mixList             []any
}

func castList[T any](val []any) []T {
	res := make([]T, len(val))
	for i, v := range val {
		res[i] = v.(T)
	}
	return res
}

func castMapListToStruct[T any](t *testing.T, val []any) []T {
	res := make([]T, len(val))
	for i, v := range val {
		err := mapstructure.Decode(v, &res[i])
		require.NoError(t, err)
	}
	return res
}

func castMapToStruct[T any](t *testing.T, val any) T {
	var res T
	err := mapstructure.Decode(val, &res)
	require.NoError(t, err)
	return res
}

func randInt(lo int64, hi int64) int64 {
	return rand.Int63n(hi-lo+1) + lo
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func cleanupAppender(t *testing.T, c *Connector, con driver.Conn, a *Appender) {
	require.NoError(t, a.Close())
	require.NoError(t, con.Close())
	require.NoError(t, c.Close())
}

func prepareAppender(t *testing.T, createTbl string) (*Connector, driver.Conn, *Appender) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	_, err = sql.OpenDB(c).Exec(createTbl)
	require.NoError(t, err)

	con, err := c.Connect(context.Background())
	require.NoError(t, err)

	a, err := NewAppenderFromConn(con, "", "test")
	require.NoError(t, err)

	return c, con, a
}

func TestAppenderClose(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (i INTEGER)`)
	require.NoError(t, a.AppendRow(int32(42)))
	cleanupAppender(t, c, con, a)
}

func TestAppenderPrimitive(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
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
			timestampS TIMESTAMP_S,
			timestampMS TIMESTAMP_MS,
			timestampNS TIMESTAMP_NS,
			timestampTZ TIMESTAMPTZ,
			float REAL,
			double DOUBLE,
			string VARCHAR,
			bool BOOLEAN
	  	)`)

	type row struct {
		ID          int64
		UInt8       uint8
		Int8        int8
		UInt16      uint16
		Int16       int16
		UInt32      uint32
		Int32       int32
		UInt64      uint64
		Int64       int64
		Timestamp   time.Time
		TimestampS  time.Time
		TimestampMS time.Time
		TimestampNS time.Time
		TimestampTZ time.Time
		Float       float32
		Double      float64
		String      string
		Bool        bool
	}

	// Get the timestamp for all TS columns.
	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"
	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", IST)
	require.NoError(t, err)

	rowsToAppend := make([]row, numAppenderTestRows)
	for i := 0; i < numAppenderTestRows; i++ {

		u64 := rand.Uint64()
		// Go SQL does not support uint64 values with their high bit set (see for example https://github.com/lib/pq/issues/72).
		if u64 > 9223372036854775807 {
			u64 = 9223372036854775807
		}

		rowsToAppend[i] = row{
			ID:          int64(i),
			UInt8:       uint8(randInt(0, 255)),
			Int8:        int8(randInt(-128, 127)),
			UInt16:      uint16(randInt(0, 65535)),
			Int16:       int16(randInt(-32768, 32767)),
			UInt32:      uint32(randInt(0, 4294967295)),
			Int32:       int32(randInt(-2147483648, 2147483647)),
			UInt64:      u64,
			Int64:       rand.Int63(),
			Timestamp:   ts,
			TimestampS:  ts,
			TimestampMS: ts,
			TimestampNS: ts,
			TimestampTZ: ts,
			Float:       rand.Float32(),
			Double:      rand.Float64(),
			String:      randString(int(randInt(0, 128))),
			Bool:        rand.Int()%2 == 0,
		}

		require.NoError(t, a.AppendRow(
			rowsToAppend[i].ID,
			rowsToAppend[i].UInt8,
			rowsToAppend[i].Int8,
			rowsToAppend[i].UInt16,
			rowsToAppend[i].Int16,
			rowsToAppend[i].UInt32,
			rowsToAppend[i].Int32,
			rowsToAppend[i].UInt64,
			rowsToAppend[i].Int64,
			rowsToAppend[i].Timestamp,
			rowsToAppend[i].TimestampS,
			rowsToAppend[i].TimestampMS,
			rowsToAppend[i].TimestampNS,
			rowsToAppend[i].TimestampTZ,
			rowsToAppend[i].Float,
			rowsToAppend[i].Double,
			rowsToAppend[i].String,
			rowsToAppend[i].Bool,
		))

		if i%1000 == 0 {
			require.NoError(t, a.Flush())
		}
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT * FROM test ORDER BY id`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		r := row{}
		require.NoError(t, res.Scan(
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
			&r.TimestampS,
			&r.TimestampMS,
			&r.TimestampNS,
			&r.TimestampTZ,
			&r.Float,
			&r.Double,
			&r.String,
			&r.Bool,
		))
		rowsToAppend[i].Timestamp = rowsToAppend[i].Timestamp.UTC()
		rowsToAppend[i].TimestampS = rowsToAppend[i].TimestampS.UTC()
		rowsToAppend[i].TimestampMS = rowsToAppend[i].TimestampMS.UTC()
		rowsToAppend[i].TimestampNS = rowsToAppend[i].TimestampNS.UTC()
		rowsToAppend[i].TimestampTZ = rowsToAppend[i].TimestampTZ.UTC()
		require.Equal(t, rowsToAppend[i], r)
		i++
	}

	require.Equal(t, numAppenderTestRows, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderList(t *testing.T) {
	c, con, a := prepareAppender(t, `
	CREATE TABLE test (
		string_list VARCHAR[],
		int_list INTEGER[]
	)`)

	rowsToAppend := make([]nestedDataRow, 10)
	for i := 0; i < 10; i++ {
		rowsToAppend[i].stringList = []string{"a", "b", "c"}
		rowsToAppend[i].intList = []int32{1, 2, 3}
	}

	for _, row := range rowsToAppend {
		require.NoError(t, a.AppendRow(row.stringList, row.intList))
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		var r resultRow
		require.NoError(t, res.Scan(&r.stringList, &r.intList))
		require.Equal(t, rowsToAppend[i].stringList, castList[string](r.stringList))
		require.Equal(t, rowsToAppend[i].intList, castList[int32](r.intList))
		i++
	}

	require.Equal(t, 10, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderNested(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
			id BIGINT,
			string_list VARCHAR[],
			int_list INT[],
			nested_int_list INT[][],
			triple_nested_int_list INT[][][],
			simple_struct STRUCT(A INT, B VARCHAR),
			wrapped_struct STRUCT(N VARCHAR, M STRUCT(A INT, B VARCHAR)),
			double_wrapped_struct STRUCT(
				X VARCHAR,
				Y STRUCT(
					N VARCHAR,
					M STRUCT(
						A INT,
						B VARCHAR
					)
				)
			),
			struct_list STRUCT(A INT, B VARCHAR)[],
			struct_with_list STRUCT(L INT[]),
			mix STRUCT(
				A STRUCT(L VARCHAR[]),
				B STRUCT(L INT[])[]
			),
			mix_list STRUCT(
				A STRUCT(L VARCHAR[]),
				B STRUCT(L INT[])[]
			)[]
		)
	`)

	ms := mixedStruct{
		A: struct {
			L []string
		}{
			[]string{"a", "b", "c"},
		},
		B: []struct {
			L []int32
		}{
			{[]int32{1, 2, 3}},
		},
	}
	rowsToAppend := make([]nestedDataRow, 10)
	for i := 0; i < 10; i++ {
		rowsToAppend[i].ID = int64(i)
		rowsToAppend[i].stringList = []string{"a", "b", "c"}
		rowsToAppend[i].intList = []int32{1, 2, 3}
		rowsToAppend[i].nestedIntList = [][]int32{{1, 2, 3}, {4, 5, 6}}
		rowsToAppend[i].tripleNestedIntList = [][][]int32{
			{{1, 2, 3}, {4, 5, 6}},
			{{7, 8, 9}, {10, 11, 12}},
		}
		rowsToAppend[i].simpleStruct = simpleStruct{A: 1, B: "foo"}
		rowsToAppend[i].wrappedStruct = wrappedStruct{"wrapped", simpleStruct{1, "foo"}}
		rowsToAppend[i].doubleWrappedStruct = doubleWrappedStruct{
			"so much nesting",
			wrappedStruct{
				"wrapped",
				simpleStruct{1, "foo"},
			},
		}
		rowsToAppend[i].structList = []simpleStruct{{1, "a"}, {2, "b"}, {3, "c"}}
		rowsToAppend[i].structWithList.L = []int32{6, 7, 8}
		rowsToAppend[i].mix = ms
		rowsToAppend[i].mixList = []mixedStruct{ms, ms}
	}

	for _, row := range rowsToAppend {
		require.NoError(t, a.AppendRow(
			row.ID,
			row.stringList,
			row.intList,
			row.nestedIntList,
			row.tripleNestedIntList,
			row.simpleStruct,
			row.wrappedStruct,
			row.doubleWrappedStruct,
			row.structList,
			row.structWithList,
			row.mix,
			row.mixList))
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT * FROM test ORDER BY id`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		var r resultRow
		require.NoError(t, res.Scan(
			&r.ID,
			&r.stringList,
			&r.intList,
			&r.nestedIntList,
			&r.tripleNestedIntList,
			&r.simpleStruct,
			&r.wrappedStruct,
			&r.doubleWrappedStruct,
			&r.structList,
			&r.structWithList,
			&r.mix,
			&r.mixList,
		))

		require.Equal(t, rowsToAppend[i].ID, r.ID)
		require.Equal(t, rowsToAppend[i].stringList, castList[string](r.stringList))
		require.Equal(t, rowsToAppend[i].intList, castList[int32](r.intList))

		strRes := fmt.Sprintf("%v", r.nestedIntList)
		require.Equal(t, strRes, "[[1 2 3] [4 5 6]]")
		strRes = fmt.Sprintf("%v", r.tripleNestedIntList)
		require.Equal(t, strRes, "[[[1 2 3] [4 5 6]] [[7 8 9] [10 11 12]]]")

		require.Equal(t, rowsToAppend[i].simpleStruct, castMapToStruct[simpleStruct](t, r.simpleStruct))
		require.Equal(t, rowsToAppend[i].wrappedStruct, castMapToStruct[wrappedStruct](t, r.wrappedStruct))
		require.Equal(t, rowsToAppend[i].doubleWrappedStruct, castMapToStruct[doubleWrappedStruct](t, r.doubleWrappedStruct))

		require.Equal(t, rowsToAppend[i].structList, castMapListToStruct[simpleStruct](t, r.structList))
		require.Equal(t, rowsToAppend[i].structWithList, castMapToStruct[structWithList](t, r.structWithList))
		require.Equal(t, rowsToAppend[i].mix, castMapToStruct[mixedStruct](t, r.mix))
		require.Equal(t, rowsToAppend[i].mixList, castMapListToStruct[mixedStruct](t, r.mixList))

		i++
	}

	require.Equal(t, 10, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderNullList(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (int_slice VARCHAR[][][])`)

	require.NoError(t, a.AppendRow([][][]string{{{}}}))
	require.NoError(t, a.AppendRow([][][]string{{{"1", "2", "3"}, {"4", "5", "6"}}}))
	require.NoError(t, a.AppendRow([][][]string{{{"1"}, nil}}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow([][][]string{nil, {{"2"}}}))
	require.NoError(t, a.AppendRow([][][]string{{nil, {"3"}}, {{"4"}}}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT int_slice FROM test`)
	require.NoError(t, err)

	var strResult []string
	strResult = append(strResult, "[[[]]]")
	strResult = append(strResult, "[[[1 2 3] [4 5 6]]]")
	strResult = append(strResult, "[[[1] <nil>]]")
	strResult = append(strResult, "<nil>")
	strResult = append(strResult, "[<nil> [[2]]]")
	strResult = append(strResult, "[[<nil> [3]] [[4]]]")

	i := 0
	for res.Next() {
		var strS string
		var intS []any
		err = res.Scan(&intS)
		if err != nil {
			strS = "<nil>"
		} else {
			strS = fmt.Sprintf("%v", intS)
		}

		require.Equal(t, strResult[i], strS, fmt.Sprintf("row %d: expected %v, got %v", i, strResult[i], strS))
		i++
	}

	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderNullStruct(t *testing.T) {
	c, con, a := prepareAppender(t, `
	CREATE TABLE test (
		simple_struct STRUCT(A INT, B VARCHAR)
	)`)

	require.NoError(t, a.AppendRow(simpleStruct{1, "hello"}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT simple_struct FROM test`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		var row any
		err = res.Scan(&row)
		if i == 0 {
			require.NoError(t, err)
		} else if i == 1 {
			require.Equal(t, nil, row)
		}
		i++
	}

	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderNestedNullStruct(t *testing.T) {
	c, con, a := prepareAppender(t, `
	CREATE TABLE test (
		double_wrapped_struct STRUCT(
				X VARCHAR,
				Y STRUCT(
					N VARCHAR,
					M STRUCT(
						A INT,
						B VARCHAR
					)
				)
			)
	)`)

	require.NoError(t, a.AppendRow(doubleWrappedStruct{
		"so much nesting",
		wrappedStruct{
			"wrapped",
			simpleStruct{1, "foo"},
		},
	}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow(doubleWrappedStruct{
		"now we are done nesting NULLs",
		wrappedStruct{
			"unwrap",
			simpleStruct{21, "bar"},
		},
	}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT double_wrapped_struct FROM test`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		var row any
		err = res.Scan(&row)
		if i == 1 {
			require.Equal(t, nil, row)
		} else {
			require.NoError(t, err)
		}
		i++
	}

	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderNullIntAndString(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (id BIGINT, str VARCHAR)`)

	require.NoError(t, a.AppendRow(int64(32), "hello"))
	require.NoError(t, a.AppendRow(nil, nil))
	require.NoError(t, a.AppendRow(nil, "half valid thingy"))
	require.NoError(t, a.AppendRow(int64(60), nil))
	require.NoError(t, a.AppendRow(int64(42), "valid again"))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)

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
		} else if i > 0 && i < 4 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, id, 42)
			require.Equal(t, str, "valid again")
		}
		i++
	}

	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderUUID(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (id UUID)`)

	id := UUID(uuid.New())
	require.NoError(t, a.AppendRow(id))
	require.NoError(t, a.Flush())

	// Verify results.
	row := sql.OpenDB(c).QueryRowContext(context.Background(), `SELECT id FROM test`)

	var res UUID
	require.NoError(t, row.Scan(&res))
	require.Equal(t, id, res)
	cleanupAppender(t, c, con, a)
}

func TestAppenderTime(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (timestamp TIMESTAMP)`)

	ts := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, a.AppendRow(ts))
	require.NoError(t, a.Flush())

	// Verify results.
	row := sql.OpenDB(c).QueryRowContext(context.Background(), `SELECT timestamp FROM test`)

	var res time.Time
	require.NoError(t, row.Scan(&res))
	require.Equal(t, ts, res)
	cleanupAppender(t, c, con, a)
}

func TestAppenderBlob(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (data BLOB)`)

	data := []byte{0x01, 0x02, 0x00, 0x03, 0x04}
	require.NoError(t, a.AppendRow(data))

	// Treat []uint8 the same as []byte.
	uint8Slice := []uint8{0x01, 0x02, 0x00, 0x03, 0x04}
	require.NoError(t, a.AppendRow(uint8Slice))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT data FROM test`)
	require.NoError(t, err)

	i := 0
	for res.Next() {
		var b []byte
		require.NoError(t, res.Scan(&b))
		require.Equal(t, data, b)
		i++
	}

	require.Equal(t, 2, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderBlobTinyInt(t *testing.T) {
	c, con, a := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)

	require.NoError(t, a.AppendRow(nil))

	// We treat the byte slice as a list, as that's the type we set when creating the appender.
	require.NoError(t, a.AppendRow([]byte{0x01, 0x02, 0x03, 0x04}))
	require.NoError(t, a.AppendRow([]byte{0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04}))
	require.NoError(t, a.AppendRow([]byte{}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`)
	require.NoError(t, err)

	expected := []string{
		"NULL",
		"[1, 2, 3, 4]",
		"[1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		require.NoError(t, res.Scan(&str))
		require.Equal(t, expected[i], str)
		i++
	}

	require.Equal(t, 4, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderUint8SliceTinyInt(t *testing.T) {
	c, con, a := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)

	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow([]uint8{0x01, 0x00, 0x03, 0x04, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0}))
	require.NoError(t, a.AppendRow([]uint8{}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`)
	require.NoError(t, err)

	expected := []string{
		"NULL",
		"[1, 0, 3, 4, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		require.NoError(t, res.Scan(&str))
		require.Equal(t, expected[i], str)
		i++
	}

	require.Equal(t, 3, i)
	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}

func TestAppenderWithJSON(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
		    id DOUBLE,
			l DOUBLE[],
			s STRUCT(a DOUBLE, b VARCHAR)
	  	)`)

	jsonBytes := []byte(`{"id": 42, "l":[1, 2, 3], "s":{"a":101, "b":"hello"}}`)
	var jsonData map[string]interface{}
	err := json.Unmarshal(jsonBytes, &jsonData)
	require.NoError(t, err)

	require.NoError(t, a.AppendRow(jsonData["id"], jsonData["l"], jsonData["s"]))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := sql.OpenDB(c).QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)

	for res.Next() {
		var (
			id uint64
			l  interface{}
			s  interface{}
		)
		err := res.Scan(&id, &l, &s)
		require.NoError(t, err)
		require.Equal(t, uint64(42), id)
		require.Equal(t, "[1 2 3]", fmt.Sprint(l))
		require.Equal(t, "map[a:101 b:hello]", fmt.Sprint(s))
	}

	require.NoError(t, res.Close())
	cleanupAppender(t, c, con, a)
}
