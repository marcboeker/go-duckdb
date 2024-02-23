package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/google/uuid"
	"math/rand"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
)

const numAppenderTestRows = 10000

type simpleStruct struct {
	A int32
	B string
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

func prepareAppender(t *testing.T, createTbl string) (*Connector, driver.Conn, *Appender) {
	connector, err := NewConnector("", nil)
	require.NoError(t, err)

	// Create the table that we'll append to.
	db := sql.OpenDB(connector)
	_, err = db.Exec(createTbl)
	require.NoError(t, err)

	con, err := connector.Connect(context.Background())
	require.NoError(t, err)

	appender, err := NewAppenderFromConn(con, "", "test")
	require.NoError(t, err)

	return connector, con, appender
}

func TestAppenderPrimitive(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
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
			float REAL,
			double DOUBLE,
			string VARCHAR,
			bool BOOLEAN
	  	)`)
	defer con.Close()
	defer connector.Close()

	type dataRow struct {
		ID        int64
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

	rows := make([]dataRow, numAppenderTestRows)
	for i := 0; i < numAppenderTestRows; i++ {
		u64 := rand.Uint64()
		// go sql doesn't support uint64 values with high bit set (see for example https://github.com/lib/pq/issues/72)
		if u64 > 9223372036854775807 {
			u64 = 9223372036854775807
		}
		rows[i] = dataRow{
			ID:        int64(i),
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
			Bool:      rand.Int()%2 == 0,
		}

		err := appender.AppendRow(
			rows[i].ID,
			rows[i].UInt8,
			rows[i].Int8,
			rows[i].UInt16,
			rows[i].Int16,
			rows[i].UInt32,
			rows[i].Int32,
			rows[i].UInt64,
			rows[i].Int64,
			rows[i].Timestamp,
			rows[i].Float,
			rows[i].Double,
			rows[i].String,
			rows[i].Bool,
		)
		require.NoError(t, err)

		if i%1000 == 0 {
			err := appender.Flush()
			require.NoError(t, err)
		}
	}

	err := appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(), `
			SELECT * FROM test ORDER BY id`,
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

	require.Equal(t, numAppenderTestRows, i)
}

func TestAppenderList(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
	CREATE TABLE test (
		string_list VARCHAR[],
		int_list INTEGER[]
	)`)
	defer con.Close()
	defer connector.Close()

	rows := make([]nestedDataRow, 10)
	for i := 0; i < 10; i++ {
		rows[i].stringList = []string{"a", "b", "c"}
		rows[i].intList = []int32{1, 2, 3}
	}

	for _, row := range rows {
		err := appender.AppendRow(
			row.stringList,
			row.intList,
		)
		require.NoError(t, err)
	}
	err := appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test`,
	)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var r resultRow
		err := res.Scan(
			&r.stringList,
			&r.intList,
		)
		require.NoError(t, err)

		require.Equal(t, rows[i].stringList, castList[string](r.stringList))
		require.Equal(t, rows[i].intList, castList[int32](r.intList))

		i++
	}

	require.Equal(t, 10, i)
}

func TestAppenderNested(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
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
	defer con.Close()
	defer connector.Close()

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

	rows := make([]nestedDataRow, 10)
	for i := 0; i < 10; i++ {
		rows[i].ID = int64(i)
		rows[i].stringList = []string{"a", "b", "c"}
		rows[i].intList = []int32{1, 2, 3}
		rows[i].nestedIntList = [][]int32{{1, 2, 3}, {4, 5, 6}}
		rows[i].tripleNestedIntList = [][][]int32{
			{{1, 2, 3}, {4, 5, 6}},
			{{7, 8, 9}, {10, 11, 12}},
		}
		rows[i].simpleStruct = simpleStruct{A: 1, B: "foo"}
		rows[i].wrappedStruct = wrappedStruct{"wrapped", simpleStruct{1, "foo"}}
		rows[i].doubleWrappedStruct = doubleWrappedStruct{
			"so much nesting",
			wrappedStruct{"wrapped",
				simpleStruct{1, "foo"}},
		}
		rows[i].structList = []simpleStruct{{1, "a"}, {2, "b"}, {3, "c"}}
		rows[i].structWithList.L = []int32{6, 7, 8}
		rows[i].mix = ms
		rows[i].mixList = []mixedStruct{ms, ms}
	}

	for _, row := range rows {
		err := appender.AppendRow(
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
			row.mixList,
		)
		require.NoError(t, err)
	}
	err := appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test ORDER BY id`,
	)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var r resultRow
		err := res.Scan(
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
		)
		require.NoError(t, err)

		require.Equal(t, rows[i].ID, r.ID)
		require.Equal(t, rows[i].stringList, castList[string](r.stringList))
		require.Equal(t, rows[i].intList, castList[int32](r.intList))

		strRes := fmt.Sprintf("%v", r.nestedIntList)
		require.Equal(t, strRes, "[[1 2 3] [4 5 6]]")
		strRes = fmt.Sprintf("%v", r.tripleNestedIntList)
		require.Equal(t, strRes, "[[[1 2 3] [4 5 6]] [[7 8 9] [10 11 12]]]")

		require.Equal(t, rows[i].simpleStruct, castMapToStruct[simpleStruct](t, r.simpleStruct))
		require.Equal(t, rows[i].wrappedStruct, castMapToStruct[wrappedStruct](t, r.wrappedStruct))
		require.Equal(t, rows[i].doubleWrappedStruct, castMapToStruct[doubleWrappedStruct](t, r.doubleWrappedStruct))

		require.Equal(t, rows[i].structList, castMapListToStruct[simpleStruct](t, r.structList))
		require.Equal(t, rows[i].structWithList, castMapToStruct[structWithList](t, r.structWithList))
		require.Equal(t, rows[i].mix, castMapToStruct[mixedStruct](t, r.mix))
		require.Equal(t, rows[i].mixList, castMapListToStruct[mixedStruct](t, r.mixList))

		i++
	}

	require.Equal(t, 10, i)
}

func TestAppenderNullList(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (int_slice VARCHAR[][][])`)
	defer con.Close()
	defer connector.Close()

	// An empty list must also initialize the logical types.
	err := appender.AppendRow([][][]string{{{}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]string{{{"1", "2", "3"}, {"4", "5", "6"}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]string{{{"1"}, nil}})
	require.NoError(t, err)

	err = appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.AppendRow([][][]string{nil, {{"2"}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]string{{nil, {"3"}}, {{"4"}}})
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT int_slice FROM test`)
	require.NoError(t, err)
	defer res.Close()

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
		err := res.Scan(
			&intS,
		)
		if err != nil {
			strS = "<nil>"
		} else {
			strS = fmt.Sprintf("%v", intS)
		}

		require.Equal(t, strResult[i], strS, fmt.Sprintf("row %d: expected %v, got %v", i, strResult[i], strS))
		i++
	}
}

func TestAppenderNullStruct(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
	CREATE TABLE test (
		simple_struct STRUCT(A INT, B VARCHAR)
	)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(simpleStruct{1, "hello"})
	require.NoError(t, err)

	err = appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT simple_struct FROM test`)
	require.NoError(t, err)
	defer res.Close()

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
}

func TestAppenderNestedNullStruct(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
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
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(doubleWrappedStruct{
		"so much nesting",
		wrappedStruct{"wrapped",
			simpleStruct{1, "foo"}},
	})
	require.NoError(t, err)

	// We propagate the NULL to all nested children.
	err = appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.AppendRow(doubleWrappedStruct{
		"now we are done nesting NULLs",
		wrappedStruct{"unwrap",
			simpleStruct{21, "bar"}},
	})
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT double_wrapped_struct FROM test`)
	require.NoError(t, err)
	defer res.Close()

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
}

func TestAppenderNullIntAndString(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (id BIGINT, str VARCHAR)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(int64(32), "hello")
	require.NoError(t, err)

	err = appender.AppendRow(nil, nil)
	require.NoError(t, err)

	err = appender.AppendRow(nil, "half valid thingy")
	require.NoError(t, err)

	err = appender.AppendRow(int64(60), nil)
	require.NoError(t, err)

	err = appender.AppendRow(int64(42), "valid again")
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test`,
	)
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
		} else if i > 0 && i < 4 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, id, 42)
			require.Equal(t, str, "valid again")
		}

		i++
	}
}

func TestAppenderNestedListMismatch(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test(int_slice INT[][][])`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow([][][]int32{{{}}})
	require.NoError(t, err)

	err = appender.AppendRow([]int32{1, 2, 3})
	require.Error(t, err, "expected: [][][]int32, actual: []int32")

	err = appender.AppendRow(1)
	require.ErrorContains(t, err, "expected: [][][]int32, actual: int64")

	err = appender.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	require.ErrorContains(t, err, "expected: [][][]int32, actual: [][]int32")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderListMismatch(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test(intSlice INT[])`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow([]int32{})
	require.NoError(t, err)

	err = appender.AppendRow([]string{"foo", "bar", "baz"})
	require.ErrorContains(t, err, "expected: []int32, actual: []string")

	err = appender.AppendRow(
		[][]int32{{1, 2, 3}, {4, 5, 6}},
	)
	require.ErrorContains(t, err, "expected: []int32, actual: [][]int32")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderStructMismatch(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
		CREATE TABLE test (
			simple_struct STRUCT(A INT, B VARCHAR)
		)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(simpleStruct{1, "hello"})
	require.NoError(t, err)

	err = appender.AppendRow(1)
	require.ErrorContains(t, err, "expected: {int32, string}, actual: int64")

	err = appender.AppendRow("hello")
	require.ErrorContains(t, err, "expected: {int32, string}, actual: string")

	type other struct {
		S string
		I int
	}
	err = appender.AppendRow(other{"hello", 1})
	require.ErrorContains(t, err, "expected: {int32, string}, actual: {string, int64}")

	err = appender.AppendRow(
		wrappedStruct{
			"hello there",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	require.ErrorContains(t, err, "expected: {int32, string}, actual: {string, {int32, string}}")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderWrappedStructMismatch(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
		CREATE TABLE test (
			wrapped_struct STRUCT(N VARCHAR, M STRUCT(A INT, B VARCHAR)),
		)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(
		wrappedStruct{
			"it's me again",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	require.NoError(t, err)

	err = appender.AppendRow(simpleStruct{1, "hello"})
	require.ErrorContains(t, err, "expected: {string, {int32, string}}, actual: {int32, string}")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderMismatchStructWithList(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (struct_with_list STRUCT(L INT[]))`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(structWithList{L: []int32{1, 2, 3}})
	require.NoError(t, err)

	err = appender.AppendRow([]int32{1, 2, 3})
	require.ErrorContains(t, err, "expected: {[]int32}, actual: []int32")

	l := struct{ L []string }{L: []string{"a", "b", "c"}}
	err = appender.AppendRow(l)
	require.ErrorContains(t, err, "expected: {[]int32}, actual: {[]string}")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderMismatchStruct(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
		CREATE TABLE test (
			mix STRUCT(A STRUCT(L VARCHAR[]), B STRUCT(L INT[])[])
		)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(
		mixedStruct{
			A: struct {
				L []string
			}{
				L: []string{"a", "b", "c"},
			},
			B: []struct {
				L []int32
			}{
				{[]int32{1, 2, 3}},
			},
		},
	)
	require.NoError(t, err)

	err = appender.AppendRow(simpleStruct{1, "hello"})
	require.ErrorContains(t, err, "expected: {{[]string}, []{[]int32}}, actual: {int32, string}")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderMismatch(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (id BIGINT, str VARCHAR)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow("hello")
	require.ErrorContains(t, err, "type mismatch")

	err = appender.AppendRow(false)
	require.ErrorContains(t, err, "type mismatch")

	err = appender.Close()
	require.ErrorContains(t, err, "duckdb has returned an error while appending, all data has been invalidated.")
}

func TestAppenderUUID(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (id UUID)`)
	defer con.Close()
	defer connector.Close()

	id := UUID(uuid.New())
	err := appender.AppendRow(id)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	row := db.QueryRowContext(context.Background(), `SELECT id FROM test`)

	var res UUID
	err = row.Scan(&res)
	require.NoError(t, err)
	require.Equal(t, id, res)
}

func TestAppenderTime(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (timestamp TIMESTAMP)`)
	defer con.Close()
	defer connector.Close()

	ts := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)
	err := appender.AppendRow(ts)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	row := db.QueryRowContext(context.Background(), `SELECT timestamp FROM test`)

	var res time.Time
	err = row.Scan(&res)
	require.NoError(t, err)
	require.Equal(t, ts, res)
}

func TestAppenderBlob(t *testing.T) {
	connector, con, appender := prepareAppender(t, `CREATE TABLE test (data BLOB)`)
	defer con.Close()
	defer connector.Close()

	data := []byte{0x01, 0x02, 0x00, 0x03, 0x04}
	err := appender.AppendRow(data)
	require.NoError(t, err)

	// Treat []uint8 the same as []byte.
	uint8Slice := []uint8{0x01, 0x02, 0x00, 0x03, 0x04}
	err = appender.AppendRow(uint8Slice)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT data FROM test`,
	)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var b []byte
		err = res.Scan(
			&b,
		)
		require.NoError(t, err)
		require.Equal(t, data, b)
		i++
	}
	require.Equal(t, 2, i)
}

func TestAppenderBlobTinyInt(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(nil)
	require.NoError(t, err)

	// We treat the byte slice as a list, as that's the type we set when creating the appender.
	err = appender.AppendRow([]byte{0x01, 0x02, 0x03, 0x04})
	require.NoError(t, err)

	err = appender.AppendRow([]byte{0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04})
	require.NoError(t, err)

	err = appender.AppendRow([]byte{})
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`,
	)
	require.NoError(t, err)
	defer res.Close()

	expected := []string{
		"NULL",
		"[1, 2, 3, 4]",
		"[1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		err = res.Scan(
			&str,
		)
		require.NoError(t, err)
		require.Equal(t, expected[i], str)
		i++
	}
	require.Equal(t, 4, i)
}

func TestAppenderUint8SliceTinyInt(t *testing.T) {
	connector, con, appender := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)
	defer con.Close()
	defer connector.Close()

	err := appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.AppendRow([]uint8{0x01, 0x00, 0x03, 0x04, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0})
	require.NoError(t, err)

	err = appender.AppendRow([]uint8{})
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	// Verify results.
	db := sql.OpenDB(connector)
	res, err := db.QueryContext(
		context.Background(),
		`SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`,
	)
	require.NoError(t, err)
	defer res.Close()

	expected := []string{
		"NULL",
		"[1, 0, 3, 4, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		err = res.Scan(
			&str,
		)
		require.NoError(t, err)
		require.Equal(t, expected[i], str)
		i++
	}
	require.Equal(t, 3, i)
}

func TestAppenderUnsupportedType(t *testing.T) {

	connector, err := NewConnector("", nil)
	require.NoError(t, err)

	// Create the table that we'll append to.
	db := sql.OpenDB(connector)
	_, err = db.Exec(`CREATE TABLE test AS SELECT MAP() AS m`)
	require.NoError(t, err)

	con, err := connector.Connect(context.Background())
	require.NoError(t, err)

	_, err = NewAppenderFromConn(con, "", "test")
	require.ErrorContains(t, err, "does not support the column type of column 1: MAP")

	con.Close()
	connector.Close()
}
