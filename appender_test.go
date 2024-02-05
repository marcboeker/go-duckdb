package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
)

const numAppenderTestRows = 100

type simpleStruct struct {
	A int32
	B string
}

type wrappedStruct struct {
	A simpleStruct
}

type doubleWrappedStruct struct {
	A wrappedStruct
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
	ID                  int
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
	ID                  int
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

func castMapListToStruct[T any](val []any) []T {
	res := make([]T, len(val))
	for i, v := range val {
		mapstructure.Decode(v, &res[i])
	}
	return res
}

func castMapToStruct[T any](val any) T {
	var res T
	mapstructure.Decode(val, &res)
	return res
}

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
	defer db.Close()

	_, err = db.Exec(`
  CREATE TABLE test (
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
  )`)
	require.NoError(t, err)

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
			Bool:      rand.Int()%2 == 0,
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
				bool
      FROM test
      ORDER BY id`,
	)
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
		)
		require.NoError(t, err)
		copy(r.UUID[:], scannedUUID)
		require.Equal(t, rows[i], r)
		i++
	}

	require.Equal(t, i, numAppenderTestRows)
}

func TestAppenderNested(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE test (
		id BIGINT,
		string_list VARCHAR[],
		int_list INT[],
		nested_int_list INT[][],
		triple_nested_int_list INT[][][],
		simple_struct STRUCT(A INT, B VARCHAR),
		wrapped_struct STRUCT(A STRUCT(A INT, B VARCHAR)),
		double_wrapped_struct STRUCT(
			A STRUCT(
				A STRUCT(
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
	)`)
	require.NoError(t, err)

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
		rows[i].ID = i
		rows[i].stringList = []string{"a", "b", "c"}
		rows[i].intList = []int32{1, 2, 3}
		rows[i].nestedIntList = [][]int32{{1, 2, 3}, {4, 5, 6}}
		rows[i].tripleNestedIntList = [][][]int32{
			{{1, 2, 3}, {4, 5, 6}},
			{{7, 8, 9}, {10, 11, 12}},
		}
		rows[i].simpleStruct = simpleStruct{A: 1, B: "foo"}
		rows[i].wrappedStruct = wrappedStruct{simpleStruct{1, "foo"}}
		rows[i].doubleWrappedStruct = doubleWrappedStruct{
			wrappedStruct{simpleStruct{1, "foo"}},
		}
		rows[i].structList = []simpleStruct{{1, "a"}, {2, "b"}, {3, "c"}}
		rows[i].structWithList.L = []int32{6, 7, 8}
		rows[i].mix = ms
		rows[i].mixList = []mixedStruct{ms, ms}
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
	err = appender.Flush()
	require.NoError(t, err)

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
		// TODO: check nested lists

		require.Equal(t, rows[i].simpleStruct, castMapToStruct[simpleStruct](r.simpleStruct))
		require.Equal(t, rows[i].wrappedStruct, castMapToStruct[wrappedStruct](r.wrappedStruct))
		require.Equal(t, rows[i].doubleWrappedStruct, castMapToStruct[doubleWrappedStruct](r.doubleWrappedStruct))

		require.Equal(t, rows[i].structList, castMapListToStruct[simpleStruct](r.structList))
		require.Equal(t, rows[i].structWithList, castMapToStruct[structWithList](r.structWithList))
		require.Equal(t, rows[i].mix, castMapToStruct[mixedStruct](r.mix))
		require.Equal(t, rows[i].mixList, castMapListToStruct[mixedStruct](r.mixList))

		i++
	}

	require.Equal(t, 10, i)
}

func TestAppenderNullList(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.Exec(`CREATE OR REPLACE TABLE test(int_slice INT[][][])`)
	require.NoError(t, err)

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	// An empty list should also be able to initialize the logical types
	err = appender.AppendRow([][][]int32{{{}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]int32{{{1, 2, 3}, {4, 5, 6}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]int32{{{1}, nil}})
	require.NoError(t, err)

	err = appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.AppendRow([][][]int32{nil, {{2}}})
	require.NoError(t, err)

	err = appender.AppendRow([][][]int32{{nil, {3}}, {{4}}})
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

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

		if strResult[i] != strS {
			panic(fmt.Sprintf("row %d: expected %v, got %v", i, strResult[i], strS))
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
			simple_struct STRUCT(A INT, B VARCHAR),
    )`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(simpleStruct{1, "hello"})
	require.NoError(t, err)

	err = appender.AppendRow(nil)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT simple_struct FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var row any
		err := res.Scan(&row)
		if i == 0 {
			require.NoError(t, err)
		} else if i == 1 {
			require.Equal(t, nil, row)
		}

		i++
	}
}

func TestAppenderNestedListMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
			int_slice INT[][][],
    )`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow([][][]int32{{{}}})
	require.NoError(t, err)

	err = appender.AppendRow([]int32{1, 2, 3})
	require.Error(t, err, "expected: \"int32[][][]\" \nactual: \"int32[]\"")

	err = appender.AppendRow(1)
	require.ErrorContains(t, err, "expected: \"int32[][][]\" \nactual: \"int64\"")

	err = appender.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	require.ErrorContains(t, err, "expected: \"int32[][][]\" \nactual: \"int32[][]\"")

	err = appender.Close()
	require.NoError(t, err)

	// test incorrect nested type insert (double nested into single nested)
	_, err = db.Exec(`
		CREATE TABLE test2(
		  intSlice INT[]
  	)`)
	require.NoError(t, err)
	defer db.Close()

	conn, err = c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err = NewAppenderFromConn(conn, "", "test2")
	require.NoError(t, err)

	err = appender.AppendRow([]int32{})
	require.NoError(t, err)

	err = appender.AppendRow([]string{"foo", "bar", "baz"})
	require.ErrorContains(t, err, "expected: \"int32[]\" \nactual: \"string[]\"")

	err = appender.AppendRow(
		[][]int32{{1, 2, 3}, {4, 5, 6}},
	)
	require.ErrorContains(t, err, "expected: \"int32[]\" \nactual: \"int32[][]\"")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderNestedStructMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test (
			simple_struct STRUCT(A INT, B VARCHAR),
		)`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(simpleStruct{1, "hello"})
	require.NoError(t, err)

	err = appender.AppendRow(1)
	require.ErrorContains(t, err, "expected: \"{int32, string}\" \nactual: \"int64\"")

	err = appender.AppendRow("hello")
	require.ErrorContains(t, err, "expected: \"{int32, string}\" \nactual: \"string\"")

	type other struct {
		S string
		I int
	}
	err = appender.AppendRow(other{"hello", 1})
	require.ErrorContains(t, err, "expected: \"{int32, string}\" \nactual: \"{string, int64}\"")

	err = appender.AppendRow(
		wrappedStruct{
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	require.ErrorContains(t, err, "expected: \"{int32, string}\" \nactual: \"{{int32, string}}\"")

	err = appender.Close()
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE test2 (
			wrapped_struct STRUCT(A STRUCT(A INT, B VARCHAR)),
		)`)
	require.NoError(t, err)
	defer db.Close()

	conn, err = c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err = NewAppenderFromConn(conn, "", "test2")
	require.NoError(t, err)

	err = appender.AppendRow(
		wrappedStruct{
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	require.NoError(t, err)

	err = appender.AppendRow(simpleStruct{1, "hello"})
	require.ErrorContains(t, err, "expected: \"{{int32, string}}\" \nactual: \"{int32, string}\"")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderNestedMixedMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test (
			struct_with_list STRUCT(L INT[]),
		)`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(structWithList{L: []int32{1, 2, 3}})
	require.NoError(t, err)

	err = appender.AppendRow([]int32{1, 2, 3})
	require.ErrorContains(t, err, "expected: \"{int32[]}\" \nactual: \"int32[]\"")

	l := struct{ L []string }{L: []string{"a", "b", "c"}}
	err = appender.AppendRow(l)
	require.ErrorContains(t, err, "expected: \"{int32[]}\" \nactual: \"{string[]}\"")

	err = appender.Close()
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE test2 (
			mix STRUCT(A STRUCT(L VARCHAR[]), B STRUCT(L INT[])[]),
		)`)
	require.NoError(t, err)
	defer db.Close()

	conn, err = c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err = NewAppenderFromConn(conn, "", "test2")
	require.NoError(t, err)

	err = appender.AppendRow(
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
	require.ErrorContains(t, err, "expected: \"{{string[]}, {int32[]}[]}\" \nactual: \"{int32, string}\"")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderNullIntAndString(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE test (
			ID BIGINT,
			str VARCHAR
		)`)
	require.NoError(t, err)

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(32, "hello")
	require.NoError(t, err)

	err = appender.AppendRow(nil, nil)
	require.NoError(t, err)

	err = appender.Close()
	require.NoError(t, err)

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
		} else {
			require.Error(t, err)
		}

		i++
	}

}

func TestAppenderMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE test (
			ID BIGINT,
    )`)
	require.NoError(t, err)

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow("hello")
	require.NoError(t, err)

	err = appender.AppendRow(false)
	require.ErrorContains(t, err, "Type mismatch")

	err = appender.Close()
	require.ErrorContains(t, err, "Duckdb has returned an error while appending, all data has been invalidated.")
}
