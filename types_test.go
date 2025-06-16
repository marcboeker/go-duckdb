package duckdb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// First, this test inserts all types (except UUID and DECIMAL) with the Appender.
// Then, it tests scanning these types.

type testTypesEnum string

const testTypesEnumSQL = `CREATE TYPE my_enum AS ENUM ('0', '1', '2')`

type testTypesStruct struct {
	A int32
	B string
}

type testTypesRow struct {
	Boolean_col      bool
	Tinyint_col      int8
	Smallint_col     int16
	Integer_col      int32
	Bigint_col       int64
	Utinyint_col     uint8
	Usmallint_col    uint16
	Uinteger_col     uint32
	Ubigint_col      uint64
	Float_col        float32
	Double_col       float64
	Timestamp_col    time.Time
	Date_col         time.Time
	Time_col         time.Time
	Interval_col     Interval
	Hugeint_col      *big.Int
	Varchar_col      string
	Blob_col         []byte
	Timestamp_s_col  time.Time
	Timestamp_ms_col time.Time
	Timestamp_ns_col time.Time
	Enum_col         testTypesEnum
	List_col         Composite[[]int32]
	Struct_col       Composite[testTypesStruct]
	Map_col          Map
	Array_col        Composite[[3]int32]
	Time_tz_col      time.Time
	Timestamp_tz_col time.Time
	Json_col_map     Composite[map[string]any]
	Json_col_array   Composite[[]any]
	Json_col_string  string
	Json_col_bool    bool
	Json_col_float64 float64
}

const testTypesTableSQL = `CREATE TABLE test (
	Boolean_col BOOLEAN,
	Tinyint_col TINYINT,
	Smallint_col SMALLINT,
	Integer_col INTEGER,
	Bigint_col BIGINT,
	Utinyint_col UTINYINT,
	Usmallint_col USMALLINT,
	Uinteger_col UINTEGER,
	Ubigint_col UBIGINT,
	Float_col FLOAT,
	Double_col DOUBLE,
	Timestamp_col TIMESTAMP,
	Date_col DATE,
	Time_col TIME,
	Interval_col INTERVAL,
	Hugeint_col HUGEINT,
	Varchar_col VARCHAR,
	Blob_col BLOB,
	Timestamp_s_col TIMESTAMP_S,
	Timestamp_ms_col TIMESTAMP_MS,
	Timestamp_ns_col TIMESTAMP_NS,
	Enum_col my_enum,
	List_col INTEGER[],
	Struct_col STRUCT(A INTEGER, B VARCHAR),
	Map_col MAP(INTEGER, VARCHAR),
	Array_col INTEGER[3],
	Time_tz_col TIMETZ,
	Timestamp_tz_col TIMESTAMPTZ,
	Json_col_map JSON,
	Json_col_array JSON,
	Json_col_string JSON,
	Json_col_bool JSON,
	Json_col_float64 JSON
)`

func (r *testTypesRow) toUTC() {
	r.Timestamp_col = r.Timestamp_col.UTC()
	r.Timestamp_s_col = r.Timestamp_s_col.UTC()
	r.Timestamp_ms_col = r.Timestamp_ms_col.UTC()
	r.Timestamp_ns_col = r.Timestamp_ns_col.UTC()
	r.Time_tz_col = r.Time_tz_col.UTC()
	r.Timestamp_tz_col = r.Timestamp_tz_col.UTC()
}

func testTypesGenerateRow[T require.TestingT](t T, i int) testTypesRow {
	// Get the timestamp for all TS columns.
	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"
	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", IST)
	require.NoError(t, err)

	// Get the DATE, TIME, and TIMETZ column values.
	dateUTC := time.Date(1992, time.September, 20, 0, 0, 0, 0, time.UTC)
	timeUTC := time.Date(1, time.January, 1, 11, 42, 7, 0, time.UTC)
	timeTZ := time.Date(1, time.January, 1, 11, 42, 7, 0, IST)

	var buffer bytes.Buffer
	for j := 0; j < i; j++ {
		buffer.WriteString("hello!")
	}
	varcharCol := buffer.String()

	listCol := Composite[[]int32]{
		[]int32{int32(i)},
	}
	structCol := Composite[testTypesStruct]{
		testTypesStruct{int32(i), "a" + strconv.Itoa(i)},
	}
	mapCol := Map{
		int32(i): "other_longer_val",
	}
	arrayCol := Composite[[3]int32]{
		[3]int32{int32(i), int32(i), int32(i)},
	}
	jsonMapCol := Composite[map[string]any]{
		map[string]any{
			"hello": float64(42),
			"world": float64(84),
		},
	}
	jsonArrayCol := Composite[[]any]{
		[]any{"hello", "world"},
	}

	return testTypesRow{
		i%2 == 1,
		int8(i % 127),
		int16(i % 32767),
		int32(2147483647 - i),
		int64(9223372036854775807 - i),
		uint8(i % 256),
		uint16(i % 65535),
		uint32(2147483647 - i),
		uint64(9223372036854775807 - i),
		float32(i),
		float64(i),
		ts,
		dateUTC,
		timeUTC,
		Interval{Days: 0, Months: int32(i), Micros: 0},
		big.NewInt(int64(i)),
		varcharCol,
		[]byte{'A', 'B'},
		ts,
		ts,
		ts,
		testTypesEnum(strconv.Itoa(i % 3)),
		listCol,
		structCol,
		mapCol,
		arrayCol,
		timeTZ,
		ts,
		jsonMapCol,
		jsonArrayCol,
		varcharCol,
		i%2 == 1,
		float64(i),
	}
}

func testTypesGenerateRows[T require.TestingT](t T, rowCount int) []testTypesRow {
	var expectedRows []testTypesRow
	for i := 0; i < rowCount; i++ {
		r := testTypesGenerateRow(t, i)
		expectedRows = append(expectedRows, r)
	}
	return expectedRows
}

func testTypesReset[T require.TestingT](t T, c *Connector) {
	_, err := sql.OpenDB(c).ExecContext(context.Background(), `DELETE FROM test`)
	require.NoError(t, err)
}

func testTypes[T require.TestingT](t T, db *sql.DB, a *Appender, expectedRows []testTypesRow) []testTypesRow {
	// Append the rows. We cannot append Composite types.
	for i := 0; i < len(expectedRows); i++ {
		r := &expectedRows[i]
		err := a.AppendRow(
			r.Boolean_col,
			r.Tinyint_col,
			r.Smallint_col,
			r.Integer_col,
			r.Bigint_col,
			r.Utinyint_col,
			r.Usmallint_col,
			r.Uinteger_col,
			r.Ubigint_col,
			r.Float_col,
			r.Double_col,
			r.Timestamp_col,
			r.Date_col,
			r.Time_col,
			r.Interval_col,
			r.Hugeint_col,
			r.Varchar_col,
			r.Blob_col,
			r.Timestamp_s_col,
			r.Timestamp_ms_col,
			r.Timestamp_ns_col,
			string(r.Enum_col),
			r.List_col.Get(),
			r.Struct_col.Get(),
			r.Map_col,
			r.Array_col.Get(),
			r.Time_tz_col,
			r.Timestamp_tz_col,
			r.Json_col_map.Get(),
			r.Json_col_array.Get(),
			r.Json_col_string,
			r.Json_col_bool,
			r.Json_col_float64)
		require.NoError(t, err)
	}
	require.NoError(t, a.Flush())

	res, err := db.QueryContext(context.Background(), `SELECT * FROM test ORDER BY Smallint_col`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	// Scan the rows.
	var actualRows []testTypesRow
	for res.Next() {
		var r testTypesRow
		err = res.Scan(
			&r.Boolean_col,
			&r.Tinyint_col,
			&r.Smallint_col,
			&r.Integer_col,
			&r.Bigint_col,
			&r.Utinyint_col,
			&r.Usmallint_col,
			&r.Uinteger_col,
			&r.Ubigint_col,
			&r.Float_col,
			&r.Double_col,
			&r.Timestamp_col,
			&r.Date_col,
			&r.Time_col,
			&r.Interval_col,
			&r.Hugeint_col,
			&r.Varchar_col,
			&r.Blob_col,
			&r.Timestamp_s_col,
			&r.Timestamp_ms_col,
			&r.Timestamp_ns_col,
			&r.Enum_col,
			&r.List_col,
			&r.Struct_col,
			&r.Map_col,
			&r.Array_col,
			&r.Time_tz_col,
			&r.Timestamp_tz_col,
			&r.Json_col_map,
			&r.Json_col_array,
			&r.Json_col_string,
			&r.Json_col_bool,
			&r.Json_col_float64)
		require.NoError(t, err)
		actualRows = append(actualRows, r)
	}

	require.NoError(t, err)
	require.Equal(t, len(expectedRows), len(actualRows))
	return actualRows
}

func TestTypes(t *testing.T) {
	expectedRows := testTypesGenerateRows(t, 3)
	c, db, conn, a := prepareAppender(t, testTypesEnumSQL+";"+testTypesTableSQL)
	defer cleanupAppender(t, c, db, conn, a)
	actualRows := testTypes(t, db, a, expectedRows)

	for i := range actualRows {
		expectedRows[i].toUTC()
		require.Equal(t, expectedRows[i], actualRows[i])
	}
	require.Equal(t, len(expectedRows), len(actualRows))
}

// NOTE: go-duckdb only contains very few benchmarks. The purpose of those benchmarks is to avoid regressions
// of its main functionalities. I.e., functions related to implementing the database/sql interface.
var benchmarkTypesResult []testTypesRow

func BenchmarkTypes(b *testing.B) {
	expectedRows := testTypesGenerateRows(b, GetDataChunkCapacity()*3+10)
	c, db, conn, a := prepareAppender(b, testTypesEnumSQL+";"+testTypesTableSQL)
	defer cleanupAppender(b, c, db, conn, a)

	var r []testTypesRow
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		r = testTypes(b, db, a, expectedRows)
		testTypesReset(b, c)
	}
	b.StopTimer()

	// Ensure that the compiler does not eliminate the line by storing the result.
	benchmarkTypesResult = r
}

func compareDecimal(t *testing.T, want Decimal, got Decimal) {
	require.Equal(t, want.Scale, got.Scale)
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, want.Value.String(), got.Value.String())
}

func TestDecimal(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT all possible DECIMAL widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			r := db.QueryRow(fmt.Sprintf(`SELECT 0::DECIMAL(%d, 1)`, i))
			var actual Decimal
			require.NoError(t, r.Scan(&actual))
			expected := Decimal{Width: uint8(i), Value: big.NewInt(0), Scale: 1}
			require.Equal(t, expected, actual)
		}
	})

	t.Run("SELECT different DECIMAL types", func(t *testing.T) {
		res, err := db.Query(`SELECT * FROM (VALUES
			(1.23::DECIMAL(3, 2)),
			(-1.23::DECIMAL(3, 2)),
			(123.45::DECIMAL(5, 2)),
			(-123.45::DECIMAL(5, 2)),
			(123456789.01::DECIMAL(11, 2)),
			(-123456789.01::DECIMAL(11, 2)),
			(1234567890123456789.234::DECIMAL(22, 3)),
			(-1234567890123456789.234::DECIMAL(22, 3)),
		) v
		ORDER BY v ASC`)
		require.NoError(t, err)
		defer closeRowsWrapper(t, res)

		bigNumber, success := new(big.Int).SetString("1234567890123456789234", 10)
		require.True(t, success)
		bigNegativeNumber, success := new(big.Int).SetString("-1234567890123456789234", 10)
		require.True(t, success)
		tests := []struct {
			input string
			want  Decimal
		}{
			{input: "1.23::DECIMAL(3, 2)", want: Decimal{Value: big.NewInt(123), Width: 3, Scale: 2}},
			{input: "-1.23::DECIMAL(3, 2)", want: Decimal{Value: big.NewInt(-123), Width: 3, Scale: 2}},
			{input: "123.45::DECIMAL(5, 2)", want: Decimal{Value: big.NewInt(12345), Width: 5, Scale: 2}},
			{input: "-123.45::DECIMAL(5, 2)", want: Decimal{Value: big.NewInt(-12345), Width: 5, Scale: 2}},
			{input: "123456789.01::DECIMAL(11, 2)", want: Decimal{Value: big.NewInt(12345678901), Width: 11, Scale: 2}},
			{input: "-123456789.01::DECIMAL(11, 2)", want: Decimal{Value: big.NewInt(-12345678901), Width: 11, Scale: 2}},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: Decimal{Value: bigNumber, Width: 22, Scale: 3}},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: Decimal{Value: bigNegativeNumber, Width: 22, Scale: 3}},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf(`SELECT %s`, test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			compareDecimal(t, test.want, fs)
		}
	})

	t.Run("SELECT a huge DECIMAL ", func(t *testing.T) {
		bigInt, success := new(big.Int).SetString("12345678901234567890123456789", 10)
		require.True(t, success)
		var f Decimal
		require.NoError(t, db.QueryRow("SELECT 123456789.01234567890123456789::DECIMAL(29, 20)").Scan(&f))
		compareDecimal(t, Decimal{Value: bigInt, Width: 29, Scale: 20}, f)
	})

	t.Run("SELECT DECIMAL types and compare them to FLOAT64", func(t *testing.T) {
		tests := []struct {
			input string
			want  float64
		}{
			{input: "1.23::DECIMAL(3, 2)", want: 1.23},
			{input: "-1.23::DECIMAL(3, 2)", want: -1.23},
			{input: "123.45::DECIMAL(5, 2)", want: 123.45},
			{input: "-123.45::DECIMAL(5, 2)", want: -123.45},
			{input: "123456789.01::DECIMAL(11, 2)", want: 123456789.01},
			{input: "-123456789.01::DECIMAL(11, 2)", want: -123456789.01},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: 1234567890123456789.234},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: -1234567890123456789.234},
			{input: "123456789.01234567890123456789::DECIMAL(29, 20)", want: 123456789.01234567890123456789},
			{input: "-123456789.01234567890123456789::DECIMAL(29, 20)", want: -123456789.01234567890123456789},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf("SELECT %s", test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			require.Equal(t, test.want, fs.Float64())
		}
	})

	t.Run("SELECT DECIMAL types and compare them to STRING", func(t *testing.T) {
		tests := []struct {
			input string
			want  string
		}{
			{input: "1.23::DECIMAL(3, 2)", want: "1.23"},
			{input: "-1.23::DECIMAL(3, 2)", want: "-1.23"},
			{input: "123.45::DECIMAL(5, 2)", want: "123.45"},
			{input: "-123.45::DECIMAL(5, 2)", want: "-123.45"},
			{input: "123456789.01::DECIMAL(11, 2)", want: "123456789.01"},
			{input: "-123456789.01::DECIMAL(11, 2)", want: "-123456789.01"},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: "1234567890123456789.234"},
			{input: "-1234567890123456789.234::DECIMAL(22, 3)", want: "-1234567890123456789.234"},
			{input: "123456789.01234567890123456789::DECIMAL(29, 20)", want: "123456789.01234567890123456789"},
			{input: "-123456789.01234567890123456789::DECIMAL(29, 20)", want: "-123456789.01234567890123456789"},
		}
		for _, test := range tests {
			r := db.QueryRow(fmt.Sprintf("SELECT %s", test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			require.Equal(t, test.want, fs.String())
			// confirms Decimal implements fmt.Stringer correctly (see #424)
			require.Equal(t, test.want, fmt.Sprint(fs))
		}
	})
}

func TestDecimalString(t *testing.T) {
	testCases := []struct {
		input    Decimal
		expected string
	}{
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(0),
			},
			expected: "0",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(0),
			},
			expected: "0",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(1234567890),
			},
			expected: "1234567890",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 0,
				Value: big.NewInt(-1234567890),
			},
			expected: "-1234567890",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(1234567890),
			},
			expected: "123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(-1234567890),
			},
			expected: "-123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 2,
				Value: big.NewInt(1234567890),
			},
			expected: "12345678.9",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 2,
				Value: big.NewInt(-1234567890),
			},
			expected: "-12345678.9",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(1234567890),
			},
			expected: "1234.56789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 6,
				Value: big.NewInt(-1234567890),
			},
			expected: "-1234.56789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 12,
				Value: big.NewInt(1234567890),
			},
			expected: "0.00123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 12,
				Value: big.NewInt(-1234567890),
			},
			expected: "-0.00123456789",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(1234500000),
			},
			expected: "123450000",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 1,
				Value: big.NewInt(-1234500000),
			},
			expected: "-123450000",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 8,
				Value: big.NewInt(-705399),
			},
			expected: "-0.00705399",
		},
		{
			input: Decimal{
				Width: 18,
				Scale: 8,
				Value: big.NewInt(821662),
			},
			expected: "0.00821662",
		},
	}

	for _, tc := range testCases {
		actual := tc.input.String()
		if actual != tc.expected {
			require.Equal(t, tc.expected, actual)
		}
	}
}

func TestBlob(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Scan a hexadecimal value.
	var b []byte
	require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&b))
	require.Equal(t, []byte{0xAA}, b)
}

func TestList(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Test a LIST exceeding duckdb's standard vector size.
	const n = 4000
	var row Composite[[]int]
	require.NoError(t, db.QueryRow("SELECT range(0, ?, 1)", n).Scan(&row))
	require.Len(t, row.Get(), n)
	for i := 0; i < n; i++ {
		require.Equal(t, i, row.Get()[i])
	}
}

func TestUUID(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE uuid_test(uuid UUID)`)
	require.NoError(t, err)

	tests := []uuid.UUID{
		uuid.New(),
		uuid.Nil,
		uuid.MustParse("80000000-0000-0000-0000-200000000000"),
	}
	for _, test := range tests {
		_, err = db.Exec(`INSERT INTO uuid_test VALUES(?)`, test)
		require.NoError(t, err)

		var val uuid.UUID
		require.NoError(t, db.QueryRow(`SELECT uuid FROM uuid_test WHERE uuid = ?`, test).Scan(&val))
		require.Equal(t, test, val)

		require.NoError(t, db.QueryRow(`SELECT ?`, test).Scan(&val))
		require.Equal(t, test, val)

		require.NoError(t, db.QueryRow(`SELECT ?::uuid`, test).Scan(&val))
		require.Equal(t, test, val)

		var u UUID
		require.NoError(t, db.QueryRow(`SELECT uuid FROM uuid_test WHERE uuid = ?`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())

		require.NoError(t, db.QueryRow(`SELECT ?`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())

		require.NoError(t, db.QueryRow(`SELECT ?::uuid`, test).Scan(&u))
		require.Equal(t, test.String(), u.String())
	}
}

func TestUUIDScanError(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var u UUID
	// invalid value type
	require.Error(t, db.QueryRow(`SELECT 12345`).Scan(&u))
	// string value not valid
	require.Error(t, db.QueryRow(`SELECT 'I am not a UUID.'`).Scan(&u))
	// blob value not valid
	require.Error(t, db.QueryRow(`SELECT '123456789012345678901234567890123456'::BLOB`).Scan(&u))
}

func TestDate(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	tests := map[string]struct {
		want  time.Time
		input string
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, time.December, 12, 0, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as DATE)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}

	ts, err := time.Parse(time.DateTime, time.DateTime)
	require.NoError(t, err)

	var res time.Time
	err = db.QueryRow(`SELECT ?::DATE`, ts).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, time.Date(2006, time.January, 0o2, 0, 0, 0, 0, time.UTC), res)
}

func TestTime(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	timeUTC := time.Date(1, time.January, 1, 11, 42, 7, 0, time.UTC)

	var res time.Time
	err = db.QueryRow(`SELECT ?::TIME`, timeUTC).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, timeUTC, res)

	timeTZ := time.Date(1, time.January, 1, 11, 42, 7, 0, IST)

	err = db.QueryRow(`SELECT ?::TIMETZ`, timeTZ).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, timeTZ.UTC(), res)
}

func TestENUMs(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	type environment string
	const (
		Sea  environment = "Sea"
		Air  environment = "Air"
		Land environment = "Land"
	)

	_, err := db.Exec("CREATE TYPE element AS ENUM ('Sea', 'Air', 'Land')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE vehicles (name text, environment element)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO vehicles VALUES (?, ?), (?, ?)", "Aircraft", Air, "Boat", Sea)
	require.NoError(t, err)

	var name string
	var env environment
	require.NoError(t, db.QueryRow("SELECT name, environment FROM vehicles WHERE environment = ?", Air).Scan(&name, &env))
	require.Equal(t, "Aircraft", name)
	require.Equal(t, Air, env)

	_, err = db.Exec("CREATE TABLE all_enums (environments element[])")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO all_enums VALUES ([?, ?, ?])", Air, Land, Sea)
	require.NoError(t, err)

	var row Composite[[]environment]
	require.NoError(t, db.QueryRow("SELECT environments FROM all_enums").Scan(&row))
	require.ElementsMatch(t, []environment{Air, Sea, Land}, row.Get())
}

func TestHugeInt(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT different HUGEINT values", func(t *testing.T) {
		tests := []string{
			"0",
			"1",
			"-1",
			"9223372036854775807",
			"-9223372036854775808",
			"170141183460469231731687303715884105727",
			"-170141183460469231731687303715884105727",
		}
		for _, test := range tests {
			var res *big.Int
			err := db.QueryRow(fmt.Sprintf("SELECT %s::HUGEINT", test)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test, res.String())
		}
	})

	t.Run("HUGEINT binding", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE hugeint_test (number HUGEINT)")
		require.NoError(t, err)

		val := big.NewInt(1)
		val.SetBit(val, 101, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", val)
		require.NoError(t, err)

		var res *big.Int
		err = db.QueryRow("SELECT number FROM hugeint_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val.String(), res.String())

		tooHuge := big.NewInt(1)
		tooHuge.SetBit(tooHuge, 129, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", tooHuge)
		require.Error(t, err)
		require.Contains(t, err.Error(), "too big for HUGEINT")
	})
}

func TestTimestampTZ(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS tbl (tz TIMESTAMPTZ)`)
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"

	// Test a location east of GMT.
	loc, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", loc)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tbl (tz) VALUES(?)`, ts)
	require.NoError(t, err)

	var tz time.Time
	err = db.QueryRow(`SELECT tz FROM tbl`).Scan(&tz)
	require.NoError(t, err)
	require.Equal(t, ts.UTC(), tz.UTC())

	// Reset and test a location west of GMT.
	_, err = db.Exec(`DELETE FROM tbl`)
	require.NoError(t, err)

	loc, err = time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	ts, err = time.ParseInLocation(longForm, "2016-01-17 10:04:05 PDT", loc)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO tbl (tz) VALUES(?)`, ts)
	require.NoError(t, err)

	err = db.QueryRow(`SELECT tz FROM tbl`).Scan(&tz)
	require.NoError(t, err)
	require.Equal(t, ts.UTC(), tz.UTC())

	// Test other time zone.
	ti := time.Now().UTC().Truncate(time.Microsecond)

	_, err = db.Exec(`SET TimeZone = 'Etc/UTC'`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE ts_tbl (t TIMESTAMPTZ)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO ts_tbl VALUES (?)`, ti)
	require.NoError(t, err)

	var newTime time.Time
	require.NoError(t, db.QueryRow(`SELECT t FROM ts_tbl`).Scan(&newTime))
	require.Equal(t, ti, newTime)

	// Test disabling TIMESTAMP_TZ casts.

	_, err = db.Exec(`SET disable_timestamptz_casts = true`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE times (t TIMESTAMPTZ)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO times VALUES (?)`, ti)
	require.NoError(t, err)
}

func TestBoolean(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	var res bool
	require.NoError(t, db.QueryRow("SELECT ?", true).Scan(&res))
	require.True(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", false).Scan(&res))
	require.False(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", 0).Scan(&res))
	require.False(t, res)

	require.NoError(t, db.QueryRow("SELECT ?", 1).Scan(&res))
	require.True(t, res)
}

func TestTimestamp(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":         {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970":   {input: "1950-12-12", want: time.Date(1950, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":    {input: "2022-12-12", want: time.Date(2022, time.December, 12, 0, 0, 0, 0, time.UTC)},
		"HH:MM:SS":      {input: "2022-12-12 11:35:43", want: time.Date(2022, time.December, 12, 11, 35, 43, 0, time.UTC)},
		"HH:MM:SS.DDDD": {input: "2022-12-12 11:35:43.5678", want: time.Date(2022, time.December, 12, 11, 35, 43, 567800000, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as TIMESTAMP)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}
}

func TestInterval(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("INTERVAL binding", func(t *testing.T) {
		interval := Interval{Days: 10, Months: 4, Micros: 4}
		row := db.QueryRow("SELECT ?::INTERVAL", interval)

		var res Interval
		require.NoError(t, row.Scan(&res))
		require.Equal(t, interval, res)
	})

	t.Run("INTERVAL scanning", func(t *testing.T) {
		tests := map[string]struct {
			input string
			want  Interval
		}{
			"simple interval": {
				input: "INTERVAL 5 HOUR",
				want:  Interval{Days: 0, Months: 0, Micros: 18000000000},
			},
			"interval arithmetic": {
				input: "INTERVAL 1 DAY + INTERVAL 5 DAY",
				want:  Interval{Days: 6, Months: 0, Micros: 0},
			},
			"timestamp arithmetic": {
				input: "CAST('2022-05-01' as TIMESTAMP) - CAST('2022-04-01' as TIMESTAMP)",
				want:  Interval{Days: 30, Months: 0, Micros: 0},
			},
		}
		for _, test := range tests {
			var res Interval
			err := db.QueryRow(fmt.Sprintf("SELECT %s", test.input)).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, test.want, res)
		}
	})
}

func TestArray(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE needle (vec FLOAT[3])`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO needle VALUES (array[5, 5, 5])`)
	require.NoError(t, err)

	res, err := db.Query(`SELECT vec FROM needle`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	for res.Next() {
		var vec Composite[[3]float64]
		err = res.Scan(&vec)
		require.NoError(t, err)
		require.NoError(t, res.Err())
		require.Equal(t, [3]float64{5, 5, 5}, vec.Get())
	}
}

func TestJSONType(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE test (c1 STRUCT(index INTEGER))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test VALUES ({index: 1}), ({index: 2}), ({index: 2}), ({index: 3}), ({index: 3}), ({index: 3})`)
	require.NoError(t, err)

	// Verify results.
	row := db.QueryRowContext(context.Background(), `
	SELECT json_group_object(t2.status, t2.count) AS result
	FROM (
		SELECT json_extract(c1, '$.index') AS status, COUNT(*) AS count
		FROM test
		GROUP BY status
	) AS t2`)

	var res Composite[map[string]any]
	require.NoError(t, row.Scan(&res))
	require.Equal(t, float64(1), res.Get()["1"])
	require.Equal(t, float64(2), res.Get()["2"])
	require.Equal(t, float64(3), res.Get()["3"])
}

func TestJSONColType(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE OR REPLACE TABLE test AS SELECT '[10]'::JSON AS col, 1 AS val`)
	require.NoError(t, err)

	res, err := db.QueryContext(context.Background(), `SELECT col AS value, count(*) AS count FROM test GROUP BY 1`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	columnTypes, err := res.ColumnTypes()
	require.NoError(t, err)

	require.Equal(t, 2, len(columnTypes))
	require.Equal(t, aliasJSON, columnTypes[0].DatabaseTypeName())
	require.Equal(t, typeToStringMap[TYPE_BIGINT], columnTypes[1].DatabaseTypeName())
	require.Equal(t, reflect.TypeOf((*any)(nil)).Elem(), columnTypes[0].ScanType())
	require.Equal(t, reflect.TypeOf(int64(0)), columnTypes[1].ScanType())
}

func TestUnionTypes(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Test basic UNION type creation and scanning.
	t.Run("basic UNION operations", func(t *testing.T) {
		r, err := db.Query(`
            SELECT
                (123)::UNION(num INTEGER, str VARCHAR) AS int_union,
                ('hello')::UNION(num INTEGER, str VARCHAR) AS str_union,
                NULL::UNION(num INTEGER, str VARCHAR) AS null_union
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var intUnion, strUnion Union
		var nullUnion any
		err = r.Scan(&intUnion, &strUnion, &nullUnion)
		require.NoError(t, err)

		require.Equal(t, "num", intUnion.Tag)
		require.Equal(t, int32(123), intUnion.Value)

		require.Equal(t, "str", strUnion.Tag)
		require.Equal(t, "hello", strUnion.Value)

		require.Nil(t, nullUnion)
	})

	// Test UNION with different types.
	t.Run("UNION with different types", func(t *testing.T) {
		r, err := db.Query(`
            WITH unions AS (
                SELECT
                    (1.5)::UNION(d DOUBLE, i INTEGER) AS double_union,
                    ('2024-01-01'::DATE)::UNION(d DATE, s VARCHAR) AS date_union
            )
            SELECT * FROM unions
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var doubleUnion, dateUnion Union
		err = r.Scan(&doubleUnion, &dateUnion)
		require.NoError(t, err)

		require.Equal(t, "d", doubleUnion.Tag)
		require.Equal(t, float64(1.5), doubleUnion.Value)

		require.Equal(t, "d", dateUnion.Tag)
		require.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), dateUnion.Value)
	})

	// Test column type information.
	t.Run("UNION column type info", func(t *testing.T) {
		r, err := db.Query(`
            SELECT (123)::UNION(num INTEGER, str VARCHAR) AS union_col
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		types, err := r.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, "UNION(num INTEGER, str VARCHAR)", types[0].DatabaseTypeName())
	})

	// Test multiple UNION members.
	t.Run("UNION with multiple members", func(t *testing.T) {
		r, err := db.Query(`
            SELECT (123)::UNION(a INTEGER, b VARCHAR, c DOUBLE) AS multi_union
        `)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		require.True(t, r.Next())
		var val Union
		err = r.Scan(&val)
		require.NoError(t, err)
		require.Equal(t, "a", val.Tag)
		require.Equal(t, int32(123), val.Value)
	})
}
