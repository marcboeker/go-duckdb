package duckdb

import (
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type testTypesEnum string

const testTypesEnumSQL = `CREATE TYPE mood AS ENUM ('0', '1', '2')`

type testTypesStruct struct {
	A int32
	B string
}

// NOTE: Includes all supported types except UUID and DECIMAL.
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
	Hugeint_col      Composite[big.Int]
	Varchar_col      string
	Blob_col         []byte
	Timestamp_s_col  time.Time
	Timestamp_ms_col time.Time
	Timestamp_ns_col time.Time
	Enum_col         testTypesEnum
	List_col         Composite[[]int32]
	Struct_col       Composite[testTypesStruct]
	Map_col          Map
	Timestamp_tz_col time.Time
}

const testTypesTableSQL = `CREATE TABLE types_tbl (
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
	Enum_col mood,
	List_col INTEGER[],
	Struct_col STRUCT(A INTEGER, B VARCHAR),
	Map_col MAP(INTEGER, VARCHAR),
	Timestamp_tz_col TIMESTAMPTZ
)`

// We could perform the insertions via the testTypesRow struct by iterating over the rows,
// but that significantly decreases performance.
const testTypesInsertSQL = `INSERT INTO types_tbl SELECT
	r % 2 AS Boolean_col,
	r % 127 AS Tinyint_col,
	r % 32767 AS Smallint_col,
	2147483647 - r AS Integer_col,
	9223372036854775807 - r AS Bigint_col,
	r % 256 AS Utinyint_col,
	r % 65535 AS Usmallint_col,
	2147483647 - r AS Uinteger_col,
	9223372036854775807 - r AS Ubigint_col,
	r AS Float_col,
	r AS Double_col,
	'1992-09-20 11:30:00'::TIMESTAMP AS Timestamp_col,
	'1992-09-20'::DATE AS Date_col,
	'11:42:07'::TIME AS Time_col,
	INTERVAL (r) MONTH AS Interval_col,
	r AS Hugeint_col,
	repeat('hello!', r) AS Varchar_col,
	'AB' AS Blob_col,
	'1992-09-20 11:30:00'::TIMESTAMP_S AS Timestamp_s_col,
	'1992-09-20 11:30:00'::TIMESTAMP_MS AS Timestamp_ms_col,
	'1992-09-20 11:30:00'::TIMESTAMP_NS AS Timestamp_ns_col,
	(r % 3)::VARCHAR AS Enum_col,
	list_value(r) AS List_col,
	ROW(r, 'a' || r) AS Struct_col,
	MAP{r: 'other_longer_val'} AS Map_col,
	'1992-09-20 11:30:00'::TIMESTAMPTZ AS Timestamp_tz_col
FROM range(?) tbl(r)`

func testTypesGenerateRow(i int) testTypesRow {
	ts := time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC)
	date := time.Date(1992, 9, 20, 0, 0, 0, 0, time.UTC)
	t := time.Date(1970, 1, 1, 11, 42, 7, 0, time.UTC)
	hugeintCol := Composite[big.Int]{
		*big.NewInt(int64(i)),
	}
	varcharCol := ""
	for j := 0; j < i; j++ {
		varcharCol += "hello!"
	}
	listCol := Composite[[]int32]{
		[]int32{int32(i)},
	}
	structCol := Composite[testTypesStruct]{
		testTypesStruct{int32(i), "a" + strconv.Itoa(i)},
	}
	mapCol := Map{
		int32(i): "other_longer_val",
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
		date,
		t,
		Interval{Days: 0, Months: int32(i), Micros: 0},
		hugeintCol,
		varcharCol,
		[]byte{'A', 'B'},
		ts,
		ts,
		ts,
		testTypesEnum(strconv.Itoa(i % 3)),
		listCol,
		structCol,
		mapCol,
		ts,
	}
}

func testTypesQuery(db *sql.DB) ([]testTypesRow, error) {
	res, err := db.Query(`SELECT * FROM types_tbl ORDER BY Smallint_col`)
	if err != nil {
		return nil, err
	}

	var slice []testTypesRow
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
			&r.Timestamp_tz_col)
		if err != nil {
			return slice, err
		}
		slice = append(slice, r)
	}
	return slice, res.Close()
}

// TestTypes tests scanning various duckdb types.
func TestTypes(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	limit := 3

	_, err := db.Exec(testTypesEnumSQL)
	require.NoError(t, err)
	createTable(db, t, testTypesTableSQL)

	_, err = db.Exec(testTypesInsertSQL, limit)
	require.NoError(t, err)

	slice, err := testTypesQuery(db)
	require.NoError(t, err)
	require.Equal(t, limit, len(slice))

	for i, r := range slice {
		expected := testTypesGenerateRow(i)
		require.Equal(t, expected, r)
	}

	require.NoError(t, db.Close())
}

func compareDecimal(t *testing.T, want Decimal, got Decimal) {
	require.Equal(t, want.Scale, got.Scale)
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, want.Value.String(), got.Value.String())
}

func TestDecimal(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	t.Run("SELECT all possible DECIMAL widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			r := db.QueryRow(fmt.Sprintf("SELECT 0::DECIMAL(%d, 1)", i))
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
		require.NoError(t, res.Close())

		bigNumber, success := new(big.Int).SetString("1234567890123456789234", 10)
		require.Equal(t, true, success)
		bigNegativeNumber, success := new(big.Int).SetString("-1234567890123456789234", 10)
		require.Equal(t, true, success)
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
			r := db.QueryRow(fmt.Sprintf("SELECT %s", test.input))
			var fs Decimal
			require.NoError(t, r.Scan(&fs))
			compareDecimal(t, test.want, fs)
		}
	})

	t.Run("SELECT a huge DECIMAL ", func(t *testing.T) {
		bigInt, success := new(big.Int).SetString("12345678901234567890123456789", 10)
		require.Equal(t, true, success)
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

	require.NoError(t, db.Close())
}

func TestBlob(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	// Scan a hexadecimal value.
	var bytes []byte
	require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&bytes))
	require.Equal(t, []byte{0xAA}, bytes)
	require.NoError(t, db.Close())
}

func TestList(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	// Test a LIST exceeding duckdb's standard vector size.
	const n = 4000
	var row Composite[[]int]
	require.NoError(t, db.QueryRow("SELECT range(0, ?, 1)", n).Scan(&row))
	require.Equal(t, n, len(row.Get()))
	for i := 0; i < n; i++ {
		require.Equal(t, i, row.Get()[i])
	}
	require.NoError(t, db.Close())
}

func TestUUID(t *testing.T) {
	t.Parallel()
	db := openDB(t)

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
	}

	require.NoError(t, db.Close())
}

func TestDate(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	tests := map[string]struct {
		want  time.Time
		input string
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as DATE)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}

	require.NoError(t, db.Close())
}

func TestENUMs(t *testing.T) {
	t.Parallel()
	db := openDB(t)

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

	require.NoError(t, db.Close())
}

func TestHugeInt(t *testing.T) {
	t.Parallel()
	db := openDB(t)

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

	require.NoError(t, db.Close())
}

func TestTimestampTZ(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	_, err := db.Exec("CREATE TABLE IF NOT EXISTS tbl (tz TIMESTAMPTZ)")
	require.NoError(t, err)

	IST, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)

	const longForm = "2006-01-02 15:04:05 MST"
	ts, err := time.ParseInLocation(longForm, "2016-01-17 20:04:05 IST", IST)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO tbl (tz) VALUES(?)", ts)
	require.NoError(t, err)

	var tz time.Time
	err = db.QueryRow("SELECT tz FROM tbl").Scan(&tz)
	require.NoError(t, err)
	require.Equal(t, ts.UTC(), tz)
	require.NoError(t, db.Close())
}

func TestBoolean(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	var res bool
	require.NoError(t, db.QueryRow("SELECT ?", true).Scan(&res))
	require.Equal(t, true, res)

	require.NoError(t, db.QueryRow("SELECT ?", false).Scan(&res))
	require.Equal(t, false, res)

	require.NoError(t, db.QueryRow("SELECT ?", 0).Scan(&res))
	require.Equal(t, false, res)

	require.NoError(t, db.QueryRow("SELECT ?", 1).Scan(&res))
	require.Equal(t, true, res)
	require.NoError(t, db.Close())
}

func TestTimestamp(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":         {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970":   {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":    {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
		"HH:MM:SS":      {input: "2022-12-12 11:35:43", want: time.Date(2022, 12, 12, 11, 35, 43, 0, time.UTC)},
		"HH:MM:SS.DDDD": {input: "2022-12-12 11:35:43.5678", want: time.Date(2022, 12, 12, 11, 35, 43, 567800000, time.UTC)},
	}
	for _, test := range tests {
		var res time.Time
		err := db.QueryRow("SELECT CAST(? as TIMESTAMP)", test.input).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, test.want, res)
	}
	require.NoError(t, db.Close())
}

func TestInterval(t *testing.T) {
	t.Parallel()
	db := openDB(t)

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

	require.NoError(t, db.Close())
}

// NOTE: go-duckdb only contains very few benchmarks. The purpose of those benchmarks is to avoid regressions
// of its main functionalities. I.e., functions related to implementing the database/sql interface.

func BenchmarkTypes(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	limit := 50000
	defer require.NoError(b, db.Close())

	_, err = db.Exec(testTypesEnumSQL)
	require.NoError(b, err)
	_, err = db.Exec(testTypesTableSQL)
	require.NoError(b, err)
	_, err = db.Exec(testTypesInsertSQL, limit)
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		_, err = testTypesQuery(db)
		require.NoError(b, err)
	}
}
