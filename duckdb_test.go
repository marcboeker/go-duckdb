package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"reflect"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	require.NotNil(t, db)
}

func TestOpenWithConfig(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("duckdb", "?access_mode=read_write&threads=4")
	require.NoError(t, err)
	defer db.Close()

	// Check if config options have been set.
	res := db.QueryRow("SELECT current_setting('access_mode'), current_setting('threads')")
	var (
		accessMode string
		threads    int64
	)
	require.NoError(t, res.Scan(&accessMode, &threads))
	require.Equal(t, int64(4), threads)
	require.Equal(t, "read_write", accessMode)
}

func TestOpenWithInvalidConfig(t *testing.T) {
	t.Parallel()
	db, _ := sql.Open("duckdb", "?threads=NaN")
	err := db.Ping()

	if !errors.Is(err, prepareConfigError) {
		t.Fatal("invalid config should not be accepted")
	}
}

func TestExec(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	res := createTable(db, t)
	if res == nil {
		t.Error("result cannot be nil")
	}
}

func TestQuery(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

	_, err := db.Exec("INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
	require.NoError(t, err)

	rows, err := db.Query("SELECT bar, baz FROM foo WHERE baz > ?", 12344)
	require.NoError(t, err)
	defer rows.Close()

	found := false
	for rows.Next() {
		var (
			bar string
			baz int
		)
		err := rows.Scan(&bar, &baz)
		require.NoError(t, err)
		if bar != "lala" || baz != 12345 {
			t.Errorf("wrong values for bar [%s] and baz [%d]", bar, baz)
		}
		found = true
	}

	require.True(t, found, "could not find row")
}

func TestQueryJSON(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var bytes []byte
	err := db.QueryRow("select json_array('foo', 'bar')").Scan(&bytes)
	require.NoError(t, err)

	var data []string
	require.NoError(t, json.Unmarshal(bytes, &data))
	require.Equal(t, len(data), 2)
	require.Equal(t, data[0], "foo")
	require.Equal(t, data[1], "bar")
}

func TestQueryStruct(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

	_, err := db.Exec("INSERT INTO foo VALUES('lala', ?), ('lala', ?), ('lalo', 42), ('lalo', 43)", 12345, 12346)
	require.NoError(t, err)

	type row struct {
		Bar string
		Baz int
	}

	var rows []Composite[row]
	require.NoError(t, sqlscan.Select(context.Background(), db, &rows, "SELECT ROW(bar, baz) FROM foo ORDER BY baz"))

	require.Len(t, rows, 4)
	require.Equal(t, row{"lalo", 42}, rows[0].Get())
	require.Equal(t, row{"lalo", 43}, rows[1].Get())
	require.Equal(t, row{"lala", 12345}, rows[2].Get())
	require.Equal(t, row{"lala", 12346}, rows[3].Get())
}

func TestQueryMap(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var m Map
	require.NoError(t, db.QueryRow("select map(['foo', 'bar'], ['a', 'e'])").Scan(&m))
	require.Equal(t, Map{
		"foo": "a",
		"bar": "e",
	}, m)
}

func TestQueryNonStringMap(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var m Map
	require.NoError(t, db.QueryRow("select map([0], ['a'])").Scan(&m))
	require.Equal(t, Map{int64(0): "a"}, m)
}

func TestQuerySimpleList(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	const n = 1000
	var row Composite[[]int]
	require.NoError(t, sqlscan.Get(context.Background(), db, &row, "SELECT range(0, ?, 1)", n))
	require.Equal(t, n, len(row.Get()))
	for i := 0; i < n; i++ {
		require.Equal(t, i, row.Get()[i])
	}
}

func TestQueryDecimal(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("decimal widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			var f float64 = 999
			require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT 0::DECIMAL(%d, 1)", i)).Scan(&f))
			require.Equal(t, float64(0), f)
		}
	})

	t.Run("multiple rows", func(t *testing.T) {
		var fs []float64
		query := `SELECT * FROM (VALUES
			(1.23 :: DECIMAL(3, 2)),
			(123.45 :: DECIMAL(5, 2)),
			(123456789.01 :: DECIMAL(11, 2)),
			(1234567890123456789.234 :: DECIMAL(22, 3)),
		)`
		require.NoError(t, sqlscan.Select(context.Background(), db, &fs, query))
		require.Equal(t, []float64{1.23, 123.45, 123456789.01, 1234567890123456789.234}, fs)
	})

	t.Run("huge decimal", func(t *testing.T) {
		var f float64
		require.NoError(t, db.QueryRow("SELECT 123456789.01234567890123456789 :: DECIMAL(38, 20)").Scan(&f))
		require.True(t, math.Abs(float64(123456789.01234567890123456789)-f) < 0.0000001)
	})
}

func TestQueryListOfStructs(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	type point struct {
		X int
		Y int
	}
	var row Composite[[]point]
	require.NoError(t, sqlscan.Get(context.Background(), db, &row, "SELECT ARRAY[{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]"))
	require.Equal(t, 2, len(row.Get()))
	require.Equal(t, point{1, 2}, row.Get()[0])
	require.Equal(t, point{3, 4}, row.Get()[1])
}

func TestQueryUUID(t *testing.T) {
	t.Parallel()
	test := func(t *testing.T, expected uuid.UUID) {
		db := openDB(t)
		defer db.Close()

		var uuid uuid.UUID
		require.NoError(t, db.QueryRow("SELECT ?", expected).Scan(&uuid))
		require.Equal(t, expected, uuid)

		require.NoError(t, db.QueryRow("SELECT ?::uuid", expected).Scan(&uuid))
		require.Equal(t, expected, uuid)
	}

	for i := 0; i < 50; i++ {
		t.Run(fmt.Sprintf("uuid %d", i), func(t *testing.T) {
			t.Parallel()
			test(t, uuid.New())
		})
	}

	test(t, uuid.Nil)
	test(t, uuid.MustParse("80000000-0000-0000-0000-200000000000"))
}

func TestQueryCompositeAggregate(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

	_, err := db.Exec("INSERT INTO foo VALUES('lala', ?), ('lala', ?), ('lalo', 42), ('lalo', 43)", 12346, 12345)
	if err != nil {
		t.Fatal(err)
	}

	type strukt struct {
		Baz int
	}

	type row struct {
		Bar string
		Agg Composite[[]strukt]
	}

	var rows []row
	require.NoError(t, sqlscan.Select(context.Background(), db, &rows, "SELECT bar, ARRAY_AGG(ROW(baz) ORDER BY baz DESC) as agg FROM foo GROUP BY bar ORDER BY bar"))
	require.Len(t, rows, 2)
	require.Equal(t, "lala", rows[0].Bar)
	require.Equal(t, []strukt{{12346}, {12345}}, rows[0].Agg.Get())
	require.Equal(t, "lalo", rows[1].Bar)
	require.Equal(t, []strukt{{43}, {42}}, rows[1].Agg.Get())
}

func TestLargeNumberOfRows(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE integers AS SELECT * FROM range(0, 100000, 1) t(i);")
	require.NoError(t, err)

	rows, err := db.Query("SELECT i FROM integers ORDER BY i ASC")
	require.NoError(t, err)
	defer rows.Close()

	expected := 0
	for rows.Next() {
		var i int
		require.NoError(t, rows.Scan(&i))
		assert.Equal(t, expected, i)
		expected++
	}
}

func TestQueryWithWrongSyntax(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("select * from tbl col=?", 1)
	require.Error(t, err, "should return error with invalid syntax")
}

func TestQueryWithMissingParams(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("select * from tbl col=?")
	require.Error(t, err, "should return error with missing parameters")
}

// Sum(int) generate result of type HugeInt (int128)
func TestSumOfInt(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	for _, expected := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
		t.Run(fmt.Sprintf("sum(%d)", expected), func(t *testing.T) {
			var res int64
			if err := db.QueryRow("SELECT SUM(?)", expected).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if res != expected {
				t.Errorf("unexpected value %d != resulting value %d", expected, res)
			}
		})
	}
}

func TestQueryNull(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	var s *int
	require.NoError(t, db.QueryRow("select null").Scan(&s))
	require.Nil(t, s)
}

func TestVarchar(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	var s string
	require.NoError(t, db.QueryRow("select 'inline'").Scan(&s))
	require.Equal(t, "inline", s)

	// size = 12 needs to be verified
	require.NoError(t, db.QueryRow("select 'inlineSize12'").Scan(&s))
	require.Equal(t, "inlineSize12", s)

	// size = 13 needs to be verified
	require.NoError(t, db.QueryRow("select 'inlineSize13_'").Scan(&s))
	require.Equal(t, "inlineSize13_", s)

	const q = "uninlined string that is greater than 12 bytes long"
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT '%s'", q)).Scan(&s))
	require.Equal(t, q, s)
}

func TestBlob(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	var bytes []byte

	require.NoError(t, db.QueryRow("SELECT 'AB'::BLOB;").Scan(&bytes))
	require.Equal(t, "AB", string(bytes))

	require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&bytes))
	require.Equal(t, []byte{0xAA}, bytes)
}

func TestJson(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	var bytes []byte

	require.NoError(t, db.QueryRow("SELECT '{}'::JSON").Scan(&bytes))
	require.Equal(t, "{}", string(bytes))

	require.NoError(t, db.QueryRow(`SELECT '{"foo": "bar"}'::JSON`).Scan(&bytes))
	require.Equal(t, `{"foo": "bar"}`, string(bytes))
}

// CAST(? as DATE) generate result of type Date (time.Time)
func TestDate(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	tests := map[string]struct {
		want  time.Time
		input string
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res time.Time
			err := db.QueryRow("SELECT CAST(? as DATE)", tc.input).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, tc.want, res)
		})
	}
}

func TestTimestamp(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
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
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res time.Time
			if err := db.QueryRow("SELECT CAST(? as TIMESTAMP)", tc.input).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if res != tc.want {
				t.Errorf("expected value %v != resulting value %v", tc.want, res)
			}
		})
	}
}

func TestInterval(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
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
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res Interval
			if err := db.QueryRow(fmt.Sprintf("SELECT %s", tc.input)).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if !reflect.DeepEqual(tc.want, res) {
				t.Errorf("expected value %v != resulting value %v", tc.want, res)
			}
		})
	}
}

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func createTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec("CREATE TABLE foo(bar VARCHAR, baz INTEGER)")
	require.NoError(t, err)
	return &res
}
