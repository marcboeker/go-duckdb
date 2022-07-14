package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"testing"
	"time"

	"github.com/georgysavva/scany/sqlscan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	require.NotNil(t, db)
}

func TestOpenWithConfig(t *testing.T) {
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
	db, _ := sql.Open("duckdb", "?threads=NaN")
	err := db.Ping()

	if !errors.Is(err, prepareConfigError) {
		t.Fatal("invalid config should not be accepted")
	}
}

func TestExec(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	res := createTable(db, t)
	if res == nil {
		t.Error("result cannot be nil")
	}
}

func TestQuery(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

	_, err := db.Exec("INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)", 12345, 54321)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT bar, baz FROM foo WHERE baz > ?", 12344)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var (
			bar string
			baz int
		)
		err := rows.Scan(&bar, &baz)
		if err != nil {
			t.Fatal(err)
		}
		// if bar != "lala" || baz != 12345 {
		// 	t.Errorf("wrong values for bar [%s] and baz [%d]", bar, baz)
		// }
		found = true
	}

	if !found {
		t.Error("could not find row")
	}
}

func TestQueryJSON(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	var bytes []byte
	err := db.QueryRow("select json_array('foo', 'bar')").Scan(&bytes)
	if err != nil {
		log.Fatal(err)
	}

	var data []string
	require.NoError(t, json.Unmarshal(bytes, &data))
	require.Equal(t, len(data), 2)
	require.Equal(t, data[0], "foo")
	require.Equal(t, data[1], "bar")
}

func TestQueryStruct(t *testing.T) {
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

func TestQuerySimpleList(t *testing.T) {
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

func TestQueryListOfStructs(t *testing.T) {
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

func TestQueryCompositeAggregate(t *testing.T) {
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
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("select * from tbl col=?", 1)
	if err == nil {
		t.Error("should return error with invalid syntax")
	}
}

func TestQueryWithMissingParams(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Query("select * from tbl col=?")
	if err == nil {
		t.Error("should return error about missing parameters")
	}
}

// Sum(int) generate result of type HugeInt (int128)
func TestSumOfInt(t *testing.T) {
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

func TestVarchar(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	var s string
	require.NoError(t, db.QueryRow("select 'inline'").Scan(&s))
	require.Equal(t, "inline", s)

	const q = "uninlined string that is greater than 12 bytes long"
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT '%s'", q)).Scan(&s))
	require.Equal(t, q, s)
}

func TestBlob(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	var bytes []byte

	require.NoError(t, db.QueryRow("SELECT 'AB'::BLOB;").Scan(&bytes))
	require.Equal(t, "AB", string(bytes))

	require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&bytes))
	require.Equal(t, []byte{0xAA}, bytes)
}

func TestJson(t *testing.T) {
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
	db := openDB(t)
	defer db.Close()
	tests := map[string]struct {
		input string
		want  time.Time
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
	db := openDB(t)
	defer db.Close()
	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
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

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	return db
}

func createTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec("CREATE TABLE foo(bar VARCHAR, baz INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	return &res
}
