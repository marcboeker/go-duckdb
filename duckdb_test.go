package duckdb

import (
	"database/sql"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	db := openDB(t)

	if db == nil {
		t.Error("database cannot be nil")
	}
}

func TestExec(t *testing.T) {
	db := openDB(t)

	res := createTable(db, t)
	if res == nil {
		t.Error("result cannot be nil")
	}
}

func TestQuery(t *testing.T) {
	db := openDB(t)
	createTable(db, t)

	_, err := db.Exec("INSERT INTO foo VALUES('lala', 12345)")
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
		if bar != "lala" || baz != 12345 {
			t.Errorf("wrong values for bar [%s] and baz [%d]", bar, baz)
		}
		found = true
	}

	if !found {
		t.Error("could not find row")
	}
}

// Sum(int) generate result of type HugeInt (int128)
func TestSumOfInt(t *testing.T) {
	db := openDB(t)
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

// CAST(? as DATE) generate result of type Date (time.Time)
func TestDate(t *testing.T) {
	db := openDB(t)
	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0)},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC).Local()},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC).Local()},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res time.Time
			if err := db.QueryRow("SELECT CAST(? as DATE)", tc.input).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if res != tc.want {
				t.Errorf("expected value %v != resulting value %v", tc.want, res)
			}
		})
	}
}

func TestTimestamp(t *testing.T) {
	db := openDB(t)
	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0)},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC).Local()},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC).Local()},
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

	return db
}

func createTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec("CREATE TABLE foo(bar VARCHAR, baz INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	return &res
}
