package duckdb

import (
	"database/sql"
	"testing"
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