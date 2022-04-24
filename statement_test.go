package duckdb

import (
	"testing"
)

func TestPrepareQuery(t *testing.T) {

	db := openDB(t) //:memory:

	defer db.Close()

	stmt, err := db.Prepare("select * from tbl where col=?")
	if err != nil {
		t.Fatal("prepare:", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatal("query:", err)
	}
	defer rows.Close()
}
