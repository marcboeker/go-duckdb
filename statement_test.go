package duckdb

import (
	"strings"
	"testing"
)

func TestPrepareQuery(t *testing.T) {

	db := openDB(t) //:memory:

	defer db.Close()
	createTable(db, t)

	stmt, err := db.Prepare("select * from foo where baz=?")
	if err != nil {
		t.Fatal("prepare:", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(0)
	if err != nil {
		t.Fatal("query:", err)
	}
	defer rows.Close()
}

func TestPrepareWithError(t *testing.T) {

	db := openDB(t) //:memory:

	defer db.Close()
	createTable(db, t)
	testCases := []struct {
		tpl string
		err string
	}{
		{
			tpl: "select * from tbl where baz=?",
			err: "Table with name tbl does not exist",
		},
		{
			tpl: "select * from foo where col=?",
			err: `Referenced column "col" not found in FROM clause`,
		},
		{
			tpl: "select * from foo  col=?",
			err: `syntax error at or near "="`,
		},
	}
	for _, tC := range testCases {
		stmt, err := db.Prepare(tC.tpl)
		if err == nil {
			stmt.Close()
			t.Fatal("expected err:", tC.tpl)
		}

		if !strings.Contains(err.Error(), tC.err) {
			t.Error("expected contains[", tC.err, "], but:", err)
		}
	}
}
