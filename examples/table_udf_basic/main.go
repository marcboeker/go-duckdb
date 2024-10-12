package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/marcboeker/go-duckdb"
)

var db *sql.DB

type incrementTableUDF struct {
	tableSize  int64
	currentRow int64
}

// BindTableUDF creates
func BindTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	return &incrementTableUDF{
		currentRow: 0,
		tableSize:  args[0].(int64),
	}, nil
}

func (d *incrementTableUDF) Columns() []duckdb.ColumnInfo {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	return []duckdb.ColumnInfo{
		{Name: "result", T: t},
	}
}

func (d *incrementTableUDF) Init() {}

func (d *incrementTableUDF) FillRow(row duckdb.Row) (bool, error) {
	if d.currentRow+1 > d.tableSize {
		return false, nil
	}
	d.currentRow++
	duckdb.SetRowValue(row, 0, d.currentRow)
	return true, nil
}

func (d *incrementTableUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(d.tableSize),
		Exact:       true,
	}
}

func main() {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")
	check(err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	check(err)

	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	fun := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: BindTableUDF,
	}

	duckdb.RegisterTableUDF(conn, "inc", fun)

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM inc(100)")
	check(err)
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	check(err)

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		check(err)
		for i, value := range values {
			// Keep in mind that the value could be nil for NULL values.
			// This never happens in our case, so we don't check for it.
			fmt.Printf("%s: %v\n", columns[i], value)
			fmt.Printf("Type: %s\n", reflect.TypeOf(value))
		}
		fmt.Println("-----------------------------------")
	}
}

func check(err interface{}) {
	if err != nil {
		panic(err)
	}
}
