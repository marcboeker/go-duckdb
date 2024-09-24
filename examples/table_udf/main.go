package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"

	"github.com/marcboeker/go-duckdb"
)

var db *sql.DB

type tableUDF struct {
	n     int64
	count int64
}

func BindTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	return &tableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (d *tableUDF) Columns() []duckdb.ColumnInfo {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	return []duckdb.ColumnInfo{
		{Name: "result", T: t},
	}
}

func (d *tableUDF) Init() {}

func (d *tableUDF) FillRow(row duckdb.Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}
	d.count++
	duckdb.SetRowValue(row, 0, d.count)
	return true, nil
}

func (d *tableUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(d.n),
		Exact:     true,
	}
}

func main() {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())

	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	fun := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: BindTableUDF,
	}

	duckdb.RegisterTableUDF(conn, "whoo", fun)

	check(db.Ping())

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM whoo(100)")
	check(err)
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		for i, value := range values {
			switch value.(type) {
			case nil:
				fmt.Print(columns[i], ": NULL")
			case []byte:
				fmt.Print(columns[i], ": ", string(value.([]byte)))
			default:
				fmt.Print(columns[i], ": ", value)
			}
			fmt.Printf("\nType: %s\n", reflect.TypeOf(value))
		}
		fmt.Println("-----------------------------------")
	}
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
