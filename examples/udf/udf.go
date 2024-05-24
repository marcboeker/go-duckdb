package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/marcboeker/go-duckdb"
)

var db *sql.DB

type user struct {
	name    string
	age     int
	height  float32
	awesome bool
	bday    time.Time
}

type tableUDF struct {
	n     int64
	count int64
}

func (d *tableUDF) Config() duckdb.TableFunctionConfig {
	return duckdb.TableFunctionConfig{
		Arguments: []duckdb.Type{
			duckdb.NewDuckdbType[int64](),
		},
		Pushdownprojection: false,
	}
}

func (d *tableUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (duckdb.TableFunction, []duckdb.ColumnMetaData, error) {
	d.count = 0
	d.n = args[0].(int64)
	return d, []duckdb.ColumnMetaData{
		{Name: "result", T: duckdb.NewDuckdbType[int64]()},
	}, nil
}

func (d *tableUDF) Init() duckdb.TableFunctionInitData {
	return duckdb.TableFunctionInitData{
		MaxThreads: 1,
	}
}

func (d *tableUDF) FillRow(row duckdb.Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}
	d.count++
	duckdb.SetRowValue(row, 0, d.count)
	return true, nil
}

func (d *tableUDF) Cardinality() *duckdb.CardinalityData {
	return &duckdb.CardinalityData{
		Cardinality: uint(d.n),
		IsExact:     true,
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
	var fun tableUDF
	duckdb.RegisterTableUDF(conn, "whoo", &fun)

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
