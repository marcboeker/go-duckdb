package main

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/marcboeker/go-duckdb"
)

var db *sql.DB

type (
	parallelIncTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	parallelIncTableLocal struct {
		start int64
		end   int64
	}
)

func BindParallelIncTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.ThreadedRowTableSource, error) {
	return &parallelIncTableUDF{
		lock:    &sync.Mutex{},
		claimed: 0,
		n:       args[0].(int64),
	}, nil
}

func (d *parallelIncTableUDF) Columns() []duckdb.ColumnInfo {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	return []duckdb.ColumnInfo{
		{Name: "result", T: t},
	}
}

func (d *parallelIncTableUDF) Init() duckdb.ThreadedTableSourceInitData {
	return duckdb.ThreadedTableSourceInitData{
		MaxThreads: 8,
	}
}

func (d *parallelIncTableUDF) NewLocalState() any {
	return &parallelIncTableLocal{
		start: 0,
		end:   -1,
	}
}

func (d *parallelIncTableUDF) FillRow(localState any, row duckdb.Row) (bool, error) {
	state := localState.(*parallelIncTableLocal)
	if state.start >= state.end {
		// claim a new "work" unit
		d.lock.Lock()
		remaining := d.n - d.claimed
		if remaining <= 0 {
			// no more work to be done :(
			d.lock.Unlock()
			return false, nil
		} else if remaining >= 2024 {
			remaining = 2024
		}
		state.start = d.claimed
		d.claimed += remaining
		state.end = d.claimed
		d.lock.Unlock()
	}
	state.start++
	err := duckdb.SetRowValue(row, 0, state.start)
	return true, err
}

func (d *parallelIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *parallelIncTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *parallelIncTableUDF) Cardinality() *duckdb.CardinalityInfo {
	return nil
}

func (d *parallelIncTableUDF) GetFunction() duckdb.ThreadedRowTableFunction {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	return duckdb.ThreadedRowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: BindParallelIncTableUDF,
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
	fun := duckdb.ThreadedRowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: BindParallelIncTableUDF,
	}

	duckdb.RegisterTableUDF(conn, "inc", fun)

	rows, err := db.QueryContext(context.Background(), "SELECT * FROM inc(2048)")
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
