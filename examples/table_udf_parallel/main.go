package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/marcboeker/go-duckdb"
)

type (
	parallelIncrementTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	localTableState struct {
		start int64
		end   int64
	}
)

func bindParallelTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.ParallelRowTableSource, error) {
	return &parallelIncrementTableUDF{
		lock:    &sync.Mutex{},
		claimed: 0,
		n:       args[0].(int64),
	}, nil
}

func (udf *parallelIncrementTableUDF) ColumnInfos() []duckdb.ColumnInfo {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	return []duckdb.ColumnInfo{{Name: "result", T: t}}
}

func (udf *parallelIncrementTableUDF) Init() duckdb.ParallelTableSourceInfo {
	return duckdb.ParallelTableSourceInfo{
		MaxThreads: 8,
	}
}

func (udf *parallelIncrementTableUDF) NewLocalState() any {
	return &localTableState{
		start: 0,
		end:   -1,
	}
}

func (udf *parallelIncrementTableUDF) FillRow(localState any, row duckdb.Row) (bool, error) {
	state := localState.(*localTableState)

	if state.start >= state.end {
		// Claim a new work unit.
		udf.lock.Lock()
		remaining := udf.n - udf.claimed

		if remaining <= 0 {
			// No more work.
			udf.lock.Unlock()
			return false, nil
		} else if remaining >= 2024 {
			remaining = 2024
		}

		state.start = udf.claimed
		udf.claimed += remaining
		state.end = udf.claimed
		udf.lock.Unlock()
	}

	state.start++
	err := duckdb.SetRowValue(row, 0, state.start)
	return true, err
}

func (udf *parallelIncrementTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *parallelIncrementTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *parallelIncrementTableUDF) Cardinality() *duckdb.CardinalityInfo {
	return nil
}

func (udf *parallelIncrementTableUDF) GetFunction() duckdb.ParallelRowTableFunction {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)

	return duckdb.ParallelRowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: bindParallelTableUDF,
	}
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	check(err)

	conn, err := db.Conn(context.Background())
	check(err)

	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	udf := duckdb.ParallelRowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: bindParallelTableUDF,
	}

	err = duckdb.RegisterTableUDF(conn, "increment", udf)
	check(err)

	rows, err := db.QueryContext(context.Background(), `SELECT * FROM increment(10000)`)
	check(err)

	// Get the column names.
	columns, err := rows.Columns()
	check(err)

	values := make([]interface{}, len(columns))
	args := make([]interface{}, len(values))
	for i := range values {
		args[i] = &values[i]
	}

	rowSum := int64(0)
	for rows.Next() {
		err = rows.Scan(args...)
		check(err)
		for _, value := range values {
			// Keep in mind that the value could be nil for NULL values.
			// This never happens in our case, so we don't check for it.
			rowSum += value.(int64)
		}
	}
	fmt.Printf("row sum: %d", rowSum)

	check(rows.Close())
	check(conn.Close())
	check(db.Close())
}

func check(err interface{}) {
	if err != nil {
		panic(err)
	}
}
