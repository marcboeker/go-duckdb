package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/marcboeker/go-duckdb"
)

type incrementTableUDF struct {
	tableSize  int64
	currentRow int64
}

func bindTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	return &incrementTableUDF{
		currentRow: 0,
		tableSize:  args[0].(int64),
	}, nil
}

func (udf *incrementTableUDF) ColumnInfos() []duckdb.ColumnInfo {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	return []duckdb.ColumnInfo{{Name: "result", T: t}}
}

func (udf *incrementTableUDF) Init() {}

func (udf *incrementTableUDF) FillRow(row duckdb.Row) (bool, error) {
	if udf.currentRow+1 > udf.tableSize {
		return false, nil
	}
	udf.currentRow++
	err := duckdb.SetRowValue(row, 0, udf.currentRow)
	return true, err
}

func (udf *incrementTableUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(udf.tableSize),
		Exact:       true,
	}
}

func main() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	check(err)

	conn, err := db.Conn(context.Background())
	check(err)

	t, err := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	check(err)
	udf := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{t},
		},
		BindArguments: bindTableUDF,
	}

	err = duckdb.RegisterTableUDF(conn, "increment", udf)
	check(err)

	rows, err := db.QueryContext(context.Background(), `SELECT * FROM increment(100)`)
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
			// Keep in mind that the value can be nil for NULL values.
			// This never happens in this example, so we don't check for it.
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
