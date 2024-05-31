package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type scalarUDF struct {
	err error
}

func (udf *scalarUDF) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypes: []string{"INT", "INT"},
		ResultType: "INT",
	}
}

func (udf *scalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	if len(args) != 2 {
		return nil, errors.New("expected two values")
	}
	val := args[0].(int32) + args[1].(int32)
	return val, nil
}

func (udf *scalarUDF) SetError(err error) {
	udf.err = err
}

func TestScalarUDFPrimitive(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	var udf scalarUDF
	err = RegisterScalarUDF(c, "my_sum", &udf)

	var msg int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 52, msg)
	require.NoError(t, db.Close())

	// TODO: test other primitive data types
}

func TestScalarUDFErrors(t *testing.T) {
	// TODO: trigger all possible errors and move to errors_test.go
}

func TestScalarUDFNested(t *testing.T) {
	// TODO: test nested data types
}
