package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

type simpleScalarUDF struct{}

func (udf *simpleScalarUDF) Config() (ScalarFunctionConfig, error) {
	var config ScalarFunctionConfig

	intTypeInp, err := PrimitiveTypeInfo(TYPE_INTEGER)
	if err != nil {
		return config, err
	}

	config.InputTypeInfos = []TypeInfo{intTypeInp, intTypeInp}
	config.ResultTypeInfo = intTypeInp
	return config, nil
}

func (udf *simpleScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	val := args[0].(int32) + args[1].(int32)
	return val, nil
}

func TestSimpleScalarUDF(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	var udf simpleScalarUDF
	err = RegisterScalarUDF(c, "my_sum", &udf)
	require.NoError(t, err)

	var msg int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 52, msg)
	require.NoError(t, db.Close())
}

func TestAllTypesInScalarUDF(t *testing.T) {

}

// TODO: test other primitive data types

func TestScalarUDFErrors(t *testing.T) {
	// TODO: trigger all possible errors and move to errors_test.go
}

func TestScalarUDFNested(t *testing.T) {
	// TODO: test nested data types
}
