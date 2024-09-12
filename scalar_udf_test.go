package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
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

type allTypesScalarUDF struct{}

var currentType TypeInfo

func (udf *allTypesScalarUDF) Config() (ScalarFunctionConfig, error) {
	config := ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{currentType},
		ResultTypeInfo: currentType,
	}
	return config, nil
}

func (udf *allTypesScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

func TestAllTypesScalarUDF(t *testing.T) {
	typeInfos := getTypeInfos(t)
	for _, info := range typeInfos {
		currentType = info.TypeInfo

		db, err := sql.Open("duckdb", "")
		require.NoError(t, err)

		c, err := db.Conn(context.Background())
		require.NoError(t, err)

		_, err = c.ExecContext(context.Background(), `CREATE TYPE greeting AS ENUM ('hello', 'world')`)
		require.NoError(t, err)

		var udf allTypesScalarUDF
		err = RegisterScalarUDF(c, "my_identity", &udf)
		require.NoError(t, err)

		var msg string
		row := db.QueryRow(fmt.Sprintf(`SELECT my_identity(%s)::VARCHAR AS msg`, info.input))
		require.NoError(t, row.Scan(&msg))
		if info.TypeInfo.t != TYPE_UUID {
			require.Equal(t, info.output, msg, fmt.Sprintf(`output does not match expected output, input: %s`, info.input))
		} else {
			require.NotEqual(t, "", msg, "uuid empty")
		}

		require.NoError(t, db.Close())
	}
}

func TestScalarUDFErrors(t *testing.T) {
	// TODO: trigger all possible errors and move to errors_test.go
	// TODO: especially test trying to register same name twice
}

func TestScalarUDFNested(t *testing.T) {
	// TODO: test nested data types
}
