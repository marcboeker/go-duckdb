package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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

	var udf *simpleScalarUDF
	err = RegisterScalarUDF(c, "my_sum", udf)
	require.NoError(t, err)

	var msg int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 52, msg)

	require.NoError(t, c.Close())
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

		var udf *allTypesScalarUDF
		err = RegisterScalarUDF(c, "my_identity", udf)
		require.NoError(t, err)

		var msg string
		row := db.QueryRow(fmt.Sprintf(`SELECT my_identity(%s)::VARCHAR AS msg`, info.input))
		require.NoError(t, row.Scan(&msg))
		if info.TypeInfo.t != TYPE_UUID {
			require.Equal(t, info.output, msg, fmt.Sprintf(`output does not match expected output, input: %s`, info.input))
		} else {
			require.NotEqual(t, "", msg, "uuid empty")
		}

		require.NoError(t, c.Close())
		require.NoError(t, db.Close())
	}
}

type errConfigScalarUDF struct{}

func (udf *errConfigScalarUDF) Config() (ScalarFunctionConfig, error) {
	return ScalarFunctionConfig{}, errors.New("invalid configuration")
}

func (udf *errConfigScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	scalarUDF := allTypesScalarUDF{}
	return scalarUDF.ExecuteRow(args)
}

type errExecScalarUDF struct{}

func (udf *errExecScalarUDF) Config() (ScalarFunctionConfig, error) {
	scalarUDF := simpleScalarUDF{}
	return scalarUDF.Config()
}

func (udf *errExecScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], errors.New("invalid execution")
}

func TestScalarUDFErrors(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	// Invalid configuration.
	var errConfigUDF *errConfigScalarUDF
	err = RegisterScalarUDF(c, "err_config", errConfigUDF)
	testError(t, err, errAPI.Error())

	// Error during execution.
	var errExecUDF *errExecScalarUDF
	err = RegisterScalarUDF(c, "err_exec", errExecUDF)
	require.NoError(t, err)

	row := db.QueryRow(`SELECT err_exec(10, 10) AS msg`)
	testError(t, row.Err(), errAPI.Error())

	// Register the same scalar function a second time.
	// Since RegisterScalarUDF takes ownership of udf, we are now passing nil.
	var udf *simpleScalarUDF
	err = RegisterScalarUDF(c, "my_sum", udf)
	require.NoError(t, err)
	err = RegisterScalarUDF(c, "my_sum", udf)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function whose name already exists.
	var udfDuplicateName *simpleScalarUDF
	err = RegisterScalarUDF(c, "my_sum", udfDuplicateName)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function that is nil.
	err = RegisterScalarUDF(c, "my_sum", nil)
	testError(t, err, errAPI.Error(), errScalarUDFIsNil.Error())

	require.NoError(t, c.Close())

	// Test registering the scalar function on a closed connection.
	var udfOnClosedCon *simpleScalarUDF
	err = RegisterScalarUDF(c, "closed_con", udfOnClosedCon)
	require.ErrorContains(t, err, sql.ErrConnDone.Error())

	require.NoError(t, db.Close())
}
