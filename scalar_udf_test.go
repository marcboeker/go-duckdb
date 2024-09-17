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

func (udf *simpleScalarUDF) Config() ScalarFunctionConfig {
	info, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	return ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{info, info},
		ResultTypeInfo: info,
	}
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

func (udf *allTypesScalarUDF) Config() ScalarFunctionConfig {
	return ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{currentType},
		ResultTypeInfo: currentType,
	}
}

func (udf *allTypesScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

func TestAllTypesScalarUDF(t *testing.T) {
	typeInfos := getTypeInfos(t, false)
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
		if info.TypeInfo.InternalType() != TYPE_UUID {
			require.Equal(t, info.output, msg, fmt.Sprintf(`output does not match expected output, input: %s`, info.input))
		} else {
			require.NotEqual(t, "", msg, "uuid empty")
		}

		require.NoError(t, c.Close())
		require.NoError(t, db.Close())
	}
}

type errNilInputScalarUDF struct{}

func (udf *errNilInputScalarUDF) Config() ScalarFunctionConfig {
	info, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	return ScalarFunctionConfig{
		ResultTypeInfo: info,
	}
}

func (udf *errNilInputScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

type errEmptyInputScalarUDF struct{}

func (udf *errEmptyInputScalarUDF) Config() ScalarFunctionConfig {
	info, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	return ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{},
		ResultTypeInfo: info,
	}
}

func (udf *errEmptyInputScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

type errInputIsNilScalarUDF struct{}

func (udf *errInputIsNilScalarUDF) Config() ScalarFunctionConfig {
	info, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	return ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{nil},
		ResultTypeInfo: info,
	}
}

func (udf *errInputIsNilScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

type errResultIsNilScalarUDF struct{}

func (udf *errResultIsNilScalarUDF) Config() ScalarFunctionConfig {
	info, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		panic(err)
	}
	return ScalarFunctionConfig{
		InputTypeInfos: []TypeInfo{info},
	}
}

func (udf *errResultIsNilScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

type errExecScalarUDF struct{}

func (udf *errExecScalarUDF) Config() ScalarFunctionConfig {
	scalarUDF := simpleScalarUDF{}
	return scalarUDF.Config()
}

func (udf *errExecScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], errors.New("invalid execution")
}

func TestScalarUDFErrors(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	// Empty name.
	var emptyNameUDF *simpleScalarUDF
	err = RegisterScalarUDF(c, "", emptyNameUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoName.Error())

	// Invalid input parameters.

	var errNilInputUDF *errNilInputScalarUDF
	err = RegisterScalarUDF(c, "err_nil_input", errNilInputUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNilInputTypes.Error())

	var errEmptyInputUDF *errEmptyInputScalarUDF
	err = RegisterScalarUDF(c, "err_empty_input", errEmptyInputUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFEmptyInputTypes.Error())

	var errInputIsNilUDF *errInputIsNilScalarUDF
	err = RegisterScalarUDF(c, "err_input_type_is_nil", errInputIsNilUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFInputTypeIsNil.Error())

	var errResultIsNil *errResultIsNilScalarUDF
	err = RegisterScalarUDF(c, "err_result_type_is_nil", errResultIsNil)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsNil.Error())

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
