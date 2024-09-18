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

var currentInfo TypeInfo

type simpleScalarUDF struct{}

type simpleScalarUDFConfig struct{}

func (*simpleScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo, currentInfo}
}

func (*simpleScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*simpleScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*simpleScalarUDFConfig) Volatile() bool {
	return false
}

func (*simpleScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*simpleScalarUDF) Config() ScalarFunctionConfig {
	return &simpleScalarUDFConfig{}
}

func (*simpleScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil
	}
	val := args[0].(int32) + args[1].(int32)
	return val, nil
}

func TestSimpleScalarUDF(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *simpleScalarUDF
	err = RegisterScalarUDF(c, "my_sum", udf)
	require.NoError(t, err)

	var msg *int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, 52, *msg)

	row = db.QueryRow(`SELECT my_sum(NULL, 42) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, (*int)(nil), msg)

	row = db.QueryRow(`SELECT my_sum(42, NULL) AS msg`)
	require.NoError(t, row.Scan(&msg))
	require.Equal(t, (*int)(nil), msg)

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

type allTypesScalarUDF struct{}

type allTypesScalarUDFConfig struct{}

func (*allTypesScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*allTypesScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*allTypesScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*allTypesScalarUDFConfig) Volatile() bool {
	return false
}

func (*allTypesScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*allTypesScalarUDF) Config() ScalarFunctionConfig {
	return &allTypesScalarUDFConfig{}
}

func (*allTypesScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	return args[0], nil
}

func TestAllTypesScalarUDF(t *testing.T) {
	typeInfos := getTypeInfos(t, false)
	for _, info := range typeInfos {
		currentInfo = info.TypeInfo

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

type variadicScalarUDF struct{}

type variadicScalarUDFConfig struct{}

func (*variadicScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return nil
}

func (*variadicScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*variadicScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return currentInfo
}

func (*variadicScalarUDFConfig) Volatile() bool {
	return true
}

func (*variadicScalarUDFConfig) SpecialNullHandling() bool {
	return true
}

func (*variadicScalarUDF) Config() ScalarFunctionConfig {
	return &variadicScalarUDFConfig{}
}

func (*variadicScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	sum := int32(0)
	for _, val := range args {
		if val == nil {
			return nil, nil
		}
		sum += val.(int32)
	}
	return sum, nil
}

func TestScalarUDFSet(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf1 *simpleScalarUDF
	var udf2 *allTypesScalarUDF
	err = RegisterScalarUDFSet(c, "my_addition", udf1, udf2)
	require.NoError(t, err)

	var sum int
	row := db.QueryRow(`SELECT my_addition(10, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 52, sum)

	row = db.QueryRow(`SELECT my_addition(42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 42, sum)

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

func TestVariadicScalarUDF(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *variadicScalarUDF
	err = RegisterScalarUDF(c, "my_variadic_sum", udf)
	require.NoError(t, err)

	var sum *int
	row := db.QueryRow(`SELECT my_variadic_sum(10, NULL, NULL) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_variadic_sum(10, 42, 2, 2, 2) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 58, *sum)

	row = db.QueryRow(`SELECT my_variadic_sum(10) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 10, *sum)

	row = db.QueryRow(`SELECT my_variadic_sum(NULL) AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_variadic_sum() AS msg`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 0, *sum)

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

type anyScalarUDF struct{}

type anyScalarUDFConfig struct{}

func (*anyScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return nil
}

func (*anyScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*anyScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}
	return info
}

func (*anyScalarUDFConfig) Volatile() bool {
	return true
}

func (*anyScalarUDFConfig) SpecialNullHandling() bool {
	return true
}

func (*anyScalarUDF) Config() ScalarFunctionConfig {
	return &anyScalarUDFConfig{}
}

func (*anyScalarUDF) ExecuteRow(args []driver.Value) (any, error) {
	count := int32(0)
	for _, val := range args {
		if val == nil {
			count++
		}
	}
	return count, nil
}

func TestANYScalarUDF(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *anyScalarUDF
	err = RegisterScalarUDF(c, "my_null_count", udf)
	require.NoError(t, err)

	var count int
	row := db.QueryRow(`SELECT my_null_count(10, 'hello', 2, [2], 2) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	row = db.QueryRow(`SELECT my_null_count(10, NULL, NULL, [NULL], {'hello': NULL}) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 2, count)

	row = db.QueryRow(`SELECT my_null_count(10, True) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	row = db.QueryRow(`SELECT my_null_count(NULL) AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 1, count)

	row = db.QueryRow(`SELECT my_null_count() AS msg`)
	require.NoError(t, row.Scan(&count))
	require.Equal(t, 0, count)

	require.NoError(t, c.Close())
	require.NoError(t, db.Close())
}

type errNilInputScalarUDF struct{}

type errNilInputScalarUDFConfig struct{}

func (*errNilInputScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return nil
}

func (*errNilInputScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*errNilInputScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*errNilInputScalarUDFConfig) Volatile() bool {
	return false
}

func (*errNilInputScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*errNilInputScalarUDF) Config() ScalarFunctionConfig {
	return &errNilInputScalarUDFConfig{}
}

func (*errNilInputScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errEmptyInputScalarUDF struct{}

type errEmptyInputScalarUDFConfig struct{}

func (*errEmptyInputScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{}
}

func (*errEmptyInputScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*errEmptyInputScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*errEmptyInputScalarUDFConfig) Volatile() bool {
	return false
}

func (*errEmptyInputScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*errEmptyInputScalarUDF) Config() ScalarFunctionConfig {
	return &errEmptyInputScalarUDFConfig{}
}

func (*errEmptyInputScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errInputIsNilScalarUDF struct{}

type errInputIsNilScalarUDFConfig struct{}

func (*errInputIsNilScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{nil}
}

func (*errInputIsNilScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*errInputIsNilScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*errInputIsNilScalarUDFConfig) Volatile() bool {
	return false
}

func (*errInputIsNilScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*errInputIsNilScalarUDF) Config() ScalarFunctionConfig {
	return &errInputIsNilScalarUDFConfig{}
}

func (*errInputIsNilScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errResultIsNilScalarUDF struct{}

type errResultIsNilScalarUDFConfig struct{}

func (*errResultIsNilScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*errResultIsNilScalarUDFConfig) ResultTypeInfo() TypeInfo {
	return nil
}

func (*errResultIsNilScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*errResultIsNilScalarUDFConfig) Volatile() bool {
	return false
}

func (*errResultIsNilScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*errResultIsNilScalarUDF) Config() ScalarFunctionConfig {
	return &errResultIsNilScalarUDFConfig{}
}

func (*errResultIsNilScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errResultIsANYScalarUDF struct{}

type errResultIsANYScalarUDFConfig struct{}

func (*errResultIsANYScalarUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*errResultIsANYScalarUDFConfig) ResultTypeInfo() TypeInfo {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}
	return info
}

func (*errResultIsANYScalarUDFConfig) VariadicTypeInfo() TypeInfo {
	return nil
}

func (*errResultIsANYScalarUDFConfig) Volatile() bool {
	return false
}

func (*errResultIsANYScalarUDFConfig) SpecialNullHandling() bool {
	return false
}

func (*errResultIsANYScalarUDF) Config() ScalarFunctionConfig {
	return &errResultIsANYScalarUDFConfig{}
}

func (*errResultIsANYScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errExecScalarUDF struct{}

func (*errExecScalarUDF) Config() ScalarFunctionConfig {
	scalarUDF := simpleScalarUDF{}
	return scalarUDF.Config()
}

func (*errExecScalarUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, errors.New("test invalid execution")
}

func TestScalarUDFErrors(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
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

	// Invalid result parameters.

	var errResultIsNil *errResultIsNilScalarUDF
	err = RegisterScalarUDF(c, "err_result_type_is_nil", errResultIsNil)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsNil.Error())

	var errResultIsANY *errResultIsANYScalarUDF
	err = RegisterScalarUDF(c, "err_result_type_is_any", errResultIsANY)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsANY.Error())

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
