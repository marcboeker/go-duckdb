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

type simpleSUDF struct{}

type simpleSUDFConfig struct{}

func (*simpleSUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo, currentInfo}
}

func (*simpleSUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*simpleSUDF) Config() ScalarFuncConfig {
	return &simpleSUDFConfig{}
}

func (*simpleSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*simpleSUDF) ExecuteRow(args []driver.Value) (any, error) {
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

	var udf *simpleSUDF
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

type typesSUDF struct{}

type typesSUDFConfig struct{}

func (*typesSUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*typesSUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*typesSUDF) Config() ScalarFuncConfig {
	return &typesSUDFConfig{}
}

func (*typesSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*typesSUDF) ExecuteRow(args []driver.Value) (any, error) {
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

		var udf *typesSUDF
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

func TestScalarUDFSet(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf1 *simpleSUDF
	var udf2 *typesSUDF
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

type variadicSUDF struct{}

type variadicSUDFConfig struct{}

type variadicSUDFExtraInfo struct{}

func (*variadicSUDFConfig) InputTypeInfos() []TypeInfo {
	return nil
}

func (*variadicSUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*variadicSUDFExtraInfo) VariadicTypeInfo() TypeInfo {
	return currentInfo
}

func (*variadicSUDFExtraInfo) Volatile() bool {
	return true
}

func (*variadicSUDFExtraInfo) SpecialNullHandling() bool {
	return true
}

func (*variadicSUDF) Config() ScalarFuncConfig {
	return &variadicSUDFConfig{}
}

func (*variadicSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return &variadicSUDFExtraInfo{}
}

func (*variadicSUDF) ExecuteRow(args []driver.Value) (any, error) {
	sum := int32(0)
	for _, val := range args {
		if val == nil {
			return nil, nil
		}
		sum += val.(int32)
	}
	return sum, nil
}

func TestVariadicScalarUDF(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	c, err := db.Conn(context.Background())
	require.NoError(t, err)

	currentInfo, err = NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	var udf *variadicSUDF
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

type anyTypeSUDF struct{}

type anyTypeSUDFExtraInfo struct{}

func (*anyTypeSUDFExtraInfo) VariadicTypeInfo() TypeInfo {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}
	return info
}

func (*anyTypeSUDFExtraInfo) Volatile() bool {
	return true
}

func (*anyTypeSUDFExtraInfo) SpecialNullHandling() bool {
	return true
}

func (*anyTypeSUDF) Config() ScalarFuncConfig {
	return &variadicSUDFConfig{}
}

func (*anyTypeSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return &anyTypeSUDFExtraInfo{}
}

func (*anyTypeSUDF) ExecuteRow(args []driver.Value) (any, error) {
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

	var udf *anyTypeSUDF
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

type errInputSUDF struct{}

func (*errInputSUDF) Config() ScalarFuncConfig {
	return &variadicSUDFConfig{}
}

func (*errInputSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errInputSUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errEmptyInputSUDF struct{}

type errEmptyInputSUDFConfig struct{}

func (*errEmptyInputSUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{}
}

func (*errEmptyInputSUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*errEmptyInputSUDF) Config() ScalarFuncConfig {
	return &errEmptyInputSUDFConfig{}
}

func (*errEmptyInputSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errEmptyInputSUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errInputNilSUDF struct{}

type errInputNilSUDFConfig struct{}

func (*errInputNilSUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{nil}
}

func (*errInputNilSUDFConfig) ResultTypeInfo() TypeInfo {
	return currentInfo
}

func (*errInputNilSUDF) Config() ScalarFuncConfig {
	return &errInputNilSUDFConfig{}
}

func (*errInputNilSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errInputNilSUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errResultNilSUDF struct{}

type errResultNilSUDFConfig struct{}

func (*errResultNilSUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*errResultNilSUDFConfig) ResultTypeInfo() TypeInfo {
	return nil
}

func (*errResultNilSUDF) Config() ScalarFuncConfig {
	return &errResultNilSUDFConfig{}
}

func (*errResultNilSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errResultNilSUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errResultAnySUDF struct{}

type errResultAnySUDFConfig struct{}

func (*errResultAnySUDFConfig) InputTypeInfos() []TypeInfo {
	return []TypeInfo{currentInfo}
}

func (*errResultAnySUDFConfig) ResultTypeInfo() TypeInfo {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}
	return info
}

func (*errResultAnySUDF) Config() ScalarFuncConfig {
	return &errResultAnySUDFConfig{}
}

func (*errResultAnySUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errResultAnySUDF) ExecuteRow([]driver.Value) (any, error) {
	return nil, nil
}

type errExecSUDF struct{}

func (*errExecSUDF) Config() ScalarFuncConfig {
	scalarUDF := simpleSUDF{}
	return scalarUDF.Config()
}

func (*errExecSUDF) ExtraInfo() ScalarFuncExtraInfo {
	return nil
}

func (*errExecSUDF) ExecuteRow([]driver.Value) (any, error) {
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
	var emptyNameUDF *simpleSUDF
	err = RegisterScalarUDF(c, "", emptyNameUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoName.Error())

	// Invalid input parameters.

	var errInputUDF *errInputSUDF
	err = RegisterScalarUDF(c, "err_input", errInputUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNilInputTypes.Error())

	var errEmptyInputUDF *errEmptyInputSUDF
	err = RegisterScalarUDF(c, "err_empty_input", errEmptyInputUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFEmptyInputTypes.Error())

	var errInputNilUDF *errInputNilSUDF
	err = RegisterScalarUDF(c, "err_input_type_is_nil", errInputNilUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFInputTypeIsNil.Error())

	// Invalid result parameters.

	var errResultNil *errResultNilSUDF
	err = RegisterScalarUDF(c, "err_result_type_is_nil", errResultNil)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsNil.Error())

	var errResultAny *errResultAnySUDF
	err = RegisterScalarUDF(c, "err_result_type_is_any", errResultAny)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsANY.Error())

	// Error during execution.
	var errExecUDF *errExecSUDF
	err = RegisterScalarUDF(c, "err_exec", errExecUDF)
	require.NoError(t, err)
	row := db.QueryRow(`SELECT err_exec(10, 10) AS msg`)
	testError(t, row.Err(), errAPI.Error())

	// Register the same scalar function a second time.
	// Since RegisterScalarUDF takes ownership of udf, we are now passing nil.
	var udf *simpleSUDF
	err = RegisterScalarUDF(c, "my_sum", udf)
	require.NoError(t, err)
	err = RegisterScalarUDF(c, "my_sum", udf)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function whose name already exists.
	var errDuplicateUDF *simpleSUDF
	err = RegisterScalarUDF(c, "my_sum", errDuplicateUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function that is nil.
	err = RegisterScalarUDF(c, "my_sum", nil)
	testError(t, err, errAPI.Error(), errScalarUDFIsNil.Error())
	require.NoError(t, c.Close())

	// Test registering the scalar function on a closed connection.
	var errClosedConUDF *simpleSUDF
	err = RegisterScalarUDF(c, "closed_con", errClosedConUDF)
	require.ErrorContains(t, err, sql.ErrConnDone.Error())
	require.NoError(t, db.Close())
}
