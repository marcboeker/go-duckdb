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

type (
	simpleSUDF        struct{}
	constantSUDF      struct{}
	otherConstantSUDF struct{}
	typesSUDF         struct{}
	variadicSUDF      struct{}
	anyTypeSUDF       struct{}
	errExecutorSUDF   struct{}
	errInputNilSUDF   struct{}
	errResultNilSUDF  struct{}
	errResultAnySUDF  struct{}
	errExecSUDF       struct{}
)

func simpleSum(values []driver.Value) (any, error) {
	if values[0] == nil || values[1] == nil {
		return nil, nil
	}
	val := values[0].(int32) + values[1].(int32)
	return val, nil
}

func constantOne([]driver.Value) (any, error) {
	return int32(1), nil
}

func identity(values []driver.Value) (any, error) {
	return values[0], nil
}

func variadicSum(values []driver.Value) (any, error) {
	sum := int32(0)
	for _, val := range values {
		if val == nil {
			return nil, nil
		}
		sum += val.(int32)
	}
	return sum, nil
}

func nilCount(values []driver.Value) (any, error) {
	count := int32(0)
	for _, val := range values {
		if val == nil {
			count++
		}
	}
	return count, nil
}

func constantError([]driver.Value) (any, error) {
	return nil, errors.New("test invalid execution")
}

func (*simpleSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo, currentInfo}, currentInfo, nil, false, false}
}

func (*simpleSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{simpleSum}
}

func (*constantSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{ResultTypeInfo: currentInfo}
}

func (*constantSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantOne}
}

func (*otherConstantSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{}, currentInfo, nil, false, false}
}

func (*otherConstantSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantOne}
}

func (*typesSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo}, currentInfo, nil, false, false}
}

func (*typesSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{identity}
}

func (*variadicSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{nil, currentInfo, currentInfo, true, true}
}

func (*variadicSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{variadicSum}
}

func (*anyTypeSUDF) Config() ScalarFuncConfig {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}

	return ScalarFuncConfig{nil, currentInfo, info, false, true}
}

func (*anyTypeSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{nilCount}
}

func (*errExecutorSUDF) Config() ScalarFuncConfig {
	scalarUDF := simpleSUDF{}
	return scalarUDF.Config()
}

func (*errExecutorSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{nil}
}

func (*errInputNilSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{nil}, currentInfo, nil, false, false}
}

func (*errInputNilSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantOne}
}

func (*errResultNilSUDF) Config() ScalarFuncConfig {
	return ScalarFuncConfig{[]TypeInfo{currentInfo}, nil, nil, false, false}
}

func (*errResultNilSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantOne}
}

func (*errResultAnySUDF) Config() ScalarFuncConfig {
	info, err := NewTypeInfo(TYPE_ANY)
	if err != nil {
		panic(err)
	}

	return ScalarFuncConfig{[]TypeInfo{currentInfo}, info, nil, false, false}
}

func (*errResultAnySUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantOne}
}

func (*errExecSUDF) Config() ScalarFuncConfig {
	scalarUDF := simpleSUDF{}
	return scalarUDF.Config()
}

func (*errExecSUDF) Executor() ScalarFuncExecutor {
	return ScalarFuncExecutor{constantError}
}

func TestSimpleScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf *simpleSUDF
	err := RegisterScalarUDF(conn, "my_sum", udf)
	require.NoError(t, err)

	var sum *int
	row := db.QueryRow(`SELECT my_sum(10, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 52, *sum)

	row = db.QueryRow(`SELECT my_sum(NULL, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)

	row = db.QueryRow(`SELECT my_sum(42, NULL) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, (*int)(nil), sum)
}

func TestConstantScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf *constantSUDF
	err := RegisterScalarUDF(conn, "constant_one", udf)
	require.NoError(t, err)

	var otherUDF *otherConstantSUDF
	err = RegisterScalarUDF(conn, "other_constant_one", otherUDF)
	require.NoError(t, err)

	var one int
	row := db.QueryRow(`SELECT constant_one() AS one`)
	require.NoError(t, row.Scan(&one))
	require.Equal(t, 1, one)

	row = db.QueryRow(`SELECT other_constant_one() AS one`)
	require.NoError(t, row.Scan(&one))
	require.Equal(t, 1, one)
}

func TestAllTypesScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	typeInfos := getTypeInfos(t, false)
	for _, info := range typeInfos {
		currentInfo = info.TypeInfo

		db := openDbWrapper(t, ``)
		conn := openConnWrapper(t, db, context.Background())

		_, err := conn.ExecContext(context.Background(), `CREATE TYPE greeting AS ENUM ('hello', 'world')`)
		require.NoError(t, err)

		var udf *typesSUDF
		err = RegisterScalarUDF(conn, "my_identity", udf)
		require.NoError(t, err)

		var res string
		row := db.QueryRow(fmt.Sprintf(`SELECT my_identity(%s)::VARCHAR AS res`, info.input))
		require.NoError(t, row.Scan(&res))
		if info.TypeInfo.InternalType() != TYPE_UUID {
			require.Equal(t, info.output, res, `output does not match expected output, input: %s`, info.input)
		} else {
			require.NotEqual(t, "", res, "uuid empty")
		}

		closeConnWrapper(t, conn)
		closeDbWrapper(t, db)
	}
}

func TestScalarUDFSet(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf1 *simpleSUDF
	var udf2 *typesSUDF
	err := RegisterScalarUDFSet(conn, "my_addition", udf1, udf2)
	require.NoError(t, err)

	var sum int
	row := db.QueryRow(`SELECT my_addition(10, 42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 52, sum)

	row = db.QueryRow(`SELECT my_addition(42) AS sum`)
	require.NoError(t, row.Scan(&sum))
	require.Equal(t, 42, sum)
}

func TestVariadicScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf *variadicSUDF
	err := RegisterScalarUDF(conn, "my_variadic_sum", udf)
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
}

func TestANYScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf *anyTypeSUDF
	err := RegisterScalarUDF(conn, "my_null_count", udf)
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
}

func TestErrScalarUDF(t *testing.T) {
	defer apiVerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())

	// Empty name.
	var emptyNameUDF *simpleSUDF
	err := RegisterScalarUDF(conn, "", emptyNameUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoName.Error())

	// Invalid executor.
	var errExecutorUDF *errExecutorSUDF
	err = RegisterScalarUDF(conn, "err_executor_is_nil", errExecutorUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFNoExecutor.Error())

	// Invalid input parameter.
	var errInputNilUDF *errInputNilSUDF
	err = RegisterScalarUDF(conn, "err_input_type_is_nil", errInputNilUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFInputTypeIsNil.Error())

	// Invalid result parameters.
	var errResultNil *errResultNilSUDF
	err = RegisterScalarUDF(conn, "err_result_type_is_nil", errResultNil)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsNil.Error())
	var errResultAny *errResultAnySUDF
	err = RegisterScalarUDF(conn, "err_result_type_is_any", errResultAny)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error(), errScalarUDFResultTypeIsANY.Error())

	// Error during execution.
	var errExecUDF *errExecSUDF
	err = RegisterScalarUDF(conn, "err_exec", errExecUDF)
	require.NoError(t, err)
	row := db.QueryRow(`SELECT err_exec(10, 10) AS res`)
	testError(t, row.Err(), errAPI.Error())

	// Register the same scalar function a second time.
	// Since RegisterScalarUDF takes ownership of udf, we are now passing nil.
	var udf *simpleSUDF
	err = RegisterScalarUDF(conn, "my_sum", udf)
	require.NoError(t, err)
	err = RegisterScalarUDF(conn, "my_sum", udf)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function whose name already exists.
	var errDuplicateUDF *simpleSUDF
	err = RegisterScalarUDF(conn, "my_sum", errDuplicateUDF)
	testError(t, err, errAPI.Error(), errScalarUDFCreate.Error())

	// Register a scalar function that is nil.
	err = RegisterScalarUDF(conn, "my_sum", nil)
	testError(t, err, errAPI.Error(), errScalarUDFIsNil.Error())

	// Test registering the scalar function on a closed connection.
	closeConnWrapper(t, conn)
	var errClosedConUDF *simpleSUDF
	err = RegisterScalarUDF(conn, "closed_con", errClosedConUDF)
	require.ErrorContains(t, err, sql.ErrConnDone.Error())
}
