package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/marcboeker/go-duckdb/v2"
)

// Overload my_length with two user-defined scalar functions.
// varcharLen takes a VARCHAR as its input parameter.
// listLen takes a LIST(ANY) as its input parameter.

type (
	varcharLen struct{}
	listLen    struct{}
)

func varcharLenFn(values []driver.Value) (any, error) {
	str := values[0].(string)
	return int32(len(str)), nil
}

func (*varcharLen) Config() duckdb.ScalarFuncConfig {
	inputTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	check(err)
	resultTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	check(err)

	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{inputTypeInfo},
		ResultTypeInfo: resultTypeInfo,
	}
}

func (*varcharLen) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: varcharLenFn}
}

func listLenFn(values []driver.Value) (any, error) {
	list := values[0].([]any)
	return int32(len(list)), nil
}

func (*listLen) Config() duckdb.ScalarFuncConfig {
	anyTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	check(err)
	inputTypeInfo, err := duckdb.NewListInfo(anyTypeInfo)
	check(err)
	resultTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	check(err)

	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{inputTypeInfo},
		ResultTypeInfo: resultTypeInfo,
	}
}

func (*listLen) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: listLenFn}
}

func myLengthScalarUDFSet() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	check(err)

	c, err := db.Conn(context.Background())
	check(err)

	var varcharUDF *varcharLen
	var listUDF *listLen
	err = duckdb.RegisterScalarUDFSet(c, "my_length", varcharUDF, listUDF)
	check(err)

	var length int32
	row := db.QueryRow(`SELECT my_length('hello world') AS sum`)
	check(row.Scan(&length))
	if length != 11 {
		panic(errors.New("incorrect length"))
	}

	row = db.QueryRow(`SELECT my_length([1, 2, NULL, 4, NULL]) AS sum`)
	check(row.Scan(&length))
	if length != 5 {
		panic(errors.New("incorrect length"))
	}

	check(c.Close())
	check(db.Close())
}

// wrapSum takes a VARCHAR prefix, a VARCHAR suffix, and a variadic number of integer values.
// It computes the sum of the integer values. Then, it emits a VARCHAR by concatenating prefix || sum || suffix.

type wrapSum struct{}

func wrapSumFn(values []driver.Value) (any, error) {
	sum := int32(0)
	for i := 2; i < len(values); i++ {
		sum += values[i].(int32)
	}
	strSum := fmt.Sprintf("%d", sum)
	prefix := values[0].(string)
	suffix := values[1].(string)
	return prefix + strSum + suffix, nil
}

func (*wrapSum) Config() duckdb.ScalarFuncConfig {
	varcharTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	check(err)
	intTypeInfo, err := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	check(err)

	return duckdb.ScalarFuncConfig{
		InputTypeInfos:   []duckdb.TypeInfo{varcharTypeInfo, varcharTypeInfo},
		ResultTypeInfo:   varcharTypeInfo,
		VariadicTypeInfo: intTypeInfo,
	}
}

func (*wrapSum) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: wrapSumFn}
}

func wrapSumScalarUDF() {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	check(err)

	c, err := db.Conn(context.Background())
	check(err)

	var wrapSumUDF *wrapSum
	err = duckdb.RegisterScalarUDF(c, "wrap_sum", wrapSumUDF)
	check(err)

	var res string
	row := db.QueryRow(`SELECT wrap_sum('hello', ' world', 1, 2, 3, 4) AS sum`)
	check(row.Scan(&res))
	if res != "hello10 world" {
		panic(errors.New("incorrect result"))
	}

	row = db.QueryRow(`SELECT wrap_sum('hello', ' world') AS sum`)
	check(row.Scan(&res))
	if res != "hello0 world" {
		panic(errors.New("incorrect result"))
	}

	check(c.Close())
	check(db.Close())
}

func main() {
	myLengthScalarUDFSet()
	wrapSumScalarUDF()
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
