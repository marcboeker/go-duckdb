package duckdb

// Related issues: https://golang.org/issue/19835, https://golang.org/issue/19837.

/*
#include <duckdb.h>

void scalar_udf_callback(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
void scalar_udf_delete_callback(void *);

typedef void (*scalar_udf_callback_t)(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"runtime/cgo"
	"unsafe"
)

type ScalarFuncConfig struct {
	InputTypeInfos []TypeInfo
	ResultTypeInfo TypeInfo

	VariadicTypeInfo    *TypeInfo
	Volatile            bool
	SpecialNullHandling bool
}

type ScalarFunc interface {
	Config() ScalarFuncConfig
	ExecuteRow(args []driver.Value) (any, error)
}

func setFuncError(function_info C.duckdb_function_info, msg string) {
	err := C.CString(msg)
	C.duckdb_scalar_function_set_error(function_info, err)
	C.duckdb_free(unsafe.Pointer(err))
}

//export scalar_udf_callback
func scalar_udf_callback(function_info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	extraInfo := C.duckdb_scalar_function_get_extra_info(function_info)

	// extraInfo is a void* pointer to our ScalarFunc.
	h := *(*cgo.Handle)(unsafe.Pointer(extraInfo))
	function := h.Value().(ScalarFunc)

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err := inputChunk.initFromDuckDataChunk(input, false); err != nil {
		setFuncError(function_info, getError(errAPI, err).Error())
		return
	}

	// Initialize the output chunk.
	var outputChunk DataChunk
	if err := outputChunk.initFromDuckVector(output, true); err != nil {
		setFuncError(function_info, getError(errAPI, err).Error())
		return
	}

	// Execute the user-defined scalar function for each row.
	values := make([]driver.Value, len(inputChunk.columns))
	rowCount := inputChunk.GetSize()
	columnCount := len(values)
	var err error

	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		// Set the values for each row.
		for colIdx := 0; colIdx < columnCount; colIdx++ {
			if values[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				setFuncError(function_info, getError(errAPI, err).Error())
				return
			}
		}

		// Execute the function and write the result to the output vector.
		var val any
		if val, err = function.ExecuteRow(values); err != nil {
			break
		}
		if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
			break
		}
	}

	if err != nil {
		setFuncError(function_info, getError(errAPI, err).Error())
	}
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(extraInfo unsafe.Pointer) {
	h := (*cgo.Handle)(extraInfo)
	h.Delete()
}

func registerInputParams(config ScalarFuncConfig, f C.duckdb_scalar_function) error {
	// Set variadic input parameters.
	if config.VariadicTypeInfo != nil {
		t := (*config.VariadicTypeInfo).logicalType()
		C.duckdb_scalar_function_set_varargs(f, t)
		C.duckdb_destroy_logical_type(&t)
		return nil
	}

	// Set normal input parameters.
	if config.InputTypeInfos == nil {
		return errScalarUDFNilInputTypes
	}
	if len(config.InputTypeInfos) == 0 {
		return errScalarUDFEmptyInputTypes
	}

	for i, info := range config.InputTypeInfos {
		if info == nil {
			return addIndexToError(errScalarUDFInputTypeIsNil, i)
		}
		t := info.logicalType()
		C.duckdb_scalar_function_add_parameter(f, t)
		C.duckdb_destroy_logical_type(&t)
	}
	return nil
}

func registerResultParams(config ScalarFuncConfig, f C.duckdb_scalar_function) error {
	if config.ResultTypeInfo == nil {
		return errScalarUDFResultTypeIsNil
	}
	if config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return errScalarUDFResultTypeIsANY
	}
	t := config.ResultTypeInfo.logicalType()
	C.duckdb_scalar_function_set_return_type(f, t)
	C.duckdb_destroy_logical_type(&t)
	return nil
}

func createScalarFunc(name string, f ScalarFunc) (C.duckdb_scalar_function, error) {
	if name == "" {
		return nil, errScalarUDFNoName
	}
	if f == nil {
		return nil, errScalarUDFIsNil
	}
	function := C.duckdb_create_scalar_function()

	// Set the name.
	cName := C.CString(name)
	C.duckdb_scalar_function_set_name(function, cName)
	C.duckdb_free(unsafe.Pointer(cName))

	// Configure the scalar function.
	config := f.Config()
	if err := registerInputParams(config, function); err != nil {
		return nil, err
	}
	if err := registerResultParams(config, function); err != nil {
		return nil, err
	}
	if config.SpecialNullHandling {
		C.duckdb_scalar_function_set_special_handling(function)
	}
	if config.Volatile {
		C.duckdb_scalar_function_set_volatile(function)
	}

	// Set the function callback.
	C.duckdb_scalar_function_set_function(function, C.scalar_udf_callback_t(C.scalar_udf_callback))

	// Set data available during execution.
	h := cgo.NewHandle(f)
	C.duckdb_scalar_function_set_extra_info(
		function,
		unsafe.Pointer(&h),
		C.duckdb_delete_callback_t(C.scalar_udf_delete_callback))

	return function, nil
}

// RegisterScalarUDF registers a scalar user-defined function.
// The function takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error {
	scalarFunc, err := createScalarFunc(name, f)
	if err != nil {
		return getError(errAPI, err)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err = c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		state := C.duckdb_register_scalar_function(con.duckdbCon, scalarFunc)
		C.duckdb_destroy_scalar_function(&scalarFunc)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreate)
		}
		return nil
	})
	return err
}

func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunc) error {
	cName := C.CString(name)
	set := C.duckdb_create_scalar_function_set(cName)
	C.duckdb_free(unsafe.Pointer(cName))

	// Create each function and add it to the set.
	for i, f := range functions {
		scalarFunction, err := createScalarFunc(name, f)
		if err != nil {
			C.duckdb_destroy_scalar_function(&scalarFunction)
			C.duckdb_destroy_scalar_function_set(&set)
			return getError(errAPI, err)
		}

		state := C.duckdb_add_scalar_function_to_set(set, scalarFunction)
		C.duckdb_destroy_scalar_function(&scalarFunction)
		if state == C.DuckDBError {
			C.duckdb_destroy_scalar_function_set(&set)
			return getError(errAPI, addIndexToError(errScalarUDFAddToSet, i))
		}
	}

	// Register the function set on the underlying driver connection exposed by c.Raw.
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		state := C.duckdb_register_scalar_function_set(con.duckdbCon, set)
		C.duckdb_destroy_scalar_function_set(&set)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreateSet)
		}
		return nil
	})
	return err
}
