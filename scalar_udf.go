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

type ScalarFunctionConfig interface {
	InputTypeInfos() []TypeInfo
	ResultTypeInfo() TypeInfo
	VariadicTypeInfo() TypeInfo
	Volatile() bool
	SpecialNullHandling() bool
}

type ScalarFunction interface {
	Config() ScalarFunctionConfig
	ExecuteRow(args []driver.Value) (any, error)
}

func setFunctionError(info C.duckdb_function_info, msg string) {
	err := C.CString(msg)
	C.duckdb_scalar_function_set_error(info, err)
	C.duckdb_free(unsafe.Pointer(err))
}

//export scalar_udf_callback
func scalar_udf_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	extraInfo := C.duckdb_scalar_function_get_extra_info(info)

	// extraInfo is a void* pointer to our ScalarFunction.
	h := *(*cgo.Handle)(unsafe.Pointer(extraInfo))
	scalarFunction := h.Value().(ScalarFunction)

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err := inputChunk.initFromDuckDataChunk(input, false); err != nil {
		setFunctionError(info, getError(errAPI, err).Error())
		return
	}

	// Initialize the output chunk.
	var outputChunk DataChunk
	if err := outputChunk.initFromDuckVector(output, true); err != nil {
		setFunctionError(info, getError(errAPI, err).Error())
		return
	}

	// Execute the user-defined scalar function for each row.
	args := make([]driver.Value, len(inputChunk.columns))
	rowCount := inputChunk.GetSize()
	columnCount := len(args)
	var err error

	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		// Set the input arguments for each column of a row.
		for colIdx := 0; colIdx < columnCount; colIdx++ {
			if args[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				setFunctionError(info, getError(errAPI, err).Error())
				return
			}
		}

		// Execute the function and write the result to the output vector.
		var val any
		if val, err = scalarFunction.ExecuteRow(args); err != nil {
			break
		}
		if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
			break
		}
	}

	if err != nil {
		setFunctionError(info, getError(errAPI, err).Error())
	}
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(extraInfo unsafe.Pointer) {
	h := (*cgo.Handle)(extraInfo)
	h.Delete()
}

func registerInputParameters(config ScalarFunctionConfig, scalarFunction C.duckdb_scalar_function) error {
	// Set variadic input parameters.
	if config.VariadicTypeInfo() != nil {
		logicalType := config.VariadicTypeInfo().logicalType()
		C.duckdb_scalar_function_set_varargs(scalarFunction, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
		return nil
	}

	// Set fixed input parameters.
	if config.InputTypeInfos() == nil {
		return errScalarUDFNilInputTypes
	}
	if len(config.InputTypeInfos()) == 0 {
		return errScalarUDFEmptyInputTypes
	}

	for i, inputTypeInfo := range config.InputTypeInfos() {
		if inputTypeInfo == nil {
			return addIndexToError(errScalarUDFInputTypeIsNil, i)
		}
		logicalType := inputTypeInfo.logicalType()
		C.duckdb_scalar_function_add_parameter(scalarFunction, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
	}
	return nil
}

func registerResultParameters(config ScalarFunctionConfig, scalarFunction C.duckdb_scalar_function) error {
	if config.ResultTypeInfo() == nil {
		return errScalarUDFResultTypeIsNil
	}
	if config.ResultTypeInfo().InternalType() == TYPE_ANY {
		return errScalarUDFResultTypeIsANY
	}
	logicalType := config.ResultTypeInfo().logicalType()
	C.duckdb_scalar_function_set_return_type(scalarFunction, logicalType)
	C.duckdb_destroy_logical_type(&logicalType)
	return nil
}

func createScalarFunction(name string, f ScalarFunction) (C.duckdb_scalar_function, error) {
	if name == "" {
		return nil, errScalarUDFNoName
	}
	if f == nil {
		return nil, errScalarUDFIsNil
	}
	scalarFunction := C.duckdb_create_scalar_function()

	// Set the name.
	functionName := C.CString(name)
	C.duckdb_scalar_function_set_name(scalarFunction, functionName)
	C.duckdb_free(unsafe.Pointer(functionName))

	// Configure the scalar function.
	config := f.Config()
	if err := registerInputParameters(config, scalarFunction); err != nil {
		return nil, err
	}
	if err := registerResultParameters(config, scalarFunction); err != nil {
		return nil, err
	}
	if config.SpecialNullHandling() {
		C.duckdb_scalar_function_set_special_handling(scalarFunction)
	}
	if config.Volatile() {
		C.duckdb_scalar_function_set_volatile(scalarFunction)
	}

	// Set the function callback.
	C.duckdb_scalar_function_set_function(scalarFunction, C.scalar_udf_callback_t(C.scalar_udf_callback))

	// Set data available during execution.
	extraInfoHandle := cgo.NewHandle(f)
	C.duckdb_scalar_function_set_extra_info(
		scalarFunction,
		unsafe.Pointer(&extraInfoHandle),
		C.duckdb_delete_callback_t(C.scalar_udf_delete_callback))

	return scalarFunction, nil
}

// RegisterScalarUDF registers a scalar UDF.
// This function takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunction) error {
	scalarFunction, err := createScalarFunction(name, f)
	if err != nil {
		return getError(errAPI, err)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err = c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		state := C.duckdb_register_scalar_function(con.duckdbCon, scalarFunction)
		C.duckdb_destroy_scalar_function(&scalarFunction)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreate)
		}
		return nil
	})
	return err
}

func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunction) error {
	functionName := C.CString(name)
	set := C.duckdb_create_scalar_function_set(functionName)
	C.duckdb_free(unsafe.Pointer(functionName))

	// Create each function and add it to the set.
	for i, f := range functions {
		scalarFunction, err := createScalarFunction(name, f)
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
