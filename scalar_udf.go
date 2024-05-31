package duckdb

// Related issues: https://golang.org/issue/19835, https://golang.org/issue/19837.

/*
#include <stdlib.h>
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
	"strings"
	"unsafe"
)

/*
Interface notes.
- Currently, you must cast your driver.Value's to int32. Casting to just int is not implemented.
*/

type ScalarFunctionConfig struct {
	InputTypes []string
	ResultType string
}

type ScalarFunction interface {
	Config() ScalarFunctionConfig
	ExecuteRow(args []driver.Value) (any, error)
	SetError(err error)
}

//export scalar_udf_callback
func scalar_udf_callback(info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	// info is a void* pointer to our ScalarFunction.
	h := *(*cgo.Handle)(unsafe.Pointer(info))
	scalarFunction := h.Value().(ScalarFunction)

	var err error

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err = inputChunk.InitFromDuckDataChunk(input); err != nil {
		scalarFunction.SetError(err)
		return
	}
	// Initialize the output chunk.
	var outputChunk DataChunk
	if err = outputChunk.InitFromDuckVector(output); err != nil {
		scalarFunction.SetError(err)
		return
	}

	// Execute the user-defined scalar function for each row.
	inputSize := inputChunk.GetSize()
	args := make([]driver.Value, len(scalarFunction.Config().InputTypes))
	for rowIdx := 0; rowIdx < inputSize; rowIdx++ {

		// Set the input arguments for each column of a row.
		for colIdx := 0; colIdx < len(args); colIdx++ {
			if args[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				scalarFunction.SetError(err)
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
		scalarFunction.SetError(err)
	}
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(data unsafe.Pointer) {
	h := cgo.Handle(data)
	h.Delete()
}

// RegisterScalarUDF registers a scalar UDF.
func RegisterScalarUDF(c *sql.Conn, name string, function ScalarFunction) error {
	if name == "" {
		return errScalarUDFNoName
	}

	// c.Raw exposes the underlying driver connection.
	err := c.Raw(func(anyConn any) error {
		driverConn := anyConn.(*conn)
		functionName := C.CString(name)
		defer C.free(unsafe.Pointer(functionName))

		extraInfoHandle := cgo.NewHandle(function)

		scalarFunction := C.duckdb_create_scalar_function()
		C.duckdb_scalar_function_set_name(scalarFunction, functionName)

		// Add input parameters.
		for _, inputType := range function.Config().InputTypes {
			sqlType := strings.ToUpper(inputType)
			duckdbType, ok := SQLToDuckDBMap[sqlType]
			if !ok {
				return unsupportedTypeError(sqlType)
			}
			logicalType := C.duckdb_create_logical_type(duckdbType)
			C.duckdb_scalar_function_add_parameter(scalarFunction, logicalType)
			C.duckdb_destroy_logical_type(&logicalType)
		}

		// Add result parameter.
		sqlType := strings.ToUpper(function.Config().ResultType)
		duckdbType, ok := SQLToDuckDBMap[sqlType]
		if !ok {
			return unsupportedTypeError(sqlType)
		}
		logicalType := C.duckdb_create_logical_type(duckdbType)
		C.duckdb_scalar_function_set_return_type(scalarFunction, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)

		// Set the actual function.
		C.duckdb_scalar_function_set_function(scalarFunction, C.scalar_udf_callback_t(C.scalar_udf_callback))

		// Set data available during execution.
		C.duckdb_scalar_function_set_extra_info(
			scalarFunction,
			unsafe.Pointer(&extraInfoHandle),
			C.duckdb_delete_callback_t(C.scalar_udf_delete_callback))

		// Register the function.
		state := C.duckdb_register_scalar_function(driverConn.duckdbCon, scalarFunction)
		C.duckdb_destroy_scalar_function(&scalarFunction)
		if state == C.DuckDBError {
			return getError(errDriver, nil)
		}
		return nil
	})
	return err
}
