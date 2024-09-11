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

type ScalarFunctionConfig struct {
	InputTypeInfos []TypeInfo
	ResultTypeInfo TypeInfo
}

type ScalarFunction interface {
	Config() (ScalarFunctionConfig, error)
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
	if err = inputChunk.initFromDuckDataChunk(input, false); err != nil {
		scalarFunction.SetError(getError(errAPI, err))
		return
	}
	// Initialize the output chunk.
	var outputChunk DataChunk
	if err = outputChunk.initFromDuckVector(output, true); err != nil {
		scalarFunction.SetError(getError(errAPI, err))
		return
	}

	// Execute the user-defined scalar function for each row.
	args := make([]driver.Value, len(inputChunk.columns))
	rowCount := inputChunk.GetSize()
	columnCount := len(args)
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {

		// Set the input arguments for each column of a row.
		for colIdx := 0; colIdx < columnCount; colIdx++ {
			if args[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				scalarFunction.SetError(getError(errAPI, err))
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
		scalarFunction.SetError(getError(errAPI, err))
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
		return getError(errAPI, errScalarUDFNoName)
	}

	// c.Raw exposes the underlying driver connection.
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		functionName := C.CString(name)
		defer C.duckdb_free(unsafe.Pointer(functionName))

		extraInfoHandle := cgo.NewHandle(function)

		scalarFunction := C.duckdb_create_scalar_function()
		C.duckdb_scalar_function_set_name(scalarFunction, functionName)

		// Get the configuration.
		config, err := function.Config()
		if err != nil {
			return getError(errAPI, err)
		}

		// Add input parameters.
		for _, inputTypeInfo := range config.InputTypeInfos {
			typeName, ok := unsupportedTypeToStringMap[inputTypeInfo.t]
			if ok {
				return getError(errAPI, unsupportedTypeError(typeName))
			}

			logicalType, errInputType := inputTypeInfo.logicalType()
			if errInputType != nil {
				C.duckdb_destroy_logical_type(&logicalType)
				return getError(errAPI, errInputType)
			}

			C.duckdb_scalar_function_add_parameter(scalarFunction, logicalType)
			C.duckdb_destroy_logical_type(&logicalType)
		}

		// Add result parameter.
		typeName, ok := unsupportedTypeToStringMap[config.ResultTypeInfo.t]
		if ok {
			return getError(errAPI, unsupportedTypeError(typeName))
		}

		logicalType, err := config.ResultTypeInfo.logicalType()
		if err != nil {
			C.duckdb_destroy_logical_type(&logicalType)
			return getError(errAPI, err)
		}

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
		state := C.duckdb_register_scalar_function(con.duckdbCon, scalarFunction)
		C.duckdb_destroy_scalar_function(&scalarFunction)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreate)
		}
		return nil
	})
	return err
}