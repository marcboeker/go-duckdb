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

// RegisterScalarUDF registers a scalar UDF.
// This function takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunction) error {
	if name == "" {
		return getError(errAPI, errScalarUDFNoName)
	}
	if f == nil {
		return getError(errAPI, errScalarUDFIsNil)
	}

	// c.Raw exposes the underlying driver connection.
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		functionName := C.CString(name)
		defer C.duckdb_free(unsafe.Pointer(functionName))

		extraInfoHandle := cgo.NewHandle(f)

		scalarFunction := C.duckdb_create_scalar_function()
		C.duckdb_scalar_function_set_name(scalarFunction, functionName)

		// Get the configuration.
		config := f.Config()
		if config.InputTypeInfos == nil {
			return getError(errAPI, errScalarUDFNilInputTypes)
		}
		if len(config.InputTypeInfos) == 0 {
			return getError(errAPI, errScalarUDFEmptyInputTypes)
		}

		// Add input parameters.
		for i, inputTypeInfo := range config.InputTypeInfos {
			if inputTypeInfo == nil {
				return getError(errAPI, addIndexToError(errScalarUDFInputTypeIsNil, i))
			}

			typeName, ok := unsupportedTypeToStringMap[inputTypeInfo.InternalType()]
			if ok {
				return getError(errAPI, unsupportedTypeError(typeName))
			}

			logicalType := inputTypeInfo.logicalType()
			C.duckdb_scalar_function_add_parameter(scalarFunction, logicalType)
			C.duckdb_destroy_logical_type(&logicalType)
		}

		// Add result parameter.
		if config.ResultTypeInfo == nil {
			return getError(errAPI, errScalarUDFResultTypeIsNil)
		}
		typeName, ok := unsupportedTypeToStringMap[config.ResultTypeInfo.InternalType()]
		if ok {
			return getError(errAPI, unsupportedTypeError(typeName))
		}

		logicalType := config.ResultTypeInfo.logicalType()
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
