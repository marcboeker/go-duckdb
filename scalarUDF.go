package duckdb

/*
void scalar_udf_callback(void *, void *, void *);
void udf_delete_callback(void *);

typedef void (*scalar_udf_callback_t)(void *, void *, void *);
typedef void (*udf_delete_callback_t)(void *);
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

// ScalarFuncConfig contains the fields to configure a user-defined scalar function.
type ScalarFuncConfig struct {
	// InputTypeInfos contains Type information for each input parameter of the scalar function.
	InputTypeInfos []TypeInfo
	// ResultTypeInfo holds the Type information of the scalar function's result type.
	ResultTypeInfo TypeInfo

	// VariadicTypeInfo configures the number of input parameters.
	// If this field is nil, then the input parameters match InputTypeInfos.
	// Otherwise, the scalar function's input parameters are set to variadic, allowing any number of input parameters.
	// The Type of the first len(InputTypeInfos) parameters is configured by InputTypeInfos, and all
	// remaining parameters must match the variadic Type. To configure different variadic parameter types,
	// you must set the VariadicTypeInfo's Type to TYPE_ANY.
	VariadicTypeInfo TypeInfo
	// Volatile sets the stability of the scalar function to volatile, if true.
	// Volatile scalar functions might create a different result per row.
	// E.g., random() is a volatile scalar function.
	Volatile bool
	// SpecialNullHandling disables the default NULL handling of scalar functions, if true.
	// The default NULL handling is: NULL in, NULL out. I.e., if any input parameter is NULL, then the result is NULL.
	SpecialNullHandling bool
}

// ScalarFuncExecutor contains the callback function to execute a user-defined scalar function.
// Currently, its only field is a row-based executor.
type ScalarFuncExecutor struct {
	// RowExecutor accepts a row-based execution function.
	// []driver.Value contains the row values, and it returns the row execution result, or error.
	RowExecutor func(values []driver.Value) (any, error)
}

// ScalarFunc is the user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
type ScalarFunc interface {
	// Config returns ScalarFuncConfig to configure the scalar function.
	Config() ScalarFuncConfig
	// Executor returns ScalarFuncExecutor to execute the scalar function.
	Executor() ScalarFuncExecutor
}

// RegisterScalarUDF registers a user-defined scalar function.
// *sql.Conn is the SQL connection on which to register the scalar function.
// name is the function name, and f is the scalar function's interface ScalarFunc.
// RegisterScalarUDF takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error {
	function, err := createScalarFunc(name, f)
	if err != nil {
		return getError(errAPI, err)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err = c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		state := apiRegisterScalarFunction(conn.apiConn, function)
		apiDestroyScalarFunction(&function)
		if apiState(state) == apiError {
			return getError(errAPI, errScalarUDFCreate)
		}
		return nil
	})
	return err
}

// RegisterScalarUDFSet registers a set of user-defined scalar functions with the same name.
// This enables overloading of scalar functions.
// E.g., the function my_length() can have implementations like my_length(LIST(ANY)) and my_length(VARCHAR).
// *sql.Conn is the SQL connection on which to register the scalar function set.
// name is the function name of each function in the set.
// functions contains all ScalarFunc functions of the scalar function set.
func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunc) error {
	set := apiCreateScalarFunctionSet(name)

	// Create each function and add it to the set.
	for i, f := range functions {
		function, err := createScalarFunc(name, f)
		if err != nil {
			apiDestroyScalarFunction(&function)
			apiDestroyScalarFunctionSet(&set)
			return getError(errAPI, err)
		}

		state := apiAddScalarFunctionToSet(set, function)
		apiDestroyScalarFunction(&function)
		if apiState(state) == apiError {
			apiDestroyScalarFunctionSet(&set)
			return getError(errAPI, addIndexToError(errScalarUDFAddToSet, i))
		}
	}

	// Register the function set on the underlying driver connection exposed by c.Raw.
	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		state := apiRegisterScalarFunctionSet(conn.apiConn, set)
		apiDestroyScalarFunctionSet(&set)
		if apiState(state) == apiError {
			return getError(errAPI, errScalarUDFCreateSet)
		}
		return nil
	})
	return err
}

//export scalar_udf_callback
func scalar_udf_callback(functionInfoPtr unsafe.Pointer, inputPtr unsafe.Pointer, outputPtr unsafe.Pointer) {
	functionInfo := apiFunctionInfo{Ptr: functionInfoPtr}
	input := apiDataChunk{Ptr: inputPtr}
	output := apiVector{Ptr: outputPtr}

	extraInfo := apiScalarFunctionGetExtraInfo(functionInfo)
	function := getPinned[ScalarFunc](extraInfo)

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err := inputChunk.initFromDuckDataChunk(input, false); err != nil {
		apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
		return
	}

	// Initialize the output chunk.
	var outputChunk DataChunk
	if err := outputChunk.initFromDuckVector(output, true); err != nil {
		apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
		return
	}

	executor := function.Executor()
	nullInNullOut := !function.Config().SpecialNullHandling
	values := make([]driver.Value, len(inputChunk.columns))
	columnCount := len(values)
	rowCount := inputChunk.GetSize()

	// Execute the user-defined scalar function for each row.
	var err error
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		nullRow := false

		// Get each column value.
		for colIdx := 0; colIdx < columnCount; colIdx++ {
			if values[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
				return
			}

			// NULL handling.
			if nullInNullOut && values[colIdx] == nil {
				if err = outputChunk.SetValue(0, rowIdx, nil); err != nil {
					apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
					return
				}
				nullRow = true
				break
			}
		}
		if nullRow {
			continue
		}

		// Execute the function.
		var val any
		if val, err = executor.RowExecutor(values); err != nil {
			apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
			return
		}

		// Write the result to the output chunk.
		if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
			apiScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
			return
		}
	}
}

func registerInputParams(config ScalarFuncConfig, f apiScalarFunction) error {
	// Set variadic input parameters.
	if config.VariadicTypeInfo != nil {
		t := config.VariadicTypeInfo.logicalType()
		apiScalarFunctionSetVarargs(f, t)
		apiDestroyLogicalType(&t)
	}

	// Early-out, if the function does not take any (non-variadic) parameters.
	if config.InputTypeInfos == nil {
		return nil
	}
	if len(config.InputTypeInfos) == 0 {
		return nil
	}

	// Set non-variadic input parameters.
	for i, info := range config.InputTypeInfos {
		if info == nil {
			return addIndexToError(errScalarUDFInputTypeIsNil, i)
		}
		t := info.logicalType()
		apiScalarFunctionAddParameter(f, t)
		apiDestroyLogicalType(&t)
	}
	return nil
}

func registerResultParams(config ScalarFuncConfig, f apiScalarFunction) error {
	if config.ResultTypeInfo == nil {
		return errScalarUDFResultTypeIsNil
	}
	if config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return errScalarUDFResultTypeIsANY
	}
	t := config.ResultTypeInfo.logicalType()
	apiScalarFunctionSetReturnType(f, t)
	apiDestroyLogicalType(&t)
	return nil
}

func createScalarFunc(name string, f ScalarFunc) (apiScalarFunction, error) {
	if name == "" {
		return apiScalarFunction{}, errScalarUDFNoName
	}
	if f == nil {
		return apiScalarFunction{}, errScalarUDFIsNil
	}
	if f.Executor().RowExecutor == nil {
		return apiScalarFunction{}, errScalarUDFNoExecutor
	}

	function := apiCreateScalarFunction()

	// Set the name.
	apiScalarFunctionSetName(function, name)

	// Configure the scalar function.
	config := f.Config()
	if err := registerInputParams(config, function); err != nil {
		return function, err
	}
	if err := registerResultParams(config, function); err != nil {
		return function, err
	}
	if config.SpecialNullHandling {
		apiScalarFunctionSetSpecialHandling(function)
	}
	if config.Volatile {
		apiScalarFunctionSetVolatile(function)
	}

	// Set the function callback.
	callbackPtr := unsafe.Pointer(C.scalar_udf_callback_t(C.scalar_udf_callback))
	apiScalarFunctionSetFunction(function, callbackPtr)

	// Pin the ScalarFunc f.
	value := pinnedValue[ScalarFunc]{
		pinner: &runtime.Pinner{},
		value:  f,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the ScalarFunc f.
	deleteCallbackPtr := unsafe.Pointer(C.udf_delete_callback_t(C.udf_delete_callback))
	apiScalarFunctionSetExtraInfo(function, unsafe.Pointer(&h), deleteCallbackPtr)
	return function, nil
}
