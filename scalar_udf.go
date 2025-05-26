package duckdb

/*
void scalar_udf_callback(void *, void *, void *);
typedef void (*scalar_udf_callback_t)(void *, void *, void *);

void scalar_udf_delete_callback(void *);
typedef void (*scalar_udf_delete_callback_t)(void *);

void scalar_udf_bind_callback(void *);
typedef void (*scalar_udf_bind_callback_t)(void *);
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"runtime"
	"runtime/cgo"
	"unsafe"

	"github.com/marcboeker/go-duckdb/mapping"
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

// BindData TODO: How should this look like?
// TODO: In future PRs, clients should be able to set their custom data here during `func Binder(BindInfo) any`,
// TODO: so they can access is during execution.
type BindData struct {
	ClientData any
	ConnId     uint64
}

type scalarFuncInfo interface {
	// Config returns ScalarFuncConfig to configure the scalar function.
	Config() ScalarFuncConfig
}

// ScalarFuncExecutor contains the row-based function to execute a user-defined scalar function.
type ScalarFuncExecutor struct {
	// RowExecutor accepts a row-based execution function.
	// []driver.Value contains the row values.
	// It returns the row execution result, or error.
	RowExecutor func(values []driver.Value) (any, error)
}

// ScalarFunc is the default row-based user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
type ScalarFunc interface {
	scalarFuncInfo
	// Executor returns ScalarFuncExecutor to execute the scalar function.
	Executor() ScalarFuncExecutor
}

// ScalarFuncExecutorWithBindData contains the row-based function to execute a user-defined scalar function.
// It has access to any data set during the binding phase of the scalar function.
type ScalarFuncExecutorWithBindData struct {
	// RowExecutor accepts a row-based execution function.
	// BindData contains any data set during the binding phase.
	// []driver.Value contains the row values.
	// It returns the row execution result, or error.
	RowExecutor func(bindData BindData, values []driver.Value) (any, error)
}

// ScalarFuncWithBindData is a row-based user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
// In addition to the default ScalarFunc interface, ScalarFuncWithBindData has
// access to any data set during the binding phase.
type ScalarFuncWithBindData interface {
	scalarFuncInfo
	// Executor returns ScalarFuncExecutorWithBindData to execute the scalar function.
	Executor() ScalarFuncExecutorWithBindData
}

type scalarFuncWrapper struct {
	f any
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
	defer mapping.DestroyScalarFunction(&function)

	// Register the function on the underlying driver connection exposed by c.Raw.
	err = c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		state := mapping.RegisterScalarFunction(conn.conn, function)
		if state == mapping.StateError {
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
	set := mapping.CreateScalarFunctionSet(name)

	// Create each function and add it to the set.
	for i, f := range functions {
		function, err := createScalarFunc(name, f)
		if err != nil {
			mapping.DestroyScalarFunctionSet(&set)
			return getError(errAPI, err)
		}

		state := mapping.AddScalarFunctionToSet(set, function)
		mapping.DestroyScalarFunction(&function)
		if state == mapping.StateError {
			mapping.DestroyScalarFunctionSet(&set)
			return getError(errAPI, addIndexToError(errScalarUDFAddToSet, i))
		}
	}

	// Register the function set on the underlying driver connection exposed by c.Raw.
	err := c.Raw(func(driverConn any) error {
		conn := driverConn.(*Conn)
		state := mapping.RegisterScalarFunctionSet(conn.conn, set)
		mapping.DestroyScalarFunctionSet(&set)
		if state == mapping.StateError {
			return getError(errAPI, errScalarUDFCreateSet)
		}
		return nil
	})
	return err
}

func getColumnValues(inputChunk *DataChunk, outputChunk *DataChunk, values *[]driver.Value,
	functionInfo *mapping.FunctionInfo, rowIdx int, nullInNullOut bool,
) (bool, error) {
	// Get each column value.
	var err error
	for colIdx := 0; colIdx < len(*values); colIdx++ {
		if (*values)[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
			mapping.ScalarFunctionSetError(*functionInfo, getError(errAPI, err).Error())
			return false, err
		}

		// NULL handling.
		if nullInNullOut && (*values)[colIdx] == nil {
			if err = outputChunk.SetValue(0, rowIdx, nil); err != nil {
				mapping.ScalarFunctionSetError(*functionInfo, getError(errAPI, err).Error())
				return false, err
			}
			return true, nil
		}
	}

	return false, nil
}

func executeRow(f *ScalarFunc, inputChunk *DataChunk, outputChunk *DataChunk, functionInfo *mapping.FunctionInfo) {
	executor := (*f).Executor()
	nullInNullOut := !(*f).Config().SpecialNullHandling
	values := make([]driver.Value, len(inputChunk.columns))
	rowCount := inputChunk.GetSize()

	// Execute the user-defined scalar function for each row.
	var val any
	var err error
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		// Get each column value.
		if nullRow, e := getColumnValues(inputChunk, outputChunk, &values, functionInfo, rowIdx, nullInNullOut); e != nil {
			return
		} else if nullRow {
			continue
		}

		// Execute the user-defined scalar function.
		if val, err = executor.RowExecutor(values); err != nil {
			mapping.ScalarFunctionSetError(*functionInfo, getError(errAPI, err).Error())
			return
		}

		// Write the result to the output chunk.
		if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
			mapping.ScalarFunctionSetError(*functionInfo, getError(errAPI, err).Error())
			return
		}
	}
}

//export scalar_udf_callback
func scalar_udf_callback(functionInfoPtr unsafe.Pointer, inputPtr unsafe.Pointer, outputPtr unsafe.Pointer) {
	functionInfo := mapping.FunctionInfo{Ptr: functionInfoPtr}
	input := mapping.DataChunk{Ptr: inputPtr}
	output := mapping.Vector{Ptr: outputPtr}

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err := inputChunk.initFromDuckDataChunk(input, false); err != nil {
		mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
		return
	}

	// Initialize the output chunk.
	var outputChunk DataChunk
	if err := outputChunk.initFromDuckVector(output, true); err != nil {
		mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
		return
	}

	// bindDataPtr := mapping.ScalarFunctionGetBindData(functionInfo)
	// bindData := getPinned[BindData](bindDataPtr)

	extraInfo := mapping.ScalarFunctionGetExtraInfo(functionInfo)
	function := getPinned[scalarFuncWrapper](extraInfo)

	if f, ok := function.f.(ScalarFunc); ok {
		executeRow(&f, &inputChunk, &outputChunk, &functionInfo)
	} else {
		// TODO: set error that function is invalid
	}
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(info unsafe.Pointer) {
	h := (*cgo.Handle)(info)
	h.Value().(unpinner).unpin()
	h.Delete()
}

//export scalar_udf_bind_callback
func scalar_udf_bind_callback(bindInfoPtr unsafe.Pointer) {
	bindInfo := mapping.BindInfo{Ptr: bindInfoPtr}

	// FIXME: Once available through the duckdb-go-bindings (and the C API),
	// FIXME: we want to get extraInfo here, to access user-defined `func Binder(BindInfo) any` callbacks.
	// FIXME: With these callbacks, we can set additional user-defined bind data.

	var ctx mapping.ClientContext
	mapping.ScalarFunctionGetClientContext(bindInfo, &ctx)
	defer mapping.DestroyClientContext(&ctx)

	connId := mapping.ClientContextGetConnectionId(ctx)
	data := BindData{ConnId: uint64(connId)}

	// Pin the bind data.
	value := pinnedValue[BindData]{
		pinner: &runtime.Pinner{},
		value:  data,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the bind data.
	deleteCallbackPtr := unsafe.Pointer(C.scalar_udf_delete_callback_t(C.scalar_udf_delete_callback))
	mapping.ScalarFunctionSetBindData(bindInfo, unsafe.Pointer(&h), deleteCallbackPtr)
}

func registerInputParams(config ScalarFuncConfig, f mapping.ScalarFunction) error {
	// Set variadic input parameters.
	if config.VariadicTypeInfo != nil {
		t := config.VariadicTypeInfo.logicalType()
		mapping.ScalarFunctionSetVarargs(f, t)
		mapping.DestroyLogicalType(&t)
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
		mapping.ScalarFunctionAddParameter(f, t)
		mapping.DestroyLogicalType(&t)
	}
	return nil
}

func registerResultParams(config ScalarFuncConfig, f mapping.ScalarFunction) error {
	if config.ResultTypeInfo == nil {
		return errScalarUDFResultTypeIsNil
	}
	if config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return errScalarUDFResultTypeIsANY
	}
	t := config.ResultTypeInfo.logicalType()
	mapping.ScalarFunctionSetReturnType(f, t)
	mapping.DestroyLogicalType(&t)
	return nil
}

func createScalarFunc(name string, f ScalarFunc) (mapping.ScalarFunction, error) {
	if name == "" {
		return mapping.ScalarFunction{}, errScalarUDFNoName
	}
	if f == nil {
		return mapping.ScalarFunction{}, errScalarUDFIsNil
	}
	if f.Executor().RowExecutor == nil {
		return mapping.ScalarFunction{}, errScalarUDFNoExecutor
	}

	function := mapping.CreateScalarFunction()
	mapping.ScalarFunctionSetName(function, name)

	// Configure the scalar function.
	config := f.Config()
	if err := registerInputParams(config, function); err != nil {
		mapping.DestroyScalarFunction(&function)
		return function, err
	}
	if err := registerResultParams(config, function); err != nil {
		mapping.DestroyScalarFunction(&function)
		return function, err
	}
	if config.SpecialNullHandling {
		mapping.ScalarFunctionSetSpecialHandling(function)
	}
	if config.Volatile {
		mapping.ScalarFunctionSetVolatile(function)
	}

	// Set the function callback.
	callbackPtr := unsafe.Pointer(C.scalar_udf_callback_t(C.scalar_udf_callback))
	mapping.ScalarFunctionSetFunction(function, callbackPtr)

	// Pin the ScalarFunc f.
	wrapper := scalarFuncWrapper{f}
	value := pinnedValue[scalarFuncWrapper]{
		pinner: &runtime.Pinner{},
		value:  wrapper,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the ScalarFunc f.
	deleteCallbackPtr := unsafe.Pointer(C.scalar_udf_delete_callback_t(C.scalar_udf_delete_callback))
	mapping.ScalarFunctionSetExtraInfo(function, unsafe.Pointer(&h), deleteCallbackPtr)
	return function, nil
}
