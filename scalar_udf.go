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
	"context"
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

// BindInfo TODO: private/don't export? set as a key in the context?
type BindInfo struct {
	ConnId uint64
}

type (
	// RowExecutorFn is the type for any row-based execution function.
	// values contains the row values.
	// It returns the row execution result, or error.
	RowExecutorFn func(values []driver.Value) (any, error)
	// RowContextExecutorFn accepts a row-based execution function using a context.
	// ctx is the current context of the connection.
	// values contains the row values.
	// It returns the row execution result, or error.
	RowContextExecutorFn func(ctx context.Context, values []driver.Value) (any, error)
)

// ScalarFuncExecutor contains the functions to execute a user-defined scalar function.
// It invokes its first non-nil member.
type ScalarFuncExecutor struct {
	// RowExecutor accepts a row-based execution function of type RowExecutorFn.
	RowExecutor RowExecutorFn
	// RowContextExecutor accepts a row-based execution function of type RowContextExecutorFn.
	RowContextExecutor RowContextExecutorFn
}

// ScalarFunc is the user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
type ScalarFunc interface {
	// Config returns ScalarFuncConfig to configure the scalar function.
	Config() ScalarFuncConfig
	// Executor returns ScalarFuncExecutor to execute the scalar function.
	Executor() ScalarFuncExecutor
}

// scalarFuncContext is a wrapper around a ScalarFunc providing an execution context.
type scalarFuncContext struct {
	f        ScalarFunc
	ctxStore *contextStore
}

// Config returns the ScalarFuncConfig of the scalar function.
func (s *scalarFuncContext) Config() ScalarFuncConfig {
	return s.f.Config()
}

// RowExecutor returns a RowExecutorFn that executes the scalar function.
// It uses the BindInfo to get the context for execution.
func (s *scalarFuncContext) RowExecutor(info *BindInfo) RowExecutorFn {
	e := s.f.Executor()
	if e.RowExecutor != nil {
		return e.RowExecutor
	}
	ctx := s.ctxStore.load(info.ConnId)

	return func(values []driver.Value) (any, error) {
		return e.RowContextExecutor(ctx, values)
	}
}

// RegisterScalarUDF registers a user-defined scalar function.
// *sql.Conn is the SQL connection on which to register the scalar function.
// name is the function name, and f is the scalar function's interface ScalarFunc.
// RegisterScalarUDF takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error {
	function, err := createScalarFunc(c, name, f)
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
		function, err := createScalarFunc(c, name, f)
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

	extraInfo := mapping.ScalarFunctionGetExtraInfo(functionInfo)
	function := getPinned[*scalarFuncContext](extraInfo)
	nullInNullOut := !function.Config().SpecialNullHandling

	bindDataPtr := mapping.ScalarFunctionGetBindData(functionInfo)
	bindInfo := getPinned[BindInfo](bindDataPtr)

	f := function.RowExecutor(&bindInfo)
	values := make([]driver.Value, len(inputChunk.columns))

	// Execute the user-defined scalar function for each row.
	for rowIdx := 0; rowIdx < inputChunk.GetSize(); rowIdx++ {

		// Get each column value.
		var err error
		nullRow := false
		for colIdx := 0; colIdx < len(values); colIdx++ {
			if values[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
				return
			}

			// NULL handling.
			if nullInNullOut && values[colIdx] == nil {
				if err = outputChunk.SetValue(0, rowIdx, nil); err != nil {
					mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
					return
				}
				nullRow = true
				break
			}
		}

		if nullRow {
			continue
		}

		// Execute the user-defined scalar function.
		if val, e := f(values); e != nil {
			mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, e).Error())
			return
		} else {
			// Write the result to the output chunk.
			if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
				mapping.ScalarFunctionSetError(functionInfo, getError(errAPI, err).Error())
				return
			}
		}
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
	// FIXME: we want to get extraInfo here, to access user-defined `func Binder(BindInfo) (any, any)` callbacks.
	// FIXME: With these callbacks, we can set additional user-defined bind data in the context.

	var ctx mapping.ClientContext
	mapping.ScalarFunctionGetClientContext(bindInfo, &ctx)
	defer mapping.DestroyClientContext(&ctx)

	id := mapping.ClientContextGetConnectionId(ctx)
	data := BindInfo{ConnId: uint64(id)}

	// Pin the bind data.
	value := pinnedValue[BindInfo]{
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

func createScalarFunc(c *sql.Conn, name string, f ScalarFunc) (mapping.ScalarFunction, error) {
	if name == "" {
		return mapping.ScalarFunction{}, errScalarUDFNoName
	}
	if f == nil {
		return mapping.ScalarFunction{}, errScalarUDFIsNil
	}

	if f.Executor().RowExecutor == nil && f.Executor().RowContextExecutor == nil {
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

	// Set the bind callback.
	bindPtr := unsafe.Pointer(C.scalar_udf_bind_callback_t(C.scalar_udf_bind_callback))
	mapping.ScalarFunctionSetBind(function, bindPtr)

	// Set the function callback.
	functionPtr := unsafe.Pointer(C.scalar_udf_callback_t(C.scalar_udf_callback))
	mapping.ScalarFunctionSetFunction(function, functionPtr)

	// Get the context store of the connection.
	ctxStore, err := contextStoreFromConn(c)
	if err != nil {
		mapping.DestroyScalarFunction(&function)
		return function, err
	}

	// Pin the ScalarFunc f.
	value := pinnedValue[*scalarFuncContext]{
		pinner: &runtime.Pinner{},
		value:  &scalarFuncContext{f: f, ctxStore: ctxStore},
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the ScalarFunc f.
	deleteCallbackPtr := unsafe.Pointer(C.scalar_udf_delete_callback_t(C.scalar_udf_delete_callback))
	mapping.ScalarFunctionSetExtraInfo(function, unsafe.Pointer(&h), deleteCallbackPtr)
	return function, nil
}
