package duckdb

/*
// For the function definitions see https://golang.org/issue/19837.
// For the typedef's see https://golang.org/issue/19835.

void table_udf_bind_row(void *);
void table_udf_bind_chunk(void *);
typedef void (*table_udf_bind_t)(void *);

void table_udf_init(void *);
void table_udf_local_init(void *);
typedef void (*table_udf_init_t)(void *);

void table_udf_callback(void *, void *);
typedef void (*table_udf_callback_t)(void *, void *, void *);

void table_udf_delete_callback(void *);
typedef void (*table_udf_delete_callback_t)(void *);
*/
import "C"

import (
	"database/sql"
	"runtime"
	"runtime/cgo"
	"unsafe"

	"github.com/marcboeker/go-duckdb/mapping"
)

type (
	// ColumnInfo contains the metadata of a column.
	ColumnInfo struct {
		// The column Name.
		Name string
		// The column type T.
		T TypeInfo
	}

	// CardinalityInfo contains the cardinality of a (table) function.
	// If it is impossible or difficult to determine the exact cardinality, an approximate cardinality may be used.
	CardinalityInfo struct {
		// The absolute Cardinality.
		Cardinality uint
		// IsExact indicates whether the cardinality is exact.
		Exact bool
	}

	tableFunctionData struct {
		fun        any
		projection []int
	}

	// TableFunctionConfig contains any information passed to DuckDB when registering the table function.
	TableFunctionConfig struct {
		// The Arguments of the table function.
		Arguments []TypeInfo
		// The NamedArguments of the table function.
		NamedArguments map[string]TypeInfo
	}

	// TableFunction implements different table function types:
	// RowTableFunction, ParallelRowTableFunction, ChunkTableFunction, and ParallelChunkTableFunction.
	TableFunction interface {
		RowTableFunction | ParallelRowTableFunction | ChunkTableFunction | ParallelChunkTableFunction
	}

	// parallelTableFunction implements different table function types:
	// ParallelRowTableFunction and ParallelChunkTableFunction.
	parallelTableFunction interface {
		ParallelRowTableFunction | ParallelChunkTableFunction
	}

	// A RowTableFunction is a type which can be bound to return a RowTableSource.
	RowTableFunction = tableFunction[RowTableSource]
	// A ParallelRowTableFunction is a type which can be bound to return a ParallelRowTableSource.
	ParallelRowTableFunction = tableFunction[ParallelRowTableSource]
	// A ChunkTableFunction is a type which can be bound to return a ChunkTableSource.
	ChunkTableFunction = tableFunction[ChunkTableSource]
	// A ParallelChunkTableFunction is a type which can be bound to return a ParallelChunkTableSource.
	ParallelChunkTableFunction = tableFunction[ParallelChunkTableSource]

	tableFunction[T any] struct {
		// Config returns the table function configuration, including the function arguments.
		Config TableFunctionConfig
		// BindArguments binds the arguments and returns a TableSource.
		BindArguments func(named map[string]any, args ...any) (T, error)
	}
)

func wrapRowTF(f RowTableFunction) ParallelRowTableFunction {
	return ParallelRowTableFunction{
		Config: f.Config,
		BindArguments: func(named map[string]any, args ...any) (ParallelRowTableSource, error) {
			rts, err := f.BindArguments(named, args...)
			return parallelRowTSWrapper{s: rts}, err
		},
	}
}

func wrapChunkTF(f ChunkTableFunction) ParallelChunkTableFunction {
	return ParallelChunkTableFunction{
		Config: f.Config,
		BindArguments: func(named map[string]any, args ...any) (ParallelChunkTableSource, error) {
			rts, err := f.BindArguments(named, args...)
			return parallelChunkTSWrapper{s: rts}, err
		},
	}
}

func isRowIdColumn(i mapping.IdxT) bool {
	// FIXME: Replace this with mapping.IsRowIdColumn(i) / virtual column changes, once available in the C API.
	return i == 18446744073709551615
}

func (tfd *tableFunctionData) setColumnCount(info mapping.InitInfo) {
	count := mapping.InitGetColumnCount(info)
	for i := mapping.IdxT(0); i < count; i++ {
		srcPos := mapping.InitGetColumnIndex(info, i)
		// FIXME: Special-case, should just work post duckdb v1.3.0.
		// See: https://github.com/duckdb/duckdb/pull/16248
		if !isRowIdColumn(srcPos) {
			tfd.projection[int(srcPos)] = int(i)
		}
	}
}

//export table_udf_bind_row
func table_udf_bind_row(infoPtr unsafe.Pointer) {
	udfBindTyped[ParallelRowTableSource](infoPtr)
}

//export table_udf_bind_chunk
func table_udf_bind_chunk(infoPtr unsafe.Pointer) {
	udfBindTyped[ParallelChunkTableSource](infoPtr)
}

func udfBindTyped[T tableSource](infoPtr unsafe.Pointer) {
	info := mapping.BindInfo{Ptr: infoPtr}

	f := getPinned[tableFunction[T]](mapping.BindGetExtraInfo(info))
	config := f.Config

	argCount := len(config.Arguments)
	args := make([]any, argCount)
	namedArgs := make(map[string]any)

	for i, t := range config.Arguments {
		var err error
		value := mapping.BindGetParameter(info, mapping.IdxT(i))
		args[i], err = getValue(t, value)
		mapping.DestroyValue(&value)

		if err != nil {
			mapping.BindSetError(info, err.Error())
			return
		}
	}

	for name, t := range config.NamedArguments {
		var err error
		value := mapping.BindGetNamedParameter(info, name)
		namedArgs[name], err = getValue(t, value)
		mapping.DestroyValue(&value)

		if err != nil {
			mapping.BindSetError(info, err.Error())
			return
		}
	}

	instance, err := f.BindArguments(namedArgs, args...)
	if err != nil {
		mapping.BindSetError(info, err.Error())
		return
	}

	columnInfos := instance.ColumnInfos()
	instanceData := tableFunctionData{
		fun:        instance,
		projection: make([]int, len(columnInfos)),
	}

	for i, v := range columnInfos {
		if v.T == nil {
			mapping.BindSetError(info, errTableUDFColumnTypeIsNil.Error())
			return
		}
		logicalType := v.T.logicalType()
		mapping.BindAddResultColumn(info, v.Name, logicalType)
		mapping.DestroyLogicalType(&logicalType)
		instanceData.projection[i] = -1
	}

	cardinality := instance.Cardinality()
	if cardinality != nil {
		mapping.BindSetCardinality(info, mapping.IdxT(cardinality.Cardinality), cardinality.Exact)
	}

	pinnedInstanceData := pinnedValue[tableFunctionData]{
		pinner: &runtime.Pinner{},
		value:  instanceData,
	}

	h := cgo.NewHandle(pinnedInstanceData)
	pinnedInstanceData.pinner.Pin(&h)
	deleteCallbackPtr := unsafe.Pointer(C.table_udf_delete_callback_t(C.table_udf_delete_callback))
	mapping.BindSetBindData(info, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export table_udf_init
func table_udf_init(infoPtr unsafe.Pointer) {
	info := mapping.InitInfo{Ptr: infoPtr}
	instance := getPinned[tableFunctionData](mapping.InitGetBindData(info))
	instance.setColumnCount(info)
	initData := instance.fun.(parallelTableSource).Init()
	mapping.InitSetMaxThreads(info, mapping.IdxT(initData.MaxThreads))
}

//export table_udf_local_init
func table_udf_local_init(infoPtr unsafe.Pointer) {
	info := mapping.InitInfo{Ptr: infoPtr}
	instance := getPinned[tableFunctionData](mapping.InitGetBindData(info))
	localState := pinnedValue[any]{
		pinner: &runtime.Pinner{},
		value:  instance.fun.(parallelTableSource).NewLocalState(),
	}
	h := cgo.NewHandle(localState)
	localState.pinner.Pin(&h)
	deleteCallbackPtr := unsafe.Pointer(C.table_udf_delete_callback_t(C.table_udf_delete_callback))
	mapping.InitSetInitData(info, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export table_udf_callback
func table_udf_callback(infoPtr, outputPtr unsafe.Pointer) {
	info := mapping.FunctionInfo{Ptr: infoPtr}
	output := mapping.DataChunk{Ptr: outputPtr}

	instance := getPinned[tableFunctionData](mapping.FunctionGetBindData(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		mapping.FunctionSetError(info, err.Error())
		return
	}

	localState := getPinned[any](mapping.FunctionGetLocalInitData(info))

	switch fun := instance.fun.(type) {
	case ParallelRowTableSource:
		row := Row{
			chunk:      &chunk,
			projection: instance.projection,
		}
		maxSize := mapping.IdxT(GetDataChunkCapacity())

		// At the end of the loop row.r must be the index of the last row.
		for row.r = 0; row.r < maxSize; row.r++ {
			next, errRow := fun.FillRow(localState, row)
			if errRow != nil {
				mapping.FunctionSetError(info, errRow.Error())
				break
			}
			if !next {
				break
			}
		}
		mapping.DataChunkSetSize(output, row.r)
	case ParallelChunkTableSource:
		err = fun.FillChunk(localState, chunk)
		if err != nil {
			mapping.FunctionSetError(info, err.Error())
		}
	}
}

//export table_udf_delete_callback
func table_udf_delete_callback(info unsafe.Pointer) {
	h := (*cgo.Handle)(info)
	h.Value().(unpinner).unpin()
	h.Delete()
}

// RegisterTableUDF registers a user-defined table function.
// Projection pushdown is enabled by default.
func RegisterTableUDF[TFT TableFunction](conn *sql.Conn, name string, f TFT) error {
	if name == "" {
		return getError(errAPI, errTableUDFNoName)
	}

	// normalise the function
	var x any = f
	switch tableFunc := x.(type) {
	case RowTableFunction:
		return registerParallelTableUDF(conn, name, wrapRowTF(tableFunc))
	case ChunkTableFunction:
		return registerParallelTableUDF(conn, name, wrapChunkTF(tableFunc))
	case ParallelRowTableFunction:
		return registerParallelTableUDF(conn, name, tableFunc)
	case ParallelChunkTableFunction:
		return registerParallelTableUDF(conn, name, tableFunc)
	default:
		return getError(errInternal, nil)
	}
}

func registerParallelTableUDF[TFT parallelTableFunction](conn *sql.Conn, name string, f TFT) error {
	function := mapping.CreateTableFunction()
	mapping.TableFunctionSetName(function, name)

	var config TableFunctionConfig

	// Pin the table function f.
	value := pinnedValue[TFT]{
		pinner: &runtime.Pinner{},
		value:  f,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the table function f.
	deleteCallbackPtr := unsafe.Pointer(C.table_udf_delete_callback_t(C.table_udf_delete_callback))
	mapping.TableFunctionSetExtraInfo(function, unsafe.Pointer(&h), deleteCallbackPtr)

	mapping.TableFunctionSupportsProjectionPushdown(function, true)

	initCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_init))
	mapping.TableFunctionSetInit(function, initCallbackPtr)

	localInitCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_local_init))
	mapping.TableFunctionSetLocalInit(function, localInitCallbackPtr)

	callbackPtr := unsafe.Pointer(C.table_udf_callback_t(C.table_udf_callback))
	mapping.TableFunctionSetFunction(function, callbackPtr)

	var x any = f
	switch tableFunc := x.(type) {
	case ParallelRowTableFunction:
		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_row))
		mapping.TableFunctionSetBind(function, bindCallbackPtr)

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ParallelChunkTableFunction:
		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_chunk))
		mapping.TableFunctionSetBind(function, bindCallbackPtr)

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	default:
		return getError(errInternal, nil)
	}

	// Set the arguments.
	for _, t := range config.Arguments {
		if t == nil {
			return getError(errAPI, errTableUDFArgumentIsNil)
		}
		logicalType := t.logicalType()
		mapping.TableFunctionAddParameter(function, logicalType)
		mapping.DestroyLogicalType(&logicalType)
	}

	// Set the named arguments.
	for arg, t := range config.NamedArguments {
		if t == nil {
			return getError(errAPI, errTableUDFArgumentIsNil)
		}
		logicalType := t.logicalType()
		mapping.TableFunctionAddNamedParameter(function, arg, logicalType)
		mapping.DestroyLogicalType(&logicalType)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err := conn.Raw(func(driverConn any) error {
		c := driverConn.(*Conn)
		state := mapping.RegisterTableFunction(c.conn, function)
		mapping.DestroyTableFunction(&function)
		if state == mapping.StateError {
			return getError(errAPI, errTableUDFCreate)
		}
		return nil
	})
	return err
}
