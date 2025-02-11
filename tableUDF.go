package duckdb

/*
// For the function definitions see https://golang.org/issue/19837.
// For the typedef's see https://golang.org/issue/19835.

void table_udf_bind_row(void *);
void table_udf_bind_chunk(void *);
void table_udf_bind_parallel_row(void *);
void table_udf_bind_parallel_chunk(void *);
typedef void (*table_udf_bind_t)(void *);

void table_udf_init(void *);
void table_udf_init_parallel(void *);
void table_udf_local_init(void *);
typedef void (*table_udf_init_t)(void *);

void table_udf_row_callback(void *, void *);
void table_udf_chunk_callback(void *, void *);
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

	// ParallelTableSourceInfo contains information for initializing a parallelism-aware table source.
	ParallelTableSourceInfo struct {
		// MaxThreads is the maximum number of threads on which to run the table source function.
		// If set to 0, it uses DuckDB's default thread configuration.
		MaxThreads int
	}

	tableFunctionData struct {
		fun        any
		projection []int
	}

	tableSource interface {
		// ColumnInfos returns column information for each column of the table function.
		ColumnInfos() []ColumnInfo
		// Cardinality returns the cardinality information of the table function.
		// Optionally, if no cardinality exists, it may return nil.
		Cardinality() *CardinalityInfo
	}

	parallelTableSource interface {
		tableSource
		// Init the table source.
		// Additionally, it returns information for the parallelism-aware table source.
		Init() ParallelTableSourceInfo
		// NewLocalState returns a thread-local execution state.
		// It must return a pointer or a reference type for correct state updates.
		// go-duckdb does not prevent non-reference values.
		NewLocalState() any
	}

	sequentialTableSource interface {
		tableSource
		// Init the table source.
		Init()
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the RowTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillRow method with a single thread.
	RowTableSource interface {
		sequentialTableSource
		// FillRow takes a Row and fills it with values.
		// It returns true, if there are more rows to fill.
		FillRow(Row) (bool, error)
	}

	// A ParallelRowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelRowTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillRow method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillRow must use synchronisation
	// primitives to avoid race conditions.
	ParallelRowTableSource interface {
		parallelTableSource
		// FillRow takes a Row and fills it with values.
		// It returns true, if there are more rows to fill.
		FillRow(any, Row) (bool, error)
	}

	// A ChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ChunkTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillChunk method with a single thread.
	ChunkTableSource interface {
		sequentialTableSource
		// FillChunk takes a Chunk and fills it with values.
		// It returns true, if there are more chunks to fill.
		FillChunk(DataChunk) error
	}

	// A ParallelChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelChunkTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillChunk method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillChunk must use synchronization
	// primitives to avoid race conditions.
	ParallelChunkTableSource interface {
		parallelTableSource
		// FillChunk takes a Chunk and fills it with values.
		// It returns true, if there are more chunks to fill.
		FillChunk(any, DataChunk) error
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

func (tfd *tableFunctionData) setColumnCount(info apiInitInfo) {
	count := apiInitGetColumnCount(info)
	for i := uint64(0); i < count; i++ {
		srcPos := apiInitGetColumnIndex(info, i)
		tfd.projection[int(srcPos)] = int(i)
	}
}

//export table_udf_bind_row
func table_udf_bind_row(infoPtr unsafe.Pointer) {
	udfBindTyped[RowTableSource](infoPtr)
}

//export table_udf_bind_chunk
func table_udf_bind_chunk(infoPtr unsafe.Pointer) {
	udfBindTyped[ChunkTableSource](infoPtr)
}

//export table_udf_bind_parallel_row
func table_udf_bind_parallel_row(infoPtr unsafe.Pointer) {
	udfBindTyped[ParallelRowTableSource](infoPtr)
}

//export table_udf_bind_parallel_chunk
func table_udf_bind_parallel_chunk(infoPtr unsafe.Pointer) {
	udfBindTyped[ParallelChunkTableSource](infoPtr)
}

func udfBindTyped[T tableSource](infoPtr unsafe.Pointer) {
	info := apiBindInfo{Ptr: infoPtr}

	f := getPinned[tableFunction[T]](apiBindGetExtraInfo(info))
	config := f.Config

	argCount := len(config.Arguments)
	args := make([]any, argCount)
	namedArgs := make(map[string]any)

	for i, t := range config.Arguments {
		var err error
		value := apiBindGetParameter(info, uint64(i))
		args[i], err = getValue(t, value)
		apiDestroyValue(&value)

		if err != nil {
			apiBindSetError(info, err.Error())
			return
		}
	}

	for name, t := range config.NamedArguments {
		var err error
		value := apiBindGetNamedParameter(info, name)
		namedArgs[name], err = getValue(t, value)
		apiDestroyValue(&value)

		if err != nil {
			apiBindSetError(info, err.Error())
			return
		}
	}

	instance, err := f.BindArguments(namedArgs, args...)
	if err != nil {
		apiBindSetError(info, err.Error())
		return
	}

	columnInfos := instance.ColumnInfos()
	instanceData := tableFunctionData{
		fun:        instance,
		projection: make([]int, len(columnInfos)),
	}

	for i, v := range columnInfos {
		if v.T == nil {
			apiBindSetError(info, errTableUDFColumnTypeIsNil.Error())
			return
		}
		logicalType := v.T.logicalType()
		apiBindAddResultColumn(info, v.Name, logicalType)
		apiDestroyLogicalType(&logicalType)
		instanceData.projection[i] = -1
	}

	cardinality := instance.Cardinality()
	if cardinality != nil {
		apiBindSetCardinality(info, uint64(cardinality.Cardinality), cardinality.Exact)
	}

	pinnedInstanceData := pinnedValue[tableFunctionData]{
		pinner: &runtime.Pinner{},
		value:  instanceData,
	}

	h := cgo.NewHandle(pinnedInstanceData)
	pinnedInstanceData.pinner.Pin(&h)
	deleteCallbackPtr := unsafe.Pointer(C.table_udf_delete_callback_t(C.table_udf_delete_callback))
	apiBindSetBindData(info, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export table_udf_init
func table_udf_init(infoPtr unsafe.Pointer) {
	info := apiInitInfo{Ptr: infoPtr}
	instance := getPinned[tableFunctionData](apiInitGetBindData(info))
	instance.setColumnCount(info)
	instance.fun.(sequentialTableSource).Init()
}

//export table_udf_init_parallel
func table_udf_init_parallel(infoPtr unsafe.Pointer) {
	info := apiInitInfo{Ptr: infoPtr}
	instance := getPinned[tableFunctionData](apiInitGetBindData(info))
	instance.setColumnCount(info)
	initData := instance.fun.(parallelTableSource).Init()
	maxThreads := initData.MaxThreads
	apiInitSetMaxThreads(info, uint64(maxThreads))
}

//export table_udf_local_init
func table_udf_local_init(infoPtr unsafe.Pointer) {
	info := apiInitInfo{Ptr: infoPtr}
	instance := getPinned[tableFunctionData](apiInitGetBindData(info))
	localState := pinnedValue[any]{
		pinner: &runtime.Pinner{},
		value:  instance.fun.(parallelTableSource).NewLocalState(),
	}
	h := cgo.NewHandle(localState)
	localState.pinner.Pin(&h)
	deleteCallbackPtr := unsafe.Pointer(C.table_udf_delete_callback_t(C.table_udf_delete_callback))
	apiInitSetInitData(info, unsafe.Pointer(&h), deleteCallbackPtr)
}

//export table_udf_row_callback
func table_udf_row_callback(infoPtr unsafe.Pointer, outputPtr unsafe.Pointer) {
	info := apiFunctionInfo{Ptr: infoPtr}
	output := apiDataChunk{Ptr: outputPtr}

	instance := getPinned[tableFunctionData](apiFunctionGetBindData(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		apiFunctionSetError(info, err.Error())
		return
	}

	row := Row{
		chunk:      &chunk,
		projection: instance.projection,
	}
	maxSize := uint64(GetDataChunkCapacity())

	switch fun := instance.fun.(type) {
	case RowTableSource:
		// At the end of the loop row.r must be the index of the last row.
		for row.r = 0; row.r < maxSize; row.r++ {
			next, errRow := fun.FillRow(row)
			if errRow != nil {
				apiFunctionSetError(info, errRow.Error())
				break
			}
			if !next {
				break
			}
		}
	case ParallelRowTableSource:
		// At the end of the loop row.r must be the index of the last row.
		localState := getPinned[any](apiFunctionGetLocalInitData(info))
		for row.r = 0; row.r < maxSize; row.r++ {
			next, errRow := fun.FillRow(localState, row)
			if errRow != nil {
				apiFunctionSetError(info, errRow.Error())
				break
			}
			if !next {
				break
			}
		}
	}
	apiDataChunkSetSize(output, row.r)
}

//export table_udf_chunk_callback
func table_udf_chunk_callback(infoPtr unsafe.Pointer, outputPtr unsafe.Pointer) {
	info := apiFunctionInfo{Ptr: infoPtr}
	output := apiDataChunk{Ptr: outputPtr}

	instance := getPinned[tableFunctionData](apiFunctionGetBindData(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		apiFunctionSetError(info, err.Error())
		return
	}

	switch fun := instance.fun.(type) {
	case ChunkTableSource:
		err = fun.FillChunk(chunk)
	case ParallelChunkTableSource:
		localState := getPinned[*any](apiFunctionGetLocalInitData(info))
		err = fun.FillChunk(localState, chunk)
	}
	if err != nil {
		apiFunctionSetError(info, err.Error())
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
	function := apiCreateTableFunction()
	apiTableFunctionSetName(function, name)

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
	apiTableFunctionSetExtraInfo(function, unsafe.Pointer(&h), deleteCallbackPtr)

	apiTableFunctionSupportsProjectionPushdown(function, true)

	// Set the config.
	var x any = f
	switch tableFunc := x.(type) {
	case RowTableFunction:
		initCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_init))
		apiTableFunctionSetInit(function, initCallbackPtr)

		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_row))
		apiTableFunctionSetBind(function, bindCallbackPtr)

		callbackPtr := unsafe.Pointer(C.table_udf_callback_t(C.table_udf_row_callback))
		apiTableFunctionSetFunction(function, callbackPtr)

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ChunkTableFunction:
		initCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_init))
		apiTableFunctionSetInit(function, initCallbackPtr)

		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_chunk))
		apiTableFunctionSetBind(function, bindCallbackPtr)

		callbackPtr := unsafe.Pointer(C.table_udf_callback_t(C.table_udf_chunk_callback))
		apiTableFunctionSetFunction(function, callbackPtr)

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ParallelRowTableFunction:
		initCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_init_parallel))
		apiTableFunctionSetInit(function, initCallbackPtr)

		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_parallel_row))
		apiTableFunctionSetBind(function, bindCallbackPtr)

		callbackPtr := unsafe.Pointer(C.table_udf_callback_t(C.table_udf_row_callback))
		apiTableFunctionSetFunction(function, callbackPtr)

		localInitCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_local_init))
		apiTableFunctionSetLocalInit(function, localInitCallbackPtr)

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ParallelChunkTableFunction:
		initCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_init_parallel))
		apiTableFunctionSetInit(function, initCallbackPtr)

		bindCallbackPtr := unsafe.Pointer(C.table_udf_bind_t(C.table_udf_bind_parallel_chunk))
		apiTableFunctionSetBind(function, bindCallbackPtr)

		callbackPtr := unsafe.Pointer(C.table_udf_callback_t(C.table_udf_chunk_callback))
		apiTableFunctionSetFunction(function, callbackPtr)

		localInitCallbackPtr := unsafe.Pointer(C.table_udf_init_t(C.table_udf_local_init))
		apiTableFunctionSetLocalInit(function, localInitCallbackPtr)

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
		apiTableFunctionAddParameter(function, logicalType)
		apiDestroyLogicalType(&logicalType)
	}

	// Set the named arguments.
	for arg, t := range config.NamedArguments {
		if t == nil {
			return getError(errAPI, errTableUDFArgumentIsNil)
		}
		logicalType := t.logicalType()
		apiTableFunctionAddNamedParameter(function, arg, logicalType)
		apiDestroyLogicalType(&logicalType)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err := conn.Raw(func(driverConn any) error {
		c := driverConn.(*Conn)
		state := apiRegisterTableFunction(c.apiConn, function)
		apiDestroyTableFunction(&function)
		if apiState(state) == apiStateError {
			return getError(errAPI, errTableUDFCreate)
		}
		return nil
	})
	return err
}
