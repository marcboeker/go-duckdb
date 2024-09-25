package duckdb

/*
#include <duckdb.h>

void table_udf_bind_row(duckdb_bind_info info);
void table_udf_bind_chunk(duckdb_bind_info info);
void table_udf_bind_par_row(duckdb_bind_info info);
void table_udf_bind_par_chunk(duckdb_bind_info info);

void table_udf_init(duckdb_init_info info);
void table_udf_init_threaded(duckdb_init_info info);
void table_udf_local_init(duckdb_init_info info);

void table_udf_row_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837
void table_udf_chunk_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835

void udf_delete_callback(void *);
*/
import "C"

import (
	"database/sql"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

type (
	// A ColumnInfo value indicates the metatada of a column.
	// This is used, for example, to indicate the type of returned column for tablefunctions.
	ColumnInfo struct {
		Name string   // Name of the column
		T    TypeInfo // type of the column
	}

	// CardinalityInfo is used to indicate the cardinality of a (table)function.
	// The IsExact is used to indicate wether or not the cardinality is exact.
	// When it is impossible/difficult to determine the  cardinality, an inexact cardinality
	// can be used.
	CardinalityInfo struct {
		Cardinality uint // Cardinality
		Exact       bool // Wether or not the cardinality is exact
	}

	// ThreadedTableSourceInitData contains any info that can be passed to duckdb when
	// initialising the tablefunction.
	ThreadedTableSourceInitData struct {
		// On how many threads the TableSource is allowed to run.
		// If left at 0, the default from duckdb is used. This is may change in the future.
		MaxThreads int
	}

	tableFunctionData struct {
		fun        any
		projection []int
	}

	tableSource interface {
		Columns() []ColumnInfo
		Cardinality() *CardinalityInfo
	}

	threadedTableSource interface {
		tableSource
		Init() ThreadedTableSourceInitData
		// NewLocalState must return a pointer, or reference type.
		// If this is not the case, the state cannot be updated
		// by FillRow or FillChunk. go-duckdb does not prevent
		// non-reference values.
		NewLocalState() any
	}

	unthreadedTableSource interface {
		tableSource
		Init()
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality will be requested before the function is initialised.
	// After the RowTableSource is initialised, the rows will be requested. The `FillRow` method
	// can be called by multiple threads at the same time.
	RowTableSource interface {
		unthreadedTableSource
		FillRow(Row) (bool, error)
	}

	// A ThreadedRowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality will be requested before the function is initialised.
	// After the ThreadedRowTableSource is initialised, the rows will be requested. The `FillRow`
	// method can be called by multiple threads at the same time. If `TableSourceInitData.MaxThreads`
	// is not 1, `FillRow` must use synchronisation primitives to avoid race conditions.
	ThreadedRowTableSource interface {
		threadedTableSource
		FillRow(any, Row) (bool, error)
	}

	// A ChunkTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality will be requested before the function is initialised.
	// After the ChunkTableSource is initialised, the rows will be requested. The `FillRow` method
	// can be called by multiple threads at the same time.
	ChunkTableSource interface {
		unthreadedTableSource
		FillChunk(DataChunk) error
	}

	// A ThreadedChunkTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality will be requested before the function is initialised.
	// After the ThreadedChunkTableSource is initialised, the rows will be requested. The `FillRow`
	// method can be called by multiple threads at the same time. If `TableSourceInitData.MaxThreads`
	// is not 1, `FillChunk` must use synchronisation primitives to avoid race conditions.
	ThreadedChunkTableSource interface {
		threadedTableSource
		FillChunk(any, DataChunk) error
	}

	// TableFunctionConfig contains any information passed to duckdb when registring the
	// tablefunction. At the moment this mostly consists of the arguments of the function.
	TableFunctionConfig struct {
		Arguments      []TypeInfo
		NamedArguments map[string]TypeInfo
	}

	TableFunction interface {
		RowTableFunction | ChunkTableFunction | ThreadedRowTableFunction | ThreadedChunkTableFunction
	}

	// A RowTableFunction is a type which can be bound to return a RowTableSource.
	// The `Config` method returns the configuration, including the arguments the function
	// take. `BindArguments` binds the arguments, returning a TableSource.
	RowTableFunction = tableFunction[RowTableSource]
	// A ChunkTableFunction is a type which can be bound to return a ChunkTableSource.
	// The `Config` method returns the configuration, including the arguments the function
	// take. `BindArguments` binds the arguments, returning a TableSource.
	ChunkTableFunction = tableFunction[ChunkTableSource]
	// A ThreadedRowTableFunction is a type which can be bound to return a ThreadedRowTableSource.
	// The `Config` method returns the configuration, including the arguments the function
	// take. `BindArguments` binds the arguments, returning a TableSource.
	ThreadedRowTableFunction = tableFunction[ThreadedRowTableSource]
	// A ThreadedChunkTableFunction is a type which can be bound to return a ThreadedChunkTableSource.
	// The `Config` method returns the configuration, including the arguments the function
	// take. `BindArguments` binds the arguments, returning a TableSource.
	ThreadedChunkTableFunction = tableFunction[ThreadedChunkTableSource]

	tableFunction[T any] struct {
		Config        TableFunctionConfig
		BindArguments func(named map[string]any, args ...any) (T, error)
	}
)

func (tfd *tableFunctionData) setColumnCount(info C.duckdb_init_info) {
	columnCount := C.duckdb_init_get_column_count(info)
	for i := C.idx_t(0); i < columnCount; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, i))
		tfd.projection[srcPos] = int(i)
	}
}

//export table_udf_bind_row
func table_udf_bind_row(info C.duckdb_bind_info) {
	udfBindTyped[RowTableSource](info)
}

//export table_udf_bind_chunk
func table_udf_bind_chunk(info C.duckdb_bind_info) {
	udfBindTyped[ChunkTableSource](info)
}

//export table_udf_bind_par_row
func table_udf_bind_par_row(info C.duckdb_bind_info) {
	udfBindTyped[ThreadedRowTableSource](info)
}

//export table_udf_bind_par_chunk
func table_udf_bind_par_chunk(info C.duckdb_bind_info) {
	udfBindTyped[ThreadedChunkTableSource](info)
}

func udfBindTyped[T tableSource](info C.duckdb_bind_info) {
	tfunc := getPinned[tableFunction[T]](C.duckdb_bind_get_extra_info(info))
	config := tfunc.Config

	argCount := len(config.Arguments)
	args := make([]any, argCount)
	namedArgs := make(map[string]any)
	for i, t := range config.Arguments {
		value := C.duckdb_bind_get_parameter(info, C.idx_t(i))
		var err error
		args[i], err = getValue(t, value)
		if err != nil {
			setBindError(info, err.Error())
			return
		}
	}

	for name, t := range config.NamedArguments {
		argName := C.CString(name)
		defer C.duckdb_free(unsafe.Pointer(argName))
		value := C.duckdb_bind_get_named_parameter(info, argName)

		var err error
		namedArgs[name], err = getValue(t, value)
		if err != nil {
			setBindError(info, err.Error())
			return
		}
	}

	instance, err := tfunc.BindArguments(namedArgs, args...)
	if err != nil {
		setBindError(info, err.Error())
		return
	}

	columns := instance.Columns()

	instanceData := tableFunctionData{
		fun:        instance,
		projection: make([]int, len(columns)),
	}

	for i, v := range columns {
		dt := v.T.logicalType()
		defer C.duckdb_destroy_logical_type(&dt)
		colName := C.CString(v.Name)
		defer C.duckdb_free(unsafe.Pointer(colName))
		C.duckdb_bind_add_result_column(info, colName, dt)

		instanceData.projection[i] = -1
	}

	cardinality := instance.Cardinality()
	if cardinality != nil {
		C.duckdb_bind_set_cardinality(info, C.idx_t(cardinality.Cardinality), C.bool(cardinality.Exact))
	}

	pinnedInstanceData := pinnedValue[tableFunctionData]{
		pinner: &runtime.Pinner{},
		value:  instanceData,
	}

	handle := cgo.NewHandle(pinnedInstanceData)
	pinnedInstanceData.pinner.Pin(&handle)
	C.duckdb_bind_set_bind_data(info, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_delete_callback))
}

//export table_udf_init
func table_udf_init(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	instance.setColumnCount(info)
	instance.fun.(unthreadedTableSource).Init()
}

//export table_udf_init_threaded
func table_udf_init_threaded(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	instance.setColumnCount(info)
	initData := instance.fun.(threadedTableSource).Init()
	C.duckdb_init_set_max_threads(info, C.idx_t(initData.MaxThreads))
}

//export table_udf_local_init
func table_udf_local_init(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	localState := pinnedValue[any]{
		pinner: &runtime.Pinner{},
		value: instance.fun.(threadedTableSource).NewLocalState(), 
	}
	handle := cgo.NewHandle(localState)
	localState.pinner.Pin(&handle)
	C.duckdb_init_set_init_data(info, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_delete_callback))
}

//export table_udf_row_callback
func table_udf_row_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	instance := getPinned[tableFunctionData](C.duckdb_function_get_bind_data(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		setFuncError(info, err.Error())
		return
	}

	row := Row{
		chunk:      &chunk,
		projection: instance.projection,
	}

	maxSize := C.duckdb_vector_size()

	switch fun := instance.fun.(type) {
	case RowTableSource:
		// At the end of the loop row.r must be the index one past the last added row
		for row.r = 0; row.r < maxSize; row.r++ {
			nextResults, err := fun.FillRow(row)
			if err != nil {
				setFuncError(info, err.Error())
			}
			if !nextResults {
				break
			}
		}
	case ThreadedRowTableSource:
		localState := getPinned[any](C.duckdb_function_get_local_init_data(info))

		// At the end of the loop row.r must be the index one past the last added row
		for row.r = 0; row.r < maxSize; row.r++ {
			nextResults, err := fun.FillRow(localState, row)
			if err != nil {
				setFuncError(info, err.Error())
			}
			if !nextResults {
				break
			}
		}
	}

	// since row.r points to one past the last value, it is also the size
	C.duckdb_data_chunk_set_size(output, row.r)
}

//export table_udf_chunk_callback
func table_udf_chunk_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	instance := getPinned[tableFunctionData](C.duckdb_function_get_bind_data(info))

	var chunk DataChunk
	// false, maybe this needs to be true but I don't think we need to read the validity mask
	// It should be set after executing the function, but never read.
	err := chunk.initFromDuckDataChunk(output, false)
	if err != nil {
		setFuncError(info, err.Error())
		return
	}

	switch fun := instance.fun.(type) {
	case ChunkTableSource:
		err := fun.FillChunk(chunk)
		if err != nil {
			setFuncError(info, err.Error())
		}
	case ThreadedChunkTableSource:
		localState := getPinned[*any](C.duckdb_function_get_local_init_data(info))
		err := fun.FillChunk(localState, chunk)
		if err != nil {
			setFuncError(info, err.Error())
		}
	}
}

// RegisterTableUDF registers a TableFunctionProvider to duckdb.
// Projectionpushdown is enabled by default, and implemented transparently.
func RegisterTableUDF[TFT TableFunction](c *sql.Conn, name string, function TFT) error {
	if name == "" {
		return errTableUDFNoName
	}
	err := c.Raw(func(dconn any) error {
		ddconn := dconn.(*conn)
		name := C.CString(name)
		defer C.duckdb_free(unsafe.Pointer(name))

		cfunction := pinnedValue[TFT]{
			pinner: &runtime.Pinner{},
			value:  function,
		}

		handle := cgo.NewHandle(cfunction)
		cfunction.pinner.Pin(&handle)
		tableFunction := C.duckdb_create_table_function()
		C.duckdb_table_function_set_name(tableFunction, name)
		C.duckdb_table_function_set_extra_info(tableFunction, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_delete_callback))
		C.duckdb_table_function_supports_projection_pushdown(tableFunction, C.bool(true))

		var config TableFunctionConfig
		var x any = function
		switch tfunc := x.(type) {
		case RowTableFunction:
			C.duckdb_table_function_set_init(tableFunction, C.init(C.table_udf_init))
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.table_udf_bind_row))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.table_udf_row_callback))

			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return errTableUDFMissingBindags
			}
		case ChunkTableFunction:
			C.duckdb_table_function_set_init(tableFunction, C.init(C.table_udf_init))
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.table_udf_bind_chunk))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.table_udf_chunk_callback))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return errTableUDFMissingBindags
			}
		case ThreadedRowTableFunction:
			C.duckdb_table_function_set_init(tableFunction, C.init(C.table_udf_init_threaded))
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.table_udf_bind_par_row))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.table_udf_row_callback))
			C.duckdb_table_function_set_local_init(tableFunction, C.init(C.table_udf_local_init))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return errTableUDFMissingBindags
			}
		case ThreadedChunkTableFunction:
			C.duckdb_table_function_set_init(tableFunction, C.init(C.table_udf_init_threaded))
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.table_udf_bind_par_chunk))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.table_udf_chunk_callback))
			C.duckdb_table_function_set_local_init(tableFunction, C.init(C.table_udf_local_init))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return errTableUDFMissingBindags
			}
		default:
			panic("This code should be unreachable, please open a bug report for go-duckdb.")
		}

		for _, t := range config.Arguments {
			dt := t.logicalType()
			defer C.duckdb_destroy_logical_type(&dt)
			C.duckdb_table_function_add_parameter(tableFunction, dt)
		}

		for name, t := range config.NamedArguments {
			dt := t.logicalType()
			defer C.duckdb_destroy_logical_type(&dt)
			argName := C.CString(name)
			defer C.duckdb_free(unsafe.Pointer(argName))
			C.duckdb_table_function_add_named_parameter(tableFunction, argName, dt)
		}

		state := C.duckdb_register_table_function(ddconn.duckdbCon, tableFunction)
		C.duckdb_destroy_table_function(&tableFunction)
		if state != 0 {
			return errTableUDFCreate
		}
		return nil
	})
	return err
}

func getValue(t TypeInfo, v C.duckdb_value) (any, error) {
	// FIXME: Add support for more types
	switch t.(*typeInfo).Type {
	case TYPE_UTINYINT:
		return uint8(C.duckdb_get_int64(v)), nil
	case TYPE_USMALLINT:
		return uint16(C.duckdb_get_int64(v)), nil
	case TYPE_UINTEGER:
		return uint32(C.duckdb_get_int64(v)), nil
	case TYPE_TINYINT:
		return int8(C.duckdb_get_int64(v)), nil
	case TYPE_SMALLINT:
		return int16(C.duckdb_get_int64(v)), nil
	case TYPE_INTEGER:
		return int32(C.duckdb_get_int64(v)), nil
	case TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
	case TYPE_VARCHAR:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t.(*typeInfo).Type])
	}
}
