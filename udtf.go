package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>

void udf_bind_row(duckdb_bind_info info);
void udf_bind_chunk(duckdb_bind_info info);

void udf_init(duckdb_init_info info);
void udf_local_init(duckdb_init_info info);

void udf_row_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837
void udf_chunk_callback(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19837

void udf_destroy_data(void *);

typedef void (*init)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*bind)(duckdb_function_info);  // https://golang.org/issue/19835
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);  // https://golang.org/issue/19835
*/
import "C"

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime/cgo"
	"unsafe"
)

type (
	// A ColumnMetaData value indicates the metatada of a column.
	// This is used, for example, to indicate the type of returned column for tablefunctions.
	ColumnMetaData struct {
		Name string // Name of the column
		T    Type   // type of the column
	}

	// CardinalityData is used to indicate the cardinality of a (table)function.
	// The IsExact is used to indicate wether or not the cardinality is exact.
	// When it is impossible/difficult to determine the  cardinality, an inexact cardinality
	// can be used.
	CardinalityData struct {
		Cardinality uint // Cardinality
		IsExact     bool // Wether or not the cardinality is exact
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

	threadedTableSource interface {
		Columns() ([]ColumnMetaData, error)
		Init() ThreadedTableSourceInitData
		Cardinality() *CardinalityData
		NewLocalState() any
	}

	tableSource interface {
		Columns() ([]ColumnMetaData, error)
		Init()
		Cardinality() *CardinalityData
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality will be requested before the function is initialised.
	// After the RowTableSource is initialised, the rows will be requested. The `FillRow` method
	// can be called by multiple threads at the same time.
	RowTableSource interface {
		tableSource
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
		tableSource
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
		Arguments      []Type
		NamedArguments map[string]Type
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

//export udf_destroy_data
func udf_destroy_data(data unsafe.Pointer) {
	h := *(*cgo.Handle)(data)
	h.Delete()
}

//export udf_bind_row
func udf_bind_row(info C.duckdb_bind_info) {
	udfBindTyped[RowTableSource](info)
}

//export udf_bind_chunk
func udf_bind_chunk(info C.duckdb_bind_info) {
	udfBindTyped[ChunkTableSource](info)
}

func udfBindTyped[T tableSource](info C.duckdb_bind_info) {
	extra_info := C.duckdb_bind_get_extra_info(info)
	h := *(*cgo.Handle)(extra_info)

	tfunc := h.Value().(tableFunction[T])
	config := tfunc.Config

	argCount := len(config.Arguments)
	args := make([]any, argCount)
	namedArgs := make(map[string]any)
	for i, t := range config.Arguments {
		value := C.duckdb_bind_get_parameter(info, C.idx_t(i))
		var err error
		args[i], err = getValue(t, value)
		if err != nil {
			errstr := C.CString(err.Error())
			defer C.free(unsafe.Pointer(errstr))
			C.duckdb_bind_set_error(info, errstr)
			return
		}
	}

	for name, t := range config.NamedArguments {
		argName := C.CString(name)
		defer C.free(unsafe.Pointer(argName))
		value := C.duckdb_bind_get_named_parameter(info, argName)

		var err error
		namedArgs[name], err = getValue(t, value)
		if err != nil {
			errstr := C.CString(err.Error())
			defer C.free(unsafe.Pointer(errstr))
			C.duckdb_bind_set_error(info, errstr)
			return
		}
	}

	instance, err := tfunc.BindArguments(namedArgs, args...)
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_bind_set_error(info, errstr)
		return
	}

	columns, err := instance.Columns()
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_bind_set_error(info, errstr)
		return
	}

	instanceData := tableFunctionData{
		fun:        instance,
		projection: make([]int, len(columns)),
	}

	for i, v := range columns {
		dt := v.T.toDuckdb()
		defer C.duckdb_destroy_logical_type(&dt)
		colName := C.CString(v.Name)
		defer C.free(unsafe.Pointer(colName))
		C.duckdb_bind_add_result_column(info, colName, dt)

		instanceData.projection[i] = -1
	}

	cardinality := instance.Cardinality()
	if cardinality != nil {
		C.duckdb_bind_set_cardinality(info, C.idx_t(cardinality.Cardinality), C.bool(cardinality.IsExact))
	}

	handle := cgo.NewHandle(instanceData)
	C.duckdb_bind_set_bind_data(info, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_destroy_data))
}

//export udf_init
func udf_init(info C.duckdb_init_info) {
	h := *(*cgo.Handle)(C.duckdb_init_get_bind_data(info))
	fmt.Printf("GET HANDLE: %p: %v\n", (*cgo.Handle)(C.duckdb_init_get_bind_data(info)), h)
	instance := h.Value().(tableFunctionData)

	columnCount := C.duckdb_init_get_column_count(info)
	for i := C.idx_t(0); i < columnCount; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, i))
		instance.projection[srcPos] = int(i)
	}
	instance.fun.(tableSource).Init()
}

//export udf_init_threaded
func udf_init_threaded(info C.duckdb_init_info) {
	h := *(*cgo.Handle)(C.duckdb_init_get_bind_data(info))
	fmt.Printf("GET HANDLE: %p: %v\n", (*cgo.Handle)(C.duckdb_init_get_bind_data(info)), h)
	instance := h.Value().(tableFunctionData)
	
	columnCount := C.duckdb_init_get_column_count(info)
	for i := C.idx_t(0); i < columnCount; i++ {
		srcPos := int(C.duckdb_init_get_column_index(info, i))
		instance.projection[srcPos] = int(i)
	}

	initData := instance.fun.(threadedTableSource).Init()
	C.duckdb_init_set_max_threads(info, C.idx_t(initData.MaxThreads))
}

//export udf_local_init
func udf_local_init(info C.duckdb_init_info) {
	h := *(*cgo.Handle)(C.duckdb_init_get_bind_data(info))
	fmt.Printf("GET HANDLE: %p: %v\n", (*cgo.Handle)(C.duckdb_init_get_bind_data(info)), h)
	instance := h.Value().(tableFunctionData)
	localState := instance.fun.(threadedTableSource).NewLocalState()
	handle := cgo.NewHandle(localState)
	C.duckdb_init_set_init_data(info, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_destroy_data))
}

//export udf_row_callback
func udf_row_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	h := *(*cgo.Handle)(C.duckdb_function_get_bind_data(info))
	fmt.Printf("GET HANDLE: %p: %v\n", (*cgo.Handle)(C.duckdb_function_get_bind_data(info)), h)
	instance := h.Value().(tableFunctionData)

	var chunk DataChunk
	err := chunk.initFromChunk(output)
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_function_set_error(info, errstr)
	}

	row := Row{
		chunk:      chunk,
		projection: instance.projection,
	}

	maxSize := C.duckdb_vector_size()

	switch fun := instance.fun.(type) {
	case RowTableSource:
		// At the end of the loop row.r must be the index one past the last added row
		for row.r = 0; row.r < maxSize; row.r++ {
			nextResults, err := fun.FillRow(row)
			if err != nil {
				errstr := C.CString(err.Error())
				defer C.free(unsafe.Pointer(errstr))
				C.duckdb_function_set_error(info, errstr)
			}
			if !nextResults {
				break
			}
		}
	case ThreadedRowTableSource:
		localstateHandle := *(*cgo.Handle)(C.duckdb_function_get_local_init_data(info))
		localState := localstateHandle.Value()

		// At the end of the loop row.r must be the index one past the last added row
		for row.r = 0; row.r < maxSize; row.r++ {
			nextResults, err := fun.FillRow(localState, row)
			if err != nil {
				errstr := C.CString(err.Error())
				defer C.free(unsafe.Pointer(errstr))
				C.duckdb_function_set_error(info, errstr)
			}
			if !nextResults {
				break
			}
		}
	}
	// since row.r points to one past the last value, it is also the size
	C.duckdb_data_chunk_set_size(output, row.r)
}

//export udf_chunk_callback
func udf_chunk_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	h := *(*cgo.Handle)(C.duckdb_function_get_bind_data(info))
	fmt.Printf("GET HANDLE: %p: %v\n", (*cgo.Handle)(C.duckdb_function_get_bind_data(info)), h)
	instance := h.Value().(tableFunctionData)
	var chunk DataChunk
	err := chunk.initFromChunk(output)
	if err != nil {
		errstr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(errstr))
		C.duckdb_function_set_error(info, errstr)
	}

	switch fun := instance.fun.(type) {
	case ChunkTableSource:
		err := fun.FillChunk(chunk)
		if err != nil {
			errstr := C.CString(err.Error())
			defer C.free(unsafe.Pointer(errstr))
			C.duckdb_function_set_error(info, errstr)
		}
	case ThreadedChunkTableSource:
		localstateHandle := *(*cgo.Handle)(C.duckdb_function_get_local_init_data(info))
		localState := localstateHandle.Value()
		err := fun.FillChunk(localState, chunk)
		if err != nil {
			errstr := C.CString(err.Error())
			defer C.free(unsafe.Pointer(errstr))
			C.duckdb_function_set_error(info, errstr)
		}
	}
}

// RegisterTableUDF registers a TableFunctionProvider to duckdb.
// Projectionpushdown is enabled by default, and implemented transparently.
func RegisterTableUDF[TFT TableFunction](c *sql.Conn, name string, function TFT) error {
	err := c.Raw(func(dconn any) error {
		ddconn := dconn.(*conn)
		name := C.CString(name)
		defer C.free(unsafe.Pointer(name))

		handle := cgo.NewHandle(function)
		fmt.Printf("CREATE FUNCTION: %p: %v\n", &handle, handle)
		tableFunction := C.duckdb_create_table_function()
		C.duckdb_table_function_set_name(tableFunction, name)
		C.duckdb_table_function_set_init(tableFunction, C.init(C.udf_init))
		C.duckdb_table_function_set_extra_info(tableFunction, unsafe.Pointer(&handle), C.duckdb_delete_callback_t(C.udf_destroy_data))
		C.duckdb_table_function_supports_projection_pushdown(tableFunction, C.bool(true))

		var config TableFunctionConfig
		var x any = function
		switch tfunc := x.(type) {
		case RowTableFunction:
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind_row))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_row_callback))

			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return invalidTableFunctionError()
			}
		case ChunkTableFunction:
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind_chunk))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_chunk_callback))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return invalidTableFunctionError()
			}
		case ThreadedRowTableFunction:
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind_row))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_row_callback))
			C.duckdb_table_function_set_local_init(tableFunction, C.init(C.udf_local_init))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return invalidTableFunctionError()
			}
		case ThreadedChunkTableFunction:
			C.duckdb_table_function_set_bind(tableFunction, C.bind(C.udf_bind_chunk))
			C.duckdb_table_function_set_function(tableFunction, C.callback(C.udf_chunk_callback))
			C.duckdb_table_function_set_local_init(tableFunction, C.init(C.udf_local_init))
			config = tfunc.Config
			if tfunc.BindArguments == nil {
				return invalidTableFunctionError()
			}
		default:
			panic("This code should be unreachable, please open a bug report for go-duckdb.")
		}

		for _, t := range config.Arguments {
			dt := t.toDuckdb()
			defer C.duckdb_destroy_logical_type(&dt)
			C.duckdb_table_function_add_parameter(tableFunction, dt)
		}

		for name, t := range config.NamedArguments {
			dt := t.toDuckdb()
			defer C.duckdb_destroy_logical_type(&dt)
			argName := C.CString(name)
			defer C.free(unsafe.Pointer(argName))
			C.duckdb_table_function_add_named_parameter(tableFunction, argName, dt)
		}

		state := C.duckdb_register_table_function(ddconn.duckdbCon, tableFunction)
		if state != 0 {
			return invalidTableFunctionError()
		}
		return nil
	})
	return err
}

func getValue(t Type, v C.duckdb_value) (any, error) {
	switch t.t0.Kind() {
	case reflect.Uint8:
		return uint8(C.duckdb_get_int64(v)), nil
	case reflect.Uint16:
		return uint16(C.duckdb_get_int64(v)), nil
	case reflect.Uint32:
		return uint32(C.duckdb_get_int64(v)), nil
	case reflect.Int8:
		return int8(C.duckdb_get_int64(v)), nil
	case reflect.Int16:
		return int16(C.duckdb_get_int64(v)), nil
	case reflect.Int32:
		return int32(C.duckdb_get_int64(v)), nil
	case reflect.Int64:
		return int64(C.duckdb_get_int64(v)), nil
	case reflect.String:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	default:
		return nil, unsupportedTypeError(t.t0.String())
	}
}
