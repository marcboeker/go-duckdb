package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"reflect"
	"time"
	"unsafe"
)

// vector storage of a DuckDB column.
type vector struct {
	// The underlying DuckDB vector.
	duckdbVector C.duckdb_vector
	// A callback function to write to this vector.
	fn fnSetVectorValue
	// The data type of the vector.
	duckdbType C.duckdb_type
	// The child names of STRUCT vectors.
	childNames []string
	// The child vectors of nested data types.
	childVectors []vector
}

// fnSetVectorValue is the setter callback function for any (nested) vectors.
type fnSetVectorValue func(vec *vector, rowIdx C.idx_t, val any)

func (vec *vector) init(logicalType C.duckdb_logical_type, colIdx int) error {
	var err error
	duckdbType := C.duckdb_get_type_id(logicalType)

	switch duckdbType {
	case C.DUCKDB_TYPE_UTINYINT:
		initPrimitive[uint8](vec, C.DUCKDB_TYPE_UTINYINT)
	case C.DUCKDB_TYPE_TINYINT:
		initPrimitive[int8](vec, C.DUCKDB_TYPE_TINYINT)
	case C.DUCKDB_TYPE_USMALLINT:
		initPrimitive[uint16](vec, C.DUCKDB_TYPE_USMALLINT)
	case C.DUCKDB_TYPE_SMALLINT:
		initPrimitive[int16](vec, C.DUCKDB_TYPE_SMALLINT)
	case C.DUCKDB_TYPE_UINTEGER:
		initPrimitive[uint32](vec, C.DUCKDB_TYPE_UINTEGER)
	case C.DUCKDB_TYPE_INTEGER:
		initPrimitive[int32](vec, C.DUCKDB_TYPE_INTEGER)
	case C.DUCKDB_TYPE_UBIGINT:
		initPrimitive[uint64](vec, C.DUCKDB_TYPE_UBIGINT)
	case C.DUCKDB_TYPE_BIGINT:
		initPrimitive[int64](vec, C.DUCKDB_TYPE_BIGINT)
	case C.DUCKDB_TYPE_FLOAT:
		initPrimitive[float32](vec, C.DUCKDB_TYPE_FLOAT)
	case C.DUCKDB_TYPE_DOUBLE:
		initPrimitive[float64](vec, C.DUCKDB_TYPE_DOUBLE)
	case C.DUCKDB_TYPE_BOOLEAN:
		initPrimitive[bool](vec, C.DUCKDB_TYPE_BOOLEAN)
	case C.DUCKDB_TYPE_VARCHAR:
		vec.initVarchar()
	case C.DUCKDB_TYPE_BLOB:
		vec.initBlob()
	case C.DUCKDB_TYPE_TIMESTAMP:
		vec.initTS(C.DUCKDB_TYPE_TIMESTAMP)
	case C.DUCKDB_TYPE_UUID:
		vec.initUUID()
	case C.DUCKDB_TYPE_LIST:
		err = vec.initList(logicalType, colIdx)
	case C.DUCKDB_TYPE_STRUCT:
		err = vec.initStruct(logicalType)
	default:
		name, found := unsupportedAppenderTypeMap[duckdbType]
		if !found {
			name = "unknown type"
		}
		err = columnError(unsupportedTypeError(name), colIdx+1)
	}

	return err
}

func (vec *vector) getChildVectors(vector C.duckdb_vector) {
	switch vec.duckdbType {

	case C.DUCKDB_TYPE_LIST:
		child := C.duckdb_list_vector_get_child(vector)
		vec.childVectors[0].duckdbVector = child
		vec.childVectors[0].getChildVectors(child)

	case C.DUCKDB_TYPE_STRUCT:
		for i := 0; i < len(vec.childVectors); i++ {
			child := C.duckdb_struct_vector_get_child(vector, C.idx_t(i))
			vec.childVectors[i].duckdbVector = child
			vec.childVectors[i].getChildVectors(child)
		}
	}
}

func (vec *vector) setNull(rowIdx C.idx_t) {
	C.duckdb_vector_ensure_validity_writable(vec.duckdbVector)
	mask := C.duckdb_vector_get_validity(vec.duckdbVector)
	C.duckdb_validity_set_row_invalid(mask, rowIdx)

	if vec.duckdbType == C.DUCKDB_TYPE_STRUCT {
		for i := 0; i < len(vec.childVectors); i++ {
			vec.childVectors[i].setNull(rowIdx)
		}
	}
}

func setPrimitive[T any](vec *vector, rowIdx C.idx_t, v T) {
	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = v
}

func (vec *vector) setCString(rowIdx C.idx_t, value string, len int) {
	// This setter also writes BLOBs.
	str := C.CString(value)
	C.duckdb_vector_assign_string_element_len(vec.duckdbVector, rowIdx, str, C.idx_t(len))
	C.free(unsafe.Pointer(str))
}

func (vec *vector) setTime(rowIdx C.idx_t, value time.Time) {
	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(value.UTC().UnixMicro())
	setPrimitive[C.duckdb_timestamp](vec, rowIdx, ts)
}

func (vec *vector) setList(rowIdx C.idx_t, value driver.Value) {

	refVal := reflect.ValueOf(value)
	childVector := vec.childVectors[0]

	if refVal.IsNil() {
		vec.setNull(rowIdx)
	}

	// Convert the refVal to []any to iterate over it.
	values := make([]any, refVal.Len())
	for i := 0; i < refVal.Len(); i++ {
		values[i] = refVal.Index(i).Interface()
	}

	childVectorSize := C.duckdb_list_vector_get_size(vec.duckdbVector)

	// Set the offset and length of the list vector using the current size of the child vector.
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(refVal.Len()),
	}

	setPrimitive[C.duckdb_list_entry](vec, rowIdx, listEntry)

	newLength := C.idx_t(refVal.Len()) + childVectorSize
	C.duckdb_list_vector_set_size(vec.duckdbVector, newLength)
	C.duckdb_list_vector_reserve(vec.duckdbVector, newLength)

	// Insert the values into the child vector.
	for i, e := range values {
		childVectorRow := C.idx_t(i) + childVectorSize
		childVector.fn(&childVector, childVectorRow, e)
	}
}

func (vec *vector) setStruct(rowIdx C.idx_t, value driver.Value) {
	if value == nil {
		vec.setNull(rowIdx)
	}

	v := reflect.ValueOf(value)
	structType := v.Type()

	for i := 0; i < structType.NumField(); i++ {
		childVector := vec.childVectors[i]
		childVector.fn(&childVector, rowIdx, v.Field(i).Interface())
	}
}

func initPrimitive[T any](vec *vector, duckdbType C.duckdb_type) {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		setPrimitive[T](vec, rowIdx, val.(T))
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initVarchar() {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		v := val.(string)
		vec.setCString(rowIdx, v, len(v))
	}
	vec.duckdbType = C.DUCKDB_TYPE_VARCHAR
}

func (vec *vector) initBlob() {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		blob := val.([]byte)
		vec.setCString(rowIdx, string(blob[:]), len(blob))
	}
	vec.duckdbType = C.DUCKDB_TYPE_BLOB
}

func (vec *vector) initTS(duckdbType C.duckdb_type) {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.setTime(rowIdx, val.(time.Time))
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initUUID() {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		setPrimitive[C.duckdb_hugeint](vec, rowIdx, uuidToHugeInt(val.(UUID)))
	}
	vec.duckdbType = C.DUCKDB_TYPE_UUID
}

func (vec *vector) initList(logicalType C.duckdb_logical_type, colIdx int) error {
	// Get the child vector type.
	childType := C.duckdb_list_type_child_type(logicalType)
	defer C.duckdb_destroy_logical_type(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.setList(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_LIST
	return nil
}

func (vec *vector) initStruct(logicalType C.duckdb_logical_type) error {
	childCount := int(C.duckdb_struct_type_child_count(logicalType))
	var childNames []string
	for i := 0; i < childCount; i++ {
		childName := C.duckdb_struct_type_child_name(logicalType, C.idx_t(i))
		childNames = append(childNames, C.GoString(childName))
		C.free(unsafe.Pointer(childName))
	}

	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.setStruct(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_STRUCT
	vec.childVectors = make([]vector, childCount)
	vec.childNames = childNames

	// Recurse into the children.
	for i := 0; i < childCount; i++ {
		childType := C.duckdb_struct_type_child_type(logicalType, C.idx_t(i))
		err := vec.childVectors[i].init(childType, i)
		C.duckdb_destroy_logical_type(&childType)

		if err != nil {
			return err
		}
	}

	return nil
}
