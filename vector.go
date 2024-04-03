package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
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

func tryPrimitiveCast[T any](val any, expected string) (any, error) {
	if v, ok := val.(T); ok {
		return v, nil
	}

	goType := reflect.TypeOf(val)
	return nil, castError(goType.String(), expected)
}

func (vec *vector) tryCast(val driver.Value) (any, error) {
	if val == nil {
		return val, nil
	}

	switch vec.duckdbType {
	case C.DUCKDB_TYPE_UTINYINT:
		return tryPrimitiveCast[uint8](val, reflect.Uint8.String())
	case C.DUCKDB_TYPE_TINYINT:
		return tryPrimitiveCast[int8](val, reflect.Int8.String())
	case C.DUCKDB_TYPE_USMALLINT:
		return tryPrimitiveCast[uint16](val, reflect.Uint16.String())
	case C.DUCKDB_TYPE_SMALLINT:
		return tryPrimitiveCast[int16](val, reflect.Int16.String())
	case C.DUCKDB_TYPE_UINTEGER:
		return tryPrimitiveCast[uint32](val, reflect.Uint32.String())
	case C.DUCKDB_TYPE_INTEGER:
		return tryPrimitiveCast[int32](val, reflect.Int32.String())
	case C.DUCKDB_TYPE_UBIGINT:
		return tryPrimitiveCast[uint64](val, reflect.Uint64.String())
	case C.DUCKDB_TYPE_BIGINT:
		return tryPrimitiveCast[int64](val, reflect.Int64.String())
	case C.DUCKDB_TYPE_FLOAT:
		return tryPrimitiveCast[float32](val, reflect.Float32.String())
	case C.DUCKDB_TYPE_DOUBLE:
		return tryPrimitiveCast[float64](val, reflect.Float64.String())
	case C.DUCKDB_TYPE_BOOLEAN:
		return tryPrimitiveCast[bool](val, reflect.Bool.String())
	case C.DUCKDB_TYPE_VARCHAR:
		return tryPrimitiveCast[string](val, reflect.String.String())
	case C.DUCKDB_TYPE_BLOB:
		return tryPrimitiveCast[[]byte](val, reflect.TypeOf([]byte{}).String())
	case C.DUCKDB_TYPE_TIMESTAMP:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_UUID:
		return tryPrimitiveCast[UUID](val, reflect.TypeOf(UUID{}).String())
	case C.DUCKDB_TYPE_LIST:
		return vec.tryCastList(val)
	case C.DUCKDB_TYPE_STRUCT:
		return vec.tryCastStruct(val)
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	}

	return nil, getError(errDriver, nil)
}

func (vec *vector) canNil(val reflect.Value) bool {
	//if val.K
	return true
}

func (vec *vector) tryCastList(val driver.Value) ([]any, error) {
	goType := reflect.TypeOf(val)
	if goType.Kind() != reflect.Slice {
		return nil, castError(goType.String(), reflect.Slice.String())
	}

	v := reflect.ValueOf(val)
	childVector := vec.childVectors[0]

	// Convert v to []any.
	list := make([]any, v.Len())
	var err error

	for i := 0; i < v.Len(); i++ {

		idx := v.Index(i)

		fmt.Println(idx)
		if idx.IsNil() {
			list[i] = nil
			continue
		}
		//if idx.IsNil() {
		//	list[i] = nil
		//	continue
		//}
		list[i], err = childVector.tryCast(idx)
		if err != nil {
			return nil, err
		}
	}
	return list, nil
}

func (vec *vector) tryCastStruct(val driver.Value) (map[string]any, error) {
	m, isMap := val.(map[string]any)

	// Transform the struct into map[string]any.
	if !isMap {
		// Catch mismatching types.
		goType := reflect.TypeOf(val)
		if reflect.TypeOf(val).Kind() != reflect.Struct {
			return nil, castError(goType.String(), reflect.Struct.String())
		}

		m = make(map[string]any)
		v := reflect.ValueOf(val)
		structType := v.Type()

		for i := 0; i < structType.NumField(); i++ {
			fieldName := structType.Field(i).Name
			m[fieldName] = v.Field(i).Interface()
		}
	}

	// Catch mismatching field count.
	if len(m) != len(vec.childNames) {
		return nil, structFieldError(strconv.Itoa(len(m)), strconv.Itoa(len(vec.childNames)))
	}

	// Cast child entries and return the map.
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := vec.childVectors[i]
		childName := vec.childNames[i]
		v, ok := m[childName]

		// Catch mismatching field names.
		if !ok {
			return nil, structFieldError("", childName)
		}

		var err error
		m[childName], err = childVector.tryCast(v)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

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
		vec.initTS(duckdbType)
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		vec.initTS(duckdbType)
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		vec.initTS(duckdbType)
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		vec.initTS(duckdbType)
	case C.DUCKDB_TYPE_UUID:
		vec.initUUID()
	case C.DUCKDB_TYPE_LIST:
		err = vec.initList(logicalType, colIdx)
	case C.DUCKDB_TYPE_STRUCT:
		err = vec.initStruct(logicalType)
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		vec.initTS(duckdbType)
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

func setPrimitive[T any](vec *vector, rowIdx C.idx_t, v any) {
	if v == nil {
		vec.setNull(rowIdx)
		return
	}

	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = v.(T)
}

func (vec *vector) setCString(rowIdx C.idx_t, value string, len int) {
	// This setter also writes BLOBs.
	str := C.CString(value)
	C.duckdb_vector_assign_string_element_len(vec.duckdbVector, rowIdx, str, C.idx_t(len))
	C.free(unsafe.Pointer(str))
}

func (vec *vector) setTime(rowIdx C.idx_t, ticks int64) {
	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(ticks)
	setPrimitive[C.duckdb_timestamp](vec, rowIdx, ts)
}

func (vec *vector) setList(rowIdx C.idx_t, v []any) {
	childVectorSize := C.duckdb_list_vector_get_size(vec.duckdbVector)

	// Set the offset and length of the list vector using the current size of the child vector.
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(len(v)),
	}

	setPrimitive[C.duckdb_list_entry](vec, rowIdx, listEntry)

	newLength := C.idx_t(len(v)) + childVectorSize
	C.duckdb_list_vector_set_size(vec.duckdbVector, newLength)
	C.duckdb_list_vector_reserve(vec.duckdbVector, newLength)

	// Insert the values into the child vector.
	childVector := vec.childVectors[0]
	for i, e := range v {
		offset := C.idx_t(i) + childVectorSize
		childVector.fn(&childVector, offset, e)
	}
}

func (vec *vector) setStruct(rowIdx C.idx_t, m map[string]any) {
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := vec.childVectors[i]
		childName := vec.childNames[i]
		childVector.fn(&childVector, rowIdx, m[childName])
	}
}

func initPrimitive[T any](vec *vector, duckdbType C.duckdb_type) {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		setPrimitive[T](vec, rowIdx, val)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initVarchar() {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		v := val.(string)
		vec.setCString(rowIdx, v, len(v))
	}
	vec.duckdbType = C.DUCKDB_TYPE_VARCHAR
}

func (vec *vector) initBlob() {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		blob := val.([]byte)
		vec.setCString(rowIdx, string(blob[:]), len(blob))
	}
	vec.duckdbType = C.DUCKDB_TYPE_BLOB
}

func (vec *vector) initTS(duckdbType C.duckdb_type) {
	vec.fn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		v := val.(time.Time)
		var ticks int64
		switch duckdbType {
		case C.DUCKDB_TYPE_TIMESTAMP:
			ticks = v.UTC().UnixMicro()
		case C.DUCKDB_TYPE_TIMESTAMP_S:
			ticks = v.UTC().Unix()
		case C.DUCKDB_TYPE_TIMESTAMP_MS:
			ticks = v.UTC().UnixMilli()
		case C.DUCKDB_TYPE_TIMESTAMP_NS:
			ticks = v.UTC().UnixNano()
		case C.DUCKDB_TYPE_TIMESTAMP_TZ:
			ticks = v.UTC().UnixMicro()
		}
		vec.setTime(rowIdx, ticks)
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
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		v := val.([]any)
		vec.setList(rowIdx, v)
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
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		v := val.(map[string]any)
		vec.setStruct(rowIdx, v)
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
