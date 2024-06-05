package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx C.idx_t) any

func (vec *vector) getNull(rowIdx C.idx_t) bool {
	mask := C.duckdb_vector_get_validity(vec.duckdbVector)
	return !bool(C.duckdb_validity_row_is_valid(mask, rowIdx))
}

func getPrimitive[T any](vec *vector, rowIdx C.idx_t) T {
	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	return xs[rowIdx]
}

func (vec *vector) getTS(duckdbType C.duckdb_type, rowIdx C.idx_t) time.Time {
	val := getPrimitive[C.duckdb_timestamp](vec, rowIdx)
	micros := val.micros

	switch duckdbType {
	case C.DUCKDB_TYPE_TIMESTAMP:
		return time.UnixMicro(int64(micros)).UTC()
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		return time.Unix(int64(micros), 0).UTC()
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		return time.UnixMilli(int64(micros)).UTC()
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		return time.Unix(0, int64(micros)).UTC()
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(int64(micros)).UTC()
	}

	return time.Time{}
}

func (vec *vector) getDate(rowIdx C.idx_t) time.Time {
	primitiveDate := getPrimitive[C.duckdb_date](vec, rowIdx)
	date := C.duckdb_from_date(primitiveDate)
	return time.Date(int(date.year), time.Month(date.month), int(date.day), 0, 0, 0, 0, time.UTC)
}

func (vec *vector) getTime(rowIdx C.idx_t) time.Time {
	val := getPrimitive[C.duckdb_time](vec, rowIdx)
	micros := val.micros
	return time.UnixMicro(int64(micros)).UTC()
}

func (vec *vector) getCString(rowIdx C.idx_t) string {
	cStr := getPrimitive[duckdb_string_t](vec, rowIdx)
	if cStr.length <= stringInlineLength {
		// Inlined data is stored from byte 4 to stringInlineLength + 4.
		return string(C.GoBytes(unsafe.Pointer(&cStr.prefix), C.int(cStr.length)))
	}

	// Any strings exceeding stringInlineLength are stored as a pointer in `ptr`.
	return string(C.GoBytes(unsafe.Pointer(cStr.ptr), C.int(cStr.length)))
}

func (vec *vector) getEnum(idx uint64) string {
	logicalType := C.duckdb_vector_get_column_type(vec.duckdbVector)
	defer C.duckdb_destroy_logical_type(&logicalType)

	val := C.duckdb_enum_dictionary_value(logicalType, (C.idx_t)(idx))
	defer C.duckdb_free(unsafe.Pointer(val))
	return C.GoString(val)
}

func (vec *vector) getList(rowIdx C.idx_t) []any {
	listEntry := getPrimitive[duckdb_list_entry_t](vec, rowIdx)
	slice := make([]any, 0, listEntry.length)

	childVector := &vec.childVectors[0]
	maxOffset := listEntry.offset + listEntry.length

	// Fill the slice with all child values.
	for i := listEntry.offset; i < maxOffset; i++ {
		val := childVector.getFn(childVector, i)
		slice = append(slice, val)
	}

	return slice
}

func (vec *vector) getStruct(rowIdx C.idx_t) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := &vec.childVectors[i]
		val := childVector.getFn(childVector, rowIdx)
		m[vec.childNames[i]] = val
	}
	return m
}

func (vec *vector) getMap(rowIdx C.idx_t) Map {
	// A MAP is a LIST of STRUCT values. Each STRUCT holds two children: a key and a value.
	list := vec.getList(rowIdx)

	m := Map{}
	for i := 0; i < len(list); i++ {
		mapItem := list[i].(map[string]any)
		key := mapItem[mapKeysField()]
		val := mapItem[mapValuesField()]
		m[key] = val
	}
	return m
}
