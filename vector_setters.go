package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"log"
	"math/big"
	"time"
	"unsafe"
)

// fnSetVectorValue is the setter callback function for any (nested) vector.
type fnSetVectorValue func(vec *vector, rowIdx C.idx_t, val any)

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

func setPrimitive[T any](vec *vector, rowIdx C.idx_t, val any) {
	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = val.(T)
}

func (vec *vector) setTS(duckdbType C.duckdb_type, rowIdx C.idx_t, val any) {
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

	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(ticks)
	setPrimitive[C.duckdb_timestamp](vec, rowIdx, ts)
}

func (vec *vector) setDate(rowIdx C.idx_t, val any) {
	// Days since 1970-01-01.
	v := val.(time.Time)
	days := int32(v.UTC().Unix() / secondsPerDay)

	var date C.duckdb_date
	date.days = C.int32_t(days)
	setPrimitive[C.duckdb_date](vec, rowIdx, date)
}

func (vec *vector) setTime(rowIdx C.idx_t, val any) {
	v := val.(time.Time)
	ticks := v.UTC().UnixMicro()

	var t C.duckdb_time
	t.micros = C.int64_t(ticks)
	setPrimitive[C.duckdb_time](vec, rowIdx, t)
}

func (vec *vector) setInterval(rowIdx C.idx_t, val any) {
	v := val.(Interval)
	var interval C.duckdb_interval
	interval.days = C.int32_t(v.Days)
	interval.months = C.int32_t(v.Months)
	interval.micros = C.int64_t(v.Micros)
	setPrimitive[C.duckdb_interval](vec, rowIdx, interval)
}

func (vec *vector) setHugeint(rowIdx C.idx_t, val any) {
	v := val.(*big.Int)
	hugeInt, err := hugeIntFromNative(v)
	if err != nil {
		// TODO: fatal logging...
		log.Fatal(getError(errDriver, err))
	}
	setPrimitive[C.duckdb_hugeint](vec, rowIdx, hugeInt)
}

func (vec *vector) setCString(rowIdx C.idx_t, val any) {
	var str string
	if vec.duckdbType == C.DUCKDB_TYPE_VARCHAR {
		str = val.(string)
	} else if vec.duckdbType == C.DUCKDB_TYPE_BLOB {
		str = string(val.([]byte)[:])
	}

	// This setter also writes BLOBs.
	cStr := C.CString(str)
	C.duckdb_vector_assign_string_element_len(vec.duckdbVector, rowIdx, cStr, C.idx_t(len(str)))
	C.free(unsafe.Pointer(cStr))
}

func (vec *vector) setList(rowIdx C.idx_t, val any) {
	list := val.([]any)
	childVectorSize := C.duckdb_list_vector_get_size(vec.duckdbVector)

	// Set the offset and length of the list vector using the current size of the child vector.
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(len(list)),
	}
	setPrimitive[C.duckdb_list_entry](vec, rowIdx, listEntry)

	newLength := C.idx_t(len(list)) + childVectorSize
	C.duckdb_list_vector_set_size(vec.duckdbVector, newLength)
	C.duckdb_list_vector_reserve(vec.duckdbVector, newLength)

	// Insert the values into the child vector.
	childVector := &vec.childVectors[0]
	for i, entry := range list {
		offset := C.idx_t(i) + childVectorSize
		childVector.setFn(childVector, offset, entry)
	}
}

func (vec *vector) setStruct(rowIdx C.idx_t, val any) {
	m := val.(map[string]any)
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := &vec.childVectors[i]
		childName := vec.childNames[i]
		childVector.setFn(childVector, rowIdx, m[childName])
	}
}

func (vec *vector) setMap(rowIdx C.idx_t, val any) {
	m := val.(Map)

	// Create a LIST of STRUCT values.
	entries := make([]map[string]any, len(m))
	for key, value := range m {
		entry := map[string]any{mapKeysField(): key, mapValuesField(): value}
		entries = append(entries, entry)
	}

	vec.setList(rowIdx, entries)
}
