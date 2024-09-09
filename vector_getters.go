package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"math/big"
	"time"
	"unsafe"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx C.idx_t) any

func (vec *vector) getNull(rowIdx C.idx_t) bool {
	mask := unsafe.Pointer(vec.mask)
	if mask == nil {
		return false
	}

	entryIdx := rowIdx / 64
	idxInEntry := rowIdx % 64
	maskPtr := (*[1 << 31]C.uint64_t)(mask)
	isValid := maskPtr[entryIdx] & (C.uint64_t(1) << idxInEntry)
	return uint64(isValid) == 0
}

func getPrimitive[T any](vec *vector, rowIdx C.idx_t) T {
	xs := (*[1 << 31]T)(vec.ptr)
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

func (vec *vector) getInterval(rowIdx C.idx_t) Interval {
	val := getPrimitive[C.duckdb_interval](vec, rowIdx)
	interval := Interval{
		Days:   int32(val.days),
		Months: int32(val.months),
		Micros: int64(val.micros),
	}
	return interval
}

func (vec *vector) getHugeint(rowIdx C.idx_t) *big.Int {
	hugeInt := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
	return hugeIntToNative(hugeInt)
}

func (vec *vector) getCString(rowIdx C.idx_t) any {
	cStr := getPrimitive[duckdb_string_t](vec, rowIdx)

	var blob []byte
	if cStr.length <= stringInlineLength {
		// Inlined data is stored from byte 4 to stringInlineLength + 4.
		blob = C.GoBytes(unsafe.Pointer(&cStr.prefix), C.int(cStr.length))
	} else {
		// Any strings exceeding stringInlineLength are stored as a pointer in `ptr`.
		blob = C.GoBytes(unsafe.Pointer(cStr.ptr), C.int(cStr.length))
	}

	if vec.duckdbType == C.DUCKDB_TYPE_VARCHAR {
		return string(blob)
	}
	return blob
}

func (vec *vector) getDecimal(internalType C.duckdb_type, rowIdx C.idx_t) Decimal {
	var val *big.Int
	switch internalType {
	case C.DUCKDB_TYPE_SMALLINT:
		v := getPrimitive[int16](vec, rowIdx)
		val = big.NewInt(int64(v))
	case C.DUCKDB_TYPE_INTEGER:
		v := getPrimitive[int32](vec, rowIdx)
		val = big.NewInt(int64(v))
	case C.DUCKDB_TYPE_BIGINT:
		v := getPrimitive[int64](vec, rowIdx)
		val = big.NewInt(v)
	case C.DUCKDB_TYPE_HUGEINT:
		v := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
		val = hugeIntToNative(C.duckdb_hugeint{
			lower: v.lower,
			upper: v.upper,
		})
	}

	return Decimal{Width: vec.width, Scale: vec.scale, Value: val}
}

func (vec *vector) getEnum(internalType C.duckdb_type, rowIdx C.idx_t) string {
	var idx uint64
	switch internalType {
	case C.DUCKDB_TYPE_UTINYINT:
		idx = uint64(getPrimitive[uint8](vec, rowIdx))
	case C.DUCKDB_TYPE_USMALLINT:
		idx = uint64(getPrimitive[uint16](vec, rowIdx))
	case C.DUCKDB_TYPE_UINTEGER:
		idx = uint64(getPrimitive[uint32](vec, rowIdx))
	case C.DUCKDB_TYPE_UBIGINT:
		idx = getPrimitive[uint64](vec, rowIdx)
	}

	logicalType := C.duckdb_vector_get_column_type(vec.duckdbVector)
	defer C.duckdb_destroy_logical_type(&logicalType)

	val := C.duckdb_enum_dictionary_value(logicalType, (C.idx_t)(idx))
	defer C.duckdb_free(unsafe.Pointer(val))
	return C.GoString(val)
}

func (vec *vector) getList(rowIdx C.idx_t) []any {
	entry := getPrimitive[duckdb_list_entry_t](vec, rowIdx)
	slice := make([]any, 0, entry.length)
	childVector := &vec.childVectors[0]

	// Fill the slice with all child values.
	for i := C.idx_t(0); i < entry.length; i++ {
		val := childVector.getFn(childVector, i+entry.offset)
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
