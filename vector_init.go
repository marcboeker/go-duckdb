package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"math/big"
	"unsafe"
)

func (vec *vector) init(logicalType C.duckdb_logical_type, colIdx int) error {
	// FIXME: implement support for UHUGEINT, ARRAY, UNION, BIT, TIME_TZ
	duckdbType := C.duckdb_get_type_id(logicalType)

	switch duckdbType {
	case C.DUCKDB_TYPE_INVALID:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	case C.DUCKDB_TYPE_BOOLEAN:
		initPrimitive[bool](vec, C.DUCKDB_TYPE_BOOLEAN)
	case C.DUCKDB_TYPE_TINYINT:
		initPrimitive[int8](vec, C.DUCKDB_TYPE_TINYINT)
	case C.DUCKDB_TYPE_SMALLINT:
		initPrimitive[int16](vec, C.DUCKDB_TYPE_SMALLINT)
	case C.DUCKDB_TYPE_INTEGER:
		initPrimitive[int32](vec, C.DUCKDB_TYPE_INTEGER)
	case C.DUCKDB_TYPE_BIGINT:
		initPrimitive[int64](vec, C.DUCKDB_TYPE_BIGINT)
	case C.DUCKDB_TYPE_UTINYINT:
		initPrimitive[uint8](vec, C.DUCKDB_TYPE_UTINYINT)
	case C.DUCKDB_TYPE_USMALLINT:
		initPrimitive[uint16](vec, C.DUCKDB_TYPE_USMALLINT)
	case C.DUCKDB_TYPE_UINTEGER:
		initPrimitive[uint32](vec, C.DUCKDB_TYPE_UINTEGER)
	case C.DUCKDB_TYPE_UBIGINT:
		initPrimitive[uint64](vec, C.DUCKDB_TYPE_UBIGINT)
	case C.DUCKDB_TYPE_FLOAT:
		initPrimitive[float32](vec, C.DUCKDB_TYPE_FLOAT)
	case C.DUCKDB_TYPE_DOUBLE:
		initPrimitive[float64](vec, C.DUCKDB_TYPE_DOUBLE)
	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS,
		C.DUCKDB_TYPE_TIMESTAMP_NS, C.DUCKDB_TYPE_TIMESTAMP_TZ:
		vec.initTS(duckdbType)
	case C.DUCKDB_TYPE_DATE:
		vec.initDate()
	case C.DUCKDB_TYPE_TIME:
		vec.initTime()
	case C.DUCKDB_TYPE_INTERVAL:
		vec.initInterval()
	case C.DUCKDB_TYPE_HUGEINT:
		vec.initHugeint()
	case C.DUCKDB_TYPE_UHUGEINT:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	case C.DUCKDB_TYPE_VARCHAR, C.DUCKDB_TYPE_BLOB:
		vec.initCString(duckdbType)
	case C.DUCKDB_TYPE_DECIMAL:
		return vec.initDecimal(logicalType, colIdx)
	case C.DUCKDB_TYPE_ENUM:
		return vec.initEnum(logicalType, colIdx)
	case C.DUCKDB_TYPE_LIST:
		return vec.initList(logicalType, colIdx)
	case C.DUCKDB_TYPE_STRUCT:
		return vec.initStruct(logicalType, colIdx)
	case C.DUCKDB_TYPE_MAP:
		return vec.initMap(logicalType, colIdx)
	case C.DUCKDB_TYPE_ARRAY:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	case C.DUCKDB_TYPE_UUID:
		vec.initUUID()
	case C.DUCKDB_TYPE_UNION:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	case C.DUCKDB_TYPE_BIT:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	case C.DUCKDB_TYPE_TIME_TZ:
		return columnError(unsupportedTypeError(duckdbTypeMap[duckdbType]), colIdx)
	default:
		return columnError(unsupportedTypeError("unknown type"), colIdx)
	}
	return nil
}

func initPrimitive[T any](vec *vector, duckdbType C.duckdb_type) {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		setPrimitive[T](vec, rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[T](vec, rowIdx)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initTS(duckdbType C.duckdb_type) {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setTS(duckdbType, rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTS(duckdbType, rowIdx)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initDate() {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setDate(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getDate(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_DATE
}

func (vec *vector) initTime() {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setTime(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTime(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_TIME
}

func (vec *vector) initInterval() {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setInterval(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getInterval(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_INTERVAL
}

func (vec *vector) initHugeint() {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setHugeint(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getHugeint(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_HUGEINT
}

func (vec *vector) initCString(duckdbType C.duckdb_type) {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setCString(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getCString(rowIdx)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initDecimal(logicalType C.duckdb_logical_type, colIdx int) error {
	// TODO: setDecimal

	scale := C.duckdb_decimal_scale(logicalType)
	width := C.duckdb_decimal_width(logicalType)

	internalType := C.duckdb_decimal_internal_type(logicalType)
	switch internalType {
	case C.DUCKDB_TYPE_SMALLINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			v := getPrimitive[int16](vec, rowIdx)
			val := big.NewInt(int64(v))
			return Decimal{Width: uint8(width), Scale: uint8(scale), Value: val}
		}
	case C.DUCKDB_TYPE_INTEGER:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			v := getPrimitive[int32](vec, rowIdx)
			val := big.NewInt(int64(v))
			return Decimal{Width: uint8(width), Scale: uint8(scale), Value: val}
		}
	case C.DUCKDB_TYPE_BIGINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			v := getPrimitive[int64](vec, rowIdx)
			val := big.NewInt(v)
			return Decimal{Width: uint8(width), Scale: uint8(scale), Value: val}
		}
	case C.DUCKDB_TYPE_HUGEINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			v := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
			val := hugeIntToNative(C.duckdb_hugeint{
				lower: v.lower,
				upper: v.upper,
			})
			return Decimal{Width: uint8(width), Scale: uint8(scale), Value: val}
		}
	default:
		return columnError(unsupportedTypeError(duckdbTypeMap[internalType]), colIdx)
	}

	vec.duckdbType = C.DUCKDB_TYPE_DECIMAL
	return nil
}

func (vec *vector) initEnum(logicalType C.duckdb_logical_type, colIdx int) error {
	// TODO: setEnum

	internalType := C.duckdb_enum_internal_type(logicalType)
	switch internalType {
	case C.DUCKDB_TYPE_UTINYINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			idx := uint64(getPrimitive[uint8](vec, rowIdx))
			return vec.getEnum(idx)
		}
	case C.DUCKDB_TYPE_USMALLINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			idx := uint64(getPrimitive[uint16](vec, rowIdx))
			return vec.getEnum(idx)
		}
	case C.DUCKDB_TYPE_UINTEGER:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			idx := uint64(getPrimitive[uint32](vec, rowIdx))
			return vec.getEnum(idx)
		}
	case C.DUCKDB_TYPE_UBIGINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			idx := getPrimitive[uint64](vec, rowIdx)
			return vec.getEnum(idx)
		}
	default:
		return columnError(unsupportedTypeError(duckdbTypeMap[internalType]), colIdx)
	}

	vec.duckdbType = C.DUCKDB_TYPE_ENUM
	return nil
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

	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setList(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getList(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_LIST
	return nil
}

func (vec *vector) initStruct(logicalType C.duckdb_logical_type, colIdx int) error {
	childCount := int(C.duckdb_struct_type_child_count(logicalType))
	var childNames []string
	for i := 0; i < childCount; i++ {
		childName := C.duckdb_struct_type_child_name(logicalType, C.idx_t(i))
		childNames = append(childNames, C.GoString(childName))
		C.free(unsafe.Pointer(childName))
	}

	vec.childVectors = make([]vector, childCount)
	vec.childNames = childNames

	// Recurse into the children.
	for i := 0; i < childCount; i++ {
		childType := C.duckdb_struct_type_child_type(logicalType, C.idx_t(i))
		err := vec.childVectors[i].init(childType, colIdx)
		C.duckdb_destroy_logical_type(&childType)

		if err != nil {
			return err
		}
	}

	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setStruct(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getStruct(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_STRUCT
	return nil
}

func (vec *vector) initMap(logicalType C.duckdb_logical_type, colIdx int) error {
	// A MAP is a LIST of STRUCT values. Each STRUCT holds two children: a key and a value.

	// Get the child vector type.
	childType := C.duckdb_list_type_child_type(logicalType)
	defer C.duckdb_destroy_logical_type(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	// DuckDB supports more MAP key types than Go, which only supports comparable types.
	// We ensure that the key type itself is comparable.
	keyType := C.duckdb_map_type_key_type(logicalType)
	defer C.duckdb_destroy_logical_type(&keyType)

	duckdbKeyType := C.duckdb_get_type_id(keyType)
	switch duckdbKeyType {
	case C.DUCKDB_TYPE_LIST, C.DUCKDB_TYPE_STRUCT, C.DUCKDB_TYPE_MAP, C.DUCKDB_TYPE_ARRAY:
		return columnError(errUnsupportedMapKeyType, colIdx)
	}

	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setMap(rowIdx, val)
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getMap(rowIdx)
	}
	vec.duckdbType = C.DUCKDB_TYPE_MAP
	return nil
}

func (vec *vector) initUUID() {
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		vec.size++
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		setPrimitive[C.duckdb_hugeint](vec, rowIdx, uuidToHugeInt(val.(UUID)))
	}
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		hugeInt := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
		return hugeIntToUUID(hugeInt)
	}
	vec.duckdbType = C.DUCKDB_TYPE_UUID
}
