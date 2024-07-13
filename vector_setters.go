package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"math/big"
	"reflect"
	"time"
	"unsafe"
)

// secondsPerDay to calculate the days since 1970-01-01.
const secondsPerDay = 24 * 60 * 60

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

func setPrimitive[T any](vec *vector, rowIdx C.idx_t, v T) {
	ptr := C.duckdb_vector_get_data(vec.duckdbVector)
	xs := (*[1 << 31]T)(ptr)
	xs[rowIdx] = v
}

func (vec *vector) setDate(rowIdx C.idx_t, val any) {
	// Days since 1970-01-01.
	v := val.(time.Time)
	days := int32(v.UTC().Unix() / secondsPerDay)

	var date C.duckdb_date
	date.days = C.int32_t(days)
	setPrimitive(vec, rowIdx, date)
}

func (vec *vector) setTime(rowIdx C.idx_t, val any) {
	v := val.(time.Time)
	ticks := v.UTC().UnixMicro()

	var t C.duckdb_time
	t.micros = C.int64_t(ticks)
	setPrimitive(vec, rowIdx, t)
}

func (vec *vector) setInterval(rowIdx C.idx_t, val any) {
	v := val.(Interval)
	var interval C.duckdb_interval
	interval.days = C.int32_t(v.Days)
	interval.months = C.int32_t(v.Months)
	interval.micros = C.int64_t(v.Micros)
	setPrimitive(vec, rowIdx, interval)
}

func (vec *vector) setHugeint(rowIdx C.idx_t, val any) {
	v := val.(*big.Int)
	hugeInt, _ := hugeIntFromNative(v)
	setPrimitive(vec, rowIdx, hugeInt)
}

func (vec *vector) setDecimal(internalType C.duckdb_type, rowIdx C.idx_t, val any) {
	v := val.(Decimal)

	switch internalType {
	case C.DUCKDB_TYPE_SMALLINT:
		setPrimitive(vec, rowIdx, int16(v.Value.Int64()))
	case C.DUCKDB_TYPE_INTEGER:
		setPrimitive(vec, rowIdx, int32(v.Value.Int64()))
	case C.DUCKDB_TYPE_BIGINT:
		setPrimitive(vec, rowIdx, v.Value.Int64())
	case C.DUCKDB_TYPE_HUGEINT:
		value, _ := hugeIntFromNative(v.Value)
		setPrimitive(vec, rowIdx, value)
	}
}

func (vec *vector) setEnum(internalType C.duckdb_type, rowIdx C.idx_t, val any) {
	v := vec.dict[val.(string)]

	switch internalType {
	case C.DUCKDB_TYPE_UTINYINT:
		setPrimitive(vec, rowIdx, uint8(v))
	case C.DUCKDB_TYPE_USMALLINT:
		setPrimitive(vec, rowIdx, uint16(v))
	case C.DUCKDB_TYPE_UINTEGER:
		setPrimitive(vec, rowIdx, v)
	case C.DUCKDB_TYPE_UBIGINT:
		setPrimitive(vec, rowIdx, uint64(v))
	}
}

func (vec *vector) setMap(rowIdx C.idx_t, val any) {
	m := val.(Map)

	// Create a LIST of STRUCT values.
	i := 0
	list := make([]any, len(m))
	for key, value := range m {
		list[i] = map[string]any{mapKeysField(): key, mapValuesField(): value}
		i++
	}

	setList(vec, rowIdx, list)
}

func setNumeric[S any, T numericType](vec *vector, rowIdx C.idx_t, val S) error {
	var fv T
	switch v := any(val).(type) {
	case uint8:
		fv = T(v)
	case int8:
		fv = T(v)
	case uint16:
		fv = T(v)
	case int16:
		fv = T(v)
	case uint32:
		fv = T(v)
	case int32:
		fv = T(v)
	case uint64:
		fv = T(v)
	case int64:
		fv = T(v)
	case uint:
		fv = T(v)
	case int:
		fv = T(v)
	case float32:
		fv = T(v)
	case float64:
		fv = T(v)
	case bool:
		if v {
			fv = 1
		} else {
			fv = 0
		}
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
	}
	setPrimitive(vec, rowIdx, fv)
	return nil
}

func setBool[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var fv bool
	switch v := any(val).(type) {
	case uint8:
		fv = v == 0
	case int8:
		fv = v == 0
	case uint16:
		fv = v == 0
	case int16:
		fv = v == 0
	case uint32:
		fv = v == 0
	case int32:
		fv = v == 0
	case uint64:
		fv = v == 0
	case int64:
		fv = v == 0
	case uint:
		fv = v == 0
	case int:
		fv = v == 0
	case float32:
		fv = v == 0
	case float64:
		fv = v == 0
	case bool:
		fv = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
	}
	setPrimitive(vec, rowIdx, fv)
	return nil
}

func setString[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var cStr *C.char
	var length int
	switch v := any(val).(type) {
	case string:
		cStr = C.CString(v)
		defer C.free(unsafe.Pointer(cStr))
		length = len(v)
	case []byte:
		cStr = (*C.char)(C.CBytes(v))
		defer C.free(unsafe.Pointer(cStr))
		length = len(v)
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(cStr).String())
	}

	C.duckdb_vector_assign_string_element_len(vec.duckdbVector, rowIdx, cStr, C.idx_t(length))
	return nil
}

func setTS[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var t time.Time
	switch v := any(val).(type) {
	case time.Time:
		t = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(t).String())
	}
	var ticks int64
	switch vec.duckdbType {
	case C.DUCKDB_TYPE_TIMESTAMP:
		ticks = t.UTC().UnixMicro()
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		ticks = t.UTC().Unix()
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		ticks = t.UTC().UnixMilli()
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		ticks = t.UTC().UnixNano()
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		ticks = t.UTC().UnixMicro()
	}
	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(ticks)
	setPrimitive(vec, rowIdx, ts)
	return nil
}

func setUUID[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var uuid UUID
	switch v := any(val).(type) {
	case UUID:
		uuid = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(uuid).String())
	}
	hi := uuidToHugeInt(uuid)
	setPrimitive(vec, rowIdx, hi)
	return nil
}

func setList[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var list []any
	switch v := any(val).(type) {
	case []any:
		list = v
	default:
		// Insert the values into the child vector.
		rv := reflect.ValueOf(val)
		list = make([]any, rv.Len())
		childVector := vec.childVectors[0]

		for i := 0; i < rv.Len(); i++ {
			idx := rv.Index(i)
			if vec.canNil(idx) && idx.IsNil() {
				list[i] = nil
				continue
			}

			var err error
			list[i], err = childVector.tryCast(idx.Interface())
			if err != nil {
				return err
			}
		}
	}
	childVectorSize := C.duckdb_list_vector_get_size(vec.duckdbVector)

	// Set the offset and length of the list vector using the current size of the child vector.
	listEntry := C.duckdb_list_entry{
		offset: C.idx_t(childVectorSize),
		length: C.idx_t(len(list)),
	}
	setPrimitive(vec, rowIdx, listEntry)

	newLength := C.idx_t(len(list)) + childVectorSize
	C.duckdb_list_vector_set_size(vec.duckdbVector, newLength)
	C.duckdb_list_vector_reserve(vec.duckdbVector, newLength)

	// Insert the values into the child vector.
	childVector := vec.childVectors[0]
	for i, e := range list {
		offset := C.idx_t(i) + childVectorSize
		childVector.setFn(&childVector, offset, e)
	}
	return nil
}

func setStruct[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var m map[string]any
	switch v := any(val).(type) {
	case map[string]any:
		m = v
	default:
		//TODO type.Kind() == reflect.Map??
		// Catch mismatching types.
		goType := reflect.TypeOf(val)
		if reflect.TypeOf(val).Kind() != reflect.Struct {
			return castError(goType.String(), reflect.Struct.String())
		}

		m = make(map[string]any)
		rv := reflect.ValueOf(val)
		structType := rv.Type()

		for i := 0; i < structType.NumField(); i++ {
			if !rv.Field(i).CanInterface() {
				continue
			}
			fieldName := structType.Field(i).Name
			m[fieldName] = rv.Field(i).Interface()
		}
	}

	for i := 0; i < len(vec.childVectors); i++ {
		childVector := vec.childVectors[i]
		childName := vec.childNames[i]
		childVector.setFn(&childVector, rowIdx, m[childName])
	}
	return nil
}

// TODO: Support map type, Time, Enum, Decimal
func setVectorVal[S any](vec *vector, rowIdx C.idx_t, val S) error {
	switch vec.duckdbType {
	case C.DUCKDB_TYPE_UTINYINT:
		return setNumeric[S, uint8](vec, rowIdx, val)
	case C.DUCKDB_TYPE_TINYINT:
		return setNumeric[S, int8](vec, rowIdx, val)
	case C.DUCKDB_TYPE_USMALLINT:
		return setNumeric[S, uint16](vec, rowIdx, val)
	case C.DUCKDB_TYPE_SMALLINT:
		return setNumeric[S, int16](vec, rowIdx, val)
	case C.DUCKDB_TYPE_UINTEGER:
		return setNumeric[S, uint32](vec, rowIdx, val)
	case C.DUCKDB_TYPE_INTEGER:
		return setNumeric[S, int32](vec, rowIdx, val)
	case C.DUCKDB_TYPE_UBIGINT:
		return setNumeric[S, uint64](vec, rowIdx, val)
	case C.DUCKDB_TYPE_BIGINT:
		return setNumeric[S, int64](vec, rowIdx, val)
	case C.DUCKDB_TYPE_FLOAT:
		return setNumeric[S, float32](vec, rowIdx, val)
	case C.DUCKDB_TYPE_DOUBLE:
		return setNumeric[S, float64](vec, rowIdx, val)
	case C.DUCKDB_TYPE_BOOLEAN:
		return setBool[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_VARCHAR:
		return setString[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_BLOB:
		return setString[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS,
		C.DUCKDB_TYPE_TIMESTAMP_NS, C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return setTS[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_UUID:
		return setUUID[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_LIST:
		return setList[S](vec, rowIdx, val)
	case C.DUCKDB_TYPE_STRUCT:
		return setStruct[S](vec, rowIdx, val)
	default:
		return unsupportedTypeError("Could not convert type " + reflect.TypeOf(val).Name() + " to a duckdb type")
	}
}
