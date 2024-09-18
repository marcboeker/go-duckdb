package duckdb

/*
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
	C.duckdb_validity_set_row_invalid(vec.mask, rowIdx)

	if vec.Type == TYPE_STRUCT {
		for i := 0; i < len(vec.childVectors); i++ {
			vec.childVectors[i].setNull(rowIdx)
		}
	}
}

func setPrimitive[T any](vec *vector, rowIdx C.idx_t, v T) {
	xs := (*[1 << 31]T)(vec.ptr)
	xs[rowIdx] = v
}

// TODO: support Decimal here and bool and hugeint
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
	case *big.Int:
		if v.IsUint64() {
			fv = T(v.Uint64())
		} else {
			fv = T(v.Int64())
		}
	case Decimal:
		if v.Value.IsUint64() {
			fv = T(v.Value.Uint64())
		} else {
			fv = T(v.Value.Int64())
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
	case *big.Int:
		fv = v.Uint64() == 0
	case Decimal:
		fv = v.Value.Uint64() == 0
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
	}
	setPrimitive(vec, rowIdx, fv)
	return nil
}
func setTS[S any](vec *vector, t Type, rowIdx C.idx_t, val S) error {
	var ti time.Time
	switch v := any(val).(type) {
	case time.Time:
		ti = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(t).String())
	}
	var ticks int64
	switch t {
	case TYPE_TIMESTAMP:
		ticks = ti.UTC().UnixMicro()
	case TYPE_TIMESTAMP_S:
		ticks = ti.UTC().Unix()
	case TYPE_TIMESTAMP_MS:
		ticks = ti.UTC().UnixMilli()
	case TYPE_TIMESTAMP_NS:
		ticks = ti.UTC().UnixNano()
	case TYPE_TIMESTAMP_TZ:
		ticks = ti.UTC().UnixMicro()
	}
	var ts C.duckdb_timestamp
	ts.micros = C.int64_t(ticks)
	setPrimitive(vec, rowIdx, ts)
	return nil
}
func setDate[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var date time.Time
	switch v := any(val).(type) {
	case time.Time:
		date = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(date).String())
	}
	days := int32(date.UTC().Unix() / secondsPerDay)
	var date2 C.duckdb_date
	date2.days = C.int32_t(days)
	setPrimitive(vec, rowIdx, date2)
	return nil
}

func setTime[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var date time.Time
	switch v := any(val).(type) {
	case time.Time:
		date = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(date).String())
	}
	ticks := date.UTC().UnixMicro()

	var t C.duckdb_time
	t.micros = C.int64_t(ticks)
	setPrimitive(vec, rowIdx, t)
	return nil
}

func setInterval[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var interval Interval
	switch v := any(val).(type) {
	case Interval:
		interval = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(interval).String())
	}
	var interval2 C.duckdb_interval
	interval2.days = C.int32_t(interval.Days)
	interval2.months = C.int32_t(interval.Months)
	interval2.micros = C.int64_t(interval.Micros)
	setPrimitive(vec, rowIdx, interval2)
	return nil
}

func setHugeint[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var fv C.duckdb_hugeint
	switch v := any(val).(type) {
	case uint8:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case int8:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case uint16:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case int16:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case uint32:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case int32:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case uint64:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case int64:
		fv, _ = hugeIntFromNative(big.NewInt(v))
	case uint:
		fv = C.duckdb_hugeint{lower: C.uint64_t(v)}
	case int:
		fv, _ = hugeIntFromNative(big.NewInt(int64(v)))
	case float32:
		fv, _ = hugeIntFromNative(big.NewInt(int64(v)))
	case float64:
		fv, _ = hugeIntFromNative(big.NewInt(int64(v)))
	case bool:
		if v {
			fv = C.duckdb_hugeint{lower: C.uint64_t(1)}
		} else {
			fv = C.duckdb_hugeint{lower: C.uint64_t(0)}
		}
	case *big.Int:
		fv, _ = hugeIntFromNative(v)
	case Decimal:
		fv, _ = hugeIntFromNative(v.Value)
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
		defer C.duckdb_free(unsafe.Pointer(cStr))
		length = len(v)
	case []byte:
		cStr = (*C.char)(C.CBytes(v))
		defer C.duckdb_free(unsafe.Pointer(cStr))
		length = len(v)
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(cStr).String())
	}

	C.duckdb_vector_assign_string_element_len(vec.duckdbVector, rowIdx, cStr, C.idx_t(length))
	return nil
}

func setDecimal[S any](vec *vector, rowIdx C.idx_t, val S) error {
	switch vec.internalType {
	case TYPE_SMALLINT:
		setNumeric[S, int16](vec, rowIdx, val)
	case TYPE_INTEGER:
		setNumeric[S, int32](vec, rowIdx, val)
	case TYPE_BIGINT:
		setNumeric[S, int64](vec, rowIdx, val)
	case TYPE_HUGEINT:
		setHugeint(vec, rowIdx, val)
	}
	return nil
}

func setEnum[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var str string
	switch v := any(val).(type) {
	case string:
		str = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(str).String())
	}

	v := vec.dict[str]

	switch vec.internalType {
	case TYPE_UTINYINT:
		setNumeric[uint32, int8](vec, rowIdx, v)
	case TYPE_SMALLINT:
		setNumeric[uint32, int16](vec, rowIdx, v)
	case TYPE_INTEGER:
		setNumeric[uint32, int32](vec, rowIdx, v)
	case TYPE_BIGINT:
		setNumeric[uint32, int64](vec, rowIdx, v)
	}
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
	childVector := &vec.childVectors[0]
	for i, entry := range list {
		offset := C.idx_t(i) + childVectorSize
		childVector.setFn(childVector, offset, entry)
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
		child := &vec.childVectors[i]
		name := vec.structEntries[i].Name()
		child.setFn(child, rowIdx, m[name])
	}
	return nil
}

func setMap[S any](vec *vector, rowIdx C.idx_t, val S) error {
	var m Map
	switch v := any(val).(type) {
	case Map:
		m = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(m).String())
	}

	// Create a LIST of STRUCT values.
	i := 0
	list := make([]any, len(m))
	for key, value := range m {
		list[i] = map[string]any{mapKeysField(): key, mapValuesField(): value}
		i++
	}

	setList(vec, rowIdx, list)
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

// TODO: Support ARRAY UNION BIT ANY VARINT,
func setVectorVal[S any](vec *vector, rowIdx C.idx_t, val S) error {
	switch vec.Type {
	case TYPE_BOOLEAN:
		return setBool[S](vec, rowIdx, val)
	case TYPE_TINYINT:
		return setNumeric[S, int8](vec, rowIdx, val)
	case TYPE_SMALLINT:
		return setNumeric[S, int16](vec, rowIdx, val)
	case TYPE_INTEGER:
		return setNumeric[S, int32](vec, rowIdx, val)
	case TYPE_BIGINT:
		return setNumeric[S, int64](vec, rowIdx, val)
	case TYPE_UTINYINT:
		return setNumeric[S, uint8](vec, rowIdx, val)
	case TYPE_USMALLINT:
		return setNumeric[S, uint16](vec, rowIdx, val)
	case TYPE_UINTEGER:
		return setNumeric[S, uint32](vec, rowIdx, val)
	case TYPE_UBIGINT:
		return setNumeric[S, uint64](vec, rowIdx, val)
	case TYPE_FLOAT:
		return setNumeric[S, float32](vec, rowIdx, val)
	case TYPE_DOUBLE:
		return setNumeric[S, float64](vec, rowIdx, val)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		return setTS[S](vec, vec.Type, rowIdx, val)
	case TYPE_DATE:
		return setDate[S](vec, rowIdx, val)
	case TYPE_TIME:
		return setTime[S](vec, rowIdx, val)
	case TYPE_INTERVAL:
		return setInterval[S](vec, rowIdx, val)
	case TYPE_HUGEINT:
		return setHugeint[S](vec, rowIdx, val)
		// UHUGEINT is not supported
	case TYPE_VARCHAR:
		return setString[S](vec, rowIdx, val)
	case TYPE_BLOB:
		return setString[S](vec, rowIdx, val)
	case TYPE_DECIMAL:
		return setDecimal[S](vec, rowIdx, val)
	case TYPE_ENUM:
		return setEnum[S](vec, rowIdx, val)
	case TYPE_LIST:
		return setList[S](vec, rowIdx, val)
	case TYPE_STRUCT:
		return setStruct[S](vec, rowIdx, val)
	case TYPE_UUID:
		return setUUID[S](vec, rowIdx, val)
	default:
		return unsupportedTypeError("Could not convert type " + reflect.TypeOf(val).Name() + " to a duckdb type")
	}
}
