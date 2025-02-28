package duckdb

import (
	"encoding/json"
	"math/big"
	"reflect"
	"strconv"

	"github.com/marcboeker/go-duckdb/mapping"
)

// secondsPerDay to calculate the days since 1970-01-01.
const secondsPerDay = 24 * 60 * 60

// fnSetVectorValue is the setter callback function for any (nested) vector.
type fnSetVectorValue func(vec *vector, rowIdx mapping.IdxT, val any) error

func (vec *vector) setNull(rowIdx mapping.IdxT) {
	mapping.ValiditySetRowInvalid(vec.maskPtr, rowIdx)
	if vec.Type == TYPE_STRUCT {
		for i := 0; i < len(vec.childVectors); i++ {
			vec.childVectors[i].setNull(rowIdx)
		}
	}
}

func setPrimitive[T any](vec *vector, rowIdx mapping.IdxT, v T) {
	xs := (*[1 << 31]T)(vec.dataPtr)
	xs[rowIdx] = v
}

func setNumeric[S any, T numericType](vec *vector, rowIdx mapping.IdxT, val S) error {
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
	case Decimal:
		if v.Value == nil {
			return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
		}
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

func setBool[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var b bool
	switch v := any(val).(type) {
	case bool:
		b = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(b).String())
	}
	setPrimitive(vec, rowIdx, b)
	return nil
}

func setTS[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	ts, err := getMappedTimestamp(vec.Type, val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, ts)
	return nil
}

func setDate[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	date, err := getMappedDate(val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, date)
	return nil
}

func setTime[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	ticks, err := getTimeTicks(val)
	if err != nil {
		return err
	}

	switch vec.Type {
	case TYPE_TIME:
		var ti mapping.Time
		mapping.TimeSetMicros(&ti, ticks)
		setPrimitive(vec, rowIdx, ti)
	case TYPE_TIME_TZ:
		// The UTC offset is 0.
		ti := mapping.CreateTimeTZ(ticks, 0)
		setPrimitive(vec, rowIdx, ti)
	}
	return nil
}

func setInterval[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var i Interval
	switch v := any(val).(type) {
	case Interval:
		i = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(i).String())
	}
	var interval mapping.Interval
	mapping.IntervalSetMonths(&interval, i.Months)
	mapping.IntervalSetDays(&interval, i.Days)
	mapping.IntervalSetMicros(&interval, i.Micros)
	setPrimitive(vec, rowIdx, interval)
	return nil
}

func setHugeint[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var err error
	var fv mapping.HugeInt
	switch v := any(val).(type) {
	case uint8:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case int8:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case uint16:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case int16:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case uint32:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case int32:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case uint64:
		mapping.HugeIntSetLower(&fv, v)
		mapping.HugeIntSetUpper(&fv, 0)
	case int64:
		fv, err = hugeIntFromNative(big.NewInt(v))
		if err != nil {
			return err
		}
	case uint:
		mapping.HugeIntSetLower(&fv, uint64(v))
		mapping.HugeIntSetUpper(&fv, 0)
	case int:
		fv, err = hugeIntFromNative(big.NewInt(int64(v)))
		if err != nil {
			return err
		}
	case float32:
		fv, err = hugeIntFromNative(big.NewInt(int64(v)))
		if err != nil {
			return err
		}
	case float64:
		fv, err = hugeIntFromNative(big.NewInt(int64(v)))
		if err != nil {
			return err
		}
	case *big.Int:
		if v == nil {
			return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
		}
		if fv, err = hugeIntFromNative(v); err != nil {
			return err
		}
	case Decimal:
		if v.Value == nil {
			return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
		}
		if fv, err = hugeIntFromNative(v.Value); err != nil {
			return err
		}
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(fv).String())
	}
	setPrimitive(vec, rowIdx, fv)
	return nil
}

func setBytes[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	switch v := any(val).(type) {
	case string:
		mapping.VectorAssignStringElement(vec.vec, rowIdx, v)
	case []byte:
		mapping.VectorAssignStringElementLen(vec.vec, rowIdx, v, mapping.IdxT(len(v)))
	default:
		return castError(reflect.TypeOf(val).String(), reflect.String.String())
	}
	return nil
}

func setJSON[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return setBytes(vec, rowIdx, bytes)
}

func setDecimal[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	switch vec.internalType {
	case TYPE_SMALLINT:
		return setNumeric[S, int16](vec, rowIdx, val)
	case TYPE_INTEGER:
		return setNumeric[S, int32](vec, rowIdx, val)
	case TYPE_BIGINT:
		return setNumeric[S, int64](vec, rowIdx, val)
	case TYPE_HUGEINT:
		return setHugeint(vec, rowIdx, val)
	}
	return nil
}

func setEnum[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var str string
	switch v := any(val).(type) {
	case string:
		str = v
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(str).String())
	}

	if v, ok := vec.dict[str]; ok {
		switch vec.internalType {
		case TYPE_UTINYINT:
			return setNumeric[uint32, int8](vec, rowIdx, v)
		case TYPE_SMALLINT:
			return setNumeric[uint32, int16](vec, rowIdx, v)
		case TYPE_INTEGER:
			return setNumeric[uint32, int32](vec, rowIdx, v)
		case TYPE_BIGINT:
			return setNumeric[uint32, int64](vec, rowIdx, v)
		}
	} else {
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(str).String())
	}
	return nil
}

func setList[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	list, err := extractSlice(vec, val)
	if err != nil {
		return err
	}

	// Set the offset and length of the list vector using the current size of the child vector.
	childVectorSize := mapping.ListVectorGetSize(vec.vec)
	var listEntry mapping.ListEntry
	mapping.ListEntrySetOffset(&listEntry, uint64(childVectorSize))
	mapping.ListEntrySetLength(&listEntry, uint64(len(list)))
	setPrimitive(vec, rowIdx, listEntry)

	newLength := mapping.IdxT(len(list)) + childVectorSize
	vec.resizeListVector(newLength)
	return setSliceChildren(vec, list, childVectorSize)
}

func setStruct[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var m map[string]any
	switch v := any(val).(type) {
	case map[string]any:
		m = v
	default:
		// FIXME: Add support for all map types.

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
			if name, ok := structType.Field(i).Tag.Lookup("db"); ok {
				fieldName = name
			}
			if _, ok := m[fieldName]; ok {
				return duplicateNameError(fieldName)
			}
			m[fieldName] = rv.Field(i).Interface()
		}
	}

	for i := 0; i < len(vec.childVectors); i++ {
		child := &vec.childVectors[i]
		name := vec.structEntries[i].Name()
		v, ok := m[name]
		if !ok {
			return structFieldError("missing field", name)
		}
		err := child.setFn(child, rowIdx, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func setMap[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
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

	return setList(vec, rowIdx, list)
}

func setArray[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	array, err := extractSlice(vec, val)
	if err != nil {
		return err
	}
	if len(array) != int(vec.arrayLength) {
		return invalidInputError(strconv.Itoa(len(array)), strconv.Itoa(int(vec.arrayLength)))
	}
	return setSliceChildren(vec, array, rowIdx*vec.arrayLength)
}

func extractSlice[S any](vec *vector, val S) ([]any, error) {
	var s []any
	switch v := any(val).(type) {
	case []any:
		s = v
	default:
		kind := reflect.TypeOf(val).Kind()
		if kind != reflect.Array && kind != reflect.Slice {
			return nil, castError(reflect.TypeOf(val).String(), reflect.TypeOf(s).String())
		}
		// Insert the values into the child vector.
		rv := reflect.ValueOf(val)
		s = make([]any, rv.Len())

		for i := 0; i < rv.Len(); i++ {
			idx := rv.Index(i)
			if vec.canNil(idx) && idx.IsNil() {
				s[i] = nil
				continue
			}

			s[i] = idx.Interface()
		}
	}
	return s, nil
}

func setSliceChildren(vec *vector, s []any, offset mapping.IdxT) error {
	childVector := &vec.childVectors[0]
	for i, entry := range s {
		rowIdx := mapping.IdxT(i) + offset
		err := childVector.setFn(childVector, rowIdx, entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func setUUID[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	var uuid UUID
	switch v := any(val).(type) {
	case UUID:
		uuid = v
	case *UUID:
		uuid = *v
	case []uint8:
		if len(v) != uuidLength {
			return castError(reflect.TypeOf(val).String(), reflect.TypeOf(uuid).String())
		}
		for i := 0; i < uuidLength; i++ {
			uuid[i] = v[i]
		}
	default:
		return castError(reflect.TypeOf(val).String(), reflect.TypeOf(uuid).String())
	}
	hi := uuidToHugeInt(uuid)
	setPrimitive(vec, rowIdx, hi)
	return nil
}

func setVectorVal[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	name, inMap := unsupportedTypeToStringMap[vec.Type]
	if inMap {
		return unsupportedTypeError(name)
	}

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
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		return setTS[S](vec, rowIdx, val)
	case TYPE_DATE:
		return setDate[S](vec, rowIdx, val)
	case TYPE_TIME, TYPE_TIME_TZ:
		return setTime[S](vec, rowIdx, val)
	case TYPE_INTERVAL:
		return setInterval[S](vec, rowIdx, val)
	case TYPE_HUGEINT:
		return setHugeint[S](vec, rowIdx, val)
	case TYPE_VARCHAR:
		return setBytes[S](vec, rowIdx, val)
	case TYPE_BLOB:
		return setBytes[S](vec, rowIdx, val)
	case TYPE_DECIMAL:
		return setDecimal[S](vec, rowIdx, val)
	case TYPE_ENUM:
		return setEnum[S](vec, rowIdx, val)
	case TYPE_LIST:
		return setList[S](vec, rowIdx, val)
	case TYPE_STRUCT:
		return setStruct[S](vec, rowIdx, val)
	case TYPE_MAP, TYPE_ARRAY:
		// FIXME: Is this already supported? And tested?
		return unsupportedTypeError(unsupportedTypeToStringMap[vec.Type])
	case TYPE_UUID:
		return setUUID[S](vec, rowIdx, val)
	default:
		return unsupportedTypeError(unknownTypeErrMsg)
	}
}
