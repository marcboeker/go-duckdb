package duckdb

import (
	"encoding/json"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/marcboeker/go-duckdb/mapping"
)

// secondsPerDay to calculate the days since 1970-01-01.
const secondsPerDay = 24 * 60 * 60

// fnSetVectorValue is the setter callback function for any (nested) vector.
type fnSetVectorValue func(vec *vector, rowIdx mapping.IdxT, val any) error

func (vec *vector) setNull(rowIdx mapping.IdxT) {
	mapping.ValiditySetRowInvalid(vec.maskPtr, rowIdx)
	if vec.Type == TYPE_STRUCT || vec.Type == TYPE_UNION {
		for i := range vec.childVectors {
			vec.childVectors[i].setNull(rowIdx)
		}
	}
}

func setPrimitive[T any](vec *vector, rowIdx mapping.IdxT, v T) {
	elementSize := unsafe.Sizeof(v)
	offset := uintptr(rowIdx) * elementSize
	ptr := unsafe.Add(vec.dataPtr, offset)
	*(*T)(ptr) = v
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

func setTS(vec *vector, rowIdx mapping.IdxT, val any) error {
	switch vec.Type {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts, err := inferTimestamp(vec.Type, val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ts)
	case TYPE_TIMESTAMP_S:
		ts, err := inferTimestampS(val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ts)
	case TYPE_TIMESTAMP_MS:
		ts, err := inferTimestampMS(val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ts)
	case TYPE_TIMESTAMP_NS:
		ts, err := inferTimestampNS(val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ts)
	default:
		return castError(reflect.TypeOf(val).String(), "")
	}
	return nil
}

func setDate[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	date, err := inferDate(val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, date)
	return nil
}

func setTime[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	switch vec.Type {
	case TYPE_TIME:
		ti, err := inferTime(val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ti)
	case TYPE_TIME_TZ:
		// The UTC offset is 0.
		ti, err := inferTimeTZ(val)
		if err != nil {
			return err
		}
		setPrimitive(vec, rowIdx, ti)
	}
	return nil
}

func setInterval[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	i, err := inferInterval(val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, i)
	return nil
}

func setHugeint[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	hi, err := inferHugeInt(val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, hi)
	return nil
}

func setBytes[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	switch v := any(val).(type) {
	case string:
		mapping.VectorAssignStringElement(vec.vec, rowIdx, v)
	case []byte:
		mapping.VectorAssignStringElementLen(vec.vec, rowIdx, v)
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

	if v, ok := vec.namesDict[str]; ok {
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
	list, err := extractSlice(val)
	if err != nil {
		return err
	}

	// Set the offset and length of the list vector using the current size of the child vector.
	childVectorSize := mapping.ListVectorGetSize(vec.vec)
	listEntry := mapping.NewListEntry(uint64(childVectorSize), uint64(len(list)))
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

		for i := range structType.NumField() {
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

	for i := range vec.childVectors {
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
	array, err := extractSlice(val)
	if err != nil {
		return err
	}
	if len(array) != int(vec.arrayLength) {
		return invalidInputError(strconv.Itoa(len(array)), strconv.Itoa(int(vec.arrayLength)))
	}
	return setSliceChildren(vec, array, rowIdx*vec.arrayLength)
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
	id, err := inferUUID(val)
	if err != nil {
		return err
	}
	setPrimitive(vec, rowIdx, id)
	return nil
}

func setUnion[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	switch v := any(val).(type) {
	case Union:
		// Get the tag index.
		tag, found := vec.namesDict[v.Tag]
		if !found {
			return invalidInputError("tag", v.Tag)
		}

		// Set the tag in the tag vector.
		setPrimitive(&vec.childVectors[0], rowIdx, uint8(tag))

		// Set the value in the tagged member vector, and set all other members to NULL.
		for i := 1; i < len(vec.childVectors); i++ {
			child := &vec.childVectors[i]
			if uint32(i) == tag+1 {
				if err := child.setFn(child, rowIdx, v.Value); err != nil {
					return err
				}
				continue
			}
			if err := child.setFn(child, rowIdx, nil); err != nil {
				return err
			}
		}

		return nil

	default:
		// Try to match the type with a UNION member.
		anyVal := any(val)

		// Try each member until we find one accepting the value.
		match := 0
		for i := 1; i < len(vec.childVectors); i++ {
			childVec := &vec.childVectors[i]
			err := childVec.setFn(childVec, rowIdx, anyVal)
			if err == nil {
				// The member accepted the value.
				match = i
				// Set the tag.
				setPrimitive(&vec.childVectors[0], rowIdx, uint8(i-1))
				break
			}
		}
		if match != 0 {
			// Set all other members to NULL.
			for i := 1; i < len(vec.childVectors); i++ {
				child := &vec.childVectors[i]
				if i == match {
					continue
				}
				if err := child.setFn(child, rowIdx, nil); err != nil {
					return err
				}
			}
			return nil
		}

		// No member accepted the value.
		return castError(reflect.TypeOf(val).String(), "UNION member")
	}
}

func setVectorVal[S any](vec *vector, rowIdx mapping.IdxT, val S) error {
	name, inMap := unsupportedTypeToStringMap[vec.Type]
	if inMap {
		return unsupportedTypeError(name)
	}

	switch vec.Type {
	case TYPE_BOOLEAN:
		return setBool(vec, rowIdx, val)
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
		return setTS(vec, rowIdx, val)
	case TYPE_DATE:
		return setDate(vec, rowIdx, val)
	case TYPE_TIME, TYPE_TIME_TZ:
		return setTime(vec, rowIdx, val)
	case TYPE_INTERVAL:
		return setInterval(vec, rowIdx, val)
	case TYPE_HUGEINT:
		return setHugeint(vec, rowIdx, val)
	case TYPE_VARCHAR:
		return setBytes(vec, rowIdx, val)
	case TYPE_BLOB:
		return setBytes(vec, rowIdx, val)
	case TYPE_DECIMAL:
		return setDecimal(vec, rowIdx, val)
	case TYPE_ENUM:
		return setEnum(vec, rowIdx, val)
	case TYPE_LIST:
		return setList(vec, rowIdx, val)
	case TYPE_STRUCT:
		return setStruct(vec, rowIdx, val)
	case TYPE_MAP, TYPE_ARRAY:
		// FIXME: Is this already supported? And tested?
		return unsupportedTypeError(unsupportedTypeToStringMap[vec.Type])
	case TYPE_UUID:
		return setUUID(vec, rowIdx, val)
	case TYPE_UNION:
		return setUnion(vec, rowIdx, val)
	default:
		return unsupportedTypeError(unknownTypeErrMsg)
	}
}
