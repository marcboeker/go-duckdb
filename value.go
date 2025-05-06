package duckdb

import (
	"fmt"
	"reflect"

	"github.com/marcboeker/go-duckdb/mapping"
)

func getValue(info TypeInfo, v mapping.Value) (any, error) {
	t := info.InternalType()
	switch t {
	case TYPE_BOOLEAN:
		return mapping.GetBool(v), nil
	case TYPE_TINYINT:
		return mapping.GetInt8(v), nil
	case TYPE_SMALLINT:
		return mapping.GetInt16(v), nil
	case TYPE_INTEGER:
		return mapping.GetInt32(v), nil
	case TYPE_BIGINT:
		return mapping.GetInt64(v), nil
	case TYPE_UTINYINT:
		return mapping.GetUInt8(v), nil
	case TYPE_USMALLINT:
		return mapping.GetUInt16(v), nil
	case TYPE_UINTEGER:
		return mapping.GetUInt32(v), nil
	case TYPE_UBIGINT:
		return mapping.GetUInt64(v), nil
	case TYPE_FLOAT:
		return mapping.GetFloat(v), nil
	case TYPE_DOUBLE:
		return mapping.GetDouble(v), nil
	case TYPE_TIMESTAMP_S:
		ts := mapping.GetTimestampS(v)
		return getTSS(&ts), nil
	case TYPE_TIMESTAMP_MS:
		ts := mapping.GetTimestampMS(v)
		return getTSMS(&ts), nil
	case TYPE_TIMESTAMP_NS:
		ts := mapping.GetTimestampNS(v)
		return getTSNS(&ts), nil
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := mapping.GetTimestamp(v)
		return getTS(t, &ts), nil
	case TYPE_DATE:
		date := mapping.GetDate(v)
		return getDate(&date), nil
	case TYPE_TIME:
		ti := mapping.GetTime(v)
		return getTime(&ti), nil
	case TYPE_TIME_TZ:
		ti := mapping.GetTimeTZ(v)
		return getTimeTZ(&ti), nil
	case TYPE_INTERVAL:
		interval := mapping.GetInterval(v)
		return getInterval(&interval), nil
	case TYPE_HUGEINT:
		hugeInt := mapping.GetHugeInt(v)
		return hugeIntToNative(&hugeInt), nil
	case TYPE_VARCHAR:
		return mapping.GetVarchar(v), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}

func createValue(lt mapping.LogicalType, v any) (*mapping.Value, error) {
	var vv mapping.Value
	var err error
	t := Type(mapping.GetTypeId(lt))
	switch t {
	case TYPE_BOOLEAN:
		vv, err = mapping.CreateBool(v.(bool)), nil
	case TYPE_TINYINT:
		vv, err = mapping.CreateInt8(v.(int8)), nil
	case TYPE_SMALLINT:
		vv, err = mapping.CreateInt16(v.(int16)), nil
	case TYPE_INTEGER:
		vv, err = mapping.CreateInt32(v.(int32)), nil
	case TYPE_BIGINT:
		vv, err = mapping.CreateInt64(v.(int64)), nil
	case TYPE_UTINYINT:
		vv, err = mapping.CreateUInt8(v.(uint8)), nil
	case TYPE_USMALLINT:
		vv, err = mapping.CreateUInt16(v.(uint16)), nil
	case TYPE_UINTEGER:
		vv, err = mapping.CreateUInt32(v.(uint32)), nil
	case TYPE_UBIGINT:
		vv, err = mapping.CreateUInt64(v.(uint64)), nil
	case TYPE_FLOAT:
		vv, err = mapping.CreateFloat(v.(float32)), nil
	case TYPE_DOUBLE:
		vv, err = mapping.CreateDouble(v.(float64)), nil
	case TYPE_VARCHAR:
		vv, err = mapping.CreateVarchar(v.(string)), nil
	case TYPE_ARRAY:
		return getMappedSliceValue(lt, t, v)
	case TYPE_LIST:
		return getMappedSliceValue(lt, t, v)
	case TYPE_STRUCT:
		return getMappedStructValue(lt, v)
	default:
		return nil, unsupportedTypeError(reflect.TypeOf(v).Name())
	}

	return &vv, err
}

func getMappedSliceValue[T any](lt mapping.LogicalType, t Type, val T) (*mapping.Value, error) {
	var childType mapping.LogicalType
	if t == TYPE_ARRAY {
		childType = mapping.ArrayTypeChildType(lt)
	} else if t == TYPE_LIST {
		childType = mapping.ListTypeChildType(lt)
	}
	defer mapping.DestroyLogicalType(&childType)

	vSlice, err := extractSlice(val)
	if err != nil {
		return nil, fmt.Errorf("could not cast %T to []any: %s", val, err)
	}

	var childValues []mapping.Value
	defer destroyValueSlice(childValues)

	for _, v := range vSlice {
		vv, err := createValue(childType, v)
		if err != nil {
			return nil, fmt.Errorf("could not create value %s", err)
		}
		childValues = append(childValues, *vv)
	}

	var v mapping.Value
	if t == TYPE_ARRAY {
		v = mapping.CreateArrayValue(childType, childValues)
	} else if t == TYPE_LIST {
		v = mapping.CreateListValue(childType, childValues)
	}

	return &v, nil
}

func getMappedStructValue(lt mapping.LogicalType, val any) (*mapping.Value, error) {
	vMap, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("could not cast %T to map[string]any", val)
	}

	var values []mapping.Value
	defer destroyValueSlice(values)

	childCount := mapping.StructTypeChildCount(lt)
	for i := mapping.IdxT(0); i < childCount; i++ {
		childName := mapping.StructTypeChildName(lt, i)
		childType := mapping.StructTypeChildType(lt, i)
		defer mapping.DestroyLogicalType(&childType)

		v, exists := vMap[childName]
		if exists {
			vv, err := createValue(childType, v)
			if err != nil {
				return nil, fmt.Errorf("could not create value %s", err)
			}
			values = append(values, *vv)
		} else {
			values = append(values, mapping.CreateNullValue())
		}
	}

	structValue := mapping.CreateStructValue(lt, values)
	return &structValue, nil
}

func destroyValueSlice(values []mapping.Value) {
	for _, v := range values {
		mapping.DestroyValue(&v)
	}
}

func canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	default:
		return false
	}
}

func extractSlice[S any](val S) ([]any, error) {
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
			if canNil(idx) && idx.IsNil() {
				s[i] = nil
				continue
			}

			s[i] = idx.Interface()
		}
	}

	return s, nil
}
