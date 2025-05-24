package duckdb

import (
	"fmt"
	"reflect"
	"time"

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
	t := mapping.GetTypeId(lt)
	if r := createValueByTypeId(t, v); r != nil {
		return r, nil
	}
	switch t {
	case TYPE_ARRAY:
		return getMappedSliceValue(lt, t, v)
	case TYPE_LIST:
		return getMappedSliceValue(lt, t, v)
	case TYPE_STRUCT:
		return getMappedStructValue(lt, v)
	default:
		return nil, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func createValueByTypeId(t mapping.Type, v any) *mapping.Value {
	switch t {
	case TYPE_SQLNULL:
		return toPtr(mapping.CreateNullValue())
	case TYPE_BOOLEAN:
		return toPtr(mapping.CreateBool(v.(bool)))
	case TYPE_TINYINT:
		return toPtr(mapping.CreateInt8(v.(int8)))
	case TYPE_SMALLINT:
		return toPtr(mapping.CreateInt16(v.(int16)))
	case TYPE_INTEGER:
		return toPtr(mapping.CreateInt32(v.(int32)))
	case TYPE_BIGINT:
		return toPtr(mapping.CreateInt64(v.(int64)))
	case TYPE_UTINYINT:
		return toPtr(mapping.CreateUInt8(v.(uint8)))
	case TYPE_USMALLINT:
		return toPtr(mapping.CreateUInt16(v.(uint16)))
	case TYPE_UINTEGER:
		return toPtr(mapping.CreateUInt32(v.(uint32)))
	case TYPE_UBIGINT:
		return toPtr(mapping.CreateUInt64(v.(uint64)))
	case TYPE_FLOAT:
		return toPtr(mapping.CreateFloat(v.(float32)))
	case TYPE_DOUBLE:
		return toPtr(mapping.CreateDouble(v.(float64)))
	case TYPE_VARCHAR:
		return toPtr(mapping.CreateVarchar(v.(string)))
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		vv, err := getMappedTimestamp(v)
		if err != nil {
			return nil
		}
		return toPtr(mapping.CreateTimestamp(*vv))
	case TYPE_TIMESTAMP_S:
		vv, err := getMappedTimestampS(v)
		if err != nil {
			return nil
		}
		return toPtr(mapping.CreateTimestampS(*vv))
	case TYPE_TIMESTAMP_MS:
		vv, err := getMappedTimestampMS(v)
		if err != nil {
			return nil
		}
		return toPtr(mapping.CreateTimestampMS(*vv))
	case TYPE_TIMESTAMP_NS:
		vv, err := getMappedTimestampNS(v)
		if err != nil {
			return nil
		}
		return toPtr(mapping.CreateTimestampNS(*vv))
	}
	return nil
}

func toPtr[T any](x T) *T {
	return &x
}

func getPointerValue(v any) any {
	for {
		if v == nil {
			return nil
		}
		vo := reflect.ValueOf(v)
		if vo.Kind() == reflect.Ptr {
			if vo.IsNil() {
				return nil
			}
			v = vo.Elem().Interface()
			continue
		}
		return v
	}
}

func createValueByReflection(v any) (Type, *mapping.Value, error) {
	t := TYPE_INVALID
	switch v.(type) {
	case nil:
		t = TYPE_SQLNULL
	case bool:
		t = TYPE_BOOLEAN
	case int8:
		t = TYPE_TINYINT
	case int16:
		t = TYPE_SMALLINT
	case int32:
		t = TYPE_INTEGER
	case int64:
		t = TYPE_BIGINT
	case int:
		t = TYPE_BIGINT
		v = int64(v.(int))
	case uint8:
		t = TYPE_UTINYINT
	case uint16:
		t = TYPE_USMALLINT
	case uint32:
		t = TYPE_UINTEGER
	case uint64:
		t = TYPE_UBIGINT
	case uint:
		t = TYPE_UBIGINT
		v = uint64(v.(uint))
	case float32:
		t = TYPE_FLOAT
	case float64:
		t = TYPE_DOUBLE
	case string:
		t = TYPE_VARCHAR
	case []byte:
		t = TYPE_VARCHAR
		v = string(v.([]byte))
	case time.Time:
		t = TYPE_TIMESTAMP
	}
	if t != TYPE_INVALID {
		return t, createValueByTypeId(t, v), nil
	}
	r := reflect.Indirect(reflect.ValueOf(v))
	if r.IsNil() {
		t = TYPE_SQLNULL
		return t, createValueByTypeId(t, v), nil
	}
	switch r.Kind() {
	case reflect.Ptr:
		return createValueByReflection(getPointerValue(v))
	case reflect.Slice:
		vv, err := getMappedSliceValueByReflection(r.Interface(), false)
		return TYPE_LIST, vv, err
	case reflect.Array:
		vv, err := getMappedSliceValueByReflection(r.Interface(), true)
		return TYPE_ARRAY, vv, err
	default:
		return t, nil, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func getMappedSliceValueByReflection[T any](val T, isArray bool) (*mapping.Value, error) {
	createFunc := mapping.CreateListValue
	if isArray {
		createFunc = mapping.CreateArrayValue
	}
	vSlice, err := extractSlice(val)
	if err != nil {
		return nil, fmt.Errorf("could not cast %T to []any: %s", val, err)
	}
	var childValues []mapping.Value
	defer destroyValueSlice(childValues)
	if len(vSlice) == 0 {
		lt := mapping.CreateLogicalType(TYPE_SQLNULL)
		defer destroyLogicalTypes(toPtr([]mapping.LogicalType{lt}))
		return toPtr(createFunc(lt, childValues)), nil
	}
	elementType := TYPE_INVALID
	for _, v := range vSlice {
		et, vv, err := createValueByReflection(v)
		if err != nil {
			return nil, err
		}
		if elementType == TYPE_INVALID || elementType == TYPE_SQLNULL {
			elementType = et
		}
		childValues = append(childValues, *vv)
	}
	if elementType == TYPE_INVALID {
		return nil, unsupportedTypeError(reflect.TypeOf(val).Name())
	}
	lt := mapping.CreateLogicalType(elementType)
	defer destroyLogicalTypes(toPtr([]mapping.LogicalType{lt}))
	return toPtr(createFunc(lt, childValues)), nil
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
