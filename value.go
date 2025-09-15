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

func createValue(lt mapping.LogicalType, v any) (mapping.Value, error) {
	t := mapping.GetTypeId(lt)
	r, err := tryCreateValueByTypeId(t, v)
	if err != nil {
		return r, err
	}
	if r.Ptr != nil {
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
		return mapping.Value{}, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func tryCreateValueByTypeId(t mapping.Type, v any) (mapping.Value, error) {
	switch t {
	case TYPE_SQLNULL:
		return mapping.CreateNullValue(), nil
	case TYPE_BOOLEAN:
		return mapping.CreateBool(v.(bool)), nil
	case TYPE_TINYINT:
		return mapping.CreateInt8(v.(int8)), nil
	case TYPE_SMALLINT:
		return mapping.CreateInt16(v.(int16)), nil
	case TYPE_INTEGER:
		return mapping.CreateInt32(v.(int32)), nil
	case TYPE_BIGINT:
		return mapping.CreateInt64(v.(int64)), nil
	case TYPE_UTINYINT:
		return mapping.CreateUInt8(v.(uint8)), nil
	case TYPE_USMALLINT:
		return mapping.CreateUInt16(v.(uint16)), nil
	case TYPE_UINTEGER:
		return mapping.CreateUInt32(v.(uint32)), nil
	case TYPE_UBIGINT:
		return mapping.CreateUInt64(v.(uint64)), nil
	case TYPE_FLOAT:
		return mapping.CreateFloat(v.(float32)), nil
	case TYPE_DOUBLE:
		return mapping.CreateDouble(v.(float64)), nil
	case TYPE_VARCHAR:
		return mapping.CreateVarchar(v.(string)), nil
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		vv, err := getMappedTimestamp(t, v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestamp(*vv), nil
	case TYPE_TIMESTAMP_S:
		vv, err := getMappedTimestampS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampS(*vv), nil
	case TYPE_TIMESTAMP_MS:
		vv, err := getMappedTimestampMS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampMS(*vv), nil
	case TYPE_TIMESTAMP_NS:
		vv, err := getMappedTimestampNS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampNS(*vv), nil
	}
	return mapping.Value{}, nil
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

func isNil(i any) bool {
	if i == nil {
		return true
	}

	value := reflect.ValueOf(i)
	kind := value.Kind()

	switch kind {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Interface, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// leave the logic type clean up call to caller
func createValueByReflection(v any) (mapping.LogicalType, mapping.Value, error) {
	t, vv := inferTypeId(v)
	if t != TYPE_INVALID {
		retVal, err := tryCreateValueByTypeId(t, vv)
		return mapping.CreateLogicalType(t), retVal, err
	}
	if ss, ok := v.(fmt.Stringer); ok {
		t = TYPE_VARCHAR
		retVal, err := tryCreateValueByTypeId(t, ss.String())
		return mapping.CreateLogicalType(t), retVal, err
	}
	if isNil(v) {
		t = TYPE_SQLNULL
		retVal, err := tryCreateValueByTypeId(t, v)
		return mapping.CreateLogicalType(t), retVal, err
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Ptr:
		return createValueByReflection(getPointerValue(v))
	case reflect.Slice:
		return tryGetMappedSliceValue(r.Interface(), false, r.Len())
	case reflect.Array:
		return tryGetMappedSliceValue(r.Interface(), true, r.Len())
	default:
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func inferTypeId(v any) (Type, any) {
	t := TYPE_INVALID
	switch vv := v.(type) {
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
		v = int64(vv)
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
		v = uint64(vv)
	case float32:
		t = TYPE_FLOAT
	case float64:
		t = TYPE_DOUBLE
	case string:
		t = TYPE_VARCHAR
	case []byte:
		t = TYPE_VARCHAR
		v = string(vv)
	case time.Time:
		t = TYPE_TIMESTAMP
	}
	return t, v
}

func tryGetMappedSliceValue[T any](val T, isArray bool, sliceLength int) (mapping.LogicalType, mapping.Value, error) {
	createFunc := mapping.CreateListValue
	typeFunc := mapping.CreateListType
	if isArray {
		createFunc = mapping.CreateArrayValue
		typeFunc = func(child mapping.LogicalType) mapping.LogicalType {
			return mapping.CreateArrayType(child, mapping.IdxT(sliceLength))
		}
	}

	vSlice, err := extractSlice(val)
	if err != nil {
		return mapping.LogicalType{}, mapping.Value{}, fmt.Errorf("could not cast %T to []any: %w", val, err)
	}
	childValues := make([]mapping.Value, 0, sliceLength)
	defer destroyValueSlice(childValues)
	childLogicTypes := make([]mapping.LogicalType, 0, sliceLength)
	defer destroyLogicalTypes(&childLogicTypes)
	if len(vSlice) == 0 {
		lt := mapping.CreateLogicalType(TYPE_SQLNULL)
		defer mapping.DestroyLogicalType(&lt)
		return typeFunc(lt), createFunc(lt, childValues), nil
	}
	elementLogicType := mapping.LogicalType{}
	expectedIndex := -1
	expectedType := mapping.Type(0)
	for i, v := range vSlice {
		et, vv, err := createValueByReflection(v)
		if err != nil {
			return mapping.LogicalType{}, mapping.Value{}, err
		}
		if et.Ptr != nil {
			if elementLogicType.Ptr == nil {
				elementLogicType = et
				expectedIndex = i
				expectedType = mapping.GetTypeId(elementLogicType)
			} else {
				// Check if this element's type matches the first non-null element's type
				if logicalTypeString(elementLogicType) != logicalTypeString(et) {
					currentType := mapping.GetTypeId(et)
					return mapping.LogicalType{}, mapping.Value{},
						fmt.Errorf("mixed types in slice: cannot bind %s (index %d) and %s (index %d)",
							typeToStringMap[expectedType], expectedIndex, typeToStringMap[currentType], i)
				}
			}
		}
		childValues = append(childValues, vv)
		childLogicTypes = append(childLogicTypes, et)
	}
	if elementLogicType.Ptr == nil {
		return elementLogicType, mapping.Value{}, unsupportedTypeError(reflect.TypeOf(val).Name())
	}

	return typeFunc(elementLogicType), createFunc(elementLogicType, childValues), nil
}

func getMappedSliceValue[T any](lt mapping.LogicalType, t Type, val T) (mapping.Value, error) {
	var childType mapping.LogicalType
	switch t {
	case TYPE_ARRAY:
		childType = mapping.ArrayTypeChildType(lt)
	case TYPE_LIST:
		childType = mapping.ListTypeChildType(lt)
	}
	defer mapping.DestroyLogicalType(&childType)

	vSlice, err := extractSlice(val)
	if err != nil {
		return mapping.Value{}, fmt.Errorf("could not cast %T to []any: %w", val, err)
	}

	var childValues []mapping.Value
	defer destroyValueSlice(childValues)

	for _, v := range vSlice {
		vv, err := createValue(childType, v)
		if err != nil {
			return mapping.Value{}, fmt.Errorf("could not create value %w", err)
		}
		childValues = append(childValues, vv)
	}

	var v mapping.Value
	switch t {
	case TYPE_ARRAY:
		v = mapping.CreateArrayValue(childType, childValues)
	case TYPE_LIST:
		v = mapping.CreateListValue(childType, childValues)
	}
	return v, nil
}

func getMappedStructValue(lt mapping.LogicalType, val any) (mapping.Value, error) {
	vMap, ok := val.(map[string]any)
	if !ok {
		return mapping.Value{}, fmt.Errorf("could not cast %T to map[string]any", val)
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
				return mapping.Value{}, fmt.Errorf("could not create value %w", err)
			}
			values = append(values, vv)
		} else {
			values = append(values, mapping.CreateNullValue())
		}
	}
	return mapping.CreateStructValue(lt, values), nil
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

		for i := range rv.Len() {
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
