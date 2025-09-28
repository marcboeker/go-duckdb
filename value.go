package duckdb

import (
	"fmt"
	"math/big"
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
	r, err := createPrimitiveValue(t, v)
	if err != nil {
		return r, err
	}

	if r.Ptr != nil {
		return r, nil
	}

	switch t {
	case TYPE_ARRAY:
		return createSliceValue(lt, t, v)
	case TYPE_LIST:
		return createSliceValue(lt, t, v)
	case TYPE_STRUCT:
		return createStructValue(lt, v)
	default:
		return mapping.Value{}, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

//nolint:gocyclo
func createPrimitiveValue(t mapping.Type, v any) (mapping.Value, error) {
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
		vv, err := inferTimestamp(t, v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestamp(vv), nil
	case TYPE_TIMESTAMP_S:
		vv, err := inferTimestampS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampS(vv), nil
	case TYPE_TIMESTAMP_MS:
		vv, err := inferTimestampMS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampMS(vv), nil
	case TYPE_TIMESTAMP_NS:
		vv, err := inferTimestampNS(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimestampNS(vv), nil
	case TYPE_DATE:
		vv, err := inferDate(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateDate(vv), nil
	case TYPE_TIME:
		vv, err := inferTime(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTime(vv), nil
	case TYPE_TIME_TZ:
		vv, err := inferTimeTZ(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateTimeTZValue(vv), nil
	case TYPE_INTERVAL:
		vv, err := inferInterval(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateInterval(vv), nil
	case TYPE_HUGEINT:
		vv, err := inferHugeInt(v)
		if err != nil {
			return mapping.Value{}, err
		}
		return mapping.CreateHugeInt(vv), nil
	case TYPE_UUID:
		vv, err := inferUUID(v)
		if err != nil {
			return mapping.Value{}, err
		}
		lower, upper := mapping.HugeIntMembers(&vv)
		uHugeInt := mapping.NewUHugeInt(lower, uint64(upper))
		return mapping.CreateUUID(uHugeInt), nil
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION:
		return mapping.Value{}, nil
	}
	return mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
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

func inferLogicalTypeAndValue(v any) (mapping.LogicalType, mapping.Value, error) {
	// Try to create a primitive type.
	t, vv := inferPrimitiveType(v)
	if isPrimitiveType(t) {
		val, err := createPrimitiveValue(t, vv)
		return mapping.CreateLogicalType(t), val, err
	}

	// User-provided type with a Stringer interface:
	// We create a string and return a VARCHAR value.
	// TYPE_DECIMAL has a Stringer interface.
	if ss, ok := v.(fmt.Stringer); ok {
		t = TYPE_VARCHAR
		val, err := createPrimitiveValue(t, ss.String())
		return mapping.CreateLogicalType(t), val, err
	}

	// SQLNULL type.
	if isNil(v) {
		t = TYPE_SQLNULL
		val, err := createPrimitiveValue(t, v)
		return mapping.CreateLogicalType(t), val, err
	}

	if t == TYPE_MAP {
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
	}
	if t == TYPE_UNION {
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[t])
	}

	// Complex types.
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Struct, reflect.Map:
		// TODO.
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(typeToStringMap[TYPE_STRUCT])
	case reflect.Ptr:
		// Extract pointer and recurse.
		return inferLogicalTypeAndValue(getPointerValue(v))
	case reflect.Array, reflect.Slice:
		return inferSliceLogicalTypeAndValue(r.Interface(), r.Kind() == reflect.Array, r.Len())
	default:
		return mapping.LogicalType{}, mapping.Value{}, unsupportedTypeError(reflect.TypeOf(v).Name())
	}
}

func inferPrimitiveType(v any) (Type, any) {
	// Return TYPE_INVALID for
	// TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_ARRAY, and for the unsupported types.
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
		// No support for TYPE_BLOB.
		t = TYPE_VARCHAR
		v = string(vv)
	case time.Time:
		// There is no way to distinguish between
		// TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
		// TYPE_TIME_TZ, TYPE_TIMESTAMP_TZ.
		t = TYPE_TIMESTAMP
	case Interval:
		t = TYPE_INTERVAL
	case *big.Int:
		t = TYPE_HUGEINT
	case Decimal:
		t = TYPE_DECIMAL
	case UUID:
		t = TYPE_UUID
	case Map:
		// We special-case TYPE_MAP to disambiguate with structs passed as map[string]any.
		t = TYPE_MAP
	case Union:
		t = TYPE_UNION
	}

	return t, v
}

func isPrimitiveType(t Type) bool {
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION:
		// Complex type.
		return false
	case TYPE_INVALID, TYPE_UHUGEINT, TYPE_BIT, TYPE_ANY, TYPE_BIGNUM, TYPE_SQLNULL:
		// Invalid or unsupported.
		return false
	}
	return true
}

func inferSliceLogicalTypeAndValue[T any](val T, array bool, length int) (mapping.LogicalType, mapping.Value, error) {
	createFunc := mapping.CreateListValue
	typeFunc := mapping.CreateListType
	if array {
		createFunc = mapping.CreateArrayValue
		typeFunc = func(child mapping.LogicalType) mapping.LogicalType {
			return mapping.CreateArrayType(child, mapping.IdxT(length))
		}
	}

	slice, err := extractSlice(val)
	if err != nil {
		return mapping.LogicalType{}, mapping.Value{}, err
	}

	values := make([]mapping.Value, 0, length)
	defer destroyValueSlice(values)

	if len(slice) == 0 {
		lt := mapping.CreateLogicalType(TYPE_SQLNULL)
		defer mapping.DestroyLogicalType(&lt)
		return typeFunc(lt), createFunc(lt, values), nil
	}

	logicalTypes := make([]mapping.LogicalType, 0, length)
	defer destroyLogicalTypes(&logicalTypes)

	var elemLogicalType mapping.LogicalType
	for _, v := range slice {
		et, vv, err := inferLogicalTypeAndValue(v)
		if err != nil {
			return mapping.LogicalType{}, mapping.Value{}, err
		}
		if et.Ptr != nil {
			elemLogicalType = et
		}
		values = append(values, vv)
		logicalTypes = append(logicalTypes, et)
	}

	if elemLogicalType.Ptr == nil {
		return elemLogicalType, mapping.Value{}, unsupportedTypeError(reflect.TypeOf(val).Name())
	}
	return typeFunc(elemLogicalType), createFunc(elemLogicalType, values), nil
}

func createSliceValue[T any](lt mapping.LogicalType, t Type, val T) (mapping.Value, error) {
	var childType mapping.LogicalType
	switch t {
	case TYPE_ARRAY:
		childType = mapping.ArrayTypeChildType(lt)
	case TYPE_LIST:
		childType = mapping.ListTypeChildType(lt)
	}
	defer mapping.DestroyLogicalType(&childType)

	slice, err := extractSlice(val)
	if err != nil {
		return mapping.Value{}, err
	}

	var values []mapping.Value
	defer destroyValueSlice(values)

	for _, v := range slice {
		vv, err := createValue(childType, v)
		if err != nil {
			return mapping.Value{}, err
		}
		values = append(values, vv)
	}

	var v mapping.Value
	switch t {
	case TYPE_ARRAY:
		v = mapping.CreateArrayValue(childType, values)
	case TYPE_LIST:
		v = mapping.CreateListValue(childType, values)
	}

	return v, nil
}

func createStructValue(lt mapping.LogicalType, val any) (mapping.Value, error) {
	m, ok := val.(map[string]any)
	if !ok {
		return mapping.Value{}, castError(reflect.TypeOf(val).Name(), reflect.TypeOf(map[string]any{}).Name())
	}

	var values []mapping.Value
	defer destroyValueSlice(values)

	childCount := mapping.StructTypeChildCount(lt)
	for i := mapping.IdxT(0); i < childCount; i++ {
		name := mapping.StructTypeChildName(lt, i)
		t := mapping.StructTypeChildType(lt, i)
		defer mapping.DestroyLogicalType(&t)

		v, exists := m[name]
		if exists {
			vv, err := createValue(t, v)
			if err != nil {
				return mapping.Value{}, err
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

		// Insert the values into the slice.
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
