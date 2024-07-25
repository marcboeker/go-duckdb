package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"math/big"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

// vector storage of a DuckDB column.
type vector struct {
	// The underlying DuckDB vector.
	duckdbVector C.duckdb_vector
	// The underlying data ptr.
	ptr unsafe.Pointer
	// The vector's validity mask.
	mask *C.uint64_t
	// A callback function to get a value from this vector.
	getFn fnGetVectorValue
	// A callback function to write to this vector.
	setFn fnSetVectorValue
	// The data type of the vector.
	duckdbType C.duckdb_type
	// The child vectors of nested data types.
	childVectors []vector

	// The child names of STRUCT vectors.
	childNames []string
	// The dictionary for ENUM types.
	dict map[string]uint32
	// The  width of DECIMAL types.
	width uint8
	// The scale of DECIMAL types.
	scale uint8
}

func (vec *vector) tryCast(val any) (any, error) {
	if val == nil {
		return val, nil
	}

	switch vec.duckdbType {
	case C.DUCKDB_TYPE_INVALID:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_BOOLEAN:
		return tryPrimitiveCast[bool](val, reflect.Bool.String())
	case C.DUCKDB_TYPE_TINYINT:
		return tryNumericCast[int8](val, reflect.Int8.String())
	case C.DUCKDB_TYPE_SMALLINT:
		return tryNumericCast[int16](val, reflect.Int16.String())
	case C.DUCKDB_TYPE_INTEGER:
		return tryNumericCast[int32](val, reflect.Int32.String())
	case C.DUCKDB_TYPE_BIGINT:
		return tryNumericCast[int64](val, reflect.Int64.String())
	case C.DUCKDB_TYPE_UTINYINT:
		return tryNumericCast[uint8](val, reflect.Uint8.String())
	case C.DUCKDB_TYPE_USMALLINT:
		return tryNumericCast[uint16](val, reflect.Uint16.String())
	case C.DUCKDB_TYPE_UINTEGER:
		return tryNumericCast[uint32](val, reflect.Uint32.String())
	case C.DUCKDB_TYPE_UBIGINT:
		return tryNumericCast[uint64](val, reflect.Uint64.String())
	case C.DUCKDB_TYPE_FLOAT:
		return tryNumericCast[float32](val, reflect.Float32.String())
	case C.DUCKDB_TYPE_DOUBLE:
		return tryNumericCast[float64](val, reflect.Float64.String())
	case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS,
		C.DUCKDB_TYPE_TIMESTAMP_NS, C.DUCKDB_TYPE_TIMESTAMP_TZ, C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIME:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case C.DUCKDB_TYPE_INTERVAL:
		return tryPrimitiveCast[Interval](val, reflect.TypeOf(Interval{}).String())
	case C.DUCKDB_TYPE_HUGEINT:
		// Note that this expects *big.Int.
		return tryPrimitiveCast[*big.Int](val, reflect.TypeOf(big.Int{}).String())
	case C.DUCKDB_TYPE_UHUGEINT:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_VARCHAR:
		return tryPrimitiveCast[string](val, reflect.String.String())
	case C.DUCKDB_TYPE_BLOB:
		return tryPrimitiveCast[[]byte](val, reflect.TypeOf([]byte{}).String())
	case C.DUCKDB_TYPE_DECIMAL:
		return vec.tryCastDecimal(val)
	case C.DUCKDB_TYPE_ENUM:
		return vec.tryCastEnum(val)
	case C.DUCKDB_TYPE_LIST:
		return vec.tryCastList(val)
	case C.DUCKDB_TYPE_STRUCT:
		return vec.tryCastStruct(val)
	case C.DUCKDB_TYPE_MAP:
		return tryPrimitiveCast[Map](val, reflect.TypeOf(Map{}).String())
	case C.DUCKDB_TYPE_ARRAY:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_UUID:
		return tryPrimitiveCast[UUID](val, reflect.TypeOf(UUID{}).String())
	case C.DUCKDB_TYPE_UNION:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_BIT:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	case C.DUCKDB_TYPE_TIME_TZ:
		return nil, unsupportedTypeError(duckdbTypeMap[vec.duckdbType])
	default:
		return nil, unsupportedTypeError("unknown type")
	}
}

func (*vector) canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	}
	return false
}

func tryPrimitiveCast[T any](val any, expected string) (T, error) {
	v, ok := val.(T)
	if ok {
		return v, nil
	}

	goType := reflect.TypeOf(val)
	return v, castError(goType.String(), expected)
}

func tryNumericCast[T numericType](val any, expected string) (T, error) {
	v, ok := val.(T)
	if ok {
		return v, nil
	}

	// JSON unmarshalling uses float64 for numbers.
	// We might want to add more implicit casts here.
	switch value := val.(type) {
	case float64:
		return convertNumericType[float64, T](value), nil
	}

	goType := reflect.TypeOf(val)
	return v, castError(goType.String(), expected)
}

func (vec *vector) tryCastDecimal(val any) (Decimal, error) {
	v, ok := val.(Decimal)
	if !ok {
		goType := reflect.TypeOf(val)
		return v, castError(goType.String(), reflect.TypeOf(Decimal{}).String())
	}

	if v.Width != vec.width || v.Scale != vec.scale {
		d := Decimal{Width: vec.width, Scale: vec.scale}
		return v, castError(d.toString(), v.toString())
	}
	return v, nil
}

func (vec *vector) tryCastEnum(val any) (string, error) {
	v, ok := val.(string)
	if !ok {
		goType := reflect.TypeOf(val)
		return v, castError(goType.String(), reflect.String.String())
	}

	_, ok = vec.dict[v]
	if !ok {
		return v, castError(v, "ENUM value")
	}
	return v, nil
}

func (vec *vector) tryCastList(val any) ([]any, error) {
	goType := reflect.TypeOf(val)
	if goType.Kind() != reflect.Slice {
		return nil, castError(goType.String(), reflect.Slice.String())
	}

	v := reflect.ValueOf(val)
	list := make([]any, v.Len())
	childVector := vec.childVectors[0]

	for i := 0; i < v.Len(); i++ {
		idx := v.Index(i)
		if vec.canNil(idx) && idx.IsNil() {
			list[i] = nil
			continue
		}

		var err error
		list[i], err = childVector.tryCast(idx.Interface())
		if err != nil {
			return nil, err
		}
	}
	return list, nil
}

func (vec *vector) tryCastStruct(val any) (map[string]any, error) {
	m, isMap := val.(map[string]any)

	// Transform the struct into map[string]any.
	if !isMap {
		// Catch mismatching types.
		goType := reflect.TypeOf(val)
		if reflect.TypeOf(val).Kind() != reflect.Struct {
			return nil, castError(goType.String(), reflect.Struct.String())
		}

		m = make(map[string]any)
		v := reflect.ValueOf(val)
		structType := v.Type()

		for i := 0; i < structType.NumField(); i++ {
			fieldName := structType.Field(i).Name
			m[fieldName] = v.Field(i).Interface()
		}
	}

	// Catch mismatching field count.
	if len(m) != len(vec.childNames) {
		return nil, structFieldError(strconv.Itoa(len(m)), strconv.Itoa(len(vec.childNames)))
	}

	// Cast child entries and return the map.
	for i := 0; i < len(vec.childVectors); i++ {
		childVector := vec.childVectors[i]
		childName := vec.childNames[i]
		v, ok := m[childName]

		// Catch mismatching field names.
		if !ok {
			return nil, structFieldError("missing field", childName)
		}

		var err error
		m[childName], err = childVector.tryCast(v)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (vec *vector) init(logicalType C.duckdb_logical_type, colIdx int) error {
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

func (vec *vector) initVectors(v C.duckdb_vector, writable bool) {
	vec.duckdbVector = v
	vec.ptr = C.duckdb_vector_get_data(v)
	if writable {
		C.duckdb_vector_ensure_validity_writable(v)
	}
	vec.mask = C.duckdb_vector_get_validity(v)
	vec.getChildVectors(v, writable)
}

func (vec *vector) getChildVectors(v C.duckdb_vector, writable bool) {
	switch vec.duckdbType {

	case C.DUCKDB_TYPE_LIST, C.DUCKDB_TYPE_MAP:
		child := C.duckdb_list_vector_get_child(v)
		vec.childVectors[0].initVectors(child, writable)

	case C.DUCKDB_TYPE_STRUCT:
		for i := 0; i < len(vec.childVectors); i++ {
			child := C.duckdb_struct_vector_get_child(v, C.idx_t(i))
			vec.childVectors[i].initVectors(child, writable)
		}
	}
}

func initPrimitive[T any](vec *vector, duckdbType C.duckdb_type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[T](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		setPrimitive(vec, rowIdx, val.(T))
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initTS(duckdbType C.duckdb_type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTS(duckdbType, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setTS(duckdbType, rowIdx, val)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initDate() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getDate(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setDate(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_DATE
}

func (vec *vector) initTime() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTime(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setTime(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_TIME
}

func (vec *vector) initInterval() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getInterval(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setInterval(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_INTERVAL
}

func (vec *vector) initHugeint() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getHugeint(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setHugeint(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_HUGEINT
}

func (vec *vector) initCString(duckdbType C.duckdb_type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getCString(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setCString(rowIdx, val)
	}
	vec.duckdbType = duckdbType
}

func (vec *vector) initDecimal(logicalType C.duckdb_logical_type, colIdx int) error {
	vec.width = uint8(C.duckdb_decimal_width(logicalType))
	vec.scale = uint8(C.duckdb_decimal_scale(logicalType))

	internalType := C.duckdb_decimal_internal_type(logicalType)
	switch internalType {
	case C.DUCKDB_TYPE_SMALLINT, C.DUCKDB_TYPE_INTEGER, C.DUCKDB_TYPE_BIGINT, C.DUCKDB_TYPE_HUGEINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getDecimal(internalType, rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
			if val == nil {
				vec.setNull(rowIdx)
				return
			}
			vec.setDecimal(internalType, rowIdx, val)
		}
	default:
		return columnError(unsupportedTypeError(duckdbTypeMap[internalType]), colIdx)
	}

	vec.duckdbType = C.DUCKDB_TYPE_DECIMAL
	return nil
}

func (vec *vector) initEnum(logicalType C.duckdb_logical_type, colIdx int) error {
	// Initialize the dictionary.
	dictSize := uint32(C.duckdb_enum_dictionary_size(logicalType))
	vec.dict = make(map[string]uint32)
	for i := uint32(0); i < dictSize; i++ {
		cStr := C.duckdb_enum_dictionary_value(logicalType, C.idx_t(i))
		str := C.GoString(cStr)
		vec.dict[str] = i
		C.duckdb_free(unsafe.Pointer(cStr))
	}

	internalType := C.duckdb_enum_internal_type(logicalType)
	switch internalType {
	case C.DUCKDB_TYPE_UTINYINT, C.DUCKDB_TYPE_USMALLINT, C.DUCKDB_TYPE_UINTEGER, C.DUCKDB_TYPE_UBIGINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getEnum(internalType, rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
			if val == nil {
				vec.setNull(rowIdx)
				return
			}
			vec.setEnum(internalType, rowIdx, val)
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

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getList(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setList(rowIdx, val)
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

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getStruct(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setStruct(rowIdx, val)
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

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getMap(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setMap(rowIdx, val)
	}
	vec.duckdbType = C.DUCKDB_TYPE_MAP
	return nil
}

func (vec *vector) initUUID() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		hugeInt := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
		return hugeIntToUUID(hugeInt)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		setPrimitive(vec, rowIdx, uuidToHugeInt(val.(UUID)))
	}
	vec.duckdbType = C.DUCKDB_TYPE_UUID
}
