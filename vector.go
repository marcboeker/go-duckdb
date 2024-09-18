package duckdb

/*
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
	// The child vectors of nested data types.
	childVectors []vector

	// FIXME: This is a workaround until the C API exposes SQLNULL.
	// FIXME: Then, SQLNULL becomes another Type value (C.DUCKDB_TYPE_SQLNULL).
	isSQLNull bool

	// The vector's type information.
	vectorTypeInfo
}

func (vec *vector) tryCast(val any) (any, error) {
	if val == nil {
		return val, nil
	}

	name, inMap := unsupportedTypeToStringMap[vec.Type]
	if inMap {
		return nil, unsupportedTypeError(name)
	}

	switch vec.Type {
	case TYPE_BOOLEAN:
		return tryPrimitiveCast[bool](val, reflect.Bool.String())
	case TYPE_TINYINT:
		return tryNumericCast[int8](val, reflect.Int8.String())
	case TYPE_SMALLINT:
		return tryNumericCast[int16](val, reflect.Int16.String())
	case TYPE_INTEGER:
		return tryNumericCast[int32](val, reflect.Int32.String())
	case TYPE_BIGINT:
		return tryNumericCast[int64](val, reflect.Int64.String())
	case TYPE_UTINYINT:
		return tryNumericCast[uint8](val, reflect.Uint8.String())
	case TYPE_USMALLINT:
		return tryNumericCast[uint16](val, reflect.Uint16.String())
	case TYPE_UINTEGER:
		return tryNumericCast[uint32](val, reflect.Uint32.String())
	case TYPE_UBIGINT:
		return tryNumericCast[uint64](val, reflect.Uint64.String())
	case TYPE_FLOAT:
		return tryNumericCast[float32](val, reflect.Float32.String())
	case TYPE_DOUBLE:
		return tryNumericCast[float64](val, reflect.Float64.String())
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ,
		TYPE_DATE, TYPE_TIME:
		return tryPrimitiveCast[time.Time](val, reflect.TypeOf(time.Time{}).String())
	case TYPE_INTERVAL:
		return tryPrimitiveCast[Interval](val, reflect.TypeOf(Interval{}).String())
	case TYPE_HUGEINT:
		return tryPrimitiveCast[*big.Int](val, reflect.TypeOf(big.Int{}).String())
	case TYPE_VARCHAR:
		return tryPrimitiveCast[string](val, reflect.String.String())
	case TYPE_BLOB:
		return tryPrimitiveCast[[]byte](val, reflect.TypeOf([]byte{}).String())
	case TYPE_DECIMAL:
		return vec.tryCastDecimal(val)
	case TYPE_ENUM:
		return vec.tryCastEnum(val)
	case TYPE_LIST:
		return vec.tryCastList(val)
	case TYPE_STRUCT:
		return vec.tryCastStruct(val)
	case TYPE_MAP:
		return tryPrimitiveCast[Map](val, reflect.TypeOf(Map{}).String())
	case TYPE_UUID:
		return vec.tryCastUUID(val)
	}
	return nil, unsupportedTypeError(unknownTypeErrMsg)
}

func (*vector) canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	default:
		return false
	}
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

	if v.Width != vec.decimalWidth || v.Scale != vec.decimalScale {
		d := Decimal{Width: vec.decimalWidth, Scale: vec.decimalScale}
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
	count := len(vec.structEntries)
	if len(m) != count {
		return nil, structFieldError(strconv.Itoa(len(m)), strconv.Itoa(count))
	}

	// Cast child entries and return the map.
	for i := 0; i < count; i++ {
		childVector := vec.childVectors[i]
		name := vec.structEntries[i].Name()
		v, ok := m[name]

		// Catch mismatching field names.
		if !ok {
			return nil, structFieldError("missing field", name)
		}

		var err error
		m[name], err = childVector.tryCast(v)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (vec *vector) tryCastUUID(val any) (UUID, error) {
	uuid, ok := val.(UUID)
	if ok {
		return uuid, nil
	}

	bytes, ok := val.([]byte)
	if ok {
		if len(bytes) == UUIDLength {
			for i := 0; i < UUIDLength; i++ {
				uuid[i] = bytes[i]
			}
			return uuid, nil
		}
	}

	goType := reflect.TypeOf(val)
	return uuid, castError(goType.String(), reflect.TypeOf(UUID{}).String())
}

func (vec *vector) init(logicalType C.duckdb_logical_type, colIdx int) error {
	t := Type(C.duckdb_get_type_id(logicalType))

	if t == TYPE_INVALID {
		vec.isSQLNull = true
		return nil
	}

	name, inMap := unsupportedTypeToStringMap[t]
	if inMap {
		return addIndexToError(unsupportedTypeError(name), colIdx)
	}

	switch t {
	case TYPE_BOOLEAN:
		initPrimitive[bool](vec, t)
	case TYPE_TINYINT:
		initPrimitive[int8](vec, t)
	case TYPE_SMALLINT:
		initPrimitive[int16](vec, t)
	case TYPE_INTEGER:
		initPrimitive[int32](vec, t)
	case TYPE_BIGINT:
		initPrimitive[int64](vec, t)
	case TYPE_UTINYINT:
		initPrimitive[uint8](vec, t)
	case TYPE_USMALLINT:
		initPrimitive[uint16](vec, t)
	case TYPE_UINTEGER:
		initPrimitive[uint32](vec, t)
	case TYPE_UBIGINT:
		initPrimitive[uint64](vec, t)
	case TYPE_FLOAT:
		initPrimitive[float32](vec, t)
	case TYPE_DOUBLE:
		initPrimitive[float64](vec, t)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		vec.initTS(t)
	case TYPE_DATE:
		vec.initDate()
	case TYPE_TIME:
		vec.initTime()
	case TYPE_INTERVAL:
		vec.initInterval()
	case TYPE_HUGEINT:
		vec.initHugeint()
	case TYPE_VARCHAR, TYPE_BLOB:
		vec.initCString(t)
	case TYPE_DECIMAL:
		return vec.initDecimal(logicalType, colIdx)
	case TYPE_ENUM:
		return vec.initEnum(logicalType, colIdx)
	case TYPE_LIST:
		return vec.initList(logicalType, colIdx)
	case TYPE_STRUCT:
		return vec.initStruct(logicalType, colIdx)
	case TYPE_MAP:
		return vec.initMap(logicalType, colIdx)
	case TYPE_UUID:
		vec.initUUID()
	default:
		return addIndexToError(unsupportedTypeError(unknownTypeErrMsg), colIdx)
	}
	return nil
}

func (vec *vector) initVectors(v C.duckdb_vector, writable bool) {
	if vec.isSQLNull {
		return
	}

	vec.duckdbVector = v
	vec.ptr = C.duckdb_vector_get_data(v)
	if writable {
		C.duckdb_vector_ensure_validity_writable(v)
	}
	vec.mask = C.duckdb_vector_get_validity(v)
	vec.getChildVectors(v, writable)
}

func (vec *vector) getChildVectors(v C.duckdb_vector, writable bool) {
	switch vec.Type {

	case TYPE_LIST, TYPE_MAP:
		child := C.duckdb_list_vector_get_child(v)
		vec.childVectors[0].initVectors(child, writable)

	case TYPE_STRUCT:
		for i := 0; i < len(vec.childVectors); i++ {
			child := C.duckdb_struct_vector_get_child(v, C.idx_t(i))
			vec.childVectors[i].initVectors(child, writable)
		}
	}
}

func initPrimitive[T any](vec *vector, t Type) {
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
	vec.Type = t
}

func (vec *vector) initTS(t Type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTS(t, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
		if val == nil {
			vec.setNull(rowIdx)
			return
		}
		vec.setTS(t, rowIdx, val)
	}
	vec.Type = t
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
	vec.Type = TYPE_DATE
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
	vec.Type = TYPE_TIME
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
	vec.Type = TYPE_INTERVAL
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
	vec.Type = TYPE_HUGEINT
}

func (vec *vector) initCString(t Type) {
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
	vec.Type = t
}

func (vec *vector) initDecimal(logicalType C.duckdb_logical_type, colIdx int) error {
	vec.decimalWidth = uint8(C.duckdb_decimal_width(logicalType))
	vec.decimalScale = uint8(C.duckdb_decimal_scale(logicalType))

	t := Type(C.duckdb_decimal_internal_type(logicalType))
	switch t {
	case TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_HUGEINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getDecimal(t, rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
			if val == nil {
				vec.setNull(rowIdx)
				return
			}
			vec.setDecimal(t, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_DECIMAL
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

	t := Type(C.duckdb_enum_internal_type(logicalType))
	switch t {
	case TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getEnum(t, rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) {
			if val == nil {
				vec.setNull(rowIdx)
				return
			}
			vec.setEnum(t, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_ENUM
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
	vec.Type = TYPE_LIST
	return nil
}

func (vec *vector) initStruct(logicalType C.duckdb_logical_type, colIdx int) error {
	childCount := int(C.duckdb_struct_type_child_count(logicalType))
	var structEntries []StructEntry
	for i := 0; i < childCount; i++ {
		name := C.duckdb_struct_type_child_name(logicalType, C.idx_t(i))
		entry, err := NewStructEntry(nil, C.GoString(name))
		structEntries = append(structEntries, entry)
		C.duckdb_free(unsafe.Pointer(name))
		if err != nil {
			return err
		}
	}

	vec.childVectors = make([]vector, childCount)
	vec.structEntries = structEntries

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
	vec.Type = TYPE_STRUCT
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

	t := Type(C.duckdb_get_type_id(keyType))
	switch t {
	case TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY:
		return addIndexToError(errUnsupportedMapKeyType, colIdx)
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
	vec.Type = TYPE_MAP
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
	vec.Type = TYPE_UUID
}
