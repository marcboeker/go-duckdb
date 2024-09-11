package duckdb

/*
   #include <duckdb.h>
*/
import "C"

import (
	"reflect"
	"runtime"
	"unsafe"
)

type baseTypeInfo struct {
	// The data type.
	t Type
	// This slice serves different purposes.
	// - STRUCT child names.
	// - ENUM dictionary names.
	names []string
	// The  width of DECIMAL types.
	width uint8
	// The scale of DECIMAL types.
	scale uint8
}

// TypeInfo contains all information to work with DuckDB types.
type TypeInfo struct {
	baseTypeInfo
	// The child type information of nested types.
	childTypes []TypeInfo
}

type vectorTypeInfo struct {
	baseTypeInfo
	// The dictionary for ENUM types.
	dict map[string]uint32
}

// PrimitiveTypeInfo returns primitive type information.
func PrimitiveTypeInfo(t Type) (TypeInfo, error) {
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap {
		return TypeInfo{baseTypeInfo: baseTypeInfo{t: t}}, getError(errAPI, unsupportedTypeError(name))
	}

	switch t {
	case TYPE_DECIMAL:
		return TypeInfo{}, getError(errAPI, tryOtherFuncError(funcName(DecimalTypeInfo)))
	case TYPE_ENUM:
		return TypeInfo{}, getError(errAPI, tryOtherFuncError(funcName(EnumTypeInfo)))
	case TYPE_LIST:
		return TypeInfo{}, getError(errAPI, tryOtherFuncError(funcName(ListTypeInfo)))
	case TYPE_STRUCT:
		return TypeInfo{}, getError(errAPI, tryOtherFuncError(funcName(StructTypeInfo)))
	case TYPE_MAP:
		return TypeInfo{}, getError(errAPI, tryOtherFuncError(funcName(MapTypeInfo)))
	}

	return TypeInfo{baseTypeInfo: baseTypeInfo{t: t}}, nil
}

// DecimalTypeInfo returns DECIMAL type information.
func DecimalTypeInfo(width uint8, scale uint8) TypeInfo {
	return TypeInfo{
		baseTypeInfo: baseTypeInfo{
			t:     TYPE_DECIMAL,
			width: width,
			scale: scale,
		},
	}
}

// EnumTypeInfo returns ENUM type information.
func EnumTypeInfo(dict []string) (TypeInfo, error) {
	if len(dict) == 0 {
		return TypeInfo{}, getError(errAPI, errEmptySlice)
	}

	typeInfo := TypeInfo{baseTypeInfo: baseTypeInfo{t: TYPE_ENUM}}
	typeInfo.names = make([]string, len(dict))
	copy(typeInfo.names, dict)
	return typeInfo, nil
}

// ListTypeInfo returns LIST type information.
func ListTypeInfo(childInfo TypeInfo) (TypeInfo, error) {
	t := TypeInfo{
		baseTypeInfo: baseTypeInfo{t: TYPE_LIST},
		childTypes:   make([]TypeInfo, 1),
	}

	if childInfo.t == TYPE_INVALID {
		return t, getError(errAPI, errInvalidChildType)
	}
	t.childTypes[0] = childInfo
	return t, nil
}

// StructTypeInfo returns STRUCT type information.
func StructTypeInfo(typeInfos []TypeInfo, names []string) (TypeInfo, error) {
	t := TypeInfo{
		baseTypeInfo: baseTypeInfo{
			t:     TYPE_STRUCT,
			names: make([]string, len(names)),
		},
		childTypes: make([]TypeInfo, len(typeInfos)),
	}

	if len(typeInfos) == 0 || len(names) == 0 {
		return TypeInfo{}, getError(errAPI, errEmptySlice)
	}
	if len(typeInfos) != len(names) {
		return t, getError(errAPI, structFieldCountError(len(names), len(typeInfos)))
	}
	for i, childType := range typeInfos {
		if childType.t == TYPE_INVALID {
			return t, getError(errAPI, addIndexToError(errInvalidChildType, i))
		}
	}
	for i, name := range names {
		if name == "" {
			return t, getError(errAPI, addIndexToError(errEmptyName, i))
		}
	}

	copy(t.childTypes, typeInfos)
	copy(t.names, names)
	return t, nil
}

// MapTypeInfo returns MAP type information.
func MapTypeInfo(keyInfo TypeInfo, valueInfo TypeInfo) (TypeInfo, error) {
	t := TypeInfo{
		baseTypeInfo: baseTypeInfo{t: TYPE_MAP},
		childTypes:   make([]TypeInfo, 2),
	}

	if keyInfo.t == TYPE_INVALID {
		return t, getError(errAPI, errInvalidKeyType)
	}
	if valueInfo.t == TYPE_INVALID {
		return t, getError(errAPI, errInvalidValueType)
	}

	t.childTypes[0] = keyInfo
	t.childTypes[1] = valueInfo
	return t, nil
}

func (typeInfo *TypeInfo) logicalType() (C.duckdb_logical_type, error) {
	switch typeInfo.t {
	case TYPE_INVALID, TYPE_UHUGEINT, TYPE_ARRAY, TYPE_UNION, TYPE_BIT, TYPE_TIME_TZ:
		return nil, unsupportedTypeError(unsupportedTypeToStringMap[typeInfo.t])

	case TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_UTINYINT, TYPE_USMALLINT,
		TYPE_UINTEGER, TYPE_UBIGINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ, TYPE_DATE, TYPE_TIME, TYPE_INTERVAL, TYPE_HUGEINT, TYPE_VARCHAR,
		TYPE_BLOB, TYPE_UUID:
		return C.duckdb_create_logical_type(C.duckdb_type(typeInfo.t)), nil

	case TYPE_DECIMAL:
		return C.duckdb_create_decimal_type(C.uint8_t(typeInfo.width), C.uint8_t(typeInfo.scale)), nil
	case TYPE_ENUM:
		return typeInfo.logicalEnumType(), nil
	case TYPE_LIST:
		return typeInfo.logicalListType(), nil
	case TYPE_STRUCT:
		return typeInfo.logicalStructType(), nil
	case TYPE_MAP:
		return typeInfo.logicalMapType(), nil
	}
	return nil, unsupportedTypeError(unknownTypeErrMsg)
}

func (typeInfo *TypeInfo) logicalEnumType() C.duckdb_logical_type {
	count := len(typeInfo.names)
	size := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, name := range typeInfo.names {
		(*names)[i] = C.CString(name)
	}
	cNames := (**C.char)(unsafe.Pointer(names))
	logicalType := C.duckdb_create_enum_type(cNames, C.idx_t(count))

	for i := 0; i < count; i++ {
		C.duckdb_free(unsafe.Pointer((*names)[i]))
	}
	C.duckdb_free(unsafe.Pointer(names))
	return logicalType
}

func (typeInfo *TypeInfo) logicalListType() C.duckdb_logical_type {
	child, _ := typeInfo.childTypes[0].logicalType()
	logicalType := C.duckdb_create_list_type(child)
	C.duckdb_destroy_logical_type(&child)
	return logicalType
}

func (typeInfo *TypeInfo) logicalStructType() C.duckdb_logical_type {
	count := len(typeInfo.childTypes)
	size := C.size_t(unsafe.Sizeof(C.duckdb_logical_type(nil)))
	types := (*[1 << 31]C.duckdb_logical_type)(C.malloc(C.size_t(count) * size))

	size = C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, childType := range typeInfo.childTypes {
		(*types)[i], _ = childType.logicalType()
		(*names)[i] = C.CString(typeInfo.names[i])
	}

	cTypes := (*C.duckdb_logical_type)(unsafe.Pointer(types))
	cNames := (**C.char)(unsafe.Pointer(names))
	logicalType := C.duckdb_create_struct_type(cTypes, cNames, C.idx_t(count))

	for i := 0; i < count; i++ {
		C.duckdb_destroy_logical_type(&types[i])
		C.duckdb_free(unsafe.Pointer((*names)[i]))
	}
	C.duckdb_free(unsafe.Pointer(types))
	C.duckdb_free(unsafe.Pointer(names))

	return logicalType
}

func (typeInfo *TypeInfo) logicalMapType() C.duckdb_logical_type {
	key, _ := typeInfo.childTypes[0].logicalType()
	value, _ := typeInfo.childTypes[1].logicalType()
	logicalType := C.duckdb_create_map_type(key, value)
	C.duckdb_destroy_logical_type(&key)
	C.duckdb_destroy_logical_type(&value)
	return logicalType
}

func deleteLogicalType(logicalType C.duckdb_logical_type) {
	// FIXME: This is a placeholder for testing until we can test the logical types with UDFs.
	C.duckdb_destroy_logical_type(&logicalType)
}

func funcName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
