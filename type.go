package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"reflect"
	"runtime"
	"unsafe"
)

// Type represents a DuckDB type.
type Type struct {
	baseType
	// The child types of nested types.
	childTypes []Type
	// The dictionary names for ENUM types.
	dict []string
}

// NewPrimitiveType returns a new primitive type. go-duckdb's primitive types (without aliases) are as follows:
// BOOL, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER, UBIGINT, FLOAT, DOUBLE, VARCHAR.
func NewPrimitiveType(kind reflect.Kind) (Type, error) {
	switch kind {
	case reflect.Bool:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_BOOLEAN}}, nil
	case reflect.Int8:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TINYINT}}, nil
	case reflect.Int16:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_SMALLINT}}, nil
	case reflect.Int32, reflect.Int:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_INTEGER}}, nil
	case reflect.Int64:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_BIGINT}}, nil
	case reflect.Uint8:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UTINYINT}}, nil
	case reflect.Uint16:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_USMALLINT}}, nil
	case reflect.Uint32, reflect.Uint:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UINTEGER}}, nil
	case reflect.Uint64:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UBIGINT}}, nil
	case reflect.Float32:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_FLOAT}}, nil
	case reflect.Float64:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_DOUBLE}}, nil
	case reflect.String:
		return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_VARCHAR}}, nil
	case reflect.Array, reflect.Slice:
		return Type{}, getError(errAPI, tryOtherFuncError(funcName(NewListType)))
	case reflect.Struct:
		return Type{}, getError(errAPI, tryOtherFuncError(funcName(NewStructType)))
	case reflect.Map:
		return Type{}, getError(errAPI, tryOtherFuncError(funcName(NewMapType)))
	}

	// Remaining cases:	Invalid, Uintptr, Complex64, Complex128, Chan, Func, Interface, Pointer, UnsafePointer.
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_INVALID}}, getError(errAPI, unsupportedTypeError(kind.String()))
}

// NewTimestampType returns a new TIMESTAMP type.
func NewTimestampType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP}}
}

// NewTimestampSType returns a new TIMESTAMP_S type.
func NewTimestampSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_S}}
}

// NewTimestampMSType returns a new TIMESTAMP_MS type.
func NewTimestampMSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_MS}}
}

// NewTimestampNSType returns a new TIMESTAMP_NS type.
func NewTimestampNSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_NS}}
}

// NewTimestampTZType returns a new TIMESTAMP_TZ type.
func NewTimestampTZType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_TZ}}
}

// NewDateType returns a new DATE type.
func NewDateType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_DATE}}
}

// NewTimeType returns a new TIME type.
func NewTimeType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIME}}
}

// NewIntervalType returns a new INTERVAL type.
func NewIntervalType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_INTERVAL}}
}

// NewHugeIntType returns a new HUGEINT type.
func NewHugeIntType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UHUGEINT}}
}

// NewBlobType returns a new BLOB type.
func NewBlobType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_BLOB}}
}

// NewDecimalType returns a new DECIMAL type.
func NewDecimalType(width uint8, scale uint8) Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_DECIMAL,
		width: width,
		scale: scale,
	},
	}
}

// NewEnumType returns a new ENUM type.
func NewEnumType(dict []string) (Type, error) {
	if len(dict) == 0 {
		return Type{}, getError(errAPI, errEmptyDict)
	}

	t := Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_ENUM}}
	t.dict = make([]string, len(dict))
	copy(t.dict, dict)
	return t, nil
}

// NewListType returns a new LIST type.
func NewListType(child Type) (Type, error) {
	t := Type{
		baseType:   baseType{duckdbType: C.DUCKDB_TYPE_LIST},
		childTypes: make([]Type, 1),
	}

	if child.duckdbType == C.DUCKDB_TYPE_INVALID {
		return t, getError(errAPI, errInvalidChildType)
	}
	t.childTypes[0] = child
	return t, nil
}

// NewStructType returns a new STRUCT type.
func NewStructType(types []Type, names []string) (Type, error) {
	t := Type{
		baseType: baseType{
			duckdbType: C.DUCKDB_TYPE_STRUCT,
			names:      make([]string, len(names))},
		childTypes: make([]Type, len(types)),
	}

	if len(types) != len(names) {
		return t, getError(errAPI, structFieldCountError(len(names), len(types)))
	}
	for i, childType := range types {
		if childType.duckdbType == C.DUCKDB_TYPE_INVALID {
			return t, getError(errAPI, addIndexToError(errInvalidChildType, i))
		}
	}
	for i, name := range names {
		if name == "" {
			return t, getError(errAPI, addIndexToError(errEmptyName, i))
		}
	}

	copy(t.childTypes, types)
	copy(t.names, names)
	return t, nil
}

// NewMapType returns a new MAP type.
func NewMapType(key Type, value Type) (Type, error) {
	t := Type{
		baseType:   baseType{duckdbType: C.DUCKDB_TYPE_MAP},
		childTypes: make([]Type, 2),
	}

	if key.duckdbType == C.DUCKDB_TYPE_INVALID {
		return t, getError(errAPI, errInvalidKeyType)
	}
	if value.duckdbType == C.DUCKDB_TYPE_INVALID {
		return t, getError(errAPI, errInvalidValueType)
	}

	t.childTypes[0] = key
	t.childTypes[1] = value
	return t, nil
}

// NewUuidType returns a new UUID type.
func NewUuidType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UUID}}
}

func (t Type) logicalType() (C.duckdb_logical_type, error) {
	switch t.duckdbType {
	case C.DUCKDB_TYPE_INVALID, C.DUCKDB_TYPE_UHUGEINT, C.DUCKDB_TYPE_ARRAY, C.DUCKDB_TYPE_UNION,
		C.DUCKDB_TYPE_BIT, C.DUCKDB_TYPE_TIME_TZ:
		return nil, unsupportedTypeError(duckdbTypeMap[t.duckdbType])
	case C.DUCKDB_TYPE_BOOLEAN, C.DUCKDB_TYPE_TINYINT, C.DUCKDB_TYPE_SMALLINT, C.DUCKDB_TYPE_INTEGER, C.DUCKDB_TYPE_BIGINT,
		C.DUCKDB_TYPE_UTINYINT, C.DUCKDB_TYPE_USMALLINT, C.DUCKDB_TYPE_UINTEGER, C.DUCKDB_TYPE_UBIGINT, C.DUCKDB_TYPE_FLOAT,
		C.DUCKDB_TYPE_DOUBLE, C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS,
		C.DUCKDB_TYPE_TIMESTAMP_NS, C.DUCKDB_TYPE_TIMESTAMP_TZ, C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIME, C.DUCKDB_TYPE_INTERVAL,
		C.DUCKDB_TYPE_HUGEINT, C.DUCKDB_TYPE_VARCHAR, C.DUCKDB_TYPE_BLOB, C.DUCKDB_TYPE_UUID:
		return C.duckdb_create_logical_type(t.duckdbType), nil
	case C.DUCKDB_TYPE_DECIMAL:
		return C.duckdb_create_decimal_type(C.uint8_t(t.width), C.uint8_t(t.scale)), nil
	case C.DUCKDB_TYPE_ENUM:
		return t.logicalEnumType(), nil
	case C.DUCKDB_TYPE_LIST:
		return t.logicalListType(), nil
	case C.DUCKDB_TYPE_STRUCT:
		return t.logicalStructType(), nil
	case C.DUCKDB_TYPE_MAP:
		return t.logicalMapType(), nil
	}
	return nil, unsupportedTypeError(unknownTypeErrMsg)
}

func (t Type) logicalEnumType() C.duckdb_logical_type {
	count := len(t.names)
	size := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, name := range t.names {
		(*names)[i] = C.CString(name)
	}
	cNames := (**C.char)(unsafe.Pointer(names))
	logicalType := C.duckdb_create_enum_type(cNames, C.idx_t(count))

	for i := 0; i < count; i++ {
		C.free(unsafe.Pointer((*names)[i]))
	}
	C.free(unsafe.Pointer(names))
	return logicalType
}

func (t Type) logicalListType() C.duckdb_logical_type {
	child, _ := t.childTypes[0].logicalType()
	logicalType := C.duckdb_create_list_type(child)
	C.duckdb_destroy_logical_type(&child)
	return logicalType
}

func (t Type) logicalStructType() C.duckdb_logical_type {
	count := len(t.childTypes)
	size := C.size_t(unsafe.Sizeof(C.duckdb_logical_type(nil)))
	types := (*[1 << 31]C.duckdb_logical_type)(C.malloc(C.size_t(count) * size))

	size = C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, childType := range t.childTypes {
		(*types)[i], _ = childType.logicalType()
		(*names)[i] = C.CString(t.names[i])
	}

	cTypes := (*C.duckdb_logical_type)(unsafe.Pointer(types))
	cNames := (**C.char)(unsafe.Pointer(names))
	logicalType := C.duckdb_create_struct_type(cTypes, cNames, C.idx_t(count))

	for i := 0; i < count; i++ {
		C.duckdb_destroy_logical_type(&types[i])
		C.free(unsafe.Pointer((*names)[i]))
	}
	C.free(unsafe.Pointer(types))
	C.free(unsafe.Pointer(names))

	return logicalType
}

func (t Type) logicalMapType() C.duckdb_logical_type {
	key, _ := t.childTypes[0].logicalType()
	value, _ := t.childTypes[1].logicalType()
	logicalType := C.duckdb_create_map_type(key, value)
	C.duckdb_destroy_logical_type(&key)
	C.duckdb_destroy_logical_type(&value)
	return logicalType
}

func funcName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

type baseType struct {
	// The data type of the vector.
	duckdbType C.duckdb_type
	// The names slice serves different purposes depending on the vector's type.
	// - STRUCT child names.
	// - ENUM dictionary names.
	names []string
	// The  width of DECIMAL types.
	width uint8
	// The scale of DECIMAL types.
	scale uint8
}

type vectorType struct {
	baseType
	// The dictionary for ENUM types.
	dict map[string]uint32
}
