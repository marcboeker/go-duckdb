package duckdb

/*
   #include <stdlib.h>
   #include <duckdb.h>
*/
import "C"

import (
	"reflect"
	"runtime"
)

func funcName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

type baseType struct {
	// The data type of the vector.
	duckdbType C.duckdb_type
	// The child names of STRUCT vectors.
	childNames []string
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
func NewStructType(names []string, types []Type) (Type, error) {
	t := Type{
		baseType: baseType{
			duckdbType: C.DUCKDB_TYPE_STRUCT,
			childNames: make([]string, len(names))},
		childTypes: make([]Type, len(types)),
	}

	if len(names) != len(types) {
		return t, getError(errAPI, structFieldCountError(len(types), len(names)))
	}
	for i, name := range names {
		if name == "" {
			return t, getError(errAPI, addIndexToError(errEmptyName, i))
		}
	}
	for i, childType := range types {
		if childType.duckdbType == C.DUCKDB_TYPE_INVALID {
			return t, getError(errAPI, addIndexToError(errInvalidChildType, i))
		}
	}

	copy(t.childNames, names)
	copy(t.childTypes, types)
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

func (t Type) getLogicalType() C.duckdb_logical_type {
	// TODO
	return nil
}

//func tryGetDuckdbTypeFromValue(rt reflect.Type) (C.duckdb_logical_type, error) {
//	switch rt {
//	case reflect.TypeOf(time.Time{}):
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP_NS), nil
//	case reflect.TypeOf(UUID{}):
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UHUGEINT), nil
//	case reflect.TypeOf([]byte{}):
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID), nil
//	}
//	switch rt.Kind() {
//	// Invalid types
//	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Int, reflect.Uint, reflect.Uintptr, reflect.Complex64, reflect.Complex128:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), unsupportedTypeError(rt.String())
//	// Valid types
//	case reflect.Bool:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN), nil
//	case reflect.Int8:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT), nil
//	case reflect.Int16:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT), nil
//	case reflect.Int32:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER), nil
//	case reflect.Int64:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), nil
//	case reflect.Uint8:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT), nil
//	case reflect.Uint16:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT), nil
//	case reflect.Uint32:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER), nil
//	case reflect.Uint64:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), nil
//	case reflect.Float32:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT), nil
//	case reflect.Float64:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE), nil
//	case reflect.String:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), nil
//	case reflect.Struct:
//		var fields []reflect.StructField
//		for i := 0; i < rt.NumField(); i++ {
//			if rt.Field(i).IsExported() {
//				fields = append(fields, rt.Field(i))
//			}
//		}
//
//		types := (*[1 << 31]C.duckdb_logical_type)(C.malloc(C.ulong(uintptr(len(fields)) * unsafe.Sizeof(C.duckdb_logical_type(nil)))))
//		names := (*[1 << 31]*C.char)(C.malloc(C.ulong(uintptr(len(fields)) * unsafe.Sizeof((*C.char)(nil)))))
//		defer C.free(unsafe.Pointer(types))
//		defer C.free(unsafe.Pointer(names))
//		for i, field := range fields {
//			var err error
//			(*types)[i], err = tryGetDuckdbTypeFromValue(field.Type)
//			if err != nil {
//				return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), err
//			}
//			(*names)[i] = C.CString(field.Name)
//			defer C.free(unsafe.Pointer((*names)[i]))
//		}
//		ctypes := (*C.duckdb_logical_type)(unsafe.Pointer(types))
//		cnames := (**C.char)(unsafe.Pointer(names))
//		return C.duckdb_create_struct_type(ctypes, cnames, C.idx_t(len(fields))), nil
//	case reflect.Array:
//		elemt := rt.Elem()
//		t, err := tryGetDuckdbTypeFromValue(elemt)
//		return C.duckdb_create_array_type(t, C.idx_t(rt.Len())), err
//	case reflect.Slice:
//		elemt := rt.Elem()
//		t, err := tryGetDuckdbTypeFromValue(elemt)
//		return C.duckdb_create_list_type(t), err
//	case reflect.Map:
//		keyt := rt.Key()
//		kt, err := tryGetDuckdbTypeFromValue(keyt)
//		if err != nil {
//			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), err
//		}
//		elemt := rt.Elem()
//		vt, err := tryGetDuckdbTypeFromValue(elemt)
//		return C.duckdb_create_map_type(kt, vt), err
//	case reflect.Pointer, reflect.Interface:
//		return tryGetDuckdbTypeFromValue(rt.Elem())
//	// This case should never be reached
//	default:
//		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), nil
//	}
//}
