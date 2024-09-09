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
		return Type{}, getError(errAPI, apiUsageError(funcName(NewListType)))
	case reflect.Struct:
		return Type{}, getError(errAPI, apiUsageError(funcName(NewStructType)))
	case reflect.Map:
		return Type{}, getError(errAPI, apiUsageError(funcName(NewMapType)))
	}

	// Remaining cases:	Invalid, Uintptr, Complex64, Complex128, Chan, Func, Interface, Pointer, UnsafePointer.
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_INVALID}}, getError(errAPI, unsupportedTypeError(kind.String()))
}

func NewTimestampType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP}}
}

func NewTimestampSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_S}}
}

func NewTimestampMSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_MS}}
}

func NewTimestampNSType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_NS}}
}

func NewTimestampTZType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIMESTAMP_TZ}}
}

func NewDateType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_DATE}}
}

func NewTimeType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_TIME}}
}

func NewIntervalType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_INTERVAL}}
}

func NewHugeIntType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_UHUGEINT}}
}

func NewBlobType() Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_BLOB}}
}

func NewDecimalType(width uint8, scale uint8) Type {
	return Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_DECIMAL, width: width, scale: scale}}
}

func NewEnumType(dict []string) (Type, error) {
	if len(dict) == 0 {
		return Type{}, getError(errAPI, unsupportedTypeError(emptyDictErrMsg))
	}

	t := Type{baseType: baseType{duckdbType: C.DUCKDB_TYPE_ENUM}}
	t.dict = make([]string, len(dict))
	copy(t.dict, dict)
	return t, nil
}

func NewListType() (Type, error) {
	// TODO
	return Type{}, nil
}

func NewStructType() (Type, error) {
	// TODO
	return Type{}, nil
}

func NewMapType() (Type, error) {
	// TODO
	return Type{}, nil
}

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
