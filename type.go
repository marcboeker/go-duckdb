package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"
import (
	"reflect"
	"time"
	"unsafe"
)

type (
	// Contains a duckdb type, used for indicating the type of columns or parameters.
	// Consumes resources until closed with `(type).Free`
	Type struct {
		t0 reflect.Type
	}
)

func (t Type) toDuckdb() C.duckdb_logical_type {
	v, _ := tryGetDuckdbTypeFromValue(t.t0)
	return v
}

type SaveTypes interface {
	~bool | ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | time.Time | UUID | ~string | ~[]byte
}

func NewDuckdbType[T SaveTypes]() Type {
	return Type{
		t0: reflect.TypeFor[T](),
	}
}

func TryNewDuckdbType[T any]() (Type, error) {
	var err error
	if !canConvertToDuckdb(reflect.TypeFor[T]()) {
		err = unsupportedTypeError(reflect.TypeFor[T]().String())
	}
	return Type{
		t0: reflect.TypeFor[T](),
	}, err
}

func TryNewDuckdbTypeFromValue(v any) (Type, error) {
	var err error
	if !canConvertToDuckdb(reflect.TypeOf(v)) {
		err = unsupportedTypeError(reflect.TypeOf(v).String())
	}
	return Type{
		t0: reflect.TypeOf(v),
	}, err
}

func tryGetDuckdbType[T any]() (C.duckdb_logical_type, error) {
	return tryGetDuckdbTypeFromValue(reflect.TypeFor[T]())
}

func canConvertToDuckdb(rt reflect.Type) bool {
	switch rt {
	case reflect.TypeFor[time.Time](),
		reflect.TypeFor[UUID](),
		reflect.TypeFor[[]byte]():
		return true
	}
	switch rt.Kind() {
	// Invalid types
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Int, reflect.Uint, reflect.Uintptr, reflect.Complex64, reflect.Complex128:
		return false
	// Valid types
	case reflect.Bool,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		return true
	case reflect.Struct:
		var nfields int
		for i := rt.NumField() - 1; i >= 0; i-- {
			if rt.Field(0).IsExported() {
				nfields++
			}
		}

		for i := 0; i < nfields; i++ {
			if !rt.Field(i).IsExported() {
				continue
			}
			if !canConvertToDuckdb(rt.Field(i).Type) {
				return false
			}
		}
		return true
	case reflect.Array, reflect.Slice:
		elemt := rt.Elem()
		return canConvertToDuckdb(elemt)
	case reflect.Map:
		keyt := rt.Key()
		elemt := rt.Elem()
		return canConvertToDuckdb(elemt) && canConvertToDuckdb(keyt)
	case reflect.Pointer, reflect.Interface:
		return canConvertToDuckdb(rt.Elem())
	// This case should never be reached
	default:
		return false
	}
}

func tryGetDuckdbTypeFromValue(rt reflect.Type) (C.duckdb_logical_type, error) {
	switch rt {
	case reflect.TypeFor[time.Time]():
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TIMESTAMP_NS), nil
	case reflect.TypeFor[UUID]():
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UHUGEINT), nil
	case reflect.TypeFor[[]byte]():
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UUID), nil
	}
	switch rt.Kind() {
	// Invalid types
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Int, reflect.Uint, reflect.Uintptr, reflect.Complex64, reflect.Complex128:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), unsupportedTypeError(rt.String())
	// Valid types
	case reflect.Bool:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BOOLEAN), nil
	case reflect.Int8:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_TINYINT), nil
	case reflect.Int16:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_SMALLINT), nil
	case reflect.Int32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INTEGER), nil
	case reflect.Int64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_BIGINT), nil
	case reflect.Uint8:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UTINYINT), nil
	case reflect.Uint16:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_USMALLINT), nil
	case reflect.Uint32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UINTEGER), nil
	case reflect.Uint64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_UBIGINT), nil
	case reflect.Float32:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_FLOAT), nil
	case reflect.Float64:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_DOUBLE), nil
	case reflect.String:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_VARCHAR), nil
	case reflect.Struct:
		var nfields int
		for i := rt.NumField() - 1; i >= 0; i-- {
			if rt.Field(0).IsExported() {
				nfields++
			}
		}

		types := (*[1 << 31]C.duckdb_logical_type)(C.malloc(C.ulong(uintptr(nfields) * unsafe.Sizeof(C.duckdb_logical_type(nil)))))
		names := (*[1 << 31]*C.char)(C.malloc(C.ulong(uintptr(nfields) * unsafe.Sizeof((*C.char)(nil)))))
		defer C.free(unsafe.Pointer(types))
		defer C.free(unsafe.Pointer(names))
		for i := 0; i < nfields; i++ {
			if !rt.Field(i).IsExported() {
				continue
			}
			var err error
			(*types)[i], err = tryGetDuckdbTypeFromValue(rt.Field(i).Type)
			if err != nil {
				return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), err
			}
			(*names)[i] = C.CString(rt.Field(i).Name)
		}
		ctypes := (*C.duckdb_logical_type)(unsafe.Pointer(types))
		cnames := (**C.char)(unsafe.Pointer(names))
		return C.duckdb_create_struct_type(ctypes, cnames, C.idx_t(nfields)), nil
	case reflect.Array:
		elemt := rt.Elem()
		t, err := tryGetDuckdbTypeFromValue(elemt)
		return C.duckdb_create_array_type(t, C.idx_t(rt.Len())), err
	case reflect.Slice:
		elemt := rt.Elem()
		t, err := tryGetDuckdbTypeFromValue(elemt)
		return C.duckdb_create_list_type(t), err
	case reflect.Map:
		keyt := rt.Key()
		kt, err := tryGetDuckdbTypeFromValue(keyt)
		if err != nil {
			return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), err
		}
		elemt := rt.Elem()
		vt, err := tryGetDuckdbTypeFromValue(elemt)
		return C.duckdb_create_map_type(kt, vt), err
	case reflect.Pointer, reflect.Interface:
		return tryGetDuckdbTypeFromValue(rt.Elem())
	// This case should never be reached
	default:
		return C.duckdb_create_logical_type(C.DUCKDB_TYPE_INVALID), nil
	}
}
