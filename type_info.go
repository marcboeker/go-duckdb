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

type structEntry struct {
	TypeInfo
	name string
}

// StructEntry is an interface to provide STRUCT entry information.
type StructEntry interface {
	// Info returns a STRUCT entry's type information.
	Info() TypeInfo
	// Name returns a STRUCT entry's name.
	Name() string
}

// NewStructEntry returns a STRUCT entry.
func NewStructEntry(info TypeInfo, name string) (StructEntry, error) {
	if name == "" {
		return nil, getError(errAPI, errEmptyName)
	}

	return &structEntry{
		TypeInfo: info,
		name:     name,
	}, nil
}

// Info returns a STRUCT entry's type information.
func (entry *structEntry) Info() TypeInfo {
	return entry.TypeInfo
}

// Name returns a STRUCT entry's name.
func (entry *structEntry) Name() string {
	return entry.name
}

type baseTypeInfo struct {
	Type
	structEntries []StructEntry
	decimalWidth  uint8
	decimalScale  uint8
}

type vectorTypeInfo struct {
	baseTypeInfo
	dict map[string]uint32
}

type typeInfo struct {
	baseTypeInfo
	childTypes []TypeInfo
	enumNames  []string
}

// TypeInfo is an interface for DuckDB types.
type TypeInfo interface {
	logicalType() C.duckdb_logical_type
}

// NewTypeInfo returns type information for DuckDB's primitive types.
func NewTypeInfo(t Type) (TypeInfo, error) {
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap {
		return nil, getError(errAPI, unsupportedTypeError(name))
	}

	switch t {
	case TYPE_DECIMAL:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewDecimalInfo)))
	case TYPE_ENUM:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewEnumInfo)))
	case TYPE_LIST:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewListInfo)))
	case TYPE_STRUCT:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewStructInfo)))
	case TYPE_MAP:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewMapInfo)))
	}

	return &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: t},
	}, nil
}

// NewDecimalInfo returns DECIMAL type information.
func NewDecimalInfo(width uint8, scale uint8) TypeInfo {
	return &typeInfo{
		baseTypeInfo: baseTypeInfo{
			Type:         TYPE_DECIMAL,
			decimalWidth: width,
			decimalScale: scale,
		},
	}
}

// NewEnumInfo returns ENUM type information.
func NewEnumInfo(first string, others ...string) TypeInfo {
	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{
			Type: TYPE_ENUM,
		},
		enumNames: make([]string, 0),
	}
	info.enumNames = append(info.enumNames, first)
	info.enumNames = append(info.enumNames, others...)
	return info
}

// NewListInfo returns LIST type information.
func NewListInfo(childInfo TypeInfo) (TypeInfo, error) {
	if childInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("childInfo"))
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: TYPE_LIST},
		childTypes:   make([]TypeInfo, 1),
	}
	info.childTypes[0] = childInfo
	return info, nil
}

// NewStructInfo returns STRUCT type information.
func NewStructInfo(firstEntry StructEntry, others ...StructEntry) (TypeInfo, error) {
	if firstEntry == nil {
		return nil, getError(errAPI, interfaceIsNilError("firstEntry"))
	}
	if firstEntry.Info() == nil {
		return nil, getError(errAPI, interfaceIsNilError("firstEntry.Info()"))
	}
	for i, entry := range others {
		if entry == nil {
			return nil, getError(errAPI, addIndexToError(interfaceIsNilError("entry"), i))
		}
		if entry.Info() == nil {
			return nil, getError(errAPI, addIndexToError(interfaceIsNilError("entry.Info()"), i))
		}
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{
			Type:          TYPE_STRUCT,
			structEntries: make([]StructEntry, 0),
		},
	}
	info.structEntries = append(info.structEntries, firstEntry)
	info.structEntries = append(info.structEntries, others...)
	return info, nil
}

// NewMapInfo returns MAP type information.
func NewMapInfo(keyInfo TypeInfo, valueInfo TypeInfo) (TypeInfo, error) {
	if keyInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("keyInfo"))
	}
	if valueInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("valueInfo"))
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: TYPE_MAP},
		childTypes:   make([]TypeInfo, 2),
	}
	info.childTypes[0] = keyInfo
	info.childTypes[1] = valueInfo
	return info, nil
}

func (info *typeInfo) logicalType() C.duckdb_logical_type {
	switch info.Type {
	case TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_UTINYINT, TYPE_USMALLINT,
		TYPE_UINTEGER, TYPE_UBIGINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ, TYPE_DATE, TYPE_TIME, TYPE_INTERVAL, TYPE_HUGEINT, TYPE_VARCHAR,
		TYPE_BLOB, TYPE_UUID:
		return C.duckdb_create_logical_type(C.duckdb_type(info.Type))

	case TYPE_DECIMAL:
		return C.duckdb_create_decimal_type(C.uint8_t(info.decimalWidth), C.uint8_t(info.decimalScale))
	case TYPE_ENUM:
		return info.logicalEnumType()
	case TYPE_LIST:
		return info.logicalListType()
	case TYPE_STRUCT:
		return info.logicalStructType()
	case TYPE_MAP:
		return info.logicalMapType()
	}
	return nil
}

func (info *typeInfo) logicalEnumType() C.duckdb_logical_type {
	count := len(info.enumNames)
	size := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, name := range info.enumNames {
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

func (info *typeInfo) logicalListType() C.duckdb_logical_type {
	child := info.childTypes[0].logicalType()
	logicalType := C.duckdb_create_list_type(child)
	C.duckdb_destroy_logical_type(&child)
	return logicalType
}

func (info *typeInfo) logicalStructType() C.duckdb_logical_type {
	count := len(info.structEntries)
	size := C.size_t(unsafe.Sizeof(C.duckdb_logical_type(nil)))
	types := (*[1 << 31]C.duckdb_logical_type)(C.malloc(C.size_t(count) * size))

	size = C.size_t(unsafe.Sizeof((*C.char)(nil)))
	names := (*[1 << 31]*C.char)(C.malloc(C.size_t(count) * size))

	for i, entry := range info.structEntries {
		(*types)[i] = entry.Info().logicalType()
		(*names)[i] = C.CString(entry.Name())
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

func (info *typeInfo) logicalMapType() C.duckdb_logical_type {
	key := info.childTypes[0].logicalType()
	value := info.childTypes[1].logicalType()
	logicalType := C.duckdb_create_map_type(key, value)

	C.duckdb_destroy_logical_type(&key)
	C.duckdb_destroy_logical_type(&value)
	return logicalType
}

func funcName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
