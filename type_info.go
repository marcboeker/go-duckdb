package duckdb

import (
	"errors"
	"reflect"
	"runtime"

	"github.com/marcboeker/go-duckdb/mapping"
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
// info contains information about the entry's type, and name holds the entry's name.
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
	arrayLength   mapping.IdxT
	// The internal type for ENUM and DECIMAL values.
	internalType Type
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

// TypeInfo is an interface for a DuckDB type.
type TypeInfo interface {
	// InternalType returns the Type.
	InternalType() Type
	logicalType() mapping.LogicalType
}

func (info *typeInfo) InternalType() Type {
	return info.Type
}

// NewTypeInfo returns type information for DuckDB's primitive types.
// It returns the TypeInfo, if the Type parameter is a valid primitive type.
// Else, it returns nil, and an error.
// Valid types are:
// TYPE_[BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER,
// UBIGINT, FLOAT, DOUBLE, TIMESTAMP, DATE, TIME, INTERVAL, HUGEINT, VARCHAR, BLOB,
// TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, UUID, TIMESTAMP_TZ, ANY].
func NewTypeInfo(t Type) (TypeInfo, error) {
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap && t != TYPE_ANY {
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
	case TYPE_ARRAY:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewArrayInfo)))
	case TYPE_UNION:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewUnionInfo)))
	case TYPE_SQLNULL:
		return nil, getError(errAPI, unsupportedTypeError(typeToStringMap[t]))
	}

	return &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: t},
	}, nil
}

// NewDecimalInfo returns DECIMAL type information.
// Its input parameters are the width and scale of the DECIMAL type.
func NewDecimalInfo(width uint8, scale uint8) (TypeInfo, error) {
	if width < 1 || width > max_decimal_width {
		return nil, getError(errAPI, errInvalidDecimalWidth)
	}
	if scale > width {
		return nil, getError(errAPI, errInvalidDecimalScale)
	}

	return &typeInfo{
		baseTypeInfo: baseTypeInfo{
			Type:         TYPE_DECIMAL,
			decimalWidth: width,
			decimalScale: scale,
		},
	}, nil
}

// NewEnumInfo returns ENUM type information.
// Its input parameters are the dictionary values.
func NewEnumInfo(first string, others ...string) (TypeInfo, error) {
	// Check for duplicate names.
	m := map[string]bool{}
	m[first] = true
	for _, name := range others {
		_, inMap := m[name]
		if inMap {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{
			Type: TYPE_ENUM,
		},
		enumNames: make([]string, 0),
	}

	info.enumNames = append(info.enumNames, first)
	info.enumNames = append(info.enumNames, others...)
	return info, nil
}

// NewListInfo returns LIST type information.
// childInfo contains the type information of the LIST's elements.
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
// Its input parameters are the STRUCT entries.
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

	// Check for duplicate names.
	m := map[string]bool{}
	m[firstEntry.Name()] = true
	for _, entry := range others {
		name := entry.Name()
		_, inMap := m[name]
		if inMap {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
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
// keyInfo contains the type information of the MAP keys.
// valueInfo contains the type information of the MAP values.
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

// NewArrayInfo returns ARRAY type information.
// childInfo contains the type information of the ARRAY's elements.
// size is the ARRAY's fixed size.
func NewArrayInfo(childInfo TypeInfo, size uint64) (TypeInfo, error) {
	if childInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("childInfo"))
	}
	if size == 0 {
		return nil, getError(errAPI, errInvalidArraySize)
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: TYPE_ARRAY, arrayLength: mapping.IdxT(size)},
		childTypes:   make([]TypeInfo, 1),
	}
	info.childTypes[0] = childInfo
	return info, nil
}

// NewUnionInfo returns union type information.
// memberTypes contains the type information of the union members.
// memberNames contains the names of the union members.
func NewUnionInfo(memberTypes []TypeInfo, memberNames []string) (TypeInfo, error) {
	if len(memberTypes) == 0 {
		return nil, getError(errAPI, errors.New("union type must have at least one member"))
	}
	if len(memberTypes) != len(memberNames) {
		return nil, getError(errAPI, errors.New("member types and names must have same length"))
	}

	// Check for duplicate names
	m := map[string]bool{}
	for _, name := range memberNames {
		if name == "" {
			return nil, getError(errAPI, errEmptyName)
		}
		if m[name] {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
	}

	info := &typeInfo{
		baseTypeInfo: baseTypeInfo{Type: TYPE_UNION},
		childTypes:   memberTypes,
		// Store member names
		enumNames: memberNames, // We can reuse the enumNames field for union member names
	}
	return info, nil
}

func (info *typeInfo) logicalType() mapping.LogicalType {
	switch info.Type {
	case TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_UTINYINT, TYPE_USMALLINT,
		TYPE_UINTEGER, TYPE_UBIGINT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS,
		TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ, TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_INTERVAL, TYPE_HUGEINT, TYPE_VARCHAR,
		TYPE_BLOB, TYPE_UUID, TYPE_ANY:
		return mapping.CreateLogicalType(info.Type)
	case TYPE_DECIMAL:
		return mapping.CreateDecimalType(info.decimalWidth, info.decimalScale)
	case TYPE_ENUM:
		return mapping.CreateEnumType(info.enumNames)
	case TYPE_LIST:
		return info.logicalListType()
	case TYPE_STRUCT:
		return info.logicalStructType()
	case TYPE_MAP:
		return info.logicalMapType()
	case TYPE_ARRAY:
		return info.logicalArrayType()
	case TYPE_UNION:
		return info.logicalUnionType()
	}
	return mapping.LogicalType{}
}

func (info *typeInfo) logicalListType() mapping.LogicalType {
	child := info.childTypes[0].logicalType()
	defer mapping.DestroyLogicalType(&child)
	return mapping.CreateListType(child)
}

func (info *typeInfo) logicalStructType() mapping.LogicalType {
	var types []mapping.LogicalType
	defer destroyLogicalTypes(&types)

	var names []string
	for _, entry := range info.structEntries {
		types = append(types, entry.Info().logicalType())
		names = append(names, entry.Name())
	}
	return mapping.CreateStructType(types, names)
}

func (info *typeInfo) logicalMapType() mapping.LogicalType {
	key := info.childTypes[0].logicalType()
	defer mapping.DestroyLogicalType(&key)
	value := info.childTypes[1].logicalType()
	defer mapping.DestroyLogicalType(&value)
	return mapping.CreateMapType(key, value)
}

func (info *typeInfo) logicalArrayType() mapping.LogicalType {
	child := info.childTypes[0].logicalType()
	defer mapping.DestroyLogicalType(&child)
	return mapping.CreateArrayType(child, info.arrayLength)
}

func (info *typeInfo) logicalUnionType() mapping.LogicalType {
	var types []mapping.LogicalType
	defer destroyLogicalTypes(&types)

	var names []string
	for _, entry := range info.structEntries {
		types = append(types, entry.Info().logicalType())
		names = append(names, entry.Name())
	}
	return mapping.CreateUnionType(types, names)
}

func funcName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func destroyLogicalTypes(types *[]mapping.LogicalType) {
	for _, t := range *types {
		mapping.DestroyLogicalType(&t)
	}
}
