package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypeInterface(t *testing.T) {
	var primitiveTypes []Type
	for k := range typeToStringMap {
		_, inMap := unsupportedTypeToStringMap[k]
		if inMap {
			continue
		}
		switch k {
		case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP:
			continue
		}
		primitiveTypes = append(primitiveTypes, k)
	}

	// Create each primitive type information.
	var typeInfos []TypeInfo
	for _, primitive := range primitiveTypes {
		info, err := NewTypeInfo(primitive)
		require.NoError(t, err)
		typeInfos = append(typeInfos, info)
	}

	// Create nested types.
	decimalInfo := NewDecimalInfo(3, 2)
	enumInfo := NewEnumInfo("hello", "world")
	listInfo := NewListInfo(decimalInfo)
	nestedListInfo := NewListInfo(listInfo)

	firstEntry, err := NewStructEntry(enumInfo, "hello")
	require.NoError(t, err)
	secondEntry, err := NewStructEntry(nestedListInfo, "world")
	require.NoError(t, err)
	structInfo := NewStructInfo(firstEntry, secondEntry)

	firstEntry, err = NewStructEntry(structInfo, "hello")
	require.NoError(t, err)
	secondEntry, err = NewStructEntry(listInfo, "world")
	require.NoError(t, err)
	nestedStructInfo := NewStructInfo(firstEntry, secondEntry)

	mapInfo := NewMapInfo(nestedStructInfo, nestedListInfo)

	typeInfos = append(typeInfos, decimalInfo, enumInfo, listInfo, nestedListInfo, structInfo, nestedStructInfo, mapInfo)

	// Use each type as a child and to create the respective logical type.
	for _, info := range typeInfos {
		NewListInfo(info)
		require.NoError(t, err)
	}
}
