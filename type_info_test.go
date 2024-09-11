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
		typeInfo, err := PrimitiveTypeInfo(primitive)
		require.NoError(t, err)
		typeInfos = append(typeInfos, typeInfo)
	}

	// Create nested types.
	decimalInfo := DecimalTypeInfo(3, 2)

	names := []string{"hello", "world"}
	enumInfo, err := EnumTypeInfo(names)
	require.NoError(t, err)

	listInfo, err := ListTypeInfo(decimalInfo)
	require.NoError(t, err)
	nestedListInfo, err := ListTypeInfo(listInfo)
	require.NoError(t, err)

	childTypeInfos := []TypeInfo{enumInfo, nestedListInfo}
	structTypeInfo, err := StructTypeInfo(childTypeInfos, names)
	require.NoError(t, err)

	nestedChildTypeInfos := []TypeInfo{structTypeInfo, listInfo}
	nestedStructTypeInfo, err := StructTypeInfo(nestedChildTypeInfos, names)
	require.NoError(t, err)

	mapTypeInfo, err := MapTypeInfo(nestedStructTypeInfo, nestedListInfo)
	require.NoError(t, err)

	typeInfos = append(typeInfos, decimalInfo, enumInfo, listInfo, nestedListInfo, structTypeInfo, nestedStructTypeInfo, mapTypeInfo)

	// Use each type as a child and to create the respective logical type.
	for _, info := range typeInfos {
		_, err = ListTypeInfo(info)
		require.NoError(t, err)
	}
}
