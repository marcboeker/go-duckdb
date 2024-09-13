package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypeInfo(t *testing.T) {
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
	listInfo, err := NewListInfo(decimalInfo)
	require.NoError(t, err)
	nestedListInfo, err := NewListInfo(listInfo)
	require.NoError(t, err)

	firstEntry, err := NewStructEntry(enumInfo, "hello")
	require.NoError(t, err)
	secondEntry, err := NewStructEntry(nestedListInfo, "world")
	require.NoError(t, err)
	structInfo, err := NewStructInfo(firstEntry, secondEntry)
	require.NoError(t, err)

	firstEntry, err = NewStructEntry(structInfo, "hello")
	require.NoError(t, err)
	secondEntry, err = NewStructEntry(listInfo, "world")
	require.NoError(t, err)
	nestedStructInfo, err := NewStructInfo(firstEntry, secondEntry)
	require.NoError(t, err)

	mapInfo, err := NewMapInfo(nestedStructInfo, nestedListInfo)
	require.NoError(t, err)

	typeInfos = append(typeInfos, decimalInfo, enumInfo, listInfo, nestedListInfo, structInfo, nestedStructInfo, mapInfo)

	// Use each type as a child and to create the respective logical type.
	for _, info := range typeInfos {
		_, err = NewListInfo(info)
		require.NoError(t, err)
	}
}

func TestErrTypeInfo(t *testing.T) {
	t.Parallel()

	var incorrectTypes []Type
	incorrectTypes = append(incorrectTypes, TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP)

	for _, incorrect := range incorrectTypes {
		_, err := NewTypeInfo(incorrect)
		testError(t, err, errAPI.Error(), tryOtherFuncErrMsg)
	}

	var unsupportedTypes []Type
	for k := range unsupportedTypeToStringMap {
		unsupportedTypes = append(unsupportedTypes, k)
	}

	for _, unsupported := range unsupportedTypes {
		_, err := NewTypeInfo(unsupported)
		testError(t, err, errAPI.Error(), unsupportedTypeErrMsg)
	}

	validInfo, err := NewTypeInfo(TYPE_FLOAT)
	require.NoError(t, err)

	// Invalid STRUCT entry.
	_, err = NewStructEntry(validInfo, "")
	testError(t, err, errAPI.Error(), errEmptyName.Error())

	validStructEntry, err := NewStructEntry(validInfo, "hello")
	require.NoError(t, err)
	nilStructEntry, err := NewStructEntry(nil, "hello")
	require.NoError(t, err)

	// Invalid interfaces.
	_, err = NewListInfo(nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	_, err = NewStructInfo(nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(validStructEntry, nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(nilStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewStructInfo(validStructEntry, nilStructEntry)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	_, err = NewMapInfo(nil, validInfo)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewMapInfo(validInfo, nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
}
