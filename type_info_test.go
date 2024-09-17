package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypeInfo(t *testing.T) {
	var primitiveTypes []Type
	for k := range typeToStringMap {
		_, inMap := unsupportedTypeToStringMap[k]
		if inMap && k != TYPE_ANY {
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
	decimalInfo, err := NewDecimalInfo(3, 2)
	require.NoError(t, err)
	enumInfo, err := NewEnumInfo("hello", "world", "!")
	require.NoError(t, err)
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

	// Use each type as a child.
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
		if k != TYPE_ANY {
			unsupportedTypes = append(unsupportedTypes, k)
		}
	}

	for _, unsupported := range unsupportedTypes {
		_, err := NewTypeInfo(unsupported)
		testError(t, err, errAPI.Error(), unsupportedTypeErrMsg)
	}

	// Invalid DECIMAL.
	_, err := NewDecimalInfo(0, 0)
	testError(t, err, errAPI.Error(), errInvalidDecimalWidth.Error())
	_, err = NewDecimalInfo(42, 20)
	testError(t, err, errAPI.Error(), errInvalidDecimalWidth.Error())
	_, err = NewDecimalInfo(5, 6)
	testError(t, err, errAPI.Error(), errInvalidDecimalScale.Error())

	// Invalid ENUM.
	_, err = NewEnumInfo("hello", "hello")
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
	_, err = NewEnumInfo("hello", "world", "hello")
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)

	validInfo, err := NewTypeInfo(TYPE_FLOAT)
	require.NoError(t, err)

	// Invalid STRUCT entry.
	_, err = NewStructEntry(validInfo, "")
	testError(t, err, errAPI.Error(), errEmptyName.Error())

	validStructEntry, err := NewStructEntry(validInfo, "hello")
	require.NoError(t, err)
	otherValidStructEntry, err := NewStructEntry(validInfo, "you")
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
	_, err = NewStructInfo(validStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
	_, err = NewStructInfo(validStructEntry, otherValidStructEntry, validStructEntry)
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)

	_, err = NewMapInfo(nil, validInfo)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
	_, err = NewMapInfo(validInfo, nil)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)
}
