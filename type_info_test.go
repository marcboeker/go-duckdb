package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testTypeValues struct {
	input  string
	output string
}

type testTypeInfo struct {
	TypeInfo
	testTypeValues
}

var testPrimitiveSQLValues = map[Type]testTypeValues{
	TYPE_BOOLEAN:      {input: `true::BOOLEAN`, output: `true`},
	TYPE_TINYINT:      {input: `42::TINYINT`, output: `42`},
	TYPE_SMALLINT:     {input: `42::SMALLINT`, output: `42`},
	TYPE_INTEGER:      {input: `42::INTEGER`, output: `42`},
	TYPE_BIGINT:       {input: `42::BIGINT`, output: `42`},
	TYPE_UTINYINT:     {input: `43::UTINYINT`, output: `43`},
	TYPE_USMALLINT:    {input: `43::USMALLINT`, output: `43`},
	TYPE_UINTEGER:     {input: `43::UINTEGER`, output: `43`},
	TYPE_UBIGINT:      {input: `43::UBIGINT`, output: `43`},
	TYPE_FLOAT:        {input: `1.7::FLOAT`, output: `1.7`},
	TYPE_DOUBLE:       {input: `1.7::DOUBLE`, output: `1.7`},
	TYPE_TIMESTAMP:    {input: `TIMESTAMP '1992-09-20 11:30:00.123456'`, output: `1992-09-20 11:30:00.123456 +0000 UTC`},
	TYPE_DATE:         {input: `DATE '1992-09-20 11:30:00.123456789'`, output: `1992-09-20 00:00:00 +0000 UTC`},
	TYPE_TIME:         {input: `TIME '1992-09-20 11:30:00.123456'`, output: `0001-01-01 11:30:00.123456 +0000 UTC`},
	TYPE_INTERVAL:     {input: `INTERVAL 1 YEAR`, output: `{0 12 0}`},
	TYPE_HUGEINT:      {input: `44::HUGEINT`, output: `44`},
	TYPE_VARCHAR:      {input: `'hello world'::VARCHAR`, output: `hello world`},
	TYPE_BLOB:         {input: `'\xAA'::BLOB`, output: `[170]`},
	TYPE_TIMESTAMP_S:  {input: `TIMESTAMP_S '1992-09-20 11:30:00'`, output: `1992-09-20 11:30:00 +0000 UTC`},
	TYPE_TIMESTAMP_MS: {input: `TIMESTAMP_MS '1992-09-20 11:30:00.123'`, output: `1992-09-20 11:30:00.123 +0000 UTC`},
	TYPE_TIMESTAMP_NS: {input: `TIMESTAMP_NS '1992-09-20 11:30:00.123456789'`, output: `1992-09-20 11:30:00.123456789 +0000 UTC`},
	TYPE_UUID:         {input: `uuid()`, output: ``},
	TYPE_TIME_TZ:      {input: `'11:30:00.123456+06'::TIMETZ`, output: `0001-01-01 05:30:00.123456 +0000 UTC`},
	TYPE_TIMESTAMP_TZ: {input: `TIMESTAMPTZ '1992-09-20 11:30:00.123456+04'`, output: `1992-09-20 07:30:00.123456 +0000 UTC`},
}

func getTypeInfos(t *testing.T, useAny bool) []testTypeInfo {
	var primitiveTypes []Type
	for k := range typeToStringMap {
		_, inMap := unsupportedTypeToStringMap[k]
		if inMap && k != TYPE_ANY {
			continue
		}
		if k == TYPE_ANY && !useAny {
			continue
		}
		switch k {
		case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION, TYPE_SQLNULL:
			continue
		}
		primitiveTypes = append(primitiveTypes, k)
	}

	// Create each primitive type information.
	var testTypeInfos []testTypeInfo
	for _, primitive := range primitiveTypes {
		info, err := NewTypeInfo(primitive)
		require.NoError(t, err)
		testInfo := testTypeInfo{
			TypeInfo:       info,
			testTypeValues: testPrimitiveSQLValues[primitive],
		}
		testTypeInfos = append(testTypeInfos, testInfo)
	}

	// Create nested type information.

	info, err := NewDecimalInfo(3, 2)
	require.NoError(t, err)
	decimalTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `4::DECIMAL(3, 2)`,
			output: `4`,
		},
	}

	info, err = NewEnumInfo("hello", "world", "!")
	require.NoError(t, err)
	enumTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `'hello'::greeting`,
			output: `hello`,
		},
	}

	info, err = NewListInfo(decimalTypeInfo)
	require.NoError(t, err)
	listTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `[4::DECIMAL(3, 2)]`,
			output: `[4]`,
		},
	}

	info, err = NewListInfo(listTypeInfo)
	require.NoError(t, err)
	nestedListTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `[[4::DECIMAL(3, 2)]]`,
			output: `[[4]]`,
		},
	}

	firstEntry, err := NewStructEntry(enumTypeInfo, "hello")
	require.NoError(t, err)
	secondEntry, err := NewStructEntry(nestedListTypeInfo, "world")
	require.NoError(t, err)
	info, err = NewStructInfo(firstEntry, secondEntry)
	require.NoError(t, err)
	structTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `{'hello': 'hello'::greeting, 'world': [[4::DECIMAL(3, 2)]]}`,
			output: `map[hello:hello world:[[4]]]`,
		},
	}

	firstEntry, err = NewStructEntry(structTypeInfo, "hello")
	require.NoError(t, err)
	secondEntry, err = NewStructEntry(listTypeInfo, "world")
	require.NoError(t, err)
	info, err = NewStructInfo(firstEntry, secondEntry)
	require.NoError(t, err)
	nestedStructTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input: `{
						'hello': {'hello': 'hello'::greeting, 'world': [[4::DECIMAL(3, 2)]]},
						'world': [4::DECIMAL(3, 2)]
					}`,
			output: `map[hello:map[hello:hello world:[[4]]] world:[4]]`,
		},
	}

	info, err = NewMapInfo(decimalTypeInfo, nestedStructTypeInfo)
	require.NoError(t, err)
	mapTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input: `MAP {
					4::DECIMAL(3, 2) : {
						'hello': {'hello': 'hello'::greeting, 'world': [[4::DECIMAL(3, 2)]]},
						'world': [4::DECIMAL(3, 2)]
					}
					}`,
			output: `map[4:map[hello:map[hello:hello world:[[4]]] world:[4]]]`,
		},
	}

	primitiveInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)

	info, err = NewArrayInfo(primitiveInfo, 3)
	require.NoError(t, err)
	arrayTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `[4::INT, 8::INT, 16::INT]`,
			output: `[4 8 16]`,
		},
	}

	info, err = NewArrayInfo(arrayTypeInfo, 2)
	require.NoError(t, err)
	nestedArrayTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `[[4::INT, 8::INT, 16::INT], [3::INT, 6::INT, 9::INT]]`,
			output: `[[4 8 16] [3 6 9]]`,
		},
	}

	unionIntInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	unionStringInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	info, err = NewUnionInfo(
		[]TypeInfo{unionIntInfo, unionStringInfo},
		[]string{"int_val", "str_val"},
	)
	require.NoError(t, err)
	unionTypeInfo := testTypeInfo{
		TypeInfo: info,
		testTypeValues: testTypeValues{
			input:  `UNION_VALUE(int_val := 1::INTEGER)`,
			output: `{1 int_val}`,
		},
	}

	testTypeInfos = append(testTypeInfos, decimalTypeInfo, enumTypeInfo,
		listTypeInfo, nestedListTypeInfo, structTypeInfo, nestedStructTypeInfo, mapTypeInfo,
		arrayTypeInfo, nestedArrayTypeInfo, unionTypeInfo)
	return testTypeInfos
}

func TestTypeInterface(t *testing.T) {
	testTypeInfos := getTypeInfos(t, true)

	// Use each type as a child.
	for _, info := range testTypeInfos {
		_, err := NewListInfo(info.TypeInfo)
		require.NoError(t, err)
	}

	// Test UNION type creation.
	unionIntInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	unionStringInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	unionInfo, err := NewUnionInfo(
		[]TypeInfo{unionIntInfo, unionStringInfo},
		[]string{"int_val", "str_val"},
	)
	require.NoError(t, err)

	// Verify that we can use the UNION type as a child type.
	_, err = NewListInfo(unionInfo)
	require.NoError(t, err)
}

func TestErrTypeInfo(t *testing.T) {
	var incorrectTypes []Type
	incorrectTypes = append(incorrectTypes, TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION)

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
	unsupportedTypes = append(unsupportedTypes, TYPE_SQLNULL)

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

	// Invalid ARRAY entry.
	_, err = NewArrayInfo(validInfo, 0)
	testError(t, err, errAPI.Error(), errInvalidArraySize.Error())

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

	_, err = NewArrayInfo(nil, 3)
	testError(t, err, errAPI.Error(), interfaceIsNilErrMsg)

	// Invalid UNION types.
	unionIntInfo, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	unionStringInfo, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	// Test empty members.
	_, err = NewUnionInfo([]TypeInfo{}, []string{})
	testError(t, err, errAPI.Error(), "UNION type must have at least one member")

	// Test mismatched lengths.
	_, err = NewUnionInfo(
		[]TypeInfo{unionIntInfo, unionStringInfo},
		[]string{"single_name"},
	)
	testError(t, err, errAPI.Error(), "member types and names must have the same length")

	// Test empty name.
	_, err = NewUnionInfo(
		[]TypeInfo{unionIntInfo},
		[]string{""},
	)
	testError(t, err, errAPI.Error(), errEmptyName.Error())

	// Test duplicate names.
	_, err = NewUnionInfo(
		[]TypeInfo{unionIntInfo, unionStringInfo},
		[]string{"same_name", "same_name"},
	)
	testError(t, err, errAPI.Error(), duplicateNameErrMsg)
}
