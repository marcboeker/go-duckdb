package duckdb

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestTypeInterface(t *testing.T) {
	var kinds []reflect.Kind
	kinds = append(kinds, reflect.Bool, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int,
		reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64, reflect.Float32,
		reflect.Float64, reflect.String)

	// Create each type.
	var types []Type
	for _, primitiveType := range kinds {
		newType, err := NewPrimitiveType(primitiveType)
		require.NoError(t, err)
		types = append(types, newType)
	}

	types = append(types, NewTimestampType(), NewTimestampSType(), NewTimestampMSType(), NewTimestampNSType(),
		NewTimestampTZType(), NewDateType(), NewTimeType(), NewIntervalType(), NewHugeIntType(), NewBlobType(),
		NewDecimalType(3, 2), NewUuidType())

	names := []string{"hello", "world"}
	enum, err := NewEnumType(names)
	require.NoError(t, err)

	child := NewTimestampType()
	list, err := NewListType(child)
	require.NoError(t, err)
	nestedList, err := NewListType(list)

	childTypes := []Type{NewIntervalType(), nestedList}
	structType, err := NewStructType(childTypes, names)
	require.NoError(t, err)

	nestedChildTypes := []Type{structType, list}
	nestedStructType, err := NewStructType(nestedChildTypes, names)
	require.NoError(t, err)

	mapType, err := NewMapType(nestedStructType, nestedList)
	require.NoError(t, err)

	types = append(types, enum, list, nestedList, structType, nestedStructType, mapType)

	// Use each type as a child.
	for _, childType := range types {
		_, err = NewListType(childType)
		require.NoError(t, err)
	}
}
