package duckdb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/marcboeker/go-duckdb/mapping"
)

func TestSetGetPrimitive(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(int32(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []int32{-100, 0, 42, 1337, 2147483647}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[int32](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})

	t.Run("float64", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(float64(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []float64{-3.14, 0.0, 2.718, 1e10, -1e-10}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[float64](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})

	t.Run("bool", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(bool(false)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		setPrimitive(vec, 0, true)
		setPrimitive(vec, 1, false)
		setPrimitive(vec, 2, true)

		require.True(t, getPrimitive[bool](vec, 0))
		require.False(t, getPrimitive[bool](vec, 1))
		require.True(t, getPrimitive[bool](vec, 2))
	})

	t.Run("uint64", func(t *testing.T) {
		data := make([]byte, 100*unsafe.Sizeof(uint64(0)))
		vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

		testValues := []uint64{0, 1, 42, 18446744073709551615}
		for i, val := range testValues {
			setPrimitive(vec, mapping.IdxT(i), val)
			got := getPrimitive[uint64](vec, mapping.IdxT(i))
			require.Equal(t, val, got, "value at index %d", i)
		}
	})
}

func TestSetGetPrimitiveLargeIndex(t *testing.T) {
	data := make([]byte, 10000*int(unsafe.Sizeof(int32(0))))
	vec := &vector{dataPtr: unsafe.Pointer(&data[0])}

	testCases := []struct {
		idx mapping.IdxT
		val int32
	}{
		{0, 100},
		{100, 200},
		{1000, 300},
		{5000, 400},
		{9999, 500},
	}

	for _, tc := range testCases {
		setPrimitive(vec, tc.idx, tc.val)
		got := getPrimitive[int32](vec, tc.idx)
		require.Equal(t, tc.val, got, "value at index %d", tc.idx)
	}
}
