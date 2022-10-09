package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/mitchellh/mapstructure"
)

// HugeInt are composed in a (lower, upper) component
// The value of the HugeInt is upper * 2^64 + lower
type HugeInt C.duckdb_hugeint

func (v HugeInt) UUID() []byte {
	var uuid [16]byte
	// We need to flip the `sign bit` of the signed `HugeInt` to transform it to UUID bytes
	binary.BigEndian.PutUint64(uuid[:8], uint64(v.upper)^1<<63)
	binary.BigEndian.PutUint64(uuid[8:], uint64(v.lower))
	return uuid[:]
}

func (v HugeInt) Int64() (int64, error) {
	if v.upper == 0 && v.lower <= math.MaxInt64 {
		return int64(v.lower), nil
	} else if v.upper == -1 {
		return -int64(math.MaxUint64 - v.lower + 1), nil
	} else {
		return 0, fmt.Errorf("can not convert duckdb:hugeint to go:int64 (upper:%d, lower:%d)", v.upper, v.lower)
	}
}

type Map map[any]any

func (m *Map) Scan(v any) error {
	data, ok := v.(Map)
	if !ok {
		return fmt.Errorf("invalid type `%T` for scanning `Map`, expected `Map`", data)
	}

	*m = data
	return nil
}

type Interval struct {
	Days   int32 `json:"days"`
	Months int32 `json:"months"`
	Micros int64 `json:"micros"`
}

// Use as the `Scanner` type for any composite types (maps, lists, structs)
type Composite[T any] struct {
	t T
}

func (s Composite[T]) Get() T {
	return s.t
}

func (s *Composite[T]) Scan(v any) error {
	return mapstructure.Decode(v, &s.t)
}
