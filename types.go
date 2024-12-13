package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/marcboeker/go-duckdb/duckdbtypes"
)

type numericType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// duckdb_hugeint is composed of (lower, upper) components.
// The value is computed as: upper * 2^64 + lower

func hugeIntToUUID(hi C.duckdb_hugeint) []byte {
	var uuid [duckdbtypes.UUIDLength]byte
	// We need to flip the sign bit of the signed hugeint to transform it to UUID bytes
	binary.BigEndian.PutUint64(uuid[:8], uint64(hi.upper)^1<<63)
	binary.BigEndian.PutUint64(uuid[8:], uint64(hi.lower))
	return uuid[:]
}

func uuidToHugeInt(uuid duckdbtypes.UUID) C.duckdb_hugeint {
	var dt C.duckdb_hugeint
	upper := binary.BigEndian.Uint64(uuid[:8])
	// flip the sign bit
	upper = upper ^ (1 << 63)
	dt.upper = C.int64_t(upper)
	dt.lower = C.uint64_t(binary.BigEndian.Uint64(uuid[8:]))
	return dt
}

func hugeIntToNative(hi C.duckdb_hugeint) *big.Int {
	i := big.NewInt(int64(hi.upper))
	i.Lsh(i, 64)
	i.Add(i, new(big.Int).SetUint64(uint64(hi.lower)))
	return i
}

func hugeIntFromNative(i *big.Int) (C.duckdb_hugeint, error) {
	d := big.NewInt(1)
	d.Lsh(d, 64)

	q := new(big.Int)
	r := new(big.Int)
	q.DivMod(i, d, r)

	if !q.IsInt64() {
		return C.duckdb_hugeint{}, fmt.Errorf("big.Int(%s) is too big for HUGEINT", i.String())
	}

	return C.duckdb_hugeint{
		lower: C.uint64_t(r.Uint64()),
		upper: C.int64_t(q.Int64()),
	}, nil
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

func mapKeysField() string {
	return "key"
}

func mapValuesField() string {
	return "value"
}

const max_decimal_width = 38
