package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/mitchellh/mapstructure"
)

// FIXME: Implement support for these types.
var unsupportedTypeMap = map[C.duckdb_type]string{
	C.DUCKDB_TYPE_INVALID:  "INVALID",
	C.DUCKDB_TYPE_UHUGEINT: "UHUGEINT",
	C.DUCKDB_TYPE_ARRAY:    "ARRAY",
	C.DUCKDB_TYPE_UNION:    "UNION",
	C.DUCKDB_TYPE_BIT:      "BIT",
	C.DUCKDB_TYPE_TIME_TZ:  "TIME_TZ",
}

var duckdbTypeMap = map[C.duckdb_type]string{
	C.DUCKDB_TYPE_INVALID:      "INVALID",
	C.DUCKDB_TYPE_BOOLEAN:      "BOOLEAN",
	C.DUCKDB_TYPE_TINYINT:      "TINYINT",
	C.DUCKDB_TYPE_SMALLINT:     "SMALLINT",
	C.DUCKDB_TYPE_INTEGER:      "INTEGER",
	C.DUCKDB_TYPE_BIGINT:       "BIGINT",
	C.DUCKDB_TYPE_UTINYINT:     "UTINYINT",
	C.DUCKDB_TYPE_USMALLINT:    "USMALLINT",
	C.DUCKDB_TYPE_UINTEGER:     "UINTEGER",
	C.DUCKDB_TYPE_UBIGINT:      "UBIGINT",
	C.DUCKDB_TYPE_FLOAT:        "FLOAT",
	C.DUCKDB_TYPE_DOUBLE:       "DOUBLE",
	C.DUCKDB_TYPE_TIMESTAMP:    "TIMESTAMP",
	C.DUCKDB_TYPE_DATE:         "DATE",
	C.DUCKDB_TYPE_TIME:         "TIME",
	C.DUCKDB_TYPE_INTERVAL:     "INTERVAL",
	C.DUCKDB_TYPE_HUGEINT:      "HUGEINT",
	C.DUCKDB_TYPE_UHUGEINT:     "UHUGEINT",
	C.DUCKDB_TYPE_VARCHAR:      "VARCHAR",
	C.DUCKDB_TYPE_BLOB:         "BLOB",
	C.DUCKDB_TYPE_DECIMAL:      "DECIMAL",
	C.DUCKDB_TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	C.DUCKDB_TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	C.DUCKDB_TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	C.DUCKDB_TYPE_ENUM:         "ENUM",
	C.DUCKDB_TYPE_LIST:         "LIST",
	C.DUCKDB_TYPE_STRUCT:       "STRUCT",
	C.DUCKDB_TYPE_MAP:          "MAP",
	C.DUCKDB_TYPE_ARRAY:        "ARRAY",
	C.DUCKDB_TYPE_UUID:         "UUID",
	C.DUCKDB_TYPE_UNION:        "UNION",
	C.DUCKDB_TYPE_BIT:          "BIT",
	C.DUCKDB_TYPE_TIME_TZ:      "TIMETZ",
	C.DUCKDB_TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
}

type numericType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func convertNumericType[srcT numericType, destT numericType](val srcT) destT {
	return destT(val)
}

type UUID [16]byte

func (u *UUID) Scan(v any) error {
	if n := copy(u[:], v.([]byte)); n != 16 {
		return fmt.Errorf("invalid UUID length: %d", n)
	}
	return nil
}

// duckdb_hugeint is composed of (lower, upper) components.
// The value is computed as: upper * 2^64 + lower

func hugeIntToUUID(hi C.duckdb_hugeint) []byte {
	var uuid [16]byte
	// We need to flip the sign bit of the signed hugeint to transform it to UUID bytes
	binary.BigEndian.PutUint64(uuid[:8], uint64(hi.upper)^1<<63)
	binary.BigEndian.PutUint64(uuid[8:], uint64(hi.lower))
	return uuid[:]
}

func uuidToHugeInt(uuid UUID) C.duckdb_hugeint {
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

type Decimal struct {
	Width uint8
	Scale uint8
	Value *big.Int
}

func (d *Decimal) Float64() float64 {
	scale := big.NewInt(int64(d.Scale))
	factor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), scale, nil))
	value := new(big.Float).SetInt(d.Value)
	value.Quo(value, factor)
	f, _ := value.Float64()
	return f
}

func (d *Decimal) toString() string {
	return fmt.Sprintf("DECIMAL(%d,%d)", d.Width, d.Scale)
}
