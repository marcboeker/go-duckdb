package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/marcboeker/go-duckdb/duckdbtypes"
	"github.com/marcboeker/go-duckdb/internal/uuidx"
)

type numericType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

// duckdb_hugeint is composed of (lower, upper) components.
// The value is computed as: upper * 2^64 + lower

func hugeIntToUUID(hi C.duckdb_hugeint) []byte {
	// Flip the sign bit of the signed hugeint to transform it to UUID bytes.
	var val [uuidx.ByteLength]byte
	binary.BigEndian.PutUint64(val[:8], uint64(hi.upper)^1<<63)
	binary.BigEndian.PutUint64(val[8:], uint64(hi.lower))
	return val[:]
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

func mapKeysField() string {
	return "key"
}

func mapValuesField() string {
	return "value"
}

const max_decimal_width = 38

func castToTime[T any](val T) (time.Time, error) {
	var ti time.Time
	switch v := any(val).(type) {
	case time.Time:
		ti = v
	default:
		return ti, castError(reflect.TypeOf(val).String(), reflect.TypeOf(ti).String())
	}
	return ti.UTC(), nil
}

func getTSTicks[T any](t Type, val T) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	if t == TYPE_TIMESTAMP_S {
		return ti.Unix(), nil
	}
	if t == TYPE_TIMESTAMP_MS {
		return ti.UnixMilli(), nil
	}

	year := ti.Year()
	if t == TYPE_TIMESTAMP || t == TYPE_TIMESTAMP_TZ {
		if year < -290307 || year > 294246 {
			return 0, conversionError(year, -290307, 294246)
		}
		return ti.UnixMicro(), nil
	}

	// TYPE_TIMESTAMP_NS:
	if year < 1678 || year > 2262 {
		return 0, conversionError(year, -290307, 294246)
	}
	return ti.UnixNano(), nil
}

func getCTimestamp[T any](t Type, val T) (C.duckdb_timestamp, error) {
	var ts C.duckdb_timestamp
	ticks, err := getTSTicks(t, val)
	if err != nil {
		return ts, err
	}

	ts.micros = C.int64_t(ticks)
	return ts, nil
}

func getCDate[T any](val T) (C.duckdb_date, error) {
	var date C.duckdb_date
	ti, err := castToTime(val)
	if err != nil {
		return date, err
	}

	days := int32(ti.Unix() / secondsPerDay)
	date.days = C.int32_t(days)
	return date, nil
}

func getTimeTicks[T any](val T) (int64, error) {
	ti, err := castToTime(val)
	if err != nil {
		return 0, err
	}

	// DuckDB stores time as microseconds since 00:00:00.
	base := time.Date(1970, time.January, 1, ti.Hour(), ti.Minute(), ti.Second(), ti.Nanosecond(), time.UTC)
	return base.UnixMicro(), err
}
