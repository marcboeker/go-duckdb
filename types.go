package duckdb

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/google/uuid"

	"github.com/marcboeker/go-duckdb/mapping"
)

// Precomputed reflect type values to avoid repeated allocations.
var (
	reflectTypeBool      = reflect.TypeOf(true)
	reflectTypeInt8      = reflect.TypeOf(int8(0))
	reflectTypeInt16     = reflect.TypeOf(int16(0))
	reflectTypeInt32     = reflect.TypeOf(int32(0))
	reflectTypeInt64     = reflect.TypeOf(int64(0))
	reflectTypeUint8     = reflect.TypeOf(uint8(0))
	reflectTypeUint16    = reflect.TypeOf(uint16(0))
	reflectTypeUint32    = reflect.TypeOf(uint32(0))
	reflectTypeUint64    = reflect.TypeOf(uint64(0))
	reflectTypeFloat32   = reflect.TypeOf(float32(0))
	reflectTypeFloat64   = reflect.TypeOf(float64(0))
	reflectTypeTime      = reflect.TypeOf(time.Time{})
	reflectTypeInterval  = reflect.TypeOf(Interval{})
	reflectTypeBigInt    = reflect.TypeOf(big.NewInt(0))
	reflectTypeString    = reflect.TypeOf("")
	reflectTypeBytes     = reflect.TypeOf([]byte{})
	reflectTypeDecimal   = reflect.TypeOf(Decimal{})
	reflectTypeSliceAny  = reflect.TypeOf([]any{})
	reflectTypeMapString = reflect.TypeOf(map[string]any{})
	reflectTypeMap       = reflect.TypeOf(Map{})
	reflectTypeUnion     = reflect.TypeOf(Union{})
	reflectTypeAny       = reflect.TypeFor[any]()
)

type numericType interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

const uuidLength = 16

type UUID [uuidLength]byte

// Scan implements the sql.Scanner interface.
func (u *UUID) Scan(v any) error {
	switch val := v.(type) {
	case []byte:
		if len(val) != uuidLength {
			return u.Scan(string(val))
		}
		copy(u[:], val)
	case string:
		id, err := uuid.Parse(val)
		if err != nil {
			return err
		}
		copy(u[:], id[:])
	default:
		return fmt.Errorf("invalid UUID value type: %T", val)
	}
	return nil
}

// String implements the fmt.Stringer interface.
func (u *UUID) String() string {
	buf := make([]byte, 36)

	hex.Encode(buf, u[:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:], u[10:])

	return string(buf)
}

// Value implements the driver.Valuer interface.
func (u *UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

// duckdb_hugeint is composed of (lower, upper) components.
// The value is computed as: upper * 2^64 + lower

func hugeIntToUUID(hugeInt *mapping.HugeInt) []byte {
	// Flip the sign bit of the signed hugeint to transform it to UUID bytes.
	var val [uuidLength]byte
	lower, upper := mapping.HugeIntMembers(hugeInt)
	binary.BigEndian.PutUint64(val[:8], uint64(upper)^1<<63)
	binary.BigEndian.PutUint64(val[8:], lower)
	return val[:]
}

func uuidToHugeInt(uuid UUID) mapping.HugeInt {
	// Flip the sign bit.
	lower := binary.BigEndian.Uint64(uuid[8:])
	upper := binary.BigEndian.Uint64(uuid[:8])
	return mapping.NewHugeInt(lower, int64(upper^(1<<63)))
}

func hugeIntToNative(hugeInt *mapping.HugeInt) *big.Int {
	lower, upper := mapping.HugeIntMembers(hugeInt)
	i := big.NewInt(upper)
	i.Lsh(i, 64)
	i.Add(i, new(big.Int).SetUint64(lower))
	return i
}

func hugeIntFromNative(i *big.Int) (mapping.HugeInt, error) {
	d := big.NewInt(1)
	d.Lsh(d, 64)

	q := new(big.Int)
	r := new(big.Int)
	q.DivMod(i, d, r)

	if !q.IsInt64() {
		return mapping.HugeInt{}, fmt.Errorf("big.Int(%s) is too big for HUGEINT", i.String())
	}

	return mapping.NewHugeInt(r.Uint64(), q.Int64()), nil
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

func (i *Interval) getMappedInterval() mapping.Interval {
	return mapping.NewInterval(i.Months, i.Days, i.Micros)
}

// Composite can be used as the `Scanner` type for any composite types (maps, lists, structs).
type Composite[T any] struct {
	t T
}

func (s Composite[T]) Get() T {
	return s.t
}

func (s *Composite[T]) Scan(v any) error {
	return mapstructure.Decode(v, &s.t)
}

const max_decimal_width = 38

type Decimal struct {
	Width uint8
	Scale uint8
	Value *big.Int
}

func (d Decimal) Float64() float64 {
	scale := big.NewInt(int64(d.Scale))
	factor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), scale, nil))
	value := new(big.Float).SetInt(d.Value)
	value.Quo(value, factor)
	f, _ := value.Float64()
	return f
}

func (d Decimal) String() string {
	// Get the sign, and return early, if zero.
	if d.Value.Sign() == 0 {
		return "0"
	}

	// Remove the sign from the string integer value
	var signStr string
	scaleless := d.Value.String()
	if d.Value.Sign() < 0 {
		signStr = "-"
		scaleless = scaleless[1:]
	}

	// Remove all zeros from the right side
	zeroTrimmed := strings.TrimRightFunc(scaleless, func(r rune) bool { return r == '0' })
	scale := int(d.Scale) - (len(scaleless) - len(zeroTrimmed))

	// If the string is still bigger than the scale factor, output it without a decimal point
	if scale <= 0 {
		return signStr + zeroTrimmed + strings.Repeat("0", -1*scale)
	}

	// Pad a number with 0.0's if needed
	if len(zeroTrimmed) <= scale {
		return fmt.Sprintf("%s0.%s%s", signStr, strings.Repeat("0", scale-len(zeroTrimmed)), zeroTrimmed)
	}
	return signStr + zeroTrimmed[:len(zeroTrimmed)-scale] + "." + zeroTrimmed[len(zeroTrimmed)-scale:]
}

type Union struct {
	Value driver.Value `json:"value"`
	Tag   string       `json:"tag"`
}

func castToTime(val any) (time.Time, error) {
	var ti time.Time
	switch v := val.(type) {
	case time.Time:
		ti = v
	default:
		return ti, castError(reflect.TypeOf(val).String(), reflect.TypeOf(ti).String())
	}
	return ti.UTC(), nil
}

func getTSTicks(t Type, val any) (int64, error) {
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

func getMappedTimestamp(t Type, val any) (mapping.Timestamp, error) {
	ticks, err := getTSTicks(t, val)
	return mapping.NewTimestamp(ticks), err
}

func getMappedTimestampS(val any) (mapping.TimestampS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_S, val)
	return mapping.NewTimestampS(ticks), err
}

func getMappedTimestampMS(val any) (mapping.TimestampMS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_MS, val)
	return mapping.NewTimestampMS(ticks), err
}

func getMappedTimestampNS(val any) (mapping.TimestampNS, error) {
	ticks, err := getTSTicks(TYPE_TIMESTAMP_NS, val)
	return mapping.NewTimestampNS(ticks), err
}

func getMappedDate[T any](val T) (mapping.Date, error) {
	ti, err := castToTime(val)
	if err != nil {
		return mapping.Date{}, err
	}

	date := mapping.NewDate(int32(ti.Unix() / secondsPerDay))
	return date, err
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
