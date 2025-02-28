package duckdb

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/marcboeker/go-duckdb/mapping"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx mapping.IdxT) any

func (vec *vector) getNull(rowIdx mapping.IdxT) bool {
	if vec.maskPtr == nil {
		return false
	}
	return !mapping.ValidityMaskValueIsValid(vec.maskPtr, rowIdx)
}

func getPrimitive[T any](vec *vector, rowIdx mapping.IdxT) T {
	xs := (*[1 << 31]T)(vec.dataPtr)
	return xs[rowIdx]
}

func (vec *vector) getTS(t Type, rowIdx mapping.IdxT) time.Time {
	val := getPrimitive[mapping.Timestamp](vec, rowIdx)
	return getTS(t, val)
}

func getTS(t Type, ts mapping.Timestamp) time.Time {
	switch t {
	case TYPE_TIMESTAMP:
		return time.UnixMicro(mapping.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_S:
		return time.Unix(mapping.TimestampGetMicros(&ts), 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(mapping.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, mapping.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(mapping.TimestampGetMicros(&ts)).UTC()
	}
	return time.Time{}
}

func (vec *vector) getDate(rowIdx mapping.IdxT) time.Time {
	date := getPrimitive[mapping.Date](vec, rowIdx)
	return getDate(date)
}

func getDate(date mapping.Date) time.Time {
	d := mapping.FromDate(date)
	year := int(mapping.DateStructGetYear(&d))
	month := time.Month(mapping.DateStructGetMonth(&d))
	day := int(mapping.DateStructGetDay(&d))
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func (vec *vector) getTime(rowIdx mapping.IdxT) time.Time {
	switch vec.Type {
	case TYPE_TIME:
		val := getPrimitive[mapping.Time](vec, rowIdx)
		return getTime(val)
	case TYPE_TIME_TZ:
		ti := getPrimitive[mapping.TimeTZ](vec, rowIdx)
		return getTimeTZ(ti)
	}
	return time.Time{}
}

func getTime(ti mapping.Time) time.Time {
	micros := mapping.TimeGetMicros(&ti)
	unix := time.UnixMicro(micros).UTC()
	return time.Date(1, time.January, 1, unix.Hour(), unix.Minute(), unix.Second(), unix.Nanosecond(), time.UTC)
}

func getTimeTZ(ti mapping.TimeTZ) time.Time {
	timeTZStruct := mapping.FromTimeTZ(ti)
	timeStruct := mapping.TimeTZStructGetTimeStruct(&timeTZStruct)
	offset := mapping.TimeTZStructGetOffset(&timeTZStruct)

	// TIMETZ has microsecond precision.
	nanos := int(mapping.TimeStructGetMicros(&timeStruct)) * 1000
	hour := int(mapping.TimeStructGetHour(&timeStruct))
	minute := int(mapping.TimeStructGetMinute(&timeStruct))
	sec := int(mapping.TimeStructGetSecond(&timeStruct))
	loc := time.FixedZone("", int(offset))
	return time.Date(1, time.January, 1, hour, minute, sec, nanos, loc).UTC()
}

func (vec *vector) getInterval(rowIdx mapping.IdxT) Interval {
	interval := getPrimitive[mapping.Interval](vec, rowIdx)
	return getInterval(interval)
}

func getInterval(interval mapping.Interval) Interval {
	return Interval{
		Days:   mapping.IntervalGetDays(&interval),
		Months: mapping.IntervalGetMonths(&interval),
		Micros: mapping.IntervalGetMicros(&interval),
	}
}

func (vec *vector) getHugeint(rowIdx mapping.IdxT) *big.Int {
	hugeInt := getPrimitive[mapping.HugeInt](vec, rowIdx)
	return hugeIntToNative(hugeInt)
}

func (vec *vector) getBytes(rowIdx mapping.IdxT) any {
	apiStr := getPrimitive[mapping.StringT](vec, rowIdx)
	str := mapping.StringTData(&apiStr)
	if vec.Type == TYPE_VARCHAR {
		return str
	}
	return []byte(str)
}

func (vec *vector) getJSON(rowIdx mapping.IdxT) any {
	bytes := vec.getBytes(rowIdx).(string)
	var value any
	_ = json.Unmarshal([]byte(bytes), &value)
	return value
}

func (vec *vector) getDecimal(rowIdx mapping.IdxT) Decimal {
	var val *big.Int
	switch vec.internalType {
	case TYPE_SMALLINT:
		v := getPrimitive[int16](vec, rowIdx)
		val = big.NewInt(int64(v))
	case TYPE_INTEGER:
		v := getPrimitive[int32](vec, rowIdx)
		val = big.NewInt(int64(v))
	case TYPE_BIGINT:
		v := getPrimitive[int64](vec, rowIdx)
		val = big.NewInt(v)
	case TYPE_HUGEINT:
		v := getPrimitive[mapping.HugeInt](vec, rowIdx)
		val = hugeIntToNative(v)
	}
	return Decimal{Width: vec.decimalWidth, Scale: vec.decimalScale, Value: val}
}

func (vec *vector) getEnum(rowIdx mapping.IdxT) string {
	var idx mapping.IdxT
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = mapping.IdxT(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = mapping.IdxT(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = mapping.IdxT(getPrimitive[uint32](vec, rowIdx))
	case TYPE_UBIGINT:
		idx = mapping.IdxT(getPrimitive[uint64](vec, rowIdx))
	}

	logicalType := mapping.VectorGetColumnType(vec.vec)
	defer mapping.DestroyLogicalType(&logicalType)
	return mapping.EnumDictionaryValue(logicalType, idx)
}

func (vec *vector) getList(rowIdx mapping.IdxT) []any {
	entry := getPrimitive[mapping.ListEntry](vec, rowIdx)
	offset := mapping.ListEntryGetOffset(&entry)
	length := mapping.ListEntryGetLength(&entry)
	return vec.getSliceChild(offset, length)
}

func (vec *vector) getStruct(rowIdx mapping.IdxT) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(vec.childVectors); i++ {
		child := &vec.childVectors[i]
		val := child.getFn(child, rowIdx)
		m[vec.structEntries[i].Name()] = val
	}
	return m
}

func (vec *vector) getMap(rowIdx mapping.IdxT) Map {
	list := vec.getList(rowIdx)

	m := Map{}
	for i := 0; i < len(list); i++ {
		mapItem := list[i].(map[string]any)
		key := mapItem[mapKeysField()]
		val := mapItem[mapValuesField()]
		m[key] = val
	}
	return m
}

func (vec *vector) getArray(rowIdx mapping.IdxT) []any {
	length := uint64(vec.arrayLength)
	return vec.getSliceChild(uint64(rowIdx)*length, length)
}

func (vec *vector) getSliceChild(offset uint64, length uint64) []any {
	slice := make([]any, 0, length)
	child := &vec.childVectors[0]

	// Fill the slice with all child values.
	for i := uint64(0); i < length; i++ {
		val := child.getFn(child, mapping.IdxT(i+offset))
		slice = append(slice, val)
	}
	return slice
}
