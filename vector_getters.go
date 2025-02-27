package duckdb

import (
	"encoding/json"
	"math/big"
	"time"

	m "github.com/marcboeker/go-duckdb/mapping"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx m.IdxT) any

func (vec *vector) getNull(rowIdx m.IdxT) bool {
	if vec.maskPtr == nil {
		return false
	}
	return !m.ValidityMaskValueIsValid(vec.maskPtr, rowIdx)
}

func getPrimitive[T any](vec *vector, rowIdx m.IdxT) T {
	xs := (*[1 << 31]T)(vec.dataPtr)
	return xs[rowIdx]
}

func (vec *vector) getTS(t Type, rowIdx m.IdxT) time.Time {
	val := getPrimitive[m.Timestamp](vec, rowIdx)
	return getTS(t, val)
}

func getTS(t Type, ts m.Timestamp) time.Time {
	switch t {
	case TYPE_TIMESTAMP:
		return time.UnixMicro(m.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_S:
		return time.Unix(m.TimestampGetMicros(&ts), 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(m.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, m.TimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(m.TimestampGetMicros(&ts)).UTC()
	}
	return time.Time{}
}

func (vec *vector) getDate(rowIdx m.IdxT) time.Time {
	date := getPrimitive[m.Date](vec, rowIdx)
	return getDate(date)
}

func getDate(date m.Date) time.Time {
	d := m.FromDate(date)
	year := int(m.DateStructGetYear(&d))
	month := time.Month(m.DateStructGetMonth(&d))
	day := int(m.DateStructGetDay(&d))
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func (vec *vector) getTime(rowIdx m.IdxT) time.Time {
	switch vec.Type {
	case TYPE_TIME:
		val := getPrimitive[m.Time](vec, rowIdx)
		return getTime(val)
	case TYPE_TIME_TZ:
		ti := getPrimitive[m.TimeTZ](vec, rowIdx)
		return getTimeTZ(ti)
	}
	return time.Time{}
}

func getTime(ti m.Time) time.Time {
	micros := m.TimeGetMicros(&ti)
	unix := time.UnixMicro(micros).UTC()
	return time.Date(1, time.January, 1, unix.Hour(), unix.Minute(), unix.Second(), unix.Nanosecond(), time.UTC)
}

func getTimeTZ(ti m.TimeTZ) time.Time {
	timeTZStruct := m.FromTimeTZ(ti)
	timeStruct := m.TimeTZStructGetTimeStruct(&timeTZStruct)
	offset := m.TimeTZStructGetOffset(&timeTZStruct)

	// TIMETZ has microsecond precision.
	nanos := int(m.TimeStructGetMicros(&timeStruct)) * 1000
	hour := int(m.TimeStructGetHour(&timeStruct))
	minute := int(m.TimeStructGetMinute(&timeStruct))
	sec := int(m.TimeStructGetSecond(&timeStruct))
	loc := time.FixedZone("", int(offset))
	return time.Date(1, time.January, 1, hour, minute, sec, nanos, loc).UTC()
}

func (vec *vector) getInterval(rowIdx m.IdxT) Interval {
	interval := getPrimitive[m.Interval](vec, rowIdx)
	return getInterval(interval)
}

func getInterval(interval m.Interval) Interval {
	return Interval{
		Days:   m.IntervalGetDays(&interval),
		Months: m.IntervalGetMonths(&interval),
		Micros: m.IntervalGetMicros(&interval),
	}
}

func (vec *vector) getHugeint(rowIdx m.IdxT) *big.Int {
	hugeInt := getPrimitive[m.HugeInt](vec, rowIdx)
	return hugeIntToNative(hugeInt)
}

func (vec *vector) getBytes(rowIdx m.IdxT) any {
	apiStr := getPrimitive[m.StringT](vec, rowIdx)
	str := m.StringTData(&apiStr)
	if vec.Type == TYPE_VARCHAR {
		return str
	}
	return []byte(str)
}

func (vec *vector) getJSON(rowIdx m.IdxT) any {
	bytes := vec.getBytes(rowIdx).(string)
	var value any
	_ = json.Unmarshal([]byte(bytes), &value)
	return value
}

func (vec *vector) getDecimal(rowIdx m.IdxT) Decimal {
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
		v := getPrimitive[m.HugeInt](vec, rowIdx)
		val = hugeIntToNative(v)
	}
	return Decimal{Width: vec.decimalWidth, Scale: vec.decimalScale, Value: val}
}

func (vec *vector) getEnum(rowIdx m.IdxT) string {
	var idx m.IdxT
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = m.IdxT(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = m.IdxT(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = m.IdxT(getPrimitive[uint32](vec, rowIdx))
	case TYPE_UBIGINT:
		idx = m.IdxT(getPrimitive[uint64](vec, rowIdx))
	}

	logicalType := m.VectorGetColumnType(vec.vec)
	defer m.DestroyLogicalType(&logicalType)
	return m.EnumDictionaryValue(logicalType, idx)
}

func (vec *vector) getList(rowIdx m.IdxT) []any {
	entry := getPrimitive[m.ListEntry](vec, rowIdx)
	offset := m.ListEntryGetOffset(&entry)
	length := m.ListEntryGetLength(&entry)
	return vec.getSliceChild(offset, length)
}

func (vec *vector) getStruct(rowIdx m.IdxT) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(vec.childVectors); i++ {
		child := &vec.childVectors[i]
		val := child.getFn(child, rowIdx)
		m[vec.structEntries[i].Name()] = val
	}
	return m
}

func (vec *vector) getMap(rowIdx m.IdxT) Map {
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

func (vec *vector) getArray(rowIdx m.IdxT) []any {
	length := uint64(vec.arrayLength)
	return vec.getSliceChild(uint64(rowIdx)*length, length)
}

func (vec *vector) getSliceChild(offset uint64, length uint64) []any {
	slice := make([]any, 0, length)
	child := &vec.childVectors[0]

	// Fill the slice with all child values.
	for i := uint64(0); i < length; i++ {
		val := child.getFn(child, m.IdxT(i+offset))
		slice = append(slice, val)
	}
	return slice
}
