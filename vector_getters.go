package duckdb

import (
	"encoding/json"
	"math/big"
	"time"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx uint64) any

func (vec *vector) getNull(rowIdx uint64) bool {
	if vec.maskPtr == nil {
		return false
	}
	return !apiValidityMaskValueIsValid(vec.maskPtr, rowIdx)
}

func getPrimitive[T any](vec *vector, rowIdx uint64) T {
	xs := (*[1 << 31]T)(vec.dataPtr)
	return xs[rowIdx]
}

func (vec *vector) getTS(t Type, rowIdx uint64) time.Time {
	val := getPrimitive[apiTimestamp](vec, rowIdx)
	return getTS(t, val)
}

func getTS(t Type, ts apiTimestamp) time.Time {
	switch t {
	case TYPE_TIMESTAMP:
		return time.UnixMicro(apiTimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_S:
		return time.Unix(apiTimestampGetMicros(&ts), 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(apiTimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, apiTimestampGetMicros(&ts)).UTC()
	case TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(apiTimestampGetMicros(&ts)).UTC()
	}
	return time.Time{}
}

func (vec *vector) getDate(rowIdx uint64) time.Time {
	date := getPrimitive[apiDate](vec, rowIdx)
	return getDate(date)
}

func getDate(date apiDate) time.Time {
	d := apiFromDate(date)
	year := int(apiDateStructGetYear(&d))
	month := time.Month(apiDateStructGetMonth(&d))
	day := int(apiDateStructGetDay(&d))
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func (vec *vector) getTime(rowIdx uint64) time.Time {
	switch vec.Type {
	case TYPE_TIME:
		val := getPrimitive[apiTime](vec, rowIdx)
		return getTime(val)
	case TYPE_TIME_TZ:
		ti := getPrimitive[apiTimeTZ](vec, rowIdx)
		return getTimeTZ(ti)
	}
	return time.Time{}
}

func getTime(ti apiTime) time.Time {
	micros := apiTimeGetMicros(&ti)
	unix := time.UnixMicro(micros).UTC()
	return time.Date(1, time.January, 1, unix.Hour(), unix.Minute(), unix.Second(), unix.Nanosecond(), time.UTC)
}

func getTimeTZ(ti apiTimeTZ) time.Time {
	timeTZStruct := apiFromTimeTZ(ti)
	timeStruct := apiTimeTZStructGetTimeStruct(&timeTZStruct)
	offset := apiTimeTZStructGetOffset(&timeTZStruct)

	// TIMETZ has microsecond precision.
	nanos := int(apiTimeStructGetMicros(&timeStruct)) * 1000
	hour := int(apiTimeStructGetHour(&timeStruct))
	minute := int(apiTimeStructGetMinute(&timeStruct))
	sec := int(apiTimeStructGetSecond(&timeStruct))
	loc := time.FixedZone("", int(offset))
	return time.Date(1, time.January, 1, hour, minute, sec, nanos, loc).UTC()
}

func (vec *vector) getInterval(rowIdx uint64) Interval {
	interval := getPrimitive[apiInterval](vec, rowIdx)
	return getInterval(interval)
}

func getInterval(interval apiInterval) Interval {
	return Interval{
		Days:   apiIntervalGetDays(&interval),
		Months: apiIntervalGetMonths(&interval),
		Micros: apiIntervalGetMicros(&interval),
	}
}

func (vec *vector) getHugeint(rowIdx uint64) *big.Int {
	hugeInt := getPrimitive[apiHugeInt](vec, rowIdx)
	return hugeIntToNative(hugeInt)
}

func (vec *vector) getBytes(rowIdx uint64) any {
	apiStr := getPrimitive[apiStringT](vec, rowIdx)
	str := apiStringTData(&apiStr)
	if vec.Type == TYPE_VARCHAR {
		return str
	}
	return []byte(str)
}

func (vec *vector) getJSON(rowIdx uint64) any {
	bytes := vec.getBytes(rowIdx).(string)
	var value any
	_ = json.Unmarshal([]byte(bytes), &value)
	return value
}

func (vec *vector) getDecimal(rowIdx uint64) Decimal {
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
		v := getPrimitive[apiHugeInt](vec, rowIdx)
		val = hugeIntToNative(v)
	}
	return Decimal{Width: vec.decimalWidth, Scale: vec.decimalScale, Value: val}
}

func (vec *vector) getEnum(rowIdx uint64) string {
	var idx uint64
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = uint64(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = uint64(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = uint64(getPrimitive[uint32](vec, rowIdx))
	case TYPE_UBIGINT:
		idx = getPrimitive[uint64](vec, rowIdx)
	}

	logicalType := apiVectorGetColumnType(vec.vec)
	defer apiDestroyLogicalType(&logicalType)
	return apiEnumDictionaryValue(logicalType, idx)
}

func (vec *vector) getList(rowIdx uint64) []any {
	entry := getPrimitive[apiListEntry](vec, rowIdx)
	offset := apiListEntryGetOffset(&entry)
	length := apiListEntryGetLength(&entry)
	return vec.getSliceChild(offset, length)
}

func (vec *vector) getStruct(rowIdx uint64) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(vec.childVectors); i++ {
		child := &vec.childVectors[i]
		val := child.getFn(child, rowIdx)
		m[vec.structEntries[i].Name()] = val
	}
	return m
}

func (vec *vector) getMap(rowIdx uint64) Map {
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

func (vec *vector) getArray(rowIdx uint64) []any {
	length := vec.arrayLength
	return vec.getSliceChild(rowIdx*length, length)
}

func (vec *vector) getSliceChild(offset uint64, length uint64) []any {
	slice := make([]any, 0, length)
	child := &vec.childVectors[0]

	// Fill the slice with all child values.
	for i := uint64(0); i < length; i++ {
		val := child.getFn(child, i+offset)
		slice = append(slice, val)
	}
	return slice
}
