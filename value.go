package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"time"
	"unsafe"
)

func getValue(t TypeInfo, v C.duckdb_value) (any, error) {
	switch t.(*typeInfo).Type {
	case TYPE_BOOLEAN:
		return bool(C.duckdb_get_bool(v)), nil
	case TYPE_TINYINT:
		return int8(C.duckdb_get_int8(v)), nil
	case TYPE_SMALLINT:
		return int16(C.duckdb_get_int16(v)), nil
	case TYPE_INTEGER:
		return int32(C.duckdb_get_int32(v)), nil
	case TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
	case TYPE_UTINYINT:
		return uint8(C.duckdb_get_uint8(v)), nil
	case TYPE_USMALLINT:
		return uint16(C.duckdb_get_uint16(v)), nil
	case TYPE_UINTEGER:
		return uint32(C.duckdb_get_uint32(v)), nil
	case TYPE_UBIGINT:
		return uint64(C.duckdb_get_uint64(v)), nil
	case TYPE_FLOAT:
		return float32(C.duckdb_get_float(v)), nil
	case TYPE_DOUBLE:
		return float64(C.duckdb_get_double(v)), nil
	case TYPE_TIMESTAMP:
		val := C.duckdb_get_timestamp(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	case TYPE_DATE:
		primitiveDate := C.duckdb_get_date(v)
		date := C.duckdb_from_date(primitiveDate)
		return time.Date(int(date.year), time.Month(date.month), int(date.day), 0, 0, 0, 0, time.UTC), nil
	case TYPE_TIME:
		val := C.duckdb_get_time(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	case TYPE_INTERVAL:
		interval := C.duckdb_get_interval(v)
		return Interval{
			Days:   int32(interval.days),
			Months: int32(interval.months),
			Micros: int64(interval.micros),
		}, nil
	case TYPE_HUGEINT:
		hugeint := C.duckdb_get_hugeint(v)
		return hugeIntToNative(hugeint), nil
	case TYPE_VARCHAR:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	case TYPE_TIMESTAMP_S:
		val := C.duckdb_get_timestamp(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	case TYPE_TIMESTAMP_MS:
		val := C.duckdb_get_timestamp(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	case TYPE_TIMESTAMP_NS:
		val := C.duckdb_get_timestamp(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	case TYPE_TIMESTAMP_TZ:
		val := C.duckdb_get_timestamp(v)
		return time.UnixMicro(int64(val.micros)).UTC(), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t.InternalType()])
	}
}
