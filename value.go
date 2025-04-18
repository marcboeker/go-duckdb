package duckdb

import (
	"reflect"

	"github.com/marcboeker/go-duckdb/mapping"
)

func getValue(info TypeInfo, v mapping.Value) (any, error) {
	t := info.InternalType()
	switch t {
	case TYPE_BOOLEAN:
		return mapping.GetBool(v), nil
	case TYPE_TINYINT:
		return mapping.GetInt8(v), nil
	case TYPE_SMALLINT:
		return mapping.GetInt16(v), nil
	case TYPE_INTEGER:
		return mapping.GetInt32(v), nil
	case TYPE_BIGINT:
		return mapping.GetInt64(v), nil
	case TYPE_UTINYINT:
		return mapping.GetUInt8(v), nil
	case TYPE_USMALLINT:
		return mapping.GetUInt16(v), nil
	case TYPE_UINTEGER:
		return mapping.GetUInt32(v), nil
	case TYPE_UBIGINT:
		return mapping.GetUInt64(v), nil
	case TYPE_FLOAT:
		return mapping.GetFloat(v), nil
	case TYPE_DOUBLE:
		return mapping.GetDouble(v), nil
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS:
		// DuckDB's C API does not yet support get_timestamp_s|ms|ns.
		return nil, unsupportedTypeError(typeToStringMap[t])
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := mapping.GetTimestamp(v)
		return getTS(t, &ts), nil
	case TYPE_DATE:
		date := mapping.GetDate(v)
		return getDate(&date), nil
	case TYPE_TIME:
		ti := mapping.GetTime(v)
		return getTime(&ti), nil
	case TYPE_TIME_TZ:
		ti := mapping.GetTimeTZ(v)
		return getTimeTZ(&ti), nil
	case TYPE_INTERVAL:
		interval := mapping.GetInterval(v)
		return getInterval(&interval), nil
	case TYPE_HUGEINT:
		hugeInt := mapping.GetHugeInt(v)
		return hugeIntToNative(&hugeInt), nil
	case TYPE_VARCHAR:
		return mapping.GetVarchar(v), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}

func createValue(lt mapping.LogicalType, v any) (mapping.Value, error) {
	t := Type(mapping.GetTypeId(lt))
	switch t {
	case TYPE_BOOLEAN:
		return mapping.CreateBool(v.(bool)), nil
	case TYPE_TINYINT:
		return mapping.CreateInt8(v.(int8)), nil
	case TYPE_SMALLINT:
		return mapping.CreateInt16(v.(int16)), nil
	case TYPE_INTEGER:
		return mapping.CreateInt32(v.(int32)), nil
	case TYPE_BIGINT:
		return mapping.CreateInt64(v.(int64)), nil
	case TYPE_UTINYINT:
		return mapping.CreateUInt8(v.(uint8)), nil
	case TYPE_USMALLINT:
		return mapping.CreateUInt16(v.(uint16)), nil
	case TYPE_UINTEGER:
		return mapping.CreateUInt32(v.(uint32)), nil
	case TYPE_UBIGINT:
		return mapping.CreateUInt64(v.(uint64)), nil
	case TYPE_FLOAT:
		return mapping.CreateFloat(v.(float32)), nil
	case TYPE_DOUBLE:
		return mapping.CreateDouble(v.(float64)), nil
	case TYPE_VARCHAR:
		return mapping.CreateVarchar(v.(string)), nil
	}

	var mv mapping.Value
	return mv, unsupportedTypeError(reflect.TypeOf(v).Name())
}
