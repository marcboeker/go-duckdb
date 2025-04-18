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

func createValue(lt mapping.LogicalType, v any) (*mapping.Value, error) {
	var vv mapping.Value
	var err error
	t := Type(mapping.GetTypeId(lt))
	switch t {
	case TYPE_BOOLEAN:
		vv, err = mapping.CreateBool(v.(bool)), nil
	case TYPE_TINYINT:
		vv, err = mapping.CreateInt8(v.(int8)), nil
	case TYPE_SMALLINT:
		vv, err = mapping.CreateInt16(v.(int16)), nil
	case TYPE_INTEGER:
		// TODO: do all int types need this casting?
		if i, ok := v.(int); ok {
			vv, err = mapping.CreateInt32(int32(i)), nil
		} else {
			vv, err = mapping.CreateInt32(v.(int32)), nil
		}
	case TYPE_BIGINT:
		vv, err = mapping.CreateInt64(v.(int64)), nil
	case TYPE_UTINYINT:
		vv, err = mapping.CreateUInt8(v.(uint8)), nil
	case TYPE_USMALLINT:
		vv, err = mapping.CreateUInt16(v.(uint16)), nil
	case TYPE_UINTEGER:
		vv, err = mapping.CreateUInt32(v.(uint32)), nil
	case TYPE_UBIGINT:
		vv, err = mapping.CreateUInt64(v.(uint64)), nil
	case TYPE_FLOAT:
		vv, err = mapping.CreateFloat(v.(float32)), nil
	case TYPE_DOUBLE:
		vv, err = mapping.CreateDouble(v.(float64)), nil
	case TYPE_VARCHAR:
		vv, err = mapping.CreateVarchar(v.(string)), nil
	case TYPE_ARRAY:
		return getMappedArrayValue(lt, v)
	case TYPE_LIST:
		return getMappedListValue(lt, v)
	case TYPE_STRUCT:
		return getMappedStructValue(lt, v)
	default:
		return nil, unsupportedTypeError(reflect.TypeOf(v).Name())
	}

	if err != nil {
		return nil, err
	}

	return &vv, err
}
