package duckdb

import (
	m "github.com/marcboeker/go-duckdb/mapping"
)

func getValue(info TypeInfo, v m.Value) (any, error) {
	t := info.InternalType()
	switch t {
	case TYPE_BOOLEAN:
		return m.GetBool(v), nil
	case TYPE_TINYINT:
		return m.GetInt8(v), nil
	case TYPE_SMALLINT:
		return m.GetInt16(v), nil
	case TYPE_INTEGER:
		return m.GetInt32(v), nil
	case TYPE_BIGINT:
		return m.GetInt64(v), nil
	case TYPE_UTINYINT:
		return m.GetUInt8(v), nil
	case TYPE_USMALLINT:
		return m.GetUInt16(v), nil
	case TYPE_UINTEGER:
		return m.GetUInt32(v), nil
	case TYPE_UBIGINT:
		return m.GetUInt64(v), nil
	case TYPE_FLOAT:
		return m.GetFloat(v), nil
	case TYPE_DOUBLE:
		return m.GetDouble(v), nil
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS:
		// DuckDB's C API does not yet support get_timestamp_s|ms|ns.
		return nil, unsupportedTypeError(typeToStringMap[t])
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := m.GetTimestamp(v)
		return getTS(t, ts), nil
	case TYPE_DATE:
		date := m.GetDate(v)
		return getDate(date), nil
	case TYPE_TIME:
		ti := m.GetTime(v)
		return getTime(ti), nil
	case TYPE_TIME_TZ:
		ti := m.GetTimeTZ(v)
		return getTimeTZ(ti), nil
	case TYPE_INTERVAL:
		interval := m.GetInterval(v)
		return getInterval(interval), nil
	case TYPE_HUGEINT:
		hugeInt := m.GetHugeInt(v)
		return hugeIntToNative(hugeInt), nil
	case TYPE_VARCHAR:
		return m.GetVarchar(v), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}
