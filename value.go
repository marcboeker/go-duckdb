package duckdb

func getValue(info TypeInfo, v apiValue) (any, error) {
	t := info.InternalType()
	switch t {
	case TYPE_BOOLEAN:
		return apiGetBool(v), nil
	case TYPE_TINYINT:
		return apiGetInt8(v), nil
	case TYPE_SMALLINT:
		return apiGetInt16(v), nil
	case TYPE_INTEGER:
		return apiGetInt32(v), nil
	case TYPE_BIGINT:
		return apiGetInt64(v), nil
	case TYPE_UTINYINT:
		return apiGetUInt8(v), nil
	case TYPE_USMALLINT:
		return apiGetUInt16(v), nil
	case TYPE_UINTEGER:
		return apiGetUInt32(v), nil
	case TYPE_UBIGINT:
		return apiGetUInt64(v), nil
	case TYPE_FLOAT:
		return apiGetFloat(v), nil
	case TYPE_DOUBLE:
		return apiGetDouble(v), nil
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS:
		// DuckDB's C API does not yet support get_timestamp_s|ms|ns.
		return nil, unsupportedTypeError(typeToStringMap[t])
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := apiGetTimestamp(v)
		return getTS(t, ts), nil
	case TYPE_DATE:
		date := apiGetDate(v)
		return getDate(date), nil
	case TYPE_TIME:
		ti := apiGetTime(v)
		return getTime(ti), nil
	case TYPE_TIME_TZ:
		ti := apiGetTimeTZ(v)
		return getTimeTZ(ti), nil
	case TYPE_INTERVAL:
		interval := apiGetInterval(v)
		return getInterval(interval), nil
	case TYPE_HUGEINT:
		hugeInt := apiGetHugeInt(v)
		return hugeIntToNative(hugeInt), nil
	case TYPE_VARCHAR:
		return apiGetVarchar(v), nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}
