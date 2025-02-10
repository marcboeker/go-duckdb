package duckdb

// Type wraps the corresponding DuckDB type enum.
type Type apiType

const (
	TYPE_INVALID      = Type(apiTypeInvalid)
	TYPE_BOOLEAN      = Type(apiTypeBoolean)
	TYPE_TINYINT      = Type(apiTypeTinyInt)
	TYPE_SMALLINT     = Type(apiTypeSmallInt)
	TYPE_INTEGER      = Type(apiTypeInteger)
	TYPE_BIGINT       = Type(apiTypeBigInt)
	TYPE_UTINYINT     = Type(apiTypeUTinyInt)
	TYPE_USMALLINT    = Type(apiTypeUSmallInt)
	TYPE_UINTEGER     = Type(apiTypeUInteger)
	TYPE_UBIGINT      = Type(apiTypeUBigInt)
	TYPE_FLOAT        = Type(apiTypeFloat)
	TYPE_DOUBLE       = Type(apiTypeDouble)
	TYPE_TIMESTAMP    = Type(apiTypeTimestamp)
	TYPE_DATE         = Type(apiTypeDate)
	TYPE_TIME         = Type(apiTypeTime)
	TYPE_INTERVAL     = Type(apiTypeInterval)
	TYPE_HUGEINT      = Type(apiTypeHugeInt)
	TYPE_UHUGEINT     = Type(apiTypeUHugeInt)
	TYPE_VARCHAR      = Type(apiTypeVarchar)
	TYPE_BLOB         = Type(apiTypeBlob)
	TYPE_DECIMAL      = Type(apiTypeDecimal)
	TYPE_TIMESTAMP_S  = Type(apiTypeTimestampS)
	TYPE_TIMESTAMP_MS = Type(apiTypeTimestampMS)
	TYPE_TIMESTAMP_NS = Type(apiTypeTimestampNS)
	TYPE_ENUM         = Type(apiTypeEnum)
	TYPE_LIST         = Type(apiTypeList)
	TYPE_STRUCT       = Type(apiTypeStruct)
	TYPE_MAP          = Type(apiTypeMap)
	TYPE_ARRAY        = Type(apiTypeArray)
	TYPE_UUID         = Type(apiTypeUUID)
	TYPE_UNION        = Type(apiTypeUnion)
	TYPE_BIT          = Type(apiTypeBit)
	TYPE_TIME_TZ      = Type(apiTypeTimeTZ)
	TYPE_TIMESTAMP_TZ = Type(apiTypeTimestampTZ)
	TYPE_ANY          = Type(apiTypeAny)
	TYPE_VARINT       = Type(apiTypeVarInt)
	TYPE_SQLNULL      = Type(apiTypeSQLNull)
)

// FIXME: Implement support for these types.
var unsupportedTypeToStringMap = map[Type]string{
	TYPE_INVALID:  "INVALID",
	TYPE_UHUGEINT: "UHUGEINT",
	TYPE_UNION:    "UNION",
	TYPE_BIT:      "BIT",
	TYPE_ANY:      "ANY",
	TYPE_VARINT:   "VARINT",
}

var typeToStringMap = map[Type]string{
	TYPE_INVALID:      "INVALID",
	TYPE_BOOLEAN:      "BOOLEAN",
	TYPE_TINYINT:      "TINYINT",
	TYPE_SMALLINT:     "SMALLINT",
	TYPE_INTEGER:      "INTEGER",
	TYPE_BIGINT:       "BIGINT",
	TYPE_UTINYINT:     "UTINYINT",
	TYPE_USMALLINT:    "USMALLINT",
	TYPE_UINTEGER:     "UINTEGER",
	TYPE_UBIGINT:      "UBIGINT",
	TYPE_FLOAT:        "FLOAT",
	TYPE_DOUBLE:       "DOUBLE",
	TYPE_TIMESTAMP:    "TIMESTAMP",
	TYPE_DATE:         "DATE",
	TYPE_TIME:         "TIME",
	TYPE_INTERVAL:     "INTERVAL",
	TYPE_HUGEINT:      "HUGEINT",
	TYPE_UHUGEINT:     "UHUGEINT",
	TYPE_VARCHAR:      "VARCHAR",
	TYPE_BLOB:         "BLOB",
	TYPE_DECIMAL:      "DECIMAL",
	TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	TYPE_ENUM:         "ENUM",
	TYPE_LIST:         "LIST",
	TYPE_STRUCT:       "STRUCT",
	TYPE_MAP:          "MAP",
	TYPE_ARRAY:        "ARRAY",
	TYPE_UUID:         "UUID",
	TYPE_UNION:        "UNION",
	TYPE_BIT:          "BIT",
	TYPE_TIME_TZ:      "TIMETZ",
	TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
	TYPE_ANY:          "ANY",
	TYPE_VARINT:       "VARINT",
	TYPE_SQLNULL:      "SQLNULL",
}

const aliasJSON = "JSON"
