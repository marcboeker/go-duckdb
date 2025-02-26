package duckdb

// Type wraps the corresponding DuckDB type enum.
type Type = apiType

const (
	TYPE_INVALID      = apiTypeInvalid
	TYPE_BOOLEAN      = apiTypeBoolean
	TYPE_TINYINT      = apiTypeTinyInt
	TYPE_SMALLINT     = apiTypeSmallInt
	TYPE_INTEGER      = apiTypeInteger
	TYPE_BIGINT       = apiTypeBigInt
	TYPE_UTINYINT     = apiTypeUTinyInt
	TYPE_USMALLINT    = apiTypeUSmallInt
	TYPE_UINTEGER     = apiTypeUInteger
	TYPE_UBIGINT      = apiTypeUBigInt
	TYPE_FLOAT        = apiTypeFloat
	TYPE_DOUBLE       = apiTypeDouble
	TYPE_TIMESTAMP    = apiTypeTimestamp
	TYPE_DATE         = apiTypeDate
	TYPE_TIME         = apiTypeTime
	TYPE_INTERVAL     = apiTypeInterval
	TYPE_HUGEINT      = apiTypeHugeInt
	TYPE_UHUGEINT     = apiTypeUHugeInt
	TYPE_VARCHAR      = apiTypeVarchar
	TYPE_BLOB         = apiTypeBlob
	TYPE_DECIMAL      = apiTypeDecimal
	TYPE_TIMESTAMP_S  = apiTypeTimestampS
	TYPE_TIMESTAMP_MS = apiTypeTimestampMS
	TYPE_TIMESTAMP_NS = apiTypeTimestampNS
	TYPE_ENUM         = apiTypeEnum
	TYPE_LIST         = apiTypeList
	TYPE_STRUCT       = apiTypeStruct
	TYPE_MAP          = apiTypeMap
	TYPE_ARRAY        = apiTypeArray
	TYPE_UUID         = apiTypeUUID
	TYPE_UNION        = apiTypeUnion
	TYPE_BIT          = apiTypeBit
	TYPE_TIME_TZ      = apiTypeTimeTZ
	TYPE_TIMESTAMP_TZ = apiTypeTimestampTZ
	TYPE_ANY          = apiTypeAny
	TYPE_VARINT       = apiTypeVarInt
	TYPE_SQLNULL      = apiTypeSQLNull
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
