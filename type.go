package duckdb

import (
	"github.com/marcboeker/go-duckdb/mapping"
)

// Type wraps the corresponding DuckDB type enumapping.
type Type = mapping.Type

const (
	TYPE_INVALID      = mapping.TypeInvalid
	TYPE_BOOLEAN      = mapping.TypeBoolean
	TYPE_TINYINT      = mapping.TypeTinyInt
	TYPE_SMALLINT     = mapping.TypeSmallInt
	TYPE_INTEGER      = mapping.TypeInteger
	TYPE_BIGINT       = mapping.TypeBigInt
	TYPE_UTINYINT     = mapping.TypeUTinyInt
	TYPE_USMALLINT    = mapping.TypeUSmallInt
	TYPE_UINTEGER     = mapping.TypeUInteger
	TYPE_UBIGINT      = mapping.TypeUBigInt
	TYPE_FLOAT        = mapping.TypeFloat
	TYPE_DOUBLE       = mapping.TypeDouble
	TYPE_TIMESTAMP    = mapping.TypeTimestamp
	TYPE_DATE         = mapping.TypeDate
	TYPE_TIME         = mapping.TypeTime
	TYPE_INTERVAL     = mapping.TypeInterval
	TYPE_HUGEINT      = mapping.TypeHugeInt
	TYPE_UHUGEINT     = mapping.TypeUHugeInt
	TYPE_VARCHAR      = mapping.TypeVarchar
	TYPE_BLOB         = mapping.TypeBlob
	TYPE_DECIMAL      = mapping.TypeDecimal
	TYPE_TIMESTAMP_S  = mapping.TypeTimestampS
	TYPE_TIMESTAMP_MS = mapping.TypeTimestampMS
	TYPE_TIMESTAMP_NS = mapping.TypeTimestampNS
	TYPE_ENUM         = mapping.TypeEnum
	TYPE_LIST         = mapping.TypeList
	TYPE_STRUCT       = mapping.TypeStruct
	TYPE_MAP          = mapping.TypeMap
	TYPE_ARRAY        = mapping.TypeArray
	TYPE_UUID         = mapping.TypeUUID
	TYPE_UNION        = mapping.TypeUnion
	TYPE_BIT          = mapping.TypeBit
	TYPE_TIME_TZ      = mapping.TypeTimeTZ
	TYPE_TIMESTAMP_TZ = mapping.TypeTimestampTZ
	TYPE_ANY          = mapping.TypeAny
	TYPE_VARINT       = mapping.TypeVarInt
	TYPE_SQLNULL      = mapping.TypeSQLNull
)

// FIXME: Implement support for these types.
var unsupportedTypeToStringMap = map[Type]string{
	TYPE_INVALID:  "INVALID",
	TYPE_UHUGEINT: "UHUGEINT",
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
