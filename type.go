package duckdb

import (
	m "github.com/marcboeker/go-duckdb/mapping"
)

// Type wraps the corresponding DuckDB type enum.
type Type = m.Type

const (
	TYPE_INVALID      = m.TypeInvalid
	TYPE_BOOLEAN      = m.TypeBoolean
	TYPE_TINYINT      = m.TypeTinyInt
	TYPE_SMALLINT     = m.TypeSmallInt
	TYPE_INTEGER      = m.TypeInteger
	TYPE_BIGINT       = m.TypeBigInt
	TYPE_UTINYINT     = m.TypeUTinyInt
	TYPE_USMALLINT    = m.TypeUSmallInt
	TYPE_UINTEGER     = m.TypeUInteger
	TYPE_UBIGINT      = m.TypeUBigInt
	TYPE_FLOAT        = m.TypeFloat
	TYPE_DOUBLE       = m.TypeDouble
	TYPE_TIMESTAMP    = m.TypeTimestamp
	TYPE_DATE         = m.TypeDate
	TYPE_TIME         = m.TypeTime
	TYPE_INTERVAL     = m.TypeInterval
	TYPE_HUGEINT      = m.TypeHugeInt
	TYPE_UHUGEINT     = m.TypeUHugeInt
	TYPE_VARCHAR      = m.TypeVarchar
	TYPE_BLOB         = m.TypeBlob
	TYPE_DECIMAL      = m.TypeDecimal
	TYPE_TIMESTAMP_S  = m.TypeTimestampS
	TYPE_TIMESTAMP_MS = m.TypeTimestampMS
	TYPE_TIMESTAMP_NS = m.TypeTimestampNS
	TYPE_ENUM         = m.TypeEnum
	TYPE_LIST         = m.TypeList
	TYPE_STRUCT       = m.TypeStruct
	TYPE_MAP          = m.TypeMap
	TYPE_ARRAY        = m.TypeArray
	TYPE_UUID         = m.TypeUUID
	TYPE_UNION        = m.TypeUnion
	TYPE_BIT          = m.TypeBit
	TYPE_TIME_TZ      = m.TypeTimeTZ
	TYPE_TIMESTAMP_TZ = m.TypeTimestampTZ
	TYPE_ANY          = m.TypeAny
	TYPE_VARINT       = m.TypeVarInt
	TYPE_SQLNULL      = m.TypeSQLNull
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
