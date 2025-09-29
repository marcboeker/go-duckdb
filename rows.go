package duckdb

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/marcboeker/go-duckdb/mapping"
)

// rows is a helper struct for scanning a duckdb result.
type rows struct {
	// stmt is a pointer to the stmt of which we are scanning the result.
	stmt *Stmt
	// res is the result of stmt.
	res mapping.Result
	// chunk holds the currently active data chunk.
	chunk DataChunk
	// closeChunk is true after the first iteration of Next.
	closeChunk bool
	// chunkCount is the number of chunks in the result.
	chunkCount mapping.IdxT
	// chunkIdx is the chunk index in the result.
	chunkIdx mapping.IdxT
	// rowCount is the number of scanned rows.
	rowCount int
	// cached column metadata to avoid repeated CGO calls
	scanTypes   []reflect.Type
	dbTypeNames []string
}

func newRowsWithStmt(res mapping.Result, stmt *Stmt) *rows {
	columnCount := mapping.ColumnCount(&res)
	r := rows{
		res:         res,
		stmt:        stmt,
		chunk:       DataChunk{},
		chunkCount:  mapping.ResultChunkCount(res),
		chunkIdx:    0,
		rowCount:    0,
		scanTypes:   make([]reflect.Type, columnCount),
		dbTypeNames: make([]string, columnCount),
	}

	for i := mapping.IdxT(0); i < columnCount; i++ {
		columnName := mapping.ColumnName(&res, i)
		r.chunk.columnNames = append(r.chunk.columnNames, columnName)

		// Cache column metadata
		logicalType := mapping.ColumnLogicalType(&res, i)
		r.scanTypes[i] = r.getScanType(logicalType, i)
		r.dbTypeNames[i] = logicalTypeString(logicalType)
		mapping.DestroyLogicalType(&logicalType)
	}

	return &r
}

func (r *rows) Columns() []string {
	return r.chunk.columnNames
}

func (r *rows) Next(dst []driver.Value) error {
	for r.rowCount == r.chunk.size {
		if r.closeChunk {
			r.chunk.close()
			r.closeChunk = false
		}
		if r.chunkIdx == r.chunkCount {
			return io.EOF
		}
		chunk := mapping.ResultGetChunk(r.res, r.chunkIdx)
		r.closeChunk = true
		if err := r.chunk.initFromDuckDataChunk(chunk, false); err != nil {
			return getError(err, nil)
		}

		r.chunkIdx++
		r.rowCount = 0
	}

	columnCount := len(r.chunk.columns)
	for colIdx := range columnCount {
		var err error
		if dst[colIdx], err = r.chunk.GetValue(colIdx, r.rowCount); err != nil {
			return err
		}
	}
	r.rowCount++

	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return r.scanTypes[index]
}

func (r *rows) getScanType(logicalType mapping.LogicalType, index mapping.IdxT) reflect.Type {
	alias := mapping.LogicalTypeGetAlias(logicalType)
	if alias == aliasJSON {
		return reflectTypeAny
	}

	t := mapping.ColumnType(&r.res, index)
	switch t {
	case TYPE_INVALID:
		return nil
	case TYPE_BOOLEAN:
		return reflectTypeBool
	case TYPE_TINYINT:
		return reflectTypeInt8
	case TYPE_SMALLINT:
		return reflectTypeInt16
	case TYPE_INTEGER:
		return reflectTypeInt32
	case TYPE_BIGINT:
		return reflectTypeInt64
	case TYPE_UTINYINT:
		return reflectTypeUint8
	case TYPE_USMALLINT:
		return reflectTypeUint16
	case TYPE_UINTEGER:
		return reflectTypeUint32
	case TYPE_UBIGINT:
		return reflectTypeUint64
	case TYPE_FLOAT:
		return reflectTypeFloat32
	case TYPE_DOUBLE:
		return reflectTypeFloat64
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_TIMESTAMP_TZ:
		return reflectTypeTime
	case TYPE_INTERVAL:
		return reflectTypeInterval
	case TYPE_HUGEINT:
		return reflectTypeBigInt
	case TYPE_VARCHAR, TYPE_ENUM:
		return reflectTypeString
	case TYPE_BLOB:
		return reflectTypeBytes
	case TYPE_DECIMAL:
		return reflectTypeDecimal
	case TYPE_LIST:
		return reflectTypeSliceAny
	case TYPE_STRUCT:
		return reflectTypeMapString
	case TYPE_MAP:
		return reflectTypeMap
	case TYPE_ARRAY:
		return reflectTypeSliceAny
	case TYPE_UNION:
		return reflectTypeUnion
	case TYPE_UUID:
		return reflectTypeBytes
	default:
		return nil
	}
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.dbTypeNames[index]
}

func (r *rows) Close() error {
	if r.closeChunk {
		r.chunk.close()
	}
	mapping.DestroyResult(&r.res)

	var err error
	if r.stmt != nil {
		r.stmt.rows = false
		if r.stmt.closeOnRowsClose {
			err = r.stmt.Close()
		}
		r.stmt = nil
	}

	return err
}

// logicalTypeString converts a LogicalType to its string representation.
func logicalTypeString(logicalType mapping.LogicalType) string {
	alias := mapping.LogicalTypeGetAlias(logicalType)
	if alias == aliasJSON {
		return aliasJSON
	}

	t := mapping.GetTypeId(logicalType)
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION:
		return logicalTypeName(logicalType)
	default:
		return typeToStringMap[t]
	}
}

func logicalTypeName(logicalType mapping.LogicalType) string {
	t := mapping.GetTypeId(logicalType)
	switch t {
	case TYPE_DECIMAL:
		return logicalTypeNameDecimal(logicalType)
	case TYPE_ENUM:
		// The C API does not expose ENUM names.
		return "ENUM"
	case TYPE_LIST:
		return logicalTypeNameList(logicalType)
	case TYPE_STRUCT:
		return logicalTypeNameStruct(logicalType)
	case TYPE_MAP:
		return logicalTypeNameMap(logicalType)
	case TYPE_ARRAY:
		return logicalTypeNameArray(logicalType)
	case TYPE_UNION:
		return logicalTypeNameUnion(logicalType)
	default:
		return typeToStringMap[t]
	}
}

func logicalTypeNameDecimal(logicalType mapping.LogicalType) string {
	width := mapping.DecimalWidth(logicalType)
	scale := mapping.DecimalScale(logicalType)
	return fmt.Sprintf("DECIMAL(%d,%d)", int(width), int(scale))
}

func logicalTypeNameList(logicalType mapping.LogicalType) string {
	childType := mapping.ListTypeChildType(logicalType)
	defer mapping.DestroyLogicalType(&childType)
	childName := logicalTypeName(childType)
	return fmt.Sprintf("%s[]", childName)
}

func logicalTypeNameStruct(logicalType mapping.LogicalType) string {
	var sb strings.Builder
	sb.WriteString("STRUCT(")

	count := mapping.StructTypeChildCount(logicalType)

	for i := mapping.IdxT(0); i < count; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}

		childName := mapping.StructTypeChildName(logicalType, i)
		childType := mapping.StructTypeChildType(logicalType, i)

		sb.WriteString(escapeStructFieldName(childName))
		sb.WriteByte(' ')
		sb.WriteString(logicalTypeName(childType))

		mapping.DestroyLogicalType(&childType)
	}

	sb.WriteByte(')')

	return sb.String()
}

func logicalTypeNameMap(logicalType mapping.LogicalType) string {
	keyType := mapping.MapTypeKeyType(logicalType)
	defer mapping.DestroyLogicalType(&keyType)

	valueType := mapping.MapTypeValueType(logicalType)
	defer mapping.DestroyLogicalType(&valueType)

	return fmt.Sprintf("MAP(%s, %s)", logicalTypeName(keyType), logicalTypeName(valueType))
}

func logicalTypeNameArray(logicalType mapping.LogicalType) string {
	size := mapping.ArrayTypeArraySize(logicalType)
	childType := mapping.ArrayTypeChildType(logicalType)
	defer mapping.DestroyLogicalType(&childType)
	childName := logicalTypeName(childType)

	return fmt.Sprintf("%s[%d]", childName, int(size))
}

func logicalTypeNameUnion(logicalType mapping.LogicalType) string {
	var sb strings.Builder
	sb.WriteString("UNION(")

	count := int(mapping.UnionTypeMemberCount(logicalType))

	for i := range count {
		if i > 0 {
			sb.WriteString(", ")
		}

		memberName := mapping.UnionTypeMemberName(logicalType, mapping.IdxT(i))
		memberType := mapping.UnionTypeMemberType(logicalType, mapping.IdxT(i))

		sb.WriteString(escapeStructFieldName(memberName))
		sb.WriteByte(' ')
		sb.WriteString(logicalTypeName(memberType))

		mapping.DestroyLogicalType(&memberType)
	}

	sb.WriteByte(')')
	return sb.String()
}

func escapeStructFieldName(s string) string {
	// DuckDB escapes STRUCT field names by doubling double quotes, then wrapping in double quotes.
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
