package duckdb

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"
)

// rows is a helper struct for scanning a duckdb result.
type rows struct {
	// stmt is a pointer to the stmt of which we are scanning the result.
	stmt *Stmt
	// res is the result of stmt.
	res apiResult
	// chunk holds the currently active data chunk.
	chunk DataChunk
	// closeChunk is true after the first iteration of Next.
	closeChunk bool
	// chunkCount is the number of chunks in the result.
	chunkCount apiIdxT
	// chunkIdx is the chunk index in the result.
	chunkIdx apiIdxT
	// rowCount is the number of scanned rows.
	rowCount int
}

func newRowsWithStmt(res apiResult, stmt *Stmt) *rows {
	columnCount := apiColumnCount(&res)
	r := rows{
		res:        res,
		stmt:       stmt,
		chunk:      DataChunk{},
		chunkCount: apiResultChunkCount(res),
		chunkIdx:   0,
		rowCount:   0,
	}

	for i := apiIdxT(0); i < columnCount; i++ {
		columnName := apiColumnName(&res, apiIdxT(i))
		r.chunk.columnNames = append(r.chunk.columnNames, columnName)
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
		apiChunk := apiResultGetChunk(r.res, r.chunkIdx)
		r.closeChunk = true
		if err := r.chunk.initFromDuckDataChunk(apiChunk, false); err != nil {
			return getError(err, nil)
		}

		r.chunkIdx++
		r.rowCount = 0
	}

	columnCount := len(r.chunk.columns)
	for colIdx := 0; colIdx < columnCount; colIdx++ {
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
	t := Type(apiColumnType(&r.res, apiIdxT(index)))
	switch t {
	case TYPE_INVALID:
		return nil
	case TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case TYPE_UTINYINT:
		return reflect.TypeOf(uint8(0))
	case TYPE_USMALLINT:
		return reflect.TypeOf(uint16(0))
	case TYPE_UINTEGER:
		return reflect.TypeOf(uint32(0))
	case TYPE_UBIGINT:
		return reflect.TypeOf(uint64(0))
	case TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_TIMESTAMP_TZ:
		return reflect.TypeOf(time.Time{})
	case TYPE_INTERVAL:
		return reflect.TypeOf(Interval{})
	case TYPE_HUGEINT:
		return reflect.TypeOf(big.NewInt(0))
	case TYPE_VARCHAR, TYPE_ENUM:
		return reflect.TypeOf("")
	case TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case TYPE_DECIMAL:
		return reflect.TypeOf(Decimal{})
	case TYPE_LIST:
		return reflect.TypeOf([]any{})
	case TYPE_STRUCT:
		return reflect.TypeOf(map[string]any{})
	case TYPE_MAP:
		return reflect.TypeOf(Map{})
	case TYPE_ARRAY:
		return reflect.TypeOf([]any{})
	case TYPE_UUID:
		return reflect.TypeOf([]byte{})
	default:
		return nil
	}
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	t := Type(apiColumnType(&r.res, apiIdxT(index)))
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY:
		// Only allocate the logical type if necessary.
		logicalType := apiColumnLogicalType(&r.res, apiIdxT(index))
		defer apiDestroyLogicalType(&logicalType)
		return logicalTypeName(logicalType)
	default:
		return typeToStringMap[t]
	}
}

func (r *rows) Close() error {
	if r.closeChunk {
		r.chunk.close()
	}
	apiDestroyResult(&r.res)

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

func logicalTypeName(logicalType apiLogicalType) string {
	t := Type(apiGetTypeId(logicalType))
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
	default:
		return typeToStringMap[t]
	}
}

func logicalTypeNameDecimal(logicalType apiLogicalType) string {
	width := apiDecimalWidth(logicalType)
	scale := apiDecimalScale(logicalType)
	return fmt.Sprintf("DECIMAL(%d,%d)", int(width), int(scale))
}

func logicalTypeNameList(logicalType apiLogicalType) string {
	childType := apiListTypeChildType(logicalType)
	defer apiDestroyLogicalType(&childType)
	childName := logicalTypeName(childType)
	return fmt.Sprintf("%s[]", childName)
}

func logicalTypeNameStruct(logicalType apiLogicalType) string {
	count := apiStructTypeChildCount(logicalType)
	name := "STRUCT("

	for i := apiIdxT(0); i < count; i++ {
		childName := apiStructTypeChildName(logicalType, i)
		childType := apiStructTypeChildType(logicalType, i)

		// Add comma if not at the end of the list.
		name += escapeStructFieldName(childName) + " " + logicalTypeName(childType)
		if i != count-1 {
			name += ", "
		}
		apiDestroyLogicalType(&childType)
	}
	return name + ")"
}

func logicalTypeNameMap(logicalType apiLogicalType) string {
	keyType := apiMapTypeKeyType(logicalType)
	defer apiDestroyLogicalType(&keyType)

	valueType := apiMapTypeValueType(logicalType)
	defer apiDestroyLogicalType(&valueType)

	return fmt.Sprintf("MAP(%s, %s)", logicalTypeName(keyType), logicalTypeName(valueType))
}

func logicalTypeNameArray(logicalType apiLogicalType) string {
	size := apiArrayTypeArraySize(logicalType)
	childType := apiArrayTypeChildType(logicalType)
	defer apiDestroyLogicalType(&childType)
	childName := logicalTypeName(childType)
	return fmt.Sprintf("%s[%d]", childName, int(size))
}

func escapeStructFieldName(s string) string {
	// DuckDB escapes STRUCT field names by doubling double quotes, then wrapping in double quotes.
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
