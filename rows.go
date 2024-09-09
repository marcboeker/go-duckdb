package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

// rows is a helper struct for scanning a duckdb result.
type rows struct {
	// stmt is a pointer to the stmt of which we are scanning the result.
	stmt *stmt
	// res is the result of stmt.
	res C.duckdb_result
	// chunk holds the currently active data chunk.
	chunk DataChunk
	// chunkCount is the number of chunks in the result.
	chunkCount C.idx_t
	// chunkIdx is the chunk index in the result.
	chunkIdx C.idx_t
	// rowCount is the number of scanned rows.
	rowCount int
}

func newRowsWithStmt(res C.duckdb_result, stmt *stmt) *rows {
	columnCount := C.duckdb_column_count(&res)
	r := rows{
		res:        res,
		stmt:       stmt,
		chunk:      DataChunk{},
		chunkCount: C.duckdb_result_chunk_count(res),
		chunkIdx:   0,
		rowCount:   0,
	}

	for i := C.idx_t(0); i < columnCount; i++ {
		columnName := C.GoString(C.duckdb_column_name(&res, i))
		r.chunk.columnNames = append(r.chunk.columnNames, columnName)
	}
	return &r
}

func (r *rows) Columns() []string {
	return r.chunk.columnNames
}

func (r *rows) Next(dst []driver.Value) error {
	for r.rowCount == r.chunk.size {
		r.chunk.close()
		if r.chunkIdx == r.chunkCount {
			return io.EOF
		}
		data := C.duckdb_result_get_chunk(r.res, r.chunkIdx)
		if err := r.chunk.initFromDuckDataChunk(data, false); err != nil {
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

// Implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	colType := C.duckdb_column_type(&r.res, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_INVALID:
		return nil
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_UTINYINT:
		return reflect.TypeOf(uint8(0))
	case C.DUCKDB_TYPE_USMALLINT:
		return reflect.TypeOf(uint16(0))
	case C.DUCKDB_TYPE_UINTEGER:
		return reflect.TypeOf(uint32(0))
	case C.DUCKDB_TYPE_UBIGINT:
		return reflect.TypeOf(uint64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_DATE:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_TIME:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_INTERVAL:
		return reflect.TypeOf(Interval{})
	case C.DUCKDB_TYPE_HUGEINT:
		return reflect.TypeOf(big.NewInt(0))
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf("")
	case C.DUCKDB_TYPE_ENUM:
		return reflect.TypeOf("")
	case C.DUCKDB_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case C.DUCKDB_TYPE_DECIMAL:
		return reflect.TypeOf(Decimal{})
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_LIST:
		return reflect.TypeOf([]any{})
	case C.DUCKDB_TYPE_STRUCT:
		return reflect.TypeOf(map[string]any{})
	case C.DUCKDB_TYPE_MAP:
		return reflect.TypeOf(Map{})
	case C.DUCKDB_TYPE_UUID:
		return reflect.TypeOf([]byte{})
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return reflect.TypeOf(time.Time{})
	default:
		return nil
	}
}

// Implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	// Only allocate logical type if necessary
	colType := C.duckdb_column_type(&r.res, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_DECIMAL:
		fallthrough
	case C.DUCKDB_TYPE_ENUM:
		fallthrough
	case C.DUCKDB_TYPE_LIST:
		fallthrough
	case C.DUCKDB_TYPE_STRUCT:
		fallthrough
	case C.DUCKDB_TYPE_MAP:
		logColType := C.duckdb_column_logical_type(&r.res, C.idx_t(index))
		defer C.duckdb_destroy_logical_type(&logColType)
		return logicalTypeName(logColType)
	default:
		// Handle as primitive type
		return duckdbTypeMap[colType]
	}
}

func (r *rows) Close() error {
	r.chunk.close()
	C.duckdb_destroy_result(&r.res)

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

func logicalTypeName(lt C.duckdb_logical_type) string {
	t := C.duckdb_get_type_id(lt)
	switch t {
	case C.DUCKDB_TYPE_DECIMAL:
		width := C.duckdb_decimal_width(lt)
		scale := C.duckdb_decimal_scale(lt)
		return fmt.Sprintf("DECIMAL(%d,%d)", width, scale)
	case C.DUCKDB_TYPE_ENUM:
		// C API does not currently expose enum name
		return "ENUM"
	case C.DUCKDB_TYPE_LIST:
		clt := C.duckdb_list_type_child_type(lt)
		defer C.duckdb_destroy_logical_type(&clt)
		return logicalTypeName(clt) + "[]"
	case C.DUCKDB_TYPE_STRUCT:
		return logicalTypeNameStruct(lt)
	case C.DUCKDB_TYPE_MAP:
		return logicalTypeNameMap(lt)
	default:
		return duckdbTypeMap[t]
	}
}

func logicalTypeNameStruct(lt C.duckdb_logical_type) string {
	count := int(C.duckdb_struct_type_child_count(lt))
	name := "STRUCT("
	for i := 0; i < count; i++ {
		ptrToChildName := C.duckdb_struct_type_child_name(lt, C.idx_t(i))
		childName := C.GoString(ptrToChildName)
		childLogicalType := C.duckdb_struct_type_child_type(lt, C.idx_t(i))

		// Add comma if not at end of list
		name += escapeStructFieldName(childName) + " " + logicalTypeName(childLogicalType)
		if i != count-1 {
			name += ", "
		}

		C.duckdb_free(unsafe.Pointer(ptrToChildName))
		C.duckdb_destroy_logical_type(&childLogicalType)
	}
	return name + ")"
}

func logicalTypeNameMap(lt C.duckdb_logical_type) string {
	// Key logical type
	klt := C.duckdb_map_type_key_type(lt)
	defer C.duckdb_destroy_logical_type(&klt)

	// Value logical type
	vlt := C.duckdb_map_type_value_type(lt)
	defer C.duckdb_destroy_logical_type(&vlt)

	return fmt.Sprintf("MAP(%s, %s)", logicalTypeName(klt), logicalTypeName(vlt))
}

// DuckDB escapes struct field names by doubling double quotes, then wrapping in double quotes.
func escapeStructFieldName(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
