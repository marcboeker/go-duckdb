package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
	"unsafe"
)

type rows struct {
	res           C.duckdb_result
	stmt          *stmt
	chunk         C.duckdb_data_chunk
	columns       []string
	chunkCount    C.idx_t
	chunkRowCount C.idx_t
	chunkIdx      C.idx_t
	chunkRowIdx   C.idx_t
}

func newRows(res C.duckdb_result) *rows {
	return newRowsWithStmt(res, nil)
}

func newRowsWithStmt(res C.duckdb_result, stmt *stmt) *rows {
	n := C.duckdb_column_count(&res)
	columns := make([]string, 0, n)
	for i := C.idx_t(0); i < n; i++ {
		columns = append(columns, C.GoString(C.duckdb_column_name(&res, i)))
	}

	return &rows{
		res:           res,
		stmt:          stmt,
		columns:       columns,
		chunkCount:    C.duckdb_result_chunk_count(res),
		chunkRowCount: 0,
		chunkIdx:      0,
		chunkRowIdx:   0,
	}
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(dst []driver.Value) error {
	for r.chunkRowIdx == r.chunkRowCount {
		if r.chunkIdx == r.chunkCount {
			return io.EOF
		}
		r.chunk = C.duckdb_result_get_chunk(r.res, r.chunkIdx)
		r.chunkIdx++
		r.chunkRowCount = C.duckdb_data_chunk_get_size(r.chunk)
		r.chunkRowIdx = 0
	}

	colCount := len(r.columns)

	for colIdx := C.idx_t(0); colIdx < C.idx_t(colCount); colIdx++ {
		vector := C.duckdb_data_chunk_get_vector(r.chunk, colIdx)
		value, err := scanValue(vector, r.chunkRowIdx)
		if err != nil {
			return err
		}
		dst[colIdx] = value
	}

	r.chunkRowIdx++

	return nil
}

func scanValue(vector C.duckdb_vector, rowIdx C.idx_t) (any, error) {
	v, err := scan(vector, rowIdx)
	if err != nil {
		return nil, err
	}

	switch value := v.(type) {
	case map[string]any, []any, Map, driver.Value:
		return value, nil
	case nil:
		return nil, nil
	default:
		panic(fmt.Sprintf("BUG: found unexpected type when scanning: %T", value))
	}
}

func scan(vector C.duckdb_vector, rowIdx C.idx_t) (any, error) {
	validity := C.duckdb_vector_get_validity(vector)
	if !C.duckdb_validity_row_is_valid(validity, rowIdx) {
		return nil, nil
	}

	ty := C.duckdb_vector_get_column_type(vector)
	typeId := C.duckdb_get_type_id(ty)
	switch typeId {
	case C.DUCKDB_TYPE_INVALID:
		return nil, errInvalidType
	case C.DUCKDB_TYPE_BOOLEAN:
		return get[bool](vector, rowIdx), nil
	case C.DUCKDB_TYPE_TINYINT:
		return get[int8](vector, rowIdx), nil
	case C.DUCKDB_TYPE_SMALLINT:
		return get[int16](vector, rowIdx), nil
	case C.DUCKDB_TYPE_INTEGER:
		return get[int32](vector, rowIdx), nil
	case C.DUCKDB_TYPE_BIGINT:
		return get[int64](vector, rowIdx), nil
	case C.DUCKDB_TYPE_HUGEINT:
		return get[HugeInt](vector, rowIdx), nil
	case C.DUCKDB_TYPE_FLOAT:
		return get[float32](vector, rowIdx), nil
	case C.DUCKDB_TYPE_DOUBLE:
		return get[float64](vector, rowIdx), nil
	case C.DUCKDB_TYPE_DATE:
		date := C.duckdb_from_date(get[C.duckdb_date](vector, rowIdx))
		return time.Date(int(date.year), time.Month(date.month), int(date.day), 0, 0, 0, 0, time.UTC), nil
	case C.DUCKDB_TYPE_BLOB:
		return scanBlob(vector, rowIdx), nil
	case C.DUCKDB_TYPE_VARCHAR:
		return scanString(vector, rowIdx), nil
	case C.DUCKDB_TYPE_TIMESTAMP:
		return time.UnixMicro(int64(get[C.duckdb_timestamp](vector, rowIdx).micros)).UTC(), nil
	case C.DUCKDB_TYPE_INTERVAL:
		return scanInterval(vector, rowIdx)
	case C.DUCKDB_TYPE_DECIMAL:
		return scanDecimal(ty, vector, rowIdx)
	case C.DUCKDB_TYPE_LIST:
		return scanList(vector, rowIdx)
	case C.DUCKDB_TYPE_STRUCT:
		return scanStruct(ty, vector, rowIdx)
	case C.DUCKDB_TYPE_MAP:
		return scanMap(ty, vector, rowIdx)
	case C.DUCKDB_TYPE_JSON:
		return scanString(vector, rowIdx), nil
	case C.DUCKDB_TYPE_UUID:
		return get[HugeInt](vector, rowIdx).UUID(), nil
	default:
		return nil, fmt.Errorf("unsupported type %d", typeId)
	}
}

// Implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	colType := C.duckdb_column_type(&r.res, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case C.DUCKDB_TYPE_DECIMAL:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_DATE, C.DUCKDB_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf("")
	case C.DUCKDB_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case C.DUCKDB_TYPE_MAP:
		return reflect.TypeOf(Map{})
	}
	return nil
}

// Implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	colType := C.duckdb_column_type(&r.res, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return "BOOLEAN"
	case C.DUCKDB_TYPE_TINYINT:
		return "TINYINT"
	case C.DUCKDB_TYPE_SMALLINT:
		return "SMALLINT"
	case C.DUCKDB_TYPE_INTEGER:
		return "INTEGER"
	case C.DUCKDB_TYPE_BIGINT:
		return "BIGINT"
	case C.DUCKDB_TYPE_FLOAT:
		return "FLOAT"
	case C.DUCKDB_TYPE_DOUBLE:
		return "DOUBLE"
	case C.DUCKDB_TYPE_DATE:
		return "DATE"
	case C.DUCKDB_TYPE_VARCHAR:
		return "VARCHAR"
	case C.DUCKDB_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case C.DUCKDB_TYPE_BLOB:
		return "BLOB"
	case C.DUCKDB_TYPE_MAP:
		return "MAP"
	}
	return ""
}

func (r *rows) Close() error {
	C.duckdb_destroy_result(&r.res)

	if r.stmt != nil {
		C.duckdb_destroy_prepare(r.stmt.stmt)
		r.stmt.rows = false
		r.stmt = nil
	}

	return nil
}

func get[T any](vector C.duckdb_vector, rowIdx C.idx_t) T {
	ptr := C.duckdb_vector_get_data(vector)
	xs := (*[1 << 31]T)(ptr)
	return xs[rowIdx]
}

func scanMap(ty C.duckdb_logical_type, vector C.duckdb_vector, rowIdx C.idx_t) (Map, error) {
	data, err := scanStruct(ty, vector, rowIdx)
	if err != nil {
		return nil, err
	}

	out := Map{}
	keys := data["key"].([]any)
	values := data["value"].([]any)
	for i := 0; i < len(keys); i++ {
		out[keys[i]] = values[i]
	}

	return out, nil
}

func scanString(vector C.duckdb_vector, rowIdx C.idx_t) string {
	return string(scanBlob(vector, rowIdx))
}

// duckdb/tools/juliapkg/src/ctypes.jl
// `json`, `varchar`, and `blob` have the same repr
func scanBlob(vector C.duckdb_vector, rowIdx C.idx_t) []byte {
	s := get[duckdb_string_t](vector, rowIdx)
	if s.length <= stringInlineLength {
		// inline data is stored from byte 4..16 (up to 12 bytes)
		return C.GoBytes(unsafe.Pointer(&s.prefix), C.int(s.length))
	} else {
		// any longer strings are stored as a pointer in `ptr`
		return C.GoBytes(unsafe.Pointer(s.ptr), C.int(s.length))
	}
}

func scanList(vector C.duckdb_vector, rowIdx C.idx_t) ([]any, error) {
	data := C.duckdb_list_vector_get_child(vector)
	entry := get[duckdb_list_entry_t](vector, rowIdx)
	converted := make([]any, 0, entry.length)

	for i := entry.offset; i < entry.offset+entry.length; i++ {
		value, err := scan(data, i)
		if err != nil {
			return nil, err
		}
		converted = append(converted, value)
	}

	return converted, nil
}

func scanStruct(ty C.duckdb_logical_type, vector C.duckdb_vector, rowIdx C.idx_t) (map[string]any, error) {
	data := map[string]any{}
	for j := C.idx_t(0); j < C.duckdb_struct_type_child_count(ty); j++ {
		name := C.GoString(C.duckdb_struct_type_child_name(ty, j))
		child := C.duckdb_struct_vector_get_child(vector, j)
		value, err := scan(child, rowIdx)
		if err != nil {
			return nil, err
		}
		data[name] = value
	}
	return data, nil
}

func scanDecimal(ty C.duckdb_logical_type, vector C.duckdb_vector, rowIdx C.idx_t) (float64, error) {
	scale := C.duckdb_decimal_scale(ty)
	width := C.duckdb_decimal_width(ty)
	var value C.duckdb_hugeint
	switch C.duckdb_decimal_internal_type(ty) {
	case C.DUCKDB_TYPE_SMALLINT:
		value.lower = C.uint64_t(get[uint16](vector, rowIdx))
	case C.DUCKDB_TYPE_INTEGER:
		value.lower = C.uint64_t(get[uint32](vector, rowIdx))
	case C.DUCKDB_TYPE_BIGINT:
		value.lower = C.uint64_t(get[uint64](vector, rowIdx))
	case C.DUCKDB_TYPE_HUGEINT:
		i := get[C.duckdb_hugeint](vector, rowIdx)
		value.lower = i.lower
		value.upper = i.upper
	default:
		return 0, errInvalidType
	}
	decimal := C.duckdb_decimal{width, scale, value}
	return float64(C.duckdb_decimal_to_double(decimal)), nil
}

func scanInterval(vector C.duckdb_vector, rowIdx C.idx_t) (Interval, error) {
	i := get[C.duckdb_interval](vector, rowIdx)
	data := Interval{
		Days:   int32(i.days),
		Months: int32(i.months),
		Micros: int64(i.micros),
	}
	return data, nil
}

var (
	errInvalidType = errors.New("invalid data type")
)
