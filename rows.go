package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/mitchellh/mapstructure"
)

type rows struct {
	res           C.duckdb_result
	stmt          *stmt
	chunk         C.duckdb_data_chunk
	columns       []string
	chunkCount    C.ulong
	chunkRowCount C.ulong
	chunkIdx      C.idx_t
	chunkRowIdx   C.idx_t
}

func NewRows(res C.duckdb_result) *rows {
	return NewRowsWithStmt(res, nil)
}

func NewRowsWithStmt(res C.duckdb_result, stmt *stmt) *rows {
	n := C.duckdb_column_count(&res)
	columns := make([]string, 0, n)
	for i := C.ulong(0); i < n; i++ {
		columns = append(columns, C.GoString(C.duckdb_column_name(&res, i)))
	}

	return &rows{
		res:        res,
		stmt:       stmt,
		columns:    columns,
		chunkCount: C.duckdb_result_chunk_count(res),
	}
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(dst []driver.Value) error {
	if r.chunkRowIdx >= r.chunkRowCount {
		r.chunk = C.duckdb_result_get_chunk(r.res, r.chunkIdx)
		r.chunkRowCount = C.duckdb_data_chunk_get_size(r.chunk)
		r.chunkIdx++
		r.chunkRowIdx = 0
	}

	if r.chunkIdx > r.chunkCount {
		return io.EOF
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

func get[T any](vector C.duckdb_vector, rowIdx C.idx_t) T {
	ptr := C.duckdb_vector_get_data(vector)
	xs := (*[1 << 31]T)(ptr)
	return xs[rowIdx]
}

func decode(data any, out any) error {
	return mapstructure.Decode(data, out)
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
		return int64(get[int32](vector, rowIdx)), nil
	case C.DUCKDB_TYPE_BIGINT:
		return get[int64](vector, rowIdx), nil
	case C.DUCKDB_TYPE_HUGEINT:
		return get[HugeInt](vector, rowIdx).Int64()
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
		return nil, fmt.Errorf("not supported type %d", typeId)
	}
}

// Implements driver.RowsColumnTypeScanType
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	colType := C.duckdb_column_type(&r.res, C.idx_t(index))
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int(0))
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
		return "INT"
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
	}
	return ""
}

func (r *rows) Close() error {
	C.duckdb_destroy_result(&r.res)

	if r.stmt != nil {
		r.stmt.rows = false
		r.stmt = nil
	}

	return nil
}

var (
	errInvalidType = errors.New("invalid data type")
)

// HugeInt are composed in a (lower, upper) component
// The value of the HugeInt is upper * 2^64 + lower
type HugeInt struct {
	lower uint64
	upper int64
}

func (v HugeInt) UUID() []byte {
	var uuid [16]byte
	// We need to flip the `sign bit` of the signed `HugeInt` to transform it to UUID bytes
	binary.BigEndian.PutUint64(uuid[:8], uint64(v.upper)^1<<63)
	binary.BigEndian.PutUint64(uuid[8:], v.lower)
	return uuid[:]
}

func (v HugeInt) Int64() (int64, error) {
	if v.upper == 0 && v.lower <= math.MaxInt64 {
		return int64(v.lower), nil
	} else if v.upper == -1 {
		return -int64(math.MaxUint64 - v.lower + 1), nil
	} else {
		return 0, fmt.Errorf("can not convert duckdb:hugeint to go:int64 (upper:%d,lower:%d)", v.upper, v.lower)
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

const stringInlineLength = 12
const stringPrefixLength = 4

func scanString(vector C.duckdb_vector, rowIdx C.idx_t) string {
	return string(scanBlob(vector, rowIdx))
}

// duckdb/tools/juliapkg/src/ctypes.jl
// `json`, `varchar`, and `blob` have the same repr
func scanBlob(vector C.duckdb_vector, rowIdx C.idx_t) []byte {
	s := get[duckdb_string_t](vector, rowIdx)
	if s.length < stringInlineLength {
		// inline data is stored from byte 4..16 (up to 12 bytes)
		return C.GoBytes(unsafe.Pointer(&s.prefix), C.int(s.length))
	} else {
		// any longer strings are stored as a pointer in `ptr`
		return C.GoBytes(unsafe.Pointer(s.ptr), C.int(s.length))
	}
}

// Use as the `Scanner` type for any composite types (maps, lists, structs)
type Composite[T any] struct {
	t T
}

func (s Composite[T]) Get() T {
	return s.t
}

func (s *Composite[T]) Scan(v any) error {
	return decode(v, &s.t)
}

type Map map[any]any

func (m *Map) Scan(v any) error {
	data, ok := v.(Map)
	if !ok {
		return fmt.Errorf("invalid type `%T` for scanning `Map`, expected `Map`", data)
	}

	*m = data
	return nil
}
