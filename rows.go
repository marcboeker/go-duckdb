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
	"math/big"
	"reflect"
	"strings"
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
		C.duckdb_destroy_data_chunk(&r.chunk)
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
	// FIXME: implement support for these types:
	// DUCKDB_TYPE_UHUGEINT
	// DUCKDB_TYPE_UNION
	// DUCKDB_TYPE_BIT
	// DUCKDB_TYPE_TIME_TZ

	validity := C.duckdb_vector_get_validity(vector)
	if !C.duckdb_validity_row_is_valid(validity, rowIdx) {
		return nil, nil
	}

	columnType := C.duckdb_vector_get_column_type(vector)
	defer C.duckdb_destroy_logical_type(&columnType)

	typeId := C.duckdb_get_type_id(columnType)
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
	case C.DUCKDB_TYPE_UTINYINT:
		return get[uint8](vector, rowIdx), nil
	case C.DUCKDB_TYPE_USMALLINT:
		return get[uint16](vector, rowIdx), nil
	case C.DUCKDB_TYPE_UINTEGER:
		return get[uint32](vector, rowIdx), nil
	case C.DUCKDB_TYPE_UBIGINT:
		return get[uint64](vector, rowIdx), nil
	case C.DUCKDB_TYPE_FLOAT:
		return get[float32](vector, rowIdx), nil
	case C.DUCKDB_TYPE_DOUBLE:
		return get[float64](vector, rowIdx), nil
	case C.DUCKDB_TYPE_TIMESTAMP:
		return time.UnixMicro(int64(get[C.duckdb_timestamp](vector, rowIdx).micros)).UTC(), nil
	case C.DUCKDB_TYPE_DATE:
		date := C.duckdb_from_date(get[C.duckdb_date](vector, rowIdx))
		return time.Date(int(date.year), time.Month(date.month), int(date.day), 0, 0, 0, 0, time.UTC), nil
	case C.DUCKDB_TYPE_TIME:
		return time.UnixMicro(int64(get[C.duckdb_time](vector, rowIdx).micros)).UTC(), nil
	case C.DUCKDB_TYPE_INTERVAL:
		return scanInterval(vector, rowIdx)
	case C.DUCKDB_TYPE_HUGEINT:
		hugeInt := get[C.duckdb_hugeint](vector, rowIdx)
		return hugeIntToNative(hugeInt), nil
	case C.DUCKDB_TYPE_VARCHAR:
		return scanString(vector, rowIdx), nil
	case C.DUCKDB_TYPE_BLOB:
		return scanBlob(vector, rowIdx), nil
	case C.DUCKDB_TYPE_DECIMAL:
		return scanDecimal(columnType, vector, rowIdx)
	case C.DUCKDB_TYPE_TIMESTAMP_S:
		return time.Unix(int64(get[C.duckdb_timestamp](vector, rowIdx).micros), 0).UTC(), nil
	case C.DUCKDB_TYPE_TIMESTAMP_MS:
		return time.UnixMilli(int64(get[C.duckdb_timestamp](vector, rowIdx).micros)).UTC(), nil
	case C.DUCKDB_TYPE_TIMESTAMP_NS:
		return time.Unix(0, int64(get[C.duckdb_timestamp](vector, rowIdx).micros)).UTC(), nil
	case C.DUCKDB_TYPE_ENUM:
		return scanENUM(columnType, vector, rowIdx)
	case C.DUCKDB_TYPE_LIST:
		return scanList(vector, rowIdx)
	case C.DUCKDB_TYPE_STRUCT:
		return scanStruct(columnType, vector, rowIdx)
	case C.DUCKDB_TYPE_MAP:
		return scanMap(vector, rowIdx)
	case C.DUCKDB_TYPE_UUID:
		hugeInt := get[C.duckdb_hugeint](vector, rowIdx)
		return hugeIntToUUID(hugeInt), nil
	case C.DUCKDB_TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(int64(get[C.duckdb_timestamp](vector, rowIdx).micros)).UTC(), nil
	default:
		return nil, fmt.Errorf("unsupported type %d", typeId)
	}
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
	C.duckdb_destroy_data_chunk(&r.chunk)
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

func get[T any](vector C.duckdb_vector, rowIdx C.idx_t) T {
	ptr := C.duckdb_vector_get_data(vector)
	xs := (*[1 << 31]T)(ptr)
	return xs[rowIdx]
}

func scanMap(vector C.duckdb_vector, rowIdx C.idx_t) (Map, error) {
	list, err := scanList(vector, rowIdx)
	if err != nil {
		return nil, err
	}

	// DuckDB supports more map key types than Go, which only supports comparable types.
	// To avoid a panic, we check that the map key type is comparable.
	// All keys in a DuckDB map have the same type, so we just do this check for the first value.
	if len(list) > 0 {
		mapItem := list[0].(map[string]any)
		key, ok := mapItem["key"]
		if !ok {
			return nil, errMissingKeyOrValue
		}
		if !reflect.TypeOf(key).Comparable() {
			return nil, getError(errUnsupportedMapKeyType, nil)
		}
	}

	out := Map{}
	for i := 0; i < len(list); i++ {
		mapItem := list[i].(map[string]any)
		key, ok := mapItem["key"]
		if !ok {
			return nil, errMissingKeyOrValue
		}
		val, ok := mapItem["value"]
		if !ok {
			return nil, errMissingKeyOrValue
		}
		out[key] = val
	}

	return out, nil
}

func scanString(vector C.duckdb_vector, rowIdx C.idx_t) string {
	return string(scanBlob(vector, rowIdx))
}

// duckdb/tools/juliapkg/src/ctypes.jl
// `json`, `varchar`, and `blob` are C-style char arrays
func scanBlob(vector C.duckdb_vector, rowIdx C.idx_t) []byte {
	// we don't have to free s.ptr, as it is part of the data in the vector
	s := get[duckdb_string_t](vector, rowIdx)

	if s.length <= stringInlineLength {
		// inlined data is stored from byte 4..16 (up to 12 bytes)
		return C.GoBytes(unsafe.Pointer(&s.prefix), C.int(s.length))
	}

	// any longer strings are stored as a pointer in `ptr`
	return C.GoBytes(unsafe.Pointer(s.ptr), C.int(s.length))
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
		ptrToChildName := C.duckdb_struct_type_child_name(ty, j)
		name := C.GoString(ptrToChildName)
		C.duckdb_free(unsafe.Pointer(ptrToChildName))

		child := C.duckdb_struct_vector_get_child(vector, j)
		value, err := scan(child, rowIdx)
		if err != nil {
			return nil, err
		}
		data[name] = value
	}
	return data, nil
}

func scanDecimal(ty C.duckdb_logical_type, vector C.duckdb_vector, rowIdx C.idx_t) (Decimal, error) {
	scale := C.duckdb_decimal_scale(ty)
	width := C.duckdb_decimal_width(ty)
	var nativeValue *big.Int
	switch C.duckdb_decimal_internal_type(ty) {
	case C.DUCKDB_TYPE_SMALLINT:
		nativeValue = big.NewInt(int64(get[int16](vector, rowIdx)))
	case C.DUCKDB_TYPE_INTEGER:
		nativeValue = big.NewInt(int64(get[int32](vector, rowIdx)))
	case C.DUCKDB_TYPE_BIGINT:
		nativeValue = big.NewInt(int64(get[int64](vector, rowIdx)))
	case C.DUCKDB_TYPE_HUGEINT:
		i := get[C.duckdb_hugeint](vector, rowIdx)
		nativeValue = hugeIntToNative(C.duckdb_hugeint{
			lower: i.lower,
			upper: i.upper,
		})
	default:
		return Decimal{}, errInvalidType
	}

	if nativeValue == nil {
		return Decimal{}, fmt.Errorf("unable to convert hugeint to native type")
	}

	return Decimal{Width: uint8(width), Scale: uint8(scale), Value: nativeValue}, nil
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

func scanENUM(ty C.duckdb_logical_type, vector C.duckdb_vector, rowIdx C.idx_t) (string, error) {
	var idx uint64
	internalType := C.duckdb_enum_internal_type(ty)
	switch internalType {
	case C.DUCKDB_TYPE_UTINYINT:
		idx = uint64(get[uint8](vector, rowIdx))
	case C.DUCKDB_TYPE_USMALLINT:
		idx = uint64(get[uint16](vector, rowIdx))
	case C.DUCKDB_TYPE_UINTEGER:
		idx = uint64(get[uint32](vector, rowIdx))
	case C.DUCKDB_TYPE_UBIGINT:
		idx = get[uint64](vector, rowIdx)
	default:
		return "", errInvalidType
	}

	val := C.duckdb_enum_dictionary_value(ty, (C.idx_t)(idx))
	defer C.duckdb_free(unsafe.Pointer(val))
	return C.GoString(val), nil
}

var (
	errInvalidType       = errors.New("invalid data type")
	errMissingKeyOrValue = errors.New("missing key and/or value for map item")
)

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
