package duckdb

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type (
	testTableFunction[T TableFunction] interface {
		GetFunction() T
		GetValue(r, c int) any
		GetTypes() []any
	}

	tableUDFTest[T TableFunction] struct {
		udf         testTableFunction[T]
		name        string
		query       string
		resultCount int
	}

	// Row-based table UDF tests.

	incTableUDF struct {
		n     int64
		count int64
	}

	structTableUDF struct {
		n     int64
		count int64
	}

	otherStructTableUDF struct {
		I int64
	}

	pushdownTableUDF struct {
		n     int64
		count int64
	}

	incTableNamedUDF struct {
		n     int64
		count int64
	}

	constTableUDF[T any] struct {
		count int64
		value T
		t     Type
	}

	// Parallel row-based table UDF tests.

	parallelIncTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	parallelIncTableLocalState struct {
		start int64
		end   int64
	}

	// Chunk-based table UDF tests.

	chunkIncTableUDF struct {
		n     int64
		count int64
	}

	unionTableUDF struct {
		n     int64
		count int64
	}

	// Parallel chunk-based table UDF tests.

	parallelChunkIncTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	parallelChunkIncTableLocalState struct {
		start int64
		end   int64
	}
)

var (
	rowTableUDFs = []tableUDFTest[RowTableFunction]{
		{
			udf:         &incTableUDF{},
			name:        "incTableUDF_non_full_vector",
			query:       `SELECT * FROM %s(2047)`,
			resultCount: 2047,
		},
		{
			udf:         &incTableUDF{},
			name:        "incTableUDF",
			query:       `SELECT * FROM %s(10000)`,
			resultCount: 10000,
		},
		{
			udf:         &structTableUDF{},
			name:        "structTableUDF",
			query:       `SELECT * FROM %s(2048)`,
			resultCount: 2048,
		},
		{
			udf:         &pushdownTableUDF{},
			name:        "pushdownTableUDF",
			query:       `SELECT result2 FROM %s(2048)`,
			resultCount: 2048,
		},
		{
			udf:         &incTableNamedUDF{},
			name:        "incTableNamedUDF",
			query:       `SELECT * FROM %s(ARG=2048)`,
			resultCount: 2048,
		},
		{
			udf:         &constTableUDF[bool]{value: false, t: TYPE_BOOLEAN},
			name:        "constTableUDF_bool",
			query:       `SELECT * FROM %s(false)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int8]{value: -8, t: TYPE_TINYINT},
			name:        "constTableUDF_int8",
			query:       `SELECT * FROM %s(CAST(-8 AS TINYINT))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int16]{value: -16, t: TYPE_SMALLINT},
			name:        "constTableUDF_int16",
			query:       `SELECT * FROM %s(CAST(-16 AS SMALLINT))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int32]{value: -32, t: TYPE_INTEGER},
			name:        "constTableUDF_int32",
			query:       `SELECT * FROM %s(-32)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int64]{value: -64, t: TYPE_BIGINT},
			name:        "constTableUDF_int64",
			query:       `SELECT * FROM %s(-64)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint8]{value: 8, t: TYPE_UTINYINT},
			name:        "constTableUDF_uint8",
			query:       `SELECT * FROM %s(CAST(8 AS UTINYINT))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint16]{value: 16, t: TYPE_USMALLINT},
			name:        "constTableUDF_uint16",
			query:       `SELECT * FROM %s(CAST(16 AS USMALLINT))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint32]{value: 32, t: TYPE_UINTEGER},
			name:        "constTableUDF_uint32",
			query:       `SELECT * FROM %s(CAST(32 AS UINTEGER))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint64]{value: 64, t: TYPE_UBIGINT},
			name:        "constTableUDF_uint64",
			query:       `SELECT * FROM %s(CAST(64 AS UBIGINT))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[float32]{value: 32, t: TYPE_FLOAT},
			name:        "constTableUDF_float32",
			query:       `SELECT * FROM %s(32)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[float64]{value: 64, t: TYPE_DOUBLE},
			name:        "constTableUDF_float64",
			query:       `SELECT * FROM %s(64)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2008, time.July, 8, 12, 34, 59, 123456000, time.UTC), t: TYPE_TIMESTAMP},
			name:        "constTableUDF_timestamp",
			query:       `SELECT * FROM %s(CAST('2008-07-08 12:34:59.123456789' AS TIMESTAMP))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2007, time.July, 8, 0, 0, 0, 0, time.UTC), t: TYPE_DATE},
			name:        "constTableUDF_date",
			query:       `SELECT * FROM %s(CAST('2007-07-08 12:34:59.123456789' AS DATE))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(1, time.January, 1, 12, 34, 59, 123456000, time.UTC), t: TYPE_TIME},
			name:        "constTableUDF_time",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIME))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[Interval]{value: Interval{Months: 16, Days: 10, Micros: 172800000000}, t: TYPE_INTERVAL},
			name:        "constTableUDF_interval",
			query:       `SELECT * FROM %s('16 months 10 days 48:00:00'::INTERVAL)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[*big.Int]{value: big.NewInt(10000000000000000), t: TYPE_HUGEINT},
			name:        "constTableUDF_bigint",
			query:       `SELECT * FROM %s(10000000000000000)`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[string]{value: "my_lovely_string", t: TYPE_VARCHAR},
			name:        "constTableUDF_string",
			query:       `SELECT * FROM %s('my_lovely_string')`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 123456000, time.UTC), t: TYPE_TIMESTAMP_TZ},
			name:        "constTableUDF_timestamp_tz",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789+00' AS TIMESTAMPTZ))`,
			resultCount: 1,
		},
		{
			udf:         &unionTableUDF{},
			name:        "unionTableUDF",
			query:       `SELECT * FROM %s(4)`,
			resultCount: 4,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, time.July, 8, 12, 34, 59, 0, time.UTC), t: TYPE_TIMESTAMP_S},
			name:        "constTableUDF_timestamp_s",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_S))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, time.July, 8, 12, 34, 59, 123000000, time.UTC), t: TYPE_TIMESTAMP_MS},
			name:        "constTableUDF_timestamp_ms",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_MS))`,
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, time.July, 8, 12, 34, 59, 123456789, time.UTC), t: TYPE_TIMESTAMP_NS},
			name:        "constTableUDF_timestamp_ns",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_NS))`,
			resultCount: 1,
		},
	}
	parallelTableUDFs = []tableUDFTest[ParallelRowTableFunction]{
		{
			udf:         &parallelIncTableUDF{},
			name:        "parallelIncTableUDF",
			query:       `SELECT * FROM %s(2048) ORDER BY result`,
			resultCount: 2048,
		},
	}
	chunkTableUDFs = []tableUDFTest[ChunkTableFunction]{
		{
			udf:         &chunkIncTableUDF{},
			name:        "chunkIncTableUDF",
			query:       `SELECT * FROM %s(2048)`,
			resultCount: 2048,
		},
	}
	parallelChunkTableUDFs = []tableUDFTest[ParallelChunkTableFunction]{
		{
			udf:         &parallelChunkIncTableUDF{},
			name:        "parallelChunkIncTableUDF",
			query:       `SELECT * FROM %s(2048) ORDER BY result`,
			resultCount: 2048,
		},
	}
)

var (
	typeBigintTableUDF, _ = NewTypeInfo(TYPE_BIGINT)
	typeStructTableUDF    = makeStructTableUDF()
)

func makeStructTableUDF() TypeInfo {
	entry, _ := NewStructEntry(typeBigintTableUDF, "I")
	info, _ := NewStructInfo(entry)
	return info
}

func (udf *incTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindIncTableUDF,
	}
}

func bindIncTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &incTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (udf *incTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeBigintTableUDF}}
}

func (udf *incTableUDF) Init() {}

func (udf *incTableUDF) FillRow(row Row) (bool, error) {
	if udf.count >= udf.n {
		return false, nil
	}
	udf.count++
	err := SetRowValue(row, 0, udf.count)
	return true, err
}

func (udf *incTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *incTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *incTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func bindParallelIncTableUDF(namedArgs map[string]any, args ...interface{}) (ParallelRowTableSource, error) {
	return &parallelIncTableUDF{
		lock:    &sync.Mutex{},
		claimed: 0,
		n:       args[0].(int64),
	}, nil
}

func (udf *parallelIncTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeBigintTableUDF}}
}

func (udf *parallelIncTableUDF) Init() ParallelTableSourceInfo {
	return ParallelTableSourceInfo{MaxThreads: 8}
}

func (udf *parallelIncTableUDF) NewLocalState() any {
	return &parallelIncTableLocalState{
		start: 0,
		end:   -1,
	}
}

func (udf *parallelIncTableUDF) FillRow(localState any, row Row) (bool, error) {
	state := localState.(*parallelIncTableLocalState)

	if state.start >= state.end {
		// Claim a new work unit.
		udf.lock.Lock()
		remaining := udf.n - udf.claimed

		if remaining <= 0 {
			// No more work.
			udf.lock.Unlock()
			return false, nil
		} else if remaining >= 2024 {
			remaining = 2024
		}

		state.start = udf.claimed
		state.end = udf.claimed + remaining
		udf.claimed += remaining
		udf.lock.Unlock()
	}

	state.start++
	err := SetRowValue(row, 0, state.start)
	return true, err
}

func (udf *parallelIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *parallelIncTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *parallelIncTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *parallelIncTableUDF) GetFunction() ParallelRowTableFunction {
	return ParallelRowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindParallelIncTableUDF,
	}
}

func bindParallelChunkIncTableUDF(namedArgs map[string]any, args ...interface{}) (ParallelChunkTableSource, error) {
	return &parallelChunkIncTableUDF{
		lock:    &sync.Mutex{},
		claimed: 0,
		n:       args[0].(int64),
	}, nil
}

func (udf *parallelChunkIncTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeBigintTableUDF}}
}

func (udf *parallelChunkIncTableUDF) Init() ParallelTableSourceInfo {
	return ParallelTableSourceInfo{MaxThreads: 8}
}

func (udf *parallelChunkIncTableUDF) NewLocalState() any {
	return &parallelChunkIncTableLocalState{
		start: 0,
		end:   -1,
	}
}

func (udf *parallelChunkIncTableUDF) FillChunk(localState any, chunk DataChunk) error {
	state := localState.(*parallelChunkIncTableLocalState)

	// Claim a new work unit.
	udf.lock.Lock()
	remaining := udf.n - udf.claimed
	if remaining <= 0 {
		// No more work.
		udf.lock.Unlock()
		return nil
	} else if remaining >= 2048 {
		remaining = 2048
	}
	state.start = udf.claimed
	state.end = udf.claimed + remaining
	udf.claimed += remaining
	udf.lock.Unlock()

	for i := 0; i < int(remaining); i++ {
		err := chunk.SetValue(0, i, int64(i)+state.start+1)
		if err != nil {
			return err
		}
	}

	err := chunk.SetSize(int(remaining))
	return err
}

func (udf *parallelChunkIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *parallelChunkIncTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *parallelChunkIncTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *parallelChunkIncTableUDF) GetFunction() ParallelChunkTableFunction {
	return ParallelChunkTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF}, // 参数类型为BIGINT
		},
		BindArguments: bindParallelChunkIncTableUDF, // 绑定参数函数
	}
}

func (udf *structTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindStructTableUDF,
	}
}

func bindStructTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &structTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (udf *structTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeStructTableUDF}}
}

func (udf *structTableUDF) Init() {}

func (udf *structTableUDF) FillRow(row Row) (bool, error) {
	if udf.count >= udf.n {
		return false, nil
	}
	udf.count++
	err := SetRowValue(row, 0, otherStructTableUDF{I: udf.count})
	return true, err
}

func (udf *structTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *structTableUDF) GetValue(r, c int) any {
	return map[string]any{"I": int64(r + 1)}
}

func (udf *structTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *pushdownTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindPushdownTableUDF,
	}
}

func bindPushdownTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &pushdownTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (udf *pushdownTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: typeBigintTableUDF},
		{Name: "result2", T: typeBigintTableUDF},
	}
}

func (udf *pushdownTableUDF) Init() {}

func (udf *pushdownTableUDF) FillRow(row Row) (bool, error) {
	if udf.count >= udf.n {
		return false, nil
	}

	if row.IsProjected(0) {
		err := fmt.Errorf("column 0 is projected while it should not be")
		return false, err
	}

	udf.count++
	if err := SetRowValue(row, 0, udf.count); err != nil {
		return false, err
	}
	if err := SetRowValue(row, 1, udf.count); err != nil {
		return false, err
	}
	return true, nil
}

func (udf *pushdownTableUDF) GetName() string {
	return "pushdownTableUDF"
}

func (udf *pushdownTableUDF) GetTypes() []any {
	return []any{int64(0)}
}

func (udf *pushdownTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *pushdownTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *incTableNamedUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			NamedArguments: map[string]TypeInfo{"ARG": typeBigintTableUDF},
		},
		BindArguments: bindIncTableNamedUDF,
	}
}

func bindIncTableNamedUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &incTableNamedUDF{
		count: 0,
		n:     namedArgs["ARG"].(int64),
	}, nil
}

func (udf *incTableNamedUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeBigintTableUDF}}
}

func (udf *incTableNamedUDF) Init() {}

func (udf *incTableNamedUDF) FillRow(row Row) (bool, error) {
	if udf.count >= udf.n {
		return false, nil
	}
	udf.count++
	err := SetRowValue(row, 0, udf.count)
	return true, err
}

func (udf *incTableNamedUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *incTableNamedUDF) GetTypes() []any {
	return []any{0}
}

func (udf *incTableNamedUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *constTableUDF[T]) GetFunction() RowTableFunction {
	info, _ := NewTypeInfo(udf.t)
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{info},
		},
		BindArguments: bindConstTableUDF(udf.value, udf.t),
	}
}

func bindConstTableUDF[T any](val T, t Type) func(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return func(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
		return &constTableUDF[T]{
			count: 0,
			value: args[0].(T),
			t:     t,
		}, nil
	}
}

func (udf *constTableUDF[T]) ColumnInfos() []ColumnInfo {
	info, _ := NewTypeInfo(udf.t)
	return []ColumnInfo{{Name: "result", T: info}}
}

func (udf *constTableUDF[T]) Init() {}

func (udf *constTableUDF[T]) FillRow(row Row) (bool, error) {
	if udf.count >= 1 {
		return false, nil
	}
	udf.count++
	err := SetRowValue(row, 0, udf.value)
	return true, err
}

func (udf *constTableUDF[T]) GetValue(r, c int) any {
	return udf.value
}

func (udf *constTableUDF[T]) GetTypes() []any {
	return []any{udf.value}
}

func (udf *constTableUDF[T]) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *chunkIncTableUDF) GetFunction() ChunkTableFunction {
	return ChunkTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindChunkIncTableUDF,
	}
}

func bindChunkIncTableUDF(namedArgs map[string]any, args ...interface{}) (ChunkTableSource, error) {
	return &chunkIncTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (udf *chunkIncTableUDF) ColumnInfos() []ColumnInfo {
	return []ColumnInfo{{Name: "result", T: typeBigintTableUDF}}
}

func (udf *chunkIncTableUDF) Init() {}

func (udf *chunkIncTableUDF) FillChunk(chunk DataChunk) error {
	size := 2048
	i := 0

	for ; i < size; i++ {
		if udf.count >= udf.n {
			err := chunk.SetSize(i)
			return err
		}
		udf.count++
		err := chunk.SetValue(0, i, udf.count)
		if err != nil {
			return err
		}
	}

	err := chunk.SetSize(i)
	return err
}

func (udf *chunkIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (udf *chunkIncTableUDF) GetTypes() []any {
	return []any{0}
}

func (udf *chunkIncTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (udf *unionTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeBigintTableUDF},
		},
		BindArguments: bindUnionTableUDF,
	}
}

func bindUnionTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &unionTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (udf *unionTableUDF) ColumnInfos() []ColumnInfo {
	// Create member types.
	intInfo, _ := NewTypeInfo(TYPE_INTEGER)
	varcharInfo, _ := NewTypeInfo(TYPE_VARCHAR)

	// Create UNION type info.
	unionInfo, _ := NewUnionInfo(
		[]TypeInfo{intInfo, varcharInfo},
		[]string{"number", "text"},
	)

	return []ColumnInfo{{Name: "result", T: unionInfo}}
}

func (udf *unionTableUDF) Init() {}

func (udf *unionTableUDF) FillRow(row Row) (bool, error) {
	if udf.count >= udf.n {
		return false, nil
	}
	udf.count++

	var val Union
	if udf.count%2 == 0 {
		// Even numbers: store as number.
		val = Union{
			Value: int32(udf.count),
			Tag:   "number",
		}
	} else {
		// Odd numbers: store as text.
		val = Union{
			Value: fmt.Sprintf("text_%d", udf.count),
			Tag:   "text",
		}
	}

	err := SetRowValue(row, 0, val)
	return true, err
}

func (udf *unionTableUDF) GetValue(r, c int) any {
	count := int64(r + 1)
	if count%2 == 0 {
		return Union{
			Value: int32(count),
			Tag:   "number",
		}
	}
	return Union{
		Value: fmt.Sprintf("text_%d", count),
		Tag:   "text",
	}
}

func (udf *unionTableUDF) GetTypes() []any {
	return []any{Union{}}
}

func (udf *unionTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func TestTableUDF(t *testing.T) {
	for _, udf := range rowTableUDFs {
		t.Run(udf.name, func(t *testing.T) {
			singleTableUDF(t, udf)
		})
	}

	for _, udf := range parallelTableUDFs {
		t.Run(udf.name, func(t *testing.T) {
			singleTableUDF(t, udf)
		})
	}

	for _, udf := range chunkTableUDFs {
		t.Run(udf.name, func(t *testing.T) {
			singleTableUDF(t, udf)
		})
	}

	for _, udf := range parallelChunkTableUDFs {
		t.Run(udf.name, func(t *testing.T) {
			singleTableUDF(t, udf)
		})
	}
}

func singleTableUDF[T TableFunction](t *testing.T, fun tableUDFTest[T]) {
	db := openDbWrapper(t, `?access_mode=READ_WRITE`)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	err := RegisterTableUDF(conn, fun.name, fun.udf.GetFunction())
	require.NoError(t, err)

	res, err := db.QueryContext(context.Background(), fmt.Sprintf(fun.query, fun.name))
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	values := fun.udf.GetTypes()
	args := make([]interface{}, len(values))
	for i := range values {
		args[i] = &values[i]
	}

	count := 0
	for r := 0; res.Next(); r++ {
		require.NoError(t, res.Scan(args...))
		for i, value := range values {
			expected := fun.udf.GetValue(r, i)
			require.Equal(t, expected, value, "incorrect value")
		}
		count++
	}
	require.Equal(t, count, fun.resultCount, "result count did not match the expected count")
}

func TestErrTableUDF(t *testing.T) {
	db := openDbWrapper(t, `?access_mode=READ_WRITE`)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	// Empty name.
	var emptyNameUDF incTableUDF
	err := RegisterTableUDF(conn, "", emptyNameUDF.GetFunction())
	testError(t, err, errAPI.Error(), errTableUDFCreate.Error(), errTableUDFNoName.Error())

	// FIXME: add more error tests.
}

func TestTableUDFAggregate(t *testing.T) {
	db := openDbWrapper(t, `?access_mode=READ_WRITE`)
	defer closeDbWrapper(t, db)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	var udf incTableUDF
	err := RegisterTableUDF(conn, "increment", udf.GetFunction())
	require.NoError(t, err)

	r, err := db.QueryContext(context.Background(), `SELECT COUNT() FROM increment(100)`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, r)

	for r.Next() {
		var count uint64
		err = r.Scan(&count)
		require.NoError(t, err)
		require.Equal(t, uint64(100), count)
	}
}

func BenchmarkRowTableUDF(b *testing.B) {
	b.StopTimer()
	db := openDbWrapper(b, `?access_mode=READ_WRITE`)
	defer closeDbWrapper(b, db)

	conn := openConnWrapper(b, db, context.Background())
	defer closeConnWrapper(b, conn)

	var fun incTableUDF
	err := RegisterTableUDF(conn, "whoo", fun.GetFunction())
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, errQuery := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		require.NoError(b, errQuery)
		closeRowsWrapper(b, res)
	}
}

func BenchmarkChunkTableUDF(b *testing.B) {
	b.StopTimer()
	db := openDbWrapper(b, `?access_mode=READ_WRITE`)
	defer closeDbWrapper(b, db)

	conn := openConnWrapper(b, db, context.Background())
	defer closeConnWrapper(b, conn)

	var fun chunkIncTableUDF
	err := RegisterTableUDF(conn, "whoo", fun.GetFunction())
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, errQuery := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		require.NoError(b, errQuery)
		closeRowsWrapper(b, res)
	}
}
