package duckdb

import (
	"context"
	"database/sql"
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
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMPTZ))`,
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
	unsupportedTypeUDFs = []tableUDFTest[RowTableFunction]{
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
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, time.July, 8, 12, 34, 59, 123456000, time.UTC), t: TYPE_TIMESTAMP_NS},
			name:        "constTableUDF_timestamp_ns",
			query:       `SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_NS))`,
			resultCount: 1,
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
		udf.claimed += remaining
		state.end = udf.claimed
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

func TestTableUDF(t *testing.T) {
	t.Parallel()

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
}

func singleTableUDF[T TableFunction](t *testing.T, fun tableUDFTest[T]) {
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	require.NoError(t, err)

	con, err := db.Conn(context.Background())
	require.NoError(t, err)

	err = RegisterTableUDF(con, fun.name, fun.udf.GetFunction())
	require.NoError(t, err)

	res, err := db.QueryContext(context.Background(), fmt.Sprintf(fun.query, fun.name))
	require.NoError(t, err)

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
	require.NoError(t, res.Close())
	require.NoError(t, con.Close())
	require.NoError(t, db.Close())
}

func TestErrTableUDF(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	require.NoError(t, err)

	con, err := db.Conn(context.Background())
	require.NoError(t, err)

	// Empty name.
	var emptyNameUDF incTableUDF
	err = RegisterTableUDF(con, "", emptyNameUDF.GetFunction())
	testError(t, err, errAPI.Error(), errTableUDFCreate.Error(), errTableUDFNoName.Error())

	// FIXME: add more error tests.

	require.NoError(t, con.Close())
	require.NoError(t, db.Close())
}

func TestErrTableUDFUnsupportedType(t *testing.T) {
	t.Parallel()

	for _, udf := range unsupportedTypeUDFs {
		t.Run(udf.name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
			require.NoError(t, err)

			con, err := db.Conn(context.Background())
			require.NoError(t, err)

			err = RegisterTableUDF(con, udf.name, udf.udf.GetFunction())
			require.NoError(t, err)

			_, err = db.QueryContext(context.Background(), fmt.Sprintf(udf.query, udf.name))
			require.Contains(t, err.Error(), unsupportedTypeErrMsg)

			require.NoError(t, con.Close())
			require.NoError(t, db.Close())
		})
	}
}

func BenchmarkRowTableUDF(b *testing.B) {
	b.StopTimer()
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	require.NoError(b, err)

	con, err := db.Conn(context.Background())
	require.NoError(b, err)

	var fun incTableUDF
	err = RegisterTableUDF(con, "whoo", fun.GetFunction())
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, errQuery := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		require.NoError(b, errQuery)
		require.NoError(b, res.Close())
	}

	require.NoError(b, con.Close())
	require.NoError(b, db.Close())
}

func BenchmarkChunkTableUDF(b *testing.B) {
	b.StopTimer()
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	require.NoError(b, err)

	con, err := db.Conn(context.Background())
	require.NoError(b, err)

	var fun chunkIncTableUDF
	err = RegisterTableUDF(con, "whoo", fun.GetFunction())
	require.NoError(b, err)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		res, errQuery := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		require.NoError(b, errQuery)
		require.NoError(b, res.Close())
	}

	require.NoError(b, con.Close())
	require.NoError(b, db.Close())
}
