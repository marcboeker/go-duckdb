package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"reflect"
	"sync"
	"testing"
	"time"
)

type (
	wrongValueError struct {
		rowIdx   int
		colIdx   int
		colName  string
		expected any
		got      any
	}

	testTableFunction[T TableFunction] interface {
		GetFunction() T
		GetValue(r, c int) any
		GetTypes() []any
	}

	UdtfTest[T TableFunction] struct {
		udf         testTableFunction[T]
		name        string
		query       string
		resultCount int
	}

	// Row UDTF tests
	incTableUDF struct {
		n     int64
		count int64
	}

	structTableUDF struct {
		n     int64
		count int64
	}

	structTableUDFT struct {
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
		count      int64
		value      T
		duckdbType Type
	}

	// Parallel row UDTF tests
	parallelIncTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	parallelIncTableLocal struct {
		start int64
		end   int64
	}

	// Chunk UDTF tests
	chunkIncTableUDF struct {
		n     int64
		count int64
	}
)

var (
	rowUdtfs = []UdtfTest[RowTableFunction]{
		{
			udf:         &incTableUDF{},
			name:        "incTableUDTF__non_full_vector",
			query:       "SELECT * FROM %s(2047)",
			resultCount: 2047,
		},
		{
			udf:         &incTableUDF{},
			name:        "incTableUDTF",
			query:       "SELECT * FROM %s(2048)",
			resultCount: 2048,
		},
		{
			udf:         &structTableUDF{},
			name:        "structTableUDTF",
			query:       "SELECT * FROM %s(2048)",
			resultCount: 2048,
		},
		{
			udf:         &pushdownTableUDF{},
			name:        "pushdownTableUDTF",
			query:       "SELECT result2 FROM %s(2048)",
			resultCount: 2048,
		},
		{
			udf:         &incTableNamedUDF{},
			name:        "incTableNamedUDTF",
			query:       "SELECT * FROM %s(ARG=2048)",
			resultCount: 2048,
		},
		{
			udf:         &constTableUDF[bool]{value: false, duckdbType: TYPE_BOOLEAN},
			name:        "constTableNamedUDTF_bool",
			query:       "SELECT * FROM %s(false)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int8]{value: -8, duckdbType: TYPE_TINYINT},
			name:        "constTableNamedUDTF_int8",
			query:       "SELECT * FROM %s(CAST(-8 AS TINYINT))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int16]{value: -16, duckdbType: TYPE_SMALLINT},
			name:        "constTableNamedUDTF_int16",
			query:       "SELECT * FROM %s(CAST(-16 AS SMALLINT))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int32]{value: -32, duckdbType: TYPE_INTEGER},
			name:        "constTableNamedUDTF_int32",
			query:       "SELECT * FROM %s(-32)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[int64]{value: -64, duckdbType: TYPE_BIGINT},
			name:        "constTableNamedUDTF_int64",
			query:       "SELECT * FROM %s(-64)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint8]{value: 8, duckdbType: TYPE_UTINYINT},
			name:        "constTableNamedUDTF_uint8",
			query:       "SELECT * FROM %s(CAST(8 AS UTINYINT))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint16]{value: 16, duckdbType: TYPE_USMALLINT},
			name:        "constTableNamedUDTF_uint16",
			query:       "SELECT * FROM %s(CAST(16 AS USMALLINT))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint32]{value: 32, duckdbType: TYPE_UINTEGER},
			name:        "constTableNamedUDTF_uint32",
			query:       "SELECT * FROM %s(CAST(32 AS UINTEGER))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[uint64]{value: 64, duckdbType: TYPE_UBIGINT},
			name:        "constTableNamedUDTF_uint64",
			query:       "SELECT * FROM %s(CAST(64 AS UBIGINT))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[float32]{value: 32, duckdbType: TYPE_FLOAT},
			name:        "constTableNamedUDTF_float32",
			query:       "SELECT * FROM %s(32)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[float64]{value: 64, duckdbType: TYPE_DOUBLE},
			name:        "constTableNamedUDTF_float64",
			query:       "SELECT * FROM %s(64)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 123456000, time.UTC), duckdbType: TYPE_TIMESTAMP},
			name:        "constTableNamedUDTF_TIMESTAMP",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 0,0, 0, 0, time.UTC), duckdbType: TYPE_DATE},
			name:        "constTableNamedUDTF_DATE",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS DATE))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(1970, 1, 1, 12, 34, 59, 123456000, time.UTC), duckdbType: TYPE_TIME},
			name:        "constTableNamedUDTF_TIME",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIME))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[Interval]{value: Interval{Months: 16, Days: 10, Micros: 172800000000}, duckdbType: TYPE_INTERVAL},
			name:        "constTableNamedUDTF_INTERVAL",
			query:       "SELECT * FROM %s('16 months 10 days 48:00:00'::INTERVAL)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[*big.Int]{value: big.NewInt(10000000000000000), duckdbType: TYPE_HUGEINT},
			name:        "constTableNamedUDTF_bigint",
			query:       "SELECT * FROM %s(10000000000000000)",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[string]{value: "sjbfsd", duckdbType: TYPE_VARCHAR},
			name:        "constTableNamedUDTF_string",
			query:       "SELECT * FROM %s('sjbfsd')",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 0, time.UTC), duckdbType: TYPE_TIMESTAMP_S},
			name:        "constTableNamedUDTF_TIMESTAMPS",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_S))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 123000000, time.UTC), duckdbType: TYPE_TIMESTAMP_MS},
			name:        "constTableNamedUDTF_TIMESTAMPMS",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_MS))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 123456000, time.UTC), duckdbType: TYPE_TIMESTAMP_NS},
			name:        "constTableNamedUDTF_TIMESTAMPNS",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMP_NS))",
			resultCount: 1,
		},
		{
			udf:         &constTableUDF[time.Time]{value: time.Date(2006, 7, 8, 12, 34, 59, 123456000, time.UTC), duckdbType: TYPE_TIMESTAMP_TZ},
			name:        "constTableNamedUDTF_TIMESTAMPTZ",
			query:       "SELECT * FROM %s(CAST('2006-07-08 12:34:59.123456789' AS TIMESTAMPTZ))",
			resultCount: 1,
		},
	}
	parallelRowUdtfs = []UdtfTest[ThreadedRowTableFunction]{
		{
			udf:         &parallelIncTableUDF{},
			name:        "parallelIncTableUDTF",
			query:       "SELECT * FROM %s(2048) ORDER BY result",
			resultCount: 2048,
		},
	}
	chunkUdtfs = []UdtfTest[ChunkTableFunction]{
		{
			udf:         &chunkIncTableUDF{},
			name:        "chunkIncTableUDTF",
			query:       "SELECT * FROM %s(2048)",
			resultCount: 2048,
		},
	}
)

var (
	Ti64, _          = NewTypeInfo(TYPE_BIGINT)
	TStructTableUDFT = makeStructTableUDFT()
)

func makeStructTableUDFT() TypeInfo {
	entry, _ := NewStructEntry(Ti64, "I")
	ret, _ := NewStructInfo(entry)
	return ret
}

func (d *incTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindIncTableUDF,
	}
}

func BindIncTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &incTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (d *incTableUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: Ti64},
	}
}

func (d *incTableUDF) Init() {}

func (d *incTableUDF) FillRow(row Row) (bool, error) {
	if d.count >= d.n {
		return false, nil
	}
	d.count++
	err := SetRowValue(row, 0, d.count)
	return true, err
}

func (d *incTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *incTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *incTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func BindParallelIncTableUDF(namedArgs map[string]any, args ...interface{}) (ThreadedRowTableSource, error) {
	return &parallelIncTableUDF{
		lock:    &sync.Mutex{},
		claimed: 0,
		n:       args[0].(int64),
	}, nil
}

func (d *parallelIncTableUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: Ti64},
	}
}

func (d *parallelIncTableUDF) Init() ThreadedTableSourceInitData {
	return ThreadedTableSourceInitData{
		MaxThreads: 8,
	}
}

func (d *parallelIncTableUDF) NewLocalState() any {
	return &parallelIncTableLocal{
		start: 0,
		end:   -1,
	}
}

func (d *parallelIncTableUDF) FillRow(localState any, row Row) (bool, error) {
	state := localState.(*parallelIncTableLocal)
	if state.start >= state.end {
		// claim a new "work" unit
		d.lock.Lock()
		remaining := d.n - d.claimed
		if remaining <= 0 {
			// no more work to be done :(
			d.lock.Unlock()
			return false, nil
		} else if remaining >= 2024 {
			remaining = 2024
		}
		state.start = d.claimed
		d.claimed += remaining
		state.end = d.claimed
		d.lock.Unlock()
	}
	state.start++
	err := SetRowValue(row, 0, state.start)
	return true, err
}

func (d *parallelIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *parallelIncTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *parallelIncTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (d *parallelIncTableUDF) GetFunction() ThreadedRowTableFunction {
	return ThreadedRowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindParallelIncTableUDF,
	}
}

func (d *structTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindStructTableUDF,
	}
}

func BindStructTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &structTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (d *structTableUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: TStructTableUDFT},
	}
}

func (d *structTableUDF) Init() {}

func (d *structTableUDF) FillRow(row Row) (bool, error) {
	if d.count >= d.n {
		return false, nil
	}
	d.count++
	err := SetRowValue(row, 0, structTableUDFT{I: d.count})
	return true, err
}

func (d *structTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *structTableUDF) GetValue(r, c int) any {
	return map[string]any{
		"I": int64(r + 1),
	}
}

func (d *structTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (d *pushdownTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindPushdownTableUDF,
	}
}

func BindPushdownTableUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &pushdownTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (d *pushdownTableUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: Ti64},
		{Name: "result2", T: Ti64},
	}
}

func (d *pushdownTableUDF) Init() {}

func (d *pushdownTableUDF) FillRow(row Row) (bool, error) {
	if d.count >= d.n {
		return false, nil
	}

	var err error
	if row.IsProjected(0) {
		err = fmt.Errorf("Column 0 is projected while it should not be")
	}
	d.count++
	if _err := SetRowValue(row, 0, d.count); _err != nil {
		err = _err
	}
	if _err := SetRowValue(row, 1, d.count); _err != nil {
		err = _err
	}
	return true, err
}

func (d *pushdownTableUDF) GetName() string {
	return "pushdownTableUDF"
}

func (d *pushdownTableUDF) GetTypes() []any {
	return []any{
		int64(0),
	}
}
func (d *pushdownTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *pushdownTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (d *incTableNamedUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			NamedArguments: map[string]TypeInfo{"ARG": Ti64},
		},
		BindArguments: BindIncTableNamedUDF,
	}
}

func BindIncTableNamedUDF(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return &incTableNamedUDF{
		count: 0,
		n:     namedArgs["ARG"].(int64),
	}, nil
}

func (d *incTableNamedUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: Ti64},
	}
}

func (d *incTableNamedUDF) Init() {}

func (d *incTableNamedUDF) FillRow(row Row) (bool, error) {
	if d.count >= d.n {
		return false, nil
	}
	d.count++
	err := SetRowValue(row, 0, d.count)
	return true, err
}

func (d *incTableNamedUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *incTableNamedUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *incTableNamedUDF) Cardinality() *CardinalityInfo {
	return nil
}

func (d *constTableUDF[T]) GetFunction() RowTableFunction {
	typeinfo, _ := NewTypeInfo(d.duckdbType)
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{typeinfo},
		},
		BindArguments: BindConstTableUDF(d.value, d.duckdbType),
	}
}

func BindConstTableUDF[T any](val T, duckdbType Type) func(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
	return func(namedArgs map[string]any, args ...interface{}) (RowTableSource, error) {
		return &constTableUDF[T]{
			count: 0,
			value: args[0].(T),
			duckdbType: duckdbType,
		}, nil
	}
}

func (d *constTableUDF[T]) Columns() []ColumnInfo {
	typeinfo, _ := NewTypeInfo(d.duckdbType)
	return []ColumnInfo{
		{Name: "result", T: typeinfo},
	}
}

func (d *constTableUDF[T]) Init() {}

func (d *constTableUDF[T]) FillRow(row Row) (bool, error) {
	if d.count >= 1 {
		return false, nil
	}
	d.count++
	err := SetRowValue(row, 0, d.value)
	return true, err
}

func (d *constTableUDF[T]) GetValue(r, c int) any {
	return d.value
}

func (d *constTableUDF[T]) GetTypes() []any {
	return []any{
		d.value,
	}
}

func (d *constTableUDF[T]) Cardinality() *CardinalityInfo {
	return nil
}

func (d *chunkIncTableUDF) GetFunction() ChunkTableFunction {
	return ChunkTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindChunkIncTableUDF,
	}
}

func BindChunkIncTableUDF(namedArgs map[string]any, args ...interface{}) (ChunkTableSource, error) {
	return &chunkIncTableUDF{
		count: 0,
		n:     args[0].(int64),
	}, nil
}

func (d *chunkIncTableUDF) Columns() []ColumnInfo {
	return []ColumnInfo{
		{Name: "result", T: Ti64},
	}
}

func (d *chunkIncTableUDF) Init() {}

func (d *chunkIncTableUDF) FillChunk(chunk DataChunk) error {
	size := 2048
	i := 0
	defer func() { _ = chunk.SetSize(i) }()
	for ; i < size; i++ {
		if d.count >= d.n {
			return nil
		}
		d.count++
		err := chunk.SetValue(0, i, d.count)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *chunkIncTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *chunkIncTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *chunkIncTableUDF) Cardinality() *CardinalityInfo {
	return nil
}

func TestTableUDF(t *testing.T) {
	for _, fun := range rowUdtfs {
		_fun := fun
		t.Run(_fun.name, func(t *testing.T) {
			singleTableUDF(t, _fun)
		})
	}
	for _, fun := range parallelRowUdtfs {
		_fun := fun
		t.Run(_fun.name, func(t *testing.T) {
			singleTableUDF(t, _fun)
		})
	}
	for _, fun := range chunkUdtfs {
		_fun := fun
		t.Run(_fun.name, func(t *testing.T) {
			singleTableUDF(t, _fun)
		})
	}
}

func singleTableUDF[T TableFunction](t *testing.T, fun UdtfTest[T]) {
	var err error
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	err = RegisterTableUDF(conn, fun.name, fun.udf.GetFunction())
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf(fun.query, fun.name))
	if err != nil {
		t.Fatal(err)
	}

	//TODO: check column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	values := fun.udf.GetTypes()
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	results := 0

	// Fetch rows
	for r := int(0); rows.Next(); r++ {
		err = rows.Scan(scanArgs...)
		if err != nil {
			t.Fatal(err.Error())
		}
		for i, value := range values {
			expected := fun.udf.GetValue(r, i)
			if !reflect.DeepEqual(expected, value) {
				err := wrongValueError{
					rowIdx:   r,
					colIdx:   i,
					colName:  columns[i],
					expected: expected,
					got:      value,
				}
				t.Log(err)
				t.Fail()
			}
		}
		results++
	}
	if results != fun.resultCount {
		t.Logf("Resultcount did not match up with the expected number of results. Expected %v, got %v", fun.resultCount, results)
		t.Fail()
	}
}

func BenchmarkRowTableUDF(b *testing.B) {
	b.StopTimer()
	var err error
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	var fun incTableUDF
	err = RegisterTableUDF(conn, "whoo", fun.GetFunction())
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
	}
}

func BenchmarkChunkTableUDF(b *testing.B) {
	b.StopTimer()
	var err error
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	var fun chunkIncTableUDF
	err = RegisterTableUDF(conn, "whoo", fun.GetFunction())
	if err != nil {
		b.Fatal(err)
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048*64)")
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
	}
}

func (wve wrongValueError) Error() string {
	return fmt.Sprintf("Wrong value at row %d, column %d(%s): Expected %v of type %[4]T, found %v of type %[5]T", wve.rowIdx, wve.colIdx, wve.colName, wve.expected, wve.got)
}
