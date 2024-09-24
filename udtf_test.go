package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"testing"
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

	rowUDFTest[T TableFunction] struct {
		udf   testTableFunction[T]
		name  string
		query string
	}

	incTableUDF struct {
		n     int64
		count int64
	}

	parallelIncTableUDF struct {
		lock    *sync.Mutex
		claimed int64
		n       int64
	}

	structTableUDF struct {
		n     int64
		count int64
	}

	structTableUDFT struct {
		I int64
	}

	parallelIncTableLocal struct {
		start int64
		end   int64
	}

	pushdownTableUDF struct {
		n     int64
		count int64
	}

	incTableNamedUDF struct {
		n     int64
		count int64
	}

	chunkIncTableUDF struct {
		n     int64
		count int64
	}
)

var (
	rowUdtfs = []rowUDFTest[RowTableFunction]{
		{
			udf:   &incTableUDF{},
			name:  "incTableUDTF",
			query: "SELECT * FROM %s(2048)",
		},
		{
			udf:   &parallelIncTableUDF{},
			name:  "incTableUDTF",
			query: "SELECT * FROM %s(2048) ORDER BY result",
		}, {
			udf:   &structTableUDF{},
			name:  "structTableUDTF",
			query: "SELECT * FROM %s(2048)",
		},
		{
			udf:   &pushdownTableUDF{},
			name:  "pushdownTableUDTF",
			query: "SELECT result2 FROM %s(2048)",
		},
		{
			udf:   &incTableNamedUDF{},
			name:  "incTableNamedUDTF",
			query: "SELECT * FROM %s(ARG=2048)",
		},
	}
	chunkUdtfs = []rowUDFTest[ChunkTableFunction]{
		{
			udf:   &chunkIncTableUDF{},
			name:  "incTableUDTF",
			query: "SELECT * FROM %s(2048)",
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
	if d.count > d.n {
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
	return parallelIncTableLocal{
		start: 0,
		end:   0,
	}
}

func (d *parallelIncTableUDF) FillRow(localState any, row Row) (bool, error) {
	state := localState.(parallelIncTableLocal)
	if state.start > state.end {
		// claim a new "work" unit
		d.lock.Lock()
		remaining := d.claimed - d.n
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

func (d *parallelIncTableUDF) GetFunction() RowTableFunction {
	return RowTableFunction{
		Config: TableFunctionConfig{
			Arguments: []TypeInfo{Ti64},
		},
		BindArguments: BindIncTableUDF,
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
	if d.count > d.n {
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
	if d.count > d.n {
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
	if d.count > d.n {
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
	size := chunk.GetSize()
	i := 0
	defer func() { _ = chunk.SetSize(i) }()
	for ; i < size; i++ {
		if d.count > d.n {
			return nil
		}
		d.count++
		err := chunk.SetValue(i, 0, d.count)
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
	for _, fun := range chunkUdtfs {
		_fun := fun
		t.Run(_fun.name, func(t *testing.T) {
			singleTableUDF(t, _fun)
		})
	}
}

func singleTableUDF[T TableFunction](t *testing.T, fun rowUDFTest[T]) {
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
