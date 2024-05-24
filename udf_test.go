package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
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

	testTableFunction interface {
		TableFunctionProvider
		GetValue(r, c int) any
		GetTypes() []any
	}

	udfTest struct {
		udf   testTableFunction
		name  string
		query string
	}

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
		//		T     testing.T
	}

	incTableNamedUDF struct {
		n     int64
		count int64
	}
)

var (
	tudfs = []udfTest{
		{
			udf:   &incTableUDF{},
			name:  "incTableUDTF",
			query: "SELECT * FROM %s(2048)",
		},
		{
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
)

func (d *incTableUDF) Config() TableFunctionConfig {
	return TableFunctionConfig{
		Arguments: []Type{NewDuckdbType[int64]()},
	}
}

func (d *incTableUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (TableFunction, []ColumnMetaData, error) {
	d.count = 0
	d.n = args[0].(int64)
	return d, []ColumnMetaData{
		{Name: "result", T: NewDuckdbType[int64]()},
	}, nil
}

func (d *incTableUDF) Init() TableFunctionInitData {
	return TableFunctionInitData{
		MaxThreads: 1,
	}
}

func (d *incTableUDF) FillRow(row Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}
	d.count++
	SetRowValue(row, 0, d.count)
	return true, nil
}

func (d *incTableUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *incTableUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *incTableUDF) Cardinality() *CardinalityData {
	return nil
}

func (d *structTableUDF) Config() TableFunctionConfig {
	return TableFunctionConfig{
		Arguments: []Type{NewDuckdbType[int64]()},
	}
}

func (d *structTableUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (TableFunction, []ColumnMetaData, error) {
	d.count = 0
	d.n = args[0].(int64)

	t, _ := TryNewDuckdbType[structTableUDFT]()
	return d, []ColumnMetaData{
		{Name: "result", T: t},
	}, nil
}

func (d *structTableUDF) Init() TableFunctionInitData {
	return TableFunctionInitData{
		MaxThreads: 1,
	}
}

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

func (d *structTableUDF) Cardinality() *CardinalityData {
	return nil
}

func (d *pushdownTableUDF) Config() TableFunctionConfig {
	return TableFunctionConfig{
		Arguments:          []Type{NewDuckdbType[int64]()},
		Pushdownprojection: true,
	}
}

func (d *pushdownTableUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (TableFunction, []ColumnMetaData, error) {
	d.count = 0
	d.n = args[0].(int64)
	return d, []ColumnMetaData{
		{Name: "result", T: NewDuckdbType[int64]()},
		{Name: "result2", T: NewDuckdbType[int64]()},
	}, nil
}

func (d *pushdownTableUDF) Init() TableFunctionInitData {
	return TableFunctionInitData{
		MaxThreads: 1,
	}
}

func (d *pushdownTableUDF) FillRow(row Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}

	var err error
	if row.IsProjected(0) {
		err = fmt.Errorf("Column 0 is projected while it should not be")
	}
	d.count++
	if _err := SetRowValue(row, 0, d.count); err != nil {
		err = _err
	}
	if _err := SetRowValue(row, 1, d.count); err != nil {
		err = _err
	}
	return true, nil
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

func (d *pushdownTableUDF) Cardinality() *CardinalityData {
	return nil
}

func (d *incTableNamedUDF) Config() TableFunctionConfig {
	return TableFunctionConfig{
		NamedArguments: map[string]Type{"ARG": NewDuckdbType[int64]()},
	}
}

func (d *incTableNamedUDF) BindArguments(namedArgs map[string]any, args ...interface{}) (TableFunction, []ColumnMetaData, error) {
	d.count = 0
	fmt.Println(namedArgs)
	d.n = namedArgs["ARG"].(int64)
	return d, []ColumnMetaData{
		{Name: "result", T: NewDuckdbType[int64]()},
	}, nil
}

func (d *incTableNamedUDF) Init() TableFunctionInitData {
	return TableFunctionInitData{
		MaxThreads: 1,
	}
}

func (d *incTableNamedUDF) FillRow(row Row) (bool, error) {
	if d.count > d.n {
		return false, nil
	}
	d.count++
	SetRowValue(row, 0, d.count)
	return true, nil
}

func (d *incTableNamedUDF) GetValue(r, c int) any {
	return int64(r + 1)
}

func (d *incTableNamedUDF) GetTypes() []any {
	return []any{
		int(0),
	}
}

func (d *incTableNamedUDF) Cardinality() *CardinalityData {
	return nil
}

func TestTableUDF(t *testing.T) {
	for _, fun := range tudfs {
		_fun := fun
		t.Run(_fun.name, func(t *testing.T) {
			singleTableUDF(t, _fun)
		})
	}
}

func singleTableUDF(t *testing.T, fun udfTest) {
	var err error
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	RegisterTableUDF(conn, fun.name, fun.udf)
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

func BenchmarkTableUDF(b *testing.B) {
	b.StopTimer()
	var err error
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	conn, _ := db.Conn(context.Background())
	var fun incTableUDF
	RegisterTableUDF(conn, "whoo", &fun)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		rows, err := db.QueryContext(context.Background(), "SELECT * FROM whoo(2048)")
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()
	}
}

func (wve wrongValueError) Error() string {
	return fmt.Sprintf("Wrong value at row %d, column %d(%s): Expected %v of type %[4]T, found %v of type %[5]T", wve.rowIdx, wve.colIdx, wve.colName, wve.expected, wve.got)
}
