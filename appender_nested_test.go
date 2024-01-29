package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

const (
	testAppenderTableNested = `
CREATE TABLE test(
	id BIGINT,
	stringList VARCHAR[],
	intList INT[],
	nestedListInt INT[][],
	tripleNestedListInt INT[][][],
	base STRUCT(I INT, V VARCHAR),
	wrapper STRUCT(Base STRUCT(I INT, V VARCHAR)),
	topWrapper STRUCT(Wrapper STRUCT(Base STRUCT(I INT, V VARCHAR))),
	structList STRUCT(I INT, V VARCHAR)[],
	listStruct STRUCT(L INT[]),
	mix STRUCT(A STRUCT(L VARCHAR[]), B STRUCT(L INT[])[]),
	mixList STRUCT(A STRUCT(L VARCHAR[]), B STRUCT(L INT[])[])[]
)`
)

func createAppenderNestedTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec(testAppenderTableNested)
	require.NoError(t, err)
	return &res
}

type nestedDataRowInterface struct {
	stringList          []interface{}
	intList             []interface{}
	nestedListInt       []interface{}
	tripleNestedListInt []interface{}
	base                interface{}
	wrapper             interface{}
	topWrapper          interface{}
	structList          []interface{}
	listStruct          interface{}
	mix                 interface{}
	mixList             []interface{}
}

type nestedDataRow struct {
	ID                  int
	stringList          ListString
	intList             Int32List
	nestedListInt       Int32ListList
	tripleNestedListInt Int32ListListList
	base                Base
	wrapper             Wrapper
	topWrapper          TopWrapper
	structList          BaseList
	listStruct          ListInt
	mix                 Mix
	mixList             MixList
}

func (dR *nestedDataRow) Convert(i *nestedDataRowInterface) {
	dR.stringList.FillFromInterface(i.stringList)
	dR.intList.FillInnerFromInterface(i.intList)
	dR.nestedListInt.FillFromInterface(i.nestedListInt)
	dR.tripleNestedListInt.FillFromInterface(i.tripleNestedListInt)
	dR.base.FillFromInterface(i.base)
	dR.wrapper.FillFromInterface(i.wrapper)
	dR.topWrapper.FillFromInterface(i.topWrapper)
	dR.structList.ListFillFromInterface(i.structList)
	dR.listStruct.L.FillFromInterface(i.listStruct)
	dR.mix.FillFromInterface(i.mix)
	dR.mixList.ListFillFromInterface(i.mixList)
}

func TestNestedAppender(t *testing.T) {

	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	createAppenderNestedTable(db, t)
	defer db.Close()

	initRow := func(i int) nestedDataRow {
		dR := nestedDataRow{ID: i}
		dR.stringList.Fill()
		dR.intList.Fill()
		dR.nestedListInt.Fill()
		dR.tripleNestedListInt.Fill()
		dR.base.Fill(i)
		dR.wrapper.Fill(i)
		dR.topWrapper.Fill(i)
		dR.structList.ListFill()
		dR.listStruct.L.Fill()
		dR.mix.Fill()
		dR.mixList.ListFill()
		return dR
	}
	rows := make([]nestedDataRow, 100)
	for i := 0; i < 100; i++ {
		rows[i] = initRow(i)
	}

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)
	defer appender.Close()

	for _, row := range rows {
		err := appender.AppendRow(
			row.ID,
			row.stringList.L,
			row.intList,
			row.nestedListInt,
			row.tripleNestedListInt,
			row.base,
			row.wrapper,
			row.topWrapper,
			row.structList,
			row.listStruct,
			row.mix,
			row.mixList,
		)
		require.NoError(t, err)
	}
	err = appender.Flush()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test ORDER BY id`,
	)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := nestedDataRow{}
		interfaces := nestedDataRowInterface{}
		err := res.Scan(
			&r.ID,
			&interfaces.stringList,
			&interfaces.intList,
			&interfaces.nestedListInt,
			&interfaces.tripleNestedListInt,
			&interfaces.base,
			&interfaces.wrapper,
			&interfaces.topWrapper,
			&interfaces.structList,
			&interfaces.listStruct,
			&interfaces.mix,
			&interfaces.mixList,
		)
		require.NoError(t, err)
		r.Convert(&interfaces)
		require.Equal(t, rows[i], r)
		i++
	}
	// Ensure that the number of fetched rows equals the number of inserted rows.
	require.Equal(t, 100, i)
}

func TestAppenderNullList(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`CREATE OR REPLACE TABLE test(intSlice INT[][][])`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	// An empty list should also be able to initialize the logical types
	err = appender.AppendRow(
		Int32ListListList{{{}}},
	)

	err = appender.AppendRow(
		Int32ListListList{{{1, 2, 3}, {4, 5, 6}}},
	)

	err = appender.AppendRow(
		Int32ListListList{{{1}, nil}},
	)

	err = appender.AppendRow(
		nil,
	)

	err = appender.AppendRow(
		Int32ListListList{nil, {{2}}},
	)

	err = appender.AppendRow(
		Int32ListListList{{nil, {3}}, {{4}}},
	)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT intSlice FROM test`)
	require.NoError(t, err)
	defer res.Close()

	var strResult []string
	strResult = append(strResult, "[[[]]]")
	strResult = append(strResult, "[[[1 2 3] [4 5 6]]]")
	strResult = append(strResult, "[[[1] <nil>]]")
	strResult = append(strResult, "<nil>")
	strResult = append(strResult, "[<nil> [[2]]]")
	strResult = append(strResult, "[[<nil> [3]] [[4]]]")

	i := 0
	for res.Next() {
		var strS string
		var intS []interface{}
		err := res.Scan(
			&intS,
		)
		if err != nil {
			strS = "<nil>"
		} else {
			strS = fmt.Sprintf("%v", intS)
		}

		if strResult[i] != strS {
			panic(fmt.Sprintf("row %d: expected %v, got %v", i, strResult[i], strS))
		}
		i++
	}

}

// Only the base layer is tested, since a struct cannot be nil, so it is not possible to test
// a nested struct with a nil base.
func TestAppenderNullStruct(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
			base STRUCT(I INT, V VARCHAR),
    	)
    `)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Base{1, "hello"},
	)

	err = appender.AppendRow(
		nil,
	)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT * FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var intS interface{}
		err := res.Scan(
			&intS,
		)
		if i == 0 {
			require.NoError(t, err)
		} else if i == 1 {
			require.Equal(t, nil, intS)
		}
		i++
	}
}

func TestAppenderNestedListMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
				intSlice INT[][][],
    	)
    `)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Int32ListListList{{{}}},
	)
	require.NoError(t, err)

	err = appender.AppendRow(
		Int32List{1, 2, 3},
	)
	require.Error(t, err, "expected: \"int32[][][]\" \nactual: \"int32[]\"")

	err = appender.AppendRow(
		1,
	)
	require.ErrorContains(t, err, "expected: \"int32[][][]\" \nactual: \"int64\"")

	err = appender.AppendRow(
		Int32ListList{{1, 2, 3}, {4, 5, 6}},
	)
	require.ErrorContains(t, err, "expected: \"int32[][][]\" \nactual: \"int32[][]\"")

	err = appender.Close()
	require.NoError(t, err)

	// test incorrect nested type insert (double nested into single nested)
	_, err = db.Exec(`
		CREATE TABLE test2(
		    				intSlice INT[]
		    	)
		    `)
	require.NoError(t, err)
	defer db.Close()

	conn, err = c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err = NewAppenderFromConn(conn, "", "test2")
	require.NoError(t, err)

	err = appender.AppendRow(
		Int32List{},
	)
	require.NoError(t, err)

	var l ListString
	l.Fill()
	err = appender.AppendRow(
		l.L,
	)
	require.ErrorContains(t, err, "expected: \"int32[]\" \nactual: \"string[]\"")

	err = appender.AppendRow(
		Int32ListList{{1, 2, 3}, {4, 5, 6}},
	)
	require.ErrorContains(t, err, "expected: \"int32[]\" \nactual: \"int32[][]\"")

	err = appender.Close()
	require.NoError(t, err)
}

func TestAppenderNestedStructMismatch(t *testing.T) {
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	db := sql.OpenDB(c)
	_, err = db.Exec(`
		CREATE TABLE test(
				base STRUCT(I INT, V VARCHAR),
		)
	`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Base{1, "hello"},
	)
	require.NoError(t, err)

	//err = appender.AppendRow(
	//	1,
	//)
	//require.ErrorContains(t, err, "expected: \"struct\" \nactual: \"int64\"")
	//
	//err = appender.AppendRow(
	//	"hello",
	//)
	//require.ErrorContains(t, err, "expected: \"struct\" \nactual: \"string\"")

	type other struct {
		S string
		I int
	}

	err = appender.AppendRow(
		other{"hello", 1},
	)
	require.NoError(t, err, "expected: \"{int32, string}\" \nactual: \"{string, int64}\"")

	err = appender.Close()
	require.NoError(t, err)
}
