package duckdb

import (
	"context"
	"database/sql"
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
	_, err = db.Exec(`CREATE TABLE test(intSlice INT[][])`)
	require.NoError(t, err)
	defer db.Close()

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", "test")
	require.NoError(t, err)

	err = appender.AppendRow(
		Int32ListList{{}}, // empty list should also work
	)

	err = appender.AppendRow(
		Int32ListList{{1, 2, 3}, {4, 5, 6}},
	)

	err = appender.AppendRow(
		nil,
	)

	err = appender.AppendRow(
		Int32ListList{{1, 2, 3}, nil},
	)

	err = appender.AppendRow(
		Int32ListList{nil, {4, 5, 6}},
	)

	err = appender.Close()
	require.NoError(t, err)

	res, err := db.QueryContext(
		context.Background(),
		`SELECT intSlice FROM test`)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		var intS []interface{}
		err := res.Scan(
			&intS,
		)
		if i == 0 || i == 1 {
			require.NoError(t, err)
		} else if i == 2 {
			require.Error(t, err)
		} else if i == 3 {
			require.NoError(t, err)
			if intS != nil {
				c := intS[0].([]interface{})
				if c[0].(int32) != 1 || c[1].(int32) != 2 || c[2].(int32) != 3 {
					panic("expected [1, 2, 3]")
				}
				if intS[1] != nil {
					panic("expected nil")
				}
			} else {
				panic("expected non-nil")
			}
		} else if i == 4 {
			require.NoError(t, err)
			if intS != nil {
				if intS[0] != nil {
					panic("expected nil")
				}
				// cast to []int32
				c := intS[1].([]interface{})
				if c[0].(int32) != 4 || c[1].(int32) != 5 || c[2].(int32) != 6 {
					panic("expected [4, 5, 6]")
				}
			} else {
				panic("expected non-nil")
			}
		}
		i++
	}
}

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
