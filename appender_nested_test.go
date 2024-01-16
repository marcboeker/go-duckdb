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

type dataRowInterface struct {
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

type dataRow struct {
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

func (dR *dataRow) Convert(i *dataRowInterface) {
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

	initRow := func(i int) dataRow {
		dR := dataRow{ID: i}
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
	rows := make([]dataRow, 100)
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
		context.Background(), `
			SELECT * FROM test ORDER BY id
    `)
	require.NoError(t, err)
	defer res.Close()

	i := 0
	for res.Next() {
		r := dataRow{}
		interfaces := dataRowInterface{}
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
	require.Equal(t, i, 100)
}
