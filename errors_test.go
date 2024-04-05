package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testErrorInternal(t *testing.T, actual error, contains []string) {
	for _, msg := range contains {
		require.Contains(t, actual.Error(), msg)
	}

	levels := strings.Count(actual.Error(), driverErrMsg)
	require.Equal(t, 1, levels)
}

func testError(t *testing.T, actual error, contains ...string) {
	testErrorInternal(t, actual, contains)
}

func TestErrOpen(t *testing.T) {
	t.Run(errParseDSN.Error(), func(t *testing.T) {
		_, err := sql.Open("duckdb", ":mem ory:")
		testError(t, err, errParseDSN.Error())
	})

	t.Run(errOpen.Error(), func(t *testing.T) {
		_, err := sql.Open("duckdb", "?readonly")
		testError(t, err, errOpen.Error(), duckdbErrMsg)
	})

	t.Run(errSetConfig.Error(), func(t *testing.T) {
		_, err := sql.Open("duckdb", "?threads=NaN")
		testError(t, err, errSetConfig.Error())
	})

	t.Run("local config option", func(t *testing.T) {
		_, err := sql.Open("duckdb", "?schema=main")
		testError(t, err, errSetConfig.Error())
	})
}

func TestErrAppender(t *testing.T) {
	t.Run(errAppenderInvalidCon.Error(), func(t *testing.T) {
		var con driver.Conn
		_, err := NewAppenderFromConn(con, "", "test")
		testError(t, err, errAppenderInvalidCon.Error())
	})

	t.Run(errAppenderClosedCon.Error(), func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)

		con, err := c.Connect(context.Background())
		require.NoError(t, err)
		require.NoError(t, con.Close())

		_, err = NewAppenderFromConn(con, "", "test")
		testError(t, err, errAppenderClosedCon.Error())
		require.NoError(t, c.Close())
	})

	t.Run(errAppenderCreation.Error(), func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)

		con, err := c.Connect(context.Background())
		require.NoError(t, err)

		_, err = NewAppenderFromConn(con, "", "does_not_exist")
		testError(t, err, errAppenderCreation.Error(), duckdbErrMsg)
		require.NoError(t, con.Close())
		require.NoError(t, c.Close())
	})

	t.Run(errAppenderDoubleClose.Error(), func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)

		_, err = sql.OpenDB(c).Exec(`CREATE TABLE tbl (i INTEGER)`)
		require.NoError(t, err)

		con, err := c.Connect(context.Background())
		require.NoError(t, err)

		a, err := NewAppenderFromConn(con, "", "tbl")
		require.NoError(t, err)
		cleanupAppender(t, c, con, a)

		err = a.Close()
		testError(t, err, errAppenderDoubleClose.Error())
	})

	t.Run(unsupportedTypeErrMsg, func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)

		_, err = sql.OpenDB(c).Exec(`CREATE TABLE test AS SELECT MAP() AS m`)
		require.NoError(t, err)

		con, err := c.Connect(context.Background())
		require.NoError(t, err)

		_, err = NewAppenderFromConn(con, "", "test")
		testError(t, err, errAppenderCreation.Error(), unsupportedTypeErrMsg)

		require.NoError(t, con.Close())
		require.NoError(t, c.Close())
	})

	t.Run(errAppenderClose.Error(), func(t *testing.T) {
		c, con, a := prepareAppender(t, `CREATE TABLE test (a VARCHAR, b VARCHAR)`)
		require.NoError(t, a.AppendRow("hello"))
		err := a.Close()

		testError(t, err, errAppenderClose.Error(), duckdbErrMsg, invalidatedAppenderMsg)
		require.NoError(t, con.Close())
		require.NoError(t, c.Close())
	})

	t.Run(errAppenderFlush.Error(), func(t *testing.T) {
		c, con, a := prepareAppender(t, `CREATE TABLE test (a VARCHAR, b VARCHAR)`)
		require.NoError(t, a.AppendRow("hello"))
		err := a.Flush()

		testError(t, err, errAppenderFlush.Error(), duckdbErrMsg, invalidatedAppenderMsg)
		cleanupAppender(t, c, con, a)
	})

	t.Run(errAppenderAppendAfterClose.Error(), func(t *testing.T) {
		c, con, a := prepareAppender(t, `CREATE TABLE test (str VARCHAR)`)
		require.NoError(t, a.Close())
		err := a.AppendRow("hello")
		testError(t, err, errAppenderAppendAfterClose.Error())
		require.NoError(t, con.Close())
		require.NoError(t, c.Close())
	})
}

func TestErrAppend(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (id BIGINT, str VARCHAR)`)

	err := a.AppendRow("hello", "world")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(false, 42)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	cleanupAppender(t, c, con, a)
}

func TestErrAppendSimpleStruct(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
			simple_struct STRUCT(A INT, B VARCHAR)
		)`)

	err := a.AppendRow(1)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow("hello")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	type other struct {
		S string
		I int
	}
	err = a.AppendRow(other{"hello", 1})
	testError(t, err, errAppenderAppendRow.Error(), structFieldErrMsg)

	err = a.AppendRow(
		wrappedSimpleStruct{
			"hello there",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	err = a.AppendRow(
		wrappedStruct{
			"hello there",
			simpleStruct{A: 0, B: "one billion ducks"},
		},
	)
	testError(t, err, errAppenderAppendRow.Error(), structFieldErrMsg)

	cleanupAppender(t, c, con, a)
}

func TestErrAppendStruct(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
			mix STRUCT(A STRUCT(L VARCHAR[]), B STRUCT(L INT[])[])
		)`)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	cleanupAppender(t, c, con, a)
}

func TestErrAppendList(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test(intSlice INT[])`)

	err := a.AppendRow([]string{"foo", "bar", "baz"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	cleanupAppender(t, c, con, a)
}

func TestErrAppendStructWithList(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test (struct_with_list STRUCT(L INT[]))`)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	l := struct{ L []string }{L: []string{"a", "b", "c"}}
	testError(t, a.AppendRow(l), errAppenderAppendRow.Error(), castErrMsg)

	cleanupAppender(t, c, con, a)
}

func TestErrAppendNestedStruct(t *testing.T) {
	c, con, a := prepareAppender(t, `
		CREATE TABLE test (
			wrapped_simple_struct STRUCT(A VARCHAR, B STRUCT(A INT, B VARCHAR)),
		)`)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	cleanupAppender(t, c, con, a)
}

func TestErrAppendNestedList(t *testing.T) {
	c, con, a := prepareAppender(t, `CREATE TABLE test(int_slice INT[][][])`)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(1)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)

	cleanupAppender(t, c, con, a)
}
