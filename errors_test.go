package duckdb

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

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

func TestErrConnect(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	t.Run(errParseDSN.Error(), func(t *testing.T) {
		db, err := openDbWrapper(t, counters, true, `:mem ory:`)
		defer closeDbWrapper(t, counters, db)
		testError(t, err, errParseDSN.Error())
	})

	t.Run(errConnect.Error(), func(t *testing.T) {
		db, err := openDbWrapper(t, counters, true, `?readonly`)
		defer closeDbWrapper(t, counters, db)
		testError(t, err, errConnect.Error())
	})

	t.Run(errSetConfig.Error(), func(t *testing.T) {
		db, err := openDbWrapper(t, counters, true, `?threads=NaN`)
		defer closeDbWrapper(t, counters, db)
		testError(t, err, errSetConfig.Error())
	})

	t.Run("local config option", func(t *testing.T) {
		db, err := openDbWrapper(t, counters, true, `?schema=main`)
		defer closeDbWrapper(t, counters, db)
		testError(t, err, errSetConfig.Error())
	})
}

func TestErrNestedMap(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	db, _ := openDbWrapper(t, counters, false, ``)

	var m Map
	err := db.QueryRow(`SELECT MAP([MAP([1], [1]), MAP([2], [2])], ['a', 'e'])`).Scan(&m)
	testError(t, err, errUnsupportedMapKeyType.Error())
	require.NoError(t, db.Close())
}

func TestErrAppender(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	t.Run(errInvalidCon.Error(), func(t *testing.T) {
		var conn driver.Conn
		a, err := newAppenderWrapper(t, counters, true, &conn, "", "test")
		defer closeAppenderWrapper(t, counters, false, a)
		testError(t, err, errInvalidCon.Error())
	})

	t.Run(errClosedCon.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, counters, ``, nil)
		defer closeConnectorWrapper(t, counters, c)

		conn := openConnectorConnWrapper(t, counters, c)
		closeDriverConnWrapper(t, counters, &conn)

		a, err := newAppenderWrapper(t, counters, true, &conn, "", "test")
		defer closeAppenderWrapper(t, counters, false, a)
		testError(t, err, errClosedCon.Error())
	})

	t.Run(errAppenderCreation.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, counters, ``, nil)
		defer closeConnectorWrapper(t, counters, c)

		conn := openConnectorConnWrapper(t, counters, c)
		defer closeDriverConnWrapper(t, counters, &conn)

		a, err := newAppenderWrapper(t, counters, true, &conn, "", "does_not_exist")
		defer closeAppenderWrapper(t, counters, false, a)
		testError(t, err, errAppenderCreation.Error())
	})

	t.Run(errAppenderDoubleClose.Error(), func(t *testing.T) {
		c := newConnectorWrapper(t, counters, ``, nil)
		_, err := sql.OpenDB(c).Exec(`CREATE TABLE tbl (i INTEGER)`)
		require.NoError(t, err)
		conn := openConnectorConnWrapper(t, counters, c)

		a, _ := newAppenderWrapper(t, counters, false, &conn, "", "tbl")
		cleanupAppender(t, counters, c, conn, a)

		err = a.Close()
		testError(t, err, errAppenderDoubleClose.Error())
	})

	t.Run(unsupportedTypeErrMsg, func(t *testing.T) {
		c := newConnectorWrapper(t, counters, ``, nil)
		defer closeConnectorWrapper(t, counters, c)

		_, err := sql.OpenDB(c).Exec(`CREATE TABLE test (bit_col BIT)`)
		require.NoError(t, err)

		conn := openConnectorConnWrapper(t, counters, c)
		defer closeDriverConnWrapper(t, counters, &conn)

		a, err := newAppenderWrapper(t, counters, true, &conn, "", "test")
		defer closeAppenderWrapper(t, counters, false, a)
		testError(t, err, errAppenderCreation.Error(), unsupportedTypeErrMsg)
	})

	t.Run(columnCountErrMsg, func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (a VARCHAR, b VARCHAR)`)
		defer cleanupAppender(t, counters, c, conn, a)
		err := a.AppendRow("hello")
		testError(t, err, errAppenderAppendRow.Error(), columnCountErrMsg)
	})

	t.Run(errAppenderAppendAfterClose.Error(), func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (str VARCHAR)`)
		_ = closeAppenderWrapper(t, counters, false, a)
		defer closeDriverConnWrapper(t, counters, &conn)
		defer closeConnectorWrapper(t, counters, c)

		err := a.AppendRow("hello")
		testError(t, err, errAppenderAppendAfterClose.Error())
	})

	t.Run(errAppenderFlush.Error(), func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (c1 INTEGER PRIMARY KEY)`)
		defer closeDriverConnWrapper(t, counters, &conn)
		defer closeConnectorWrapper(t, counters, c)

		require.NoError(t, a.AppendRow(int32(1)))
		require.NoError(t, a.AppendRow(int32(1)))
		err := a.Flush()
		testError(t, err, errAppenderFlush.Error())

		err = closeAppenderWrapper(t, counters, true, a)
		testError(t, err, errAppenderClose.Error())
	})

	t.Run(errAppenderClose.Error(), func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (c1 INTEGER PRIMARY KEY)`)
		defer closeDriverConnWrapper(t, counters, &conn)
		defer closeConnectorWrapper(t, counters, c)

		require.NoError(t, a.AppendRow(int32(1)))
		require.NoError(t, a.AppendRow(int32(1)))

		err := closeAppenderWrapper(t, counters, true, a)
		testError(t, err, errAppenderClose.Error())
	})

	t.Run(errUnsupportedMapKeyType.Error(), func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (m MAP(INT[], STRUCT(v INT)))`)
		defer cleanupAppender(t, counters, c, conn, a)
		err := a.AppendRow(nil)
		testError(t, err, errAppenderAppendRow.Error(), errUnsupportedMapKeyType.Error())
	})

	t.Run(invalidInputErrMsg, func(t *testing.T) {
		c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (col INT[3])`)
		defer cleanupAppender(t, counters, c, conn, a)
		err := a.AppendRow([]int32{1, 2})
		testError(t, err, errAppenderAppendRow.Error(), invalidInputErrMsg)
	})
}

func TestErrAppend(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (id BIGINT, str VARCHAR)`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow("hello", "world")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(false, 42)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendDecimal(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (d DECIMAL(8, 2))`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow(Decimal{Width: 9, Scale: 2})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(Decimal{Width: 8, Scale: 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendEnum(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, testTypesEnumSQL+";"+`CREATE TABLE test (e my_enum)`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow("3")
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendSimpleStruct(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `
		CREATE TABLE test (
			simple_struct STRUCT(A INT, B VARCHAR)
		)`)
	defer cleanupAppender(t, counters, c, conn, a)

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
}

func TestErrAppendDuplicateStruct(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `
		CREATE TABLE test (
			duplicate_struct STRUCT(Duplicate INT)
		)`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow(duplicateKeyStruct{1, 2})
	testError(t, err, errAppenderAppendRow.Error(), duplicateNameErrMsg)
}

func TestErrAppendStruct(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `
		CREATE TABLE test (
			mix STRUCT(a STRUCT(L VARCHAR[]), B STRUCT(L INT[])[])
		)`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendList(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `CREATE TABLE test(intSlice INT[])`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow([]string{"foo", "bar", "baz"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendStructWithList(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (struct_with_list STRUCT(L INT[]))`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	l := struct{ L []string }{L: []string{"a", "b", "c"}}
	testError(t, a.AppendRow(l), errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendNestedStruct(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `
		CREATE TABLE test (
			wrapped_simple_struct STRUCT(a VARCHAR, B STRUCT(A INT, B VARCHAR)),
		)`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow(simpleStruct{1, "hello"})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppendNestedList(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	c, conn, a := prepareAppender(t, counters, `CREATE TABLE test(int_slice INT[][][])`)
	defer cleanupAppender(t, counters, c, conn, a)

	err := a.AppendRow([]int32{1, 2, 3})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow(1)
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
	err = a.AppendRow([][]int32{{1, 2, 3}, {4, 5, 6}})
	testError(t, err, errAppenderAppendRow.Error(), castErrMsg)
}

func TestErrAppenderTSConversion(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	testCases := []string{"TIMESTAMP_NS", "TIMESTAMP", "TIMESTAMPTZ"}
	for _, tc := range testCases {
		t.Run(tc+" conversion error", func(t *testing.T) {
			c, conn, a := prepareAppender(t, counters, `CREATE TABLE test (t `+tc+`)`)
			defer cleanupAppender(t, counters, c, conn, a)

			tsLess := time.Date(-290407, time.January, 1, 15, 0o4, 5, 123456, time.UTC)
			err := a.AppendRow(tsLess)
			testError(t, err, errAppenderAppendRow.Error(), convertErrMsg)

			tsGreater := time.Date(294346, time.January, 1, 15, 0o4, 5, 123456, time.UTC)
			err = a.AppendRow(tsGreater)
			testError(t, err, errAppenderAppendRow.Error(), convertErrMsg)
		})
	}
}

func TestErrAPISetValue(t *testing.T) {
	t.Parallel()

	var chunk DataChunk
	err := chunk.SetValue(1, 42, "hello")
	testError(t, err, errAPI.Error(), columnCountErrMsg)
	err = SetChunkValue(chunk, 1, 42, "hello")
	testError(t, err, errAPI.Error(), columnCountErrMsg)
}

func TestDuckDBErrors(t *testing.T) {
	t.Parallel()
	counters := &callCounters{}
	defer verifyCounters(t, counters)

	db, err := openDbWrapper(t, counters, false, ``)
	defer closeDbWrapper(t, counters, db)

	createTable(t, db, `CREATE TABLE duckdb_error_test(bar VARCHAR UNIQUE, baz INT32, u_1 UNION("string" VARCHAR))`)
	_, err = db.Exec(`INSERT INTO duckdb_error_test(bar, baz) VALUES ('bar', 0)`)
	require.NoError(t, err)

	testCases := []struct {
		tpl    string
		errTyp ErrorType
	}{
		{
			tpl:    `SELECT * FROM not_exist WHERE baz=0`,
			errTyp: ErrorTypeCatalog,
		},
		{
			tpl:    `SELECT * FROM duckdb_error_test WHERE col=?`,
			errTyp: ErrorTypeBinder,
		},
		{
			tpl:    `SELEC * FROM duckdb_error_test baz=0`,
			errTyp: ErrorTypeParser,
		},
		{
			tpl:    `INSERT INTO duckdb_error_test(bar, baz) VALUES ('bar', 1)`,
			errTyp: ErrorTypeConstraint,
		},
		{
			tpl:    `INSERT INTO duckdb_error_test(bar, baz) VALUES ('foo', 18446744073709551615)`,
			errTyp: ErrorTypeConversion,
		},
		{
			tpl:    `INSTALL not_exist`,
			errTyp: ErrorTypeHTTP,
		},
		{
			tpl:    `LOAD not_exist`,
			errTyp: ErrorTypeIO,
		},
		{
			tpl:    `SELECT array_length(array_value(array_value(1, 2, 2), array_value(3, 4, 3)), 3)`,
			errTyp: ErrorTypeOutOfRange,
		},
		{
			tpl:    `SELECT '010110'::BIT & '11000'::BIT`,
			errTyp: ErrorTypeInvalidInput,
		},
		{
			tpl:    `SET external_threads=-1`,
			errTyp: ErrorTypeInvalidInput,
		},
		{
			tpl:    `CREATE UNIQUE INDEX idx ON duckdb_error_test(u_1)`,
			errTyp: ErrorTypeInvalidType,
		},
	}
	for _, tc := range testCases {
		_, err = db.Exec(tc.tpl)
		var de *Error
		ok := errors.As(err, &de)
		if !ok {
			require.Fail(t, "error type is not (*duckdb.Error)", "tql: %s\ngot: %#v", tc.tpl, err)
		}
		require.Equal(t, de.Type, tc.errTyp, "tpl: %s\nactual error msg: %s", tc.tpl, de.Msg)
	}
}

func TestDuckDBErrorsCornerCases(t *testing.T) {
	testCases := []*Error{
		{
			Msg:  "",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Unknown",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Error: xxx",
			Type: ErrorTypeUnknownType,
		},
		// Prefix testing.
		{
			Msg:  "Invalid Error: xxx",
			Type: ErrorTypeInvalid,
		},
		{
			Msg:  "Invalid Input Error: xxx",
			Type: ErrorTypeInvalidInput,
		},
		{
			Msg:  "Invalid Configuration Error: xxx",
			Type: ErrorTypeInvalidConfiguration,
		},
	}

	for _, tc := range testCases {
		var err *Error
		errors.As(getDuckDBError(tc.Msg), &err)
		require.Equal(t, tc, err)
	}
}

type wrappedDuckDBError struct {
	e *Error
}

func (w *wrappedDuckDBError) Error() string {
	return w.e.Error()
}

func (w *wrappedDuckDBError) Unwrap() error {
	return w.e
}

func TestGetDuckDBErrorIs(t *testing.T) {
	const errMsg = "Out of Range Error: Overflow"
	outOfRangeErr1 := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  errMsg,
	}
	outOfRangeErr1Copy := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  errMsg,
	}
	outOfRangeErr2 := &Error{
		Type: ErrorTypeOutOfRange,
		Msg:  "Out of Range Error: array_length dimension '3' out of range (min: '1', max: '2')",
	}
	invalidInputErr := &Error{
		Type: ErrorTypeInvalidInput,
		Msg:  "Invalid Input Error: Map keys can not be NULL",
	}

	require.ErrorIs(t, outOfRangeErr1Copy, outOfRangeErr1)
	require.ErrorIs(t, &wrappedDuckDBError{outOfRangeErr1Copy}, outOfRangeErr1)
	require.NotErrorIs(t, outOfRangeErr2, outOfRangeErr1)
	require.NotErrorIs(t, invalidInputErr, outOfRangeErr1)
	require.NotErrorIs(t, errors.New(errMsg), outOfRangeErr1)
}
