package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPrepareQuery(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.Prepare(`SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err := prepared.Query(0)
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
	require.NoError(t, err)
	res, err = prepared.Query(0)
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn interface{}) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE baz = ?`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_SELECT, stmtType)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, innerErr = stmt.ParamType(2)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		r, innerErr := stmt.QueryBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.NoError(t, innerErr)

		// Don't immediately close the rows to trigger an active rows error.
		r, innerErr = stmt.QueryBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		badRows, innerErr := stmt.QueryBound(context.Background())
		require.ErrorIs(t, innerErr, errActiveRows)
		require.Nil(t, badRows)

		badResults, innerErr := stmt.ExecBound(context.Background())
		require.ErrorIs(t, innerErr, errActiveRows)
		require.Nil(t, badResults)

		require.NoError(t, r.Close())
		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareQueryPositional(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.Prepare(`SELECT $1, $2 AS foo WHERE foo = $2`)
	require.NoError(t, err)

	var foo, bar int
	row := prepared.QueryRow(1, 2)
	require.NoError(t, err)
	require.NoError(t, row.Scan(&foo, &bar))
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $2 AND baz = $1`)
	require.NoError(t, err)
	res, err := prepared.Query(0, "hello")
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn interface{}) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `UPDATE foo SET bar = $2 WHERE baz = $1`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_UPDATE, stmtType)

		paramName, innerErr := stmt.ParamName(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramName, innerErr = stmt.ParamName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "1", paramName)

		paramName, innerErr = stmt.ParamName(2)
		require.NoError(t, innerErr)
		require.Equal(t, "2", paramName)

		paramName, innerErr = stmt.ParamName(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_INTEGER, paramType)

		paramType, innerErr = stmt.ParamType(2)
		require.NoError(t, innerErr)
		require.Equal(t, TYPE_VARCHAR, paramType)

		paramType, innerErr = stmt.ParamType(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		r, innerErr := stmt.ExecBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.NoError(t, innerErr)

		r, innerErr = stmt.ExecBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, innerErr = stmt.ParamName(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, "", paramName)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		innerErr = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}, {Ordinal: 2, Value: "hello"}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestPrepareQueryNamed(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	prepared, err := db.PrepareContext(context.Background(), `SELECT $foo, $bar, $baz, $foo`)
	require.NoError(t, err)

	var foo, bar, foo2 int
	var baz string
	row := prepared.QueryRow(sql.Named("baz", "x"), sql.Named("foo", 1), sql.Named("bar", 2))
	require.NoError(t, row.Scan(&foo, &bar, &baz, &foo2))
	require.Equal(t, 1, foo)
	require.Equal(t, 2, bar)
	require.Equal(t, "x", baz)
	require.Equal(t, 1, foo2)

	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(context.Background(), `SELECT * FROM foo WHERE bar = $bar AND baz = $baz`)
	require.NoError(t, err)
	res, err := prepared.Query(sql.Named("bar", "hello"), sql.Named("baz", 0))
	require.NoError(t, err)
	closeRowsWrapper(t, res)
	closePreparedWrapper(t, prepared)

	// Access the raw connection and statement.
	err = conn.Raw(func(driverConn interface{}) error {
		innerConn := driverConn.(*Conn)
		s, innerErr := innerConn.PrepareContext(context.Background(), `INSERT INTO foo VALUES ($bar, $baz)`)
		require.NoError(t, innerErr)
		stmt := s.(*Stmt)

		stmtType, innerErr := stmt.StatementType()
		require.NoError(t, innerErr)
		require.Equal(t, STATEMENT_TYPE_INSERT, stmtType)

		paramName, innerErr := stmt.ParamName(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramName, innerErr = stmt.ParamName(1)
		require.NoError(t, innerErr)
		require.Equal(t, "bar", paramName)

		paramName, innerErr = stmt.ParamName(2)
		require.NoError(t, innerErr)
		require.Equal(t, "baz", paramName)

		paramName, innerErr = stmt.ParamName(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, "", paramName)

		paramType, innerErr := stmt.ParamType(0)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		paramType, innerErr = stmt.ParamType(1)
		require.Equal(t, TYPE_VARCHAR, paramType)
		require.NoError(t, innerErr)

		paramType, innerErr = stmt.ParamType(2)
		require.Equal(t, TYPE_INTEGER, paramType)
		require.NoError(t, innerErr)

		paramType, innerErr = stmt.ParamType(3)
		require.ErrorContains(t, innerErr, paramIndexErrMsg)
		require.Equal(t, TYPE_INVALID, paramType)

		r, innerErr := stmt.ExecBound(context.Background())
		require.Nil(t, r)
		require.ErrorIs(t, innerErr, errNotBound)

		innerErr = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.NoError(t, innerErr)

		r, innerErr = stmt.ExecBound(context.Background())
		require.NoError(t, innerErr)
		require.NotNil(t, r)

		require.NoError(t, stmt.Close())

		stmtType, innerErr = stmt.StatementType()
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

		paramName, innerErr = stmt.ParamName(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, "", paramName)

		paramType, innerErr = stmt.ParamType(1)
		require.ErrorIs(t, innerErr, errClosedStmt)
		require.Equal(t, TYPE_INVALID, paramType)

		innerErr = stmt.Bind([]driver.NamedValue{{Name: "bar", Value: "hello"}, {Name: "baz", Value: 0}})
		require.ErrorIs(t, innerErr, errCouldNotBind)
		require.ErrorIs(t, innerErr, errClosedStmt)
		return nil
	})
	require.NoError(t, err)
}

func TestUninitializedStmt(t *testing.T) {
	stmt := &Stmt{}

	stmtType, err := stmt.StatementType()
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, STATEMENT_TYPE_INVALID, stmtType)

	paramType, err := stmt.ParamType(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, TYPE_INVALID, paramType)

	paramName, err := stmt.ParamName(1)
	require.ErrorIs(t, err, errUninitializedStmt)
	require.Equal(t, "", paramName)

	err = stmt.Bind([]driver.NamedValue{{Ordinal: 1, Value: 0}})
	require.ErrorIs(t, err, errCouldNotBind)
	require.ErrorIs(t, err, errUninitializedStmt)

	_, err = stmt.ExecBound(context.Background())
	require.ErrorIs(t, err, errNotBound)
}

func TestPrepareWithError(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	testCases := []struct {
		sql string
		err string
	}{
		{
			sql: `SELECT * FROM tbl WHERE baz = ?`,
			err: `Table with name tbl does not exist`,
		},
		{
			sql: `SELECT * FROM foo WHERE col = ?`,
			err: `Referenced column "col" not found in FROM clause`,
		},
		{
			sql: `SELECT * FROM foo col = ?`,
			err: `syntax error at or near "="`,
		},
	}
	for _, tc := range testCases {
		prepared, err := db.Prepare(tc.sql)
		if err != nil {
			var dbErr *Error
			if !errors.As(err, &dbErr) {
				require.Fail(t, "error type is not (*duckdb.Error)")
			}
			require.ErrorContains(t, err, tc.err)
		}
		closePreparedWrapper(t, prepared)
	}
}

func TestPreparePivot(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE cities(country VARCHAR, name VARCHAR, year INT, population INT)`)
	_, err := db.ExecContext(ctx, `INSERT INTO cities VALUES ('NL', 'Netherlands', '2020', '42')`)
	require.NoError(t, err)

	prepared, err := db.Prepare(`PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	var country, name string
	var population int
	row := prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)

	prepared, err = db.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)

	// Prepare on a connection.
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	prepared, err = conn.PrepareContext(ctx, `PIVOT cities ON year USING SUM(population)`)
	require.NoError(t, err)

	row = prepared.QueryRow()
	require.NoError(t, row.Scan(&country, &name, &population))
	require.Equal(t, "NL", country)
	require.Equal(t, "Netherlands", name)
	require.Equal(t, 42, population)
	closePreparedWrapper(t, prepared)
}

func TestBindWithoutResolvedParams(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	d := time.Date(2022, 0o2, 0o7, 0, 0, 0, 0, time.UTC)

	r := db.QueryRow(`SELECT a::VARCHAR, b::VARCHAR FROM (VALUES (?, ?)) t(a, b)`, d, d)
	require.NoError(t, r.Err())

	var a, b string
	require.NoError(t, r.Scan(&a, &b))
	require.Equal(t, "2022-02-07 00:00:00", a)
	require.Equal(t, "2022-02-07 00:00:00", b)

	s := []int32{1}
	r = db.QueryRow(`SELECT a::VARCHAR, b::VARCHAR FROM (VALUES (?, ?)) t(a, b)`, s, s)
	require.NoError(t, r.Scan(&a, &b))
	require.Equal(t, "[1]", a)
	require.Equal(t, "[1]", b)

	// Type without a fallback.
	r = db.QueryRow(`SELECT a.strA FROM (VALUES (?),(?)) t(a)`, Union{Tag: "strA", Value: "a"}, Union{Tag: "strB", Value: "b"})
	require.Contains(t, r.Err().Error(), "unsupported type")
}

func TestBindTimestampTypes(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE OR REPLACE TABLE foo(a TIMESTAMP, b TIMESTAMP_S, c TIMESTAMP_MS, d TIMESTAMP_NS)`)
	require.NoError(t, err)

	tm := time.Date(2025, time.May, 7, 11, 45, 10, 123456789, time.UTC)
	_, err = db.Exec(`INSERT INTO foo VALUES(?, ?, ?, ?)`, tm, tm, tm, tm)
	require.NoError(t, err)

	r := db.QueryRow(`SELECT a, b, c, d FROM foo`)
	require.NoError(t, r.Err())

	var a, b, c, d time.Time
	err = r.Scan(&a, &b, &c, &d)
	require.NoError(t, err)

	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123456000, time.UTC), a)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 0, time.UTC), b)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123000000, time.UTC), c)
	require.Equal(t, time.Date(2025, time.May, 7, 11, 45, 10, 123456789, time.UTC), d)
}

func TestPrepareComplex(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE arr_test(
		arr_int INTEGER[2],
		list_str VARCHAR[],
		str_col STRUCT(v VARCHAR, i INTEGER)
	)`)

	// Insert parameters.
	_, err := db.ExecContext(ctx, `INSERT INTO arr_test VALUES (?, ?, ?)`,
		[]int32{7, 1}, []string{"foo", "bar"}, map[string]any{"v": "baz", "i": int32(42)})
	require.NoError(t, err)

	prepared, err := db.Prepare(`SELECT * FROM arr_test
		WHERE arr_int = ? AND list_str = ? AND str_col = ?`)
	defer closePreparedWrapper(t, prepared)
	require.NoError(t, err)

	var arr Composite[[]int32]
	var list Composite[[]string]
	var struc Composite[map[string]any]

	// Test with `any` slice types.
	err = prepared.QueryRow(
		[]any{int32(7), int32(1)},
		[]any{"foo", "bar"},
		map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())

	// Test with specific slice types.
	err = prepared.QueryRow(
		[]int32{7, 1},
		[]string{"foo", "bar"},
		map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())

	// Test querying without a prepared statement.
	err = db.QueryRow(`SELECT * FROM arr_test
		WHERE arr_int = ? AND list_str = ? AND str_col = ?`,
		[]int32{7, 1}, []string{"foo", "bar"}, map[string]any{"v": "baz", "i": int32(42)},
	).Scan(&arr, &list, &struc)
	require.NoError(t, err)

	require.Equal(t, []int32{7, 1}, arr.Get())
	require.Equal(t, []string{"foo", "bar"}, list.Get())
	require.Equal(t, map[string]any{"v": "baz", "i": int32(42)}, struc.Get())
}

func TestBindJSON(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE tbl (j JSON)`)
	require.NoError(t, err)

	jsonData := []byte(`{"name": "Jimmy","age": 28}`)
	_, err = db.Exec(`INSERT INTO tbl VALUES (?)`, jsonData)
	require.NoError(t, err)

	var str string
	err = db.QueryRow(`SELECT j::VARCHAR FROM tbl`).Scan(&str)
	require.NoError(t, err)
	require.Equal(t, string(jsonData), str)
}

func TestPrepareComplexQueryParameter(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx := context.Background()
	createTable(t, db, `CREATE OR REPLACE TABLE arr_test(
		arr_float float[2]
	)`)

	// Insert parameters.
	_, err := db.ExecContext(ctx, `INSERT INTO arr_test VALUES (?)`,
		[]float32{7, 1})
	require.NoError(t, err)

	prepared, err := db.Prepare(`SELECT list_value(?,?), list_cosine_distance(?,?)`)
	defer closePreparedWrapper(t, prepared)
	require.NoError(t, err)

	var arr Composite[[][]int32]
	var dis Composite[float32]
	// Test with specific slice types.
	err = prepared.QueryRow([]int{1, 2}, []int64{1}, []float32{0.1, 0.2, 0.3}, []float32{0.2, 0.3, 0.4}).Scan(&arr, &dis)
	require.NoError(t, err)
	require.Equal(t, [][]int32{{1, 2}, {1}}, arr.Get())
	require.True(t, dis.Get() > 0)

	dynamicPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, dynamicPrepare)
	require.NoError(t, err)

	var res Composite[[]any]
	err = dynamicPrepare.QueryRow([]any{}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Get()))

	// The statement type has already been bind as a SQL_NULL LIST in previous query
	err = dynamicPrepare.QueryRow([]any{1}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{nil}, res.Get())

	ptr := &([]int64{1})[0]
	ptrPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, ptrPrepare)
	require.NoError(t, err)

	err = ptrPrepare.QueryRow([]any{&([]int64{1})[0]}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, res.Get())

	nestedPtr := &ptr
	nestedPtrPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedPtrPrepare)
	require.NoError(t, err)

	err = nestedPtrPrepare.QueryRow([]any{nestedPtr}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, res.Get())

	arrayPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, arrayPrepare)
	require.NoError(t, err)

	err = arrayPrepare.QueryRow([1]any{123}).Scan(&res)
	require.NoError(t, err)
	require.Equal(t, []any{int64(123)}, res.Get())

	var nestedListRes Composite[[][]any]
	nestedListStmt, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedListStmt)
	require.NoError(t, err)

	err = nestedListStmt.QueryRow([][]float32{{0.1}, {0.2, 0.3}}).Scan(&nestedListRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{{float32(0.1)}, {float32(0.2), float32(0.3)}}, nestedListRes.Get())

	var nestedRes Composite[[][]any]
	nestedArrayPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, arrayPrepare)
	require.NoError(t, err)

	err = nestedArrayPrepare.QueryRow([2][2]float32{{0.1, 0.2}, {0.3, 0.4}}).Scan(&nestedRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{{float32(0.1), float32(0.2)}, {float32(0.3), float32(0.4)}}, nestedRes.Get())

	var tripleNestedRes Composite[[][][]string]
	tripleNestedPrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, tripleNestedPrepare)
	require.NoError(t, err)

	err = tripleNestedPrepare.QueryRow([][][]string{{{"a"}, {"b", "c"}}, {{"1", "2"}, {"3"}}, {{"d", "e", "f"}}}).Scan(&tripleNestedRes)
	require.NoError(t, err)
	require.Equal(t, [][][]string{{{"a"}, {"b", "c"}}, {{"1", "2"}, {"3"}}, {{"d", "e", "f"}}}, tripleNestedRes.Get())

	var emptySliceRes Composite[[]any]
	emptySlicePrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, emptySlicePrepare)
	require.NoError(t, err)

	err = emptySlicePrepare.QueryRow([]float32{}).Scan(&emptySliceRes)
	require.NoError(t, err)
	require.Equal(t, []any{}, emptySliceRes.Get())

	var nestedEmptySliceRes Composite[[][]any]
	nestedEmptySlicePrepare, err := db.Prepare(`SELECT * from (VALUES (?))`)
	defer closePreparedWrapper(t, nestedEmptySlicePrepare)
	require.NoError(t, err)

	err = nestedEmptySlicePrepare.QueryRow([][]string{}).Scan(&nestedEmptySliceRes)
	require.NoError(t, err)
	require.Equal(t, [][]any{}, nestedEmptySliceRes.Get())
}

func TestBindUUID(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Create table with nullable UUID column
	_, err := db.Exec(`CREATE TABLE uuid_test (id INTEGER, uuid_col UUID)`)
	require.NoError(t, err)

	// Test 1: Insert a NULL UUID using (*uuid.UUID)(nil)
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 1, (*uuid.UUID)(nil))
	require.NoError(t, err)

	// Test 2: Insert a NULL UUID using nil
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 2, nil)
	require.NoError(t, err)

	// Test 3: Insert a valid UUID ptr, to test complex value binding
	u3 := uuid.New()
	testUUID := UUID(u3)
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 3, &testUUID)
	require.NoError(t, err)

	// Test 4: Insert a uuid.UUID pointer containing a value, to test Stringer interface
	u4 := uuid.New()
	ptrToUUID := &u4
	_, err = db.Exec(`INSERT INTO uuid_test VALUES (?, ?)`, 4, ptrToUUID)
	require.NoError(t, err)

	// Verify results by scanning back
	r, err := db.Query(`SELECT id, uuid_col FROM uuid_test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, r)

	expectedResults := []struct {
		id   int
		uuid *uuid.UUID
	}{
		{1, nil},
		{2, nil},
		{3, &u3},
		{4, &u4},
	}

	resultIndex := 0
	for r.Next() {
		var id int
		var retrievedUUID *uuid.UUID
		err = r.Scan(&id, &retrievedUUID)
		require.NoError(t, err)

		expected := expectedResults[resultIndex]
		require.Equal(t, expected.id, id, "incorrect id")

		if expected.uuid == nil {
			require.Nil(t, retrievedUUID)
		} else {
			require.NotNil(t, retrievedUUID)
			require.Equal(t, *expected.uuid, *retrievedUUID)
		}
		resultIndex++
	}
	require.Equal(t, 4, resultIndex, "incorrect count of results")

	// Verify NULL count
	var nullCount int
	err = db.QueryRow(`SELECT COUNT(*) FROM uuid_test WHERE uuid_col IS NULL`).Scan(&nullCount)
	require.NoError(t, err)
	require.Equal(t, 2, nullCount, "incorrect count of NULLs")
}
