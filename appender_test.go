package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	_ "time/tzdata"

	"github.com/go-viper/mapstructure/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type simpleStruct struct {
	A int32 `db:"a"`
	B string
}

type duplicateKeyStruct struct {
	A         int64 `db:"Duplicate"`
	Duplicate int64
}

type wrappedSimpleStruct struct {
	A string
	B simpleStruct
}

type wrappedStruct struct {
	N string
	M simpleStruct
}

type doubleWrappedStruct struct {
	X string
	Y wrappedStruct
}

type structWithList struct {
	L []int32
}

type mixedStruct struct {
	A struct {
		L []string
	}
	B []struct {
		L []int32
	}
	C struct {
		L Map
	}
}

type nestedDataRow struct {
	ID                  int64
	stringList          []string
	intList             []int32
	nestedIntList       [][]int32
	tripleNestedIntList [][][]int32
	simpleStruct        simpleStruct
	wrappedStruct       wrappedStruct
	doubleWrappedStruct doubleWrappedStruct
	structList          []simpleStruct
	structWithList      structWithList
	mix                 mixedStruct
	mixList             []mixedStruct
}

type resultRow struct {
	ID                  int64
	stringList          []any
	intList             []any
	nestedIntList       []any
	tripleNestedIntList []any
	simpleStruct        any
	wrappedStruct       any
	doubleWrappedStruct any
	structList          []any
	structWithList      any
	mix                 any
	mixList             []any
}

func castList[T any](val []any) []T {
	res := make([]T, len(val))
	for i, v := range val {
		res[i] = v.(T)
	}
	return res
}

func castMapListToStruct[T any](t *testing.T, val []any) []T {
	res := make([]T, len(val))
	for i, v := range val {
		err := mapstructure.Decode(v, &res[i])
		require.NoError(t, err)
	}
	return res
}

func castMapToStruct[T any](t *testing.T, val any) T {
	var res T
	err := mapstructure.Decode(val, &res)
	require.NoError(t, err)
	return res
}

func randInt(lo int64, hi int64) int64 {
	return rand.Int63n(hi-lo+1) + lo
}

func prepareAppender[T require.TestingT](t T, query string) (*Connector, *sql.DB, driver.Conn, *Appender) {
	c := newConnectorWrapper(t, ``, nil)

	db := sql.OpenDB(c)
	_, err := db.Exec(query)
	require.NoError(t, err)

	conn := openDriverConnWrapper(t, c)
	a := newAppenderWrapper(t, &conn, "", "test")
	return c, db, conn, a
}

func cleanupAppender[T require.TestingT](t T, c *Connector, db *sql.DB, conn driver.Conn, a *Appender) {
	closeAppenderWrapper(t, a)
	closeDriverConnWrapper(t, &conn)
	closeDbWrapper(t, db)
	closeConnectorWrapper(t, c)
}

func TestAppenderClose(t *testing.T) {
	defer VerifyAllocationCounters()
	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (i INTEGER)`)
	defer cleanupAppender(t, c, db, conn, a)
	require.NoError(t, a.AppendRow(int32(42)))
}

func TestAppendChunks(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
		CREATE TABLE test (
			id BIGINT,
			uint8 UTINYINT
	  	)`)
	defer cleanupAppender(t, c, db, conn, a)

	// Test appending a few data chunks.
	rowCount := GetDataChunkCapacity() * 5
	type row struct {
		ID    int64
		UInt8 uint8
	}

	rowsToAppend := make([]row, rowCount)
	for i := 0; i < rowCount; i++ {
		rowsToAppend[i] = row{ID: int64(i), UInt8: uint8(randInt(0, 255))}
		require.NoError(t, a.AppendRow(rowsToAppend[i].ID, rowsToAppend[i].UInt8))
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		r := row{}
		require.NoError(t, res.Scan(&r.ID, &r.UInt8))
		require.Equal(t, rowsToAppend[i], r)
		i++
	}
	require.Equal(t, rowCount, i)
}

func TestAppenderList(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		string_list VARCHAR[],
		int_list INTEGER[]
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	rowsToAppend := make([]nestedDataRow, 10)
	for i := 0; i < 10; i++ {
		rowsToAppend[i].stringList = []string{"a", "b", "c"}
		rowsToAppend[i].intList = []int32{1, 2, 3}
	}

	for _, row := range rowsToAppend {
		require.NoError(t, a.AppendRow(row.stringList, row.intList))
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var r resultRow
		require.NoError(t, res.Scan(&r.stringList, &r.intList))
		require.Equal(t, rowsToAppend[i].stringList, castList[string](r.stringList))
		require.Equal(t, rowsToAppend[i].intList, castList[int32](r.intList))
		i++
	}
	require.Equal(t, 10, i)
}

func TestAppenderArray(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (string_array VARCHAR[3])`)
	defer cleanupAppender(t, c, db, conn, a)

	count := 10
	expected := Composite[[3]string]{[3]string{"a", "b", "c"}}
	for i := 0; i < count; i++ {
		require.NoError(t, a.AppendRow([]string{"a", "b", "c"}))
		require.NoError(t, a.AppendRow(expected.Get()))
	}
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var r Composite[[3]string]
		require.NoError(t, res.Scan(&r))
		require.Equal(t, expected, r)
		i++
	}
	require.Equal(t, 2*count, i)
}

func TestAppenderNested(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, createNestedDataTableSQL)
	defer cleanupAppender(t, c, db, conn, a)

	const rowCount = 1000
	rowsToAppend := prepareNestedData(rowCount)
	appendNestedData(t, a, rowsToAppend)

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test ORDER BY id`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var r resultRow
		require.NoError(t, res.Scan(
			&r.ID,
			&r.stringList,
			&r.intList,
			&r.nestedIntList,
			&r.tripleNestedIntList,
			&r.simpleStruct,
			&r.wrappedStruct,
			&r.doubleWrappedStruct,
			&r.structList,
			&r.structWithList,
			&r.mix,
			&r.mixList,
		))

		require.Equal(t, rowsToAppend[i].ID, r.ID)
		require.Equal(t, rowsToAppend[i].stringList, castList[string](r.stringList))
		require.Equal(t, rowsToAppend[i].intList, castList[int32](r.intList))

		strRes := fmt.Sprintf("%v", r.nestedIntList)
		require.Equal(t, "[[1 2 3] [4 5 6]]", strRes)
		strRes = fmt.Sprintf("%v", r.tripleNestedIntList)
		require.Equal(t, "[[[1 2 3] [4 5 6]] [[7 8 9] [10 11 12]]]", strRes)

		require.Equal(t, rowsToAppend[i].simpleStruct, castMapToStruct[simpleStruct](t, r.simpleStruct))
		require.Equal(t, rowsToAppend[i].wrappedStruct, castMapToStruct[wrappedStruct](t, r.wrappedStruct))
		require.Equal(t, rowsToAppend[i].doubleWrappedStruct, castMapToStruct[doubleWrappedStruct](t, r.doubleWrappedStruct))

		require.Equal(t, rowsToAppend[i].structList, castMapListToStruct[simpleStruct](t, r.structList))
		require.Equal(t, rowsToAppend[i].structWithList, castMapToStruct[structWithList](t, r.structWithList))
		require.Equal(t, rowsToAppend[i].mix, castMapToStruct[mixedStruct](t, r.mix))
		require.Equal(t, rowsToAppend[i].mixList, castMapListToStruct[mixedStruct](t, r.mixList))

		i++
	}
	require.Equal(t, rowCount, i)
}

func TestAppenderNullList(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (int_slice VARCHAR[][][])`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow([][][]string{{{}}}))
	require.NoError(t, a.AppendRow([][][]string{{{"1", "2", "3"}, {"4", "5", "6"}}}))
	require.NoError(t, a.AppendRow([][][]string{{{"1"}, nil}}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow([][][]string{nil, {{"2"}}}))
	require.NoError(t, a.AppendRow([][][]string{{nil, {"3"}}, {{"4"}}}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT int_slice FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

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
		var intS []any
		err := res.Scan(&intS)
		if err != nil {
			strS = "<nil>"
		} else {
			strS = fmt.Sprintf("%v", intS)
		}

		require.Equal(t, strResult[i], strS, "row %d: expected %v, got %v", i, strResult[i], strS)
		i++
	}
}

func TestAppenderNullStruct(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		simple_struct STRUCT(a INT, B VARCHAR)
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(simpleStruct{1, "hello"}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT simple_struct FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var row any
		err := res.Scan(&row)
		switch i {
		case 0:
			require.NoError(t, err)
		case 1:
			require.Nil(t, row)
		}
		i++
	}
}

func TestAppenderNestedNullStruct(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		double_wrapped_struct STRUCT(
				X VARCHAR,
				Y STRUCT(
					N VARCHAR,
					M STRUCT(
						a INT,
						B VARCHAR
					)
				)
			)
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(doubleWrappedStruct{
		"so much nesting",
		wrappedStruct{
			"wrapped",
			simpleStruct{1, "foo"},
		},
	}))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow(doubleWrappedStruct{
		"now we are done nesting NULLs",
		wrappedStruct{
			"unwrap",
			simpleStruct{21, "bar"},
		},
	}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT double_wrapped_struct FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var row any
		err := res.Scan(&row)
		if i == 1 {
			require.Nil(t, row)
		} else {
			require.NoError(t, err)
		}
		i++
	}
}

func TestAppenderNullIntAndString(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (id BIGINT, str VARCHAR)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(int64(32), "hello"))
	require.NoError(t, a.AppendRow(nil, nil))
	require.NoError(t, a.AppendRow(nil, "half valid thingy"))
	require.NoError(t, a.AppendRow(int64(60), nil))
	require.NoError(t, a.AppendRow(int64(42), "valid again"))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var id int
		var str string
		err := res.Scan(
			&id,
			&str,
		)
		if i == 0 {
			require.NoError(t, err)
			require.Equal(t, 32, id)
			require.Equal(t, "hello", str)
		} else if i > 0 && i < 4 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, 42, id)
			require.Equal(t, "valid again", str)
		}
		i++
	}
}

func TestAppenderUUID(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (id UUID)`)
	defer cleanupAppender(t, c, db, conn, a)

	id := UUID(uuid.New())
	otherId := UUID(uuid.New())
	require.NoError(t, a.AppendRow(id))
	require.NoError(t, a.AppendRow(&otherId))
	require.NoError(t, a.AppendRow((*UUID)(nil)))
	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT id FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		if i == 0 {
			var r UUID
			require.NoError(t, res.Scan(&r))
			require.Equal(t, id, r)
		} else {
			var r *UUID
			require.NoError(t, res.Scan(&r))
			if i == 1 {
				require.Equal(t, otherId, *r)
			} else {
				require.Nil(t, r)
			}
		}
		i++
	}
}

func newAppenderHugeIntTest[T numericType](val T, c *Connector, db *sql.DB, a *Appender) func(t *testing.T) {
	return func(t *testing.T) {
		typeName := reflect.TypeOf(val).String()
		require.NoError(t, a.AppendRow(val, typeName))
		require.NoError(t, a.Flush())

		// Verify results.
		res := db.QueryRowContext(context.Background(), `SELECT val FROM test WHERE id == ?`, typeName)

		var r *big.Int
		require.NoError(t, res.Scan(&r))
		require.Equal(t, big.NewInt(int64(val)), r)
	}
}

func TestAppenderHugeInt(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (val HUGEINT, id VARCHAR)`)
	defer cleanupAppender(t, c, db, conn, a)

	tests := map[string]func(t *testing.T){
		"int8":    newAppenderHugeIntTest[int8](1, c, db, a),
		"int16":   newAppenderHugeIntTest[int16](2, c, db, a),
		"int32":   newAppenderHugeIntTest[int32](3, c, db, a),
		"int64":   newAppenderHugeIntTest[int64](4, c, db, a),
		"uint8":   newAppenderHugeIntTest[uint8](5, c, db, a),
		"uint16":  newAppenderHugeIntTest[uint16](6, c, db, a),
		"uint32":  newAppenderHugeIntTest[uint32](7, c, db, a),
		"uint64":  newAppenderHugeIntTest[uint64](8, c, db, a),
		"float32": newAppenderHugeIntTest[float32](9, c, db, a),
		"float64": newAppenderHugeIntTest[float64](10, c, db, a),
	}
	for name, test := range tests {
		t.Run(name, test)
	}
}

func TestAppenderTsNs(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (timestamp TIMESTAMP_NS)`)
	defer cleanupAppender(t, c, db, conn, a)

	ts := time.Date(2022, time.January, 1, 12, 0, 33, 242, time.UTC)
	require.NoError(t, a.AppendRow(ts))
	require.NoError(t, a.Flush())

	// Verify results.
	res := db.QueryRowContext(context.Background(), `SELECT timestamp FROM test`)

	var r time.Time
	require.NoError(t, res.Scan(&r))
	require.Equal(t, ts, r)
}

func TestAppenderDate(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (date DATE)`)
	defer cleanupAppender(t, c, db, conn, a)

	ts := time.Date(1996, time.July, 23, 11, 42, 23, 123, time.UTC)
	require.NoError(t, a.AppendRow(ts))
	require.NoError(t, a.Flush())

	// Verify results.
	res := db.QueryRowContext(context.Background(), `SELECT date FROM test`)

	var r time.Time
	require.NoError(t, res.Scan(&r))
	require.Equal(t, ts.Year(), r.Year())
	require.Equal(t, ts.Month(), r.Month())
	require.Equal(t, ts.Day(), r.Day())
}

func TestAppenderTime(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (time TIME)`)
	defer cleanupAppender(t, c, db, conn, a)

	ts := time.Date(1996, time.July, 23, 11, 42, 23, 123000, time.UTC)
	require.NoError(t, a.AppendRow(ts))
	require.NoError(t, a.Flush())

	// Verify results.
	res := db.QueryRowContext(context.Background(), `SELECT time FROM test`)

	var r time.Time
	require.NoError(t, res.Scan(&r))
	base := time.Date(1, time.January, 1, 11, 42, 23, 123000, time.UTC)
	require.Equal(t, base.UnixMicro(), r.UnixMicro())
}

func TestAppenderTimeTZ(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (time TIMETZ)`)
	defer cleanupAppender(t, c, db, conn, a)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	ts := time.Date(1996, time.July, 23, 11, 42, 23, 123000, loc)
	require.NoError(t, a.AppendRow(ts))
	require.NoError(t, a.Flush())

	// Verify results.
	res := db.QueryRowContext(context.Background(), `SELECT time FROM test`)

	var r time.Time
	require.NoError(t, res.Scan(&r))
	base := time.Date(1, time.January, 1, 3, 42, 23, 123000, time.UTC)
	require.Equal(t, base.UnixMicro(), r.UnixMicro())
}

func TestAppenderBlob(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `CREATE TABLE test (data BLOB)`)
	defer cleanupAppender(t, c, db, conn, a)

	data := []byte{0x01, 0x02, 0x00, 0x03, 0x04}
	require.NoError(t, a.AppendRow(data))

	// Treat []uint8 the same as []byte.
	uint8Slice := []uint8{0x01, 0x02, 0x00, 0x03, 0x04}
	require.NoError(t, a.AppendRow(uint8Slice))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT data FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var b []byte
		require.NoError(t, res.Scan(&b))
		require.Equal(t, data, b)
		i++
	}
	require.Equal(t, 2, i)
}

func TestAppenderBlobTinyInt(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(nil))

	// We treat the byte slice as a list, as that's the type we set when creating the appender.
	require.NoError(t, a.AppendRow([]byte{0x01, 0x02, 0x03, 0x04}))
	require.NoError(t, a.AppendRow([]byte{0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04, 0x01, 0x00, 0x03, 0x04}))
	require.NoError(t, a.AppendRow([]byte{}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	expected := []string{
		"NULL",
		"[1, 2, 3, 4]",
		"[1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4, 1, 0, 3, 4]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		require.NoError(t, res.Scan(&str))
		require.Equal(t, expected[i], str)
		i++
	}
	require.Equal(t, 4, i)
}

func TestAppenderUint8SliceTinyInt(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		data UTINYINT[]
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow([]uint8{0x01, 0x00, 0x03, 0x04, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0}))
	require.NoError(t, a.AppendRow([]uint8{}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	expected := []string{
		"NULL",
		"[1, 0, 3, 4, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0]",
		"[]",
	}

	i := 0
	for res.Next() {
		var str string
		require.NoError(t, res.Scan(&str))
		require.Equal(t, expected[i], str)
		i++
	}
	require.Equal(t, 3, i)
}

func TestAppenderDecimal(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
	CREATE TABLE test (
		data DECIMAL(4,3)
	)`)
	defer cleanupAppender(t, c, db, conn, a)

	require.NoError(t, a.AppendRow(nil))
	require.NoError(t, a.AppendRow(Decimal{Width: uint8(4), Value: big.NewInt(1), Scale: 3}))
	require.NoError(t, a.AppendRow(Decimal{Width: uint8(4), Value: big.NewInt(2), Scale: 3}))
	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT CASE WHEN data IS NULL THEN 'NULL' ELSE data::VARCHAR END FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	expected := []string{
		"NULL",
		"0.001",
		"0.002",
	}

	i := 0
	for res.Next() {
		var str string
		require.NoError(t, res.Scan(&str))
		require.Equal(t, expected[i], str)
		i++
	}
	require.Equal(t, 3, i)
}

func TestAppendToCatalog(t *testing.T) {
	defer VerifyAllocationCounters()

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`ATTACH 'hello_appender.db' AS other`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE other.test (
			col BIGINT
	  	)`)

	conn := openConnWrapper(t, db, context.Background())
	defer closeConnWrapper(t, conn)

	err = conn.Raw(func(anyConn interface{}) error {
		driverConn := anyConn.(driver.Conn)
		a, innerErr := NewAppender(driverConn, "other", "", "test")
		require.NoError(t, innerErr)

		require.NoError(t, a.AppendRow(42))
		require.NoError(t, a.Flush())
		require.NoError(t, a.Close())
		return nil
	})
	require.NoError(t, err)

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM other.test ORDER BY col`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var col int64
		require.NoError(t, res.Scan(&col))
		require.Equal(t, int64(42), col)
		i++
	}
	require.Equal(t, 1, i)
	require.NoError(t, os.Remove("hello_appender.db"))
}

var jsonInputs = [][]byte{
	[]byte(`{"c1": 42, "l1": [1, 2, 3], "s1": {"a": 101, "b": ["hello", "world"]}, "l2": [{"a": [{"a": [4.2, 7.9]}]}]}`),
	[]byte(`{"c1": null, "l1": [null, 2, null], "s1": {"a": null, "b": ["hello", null]}, "l2": [{"a": [{"a": [null, 7.9]}]}]}`),
	[]byte(`{"c1": null, "l1": null, "s1": {"a": null, "b": null}, "l2": [{"a": [{"a": null}]}]}`),
	[]byte(`{"c1": null, "l1": null, "s1": null, "l2": [{"a": [null, {"a": null}]}]}`),
	[]byte(`{"c1": null, "l1": null, "s1": null, "l2": [{"a": null}]}`),
	[]byte(`{"c1": null, "l1": null, "s1": null, "l2": [null, null]}`),
	[]byte(`{"c1": null, "l1": null, "s1": null, "l2": null}`),
}

var jsonResults = [][]string{
	{"42", "[1 2 3]", "map[a:101 b:[hello world]]", "[map[a:[map[a:[4.2 7.9]]]]]"},
	{"<nil>", "[<nil> 2 <nil>]", "map[a:<nil> b:[hello <nil>]]", "[map[a:[map[a:[<nil> 7.9]]]]]"},
	{"<nil>", "<nil>", "map[a:<nil> b:<nil>]", "[map[a:[map[a:<nil>]]]]"},
	{"<nil>", "<nil>", "<nil>", "[map[a:[<nil> map[a:<nil>]]]]"},
	{"<nil>", "<nil>", "<nil>", "[map[a:<nil>]]"},
	{"<nil>", "<nil>", "<nil>", "[<nil> <nil>]"},
	{"<nil>", "<nil>", "<nil>", "<nil>"},
}

func TestAppenderWithJSON(t *testing.T) {
	defer VerifyAllocationCounters()

	c, db, conn, a := prepareAppender(t, `
		CREATE TABLE test (
		    c1 UBIGINT,
			l1 TINYINT[],
			s1 STRUCT(a INTEGER, b VARCHAR[]),
		    l2 STRUCT(a STRUCT(a FLOAT[])[])[]              
	  	)`)
	defer cleanupAppender(t, c, db, conn, a)

	for _, jsonInput := range jsonInputs {
		var jsonData map[string]interface{}
		err := json.Unmarshal(jsonInput, &jsonData)
		require.NoError(t, err)
		require.NoError(t, a.AppendRow(jsonData["c1"], jsonData["l1"], jsonData["s1"], jsonData["l2"]))
	}

	require.NoError(t, a.Flush())

	// Verify results.
	res, err := db.QueryContext(context.Background(), `SELECT * FROM test`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)

	i := 0
	for res.Next() {
		var (
			c1 interface{}
			l1 interface{}
			s1 interface{}
			l2 interface{}
		)
		err := res.Scan(&c1, &l1, &s1, &l2)
		require.NoError(t, err)
		require.Equal(t, jsonResults[i][0], fmt.Sprint(c1))
		require.Equal(t, jsonResults[i][1], fmt.Sprint(l1))
		require.Equal(t, jsonResults[i][2], fmt.Sprint(s1))
		require.Equal(t, jsonResults[i][3], fmt.Sprint(l2))
		i++
	}
	require.Equal(t, len(jsonInputs), i)
}

func BenchmarkAppenderNested(b *testing.B) {
	c, db, conn, a := prepareAppender(b, createNestedDataTableSQL)
	defer cleanupAppender(b, c, db, conn, a)

	const rowCount = 600
	rowsToAppend := prepareNestedData(rowCount)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		appendNestedData(b, a, rowsToAppend)
	}
	b.StopTimer()
}

const createNestedDataTableSQL = `
	CREATE TABLE test (
		id BIGINT,
		string_list VARCHAR[],
		int_list INT[],
		nested_int_list INT[][],
		triple_nested_int_list INT[][][],
		simple_struct STRUCT(a INT, B VARCHAR),
		wrapped_struct STRUCT(N VARCHAR, M STRUCT(a INT, B VARCHAR)),
		double_wrapped_struct STRUCT(
			X VARCHAR,
			Y STRUCT(
				N VARCHAR,
				M STRUCT(
					a INT,
					B VARCHAR
				)
			)
		),
		struct_list STRUCT(a INT, B VARCHAR)[],
		struct_with_list STRUCT(L INT[]),
		mix STRUCT(
			A STRUCT(L VARCHAR[]),
			B STRUCT(L INT[])[],
			C STRUCT(L MAP(VARCHAR, INT))
		),
		mix_list STRUCT(
			A STRUCT(L VARCHAR[]),
			B STRUCT(L INT[])[],
			C STRUCT(L MAP(VARCHAR, INT))
		)[]
	)
`

func prepareNestedData(rowCount int) []nestedDataRow {
	ms := mixedStruct{
		A: struct {
			L []string
		}{
			[]string{"a", "b", "c"},
		},
		B: []struct {
			L []int32
		}{
			{[]int32{1, 2, 3}},
		},
		C: struct {
			L Map
		}{L: Map{"foo": int32(1), "bar": int32(2)}},
	}

	rowsToAppend := make([]nestedDataRow, rowCount)
	for i := 0; i < rowCount; i++ {
		rowsToAppend[i].ID = int64(i)
		rowsToAppend[i].stringList = []string{"a", "b", "c"}
		rowsToAppend[i].intList = []int32{1, 2, 3}
		rowsToAppend[i].nestedIntList = [][]int32{{1, 2, 3}, {4, 5, 6}}
		rowsToAppend[i].tripleNestedIntList = [][][]int32{
			{{1, 2, 3}, {4, 5, 6}},
			{{7, 8, 9}, {10, 11, 12}},
		}
		rowsToAppend[i].simpleStruct = simpleStruct{A: 1, B: "foo"}
		rowsToAppend[i].wrappedStruct = wrappedStruct{"wrapped", simpleStruct{1, "foo"}}
		rowsToAppend[i].doubleWrappedStruct = doubleWrappedStruct{
			"so much nesting",
			wrappedStruct{
				"wrapped",
				simpleStruct{1, "foo"},
			},
		}
		rowsToAppend[i].structList = []simpleStruct{{1, "a"}, {2, "b"}, {3, "c"}}
		rowsToAppend[i].structWithList.L = []int32{6, 7, 8}
		rowsToAppend[i].mix = ms
		rowsToAppend[i].mixList = []mixedStruct{ms, ms}
	}

	return rowsToAppend
}

func appendNestedData[T require.TestingT](t T, a *Appender, rowsToAppend []nestedDataRow) {
	for _, row := range rowsToAppend {
		require.NoError(t, a.AppendRow(
			row.ID,
			row.stringList,
			row.intList,
			row.nestedIntList,
			row.tripleNestedIntList,
			row.simpleStruct,
			row.wrappedStruct,
			row.doubleWrappedStruct,
			row.structList,
			row.structWithList,
			row.mix,
			row.mixList))
	}
	require.NoError(t, a.Flush())
}

var types = map[reflect.Type]string{
	reflect.TypeFor[int8](): "TINYINT",
}

func benchmarkAppenderSingle[T any](v T) func(*testing.B) {
	return func(b *testing.B) {
		if _, ok := types[reflect.TypeFor[T]()]; !ok {
			b.Fatal("Type not defined in table:", reflect.TypeFor[T]())
		}
		defer VerifyAllocationCounters()

		tableSQL := fmt.Sprintf(createSingleTableSQL, types[reflect.TypeFor[T]()])
		c, db, conn, a := prepareAppender(b, tableSQL)
		defer cleanupAppender(b, c, db, conn, a)

		const rowsToAppend = 2048
		vec := [rowsToAppend]T{}
		for i := 0; i < 2048; i++ {
			vec[i] = v
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for i := 0; i < rowsToAppend; i++ {
				if err := a.AppendRow(v); err != nil {
					b.Error(err)
				}
			}
		}
		b.StopTimer()
	}
}

func BenchmarkAppenderSingle(b *testing.B) {
	b.Run("int8", benchmarkAppenderSingle[int8](0))
}

const createSingleTableSQL = `CREATE TABLE test (nested_int_list %s)`
