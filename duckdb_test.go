package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	t.Run("without config", func(t *testing.T) {
		db := openDB(t)
		defer db.Close()
		require.NotNil(t, db)
	})

	t.Run("with config", func(t *testing.T) {
		db, err := sql.Open("duckdb", "?access_mode=read_write&threads=4")
		require.NoError(t, err)
		defer db.Close()

		var (
			accessMode string
			threads    int64
		)
		res := db.QueryRow("SELECT current_setting('access_mode'), current_setting('threads')")
		require.NoError(t, res.Scan(&accessMode, &threads))
		require.Equal(t, int64(4), threads)
		require.Equal(t, "read_write", accessMode)
	})

	t.Run("existing sqlite database", func(t *testing.T) {
		db, err := sql.Open("duckdb", "sqlite:testdata/pets.sqlite")
		require.NoError(t, err)
		defer db.Close()

		var species string
		res := db.QueryRow("SELECT species FROM pets WHERE id=1")
		require.NoError(t, res.Scan(&species))
		require.Equal(t, "Gopher", species)
	})
}

func TestConnectorBootQueries(t *testing.T) {
	t.Run("readme example", func(t *testing.T) {
		db, err := sql.Open("duckdb", "foo.db")
		require.NoError(t, err)
		_ = db.Close()

		connector, err := NewConnector("foo.db?access_mode=read_only&threads=4", func(execer driver.ExecerContext) error {
			bootQueries := []string{
				"SET schema=main",
				"SET search_path=main",
			}
			for _, query := range bootQueries {
				_, err = execer.ExecContext(context.Background(), query, nil)
				require.NoError(t, err)
			}
			return nil
		})
		require.NoError(t, err)

		db = sql.OpenDB(connector)
		_ = db.Close()

		err = os.Remove("foo.db")
		require.NoError(t, err)
	})
}

func TestConnector_Close(t *testing.T) {
	t.Parallel()

	connector, err := NewConnector("", nil)
	require.NoError(t, err)

	// check that multiple close calls don't cause panics or errors
	require.NoError(t, connector.Close())
	require.NoError(t, connector.Close())
}

func TestConnPool(t *testing.T) {
	db := openDB(t)
	db.SetMaxOpenConns(2) // set connection pool size greater than 1
	defer db.Close()
	createFooTable(db, t)

	// Get two separate connections and check they're consistent
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)
	conn2, err := db.Conn(context.Background())
	require.NoError(t, err)

	res, err := conn1.ExecContext(context.Background(), "INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), ra)

	require.NoError(t, err)
	rows, err := conn1.QueryContext(context.Background(), "select bar from foo limit 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	require.NoError(t, err)
	rows, err = conn2.QueryContext(context.Background(), "select bar from foo limit 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	err = conn1.Close()
	require.NoError(t, err)
	err = conn2.Close()
	require.NoError(t, err)
}

func TestConnInit(t *testing.T) {
	connector, err := NewConnector("", func(execer driver.ExecerContext) error {
		return nil
	})
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2) // set connection pool size greater than 1
	defer db.Close()

	// Get two separate connections and check they're consistent
	conn1, err := db.Conn(context.Background())
	require.NoError(t, err)
	conn2, err := db.Conn(context.Background())
	require.NoError(t, err)

	res, err := conn1.ExecContext(context.Background(), "CREATE TABLE example (j JSON)")
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), ra)

	res, err = conn2.ExecContext(context.Background(), "INSERT INTO example VALUES(' { \"family\": \"anatidae\", \"species\": [ \"duck\", \"goose\", \"swan\", null ] }')")
	require.NoError(t, err)
	ra, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), ra)

	require.NoError(t, err)
	rows, err := conn1.QueryContext(context.Background(), "SELECT json_valid(j) FROM example")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	require.NoError(t, err)
	rows, err = conn2.QueryContext(context.Background(), "SELECT json_valid(j) FROM example")
	require.NoError(t, err)
	require.True(t, rows.Next())
	rows.Close()

	err = conn1.Close()
	require.NoError(t, err)
	err = conn2.Close()
	require.NoError(t, err)
}

func TestExec(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	require.NotNil(t, createFooTable(db, t))
}

func TestQuery(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createFooTable(db, t)

	t.Run("simple", func(t *testing.T) {
		res, err := db.Exec("INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
		require.NoError(t, err)
		ra, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(2), ra)

		rows, err := db.Query("SELECT bar, baz FROM foo WHERE baz > ?", 12344)
		require.NoError(t, err)
		defer rows.Close()

		found := false
		for rows.Next() {
			var (
				bar string
				baz int
			)
			err := rows.Scan(&bar, &baz)
			require.NoError(t, err)
			if bar != "lala" || baz != 12345 {
				t.Errorf("wrong values for bar [%s] and baz [%d]", bar, baz)
			}
			found = true
		}

		require.True(t, found, "could not find row")
	})

	t.Run("large number of rows", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE integers AS SELECT * FROM range(0, 100000, 1) t(i);")
		require.NoError(t, err)

		rows, err := db.Query("SELECT i FROM integers ORDER BY i ASC")
		require.NoError(t, err)
		defer rows.Close()

		expected := 0
		for rows.Next() {
			var i int
			require.NoError(t, rows.Scan(&i))
			require.Equal(t, expected, i)
			expected++
		}
	})

	t.Run("wrong syntax", func(t *testing.T) {
		_, err := db.Query("select * from tbl col=?", 1)
		require.Error(t, err, "should return error with invalid syntax")
	})

	t.Run("missing parameter", func(t *testing.T) {
		_, err := db.Query("select * from tbl col=?")
		require.Error(t, err, "should return error with missing parameters")
	})

	t.Run("null", func(t *testing.T) {
		var s *int
		require.NoError(t, db.QueryRow("select null").Scan(&s))
		require.Nil(t, s)
	})
}

func TestJSON(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	var data string

	t.Run("select empty JSON", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT '{}'::JSON").Scan(&data))
		require.Equal(t, "{}", string(data))
	})

	t.Run("select from marshalled JSON", func(t *testing.T) {
		val, _ := json.Marshal(struct {
			Foo string `json:"foo"`
		}{
			Foo: "bar",
		})
		require.NoError(t, db.QueryRow(`SELECT ?::JSON->>'foo'`, string(val)).Scan(&data))
		require.Equal(t, "bar", data)
	})

	t.Run("select JSON array", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT json_array('foo', 'bar')").Scan(&data))
		require.Equal(t, `["foo","bar"]`, data)

		var items []string
		require.NoError(t, json.Unmarshal([]byte(data), &items))
		require.Equal(t, len(items), 2)
		require.Equal(t, items, []string{"foo", "bar"})
	})

	require.NoError(t, db.Close())
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	db := openDB(t)

	rows, err := db.Query(`SELECT 1 WHERE 1 = 0`)
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, db.Close())
}

func TestTypeNamesAndScanTypes(t *testing.T) {
	tests := []struct {
		sql      string
		value    any
		typeName string
	}{
		// DUCKDB_TYPE_BOOLEAN
		{
			sql:      "SELECT true AS col",
			value:    true,
			typeName: "BOOLEAN",
		},
		// DUCKDB_TYPE_TINYINT
		{
			sql:      "SELECT 31::TINYINT AS col",
			value:    int8(31),
			typeName: "TINYINT",
		},
		// DUCKDB_TYPE_SMALLINT
		{
			sql:      "SELECT 31::SMALLINT AS col",
			value:    int16(31),
			typeName: "SMALLINT",
		},
		// DUCKDB_TYPE_INTEGER
		{
			sql:      "SELECT 31::INTEGER AS col",
			value:    int32(31),
			typeName: "INTEGER",
		},
		// DUCKDB_TYPE_BIGINT
		{
			sql:      "SELECT 31::BIGINT AS col",
			value:    int64(31),
			typeName: "BIGINT",
		},
		// DUCKDB_TYPE_UTINYINT
		{
			sql:      "SELECT 31::UTINYINT AS col",
			value:    uint8(31),
			typeName: "UTINYINT",
		},
		// DUCKDB_TYPE_USMALLINT
		{
			sql:      "SELECT 31::USMALLINT AS col",
			value:    uint16(31),
			typeName: "USMALLINT",
		},
		// DUCKDB_TYPE_UINTEGER
		{
			sql:      "SELECT 31::UINTEGER AS col",
			value:    uint32(31),
			typeName: "UINTEGER",
		},
		// DUCKDB_TYPE_UBIGINT
		{
			sql:      "SELECT 31::UBIGINT AS col",
			value:    uint64(31),
			typeName: "UBIGINT",
		},
		// DUCKDB_TYPE_FLOAT
		{
			sql:      "SELECT 3.14::FLOAT AS col",
			value:    float32(3.14),
			typeName: "FLOAT",
		},
		// DUCKDB_TYPE_DOUBLE
		{
			sql:      "SELECT 3.14::DOUBLE AS col",
			value:    float64(3.14),
			typeName: "DOUBLE",
		},
		// DUCKDB_TYPE_TIMESTAMP
		{
			sql:      "SELECT '1992-09-20 11:30:00'::TIMESTAMP AS col",
			value:    time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP",
		},
		// DUCKDB_TYPE_DATE
		{
			sql:      "SELECT '1992-09-20'::DATE AS col",
			value:    time.Date(1992, 9, 20, 0, 0, 0, 0, time.UTC),
			typeName: "DATE",
		},
		// DUCKDB_TYPE_TIME
		{
			sql:      "SELECT '11:30:00'::TIME AS col",
			value:    time.Date(1970, 1, 1, 11, 30, 0, 0, time.UTC),
			typeName: "TIME",
		},
		// DUCKDB_TYPE_INTERVAL
		{
			sql:      "SELECT INTERVAL 15 MINUTES AS col",
			value:    Interval{Micros: 15 * 60 * 1000000},
			typeName: "INTERVAL",
		},
		// DUCKDB_TYPE_HUGEINT
		{
			sql:      "SELECT 31::HUGEINT AS col",
			value:    big.NewInt(31),
			typeName: "HUGEINT",
		},
		// DUCKDB_TYPE_VARCHAR
		{
			sql:      "SELECT 'foo'::VARCHAR AS col",
			value:    "foo",
			typeName: "VARCHAR",
		},
		// DUCKDB_TYPE_BLOB
		{
			sql:      "SELECT 'foo'::BLOB AS col",
			value:    []byte("foo"),
			typeName: "BLOB",
		},
		// DUCKDB_TYPE_DECIMAL
		{
			sql:      "SELECT 31::DECIMAL(30,17) AS col",
			value:    Decimal{Value: big.NewInt(3100000000000000000), Width: 30, Scale: 17},
			typeName: "DECIMAL(30,17)",
		},
		// DUCKDB_TYPE_TIMESTAMP_S
		{
			sql:      "SELECT '1992-09-20 11:30:00'::TIMESTAMP_S AS col",
			value:    time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_S",
		},
		// DUCKDB_TYPE_TIMESTAMP_MS
		{
			sql:      "SELECT '1992-09-20 11:30:00'::TIMESTAMP_MS AS col",
			value:    time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_MS",
		},
		// DUCKDB_TYPE_TIMESTAMP_NS
		{
			sql:      "SELECT '1992-09-20 11:30:00'::TIMESTAMP_NS AS col",
			value:    time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_NS",
		},
		// DUCKDB_TYPE_LIST
		{
			sql:      "SELECT [['duck', 'goose', 'heron'], NULL, ['frog', 'toad'], []] AS col",
			value:    []any{[]any{"duck", "goose", "heron"}, nil, []any{"frog", "toad"}, []any{}},
			typeName: "VARCHAR[][]",
		},
		// DUCKDB_TYPE_STRUCT
		{
			sql:      "SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345::DOUBLE} AS col",
			value:    map[string]any{"key1": "string", "key2": int32(1), "key3": float64(12.345)},
			typeName: `STRUCT("key1" VARCHAR, "key2" INTEGER, "key3" DOUBLE)`,
		},
		{
			sql:      `SELECT {'key1 (,) \ \" "" "': 1} AS col`,
			value:    map[string]any{`key1 (,) \ \" "" "`: int32(1)},
			typeName: `STRUCT("key1 (,) \ \"" """" """ INTEGER)`,
		},
		// DUCKDB_TYPE_MAP
		{
			sql:      "SELECT map([1, 5], ['a', 'e']) AS col",
			value:    Map{int32(1): "a", int32(5): "e"},
			typeName: "MAP(INTEGER, VARCHAR)",
		},
		// DUCKDB_TYPE_UUID
		{
			sql:      "SELECT '53b4e983-b287-481a-94ad-6e3c90489913'::UUID AS col",
			value:    []byte{0x53, 0xb4, 0xe9, 0x83, 0xb2, 0x87, 0x48, 0x1a, 0x94, 0xad, 0x6e, 0x3c, 0x90, 0x48, 0x99, 0x13},
			typeName: "UUID",
		},
		// DUCKDB_TYPE_TIMESTAMP_TZ
		{
			sql:      "SELECT '1992-09-20 11:30:00'::TIMESTAMPTZ AS col",
			value:    time.Date(1992, 9, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMPTZ",
		},
	}

	db := openDB(t)
	for _, test := range tests {
		t.Run(test.typeName, func(t *testing.T) {
			rows, err := db.Query(test.sql)
			require.NoError(t, err)

			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Equal(t, reflect.TypeOf(test.value), cols[0].ScanType())
			require.Equal(t, test.typeName, cols[0].DatabaseTypeName())

			var val any
			require.True(t, rows.Next())
			err = rows.Scan(&val)
			require.NoError(t, err)
			require.Equal(t, test.value, val)
			require.Equal(t, rows.Next(), false)
		})
	}
	require.NoError(t, db.Close())
}

// Running multiple statements in a single query. All statements except the last one are executed and if no error then last statement is executed with args and result returned.
func TestMultipleStatements(t *testing.T) {
	db := openDB(t)

	// test empty query
	_, err := db.Exec("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no statements found")

	// test invalid query
	_, err = db.Exec("abc;")
	require.Error(t, err)
	require.Contains(t, err.Error(), "syntax error at or near \"abc\"")

	// test valid + invalid query
	_, err = db.Exec("CREATE TABLE foo (x text); abc;")
	require.Error(t, err)
	require.Contains(t, err.Error(), "syntax error at or near \"abc\"")

	// test invalid + valid query
	_, err = db.Exec("abc; CREATE TABLE foo (x text);")
	require.Error(t, err)
	require.Contains(t, err.Error(), "syntax error at or near \"abc\"")

	_, err = db.Exec("CREATE TABLE foo (x text); CREATE TABLE bar (x text);")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO foo VALUES (?); INSERT INTO bar VALUES (?);", "hello", "world")
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect argument count for command: have 0 want 1")

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	res, err := conn.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS foo1(bar VARCHAR, baz INTEGER); INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), ra)

	require.NoError(t, err)
	rows, err := conn.QueryContext(context.Background(), "select bar from foo1 limit 1")
	require.NoError(t, err)
	require.True(t, rows.Next())
	err = rows.Close()
	require.NoError(t, err)

	// args are only applied to the last statement
	_, err = conn.ExecContext(context.Background(), "INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?); INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect argument count for command: have 0 want 2")

	rows, err = conn.QueryContext(context.Background(), "CREATE TABLE foo2(bar VARCHAR, baz INTEGER); INSERT INTO foo2 VALUES ('lala', 12345); select bar from foo2 limit 1")
	require.NoError(t, err)
	var bar string
	require.True(t, rows.Next())
	err = rows.Scan(&bar)
	require.NoError(t, err)
	require.Equal(t, "lala", bar)
	require.False(t, rows.Next())
	err = rows.Close()
	require.NoError(t, err)

	// Trying select with ExecContext also works but we don't get the result
	res, err = conn.ExecContext(context.Background(), "CREATE TABLE foo3(bar VARCHAR, baz INTEGER); INSERT INTO foo3 VALUES ('lala', 12345); select bar from foo3 limit 1")
	require.NoError(t, err)
	ra, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), ra)

	// multiple selects, but we get results only for the last one
	rows, err = conn.QueryContext(context.Background(), "INSERT INTO foo3 VALUES ('lalo', 1234); select bar from foo3 where baz=12345; select bar from foo3 where baz=$1", 1234)
	require.NoError(t, err)
	require.True(t, rows.Next())
	err = rows.Scan(&bar)
	require.NoError(t, err)
	require.Equal(t, "lalo", bar)
	require.False(t, rows.Next())
	err = rows.Close()
	require.NoError(t, err)

	// test json extension
	rows, err = conn.QueryContext(context.Background(), `CREATE TABLE example (id int, j JSON);
		INSERT INTO example VALUES(123, ' { "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }');
		SELECT j->'$.family' FROM example WHERE id=$1`, 123)
	require.NoError(t, err)
	require.True(t, rows.Next())
	var family string
	err = rows.Scan(&family)
	require.NoError(t, err)
	require.Equal(t, "\"anatidae\"", family)
	require.False(t, rows.Next())
	err = rows.Close()
	require.NoError(t, err)

	require.NoError(t, conn.Close())
	require.NoError(t, db.Close())
}

func TestParquetExtension(t *testing.T) {
	db := openDB(t)

	_, err := db.Exec("CREATE TABLE users (id int, name varchar, age int);")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO users VALUES (1, 'Jane', 30);")
	require.NoError(t, err)

	_, err = db.Exec("COPY (SELECT * FROM users) TO './users.parquet' (FORMAT 'parquet');")
	require.NoError(t, err)

	type res struct {
		ID   int
		Name string
		Age  int
	}
	row := db.QueryRow("SELECT * FROM read_parquet('./users.parquet');")
	var r res
	err = row.Scan(&r.ID, &r.Name, &r.Age)
	require.NoError(t, err)
	require.Equal(t, res{ID: 1, Name: "Jane", Age: 30}, r)

	err = os.Remove("./users.parquet")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestQueryTimeout(t *testing.T) {
	db := openDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*250)
	defer cancel()

	now := time.Now()
	_, err := db.ExecContext(ctx, "CREATE TABLE test AS SELECT * FROM range(10000000) t1, range(1000000) t2;")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// a very defensive time check, but should be good enough
	// the query takes much longer than 10 seconds
	require.Less(t, time.Since(now), 10*time.Second)
	require.NoError(t, db.Close())
}

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func createTable(db *sql.DB, t *testing.T, sql string) *sql.Result {
	res, err := db.Exec(sql)
	require.NoError(t, err)
	return &res
}

func createFooTable(db *sql.DB, t *testing.T) *sql.Result {
	return createTable(db, t, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)
}
