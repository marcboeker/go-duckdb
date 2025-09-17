package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

/* ---------- Open and Close Wrappers ---------- */

func openDbWrapper[T require.TestingT](t T, dsn string) *sql.DB {
	db, err := sql.Open(`duckdb`, dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func closeDbWrapper[T require.TestingT](t T, db *sql.DB) {
	if db == nil {
		return
	}
	require.NoError(t, db.Close())
}

func newConnectorWrapper[T require.TestingT](t T, dsn string, connInitFn func(execer driver.ExecerContext) error) *Connector {
	c, err := NewConnector(dsn, connInitFn)
	require.NoError(t, err)
	return c
}

func closeConnectorWrapper[T require.TestingT](t T, c *Connector) {
	if c == nil {
		return
	}
	require.NoError(t, c.Close())
}

func openConnWrapper[T require.TestingT](t T, db *sql.DB, ctx context.Context) *sql.Conn {
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	return conn
}

func closeConnWrapper[T require.TestingT](t T, conn *sql.Conn) {
	if conn == nil {
		return
	}
	require.NoError(t, conn.Close())
}

func openDriverConnWrapper[T require.TestingT](t T, c *Connector) driver.Conn {
	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	return conn
}

func closeDriverConnWrapper[T require.TestingT](t T, conn *driver.Conn) {
	if conn == nil {
		return
	}
	require.NoError(t, (*conn).Close())
}

func closeRowsWrapper[T require.TestingT](t T, r *sql.Rows) {
	if r == nil {
		return
	}
	require.NoError(t, r.Close())
}

func closePreparedWrapper[T require.TestingT](t T, stmt *sql.Stmt) {
	if stmt == nil {
		return
	}
	require.NoError(t, stmt.Close())
}

func newAppenderWrapper[T require.TestingT](t T, conn *driver.Conn, schema, table string) *Appender {
	a, err := NewAppenderFromConn(*conn, schema, table)
	require.NoError(t, err)
	return a
}

func closeAppenderWrapper[T require.TestingT](t T, a *Appender) {
	if a == nil {
		return
	}
	require.NoError(t, a.Close())
}

/* ---------- Test Helpers ---------- */

func createTable(t *testing.T, db *sql.DB, query string) {
	res, err := db.Exec(query)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func checkIsMemory(t *testing.T, db *sql.DB) {
	res, err := db.Query(`SELECT * FROM information_schema.schemata WHERE catalog_name = 'memory'`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, res)
	require.True(t, res.Next())
}

/* ---------- Tests ---------- */

func TestOpen(t *testing.T) {
	t.Run("without config", func(t *testing.T) {
		db := openDbWrapper(t, ``)
		defer closeDbWrapper(t, db)
		require.NotNil(t, db)
	})

	t.Run("with config", func(t *testing.T) {
		db := openDbWrapper(t, `?access_mode=read_write&threads=4`)
		defer closeDbWrapper(t, db)

		var (
			accessMode string
			threads    int64
		)
		res := db.QueryRow(`SELECT current_setting('access_mode'), current_setting('threads')`)
		require.NoError(t, res.Scan(&accessMode, &threads))
		require.Equal(t, int64(4), threads)
		require.Equal(t, "read_write", accessMode)
	})

	t.Run(":memory:", func(t *testing.T) {
		db := openDbWrapper(t, `:memory:`)
		defer closeDbWrapper(t, db)
		// Verify that we are using an in-memory DB.
		checkIsMemory(t, db)
	})

	t.Run(":memory: with config", func(t *testing.T) {
		db := openDbWrapper(t, `:memory:?threads=4`)
		defer closeDbWrapper(t, db)
		// Verify that we are using an in-memory DB.
		checkIsMemory(t, db)

		var threads int64
		res := db.QueryRow(`SELECT current_setting('threads')`)
		require.NoError(t, res.Scan(&threads))
		require.Equal(t, int64(4), threads)
	})
}

func TestConnectorBootQueries(t *testing.T) {
	t.Run("README connector example", func(t *testing.T) {
		db := openDbWrapper(t, `foo.db`)
		closeDbWrapper(t, db)
		defer func() {
			require.NoError(t, os.Remove(`foo.db`))
		}()

		c := newConnectorWrapper(t, `foo.db?access_mode=read_only&threads=4`, func(execer driver.ExecerContext) error {
			bootQueries := []string{
				`SET schema=main`,
				`SET search_path=main`,
			}
			for _, query := range bootQueries {
				_, err := execer.ExecContext(context.Background(), query, nil)
				require.NoError(t, err)
			}
			return nil
		})
		defer closeConnectorWrapper(t, c)
		db = sql.OpenDB(c)
		defer closeDbWrapper(t, db)
	})
}

func TestConnector_Close(t *testing.T) {
	c := newConnectorWrapper(t, ``, nil)
	// Multiple close calls must not cause panics or errors.
	closeConnectorWrapper(t, c)
	closeConnectorWrapper(t, c)
}

func ExampleNewConnector() {
	c, err := NewConnector(`duck.db?access_mode=READ_WRITE`, func(execer driver.ExecerContext) error {
		initQueries := []string{
			`SET memory_limit = '10GB'`,
			`SET threads TO 1`,
		}

		ctx := context.Background()
		for _, query := range initQueries {
			_, err := execer.ExecContext(ctx, query, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("failed to create a new duckdb connector: %s", err)
	}
	defer func() {
		if err = c.Close(); err != nil {
			log.Fatalf("failed to close the connector: %s", err)
		}
	}()

	db := sql.OpenDB(c)
	defer func() {
		if err = db.Close(); err != nil {
			log.Fatalf("failed to close the database: %s", err)
		}
		if err = os.Remove("duck.db"); err != nil {
			log.Fatalf("failed to remove the database file: %s", err)
		}
	}()

	var value string
	row := db.QueryRow(`SELECT value FROM duckdb_settings() WHERE name = 'max_memory'`)
	if row.Scan(&value) != nil {
		log.Fatalf("failed to scan row: %s", err)
	}

	fmt.Printf("The memory_limit is %s.", value)
	// Output: The memory_limit is 9.3 GiB.
}

func TestConnPool(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	db.SetMaxOpenConns(2)
	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	// Get two separate connections and ensure that they're consistent.
	ctx := context.Background()
	conn1 := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn1)
	conn2 := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn2)

	res, err := conn1.ExecContext(ctx, `INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)`, 12345, 1234)
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), ra)

	r, err := conn1.QueryContext(ctx, `SELECT bar FROM foo LIMIT 1`)
	require.NoError(t, err)
	require.True(t, r.Next())
	closeRowsWrapper(t, r)

	r, err = conn2.QueryContext(ctx, `SELECT bar FROM foo LIMIT 1`)
	require.NoError(t, err)
	require.True(t, r.Next())
	closeRowsWrapper(t, r)
}

func TestConnInit(t *testing.T) {
	c := newConnectorWrapper(t, ``, func(execer driver.ExecerContext) error {
		return nil
	})
	defer closeConnectorWrapper(t, c)
	db := sql.OpenDB(c)
	defer closeDbWrapper(t, db)
	db.SetMaxOpenConns(2)

	// Get two separate connections and ensure that they're consistent.
	ctx := context.Background()
	conn1 := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn1)
	conn2 := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn2)

	res, err := conn1.ExecContext(ctx, `CREATE TABLE example (j JSON)`)
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), ra)

	res, err = conn2.ExecContext(ctx, `INSERT INTO example VALUES(' { "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }')`)
	require.NoError(t, err)
	ra, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), ra)

	r, err := conn1.QueryContext(ctx, `SELECT json_valid(j) FROM example`)
	require.NoError(t, err)
	require.True(t, r.Next())
	closeRowsWrapper(t, r)

	r, err = conn2.QueryContext(ctx, `SELECT json_valid(j) FROM example`)
	require.NoError(t, err)
	require.True(t, r.Next())
	closeRowsWrapper(t, r)
}

func TestExec(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)
}

func TestQuery(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)
	createTable(t, db, `CREATE TABLE foo(bar VARCHAR, baz INTEGER)`)

	t.Run("simple", func(t *testing.T) {
		res, err := db.Exec(`INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)`, 12345, 1234)
		require.NoError(t, err)
		ra, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(2), ra)

		r, err := db.Query(`SELECT bar, baz FROM foo WHERE baz > ?`, 12344)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		found := false
		for r.Next() {
			var (
				bar string
				baz int
			)
			err = r.Scan(&bar, &baz)
			require.NoError(t, err)
			require.Equal(t, "lala", bar)
			require.Equal(t, 12345, baz)
			found = true
		}
		require.True(t, found)
	})

	t.Run("large number of rows", func(t *testing.T) {
		res, err := db.Exec(`CREATE TABLE integers AS SELECT * FROM range(0, 100000, 1) t(i)`)
		require.NoError(t, err)
		ra, err := res.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(100000), ra)

		r, err := db.Query(`SELECT i FROM integers ORDER BY i ASC`)
		require.NoError(t, err)
		defer closeRowsWrapper(t, r)

		expected := 0
		for r.Next() {
			var i int
			require.NoError(t, r.Scan(&i))
			require.Equal(t, expected, i)
			expected++
		}
	})

	t.Run("wrong syntax", func(t *testing.T) {
		r, err := db.Query(`SELECT * FROM tbl col = ?`, 1)
		require.Error(t, err)
		defer closeRowsWrapper(t, r)
	})

	t.Run("missing parameter", func(t *testing.T) {
		r, err := db.Query(`SELECT ?`)
		require.Error(t, err)
		defer closeRowsWrapper(t, r)
	})

	t.Run("select NULL", func(t *testing.T) {
		var s *int
		require.NoError(t, db.QueryRow(`SELECT NULL`).Scan(&s))
		require.Nil(t, s)
	})
}

func TestJSON(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	t.Run("SELECT an empty JSON", func(t *testing.T) {
		var res Composite[map[string]any]
		require.NoError(t, db.QueryRow(`SELECT '{}'::JSON`).Scan(&res))
		require.Empty(t, res.Get())
	})

	t.Run("SELECT a marshalled JSON", func(t *testing.T) {
		val, err := json.Marshal(struct {
			Foo string `json:"foo"`
		}{Foo: "bar"})
		require.NoError(t, err)

		var res string
		require.NoError(t, db.QueryRow(`SELECT ?::JSON->>'foo'`, string(val)).Scan(&res))
		require.Equal(t, "bar", res)
	})

	t.Run("SELECT a JSON array", func(t *testing.T) {
		var res Composite[[]any]
		require.NoError(t, db.QueryRow(`SELECT json_array('foo', 'bar')`).Scan(&res))
		require.Len(t, res.Get(), 2)
		require.Equal(t, "foo", res.Get()[0])
		require.Equal(t, "bar", res.Get()[1])
	})
}

func TestEmpty(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	r, err := db.Query(`SELECT 1 WHERE 1 = 0`)
	require.NoError(t, err)
	defer closeRowsWrapper(t, r)

	require.False(t, r.Next())
	require.NoError(t, r.Err())
}

func TestTypeNamesAndScanTypes(t *testing.T) {
	tests := []struct {
		sql      string
		value    any
		typeName string
	}{
		// DUCKDB_TYPE_BOOLEAN
		{
			sql:      `SELECT true AS col`,
			value:    true,
			typeName: "BOOLEAN",
		},
		// DUCKDB_TYPE_TINYINT
		{
			sql:      `SELECT 31::TINYINT AS col`,
			value:    int8(31),
			typeName: "TINYINT",
		},
		// DUCKDB_TYPE_SMALLINT
		{
			sql:      `SELECT 31::SMALLINT AS col`,
			value:    int16(31),
			typeName: "SMALLINT",
		},
		// DUCKDB_TYPE_INTEGER
		{
			sql:      `SELECT 31::INTEGER AS col`,
			value:    int32(31),
			typeName: "INTEGER",
		},
		// DUCKDB_TYPE_BIGINT
		{
			sql:      `SELECT 31::BIGINT AS col`,
			value:    int64(31),
			typeName: "BIGINT",
		},
		// DUCKDB_TYPE_UTINYINT
		{
			sql:      `SELECT 31::UTINYINT AS col`,
			value:    uint8(31),
			typeName: "UTINYINT",
		},
		// DUCKDB_TYPE_USMALLINT
		{
			sql:      `SELECT 31::USMALLINT AS col`,
			value:    uint16(31),
			typeName: "USMALLINT",
		},
		// DUCKDB_TYPE_UINTEGER
		{
			sql:      `SELECT 31::UINTEGER AS col`,
			value:    uint32(31),
			typeName: "UINTEGER",
		},
		// DUCKDB_TYPE_UBIGINT
		{
			sql:      `SELECT 31::UBIGINT AS col`,
			value:    uint64(31),
			typeName: "UBIGINT",
		},
		// DUCKDB_TYPE_FLOAT
		{
			sql:      `SELECT 3.14::FLOAT AS col`,
			value:    float32(3.14),
			typeName: "FLOAT",
		},
		// DUCKDB_TYPE_DOUBLE
		{
			sql:      `SELECT 3.14::DOUBLE AS col`,
			value:    3.14,
			typeName: "DOUBLE",
		},
		// DUCKDB_TYPE_TIMESTAMP
		{
			sql:      `SELECT '1992-09-20 11:30:00'::TIMESTAMP AS col`,
			value:    time.Date(1992, time.September, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP",
		},
		// DUCKDB_TYPE_DATE
		{
			sql:      `SELECT '1992-09-20'::DATE AS col`,
			value:    time.Date(1992, time.September, 20, 0, 0, 0, 0, time.UTC),
			typeName: "DATE",
		},
		// DUCKDB_TYPE_TIME
		{
			sql:      `SELECT '11:30:00'::TIME AS col`,
			value:    time.Date(1, time.January, 1, 11, 30, 0, 0, time.UTC),
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
			sql:      `SELECT 31::HUGEINT AS col`,
			value:    big.NewInt(31),
			typeName: "HUGEINT",
		},
		// DUCKDB_TYPE_VARCHAR
		{
			sql:      `SELECT 'foo'::VARCHAR AS col`,
			value:    "foo",
			typeName: "VARCHAR",
		},
		// DUCKDB_TYPE_BLOB
		{
			sql:      `SELECT 'foo'::BLOB AS col`,
			value:    []byte("foo"),
			typeName: "BLOB",
		},
		// DUCKDB_TYPE_DECIMAL
		{
			sql:      `SELECT 31::DECIMAL(30,17) AS col`,
			value:    Decimal{Value: big.NewInt(3100000000000000000), Width: 30, Scale: 17},
			typeName: "DECIMAL(30,17)",
		},
		// DUCKDB_TYPE_TIMESTAMP_S
		{
			sql:      `SELECT '1992-09-20 11:30:00'::TIMESTAMP_S AS col`,
			value:    time.Date(1992, time.September, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_S",
		},
		// DUCKDB_TYPE_TIMESTAMP_MS
		{
			sql:      `SELECT '1992-09-20 11:30:00'::TIMESTAMP_MS AS col`,
			value:    time.Date(1992, time.September, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_MS",
		},
		// DUCKDB_TYPE_TIMESTAMP_NS
		{
			sql:      `SELECT '1992-09-20 11:30:00'::TIMESTAMP_NS AS col`,
			value:    time.Date(1992, time.September, 20, 11, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMP_NS",
		},
		// DUCKDB_TYPE_LIST
		{
			sql:      `SELECT [['duck', 'goose', 'heron'], NULL, ['frog', 'toad'], []] AS col`,
			value:    []any{[]any{"duck", "goose", "heron"}, nil, []any{"frog", "toad"}, []any{}},
			typeName: "VARCHAR[][]",
		},
		// DUCKDB_TYPE_STRUCT
		{
			sql:      `SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345::DOUBLE} AS col`,
			value:    map[string]any{"key1": "string", "key2": int32(1), "key3": 12.345},
			typeName: `STRUCT("key1" VARCHAR, "key2" INTEGER, "key3" DOUBLE)`,
		},
		{
			sql:      `SELECT {'key1 (,) \ \" "" "': 1} AS col`,
			value:    map[string]any{`key1 (,) \ \" "" "`: int32(1)},
			typeName: `STRUCT("key1 (,) \ \"" """" """ INTEGER)`,
		},
		// DUCKDB_TYPE_MAP
		{
			sql:      `SELECT map([1, 5], ['a', 'e']) AS col`,
			value:    Map{int32(1): "a", int32(5): "e"},
			typeName: "MAP(INTEGER, VARCHAR)",
		},
		// DUCKDB_TYPE_ARRAY
		{
			sql:      `SELECT ['duck', 'goose', NULL]::VARCHAR[3] AS col`,
			value:    []any{"duck", "goose", nil},
			typeName: "VARCHAR[3]",
		},
		// DUCKDB_TYPE_UUID
		{
			sql:      `SELECT '53b4e983-b287-481a-94ad-6e3c90489913'::UUID AS col`,
			value:    []byte{0x53, 0xb4, 0xe9, 0x83, 0xb2, 0x87, 0x48, 0x1a, 0x94, 0xad, 0x6e, 0x3c, 0x90, 0x48, 0x99, 0x13},
			typeName: "UUID",
		},
		// DUCKDB_TYPE_TIME_TZ
		{
			sql:      `SELECT '11:30:00+03'::TIMETZ AS col`,
			value:    time.Date(1, time.January, 1, 8, 30, 0, 0, time.UTC),
			typeName: "TIMETZ",
		},
		// DUCKDB_TYPE_TIMESTAMP_TZ
		{
			sql:      `SELECT '1992-09-20 11:30:00+03'::TIMESTAMPTZ AS col`,
			value:    time.Date(1992, time.September, 20, 8, 30, 0, 0, time.UTC),
			typeName: "TIMESTAMPTZ",
		},
		// DUCKDB_TYPE_UNION
		{
			sql:      `SELECT (123)::UNION(num INTEGER, str VARCHAR) AS col`,
			value:    Union{Tag: "num", Value: int32(123)},
			typeName: "UNION(\"num\" INTEGER, \"str\" VARCHAR)",
		},
		{
			sql:      `SELECT ('hello')::UNION(num INTEGER, str VARCHAR) AS col`,
			value:    Union{Tag: "str", Value: "hello"},
			typeName: "UNION(\"num\" INTEGER, \"str\" VARCHAR)",
		},
		{
			sql:      `SELECT (1.5)::UNION(d DOUBLE, i INTEGER, s VARCHAR) AS col`,
			value:    Union{Tag: "d", Value: float64(1.5)},
			typeName: "UNION(\"d\" DOUBLE, \"i\" INTEGER, \"s\" VARCHAR)",
		},
		{
			sql:      `SELECT ('2024-01-01'::DATE)::UNION(d DATE, s VARCHAR) AS col`,
			value:    Union{Tag: "d", Value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			typeName: "UNION(\"d\" DATE, \"s\" VARCHAR)",
		},
	}

	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	for _, test := range tests {
		t.Run(test.typeName, func(t *testing.T) {
			r, err := db.Query(test.sql)
			require.NoError(t, err)
			defer closeRowsWrapper(t, r)

			cols, err := r.ColumnTypes()
			require.NoError(t, err)
			require.Equal(t, test.typeName, cols[0].DatabaseTypeName())
			require.Equal(t, reflect.TypeOf(test.value), cols[0].ScanType())

			var val any
			require.True(t, r.Next())
			require.NoError(t, r.Scan(&val))
			require.Equal(t, test.value, val)
			require.False(t, r.Next())
		})
	}
}

func TestMultipleStatements(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	// Test an empty query.
	_, err := db.Exec(``)
	require.Error(t, err)
	require.Contains(t, err.Error(), errEmptyQuery.Error())

	// Test invalid query.
	_, err = db.Exec(`abc;`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `syntax error at or near "abc"`)

	// Test a valid query followed by an invalid query.
	_, err = db.Exec(`CREATE TABLE foo (x text); abc;`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `syntax error at or near "abc"`)

	// Test an invalid query followed by a valid query.
	_, err = db.Exec(`abc; CREATE TABLE foo (x text);`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `syntax error at or near "abc"`)

	_, err = db.Exec(`CREATE TABLE foo (x text); CREATE TABLE bar (x text);`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO foo VALUES (?); INSERT INTO bar VALUES (?);`, "hello", "world")
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect argument count for command: have 0 want 1")

	ctx := context.Background()
	conn := openConnWrapper(t, db, ctx)
	defer closeConnWrapper(t, conn)

	res, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS foo1(bar VARCHAR, baz INTEGER); INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?)`, 12345, 1234)
	require.NoError(t, err)
	ra, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(2), ra)

	r, err := conn.QueryContext(ctx, `SELECT bar FROM foo1 LIMIT1`)
	require.NoError(t, err)
	closeRowsWrapper(t, r)

	// args are only applied to the last statement.
	_, err = conn.ExecContext(ctx, `INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?); INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?)`, 12345, 1234)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incorrect argument count for command: have 0 want 2")

	r, err = conn.QueryContext(ctx, `CREATE TABLE foo2(bar VARCHAR, baz INTEGER); INSERT INTO foo2 VALUES ('lala', 12345); SELECT bar FROM foo2 LIMIT 1`)
	require.NoError(t, err)

	var bar string
	require.True(t, r.Next())
	require.NoError(t, r.Scan(&bar))
	require.Equal(t, "lala", bar)
	require.False(t, r.Next())
	closeRowsWrapper(t, r)

	// SELECT with ExecContext also works, but we don't get the result.
	res, err = conn.ExecContext(ctx, `CREATE TABLE foo3(bar VARCHAR, baz INTEGER); INSERT INTO foo3 VALUES ('lala', 12345); SELECT bar FROM foo3 LIMIT 1`)
	require.NoError(t, err)
	ra, err = res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(0), ra)

	// Multiple SELECT, but we get results only for the last one.
	r, err = conn.QueryContext(ctx, `INSERT INTO foo3 VALUES ('lalo', 1234); SELECT bar FROM foo3 WHERE baz = 12345; SELECT bar FROM foo3 WHERE baz = $1`, 1234)
	require.NoError(t, err)
	require.True(t, r.Next())

	require.NoError(t, r.Scan(&bar))
	require.Equal(t, "lalo", bar)
	require.False(t, r.Next())
	closeRowsWrapper(t, r)

	// Test the json extension.
	r, err = conn.QueryContext(ctx, `CREATE TABLE example (id int, j JSON);
		INSERT INTO example VALUES(123, ' { "family": "anatidae", "species": [ "duck", "goose", "swan", null ] }');
		SELECT j->'$.family' FROM example WHERE id=$1`, 123)
	require.NoError(t, err)
	require.True(t, r.Next())

	var family string
	require.NoError(t, r.Scan(&family))
	require.Equal(t, "anatidae", family)
	require.False(t, r.Next())
	closeRowsWrapper(t, r)
}

func TestParquetExtension(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	_, err := db.Exec(`CREATE TABLE users (id INT, name VARCHAR, age INT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO users VALUES (1, 'Jane', 30)`)
	require.NoError(t, err)

	_, err = db.Exec(`COPY (SELECT * FROM users) TO './users.parquet' (FORMAT 'parquet')`)
	require.NoError(t, err)

	type res struct {
		ID   int
		Name string
		Age  int
	}
	row := db.QueryRow(`SELECT * FROM read_parquet('./users.parquet')`)
	var r res
	err = row.Scan(&r.ID, &r.Name, &r.Age)
	require.NoError(t, err)
	require.Equal(t, res{ID: 1, Name: "Jane", Age: 30}, r)
	require.NoError(t, os.Remove("./users.parquet"))
}

func TestQueryTimeout(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*250)
	defer cancel()

	now := time.Now()
	_, err := db.ExecContext(ctx, `CREATE TABLE test AS SELECT * FROM range(10000000) t1, range(1000000) t2`)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// This check is a very defensive time check, but it should be good enough.
	// The query takes significantly longer than 10 seconds.
	require.Less(t, time.Since(now), 10*time.Second)
}

func TestInstanceCache(t *testing.T) {
	// We loop a few times to try and trigger different concurrent behavior.
	for range 20 {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		db := openDbWrapper(t, `instance_cache.db`)
		go func() {
			db1 := openDbWrapper(t, `instance_cache.db`)
			closeDbWrapper(t, db1)
			wg.Done()
		}()
		closeDbWrapper(t, db)
		wg.Wait()
		require.NoError(t, os.Remove("instance_cache.db"))
	}
}

func TestInstanceCacheWithInMemoryDB(t *testing.T) {
	db1 := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db1)

	_, err := db1.Exec(`CREATE TABLE test AS SELECT 1 AS id`)
	require.NoError(t, err)

	db2 := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db2)

	var id int
	err = db2.QueryRow(`SELECT * FROM test`).Scan(&id)
	require.ErrorContains(t, err, "Table with name test does not exist")
}

func TestHugeUnionQuery(t *testing.T) {
	db := openDbWrapper(t, ``)
	defer closeDbWrapper(t, db)

	part := ` UNION SELECT 1`
	query := `SELECT 1`
	for range 350 {
		query += part
	}

	wg := sync.WaitGroup{}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := db.Query(query)
			require.NoError(t, err)
			defer closeRowsWrapper(t, r)
			require.True(t, r.Next())
		}()
	}
	wg.Wait()
}

func Example_simpleConnection() {
	// Connect to DuckDB using '[database/sql.Open]'.
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	defer func() {
		if err = db.Close(); err != nil {
			log.Fatalf("failed to close the database: %s", err)
		}
	}()
	if err != nil {
		log.Fatalf("failed to open connection to duckdb: %s", err)
	}

	ctx := context.Background()

	createStmt := `CREATE table users(name VARCHAR, age INTEGER)`
	_, err = db.ExecContext(ctx, createStmt)
	if err != nil {
		log.Fatalf("failed to create table: %s", err)
	}

	insertStmt := `INSERT INTO users(name, age) VALUES (?, ?)`
	res, err := db.ExecContext(ctx, insertStmt, "Marc", 30)
	if err != nil {
		log.Fatalf("failed to insert users: %s", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Fatal("failed to get number of rows affected")
	}

	fmt.Printf("Inserted %d row(s) into users table", rowsAffected)
	// Output: Inserted 1 row(s) into users table
}
