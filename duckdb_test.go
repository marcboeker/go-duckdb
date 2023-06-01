package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

	t.Run("with invalid config", func(t *testing.T) {
		_, err := sql.Open("duckdb", "?threads=NaN")

		if !errors.Is(err, prepareConfigError) {
			t.Fatal("invalid config should not be accepted")
		}
	})
}

func TestConnPool(t *testing.T) {
	db := openDB(t)
	db.SetMaxOpenConns(2) // set connection pool size greater than 1
	defer db.Close()
	createTable(db, t)

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
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
		}

		for _, qry := range bootQueries {
			_, err := execer.ExecContext(context.Background(), qry, nil)
			if err != nil {
				return err
			}
		}
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
	require.NotNil(t, createTable(db, t))
}

func TestQuery(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

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
			assert.Equal(t, expected, i)
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

func TestStruct(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	createTable(db, t)

	t.Run("scan single struct", func(t *testing.T) {
		row := db.QueryRow("SELECT {'x': 1, 'y': 2}")

		type point struct {
			X int
			Y int
		}

		want := Composite[point]{
			point{1, 2},
		}

		var result Composite[point]
		require.NoError(t, row.Scan(&result))
		require.Equal(t, want, result)
	})

	t.Run("scan slice of structs", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO foo VALUES('lala', ?), ('lala', ?), ('lalo', 42), ('lalo', 43)", 12345, 12346)
		require.NoError(t, err)

		type row struct {
			Bar string
			Baz int
		}

		want := []Composite[row]{
			{row{"lalo", 42}},
			{row{"lalo", 43}},
			{row{"lala", 12345}},
			{row{"lala", 12346}},
		}

		rows, err := db.Query("SELECT ROW(bar, baz) FROM foo ORDER BY baz")
		require.NoError(t, err)
		defer rows.Close()

		var result []Composite[row]
		for rows.Next() {
			var res Composite[row]
			err := rows.Scan(&res)
			require.NoError(t, err)
			result = append(result, res)
		}
		require.Equal(t, want, result)
	})
}

func TestMap(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("select map", func(t *testing.T) {
		var m Map
		require.NoError(t, db.QueryRow("SELECT map(['foo', 'bar'], ['a', 'e'])").Scan(&m))
		require.Equal(t, Map{
			"foo": "a",
			"bar": "e",
		}, m)
	})

	t.Run("select nested map", func(t *testing.T) {
		var m Map
		err := db.QueryRow("SELECT map([map([1], [1]), map([2], [2])], ['a', 'e'])").Scan(&m)
		require.ErrorIs(t, err, errUnsupportedMapKeyType)
	})

	t.Run("insert map", func(t *testing.T) {
		tests := []struct {
			sqlType string
			input   Map
		}{
			// {
			// 	"MAP(VARCHAR, VARCHAR)",
			// 	Map{"foo": "bar"},
			// },
			// {
			// 	"MAP(INTEGER, VARCHAR)",
			// 	Map{int32(1): "bar"},
			// },
			// {
			// 	"MAP(VARCHAR, BIGINT)",
			// 	Map{"foo": int64(2)},
			// },
			// {
			// 	"MAP(VARCHAR, BOOLEAN)",
			// 	Map{"foo": true},
			// },
			// {
			// 	"MAP(DOUBLE, VARCHAR)",
			// 	Map{1.1: "foobar"},
			// },
		}
		for i, test := range tests {
			_, err := db.Exec(fmt.Sprintf("CREATE TABLE map_test_%d(properties %s)", i, test.sqlType))
			require.NoError(t, err)

			_, err = db.Exec(fmt.Sprintf("INSERT INTO map_test_%d VALUES(?)", i), test.input)
			require.NoError(t, err, test.input)

			var m Map
			require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT properties FROM map_test_%d", i)).Scan(&m))
			require.Equal(t, test.input, m)
		}
	})
}

func TestList(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("integer list", func(t *testing.T) {
		const n = 1000
		var row Composite[[]int]
		require.NoError(t, db.QueryRow("SELECT range(0, ?, 1)", n).Scan(&row))
		require.Equal(t, n, len(row.Get()))
		for i := 0; i < n; i++ {
			require.Equal(t, i, row.Get()[i])
		}
	})
}

func compareDecimal(t *testing.T, want Decimal, got Decimal) {
	require.Equal(t, want.Scale, got.Scale)
	require.Equal(t, want.Width, got.Width)
	require.Equal(t, want.Value.String(), got.Value.String())
}

func TestDecimal(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("decimal widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			var f Decimal
			require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT 0::DECIMAL(%d, 1)", i)).Scan(&f))
			require.Equal(t, Decimal{Width: uint8(i), Value: big.NewInt(0), Scale: 1}, f)
		}
	})

	t.Run("multiple decimal types", func(t *testing.T) {
		rows, err := db.Query(`SELECT * FROM (VALUES
			(1.23::DECIMAL(3, 2)),
			(123.45::DECIMAL(5, 2)),
			(123456789.01::DECIMAL(11, 2)),
			(1234567890123456789.234::DECIMAL(22, 3)),
		) v
		ORDER BY v ASC`)
		require.NoError(t, err)
		defer rows.Close()

		bigNumber, success := new(big.Int).SetString("1234567890123456789234", 10)
		require.True(t, success, "failed to parse big number")
		tests := []struct {
			input string
			want  Decimal
		}{
			{input: "1.23::DECIMAL(3, 2)", want: Decimal{Value: big.NewInt(123), Width: 3, Scale: 2}},
			{input: "123.45::DECIMAL(5, 2)", want: Decimal{Value: big.NewInt(12345), Width: 5, Scale: 2}},
			{input: "123456789.01::DECIMAL(11, 2)", want: Decimal{Value: big.NewInt(12345678901), Width: 11, Scale: 2}},
			{input: "1234567890123456789.234::DECIMAL(22, 3)", want: Decimal{Value: bigNumber, Width: 22, Scale: 3}},
		}
		for _, tc := range tests {
			row := db.QueryRow(fmt.Sprintf("SELECT %s", tc.input))
			var fs Decimal
			require.NoError(t, row.Scan(&fs))
			compareDecimal(t, tc.want, fs)
		}
	})

	t.Run("huge decimal", func(t *testing.T) {
		bigNumber, success := new(big.Int).SetString("12345678901234567890123456789", 10)
		require.True(t, success, "failed to parse big number")
		var f Decimal
		require.NoError(t, db.QueryRow("SELECT 123456789.01234567890123456789::DECIMAL(29, 20)").Scan(&f))
		compareDecimal(t, Decimal{Value: bigNumber, Width: 29, Scale: 20}, f)
	})
}

func TestUUID(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TABLE uuid_test(uuid UUID)")
	require.NoError(t, err)

	tests := []uuid.UUID{
		uuid.New(),
		uuid.Nil,
		uuid.MustParse("80000000-0000-0000-0000-200000000000"),
	}
	for _, v := range tests {
		_, err := db.Exec("INSERT INTO uuid_test VALUES(?)", v)
		require.NoError(t, err)

		var uuid uuid.UUID
		require.NoError(t, db.QueryRow("SELECT uuid FROM uuid_test WHERE uuid = ?", v).Scan(&uuid))
		require.Equal(t, v, uuid)

		require.NoError(t, db.QueryRow("SELECT ?", v).Scan(&uuid))
		require.Equal(t, v, uuid)

		require.NoError(t, db.QueryRow("SELECT ?::uuid", v).Scan(&uuid))
		require.Equal(t, v, uuid)
	}
}

func TestENUMs(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	defer db.Close()

	_, err := db.Exec("CREATE TYPE element AS ENUM ('Sea', 'Air', 'Land')")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE vehicles (name text, environment element)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO vehicles VALUES (?, ?), (?, ?)", "Aircraft", "Air", "Boat", "Sea")
	require.NoError(t, err)

	var name string
	var env string
	require.NoError(t, db.QueryRow("SELECT name, CAST(environment AS text) FROM vehicles WHERE environment = ?", "Air").Scan(&name, &env))
	require.Equal(t, "Aircraft", name)
	require.Equal(t, "Air", env)
}

func TestHugeInt(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("scan hugeint", func(t *testing.T) {
		tests := []string{
			"0",
			"1",
			"-1",
			"9223372036854775807",
			"-9223372036854775808",
			"170141183460469231731687303715884105727",
			"-170141183460469231731687303715884105727",
		}
		for _, expected := range tests {
			t.Run(expected, func(t *testing.T) {
				var res *big.Int
				err := db.QueryRow(fmt.Sprintf("SELECT %s::HUGEINT", expected)).Scan(&res)
				require.NoError(t, err)
				require.Equal(t, expected, res.String())
			})
		}
	})

	t.Run("bind hugeint", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE hugeint_test (number HUGEINT)")
		require.NoError(t, err)

		val := big.NewInt(1)
		val.SetBit(val, 101, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", val)
		require.NoError(t, err)

		var res *big.Int
		err = db.QueryRow("SELECT number FROM hugeint_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val.String(), res.String())

		tooHuge := big.NewInt(1)
		tooHuge.SetBit(tooHuge, 129, 1)
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", tooHuge)
		require.Error(t, err)
		require.Contains(t, err.Error(), "too big")
	})
}

func TestVarchar(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var s string

	t.Run("size is smaller than 12", func(t *testing.T) {
		require.NoError(t, db.QueryRow("select 'inline'").Scan(&s))
		require.Equal(t, "inline", s)
	})

	t.Run("size is exactly 12", func(t *testing.T) {
		require.NoError(t, db.QueryRow("select 'inlineSize12'").Scan(&s))
		require.Equal(t, "inlineSize12", s)
	})

	t.Run("size is 13", func(t *testing.T) {
		require.NoError(t, db.QueryRow("select 'inlineSize13_'").Scan(&s))
		require.Equal(t, "inlineSize13_", s)
	})

	t.Run("size is greater than 12", func(t *testing.T) {
		const q = "uninlined string that is greater than 12 bytes long"
		require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT '%s'", q)).Scan(&s))
		require.Equal(t, q, s)
	})
}

func TestBlob(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var bytes []byte

	t.Run("select as string", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT 'AB'::BLOB;").Scan(&bytes))
		require.Equal(t, "AB", string(bytes))
	})

	t.Run("select as hex", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT '\\xAA'::BLOB").Scan(&bytes))
		require.Equal(t, []byte{0xAA}, bytes)
	})
}

func TestBoolean(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	var res bool

	t.Run("scan", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT true").Scan(&res))
		require.Equal(t, true, res)

		require.NoError(t, db.QueryRow("SELECT false").Scan(&res))
		require.Equal(t, false, res)
	})

	t.Run("bind", func(t *testing.T) {
		require.NoError(t, db.QueryRow("SELECT ?", true).Scan(&res))
		require.Equal(t, true, res)

		require.NoError(t, db.QueryRow("SELECT ?", false).Scan(&res))
		require.Equal(t, false, res)

		require.NoError(t, db.QueryRow("SELECT ?", 0).Scan(&res))
		require.Equal(t, false, res)

		require.NoError(t, db.QueryRow("SELECT ?", 1).Scan(&res))
		require.Equal(t, true, res)
	})
}

func TestJSON(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	loadJSONExt(t, db)

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
}

// CAST(? as DATE) generate result of type Date (time.Time)
func TestDate(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	tests := map[string]struct {
		want  time.Time
		input string
	}{
		"epoch":       {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970": {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":  {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res time.Time
			err := db.QueryRow("SELECT CAST(? as DATE)", tc.input).Scan(&res)
			require.NoError(t, err)
			require.Equal(t, tc.want, res)
		})
	}
}

func TestTimestamp(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()
	tests := map[string]struct {
		input string
		want  time.Time
	}{
		"epoch":         {input: "1970-01-01", want: time.UnixMilli(0).UTC()},
		"before 1970":   {input: "1950-12-12", want: time.Date(1950, 12, 12, 0, 0, 0, 0, time.UTC)},
		"after 1970":    {input: "2022-12-12", want: time.Date(2022, 12, 12, 0, 0, 0, 0, time.UTC)},
		"HH:MM:SS":      {input: "2022-12-12 11:35:43", want: time.Date(2022, 12, 12, 11, 35, 43, 0, time.UTC)},
		"HH:MM:SS.DDDD": {input: "2022-12-12 11:35:43.5678", want: time.Date(2022, 12, 12, 11, 35, 43, 567800000, time.UTC)},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var res time.Time
			if err := db.QueryRow("SELECT CAST(? as TIMESTAMP)", tc.input).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if res != tc.want {
				t.Errorf("expected value %v != resulting value %v", tc.want, res)
			}
		})
	}
}

func TestInterval(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("bind interval", func(t *testing.T) {
		interval := Interval{Days: 10, Months: 4, Micros: 4}
		row := db.QueryRow("SELECT ?::INTERVAL", interval)

		var res Interval
		require.NoError(t, row.Scan(&res))
		require.Equal(t, interval, res)
	})

	t.Run("scan interval", func(t *testing.T) {
		tests := map[string]struct {
			input string
			want  Interval
		}{
			"simple interval": {
				input: "INTERVAL 5 HOUR",
				want:  Interval{Days: 0, Months: 0, Micros: 18000000000},
			},
			"interval arithmetic": {
				input: "INTERVAL 1 DAY + INTERVAL 5 DAY",
				want:  Interval{Days: 6, Months: 0, Micros: 0},
			},
			"timestamp arithmetic": {
				input: "CAST('2022-05-01' as TIMESTAMP) - CAST('2022-04-01' as TIMESTAMP)",
				want:  Interval{Days: 30, Months: 0, Micros: 0},
			},
		}
		for name, tc := range tests {
			t.Run(name, func(t *testing.T) {
				var res Interval
				require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT %s", tc.input)).Scan(&res))
				require.Equal(t, tc.want, res)
			})
		}
	})
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT 1 WHERE 1 = 0`)
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestEmptyArrow(t *testing.T) {
	t.Parallel()
	c, err := NewConnector("", nil)
	require.NoError(t, err)

	conn, err := c.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	arrowQuery, err := NewArrowQueryFromConn(conn)
	require.NoError(t, err)

	r, err := arrowQuery.QueryContext(context.Background(), `SELECT 1 WHERE 1 = 0`)
	require.NoError(t, err)
	defer r.Release()
	require.Equal(t, 1, r.NumRows())
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
			rows.Scan(&val)
			require.Equal(t, test.value, val)
		})
	}
}

// Running multiple statements in a single query. All statements except the last one are executed and if no error then last statement is executed with args and result returned.
func TestMultipleStatements(t *testing.T) {
	db := openDB(t)
	defer db.Close()

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
	res, err = conn.ExecContext(context.Background(), "INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?); INSERT INTO foo1 VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
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
	rows, err = conn.QueryContext(context.Background(), `INSTALL 'json'; LOAD 'json'; CREATE TABLE example (id int, j JSON);
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

	err = conn.Close()
	require.NoError(t, err)
}

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func loadJSONExt(t *testing.T, db *sql.DB) {
	_, err := db.Exec("INSTALL 'json'")
	require.NoError(t, err)
	_, err = db.Exec("LOAD 'json'")
	require.NoError(t, err)
}

func createTable(db *sql.DB, t *testing.T) *sql.Result {
	res, err := db.Exec("CREATE TABLE foo(bar VARCHAR, baz INTEGER)")
	require.NoError(t, err)
	return &res
}
