package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/georgysavva/scany/sqlscan"
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

	t.Run("with invalid config", func(t *testing.T) {
		db, _ := sql.Open("duckdb", "?threads=NaN")
		err := db.Ping()

		if !errors.Is(err, prepareConfigError) {
			t.Fatal("invalid config should not be accepted")
		}
	})
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
		_, err := db.Exec("INSERT INTO foo VALUES ('lala', ?), ('lalo', ?)", 12345, 1234)
		require.NoError(t, err)

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

	t.Run("select struct", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO foo VALUES('lala', ?), ('lala', ?), ('lalo', 42), ('lalo', 43)", 12345, 12346)
		require.NoError(t, err)

		type row struct {
			Bar string
			Baz int
		}

		want := []row{
			{"lalo", 42},
			{"lalo", 43},
			{"lala", 12345},
			{"lala", 12346},
		}

		rows, err := db.Query("SELECT ROW(bar, baz) FROM foo ORDER BY baz")
		require.NoError(t, err)
		defer rows.Close()

		i := 0
		for rows.Next() {
			res := Composite[row]{}
			err := rows.Scan(&res)
			require.NoError(t, err)
			require.Equal(t, want[i], res.Get())
			i++
		}
	})

	t.Run("select struct slice", func(t *testing.T) {
		type point struct {
			X int
			Y int
		}
		var row Composite[[]point]
		require.NoError(t, sqlscan.Get(context.Background(), db, &row, "SELECT ARRAY[{'x': 1, 'y': 2}, {'x': 3, 'y': 4}]"))
		require.Equal(t, 2, len(row.Get()))
		require.Equal(t, point{1, 2}, row.Get()[0])
		require.Equal(t, point{3, 4}, row.Get()[1])
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

func TestDecimal(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("decimal widths", func(t *testing.T) {
		for i := 1; i <= 38; i++ {
			var f float64 = 999
			require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT 0::DECIMAL(%d, 1)", i)).Scan(&f))
			require.Equal(t, float64(0), f)
		}
	})

	t.Run("multiple rows", func(t *testing.T) {
		rows, err := db.Query(`SELECT * FROM (VALUES
			(1.23 :: DECIMAL(3, 2)),
			(123.45 :: DECIMAL(5, 2)),
			(123456789.01 :: DECIMAL(11, 2)),
			(1234567890123456789.234 :: DECIMAL(22, 3)),
		) v
		ORDER BY v ASC`)
		require.NoError(t, err)
		defer rows.Close()

		want := []float64{1.23, 123.45, 123456789.01, 1234567890123456789.234}
		i := 0
		for rows.Next() {
			var fs float64
			require.NoError(t, rows.Scan(&fs))
			require.Equal(t, want[i], fs)
			i++
		}
	})

	t.Run("huge decimal", func(t *testing.T) {
		var f float64
		require.NoError(t, db.QueryRow("SELECT 123456789.01234567890123456789 :: DECIMAL(38, 20)").Scan(&f))
		require.True(t, math.Abs(float64(123456789.01234567890123456789)-f) < 0.0000001)
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

	var str string
	require.NoError(t, db.QueryRow("SELECT name FROM vehicles WHERE environment = ?", "Air").Scan(&str))
	require.Equal(t, "Aircraft", str)
}

func TestHugeInt(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	t.Run("scan HugeInt", func(t *testing.T) {
		for _, expected := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64} {
			t.Run(fmt.Sprintf("sum(%d)", expected), func(t *testing.T) {
				var res HugeInt
				err := db.QueryRow("SELECT SUM(?)", expected).Scan(&res)
				require.NoError(t, err)

				expct, err := res.Int64()
				require.NoError(t, err)
				require.Equal(t, expected, expct)
			})
		}
	})

	t.Run("bind HugeInt", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE hugeint_test (number HUGEINT)")
		require.NoError(t, err)

		val := HugeInt{upper: 1, lower: 2}
		_, err = db.Exec("INSERT INTO hugeint_test VALUES(?)", val)
		require.NoError(t, err)

		var res HugeInt
		err = db.QueryRow("SELECT number FROM hugeint_test WHERE number = ?", val).Scan(&res)
		require.NoError(t, err)
		require.Equal(t, val, res)
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
		require.NoError(t, db.QueryRow(`SELECT ?::JSON->>'foo'`, val).Scan(&data))
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
			if err := db.QueryRow(fmt.Sprintf("SELECT %s", tc.input)).Scan(&res); err != nil {
				t.Errorf("can not scan value %v", err)
			} else if !reflect.DeepEqual(tc.want, res) {
				t.Errorf("expected value %v != resulting value %v", tc.want, res)
			}
		})
	}
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
