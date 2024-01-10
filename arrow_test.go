package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrow(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	db.SetMaxOpenConns(2) // set connection pool size greater than 1
	defer db.Close()

	t.Run("select_series", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Raw(func(driverConn any) error {
			ar, err := NewArrowFromConn(driverConn)
			require.NoError(t, err)

			rdr, err := ar.Query(context.Background(), "SELECT * FROM generate_series(1, 10)")
			require.NoError(t, err, "should query arrow")
			defer rdr.Release()

			for rdr.Next() {
				rec := rdr.Record()
				require.Equal(t, int64(10), rec.NumRows())
				bs, err := rec.MarshalJSON()
				require.NoError(t, err)

				t.Log(string(bs))
			}

			require.NoError(t, rdr.Err())

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("select_long_series", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Raw(func(driverConn any) error {
			ar, err := NewArrowFromConn(driverConn)
			require.NoError(t, err)

			rdr, err := ar.Query(context.Background(), "SELECT * FROM generate_series(1, 10000)")
			require.NoError(t, err, "should query arrow")
			defer rdr.Release()

			var totalRows int64
			for rdr.Next() {
				rec := rdr.Record()
				totalRows += rec.NumRows()
			}

			require.Equal(t, int64(10000), totalRows)

			require.NoError(t, rdr.Err())

			return nil
		})
		require.NoError(t, err)
	})

	createTable(db, t)

	t.Run("query_table_and_filter_results", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Raw(func(driverConn any) error {
			ar, err := NewArrowFromConn(driverConn)
			require.NoError(t, err)

			rdr, err := ar.Query(context.Background(), "SELECT bar, baz FROM foo WHERE baz > ?", 12344)
			require.NoError(t, err, "should query arrow")
			defer rdr.Release()

			for rdr.Next() {
				rec := rdr.Record()
				require.Equal(t, int64(1), rec.NumRows())
				bs, err := rec.MarshalJSON()
				require.NoError(t, err)

				t.Log(string(bs))
			}

			require.NoError(t, rdr.Err())

			return nil
		})
		require.NoError(t, err)
	})

	t.Run("query error", func(t *testing.T) {
		conn, err := db.Conn(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		err = conn.Raw(func(driverConn any) error {
			ar, err := NewArrowFromConn(driverConn)
			require.NoError(t, err)

			_, err = ar.Query(context.Background(), "select bar")
			require.Error(t, err)

			return nil
		})
		require.NoError(t, err)
	})
}
