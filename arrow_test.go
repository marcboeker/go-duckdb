package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrow(t *testing.T) {
	t.Parallel()
	db := openDB(t)
	defer db.Close()

	createTable(db, t)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	t.Run("select series", func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)
		defer c.Close()

		conn, err := c.Connect(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		ar, err := NewArrowFromConn(conn)
		require.NoError(t, err)

		rdr, err := ar.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10)")
		require.NoError(t, err)
		defer rdr.Release()

		for rdr.Next() {
			rec := rdr.Record()
			require.Equal(t, int64(10), rec.NumRows())
			require.NoError(t, err)
		}

		require.NoError(t, rdr.Err())
	})

	t.Run("select long series", func(t *testing.T) {
		c, err := NewConnector("", nil)
		require.NoError(t, err)
		defer c.Close()

		conn, err := c.Connect(context.Background())
		require.NoError(t, err)
		defer conn.Close()

		ar, err := NewArrowFromConn(conn)
		require.NoError(t, err)

		rdr, err := ar.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10000)")
		require.NoError(t, err)
		defer rdr.Release()

		var totalRows int64
		for rdr.Next() {
			rec := rdr.Record()
			totalRows += rec.NumRows()
		}

		require.Equal(t, int64(10000), totalRows)

		require.NoError(t, rdr.Err())
	})

	t.Run("query table and filter results", func(t *testing.T) {
		err = conn.Raw(func(driverConn any) error {
			conn, ok := driverConn.(driver.Conn)
			require.True(t, ok)

			ar, err := NewArrowFromConn(conn)
			require.NoError(t, err)

			_, err = db.ExecContext(context.Background(), "INSERT INTO foo (bar, baz) VALUES ('lala', 2), ('dada', 3)")
			require.NoError(t, err)

			reader, err := ar.QueryContext(context.Background(), "SELECT bar, baz FROM foo WHERE baz > ?", 1)
			require.NoError(t, err)
			defer reader.Release()

			for reader.Next() {
				rec := reader.Record()
				require.Equal(t, int64(2), rec.NumRows())
				require.Equal(t, "lala", rec.Column(0).ValueStr(0))
				require.Equal(t, "dada", rec.Column(0).ValueStr(1))
			}
			require.NoError(t, reader.Err())
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("query error", func(t *testing.T) {
		err := conn.Raw(func(driverConn any) error {
			conn, ok := driverConn.(driver.Conn)
			require.True(t, ok)

			ar, err := NewArrowFromConn(conn)
			require.NoError(t, err)

			_, err = ar.QueryContext(context.Background(), "SELECT bar")
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)
	})
}
