package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
)

func checkErrWithMessage(m string, err error) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", m, err))
	}
}

const CREATE_TABLE = `
    CREATE TABLE IF NOT EXISTS %s (
      item_id		INTEGER,
      status_0		INTEGER,
      number_0		INTEGER,
      number_1		INTEGER,
      text_0     	VARCHAR,
      text_1		VARCHAR,
    )`

const INSERT_INTO = `
        INSERT INTO %s
        VALUES
          (?, ?, ?, ?, ?, ?)`

const ROW_COUNT = 10000

func BenchmarkNaive(b *testing.B) {
	c, err := NewConnector("", nil)
	checkErrWithMessage("failed to connect", err)

	db := sql.OpenDB(c)

	tableName := "naive_table"

	_, err = db.Exec(fmt.Sprintf(CREATE_TABLE, tableName))
	checkErrWithMessage("failed to create table", err)

	// insert ROW_COUNT random row
	for i := 0; i < ROW_COUNT; i++ {
		_, err = db.Exec(fmt.Sprintf(INSERT_INTO, tableName),
			i, rand.Intn(10), rand.Intn(1000), rand.Intn(1000), "ducks are cool", "so are geese")
		checkErrWithMessage("failed to insert row", err)
	}
}

func BenchmarkAppender_AppendRow(b *testing.B) {
	c, err := NewConnector("", nil)
	checkErrWithMessage("failed to connect", err)

	db := sql.OpenDB(c)

	tableName := "appender_table"

	_, err = db.Exec(fmt.Sprintf(CREATE_TABLE, tableName))

	conn, err := c.Connect(context.Background())
	checkErrWithMessage("failed to connect", err)
	defer conn.Close()

	appender, err := NewAppenderFromConn(conn, "", tableName)
	checkErrWithMessage("failed to create appender", err)

	for i := 0; i < ROW_COUNT; i++ {
		err := appender.AppendRow(
			i,
			rand.Intn(10),
			rand.Intn(1000),
			rand.Intn(1000),
			"ducks are cool",
			"so are geese",
		)
		checkErrWithMessage("failed to append row", err)
	}

	appender.Close()
}
