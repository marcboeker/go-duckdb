package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/marcboeker/go-duckdb"
)

func main() {
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		log.Fatalf("could not initialize new connector: %s", err.Error())
	}

	con, err := c.Connect(context.Background())
	if err != nil {
		log.Fatalf("could not connect: %s", err.Error())
	}

	db := sql.OpenDB(c)
	if _, err := db.Exec(`CREATE TABLE users (name VARCHAR, age INTEGER)`); err != nil {
		log.Fatalf("could not create table users: %s", err.Error())
	}

	a, err := duckdb.NewAppenderFromConn(con, "", "users")
	if err != nil {
		log.Fatalf("could not create new appender for users: %s", err.Error())
	}

	if err := a.AppendRow("Fred", int32(34)); err != nil {
		log.Fatalf("could not append row to users: %s", err.Error())
	}

	if err := a.Close(); err != nil {
		log.Fatalf("could not flush and close appender: %s", err.Error())
	}

	var (
		name string
		age  int
	)
	row := db.QueryRowContext(context.Background(), `SELECT name, age FROM users`)
	if err := row.Scan(&name, &age); err != nil {
		log.Fatalf("could not retrieve user from db: %s", err.Error())
	}

	log.Printf("User: name=%s, age=%d", name, age)
}
