// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	// Use second argument to store DB on disk
	// db, err := sql.Open("duckdb", "foobar.db")

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	check(db.Ping())

	check(db.Exec("CREATE TABLE users(name VARCHAR, age INTEGER)"))
	check(db.Exec("INSERT INTO users VALUES('marc', 33)"))
	check(db.Exec("INSERT INTO users VALUES('macgyver', 55)"))

	type user struct {
		name string
		age  int
	}

	rows, err := db.Query("SELECT name, age FROM users WHERE name = ? OR name = ? AND age > ?", "macgyver", "marc", 30)
	check(err)
	defer rows.Close()

	for rows.Next() {
		u := new(user)
		err := rows.Scan(&u.name, &u.age)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s is %d years old\n", u.name, u.age)
	}
	check(rows.Err())

	check(db.Exec("DELETE FROM users"))
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
