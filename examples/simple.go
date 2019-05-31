// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	db *sql.DB
)

type user struct {
	name    string
	age     int
	height  float32
	awesome bool
	bday    time.Time
}

func main() {
	// Use second argument to store DB on disk
	// db, err := sql.Open("duckdb", "foobar.db")

	var err error
	db, err = sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	check(db.Ping())

	check(db.Exec("CREATE TABLE users(name VARCHAR, age INTEGER, height FLOAT, awesome BOOLEAN, bday DATE)"))
	check(db.Exec("INSERT INTO users VALUES('marc', 99, 1.91, true, '1970-01-01')"))
	check(db.Exec("INSERT INTO users VALUES('macgyver', 70, 1.85, true, '1951-01-23')"))

	rows, err := db.Query(`
		SELECT name, age, height, awesome, bday
		FROM users 
		WHERE (name = ? OR name = ?) AND age > ? AND awesome = ?`,
		"macgyver", "marc", 30, true,
	)
	check(err)
	defer rows.Close()

	for rows.Next() {
		u := new(user)
		err := rows.Scan(&u.name, &u.age, &u.height, &u.awesome, &u.bday)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"%s is %d years old, %.2f tall, bday on %s and has awesomeness: %t\n",
			u.name, u.age, u.height, u.bday.Format(time.RFC3339), u.awesome,
		)
	}
	check(rows.Err())

	res, err := db.Exec("DELETE FROM users")
	check(err)

	ra, _ := res.RowsAffected()
	log.Printf("Deleted %d rows\n", ra)

	runTransaction()
	testPreparedStmt()
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

func runTransaction() {
	log.Println("Starting transaction...")
	tx, err := db.Begin()
	check(err)

	check(tx.Exec("INSERT INTO users VALUES('gru', 25, 1.35, false, '1996-04-03')"))
	row := tx.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	var count int64
	check(row.Scan(&count))
	if count > 0 {
		log.Println("User Gru was inserted")
	}

	log.Println("Rolling back transaction...")
	check(tx.Rollback())

	row = db.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	check(row.Scan(&count))
	if count > 0 {
		log.Println("Found user Gru")
	} else {
		log.Println("Didn't find user Gru")
	}
}

func testPreparedStmt() {
	stmt, err := db.Prepare("INSERT INTO users VALUES(?, ?, ?, ?, ?)")
	check(err)
	defer stmt.Close()

	check(stmt.Exec("Kevin", 11, 0.55, true, "2013-07-06"))
	check(stmt.Exec("Bob", 12, 0.73, true, "2012-11-04"))
	check(stmt.Exec("Stuart", 13, 0.66, true, "2014-02-12"))

	stmt, err = db.Prepare("SELECT * FROM users WHERE age > ?")
	check(err)

	rows, err := stmt.Query(1)
	check(err)
	defer rows.Close()

	for rows.Next() {
		u := new(user)
		err := rows.Scan(&u.name, &u.age, &u.height, &u.awesome, &u.bday)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf(
			"%s is %d years old, %.2f tall, bday on %s and has awesomeness: %t\n",
			u.name, u.age, u.height, u.bday.Format(time.RFC3339), u.awesome,
		)
	}
}
