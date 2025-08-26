package main

import (
	"database/sql"
	"log"

	"github.com/marcboeker/go-duckdb/v2"
)

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE")

	check(err)
	defer func() { check(db.Close()) }()

	check(db.Ping())

	var jsonArray duckdb.Composite[[]any]
	row := db.QueryRow(`SELECT json_array('foo', 'bar');`)
	check(row.Err())
	check(row.Scan(&jsonArray))

	log.Printf("first element: %s \n", jsonArray.Get()[0])
	log.Printf("second element: %s \n", jsonArray.Get()[1])

	var jsonMap duckdb.Composite[map[string]any]
	row = db.QueryRow(`SELECT '{"family": "anatidae", "species": ["duck", "goose"], "coolness": 42.42}'::JSON;`)
	check(row.Err())
	check(row.Scan(&jsonMap))

	log.Printf("family: %s", jsonMap.Get()["family"])
	log.Printf("species: %s", jsonMap.Get()["species"])
	log.Printf("coolness: %f", jsonMap.Get()["coolness"])
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}
