package duckdb_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

// ExampleSimpleConnection demonstrates the 'simple' way of connecting to an in-process DuckDB database.
// For more details on database usage, see the [database/sql.Open] documentation.
func Example_simpleConnection() {
	// Connect to DuckDB using the normal '[database/sql.Open]' methodology
	var ctx = context.Background()
	db, err := sql.Open("duckdb", "?access_mode=READ_WRITE")
	if err != nil {
		log.Fatalf("failed to open connection to duckdb: %s", err)
	}

	defer db.Close()

	_, err = db.ExecContext(ctx, "CREATE table users(name VARCHAR, age INTEGER)")
	if err != nil {
		log.Fatalf("failed to create users table: %s", err)
	}

	const insertStatement = "INSERT INTO users(name, age) VALUES (?, ?)"

	res, err := db.ExecContext(ctx, insertStatement, "Marc", 30)
	if err != nil {
		log.Fatalf("failed to insert to users table: %s", err)
	}

	numInserted, err := res.RowsAffected()
	if err != nil {
		log.Fatalf("failed to get rows affected: %s", err)
	}

	fmt.Printf("Inserted %d row into users table", numInserted)
	// Output: Inserted 1 row into users table
}
