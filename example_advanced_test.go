package duckdb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/marcboeker/go-duckdb"
)

// ExampleAdvancedConnection describes the 'advanced' method of connecting to DuckDB. Users can use
// this way to install extensions, configure DuckDB's settings, etc.
func Example_advancedConnection() {
	var ctx = context.Background()
	connector, err := duckdb.NewConnector("duckdb?access_mode=READ_WRITE", func(execer driver.ExecerContext) error {
		initCommands := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
		}
		for _, cmd := range initCommands {
			timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			_, err := execer.ExecContext(timeoutCtx, cmd, nil)
			if err != nil {
				return fmt.Errorf("failed to run init command: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("failed to create new duckdb connector: %s", err)
	}

	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	var installed, loaded bool
	row := db.QueryRowContext(ctx, "SELECT loaded, installed FROM duckdb_extensions() WHERE extension_name = 'json'")
	err = row.Scan(&loaded, &installed)
	if err != nil {
		log.Fatalf("failed to scan row: %s", err)
	}

	fmt.Printf("JSON Extension Status: installed: %t, loaded: %t", installed, loaded)
	// Output: JSON Extension Status: installed: true, loaded: true
}
