# Go SQL driver for [DuckDB](https://github.com/duckdb/duckdb)

The DuckDB driver conforms to the built-in `database/sql` interface.

![Tests status](https://github.com/marcboeker/go-duckdb/actions/workflows/tests.yaml/badge.svg)

## Installation

```
go get github.com/marcboeker/go-duckdb
```

`go-duckdb` uses `CGO` to make calls to DuckDB. You must build your binaries with `CGO_ENABLED=1`.

## Usage

`go-duckdb` hooks into the `database/sql` interface provided by the Go `stdlib`. To open a connection, simply specify the driver type as `duckdb`.

```go
db, err := sql.Open("duckdb", "")
if err != nil {
    ...
}
defer db.Close()
```

This creates an in-memory instance of DuckDB. To open a persistent database, you need to specify a filepath to the database file. If
the file does not exist, then DuckDB creates it.


```go
db, err := sql.Open("duckdb", "/path/to/foo.db")
if err != nil {
	...
}
defer db.Close()
```

If you want to set specific [config options for DuckDB](https://duckdb.org/docs/sql/configuration), you can add them as query style parameters in the form of `name=value` pairs to the DSN.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db?access_mode=read_only&threads=4")
if err != nil {
    ...
}
defer db.Close()
```

Alternatively, you can use [sql.OpenDB](https://cs.opensource.google/go/go/+/go1.21.6:src/database/sql/sql.go;l=781). That way, you can perform initialization steps in a callback function before opening the database.
Here's an example that installs and loads the JSON extension when opening a database with `sql.OpenDB(connector)`.

```go
connector, err := duckdb.NewConnector("/path/to/foo.db?access_mode=read_only&threads=4", func(execer driver.Execer) error {
    bootQueries := []string{
        "INSTALL 'json'",
        "LOAD 'json'",
    }

    for _, query := range bootQueries {
        _, err = execer.Exec(query, nil)
        if err != nil {
            ...
        }
    }
    return nil
})
if err != nil {
    ...
}

db := sql.OpenDB(connector)
defer db.Close()
```

Please refer to the [database/sql](https://godoc.org/database/sql) documentation for further usage instructions.

## A Note on Memory Allocation

DuckDB lives in-process. Therefore, all its memory lives in the driver. All allocations live in the host process, which
is the Go application. Especially for long-running applications, it is crucial to call the corresponding `Close`-functions as specified
in [database/sql](https://godoc.org/database/sql). The following is a list of examples.
```go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

rows, err := conn.QueryContext(context.Background(), "SELECT 42")
// alternatively, rows.Next() has to return false
rows.Close()

appender, err := NewAppenderFromConn(conn, "", "test")
defer appender.Close()

// if not passed to sql.OpenDB
connector, err := NewConnector("", nil)
defer connector.Close()
```

## DuckDB Appender API

If you want to use the [DuckDB Appender API](https://duckdb.org/docs/data/appender.html), you can obtain a new `Appender` by passing a DuckDB connection to `NewAppenderFromConn()`.

```go
connector, err := duckdb.NewConnector("test.db", nil)
if err != nil {
	...
}
defer connector.Close()

conn, err := connector.Connect(context.Background())
if err != nil {
	...
}
defer conn.Close()

// obtain an appender from the connection
// NOTE: the table 'test_tbl' must exist in test.db
appender, err := NewAppenderFromConn(conn, "", "test_tbl")
if err != nil {
	...
}
defer appender.Close()

err = appender.AppendRow(...)
if err != nil {
	...
}
```

## DuckDB Apache Arrow Interface

If you want to use the [DuckDB Arrow Interface](https://duckdb.org/docs/api/c/api#arrow-interface), you can obtain a new `Arrow` by passing a DuckDB connection to `NewArrowFromConn()`.

```go
connector, err := duckdb.NewConnector("", nil)
if err != nil {
	...
}
defer connector.Close()

conn, err := connector.Connect(context.Background())
if err != nil {
	...
}
defer conn.Close()

// obtain the Arrow from the connection
arrow, err := duckdb.NewArrowFromConn(conn)
if err != nil w
	...
}

rdr, err := arrow.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10)")
if err != nil {
	...
}
defer rdr.Release()

for rdr.Next() {
  // process records
}
```

## Linking DuckDB

By default, `go-duckdb` statically links DuckDB into your binary. Statically linking DuckDB adds around 30 MB to your binary size. On Linux (Intel) and macOS (Intel and ARM), `go-duckdb` bundles pre-compiled static libraries for fast builds.

Alternatively, you can dynamically link DuckDB by passing `-tags=duckdb_use_lib` to `go build`. You must have a copy of `libduckdb` available on your system (`.so` on Linux or `.dylib` on macOS), which you can download from the DuckDB [releases page](https://github.com/duckdb/duckdb/releases). For example:

```sh
# On Linux
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go build -tags=duckdb_use_lib main.go
LD_LIBRARY_PATH=/path/to/libs ./main

# On macOS
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go build -tags=duckdb_use_lib main.go
DYLD_LIBRARY_PATH=/path/to/libs ./main
```
