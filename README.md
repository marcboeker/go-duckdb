# Go SQL Driver For [DuckDB](https://github.com/duckdb/duckdb)

The DuckDB driver conforms to the built-in `database/sql` interface.

Current DuckDB version: `v1.2.0`.

![Tests status](https://github.com/marcboeker/go-duckdb/actions/workflows/tests.yaml/badge.svg)
[![GoDoc](https://godoc.org/github.com/marcboeker/go-duckdb?status.svg)](https://pkg.go.dev/github.com/marcboeker/go-duckdb)

```diff
! Starting with v2.0.0, go-duckdb supports DuckDB v1.2.0.
! Breaking changes are:
! - Drops pre-built FreeBSD support.
! - The Arrow dependency is now opt-in via -tags=duckdb_arrow
```

## Installation

```sh
go get github.com/marcboeker/go-duckdb
```

### Windows

You must have the correct version of gcc and the necessary runtime libraries installed on Windows.
One method to do this is using msys64.
To begin, install msys64 using their installer.
Once you installed msys64, open a msys64 shell and run:

```sh
pacman -S mingw-w64-ucrt-x86_64-gcc
```

Select "yes" when necessary; it is okay if the shell closes.
Then, add gcc to the path using whatever method you prefer.
In powershell this is `$env:PATH = "C:\msys64\ucrt64\bin:$env:PATH"`.
After, you can compile this package in Windows.

## Usage

_Note: For readability, we omit error handling in most examples._

`go-duckdb` hooks into the `database/sql` interface provided by the Go `stdlib`.
To open a connection, specify the driver type as `duckdb`.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()
```

The above lines create an in-memory instance of DuckDB.
To open a persistent database, specify a file path to the database file.
If the file does not exist, then DuckDB creates it.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db")
defer db.Close()
```

If you want to set specific [config options for DuckDB](https://duckdb.org/docs/sql/configuration), 
you can add them as query style parameters in the form of `name=value` pairs to the DSN.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db?access_mode=read_only&threads=4")
defer db.Close()
```

Alternatively, you can use [sql.OpenDB](https://cs.opensource.google/go/go/+/refs/tags/go1.23.0:src/database/sql/sql.go;l=824).
That way, you can perform initialization steps in a callback function before opening the database.
Here's an example that configures some parameters when opening a database with `sql.OpenDB(connector)`.

```go
c, err := duckdb.NewConnector("/path/to/foo.db?access_mode=read_only&threads=4", func(execer driver.ExecerContext) error {
    bootQueries := []string{
        `SET schema=main`,
        `SET search_path=main`,
    }
    for _, query := range bootQueries {
        _, err = execer.ExecContext(context.Background(), query, nil)
        if err != nil {
            return err
        }
    }
    return nil
})
defer c.Close()
db := sql.OpenDB(c)
defer db.Close()
```

Please refer to the [database/sql](https://godoc.org/database/sql) documentation for further instructions on usage.

## Linking DuckDB

By default, `go-duckdb` statically links pre-built DuckDB libraries into your binary.
Statically linking DuckDB increases your binary size.

`go-duckdb` bundles the following pre-compiled static libraries.
- MacOS: amd64, arm64.
- Linux: amd64, arm64.
- Windows: amd64.

### Linking a Static Library

If none of the pre-built libraries satisfy your needs, you can build a custom static library.

1. Clone and build the DuckDB source code.
   - Use their `bundle-library` Makefile target (e.g., `make bundle-library`).
   - Common build flags are: `DUCKDB_PLATFORM=any BUILD_EXTENSIONS="icu;json;parquet;autocomplete"`.
   - See DuckDB's [development](https://github.com/duckdb/duckdb#development) instructions for more details.
2. Link against the resulting static library, which you can find in: `duckdb/build/release/libduckdb_bundle.a`.

For Darwin ARM64, you can then build your module like so:
```sh
CGO_ENABLED=1 CPPFLAGS="-DDUCKDB_STATIC_BUILD" CGO_LDFLAGS="-lduckdb_bundle -lc++ -L/path/to/libs" go build -tags=duckdb_use_static_lib
```

You can also find these steps in the `Makefile` and the `tests.yaml`.
The DuckDB team also publishes some pre-built bundled libraries as part of their [releases](https://github.com/duckdb/duckdb/releases).

#### Note on FreeBSD

Starting with `v2`, go-duckdb drops pre-built FreeBSD support.
This change is because DuckDB does not publish any bundled FreeBSD libraries.
Thus, you must build your static library for FreeBSD using the steps above.

### Linking a Dynamic Library

Alternatively, you can dynamically link DuckDB by passing `-tags=duckdb_use_lib` to `go build`.
You must have a copy of `libduckdb` available on your system (`.so` on Linux or `.dylib` on macOS), 
which you can download from the DuckDB [releases page](https://github.com/duckdb/duckdb/releases).

For example:

```sh
# On Linux.
CGO_ENABLED=1 CGO_LDFLAGS="-lduckdb -L/path/to/libs" go build -tags=duckdb_use_lib main.go
LD_LIBRARY_PATH=/path/to/libs ./main

# On MacOS.
CGO_ENABLED=1 CGO_LDFLAGS="-lduckdb -L/path/to/libs" go build -tags=duckdb_use_lib main.go
DYLD_LIBRARY_PATH=/path/to/libs ./main
```

You can also find these steps in the `Makefile` and the `tests.yaml`.

## Notes and FAQs

**`undefined: conn`**

Some people encounter an `undefined: conn` error when building this package.
This error is due to the Go compiler determining that CGO is unavailable.
This error can happen due to a few issues.

The first cause, as noted in the [comment here](https://github.com/marcboeker/go-duckdb/issues/275#issuecomment-2355712997), 
might be that the `buildtools` are not installed.
To fix this for ubuntu, you can install them using:
```
sudo apt-get update && sudo apt-get install build-essential
```

Another cause can be cross-compilation since the Go compiler automatically disables CGO when cross-compiling.
To enable CGO when cross-compiling, use `CC={C cross compiler} CGO_ENABLED=1 {command}` to force-enable CGO and set the right cross-compiler.

**`TIMESTAMP vs. TIMESTAMP_TZ`**

In the C API, DuckDB stores both `TIMESTAMP` and `TIMESTAMP_TZ` as `duckdb_timestamp`, which holds the number of
microseconds elapsed since January 1, 1970, UTC (i.e., an instant without offset information).
When passing a `time.Time` to go-duckdb, go-duckdb transforms it to an instant with `UnixMicro()`,
even when using `TIMESTAMP_TZ`. Later, scanning either type of value returns an instant, as SQL types do not model
time zone information for individual values.

## Memory Allocation

DuckDB lives in process.
Therefore, all its memory lives in the driver. 
All allocations live in the host process, which is the Go application. 
Especially for long-running applications, it is crucial to call the corresponding `Close`-functions as specified in [database/sql](https://godoc.org/database/sql). 
The following is a list of examples.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

rows, err := conn.QueryContext(context.Background(), "SELECT 42")
// Alternatively, rows.Next() has to return false.
rows.Close()

appender, err := NewAppenderFromConn(conn, "", "test")
defer appender.Close()

c, err := NewConnector("", nil)
// Optional, if passed to sql.OpenDB.
defer c.Close()
```

## DuckDB Appender API

If you want to use the [DuckDB Appender API](https://duckdb.org/docs/data/appender.html), you can obtain a new `Appender` by passing a DuckDB connection to `NewAppenderFromConn()`.
See `examples/appender.go` for a complete example.

```go
c, err := duckdb.NewConnector("test.db", nil)
defer c.Close()

conn, err := c.Connect(context.Background())
defer conn.Close()

// Obtain an appender from the connection.
// NOTE: The table 'test_tbl' must exist in test.db.
appender, err := NewAppenderFromConn(conn, "", "test_tbl")
defer appender.Close()

err = appender.AppendRow(...)
```

## DuckDB Profiling API

This section describes using the [DuckDB Profiling API](https://duckdb.org/docs/dev/profiling.html).
DuckDB's profiling information is connection-local.
The following example walks you through the necessary steps to obtain the `ProfilingInfo` type, which contains all available metrics.
Please refer to the [DuckDB documentation](https://duckdb.org/docs/dev/profiling.html) on configuring and collecting specific metrics.

- First, you need to obtain a connection.
- Then, you enable profiling for the connection.
- Now, for each subsequent query on this connection, DuckDB will collect profiling information.
    - Optionally, you can turn off profiling at any point.
- Next, you execute the query for which you want to obtain profiling information.
- Finally, directly after executing the query, retrieve any available profiling information.

```Go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

_, err = conn.ExecContext(context.Background(), `PRAGMA enable_profiling = 'no_output'`)
_, err = conn.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)

res, err := conn.QueryContext(context.Background(), `SELECT 42`)
defer res.Close()

info, err := GetProfilingInfo(conn)

_, err = conn.ExecContext(context.Background(), `PRAGMA disable_profiling`)
```

## DuckDB Apache Arrow Interface

The [DuckDB Arrow Interface](https://duckdb.org/docs/api/c/api#arrow-interface) is a heavy dependency.
Starting with `v2`, the DuckDB Arrow Interface is opt-in instead of opt-out.
If you want to use it, you can enable it by passing `-tags=duckdb_arrow` to `go build`.

```sh
go build -tags="duckdb_arrow"
```

You can obtain a new `Arrow` by passing a DuckDB connection to `NewArrowFromConn()`.

```go
c, err := duckdb.NewConnector("", nil)
defer c.Close()

conn, err := c.Connect(context.Background())
defer conn.Close()

// Obtain the Arrow from the connection.
arrow, err := duckdb.NewArrowFromConn(conn)

rdr, err := arrow.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10)")
defer rdr.Release()

for rdr.Next() {
  // Process each record.
}
```

## DuckDB Extensions

`go-duckdb` relies on the [`duckdb-go-bindings` module](https://github.com/duckdb/duckdb-go-bindings).
Any pre-built library in `duckdb-go-bindings` statically links the default extensions: ICU, JSON, Parquet, and Autocomplete.
Additionally, automatic extension loading is enabled.
