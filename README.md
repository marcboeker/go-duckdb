# Go SQL driver for [DuckDB](https://github.com/duckdb/duckdb)

The DuckDB driver conforms to the built-in `database/sql` interface.

## Installation

```
go get github.com/marcboeker/go-duckdb
```

`go-duckdb` uses `CGO` to make calls to the linked DuckDB database. Therefor you need to have a compiled version of DuckDB availabe.


**Please use the latest DuckDB version from master.**

```
git clone https://github.com/duckdb/duckdb.git
cd duckdb
make
```

Please locate the following files in your DuckDB directory, as we need them to build the `go-duckdb` driver:

- `build/release/src/libduckdb{_static}.a` or `build/release/src/libduckdb.so` (for Linux)
- `build/release/src/libduckdb.dylib` (for macOS)
- `build/release/src/libduckdb.dll` (for Windows)
- `src/include/duckdb.h`

If you don't want to compile DuckDB yourself, you could also use the pre-compiled libraries from their [releases page](https://github.com/duckdb/duckdb/releases).

To run the example or execute the tests please specify the following `CGO_LDFLAGS`, `CGO_CFLAGS` and the `DYLD_LIBRARY_PATH` (if you are on macOS) env variables.

```
CGO_LDFLAGS="-L/path/to/libduckdb_static.a" CGO_CFLAGS="-I/path/to/duckdb.h" DYLD_LIBRARY_PATH="/path/to/libduckdb.dylib" go run examples/simple.go
```

For example on macOS:

```
CGO_LDFLAGS="-L/path/to/duckdb/build/release/src" CGO_CFLAGS="-I/path/to/duckdb/src/include" DYLD_LIBRARY_PATH="/path/to/duckdb/build/release/src" go run examples/simple.go
```

## Usage

`go-duckdb` hooks into the `database/sql` interface provided by the Go stdlib. To open a connection, simply specify the driver type as `duckdb`:

```
db, err := sql.Open("duckdb", "")
```

This creates an in-memory instance of DuckDB. If you would like to store the data on the filesystem, you need to specify the path where to store the database:

```
db, err := sql.Open("duckdb", "/path/to/foo.db")
```

Please refer to the [database/sql](https://godoc.org/database/sql) GoDoc for further usage instructions.
