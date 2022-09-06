//go:build duckdb_use_source || !darwin

package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/deps/source/include
*/
import "C"

// This import contains the source code and C++ compilation flags
import _ "github.com/marcboeker/go-duckdb/deps/source"
