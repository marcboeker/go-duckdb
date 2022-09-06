//go:build duckdb_use_lib

package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/deps/source/include
#cgo LDFLAGS: -lduckdb
*/
import "C"
