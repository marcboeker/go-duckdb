//go:build duckdb_use_lib

package duckdb

/*
#cgo CXXFLAGS: -I${SRCDIR}/deps/source/include
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"
