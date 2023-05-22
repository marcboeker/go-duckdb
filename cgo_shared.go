//go:build duckdb_use_lib

package duckdb

/*
#cgo CXXFLAGS: -DNDEBUG
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"
