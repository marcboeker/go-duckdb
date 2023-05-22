//go:build duckdb_use_lib

package duckdb

/*
#cgo CFLAGS: -DNDEBUG
#cgo LDFLAGS: -lduckdb
#include <duckdb.h>
*/
import "C"
