//go:build duckdb_use_static_lib

package duckdb

/*
#cgo LDFLAGS: -lduckdb_static
#include <duckdb.h>
*/
import "C"
