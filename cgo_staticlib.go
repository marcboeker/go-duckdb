//go:build duckdb_use_staticlib

package duckdb

/*
#cgo LDFLAGS: -lduckdb_static
#include <duckdb.h>
*/
import "C"
