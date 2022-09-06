//go:build duckdb_use_source || !darwin

package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/deps/source/include
#cgo CXXFLAGS: -std=c++11 -fPIC -g0 -O3
#cgo windows CXXFLAGS: -DWIN32 -DDUCKDB_BUILD_LIBRARY
#cgo linux LDFLAGS: -ldl
#cgo windows LDFLAGS: -lws2_32
#include <duckdb.h>
*/
import "C"
