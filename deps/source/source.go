// Package source compiles DuckDB from source
package source

/*
#cgo CXXFLAGS: -std=c++11 -fPIC -g0 -O3
#cgo windows CXXFLAGS: -DWIN32 -DDUCKDB_BUILD_LIBRARY
#cgo linux LDFLAGS: -ldl
#cgo windows LDFLAGS: -lws2_32
*/
import "C"
