//go:build darwin

package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/deps/source/include
#cgo LDFLAGS: -lduckdb
#cgo darwin,amd64 LDFLAGS: -lc++ -L${SRCDIR}/deps/darwin_amd64
#cgo darwin,arm64 LDFLAGS: -lc++ -L${SRCDIR}/deps/darwin_arm64
#include <duckdb.h>
*/
import "C"
