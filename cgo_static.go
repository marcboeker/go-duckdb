//go:build !duckdb_use_lib && (darwin || (linux && (amd64 || arm64)) || (windows && amd64))

package duckdb

/*
#cgo darwin,amd64 LDFLAGS: -lduckdb -lc++ -L${SRCDIR}/deps/darwin_amd64
#cgo darwin,arm64 LDFLAGS: -lduckdb -lc++ -L${SRCDIR}/deps/darwin_arm64
#cgo linux,amd64 LDFLAGS: -lduckdb -lstdc++ -lm -ldl -L${SRCDIR}/deps/linux_amd64
#cgo linux,arm64 LDFLAGS: -lduckdb -lstdc++ -lm -ldl -L${SRCDIR}/deps/linux_arm64
#cgo windows,amd64 LDFLAGS: -lduckdb -lws2_32 -lstdc++ -Wl,-Bstatic -lpthread -lm -L${SRCDIR}/deps/windows_amd64
#include <duckdb.h>
*/
import "C"
