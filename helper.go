package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import "unsafe"

// secondsPerDay to calculate the days since 1970-01-01.
const secondsPerDay = 24 * 60 * 60

func mallocTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummy C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummy))

	ptr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	slice := (*[1 << 30]C.duckdb_logical_type)(ptr)[:count:count]

	return ptr, slice
}

func destroyTypeSlice(ptr unsafe.Pointer, slice []C.duckdb_logical_type) {
	for _, t := range slice {
		C.duckdb_destroy_logical_type(&t)
	}
	C.free(ptr)
}
