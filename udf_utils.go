package duckdb

import (
	"runtime"
	"runtime/cgo"
	"unsafe"
)

// Helpers for passing values to C and back.

type pinnedValue[T any] struct {
	pinner *runtime.Pinner
	value  T
}

type unpinner interface {
	unpin()
}

func (v pinnedValue[T]) unpin() {
	v.pinner.Unpin()
}

func getPinned[T any](handle unsafe.Pointer) T {
	h := *(*cgo.Handle)(handle)
	return h.Value().(pinnedValue[T]).value
}
