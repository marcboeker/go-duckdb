package duckdb

import "runtime"

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
