//go:build duckdb_arrow && (duckdb_use_lib || duckdb_use_static_lib)

package arrowmapping

import (
	bindings "github.com/duckdb/duckdb-go-bindings"
)

// Pointers

type (
	Arrow       = bindings.Arrow
	ArrowStream = bindings.ArrowStream
	ArrowSchema = bindings.ArrowSchema
	ArrowArray  = bindings.ArrowArray
)

// Functions

var (
	QueryArrowSchema     = bindings.QueryArrowSchema
	QueryArrowArray      = bindings.QueryArrowArray
	ArrowRowCount        = bindings.ArrowRowCount
	QueryArrowError      = bindings.QueryArrowError
	DestroyArrow         = bindings.DestroyArrow
	ExecutePreparedArrow = bindings.ExecutePreparedArrow
	ArrowScan            = bindings.ArrowScan
)
