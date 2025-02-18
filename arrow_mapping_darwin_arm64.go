//go:build duckdb_arrow && !duckdb_use_lib && !duckdb_use_static_lib

package duckdb

import (
	bindings "github.com/duckdb/duckdb-go-bindings/darwin_arm64"
)

// ------------------------------------------------------------------ //
// Pointers
// ------------------------------------------------------------------ //

type (
	apiArrow       = bindings.Arrow
	apiArrowStream = bindings.ArrowStream
	apiArrowSchema = bindings.ArrowSchema
	apiArrowArray  = bindings.ArrowArray
)

// ------------------------------------------------------------------ //
// Functions
// ------------------------------------------------------------------ //

var (
	apiQueryArrowSchema     = bindings.QueryArrowSchema
	apiQueryArrowArray      = bindings.QueryArrowArray
	apiArrowRowCount        = bindings.ArrowRowCount
	apiQueryArrowError      = bindings.QueryArrowError
	apiDestroyArrow         = bindings.DestroyArrow
	apiExecutePreparedArrow = bindings.ExecutePreparedArrow
	apiArrowScan            = bindings.ArrowScan
)
