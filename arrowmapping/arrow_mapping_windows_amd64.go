//go:build duckdb_arrow && !duckdb_use_lib && !duckdb_use_static_lib

package arrowmapping

import (
	bindings "github.com/duckdb/duckdb-go-bindings/windows-amd64"
)

// Pointers.

type (
	Arrow                = bindings.Arrow
	ArrowStream          = bindings.ArrowStream
	ArrowSchema          = bindings.ArrowSchema
	ArrowConvertedSchema = bindings.ArrowConvertedSchema
	ArrowArray           = bindings.ArrowArray
	ArrowOptions         = bindings.ArrowOptions
)

// Functions.

var (
	ConnectionGetArrowOptions   = bindings.ConnectionGetArrowOptions
	DestroyArrowOptions         = bindings.DestroyArrowOptions
	ResultGetArrowOptions       = bindings.ResultGetArrowOptions
	DestroyArrowConvertedSchema = bindings.DestroyArrowConvertedSchema
	QueryArrowSchema            = bindings.QueryArrowSchema
	QueryArrowArray             = bindings.QueryArrowArray
	ArrowRowCount               = bindings.ArrowRowCount
	QueryArrowError             = bindings.QueryArrowError
	DestroyArrow                = bindings.DestroyArrow
	ExecutePreparedArrow        = bindings.ExecutePreparedArrow
	ArrowScan                   = bindings.ArrowScan
)
