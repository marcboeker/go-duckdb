package duckdb

/*
#include <duckdb.h>
*/
import "C"
import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
)

// ArrowQuery holds the duckdb Arrow Query interface. It allows querying and receiving Arrow records.
type ArrowQuery struct {
	c *conn
}

// NewArrowQueryFromConn returns a new ArrowQuery from a DuckDB driver connection.
func NewArrowQueryFromConn(driverConn driver.Conn) (*ArrowQuery, error) {
	dbConn, ok := driverConn.(*conn)
	if !ok {
		return nil, fmt.Errorf("not a duckdb driver connection")
	}

	if dbConn.closed {
		panic("database/sql/driver: misuse of duckdb driver: ArrowQuery after Close")
	}

	return &ArrowQuery{c: dbConn}, nil
}

func (aq *ArrowQuery) QueryContext(ctx context.Context, query string) (arrow.Record, error) {
	return aq.c.QueryArrowContext(ctx, query)
}
