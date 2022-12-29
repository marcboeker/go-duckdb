// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a database/sql driver for the DuckDB database.
package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"unsafe"
)

func init() {
	sql.Register("duckdb", Driver{})
}

type Driver struct{}

func (d Driver) Open(dataSourceName string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dataSourceName)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (Driver) OpenConnector(dataSourceName string) (driver.Connector, error) {
	return createConnector(dataSourceName, func(execerContext driver.Execer) error { return nil })
}

// NewConnector creates a new Connector for the DuckDB database.
func NewConnector(dsn string, connInitFn func(execer driver.Execer) error) (driver.Connector, error) {
	return createConnector(dsn, connInitFn)
}

func createConnector(dataSourceName string, connInitFn func(execer driver.Execer) error) (driver.Connector, error) {
	var db C.duckdb_database

	parsedDSN, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", parseConfigError, err.Error())
	}

	path := C.CString(parsedDSN.Path)
	defer C.free(unsafe.Pointer(path))

	// Check for config options.
	if len(parsedDSN.RawQuery) == 0 {
		if state := C.duckdb_open(path, &db); state == C.DuckDBError {
			return nil, openError
		}
	} else {
		config, err := prepareConfig(parsedDSN.Query())
		if err != nil {
			return nil, err
		}

		errMsg := C.CString("")
		defer C.duckdb_free(unsafe.Pointer(errMsg))

		if state := C.duckdb_open_ext(path, &db, config, &errMsg); state == C.DuckDBError {
			return nil, fmt.Errorf("%w: %s", openError, C.GoString(errMsg))
		}
	}

	return &connector{db: &db, connInitFn: connInitFn}, nil
}

type connector struct {
	db         *C.duckdb_database
	connInitFn func(execer driver.Execer) error
}

func (c *connector) Driver() driver.Driver {
	return Driver{}
}

func (c *connector) Connect(context.Context) (driver.Conn, error) {
	var con C.duckdb_connection
	if state := C.duckdb_connect(*c.db, &con); state == C.DuckDBError {
		return nil, openError
	}
	conn := &conn{con: &con}
	// call the connection init function
	err := c.connInitFn(conn)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *connector) Close() error {
	C.duckdb_close(c.db)
	c.db = nil
	return nil
}

func prepareConfig(options map[string][]string) (C.duckdb_config, error) {
	var config C.duckdb_config
	if state := C.duckdb_create_config(&config); state == C.DuckDBError {
		return nil, createConfigError
	}

	for k, v := range options {
		if len(v) > 0 {
			state := C.duckdb_set_config(config, C.CString(k), C.CString(v[0]))
			if state == C.DuckDBError {
				return nil, fmt.Errorf("%w: affected config option %s=%s", prepareConfigError, k, v[0])
			}
		}
	}

	return config, nil
}

var (
	openError          = errors.New("could not open database")
	parseConfigError   = errors.New("could not parse config for database")
	createConfigError  = errors.New("could not create config for database")
	prepareConfigError = errors.New("could not set config for database")
)
