// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a database/sql driver for the DuckDB database.
package duckdb

/*
#include <stdlib.h>
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
	"strings"
	"unsafe"
)

func init() {
	sql.Register("duckdb", Driver{})
}

type Driver struct{}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn, func(execerContext driver.ExecerContext) error {
		return nil
	})
}

// NewConnector opens a new Connector for a DuckDB database.
// The user must close the returned Connector, if it is not passed to the sql.OpenDB function.
// Otherwise, sql.DB closes the Connector when calling sql.DB.Close().
func NewConnector(dsn string, connInitFn func(execer driver.ExecerContext) error) (*Connector, error) {
	var db C.duckdb_database

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errParseDSN, err.Error())
	}

	config, err := prepareConfig(parsedDSN)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_config(&config)

	connStr := C.CString(extractConnectionString(dsn))
	defer C.free(unsafe.Pointer(connStr))

	var errOpenMsg *C.char
	defer C.duckdb_free(unsafe.Pointer(errOpenMsg))

	if state := C.duckdb_open_ext(connStr, &db, config, &errOpenMsg); state == C.DuckDBError {
		return nil, fmt.Errorf("%w: %s", errOpen, C.GoString(errOpenMsg))
	}

	return &Connector{
		db:         db,
		connInitFn: connInitFn,
	}, nil
}

type Connector struct {
	db         C.duckdb_database
	connInitFn func(execer driver.ExecerContext) error
}

func (c *Connector) Driver() driver.Driver {
	return Driver{}
}

func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	var con C.duckdb_connection
	if state := C.duckdb_connect(c.db, &con); state == C.DuckDBError {
		return nil, errOpen
	}

	conn := &conn{con: &con}

	// Call the connection init function if defined
	if c.connInitFn != nil {
		if err := c.connInitFn(conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (c *Connector) Close() error {
	C.duckdb_close(&c.db)
	c.db = nil
	return nil
}

func extractConnectionString(dsn string) string {
	var queryIndex = strings.Index(dsn, "?")
	if queryIndex < 0 {
		queryIndex = len(dsn)
	}

	return dsn[0:queryIndex]
}

func prepareConfig(parsedDSN *url.URL) (C.duckdb_config, error) {
	var config C.duckdb_config
	if state := C.duckdb_create_config(&config); state == C.DuckDBError {
		C.duckdb_destroy_config(&config)
		return nil, errCreateConfig
	}

	if err := setConfig(config, "duckdb_api", "go"); err != nil {
		return nil, err
	}

	// early-out
	if len(parsedDSN.RawQuery) == 0 {
		return config, nil
	}

	for k, v := range parsedDSN.Query() {
		if len(v) == 0 {
			continue
		}
		if err := setConfig(config, k, v[0]); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func setConfig(config C.duckdb_config, name string, option string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cOption := C.CString(option)
	defer C.free(unsafe.Pointer(cOption))

	state := C.duckdb_set_config(config, cName, cOption)
	if state == C.DuckDBError {
		C.duckdb_destroy_config(&config)
		return fmt.Errorf("%w: affected config option %s=%s", errSetConfig, name, option)
	}

	return nil
}

var (
	errOpen         = errors.New("could not open database")
	errParseDSN     = errors.New("could not parse DSN for database")
	errCreateConfig = errors.New("could not create config for database")
	errSetConfig    = errors.New("could not set config for database")
)
