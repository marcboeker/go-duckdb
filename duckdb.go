// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a database/sql driver for the DuckDB database.
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
)

func init() {
	sql.Register("duckdb", Driver{})
}

type Driver struct{}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn, func(execerContext driver.ExecerContext) error {
		return nil
	})
}

// NewConnector opens a new Connector for a DuckDB database.
// The user must close the Connector, if it is not passed to the sql.OpenDB function.
// Otherwise, sql.DB closes the Connector when calling sql.DB.Close().
func NewConnector(dsn string, connInitFn func(execer driver.ExecerContext) error) (*Connector, error) {
	var db apiDatabase

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, getError(errParseDSN, err)
	}

	config, err := prepareConfig(parsedDSN)
	if err != nil {
		return nil, err
	}
	defer apiDestroyConfig(&config)

	connStr := getConnString(dsn)
	var errMsg string

	state := apiOpenExt(connStr, &db, config, &errMsg)
	if apiState(state) == apiError {
		return nil, getError(errConnect, getDuckDBError(errMsg))
	}

	return &Connector{
		db:         db,
		connInitFn: connInitFn,
	}, nil
}

type Connector struct {
	db         apiDatabase
	connInitFn func(execer driver.ExecerContext) error
}

func (*Connector) Driver() driver.Driver {
	return Driver{}
}

func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	var apiConn apiConnection

	state := apiConnect(c.db, &apiConn)
	if apiState(state) == apiError {
		return nil, getError(errConnect, nil)
	}

	con := &Conn{apiConn: apiConn}

	if c.connInitFn != nil {
		if err := c.connInitFn(con); err != nil {
			return nil, err
		}
	}

	return con, nil
}

func (c *Connector) Close() error {
	apiClose(&c.db)
	return nil
}

func getConnString(dsn string) string {
	idx := strings.Index(dsn, "?")
	if idx < 0 {
		idx = len(dsn)
	}
	return dsn[0:idx]
}

func prepareConfig(parsedDSN *url.URL) (apiConfig, error) {
	var config apiConfig

	state := apiCreateConfig(&config)
	if apiState(state) == apiError {
		apiDestroyConfig(&config)
		return config, getError(errCreateConfig, nil)
	}

	if err := setConfigOption(config, "duckdb_api", "go"); err != nil {
		return config, err
	}

	// Early-out, if the DSN does not contain configuration options.
	if len(parsedDSN.RawQuery) == 0 {
		return config, nil
	}

	for k, v := range parsedDSN.Query() {
		if len(v) == 0 {
			continue
		}
		if err := setConfigOption(config, k, v[0]); err != nil {
			return config, err
		}
	}

	return config, nil
}

func setConfigOption(config apiConfig, name string, option string) error {
	state := apiSetConfig(config, name, option)
	if apiState(state) == apiError {
		apiDestroyConfig(&config)
		return getError(errSetConfig, fmt.Errorf("%s=%s", name, option))
	}
	return nil
}
