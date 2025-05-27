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
	"sync"

	"github.com/marcboeker/go-duckdb/mapping"
)

var GetInstanceCache = sync.OnceValue[mapping.InstanceCache](
	func() mapping.InstanceCache {
		return mapping.CreateInstanceCache()
	})

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
	inMemory := false
	const inMemoryName = ":memory:"

	// If necessary, trim the in-memory prefix, and determine if this is an in-memory database.
	if dsn == inMemoryName || strings.HasPrefix(dsn, inMemoryName+"?") {
		dsn = dsn[len(inMemoryName):]
		inMemory = true
	} else if dsn == "" || strings.HasPrefix(dsn, "?") {
		inMemory = true
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, getError(errParseDSN, err)
	}

	config, err := prepareConfig(parsedDSN)
	if err != nil {
		return nil, err
	}
	defer mapping.DestroyConfig(&config)

	var db mapping.Database
	var errMsg string
	var state mapping.State

	if inMemory {
		// Open an in-memory database.
		state = mapping.OpenExt("", &db, config, &errMsg)
	} else {
		// Open a file-backed database.
		state = mapping.GetOrCreateFromCache(GetInstanceCache(), getDBPath(dsn), &db, config, &errMsg)
	}
	if state == mapping.StateError {
		mapping.Close(&db)
		return nil, getError(errConnect, getDuckDBError(errMsg))
	}

	return &Connector{
		db:         db,
		connInitFn: connInitFn,
		ctxStore:   newContextStore(),
	}, nil
}

type Connector struct {
	// The internal DuckDB database.
	db mapping.Database
	// Callback to perform additional initialization steps.
	connInitFn func(execer driver.ExecerContext) error
	// ctxStore stores the context of the latest query/exec/etc. function
	// invoked per connection.
	ctxStore contextStore
	// True, if the connector has been closed, else false.
	closed bool
}

func (*Connector) Driver() driver.Driver {
	return Driver{}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	var mc mapping.Connection
	if mapping.Connect(c.db, &mc) == mapping.StateError {
		return nil, getError(errConnect, nil)
	}

	conn := newConn(mc, c.ctxStore)
	c.ctxStore.set(conn.id, ctx)
	if c.connInitFn != nil {
		if err := c.connInitFn(conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (c *Connector) Close() error {
	if c.closed {
		return nil
	}
	mapping.Close(&c.db)
	c.closed = true

	return nil
}

func getDBPath(dsn string) string {
	idx := strings.Index(dsn, "?")
	if idx < 0 {
		idx = len(dsn)
	}
	return dsn[0:idx]
}

func prepareConfig(parsedDSN *url.URL) (mapping.Config, error) {
	var config mapping.Config
	if mapping.CreateConfig(&config) == mapping.StateError {
		mapping.DestroyConfig(&config)
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

func setConfigOption(config mapping.Config, name string, option string) error {
	if mapping.SetConfig(config, name, option) == mapping.StateError {
		mapping.DestroyConfig(&config)
		return getError(errSetConfig, fmt.Errorf("%s=%s", name, option))
	}

	return nil
}
