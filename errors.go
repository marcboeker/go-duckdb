package duckdb

import "C"
import (
	"errors"
	"fmt"
)

func getError(errDriver error, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %s", driverErrMsg, errDriver.Error())
	}
	return fmt.Errorf("%s: %s: %s", driverErrMsg, errDriver.Error(), err.Error())
}

func getDuckDBError(err *C.char) error {
	return errors.New(C.GoString(err))
}

var driverErrMsg = "database/sql/driver"

var (
	errParseDSN  = errors.New("could not parse DSN for database")
	errOpen      = errors.New("could not open database")
	errSetConfig = errors.New("could not set invalid or local option for global database config")

	// Errors not covered in tests.
	errConnect      = errors.New("could not connect to database")
	errCreateConfig = errors.New("could not create config for database")
)
