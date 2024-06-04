package duckdb

import "C"
import (
	"errors"
	"fmt"
)

func getError(errDriver error, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", driverErrMsg, errDriver)
	}
	return fmt.Errorf("%s: %w: %s", driverErrMsg, errDriver, err.Error())
}

func duckdbError(err *C.char) error {
	return fmt.Errorf("%s: %w", duckdbErrMsg, errors.New(C.GoString(err)))
}

func castError(actual string, expected string) error {
	return fmt.Errorf("%s: cannot cast %s to %s", castErrMsg, actual, expected)
}

func structFieldError(actual string, expected string) error {
	return fmt.Errorf("%s: expected %s, got %s", structFieldErrMsg, expected, actual)
}

func columnError(err error, colIdx int) error {
	return fmt.Errorf("%w: %s: %d", err, columnErrMsg, colIdx)
}

func columnCountError(actual int, expected int) error {
	return fmt.Errorf("%s: expected %d, got %d", columnCountErrMsg, expected, actual)
}

func unsupportedTypeError(name string) error {
	return fmt.Errorf("%s: %s", unsupportedTypeErrMsg, name)
}

func invalidatedAppenderError(err error) error {
	if err == nil {
		return fmt.Errorf(invalidatedAppenderMsg)
	}
	return fmt.Errorf("%w: %s", err, invalidatedAppenderMsg)
}

const (
	driverErrMsg           = "database/sql/driver"
	duckdbErrMsg           = "duckdb error"
	castErrMsg             = "cast error"
	structFieldErrMsg      = "invalid STRUCT field"
	columnErrMsg           = "column index"
	columnCountErrMsg      = "invalid column count"
	unsupportedTypeErrMsg  = "unsupported data type"
	invalidatedAppenderMsg = "appended data has been invalidated due to corrupt row"
)

var (
	errDriver = errors.New("internal driver error, please file a bug report")

	errParseDSN  = errors.New("could not parse DSN for database")
	errOpen      = errors.New("could not open database")
	errSetConfig = errors.New("could not set invalid or local option for global database config")

	errUnsupportedMapKeyType = errors.New("MAP key type not supported")

	errAppenderInvalidCon       = errors.New("could not create appender: not a DuckDB driver connection")
	errAppenderClosedCon        = errors.New("could not create appender: appender creation on a closed connection")
	errAppenderCreation         = errors.New("could not create appender")
	errAppenderDoubleClose      = errors.New("could not close appender: already closed")
	errAppenderAppendRow        = errors.New("could not append row")
	errAppenderAppendAfterClose = errors.New("could not append row: appender already closed")
	errAppenderClose            = errors.New("could not close appender")
	errAppenderFlush            = errors.New("could not flush appender")

	// Errors not covered in tests.
	errConnect      = errors.New("could not connect to database")
	errCreateConfig = errors.New("could not create config for database")
)
