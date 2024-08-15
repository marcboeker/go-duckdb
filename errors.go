package duckdb

import "C"
import (
	"errors"
	"fmt"
	"strings"
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
		return errors.New(invalidatedAppenderMsg)
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
	errAPI        = errors.New("API error")
	errVectorSize = errors.New("data chunks cannot exceed duckdb's internal vector size")

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

type ErrorType int

const (
	ErrorTypeInvalid              ErrorType = iota // invalid type
	ErrorTypeOutOfRange                            // value out of range error
	ErrorTypeConversion                            // conversion/casting error
	ErrorTypeUnknownType                           // unknown type error
	ErrorTypeDecimal                               // decimal related
	ErrorTypeMismatchType                          // type mismatch
	ErrorTypeDivideByZero                          // divide by 0
	ErrorTypeObjectSize                            // object size exceeded
	ErrorTypeInvalidType                           // incompatible for operation
	ErrorTypeSerialization                         // serialization
	ErrorTypeTransaction                           // transaction management
	ErrorTypeNotImplemented                        // method not implemented
	ErrorTypeExpression                            // expression parsing
	ErrorTypeCatalog                               // catalog related
	ErrorTypeParser                                // parser related
	ErrorTypePlanner                               // planner related
	ErrorTypeScheduler                             // scheduler related
	ErrorTypeExecutor                              // executor related
	ErrorTypeConstraint                            // constraint related
	ErrorTypeIndex                                 // index related
	ErrorTypeStat                                  // stat related
	ErrorTypeConnection                            // connection related
	ErrorTypeSyntax                                // syntax related
	ErrorTypeSettings                              // settings related
	ErrorTypeBinder                                // binder related
	ErrorTypeNetwork                               // network related
	ErrorTypeOptimizer                             // optimizer related
	ErrorTypeNullPointer                           // nullptr exception
	ErrorTypeIO                                    // IO exception
	ErrorTypeInterrupt                             // interrupt
	ErrorTypeFatal                                 // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	ErrorTypeInternal                              // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
	ErrorTypeInvalidInput                          // Input or arguments error
	ErrorTypeOutOfMemory                           // out of memory
	ErrorTypePermission                            // insufficient permissions
	ErrorTypeParameterNotResolved                  // parameter types could not be resolved
	ErrorTypeParameterNotAllowed                   // parameter types not allowed
	ErrorTypeDependency                            // dependency
	ErrorTypeHTTP
	ErrorTypeMissingExtension // Thrown when an extension is used but not loaded
	ErrorTypeAutoLoad         // Thrown when an extension is used but not loaded
	ErrorTypeSequence
)

var errorPrefixMap = map[string]ErrorType{
	"Invalid Error":                ErrorTypeInvalid,
	"Out of Range Error":           ErrorTypeOutOfRange,
	"Conversion Error":             ErrorTypeConversion,
	"Error":                        ErrorTypeUnknownType,
	"Decimal Error":                ErrorTypeDecimal,
	"Mismatch Type Error":          ErrorTypeMismatchType,
	"Divide by Zero Error":         ErrorTypeDivideByZero,
	"Object Size Error":            ErrorTypeObjectSize,
	"Invalid type Error":           ErrorTypeInvalidType,
	"Serialization Error":          ErrorTypeSerialization,
	"TransactionContext Error":     ErrorTypeTransaction,
	"Not implemented Error":        ErrorTypeNotImplemented,
	"Expression Error":             ErrorTypeExpression,
	"Catalog Error":                ErrorTypeCatalog,
	"Parser Error":                 ErrorTypeParser,
	"Planner Error":                ErrorTypePlanner,
	"Scheduler Error":              ErrorTypeScheduler,
	"Executor Error":               ErrorTypeExecutor,
	"Constraint Error":             ErrorTypeConstraint,
	"Index Error":                  ErrorTypeIndex,
	"Stat Error":                   ErrorTypeStat,
	"Connection Error":             ErrorTypeConnection,
	"Syntax Error":                 ErrorTypeSyntax,
	"Settings Error":               ErrorTypeSettings,
	"Binder Error":                 ErrorTypeBinder,
	"Network Error":                ErrorTypeNetwork,
	"Optimizer Error":              ErrorTypeOptimizer,
	"NullPointer Error":            ErrorTypeNullPointer,
	"IO Error":                     ErrorTypeIO,
	"INTERRUPT Error":              ErrorTypeInterrupt,
	"FATAL Error":                  ErrorTypeFatal,
	"INTERNAL Error":               ErrorTypeInternal,
	"Invalid Input Error":          ErrorTypeInvalidInput,
	"Out of Memory Error":          ErrorTypeOutOfMemory,
	"Permission Error":             ErrorTypePermission,
	"Parameter Not Resolved Error": ErrorTypeParameterNotResolved,
	"Parameter Not Allowed Error":  ErrorTypeParameterNotAllowed,
	"Dependency Error":             ErrorTypeDependency,
	"HTTP Error":                   ErrorTypeHTTP,
	"Missing Extension Error":      ErrorTypeMissingExtension,
	"Extension Autoloading Error":  ErrorTypeAutoLoad,
	"Sequence Error":               ErrorTypeSequence,
}

type Error struct {
	Type ErrorType
	Msg  string
}

func (e *Error) Error() string {
	return e.Msg
}

func (e *Error) Is(err error) bool {
	if other, ok := err.(*Error); ok {
		return other.Msg == e.Msg
	}
	return false
}

func getDuckDBError(errMsg string) error {
	errType := ErrorTypeInvalid
	// find the end of the prefix ("<error-type> Error: ")
	if idx := strings.Index(errMsg, ": "); idx != -1 {
		if typ, ok := errorPrefixMap[errMsg[:idx]]; ok {
			errType = typ
		}
	}
	return &Error{
		Type: errType,
		Msg:  errMsg,
	}
}
