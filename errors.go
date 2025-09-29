package duckdb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/marcboeker/go-duckdb/mapping"
)

func getError(errDriver, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", driverErrMsg, errDriver)
	}
	return fmt.Errorf("%s: %w: %s", driverErrMsg, errDriver, err.Error())
}

func castError(actual, expected string) error {
	return fmt.Errorf("%s: cannot cast %s to %s", castErrMsg, actual, expected)
}

func conversionError(actual, min, max int) error {
	return fmt.Errorf("%s: cannot convert %d, minimum: %d, maximum: %d", convertErrMsg, actual, min, max)
}

func invalidInputError(actual, expected string) error {
	return fmt.Errorf("%s: expected %s, got %s", invalidInputErrMsg, expected, actual)
}

func structFieldError(actual, expected string) error {
	return fmt.Errorf("%s: expected %s, got %s", structFieldErrMsg, expected, actual)
}

func columnCountError(actual, expected int) error {
	return fmt.Errorf("%s: expected %d, got %d", columnCountErrMsg, expected, actual)
}

func paramIndexError(idx int, max uint64) error {
	return fmt.Errorf("%s: %d is out of range [1, %d]", paramIndexErrMsg, idx, max)
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

func tryOtherFuncError(hint string) error {
	return fmt.Errorf("%s: %s", tryOtherFuncErrMsg, hint)
}

func addIndexToError(err error, idx int) error {
	return fmt.Errorf("%w: %s: %d", err, indexErrMsg, idx)
}

func interfaceIsNilError(interfaceName string) error {
	return fmt.Errorf("%s: %s", interfaceIsNilErrMsg, interfaceName)
}

func duplicateNameError(name string) error {
	return fmt.Errorf("%s: %s", duplicateNameErrMsg, name)
}

const (
	driverErrMsg           = "database/sql/driver"
	castErrMsg             = "cast error"
	convertErrMsg          = "conversion error"
	invalidInputErrMsg     = "invalid input"
	structFieldErrMsg      = "invalid STRUCT field"
	columnCountErrMsg      = "invalid column count"
	unsupportedTypeErrMsg  = "unsupported data type"
	invalidatedAppenderMsg = "appended and not yet flushed data has been invalidated due to error"
	tryOtherFuncErrMsg     = "please try this function instead"
	indexErrMsg            = "index"
	unknownTypeErrMsg      = "unknown type"
	interfaceIsNilErrMsg   = "interface is nil"
	duplicateNameErrMsg    = "duplicate name"
	paramIndexErrMsg       = "invalid parameter index"
)

var (
	errInternal   = errors.New("internal error: please file a bug report at go-duckdb")
	errAPI        = errors.New("API error")
	errVectorSize = errors.New("data chunks cannot exceed duckdb's internal vector size")

	errConnect      = errors.New("could not connect to database")
	errParseDSN     = errors.New("could not parse DSN for database")
	errSetConfig    = errors.New("could not set invalid or local option for global database config")
	errCreateConfig = errors.New("could not create config for database")

	errInvalidCon = errors.New("not a DuckDB driver connection")
	errClosedCon  = errors.New("closed connection")

	errClosedStmt        = errors.New("closed statement")
	errUninitializedStmt = errors.New("uninitialized statement")

	errPrepare                    = errors.New("could not prepare query")
	errMissingPrepareContext      = errors.New("missing context for multi-statement query: try using PrepareContext")
	errEmptyQuery                 = errors.New("empty query")
	errCouldNotBind               = errors.New("could not bind parameter")
	errActiveRows                 = errors.New("ExecContext or QueryContext with active Rows")
	errNotBound                   = errors.New("parameters have not been bound")
	errBeginTx                    = errors.New("could not begin transaction")
	errMultipleTx                 = errors.New("multiple transactions")
	errReadOnlyTxNotSupported     = errors.New("read-only transactions are not supported")
	errIsolationLevelNotSupported = errors.New("isolation level not supported: go-duckdb only supports the default isolation level")

	errAppenderCreation         = errors.New("could not create appender")
	errAppenderClose            = errors.New("could not close appender")
	errAppenderDoubleClose      = fmt.Errorf("%w: already closed", errAppenderClose)
	errAppenderAppendRow        = errors.New("could not append row")
	errAppenderAppendAfterClose = fmt.Errorf("%w: appender already closed", errAppenderAppendRow)
	errAppenderFlush            = errors.New("could not flush appender")

	errUnsupportedMapKeyType = errors.New("MAP key type not supported")
	errEmptyName             = errors.New("empty name")
	errInvalidDecimalWidth   = fmt.Errorf("the DECIMAL with must be between 1 and %d", max_decimal_width)
	errInvalidDecimalScale   = errors.New("the DECIMAL scale must be less than or equal to the width")
	errInvalidArraySize      = errors.New("invalid ARRAY size")
	errSetSQLNULLValue       = errors.New("cannot write to a NULL column")

	errScalarUDFCreate          = errors.New("could not create scalar UDF")
	errScalarUDFNoName          = fmt.Errorf("%w: missing name", errScalarUDFCreate)
	errScalarUDFIsNil           = fmt.Errorf("%w: function is nil", errScalarUDFCreate)
	errScalarUDFNoExecutor      = fmt.Errorf("%w: executor is nil", errScalarUDFCreate)
	errScalarUDFInputTypeIsNil  = fmt.Errorf("%w: input type is nil", errScalarUDFCreate)
	errScalarUDFResultTypeIsNil = fmt.Errorf("%w: result type is nil", errScalarUDFCreate)
	errScalarUDFResultTypeIsANY = fmt.Errorf("%w: result type is ANY, which is not supported", errScalarUDFCreate)
	errScalarUDFCreateSet       = fmt.Errorf("could not create scalar UDF set")
	errScalarUDFAddToSet        = fmt.Errorf("%w: could not add the function to the set", errScalarUDFCreateSet)

	errTableUDFCreate          = errors.New("could not create table UDF")
	errTableUDFNoName          = fmt.Errorf("%w: missing name", errTableUDFCreate)
	errTableUDFMissingBindArgs = fmt.Errorf("%w: missing bind arguments", errTableUDFCreate)
	errTableUDFArgumentIsNil   = fmt.Errorf("%w: argument is nil", errTableUDFCreate)
	errTableUDFColumnTypeIsNil = fmt.Errorf("%w: column type is nil", errTableUDFCreate)

	errProfilingInfoEmpty = errors.New("no profiling information available for this connection")
)

type ErrorType int

const (
	ErrorTypeInvalid              = ErrorType(mapping.ErrorTypeInvalid)              // Invalid type.
	ErrorTypeOutOfRange           = ErrorType(mapping.ErrorTypeOutOfRange)           // The type's value is out of range.
	ErrorTypeConversion           = ErrorType(mapping.ErrorTypeConversion)           // Conversion/casting error.
	ErrorTypeUnknownType          = ErrorType(mapping.ErrorTypeUnknownType)          // The type is unknown.
	ErrorTypeDecimal              = ErrorType(mapping.TypeDecimal)                   // Decimal-related error.
	ErrorTypeMismatchType         = ErrorType(mapping.ErrorTypeMismatchType)         // Types don't match.
	ErrorTypeDivideByZero         = ErrorType(mapping.ErrorTypeDivideByZero)         // Division by zero.
	ErrorTypeObjectSize           = ErrorType(mapping.ErrorTypeObjectSize)           // Exceeds object size.
	ErrorTypeInvalidType          = ErrorType(mapping.ErrorTypeInvalidType)          // Incompatible types.
	ErrorTypeSerialization        = ErrorType(mapping.ErrorTypeSerialization)        // Type serialization error.
	ErrorTypeTransaction          = ErrorType(mapping.ErrorTypeTransaction)          // Transaction conflict.
	ErrorTypeNotImplemented       = ErrorType(mapping.ErrorTypeNotImplemented)       // Missing functionality.
	ErrorTypeExpression           = ErrorType(mapping.ErrorTypeExpression)           // Expression error.
	ErrorTypeCatalog              = ErrorType(mapping.ErrorTypeCatalog)              // Catalog error.
	ErrorTypeParser               = ErrorType(mapping.ErrorTypeParser)               // Error during parsing.
	ErrorTypePlanner              = ErrorType(mapping.ErrorTypePlanner)              // Error during planning.
	ErrorTypeScheduler            = ErrorType(mapping.ErrorTypeScheduler)            // Scheduling error.
	ErrorTypeExecutor             = ErrorType(mapping.ErrorTypeExecutor)             // Executor error.
	ErrorTypeConstraint           = ErrorType(mapping.ErrorTypeConstraint)           // Constraint violation.
	ErrorTypeIndex                = ErrorType(mapping.ErrorTypeIndex)                // Index error.
	ErrorTypeStat                 = ErrorType(mapping.ErrorTypeStat)                 // Statistics error.
	ErrorTypeConnection           = ErrorType(mapping.ErrorTypeConnection)           // Connection error.
	ErrorTypeSyntax               = ErrorType(mapping.ErrorTypeSyntax)               // Invalid syntax.
	ErrorTypeSettings             = ErrorType(mapping.ErrorTypeSettings)             // Settings-related error.
	ErrorTypeBinder               = ErrorType(mapping.ErrorTypeBinder)               // Binding error.
	ErrorTypeNetwork              = ErrorType(mapping.ErrorTypeNetwork)              // Network error.
	ErrorTypeOptimizer            = ErrorType(mapping.ErrorTypeOptimizer)            // Optimizer error.
	ErrorTypeNullPointer          = ErrorType(mapping.ErrorTypeNullPointer)          // Null-pointer exception.
	ErrorTypeIO                   = ErrorType(mapping.ErrorTypeErrorIO)              // IO exception.
	ErrorTypeInterrupt            = ErrorType(mapping.ErrorTypeInterrupt)            // Query interruption.
	ErrorTypeFatal                = ErrorType(mapping.ErrorTypeFatal)                // Fatal exception. Non-recoverable. The DB enters an invalid state and must be restarted.
	ErrorTypeInternal             = ErrorType(mapping.ErrorTypeInternal)             // Internal exception. Indicates a bug, and should be reported.
	ErrorTypeInvalidInput         = ErrorType(mapping.ErrorTypeInvalidInput)         // Invalid input.
	ErrorTypeOutOfMemory          = ErrorType(mapping.ErrorTypeOutOfMemory)          // Out-of-memory error.
	ErrorTypePermission           = ErrorType(mapping.ErrorTypePermission)           // Invalid permissions.
	ErrorTypeParameterNotResolved = ErrorType(mapping.ErrorTypeParameterNotResolved) // Error when resolving types.
	ErrorTypeParameterNotAllowed  = ErrorType(mapping.ErrorTypeParameterNotAllowed)  // Invalid parameter.
	ErrorTypeDependency           = ErrorType(mapping.ErrorTypeDependency)           // Dependency error.
	ErrorTypeHTTP                 = ErrorType(mapping.ErrorTypeHTTP)                 // HTTP error.
	ErrorTypeMissingExtension     = ErrorType(mapping.ErrorTypeMissingExtension)     // Usage of a non-loaded extension.
	ErrorTypeAutoLoad             = ErrorType(mapping.ErrorTypeAutoload)             // Usage of a non-loaded extension that cannot be loaded automatically.
	ErrorTypeSequence             = ErrorType(mapping.ErrorTypeSequence)             // Sequence error.
	ErrorTypeInvalidConfiguration = ErrorType(mapping.ErrorTypeInvalidConfiguration) // Indicates an invalid configuration, e.g., a missing Secret parameter, or a mandatory setting is not provided.
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
	"Invalid Configuration Error":  ErrorTypeInvalidConfiguration,
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

func errorDataError(errorData mapping.ErrorData) error {
	defer mapping.DestroyErrorData(&errorData)
	if !mapping.ErrorDataHasError(errorData) {
		return nil
	}

	t := mapping.ErrorDataErrorType(errorData)
	msg := mapping.ErrorDataMessage(errorData)

	return &Error{ErrorType(t), msg}
}

func getDuckDBError(errMsg string) error {
	errType := ErrorTypeInvalid

	// Find the end of the prefix ("<error-type> Error: ").
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
