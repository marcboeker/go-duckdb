package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/marcboeker/go-duckdb/mapping"
	"math/big"
)

type StmtType mapping.StatementType

const (
	STATEMENT_TYPE_INVALID      = StmtType(mapping.StatementTypeInvalid)
	STATEMENT_TYPE_SELECT       = StmtType(mapping.StatementTypeSelect)
	STATEMENT_TYPE_INSERT       = StmtType(mapping.StatementTypeInsert)
	STATEMENT_TYPE_UPDATE       = StmtType(mapping.StatementTypeUpdate)
	STATEMENT_TYPE_EXPLAIN      = StmtType(mapping.StatementTypeExplain)
	STATEMENT_TYPE_DELETE       = StmtType(mapping.StatementTypeDelete)
	STATEMENT_TYPE_PREPARE      = StmtType(mapping.StatementTypePrepare)
	STATEMENT_TYPE_CREATE       = StmtType(mapping.StatementTypeCreate)
	STATEMENT_TYPE_EXECUTE      = StmtType(mapping.StatementTypeExecute)
	STATEMENT_TYPE_ALTER        = StmtType(mapping.StatementTypeAlter)
	STATEMENT_TYPE_TRANSACTION  = StmtType(mapping.StatementTypeTransaction)
	STATEMENT_TYPE_COPY         = StmtType(mapping.StatementTypeCopy)
	STATEMENT_TYPE_ANALYZE      = StmtType(mapping.StatementTypeAnalyze)
	STATEMENT_TYPE_VARIABLE_SET = StmtType(mapping.StatementTypeVariableSet)
	STATEMENT_TYPE_CREATE_FUNC  = StmtType(mapping.StatementTypeCreateFunc)
	STATEMENT_TYPE_DROP         = StmtType(mapping.StatementTypeDrop)
	STATEMENT_TYPE_EXPORT       = StmtType(mapping.StatementTypeExport)
	STATEMENT_TYPE_PRAGMA       = StmtType(mapping.StatementTypePragma)
	STATEMENT_TYPE_VACUUM       = StmtType(mapping.StatementTypeVacuum)
	STATEMENT_TYPE_CALL         = StmtType(mapping.StatementTypeCall)
	STATEMENT_TYPE_SET          = StmtType(mapping.StatementTypeSet)
	STATEMENT_TYPE_LOAD         = StmtType(mapping.StatementTypeLoad)
	STATEMENT_TYPE_RELATION     = StmtType(mapping.StatementTypeRelation)
	STATEMENT_TYPE_EXTENSION    = StmtType(mapping.StatementTypeExtension)
	STATEMENT_TYPE_LOGICAL_PLAN = StmtType(mapping.StatementTypeLogicalPlan)
	STATEMENT_TYPE_ATTACH       = StmtType(mapping.StatementTypeAttach)
	STATEMENT_TYPE_DETACH       = StmtType(mapping.StatementTypeDetach)
	STATEMENT_TYPE_MULTI        = StmtType(mapping.StatementTypeMulti)
)

// Stmt implements the driver.Stmt interface.
type Stmt struct {
	conn             *Conn
	preparedStmt     *mapping.PreparedStatement
	closeOnRowsClose bool
	bound            bool
	closed           bool
	rows             bool
}

// Close the statement.
// Implements the driver.Stmt interface.
func (s *Stmt) Close() error {
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	mapping.DestroyPrepare(s.preparedStmt)
	return nil
}

// NumInput returns the number of placeholder parameters.
// Implements the driver.Stmt interface.
func (s *Stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	count := mapping.NParams(*s.preparedStmt)
	return int(count)
}

// ParamName returns the name of the parameter at the given index (1-based).
func (s *Stmt) ParamName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	if s.preparedStmt == nil {
		return "", errUninitializedStmt
	}

	count := mapping.NParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return "", getError(errAPI, paramIndexError(n, uint64(count)))
	}

	name := mapping.ParameterName(*s.preparedStmt, mapping.IdxT(n))
	return name, nil
}

// ParamType returns the expected type of the parameter at the given index (1-based).
func (s *Stmt) ParamType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	if s.preparedStmt == nil {
		return TYPE_INVALID, errUninitializedStmt
	}

	count := mapping.NParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return TYPE_INVALID, getError(errAPI, paramIndexError(n, uint64(count)))
	}

	t := mapping.ParamType(*s.preparedStmt, mapping.IdxT(n))
	return Type(t), nil
}

func (s *Stmt) paramLogicalType(n int) (mapping.LogicalType, error) {
	var lt mapping.LogicalType
	if s.closed {
		return lt, errClosedStmt
	}
	if s.preparedStmt == nil {
		return lt, errUninitializedStmt
	}

	count := mapping.NParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return lt, getError(errAPI, paramIndexError(n, uint64(count)))
	}

	return mapping.ParamLogicalType(*s.preparedStmt, mapping.IdxT(n)), nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	if s.preparedStmt == nil {
		return STATEMENT_TYPE_INVALID, errUninitializedStmt
	}

	t := mapping.PreparedStatementType(*s.preparedStmt)
	return StmtType(t), nil
}

// Bind the parameters to the statement.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) Bind(args []driver.NamedValue) error {
	if s.closed {
		return errors.Join(errCouldNotBind, errClosedStmt)
	}
	if s.preparedStmt == nil {
		return errors.Join(errCouldNotBind, errUninitializedStmt)
	}
	return s.bind(args)
}

func (s *Stmt) bindHugeint(val *big.Int, n int) (mapping.State, error) {
	hugeint, err := hugeIntFromNative(val)
	if err != nil {
		return mapping.StateError, err
	}
	state := mapping.BindHugeInt(*s.preparedStmt, mapping.IdxT(n+1), *hugeint)
	return state, nil
}

func (s *Stmt) bindTimestamp(val driver.NamedValue, t Type, n int) (mapping.State, error) {
	var state mapping.State
	switch t {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		v, err := getMappedTimestamp(val.Value)
		if err != nil {
			return mapping.StateError, err
		}
		state = mapping.BindTimestamp(*s.preparedStmt, mapping.IdxT(n+1), *v)
	case TYPE_TIMESTAMP_S:
		v, err := getMappedTimestampS(val.Value)
		if err != nil {
			return mapping.StateError, err
		}
		tS := mapping.CreateTimestampS(*v)
		state = mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), tS)
		mapping.DestroyValue(&tS)
	case TYPE_TIMESTAMP_MS:
		v, err := getMappedTimestampMS(val.Value)
		if err != nil {
			return mapping.StateError, err
		}
		tMS := mapping.CreateTimestampMS(*v)
		state = mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), tMS)
		mapping.DestroyValue(&tMS)
	case TYPE_TIMESTAMP_NS:
		v, err := getMappedTimestampNS(val.Value)
		if err != nil {
			return mapping.StateError, err
		}
		tMS := mapping.CreateTimestampNS(*v)
		state = mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), tMS)
		mapping.DestroyValue(&tMS)
	}
	return state, nil
}

func (s *Stmt) bindDate(val driver.NamedValue, n int) (mapping.State, error) {
	date, err := getMappedDate(val.Value)
	if err != nil {
		return mapping.StateError, err
	}
	state := mapping.BindDate(*s.preparedStmt, mapping.IdxT(n+1), *date)
	return state, nil
}

func (s *Stmt) bindTime(val driver.NamedValue, t Type, n int) (mapping.State, error) {
	ticks, err := getTimeTicks(val.Value)
	if err != nil {
		return mapping.StateError, err
	}

	if t == TYPE_TIME {
		ti := mapping.NewTime(ticks)
		state := mapping.BindTime(*s.preparedStmt, mapping.IdxT(n+1), *ti)
		return state, nil
	}

	// TYPE_TIME_TZ: The UTC offset is 0.
	ti := mapping.CreateTimeTZ(ticks, 0)
	v := mapping.CreateTimeTZValue(ti)
	state := mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), v)
	mapping.DestroyValue(&v)
	return state, nil
}

func (s *Stmt) bindJSON(val driver.NamedValue, n int) (mapping.State, error) {
	switch v := val.Value.(type) {
	case []byte:
		return mapping.BindVarchar(*s.preparedStmt, mapping.IdxT(n+1), string(v)), nil
	case string:
		return mapping.BindVarchar(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	}
	return mapping.StateError, addIndexToError(unsupportedTypeError("JSON interface, need []byte or string"), n+1)
}

// Used for binding Array, List, Struct. In the future, also Map and Union
func (s *Stmt) bindCompositeValue(val driver.NamedValue, n int) (mapping.State, error) {
	lt, err := s.paramLogicalType(n + 1)
	defer mapping.DestroyLogicalType(&lt)
	if err != nil {
		return mapping.StateError, err
	}

	mappedVal, err := createValue(lt, val.Value)
	defer mapping.DestroyValue(mappedVal)
	if err != nil {
		return mapping.StateError, addIndexToError(err, n+1)
	}

	state := mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), *mappedVal)
	return state, nil
}

func (s *Stmt) bindValueByReflection(val driver.NamedValue, n int) (mapping.State, error) {
	_, mappedVal, err := createValueByReflection(val.Value)
	defer mapping.DestroyValue(mappedVal)
	if err != nil {
		return mapping.StateError, addIndexToError(err, n+1)
	}
	state := mapping.BindValue(*s.preparedStmt, mapping.IdxT(n+1), *mappedVal)
	return state, nil
}

func (s *Stmt) bindComplexValue(val driver.NamedValue, n int, t Type, name string) (mapping.State, error) {
	// We could not resolve this parameter when binding the query.
	// Fall back to the Go type.
	if t == TYPE_INVALID {
		return s.bindValueByReflection(val, n)
	}

	switch t {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS:
		return s.bindTimestamp(val, t, n)
	case TYPE_DATE:
		return s.bindDate(val, n)
	case TYPE_TIME, TYPE_TIME_TZ:
		return s.bindTime(val, t, n)
	case TYPE_ARRAY, TYPE_LIST, TYPE_STRUCT:
		return s.bindCompositeValue(val, n)
	case TYPE_MAP, TYPE_ENUM, TYPE_UNION:
		// FIXME: for other types: duckdb_param_logical_type once available, then create duckdb_value + duckdb_bind_value
		// FIXME: for other types: use NamedValueChecker to support.
		return mapping.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	}
	return mapping.StateError, addIndexToError(unsupportedTypeError(unknownTypeErrMsg), n+1)
}

func (s *Stmt) bindValue(val driver.NamedValue, n int) (mapping.State, error) {
	// For some queries, we cannot resolve the parameter type when preparing the query.
	// E.g., for "SELECT * FROM (VALUES (?, ?)) t(a, b)", we cannot know the parameter types from the SQL statement alone.
	// For these cases, ParamType returns TYPE_INVALID.
	t, err := s.ParamType(n + 1)
	if err != nil {
		return mapping.StateError, err
	}

	name, ok := unsupportedTypeToStringMap[t]
	if ok && t != TYPE_INVALID {
		return mapping.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	}

	if t != TYPE_INVALID {
		lt, e := s.paramLogicalType(n + 1)
		if e != nil {
			return mapping.StateError, e
		}
		alias := mapping.LogicalTypeGetAlias(lt)
		switch alias {
		case aliasJSON:
			return s.bindJSON(val, n)
		}
	}

	switch v := val.Value.(type) {
	case bool:
		return mapping.BindBoolean(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case int8:
		return mapping.BindInt8(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case int16:
		return mapping.BindInt16(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case int32:
		return mapping.BindInt32(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case int64:
		return mapping.BindInt64(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case int:
		// int is at least 32 bits.
		return mapping.BindInt64(*s.preparedStmt, mapping.IdxT(n+1), int64(v)), nil
	case *big.Int:
		return s.bindHugeint(v, n)
	case Decimal:
		// FIXME: use NamedValueChecker to support this type.
		name := typeToStringMap[TYPE_DECIMAL]
		return mapping.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	case uint8:
		return mapping.BindUInt8(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case uint16:
		return mapping.BindUInt16(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case uint32:
		return mapping.BindUInt32(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case uint64:
		return mapping.BindUInt64(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case float32:
		return mapping.BindFloat(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case float64:
		return mapping.BindDouble(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case string:
		return mapping.BindVarchar(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case []byte:
		return mapping.BindBlob(*s.preparedStmt, mapping.IdxT(n+1), v), nil
	case Interval:
		return mapping.BindInterval(*s.preparedStmt, mapping.IdxT(n+1), *v.getMappedInterval()), nil
	case nil:
		return mapping.BindNull(*s.preparedStmt, mapping.IdxT(n+1)), nil
	}
	return s.bindComplexValue(val, n, t, name)
}

func (s *Stmt) bind(args []driver.NamedValue) error {
	if s.NumInput() > len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	// relaxed length check allow for unused parameters.
	for i := 0; i < s.NumInput(); i++ {
		name := mapping.ParameterName(*s.preparedStmt, mapping.IdxT(i+1))

		// fallback on index position
		arg := args[i]

		// override with ordinal if set
		for _, v := range args {
			if v.Ordinal == i+1 {
				arg = v
			}
		}

		// override with name if set
		for _, v := range args {
			if v.Name == name {
				arg = v
			}
		}

		state, err := s.bindValue(arg, i)
		if state == mapping.StateError {
			errMsg := mapping.PrepareError(*s.preparedStmt)
			err = errors.Join(err, getDuckDBError(errMsg))
			return errors.Join(errCouldNotBind, err)
		}
	}

	s.bound = true
	return nil
}

// Deprecated: Use ExecContext instead.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), argsToNamedArgs(args))
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.StmtExecContext interface.
func (s *Stmt) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	defer mapping.DestroyResult(res)

	ra := mapping.ValueInt64(res, 0, 0)
	return &result{ra}, nil
}

// ExecBound executes a bound query that doesn't return rows, such as an INSERT or UPDATE.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) ExecBound(ctx context.Context) (driver.Result, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	defer mapping.DestroyResult(res)

	ra := mapping.ValueInt64(res, 0, 0)
	return &result{ra}, nil
}

// Deprecated: Use QueryContext instead.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), argsToNamedArgs(args))
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.StmtQueryContext interface.
func (s *Stmt) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// QueryBound executes a bound query that may return rows, such as a SELECT.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) QueryBound(ctx context.Context) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// This method executes the query in steps and checks if context is cancelled before executing each step.
// It uses Pending Result Interface C APIs to achieve this. Reference - https://duckdb.org/docs/api/c/api#pending-result-interface
func (s *Stmt) execute(ctx context.Context, args []driver.NamedValue) (*mapping.Result, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext with active Rows")
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}
	return s.executeBound(ctx)
}

func (s *Stmt) executeBound(ctx context.Context) (*mapping.Result, error) {
	var pendingRes mapping.PendingResult
	if mapping.PendingPrepared(*s.preparedStmt, &pendingRes) == mapping.StateError {
		dbErr := getDuckDBError(mapping.PendingError(pendingRes))
		mapping.DestroyPending(&pendingRes)
		return nil, dbErr
	}
	defer mapping.DestroyPending(&pendingRes)

	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})

	// go-routine waiting to receive on the context or main channel.
	go func() {
		select {
		// Await an interrupt on the context.
		case <-ctx.Done():
			mapping.Interrupt(s.conn.conn)
			break
		// Await a done-signal on the main channel.
		// Reading from a closed channel succeeds immediately.
		case <-mainDoneCh:
			break
		}
		close(bgDoneCh)
	}()

	var res mapping.Result
	state := mapping.ExecutePending(pendingRes, &res)

	// We finished executing the pending query.
	// Close the main channel.
	close(mainDoneCh)

	// Wait for the background go-routine to finish, too.
	// Sometimes the go-routine is not scheduled immediately.
	// By the time it is scheduled, another query might be running on this connection.
	// If we don't wait for the go-routine to finish, it can cancel that new query.
	<-bgDoneCh

	if state == mapping.StateError {
		if ctx.Err() != nil {
			mapping.DestroyResult(&res)
			return nil, ctx.Err()
		}

		err := getDuckDBError(mapping.ResultError(&res))
		mapping.DestroyResult(&res)
		return nil, err
	}

	return &res, nil
}

func argsToNamedArgs(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for n, param := range values {
		args[n].Value = param
		args[n].Ordinal = n + 1
	}
	return args
}
