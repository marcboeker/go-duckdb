package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"

	m "github.com/marcboeker/go-duckdb/mapping"
)

type StmtType m.StatementType

const (
	STATEMENT_TYPE_INVALID      = StmtType(m.StatementTypeInvalid)
	STATEMENT_TYPE_SELECT       = StmtType(m.StatementTypeSelect)
	STATEMENT_TYPE_INSERT       = StmtType(m.StatementTypeInsert)
	STATEMENT_TYPE_UPDATE       = StmtType(m.StatementTypeUpdate)
	STATEMENT_TYPE_EXPLAIN      = StmtType(m.StatementTypeExplain)
	STATEMENT_TYPE_DELETE       = StmtType(m.StatementTypeDelete)
	STATEMENT_TYPE_PREPARE      = StmtType(m.StatementTypePrepare)
	STATEMENT_TYPE_CREATE       = StmtType(m.StatementTypeCreate)
	STATEMENT_TYPE_EXECUTE      = StmtType(m.StatementTypeExecute)
	STATEMENT_TYPE_ALTER        = StmtType(m.StatementTypeAlter)
	STATEMENT_TYPE_TRANSACTION  = StmtType(m.StatementTypeTransaction)
	STATEMENT_TYPE_COPY         = StmtType(m.StatementTypeCopy)
	STATEMENT_TYPE_ANALYZE      = StmtType(m.StatementTypeAnalyze)
	STATEMENT_TYPE_VARIABLE_SET = StmtType(m.StatementTypeVariableSet)
	STATEMENT_TYPE_CREATE_FUNC  = StmtType(m.StatementTypeCreateFunc)
	STATEMENT_TYPE_DROP         = StmtType(m.StatementTypeDrop)
	STATEMENT_TYPE_EXPORT       = StmtType(m.StatementTypeExport)
	STATEMENT_TYPE_PRAGMA       = StmtType(m.StatementTypePragma)
	STATEMENT_TYPE_VACUUM       = StmtType(m.StatementTypeVacuum)
	STATEMENT_TYPE_CALL         = StmtType(m.StatementTypeCall)
	STATEMENT_TYPE_SET          = StmtType(m.StatementTypeSet)
	STATEMENT_TYPE_LOAD         = StmtType(m.StatementTypeLoad)
	STATEMENT_TYPE_RELATION     = StmtType(m.StatementTypeRelation)
	STATEMENT_TYPE_EXTENSION    = StmtType(m.StatementTypeExtension)
	STATEMENT_TYPE_LOGICAL_PLAN = StmtType(m.StatementTypeLogicalPlan)
	STATEMENT_TYPE_ATTACH       = StmtType(m.StatementTypeAttach)
	STATEMENT_TYPE_DETACH       = StmtType(m.StatementTypeDetach)
	STATEMENT_TYPE_MULTI        = StmtType(m.StatementTypeMulti)
)

// Stmt implements the driver.Stmt interface.
type Stmt struct {
	conn             *Conn
	preparedStmt     *m.PreparedStatement
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
	m.DestroyPrepare(s.preparedStmt)
	return nil
}

// NumInput returns the number of placeholder parameters.
// Implements the driver.Stmt interface.
func (s *Stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	count := m.NParams(*s.preparedStmt)
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

	count := m.NParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return "", getError(errAPI, paramIndexError(n, uint64(count)))
	}

	name := m.ParameterName(*s.preparedStmt, m.IdxT(n))
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

	count := m.NParams(*s.preparedStmt)
	if n == 0 || n > int(count) {
		return TYPE_INVALID, getError(errAPI, paramIndexError(n, uint64(count)))
	}

	t := m.ParamType(*s.preparedStmt, m.IdxT(n))
	return Type(t), nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	if s.preparedStmt == nil {
		return STATEMENT_TYPE_INVALID, errUninitializedStmt
	}

	t := m.PreparedStatementType(*s.preparedStmt)
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

func (s *Stmt) bindHugeint(val *big.Int, n int) (m.State, error) {
	hugeint, err := hugeIntFromNative(val)
	if err != nil {
		return m.StateError, err
	}
	state := m.BindHugeInt(*s.preparedStmt, m.IdxT(n+1), hugeint)
	return state, nil
}

func (s *Stmt) bindTimestamp(val driver.NamedValue, t Type, n int) (m.State, error) {
	ts, err := getAPITimestamp(t, val.Value)
	if err != nil {
		return m.StateError, err
	}
	state := m.BindTimestamp(*s.preparedStmt, m.IdxT(n+1), ts)
	return state, nil
}

func (s *Stmt) bindDate(val driver.NamedValue, n int) (m.State, error) {
	date, err := getAPIDate(val.Value)
	if err != nil {
		return m.StateError, err
	}
	state := m.BindDate(*s.preparedStmt, m.IdxT(n+1), date)
	return state, nil
}

func (s *Stmt) bindTime(val driver.NamedValue, t Type, n int) (m.State, error) {
	ticks, err := getTimeTicks(val.Value)
	if err != nil {
		return m.StateError, err
	}

	if t == TYPE_TIME {
		var ti m.Time
		m.TimeSetMicros(&ti, ticks)
		state := m.BindTime(*s.preparedStmt, m.IdxT(n+1), ti)
		return state, nil
	}

	// TYPE_TIME_TZ: The UTC offset is 0.
	ti := m.CreateTimeTZ(ticks, 0)
	v := m.CreateTimeTZValue(ti)
	state := m.BindValue(*s.preparedStmt, m.IdxT(n+1), v)
	m.DestroyValue(&v)
	return state, nil
}

func (s *Stmt) bindComplexValue(val driver.NamedValue, n int) (m.State, error) {
	t, err := s.ParamType(n + 1)
	if err != nil {
		return m.StateError, err
	}
	if name, ok := unsupportedTypeToStringMap[t]; ok {
		return m.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	}

	switch t {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		return s.bindTimestamp(val, t, n)
	case TYPE_DATE:
		return s.bindDate(val, n)
	case TYPE_TIME, TYPE_TIME_TZ:
		return s.bindTime(val, t, n)
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_LIST, TYPE_STRUCT, TYPE_MAP,
		TYPE_ARRAY, TYPE_ENUM:
		// FIXME: for timestamps: distinguish between timestamp[_s|ms|ns] once available.
		// FIXME: for other types: duckdb_param_logical_type once available, then create duckdb_value + duckdb_bind_value
		// FIXME: for other types: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[t]
		return m.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	}
	return m.StateError, addIndexToError(unsupportedTypeError(unknownTypeErrMsg), n+1)
}

func (s *Stmt) bindValue(val driver.NamedValue, n int) (m.State, error) {
	switch v := val.Value.(type) {
	case bool:
		return m.BindBoolean(*s.preparedStmt, m.IdxT(n+1), v), nil
	case int8:
		return m.BindInt8(*s.preparedStmt, m.IdxT(n+1), v), nil
	case int16:
		return m.BindInt16(*s.preparedStmt, m.IdxT(n+1), v), nil
	case int32:
		return m.BindInt32(*s.preparedStmt, m.IdxT(n+1), v), nil
	case int64:
		return m.BindInt64(*s.preparedStmt, m.IdxT(n+1), v), nil
	case int:
		// int is at least 32 bits.
		return m.BindInt64(*s.preparedStmt, m.IdxT(n+1), int64(v)), nil
	case *big.Int:
		return s.bindHugeint(v, n)
	case Decimal:
		// FIXME: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[TYPE_DECIMAL]
		return m.StateError, addIndexToError(unsupportedTypeError(name), n+1)
	case uint8:
		return m.BindUInt8(*s.preparedStmt, m.IdxT(n+1), v), nil
	case uint16:
		return m.BindUInt16(*s.preparedStmt, m.IdxT(n+1), v), nil
	case uint32:
		return m.BindUInt32(*s.preparedStmt, m.IdxT(n+1), v), nil
	case uint64:
		return m.BindUInt64(*s.preparedStmt, m.IdxT(n+1), v), nil
	case float32:
		return m.BindFloat(*s.preparedStmt, m.IdxT(n+1), v), nil
	case float64:
		return m.BindDouble(*s.preparedStmt, m.IdxT(n+1), v), nil
	case string:
		return m.BindVarchar(*s.preparedStmt, m.IdxT(n+1), v), nil
	case []byte:
		return m.BindBlob(*s.preparedStmt, m.IdxT(n+1), v), nil
	case Interval:
		return m.BindInterval(*s.preparedStmt, m.IdxT(n+1), v.getAPIInterval()), nil
	case nil:
		return m.BindNull(*s.preparedStmt, m.IdxT(n+1)), nil
	}
	return s.bindComplexValue(val, n)
}

func (s *Stmt) bind(args []driver.NamedValue) error {
	if s.NumInput() > len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	// relaxed length check allow for unused parameters.
	for i := 0; i < s.NumInput(); i++ {
		name := m.ParameterName(*s.preparedStmt, m.IdxT(i+1))

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
		if state == m.StateError {
			errMsg := m.PrepareError(*s.preparedStmt)
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
	defer m.DestroyResult(res)

	ra := m.ValueInt64(res, 0, 0)
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
	defer m.DestroyResult(res)

	ra := m.ValueInt64(res, 0, 0)
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
func (s *Stmt) execute(ctx context.Context, args []driver.NamedValue) (*m.Result, error) {
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

func (s *Stmt) executeBound(ctx context.Context) (*m.Result, error) {
	var pendingRes m.PendingResult
	if m.PendingPrepared(*s.preparedStmt, &pendingRes) == m.StateError {
		dbErr := getDuckDBError(m.PendingError(pendingRes))
		m.DestroyPending(&pendingRes)
		return nil, dbErr
	}
	defer m.DestroyPending(&pendingRes)

	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			m.Interrupt(s.conn.conn)
			close(bgDoneCh)
			return
		case <-mainDoneCh:
			close(bgDoneCh)
			return
		}
	}()

	var res m.Result
	state := m.ExecutePending(pendingRes, &res)
	close(mainDoneCh)
	// also wait for background goroutine to finish
	// sometimes the bg goroutine is not scheduled immediately and by that time if another query is running on this connection
	// it can cancel that query so need to wait for it to finish as well
	<-bgDoneCh
	if state == m.StateError {
		if ctx.Err() != nil {
			m.DestroyResult(&res)
			return nil, ctx.Err()
		}

		err := getDuckDBError(m.ResultError(&res))
		m.DestroyResult(&res)
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
