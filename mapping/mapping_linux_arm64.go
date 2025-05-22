//go:build !duckdb_use_lib && !duckdb_use_static_lib

package mapping

import (
	bindings "github.com/duckdb/duckdb-go-bindings/linux-arm64"
)

// Enums.

type Type = bindings.Type

const (
	TypeInvalid     = bindings.TypeInvalid
	TypeBoolean     = bindings.TypeBoolean
	TypeTinyInt     = bindings.TypeTinyInt
	TypeSmallInt    = bindings.TypeSmallInt
	TypeInteger     = bindings.TypeInteger
	TypeBigInt      = bindings.TypeBigInt
	TypeUTinyInt    = bindings.TypeUTinyInt
	TypeUSmallInt   = bindings.TypeUSmallInt
	TypeUInteger    = bindings.TypeUInteger
	TypeUBigInt     = bindings.TypeUBigInt
	TypeFloat       = bindings.TypeFloat
	TypeDouble      = bindings.TypeDouble
	TypeTimestamp   = bindings.TypeTimestamp
	TypeDate        = bindings.TypeDate
	TypeTime        = bindings.TypeTime
	TypeInterval    = bindings.TypeInterval
	TypeHugeInt     = bindings.TypeHugeInt
	TypeUHugeInt    = bindings.TypeUHugeInt
	TypeVarchar     = bindings.TypeVarchar
	TypeBlob        = bindings.TypeBlob
	TypeDecimal     = bindings.TypeDecimal
	TypeTimestampS  = bindings.TypeTimestampS
	TypeTimestampMS = bindings.TypeTimestampMS
	TypeTimestampNS = bindings.TypeTimestampNS
	TypeEnum        = bindings.TypeEnum
	TypeList        = bindings.TypeList
	TypeStruct      = bindings.TypeStruct
	TypeMap         = bindings.TypeMap
	TypeArray       = bindings.TypeArray
	TypeUUID        = bindings.TypeUUID
	TypeUnion       = bindings.TypeUnion
	TypeBit         = bindings.TypeBit
	TypeTimeTZ      = bindings.TypeTimeTZ
	TypeTimestampTZ = bindings.TypeTimestampTZ
	TypeAny         = bindings.TypeAny
	TypeVarInt      = bindings.TypeVarInt
	TypeSQLNull     = bindings.TypeSQLNull
)

type State = bindings.State

const (
	StateSuccess = bindings.StateSuccess
	StateError   = bindings.StateError
)

type PendingState = bindings.PendingState

const (
	PendingStateResultReady      = bindings.PendingStateResultReady
	PendingStateResultNotReady   = bindings.PendingStateResultNotReady
	PendingStateError            = bindings.PendingStateError
	PendingStateNoTasksAvailable = bindings.PendingStateNoTasksAvailable
)

type ResultType = bindings.ResultType

const (
	ResultTypeInvalid     = bindings.ResultTypeInvalid
	ResultTypeChangedRows = bindings.ResultTypeChangedRows
	ResultTypeNothing     = bindings.ResultTypeNothing
	ResultTypeQueryResult = bindings.ResultTypeQueryResult
)

type StatementType = bindings.StatementType

const (
	StatementTypeInvalid     = bindings.StatementTypeInvalid
	StatementTypeSelect      = bindings.StatementTypeSelect
	StatementTypeInsert      = bindings.StatementTypeInsert
	StatementTypeUpdate      = bindings.StatementTypeUpdate
	StatementTypeExplain     = bindings.StatementTypeExplain
	StatementTypeDelete      = bindings.StatementTypeDelete
	StatementTypePrepare     = bindings.StatementTypePrepare
	StatementTypeCreate      = bindings.StatementTypeCreate
	StatementTypeExecute     = bindings.StatementTypeExecute
	StatementTypeAlter       = bindings.StatementTypeAlter
	StatementTypeTransaction = bindings.StatementTypeTransaction
	StatementTypeCopy        = bindings.StatementTypeCopy
	StatementTypeAnalyze     = bindings.StatementTypeAnalyze
	StatementTypeVariableSet = bindings.StatementTypeVariableSet
	StatementTypeCreateFunc  = bindings.StatementTypeCreateFunc
	StatementTypeDrop        = bindings.StatementTypeDrop
	StatementTypeExport      = bindings.StatementTypeExport
	StatementTypePragma      = bindings.StatementTypePragma
	StatementTypeVacuum      = bindings.StatementTypeVacuum
	StatementTypeCall        = bindings.StatementTypeCall
	StatementTypeSet         = bindings.StatementTypeSet
	StatementTypeLoad        = bindings.StatementTypeLoad
	StatementTypeRelation    = bindings.StatementTypeRelation
	StatementTypeExtension   = bindings.StatementTypeExtension
	StatementTypeLogicalPlan = bindings.StatementTypeLogicalPlan
	StatementTypeAttach      = bindings.StatementTypeAttach
	StatementTypeDetach      = bindings.StatementTypeDetach
	StatementTypeMulti       = bindings.StatementTypeMulti
)

type ErrorType = bindings.ErrorType

const (
	ErrorTypeInvalid              = bindings.ErrorTypeInvalid
	ErrorTypeOutOfRange           = bindings.ErrorTypeOutOfRange
	ErrorTypeConversion           = bindings.ErrorTypeConversion
	ErrorTypeUnknownType          = bindings.ErrorTypeUnknownType
	ErrorTypeDecimal              = bindings.ErrorTypeDecimal
	ErrorTypeMismatchType         = bindings.ErrorTypeMismatchType
	ErrorTypeDivideByZero         = bindings.ErrorTypeDivideByZero
	ErrorTypeObjectSize           = bindings.ErrorTypeObjectSize
	ErrorTypeInvalidType          = bindings.ErrorTypeInvalidType
	ErrorTypeSerialization        = bindings.ErrorTypeSerialization
	ErrorTypeTransaction          = bindings.ErrorTypeTransaction
	ErrorTypeNotImplemented       = bindings.ErrorTypeNotImplemented
	ErrorTypeExpression           = bindings.ErrorTypeExpression
	ErrorTypeCatalog              = bindings.ErrorTypeCatalog
	ErrorTypeParser               = bindings.ErrorTypeParser
	ErrorTypePlanner              = bindings.ErrorTypePlanner
	ErrorTypeScheduler            = bindings.ErrorTypeScheduler
	ErrorTypeExecutor             = bindings.ErrorTypeExecutor
	ErrorTypeConstraint           = bindings.ErrorTypeConstraint
	ErrorTypeIndex                = bindings.ErrorTypeIndex
	ErrorTypeStat                 = bindings.ErrorTypeStat
	ErrorTypeConnection           = bindings.ErrorTypeConnection
	ErrorTypeSyntax               = bindings.ErrorTypeSyntax
	ErrorTypeSettings             = bindings.ErrorTypeSettings
	ErrorTypeBinder               = bindings.ErrorTypeBinder
	ErrorTypeNetwork              = bindings.ErrorTypeNetwork
	ErrorTypeOptimizer            = bindings.ErrorTypeOptimizer
	ErrorTypeNullPointer          = bindings.ErrorTypeNullPointer
	ErrorTypeErrorIO              = bindings.ErrorTypeErrorIO
	ErrorTypeInterrupt            = bindings.ErrorTypeInterrupt
	ErrorTypeFatal                = bindings.ErrorTypeFatal
	ErrorTypeInternal             = bindings.ErrorTypeInternal
	ErrorTypeInvalidInput         = bindings.ErrorTypeInvalidInput
	ErrorTypeOutOfMemory          = bindings.ErrorTypeOutOfMemory
	ErrorTypePermission           = bindings.ErrorTypePermission
	ErrorTypeParameterNotResolved = bindings.ErrorTypeParameterNotResolved
	ErrorTypeParameterNotAllowed  = bindings.ErrorTypeParameterNotAllowed
	ErrorTypeDependency           = bindings.ErrorTypeDependency
	ErrorTypeHTTP                 = bindings.ErrorTypeHTTP
	ErrorTypeMissingExtension     = bindings.ErrorTypeMissingExtension
	ErrorTypeAutoload             = bindings.ErrorTypeAutoload
	ErrorTypeSequence             = bindings.ErrorTypeSequence
	ErrorTypeInvalidConfiguration = bindings.ErrorTypeInvalidConfiguration
)

type CastMode = bindings.CastMode

const (
	CastModeNormal = bindings.CastModeNormal
	CastModeTry    = bindings.CastModeTry
)

// Types.

// Types without internal pointers.

type (
	IdxT              = bindings.IdxT
	SelT              = bindings.SelT
	Date              = bindings.Date
	DateStruct        = bindings.DateStruct
	Time              = bindings.Time
	TimeStruct        = bindings.TimeStruct
	TimeTZ            = bindings.TimeTZ
	TimeTZStruct      = bindings.TimeTZStruct
	Timestamp         = bindings.Timestamp
	TimestampS        = bindings.TimestampS
	TimestampMS       = bindings.TimestampMS
	TimestampNS       = bindings.TimestampNS
	Interval          = bindings.Interval
	HugeInt           = bindings.HugeInt
	UHugeInt          = bindings.UHugeInt
	Decimal           = bindings.Decimal
	QueryProgressType = bindings.QueryProgressType
	StringT           = bindings.StringT
	ListEntry         = bindings.ListEntry
	Blob              = bindings.Blob
	Bit               = bindings.Bit
	VarInt            = bindings.VarInt
)

// Helper functions for types without internal pointers.

var (
	NewDate                  = bindings.NewDate
	DateMembers              = bindings.DateMembers
	NewDateStruct            = bindings.NewDateStruct
	DateStructMembers        = bindings.DateStructMembers
	NewTime                  = bindings.NewTime
	TimeMembers              = bindings.TimeMembers
	NewTimeStruct            = bindings.NewTimeStruct
	TimeStructMembers        = bindings.TimeStructMembers
	NewTimeTZ                = bindings.NewTimeTZ
	TimeTZMembers            = bindings.TimeTZMembers
	NewTimeTZStruct          = bindings.NewTimeTZStruct
	TimeTZStructMembers      = bindings.TimeTZStructMembers
	NewTimestamp             = bindings.NewTimestamp
	TimestampMembers         = bindings.TimestampMembers
	NewTimestampS            = bindings.NewTimestampS
	TimestampSMembers        = bindings.TimestampSMembers
	NewTimestampMS           = bindings.NewTimestampMS
	TimestampMSMembers       = bindings.TimestampMSMembers
	NewTimestampNS           = bindings.NewTimestampNS
	TimestampNSMembers       = bindings.TimestampNSMembers
	NewTimestampStruct       = bindings.NewTimestampStruct
	TimestampStructMembers   = bindings.TimestampStructMembers
	NewInterval              = bindings.NewInterval
	IntervalMembers          = bindings.IntervalMembers
	NewHugeInt               = bindings.NewHugeInt
	HugeIntMembers           = bindings.HugeIntMembers
	NewUHugeInt              = bindings.NewUHugeInt
	UHugeIntMembers          = bindings.UHugeIntMembers
	NewDecimal               = bindings.NewDecimal
	DecimalMembers           = bindings.DecimalMembers
	NewQueryProgressType     = bindings.NewQueryProgressType
	QueryProgressTypeMembers = bindings.QueryProgressTypeMembers
	NewListEntry             = bindings.NewListEntry
	ListEntryMembers         = bindings.ListEntryMembers
)

// Helper functions for types with internal fields that need freeing.

var (
	DestroyBlob   = bindings.DestroyBlob
	DestroyBit    = bindings.DestroyBit
	DestroyVarInt = bindings.DestroyVarInt
)

// Types with internal pointers.

type (
	Column = bindings.Column
	Result = bindings.Result
)

// Pointer types.

type (
	Vector              = bindings.Vector
	SelectionVector     = bindings.SelectionVector
	InstanceCache       = bindings.InstanceCache
	Database            = bindings.Database
	Connection          = bindings.Connection
	ClientContext       = bindings.ClientContext
	PreparedStatement   = bindings.PreparedStatement
	ExtractedStatements = bindings.ExtractedStatements
	PendingResult       = bindings.PendingResult
	Appender            = bindings.Appender
	TableDescription    = bindings.TableDescription
	Config              = bindings.Config
	LogicalType         = bindings.LogicalType
	CreateTypeInfo      = bindings.CreateTypeInfo
	DataChunk           = bindings.DataChunk
	Value               = bindings.Value
	ProfilingInfo       = bindings.ProfilingInfo
	FunctionInfo        = bindings.FunctionInfo
	ScalarFunction      = bindings.ScalarFunction
	ScalarFunctionSet   = bindings.ScalarFunctionSet
	TableFunction       = bindings.TableFunction
	BindInfo            = bindings.BindInfo
	InitInfo            = bindings.InitInfo
	ReplacementScanInfo = bindings.ReplacementScanInfo
)

// Functions.

// Open, connect.

var (
	CreateInstanceCache          = bindings.CreateInstanceCache
	GetOrCreateFromCache         = bindings.GetOrCreateFromCache
	DestroyInstanceCache         = bindings.DestroyInstanceCache
	Open                         = bindings.Open
	OpenExt                      = bindings.OpenExt
	Close                        = bindings.Close
	Connect                      = bindings.Connect
	Interrupt                    = bindings.Interrupt
	QueryProgress                = bindings.QueryProgress
	Disconnect                   = bindings.Disconnect
	ConnectionGetClientContext   = bindings.ConnectionGetClientContext
	ClientContextGetConnectionId = bindings.ClientContextGetConnectionId
	DestroyClientContext         = bindings.DestroyClientContext
	LibraryVersion               = bindings.LibraryVersion
	GetTableNames                = bindings.GetTableNames
)

// Configuration.

var (
	CreateConfig  = bindings.CreateConfig
	ConfigCount   = bindings.ConfigCount
	GetConfigFlag = bindings.GetConfigFlag
	SetConfig     = bindings.SetConfig
	DestroyConfig = bindings.DestroyConfig
)

// Query execution.

var (
	Query               = bindings.Query
	DestroyResult       = bindings.DestroyResult
	ColumnName          = bindings.ColumnName
	ColumnType          = bindings.ColumnType
	ResultStatementType = bindings.ResultStatementType
	ColumnLogicalType   = bindings.ColumnLogicalType
	ColumnCount         = bindings.ColumnCount
	RowsChanged         = bindings.RowsChanged
	ResultError         = bindings.ResultError
	ResultErrorType     = bindings.ResultErrorType
)

// Result functions.

var (
	ResultGetChunk   = bindings.ResultGetChunk
	ResultChunkCount = bindings.ResultChunkCount
	ResultReturnType = bindings.ResultReturnType
)

// Safe fetch functions.

var ValueInt64 = bindings.ValueInt64

// Helpers.

var (
	Free            = bindings.Free
	VectorSize      = bindings.VectorSize
	StringIsInlined = bindings.StringIsInlined
	StringTLength   = bindings.StringTLength
	StringTData     = bindings.StringTData
)

// Date time timestamp helpers.

var (
	FromDate            = bindings.FromDate
	ToDate              = bindings.ToDate
	IsFiniteDate        = bindings.IsFiniteDate
	FromTime            = bindings.FromTime
	CreateTimeTZ        = bindings.CreateTimeTZ
	FromTimeTZ          = bindings.FromTimeTZ
	ToTime              = bindings.ToTime
	FromTimestamp       = bindings.FromTimestamp
	ToTimestamp         = bindings.ToTimestamp
	IsFiniteTimestamp   = bindings.IsFiniteTimestamp
	IsFiniteTimestampS  = bindings.IsFiniteTimestampS
	IsFiniteTimestampMS = bindings.IsFiniteTimestampMS
	IsFiniteTimestampNS = bindings.IsFiniteTimestampNS
)

// Hugeint helpers.

var (
	HugeIntToDouble = bindings.HugeIntToDouble
	DoubleToHugeInt = bindings.DoubleToHugeInt
)

// Unsigned hugeint helpers.

var (
	UHugeIntToDouble = bindings.UHugeIntToDouble
	DoubleToUHugeInt = bindings.DoubleToUHugeInt
)

// Decimal helpers.

var (
	DoubleToDecimal = bindings.DoubleToDecimal
	DecimalToDouble = bindings.DecimalToDouble
)

// Prepared statements.

var (
	Prepare               = bindings.Prepare
	DestroyPrepare        = bindings.DestroyPrepare
	PrepareError          = bindings.PrepareError
	NParams               = bindings.NParams
	ParameterName         = bindings.ParameterName
	ParamType             = bindings.ParamType
	ParamLogicalType      = bindings.ParamLogicalType
	ClearBindings         = bindings.ClearBindings
	PreparedStatementType = bindings.PreparedStatementType
)

// Bind values to prepared statements.

var (
	BindValue          = bindings.BindValue
	BindParameterIndex = bindings.BindParameterIndex
	BindBoolean        = bindings.BindBoolean
	BindInt8           = bindings.BindInt8
	BindInt16          = bindings.BindInt16
	BindInt32          = bindings.BindInt32
	BindInt64          = bindings.BindInt64
	BindHugeInt        = bindings.BindHugeInt
	BindUHugeInt       = bindings.BindUHugeInt
	BindDecimal        = bindings.BindDecimal
	BindUInt8          = bindings.BindUInt8
	BindUInt16         = bindings.BindUInt16
	BindUInt32         = bindings.BindUInt32
	BindUInt64         = bindings.BindUInt64
	BindFloat          = bindings.BindFloat
	BindDouble         = bindings.BindDouble
	BindDate           = bindings.BindDate
	BindTime           = bindings.BindTime
	BindTimestamp      = bindings.BindTimestamp
	BindTimestampTZ    = bindings.BindTimestampTZ
	BindInterval       = bindings.BindInterval
	BindVarchar        = bindings.BindVarchar
	BindVarcharLength  = bindings.BindVarcharLength
	BindBlob           = bindings.BindBlob
	BindNull           = bindings.BindNull
)

// Execute prepared statements.

var ExecutePrepared = bindings.ExecutePrepared

// Extract statements.

var (
	ExtractStatements         = bindings.ExtractStatements
	PrepareExtractedStatement = bindings.PrepareExtractedStatement
	ExtractStatementsError    = bindings.ExtractStatementsError
	DestroyExtracted          = bindings.DestroyExtracted
)

// Pending result interface.

var (
	PendingPrepared            = bindings.PendingPrepared
	DestroyPending             = bindings.DestroyPending
	PendingError               = bindings.PendingError
	PendingExecuteTask         = bindings.PendingExecuteTask
	PendingExecuteCheckState   = bindings.PendingExecuteCheckState
	ExecutePending             = bindings.ExecutePending
	PendingExecutionIsFinished = bindings.PendingExecutionIsFinished
)

// Value interface.

var (
	DestroyValue        = bindings.DestroyValue
	CreateVarchar       = bindings.CreateVarchar
	CreateVarcharLength = bindings.CreateVarcharLength
	CreateBool          = bindings.CreateBool
	CreateInt8          = bindings.CreateInt8
	CreateUInt8         = bindings.CreateUInt8
	CreateInt16         = bindings.CreateInt16
	CreateUInt16        = bindings.CreateUInt16
	CreateInt32         = bindings.CreateInt32
	CreateUInt32        = bindings.CreateUInt32
	CreateUInt64        = bindings.CreateUInt64
	CreateInt64         = bindings.CreateInt64
	CreateHugeInt       = bindings.CreateHugeInt
	CreateUHugeInt      = bindings.CreateUHugeInt
	CreateVarint        = bindings.CreateVarint
	CreateDecimal       = bindings.CreateDecimal
	CreateFloat         = bindings.CreateFloat
	CreateDouble        = bindings.CreateDouble
	CreateDate          = bindings.CreateDate
	CreateTime          = bindings.CreateTime
	CreateTimeTZValue   = bindings.CreateTimeTZValue
	CreateTimestamp     = bindings.CreateTimestamp
	CreateTimestampTZ   = bindings.CreateTimestampTZ
	CreateTimestampS    = bindings.CreateTimestampS
	CreateTimestampMS   = bindings.CreateTimestampMS
	CreateTimestampNS   = bindings.CreateTimestampNS
	CreateInterval      = bindings.CreateInterval
	CreateBlob          = bindings.CreateBlob
	CreateBit           = bindings.CreateBit
	CreateUUID          = bindings.CreateUUID
	GetBool             = bindings.GetBool
	GetInt8             = bindings.GetInt8
	GetUInt8            = bindings.GetUInt8
	GetInt16            = bindings.GetInt16
	GetUInt16           = bindings.GetUInt16
	GetInt32            = bindings.GetInt32
	GetUInt32           = bindings.GetUInt32
	GetInt64            = bindings.GetInt64
	GetUInt64           = bindings.GetUInt64
	GetHugeInt          = bindings.GetHugeInt
	GetUHugeInt         = bindings.GetUHugeInt
	GetVarInt           = bindings.GetVarInt
	GetDecimal          = bindings.GetDecimal
	GetFloat            = bindings.GetFloat
	GetDouble           = bindings.GetDouble
	GetDate             = bindings.GetDate
	GetTime             = bindings.GetTime
	GetTimeTZ           = bindings.GetTimeTZ
	GetTimestamp        = bindings.GetTimestamp
	GetTimestampTZ      = bindings.GetTimestampTZ
	GetTimestampS       = bindings.GetTimestampS
	GetTimestampMS      = bindings.GetTimestampMS
	GetTimestampNS      = bindings.GetTimestampNS
	GetInterval         = bindings.GetInterval
	GetValueType        = bindings.GetValueType
	GetBlob             = bindings.GetBlob
	GetBit              = bindings.GetBit
	GetUUID             = bindings.GetUUID
	GetVarchar          = bindings.GetVarchar
	CreateStructValue   = bindings.CreateStructValue
	CreateListValue     = bindings.CreateListValue
	CreateArrayValue    = bindings.CreateArrayValue
	CreateMapValue      = bindings.CreateMapValue
	CreateUnionValue    = bindings.CreateUnionValue
	GetMapSize          = bindings.GetMapSize
	GetMapKey           = bindings.GetMapKey
	GetMapValue         = bindings.GetMapValue
	IsNullValue         = bindings.IsNullValue
	CreateNullValue     = bindings.CreateNullValue
	GetListSize         = bindings.GetListSize
	GetListChild        = bindings.GetListChild
	CreateEnumValue     = bindings.CreateEnumValue
	GetEnumValue        = bindings.GetEnumValue
	GetStructChild      = bindings.GetStructChild
	ValueToString       = bindings.ValueToString
)

// Logical type interface.

var (
	CreateLogicalType    = bindings.CreateLogicalType
	LogicalTypeGetAlias  = bindings.LogicalTypeGetAlias
	LogicalTypeSetAlias  = bindings.LogicalTypeSetAlias
	CreateListType       = bindings.CreateListType
	CreateArrayType      = bindings.CreateArrayType
	CreateMapType        = bindings.CreateMapType
	CreateUnionType      = bindings.CreateUnionType
	CreateStructType     = bindings.CreateStructType
	CreateEnumType       = bindings.CreateEnumType
	CreateDecimalType    = bindings.CreateDecimalType
	GetTypeId            = bindings.GetTypeId
	DecimalWidth         = bindings.DecimalWidth
	DecimalScale         = bindings.DecimalScale
	DecimalInternalType  = bindings.DecimalInternalType
	EnumInternalType     = bindings.EnumInternalType
	EnumDictionarySize   = bindings.EnumDictionarySize
	EnumDictionaryValue  = bindings.EnumDictionaryValue
	ListTypeChildType    = bindings.ListTypeChildType
	ArrayTypeChildType   = bindings.ArrayTypeChildType
	ArrayTypeArraySize   = bindings.ArrayTypeArraySize
	MapTypeKeyType       = bindings.MapTypeKeyType
	MapTypeValueType     = bindings.MapTypeValueType
	StructTypeChildCount = bindings.StructTypeChildCount
	StructTypeChildName  = bindings.StructTypeChildName
	StructTypeChildType  = bindings.StructTypeChildType
	UnionTypeMemberCount = bindings.UnionTypeMemberCount
	UnionTypeMemberName  = bindings.UnionTypeMemberName
	UnionTypeMemberType  = bindings.UnionTypeMemberType
	DestroyLogicalType   = bindings.DestroyLogicalType
	RegisterLogicalType  = bindings.RegisterLogicalType
)

// Data chunk interface.

var (
	CreateDataChunk         = bindings.CreateDataChunk
	DestroyDataChunk        = bindings.DestroyDataChunk
	DataChunkReset          = bindings.DataChunkReset
	DataChunkGetColumnCount = bindings.DataChunkGetColumnCount
	DataChunkGetVector      = bindings.DataChunkGetVector
	DataChunkGetSize        = bindings.DataChunkGetSize
	DataChunkSetSize        = bindings.DataChunkSetSize
)

// Vector interface.

var (
	CreateVector                 = bindings.CreateVector
	DestroyVector                = bindings.DestroyVector
	VectorGetColumnType          = bindings.VectorGetColumnType
	VectorGetData                = bindings.VectorGetData
	VectorGetValidity            = bindings.VectorGetValidity
	VectorEnsureValidityWritable = bindings.VectorEnsureValidityWritable
	VectorAssignStringElement    = bindings.VectorAssignStringElement
	VectorAssignStringElementLen = bindings.VectorAssignStringElementLen
	ListVectorGetChild           = bindings.ListVectorGetChild
	ListVectorGetSize            = bindings.ListVectorGetSize
	ListVectorSetSize            = bindings.ListVectorSetSize
	ListVectorReserve            = bindings.ListVectorReserve
	StructVectorGetChild         = bindings.StructVectorGetChild
	ArrayVectorGetChild          = bindings.ArrayVectorGetChild
	SliceVector                  = bindings.SliceVector
	VectorReferenceValue         = bindings.VectorReferenceValue
	VectorReferenceVector        = bindings.VectorReferenceVector
)

// Validity mask functions.

var (
	ValidityRowIsValid     = bindings.ValidityRowIsValid
	ValiditySetRowValidity = bindings.ValiditySetRowValidity
	ValiditySetRowInvalid  = bindings.ValiditySetRowInvalid
	ValiditySetRowValid    = bindings.ValiditySetRowValid
)

// Scalar functions.

var (
	CreateScalarFunction             = bindings.CreateScalarFunction
	DestroyScalarFunction            = bindings.DestroyScalarFunction
	ScalarFunctionSetName            = bindings.ScalarFunctionSetName
	ScalarFunctionSetVarargs         = bindings.ScalarFunctionSetVarargs
	ScalarFunctionSetSpecialHandling = bindings.ScalarFunctionSetSpecialHandling
	ScalarFunctionSetVolatile        = bindings.ScalarFunctionSetVolatile
	ScalarFunctionAddParameter       = bindings.ScalarFunctionAddParameter
	ScalarFunctionSetReturnType      = bindings.ScalarFunctionSetReturnType
	ScalarFunctionSetExtraInfo       = bindings.ScalarFunctionSetExtraInfo
	ScalarFunctionSetBind            = bindings.ScalarFunctionSetBind
	ScalarFunctionSetBindData        = bindings.ScalarFunctionSetBindData
	ScalarFunctionBindSetError       = bindings.ScalarFunctionBindSetError
	ScalarFunctionSetFunction        = bindings.ScalarFunctionSetFunction
	RegisterScalarFunction           = bindings.RegisterScalarFunction
	ScalarFunctionGetExtraInfo       = bindings.ScalarFunctionGetExtraInfo
	ScalarFunctionGetBindData        = bindings.ScalarFunctionGetBindData
	ScalarFunctionGetClientContext   = bindings.ScalarFunctionGetClientContext
	ScalarFunctionSetError           = bindings.ScalarFunctionSetError
	CreateScalarFunctionSet          = bindings.CreateScalarFunctionSet
	DestroyScalarFunctionSet         = bindings.DestroyScalarFunctionSet
	AddScalarFunctionToSet           = bindings.AddScalarFunctionToSet
	RegisterScalarFunctionSet        = bindings.RegisterScalarFunctionSet
)

// Selection vector functions.

var (
	CreateSelectionVector     = bindings.CreateSelectionVector
	DestroySelectionVector    = bindings.DestroySelectionVector
	SelectionVectorGetDataPtr = bindings.SelectionVectorGetDataPtr
)

// Table functions.

var (
	CreateTableFunction                     = bindings.CreateTableFunction
	DestroyTableFunction                    = bindings.DestroyTableFunction
	TableFunctionSetName                    = bindings.TableFunctionSetName
	TableFunctionAddParameter               = bindings.TableFunctionAddParameter
	TableFunctionAddNamedParameter          = bindings.TableFunctionAddNamedParameter
	TableFunctionSetExtraInfo               = bindings.TableFunctionSetExtraInfo
	TableFunctionSetBind                    = bindings.TableFunctionSetBind
	TableFunctionSetInit                    = bindings.TableFunctionSetInit
	TableFunctionSetLocalInit               = bindings.TableFunctionSetLocalInit
	TableFunctionSetFunction                = bindings.TableFunctionSetFunction
	TableFunctionSupportsProjectionPushdown = bindings.TableFunctionSupportsProjectionPushdown
	RegisterTableFunction                   = bindings.RegisterTableFunction
)

// Table function bind.

var (
	BindGetExtraInfo      = bindings.BindGetExtraInfo
	BindAddResultColumn   = bindings.BindAddResultColumn
	BindGetParameterCount = bindings.BindGetParameterCount
	BindGetParameter      = bindings.BindGetParameter
	BindGetNamedParameter = bindings.BindGetNamedParameter
	BindSetBindData       = bindings.BindSetBindData
	BindSetCardinality    = bindings.BindSetCardinality
	BindSetError          = bindings.BindSetError
)

// Table function init.

var (
	InitGetExtraInfo   = bindings.InitGetExtraInfo
	InitGetBindData    = bindings.InitGetBindData
	InitSetInitData    = bindings.InitSetInitData
	InitGetColumnCount = bindings.InitGetColumnCount
	InitGetColumnIndex = bindings.InitGetColumnIndex
	InitSetMaxThreads  = bindings.InitSetMaxThreads
	InitSetError       = bindings.InitSetError
)

// Table function.

var (
	FunctionGetExtraInfo     = bindings.FunctionGetExtraInfo
	FunctionGetBindData      = bindings.FunctionGetBindData
	FunctionGetInitData      = bindings.FunctionGetInitData
	FunctionGetLocalInitData = bindings.FunctionGetLocalInitData
	FunctionSetError         = bindings.FunctionSetError
)

// Replacement scans.

var (
	AddReplacementScan             = bindings.AddReplacementScan
	ReplacementScanSetFunctionName = bindings.ReplacementScanSetFunctionName
	ReplacementScanAddParameter    = bindings.ReplacementScanAddParameter
	ReplacementScanSetError        = bindings.ReplacementScanSetError
)

// Profiling info.

var (
	GetProfilingInfo           = bindings.GetProfilingInfo
	ProfilingInfoGetValue      = bindings.ProfilingInfoGetValue
	ProfilingInfoGetMetrics    = bindings.ProfilingInfoGetMetrics
	ProfilingInfoGetChildCount = bindings.ProfilingInfoGetChildCount
	ProfilingInfoGetChild      = bindings.ProfilingInfoGetChild
)

// Appender.

var (
	AppenderCreate       = bindings.AppenderCreate
	AppenderCreateExt    = bindings.AppenderCreateExt
	AppenderColumnCount  = bindings.AppenderColumnCount
	AppenderColumnType   = bindings.AppenderColumnType
	AppenderError        = bindings.AppenderError
	AppenderFlush        = bindings.AppenderFlush
	AppenderClose        = bindings.AppenderClose
	AppenderDestroy      = bindings.AppenderDestroy
	AppenderAddColumn    = bindings.AppenderAddColumn
	AppenderClearColumns = bindings.AppenderClearColumns
	AppendDefaultToChunk = bindings.AppendDefaultToChunk
	AppendDataChunk      = bindings.AppendDataChunk
)

// Table description.

var (
	TableDescriptionCreate        = bindings.TableDescriptionCreate
	TableDescriptionCreateExt     = bindings.TableDescriptionCreateExt
	TableDescriptionDestroy       = bindings.TableDescriptionDestroy
	TableDescriptionError         = bindings.TableDescriptionError
	ColumnHasDefault              = bindings.ColumnHasDefault
	TableDescriptionGetColumnName = bindings.TableDescriptionGetColumnName
)

// Go bindings helper.

var ValidityMaskValueIsValid = bindings.ValidityMaskValueIsValid

// duckdb-go-bindings helper.

var VerifyAllocationCounters = bindings.VerifyAllocationCounters
