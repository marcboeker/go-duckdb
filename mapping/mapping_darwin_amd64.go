//go:build !duckdb_use_lib && !duckdb_use_static_lib

package mapping

import (
	bindings "github.com/duckdb/duckdb-go-bindings/darwin-amd64"
)

// ------------------------------------------------------------------ //
// Enums
// ------------------------------------------------------------------ //

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
	StateError = bindings.StateError
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

// ------------------------------------------------------------------ //
// Types
// ------------------------------------------------------------------ //

type (
	IdxT      = bindings.IdxT
	Date      = bindings.Date
	Time      = bindings.Time
	TimeTZ    = bindings.TimeTZ
	Timestamp = bindings.Timestamp
	Interval  = bindings.Interval
	HugeInt   = bindings.HugeInt
	StringT   = bindings.StringT
	ListEntry = bindings.ListEntry
)

var (
	DateSetDays               = bindings.DateSetDays
	DateStructGetYear         = bindings.DateStructGetYear
	DateStructGetMonth        = bindings.DateStructGetMonth
	DateStructGetDay          = bindings.DateStructGetDay
	TimeGetMicros             = bindings.TimeGetMicros
	TimeSetMicros             = bindings.TimeSetMicros
	TimeStructGetHour         = bindings.TimeStructGetHour
	TimeStructGetMinute       = bindings.TimeStructGetMinute
	TimeStructGetSecond       = bindings.TimeStructGetSecond
	TimeStructGetMicros       = bindings.TimeStructGetMicros
	TimeTZStructGetTimeStruct = bindings.TimeTZStructGetTimeStruct
	TimeTZStructGetOffset     = bindings.TimeTZStructGetOffset
	TimestampGetMicros        = bindings.TimestampGetMicros
	TimestampSetMicros        = bindings.TimestampSetMicros
	IntervalGetMonths         = bindings.IntervalGetMonths
	IntervalSetMonths         = bindings.IntervalSetMonths
	IntervalGetDays           = bindings.IntervalGetDays
	IntervalSetDays           = bindings.IntervalSetDays
	IntervalGetMicros         = bindings.IntervalGetMicros
	IntervalSetMicros         = bindings.IntervalSetMicros
	HugeIntGetLower           = bindings.HugeIntGetLower
	HugeIntSetLower           = bindings.HugeIntSetLower
	HugeIntGetUpper           = bindings.HugeIntGetUpper
	HugeIntSetUpper           = bindings.HugeIntSetUpper
	ListEntryGetOffset        = bindings.ListEntryGetOffset
	ListEntrySetOffset        = bindings.ListEntrySetOffset
	ListEntryGetLength        = bindings.ListEntryGetLength
	ListEntrySetLength        = bindings.ListEntrySetLength
)

// ------------------------------------------------------------------ //
// Pointers
// ------------------------------------------------------------------ //

type (
	Vector              = bindings.Vector
	Result              = bindings.Result
	Database            = bindings.Database
	Connection          = bindings.Connection
	PreparedStatement   = bindings.PreparedStatement
	ExtractedStatements = bindings.ExtractedStatements
	PendingResult       = bindings.PendingResult
	Appender            = bindings.Appender
	Config              = bindings.Config
	LogicalType         = bindings.LogicalType
	DataChunk           = bindings.DataChunk
	Value               = bindings.Value
	ProfilingInfo       = bindings.ProfilingInfo
	FunctionInfo        = bindings.FunctionInfo
	ScalarFunction      = bindings.ScalarFunction
	BindInfo            = bindings.BindInfo
	InitInfo            = bindings.InitInfo
	ReplacementScanInfo = bindings.ReplacementScanInfo
)

// ------------------------------------------------------------------ //
// Functions
// ------------------------------------------------------------------ //

var (
	OpenExt                                 = bindings.OpenExt
	Close                                   = bindings.Close
	Connect                                 = bindings.Connect
	Interrupt                               = bindings.Interrupt
	Disconnect                              = bindings.Disconnect
	CreateConfig                            = bindings.CreateConfig
	SetConfig                               = bindings.SetConfig
	DestroyConfig                           = bindings.DestroyConfig
	DestroyResult                           = bindings.DestroyResult
	ColumnName                              = bindings.ColumnName
	ColumnType                              = bindings.ColumnType
	ColumnLogicalType                       = bindings.ColumnLogicalType
	ColumnCount                             = bindings.ColumnCount
	ResultError                             = bindings.ResultError
	VectorSize                              = bindings.VectorSize
	StringTData                             = bindings.StringTData
	FromDate                                = bindings.FromDate
	CreateTimeTZ                            = bindings.CreateTimeTZ
	FromTimeTZ                              = bindings.FromTimeTZ
	DestroyPrepare                          = bindings.DestroyPrepare
	PrepareError                            = bindings.PrepareError
	NParams                                 = bindings.NParams
	ParameterName                           = bindings.ParameterName
	ParamType                               = bindings.ParamType
	PreparedStatementType                   = bindings.PreparedStatementType
	BindValue                               = bindings.BindValue
	BindBoolean                             = bindings.BindBoolean
	BindInt8                                = bindings.BindInt8
	BindInt16                               = bindings.BindInt16
	BindInt32                               = bindings.BindInt32
	BindInt64                               = bindings.BindInt64
	BindHugeInt                             = bindings.BindHugeInt
	BindUInt8                               = bindings.BindUInt8
	BindUInt16                              = bindings.BindUInt16
	BindUInt32                              = bindings.BindUInt32
	BindUInt64                              = bindings.BindUInt64
	BindFloat                               = bindings.BindFloat
	BindDouble                              = bindings.BindDouble
	BindDate                                = bindings.BindDate
	BindTime                                = bindings.BindTime
	BindTimestamp                           = bindings.BindTimestamp
	BindInterval                            = bindings.BindInterval
	BindVarchar                             = bindings.BindVarchar
	BindBlob                                = bindings.BindBlob
	BindNull                                = bindings.BindNull
	ExtractStatements                       = bindings.ExtractStatements
	PrepareExtractedStatement               = bindings.PrepareExtractedStatement
	ExtractStatementsError                  = bindings.ExtractStatementsError
	DestroyExtracted                        = bindings.DestroyExtracted
	PendingPrepared                         = bindings.PendingPrepared
	DestroyPending                          = bindings.DestroyPending
	PendingError                            = bindings.PendingError
	ExecutePending                          = bindings.ExecutePending
	DestroyValue                            = bindings.DestroyValue
	CreateVarchar                           = bindings.CreateVarchar
	CreateInt64                             = bindings.CreateInt64
	CreateTimeTZValue                       = bindings.CreateTimeTZValue
	GetBool                                 = bindings.GetBool
	GetInt8                                 = bindings.GetInt8
	GetUInt8                                = bindings.GetUInt8
	GetInt16                                = bindings.GetInt16
	GetUInt16                               = bindings.GetUInt16
	GetInt32                                = bindings.GetInt32
	GetUInt32                               = bindings.GetUInt32
	GetInt64                                = bindings.GetInt64
	GetUInt64                               = bindings.GetUInt64
	GetHugeInt                              = bindings.GetHugeInt
	GetFloat                                = bindings.GetFloat
	GetDouble                               = bindings.GetDouble
	GetDate                                 = bindings.GetDate
	GetTime                                 = bindings.GetTime
	GetTimeTZ                               = bindings.GetTimeTZ
	GetTimestamp                            = bindings.GetTimestamp
	GetInterval                             = bindings.GetInterval
	GetVarchar                              = bindings.GetVarchar
	GetMapSize                              = bindings.GetMapSize
	GetMapKey                               = bindings.GetMapKey
	GetMapValue                             = bindings.GetMapValue
	CreateLogicalType                       = bindings.CreateLogicalType
	LogicalTypeGetAlias                     = bindings.LogicalTypeGetAlias
	CreateListType                          = bindings.CreateListType
	CreateArrayType                         = bindings.CreateArrayType
	CreateMapType                           = bindings.CreateMapType
	CreateStructType                        = bindings.CreateStructType
	CreateEnumType                          = bindings.CreateEnumType
	CreateDecimalType                       = bindings.CreateDecimalType
	GetTypeId                               = bindings.GetTypeId
	DecimalWidth                            = bindings.DecimalWidth
	DecimalScale                            = bindings.DecimalScale
	DecimalInternalType                     = bindings.DecimalInternalType
	EnumInternalType                        = bindings.EnumInternalType
	EnumDictionarySize                      = bindings.EnumDictionarySize
	EnumDictionaryValue                     = bindings.EnumDictionaryValue
	ListTypeChildType                       = bindings.ListTypeChildType
	ArrayTypeChildType                      = bindings.ArrayTypeChildType
	ArrayTypeArraySize                      = bindings.ArrayTypeArraySize
	MapTypeKeyType                          = bindings.MapTypeKeyType
	MapTypeValueType                        = bindings.MapTypeValueType
	StructTypeChildCount                    = bindings.StructTypeChildCount
	StructTypeChildName                     = bindings.StructTypeChildName
	StructTypeChildType                     = bindings.StructTypeChildType
	DestroyLogicalType                      = bindings.DestroyLogicalType
	CreateDataChunk                         = bindings.CreateDataChunk
	DestroyDataChunk                        = bindings.DestroyDataChunk
	DataChunkGetColumnCount                 = bindings.DataChunkGetColumnCount
	DataChunkGetVector                      = bindings.DataChunkGetVector
	DataChunkGetSize                        = bindings.DataChunkGetSize
	DataChunkSetSize                        = bindings.DataChunkSetSize
	VectorGetColumnType                     = bindings.VectorGetColumnType
	VectorGetData                           = bindings.VectorGetData
	VectorGetValidity                       = bindings.VectorGetValidity
	VectorEnsureValidityWritable            = bindings.VectorEnsureValidityWritable
	VectorAssignStringElement               = bindings.VectorAssignStringElement
	VectorAssignStringElementLen            = bindings.VectorAssignStringElementLen
	ListVectorGetChild                      = bindings.ListVectorGetChild
	ListVectorGetSize                       = bindings.ListVectorGetSize
	ListVectorSetSize                       = bindings.ListVectorSetSize
	ListVectorReserve                       = bindings.ListVectorReserve
	StructVectorGetChild                    = bindings.StructVectorGetChild
	ArrayVectorGetChild                     = bindings.ArrayVectorGetChild
	ValiditySetRowInvalid                   = bindings.ValiditySetRowInvalid
	CreateScalarFunction                    = bindings.CreateScalarFunction
	DestroyScalarFunction                   = bindings.DestroyScalarFunction
	ScalarFunctionSetName                   = bindings.ScalarFunctionSetName
	ScalarFunctionSetVarargs                = bindings.ScalarFunctionSetVarargs
	ScalarFunctionSetSpecialHandling        = bindings.ScalarFunctionSetSpecialHandling
	ScalarFunctionSetVolatile               = bindings.ScalarFunctionSetVolatile
	ScalarFunctionAddParameter              = bindings.ScalarFunctionAddParameter
	ScalarFunctionSetReturnType             = bindings.ScalarFunctionSetReturnType
	ScalarFunctionSetExtraInfo              = bindings.ScalarFunctionSetExtraInfo
	ScalarFunctionSetFunction               = bindings.ScalarFunctionSetFunction
	RegisterScalarFunction                  = bindings.RegisterScalarFunction
	ScalarFunctionGetExtraInfo              = bindings.ScalarFunctionGetExtraInfo
	ScalarFunctionSetError                  = bindings.ScalarFunctionSetError
	CreateScalarFunctionSet                 = bindings.CreateScalarFunctionSet
	DestroyScalarFunctionSet                = bindings.DestroyScalarFunctionSet
	AddScalarFunctionToSet                  = bindings.AddScalarFunctionToSet
	RegisterScalarFunctionSet               = bindings.RegisterScalarFunctionSet
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
	BindGetExtraInfo                        = bindings.BindGetExtraInfo
	BindAddResultColumn                     = bindings.BindAddResultColumn
	BindGetParameter                        = bindings.BindGetParameter
	BindGetNamedParameter                   = bindings.BindGetNamedParameter
	BindSetBindData                         = bindings.BindSetBindData
	BindSetCardinality                      = bindings.BindSetCardinality
	BindSetError                            = bindings.BindSetError
	InitGetBindData                         = bindings.InitGetBindData
	InitSetInitData                         = bindings.InitSetInitData
	InitGetColumnCount                      = bindings.InitGetColumnCount
	InitGetColumnIndex                      = bindings.InitGetColumnIndex
	InitSetMaxThreads                       = bindings.InitSetMaxThreads
	FunctionGetBindData                     = bindings.FunctionGetBindData
	FunctionGetLocalInitData                = bindings.FunctionGetLocalInitData
	FunctionSetError                        = bindings.FunctionSetError
	AddReplacementScan                      = bindings.AddReplacementScan
	ReplacementScanSetFunctionName          = bindings.ReplacementScanSetFunctionName
	ReplacementScanAddParameter             = bindings.ReplacementScanAddParameter
	ReplacementScanSetError                 = bindings.ReplacementScanSetError
	GetProfilingInfo                        = bindings.GetProfilingInfo
	ProfilingInfoGetMetrics                 = bindings.ProfilingInfoGetMetrics
	ProfilingInfoGetChildCount              = bindings.ProfilingInfoGetChildCount
	ProfilingInfoGetChild                   = bindings.ProfilingInfoGetChild
	AppenderCreate                          = bindings.AppenderCreate
	AppenderColumnCount                     = bindings.AppenderColumnCount
	AppenderColumnType                      = bindings.AppenderColumnType
	AppenderError                           = bindings.AppenderError
	AppenderFlush                           = bindings.AppenderFlush
	AppenderDestroy                         = bindings.AppenderDestroy
	AppendDataChunk                         = bindings.AppendDataChunk
	ResultGetChunk                          = bindings.ResultGetChunk
	ResultChunkCount                        = bindings.ResultChunkCount
	ValueInt64                              = bindings.ValueInt64
	ValidityMaskValueIsValid                = bindings.ValidityMaskValueIsValid
)
