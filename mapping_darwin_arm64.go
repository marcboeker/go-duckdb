//go:build !duckdb_use_lib && !duckdb_use_static_lib

package duckdb

import (
	bindings "github.com/duckdb/duckdb-go-bindings/darwin-arm64"
)

// ------------------------------------------------------------------ //
// Enums
// ------------------------------------------------------------------ //

type apiType bindings.Type

const (
	apiTypeInvalid     = apiType(bindings.TypeInvalid)
	apiTypeBoolean     = apiType(bindings.TypeBoolean)
	apiTypeTinyInt     = apiType(bindings.TypeTinyInt)
	apiTypeSmallInt    = apiType(bindings.TypeSmallInt)
	apiTypeInteger     = apiType(bindings.TypeInteger)
	apiTypeBigInt      = apiType(bindings.TypeBigInt)
	apiTypeUTinyInt    = apiType(bindings.TypeUTinyInt)
	apiTypeUSmallInt   = apiType(bindings.TypeUSmallInt)
	apiTypeUInteger    = apiType(bindings.TypeUInteger)
	apiTypeUBigInt     = apiType(bindings.TypeUBigInt)
	apiTypeFloat       = apiType(bindings.TypeFloat)
	apiTypeDouble      = apiType(bindings.TypeDouble)
	apiTypeTimestamp   = apiType(bindings.TypeTimestamp)
	apiTypeDate        = apiType(bindings.TypeDate)
	apiTypeTime        = apiType(bindings.TypeTime)
	apiTypeInterval    = apiType(bindings.TypeInterval)
	apiTypeHugeInt     = apiType(bindings.TypeHugeInt)
	apiTypeUHugeInt    = apiType(bindings.TypeUHugeInt)
	apiTypeVarchar     = apiType(bindings.TypeVarchar)
	apiTypeBlob        = apiType(bindings.TypeBlob)
	apiTypeDecimal     = apiType(bindings.TypeDecimal)
	apiTypeTimestampS  = apiType(bindings.TypeTimestampS)
	apiTypeTimestampMS = apiType(bindings.TypeTimestampMS)
	apiTypeTimestampNS = apiType(bindings.TypeTimestampNS)
	apiTypeEnum        = apiType(bindings.TypeEnum)
	apiTypeList        = apiType(bindings.TypeList)
	apiTypeStruct      = apiType(bindings.TypeStruct)
	apiTypeMap         = apiType(bindings.TypeMap)
	apiTypeArray       = apiType(bindings.TypeArray)
	apiTypeUUID        = apiType(bindings.TypeUUID)
	apiTypeUnion       = apiType(bindings.TypeUnion)
	apiTypeBit         = apiType(bindings.TypeBit)
	apiTypeTimeTZ      = apiType(bindings.TypeTimeTZ)
	apiTypeTimestampTZ = apiType(bindings.TypeTimestampTZ)
	apiTypeAny         = apiType(bindings.TypeAny)
	apiTypeVarInt      = apiType(bindings.TypeVarInt)
	apiTypeSQLNull     = apiType(bindings.TypeSQLNull)
)

type apiState bindings.State

const (
	apiStateError = apiState(bindings.StateError)
)

type apiStatementType bindings.StatementType

const (
	apiStatementTypeInvalid     = apiStatementType(bindings.StatementTypeInvalid)
	apiStatementTypeSelect      = apiStatementType(bindings.StatementTypeSelect)
	apiStatementTypeInsert      = apiStatementType(bindings.StatementTypeInsert)
	apiStatementTypeUpdate      = apiStatementType(bindings.StatementTypeUpdate)
	apiStatementTypeExplain     = apiStatementType(bindings.StatementTypeExplain)
	apiStatementTypeDelete      = apiStatementType(bindings.StatementTypeDelete)
	apiStatementTypePrepare     = apiStatementType(bindings.StatementTypePrepare)
	apiStatementTypeCreate      = apiStatementType(bindings.StatementTypeCreate)
	apiStatementTypeExecute     = apiStatementType(bindings.StatementTypeExecute)
	apiStatementTypeAlter       = apiStatementType(bindings.StatementTypeAlter)
	apiStatementTypeTransaction = apiStatementType(bindings.StatementTypeTransaction)
	apiStatementTypeCopy        = apiStatementType(bindings.StatementTypeCopy)
	apiStatementTypeAnalyze     = apiStatementType(bindings.StatementTypeAnalyze)
	apiStatementTypeVariableSet = apiStatementType(bindings.StatementTypeVariableSet)
	apiStatementTypeCreateFunc  = apiStatementType(bindings.StatementTypeCreateFunc)
	apiStatementTypeDrop        = apiStatementType(bindings.StatementTypeDrop)
	apiStatementTypeExport      = apiStatementType(bindings.StatementTypeExport)
	apiStatementTypePragma      = apiStatementType(bindings.StatementTypePragma)
	apiStatementTypeVacuum      = apiStatementType(bindings.StatementTypeVacuum)
	apiStatementTypeCall        = apiStatementType(bindings.StatementTypeCall)
	apiStatementTypeSet         = apiStatementType(bindings.StatementTypeSet)
	apiStatementTypeLoad        = apiStatementType(bindings.StatementTypeLoad)
	apiStatementTypeRelation    = apiStatementType(bindings.StatementTypeRelation)
	apiStatementTypeExtension   = apiStatementType(bindings.StatementTypeExtension)
	apiStatementTypeLogicalPlan = apiStatementType(bindings.StatementTypeLogicalPlan)
	apiStatementTypeAttach      = apiStatementType(bindings.StatementTypeAttach)
	apiStatementTypeDetach      = apiStatementType(bindings.StatementTypeDetach)
	apiStatementTypeMulti       = apiStatementType(bindings.StatementTypeMulti)
)

// ------------------------------------------------------------------ //
// Types
// ------------------------------------------------------------------ //

type (
	apiDate      = bindings.Date
	apiTime      = bindings.Time
	apiTimeTZ    = bindings.TimeTZ
	apiTimestamp = bindings.Timestamp
	apiInterval  = bindings.Interval
	apiHugeInt   = bindings.HugeInt
	apiStringT   = bindings.StringT
	apiListEntry = bindings.ListEntry
)

var (
	apiDateSetDays               = bindings.DateSetDays
	apiDateStructGetYear         = bindings.DateStructGetYear
	apiDateStructGetMonth        = bindings.DateStructGetMonth
	apiDateStructGetDay          = bindings.DateStructGetDay
	apiTimeGetMicros             = bindings.TimeGetMicros
	apiTimeSetMicros             = bindings.TimeSetMicros
	apiTimeStructGetHour         = bindings.TimeStructGetHour
	apiTimeStructGetMinute       = bindings.TimeStructGetMinute
	apiTimeStructGetSecond       = bindings.TimeStructGetSecond
	apiTimeStructGetMicros       = bindings.TimeStructGetMicros
	apiTimeTZStructGetTimeStruct = bindings.TimeTZStructGetTimeStruct
	apiTimeTZStructGetOffset     = bindings.TimeTZStructGetOffset
	apiTimestampGetMicros        = bindings.TimestampGetMicros
	apiTimestampSetMicros        = bindings.TimestampSetMicros
	apiIntervalGetMonths         = bindings.IntervalGetMonths
	apiIntervalSetMonths         = bindings.IntervalSetMonths
	apiIntervalGetDays           = bindings.IntervalGetDays
	apiIntervalSetDays           = bindings.IntervalSetDays
	apiIntervalGetMicros         = bindings.IntervalGetMicros
	apiIntervalSetMicros         = bindings.IntervalSetMicros
	apiHugeIntGetLower           = bindings.HugeIntGetLower
	apiHugeIntSetLower           = bindings.HugeIntSetLower
	apiHugeIntGetUpper           = bindings.HugeIntGetUpper
	apiHugeIntSetUpper           = bindings.HugeIntSetUpper
	apiListEntryGetOffset        = bindings.ListEntryGetOffset
	apiListEntrySetOffset        = bindings.ListEntrySetOffset
	apiListEntryGetLength        = bindings.ListEntryGetLength
	apiListEntrySetLength        = bindings.ListEntrySetLength
)

// ------------------------------------------------------------------ //
// Pointers
// ------------------------------------------------------------------ //

type (
	apiVector              = bindings.Vector
	apiResult              = bindings.Result
	apiDatabase            = bindings.Database
	apiConnection          = bindings.Connection
	apiPreparedStatement   = bindings.PreparedStatement
	apiExtractedStatements = bindings.ExtractedStatements
	apiPendingResult       = bindings.PendingResult
	apiAppender            = bindings.Appender
	apiConfig              = bindings.Config
	apiLogicalType         = bindings.LogicalType
	apiDataChunk           = bindings.DataChunk
	apiValue               = bindings.Value
	apiProfilingInfo       = bindings.ProfilingInfo
	apiFunctionInfo        = bindings.FunctionInfo
	apiScalarFunction      = bindings.ScalarFunction
	apiBindInfo            = bindings.BindInfo
	apiInitInfo            = bindings.InitInfo
	apiReplacementScanInfo = bindings.ReplacementScanInfo
)

// ------------------------------------------------------------------ //
// Functions
// ------------------------------------------------------------------ //

var (
	apiOpenExt                                 = bindings.OpenExt
	apiClose                                   = bindings.Close
	apiConnect                                 = bindings.Connect
	apiInterrupt                               = bindings.Interrupt
	apiDisconnect                              = bindings.Disconnect
	apiCreateConfig                            = bindings.CreateConfig
	apiSetConfig                               = bindings.SetConfig
	apiDestroyConfig                           = bindings.DestroyConfig
	apiDestroyResult                           = bindings.DestroyResult
	apiColumnName                              = bindings.ColumnName
	apiColumnType                              = bindings.ColumnType
	apiColumnLogicalType                       = bindings.ColumnLogicalType
	apiColumnCount                             = bindings.ColumnCount
	apiResultError                             = bindings.ResultError
	apiVectorSize                              = bindings.VectorSize
	apiStringTData                             = bindings.StringTData
	apiFromDate                                = bindings.FromDate
	apiCreateTimeTZ                            = bindings.CreateTimeTZ
	apiFromTimeTZ                              = bindings.FromTimeTZ
	apiDestroyPrepare                          = bindings.DestroyPrepare
	apiPrepareError                            = bindings.PrepareError
	apiNParams                                 = bindings.NParams
	apiParameterName                           = bindings.ParameterName
	apiParamType                               = bindings.ParamType
	apiPreparedStatementType                   = bindings.PreparedStatementType
	apiBindValue                               = bindings.BindValue
	apiBindBoolean                             = bindings.BindBoolean
	apiBindInt8                                = bindings.BindInt8
	apiBindInt16                               = bindings.BindInt16
	apiBindInt32                               = bindings.BindInt32
	apiBindInt64                               = bindings.BindInt64
	apiBindHugeInt                             = bindings.BindHugeInt
	apiBindUInt8                               = bindings.BindUInt8
	apiBindUInt16                              = bindings.BindUInt16
	apiBindUInt32                              = bindings.BindUInt32
	apiBindUInt64                              = bindings.BindUInt64
	apiBindFloat                               = bindings.BindFloat
	apiBindDouble                              = bindings.BindDouble
	apiBindDate                                = bindings.BindDate
	apiBindTime                                = bindings.BindTime
	apiBindTimestamp                           = bindings.BindTimestamp
	apiBindInterval                            = bindings.BindInterval
	apiBindVarchar                             = bindings.BindVarchar
	apiBindBlob                                = bindings.BindBlob
	apiBindNull                                = bindings.BindNull
	apiExtractStatements                       = bindings.ExtractStatements
	apiPrepareExtractedStatement               = bindings.PrepareExtractedStatement
	apiExtractStatementsError                  = bindings.ExtractStatementsError
	apiDestroyExtracted                        = bindings.DestroyExtracted
	apiPendingPrepared                         = bindings.PendingPrepared
	apiDestroyPending                          = bindings.DestroyPending
	apiPendingError                            = bindings.PendingError
	apiExecutePending                          = bindings.ExecutePending
	apiDestroyValue                            = bindings.DestroyValue
	apiCreateVarchar                           = bindings.CreateVarchar
	apiCreateInt64                             = bindings.CreateInt64
	apiCreateTimeTZValue                       = bindings.CreateTimeTZValue
	apiGetBool                                 = bindings.GetBool
	apiGetInt8                                 = bindings.GetInt8
	apiGetUInt8                                = bindings.GetUInt8
	apiGetInt16                                = bindings.GetInt16
	apiGetUInt16                               = bindings.GetUInt16
	apiGetInt32                                = bindings.GetInt32
	apiGetUInt32                               = bindings.GetUInt32
	apiGetInt64                                = bindings.GetInt64
	apiGetUInt64                               = bindings.GetUInt64
	apiGetHugeInt                              = bindings.GetHugeInt
	apiGetFloat                                = bindings.GetFloat
	apiGetDouble                               = bindings.GetDouble
	apiGetDate                                 = bindings.GetDate
	apiGetTime                                 = bindings.GetTime
	apiGetTimeTZ                               = bindings.GetTimeTZ
	apiGetTimestamp                            = bindings.GetTimestamp
	apiGetInterval                             = bindings.GetInterval
	apiGetVarchar                              = bindings.GetVarchar
	apiGetMapSize                              = bindings.GetMapSize
	apiGetMapKey                               = bindings.GetMapKey
	apiGetMapValue                             = bindings.GetMapValue
	apiLogicalTypeGetAlias                     = bindings.LogicalTypeGetAlias
	apiCreateListType                          = bindings.CreateListType
	apiCreateArrayType                         = bindings.CreateArrayType
	apiCreateMapType                           = bindings.CreateMapType
	apiCreateStructType                        = bindings.CreateStructType
	apiCreateEnumType                          = bindings.CreateEnumType
	apiCreateDecimalType                       = bindings.CreateDecimalType
	apiGetTypeId                               = bindings.GetTypeId
	apiDecimalWidth                            = bindings.DecimalWidth
	apiDecimalScale                            = bindings.DecimalScale
	apiDecimalInternalType                     = bindings.DecimalInternalType
	apiEnumInternalType                        = bindings.EnumInternalType
	apiEnumDictionarySize                      = bindings.EnumDictionarySize
	apiEnumDictionaryValue                     = bindings.EnumDictionaryValue
	apiListTypeChildType                       = bindings.ListTypeChildType
	apiArrayTypeChildType                      = bindings.ArrayTypeChildType
	apiArrayTypeArraySize                      = bindings.ArrayTypeArraySize
	apiMapTypeKeyType                          = bindings.MapTypeKeyType
	apiMapTypeValueType                        = bindings.MapTypeValueType
	apiStructTypeChildCount                    = bindings.StructTypeChildCount
	apiStructTypeChildName                     = bindings.StructTypeChildName
	apiStructTypeChildType                     = bindings.StructTypeChildType
	apiDestroyLogicalType                      = bindings.DestroyLogicalType
	apiCreateDataChunk                         = bindings.CreateDataChunk
	apiDestroyDataChunk                        = bindings.DestroyDataChunk
	apiDataChunkGetColumnCount                 = bindings.DataChunkGetColumnCount
	apiDataChunkGetVector                      = bindings.DataChunkGetVector
	apiDataChunkGetSize                        = bindings.DataChunkGetSize
	apiDataChunkSetSize                        = bindings.DataChunkSetSize
	apiVectorGetColumnType                     = bindings.VectorGetColumnType
	apiVectorGetData                           = bindings.VectorGetData
	apiVectorGetValidity                       = bindings.VectorGetValidity
	apiVectorEnsureValidityWritable            = bindings.VectorEnsureValidityWritable
	apiVectorAssignStringElement               = bindings.VectorAssignStringElement
	apiVectorAssignStringElementLen            = bindings.VectorAssignStringElementLen
	apiListVectorGetChild                      = bindings.ListVectorGetChild
	apiListVectorGetSize                       = bindings.ListVectorGetSize
	apiListVectorSetSize                       = bindings.ListVectorSetSize
	apiListVectorReserve                       = bindings.ListVectorReserve
	apiStructVectorGetChild                    = bindings.StructVectorGetChild
	apiArrayVectorGetChild                     = bindings.ArrayVectorGetChild
	apiValiditySetRowInvalid                   = bindings.ValiditySetRowInvalid
	apiCreateScalarFunction                    = bindings.CreateScalarFunction
	apiDestroyScalarFunction                   = bindings.DestroyScalarFunction
	apiScalarFunctionSetName                   = bindings.ScalarFunctionSetName
	apiScalarFunctionSetVarargs                = bindings.ScalarFunctionSetVarargs
	apiScalarFunctionSetSpecialHandling        = bindings.ScalarFunctionSetSpecialHandling
	apiScalarFunctionSetVolatile               = bindings.ScalarFunctionSetVolatile
	apiScalarFunctionAddParameter              = bindings.ScalarFunctionAddParameter
	apiScalarFunctionSetReturnType             = bindings.ScalarFunctionSetReturnType
	apiScalarFunctionSetExtraInfo              = bindings.ScalarFunctionSetExtraInfo
	apiScalarFunctionSetFunction               = bindings.ScalarFunctionSetFunction
	apiRegisterScalarFunction                  = bindings.RegisterScalarFunction
	apiScalarFunctionGetExtraInfo              = bindings.ScalarFunctionGetExtraInfo
	apiScalarFunctionSetError                  = bindings.ScalarFunctionSetError
	apiCreateScalarFunctionSet                 = bindings.CreateScalarFunctionSet
	apiDestroyScalarFunctionSet                = bindings.DestroyScalarFunctionSet
	apiAddScalarFunctionToSet                  = bindings.AddScalarFunctionToSet
	apiRegisterScalarFunctionSet               = bindings.RegisterScalarFunctionSet
	apiCreateTableFunction                     = bindings.CreateTableFunction
	apiDestroyTableFunction                    = bindings.DestroyTableFunction
	apiTableFunctionSetName                    = bindings.TableFunctionSetName
	apiTableFunctionAddParameter               = bindings.TableFunctionAddParameter
	apiTableFunctionAddNamedParameter          = bindings.TableFunctionAddNamedParameter
	apiTableFunctionSetExtraInfo               = bindings.TableFunctionSetExtraInfo
	apiTableFunctionSetBind                    = bindings.TableFunctionSetBind
	apiTableFunctionSetInit                    = bindings.TableFunctionSetInit
	apiTableFunctionSetLocalInit               = bindings.TableFunctionSetLocalInit
	apiTableFunctionSetFunction                = bindings.TableFunctionSetFunction
	apiTableFunctionSupportsProjectionPushdown = bindings.TableFunctionSupportsProjectionPushdown
	apiRegisterTableFunction                   = bindings.RegisterTableFunction
	apiBindGetExtraInfo                        = bindings.BindGetExtraInfo
	apiBindAddResultColumn                     = bindings.BindAddResultColumn
	apiBindGetParameter                        = bindings.BindGetParameter
	apiBindGetNamedParameter                   = bindings.BindGetNamedParameter
	apiBindSetBindData                         = bindings.BindSetBindData
	apiBindSetCardinality                      = bindings.BindSetCardinality
	apiBindSetError                            = bindings.BindSetError
	apiInitGetBindData                         = bindings.InitGetBindData
	apiInitSetInitData                         = bindings.InitSetInitData
	apiInitGetColumnCount                      = bindings.InitGetColumnCount
	apiInitGetColumnIndex                      = bindings.InitGetColumnIndex
	apiInitSetMaxThreads                       = bindings.InitSetMaxThreads
	apiFunctionGetBindData                     = bindings.FunctionGetBindData
	apiFunctionGetLocalInitData                = bindings.FunctionGetLocalInitData
	apiFunctionSetError                        = bindings.FunctionSetError
	apiAddReplacementScan                      = bindings.AddReplacementScan
	apiReplacementScanSetFunctionName          = bindings.ReplacementScanSetFunctionName
	apiReplacementScanAddParameter             = bindings.ReplacementScanAddParameter
	apiReplacementScanSetError                 = bindings.ReplacementScanSetError
	apiGetProfilingInfo                        = bindings.GetProfilingInfo
	apiProfilingInfoGetMetrics                 = bindings.ProfilingInfoGetMetrics
	apiProfilingInfoGetChildCount              = bindings.ProfilingInfoGetChildCount
	apiProfilingInfoGetChild                   = bindings.ProfilingInfoGetChild
	apiAppenderCreate                          = bindings.AppenderCreate
	apiAppenderColumnCount                     = bindings.AppenderColumnCount
	apiAppenderColumnType                      = bindings.AppenderColumnType
	apiAppenderError                           = bindings.AppenderError
	apiAppenderFlush                           = bindings.AppenderFlush
	apiAppenderDestroy                         = bindings.AppenderDestroy
	apiAppendDataChunk                         = bindings.AppendDataChunk
	apiResultGetChunk                          = bindings.ResultGetChunk
	apiResultChunkCount                        = bindings.ResultChunkCount
	apiValueInt64                              = bindings.ValueInt64
	apiValidityMaskValueIsValid                = bindings.ValidityMaskValueIsValid
)

func apiCreateLogicalType(t apiType) apiLogicalType {
	return bindings.CreateLogicalType(bindings.Type(t))
}

// ------------------------------------------------------------------ //
// Memory Safety
// ------------------------------------------------------------------ //

var VerifyAllocationCounters = bindings.VerifyAllocationCounters
