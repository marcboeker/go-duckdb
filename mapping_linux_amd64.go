//go:build !duckdb_use_lib && !duckdb_use_static_lib

package duckdb

// ------------------------------------------------------------------ //
// Enums
// ------------------------------------------------------------------ //

type apiType = bindings.Type

const (
	apiTypeInvalid     = bindings.TypeInvalid
	apiTypeBoolean     = bindings.TypeBoolean
	apiTypeTinyInt     = bindings.TypeTinyInt
	apiTypeSmallInt    = bindings.TypeSmallInt
	apiTypeInteger     = bindings.TypeInteger
	apiTypeBigInt      = bindings.TypeBigInt
	apiTypeUTinyInt    = bindings.TypeUTinyInt
	apiTypeUSmallInt   = bindings.TypeUSmallInt
	apiTypeUInteger    = bindings.TypeUInteger
	apiTypeUBigInt     = bindings.TypeUBigInt
	apiTypeFloat       = bindings.TypeFloat
	apiTypeDouble      = bindings.TypeDouble
	apiTypeTimestamp   = bindings.TypeTimestamp
	apiTypeDate        = bindings.TypeDate
	apiTypeTime        = bindings.TypeTime
	apiTypeInterval    = bindings.TypeInterval
	apiTypeHugeInt     = bindings.TypeHugeInt
	apiTypeUHugeInt    = bindings.TypeUHugeInt
	apiTypeVarchar     = bindings.TypeVarchar
	apiTypeBlob        = bindings.TypeBlob
	apiTypeDecimal     = bindings.TypeDecimal
	apiTypeTimestampS  = bindings.TypeTimestampS
	apiTypeTimestampMS = bindings.TypeTimestampMS
	apiTypeTimestampNS = bindings.TypeTimestampNS
	apiTypeEnum        = bindings.TypeEnum
	apiTypeList        = bindings.TypeList
	apiTypeStruct      = bindings.TypeStruct
	apiTypeMap         = bindings.TypeMap
	apiTypeArray       = bindings.TypeArray
	apiTypeUUID        = bindings.TypeUUID
	apiTypeUnion       = bindings.TypeUnion
	apiTypeBit         = bindings.TypeBit
	apiTypeTimeTZ      = bindings.TypeTimeTZ
	apiTypeTimestampTZ = bindings.TypeTimestampTZ
	apiTypeAny         = bindings.TypeAny
	apiTypeVarInt      = bindings.TypeVarInt
	apiTypeSQLNull     = bindings.TypeSQLNull
)

type apiState = bindings.State

const (
	apiStateError = bindings.StateError
)

type apiStatementType = bindings.StatementType

const (
	apiStatementTypeInvalid     = bindings.StatementTypeInvalid
	apiStatementTypeSelect      = bindings.StatementTypeSelect
	apiStatementTypeInsert      = bindings.StatementTypeInsert
	apiStatementTypeUpdate      = bindings.StatementTypeUpdate
	apiStatementTypeExplain     = bindings.StatementTypeExplain
	apiStatementTypeDelete      = bindings.StatementTypeDelete
	apiStatementTypePrepare     = bindings.StatementTypePrepare
	apiStatementTypeCreate      = bindings.StatementTypeCreate
	apiStatementTypeExecute     = bindings.StatementTypeExecute
	apiStatementTypeAlter       = bindings.StatementTypeAlter
	apiStatementTypeTransaction = bindings.StatementTypeTransaction
	apiStatementTypeCopy        = bindings.StatementTypeCopy
	apiStatementTypeAnalyze     = bindings.StatementTypeAnalyze
	apiStatementTypeVariableSet = bindings.StatementTypeVariableSet
	apiStatementTypeCreateFunc  = bindings.StatementTypeCreateFunc
	apiStatementTypeDrop        = bindings.StatementTypeDrop
	apiStatementTypeExport      = bindings.StatementTypeExport
	apiStatementTypePragma      = bindings.StatementTypePragma
	apiStatementTypeVacuum      = bindings.StatementTypeVacuum
	apiStatementTypeCall        = bindings.StatementTypeCall
	apiStatementTypeSet         = bindings.StatementTypeSet
	apiStatementTypeLoad        = bindings.StatementTypeLoad
	apiStatementTypeRelation    = bindings.StatementTypeRelation
	apiStatementTypeExtension   = bindings.StatementTypeExtension
	apiStatementTypeLogicalPlan = bindings.StatementTypeLogicalPlan
	apiStatementTypeAttach      = bindings.StatementTypeAttach
	apiStatementTypeDetach      = bindings.StatementTypeDetach
	apiStatementTypeMulti       = bindings.StatementTypeMulti
)

// ------------------------------------------------------------------ //
// Types
// ------------------------------------------------------------------ //

type (
	apiIdxT      = bindings.IdxT
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
	apiCreateLogicalType                       = bindings.CreateLogicalType
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
