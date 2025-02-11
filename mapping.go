package duckdb

import "C"
import (
	bindings "github.com/duckdb/duckdb-go-bindings"
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
	apiStateSuccess = apiState(bindings.StateSuccess)
	apiStateError   = apiState(bindings.StateError)
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

// ...

// ------------------------------------------------------------------ //
// Types
// ------------------------------------------------------------------ //

type (
	apiDate         = bindings.Date
	apiDateStruct   = bindings.DateStruct
	apiTime         = bindings.Time
	apiTimeStruct   = bindings.TimeStruct
	apiTimeTZ       = bindings.TimeTZ
	apiTimeTZStruct = bindings.TimeTZStruct
	apiTimestamp    = bindings.Timestamp
	apiInterval     = bindings.Interval
	apiHugeInt      = bindings.HugeInt
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
)

// ...

// ------------------------------------------------------------------ //
// Pointers
// ------------------------------------------------------------------ //

// type Column C.duckdb_column

type (
	apiVector              = bindings.Vector
	apiResult              = bindings.Result
	apiDatabase            = bindings.Database
	apiConnection          = bindings.Connection
	apiPreparedStatement   = bindings.PreparedStatement
	apiExtractedStatements = bindings.ExtractedStatements
	apiPendingResult       = bindings.PendingResult
	apiAppender            = bindings.Appender
)

//type TableDescription *C.duckdb_table_description

type (
	apiConfig      = bindings.Config
	apiLogicalType = bindings.LogicalType
)

//create_type_info

type (
	apiDataChunk     = bindings.DataChunk
	apiValue         = bindings.Value
	apiProfilingInfo = bindings.ProfilingInfo
)

//profiling_info
// ?extension_info??

type (
	apiFunctionInfo      = bindings.FunctionInfo
	apiScalarFunction    = bindings.ScalarFunction
	apiScalarFunctionSet = bindings.ScalarFunctionSet
)

//aggregate_function
//aggregate_function_set
//aggregate_state

type (
	apiTableFunction = bindings.TableFunction
	apiBindInfo      = bindings.BindInfo
	apiInitInfo      = bindings.InitInfo
)

//cast_function

type (
	apiReplacementScanInfo = bindings.ReplacementScanInfo
	apiArrow               = bindings.Arrow
	apiArrowStream         = bindings.ArrowStream
	apiArrowSchema         = bindings.ArrowSchema
	apiArrowArray          = bindings.ArrowArray
)

// ------------------------------------------------------------------ //
// Functions
// ------------------------------------------------------------------ //

//#define duckdb_open                                    duckdb_ext_api.duckdb_open

var (
	apiOpenExt   = bindings.OpenExt
	apiClose     = bindings.Close
	apiConnect   = bindings.Connect
	apiInterrupt = bindings.Interrupt
)

//#define duckdb_query_progress                          duckdb_ext_api.duckdb_query_progress

var (
	apiDisconnect = bindings.Disconnect
)

//#define duckdb_library_version                         duckdb_ext_api.duckdb_library_version

var (
	apiCreateConfig = bindings.CreateConfig
)

//#define duckdb_config_count                            duckdb_ext_api.duckdb_config_count
//#define duckdb_get_config_flag                         duckdb_ext_api.duckdb_get_config_flag

var (
	apiSetConfig     = bindings.SetConfig
	apiDestroyConfig = bindings.DestroyConfig
)

//#define duckdb_query                                   duckdb_ext_api.duckdb_query

var (
	apiDestroyResult = bindings.DestroyResult
	apiColumnName    = bindings.ColumnName
	apiColumnType    = bindings.ColumnType
)

//#define duckdb_result_statement_type                   duckdb_ext_api.duckdb_result_statement_type

var (
	apiColumnLogicalType = bindings.ColumnLogicalType
	apiColumnCount       = bindings.ColumnCount
)

//#define duckdb_rows_changed                            duckdb_ext_api.duckdb_rows_changed

var (
	apiResultError = bindings.ResultError
)

//#define duckdb_result_error_type                       duckdb_ext_api.duckdb_result_error_type
//#define duckdb_result_return_type                      duckdb_ext_api.duckdb_result_return_type
//#define duckdb_malloc                                  duckdb_ext_api.duckdb_malloc

var (
	apiFree       = bindings.Free
	apiVectorSize = bindings.VectorSize
)

//#define duckdb_string_is_inlined                       duckdb_ext_api.duckdb_string_is_inlined
//#define duckdb_string_t_length                         duckdb_ext_api.duckdb_string_t_length
//#define duckdb_string_t_data                           duckdb_ext_api.duckdb_string_t_data

var (
	apiFromDate = bindings.FromDate
)

//#define duckdb_to_date                                 duckdb_ext_api.duckdb_to_date
//#define duckdb_is_finite_date                          duckdb_ext_api.duckdb_is_finite_date
//#define duckdb_from_time                               duckdb_ext_api.duckdb_from_time

var (
	apiCreateTimeTZ = bindings.CreateTimeTZ
	apiFromTimeTZ   = bindings.FromTimeTZ
)

//#define duckdb_to_time                                 duckdb_ext_api.duckdb_to_time
//#define duckdb_from_timestamp                          duckdb_ext_api.duckdb_from_timestamp
//#define duckdb_to_timestamp                            duckdb_ext_api.duckdb_to_timestamp
//#define duckdb_is_finite_timestamp                     duckdb_ext_api.duckdb_is_finite_timestamp
//#define duckdb_is_finite_timestamp_s                   duckdb_ext_api.duckdb_is_finite_timestamp_s
//#define duckdb_is_finite_timestamp_ms                  duckdb_ext_api.duckdb_is_finite_timestamp_ms
//#define duckdb_is_finite_timestamp_ns                  duckdb_ext_api.duckdb_is_finite_timestamp_ns
//#define duckdb_hugeint_to_double                       duckdb_ext_api.duckdb_hugeint_to_double
//#define duckdb_double_to_hugeint                       duckdb_ext_api.duckdb_double_to_hugeint
//#define duckdb_uhugeint_to_double                      duckdb_ext_api.duckdb_uhugeint_to_double
//#define duckdb_double_to_uhugeint                      duckdb_ext_api.duckdb_double_to_uhugeint
//#define duckdb_double_to_decimal                       duckdb_ext_api.duckdb_double_to_decimal
//#define duckdb_decimal_to_double                       duckdb_ext_api.duckdb_decimal_to_double
//#define duckdb_prepare                                 duckdb_ext_api.duckdb_prepare

var (
	apiDestroyPrepare = bindings.DestroyPrepare
	apiPrepareError   = bindings.PrepareError
	apiNParams        = bindings.NParams
	apiParameterName  = bindings.ParameterName
	apiParamType      = bindings.ParamType
)

//#define duckdb_param_logical_type                      duckdb_ext_api.duckdb_param_logical_type
//#define duckdb_clear_bindings                          duckdb_ext_api.duckdb_clear_bindings

var (
	apiPreparedStatementType = bindings.PreparedStatementType
	apiBindValue             = bindings.BindValue
)

//#define duckdb_bind_parameter_index                    duckdb_ext_api.duckdb_bind_parameter_index

var (
	apiBindBoolean = bindings.BindBoolean
	apiBindInt8    = bindings.BindInt8
	apiBindInt16   = bindings.BindInt16
	apiBindInt32   = bindings.BindInt32
	apiBindInt64   = bindings.BindInt64
	apiBindHugeInt = bindings.BindHugeInt
)

//#define duckdb_bind_uhugeint                           duckdb_ext_api.duckdb_bind_uhugeint

var (
	apiBindDecimal   = bindings.BindDecimal
	apiBindUInt8     = bindings.BindUInt8
	apiBindUInt16    = bindings.BindUInt16
	apiBindUInt32    = bindings.BindUInt32
	apiBindUInt64    = bindings.BindUInt64
	apiBindFloat     = bindings.BindFloat
	apiBindDouble    = bindings.BindDouble
	apiBindDate      = bindings.BindDate
	apiBindTime      = bindings.BindTime
	apiBindTimestamp = bindings.BindTimestamp
)

//#define duckdb_bind_timestamp_tz                       duckdb_ext_api.duckdb_bind_timestamp_tz

var (
	apiBindInterval = bindings.BindInterval
	apiBindVarchar  = bindings.BindVarchar
)

//#define duckdb_bind_varchar_length                     duckdb_ext_api.duckdb_bind_varchar_length

var (
	apiBindBlob = bindings.BindBlob
	apiBindNull = bindings.BindNull
)

//#define duckdb_execute_prepared                        duckdb_ext_api.duckdb_execute_prepared

var (
	apiExtractStatements         = bindings.ExtractStatements
	apiPrepareExtractedStatement = bindings.PrepareExtractedStatement
	apiExtractStatementsError    = bindings.ExtractStatementsError
	apiDestroyExtracted          = bindings.DestroyExtracted
	apiPendingPrepared           = bindings.PendingPrepared
	apiDestroyPending            = bindings.DestroyPending
	apiPendingError              = bindings.PendingError
)

//#define duckdb_pending_execute_task                    duckdb_ext_api.duckdb_pending_execute_task
//#define duckdb_pending_execute_check_state             duckdb_ext_api.duckdb_pending_execute_check_state

var (
	apiExecutePending = bindings.ExecutePending
)

//#define duckdb_pending_execution_is_finished           duckdb_ext_api.duckdb_pending_execution_is_finished

var (
	apiDestroyValue  = bindings.DestroyValue
	apiCreateVarchar = bindings.CreateVarchar
)

//#define duckdb_create_varchar_length                   duckdb_ext_api.duckdb_create_varchar_length
//#define duckdb_create_bool                             duckdb_ext_api.duckdb_create_bool
//#define duckdb_create_int8                             duckdb_ext_api.duckdb_create_int8
//#define duckdb_create_uint8                            duckdb_ext_api.duckdb_create_uint8
//#define duckdb_create_int16                            duckdb_ext_api.duckdb_create_int16
//#define duckdb_create_uint16                           duckdb_ext_api.duckdb_create_uint16
//#define duckdb_create_int32                            duckdb_ext_api.duckdb_create_int32
//#define duckdb_create_uint32                           duckdb_ext_api.duckdb_create_uint32
//#define duckdb_create_uint64                           duckdb_ext_api.duckdb_create_uint64

var (
	apiCreateInt64 = bindings.CreateInt64
)

//#define duckdb_create_hugeint                          duckdb_ext_api.duckdb_create_hugeint
//#define duckdb_create_uhugeint                         duckdb_ext_api.duckdb_create_uhugeint
//#define duckdb_create_varint                           duckdb_ext_api.duckdb_create_varint
//#define duckdb_create_decimal                          duckdb_ext_api.duckdb_create_decimal
//#define duckdb_create_float                            duckdb_ext_api.duckdb_create_float
//#define duckdb_create_double                           duckdb_ext_api.duckdb_create_double
//#define duckdb_create_date                             duckdb_ext_api.duckdb_create_date
//#define duckdb_create_time                             duckdb_ext_api.duckdb_create_time

var (
	apiCreateTimeTZValue = bindings.CreateTimeTZValue
)

//#define duckdb_create_timestamp                        duckdb_ext_api.duckdb_create_timestamp
//#define duckdb_create_timestamp_tz                     duckdb_ext_api.duckdb_create_timestamp_tz
//#define duckdb_create_timestamp_s                      duckdb_ext_api.duckdb_create_timestamp_s
//#define duckdb_create_timestamp_ms                     duckdb_ext_api.duckdb_create_timestamp_ms
//#define duckdb_create_timestamp_ns                     duckdb_ext_api.duckdb_create_timestamp_ns
//#define duckdb_create_interval                         duckdb_ext_api.duckdb_create_interval
//#define duckdb_create_blob                             duckdb_ext_api.duckdb_create_blob
//#define duckdb_create_bit                              duckdb_ext_api.duckdb_create_bit
//#define duckdb_create_uuid                             duckdb_ext_api.duckdb_create_uuid

var (
	apiGetBool    = bindings.GetBool
	apiGetInt8    = bindings.GetInt8
	apiGetUInt8   = bindings.GetUInt8
	apiGetInt16   = bindings.GetInt16
	apiGetUInt16  = bindings.GetUInt16
	apiGetInt32   = bindings.GetInt32
	apiGetUInt32  = bindings.GetUInt32
	apiGetInt64   = bindings.GetInt64
	apiGetUInt64  = bindings.GetUInt64
	apiGetHugeInt = bindings.GetHugeInt
)

//#define duckdb_get_uhugeint                            duckdb_ext_api.duckdb_get_uhugeint
//#define duckdb_get_varint                              duckdb_ext_api.duckdb_get_varint
//#define duckdb_get_decimal                             duckdb_ext_api.duckdb_get_decimal

var (
	apiGetFloat     = bindings.GetFloat
	apiGetDouble    = bindings.GetDouble
	apiGetDate      = bindings.GetDate
	apiGetTime      = bindings.GetTime
	apiGetTimeTZ    = bindings.GetTimeTZ
	apiGetTimestamp = bindings.GetTimestamp
)

//#define duckdb_get_timestamp_tz                        duckdb_ext_api.duckdb_get_timestamp_tz
//#define duckdb_get_timestamp_s                         duckdb_ext_api.duckdb_get_timestamp_s
//#define duckdb_get_timestamp_ms                        duckdb_ext_api.duckdb_get_timestamp_ms
//#define duckdb_get_timestamp_ns                        duckdb_ext_api.duckdb_get_timestamp_ns

var (
	apiGetInterval = bindings.GetInterval
)

//#define duckdb_get_value_type                          duckdb_ext_api.duckdb_get_value_type
//#define duckdb_get_blob                                duckdb_ext_api.duckdb_get_blob
//#define duckdb_get_bit                                 duckdb_ext_api.duckdb_get_bit
//#define duckdb_get_uuid                                duckdb_ext_api.duckdb_get_uuid

var (
	apiGetVarchar = bindings.GetVarchar
)

//#define duckdb_create_struct_value                     duckdb_ext_api.duckdb_create_struct_value
//#define duckdb_create_list_value                       duckdb_ext_api.duckdb_create_list_value
//#define duckdb_create_array_value                      duckdb_ext_api.duckdb_create_array_value

var (
	apiGetMapSize  = bindings.GetMapSize
	apiGetMapKey   = bindings.GetMapKey
	apiGetMapValue = bindings.GetMapValue
)

//#define duckdb_is_null_value                           duckdb_ext_api.duckdb_is_null_value
//#define duckdb_create_null_value                       duckdb_ext_api.duckdb_create_null_value
//#define duckdb_get_list_size                           duckdb_ext_api.duckdb_get_list_size
//#define duckdb_get_list_child                          duckdb_ext_api.duckdb_get_list_child
//#define duckdb_create_enum_value                       duckdb_ext_api.duckdb_create_enum_value
//#define duckdb_get_enum_value                          duckdb_ext_api.duckdb_get_enum_value
//#define duckdb_get_struct_child                        duckdb_ext_api.duckdb_get_struct_child

func apiCreateLogicalType(t apiType) apiLogicalType {
	return bindings.CreateLogicalType(bindings.Type(t))
}

var (
	apiLogicalTypeGetAlias = bindings.LogicalTypeGetAlias
)

//#define duckdb_logical_type_set_alias                  duckdb_ext_api.duckdb_logical_type_set_alias

var (
	apiCreateListType  = bindings.CreateListType
	apiCreateArrayType = bindings.CreateArrayType
	apiCreateMapType   = bindings.CreateMapType
)

//#define duckdb_create_union_type                       duckdb_ext_api.duckdb_create_union_type

var (
	apiCreateStructType     = bindings.CreateStructType
	apiCreateEnumType       = bindings.CreateEnumType
	apiCreateDecimalType    = bindings.CreateDecimalType
	apiGetTypeId            = bindings.GetTypeId
	apiDecimalWidth         = bindings.DecimalWidth
	apiDecimalScale         = bindings.DecimalScale
	apiDecimalInternalType  = bindings.DecimalInternalType
	apiEnumInternalType     = bindings.EnumInternalType
	apiEnumDictionarySize   = bindings.EnumDictionarySize
	apiEnumDictionaryValue  = bindings.EnumDictionaryValue
	apiListTypeChildType    = bindings.ListTypeChildType
	apiArrayTypeChildType   = bindings.ArrayTypeChildType
	apiArrayTypeArraySize   = bindings.ArrayTypeArraySize
	apiMapTypeKeyType       = bindings.MapTypeKeyType
	apiMapTypeValueType     = bindings.MapTypeValueType
	apiStructTypeChildCount = bindings.StructTypeChildCount
	apiStructTypeChildName  = bindings.StructTypeChildName
	apiStructTypeChildType  = bindings.StructTypeChildType
)

//#define duckdb_union_type_member_count                 duckdb_ext_api.duckdb_union_type_member_count
//#define duckdb_union_type_member_name                  duckdb_ext_api.duckdb_union_type_member_name
//#define duckdb_union_type_member_type                  duckdb_ext_api.duckdb_union_type_member_type

var (
	apiDestroyLogicalType = bindings.DestroyLogicalType
)

//#define duckdb_register_logical_type                   duckdb_ext_api.duckdb_register_logical_type

var (
	apiCreateDataChunk  = bindings.CreateDataChunk
	apiDestroyDataChunk = bindings.DestroyDataChunk
)

//#define duckdb_data_chunk_reset                        duckdb_ext_api.duckdb_data_chunk_reset

var (
	apiDataChunkGetColumnCount      = bindings.DataChunkGetColumnCount
	apiDataChunkGetVector           = bindings.DataChunkGetVector
	apiDataChunkGetSize             = bindings.DataChunkGetSize
	apiDataChunkSetSize             = bindings.DataChunkSetSize
	apiVectorGetColumnType          = bindings.VectorGetColumnType
	apiVectorGetData                = bindings.VectorGetData
	apiVectorGetValidity            = bindings.VectorGetValidity
	apiVectorEnsureValidityWritable = bindings.VectorEnsureValidityWritable
)

//#define duckdb_vector_assign_string_element            duckdb_ext_api.duckdb_vector_assign_string_element
//#define duckdb_vector_assign_string_element_len        duckdb_ext_api.duckdb_vector_assign_string_element_len

var (
	apiListVectorGetChild   = bindings.ListVectorGetChild
	apiListVectorGetSize    = bindings.ListVectorGetSize
	apiListVectorSetSize    = bindings.ListVectorSetSize
	apiListVectorReserve    = bindings.ListVectorReserve
	apiStructVectorGetChild = bindings.StructVectorGetChild
	apiArrayVectorGetChild  = bindings.ArrayVectorGetChild
)

//#define duckdb_validity_row_is_valid                   duckdb_ext_api.duckdb_validity_row_is_valid
//#define duckdb_validity_set_row_validity               duckdb_ext_api.duckdb_validity_set_row_validity

var (
	apiValiditySetRowInvalid = bindings.ValiditySetRowInvalid
)

//#define duckdb_validity_set_row_valid                  duckdb_ext_api.duckdb_validity_set_row_valid

var (
	apiCreateScalarFunction             = bindings.CreateScalarFunction
	apiDestroyScalarFunction            = bindings.DestroyScalarFunction
	apiScalarFunctionSetName            = bindings.ScalarFunctionSetName
	apiScalarFunctionSetVarargs         = bindings.ScalarFunctionSetVarargs
	apiScalarFunctionSetSpecialHandling = bindings.ScalarFunctionSetSpecialHandling
	apiScalarFunctionSetVolatile        = bindings.ScalarFunctionSetVolatile
	apiScalarFunctionAddParameter       = bindings.ScalarFunctionAddParameter
	apiScalarFunctionSetReturnType      = bindings.ScalarFunctionSetReturnType
	apiScalarFunctionSetExtraInfo       = bindings.ScalarFunctionSetExtraInfo
	apiScalarFunctionSetFunction        = bindings.ScalarFunctionSetFunction
	apiRegisterScalarFunction           = bindings.RegisterScalarFunction
	apiScalarFunctionGetExtraInfo       = bindings.ScalarFunctionGetExtraInfo
	apiScalarFunctionSetError           = bindings.ScalarFunctionSetError
	apiCreateScalarFunctionSet          = bindings.CreateScalarFunctionSet
	apiDestroyScalarFunctionSet         = bindings.DestroyScalarFunctionSet
	apiAddScalarFunctionToSet           = bindings.AddScalarFunctionToSet
	apiRegisterScalarFunctionSet        = bindings.RegisterScalarFunctionSet
)

//#define duckdb_create_aggregate_function               duckdb_ext_api.duckdb_create_aggregate_function
//#define duckdb_destroy_aggregate_function              duckdb_ext_api.duckdb_destroy_aggregate_function
//#define duckdb_aggregate_function_set_name             duckdb_ext_api.duckdb_aggregate_function_set_name
//#define duckdb_aggregate_function_add_parameter        duckdb_ext_api.duckdb_aggregate_function_add_parameter
//#define duckdb_aggregate_function_set_return_type      duckdb_ext_api.duckdb_aggregate_function_set_return_type
//#define duckdb_aggregate_function_set_functions        duckdb_ext_api.duckdb_aggregate_function_set_functions
//#define duckdb_aggregate_function_set_destructor       duckdb_ext_api.duckdb_aggregate_function_set_destructor
//#define duckdb_register_aggregate_function             duckdb_ext_api.duckdb_register_aggregate_function
//#define duckdb_aggregate_function_set_special_handling duckdb_ext_api.duckdb_aggregate_function_set_special_handling
//#define duckdb_aggregate_function_set_extra_info       duckdb_ext_api.duckdb_aggregate_function_set_extra_info
//#define duckdb_aggregate_function_get_extra_info       duckdb_ext_api.duckdb_aggregate_function_get_extra_info
//#define duckdb_aggregate_function_set_error            duckdb_ext_api.duckdb_aggregate_function_set_error
//#define duckdb_create_aggregate_function_set           duckdb_ext_api.duckdb_create_aggregate_function_set
//#define duckdb_destroy_aggregate_function_set          duckdb_ext_api.duckdb_destroy_aggregate_function_set
//#define duckdb_add_aggregate_function_to_set           duckdb_ext_api.duckdb_add_aggregate_function_to_set
//#define duckdb_register_aggregate_function_set         duckdb_ext_api.duckdb_register_aggregate_function_set

var (
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
)

//#define duckdb_bind_get_parameter_count             duckdb_ext_api.duckdb_bind_get_parameter_count

var (
	apiBindGetParameter      = bindings.BindGetParameter
	apiBindGetNamedParameter = bindings.BindGetNamedParameter
	apiBindSetBindData       = bindings.BindSetBindData
	apiBindSetCardinality    = bindings.BindSetCardinality
	apiBindSetError          = bindings.BindSetError
)

//#define duckdb_init_get_extra_info                  duckdb_ext_api.duckdb_init_get_extra_info

var (
	apiInitGetBindData    = bindings.InitGetBindData
	apiInitSetInitData    = bindings.InitSetInitData
	apiInitGetColumnCount = bindings.InitGetColumnCount
	apiInitGetColumnIndex = bindings.InitGetColumnIndex
	apiInitSetMaxThreads  = bindings.InitSetMaxThreads
)

//#define duckdb_init_set_error                       duckdb_ext_api.duckdb_init_set_error
//#define duckdb_function_get_extra_info              duckdb_ext_api.duckdb_function_get_extra_info

var (
	apiFunctionGetBindData = bindings.FunctionGetBindData
)

//#define duckdb_function_get_init_data               duckdb_ext_api.duckdb_function_get_init_data

var (
	apiFunctionGetLocalInitData       = bindings.FunctionGetLocalInitData
	apiFunctionSetError               = bindings.FunctionSetError
	apiAddReplacementScan             = bindings.AddReplacementScan
	apiReplacementScanSetFunctionName = bindings.ReplacementScanSetFunctionName
	apiReplacementScanAddParameter    = bindings.ReplacementScanAddParameter
	apiReplacementScanSetError        = bindings.ReplacementScanSetError
	apiGetProfilingInfo               = bindings.GetProfilingInfo
)

//#define duckdb_profiling_info_get_value             duckdb_ext_api.duckdb_profiling_info_get_value

var (
	apiProfilingInfoGetMetrics    = bindings.ProfilingInfoGetMetrics
	apiProfilingInfoGetChildCount = bindings.ProfilingInfoGetChildCount
	apiProfilingInfoGetChild      = bindings.ProfilingInfoGetChild
	apiAppenderCreate             = bindings.AppenderCreate
)

//#define duckdb_appender_create_ext                  duckdb_ext_api.duckdb_appender_create_ext

var (
	apiAppenderColumnCount = bindings.AppenderColumnCount
	apiAppenderColumnType  = bindings.AppenderColumnType
	apiAppenderError       = bindings.AppenderError
	apiAppenderFlush       = bindings.AppenderFlush
	apiAppenderClose       = bindings.AppenderClose
	apiAppenderDestroy     = bindings.AppenderDestroy
)

//#define duckdb_appender_add_column                  duckdb_ext_api.duckdb_appender_add_column
//#define duckdb_appender_clear_columns               duckdb_ext_api.duckdb_appender_clear_columns
//#define duckdb_appender_begin_row                   duckdb_ext_api.duckdb_appender_begin_row
//#define duckdb_appender_end_row                     duckdb_ext_api.duckdb_appender_end_row
//#define duckdb_append_default                       duckdb_ext_api.duckdb_append_default
//#define duckdb_append_bool                          duckdb_ext_api.duckdb_append_bool
//#define duckdb_append_int8                          duckdb_ext_api.duckdb_append_int8
//#define duckdb_append_int16                         duckdb_ext_api.duckdb_append_int16
//#define duckdb_append_int32                         duckdb_ext_api.duckdb_append_int32
//#define duckdb_append_int64                         duckdb_ext_api.duckdb_append_int64
//#define duckdb_append_hugeint                       duckdb_ext_api.duckdb_append_hugeint
//#define duckdb_append_uint8                         duckdb_ext_api.duckdb_append_uint8
//#define duckdb_append_uint16                        duckdb_ext_api.duckdb_append_uint16
//#define duckdb_append_uint32                        duckdb_ext_api.duckdb_append_uint32
//#define duckdb_append_uint64                        duckdb_ext_api.duckdb_append_uint64
//#define duckdb_append_uhugeint                      duckdb_ext_api.duckdb_append_uhugeint
//#define duckdb_append_float                         duckdb_ext_api.duckdb_append_float
//#define duckdb_append_double                        duckdb_ext_api.duckdb_append_double
//#define duckdb_append_date                          duckdb_ext_api.duckdb_append_date
//#define duckdb_append_time                          duckdb_ext_api.duckdb_append_time
//#define duckdb_append_timestamp                     duckdb_ext_api.duckdb_append_timestamp
//#define duckdb_append_interval                      duckdb_ext_api.duckdb_append_interval
//#define duckdb_append_varchar                       duckdb_ext_api.duckdb_append_varchar
//#define duckdb_append_varchar_length                duckdb_ext_api.duckdb_append_varchar_length
//#define duckdb_append_blob                          duckdb_ext_api.duckdb_append_blob
//#define duckdb_append_null                          duckdb_ext_api.duckdb_append_null
//#define duckdb_append_value                         duckdb_ext_api.duckdb_append_value

var (
	apiAppendDataChunk = bindings.AppendDataChunk
)

//#define duckdb_table_description_create             duckdb_ext_api.duckdb_table_description_create
//#define duckdb_table_description_create_ext         duckdb_ext_api.duckdb_table_description_create_ext
//#define duckdb_table_description_destroy            duckdb_ext_api.duckdb_table_description_destroy
//#define duckdb_table_description_error              duckdb_ext_api.duckdb_table_description_error
//#define duckdb_column_has_default                   duckdb_ext_api.duckdb_column_has_default
//#define duckdb_table_description_get_column_name    duckdb_ext_api.duckdb_table_description_get_column_name
//#define duckdb_execute_tasks                        duckdb_ext_api.duckdb_execute_tasks
//#define duckdb_create_task_state                    duckdb_ext_api.duckdb_create_task_state
//#define duckdb_execute_tasks_state                  duckdb_ext_api.duckdb_execute_tasks_state
//#define duckdb_execute_n_tasks_state                duckdb_ext_api.duckdb_execute_n_tasks_state
//#define duckdb_finish_execution                     duckdb_ext_api.duckdb_finish_execution
//#define duckdb_task_state_is_finished               duckdb_ext_api.duckdb_task_state_is_finished
//#define duckdb_destroy_task_state                   duckdb_ext_api.duckdb_destroy_task_state
//#define duckdb_execution_is_finished                duckdb_ext_api.duckdb_execution_is_finished
//#define duckdb_fetch_chunk                          duckdb_ext_api.duckdb_fetch_chunk
//#define duckdb_create_cast_function                 duckdb_ext_api.duckdb_create_cast_function
//#define duckdb_cast_function_set_source_type        duckdb_ext_api.duckdb_cast_function_set_source_type
//#define duckdb_cast_function_set_target_type        duckdb_ext_api.duckdb_cast_function_set_target_type
//#define duckdb_cast_function_set_implicit_cast_cost duckdb_ext_api.duckdb_cast_function_set_implicit_cast_cost
//#define duckdb_cast_function_set_function           duckdb_ext_api.duckdb_cast_function_set_function
//#define duckdb_cast_function_set_extra_info         duckdb_ext_api.duckdb_cast_function_set_extra_info
//#define duckdb_cast_function_get_extra_info         duckdb_ext_api.duckdb_cast_function_get_extra_info
//#define duckdb_cast_function_get_cast_mode          duckdb_ext_api.duckdb_cast_function_get_cast_mode
//#define duckdb_cast_function_set_error              duckdb_ext_api.duckdb_cast_function_set_error
//#define duckdb_cast_function_set_row_error          duckdb_ext_api.duckdb_cast_function_set_row_error
//#define duckdb_register_cast_function               duckdb_ext_api.duckdb_register_cast_function
//#define duckdb_destroy_cast_function                duckdb_ext_api.duckdb_destroy_cast_function
//
//// Version unstable_deprecated
//#define duckdb_row_count                  duckdb_ext_api.duckdb_row_count
//#define duckdb_column_data                duckdb_ext_api.duckdb_column_data
//#define duckdb_nullmask_data              duckdb_ext_api.duckdb_nullmask_data

var (
	apiResultGetChunk = bindings.ResultGetChunk
)

//#define duckdb_result_is_streaming        duckdb_ext_api.duckdb_result_is_streaming

var (
	apiResultChunkCount = bindings.ResultChunkCount
)

//#define duckdb_value_boolean              duckdb_ext_api.duckdb_value_boolean
//#define duckdb_value_int8                 duckdb_ext_api.duckdb_value_int8
//#define duckdb_value_int16                duckdb_ext_api.duckdb_value_int16
//#define duckdb_value_int32                duckdb_ext_api.duckdb_value_int32
//#define duckdb_value_int64                duckdb_ext_api.duckdb_value_int64

var (
	apiValueInt64 = bindings.ValueInt64
)

//#define duckdb_value_hugeint              duckdb_ext_api.duckdb_value_hugeint
//#define duckdb_value_uhugeint             duckdb_ext_api.duckdb_value_uhugeint
//#define duckdb_value_decimal              duckdb_ext_api.duckdb_value_decimal
//#define duckdb_value_uint8                duckdb_ext_api.duckdb_value_uint8
//#define duckdb_value_uint16               duckdb_ext_api.duckdb_value_uint16
//#define duckdb_value_uint32               duckdb_ext_api.duckdb_value_uint32
//#define duckdb_value_uint64               duckdb_ext_api.duckdb_value_uint64
//#define duckdb_value_float                duckdb_ext_api.duckdb_value_float
//#define duckdb_value_double               duckdb_ext_api.duckdb_value_double
//#define duckdb_value_date                 duckdb_ext_api.duckdb_value_date
//#define duckdb_value_time                 duckdb_ext_api.duckdb_value_time
//#define duckdb_value_timestamp            duckdb_ext_api.duckdb_value_timestamp
//#define duckdb_value_interval             duckdb_ext_api.duckdb_value_interval
//#define duckdb_value_varchar              duckdb_ext_api.duckdb_value_varchar
//#define duckdb_value_string               duckdb_ext_api.duckdb_value_string
//#define duckdb_value_varchar_internal     duckdb_ext_api.duckdb_value_varchar_internal
//#define duckdb_value_string_internal      duckdb_ext_api.duckdb_value_string_internal
//#define duckdb_value_blob                 duckdb_ext_api.duckdb_value_blob
//#define duckdb_value_is_null              duckdb_ext_api.duckdb_value_is_null
//#define duckdb_execute_prepared_streaming duckdb_ext_api.duckdb_execute_prepared_streaming
//#define duckdb_pending_prepared_streaming duckdb_ext_api.duckdb_pending_prepared_streaming
//#define duckdb_query_arrow                duckdb_ext_api.duckdb_query_arrow

var (
	apiQueryArrowSchema = bindings.QueryArrowSchema
)

//#define duckdb_prepared_arrow_schema      duckdb_ext_api.duckdb_prepared_arrow_schema
//#define duckdb_result_arrow_array         duckdb_ext_api.duckdb_result_arrow_array

var (
	apiQueryArrowArray = bindings.QueryArrowArray
)

//#define duckdb_arrow_column_count         duckdb_ext_api.duckdb_arrow_column_count

var (
	apiArrowRowCount = bindings.ArrowRowCount
)

//#define duckdb_arrow_rows_changed         duckdb_ext_api.duckdb_arrow_rows_changed

var (
	apiQueryArrowError = bindings.QueryArrowError
	apiDestroyArrow    = bindings.DestroyArrow
)

//#define duckdb_destroy_arrow_stream       duckdb_ext_api.duckdb_destroy_arrow_stream

var (
	apiExecutePreparedArrow = bindings.ExecutePreparedArrow
	apiArrowScan            = bindings.ArrowScan
)

//#define duckdb_arrow_array_scan           duckdb_ext_api.duckdb_arrow_array_scan
//#define duckdb_stream_fetch_chunk         duckdb_ext_api.duckdb_stream_fetch_chunk

//// Version unstable_instance_cache
//#define duckdb_create_instance_cache    duckdb_ext_api.duckdb_create_instance_cache
//#define duckdb_get_or_create_from_cache duckdb_ext_api.duckdb_get_or_create_from_cache
//#define duckdb_destroy_instance_cache   duckdb_ext_api.duckdb_destroy_instance_cache

//// Version unstable_new_append_functions
//#define duckdb_append_default_to_chunk duckdb_ext_api.duckdb_append_default_to_chunk
