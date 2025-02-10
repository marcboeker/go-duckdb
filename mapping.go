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
	apiSuccess = apiState(bindings.Success)
	apiError   = apiState(bindings.Error)
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
	apiIdxT      = bindings.IdxT
	apiDate      = bindings.Date
	apiTimestamp = bindings.Timestamp
	apiInterval  = bindings.Interval
	apiHugeInt   = bindings.HugeInt
)

// ...

// ------------------------------------------------------------------ //
// Pointers
// ------------------------------------------------------------------ //

// type Column C.duckdb_column

type (
	apiVector = bindings.Vector
)

// type Result C.duckdb_result

type (
	apiDatabase            = bindings.Database
	apiConnection          = bindings.Connection
	apiPreparedStatement   = bindings.PreparedStatement
	apiExtractedStatements = bindings.ExtractedStatements
)

//type PendingResult *C.duckdb_pending_result

type (
	apiAppender = bindings.Appender
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
//table_function

type (
	apiBindInfo = bindings.BindInfo
)

//init_info
//cast_function
//replacement_scan_info

type (
	apiArrow       = bindings.Arrow
	apiArrowStream = bindings.ArrowStream
	apiArrowSchema = bindings.ArrowSchema
	apiArrowArray  = bindings.ArrowArray
)

// ------------------------------------------------------------------ //
// Functions
// ------------------------------------------------------------------ //

//#define duckdb_open                                    duckdb_ext_api.duckdb_open

var (
	apiOpenExt = bindings.OpenExt
	apiClose   = bindings.Close
	apiConnect = bindings.Connect
)

//#define duckdb_interrupt                               duckdb_ext_api.duckdb_interrupt
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
//#define duckdb_destroy_result                          duckdb_ext_api.duckdb_destroy_result
//#define duckdb_column_name                             duckdb_ext_api.duckdb_column_name
//#define duckdb_column_type                             duckdb_ext_api.duckdb_column_type
//#define duckdb_result_statement_type                   duckdb_ext_api.duckdb_result_statement_type
//#define duckdb_column_logical_type                     duckdb_ext_api.duckdb_column_logical_type
//#define duckdb_column_count                            duckdb_ext_api.duckdb_column_count
//#define duckdb_rows_changed                            duckdb_ext_api.duckdb_rows_changed
//#define duckdb_result_error                            duckdb_ext_api.duckdb_result_error
//#define duckdb_result_error_type                       duckdb_ext_api.duckdb_result_error_type
//#define duckdb_result_return_type                      duckdb_ext_api.duckdb_result_return_type
//#define duckdb_malloc                                  duckdb_ext_api.duckdb_malloc

var (
	apiFree = bindings.Free
)

//#define duckdb_vector_size                             duckdb_ext_api.duckdb_vector_size
//#define duckdb_string_is_inlined                       duckdb_ext_api.duckdb_string_is_inlined
//#define duckdb_string_t_length                         duckdb_ext_api.duckdb_string_t_length
//#define duckdb_string_t_data                           duckdb_ext_api.duckdb_string_t_data
//#define duckdb_from_date                               duckdb_ext_api.duckdb_from_date
//#define duckdb_to_date                                 duckdb_ext_api.duckdb_to_date
//#define duckdb_is_finite_date                          duckdb_ext_api.duckdb_is_finite_date
//#define duckdb_from_time                               duckdb_ext_api.duckdb_from_time
//#define duckdb_create_time_tz                          duckdb_ext_api.duckdb_create_time_tz
//#define duckdb_from_time_tz                            duckdb_ext_api.duckdb_from_time_tz
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
)

//#define duckdb_bind_value                              duckdb_ext_api.duckdb_bind_value
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
)

//#define duckdb_pending_prepared                        duckdb_ext_api.duckdb_pending_prepared
//#define duckdb_destroy_pending                         duckdb_ext_api.duckdb_destroy_pending
//#define duckdb_pending_error                           duckdb_ext_api.duckdb_pending_error
//#define duckdb_pending_execute_task                    duckdb_ext_api.duckdb_pending_execute_task
//#define duckdb_pending_execute_check_state             duckdb_ext_api.duckdb_pending_execute_check_state
//#define duckdb_execute_pending                         duckdb_ext_api.duckdb_execute_pending
//#define duckdb_pending_execution_is_finished           duckdb_ext_api.duckdb_pending_execution_is_finished

var (
	apiDestroyValue = bindings.DestroyValue
)

//#define duckdb_create_varchar                          duckdb_ext_api.duckdb_create_varchar
//#define duckdb_create_varchar_length                   duckdb_ext_api.duckdb_create_varchar_length
//#define duckdb_create_bool                             duckdb_ext_api.duckdb_create_bool
//#define duckdb_create_int8                             duckdb_ext_api.duckdb_create_int8
//#define duckdb_create_uint8                            duckdb_ext_api.duckdb_create_uint8
//#define duckdb_create_int16                            duckdb_ext_api.duckdb_create_int16
//#define duckdb_create_uint16                           duckdb_ext_api.duckdb_create_uint16
//#define duckdb_create_int32                            duckdb_ext_api.duckdb_create_int32
//#define duckdb_create_uint32                           duckdb_ext_api.duckdb_create_uint32
//#define duckdb_create_uint64                           duckdb_ext_api.duckdb_create_uint64
//#define duckdb_create_int64                            duckdb_ext_api.duckdb_create_int64
//#define duckdb_create_hugeint                          duckdb_ext_api.duckdb_create_hugeint
//#define duckdb_create_uhugeint                         duckdb_ext_api.duckdb_create_uhugeint
//#define duckdb_create_varint                           duckdb_ext_api.duckdb_create_varint
//#define duckdb_create_decimal                          duckdb_ext_api.duckdb_create_decimal
//#define duckdb_create_float                            duckdb_ext_api.duckdb_create_float
//#define duckdb_create_double                           duckdb_ext_api.duckdb_create_double
//#define duckdb_create_date                             duckdb_ext_api.duckdb_create_date
//#define duckdb_create_time                             duckdb_ext_api.duckdb_create_time
//#define duckdb_create_time_tz_value                    duckdb_ext_api.duckdb_create_time_tz_value
//#define duckdb_create_timestamp                        duckdb_ext_api.duckdb_create_timestamp
//#define duckdb_create_timestamp_tz                     duckdb_ext_api.duckdb_create_timestamp_tz
//#define duckdb_create_timestamp_s                      duckdb_ext_api.duckdb_create_timestamp_s
//#define duckdb_create_timestamp_ms                     duckdb_ext_api.duckdb_create_timestamp_ms
//#define duckdb_create_timestamp_ns                     duckdb_ext_api.duckdb_create_timestamp_ns
//#define duckdb_create_interval                         duckdb_ext_api.duckdb_create_interval
//#define duckdb_create_blob                             duckdb_ext_api.duckdb_create_blob
//#define duckdb_create_bit                              duckdb_ext_api.duckdb_create_bit
//#define duckdb_create_uuid                             duckdb_ext_api.duckdb_create_uuid
//#define duckdb_get_bool                                duckdb_ext_api.duckdb_get_bool
//#define duckdb_get_int8                                duckdb_ext_api.duckdb_get_int8
//#define duckdb_get_uint8                               duckdb_ext_api.duckdb_get_uint8
//#define duckdb_get_int16                               duckdb_ext_api.duckdb_get_int16
//#define duckdb_get_uint16                              duckdb_ext_api.duckdb_get_uint16
//#define duckdb_get_int32                               duckdb_ext_api.duckdb_get_int32
//#define duckdb_get_uint32                              duckdb_ext_api.duckdb_get_uint32
//#define duckdb_get_int64                               duckdb_ext_api.duckdb_get_int64
//#define duckdb_get_uint64                              duckdb_ext_api.duckdb_get_uint64
//#define duckdb_get_hugeint                             duckdb_ext_api.duckdb_get_hugeint
//#define duckdb_get_uhugeint                            duckdb_ext_api.duckdb_get_uhugeint
//#define duckdb_get_varint                              duckdb_ext_api.duckdb_get_varint
//#define duckdb_get_decimal                             duckdb_ext_api.duckdb_get_decimal
//#define duckdb_get_float                               duckdb_ext_api.duckdb_get_float
//#define duckdb_get_double                              duckdb_ext_api.duckdb_get_double
//#define duckdb_get_date                                duckdb_ext_api.duckdb_get_date
//#define duckdb_get_time                                duckdb_ext_api.duckdb_get_time
//#define duckdb_get_time_tz                             duckdb_ext_api.duckdb_get_time_tz
//#define duckdb_get_timestamp                           duckdb_ext_api.duckdb_get_timestamp
//#define duckdb_get_timestamp_tz                        duckdb_ext_api.duckdb_get_timestamp_tz
//#define duckdb_get_timestamp_s                         duckdb_ext_api.duckdb_get_timestamp_s
//#define duckdb_get_timestamp_ms                        duckdb_ext_api.duckdb_get_timestamp_ms
//#define duckdb_get_timestamp_ns                        duckdb_ext_api.duckdb_get_timestamp_ns
//#define duckdb_get_interval                            duckdb_ext_api.duckdb_get_interval
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

//#define duckdb_logical_type_get_alias                  duckdb_ext_api.duckdb_logical_type_get_alias
//#define duckdb_logical_type_set_alias                  duckdb_ext_api.duckdb_logical_type_set_alias

var (
	apiCreateListType  = bindings.CreateListType
	apiCreateArrayType = bindings.CreateArrayType
	apiCreateMapType   = bindings.CreateMapType
)

//#define duckdb_create_union_type                       duckdb_ext_api.duckdb_create_union_type

var (
	apiCreateStructType  = bindings.CreateStructType
	apiCreateEnumType    = bindings.CreateEnumType
	apiCreateDecimalType = bindings.CreateDecimalType
	apiGetTypeId         = bindings.GetTypeId
)

//#define duckdb_decimal_width                           duckdb_ext_api.duckdb_decimal_width
//#define duckdb_decimal_scale                           duckdb_ext_api.duckdb_decimal_scale
//#define duckdb_decimal_internal_type                   duckdb_ext_api.duckdb_decimal_internal_type
//#define duckdb_enum_internal_type                      duckdb_ext_api.duckdb_enum_internal_type
//#define duckdb_enum_dictionary_size                    duckdb_ext_api.duckdb_enum_dictionary_size
//#define duckdb_enum_dictionary_value                   duckdb_ext_api.duckdb_enum_dictionary_value
//#define duckdb_list_type_child_type                    duckdb_ext_api.duckdb_list_type_child_type
//#define duckdb_array_type_child_type                   duckdb_ext_api.duckdb_array_type_child_type
//#define duckdb_array_type_array_size                   duckdb_ext_api.duckdb_array_type_array_size
//#define duckdb_map_type_key_type                       duckdb_ext_api.duckdb_map_type_key_type
//#define duckdb_map_type_value_type                     duckdb_ext_api.duckdb_map_type_value_type
//#define duckdb_struct_type_child_count                 duckdb_ext_api.duckdb_struct_type_child_count
//#define duckdb_struct_type_child_name                  duckdb_ext_api.duckdb_struct_type_child_name
//#define duckdb_struct_type_child_type                  duckdb_ext_api.duckdb_struct_type_child_type
//#define duckdb_union_type_member_count                 duckdb_ext_api.duckdb_union_type_member_count
//#define duckdb_union_type_member_name                  duckdb_ext_api.duckdb_union_type_member_name
//#define duckdb_union_type_member_type                  duckdb_ext_api.duckdb_union_type_member_type

var (
	apiDestroyLogicalType = bindings.DestroyLogicalType
)

//#define duckdb_register_logical_type                   duckdb_ext_api.duckdb_register_logical_type
//#define duckdb_create_data_chunk                       duckdb_ext_api.duckdb_create_data_chunk
//#define duckdb_destroy_data_chunk                      duckdb_ext_api.duckdb_destroy_data_chunk
//#define duckdb_data_chunk_reset                        duckdb_ext_api.duckdb_data_chunk_reset
//#define duckdb_data_chunk_get_column_count             duckdb_ext_api.duckdb_data_chunk_get_column_count
//#define duckdb_data_chunk_get_vector                   duckdb_ext_api.duckdb_data_chunk_get_vector
//#define duckdb_data_chunk_get_size                     duckdb_ext_api.duckdb_data_chunk_get_size
//#define duckdb_data_chunk_set_size                     duckdb_ext_api.duckdb_data_chunk_set_size
//#define duckdb_vector_get_column_type                  duckdb_ext_api.duckdb_vector_get_column_type
//#define duckdb_vector_get_data                         duckdb_ext_api.duckdb_vector_get_data
//#define duckdb_vector_get_validity                     duckdb_ext_api.duckdb_vector_get_validity
//#define duckdb_vector_ensure_validity_writable         duckdb_ext_api.duckdb_vector_ensure_validity_writable
//#define duckdb_vector_assign_string_element            duckdb_ext_api.duckdb_vector_assign_string_element
//#define duckdb_vector_assign_string_element_len        duckdb_ext_api.duckdb_vector_assign_string_element_len
//#define duckdb_list_vector_get_child                   duckdb_ext_api.duckdb_list_vector_get_child
//#define duckdb_list_vector_get_size                    duckdb_ext_api.duckdb_list_vector_get_size
//#define duckdb_list_vector_set_size                    duckdb_ext_api.duckdb_list_vector_set_size
//#define duckdb_list_vector_reserve                     duckdb_ext_api.duckdb_list_vector_reserve
//#define duckdb_struct_vector_get_child                 duckdb_ext_api.duckdb_struct_vector_get_child
//#define duckdb_array_vector_get_child                  duckdb_ext_api.duckdb_array_vector_get_child
//#define duckdb_validity_row_is_valid                   duckdb_ext_api.duckdb_validity_row_is_valid
//#define duckdb_validity_set_row_validity               duckdb_ext_api.duckdb_validity_set_row_validity
//#define duckdb_validity_set_row_invalid                duckdb_ext_api.duckdb_validity_set_row_invalid
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
//#define duckdb_create_table_function                   duckdb_ext_api.duckdb_create_table_function
//#define duckdb_destroy_table_function                  duckdb_ext_api.duckdb_destroy_table_function
//#define duckdb_table_function_set_name                 duckdb_ext_api.duckdb_table_function_set_name
//#define duckdb_table_function_add_parameter            duckdb_ext_api.duckdb_table_function_add_parameter
//#define duckdb_table_function_add_named_parameter      duckdb_ext_api.duckdb_table_function_add_named_parameter
//#define duckdb_table_function_set_extra_info           duckdb_ext_api.duckdb_table_function_set_extra_info
//#define duckdb_table_function_set_bind                 duckdb_ext_api.duckdb_table_function_set_bind
//#define duckdb_table_function_set_init                 duckdb_ext_api.duckdb_table_function_set_init
//#define duckdb_table_function_set_local_init           duckdb_ext_api.duckdb_table_function_set_local_init
//#define duckdb_table_function_set_function             duckdb_ext_api.duckdb_table_function_set_function
//#define duckdb_table_function_supports_projection_pushdown                                                             \
//	duckdb_ext_api.duckdb_table_function_supports_projection_pushdown
//#define duckdb_register_table_function              duckdb_ext_api.duckdb_register_table_function
//#define duckdb_bind_get_extra_info                  duckdb_ext_api.duckdb_bind_get_extra_info
//#define duckdb_bind_add_result_column               duckdb_ext_api.duckdb_bind_add_result_column
//#define duckdb_bind_get_parameter_count             duckdb_ext_api.duckdb_bind_get_parameter_count
//#define duckdb_bind_get_parameter                   duckdb_ext_api.duckdb_bind_get_parameter
//#define duckdb_bind_get_named_parameter             duckdb_ext_api.duckdb_bind_get_named_parameter
//#define duckdb_bind_set_bind_data                   duckdb_ext_api.duckdb_bind_set_bind_data
//#define duckdb_bind_set_cardinality                 duckdb_ext_api.duckdb_bind_set_cardinality

var (
	apiBindSetError = bindings.BindSetError
)

//#define duckdb_init_get_extra_info                  duckdb_ext_api.duckdb_init_get_extra_info
//#define duckdb_init_get_bind_data                   duckdb_ext_api.duckdb_init_get_bind_data
//#define duckdb_init_set_init_data                   duckdb_ext_api.duckdb_init_set_init_data
//#define duckdb_init_get_column_count                duckdb_ext_api.duckdb_init_get_column_count
//#define duckdb_init_get_column_index                duckdb_ext_api.duckdb_init_get_column_index
//#define duckdb_init_set_max_threads                 duckdb_ext_api.duckdb_init_set_max_threads
//#define duckdb_init_set_error                       duckdb_ext_api.duckdb_init_set_error
//#define duckdb_function_get_extra_info              duckdb_ext_api.duckdb_function_get_extra_info
//#define duckdb_function_get_bind_data               duckdb_ext_api.duckdb_function_get_bind_data
//#define duckdb_function_get_init_data               duckdb_ext_api.duckdb_function_get_init_data
//#define duckdb_function_get_local_init_data         duckdb_ext_api.duckdb_function_get_local_init_data
//#define duckdb_function_set_error                   duckdb_ext_api.duckdb_function_set_error
//#define duckdb_add_replacement_scan                 duckdb_ext_api.duckdb_add_replacement_scan
//#define duckdb_replacement_scan_set_function_name   duckdb_ext_api.duckdb_replacement_scan_set_function_name
//#define duckdb_replacement_scan_add_parameter       duckdb_ext_api.duckdb_replacement_scan_add_parameter
//#define duckdb_replacement_scan_set_error           duckdb_ext_api.duckdb_replacement_scan_set_error

var (
	apiGetProfilingInfo = bindings.GetProfilingInfo
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
//#define duckdb_result_get_chunk           duckdb_ext_api.duckdb_result_get_chunk
//#define duckdb_result_is_streaming        duckdb_ext_api.duckdb_result_is_streaming
//#define duckdb_result_chunk_count         duckdb_ext_api.duckdb_result_chunk_count
//#define duckdb_value_boolean              duckdb_ext_api.duckdb_value_boolean
//#define duckdb_value_int8                 duckdb_ext_api.duckdb_value_int8
//#define duckdb_value_int16                duckdb_ext_api.duckdb_value_int16
//#define duckdb_value_int32                duckdb_ext_api.duckdb_value_int32
//#define duckdb_value_int64                duckdb_ext_api.duckdb_value_int64
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

// ------------------------------------------------------------------ //
// Helper
// ------------------------------------------------------------------ //

var (
	apiMallocLogicalTypeSlice = bindings.MallocLogicalTypeSlice
)
