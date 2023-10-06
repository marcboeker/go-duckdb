#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif










namespace duckdb {

// current_query
static void CurrentQueryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(state.GetContext().GetCurrentQuery());
	result.Reference(val);
}

// current_schema
static void CurrentSchemaFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(ClientData::Get(state.GetContext()).catalog_search_path->GetDefault().schema);
	result.Reference(val);
}

// current_database
static void CurrentDatabaseFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(DatabaseManager::GetDefaultDatabase(state.GetContext()));
	result.Reference(val);
}

// current_schemas
static void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	if (!input.AllConstant()) {
		throw NotImplementedException("current_schemas requires a constant input");
	}
	if (ConstantVector::IsNull(input.data[0])) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}
	auto implicit_schemas = *ConstantVector::GetData<bool>(input.data[0]);
	vector<Value> schema_list;
	auto &catalog_search_path = ClientData::Get(state.GetContext()).catalog_search_path;
	auto &search_path = implicit_schemas ? catalog_search_path->Get() : catalog_search_path->GetSetPaths();
	std::transform(search_path.begin(), search_path.end(), std::back_inserter(schema_list),
	               [](const CatalogSearchEntry &s) -> Value { return Value(s.schema); });

	auto val = Value::LIST(LogicalType::VARCHAR, schema_list);
	result.Reference(val);
}

// in_search_path
static void InSearchPathFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &search_path = ClientData::Get(context).catalog_search_path;
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    input.data[0], input.data[1], result, input.size(), [&](string_t db_name, string_t schema_name) {
		    return search_path->SchemaInSearchPath(context, db_name.GetString(), schema_name.GetString());
	    });
}

// txid_current
static void TransactionIdCurrent(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction = DuckTransaction::Get(context, catalog);
	auto val = Value::BIGINT(transaction.start_time);
	result.Reference(val);
}

// version
static void VersionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto val = Value(DuckDB::LibraryVersion());
	result.Reference(val);
}

ScalarFunction CurrentQueryFun::GetFunction() {
	ScalarFunction current_query({}, LogicalType::VARCHAR, CurrentQueryFunction);
	current_query.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_query;
}

ScalarFunction CurrentSchemaFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, CurrentSchemaFunction);
}

ScalarFunction CurrentDatabaseFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, CurrentDatabaseFunction);
}

ScalarFunction CurrentSchemasFun::GetFunction() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	return ScalarFunction({LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction);
}

ScalarFunction InSearchPathFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, InSearchPathFunction);
}

ScalarFunction CurrentTransactionIdFun::GetFunction() {
	return ScalarFunction({}, LogicalType::BIGINT, TransactionIdCurrent);
}

ScalarFunction VersionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, VersionFunction);
}

} // namespace duckdb


namespace duckdb {

static void TypeOfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Value v(args.data[0].GetType().ToString());
	result.Reference(v);
}

ScalarFunction TypeOfFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, TypeOfFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb









namespace duckdb {

struct ListSliceBindData : public FunctionData {
	ListSliceBindData(const LogicalType &return_type_p, bool begin_is_empty_p, bool end_is_empty_p)
	    : return_type(return_type_p), begin_is_empty(begin_is_empty_p), end_is_empty(end_is_empty_p) {
	}
	~ListSliceBindData() override;

	LogicalType return_type;

	bool begin_is_empty;
	bool end_is_empty;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListSliceBindData::~ListSliceBindData() {
}

bool ListSliceBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListSliceBindData>();
	return return_type == other.return_type && begin_is_empty == other.begin_is_empty &&
	       end_is_empty == other.end_is_empty;
}

unique_ptr<FunctionData> ListSliceBindData::Copy() const {
	return make_uniq<ListSliceBindData>(return_type, begin_is_empty, end_is_empty);
}

template <typename INDEX_TYPE>
static int CalculateSliceLength(idx_t begin, idx_t end, INDEX_TYPE step, bool svalid) {
	if (step < 0) {
		step = abs(step);
	}
	if (step == 0 && svalid) {
		throw InvalidInputException("Slice step cannot be zero");
	}
	if (step == 1) {
		return end - begin;
	} else if (static_cast<idx_t>(step) >= (end - begin)) {
		return 1;
	}
	if ((end - begin) % step != 0) {
		return (end - begin) / step + 1;
	}
	return (end - begin) / step;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INDEX_TYPE ValueLength(const INPUT_TYPE &value) {
	return 0;
}

template <>
int64_t ValueLength(const list_entry_t &value) {
	return value.length;
}

template <>
int64_t ValueLength(const string_t &value) {
	return LengthFun::Length<string_t, int64_t>(value);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ClampIndex(INDEX_TYPE &index, const INPUT_TYPE &value, const INDEX_TYPE length, bool is_min) {
	if (index < 0) {
		index = (!is_min) ? index + 1 : index;
		index = length + index;
		return;
	} else if (index > length) {
		index = length;
	}
	return;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static bool ClampSlice(const INPUT_TYPE &value, INDEX_TYPE &begin, INDEX_TYPE &end) {
	// Clamp offsets
	begin = (begin != 0 && begin != (INDEX_TYPE)NumericLimits<int64_t>::Minimum()) ? begin - 1 : begin;

	bool is_min = false;
	if (begin == (INDEX_TYPE)NumericLimits<int64_t>::Minimum()) {
		begin++;
		is_min = true;
	}

	const auto length = ValueLength<INPUT_TYPE, INDEX_TYPE>(value);
	if (begin < 0 && -begin > length && end < 0 && -end > length) {
		begin = 0;
		end = 0;
		return true;
	}
	if (begin < 0 && -begin > length) {
		begin = 0;
	}
	ClampIndex(begin, value, length, is_min);
	ClampIndex(end, value, length, false);
	end = MaxValue<INDEX_TYPE>(begin, end);

	return true;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValue(Vector &result, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end) {
	return input;
}

template <>
list_entry_t SliceValue(Vector &result, list_entry_t input, int64_t begin, int64_t end) {
	input.offset += begin;
	input.length = end - begin;
	return input;
}

template <>
string_t SliceValue(Vector &result, string_t input, int64_t begin, int64_t end) {
	// one-based - zero has strange semantics
	return SubstringFun::SubstringUnicode(result, input, begin + 1, end - begin);
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
INPUT_TYPE SliceValueWithSteps(Vector &result, SelectionVector &sel, INPUT_TYPE input, INDEX_TYPE begin, INDEX_TYPE end,
                               INDEX_TYPE step, idx_t &sel_idx) {
	return input;
}

template <>
list_entry_t SliceValueWithSteps(Vector &result, SelectionVector &sel, list_entry_t input, int64_t begin, int64_t end,
                                 int64_t step, idx_t &sel_idx) {
	if (end - begin == 0) {
		input.length = 0;
		input.offset = sel_idx;
		return input;
	}
	input.length = CalculateSliceLength(begin, end, step, true);
	idx_t child_idx = input.offset + begin;
	if (step < 0) {
		child_idx = input.offset + end - 1;
	}
	input.offset = sel_idx;
	for (idx_t i = 0; i < input.length; i++) {
		sel.set_index(sel_idx, child_idx);
		child_idx += step;
		sel_idx++;
	}
	return input;
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteConstantSlice(Vector &result, Vector &str_vector, Vector &begin_vector, Vector &end_vector,
                                 optional_ptr<Vector> step_vector, const idx_t count, SelectionVector &sel,
                                 idx_t &sel_idx, optional_ptr<Vector> result_child_vector, bool begin_is_empty,
                                 bool end_is_empty) {
	auto result_data = ConstantVector::GetData<INPUT_TYPE>(result);
	auto str_data = ConstantVector::GetData<INPUT_TYPE>(str_vector);
	auto begin_data = ConstantVector::GetData<INDEX_TYPE>(begin_vector);
	auto end_data = ConstantVector::GetData<INDEX_TYPE>(end_vector);
	auto step_data = step_vector ? ConstantVector::GetData<INDEX_TYPE>(*step_vector) : nullptr;

	auto str = str_data[0];
	auto begin = begin_is_empty ? 0 : begin_data[0];
	auto end = end_is_empty ? ValueLength<INPUT_TYPE, INDEX_TYPE>(str) : end_data[0];
	auto step = step_data ? step_data[0] : 1;

	if (step < 0) {
		swap(begin, end);
		begin = end_is_empty ? 0 : begin;
		end = begin_is_empty ? ValueLength<INPUT_TYPE, INDEX_TYPE>(str) : end;
	}

	auto str_valid = !ConstantVector::IsNull(str_vector);
	auto begin_valid = !ConstantVector::IsNull(begin_vector);
	auto end_valid = !ConstantVector::IsNull(end_vector);
	auto step_valid = step_vector && !ConstantVector::IsNull(*step_vector);

	// Clamp offsets
	bool clamp_result = false;
	if (str_valid && begin_valid && end_valid && (step_valid || step == 1)) {
		clamp_result = ClampSlice(str, begin, end);
	}

	auto sel_length = 0;
	bool sel_valid = false;
	if (step_vector && step_valid && str_valid && begin_valid && end_valid && step != 1 && end - begin > 0) {
		sel_length = CalculateSliceLength(begin, end, step, step_valid);
		sel.Initialize(sel_length);
		sel_valid = true;
	}

	// Try to slice
	if (!str_valid || !begin_valid || !end_valid || (step_vector && !step_valid) || !clamp_result) {
		ConstantVector::SetNull(result, true);
	} else if (step == 1) {
		result_data[0] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, str, begin, end);
	} else {
		result_data[0] = SliceValueWithSteps<INPUT_TYPE, INDEX_TYPE>(result, sel, str, begin, end, step, sel_idx);
	}

	if (sel_valid) {
		result_child_vector->Slice(sel, sel_length);
		ListVector::SetListSize(result, sel_length);
	}
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteFlatSlice(Vector &result, Vector &list_vector, Vector &begin_vector, Vector &end_vector,
                             optional_ptr<Vector> step_vector, const idx_t count, SelectionVector &sel, idx_t &sel_idx,
                             optional_ptr<Vector> result_child_vector, bool begin_is_empty, bool end_is_empty) {
	UnifiedVectorFormat list_data, begin_data, end_data, step_data;
	idx_t sel_length = 0;

	list_vector.ToUnifiedFormat(count, list_data);
	begin_vector.ToUnifiedFormat(count, begin_data);
	end_vector.ToUnifiedFormat(count, end_data);
	if (step_vector) {
		step_vector->ToUnifiedFormat(count, step_data);
		sel.Initialize(ListVector::GetListSize(list_vector));
	}

	auto result_data = FlatVector::GetData<INPUT_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; ++i) {
		auto list_idx = list_data.sel->get_index(i);
		auto begin_idx = begin_data.sel->get_index(i);
		auto end_idx = end_data.sel->get_index(i);
		auto step_idx = step_vector ? step_data.sel->get_index(i) : 0;

		auto list_valid = list_data.validity.RowIsValid(list_idx);
		auto begin_valid = begin_data.validity.RowIsValid(begin_idx);
		auto end_valid = end_data.validity.RowIsValid(end_idx);
		auto step_valid = step_vector && step_data.validity.RowIsValid(step_idx);

		if (!list_valid || !begin_valid || !end_valid || (step_vector && !step_valid)) {
			result_mask.SetInvalid(i);
			continue;
		}

		auto sliced = reinterpret_cast<INPUT_TYPE *>(list_data.data)[list_idx];
		auto begin = begin_is_empty ? 0 : reinterpret_cast<INDEX_TYPE *>(begin_data.data)[begin_idx];
		auto end = end_is_empty ? ValueLength<INPUT_TYPE, INDEX_TYPE>(sliced)
		                        : reinterpret_cast<INDEX_TYPE *>(end_data.data)[end_idx];
		auto step = step_vector ? reinterpret_cast<INDEX_TYPE *>(step_data.data)[step_idx] : 1;

		if (step < 0) {
			swap(begin, end);
			begin = end_is_empty ? 0 : begin;
			end = begin_is_empty ? ValueLength<INPUT_TYPE, INDEX_TYPE>(sliced) : end;
		}

		bool clamp_result = false;
		if (step_valid || step == 1) {
			clamp_result = ClampSlice(sliced, begin, end);
		}

		auto length = 0;
		if (end - begin > 0) {
			length = CalculateSliceLength(begin, end, step, step_valid);
		}
		sel_length += length;

		if (!clamp_result) {
			result_mask.SetInvalid(i);
		} else if (!step_vector) {
			result_data[i] = SliceValue<INPUT_TYPE, INDEX_TYPE>(result, sliced, begin, end);
		} else {
			result_data[i] =
			    SliceValueWithSteps<INPUT_TYPE, INDEX_TYPE>(result, sel, sliced, begin, end, step, sel_idx);
		}
	}
	if (step_vector) {
		SelectionVector new_sel(sel_length);
		for (idx_t i = 0; i < sel_length; ++i) {
			new_sel.set_index(i, sel.get_index(i));
		}
		result_child_vector->Slice(new_sel, sel_length);
		ListVector::SetListSize(result, sel_length);
	}
}

template <typename INPUT_TYPE, typename INDEX_TYPE>
static void ExecuteSlice(Vector &result, Vector &list_or_str_vector, Vector &begin_vector, Vector &end_vector,
                         optional_ptr<Vector> step_vector, const idx_t count, bool begin_is_empty, bool end_is_empty) {
	optional_ptr<Vector> result_child_vector;
	if (step_vector) {
		result_child_vector = &ListVector::GetEntry(result);
	}

	SelectionVector sel;
	idx_t sel_idx = 0;

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		ExecuteConstantSlice<INPUT_TYPE, INDEX_TYPE>(result, list_or_str_vector, begin_vector, end_vector, step_vector,
		                                             count, sel, sel_idx, result_child_vector, begin_is_empty,
		                                             end_is_empty);
	} else {
		ExecuteFlatSlice<INPUT_TYPE, INDEX_TYPE>(result, list_or_str_vector, begin_vector, end_vector, step_vector,
		                                         count, sel, sel_idx, result_child_vector, begin_is_empty,
		                                         end_is_empty);
	}
	result.Verify(count);
}

static void ArraySliceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 4);
	D_ASSERT(args.data.size() == 3 || args.data.size() == 4);
	auto count = args.size();

	Vector &list_or_str_vector = args.data[0];
	if (list_or_str_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		auto &result_validity = FlatVector::Validity(result);
		result_validity.SetInvalid(0);
		return;
	}

	Vector &begin_vector = args.data[1];
	Vector &end_vector = args.data[2];

	optional_ptr<Vector> step_vector;
	if (args.ColumnCount() == 4) {
		step_vector = &args.data[3];
	}

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListSliceBindData>();
	auto begin_is_empty = info.begin_is_empty;
	auto end_is_empty = info.end_is_empty;

	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
	switch (result.GetType().id()) {
	case LogicalTypeId::LIST: {
		// Share the value dictionary as we are just going to slice it
		if (list_or_str_vector.GetVectorType() != VectorType::FLAT_VECTOR &&
		    list_or_str_vector.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			list_or_str_vector.Flatten(count);
		}
		ListVector::ReferenceEntry(result, list_or_str_vector);
		ExecuteSlice<list_entry_t, int64_t>(result, list_or_str_vector, begin_vector, end_vector, step_vector, count,
		                                    begin_is_empty, end_is_empty);
		break;
	}
	case LogicalTypeId::VARCHAR: {
		ExecuteSlice<string_t, int64_t>(result, list_or_str_vector, begin_vector, end_vector, step_vector, count,
		                                begin_is_empty, end_is_empty);
		break;
	}
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static bool CheckIfParamIsEmpty(duckdb::unique_ptr<duckdb::Expression> &param) {
	bool is_empty = false;
	if (param->return_type.id() == LogicalTypeId::LIST) {
		auto empty_list = make_uniq<BoundConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
		is_empty = param->Equals(*empty_list);
		if (!is_empty) {
			// if the param is not empty, the user has entered a list instead of a BIGINT
			throw BinderException("The upper and lower bounds of the slice must be a BIGINT");
		}
	}
	return is_empty;
}

static unique_ptr<FunctionData> ArraySliceBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 3 || arguments.size() == 4);
	D_ASSERT(bound_function.arguments.size() == 3 || bound_function.arguments.size() == 4);

	switch (arguments[0]->return_type.id()) {
	case LogicalTypeId::LIST:
		// The result is the same type
		bound_function.return_type = arguments[0]->return_type;
		break;
	case LogicalTypeId::VARCHAR:
		// string slice returns a string
		if (bound_function.arguments.size() == 4) {
			throw NotImplementedException(
			    "Slice with steps has not been implemented for string types, you can consider rewriting your query as "
			    "follows:\n SELECT array_to_string((str_split(string, '')[begin:end:step], '');");
		}
		bound_function.return_type = arguments[0]->return_type;
		for (idx_t i = 1; i < 3; i++) {
			if (arguments[i]->return_type.id() != LogicalTypeId::LIST) {
				bound_function.arguments[i] = LogicalType::BIGINT;
			}
		}
		break;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::UNKNOWN:
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		break;
	default:
		throw BinderException("ARRAY_SLICE can only operate on LISTs and VARCHARs");
	}

	bool begin_is_empty = CheckIfParamIsEmpty(arguments[1]);
	if (!begin_is_empty) {
		bound_function.arguments[1] = LogicalType::BIGINT;
	}
	bool end_is_empty = CheckIfParamIsEmpty(arguments[2]);
	if (!end_is_empty) {
		bound_function.arguments[2] = LogicalType::BIGINT;
	}

	return make_uniq<ListSliceBindData>(bound_function.return_type, begin_is_empty, end_is_empty);
}

ScalarFunctionSet ListSliceFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun({LogicalType::ANY, LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, ArraySliceFunction,
	                   ArraySliceBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunctionSet set;
	set.AddFunction(fun);
	fun.arguments.push_back(LogicalType::BIGINT);
	set.AddFunction(fun);
	return set;
}

} // namespace duckdb






namespace duckdb {

void ListFlattenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	Vector &input = args.data[0];
	if (input.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(input);
		return;
	}

	idx_t count = args.size();

	UnifiedVectorFormat list_data;
	input.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto &child_vector = ListVector::GetEntry(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (child_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		for (idx_t i = 0; i < count; i++) {
			auto list_index = list_data.sel->get_index(i);
			if (!list_data.validity.RowIsValid(list_index)) {
				result_validity.SetInvalid(i);
				continue;
			}
			result_entries[i].offset = 0;
			result_entries[i].length = 0;
		}
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return;
	}

	auto child_size = ListVector::GetListSize(input);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(child_size, child_data);
	auto child_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_data);
	auto &data_vector = ListVector::GetEntry(child_vector);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto list_entry = list_entries[list_index];

		idx_t source_offset = 0;
		// Find first valid child list entry to get offset
		for (idx_t j = 0; j < list_entry.length; j++) {
			auto child_list_index = child_data.sel->get_index(list_entry.offset + j);
			if (child_data.validity.RowIsValid(child_list_index)) {
				source_offset = child_entries[child_list_index].offset;
				break;
			}
		}

		idx_t length = 0;
		// Find last valid child list entry to get length
		for (idx_t j = list_entry.length - 1; j != (idx_t)-1; j--) {
			auto child_list_index = child_data.sel->get_index(list_entry.offset + j);
			if (child_data.validity.RowIsValid(child_list_index)) {
				auto child_entry = child_entries[child_list_index];
				length = child_entry.offset + child_entry.length - source_offset;
				break;
			}
		}
		ListVector::Append(result, data_vector, source_offset + length, source_offset);

		result_entries[i].offset = offset;
		result_entries[i].length = length;
		offset += length;
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListFlattenBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &input_type = arguments[0]->return_type;
	bound_function.arguments[0] = input_type;
	if (input_type.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalType(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}
	D_ASSERT(input_type.id() == LogicalTypeId::LIST);

	auto child_type = ListType::GetChildType(input_type);
	if (child_type.id() == LogicalType::SQLNULL) {
		bound_function.return_type = input_type;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalType(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}
	D_ASSERT(child_type.id() == LogicalTypeId::LIST);

	bound_function.return_type = child_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListFlattenStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &list_child_stats = ListStats::GetChildStats(child_stats[0]);
	auto child_copy = list_child_stats.Copy();
	child_copy.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	return child_copy.ToUnique();
}

ScalarFunction ListFlattenFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::LIST(LogicalType::ANY))}, LogicalType::LIST(LogicalType::ANY),
	                      ListFlattenFunction, ListFlattenBind, nullptr, ListFlattenStats);
}

} // namespace duckdb












namespace duckdb {

// FIXME: use a local state for each thread to increase performance?
// FIXME: benchmark the use of simple_update against using update (if applicable)

static unique_ptr<FunctionData> ListAggregatesBindFailure(ScalarFunction &bound_function) {
	bound_function.arguments[0] = LogicalType::SQLNULL;
	bound_function.return_type = LogicalType::SQLNULL;
	return make_uniq<VariableReturnBindData>(LogicalType::SQLNULL);
}

struct ListAggregatesBindData : public FunctionData {
	ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p);
	~ListAggregatesBindData() override;

	LogicalType stype;
	unique_ptr<Expression> aggr_expr;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ListAggregatesBindData>(stype, aggr_expr->Copy());
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ListAggregatesBindData>();
		return stype == other.stype && aggr_expr->Equals(*other.aggr_expr);
	}
	void Serialize(Serializer &serializer) const {
		serializer.WriteProperty(1, "stype", stype);
		serializer.WriteProperty(2, "aggr_expr", aggr_expr);
	}
	static unique_ptr<ListAggregatesBindData> Deserialize(Deserializer &deserializer) {
		auto stype = deserializer.ReadProperty<LogicalType>(1, "stype");
		auto aggr_expr = deserializer.ReadProperty<unique_ptr<Expression>>(2, "aggr_expr");
		auto result = make_uniq<ListAggregatesBindData>(std::move(stype), std::move(aggr_expr));
		return result;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		auto bind_data = dynamic_cast<const ListAggregatesBindData *>(bind_data_p.get());
		serializer.WritePropertyWithDefault(100, "bind_data", bind_data, (const ListAggregatesBindData *)nullptr);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &bound_function) {
		auto result = deserializer.ReadPropertyWithDefault<unique_ptr<ListAggregatesBindData>>(
		    100, "bind_data", unique_ptr<ListAggregatesBindData>(nullptr));
		if (!result) {
			return ListAggregatesBindFailure(bound_function);
		}
		return std::move(result);
	}
};

ListAggregatesBindData::ListAggregatesBindData(const LogicalType &stype_p, unique_ptr<Expression> aggr_expr_p)
    : stype(stype_p), aggr_expr(std::move(aggr_expr_p)) {
}

ListAggregatesBindData::~ListAggregatesBindData() {
}

struct StateVector {
	StateVector(idx_t count_p, unique_ptr<Expression> aggr_expr_p)
	    : count(count_p), aggr_expr(std::move(aggr_expr_p)), state_vector(Vector(LogicalType::POINTER, count_p)) {
	}

	~StateVector() { // NOLINT
		// destroy objects within the aggregate states
		auto &aggr = aggr_expr->Cast<BoundAggregateExpression>();
		if (aggr.function.destructor) {
			ArenaAllocator allocator(Allocator::DefaultAllocator());
			AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);
			aggr.function.destructor(state_vector, aggr_input_data, count);
		}
	}

	idx_t count;
	unique_ptr<Expression> aggr_expr;
	Vector state_vector;
};

struct FinalizeValueFunctor {
	template <class T>
	static Value FinalizeValue(T first) {
		return Value::CreateValue(first);
	}
};

struct FinalizeStringValueFunctor {
	template <class T>
	static Value FinalizeValue(T first) {
		string_t value = first;
		return Value::CreateValue(value);
	}
};

struct AggregateFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {
	}
};

struct DistinctFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

		auto result_data = FlatVector::GetData<list_entry_t>(result);

		idx_t offset = 0;
		for (idx_t i = 0; i < count; i++) {

			auto state = states[sdata.sel->get_index(i)];
			result_data[i].offset = offset;

			if (!state->hist) {
				result_data[i].length = 0;
				continue;
			}

			result_data[i].length = state->hist->size();
			offset += state->hist->size();

			for (auto &entry : *state->hist) {
				Value bucket_value = OP::template FinalizeValue<T>(entry.first);
				ListVector::PushBack(result, bucket_value);
			}
		}
		result.Verify(count);
	}
};

struct UniqueFunctor {
	template <class OP, class T, class MAP_TYPE = unordered_map<T, idx_t>>
	static void ListExecuteFunction(Vector &result, Vector &state_vector, idx_t count) {

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

		auto result_data = FlatVector::GetData<uint64_t>(result);

		for (idx_t i = 0; i < count; i++) {

			auto state = states[sdata.sel->get_index(i)];

			if (!state->hist) {
				result_data[i] = 0;
				continue;
			}

			result_data[i] = state->hist->size();
		}
		result.Verify(count);
	}
};

template <class FUNCTION_FUNCTOR, bool IS_AGGR = false>
static void ListAggregatesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	Vector &lists = args.data[0];

	// set the result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// get the aggregate function
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListAggregatesBindData>();
	auto &aggr = info.aggr_expr->Cast<BoundAggregateExpression>();
	ArenaAllocator allocator(Allocator::DefaultAllocator());
	AggregateInputData aggr_input_data(aggr.bind_info.get(), allocator);

	D_ASSERT(aggr.function.update);

	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	child_vector.Flatten(lists_size);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// state_buffer holds the state for each list of this chunk
	idx_t size = aggr.function.state_size();
	auto state_buffer = make_unsafe_uniq_array<data_t>(size * count);

	// state vector for initialize and finalize
	StateVector state_vector(count, info.aggr_expr->Copy());
	auto states = FlatVector::GetData<data_ptr_t>(state_vector.state_vector);

	// state vector of STANDARD_VECTOR_SIZE holds the pointers to the states
	Vector state_vector_update = Vector(LogicalType::POINTER);
	auto states_update = FlatVector::GetData<data_ptr_t>(state_vector_update);

	// selection vector pointing to the data
	SelectionVector sel_vector(STANDARD_VECTOR_SIZE);
	idx_t states_idx = 0;

	for (idx_t i = 0; i < count; i++) {

		// initialize the state for this list
		auto state_ptr = state_buffer.get() + size * i;
		states[i] = state_ptr;
		aggr.function.initialize(states[i]);

		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// skip empty list
		if (list_entry.length == 0) {
			continue;
		}

		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			// states vector is full, update
			if (states_idx == STANDARD_VECTOR_SIZE) {
				// update the aggregate state(s)
				Vector slice(child_vector, sel_vector, states_idx);
				aggr.function.update(&slice, aggr_input_data, 1, state_vector_update, states_idx);

				// reset values
				states_idx = 0;
			}

			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			sel_vector.set_index(states_idx, source_idx);
			states_update[states_idx] = state_ptr;
			states_idx++;
		}
	}

	// update the remaining elements of the last list(s)
	if (states_idx != 0) {
		Vector slice(child_vector, sel_vector, states_idx);
		aggr.function.update(&slice, aggr_input_data, 1, state_vector_update, states_idx);
	}

	if (IS_AGGR) {
		// finalize all the aggregate states
		aggr.function.finalize(state_vector.state_vector, aggr_input_data, result, count, 0);

	} else {
		// finalize manually to use the map
		D_ASSERT(aggr.function.arguments.size() == 1);
		auto key_type = aggr.function.arguments[0];

		switch (key_type.InternalType()) {
		case PhysicalType::BOOL:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, bool>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT8:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint8_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT16:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint16_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT32:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint32_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::UINT64:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, uint64_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT8:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int8_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT16:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int16_t>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::INT32:
			if (key_type.id() == LogicalTypeId::DATE) {
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, date_t>(
				    result, state_vector.state_vector, count);
			} else {
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int32_t>(
				    result, state_vector.state_vector, count);
			}
			break;
		case PhysicalType::INT64:
			switch (key_type.id()) {
			case LogicalTypeId::TIME:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, dtime_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIME_TZ:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, dtime_tz_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_MS:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_ms_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_NS:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_ns_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_SEC:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_sec_t>(
				    result, state_vector.state_vector, count);
				break;
			case LogicalTypeId::TIMESTAMP_TZ:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, timestamp_tz_t>(
				    result, state_vector.state_vector, count);
				break;
			default:
				FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, int64_t>(
				    result, state_vector.state_vector, count);
				break;
			}
			break;
		case PhysicalType::FLOAT:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, float>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::DOUBLE:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeValueFunctor, double>(
			    result, state_vector.state_vector, count);
			break;
		case PhysicalType::VARCHAR:
			FUNCTION_FUNCTOR::template ListExecuteFunction<FinalizeStringValueFunctor, string>(
			    result, state_vector.state_vector, count);
			break;
		default:
			throw InternalException("Unimplemented histogram aggregate");
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ListAggregateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 2);
	ListAggregatesFunction<AggregateFunctor, true>(args, state, result);
}

static void ListDistinctFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	ListAggregatesFunction<DistinctFunctor>(args, state, result);
}

static void ListUniqueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	ListAggregatesFunction<UniqueFunctor>(args, state, result);
}

template <bool IS_AGGR = false>
static unique_ptr<FunctionData>
ListAggregatesBindFunction(ClientContext &context, ScalarFunction &bound_function, const LogicalType &list_child_type,
                           AggregateFunction &aggr_function, vector<unique_ptr<Expression>> &arguments) {

	// create the child expression and its type
	vector<unique_ptr<Expression>> children;
	auto expr = make_uniq<BoundConstantExpression>(Value(list_child_type));
	children.push_back(std::move(expr));
	// push any extra arguments into the list aggregate bind
	if (arguments.size() > 2) {
		for (idx_t i = 2; i < arguments.size(); i++) {
			children.push_back(std::move(arguments[i]));
		}
		arguments.resize(2);
	}

	FunctionBinder function_binder(context);
	auto bound_aggr_function = function_binder.BindAggregateFunction(aggr_function, std::move(children));
	bound_function.arguments[0] = LogicalType::LIST(bound_aggr_function->function.arguments[0]);

	if (IS_AGGR) {
		bound_function.return_type = bound_aggr_function->function.return_type;
	}
	// check if the aggregate function consumed all the extra input arguments
	if (bound_aggr_function->children.size() > 1) {
		throw InvalidInputException(
		    "Aggregate function %s is not supported for list_aggr: extra arguments were not removed during bind",
		    bound_aggr_function->ToString());
	}

	return make_uniq<ListAggregatesBindData>(bound_function.return_type, std::move(bound_aggr_function));
}

template <bool IS_AGGR = false>
static unique_ptr<FunctionData> ListAggregatesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		return ListAggregatesBindFailure(bound_function);
	}

	bool is_parameter = arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN;
	auto list_child_type = is_parameter ? LogicalTypeId::UNKNOWN : ListType::GetChildType(arguments[0]->return_type);

	string function_name = "histogram";
	if (IS_AGGR) { // get the name of the aggregate function
		if (!arguments[1]->IsFoldable()) {
			throw InvalidInputException("Aggregate function name must be a constant");
		}
		// get the function name
		Value function_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		function_name = function_value.ToString();
	}

	// look up the aggregate function in the catalog
	QueryErrorContext error_context(nullptr, 0);
	auto &func = Catalog::GetSystemCatalog(context).GetEntry<AggregateFunctionCatalogEntry>(
	    context, DEFAULT_SCHEMA, function_name, error_context);
	D_ASSERT(func.type == CatalogType::AGGREGATE_FUNCTION_ENTRY);

	if (is_parameter) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	// find a matching aggregate function
	string error;
	vector<LogicalType> types;
	types.push_back(list_child_type);
	// push any extra arguments into the type list
	for (idx_t i = 2; i < arguments.size(); i++) {
		types.push_back(arguments[i]->return_type);
	}

	FunctionBinder function_binder(context);
	auto best_function_idx = function_binder.BindFunction(func.name, func.functions, types, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException("No matching aggregate function\n%s", error);
	}

	// found a matching function, bind it as an aggregate
	auto best_function = func.functions.GetFunctionByOffset(best_function_idx);
	if (IS_AGGR) {
		return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, best_function, arguments);
	}

	// create the unordered map histogram function
	D_ASSERT(best_function.arguments.size() == 1);
	auto key_type = best_function.arguments[0];
	auto aggr_function = HistogramFun::GetHistogramUnorderedMap(key_type);
	return ListAggregatesBindFunction<IS_AGGR>(context, bound_function, list_child_type, aggr_function, arguments);
}

static unique_ptr<FunctionData> ListAggregateBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// the list column and the name of the aggregate function
	D_ASSERT(bound_function.arguments.size() >= 2);
	D_ASSERT(arguments.size() >= 2);

	return ListAggregatesBind<true>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListDistinctBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(bound_function.arguments.size() == 1);
	D_ASSERT(arguments.size() == 1);
	bound_function.return_type = arguments[0]->return_type;

	return ListAggregatesBind<>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListUniqueBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(bound_function.arguments.size() == 1);
	D_ASSERT(arguments.size() == 1);
	bound_function.return_type = LogicalType::UBIGINT;

	return ListAggregatesBind<>(context, bound_function, arguments);
}

ScalarFunction ListAggregateFun::GetFunction() {
	auto result = ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR}, LogicalType::ANY,
	                             ListAggregateFunction, ListAggregateBind);
	result.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	result.varargs = LogicalType::ANY;
	result.serialize = ListAggregatesBindData::Serialize;
	result.deserialize = ListAggregatesBindData::Deserialize;
	return result;
}

ScalarFunction ListDistinctFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                      ListDistinctFunction, ListDistinctBind);
}

ScalarFunction ListUniqueFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::UBIGINT, ListUniqueFunction,
	                      ListUniqueBind);
}

} // namespace duckdb

#include <cmath>
#include <algorithm>

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListCosineSimilarity(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto left_count = ListVector::GetListSize(left);
	auto right_count = ListVector::GetListSize(right);

	auto &left_child = ListVector::GetEntry(left);
	auto &right_child = ListVector::GetEntry(right);

	D_ASSERT(left_child.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(right_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(left_child).CheckAllValid(left_count)) {
		throw InvalidInputException("list_cosine_similarity: left argument can not contain NULL values");
	}

	if (!FlatVector::Validity(right_child).CheckAllValid(right_count)) {
		throw InvalidInputException("list_cosine_similarity: right argument can not contain NULL values");
	}

	auto left_data = FlatVector::GetData<NUMERIC_TYPE>(left_child);
	auto right_data = FlatVector::GetData<NUMERIC_TYPE>(right_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException(StringUtil::Format(
			        "list_cosine_similarity: list dimensions must be equal, got left length %d and right length %d",
			        left.length, right.length));
		    }

		    auto dimensions = left.length;

		    NUMERIC_TYPE distance = 0;
		    NUMERIC_TYPE norm_l = 0;
		    NUMERIC_TYPE norm_r = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;
		    for (idx_t i = 0; i < dimensions; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    distance += x * y;
			    norm_l += x * x;
			    norm_r += y * y;
		    }

		    auto similarity = distance / (std::sqrt(norm_l) * std::sqrt(norm_r));

		    // clamp to [-1, 1] to avoid floating point errors
		    return std::max(static_cast<NUMERIC_TYPE>(-1), std::min(similarity, static_cast<NUMERIC_TYPE>(1)));
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListCosineSimilarityFun::GetFunctions() {
	ScalarFunctionSet set("list_cosine_similarity");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListCosineSimilarity<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListCosineSimilarity<double>));
	return set;
}

} // namespace duckdb

#include <cmath>

namespace duckdb {

template <class NUMERIC_TYPE>
static void ListDistance(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto left_count = ListVector::GetListSize(left);
	auto right_count = ListVector::GetListSize(right);

	auto &left_child = ListVector::GetEntry(left);
	auto &right_child = ListVector::GetEntry(right);

	D_ASSERT(left_child.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(right_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(left_child).CheckAllValid(left_count)) {
		throw InvalidInputException("list_distance: left argument can not contain NULL values");
	}

	if (!FlatVector::Validity(right_child).CheckAllValid(right_count)) {
		throw InvalidInputException("list_distance: right argument can not contain NULL values");
	}

	auto left_data = FlatVector::GetData<NUMERIC_TYPE>(left_child);
	auto right_data = FlatVector::GetData<NUMERIC_TYPE>(right_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException(StringUtil::Format(
			        "list_distance: list dimensions must be equal, got left length %d and right length %d", left.length,
			        right.length));
		    }

		    auto dimensions = left.length;

		    NUMERIC_TYPE distance = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;

		    for (idx_t i = 0; i < dimensions; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    auto diff = x - y;
			    distance += diff * diff;
		    }

		    return std::sqrt(distance);
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListDistanceFun::GetFunctions() {
	ScalarFunctionSet set("list_distance");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListDistance<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListDistance<double>));
	return set;
}

} // namespace duckdb


namespace duckdb {

template <class NUMERIC_TYPE>
static void ListInnerProduct(DataChunk &args, ExpressionState &, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto count = args.size();
	auto &left = args.data[0];
	auto &right = args.data[1];
	auto left_count = ListVector::GetListSize(left);
	auto right_count = ListVector::GetListSize(right);

	auto &left_child = ListVector::GetEntry(left);
	auto &right_child = ListVector::GetEntry(right);

	D_ASSERT(left_child.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(right_child.GetVectorType() == VectorType::FLAT_VECTOR);

	if (!FlatVector::Validity(left_child).CheckAllValid(left_count)) {
		throw InvalidInputException("list_inner_product: left argument can not contain NULL values");
	}

	if (!FlatVector::Validity(right_child).CheckAllValid(right_count)) {
		throw InvalidInputException("list_inner_product: right argument can not contain NULL values");
	}

	auto left_data = FlatVector::GetData<NUMERIC_TYPE>(left_child);
	auto right_data = FlatVector::GetData<NUMERIC_TYPE>(right_child);

	BinaryExecutor::Execute<list_entry_t, list_entry_t, NUMERIC_TYPE>(
	    left, right, result, count, [&](list_entry_t left, list_entry_t right) {
		    if (left.length != right.length) {
			    throw InvalidInputException(StringUtil::Format(
			        "list_inner_product: list dimensions must be equal, got left length %d and right length %d",
			        left.length, right.length));
		    }

		    auto dimensions = left.length;

		    NUMERIC_TYPE distance = 0;

		    auto l_ptr = left_data + left.offset;
		    auto r_ptr = right_data + right.offset;

		    for (idx_t i = 0; i < dimensions; i++) {
			    auto x = *l_ptr++;
			    auto y = *r_ptr++;
			    distance += x * y;
		    }

		    return distance;
	    });

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ListInnerProductFun::GetFunctions() {
	ScalarFunctionSet set("list_inner_product");
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::FLOAT), LogicalType::LIST(LogicalType::FLOAT)},
	                               LogicalType::FLOAT, ListInnerProduct<float>));
	set.AddFunction(ScalarFunction({LogicalType::LIST(LogicalType::DOUBLE), LogicalType::LIST(LogicalType::DOUBLE)},
	                               LogicalType::DOUBLE, ListInnerProduct<double>));
	return set;
}

} // namespace duckdb













namespace duckdb {

struct ListLambdaBindData : public FunctionData {
	ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr);
	~ListLambdaBindData() override;

	LogicalType stype;
	unique_ptr<Expression> lambda_expr;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const ScalarFunction &function) {
		//		auto &bind_data = bind_data_p->Cast<ListLambdaBindData>();
		//		serializer.WriteProperty(100, "stype", bind_data.stype);
		//		serializer.WritePropertyWithDefault(101, "lambda_expr", bind_data.lambda_expr,
		// unique_ptr<Expression>());
		throw NotImplementedException("FIXME: list lambda serialize");
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, ScalarFunction &function) {
		//		auto stype = deserializer.ReadProperty<LogicalType>(100, "stype");
		//		auto lambda_expr =
		//		    deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "lambda_expr",
		// unique_ptr<Expression>()); 		return make_uniq<ListLambdaBindData>(stype, std::move(lambda_expr));
		throw NotImplementedException("FIXME: list lambda deserialize");
	}
};

ListLambdaBindData::ListLambdaBindData(const LogicalType &stype_p, unique_ptr<Expression> lambda_expr_p)
    : stype(stype_p), lambda_expr(std::move(lambda_expr_p)) {
}

unique_ptr<FunctionData> ListLambdaBindData::Copy() const {
	return make_uniq<ListLambdaBindData>(stype, lambda_expr ? lambda_expr->Copy() : nullptr);
}

bool ListLambdaBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListLambdaBindData>();
	return Expression::Equals(lambda_expr, other.lambda_expr) && stype == other.stype;
}

ListLambdaBindData::~ListLambdaBindData() {
}

static void AppendTransformedToResult(Vector &lambda_vector, idx_t &elem_cnt, Vector &result) {

	// append the lambda_vector to the result list
	UnifiedVectorFormat lambda_child_data;
	lambda_vector.ToUnifiedFormat(elem_cnt, lambda_child_data);
	ListVector::Append(result, lambda_vector, *lambda_child_data.sel, elem_cnt, 0);
}

static void AppendFilteredToResult(Vector &lambda_vector, list_entry_t *result_entries, idx_t &elem_cnt, Vector &result,
                                   idx_t &curr_list_len, idx_t &curr_list_offset, idx_t &appended_lists_cnt,
                                   vector<idx_t> &lists_len, idx_t &curr_original_list_len, DataChunk &input_chunk) {

	idx_t true_count = 0;
	SelectionVector true_sel(elem_cnt);
	UnifiedVectorFormat lambda_data;
	lambda_vector.ToUnifiedFormat(elem_cnt, lambda_data);

	auto lambda_values = UnifiedVectorFormat::GetData<bool>(lambda_data);
	auto &lambda_validity = lambda_data.validity;

	// compute the new lengths and offsets, and create a selection vector
	for (idx_t i = 0; i < elem_cnt; i++) {
		auto entry = lambda_data.sel->get_index(i);

		while (appended_lists_cnt < lists_len.size() && lists_len[appended_lists_cnt] == 0) {
			result_entries[appended_lists_cnt].offset = curr_list_offset;
			result_entries[appended_lists_cnt].length = 0;
			appended_lists_cnt++;
		}

		// found a true value
		if (lambda_validity.RowIsValid(entry) && lambda_values[entry]) {
			true_sel.set_index(true_count++, i);
			curr_list_len++;
		}

		curr_original_list_len++;

		if (lists_len[appended_lists_cnt] == curr_original_list_len) {
			result_entries[appended_lists_cnt].offset = curr_list_offset;
			result_entries[appended_lists_cnt].length = curr_list_len;
			curr_list_offset += curr_list_len;
			appended_lists_cnt++;
			curr_list_len = 0;
			curr_original_list_len = 0;
		}
	}

	while (appended_lists_cnt < lists_len.size() && lists_len[appended_lists_cnt] == 0) {
		result_entries[appended_lists_cnt].offset = curr_list_offset;
		result_entries[appended_lists_cnt].length = 0;
		appended_lists_cnt++;
	}

	// slice to get the new lists and append them to the result
	Vector new_lists(input_chunk.data[0], true_sel, true_count);
	new_lists.Flatten(true_count);
	UnifiedVectorFormat new_lists_child_data;
	new_lists.ToUnifiedFormat(true_count, new_lists_child_data);
	ListVector::Append(result, new_lists, *new_lists_child_data.sel, true_count, 0);
}

static void ExecuteExpression(vector<LogicalType> &types, vector<LogicalType> &result_types, idx_t &elem_cnt,
                              SelectionVector &sel, vector<SelectionVector> &sel_vectors, DataChunk &input_chunk,
                              DataChunk &lambda_chunk, Vector &child_vector, DataChunk &args,
                              ExpressionExecutor &expr_executor) {

	input_chunk.SetCardinality(elem_cnt);
	lambda_chunk.SetCardinality(elem_cnt);

	// set the list child vector
	Vector slice(child_vector, sel, elem_cnt);
	Vector second_slice(child_vector, sel, elem_cnt);
	slice.Flatten(elem_cnt);
	second_slice.Flatten(elem_cnt);

	input_chunk.data[0].Reference(slice);
	input_chunk.data[1].Reference(second_slice);

	// set the other vectors
	vector<Vector> slices;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
		slices.emplace_back(args.data[col_idx + 1], sel_vectors[col_idx], elem_cnt);
		slices[col_idx].Flatten(elem_cnt);
		input_chunk.data[col_idx + 2].Reference(slices[col_idx]);
	}

	// execute the lambda expression
	expr_executor.Execute(input_chunk, lambda_chunk);
}

template <bool IS_TRANSFORM = true>
static void ListLambdaFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	// always at least the list argument
	D_ASSERT(args.ColumnCount() >= 1);

	auto count = args.size();
	Vector &lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	// e.g. window functions in sub queries return dictionary vectors, which segfault on expression execution
	// if not flattened first
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::FLAT_VECTOR &&
		    args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			args.data[i].Flatten(count);
		}
	}

	// get the lists data
	UnifiedVectorFormat lists_data;
	lists.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// get the lambda expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListLambdaBindData>();
	auto &lambda_expr = info.lambda_expr;

	// get the child vector and child data
	auto lists_size = ListVector::GetListSize(lists);
	auto &child_vector = ListVector::GetEntry(lists);
	child_vector.Flatten(lists_size);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	// to slice the child vector
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	// this vector never contains more than one element
	vector<LogicalType> result_types;
	result_types.push_back(lambda_expr->return_type);

	// non-lambda parameter columns
	vector<UnifiedVectorFormat> columns;
	vector<idx_t> indexes;
	vector<SelectionVector> sel_vectors;

	vector<LogicalType> types;
	types.push_back(child_vector.GetType());
	types.push_back(child_vector.GetType());

	// skip the list column
	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		columns.emplace_back();
		args.data[i].ToUnifiedFormat(count, columns[i - 1]);
		indexes.push_back(0);
		sel_vectors.emplace_back(STANDARD_VECTOR_SIZE);
		types.push_back(args.data[i].GetType());
	}

	// get the expression executor
	ExpressionExecutor expr_executor(state.GetContext(), *lambda_expr);

	// these are only for the list_filter
	vector<idx_t> lists_len;
	idx_t curr_list_len = 0;
	idx_t curr_list_offset = 0;
	idx_t appended_lists_cnt = 0;
	idx_t curr_original_list_len = 0;

	if (!IS_TRANSFORM) {
		lists_len.reserve(count);
	}

	DataChunk input_chunk;
	DataChunk lambda_chunk;
	input_chunk.InitializeEmpty(types);
	lambda_chunk.Initialize(Allocator::DefaultAllocator(), result_types);

	// loop over the child entries and create chunks to be executed by the expression executor
	idx_t elem_cnt = 0;
	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {

		auto lists_index = lists_data.sel->get_index(row_idx);
		const auto &list_entry = list_entries[lists_index];

		// set the result to NULL for this row
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(row_idx);
			if (!IS_TRANSFORM) {
				lists_len.push_back(0);
			}
			continue;
		}

		// set the length and offset of the resulting lists of list_transform
		if (IS_TRANSFORM) {
			result_entries[row_idx].offset = offset;
			result_entries[row_idx].length = list_entry.length;
			offset += list_entry.length;
		} else {
			lists_len.push_back(list_entry.length);
		}

		// empty list, nothing to execute
		if (list_entry.length == 0) {
			continue;
		}

		// get the data indexes
		for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
			indexes[col_idx] = columns[col_idx].sel->get_index(row_idx);
		}

		// iterate list elements and create transformed expression columns
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			// reached STANDARD_VECTOR_SIZE elements
			if (elem_cnt == STANDARD_VECTOR_SIZE) {
				lambda_chunk.Reset();
				ExecuteExpression(types, result_types, elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk,
				                  child_vector, args, expr_executor);

				auto &lambda_vector = lambda_chunk.data[0];

				if (IS_TRANSFORM) {
					AppendTransformedToResult(lambda_vector, elem_cnt, result);
				} else {
					AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len,
					                       curr_list_offset, appended_lists_cnt, lists_len, curr_original_list_len,
					                       input_chunk);
				}
				elem_cnt = 0;
			}

			// to slice the child vector
			auto source_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			sel.set_index(elem_cnt, source_idx);

			// for each column, set the index of the selection vector to slice properly
			for (idx_t col_idx = 0; col_idx < args.ColumnCount() - 1; col_idx++) {
				sel_vectors[col_idx].set_index(elem_cnt, indexes[col_idx]);
			}
			elem_cnt++;
		}
	}

	lambda_chunk.Reset();
	ExecuteExpression(types, result_types, elem_cnt, sel, sel_vectors, input_chunk, lambda_chunk, child_vector, args,
	                  expr_executor);
	auto &lambda_vector = lambda_chunk.data[0];

	if (IS_TRANSFORM) {
		AppendTransformedToResult(lambda_vector, elem_cnt, result);
	} else {
		AppendFilteredToResult(lambda_vector, result_entries, elem_cnt, result, curr_list_len, curr_list_offset,
		                       appended_lists_cnt, lists_len, curr_original_list_len, input_chunk);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void ListTransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<>(args, state, result);
}

static void ListFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ListLambdaFunction<false>(args, state, result);
}

template <int64_t LAMBDA_PARAM_CNT>
static unique_ptr<FunctionData> ListLambdaBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.parameter_count != LAMBDA_PARAM_CNT) {
		throw BinderException("Incorrect number of parameters in lambda function! " + bound_function.name +
		                      " expects " + to_string(LAMBDA_PARAM_CNT) + " parameter(s).");
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<ListLambdaBindData>(bound_function.return_type, nullptr);
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	D_ASSERT(arguments[0]->return_type.id() == LogicalTypeId::LIST);

	// get the lambda expression and put it in the bind info
	auto lambda_expr = std::move(bound_lambda_expr.lambda_expr);
	return make_uniq<ListLambdaBindData>(bound_function.return_type, std::move(lambda_expr));
}

static unique_ptr<FunctionData> ListTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {

	// at least the list column and the lambda function
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	bound_function.return_type = LogicalType::LIST(bound_lambda_expr.lambda_expr->return_type);
	return ListLambdaBind<1>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	// at least the list column and the lambda function
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->expression_class != ExpressionClass::BOUND_LAMBDA) {
		throw BinderException("Invalid lambda expression!");
	}

	// try to cast to boolean, if the return type of the lambda filter expression is not already boolean
	auto &bound_lambda_expr = arguments[1]->Cast<BoundLambdaExpression>();
	if (bound_lambda_expr.lambda_expr->return_type != LogicalType::BOOLEAN) {
		auto cast_lambda_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_lambda_expr.lambda_expr), LogicalType::BOOLEAN);
		bound_lambda_expr.lambda_expr = std::move(cast_lambda_expr);
	}

	bound_function.return_type = arguments[0]->return_type;
	return ListLambdaBind<1>(context, bound_function, arguments);
}

ScalarFunction ListTransformFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   ListTransformFunction, ListTransformBind, nullptr, nullptr);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	return fun;
}

ScalarFunction ListFilterFun::GetFunction() {
	ScalarFunction fun({LogicalType::LIST(LogicalType::ANY), LogicalType::LAMBDA}, LogicalType::LIST(LogicalType::ANY),
	                   ListFilterFunction, ListFilterBind, nullptr, nullptr);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = ListLambdaBindData::Serialize;
	fun.deserialize = ListLambdaBindData::Deserialize;
	return fun;
}

} // namespace duckdb









namespace duckdb {

struct ListSortBindData : public FunctionData {
	ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p, const LogicalType &return_type_p,
	                 const LogicalType &child_type_p, ClientContext &context_p);
	~ListSortBindData() override;

	OrderType order_type;
	OrderByNullType null_order;
	LogicalType return_type;
	LogicalType child_type;

	vector<LogicalType> types;
	vector<LogicalType> payload_types;

	ClientContext &context;
	RowLayout payload_layout;
	vector<BoundOrderByNode> orders;

public:
	bool Equals(const FunctionData &other_p) const override;
	unique_ptr<FunctionData> Copy() const override;
};

ListSortBindData::ListSortBindData(OrderType order_type_p, OrderByNullType null_order_p,
                                   const LogicalType &return_type_p, const LogicalType &child_type_p,
                                   ClientContext &context_p)
    : order_type(order_type_p), null_order(null_order_p), return_type(return_type_p), child_type(child_type_p),
      context(context_p) {

	// get the vector types
	types.emplace_back(LogicalType::USMALLINT);
	types.emplace_back(child_type);
	D_ASSERT(types.size() == 2);

	// get the payload types
	payload_types.emplace_back(LogicalType::UINTEGER);
	D_ASSERT(payload_types.size() == 1);

	// initialize the payload layout
	payload_layout.Initialize(payload_types);

	// get the BoundOrderByNode
	auto idx_col_expr = make_uniq_base<Expression, BoundReferenceExpression>(LogicalType::USMALLINT, 0);
	auto lists_col_expr = make_uniq_base<Expression, BoundReferenceExpression>(child_type, 1);
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT, std::move(idx_col_expr));
	orders.emplace_back(order_type, null_order, std::move(lists_col_expr));
}

unique_ptr<FunctionData> ListSortBindData::Copy() const {
	return make_uniq<ListSortBindData>(order_type, null_order, return_type, child_type, context);
}

bool ListSortBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ListSortBindData>();
	return order_type == other.order_type && null_order == other.null_order;
}

ListSortBindData::~ListSortBindData() {
}

// create the key_chunk and the payload_chunk and sink them into the local_sort_state
void SinkDataChunk(Vector *child_vector, SelectionVector &sel, idx_t offset_lists_indices, vector<LogicalType> &types,
                   vector<LogicalType> &payload_types, Vector &payload_vector, LocalSortState &local_sort_state,
                   bool &data_to_sort, Vector &lists_indices) {

	// slice the child vector
	Vector slice(*child_vector, sel, offset_lists_indices);

	// initialize and fill key_chunk
	DataChunk key_chunk;
	key_chunk.InitializeEmpty(types);
	key_chunk.data[0].Reference(lists_indices);
	key_chunk.data[1].Reference(slice);
	key_chunk.SetCardinality(offset_lists_indices);

	// initialize and fill key_chunk and payload_chunk
	DataChunk payload_chunk;
	payload_chunk.InitializeEmpty(payload_types);
	payload_chunk.data[0].Reference(payload_vector);
	payload_chunk.SetCardinality(offset_lists_indices);

	key_chunk.Verify();
	payload_chunk.Verify();

	// sink
	key_chunk.Flatten();
	local_sort_state.SinkChunk(key_chunk, payload_chunk);
	data_to_sort = true;
}

static void ListSortFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() >= 1 && args.ColumnCount() <= 3);
	auto count = args.size();
	Vector &input_lists = args.data[0];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_validity = FlatVector::Validity(result);

	if (input_lists.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<ListSortBindData>();

	// initialize the global and local sorting state
	auto &buffer_manager = BufferManager::GetBufferManager(info.context);
	GlobalSortState global_sort_state(buffer_manager, info.orders, info.payload_layout);
	LocalSortState local_sort_state;
	local_sort_state.Initialize(global_sort_state, buffer_manager);

	// this ensures that we do not change the order of the entries in the input chunk
	VectorOperations::Copy(input_lists, result, count, 0, 0);

	// get the child vector
	auto lists_size = ListVector::GetListSize(result);
	auto &child_vector = ListVector::GetEntry(result);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(lists_size, child_data);

	// get the lists data
	UnifiedVectorFormat lists_data;
	result.ToUnifiedFormat(count, lists_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

	// create the lists_indices vector, this contains an element for each list's entry,
	// the element corresponds to the list's index, e.g. for [1, 2, 4], [5, 4]
	// lists_indices contains [0, 0, 0, 1, 1]
	Vector lists_indices(LogicalType::USMALLINT);
	auto lists_indices_data = FlatVector::GetData<uint16_t>(lists_indices);

	// create the payload_vector, this is just a vector containing incrementing integers
	// this will later be used as the 'new' selection vector of the child_vector, after
	// rearranging the payload according to the sorting order
	Vector payload_vector(LogicalType::UINTEGER);
	auto payload_vector_data = FlatVector::GetData<uint32_t>(payload_vector);

	// selection vector pointing to the data of the child vector,
	// used for slicing the child_vector correctly
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	idx_t offset_lists_indices = 0;
	uint32_t incr_payload_count = 0;
	bool data_to_sort = false;

	for (idx_t i = 0; i < count; i++) {
		auto lists_index = lists_data.sel->get_index(i);
		const auto &list_entry = list_entries[lists_index];

		// nothing to do for this list
		if (!lists_data.validity.RowIsValid(lists_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		// empty list, no sorting required
		if (list_entry.length == 0) {
			continue;
		}

		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			// lists_indices vector is full, sink
			if (offset_lists_indices == STANDARD_VECTOR_SIZE) {
				SinkDataChunk(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
				              local_sort_state, data_to_sort, lists_indices);
				offset_lists_indices = 0;
			}

			auto source_idx = list_entry.offset + child_idx;
			sel.set_index(offset_lists_indices, source_idx);
			lists_indices_data[offset_lists_indices] = (uint32_t)i;
			payload_vector_data[offset_lists_indices] = source_idx;
			offset_lists_indices++;
			incr_payload_count++;
		}
	}

	if (offset_lists_indices != 0) {
		SinkDataChunk(&child_vector, sel, offset_lists_indices, info.types, info.payload_types, payload_vector,
		              local_sort_state, data_to_sort, lists_indices);
	}

	if (data_to_sort) {
		// add local state to global state, which sorts the data
		global_sort_state.AddLocalState(local_sort_state);
		global_sort_state.PrepareMergePhase();

		// selection vector that is to be filled with the 'sorted' payload
		SelectionVector sel_sorted(incr_payload_count);
		idx_t sel_sorted_idx = 0;

		// scan the sorted row data
		PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
		for (;;) {
			DataChunk result_chunk;
			result_chunk.Initialize(Allocator::DefaultAllocator(), info.payload_types);
			result_chunk.SetCardinality(0);
			scanner.Scan(result_chunk);
			if (result_chunk.size() == 0) {
				break;
			}

			// construct the selection vector with the new order from the result vectors
			Vector result_vector(result_chunk.data[0]);
			auto result_data = FlatVector::GetData<uint32_t>(result_vector);
			auto row_count = result_chunk.size();

			for (idx_t i = 0; i < row_count; i++) {
				sel_sorted.set_index(sel_sorted_idx, result_data[i]);
				D_ASSERT(result_data[i] < lists_size);
				sel_sorted_idx++;
			}
		}

		D_ASSERT(sel_sorted_idx == incr_payload_count);
		child_vector.Slice(sel_sorted, sel_sorted_idx);
		child_vector.Flatten(sel_sorted_idx);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListSortBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments, OrderType &order,
                                             OrderByNullType &null_order) {

	LogicalType child_type;
	if (arguments[0]->return_type == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		child_type = bound_function.return_type;
		return make_uniq<ListSortBindData>(order, null_order, bound_function.return_type, child_type, context);
	}

	bound_function.arguments[0] = arguments[0]->return_type;
	bound_function.return_type = arguments[0]->return_type;
	child_type = ListType::GetChildType(arguments[0]->return_type);

	return make_uniq<ListSortBindData>(order, null_order, bound_function.return_type, child_type, context);
}

template <class T>
static T GetOrder(ClientContext &context, Expression &expr) {
	if (!expr.IsFoldable()) {
		throw InvalidInputException("Sorting order must be a constant");
	}
	Value order_value = ExpressionExecutor::EvaluateScalar(context, expr);
	auto order_name = StringUtil::Upper(order_value.ToString());
	return EnumUtil::FromString<T>(order_name.c_str());
}

static unique_ptr<FunctionData> ListNormalSortBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(!arguments.empty() && arguments.size() <= 3);
	auto order = OrderType::ORDER_DEFAULT;
	auto null_order = OrderByNullType::ORDER_DEFAULT;

	// get the sorting order
	if (arguments.size() >= 2) {
		order = GetOrder<OrderType>(context, *arguments[1]);
	}
	// get the null sorting order
	if (arguments.size() == 3) {
		null_order = GetOrder<OrderByNullType>(context, *arguments[2]);
	}
	auto &config = DBConfig::GetConfig(context);
	order = config.ResolveOrder(order);
	null_order = config.ResolveNullOrder(order, null_order);
	return ListSortBind(context, bound_function, arguments, order, null_order);
}

static unique_ptr<FunctionData> ListReverseSortBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	auto order = OrderType::ORDER_DEFAULT;
	auto null_order = OrderByNullType::ORDER_DEFAULT;

	if (arguments.size() == 2) {
		null_order = GetOrder<OrderByNullType>(context, *arguments[1]);
	}
	auto &config = DBConfig::GetConfig(context);
	order = config.ResolveOrder(order);
	switch (order) {
	case OrderType::ASCENDING:
		order = OrderType::DESCENDING;
		break;
	case OrderType::DESCENDING:
		order = OrderType::ASCENDING;
		break;
	default:
		throw InternalException("Unexpected order type in list reverse sort");
	}
	null_order = config.ResolveNullOrder(order, null_order);
	return ListSortBind(context, bound_function, arguments, order, null_order);
}

ScalarFunctionSet ListSortFun::GetFunctions() {
	// one parameter: list
	ScalarFunction sort({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY), ListSortFunction,
	                    ListNormalSortBind);

	// two parameters: list, order
	ScalarFunction sort_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                          LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListNormalSortBind);

	// three parameters: list, order, null order
	ScalarFunction sort_orders({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListNormalSortBind);

	ScalarFunctionSet list_sort;
	list_sort.AddFunction(sort);
	list_sort.AddFunction(sort_order);
	list_sort.AddFunction(sort_orders);
	return list_sort;
}

ScalarFunctionSet ListReverseSortFun::GetFunctions() {
	// one parameter: list
	ScalarFunction sort_reverse({LogicalType::LIST(LogicalType::ANY)}, LogicalType::LIST(LogicalType::ANY),
	                            ListSortFunction, ListReverseSortBind);

	// two parameters: list, null order
	ScalarFunction sort_reverse_null_order({LogicalType::LIST(LogicalType::ANY), LogicalType::VARCHAR},
	                                       LogicalType::LIST(LogicalType::ANY), ListSortFunction, ListReverseSortBind);

	ScalarFunctionSet list_reverse_sort;
	list_reverse_sort.AddFunction(sort_reverse);
	list_reverse_sort.AddFunction(sort_reverse_null_order);
	return list_reverse_sort;
}

} // namespace duckdb










namespace duckdb {

static void ListValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &child_type = ListType::GetChildType(result.GetType());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);
		for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
			auto val = args.GetValue(col_idx, i).DefaultCastAs(child_type);
			ListVector::PushBack(result, val);
		}
		result_data[i].length = args.ColumnCount();
	}
	result.Verify(args.size());
}

static unique_ptr<FunctionData> ListValueBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	// collect names and deconflict, construct return type
	LogicalType child_type = arguments.empty() ? LogicalType::SQLNULL : arguments[0]->return_type;
	for (idx_t i = 1; i < arguments.size(); i++) {
		child_type = LogicalType::MaxLogicalType(child_type, arguments[i]->return_type);
	}

	// this is more for completeness reasons
	bound_function.varargs = child_type;
	bound_function.return_type = LogicalType::LIST(child_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> ListValueStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto list_stats = ListStats::CreateEmpty(expr.return_type);
	auto &list_child_stats = ListStats::GetChildStats(list_stats);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		list_child_stats.Merge(child_stats[i]);
	}
	return list_stats.ToUnique();
}

ScalarFunction ListValueFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("list_value", {}, LogicalTypeId::LIST, ListValueFunction, ListValueBind, nullptr,
	                   ListValueStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb






namespace duckdb {

struct NumericRangeInfo {
	using TYPE = int64_t;
	using INCREMENT_TYPE = int64_t;

	static int64_t DefaultStart() {
		return 0;
	}
	static int64_t DefaultIncrement() {
		return 1;
	}

	static uint64_t ListLength(int64_t start_value, int64_t end_value, int64_t increment_value, bool inclusive_bound) {
		if (increment_value == 0) {
			return 0;
		}
		if (start_value > end_value && increment_value > 0) {
			return 0;
		}
		if (start_value < end_value && increment_value < 0) {
			return 0;
		}
		hugeint_t total_diff = AbsValue(hugeint_t(end_value) - hugeint_t(start_value));
		hugeint_t increment = AbsValue(hugeint_t(increment_value));
		hugeint_t total_values = total_diff / increment;
		if (total_diff % increment == 0) {
			if (inclusive_bound) {
				total_values += 1;
			}
		} else {
			total_values += 1;
		}
		if (total_values > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Lists larger than 2^32 elements are not supported");
		}
		return Hugeint::Cast<uint64_t>(total_values);
	}

	static void Increment(int64_t &input, int64_t increment) {
		input += increment;
	}
};
struct TimestampRangeInfo {
	using TYPE = timestamp_t;
	using INCREMENT_TYPE = interval_t;

	static timestamp_t DefaultStart() {
		throw InternalException("Default start not implemented for timestamp range");
	}
	static interval_t DefaultIncrement() {
		throw InternalException("Default increment not implemented for timestamp range");
	}
	static uint64_t ListLength(timestamp_t start_value, timestamp_t end_value, interval_t increment_value,
	                           bool inclusive_bound) {
		bool is_positive = increment_value.months > 0 || increment_value.days > 0 || increment_value.micros > 0;
		bool is_negative = increment_value.months < 0 || increment_value.days < 0 || increment_value.micros < 0;
		if (!is_negative && !is_positive) {
			// interval is 0: no result
			return 0;
		}
		// We don't allow infinite bounds because they generate errors or infinite loops
		if (!Timestamp::IsFinite(start_value) || !Timestamp::IsFinite(end_value)) {
			throw InvalidInputException("Interval infinite bounds not supported");
		}

		if (is_negative && is_positive) {
			// we don't allow a mix of
			throw InvalidInputException("Interval with mix of negative/positive entries not supported");
		}
		if (start_value > end_value && is_positive) {
			return 0;
		}
		if (start_value < end_value && is_negative) {
			return 0;
		}
		int64_t total_values = 0;
		if (is_negative) {
			// negative interval, start_value is going down
			while (inclusive_bound ? start_value >= end_value : start_value > end_value) {
				start_value = Interval::Add(start_value, increment_value);
				total_values++;
				if (total_values > NumericLimits<uint32_t>::Maximum()) {
					throw InvalidInputException("Lists larger than 2^32 elements are not supported");
				}
			}
		} else {
			// positive interval, start_value is going up
			while (inclusive_bound ? start_value <= end_value : start_value < end_value) {
				start_value = Interval::Add(start_value, increment_value);
				total_values++;
				if (total_values > NumericLimits<uint32_t>::Maximum()) {
					throw InvalidInputException("Lists larger than 2^32 elements are not supported");
				}
			}
		}
		return total_values;
	}

	static void Increment(timestamp_t &input, interval_t increment) {
		input = Interval::Add(input, increment);
	}
};

template <class OP, bool INCLUSIVE_BOUND>
class RangeInfoStruct {
public:
	explicit RangeInfoStruct(DataChunk &args_p) : args(args_p) {
		switch (args.ColumnCount()) {
		case 1:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			break;
		case 2:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			args.data[1].ToUnifiedFormat(args.size(), vdata[1]);
			break;
		case 3:
			args.data[0].ToUnifiedFormat(args.size(), vdata[0]);
			args.data[1].ToUnifiedFormat(args.size(), vdata[1]);
			args.data[2].ToUnifiedFormat(args.size(), vdata[2]);
			break;
		default:
			throw InternalException("Unsupported number of parameters for range");
		}
	}

	bool RowIsValid(idx_t row_idx) {
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			auto idx = vdata[i].sel->get_index(row_idx);
			if (!vdata[i].validity.RowIsValid(idx)) {
				return false;
			}
		}
		return true;
	}

	typename OP::TYPE StartListValue(idx_t row_idx) {
		if (args.ColumnCount() == 1) {
			return OP::DefaultStart();
		} else {
			auto data = (typename OP::TYPE *)vdata[0].data;
			auto idx = vdata[0].sel->get_index(row_idx);
			return data[idx];
		}
	}

	typename OP::TYPE EndListValue(idx_t row_idx) {
		idx_t vdata_idx = args.ColumnCount() == 1 ? 0 : 1;
		auto data = (typename OP::TYPE *)vdata[vdata_idx].data;
		auto idx = vdata[vdata_idx].sel->get_index(row_idx);
		return data[idx];
	}

	typename OP::INCREMENT_TYPE ListIncrementValue(idx_t row_idx) {
		if (args.ColumnCount() < 3) {
			return OP::DefaultIncrement();
		} else {
			auto data = (typename OP::INCREMENT_TYPE *)vdata[2].data;
			auto idx = vdata[2].sel->get_index(row_idx);
			return data[idx];
		}
	}

	void GetListValues(idx_t row_idx, typename OP::TYPE &start_value, typename OP::TYPE &end_value,
	                   typename OP::INCREMENT_TYPE &increment_value) {
		start_value = StartListValue(row_idx);
		end_value = EndListValue(row_idx);
		increment_value = ListIncrementValue(row_idx);
	}

	uint64_t ListLength(idx_t row_idx) {
		typename OP::TYPE start_value;
		typename OP::TYPE end_value;
		typename OP::INCREMENT_TYPE increment_value;
		GetListValues(row_idx, start_value, end_value, increment_value);
		return OP::ListLength(start_value, end_value, increment_value, INCLUSIVE_BOUND);
	}

private:
	DataChunk &args;
	UnifiedVectorFormat vdata[3];
};

template <class OP, bool INCLUSIVE_BOUND>
static void ListRangeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	RangeInfoStruct<OP, INCLUSIVE_BOUND> info(args);
	idx_t args_size = 1;
	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			args_size = args.size();
			result_type = VectorType::FLAT_VECTOR;
			break;
		}
	}
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	int64_t total_size = 0;
	for (idx_t i = 0; i < args_size; i++) {
		if (!info.RowIsValid(i)) {
			result_validity.SetInvalid(i);
			list_data[i].offset = total_size;
			list_data[i].length = 0;
		} else {
			list_data[i].offset = total_size;
			list_data[i].length = info.ListLength(i);
			total_size += list_data[i].length;
		}
	}

	// now construct the child vector of the list
	ListVector::Reserve(result, total_size);
	auto range_data = FlatVector::GetData<typename OP::TYPE>(ListVector::GetEntry(result));
	idx_t total_idx = 0;
	for (idx_t i = 0; i < args_size; i++) {
		typename OP::TYPE start_value = info.StartListValue(i);
		typename OP::INCREMENT_TYPE increment = info.ListIncrementValue(i);

		typename OP::TYPE range_value = start_value;
		for (idx_t range_idx = 0; range_idx < list_data[i].length; range_idx++) {
			if (range_idx > 0) {
				OP::Increment(range_value, increment);
			}
			range_data[total_idx++] = range_value;
		}
	}

	ListVector::SetListSize(result, total_size);
	result.SetVectorType(result_type);

	result.Verify(args.size());
}

ScalarFunctionSet ListRangeFun::GetFunctions() {
	// the arguments and return types are actually set in the binder function
	ScalarFunctionSet range_set;
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::LIST(LogicalType::BIGINT),
	                                     ListRangeFunction<NumericRangeInfo, false>));
	range_set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                     LogicalType::LIST(LogicalType::TIMESTAMP),
	                                     ListRangeFunction<TimestampRangeInfo, false>));
	return range_set;
}

ScalarFunctionSet GenerateSeriesFun::GetFunctions() {
	ScalarFunctionSet generate_series;
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::LIST(LogicalType::BIGINT),
	                                           ListRangeFunction<NumericRangeInfo, true>));
	generate_series.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                           LogicalType::LIST(LogicalType::TIMESTAMP),
	                                           ListRangeFunction<TimestampRangeInfo, true>));
	return generate_series;
}

} // namespace duckdb







namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &map = args.data[0];
	UnifiedVectorFormat map_data;
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<uint64_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	map.ToUnifiedFormat(args.size(), map_data);
	for (idx_t row = 0; row < args.size(); row++) {
		auto list_entry = UnifiedVectorFormat::GetData<list_entry_t>(map_data)[map_data.sel->get_index(row)];
		result_data[row] = list_entry.length;
		result_validity.Set(row, map_data.validity.RowIsValid(map_data.sel->get_index(row)));
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> CardinalityBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw BinderException("Cardinality must have exactly one arguments");
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("Cardinality can only operate on MAPs");
	}

	bound_function.return_type = LogicalType::UBIGINT;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction CardinalityFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY}, LogicalType::UBIGINT, CardinalityFunction, CardinalityBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	return fun;
}

} // namespace duckdb









namespace duckdb {

// Example:
// source: [1,2,3], expansion_factor: 4
// target (result): [1,2,3,1,2,3,1,2,3,1,2,3]
static void CreateExpandedVector(const Vector &source, Vector &target, idx_t expansion_factor) {
	idx_t count = ListVector::GetListSize(source);
	auto &entry = ListVector::GetEntry(source);

	idx_t target_idx = 0;
	for (idx_t copy = 0; copy < expansion_factor; copy++) {
		for (idx_t key_idx = 0; key_idx < count; key_idx++) {
			target.SetValue(target_idx, entry.GetValue(key_idx));
			target_idx++;
		}
	}
	D_ASSERT(target_idx == count * expansion_factor);
}

static void AlignVectorToReference(const Vector &original, const Vector &reference, idx_t tuple_count, Vector &result) {
	auto original_length = ListVector::GetListSize(original);
	auto new_length = ListVector::GetListSize(reference);

	Vector expanded_const(ListType::GetChildType(original.GetType()), new_length);

	auto expansion_factor = new_length / original_length;
	if (expansion_factor != tuple_count) {
		throw InvalidInputException("Error in MAP creation: key list and value list do not align. i.e. different "
		                            "size or incompatible structure");
	}
	CreateExpandedVector(original, expanded_const, expansion_factor);
	result.Reference(expanded_const);
}

static bool ListEntriesEqual(Vector &keys, Vector &values, idx_t count) {
	auto key_count = ListVector::GetListSize(keys);
	auto value_count = ListVector::GetListSize(values);
	bool same_vector_type = keys.GetVectorType() == values.GetVectorType();

	D_ASSERT(keys.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(values.GetType().id() == LogicalTypeId::LIST);

	UnifiedVectorFormat keys_data;
	UnifiedVectorFormat values_data;

	keys.ToUnifiedFormat(count, keys_data);
	values.ToUnifiedFormat(count, values_data);

	auto keys_entries = UnifiedVectorFormat::GetData<list_entry_t>(keys_data);
	auto values_entries = UnifiedVectorFormat::GetData<list_entry_t>(values_data);

	if (same_vector_type) {
		const auto key_data = keys_data.data;
		const auto value_data = values_data.data;

		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			D_ASSERT(values.GetVectorType() == VectorType::CONSTANT_VECTOR);
			// Only need to compare one entry in this case
			return memcmp(key_data, value_data, sizeof(list_entry_t)) == 0;
		}

		// Fast path if the vector types are equal, can just check if the entries are the same
		if (key_count != value_count) {
			return false;
		}
		return memcmp(key_data, value_data, count * sizeof(list_entry_t)) == 0;
	}

	// Compare the list_entries one by one
	for (idx_t i = 0; i < count; i++) {
		auto keys_idx = keys_data.sel->get_index(i);
		auto values_idx = values_data.sel->get_index(i);

		if (keys_entries[keys_idx] != values_entries[values_idx]) {
			return false;
		}
	}
	return true;
}

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	auto &key_vector = MapVector::GetKeys(result);
	auto &value_vector = MapVector::GetValues(result);
	auto result_data = ListVector::GetData(result);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	if (args.data.empty()) {
		ListVector::SetListSize(result, 0);
		result_data->offset = 0;
		result_data->length = 0;
		result.Verify(args.size());
		return;
	}

	bool keys_are_const = args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR;
	bool values_are_const = args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (!keys_are_const || !values_are_const) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}

	auto key_count = ListVector::GetListSize(args.data[0]);
	auto value_count = ListVector::GetListSize(args.data[1]);
	auto key_data = ListVector::GetData(args.data[0]);
	auto value_data = ListVector::GetData(args.data[1]);
	auto src_data = key_data;

	if (keys_are_const && !values_are_const) {
		AlignVectorToReference(args.data[0], args.data[1], args.size(), key_vector);
		src_data = value_data;
	} else if (values_are_const && !keys_are_const) {
		AlignVectorToReference(args.data[1], args.data[0], args.size(), value_vector);
	} else {
		if (!ListEntriesEqual(args.data[0], args.data[1], args.size())) {
			throw InvalidInputException("Error in MAP creation: key list and value list do not align. i.e. different "
			                            "size or incompatible structure");
		}
	}

	ListVector::SetListSize(result, MaxValue(key_count, value_count));

	result_data = ListVector::GetData(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = src_data[i];
	}

	// check whether one of the vectors has already been referenced to an expanded vector in the case of const/non-const
	// combination. If not, then referencing is still necessary
	if (!(keys_are_const && !values_are_const)) {
		key_vector.Reference(ListVector::GetEntry(args.data[0]));
	}
	if (!(values_are_const && !keys_are_const)) {
		value_vector.Reference(ListVector::GetEntry(args.data[1]));
	}

	MapVector::MapConversionVerify(result, args.size());
	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 2 && !arguments.empty()) {
		throw Exception("We need exactly two lists for a map");
	}
	if (arguments.size() == 2) {
		if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("First argument is not a list");
		}
		if (arguments[1]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("Second argument is not a list");
		}
		child_types.push_back(make_pair("key", arguments[0]->return_type));
		child_types.push_back(make_pair("value", arguments[1]->return_type));
	}

	if (arguments.empty()) {
		auto empty = LogicalType::LIST(LogicalTypeId::SQLNULL);
		child_types.push_back(make_pair("key", empty));
		child_types.push_back(make_pair("value", empty));
	}

	bound_function.return_type =
	    LogicalType::MAP(ListType::GetChildType(child_types[0].second), ListType::GetChildType(child_types[1].second));

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::MAP, MapFunction, MapBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb










namespace duckdb {

namespace {

struct MapKeyIndexPair {
	MapKeyIndexPair(idx_t map, idx_t key) : map_index(map), key_index(key) {
	}
	// The index of the map that this key comes from
	idx_t map_index;
	// The index within the maps key_list
	idx_t key_index;
};

} // namespace

vector<Value> GetListEntries(vector<Value> keys, vector<Value> values) {
	D_ASSERT(keys.size() == values.size());
	vector<Value> entries;
	for (idx_t i = 0; i < keys.size(); i++) {
		child_list_t<Value> children;
		children.emplace_back(make_pair("key", std::move(keys[i])));
		children.emplace_back(make_pair("value", std::move(values[i])));
		entries.push_back(Value::STRUCT(std::move(children)));
	}
	return entries;
}

static void MapConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		// All inputs are NULL, just return NULL
		auto &validity = FlatVector::Validity(result);
		validity.SetInvalid(0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);
	auto count = args.size();

	auto map_count = args.ColumnCount();
	vector<UnifiedVectorFormat> map_formats(map_count);
	for (idx_t i = 0; i < map_count; i++) {
		auto &map = args.data[i];
		map.ToUnifiedFormat(count, map_formats[i]);
	}
	auto result_data = FlatVector::GetData<list_entry_t>(result);

	for (idx_t i = 0; i < count; i++) {
		// Loop through all the maps per list
		// we cant do better because all the entries of the child vector have to be contiguous
		// so we cant start the next row before we have finished the one before it
		auto &result_entry = result_data[i];
		vector<MapKeyIndexPair> index_to_map;
		vector<Value> keys_list;
		for (idx_t map_idx = 0; map_idx < map_count; map_idx++) {
			if (args.data[map_idx].GetType().id() == LogicalTypeId::SQLNULL) {
				continue;
			}
			auto &map_format = map_formats[map_idx];
			auto &keys = MapVector::GetKeys(args.data[map_idx]);

			auto index = map_format.sel->get_index(i);
			auto entry = UnifiedVectorFormat::GetData<list_entry_t>(map_format)[index];

			// Update the list for this row
			for (idx_t list_idx = 0; list_idx < entry.length; list_idx++) {
				auto key_index = entry.offset + list_idx;
				auto key = keys.GetValue(key_index);
				auto entry = std::find(keys_list.begin(), keys_list.end(), key);
				if (entry == keys_list.end()) {
					// Result list does not contain this value yet
					keys_list.push_back(key);
					index_to_map.emplace_back(map_idx, key_index);
				} else {
					// Result list already contains this, update where to find the value at
					auto distance = std::distance(keys_list.begin(), entry);
					auto &mapping = *(index_to_map.begin() + distance);
					mapping.key_index = key_index;
					mapping.map_index = map_idx;
				}
			}
		}
		vector<Value> values_list;
		D_ASSERT(keys_list.size() == index_to_map.size());
		// Get the values from the mapping
		for (auto &mapping : index_to_map) {
			auto &map = args.data[mapping.map_index];
			auto &values = MapVector::GetValues(map);
			values_list.push_back(values.GetValue(mapping.key_index));
		}
		D_ASSERT(values_list.size() == keys_list.size());
		result_entry.offset = ListVector::GetListSize(result);
		result_entry.length = values_list.size();
		auto list_entries = GetListEntries(std::move(keys_list), std::move(values_list));
		for (auto &list_entry : list_entries) {
			ListVector::PushBack(result, list_entry);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static bool IsEmptyMap(const LogicalType &map) {
	D_ASSERT(map.id() == LogicalTypeId::MAP);
	auto &key_type = MapType::KeyType(map);
	auto &value_type = MapType::ValueType(map);
	return key_type.id() == LogicalType::SQLNULL && value_type.id() == LogicalType::SQLNULL;
}

static unique_ptr<FunctionData> MapConcatBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {

	auto arg_count = arguments.size();
	if (arg_count < 2) {
		throw InvalidInputException("The provided amount of arguments is incorrect, please provide 2 or more maps");
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	LogicalType expected = LogicalType::SQLNULL;

	bool is_null = true;
	// Check and verify that all the maps are of the same type
	for (idx_t i = 0; i < arg_count; i++) {
		auto &arg = arguments[i];
		auto &map = arg->return_type;
		if (map.id() == LogicalTypeId::UNKNOWN) {
			// Prepared statement
			bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
			bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
			return nullptr;
		}
		if (map.id() == LogicalTypeId::SQLNULL) {
			// The maps are allowed to be NULL
			continue;
		}
		if (map.id() != LogicalTypeId::MAP) {
			throw InvalidInputException("MAP_CONCAT only takes map arguments");
		}
		is_null = false;
		if (IsEmptyMap(map)) {
			// Map is allowed to be empty
			continue;
		}

		if (expected.id() == LogicalTypeId::SQLNULL) {
			expected = map;
		} else if (map != expected) {
			throw InvalidInputException(
			    "'value' type of map differs between arguments, expected '%s', found '%s' instead", expected.ToString(),
			    map.ToString());
		}
	}

	if (expected.id() == LogicalTypeId::SQLNULL && is_null == false) {
		expected = LogicalType::MAP(LogicalType::SQLNULL, LogicalType::SQLNULL);
	}
	bound_function.return_type = expected;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapConcatFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_concat", {}, LogicalTypeId::LIST, MapConcatFunction, MapConcatBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb








namespace duckdb {

// Reverse of map_from_entries
static void MapEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();

	result.Reinterpret(args.data[0]);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static unique_ptr<FunctionData> MapEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("Too many arguments provided, only expecting a single map");
	}
	auto &map = arguments[0]->return_type;

	if (map.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}
	auto &key_type = MapType::KeyType(map);
	auto &value_type = MapType::ValueType(map);

	child_types.push_back(make_pair("key", key_type));
	child_types.push_back(make_pair("value", value_type));

	auto row_type = LogicalType::STRUCT(child_types);

	bound_function.return_type = LogicalType::LIST(row_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapEntriesFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::LIST, MapEntriesFunction, MapEntriesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb







namespace duckdb {

struct MapKeyArgFunctor {
	// MAP is a LIST(STRUCT(K,V))
	// meaning the MAP itself is a List, but the child vector that we're interested in (the keys)
	// are a level deeper than the initial child vector

	static Vector &GetList(Vector &map) {
		return map;
	}
	static idx_t GetListSize(Vector &map) {
		return ListVector::GetListSize(map);
	}
	static Vector &GetEntry(Vector &map) {
		return MapVector::GetKeys(map);
	}
};

void FillResult(Vector &map, Vector &offsets, Vector &result, idx_t count) {
	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);

	UnifiedVectorFormat offset_data;
	offsets.ToUnifiedFormat(count, offset_data);

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto entry_count = ListVector::GetListSize(map);
	auto &values_entries = MapVector::GetValues(map);
	UnifiedVectorFormat values_entry_data;
	// Note: this vector can have a different size than the map
	values_entries.ToUnifiedFormat(entry_count, values_entry_data);

	for (idx_t row = 0; row < count; row++) {
		idx_t offset_idx = offset_data.sel->get_index(row);
		auto offset = UnifiedVectorFormat::GetData<int32_t>(offset_data)[offset_idx];

		// Get the current size of the list, for the offset
		idx_t current_offset = ListVector::GetListSize(result);
		if (!offset_data.validity.RowIsValid(offset_idx) || !offset) {
			// Set the entry data for this result row
			auto &entry = result_data[row];
			entry.length = 0;
			entry.offset = current_offset;
			continue;
		}
		// All list indices start at 1, reduce by 1 to get the actual index
		offset--;

		// Get the 'values' list entry corresponding to the offset
		idx_t value_index = map_data.sel->get_index(row);
		auto &value_list_entry = UnifiedVectorFormat::GetData<list_entry_t>(map_data)[value_index];

		// Add the values to the result
		idx_t list_offset = value_list_entry.offset + offset;
		// All keys are unique, only one will ever match
		idx_t length = 1;
		ListVector::Append(result, values_entries, length + list_offset, list_offset);

		// Set the entry data for this result row
		auto &entry = result_data[row];
		entry.length = length;
		entry.offset = current_offset;
	}
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t tuple_count = args.size();
	// Optimization: because keys are not allowed to be NULL, we can early-out
	if (args.data[1].GetType().id() == LogicalTypeId::SQLNULL) {
		//! We don't need to look through the map if the 'key' to look for is NULL
		ListVector::SetListSize(result, 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(result);
		list_data->offset = 0;
		list_data->length = 0;
		result.Verify(tuple_count);
		return;
	}

	auto &map = args.data[0];
	auto &key = args.data[1];

	UnifiedVectorFormat map_data;

	// Create the chunk we'll feed to ListPosition
	DataChunk list_position_chunk;
	vector<LogicalType> chunk_types;
	chunk_types.reserve(2);
	chunk_types.push_back(map.GetType());
	chunk_types.push_back(key.GetType());
	list_position_chunk.InitializeEmpty(chunk_types.begin(), chunk_types.end());

	// Populate it with the map keys list and the key vector
	list_position_chunk.data[0].Reference(map);
	list_position_chunk.data[1].Reference(key);
	list_position_chunk.SetCardinality(tuple_count);

	Vector position_vector(LogicalType::LIST(LogicalType::INTEGER), tuple_count);
	// We can pass around state as it's not used by ListPositionFunction anyways
	ListContainsOrPosition<int32_t, PositionFunctor, MapKeyArgFunctor>(list_position_chunk, position_vector);

	FillResult(map, position_vector, result, tuple_count);

	if (tuple_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(tuple_count);
}

static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("MAP_EXTRACT must have exactly two arguments");
	}
	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("MAP_EXTRACT can only operate on MAPs");
	}
	auto &value_type = MapType::ValueType(arguments[0]->return_type);

	//! Here we have to construct the List Type that will be returned
	bound_function.return_type = LogicalType::LIST(value_type);
	auto key_type = MapType::KeyType(arguments[0]->return_type);
	if (key_type.id() != LogicalTypeId::SQLNULL && arguments[1]->return_type.id() != LogicalTypeId::SQLNULL) {
		bound_function.arguments[1] = MapType::KeyType(arguments[0]->return_type);
	}
	return make_uniq<VariableReturnBindData>(value_type);
}

ScalarFunction MapExtractFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunction, MapExtractBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb







namespace duckdb {

static void MapFromEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	result.Reinterpret(args.data[0]);

	MapVector::MapConversionVerify(result, count);
	result.Verify(count);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> MapFromEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 1) {
		throw InvalidInputException("The input argument must be a list of structs.");
	}
	auto &list = arguments[0]->return_type;

	if (list.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (list.id() != LogicalTypeId::LIST) {
		throw InvalidInputException("The provided argument is not a list of structs");
	}
	auto &elem_type = ListType::GetChildType(list);
	if (elem_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The elements of the list must be structs");
	}
	auto &children = StructType::GetChildTypes(elem_type);
	if (children.size() != 2) {
		throw InvalidInputException("The provided struct type should only contain 2 fields, a key and a value");
	}

	bound_function.return_type = LogicalType::MAP(elem_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapFromEntriesFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::MAP, MapFromEntriesFunction, MapFromEntriesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb








namespace duckdb {

static void MapKeyValueFunction(DataChunk &args, ExpressionState &state, Vector &result,
                                Vector &(*get_child_vector)(Vector &)) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();

	auto &map = args.data[0];
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	auto child = get_child_vector(map);

	auto &entries = ListVector::GetEntry(result);
	entries.Reference(child);

	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_data.data);
	FlatVector::SetValidity(result, map_data.validity);
	auto list_size = ListVector::GetListSize(map);
	ListVector::SetListSize(result, list_size);
	if (map.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		result.Slice(*map_data.sel, count);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static void MapKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetKeys);
}

static void MapValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetValues);
}

static unique_ptr<FunctionData> MapKeyValueBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments,
                                                const LogicalType &(*type_func)(const LogicalType &)) {
	if (arguments.size() != 1) {
		throw InvalidInputException("Too many arguments provided, only expecting a single map");
	}
	auto &map = arguments[0]->return_type;

	if (map.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}

	auto &type = type_func(map);

	bound_function.return_type = LogicalType::LIST(type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<FunctionData> MapKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	return MapKeyValueBind(context, bound_function, arguments, MapType::KeyType);
}

static unique_ptr<FunctionData> MapValuesBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	return MapKeyValueBind(context, bound_function, arguments, MapType::ValueType);
}

ScalarFunction MapKeysFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::LIST, MapKeysFunction, MapKeysBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

ScalarFunction MapValuesFun::GetFunction() {
	ScalarFunction fun({}, LogicalTypeId::LIST, MapValuesFunction, MapValuesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb











#include <cmath>
#include <errno.h>

namespace duckdb {

template <class TR, class OP>
static scalar_function_t GetScalarIntegerUnaryFunctionFixedReturn(const LogicalType &type) {
	scalar_function_t function;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		function = &ScalarFunction::UnaryFunction<int8_t, TR, OP>;
		break;
	case LogicalTypeId::SMALLINT:
		function = &ScalarFunction::UnaryFunction<int16_t, TR, OP>;
		break;
	case LogicalTypeId::INTEGER:
		function = &ScalarFunction::UnaryFunction<int32_t, TR, OP>;
		break;
	case LogicalTypeId::BIGINT:
		function = &ScalarFunction::UnaryFunction<int64_t, TR, OP>;
		break;
	case LogicalTypeId::HUGEINT:
		function = &ScalarFunction::UnaryFunction<hugeint_t, TR, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerUnaryFunctionFixedReturn");
	}
	return function;
}

//===--------------------------------------------------------------------===//
// nextafter
//===--------------------------------------------------------------------===//
struct NextAfterOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA base, TB exponent) {
		throw NotImplementedException("Unimplemented type for NextAfter Function");
	}

	template <class TA, class TB, class TR>
	static inline double Operation(double input, double approximate_to) {
		return nextafter(input, approximate_to);
	}
	template <class TA, class TB, class TR>
	static inline float Operation(float input, float approximate_to) {
		return nextafterf(input, approximate_to);
	}
};

ScalarFunctionSet NextAfterFun::GetFunctions() {
	ScalarFunctionSet next_after_fun;
	next_after_fun.AddFunction(
	    ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                   ScalarFunction::BinaryFunction<double, double, double, NextAfterOperator>));
	next_after_fun.AddFunction(ScalarFunction({LogicalType::FLOAT, LogicalType::FLOAT}, LogicalType::FLOAT,
	                                          ScalarFunction::BinaryFunction<float, float, float, NextAfterOperator>));
	return next_after_fun;
}

//===--------------------------------------------------------------------===//
// abs
//===--------------------------------------------------------------------===//
static unique_ptr<BaseStatistics> PropagateAbsStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 1);
	// can only propagate stats if the children have stats
	auto &lstats = child_stats[0];
	Value new_min, new_max;
	bool potential_overflow = true;
	if (NumericStats::HasMinMax(lstats)) {
		switch (expr.return_type.InternalType()) {
		case PhysicalType::INT8:
			potential_overflow = NumericStats::Min(lstats).GetValue<int8_t>() == NumericLimits<int8_t>::Minimum();
			break;
		case PhysicalType::INT16:
			potential_overflow = NumericStats::Min(lstats).GetValue<int16_t>() == NumericLimits<int16_t>::Minimum();
			break;
		case PhysicalType::INT32:
			potential_overflow = NumericStats::Min(lstats).GetValue<int32_t>() == NumericLimits<int32_t>::Minimum();
			break;
		case PhysicalType::INT64:
			potential_overflow = NumericStats::Min(lstats).GetValue<int64_t>() == NumericLimits<int64_t>::Minimum();
			break;
		default:
			return nullptr;
		}
	}
	if (potential_overflow) {
		new_min = Value(expr.return_type);
		new_max = Value(expr.return_type);
	} else {
		// no potential overflow

		// compute stats
		auto current_min = NumericStats::Min(lstats).GetValue<int64_t>();
		auto current_max = NumericStats::Max(lstats).GetValue<int64_t>();

		int64_t min_val, max_val;

		if (current_min < 0 && current_max < 0) {
			// if both min and max are below zero, then min=abs(cur_max) and max=abs(cur_min)
			min_val = AbsValue(current_max);
			max_val = AbsValue(current_min);
		} else if (current_min < 0) {
			D_ASSERT(current_max >= 0);
			// if min is below zero and max is above 0, then min=0 and max=max(cur_max, abs(cur_min))
			min_val = 0;
			max_val = MaxValue(AbsValue(current_min), current_max);
		} else {
			// if both current_min and current_max are > 0, then the abs is a no-op and can be removed entirely
			*input.expr_ptr = std::move(input.expr.children[0]);
			return child_stats[0].ToUnique();
		}
		new_min = Value::Numeric(expr.return_type, min_val);
		new_max = Value::Numeric(expr.return_type, max_val);
		expr.function.function = ScalarFunction::GetScalarUnaryFunction<AbsOperator>(expr.return_type);
	}
	auto stats = NumericStats::CreateEmpty(expr.return_type);
	NumericStats::SetMin(stats, new_min);
	NumericStats::SetMax(stats, new_max);
	stats.CopyValidity(lstats);
	return stats.ToUnique();
}

template <class OP>
unique_ptr<FunctionData> DecimalUnaryOpBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::SMALLINT);
		break;
	case PhysicalType::INT32:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::INTEGER);
		break;
	case PhysicalType::INT64:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::BIGINT);
		break;
	default:
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<OP>(LogicalTypeId::HUGEINT);
		break;
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = decimal_type;
	return nullptr;
}

ScalarFunctionSet AbsOperatorFun::GetFunctions() {
	ScalarFunctionSet abs;
	for (auto &type : LogicalType::Numeric()) {
		switch (type.id()) {
		case LogicalTypeId::DECIMAL:
			abs.AddFunction(ScalarFunction({type}, type, nullptr, DecimalUnaryOpBind<AbsOperator>));
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT: {
			ScalarFunction func({type}, type, ScalarFunction::GetScalarUnaryFunction<TryAbsOperator>(type));
			func.statistics = PropagateAbsStats;
			abs.AddFunction(func);
			break;
		}
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
			break;
		default:
			abs.AddFunction(ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<AbsOperator>(type)));
			break;
		}
	}
	return abs;
}

//===--------------------------------------------------------------------===//
// bit_count
//===--------------------------------------------------------------------===//
struct BitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		using TU = typename std::make_unsigned<TA>::type;
		TR count = 0;
		for (auto value = TU(input); value; ++count) {
			value &= (value - 1);
		}
		return count;
	}
};

struct HugeIntBitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		using TU = typename std::make_unsigned<int64_t>::type;
		TR count = 0;

		for (auto value = TU(input.upper); value; ++count) {
			value &= (value - 1);
		}
		for (auto value = TU(input.lower); value; ++count) {
			value &= (value - 1);
		}
		return count;
	}
};

struct BitStringBitCntOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		TR count = Bit::BitCount(input);
		return count;
	}
};

ScalarFunctionSet BitCountFun::GetFunctions() {
	ScalarFunctionSet functions;
	functions.AddFunction(ScalarFunction({LogicalType::TINYINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int8_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::SMALLINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int16_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int32_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<int64_t, int8_t, BitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::TINYINT,
	                                     ScalarFunction::UnaryFunction<hugeint_t, int8_t, HugeIntBitCntOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIGINT,
	                                     ScalarFunction::UnaryFunction<string_t, idx_t, BitStringBitCntOperator>));
	return functions;
}

//===--------------------------------------------------------------------===//
// sign
//===--------------------------------------------------------------------===//
struct SignOperator {
	template <class TA, class TR>
	static TR Operation(TA input) {
		if (input == TA(0)) {
			return 0;
		} else if (input > TA(0)) {
			return 1;
		} else {
			return -1;
		}
	}
};

template <>
int8_t SignOperator::Operation(float input) {
	if (input == 0 || Value::IsNan(input)) {
		return 0;
	} else if (input > 0) {
		return 1;
	} else {
		return -1;
	}
}

template <>
int8_t SignOperator::Operation(double input) {
	if (input == 0 || Value::IsNan(input)) {
		return 0;
	} else if (input > 0) {
		return 1;
	} else {
		return -1;
	}
}

ScalarFunctionSet SignFun::GetFunctions() {
	ScalarFunctionSet sign;
	for (auto &type : LogicalType::Numeric()) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			sign.AddFunction(
			    ScalarFunction({type}, LogicalType::TINYINT,
			                   ScalarFunction::GetScalarUnaryFunctionFixedReturn<int8_t, SignOperator>(type)));
		}
	}
	return sign;
}

//===--------------------------------------------------------------------===//
// ceil
//===--------------------------------------------------------------------===//
struct CeilOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::ceil(left);
	}
};

template <class T, class POWERS_OF_TEN, class OP>
static void GenericRoundFunctionDecimal(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	OP::template Operation<T, POWERS_OF_TEN>(input, DecimalType::GetScale(func_expr.children[0]->return_type), result);
}

template <class OP>
unique_ptr<FunctionData> BindGenericRoundFunctionDecimal(ClientContext &context, ScalarFunction &bound_function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	// ceil essentially removes the scale
	auto &decimal_type = arguments[0]->return_type;
	auto scale = DecimalType::GetScale(decimal_type);
	auto width = DecimalType::GetWidth(decimal_type);
	if (scale == 0) {
		bound_function.function = ScalarFunction::NopFunction;
	} else {
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT16:
			bound_function.function = GenericRoundFunctionDecimal<int16_t, NumericHelper, OP>;
			break;
		case PhysicalType::INT32:
			bound_function.function = GenericRoundFunctionDecimal<int32_t, NumericHelper, OP>;
			break;
		case PhysicalType::INT64:
			bound_function.function = GenericRoundFunctionDecimal<int64_t, NumericHelper, OP>;
			break;
		default:
			bound_function.function = GenericRoundFunctionDecimal<hugeint_t, Hugeint, OP>;
			break;
		}
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = LogicalType::DECIMAL(width, 0);
	return nullptr;
}

struct CeilDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale];
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				// below 0 we floor the number (e.g. -10.5 -> -10)
				return input / power_of_ten;
			} else {
				// above 0 we ceil the number
				return ((input - 1) / power_of_ten) + 1;
			}
		});
	}
};

ScalarFunctionSet CeilFun::GetFunctions() {
	ScalarFunctionSet ceil;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no ceil for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, CeilOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, CeilOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<CeilDecimalOperator>;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"ceil\"");
		}
		ceil.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return ceil;
}

//===--------------------------------------------------------------------===//
// floor
//===--------------------------------------------------------------------===//
struct FloorOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::floor(left);
	}
};

struct FloorDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale];
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				// below 0 we ceil the number (e.g. -10.5 -> -11)
				return ((input + 1) / power_of_ten) - 1;
			} else {
				// above 0 we floor the number
				return input / power_of_ten;
			}
		});
	}
};

ScalarFunctionSet FloorFun::GetFunctions() {
	ScalarFunctionSet floor;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		if (type.IsIntegral()) {
			// no floor for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, FloorOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, FloorOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<FloorDecimalOperator>;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"floor\"");
		}
		floor.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return floor;
}

//===--------------------------------------------------------------------===//
// trunc
//===--------------------------------------------------------------------===//
struct TruncOperator {
	// Integer truncation is a NOP
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::trunc(left);
	}
};

struct TruncDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale];
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			//	Always floor
			return (input / power_of_ten);
		});
	}
};

ScalarFunctionSet TruncFun::GetFunctions() {
	ScalarFunctionSet trunc;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		//	Truncation of integers gets generated by some tools (e.g., Tableau/JDBC:Postgres)
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			func = ScalarFunction::UnaryFunction<float, float, TruncOperator>;
			break;
		case LogicalTypeId::DOUBLE:
			func = ScalarFunction::UnaryFunction<double, double, TruncOperator>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<TruncDecimalOperator>;
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			func = ScalarFunction::NopFunction;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"trunc\"");
		}
		trunc.AddFunction(ScalarFunction({type}, type, func, bind_func));
	}
	return trunc;
}

//===--------------------------------------------------------------------===//
// round
//===--------------------------------------------------------------------===//
struct RoundOperatorPrecision {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB precision) {
		double rounded_value;
		if (precision < 0) {
			double modifier = std::pow(10, -TA(precision));
			rounded_value = (std::round(input / modifier)) * modifier;
			if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
				return 0;
			}
		} else {
			double modifier = std::pow(10, TA(precision));
			rounded_value = (std::round(input * modifier)) / modifier;
			if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
				return input;
			}
		}
		return rounded_value;
	}
};

struct RoundOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		double rounded_value = round(input);
		if (std::isinf(rounded_value) || std::isnan(rounded_value)) {
			return input;
		}
		return rounded_value;
	}
};

struct RoundDecimalOperator {
	template <class T, class POWERS_OF_TEN_CLASS>
	static void Operation(DataChunk &input, uint8_t scale, Vector &result) {
		T power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[scale];
		T addition = power_of_ten / 2;
		// regular round rounds towards the nearest number
		// in case of a tie we round away from zero
		// i.e. -10.5 -> -11, 10.5 -> 11
		// we implement this by adding (positive) or subtracting (negative) 0.5
		// and then flooring the number
		// e.g. 10.5 + 0.5 = 11, floor(11) = 11
		//      10.4 + 0.5 = 10.9, floor(10.9) = 10
		UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
			if (input < 0) {
				input -= addition;
			} else {
				input += addition;
			}
			return input / power_of_ten;
		});
	}
};

struct RoundPrecisionFunctionData : public FunctionData {
	explicit RoundPrecisionFunctionData(int32_t target_scale) : target_scale(target_scale) {
	}

	int32_t target_scale;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RoundPrecisionFunctionData>(target_scale);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RoundPrecisionFunctionData>();
		return target_scale == other.target_scale;
	}
};

template <class T, class POWERS_OF_TEN_CLASS>
static void DecimalRoundNegativePrecisionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RoundPrecisionFunctionData>();
	auto source_scale = DecimalType::GetScale(func_expr.children[0]->return_type);
	auto width = DecimalType::GetWidth(func_expr.children[0]->return_type);
	if (info.target_scale <= -int32_t(width)) {
		// scale too big for width
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		result.SetValue(0, Value::INTEGER(0));
		return;
	}
	T divide_power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[-info.target_scale + source_scale];
	T multiply_power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[-info.target_scale];
	T addition = divide_power_of_ten / 2;

	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return input / divide_power_of_ten * multiply_power_of_ten;
	});
}

template <class T, class POWERS_OF_TEN_CLASS>
static void DecimalRoundPositivePrecisionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RoundPrecisionFunctionData>();
	auto source_scale = DecimalType::GetScale(func_expr.children[0]->return_type);
	T power_of_ten = POWERS_OF_TEN_CLASS::POWERS_OF_TEN[source_scale - info.target_scale];
	T addition = power_of_ten / 2;
	UnaryExecutor::Execute<T, T>(input.data[0], result, input.size(), [&](T input) {
		if (input < 0) {
			input -= addition;
		} else {
			input += addition;
		}
		return input / power_of_ten;
	});
}

unique_ptr<FunctionData> BindDecimalRoundPrecision(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto &decimal_type = arguments[0]->return_type;
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
	}
	Value val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]).DefaultCastAs(LogicalType::INTEGER);
	if (val.IsNull()) {
		throw NotImplementedException("ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");
	}
	// our new precision becomes the round value
	// e.g. ROUND(DECIMAL(18,3), 1) -> DECIMAL(18,1)
	// but ONLY if the round value is positive
	// if it is negative the scale becomes zero
	// i.e. ROUND(DECIMAL(18,3), -1) -> DECIMAL(18,0)
	int32_t round_value = IntegerValue::Get(val);
	uint8_t target_scale;
	auto width = DecimalType::GetWidth(decimal_type);
	auto scale = DecimalType::GetScale(decimal_type);
	if (round_value < 0) {
		target_scale = 0;
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT16:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int16_t, NumericHelper>;
			break;
		case PhysicalType::INT32:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int32_t, NumericHelper>;
			break;
		case PhysicalType::INT64:
			bound_function.function = DecimalRoundNegativePrecisionFunction<int64_t, NumericHelper>;
			break;
		default:
			bound_function.function = DecimalRoundNegativePrecisionFunction<hugeint_t, Hugeint>;
			break;
		}
	} else {
		if (round_value >= (int32_t)scale) {
			// if round_value is bigger than or equal to scale we do nothing
			bound_function.function = ScalarFunction::NopFunction;
			target_scale = scale;
		} else {
			target_scale = round_value;
			switch (decimal_type.InternalType()) {
			case PhysicalType::INT16:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int16_t, NumericHelper>;
				break;
			case PhysicalType::INT32:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int32_t, NumericHelper>;
				break;
			case PhysicalType::INT64:
				bound_function.function = DecimalRoundPositivePrecisionFunction<int64_t, NumericHelper>;
				break;
			default:
				bound_function.function = DecimalRoundPositivePrecisionFunction<hugeint_t, Hugeint>;
				break;
			}
		}
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = LogicalType::DECIMAL(width, target_scale);
	return make_uniq<RoundPrecisionFunctionData>(round_value);
}

ScalarFunctionSet RoundFun::GetFunctions() {
	ScalarFunctionSet round;
	for (auto &type : LogicalType::Numeric()) {
		scalar_function_t round_prec_func = nullptr;
		scalar_function_t round_func = nullptr;
		bind_scalar_function_t bind_func = nullptr;
		bind_scalar_function_t bind_prec_func = nullptr;
		if (type.IsIntegral()) {
			// no round for integral numbers
			continue;
		}
		switch (type.id()) {
		case LogicalTypeId::FLOAT:
			round_func = ScalarFunction::UnaryFunction<float, float, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<float, int32_t, float, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DOUBLE:
			round_func = ScalarFunction::UnaryFunction<double, double, RoundOperator>;
			round_prec_func = ScalarFunction::BinaryFunction<double, int32_t, double, RoundOperatorPrecision>;
			break;
		case LogicalTypeId::DECIMAL:
			bind_func = BindGenericRoundFunctionDecimal<RoundDecimalOperator>;
			bind_prec_func = BindDecimalRoundPrecision;
			break;
		default:
			throw InternalException("Unimplemented numeric type for function \"floor\"");
		}
		round.AddFunction(ScalarFunction({type}, type, round_func, bind_func));
		round.AddFunction(ScalarFunction({type, LogicalType::INTEGER}, type, round_prec_func, bind_prec_func));
	}
	return round;
}

//===--------------------------------------------------------------------===//
// exp
//===--------------------------------------------------------------------===//
struct ExpOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::exp(left);
	}
};

ScalarFunction ExpFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, ExpOperator>);
}

//===--------------------------------------------------------------------===//
// pow
//===--------------------------------------------------------------------===//
struct PowOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA base, TB exponent) {
		return std::pow(base, exponent);
	}
};

ScalarFunction PowOperatorFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::BinaryFunction<double, double, double, PowOperator>);
}

//===--------------------------------------------------------------------===//
// sqrt
//===--------------------------------------------------------------------===//
struct SqrtOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take square root of a negative number");
		}
		return std::sqrt(input);
	}
};

ScalarFunction SqrtFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, SqrtOperator>);
}

//===--------------------------------------------------------------------===//
// cbrt
//===--------------------------------------------------------------------===//
struct CbRtOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return std::cbrt(left);
	}
};

ScalarFunction CbrtFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, CbRtOperator>);
}

//===--------------------------------------------------------------------===//
// ln
//===--------------------------------------------------------------------===//

struct LnOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log(input);
	}
};

ScalarFunction LnFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, LnOperator>);
}

//===--------------------------------------------------------------------===//
// log
//===--------------------------------------------------------------------===//
struct Log10Operator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log10(input);
	}
};

ScalarFunction Log10Fun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, Log10Operator>);
}

//===--------------------------------------------------------------------===//
// log2
//===--------------------------------------------------------------------===//
struct Log2Operator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < 0) {
			throw OutOfRangeException("cannot take logarithm of a negative number");
		}
		if (input == 0) {
			throw OutOfRangeException("cannot take logarithm of zero");
		}
		return std::log2(input);
	}
};

ScalarFunction Log2Fun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, Log2Operator>);
}

//===--------------------------------------------------------------------===//
// pi
//===--------------------------------------------------------------------===//
static void PiFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	Value pi_value = Value::DOUBLE(PI);
	result.Reference(pi_value);
}

ScalarFunction PiFun::GetFunction() {
	return ScalarFunction({}, LogicalType::DOUBLE, PiFunction);
}

//===--------------------------------------------------------------------===//
// degrees
//===--------------------------------------------------------------------===//
struct DegreesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return left * (180 / PI);
	}
};

ScalarFunction DegreesFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, DegreesOperator>);
}

//===--------------------------------------------------------------------===//
// radians
//===--------------------------------------------------------------------===//
struct RadiansOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return left * (PI / 180);
	}
};

ScalarFunction RadiansFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, RadiansOperator>);
}

//===--------------------------------------------------------------------===//
// isnan
//===--------------------------------------------------------------------===//
struct IsNanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Value::IsNan(input);
	}
};

ScalarFunctionSet IsNanFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsNanOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsNanOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// signbit
//===--------------------------------------------------------------------===//
struct SignBitOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return std::signbit(input);
	}
};

ScalarFunctionSet SignBitFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, SignBitOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, SignBitOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// isinf
//===--------------------------------------------------------------------===//
struct IsInfiniteOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return !Value::IsNan(input) && !Value::IsFinite(input);
	}
};

template <>
bool IsInfiniteOperator::Operation(date_t input) {
	return !Value::IsFinite(input);
}

template <>
bool IsInfiniteOperator::Operation(timestamp_t input) {
	return !Value::IsFinite(input);
}

ScalarFunctionSet IsInfiniteFun::GetFunctions() {
	ScalarFunctionSet funcs("isinf");
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<date_t, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsInfiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsInfiniteOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// isfinite
//===--------------------------------------------------------------------===//
struct IsFiniteOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Value::IsFinite(input);
	}
};

ScalarFunctionSet IsFiniteFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(ScalarFunction({LogicalType::FLOAT}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<float, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<double, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<date_t, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsFiniteOperator>));
	funcs.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BOOLEAN,
	                                 ScalarFunction::UnaryFunction<timestamp_t, bool, IsFiniteOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// sin
//===--------------------------------------------------------------------===//
template <class OP>
struct NoInfiniteDoubleWrapper {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(input))) {
			if (Value::IsNan(input)) {
				return input;
			}
			throw OutOfRangeException("input value %lf is out of range for numeric function", input);
		}
		return OP::template Operation<INPUT_TYPE, RESULT_TYPE>(input);
	}
};

struct SinOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return std::sin(input);
	}
};

ScalarFunction SinFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<SinOperator>>);
}

//===--------------------------------------------------------------------===//
// cos
//===--------------------------------------------------------------------===//
struct CosOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::cos(input);
	}
};

ScalarFunction CosFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<CosOperator>>);
}

//===--------------------------------------------------------------------===//
// tan
//===--------------------------------------------------------------------===//
struct TanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::tan(input);
	}
};

ScalarFunction TanFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<TanOperator>>);
}

//===--------------------------------------------------------------------===//
// asin
//===--------------------------------------------------------------------===//
struct ASinOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw Exception("ASIN is undefined outside [-1,1]");
		}
		return (double)std::asin(input);
	}
};

ScalarFunction AsinFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<ASinOperator>>);
}

//===--------------------------------------------------------------------===//
// atan
//===--------------------------------------------------------------------===//
struct ATanOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::atan(input);
	}
};

ScalarFunction AtanFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, ATanOperator>);
}

//===--------------------------------------------------------------------===//
// atan2
//===--------------------------------------------------------------------===//
struct ATan2 {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return (double)std::atan2(left, right);
	}
};

ScalarFunction Atan2Fun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::BinaryFunction<double, double, double, ATan2>);
}

//===--------------------------------------------------------------------===//
// acos
//===--------------------------------------------------------------------===//
struct ACos {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (double)std::acos(input);
	}
};

ScalarFunction AcosFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<ACos>>);
}

//===--------------------------------------------------------------------===//
// cot
//===--------------------------------------------------------------------===//
struct CotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 1.0 / (double)std::tan(input);
	}
};

ScalarFunction CotFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, NoInfiniteDoubleWrapper<CotOperator>>);
}

//===--------------------------------------------------------------------===//
// gamma
//===--------------------------------------------------------------------===//
struct GammaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input == 0) {
			throw OutOfRangeException("cannot take gamma of zero");
		}
		return std::tgamma(input);
	}
};

ScalarFunction GammaFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, GammaOperator>);
}

//===--------------------------------------------------------------------===//
// gamma
//===--------------------------------------------------------------------===//
struct LogGammaOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		if (input == 0) {
			throw OutOfRangeException("cannot take log gamma of zero");
		}
		return std::lgamma(input);
	}
};

ScalarFunction LogGammaFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, LogGammaOperator>);
}

//===--------------------------------------------------------------------===//
// factorial(), !
//===--------------------------------------------------------------------===//
struct FactorialOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		TR ret = 1;
		for (TA i = 2; i <= left; i++) {
			ret *= i;
		}
		return ret;
	}
};

ScalarFunction FactorialOperatorFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::HUGEINT,
	                      ScalarFunction::UnaryFunction<int32_t, hugeint_t, FactorialOperator>);
}

//===--------------------------------------------------------------------===//
// even
//===--------------------------------------------------------------------===//
struct EvenOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		double value;
		if (left >= 0) {
			value = std::ceil(left);
		} else {
			value = std::ceil(-left);
			value = -value;
		}
		if (std::floor(value / 2) * 2 != value) {
			if (left >= 0) {
				return value += 1;
			}
			return value -= 1;
		}
		return value;
	}
};

ScalarFunction EvenFun::GetFunction() {
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE,
	                      ScalarFunction::UnaryFunction<double, double, EvenOperator>);
}

//===--------------------------------------------------------------------===//
// gcd
//===--------------------------------------------------------------------===//

// should be replaced with std::gcd in a newer C++ standard
template <class TA>
TA GreatestCommonDivisor(TA left, TA right) {
	TA a = left;
	TA b = right;

	// This protects the following modulo operations from a corner case,
	// where we would get a runtime error due to an integer overflow.
	if ((left == NumericLimits<TA>::Minimum() && right == -1) ||
	    (left == -1 && right == NumericLimits<TA>::Minimum())) {
		return 1;
	}

	while (true) {
		if (a == 0) {
			return TryAbsOperator::Operation<TA, TA>(b);
		}
		b %= a;

		if (b == 0) {
			return TryAbsOperator::Operation<TA, TA>(a);
		}
		a %= b;
	}
}

struct GreatestCommonDivisorOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return GreatestCommonDivisor(left, right);
	}
};

ScalarFunctionSet GreatestCommonDivisorFun::GetFunctions() {
	ScalarFunctionSet funcs;
	funcs.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, GreatestCommonDivisorOperator>));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
	                   ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, GreatestCommonDivisorOperator>));
	return funcs;
}

//===--------------------------------------------------------------------===//
// lcm
//===--------------------------------------------------------------------===//

// should be replaced with std::lcm in a newer C++ standard
struct LeastCommonMultipleOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		if (left == 0 || right == 0) {
			return 0;
		}
		TR result;
		if (!TryMultiplyOperator::Operation<TA, TB, TR>(left, right / GreatestCommonDivisor(left, right), result)) {
			throw OutOfRangeException("lcm value is out of range");
		}
		return TryAbsOperator::Operation<TR, TR>(result);
	}
};

ScalarFunctionSet LeastCommonMultipleFun::GetFunctions() {
	ScalarFunctionSet funcs;

	funcs.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, LeastCommonMultipleOperator>));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT, LogicalType::HUGEINT}, LogicalType::HUGEINT,
	                   ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, LeastCommonMultipleOperator>));
	return funcs;
}

} // namespace duckdb





namespace duckdb {

template <class OP>
static scalar_function_t GetScalarIntegerUnaryFunction(const LogicalType &type) {
	scalar_function_t function;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		function = &ScalarFunction::UnaryFunction<int8_t, int8_t, OP>;
		break;
	case LogicalTypeId::SMALLINT:
		function = &ScalarFunction::UnaryFunction<int16_t, int16_t, OP>;
		break;
	case LogicalTypeId::INTEGER:
		function = &ScalarFunction::UnaryFunction<int32_t, int32_t, OP>;
		break;
	case LogicalTypeId::BIGINT:
		function = &ScalarFunction::UnaryFunction<int64_t, int64_t, OP>;
		break;
	case LogicalTypeId::UTINYINT:
		function = &ScalarFunction::UnaryFunction<uint8_t, uint8_t, OP>;
		break;
	case LogicalTypeId::USMALLINT:
		function = &ScalarFunction::UnaryFunction<uint16_t, uint16_t, OP>;
		break;
	case LogicalTypeId::UINTEGER:
		function = &ScalarFunction::UnaryFunction<uint32_t, uint32_t, OP>;
		break;
	case LogicalTypeId::UBIGINT:
		function = &ScalarFunction::UnaryFunction<uint64_t, uint64_t, OP>;
		break;
	case LogicalTypeId::HUGEINT:
		function = &ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerUnaryFunction");
	}
	return function;
}

template <class OP>
static scalar_function_t GetScalarIntegerBinaryFunction(const LogicalType &type) {
	scalar_function_t function;
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		function = &ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
		break;
	case LogicalTypeId::SMALLINT:
		function = &ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
		break;
	case LogicalTypeId::INTEGER:
		function = &ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
		break;
	case LogicalTypeId::BIGINT:
		function = &ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
		break;
	case LogicalTypeId::UTINYINT:
		function = &ScalarFunction::BinaryFunction<uint8_t, uint8_t, uint8_t, OP>;
		break;
	case LogicalTypeId::USMALLINT:
		function = &ScalarFunction::BinaryFunction<uint16_t, uint16_t, uint16_t, OP>;
		break;
	case LogicalTypeId::UINTEGER:
		function = &ScalarFunction::BinaryFunction<uint32_t, uint32_t, uint32_t, OP>;
		break;
	case LogicalTypeId::UBIGINT:
		function = &ScalarFunction::BinaryFunction<uint64_t, uint64_t, uint64_t, OP>;
		break;
	case LogicalTypeId::HUGEINT:
		function = &ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, OP>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarIntegerBinaryFunction");
	}
	return function;
}

//===--------------------------------------------------------------------===//
// & [bitwise_and]
//===--------------------------------------------------------------------===//
struct BitwiseANDOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left & right;
	}
};

static void BitwiseANDOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t rhs, string_t lhs) {
		    string_t target = StringVector::EmptyString(result, rhs.GetSize());

		    Bit::BitwiseAnd(rhs, lhs, target);
		    return target;
	    });
}

ScalarFunctionSet BitwiseAndFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseANDOperator>(type)));
	}
	functions.AddFunction(ScalarFunction({LogicalType::BIT, LogicalType::BIT}, LogicalType::BIT, BitwiseANDOperation));
	return functions;
}

//===--------------------------------------------------------------------===//
// | [bitwise_or]
//===--------------------------------------------------------------------===//
struct BitwiseOROperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left | right;
	}
};

static void BitwiseOROperation(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t rhs, string_t lhs) {
		    string_t target = StringVector::EmptyString(result, rhs.GetSize());

		    Bit::BitwiseOr(rhs, lhs, target);
		    return target;
	    });
}

ScalarFunctionSet BitwiseOrFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseOROperator>(type)));
	}
	functions.AddFunction(ScalarFunction({LogicalType::BIT, LogicalType::BIT}, LogicalType::BIT, BitwiseOROperation));
	return functions;
}

//===--------------------------------------------------------------------===//
// # [bitwise_xor]
//===--------------------------------------------------------------------===//
struct BitwiseXOROperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return left ^ right;
	}
};

static void BitwiseXOROperation(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t rhs, string_t lhs) {
		    string_t target = StringVector::EmptyString(result, rhs.GetSize());

		    Bit::BitwiseXor(rhs, lhs, target);
		    return target;
	    });
}

ScalarFunctionSet BitwiseXorFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseXOROperator>(type)));
	}
	functions.AddFunction(ScalarFunction({LogicalType::BIT, LogicalType::BIT}, LogicalType::BIT, BitwiseXOROperation));
	return functions;
}

//===--------------------------------------------------------------------===//
// ~ [bitwise_not]
//===--------------------------------------------------------------------===//
struct BitwiseNotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return ~input;
	}
};

static void BitwiseNOTOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		string_t target = StringVector::EmptyString(result, input.GetSize());

		Bit::BitwiseNot(input, target);
		return target;
	});
}

ScalarFunctionSet BitwiseNotFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(ScalarFunction({type}, type, GetScalarIntegerUnaryFunction<BitwiseNotOperator>(type)));
	}
	functions.AddFunction(ScalarFunction({LogicalType::BIT}, LogicalType::BIT, BitwiseNOTOperation));
	return functions;
}

//===--------------------------------------------------------------------===//
// << [bitwise_left_shift]
//===--------------------------------------------------------------------===//

struct BitwiseShiftLeftOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB shift) {
		TA max_shift = TA(sizeof(TA) * 8);
		if (input < 0) {
			throw OutOfRangeException("Cannot left-shift negative number %s", NumericHelper::ToString(input));
		}
		if (shift < 0) {
			throw OutOfRangeException("Cannot left-shift by negative number %s", NumericHelper::ToString(shift));
		}
		if (shift >= max_shift) {
			if (input == 0) {
				return 0;
			}
			throw OutOfRangeException("Left-shift value %s is out of range", NumericHelper::ToString(shift));
		}
		if (shift == 0) {
			return input;
		}
		TA max_value = (TA(1) << (max_shift - shift - 1));
		if (input >= max_value) {
			throw OutOfRangeException("Overflow in left shift (%s << %s)", NumericHelper::ToString(input),
			                          NumericHelper::ToString(shift));
		}
		return input << shift;
	}
};

static void BitwiseShiftLeftOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, int32_t shift) {
		    int32_t max_shift = Bit::BitLength(input);
		    if (shift == 0) {
			    return input;
		    }
		    if (shift < 0) {
			    throw OutOfRangeException("Cannot left-shift by negative number %s", NumericHelper::ToString(shift));
		    }
		    string_t target = StringVector::EmptyString(result, input.GetSize());

		    if (shift >= max_shift) {
			    Bit::SetEmptyBitString(target, input);
			    return target;
		    }
		    Bit::LeftShift(input, shift, target);
		    return target;
	    });
}

ScalarFunctionSet LeftShiftFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseShiftLeftOperator>(type)));
	}
	functions.AddFunction(
	    ScalarFunction({LogicalType::BIT, LogicalType::INTEGER}, LogicalType::BIT, BitwiseShiftLeftOperation));
	return functions;
}

//===--------------------------------------------------------------------===//
// >> [bitwise_right_shift]
//===--------------------------------------------------------------------===//
template <class T>
bool RightShiftInRange(T shift) {
	return shift >= 0 && shift < T(sizeof(T) * 8);
}

struct BitwiseShiftRightOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB shift) {
		return RightShiftInRange(shift) ? input >> shift : 0;
	}
};

static void BitwiseShiftRightOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, int32_t shift) {
		    int32_t max_shift = Bit::BitLength(input);
		    if (shift == 0) {
			    return input;
		    }
		    string_t target = StringVector::EmptyString(result, input.GetSize());
		    if (shift < 0 || shift >= max_shift) {
			    Bit::SetEmptyBitString(target, input);
			    return target;
		    }
		    Bit::RightShift(input, shift, target);
		    return target;
	    });
}

ScalarFunctionSet RightShiftFun::GetFunctions() {
	ScalarFunctionSet functions;
	for (auto &type : LogicalType::Integral()) {
		functions.AddFunction(
		    ScalarFunction({type, type}, type, GetScalarIntegerBinaryFunction<BitwiseShiftRightOperator>(type)));
	}
	functions.AddFunction(
	    ScalarFunction({LogicalType::BIT, LogicalType::INTEGER}, LogicalType::BIT, BitwiseShiftRightOperation));
	return functions;
}

} // namespace duckdb








namespace duckdb {

struct RandomLocalState : public FunctionLocalState {
	explicit RandomLocalState(uint32_t seed) : random_engine(seed) {
	}

	RandomEngine random_engine;
};

static void RandomFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<double>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = lstate.random_engine.NextRandom();
	}
}

static unique_ptr<FunctionLocalState> RandomInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                           FunctionData *bind_data) {
	auto &random_engine = RandomEngine::Get(state.GetContext());
	lock_guard<mutex> guard(random_engine.lock);
	return make_uniq<RandomLocalState>(random_engine.NextRandomInteger());
}

ScalarFunction RandomFun::GetFunction() {
	ScalarFunction random("random", {}, LogicalType::DOUBLE, RandomFunction, nullptr, nullptr, nullptr,
	                      RandomInitLocalState);
	random.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return random;
}

static void GenerateUUIDFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RandomLocalState>();

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUID::GenerateRandomUUID(lstate.random_engine);
	}
}

ScalarFunction UUIDFun::GetFunction() {
	ScalarFunction uuid_function({}, LogicalType::UUID, GenerateUUIDFunction, nullptr, nullptr, nullptr,
	                             RandomInitLocalState);
	// generate a random uuid
	uuid_function.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return uuid_function;
}

} // namespace duckdb









namespace duckdb {

struct SetseedBindData : public FunctionData {
	//! The client context for the function call
	ClientContext &context;

	explicit SetseedBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<SetseedBindData>(context);
	}

	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void SetSeedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<SetseedBindData>();
	auto &input = args.data[0];
	input.Flatten(args.size());

	auto input_seeds = FlatVector::GetData<double>(input);
	uint32_t half_max = NumericLimits<uint32_t>::Maximum() / 2;

	auto &random_engine = RandomEngine::Get(info.context);
	for (idx_t i = 0; i < args.size(); i++) {
		if (input_seeds[i] < -1.0 || input_seeds[i] > 1.0 || Value::IsNan(input_seeds[i])) {
			throw Exception("SETSEED accepts seed values between -1.0 and 1.0, inclusive");
		}
		uint32_t norm_seed = (input_seeds[i] + 1.0) * half_max;
		random_engine.SetSeed(norm_seed);
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

unique_ptr<FunctionData> SetSeedBind(ClientContext &context, ScalarFunction &bound_function,
                                     vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<SetseedBindData>(context);
}

ScalarFunction SetseedFun::GetFunction() {
	ScalarFunction setseed("setseed", {LogicalType::DOUBLE}, LogicalType::SQLNULL, SetSeedFunction, SetSeedBind);
	setseed.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return setseed;
}

} // namespace duckdb




namespace duckdb {

struct AsciiOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		auto str = input.GetData();
		if (Utf8Proc::Analyze(str, input.GetSize()) == UnicodeType::ASCII) {
			return str[0];
		}
		int utf8_bytes = 4;
		return Utf8Proc::UTF8ToCodepoint(str, utf8_bytes);
	}
};

ScalarFunction ASCIIFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
	                      ScalarFunction::UnaryFunction<string_t, int32_t, AsciiOperator>);
}

} // namespace duckdb









namespace duckdb {

static string_t BarScalarFunction(double x, double min, double max, double max_width, string &result) {
	static const char *FULL_BLOCK = UnicodeBar::FullBlock();
	static const char *const *PARTIAL_BLOCKS = UnicodeBar::PartialBlocks();
	static const idx_t PARTIAL_BLOCKS_COUNT = UnicodeBar::PartialBlocksCount();

	if (!Value::IsFinite(max_width)) {
		throw ValueOutOfRangeException("Max bar width must not be NaN or infinity");
	}
	if (max_width < 1) {
		throw ValueOutOfRangeException("Max bar width must be >= 1");
	}
	if (max_width > 1000) {
		throw ValueOutOfRangeException("Max bar width must be <= 1000");
	}

	double width;

	if (Value::IsNan(x) || Value::IsNan(min) || Value::IsNan(max) || x <= min) {
		width = 0;
	} else if (x >= max) {
		width = max_width;
	} else {
		width = max_width * (x - min) / (max - min);
	}

	if (!Value::IsFinite(width)) {
		throw ValueOutOfRangeException("Bar width must not be NaN or infinity");
	}

	result.clear();

	int32_t width_as_int = static_cast<int32_t>(width * PARTIAL_BLOCKS_COUNT);
	idx_t full_blocks_count = (width_as_int / PARTIAL_BLOCKS_COUNT);
	for (idx_t i = 0; i < full_blocks_count; i++) {
		result += FULL_BLOCK;
	}

	idx_t remaining = width_as_int % PARTIAL_BLOCKS_COUNT;

	if (remaining) {
		result += PARTIAL_BLOCKS[remaining];
	}

	return string_t(result);
}

static void BarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 4);
	auto &x_arg = args.data[0];
	auto &min_arg = args.data[1];
	auto &max_arg = args.data[2];
	string buffer;

	if (args.ColumnCount() == 3) {
		GenericExecutor::ExecuteTernary<PrimitiveType<double>, PrimitiveType<double>, PrimitiveType<double>,
		                                PrimitiveType<string_t>>(
		    x_arg, min_arg, max_arg, result, args.size(),
		    [&](PrimitiveType<double> x, PrimitiveType<double> min, PrimitiveType<double> max) {
			    return StringVector::AddString(result, BarScalarFunction(x.val, min.val, max.val, 80, buffer));
		    });
	} else {
		auto &width_arg = args.data[3];
		GenericExecutor::ExecuteQuaternary<PrimitiveType<double>, PrimitiveType<double>, PrimitiveType<double>,
		                                   PrimitiveType<double>, PrimitiveType<string_t>>(
		    x_arg, min_arg, max_arg, width_arg, result, args.size(),
		    [&](PrimitiveType<double> x, PrimitiveType<double> min, PrimitiveType<double> max,
		        PrimitiveType<double> width) {
			    return StringVector::AddString(result, BarScalarFunction(x.val, min.val, max.val, width.val, buffer));
		    });
	}
}

ScalarFunctionSet BarFun::GetFunctions() {
	ScalarFunctionSet bar;
	bar.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::VARCHAR, BarFunction));
	bar.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::VARCHAR, BarFunction));
	return bar;
}

} // namespace duckdb




namespace duckdb {

struct ChrOperator {
	static void GetCodepoint(int32_t input, char c[], int &utf8_bytes) {
		if (input < 0 || !Utf8Proc::CodepointToUtf8(input, utf8_bytes, &c[0])) {
			throw InvalidInputException("Invalid UTF8 Codepoint %d", input);
		}
	}

	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		char c[5] = {'\0', '\0', '\0', '\0', '\0'};
		int utf8_bytes;
		GetCodepoint(input, c, utf8_bytes);
		return string_t(&c[0], utf8_bytes);
	}
};

#ifdef DUCKDB_DEBUG_NO_INLINE
// the chr function depends on the data always being inlined (which is always possible, since it outputs max 4 bytes)
// to enable chr when string inlining is disabled we create a special function here
static void ChrFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &code_vec = args.data[0];

	char c[5] = {'\0', '\0', '\0', '\0', '\0'};
	int utf8_bytes;
	UnaryExecutor::Execute<int32_t, string_t>(code_vec, result, args.size(), [&](int32_t input) {
		ChrOperator::GetCodepoint(input, c, utf8_bytes);
		return StringVector::AddString(result, &c[0], utf8_bytes);
	});
}
#endif

ScalarFunction ChrFun::GetFunction() {
	return ScalarFunction("chr", {LogicalType::INTEGER}, LogicalType::VARCHAR,
#ifdef DUCKDB_DEBUG_NO_INLINE
	                      ChrFunction
#else
	                      ScalarFunction::UnaryFunction<int32_t, string_t, ChrOperator>
#endif
	);
}

} // namespace duckdb




namespace duckdb {

// Using Lowrance-Wagner (LW) algorithm: https://doi.org/10.1145%2F321879.321880
// Can't calculate as trivial modification to levenshtein algorithm
// as we need to potentially know about earlier in the string
static idx_t DamerauLevenshteinDistance(const string_t &source, const string_t &target) {
	// costs associated with each type of edit, to aid readability
	constexpr uint8_t COST_SUBSTITUTION = 1;
	constexpr uint8_t COST_INSERTION = 1;
	constexpr uint8_t COST_DELETION = 1;
	constexpr uint8_t COST_TRANSPOSITION = 1;
	const auto source_len = source.GetSize();
	const auto target_len = target.GetSize();

	// If one string is empty, the distance equals the length of the other string
	// either through target_len insertions
	// or source_len deletions
	if (source_len == 0) {
		return target_len * COST_INSERTION;
	} else if (target_len == 0) {
		return source_len * COST_DELETION;
	}

	const auto source_str = source.GetData();
	const auto target_str = target.GetData();

	// larger than the largest possible value:
	const auto inf = source_len * COST_DELETION + target_len * COST_INSERTION + 1;
	// minimum edit distance from prefix of source string to prefix of target string
	// same object as H in LW paper (with indices offset by 1)
	vector<vector<idx_t>> distance(source_len + 2, vector<idx_t>(target_len + 2, inf));
	// keeps track of the largest string indices of source string matching each character
	// same as DA in LW paper
	map<char, idx_t> largest_source_chr_matching;

	// initialise row/column corresponding to zero-length strings
	// partial string -> empty requires a deletion for each character
	for (idx_t source_idx = 0; source_idx <= source_len; source_idx++) {
		distance[source_idx + 1][1] = source_idx * COST_DELETION;
	}
	// and empty -> partial string means simply inserting characters
	for (idx_t target_idx = 1; target_idx <= target_len; target_idx++) {
		distance[1][target_idx + 1] = target_idx * COST_INSERTION;
	}
	// loop through string indices - these are offset by 2 from distance indices
	for (idx_t source_idx = 0; source_idx < source_len; source_idx++) {
		// keeps track of the largest string indices of target string matching current source character
		// same as DB in LW paper
		idx_t largest_target_chr_matching;
		largest_target_chr_matching = 0;
		for (idx_t target_idx = 0; target_idx < target_len; target_idx++) {
			// correspond to i1 and j1 in LW paper respectively
			idx_t largest_source_chr_matching_target;
			idx_t largest_target_chr_matching_source;
			// cost associated to diagnanl shift in distance matrix
			// corresponds to d in LW paper
			uint8_t cost_diagonal_shift;
			largest_source_chr_matching_target = largest_source_chr_matching[target_str[target_idx]];
			largest_target_chr_matching_source = largest_target_chr_matching;
			// if characters match, diagonal move costs nothing and we update our largest target index
			// otherwise move is substitution and costs as such
			if (source_str[source_idx] == target_str[target_idx]) {
				cost_diagonal_shift = 0;
				largest_target_chr_matching = target_idx + 1;
			} else {
				cost_diagonal_shift = COST_SUBSTITUTION;
			}
			distance[source_idx + 2][target_idx + 2] = MinValue(
			    distance[source_idx + 1][target_idx + 1] + cost_diagonal_shift,
			    MinValue(distance[source_idx + 2][target_idx + 1] + COST_INSERTION,
			             MinValue(distance[source_idx + 1][target_idx + 2] + COST_DELETION,
			                      distance[largest_source_chr_matching_target][largest_target_chr_matching_source] +
			                          (source_idx - largest_source_chr_matching_target) * COST_DELETION +
			                          COST_TRANSPOSITION +
			                          (target_idx - largest_target_chr_matching_source) * COST_INSERTION)));
		}
		largest_source_chr_matching[source_str[source_idx]] = source_idx + 1;
	}
	return distance[source_len + 1][target_len + 1];
}

static int64_t DamerauLevenshteinScalarFunction(Vector &result, const string_t source, const string_t target) {
	return (int64_t)DamerauLevenshteinDistance(source, target);
}

static void DamerauLevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &source_vec = args.data[0];
	auto &target_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    source_vec, target_vec, result, args.size(),
	    [&](string_t source, string_t target) { return DamerauLevenshteinScalarFunction(result, source, target); });
}

ScalarFunction DamerauLevenshteinFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                      DamerauLevenshteinFunction);
}

} // namespace duckdb




namespace duckdb {

static void FormatBytesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<int64_t, string_t>(args.data[0], result, args.size(), [&](int64_t bytes) {
		bool is_negative = bytes < 0;
		idx_t unsigned_bytes;
		if (bytes < 0) {
			if (bytes == NumericLimits<int64_t>::Minimum()) {
				unsigned_bytes = idx_t(NumericLimits<int64_t>::Maximum()) + 1;
			} else {
				unsigned_bytes = idx_t(-bytes);
			}
		} else {
			unsigned_bytes = idx_t(bytes);
		}
		return StringVector::AddString(result, (is_negative ? "-" : "") +
		                                           StringUtil::BytesToHumanReadableString(unsigned_bytes));
	});
}

ScalarFunction FormatBytesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, FormatBytesFunction);
}

} // namespace duckdb



#include <ctype.h>
#include <algorithm>

namespace duckdb {

static int64_t MismatchesScalarFunction(Vector &result, const string_t str, string_t tgt) {
	idx_t str_len = str.GetSize();
	idx_t tgt_len = tgt.GetSize();

	if (str_len != tgt_len) {
		throw InvalidInputException("Mismatch Function: Strings must be of equal length!");
	}
	if (str_len < 1) {
		throw InvalidInputException("Mismatch Function: Strings must be of length > 0!");
	}

	idx_t mismatches = 0;
	auto str_str = str.GetData();
	auto tgt_str = tgt.GetData();

	for (idx_t idx = 0; idx < str_len; ++idx) {
		if (str_str[idx] != tgt_str[idx]) {
			mismatches++;
		}
	}
	return (int64_t)mismatches;
}

static void MismatchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return MismatchesScalarFunction(result, str, tgt); });
}

ScalarFunction HammingFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT, MismatchesFunction);
}

} // namespace duckdb









namespace duckdb {

static void WriteHexBytes(uint64_t x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size * 4;

	for (; offset >= 4; offset -= 4) {
		uint8_t byte = (x >> (offset - 4)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}
}

static void WriteHugeIntHexBytes(hugeint_t x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size * 4;
	auto upper = x.upper;
	auto lower = x.lower;

	for (; offset >= 68; offset -= 4) {
		uint8_t byte = (upper >> (offset - 68)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}

	for (; offset >= 4; offset -= 4) {
		uint8_t byte = (lower >> (offset - 4)) & 0x0F;
		*output = Blob::HEX_TABLE[byte];
		output++;
	}
}

static void WriteBinBytes(uint64_t x, char *&output, idx_t buffer_size) {
	idx_t offset = buffer_size;
	for (; offset >= 1; offset -= 1) {
		*output = ((x >> (offset - 1)) & 0x01) + '0';
		output++;
	}
}

static void WriteHugeIntBinBytes(hugeint_t x, char *&output, idx_t buffer_size) {
	auto upper = x.upper;
	auto lower = x.lower;
	idx_t offset = buffer_size;

	for (; offset >= 65; offset -= 1) {
		*output = ((upper >> (offset - 65)) & 0x01) + '0';
		output++;
	}

	for (; offset >= 1; offset -= 1) {
		*output = ((lower >> (offset - 1)) & 0x01) + '0';
		output++;
	}
}

struct HexStrOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 2);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			*output = Blob::HEX_TABLE[(data[i] >> 4) & 0x0F];
			output++;
			*output = Blob::HEX_TABLE[data[i] & 0x0F];
			output++;
		}

		target.Finalize();
		return target;
	}
};

struct HexIntegralOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		idx_t num_leading_zero = CountZeros<uint64_t>::Leading(input);
		idx_t num_bits_to_check = 64 - num_leading_zero;
		D_ASSERT(num_bits_to_check <= sizeof(INPUT_TYPE) * 8);

		idx_t buffer_size = (num_bits_to_check + 3) / 4;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHexBytes(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct HexHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		idx_t num_leading_zero = CountZeros<hugeint_t>::Leading(input);
		idx_t buffer_size = sizeof(INPUT_TYPE) * 2 - (num_leading_zero / 4);

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntHexBytes(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

template <class INPUT, class OP>
static void ToHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, string_t, OP>(input, result, count);
}

struct BinaryStrOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		// Allocate empty space
		auto target = StringVector::EmptyString(result, size * 8);
		auto output = target.GetDataWriteable();

		for (idx_t i = 0; i < size; ++i) {
			uint8_t byte = data[i];
			for (idx_t i = 8; i >= 1; --i) {
				*output = ((byte >> (i - 1)) & 0x01) + '0';
				output++;
			}
		}

		target.Finalize();
		return target;
	}
};

struct BinaryIntegralOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {

		idx_t num_leading_zero = CountZeros<uint64_t>::Leading(input);
		idx_t num_bits_to_check = 64 - num_leading_zero;
		D_ASSERT(num_bits_to_check <= sizeof(INPUT_TYPE) * 8);

		idx_t buffer_size = num_bits_to_check;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		D_ASSERT(buffer_size > 0);
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteBinBytes(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct BinaryHugeIntOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		idx_t num_leading_zero = CountZeros<hugeint_t>::Leading(input);
		idx_t buffer_size = sizeof(INPUT_TYPE) * 8 - num_leading_zero;

		// Special case: All bits are zero
		if (buffer_size == 0) {
			auto target = StringVector::EmptyString(result, 1);
			auto output = target.GetDataWriteable();
			*output = '0';
			target.Finalize();
			return target;
		}

		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		WriteHugeIntBinBytes(input, output, buffer_size);

		target.Finalize();
		return target;
	}
};

struct FromHexOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		if (size > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Hexadecimal input length larger than 2^32 are not supported");
		}

		D_ASSERT(size <= NumericLimits<uint32_t>::Maximum());
		auto buffer_size = (size + 1) / 2;

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		// Treated as a single byte
		idx_t i = 0;
		if (size % 2 != 0) {
			*output = StringUtil::GetHexValue(data[i]);
			i++;
			output++;
		}

		for (; i < size; i += 2) {
			uint8_t major = StringUtil::GetHexValue(data[i]);
			uint8_t minor = StringUtil::GetHexValue(data[i + 1]);
			*output = (major << 4) | minor;
			output++;
		}

		target.Finalize();
		return target;
	}
};

struct FromBinaryOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		if (size > NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("Binary input length larger than 2^32 are not supported");
		}

		D_ASSERT(size <= NumericLimits<uint32_t>::Maximum());
		auto buffer_size = (size + 7) / 8;

		// Allocate empty space
		auto target = StringVector::EmptyString(result, buffer_size);
		auto output = target.GetDataWriteable();

		// Treated as a single byte
		idx_t i = 0;
		if (size % 8 != 0) {
			uint8_t byte = 0;
			for (idx_t j = size % 8; j > 0; --j) {
				byte |= StringUtil::GetBinaryValue(data[i]) << (j - 1);
				i++;
			}
			*output = byte;
			output++;
		}

		while (i < size) {
			uint8_t byte = 0;
			for (idx_t j = 8; j > 0; --j) {
				byte |= StringUtil::GetBinaryValue(data[i]) << (j - 1);
				i++;
			}
			*output = byte;
			output++;
		}

		target.Finalize();
		return target;
	}
};

template <class INPUT, class OP>
static void ToBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	idx_t count = args.size();
	UnaryExecutor::ExecuteString<INPUT, string_t, OP>(input, result, count);
}

static void FromBinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	D_ASSERT(args.data[0].GetType().InternalType() == PhysicalType::VARCHAR);
	auto &input = args.data[0];
	idx_t count = args.size();

	UnaryExecutor::ExecuteString<string_t, string_t, FromBinaryOperator>(input, result, count);
}

static void FromHexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	D_ASSERT(args.data[0].GetType().InternalType() == PhysicalType::VARCHAR);
	auto &input = args.data[0];
	idx_t count = args.size();

	UnaryExecutor::ExecuteString<string_t, string_t, FromHexOperator>(input, result, count);
}

ScalarFunctionSet HexFun::GetFunctions() {
	ScalarFunctionSet to_hex;
	to_hex.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToHexFunction<string_t, HexStrOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToHexFunction<int64_t, HexIntegralOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR, ToHexFunction<uint64_t, HexIntegralOperator>));

	to_hex.AddFunction(
	    ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR, ToHexFunction<hugeint_t, HexHugeIntOperator>));
	return to_hex;
}

ScalarFunction UnhexFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromHexFunction);
}

ScalarFunctionSet BinFun::GetFunctions() {
	ScalarFunctionSet to_binary;

	to_binary.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, ToBinaryFunction<string_t, BinaryStrOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<uint64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR, ToBinaryFunction<int64_t, BinaryIntegralOperator>));
	to_binary.AddFunction(ScalarFunction({LogicalType::HUGEINT}, LogicalType::VARCHAR,
	                                     ToBinaryFunction<hugeint_t, BinaryHugeIntOperator>));
	return to_binary;
}

ScalarFunction UnbinFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, FromBinaryFunction);
}

} // namespace duckdb








namespace duckdb {

struct InstrOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		int64_t string_position = 0;

		auto location = ContainsFun::Find(haystack, needle);
		if (location != DConstants::INVALID_INDEX) {
			auto len = (utf8proc_ssize_t)location;
			auto str = reinterpret_cast<const utf8proc_uint8_t *>(haystack.GetData());
			D_ASSERT(len <= (utf8proc_ssize_t)haystack.GetSize());
			for (++string_position; len > 0; ++string_position) {
				utf8proc_int32_t codepoint;
				auto bytes = utf8proc_iterate(str, len, &codepoint);
				str += bytes;
				len -= bytes;
			}
		}
		return string_position;
	}
};

struct InstrAsciiOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA haystack, TB needle) {
		auto location = ContainsFun::Find(haystack, needle);
		return location == DConstants::INVALID_INDEX ? 0 : location + 1;
	}
};

static unique_ptr<BaseStatistics> InStrPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	D_ASSERT(child_stats.size() == 2);
	// can only propagate stats if the children have stats
	// for strpos, we only care if the FIRST string has unicode or not
	if (!StringStats::CanContainUnicode(child_stats[0])) {
		expr.function.function = ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrAsciiOperator>;
	}
	return nullptr;
}

ScalarFunction InstrFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT,
	                      ScalarFunction::BinaryFunction<string_t, string_t, int64_t, InstrOperator>, nullptr, nullptr,
	                      InStrPropagateStats);
}

} // namespace duckdb




#include <ctype.h>

namespace duckdb {

static inline map<char, idx_t> GetSet(const string_t &str) {
	auto map_of_chars = map<char, idx_t> {};
	idx_t str_len = str.GetSize();
	auto s = str.GetData();

	for (idx_t pos = 0; pos < str_len; pos++) {
		map_of_chars.insert(std::make_pair(s[pos], 1));
	}
	return map_of_chars;
}

static double JaccardSimilarity(const string_t &str, const string_t &txt) {
	if (str.GetSize() < 1 || txt.GetSize() < 1) {
		throw InvalidInputException("Jaccard Function: An argument too short!");
	}
	map<char, idx_t> m_str, m_txt;

	m_str = GetSet(str);
	m_txt = GetSet(txt);

	if (m_str.size() > m_txt.size()) {
		m_str.swap(m_txt);
	}

	for (auto const &achar : m_str) {
		++m_txt[achar.first];
	}
	// m_txt.size is now size of union.

	idx_t size_intersect = 0;
	for (const auto &apair : m_txt) {
		if (apair.second > 1) {
			size_intersect++;
		}
	}

	return (double)size_intersect / (double)m_txt.size();
}

static double JaccardScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (double)JaccardSimilarity(str, tgt);
}

static void JaccardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, double>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return JaccardScalarFunction(result, str, tgt); });
}

ScalarFunction JaccardFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaccardFunction);
}

} // namespace duckdb




namespace duckdb {

static inline double JaroScalarFunction(const string_t &s1, const string_t &s2) {
	auto s1_begin = s1.GetData();
	auto s2_begin = s2.GetData();
	return duckdb_jaro_winkler::jaro_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin, s2_begin + s2.GetSize());
}

static inline double JaroWinklerScalarFunction(const string_t &s1, const string_t &s2) {
	auto s1_begin = s1.GetData();
	auto s2_begin = s2.GetData();
	return duckdb_jaro_winkler::jaro_winkler_similarity(s1_begin, s1_begin + s1.GetSize(), s2_begin,
	                                                    s2_begin + s2.GetSize());
}

template <class CACHED_SIMILARITY>
static void CachedFunction(Vector &constant, Vector &other, Vector &result, idx_t count) {
	auto val = constant.GetValue(0);
	if (val.IsNull()) {
		auto &result_validity = FlatVector::Validity(result);
		result_validity.SetAllInvalid(count);
		return;
	}

	auto str_val = StringValue::Get(val);
	auto cached = CACHED_SIMILARITY(str_val);
	UnaryExecutor::Execute<string_t, double>(other, result, count, [&](const string_t &other_str) {
		auto other_str_begin = other_str.GetData();
		return cached.similarity(other_str_begin, other_str_begin + other_str.GetSize());
	});
}

template <class CACHED_SIMILARITY, class SIMILARITY_FUNCTION = std::function<double(string_t, string_t)>>
static void TemplatedJaroWinklerFunction(DataChunk &args, Vector &result, SIMILARITY_FUNCTION fun) {
	bool arg0_constant = args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR;
	bool arg1_constant = args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (!(arg0_constant ^ arg1_constant)) {
		// We can't optimize by caching one of the two strings
		BinaryExecutor::Execute<string_t, string_t, double>(args.data[0], args.data[1], result, args.size(), fun);
		return;
	}

	if (arg0_constant) {
		CachedFunction<CACHED_SIMILARITY>(args.data[0], args.data[1], result, args.size());
	} else {
		CachedFunction<CACHED_SIMILARITY>(args.data[1], args.data[0], result, args.size());
	}
}

static void JaroFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroSimilarity<char>>(args, result, JaroScalarFunction);
}

static void JaroWinklerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	TemplatedJaroWinklerFunction<duckdb_jaro_winkler::CachedJaroWinklerSimilarity<char>>(args, result,
	                                                                                     JaroWinklerScalarFunction);
}

ScalarFunction JaroSimilarityFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaroFunction);
}

ScalarFunction JaroWinklerSimilarityFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, JaroWinklerFunction);
}

} // namespace duckdb





#include <ctype.h>
#include <algorithm>

namespace duckdb {

struct LeftRightUnicode {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::Length<TA, TR>(input);
	}

	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringFun::SubstringUnicode(result, input, offset, length);
	}
};

struct LeftRightGrapheme {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return LengthFun::GraphemeCount<TA, TR>(input);
	}

	static string_t Substring(Vector &result, string_t input, int64_t offset, int64_t length) {
		return SubstringFun::SubstringGrapheme(result, input, offset, length);
	}
};

template <class OP>
static string_t LeftScalarFunction(Vector &result, const string_t str, int64_t pos) {
	if (pos >= 0) {
		return OP::Substring(result, str, 1, pos);
	}

	int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
	pos = MaxValue<int64_t>(0, num_characters + pos);
	return OP::Substring(result, str, 1, pos);
}

template <class OP>
static void LeftFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return LeftScalarFunction<OP>(result, str, pos); });
}

ScalarFunction LeftFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      LeftFunction<LeftRightUnicode>);
}

ScalarFunction LeftGraphemeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      LeftFunction<LeftRightGrapheme>);
}

template <class OP>
static string_t RightScalarFunction(Vector &result, const string_t str, int64_t pos) {
	int64_t num_characters = OP::template Operation<string_t, int64_t>(str);
	if (pos >= 0) {
		int64_t len = MinValue<int64_t>(num_characters, pos);
		int64_t start = num_characters - len + 1;
		return OP::Substring(result, str, start, len);
	}

	int64_t len = 0;
	if (pos != std::numeric_limits<int64_t>::min()) {
		len = num_characters - MinValue<int64_t>(num_characters, -pos);
	}
	int64_t start = num_characters - len + 1;
	return OP::Substring(result, str, start, len);
}

template <class OP>
static void RightFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &pos_vec = args.data[1];
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vec, pos_vec, result, args.size(),
	    [&](string_t str, int64_t pos) { return RightScalarFunction<OP>(result, str, pos); });
}

ScalarFunction RightFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      RightFunction<LeftRightUnicode>);
}

ScalarFunction RightGraphemeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::BIGINT}, LogicalType::VARCHAR,
	                      RightFunction<LeftRightGrapheme>);
}

} // namespace duckdb




#include <ctype.h>
#include <algorithm>

namespace duckdb {

// See: https://www.kdnuggets.com/2020/10/optimizing-levenshtein-distance-measuring-text-similarity.html
// And: Iterative 2-row algorithm: https://en.wikipedia.org/wiki/Levenshtein_distance
// Note: A first implementation using the array algorithm version resulted in an error raised by duckdb
// (too muach memory usage)

static idx_t LevenshteinDistance(const string_t &txt, const string_t &tgt) {
	auto txt_len = txt.GetSize();
	auto tgt_len = tgt.GetSize();

	// If one string is empty, the distance equals the length of the other string
	if (txt_len == 0) {
		return tgt_len;
	} else if (tgt_len == 0) {
		return txt_len;
	}

	auto txt_str = txt.GetData();
	auto tgt_str = tgt.GetData();

	// Create two working vectors
	vector<idx_t> distances0(tgt_len + 1, 0);
	vector<idx_t> distances1(tgt_len + 1, 0);

	idx_t cost_substitution = 0;
	idx_t cost_insertion = 0;
	idx_t cost_deletion = 0;

	// initialize distances0 vector
	// edit distance for an empty txt string is just the number of characters to delete from tgt
	for (idx_t pos_tgt = 0; pos_tgt <= tgt_len; pos_tgt++) {
		distances0[pos_tgt] = pos_tgt;
	}

	for (idx_t pos_txt = 0; pos_txt < txt_len; pos_txt++) {
		// calculate distances1 (current raw distances) from the previous row

		distances1[0] = pos_txt + 1;

		for (idx_t pos_tgt = 0; pos_tgt < tgt_len; pos_tgt++) {
			cost_deletion = distances0[pos_tgt + 1] + 1;
			cost_insertion = distances1[pos_tgt] + 1;
			cost_substitution = distances0[pos_tgt];

			if (txt_str[pos_txt] != tgt_str[pos_tgt]) {
				cost_substitution += 1;
			}

			distances1[pos_tgt + 1] = MinValue(cost_deletion, MinValue(cost_substitution, cost_insertion));
		}
		// copy distances1 (current row) to distances0 (previous row) for next iteration
		// since data in distances1 is always invalidated, a swap without copy is more efficient
		distances0 = distances1;
	}

	return distances0[tgt_len];
}

static int64_t LevenshteinScalarFunction(Vector &result, const string_t str, string_t tgt) {
	return (int64_t)LevenshteinDistance(str, tgt);
}

static void LevenshteinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vec = args.data[0];
	auto &tgt_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, int64_t>(
	    str_vec, tgt_vec, result, args.size(),
	    [&](string_t str, string_t tgt) { return LevenshteinScalarFunction(result, str, tgt); });
}

ScalarFunction LevenshteinFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT, LevenshteinFunction);
}

} // namespace duckdb






namespace duckdb {

struct MD5Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, MD5Context::MD5_HASH_LENGTH_TEXT);
		MD5Context context;
		context.Add(input);
		context.FinishHex(hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	}
};

struct MD5Number128Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		data_t digest[MD5Context::MD5_HASH_LENGTH_BINARY];

		MD5Context context;
		context.Add(input);
		context.Finish(digest);
		return *reinterpret_cast<hugeint_t *>(digest);
	}
};

template <bool lower>
struct MD5Number64Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		data_t digest[MD5Context::MD5_HASH_LENGTH_BINARY];

		MD5Context context;
		context.Add(input);
		context.Finish(digest);
		return *reinterpret_cast<uint64_t *>(&digest[lower ? 8 : 0]);
	}
};

static void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, MD5Operator>(input, result, args.size());
}

static void MD5NumberFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, hugeint_t, MD5Number128Operator>(input, result, args.size());
}

static void MD5NumberUpperFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t, MD5Number64Operator<false>>(input, result, args.size());
}

static void MD5NumberLowerFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, uint64_t, MD5Number64Operator<true>>(input, result, args.size());
}

ScalarFunction MD5Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, MD5Function);
}

ScalarFunction MD5NumberFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::HUGEINT, MD5NumberFunction);
}

ScalarFunction MD5NumberUpperFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, MD5NumberUpperFunction);
}

ScalarFunction MD5NumberLowerFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::UBIGINT, MD5NumberLowerFunction);
}

} // namespace duckdb










namespace duckdb {

static pair<idx_t, idx_t> PadCountChars(const idx_t len, const char *data, const idx_t size) {
	//  Count how much of str will fit in the output
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);
	idx_t nbytes = 0;
	idx_t nchars = 0;
	for (; nchars < len && nbytes < size; ++nchars) {
		utf8proc_int32_t codepoint;
		auto bytes = utf8proc_iterate(str + nbytes, size - nbytes, &codepoint);
		D_ASSERT(bytes > 0);
		nbytes += bytes;
	}

	return pair<idx_t, idx_t>(nbytes, nchars);
}

static bool InsertPadding(const idx_t len, const string_t &pad, vector<char> &result) {
	//  Copy the padding until the output is long enough
	auto data = pad.GetData();
	auto size = pad.GetSize();

	//  Check whether we need data that we don't have
	if (len > 0 && size == 0) {
		return false;
	}

	//  Insert characters until we have all we need.
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);
	idx_t nbytes = 0;
	for (idx_t nchars = 0; nchars < len; ++nchars) {
		//  If we are at the end of the pad, flush all of it and loop back
		if (nbytes >= size) {
			result.insert(result.end(), data, data + size);
			nbytes = 0;
		}

		//  Write the next character
		utf8proc_int32_t codepoint;
		auto bytes = utf8proc_iterate(str + nbytes, size - nbytes, &codepoint);
		D_ASSERT(bytes > 0);
		nbytes += bytes;
	}

	//  Flush the remaining pad
	result.insert(result.end(), data, data + nbytes);

	return true;
}

static string_t LeftPadFunction(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	auto data_str = str.GetData();
	auto size_str = str.GetSize();

	//  Count how much of str will fit in the output
	auto written = PadCountChars(len, data_str, size_str);

	//  Left pad by the number of characters still needed
	if (!InsertPadding(len - written.second, pad, result)) {
		throw Exception("Insufficient padding in LPAD.");
	}

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	return string_t(result.data(), result.size());
}

struct LeftPadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return LeftPadFunction(str, len, pad, result);
	}
};

static string_t RightPadFunction(const string_t &str, const int32_t len, const string_t &pad, vector<char> &result) {
	//  Reuse the buffer
	result.clear();

	// Get information about the base string
	auto data_str = str.GetData();
	auto size_str = str.GetSize();

	// Count how much of str will fit in the output
	auto written = PadCountChars(len, data_str, size_str);

	//  Append as much of the original string as fits
	result.insert(result.end(), data_str, data_str + written.first);

	//  Right pad by the number of characters still needed
	if (!InsertPadding(len - written.second, pad, result)) {
		throw Exception("Insufficient padding in RPAD.");
	};

	return string_t(result.data(), result.size());
}

struct RightPadOperator {
	static inline string_t Operation(const string_t &str, const int32_t len, const string_t &pad,
	                                 vector<char> &result) {
		return RightPadFunction(str, len, pad, result);
	}
};

template <class OP>
static void PadFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vector = args.data[0];
	auto &len_vector = args.data[1];
	auto &pad_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, int32_t, string_t, string_t>(
	    str_vector, len_vector, pad_vector, result, args.size(), [&](string_t str, int32_t len, string_t pad) {
		    len = MaxValue<int32_t>(len, 0);
		    return StringVector::AddString(result, OP::Operation(str, len, pad, buffer));
	    });
}

ScalarFunction LpadFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      PadFunction<LeftPadOperator>);
}

ScalarFunction RpadFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      PadFunction<RightPadOperator>);
}

} // namespace duckdb






namespace duckdb {

struct FMTPrintf {
	template <class CTX>
	static string OP(const char *format_str, vector<duckdb_fmt::basic_format_arg<CTX>> &format_args) {
		return duckdb_fmt::vsprintf(
		    format_str, duckdb_fmt::basic_format_args<CTX>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

struct FMTFormat {
	template <class CTX>
	static string OP(const char *format_str, vector<duckdb_fmt::basic_format_arg<CTX>> &format_args) {
		return duckdb_fmt::vformat(
		    format_str, duckdb_fmt::basic_format_args<CTX>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

unique_ptr<FunctionData> BindPrintfFunction(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	for (idx_t i = 1; i < arguments.size(); i++) {
		switch (arguments[i]->return_type.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::VARCHAR:
			// these types are natively supported
			bound_function.arguments.push_back(arguments[i]->return_type);
			break;
		case LogicalTypeId::DECIMAL:
			// decimal type: add cast to double
			bound_function.arguments.emplace_back(LogicalType::DOUBLE);
			break;
		case LogicalTypeId::UNKNOWN:
			// parameter: accept any input and rebind later
			bound_function.arguments.emplace_back(LogicalType::ANY);
			break;
		default:
			// all other types: add cast to string
			bound_function.arguments.emplace_back(LogicalType::VARCHAR);
			break;
		}
	}
	return nullptr;
}

template <class FORMAT_FUN, class CTX>
static void PrintfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &format_string = args.data[0];
	auto &result_validity = FlatVector::Validity(result);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result_validity.Initialize(args.size());
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		switch (args.data[i].GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(args.data[i])) {
				// constant null! result is always NULL regardless of other input
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
				return;
			}
			break;
		default:
			// FLAT VECTOR, we can directly OR the nullmask
			args.data[i].Flatten(args.size());
			result.SetVectorType(VectorType::FLAT_VECTOR);
			result_validity.Combine(FlatVector::Validity(args.data[i]), args.size());
			break;
		}
	}
	idx_t count = result.GetVectorType() == VectorType::CONSTANT_VECTOR ? 1 : args.size();

	auto format_data = FlatVector::GetData<string_t>(format_string);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t idx = 0; idx < count; idx++) {
		if (result.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::IsNull(result, idx)) {
			// this entry is NULL: skip it
			continue;
		}

		// first fetch the format string
		auto fmt_idx = format_string.GetVectorType() == VectorType::CONSTANT_VECTOR ? 0 : idx;
		auto format_string = format_data[fmt_idx].GetString();

		// now gather all the format arguments
		vector<duckdb_fmt::basic_format_arg<CTX>> format_args;
		vector<unsafe_unique_array<data_t>> string_args;

		for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
			auto &col = args.data[col_idx];
			idx_t arg_idx = col.GetVectorType() == VectorType::CONSTANT_VECTOR ? 0 : idx;
			switch (col.GetType().id()) {
			case LogicalTypeId::BOOLEAN: {
				auto arg_data = FlatVector::GetData<bool>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::TINYINT: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::SMALLINT: {
				auto arg_data = FlatVector::GetData<int16_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::INTEGER: {
				auto arg_data = FlatVector::GetData<int32_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::BIGINT: {
				auto arg_data = FlatVector::GetData<int64_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::FLOAT: {
				auto arg_data = FlatVector::GetData<float>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::DOUBLE: {
				auto arg_data = FlatVector::GetData<double>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::VARCHAR: {
				auto arg_data = FlatVector::GetData<string_t>(col);
				auto string_view =
				    duckdb_fmt::basic_string_view<char>(arg_data[arg_idx].GetData(), arg_data[arg_idx].GetSize());
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(string_view));
				break;
			}
			default:
				throw InternalException("Unexpected type for printf format");
			}
		}
		// finally actually perform the format
		string dynamic_result = FORMAT_FUN::template OP<CTX>(format_string.c_str(), format_args);
		result_data[idx] = StringVector::AddString(result, dynamic_result);
	}
}

ScalarFunction PrintfFun::GetFunction() {
	// duckdb_fmt::printf_context, duckdb_fmt::vsprintf
	ScalarFunction printf_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTPrintf, duckdb_fmt::printf_context>, BindPrintfFunction);
	printf_fun.varargs = LogicalType::ANY;
	return printf_fun;
}

ScalarFunction FormatFun::GetFunction() {
	// duckdb_fmt::format_context, duckdb_fmt::vformat
	ScalarFunction format_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTFormat, duckdb_fmt::format_context>, BindPrintfFunction);
	format_fun.varargs = LogicalType::ANY;
	return format_fun;
}

} // namespace duckdb




#include <ctype.h>
#include <string.h>

namespace duckdb {

static string_t RepeatScalarFunction(const string_t &str, const int64_t cnt, vector<char> &result) {
	// Get information about the repeated string
	auto input_str = str.GetData();
	auto size_str = str.GetSize();

	//  Reuse the buffer
	result.clear();
	for (auto remaining = cnt; remaining-- > 0;) {
		result.insert(result.end(), input_str, input_str + size_str);
	}

	return string_t(result.data(), result.size());
}

static void RepeatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &str_vector = args.data[0];
	auto &cnt_vector = args.data[1];

	vector<char> buffer;
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    str_vector, cnt_vector, result, args.size(), [&](string_t str, int64_t cnt) {
		    return StringVector::AddString(result, RepeatScalarFunction(str, cnt, buffer));
	    });
}

ScalarFunctionSet RepeatFun::GetFunctions() {
	ScalarFunctionSet repeat;
	for (const auto &type : {LogicalType::VARCHAR, LogicalType::BLOB}) {
		repeat.AddFunction(ScalarFunction({type, LogicalType::BIGINT}, type, RepeatFunction));
	}
	return repeat;
}

} // namespace duckdb






#include <string.h>
#include <ctype.h>
#include <unordered_map>

namespace duckdb {

static idx_t NextNeedle(const char *input_haystack, idx_t size_haystack, const char *input_needle,
                        const idx_t size_needle) {
	// Needle needs something to proceed
	if (size_needle > 0) {
		// Haystack should be bigger or equal size to the needle
		for (idx_t string_position = 0; (size_haystack - string_position) >= size_needle; ++string_position) {
			// Compare Needle to the Haystack
			if ((memcmp(input_haystack + string_position, input_needle, size_needle) == 0)) {
				return string_position;
			}
		}
	}
	// Did not find the needle
	return size_haystack;
}

static string_t ReplaceScalarFunction(const string_t &haystack, const string_t &needle, const string_t &thread,
                                      vector<char> &result) {
	// Get information about the needle, the haystack and the "thread"
	auto input_haystack = haystack.GetData();
	auto size_haystack = haystack.GetSize();

	auto input_needle = needle.GetData();
	auto size_needle = needle.GetSize();

	auto input_thread = thread.GetData();
	auto size_thread = thread.GetSize();

	//  Reuse the buffer
	result.clear();

	for (;;) {
		//  Append the non-matching characters
		auto string_position = NextNeedle(input_haystack, size_haystack, input_needle, size_needle);
		result.insert(result.end(), input_haystack, input_haystack + string_position);
		input_haystack += string_position;
		size_haystack -= string_position;

		//  Stop when we have read the entire haystack
		if (size_haystack == 0) {
			break;
		}

		//  Replace the matching characters
		result.insert(result.end(), input_thread, input_thread + size_thread);
		input_haystack += size_needle;
		size_haystack -= size_needle;
	}

	return string_t(result.data(), result.size());
}

static void ReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &haystack_vector = args.data[0];
	auto &needle_vector = args.data[1];
	auto &thread_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    haystack_vector, needle_vector, thread_vector, result, args.size(),
	    [&](string_t input_string, string_t needle_string, string_t thread_string) {
		    return StringVector::AddString(result,
		                                   ReplaceScalarFunction(input_string, needle_string, thread_string, buffer));
	    });
}

ScalarFunction ReplaceFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      ReplaceFunction);
}

} // namespace duckdb







#include <string.h>

namespace duckdb {

//! Fast ASCII string reverse, returns false if the input data is not ascii
static bool StrReverseASCII(const char *input, idx_t n, char *output) {
	for (idx_t i = 0; i < n; i++) {
		if (input[i] & 0x80) {
			// non-ascii character
			return false;
		}
		output[n - i - 1] = input[i];
	}
	return true;
}

//! Unicode string reverse using grapheme breakers
static void StrReverseUnicode(const char *input, idx_t n, char *output) {
	utf8proc_grapheme_callback(input, n, [&](size_t start, size_t end) {
		memcpy(output + n - end, input + start, end - start);
		return true;
	});
}

struct ReverseOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();

		auto target = StringVector::EmptyString(result, input_length);
		auto target_data = target.GetDataWriteable();
		if (!StrReverseASCII(input_data, input_length, target_data)) {
			StrReverseUnicode(input_data, input_length, target_data);
		}
		target.Finalize();
		return target;
	}
};

static void ReverseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, ReverseOperator>(args.data[0], result, args.size());
}

ScalarFunction ReverseFun::GetFunction() {
	return ScalarFunction("reverse", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ReverseFunction);
}

} // namespace duckdb





namespace duckdb {

struct SHA256Operator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto hash = StringVector::EmptyString(result, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT);

		duckdb_mbedtls::MbedTlsWrapper::SHA256State state;
		state.AddString(input.GetString());
		state.FinishHex(hash.GetDataWriteable());

		hash.Finalize();
		return hash;
	}
};

static void SHA256Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::ExecuteString<string_t, string_t, SHA256Operator>(input, result, args.size());
}

ScalarFunction SHA256Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, SHA256Function);
}

} // namespace duckdb






namespace duckdb {

static bool StartsWith(const unsigned char *haystack, idx_t haystack_size, const unsigned char *needle,
                       idx_t needle_size) {
	D_ASSERT(needle_size > 0);
	if (needle_size > haystack_size) {
		// needle is bigger than haystack: haystack cannot start with needle
		return false;
	}
	return memcmp(haystack, needle, needle_size) == 0;
}

static bool StartsWith(const string_t &haystack_s, const string_t &needle_s) {

	auto haystack = const_uchar_ptr_cast(haystack_s.GetData());
	auto haystack_size = haystack_s.GetSize();
	auto needle = const_uchar_ptr_cast(needle_s.GetData());
	auto needle_size = needle_s.GetSize();
	if (needle_size == 0) {
		// empty needle: always true
		return true;
	}
	return StartsWith(haystack, haystack_size, needle, needle_size);
}

struct StartsWithOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
		return StartsWith(left, right);
	}
};

ScalarFunction StartsWithOperatorFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                      ScalarFunction::BinaryFunction<string_t, string_t, bool, StartsWithOperator>);
}

} // namespace duckdb









namespace duckdb {

struct StringSplitInput {
	StringSplitInput(Vector &result_list, Vector &result_child, idx_t offset)
	    : result_list(result_list), result_child(result_child), offset(offset) {
	}

	Vector &result_list;
	Vector &result_child;
	idx_t offset;

	void AddSplit(const char *split_data, idx_t split_size, idx_t list_idx) {
		auto list_entry = offset + list_idx;
		if (list_entry >= ListVector::GetListCapacity(result_list)) {
			ListVector::SetListSize(result_list, offset + list_idx);
			ListVector::Reserve(result_list, ListVector::GetListCapacity(result_list) * 2);
		}
		FlatVector::GetData<string_t>(result_child)[list_entry] =
		    StringVector::AddString(result_child, split_data, split_size);
	}
};

struct RegularStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		match_size = delim_size;
		if (delim_size == 0) {
			return 0;
		}
		return ContainsFun::Find(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(delim_data),
		                         delim_size);
	}
};

struct ConstantRegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		D_ASSERT(data);
		auto regex = reinterpret_cast<duckdb_re2::RE2 *>(data);
		duckdb_re2::StringPiece match;
		if (!regex->Match(duckdb_re2::StringPiece(input_data, input_size), 0, input_size, RE2::UNANCHORED, &match, 1)) {
			return DConstants::INVALID_INDEX;
		}
		match_size = match.size();
		return match.data() - input_data;
	}
};

struct RegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		duckdb_re2::RE2 regex(duckdb_re2::StringPiece(delim_data, delim_size));
		if (!regex.ok()) {
			throw InvalidInputException(regex.error());
		}
		return ConstantRegexpStringSplit::Find(input_data, input_size, delim_data, delim_size, match_size, &regex);
	}
};

struct StringSplitter {
	template <class OP>
	static idx_t Split(string_t input, string_t delim, StringSplitInput &state, void *data) {
		auto input_data = input.GetData();
		auto input_size = input.GetSize();
		auto delim_data = delim.GetData();
		auto delim_size = delim.GetSize();
		idx_t list_idx = 0;
		while (input_size > 0) {
			idx_t match_size = 0;
			auto pos = OP::Find(input_data, input_size, delim_data, delim_size, match_size, data);
			if (pos > input_size) {
				break;
			}
			if (match_size == 0 && pos == 0) {
				// special case: 0 length match and pos is 0
				// move to the next character
				for (pos++; pos < input_size; pos++) {
					if (LengthFun::IsCharacter(input_data[pos])) {
						break;
					}
				}
				if (pos == input_size) {
					break;
				}
			}
			D_ASSERT(input_size >= pos + match_size);
			state.AddSplit(input_data, pos, list_idx);

			list_idx++;
			input_data += (pos + match_size);
			input_size -= (pos + match_size);
		}
		state.AddSplit(input_data, input_size, list_idx);
		list_idx++;
		return list_idx;
	}
};

template <class OP>
static void StringSplitExecutor(DataChunk &args, ExpressionState &state, Vector &result, void *data = nullptr) {
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(args.size(), input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	UnifiedVectorFormat delim_data;
	args.data[1].ToUnifiedFormat(args.size(), delim_data);
	auto delims = UnifiedVectorFormat::GetData<string_t>(delim_data);

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::SetListSize(result, 0);

	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);

	// count all the splits and set up the list entries
	auto &child_entry = ListVector::GetEntry(result);
	auto &result_mask = FlatVector::Validity(result);
	idx_t total_splits = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		auto input_idx = input_data.sel->get_index(i);
		auto delim_idx = delim_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(input_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		StringSplitInput split_input(result, child_entry, total_splits);
		if (!delim_data.validity.RowIsValid(delim_idx)) {
			// delim is NULL: copy the complete entry
			split_input.AddSplit(inputs[input_idx].GetData(), inputs[input_idx].GetSize(), 0);
			list_struct_data[i].length = 1;
			list_struct_data[i].offset = total_splits;
			total_splits++;
			continue;
		}
		auto list_length = StringSplitter::Split<OP>(inputs[input_idx], delims[delim_idx], split_input, data);
		list_struct_data[i].length = list_length;
		list_struct_data[i].offset = total_splits;
		total_splits += list_length;
	}
	ListVector::SetListSize(result, total_splits);
	D_ASSERT(ListVector::GetListSize(result) == total_splits);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void StringSplitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	StringSplitExecutor<RegularStringSplit>(args, state, result, nullptr);
}

static void StringSplitRegexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RegexpMatchesBindData>();
	if (info.constant_pattern) {
		// fast path: pre-compiled regex
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
		StringSplitExecutor<ConstantRegexpStringSplit>(args, state, result, &lstate.constant_pattern);
	} else {
		// slow path: have to re-compile regex for every row
		StringSplitExecutor<RegexpStringSplit>(args, state, result);
	}
}

ScalarFunction StringSplitFun::GetFunction() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);

	ScalarFunction string_split({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitFunction);
	string_split.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return string_split;
}

ScalarFunctionSet StringSplitRegexFun::GetFunctions() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet regexp_split;
	ScalarFunction regex_fun({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitRegexFunction,
	                         RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	                         FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING);
	regexp_split.AddFunction(regex_fun);
	// regexp options
	regex_fun.arguments.emplace_back(LogicalType::VARCHAR);
	regexp_split.AddFunction(regex_fun);
	return regexp_split;
}

} // namespace duckdb




namespace duckdb {

static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

static unique_ptr<FunctionData> ToBaseBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	// If no min_length is specified, default to 0
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	if (arguments.size() == 2) {
		arguments.push_back(make_uniq_base<Expression, BoundConstantExpression>(Value::INTEGER(0)));
	}
	return nullptr;
}

static void ToBaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto &radix = args.data[1];
	auto &min_length = args.data[2];
	auto count = args.size();

	TernaryExecutor::Execute<int64_t, int32_t, int32_t, string_t>(
	    input, radix, min_length, result, count, [&](int64_t input, int32_t radix, int32_t min_length) {
		    if (input < 0) {
			    throw InvalidInputException("'to_base' number must be greater than or equal to 0");
		    }
		    if (radix < 2 || radix > 36) {
			    throw InvalidInputException("'to_base' radix must be between 2 and 36");
		    }
		    if (min_length > 64 || min_length < 0) {
			    throw InvalidInputException("'to_base' min_length must be between 0 and 64");
		    }

		    char buf[64];
		    char *end = buf + sizeof(buf);
		    char *ptr = end;
		    do {
			    *--ptr = alphabet[input % radix];
			    input /= radix;
		    } while (input > 0);

		    auto length = end - ptr;
		    while (length < min_length) {
			    *--ptr = '0';
			    length++;
		    }

		    return StringVector::AddString(result, ptr, end - ptr);
	    });
}

ScalarFunctionSet ToBaseFun::GetFunctions() {
	ScalarFunctionSet set("to_base");

	set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER}, LogicalType::VARCHAR, ToBaseFunction, ToBaseBind));
	set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::INTEGER},
	                               LogicalType::VARCHAR, ToBaseFunction, ToBaseBind));

	return set;
}

} // namespace duckdb








#include <string.h>
#include <ctype.h>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

static string_t TranslateScalarFunction(const string_t &haystack, const string_t &needle, const string_t &thread,
                                        vector<char> &result) {
	// Get information about the haystack, the needle and the "thread"
	auto input_haystack = haystack.GetData();
	auto size_haystack = haystack.GetSize();

	auto input_needle = needle.GetData();
	auto size_needle = needle.GetSize();

	auto input_thread = thread.GetData();
	auto size_thread = thread.GetSize();

	// Reuse the buffer
	result.clear();
	result.reserve(size_haystack);

	idx_t i = 0, j = 0;
	int sz = 0, c_sz = 0;

	// Character to be replaced
	unordered_map<int32_t, int32_t> to_replace;
	while (i < size_needle && j < size_thread) {
		auto codepoint_needle = Utf8Proc::UTF8ToCodepoint(input_needle, sz);
		input_needle += sz;
		i += sz;
		auto codepoint_thread = Utf8Proc::UTF8ToCodepoint(input_thread, sz);
		input_thread += sz;
		j += sz;
		// Ignore unicode character that is existed in to_replace
		if (to_replace.count(codepoint_needle) == 0) {
			to_replace[codepoint_needle] = codepoint_thread;
		}
	}

	// Character to be deleted
	unordered_set<int32_t> to_delete;
	while (i < size_needle) {
		auto codepoint_needle = Utf8Proc::UTF8ToCodepoint(input_needle, sz);
		input_needle += sz;
		i += sz;
		// Add unicode character that will be deleted
		if (to_replace.count(codepoint_needle) == 0) {
			to_delete.insert(codepoint_needle);
		}
	}

	char c[5] = {'\0', '\0', '\0', '\0', '\0'};
	for (i = 0; i < size_haystack; i += sz) {
		auto codepoint_haystack = Utf8Proc::UTF8ToCodepoint(input_haystack, sz);
		if (to_replace.count(codepoint_haystack) != 0) {
			Utf8Proc::CodepointToUtf8(to_replace[codepoint_haystack], c_sz, c);
			result.insert(result.end(), c, c + c_sz);
		} else if (to_delete.count(codepoint_haystack) == 0) {
			result.insert(result.end(), input_haystack, input_haystack + sz);
		}
		input_haystack += sz;
	}

	return string_t(result.data(), result.size());
}

static void TranslateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &haystack_vector = args.data[0];
	auto &needle_vector = args.data[1];
	auto &thread_vector = args.data[2];

	vector<char> buffer;
	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    haystack_vector, needle_vector, thread_vector, result, args.size(),
	    [&](string_t input_string, string_t needle_string, string_t thread_string) {
		    return StringVector::AddString(result,
		                                   TranslateScalarFunction(input_string, needle_string, thread_string, buffer));
	    });
}

ScalarFunction TranslateFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      TranslateFunction);
}

} // namespace duckdb







#include <string.h>

namespace duckdb {

template <bool LTRIM, bool RTRIM>
struct TrimOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto data = input.GetData();
		auto size = input.GetSize();

		utf8proc_int32_t codepoint;
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		// Find the first character that is not left trimmed
		idx_t begin = 0;
		if (LTRIM) {
			while (begin < size) {
				auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
				D_ASSERT(bytes > 0);
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					break;
				}
				begin += bytes;
			}
		}

		// Find the last character that is not right trimmed
		idx_t end;
		if (RTRIM) {
			end = begin;
			for (auto next = begin; next < size;) {
				auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
				D_ASSERT(bytes > 0);
				next += bytes;
				if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
					end = next;
				}
			}
		} else {
			end = size;
		}

		// Copy the trimmed string
		auto target = StringVector::EmptyString(result, end - begin);
		auto output = target.GetDataWriteable();
		memcpy(output, data + begin, end - begin);

		target.Finalize();
		return target;
	}
};

template <bool LTRIM, bool RTRIM>
static void UnaryTrimFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::ExecuteString<string_t, string_t, TrimOperator<LTRIM, RTRIM>>(args.data[0], result, args.size());
}

static void GetIgnoredCodepoints(string_t ignored, unordered_set<utf8proc_int32_t> &ignored_codepoints) {
	auto dataptr = reinterpret_cast<const utf8proc_uint8_t *>(ignored.GetData());
	auto size = ignored.GetSize();
	idx_t pos = 0;
	while (pos < size) {
		utf8proc_int32_t codepoint;
		pos += utf8proc_iterate(dataptr + pos, size - pos, &codepoint);
		ignored_codepoints.insert(codepoint);
	}
}

template <bool LTRIM, bool RTRIM>
static void BinaryTrimFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    input.data[0], input.data[1], result, input.size(), [&](string_t input, string_t ignored) {
		    auto data = input.GetData();
		    auto size = input.GetSize();

		    unordered_set<utf8proc_int32_t> ignored_codepoints;
		    GetIgnoredCodepoints(ignored, ignored_codepoints);

		    utf8proc_int32_t codepoint;
		    auto str = reinterpret_cast<const utf8proc_uint8_t *>(data);

		    // Find the first character that is not left trimmed
		    idx_t begin = 0;
		    if (LTRIM) {
			    while (begin < size) {
				    auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
				    if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					    break;
				    }
				    begin += bytes;
			    }
		    }

		    // Find the last character that is not right trimmed
		    idx_t end;
		    if (RTRIM) {
			    end = begin;
			    for (auto next = begin; next < size;) {
				    auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
				    D_ASSERT(bytes > 0);
				    next += bytes;
				    if (ignored_codepoints.find(codepoint) == ignored_codepoints.end()) {
					    end = next;
				    }
			    }
		    } else {
			    end = size;
		    }

		    // Copy the trimmed string
		    auto target = StringVector::EmptyString(result, end - begin);
		    auto output = target.GetDataWriteable();
		    memcpy(output, data + begin, end - begin);

		    target.Finalize();
		    return target;
	    });
}

ScalarFunctionSet TrimFun::GetFunctions() {
	ScalarFunctionSet trim;
	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<true, true>));

	trim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                BinaryTrimFunction<true, true>));
	return trim;
}

ScalarFunctionSet LtrimFun::GetFunctions() {
	ScalarFunctionSet ltrim;
	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<true, false>));
	ltrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                 BinaryTrimFunction<true, false>));
	return ltrim;
}

ScalarFunctionSet RtrimFun::GetFunctions() {
	ScalarFunctionSet rtrim;
	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, UnaryTrimFunction<false, true>));

	rtrim.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                 BinaryTrimFunction<false, true>));
	return rtrim;
}

} // namespace duckdb







#include <string.h>

namespace duckdb {

struct UnicodeOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		auto str = reinterpret_cast<const utf8proc_uint8_t *>(input.GetData());
		auto len = input.GetSize();
		utf8proc_int32_t codepoint;
		(void)utf8proc_iterate(str, len, &codepoint);
		return codepoint;
	}
};

ScalarFunction UnicodeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
	                      ScalarFunction::UnaryFunction<string_t, int32_t, UnicodeOperator>);
}

} // namespace duckdb









namespace duckdb {

static void StructInsertFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &starting_vec = args.data[0];

	starting_vec.Verify(args.size());

	auto &starting_child_entries = StructVector::GetEntries(starting_vec);
	auto &result_child_entries = StructVector::GetEntries(result);

	// Assign the starting vector entries to the result vector
	for (size_t i = 0; i < starting_child_entries.size(); i++) {
		auto &starting_child = starting_child_entries[i];
		result_child_entries[i]->Reference(*starting_child);
	}

	// Assign the new entries to the result vector
	for (size_t i = 1; i < args.ColumnCount(); i++) {
		result_child_entries[starting_child_entries.size() + i - 1]->Reference(args.data[i]);
	}

	result.Verify(args.size());

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> StructInsertBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	case_insensitive_set_t name_collision_set;

	if (arguments.empty()) {
		throw Exception("Missing required arguments for struct_insert function.");
	}

	if (LogicalTypeId::STRUCT != arguments[0]->return_type.id()) {
		throw Exception("The first argument to struct_insert must be a STRUCT");
	}

	if (arguments.size() < 2) {
		throw Exception("Can't insert nothing into a struct");
	}

	child_list_t<LogicalType> new_struct_children;

	auto &existing_struct_children = StructType::GetChildTypes(arguments[0]->return_type);

	for (size_t i = 0; i < existing_struct_children.size(); i++) {
		auto &child = existing_struct_children[i];
		name_collision_set.insert(child.first);
		new_struct_children.push_back(make_pair(child.first, child.second));
	}

	// Loop through the additional arguments (name/value pairs)
	for (idx_t i = 1; i < arguments.size(); i++) {
		auto &child = arguments[i];
		if (child->alias.empty() && bound_function.name == "struct_insert") {
			throw BinderException("Need named argument for struct insert, e.g. STRUCT_PACK(a := b)");
		}
		if (name_collision_set.find(child->alias) != name_collision_set.end()) {
			throw BinderException("Duplicate struct entry name \"%s\"", child->alias);
		}
		name_collision_set.insert(child->alias);
		new_struct_children.push_back(make_pair(child->alias, arguments[i]->return_type));
	}

	// this is more for completeness reasons
	bound_function.return_type = LogicalType::STRUCT(new_struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructInsertStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto new_struct_stats = StructStats::CreateUnknown(expr.return_type);

	auto existing_count = StructType::GetChildCount(child_stats[0].GetType());
	auto existing_stats = StructStats::GetChildStats(child_stats[0]);
	for (idx_t i = 0; i < existing_count; i++) {
		StructStats::SetChildStats(new_struct_stats, i, existing_stats[i]);
	}
	auto new_count = StructType::GetChildCount(expr.return_type);
	auto offset = new_count - child_stats.size();
	for (idx_t i = 1; i < child_stats.size(); i++) {
		StructStats::SetChildStats(new_struct_stats, offset + i, child_stats[i]);
	}
	return new_struct_stats.ToUnique();
}

ScalarFunction StructInsertFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::STRUCT, StructInsertFunction, StructInsertBind, nullptr, StructInsertStats);
	fun.varargs = LogicalType::ANY;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb









namespace duckdb {

static void StructPackFunction(DataChunk &args, ExpressionState &state, Vector &result) {
#ifdef DEBUG
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<VariableReturnBindData>();
	// this should never happen if the binder below is sane
	D_ASSERT(args.ColumnCount() == StructType::GetChildTypes(info.stype).size());
#endif
	bool all_const = true;
	auto &child_entries = StructVector::GetEntries(result);
	for (size_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_const = false;
		}
		// same holds for this
		child_entries[i]->Reference(args.data[i]);
	}
	result.SetVectorType(all_const ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);

	result.Verify(args.size());
}

template <bool IS_STRUCT_PACK>
static unique_ptr<FunctionData> StructPackBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	case_insensitive_set_t name_collision_set;

	// collect names and deconflict, construct return type
	if (arguments.empty()) {
		throw Exception("Can't pack nothing into a struct");
	}
	child_list_t<LogicalType> struct_children;
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &child = arguments[i];
		string alias;
		if (IS_STRUCT_PACK) {
			if (child->alias.empty()) {
				throw BinderException("Need named argument for struct pack, e.g. STRUCT_PACK(a := b)");
			}
			alias = child->alias;
			if (name_collision_set.find(alias) != name_collision_set.end()) {
				throw BinderException("Duplicate struct entry name \"%s\"", alias);
			}
			name_collision_set.insert(alias);
		}
		struct_children.push_back(make_pair(alias, arguments[i]->return_type));
	}

	// this is more for completeness reasons
	bound_function.return_type = LogicalType::STRUCT(struct_children);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

unique_ptr<BaseStatistics> StructPackStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &expr = input.expr;
	auto struct_stats = StructStats::CreateUnknown(expr.return_type);
	for (idx_t i = 0; i < child_stats.size(); i++) {
		StructStats::SetChildStats(struct_stats, i, child_stats[i]);
	}
	return struct_stats.ToUnique();
}

template <bool IS_STRUCT_PACK>
ScalarFunction GetStructPackFunction() {
	ScalarFunction fun(IS_STRUCT_PACK ? "struct_pack" : "row", {}, LogicalTypeId::STRUCT, StructPackFunction,
	                   StructPackBind<IS_STRUCT_PACK>, nullptr, StructPackStats);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

ScalarFunction StructPackFun::GetFunction() {
	return GetStructPackFunction<true>();
}

ScalarFunction RowFun::GetFunction() {
	return GetStructPackFunction<false>();
}

} // namespace duckdb






namespace duckdb {

struct UnionExtractBindData : public FunctionData {
	UnionExtractBindData(string key, idx_t index, LogicalType type)
	    : key(std::move(key)), index(index), type(std::move(type)) {
	}

	string key;
	idx_t index;
	LogicalType type;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnionExtractBindData>(key, index, type);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnionExtractBindData>();
		return key == other.key && index == other.index && type == other.type;
	}
};

static void UnionExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<UnionExtractBindData>();

	// this should be guaranteed by the binder
	auto &vec = args.data[0];
	vec.Verify(args.size());

	D_ASSERT(info.index < UnionType::GetMemberCount(vec.GetType()));
	auto &member = UnionVector::GetMember(vec, info.index);
	result.Reference(member);
	result.Verify(args.size());
}

static unique_ptr<FunctionData> UnionExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(LogicalTypeId::UNION == arguments[0]->return_type.id());
	idx_t union_member_count = UnionType::GetMemberCount(arguments[0]->return_type);
	if (union_member_count == 0) {
		throw InternalException("Can't extract something from an empty union");
	}
	bound_function.arguments[0] = arguments[0]->return_type;

	auto &key_child = arguments[1];
	if (key_child->HasParameter()) {
		throw ParameterNotResolvedException();
	}

	if (key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw BinderException("Key name for union_extract needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	auto &key_str = StringValue::Get(key_val);
	if (key_val.IsNull() || key_str.empty()) {
		throw BinderException("Key name for union_extract needs to be neither NULL nor empty");
	}
	string key = StringUtil::Lower(key_str);

	LogicalType return_type;
	idx_t key_index = 0;
	bool found_key = false;

	for (size_t i = 0; i < union_member_count; i++) {
		auto &member_name = UnionType::GetMemberName(arguments[0]->return_type, i);
		if (StringUtil::Lower(member_name) == key) {
			found_key = true;
			key_index = i;
			return_type = UnionType::GetMemberType(arguments[0]->return_type, i);
			break;
		}
	}

	if (!found_key) {
		vector<string> candidates;
		candidates.reserve(union_member_count);
		for (idx_t i = 0; i < union_member_count; i++) {
			candidates.push_back(UnionType::GetMemberName(arguments[0]->return_type, i));
		}
		auto closest_settings = StringUtil::TopNLevenshtein(candidates, key);
		auto message = StringUtil::CandidatesMessage(closest_settings, "Candidate Entries");
		throw BinderException("Could not find key \"%s\" in union\n%s", key, message);
	}

	bound_function.return_type = return_type;
	return make_uniq<UnionExtractBindData>(key, key_index, return_type);
}

ScalarFunction UnionExtractFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	return ScalarFunction({LogicalTypeId::UNION, LogicalType::VARCHAR}, LogicalType::ANY, UnionExtractFunction,
	                      UnionExtractBind, nullptr, nullptr);
}

} // namespace duckdb






namespace duckdb {

static unique_ptr<FunctionData> UnionTagBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {

	if (arguments.empty()) {
		throw BinderException("Missing required arguments for union_tag function.");
	}

	if (LogicalTypeId::UNKNOWN == arguments[0]->return_type.id()) {
		throw ParameterNotResolvedException();
	}

	if (LogicalTypeId::UNION != arguments[0]->return_type.id()) {
		throw BinderException("First argument to union_tag function must be a union type.");
	}

	if (arguments.size() > 1) {
		throw BinderException("Too many arguments, union_tag takes at most one argument.");
	}

	auto member_count = UnionType::GetMemberCount(arguments[0]->return_type);
	if (member_count == 0) {
		// this should never happen, empty unions are not allowed
		throw InternalException("Can't get tags from an empty union");
	}

	bound_function.arguments[0] = arguments[0]->return_type;

	auto varchar_vector = Vector(LogicalType::VARCHAR, member_count);
	for (idx_t i = 0; i < member_count; i++) {
		auto str = string_t(UnionType::GetMemberName(arguments[0]->return_type, i));
		FlatVector::GetData<string_t>(varchar_vector)[i] =
		    str.IsInlined() ? str : StringVector::AddString(varchar_vector, str);
	}
	auto enum_type = LogicalType::ENUM(varchar_vector, member_count);
	bound_function.return_type = enum_type;

	return nullptr;
}

static void UnionTagFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::ENUM);
	result.Reinterpret(UnionVector::GetTags(args.data[0]));
}

ScalarFunction UnionTagFun::GetFunction() {
	return ScalarFunction({LogicalTypeId::UNION}, LogicalTypeId::ANY, UnionTagFunction, UnionTagBind, nullptr,
	                      nullptr); // TODO: Statistics?
}

} // namespace duckdb







namespace duckdb {

struct UnionValueBindData : public FunctionData {
	UnionValueBindData() {
	}

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnionValueBindData>();
	}
	bool Equals(const FunctionData &other_p) const override {
		return true;
	}
};

static void UnionValueFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// Assign the new entries to the result vector
	UnionVector::GetMember(result, 0).Reference(args.data[0]);

	// Set the result tag vector to a constant value
	auto &tag_vector = UnionVector::GetTags(result);
	tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<union_tag_t>(tag_vector)[0] = 0;

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> UnionValueBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {

	if (arguments.size() != 1) {
		throw BinderException("union_value takes exactly one argument");
	}
	auto &child = arguments[0];

	if (child->alias.empty()) {
		throw BinderException("Need named argument for union tag, e.g. UNION_VALUE(a := b)");
	}

	child_list_t<LogicalType> union_members;

	union_members.push_back(make_pair(child->alias, child->return_type));

	bound_function.return_type = LogicalType::UNION(std::move(union_members));
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction UnionValueFun::GetFunction() {
	ScalarFunction fun("union_value", {}, LogicalTypeId::UNION, UnionValueFunction, UnionValueBind, nullptr, nullptr);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.serialize = VariableReturnBindData::Serialize;
	fun.deserialize = VariableReturnBindData::Deserialize;
	return fun;
}

} // namespace duckdb





namespace duckdb {

AdaptiveFilter::AdaptiveFilter(const Expression &expr)
    : iteration_count(0), observe_interval(10), execute_interval(20), warmup(true) {
	auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
	D_ASSERT(conj_expr.children.size() > 1);
	for (idx_t idx = 0; idx < conj_expr.children.size(); idx++) {
		permutation.push_back(idx);
		if (idx != conj_expr.children.size() - 1) {
			swap_likeliness.push_back(100);
		}
	}
	right_random_border = 100 * (conj_expr.children.size() - 1);
}

AdaptiveFilter::AdaptiveFilter(TableFilterSet *table_filters)
    : iteration_count(0), observe_interval(10), execute_interval(20), warmup(true) {
	for (auto &table_filter : table_filters->filters) {
		permutation.push_back(table_filter.first);
		swap_likeliness.push_back(100);
	}
	swap_likeliness.pop_back();
	right_random_border = 100 * (table_filters->filters.size() - 1);
}
void AdaptiveFilter::AdaptRuntimeStatistics(double duration) {
	iteration_count++;
	runtime_sum += duration;

	if (!warmup) {
		// the last swap was observed
		if (observe && iteration_count == observe_interval) {
			// keep swap if runtime decreased, else reverse swap
			if (prev_mean - (runtime_sum / iteration_count) <= 0) {
				// reverse swap because runtime didn't decrease
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// decrease swap likeliness, but make sure there is always a small likeliness left
				if (swap_likeliness[swap_idx] > 1) {
					swap_likeliness[swap_idx] /= 2;
				}
			} else {
				// keep swap because runtime decreased, reset likeliness
				swap_likeliness[swap_idx] = 100;
			}
			observe = false;

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		} else if (!observe && iteration_count == execute_interval) {
			// save old mean to evaluate swap
			prev_mean = runtime_sum / iteration_count;

			// get swap index and swap likeliness
			std::uniform_int_distribution<int> distribution(1, right_random_border); // a <= i <= b
			idx_t random_number = distribution(generator) - 1;

			swap_idx = random_number / 100;                    // index to be swapped
			idx_t likeliness = random_number - 100 * swap_idx; // random number between [0, 100)

			// check if swap is going to happen
			if (swap_likeliness[swap_idx] > likeliness) { // always true for the first swap of an index
				// swap
				std::swap(permutation[swap_idx], permutation[swap_idx + 1]);

				// observe whether swap will be applied
				observe = true;
			}

			// reset values
			iteration_count = 0;
			runtime_sum = 0.0;
		}
	} else {
		if (iteration_count == 5) {
			// initially set all values
			iteration_count = 0;
			runtime_sum = 0.0;
			observe = false;
			warmup = false;
		}
	}
}

} // namespace duckdb













namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types, vector<LogicalType> payload_types,
                                                     const vector<BoundAggregateExpression *> &bindings,
                                                     idx_t initial_capacity, idx_t radix_bits)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), std::move(payload_types),
                                AggregateObject::CreateAggregateObjects(bindings), initial_capacity, radix_bits) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types)
    : GroupedAggregateHashTable(context, allocator, std::move(group_types), {}, vector<AggregateObject>()) {
}

GroupedAggregateHashTable::AggregateHTAppendState::AggregateHTAppendState()
    : ht_offsets(LogicalType::UBIGINT), hash_salts(LogicalType::HASH), group_compare_vector(STANDARD_VECTOR_SIZE),
      no_match_vector(STANDARD_VECTOR_SIZE), empty_vector(STANDARD_VECTOR_SIZE), new_groups(STANDARD_VECTOR_SIZE),
      addresses(LogicalType::POINTER) {
}

GroupedAggregateHashTable::GroupedAggregateHashTable(ClientContext &context, Allocator &allocator,
                                                     vector<LogicalType> group_types_p,
                                                     vector<LogicalType> payload_types_p,
                                                     vector<AggregateObject> aggregate_objects_p,
                                                     idx_t initial_capacity, idx_t radix_bits)
    : BaseAggregateHashTable(context, allocator, aggregate_objects_p, std::move(payload_types_p)),
      radix_bits(radix_bits), count(0), capacity(0), aggregate_allocator(make_shared<ArenaAllocator>(allocator)) {

	// Append hash column to the end and initialise the row layout
	group_types_p.emplace_back(LogicalType::HASH);
	layout.Initialize(std::move(group_types_p), std::move(aggregate_objects_p));

	hash_offset = layout.GetOffsets()[layout.ColumnCount() - 1];

	// Partitioned data and pointer table
	InitializePartitionedData();
	Resize(initial_capacity);

	// Predicates
	predicates.resize(layout.ColumnCount() - 1, ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	row_matcher.Initialize(true, layout, predicates);
}

void GroupedAggregateHashTable::InitializePartitionedData() {
	if (!partitioned_data || RadixPartitioning::RadixBits(partitioned_data->PartitionCount()) != radix_bits) {
		D_ASSERT(!partitioned_data || partitioned_data->Count() == 0);
		partitioned_data =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
	} else {
		partitioned_data->Reset();
	}

	D_ASSERT(GetLayout().GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(GetLayout().GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(GetLayout().GetRowWidth() == layout.GetRowWidth());

	partitioned_data->InitializeAppendState(state.append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
}

unique_ptr<PartitionedTupleData> &GroupedAggregateHashTable::GetPartitionedData() {
	return partitioned_data;
}

shared_ptr<ArenaAllocator> GroupedAggregateHashTable::GetAggregateAllocator() {
	return aggregate_allocator;
}

GroupedAggregateHashTable::~GroupedAggregateHashTable() {
	Destroy();
}

void GroupedAggregateHashTable::Destroy() {
	if (!partitioned_data || partitioned_data->Count() == 0 || !layout.HasDestructor()) {
		return;
	}

	// There are aggregates with destructors: Call the destructor for each of the aggregates
	// Currently does not happen because aggregate destructors are called while scanning in RadixPartitionedHashTable
	// LCOV_EXCL_START
	RowOperationsState row_state(*aggregate_allocator);
	for (auto &data_collection : partitioned_data->GetPartitions()) {
		if (data_collection->Count() == 0) {
			continue;
		}
		TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::DESTROY_AFTER_DONE, false);
		auto &row_locations = iterator.GetChunkState().row_locations;
		do {
			RowOperations::DestroyStates(row_state, layout, row_locations, iterator.GetCurrentChunkCount());
		} while (iterator.Next());
		data_collection->Reset();
	}
	// LCOV_EXCL_STOP
}

const TupleDataLayout &GroupedAggregateHashTable::GetLayout() const {
	return partitioned_data->GetLayout();
}

idx_t GroupedAggregateHashTable::Count() const {
	return count;
}

idx_t GroupedAggregateHashTable::InitialCapacity() {
	return STANDARD_VECTOR_SIZE * 2ULL;
}

idx_t GroupedAggregateHashTable::GetCapacityForCount(idx_t count) {
	count = MaxValue<idx_t>(InitialCapacity(), count);
	return NextPowerOfTwo(count * LOAD_FACTOR);
}

idx_t GroupedAggregateHashTable::Capacity() const {
	return capacity;
}

idx_t GroupedAggregateHashTable::ResizeThreshold() const {
	return Capacity() / LOAD_FACTOR;
}

idx_t GroupedAggregateHashTable::ApplyBitMask(hash_t hash) const {
	return hash & bitmask;
}

void GroupedAggregateHashTable::Verify() {
#ifdef DEBUG
	idx_t total_count = 0;
	for (idx_t i = 0; i < capacity; i++) {
		const auto &entry = entries[i];
		if (!entry.IsOccupied()) {
			continue;
		}
		auto hash = Load<hash_t>(entry.GetPointer() + hash_offset);
		D_ASSERT(entry.GetSalt() == aggr_ht_entry_t::ExtractSalt(hash));
		total_count++;
	}
	D_ASSERT(total_count == Count());
#endif
}

void GroupedAggregateHashTable::ClearPointerTable() {
	std::fill_n(entries, capacity, aggr_ht_entry_t(0));
}

void GroupedAggregateHashTable::ResetCount() {
	count = 0;
}

void GroupedAggregateHashTable::SetRadixBits(idx_t radix_bits_p) {
	radix_bits = radix_bits_p;
}

void GroupedAggregateHashTable::Resize(idx_t size) {
	D_ASSERT(size >= STANDARD_VECTOR_SIZE);
	D_ASSERT(IsPowerOfTwo(size));
	if (size < capacity) {
		throw InternalException("Cannot downsize a hash table!");
	}

	capacity = size;
	hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(aggr_ht_entry_t));
	entries = reinterpret_cast<aggr_ht_entry_t *>(hash_map.get());
	ClearPointerTable();
	bitmask = capacity - 1;

	if (Count() != 0) {
		for (auto &data_collection : partitioned_data->GetPartitions()) {
			if (data_collection->Count() == 0) {
				continue;
			}
			TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::ALREADY_PINNED, false);
			const auto row_locations = iterator.GetRowLocations();
			do {
				for (idx_t i = 0; i < iterator.GetCurrentChunkCount(); i++) {
					const auto &row_location = row_locations[i];
					const auto hash = Load<hash_t>(row_location + hash_offset);

					// Find an empty entry
					auto entry_idx = ApplyBitMask(hash);
					D_ASSERT(entry_idx == hash % capacity);
					while (entries[entry_idx].IsOccupied() > 0) {
						entry_idx++;
						if (entry_idx >= capacity) {
							entry_idx = 0;
						}
					}
					auto &entry = entries[entry_idx];
					D_ASSERT(!entry.IsOccupied());
					entry.SetSalt(aggr_ht_entry_t::ExtractSalt(hash));
					entry.SetPointer(row_location);
					D_ASSERT(entry.IsOccupied());
				}
			} while (iterator.Next());
		}
	}

	Verify();
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, AggregateType filter) {
	unsafe_vector<idx_t> aggregate_filter;

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		if (aggregate.aggr_type == filter) {
			aggregate_filter.push_back(i);
		}
	}
	return AddChunk(groups, payload, aggregate_filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, DataChunk &payload, const unsafe_vector<idx_t> &filter) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);

	return AddChunk(groups, hashes, payload, filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload,
                                          const unsafe_vector<idx_t> &filter) {
	if (groups.size() == 0) {
		return 0;
	}

#ifdef DEBUG
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < groups.ColumnCount(); i++) {
		D_ASSERT(groups.GetTypes()[i] == layout.GetTypes()[i]);
	}
#endif

	const auto new_group_count = FindOrCreateGroups(groups, group_hashes, state.addresses, state.new_groups);
	VectorOperations::AddInPlace(state.addresses, layout.GetAggrOffset(), payload.size());

	// Now every cell has an entry, update the aggregates
	auto &aggregates = layout.GetAggregates();
	idx_t filter_idx = 0;
	idx_t payload_idx = 0;
	RowOperationsState row_state(*aggregate_allocator);
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggr = aggregates[i];
		if (filter_idx >= filter.size() || i < filter[filter_idx]) {
			// Skip all the aggregates that are not in the filter
			payload_idx += aggr.child_count;
			VectorOperations::AddInPlace(state.addresses, aggr.payload_size, payload.size());
			continue;
		}
		D_ASSERT(i == filter[filter_idx]);

		if (aggr.aggr_type != AggregateType::DISTINCT && aggr.filter) {
			RowOperations::UpdateFilteredStates(row_state, filter_set.GetFilterData(i), aggr, state.addresses, payload,
			                                    payload_idx);
		} else {
			RowOperations::UpdateStates(row_state, aggr, state.addresses, payload, payload_idx, payload.size());
		}

		// Move to the next aggregate
		payload_idx += aggr.child_count;
		VectorOperations::AddInPlace(state.addresses, aggr.payload_size, payload.size());
		filter_idx++;
	}

	Verify();
	return new_group_count;
}

void GroupedAggregateHashTable::FetchAggregates(DataChunk &groups, DataChunk &result) {
#ifdef DEBUG
	groups.Verify();
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	for (idx_t i = 0; i < result.ColumnCount(); i++) {
		D_ASSERT(result.data[i].GetType() == payload_types[i]);
	}
#endif

	result.SetCardinality(groups);
	if (groups.size() == 0) {
		return;
	}

	// find the groups associated with the addresses
	// FIXME: this should not use the FindOrCreateGroups, creating them is unnecessary
	Vector addresses(LogicalType::POINTER);
	FindOrCreateGroups(groups, addresses);
	// now fetch the aggregates
	RowOperationsState row_state(*aggregate_allocator);
	RowOperations::FinalizeStates(row_state, layout, addresses, result, 0);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes_v,
                                                            Vector &addresses_v, SelectionVector &new_groups_out) {
	D_ASSERT(groups.ColumnCount() + 1 == layout.ColumnCount());
	D_ASSERT(group_hashes_v.GetType() == LogicalType::HASH);
	D_ASSERT(state.ht_offsets.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(state.ht_offsets.GetType() == LogicalType::UBIGINT);
	D_ASSERT(addresses_v.GetType() == LogicalType::POINTER);
	D_ASSERT(state.hash_salts.GetType() == LogicalType::HASH);

	// Need to fit the entire vector, and resize at threshold
	if (Count() + groups.size() > capacity || Count() + groups.size() > ResizeThreshold()) {
		Verify();
		Resize(capacity * 2);
	}
	D_ASSERT(capacity - Count() >= groups.size()); // we need to be able to fit at least one vector of data

	group_hashes_v.Flatten(groups.size());
	auto hashes = FlatVector::GetData<hash_t>(group_hashes_v);

	addresses_v.Flatten(groups.size());
	auto addresses = FlatVector::GetData<data_ptr_t>(addresses_v);

	// Compute the entry in the table based on the hash using a modulo,
	// and precompute the hash salts for faster comparison below
	auto ht_offsets = FlatVector::GetData<uint64_t>(state.ht_offsets);
	auto hash_salts = FlatVector::GetData<hash_t>(state.hash_salts);
	for (idx_t r = 0; r < groups.size(); r++) {
		const auto &hash = hashes[r];
		ht_offsets[r] = ApplyBitMask(hash);
		D_ASSERT(ht_offsets[r] == hash % capacity);
		hash_salts[r] = aggr_ht_entry_t::ExtractSalt(hash);
	}

	// we start out with all entries [0, 1, 2, ..., groups.size()]
	const SelectionVector *sel_vector = FlatVector::IncrementalSelectionVector();

	// Make a chunk that references the groups and the hashes and convert to unified format
	if (state.group_chunk.ColumnCount() == 0) {
		state.group_chunk.InitializeEmpty(layout.GetTypes());
	}
	D_ASSERT(state.group_chunk.ColumnCount() == layout.GetTypes().size());
	for (idx_t grp_idx = 0; grp_idx < groups.ColumnCount(); grp_idx++) {
		state.group_chunk.data[grp_idx].Reference(groups.data[grp_idx]);
	}
	state.group_chunk.data[groups.ColumnCount()].Reference(group_hashes_v);
	state.group_chunk.SetCardinality(groups);

	// convert all vectors to unified format
	auto &chunk_state = state.append_state.chunk_state;
	TupleDataCollection::ToUnifiedFormat(chunk_state, state.group_chunk);
	if (!state.group_data) {
		state.group_data = make_unsafe_uniq_array<UnifiedVectorFormat>(state.group_chunk.ColumnCount());
	}
	TupleDataCollection::GetVectorData(chunk_state, state.group_data.get());

	idx_t new_group_count = 0;
	idx_t remaining_entries = groups.size();
	while (remaining_entries > 0) {
		idx_t new_entry_count = 0;
		idx_t need_compare_count = 0;
		idx_t no_match_count = 0;

		// For each remaining entry, figure out whether or not it belongs to a full or empty group
		for (idx_t i = 0; i < remaining_entries; i++) {
			const auto index = sel_vector->get_index(i);
			const auto &salt = hash_salts[index];
			auto &entry = entries[ht_offsets[index]];
			if (entry.IsOccupied()) { // Cell is occupied: Compare salts
				if (entry.GetSalt() == salt) {
					state.group_compare_vector.set_index(need_compare_count++, index);
				} else {
					state.no_match_vector.set_index(no_match_count++, index);
				}
			} else { // Cell is unoccupied
				// Set salt (also marks as occupied)
				entry.SetSalt(salt);

				// Update selection lists for outer loops
				state.empty_vector.set_index(new_entry_count++, index);
				new_groups_out.set_index(new_group_count++, index);
			}
		}

		if (new_entry_count != 0) {
			// Append everything that belongs to an empty group
			partitioned_data->AppendUnified(state.append_state, state.group_chunk, state.empty_vector, new_entry_count);
			RowOperations::InitializeStates(layout, chunk_state.row_locations,
			                                *FlatVector::IncrementalSelectionVector(), new_entry_count);

			// Set the entry pointers in the 1st part of the HT now that the data has been appended
			const auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);
			const auto &row_sel = state.append_state.reverse_partition_sel;
			for (idx_t new_entry_idx = 0; new_entry_idx < new_entry_count; new_entry_idx++) {
				const auto index = state.empty_vector.get_index(new_entry_idx);
				const auto row_idx = row_sel.get_index(index);
				const auto &row_location = row_locations[row_idx];

				auto &entry = entries[ht_offsets[index]];

				entry.SetPointer(row_location);
				addresses[index] = row_location;
			}
		}

		if (need_compare_count != 0) {
			// Get the pointers to the rows that need to be compared
			for (idx_t need_compare_idx = 0; need_compare_idx < need_compare_count; need_compare_idx++) {
				const auto index = state.group_compare_vector.get_index(need_compare_idx);
				const auto &entry = entries[ht_offsets[index]];
				addresses[index] = entry.GetPointer();
			}

			// Perform group comparisons
			row_matcher.Match(state.group_chunk, chunk_state.vector_data, state.group_compare_vector,
			                  need_compare_count, layout, addresses_v, &state.no_match_vector, no_match_count);
		}

		// Linear probing: each of the entries that do not match move to the next entry in the HT
		for (idx_t i = 0; i < no_match_count; i++) {
			idx_t index = state.no_match_vector.get_index(i);
			ht_offsets[index]++;
			if (ht_offsets[index] >= capacity) {
				ht_offsets[index] = 0;
			}
		}
		sel_vector = &state.no_match_vector;
		remaining_entries = no_match_count;
	}

	count += new_group_count;
	return new_group_count;
}

// this is to support distinct aggregations where we need to record whether we
// have already seen a value for a group
idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	return FindOrCreateGroupsInternal(groups, group_hashes, addresses_out, new_groups_out);
}

void GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses) {
	// create a dummy new_groups sel vector
	FindOrCreateGroups(groups, addresses, state.new_groups);
}

idx_t GroupedAggregateHashTable::FindOrCreateGroups(DataChunk &groups, Vector &addresses_out,
                                                    SelectionVector &new_groups_out) {
	Vector hashes(LogicalType::HASH);
	groups.Hash(hashes);
	return FindOrCreateGroups(groups, hashes, addresses_out, new_groups_out);
}

struct FlushMoveState {
	explicit FlushMoveState(TupleDataCollection &collection_p)
	    : collection(collection_p), hashes(LogicalType::HASH), group_addresses(LogicalType::POINTER),
	      new_groups_sel(STANDARD_VECTOR_SIZE) {
		const auto &layout = collection.GetLayout();
		vector<column_t> column_ids;
		column_ids.reserve(layout.ColumnCount() - 1);
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount() - 1; col_idx++) {
			column_ids.emplace_back(col_idx);
		}
		collection.InitializeScan(scan_state, column_ids, TupleDataPinProperties::DESTROY_AFTER_DONE);
		collection.InitializeScanChunk(scan_state, groups);
		hash_col_idx = layout.ColumnCount() - 1;
	}

	bool Scan() {
		if (collection.Scan(scan_state, groups)) {
			collection.Gather(scan_state.chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(),
			                  groups.size(), hash_col_idx, hashes, *FlatVector::IncrementalSelectionVector());
			return true;
		}

		collection.FinalizePinState(scan_state.pin_state);
		return false;
	}

	TupleDataCollection &collection;
	TupleDataScanState scan_state;
	DataChunk groups;

	idx_t hash_col_idx;
	Vector hashes;

	Vector group_addresses;
	SelectionVector new_groups_sel;
};

void GroupedAggregateHashTable::Combine(GroupedAggregateHashTable &other) {
	auto other_data = other.partitioned_data->GetUnpartitioned();
	Combine(*other_data);

	// Inherit ownership to all stored aggregate allocators
	stored_allocators.emplace_back(other.aggregate_allocator);
	for (const auto &stored_allocator : other.stored_allocators) {
		stored_allocators.emplace_back(stored_allocator);
	}
}

void GroupedAggregateHashTable::Combine(TupleDataCollection &other_data) {
	D_ASSERT(other_data.GetLayout().GetAggrWidth() == layout.GetAggrWidth());
	D_ASSERT(other_data.GetLayout().GetDataWidth() == layout.GetDataWidth());
	D_ASSERT(other_data.GetLayout().GetRowWidth() == layout.GetRowWidth());

	if (other_data.Count() == 0) {
		return;
	}

	FlushMoveState fm_state(other_data);
	RowOperationsState row_state(*aggregate_allocator);
	while (fm_state.Scan()) {
		FindOrCreateGroups(fm_state.groups, fm_state.hashes, fm_state.group_addresses, fm_state.new_groups_sel);
		RowOperations::CombineStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
		                             fm_state.group_addresses, fm_state.groups.size());
		if (layout.HasDestructor()) {
			RowOperations::DestroyStates(row_state, layout, fm_state.scan_state.chunk_state.row_locations,
			                             fm_state.groups.size());
		}
	}

	Verify();
}

void GroupedAggregateHashTable::UnpinData() {
	partitioned_data->FlushAppendState(state.append_state);
	partitioned_data->Unpin();
}

} // namespace duckdb




namespace duckdb {

BaseAggregateHashTable::BaseAggregateHashTable(ClientContext &context, Allocator &allocator,
                                               const vector<AggregateObject> &aggregates,
                                               vector<LogicalType> payload_types_p)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)),
      payload_types(std::move(payload_types_p)) {
	filter_set.Initialize(context, aggregates, payload_types);
}

} // namespace duckdb














namespace duckdb {

ColumnBindingResolver::ColumnBindingResolver() {
}

void ColumnBindingResolver::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		// special case: comparison join
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		// first get the bindings of the LHS and resolve the LHS expressions
		VisitOperator(*comp_join.children[0]);
		for (auto &cond : comp_join.conditions) {
			VisitExpression(&cond.left);
		}
		// visit the duplicate eliminated columns on the LHS, if any
		for (auto &expr : comp_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}
		// then get the bindings of the RHS and resolve the RHS expressions
		VisitOperator(*comp_join.children[1]);
		for (auto &cond : comp_join.conditions) {
			VisitExpression(&cond.right);
		}
		// finally update the bindings with the result bindings of the join
		bindings = op.GetColumnBindings();
		return;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		// ANY join, this join is different because we evaluate the expression on the bindings of BOTH join sides at
		// once i.e. we set the bindings first to the bindings of the entire join, and then resolve the expressions of
		// this operator
		VisitOperatorChildren(op);
		bindings = op.GetColumnBindings();
		auto &any_join = op.Cast<LogicalAnyJoin>();
		if (any_join.join_type == JoinType::SEMI || any_join.join_type == JoinType::ANTI) {
			auto right_bindings = op.children[1]->GetColumnBindings();
			bindings.insert(bindings.end(), right_bindings.begin(), right_bindings.end());
		}
		VisitOperatorExpressions(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_CREATE_INDEX: {
		// CREATE INDEX statement, add the columns of the table with table index 0 to the binding set
		// afterwards bind the expressions of the CREATE INDEX statement
		auto &create_index = op.Cast<LogicalCreateIndex>();
		bindings = LogicalOperator::GenerateColumnBindings(0, create_index.table.GetColumns().LogicalColumnCount());
		VisitOperatorExpressions(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		//! We first need to update the current set of bindings and then visit operator expressions
		bindings = op.GetColumnBindings();
		VisitOperatorExpressions(op);
		return;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		//! We want to execute the normal path, but also add a dummy 'excluded' binding if there is a
		// ON CONFLICT DO UPDATE clause
		auto &insert_op = op.Cast<LogicalInsert>();
		if (insert_op.action_type != OnConflictAction::THROW) {
			// Get the bindings from the children
			VisitOperatorChildren(op);
			auto column_count = insert_op.table.GetColumns().PhysicalColumnCount();
			auto dummy_bindings = LogicalOperator::GenerateColumnBindings(insert_op.excluded_table_index, column_count);
			// Now insert our dummy bindings at the start of the bindings,
			// so the first 'column_count' indices of the chunk are reserved for our 'excluded' columns
			bindings.insert(bindings.begin(), dummy_bindings.begin(), dummy_bindings.end());
			if (insert_op.on_conflict_condition) {
				VisitExpression(&insert_op.on_conflict_condition);
			}
			if (insert_op.do_update_condition) {
				VisitExpression(&insert_op.do_update_condition);
			}
			VisitOperatorExpressions(op);
			bindings = op.GetColumnBindings();
			return;
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR: {
		auto &ext_op = op.Cast<LogicalExtensionOperator>();
		ext_op.ResolveColumnBindings(*this, bindings);
		return;
	}
	default:
		break;
	}

	// general case
	// first visit the children of this operator
	VisitOperatorChildren(op);
	// now visit the expressions of this operator to resolve any bound column references
	VisitOperatorExpressions(op);
	// finally update the current set of bindings to the current set of column bindings
	bindings = op.GetColumnBindings();
}

unique_ptr<Expression> ColumnBindingResolver::VisitReplace(BoundColumnRefExpression &expr,
                                                           unique_ptr<Expression> *expr_ptr) {
	D_ASSERT(expr.depth == 0);
	// check the current set of column bindings to see which index corresponds to the column reference
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (expr.binding == bindings[i]) {
			return make_uniq<BoundReferenceExpression>(expr.alias, expr.return_type, i);
		}
	}
	// LCOV_EXCL_START
	// could not bind the column reference, this should never happen and indicates a bug in the code
	// generate an error message
	string bound_columns = "[";
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (i != 0) {
			bound_columns += " ";
		}
		bound_columns += to_string(bindings[i].table_index) + "." + to_string(bindings[i].column_index);
	}
	bound_columns += "]";

	throw InternalException("Failed to bind column reference \"%s\" [%d.%d] (bindings: %s)", expr.alias,
	                        expr.binding.table_index, expr.binding.column_index, bound_columns);
	// LCOV_EXCL_STOP
}

unordered_set<idx_t> ColumnBindingResolver::VerifyInternal(LogicalOperator &op) {
	unordered_set<idx_t> result;
	for (auto &child : op.children) {
		auto child_indexes = VerifyInternal(*child);
		for (auto index : child_indexes) {
			D_ASSERT(index != DConstants::INVALID_INDEX);
			if (result.find(index) != result.end()) {
				throw InternalException("Duplicate table index \"%lld\" found", index);
			}
			result.insert(index);
		}
	}
	auto indexes = op.GetTableIndex();
	for (auto index : indexes) {
		D_ASSERT(index != DConstants::INVALID_INDEX);
		if (result.find(index) != result.end()) {
			throw InternalException("Duplicate table index \"%lld\" found", index);
		}
		result.insert(index);
	}
	return result;
}

void ColumnBindingResolver::Verify(LogicalOperator &op) {
#ifdef DEBUG
	VerifyInternal(op);
#endif
}

} // namespace duckdb






namespace duckdb {

struct BothInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct LowerInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThanEquals::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

struct UpperInclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThanEquals::Operation<T>(input, upper);
	}
};

struct ExclusiveBetweenOperator {
	template <class T>
	static inline bool Operation(T input, T lower, T upper) {
		return GreaterThan::Operation<T>(input, lower) && LessThan::Operation<T>(input, upper);
	}
};

template <class OP>
static idx_t BetweenLoopTypeSwitch(Vector &input, Vector &lower, Vector &upper, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TernaryExecutor::Select<int8_t, int8_t, int8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::INT16:
		return TernaryExecutor::Select<int16_t, int16_t, int16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT32:
		return TernaryExecutor::Select<int32_t, int32_t, int32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT64:
		return TernaryExecutor::Select<int64_t, int64_t, int64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::INT128:
		return TernaryExecutor::Select<hugeint_t, hugeint_t, hugeint_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                    false_sel);
	case PhysicalType::UINT8:
		return TernaryExecutor::Select<uint8_t, uint8_t, uint8_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                              false_sel);
	case PhysicalType::UINT16:
		return TernaryExecutor::Select<uint16_t, uint16_t, uint16_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT32:
		return TernaryExecutor::Select<uint32_t, uint32_t, uint32_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::UINT64:
		return TernaryExecutor::Select<uint64_t, uint64_t, uint64_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::FLOAT:
		return TernaryExecutor::Select<float, float, float, OP>(input, lower, upper, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return TernaryExecutor::Select<double, double, double, OP>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	case PhysicalType::VARCHAR:
		return TernaryExecutor::Select<string_t, string_t, string_t, OP>(input, lower, upper, sel, count, true_sel,
		                                                                 false_sel);
	case PhysicalType::INTERVAL:
		return TernaryExecutor::Select<interval_t, interval_t, interval_t, OP>(input, lower, upper, sel, count,
		                                                                       true_sel, false_sel);
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for BETWEEN");
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundBetweenExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->AddChild(expr.input.get());
	result->AddChild(expr.lower.get());
	result->AddChild(expr.upper.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	// resolve the children
	state->intermediate_chunk.Reset();

	auto &input = state->intermediate_chunk.data[0];
	auto &lower = state->intermediate_chunk.data[1];
	auto &upper = state->intermediate_chunk.data[2];

	Execute(*expr.input, state->child_states[0].get(), sel, count, input);
	Execute(*expr.lower, state->child_states[1].get(), sel, count, lower);
	Execute(*expr.upper, state->child_states[2].get(), sel, count, upper);

	Vector intermediate1(LogicalType::BOOLEAN);
	Vector intermediate2(LogicalType::BOOLEAN);

	if (expr.upper_inclusive && expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else if (expr.lower_inclusive) {
		VectorOperations::GreaterThanEquals(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	} else if (expr.upper_inclusive) {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThanEquals(input, upper, intermediate2, count);
	} else {
		VectorOperations::GreaterThan(input, lower, intermediate1, count);
		VectorOperations::LessThan(input, upper, intermediate2, count);
	}
	VectorOperations::And(intermediate1, intermediate2, result, count);
}

idx_t ExpressionExecutor::Select(const BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	// resolve the children
	Vector input(state->intermediate_chunk.data[0]);
	Vector lower(state->intermediate_chunk.data[1]);
	Vector upper(state->intermediate_chunk.data[2]);

	Execute(*expr.input, state->child_states[0].get(), sel, count, input);
	Execute(*expr.lower, state->child_states[1].get(), sel, count, lower);
	Execute(*expr.upper, state->child_states[2].get(), sel, count, upper);

	if (expr.upper_inclusive && expr.lower_inclusive) {
		return BetweenLoopTypeSwitch<BothInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                           false_sel);
	} else if (expr.lower_inclusive) {
		return BetweenLoopTypeSwitch<LowerInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                            false_sel);
	} else if (expr.upper_inclusive) {
		return BetweenLoopTypeSwitch<UpperInclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel,
		                                                            false_sel);
	} else {
		return BetweenLoopTypeSwitch<ExclusiveBetweenOperator>(input, lower, upper, sel, count, true_sel, false_sel);
	}
}

} // namespace duckdb




namespace duckdb {

struct CaseExpressionState : public ExpressionState {
	CaseExpressionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE) {
	}

	SelectionVector true_sel;
	SelectionVector false_sel;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<CaseExpressionState>(expr, root);
	for (auto &case_check : expr.case_checks) {
		result->AddChild(case_check.when_expr.get());
		result->AddChild(case_check.then_expr.get());
	}
	result->AddChild(expr.else_expr.get());
	result->Finalize();
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundCaseExpression &expr, ExpressionState *state_p, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto &state = state_p->Cast<CaseExpressionState>();

	state.intermediate_chunk.Reset();

	// first execute the check expression
	auto current_true_sel = &state.true_sel;
	auto current_false_sel = &state.false_sel;
	auto current_sel = sel;
	idx_t current_count = count;
	for (idx_t i = 0; i < expr.case_checks.size(); i++) {
		auto &case_check = expr.case_checks[i];
		auto &intermediate_result = state.intermediate_chunk.data[i * 2 + 1];
		auto check_state = state.child_states[i * 2].get();
		auto then_state = state.child_states[i * 2 + 1].get();

		idx_t tcount =
		    Select(*case_check.when_expr, check_state, current_sel, current_count, current_true_sel, current_false_sel);
		if (tcount == 0) {
			// everything is false: do nothing
			continue;
		}
		idx_t fcount = current_count - tcount;
		if (fcount == 0 && current_count == count) {
			// everything is true in the first CHECK statement
			// we can skip the entire case and only execute the TRUE side
			Execute(*case_check.then_expr, then_state, sel, count, result);
			return;
		} else {
			// we need to execute and then fill in the desired tuples in the result
			Execute(*case_check.then_expr, then_state, current_true_sel, tcount, intermediate_result);
			FillSwitch(intermediate_result, result, *current_true_sel, tcount);
		}
		// continue with the false tuples
		current_sel = current_false_sel;
		current_count = fcount;
		if (fcount == 0) {
			// everything is true: we are done
			break;
		}
	}
	if (current_count > 0) {
		auto else_state = state.child_states.back().get();
		if (current_count == count) {
			// everything was false, we can just evaluate the else expression directly
			Execute(*expr.else_expr, else_state, sel, count, result);
			return;
		} else {
			auto &intermediate_result = state.intermediate_chunk.data[expr.case_checks.size() * 2];

			D_ASSERT(current_sel);
			Execute(*expr.else_expr, else_state, current_sel, current_count, intermediate_result);
			FillSwitch(intermediate_result, result, *current_sel, current_count);
		}
	}
	if (sel) {
		result.Slice(*sel, count);
	}
}

template <class T>
void TemplatedFillLoop(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto res = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto data = ConstantVector::GetData<T>(vector);
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				res[sel.get_index(i)] = *data;
			}
		}
	} else {
		UnifiedVectorFormat vdata;
		vector.ToUnifiedFormat(count, vdata);
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			auto res_idx = sel.get_index(i);

			res[res_idx] = data[source_idx];
			result_mask.Set(res_idx, vdata.validity.RowIsValid(source_idx));
		}
	}
}

void ValidityFillLoop(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		}
	} else {
		UnifiedVectorFormat vdata;
		vector.ToUnifiedFormat(count, vdata);
		if (vdata.validity.AllValid()) {
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(source_idx)) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		}
	}
}

void ExpressionExecutor::FillSwitch(Vector &vector, Vector &result, const SelectionVector &sel, sel_t count) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedFillLoop<int8_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedFillLoop<int16_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedFillLoop<int32_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedFillLoop<int64_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedFillLoop<uint8_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedFillLoop<uint16_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedFillLoop<uint32_t>(vector, result, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedFillLoop<uint64_t>(vector, result, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedFillLoop<hugeint_t>(vector, result, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedFillLoop<float>(vector, result, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedFillLoop<double>(vector, result, sel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedFillLoop<interval_t>(vector, result, sel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedFillLoop<string_t>(vector, result, sel, count);
		StringVector::AddHeapReference(result, vector);
		break;
	case PhysicalType::STRUCT: {
		auto &vector_entries = StructVector::GetEntries(vector);
		auto &result_entries = StructVector::GetEntries(result);
		ValidityFillLoop(vector, result, sel, count);
		D_ASSERT(vector_entries.size() == result_entries.size());
		for (idx_t i = 0; i < vector_entries.size(); i++) {
			FillSwitch(*vector_entries[i], *result_entries[i], sel, count);
		}
		break;
	}
	case PhysicalType::LIST: {
		idx_t offset = ListVector::GetListSize(result);
		auto &list_child = ListVector::GetEntry(vector);
		ListVector::Append(result, list_child, ListVector::GetListSize(vector));

		// all the false offsets need to be incremented by true_child.count
		TemplatedFillLoop<list_entry_t>(vector, result, sel, count);
		if (offset == 0) {
			break;
		}

		auto result_data = FlatVector::GetData<list_entry_t>(result);
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = sel.get_index(i);
			result_data[result_idx].offset += offset;
		}

		Vector::Verify(result, sel, count);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for case expression: %s", result.GetType().ToString());
	}
}

} // namespace duckdb





namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCastExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	result->AddChild(expr.child.get());
	result->Finalize();
	if (expr.bound_cast.init_local_state) {
		CastLocalStateParameters parameters(root.executor->GetContext(), expr.bound_cast.cast_data);
		result->local_state = expr.bound_cast.init_local_state(parameters);
	}
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto lstate = ExecuteFunctionState::GetFunctionState(*state);

	// resolve the child
	state->intermediate_chunk.Reset();

	auto &child = state->intermediate_chunk.data[0];
	auto child_state = state->child_states[0].get();

	Execute(*expr.child, child_state, sel, count, child);
	if (expr.try_cast) {
		string error_message;
		CastParameters parameters(expr.bound_cast.cast_data.get(), false, &error_message, lstate);
		expr.bound_cast.function(child, result, count, parameters);
	} else {
		// cast it to the type specified by the cast expression
		D_ASSERT(result.GetType() == expr.return_type);
		CastParameters parameters(expr.bound_cast.cast_data.get(), false, nullptr, lstate);
		expr.bound_cast.function(child, result, count, parameters);
	}
}

} // namespace duckdb






#include <algorithm>

namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundComparisonExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->AddChild(expr.left.get());
	result->AddChild(expr.right.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(left, right, result, count);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(left, right, result, count);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		VectorOperations::DistinctFrom(left, right, result, count);
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		VectorOperations::NotDistinctFrom(left, right, result, count);
		break;
	default:
		throw InternalException("Unknown comparison type!");
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel);

template <class OP>
static idx_t TemplatedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                      SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return BinaryExecutor::Select<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return BinaryExecutor::Select<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return BinaryExecutor::Select<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return BinaryExecutor::Select<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return BinaryExecutor::Select<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return BinaryExecutor::Select<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return BinaryExecutor::Select<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return BinaryExecutor::Select<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return BinaryExecutor::Select<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return BinaryExecutor::Select<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return BinaryExecutor::Select<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return BinaryExecutor::Select<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
		return NestedSelectOperation<OP>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Invalid type for comparison");
	}
}

struct NestedSelector {
	// Select the matching rows for the values of a nested type that are not both NULL.
	// Those semantics are the same as the corresponding non-distinct comparator
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		throw InvalidTypeException(left.GetType(), "Invalid operation for nested SELECT");
	}
};

template <>
idx_t NestedSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                             SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                     idx_t count, SelectionVector *true_sel,
                                                     SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t NestedSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

static inline idx_t SelectNotNull(Vector &left, Vector &right, const idx_t count, const SelectionVector &sel,
                                  SelectionVector &maybe_vec, OptionalSelection &false_opt) {

	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(count, lvdata);
	right.ToUnifiedFormat(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

	// For top-level comparisons, NULL semantics are in effect,
	// so filter out any NULLs
	idx_t remaining = 0;
	if (lmask.AllValid() && rmask.AllValid()) {
		//	None are NULL, distinguish values.
		for (idx_t i = 0; i < count; ++i) {
			const auto idx = sel.get_index(i);
			maybe_vec.set_index(remaining++, idx);
		}
		return remaining;
	}

	// Slice the Vectors down to the rows that are not determined (i.e., neither is NULL)
	SelectionVector slicer(count);
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		if (!lmask.RowIsValid(lidx) || !rmask.RowIsValid(ridx)) {
			false_opt.Append(false_count, result_idx);
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

static void ScatterSelection(SelectionVector *target, const idx_t count, const SelectionVector &dense_vec) {
	if (target) {
		for (idx_t i = 0; i < count; ++i) {
			target->set_index(i, dense_vec.get_index(i));
		}
	}
}

template <typename OP>
static idx_t NestedSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	// The Select operations all use a dense pair of input vectors to partition
	// a selection vector in a single pass. But to implement progressive comparisons,
	// we have to make multiple passes, so we need to keep track of the original input positions
	// and then scatter the output selections when we are done.
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}

	// Make buffered selections for progressive comparisons
	// TODO: Remove unnecessary allocations
	SelectionVector true_vec(count);
	OptionalSelection true_opt(&true_vec);

	SelectionVector false_vec(count);
	OptionalSelection false_opt(&false_vec);

	SelectionVector maybe_vec(count);

	// Handle NULL nested values
	Vector l_not_null(left);
	Vector r_not_null(right);

	auto match_count = SelectNotNull(l_not_null, r_not_null, count, *sel, maybe_vec, false_opt);
	auto no_match_count = count - match_count;
	count = match_count;

	//	Now that we have handled the NULLs, we can use the recursive nested comparator for the rest.
	match_count = NestedSelector::Select<OP>(l_not_null, r_not_null, maybe_vec, count, true_opt, false_opt);
	no_match_count += (count - match_count);

	// Copy the buffered selections to the output selections
	ScatterSelection(true_sel, match_count, true_vec);
	ScatterSelection(false_sel, no_match_count, false_vec);

	return match_count;
}

idx_t VectorOperations::Equals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::Equals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::NotEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::NotEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::GreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(left, right, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThan>(right, left, sel, count, true_sel, false_sel);
}

idx_t VectorOperations::LessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedSelectOperation<duckdb::GreaterThanEquals>(right, left, sel, count, true_sel, false_sel);
}

idx_t ExpressionExecutor::Select(const BoundComparisonExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	// resolve the children
	state->intermediate_chunk.Reset();
	auto &left = state->intermediate_chunk.data[0];
	auto &right = state->intermediate_chunk.data[1];

	Execute(*expr.left, state->child_states[0].get(), sel, count, left);
	Execute(*expr.right, state->child_states[1].get(), sel, count, right);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOTEQUAL:
		return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHAN:
		return VectorOperations::LessThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHAN:
		return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return VectorOperations::LessThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return VectorOperations::DistinctFrom(left, right, sel, count, true_sel, false_sel);
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return VectorOperations::NotDistinctFrom(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Unknown comparison type!");
	}
}

} // namespace duckdb






#include <random>

namespace duckdb {

struct ConjunctionState : public ExpressionState {
	ConjunctionState(const Expression &expr, ExpressionExecutorState &root) : ExpressionState(expr, root) {
		adaptive_filter = make_uniq<AdaptiveFilter>(expr);
	}
	unique_ptr<AdaptiveFilter> adaptive_filter;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundConjunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ConjunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return std::move(result);
}

void ExpressionExecutor::Execute(const BoundConjunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// execute the children
	state->intermediate_chunk.Reset();
	for (idx_t i = 0; i < expr.children.size(); i++) {
		auto &current_result = state->intermediate_chunk.data[i];
		Execute(*expr.children[i], state->child_states[i].get(), sel, count, current_result);
		if (i == 0) {
			// move the result
			result.Reference(current_result);
		} else {
			Vector intermediate(LogicalType::BOOLEAN);
			// AND/OR together
			switch (expr.type) {
			case ExpressionType::CONJUNCTION_AND:
				VectorOperations::And(current_result, result, intermediate, count);
				break;
			case ExpressionType::CONJUNCTION_OR:
				VectorOperations::Or(current_result, result, intermediate, count);
				break;
			default:
				throw InternalException("Unknown conjunction type!");
			}
			result.Reference(intermediate);
		}
	}
}

idx_t ExpressionExecutor::Select(const BoundConjunctionExpression &expr, ExpressionState *state_p,
                                 const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                 SelectionVector *false_sel) {
	auto &state = state_p->Cast<ConjunctionState>();

	if (expr.type == ExpressionType::CONJUNCTION_AND) {
		// get runtime statistics
		auto start_time = high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t false_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (false_sel) {
			temp_false = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!true_sel) {
			temp_true = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			true_sel = temp_true.get();
		}
		for (idx_t i = 0; i < expr.children.size(); i++) {
			idx_t tcount = Select(*expr.children[state.adaptive_filter->permutation[i]],
			                      state.child_states[state.adaptive_filter->permutation[i]].get(), current_sel,
			                      current_count, true_sel, temp_false.get());
			idx_t fcount = current_count - tcount;
			if (fcount > 0 && false_sel) {
				// move failing tuples into the false_sel
				// tuples passed, move them into the actual result vector
				for (idx_t i = 0; i < fcount; i++) {
					false_sel->set_index(false_count++, temp_false->get_index(i));
				}
			}
			current_count = tcount;
			if (current_count == 0) {
				break;
			}
			if (current_count < count) {
				// tuples were filtered out: move on to using the true_sel to only evaluate passing tuples in subsequent
				// iterations
				current_sel = true_sel;
			}
		}

		// adapt runtime statistics
		auto end_time = high_resolution_clock::now();
		state.adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
		return current_count;
	} else {
		// get runtime statistics
		auto start_time = high_resolution_clock::now();

		const SelectionVector *current_sel = sel;
		idx_t current_count = count;
		idx_t result_count = 0;

		unique_ptr<SelectionVector> temp_true, temp_false;
		if (true_sel) {
			temp_true = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
		}
		if (!false_sel) {
			temp_false = make_uniq<SelectionVector>(STANDARD_VECTOR_SIZE);
			false_sel = temp_false.get();
		}
		for (idx_t i = 0; i < expr.children.size(); i++) {
			idx_t tcount = Select(*expr.children[state.adaptive_filter->permutation[i]],
			                      state.child_states[state.adaptive_filter->permutation[i]].get(), current_sel,
			                      current_count, temp_true.get(), false_sel);
			if (tcount > 0) {
				if (true_sel) {
					// tuples passed, move them into the actual result vector
					for (idx_t i = 0; i < tcount; i++) {
						true_sel->set_index(result_count++, temp_true->get_index(i));
					}
				}
				// now move on to check only the non-passing tuples
				current_count -= tcount;
				current_sel = false_sel;
			}
		}

		// adapt runtime statistics
		auto end_time = high_resolution_clock::now();
		state.adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
		return result_count;
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundConstantExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundConstantExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.value.type() == expr.return_type);
	result.Reference(expr.value);
}

} // namespace duckdb



namespace duckdb {

ExecuteFunctionState::ExecuteFunctionState(const Expression &expr, ExpressionExecutorState &root)
    : ExpressionState(expr, root) {
}

ExecuteFunctionState::~ExecuteFunctionState() {
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundFunctionExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExecuteFunctionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	if (expr.function.init_local_state) {
		result->local_state = expr.function.init_local_state(*result, expr, expr.bind_info.get());
	}
	return std::move(result);
}

static void VerifyNullHandling(const BoundFunctionExpression &expr, DataChunk &args, Vector &result) {
#ifdef DEBUG
	if (args.data.empty() || expr.function.null_handling != FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return;
	}

	// Combine all the argument validity masks into a flat validity mask
	idx_t count = args.size();
	ValidityMask combined_mask(count);
	for (auto &arg : args.data) {
		UnifiedVectorFormat arg_data;
		arg.ToUnifiedFormat(count, arg_data);

		for (idx_t i = 0; i < count; i++) {
			auto idx = arg_data.sel->get_index(i);
			if (!arg_data.validity.RowIsValid(idx)) {
				combined_mask.SetInvalid(i);
			}
		}
	}

	// Default is that if any of the arguments are NULL, the result is also NULL
	UnifiedVectorFormat result_data;
	result.ToUnifiedFormat(count, result_data);
	for (idx_t i = 0; i < count; i++) {
		if (!combined_mask.RowIsValid(i)) {
			auto idx = result_data.sel->get_index(i);
			D_ASSERT(!result_data.validity.RowIsValid(idx));
		}
	}
#endif
}

void ExpressionExecutor::Execute(const BoundFunctionExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	state->intermediate_chunk.Reset();
	auto &arguments = state->intermediate_chunk;
	if (!state->types.empty()) {
		for (idx_t i = 0; i < expr.children.size(); i++) {
			D_ASSERT(state->types[i] == expr.children[i]->return_type);
			Execute(*expr.children[i], state->child_states[i].get(), sel, count, arguments.data[i]);
#ifdef DEBUG
			if (expr.children[i]->return_type.id() == LogicalTypeId::VARCHAR) {
				arguments.data[i].UTFVerify(count);
			}
#endif
		}
		arguments.Verify();
	}
	arguments.SetCardinality(count);

	state->profiler.BeginSample();
	D_ASSERT(expr.function.function);
	expr.function.function(arguments, *state, result);
	state->profiler.EndSample(count);

	VerifyNullHandling(expr, arguments, result);
	D_ASSERT(result.GetType() == expr.return_type);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundOperatorExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	for (auto &child : expr.children) {
		result->AddChild(child.get());
	}
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundOperatorExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		if (expr.children.size() < 2) {
			throw InvalidInputException("IN needs at least two children");
		}

		Vector left(expr.children[0]->return_type);
		// eval left side
		Execute(*expr.children[0], state->child_states[0].get(), sel, count, left);

		// init result to false
		Vector intermediate(LogicalType::BOOLEAN);
		Value false_val = Value::BOOLEAN(false);
		intermediate.Reference(false_val);

		// in rhs is a list of constants
		// for every child, OR the result of the comparision with the left
		// to get the overall result.
		for (idx_t child = 1; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Vector comp_res(LogicalType::BOOLEAN);

			Execute(*expr.children[child], state->child_states[child].get(), sel, count, vector_to_check);
			VectorOperations::Equals(left, vector_to_check, comp_res, count);

			if (child == 1) {
				// first child: move to result
				intermediate.Reference(comp_res);
			} else {
				// otherwise OR together
				Vector new_result(LogicalType::BOOLEAN, true, false);
				VectorOperations::Or(intermediate, comp_res, new_result, count);
				intermediate.Reference(new_result);
			}
		}
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// NOT IN: invert result
			VectorOperations::Not(intermediate, result, count);
		} else {
			// directly use the result
			result.Reference(intermediate);
		}
	} else if (expr.type == ExpressionType::OPERATOR_COALESCE) {
		SelectionVector sel_a(count);
		SelectionVector sel_b(count);
		SelectionVector slice_sel(count);
		SelectionVector result_sel(count);
		SelectionVector *next_sel = &sel_a;
		const SelectionVector *current_sel = sel;
		idx_t remaining_count = count;
		idx_t next_count;
		for (idx_t child = 0; child < expr.children.size(); child++) {
			Vector vector_to_check(expr.children[child]->return_type);
			Execute(*expr.children[child], state->child_states[child].get(), current_sel, remaining_count,
			        vector_to_check);

			UnifiedVectorFormat vdata;
			vector_to_check.ToUnifiedFormat(remaining_count, vdata);

			idx_t result_count = 0;
			next_count = 0;
			for (idx_t i = 0; i < remaining_count; i++) {
				auto base_idx = current_sel ? current_sel->get_index(i) : i;
				auto idx = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(idx)) {
					slice_sel.set_index(result_count, i);
					result_sel.set_index(result_count++, base_idx);
				} else {
					next_sel->set_index(next_count++, base_idx);
				}
			}
			if (result_count > 0) {
				vector_to_check.Slice(slice_sel, result_count);
				FillSwitch(vector_to_check, result, result_sel, result_count);
			}
			current_sel = next_sel;
			next_sel = next_sel == &sel_a ? &sel_b : &sel_a;
			remaining_count = next_count;
			if (next_count == 0) {
				break;
			}
		}
		if (remaining_count > 0) {
			for (idx_t i = 0; i < remaining_count; i++) {
				FlatVector::SetNull(result, current_sel->get_index(i), true);
			}
		}
		if (sel) {
			result.Slice(*sel, count);
		} else if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	} else if (expr.children.size() == 1) {
		state->intermediate_chunk.Reset();
		auto &child = state->intermediate_chunk.data[0];

		Execute(*expr.children[0], state->child_states[0].get(), sel, count, child);
		switch (expr.type) {
		case ExpressionType::OPERATOR_NOT: {
			VectorOperations::Not(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			VectorOperations::IsNull(child, result, count);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			VectorOperations::IsNotNull(child, result, count);
			break;
		}
		default:
			throw NotImplementedException("Unsupported operator type with 1 child!");
		}
	} else {
		throw NotImplementedException("operator");
	}
}

} // namespace duckdb




namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundParameterExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundParameterExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.parameter_data);
	D_ASSERT(expr.parameter_data->return_type == expr.return_type);
	D_ASSERT(expr.parameter_data->GetValue().type() == expr.return_type);
	result.Reference(expr.parameter_data->GetValue());
}

} // namespace duckdb



namespace duckdb {

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundReferenceExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_uniq<ExpressionState>(expr, root);
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundReferenceExpression &expr, ExpressionState *state,
                                 const SelectionVector *sel, idx_t count, Vector &result) {
	D_ASSERT(expr.index != DConstants::INVALID_INDEX);
	D_ASSERT(expr.index < chunk->ColumnCount());

	if (sel) {
		result.Slice(chunk->data[expr.index], *sel, count);
	} else {
		result.Reference(chunk->data[expr.index]);
	}
}

} // namespace duckdb







namespace duckdb {

ExpressionExecutor::ExpressionExecutor(ClientContext &context) : context(&context) {
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression *expression)
    : ExpressionExecutor(context) {
	D_ASSERT(expression);
	AddExpression(*expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const Expression &expression)
    : ExpressionExecutor(context) {
	AddExpression(expression);
}

ExpressionExecutor::ExpressionExecutor(ClientContext &context, const vector<unique_ptr<Expression>> &exprs)
    : ExpressionExecutor(context) {
	D_ASSERT(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

ExpressionExecutor::ExpressionExecutor(const vector<unique_ptr<Expression>> &exprs) : context(nullptr) {
	D_ASSERT(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

ExpressionExecutor::ExpressionExecutor() : context(nullptr) {
}

bool ExpressionExecutor::HasContext() {
	return context;
}

ClientContext &ExpressionExecutor::GetContext() {
	if (!context) {
		throw InternalException("Calling ExpressionExecutor::GetContext on an expression executor without a context");
	}
	return *context;
}

Allocator &ExpressionExecutor::GetAllocator() {
	return context ? Allocator::Get(*context) : Allocator::DefaultAllocator();
}

void ExpressionExecutor::AddExpression(const Expression &expr) {
	expressions.push_back(&expr);
	auto state = make_uniq<ExpressionExecutorState>();
	Initialize(expr, *state);
	state->Verify();
	states.push_back(std::move(state));
}

void ExpressionExecutor::Initialize(const Expression &expression, ExpressionExecutorState &state) {
	state.executor = this;
	state.root_state = InitializeState(expression, state);
}

void ExpressionExecutor::Execute(DataChunk *input, DataChunk &result) {
	SetChunk(input);
	D_ASSERT(expressions.size() == result.ColumnCount());
	D_ASSERT(!expressions.empty());

	for (idx_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(i, result.data[i]);
	}
	result.SetCardinality(input ? input->size() : 1);
	result.Verify();
}

void ExpressionExecutor::ExecuteExpression(DataChunk &input, Vector &result) {
	SetChunk(&input);
	ExecuteExpression(result);
}

idx_t ExpressionExecutor::SelectExpression(DataChunk &input, SelectionVector &sel) {
	D_ASSERT(expressions.size() == 1);
	SetChunk(&input);
	states[0]->profiler.BeginSample();
	idx_t selected_tuples = Select(*expressions[0], states[0]->root_state.get(), nullptr, input.size(), &sel, nullptr);
	states[0]->profiler.EndSample(chunk ? chunk->size() : 0);
	return selected_tuples;
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	D_ASSERT(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(idx_t expr_idx, Vector &result) {
	D_ASSERT(expr_idx < expressions.size());
	D_ASSERT(result.GetType().id() == expressions[expr_idx]->return_type.id());
	states[expr_idx]->profiler.BeginSample();
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), nullptr, chunk ? chunk->size() : 1, result);
	states[expr_idx]->profiler.EndSample(chunk ? chunk->size() : 0);
}

Value ExpressionExecutor::EvaluateScalar(ClientContext &context, const Expression &expr, bool allow_unfoldable) {
	D_ASSERT(allow_unfoldable || expr.IsFoldable());
	D_ASSERT(expr.IsScalar());
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor(context, expr);

	Vector result(expr.return_type);
	executor.ExecuteExpression(result);

	D_ASSERT(allow_unfoldable || result.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto result_value = result.GetValue(0);
	D_ASSERT(result_value.type().InternalType() == expr.return_type.InternalType());
	return result_value;
}

bool ExpressionExecutor::TryEvaluateScalar(ClientContext &context, const Expression &expr, Value &result) {
	try {
		result = EvaluateScalar(context, expr);
		return true;
	} catch (InternalException &ex) {
		throw;
	} catch (...) {
		return false;
	}
}

void ExpressionExecutor::Verify(const Expression &expr, Vector &vector, idx_t count) {
	D_ASSERT(expr.return_type.id() == vector.GetType().id());
	vector.Verify(count);
	if (expr.verification_stats) {
		expr.verification_stats->Verify(vector, count);
	}
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const Expression &expr,
                                                                ExpressionExecutorState &state) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_REF:
		return InitializeState(expr.Cast<BoundReferenceExpression>(), state);
	case ExpressionClass::BOUND_BETWEEN:
		return InitializeState(expr.Cast<BoundBetweenExpression>(), state);
	case ExpressionClass::BOUND_CASE:
		return InitializeState(expr.Cast<BoundCaseExpression>(), state);
	case ExpressionClass::BOUND_CAST:
		return InitializeState(expr.Cast<BoundCastExpression>(), state);
	case ExpressionClass::BOUND_COMPARISON:
		return InitializeState(expr.Cast<BoundComparisonExpression>(), state);
	case ExpressionClass::BOUND_CONJUNCTION:
		return InitializeState(expr.Cast<BoundConjunctionExpression>(), state);
	case ExpressionClass::BOUND_CONSTANT:
		return InitializeState(expr.Cast<BoundConstantExpression>(), state);
	case ExpressionClass::BOUND_FUNCTION:
		return InitializeState(expr.Cast<BoundFunctionExpression>(), state);
	case ExpressionClass::BOUND_OPERATOR:
		return InitializeState(expr.Cast<BoundOperatorExpression>(), state);
	case ExpressionClass::BOUND_PARAMETER:
		return InitializeState(expr.Cast<BoundParameterExpression>(), state);
	default:
		throw InternalException("Attempting to initialize state of expression of unknown type!");
	}
}

void ExpressionExecutor::Execute(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
#ifdef DEBUG
	//! The result Vector must be "clean"
	if (result.GetVectorType() == VectorType::FLAT_VECTOR) {
		D_ASSERT(FlatVector::Validity(result).CheckAllValid(count));
	}
#endif

	if (count == 0) {
		return;
	}
	if (result.GetType().id() != expr.return_type.id()) {
		throw InternalException(
		    "ExpressionExecutor::Execute called with a result vector of type %s that does not match expression type %s",
		    result.GetType(), expr.return_type);
	}
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute(expr.Cast<BoundBetweenExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_REF:
		Execute(expr.Cast<BoundReferenceExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute(expr.Cast<BoundCaseExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute(expr.Cast<BoundCastExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute(expr.Cast<BoundComparisonExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute(expr.Cast<BoundConjunctionExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute(expr.Cast<BoundConstantExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute(expr.Cast<BoundFunctionExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute(expr.Cast<BoundOperatorExpression>(), state, sel, count, result);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute(expr.Cast<BoundParameterExpression>(), state, sel, count, result);
		break;
	default:
		throw InternalException("Attempting to execute expression of unknown type!");
	}
	Verify(expr, result, count);
}

idx_t ExpressionExecutor::Select(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (count == 0) {
		return 0;
	}
	D_ASSERT(true_sel || false_sel);
	D_ASSERT(expr.return_type.id() == LogicalTypeId::BOOLEAN);
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		return Select(expr.Cast<BoundBetweenExpression>(), state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_COMPARISON:
		return Select(expr.Cast<BoundComparisonExpression>(), state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select(expr.Cast<BoundConjunctionExpression>(), state, sel, count, true_sel, false_sel);
	default:
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
}

template <bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DefaultSelectLoop(const SelectionVector *bsel, const uint8_t *__restrict bdata, ValidityMask &mask,
                                      const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                      SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto bidx = bsel->get_index(i);
		auto result_idx = sel->get_index(i);
		if (bdata[bidx] > 0 && (NO_NULL || mask.RowIsValid(bidx))) {
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count++, result_idx);
			}
		} else {
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count++, result_idx);
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <bool NO_NULL>
static inline idx_t DefaultSelectSwitch(UnifiedVectorFormat &idata, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DefaultSelectLoop<NO_NULL, true, true>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                              idata.validity, sel, count, true_sel, false_sel);
	} else if (true_sel) {
		return DefaultSelectLoop<NO_NULL, true, false>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                               idata.validity, sel, count, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DefaultSelectLoop<NO_NULL, false, true>(idata.sel, UnifiedVectorFormat::GetData<uint8_t>(idata),
		                                               idata.validity, sel, count, true_sel, false_sel);
	}
}

idx_t ExpressionExecutor::DefaultSelect(const Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                        idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(LogicalType::BOOLEAN, data_ptr_cast(intermediate_bools));
	Execute(expr, state, sel, count, intermediate);

	UnifiedVectorFormat idata;
	intermediate.ToUnifiedFormat(count, idata);

	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	if (!idata.validity.AllValid()) {
		return DefaultSelectSwitch<false>(idata, sel, count, true_sel, false_sel);
	} else {
		return DefaultSelectSwitch<true>(idata, sel, count, true_sel, false_sel);
	}
}

vector<unique_ptr<ExpressionExecutorState>> &ExpressionExecutor::GetStates() {
	return states;
}

} // namespace duckdb





namespace duckdb {

void ExpressionState::AddChild(Expression *expr) {
	types.push_back(expr->return_type);
	child_states.push_back(ExpressionExecutor::InitializeState(*expr, root));
}

void ExpressionState::Finalize() {
	if (!types.empty()) {
		intermediate_chunk.Initialize(GetAllocator(), types);
	}
}

Allocator &ExpressionState::GetAllocator() {
	return root.executor->GetAllocator();
}

bool ExpressionState::HasContext() {
	return root.executor->HasContext();
}

ClientContext &ExpressionState::GetContext() {
	if (!HasContext()) {
		throw BinderException("Cannot use %s in this context", (expr.Cast<BoundFunctionExpression>()).function.name);
	}
	return root.executor->GetContext();
}

ExpressionState::ExpressionState(const Expression &expr, ExpressionExecutorState &root) : expr(expr), root(root) {
}

ExpressionExecutorState::ExpressionExecutorState() : profiler() {
}

void ExpressionState::Verify(ExpressionExecutorState &root_executor) {
	D_ASSERT(&root_executor == &root);
	for (auto &entry : child_states) {
		entry->Verify(root_executor);
	}
}

void ExpressionExecutorState::Verify() {
	D_ASSERT(executor);
	root_state->Verify(*this);
}

} // namespace duckdb



















#include <algorithm>

namespace duckdb {

struct ARTIndexScanState : public IndexScanState {

	//! Scan predicates (single predicate scan or range scan)
	Value values[2];
	//! Expressions of the scan predicates
	ExpressionType expressions[2];
	bool checked = false;
	//! All scanned row IDs
	vector<row_t> result_ids;
	Iterator iterator;
};

ART::ART(const vector<column_t> &column_ids, TableIOManager &table_io_manager,
         const vector<unique_ptr<Expression>> &unbound_expressions, const IndexConstraintType constraint_type,
         AttachedDatabase &db, const shared_ptr<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>> &allocators_ptr,
         const BlockPointer &pointer)
    : Index(db, IndexType::ART, table_io_manager, column_ids, unbound_expressions, constraint_type),
      allocators(allocators_ptr), owns_data(false) {
	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("ART indexes are not supported on big endian architectures");
	}

	// initialize all allocators
	if (!allocators) {
		owns_data = true;
		auto &block_manager = table_io_manager.GetIndexBlockManager();

		array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT> allocator_array = {
		    make_uniq<FixedSizeAllocator>(sizeof(Prefix), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Leaf), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node4), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node16), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node48), block_manager),
		    make_uniq<FixedSizeAllocator>(sizeof(Node256), block_manager)};
		allocators = make_shared<array<unique_ptr<FixedSizeAllocator>, ALLOCATOR_COUNT>>(std::move(allocator_array));
	}

	if (pointer.IsValid()) {
		Deserialize(pointer);
	}

	// validate the types of the key columns
	for (idx_t i = 0; i < types.size(); i++) {
		switch (types[i]) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		case PhysicalType::FLOAT:
		case PhysicalType::DOUBLE:
		case PhysicalType::VARCHAR:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index key.");
		}
	}
}

//===--------------------------------------------------------------------===//
// Initialize Predicate Scans
//===--------------------------------------------------------------------===//

unique_ptr<IndexScanState> ART::InitializeScanSinglePredicate(const Transaction &transaction, const Value &value,
                                                              const ExpressionType expression_type) {
	// initialize point lookup
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> ART::InitializeScanTwoPredicates(const Transaction &transaction, const Value &low_value,
                                                            const ExpressionType low_expression_type,
                                                            const Value &high_value,
                                                            const ExpressionType high_expression_type) {
	// initialize range lookup
	auto result = make_uniq<ARTIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Keys
//===--------------------------------------------------------------------===//

template <class T>
static void TemplatedGenerateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	D_ASSERT(keys.size() >= count);
	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);
		if (idata.validity.RowIsValid(idx)) {
			ARTKey::CreateARTKey<T>(allocator, input.GetType(), keys[i], input_data[idx]);
		} else {
			// we need to possibly reset the former key value in the keys vector
			keys[i] = ARTKey();
		}
	}
}

template <class T>
static void ConcatenateKeys(ArenaAllocator &allocator, Vector &input, idx_t count, vector<ARTKey> &keys) {
	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);

	auto input_data = UnifiedVectorFormat::GetData<T>(idata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = idata.sel->get_index(i);

		// key is not NULL (no previous column entry was NULL)
		if (!keys[i].Empty()) {
			if (!idata.validity.RowIsValid(idx)) {
				// this column entry is NULL, set whole key to NULL
				keys[i] = ARTKey();
			} else {
				auto other_key = ARTKey::CreateARTKey<T>(allocator, input.GetType(), input_data[idx]);
				keys[i].ConcatenateARTKey(allocator, other_key);
			}
		}
	}
}

void ART::GenerateKeys(ArenaAllocator &allocator, DataChunk &input, vector<ARTKey> &keys) {
	// generate keys for the first input column
	switch (input.data[0].GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedGenerateKeys<bool>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT8:
		TemplatedGenerateKeys<int8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateKeys<int16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateKeys<int32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateKeys<int64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::INT128:
		TemplatedGenerateKeys<hugeint_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT8:
		TemplatedGenerateKeys<uint8_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT16:
		TemplatedGenerateKeys<uint16_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT32:
		TemplatedGenerateKeys<uint32_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::UINT64:
		TemplatedGenerateKeys<uint64_t>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateKeys<float>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateKeys<double>(allocator, input.data[0], input.size(), keys);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGenerateKeys<string_t>(allocator, input.data[0], input.size(), keys);
		break;
	default:
		throw InternalException("Invalid type for index");
	}

	for (idx_t i = 1; i < input.ColumnCount(); i++) {
		// for each of the remaining columns, concatenate
		switch (input.data[i].GetType().InternalType()) {
		case PhysicalType::BOOL:
			ConcatenateKeys<bool>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT8:
			ConcatenateKeys<int8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT16:
			ConcatenateKeys<int16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT32:
			ConcatenateKeys<int32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT64:
			ConcatenateKeys<int64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::INT128:
			ConcatenateKeys<hugeint_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT8:
			ConcatenateKeys<uint8_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT16:
			ConcatenateKeys<uint16_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT32:
			ConcatenateKeys<uint32_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::UINT64:
			ConcatenateKeys<uint64_t>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::FLOAT:
			ConcatenateKeys<float>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::DOUBLE:
			ConcatenateKeys<double>(allocator, input.data[i], input.size(), keys);
			break;
		case PhysicalType::VARCHAR:
			ConcatenateKeys<string_t>(allocator, input.data[i], input.size(), keys);
			break;
		default:
			throw InternalException("Invalid type for index");
		}
	}
}

//===--------------------------------------------------------------------===//
// Construct from sorted data (only during CREATE (UNIQUE) INDEX statements)
//===--------------------------------------------------------------------===//

struct KeySection {
	KeySection(idx_t start_p, idx_t end_p, idx_t depth_p, data_t key_byte_p)
	    : start(start_p), end(end_p), depth(depth_p), key_byte(key_byte_p) {};
	KeySection(idx_t start_p, idx_t end_p, vector<ARTKey> &keys, KeySection &key_section)
	    : start(start_p), end(end_p), depth(key_section.depth + 1), key_byte(keys[end_p].data[key_section.depth]) {};
	idx_t start;
	idx_t end;
	idx_t depth;
	data_t key_byte;
};

void GetChildSections(vector<KeySection> &child_sections, vector<ARTKey> &keys, KeySection &key_section) {

	idx_t child_start_idx = key_section.start;
	for (idx_t i = key_section.start + 1; i <= key_section.end; i++) {
		if (keys[i - 1].data[key_section.depth] != keys[i].data[key_section.depth]) {
			child_sections.emplace_back(child_start_idx, i - 1, keys, key_section);
			child_start_idx = i;
		}
	}
	child_sections.emplace_back(child_start_idx, key_section.end, keys, key_section);
}

bool Construct(ART &art, vector<ARTKey> &keys, row_t *row_ids, Node &node, KeySection &key_section,
               bool &has_constraint) {

	D_ASSERT(key_section.start < keys.size());
	D_ASSERT(key_section.end < keys.size());
	D_ASSERT(key_section.start <= key_section.end);

	auto &start_key = keys[key_section.start];
	auto &end_key = keys[key_section.end];

	// increment the depth until we reach a leaf or find a mismatching byte
	auto prefix_start = key_section.depth;
	while (start_key.len != key_section.depth && start_key.ByteMatches(end_key, key_section.depth)) {
		key_section.depth++;
	}

	// we reached a leaf, i.e. all the bytes of start_key and end_key match
	if (start_key.len == key_section.depth) {
		// end_idx is inclusive
		auto num_row_ids = key_section.end - key_section.start + 1;

		// check for possible constraint violation
		auto single_row_id = num_row_ids == 1;
		if (has_constraint && !single_row_id) {
			return false;
		}

		reference<Node> ref_node(node);
		Prefix::New(art, ref_node, start_key, prefix_start, start_key.len - prefix_start);
		if (single_row_id) {
			Leaf::New(ref_node, row_ids[key_section.start]);
		} else {
			Leaf::New(art, ref_node, row_ids + key_section.start, num_row_ids);
		}
		return true;
	}

	// create a new node and recurse

	// we will find at least two child entries of this node, otherwise we'd have reached a leaf
	vector<KeySection> child_sections;
	GetChildSections(child_sections, keys, key_section);

	// set the prefix
	reference<Node> ref_node(node);
	auto prefix_length = key_section.depth - prefix_start;
	Prefix::New(art, ref_node, start_key, prefix_start, prefix_length);

	// set the node
	auto node_type = Node::GetARTNodeTypeByCount(child_sections.size());
	Node::New(art, ref_node, node_type);

	// recurse on each child section
	for (auto &child_section : child_sections) {
		Node new_child;
		auto no_violation = Construct(art, keys, row_ids, new_child, child_section, has_constraint);
		Node::InsertChild(art, ref_node, child_section.key_byte, new_child);
		if (!no_violation) {
			return false;
		}
	}
	return true;
}

bool ART::ConstructFromSorted(idx_t count, vector<ARTKey> &keys, Vector &row_identifiers) {

	// prepare the row_identifiers
	row_identifiers.Flatten(count);
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	auto key_section = KeySection(0, count - 1, 0, 0);
	auto has_constraint = IsUnique();
	if (!Construct(*this, keys, row_ids, tree, key_section, has_constraint)) {
		return false;
	}

#ifdef DEBUG
	D_ASSERT(!VerifyAndToStringInternal(true).empty());
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(!keys[i].Empty());
		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_ids[i]));
	}
#endif

	return true;
}

//===--------------------------------------------------------------------===//
// Insert / Verification / Constraint Checking
//===--------------------------------------------------------------------===//
PreservedError ART::Insert(IndexLock &lock, DataChunk &input, Vector &row_ids) {

	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(logical_types[0] == input.data[0].GetType());

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(input.size());
	GenerateKeys(arena_allocator, input, keys);

	// get the corresponding row IDs
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	// now insert the elements into the index
	idx_t failed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		row_t row_id = row_identifiers[i];
		if (!Insert(tree, keys[i], 0, row_id)) {
			// failed to insert because of constraint violation
			failed_index = i;
			break;
		}
	}

	// failed to insert because of constraint violation: remove previously inserted entries
	if (failed_index != DConstants::INVALID_INDEX) {
		for (idx_t i = 0; i < failed_index; i++) {
			if (keys[i].Empty()) {
				continue;
			}
			row_t row_id = row_identifiers[i];
			Erase(tree, keys[i], 0, row_id);
		}
	}

	if (failed_index != DConstants::INVALID_INDEX) {
		return PreservedError(ConstraintException("PRIMARY KEY or UNIQUE constraint violated: duplicate key \"%s\"",
		                                          AppendRowError(input, failed_index)));
	}

#ifdef DEBUG
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		D_ASSERT(Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
	}
#endif

	return PreservedError();
}

PreservedError ART::Append(IndexLock &lock, DataChunk &appended_data, Vector &row_identifiers) {
	DataChunk expression_result;
	expression_result.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions for the index
	ExecuteExpressions(appended_data, expression_result);

	// now insert into the index
	return Insert(lock, expression_result, row_identifiers);
}

void ART::VerifyAppend(DataChunk &chunk) {
	ConflictManager conflict_manager(VerifyExistenceType::APPEND, chunk.size());
	CheckConstraintsForChunk(chunk, conflict_manager);
}

void ART::VerifyAppend(DataChunk &chunk, ConflictManager &conflict_manager) {
	D_ASSERT(conflict_manager.LookupType() == VerifyExistenceType::APPEND);
	CheckConstraintsForChunk(chunk, conflict_manager);
}

bool ART::InsertToLeaf(Node &leaf, const row_t &row_id) {

	if (IsUnique()) {
		return false;
	}

	Leaf::Insert(*this, leaf, row_id);
	return true;
}

bool ART::Insert(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	// node is currently empty, create a leaf here with the key
	if (!node.HasMetadata()) {
		D_ASSERT(depth <= key.len);
		reference<Node> ref_node(node);
		Prefix::New(*this, ref_node, key, depth, key.len - depth);
		Leaf::New(ref_node, row_id);
		return true;
	}

	auto node_type = node.GetType();

	// insert the row ID into this leaf
	if (node_type == NType::LEAF || node_type == NType::LEAF_INLINED) {
		return InsertToLeaf(node, row_id);
	}

	if (node_type != NType::PREFIX) {
		D_ASSERT(depth < key.len);
		auto child = node.GetChildMutable(*this, key[depth]);

		// recurse, if a child exists at key[depth]
		if (child) {
			bool success = Insert(*child, key, depth + 1, row_id);
			node.ReplaceChild(*this, key[depth], *child);
			return success;
		}

		// insert a new leaf node at key[depth]
		Node leaf_node;
		reference<Node> ref_node(leaf_node);
		if (depth + 1 < key.len) {
			Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
		}
		Leaf::New(ref_node, row_id);
		Node::InsertChild(*this, node, key[depth], leaf_node);
		return true;
	}

	// this is a prefix node, traverse
	reference<Node> next_node(node);
	auto mismatch_position = Prefix::TraverseMutable(*this, next_node, key, depth);

	// prefix matches key
	if (next_node.get().GetType() != NType::PREFIX) {
		return Insert(next_node, key, depth, row_id);
	}

	// prefix does not match the key, we need to create a new Node4; this new Node4 has two children,
	// the remaining part of the prefix, and the new leaf
	Node remaining_prefix;
	auto prefix_byte = Prefix::GetByte(*this, next_node, mismatch_position);
	Prefix::Split(*this, next_node, remaining_prefix, mismatch_position);
	Node4::New(*this, next_node);

	// insert remaining prefix
	Node4::InsertChild(*this, next_node, prefix_byte, remaining_prefix);

	// insert new leaf
	Node leaf_node;
	reference<Node> ref_node(leaf_node);
	if (depth + 1 < key.len) {
		Prefix::New(*this, ref_node, key, depth + 1, key.len - depth - 1);
	}
	Leaf::New(ref_node, row_id);
	Node4::InsertChild(*this, next_node, key[depth], leaf_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Drop and Delete
//===--------------------------------------------------------------------===//

void ART::CommitDrop(IndexLock &index_lock) {
	for (auto &allocator : *allocators) {
		allocator->Reset();
	}
	tree.Clear();
}

void ART::Delete(IndexLock &state, DataChunk &input, Vector &row_ids) {

	DataChunk expression;
	expression.Initialize(Allocator::DefaultAllocator(), logical_types);

	// first resolve the expressions
	ExecuteExpressions(input, expression);

	// then generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(expression.size());
	GenerateKeys(arena_allocator, expression, keys);

	// now erase the elements from the database
	row_ids.Flatten(input.size());
	auto row_identifiers = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}
		Erase(tree, keys[i], 0, row_identifiers[i]);
	}

#ifdef DEBUG
	// verify that we removed all row IDs
	for (idx_t i = 0; i < input.size(); i++) {
		if (keys[i].Empty()) {
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		if (leaf) {
			D_ASSERT(!Leaf::ContainsRowId(*this, *leaf, row_identifiers[i]));
		}
	}
#endif
}

void ART::Erase(Node &node, const ARTKey &key, idx_t depth, const row_t &row_id) {

	if (!node.HasMetadata()) {
		return;
	}

	// handle prefix
	reference<Node> next_node(node);
	if (next_node.get().GetType() == NType::PREFIX) {
		Prefix::TraverseMutable(*this, next_node, key, depth);
		if (next_node.get().GetType() == NType::PREFIX) {
			return;
		}
	}

	// delete a row ID from a leaf (root is leaf with possible prefix nodes)
	if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
		if (Leaf::Remove(*this, next_node, row_id)) {
			Node::Free(*this, node);
		}
		return;
	}

	D_ASSERT(depth < key.len);
	auto child = next_node.get().GetChildMutable(*this, key[depth]);
	if (child) {
		D_ASSERT(child->HasMetadata());

		auto temp_depth = depth + 1;
		reference<Node> child_node(*child);
		if (child_node.get().GetType() == NType::PREFIX) {
			Prefix::TraverseMutable(*this, child_node, key, temp_depth);
			if (child_node.get().GetType() == NType::PREFIX) {
				return;
			}
		}

		if (child_node.get().GetType() == NType::LEAF || child_node.get().GetType() == NType::LEAF_INLINED) {
			// leaf found, remove entry
			if (Leaf::Remove(*this, child_node, row_id)) {
				Node::DeleteChild(*this, next_node, node, key[depth]);
			}
			return;
		}

		// recurse
		Erase(*child, key, depth + 1, row_id);
		next_node.get().ReplaceChild(*this, key[depth], *child);
	}
}

//===--------------------------------------------------------------------===//
// Point Query (Equal)
//===--------------------------------------------------------------------===//

static ARTKey CreateKey(ArenaAllocator &allocator, PhysicalType type, Value &value) {
	D_ASSERT(type == value.type().InternalType());
	switch (type) {
	case PhysicalType::BOOL:
		return ARTKey::CreateARTKey<bool>(allocator, value.type(), value);
	case PhysicalType::INT8:
		return ARTKey::CreateARTKey<int8_t>(allocator, value.type(), value);
	case PhysicalType::INT16:
		return ARTKey::CreateARTKey<int16_t>(allocator, value.type(), value);
	case PhysicalType::INT32:
		return ARTKey::CreateARTKey<int32_t>(allocator, value.type(), value);
	case PhysicalType::INT64:
		return ARTKey::CreateARTKey<int64_t>(allocator, value.type(), value);
	case PhysicalType::UINT8:
		return ARTKey::CreateARTKey<uint8_t>(allocator, value.type(), value);
	case PhysicalType::UINT16:
		return ARTKey::CreateARTKey<uint16_t>(allocator, value.type(), value);
	case PhysicalType::UINT32:
		return ARTKey::CreateARTKey<uint32_t>(allocator, value.type(), value);
	case PhysicalType::UINT64:
		return ARTKey::CreateARTKey<uint64_t>(allocator, value.type(), value);
	case PhysicalType::INT128:
		return ARTKey::CreateARTKey<hugeint_t>(allocator, value.type(), value);
	case PhysicalType::FLOAT:
		return ARTKey::CreateARTKey<float>(allocator, value.type(), value);
	case PhysicalType::DOUBLE:
		return ARTKey::CreateARTKey<double>(allocator, value.type(), value);
	case PhysicalType::VARCHAR:
		return ARTKey::CreateARTKey<string_t>(allocator, value.type(), value);
	default:
		throw InternalException("Invalid type for the ART key");
	}
}

bool ART::SearchEqual(ARTKey &key, idx_t max_count, vector<row_t> &result_ids) {

	auto leaf = Lookup(tree, key, 0);
	if (!leaf) {
		return true;
	}
	return Leaf::GetRowIds(*this, *leaf, result_ids, max_count);
}

void ART::SearchEqualJoinNoFetch(ARTKey &key, idx_t &result_size) {

	// we need to look for a leaf
	auto leaf_node = Lookup(tree, key, 0);
	if (!leaf_node) {
		result_size = 0;
		return;
	}

	// we only perform index joins on PK/FK columns
	D_ASSERT(leaf_node->GetType() == NType::LEAF_INLINED);
	result_size = 1;
	return;
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//

optional_ptr<const Node> ART::Lookup(const Node &node, const ARTKey &key, idx_t depth) {

	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {

		// traverse prefix, if exists
		reference<const Node> next_node(node_ref.get());
		if (next_node.get().GetType() == NType::PREFIX) {
			Prefix::Traverse(*this, next_node, key, depth);
			if (next_node.get().GetType() == NType::PREFIX) {
				return nullptr;
			}
		}

		if (next_node.get().GetType() == NType::LEAF || next_node.get().GetType() == NType::LEAF_INLINED) {
			return &next_node.get();
		}

		D_ASSERT(depth < key.len);
		auto child = next_node.get().GetChild(*this, key[depth]);
		if (!child) {
			// prefix matches key, but no child at byte, ART/subtree does not contain key
			return nullptr;
		}

		// lookup in child node
		node_ref = *child;
		D_ASSERT(node_ref.get().HasMetadata());
		depth++;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// Greater Than and Less Than
//===--------------------------------------------------------------------===//

bool ART::SearchGreater(ARTIndexScanState &state, ARTKey &key, bool equal, idx_t max_count, vector<row_t> &result_ids) {

	if (!tree.HasMetadata()) {
		return true;
	}
	Iterator &it = state.iterator;

	// find the lowest value that satisfies the predicate
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, key, equal, 0)) {
			// early-out, if the maximum value in the ART is lower than the lower bound
			return true;
		}
	}

	// after that we continue the scan; we don't need to check the bounds as any value following this value is
	// automatically bigger and hence satisfies our predicate
	ARTKey empty_key = ARTKey();
	return it.Scan(empty_key, max_count, result_ids, false);
}

bool ART::SearchLess(ARTIndexScanState &state, ARTKey &upper_bound, bool equal, idx_t max_count,
                     vector<row_t> &result_ids) {

	if (!tree.HasMetadata()) {
		return true;
	}
	Iterator &it = state.iterator;

	if (!it.art) {
		it.art = this;
		// find the minimum value in the ART: we start scanning from this value
		it.FindMinimum(tree);
		// early-out, if the minimum value is higher than the upper bound
		if (it.current_key > upper_bound) {
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, equal);
}

//===--------------------------------------------------------------------===//
// Closed Range Query
//===--------------------------------------------------------------------===//

bool ART::SearchCloseRange(ARTIndexScanState &state, ARTKey &lower_bound, ARTKey &upper_bound, bool left_equal,
                           bool right_equal, idx_t max_count, vector<row_t> &result_ids) {

	Iterator &it = state.iterator;

	// find the first node that satisfies the left predicate
	if (!it.art) {
		it.art = this;
		if (!it.LowerBound(tree, lower_bound, left_equal, 0)) {
			// early-out, if the maximum value in the ART is lower than the lower bound
			return true;
		}
	}

	// now continue the scan until we reach the upper bound
	return it.Scan(upper_bound, max_count, result_ids, right_equal);
}

bool ART::Scan(const Transaction &transaction, const DataTable &table, IndexScanState &state, const idx_t max_count,
               vector<row_t> &result_ids) {

	auto &scan_state = state.Cast<ARTIndexScanState>();
	vector<row_t> row_ids;
	bool success;

	// FIXME: the key directly owning the data for a single key might be more efficient
	D_ASSERT(scan_state.values[0].type().InternalType() == types[0]);
	ArenaAllocator arena_allocator(Allocator::Get(db));
	auto key = CreateKey(arena_allocator, types[0], scan_state.values[0]);

	if (scan_state.values[1].IsNull()) {

		// single predicate
		lock_guard<mutex> l(lock);
		switch (scan_state.expressions[0]) {
		case ExpressionType::COMPARE_EQUAL:
			success = SearchEqual(key, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			success = SearchGreater(scan_state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			success = SearchGreater(scan_state, key, false, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			success = SearchLess(scan_state, key, true, max_count, row_ids);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			success = SearchLess(scan_state, key, false, max_count, row_ids);
			break;
		default:
			throw InternalException("Index scan type not implemented");
		}

	} else {

		// two predicates
		lock_guard<mutex> l(lock);

		D_ASSERT(scan_state.values[1].type().InternalType() == types[0]);
		auto upper_bound = CreateKey(arena_allocator, types[0], scan_state.values[1]);

		bool left_equal = scan_state.expressions[0] == ExpressionType ::COMPARE_GREATERTHANOREQUALTO;
		bool right_equal = scan_state.expressions[1] == ExpressionType ::COMPARE_LESSTHANOREQUALTO;
		success = SearchCloseRange(scan_state, key, upper_bound, left_equal, right_equal, max_count, row_ids);
	}

	if (!success) {
		return false;
	}
	if (row_ids.empty()) {
		return true;
	}

	// sort the row ids
	sort(row_ids.begin(), row_ids.end());
	// duplicate eliminate the row ids and append them to the row ids of the state
	result_ids.reserve(row_ids.size());

	result_ids.push_back(row_ids[0]);
	for (idx_t i = 1; i < row_ids.size(); i++) {
		if (row_ids[i] != row_ids[i - 1]) {
			result_ids.push_back(row_ids[i]);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// More Verification / Constraint Checking
//===--------------------------------------------------------------------===//

string ART::GenerateErrorKeyName(DataChunk &input, idx_t row) {

	// FIXME: why exactly can we not pass the expression_chunk as an argument to this
	// FIXME: function instead of re-executing?
	// re-executing the expressions is not very fast, but we're going to throw, so we don't care
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	string key_name;
	for (idx_t k = 0; k < expression_chunk.ColumnCount(); k++) {
		if (k > 0) {
			key_name += ", ";
		}
		key_name += unbound_expressions[k]->GetName() + ": " + expression_chunk.data[k].GetValue(row).ToString();
	}
	return key_name;
}

string ART::GenerateConstraintErrorMessage(VerifyExistenceType verify_type, const string &key_name) {
	switch (verify_type) {
	case VerifyExistenceType::APPEND: {
		// APPEND to PK/UNIQUE table, but node/key already exists in PK/UNIQUE table
		string type = IsPrimary() ? "primary key" : "unique";
		return StringUtil::Format(
		    "Duplicate key \"%s\" violates %s constraint. "
		    "If this is an unexpected constraint violation please double "
		    "check with the known index limitations section in our documentation (docs - sql - indexes).",
		    key_name, type);
	}
	case VerifyExistenceType::APPEND_FK: {
		// APPEND_FK to FK table, node/key does not exist in PK/UNIQUE table
		return StringUtil::Format(
		    "Violates foreign key constraint because key \"%s\" does not exist in the referenced table", key_name);
	}
	case VerifyExistenceType::DELETE_FK: {
		// DELETE_FK that still exists in a FK table, i.e., not a valid delete
		return StringUtil::Format("Violates foreign key constraint because key \"%s\" is still referenced by a foreign "
		                          "key in a different table",
		                          key_name);
	}
	default:
		throw NotImplementedException("Type not implemented for VerifyExistenceType");
	}
}

void ART::CheckConstraintsForChunk(DataChunk &input, ConflictManager &conflict_manager) {

	// don't alter the index during constraint checking
	lock_guard<mutex> l(lock);

	// first resolve the expressions for the index
	DataChunk expression_chunk;
	expression_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(input, expression_chunk);

	// generate the keys for the given input
	ArenaAllocator arena_allocator(BufferAllocator::Get(db));
	vector<ARTKey> keys(expression_chunk.size());
	GenerateKeys(arena_allocator, expression_chunk, keys);

	idx_t found_conflict = DConstants::INVALID_INDEX;
	for (idx_t i = 0; found_conflict == DConstants::INVALID_INDEX && i < input.size(); i++) {

		if (keys[i].Empty()) {
			if (conflict_manager.AddNull(i)) {
				found_conflict = i;
			}
			continue;
		}

		auto leaf = Lookup(tree, keys[i], 0);
		if (!leaf) {
			if (conflict_manager.AddMiss(i)) {
				found_conflict = i;
			}
			continue;
		}

		// when we find a node, we need to update the 'matches' and 'row_ids'
		// NOTE: leaves can have more than one row_id, but for UNIQUE/PRIMARY KEY they will only have one
		D_ASSERT(leaf->GetType() == NType::LEAF_INLINED);
		if (conflict_manager.AddHit(i, leaf->GetRowId())) {
			found_conflict = i;
		}
	}

	conflict_manager.FinishLookup();

	if (found_conflict == DConstants::INVALID_INDEX) {
		return;
	}

	auto key_name = GenerateErrorKeyName(input, found_conflict);
	auto exception_msg = GenerateConstraintErrorMessage(conflict_manager.LookupType(), key_name);
	throw ConstraintException(exception_msg);
}

//===--------------------------------------------------------------------===//
// Serialization
//===--------------------------------------------------------------------===//

BlockPointer ART::Serialize(MetadataWriter &writer) {

	D_ASSERT(owns_data);

	// early-out, if all allocators are empty
	if (!tree.HasMetadata()) {
		root_block_pointer = BlockPointer();
		return root_block_pointer;
	}

	lock_guard<mutex> l(lock);
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	PartialBlockManager partial_block_manager(block_manager, CheckpointType::FULL_CHECKPOINT);

	vector<BlockPointer> allocator_pointers;
	for (auto &allocator : *allocators) {
		allocator_pointers.push_back(allocator->Serialize(partial_block_manager, writer));
	}
	partial_block_manager.FlushPartialBlocks();

	root_block_pointer = writer.GetBlockPointer();
	writer.Write(tree);
	for (auto &allocator_pointer : allocator_pointers) {
		writer.Write(allocator_pointer);
	}

	return root_block_pointer;
}

void ART::Deserialize(const BlockPointer &pointer) {

	D_ASSERT(pointer.IsValid());
	MetadataReader reader(table_io_manager.GetMetadataManager(), pointer);
	tree = reader.Read<Node>();

	for (idx_t i = 0; i < ALLOCATOR_COUNT; i++) {
		(*allocators)[i]->Deserialize(reader.Read<BlockPointer>());
	}
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void ART::InitializeVacuum(ARTFlags &flags) {

	flags.vacuum_flags.reserve(allocators->size());
	for (auto &allocator : *allocators) {
		flags.vacuum_flags.push_back(allocator->InitializeVacuum());
	}
}

void ART::FinalizeVacuum(const ARTFlags &flags) {

	for (idx_t i = 0; i < allocators->size(); i++) {
		if (flags.vacuum_flags[i]) {
			(*allocators)[i]->FinalizeVacuum();
		}
	}
}

void ART::Vacuum(IndexLock &state) {

	D_ASSERT(owns_data);

	if (!tree.HasMetadata()) {
		for (auto &allocator : *allocators) {
			allocator->Reset();
		}
		return;
	}

	// holds true, if an allocator needs a vacuum, and false otherwise
	ARTFlags flags;
	InitializeVacuum(flags);

	// skip vacuum if no allocators require it
	auto perform_vacuum = false;
	for (const auto &vacuum_flag : flags.vacuum_flags) {
		if (vacuum_flag) {
			perform_vacuum = true;
			break;
		}
	}
	if (!perform_vacuum) {
		return;
	}

	// traverse the allocated memory of the tree to perform a vacuum
	tree.Vacuum(*this, flags);

	// finalize the vacuum operation
	FinalizeVacuum(flags);
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void ART::InitializeMerge(ARTFlags &flags) {

	D_ASSERT(owns_data);

	flags.merge_buffer_counts.reserve(allocators->size());
	for (auto &allocator : *allocators) {
		flags.merge_buffer_counts.emplace_back(allocator->GetUpperBoundBufferId());
	}
}

bool ART::MergeIndexes(IndexLock &state, Index &other_index) {

	auto &other_art = other_index.Cast<ART>();
	if (!other_art.tree.HasMetadata()) {
		return true;
	}

	if (other_art.owns_data) {
		if (tree.HasMetadata()) {
			//  fully deserialize other_index, and traverse it to increment its buffer IDs
			ARTFlags flags;
			InitializeMerge(flags);
			other_art.tree.InitializeMerge(other_art, flags);
		}

		// merge the node storage
		for (idx_t i = 0; i < allocators->size(); i++) {
			(*allocators)[i]->Merge(*(*other_art.allocators)[i]);
		}
	}

	// merge the ARTs
	if (!tree.Merge(*this, other_art.tree)) {
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string ART::VerifyAndToString(IndexLock &state, const bool only_verify) {
	// FIXME: this can be improved by counting the allocations of each node type,
	// FIXME: and by asserting that each fixed-size allocator lists an equal number of
	// FIXME: allocations of that type
	return VerifyAndToStringInternal(only_verify);
}

string ART::VerifyAndToStringInternal(const bool only_verify) {
	if (tree.HasMetadata()) {
		return "ART: " + tree.VerifyAndToString(*this, only_verify);
	}
	return "[empty]";
}

} // namespace duckdb


namespace duckdb {

ARTKey::ARTKey() : len(0) {
}

ARTKey::ARTKey(const data_ptr_t &data, const uint32_t &len) : len(len), data(data) {
}

ARTKey::ARTKey(ArenaAllocator &allocator, const uint32_t &len) : len(len) {
	data = allocator.Allocate(len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, string_t value) {
	uint32_t len = value.GetSize() + 1;
	auto data = allocator.Allocate(len);
	memcpy(data, value.GetData(), len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (uint32_t i = 0; i < len - 1; i++) {
			if (data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	data[len - 1] = '\0';
	return ARTKey(data, len);
}

template <>
ARTKey ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, const char *value) {
	return ARTKey::CreateARTKey(allocator, type, string_t(value, strlen(value)));
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, string_t value) {
	key.len = value.GetSize() + 1;
	key.data = allocator.Allocate(key.len);
	memcpy(key.data, value.GetData(), key.len - 1);

	// FIXME: rethink this
	if (type == LogicalType::BLOB || type == LogicalType::VARCHAR) {
		// indexes cannot contain BLOBs (or BLOBs cast to VARCHARs) that contain null-terminated bytes
		for (uint32_t i = 0; i < key.len - 1; i++) {
			if (key.data[i] == '\0') {
				throw NotImplementedException("Indexes cannot contain BLOBs that contain null-terminated bytes.");
			}
		}
	}

	key.data[key.len - 1] = '\0';
}

template <>
void ARTKey::CreateARTKey(ArenaAllocator &allocator, const LogicalType &type, ARTKey &key, const char *value) {
	ARTKey::CreateARTKey(allocator, type, key, string_t(value, strlen(value)));
}

bool ARTKey::operator>(const ARTKey &k) const {
	for (uint32_t i = 0; i < MinValue<uint32_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len > k.len;
}

bool ARTKey::operator>=(const ARTKey &k) const {
	for (uint32_t i = 0; i < MinValue<uint32_t>(len, k.len); i++) {
		if (data[i] > k.data[i]) {
			return true;
		} else if (data[i] < k.data[i]) {
			return false;
		}
	}
	return len >= k.len;
}

bool ARTKey::operator==(const ARTKey &k) const {
	if (len != k.len) {
		return false;
	}
	for (uint32_t i = 0; i < len; i++) {
		if (data[i] != k.data[i]) {
			return false;
		}
	}
	return true;
}

void ARTKey::ConcatenateARTKey(ArenaAllocator &allocator, ARTKey &other_key) {

	auto compound_data = allocator.Allocate(len + other_key.len);
	memcpy(compound_data, data, len);
	memcpy(compound_data + len, other_key.data, other_key.len);
	len += other_key.len;
	data = compound_data;
}
} // namespace duckdb







namespace duckdb {

bool IteratorKey::operator>(const ARTKey &key) const {
	for (idx_t i = 0; i < MinValue<idx_t>(key_bytes.size(), key.len); i++) {
		if (key_bytes[i] > key.data[i]) {
			return true;
		} else if (key_bytes[i] < key.data[i]) {
			return false;
		}
	}
	return key_bytes.size() > key.len;
}

bool IteratorKey::operator>=(const ARTKey &key) const {
	for (idx_t i = 0; i < MinValue<idx_t>(key_bytes.size(), key.len); i++) {
		if (key_bytes[i] > key.data[i]) {
			return true;
		} else if (key_bytes[i] < key.data[i]) {
			return false;
		}
	}
	return key_bytes.size() >= key.len;
}

bool IteratorKey::operator==(const ARTKey &key) const {
	// NOTE: we only use this for finding the LowerBound, in which case the length
	// has to be equal
	D_ASSERT(key_bytes.size() == key.len);
	for (idx_t i = 0; i < key_bytes.size(); i++) {
		if (key_bytes[i] != key.data[i]) {
			return false;
		}
	}
	return true;
}

bool Iterator::Scan(const ARTKey &upper_bound, const idx_t max_count, vector<row_t> &result_ids, const bool equal) {

	bool has_next;
	do {
		if (!upper_bound.Empty()) {
			// no more row IDs within the key bounds
			if (equal) {
				if (current_key > upper_bound) {
					return true;
				}
			} else {
				if (current_key >= upper_bound) {
					return true;
				}
			}
		}

		// copy all row IDs of this leaf into the result IDs (if they don't exceed max_count)
		if (!Leaf::GetRowIds(*art, last_leaf, result_ids, max_count)) {
			return false;
		}

		// get the next leaf
		has_next = Next();

	} while (has_next);

	return true;
}

void Iterator::FindMinimum(const Node &node) {

	D_ASSERT(node.HasMetadata());

	// found the minimum
	if (node.GetType() == NType::LEAF || node.GetType() == NType::LEAF_INLINED) {
		last_leaf = node;
		return;
	}

	// traverse the prefix
	if (node.GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(*art, node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			current_key.Push(prefix.data[i]);
		}
		nodes.emplace(node, 0);
		return FindMinimum(prefix.ptr);
	}

	// go to the leftmost entry in the current node and recurse
	uint8_t byte = 0;
	auto next = node.GetNextChild(*art, byte);
	D_ASSERT(next);
	current_key.Push(byte);
	nodes.emplace(node, byte);
	FindMinimum(*next);
}

bool Iterator::LowerBound(const Node &node, const ARTKey &key, const bool equal, idx_t depth) {

	if (!node.HasMetadata()) {
		return false;
	}

	// we found the lower bound
	if (node.GetType() == NType::LEAF || node.GetType() == NType::LEAF_INLINED) {
		if (!equal && current_key == key) {
			return Next();
		}
		last_leaf = node;
		return true;
	}

	if (node.GetType() != NType::PREFIX) {
		auto next_byte = key[depth];
		auto child = node.GetNextChild(*art, next_byte);
		if (!child) {
			// the key is greater than any key in this subtree
			return Next();
		}

		current_key.Push(next_byte);
		nodes.emplace(node, next_byte);

		if (next_byte > key[depth]) {
			// we only need to find the minimum from here
			// because all keys will be greater than the lower bound
			FindMinimum(*child);
			return true;
		}

		// recurse into the child
		return LowerBound(*child, key, equal, depth + 1);
	}

	// resolve the prefix
	auto &prefix = Node::Ref<const Prefix>(*art, node, NType::PREFIX);
	for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
		current_key.Push(prefix.data[i]);
	}
	nodes.emplace(node, 0);

	for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
		// the key down to this node is less than the lower bound, the next key will be
		// greater than the lower bound
		if (prefix.data[i] < key[depth + i]) {
			return Next();
		}
		// we only need to find the minimum from here
		// because all keys will be greater than the lower bound
		if (prefix.data[i] > key[depth + i]) {
			FindMinimum(prefix.ptr);
			return true;
		}
	}

	// recurse into the child
	depth += prefix.data[Node::PREFIX_SIZE];
	return LowerBound(prefix.ptr, key, equal, depth);
}

bool Iterator::Next() {

	while (!nodes.empty()) {

		auto &top = nodes.top();
		D_ASSERT(top.node.GetType() != NType::LEAF && top.node.GetType() != NType::LEAF_INLINED);

		if (top.node.GetType() == NType::PREFIX) {
			PopNode();
			continue;
		}

		if (top.byte == NumericLimits<uint8_t>::Maximum()) {
			// no node found: move up the tree, pop key byte of current node
			PopNode();
			continue;
		}

		top.byte++;
		auto next_node = top.node.GetNextChild(*art, top.byte);
		if (!next_node) {
			PopNode();
			continue;
		}

		current_key.Pop(1);
		current_key.Push(top.byte);

		FindMinimum(*next_node);
		return true;
	}
	return false;
}

void Iterator::PopNode() {
	if (nodes.top().node.GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(*art, nodes.top().node, NType::PREFIX);
		auto prefix_byte_count = prefix.data[Node::PREFIX_SIZE];
		current_key.Pop(prefix_byte_count);
	} else {
		current_key.Pop(1);
	}
	nodes.pop();
}

} // namespace duckdb





namespace duckdb {

void Leaf::New(Node &node, const row_t row_id) {

	// we directly inline this row ID into the node pointer
	D_ASSERT(row_id < MAX_ROW_ID_LOCAL);
	node.Clear();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF_INLINED));
	node.SetRowId(row_id);
}

void Leaf::New(ART &art, reference<Node> &node, const row_t *row_ids, idx_t count) {

	D_ASSERT(count > 1);

	idx_t copy_count = 0;
	while (count) {
		node.get() = Node::GetAllocator(art, NType::LEAF).New();
		node.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));

		auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

		leaf.count = MinValue((idx_t)Node::LEAF_SIZE, count);

		for (idx_t i = 0; i < leaf.count; i++) {
			leaf.row_ids[i] = row_ids[copy_count + i];
		}

		copy_count += leaf.count;
		count -= leaf.count;

		node = leaf.ptr;
		leaf.ptr.Clear();
	}
}

Leaf &Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::LEAF));
	auto &leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	leaf.count = 0;
	leaf.ptr.Clear();
	return leaf;
}

void Leaf::Free(ART &art, Node &node) {

	Node current_node = node;
	Node next_node;
	while (current_node.HasMetadata()) {
		next_node = Node::RefMutable<Leaf>(art, current_node, NType::LEAF).ptr;
		Node::GetAllocator(art, NType::LEAF).Free(current_node);
		current_node = next_node;
	}

	node.Clear();
}

void Leaf::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::LEAF) - 1];

	Node next_node = node;
	node.IncreaseBufferId(merge_buffer_count);

	while (next_node.HasMetadata()) {
		auto &leaf = Node::RefMutable<Leaf>(art, next_node, NType::LEAF);
		next_node = leaf.ptr;
		if (leaf.ptr.HasMetadata()) {
			leaf.ptr.IncreaseBufferId(merge_buffer_count);
		}
	}
}

void Leaf::Merge(ART &art, Node &l_node, Node &r_node) {

	D_ASSERT(l_node.HasMetadata() && r_node.HasMetadata());

	// copy inlined row ID of r_node
	if (r_node.GetType() == NType::LEAF_INLINED) {
		Insert(art, l_node, r_node.GetRowId());
		r_node.Clear();
		return;
	}

	// l_node has an inlined row ID, swap and insert
	if (l_node.GetType() == NType::LEAF_INLINED) {
		auto row_id = l_node.GetRowId();
		l_node = r_node;
		Insert(art, l_node, row_id);
		r_node.Clear();
		return;
	}

	D_ASSERT(l_node.GetType() != NType::LEAF_INLINED);
	D_ASSERT(r_node.GetType() != NType::LEAF_INLINED);

	reference<Node> l_node_ref(l_node);
	reference<Leaf> l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);

	// find a non-full node
	while (l_leaf.get().count == Node::LEAF_SIZE) {
		l_node_ref = l_leaf.get().ptr;

		// the last leaf is full
		if (!l_leaf.get().ptr.HasMetadata()) {
			break;
		}
		l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);
	}

	// store the last leaf and then append r_node
	auto last_leaf_node = l_node_ref.get();
	l_node_ref.get() = r_node;
	r_node.Clear();

	// append the remaining row IDs of the last leaf node
	if (last_leaf_node.HasMetadata()) {
		// find the tail
		l_leaf = Node::RefMutable<Leaf>(art, l_node_ref, NType::LEAF);
		while (l_leaf.get().ptr.HasMetadata()) {
			l_leaf = Node::RefMutable<Leaf>(art, l_leaf.get().ptr, NType::LEAF);
		}
		// append the row IDs
		auto &last_leaf = Node::RefMutable<Leaf>(art, last_leaf_node, NType::LEAF);
		for (idx_t i = 0; i < last_leaf.count; i++) {
			l_leaf = l_leaf.get().Append(art, last_leaf.row_ids[i]);
		}
		Node::GetAllocator(art, NType::LEAF).Free(last_leaf_node);
	}
}

void Leaf::Insert(ART &art, Node &node, const row_t row_id) {

	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		MoveInlinedToLeaf(art, node);
		Insert(art, node, row_id);
		return;
	}

	// append to the tail
	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
	while (leaf.get().ptr.HasMetadata()) {
		leaf = Node::RefMutable<Leaf>(art, leaf.get().ptr, NType::LEAF);
	}
	leaf.get().Append(art, row_id);
}

bool Leaf::Remove(ART &art, reference<Node> &node, const row_t row_id) {

	D_ASSERT(node.get().HasMetadata());

	if (node.get().GetType() == NType::LEAF_INLINED) {
		if (node.get().GetRowId() == row_id) {
			return true;
		}
		return false;
	}

	reference<Leaf> leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);

	// inline the remaining row ID
	if (leaf.get().count == 2) {
		if (leaf.get().row_ids[0] == row_id || leaf.get().row_ids[1] == row_id) {
			auto remaining_row_id = leaf.get().row_ids[0] == row_id ? leaf.get().row_ids[1] : leaf.get().row_ids[0];
			Node::Free(art, node);
			New(node, remaining_row_id);
		}
		return false;
	}

	// get the last row ID (the order within a leaf does not matter)
	// because we want to overwrite the row ID to remove with that one

	// go to the tail and keep track of the previous leaf node
	reference<Leaf> prev_leaf(leaf);
	while (leaf.get().ptr.HasMetadata()) {
		prev_leaf = leaf;
		leaf = Node::RefMutable<Leaf>(art, leaf.get().ptr, NType::LEAF);
	}

	auto last_idx = leaf.get().count;
	auto last_row_id = leaf.get().row_ids[last_idx - 1];

	// only one row ID in this leaf segment, free it
	if (leaf.get().count == 1) {
		Node::Free(art, prev_leaf.get().ptr);
		if (last_row_id == row_id) {
			return false;
		}
	} else {
		leaf.get().count--;
	}

	// find the row ID and copy the last row ID to that position
	while (node.get().HasMetadata()) {
		leaf = Node::RefMutable<Leaf>(art, node, NType::LEAF);
		for (idx_t i = 0; i < leaf.get().count; i++) {
			if (leaf.get().row_ids[i] == row_id) {
				leaf.get().row_ids[i] = last_row_id;
				return false;
			}
		}
		node = leaf.get().ptr;
	}
	return false;
}

idx_t Leaf::TotalCount(ART &art, const Node &node) {

	D_ASSERT(node.HasMetadata());
	if (node.GetType() == NType::LEAF_INLINED) {
		return 1;
	}

	idx_t count = 0;
	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, node_ref, NType::LEAF);
		count += leaf.count;
		node_ref = leaf.ptr;
	}
	return count;
}

bool Leaf::GetRowIds(ART &art, const Node &node, vector<row_t> &result_ids, idx_t max_count) {

	// adding more elements would exceed the maximum count
	D_ASSERT(node.HasMetadata());
	if (result_ids.size() + TotalCount(art, node) > max_count) {
		return false;
	}

	if (node.GetType() == NType::LEAF_INLINED) {
		// push back the inlined row ID of this leaf
		result_ids.push_back(node.GetRowId());

	} else {
		// push back all the row IDs of this leaf
		reference<const Node> last_leaf_ref(node);
		while (last_leaf_ref.get().HasMetadata()) {
			auto &leaf = Node::Ref<const Leaf>(art, last_leaf_ref, NType::LEAF);
			for (idx_t i = 0; i < leaf.count; i++) {
				result_ids.push_back(leaf.row_ids[i]);
			}
			last_leaf_ref = leaf.ptr;
		}
	}

	return true;
}

bool Leaf::ContainsRowId(ART &art, const Node &node, const row_t row_id) {

	D_ASSERT(node.HasMetadata());

	if (node.GetType() == NType::LEAF_INLINED) {
		return node.GetRowId() == row_id;
	}

	reference<const Node> ref_node(node);
	while (ref_node.get().HasMetadata()) {
		auto &leaf = Node::Ref<const Leaf>(art, ref_node, NType::LEAF);
		for (idx_t i = 0; i < leaf.count; i++) {
			if (leaf.row_ids[i] == row_id) {
				return true;
			}
		}
		ref_node = leaf.ptr;
	}

	return false;
}

string Leaf::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {

	if (node.GetType() == NType::LEAF_INLINED) {
		return only_verify ? "" : "Leaf [count: 1, row ID: " + to_string(node.GetRowId()) + "]";
	}

	string str = "";

	reference<const Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {

		auto &leaf = Node::Ref<const Leaf>(art, node_ref, NType::LEAF);
		D_ASSERT(leaf.count <= Node::LEAF_SIZE);

		str += "Leaf [count: " + to_string(leaf.count) + ", row IDs: ";
		for (idx_t i = 0; i < leaf.count; i++) {
			str += to_string(leaf.row_ids[i]) + "-";
		}
		str += "] ";

		node_ref = leaf.ptr;
	}
	return only_verify ? "" : str;
}

void Leaf::Vacuum(ART &art, Node &node) {

	auto &allocator = Node::GetAllocator(art, NType::LEAF);

	reference<Node> node_ref(node);
	while (node_ref.get().HasMetadata()) {
		if (allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(NType::LEAF));
		}
		auto &leaf = Node::RefMutable<Leaf>(art, node_ref, NType::LEAF);
		node_ref = leaf.ptr;
	}
}

void Leaf::MoveInlinedToLeaf(ART &art, Node &node) {

	D_ASSERT(node.GetType() == NType::LEAF_INLINED);
	auto row_id = node.GetRowId();
	auto &leaf = New(art, node);

	leaf.count = 1;
	leaf.row_ids[0] = row_id;
}

Leaf &Leaf::Append(ART &art, const row_t row_id) {

	reference<Leaf> leaf(*this);

	// we need a new leaf node
	if (leaf.get().count == Node::LEAF_SIZE) {
		leaf = New(art, leaf.get().ptr);
	}

	leaf.get().row_ids[leaf.get().count] = row_id;
	leaf.get().count++;
	return leaf.get();
}

} // namespace duckdb













namespace duckdb {

//===--------------------------------------------------------------------===//
// New / Free
//===--------------------------------------------------------------------===//

void Node::New(ART &art, Node &node, const NType type) {

	// NOTE: leaves and prefixes should not pass through this function

	switch (type) {
	case NType::NODE_4:
		Node4::New(art, node);
		break;
	case NType::NODE_16:
		Node16::New(art, node);
		break;
	case NType::NODE_48:
		Node48::New(art, node);
		break;
	case NType::NODE_256:
		Node256::New(art, node);
		break;
	default:
		throw InternalException("Invalid node type for New.");
	}
}

void Node::Free(ART &art, Node &node) {

	if (!node.HasMetadata()) {
		return node.Clear();
	}

	// free the children of the nodes
	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX:
		// iterative
		return Prefix::Free(art, node);
	case NType::LEAF:
		// iterative
		return Leaf::Free(art, node);
	case NType::NODE_4:
		Node4::Free(art, node);
		break;
	case NType::NODE_16:
		Node16::Free(art, node);
		break;
	case NType::NODE_48:
		Node48::Free(art, node);
		break;
	case NType::NODE_256:
		Node256::Free(art, node);
		break;
	case NType::LEAF_INLINED:
		return node.Clear();
	}

	GetAllocator(art, type).Free(node);
	node.Clear();
}

//===--------------------------------------------------------------------===//
// Get Allocators
//===--------------------------------------------------------------------===//

FixedSizeAllocator &Node::GetAllocator(const ART &art, const NType type) {
	return *(*art.allocators)[static_cast<uint8_t>(type) - 1];
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void Node::ReplaceChild(const ART &art, const uint8_t byte, const Node child) const {

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).ReplaceChild(byte, child);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).ReplaceChild(byte, child);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).ReplaceChild(byte, child);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).ReplaceChild(byte, child);
	default:
		throw InternalException("Invalid node type for ReplaceChild.");
	}
}

void Node::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	switch (node.GetType()) {
	case NType::NODE_4:
		return Node4::InsertChild(art, node, byte, child);
	case NType::NODE_16:
		return Node16::InsertChild(art, node, byte, child);
	case NType::NODE_48:
		return Node48::InsertChild(art, node, byte, child);
	case NType::NODE_256:
		return Node256::InsertChild(art, node, byte, child);
	default:
		throw InternalException("Invalid node type for InsertChild.");
	}
}

//===--------------------------------------------------------------------===//
// Deletes
//===--------------------------------------------------------------------===//

void Node::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {

	switch (node.GetType()) {
	case NType::NODE_4:
		return Node4::DeleteChild(art, node, prefix, byte);
	case NType::NODE_16:
		return Node16::DeleteChild(art, node, byte);
	case NType::NODE_48:
		return Node48::DeleteChild(art, node, byte);
	case NType::NODE_256:
		return Node256::DeleteChild(art, node, byte);
	default:
		throw InternalException("Invalid node type for DeleteChild.");
	}
}

//===--------------------------------------------------------------------===//
// Get functions
//===--------------------------------------------------------------------===//

optional_ptr<const Node> Node::GetChild(ART &art, const uint8_t byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return Ref<const Node4>(art, *this, NType::NODE_4).GetChild(byte);
	case NType::NODE_16:
		return Ref<const Node16>(art, *this, NType::NODE_16).GetChild(byte);
	case NType::NODE_48:
		return Ref<const Node48>(art, *this, NType::NODE_48).GetChild(byte);
	case NType::NODE_256:
		return Ref<const Node256>(art, *this, NType::NODE_256).GetChild(byte);
	default:
		throw InternalException("Invalid node type for GetChild.");
	}
}

optional_ptr<Node> Node::GetChildMutable(ART &art, const uint8_t byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).GetChildMutable(byte);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).GetChildMutable(byte);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).GetChildMutable(byte);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).GetChildMutable(byte);
	default:
		throw InternalException("Invalid node type for GetChildMutable.");
	}
}

optional_ptr<const Node> Node::GetNextChild(ART &art, uint8_t &byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return Ref<const Node4>(art, *this, NType::NODE_4).GetNextChild(byte);
	case NType::NODE_16:
		return Ref<const Node16>(art, *this, NType::NODE_16).GetNextChild(byte);
	case NType::NODE_48:
		return Ref<const Node48>(art, *this, NType::NODE_48).GetNextChild(byte);
	case NType::NODE_256:
		return Ref<const Node256>(art, *this, NType::NODE_256).GetNextChild(byte);
	default:
		throw InternalException("Invalid node type for GetNextChild.");
	}
}

optional_ptr<Node> Node::GetNextChildMutable(ART &art, uint8_t &byte) const {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).GetNextChildMutable(byte);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).GetNextChildMutable(byte);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).GetNextChildMutable(byte);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).GetNextChildMutable(byte);
	default:
		throw InternalException("Invalid node type for GetNextChildMutable.");
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

string Node::VerifyAndToString(ART &art, const bool only_verify) const {

	D_ASSERT(HasMetadata());

	if (GetType() == NType::LEAF || GetType() == NType::LEAF_INLINED) {
		auto str = Leaf::VerifyAndToString(art, *this, only_verify);
		return only_verify ? "" : "\n" + str;
	}
	if (GetType() == NType::PREFIX) {
		auto str = Prefix::VerifyAndToString(art, *this, only_verify);
		return only_verify ? "" : "\n" + str;
	}

	string str = "Node" + to_string(GetCapacity()) + ": [";
	uint8_t byte = 0;
	auto child = GetNextChild(art, byte);

	while (child) {
		str += "(" + to_string(byte) + ", " + child->VerifyAndToString(art, only_verify) + ")";
		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}

		byte++;
		child = GetNextChild(art, byte);
	}

	return only_verify ? "" : "\n" + str + "]";
}

idx_t Node::GetCapacity() const {

	switch (GetType()) {
	case NType::NODE_4:
		return NODE_4_CAPACITY;
	case NType::NODE_16:
		return NODE_16_CAPACITY;
	case NType::NODE_48:
		return NODE_48_CAPACITY;
	case NType::NODE_256:
		return NODE_256_CAPACITY;
	default:
		throw InternalException("Invalid node type for GetCapacity.");
	}
}

NType Node::GetARTNodeTypeByCount(const idx_t count) {

	if (count <= NODE_4_CAPACITY) {
		return NType::NODE_4;
	} else if (count <= NODE_16_CAPACITY) {
		return NType::NODE_16;
	} else if (count <= NODE_48_CAPACITY) {
		return NType::NODE_48;
	}
	return NType::NODE_256;
}

//===--------------------------------------------------------------------===//
// Merging
//===--------------------------------------------------------------------===//

void Node::InitializeMerge(ART &art, const ARTFlags &flags) {

	D_ASSERT(HasMetadata());

	switch (GetType()) {
	case NType::PREFIX:
		// iterative
		return Prefix::InitializeMerge(art, *this, flags);
	case NType::LEAF:
		// iterative
		return Leaf::InitializeMerge(art, *this, flags);
	case NType::NODE_4:
		RefMutable<Node4>(art, *this, NType::NODE_4).InitializeMerge(art, flags);
		break;
	case NType::NODE_16:
		RefMutable<Node16>(art, *this, NType::NODE_16).InitializeMerge(art, flags);
		break;
	case NType::NODE_48:
		RefMutable<Node48>(art, *this, NType::NODE_48).InitializeMerge(art, flags);
		break;
	case NType::NODE_256:
		RefMutable<Node256>(art, *this, NType::NODE_256).InitializeMerge(art, flags);
		break;
	case NType::LEAF_INLINED:
		return;
	}

	IncreaseBufferId(flags.merge_buffer_counts[static_cast<uint8_t>(GetType()) - 1]);
}

bool Node::Merge(ART &art, Node &other) {

	if (!HasMetadata()) {
		*this = other;
		other = Node();
		return true;
	}

	return ResolvePrefixes(art, other);
}

bool MergePrefixContainsOtherPrefix(ART &art, reference<Node> &l_node, reference<Node> &r_node,
                                    idx_t &mismatch_position) {

	// r_node's prefix contains l_node's prefix
	// l_node cannot be a leaf, otherwise the key represented by l_node would be a subset of another key
	// which is not possible by our construction
	D_ASSERT(l_node.get().GetType() != NType::LEAF && l_node.get().GetType() != NType::LEAF_INLINED);

	// test if the next byte (mismatch_position) in r_node (prefix) exists in l_node
	auto mismatch_byte = Prefix::GetByte(art, r_node, mismatch_position);
	auto child_node = l_node.get().GetChildMutable(art, mismatch_byte);

	// update the prefix of r_node to only consist of the bytes after mismatch_position
	Prefix::Reduce(art, r_node, mismatch_position);

	if (!child_node) {
		// insert r_node as a child of l_node at the empty position
		Node::InsertChild(art, l_node, mismatch_byte, r_node);
		r_node.get().Clear();
		return true;
	}

	// recurse
	return child_node->ResolvePrefixes(art, r_node);
}

void MergePrefixesDiffer(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position) {

	// create a new node and insert both nodes as children

	Node l_child;
	auto l_byte = Prefix::GetByte(art, l_node, mismatch_position);
	Prefix::Split(art, l_node, l_child, mismatch_position);
	Node4::New(art, l_node);

	// insert children
	Node4::InsertChild(art, l_node, l_byte, l_child);
	auto r_byte = Prefix::GetByte(art, r_node, mismatch_position);
	Prefix::Reduce(art, r_node, mismatch_position);
	Node4::InsertChild(art, l_node, r_byte, r_node);

	r_node.get().Clear();
}

bool Node::ResolvePrefixes(ART &art, Node &other) {

	// NOTE: we always merge into the left ART

	D_ASSERT(HasMetadata() && other.HasMetadata());

	// case 1: both nodes have no prefix
	if (GetType() != NType::PREFIX && other.GetType() != NType::PREFIX) {
		return MergeInternal(art, other);
	}

	reference<Node> l_node(*this);
	reference<Node> r_node(other);

	idx_t mismatch_position = DConstants::INVALID_INDEX;

	// traverse prefixes
	if (l_node.get().GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {

		if (!Prefix::Traverse(art, l_node, r_node, mismatch_position)) {
			return false;
		}
		// we already recurse because the prefixes matched (so far)
		if (mismatch_position == DConstants::INVALID_INDEX) {
			return true;
		}

	} else {

		// l_prefix contains r_prefix
		if (l_node.get().GetType() == NType::PREFIX) {
			swap(*this, other);
		}
		mismatch_position = 0;
	}
	D_ASSERT(mismatch_position != DConstants::INVALID_INDEX);

	// case 2: one prefix contains the other prefix
	if (l_node.get().GetType() != NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
		return MergePrefixContainsOtherPrefix(art, l_node, r_node, mismatch_position);
	}

	// case 3: prefixes differ at a specific byte
	MergePrefixesDiffer(art, l_node, r_node, mismatch_position);
	return true;
}

bool Node::MergeInternal(ART &art, Node &other) {

	D_ASSERT(HasMetadata() && other.HasMetadata());
	D_ASSERT(GetType() != NType::PREFIX && other.GetType() != NType::PREFIX);

	// always try to merge the smaller node into the bigger node
	// because maybe there is enough free space in the bigger node to fit the smaller one
	// without too much recursion
	if (GetType() < other.GetType()) {
		swap(*this, other);
	}

	Node empty_node;
	auto &l_node = *this;
	auto &r_node = other;

	if (r_node.GetType() == NType::LEAF || r_node.GetType() == NType::LEAF_INLINED) {
		D_ASSERT(l_node.GetType() == NType::LEAF || l_node.GetType() == NType::LEAF_INLINED);

		if (art.IsUnique()) {
			return false;
		}

		Leaf::Merge(art, l_node, r_node);
		return true;
	}

	uint8_t byte = 0;
	auto r_child = r_node.GetNextChildMutable(art, byte);

	// while r_node still has children to merge
	while (r_child) {
		auto l_child = l_node.GetChildMutable(art, byte);
		if (!l_child) {
			// insert child at empty byte
			InsertChild(art, l_node, byte, *r_child);
			r_node.ReplaceChild(art, byte, empty_node);

		} else {
			// recurse
			if (!l_child->ResolvePrefixes(art, *r_child)) {
				return false;
			}
		}

		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		r_child = r_node.GetNextChildMutable(art, byte);
	}

	Free(art, r_node);
	return true;
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void Node::Vacuum(ART &art, const ARTFlags &flags) {

	D_ASSERT(HasMetadata());

	auto node_type = GetType();
	auto node_type_idx = static_cast<uint8_t>(node_type);

	// iterative functions
	if (node_type == NType::PREFIX) {
		return Prefix::Vacuum(art, *this, flags);
	}
	if (node_type == NType::LEAF_INLINED) {
		return;
	}
	if (node_type == NType::LEAF) {
		if (flags.vacuum_flags[node_type_idx - 1]) {
			Leaf::Vacuum(art, *this);
		}
		return;
	}

	auto &allocator = GetAllocator(art, node_type);
	auto needs_vacuum = flags.vacuum_flags[node_type_idx - 1] && allocator.NeedsVacuum(*this);
	if (needs_vacuum) {
		*this = allocator.VacuumPointer(*this);
		SetMetadata(node_type_idx);
	}

	// recursive functions
	switch (node_type) {
	case NType::NODE_4:
		return RefMutable<Node4>(art, *this, NType::NODE_4).Vacuum(art, flags);
	case NType::NODE_16:
		return RefMutable<Node16>(art, *this, NType::NODE_16).Vacuum(art, flags);
	case NType::NODE_48:
		return RefMutable<Node48>(art, *this, NType::NODE_48).Vacuum(art, flags);
	case NType::NODE_256:
		return RefMutable<Node256>(art, *this, NType::NODE_256).Vacuum(art, flags);
	default:
		throw InternalException("Invalid node type for Vacuum.");
	}
}

} // namespace duckdb





namespace duckdb {

Node16 &Node16::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_16).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_16));
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	n16.count = 0;
	return n16;
}

void Node16::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	// free all children
	for (idx_t i = 0; i < n16.count; i++) {
		Node::Free(art, n16.children[i]);
	}
}

Node16 &Node16::GrowNode4(ART &art, Node &node16, Node &node4) {

	auto &n4 = Node::RefMutable<Node4>(art, node4, NType::NODE_4);
	auto &n16 = New(art, node16);

	n16.count = n4.count;
	for (idx_t i = 0; i < n4.count; i++) {
		n16.key[i] = n4.key[i];
		n16.children[i] = n4.children[i];
	}

	n4.count = 0;
	Node::Free(art, node4);
	return n16;
}

Node16 &Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {

	auto &n16 = New(art, node16);
	auto &n48 = Node::RefMutable<Node48>(art, node48, NType::NODE_48);

	n16.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		D_ASSERT(n16.count <= Node::NODE_16_CAPACITY);
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n16.key[n16.count] = i;
			n16.children[n16.count] = n48.children[n48.child_index[i]];
			n16.count++;
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n16;
}

void Node16::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, flags);
	}
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n16.count; i++) {
		D_ASSERT(n16.key[i] != byte);
	}

	// insert new child node into node
	if (n16.count < Node::NODE_16_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = 0;
		while (child_pos < n16.count && n16.key[child_pos] < byte) {
			child_pos++;
		}
		// move children backwards to make space
		for (idx_t i = n16.count; i > child_pos; i--) {
			n16.key[i] = n16.key[i - 1];
			n16.children[i] = n16.children[i - 1];
		}

		n16.key[child_pos] = byte;
		n16.children[child_pos] = child;
		n16.count++;

	} else {
		// node is full, grow to Node48
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
	}
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n16 = Node::RefMutable<Node16>(art, node, NType::NODE_16);

	idx_t child_pos = 0;
	for (; child_pos < n16.count; child_pos++) {
		if (n16.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n16.count);

	// free the child and decrease the count
	Node::Free(art, n16.children[child_pos]);
	n16.count--;

	// potentially move any children backwards
	for (idx_t i = child_pos; i < n16.count; i++) {
		n16.key[i] = n16.key[i + 1];
		n16.children[i] = n16.children[i + 1];
	}

	// shrink node to Node4
	if (n16.count < Node::NODE_4_CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

void Node16::ReplaceChild(const uint8_t byte, const Node child) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			children[i] = child;
			return;
		}
	}
}

optional_ptr<const Node> Node16::GetChild(const uint8_t byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node16::GetChildMutable(const uint8_t byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<const Node> Node16::GetNextChild(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node16::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

void Node16::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].Vacuum(art, flags);
	}
}

} // namespace duckdb




namespace duckdb {

Node256 &Node256::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_256).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_256));
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	n256.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n256.children[i].Clear();
	}

	return n256;
}

void Node256::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	if (!n256.count) {
		return;
	}

	// free all children
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n256.children[i].HasMetadata()) {
			Node::Free(art, n256.children[i]);
		}
	}
}

Node256 &Node256::GrowNode48(ART &art, Node &node256, Node &node48) {

	auto &n48 = Node::RefMutable<Node48>(art, node48, NType::NODE_48);
	auto &n256 = New(art, node256);

	n256.count = n48.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			n256.children[i] = n48.children[n48.child_index[i]];
		} else {
			n256.children[i].Clear();
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n256;
}

void Node256::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			children[i].InitializeMerge(art, flags);
		}
	}
}

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	// ensure that there is no other child at the same byte
	D_ASSERT(!n256.children[byte].HasMetadata());

	n256.count++;
	D_ASSERT(n256.count <= Node::NODE_256_CAPACITY);
	n256.children[byte] = child;
}

void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256>(art, node, NType::NODE_256);

	// free the child and decrease the count
	Node::Free(art, n256.children[byte]);
	n256.count--;

	// shrink node to Node48
	if (n256.count <= Node::NODE_256_SHRINK_THRESHOLD) {
		auto node256 = node;
		Node48::ShrinkNode256(art, node, node256);
	}
}

optional_ptr<const Node> Node256::GetChild(const uint8_t byte) const {
	if (children[byte].HasMetadata()) {
		return &children[byte];
	}
	return nullptr;
}

optional_ptr<Node> Node256::GetChildMutable(const uint8_t byte) {
	if (children[byte].HasMetadata()) {
		return &children[byte];
	}
	return nullptr;
}

optional_ptr<const Node> Node256::GetNextChild(uint8_t &byte) const {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			byte = i;
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node256::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			byte = i;
			return &children[i];
		}
	}
	return nullptr;
}

void Node256::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (children[i].HasMetadata()) {
			children[i].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb





namespace duckdb {

Node4 &Node4::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_4).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_4));
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	n4.count = 0;
	return n4;
}

void Node4::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	// free all children
	for (idx_t i = 0; i < n4.count; i++) {
		Node::Free(art, n4.children[i]);
	}
}

Node4 &Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {

	auto &n4 = New(art, node4);
	auto &n16 = Node::RefMutable<Node16>(art, node16, NType::NODE_16);

	D_ASSERT(n16.count <= Node::NODE_4_CAPACITY);
	n4.count = n16.count;
	for (idx_t i = 0; i < n16.count; i++) {
		n4.key[i] = n16.key[i];
		n4.children[i] = n16.children[i];
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n4;
}

void Node4::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].InitializeMerge(art, flags);
	}
}

void Node4::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	// ensure that there is no other child at the same byte
	for (idx_t i = 0; i < n4.count; i++) {
		D_ASSERT(n4.key[i] != byte);
	}

	// insert new child node into node
	if (n4.count < Node::NODE_4_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = 0;
		while (child_pos < n4.count && n4.key[child_pos] < byte) {
			child_pos++;
		}
		// move children backwards to make space
		for (idx_t i = n4.count; i > child_pos; i--) {
			n4.key[i] = n4.key[i - 1];
			n4.children[i] = n4.children[i - 1];
		}

		n4.key[child_pos] = byte;
		n4.children[child_pos] = child;
		n4.count++;

	} else {
		// node is full, grow to Node16
		auto node4 = node;
		Node16::GrowNode4(art, node, node4);
		Node16::InsertChild(art, node, byte, child);
	}
}

void Node4::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n4 = Node::RefMutable<Node4>(art, node, NType::NODE_4);

	idx_t child_pos = 0;
	for (; child_pos < n4.count; child_pos++) {
		if (n4.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n4.count);
	D_ASSERT(n4.count > 1);

	// free the child and decrease the count
	Node::Free(art, n4.children[child_pos]);
	n4.count--;

	// potentially move any children backwards
	for (idx_t i = child_pos; i < n4.count; i++) {
		n4.key[i] = n4.key[i + 1];
		n4.children[i] = n4.children[i + 1];
	}

	// this is a one way node, compress
	if (n4.count == 1) {

		// we need to keep track of the old node pointer
		// because Concatenate() might overwrite that pointer while appending bytes to
		// the prefix (and by doing so overwriting the subsequent node with
		// new prefix nodes)
		auto old_n4_node = node;

		// get only child and concatenate prefixes
		auto child = *n4.GetChildMutable(n4.key[0]);
		Prefix::Concatenate(art, prefix, n4.key[0], child);

		n4.count--;
		Node::Free(art, old_n4_node);
	}
}

void Node4::ReplaceChild(const uint8_t byte, const Node child) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			children[i] = child;
			return;
		}
	}
}

optional_ptr<const Node> Node4::GetChild(const uint8_t byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node4::GetChildMutable(const uint8_t byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] == byte) {
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<const Node> Node4::GetNextChild(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node4::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			D_ASSERT(children[i].HasMetadata());
			return &children[i];
		}
	}
	return nullptr;
}

void Node4::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < count; i++) {
		children[i].Vacuum(art, flags);
	}
}

} // namespace duckdb





namespace duckdb {

Node48 &Node48::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::NODE_48).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_48));
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}
	for (idx_t i = 0; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	return n48;
}

void Node48::Free(ART &art, Node &node) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	if (!n48.count) {
		return;
	}

	// free all children
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (n48.child_index[i] != Node::EMPTY_MARKER) {
			Node::Free(art, n48.children[n48.child_index[i]]);
		}
	}
}

Node48 &Node48::GrowNode16(ART &art, Node &node48, Node &node16) {

	auto &n16 = Node::RefMutable<Node16>(art, node16, NType::NODE_16);
	auto &n48 = New(art, node48);

	n48.count = n16.count;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		n48.child_index[i] = Node::EMPTY_MARKER;
	}

	for (idx_t i = 0; i < n16.count; i++) {
		n48.child_index[n16.key[i]] = i;
		n48.children[i] = n16.children[i];
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n16.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n48;
}

Node48 &Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {

	auto &n48 = New(art, node48);
	auto &n256 = Node::RefMutable<Node256>(art, node256, NType::NODE_256);

	n48.count = 0;
	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		D_ASSERT(n48.count <= Node::NODE_48_CAPACITY);
		if (n256.children[i].HasMetadata()) {
			n48.child_index[i] = n48.count;
			n48.children[n48.count] = n256.children[i];
			n48.count++;
		} else {
			n48.child_index[i] = Node::EMPTY_MARKER;
		}
	}

	// necessary for faster child insertion/deletion
	for (idx_t i = n48.count; i < Node::NODE_48_CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n256.count = 0;
	Node::Free(art, node256);
	return n48;
}

void Node48::InitializeMerge(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			children[child_index[i]].InitializeMerge(art, flags);
		}
	}
}

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	// ensure that there is no other child at the same byte
	D_ASSERT(n48.child_index[byte] == Node::EMPTY_MARKER);

	// insert new child node into node
	if (n48.count < Node::NODE_48_CAPACITY) {
		// still space, just insert the child
		idx_t child_pos = n48.count;
		if (n48.children[child_pos].HasMetadata()) {
			// find an empty position in the node list if the current position is occupied
			child_pos = 0;
			while (n48.children[child_pos].HasMetadata()) {
				child_pos++;
			}
		}
		n48.children[child_pos] = child;
		n48.child_index[byte] = child_pos;
		n48.count++;

	} else {
		// node is full, grow to Node256
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
	}
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {

	D_ASSERT(node.HasMetadata());
	auto &n48 = Node::RefMutable<Node48>(art, node, NType::NODE_48);

	// free the child and decrease the count
	Node::Free(art, n48.children[n48.child_index[byte]]);
	n48.child_index[byte] = Node::EMPTY_MARKER;
	n48.count--;

	// shrink node to Node16
	if (n48.count < Node::NODE_48_SHRINK_THRESHOLD) {
		auto node48 = node;
		Node16::ShrinkNode48(art, node, node48);
	}
}

optional_ptr<const Node> Node48::GetChild(const uint8_t byte) const {
	if (child_index[byte] != Node::EMPTY_MARKER) {
		D_ASSERT(children[child_index[byte]].HasMetadata());
		return &children[child_index[byte]];
	}
	return nullptr;
}

optional_ptr<Node> Node48::GetChildMutable(const uint8_t byte) {
	if (child_index[byte] != Node::EMPTY_MARKER) {
		D_ASSERT(children[child_index[byte]].HasMetadata());
		return &children[child_index[byte]];
	}
	return nullptr;
}

optional_ptr<const Node> Node48::GetNextChild(uint8_t &byte) const {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			byte = i;
			D_ASSERT(children[child_index[i]].HasMetadata());
			return &children[child_index[i]];
		}
	}
	return nullptr;
}

optional_ptr<Node> Node48::GetNextChildMutable(uint8_t &byte) {
	for (idx_t i = byte; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			byte = i;
			D_ASSERT(children[child_index[i]].HasMetadata());
			return &children[child_index[i]];
		}
	}
	return nullptr;
}

void Node48::Vacuum(ART &art, const ARTFlags &flags) {

	for (idx_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (child_index[i] != Node::EMPTY_MARKER) {
			children[child_index[i]].Vacuum(art, flags);
		}
	}
}

} // namespace duckdb







namespace duckdb {

Prefix &Prefix::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(NType::PREFIX));

	auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);
	prefix.data[Node::PREFIX_SIZE] = 0;
	return prefix;
}

Prefix &Prefix::New(ART &art, Node &node, uint8_t byte, const Node &next) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(NType::PREFIX));

	auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);
	prefix.data[Node::PREFIX_SIZE] = 1;
	prefix.data[0] = byte;
	prefix.ptr = next;
	return prefix;
}

void Prefix::New(ART &art, reference<Node> &node, const ARTKey &key, const uint32_t depth, uint32_t count) {

	if (count == 0) {
		return;
	}
	idx_t copy_count = 0;

	while (count) {
		node.get() = Node::GetAllocator(art, NType::PREFIX).New();
		node.get().SetMetadata(static_cast<uint8_t>(NType::PREFIX));
		auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);

		auto this_count = MinValue((uint32_t)Node::PREFIX_SIZE, count);
		prefix.data[Node::PREFIX_SIZE] = (uint8_t)this_count;
		memcpy(prefix.data, key.data + depth + copy_count, this_count);

		node = prefix.ptr;
		copy_count += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {

	Node current_node = node;
	Node next_node;
	while (current_node.HasMetadata() && current_node.GetType() == NType::PREFIX) {
		next_node = Node::RefMutable<Prefix>(art, current_node, NType::PREFIX).ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(current_node);
		current_node = next_node;
	}

	Node::Free(art, current_node);
	node.Clear();
}

void Prefix::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::PREFIX) - 1];

	Node next_node = node;
	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, next_node, NType::PREFIX);

	while (next_node.GetType() == NType::PREFIX) {
		next_node = prefix.get().ptr;
		if (prefix.get().ptr.GetType() == NType::PREFIX) {
			prefix.get().ptr.IncreaseBufferId(merge_buffer_count);
			prefix = Node::RefMutable<Prefix>(art, next_node, NType::PREFIX);
		}
	}

	node.IncreaseBufferId(merge_buffer_count);
	prefix.get().ptr.InitializeMerge(art, flags);
}

void Prefix::Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node) {

	D_ASSERT(prefix_node.HasMetadata() && child_prefix_node.HasMetadata());

	// append a byte and a child_prefix to prefix
	if (prefix_node.GetType() == NType::PREFIX) {

		// get the tail
		reference<Prefix> prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);
		D_ASSERT(prefix.get().ptr.HasMetadata());

		while (prefix.get().ptr.GetType() == NType::PREFIX) {
			prefix = Node::RefMutable<Prefix>(art, prefix.get().ptr, NType::PREFIX);
			D_ASSERT(prefix.get().ptr.HasMetadata());
		}

		// append the byte
		prefix = prefix.get().Append(art, byte);

		if (child_prefix_node.GetType() == NType::PREFIX) {
			// append the child prefix
			prefix.get().Append(art, child_prefix_node);
		} else {
			// set child_prefix_node to succeed prefix
			prefix.get().ptr = child_prefix_node;
		}
		return;
	}

	// create a new prefix node containing the byte, then append the child_prefix to it
	if (prefix_node.GetType() != NType::PREFIX && child_prefix_node.GetType() == NType::PREFIX) {

		auto child_prefix = child_prefix_node;
		auto &prefix = New(art, prefix_node, byte);
		prefix.Append(art, child_prefix);
		return;
	}

	// neither prefix nor child_prefix are prefix nodes
	// create a new prefix containing the byte
	New(art, prefix_node, byte, child_prefix_node);
}

idx_t Prefix::Traverse(ART &art, reference<const Node> &prefix_node, const ARTKey &key, idx_t &depth) {

	D_ASSERT(prefix_node.get().HasMetadata());
	D_ASSERT(prefix_node.get().GetType() == NType::PREFIX);

	// compare prefix nodes to key bytes
	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(art, prefix_node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().HasMetadata());
	}

	return DConstants::INVALID_INDEX;
}

idx_t Prefix::TraverseMutable(ART &art, reference<Node> &prefix_node, const ARTKey &key, idx_t &depth) {

	D_ASSERT(prefix_node.get().HasMetadata());
	D_ASSERT(prefix_node.get().GetType() == NType::PREFIX);

	// compare prefix nodes to key bytes
	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().HasMetadata());
	}

	return DConstants::INVALID_INDEX;
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position) {

	auto &l_prefix = Node::RefMutable<Prefix>(art, l_node.get(), NType::PREFIX);
	auto &r_prefix = Node::RefMutable<Prefix>(art, r_node.get(), NType::PREFIX);

	// compare prefix bytes
	idx_t max_count = MinValue(l_prefix.data[Node::PREFIX_SIZE], r_prefix.data[Node::PREFIX_SIZE]);
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			mismatch_position = i;
			break;
		}
	}

	if (mismatch_position == DConstants::INVALID_INDEX) {

		// prefixes match (so far)
		if (l_prefix.data[Node::PREFIX_SIZE] == r_prefix.data[Node::PREFIX_SIZE]) {
			return l_prefix.ptr.ResolvePrefixes(art, r_prefix.ptr);
		}

		mismatch_position = max_count;

		// l_prefix contains r_prefix
		if (r_prefix.ptr.GetType() != NType::PREFIX && r_prefix.data[Node::PREFIX_SIZE] == max_count) {
			swap(l_node.get(), r_node.get());
			l_node = r_prefix.ptr;

		} else {
			// r_prefix contains l_prefix
			l_node = l_prefix.ptr;
		}
	}

	return true;
}

void Prefix::Reduce(ART &art, Node &prefix_node, const idx_t n) {

	D_ASSERT(prefix_node.HasMetadata());
	D_ASSERT(n < Node::PREFIX_SIZE);

	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);

	// free this prefix node
	if (n == (idx_t)(prefix.get().data[Node::PREFIX_SIZE] - 1)) {
		auto next_ptr = prefix.get().ptr;
		D_ASSERT(next_ptr.HasMetadata());
		prefix.get().ptr.Clear();
		Node::Free(art, prefix_node);
		prefix_node = next_ptr;
		return;
	}

	// shift by n bytes in the current prefix
	for (idx_t i = 0; i < Node::PREFIX_SIZE - n - 1; i++) {
		prefix.get().data[i] = prefix.get().data[n + i + 1];
	}
	D_ASSERT(n < (idx_t)(prefix.get().data[Node::PREFIX_SIZE] - 1));
	prefix.get().data[Node::PREFIX_SIZE] -= n + 1;

	// append the remaining prefix bytes
	prefix.get().Append(art, prefix.get().ptr);
}

void Prefix::Split(ART &art, reference<Node> &prefix_node, Node &child_node, idx_t position) {

	D_ASSERT(prefix_node.get().HasMetadata());

	auto &prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);

	// the split is at the last byte of this prefix, so the child_node contains all subsequent
	// prefix nodes (prefix.ptr) (if any), and the count of this prefix decreases by one,
	// then, we reference prefix.ptr, to overwrite it with a new node later
	if (position + 1 == Node::PREFIX_SIZE) {
		prefix.data[Node::PREFIX_SIZE]--;
		prefix_node = prefix.ptr;
		child_node = prefix.ptr;
		return;
	}

	// append the remaining bytes after the split
	if (position + 1 < prefix.data[Node::PREFIX_SIZE]) {
		reference<Prefix> child_prefix = New(art, child_node);
		for (idx_t i = position + 1; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			child_prefix = child_prefix.get().Append(art, prefix.data[i]);
		}

		D_ASSERT(prefix.ptr.HasMetadata());

		if (prefix.ptr.GetType() == NType::PREFIX) {
			child_prefix.get().Append(art, prefix.ptr);
		} else {
			// this is the last prefix node of the prefix
			child_prefix.get().ptr = prefix.ptr;
		}
	}

	// this is the last prefix node of the prefix
	if (position + 1 == prefix.data[Node::PREFIX_SIZE]) {
		child_node = prefix.ptr;
	}

	// set the new size of this node
	prefix.data[Node::PREFIX_SIZE] = position;

	// no bytes left before the split, free this node
	if (position == 0) {
		prefix.ptr.Clear();
		Node::Free(art, prefix_node.get());
		return;
	}

	// bytes left before the split, reference subsequent node
	prefix_node = prefix.ptr;
	return;
}

string Prefix::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {

	// NOTE: we could do this recursively, but the function-call overhead can become kinda crazy
	string str = "";

	reference<const Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {

		auto &prefix = Node::Ref<const Prefix>(art, node_ref, NType::PREFIX);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] != 0);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] <= Node::PREFIX_SIZE);

		str += " prefix_bytes:[";
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += "] ";

		node_ref = prefix.ptr;
	}

	auto subtree = node_ref.get().VerifyAndToString(art, only_verify);
	return only_verify ? "" : str + subtree;
}

void Prefix::Vacuum(ART &art, Node &node, const ARTFlags &flags) {

	bool flag_set = flags.vacuum_flags[static_cast<uint8_t>(NType::PREFIX) - 1];
	auto &allocator = Node::GetAllocator(art, NType::PREFIX);

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {
		if (flag_set && allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(NType::PREFIX));
		}
		auto &prefix = Node::RefMutable<Prefix>(art, node_ref, NType::PREFIX);
		node_ref = prefix.ptr;
	}

	node_ref.get().Vacuum(art, flags);
}

Prefix &Prefix::Append(ART &art, const uint8_t byte) {

	reference<Prefix> prefix(*this);

	// we need a new prefix node
	if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
		prefix = New(art, prefix.get().ptr);
	}

	prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
	prefix.get().data[Node::PREFIX_SIZE]++;
	return prefix.get();
}

void Prefix::Append(ART &art, Node other_prefix) {

	D_ASSERT(other_prefix.HasMetadata());

	reference<Prefix> prefix(*this);
	while (other_prefix.GetType() == NType::PREFIX) {

		// copy prefix bytes
		auto &other = Node::RefMutable<Prefix>(art, other_prefix, NType::PREFIX);
		for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
			prefix = prefix.get().Append(art, other.data[i]);
		}

		D_ASSERT(other.ptr.HasMetadata());

		prefix.get().ptr = other.ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(other_prefix);
		other_prefix = prefix.get().ptr;
	}

	D_ASSERT(prefix.get().ptr.GetType() != NType::PREFIX);
}

} // namespace duckdb




namespace duckdb {

FixedSizeAllocator::FixedSizeAllocator(const idx_t segment_size, BlockManager &block_manager)
    : block_manager(block_manager), buffer_manager(block_manager.buffer_manager),
      metadata_manager(block_manager.GetMetadataManager()), segment_size(segment_size), total_segment_count(0) {

	if (segment_size > Storage::BLOCK_SIZE - sizeof(validity_t)) {
		throw InternalException("The maximum segment size of fixed-size allocators is " +
		                        to_string(Storage::BLOCK_SIZE - sizeof(validity_t)));
	}

	// calculate how many segments fit into one buffer (available_segments_per_buffer)

	idx_t bits_per_value = sizeof(validity_t) * 8;
	idx_t byte_count = 0;

	bitmask_count = 0;
	available_segments_per_buffer = 0;

	while (byte_count < Storage::BLOCK_SIZE) {
		if (!bitmask_count || (bitmask_count * bits_per_value) % available_segments_per_buffer == 0) {
			// we need to add another validity_t value to the bitmask, to allow storing another
			// bits_per_value segments on a buffer
			bitmask_count++;
			byte_count += sizeof(validity_t);
		}

		auto remaining_bytes = Storage::BLOCK_SIZE - byte_count;
		auto remaining_segments = MinValue(remaining_bytes / segment_size, bits_per_value);

		if (remaining_segments == 0) {
			break;
		}

		available_segments_per_buffer += remaining_segments;
		byte_count += remaining_segments * segment_size;
	}

	bitmask_offset = bitmask_count * sizeof(validity_t);
}

IndexPointer FixedSizeAllocator::New() {

	// no more segments available
	if (buffers_with_free_space.empty()) {

		// add a new buffer
		auto buffer_id = GetAvailableBufferId();
		FixedSizeBuffer new_buffer(block_manager);
		buffers.insert(make_pair(buffer_id, std::move(new_buffer)));
		buffers_with_free_space.insert(buffer_id);

		// set the bitmask
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		auto &buffer = buffers.find(buffer_id)->second;
		ValidityMask mask(reinterpret_cast<validity_t *>(buffer.Get()));

		// zero-initialize the bitmask to avoid leaking memory to disk
		auto data = mask.GetData();
		for (idx_t i = 0; i < bitmask_count; i++) {
			data[i] = 0;
		}

		// initializing the bitmask of the new buffer
		mask.SetAllValid(available_segments_per_buffer);
	}

	// return a pointer to a free segment
	D_ASSERT(!buffers_with_free_space.empty());
	auto buffer_id = uint32_t(*buffers_with_free_space.begin());

	D_ASSERT(buffers.find(buffer_id) != buffers.end());
	auto &buffer = buffers.find(buffer_id)->second;
	auto offset = buffer.GetOffset(bitmask_count);

	total_segment_count++;
	buffer.segment_count++;
	if (buffer.segment_count == available_segments_per_buffer) {
		buffers_with_free_space.erase(buffer_id);
	}

	// zero-initialize that segment
	auto buffer_ptr = buffer.Get();
	auto offset_in_buffer = buffer_ptr + offset * segment_size + bitmask_offset;
	memset(offset_in_buffer, 0, segment_size);

	return IndexPointer(buffer_id, offset);
}

void FixedSizeAllocator::Free(const IndexPointer ptr) {

	auto buffer_id = ptr.GetBufferId();
	auto offset = ptr.GetOffset();

	D_ASSERT(buffers.find(buffer_id) != buffers.end());
	auto &buffer = buffers.find(buffer_id)->second;

	auto bitmask_ptr = reinterpret_cast<validity_t *>(buffer.Get());
	ValidityMask mask(bitmask_ptr);
	D_ASSERT(!mask.RowIsValid(offset));
	mask.SetValid(offset);

	D_ASSERT(total_segment_count > 0);
	D_ASSERT(buffer.segment_count > 0);

	// adjust the allocator fields
	buffers_with_free_space.insert(buffer_id);
	total_segment_count--;
	buffer.segment_count--;
}

void FixedSizeAllocator::Reset() {
	for (auto &buffer : buffers) {
		buffer.second.Destroy();
	}
	buffers.clear();
	buffers_with_free_space.clear();
	total_segment_count = 0;
}

idx_t FixedSizeAllocator::GetMemoryUsage() const {
	idx_t memory_usage = 0;
	for (auto &buffer : buffers) {
		if (buffer.second.InMemory()) {
			memory_usage += Storage::BLOCK_SIZE;
		}
	}
	return memory_usage;
}

idx_t FixedSizeAllocator::GetUpperBoundBufferId() const {
	idx_t upper_bound_id = 0;
	for (auto &buffer : buffers) {
		if (buffer.first >= upper_bound_id) {
			upper_bound_id = buffer.first + 1;
		}
	}
	return upper_bound_id;
}

void FixedSizeAllocator::Merge(FixedSizeAllocator &other) {

	D_ASSERT(segment_size == other.segment_size);

	// remember the buffer count and merge the buffers
	idx_t upper_bound_id = GetUpperBoundBufferId();
	for (auto &buffer : other.buffers) {
		buffers.insert(make_pair(buffer.first + upper_bound_id, std::move(buffer.second)));
	}
	other.buffers.clear();

	// merge the buffers with free spaces
	for (auto &buffer_id : other.buffers_with_free_space) {
		buffers_with_free_space.insert(buffer_id + upper_bound_id);
	}
	other.buffers_with_free_space.clear();

	// add the total allocations
	total_segment_count += other.total_segment_count;
}

bool FixedSizeAllocator::InitializeVacuum() {

	// NOTE: we do not vacuum buffers that are not in memory. We might consider changing this
	// in the future, although buffers on disk should almost never be eligible for a vacuum

	if (total_segment_count == 0) {
		Reset();
		return false;
	}

	// remove all empty buffers
	auto buffer_it = buffers.begin();
	while (buffer_it != buffers.end()) {
		if (!buffer_it->second.segment_count) {
			buffers_with_free_space.erase(buffer_it->first);
			buffer_it->second.Destroy();
			buffer_it = buffers.erase(buffer_it);
		} else {
			buffer_it++;
		}
	}

	// determine if a vacuum is necessary
	multimap<idx_t, idx_t> temporary_vacuum_buffers;
	D_ASSERT(vacuum_buffers.empty());
	idx_t available_segments_in_memory = 0;

	for (auto &buffer : buffers) {
		buffer.second.vacuum = false;
		if (buffer.second.InMemory()) {
			auto available_segments_in_buffer = available_segments_per_buffer - buffer.second.segment_count;
			available_segments_in_memory += available_segments_in_buffer;
			temporary_vacuum_buffers.emplace(available_segments_in_buffer, buffer.first);
		}
	}

	// no buffers in memory
	if (temporary_vacuum_buffers.empty()) {
		return false;
	}

	auto excess_buffer_count = available_segments_in_memory / available_segments_per_buffer;

	// calculate the vacuum threshold adaptively
	D_ASSERT(excess_buffer_count < temporary_vacuum_buffers.size());
	idx_t memory_usage = GetMemoryUsage();
	idx_t excess_memory_usage = excess_buffer_count * Storage::BLOCK_SIZE;
	auto excess_percentage = double(excess_memory_usage) / double(memory_usage);
	auto threshold = double(VACUUM_THRESHOLD) / 100.0;
	if (excess_percentage < threshold) {
		return false;
	}

	D_ASSERT(excess_buffer_count <= temporary_vacuum_buffers.size());
	D_ASSERT(temporary_vacuum_buffers.size() <= buffers.size());

	// erasing from a multimap, we vacuum the buffers with the most free spaces (least full)
	while (temporary_vacuum_buffers.size() != excess_buffer_count) {
		temporary_vacuum_buffers.erase(temporary_vacuum_buffers.begin());
	}

	// adjust the buffers, and erase all to-be-vacuumed buffers from the available buffer list
	for (auto &vacuum_buffer : temporary_vacuum_buffers) {
		auto buffer_id = vacuum_buffer.second;
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		buffers.find(buffer_id)->second.vacuum = true;
		buffers_with_free_space.erase(buffer_id);
	}

	for (auto &vacuum_buffer : temporary_vacuum_buffers) {
		vacuum_buffers.insert(vacuum_buffer.second);
	}

	return true;
}

void FixedSizeAllocator::FinalizeVacuum() {

	for (auto &buffer_id : vacuum_buffers) {
		D_ASSERT(buffers.find(buffer_id) != buffers.end());
		auto &buffer = buffers.find(buffer_id)->second;
		D_ASSERT(buffer.InMemory());
		buffer.Destroy();
		buffers.erase(buffer_id);
	}
	vacuum_buffers.clear();
}

IndexPointer FixedSizeAllocator::VacuumPointer(const IndexPointer ptr) {

	// we do not need to adjust the bitmask of the old buffer, because we will free the entire
	// buffer after the vacuum operation

	auto new_ptr = New();
	// new increases the allocation count, we need to counter that here
	total_segment_count--;

	memcpy(Get(new_ptr), Get(ptr), segment_size);
	return new_ptr;
}

BlockPointer FixedSizeAllocator::Serialize(PartialBlockManager &partial_block_manager, MetadataWriter &writer) {

	for (auto &buffer : buffers) {
		buffer.second.Serialize(partial_block_manager, available_segments_per_buffer, segment_size, bitmask_offset);
	}

	auto block_pointer = writer.GetBlockPointer();
	writer.Write(segment_size);
	writer.Write(static_cast<idx_t>(buffers.size()));
	writer.Write(static_cast<idx_t>(buffers_with_free_space.size()));

	for (auto &buffer : buffers) {
		writer.Write(buffer.first);
		writer.Write(buffer.second.block_pointer);
		writer.Write(buffer.second.segment_count);
		writer.Write(buffer.second.allocation_size);
	}
	for (auto &buffer_id : buffers_with_free_space) {
		writer.Write(buffer_id);
	}

	return block_pointer;
}

void FixedSizeAllocator::Deserialize(const BlockPointer &block_pointer) {

	MetadataReader reader(metadata_manager, block_pointer);
	segment_size = reader.Read<idx_t>();
	auto buffer_count = reader.Read<idx_t>();
	auto buffers_with_free_space_count = reader.Read<idx_t>();

	total_segment_count = 0;

	for (idx_t i = 0; i < buffer_count; i++) {
		auto buffer_id = reader.Read<idx_t>();
		auto buffer_block_pointer = reader.Read<BlockPointer>();
		auto segment_count = reader.Read<idx_t>();
		auto allocation_size = reader.Read<idx_t>();
		FixedSizeBuffer new_buffer(block_manager, segment_count, allocation_size, buffer_block_pointer);
		buffers.insert(make_pair(buffer_id, std::move(new_buffer)));
		total_segment_count += segment_count;
	}
	for (idx_t i = 0; i < buffers_with_free_space_count; i++) {
		buffers_with_free_space.insert(reader.Read<idx_t>());
	}
}

idx_t FixedSizeAllocator::GetAvailableBufferId() const {
	idx_t buffer_id = buffers.size();
	while (buffers.find(buffer_id) != buffers.end()) {
		D_ASSERT(buffer_id > 0);
		buffer_id--;
	}
	return buffer_id;
}

} // namespace duckdb





namespace duckdb {

//===--------------------------------------------------------------------===//
// PartialBlockForIndex
//===--------------------------------------------------------------------===//

PartialBlockForIndex::PartialBlockForIndex(PartialBlockState state, BlockManager &block_manager,
                                           const shared_ptr<BlockHandle> &block_handle)
    : PartialBlock(state, block_manager, block_handle) {
}

void PartialBlockForIndex::Flush(const idx_t free_space_left) {
	FlushInternal(free_space_left);
	block_handle = block_manager.ConvertToPersistent(state.block_id, std::move(block_handle));
	Clear();
}

void PartialBlockForIndex::Merge(PartialBlock &other, idx_t offset, idx_t other_size) {
	throw InternalException("no merge for PartialBlockForIndex");
}

void PartialBlockForIndex::Clear() {
	block_handle.reset();
}

//===--------------------------------------------------------------------===//
// FixedSizeBuffer
//===--------------------------------------------------------------------===//

constexpr idx_t FixedSizeBuffer::BASE[];
constexpr uint8_t FixedSizeBuffer::SHIFT[];

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager)
    : block_manager(block_manager), segment_count(0), allocation_size(0), dirty(false), vacuum(false), block_pointer(),
      block_handle(nullptr) {

	auto &buffer_manager = block_manager.buffer_manager;
	buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &block_handle);
}

FixedSizeBuffer::FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const idx_t allocation_size,
                                 const BlockPointer &block_pointer)
    : block_manager(block_manager), segment_count(segment_count), allocation_size(allocation_size), dirty(false),
      vacuum(false), block_pointer(block_pointer) {

	D_ASSERT(block_pointer.IsValid());
	block_handle = block_manager.RegisterBlock(block_pointer.block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);
}

void FixedSizeBuffer::Destroy() {
	if (InMemory()) {
		// we can have multiple readers on a pinned block, and unpinning the buffer handle
		// decrements the reader count on the underlying block handle (Destroy() unpins)
		buffer_handle.Destroy();
	}
	if (OnDisk()) {
		// marking a block as modified decreases the reference count of multi-use blocks
		block_manager.MarkBlockAsModified(block_pointer.block_id);
	}
}

void FixedSizeBuffer::Serialize(PartialBlockManager &partial_block_manager, const idx_t available_segments,
                                const idx_t segment_size, const idx_t bitmask_offset) {

	// we do not serialize a block that is already on disk and not in memory
	if (!InMemory()) {
		if (!OnDisk() || dirty) {
			throw InternalException("invalid or missing buffer in FixedSizeAllocator");
		}
		return;
	}

	// we do not serialize a block that is already on disk and not dirty
	if (!dirty && OnDisk()) {
		return;
	}

	if (dirty) {
		// the allocation possibly changed
		auto max_offset = GetMaxOffset(available_segments);
		allocation_size = max_offset * segment_size + bitmask_offset;
	}

	// the buffer is in memory, so we copied it onto a new buffer when pinning
	D_ASSERT(InMemory() && !OnDisk());

	// now we write the changes, first get a partial block allocation
	PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(allocation_size);
	block_pointer.block_id = allocation.state.block_id;
	block_pointer.offset = allocation.state.offset;

	auto &buffer_manager = block_manager.buffer_manager;

	if (allocation.partial_block) {
		// copy to an existing partial block
		D_ASSERT(block_pointer.offset > 0);
		auto &p_block_for_index = allocation.partial_block->Cast<PartialBlockForIndex>();
		auto dst_handle = buffer_manager.Pin(p_block_for_index.block_handle);
		memcpy(dst_handle.Ptr() + block_pointer.offset, buffer_handle.Ptr(), allocation_size);
		SetUninitializedRegions(p_block_for_index, segment_size, block_pointer.offset, bitmask_offset);

	} else {
		// create a new block that can potentially be used as a partial block
		D_ASSERT(block_handle);
		D_ASSERT(!block_pointer.offset);
		auto p_block_for_index = make_uniq<PartialBlockForIndex>(allocation.state, block_manager, block_handle);
		SetUninitializedRegions(*p_block_for_index, segment_size, block_pointer.offset, bitmask_offset);
		allocation.partial_block = std::move(p_block_for_index);
	}

	partial_block_manager.RegisterPartialBlock(std::move(allocation));

	// resetting this buffer
	buffer_handle.Destroy();
	block_handle = block_manager.RegisterBlock(block_pointer.block_id);
	D_ASSERT(block_handle->BlockId() < MAXIMUM_BLOCK);

	// we persist any changes, so the buffer is no longer dirty
	dirty = false;
}

void FixedSizeBuffer::Pin() {

	auto &buffer_manager = block_manager.buffer_manager;
	D_ASSERT(block_pointer.IsValid());
	D_ASSERT(block_handle && block_handle->BlockId() < MAXIMUM_BLOCK);
	D_ASSERT(!dirty);

	buffer_handle = buffer_manager.Pin(block_handle);

	// we need to copy the (partial) data into a new (not yet disk-backed) buffer handle
	shared_ptr<BlockHandle> new_block_handle;
	auto new_buffer_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block_handle);

	memcpy(new_buffer_handle.Ptr(), buffer_handle.Ptr() + block_pointer.offset, allocation_size);

	Destroy();
	buffer_handle = std::move(new_buffer_handle);
	block_handle = new_block_handle;
	block_pointer = BlockPointer();
}

uint32_t FixedSizeBuffer::GetOffset(const idx_t bitmask_count) {

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);
	auto data = mask.GetData();

	// fills up a buffer sequentially before searching for free bits
	if (mask.RowIsValid(segment_count)) {
		mask.SetInvalid(segment_count);
		return segment_count;
	}

	for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
		// get an entry with free bits
		if (data[entry_idx] == 0) {
			continue;
		}

		// find the position of the free bit
		auto entry = data[entry_idx];
		idx_t first_valid_bit = 0;

		// this loop finds the position of the rightmost set bit in entry and stores it
		// in first_valid_bit
		for (idx_t i = 0; i < 6; i++) {
			// set the left half of the bits of this level to zero and test if the entry is still not zero
			if (entry & BASE[i]) {
				// first valid bit is in the rightmost s[i] bits
				// permanently set the left half of the bits to zero
				entry &= BASE[i];
			} else {
				// first valid bit is in the leftmost s[i] bits
				// shift by s[i] for the next iteration and add s[i] to the position of the rightmost set bit
				entry >>= SHIFT[i];
				first_valid_bit += SHIFT[i];
			}
		}
		D_ASSERT(entry);

		auto prev_bits = entry_idx * sizeof(validity_t) * 8;
		D_ASSERT(mask.RowIsValid(prev_bits + first_valid_bit));
		mask.SetInvalid(prev_bits + first_valid_bit);
		return (prev_bits + first_valid_bit);
	}

	throw InternalException("Invalid bitmask for FixedSizeAllocator");
}

uint32_t FixedSizeBuffer::GetMaxOffset(const idx_t available_segments) {

	// this function calls Get() on the buffer
	D_ASSERT(InMemory());

	// finds the maximum zero bit in a bitmask, and adds one to it,
	// so that max_offset * segment_size = allocated_size of this bitmask's buffer
	idx_t entry_size = sizeof(validity_t) * 8;
	idx_t bitmask_count = available_segments / entry_size;
	if (available_segments % entry_size != 0) {
		bitmask_count++;
	}
	uint32_t max_offset = bitmask_count * sizeof(validity_t) * 8;
	auto bits_in_last_entry = available_segments % (sizeof(validity_t) * 8);

	// get the bitmask data
	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	const ValidityMask mask(bitmask_ptr);
	const auto data = mask.GetData();

	D_ASSERT(bitmask_count > 0);
	for (idx_t i = bitmask_count; i > 0; i--) {

		auto entry = data[i - 1];

		// set all bits after bits_in_last_entry
		if (i == bitmask_count) {
			entry |= ~idx_t(0) << bits_in_last_entry;
		}

		if (entry == ~idx_t(0)) {
			max_offset -= sizeof(validity_t) * 8;
			continue;
		}

		// invert data[entry_idx]
		auto entry_inv = ~entry;
		idx_t first_valid_bit = 0;

		// then find the position of the LEFTMOST set bit
		for (idx_t level = 0; level < 6; level++) {

			// set the right half of the bits of this level to zero and test if the entry is still not zero
			if (entry_inv & ~BASE[level]) {
				// first valid bit is in the leftmost s[level] bits
				// shift by s[level] for the next iteration and add s[level] to the position of the leftmost set bit
				entry_inv >>= SHIFT[level];
				first_valid_bit += SHIFT[level];
			} else {
				// first valid bit is in the rightmost s[level] bits
				// permanently set the left half of the bits to zero
				entry_inv &= BASE[level];
			}
		}
		D_ASSERT(entry_inv);
		max_offset -= sizeof(validity_t) * 8 - first_valid_bit;
		D_ASSERT(!mask.RowIsValid(max_offset));
		return max_offset + 1;
	}

	// there are no allocations in this buffer
	throw InternalException("tried to serialize empty buffer");
}

void FixedSizeBuffer::SetUninitializedRegions(PartialBlockForIndex &p_block_for_index, const idx_t segment_size,
                                              const idx_t offset, const idx_t bitmask_offset) {

	// this function calls Get() on the buffer
	D_ASSERT(InMemory());

	auto bitmask_ptr = reinterpret_cast<validity_t *>(Get());
	ValidityMask mask(bitmask_ptr);

	idx_t i = 0;
	idx_t max_offset = offset + allocation_size;
	idx_t current_offset = offset + bitmask_offset;
	while (current_offset < max_offset) {

		if (mask.RowIsValid(i)) {
			D_ASSERT(current_offset + segment_size <= max_offset);
			p_block_for_index.AddUninitializedRegion(current_offset, current_offset + segment_size);
		}
		current_offset += segment_size;
		i++;
	}
}

} // namespace duckdb









namespace duckdb {

using ValidityBytes = JoinHashTable::ValidityBytes;
using ScanStructure = JoinHashTable::ScanStructure;
using ProbeSpill = JoinHashTable::ProbeSpill;
using ProbeSpillLocalState = JoinHashTable::ProbeSpillLocalAppendState;

JoinHashTable::JoinHashTable(BufferManager &buffer_manager_p, const vector<JoinCondition> &conditions_p,
                             vector<LogicalType> btypes, JoinType type_p)
    : buffer_manager(buffer_manager_p), conditions(conditions_p), build_types(std::move(btypes)), entry_size(0),
      tuple_size(0), vfound(Value::BOOLEAN(false)), join_type(type_p), finalized(false), has_null(false),
      external(false), radix_bits(4), partition_start(0), partition_end(0) {

	for (auto &condition : conditions) {
		D_ASSERT(condition.left->return_type == condition.right->return_type);
		auto type = condition.left->return_type;
		if (condition.comparison == ExpressionType::COMPARE_EQUAL ||
		    condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {

			// ensure that all equality conditions are at the front,
			// and that all other conditions are at the back
			D_ASSERT(equality_types.size() == condition_types.size());
			equality_types.push_back(type);
		}

		predicates.push_back(condition.comparison);
		null_values_are_equal.push_back(condition.comparison == ExpressionType::COMPARE_DISTINCT_FROM ||
		                                condition.comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		condition_types.push_back(type);
	}
	// at least one equality is necessary
	D_ASSERT(!equality_types.empty());

	// Types for the layout
	vector<LogicalType> layout_types(condition_types);
	layout_types.insert(layout_types.end(), build_types.begin(), build_types.end());
	if (IsRightOuterJoin(join_type)) {
		// full/right outer joins need an extra bool to keep track of whether or not a tuple has found a matching entry
		// we place the bool before the NEXT pointer
		layout_types.emplace_back(LogicalType::BOOLEAN);
	}
	layout_types.emplace_back(LogicalType::HASH);
	layout.Initialize(layout_types, false);
	row_matcher.Initialize(false, layout, predicates);
	row_matcher_no_match_sel.Initialize(true, layout, predicates);

	const auto &offsets = layout.GetOffsets();
	tuple_size = offsets[condition_types.size() + build_types.size()];
	pointer_offset = offsets.back();
	entry_size = layout.GetRowWidth();

	data_collection = make_uniq<TupleDataCollection>(buffer_manager, layout);
	sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
}

JoinHashTable::~JoinHashTable() {
}

void JoinHashTable::Merge(JoinHashTable &other) {
	{
		lock_guard<mutex> guard(data_lock);
		data_collection->Combine(*other.data_collection);
	}

	if (join_type == JoinType::MARK) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		has_null = has_null || other.has_null;
		if (!info.correlated_types.empty()) {
			auto &other_info = other.correlated_mark_join_info;
			info.correlated_counts->Combine(*other_info.correlated_counts);
		}
	}

	sink_collection->Combine(*other.sink_collection);
}

void JoinHashTable::ApplyBitmask(Vector &hashes, idx_t count) {
	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		D_ASSERT(!ConstantVector::IsNull(hashes));
		auto indices = ConstantVector::GetData<hash_t>(hashes);
		*indices = *indices & bitmask;
	} else {
		hashes.Flatten(count);
		auto indices = FlatVector::GetData<hash_t>(hashes);
		for (idx_t i = 0; i < count; i++) {
			indices[i] &= bitmask;
		}
	}
}

void JoinHashTable::ApplyBitmask(Vector &hashes, const SelectionVector &sel, idx_t count, Vector &pointers) {
	UnifiedVectorFormat hdata;
	hashes.ToUnifiedFormat(count, hdata);

	auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hdata);
	auto result_data = FlatVector::GetData<data_ptr_t *>(pointers);
	auto main_ht = reinterpret_cast<data_ptr_t *>(hash_map.get());
	for (idx_t i = 0; i < count; i++) {
		auto rindex = sel.get_index(i);
		auto hindex = hdata.sel->get_index(rindex);
		auto hash = hash_data[hindex];
		result_data[rindex] = main_ht + (hash & bitmask);
	}
}

void JoinHashTable::Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes) {
	if (count == keys.size()) {
		// no null values are filtered: use regular hash functions
		VectorOperations::Hash(keys.data[0], hashes, keys.size());
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], keys.size());
		}
	} else {
		// null values were filtered: use selection vector
		VectorOperations::Hash(keys.data[0], hashes, sel, count);
		for (idx_t i = 1; i < equality_types.size(); i++) {
			VectorOperations::CombineHash(hashes, keys.data[i], sel, count);
		}
	}
}

static idx_t FilterNullValues(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                              SelectionVector &result) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto key_idx = vdata.sel->get_index(idx);
		if (vdata.validity.RowIsValid(key_idx)) {
			result.set_index(result_count++, idx);
		}
	}
	return result_count;
}

void JoinHashTable::Build(PartitionedTupleDataAppendState &append_state, DataChunk &keys, DataChunk &payload) {
	D_ASSERT(!finalized);
	D_ASSERT(keys.size() == payload.size());
	if (keys.size() == 0) {
		return;
	}
	// special case: correlated mark join
	if (join_type == JoinType::MARK && !correlated_mark_join_info.correlated_types.empty()) {
		auto &info = correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);
		// Correlated MARK join
		// for the correlated mark join we need to keep track of COUNT(*) and COUNT(COLUMN) for each of the correlated
		// columns push into the aggregate hash table
		D_ASSERT(info.correlated_counts);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.correlated_types.size(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		if (info.correlated_payload.data.empty()) {
			vector<LogicalType> types;
			types.push_back(keys.data[info.correlated_types.size()].GetType());
			info.correlated_payload.InitializeEmpty(types);
		}
		info.correlated_payload.SetCardinality(keys);
		info.correlated_payload.data[0].Reference(keys.data[info.correlated_types.size()]);
		info.correlated_counts->AddChunk(info.group_chunk, info.correlated_payload, AggregateType::NON_DISTINCT);
	}

	// build a chunk to append to the data collection [keys, payload, (optional "found" boolean), hash]
	DataChunk source_chunk;
	source_chunk.InitializeEmpty(layout.GetTypes());
	for (idx_t i = 0; i < keys.ColumnCount(); i++) {
		source_chunk.data[i].Reference(keys.data[i]);
	}
	idx_t col_offset = keys.ColumnCount();
	D_ASSERT(build_types.size() == payload.ColumnCount());
	for (idx_t i = 0; i < payload.ColumnCount(); i++) {
		source_chunk.data[col_offset + i].Reference(payload.data[i]);
	}
	col_offset += payload.ColumnCount();
	if (IsRightOuterJoin(join_type)) {
		// for FULL/RIGHT OUTER joins initialize the "found" boolean to false
		source_chunk.data[col_offset].Reference(vfound);
		col_offset++;
	}
	Vector hash_values(LogicalType::HASH);
	source_chunk.data[col_offset].Reference(hash_values);
	source_chunk.SetCardinality(keys);

	// ToUnifiedFormat the source chunk
	TupleDataCollection::ToUnifiedFormat(append_state.chunk_state, source_chunk);

	// prepare the keys for processing
	const SelectionVector *current_sel;
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t added_count = PrepareKeys(keys, append_state.chunk_state.vector_data, current_sel, sel, true);
	if (added_count < keys.size()) {
		has_null = true;
	}
	if (added_count == 0) {
		return;
	}

	// hash the keys and obtain an entry in the list
	// note that we only hash the keys used in the equality comparison
	Hash(keys, *current_sel, added_count, hash_values);

	// Re-reference and ToUnifiedFormat the hash column after computing it
	source_chunk.data[col_offset].Reference(hash_values);
	hash_values.ToUnifiedFormat(source_chunk.size(), append_state.chunk_state.vector_data.back().unified);

	// We already called TupleDataCollection::ToUnifiedFormat, so we can AppendUnified here
	sink_collection->AppendUnified(append_state, source_chunk, *current_sel, added_count);
}

idx_t JoinHashTable::PrepareKeys(DataChunk &keys, vector<TupleDataVectorFormat> &vector_data,
                                 const SelectionVector *&current_sel, SelectionVector &sel, bool build_side) {
	// figure out which keys are NULL, and create a selection vector out of them
	current_sel = FlatVector::IncrementalSelectionVector();
	idx_t added_count = keys.size();
	if (build_side && IsRightOuterJoin(join_type)) {
		// in case of a right or full outer join, we cannot remove NULL keys from the build side
		return added_count;
	}

	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		if (!null_values_are_equal[col_idx]) {
			auto &col_key_data = vector_data[col_idx].unified;
			if (col_key_data.validity.AllValid()) {
				continue;
			}
			added_count = FilterNullValues(col_key_data, *current_sel, added_count, sel);
			// null values are NOT equal for this column, filter them out
			current_sel = &sel;
		}
	}
	return added_count;
}

template <bool PARALLEL>
static inline void InsertHashesLoop(atomic<data_ptr_t> pointers[], const hash_t indices[], const idx_t count,
                                    const data_ptr_t key_locations[], const idx_t pointer_offset) {
	for (idx_t i = 0; i < count; i++) {
		const auto index = indices[i];
		if (PARALLEL) {
			data_ptr_t head;
			do {
				head = pointers[index];
				Store<data_ptr_t>(head, key_locations[i] + pointer_offset);
			} while (!std::atomic_compare_exchange_weak(&pointers[index], &head, key_locations[i]));
		} else {
			// set prev in current key to the value (NOTE: this will be nullptr if there is none)
			Store<data_ptr_t>(pointers[index], key_locations[i] + pointer_offset);

			// set pointer to current tuple
			pointers[index] = key_locations[i];
		}
	}
}

void JoinHashTable::InsertHashes(Vector &hashes, idx_t count, data_ptr_t key_locations[], bool parallel) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);

	// use bitmask to get position in array
	ApplyBitmask(hashes, count);

	hashes.Flatten(count);
	D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);

	auto pointers = reinterpret_cast<atomic<data_ptr_t> *>(hash_map.get());
	auto indices = FlatVector::GetData<hash_t>(hashes);

	if (parallel) {
		InsertHashesLoop<true>(pointers, indices, count, key_locations, pointer_offset);
	} else {
		InsertHashesLoop<false>(pointers, indices, count, key_locations, pointer_offset);
	}
}

void JoinHashTable::InitializePointerTable() {
	idx_t capacity = PointerTableCapacity(Count());
	D_ASSERT(IsPowerOfTwo(capacity));

	if (hash_map.get()) {
		// There is already a hash map
		auto current_capacity = hash_map.GetSize() / sizeof(data_ptr_t);
		if (capacity > current_capacity) {
			// Need more space
			hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
		} else {
			// Just use the current hash map
			capacity = current_capacity;
		}
	} else {
		// Allocate a hash map
		hash_map = buffer_manager.GetBufferAllocator().Allocate(capacity * sizeof(data_ptr_t));
	}
	D_ASSERT(hash_map.GetSize() == capacity * sizeof(data_ptr_t));

	// initialize HT with all-zero entries
	std::fill_n(reinterpret_cast<data_ptr_t *>(hash_map.get()), capacity, nullptr);

	bitmask = capacity - 1;
}

void JoinHashTable::Finalize(idx_t chunk_idx_from, idx_t chunk_idx_to, bool parallel) {
	// Pointer table should be allocated
	D_ASSERT(hash_map.get());

	Vector hashes(LogicalType::HASH);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	TupleDataChunkIterator iterator(*data_collection, TupleDataPinProperties::KEEP_EVERYTHING_PINNED, chunk_idx_from,
	                                chunk_idx_to, false);
	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			hash_data[i] = Load<hash_t>(row_locations[i] + pointer_offset);
		}
		InsertHashes(hashes, count, row_locations, parallel);
	} while (iterator.Next());
}

unique_ptr<ScanStructure> JoinHashTable::InitializeScanStructure(DataChunk &keys, TupleDataChunkState &key_state,
                                                                 const SelectionVector *&current_sel) {
	D_ASSERT(Count() > 0); // should be handled before
	D_ASSERT(finalized);

	// set up the scan structure
	auto ss = make_uniq<ScanStructure>(*this, key_state);

	if (join_type != JoinType::INNER) {
		ss->found_match = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
		memset(ss->found_match.get(), 0, sizeof(bool) * STANDARD_VECTOR_SIZE);
	}

	// first prepare the keys for probing
	TupleDataCollection::ToUnifiedFormat(key_state, keys);
	ss->count = PrepareKeys(keys, key_state.vector_data, current_sel, ss->sel_vector, false);
	return ss;
}

unique_ptr<ScanStructure> JoinHashTable::Probe(DataChunk &keys, TupleDataChunkState &key_state,
                                               Vector *precomputed_hashes) {
	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, key_state, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	if (precomputed_hashes) {
		ApplyBitmask(*precomputed_hashes, *current_sel, ss->count, ss->pointers);
	} else {
		// hash all the keys
		Vector hashes(LogicalType::HASH);
		Hash(keys, *current_sel, ss->count, hashes);

		// now initialize the pointers of the scan structure based on the hashes
		ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);
	}

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ScanStructure::ScanStructure(JoinHashTable &ht_p, TupleDataChunkState &key_state_p)
    : key_state(key_state_p), pointers(LogicalType::POINTER), sel_vector(STANDARD_VECTOR_SIZE), ht(ht_p),
      finished(false) {
}

void ScanStructure::Next(DataChunk &keys, DataChunk &left, DataChunk &result) {
	if (finished) {
		return;
	}
	switch (ht.join_type) {
	case JoinType::INNER:
	case JoinType::RIGHT:
		NextInnerJoin(keys, left, result);
		break;
	case JoinType::SEMI:
		NextSemiJoin(keys, left, result);
		break;
	case JoinType::MARK:
		NextMarkJoin(keys, left, result);
		break;
	case JoinType::ANTI:
		NextAntiJoin(keys, left, result);
		break;
	case JoinType::OUTER:
	case JoinType::LEFT:
		NextLeftJoin(keys, left, result);
		break;
	case JoinType::SINGLE:
		NextSingleJoin(keys, left, result);
		break;
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
}

idx_t ScanStructure::ResolvePredicates(DataChunk &keys, SelectionVector &match_sel, SelectionVector *no_match_sel) {
	// Start with the scan selection
	for (idx_t i = 0; i < this->count; ++i) {
		match_sel.set_index(i, this->sel_vector.get_index(i));
	}
	idx_t no_match_count = 0;

	auto &matcher = no_match_sel ? ht.row_matcher_no_match_sel : ht.row_matcher;
	return matcher.Match(keys, key_state.vector_data, match_sel, this->count, ht.layout, pointers, no_match_sel,
	                     no_match_count);
}

idx_t ScanStructure::ScanInnerJoin(DataChunk &keys, SelectionVector &result_vector) {
	while (true) {
		// resolve the predicates for this set of keys
		idx_t result_count = ResolvePredicates(keys, result_vector, nullptr);

		// after doing all the comparisons set the found_match vector
		if (found_match) {
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				found_match[idx] = true;
			}
		}
		if (result_count > 0) {
			return result_count;
		}
		// no matches found: check the next set of pointers
		AdvancePointers();
		if (this->count == 0) {
			return 0;
		}
	}
}

void ScanStructure::AdvancePointers(const SelectionVector &sel, idx_t sel_count) {
	// now for all the pointers, we move on to the next set of pointers
	idx_t new_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(this->pointers);
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx] + ht.pointer_offset);
		if (ptrs[idx]) {
			this->sel_vector.set_index(new_count++, idx);
		}
	}
	this->count = new_count;
}

void ScanStructure::InitializeSelectionVector(const SelectionVector *&current_sel) {
	idx_t non_empty_count = 0;
	auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
	auto cnt = count;
	for (idx_t i = 0; i < cnt; i++) {
		const auto idx = current_sel->get_index(i);
		ptrs[idx] = Load<data_ptr_t>(ptrs[idx]);
		if (ptrs[idx]) {
			sel_vector.set_index(non_empty_count++, idx);
		}
	}
	count = non_empty_count;
}

void ScanStructure::AdvancePointers() {
	AdvancePointers(this->sel_vector, this->count);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &result_vector,
                                 const SelectionVector &sel_vector, const idx_t count, const idx_t col_no) {
	ht.data_collection->Gather(pointers, sel_vector, count, col_no, result, result_vector);
}

void ScanStructure::GatherResult(Vector &result, const SelectionVector &sel_vector, const idx_t count,
                                 const idx_t col_idx) {
	GatherResult(result, *FlatVector::IncrementalSelectionVector(), sel_vector, count, col_idx);
}

void ScanStructure::NextInnerJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == left.ColumnCount() + ht.build_types.size());
	if (this->count == 0) {
		// no pointers left to chase
		return;
	}

	SelectionVector result_vector(STANDARD_VECTOR_SIZE);

	idx_t result_count = ScanInnerJoin(keys, result_vector);
	if (result_count > 0) {
		if (IsRightOuterJoin(ht.join_type)) {
			// full/right outer join: mark join matches as FOUND in the HT
			auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
			for (idx_t i = 0; i < result_count; i++) {
				auto idx = result_vector.get_index(i);
				// NOTE: threadsan reports this as a data race because this can be set concurrently by separate threads
				// Technically it is, but it does not matter, since the only value that can be written is "true"
				Store<bool>(true, ptrs[idx] + ht.tuple_size);
			}
		}
		// matches were found
		// construct the result
		// on the LHS, we create a slice using the result vector
		result.Slice(left, result_vector, result_count);

		// on the RHS, we need to fetch the data from the hash table
		for (idx_t i = 0; i < ht.build_types.size(); i++) {
			auto &vector = result.data[left.ColumnCount() + i];
			D_ASSERT(vector.GetType() == ht.build_types[i]);
			GatherResult(vector, result_vector, result_count, i + ht.condition_types.size());
		}
		AdvancePointers();
	}
}

void ScanStructure::ScanKeyMatches(DataChunk &keys) {
	// the semi-join, anti-join and mark-join we handle a differently from the inner join
	// since there can be at most STANDARD_VECTOR_SIZE results
	// we handle the entire chunk in one call to Next().
	// for every pointer, we keep chasing pointers and doing comparisons.
	// this results in a boolean array indicating whether or not the tuple has a match
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			found_match[match_sel.get_index(i)] = true;
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
}

template <bool MATCH>
void ScanStructure::NextSemiOrAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	D_ASSERT(left.ColumnCount() == result.ColumnCount());
	D_ASSERT(keys.size() == left.size());
	// create the selection vector from the matches that were found
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < keys.size(); i++) {
		if (found_match[i] == MATCH) {
			// part of the result
			sel.set_index(result_count++, i);
		}
	}
	// construct the final result
	if (result_count > 0) {
		// we only return the columns on the left side
		// reference the columns of the left side from the result
		result.Slice(left, sel, result_count);
	} else {
		D_ASSERT(result.size() == 0);
	}
}

void ScanStructure::NextSemiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples with a match
	NextSemiOrAntiJoin<true>(keys, left, result);

	finished = true;
}

void ScanStructure::NextAntiJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// first scan for key matches
	ScanKeyMatches(keys);
	// then construct the result from all tuples that did not find a match
	NextSemiOrAntiJoin<false>(keys, left, result);

	finished = true;
}

void ScanStructure::ConstructMarkJoinResult(DataChunk &join_keys, DataChunk &child, DataChunk &result) {
	// for the initial set of columns we just reference the left side
	result.SetCardinality(child);
	for (idx_t i = 0; i < child.ColumnCount(); i++) {
		result.data[i].Reference(child.data[i]);
	}
	auto &mark_vector = result.data.back();
	mark_vector.SetVectorType(VectorType::FLAT_VECTOR);
	// first we set the NULL values from the join keys
	// if there is any NULL in the keys, the result is NULL
	auto bool_result = FlatVector::GetData<bool>(mark_vector);
	auto &mask = FlatVector::Validity(mark_vector);
	for (idx_t col_idx = 0; col_idx < join_keys.ColumnCount(); col_idx++) {
		if (ht.null_values_are_equal[col_idx]) {
			continue;
		}
		UnifiedVectorFormat jdata;
		join_keys.data[col_idx].ToUnifiedFormat(join_keys.size(), jdata);
		if (!jdata.validity.AllValid()) {
			for (idx_t i = 0; i < join_keys.size(); i++) {
				auto jidx = jdata.sel->get_index(i);
				mask.Set(i, jdata.validity.RowIsValidUnsafe(jidx));
			}
		}
	}
	// now set the remaining entries to either true or false based on whether a match was found
	if (found_match) {
		for (idx_t i = 0; i < child.size(); i++) {
			bool_result[i] = found_match[i];
		}
	} else {
		memset(bool_result, 0, sizeof(bool) * child.size());
	}
	// if the right side contains NULL values, the result of any FALSE becomes NULL
	if (ht.has_null) {
		for (idx_t i = 0; i < child.size(); i++) {
			if (!bool_result[i]) {
				mask.SetInvalid(i);
			}
		}
	}
}

void ScanStructure::NextMarkJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	D_ASSERT(result.ColumnCount() == input.ColumnCount() + 1);
	D_ASSERT(result.data.back().GetType() == LogicalType::BOOLEAN);
	// this method should only be called for a non-empty HT
	D_ASSERT(ht.Count() > 0);

	ScanKeyMatches(keys);
	if (ht.correlated_mark_join_info.correlated_types.empty()) {
		ConstructMarkJoinResult(keys, input, result);
	} else {
		auto &info = ht.correlated_mark_join_info;
		lock_guard<mutex> mj_lock(info.mj_lock);

		// there are correlated columns
		// first we fetch the counts from the aggregate hashtable corresponding to these entries
		D_ASSERT(keys.ColumnCount() == info.group_chunk.ColumnCount() + 1);
		info.group_chunk.SetCardinality(keys);
		for (idx_t i = 0; i < info.group_chunk.ColumnCount(); i++) {
			info.group_chunk.data[i].Reference(keys.data[i]);
		}
		info.correlated_counts->FetchAggregates(info.group_chunk, info.result_chunk);

		// for the initial set of columns we just reference the left side
		result.SetCardinality(input);
		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			result.data[i].Reference(input.data[i]);
		}
		// create the result matching vector
		auto &last_key = keys.data.back();
		auto &result_vector = result.data.back();
		// first set the nullmask based on whether or not there were NULL values in the join key
		result_vector.SetVectorType(VectorType::FLAT_VECTOR);
		auto bool_result = FlatVector::GetData<bool>(result_vector);
		auto &mask = FlatVector::Validity(result_vector);
		switch (last_key.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(last_key)) {
				mask.SetAllInvalid(input.size());
			}
			break;
		case VectorType::FLAT_VECTOR:
			mask.Copy(FlatVector::Validity(last_key), input.size());
			break;
		default: {
			UnifiedVectorFormat kdata;
			last_key.ToUnifiedFormat(keys.size(), kdata);
			for (idx_t i = 0; i < input.size(); i++) {
				auto kidx = kdata.sel->get_index(i);
				mask.Set(i, kdata.validity.RowIsValid(kidx));
			}
			break;
		}
		}

		auto count_star = FlatVector::GetData<int64_t>(info.result_chunk.data[0]);
		auto count = FlatVector::GetData<int64_t>(info.result_chunk.data[1]);
		// set the entries to either true or false based on whether a match was found
		for (idx_t i = 0; i < input.size(); i++) {
			D_ASSERT(count_star[i] >= count[i]);
			bool_result[i] = found_match ? found_match[i] : false;
			if (!bool_result[i] && count_star[i] > count[i]) {
				// RHS has NULL value and result is false: set to null
				mask.SetInvalid(i);
			}
			if (count_star[i] == 0) {
				// count == 0, set nullmask to false (we know the result is false now)
				mask.SetValid(i);
			}
		}
	}
	finished = true;
}

void ScanStructure::NextLeftJoin(DataChunk &keys, DataChunk &left, DataChunk &result) {
	// a LEFT OUTER JOIN is identical to an INNER JOIN except all tuples that do
	// not have a match must return at least one tuple (with the right side set
	// to NULL in every column)
	NextInnerJoin(keys, left, result);
	if (result.size() == 0) {
		// no entries left from the normal join
		// fill in the result of the remaining left tuples
		// together with NULL values on the right-hand side
		idx_t remaining_count = 0;
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < left.size(); i++) {
			if (!found_match[i]) {
				sel.set_index(remaining_count++, i);
			}
		}
		if (remaining_count > 0) {
			// have remaining tuples
			// slice the left side with tuples that did not find a match
			result.Slice(left, sel, remaining_count);

			// now set the right side to NULL
			for (idx_t i = left.ColumnCount(); i < result.ColumnCount(); i++) {
				Vector &vec = result.data[i];
				vec.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(vec, true);
			}
		}
		finished = true;
	}
}

void ScanStructure::NextSingleJoin(DataChunk &keys, DataChunk &input, DataChunk &result) {
	// single join
	// this join is similar to the semi join except that
	// (1) we actually return data from the RHS and
	// (2) we return NULL for that data if there is no match
	idx_t result_count = 0;
	SelectionVector result_sel(STANDARD_VECTOR_SIZE);
	SelectionVector match_sel(STANDARD_VECTOR_SIZE), no_match_sel(STANDARD_VECTOR_SIZE);
	while (this->count > 0) {
		// resolve the predicates for the current set of pointers
		idx_t match_count = ResolvePredicates(keys, match_sel, &no_match_sel);
		idx_t no_match_count = this->count - match_count;

		// mark each of the matches as found
		for (idx_t i = 0; i < match_count; i++) {
			// found a match for this index
			auto index = match_sel.get_index(i);
			found_match[index] = true;
			result_sel.set_index(result_count++, index);
		}
		// continue searching for the ones where we did not find a match yet
		AdvancePointers(no_match_sel, no_match_count);
	}
	// reference the columns of the left side from the result
	D_ASSERT(input.ColumnCount() > 0);
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		result.data[i].Reference(input.data[i]);
	}
	// now fetch the data from the RHS
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto &vector = result.data[input.ColumnCount() + i];
		// set NULL entries for every entry that was not found
		for (idx_t j = 0; j < input.size(); j++) {
			if (!found_match[j]) {
				FlatVector::SetNull(vector, j, true);
			}
		}
		// for the remaining values we fetch the values
		GatherResult(vector, result_sel, result_sel, result_count, i + ht.condition_types.size());
	}
	result.SetCardinality(input.size());

	// like the SEMI, ANTI and MARK join types, the SINGLE join only ever does one pass over the HT per input chunk
	finished = true;
}

void JoinHashTable::ScanFullOuter(JoinHTScanState &state, Vector &addresses, DataChunk &result) {
	// scan the HT starting from the current position and check which rows from the build side did not find a match
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t found_entries = 0;

	auto &iterator = state.iterator;
	if (iterator.Done()) {
		return;
	}

	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = state.offset_in_chunk; i < count; i++) {
			auto found_match = Load<bool>(row_locations[i] + tuple_size);
			if (!found_match) {
				key_locations[found_entries++] = row_locations[i];
				if (found_entries == STANDARD_VECTOR_SIZE) {
					state.offset_in_chunk = i + 1;
					break;
				}
			}
		}
		if (found_entries == STANDARD_VECTOR_SIZE) {
			break;
		}
		state.offset_in_chunk = 0;
	} while (iterator.Next());

	// now gather from the found rows
	if (found_entries == 0) {
		return;
	}
	result.SetCardinality(found_entries);
	idx_t left_column_count = result.ColumnCount() - build_types.size();
	const auto &sel_vector = *FlatVector::IncrementalSelectionVector();
	// set the left side as a constant NULL
	for (idx_t i = 0; i < left_column_count; i++) {
		Vector &vec = result.data[i];
		vec.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(vec, true);
	}

	// gather the values from the RHS
	for (idx_t i = 0; i < build_types.size(); i++) {
		auto &vector = result.data[left_column_count + i];
		D_ASSERT(vector.GetType() == build_types[i]);
		const auto col_no = condition_types.size() + i;
		data_collection->Gather(addresses, sel_vector, found_entries, col_no, vector, sel_vector);
	}
}

idx_t JoinHashTable::FillWithHTOffsets(JoinHTScanState &state, Vector &addresses) {
	// iterate over HT
	auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
	idx_t key_count = 0;

	auto &iterator = state.iterator;
	const auto row_locations = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			key_locations[key_count + i] = row_locations[i];
		}
		key_count += count;
	} while (iterator.Next());

	return key_count;
}

bool JoinHashTable::RequiresExternalJoin(ClientConfig &config, vector<unique_ptr<JoinHashTable>> &local_hts) {
	total_count = 0;
	idx_t data_size = 0;
	for (auto &ht : local_hts) {
		auto &local_sink_collection = ht->GetSinkCollection();
		total_count += local_sink_collection.Count();
		data_size += local_sink_collection.SizeInBytes();
	}

	if (total_count == 0) {
		return false;
	}

	if (config.force_external) {
		// Do 1 round per partition if forcing external join to test all code paths
		const auto r = RadixPartitioning::NumberOfPartitions(radix_bits);
		auto data_size_per_round = (data_size + r - 1) / r;
		auto count_per_round = (total_count + r - 1) / r;
		max_ht_size = data_size_per_round + PointerTableSize(count_per_round);
		external = true;
	} else {
		auto ht_size = data_size + PointerTableSize(total_count);
		external = ht_size > max_ht_size;
	}
	return external;
}

void JoinHashTable::Unpartition() {
	for (auto &partition : sink_collection->GetPartitions()) {
		data_collection->Combine(*partition);
	}
}

bool JoinHashTable::RequiresPartitioning(ClientConfig &config, vector<unique_ptr<JoinHashTable>> &local_hts) {
	D_ASSERT(total_count != 0);
	D_ASSERT(external);

	idx_t num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	vector<idx_t> partition_counts(num_partitions, 0);
	vector<idx_t> partition_sizes(num_partitions, 0);
	for (auto &ht : local_hts) {
		const auto &local_partitions = ht->GetSinkCollection().GetPartitions();
		for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
			auto &local_partition = local_partitions[partition_idx];
			partition_counts[partition_idx] += local_partition->Count();
			partition_sizes[partition_idx] += local_partition->SizeInBytes();
		}
	}

	// Figure out if we can fit all single partitions in memory
	idx_t max_partition_idx = 0;
	idx_t max_partition_size = 0;
	for (idx_t partition_idx = 0; partition_idx < num_partitions; partition_idx++) {
		const auto &partition_count = partition_counts[partition_idx];
		const auto &partition_size = partition_sizes[partition_idx];
		auto partition_ht_size = partition_size + PointerTableSize(partition_count);
		if (partition_ht_size > max_partition_size) {
			max_partition_size = partition_ht_size;
			max_partition_idx = partition_idx;
		}
	}

	if (config.force_external || max_partition_size > max_ht_size) {
		const auto partition_count = partition_counts[max_partition_idx];
		const auto partition_size = partition_sizes[max_partition_idx];

		const auto max_added_bits = RadixPartitioning::MAX_RADIX_BITS - radix_bits;
		idx_t added_bits = config.force_external ? 2 : 1;
		for (; added_bits < max_added_bits; added_bits++) {
			double partition_multiplier = RadixPartitioning::NumberOfPartitions(added_bits);

			auto new_estimated_count = double(partition_count) / partition_multiplier;
			auto new_estimated_size = double(partition_size) / partition_multiplier;
			auto new_estimated_ht_size = new_estimated_size + PointerTableSize(new_estimated_count);

			if (config.force_external || new_estimated_ht_size <= double(max_ht_size) / 4) {
				// Aim for an estimated partition size of max_ht_size / 4
				break;
			}
		}
		radix_bits += added_bits;
		sink_collection =
		    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, radix_bits, layout.ColumnCount() - 1);
		return true;
	} else {
		return false;
	}
}

void JoinHashTable::Partition(JoinHashTable &global_ht) {
	auto new_sink_collection =
	    make_uniq<RadixPartitionedTupleData>(buffer_manager, layout, global_ht.radix_bits, layout.ColumnCount() - 1);
	sink_collection->Repartition(*new_sink_collection);
	sink_collection = std::move(new_sink_collection);
	global_ht.Merge(*this);
}

void JoinHashTable::Reset() {
	data_collection->Reset();
	finalized = false;
}

bool JoinHashTable::PrepareExternalFinalize() {
	if (finalized) {
		Reset();
	}

	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	if (partition_end == num_partitions) {
		return false;
	}

	// Start where we left off
	auto &partitions = sink_collection->GetPartitions();
	partition_start = partition_end;

	// Determine how many partitions we can do next (at least one)
	idx_t count = 0;
	idx_t data_size = 0;
	idx_t partition_idx;
	for (partition_idx = partition_start; partition_idx < num_partitions; partition_idx++) {
		auto incl_count = count + partitions[partition_idx]->Count();
		auto incl_data_size = data_size + partitions[partition_idx]->SizeInBytes();
		auto incl_ht_size = incl_data_size + PointerTableSize(incl_count);
		if (count > 0 && incl_ht_size > max_ht_size) {
			break;
		}
		count = incl_count;
		data_size = incl_data_size;
	}
	partition_end = partition_idx;

	// Move the partitions to the main data collection
	for (partition_idx = partition_start; partition_idx < partition_end; partition_idx++) {
		data_collection->Combine(*partitions[partition_idx]);
	}
	D_ASSERT(Count() == count);

	return true;
}

static void CreateSpillChunk(DataChunk &spill_chunk, DataChunk &keys, DataChunk &payload, Vector &hashes) {
	spill_chunk.Reset();
	idx_t spill_col_idx = 0;
	for (idx_t col_idx = 0; col_idx < keys.ColumnCount(); col_idx++) {
		spill_chunk.data[col_idx].Reference(keys.data[col_idx]);
	}
	spill_col_idx += keys.ColumnCount();
	for (idx_t col_idx = 0; col_idx < payload.data.size(); col_idx++) {
		spill_chunk.data[spill_col_idx + col_idx].Reference(payload.data[col_idx]);
	}
	spill_col_idx += payload.ColumnCount();
	spill_chunk.data[spill_col_idx].Reference(hashes);
}

unique_ptr<ScanStructure> JoinHashTable::ProbeAndSpill(DataChunk &keys, TupleDataChunkState &key_state,
                                                       DataChunk &payload, ProbeSpill &probe_spill,
                                                       ProbeSpillLocalAppendState &spill_state,
                                                       DataChunk &spill_chunk) {
	// hash all the keys
	Vector hashes(LogicalType::HASH);
	Hash(keys, *FlatVector::IncrementalSelectionVector(), keys.size(), hashes);

	// find out which keys we can match with the current pinned partitions
	SelectionVector true_sel;
	SelectionVector false_sel;
	true_sel.Initialize();
	false_sel.Initialize();
	auto true_count = RadixPartitioning::Select(hashes, FlatVector::IncrementalSelectionVector(), keys.size(),
	                                            radix_bits, partition_end, &true_sel, &false_sel);
	auto false_count = keys.size() - true_count;

	CreateSpillChunk(spill_chunk, keys, payload, hashes);

	// can't probe these values right now, append to spill
	spill_chunk.Slice(false_sel, false_count);
	spill_chunk.Verify();
	probe_spill.Append(spill_chunk, spill_state);

	// slice the stuff we CAN probe right now
	hashes.Slice(true_sel, true_count);
	keys.Slice(true_sel, true_count);
	payload.Slice(true_sel, true_count);

	const SelectionVector *current_sel;
	auto ss = InitializeScanStructure(keys, key_state, current_sel);
	if (ss->count == 0) {
		return ss;
	}

	// now initialize the pointers of the scan structure based on the hashes
	ApplyBitmask(hashes, *current_sel, ss->count, ss->pointers);

	// create the selection vector linking to only non-empty entries
	ss->InitializeSelectionVector(current_sel);

	return ss;
}

ProbeSpill::ProbeSpill(JoinHashTable &ht, ClientContext &context, const vector<LogicalType> &probe_types)
    : ht(ht), context(context), probe_types(probe_types) {
	auto remaining_count = ht.GetSinkCollection().Count();
	auto remaining_data_size = ht.GetSinkCollection().SizeInBytes();
	auto remaining_ht_size = remaining_data_size + ht.PointerTableSize(remaining_count);
	if (remaining_ht_size <= ht.max_ht_size) {
		// No need to partition as we will only have one more probe round
		partitioned = false;
	} else {
		// More than one probe round to go, so we need to partition
		partitioned = true;
		global_partitions =
		    make_uniq<RadixPartitionedColumnData>(context, probe_types, ht.radix_bits, probe_types.size() - 1);
	}
	column_ids.reserve(probe_types.size());
	for (column_t column_id = 0; column_id < probe_types.size(); column_id++) {
		column_ids.emplace_back(column_id);
	}
}

ProbeSpillLocalState ProbeSpill::RegisterThread() {
	ProbeSpillLocalAppendState result;
	lock_guard<mutex> guard(lock);
	if (partitioned) {
		local_partitions.emplace_back(global_partitions->CreateShared());
		local_partition_append_states.emplace_back(make_uniq<PartitionedColumnDataAppendState>());
		local_partitions.back()->InitializeAppendState(*local_partition_append_states.back());

		result.local_partition = local_partitions.back().get();
		result.local_partition_append_state = local_partition_append_states.back().get();
	} else {
		local_spill_collections.emplace_back(
		    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types));
		local_spill_append_states.emplace_back(make_uniq<ColumnDataAppendState>());
		local_spill_collections.back()->InitializeAppend(*local_spill_append_states.back());

		result.local_spill_collection = local_spill_collections.back().get();
		result.local_spill_append_state = local_spill_append_states.back().get();
	}
	return result;
}

void ProbeSpill::Append(DataChunk &chunk, ProbeSpillLocalAppendState &local_state) {
	if (partitioned) {
		local_state.local_partition->Append(*local_state.local_partition_append_state, chunk);
	} else {
		local_state.local_spill_collection->Append(*local_state.local_spill_append_state, chunk);
	}
}

void ProbeSpill::Finalize() {
	if (partitioned) {
		D_ASSERT(local_partitions.size() == local_partition_append_states.size());
		for (idx_t i = 0; i < local_partition_append_states.size(); i++) {
			local_partitions[i]->FlushAppendState(*local_partition_append_states[i]);
		}
		for (auto &local_partition : local_partitions) {
			global_partitions->Combine(*local_partition);
		}
		local_partitions.clear();
		local_partition_append_states.clear();
	} else {
		if (local_spill_collections.empty()) {
			global_spill_collection =
			    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types);
		} else {
			global_spill_collection = std::move(local_spill_collections[0]);
			for (idx_t i = 1; i < local_spill_collections.size(); i++) {
				global_spill_collection->Combine(*local_spill_collections[i]);
			}
		}
		local_spill_collections.clear();
		local_spill_append_states.clear();
	}
}

void ProbeSpill::PrepareNextProbe() {
	if (partitioned) {
		auto &partitions = global_partitions->GetPartitions();
		if (partitions.empty() || ht.partition_start == partitions.size()) {
			// Can't probe, just make an empty one
			global_spill_collection =
			    make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), probe_types);
		} else {
			// Move specific partitions to the global spill collection
			global_spill_collection = std::move(partitions[ht.partition_start]);
			for (idx_t i = ht.partition_start + 1; i < ht.partition_end; i++) {
				auto &partition = partitions[i];
				if (global_spill_collection->Count() == 0) {
					global_spill_collection = std::move(partition);
				} else {
					global_spill_collection->Combine(*partition);
				}
			}
		}
	}
	consumer = make_uniq<ColumnDataConsumer>(*global_spill_collection, column_ids);
	consumer->InitializeScan();
}

} // namespace duckdb



namespace duckdb {

struct InitialNestedLoopJoin {
	template <class T, class OP>
	static idx_t Operation(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos, idx_t &rpos,
	                       SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
		using MATCH_OP = ComparisonOperationWrapper<OP>;

		// initialize phase of nested loop join
		// fill lvector and rvector with matches from the base vectors
		UnifiedVectorFormat left_data, right_data;
		left.ToUnifiedFormat(left_size, left_data);
		right.ToUnifiedFormat(right_size, right_data);

		auto ldata = UnifiedVectorFormat::GetData<T>(left_data);
		auto rdata = UnifiedVectorFormat::GetData<T>(right_data);
		idx_t result_count = 0;
		for (; rpos < right_size; rpos++) {
			idx_t right_position = right_data.sel->get_index(rpos);
			bool right_is_valid = right_data.validity.RowIsValid(right_position);
			for (; lpos < left_size; lpos++) {
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
				idx_t left_position = left_data.sel->get_index(lpos);
				bool left_is_valid = left_data.validity.RowIsValid(left_position);
				if (MATCH_OP::Operation(ldata[left_position], rdata[right_position], !left_is_valid, !right_is_valid)) {
					// emit tuple
					lvector.set_index(result_count, lpos);
					rvector.set_index(result_count, rpos);
					result_count++;
				}
			}
			lpos = 0;
		}
		return result_count;
	}
};

struct RefineNestedLoopJoin {
	template <class T, class OP>
	static idx_t Operation(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos, idx_t &rpos,
	                       SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
		using MATCH_OP = ComparisonOperationWrapper<OP>;

		UnifiedVectorFormat left_data, right_data;
		left.ToUnifiedFormat(left_size, left_data);
		right.ToUnifiedFormat(right_size, right_data);

		// refine phase of the nested loop join
		// refine lvector and rvector based on matches of subsequent conditions (in case there are multiple conditions
		// in the join)
		D_ASSERT(current_match_count > 0);
		auto ldata = UnifiedVectorFormat::GetData<T>(left_data);
		auto rdata = UnifiedVectorFormat::GetData<T>(right_data);
		idx_t result_count = 0;
		for (idx_t i = 0; i < current_match_count; i++) {
			auto lidx = lvector.get_index(i);
			auto ridx = rvector.get_index(i);
			auto left_idx = left_data.sel->get_index(lidx);
			auto right_idx = right_data.sel->get_index(ridx);
			bool left_is_valid = left_data.validity.RowIsValid(left_idx);
			bool right_is_valid = right_data.validity.RowIsValid(right_idx);
			if (MATCH_OP::Operation(ldata[left_idx], rdata[right_idx], !left_is_valid, !right_is_valid)) {
				lvector.set_index(result_count, lidx);
				rvector.set_index(result_count, ridx);
				result_count++;
			}
		}
		return result_count;
	}
};

template <class NLTYPE, class OP>
static idx_t NestedLoopJoinTypeSwitch(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos,
                                      idx_t &rpos, SelectionVector &lvector, SelectionVector &rvector,
                                      idx_t current_match_count) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return NLTYPE::template Operation<int8_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                              current_match_count);
	case PhysicalType::INT16:
		return NLTYPE::template Operation<int16_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::INT32:
		return NLTYPE::template Operation<int32_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::INT64:
		return NLTYPE::template Operation<int64_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::UINT8:
		return NLTYPE::template Operation<uint8_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                               current_match_count);
	case PhysicalType::UINT16:
		return NLTYPE::template Operation<uint16_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::UINT32:
		return NLTYPE::template Operation<uint32_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::UINT64:
		return NLTYPE::template Operation<uint64_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case PhysicalType::INT128:
		return NLTYPE::template Operation<hugeint_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                 rvector, current_match_count);
	case PhysicalType::FLOAT:
		return NLTYPE::template Operation<float, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                             current_match_count);
	case PhysicalType::DOUBLE:
		return NLTYPE::template Operation<double, OP>(left, right, left_size, right_size, lpos, rpos, lvector, rvector,
		                                              current_match_count);
	case PhysicalType::INTERVAL:
		return NLTYPE::template Operation<interval_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                  rvector, current_match_count);
	case PhysicalType::VARCHAR:
		return NLTYPE::template Operation<string_t, OP>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	default:
		throw InternalException("Unimplemented type for join!");
	}
}

template <class NLTYPE>
idx_t NestedLoopJoinComparisonSwitch(Vector &left, Vector &right, idx_t left_size, idx_t right_size, idx_t &lpos,
                                     idx_t &rpos, SelectionVector &lvector, SelectionVector &rvector,
                                     idx_t current_match_count, ExpressionType comparison_type) {
	D_ASSERT(left.GetType() == right.GetType());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return NestedLoopJoinTypeSwitch<NLTYPE, Equals>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                rvector, current_match_count);
	case ExpressionType::COMPARE_NOTEQUAL:
		return NestedLoopJoinTypeSwitch<NLTYPE, NotEquals>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                   rvector, current_match_count);
	case ExpressionType::COMPARE_LESSTHAN:
		return NestedLoopJoinTypeSwitch<NLTYPE, LessThan>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                  rvector, current_match_count);
	case ExpressionType::COMPARE_GREATERTHAN:
		return NestedLoopJoinTypeSwitch<NLTYPE, GreaterThan>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                     rvector, current_match_count);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return NestedLoopJoinTypeSwitch<NLTYPE, LessThanEquals>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                        rvector, current_match_count);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return NestedLoopJoinTypeSwitch<NLTYPE, GreaterThanEquals>(left, right, left_size, right_size, lpos, rpos,
		                                                           lvector, rvector, current_match_count);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return NestedLoopJoinTypeSwitch<NLTYPE, DistinctFrom>(left, right, left_size, right_size, lpos, rpos, lvector,
		                                                      rvector, current_match_count);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

idx_t NestedLoopJoinInner::Perform(idx_t &lpos, idx_t &rpos, DataChunk &left_conditions, DataChunk &right_conditions,
                                   SelectionVector &lvector, SelectionVector &rvector,
                                   const vector<JoinCondition> &conditions) {
	D_ASSERT(left_conditions.ColumnCount() == right_conditions.ColumnCount());
	if (lpos >= left_conditions.size() || rpos >= right_conditions.size()) {
		return 0;
	}
	// for the first condition, lvector and rvector are not set yet
	// we initialize them using the InitialNestedLoopJoin
	idx_t match_count = NestedLoopJoinComparisonSwitch<InitialNestedLoopJoin>(
	    left_conditions.data[0], right_conditions.data[0], left_conditions.size(), right_conditions.size(), lpos, rpos,
	    lvector, rvector, 0, conditions[0].comparison);
	// now resolve the rest of the conditions
	for (idx_t i = 1; i < conditions.size(); i++) {
		// check if we have run out of tuples to compare
		if (match_count == 0) {
			return 0;
		}
		// if not, get the vectors to compare
		Vector &l = left_conditions.data[i];
		Vector &r = right_conditions.data[i];
		// then we refine the currently obtained results using the RefineNestedLoopJoin
		match_count = NestedLoopJoinComparisonSwitch<RefineNestedLoopJoin>(
		    l, r, left_conditions.size(), right_conditions.size(), lpos, rpos, lvector, rvector, match_count,
		    conditions[i].comparison);
	}
	return match_count;
}

} // namespace duckdb




namespace duckdb {

template <class T, class OP>
static void TemplatedMarkJoin(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	using MATCH_OP = ComparisonOperationWrapper<OP>;

	UnifiedVectorFormat left_data, right_data;
	left.ToUnifiedFormat(lcount, left_data);
	right.ToUnifiedFormat(rcount, right_data);

	auto ldata = UnifiedVectorFormat::GetData<T>(left_data);
	auto rdata = UnifiedVectorFormat::GetData<T>(right_data);
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		auto lidx = left_data.sel->get_index(i);
		const auto left_null = !left_data.validity.RowIsValid(lidx);
		if (!MATCH_OP::COMPARE_NULL && left_null) {
			continue;
		}
		for (idx_t j = 0; j < rcount; j++) {
			auto ridx = right_data.sel->get_index(j);
			const auto right_null = !right_data.validity.RowIsValid(ridx);
			if (!MATCH_OP::COMPARE_NULL && right_null) {
				continue;
			}
			if (MATCH_OP::template Operation<T>(ldata[lidx], rdata[ridx], left_null, right_null)) {
				found_match[i] = true;
				break;
			}
		}
	}
}

static void MarkJoinNested(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                           ExpressionType comparison_type) {
	Vector left_reference(left.GetType());
	SelectionVector true_sel(rcount);
	for (idx_t i = 0; i < lcount; i++) {
		if (found_match[i]) {
			continue;
		}
		ConstantVector::Reference(left_reference, left, i, rcount);
		idx_t count;
		switch (comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			count = VectorOperations::Equals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			count = VectorOperations::NotEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			count = VectorOperations::LessThan(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			count = VectorOperations::GreaterThan(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			count = VectorOperations::LessThanEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			count = VectorOperations::GreaterThanEquals(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_DISTINCT_FROM:
			count = VectorOperations::DistinctFrom(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			count = VectorOperations::NotDistinctFrom(left_reference, right, nullptr, rcount, nullptr, nullptr);
			break;
		default:
			throw InternalException("Unsupported comparison type for MarkJoinNested");
		}
		if (count > 0) {
			found_match[i] = true;
		}
	}
}

template <class OP>
static void MarkJoinSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[]) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedMarkJoin<int8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT16:
		return TemplatedMarkJoin<int16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT32:
		return TemplatedMarkJoin<int32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT64:
		return TemplatedMarkJoin<int64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::INT128:
		return TemplatedMarkJoin<hugeint_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT8:
		return TemplatedMarkJoin<uint8_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT16:
		return TemplatedMarkJoin<uint16_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT32:
		return TemplatedMarkJoin<uint32_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::UINT64:
		return TemplatedMarkJoin<uint64_t, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::FLOAT:
		return TemplatedMarkJoin<float, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::DOUBLE:
		return TemplatedMarkJoin<double, OP>(left, right, lcount, rcount, found_match);
	case PhysicalType::VARCHAR:
		return TemplatedMarkJoin<string_t, OP>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented type for mark join!");
	}
}

static void MarkJoinComparisonSwitch(Vector &left, Vector &right, idx_t lcount, idx_t rcount, bool found_match[],
                                     ExpressionType comparison_type) {
	switch (left.GetType().InternalType()) {
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		return MarkJoinNested(left, right, lcount, rcount, found_match, comparison_type);
	default:
		break;
	}
	D_ASSERT(left.GetType() == right.GetType());
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return MarkJoinSwitch<Equals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_NOTEQUAL:
		return MarkJoinSwitch<NotEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHAN:
		return MarkJoinSwitch<LessThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHAN:
		return MarkJoinSwitch<GreaterThan>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return MarkJoinSwitch<LessThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return MarkJoinSwitch<GreaterThanEquals>(left, right, lcount, rcount, found_match);
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return MarkJoinSwitch<DistinctFrom>(left, right, lcount, rcount, found_match);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

void NestedLoopJoinMark::Perform(DataChunk &left, ColumnDataCollection &right, bool found_match[],
                                 const vector<JoinCondition> &conditions) {
	// initialize a new temporary selection vector for the left chunk
	// loop over all chunks in the RHS
	ColumnDataScanState scan_state;
	right.InitializeScan(scan_state);

	DataChunk scan_chunk;
	right.InitializeScanChunk(scan_chunk);

	while (right.Scan(scan_state, scan_chunk)) {
		for (idx_t i = 0; i < conditions.size(); i++) {
			MarkJoinComparisonSwitch(left.data[i], scan_chunk.data[i], left.size(), scan_chunk.size(), found_match,
			                         conditions[i].comparison);
		}
	}
}

} // namespace duckdb





namespace duckdb {

AggregateObject::AggregateObject(AggregateFunction function, FunctionData *bind_data, idx_t child_count,
                                 idx_t payload_size, AggregateType aggr_type, PhysicalType return_type,
                                 Expression *filter)
    : function(std::move(function)),
      bind_data_wrapper(bind_data ? make_shared<FunctionDataWrapper>(bind_data->Copy()) : nullptr),
      child_count(child_count), payload_size(payload_size), aggr_type(aggr_type), return_type(return_type),
      filter(filter) {
}

AggregateObject::AggregateObject(BoundAggregateExpression *aggr)
    : AggregateObject(aggr->function, aggr->bind_info.get(), aggr->children.size(),
                      AlignValue(aggr->function.state_size()), aggr->aggr_type, aggr->return_type.InternalType(),
                      aggr->filter.get()) {
}

AggregateObject::AggregateObject(BoundWindowExpression &window)
    : AggregateObject(*window.aggregate, window.bind_info.get(), window.children.size(),
                      AlignValue(window.aggregate->state_size()), AggregateType::NON_DISTINCT,
                      window.return_type.InternalType(), window.filter_expr.get()) {
}

vector<AggregateObject> AggregateObject::CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings) {
	vector<AggregateObject> aggregates;
	aggregates.reserve(aggregates.size());
	for (auto &binding : bindings) {
		aggregates.emplace_back(binding);
	}
	return aggregates;
}

AggregateFilterData::AggregateFilterData(ClientContext &context, Expression &filter_expr,
                                         const vector<LogicalType> &payload_types)
    : filter_executor(context, &filter_expr), true_sel(STANDARD_VECTOR_SIZE) {
	if (payload_types.empty()) {
		return;
	}
	filtered_payload.Initialize(Allocator::Get(context), payload_types);
}

idx_t AggregateFilterData::ApplyFilter(DataChunk &payload) {
	filtered_payload.Reset();

	auto count = filter_executor.SelectExpression(payload, true_sel);
	filtered_payload.Slice(payload, true_sel, count);
	return count;
}

AggregateFilterDataSet::AggregateFilterDataSet() {
}

void AggregateFilterDataSet::Initialize(ClientContext &context, const vector<AggregateObject> &aggregates,
                                        const vector<LogicalType> &payload_types) {
	bool has_filters = false;
	for (auto &aggregate : aggregates) {
		if (aggregate.filter) {
			has_filters = true;
			break;
		}
	}
	if (!has_filters) {
		// no filters: nothing to do
		return;
	}
	filter_data.resize(aggregates.size());
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx];
		if (aggr.filter) {
			filter_data[aggr_idx] = make_uniq<AggregateFilterData>(context, *aggr.filter, payload_types);
		}
	}
}

AggregateFilterData &AggregateFilterDataSet::GetFilterData(idx_t aggr_idx) {
	D_ASSERT(aggr_idx < filter_data.size());
	D_ASSERT(filter_data[aggr_idx]);
	return *filter_data[aggr_idx];
}
} // namespace duckdb






namespace duckdb {

//! Shared information about a collection of distinct aggregates
DistinctAggregateCollectionInfo::DistinctAggregateCollectionInfo(const vector<unique_ptr<Expression>> &aggregates,
                                                                 vector<idx_t> indices)
    : indices(std::move(indices)), aggregates(aggregates) {
	table_count = CreateTableIndexMap();

	const idx_t aggregate_count = aggregates.size();

	total_child_count = 0;
	for (idx_t i = 0; i < aggregate_count; i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();

		if (!aggregate.IsDistinct()) {
			continue;
		}
		total_child_count += aggregate.children.size();
	}
}

//! Stateful data for the distinct aggregates

DistinctAggregateState::DistinctAggregateState(const DistinctAggregateData &data, ClientContext &client)
    : child_executor(client) {

	radix_states.resize(data.info.table_count);
	distinct_output_chunks.resize(data.info.table_count);

	idx_t aggregate_count = data.info.aggregates.size();
	for (idx_t i = 0; i < aggregate_count; i++) {
		auto &aggregate = data.info.aggregates[i]->Cast<BoundAggregateExpression>();

		// Initialize the child executor and get the payload types for every aggregate
		for (auto &child : aggregate.children) {
			child_executor.AddExpression(*child);
		}
		if (!aggregate.IsDistinct()) {
			continue;
		}
		D_ASSERT(data.info.table_map.count(i));
		idx_t table_idx = data.info.table_map.at(i);
		if (data.radix_tables[table_idx] == nullptr) {
			//! This table is unused because the aggregate shares its data with another
			continue;
		}

		// Get the global sinkstate for the aggregate
		auto &radix_table = *data.radix_tables[table_idx];
		radix_states[table_idx] = radix_table.GetGlobalSinkState(client);

		// Fill the chunk_types (group_by + children)
		vector<LogicalType> chunk_types;
		for (auto &group_type : data.grouped_aggregate_data[table_idx]->group_types) {
			chunk_types.push_back(group_type);
		}

		// This is used in Finalize to get the data from the radix table
		distinct_output_chunks[table_idx] = make_uniq<DataChunk>();
		distinct_output_chunks[table_idx]->Initialize(client, chunk_types);
	}
}

//! Persistent + shared (read-only) data for the distinct aggregates
DistinctAggregateData::DistinctAggregateData(const DistinctAggregateCollectionInfo &info)
    : DistinctAggregateData(info, {}, nullptr) {
}

DistinctAggregateData::DistinctAggregateData(const DistinctAggregateCollectionInfo &info, const GroupingSet &groups,
                                             const vector<unique_ptr<Expression>> *group_expressions)
    : info(info) {
	grouped_aggregate_data.resize(info.table_count);
	radix_tables.resize(info.table_count);
	grouping_sets.resize(info.table_count);

	for (auto &i : info.indices) {
		auto &aggregate = info.aggregates[i]->Cast<BoundAggregateExpression>();

		D_ASSERT(info.table_map.count(i));
		idx_t table_idx = info.table_map.at(i);
		if (radix_tables[table_idx] != nullptr) {
			//! This aggregate shares a table with another aggregate, and the table is already initialized
			continue;
		}
		// The grouping set contains the indices of the chunk that correspond to the data vector
		// that will be used to figure out in which bucket the payload should be put
		auto &grouping_set = grouping_sets[table_idx];
		//! Populate the group with the children of the aggregate
		for (auto &group : groups) {
			grouping_set.insert(group);
		}
		idx_t group_by_size = group_expressions ? group_expressions->size() : 0;
		for (idx_t set_idx = 0; set_idx < aggregate.children.size(); set_idx++) {
			grouping_set.insert(set_idx + group_by_size);
		}
		// Create the hashtable for the aggregate
		grouped_aggregate_data[table_idx] = make_uniq<GroupedAggregateData>();
		grouped_aggregate_data[table_idx]->InitializeDistinct(info.aggregates[i], group_expressions);
		radix_tables[table_idx] =
		    make_uniq<RadixPartitionedHashTable>(grouping_set, *grouped_aggregate_data[table_idx]);

		// Fill the chunk_types (only contains the payload of the distinct aggregates)
		vector<LogicalType> chunk_types;
		for (auto &child_p : aggregate.children) {
			chunk_types.push_back(child_p->return_type);
		}
	}
}

using aggr_ref_t = reference<BoundAggregateExpression>;

struct FindMatchingAggregate {
	explicit FindMatchingAggregate(const aggr_ref_t &aggr) : aggr_r(aggr) {
	}
	bool operator()(const aggr_ref_t other_r) {
		auto &other = other_r.get();
		auto &aggr = aggr_r.get();
		if (other.children.size() != aggr.children.size()) {
			return false;
		}
		if (!Expression::Equals(aggr.filter, other.filter)) {
			return false;
		}
		for (idx_t i = 0; i < aggr.children.size(); i++) {
			auto &other_child = other.children[i]->Cast<BoundReferenceExpression>();
			auto &aggr_child = aggr.children[i]->Cast<BoundReferenceExpression>();
			if (other_child.index != aggr_child.index) {
				return false;
			}
		}
		return true;
	}
	const aggr_ref_t aggr_r;
};

idx_t DistinctAggregateCollectionInfo::CreateTableIndexMap() {
	vector<aggr_ref_t> table_inputs;

	D_ASSERT(table_map.empty());
	for (auto &agg_idx : indices) {
		D_ASSERT(agg_idx < aggregates.size());
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		auto matching_inputs =
		    std::find_if(table_inputs.begin(), table_inputs.end(), FindMatchingAggregate(std::ref(aggregate)));
		if (matching_inputs != table_inputs.end()) {
			//! Assign the existing table to the aggregate
			idx_t found_idx = std::distance(table_inputs.begin(), matching_inputs);
			table_map[agg_idx] = found_idx;
			continue;
		}
		//! Create a new table and assign its index to the aggregate
		table_map[agg_idx] = table_inputs.size();
		table_inputs.push_back(std::ref(aggregate));
	}
	//! Every distinct aggregate needs to be assigned an index
	D_ASSERT(table_map.size() == indices.size());
	//! There can not be more tables than there are distinct aggregates
	D_ASSERT(table_inputs.size() <= indices.size());

	return table_inputs.size();
}

bool DistinctAggregateCollectionInfo::AnyDistinct() const {
	return !indices.empty();
}

const unsafe_vector<idx_t> &DistinctAggregateCollectionInfo::Indices() const {
	return this->indices;
}

static vector<idx_t> GetDistinctIndices(vector<unique_ptr<Expression>> &aggregates) {
	vector<idx_t> distinct_indices;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.IsDistinct()) {
			distinct_indices.push_back(i);
		}
	}
	return distinct_indices;
}

unique_ptr<DistinctAggregateCollectionInfo>
DistinctAggregateCollectionInfo::Create(vector<unique_ptr<Expression>> &aggregates) {
	vector<idx_t> indices = GetDistinctIndices(aggregates);
	if (indices.empty()) {
		return nullptr;
	}
	return make_uniq<DistinctAggregateCollectionInfo>(aggregates, std::move(indices));
}

bool DistinctAggregateData::IsDistinct(idx_t index) const {
	bool is_distinct = !radix_tables.empty() && info.table_map.count(index);
#ifdef DEBUG
	//! Make sure that if it is distinct, it's also in the indices
	//! And if it's not distinct, that it's also not in the indices
	bool found = false;
	for (auto &idx : info.indices) {
		if (idx == index) {
			found = true;
			break;
		}
	}
	D_ASSERT(found == is_distinct);
#endif
	return is_distinct;
}

} // namespace duckdb


namespace duckdb {

idx_t GroupedAggregateData::GroupCount() const {
	return groups.size();
}

const vector<vector<idx_t>> &GroupedAggregateData::GetGroupingFunctions() const {
	return grouping_functions;
}

void GroupedAggregateData::InitializeGroupby(vector<unique_ptr<Expression>> groups,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unsafe_vector<idx_t>> grouping_functions) {
	InitializeGroupbyGroups(std::move(groups));
	vector<LogicalType> payload_types_filters;

	SetGroupingFunctions(grouping_functions);

	filter_count = 0;
	for (auto &expr : expressions) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		aggregate_return_types.push_back(aggr.return_type);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			filter_count++;
			payload_types_filters.push_back(aggr.filter->return_type);
		}
		if (!aggr.function.combine) {
			throw InternalException("Aggregate function %s is missing a combine method", aggr.function.name);
		}
		aggregates.push_back(std::move(expr));
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
}

void GroupedAggregateData::InitializeDistinct(const unique_ptr<Expression> &aggregate,
                                              const vector<unique_ptr<Expression>> *groups_p) {
	auto &aggr = aggregate->Cast<BoundAggregateExpression>();
	D_ASSERT(aggr.IsDistinct());

	// Add the (empty in ungrouped case) groups of the aggregates
	InitializeDistinctGroups(groups_p);

	// bindings.push_back(&aggr);
	filter_count = 0;
	aggregate_return_types.push_back(aggr.return_type);
	for (idx_t i = 0; i < aggr.children.size(); i++) {
		auto &child = aggr.children[i];
		group_types.push_back(child->return_type);
		groups.push_back(child->Copy());
		payload_types.push_back(child->return_type);
		if (aggr.filter) {
			filter_count++;
		}
	}
	if (!aggr.function.combine) {
		throw InternalException("Aggregate function %s is missing a combine method", aggr.function.name);
	}
}

void GroupedAggregateData::InitializeDistinctGroups(const vector<unique_ptr<Expression>> *groups_p) {
	if (!groups_p) {
		return;
	}
	for (auto &expr : *groups_p) {
		group_types.push_back(expr->return_type);
		groups.push_back(expr->Copy());
	}
}

void GroupedAggregateData::InitializeGroupbyGroups(vector<unique_ptr<Expression>> groups) {
	// Add all the expressions of the group by clause
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	this->groups = std::move(groups);
}

void GroupedAggregateData::SetGroupingFunctions(vector<unsafe_vector<idx_t>> &functions) {
	grouping_functions.reserve(functions.size());
	for (idx_t i = 0; i < functions.size(); i++) {
		grouping_functions.push_back(std::move(functions[i]));
	}
}

} // namespace duckdb

















namespace duckdb {

HashAggregateGroupingData::HashAggregateGroupingData(GroupingSet &grouping_set_p,
                                                     const GroupedAggregateData &grouped_aggregate_data,
                                                     unique_ptr<DistinctAggregateCollectionInfo> &info)
    : table_data(grouping_set_p, grouped_aggregate_data) {
	if (info) {
		distinct_data = make_uniq<DistinctAggregateData>(*info, grouping_set_p, &grouped_aggregate_data.groups);
	}
}

bool HashAggregateGroupingData::HasDistinct() const {
	return distinct_data != nullptr;
}

HashAggregateGroupingGlobalState::HashAggregateGroupingGlobalState(const HashAggregateGroupingData &data,
                                                                   ClientContext &context) {
	table_state = data.table_data.GetGlobalSinkState(context);
	if (data.HasDistinct()) {
		distinct_state = make_uniq<DistinctAggregateState>(*data.distinct_data, context);
	}
}

HashAggregateGroupingLocalState::HashAggregateGroupingLocalState(const PhysicalHashAggregate &op,
                                                                 const HashAggregateGroupingData &data,
                                                                 ExecutionContext &context) {
	table_state = data.table_data.GetLocalSinkState(context);
	if (!data.HasDistinct()) {
		return;
	}
	auto &distinct_data = *data.distinct_data;

	auto &distinct_indices = op.distinct_collection_info->Indices();
	D_ASSERT(!distinct_indices.empty());

	distinct_states.resize(op.distinct_collection_info->aggregates.size());
	auto &table_map = op.distinct_collection_info->table_map;

	for (auto &idx : distinct_indices) {
		idx_t table_idx = table_map[idx];
		auto &radix_table = distinct_data.radix_tables[table_idx];
		if (radix_table == nullptr) {
			// This aggregate has identical input as another aggregate, so no table is created for it
			continue;
		}
		// Initialize the states of the radix tables used for the distinct aggregates
		distinct_states[table_idx] = radix_table->GetLocalSinkState(context);
	}
}

static vector<LogicalType> CreateGroupChunkTypes(vector<unique_ptr<Expression>> &groups) {
	set<idx_t> group_indices;

	if (groups.empty()) {
		return {};
	}

	for (auto &group : groups) {
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
		group_indices.insert(bound_ref.index);
	}
	idx_t highest_index = *group_indices.rbegin();
	vector<LogicalType> types(highest_index + 1, LogicalType::SQLNULL);
	for (auto &group : groups) {
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
		types[bound_ref.index] = bound_ref.return_type;
	}
	return types;
}

bool PhysicalHashAggregate::CanSkipRegularSink() const {
	if (!filter_indexes.empty()) {
		// If we have filters, we can't skip the regular sink, because we might lose groups otherwise.
		return false;
	}
	if (grouped_aggregate_data.aggregates.empty()) {
		// When there are no aggregates, we have to add to the main ht right away
		return false;
	}
	if (!non_distinct_filter.empty()) {
		return false;
	}
	return true;
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality)
    : PhysicalHashAggregate(context, std::move(types), std::move(expressions), {}, estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality)
    : PhysicalHashAggregate(context, std::move(types), std::move(expressions), std::move(groups_p), {}, {},
                            estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<unsafe_vector<idx_t>> grouping_functions_p,
                                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::HASH_GROUP_BY, std::move(types), estimated_cardinality),
      grouping_sets(std::move(grouping_sets_p)) {
	// get a list of all aggregates to be computed
	const idx_t group_count = groups_p.size();
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_count; i++) {
			set.insert(i);
		}
		grouping_sets.push_back(std::move(set));
	}
	input_group_types = CreateGroupChunkTypes(groups_p);

	grouped_aggregate_data.InitializeGroupby(std::move(groups_p), std::move(expressions),
	                                         std::move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	// Because everything that lives in this class should be read-only at execution time
	idx_t aggregate_input_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		aggregate_input_idx += aggr.children.size();
		if (aggr.aggr_type == AggregateType::DISTINCT) {
			distinct_filter.push_back(i);
		} else if (aggr.aggr_type == AggregateType::NON_DISTINCT) {
			non_distinct_filter.push_back(i);
		} else { // LCOV_EXCL_START
			throw NotImplementedException("AggregateType not implemented in PhysicalHashAggregate");
		} // LCOV_EXCL_STOP
	}

	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto &bound_ref_expr = aggr.filter->Cast<BoundReferenceExpression>();
			if (!filter_indexes.count(aggr.filter.get())) {
				// Replace the bound reference expression's index with the corresponding index of the payload chunk
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx;
			}
			aggregate_input_idx++;
		}
	}

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(grouped_aggregate_data.aggregates);

	for (idx_t i = 0; i < grouping_sets.size(); i++) {
		groupings.emplace_back(grouping_sets[i], grouped_aggregate_data, distinct_collection_info);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSinkState : public GlobalSinkState {
public:
	HashAggregateGlobalSinkState(const PhysicalHashAggregate &op, ClientContext &context) {
		grouping_states.reserve(op.groupings.size());
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			grouping_states.emplace_back(grouping, context);
		}
		vector<LogicalType> filter_types;
		for (auto &aggr : op.grouped_aggregate_data.aggregates) {
			auto &aggregate = aggr->Cast<BoundAggregateExpression>();
			for (auto &child : aggregate.children) {
				payload_types.push_back(child->return_type);
			}
			if (aggregate.filter) {
				filter_types.push_back(aggregate.filter->return_type);
			}
		}
		payload_types.reserve(payload_types.size() + filter_types.size());
		payload_types.insert(payload_types.end(), filter_types.begin(), filter_types.end());
	}

	vector<HashAggregateGroupingGlobalState> grouping_states;
	vector<LogicalType> payload_types;
	//! Whether or not the aggregate is finished
	bool finished = false;
};

class HashAggregateLocalSinkState : public LocalSinkState {
public:
	HashAggregateLocalSinkState(const PhysicalHashAggregate &op, ExecutionContext &context) {

		auto &payload_types = op.grouped_aggregate_data.payload_types;
		if (!payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(payload_types);
		}

		grouping_states.reserve(op.groupings.size());
		for (auto &grouping : op.groupings) {
			grouping_states.emplace_back(op, grouping, context);
		}
		// The filter set is only needed here for the distinct aggregates
		// the filtering of data for the regular aggregates is done within the hashtable
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : op.grouped_aggregate_data.aggregates) {
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			aggregate_objects.emplace_back(&aggr);
		}

		filter_set.Initialize(context.client, aggregate_objects, payload_types);
	}

	DataChunk aggregate_input_chunk;
	vector<HashAggregateGroupingLocalState> grouping_states;
	AggregateFilterDataSet filter_set;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = state.Cast<HashAggregateGlobalSinkState>();
	for (auto &grouping_state : gstate.grouping_states) {
		RadixPartitionedHashTable::SetMultiScan(*grouping_state.table_state);
		if (!grouping_state.distinct_state) {
			continue;
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<HashAggregateLocalSinkState>(*this, context);
}

void PhysicalHashAggregate::SinkDistinctGrouping(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                                 idx_t grouping_idx) const {
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();

	auto &grouping_gstate = global_sink.grouping_states[grouping_idx];
	auto &grouping_lstate = sink.grouping_states[grouping_idx];
	auto &distinct_info = *distinct_collection_info;

	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;

	DataChunk empty_chunk;

	// Create an empty filter for Sink, since we don't need to update any aggregate states here
	unsafe_vector<idx_t> empty_filter;

	for (idx_t &idx : distinct_info.indices) {
		auto &aggregate = grouped_aggregate_data.aggregates[idx]->Cast<BoundAggregateExpression>();

		D_ASSERT(distinct_info.table_map.count(idx));
		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

		InterruptState interrupt_state;
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, interrupt_state};

		if (aggregate.filter) {
			DataChunk filter_chunk;
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			filter_chunk.InitializeEmpty(filtered_data.filtered_payload.GetTypes());

			// Add the filter Vector (BOOL)
			auto it = filter_indexes.find(aggregate.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			auto &filter_bound_ref = aggregate.filter->Cast<BoundReferenceExpression>();
			filter_chunk.data[filter_bound_ref.index].Reference(chunk.data[it->second]);
			filter_chunk.SetCardinality(chunk.size());

			// We cant use the AggregateFilterData::ApplyFilter method, because the chunk we need to
			// apply the filter to also has the groups, and the filtered_data.filtered_payload does not have those.
			SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
			idx_t count = filtered_data.filter_executor.SelectExpression(filter_chunk, sel_vec);

			if (count == 0) {
				continue;
			}

			// Because the 'input' chunk needs to be re-used after this, we need to create
			// a duplicate of it, that we can apply the filter to
			DataChunk filtered_input;
			filtered_input.InitializeEmpty(chunk.GetTypes());

			for (idx_t group_idx = 0; group_idx < grouped_aggregate_data.groups.size(); group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref = group->Cast<BoundReferenceExpression>();
				filtered_input.data[bound_ref.index].Reference(chunk.data[bound_ref.index]);
			}
			for (idx_t child_idx = 0; child_idx < aggregate.children.size(); child_idx++) {
				auto &child = aggregate.children[child_idx];
				auto &bound_ref = child->Cast<BoundReferenceExpression>();

				filtered_input.data[bound_ref.index].Reference(chunk.data[bound_ref.index]);
			}
			filtered_input.Slice(sel_vec, count);
			filtered_input.SetCardinality(count);

			radix_table.Sink(context, filtered_input, sink_input, empty_chunk, empty_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, empty_filter);
		}
	}
}

void PhysicalHashAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	for (idx_t i = 0; i < groupings.size(); i++) {
		SinkDistinctGrouping(context, chunk, input, i);
	}
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_state = input.global_state.Cast<HashAggregateGlobalSinkState>();

	if (distinct_collection_info) {
		SinkDistinct(context, chunk, input);
	}

	if (CanSkipRegularSink()) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	DataChunk &aggregate_input_chunk = local_state.aggregate_input_chunk;
	auto &aggregates = grouped_aggregate_data.aggregates;
	idx_t aggregate_input_idx = 0;

	// Populate the aggregate child vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			D_ASSERT(bound_ref_expr.index < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.index]);
		}
	}
	// Populate the filter vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(chunk.size());
	aggregate_input_chunk.Verify();

	// For every grouping set there is one radix_table
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_local_state = global_state.grouping_states[i];
		auto &grouping_global_state = local_state.grouping_states[i];
		InterruptState interrupt_state;
		OperatorSinkInput sink_input {*grouping_local_state.table_state, *grouping_global_state.table_state,
		                              interrupt_state};

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Sink(context, chunk, sink_input, aggregate_input_chunk, non_distinct_filter);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalHashAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {

	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();

	if (!distinct_collection_info) {
		return;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = global_sink.grouping_states[i];
		auto &grouping_lstate = sink.grouping_states[i];

		auto &distinct_data = groupings[i].distinct_data;
		auto &distinct_state = grouping_gstate.distinct_state;

		const auto table_count = distinct_data->radix_tables.size();
		for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
			if (!distinct_data->radix_tables[table_idx]) {
				continue;
			}
			auto &radix_table = *distinct_data->radix_tables[table_idx];
			auto &radix_global_sink = *distinct_state->radix_states[table_idx];
			auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

			radix_table.Combine(context, radix_global_sink, radix_local_sink);
		}
	}
}

SinkCombineResultType PhysicalHashAggregate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &llstate = input.local_state.Cast<HashAggregateLocalSinkState>();

	OperatorSinkCombineInput combine_distinct_input {gstate, llstate, input.interrupt_state};
	CombineDistinct(context, combine_distinct_input);

	if (CanSkipRegularSink()) {
		return SinkCombineResultType::FINISHED;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Combine(context, *grouping_gstate.table_state, *grouping_lstate.table_state);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	//! "Regular" Finalize Event that is scheduled after combining the thread-local distinct HTs
	HashAggregateFinalizeEvent(ClientContext &context, Pipeline *pipeline_p, const PhysicalHashAggregate &op_p,
	                           HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(*pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

class HashAggregateFinalizeTask : public ExecutorTask {
public:
	HashAggregateFinalizeTask(ClientContext &context, Pipeline &pipeline, shared_ptr<Event> event_p,
	                          const PhysicalHashAggregate &op, HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor), context(context), pipeline(pipeline), event(std::move(event_p)), op(op),
	      gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	ClientContext &context;
	Pipeline &pipeline;
	shared_ptr<Event> event;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

void HashAggregateFinalizeEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<HashAggregateFinalizeTask>(context, *pipeline, shared_from_this(), op, gstate));
	D_ASSERT(!tasks.empty());
	SetTasks(std::move(tasks));
}

TaskExecutionResult HashAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	op.FinalizeInternal(pipeline, *event, context, gstate, false);
	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

class HashAggregateDistinctFinalizeEvent : public BasePipelineEvent {
public:
	//! Distinct Finalize Event that is scheduled if we have distinct aggregates
	HashAggregateDistinctFinalizeEvent(ClientContext &context, Pipeline &pipeline_p, const PhysicalHashAggregate &op_p,
	                                   HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;
	void FinishEvent() override;

private:
	void CreateGlobalSources();

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;

public:
	//! The GlobalSourceStates for all the radix tables of the distinct aggregates
	vector<vector<unique_ptr<GlobalSourceState>>> global_source_states;
};

class HashAggregateDistinctFinalizeTask : public ExecutorTask {
public:
	HashAggregateDistinctFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, const PhysicalHashAggregate &op,
	                                  HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor), pipeline(pipeline), event(std::move(event_p)), op(op), gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void AggregateDistinctGrouping(const idx_t grouping_idx);

private:
	Pipeline &pipeline;
	shared_ptr<Event> event;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

void HashAggregateDistinctFinalizeEvent::Schedule() {
	CreateGlobalSources();

	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_threads; i++) {
		tasks.push_back(make_uniq<HashAggregateDistinctFinalizeTask>(*pipeline, shared_from_this(), op, gstate));
	}
	SetTasks(std::move(tasks));
}

void HashAggregateDistinctFinalizeEvent::CreateGlobalSources() {
	auto &aggregates = op.grouped_aggregate_data.aggregates;
	global_source_states.reserve(op.groupings.size());
	for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
		auto &grouping = op.groupings[grouping_idx];
		auto &distinct_data = *grouping.distinct_data;

		vector<unique_ptr<GlobalSourceState>> aggregate_sources;
		aggregate_sources.reserve(aggregates.size());
		for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
			auto &aggregate = aggregates[agg_idx];
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();

			if (!aggr.IsDistinct()) {
				aggregate_sources.push_back(nullptr);
				continue;
			}
			D_ASSERT(distinct_data.info.table_map.count(agg_idx));

			auto table_idx = distinct_data.info.table_map.at(agg_idx);
			auto &radix_table_p = distinct_data.radix_tables[table_idx];
			aggregate_sources.push_back(radix_table_p->GetGlobalSourceState(context));
		}
		global_source_states.push_back(std::move(aggregate_sources));
	}
}

void HashAggregateDistinctFinalizeEvent::FinishEvent() {
	// Now that everything is added to the main ht, we can actually finalize
	auto new_event = make_shared<HashAggregateFinalizeEvent>(context, pipeline.get(), op, gstate);
	this->InsertEvent(std::move(new_event));
}

TaskExecutionResult HashAggregateDistinctFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
		AggregateDistinctGrouping(grouping_idx);
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void HashAggregateDistinctFinalizeTask::AggregateDistinctGrouping(const idx_t grouping_idx) {
	D_ASSERT(op.distinct_collection_info);
	auto &info = *op.distinct_collection_info;

	auto &grouping_data = op.groupings[grouping_idx];
	auto &grouping_state = gstate.grouping_states[grouping_idx];
	D_ASSERT(grouping_state.distinct_state);
	auto &distinct_state = *grouping_state.distinct_state;
	auto &distinct_data = *grouping_data.distinct_data;

	auto &aggregates = info.aggregates;

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, &pipeline);

	// Sink state to sink into global HTs
	InterruptState interrupt_state;
	auto &global_sink_state = *grouping_state.table_state;
	auto local_sink_state = grouping_data.table_data.GetLocalSinkState(execution_context);
	OperatorSinkInput sink_input {global_sink_state, *local_sink_state, interrupt_state};

	// Create a chunk that mimics the 'input' chunk in Sink, for storing the group vectors
	DataChunk group_chunk;
	if (!op.input_group_types.empty()) {
		group_chunk.Initialize(executor.context, op.input_group_types);
	}

	auto &groups = op.grouped_aggregate_data.groups;
	const idx_t group_by_size = groups.size();

	DataChunk aggregate_input_chunk;
	if (!gstate.payload_types.empty()) {
		aggregate_input_chunk.Initialize(executor.context, gstate.payload_types);
	}

	auto &finalize_event = event->Cast<HashAggregateDistinctFinalizeEvent>();

	idx_t payload_idx;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < op.grouped_aggregate_data.aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		D_ASSERT(distinct_data.info.table_map.count(agg_idx));
		const auto &table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = distinct_data.radix_tables[table_idx];

		auto &sink = *distinct_state.radix_states[table_idx];
		auto local_source = radix_table->GetLocalSourceState(execution_context);
		OperatorSourceInput source_input {*finalize_event.global_source_states[grouping_idx][agg_idx], *local_source,
		                                  interrupt_state};

		// Create a duplicate of the output_chunk, because of multi-threading we cant alter the original
		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		// Fetch all the data from the aggregate ht, and Sink it into the main ht
		while (true) {
			output_chunk.Reset();
			group_chunk.Reset();
			aggregate_input_chunk.Reset();

			auto res = radix_table->GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				throw InternalException(
				    "Unexpected interrupt from radix table GetData in HashAggregateDistinctFinalizeTask");
			}

			auto &grouped_aggregate_data = *distinct_data.grouped_aggregate_data[table_idx];
			for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
				group_chunk.data[bound_ref_expr.index].Reference(output_chunk.data[group_idx]);
			}
			group_chunk.SetCardinality(output_chunk);

			for (idx_t child_idx = 0; child_idx < grouped_aggregate_data.groups.size() - group_by_size; child_idx++) {
				aggregate_input_chunk.data[payload_idx + child_idx].Reference(
				    output_chunk.data[group_by_size + child_idx]);
			}
			aggregate_input_chunk.SetCardinality(output_chunk);

			// Sink it into the main ht
			grouping_data.table_data.Sink(execution_context, group_chunk, sink_input, aggregate_input_chunk, {agg_idx});
		}
	}
	grouping_data.table_data.Combine(execution_context, global_sink_state, *local_sink_state);
}

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();
	D_ASSERT(distinct_collection_info);

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &distinct_data = *grouping.distinct_data;
		auto &distinct_state = *gstate.grouping_states[i].distinct_state;

		for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
			if (!distinct_data.radix_tables[table_idx]) {
				continue;
			}
			auto &radix_table = distinct_data.radix_tables[table_idx];
			auto &radix_state = *distinct_state.radix_states[table_idx];
			radix_table->Finalize(context, radix_state);
		}
	}
	auto new_event = make_shared<HashAggregateDistinctFinalizeEvent>(context, pipeline, *this, gstate);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::FinalizeInternal(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p, bool check_distinct) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();

	if (check_distinct && distinct_collection_info) {
		// There are distinct aggregates
		// If these are partitioned those need to be combined first
		// Then we Finalize again, skipping this step
		return FinalizeDistinct(pipeline, event, context, gstate_p);
	}

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &grouping_gstate = gstate.grouping_states[i];
		grouping.table_data.Finalize(context, *grouping_gstate.table_state);
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	return FinalizeInternal(pipeline, event, context, input.global_state, true);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSourceState : public GlobalSourceState {
public:
	HashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op) : op(op), state_index(0) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
			radix_states.push_back(rt.GetGlobalSourceState(context));
		}
	}

	const PhysicalHashAggregate &op;
	mutex lock;
	atomic<idx_t> state_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;

public:
	idx_t MaxThreads() override {
		// If there are no tables, we only need one thread.
		if (op.groupings.empty()) {
			return 1;
		}

		auto &ht_state = op.sink_state->Cast<HashAggregateGlobalSinkState>();
		idx_t partitions = 0;
		for (size_t sidx = 0; sidx < op.groupings.size(); ++sidx) {
			auto &grouping = op.groupings[sidx];
			auto &grouping_gstate = ht_state.grouping_states[sidx];
			partitions += grouping.table_data.NumberOfPartitions(*grouping_gstate.table_state);
		}
		return MaxValue<idx_t>(1, partitions);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSourceState>(context, *this);
}

class HashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateLocalSourceState(ExecutionContext &context, const PhysicalHashAggregate &op) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
			radix_states.push_back(rt.GetLocalSourceState(context));
		}
	}

	vector<unique_ptr<LocalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
	return make_uniq<HashAggregateLocalSourceState>(context, *this);
}

SourceResultType PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &sink_gstate = sink_state->Cast<HashAggregateGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashAggregateLocalSourceState>();
	while (true) {
		idx_t radix_idx = gstate.state_index;
		if (radix_idx >= groupings.size()) {
			break;
		}
		auto &grouping = groupings[radix_idx];
		auto &radix_table = grouping.table_data;
		auto &grouping_gstate = sink_gstate.grouping_states[radix_idx];

		InterruptState interrupt_state;
		OperatorSourceInput source_input {*gstate.radix_states[radix_idx], *lstate.radix_states[radix_idx],
		                                  interrupt_state};
		auto res = radix_table.GetData(context, chunk, *grouping_gstate.table_state, source_input);
		if (chunk.size() != 0) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		} else if (res == SourceResultType::BLOCKED) {
			throw InternalException("Unexpectedly Blocked from radix_table");
		}

		// move to the next table
		lock_guard<mutex> l(gstate.lock);
		radix_idx++;
		if (radix_idx > gstate.state_index) {
			// we have not yet worked on the table
			// move the global index forwards
			gstate.state_index = radix_idx;
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalHashAggregate::ParamsToString() const {
	string result;
	auto &groups = grouped_aggregate_data.groups;
	auto &aggregates = grouped_aggregate_data.aggregates;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb







namespace duckdb {

PhysicalPerfectHashAggregate::PhysicalPerfectHashAggregate(ClientContext &context, vector<LogicalType> types_p,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           const vector<unique_ptr<BaseStatistics>> &group_stats,
                                                           vector<idx_t> required_bits_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PERFECT_HASH_GROUP_BY, std::move(types_p), estimated_cardinality),
      groups(std::move(groups_p)), aggregates(std::move(aggregates_p)), required_bits(std::move(required_bits_p)) {
	D_ASSERT(groups.size() == group_stats.size());
	group_minima.reserve(group_stats.size());
	for (auto &stats : group_stats) {
		D_ASSERT(stats);
		auto &nstats = *stats;
		D_ASSERT(NumericStats::HasMin(nstats));
		group_minima.push_back(NumericStats::Min(nstats));
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}

	vector<BoundAggregateExpression *> bindings;
	vector<LogicalType> payload_types_filters;
	for (auto &expr : aggregates) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		bindings.push_back(&aggr);

		D_ASSERT(!aggr.IsDistinct());
		D_ASSERT(aggr.function.combine);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
	}
	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}
	aggregate_objects = AggregateObject::CreateAggregateObjects(bindings);

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto &bound_ref_expr = aggr.filter->Cast<BoundReferenceExpression>();
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}
}

unique_ptr<PerfectAggregateHashTable> PhysicalPerfectHashAggregate::CreateHT(Allocator &allocator,
                                                                             ClientContext &context) const {
	return make_uniq<PerfectAggregateHashTable>(context, allocator, group_types, payload_types, aggregate_objects,
	                                            group_minima, required_bits);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PerfectHashAggregateGlobalState : public GlobalSinkState {
public:
	PerfectHashAggregateGlobalState(const PhysicalPerfectHashAggregate &op, ClientContext &context)
	    : ht(op.CreateHT(Allocator::Get(context), context)) {
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
};

class PerfectHashAggregateLocalState : public LocalSinkState {
public:
	PerfectHashAggregateLocalState(const PhysicalPerfectHashAggregate &op, ExecutionContext &context)
	    : ht(op.CreateHT(Allocator::Get(context.client), context.client)) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}
	}

	//! The local aggregate hash table
	unique_ptr<PerfectAggregateHashTable> ht;
	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;
};

unique_ptr<GlobalSinkState> PhysicalPerfectHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PerfectHashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalPerfectHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PerfectHashAggregateLocalState>(*this, context);
}

SinkResultType PhysicalPerfectHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	DataChunk &group_chunk = lstate.group_chunk;
	DataChunk &aggregate_input_chunk = lstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
		group_chunk.data[group_idx].Reference(chunk.data[bound_ref_expr.index]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}

	group_chunk.SetCardinality(chunk.size());

	aggregate_input_chunk.SetCardinality(chunk.size());

	group_chunk.Verify();
	aggregate_input_chunk.Verify();
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	lstate.ht->AddChunk(group_chunk, aggregate_input_chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalPerfectHashAggregate::Combine(ExecutionContext &context,
                                                            OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PerfectHashAggregateLocalState>();
	auto &gstate = input.global_state.Cast<PerfectHashAggregateGlobalState>();

	lock_guard<mutex> l(gstate.lock);
	gstate.ht->Combine(*lstate.ht);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PerfectHashAggregateState : public GlobalSourceState {
public:
	PerfectHashAggregateState() : ht_scan_position(0) {
	}

	//! The current position to scan the HT for output tuples
	idx_t ht_scan_position;
};

unique_ptr<GlobalSourceState> PhysicalPerfectHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PerfectHashAggregateState>();
}

SourceResultType PhysicalPerfectHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &state = input.global_state.Cast<PerfectHashAggregateState>();
	auto &gstate = sink_state->Cast<PerfectHashAggregateGlobalState>();

	gstate.ht->Scan(state.ht_scan_position, chunk);

	if (chunk.size() > 0) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	} else {
		return SourceResultType::FINISHED;
	}
}

string PhysicalPerfectHashAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb








namespace duckdb {

PhysicalStreamingWindow::PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                                 idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list)) {
}

class StreamingWindowGlobalState : public GlobalOperatorState {
public:
	StreamingWindowGlobalState() : row_number(1) {
	}

	//! The next row number.
	std::atomic<int64_t> row_number;
};

class StreamingWindowState : public OperatorState {
public:
	using StateBuffer = vector<data_t>;

	StreamingWindowState()
	    : initialized(false), allocator(Allocator::DefaultAllocator()),
	      statev(LogicalType::POINTER, data_ptr_cast(&state_ptr)) {
	}

	~StreamingWindowState() override {
		for (size_t i = 0; i < aggregate_dtors.size(); ++i) {
			auto dtor = aggregate_dtors[i];
			if (dtor) {
				AggregateInputData aggr_input_data(aggregate_bind_data[i], allocator);
				state_ptr = aggregate_states[i].data();
				dtor(statev, aggr_input_data, 1);
			}
		}
	}

	void Initialize(ClientContext &context, DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		const_vectors.resize(expressions.size());
		aggregate_states.resize(expressions.size());
		aggregate_bind_data.resize(expressions.size(), nullptr);
		aggregate_dtors.resize(expressions.size(), nullptr);

		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			auto &wexpr = expr.Cast<BoundWindowExpression>();
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_AGGREGATE: {
				auto &aggregate = *wexpr.aggregate;
				auto &state = aggregate_states[expr_idx];
				aggregate_bind_data[expr_idx] = wexpr.bind_info.get();
				aggregate_dtors[expr_idx] = aggregate.destructor;
				state.resize(aggregate.state_size());
				aggregate.initialize(state.data());
				break;
			}
			case ExpressionType::WINDOW_FIRST_VALUE: {
				// Just execute the expression once
				ExpressionExecutor executor(context);
				executor.AddExpression(*wexpr.children[0]);
				DataChunk result;
				result.Initialize(Allocator::Get(context), {wexpr.children[0]->return_type});
				executor.Execute(input, result);

				const_vectors[expr_idx] = make_uniq<Vector>(result.GetValue(0, 0));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((double)0));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((int64_t)1));
				break;
			}
			default:
				break;
			}
		}
		initialized = true;
	}

public:
	bool initialized;
	vector<unique_ptr<Vector>> const_vectors;
	ArenaAllocator allocator;

	// Aggregation
	vector<StateBuffer> aggregate_states;
	vector<FunctionData *> aggregate_bind_data;
	vector<aggregate_destructor_t> aggregate_dtors;
	data_ptr_t state_ptr;
	Vector statev;
};

unique_ptr<GlobalOperatorState> PhysicalStreamingWindow::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<StreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalStreamingWindow::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<StreamingWindowState>();
}

OperatorResultType PhysicalStreamingWindow::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<StreamingWindowGlobalState>();
	auto &state = state_p.Cast<StreamingWindowState>();
	state.allocator.Reset();

	if (!state.initialized) {
		state.Initialize(context.client, input, select_list);
	}
	// Put payload columns in place
	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		chunk.data[col_idx].Reference(input.data[col_idx]);
	}
	// Compute window function
	const idx_t count = input.size();
	for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		idx_t col_idx = input.data.size() + expr_idx;
		auto &expr = *select_list[expr_idx];
		auto &result = chunk.data[col_idx];
		switch (expr.GetExpressionType()) {
		case ExpressionType::WINDOW_AGGREGATE: {
			//	Establish the aggregation environment
			auto &wexpr = expr.Cast<BoundWindowExpression>();
			auto &aggregate = *wexpr.aggregate;
			auto &statev = state.statev;
			state.state_ptr = state.aggregate_states[expr_idx].data();
			AggregateInputData aggr_input_data(wexpr.bind_info.get(), state.allocator);

			// Check for COUNT(*)
			if (wexpr.children.empty()) {
				D_ASSERT(GetTypeIdSize(result.GetType().InternalType()) == sizeof(int64_t));
				auto data = FlatVector::GetData<int64_t>(result);
				int64_t start_row = gstate.row_number;
				for (idx_t i = 0; i < input.size(); ++i) {
					data[i] = start_row + i;
				}
				break;
			}

			// Compute the arguments
			auto &allocator = Allocator::Get(context.client);
			ExpressionExecutor executor(context.client);
			vector<LogicalType> payload_types;
			for (auto &child : wexpr.children) {
				payload_types.push_back(child->return_type);
				executor.AddExpression(*child);
			}

			DataChunk payload;
			payload.Initialize(allocator, payload_types);
			executor.Execute(input, payload);

			// Iterate through them using a single SV
			payload.Flatten();
			DataChunk row;
			row.Initialize(allocator, payload_types);
			sel_t s = 0;
			SelectionVector sel(&s);
			row.Slice(sel, 1);
			for (size_t col_idx = 0; col_idx < payload.ColumnCount(); ++col_idx) {
				DictionaryVector::Child(row.data[col_idx]).Reference(payload.data[col_idx]);
			}

			// Update the state and finalize it one row at a time.
			for (idx_t i = 0; i < input.size(); ++i) {
				sel.set_index(0, i);
				aggregate.update(row.data.data(), aggr_input_data, row.ColumnCount(), statev, 1);
				aggregate.finalize(statev, aggr_input_data, result, 1, i);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE: {
			// Reference constant vector
			chunk.data[col_idx].Reference(*state.const_vectors[expr_idx]);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			// Set row numbers
			int64_t start_row = gstate.row_number;
			auto rdata = FlatVector::GetData<int64_t>(chunk.data[col_idx]);
			for (idx_t i = 0; i < count; i++) {
				rdata[i] = start_row + i;
			}
			break;
		}
		default:
			throw NotImplementedException("%s for StreamingWindow", ExpressionTypeToString(expr.GetExpressionType()));
		}
	}
	gstate.row_number += count;
	chunk.SetCardinality(count);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalStreamingWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb

















#include <functional>

namespace duckdb {

PhysicalUngroupedAggregate::PhysicalUngroupedAggregate(vector<LogicalType> types,
                                                       vector<unique_ptr<Expression>> expressions,
                                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNGROUPED_AGGREGATE, std::move(types), estimated_cardinality),
      aggregates(std::move(expressions)) {

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(aggregates);
	if (!distinct_collection_info) {
		return;
	}
	distinct_data = make_uniq<DistinctAggregateData>(*distinct_collection_info);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregateState {
	explicit AggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions) {
		counts = make_uniq_array<atomic<idx_t>>(aggregate_expressions.size());
		for (idx_t i = 0; i < aggregate_expressions.size(); i++) {
			auto &aggregate = aggregate_expressions[i];
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			auto state = make_unsafe_uniq_array<data_t>(aggr.function.state_size());
			aggr.function.initialize(state.get());
			aggregates.push_back(std::move(state));
			bind_data.push_back(aggr.bind_info.get());
			destructors.push_back(aggr.function.destructor);
#ifdef DEBUG
			counts[i] = 0;
#endif
		}
	}
	~AggregateState() {
		D_ASSERT(destructors.size() == aggregates.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregates[i].get())));
			state_vector.SetVectorType(VectorType::FLAT_VECTOR);

			ArenaAllocator allocator(Allocator::DefaultAllocator());
			AggregateInputData aggr_input_data(bind_data[i], allocator);
			destructors[i](state_vector, aggr_input_data, 1);
		}
	}

	void Move(AggregateState &other) {
		other.aggregates = std::move(aggregates);
		other.destructors = std::move(destructors);
	}

	//! The aggregate values
	vector<unsafe_unique_array<data_t>> aggregates;
	//! The bind data
	vector<FunctionData *> bind_data;
	//! The destructors
	vector<aggregate_destructor_t> destructors;
	//! Counts (used for verification)
	unique_array<atomic<idx_t>> counts;
};

class UngroupedAggregateGlobalSinkState : public GlobalSinkState {
public:
	UngroupedAggregateGlobalSinkState(const PhysicalUngroupedAggregate &op, ClientContext &client)
	    : state(op.aggregates), finished(false), allocator(BufferAllocator::Get(client)) {
		if (op.distinct_data) {
			distinct_state = make_uniq<DistinctAggregateState>(*op.distinct_data, client);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate state
	AggregateState state;
	//! Whether or not the aggregate is finished
	bool finished;
	//! The data related to the distinct aggregates (if there are any)
	unique_ptr<DistinctAggregateState> distinct_state;
	//! Global arena allocator
	ArenaAllocator allocator;
};

class UngroupedAggregateLocalSinkState : public LocalSinkState {
public:
	UngroupedAggregateLocalSinkState(const PhysicalUngroupedAggregate &op, const vector<LogicalType> &child_types,
	                                 GlobalSinkState &gstate_p, ExecutionContext &context)
	    : allocator(BufferAllocator::Get(context.client)), state(op.aggregates), child_executor(context.client),
	      aggregate_input_chunk(), filter_set() {
		auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();

		auto &allocator = BufferAllocator::Get(context.client);
		InitializeDistinctAggregates(op, gstate, context);

		vector<LogicalType> payload_types;
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : op.aggregates) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			// initialize the payload chunk
			for (auto &child : aggr.children) {
				payload_types.push_back(child->return_type);
				child_executor.AddExpression(*child);
			}
			aggregate_objects.emplace_back(&aggr);
		}
		if (!payload_types.empty()) { // for select count(*) from t; there is no payload at all
			aggregate_input_chunk.Initialize(allocator, payload_types);
		}
		filter_set.Initialize(context.client, aggregate_objects, child_types);
	}

	//! Local arena allocator
	ArenaAllocator allocator;
	//! The local aggregate state
	AggregateState state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk, containing all the Vectors for the aggregates
	DataChunk aggregate_input_chunk;
	//! Aggregate filter data set
	AggregateFilterDataSet filter_set;
	//! The local sink states of the distinct aggregates hash tables
	vector<unique_ptr<LocalSinkState>> radix_states;

public:
	void Reset() {
		aggregate_input_chunk.Reset();
	}
	void InitializeDistinctAggregates(const PhysicalUngroupedAggregate &op,
	                                  const UngroupedAggregateGlobalSinkState &gstate, ExecutionContext &context) {

		if (!op.distinct_data) {
			return;
		}
		auto &data = *op.distinct_data;
		auto &state = *gstate.distinct_state;
		D_ASSERT(!data.radix_tables.empty());

		const idx_t aggregate_count = state.radix_states.size();
		radix_states.resize(aggregate_count);

		auto &distinct_info = *op.distinct_collection_info;

		for (auto &idx : distinct_info.indices) {
			idx_t table_idx = distinct_info.table_map[idx];
			if (data.radix_tables[table_idx] == nullptr) {
				// This aggregate has identical input as another aggregate, so no table is created for it
				continue;
			}
			auto &radix_table = *data.radix_tables[table_idx];
			radix_states[table_idx] = radix_table.GetLocalSinkState(context);
		}
	}
};

bool PhysicalUngroupedAggregate::SinkOrderDependent() const {
	for (auto &expr : aggregates) {
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		if (aggr.function.order_dependent == AggregateOrderDependent::ORDER_DEPENDENT) {
			return true;
		}
	}
	return false;
}

unique_ptr<GlobalSinkState> PhysicalUngroupedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<UngroupedAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalUngroupedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = *sink_state;
	return make_uniq<UngroupedAggregateLocalSinkState>(*this, children[0]->GetTypes(), gstate, context);
}

void PhysicalUngroupedAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *global_sink.distinct_state;
	auto &distinct_info = *distinct_collection_info;
	auto &distinct_indices = distinct_info.Indices();

	DataChunk empty_chunk;

	auto &distinct_filter = distinct_info.Indices();

	for (auto &idx : distinct_indices) {
		auto &aggregate = aggregates[idx]->Cast<BoundAggregateExpression>();

		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			// This distinct aggregate shares its data with another
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state.radix_states[table_idx];
		auto &radix_local_sink = *sink.radix_states[table_idx];
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, input.interrupt_state};

		if (aggregate.filter) {
			// The hashtable can apply a filter, but only on the payload
			// And in our case, we need to filter the groups (the distinct aggr children)

			// Apply the filter before inserting into the hashtable
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			idx_t count = filtered_data.ApplyFilter(chunk);
			filtered_data.filtered_payload.SetCardinality(count);

			radix_table.Sink(context, filtered_data.filtered_payload, sink_input, empty_chunk, distinct_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, distinct_filter);
		}
	}
}

SinkResultType PhysicalUngroupedAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	// perform the aggregation inside the local state
	sink.Reset();

	if (distinct_data) {
		SinkDistinct(context, chunk, input);
	}

	DataChunk &payload_chunk = sink.aggregate_input_chunk;

	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;

	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		if (aggregate.IsDistinct()) {
			continue;
		}

		idx_t payload_cnt = 0;
		// resolve the filter (if any)
		if (aggregate.filter) {
			auto &filtered_data = sink.filter_set.GetFilterData(aggr_idx);
			auto count = filtered_data.ApplyFilter(chunk);

			sink.child_executor.SetChunk(filtered_data.filtered_payload);
			payload_chunk.SetCardinality(count);
		} else {
			sink.child_executor.SetChunk(chunk);
			payload_chunk.SetCardinality(chunk);
		}

#ifdef DEBUG
		sink.state.counts[aggr_idx] += payload_chunk.size();
#endif

		// resolve the child expressions of the aggregate (if any)
		for (idx_t i = 0; i < aggregate.children.size(); ++i) {
			sink.child_executor.ExecuteExpression(payload_idx + payload_cnt,
			                                      payload_chunk.data[payload_idx + payload_cnt]);
			payload_cnt++;
		}

		auto start_of_input = payload_cnt == 0 ? nullptr : &payload_chunk.data[payload_idx];
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), sink.allocator);
		aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
		                                 sink.state.aggregates[aggr_idx].get(), payload_chunk.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalUngroupedAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	if (!distinct_data) {
		return;
	}
	auto &distinct_state = gstate.distinct_state;
	auto table_count = distinct_data->radix_tables.size();
	for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *lstate.radix_states[table_idx];

		radix_table.Combine(context, radix_global_sink, radix_local_sink);
	}
}

SinkCombineResultType PhysicalUngroupedAggregate::Combine(ExecutionContext &context,
                                                          OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	D_ASSERT(!gstate.finished);

	// finalize: combine the local state into the global state
	// all aggregates are combinable: we might be doing a parallel aggregate
	// use the combine method to combine the partial aggregates
	OperatorSinkCombineInput distinct_input {gstate, lstate, input.interrupt_state};
	CombineDistinct(context, distinct_input);

	lock_guard<mutex> glock(gstate.lock);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		if (aggregate.IsDistinct()) {
			continue;
		}

		Vector source_state(Value::POINTER(CastPointerToValue(lstate.state.aggregates[aggr_idx].get())));
		Vector dest_state(Value::POINTER(CastPointerToValue(gstate.state.aggregates[aggr_idx].get())));

		AggregateInputData aggr_input_data(aggregate.bind_info.get(), gstate.allocator);
		aggregate.function.combine(source_state, dest_state, aggr_input_data, 1);
#ifdef DEBUG
		gstate.state.counts[aggr_idx] += lstate.state.counts[aggr_idx];
#endif
	}
	lstate.allocator.Destroy();

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this, lstate.child_executor, "child_executor", 0);
	client_profiler.Flush(context.thread.profiler);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class UngroupedDistinctAggregateFinalizeEvent : public BasePipelineEvent {
public:
	UngroupedDistinctAggregateFinalizeEvent(ClientContext &context, const PhysicalUngroupedAggregate &op_p,
	                                        UngroupedAggregateGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p), tasks_scheduled(0),
	      tasks_done(0) {
	}

public:
	void Schedule() override;

private:
	ClientContext &context;

	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

public:
	mutex lock;
	idx_t tasks_scheduled;
	idx_t tasks_done;

	vector<unique_ptr<GlobalSourceState>> global_source_states;
};

class UngroupedDistinctAggregateFinalizeTask : public ExecutorTask {
public:
	UngroupedDistinctAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p,
	                                       const PhysicalUngroupedAggregate &op,
	                                       UngroupedAggregateGlobalSinkState &state_p)
	    : ExecutorTask(executor), event(std::move(event_p)), op(op), gstate(state_p),
	      allocator(BufferAllocator::Get(executor.context)) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void AggregateDistinct();

private:
	shared_ptr<Event> event;

	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

	ArenaAllocator allocator;
};

void UngroupedDistinctAggregateFinalizeEvent::Schedule() {
	D_ASSERT(gstate.distinct_state);
	auto &aggregates = op.aggregates;
	auto &distinct_data = *op.distinct_data;

	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			global_source_states.push_back(nullptr);
			continue;
		}
		D_ASSERT(distinct_data.info.table_map.count(agg_idx));

		// Create global state for scanning
		auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table_p = *distinct_data.radix_tables[table_idx];
		global_source_states.push_back(radix_table_p.GetGlobalSourceState(context));
	}

	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_threads; i++) {
		tasks.push_back(
		    make_uniq<UngroupedDistinctAggregateFinalizeTask>(pipeline->executor, shared_from_this(), op, gstate));
		tasks_scheduled++;
	}
	SetTasks(std::move(tasks));
}

TaskExecutionResult UngroupedDistinctAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	AggregateDistinct();
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void UngroupedDistinctAggregateFinalizeTask::AggregateDistinct() {
	D_ASSERT(gstate.distinct_state);
	auto &distinct_state = *gstate.distinct_state;
	auto &distinct_data = *op.distinct_data;

	// Create thread-local copy of aggregate state
	auto &aggregates = op.aggregates;
	AggregateState state(aggregates);

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, nullptr);

	auto &finalize_event = event->Cast<UngroupedDistinctAggregateFinalizeEvent>();

	// Now loop through the distinct aggregates, scanning the distinct HTs
	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		const auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = *distinct_data.radix_tables[table_idx];
		auto lstate = radix_table.GetLocalSourceState(execution_context);

		auto &sink = *distinct_state.radix_states[table_idx];
		InterruptState interrupt_state;
		OperatorSourceInput source_input {*finalize_event.global_source_states[agg_idx], *lstate, interrupt_state};

		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		DataChunk payload_chunk;
		payload_chunk.InitializeEmpty(distinct_data.grouped_aggregate_data[table_idx]->group_types);
		payload_chunk.SetCardinality(0);

		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator);
		while (true) {
			output_chunk.Reset();

			auto res = radix_table.GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				throw InternalException(
				    "Unexpected interrupt from radix table GetData in UngroupedDistinctAggregateFinalizeTask");
			}

			// We dont need to resolve the filter, we already did this in Sink
			idx_t payload_cnt = aggregate.children.size();
			for (idx_t i = 0; i < payload_cnt; i++) {
				payload_chunk.data[i].Reference(output_chunk.data[i]);
			}
			payload_chunk.SetCardinality(output_chunk);

#ifdef DEBUG
			gstate.state.counts[agg_idx] += payload_chunk.size();
#endif

			// Update the aggregate state
			auto start_of_input = payload_cnt ? &payload_chunk.data[0] : nullptr;
			aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
			                                 state.aggregates[agg_idx].get(), payload_chunk.size());
		}
	}

	// After scanning the distinct HTs, we can combine the thread-local agg states with the thread-global
	lock_guard<mutex> guard(finalize_event.lock);
	payload_idx = 0;
	next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator);

		Vector state_vec(Value::POINTER(CastPointerToValue(state.aggregates[agg_idx].get())));
		Vector combined_vec(Value::POINTER(CastPointerToValue(gstate.state.aggregates[agg_idx].get())));
		aggregate.function.combine(state_vec, combined_vec, aggr_input_data, 1);
	}

	D_ASSERT(!gstate.finished);
	if (++finalize_event.tasks_done == finalize_event.tasks_scheduled) {
		gstate.finished = true;
	}
}

SinkFinalizeType PhysicalUngroupedAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                              GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *gstate.distinct_state;

	for (idx_t table_idx = 0; table_idx < distinct_data->radix_tables.size(); table_idx++) {
		auto &radix_table_p = distinct_data->radix_tables[table_idx];
		auto &radix_state = *distinct_state.radix_states[table_idx];
		radix_table_p->Finalize(context, radix_state);
	}
	auto new_event = make_shared<UngroupedDistinctAggregateFinalizeEvent>(context, *this, gstate, pipeline);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalUngroupedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();

	if (distinct_data) {
		return FinalizeDistinct(pipeline, event, context, input.global_state);
	}

	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void VerifyNullHandling(DataChunk &chunk, AggregateState &state, const vector<unique_ptr<Expression>> &aggregates) {
#ifdef DEBUG
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();
		if (state.counts[aggr_idx] == 0 && aggr.function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			// Default is when 0 values go in, NULL comes out
			UnifiedVectorFormat vdata;
			chunk.data[aggr_idx].ToUnifiedFormat(1, vdata);
			D_ASSERT(!vdata.validity.RowIsValid(vdata.sel->get_index(0)));
		}
	}
#endif
}

SourceResultType PhysicalUngroupedAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(gstate.finished);

	// initialize the result chunk with the aggregate values
	chunk.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		Vector state_vector(Value::POINTER(CastPointerToValue(gstate.state.aggregates[aggr_idx].get())));
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), gstate.allocator);
		aggregate.function.finalize(state_vector, aggr_input_data, chunk.data[aggr_idx], 1, 0);
	}
	VerifyNullHandling(chunk, gstate.state, aggregates);

	return SourceResultType::FINISHED;
}

string PhysicalUngroupedAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
























#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

//	Global sink state
class WindowGlobalSinkState : public GlobalSinkState {
public:
	WindowGlobalSinkState(const PhysicalWindow &op, ClientContext &context)
	    : op(op), mode(DBConfig::GetConfig(context).options.window_mode) {

		D_ASSERT(op.select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[0]->Cast<BoundWindowExpression>();

		global_partition =
		    make_uniq<PartitionGlobalSinkState>(context, wexpr.partitions, wexpr.orders, op.children[0]->types,
		                                        wexpr.partitions_stats, op.estimated_cardinality);
	}

	const PhysicalWindow &op;
	unique_ptr<PartitionGlobalSinkState> global_partition;
	WindowAggregationMode mode;
};

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	WindowLocalSinkState(ClientContext &context, const WindowGlobalSinkState &gstate)
	    : local_partition(context, *gstate.global_partition) {
	}

	void Sink(DataChunk &input_chunk) {
		local_partition.Sink(input_chunk);
	}

	void Combine() {
		local_partition.Combine();
	}

	PartitionLocalSinkState local_partition;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list_p,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list_p)) {
	is_order_dependent = false;
	for (auto &expr : select_list) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_WINDOW);
		auto &bound_window = expr->Cast<BoundWindowExpression>();
		if (bound_window.partitions.empty() && bound_window.orders.empty()) {
			is_order_dependent = true;
		}
	}
}

static unique_ptr<WindowExecutor> WindowExecutorFactory(BoundWindowExpression &wexpr, ClientContext &context,
                                                        const ValidityMask &partition_mask,
                                                        const ValidityMask &order_mask, const idx_t payload_count,
                                                        WindowAggregationMode mode) {
	switch (wexpr.type) {
	case ExpressionType::WINDOW_AGGREGATE:
		return make_uniq<WindowAggregateExecutor>(wexpr, context, payload_count, partition_mask, order_mask, mode);
	case ExpressionType::WINDOW_ROW_NUMBER:
		return make_uniq<WindowRowNumberExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_RANK_DENSE:
		return make_uniq<WindowDenseRankExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_RANK:
		return make_uniq<WindowRankExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_PERCENT_RANK:
		return make_uniq<WindowPercentRankExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_CUME_DIST:
		return make_uniq<WindowCumeDistExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_NTILE:
		return make_uniq<WindowNtileExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
		return make_uniq<WindowLeadLagExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_FIRST_VALUE:
		return make_uniq<WindowFirstValueExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_LAST_VALUE:
		return make_uniq<WindowLastValueExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
	case ExpressionType::WINDOW_NTH_VALUE:
		return make_uniq<WindowNthValueExecutor>(wexpr, context, payload_count, partition_mask, order_mask);
		break;
	default:
		throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr.type));
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalWindow::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();

	lstate.Sink(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalWindow::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<WindowLocalSinkState>();
	lstate.Combine();

	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowLocalSinkState>(context.client, gstate);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<WindowGlobalSinkState>(*this, context);
}

SinkFinalizeType PhysicalWindow::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &state = input.global_state.Cast<WindowGlobalSinkState>();

	//	Did we get any data?
	if (!state.global_partition->count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Do we have any sorting to schedule?
	if (state.global_partition->rows) {
		D_ASSERT(!state.global_partition->grouping_data);
		return state.global_partition->rows->count ? SinkFinalizeType::READY : SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Find the first group to sort
	if (!state.global_partition->HasMergeTasks()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Schedule all the sorts for maximum thread utilisation
	auto new_event = make_shared<PartitionMergeEvent>(*state.global_partition, pipeline);
	event.InsertEvent(std::move(new_event));

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowPartitionSourceState;

class WindowGlobalSourceState : public GlobalSourceState {
public:
	using HashGroupSourcePtr = unique_ptr<WindowPartitionSourceState>;
	using ScannerPtr = unique_ptr<RowDataCollectionScanner>;
	using Task = std::pair<WindowPartitionSourceState *, ScannerPtr>;

	WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p);

	//! Get the next task
	Task NextTask(idx_t hash_bin);

	//! Context for executing computations
	ClientContext &context;
	//! All the sunk data
	WindowGlobalSinkState &gsink;
	//! The next group to build.
	atomic<idx_t> next_build;
	//! The built groups
	vector<HashGroupSourcePtr> built;
	//! Serialise access to the built hash groups
	mutable mutex built_lock;
	//! The number of unfinished tasks
	atomic<idx_t> tasks_remaining;

public:
	idx_t MaxThreads() override {
		return tasks_remaining;
	}

private:
	Task CreateTask(idx_t hash_bin);
	Task StealWork();
};

WindowGlobalSourceState::WindowGlobalSourceState(ClientContext &context_p, WindowGlobalSinkState &gsink_p)
    : context(context_p), gsink(gsink_p), next_build(0), tasks_remaining(0) {
	auto &hash_groups = gsink.global_partition->hash_groups;

	auto &gpart = gsink.global_partition;
	if (hash_groups.empty()) {
		//	OVER()
		built.resize(1);
		if (gpart->rows) {
			tasks_remaining += gpart->rows->blocks.size();
		}
	} else {
		built.resize(hash_groups.size());
		idx_t batch_base = 0;
		for (auto &hash_group : hash_groups) {
			if (!hash_group) {
				continue;
			}
			auto &global_sort_state = *hash_group->global_sort;
			if (global_sort_state.sorted_blocks.empty()) {
				continue;
			}

			D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
			auto &sb = *global_sort_state.sorted_blocks[0];
			auto &sd = *sb.payload_data;
			tasks_remaining += sd.data_blocks.size();

			hash_group->batch_base = batch_base;
			batch_base += sd.data_blocks.size();
		}
	}
}

// Per-bin evaluation state (build and evaluate)
class WindowPartitionSourceState {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;
	using ExecutorPtr = unique_ptr<WindowExecutor>;
	using Executors = vector<ExecutorPtr>;

	WindowPartitionSourceState(ClientContext &context, WindowGlobalSourceState &gsource)
	    : context(context), op(gsource.gsink.op), gsource(gsource), read_block_idx(0), unscanned(0) {
		layout.Initialize(gsource.gsink.global_partition->payload_types);
	}

	unique_ptr<RowDataCollectionScanner> GetScanner() const;
	void MaterializeSortedData();
	void BuildPartition(WindowGlobalSinkState &gstate, const idx_t hash_bin);

	ClientContext &context;
	const PhysicalWindow &op;
	WindowGlobalSourceState &gsource;

	HashGroupPtr hash_group;
	//! The generated input chunks
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> heap;
	RowLayout layout;
	//! The partition boundary mask
	vector<validity_t> partition_bits;
	ValidityMask partition_mask;
	//! The order boundary mask
	vector<validity_t> order_bits;
	ValidityMask order_mask;
	//! External paging
	bool external;
	//! The current execution functions
	Executors executors;

	//! The bin number
	idx_t hash_bin;

	//! The next block to read.
	mutable atomic<idx_t> read_block_idx;
	//! The number of remaining unscanned blocks.
	atomic<idx_t> unscanned;
};

void WindowPartitionSourceState::MaterializeSortedData() {
	auto &global_sort_state = *hash_group->global_sort;
	if (global_sort_state.sorted_blocks.empty()) {
		return;
	}

	// scan the sorted row data
	D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
	auto &sb = *global_sort_state.sorted_blocks[0];

	// Free up some memory before allocating more
	sb.radix_sorting_data.clear();
	sb.blob_sorting_data = nullptr;

	// Move the sorting row blocks into our RDCs
	auto &buffer_manager = global_sort_state.buffer_manager;
	auto &sd = *sb.payload_data;

	// Data blocks are required
	D_ASSERT(!sd.data_blocks.empty());
	auto &block = sd.data_blocks[0];
	rows = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
	rows->blocks = std::move(sd.data_blocks);
	rows->count = std::accumulate(rows->blocks.begin(), rows->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });

	// Heap blocks are optional, but we want both for iteration.
	if (!sd.heap_blocks.empty()) {
		auto &block = sd.heap_blocks[0];
		heap = make_uniq<RowDataCollection>(buffer_manager, block->capacity, block->entry_size);
		heap->blocks = std::move(sd.heap_blocks);
		hash_group.reset();
	} else {
		heap = make_uniq<RowDataCollection>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	heap->count = std::accumulate(heap->blocks.begin(), heap->blocks.end(), idx_t(0),
	                              [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
}

unique_ptr<RowDataCollectionScanner> WindowPartitionSourceState::GetScanner() const {
	auto &gsink = *gsource.gsink.global_partition;
	if ((gsink.rows && !hash_bin) || hash_bin < gsink.hash_groups.size()) {
		const auto block_idx = read_block_idx++;
		if (block_idx >= rows->blocks.size()) {
			return nullptr;
		}
		//	Second pass can flush
		--gsource.tasks_remaining;
		return make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, block_idx, true);
	}
	return nullptr;
}

void WindowPartitionSourceState::BuildPartition(WindowGlobalSinkState &gstate, const idx_t hash_bin_p) {
	//	Get rid of any stale data
	hash_bin = hash_bin_p;

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)

	//	How big is the partition?
	auto &gpart = *gsource.gsink.global_partition;
	idx_t count = 0;
	if (hash_bin < gpart.hash_groups.size() && gpart.hash_groups[hash_bin]) {
		count = gpart.hash_groups[hash_bin]->count;
	} else if (gpart.rows && !hash_bin) {
		count = gpart.count;
	} else {
		return;
	}

	//	Initialise masks to false
	const auto bit_count = ValidityMask::ValidityMaskSize(count);
	partition_bits.clear();
	partition_bits.resize(bit_count, 0);
	partition_mask.Initialize(partition_bits.data());

	order_bits.clear();
	order_bits.resize(bit_count, 0);
	order_mask.Initialize(order_bits.data());

	// Scan the sorted data into new Collections
	external = gpart.external;
	if (gpart.rows && !hash_bin) {
		// Simple mask
		partition_mask.SetValidUnsafe(0);
		order_mask.SetValidUnsafe(0);
		//	No partition - align the heap blocks with the row blocks
		rows = gpart.rows->CloneEmpty(gpart.rows->keep_pinned);
		heap = gpart.strings->CloneEmpty(gpart.strings->keep_pinned);
		RowDataCollectionScanner::AlignHeapBlocks(*rows, *heap, *gpart.rows, *gpart.strings, layout);
		external = true;
	} else if (hash_bin < gpart.hash_groups.size()) {
		// Overwrite the collections with the sorted data
		D_ASSERT(gpart.hash_groups[hash_bin].get());
		hash_group = std::move(gpart.hash_groups[hash_bin]);
		hash_group->ComputeMasks(partition_mask, order_mask);
		external = hash_group->global_sort->external;
		MaterializeSortedData();
	} else {
		return;
	}

	// Create the executors for each function
	executors.clear();
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		auto wexec = WindowExecutorFactory(wexpr, context, partition_mask, order_mask, count, gstate.mode);
		executors.emplace_back(std::move(wexec));
	}

	//	First pass over the input without flushing
	DataChunk input_chunk;
	input_chunk.Initialize(gpart.allocator, gpart.payload_types);
	auto scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, external, false);
	idx_t input_idx = 0;
	while (true) {
		input_chunk.Reset();
		scanner->Scan(input_chunk);
		if (input_chunk.size() == 0) {
			break;
		}

		//	TODO: Parallelization opportunity
		for (auto &wexec : executors) {
			wexec->Sink(input_chunk, input_idx, scanner->Count());
		}
		input_idx += input_chunk.size();
	}

	//	TODO: Parallelization opportunity
	for (auto &wexec : executors) {
		wexec->Finalize();
	}

	// External scanning assumes all blocks are swizzled.
	scanner->ReSwizzle();

	//	Start the block countdown
	unscanned = rows->blocks.size();
}

// Per-thread scan state
class WindowLocalSourceState : public LocalSourceState {
public:
	using ReadStatePtr = unique_ptr<WindowExecutorState>;
	using ReadStates = vector<ReadStatePtr>;

	explicit WindowLocalSourceState(WindowGlobalSourceState &gsource);
	void UpdateBatchIndex();
	bool NextPartition();
	void Scan(DataChunk &chunk);

	//! The shared source state
	WindowGlobalSourceState &gsource;
	//! The current bin being processed
	idx_t hash_bin;
	//! The current batch index (for output reordering)
	idx_t batch_index;
	//! The current source being processed
	optional_ptr<WindowPartitionSourceState> partition_source;
	//! The read cursor
	unique_ptr<RowDataCollectionScanner> scanner;
	//! Buffer for the inputs
	DataChunk input_chunk;
	//! Executor read states.
	ReadStates read_states;
	//! Buffer for window results
	DataChunk output_chunk;
};

WindowLocalSourceState::WindowLocalSourceState(WindowGlobalSourceState &gsource)
    : gsource(gsource), hash_bin(gsource.built.size()), batch_index(0) {
	auto &gsink = *gsource.gsink.global_partition;
	auto &op = gsource.gsink.op;

	input_chunk.Initialize(gsink.allocator, gsink.payload_types);

	vector<LogicalType> output_types;
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto &wexpr = op.select_list[expr_idx]->Cast<BoundWindowExpression>();
		output_types.emplace_back(wexpr.return_type);
	}
	output_chunk.Initialize(Allocator::Get(gsource.context), output_types);
}

WindowGlobalSourceState::Task WindowGlobalSourceState::CreateTask(idx_t hash_bin) {
	//	Build outside the lock so no one tries to steal before we are done.
	auto partition_source = make_uniq<WindowPartitionSourceState>(context, *this);
	partition_source->BuildPartition(gsink, hash_bin);
	Task result(partition_source.get(), partition_source->GetScanner());

	//	Is there any data to scan?
	if (result.second) {
		lock_guard<mutex> built_guard(built_lock);
		built[hash_bin] = std::move(partition_source);

		return result;
	}

	return Task();
}

WindowGlobalSourceState::Task WindowGlobalSourceState::StealWork() {
	for (idx_t hash_bin = 0; hash_bin < built.size(); ++hash_bin) {
		lock_guard<mutex> built_guard(built_lock);
		auto &partition_source = built[hash_bin];
		if (!partition_source) {
			continue;
		}

		Task result(partition_source.get(), partition_source->GetScanner());

		//	Is there any data to scan?
		if (result.second) {
			return result;
		}
	}

	//	Nothing to steal
	return Task();
}

WindowGlobalSourceState::Task WindowGlobalSourceState::NextTask(idx_t hash_bin) {
	auto &hash_groups = gsink.global_partition->hash_groups;
	const auto bin_count = built.size();

	//	Flush unneeded data
	if (hash_bin < bin_count) {
		//	Lock and delete when all blocks have been scanned
		//	We do this here instead of in NextScan so the WindowLocalSourceState
		//	has a chance to delete its state objects first,
		//	which may reference the partition_source

		//	Delete data outside the lock in case it is slow
		HashGroupSourcePtr killed;
		lock_guard<mutex> built_guard(built_lock);
		auto &partition_source = built[hash_bin];
		if (partition_source && !partition_source->unscanned) {
			killed = std::move(partition_source);
		}
	}

	hash_bin = next_build++;
	if (hash_bin < bin_count) {
		//	Find a non-empty hash group.
		for (; hash_bin < hash_groups.size(); hash_bin = next_build++) {
			if (hash_groups[hash_bin] && hash_groups[hash_bin]->count) {
				auto result = CreateTask(hash_bin);
				if (result.second) {
					return result;
				}
			}
		}

		//	OVER() doesn't have a hash_group
		if (hash_groups.empty()) {
			auto result = CreateTask(hash_bin);
			if (result.second) {
				return result;
			}
		}
	}

	//	Work stealing
	while (!context.interrupted && tasks_remaining) {
		auto result = StealWork();
		if (result.second) {
			return result;
		}

		//	If there is nothing to steal but there are unfinished partitions,
		//	yield until any pending builds are done.
		TaskScheduler::YieldThread();
	}

	return Task();
}

void WindowLocalSourceState::UpdateBatchIndex() {
	D_ASSERT(partition_source);
	D_ASSERT(scanner.get());

	batch_index = partition_source->hash_group ? partition_source->hash_group->batch_base : 0;
	batch_index += scanner->BlockIndex();
}

bool WindowLocalSourceState::NextPartition() {
	//	Release old states before the source
	scanner.reset();
	read_states.clear();

	//	Get a partition_source that is not finished
	while (!scanner) {
		auto task = gsource.NextTask(hash_bin);
		if (!task.first) {
			return false;
		}
		partition_source = task.first;
		scanner = std::move(task.second);
		hash_bin = partition_source->hash_bin;
		UpdateBatchIndex();
	}

	for (auto &wexec : partition_source->executors) {
		read_states.emplace_back(wexec->GetExecutorState());
	}

	return true;
}

void WindowLocalSourceState::Scan(DataChunk &result) {
	D_ASSERT(scanner);
	if (!scanner->Remaining()) {
		lock_guard<mutex> built_guard(gsource.built_lock);
		--partition_source->unscanned;
		scanner = partition_source->GetScanner();

		if (!scanner) {
			partition_source = nullptr;
			read_states.clear();
			return;
		}

		UpdateBatchIndex();
	}

	const auto position = scanner->Scanned();
	input_chunk.Reset();
	scanner->Scan(input_chunk);

	auto &executors = partition_source->executors;
	output_chunk.Reset();
	for (idx_t expr_idx = 0; expr_idx < executors.size(); ++expr_idx) {
		auto &executor = *executors[expr_idx];
		auto &lstate = *read_states[expr_idx];
		auto &result = output_chunk.data[expr_idx];
		executor.Evaluate(position, input_chunk, result, lstate);
	}
	output_chunk.SetCardinality(input_chunk);
	output_chunk.Verify();

	idx_t out_idx = 0;
	result.SetCardinality(input_chunk);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(input_chunk.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < output_chunk.ColumnCount(); col_idx++) {
		result.data[out_idx++].Reference(output_chunk.data[col_idx]);
	}
	result.Verify();
}

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gsource_p) const {
	auto &gsource = gsource_p.Cast<WindowGlobalSourceState>();
	return make_uniq<WindowLocalSourceState>(gsource);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	auto &gsink = sink_state->Cast<WindowGlobalSinkState>();
	return make_uniq<WindowGlobalSourceState>(context, gsink);
}

bool PhysicalWindow::SupportsBatchIndex() const {
	//	We can only preserve order for single partitioning
	//	or work stealing causes out of order batch numbers
	auto &wexpr = select_list[0]->Cast<BoundWindowExpression>();
	return wexpr.partitions.empty() && !wexpr.orders.empty();
}

OrderPreservationType PhysicalWindow::SourceOrder() const {
	return SupportsBatchIndex() ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::NO_ORDER;
}

idx_t PhysicalWindow::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                    LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<WindowLocalSourceState>();
	return lstate.batch_index;
}

SourceResultType PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &lsource = input.local_state.Cast<WindowLocalSourceState>();
	while (chunk.size() == 0) {
		//	Move to the next bin if we are done.
		while (!lsource.scanner) {
			if (!lsource.NextPartition()) {
				return chunk.size() > 0 ? SourceResultType::HAVE_MORE_OUTPUT : SourceResultType::FINISHED;
			}
		}

		lsource.Scan(chunk);
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb























#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

string BaseCSVReader::GetLineNumberStr(idx_t line_error, bool is_line_estimated, idx_t buffer_idx) {
	// If an error happens during auto-detect it is an estimated line
	string estimated = (is_line_estimated ? string(" (estimated)") : string(""));
	return to_string(GetLineError(line_error, buffer_idx)) + estimated;
}

BaseCSVReader::BaseCSVReader(ClientContext &context_p, CSVReaderOptions options_p,
                             const vector<LogicalType> &requested_types)
    : context(context_p), fs(FileSystem::GetFileSystem(context)), allocator(BufferAllocator::Get(context)),
      options(std::move(options_p)) {
}

BaseCSVReader::~BaseCSVReader() {
}

unique_ptr<CSVFileHandle> BaseCSVReader::OpenCSV(ClientContext &context, const CSVReaderOptions &options_p) {
	return CSVFileHandle::OpenFile(FileSystem::GetFileSystem(context), BufferAllocator::Get(context),
	                               options_p.file_path, options_p.compression);
}

void BaseCSVReader::InitParseChunk(idx_t num_cols) {
	// adapt not null info
	if (options.force_not_null.size() != num_cols) {
		options.force_not_null.resize(num_cols, false);
	}
	if (num_cols == parse_chunk.ColumnCount()) {
		parse_chunk.Reset();
	} else {
		parse_chunk.Destroy();

		// initialize the parse_chunk with a set of VARCHAR types
		vector<LogicalType> varchar_types(num_cols, LogicalType::VARCHAR);
		parse_chunk.Initialize(allocator, varchar_types);
	}
}

void BaseCSVReader::InitializeProjection() {
	for (idx_t i = 0; i < GetTypes().size(); i++) {
		reader_data.column_ids.push_back(i);
		reader_data.column_mapping.push_back(i);
	}
}

template <class OP, class T>
static bool TemplatedTryCastDateVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                       Vector &result_vector, idx_t count, string &error_message, idx_t &line_error) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	idx_t cur_line = 0;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(options, input, result, error_message)) {
			line_error = cur_line;
			all_converted = false;
		}
		cur_line++;
		return result;
	});
	return all_converted;
}

struct TryCastDateOperator {
	static bool Operation(map<LogicalTypeId, StrpTimeFormat> &options, string_t input, date_t &result,
	                      string &error_message) {
		return options[LogicalTypeId::DATE].TryParseDate(input, result, error_message);
	}
};

struct TryCastTimestampOperator {
	static bool Operation(map<LogicalTypeId, StrpTimeFormat> &options, string_t input, timestamp_t &result,
	                      string &error_message) {
		return options[LogicalTypeId::TIMESTAMP].TryParseTimestamp(input, result, error_message);
	}
};

bool BaseCSVReader::TryCastDateVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                      Vector &result_vector, idx_t count, string &error_message, idx_t &line_error) {
	return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
	                                                               error_message, line_error);
}

bool BaseCSVReader::TryCastTimestampVector(map<LogicalTypeId, StrpTimeFormat> &options, Vector &input_vector,
                                           Vector &result_vector, idx_t count, string &error_message) {
	idx_t line_error;
	return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
	                                                                         count, error_message, line_error);
}

void BaseCSVReader::VerifyLineLength(idx_t line_size, idx_t buffer_idx) {
	if (line_size > options.maximum_line_size) {
		throw InvalidInputException(
		    "Error in file \"%s\" on line %s: Maximum line size of %llu bytes exceeded!", options.file_path,
		    GetLineNumberStr(parse_chunk.size(), linenr_estimated, buffer_idx).c_str(), options.maximum_line_size);
	}
}

template <class OP, class T>
bool TemplatedTryCastFloatingVector(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                                    string &error_message, idx_t &line_error) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	idx_t row = 0;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(input, result, &error_message)) {
			line_error = row;
			all_converted = false;
		} else {
			row++;
		}
		return result;
	});
	return all_converted;
}

template <class OP, class T>
bool TemplatedTryCastDecimalVector(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                                   string &error_message, uint8_t width, uint8_t scale) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(input, result, &error_message, width, scale)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

void BaseCSVReader::AddValue(string_t str_val, idx_t &column, vector<idx_t> &escape_positions, bool has_quotes,
                             idx_t buffer_idx) {
	auto length = str_val.GetSize();
	if (length == 0 && column == 0) {
		row_empty = true;
	} else {
		row_empty = false;
	}
	if (!return_types.empty() && column == return_types.size() && length == 0) {
		// skip a single trailing delimiter in last column
		return;
	}
	if (column >= return_types.size()) {
		if (options.ignore_errors) {
			error_column_overflow = true;
			return;
		} else {
			throw InvalidInputException(
			    "Error in file \"%s\", on line %s: expected %lld values per row, but got more. (%s)", options.file_path,
			    GetLineNumberStr(linenr, linenr_estimated, buffer_idx).c_str(), return_types.size(),
			    options.ToString());
		}
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	// test against null string, but only if the value was not quoted
	if ((!(has_quotes && !options.allow_quoted_nulls) || return_types[column].id() != LogicalTypeId::VARCHAR) &&
	    !options.force_not_null[column] && Equals::Operation(str_val, string_t(options.null_str))) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (!escape_positions.empty()) {
			// remove escape characters (if any)
			string old_val = str_val.GetString();
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);
				prev_pos = ++next_pos;
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddStringOrBlob(v, string_t(new_val));
		} else {
			parse_data[row_entry] = str_val;
		}
	}

	// move to the next column
	column++;
}

bool BaseCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column, string &error_message, idx_t buffer_idx) {
	linenr++;

	if (row_empty) {
		row_empty = false;
		if (return_types.size() != 1) {
			if (mode == ParserMode::PARSING) {
				FlatVector::SetNull(parse_chunk.data[0], parse_chunk.size(), false);
			}
			column = 0;
			return false;
		}
	}

	// Error forwarded by 'ignore_errors' - originally encountered in 'AddValue'
	if (error_column_overflow) {
		D_ASSERT(options.ignore_errors);
		error_column_overflow = false;
		column = 0;
		return false;
	}

	if (column < return_types.size()) {
		if (options.null_padding) {
			for (; column < return_types.size(); column++) {
				FlatVector::SetNull(parse_chunk.data[column], parse_chunk.size(), true);
			}
		} else if (options.ignore_errors) {
			column = 0;
			return false;
		} else {
			if (mode == ParserMode::SNIFFING_DATATYPES) {
				error_message = "Error when adding line";
				return false;
			} else {
				throw InvalidInputException(
				    "Error in file \"%s\" on line %s: expected %lld values per row, but got %d.\nParser options:\n%s",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated, buffer_idx).c_str(),
				    return_types.size(), column, options.ToString());
			}
		}
	}

	parse_chunk.SetCardinality(parse_chunk.size() + 1);

	if (mode == ParserMode::PARSING_HEADER) {
		return true;
	}

	if (mode == ParserMode::SNIFFING_DATATYPES) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk, buffer_idx);
		return true;
	}

	column = 0;
	return false;
}

void BaseCSVReader::VerifyUTF8(idx_t col_idx, idx_t row_idx, DataChunk &chunk, int64_t offset) {
	D_ASSERT(col_idx < chunk.data.size());
	D_ASSERT(row_idx < chunk.size());
	auto &v = chunk.data[col_idx];
	if (FlatVector::IsNull(v, row_idx)) {
		return;
	}

	auto parse_data = FlatVector::GetData<string_t>(chunk.data[col_idx]);
	auto s = parse_data[row_idx];
	auto utf_type = Utf8Proc::Analyze(s.GetData(), s.GetSize());
	if (utf_type == UnicodeType::INVALID) {
		string col_name = to_string(col_idx);
		if (col_idx < names.size()) {
			col_name = "\"" + names[col_idx] + "\"";
		}
		int64_t error_line = linenr - (chunk.size() - row_idx) + 1 + offset;
		D_ASSERT(error_line >= 0);
		throw InvalidInputException("Error in file \"%s\" at line %llu in column \"%s\": "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, col_name,
		                            ErrorManager::InvalidUnicodeError(s.GetString(), "CSV file"), options.ToString());
	}
}

void BaseCSVReader::VerifyUTF8(idx_t col_idx) {
	D_ASSERT(col_idx < parse_chunk.data.size());
	for (idx_t i = 0; i < parse_chunk.size(); i++) {
		VerifyUTF8(col_idx, i, parse_chunk);
	}
}

bool TryCastDecimalVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                        idx_t count, string &error_message, const LogicalType &result_type) {
	auto width = DecimalType::GetWidth(result_type);
	auto scale = DecimalType::GetScale(result_type);
	switch (result_type.InternalType()) {
	case PhysicalType::INT16:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int16_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT32:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int32_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT64:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int64_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	case PhysicalType::INT128:
		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, hugeint_t>(
		    options, input_vector, result_vector, count, error_message, width, scale);
	default:
		throw InternalException("Unimplemented physical type for decimal");
	}
}

bool TryCastFloatingVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                         idx_t count, string &error_message, const LogicalType &result_type,
                                         idx_t &line_error) {
	switch (result_type.InternalType()) {
	case PhysicalType::DOUBLE:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, double>(
		    options, input_vector, result_vector, count, error_message, line_error);
	case PhysicalType::FLOAT:
		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, float>(
		    options, input_vector, result_vector, count, error_message, line_error);
	default:
		throw InternalException("Unimplemented physical type for floating");
	}
}

// Location of erroneous value in the current parse chunk
struct ErrorLocation {
	idx_t row_idx;
	idx_t col_idx;
	idx_t row_line;

	ErrorLocation(idx_t row_idx, idx_t col_idx, idx_t row_line)
	    : row_idx(row_idx), col_idx(col_idx), row_line(row_line) {
	}
};

bool BaseCSVReader::Flush(DataChunk &insert_chunk, idx_t buffer_idx, bool try_add_line) {
	if (parse_chunk.size() == 0) {
		return true;
	}

	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	if (reader_data.column_ids.empty() && !reader_data.empty_columns) {
		throw InternalException("BaseCSVReader::Flush called on a CSV reader that was not correctly initialized. Call "
		                        "MultiFileReader::InitializeReader or InitializeProjection");
	}
	D_ASSERT(reader_data.column_ids.size() == reader_data.column_mapping.size());
	for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
		auto col_idx = reader_data.column_ids[c];
		auto result_idx = reader_data.column_mapping[c];
		auto &parse_vector = parse_chunk.data[col_idx];
		auto &result_vector = insert_chunk.data[result_idx];
		auto &type = result_vector.GetType();
		if (type.id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			VerifyUTF8(col_idx);
			// reinterpret rather than reference so we can deal with user-defined types
			result_vector.Reinterpret(parse_vector);
		} else {
			string error_message;
			bool success;
			idx_t line_error = 0;
			bool target_type_not_varchar = false;
			if (options.dialect_options.has_format[LogicalTypeId::DATE] && type.id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = TryCastDateVector(options.dialect_options.date_format, parse_vector, result_vector,
				                            parse_chunk.size(), error_message, line_error);
			} else if (options.dialect_options.has_format[LogicalTypeId::TIMESTAMP] &&
			           type.id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = TryCastTimestampVector(options.dialect_options.date_format, parse_vector, result_vector,
				                                 parse_chunk.size(), error_message);
			} else if (options.decimal_separator != "." &&
			           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
				success = TryCastFloatingVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
				                                              error_message, type, line_error);
			} else if (options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
				success = TryCastDecimalVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
				                                             error_message, type);
			} else {
				// target type is not varchar: perform a cast
				target_type_not_varchar = true;
				success =
				    VectorOperations::TryCast(context, parse_vector, result_vector, parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			if (try_add_line) {
				return false;
			}

			string col_name = to_string(col_idx);
			if (col_idx < names.size()) {
				col_name = "\"" + names[col_idx] + "\"";
			}

			// figure out the exact line number
			if (target_type_not_varchar) {
				UnifiedVectorFormat inserted_column_data;
				result_vector.ToUnifiedFormat(parse_chunk.size(), inserted_column_data);
				for (; line_error < parse_chunk.size(); line_error++) {
					if (!inserted_column_data.validity.RowIsValid(line_error) &&
					    !FlatVector::IsNull(parse_vector, line_error)) {
						break;
					}
				}
			}

			// The line_error must be summed with linenr (All lines emmited from this batch)
			// But subtracted from the parse_chunk
			D_ASSERT(line_error + linenr >= parse_chunk.size());
			line_error += linenr;
			line_error -= parse_chunk.size();

			auto error_line = GetLineError(line_error, buffer_idx);

			if (options.ignore_errors) {
				conversion_error_ignored = true;

			} else if (options.auto_detect) {
				throw InvalidInputException("%s in column %s, at line %llu.\n\nParser "
				                            "options:\n%s.\n\nConsider either increasing the sample size "
				                            "(SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), "
				                            "or skipping column conversion (ALL_VARCHAR=1)",
				                            error_message, col_name, error_line, options.ToString());
			} else {
				throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ", error_message,
				                            error_line, col_name, options.ToString());
			}
		}
	}
	if (conversion_error_ignored) {
		D_ASSERT(options.ignore_errors);

		SelectionVector succesful_rows(parse_chunk.size());
		idx_t sel_size = 0;

		// Keep track of failed cells
		vector<ErrorLocation> failed_cells;

		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {

			auto global_row_idx = row_idx + linenr - parse_chunk.size();
			auto row_line = GetLineError(global_row_idx, buffer_idx, false);

			bool row_failed = false;
			for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
				auto col_idx = reader_data.column_ids[c];
				auto result_idx = reader_data.column_mapping[c];

				auto &parse_vector = parse_chunk.data[col_idx];
				auto &result_vector = insert_chunk.data[result_idx];

				bool was_already_null = FlatVector::IsNull(parse_vector, row_idx);
				if (!was_already_null && FlatVector::IsNull(result_vector, row_idx)) {
					Increment(buffer_idx);
					auto bla = GetLineError(global_row_idx, buffer_idx, false);
					row_idx += bla;
					row_idx -= bla;
					row_failed = true;
					failed_cells.emplace_back(row_idx, col_idx, row_line);
				}
			}
			if (!row_failed) {
				succesful_rows.set_index(sel_size++, row_idx);
			}
		}

		// Now do a second pass to produce the reject table entries
		if (!failed_cells.empty() && !options.rejects_table_name.empty()) {
			auto limit = options.rejects_limit;

			auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_table_name);
			lock_guard<mutex> lock(rejects->write_lock);

			// short circuit if we already have too many rejects
			if (limit == 0 || rejects->count < limit) {
				auto &table = rejects->GetTable(context);
				InternalAppender appender(context, table);
				auto file_name = GetFileName();

				for (auto &cell : failed_cells) {
					if (limit != 0 && rejects->count >= limit) {
						break;
					}
					rejects->count++;

					auto row_idx = cell.row_idx;
					auto col_idx = cell.col_idx;
					auto row_line = cell.row_line;

					auto col_name = to_string(col_idx);
					if (col_idx < names.size()) {
						col_name = "\"" + names[col_idx] + "\"";
					}

					auto &parse_vector = parse_chunk.data[col_idx];
					auto parsed_str = FlatVector::GetData<string_t>(parse_vector)[row_idx];
					auto &type = insert_chunk.data[col_idx].GetType();
					auto row_error_msg = StringUtil::Format("Could not convert string '%s' to '%s'",
					                                        parsed_str.GetString(), type.ToString());

					// Add the row to the rejects table
					appender.BeginRow();
					appender.Append(string_t(file_name));
					appender.Append(row_line);
					appender.Append(col_idx);
					appender.Append(string_t(col_name));
					appender.Append(parsed_str);

					if (!options.rejects_recovery_columns.empty()) {
						child_list_t<Value> recovery_key;
						for (auto &key_idx : options.rejects_recovery_column_ids) {
							// Figure out if the recovery key is valid.
							// If not, error out for real.
							auto &component_vector = parse_chunk.data[key_idx];
							if (FlatVector::IsNull(component_vector, row_idx)) {
								throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ",
								                            "Could not parse recovery column", row_line, col_name,
								                            options.ToString());
							}
							auto component = Value(FlatVector::GetData<string_t>(component_vector)[row_idx]);
							recovery_key.emplace_back(names[key_idx], component);
						}
						appender.Append(Value::STRUCT(recovery_key));
					}

					appender.Append(string_t(row_error_msg));
					appender.EndRow();
				}
				appender.Close();
			}
		}

		// Now slice the insert chunk to only include the succesful rows
		insert_chunk.Slice(succesful_rows, sel_size);
	}
	parse_chunk.Reset();
	return true;
}

void BaseCSVReader::SetNewLineDelimiter(bool carry, bool carry_followed_by_nl) {
	if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
		if (options.dialect_options.new_line == NewLineIdentifier::MIX) {
			return;
		}
		NewLineIdentifier this_line_identifier;
		if (carry) {
			if (carry_followed_by_nl) {
				this_line_identifier = NewLineIdentifier::CARRY_ON;
			} else {
				this_line_identifier = NewLineIdentifier::SINGLE;
			}
		} else {
			this_line_identifier = NewLineIdentifier::SINGLE;
		}
		if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
			options.dialect_options.new_line = this_line_identifier;
			return;
		}
		if (options.dialect_options.new_line != this_line_identifier) {
			options.dialect_options.new_line = NewLineIdentifier::MIX;
			return;
		}
		options.dialect_options.new_line = this_line_identifier;
	}
}
} // namespace duckdb





















#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

BufferedCSVReader::BufferedCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BaseCSVReader(context, std::move(options_p), requested_types), buffer_size(0), position(0), start(0) {
	file_handle = OpenCSV(context, options);
	Initialize(requested_types);
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, string filename, CSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BaseCSVReader(context, std::move(options_p), requested_types), buffer_size(0), position(0), start(0) {
	options.file_path = std::move(filename);
	file_handle = OpenCSV(context, options);
	Initialize(requested_types);
}

void BufferedCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	if (options.auto_detect && options.file_options.union_by_name) {
		// This is required for the sniffer to work on Union By Name
		D_ASSERT(options.file_path == file_handle->GetFilePath());
		auto bm_file_handle = BaseCSVReader::OpenCSV(context, options);
		auto csv_buffer_manager = make_shared<CSVBufferManager>(context, std::move(bm_file_handle), options);
		CSVSniffer sniffer(options, csv_buffer_manager, state_machine_cache);
		auto sniffer_result = sniffer.SniffCSV();
		return_types = sniffer_result.return_types;
		names = sniffer_result.names;
		if (return_types.empty()) {
			throw InvalidInputException("Failed to detect column types from CSV: is the file a valid CSV file?");
		}
	} else {
		return_types = requested_types;
		ResetBuffer();
	}
	SkipRowsAndReadHeader(options.dialect_options.skip_rows, options.dialect_options.header);
	InitParseChunk(return_types.size());
}

void BufferedCSVReader::ResetBuffer() {
	buffer.reset();
	buffer_size = 0;
	position = 0;
	start = 0;
	cached_buffers.clear();
}

void BufferedCSVReader::SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header) {
	for (idx_t i = 0; i < skip_rows; i++) {
		// ignore skip rows
		string read_line = file_handle->ReadLine();
		linenr++;
	}

	if (skip_header) {
		// ignore the first line as a header line
		InitParseChunk(return_types.size());
		ParseCSV(ParserMode::PARSING_HEADER);
	}
}

string BufferedCSVReader::ColumnTypesError(case_insensitive_map_t<idx_t> sql_types_per_column,
                                           const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		auto it = sql_types_per_column.find(names[i]);
		if (it != sql_types_per_column.end()) {
			sql_types_per_column.erase(names[i]);
			continue;
		}
	}
	if (sql_types_per_column.empty()) {
		return string();
	}
	string exception = "COLUMN_TYPES error: Columns with names: ";
	for (auto &col : sql_types_per_column) {
		exception += "\"" + col.first + "\",";
	}
	exception.pop_back();
	exception += " do not exist in the CSV File";
	return exception;
}

void BufferedCSVReader::SkipEmptyLines() {
	if (parse_chunk.data.size() == 1) {
		// Empty lines are null data.
		return;
	}
	for (; position < buffer_size; position++) {
		if (!StringUtil::CharacterIsNewline(buffer[position])) {
			return;
		}
	}
}

void UpdateMaxLineLength(ClientContext &context, idx_t line_length) {
	if (!context.client_data->debug_set_max_line_length) {
		return;
	}
	if (line_length < context.client_data->debug_max_line_length) {
		return;
	}
	context.client_data->debug_max_line_length = line_length;
}

bool BufferedCSVReader::ReadBuffer(idx_t &start, idx_t &line_start) {
	if (start > buffer_size) {
		return false;
	}
	auto old_buffer = std::move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;

	idx_t buffer_read_size = INITIAL_BUFFER_SIZE_LARGE;

	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}

	// Check line length
	if (remaining > options.maximum_line_size) {
		throw InvalidInputException("Maximum line size of %llu bytes exceeded on line %s!", options.maximum_line_size,
		                            GetLineNumberStr(linenr, linenr_estimated));
	}

	buffer = make_unsafe_uniq_array<char>(buffer_read_size + remaining + 1);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	idx_t read_count = file_handle->Read(buffer.get() + remaining, buffer_read_size);

	bytes_in_chunk += read_count;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(std::move(old_buffer));
	}
	start = 0;
	position = remaining;
	if (!bom_checked) {
		bom_checked = true;
		if (read_count >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
			start += 3;
			position += 3;
		}
	}
	line_start = start;

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

void BufferedCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	bool has_quotes = false;
	vector<idx_t> escape_positions;

	idx_t line_start = position;
	idx_t line_size = 0;
	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start, line_start)) {
			return true;
		}
	}

	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == options.dialect_options.state_machine_options.quote) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start = position + 1;
		line_size++;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start = position;
		goto normal;
	}
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	do {
		for (; position < buffer_size; position++) {
			line_size++;
			if (buffer[position] == options.dialect_options.state_machine_options.delimiter) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				// newline: add row
				goto add_row;
			}
		}
	} while (ReadBuffer(start, line_start));
	// file ends during normal scan: go to end state
	goto final_state;
add_value:
	AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	start = ++position;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
	if (!error_message.empty()) {
		return false;
	}
	VerifyLineLength(position - line_start);

	finished_chunk = AddRow(insert_chunk, column, error_message);
	UpdateMaxLineLength(context, position - line_start);
	if (!error_message.empty()) {
		return false;
	}
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	position++;
	line_size = 0;
	start = position;
	line_start = position;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		SetNewLineDelimiter();
		SkipEmptyLines();

		start = position;
		line_start = position;
		if (position >= buffer_size && !ReadBuffer(start, line_start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
		// \n newline, move to value start
		if (finished_chunk) {
			return true;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	has_quotes = true;
	position++;
	line_size++;
	do {
		for (; position < buffer_size; position++) {
			line_size++;
			if (buffer[position] == options.dialect_options.state_machine_options.quote) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == options.dialect_options.state_machine_options.escape) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start, line_start));
	// still in quoted state at the end of the file, error:
	throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                            GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position++;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after unquote, go to final state
		offset = 1;
		goto final_state;
	}
	if (buffer[position] == options.dialect_options.state_machine_options.quote &&
	    (options.dialect_options.state_machine_options.escape == '\0' ||
	     options.dialect_options.state_machine_options.escape == options.dialect_options.state_machine_options.quote)) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == options.dialect_options.state_machine_options.delimiter) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s)",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	line_size++;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	if (buffer[position] != options.dialect_options.state_machine_options.quote &&
	    buffer[position] != options.dialect_options.state_machine_options.escape) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		SetNewLineDelimiter(true, true);
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start = ++position;
		line_size++;

		if (position >= buffer_size && !ReadBuffer(start, line_start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
	} else {
		SetNewLineDelimiter(true, false);
	}
	if (finished_chunk) {
		return true;
	}
	SkipEmptyLines();
	start = position;
	line_start = position;
	if (position >= buffer_size && !ReadBuffer(start, line_start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}

	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
	}

	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(string_t(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
		VerifyLineLength(position - line_start);

		finished_chunk = AddRow(insert_chunk, column, error_message);
		SkipEmptyLines();
		UpdateMaxLineLength(context, line_size);
		if (!error_message.empty()) {
			return false;
		}
	}

	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}

	end_of_file_reached = true;
	return true;
}

} // namespace duckdb



namespace duckdb {

CSVBuffer::CSVBuffer(ClientContext &context, idx_t buffer_size_p, CSVFileHandle &file_handle,
                     idx_t &global_csv_current_position, idx_t file_number_p)
    : context(context), first_buffer(true), file_number(file_number_p), can_seek(file_handle.CanSeek()) {
	AllocateBuffer(buffer_size_p);
	auto buffer = Ptr();
	actual_buffer_size = file_handle.Read(buffer, buffer_size_p);
	while (actual_buffer_size < buffer_size_p && !file_handle.FinishedReading()) {
		// We keep reading until this block is full
		actual_buffer_size += file_handle.Read(&buffer[actual_buffer_size], buffer_size_p - actual_buffer_size);
	}
	global_csv_start = global_csv_current_position;
	// BOM check (https://en.wikipedia.org/wiki/Byte_order_mark)
	if (actual_buffer_size >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
		start_position += 3;
	}
	last_buffer = file_handle.FinishedReading();
}

CSVBuffer::CSVBuffer(CSVFileHandle &file_handle, ClientContext &context, idx_t buffer_size,
                     idx_t global_csv_current_position, idx_t file_number_p)
    : context(context), global_csv_start(global_csv_current_position), file_number(file_number_p),
      can_seek(file_handle.CanSeek()) {
	AllocateBuffer(buffer_size);
	auto buffer = handle.Ptr();
	actual_buffer_size = file_handle.Read(handle.Ptr(), buffer_size);
	while (actual_buffer_size < buffer_size && !file_handle.FinishedReading()) {
		// We keep reading until this block is full
		actual_buffer_size += file_handle.Read(&buffer[actual_buffer_size], buffer_size - actual_buffer_size);
	}
	last_buffer = file_handle.FinishedReading();
}

shared_ptr<CSVBuffer> CSVBuffer::Next(CSVFileHandle &file_handle, idx_t buffer_size, idx_t file_number_p) {
	auto next_csv_buffer =
	    make_shared<CSVBuffer>(file_handle, context, buffer_size, global_csv_start + actual_buffer_size, file_number_p);
	if (next_csv_buffer->GetBufferSize() == 0) {
		// We are done reading
		return nullptr;
	}
	return next_csv_buffer;
}

void CSVBuffer::AllocateBuffer(idx_t buffer_size) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	bool can_destroy = can_seek;
	handle = buffer_manager.Allocate(MaxValue<idx_t>(Storage::BLOCK_SIZE, buffer_size), can_destroy, &block);
}

idx_t CSVBuffer::GetBufferSize() {
	return actual_buffer_size;
}

void CSVBuffer::Reload(CSVFileHandle &file_handle) {
	AllocateBuffer(actual_buffer_size);
	file_handle.Seek(global_csv_start);
	file_handle.Read(handle.Ptr(), actual_buffer_size);
}

unique_ptr<CSVBufferHandle> CSVBuffer::Pin(CSVFileHandle &file_handle) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	if (can_seek && block->IsUnloaded()) {
		// We have to reload it from disk
		block = nullptr;
		Reload(file_handle);
	}
	return make_uniq<CSVBufferHandle>(buffer_manager.Pin(block), actual_buffer_size, first_buffer, last_buffer,
	                                  global_csv_start, start_position, file_number);
}

void CSVBuffer::Unpin() {
	if (handle.IsValid()) {
		handle.Destroy();
	}
}

idx_t CSVBuffer::GetStart() {
	return start_position;
}

bool CSVBuffer::IsCSVFileLastBuffer() {
	return last_buffer;
}

} // namespace duckdb


namespace duckdb {

CSVBufferManager::CSVBufferManager(ClientContext &context_p, unique_ptr<CSVFileHandle> file_handle_p,
                                   const CSVReaderOptions &options, idx_t file_idx_p)
    : file_handle(std::move(file_handle_p)), context(context_p), file_idx(file_idx_p),
      buffer_size(CSVBuffer::CSV_BUFFER_SIZE) {
	if (options.skip_rows_set) {
		// Skip rows if they are set
		skip_rows = options.dialect_options.skip_rows;
	}
	auto file_size = file_handle->FileSize();
	if (file_size > 0 && file_size < buffer_size) {
		buffer_size = CSVBuffer::CSV_MINIMUM_BUFFER_SIZE;
	}
	if (options.buffer_size < buffer_size) {
		buffer_size = options.buffer_size;
	}
	for (idx_t i = 0; i < skip_rows; i++) {
		file_handle->ReadLine();
	}
	Initialize();
}

void CSVBufferManager::UnpinBuffer(idx_t cache_idx) {
	if (cache_idx < cached_buffers.size()) {
		cached_buffers[cache_idx]->Unpin();
	}
}

void CSVBufferManager::Initialize() {
	if (cached_buffers.empty()) {
		cached_buffers.emplace_back(
		    make_shared<CSVBuffer>(context, buffer_size, *file_handle, global_csv_pos, file_idx));
		last_buffer = cached_buffers.front();
	}
	start_pos = last_buffer->GetStart();
}

idx_t CSVBufferManager::GetStartPos() {
	return start_pos;
}
bool CSVBufferManager::ReadNextAndCacheIt() {
	D_ASSERT(last_buffer);
	if (!last_buffer->IsCSVFileLastBuffer()) {
		auto maybe_last_buffer = last_buffer->Next(*file_handle, buffer_size, file_idx);
		if (!maybe_last_buffer) {
			last_buffer->last_buffer = true;
			return false;
		}
		last_buffer = std::move(maybe_last_buffer);
		cached_buffers.emplace_back(last_buffer);
		return true;
	}
	return false;
}

unique_ptr<CSVBufferHandle> CSVBufferManager::GetBuffer(const idx_t pos) {
	while (pos >= cached_buffers.size()) {
		if (done) {
			return nullptr;
		}
		if (!ReadNextAndCacheIt()) {
			done = true;
		}
	}
	if (pos != 0) {
		cached_buffers[pos - 1]->Unpin();
	}
	return cached_buffers[pos]->Pin(*file_handle);
}

bool CSVBufferIterator::Finished() {
	return !cur_buffer_handle;
}

void CSVBufferIterator::Reset() {
	if (cur_buffer_handle) {
		cur_buffer_handle.reset();
	}
	if (cur_buffer_idx > 0) {
		buffer_manager->UnpinBuffer(cur_buffer_idx - 1);
	}
	cur_buffer_idx = 0;
	buffer_manager->Initialize();
	cur_pos = buffer_manager->GetStartPos();
}

} // namespace duckdb


namespace duckdb {

CSVFileHandle::CSVFileHandle(FileSystem &fs, Allocator &allocator, unique_ptr<FileHandle> file_handle_p,
                             const string &path_p, FileCompressionType compression)
    : file_handle(std::move(file_handle_p)), path(path_p) {
	can_seek = file_handle->CanSeek();
	on_disk_file = file_handle->OnDiskFile();
	file_size = file_handle->GetFileSize();
}

unique_ptr<FileHandle> CSVFileHandle::OpenFileHandle(FileSystem &fs, Allocator &allocator, const string &path,
                                                     FileCompressionType compression) {
	auto file_handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK, compression);
	if (file_handle->CanSeek()) {
		file_handle->Reset();
	}
	return file_handle;
}

unique_ptr<CSVFileHandle> CSVFileHandle::OpenFile(FileSystem &fs, Allocator &allocator, const string &path,
                                                  FileCompressionType compression) {
	auto file_handle = CSVFileHandle::OpenFileHandle(fs, allocator, path, compression);
	return make_uniq<CSVFileHandle>(fs, allocator, std::move(file_handle), path, compression);
}

bool CSVFileHandle::CanSeek() {
	return can_seek;
}

void CSVFileHandle::Seek(idx_t position) {
	if (!can_seek) {
		throw InternalException("Cannot seek in this file");
	}
	file_handle->Seek(position);
}

bool CSVFileHandle::OnDiskFile() {
	return on_disk_file;
}

idx_t CSVFileHandle::FileSize() {
	return file_size;
}

bool CSVFileHandle::FinishedReading() {
	return finished;
}

idx_t CSVFileHandle::Read(void *buffer, idx_t nr_bytes) {
	requested_bytes += nr_bytes;
	// if this is a plain file source OR we can seek we are not caching anything
	auto bytes_read = file_handle->Read(buffer, nr_bytes);
	if (!finished) {
		finished = bytes_read == 0;
	}
	return bytes_read;
}

string CSVFileHandle::ReadLine() {
	bool carriage_return = false;
	string result;
	char buffer[1];
	while (true) {
		idx_t bytes_read = Read(buffer, 1);
		if (bytes_read == 0) {
			return result;
		}
		if (carriage_return) {
			if (buffer[0] != '\n') {
				if (!file_handle->CanSeek()) {
					throw BinderException(
					    "Carriage return newlines not supported when reading CSV files in which we cannot seek");
				}
				file_handle->Seek(file_handle->SeekPosition() - 1);
				return result;
			}
		}
		if (buffer[0] == '\n') {
			return result;
		}
		if (buffer[0] != '\r') {
			result += buffer[0];
		} else {
			carriage_return = true;
		}
	}
}

string CSVFileHandle::GetFilePath() {
	return path;
}

} // namespace duckdb







namespace duckdb {

static bool ParseBoolean(const Value &value, const string &loption);

static bool ParseBoolean(const vector<Value> &set, const string &loption) {
	if (set.empty()) {
		// no option specified: default to true
		return true;
	}
	if (set.size() > 1) {
		throw BinderException("\"%s\" expects a single argument as a boolean value (e.g. TRUE or 1)", loption);
	}
	return ParseBoolean(set[0], loption);
}

static bool ParseBoolean(const Value &value, const string &loption) {

	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		return ParseBoolean(children, loption);
	}
	if (value.type() == LogicalType::FLOAT || value.type() == LogicalType::DOUBLE ||
	    value.type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("\"%s\" expects a boolean value (e.g. TRUE or 1)", loption);
	}
	return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
}

static string ParseString(const Value &value, const string &loption) {
	if (value.IsNull()) {
		return string();
	}
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			throw BinderException("\"%s\" expects a single argument as a string value", loption);
		}
		return ParseString(children[0], loption);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("\"%s\" expects a string argument!", loption);
	}
	return value.GetValue<string>();
}

static int64_t ParseInteger(const Value &value, const string &loption) {
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			// no option specified or multiple options specified
			throw BinderException("\"%s\" expects a single argument as an integer value", loption);
		}
		return ParseInteger(children[0], loption);
	}
	return value.GetValue<int64_t>();
}

bool CSVReaderOptions::GetHeader() const {
	return this->dialect_options.header;
}

void CSVReaderOptions::SetHeader(bool input) {
	this->dialect_options.header = input;
	this->has_header = true;
}

void CSVReaderOptions::SetCompression(const string &compression_p) {
	this->compression = FileCompressionTypeFromString(compression_p);
}

string CSVReaderOptions::GetEscape() const {
	return std::string(1, this->dialect_options.state_machine_options.escape);
}

void CSVReaderOptions::SetEscape(const string &input) {
	auto escape_str = input;
	if (escape_str.size() > 1) {
		throw InvalidInputException("The escape option cannot exceed a size of 1 byte.");
	}
	if (escape_str.empty()) {
		escape_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.escape = escape_str[0];
	this->has_escape = true;
}

int64_t CSVReaderOptions::GetSkipRows() const {
	return this->dialect_options.skip_rows;
}

void CSVReaderOptions::SetSkipRows(int64_t skip_rows) {
	dialect_options.skip_rows = skip_rows;
	skip_rows_set = true;
}

string CSVReaderOptions::GetDelimiter() const {
	return std::string(1, this->dialect_options.state_machine_options.delimiter);
}

void CSVReaderOptions::SetDelimiter(const string &input) {
	auto delim_str = StringUtil::Replace(input, "\\t", "\t");
	if (delim_str.size() > 1) {
		throw InvalidInputException("The delimiter option cannot exceed a size of 1 byte.");
	}
	this->has_delimiter = true;
	if (input.empty()) {
		delim_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.delimiter = delim_str[0];
}

string CSVReaderOptions::GetQuote() const {
	return std::string(1, this->dialect_options.state_machine_options.quote);
}

void CSVReaderOptions::SetQuote(const string &quote_p) {
	auto quote_str = quote_p;
	if (quote_str.size() > 1) {
		throw InvalidInputException("The quote option cannot exceed a size of 1 byte.");
	}
	if (quote_str.empty()) {
		quote_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.quote = quote_str[0];
	this->has_quote = true;
}

NewLineIdentifier CSVReaderOptions::GetNewline() const {
	return dialect_options.new_line;
}

void CSVReaderOptions::SetNewline(const string &input) {
	if (input == "\\n" || input == "\\r") {
		dialect_options.new_line = NewLineIdentifier::SINGLE;
	} else if (input == "\\r\\n") {
		dialect_options.new_line = NewLineIdentifier::CARRY_ON;
	} else {
		throw InvalidInputException("This is not accepted as a newline: " + input);
	}
	has_newline = true;
}

void CSVReaderOptions::SetDateFormat(LogicalTypeId type, const string &format, bool read_format) {
	string error;
	if (read_format) {
		error = StrTimeFormat::ParseFormatSpecifier(format, dialect_options.date_format[type]);
		dialect_options.date_format[type].format_specifier = format;
	} else {
		error = StrTimeFormat::ParseFormatSpecifier(format, write_date_format[type]);
	}
	if (!error.empty()) {
		throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
	}
	dialect_options.has_format[type] = true;
}

void CSVReaderOptions::SetReadOption(const string &loption, const Value &value, vector<string> &expected_names) {
	if (SetBaseOption(loption, value)) {
		return;
	}
	if (loption == "auto_detect") {
		auto_detect = ParseBoolean(value, loption);
	} else if (loption == "sample_size") {
		int64_t sample_size_option = ParseInteger(value, loption);
		if (sample_size_option < 1 && sample_size_option != -1) {
			throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
		}
		if (sample_size_option == -1) {
			// If -1, we basically read the whole thing
			sample_size_chunks = NumericLimits<idx_t>().Maximum();
		} else {
			sample_size_chunks = sample_size_option / STANDARD_VECTOR_SIZE;
			if (sample_size_option % STANDARD_VECTOR_SIZE != 0) {
				sample_size_chunks++;
			}
		}

	} else if (loption == "skip") {
		SetSkipRows(ParseInteger(value, loption));
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		maximum_line_size = ParseInteger(value, loption);
	} else if (loption == "force_not_null") {
		force_not_null = ParseColumnList(value, expected_names, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, true);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, true);
	} else if (loption == "ignore_errors") {
		ignore_errors = ParseBoolean(value, loption);
	} else if (loption == "buffer_size") {
		buffer_size = ParseInteger(value, loption);
		if (buffer_size == 0) {
			throw InvalidInputException("Buffer Size option must be higher than 0");
		}
	} else if (loption == "decimal_separator") {
		decimal_separator = ParseString(value, loption);
		if (decimal_separator != "." && decimal_separator != ",") {
			throw BinderException("Unsupported parameter for DECIMAL_SEPARATOR: should be '.' or ','");
		}
	} else if (loption == "null_padding") {
		null_padding = ParseBoolean(value, loption);
	} else if (loption == "allow_quoted_nulls") {
		allow_quoted_nulls = ParseBoolean(value, loption);
	} else if (loption == "parallel") {
		parallel_mode = ParseBoolean(value, loption) ? ParallelMode::PARALLEL : ParallelMode::SINGLE_THREADED;
	} else if (loption == "rejects_table") {
		// skip, handled in SetRejectsOptions
		auto table_name = ParseString(value, loption);
		if (table_name.empty()) {
			throw BinderException("REJECTS_TABLE option cannot be empty");
		}
		rejects_table_name = table_name;
	} else if (loption == "rejects_recovery_columns") {
		// Get the list of columns to use as a recovery key
		auto &children = ListValue::GetChildren(value);
		for (auto &child : children) {
			auto col_name = child.GetValue<string>();
			rejects_recovery_columns.push_back(col_name);
		}
	} else if (loption == "rejects_limit") {
		int64_t limit = ParseInteger(value, loption);
		if (limit < 0) {
			throw BinderException("Unsupported parameter for REJECTS_LIMIT: cannot be negative");
		}
		rejects_limit = limit;
	} else {
		throw BinderException("Unrecognized option for CSV reader \"%s\"", loption);
	}
}

void CSVReaderOptions::SetWriteOption(const string &loption, const Value &value) {
	if (loption == "new_line") {
		// Steal this from SetBaseOption so we can write different newlines (e.g., format JSON ARRAY)
		write_newline = ParseString(value, loption);
		return;
	}

	if (SetBaseOption(loption, value)) {
		return;
	}

	if (loption == "force_quote") {
		force_quote = ParseColumnList(value, name_list, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, false);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		if (StringUtil::Lower(format) == "iso") {
			format = "%Y-%m-%dT%H:%M:%S.%fZ";
		}
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, false);
		SetDateFormat(LogicalTypeId::TIMESTAMP_TZ, format, false);
	} else if (loption == "prefix") {
		prefix = ParseString(value, loption);
	} else if (loption == "suffix") {
		suffix = ParseString(value, loption);
	} else {
		throw BinderException("Unrecognized option CSV writer \"%s\"", loption);
	}
}

bool CSVReaderOptions::SetBaseOption(const string &loption, const Value &value) {
	// Make sure this function was only called after the option was turned into lowercase
	D_ASSERT(!std::any_of(loption.begin(), loption.end(), ::isupper));

	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		SetDelimiter(ParseString(value, loption));
	} else if (loption == "quote") {
		SetQuote(ParseString(value, loption));
	} else if (loption == "new_line") {
		SetNewline(ParseString(value, loption));
	} else if (loption == "escape") {
		SetEscape(ParseString(value, loption));
	} else if (loption == "header") {
		SetHeader(ParseBoolean(value, loption));
	} else if (loption == "null" || loption == "nullstr") {
		null_str = ParseString(value, loption);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(value, loption));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else if (loption == "compression") {
		SetCompression(ParseString(value, loption));
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

string CSVReaderOptions::ToString() const {
	return "  file=" + file_path + "\n  delimiter='" + dialect_options.state_machine_options.delimiter +
	       (has_delimiter ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) + "\n  quote='" +
	       dialect_options.state_machine_options.quote +
	       (has_quote ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) + "\n  escape='" +
	       dialect_options.state_machine_options.escape +
	       (has_escape ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       "\n  header=" + std::to_string(dialect_options.header) +
	       (has_header ? "" : (auto_detect ? " (auto detected)" : "' (default)")) +
	       "\n  sample_size=" + std::to_string(sample_size_chunks * STANDARD_VECTOR_SIZE) +
	       "\n  ignore_errors=" + std::to_string(ignore_errors) + "\n  all_varchar=" + std::to_string(all_varchar);
}

static Value StringVectorToValue(const vector<string> &vec) {
	vector<Value> content;
	content.reserve(vec.size());
	for (auto &item : vec) {
		content.push_back(Value(item));
	}
	return Value::LIST(std::move(content));
}

static uint8_t GetCandidateSpecificity(const LogicalType &candidate_type) {
	//! Const ht with accepted auto_types and their weights in specificity
	const duckdb::unordered_map<uint8_t, uint8_t> auto_type_candidates_specificity {
	    {(uint8_t)LogicalTypeId::VARCHAR, 0},  {(uint8_t)LogicalTypeId::TIMESTAMP, 1},
	    {(uint8_t)LogicalTypeId::DATE, 2},     {(uint8_t)LogicalTypeId::TIME, 3},
	    {(uint8_t)LogicalTypeId::DOUBLE, 4},   {(uint8_t)LogicalTypeId::FLOAT, 5},
	    {(uint8_t)LogicalTypeId::BIGINT, 6},   {(uint8_t)LogicalTypeId::INTEGER, 7},
	    {(uint8_t)LogicalTypeId::SMALLINT, 8}, {(uint8_t)LogicalTypeId::TINYINT, 9},
	    {(uint8_t)LogicalTypeId::BOOLEAN, 10}, {(uint8_t)LogicalTypeId::SQLNULL, 11}};

	auto id = (uint8_t)candidate_type.id();
	auto it = auto_type_candidates_specificity.find(id);
	if (it == auto_type_candidates_specificity.end()) {
		throw BinderException("Auto Type Candidate of type %s is not accepted as a valid input",
		                      EnumUtil::ToString(candidate_type.id()));
	}
	return it->second;
}

void CSVReaderOptions::FromNamedParameters(named_parameter_map_t &in, ClientContext &context,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &kv : in) {
		if (MultiFileReader::ParseOption(kv.first, kv.second, file_options, context)) {
			continue;
		}
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			explicitly_set_columns = true;
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_csv columns requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			if (names.empty()) {
				throw BinderException("read_csv requires at least a single column as input!");
			}
		} else if (loption == "auto_type_candidates") {
			auto_type_candidates.clear();
			map<uint8_t, LogicalType> candidate_types;
			// We always have the extremes of Null and Varchar, so we can default to varchar if the
			// sniffer is not able to confidently detect that column type
			candidate_types[GetCandidateSpecificity(LogicalType::VARCHAR)] = LogicalType::VARCHAR;
			candidate_types[GetCandidateSpecificity(LogicalType::SQLNULL)] = LogicalType::SQLNULL;

			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv auto_types requires a list as input");
			}
			auto &list_children = ListValue::GetChildren(kv.second);
			if (list_children.empty()) {
				throw BinderException("auto_type_candidates requires at least one type");
			}
			for (auto &child : list_children) {
				if (child.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("auto_type_candidates requires a type specification as string");
				}
				auto candidate_type = TransformStringToLogicalType(StringValue::Get(child), context);
				candidate_types[GetCandidateSpecificity(candidate_type)] = candidate_type;
			}
			for (auto &candidate_type : candidate_types) {
				auto_type_candidates.emplace_back(candidate_type.second);
			}
		} else if (loption == "column_names" || loption == "names") {
			if (!name_list.empty()) {
				throw BinderException("read_csv_auto column_names/names can only be supplied once");
			}
			if (kv.second.IsNull()) {
				throw BinderException("read_csv_auto %s cannot be NULL", kv.first);
			}
			auto &children = ListValue::GetChildren(kv.second);
			for (auto &child : children) {
				name_list.push_back(StringValue::Get(child));
			}
		} else if (loption == "column_types" || loption == "types" || loption == "dtypes") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT && child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv_auto %s requires a struct or list as input", kv.first);
			}
			if (!sql_type_list.empty()) {
				throw BinderException("read_csv_auto column_types/types/dtypes can only be supplied once");
			}
			vector<string> sql_type_names;
			if (child_type.id() == LogicalTypeId::STRUCT) {
				auto &struct_children = StructValue::GetChildren(kv.second);
				D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
				for (idx_t i = 0; i < struct_children.size(); i++) {
					auto &name = StructType::GetChildName(child_type, i);
					auto &val = struct_children[i];
					if (val.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("read_csv_auto %s requires a type specification as string", kv.first);
					}
					sql_type_names.push_back(StringValue::Get(val));
					sql_types_per_column[name] = i;
				}
			} else {
				auto &list_child = ListType::GetChildType(child_type);
				if (list_child.id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv_auto %s requires a list of types (varchar) as input", kv.first);
				}
				auto &children = ListValue::GetChildren(kv.second);
				for (auto &child : children) {
					sql_type_names.push_back(StringValue::Get(child));
				}
			}
			sql_type_list.reserve(sql_type_names.size());
			for (auto &sql_type : sql_type_names) {
				auto def_type = TransformStringToLogicalType(sql_type, context);
				if (def_type.id() == LogicalTypeId::USER) {
					throw BinderException("Unrecognized type \"%s\" for read_csv_auto %s definition", sql_type,
					                      kv.first);
				}
				sql_type_list.push_back(std::move(def_type));
			}
		} else if (loption == "all_varchar") {
			all_varchar = BooleanValue::Get(kv.second);
		} else if (loption == "normalize_names") {
			normalize_names = BooleanValue::Get(kv.second);
		} else {
			SetReadOption(loption, kv.second, names);
		}
	}
}

//! This function is used to remember options set by the sniffer, for use in ReadCSVRelation
void CSVReaderOptions::ToNamedParameters(named_parameter_map_t &named_params) {
	if (has_delimiter) {
		named_params["delim"] = Value(GetDelimiter());
	}
	if (has_newline) {
		named_params["newline"] = Value(EnumUtil::ToString(GetNewline()));
	}
	if (has_quote) {
		named_params["quote"] = Value(GetQuote());
	}
	if (has_escape) {
		named_params["escape"] = Value(GetEscape());
	}
	if (has_header) {
		named_params["header"] = Value(GetHeader());
	}
	named_params["max_line_size"] = Value::BIGINT(maximum_line_size);
	if (skip_rows_set) {
		named_params["skip"] = Value::BIGINT(GetSkipRows());
	}
	named_params["null_padding"] = Value::BOOLEAN(null_padding);
	if (!date_format.at(LogicalType::DATE).format_specifier.empty()) {
		named_params["dateformat"] = Value(date_format.at(LogicalType::DATE).format_specifier);
	}
	if (!date_format.at(LogicalType::TIMESTAMP).format_specifier.empty()) {
		named_params["timestampformat"] = Value(date_format.at(LogicalType::TIMESTAMP).format_specifier);
	}

	named_params["normalize_names"] = Value::BOOLEAN(normalize_names);
	if (!name_list.empty() && !named_params.count("column_names") && !named_params.count("names")) {
		named_params["column_names"] = StringVectorToValue(name_list);
	}
	named_params["all_varchar"] = Value::BOOLEAN(all_varchar);
	named_params["maximum_line_size"] = Value::BIGINT(maximum_line_size);
}

} // namespace duckdb






namespace duckdb {

CSVStateMachine::CSVStateMachine(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options,
                                 shared_ptr<CSVBufferManager> buffer_manager_p,
                                 CSVStateMachineCache &csv_state_machine_cache_p)
    : csv_state_machine_cache(csv_state_machine_cache_p), options(options_p),
      csv_buffer_iterator(std::move(buffer_manager_p)),
      transition_array(csv_state_machine_cache.Get(state_machine_options)) {
	dialect_options.state_machine_options = state_machine_options;
	dialect_options.has_format = options.dialect_options.has_format;
	dialect_options.date_format = options.dialect_options.date_format;
	dialect_options.skip_rows = options.dialect_options.skip_rows;
}

void CSVStateMachine::Reset() {
	csv_buffer_iterator.Reset();
}

void CSVStateMachine::VerifyUTF8() {
	auto utf_type = Utf8Proc::Analyze(value.c_str(), value.size());
	if (utf_type == UnicodeType::INVALID) {
		int64_t error_line = cur_rows;
		throw InvalidInputException("Error in file \"%s\" at line %llu: "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, ErrorManager::InvalidUnicodeError(value, "CSV file"),
		                            options.ToString());
	}
}
} // namespace duckdb



namespace duckdb {

void InitializeTransitionArray(unsigned char *transition_array, const uint8_t state) {
	for (uint32_t i = 0; i < NUM_TRANSITIONS; i++) {
		transition_array[i] = state;
	}
}

void CSVStateMachineCache::Insert(const CSVStateMachineOptions &state_machine_options) {
	D_ASSERT(state_machine_cache.find(state_machine_options) == state_machine_cache.end());
	// Initialize transition array with default values to the Standard option
	auto &transition_array = state_machine_cache[state_machine_options];
	const uint8_t standard_state = static_cast<uint8_t>(CSVState::STANDARD);
	const uint8_t field_separator_state = static_cast<uint8_t>(CSVState::DELIMITER);
	const uint8_t record_separator_state = static_cast<uint8_t>(CSVState::RECORD_SEPARATOR);
	const uint8_t carriage_return_state = static_cast<uint8_t>(CSVState::CARRIAGE_RETURN);
	const uint8_t quoted_state = static_cast<uint8_t>(CSVState::QUOTED);
	const uint8_t unquoted_state = static_cast<uint8_t>(CSVState::UNQUOTED);
	const uint8_t escape_state = static_cast<uint8_t>(CSVState::ESCAPE);
	const uint8_t empty_line_state = static_cast<uint8_t>(CSVState::EMPTY_LINE);
	const uint8_t invalid_state = static_cast<uint8_t>(CSVState::INVALID);

	for (uint32_t i = 0; i < NUM_STATES; i++) {
		switch (i) {
		case quoted_state:
			InitializeTransitionArray(transition_array[i], quoted_state);
			break;
		case unquoted_state:
		case invalid_state:
		case escape_state:
			InitializeTransitionArray(transition_array[i], invalid_state);
			break;
		default:
			InitializeTransitionArray(transition_array[i], standard_state);
			break;
		}
	}

	// Now set values depending on configuration
	// 1) Standard State
	transition_array[standard_state][static_cast<uint8_t>(state_machine_options.delimiter)] = field_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[standard_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[standard_state][static_cast<uint8_t>(state_machine_options.quote)] = quoted_state;
	// 2) Field Separator State
	transition_array[field_separator_state][static_cast<uint8_t>(state_machine_options.delimiter)] =
	    field_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[field_separator_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[field_separator_state][static_cast<uint8_t>(state_machine_options.quote)] = quoted_state;
	// 3) Record Separator State
	transition_array[record_separator_state][static_cast<uint8_t>(state_machine_options.delimiter)] =
	    field_separator_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\n')] = empty_line_state;
	transition_array[record_separator_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[record_separator_state][static_cast<uint8_t>(state_machine_options.quote)] = quoted_state;
	// 4) Carriage Return State
	transition_array[carriage_return_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[carriage_return_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[carriage_return_state][static_cast<uint8_t>(state_machine_options.escape)] = escape_state;
	// 5) Quoted State
	transition_array[quoted_state][static_cast<uint8_t>(state_machine_options.quote)] = unquoted_state;
	if (state_machine_options.quote != state_machine_options.escape) {
		transition_array[quoted_state][static_cast<uint8_t>(state_machine_options.escape)] = escape_state;
	}
	// 6) Unquoted State
	transition_array[unquoted_state][static_cast<uint8_t>('\n')] = record_separator_state;
	transition_array[unquoted_state][static_cast<uint8_t>('\r')] = carriage_return_state;
	transition_array[unquoted_state][static_cast<uint8_t>(state_machine_options.delimiter)] = field_separator_state;
	if (state_machine_options.quote == state_machine_options.escape) {
		transition_array[unquoted_state][static_cast<uint8_t>(state_machine_options.escape)] = quoted_state;
	}
	// 7) Escaped State
	transition_array[escape_state][static_cast<uint8_t>(state_machine_options.quote)] = quoted_state;
	transition_array[escape_state][static_cast<uint8_t>(state_machine_options.escape)] = quoted_state;
	// 8) Empty Line State
	transition_array[empty_line_state][static_cast<uint8_t>('\r')] = empty_line_state;
	transition_array[empty_line_state][static_cast<uint8_t>('\n')] = empty_line_state;
}

CSVStateMachineCache::CSVStateMachineCache() {
	for (auto quoterule : default_quote_rule) {
		const auto &quote_candidates = default_quote[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : default_delimiter) {
				const auto &escape_candidates = default_escape[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					Insert({delimiter, quote, escape});
				}
			}
		}
	}
}

const state_machine_t &CSVStateMachineCache::Get(const CSVStateMachineOptions &state_machine_options) {
	//! Custom State Machine, we need to create it and cache it first
	if (state_machine_cache.find(state_machine_options) == state_machine_cache.end()) {
		Insert(state_machine_options);
	}
	const auto &transition_array = state_machine_cache[state_machine_options];
	return transition_array;
}
} // namespace duckdb



















#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

ParallelCSVReader::ParallelCSVReader(ClientContext &context, CSVReaderOptions options_p,
                                     unique_ptr<CSVBufferRead> buffer_p, idx_t first_pos_first_buffer_p,
                                     const vector<LogicalType> &requested_types, idx_t file_idx_p)
    : BaseCSVReader(context, std::move(options_p), requested_types), file_idx(file_idx_p),
      first_pos_first_buffer(first_pos_first_buffer_p) {
	Initialize(requested_types);
	SetBufferRead(std::move(buffer_p));
}

void ParallelCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	return_types = requested_types;
	InitParseChunk(return_types.size());
}

bool ParallelCSVReader::NewLineDelimiter(bool carry, bool carry_followed_by_nl, bool first_char) {
	// Set the delimiter if not set yet.
	SetNewLineDelimiter(carry, carry_followed_by_nl);
	D_ASSERT(options.dialect_options.new_line == NewLineIdentifier::SINGLE ||
	         options.dialect_options.new_line == NewLineIdentifier::CARRY_ON);
	if (options.dialect_options.new_line == NewLineIdentifier::SINGLE) {
		return (!carry) || (carry && !carry_followed_by_nl);
	}
	return (carry && carry_followed_by_nl) || (!carry && first_char);
}

void ParallelCSVReader::SkipEmptyLines() {
	idx_t new_pos_buffer = position_buffer;
	if (parse_chunk.data.size() == 1) {
		// Empty lines are null data.
		return;
	}
	for (; new_pos_buffer < end_buffer; new_pos_buffer++) {
		if (StringUtil::CharacterIsNewline((*buffer)[new_pos_buffer])) {
			bool carrier_return = (*buffer)[new_pos_buffer] == '\r';
			new_pos_buffer++;
			if (carrier_return && new_pos_buffer < buffer_size && (*buffer)[new_pos_buffer] == '\n') {
				position_buffer++;
			}
			if (new_pos_buffer > end_buffer) {
				return;
			}
			position_buffer = new_pos_buffer;
		} else if ((*buffer)[new_pos_buffer] != ' ') {
			return;
		}
	}
}

bool ParallelCSVReader::SetPosition() {
	if (buffer->buffer->is_first_buffer && start_buffer == position_buffer && start_buffer == first_pos_first_buffer) {
		start_buffer = buffer->buffer->start_position;
		position_buffer = start_buffer;
		verification_positions.beginning_of_first_line = position_buffer;
		verification_positions.end_of_last_line = position_buffer;
		// First buffer doesn't need any setting

		if (options.dialect_options.header) {
			for (; position_buffer < end_buffer; position_buffer++) {
				if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
					bool carrier_return = (*buffer)[position_buffer] == '\r';
					position_buffer++;
					if (carrier_return && position_buffer < buffer_size && (*buffer)[position_buffer] == '\n') {
						position_buffer++;
					}
					if (position_buffer > end_buffer) {
						VerifyLineLength(position_buffer, buffer->batch_index);
						return false;
					}
					SkipEmptyLines();
					if (verification_positions.beginning_of_first_line == 0) {
						verification_positions.beginning_of_first_line = position_buffer;
					}
					VerifyLineLength(position_buffer, buffer->batch_index);
					verification_positions.end_of_last_line = position_buffer;
					return true;
				}
			}
			VerifyLineLength(position_buffer, buffer->batch_index);
			return false;
		}
		SkipEmptyLines();
		if (verification_positions.beginning_of_first_line == 0) {
			verification_positions.beginning_of_first_line = position_buffer;
		}

		verification_positions.end_of_last_line = position_buffer;
		return true;
	}

	// We have to move position up to next new line
	idx_t end_buffer_real = end_buffer;
	// Check if we already start in a valid line
	string error_message;
	bool successfully_read_first_line = false;
	while (!successfully_read_first_line) {
		DataChunk first_line_chunk;
		first_line_chunk.Initialize(allocator, return_types);
		// Ensure that parse_chunk has no gunk when trying to figure new line
		parse_chunk.Reset();
		for (; position_buffer < end_buffer; position_buffer++) {
			if (StringUtil::CharacterIsNewline((*buffer)[position_buffer])) {
				bool carriage_return = (*buffer)[position_buffer] == '\r';
				bool carriage_return_followed = false;
				position_buffer++;
				if (position_buffer < end_buffer) {
					if (carriage_return && (*buffer)[position_buffer] == '\n') {
						carriage_return_followed = true;
						position_buffer++;
					}
				}
				if (NewLineDelimiter(carriage_return, carriage_return_followed, position_buffer - 1 == start_buffer)) {
					break;
				}
			}
		}
		SkipEmptyLines();

		if (position_buffer > buffer_size) {
			break;
		}

		auto pos_check = position_buffer == 0 ? position_buffer : position_buffer - 1;
		if (position_buffer >= end_buffer && !StringUtil::CharacterIsNewline((*buffer)[pos_check])) {
			break;
		}

		if (position_buffer > end_buffer && options.dialect_options.new_line == NewLineIdentifier::CARRY_ON &&
		    (*buffer)[pos_check] == '\n') {
			break;
		}
		idx_t position_set = position_buffer;
		start_buffer = position_buffer;
		// We check if we can add this line
		// disable the projection pushdown while reading the first line
		// otherwise the first line parsing can be influenced by which columns we are reading
		auto column_ids = std::move(reader_data.column_ids);
		auto column_mapping = std::move(reader_data.column_mapping);
		InitializeProjection();
		try {
			successfully_read_first_line = TryParseSimpleCSV(first_line_chunk, error_message, true);
		} catch (...) {
			successfully_read_first_line = false;
		}
		// restore the projection pushdown
		reader_data.column_ids = std::move(column_ids);
		reader_data.column_mapping = std::move(column_mapping);
		end_buffer = end_buffer_real;
		start_buffer = position_set;
		if (position_buffer >= end_buffer) {
			if (successfully_read_first_line) {
				position_buffer = position_set;
			}
			break;
		}
		position_buffer = position_set;
	}
	if (verification_positions.beginning_of_first_line == 0) {
		verification_positions.beginning_of_first_line = position_buffer;
	}
	// Ensure that parse_chunk has no gunk when trying to figure new line
	parse_chunk.Reset();

	verification_positions.end_of_last_line = position_buffer;
	finished = false;
	return successfully_read_first_line;
}

void ParallelCSVReader::SetBufferRead(unique_ptr<CSVBufferRead> buffer_read_p) {
	if (!buffer_read_p->buffer) {
		throw InternalException("ParallelCSVReader::SetBufferRead - CSVBufferRead does not have a buffer to read");
	}
	position_buffer = buffer_read_p->buffer_start;
	start_buffer = buffer_read_p->buffer_start;
	end_buffer = buffer_read_p->buffer_end;
	if (buffer_read_p->next_buffer) {
		buffer_size = buffer_read_p->buffer->actual_size + buffer_read_p->next_buffer->actual_size;
	} else {
		buffer_size = buffer_read_p->buffer->actual_size;
	}
	buffer = std::move(buffer_read_p);

	reached_remainder_state = false;
	verification_positions.beginning_of_first_line = 0;
	verification_positions.end_of_last_line = 0;
	finished = false;
	D_ASSERT(end_buffer <= buffer_size);
}

VerificationPositions ParallelCSVReader::GetVerificationPositions() {
	verification_positions.beginning_of_first_line += buffer->buffer->csv_global_start;
	verification_positions.end_of_last_line += buffer->buffer->csv_global_start;
	return verification_positions;
}

// If BufferRemainder returns false, it means we are done scanning this buffer and should go to the end_state
bool ParallelCSVReader::BufferRemainder() {
	if (position_buffer >= end_buffer && !reached_remainder_state) {
		// First time we finish the buffer piece we should scan here, we set the variables
		// to allow this piece to be scanned up to the end of the buffer or the next new line
		reached_remainder_state = true;
		// end_buffer is allowed to go to buffer size to finish its last line
		end_buffer = buffer_size;
	}
	if (position_buffer >= end_buffer) {
		// buffer ends, return false
		return false;
	}
	// we can still scan stuff, return true
	return true;
}

bool AllNewLine(string_t value, idx_t column_amount) {
	auto value_str = value.GetString();
	if (value_str.empty() && column_amount == 1) {
		// This is a one column (empty)
		return false;
	}
	for (idx_t i = 0; i < value.GetSize(); i++) {
		if (!StringUtil::CharacterIsNewline(value_str[i])) {
			return false;
		}
	}
	return true;
}

bool ParallelCSVReader::TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message, bool try_add_line) {
	// If line is not set, we have to figure it out, we assume whatever is in the first line
	if (options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
		idx_t cur_pos = position_buffer;
		// we can start in the middle of a new line, so move a bit forward.
		while (cur_pos < end_buffer) {
			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
				cur_pos++;
			} else {
				break;
			}
		}
		for (; cur_pos < end_buffer; cur_pos++) {
			if (StringUtil::CharacterIsNewline((*buffer)[cur_pos])) {
				bool carriage_return = (*buffer)[cur_pos] == '\r';
				bool carriage_return_followed = false;
				cur_pos++;
				if (cur_pos < end_buffer) {
					if (carriage_return && (*buffer)[cur_pos] == '\n') {
						carriage_return_followed = true;
						cur_pos++;
					}
				}
				SetNewLineDelimiter(carriage_return, carriage_return_followed);
				break;
			}
		}
	}
	// used for parsing algorithm
	if (start_buffer == buffer_size) {
		// Nothing to read
		finished = true;
		return true;
	}
	D_ASSERT(end_buffer <= buffer_size);
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	bool has_quotes = false;

	vector<idx_t> escape_positions;
	if ((start_buffer == buffer->buffer_start || start_buffer == buffer->buffer_end) && !try_add_line) {
		// First time reading this buffer piece
		if (!SetPosition()) {
			finished = true;
			return true;
		}
	}
	if (position_buffer == buffer_size) {
		// Nothing to read
		finished = true;
		return true;
	}
	// Keep track of line size
	idx_t line_start = position_buffer;
	// start parsing the first value
	goto value_start;

value_start : {
	/* state: value_start */
	if (!BufferRemainder()) {
		goto final_state;
	}
	offset = 0;

	// this state parses the first character of a value
	if ((*buffer)[position_buffer] == options.dialect_options.state_machine_options.quote) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start_buffer = position_buffer + 1;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start_buffer = position_buffer;
		goto normal;
	}
};

normal : {
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	for (; position_buffer < end_buffer; position_buffer++) {
		auto c = (*buffer)[position_buffer];
		if (c == options.dialect_options.state_machine_options.delimiter) {
			// Check if previous character is a quote, if yes, this means we are in a non-initialized quoted value
			// This only matters for when trying to figure out where csv lines start
			if (position_buffer > 0 && try_add_line) {
				if ((*buffer)[position_buffer - 1] == options.dialect_options.state_machine_options.quote) {
					return false;
				}
			}
			// delimiter: end the value and add it to the chunk
			goto add_value;
		} else if (StringUtil::CharacterIsNewline(c)) {
			// Check if previous character is a quote, if yes, this means we are in a non-initialized quoted value
			// This only matters for when trying to figure out where csv lines start
			if (position_buffer > 0 && try_add_line) {
				if ((*buffer)[position_buffer - 1] == options.dialect_options.state_machine_options.quote) {
					return false;
				}
			}
			// newline: add row
			if (column > 0 || try_add_line || parse_chunk.data.size() == 1) {
				goto add_row;
			}
			if (column == 0 && position_buffer == start_buffer) {
				start_buffer++;
			}
		}
	}
	if (!BufferRemainder()) {
		goto final_state;
	} else {
		goto normal;
	}
};

add_value : {
	/* state: Add value to string vector */
	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes,
	         buffer->local_batch_index);
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	start_buffer = ++position_buffer;
	if (!BufferRemainder()) {
		goto final_state;
	}
	goto value_start;
};

add_row : {
	/* state: Add Row to Parse chunk */
	// check type of newline (\r or \n)
	bool carriage_return = (*buffer)[position_buffer] == '\r';

	AddValue(buffer->GetValue(start_buffer, position_buffer, offset), column, escape_positions, has_quotes,
	         buffer->local_batch_index);
	if (try_add_line) {
		bool success = column == insert_chunk.ColumnCount();
		if (success) {
			idx_t cur_linenr = linenr;
			AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
			success = Flush(insert_chunk, buffer->local_batch_index, true);
			linenr = cur_linenr;
		}
		reached_remainder_state = false;
		parse_chunk.Reset();
		return success;
	} else {
		VerifyLineLength(position_buffer - line_start, buffer->batch_index);
		line_start = position_buffer;
		finished_chunk = AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
	}
	// increase position by 1 and move start to the new position
	offset = 0;
	has_quotes = false;
	position_buffer++;
	start_buffer = position_buffer;
	verification_positions.end_of_last_line = position_buffer;
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		// optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
		if (!BufferRemainder()) {
			goto final_state;
		}
		if ((*buffer)[position_buffer] == '\n') {
			if (options.dialect_options.new_line == NewLineIdentifier::SINGLE) {
				error_message = "Wrong NewLine Identifier. Expecting \\r\\n";
				return false;
			}
			// newline after carriage return: skip
			// increase position by 1 and move start to the new position
			start_buffer = ++position_buffer;

			SkipEmptyLines();
			verification_positions.end_of_last_line = position_buffer;
			start_buffer = position_buffer;
			if (reached_remainder_state) {
				goto final_state;
			}
		} else {
			if (options.dialect_options.new_line == NewLineIdentifier::CARRY_ON) {
				error_message = "Wrong NewLine Identifier. Expecting \\r or \\n";
				return false;
			}
		}
		if (!BufferRemainder()) {
			goto final_state;
		}
		if (reached_remainder_state || finished_chunk) {
			goto final_state;
		}
		goto value_start;
	} else {
		if (options.dialect_options.new_line == NewLineIdentifier::CARRY_ON) {
			error_message = "Wrong NewLine Identifier. Expecting \\r or \\n";
			return false;
		}
		if (reached_remainder_state) {
			goto final_state;
		}
		if (!BufferRemainder()) {
			goto final_state;
		}
		SkipEmptyLines();
		if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
			error_message = "Line does not fit in one buffer. Increase the buffer size.";
			return false;
		}
		verification_positions.end_of_last_line = position_buffer;
		start_buffer = position_buffer;
		// \n newline, move to value start
		if (finished_chunk) {
			goto final_state;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes this state parses the remainder of a quoted value*/
	has_quotes = true;
	position_buffer++;
	for (; position_buffer < end_buffer; position_buffer++) {
		auto c = (*buffer)[position_buffer];
		if (c == options.dialect_options.state_machine_options.quote) {
			// quote: move to unquoted state
			goto unquote;
		} else if (c == options.dialect_options.state_machine_options.escape) {
			// escape: store the escaped position and move to handle_escape state
			escape_positions.push_back(position_buffer - start_buffer);
			goto handle_escape;
		}
	}
	if (!BufferRemainder()) {
		if (buffer->buffer->is_last_buffer) {
			if (try_add_line) {
				return false;
			}
			// still in quoted state at the end of the file or at the end of a buffer when running multithreaded, error:
			throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
			                            GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(),
			                            options.ToString());
		} else {
			goto final_state;
		}
	} else {
		position_buffer--;
		goto in_quotes;
	}

unquote : {
	/* state: unquote: this state handles the state directly after we unquote*/
	//
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position_buffer++;
	if (!BufferRemainder()) {
		offset = 1;
		goto final_state;
	}
	auto c = (*buffer)[position_buffer];
	if (c == options.dialect_options.state_machine_options.quote &&
	    (options.dialect_options.state_machine_options.escape == '\0' ||
	     options.dialect_options.state_machine_options.escape == options.dialect_options.state_machine_options.quote)) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position_buffer - start_buffer);
		goto in_quotes;
	} else if (c == options.dialect_options.state_machine_options.delimiter) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(c)) {
		offset = 1;
		// FIXME: should this be an assertion?
		D_ASSERT(try_add_line || (!try_add_line && column == parse_chunk.ColumnCount() - 1));
		goto add_row;
	} else if (position_buffer >= end_buffer) {
		// reached end of buffer
		offset = 1;
		goto final_state;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s). ",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(),
		    options.ToString());
		return false;
	}
}
handle_escape : {
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position_buffer++;
	if (!BufferRemainder()) {
		goto final_state;
	}
	if (position_buffer >= buffer_size && buffer->buffer->is_last_buffer) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(), options.ToString());
		return false;
	}
	if ((*buffer)[position_buffer] != options.dialect_options.state_machine_options.quote &&
	    (*buffer)[position_buffer] != options.dialect_options.state_machine_options.escape) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated, buffer->local_batch_index).c_str(), options.ToString());
		return false;
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
}
final_state : {
	/* state: final_stage reached after we finished reading the end_buffer of the csv buffer */
	// reset end buffer
	end_buffer = buffer->buffer_end;
	if (position_buffer == end_buffer) {
		reached_remainder_state = false;
	}
	if (finished_chunk) {
		if (position_buffer >= end_buffer) {
			if (position_buffer == end_buffer && StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1]) &&
			    position_buffer < buffer_size) {
				// last position is a new line, we still have to go through one more line of this buffer
				finished = false;
			} else {
				finished = true;
			}
		}
		buffer->lines_read += insert_chunk.size();
		return true;
	}
	// If this is the last buffer, we have to read the last value
	if (buffer->buffer->is_last_buffer || !buffer->next_buffer ||
	    (buffer->next_buffer && buffer->next_buffer->is_last_buffer)) {
		if (column > 0 || start_buffer != position_buffer || try_add_line ||
		    (insert_chunk.data.size() == 1 && start_buffer != position_buffer)) {
			// remaining values to be added to the chunk
			auto str_value = buffer->GetValue(start_buffer, position_buffer, offset);
			if (!AllNewLine(str_value, insert_chunk.data.size()) || offset == 0) {
				AddValue(str_value, column, escape_positions, has_quotes, buffer->local_batch_index);
				if (try_add_line) {
					bool success = column == return_types.size();
					if (success) {
						auto cur_linenr = linenr;
						AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
						success = Flush(insert_chunk, buffer->local_batch_index);
						linenr = cur_linenr;
					}
					parse_chunk.Reset();
					reached_remainder_state = false;
					return success;
				} else {
					VerifyLineLength(position_buffer - line_start, buffer->batch_index);
					line_start = position_buffer;
					AddRow(insert_chunk, column, error_message, buffer->local_batch_index);
					if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
						error_message = "Line does not fit in one buffer. Increase the buffer size.";
						return false;
					}
					verification_positions.end_of_last_line = position_buffer;
				}
			}
		}
	}
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk, buffer->local_batch_index);
		buffer->lines_read += insert_chunk.size();
	}
	if (position_buffer - verification_positions.end_of_last_line > options.buffer_size) {
		error_message = "Line does not fit in one buffer. Increase the buffer size.";
		return false;
	}
	end_buffer = buffer_size;
	SkipEmptyLines();
	end_buffer = buffer->buffer_end;
	verification_positions.end_of_last_line = position_buffer;
	if (position_buffer >= end_buffer) {
		if (position_buffer >= end_buffer) {
			if (position_buffer == end_buffer && StringUtil::CharacterIsNewline((*buffer)[position_buffer - 1]) &&
			    position_buffer < buffer_size) {
				// last position is a new line, we still have to go through one more line of this buffer
				finished = false;
			} else {
				finished = true;
			}
		}
	}
	return true;
};
}

void ParallelCSVReader::ParseCSV(DataChunk &insert_chunk) {
	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

idx_t ParallelCSVReader::GetLineError(idx_t line_error, idx_t buffer_idx, bool stop_at_first) {
	while (true) {
		if (buffer->line_info->CanItGetLine(file_idx, buffer_idx)) {
			auto cur_start = verification_positions.beginning_of_first_line + buffer->buffer->csv_global_start;
			return buffer->line_info->GetLine(buffer_idx, line_error, file_idx, cur_start, false, stop_at_first);
		}
	}
}

void ParallelCSVReader::Increment(idx_t buffer_idx) {
	return buffer->line_info->Increment(file_idx, buffer_idx);
}

bool ParallelCSVReader::TryParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	return TryParseCSV(mode, dummy_chunk, error_message);
}

void ParallelCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool ParallelCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;
	return TryParseSimpleCSV(insert_chunk, error_message);
}

} // namespace duckdb

#endif
