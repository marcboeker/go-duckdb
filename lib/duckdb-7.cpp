#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif




namespace duckdb {

// FLAT, CONSTANT, DICTIONARY, SEQUENCE
struct TestVectorBindData : public TableFunctionData {
	vector<LogicalType> types;
	bool all_flat = false;
};

struct TestVectorTypesData : public GlobalTableFunctionState {
	TestVectorTypesData() : offset(0) {
	}

	vector<unique_ptr<DataChunk>> entries;
	idx_t offset;
};

struct TestVectorInfo {
	TestVectorInfo(const vector<LogicalType> &types, const map<LogicalTypeId, TestType> &test_type_map,
	               vector<unique_ptr<DataChunk>> &entries)
	    : types(types), test_type_map(test_type_map), entries(entries) {
	}

	const vector<LogicalType> &types;
	const map<LogicalTypeId, TestType> &test_type_map;
	vector<unique_ptr<DataChunk>> &entries;
};

struct TestGeneratedValues {
public:
	void AddColumn(vector<Value> values) {
		if (!column_values.empty() && column_values[0].size() != values.size()) {
			throw InternalException("Size mismatch when adding a column to TestGeneratedValues");
		}
		column_values.push_back(std::move(values));
	}

	const Value &GetValue(idx_t row, idx_t column) const {
		return column_values[column][row];
	}

	idx_t Rows() const {
		return column_values.empty() ? 0 : column_values[0].size();
	}

	idx_t Columns() const {
		return column_values.size();
	}

private:
	vector<vector<Value>> column_values;
};

struct TestVectorFlat {
	static constexpr const idx_t TEST_VECTOR_CARDINALITY = 3;

	static vector<Value> GenerateValues(TestVectorInfo &info, const LogicalType &type) {
		vector<Value> result;
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			vector<child_list_t<Value>> struct_children;
			auto &child_types = StructType::GetChildTypes(type);

			struct_children.resize(TEST_VECTOR_CARDINALITY);
			for (auto &child_type : child_types) {
				auto child_values = GenerateValues(info, child_type.second);

				for (idx_t i = 0; i < child_values.size(); i++) {
					struct_children[i].push_back(make_pair(child_type.first, std::move(child_values[i])));
				}
			}
			for (auto &struct_child : struct_children) {
				result.push_back(Value::STRUCT(std::move(struct_child)));
			}
			break;
		}
		case PhysicalType::LIST: {
			auto &child_type = ListType::GetChildType(type);
			auto child_values = GenerateValues(info, child_type);

			result.push_back(Value::LIST(child_type, {child_values[0], child_values[1]}));
			result.push_back(Value::LIST(child_type, {}));
			result.push_back(Value::LIST(child_type, {child_values[2]}));
			break;
		}
		default: {
			auto entry = info.test_type_map.find(type.id());
			if (entry == info.test_type_map.end()) {
				throw NotImplementedException("Unimplemented type for test_vector_types %s", type.ToString());
			}
			result.push_back(entry->second.min_value);
			result.push_back(entry->second.max_value);
			result.emplace_back(type);
			break;
		}
		}
		return result;
	}

	static TestGeneratedValues GenerateValues(TestVectorInfo &info) {
		// generate the values for each column
		TestGeneratedValues generated_values;
		for (auto &type : info.types) {
			generated_values.AddColumn(GenerateValues(info, type));
		}
		return generated_values;
	}

	static void Generate(TestVectorInfo &info) {
		auto result_values = GenerateValues(info);
		for (idx_t cur_row = 0; cur_row < result_values.Rows(); cur_row += STANDARD_VECTOR_SIZE) {
			auto result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), info.types);
			auto cardinality = MinValue<idx_t>(STANDARD_VECTOR_SIZE, result_values.Rows() - cur_row);
			for (idx_t c = 0; c < info.types.size(); c++) {
				for (idx_t i = 0; i < cardinality; i++) {
					result->data[c].SetValue(i, result_values.GetValue(cur_row + i, c));
				}
			}
			result->SetCardinality(cardinality);
			info.entries.push_back(std::move(result));
		}
	}
};

struct TestVectorConstant {
	static void Generate(TestVectorInfo &info) {
		auto values = TestVectorFlat::GenerateValues(info);
		for (idx_t cur_row = 0; cur_row < TestVectorFlat::TEST_VECTOR_CARDINALITY; cur_row += STANDARD_VECTOR_SIZE) {
			auto result = make_uniq<DataChunk>();
			result->Initialize(Allocator::DefaultAllocator(), info.types);
			auto cardinality = MinValue<idx_t>(STANDARD_VECTOR_SIZE, TestVectorFlat::TEST_VECTOR_CARDINALITY - cur_row);
			for (idx_t c = 0; c < info.types.size(); c++) {
				result->data[c].SetValue(0, values.GetValue(0, c));
				result->data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
			}
			result->SetCardinality(cardinality);

			info.entries.push_back(std::move(result));
		}
	}
};

struct TestVectorSequence {
	static void GenerateVector(TestVectorInfo &info, const LogicalType &type, Vector &result) {
		D_ASSERT(type == result.GetType());
		switch (type.id()) {
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			result.Sequence(3, 2, 3);
			return;
		default:
			break;
		}
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			auto &child_entries = StructVector::GetEntries(result);
			for (auto &child_entry : child_entries) {
				GenerateVector(info, child_entry->GetType(), *child_entry);
			}
			break;
		}
		case PhysicalType::LIST: {
			auto data = FlatVector::GetData<list_entry_t>(result);
			data[0].offset = 0;
			data[0].length = 2;
			data[1].offset = 2;
			data[1].length = 0;
			data[2].offset = 2;
			data[2].length = 1;

			GenerateVector(info, ListType::GetChildType(type), ListVector::GetEntry(result));
			ListVector::SetListSize(result, 3);
			break;
		}
		default: {
			auto entry = info.test_type_map.find(type.id());
			if (entry == info.test_type_map.end()) {
				throw NotImplementedException("Unimplemented type for test_vector_types %s", type.ToString());
			}
			result.SetValue(0, entry->second.min_value);
			result.SetValue(1, entry->second.max_value);
			result.SetValue(2, Value(type));
			break;
		}
		}
	}

	static void Generate(TestVectorInfo &info) {
#if STANDARD_VECTOR_SIZE > 2
		auto result = make_uniq<DataChunk>();
		result->Initialize(Allocator::DefaultAllocator(), info.types);

		for (idx_t c = 0; c < info.types.size(); c++) {
			GenerateVector(info, info.types[c], result->data[c]);
		}
		result->SetCardinality(3);
		info.entries.push_back(std::move(result));
#endif
	}
};

struct TestVectorDictionary {
	static void Generate(TestVectorInfo &info) {
		idx_t current_chunk = info.entries.size();

		unordered_set<idx_t> slice_entries {1, 2};

		TestVectorFlat::Generate(info);
		idx_t current_idx = 0;
		for (idx_t i = current_chunk; i < info.entries.size(); i++) {
			auto &chunk = *info.entries[i];
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			idx_t sel_idx = 0;
			for (idx_t k = 0; k < chunk.size(); k++) {
				if (slice_entries.count(current_idx + k) > 0) {
					sel.set_index(sel_idx++, k);
				}
			}
			chunk.Slice(sel, sel_idx);
			current_idx += chunk.size();
		}
	}
};

static unique_ptr<FunctionData> TestVectorTypesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<TestVectorBindData>();
	for (idx_t i = 0; i < input.inputs.size(); i++) {
		string name = "test_vector";
		if (i > 0) {
			name += to_string(i + 1);
		}
		auto &input_val = input.inputs[i];
		names.emplace_back(name);
		return_types.push_back(input_val.type());
		result->types.push_back(input_val.type());
	}
	for (auto &entry : input.named_parameters) {
		if (entry.first == "all_flat") {
			result->all_flat = BooleanValue::Get(entry.second);
		} else {
			throw InternalException("Unrecognized named parameter for test_vector_types");
		}
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> TestVectorTypesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TestVectorBindData>();

	auto result = make_uniq<TestVectorTypesData>();

	auto test_types = TestAllTypesFun::GetTestTypes();

	map<LogicalTypeId, TestType> test_type_map;
	for (auto &test_type : test_types) {
		test_type_map.insert(make_pair(test_type.type.id(), std::move(test_type)));
	}

	TestVectorInfo info(bind_data.types, test_type_map, result->entries);
	TestVectorFlat::Generate(info);
	TestVectorConstant::Generate(info);
	TestVectorDictionary::Generate(info);
	TestVectorSequence::Generate(info);
	for (auto &entry : result->entries) {
		entry->Verify();
	}
	if (bind_data.all_flat) {
		for (auto &entry : result->entries) {
			entry->Flatten();
			entry->Verify();
		}
	}
	return std::move(result);
}

void TestVectorTypesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TestVectorTypesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	output.Reference(*data.entries[data.offset]);
	data.offset++;
}

void TestVectorTypesFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunction test_vector_types("test_vector_types", {LogicalType::ANY}, TestVectorTypesFunction,
	                                TestVectorTypesBind, TestVectorTypesInit);
	test_vector_types.varargs = LogicalType::ANY;
	test_vector_types.named_parameters["all_flat"] = LogicalType::BOOLEAN;

	set.AddFunction(std::move(test_vector_types));
}

} // namespace duckdb








namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	PragmaVersion::RegisterFunction(*this);
	PragmaPlatform::RegisterFunction(*this);
	PragmaCollations::RegisterFunction(*this);
	PragmaTableInfo::RegisterFunction(*this);
	PragmaStorageInfo::RegisterFunction(*this);
	PragmaMetadataInfo::RegisterFunction(*this);
	PragmaDatabaseSize::RegisterFunction(*this);
	PragmaLastProfilingOutput::RegisterFunction(*this);
	PragmaDetailedProfilingOutput::RegisterFunction(*this);

	DuckDBColumnsFun::RegisterFunction(*this);
	DuckDBConstraintsFun::RegisterFunction(*this);
	DuckDBDatabasesFun::RegisterFunction(*this);
	DuckDBFunctionsFun::RegisterFunction(*this);
	DuckDBKeywordsFun::RegisterFunction(*this);
	DuckDBIndexesFun::RegisterFunction(*this);
	DuckDBSchemasFun::RegisterFunction(*this);
	DuckDBDependenciesFun::RegisterFunction(*this);
	DuckDBExtensionsFun::RegisterFunction(*this);
	DuckDBSequencesFun::RegisterFunction(*this);
	DuckDBSettingsFun::RegisterFunction(*this);
	DuckDBTablesFun::RegisterFunction(*this);
	DuckDBTemporaryFilesFun::RegisterFunction(*this);
	DuckDBTypesFun::RegisterFunction(*this);
	DuckDBViewsFun::RegisterFunction(*this);
	TestAllTypesFun::RegisterFunction(*this);
	TestVectorTypesFun::RegisterFunction(*this);
}

} // namespace duckdb



















namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Scan
//===--------------------------------------------------------------------===//
bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate);

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan
	TableScanState scan_state;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
};

static storage_t GetStorageIndex(TableCatalogEntry &table, column_t column_id) {
	if (column_id == DConstants::INVALID_INDEX) {
		return column_id;
	}
	auto &col = table.GetColumn(LogicalIndex(column_id));
	return col.StorageOid();
}

struct TableScanGlobalState : public GlobalTableFunctionState {
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		max_threads = bind_data.table.GetStorage().MaxThreads(context);
	}

	ParallelTableScanState state;
	idx_t max_threads;

	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *gstate) {
	auto result = make_uniq<TableScanLocalState>();
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	vector<column_t> column_ids = input.column_ids;
	for (auto &col : column_ids) {
		auto storage_idx = GetStorageIndex(bind_data.table, col);
		col = storage_idx;
	}
	result->scan_state.Initialize(std::move(column_ids), input.filters.get());
	TableScanParallelStateNext(context.client, input.bind_data.get(), result.get(), gstate);
	if (input.CanRemoveFilterColumns()) {
		auto &tsgs = gstate->Cast<TableScanGlobalState>();
		result->all_columns.Initialize(context.client, tsgs.scanned_types);
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {

	D_ASSERT(input.bind_data);
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto result = make_uniq<TableScanGlobalState>(context, input.bind_data.get());
	bind_data.table.GetStorage().InitializeParallelScan(context, result->state);
	if (input.CanRemoveFilterColumns()) {
		result->projection_ids = input.projection_ids;
		const auto &columns = bind_data.table.GetColumns();
		for (const auto &col_idx : input.column_ids) {
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				result->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				result->scanned_types.push_back(columns.GetColumn(LogicalIndex(col_idx)).Type());
			}
		}
	}
	return std::move(result);
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	if (local_storage.Find(bind_data.table.GetStorage())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}
	return bind_data.table.GetStatistics(context, column_id);
}

static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
	auto &gstate = data_p.global_state->Cast<TableScanGlobalState>();
	auto &state = data_p.local_state->Cast<TableScanLocalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	do {
		if (bind_data.is_create_index) {
			storage.CreateIndexScan(state.scan_state, output,
			                        TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		} else if (gstate.CanRemoveFilterColumns()) {
			state.all_columns.Reset();
			storage.Scan(transaction, state.all_columns, state.scan_state);
			output.ReferenceColumns(state.all_columns, gstate.projection_ids);
		} else {
			storage.Scan(transaction, output, state.scan_state);
		}
		if (output.size() > 0) {
			return;
		}
		if (!TableScanParallelStateNext(context, data_p.bind_data.get(), data_p.local_state.get(),
		                                data_p.global_state.get())) {
			return;
		}
	} while (true);
}

bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &parallel_state = global_state->Cast<TableScanGlobalState>();
	auto &state = local_state->Cast<TableScanLocalState>();
	auto &storage = bind_data.table.GetStorage();

	return storage.NextParallelScan(context, parallel_state.state, state.scan_state);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *gstate_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &gstate = gstate_p->Cast<TableScanGlobalState>();
	auto &storage = bind_data.table.GetStorage();
	idx_t total_rows = storage.GetTotalRows();
	if (total_rows == 0) {
		//! Table is either empty or smaller than a vector size, so it is finished
		return 100;
	}
	idx_t scanned_rows = gstate.state.scan_state.processed_rows;
	scanned_rows += gstate.state.local_state.processed_rows;
	auto percentage = 100 * (double(scanned_rows) / total_rows);
	if (percentage > 100) {
		//! In case the last chunk has less elements than STANDARD_VECTOR_SIZE, if our percentage is over 100
		//! It means we finished this table.
		return 100;
	}
	return percentage;
}

idx_t TableScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                             LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate_p) {
	auto &state = local_state->Cast<TableScanLocalState>();
	if (state.scan_state.table_state.row_group) {
		return state.scan_state.table_state.batch_index;
	}
	if (state.scan_state.local_state.row_group) {
		return state.scan_state.table_state.batch_index + state.scan_state.local_state.batch_index;
	}
	return 0;
}

BindInfo TableScanGetBindInfo(const FunctionData *bind_data) {
	return BindInfo(ScanType::TABLE);
}

void TableScanDependency(DependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	entries.AddDependency(bind_data.table);
}

unique_ptr<NodeStatistics> TableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	idx_t estimated_cardinality = storage.info->cardinality + local_storage.AddedRows(bind_data.table.GetStorage());
	return make_uniq<NodeStatistics>(storage.info->cardinality, estimated_cardinality);
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
struct IndexScanGlobalState : public GlobalTableFunctionState {
	explicit IndexScanGlobalState(data_ptr_t row_id_data) : row_ids(LogicalType::ROW_TYPE, row_id_data) {
	}

	Vector row_ids;
	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<storage_t> column_ids;
	bool finished;
};

static unique_ptr<GlobalTableFunctionState> IndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	data_ptr_t row_id_data = nullptr;
	if (!bind_data.result_ids.empty()) {
		row_id_data = (data_ptr_t)&bind_data.result_ids[0]; // NOLINT - this is not pretty
	}
	auto result = make_uniq<IndexScanGlobalState>(row_id_data);
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);

	result->column_ids.reserve(input.column_ids.size());
	for (auto &id : input.column_ids) {
		result->column_ids.push_back(GetStorageIndex(bind_data.table, id));
	}
	result->local_storage_state.Initialize(result->column_ids, input.filters.get());
	local_storage.InitializeScan(bind_data.table.GetStorage(), result->local_storage_state.local_state, input.filters);

	result->finished = false;
	return std::move(result);
}

static void IndexScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
	auto &state = data_p.global_state->Cast<IndexScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &local_storage = LocalStorage::Get(transaction);

	if (!state.finished) {
		bind_data.table.GetStorage().Fetch(transaction, output, state.column_ids, state.row_ids,
		                                   bind_data.result_ids.size(), state.fetch_state);
		state.finished = true;
	}
	if (output.size() == 0) {
		local_storage.Scan(state.local_storage_state.local_state, state.column_ids, output);
	}
}

static void RewriteIndexExpression(Index &index, LogicalGet &get, Expression &expr, bool &rewrite_possible) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		// bound column ref: rewrite to fit in the current set of bound column ids
		bound_colref.binding.table_index = get.table_index;
		column_t referenced_column = index.column_ids[bound_colref.binding.column_index];
		// search for the referenced column in the set of column_ids
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			if (get.column_ids[i] == referenced_column) {
				bound_colref.binding.column_index = i;
				return;
			}
		}
		// column id not found in bound columns in the LogicalGet: rewrite not possible
		rewrite_possible = false;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteIndexExpression(index, get, child, rewrite_possible); });
}

void TableScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                    vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &table = bind_data.table;
	auto &storage = table.GetStorage();

	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_optimizer) {
		// we only push index scans if the optimizer is enabled
		return;
	}
	if (bind_data.is_index_scan) {
		return;
	}
	if (!get.table_filters.filters.empty()) {
		// if there were filters before we can't convert this to an index scan
		return;
	}
	if (!get.projection_ids.empty()) {
		// if columns were pruned by RemoveUnusedColumns we can't convert this to an index scan,
		// because index scan does not support filter_prune (yet)
		return;
	}
	if (filters.empty()) {
		// no indexes or no filters: skip the pushdown
		return;
	}
	// behold
	storage.info->indexes.Scan([&](Index &index) {
		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table

		if (index.unbound_expressions.size() > 1) {
			// NOTE: index scans are not (yet) supported for compound index keys
			return false;
		}

		auto index_expression = index.unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(index, get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			return false;
		}

		Value low_value, high_value, equal_value;
		ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;
		// try to find a matching index for any of the filter expressions
		for (auto &filter : filters) {
			auto &expr = *filter;

			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_uniq<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(*index_expression));
			matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());

			matcher.policy = SetMatcher::Policy::UNORDERED;

			vector<reference<Expression>> bindings;
			if (matcher.Match(expr, bindings)) {
				// range or equality comparison with constant value
				// we can use our index here
				// bindings[0] = the expression
				// bindings[1] = the index expression
				// bindings[2] = the constant
				auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
				auto constant_value = bindings[2].get().Cast<BoundConstantExpression>().value;
				auto comparison_type = comparison.type;
				if (comparison.left->type == ExpressionType::VALUE_CONSTANT) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisonExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			} else if (expr.type == ExpressionType::COMPARE_BETWEEN) {
				// BETWEEN expression
				auto &between = expr.Cast<BoundBetweenExpression>();
				if (!between.input->Equals(*index_expression)) {
					// expression doesn't match the current index expression
					continue;
				}
				if (between.lower->type != ExpressionType::VALUE_CONSTANT ||
				    between.upper->type != ExpressionType::VALUE_CONSTANT) {
					// not a constant comparison
					continue;
				}
				low_value = (between.lower->Cast<BoundConstantExpression>()).value;
				low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
				                                              : ExpressionType::COMPARE_GREATERTHAN;
				high_value = (between.upper->Cast<BoundConstantExpression>()).value;
				high_comparison_type = between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                               : ExpressionType::COMPARE_LESSTHAN;
				break;
			}
		}
		if (!equal_value.IsNull() || !low_value.IsNull() || !high_value.IsNull()) {
			// we can scan this index using this predicate: try a scan
			auto &transaction = Transaction::Get(context, bind_data.table.catalog);
			unique_ptr<IndexScanState> index_state;
			if (!equal_value.IsNull()) {
				// equality predicate
				index_state =
				    index.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			} else if (!low_value.IsNull() && !high_value.IsNull()) {
				// two-sided predicate
				index_state = index.InitializeScanTwoPredicates(transaction, low_value, low_comparison_type, high_value,
				                                                high_comparison_type);
			} else if (!low_value.IsNull()) {
				// less than predicate
				index_state = index.InitializeScanSinglePredicate(transaction, low_value, low_comparison_type);
			} else {
				D_ASSERT(!high_value.IsNull());
				index_state = index.InitializeScanSinglePredicate(transaction, high_value, high_comparison_type);
			}
			if (index.Scan(transaction, storage, *index_state, STANDARD_VECTOR_SIZE, bind_data.result_ids)) {
				// use an index scan!
				bind_data.is_index_scan = true;
				get.function = TableScanFunction::GetIndexScanFunction();
			} else {
				bind_data.result_ids.clear();
			}
			return true;
		}
		return false;
	});
}

string TableScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	string result = bind_data.table.name;
	return result;
}

static void TableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "is_index_scan", bind_data.is_index_scan);
	serializer.WriteProperty(104, "is_create_index", bind_data.is_create_index);
	serializer.WriteProperty(105, "result_ids", bind_data.result_ids);
}

static unique_ptr<FunctionData> TableScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto &catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), catalog, schema, table);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Cant find table for %s.%s", schema, table);
	}
	auto result = make_uniq<TableScanBindData>(catalog_entry.Cast<DuckTableEntry>());
	deserializer.ReadProperty(103, "is_index_scan", result->is_index_scan);
	deserializer.ReadProperty(104, "is_create_index", result->is_create_index);
	deserializer.ReadProperty(105, "result_ids", result->result_ids);
	return std::move(result);
}

TableFunction TableScanFunction::GetIndexScanFunction() {
	TableFunction scan_function("index_scan", {}, IndexScanFunction);
	scan_function.init_local = nullptr;
	scan_function.init_global = IndexScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = nullptr;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = nullptr;
	scan_function.get_batch_index = nullptr;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = false;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	return scan_function;
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = TableScanPushdownComplexFilter;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;
	scan_function.get_batch_index = TableScanGetBatchIndex;
	scan_function.get_batch_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	scan_function.filter_prune = true;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	return scan_function;
}

optional_ptr<TableCatalogEntry> TableScanFunction::GetTableEntry(const TableFunction &function,
                                                                 const optional_ptr<FunctionData> bind_data_p) {
	if (function.function != TableScanFunc || !bind_data_p) {
		return nullptr;
	}
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return &bind_data.table;
}

void TableScanFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet table_scan_set("seq_scan");
	table_scan_set.AddFunction(GetFunction());
	set.AddFunction(std::move(table_scan_set));

	set.AddFunction(GetIndexScanFunction());
}

void BuiltinFunctions::RegisterTableScanFunctions() {
	TableScanFunction::RegisterFunction(*this);
}

} // namespace duckdb






namespace duckdb {

struct UnnestBindData : public FunctionData {
	explicit UnnestBindData(LogicalType input_type_p) : input_type(std::move(input_type_p)) {
	}

	LogicalType input_type;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<UnnestBindData>(input_type);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<UnnestBindData>();
		return input_type == other.input_type;
	}
};

struct UnnestGlobalState : public GlobalTableFunctionState {
	UnnestGlobalState() {
	}

	vector<unique_ptr<Expression>> select_list;

	idx_t MaxThreads() const override {
		return GlobalTableFunctionState::MAX_THREADS;
	}
};

struct UnnestLocalState : public LocalTableFunctionState {
	UnnestLocalState() {
	}

	unique_ptr<OperatorState> operator_state;
};

static unique_ptr<FunctionData> UnnestBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.input_table_types.size() != 1 || input.input_table_types[0].id() != LogicalTypeId::LIST) {
		throw BinderException("UNNEST requires a single list as input");
	}
	return_types.push_back(ListType::GetChildType(input.input_table_types[0]));
	names.push_back(input.input_table_names[0]);
	return make_uniq<UnnestBindData>(input.input_table_types[0]);
}

static unique_ptr<LocalTableFunctionState> UnnestLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<UnnestGlobalState>();

	auto result = make_uniq<UnnestLocalState>();
	result->operator_state = PhysicalUnnest::GetState(context, gstate.select_list);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> UnnestInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<UnnestBindData>();
	auto result = make_uniq<UnnestGlobalState>();
	auto ref = make_uniq<BoundReferenceExpression>(bind_data.input_type, 0);
	auto bound_unnest = make_uniq<BoundUnnestExpression>(ListType::GetChildType(bind_data.input_type));
	bound_unnest->child = std::move(ref);
	result->select_list.push_back(std::move(bound_unnest));
	return std::move(result);
}

static OperatorResultType UnnestFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                         DataChunk &output) {
	auto &state = data_p.global_state->Cast<UnnestGlobalState>();
	auto &lstate = data_p.local_state->Cast<UnnestLocalState>();
	return PhysicalUnnest::ExecuteInternal(context, input, output, *lstate.operator_state, state.select_list, false);
}

void UnnestTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunction unnest_function("unnest", {LogicalTypeId::TABLE}, nullptr, UnnestBind, UnnestInit, UnnestLocalInit);
	unnest_function.in_out_function = UnnestFunction;
	set.AddFunction(unnest_function);
}

} // namespace duckdb




#include <cstdint>

namespace duckdb {

struct PragmaVersionData : public GlobalTableFunctionState {
	PragmaVersionData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaVersionBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("library_version");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_id");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaVersionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaVersionData>();
}

static void PragmaVersionFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaVersionData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::LibraryVersion());
	output.SetValue(1, 0, DuckDB::SourceID());
	data.finished = true;
}

void PragmaVersion::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_version("pragma_version", {}, PragmaVersionFunction);
	pragma_version.bind = PragmaVersionBind;
	pragma_version.init_global = PragmaVersionInit;
	set.AddFunction(pragma_version);
}

idx_t DuckDB::StandardVectorSize() {
	return STANDARD_VECTOR_SIZE;
}

const char *DuckDB::SourceID() {
	return DUCKDB_SOURCE_ID;
}

const char *DuckDB::LibraryVersion() {
	return DUCKDB_VERSION;
}

string DuckDB::Platform() {
#if defined(DUCKDB_CUSTOM_PLATFORM)
	return DUCKDB_QUOTE_DEFINE(DUCKDB_CUSTOM_PLATFORM);
#endif
#if defined(DUCKDB_WASM_VERSION)
	// DuckDB-Wasm requires CUSTOM_PLATFORM to be defined
	static_assert(0, "DUCKDB_WASM_VERSION should rely on CUSTOM_PLATFORM being provided");
#endif
	string os = "linux";
#if INTPTR_MAX == INT64_MAX
	string arch = "amd64";
#elif INTPTR_MAX == INT32_MAX
	string arch = "i686";
#else
#error Unknown pointer size or missing size macros!
#endif
	string postfix = "";

#ifdef _WIN32
	os = "windows";
#elif defined(__APPLE__)
	os = "osx";
#endif
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
	arch = "arm64";
#endif

#if !defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI == 0
	if (os == "linux") {
		postfix = "_gcc4";
	}
#endif
#if defined(__ANDROID__)
	postfix += "_android"; // using + because it may also be gcc4
#endif
#ifdef __MINGW32__
	postfix = "_mingw";
#endif
// this is used for the windows R builds which use a separate build environment
#ifdef DUCKDB_PLATFORM_RTOOLS
	postfix = "_rtools";
#endif
	return os + "_" + arch + postfix;
}

struct PragmaPlatformData : public GlobalTableFunctionState {
	PragmaPlatformData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaPlatformBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("platform");
	return_types.emplace_back(LogicalType::VARCHAR);
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> PragmaPlatformInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaPlatformData>();
}

static void PragmaPlatformFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<PragmaPlatformData>();
	if (data.finished) {
		// finished returning values
		return;
	}
	output.SetCardinality(1);
	output.SetValue(0, 0, DuckDB::Platform());
	data.finished = true;
}

void PragmaPlatform::RegisterFunction(BuiltinFunctions &set) {
	TableFunction pragma_platform("pragma_platform", {}, PragmaPlatformFunction);
	pragma_platform.bind = PragmaPlatformBind;
	pragma_platform.init_global = PragmaPlatformInit;
	set.AddFunction(pragma_platform);
}

} // namespace duckdb


namespace duckdb {

GlobalTableFunctionState::~GlobalTableFunctionState() {
}

LocalTableFunctionState::~LocalTableFunctionState() {
}

TableFunctionInfo::~TableFunctionInfo() {
}

TableFunction::TableFunction(string name, vector<LogicalType> arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : SimpleNamedParameterFunction(std::move(name), std::move(arguments)), bind(bind), bind_replace(nullptr),
      init_global(init_global), init_local(init_local), function(function), in_out_function(nullptr),
      in_out_function_final(nullptr), statistics(nullptr), dependency(nullptr), cardinality(nullptr),
      pushdown_complex_filter(nullptr), to_string(nullptr), table_scan_progress(nullptr), get_batch_index(nullptr),
      get_batch_info(nullptr), serialize(nullptr), deserialize(nullptr), projection_pushdown(false),
      filter_pushdown(false), filter_prune(false) {
}

TableFunction::TableFunction(const vector<LogicalType> &arguments, table_function_t function,
                             table_function_bind_t bind, table_function_init_global_t init_global,
                             table_function_init_local_t init_local)
    : TableFunction(string(), arguments, function, bind, init_global, init_local) {
}
TableFunction::TableFunction()
    : SimpleNamedParameterFunction("", {}), bind(nullptr), bind_replace(nullptr), init_global(nullptr),
      init_local(nullptr), function(nullptr), in_out_function(nullptr), statistics(nullptr), dependency(nullptr),
      cardinality(nullptr), pushdown_complex_filter(nullptr), to_string(nullptr), table_scan_progress(nullptr),
      get_batch_index(nullptr), get_batch_info(nullptr), serialize(nullptr), deserialize(nullptr),
      projection_pushdown(false), filter_pushdown(false), filter_prune(false) {
}

bool TableFunction::Equal(const TableFunction &rhs) const {
	// number of types
	if (this->arguments.size() != rhs.arguments.size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->arguments.size(); ++i) {
		if (this->arguments[i] != rhs.arguments[i]) {
			return false;
		}
	}
	// varargs
	if (this->varargs != rhs.varargs) {
		return false;
	}

	return true; // they are equal
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//
//! The SelectStatement of the view





namespace duckdb {

TableMacroFunction::TableMacroFunction(unique_ptr<QueryNode> query_node)
    : MacroFunction(MacroType::TABLE_MACRO), query_node(std::move(query_node)) {
}

TableMacroFunction::TableMacroFunction(void) : MacroFunction(MacroType::TABLE_MACRO) {
}

unique_ptr<MacroFunction> TableMacroFunction::Copy() const {
	auto result = make_uniq<TableMacroFunction>();
	result->query_node = query_node->Copy();
	this->CopyProperties(*result);
	return std::move(result);
}

string TableMacroFunction::ToSQL(const string &schema, const string &name) const {
	return MacroFunction::ToSQL(schema, name) + StringUtil::Format("TABLE (%s);", query_node->ToString());
}

} // namespace duckdb







namespace duckdb {

void UDFWrapper::RegisterFunction(string name, vector<LogicalType> args, LogicalType ret_type,
                                  scalar_function_t udf_function, ClientContext &context, LogicalType varargs) {

	ScalarFunction scalar_function(std::move(name), std::move(args), std::move(ret_type), std::move(udf_function));
	scalar_function.varargs = std::move(varargs);
	scalar_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	CreateScalarFunctionInfo info(scalar_function);
	info.schema = DEFAULT_SCHEMA;
	context.RegisterFunction(info);
}

void UDFWrapper::RegisterAggrFunction(AggregateFunction aggr_function, ClientContext &context, LogicalType varargs) {
	aggr_function.varargs = std::move(varargs);
	CreateAggregateFunctionInfo info(std::move(aggr_function));
	context.RegisterFunction(info);
}

} // namespace duckdb















namespace duckdb {

BaseAppender::BaseAppender(Allocator &allocator, AppenderType type_p)
    : allocator(allocator), column(0), appender_type(type_p) {
}

BaseAppender::BaseAppender(Allocator &allocator_p, vector<LogicalType> types_p, AppenderType type_p)
    : allocator(allocator_p), types(std::move(types_p)), collection(make_uniq<ColumnDataCollection>(allocator, types)),
      column(0), appender_type(type_p) {
	InitializeChunk();
}

BaseAppender::~BaseAppender() {
}

void BaseAppender::Destructor() {
	if (Exception::UncaughtException()) {
		return;
	}
	// flush any remaining chunks, but only if we are not cleaning up the appender as part of an exception stack unwind
	// wrapped in a try/catch because Close() can throw if the table was dropped in the meantime
	try {
		Close();
	} catch (...) {
	}
}

InternalAppender::InternalAppender(ClientContext &context_p, TableCatalogEntry &table_p)
    : BaseAppender(Allocator::DefaultAllocator(), table_p.GetTypes(), AppenderType::PHYSICAL), context(context_p),
      table(table_p) {
}

InternalAppender::~InternalAppender() {
	Destructor();
}

Appender::Appender(Connection &con, const string &schema_name, const string &table_name)
    : BaseAppender(Allocator::DefaultAllocator(), AppenderType::LOGICAL), context(con.context) {
	description = con.TableInfo(schema_name, table_name);
	if (!description) {
		// table could not be found
		throw CatalogException(StringUtil::Format("Table \"%s.%s\" could not be found", schema_name, table_name));
	}
	for (auto &column : description->columns) {
		types.push_back(column.Type());
	}
	InitializeChunk();
	collection = make_uniq<ColumnDataCollection>(allocator, types);
}

Appender::Appender(Connection &con, const string &table_name) : Appender(con, DEFAULT_SCHEMA, table_name) {
}

Appender::~Appender() {
	Destructor();
}

void BaseAppender::InitializeChunk() {
	chunk.Initialize(allocator, types);
}

void BaseAppender::BeginRow() {
}

void BaseAppender::EndRow() {
	// check that all rows have been appended to
	if (column != chunk.ColumnCount()) {
		throw InvalidInputException("Call to EndRow before all rows have been appended to!");
	}
	column = 0;
	chunk.SetCardinality(chunk.size() + 1);
	if (chunk.size() >= STANDARD_VECTOR_SIZE) {
		FlushChunk();
	}
}

template <class SRC, class DST>
void BaseAppender::AppendValueInternal(Vector &col, SRC input) {
	FlatVector::GetData<DST>(col)[chunk.size()] = Cast::Operation<SRC, DST>(input);
}

template <class SRC, class DST>
void BaseAppender::AppendDecimalValueInternal(Vector &col, SRC input) {
	switch (appender_type) {
	case AppenderType::LOGICAL: {
		auto &type = col.GetType();
		D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		TryCastToDecimal::Operation<SRC, DST>(input, FlatVector::GetData<DST>(col)[chunk.size()], nullptr, width,
		                                      scale);
		return;
	}
	case AppenderType::PHYSICAL: {
		AppendValueInternal<SRC, DST>(col, input);
		return;
	}
	default:
		throw InternalException("Type not implemented for AppenderType");
	}
}

template <class T>
void BaseAppender::AppendValueInternal(T input) {
	if (column >= types.size()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column];
	switch (col.GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		AppendValueInternal<T, bool>(col, input);
		break;
	case LogicalTypeId::UTINYINT:
		AppendValueInternal<T, uint8_t>(col, input);
		break;
	case LogicalTypeId::TINYINT:
		AppendValueInternal<T, int8_t>(col, input);
		break;
	case LogicalTypeId::USMALLINT:
		AppendValueInternal<T, uint16_t>(col, input);
		break;
	case LogicalTypeId::SMALLINT:
		AppendValueInternal<T, int16_t>(col, input);
		break;
	case LogicalTypeId::UINTEGER:
		AppendValueInternal<T, uint32_t>(col, input);
		break;
	case LogicalTypeId::INTEGER:
		AppendValueInternal<T, int32_t>(col, input);
		break;
	case LogicalTypeId::UBIGINT:
		AppendValueInternal<T, uint64_t>(col, input);
		break;
	case LogicalTypeId::BIGINT:
		AppendValueInternal<T, int64_t>(col, input);
		break;
	case LogicalTypeId::HUGEINT:
		AppendValueInternal<T, hugeint_t>(col, input);
		break;
	case LogicalTypeId::FLOAT:
		AppendValueInternal<T, float>(col, input);
		break;
	case LogicalTypeId::DOUBLE:
		AppendValueInternal<T, double>(col, input);
		break;
	case LogicalTypeId::DECIMAL:
		switch (col.GetType().InternalType()) {
		case PhysicalType::INT16:
			AppendDecimalValueInternal<T, int16_t>(col, input);
			break;
		case PhysicalType::INT32:
			AppendDecimalValueInternal<T, int32_t>(col, input);
			break;
		case PhysicalType::INT64:
			AppendDecimalValueInternal<T, int64_t>(col, input);
			break;
		case PhysicalType::INT128:
			AppendDecimalValueInternal<T, hugeint_t>(col, input);
			break;
		default:
			throw InternalException("Internal type not recognized for Decimal");
		}
		break;
	case LogicalTypeId::DATE:
		AppendValueInternal<T, date_t>(col, input);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		AppendValueInternal<T, timestamp_t>(col, input);
		break;
	case LogicalTypeId::TIME:
		AppendValueInternal<T, dtime_t>(col, input);
		break;
	case LogicalTypeId::TIME_TZ:
		AppendValueInternal<T, dtime_tz_t>(col, input);
		break;
	case LogicalTypeId::INTERVAL:
		AppendValueInternal<T, interval_t>(col, input);
		break;
	case LogicalTypeId::VARCHAR:
		FlatVector::GetData<string_t>(col)[chunk.size()] = StringCast::Operation<T>(input, col);
		break;
	default:
		AppendValue(Value::CreateValue<T>(input));
		return;
	}
	column++;
}

template <>
void BaseAppender::Append(bool value) {
	AppendValueInternal<bool>(value);
}

template <>
void BaseAppender::Append(int8_t value) {
	AppendValueInternal<int8_t>(value);
}

template <>
void BaseAppender::Append(int16_t value) {
	AppendValueInternal<int16_t>(value);
}

template <>
void BaseAppender::Append(int32_t value) {
	AppendValueInternal<int32_t>(value);
}

template <>
void BaseAppender::Append(int64_t value) {
	AppendValueInternal<int64_t>(value);
}

template <>
void BaseAppender::Append(hugeint_t value) {
	AppendValueInternal<hugeint_t>(value);
}

template <>
void BaseAppender::Append(uint8_t value) {
	AppendValueInternal<uint8_t>(value);
}

template <>
void BaseAppender::Append(uint16_t value) {
	AppendValueInternal<uint16_t>(value);
}

template <>
void BaseAppender::Append(uint32_t value) {
	AppendValueInternal<uint32_t>(value);
}

template <>
void BaseAppender::Append(uint64_t value) {
	AppendValueInternal<uint64_t>(value);
}

template <>
void BaseAppender::Append(const char *value) {
	AppendValueInternal<string_t>(string_t(value));
}

void BaseAppender::Append(const char *value, uint32_t length) {
	AppendValueInternal<string_t>(string_t(value, length));
}

template <>
void BaseAppender::Append(string_t value) {
	AppendValueInternal<string_t>(value);
}

template <>
void BaseAppender::Append(float value) {
	AppendValueInternal<float>(value);
}

template <>
void BaseAppender::Append(double value) {
	AppendValueInternal<double>(value);
}

template <>
void BaseAppender::Append(date_t value) {
	AppendValueInternal<date_t>(value);
}

template <>
void BaseAppender::Append(dtime_t value) {
	AppendValueInternal<dtime_t>(value);
}

template <>
void BaseAppender::Append(timestamp_t value) {
	AppendValueInternal<timestamp_t>(value);
}

template <>
void BaseAppender::Append(interval_t value) {
	AppendValueInternal<interval_t>(value);
}

template <>
void BaseAppender::Append(Value value) { // NOLINT: template shtuff
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	AppendValue(value);
}

template <>
void BaseAppender::Append(std::nullptr_t value) {
	if (column >= chunk.ColumnCount()) {
		throw InvalidInputException("Too many appends for chunk!");
	}
	auto &col = chunk.data[column++];
	FlatVector::SetNull(col, chunk.size(), true);
}

void BaseAppender::AppendValue(const Value &value) {
	chunk.SetValue(column, chunk.size(), value);
	column++;
}

void BaseAppender::AppendDataChunk(DataChunk &chunk) {
	if (chunk.GetTypes() != types) {
		throw InvalidInputException("Type mismatch in Append DataChunk and the types required for appender");
	}
	collection->Append(chunk);
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::FlushChunk() {
	if (chunk.size() == 0) {
		return;
	}
	collection->Append(chunk);
	chunk.Reset();
	if (collection->Count() >= FLUSH_COUNT) {
		Flush();
	}
}

void BaseAppender::Flush() {
	// check that all vectors have the same length before appending
	if (column != 0) {
		throw InvalidInputException("Failed to Flush appender: incomplete append to row!");
	}

	FlushChunk();
	if (collection->Count() == 0) {
		return;
	}
	FlushInternal(*collection);

	collection->Reset();
	column = 0;
}

void Appender::FlushInternal(ColumnDataCollection &collection) {
	context->Append(*description, collection);
}

void InternalAppender::FlushInternal(ColumnDataCollection &collection) {
	table.GetStorage().LocalAppend(table, context, collection);
}

void BaseAppender::Close() {
	if (column == 0 || column == types.size()) {
		Flush();
	}
}

} // namespace duckdb









namespace duckdb {

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, AttachedDatabaseType type)
    : CatalogEntry(CatalogType::DATABASE_ENTRY,
                   type == AttachedDatabaseType::SYSTEM_DATABASE ? SYSTEM_CATALOG : TEMP_CATALOG, 0),
      db(db), type(type) {
	D_ASSERT(type == AttachedDatabaseType::TEMP_DATABASE || type == AttachedDatabaseType::SYSTEM_DATABASE);
	if (type == AttachedDatabaseType::TEMP_DATABASE) {
		storage = make_uniq<SingleFileStorageManager>(*this, ":memory:", false);
	}
	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, string name_p, string file_path_p,
                                   AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db),
      type(access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
                                                : AttachedDatabaseType::READ_WRITE_DATABASE),
      parent_catalog(&catalog_p) {
	storage = make_uniq<SingleFileStorageManager>(*this, std::move(file_path_p), access_mode == AccessMode::READ_ONLY);
	catalog = make_uniq<DuckCatalog>(*this);
	transaction_manager = make_uniq<DuckTransactionManager>(*this);
	internal = true;
}

AttachedDatabase::AttachedDatabase(DatabaseInstance &db, Catalog &catalog_p, StorageExtension &storage_extension,
                                   string name_p, AttachInfo &info, AccessMode access_mode)
    : CatalogEntry(CatalogType::DATABASE_ENTRY, catalog_p, std::move(name_p)), db(db),
      type(access_mode == AccessMode::READ_ONLY ? AttachedDatabaseType::READ_ONLY_DATABASE
                                                : AttachedDatabaseType::READ_WRITE_DATABASE),
      parent_catalog(&catalog_p) {
	catalog = storage_extension.attach(storage_extension.storage_info.get(), *this, name, info, access_mode);
	if (!catalog) {
		throw InternalException("AttachedDatabase - attach function did not return a catalog");
	}
	transaction_manager =
	    storage_extension.create_transaction_manager(storage_extension.storage_info.get(), *this, *catalog);
	if (!transaction_manager) {
		throw InternalException(
		    "AttachedDatabase - create_transaction_manager function did not return a transaction manager");
	}
	internal = true;
}

AttachedDatabase::~AttachedDatabase() {
	if (Exception::UncaughtException()) {
		return;
	}
	if (!storage) {
		return;
	}

	// shutting down: attempt to checkpoint the database
	// but only if we are not cleaning up as part of an exception unwind
	try {
		if (!storage->InMemory()) {
			auto &config = DBConfig::GetConfig(db);
			if (!config.options.checkpoint_on_shutdown) {
				return;
			}
			storage->CreateCheckpoint(true);
		}
	} catch (...) {
	}
}

bool AttachedDatabase::IsSystem() const {
	D_ASSERT(!storage || type != AttachedDatabaseType::SYSTEM_DATABASE);
	return type == AttachedDatabaseType::SYSTEM_DATABASE;
}

bool AttachedDatabase::IsTemporary() const {
	return type == AttachedDatabaseType::TEMP_DATABASE;
}
bool AttachedDatabase::IsReadOnly() const {
	return type == AttachedDatabaseType::READ_ONLY_DATABASE;
}

string AttachedDatabase::ExtractDatabaseName(const string &dbpath, FileSystem &fs) {
	if (dbpath.empty() || dbpath == ":memory:") {
		return "memory";
	}
	return fs.ExtractBaseName(dbpath);
}

void AttachedDatabase::Initialize() {
	if (IsSystem()) {
		catalog->Initialize(true);
	} else {
		catalog->Initialize(false);
	}
	if (storage) {
		storage->Initialize();
	}
}

StorageManager &AttachedDatabase::GetStorageManager() {
	if (!storage) {
		throw InternalException("Internal system catalog does not have storage");
	}
	return *storage;
}

Catalog &AttachedDatabase::GetCatalog() {
	return *catalog;
}

TransactionManager &AttachedDatabase::GetTransactionManager() {
	return *transaction_manager;
}

Catalog &AttachedDatabase::ParentCatalog() {
	return *parent_catalog;
}

bool AttachedDatabase::IsInitialDatabase() const {
	return is_initial_database;
}

void AttachedDatabase::SetInitialDatabase() {
	is_initial_database = true;
}

} // namespace duckdb


using duckdb::Appender;
using duckdb::AppenderWrapper;
using duckdb::Connection;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::string_t;
using duckdb::timestamp_t;

duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                    duckdb_appender *out_appender) {
	Connection *conn = reinterpret_cast<Connection *>(connection);

	if (!connection || !table || !out_appender) {
		return DuckDBError;
	}
	if (schema == nullptr) {
		schema = DEFAULT_SCHEMA;
	}
	auto wrapper = new AppenderWrapper();
	*out_appender = (duckdb_appender)wrapper;
	try {
		wrapper->appender = duckdb::make_uniq<Appender>(*conn, schema, table);
	} catch (std::exception &ex) {
		wrapper->error = ex.what();
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown create appender error";
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_destroy(duckdb_appender *appender) {
	if (!appender || !*appender) {
		return DuckDBError;
	}
	duckdb_appender_close(*appender);
	auto wrapper = reinterpret_cast<AppenderWrapper *>(*appender);
	if (wrapper) {
		delete wrapper;
	}
	*appender = nullptr;
	return DuckDBSuccess;
}

template <class FUN>
duckdb_state duckdb_appender_run_function(duckdb_appender appender, FUN &&function) {
	if (!appender) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (!wrapper->appender) {
		return DuckDBError;
	}
	try {
		function(*wrapper->appender);
	} catch (std::exception &ex) {
		wrapper->error = ex.what();
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		wrapper->error = "Unknown error";
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

const char *duckdb_appender_error(duckdb_appender appender) {
	if (!appender) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<AppenderWrapper *>(appender);
	if (wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

duckdb_state duckdb_appender_begin_row(duckdb_appender appender) {
	return DuckDBSuccess;
}

duckdb_state duckdb_appender_end_row(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.EndRow(); });
}

template <class T>
duckdb_state duckdb_append_internal(duckdb_appender appender, T value) {
	if (!appender) {
		return DuckDBError;
	}
	auto *appender_instance = reinterpret_cast<AppenderWrapper *>(appender);
	try {
		appender_instance->appender->Append<T>(value);
	} catch (std::exception &ex) {
		appender_instance->error = ex.what();
		return DuckDBError;
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_append_bool(duckdb_appender appender, bool value) {
	return duckdb_append_internal<bool>(appender, value);
}

duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value) {
	return duckdb_append_internal<int8_t>(appender, value);
}

duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value) {
	return duckdb_append_internal<int16_t>(appender, value);
}

duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value) {
	return duckdb_append_internal<int32_t>(appender, value);
}

duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value) {
	return duckdb_append_internal<int64_t>(appender, value);
}

duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value) {
	hugeint_t internal;
	internal.lower = value.lower;
	internal.upper = value.upper;
	return duckdb_append_internal<hugeint_t>(appender, internal);
}

duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value) {
	return duckdb_append_internal<uint8_t>(appender, value);
}

duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value) {
	return duckdb_append_internal<uint16_t>(appender, value);
}

duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value) {
	return duckdb_append_internal<uint32_t>(appender, value);
}

duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value) {
	return duckdb_append_internal<uint64_t>(appender, value);
}

duckdb_state duckdb_append_float(duckdb_appender appender, float value) {
	return duckdb_append_internal<float>(appender, value);
}

duckdb_state duckdb_append_double(duckdb_appender appender, double value) {
	return duckdb_append_internal<double>(appender, value);
}

duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value) {
	return duckdb_append_internal<date_t>(appender, date_t(value.days));
}

duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value) {
	return duckdb_append_internal<dtime_t>(appender, dtime_t(value.micros));
}

duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value) {
	return duckdb_append_internal<timestamp_t>(appender, timestamp_t(value.micros));
}

duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value) {
	interval_t interval;
	interval.months = value.months;
	interval.days = value.days;
	interval.micros = value.micros;
	return duckdb_append_internal<interval_t>(appender, interval);
}

duckdb_state duckdb_append_null(duckdb_appender appender) {
	return duckdb_append_internal<std::nullptr_t>(appender, nullptr);
}

duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val) {
	return duckdb_append_internal<const char *>(appender, val);
}

duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length) {
	return duckdb_append_internal<string_t>(appender, string_t(val, length));
}
duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length) {
	auto value = duckdb::Value::BLOB((duckdb::const_data_ptr_t)data, length);
	return duckdb_append_internal<duckdb::Value>(appender, value);
}

duckdb_state duckdb_appender_flush(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Flush(); });
}

duckdb_state duckdb_appender_close(duckdb_appender appender) {
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.Close(); });
}

duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk) {
	if (!chunk) {
		return DuckDBError;
	}
	auto data_chunk = (duckdb::DataChunk *)chunk;
	return duckdb_appender_run_function(appender, [&](Appender &appender) { appender.AppendDataChunk(*data_chunk); });
}






using duckdb::ArrowConverter;
using duckdb::ArrowResultWrapper;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResult;
using duckdb::QueryResultType;

duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result) {
	Connection *conn = (Connection *)connection;
	auto wrapper = new ArrowResultWrapper();
	wrapper->result = conn->Query(query);
	*out_result = (duckdb_arrow)wrapper;
	return !wrapper->result->HasError() ? DuckDBSuccess : DuckDBError;
}

duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema) {
	if (!out_schema) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	ArrowConverter::ToArrowSchema((ArrowSchema *)*out_schema, wrapper->result->types, wrapper->result->names,
	                              wrapper->options);
	return DuckDBSuccess;
}

duckdb_state duckdb_prepared_arrow_schema(duckdb_prepared_statement prepared, duckdb_arrow_schema *out_schema) {
	if (!out_schema) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared);
	if (!wrapper || !wrapper->statement || !wrapper->statement->data) {
		return DuckDBError;
	}
	auto properties = wrapper->statement->context->GetClientProperties();
	duckdb::vector<duckdb::LogicalType> prepared_types;
	duckdb::vector<duckdb::string> prepared_names;

	auto count = wrapper->statement->data->properties.parameter_count;
	for (idx_t i = 0; i < count; i++) {
		// Every prepared parameter type is UNKNOWN, which we need to map to NULL according to the spec of
		// 'AdbcStatementGetParameterSchema'
		auto type = LogicalType::SQLNULL;

		// FIXME: we don't support named parameters yet, but when we do, this needs to be updated
		auto name = std::to_string(i);
		prepared_types.push_back(std::move(type));
		prepared_names.push_back(name);
	}

	auto result_schema = (ArrowSchema *)*out_schema;
	if (!result_schema) {
		return DuckDBError;
	}

	if (result_schema->release) {
		// Need to release the existing schema before we overwrite it
		result_schema->release(result_schema);
		result_schema->release = nullptr;
	}

	ArrowConverter::ToArrowSchema(result_schema, prepared_types, prepared_names, properties);
	return DuckDBSuccess;
}

duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array) {
	if (!out_array) {
		return DuckDBSuccess;
	}
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	auto success = wrapper->result->TryFetch(wrapper->current_chunk, wrapper->result->GetErrorObject());
	if (!success) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (!wrapper->current_chunk || wrapper->current_chunk->size() == 0) {
		return DuckDBSuccess;
	}
	ArrowConverter::ToArrowArray(*wrapper->current_chunk, reinterpret_cast<ArrowArray *>(*out_array), wrapper->options);
	return DuckDBSuccess;
}

idx_t duckdb_arrow_row_count(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	if (wrapper->result->HasError()) {
		return 0;
	}
	return wrapper->result->RowCount();
}

idx_t duckdb_arrow_column_count(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	return wrapper->result->ColumnCount();
}

idx_t duckdb_arrow_rows_changed(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	if (wrapper->result->HasError()) {
		return 0;
	}
	idx_t rows_changed = 0;
	auto &collection = wrapper->result->Collection();
	idx_t row_count = collection.Count();
	if (row_count > 0 && wrapper->result->properties.return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
		auto rows = collection.GetRows();
		D_ASSERT(row_count == 1);
		D_ASSERT(rows.size() == 1);
		rows_changed = rows[0].GetValue(0).GetValue<int64_t>();
	}
	return rows_changed;
}

const char *duckdb_query_arrow_error(duckdb_arrow result) {
	auto wrapper = reinterpret_cast<ArrowResultWrapper *>(result);
	return wrapper->result->GetError().c_str();
}

void duckdb_destroy_arrow(duckdb_arrow *result) {
	if (*result) {
		auto wrapper = reinterpret_cast<ArrowResultWrapper *>(*result);
		delete wrapper;
		*result = nullptr;
	}
}

duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement, duckdb_arrow *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError() || !out_result) {
		return DuckDBError;
	}
	auto arrow_wrapper = new ArrowResultWrapper();
	arrow_wrapper->options = wrapper->statement->context->GetClientProperties();

	auto result = wrapper->statement->Execute(wrapper->values, false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	arrow_wrapper->result = duckdb::unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	*out_result = reinterpret_cast<duckdb_arrow>(arrow_wrapper);
	return !arrow_wrapper->result->HasError() ? DuckDBSuccess : DuckDBError;
}

namespace arrow_array_stream_wrapper {
namespace {
struct PrivateData {
	ArrowSchema *schema;
	ArrowArray *array;
	bool done = false;
};

// LCOV_EXCL_START
// This function is never called, but used to set ArrowSchema's release functions to a non-null NOOP.
void EmptySchemaRelease(ArrowSchema *) {
}
// LCOV_EXCL_STOP

void EmptyArrayRelease(ArrowArray *) {
}

void EmptyStreamRelease(ArrowArrayStream *) {
}

void FactoryGetSchema(uintptr_t stream_factory_ptr, duckdb::ArrowSchemaWrapper &schema) {
	auto stream = reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr);
	stream->get_schema(stream, &schema.arrow_schema);

	// Need to nullify the root schema's release function here, because streams don't allow us to set the release
	// function. For the schema's children, we nullify the release functions in `duckdb_arrow_scan`, so we don't need to
	// handle them again here. We set this to nullptr and not EmptySchemaRelease to prevent ArrowSchemaWrapper's
	// destructor from destroying the schema (it's the caller's responsibility).
	schema.arrow_schema.release = nullptr;
}

int GetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	auto private_data = static_cast<arrow_array_stream_wrapper::PrivateData *>((stream->private_data));
	if (private_data->schema == nullptr) {
		return DuckDBError;
	}

	*out = *private_data->schema;
	out->release = EmptySchemaRelease;
	return DuckDBSuccess;
}

int GetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	auto private_data = static_cast<arrow_array_stream_wrapper::PrivateData *>((stream->private_data));
	*out = *private_data->array;
	if (private_data->done) {
		out->release = nullptr;
	} else {
		out->release = EmptyArrayRelease;
	}

	private_data->done = true;
	return DuckDBSuccess;
}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> FactoryGetNext(uintptr_t stream_factory_ptr,
                                                                   duckdb::ArrowStreamParameters &parameters) {
	auto stream = reinterpret_cast<ArrowArrayStream *>(stream_factory_ptr);
	auto ret = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
	ret->arrow_array_stream = *stream;
	ret->arrow_array_stream.release = EmptyStreamRelease;
	return ret;
}

// LCOV_EXCL_START
// This function is never be called, because it's used to construct a stream wrapping around a caller-supplied
// ArrowArray. Thus, the stream itself cannot produce an error.
const char *GetLastError(struct ArrowArrayStream *stream) {
	return nullptr;
}
// LCOV_EXCL_STOP

void Release(struct ArrowArrayStream *stream) {
	if (stream->private_data != nullptr) {
		delete reinterpret_cast<PrivateData *>(stream->private_data);
	}

	stream->private_data = nullptr;
	stream->release = nullptr;
}

duckdb_state Ingest(duckdb_connection connection, const char *table_name, struct ArrowArrayStream *input) {
	try {
		auto cconn = reinterpret_cast<duckdb::Connection *>(connection);
		cconn
		    ->TableFunction("arrow_scan", {duckdb::Value::POINTER((uintptr_t)input),
		                                   duckdb::Value::POINTER((uintptr_t)FactoryGetNext),
		                                   duckdb::Value::POINTER((uintptr_t)FactoryGetSchema)})
		    ->CreateView(table_name, true, false);
	} catch (...) { // LCOV_EXCL_START
		// Tried covering this in tests, but it proved harder than expected. At the time of writing:
		// - Passing any name to `CreateView` worked without throwing an exception
		// - Passing a null Arrow array worked without throwing an exception
		// - Passing an invalid schema (without any columns) led to an InternalException with SIGABRT, which is meant to
		//   be un-catchable. This case likely needs to be handled gracefully within `arrow_scan`.
		// Ref: https://discord.com/channels/909674491309850675/921100573732909107/1115230468699336785
		return DuckDBError;
	} // LCOV_EXCL_STOP

	return DuckDBSuccess;
}
} // namespace
} // namespace arrow_array_stream_wrapper

duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name, duckdb_arrow_stream arrow) {
	auto stream = reinterpret_cast<ArrowArrayStream *>(arrow);

	// Backup release functions - we nullify children schema release functions because we don't want to release on
	// behalf of the caller, downstream in our code. Note that Arrow releases target immediate children, but aren't
	// recursive. So we only back up immediate children here and restore their functions.
	ArrowSchema schema;
	if (stream->get_schema(stream, &schema) == DuckDBError) {
		return DuckDBError;
	}

	typedef void (*release_fn_t)(ArrowSchema *);
	std::vector<release_fn_t> release_fns(schema.n_children);
	for (int64_t i = 0; i < schema.n_children; i++) {
		auto child = schema.children[i];
		release_fns[i] = child->release;
		child->release = arrow_array_stream_wrapper::EmptySchemaRelease;
	}

	auto ret = arrow_array_stream_wrapper::Ingest(connection, table_name, stream);

	// Restore release functions.
	for (int64_t i = 0; i < schema.n_children; i++) {
		schema.children[i]->release = release_fns[i];
	}

	return ret;
}

duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name,
                                     duckdb_arrow_schema arrow_schema, duckdb_arrow_array arrow_array,
                                     duckdb_arrow_stream *out_stream) {
	auto private_data = new arrow_array_stream_wrapper::PrivateData;
	private_data->schema = reinterpret_cast<ArrowSchema *>(arrow_schema);
	private_data->array = reinterpret_cast<ArrowArray *>(arrow_array);
	private_data->done = false;

	ArrowArrayStream *stream = new ArrowArrayStream;
	*out_stream = reinterpret_cast<duckdb_arrow_stream>(stream);
	stream->get_schema = arrow_array_stream_wrapper::GetSchema;
	stream->get_next = arrow_array_stream_wrapper::GetNext;
	stream->get_last_error = arrow_array_stream_wrapper::GetLastError;
	stream->release = arrow_array_stream_wrapper::Release;
	stream->private_data = private_data;

	return duckdb_arrow_scan(connection, table_name, reinterpret_cast<duckdb_arrow_stream>(stream));
}



namespace duckdb {

//! DECIMAL -> VARCHAR
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_string &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	auto width = duckdb::DecimalType::GetWidth(source_type);
	auto scale = duckdb::DecimalType::GetScale(source_type);
	duckdb::Vector result_vec(duckdb::LogicalType::VARCHAR, false, false);
	duckdb::string_t result_string;
	void *source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);
	switch (source_type.InternalType()) {
	case duckdb::PhysicalType::INT16:
		result_string = duckdb::StringCastFromDecimal::Operation<int16_t>(UnsafeFetchFromPtr<int16_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT32:
		result_string = duckdb::StringCastFromDecimal::Operation<int32_t>(UnsafeFetchFromPtr<int32_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT64:
		result_string = duckdb::StringCastFromDecimal::Operation<int64_t>(UnsafeFetchFromPtr<int64_t>(source_address),
		                                                                  width, scale, result_vec);
		break;
	case duckdb::PhysicalType::INT128:
		result_string = duckdb::StringCastFromDecimal::Operation<hugeint_t>(
		    UnsafeFetchFromPtr<hugeint_t>(source_address), width, scale, result_vec);
		break;
	default:
		throw duckdb::InternalException("Unimplemented internal type for decimal");
	}
	result.data = reinterpret_cast<char *>(duckdb_malloc(sizeof(char) * (result_string.GetSize() + 1)));
	memcpy(result.data, result_string.GetData(), result_string.GetSize());
	result.data[result_string.GetSize()] = '\0';
	result.size = result_string.GetSize();
	return true;
}

template <class INTERNAL_TYPE>
duckdb_hugeint FetchInternals(void *source_address) {
	throw duckdb::NotImplementedException("FetchInternals not implemented for internal type");
}

template <>
duckdb_hugeint FetchInternals<int16_t>(void *source_address) {
	duckdb_hugeint result;
	int16_t intermediate_result;

	if (!TryCast::Operation<int16_t, int16_t>(UnsafeFetchFromPtr<int16_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int16_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int16_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<int32_t>(void *source_address) {
	duckdb_hugeint result;
	int32_t intermediate_result;

	if (!TryCast::Operation<int32_t, int32_t>(UnsafeFetchFromPtr<int32_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int32_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int32_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<int64_t>(void *source_address) {
	duckdb_hugeint result;
	int64_t intermediate_result;

	if (!TryCast::Operation<int64_t, int64_t>(UnsafeFetchFromPtr<int64_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<int64_t>();
	}
	hugeint_t hugeint_result = Hugeint::Cast<int64_t>(intermediate_result);
	result.lower = hugeint_result.lower;
	result.upper = hugeint_result.upper;
	return result;
}
template <>
duckdb_hugeint FetchInternals<hugeint_t>(void *source_address) {
	duckdb_hugeint result;
	hugeint_t intermediate_result;

	if (!TryCast::Operation<hugeint_t, hugeint_t>(UnsafeFetchFromPtr<hugeint_t>(source_address), intermediate_result)) {
		intermediate_result = FetchDefaultValue::Operation<hugeint_t>();
	}
	result.lower = intermediate_result.lower;
	result.upper = intermediate_result.upper;
	return result;
}

//! DECIMAL -> DECIMAL (internal fetch)
template <>
bool CastDecimalCInternal(duckdb_result *source, duckdb_decimal &result, idx_t col, idx_t row) {
	auto result_data = (duckdb::DuckDBResultData *)source->internal_data;
	result_data->result->types[col].GetDecimalProperties(result.width, result.scale);
	auto source_address = UnsafeFetchPtr<hugeint_t>(source, col, row);

	if (result.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		result.value = FetchInternals<hugeint_t>(source_address);
	} else if (result.width > duckdb::Decimal::MAX_WIDTH_INT32) {
		result.value = FetchInternals<int64_t>(source_address);
	} else if (result.width > duckdb::Decimal::MAX_WIDTH_INT16) {
		result.value = FetchInternals<int32_t>(source_address);
	} else {
		result.value = FetchInternals<int16_t>(source_address);
	}
	return true;
}

} // namespace duckdb


namespace duckdb {

template <>
duckdb_decimal FetchDefaultValue::Operation() {
	duckdb_decimal result;
	result.scale = 0;
	result.width = 0;
	result.value = {0, 0};
	return result;
}

template <>
date_t FetchDefaultValue::Operation() {
	date_t result;
	result.days = 0;
	return result;
}

template <>
dtime_t FetchDefaultValue::Operation() {
	dtime_t result;
	result.micros = 0;
	return result;
}

template <>
timestamp_t FetchDefaultValue::Operation() {
	timestamp_t result;
	result.value = 0;
	return result;
}

template <>
interval_t FetchDefaultValue::Operation() {
	interval_t result;
	result.months = 0;
	result.days = 0;
	result.micros = 0;
	return result;
}

template <>
char *FetchDefaultValue::Operation() {
	return nullptr;
}

template <>
duckdb_string FetchDefaultValue::Operation() {
	duckdb_string result;
	result.data = nullptr;
	result.size = 0;
	return result;
}

template <>
duckdb_blob FetchDefaultValue::Operation() {
	duckdb_blob result;
	result.data = nullptr;
	result.size = 0;
	return result;
}

//===--------------------------------------------------------------------===//
// Blob Casts
//===--------------------------------------------------------------------===//

template <>
bool FromCBlobCastWrapper::Operation(duckdb_blob input, duckdb_string &result) {
	string_t input_str(const_char_ptr_cast(input.data), input.size);
	return ToCStringCastWrapper<duckdb::CastFromBlob>::template Operation<string_t, duckdb_string>(input_str, result);
}

} // namespace duckdb

bool CanUseDeprecatedFetch(duckdb_result *result, idx_t col, idx_t row) {
	if (!result) {
		return false;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return false;
	}
	if (col >= result->__deprecated_column_count || row >= result->__deprecated_row_count) {
		return false;
	}
	return true;
}

bool CanFetchValue(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	if (result->__deprecated_columns[col].__deprecated_nullmask[row]) {
		return false;
	}
	return true;
}




using duckdb::DBConfig;
using duckdb::Value;

// config
duckdb_state duckdb_create_config(duckdb_config *out_config) {
	if (!out_config) {
		return DuckDBError;
	}
	DBConfig *config;
	try {
		config = new DBConfig();
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out_config = reinterpret_cast<duckdb_config>(config);
	return DuckDBSuccess;
}

size_t duckdb_config_count() {
	return DBConfig::GetOptionCount();
}

duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description) {
	auto option = DBConfig::GetOptionByIndex(index);
	if (!option) {
		return DuckDBError;
	}
	if (out_name) {
		*out_name = option->name;
	}
	if (out_description) {
		*out_description = option->description;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option) {
	if (!config || !name || !option) {
		return DuckDBError;
	}

	try {
		auto db_config = (DBConfig *)config;
		db_config->SetOptionByName(name, Value(option));
	} catch (...) {
		return DuckDBError;
	}
	return DuckDBSuccess;
}

void duckdb_destroy_config(duckdb_config *config) {
	if (!config) {
		return;
	}
	if (*config) {
		auto db_config = (DBConfig *)*config;
		delete db_config;
		*config = nullptr;
	}
}




#include <string.h>

duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *ctypes, idx_t column_count) {
	if (!ctypes) {
		return nullptr;
	}
	duckdb::vector<duckdb::LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		auto ltype = reinterpret_cast<duckdb::LogicalType *>(ctypes[i]);
		types.push_back(*ltype);
	}

	auto result = new duckdb::DataChunk();
	result->Initialize(duckdb::Allocator::DefaultAllocator(), types);
	return reinterpret_cast<duckdb_data_chunk>(result);
}

void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk) {
	if (chunk && *chunk) {
		auto dchunk = reinterpret_cast<duckdb::DataChunk *>(*chunk);
		delete dchunk;
		*chunk = nullptr;
	}
}

void duckdb_data_chunk_reset(duckdb_data_chunk chunk) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	dchunk->Reset();
}

idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return dchunk->ColumnCount();
}

duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= duckdb_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return reinterpret_cast<duckdb_vector>(&dchunk->data[col_idx]);
}

idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	return dchunk->size();
}

void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<duckdb::DataChunk *>(chunk);
	dchunk->SetCardinality(size);
}

duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(v->GetType()));
}

void *duckdb_vector_get_data(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return duckdb::FlatVector::GetData(*v);
}

uint64_t *duckdb_vector_get_validity(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return duckdb::FlatVector::Validity(*v).GetData();
}

void duckdb_vector_ensure_validity_writable(duckdb_vector vector) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	auto &validity = duckdb::FlatVector::Validity(*v);
	validity.EnsureWritable();
}

void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str) {
	duckdb_vector_assign_string_element_len(vector, index, str, strlen(str));
}

void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str, idx_t str_len) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	auto data = duckdb::FlatVector::GetData<duckdb::string_t>(*v);
	data[index] = duckdb::StringVector::AddString(*v, str, str_len);
}

duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_vector>(&duckdb::ListVector::GetEntry(*v));
}

idx_t duckdb_list_vector_get_size(duckdb_vector vector) {
	if (!vector) {
		return 0;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return duckdb::ListVector::GetListSize(*v);
}

duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size) {
	if (!vector) {
		return duckdb_state::DuckDBError;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	duckdb::ListVector::SetListSize(*v, size);
	return duckdb_state::DuckDBSuccess;
}

duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity) {
	if (!vector) {
		return duckdb_state::DuckDBError;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	duckdb::ListVector::Reserve(*v, required_capacity);
	return duckdb_state::DuckDBSuccess;
}

duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<duckdb::Vector *>(vector);
	return reinterpret_cast<duckdb_vector>(duckdb::StructVector::GetEntries(*v)[index].get());
}

bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return true;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	return validity[entry_idx] & ((idx_t)1 << idx_in_entry);
}

void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid) {
	if (valid) {
		duckdb_validity_set_row_valid(validity, row);
	} else {
		duckdb_validity_set_row_invalid(validity, row);
	}
}

void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] &= ~((uint64_t)1 << idx_in_entry);
}

void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] |= (uint64_t)1 << idx_in_entry;
}





using duckdb::Date;
using duckdb::Time;
using duckdb::Timestamp;

using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::timestamp_t;

duckdb_date_struct duckdb_from_date(duckdb_date date) {
	int32_t year, month, day;
	Date::Convert(date_t(date.days), year, month, day);

	duckdb_date_struct result;
	result.year = year;
	result.month = month;
	result.day = day;
	return result;
}

duckdb_date duckdb_to_date(duckdb_date_struct date) {
	duckdb_date result;
	result.days = Date::FromDate(date.year, date.month, date.day).days;
	return result;
}

duckdb_time_struct duckdb_from_time(duckdb_time time) {
	int32_t hour, minute, second, micros;
	Time::Convert(dtime_t(time.micros), hour, minute, second, micros);

	duckdb_time_struct result;
	result.hour = hour;
	result.min = minute;
	result.sec = second;
	result.micros = micros;
	return result;
}

duckdb_time duckdb_to_time(duckdb_time_struct time) {
	duckdb_time result;
	result.micros = Time::FromTime(time.hour, time.min, time.sec, time.micros).micros;
	return result;
}

duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp_t(ts.micros), date, time);

	duckdb_date ddate;
	ddate.days = date.days;

	duckdb_time dtime;
	dtime.micros = time.micros;

	duckdb_timestamp_struct result;
	result.date = duckdb_from_date(ddate);
	result.time = duckdb_from_time(dtime);
	return result;
}

duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts) {
	date_t date = date_t(duckdb_to_date(ts.date).days);
	dtime_t time = dtime_t(duckdb_to_time(ts.time).micros);

	duckdb_timestamp result;
	result.micros = Timestamp::FromDatetime(date, time).value;
	return result;
}


using duckdb::Connection;
using duckdb::DatabaseData;
using duckdb::DBConfig;
using duckdb::DuckDB;

duckdb_state duckdb_open_ext(const char *path, duckdb_database *out, duckdb_config config, char **error) {
	auto wrapper = new DatabaseData();
	try {
		auto db_config = (DBConfig *)config;
		wrapper->database = duckdb::make_uniq<DuckDB>(path, db_config);
	} catch (std::exception &ex) {
		if (error) {
			*error = strdup(ex.what());
		}
		delete wrapper;
		return DuckDBError;
	} catch (...) { // LCOV_EXCL_START
		if (error) {
			*error = strdup("Unknown error");
		}
		delete wrapper;
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out = (duckdb_database)wrapper;
	return DuckDBSuccess;
}

duckdb_state duckdb_open(const char *path, duckdb_database *out) {
	return duckdb_open_ext(path, out, nullptr, nullptr);
}

void duckdb_close(duckdb_database *database) {
	if (database && *database) {
		auto wrapper = reinterpret_cast<DatabaseData *>(*database);
		delete wrapper;
		*database = nullptr;
	}
}

duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out) {
	if (!database || !out) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<DatabaseData *>(database);
	Connection *connection;
	try {
		connection = new Connection(*wrapper->database);
	} catch (...) { // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	*out = (duckdb_connection)connection;
	return DuckDBSuccess;
}

void duckdb_interrupt(duckdb_connection connection) {
	if (!connection) {
		return;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	conn->Interrupt();
}

double duckdb_query_progress(duckdb_connection connection) {
	if (!connection) {
		return -1;
	}
	Connection *conn = reinterpret_cast<Connection *>(connection);
	return conn->context->GetProgress();
}

void duckdb_disconnect(duckdb_connection *connection) {
	if (connection && *connection) {
		Connection *conn = reinterpret_cast<Connection *>(*connection);
		delete conn;
		*connection = nullptr;
	}
}

duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto result = conn->Query(query);
	return duckdb_translate_result(std::move(result), out);
}

const char *duckdb_library_version() {
	return DuckDB::LibraryVersion();
}


void duckdb_destroy_value(duckdb_value *value) {
	if (value && *value) {
		auto val = reinterpret_cast<duckdb::Value *>(*value);
		delete val;
		*value = nullptr;
	}
}

duckdb_value duckdb_create_varchar_length(const char *text, idx_t length) {
	return reinterpret_cast<duckdb_value>(new duckdb::Value(std::string(text, length)));
}

duckdb_value duckdb_create_varchar(const char *text) {
	return duckdb_create_varchar_length(text, strlen(text));
}

duckdb_value duckdb_create_int64(int64_t input) {
	auto val = duckdb::Value::BIGINT(input);
	return reinterpret_cast<duckdb_value>(new duckdb::Value(val));
}

char *duckdb_get_varchar(duckdb_value value) {
	auto val = reinterpret_cast<duckdb::Value *>(value);
	auto str_val = val->DefaultCastAs(duckdb::LogicalType::VARCHAR);
	auto &str = duckdb::StringValue::Get(str_val);

	auto result = reinterpret_cast<char *>(malloc(sizeof(char) * (str.size() + 1)));
	memcpy(result, str.c_str(), str.size());
	result[str.size()] = '\0';
	return result;
}

int64_t duckdb_get_int64(duckdb_value value) {
	auto val = reinterpret_cast<duckdb::Value *>(value);
	if (!val->DefaultTryCastAs(duckdb::LogicalType::BIGINT)) {
		return 0;
	}
	return duckdb::BigIntValue::Get(*val);
}


namespace duckdb {

LogicalTypeId ConvertCTypeToCPP(duckdb_type c_type) {
	switch (c_type) {
	case DUCKDB_TYPE_BOOLEAN:
		return LogicalTypeId::BOOLEAN;
	case DUCKDB_TYPE_TINYINT:
		return LogicalTypeId::TINYINT;
	case DUCKDB_TYPE_SMALLINT:
		return LogicalTypeId::SMALLINT;
	case DUCKDB_TYPE_INTEGER:
		return LogicalTypeId::INTEGER;
	case DUCKDB_TYPE_BIGINT:
		return LogicalTypeId::BIGINT;
	case DUCKDB_TYPE_UTINYINT:
		return LogicalTypeId::UTINYINT;
	case DUCKDB_TYPE_USMALLINT:
		return LogicalTypeId::USMALLINT;
	case DUCKDB_TYPE_UINTEGER:
		return LogicalTypeId::UINTEGER;
	case DUCKDB_TYPE_UBIGINT:
		return LogicalTypeId::UBIGINT;
	case DUCKDB_TYPE_HUGEINT:
		return LogicalTypeId::HUGEINT;
	case DUCKDB_TYPE_FLOAT:
		return LogicalTypeId::FLOAT;
	case DUCKDB_TYPE_DOUBLE:
		return LogicalTypeId::DOUBLE;
	case DUCKDB_TYPE_TIMESTAMP:
		return LogicalTypeId::TIMESTAMP;
	case DUCKDB_TYPE_DATE:
		return LogicalTypeId::DATE;
	case DUCKDB_TYPE_TIME:
		return LogicalTypeId::TIME;
	case DUCKDB_TYPE_VARCHAR:
		return LogicalTypeId::VARCHAR;
	case DUCKDB_TYPE_BLOB:
		return LogicalTypeId::BLOB;
	case DUCKDB_TYPE_INTERVAL:
		return LogicalTypeId::INTERVAL;
	case DUCKDB_TYPE_TIMESTAMP_S:
		return LogicalTypeId::TIMESTAMP_SEC;
	case DUCKDB_TYPE_TIMESTAMP_MS:
		return LogicalTypeId::TIMESTAMP_MS;
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return LogicalTypeId::TIMESTAMP_NS;
	case DUCKDB_TYPE_UUID:
		return LogicalTypeId::UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return LogicalTypeId::INVALID;
	} // LCOV_EXCL_STOP
}

duckdb_type ConvertCPPTypeToC(const LogicalType &sql_type) {
	switch (sql_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return DUCKDB_TYPE_BOOLEAN;
	case LogicalTypeId::TINYINT:
		return DUCKDB_TYPE_TINYINT;
	case LogicalTypeId::SMALLINT:
		return DUCKDB_TYPE_SMALLINT;
	case LogicalTypeId::INTEGER:
		return DUCKDB_TYPE_INTEGER;
	case LogicalTypeId::BIGINT:
		return DUCKDB_TYPE_BIGINT;
	case LogicalTypeId::UTINYINT:
		return DUCKDB_TYPE_UTINYINT;
	case LogicalTypeId::USMALLINT:
		return DUCKDB_TYPE_USMALLINT;
	case LogicalTypeId::UINTEGER:
		return DUCKDB_TYPE_UINTEGER;
	case LogicalTypeId::UBIGINT:
		return DUCKDB_TYPE_UBIGINT;
	case LogicalTypeId::HUGEINT:
		return DUCKDB_TYPE_HUGEINT;
	case LogicalTypeId::FLOAT:
		return DUCKDB_TYPE_FLOAT;
	case LogicalTypeId::DOUBLE:
		return DUCKDB_TYPE_DOUBLE;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return DUCKDB_TYPE_TIMESTAMP;
	case LogicalTypeId::TIMESTAMP_SEC:
		return DUCKDB_TYPE_TIMESTAMP_S;
	case LogicalTypeId::TIMESTAMP_MS:
		return DUCKDB_TYPE_TIMESTAMP_MS;
	case LogicalTypeId::TIMESTAMP_NS:
		return DUCKDB_TYPE_TIMESTAMP_NS;
	case LogicalTypeId::DATE:
		return DUCKDB_TYPE_DATE;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return DUCKDB_TYPE_TIME;
	case LogicalTypeId::VARCHAR:
		return DUCKDB_TYPE_VARCHAR;
	case LogicalTypeId::BLOB:
		return DUCKDB_TYPE_BLOB;
	case LogicalTypeId::BIT:
		return DUCKDB_TYPE_BIT;
	case LogicalTypeId::INTERVAL:
		return DUCKDB_TYPE_INTERVAL;
	case LogicalTypeId::DECIMAL:
		return DUCKDB_TYPE_DECIMAL;
	case LogicalTypeId::ENUM:
		return DUCKDB_TYPE_ENUM;
	case LogicalTypeId::LIST:
		return DUCKDB_TYPE_LIST;
	case LogicalTypeId::STRUCT:
		return DUCKDB_TYPE_STRUCT;
	case LogicalTypeId::MAP:
		return DUCKDB_TYPE_MAP;
	case LogicalTypeId::UNION:
		return DUCKDB_TYPE_UNION;
	case LogicalTypeId::UUID:
		return DUCKDB_TYPE_UUID;
	default: // LCOV_EXCL_START
		D_ASSERT(0);
		return DUCKDB_TYPE_INVALID;
	} // LCOV_EXCL_STOP
}

idx_t GetCTypeSize(duckdb_type type) {
	switch (type) {
	case DUCKDB_TYPE_BOOLEAN:
		return sizeof(bool);
	case DUCKDB_TYPE_TINYINT:
		return sizeof(int8_t);
	case DUCKDB_TYPE_SMALLINT:
		return sizeof(int16_t);
	case DUCKDB_TYPE_INTEGER:
		return sizeof(int32_t);
	case DUCKDB_TYPE_BIGINT:
		return sizeof(int64_t);
	case DUCKDB_TYPE_UTINYINT:
		return sizeof(uint8_t);
	case DUCKDB_TYPE_USMALLINT:
		return sizeof(uint16_t);
	case DUCKDB_TYPE_UINTEGER:
		return sizeof(uint32_t);
	case DUCKDB_TYPE_UBIGINT:
		return sizeof(uint64_t);
	case DUCKDB_TYPE_HUGEINT:
	case DUCKDB_TYPE_UUID:
		return sizeof(duckdb_hugeint);
	case DUCKDB_TYPE_FLOAT:
		return sizeof(float);
	case DUCKDB_TYPE_DOUBLE:
		return sizeof(double);
	case DUCKDB_TYPE_DATE:
		return sizeof(duckdb_date);
	case DUCKDB_TYPE_TIME:
		return sizeof(duckdb_time);
	case DUCKDB_TYPE_TIMESTAMP:
	case DUCKDB_TYPE_TIMESTAMP_S:
	case DUCKDB_TYPE_TIMESTAMP_MS:
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return sizeof(duckdb_timestamp);
	case DUCKDB_TYPE_VARCHAR:
		return sizeof(const char *);
	case DUCKDB_TYPE_BLOB:
		return sizeof(duckdb_blob);
	case DUCKDB_TYPE_INTERVAL:
		return sizeof(duckdb_interval);
	case DUCKDB_TYPE_DECIMAL:
		return sizeof(duckdb_hugeint);
	default: // LCOV_EXCL_START
		// unsupported type
		D_ASSERT(0);
		return sizeof(const char *);
	} // LCOV_EXCL_STOP
}

} // namespace duckdb

void *duckdb_malloc(size_t size) {
	return malloc(size);
}

void duckdb_free(void *ptr) {
	free(ptr);
}

idx_t duckdb_vector_size() {
	return STANDARD_VECTOR_SIZE;
}

bool duckdb_string_is_inlined(duckdb_string_t string_p) {
	static_assert(sizeof(duckdb_string_t) == sizeof(duckdb::string_t),
	              "duckdb_string_t should have the same memory layout as duckdb::string_t");
	auto &string = *(duckdb::string_t *)(&string_p);
	return string.IsInlined();
}







using duckdb::Hugeint;
using duckdb::hugeint_t;
using duckdb::Value;

double duckdb_hugeint_to_double(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return Hugeint::Cast<double>(internal);
}

static duckdb_decimal to_decimal_cast(double val, uint8_t width, uint8_t scale) {
	if (width > duckdb::Decimal::MAX_WIDTH_INT64) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<hugeint_t>>(val, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT32) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int64_t>>(val, width, scale);
	}
	if (width > duckdb::Decimal::MAX_WIDTH_INT16) {
		return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int32_t>>(val, width, scale);
	}
	return duckdb::TryCastToDecimalCInternal<double, duckdb::ToCDecimalCastWrapper<int16_t>>(val, width, scale);
}

duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale) {
	if (scale > width || width > duckdb::Decimal::MAX_WIDTH_INT128) {
		return duckdb::FetchDefaultValue::Operation<duckdb_decimal>();
	}
	return to_decimal_cast(val, width, scale);
}

duckdb_hugeint duckdb_double_to_hugeint(double val) {
	hugeint_t internal_result;
	if (!Value::DoubleIsFinite(val) || !Hugeint::TryConvert<double>(val, internal_result)) {
		internal_result.lower = 0;
		internal_result.upper = 0;
	}

	duckdb_hugeint result;
	result.lower = internal_result.lower;
	result.upper = internal_result.upper;
	return result;
}

double duckdb_decimal_to_double(duckdb_decimal val) {
	double result;
	hugeint_t value;
	value.lower = val.value.lower;
	value.upper = val.value.upper;
	duckdb::TryCastFromDecimal::Operation<hugeint_t, double>(value, result, nullptr, val.width, val.scale);
	return result;
}


static bool AssertLogicalTypeId(duckdb_logical_type type, duckdb::LogicalTypeId type_id) {
	if (!type) {
		return false;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != type_id) {
		return false;
	}
	return true;
}

static bool AssertInternalType(duckdb_logical_type type, duckdb::PhysicalType physical_type) {
	if (!type) {
		return false;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.InternalType() != physical_type) {
		return false;
	}
	return true;
}

duckdb_logical_type duckdb_create_logical_type(duckdb_type type) {
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::ConvertCTypeToCPP(type)));
}

duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type) {
	if (!type) {
		return nullptr;
	}
	duckdb::LogicalType *ltype = new duckdb::LogicalType;
	*ltype = duckdb::LogicalType::LIST(*reinterpret_cast<duckdb::LogicalType *>(type));
	return reinterpret_cast<duckdb_logical_type>(ltype);
}

duckdb_logical_type duckdb_create_union_type(duckdb_logical_type member_types_p, const char **member_names,
                                             idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	duckdb::LogicalType *member_types = reinterpret_cast<duckdb::LogicalType *>(member_types_p);
	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	duckdb::child_list_t<duckdb::LogicalType> members;

	for (idx_t i = 0; i < member_count; i++) {
		members.push_back(make_pair(member_names[i], member_types[i]));
	}
	*mtype = duckdb::LogicalType::UNION(members);
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types_p, const char **member_names,
                                              idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	duckdb::LogicalType **member_types = (duckdb::LogicalType **)member_types_p;
	for (idx_t i = 0; i < member_count; i++) {
		if (!member_names[i] || !member_types[i]) {
			return nullptr;
		}
	}

	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	duckdb::child_list_t<duckdb::LogicalType> members;

	for (idx_t i = 0; i < member_count; i++) {
		members.push_back(make_pair(member_names[i], *member_types[i]));
	}
	*mtype = duckdb::LogicalType::STRUCT(members);
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type) {
	if (!key_type || !value_type) {
		return nullptr;
	}
	duckdb::LogicalType *mtype = new duckdb::LogicalType;
	*mtype = duckdb::LogicalType::MAP(*reinterpret_cast<duckdb::LogicalType *>(key_type),
	                                  *reinterpret_cast<duckdb::LogicalType *>(value_type));
	return reinterpret_cast<duckdb_logical_type>(mtype);
}

duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale) {
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::LogicalType::DECIMAL(width, scale)));
}

duckdb_type duckdb_get_type_id(duckdb_logical_type type) {
	if (!type) {
		return DUCKDB_TYPE_INVALID;
	}
	auto ltype = reinterpret_cast<duckdb::LogicalType *>(type);
	return duckdb::ConvertCPPTypeToC(*ltype);
}

void duckdb_destroy_logical_type(duckdb_logical_type *type) {
	if (type && *type) {
		auto ltype = reinterpret_cast<duckdb::LogicalType *>(*type);
		delete ltype;
		*type = nullptr;
	}
}

uint8_t duckdb_decimal_width(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::DecimalType::GetWidth(ltype);
}

uint8_t duckdb_decimal_scale(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::DecimalType::GetScale(ltype);
}

duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::DECIMAL)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	switch (ltype.InternalType()) {
	case duckdb::PhysicalType::INT16:
		return DUCKDB_TYPE_SMALLINT;
	case duckdb::PhysicalType::INT32:
		return DUCKDB_TYPE_INTEGER;
	case duckdb::PhysicalType::INT64:
		return DUCKDB_TYPE_BIGINT;
	case duckdb::PhysicalType::INT128:
		return DUCKDB_TYPE_HUGEINT;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

duckdb_type duckdb_enum_internal_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	switch (ltype.InternalType()) {
	case duckdb::PhysicalType::UINT8:
		return DUCKDB_TYPE_UTINYINT;
	case duckdb::PhysicalType::UINT16:
		return DUCKDB_TYPE_USMALLINT;
	case duckdb::PhysicalType::UINT32:
		return DUCKDB_TYPE_UINTEGER;
	default:
		return DUCKDB_TYPE_INVALID;
	}
}

uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::EnumType::GetSize(ltype);
}

char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::ENUM)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	auto &vector = duckdb::EnumType::GetValuesInsertOrder(ltype);
	auto value = vector.GetValue(index);
	return strdup(duckdb::StringValue::Get(value).c_str());
}

duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::LIST) &&
	    !AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.id() != duckdb::LogicalTypeId::LIST && ltype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::ListType::GetChildType(ltype)));
}

duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::MapType::KeyType(mtype)));
}

duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (mtype.id() != duckdb::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(duckdb::MapType::ValueType(mtype)));
}

idx_t duckdb_struct_type_child_count(duckdb_logical_type type) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return 0;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return duckdb::StructType::GetChildCount(ltype);
}

idx_t duckdb_union_type_member_count(duckdb_logical_type type) {
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return 0;
	}
	idx_t member_count = duckdb_struct_type_child_count(type);
	if (member_count != 0) {
		member_count--;
	}
	return member_count;
}

char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return strdup(duckdb::UnionType::GetMemberName(ltype, index).c_str());
}

duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, duckdb::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return reinterpret_cast<duckdb_logical_type>(
	    new duckdb::LogicalType(duckdb::UnionType::GetMemberType(ltype, index)));
}

char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	return strdup(duckdb::StructType::GetChildName(ltype, index).c_str());
}

duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index) {
	if (!AssertInternalType(type, duckdb::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &ltype = *(reinterpret_cast<duckdb::LogicalType *>(type));
	if (ltype.InternalType() != duckdb::PhysicalType::STRUCT) {
		return nullptr;
	}
	return reinterpret_cast<duckdb_logical_type>(
	    new duckdb::LogicalType(duckdb::StructType::GetChildType(ltype, index)));
}







using duckdb::case_insensitive_map_t;
using duckdb::make_uniq;
using duckdb::optional_ptr;
using duckdb::PendingExecutionResult;
using duckdb::PendingQueryResult;
using duckdb::PendingStatementWrapper;
using duckdb::PreparedStatementWrapper;
using duckdb::Value;

duckdb_state duckdb_pending_prepared_internal(duckdb_prepared_statement prepared_statement,
                                              duckdb_pending_result *out_result, bool allow_streaming) {
	if (!prepared_statement || !out_result) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	auto result = new PendingStatementWrapper();
	result->allow_streaming = allow_streaming;

	try {
		result->statement = wrapper->statement->PendingQuery(wrapper->values, allow_streaming);
	} catch (const duckdb::Exception &ex) {
		result->statement = make_uniq<PendingQueryResult>(duckdb::PreservedError(ex));
	} catch (std::exception &ex) {
		result->statement = make_uniq<PendingQueryResult>(duckdb::PreservedError(ex));
	}
	duckdb_state return_value = !result->statement->HasError() ? DuckDBSuccess : DuckDBError;
	*out_result = reinterpret_cast<duckdb_pending_result>(result);

	return return_value;
}

duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement, duckdb_pending_result *out_result) {
	return duckdb_pending_prepared_internal(prepared_statement, out_result, false);
}

duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement,
                                               duckdb_pending_result *out_result) {
	return duckdb_pending_prepared_internal(prepared_statement, out_result, true);
}

void duckdb_destroy_pending(duckdb_pending_result *pending_result) {
	if (!pending_result || !*pending_result) {
		return;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(*pending_result);
	if (wrapper->statement) {
		wrapper->statement->Close();
	}
	delete wrapper;
	*pending_result = nullptr;
}

const char *duckdb_pending_error(duckdb_pending_result pending_result) {
	if (!pending_result) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return nullptr;
	}
	return wrapper->statement->GetError().c_str();
}

duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result) {
	if (!pending_result) {
		return DUCKDB_PENDING_ERROR;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return DUCKDB_PENDING_ERROR;
	}
	if (wrapper->statement->HasError()) {
		return DUCKDB_PENDING_ERROR;
	}
	PendingExecutionResult return_value;
	try {
		return_value = wrapper->statement->ExecuteTask();
	} catch (const duckdb::Exception &ex) {
		wrapper->statement->SetError(duckdb::PreservedError(ex));
		return DUCKDB_PENDING_ERROR;
	} catch (std::exception &ex) {
		wrapper->statement->SetError(duckdb::PreservedError(ex));
		return DUCKDB_PENDING_ERROR;
	}
	switch (return_value) {
	case PendingExecutionResult::RESULT_READY:
		return DUCKDB_PENDING_RESULT_READY;
	case PendingExecutionResult::NO_TASKS_AVAILABLE:
		return DUCKDB_PENDING_NO_TASKS_AVAILABLE;
	case PendingExecutionResult::RESULT_NOT_READY:
		return DUCKDB_PENDING_RESULT_NOT_READY;
	default:
		return DUCKDB_PENDING_ERROR;
	}
}

bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state) {
	switch (pending_state) {
	case DUCKDB_PENDING_RESULT_READY:
		return PendingQueryResult::IsFinished(PendingExecutionResult::RESULT_READY);
	case DUCKDB_PENDING_NO_TASKS_AVAILABLE:
		return PendingQueryResult::IsFinished(PendingExecutionResult::NO_TASKS_AVAILABLE);
	case DUCKDB_PENDING_RESULT_NOT_READY:
		return PendingQueryResult::IsFinished(PendingExecutionResult::RESULT_NOT_READY);
	case DUCKDB_PENDING_ERROR:
		return PendingQueryResult::IsFinished(PendingExecutionResult::EXECUTION_ERROR);
	default:
		return PendingQueryResult::IsFinished(PendingExecutionResult::EXECUTION_ERROR);
	}
}

duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result) {
	if (!pending_result || !out_result) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<PendingStatementWrapper *>(pending_result);
	if (!wrapper->statement) {
		return DuckDBError;
	}

	duckdb::unique_ptr<duckdb::QueryResult> result;
	result = wrapper->statement->Execute();
	wrapper->statement.reset();
	return duckdb_translate_result(std::move(result), out_result);
}







using duckdb::case_insensitive_map_t;
using duckdb::Connection;
using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::ExtractStatementsWrapper;
using duckdb::hugeint_t;
using duckdb::LogicalType;
using duckdb::MaterializedQueryResult;
using duckdb::optional_ptr;
using duckdb::PreparedStatementWrapper;
using duckdb::QueryResultType;
using duckdb::StringUtil;
using duckdb::timestamp_t;
using duckdb::Value;

idx_t duckdb_extract_statements(duckdb_connection connection, const char *query,
                                duckdb_extracted_statements *out_extracted_statements) {
	if (!connection || !query || !out_extracted_statements) {
		return 0;
	}
	auto wrapper = new ExtractStatementsWrapper();
	Connection *conn = reinterpret_cast<Connection *>(connection);
	try {
		wrapper->statements = conn->ExtractStatements(query);
	} catch (const duckdb::ParserException &e) {
		wrapper->error = e.what();
	}

	*out_extracted_statements = (duckdb_extracted_statements)wrapper;
	return wrapper->statements.size();
}

duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection,
                                                duckdb_extracted_statements extracted_statements, idx_t index,
                                                duckdb_prepared_statement *out_prepared_statement) {
	Connection *conn = reinterpret_cast<Connection *>(connection);
	auto source_wrapper = (ExtractStatementsWrapper *)extracted_statements;

	if (!connection || !out_prepared_statement || index >= source_wrapper->statements.size()) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	wrapper->statement = conn->Prepare(std::move(source_wrapper->statements[index]));

	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return wrapper->statement->HasError() ? DuckDBError : DuckDBSuccess;
}

const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements) {
	auto wrapper = (ExtractStatementsWrapper *)extracted_statements;
	if (!wrapper || wrapper->error.empty()) {
		return nullptr;
	}
	return wrapper->error.c_str();
}

duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                            duckdb_prepared_statement *out_prepared_statement) {
	if (!connection || !query || !out_prepared_statement) {
		return DuckDBError;
	}
	auto wrapper = new PreparedStatementWrapper();
	Connection *conn = reinterpret_cast<Connection *>(connection);
	wrapper->statement = conn->Prepare(query);
	*out_prepared_statement = (duckdb_prepared_statement)wrapper;
	return !wrapper->statement->HasError() ? DuckDBSuccess : DuckDBError;
}

const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || !wrapper->statement->HasError()) {
		return nullptr;
	}
	return wrapper->statement->error.Message().c_str();
}

idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return 0;
	}
	return wrapper->statement->n_param;
}

static duckdb::string duckdb_parameter_name_internal(duckdb_prepared_statement prepared_statement, idx_t index) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return duckdb::string();
	}
	if (index > wrapper->statement->n_param) {
		return duckdb::string();
	}
	for (auto &item : wrapper->statement->named_param_map) {
		auto &identifier = item.first;
		auto &param_idx = item.second;
		if (param_idx == index) {
			// Found the matching parameter
			return identifier;
		}
	}
	// No parameter was found with this index
	return duckdb::string();
}

const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index) {
	auto identifier = duckdb_parameter_name_internal(prepared_statement, index);
	if (identifier == duckdb::string()) {
		return NULL;
	}
	return strdup(identifier.c_str());
}

duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DUCKDB_TYPE_INVALID;
	}
	LogicalType param_type;
	auto identifier = std::to_string(param_idx);
	if (wrapper->statement->data->TryGetType(identifier, param_type)) {
		return ConvertCPPTypeToC(param_type);
	}
	// The value_map is gone after executing the prepared statement
	// See if this is the case and we still have a value registered for it
	auto it = wrapper->values.find(identifier);
	if (it != wrapper->values.end()) {
		return ConvertCPPTypeToC(it->second.type());
	}
	return DUCKDB_TYPE_INVALID;
}

duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	wrapper->values.clear();
	return DuckDBSuccess;
}

duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_value val) {
	auto value = reinterpret_cast<Value *>(val);
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	if (param_idx <= 0 || param_idx > wrapper->statement->n_param) {
		wrapper->statement->error =
		    duckdb::InvalidInputException("Can not bind to parameter number %d, statement only has %d parameter(s)",
		                                  param_idx, wrapper->statement->n_param);
		return DuckDBError;
	}
	auto identifier = duckdb_parameter_name_internal(prepared_statement, param_idx);
	wrapper->values[identifier] = *value;
	return DuckDBSuccess;
}

duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out,
                                         const char *name_p) {
	auto wrapper = (PreparedStatementWrapper *)prepared_statement;
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}
	if (!name_p || !param_idx_out) {
		return DuckDBError;
	}
	auto name = std::string(name_p);
	for (auto &pair : wrapper->statement->named_param_map) {
		if (duckdb::StringUtil::CIEquals(pair.first, name)) {
			*param_idx_out = pair.second;
			return DuckDBSuccess;
		}
	}
	return DuckDBError;
}

duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val) {
	auto value = Value::BOOLEAN(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val) {
	auto value = Value::TINYINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val) {
	auto value = Value::SMALLINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val) {
	auto value = Value::INTEGER(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val) {
	auto value = Value::BIGINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

static hugeint_t duckdb_internal_hugeint(duckdb_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_hugeint val) {
	auto value = Value::HUGEINT(duckdb_internal_hugeint(val));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val) {
	auto value = Value::UTINYINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val) {
	auto value = Value::USMALLINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val) {
	auto value = Value::UINTEGER(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val) {
	auto value = Value::UBIGINT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val) {
	auto value = Value::FLOAT(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val) {
	auto value = Value::DOUBLE(val);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_date val) {
	auto value = Value::DATE(date_t(val.days));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_time val) {
	auto value = Value::TIME(dtime_t(val.micros));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                   duckdb_timestamp val) {
	auto value = Value::TIMESTAMP(timestamp_t(val.micros));
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_interval val) {
	auto value = Value::INTERVAL(val.months, val.days, val.micros);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val) {
	try {
		auto value = Value(val);
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		auto value = Value(std::string(val, length));
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	} catch (...) {
		return DuckDBError;
	}
}

duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_decimal val) {
	auto hugeint_val = duckdb_internal_hugeint(val.value);
	if (val.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		auto value = Value::DECIMAL(hugeint_val, val.width, val.scale);
		return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
	}
	auto value = hugeint_val.lower;
	auto duck_val = Value::DECIMAL((int64_t)value, val.width, val.scale);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&duck_val);
}

duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, const void *data,
                              idx_t length) {
	auto value = Value::BLOB(duckdb::const_data_ptr_cast(data), length);
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx) {
	auto value = Value();
	return duckdb_bind_value(prepared_statement, param_idx, (duckdb_value)&value);
}

duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return DuckDBError;
	}

	auto result = wrapper->statement->Execute(wrapper->values, false);
	return duckdb_translate_result(std::move(result), out_result);
}

template <class T>
void duckdb_destroy(void **wrapper) {
	if (!wrapper) {
		return;
	}

	auto casted = (T *)*wrapper;
	if (casted) {
		delete casted;
	}
	*wrapper = nullptr;
}

void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements) {
	duckdb_destroy<ExtractStatementsWrapper>(reinterpret_cast<void **>(extracted_statements));
}

void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement) {
	duckdb_destroy<PreparedStatementWrapper>(reinterpret_cast<void **>(prepared_statement));
}






namespace duckdb {

struct CAPIReplacementScanData : public ReplacementScanData {
	~CAPIReplacementScanData() {
		if (delete_callback) {
			delete_callback(extra_data);
		}
	}

	duckdb_replacement_callback_t callback;
	void *extra_data;
	duckdb_delete_callback_t delete_callback;
};

struct CAPIReplacementScanInfo {
	CAPIReplacementScanInfo(CAPIReplacementScanData *data) : data(data) {
	}

	CAPIReplacementScanData *data;
	string function_name;
	vector<Value> parameters;
	string error;
};

unique_ptr<TableRef> duckdb_capi_replacement_callback(ClientContext &context, const string &table_name,
                                                      ReplacementScanData *data) {
	auto &scan_data = reinterpret_cast<CAPIReplacementScanData &>(*data);

	CAPIReplacementScanInfo info(&scan_data);
	scan_data.callback((duckdb_replacement_scan_info)&info, table_name.c_str(), scan_data.extra_data);
	if (!info.error.empty()) {
		throw BinderException("Error in replacement scan: %s\n", info.error);
	}
	if (info.function_name.empty()) {
		// no function provided: bail-out
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	for (auto &param : info.parameters) {
		children.push_back(make_uniq<ConstantExpression>(std::move(param)));
	}
	table_function->function = make_uniq<FunctionExpression>(info.function_name, std::move(children));
	return std::move(table_function);
}

} // namespace duckdb

void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement, void *extra_data,
                                 duckdb_delete_callback_t delete_callback) {
	if (!db || !replacement) {
		return;
	}
	auto wrapper = reinterpret_cast<duckdb::DatabaseData *>(db);
	auto scan_info = duckdb::make_uniq<duckdb::CAPIReplacementScanData>();
	scan_info->callback = replacement;
	scan_info->extra_data = extra_data;
	scan_info->delete_callback = delete_callback;

	auto &config = duckdb::DBConfig::GetConfig(*wrapper->database->instance);
	config.replacement_scans.push_back(
	    duckdb::ReplacementScan(duckdb::duckdb_capi_replacement_callback, std::move(scan_info)));
}

void duckdb_replacement_scan_set_function_name(duckdb_replacement_scan_info info_p, const char *function_name) {
	if (!info_p || !function_name) {
		return;
	}
	auto info = reinterpret_cast<duckdb::CAPIReplacementScanInfo *>(info_p);
	info->function_name = function_name;
}

void duckdb_replacement_scan_add_parameter(duckdb_replacement_scan_info info_p, duckdb_value parameter) {
	if (!info_p || !parameter) {
		return;
	}
	auto info = reinterpret_cast<duckdb::CAPIReplacementScanInfo *>(info_p);
	auto val = reinterpret_cast<duckdb::Value *>(parameter);
	info->parameters.push_back(*val);
}

void duckdb_replacement_scan_set_error(duckdb_replacement_scan_info info_p, const char *error) {
	if (!info_p || !error) {
		return;
	}
	auto info = reinterpret_cast<duckdb::CAPIReplacementScanInfo *>(info_p);
	info->error = error;
}




namespace duckdb {

struct CBaseConverter {
	template <class DST>
	static void NullConvert(DST &target) {
	}
};
struct CStandardConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		return input;
	}
};

struct CStringConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		auto result = char_ptr_cast(duckdb_malloc(input.GetSize() + 1));
		assert(result);
		memcpy((void *)result, input.GetData(), input.GetSize());
		auto write_arr = char_ptr_cast(result);
		write_arr[input.GetSize()] = '\0';
		return result;
	}

	template <class DST>
	static void NullConvert(DST &target) {
		target = nullptr;
	}
};

struct CBlobConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		duckdb_blob result;
		result.data = char_ptr_cast(duckdb_malloc(input.GetSize()));
		result.size = input.GetSize();
		assert(result.data);
		memcpy(result.data, input.GetData(), input.GetSize());
		return result;
	}

	template <class DST>
	static void NullConvert(DST &target) {
		target.data = nullptr;
		target.size = 0;
	}
};

struct CTimestampMsConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		return Timestamp::FromEpochMs(input.value);
	}
};

struct CTimestampNsConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		return Timestamp::FromEpochNanoSeconds(input.value);
	}
};

struct CTimestampSecConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		return Timestamp::FromEpochSeconds(input.value);
	}
};

struct CHugeintConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		duckdb_hugeint result;
		result.lower = input.lower;
		result.upper = input.upper;
		return result;
	}
};

struct CIntervalConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		duckdb_interval result;
		result.days = input.days;
		result.months = input.months;
		result.micros = input.micros;
		return result;
	}
};

template <class T>
struct CDecimalConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		duckdb_hugeint result;
		result.lower = input;
		result.upper = 0;
		return result;
	}
};

template <class SRC, class DST = SRC, class OP = CStandardConverter>
void WriteData(duckdb_column *column, ColumnDataCollection &source, const vector<column_t> &column_ids) {
	idx_t row = 0;
	auto target = (DST *)column->__deprecated_data;
	for (auto &input : source.Chunks(column_ids)) {
		auto source = FlatVector::GetData<SRC>(input.data[0]);
		auto &mask = FlatVector::Validity(input.data[0]);

		for (idx_t k = 0; k < input.size(); k++, row++) {
			if (!mask.RowIsValid(k)) {
				OP::template NullConvert<DST>(target[row]);
			} else {
				target[row] = OP::template Convert<SRC, DST>(source[k]);
			}
		}
	}
}

duckdb_state deprecated_duckdb_translate_column(MaterializedQueryResult &result, duckdb_column *column, idx_t col) {
	D_ASSERT(!result.HasError());
	auto &collection = result.Collection();
	idx_t row_count = collection.Count();
	column->__deprecated_nullmask = (bool *)duckdb_malloc(sizeof(bool) * collection.Count());
	column->__deprecated_data = duckdb_malloc(GetCTypeSize(column->__deprecated_type) * row_count);
	if (!column->__deprecated_nullmask || !column->__deprecated_data) { // LCOV_EXCL_START
		// malloc failure
		return DuckDBError;
	} // LCOV_EXCL_STOP

	vector<column_t> column_ids {col};
	// first convert the nullmask
	{
		idx_t row = 0;
		for (auto &input : collection.Chunks(column_ids)) {
			for (idx_t k = 0; k < input.size(); k++) {
				column->__deprecated_nullmask[row++] = FlatVector::IsNull(input.data[0], k);
			}
		}
	}
	// then write the data
	switch (result.types[col].id()) {
	case LogicalTypeId::BOOLEAN:
		WriteData<bool>(column, collection, column_ids);
		break;
	case LogicalTypeId::TINYINT:
		WriteData<int8_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::SMALLINT:
		WriteData<int16_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::INTEGER:
		WriteData<int32_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::BIGINT:
		WriteData<int64_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UTINYINT:
		WriteData<uint8_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::USMALLINT:
		WriteData<uint16_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UINTEGER:
		WriteData<uint32_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UBIGINT:
		WriteData<uint64_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::FLOAT:
		WriteData<float>(column, collection, column_ids);
		break;
	case LogicalTypeId::DOUBLE:
		WriteData<double>(column, collection, column_ids);
		break;
	case LogicalTypeId::DATE:
		WriteData<date_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIME:
		WriteData<dtime_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIME_TZ:
		WriteData<dtime_tz_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		WriteData<timestamp_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::VARCHAR: {
		WriteData<string_t, const char *, CStringConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::BLOB: {
		WriteData<string_t, duckdb_blob, CBlobConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		WriteData<timestamp_t, timestamp_t, CTimestampNsConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		WriteData<timestamp_t, timestamp_t, CTimestampMsConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_SEC: {
		WriteData<timestamp_t, timestamp_t, CTimestampSecConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::HUGEINT: {
		WriteData<hugeint_t, duckdb_hugeint, CHugeintConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::INTERVAL: {
		WriteData<interval_t, duckdb_interval, CIntervalConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::DECIMAL: {
		// get data
		switch (result.types[col].InternalType()) {
		case PhysicalType::INT16: {
			WriteData<int16_t, duckdb_hugeint, CDecimalConverter<int16_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT32: {
			WriteData<int32_t, duckdb_hugeint, CDecimalConverter<int32_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT64: {
			WriteData<int64_t, duckdb_hugeint, CDecimalConverter<int64_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT128: {
			WriteData<hugeint_t, duckdb_hugeint, CHugeintConverter>(column, collection, column_ids);
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" +
			                         TypeIdToString(result.types[col].InternalType()));
		}
		break;
	}
	default: // LCOV_EXCL_START
		return DuckDBError;
	} // LCOV_EXCL_STOP
	return DuckDBSuccess;
}

duckdb_state duckdb_translate_result(unique_ptr<QueryResult> result_p, duckdb_result *out) {
	auto &result = *result_p;
	D_ASSERT(result_p);
	if (!out) {
		// no result to write to, only return the status
		return !result.HasError() ? DuckDBSuccess : DuckDBError;
	}

	memset(out, 0, sizeof(duckdb_result));

	// initialize the result_data object
	auto result_data = new DuckDBResultData();
	result_data->result = std::move(result_p);
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_NONE;
	out->internal_data = result_data;

	if (result.HasError()) {
		// write the error message
		out->__deprecated_error_message = (char *)result.GetError().c_str(); // NOLINT
		return DuckDBError;
	}
	// copy the data
	// first write the meta data
	out->__deprecated_column_count = result.ColumnCount();
	out->__deprecated_rows_changed = 0;
	return DuckDBSuccess;
}

bool deprecated_materialize_result(duckdb_result *result) {
	if (!result) {
		return false;
	}
	auto result_data = reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data);
	if (result_data->result->HasError()) {
		return false;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		// already materialized into deprecated result format
		return true;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED) {
		// already used as a new result set
		return false;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING) {
		// already used as a streaming result
		return false;
	}
	// materialize as deprecated result set
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED;
	auto column_count = result_data->result->ColumnCount();
	result->__deprecated_columns = (duckdb_column *)duckdb_malloc(sizeof(duckdb_column) * column_count);
	if (!result->__deprecated_columns) { // LCOV_EXCL_START
		// malloc failure
		return DuckDBError;
	} // LCOV_EXCL_STOP
	if (result_data->result->type == QueryResultType::STREAM_RESULT) {
		// if we are dealing with a stream result, convert it to a materialized result first
		auto &stream_result = (StreamQueryResult &)*result_data->result;
		result_data->result = stream_result.Materialize();
	}
	D_ASSERT(result_data->result->type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = reinterpret_cast<MaterializedQueryResult &>(*result_data->result);

	// convert the result to a materialized result
	// zero initialize the columns (so we can cleanly delete it in case a malloc fails)
	memset(result->__deprecated_columns, 0, sizeof(duckdb_column) * column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result->__deprecated_columns[i].__deprecated_type = ConvertCPPTypeToC(result_data->result->types[i]);
		result->__deprecated_columns[i].__deprecated_name = (char *)result_data->result->names[i].c_str(); // NOLINT
	}
	result->__deprecated_row_count = materialized.RowCount();
	if (result->__deprecated_row_count > 0 &&
	    materialized.properties.return_type == StatementReturnType::CHANGED_ROWS) {
		// update total changes
		auto row_changes = materialized.GetValue(0, 0);
		if (!row_changes.IsNull() && row_changes.DefaultTryCastAs(LogicalType::BIGINT)) {
			result->__deprecated_rows_changed = row_changes.GetValue<int64_t>();
		}
	}
	// now write the data
	for (idx_t col = 0; col < column_count; col++) {
		auto state = deprecated_duckdb_translate_column(materialized, &result->__deprecated_columns[col], col);
		if (state != DuckDBSuccess) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb

static void DuckdbDestroyColumn(duckdb_column column, idx_t count) {
	if (column.__deprecated_data) {
		if (column.__deprecated_type == DUCKDB_TYPE_VARCHAR) {
			// varchar, delete individual strings
			auto data = reinterpret_cast<char **>(column.__deprecated_data);
			for (idx_t i = 0; i < count; i++) {
				if (data[i]) {
					duckdb_free(data[i]);
				}
			}
		} else if (column.__deprecated_type == DUCKDB_TYPE_BLOB) {
			// blob, delete individual blobs
			auto data = reinterpret_cast<duckdb_blob *>(column.__deprecated_data);
			for (idx_t i = 0; i < count; i++) {
				if (data[i].data) {
					duckdb_free((void *)data[i].data);
				}
			}
		}
		duckdb_free(column.__deprecated_data);
	}
	if (column.__deprecated_nullmask) {
		duckdb_free(column.__deprecated_nullmask);
	}
}

void duckdb_destroy_result(duckdb_result *result) {
	if (result->__deprecated_columns) {
		for (idx_t i = 0; i < result->__deprecated_column_count; i++) {
			DuckdbDestroyColumn(result->__deprecated_columns[i], result->__deprecated_row_count);
		}
		duckdb_free(result->__deprecated_columns);
	}
	if (result->internal_data) {
		auto result_data = reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data);
		delete result_data;
	}
	memset(result, 0, sizeof(duckdb_result));
}

const char *duckdb_column_name(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	return result_data.result->names[col].c_str();
}

duckdb_type duckdb_column_type(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return DUCKDB_TYPE_INVALID;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	return duckdb::ConvertCPPTypeToC(result_data.result->types[col]);
}

duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col) {
	if (!result || col >= duckdb_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	return reinterpret_cast<duckdb_logical_type>(new duckdb::LogicalType(result_data.result->types[col]));
}

idx_t duckdb_column_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	return result_data.result->ColumnCount();
}

idx_t duckdb_row_count(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	if (result_data.result->type == duckdb::QueryResultType::STREAM_RESULT) {
		// We can't know the row count beforehand
		return 0;
	}
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	return materialized.RowCount();
}

idx_t duckdb_rows_changed(duckdb_result *result) {
	if (!result) {
		return 0;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return 0;
	}
	return result->__deprecated_rows_changed;
}

void *duckdb_column_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_data;
}

bool *duckdb_nullmask_data(duckdb_result *result, idx_t col) {
	if (!result || col >= result->__deprecated_column_count) {
		return nullptr;
	}
	if (!duckdb::deprecated_materialize_result(result)) {
		return nullptr;
	}
	return result->__deprecated_columns[col].__deprecated_nullmask;
}

const char *duckdb_result_error(duckdb_result *result) {
	if (!result) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result->internal_data));
	return !result_data.result->HasError() ? nullptr : result_data.result->GetError().c_str();
}

idx_t duckdb_result_chunk_count(duckdb_result result) {
	if (!result.internal_data) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return 0;
	}
	if (result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// Can't know beforehand how many chunks are returned.
		return 0;
	}
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	return materialized.Collection().ChunkCount();
}

duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_idx) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	if (result_data.result->type != duckdb::QueryResultType::MATERIALIZED_RESULT) {
		// This API is only supported for materialized query results
		return nullptr;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	auto &collection = materialized.Collection();
	if (chunk_idx >= collection.ChunkCount()) {
		return nullptr;
	}
	auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
	chunk->Initialize(duckdb::Allocator::DefaultAllocator(), collection.Types());
	collection.FetchChunk(chunk_idx, *chunk);
	return reinterpret_cast<duckdb_data_chunk>(chunk.release());
}

bool duckdb_result_is_streaming(duckdb_result result) {
	if (!result.internal_data) {
		return false;
	}
	if (duckdb_result_error(&result) != nullptr) {
		return false;
	}
	auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	return result_data.result->type == duckdb::QueryResultType::STREAM_RESULT;
}




duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *((duckdb::DuckDBResultData *)result.internal_data);
	if (result_data.result_set_type == duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	if (result_data.result->type != duckdb::QueryResultType::STREAM_RESULT) {
		// We can only fetch from a StreamQueryResult
		return nullptr;
	}
	result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING;
	auto &streaming = (duckdb::StreamQueryResult &)*result_data.result;
	if (!streaming.IsOpen()) {
		return nullptr;
	}
	// FetchRaw ? Do we care about flattening them?
	auto chunk = streaming.Fetch();
	return reinterpret_cast<duckdb_data_chunk>(chunk.release());
}







namespace duckdb {

struct CTableFunctionInfo : public TableFunctionInfo {
	~CTableFunctionInfo() {
		if (extra_info && delete_callback) {
			delete_callback(extra_info);
		}
		extra_info = nullptr;
		delete_callback = nullptr;
	}

	duckdb_table_function_bind_t bind = nullptr;
	duckdb_table_function_init_t init = nullptr;
	duckdb_table_function_init_t local_init = nullptr;
	duckdb_table_function_t function = nullptr;
	void *extra_info = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
};

struct CTableBindData : public TableFunctionData {
	CTableBindData(CTableFunctionInfo &info) : info(info) {
	}
	~CTableBindData() {
		if (bind_data && delete_callback) {
			delete_callback(bind_data);
		}
		bind_data = nullptr;
		delete_callback = nullptr;
	}

	CTableFunctionInfo &info;
	void *bind_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
	unique_ptr<NodeStatistics> stats;
};

struct CTableInternalBindInfo {
	CTableInternalBindInfo(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
	                       vector<string> &names, CTableBindData &bind_data, CTableFunctionInfo &function_info)
	    : context(context), input(input), return_types(return_types), names(names), bind_data(bind_data),
	      function_info(function_info), success(true) {
	}

	ClientContext &context;
	TableFunctionBindInput &input;
	vector<LogicalType> &return_types;
	vector<string> &names;
	CTableBindData &bind_data;
	CTableFunctionInfo &function_info;
	bool success;
	string error;
};

struct CTableInitData {
	~CTableInitData() {
		if (init_data && delete_callback) {
			delete_callback(init_data);
		}
		init_data = nullptr;
		delete_callback = nullptr;
	}

	void *init_data = nullptr;
	duckdb_delete_callback_t delete_callback = nullptr;
	idx_t max_threads = 1;
};

struct CTableGlobalInitData : public GlobalTableFunctionState {
	CTableInitData init_data;

	idx_t MaxThreads() const override {
		return init_data.max_threads;
	}
};

struct CTableLocalInitData : public LocalTableFunctionState {
	CTableInitData init_data;
};

struct CTableInternalInitInfo {
	CTableInternalInitInfo(const CTableBindData &bind_data, CTableInitData &init_data,
	                       const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters)
	    : bind_data(bind_data), init_data(init_data), column_ids(column_ids), filters(filters), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	const vector<column_t> &column_ids;
	optional_ptr<TableFilterSet> filters;
	bool success;
	string error;
};

struct CTableInternalFunctionInfo {
	CTableInternalFunctionInfo(const CTableBindData &bind_data, CTableInitData &init_data, CTableInitData &local_data)
	    : bind_data(bind_data), init_data(init_data), local_data(local_data), success(true) {
	}

	const CTableBindData &bind_data;
	CTableInitData &init_data;
	CTableInitData &local_data;
	bool success;
	string error;
};

unique_ptr<FunctionData> CTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &info = input.info->Cast<CTableFunctionInfo>();
	D_ASSERT(info.bind && info.function && info.init);
	auto result = make_uniq<CTableBindData>(info);
	CTableInternalBindInfo bind_info(context, input, return_types, names, *result, info);
	info.bind(&bind_info);
	if (!bind_info.success) {
		throw Exception(bind_info.error);
	}

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> CTableFunctionInit(ClientContext &context, TableFunctionInitInput &data_p) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableGlobalInitData>();

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.init(&init_info);
	if (!init_info.success) {
		throw Exception(init_info.error);
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> CTableFunctionLocalInit(ExecutionContext &context, TableFunctionInitInput &data_p,
                                                            GlobalTableFunctionState *gstate) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto result = make_uniq<CTableLocalInitData>();
	if (!bind_data.info.local_init) {
		return std::move(result);
	}

	CTableInternalInitInfo init_info(bind_data, result->init_data, data_p.column_ids, data_p.filters);
	bind_data.info.local_init(&init_info);
	if (!init_info.success) {
		throw Exception(init_info.error);
	}
	return std::move(result);
}

unique_ptr<NodeStatistics> CTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CTableBindData>();
	if (!bind_data.stats) {
		return nullptr;
	}
	return make_uniq<NodeStatistics>(*bind_data.stats);
}

void CTableFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CTableBindData>();
	auto &global_data = (CTableGlobalInitData &)*data_p.global_state;
	auto &local_data = (CTableLocalInitData &)*data_p.local_state;
	CTableInternalFunctionInfo function_info(bind_data, global_data.init_data, local_data.init_data);
	bind_data.info.function(&function_info, reinterpret_cast<duckdb_data_chunk>(&output));
	if (!function_info.success) {
		throw Exception(function_info.error);
	}
}

} // namespace duckdb

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
duckdb_table_function duckdb_create_table_function() {
	auto function = new duckdb::TableFunction("", {}, duckdb::CTableFunction, duckdb::CTableFunctionBind,
	                                          duckdb::CTableFunctionInit, duckdb::CTableFunctionLocalInit);
	function->function_info = duckdb::make_shared<duckdb::CTableFunctionInfo>();
	function->cardinality = duckdb::CTableFunctionCardinality;
	return function;
}

void duckdb_destroy_table_function(duckdb_table_function *function) {
	if (function && *function) {
		auto tf = (duckdb::TableFunction *)*function;
		delete tf;
		*function = nullptr;
	}
}

void duckdb_table_function_set_name(duckdb_table_function function, const char *name) {
	if (!function || !name) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	tf->name = name;
}

void duckdb_table_function_add_parameter(duckdb_table_function function, duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto logical_type = (duckdb::LogicalType *)type;
	tf->arguments.push_back(*logical_type);
}

void duckdb_table_function_add_named_parameter(duckdb_table_function function, const char *name,
                                               duckdb_logical_type type) {
	if (!function || !type) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto logical_type = (duckdb::LogicalType *)type;
	tf->named_parameters.insert({name, *logical_type});
}

void duckdb_table_function_set_extra_info(duckdb_table_function function, void *extra_info,
                                          duckdb_delete_callback_t destroy) {
	if (!function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->extra_info = extra_info;
	info->delete_callback = destroy;
}

void duckdb_table_function_set_bind(duckdb_table_function function, duckdb_table_function_bind_t bind) {
	if (!function || !bind) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->bind = bind;
}

void duckdb_table_function_set_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->init = init;
}

void duckdb_table_function_set_local_init(duckdb_table_function function, duckdb_table_function_init_t init) {
	if (!function || !init) {
		return;
	}
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->local_init = init;
}

void duckdb_table_function_set_function(duckdb_table_function table_function, duckdb_table_function_t function) {
	if (!table_function || !function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)table_function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	info->function = function;
}

void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown) {
	if (!table_function) {
		return;
	}
	auto tf = (duckdb::TableFunction *)table_function;
	tf->projection_pushdown = pushdown;
}

duckdb_state duckdb_register_table_function(duckdb_connection connection, duckdb_table_function function) {
	if (!connection || !function) {
		return DuckDBError;
	}
	auto con = (duckdb::Connection *)connection;
	auto tf = (duckdb::TableFunction *)function;
	auto info = (duckdb::CTableFunctionInfo *)tf->function_info.get();
	if (tf->name.empty() || !info->bind || !info->init || !info->function) {
		return DuckDBError;
	}
	con->context->RunFunctionInTransaction([&]() {
		auto &catalog = duckdb::Catalog::GetSystemCatalog(*con->context);
		duckdb::CreateTableFunctionInfo tf_info(*tf);

		// create the function in the catalog
		catalog.CreateTableFunction(*con->context, tf_info);
	});
	return DuckDBSuccess;
}

//===--------------------------------------------------------------------===//
// Bind Interface
//===--------------------------------------------------------------------===//
void *duckdb_bind_get_extra_info(duckdb_bind_info info) {
	if (!info) {
		return nullptr;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return bind_info->function_info.extra_info;
}

void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type) {
	if (!info || !name || !type) {
		return;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	bind_info->names.push_back(name);
	bind_info->return_types.push_back(*(reinterpret_cast<duckdb::LogicalType *>(type)));
}

idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info) {
	if (!info) {
		return 0;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return bind_info->input.inputs.size();
}

duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index) {
	if (!info || index >= duckdb_bind_get_parameter_count(info)) {
		return nullptr;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	return reinterpret_cast<duckdb_value>(new duckdb::Value(bind_info->input.inputs[index]));
}

duckdb_value duckdb_bind_get_named_parameter(duckdb_bind_info info, const char *name) {
	if (!info || !name) {
		return nullptr;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	auto t = bind_info->input.named_parameters.find(name);
	if (t == bind_info->input.named_parameters.end()) {
		return nullptr;
	} else {
		return reinterpret_cast<duckdb_value>(new duckdb::Value(t->second));
	}
}

void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	bind_info->bind_data.bind_data = bind_data;
	bind_info->bind_data.delete_callback = destroy;
}

void duckdb_bind_set_cardinality(duckdb_bind_info info, idx_t cardinality, bool is_exact) {
	if (!info) {
		return;
	}
	auto bind_info = (duckdb::CTableInternalBindInfo *)info;
	if (is_exact) {
		bind_info->bind_data.stats = duckdb::make_uniq<duckdb::NodeStatistics>(cardinality);
	} else {
		bind_info->bind_data.stats = duckdb::make_uniq<duckdb::NodeStatistics>(cardinality, cardinality);
	}
}

void duckdb_bind_set_error(duckdb_bind_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalBindInfo *)info;
	function_info->error = error;
	function_info->success = false;
}

//===--------------------------------------------------------------------===//
// Init Interface
//===--------------------------------------------------------------------===//
void *duckdb_init_get_extra_info(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	return init_info->bind_data.info.extra_info;
}

void *duckdb_init_get_bind_data(duckdb_init_info info) {
	if (!info) {
		return nullptr;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	return init_info->bind_data.bind_data;
}

void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy) {
	if (!info) {
		return;
	}
	auto init_info = (duckdb::CTableInternalInitInfo *)info;
	init_info->init_data.init_data = init_data;
	init_info->init_data.delete_callback = destroy;
}

void duckdb_init_set_error(duckdb_init_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	function_info->error = error;
	function_info->success = false;
}

idx_t duckdb_init_get_column_count(duckdb_init_info info) {
	if (!info) {
		return 0;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	return function_info->column_ids.size();
}

idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index) {
	if (!info) {
		return 0;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	if (column_index >= function_info->column_ids.size()) {
		return 0;
	}
	return function_info->column_ids[column_index];
}

void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads) {
	if (!info) {
		return;
	}
	auto function_info = (duckdb::CTableInternalInitInfo *)info;
	function_info->init_data.max_threads = max_threads;
}

//===--------------------------------------------------------------------===//
// Function Interface
//===--------------------------------------------------------------------===//
void *duckdb_function_get_extra_info(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->bind_data.info.extra_info;
}

void *duckdb_function_get_bind_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->bind_data.bind_data;
}

void *duckdb_function_get_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->init_data.init_data;
}

void *duckdb_function_get_local_init_data(duckdb_function_info info) {
	if (!info) {
		return nullptr;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	return function_info->local_data.init_data;
}

void duckdb_function_set_error(duckdb_function_info info, const char *error) {
	if (!info || !error) {
		return;
	}
	auto function_info = (duckdb::CTableInternalFunctionInfo *)info;
	function_info->error = error;
	function_info->success = false;
}



using duckdb::DatabaseData;

struct CAPITaskState {
	CAPITaskState(duckdb::DatabaseInstance &db)
	    : db(db), marker(duckdb::make_uniq<duckdb::atomic<bool>>(true)), execute_count(0) {
	}

	duckdb::DatabaseInstance &db;
	duckdb::unique_ptr<duckdb::atomic<bool>> marker;
	duckdb::atomic<idx_t> execute_count;
};

void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks) {
	if (!database) {
		return;
	}
	auto wrapper = (DatabaseData *)database;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
	scheduler.ExecuteTasks(max_tasks);
}

duckdb_task_state duckdb_create_task_state(duckdb_database database) {
	if (!database) {
		return nullptr;
	}
	auto wrapper = (DatabaseData *)database;
	auto state = new CAPITaskState(*wrapper->database->instance);
	return state;
}

void duckdb_execute_tasks_state(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
	state->execute_count++;
	scheduler.ExecuteForever(state->marker.get());
}

idx_t duckdb_execute_n_tasks_state(duckdb_task_state state_p, idx_t max_tasks) {
	if (!state_p) {
		return 0;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
	return scheduler.ExecuteTasks(state->marker.get(), max_tasks);
}

void duckdb_finish_execution(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	*state->marker = false;
	if (state->execute_count > 0) {
		// signal to the threads to wake up
		auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
		scheduler.Signal(state->execute_count);
	}
}

bool duckdb_task_state_is_finished(duckdb_task_state state_p) {
	if (!state_p) {
		return false;
	}
	auto state = (CAPITaskState *)state_p;
	return !(*state->marker);
}

void duckdb_destroy_task_state(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	delete state;
}

bool duckdb_execution_is_finished(duckdb_connection con) {
	if (!con) {
		return false;
	}
	duckdb::Connection *conn = (duckdb::Connection *)con;
	return conn->context->ExecutionIsFinished();
}








#include <cstring>

using duckdb::date_t;
using duckdb::dtime_t;
using duckdb::FetchDefaultValue;
using duckdb::GetInternalCValue;
using duckdb::hugeint_t;
using duckdb::interval_t;
using duckdb::StringCast;
using duckdb::timestamp_t;
using duckdb::ToCStringCastWrapper;
using duckdb::UnsafeFetch;

bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<bool>(result, col, row);
}

int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int8_t>(result, col, row);
}

int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int16_t>(result, col, row);
}

int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int32_t>(result, col, row);
}

int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<int64_t>(result, col, row);
}

static bool ResultIsDecimal(duckdb_result *result, idx_t col) {
	if (!result) {
		return false;
	}
	if (!result->internal_data) {
		return false;
	}
	auto result_data = (duckdb::DuckDBResultData *)result->internal_data;
	auto &query_result = result_data->result;
	auto &source_type = query_result->types[col];
	return source_type.id() == duckdb::LogicalTypeId::DECIMAL;
}

duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row) || !ResultIsDecimal(result, col)) {
		return FetchDefaultValue::Operation<duckdb_decimal>();
	}

	return GetInternalCValue<duckdb_decimal>(result, col, row);
}

duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_hugeint result_value;
	auto internal_value = GetInternalCValue<hugeint_t>(result, col, row);
	result_value.lower = internal_value.lower;
	result_value.upper = internal_value.upper;
	return result_value;
}

uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint8_t>(result, col, row);
}

uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint16_t>(result, col, row);
}

uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint32_t>(result, col, row);
}

uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<uint64_t>(result, col, row);
}

float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<float>(result, col, row);
}

double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<double>(result, col, row);
}

duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_date result_value;
	result_value.days = GetInternalCValue<date_t>(result, col, row).days;
	return result_value;
}

duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_time result_value;
	result_value.micros = GetInternalCValue<dtime_t>(result, col, row).micros;
	return result_value;
}

duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_timestamp result_value;
	result_value.micros = GetInternalCValue<timestamp_t>(result, col, row).value;
	return result_value;
}

duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row) {
	duckdb_interval result_value;
	auto ival = GetInternalCValue<interval_t>(result, col, row);
	result_value.months = ival.months;
	result_value.days = ival.days;
	result_value.micros = ival.micros;
	return result_value;
}

char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row) {
	return duckdb_value_string(result, col, row).data;
}

duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row) {
	return GetInternalCValue<duckdb_string, ToCStringCastWrapper<StringCast>>(result, col, row);
}

char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row) {
	return duckdb_value_string_internal(result, col, row).data;
}

duckdb_string duckdb_value_string_internal(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanFetchValue(result, col, row)) {
		return FetchDefaultValue::Operation<duckdb_string>();
	}
	if (duckdb_column_type(result, col) != DUCKDB_TYPE_VARCHAR) {
		return FetchDefaultValue::Operation<duckdb_string>();
	}
	// FIXME: this obviously does not work when there are null bytes in the string
	// we need to remove the deprecated C result materialization to get that to work correctly
	// since the deprecated C result materialization stores strings as null-terminated
	duckdb_string res;
	res.data = UnsafeFetch<char *>(result, col, row);
	res.size = strlen(res.data);
	return res;
}

duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row) {
	if (CanFetchValue(result, col, row) && result->__deprecated_columns[col].__deprecated_type == DUCKDB_TYPE_BLOB) {
		auto internal_result = UnsafeFetch<duckdb_blob>(result, col, row);

		duckdb_blob result_blob;
		result_blob.data = malloc(internal_result.size);
		result_blob.size = internal_result.size;
		memcpy(result_blob.data, internal_result.data, internal_result.size);
		return result_blob;
	}
	return FetchDefaultValue::Operation<duckdb_blob>();
}

bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row) {
	if (!CanUseDeprecatedFetch(result, col, row)) {
		return false;
	}
	return result->__deprecated_columns[col].__deprecated_nullmask[row];
}




namespace duckdb {

QueryResultChunkScanState::QueryResultChunkScanState(QueryResult &result) : ChunkScanState(), result(result) {
}

QueryResultChunkScanState::~QueryResultChunkScanState() {
}

bool QueryResultChunkScanState::InternalLoad(PreservedError &error) {
	D_ASSERT(!finished);
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = result.Cast<StreamQueryResult>();
		if (!stream_result.IsOpen()) {
			return true;
		}
	}
	return result.TryFetch(current_chunk, error);
}

bool QueryResultChunkScanState::HasError() const {
	return result.HasError();
}

PreservedError &QueryResultChunkScanState::GetError() {
	D_ASSERT(result.HasError());
	return result.GetErrorObject();
}

const vector<LogicalType> &QueryResultChunkScanState::Types() const {
	return result.types;
}

const vector<string> &QueryResultChunkScanState::Names() const {
	return result.names;
}

bool QueryResultChunkScanState::LoadNextChunk(PreservedError &error) {
	if (finished) {
		return !finished;
	}
	auto load_result = InternalLoad(error);
	if (!load_result) {
		finished = true;
	}
	offset = 0;
	return !finished;
}

} // namespace duckdb



namespace duckdb {

ChunkScanState::ChunkScanState() {
}

ChunkScanState::~ChunkScanState() {
}

idx_t ChunkScanState::CurrentOffset() const {
	return offset;
}

void ChunkScanState::IncreaseOffset(idx_t increment, bool unsafe) {
	D_ASSERT(unsafe || increment <= RemainingInChunk());
	offset += increment;
}

bool ChunkScanState::ChunkIsEmpty() const {
	return !current_chunk || current_chunk->size() == 0;
}

bool ChunkScanState::Finished() const {
	return finished;
}

bool ChunkScanState::ScanStarted() const {
	return !ChunkIsEmpty();
}

DataChunk &ChunkScanState::CurrentChunk() {
	// Scan must already be started
	D_ASSERT(current_chunk);
	return *current_chunk;
}

idx_t ChunkScanState::RemainingInChunk() const {
	if (ChunkIsEmpty()) {
		return 0;
	}
	D_ASSERT(current_chunk);
	D_ASSERT(offset <= current_chunk->size());
	return current_chunk->size() - offset;
}

} // namespace duckdb












































namespace duckdb {

struct ActiveQueryContext {
	//! The query that is currently being executed
	string query;
	//! The currently open result
	BaseQueryResult *open_result = nullptr;
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
};

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(std::move(database)), interrupted(false), client_data(make_uniq<ClientData>(*this)), transaction(*this) {
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
	// destroy the client context and rollback if there is an active transaction
	// but only if we are not destroying this client context as part of an exception stack unwind
	Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_uniq<ClientContextLock>(context_lock);
}

void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		transaction.ResetActiveQuery();
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
	}
	CleanupInternal(*lock);
}

unique_ptr<DataChunk> ClientContext::Fetch(ClientContextLock &lock, StreamQueryResult &result) {
	D_ASSERT(IsActiveResult(lock, &result));
	D_ASSERT(active_query->executor);
	return FetchInternal(lock, *active_query->executor, result);
}

unique_ptr<DataChunk> ClientContext::FetchInternal(ClientContextLock &lock, Executor &executor,
                                                   BaseQueryResult &result) {
	bool invalidate_query = true;
	try {
		// fetch the chunk and return it
		auto chunk = executor.FetchChunk();
		if (!chunk || chunk->size() == 0) {
			CleanupInternal(lock, &result);
		}
		return chunk;
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result.SetError(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in FetchInternal"));
	} // LCOV_EXCL_STOP
	CleanupInternal(lock, &result, invalidate_query);
	return nullptr;
}

void ClientContext::BeginTransactionInternal(ClientContextLock &lock, bool requires_valid_transaction) {
	// check if we are on AutoCommit. In this case we should start a transaction
	D_ASSERT(!active_query);
	auto &db = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db)) {
		throw FatalException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_DATABASE,
		                                                   ValidChecker::InvalidatedMessage(db)));
	}
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(transaction.ActiveTransaction())) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	active_query = make_uniq<ActiveQueryContext>();
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, const string &query) {
	BeginTransactionInternal(lock, false);
	LogQueryInternal(lock, query);
	active_query->query = query;
	query_progress = -1;
	transaction.SetActiveQuery(db->GetDatabaseManager().GetNewQueryNumber());
}

PreservedError ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction) {
	client_data->profiler->EndQuery();

	if (client_data->http_state) {
		client_data->http_state->Reset();
	}

	// Notify any registered state of query end
	for (auto const &s : registered_state) {
		s.second->QueryEnd();
	}

	D_ASSERT(active_query.get());
	active_query.reset();
	query_progress = -1;
	PreservedError error;
	try {
		if (transaction.HasActiveTransaction()) {
			// Move the query profiler into the history
			auto &prev_profilers = client_data->query_profiler_history->GetPrevProfilers();
			prev_profilers.emplace_back(transaction.GetActiveQuery(), std::move(client_data->profiler));
			// Reinitialize the query profiler
			client_data->profiler = make_shared<QueryProfiler>(*this);
			// Propagate settings of the saved query into the new profiler.
			client_data->profiler->Propagate(*prev_profilers.back().second);
			if (prev_profilers.size() >= client_data->query_profiler_history->GetPrevProfilersSize()) {
				prev_profilers.pop_front();
			}

			transaction.ResetActiveQuery();
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ValidChecker::Invalidate(ActiveTransaction(), "Failed to commit");
			}
		}
	} catch (FatalException &ex) {
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		error = PreservedError(ex);
	} catch (const Exception &ex) {
		error = PreservedError(ex);
	} catch (std::exception &ex) {
		error = PreservedError(ex);
	} catch (...) { // LCOV_EXCL_START
		error = PreservedError("Unhandled exception!");
	} // LCOV_EXCL_STOP
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	client_data->http_state = make_shared<HTTPState>();
	if (!active_query) {
		// no query currently active
		return;
	}
	if (active_query->executor) {
		active_query->executor->CancelTasks();
	}
	active_query->progress_bar.reset();

	auto error = EndQueryInternal(lock, result ? !result->HasError() : false, invalidate_transaction);
	if (result && !result->HasError()) {
		// if an error occurred while committing report it in the result
		result->SetError(error);
	}
	D_ASSERT(!active_query);
}

Executor &ClientContext::GetExecutor() {
	D_ASSERT(active_query);
	D_ASSERT(active_query->executor);
	return *active_query->executor;
}

const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

unique_ptr<QueryResult> ClientContext::FetchResultInternal(ClientContextLock &lock, PendingQueryResult &pending) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &pending);
	D_ASSERT(active_query->prepared);
	auto &executor = GetExecutor();
	auto &prepared = *active_query->prepared;
	bool create_stream_result = prepared.properties.allow_stream_result && pending.allow_stream_result;
	if (create_stream_result) {
		D_ASSERT(!executor.HasResultCollector());
		active_query->progress_bar.reset();
		query_progress = -1;

		// successfully compiled SELECT clause, and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		auto stream_result = make_uniq<StreamQueryResult>(pending.statement_type, pending.properties,
		                                                  shared_from_this(), pending.types, pending.names);
		active_query->open_result = stream_result.get();
		return std::move(stream_result);
	}
	unique_ptr<QueryResult> result;
	if (executor.HasResultCollector()) {
		// we have a result collector - fetch the result directly from the result collector
		result = executor.GetResult();
		CleanupInternal(lock, result.get(), false);
	} else {
		// no result collector - create a materialized result by continuously fetching
		auto result_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), pending.types);
		D_ASSERT(!result_collection->Types().empty());
		auto materialized_result =
		    make_uniq<MaterializedQueryResult>(pending.statement_type, pending.properties, pending.names,
		                                       std::move(result_collection), GetClientProperties());

		auto &collection = materialized_result->Collection();
		D_ASSERT(!collection.Types().empty());
		ColumnDataAppendState append_state;
		collection.InitializeAppend(append_state);
		while (true) {
			auto chunk = FetchInternal(lock, GetExecutor(), *materialized_result);
			if (!chunk || chunk->size() == 0) {
				break;
			}
#ifdef DEBUG
			for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
				if (pending.types[i].id() == LogicalTypeId::VARCHAR) {
					chunk->data[i].UTFVerify(chunk->size());
				}
			}
#endif
			collection.Append(append_state, *chunk);
		}
		result = std::move(materialized_result);
	}
	return result;
}

static bool IsExplainAnalyze(SQLStatement *statement) {
	if (!statement) {
		return false;
	}
	if (statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	auto &explain = statement->Cast<ExplainStatement>();
	return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

shared_ptr<PreparedStatementData>
ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
                                       optional_ptr<case_insensitive_map_t<Value>> values) {
	StatementType statement_type = statement->type;
	auto result = make_shared<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement.get()), true);
	profiler.StartPhase("planner");
	Planner planner(*this);
	if (values) {
		auto &parameter_values = *values;
		for (auto &value : parameter_values) {
			planner.parameter_data.emplace(value.first, BoundParameterData(value.second));
		}
	}

	client_data->http_state = make_shared<HTTPState>();
	planner.CreatePlan(std::move(statement));
	D_ASSERT(planner.plan || !planner.properties.bound_all_parameters);
	profiler.EndPhase();

	auto plan = std::move(planner.plan);
	// extract the result column names from the plan
	result->properties = planner.properties;
	result->names = planner.names;
	result->types = planner.types;
	result->value_map = std::move(planner.value_map);
	result->catalog_version = MetaTransaction::Get(*this).catalog_version;

	if (!planner.properties.bound_all_parameters) {
		return result;
	}
#ifdef DEBUG
	plan->Verify(*this);
#endif
	if (config.enable_optimizer && plan->RequireOptimizer()) {
		profiler.StartPhase("optimizer");
		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(std::move(plan));
		D_ASSERT(plan);
		profiler.EndPhase();

#ifdef DEBUG
		plan->Verify(*this);
#endif
	}

	profiler.StartPhase("physical_planner");
	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(*this);
	auto physical_plan = physical_planner.CreatePlan(std::move(plan));
	profiler.EndPhase();

#ifdef DEBUG
	D_ASSERT(!physical_plan->ToString().empty());
#endif
	result->plan = std::move(physical_plan);
	return result;
}

double ClientContext::GetProgress() {
	return query_progress.load();
}

unique_ptr<PendingQueryResult> ClientContext::PendingPreparedStatement(ClientContextLock &lock,
                                                                       shared_ptr<PreparedStatementData> statement_p,
                                                                       const PendingQueryParameters &parameters) {
	D_ASSERT(active_query);
	auto &statement = *statement_p;
	if (ValidChecker::IsInvalidated(ActiveTransaction()) && statement.properties.requires_valid_transaction) {
		throw Exception(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	auto &transaction = MetaTransaction::Get(*this);
	auto &manager = DatabaseManager::Get(*this);
	for (auto &modified_database : statement.properties.modified_databases) {
		auto entry = manager.GetDatabase(*this, modified_database);
		if (!entry) {
			throw InternalException("Database \"%s\" not found", modified_database);
		}
		if (entry->IsReadOnly()) {
			throw Exception(StringUtil::Format(
			    "Cannot execute statement of type \"%s\" on database \"%s\" which is attached in read-only mode!",
			    StatementTypeToString(statement.statement_type), modified_database));
		}
		transaction.ModifyDatabase(*entry);
	}

	// bind the bound values before execution
	case_insensitive_map_t<Value> owned_values;
	if (parameters.parameters) {
		auto &params = *parameters.parameters;
		for (auto &val : params) {
			owned_values.emplace(val);
		}
	}
	statement.Bind(std::move(owned_values));

	active_query->executor = make_uniq<Executor>(*this);
	auto &executor = *active_query->executor;
	if (config.enable_progress_bar) {
		progress_bar_display_create_func_t display_create_func = nullptr;
		if (config.print_progress_bar) {
			// If a custom display is set, use that, otherwise just use the default
			display_create_func =
			    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		}
		active_query->progress_bar = make_uniq<ProgressBar>(executor, config.wait_time, display_create_func);
		active_query->progress_bar->Start();
		query_progress = 0;
	}
	auto stream_result = parameters.allow_stream_result && statement.properties.allow_stream_result;
	if (!stream_result && statement.properties.return_type == StatementReturnType::QUERY_RESULT) {
		unique_ptr<PhysicalResultCollector> collector;
		auto &config = ClientConfig::GetConfig(*this);
		auto get_method =
		    config.result_collector ? config.result_collector : PhysicalResultCollector::GetResultCollector;
		collector = get_method(*this, statement);
		D_ASSERT(collector->type == PhysicalOperatorType::RESULT_COLLECTOR);
		executor.Initialize(std::move(collector));
	} else {
		executor.Initialize(*statement.plan);
	}
	auto types = executor.GetTypes();
	D_ASSERT(types == statement.types);
	D_ASSERT(!active_query->open_result);

	auto pending_result =
	    make_uniq<PendingQueryResult>(shared_from_this(), *statement_p, std::move(types), stream_result);
	active_query->prepared = std::move(statement_p);
	active_query->open_result = pending_result.get();
	return pending_result;
}

PendingExecutionResult ClientContext::ExecuteTaskInternal(ClientContextLock &lock, PendingQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->open_result == &result);
	try {
		auto result = active_query->executor->ExecuteTask();
		if (active_query->progress_bar) {
			active_query->progress_bar->Update(result == PendingExecutionResult::RESULT_READY);
			query_progress = active_query->progress_bar->GetCurrentPercentage();
		}
		return result;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		result.SetError(PreservedError(ex));
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
	} catch (const Exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (std::exception &ex) {
		result.SetError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		result.SetError(PreservedError("Unhandled exception in ExecuteTaskInternal"));
	} // LCOV_EXCL_STOP
	EndQueryInternal(lock, false, true);
	return PendingExecutionResult::EXECUTION_ERROR;
}

void ClientContext::InitialCleanup(ClientContextLock &lock) {
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal(lock);
	interrupted = false;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(const string &query) {
	auto lock = LockContext();
	return ParseStatementsInternal(*lock, query);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	Parser parser(GetParserOptions());
	parser.ParseQuery(query);

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(lock, parser.statements);

	return std::move(parser.statements);
}

void ClientContext::HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements) {
	auto lock = LockContext();

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(*lock, statements);
}

unique_ptr<LogicalOperator> ClientContext::ExtractPlan(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw Exception("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	client_data->http_state = make_shared<HTTPState>();
	RunFunctionInTransactionInternal(*lock, [&]() {
		Planner planner(*this);
		planner.CreatePlan(std::move(statements[0]));
		D_ASSERT(planner.plan);

		plan = std::move(planner.plan);

		if (config.enable_optimizer) {
			Optimizer optimizer(*planner.binder, *this);
			plan = optimizer.Optimize(std::move(plan));
		}

		ColumnBindingResolver resolver;
		resolver.Verify(*plan);
		resolver.VisitOperator(*plan);

		plan->ResolveOperatorTypes();
	});
	return plan;
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto n_param = statement->n_param;
	auto named_param_map = std::move(statement->named_param_map);
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	RunFunctionInTransactionInternal(
	    lock, [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, std::move(statement)); }, false);
	prepared_data->unbound_statement = std::move(unbound_statement);
	return make_uniq<PreparedStatement>(shared_from_this(), std::move(prepared_data), std::move(statement_query),
	                                    n_param, std::move(named_param_map));
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, std::move(statement));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw Exception("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, std::move(statements[0]));
	} catch (const Exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PreparedStatement>(PreservedError(ex));
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryPreparedInternal(ClientContextLock &lock, const string &query,
                                                                           shared_ptr<PreparedStatementData> &prepared,
                                                                           const PendingQueryParameters &parameters) {
	try {
		InitialCleanup(lock);
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	return PendingStatementOrPreparedStatementInternal(lock, query, nullptr, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query,
                                                           shared_ptr<PreparedStatementData> &prepared,
                                                           const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	return PendingQueryPreparedInternal(*lock, query, prepared, parameters);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               const PendingQueryParameters &parameters) {
	auto lock = LockContext();
	auto pending = PendingQueryPreparedInternal(*lock, query, prepared, parameters);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->ExecuteInternal(*lock);
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               case_insensitive_map_t<Value> &values, bool allow_stream_result) {
	PendingQueryParameters parameters;
	parameters.parameters = &values;
	parameters.allow_stream_result = allow_stream_result;
	return Execute(query, prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementInternal(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement,
                                                                       const PendingQueryParameters &parameters) {
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(lock, query, std::move(statement), parameters.parameters);
	idx_t parameter_count = !parameters.parameters ? 0 : parameters.parameters->size();
	if (prepared->properties.parameter_count > 0 && parameter_count == 0) {
		string error_message = StringUtil::Format("Expected %lld parameters, but none were supplied",
		                                          prepared->properties.parameter_count);
		return make_uniq<PendingQueryResult>(PreservedError(error_message));
	}
	if (!prepared->properties.bound_all_parameters) {
		return make_uniq<PendingQueryResult>(PreservedError("Not all parameters were bound"));
	}
	// execute the prepared statement
	return PendingPreparedStatement(lock, std::move(prepared), parameters);
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result, bool verify) {
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	auto pending = PendingQueryInternal(lock, std::move(statement), parameters, verify);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return ExecutePendingQueryInternal(lock, *pending);
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult *result) {
	if (!active_query) {
		return false;
	}
	return active_query->open_result == result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatementInternal(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (statement && config.AnyVerification()) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		switch (statement->type) {
		case StatementType::SELECT_STATEMENT: {
			// in case this is a select query, we verify the original statement
			PreservedError error;
			try {
				error = VerifyQuery(lock, query, std::move(statement));
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(copied_statement);
			break;
		}
#ifndef DUCKDB_ALTERNATIVE_VERIFY
		case StatementType::COPY_STATEMENT:
		case StatementType::INSERT_STATEMENT:
		case StatementType::DELETE_STATEMENT:
		case StatementType::UPDATE_STATEMENT: {
			Parser parser;
			PreservedError error;
			try {
				parser.ParseQuery(statement->ToString());
			} catch (const Exception &ex) {
				error = PreservedError(ex);
			} catch (std::exception &ex) {
				error = PreservedError(ex);
			}
			if (error) {
				// error in verifying query
				return make_uniq<PendingQueryResult>(error);
			}
			statement = std::move(parser.statements[0]);
			break;
		}
#endif
		default:
			statement = std::move(copied_statement);
			break;
		}
	}
	return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingStatementOrPreparedStatement(
    ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement,
    shared_ptr<PreparedStatementData> &prepared, const PendingQueryParameters &parameters) {
	unique_ptr<PendingQueryResult> result;

	try {
		BeginQueryInternal(lock, query);
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		return result;
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	// start the profiler
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query, IsExplainAnalyze(statement ? statement.get() : prepared->unbound_statement.get()));

	bool invalidate_query = true;
	try {
		if (statement) {
			result = PendingStatementInternal(lock, query, std::move(statement), parameters);
		} else {
			if (prepared->RequireRebind(*this, parameters.parameters)) {
				// catalog was modified: rebind the statement before execution
				auto new_prepared =
				    CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy(), parameters.parameters);
				D_ASSERT(new_prepared->properties.bound_all_parameters);
				new_prepared->unbound_statement = std::move(prepared->unbound_statement);
				prepared = std::move(new_prepared);
				prepared->properties.bound_all_parameters = false;
			}
			result = PendingPreparedStatement(lock, prepared, parameters);
		}
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
		invalidate_query = false;
	} catch (FatalException &ex) {
		// fatal exceptions invalidate the entire database
		if (!config.query_verification_enabled) {
			auto &db = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db, ex.what());
		}
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (const Exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		result = make_uniq<PendingQueryResult>(PreservedError(ex));
	}
	if (result->HasError()) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query);
		return result;
	}
	D_ASSERT(active_query->open_result == result.get());
	return result;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer =
			    make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                  BufferedFileWriter::DEFAULT_OPEN_FLAGS, client_data->file_opener.get());
		} catch (...) {
			return;
		}
#else
		return;
#endif
	}
	// log query path is set: log the query
	client_data->log_query_writer->WriteData(const_data_ptr_cast(query.c_str()), query.size());
	client_data->log_query_writer->WriteData(const_data_ptr_cast("\n"), 1);
	client_data->log_query_writer->Flush();
	client_data->log_query_writer->Sync();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	auto pending_query = PendingQuery(std::move(statement), allow_stream_result);
	if (pending_query->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
	}
	return pending_query->Execute();
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<MaterializedQueryResult>(std::move(error));
	}
	if (statements.empty()) {
		// no statements, return empty successful result
		StatementProperties properties;
		vector<string> names;
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
		return make_uniq<MaterializedQueryResult>(StatementType::INVALID_STATEMENT, properties, std::move(names),
		                                          std::move(collection), GetClientProperties());
	}

	unique_ptr<QueryResult> result;
	QueryResult *last_result = nullptr;
	bool last_had_result = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();
		PendingQueryParameters parameters;
		parameters.allow_stream_result = allow_stream_result && is_last_statement;
		auto pending_query = PendingQueryInternal(*lock, std::move(statement), parameters);
		auto has_result = pending_query->properties.return_type == StatementReturnType::QUERY_RESULT;
		unique_ptr<QueryResult> current_result;
		if (pending_query->HasError()) {
			current_result = make_uniq<MaterializedQueryResult>(pending_query->GetErrorObject());
		} else {
			current_result = ExecutePendingQueryInternal(*lock, *pending_query);
		}
		// now append the result to the list of results
		if (!last_result || !last_had_result) {
			// first result of the query
			result = std::move(current_result);
			last_result = result.get();
			last_had_result = has_result;
		} else {
			// later results; attach to the result chain
			// but only if there is a result
			if (!has_result) {
				continue;
			}
			last_result->next = std::move(current_result);
			last_result = last_result->next.get();
		}
	}
	return result;
}

bool ClientContext::ParseStatements(ClientContextLock &lock, const string &query,
                                    vector<unique_ptr<SQLStatement>> &result, PreservedError &error) {
	try {
		InitialCleanup(lock);
		// parse the query and transform it into a set of statements
		result = ParseStatementsInternal(lock, query);
		return true;
	} catch (const Exception &ex) {
		error = PreservedError(ex);
		return false;
	} catch (std::exception &ex) {
		error = PreservedError(ex);
		return false;
	}
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const string &query, bool allow_stream_result) {
	auto lock = LockContext();

	PreservedError error;
	vector<unique_ptr<SQLStatement>> statements;
	if (!ParseStatements(*lock, query, statements, error)) {
		return make_uniq<PendingQueryResult>(std::move(error));
	}
	if (statements.size() != 1) {
		return make_uniq<PendingQueryResult>(PreservedError("PendingQuery can only take a single statement"));
	}
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statements[0]), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(unique_ptr<SQLStatement> statement,
                                                           bool allow_stream_result) {
	auto lock = LockContext();
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(*lock, std::move(statement), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   unique_ptr<SQLStatement> statement,
                                                                   const PendingQueryParameters &parameters,
                                                                   bool verify) {
	auto query = statement->query;
	shared_ptr<PreparedStatementData> prepared;
	if (verify) {
		return PendingStatementOrPreparedStatementInternal(lock, query, std::move(statement), prepared, parameters);
	} else {
		return PendingStatementOrPreparedStatement(lock, query, std::move(statement), prepared, parameters);
	}
}

unique_ptr<QueryResult> ClientContext::ExecutePendingQueryInternal(ClientContextLock &lock, PendingQueryResult &query) {
	return query.ExecuteInternal(lock);
}

void ClientContext::Interrupt() {
	interrupted = true;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &config = ClientConfig::GetConfig(*this);
	config.enable_profiler = false;
}

void ClientContext::RegisterFunction(CreateFunctionInfo &info) {
	RunFunctionInTransaction([&]() {
		auto existing_function = Catalog::GetEntry<ScalarFunctionCatalogEntry>(*this, INVALID_CATALOG, info.schema,
		                                                                       info.name, OnEntryNotFound::RETURN_NULL);
		if (existing_function) {
			auto &new_info = info.Cast<CreateScalarFunctionInfo>();
			if (new_info.functions.MergeFunctionSet(existing_function->functions)) {
				// function info was updated from catalog entry, rewrite is needed
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			}
		}
		// create function
		auto &catalog = Catalog::GetSystemCatalog(*this);
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(ActiveTransaction())) {
		throw TransactionException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}
	// check if we are on AutoCommit. In this case we should start a transaction
	bool require_new_transaction = transaction.IsAutoCommit() && !transaction.HasActiveTransaction();
	if (require_new_transaction) {
		D_ASSERT(!active_query);
		transaction.BeginTransaction();
	}
	try {
		fun();
	} catch (StandardException &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		}
		throw;
	} catch (FatalException &ex) {
		auto &db = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db, ex.what());
		throw;
	} catch (std::exception &ex) {
		if (require_new_transaction) {
			transaction.Rollback();
		} else {
			ValidChecker::Invalidate(ActiveTransaction(), ex.what());
		}
		throw;
	}
	if (require_new_transaction) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto table = Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, schema_name, table_name,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_uniq<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->GetColumns().Logical()) {
			result->columns.emplace_back(column.Name(), column.Type());
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, ColumnDataCollection &collection) {
	RunFunctionInTransaction([&]() {
		auto &table_entry =
		    Catalog::GetEntry<TableCatalogEntry>(*this, INVALID_CATALOG, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry.GetColumns().PhysicalColumnCount()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].Type() != table_entry.GetColumns().GetColumn(PhysicalIndex(i)).Type()) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		table_entry.GetStorage().LocalAppend(table_entry, *this, collection);
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	client_data->http_state = make_shared<HTTPState>();
	RunFunctionInTransaction([&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto result = relation.Bind(*binder);
		D_ASSERT(result.names.size() == result.types.size());

		result_columns.reserve(result_columns.size() + result.names.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.emplace_back(result.names[i], result.types[i]);
		}
	});
}

unordered_set<string> ClientContext::GetTableNames(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		binder->SetBindingMode(BindingMode::EXTRACT_NAMES);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<PendingQueryResult> ClientContext::PendingQueryInternal(ClientContextLock &lock,
                                                                   const shared_ptr<Relation> &relation,
                                                                   bool allow_stream_result) {
	InitialCleanup(lock);

	string query;
	if (config.query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		relation->GetAlias();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_uniq<SelectStatement>();
			select->node = relation->GetQueryNode();
			RunStatementInternal(lock, query, std::move(select), false);
		}
	}

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	PendingQueryParameters parameters;
	parameters.allow_stream_result = allow_stream_result;
	return PendingQueryInternal(lock, std::move(relation_stmt), parameters);
}

unique_ptr<PendingQueryResult> ClientContext::PendingQuery(const shared_ptr<Relation> &relation,
                                                           bool allow_stream_result) {
	auto lock = LockContext();
	return PendingQueryInternal(*lock, relation, allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	auto &expected_columns = relation->Columns();
	auto pending = PendingQueryInternal(*lock, relation, false);
	if (!pending->success) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}

	unique_ptr<QueryResult> result;
	result = ExecutePendingQueryInternal(*lock, *pending);
	if (result->HasError()) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->types[i] != expected_columns[i].Type() || result->names[i] != expected_columns[i].Name()) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: [";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		if (i > 0) {
			err_str += ", ";
		}
		err_str += expected_columns[i].Name() + " " + expected_columns[i].Type().ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + result->types[i].ToString();
	}
	err_str += "]";
	return make_uniq<MaterializedQueryResult>(PreservedError(err_str));
}

bool ClientContext::TryGetCurrentSetting(const std::string &key, Value &result) {
	// first check the built-in settings
	auto &db_config = DBConfig::GetConfig(*this);
	auto option = db_config.GetOptionByName(key);
	if (option) {
		result = option->get_setting(*this);
		return true;
	}

	// check the client session values
	const auto &session_config_map = config.set_variables;

	auto session_value = session_config_map.find(key);
	bool found_session_value = session_value != session_config_map.end();
	if (found_session_value) {
		result = session_value->second;
		return true;
	}
	// finally check the global session values
	return db->TryGetCurrentSetting(key, result);
}

ParserOptions ClientContext::GetParserOptions() const {
	auto &client_config = ClientConfig::GetConfig(*this);
	ParserOptions options;
	options.preserve_identifier_case = client_config.preserve_identifier_case;
	options.integer_division = client_config.integer_division;
	options.max_expression_depth = client_config.max_expression_depth;
	options.extensions = &DBConfig::GetConfig(*this).parser_extensions;
	return options;
}

ClientProperties ClientContext::GetClientProperties() const {
	string timezone = "UTC";
	Value result;
	// 1) Check Set Variable
	auto &client_config = ClientConfig::GetConfig(*this);
	auto tz_config = client_config.set_variables.find("timezone");
	if (tz_config == client_config.set_variables.end()) {
		// 2) Check for Default Value
		auto default_value = db->config.extension_parameters.find("timezone");
		if (default_value != db->config.extension_parameters.end()) {
			timezone = default_value->second.default_value.GetValue<string>();
		}
	} else {
		timezone = tz_config->second.GetValue<string>();
	}
	return {timezone, db->config.options.arrow_offset_size};
}

bool ClientContext::ExecutionIsFinished() {
	if (!active_query || !active_query->executor) {
		return false;
	}
	return active_query->executor->ExecutionIsFinished();
}

} // namespace duckdb





namespace duckdb {

bool ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	return context.TryGetCurrentSetting(key, result);
}

// LCOV_EXCL_START
bool ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &) {
	return context.TryGetCurrentSetting(key, result);
}

ClientContext *FileOpener::TryGetClientContext(FileOpener *opener) {
	if (!opener) {
		return nullptr;
	}
	return opener->TryGetClientContext();
}

bool FileOpener::TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result) {
	if (!opener) {
		return false;
	}
	return opener->TryGetCurrentSetting(key, result);
}

bool FileOpener::TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result, FileOpenerInfo &info) {
	if (!opener) {
		return false;
	}
	return opener->TryGetCurrentSetting(key, result, info);
}

bool FileOpener::TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info) {
	return this->TryGetCurrentSetting(key, result);
}
// LCOV_EXCL_STOP
} // namespace duckdb















namespace duckdb {

class ClientFileSystem : public OpenerFileSystem {
public:
	explicit ClientFileSystem(ClientContext &context_p) : context(context_p) {
	}

	FileSystem &GetFileSystem() const override {
		auto &config = DBConfig::GetConfig(context);
		return *config.file_system;
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return ClientData::Get(context).file_opener.get();
	}

private:
	ClientContext &context;
};

ClientData::ClientData(ClientContext &context) : catalog_search_path(make_uniq<CatalogSearchPath>(context)) {
	auto &db = DatabaseInstance::GetDatabase(context);
	profiler = make_shared<QueryProfiler>(context);
	query_profiler_history = make_uniq<QueryProfilerHistory>();
	temporary_objects = make_shared<AttachedDatabase>(db, AttachedDatabaseType::TEMP_DATABASE);
	temporary_objects->oid = DatabaseManager::Get(db).ModifyCatalog();
	random_engine = make_uniq<RandomEngine>();
	file_opener = make_uniq<ClientContextFileOpener>(context);
	client_file_system = make_uniq<ClientFileSystem>(context);
	temporary_objects->Initialize();
}
ClientData::~ClientData() {
}

ClientData &ClientData::Get(ClientContext &context) {
	return *context.client_data;
}

RandomEngine &RandomEngine::Get(ClientContext &context) {
	return *ClientData::Get(context).random_engine;
}

} // namespace duckdb







namespace duckdb {

static void ThrowIfExceptionIsInternal(StatementVerifier &verifier) {
	if (!verifier.materialized_result) {
		return;
	}
	auto &result = *verifier.materialized_result;
	if (!result.HasError()) {
		return;
	}
	auto &error = result.GetErrorObject();
	if (error.Type() == ExceptionType::INTERNAL) {
		error.Throw();
	}
}

PreservedError ClientContext::VerifyQuery(ClientContextLock &lock, const string &query,
                                          unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// Aggressive query verification

	// The purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// ToString() of statements and expressions
	// Correctness of plans both with and without optimizers

	const auto &stmt = *statement;
	vector<unique_ptr<StatementVerifier>> statement_verifiers;
	unique_ptr<StatementVerifier> prepared_statement_verifier;
	if (config.query_verification_enabled) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::COPIED, stmt));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::DESERIALIZED, stmt));
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::UNOPTIMIZED, stmt));
		prepared_statement_verifier = StatementVerifier::Create(VerificationType::PREPARED, stmt);
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
		// This verification is quite slow, so we only run it for the async sink/source debug mode
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::NO_OPERATOR_CACHING, stmt));
#endif
	}
	if (config.verify_external) {
		statement_verifiers.emplace_back(StatementVerifier::Create(VerificationType::EXTERNAL, stmt));
	}

	auto original = make_uniq<StatementVerifier>(std::move(statement));
	for (auto &verifier : statement_verifiers) {
		original->CheckExpressions(*verifier);
	}
	original->CheckExpressions();

	// See below
	auto statement_copy_for_explain = stmt.Copy();

	// Save settings
	bool optimizer_enabled = config.enable_optimizer;
	bool profiling_is_enabled = config.enable_profiler;
	bool force_external = config.force_external;

	// Disable profiling if it is enabled
	if (profiling_is_enabled) {
		config.enable_profiler = false;
	}

	// Execute the original statement
	bool any_failed = original->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
		return RunStatementInternal(lock, q, std::move(s), false, false);
	});
	if (!any_failed) {
		statement_verifiers.emplace_back(
		    StatementVerifier::Create(VerificationType::PARSED, *statement_copy_for_explain));
	}
	// Execute the verifiers
	for (auto &verifier : statement_verifiers) {
		bool failed = verifier->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
			return RunStatementInternal(lock, q, std::move(s), false, false);
		});
		any_failed = any_failed || failed;
	}

	if (!any_failed && prepared_statement_verifier) {
		// If none failed, we execute the prepared statement verifier
		bool failed = prepared_statement_verifier->Run(*this, query, [&](const string &q, unique_ptr<SQLStatement> s) {
			return RunStatementInternal(lock, q, std::move(s), false, false);
		});
		if (!failed) {
			// PreparedStatementVerifier fails if it runs into a ParameterNotAllowedException, which is OK
			statement_verifiers.push_back(std::move(prepared_statement_verifier));
		} else {
			// If it does fail, let's make sure it's not an internal exception
			ThrowIfExceptionIsInternal(*prepared_statement_verifier);
		}
	} else {
		if (ValidChecker::IsInvalidated(*db)) {
			return original->materialized_result->GetErrorObject();
		}
	}

	// Restore config setting
	config.enable_optimizer = optimizer_enabled;
	config.force_external = force_external;

	// Check explain, only if q does not already contain EXPLAIN
	if (original->materialized_result->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_uniq<ExplainStatement>(std::move(statement_copy_for_explain));
		try {
			RunStatementInternal(lock, explain_q, std::move(explain_stmt), false, false);
		} catch (std::exception &ex) { // LCOV_EXCL_START
			interrupted = false;
			return PreservedError("EXPLAIN failed but query did not (" + string(ex.what()) + ")");
		} // LCOV_EXCL_STOP

#ifdef DUCKDB_VERIFY_BOX_RENDERER
		// this is pretty slow, so disabled by default
		// test the box renderer on the result
		// we mostly care that this does not crash
		RandomEngine random;
		BoxRendererConfig config;
		// test with a random width
		config.max_width = random.NextRandomInteger() % 500;
		BoxRenderer renderer(config);
		renderer.ToString(*this, original->materialized_result->names, original->materialized_result->Collection());
#endif
	}

	// Restore profiler setting
	if (profiling_is_enabled) {
		config.enable_profiler = true;
	}

	// Now compare the results
	// The results of all runs should be identical
	for (auto &verifier : statement_verifiers) {
		auto result = original->CompareResults(*verifier);
		if (!result.empty()) {
			return PreservedError(result);
		}
	}

	return PreservedError();
}

} // namespace duckdb







#ifndef DUCKDB_NO_THREADS

#endif

#include <cstdio>
#include <cinttypes>

namespace duckdb {

#ifdef DEBUG
bool DBConfigOptions::debug_print_bindings = false;
#endif

#define DUCKDB_GLOBAL(_PARAM)                                                                                          \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::ResetGlobal,         \
		    nullptr, _PARAM::GetSetting                                                                                \
	}
#define DUCKDB_GLOBAL_ALIAS(_ALIAS, _PARAM)                                                                            \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, nullptr, _PARAM::ResetGlobal, nullptr,      \
		    _PARAM::GetSetting                                                                                         \
	}

#define DUCKDB_LOCAL(_PARAM)                                                                                           \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, nullptr, _PARAM::ResetLocal,  \
		    _PARAM::GetSetting                                                                                         \
	}
#define DUCKDB_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                             \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, nullptr, _PARAM::SetLocal, nullptr, _PARAM::ResetLocal,        \
		    _PARAM::GetSetting                                                                                         \
	}

#define DUCKDB_GLOBAL_LOCAL(_PARAM)                                                                                    \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal,                     \
		    _PARAM::ResetGlobal, _PARAM::ResetLocal, _PARAM::GetSetting                                                \
	}
#define DUCKDB_GLOBAL_LOCAL_ALIAS(_ALIAS, _PARAM)                                                                      \
	{                                                                                                                  \
		_ALIAS, _PARAM::Description, _PARAM::InputType, _PARAM::SetGlobal, _PARAM::SetLocal, _PARAM::ResetGlobal,      \
		    _PARAM::ResetLocal, _PARAM::GetSetting                                                                     \
	}
#define FINAL_SETTING                                                                                                  \
	{ nullptr, nullptr, LogicalTypeId::INVALID, nullptr, nullptr, nullptr, nullptr, nullptr }

static ConfigurationOption internal_options[] = {DUCKDB_GLOBAL(AccessModeSetting),
                                                 DUCKDB_GLOBAL(CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL(DebugCheckpointAbort),
                                                 DUCKDB_LOCAL(DebugForceExternal),
                                                 DUCKDB_LOCAL(DebugForceNoCrossProduct),
                                                 DUCKDB_LOCAL(DebugAsOfIEJoin),
                                                 DUCKDB_LOCAL(PreferRangeJoins),
                                                 DUCKDB_GLOBAL(DebugWindowMode),
                                                 DUCKDB_GLOBAL_LOCAL(DefaultCollationSetting),
                                                 DUCKDB_GLOBAL(DefaultOrderSetting),
                                                 DUCKDB_GLOBAL(DefaultNullOrderSetting),
                                                 DUCKDB_GLOBAL(DisabledFileSystemsSetting),
                                                 DUCKDB_GLOBAL(DisabledOptimizersSetting),
                                                 DUCKDB_GLOBAL(EnableExternalAccessSetting),
                                                 DUCKDB_GLOBAL(EnableFSSTVectors),
                                                 DUCKDB_GLOBAL(AllowUnsignedExtensionsSetting),
                                                 DUCKDB_LOCAL(CustomExtensionRepository),
                                                 DUCKDB_LOCAL(AutoloadExtensionRepository),
                                                 DUCKDB_GLOBAL(AutoinstallKnownExtensions),
                                                 DUCKDB_GLOBAL(AutoloadKnownExtensions),
                                                 DUCKDB_GLOBAL(EnableObjectCacheSetting),
                                                 DUCKDB_GLOBAL(EnableHTTPMetadataCacheSetting),
                                                 DUCKDB_LOCAL(EnableProfilingSetting),
                                                 DUCKDB_LOCAL(EnableProgressBarSetting),
                                                 DUCKDB_LOCAL(EnableProgressBarPrintSetting),
                                                 DUCKDB_LOCAL(ExplainOutputSetting),
                                                 DUCKDB_GLOBAL(ExtensionDirectorySetting),
                                                 DUCKDB_GLOBAL(ExternalThreadsSetting),
                                                 DUCKDB_LOCAL(FileSearchPathSetting),
                                                 DUCKDB_GLOBAL(ForceCompressionSetting),
                                                 DUCKDB_GLOBAL(ForceBitpackingModeSetting),
                                                 DUCKDB_LOCAL(HomeDirectorySetting),
                                                 DUCKDB_LOCAL(LogQueryPathSetting),
                                                 DUCKDB_GLOBAL(LockConfigurationSetting),
                                                 DUCKDB_GLOBAL(ImmediateTransactionModeSetting),
                                                 DUCKDB_LOCAL(IntegerDivisionSetting),
                                                 DUCKDB_LOCAL(MaximumExpressionDepthSetting),
                                                 DUCKDB_GLOBAL(MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("memory_limit", MaximumMemorySetting),
                                                 DUCKDB_GLOBAL_ALIAS("null_order", DefaultNullOrderSetting),
                                                 DUCKDB_LOCAL(OrderedAggregateThreshold),
                                                 DUCKDB_GLOBAL(PasswordSetting),
                                                 DUCKDB_LOCAL(PerfectHashThresholdSetting),
                                                 DUCKDB_LOCAL(PivotFilterThreshold),
                                                 DUCKDB_LOCAL(PivotLimitSetting),
                                                 DUCKDB_LOCAL(PreserveIdentifierCase),
                                                 DUCKDB_GLOBAL(PreserveInsertionOrder),
                                                 DUCKDB_LOCAL(ProfilerHistorySize),
                                                 DUCKDB_LOCAL(ProfileOutputSetting),
                                                 DUCKDB_LOCAL(ProfilingModeSetting),
                                                 DUCKDB_LOCAL_ALIAS("profiling_output", ProfileOutputSetting),
                                                 DUCKDB_LOCAL(ProgressBarTimeSetting),
                                                 DUCKDB_LOCAL(SchemaSetting),
                                                 DUCKDB_LOCAL(SearchPathSetting),
                                                 DUCKDB_GLOBAL(TempDirectorySetting),
                                                 DUCKDB_GLOBAL(ThreadsSetting),
                                                 DUCKDB_GLOBAL(UsernameSetting),
                                                 DUCKDB_GLOBAL(ExportLargeBufferArrow),
                                                 DUCKDB_GLOBAL_ALIAS("user", UsernameSetting),
                                                 DUCKDB_GLOBAL_ALIAS("wal_autocheckpoint", CheckpointThresholdSetting),
                                                 DUCKDB_GLOBAL_ALIAS("worker_threads", ThreadsSetting),
                                                 DUCKDB_GLOBAL(FlushAllocatorSetting),
                                                 FINAL_SETTING};

vector<ConfigurationOption> DBConfig::GetOptions() {
	vector<ConfigurationOption> options;
	for (idx_t index = 0; internal_options[index].name; index++) {
		options.push_back(internal_options[index]);
	}
	return options;
}

idx_t DBConfig::GetOptionCount() {
	idx_t count = 0;
	for (idx_t index = 0; internal_options[index].name; index++) {
		count++;
	}
	return count;
}

vector<std::string> DBConfig::GetOptionNames() {
	vector<string> names;
	for (idx_t i = 0, option_count = DBConfig::GetOptionCount(); i < option_count; i++) {
		names.emplace_back(DBConfig::GetOptionByIndex(i)->name);
	}
	return names;
}

ConfigurationOption *DBConfig::GetOptionByIndex(idx_t target_index) {
	for (idx_t index = 0; internal_options[index].name; index++) {
		if (index == target_index) {
			return internal_options + index;
		}
	}
	return nullptr;
}

ConfigurationOption *DBConfig::GetOptionByName(const string &name) {
	auto lname = StringUtil::Lower(name);
	for (idx_t index = 0; internal_options[index].name; index++) {
		D_ASSERT(StringUtil::Lower(internal_options[index].name) == string(internal_options[index].name));
		if (internal_options[index].name == lname) {
			return internal_options + index;
		}
	}
	return nullptr;
}

void DBConfig::SetOption(const ConfigurationOption &option, const Value &value) {
	SetOption(nullptr, option, value);
}

void DBConfig::SetOptionByName(const string &name, const Value &value) {
	auto option = DBConfig::GetOptionByName(name);
	if (option) {
		SetOption(*option, value);
	} else {
		options.unrecognized_options[name] = value;
	}
}

void DBConfig::SetOption(DatabaseInstance *db, const ConfigurationOption &option, const Value &value) {
	lock_guard<mutex> l(config_lock);
	if (!option.set_global) {
		throw InvalidInputException("Could not set option \"%s\" as a global option", option.name);
	}
	D_ASSERT(option.reset_global);
	Value input = value.DefaultCastAs(option.parameter_type);
	option.set_global(db, *this, input);
}

void DBConfig::ResetOption(DatabaseInstance *db, const ConfigurationOption &option) {
	lock_guard<mutex> l(config_lock);
	if (!option.reset_global) {
		throw InternalException("Could not reset option \"%s\" as a global option", option.name);
	}
	D_ASSERT(option.set_global);
	option.reset_global(db, *this);
}

void DBConfig::SetOption(const string &name, Value value) {
	lock_guard<mutex> l(config_lock);
	options.set_variables[name] = std::move(value);
}

void DBConfig::ResetOption(const string &name) {
	lock_guard<mutex> l(config_lock);
	auto extension_option = extension_parameters.find(name);
	D_ASSERT(extension_option != extension_parameters.end());
	auto &default_value = extension_option->second.default_value;
	if (!default_value.IsNull()) {
		// Default is not NULL, override the setting
		options.set_variables[name] = default_value;
	} else {
		// Otherwise just remove it from the 'set_variables' map
		options.set_variables.erase(name);
	}
}

void DBConfig::AddExtensionOption(const string &name, string description, LogicalType parameter,
                                  const Value &default_value, set_option_callback_t function) {
	extension_parameters.insert(
	    make_pair(name, ExtensionOption(std::move(description), std::move(parameter), function, default_value)));
	if (!default_value.IsNull()) {
		// Default value is set, insert it into the 'set_variables' list
		options.set_variables[name] = default_value;
	}
}

CastFunctionSet &DBConfig::GetCastFunctions() {
	return *cast_functions;
}

void DBConfig::SetDefaultMaxMemory() {
	auto memory = FileSystem::GetAvailableMemory();
	if (memory != DConstants::INVALID_INDEX) {
		options.maximum_memory = memory * 8 / 10;
	}
}

idx_t CGroupBandwidthQuota(idx_t physical_cores, FileSystem &fs) {
	static constexpr const char *CPU_MAX = "/sys/fs/cgroup/cpu.max";
	static constexpr const char *CFS_QUOTA = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
	static constexpr const char *CFS_PERIOD = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

	int64_t quota, period;
	char byte_buffer[1000];
	unique_ptr<FileHandle> handle;
	int64_t read_bytes;

	if (fs.FileExists(CPU_MAX)) {
		// cgroup v2
		// https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
		handle =
		    fs.OpenFile(CPU_MAX, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK, FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
			return physical_cores;
		}
	} else if (fs.FileExists(CFS_QUOTA) && fs.FileExists(CFS_PERIOD)) {
		// cgroup v1
		// https://www.kernel.org/doc/html/latest/scheduler/sched-bwc.html#management

		// Read the quota, this indicates how many microseconds the CPU can be utilized by this cgroup per period
		handle = fs.OpenFile(CFS_QUOTA, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
		                     FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &quota) != 1) {
			return physical_cores;
		}

		// Read the time period, a cgroup can utilize the CPU up to quota microseconds every period
		handle = fs.OpenFile(CFS_PERIOD, FileFlags::FILE_FLAGS_READ, FileSystem::DEFAULT_LOCK,
		                     FileSystem::DEFAULT_COMPRESSION);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &period) != 1) {
			return physical_cores;
		}
	} else {
		// No cgroup quota
		return physical_cores;
	}
	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	} else {
		return physical_cores;
	}
}

idx_t DBConfig::GetSystemMaxThreads(FileSystem &fs) {
#ifndef DUCKDB_NO_THREADS
	idx_t physical_cores = std::thread::hardware_concurrency();
#ifdef __linux__
	auto cores_available_per_period = CGroupBandwidthQuota(physical_cores, fs);
	return MaxValue<idx_t>(cores_available_per_period, 1);
#else
	return physical_cores;
#endif
#else
	return 1;
#endif
}

void DBConfig::SetDefaultMaxThreads() {
#ifndef DUCKDB_NO_THREADS
	options.maximum_threads = GetSystemMaxThreads(*file_system);
#else
	options.maximum_threads = 1;
#endif
}

idx_t DBConfig::ParseMemoryLimit(const string &arg) {
	if (arg[0] == '-' || arg == "null" || arg == "none") {
		return DConstants::INVALID_INDEX;
	}
	// split based on the number/non-number
	idx_t idx = 0;
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t num_start = idx;
	while ((arg[idx] >= '0' && arg[idx] <= '9') || arg[idx] == '.' || arg[idx] == 'e' || arg[idx] == 'E' ||
	       arg[idx] == '-') {
		idx++;
	}
	if (idx == num_start) {
		throw ParserException("Memory limit must have a number (e.g. SET memory_limit=1GB");
	}
	string number = arg.substr(num_start, idx - num_start);

	// try to parse the number
	double limit = Cast::Operation<string_t, double>(string_t(number));

	// now parse the memory limit unit (e.g. bytes, gb, etc)
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t start = idx;
	while (idx < arg.size() && !StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	if (limit < 0) {
		// limit < 0, set limit to infinite
		return (idx_t)-1;
	}
	string unit = StringUtil::Lower(arg.substr(start, idx - start));
	idx_t multiplier;
	if (unit == "byte" || unit == "bytes" || unit == "b") {
		multiplier = 1;
	} else if (unit == "kilobyte" || unit == "kilobytes" || unit == "kb" || unit == "k") {
		multiplier = 1000LL;
	} else if (unit == "megabyte" || unit == "megabytes" || unit == "mb" || unit == "m") {
		multiplier = 1000LL * 1000LL;
	} else if (unit == "gigabyte" || unit == "gigabytes" || unit == "gb" || unit == "g") {
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (unit == "terabyte" || unit == "terabytes" || unit == "tb" || unit == "t") {
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	} else {
		throw ParserException("Unknown unit for memory_limit: %s (expected: b, mb, gb or tb)", unit);
	}
	return (idx_t)multiplier * limit;
}

// Right now we only really care about access mode when comparing DBConfigs
bool DBConfigOptions::operator==(const DBConfigOptions &other) const {
	return other.access_mode == access_mode;
}

bool DBConfig::operator==(const DBConfig &other) {
	return other.options == options;
}

bool DBConfig::operator!=(const DBConfig &other) {
	return !(other.options == options);
}

OrderType DBConfig::ResolveOrder(OrderType order_type) const {
	if (order_type != OrderType::ORDER_DEFAULT) {
		return order_type;
	}
	return options.default_order_type;
}

OrderByNullType DBConfig::ResolveNullOrder(OrderType order_type, OrderByNullType null_type) const {
	if (null_type != OrderByNullType::ORDER_DEFAULT) {
		return null_type;
	}
	switch (options.default_null_order) {
	case DefaultOrderByNullType::NULLS_FIRST:
		return OrderByNullType::NULLS_FIRST;
	case DefaultOrderByNullType::NULLS_LAST:
		return OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		return order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
	default:
		throw InternalException("Unknown null order setting");
	}
}

} // namespace duckdb



















namespace duckdb {

Connection::Connection(DatabaseInstance &database) : context(make_shared<ClientContext>(database.shared_from_this())) {
	ConnectionManager::Get(database).AddConnection(*context);
#ifdef DEBUG
	EnableProfiling();
	context->config.emit_profiler_output = false;
#endif
}

Connection::Connection(DuckDB &database) : Connection(*database.instance) {
}

Connection::~Connection() {
	ConnectionManager::Get(*context->db).RemoveConnection(*context);
}

string Connection::GetProfilingInformation(ProfilerPrintFormat format) {
	auto &profiler = QueryProfiler::Get(*context);
	if (format == ProfilerPrintFormat::JSON) {
		return profiler.ToJSON();
	} else {
		return profiler.QueryTreeToString();
	}
}

void Connection::Interrupt() {
	context->Interrupt();
}

void Connection::EnableProfiling() {
	context->EnableProfiling();
}

void Connection::DisableProfiling() {
	context->DisableProfiling();
}

void Connection::EnableQueryVerification() {
	ClientConfig::GetConfig(*context).query_verification_enabled = true;
}

void Connection::DisableQueryVerification() {
	ClientConfig::GetConfig(*context).query_verification_enabled = false;
}

void Connection::ForceParallelism() {
	ClientConfig::GetConfig(*context).verify_parallelism = true;
}

unique_ptr<QueryResult> Connection::SendQuery(const string &query) {
	return context->Query(query, true);
}

unique_ptr<MaterializedQueryResult> Connection::Query(const string &query) {
	auto result = context->Query(query, false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
}

DUCKDB_API string Connection::GetSubstrait(const string &query) {
	vector<Value> params;
	params.emplace_back(query);
	auto result = TableFunction("get_substrait", params)->Execute();
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

DUCKDB_API unique_ptr<QueryResult> Connection::FromSubstrait(const string &proto) {
	vector<Value> params;
	params.emplace_back(Value::BLOB_RAW(proto));
	return TableFunction("from_substrait", params)->Execute();
}

DUCKDB_API string Connection::GetSubstraitJSON(const string &query) {
	vector<Value> params;
	params.emplace_back(query);
	auto result = TableFunction("get_substrait_json", params)->Execute();
	auto protobuf = result->FetchRaw()->GetValue(0, 0);
	return protobuf.GetValueUnsafe<string_t>().GetString();
}

DUCKDB_API unique_ptr<QueryResult> Connection::FromSubstraitJSON(const string &json) {
	vector<Value> params;
	params.emplace_back(json);
	return TableFunction("from_substrait_json", params)->Execute();
}

unique_ptr<MaterializedQueryResult> Connection::Query(unique_ptr<SQLStatement> statement) {
	auto result = context->Query(std::move(statement), false);
	D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
}

unique_ptr<PendingQueryResult> Connection::PendingQuery(const string &query, bool allow_stream_result) {
	return context->PendingQuery(query, allow_stream_result);
}

unique_ptr<PendingQueryResult> Connection::PendingQuery(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	return context->PendingQuery(std::move(statement), allow_stream_result);
}

unique_ptr<PreparedStatement> Connection::Prepare(const string &query) {
	return context->Prepare(query);
}

unique_ptr<PreparedStatement> Connection::Prepare(unique_ptr<SQLStatement> statement) {
	return context->Prepare(std::move(statement));
}

unique_ptr<QueryResult> Connection::QueryParamsRecursive(const string &query, vector<Value> &values) {
	auto statement = Prepare(query);
	if (statement->HasError()) {
		return make_uniq<MaterializedQueryResult>(statement->error);
	}
	return statement->Execute(values, false);
}

unique_ptr<TableDescription> Connection::TableInfo(const string &table_name) {
	return TableInfo(INVALID_SCHEMA, table_name);
}

unique_ptr<TableDescription> Connection::TableInfo(const string &schema_name, const string &table_name) {
	return context->TableInfo(schema_name, table_name);
}

vector<unique_ptr<SQLStatement>> Connection::ExtractStatements(const string &query) {
	return context->ParseStatements(query);
}

unique_ptr<LogicalOperator> Connection::ExtractPlan(const string &query) {
	return context->ExtractPlan(query);
}

void Connection::Append(TableDescription &description, DataChunk &chunk) {
	if (chunk.size() == 0) {
		return;
	}
	ColumnDataCollection collection(Allocator::Get(*context), chunk.GetTypes());
	collection.Append(chunk);
	Append(description, collection);
}

void Connection::Append(TableDescription &description, ColumnDataCollection &collection) {
	context->Append(description, collection);
}

shared_ptr<Relation> Connection::Table(const string &table_name) {
	return Table(DEFAULT_SCHEMA, table_name);
}

shared_ptr<Relation> Connection::Table(const string &schema_name, const string &table_name) {
	auto table_info = TableInfo(schema_name, table_name);
	if (!table_info) {
		throw CatalogException("Table '%s' does not exist!", table_name);
	}
	return make_shared<TableRelation>(context, std::move(table_info));
}

shared_ptr<Relation> Connection::View(const string &tname) {
	return View(DEFAULT_SCHEMA, tname);
}

shared_ptr<Relation> Connection::View(const string &schema_name, const string &table_name) {
	return make_shared<ViewRelation>(context, schema_name, table_name);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname) {
	vector<Value> values;
	named_parameter_map_t named_parameters;
	return TableFunction(fname, values, named_parameters);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values,
                                               const named_parameter_map_t &named_parameters) {
	return make_shared<TableFunctionRelation>(context, fname, values, named_parameters);
}

shared_ptr<Relation> Connection::TableFunction(const string &fname, const vector<Value> &values) {
	return make_shared<TableFunctionRelation>(context, fname, values);
}

shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values) {
	vector<string> column_names;
	return Values(values, column_names);
}

shared_ptr<Relation> Connection::Values(const vector<vector<Value>> &values, const vector<string> &column_names,
                                        const string &alias) {
	return make_shared<ValueRelation>(context, values, column_names, alias);
}

shared_ptr<Relation> Connection::Values(const string &values) {
	vector<string> column_names;
	return Values(values, column_names);
}

shared_ptr<Relation> Connection::Values(const string &values, const vector<string> &column_names, const string &alias) {
	return make_shared<ValueRelation>(context, values, column_names, alias);
}

shared_ptr<Relation> Connection::ReadCSV(const string &csv_file) {
	named_parameter_map_t options;
	return ReadCSV(csv_file, std::move(options));
}

shared_ptr<Relation> Connection::ReadCSV(const string &csv_file, named_parameter_map_t &&options) {
	return make_shared<ReadCSVRelation>(context, csv_file, std::move(options));
}

shared_ptr<Relation> Connection::ReadCSV(const string &csv_file, const vector<string> &columns) {
	// parse columns
	vector<ColumnDefinition> column_list;
	for (auto &column : columns) {
		auto col_list = Parser::ParseColumnList(column, context->GetParserOptions());
		if (col_list.LogicalColumnCount() != 1) {
			throw ParserException("Expected a single column definition");
		}
		column_list.push_back(std::move(col_list.GetColumnMutable(LogicalIndex(0))));
	}
	return make_shared<ReadCSVRelation>(context, csv_file, std::move(column_list));
}

shared_ptr<Relation> Connection::ReadParquet(const string &parquet_file, bool binary_as_string) {
	vector<Value> params;
	params.emplace_back(parquet_file);
	named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(binary_as_string)}});
	return TableFunction("parquet_scan", params, named_parameters)->Alias(parquet_file);
}

unordered_set<string> Connection::GetTableNames(const string &query) {
	return context->GetTableNames(query);
}

shared_ptr<Relation> Connection::RelationFromQuery(const string &query, const string &alias, const string &error) {
	return RelationFromQuery(QueryRelation::ParseStatement(*context, query, error), alias);
}

shared_ptr<Relation> Connection::RelationFromQuery(unique_ptr<SelectStatement> select_stmt, const string &alias) {
	return make_shared<QueryRelation>(context, std::move(select_stmt), alias);
}

void Connection::BeginTransaction() {
	auto result = Query("BEGIN TRANSACTION");
	if (result->HasError()) {
		result->ThrowError();
	}
}

void Connection::Commit() {
	auto result = Query("COMMIT");
	if (result->HasError()) {
		result->ThrowError();
	}
}

void Connection::Rollback() {
	auto result = Query("ROLLBACK");
	if (result->HasError()) {
		result->ThrowError();
	}
}

void Connection::SetAutoCommit(bool auto_commit) {
	context->transaction.SetAutoCommit(auto_commit);
}

bool Connection::IsAutoCommit() {
	return context->transaction.IsAutoCommit();
}
bool Connection::HasActiveTransaction() {
	return context->transaction.HasActiveTransaction();
}

} // namespace duckdb























#ifndef DUCKDB_NO_THREADS

#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_uniq<CompressionFunctionSet>();
	cast_functions = make_uniq<CastFunctionSet>();
	error_manager = make_uniq<ErrorManager>();
}

DBConfig::DBConfig(std::unordered_map<string, string> &config_dict, bool read_only) : DBConfig::DBConfig() {
	if (read_only) {
		options.access_mode = AccessMode::READ_ONLY;
	}
	for (auto &kv : config_dict) {
		string key = kv.first;
		string val = kv.second;
		auto opt_val = Value(val);
		DBConfig::SetOptionByName(key, opt_val);
	}
}

DBConfig::~DBConfig() {
}

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return db.GetBufferManager();
}

BufferManager &BufferManager::GetBufferManager(AttachedDatabase &db) {
	return BufferManager::GetBufferManager(db.GetDatabase());
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
	return *context.db;
}

DatabaseManager &DatabaseInstance::GetDatabaseManager() {
	if (!db_manager) {
		throw InternalException("Missing DB manager");
	}
	return *db_manager;
}

Catalog &Catalog::GetSystemCatalog(DatabaseInstance &db) {
	return db.GetDatabaseManager().GetSystemCatalog();
}

Catalog &Catalog::GetCatalog(AttachedDatabase &db) {
	return db.GetCatalog();
}

FileSystem &FileSystem::GetFileSystem(DatabaseInstance &db) {
	return db.GetFileSystem();
}

FileSystem &FileSystem::Get(AttachedDatabase &db) {
	return FileSystem::GetFileSystem(db.GetDatabase());
}

DBConfig &DBConfig::GetConfig(DatabaseInstance &db) {
	return db.config;
}

ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
	return context.config;
}

DBConfig &DBConfig::Get(AttachedDatabase &db) {
	return DBConfig::GetConfig(db.GetDatabase());
}

const DBConfig &DBConfig::GetConfig(const DatabaseInstance &db) {
	return db.config;
}

const ClientConfig &ClientConfig::GetConfig(const ClientContext &context) {
	return context.config;
}

TransactionManager &TransactionManager::Get(AttachedDatabase &db) {
	return db.GetTransactionManager();
}

ConnectionManager &ConnectionManager::Get(DatabaseInstance &db) {
	return db.GetConnectionManager();
}

ClientContext *ConnectionManager::GetConnection(DatabaseInstance *db) {
	for (auto &conn : connections) {
		if (conn.first->db.get() == db) {
			return conn.first;
		}
	}
	return nullptr;
}

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
}

duckdb::unique_ptr<AttachedDatabase> DatabaseInstance::CreateAttachedDatabase(AttachInfo &info, const string &type,
                                                                              AccessMode access_mode) {
	duckdb::unique_ptr<AttachedDatabase> attached_database;
	if (!type.empty()) {
		// find the storage extension
		auto extension_name = ExtensionHelper::ApplyExtensionAlias(type);
		auto entry = config.storage_extensions.find(extension_name);
		if (entry == config.storage_extensions.end()) {
			throw BinderException("Unrecognized storage type \"%s\"", type);
		}

		if (entry->second->attach != nullptr && entry->second->create_transaction_manager != nullptr) {
			// use storage extension to create the initial database
			attached_database = make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), *entry->second,
			                                                info.name, info, access_mode);
		} else {
			attached_database =
			    make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name, info.path, access_mode);
		}
	} else {
		// check if this is an in-memory database or not
		attached_database =
		    make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name, info.path, access_mode);
	}
	return attached_database;
}

void DatabaseInstance::CreateMainDatabase() {
	AttachInfo info;
	info.name = AttachedDatabase::ExtractDatabaseName(config.options.database_path, GetFileSystem());
	info.path = config.options.database_path;

	auto attached_database = CreateAttachedDatabase(info, config.options.database_type, config.options.access_mode);
	auto initial_database = attached_database.get();
	{
		Connection con(*this);
		con.BeginTransaction();
		db_manager->AddDatabase(*con.context, std::move(attached_database));
		con.Commit();
	}

	// initialize the database
	initial_database->SetInitialDatabase();
	initial_database->Initialize();
}

void ThrowExtensionSetUnrecognizedOptions(const unordered_map<string, Value> &unrecognized_options) {
	auto unrecognized_options_iter = unrecognized_options.begin();
	string unrecognized_option_keys = unrecognized_options_iter->first;
	while (++unrecognized_options_iter != unrecognized_options.end()) {
		unrecognized_option_keys = "," + unrecognized_options_iter->first;
	}
	throw InvalidInputException("Unrecognized configuration property \"%s\"", unrecognized_option_keys);
}

void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
	DBConfig default_config;
	DBConfig *config_ptr = &default_config;
	if (user_config) {
		config_ptr = user_config;
	}

	if (config_ptr->options.temporary_directory.empty() && database_path) {
		// no directory specified: use default temp path
		config_ptr->options.temporary_directory = string(database_path) + ".tmp";

		// special treatment for in-memory mode
		if (strcmp(database_path, ":memory:") == 0) {
			config_ptr->options.temporary_directory = ".tmp";
		}
	}

	if (database_path) {
		config_ptr->options.database_path = database_path;
	} else {
		config_ptr->options.database_path.clear();
	}
	Configure(*config_ptr);

	if (user_config && !user_config->options.use_temporary_directory) {
		// temporary directories explicitly disabled
		config.options.temporary_directory = string();
	}

	db_manager = make_uniq<DatabaseManager>(*this);
	buffer_manager = make_uniq<StandardBufferManager>(*this, config.options.temporary_directory);
	scheduler = make_uniq<TaskScheduler>(*this);
	object_cache = make_uniq<ObjectCache>();
	connection_manager = make_uniq<ConnectionManager>();

	// check if we are opening a standard DuckDB database or an extension database
	if (config.options.database_type.empty()) {
		auto path_and_type = DBPathAndType::Parse(config.options.database_path, config);
		config.options.database_type = path_and_type.type;
		config.options.database_path = path_and_type.path;
	}

	// initialize the system catalog
	db_manager->InitializeSystemCatalog();

	if (!config.options.database_type.empty()) {
		// if we are opening an extension database - load the extension
		if (!config.file_system) {
			throw InternalException("No file system!?");
		}
		ExtensionHelper::LoadExternalExtension(*this, *config.file_system, config.options.database_type, nullptr);
	}

	if (!config.options.unrecognized_options.empty()) {
		ThrowExtensionSetUnrecognizedOptions(config.options.unrecognized_options);
	}

	if (!db_manager->HasDefaultDatabase()) {
		CreateMainDatabase();
	}

	// only increase thread count after storage init because we get races on catalog otherwise
	scheduler->SetThreads(config.options.maximum_threads);
}

DuckDB::DuckDB(const char *path, DBConfig *new_config) : instance(make_shared<DatabaseInstance>()) {
	instance->Initialize(path, new_config);
	if (instance->config.options.load_extensions) {
		ExtensionHelper::LoadAllExtensions(*this);
	}
}

DuckDB::DuckDB(const string &path, DBConfig *config) : DuckDB(path.c_str(), config) {
}

DuckDB::DuckDB(DatabaseInstance &instance_p) : instance(instance_p.shared_from_this()) {
}

DuckDB::~DuckDB() {
}

BufferManager &DatabaseInstance::GetBufferManager() {
	return *buffer_manager;
}

BufferPool &DatabaseInstance::GetBufferPool() {
	return *config.buffer_pool;
}

DatabaseManager &DatabaseManager::Get(DatabaseInstance &db) {
	return db.GetDatabaseManager();
}

DatabaseManager &DatabaseManager::Get(ClientContext &db) {
	return DatabaseManager::Get(*db.db);
}

TaskScheduler &DatabaseInstance::GetScheduler() {
	return *scheduler;
}

ObjectCache &DatabaseInstance::GetObjectCache() {
	return *object_cache;
}

FileSystem &DatabaseInstance::GetFileSystem() {
	return *config.file_system;
}

ConnectionManager &DatabaseInstance::GetConnectionManager() {
	return *connection_manager;
}

FileSystem &DuckDB::GetFileSystem() {
	return instance->GetFileSystem();
}

Allocator &Allocator::Get(ClientContext &context) {
	return Allocator::Get(*context.db);
}

Allocator &Allocator::Get(DatabaseInstance &db) {
	return *db.config.allocator;
}

Allocator &Allocator::Get(AttachedDatabase &db) {
	return Allocator::Get(db.GetDatabase());
}

void DatabaseInstance::Configure(DBConfig &new_config) {
	config.options = new_config.options;
	if (config.options.access_mode == AccessMode::UNDEFINED) {
		config.options.access_mode = AccessMode::READ_WRITE;
	}
	if (new_config.file_system) {
		config.file_system = std::move(new_config.file_system);
	} else {
		config.file_system = make_uniq<VirtualFileSystem>();
	}
	if (config.options.maximum_memory == (idx_t)-1) {
		config.SetDefaultMaxMemory();
	}
	if (new_config.options.maximum_threads == (idx_t)-1) {
		config.SetDefaultMaxThreads();
	}
	config.allocator = std::move(new_config.allocator);
	if (!config.allocator) {
		config.allocator = make_uniq<Allocator>();
	}
	config.replacement_scans = std::move(new_config.replacement_scans);
	config.parser_extensions = std::move(new_config.parser_extensions);
	config.error_manager = std::move(new_config.error_manager);
	if (!config.error_manager) {
		config.error_manager = make_uniq<ErrorManager>();
	}
	if (!config.default_allocator) {
		config.default_allocator = Allocator::DefaultAllocatorReference();
	}
	if (new_config.buffer_pool) {
		config.buffer_pool = std::move(new_config.buffer_pool);
	} else {
		config.buffer_pool = make_shared<BufferPool>(config.options.maximum_memory);
	}
}

DBConfig &DBConfig::GetConfig(ClientContext &context) {
	return context.db->config;
}

const DBConfig &DBConfig::GetConfig(const ClientContext &context) {
	return context.db->config;
}

idx_t DatabaseInstance::NumberOfThreads() {
	return scheduler->NumberOfThreads();
}

const unordered_set<std::string> &DatabaseInstance::LoadedExtensions() {
	return loaded_extensions;
}

idx_t DuckDB::NumberOfThreads() {
	return instance->NumberOfThreads();
}

bool DatabaseInstance::ExtensionIsLoaded(const std::string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	return loaded_extensions.find(extension_name) != loaded_extensions.end();
}

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->ExtensionIsLoaded(name);
}

void DatabaseInstance::SetExtensionLoaded(const std::string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	loaded_extensions.insert(extension_name);

	auto &callbacks = DBConfig::GetConfig(*this).extension_callbacks;
	for (auto &callback : callbacks) {
		callback->OnExtensionLoaded(*this, name);
	}
}

bool DatabaseInstance::TryGetCurrentSetting(const std::string &key, Value &result) {
	// check the session values
	auto &db_config = DBConfig::GetConfig(*this);
	const auto &global_config_map = db_config.options.set_variables;

	auto global_value = global_config_map.find(key);
	bool found_global_value = global_value != global_config_map.end();
	if (!found_global_value) {
		return false;
	}
	result = global_value->second;
	return true;
}

ValidChecker &DatabaseInstance::GetValidChecker() {
	return db_validity;
}

ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
	return db.GetValidChecker();
}

} // namespace duckdb







namespace duckdb {

DatabaseManager::DatabaseManager(DatabaseInstance &db) : catalog_version(0), current_query_number(1) {
	system = make_uniq<AttachedDatabase>(db);
	databases = make_uniq<CatalogSet>(system->GetCatalog());
}

DatabaseManager::~DatabaseManager() {
}

DatabaseManager &DatabaseManager::Get(AttachedDatabase &db) {
	return DatabaseManager::Get(db.GetDatabase());
}

void DatabaseManager::InitializeSystemCatalog() {
	system->Initialize();
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabase(ClientContext &context, const string &name) {
	if (StringUtil::Lower(name) == TEMP_CATALOG) {
		return context.client_data->temporary_objects.get();
	}
	return reinterpret_cast<AttachedDatabase *>(databases->GetEntry(context, name).get());
}

void DatabaseManager::AddDatabase(ClientContext &context, unique_ptr<AttachedDatabase> db_instance) {
	auto name = db_instance->GetName();
	db_instance->oid = ModifyCatalog();
	DependencyList dependencies;
	if (default_database.empty()) {
		default_database = name;
	}
	if (!databases->CreateEntry(context, name, std::move(db_instance), dependencies)) {
		throw BinderException("Failed to attach database: database with name \"%s\" already exists", name);
	}
}

void DatabaseManager::DetachDatabase(ClientContext &context, const string &name, OnEntryNotFound if_not_found) {
	if (GetDefaultDatabase(context) == name) {
		throw BinderException("Cannot detach database \"%s\" because it is the default database. Select a different "
		                      "database using `USE` to allow detaching this database",
		                      name);
	}
	if (!databases->DropEntry(context, name, false, true)) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw BinderException("Failed to detach database with name \"%s\": database not found", name);
		}
	}
}

optional_ptr<AttachedDatabase> DatabaseManager::GetDatabaseFromPath(ClientContext &context, const string &path) {
	auto databases = GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = db_ref.get();
		if (db.IsSystem()) {
			continue;
		}
		auto &catalog = Catalog::GetCatalog(db);
		if (catalog.InMemory()) {
			continue;
		}
		auto db_path = catalog.GetDBPath();
		if (StringUtil::CIEquals(path, db_path)) {
			return &db;
		}
	}
	return nullptr;
}

const string &DatabaseManager::GetDefaultDatabase(ClientContext &context) {
	auto &config = ClientData::Get(context);
	auto &default_entry = config.catalog_search_path->GetDefault();
	if (IsInvalidCatalog(default_entry.catalog)) {
		auto &result = DatabaseManager::Get(context).default_database;
		if (result.empty()) {
			throw InternalException("Calling DatabaseManager::GetDefaultDatabase with no default database set");
		}
		return result;
	}
	return default_entry.catalog;
}

// LCOV_EXCL_START
void DatabaseManager::SetDefaultDatabase(ClientContext &context, const string &new_value) {
	auto db_entry = GetDatabase(context, new_value);

	if (!db_entry) {
		throw InternalException("Database \"%s\" not found", new_value);
	} else if (db_entry->IsTemporary()) {
		throw InternalException("Cannot set the default database to a temporary database");
	} else if (db_entry->IsSystem()) {
		throw InternalException("Cannot set the default database to a system database");
	}

	default_database = new_value;
}
// LCOV_EXCL_STOP

vector<reference<AttachedDatabase>> DatabaseManager::GetDatabases(ClientContext &context) {
	vector<reference<AttachedDatabase>> result;
	databases->Scan(context, [&](CatalogEntry &entry) { result.push_back(entry.Cast<AttachedDatabase>()); });
	result.push_back(*system);
	result.push_back(*context.client_data->temporary_objects);
	return result;
}

Catalog &DatabaseManager::GetSystemCatalog() {
	D_ASSERT(system);
	return system->GetCatalog();
}

} // namespace duckdb





namespace duckdb {

DBPathAndType DBPathAndType::Parse(const string &combined_path, const DBConfig &config) {
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(combined_path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove it
		auto path = StringUtil::Replace(combined_path, extension + ":", "");
		auto type = ExtensionHelper::ApplyExtensionAlias(extension);
		return {path, type};
	}
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(config.file_system.get(), combined_path);
	if (file_type == DataFileType::SQLITE_FILE) {
		return {combined_path, "sqlite"};
	}
	return {combined_path, string()};
}
} // namespace duckdb



namespace duckdb {

string GetDBAbsolutePath(const string &database_p, FileSystem &fs) {
	auto database = FileSystem::ExpandPath(database_p, nullptr);
	if (database.empty()) {
		return ":memory:";
	}
	if (database.rfind(":memory:", 0) == 0) {
		// this is a memory db, just return it.
		return database;
	}
	if (!ExtensionHelper::ExtractExtensionPrefixFromPath(database).empty()) {
		// this database path is handled by a replacement open and is not a file path
		return database;
	}
	if (fs.IsPathAbsolute(database)) {
		return fs.NormalizeAbsolutePath(database);
	}
	return fs.NormalizeAbsolutePath(fs.JoinPath(FileSystem::GetWorkingDirectory(), database));
}

shared_ptr<DuckDB> DBInstanceCache::GetInstanceInternal(const string &database, const DBConfig &config) {
	shared_ptr<DuckDB> db_instance;

	auto local_fs = FileSystem::CreateLocal();
	auto abs_database_path = GetDBAbsolutePath(database, *local_fs);
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		db_instance = db_instances[abs_database_path].lock();
		if (db_instance) {
			if (db_instance->instance->config != config) {
				throw duckdb::ConnectionException(
				    "Can't open a connection to same database file with a different configuration "
				    "than existing connections");
			}
		} else {
			// clean-up
			db_instances.erase(abs_database_path);
		}
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::GetInstance(const string &database, const DBConfig &config) {
	lock_guard<mutex> l(cache_lock);
	return GetInstanceInternal(database, config);
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstanceInternal(const string &database, DBConfig &config,
                                                           bool cache_instance) {
	string abs_database_path;
	if (config.file_system) {
		abs_database_path = GetDBAbsolutePath(database, *config.file_system);
	} else {
		auto tmp_fs = FileSystem::CreateLocal();
		abs_database_path = GetDBAbsolutePath(database, *tmp_fs);
	}
	if (db_instances.find(abs_database_path) != db_instances.end()) {
		throw duckdb::Exception(ExceptionType::CONNECTION,
		                        "Instance with path: " + abs_database_path + " already exists.");
	}
	// Creates new instance
	string instance_path = abs_database_path;
	if (abs_database_path.rfind(":memory:", 0) == 0) {
		instance_path = ":memory:";
	}
	auto db_instance = make_shared<DuckDB>(instance_path, &config);
	if (cache_instance) {
		db_instances[abs_database_path] = db_instance;
	}
	return db_instance;
}

shared_ptr<DuckDB> DBInstanceCache::CreateInstance(const string &database, DBConfig &config, bool cache_instance) {
	lock_guard<mutex> l(cache_lock);
	return CreateInstanceInternal(database, config, cache_instance);
}

shared_ptr<DuckDB> DBInstanceCache::GetOrCreateInstance(const string &database, DBConfig &config_dict,
                                                        bool cache_instance) {
	lock_guard<mutex> l(cache_lock);
	if (cache_instance) {
		auto instance = GetInstanceInternal(database, config_dict);
		if (instance) {
			return instance;
		}
	}
	return CreateInstanceInternal(database, config_dict, cache_instance);
}

} // namespace duckdb




namespace duckdb {

struct DefaultError {
	ErrorType type;
	const char *error;
};

static DefaultError internal_errors[] = {
    {ErrorType::UNSIGNED_EXTENSION,
     "Extension \"%s\" could not be loaded because its signature is either missing or invalid and unsigned extensions "
     "are disabled by configuration (allow_unsigned_extensions)"},
    {ErrorType::INVALIDATED_TRANSACTION, "Current transaction is aborted (please ROLLBACK)"},
    {ErrorType::INVALIDATED_DATABASE, "Failed: database has been invalidated because of a previous fatal error. The "
                                      "database must be restarted prior to being used again.\nOriginal error: \"%s\""},
    {ErrorType::INVALID, nullptr}};

string ErrorManager::FormatExceptionRecursive(ErrorType error_type, vector<ExceptionFormatValue> &values) {
	if (error_type >= ErrorType::ERROR_COUNT) {
		throw InternalException("Invalid error type passed to ErrorManager::FormatError");
	}
	auto entry = custom_errors.find(error_type);
	string error;
	if (entry == custom_errors.end()) {
		// error was not overwritten
		error = internal_errors[int(error_type)].error;
	} else {
		// error was overwritten
		error = entry->second;
	}
	return ExceptionFormatValue::Format(error, values);
}

string ErrorManager::InvalidUnicodeError(const string &input, const string &context) {
	UnicodeInvalidReason reason;
	size_t pos;
	auto unicode = Utf8Proc::Analyze(const_char_ptr_cast(input.c_str()), input.size(), &reason, &pos);
	if (unicode != UnicodeType::INVALID) {
		return "Invalid unicode error thrown but no invalid unicode detected in " + context;
	}
	string base_message;
	switch (reason) {
	case UnicodeInvalidReason::BYTE_MISMATCH:
		base_message = "Invalid unicode (byte sequence mismatch)";
		break;
	case UnicodeInvalidReason::INVALID_UNICODE:
		base_message = "Invalid unicode";
		break;
	default:
		break;
	}
	return base_message + " detected in " + context;
}

void ErrorManager::AddCustomError(ErrorType type, string new_error) {
	custom_errors.insert(make_pair(type, std::move(new_error)));
}

ErrorManager &ErrorManager::Get(ClientContext &context) {
	return *DBConfig::GetConfig(context).error_manager;
}

ErrorManager &ErrorManager::Get(DatabaseInstance &context) {
	return *DBConfig::GetConfig(context).error_manager;
}

} // namespace duckdb


namespace duckdb {

static ExtensionAlias internal_aliases[] = {{"http", "httpfs"}, // httpfs
                                            {"https", "httpfs"},
                                            {"md", "motherduck"}, // motherduck
                                            {"s3", "httpfs"},
                                            {"postgres", "postgres_scanner"}, // postgres
                                            {"sqlite", "sqlite_scanner"},     // sqlite
                                            {"sqlite3", "sqlite_scanner"},
                                            {nullptr, nullptr}};

idx_t ExtensionHelper::ExtensionAliasCount() {
	idx_t index;
	for (index = 0; internal_aliases[index].alias != nullptr; index++) {
	}
	return index;
}

ExtensionAlias ExtensionHelper::GetExtensionAlias(idx_t index) {
	D_ASSERT(index < ExtensionAliasCount());
	return internal_aliases[index];
}

string ExtensionHelper::ApplyExtensionAlias(string extension_name) {
	auto lname = StringUtil::Lower(extension_name);
	for (idx_t index = 0; internal_aliases[index].alias; index++) {
		if (lname == internal_aliases[index].alias) {
			return internal_aliases[index].extension;
		}
	}
	return extension_name;
}

} // namespace duckdb








// Note that c++ preprocessor doesn't have a nice way to clean this up so we need to set the defines we use to false
// explicitly when they are undefined
#ifndef DUCKDB_EXTENSION_ICU_LINKED
#define DUCKDB_EXTENSION_ICU_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_EXCEL_LINKED
#define DUCKDB_EXTENSION_EXCEL_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_PARQUET_LINKED
#define DUCKDB_EXTENSION_PARQUET_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_TPCH_LINKED
#define DUCKDB_EXTENSION_TPCH_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_TPCDS_LINKED
#define DUCKDB_EXTENSION_TPCDS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_FTS_LINKED
#define DUCKDB_EXTENSION_FTS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_HTTPFS_LINKED
#define DUCKDB_EXTENSION_HTTPFS_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_JSON_LINKED
#define DUCKDB_EXTENSION_JSON_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_JEMALLOC_LINKED
#define DUCKDB_EXTENSION_JEMALLOC_LINKED false
#endif

#ifndef DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
#define DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED false
#endif

// Load the generated header file containing our list of extension headers
#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS && !defined(DUCKDB_AMALGAMATION)

#else
// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds, after that
//		 these can be removed
#if DUCKDB_EXTENSION_ICU_LINKED
#include "icu_extension.hpp"
#endif

#if DUCKDB_EXTENSION_EXCEL_LINKED
#include "excel_extension.hpp"
#endif

#if DUCKDB_EXTENSION_PARQUET_LINKED
#include "parquet_extension.hpp"
#endif

#if DUCKDB_EXTENSION_TPCH_LINKED
#include "tpch_extension.hpp"
#endif

#if DUCKDB_EXTENSION_TPCDS_LINKED
#include "tpcds_extension.hpp"
#endif

#if DUCKDB_EXTENSION_FTS_LINKED
#include "fts_extension.hpp"
#endif

#if DUCKDB_EXTENSION_HTTPFS_LINKED
#include "httpfs_extension.hpp"
#endif

#if DUCKDB_EXTENSION_JSON_LINKED
#include "json_extension.hpp"
#endif

#if DUCKDB_EXTENSION_JEMALLOC_LINKED
#include "jemalloc_extension.hpp"
#endif

#if DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
#include "autocomplete_extension.hpp"
#endif
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Default Extensions
//===--------------------------------------------------------------------===//
static DefaultExtension internal_extensions[] = {
    {"icu", "Adds support for time zones and collations using the ICU library", DUCKDB_EXTENSION_ICU_LINKED},
    {"excel", "Adds support for Excel-like format strings", DUCKDB_EXTENSION_EXCEL_LINKED},
    {"parquet", "Adds support for reading and writing parquet files", DUCKDB_EXTENSION_PARQUET_LINKED},
    {"tpch", "Adds TPC-H data generation and query support", DUCKDB_EXTENSION_TPCH_LINKED},
    {"tpcds", "Adds TPC-DS data generation and query support", DUCKDB_EXTENSION_TPCDS_LINKED},
    {"fts", "Adds support for Full-Text Search Indexes", DUCKDB_EXTENSION_FTS_LINKED},
    {"httpfs", "Adds support for reading and writing files over a HTTP(S) connection", DUCKDB_EXTENSION_HTTPFS_LINKED},
    {"json", "Adds support for JSON operations", DUCKDB_EXTENSION_JSON_LINKED},
    {"jemalloc", "Overwrites system allocator with JEMalloc", DUCKDB_EXTENSION_JEMALLOC_LINKED},
    {"autocomplete", "Adds support for autocomplete in the shell", DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED},
    {"motherduck", "Enables motherduck integration with the system", false},
    {"sqlite_scanner", "Adds support for reading SQLite database files", false},
    {"postgres_scanner", "Adds support for reading from a Postgres database", false},
    {"inet", "Adds support for IP-related data types and functions", false},
    {"spatial", "Geospatial extension that adds support for working with spatial data and functions", false},
    {"substrait", "Adds support for the Substrait integration", false},
    {"aws", "Provides features that depend on the AWS SDK", false},
    {"arrow", "A zero-copy data integration between Apache Arrow and DuckDB", false},
    {"azure", "Adds a filesystem abstraction for Azure blob storage to DuckDB", false},
    {"iceberg", "Adds support for Apache Iceberg", false},
    {"visualizer", "Creates an HTML-based visualization of the query plan", false},
    {nullptr, nullptr, false}};

idx_t ExtensionHelper::DefaultExtensionCount() {
	idx_t index;
	for (index = 0; internal_extensions[index].name != nullptr; index++) {
	}
	return index;
}

DefaultExtension ExtensionHelper::GetDefaultExtension(idx_t index) {
	D_ASSERT(index < DefaultExtensionCount());
	return internal_extensions[index];
}

//===--------------------------------------------------------------------===//
// Allow Auto-Install Extensions
//===--------------------------------------------------------------------===//
static const char *auto_install[] = {"motherduck", "postgres_scanner", "sqlite_scanner", nullptr};

// TODO: unify with new autoload mechanism
bool ExtensionHelper::AllowAutoInstall(const string &extension) {
	auto lcase = StringUtil::Lower(extension);
	for (idx_t i = 0; auto_install[i]; i++) {
		if (lcase == auto_install[i]) {
			return true;
		}
	}
	return false;
}

bool ExtensionHelper::CanAutoloadExtension(const string &ext_name) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	return false;
#endif

	if (ext_name.empty()) {
		return false;
	}
	for (const auto &ext : AUTOLOADABLE_EXTENSIONS) {
		if (ext_name == ext) {
			return true;
		}
	}
	return false;
}

string ExtensionHelper::AddExtensionInstallHintToErrorMsg(ClientContext &context, const string &base_error,
                                                          const string &extension_name) {
	auto &dbconfig = DBConfig::GetConfig(context);
	string install_hint;

	if (!ExtensionHelper::CanAutoloadExtension(extension_name)) {
		install_hint = "Please try installing and loading the " + extension_name + " extension:\nINSTALL " +
		               extension_name + ";\nLOAD " + extension_name + ";\n\n";
	} else if (!dbconfig.options.autoload_known_extensions) {
		install_hint =
		    "Please try installing and loading the " + extension_name + " extension by running:\nINSTALL " +
		    extension_name + ";\nLOAD " + extension_name +
		    ";\n\nAlternatively, consider enabling auto-install "
		    "and auto-load by running:\nSET autoinstall_known_extensions=1;\nSET autoload_known_extensions=1;";
	} else if (!dbconfig.options.autoinstall_known_extensions) {
		install_hint =
		    "Please try installing the " + extension_name + " extension by running:\nINSTALL " + extension_name +
		    ";\n\nAlternatively, consider enabling autoinstall by running:\nSET autoinstall_known_extensions=1;";
	}

	if (!install_hint.empty()) {
		return base_error + "\n\n" + install_hint;
	}

	return base_error;
}

bool ExtensionHelper::TryAutoLoadExtension(ClientContext &context, const string &extension_name) noexcept {
	auto &dbconfig = DBConfig::GetConfig(context);
	try {
		if (dbconfig.options.autoinstall_known_extensions) {
			ExtensionHelper::InstallExtension(context, extension_name, false,
			                                  context.config.autoinstall_extension_repo);
		}
		ExtensionHelper::LoadExternalExtension(context, extension_name);
		return true;
	} catch (...) {
		return false;
	}
	return false;
}

void ExtensionHelper::AutoLoadExtension(ClientContext &context, const string &extension_name) {
	auto &dbconfig = DBConfig::GetConfig(context);
	try {
#ifndef DUCKDB_WASM
		if (dbconfig.options.autoinstall_known_extensions) {
			ExtensionHelper::InstallExtension(context, extension_name, false,
			                                  context.config.autoinstall_extension_repo);
		}
#endif
		ExtensionHelper::LoadExternalExtension(context, extension_name);
	} catch (Exception &e) {
		throw AutoloadException(extension_name, e);
	}
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	// The in-tree extensions that we check. Non-cmake builds are currently limited to these for static linking
	// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds, after that
	//		 these can be removed
	unordered_set<string> extensions {"parquet", "icu",   "tpch",     "tpcds", "fts",      "httpfs",      "visualizer",
	                                  "json",    "excel", "sqlsmith", "inet",  "jemalloc", "autocomplete"};
	for (auto &ext : extensions) {
		LoadExtensionInternal(db, ext, true);
	}

#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS
	for (auto &ext : linked_extensions) {
		LoadExtensionInternal(db, ext, true);
	}
#endif
}

ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
	return LoadExtensionInternal(db, extension, false);
}

ExtensionLoadResult ExtensionHelper::LoadExtensionInternal(DuckDB &db, const std::string &extension,
                                                           bool initial_load) {
#ifdef DUCKDB_TEST_REMOTE_INSTALL
	if (!initial_load && StringUtil::Contains(DUCKDB_TEST_REMOTE_INSTALL, extension)) {
		Connection con(db);
		auto result = con.Query("INSTALL " + extension);
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		result = con.Query("LOAD " + extension);
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif

#ifdef DUCKDB_EXTENSIONS_TEST_WITH_LOADABLE
	// Note: weird comma's are on purpose to do easy string contains on a list of extension names
	if (!initial_load && StringUtil::Contains(DUCKDB_EXTENSIONS_TEST_WITH_LOADABLE, "," + extension + ",")) {
		Connection con(db);
		auto result = con.Query((string) "LOAD '" + DUCKDB_EXTENSIONS_BUILD_PATH + "/" + extension + "/" + extension +
		                        ".duckdb_extension'");
		if (result->HasError()) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif

	// This is the main extension loading mechanism that loads the extension that are statically linked.
#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS
	if (TryLoadLinkedExtension(db, extension)) {
		return ExtensionLoadResult::LOADED_EXTENSION;
	} else {
		return ExtensionLoadResult::NOT_LOADED;
	}
#endif

	// This is the fallback to the "old" extension loading mechanism for non-cmake builds
	// TODO: rewrite package_build.py to allow also loading out-of-tree extensions in non-cmake builds
	if (extension == "parquet") {
#if DUCKDB_EXTENSION_PARQUET_LINKED
		db.LoadExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#if DUCKDB_EXTENSION_ICU_LINKED
		db.LoadExtension<IcuExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#if DUCKDB_EXTENSION_TPCH_LINKED
		db.LoadExtension<TpchExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#if DUCKDB_EXTENSION_TPCDS_LINKED
		db.LoadExtension<TpcdsExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#if DUCKDB_EXTENSION_FTS_LINKED
//		db.LoadExtension<FtsExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#if DUCKDB_EXTENSION_HTTPFS_LINKED
		db.LoadExtension<HttpfsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "visualizer") {
#if DUCKDB_EXTENSION_VISUALIZER_LINKED
		db.LoadExtension<VisualizerExtension>();
#else
		// visualizer extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "json") {
#if DUCKDB_EXTENSION_JSON_LINKED
		db.LoadExtension<JsonExtension>();
#else
		// json extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "excel") {
#if DUCKDB_EXTENSION_EXCEL_LINKED
		db.LoadExtension<ExcelExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "sqlsmith") {
#if DUCKDB_EXTENSION_SQLSMITH_LINKED
		db.LoadExtension<SqlsmithExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "jemalloc") {
#if DUCKDB_EXTENSION_JEMALLOC_LINKED
		db.LoadExtension<JemallocExtension>();
#else
		// jemalloc extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "autocomplete") {
#if DUCKDB_EXTENSION_AUTOCOMPLETE_LINKED
		db.LoadExtension<AutocompleteExtension>();
#else
		// autocomplete extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "inet") {
#if DUCKDB_EXTENSION_INET_LINKED
		db.LoadExtension<InetExtension>();
#else
		// inet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	}

	return ExtensionLoadResult::LOADED_EXTENSION;
}

static vector<std::string> public_keys = {
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6aZuHUa1cLR9YDDYaEfi
UDbWY8m2t7b71S+k1ZkXfHqu+5drAxm+dIDzdOHOKZSIdwnJbT3sSqwFoG6PlXF3
g3dsJjax5qESIhbVvf98nyipwNINxoyHCkcCIPkX17QP2xpnT7V59+CqcfDJXLqB
ymjqoFSlaH8dUCHybM4OXlWnAtVHW/nmw0khF8CetcWn4LxaTUHptByaBz8CasSs
gWpXgSfaHc3R9eArsYhtsVFGyL/DEWgkEHWolxY3Llenhgm/zOf3s7PsAMe7EJX4
qlSgiXE6OVBXnqd85z4k20lCw/LAOe5hoTMmRWXIj74MudWe2U91J6GrrGEZa7zT
7QIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq8Gg1S/LI6ApMAYsFc9m
PrkFIY+nc0LXSpxm77twU8D5M0Xkz/Av4f88DQmj1OE3164bEtR7sl7xDPZojFHj
YYyucJxEI97l5OU1d3Pc1BdKXL4+mnW5FlUGj218u8qD+G1hrkySXQkrUzIjPPNw
o6knF3G/xqQF+KI+tc7ajnTni8CAlnUSxfnstycqbVS86m238PLASVPK9/SmIRgO
XCEV+ZNMlerq8EwsW4cJPHH0oNVMcaG+QT4z79roW1rbJghn9ubAVdQU6VLUAikI
b8keUyY+D0XdY9DpDBeiorb1qPYt8BPLOAQrIUAw1CgpMM9KFp9TNvW47KcG4bcB
dQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyYATA9KOQ0Azf97QAPfY
Jc/WeZyE4E1qlRgKWKqNtYSXZqk5At0V7w2ntAWtYSpczFrVepCJ0oPMDpZTigEr
NgOgfo5LEhPx5XmtCf62xY/xL3kgtfz9Mm5TBkuQy4KwY4z1npGr4NYYDXtF7kkf
LQE+FnD8Yr4E0wHBib7ey7aeeKWmwqvUjzDqG+TzaqwzO/RCUsSctqSS0t1oo2hv
4q1ofanUXsV8MXk/ujtgxu7WkVvfiSpK1zRazgeZjcrQFO9qL/pla0vBUxa1U8He
GMLnL0oRfcMg7yKrbIMrvlEl2ZmiR9im44dXJWfY42quObwr1PuEkEoCMcMisSWl
jwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4RvbWx3zLblDHH/lGUF5
Q512MT+v3YPriuibROMllv8WiCLAMeJ0QXbVaIzBOeHDeLx8yvoZZN+TENKxtT6u
IfMMneUzxHBqy0AQNfIsSsOnG5nqoeE/AwbS6VqCdH1aLfoCoPffacHYa0XvTcsi
aVlZfr+UzJS+ty8pRmFVi1UKSOADDdK8XfIovJl/zMP2TxYX2Y3fnjeLtl8Sqs2e
P+eHDoy7Wi4EPTyY7tNTCfxwKNHn1HQ5yrv5dgvMxFWIWXGz24yikFvtwLGHe8uJ
Wi+fBX+0PF0diZ6pIthZ149VU8qCqYAXjgpxZ0EZdrsiF6Ewz0cfg20SYApFcmW4
pwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyhd5AfwrUohG3O4DE0K9
O3FmgB7zE4aDnkL8UUfGCh5kdP8q7ewMjekY+c6LwWOmpdJpSwqhfV1q5ZU1l6rk
3hlt03LO3sgs28kcfOVH15hqfxts6Sg5KcRjxStE50ORmXGwXDcS9vqkJ60J1EHA
lcZqbCRSO73ZPLhdepfd0/C6tM0L7Ge6cAE62/MTmYNGv8fDzwQr/kYIJMdoS8Zp
thRpctFZJtPs3b0fffZA/TCLVKMvEVgTWs48751qKid7N/Lm/iEGx/tOf4o23Nec
Pz1IQaGLP+UOLVQbqQBHJWNOqigm7kWhDgs3N4YagWgxPEQ0WVLtFji/ZjlKZc7h
dwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnFDg3LhyV6BVE2Z3zQvN
6urrKvPhygTa5+wIPGwYTzJ8DfGALqlsX3VOXMvcJTca6SbuwwkoXHuSU5wQxfcs
bt4jTXD3NIoRwQPl+D9IbgIMuX0ACl27rJmr/f9zkY7qui4k1X82pQkxBe+/qJ4r
TBwVNONVx1fekTMnSCEhwg5yU3TNbkObu0qlQeJfuMWLDQbW/8v/qfr/Nz0JqHDN
yYKfKvFMlORxyJYiOyeOsbzNGEhkGQGOmKhRUhS35kD+oA0jqwPwMCM9O4kFg/L8
iZbpBBX2By1K3msejWMRAewTOyPas6YMQOYq9BMmWQqzVtG5xcaSJwN/YnMpJyqb
sQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1z0RU8vGrfEkrscEoZKA
GiOcGh2EMcKwjQpl4nKuR9H4o/dg+CZregVSHg7MP2f8mhLZZyoFev49oWOV4Rmi
qs99UNxm7DyKW1fF1ovowsUW5lsDoKYLvpuzHo0s4laiV4AnIYP7tHGLdzsnK2Os
Cp5dSuMwKHPZ9N25hXxFB/dRrAdIiXHvbSqr4N29XzfQloQpL3bGHLKY6guFHluH
X5dJ9eirVakWWou7BR2rnD0k9vER6oRdVnJ6YKb5uhWEOQ3NmV961oyr+uiDTcep
qqtGHWuFhENixtiWGjFJJcACwqxEAW3bz9lyrfnPDsHSW/rlQVDIAkik+fOp+R7L
kQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxwO27e1vnbNcpiDg7Wwx
K/w5aEGukXotu3529ieq+O39H0+Bak4vIbzGhDUh3/ElmxaFMAs4PYrWe/hc2WFD
H4JCOoFIn4y9gQeE855DGGFgeIVd1BnSs5S+5wUEMxLNyHdHSmINN6FsoZ535iUg
KdYjRh1iZevezg7ln8o/O36uthu925ehFBXSy6jLJgQlwmq0KxZJE0OAZhuDBM60
MtIunNa/e5y+Gw3GknFwtRLmn/nEckZx1nEtepYvvUa7UGy+8KuGuhOerCZTutbG
k8liCVgGenRve8unA2LrBbpL+AUf3CrZU/uAxxTqWmw6Z/S6TeW5ozeeyOCh8ii6
TwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsGIFOfIQ4RI5qu4klOxf
ge6eXwBMAkuTXyhyIIJDtE8CurnwQvUXVlt+Kf0SfuIFW6MY5ErcWE/vMFbc81IR
9wByOAAV2CTyiLGZT63uE8pN6FSHd6yGYCLjXd3P3cnP3Qj5pBncpLuAUDfHG4wP
bs9jIADw3HysD+eCNja8p7ZC7CzWxTcO7HsEu9deAAU19YywdpagXvQ0pJ9zV5qU
jrHxBygl31t6TmmX+3d+azjGu9Hu36E+5wcSOOhuwAFXDejb40Ixv53ItJ3fZzzH
PF2nj9sQvQ8c5ptjyOvQCBRdqkEWXIVHClxqWb+o59pDIh1G0UGcmiDN7K9Gz5HA
ZQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAt9uUnlW/CoUXT68yaZh9
SeXHzGRCPNEI98Tara+dgYxDX1z7nfOh8o15liT0QsAzx34EewZOxcKCNiV/dZX5
z4clCkD8uUbZut6IVx8Eu+7Qcd5jZthRc6hQrN9Ltv7ZQEh7KGXOHa53kT2K01ws
4jbVmd/7Nx7y0Yyqhja01pIu/CUaTkODfQxBXwriLdIzp7y/iJeF/TLqCwZWHKQx
QOZnsPEveB1F00Va9MeAtTlXFUJ/TQXquqTjeLj4HuIRtbyuNgWoc0JyF+mcafAl
bnrNEBIfxZhAT81aUCIAzRJp6AqfdeZxnZ/WwohtZQZLXAxFQPTWCcP+Z9M7OIQL
WwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA56NhfACkeCyZM07l2wmd
iTp24E2tLLKU3iByKlIRWRAvXsOejRMJTHTNHWa3cQ7uLP++Tf2St7ksNsyPMNZy
9QRTLNCYr9rN9loLwdb2sMWxFBwwzCaAOTahGI7GJQy30UB7FEND0X/5U2rZvQij
Q6K+O4aa+K9M5qyOHNMmXywmTnAgWKNaNxQHPRtD2+dSj60T6zXdtIuCrPfcNGg5
gj07qWGEXX83V/L7nSqCiIVYg/wqds1x52Yjk1nhXYNBTqlnhmOd8LynGxz/sXC7
h2Q9XsHjXIChW4FHyLIOl6b4zPMBSxzCigYm3QZJWfAkZv5PBRtnq7vhYOLHzLQj
CwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmfPLe0IWGYC0MZC6YiM3
QGfhT6zSKB0I2DW44nlBlWUcF+32jW2bFJtgE76qGGKFeU4kJBWYr99ufHoAodNg
M1Ehl/JfQ5KmbC1WIqnFTrgbmqJde79jeCvCpbFLuqnzidwO1PbXDbfRFQcgWaXT
mDVLNNVmLxA0GkCv+kydE2gtcOD9BDceg7F/56TDvclyI5QqAnjE2XIRMPZlXQP4
oF2kgz4Cn7LxLHYmkU2sS9NYLzHoyUqFplWlxkQjA4eQ0neutV1Ydmc1IX8W7R38
A7nFtaT8iI8w6Vkv7ijYN6xf5cVBPKZ3Dv7AdwPet86JD5mf5v+r7iwg5xl3r77Z
iwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoB1kWsX8YmCcFOD9ilBY
xK076HmUAN026uJ8JpmU9Hz+QT1FNXOsnj1h2G6U6btYVIdHUTHy/BvAumrDKqRz
qcEAzCuhxUjPjss54a/Zqu6nQcoIPHuG/Er39oZHIVkPR1WCvWj8wmyYv6T//dPH
unO6tW29sXXxS+J1Gah6vpbtJw1pI/liah1DZzb13KWPDI6ZzviTNnW4S05r6js/
30He+Yud6aywrdaP/7G90qcrteEFcjFy4Xf+5vG960oKoGoDplwX5poay1oCP9tb
g8AC8VSRAGi3oviTeSWZcrLXS8AtJhGvF48cXQj2q+8YeVKVDpH6fPQxJ9Sh9aeU
awIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA4NTMAIYIlCMID00ufy/I
AZXc8pocDx9N1Q5x5/cL3aIpLmx02AKo9BvTJaJuHiTjlwYhPtlhIrHV4HUVTkOX
sISp8B8v9i2I1RIvCTAcvy3gcH6rdRWZ0cdTUiMEqnnxBX9zdzl8oMzZcyauv19D
BeqJvzflIT96b8g8K3mvgJHs9a1j9f0gN8FuTA0c52DouKnrh8UwH7mlrumYerJw
6goJGQuK1HEOt6bcQuvogkbgJWOoEYwjNrPwQvIcP4wyrgSnOHg1yXOFE84oVynJ
czQEOz9ke42I3h8wrnQxilEYBVo2uX8MenqTyfGnE32lPRt3Wv1iEVQls8Cxiuy2
CQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3bUtfp66OtRyvIF/oucn
id8mo7gvbNEH04QMLO3Ok43dlWgWI3hekJAqOYc0mvoI5anqr98h8FI7aCYZm/bY
vpz0I1aXBaEPh3aWh8f/w9HME7ykBvmhMe3J+VFGWWL4eswfRl//GCtnSMBzDFhM
SaQOTvADWHkC0njeI5yXjf/lNm6fMACP1cnhuvCtnx7VP/DAtvUk9usDKG56MJnZ
UoVM3HHjbJeRwxCdlSWe12ilCdwMRKSDY92Hk38/zBLenH04C3HRQLjBGewACUmx
uvNInehZ4kSYFGa+7UxBxFtzJhlKzGR73qUjpWzZivCe1K0WfRVP5IWsKNCCESJ/
nQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyV2dE/CRUAUE8ybq/DoS
Lc7QlYXh04K+McbhN724TbHahLTuDk5mR5TAunA8Nea4euRzknKdMFAz1eh9gyy3
5x4UfXQW1fIZqNo6WNrGxYJgWAXU+pov+OvxsMQWzqS4jrTHDHbblCCLKp1akwJk
aFNyqgjAL373PcqXC+XAn8vHx4xHFoFP5lq4lLcJCOW5ee9v9El3w0USLwS+t1cF
RY3kuV6Njlr4zsRH9iM6/zaSuCALYWJ/JrPEurSJXzFZnWsvn6aQdeNeAn08+z0F
k2NwaauEo0xmLqzqTRGzjHqKKmeefN3/+M/FN2FrApDlxWQfhD2Y3USdAiN547Nj
1wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvm2+kTrEQWZXuxhWzBdl
PCbQGqbrukbeS6JKSlQLJDC8ayZIxFxatqg1Q8UPyv89MVRsHOGlG1OqFaOEtPjQ
Oo6j/moFwB4GPyJhJHOGpCKa4CLB5clhfDCLJw6ty7PcDU3T6yW4X4Qc5k4LRRWy
yzC8lVHfBdarN+1iEe0ALMOGoeiJjVn6i/AFxktRwgd8njqv/oWQyfjJZXkNMsb6
7ZDxNVAUrp/WXpE4Kq694bB9xa/pWsqv7FjQJUgTnEzvbN+qXnVPtA7dHcOYYJ8Z
SbrJUfHrf8TS5B54AiopFpWG+hIbjqqdigqabBqFpmjiRDZgDy4zJJj52xJZMnrp
rwIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwEAcVmY3589O02pLA22f
MlarLyJUgy0BeJDG5AUsi17ct8sHZzRiv9zKQVCBk1CtZY//jyqnrM7iCBLWsyby
TiTOtGYHHApaLnNjjtaHdQ6zplhbc3g2XLy+4ab8GNKG3zc8iXpsQM6r+JO5n9pm
V9vollz9dkFxS9l+1P17lZdIgCh9O3EIFJv5QCd5c9l2ezHAan2OhkWhiDtldnH/
MfRXbz7X5sqlwWLa/jhPtvY45x7dZaCHGqNzbupQZs0vHnAVdDu3vAWDmT/3sXHG
vmGxswKA9tPU0prSvQWLz4LUCnGi/cC5R+fiu+fovFM/BwvaGtqBFIF/1oWVq7bZ
4wIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA25qGwNO1+qHygC8mjm8L
3I66mV/IzslgBDHC91mE8YcI5Fq0sdrtsbUhK3z89wIN/zOhbHX0NEiXm2GxUnsI
vb5tDZXAh7AbTnXTMVbxO/e/8sPLUiObGjDvjVzyzrxOeG87yK/oIiilwk9wTsIb
wMn2Grj4ht9gVKx3oGHYV7STNdWBlzSaJj4Ou7+5M1InjPDRFZG1K31D2d3IHByX
lmcRPZtPFTa5C1uVJw00fI4F4uEFlPclZQlR5yA0G9v+0uDgLcjIUB4eqwMthUWc
dHhlmrPp04LI19eksWHCtG30RzmUaxDiIC7J2Ut0zHDqUe7aXn8tOVI7dE9tTKQD
KQIDAQAB
-----END PUBLIC KEY-----
)",
    R"(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA7EC2bx7aRnf3TcRg5gmw
QOKNCUheCelK8hoXLMsKSJqmufyJ+IHUejpXGOpvyYRbACiJ5GiNcww20MVpTBU7
YESWB2QSU2eEJJXMq84qsZSO8WGmAuKpUckI+hNHKQYJBEDOougV6/vVVEm5c5bc
SLWQo0+/ciQ21Zwz5SwimX8ep1YpqYirO04gcyGZzAfGboXRvdUwA+1bZvuUXdKC
4zsCw2QALlcVpzPwjB5mqA/3a+SPgdLAiLOwWXFDRMnQw44UjsnPJFoXgEZiUpZm
EMS5gLv50CzQqJXK9mNzPuYXNUIc4Pw4ssVWe0OfN3Od90gl5uFUwk/G9lWSYnBN
3wIDAQAB
-----END PUBLIC KEY-----
)"};

const vector<string> ExtensionHelper::GetPublicKeys() {
	return public_keys;
}

} // namespace duckdb





#ifndef DISABLE_DUCKDB_REMOTE_INSTALL
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD

#endif
#endif


#include <fstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Install Extension
//===--------------------------------------------------------------------===//
const string ExtensionHelper::NormalizeVersionTag(const string &version_tag) {
	if (version_tag.length() > 0 && version_tag[0] != 'v') {
		return "v" + version_tag;
	}
	return version_tag;
}

bool ExtensionHelper::IsRelease(const string &version_tag) {
	return !StringUtil::Contains(version_tag, "-dev");
}

const string ExtensionHelper::GetVersionDirectoryName() {
#ifdef DUCKDB_WASM_VERSION
	return DUCKDB_QUOTE_DEFINE(DUCKDB_WASM_VERSION);
#endif
	if (IsRelease(DuckDB::LibraryVersion())) {
		return NormalizeVersionTag(DuckDB::LibraryVersion());
	} else {
		return DuckDB::SourceID();
	}
}

const vector<string> ExtensionHelper::PathComponents() {
	return vector<string> {".duckdb", "extensions", GetVersionDirectoryName(), DuckDB::Platform()};
}

string ExtensionHelper::ExtensionDirectory(DBConfig &config, FileSystem &fs) {
#ifdef WASM_LOADABLE_EXTENSIONS
	throw PermissionException("ExtensionDirectory functionality is not supported in duckdb-wasm");
#endif
	string extension_directory;
	if (!config.options.extension_directory.empty()) { // create the extension directory if not present
		extension_directory = config.options.extension_directory;
		// TODO this should probably live in the FileSystem
		// convert random separators to platform-canonic
		extension_directory = fs.ConvertSeparators(extension_directory);
		// expand ~ in extension directory
		extension_directory = fs.ExpandPath(extension_directory);
		if (!fs.DirectoryExists(extension_directory)) {
			auto sep = fs.PathSeparator(extension_directory);
			auto splits = StringUtil::Split(extension_directory, sep);
			D_ASSERT(!splits.empty());
			string extension_directory_prefix;
			if (StringUtil::StartsWith(extension_directory, sep)) {
				extension_directory_prefix = sep; // this is swallowed by Split otherwise
			}
			for (auto &split : splits) {
				extension_directory_prefix = extension_directory_prefix + split + sep;
				if (!fs.DirectoryExists(extension_directory_prefix)) {
					fs.CreateDirectory(extension_directory_prefix);
				}
			}
		}
	} else { // otherwise default to home
		string home_directory = fs.GetHomeDirectory();
		// exception if the home directory does not exist, don't create whatever we think is home
		if (!fs.DirectoryExists(home_directory)) {
			throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
			                  "home_directory='/path/to/dir' option.",
			                  home_directory);
		}
		extension_directory = home_directory;
	}
	D_ASSERT(fs.DirectoryExists(extension_directory));

	auto path_components = PathComponents();
	for (auto &path_ele : path_components) {
		extension_directory = fs.JoinPath(extension_directory, path_ele);
		if (!fs.DirectoryExists(extension_directory)) {
			fs.CreateDirectory(extension_directory);
		}
	}
	return extension_directory;
}

string ExtensionHelper::ExtensionDirectory(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	return ExtensionDirectory(config, fs);
}

bool ExtensionHelper::CreateSuggestions(const string &extension_name, string &message) {
	vector<string> candidates;
	for (idx_t ext_count = ExtensionHelper::DefaultExtensionCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetDefaultExtension(i).name);
	}
	for (idx_t ext_count = ExtensionHelper::ExtensionAliasCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetExtensionAlias(i).alias);
	}
	auto closest_extensions = StringUtil::TopNLevenshtein(candidates, extension_name);
	message = StringUtil::CandidatesMessage(closest_extensions, "Candidate extensions");
	for (auto &closest : closest_extensions) {
		if (closest == extension_name) {
			message = "Extension \"" + extension_name + "\" is an existing extension.\n";
			return true;
		}
	}
	return false;
}

void ExtensionHelper::InstallExtension(DBConfig &config, FileSystem &fs, const string &extension, bool force_install,
                                       const string &repository) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	string local_path = ExtensionDirectory(config, fs);
	InstallExtensionInternal(config, nullptr, fs, local_path, extension, force_install, repository);
}

void ExtensionHelper::InstallExtension(ClientContext &context, const string &extension, bool force_install,
                                       const string &repository) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	auto &config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	string local_path = ExtensionDirectory(context);
	auto &client_config = ClientConfig::GetConfig(context);
	InstallExtensionInternal(config, &client_config, fs, local_path, extension, force_install, repository);
}

unsafe_unique_array<data_t> ReadExtensionFileFromDisk(FileSystem &fs, const string &path, idx_t &file_size) {
	auto source_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	file_size = source_file->GetFileSize();
	auto in_buffer = make_unsafe_uniq_array<data_t>(file_size);
	source_file->Read(in_buffer.get(), file_size);
	source_file->Close();
	return in_buffer;
}

void WriteExtensionFileToDisk(FileSystem &fs, const string &path, void *data, idx_t data_size) {
	auto target_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND |
	                                         FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	target_file->Write(data, data_size);
	target_file->Close();
	target_file.reset();
}

string ExtensionHelper::ExtensionUrlTemplate(optional_ptr<const ClientConfig> client_config, const string &repository) {
	string versioned_path = "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
#ifdef WASM_LOADABLE_EXTENSIONS
	string default_endpoint = "https://extensions.duckdb.org";
	versioned_path = "/duckdb-wasm" + versioned_path + ".wasm";
#else
	string default_endpoint = "http://extensions.duckdb.org";
	versioned_path = versioned_path + ".gz";
#endif
	string custom_endpoint = client_config ? client_config->custom_extension_repo : string();
	string endpoint;
	if (!repository.empty()) {
		endpoint = repository;
	} else if (!custom_endpoint.empty()) {
		endpoint = custom_endpoint;
	} else {
		endpoint = default_endpoint;
	}
	string url_template = endpoint + versioned_path;
	return url_template;
}

string ExtensionHelper::ExtensionFinalizeUrlTemplate(const string &url_template, const string &extension_name) {
	auto url = StringUtil::Replace(url_template, "${REVISION}", GetVersionDirectoryName());
	url = StringUtil::Replace(url, "${PLATFORM}", DuckDB::Platform());
	url = StringUtil::Replace(url, "${NAME}", extension_name);
	return url;
}

void ExtensionHelper::InstallExtensionInternal(DBConfig &config, ClientConfig *client_config, FileSystem &fs,
                                               const string &local_path, const string &extension, bool force_install,
                                               const string &repository) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Installing external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
		throw PermissionException("Installing extensions is disabled through configuration");
	}
	auto extension_name = ApplyExtensionAlias(fs.ExtractBaseName(extension));

	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	if (fs.FileExists(local_extension_path) && !force_install) {
		return;
	}

	auto uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string temp_path = local_extension_path + ".tmp-" + uuid;
	if (fs.FileExists(temp_path)) {
		fs.RemoveFile(temp_path);
	}
	auto is_http_url = StringUtil::Contains(extension, "http://");
	if (fs.FileExists(extension)) {
		idx_t file_size;
		auto in_buffer = ReadExtensionFileFromDisk(fs, extension, file_size);
		WriteExtensionFileToDisk(fs, temp_path, in_buffer.get(), file_size);

		if (fs.FileExists(local_extension_path) && force_install) {
			fs.RemoveFile(local_extension_path);
		}
		fs.MoveFile(temp_path, local_extension_path);
		return;
	} else if (StringUtil::Contains(extension, "/") && !is_http_url) {
		throw IOException("Failed to read extension from \"%s\": no such file", extension);
	}

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else

	string url_template = ExtensionUrlTemplate(client_config, repository);

	if (is_http_url) {
		url_template = extension;
		extension_name = "";
	}

	string url = ExtensionFinalizeUrlTemplate(url_template, extension_name);

	string no_http = StringUtil::Replace(url, "http://", "");

	idx_t next = no_http.find('/', 0);
	if (next == string::npos) {
		throw IOException("No slash in URL template");
	}

	// Special case to install extension from a local file, useful for testing
	if (!StringUtil::Contains(url_template, "http://")) {
		string file = fs.ConvertSeparators(url);
		if (!fs.FileExists(file)) {
			// check for non-gzipped variant
			file = file.substr(0, file.size() - 3);
			if (!fs.FileExists(file)) {
				throw IOException("Failed to copy local extension \"%s\" at PATH \"%s\"\n", extension_name, file);
			}
		}
		auto read_handle = fs.OpenFile(file, FileFlags::FILE_FLAGS_READ);
		auto test_data = std::unique_ptr<unsigned char[]> {new unsigned char[read_handle->GetFileSize()]};
		read_handle->Read(test_data.get(), read_handle->GetFileSize());
		WriteExtensionFileToDisk(fs, temp_path, (void *)test_data.get(), read_handle->GetFileSize());

		if (fs.FileExists(local_extension_path) && force_install) {
			fs.RemoveFile(local_extension_path);
		}
		fs.MoveFile(temp_path, local_extension_path);
		return;
	}

	// Push the substring [last, next) on to splits
	auto hostname_without_http = no_http.substr(0, next);
	auto url_local_part = no_http.substr(next);

	auto url_base = "http://" + hostname_without_http;
	duckdb_httplib::Client cli(url_base.c_str());

	duckdb_httplib::Headers headers = {{"User-Agent", StringUtil::Format("DuckDB %s %s %s", DuckDB::LibraryVersion(),
	                                                                     DuckDB::SourceID(), DuckDB::Platform())}};

	auto res = cli.Get(url_local_part.c_str(), headers);

	if (!res || res->status != 200) {
		// create suggestions
		string message;
		auto exact_match = ExtensionHelper::CreateSuggestions(extension_name, message);
		if (exact_match) {
			message += "\nAre you using a development build? In this case, extensions might not (yet) be uploaded.";
		}
		if (res.error() == duckdb_httplib::Error::Success) {
			throw HTTPException(res.value(), "Failed to download extension \"%s\" at URL \"%s%s\"\n%s", extension_name,
			                    url_base, url_local_part, message);
		} else {
			throw IOException("Failed to download extension \"%s\" at URL \"%s%s\"\n%s (ERROR %s)", extension_name,
			                  url_base, url_local_part, message, to_string(res.error()));
		}
	}
	auto decompressed_body = GZipFileSystem::UncompressGZIPString(res->body);

	WriteExtensionFileToDisk(fs, temp_path, (void *)decompressed_body.data(), decompressed_body.size());

	if (fs.FileExists(local_extension_path) && force_install) {
		fs.RemoveFile(local_extension_path);
	}
	fs.MoveFile(temp_path, local_extension_path);
#endif
#endif
}

} // namespace duckdb






#ifndef DUCKDB_NO_THREADS
#include <thread>
#endif // DUCKDB_NO_THREADS

#ifdef WASM_LOADABLE_EXTENSIONS
#include <emscripten.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Load External Extension
//===--------------------------------------------------------------------===//
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
typedef void (*ext_init_fun_t)(DatabaseInstance &);
typedef const char *(*ext_version_fun_t)(void);
typedef bool (*ext_is_storage_t)(void);

template <class T>
static T LoadFunctionFromDLL(void *dll, const string &function_name, const string &filename) {
	auto function = dlsym(dll, function_name.c_str());
	if (!function) {
		throw IOException("File \"%s\" did not contain function \"%s\": %s", filename, function_name, GetDLError());
	}
	return (T)function;
}

static void ComputeSHA256String(const std::string &to_hash, std::string *res) {
	// Invoke MbedTls function to actually compute sha256
	*res = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(to_hash);
}

static void ComputeSHA256FileSegment(FileHandle *handle, const idx_t start, const idx_t end, std::string *res) {
	idx_t iter = start;
	const idx_t segment_size = 1024 * 8;

	duckdb_mbedtls::MbedTlsWrapper::SHA256State state;

	std::string to_hash;
	while (iter < end) {
		idx_t len = std::min(end - iter, segment_size);
		to_hash.resize(len);
		handle->Read((void *)to_hash.data(), len, iter);

		state.AddString(to_hash);

		iter += segment_size;
	}

	*res = state.Finalize();
}
#endif

bool ExtensionHelper::TryInitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
                                     ExtensionInitResult &result, string &error,
                                     optional_ptr<const ClientConfig> client_config) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
		throw PermissionException("Loading external extensions is disabled through configuration");
	}
	auto filename = fs.ConvertSeparators(extension);

	// shorthand case
	if (!ExtensionHelper::IsFullPath(extension)) {
		string extension_name = ApplyExtensionAlias(extension);
#ifdef WASM_LOADABLE_EXTENSIONS
		string url_template = ExtensionUrlTemplate(client_config, "");
		string url = ExtensionFinalizeUrlTemplate(url_template, extension_name);

		char *str = (char *)EM_ASM_PTR(
		    {
			    var jsString = ((typeof runtime == 'object') && runtime && (typeof runtime.whereToLoad == 'function') &&
			                    runtime.whereToLoad)
			                       ? runtime.whereToLoad(UTF8ToString($0))
			                       : (UTF8ToString($1));
			    var lengthBytes = lengthBytesUTF8(jsString) + 1;
			    // 'jsString.length' would return the length of the string as UTF-16
			    // units, but Emscripten C strings operate as UTF-8.
			    var stringOnWasmHeap = _malloc(lengthBytes);
			    stringToUTF8(jsString, stringOnWasmHeap, lengthBytes);
			    return stringOnWasmHeap;
		    },
		    filename.c_str(), url.c_str());
		std::string address(str);
		free(str);

		filename = address;
#else

		string local_path =
		    !config.options.extension_directory.empty() ? config.options.extension_directory : fs.GetHomeDirectory();

		// convert random separators to platform-canonic
		local_path = fs.ConvertSeparators(local_path);
		// expand ~ in extension directory
		local_path = fs.ExpandPath(local_path);
		auto path_components = PathComponents();
		for (auto &path_ele : path_components) {
			local_path = fs.JoinPath(local_path, path_ele);
		}
		filename = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
#endif
	}
	if (!fs.FileExists(filename)) {
		string message;
		bool exact_match = ExtensionHelper::CreateSuggestions(extension, message);
		if (exact_match) {
			message += "\nInstall it first using \"INSTALL " + extension + "\".";
		}
		error = StringUtil::Format("Extension \"%s\" not found.\n%s", filename, message);
		return false;
	}
	if (!config.options.allow_unsigned_extensions) {
		auto handle = fs.OpenFile(filename, FileFlags::FILE_FLAGS_READ);

		// signature is the last 256 bytes of the file

		string signature;
		signature.resize(256);

		auto signature_offset = handle->GetFileSize() - signature.size();

		const idx_t maxLenChunks = 1024ULL * 1024ULL;
		const idx_t numChunks = (signature_offset + maxLenChunks - 1) / maxLenChunks;
		std::vector<std::string> hash_chunks(numChunks);
		std::vector<idx_t> splits(numChunks + 1);

		for (idx_t i = 0; i < numChunks; i++) {
			splits[i] = maxLenChunks * i;
		}
		splits.back() = signature_offset;

#ifndef DUCKDB_NO_THREADS
		std::vector<std::thread> threads;
		threads.reserve(numChunks);
		for (idx_t i = 0; i < numChunks; i++) {
			threads.emplace_back(ComputeSHA256FileSegment, handle.get(), splits[i], splits[i + 1], &hash_chunks[i]);
		}

		for (auto &thread : threads) {
			thread.join();
		}
#else
		for (idx_t i = 0; i < numChunks; i++) {
			ComputeSHA256FileSegment(handle.get(), splits[i], splits[i + 1], &hash_chunks[i]);
		}
#endif // DUCKDB_NO_THREADS

		string hash_concatenation;
		hash_concatenation.reserve(32 * numChunks); // 256 bits -> 32 bytes per chunk

		for (auto &hash_chunk : hash_chunks) {
			hash_concatenation += hash_chunk;
		}

		string two_level_hash;
		ComputeSHA256String(hash_concatenation, &two_level_hash);

		// TODO maybe we should do a stream read / hash update here
		handle->Read((void *)signature.data(), signature.size(), signature_offset);

		bool any_valid = false;
		for (auto &key : ExtensionHelper::GetPublicKeys()) {
			if (duckdb_mbedtls::MbedTlsWrapper::IsValidSha256Signature(key, signature, two_level_hash)) {
				any_valid = true;
				break;
			}
		}
		if (!any_valid) {
			throw IOException(config.error_manager->FormatException(ErrorType::UNSIGNED_EXTENSION, filename));
		}
	}
	auto basename = fs.ExtractBaseName(filename);

#ifdef WASM_LOADABLE_EXTENSIONS
	EM_ASM(
	    {
		    // Next few lines should argubly in separate JavaScript-land function call
		    // TODO: move them out / have them configurable
		    const xhr = new XMLHttpRequest();
		    xhr.open("GET", UTF8ToString($0), false);
		    xhr.responseType = "arraybuffer";
		    xhr.send(null);
		    var uInt8Array = xhr.response;
		    WebAssembly.validate(uInt8Array);
		    console.log('Loading extension ', UTF8ToString($1));

		    // Here we add the uInt8Array to Emscripten's filesystem, for it to be found by dlopen
		    FS.writeFile(UTF8ToString($1), new Uint8Array(uInt8Array));
	    },
	    filename.c_str(), basename.c_str());
	auto dopen_from = basename;
#else
	auto dopen_from = filename;
#endif

	auto lib_hdl = dlopen(dopen_from.c_str(), RTLD_NOW | RTLD_LOCAL);
	if (!lib_hdl) {
		throw IOException("Extension \"%s\" could not be loaded: %s", filename, GetDLError());
	}

	ext_version_fun_t version_fun;
	auto version_fun_name = basename + "_version";

	version_fun = LoadFunctionFromDLL<ext_version_fun_t>(lib_hdl, version_fun_name, filename);

	std::string engine_version = std::string(DuckDB::LibraryVersion());

	auto version_fun_result = (*version_fun)();
	if (version_fun_result == nullptr) {
		throw InvalidInputException("Extension \"%s\" returned a nullptr", filename);
	}
	std::string extension_version = std::string(version_fun_result);

	// Trim v's if necessary
	std::string extension_version_trimmed = extension_version;
	std::string engine_version_trimmed = engine_version;
	if (extension_version.length() > 0 && extension_version[0] == 'v') {
		extension_version_trimmed = extension_version.substr(1);
	}
	if (engine_version.length() > 0 && engine_version[0] == 'v') {
		engine_version_trimmed = engine_version.substr(1);
	}

	if (extension_version_trimmed != engine_version_trimmed) {
		throw InvalidInputException("Extension \"%s\" version (%s) does not match DuckDB version (%s)", filename,
		                            extension_version, engine_version);
	}

	result.basename = basename;
	result.filename = filename;
	result.lib_hdl = lib_hdl;
	return true;
#endif
}

ExtensionInitResult ExtensionHelper::InitialLoad(DBConfig &config, FileSystem &fs, const string &extension,
                                                 optional_ptr<const ClientConfig> client_config) {
	string error;
	ExtensionInitResult result;
	if (!TryInitialLoad(config, fs, extension, result, error, client_config)) {
		if (!ExtensionHelper::AllowAutoInstall(extension)) {
			throw IOException(error);
		}
		// the extension load failed - try installing the extension
		ExtensionHelper::InstallExtension(config, fs, extension, false);
		// try loading again
		if (!TryInitialLoad(config, fs, extension, result, error, client_config)) {
			throw IOException(error);
		}
	}
	return result;
}

bool ExtensionHelper::IsFullPath(const string &extension) {
	return StringUtil::Contains(extension, ".") || StringUtil::Contains(extension, "/") ||
	       StringUtil::Contains(extension, "\\");
}

string ExtensionHelper::GetExtensionName(const string &original_name) {
	auto extension = StringUtil::Lower(original_name);
	if (!IsFullPath(extension)) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	auto splits = StringUtil::Split(StringUtil::Replace(extension, "\\", "/"), '/');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	splits = StringUtil::Split(splits.back(), '.');
	if (splits.empty()) {
		return ExtensionHelper::ApplyExtensionAlias(extension);
	}
	return ExtensionHelper::ApplyExtensionAlias(splits.front());
}

void ExtensionHelper::LoadExternalExtension(DatabaseInstance &db, FileSystem &fs, const string &extension,
                                            optional_ptr<const ClientConfig> client_config) {
	if (db.ExtensionIsLoaded(extension)) {
		return;
	}
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Loading external extensions is disabled through a compile time flag");
#else
	auto res = InitialLoad(DBConfig::GetConfig(db), fs, extension, client_config);
	auto init_fun_name = res.basename + "_init";

	ext_init_fun_t init_fun;
	init_fun = LoadFunctionFromDLL<ext_init_fun_t>(res.lib_hdl, init_fun_name, res.filename);

	try {
		(*init_fun)(db);
	} catch (std::exception &e) {
		throw InvalidInputException("Initialization function \"%s\" from file \"%s\" threw an exception: \"%s\"",
		                            init_fun_name, res.filename, e.what());
	}

	db.SetExtensionLoaded(extension);
#endif
}

void ExtensionHelper::LoadExternalExtension(ClientContext &context, const string &extension) {
	LoadExternalExtension(DatabaseInstance::GetDatabase(context), FileSystem::GetFileSystem(context), extension,
	                      &ClientConfig::GetConfig(context));
}

string ExtensionHelper::ExtractExtensionPrefixFromPath(const string &path) {
	auto first_colon = path.find(':');
	if (first_colon == string::npos || first_colon < 2) { // needs to be at least two characters because windows c: ...
		return "";
	}
	auto extension = path.substr(0, first_colon);

	if (path.substr(first_colon, 3) == "://") {
		// these are not extensions
		return "";
	}

	D_ASSERT(extension.size() > 1);
	// needs to be alphanumeric
	for (auto &ch : extension) {
		if (!isalnum(ch) && ch != '_') {
			return "";
		}
	}
	return extension;
}

} // namespace duckdb















namespace duckdb {

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunctionSet set) {
	D_ASSERT(!set.name.empty());
	CreateScalarFunctionInfo info(std::move(set));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunction function) {
	D_ASSERT(!function.name.empty());
	ScalarFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunction function) {
	D_ASSERT(!function.name.empty());
	AggregateFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunctionSet set) {
	D_ASSERT(!set.name.empty());
	CreateAggregateFunctionInfo info(std::move(set));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunction function) {
	D_ASSERT(!function.name.empty());
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunctionSet function) {
	D_ASSERT(!function.name.empty());
	CreateTableFunctionInfo info(std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, PragmaFunction function) {
	D_ASSERT(!function.name.empty());
	PragmaFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, PragmaFunctionSet function) {
	D_ASSERT(!function.name.empty());
	auto function_name = function.name;
	CreatePragmaFunctionInfo info(std::move(function_name), std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreatePragmaFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CopyFunction function) {
	CreateCopyFunctionInfo info(std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCopyFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateMacroInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterCollation(DatabaseInstance &db, CreateCollationInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	system_catalog.CreateCollation(data, info);
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, ScalarFunction function) {
	auto &scalar_function = ExtensionUtil::GetFunction(db, function.name);
	scalar_function.functions.AddFunction(std::move(function));
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, ScalarFunctionSet functions) { // NOLINT
	D_ASSERT(!functions.name.empty());
	auto &scalar_function = ExtensionUtil::GetFunction(db, functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		scalar_function.functions.AddFunction(std::move(function));
	}
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, TableFunctionSet functions) { // NOLINT
	auto &table_function = ExtensionUtil::GetTableFunction(db, functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		table_function.functions.AddFunction(std::move(function));
	}
}

ScalarFunctionCatalogEntry &ExtensionUtil::GetFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::SCALAR_FUNCTION_ENTRY, name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionUtil::GetFunction", name);
	}
	return catalog_entry->Cast<ScalarFunctionCatalogEntry>();
}

TableFunctionCatalogEntry &ExtensionUtil::GetTableFunction(DatabaseInstance &db, const string &name) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionUtil::GetTableFunction", name);
	}
	return catalog_entry->Cast<TableFunctionCatalogEntry>();
}

void ExtensionUtil::RegisterType(DatabaseInstance &db, string type_name, LogicalType type) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type));
	info.temporary = true;
	info.internal = true;
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateType(data, info);
}

void ExtensionUtil::RegisterCastFunction(DatabaseInstance &db, const LogicalType &source, const LogicalType &target,
                                         BoundCastInfo function, int64_t implicit_cast_cost) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(source, target, std::move(function), implicit_cast_cost);
}

} // namespace duckdb


namespace duckdb {

Extension::~Extension() {
}

} // namespace duckdb





namespace duckdb {

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
                                                 vector<string> names_p, unique_ptr<ColumnDataCollection> collection_p,
                                                 ClientProperties client_properties)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, std::move(properties), collection_p->Types(),
                  std::move(names_p), std::move(client_properties)),
      collection(std::move(collection_p)), scan_initialized(false) {
}

MaterializedQueryResult::MaterializedQueryResult(PreservedError error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, std::move(error)), scan_initialized(false) {
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection->Count()) + "]\n";
		auto &coll = Collection();
		for (auto &row : coll.Rows()) {
			for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
				if (col_idx > 0) {
					result += "\t";
				}
				auto val = row.GetValue(col_idx);
				result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
			}
			result += "\n";
		}
		result += "\n";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

string MaterializedQueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	if (!success) {
		return GetError() + "\n";
	}
	if (!collection) {
		return "Internal error - result was successful but there was no collection";
	}
	BoxRenderer renderer(config);
	return renderer.ToString(context, names, Collection());
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	if (!row_collection) {
		row_collection = make_uniq<ColumnDataRowCollection>(collection->GetRows());
	}
	return row_collection->GetValue(column, index);
}

idx_t MaterializedQueryResult::RowCount() const {
	return collection ? collection->Count() : 0;
}

ColumnDataCollection &MaterializedQueryResult::Collection() {
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		throw InternalException("Missing collection from materialized query result");
	}
	return *collection;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	return FetchRaw();
}

unique_ptr<DataChunk> MaterializedQueryResult::FetchRaw() {
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", GetError());
	}
	auto result = make_uniq<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

PendingQueryResult::PendingQueryResult(shared_ptr<ClientContext> context_p, PreparedStatementData &statement,
                                       vector<LogicalType> types_p, bool allow_stream_result)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, statement.statement_type, statement.properties,
                      std::move(types_p), statement.names),
      context(std::move(context_p)), allow_stream_result(allow_stream_result) {
}

PendingQueryResult::PendingQueryResult(PreservedError error)
    : BaseQueryResult(QueryResultType::PENDING_RESULT, std::move(error)) {
}

PendingQueryResult::~PendingQueryResult() {
}

unique_ptr<ClientContextLock> PendingQueryResult::LockContext() {
	if (!context) {
		if (HasError()) {
			throw InvalidInputException(
			    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", GetError());
		}
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result");
	}
	return context->LockContext();
}

void PendingQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	bool invalidated = HasError() || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, this);
	}
	if (invalidated) {
		if (HasError()) {
			throw InvalidInputException(
			    "Attempting to execute an unsuccessful or closed pending query result\nError: %s", GetError());
		}
		throw InvalidInputException("Attempting to execute an unsuccessful or closed pending query result");
	}
}

PendingExecutionResult PendingQueryResult::ExecuteTask() {
	auto lock = LockContext();
	return ExecuteTaskInternal(*lock);
}

PendingExecutionResult PendingQueryResult::ExecuteTaskInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);
	return context->ExecuteTaskInternal(lock, *this);
}

unique_ptr<QueryResult> PendingQueryResult::ExecuteInternal(ClientContextLock &lock) {
	CheckExecutableInternal(lock);
	// Busy wait while execution is not finished
	while (!IsFinished(ExecuteTaskInternal(lock))) {
	}
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(error);
	}
	auto result = context->FetchResultInternal(lock, *this);
	Close();
	return result;
}

unique_ptr<QueryResult> PendingQueryResult::Execute() {
	auto lock = LockContext();
	return ExecuteInternal(*lock);
}

void PendingQueryResult::Close() {
	context.reset();
}

bool PendingQueryResult::IsFinished(PendingExecutionResult result) {
	if (result == PendingExecutionResult::RESULT_READY || result == PendingExecutionResult::EXECUTION_ERROR) {
		return true;
	}
	return false;
}

} // namespace duckdb





namespace duckdb {

PreparedStatement::PreparedStatement(shared_ptr<ClientContext> context, shared_ptr<PreparedStatementData> data_p,
                                     string query, idx_t n_param, case_insensitive_map_t<idx_t> named_param_map_p)
    : context(std::move(context)), data(std::move(data_p)), query(std::move(query)), success(true), n_param(n_param),
      named_param_map(std::move(named_param_map_p)) {
	D_ASSERT(data || !success);
}

PreparedStatement::PreparedStatement(PreservedError error) : context(nullptr), success(false), error(std::move(error)) {
}

PreparedStatement::~PreparedStatement() {
}

const string &PreparedStatement::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

PreservedError &PreparedStatement::GetErrorObject() {
	return error;
}

bool PreparedStatement::HasError() const {
	return !success;
}

idx_t PreparedStatement::ColumnCount() {
	D_ASSERT(data);
	return data->types.size();
}

StatementType PreparedStatement::GetStatementType() {
	D_ASSERT(data);
	return data->statement_type;
}

StatementProperties PreparedStatement::GetStatementProperties() {
	D_ASSERT(data);
	return data->properties;
}

const vector<LogicalType> &PreparedStatement::GetTypes() {
	D_ASSERT(data);
	return data->types;
}

const vector<string> &PreparedStatement::GetNames() {
	D_ASSERT(data);
	return data->names;
}

case_insensitive_map_t<LogicalType> PreparedStatement::GetExpectedParameterTypes() const {
	D_ASSERT(data);
	case_insensitive_map_t<LogicalType> expected_types(data->value_map.size());
	for (auto &it : data->value_map) {
		auto &identifier = it.first;
		D_ASSERT(data->value_map.count(identifier));
		D_ASSERT(it.second);
		expected_types[identifier] = it.second->GetValue().type();
	}
	return expected_types;
}

unique_ptr<QueryResult> PreparedStatement::Execute(case_insensitive_map_t<Value> &named_values,
                                                   bool allow_stream_result) {
	auto pending = PendingQuery(named_values, allow_stream_result);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->Execute();
}

unique_ptr<QueryResult> PreparedStatement::Execute(vector<Value> &values, bool allow_stream_result) {
	auto pending = PendingQuery(values, allow_stream_result);
	if (pending->HasError()) {
		return make_uniq<MaterializedQueryResult>(pending->GetErrorObject());
	}
	return pending->Execute();
}

unique_ptr<PendingQueryResult> PreparedStatement::PendingQuery(vector<Value> &values, bool allow_stream_result) {
	case_insensitive_map_t<Value> named_values;
	for (idx_t i = 0; i < values.size(); i++) {
		auto &val = values[i];
		named_values[std::to_string(i + 1)] = val;
	}
	return PendingQuery(named_values, allow_stream_result);
}

unique_ptr<PendingQueryResult> PreparedStatement::PendingQuery(case_insensitive_map_t<Value> &named_values,
                                                               bool allow_stream_result) {
	if (!success) {
		auto exception = InvalidInputException("Attempting to execute an unsuccessfully prepared statement!");
		return make_uniq<PendingQueryResult>(PreservedError(exception));
	}
	PendingQueryParameters parameters;
	parameters.parameters = &named_values;

	try {
		VerifyParameters(named_values, named_param_map);
	} catch (const Exception &ex) {
		return make_uniq<PendingQueryResult>(PreservedError(ex));
	}

	D_ASSERT(data);
	parameters.allow_stream_result = allow_stream_result && data->properties.allow_stream_result;
	auto result = context->PendingQuery(query, data, parameters);
	// The result should not contain any reference to the 'vector<Value> parameters.parameters'
	return result;
}

} // namespace duckdb




namespace duckdb {

PreparedStatementData::PreparedStatementData(StatementType type) : statement_type(type) {
}

PreparedStatementData::~PreparedStatementData() {
}

void PreparedStatementData::CheckParameterCount(idx_t parameter_count) {
	const auto required = properties.parameter_count;
	if (parameter_count != required) {
		throw BinderException("Parameter/argument count mismatch for prepared statement. Expected %llu, got %llu",
		                      required, parameter_count);
	}
}

bool PreparedStatementData::RequireRebind(ClientContext &context, optional_ptr<case_insensitive_map_t<Value>> values) {
	idx_t count = values ? values->size() : 0;
	CheckParameterCount(count);
	if (!unbound_statement) {
		// no unbound statement!? cannot rebind?
		return false;
	}
	if (!properties.bound_all_parameters) {
		// parameters not yet bound: query always requires a rebind
		return true;
	}
	if (Catalog::GetSystemCatalog(context).GetCatalogVersion() != catalog_version) {
		//! context is out of bounds
		return true;
	}
	for (auto &it : value_map) {
		auto &identifier = it.first;
		auto lookup = values->find(identifier);
		D_ASSERT(lookup != values->end());
		if (lookup->second.type() != it.second->return_type) {
			return true;
		}
	}
	return false;
}

void PreparedStatementData::Bind(case_insensitive_map_t<Value> values) {
	// set parameters
	D_ASSERT(!unbound_statement || unbound_statement->n_param == properties.parameter_count);
	CheckParameterCount(values.size());

	// bind the required values
	for (auto &it : value_map) {
		const string &identifier = it.first;
		auto lookup = values.find(identifier);
		if (lookup == values.end()) {
			throw BinderException("Could not find parameter with identifier %s", identifier);
		}
		D_ASSERT(it.second);
		auto &value = lookup->second;
		if (!value.DefaultTryCastAs(it.second->return_type)) {
			throw BinderException(
			    "Type mismatch for binding parameter with identifier %s, expected type %s but got type %s", identifier,
			    it.second->return_type.ToString().c_str(), value.type().ToString().c_str());
		}
		it.second->SetValue(value);
	}
}

bool PreparedStatementData::TryGetType(const string &identifier, LogicalType &result) {
	auto it = value_map.find(identifier);
	if (it == value_map.end()) {
		return false;
	}
	if (it->second->return_type.id() != LogicalTypeId::INVALID) {
		result = it->second->return_type;
	} else {
		result = it->second->GetValue().type();
	}
	return true;
}

LogicalType PreparedStatementData::GetType(const string &identifier) {
	LogicalType result;
	if (!TryGetType(identifier, result)) {
		throw BinderException("Could not find parameter identified with: %s", identifier);
	}
	return result;
}

} // namespace duckdb


















#include <algorithm>
#include <utility>

namespace duckdb {

QueryProfiler::QueryProfiler(ClientContext &context_p)
    : context(context_p), running(false), query_requires_profiling(false), is_explain_analyze(false) {
}

bool QueryProfiler::IsEnabled() const {
	return is_explain_analyze ? true : ClientConfig::GetConfig(context).enable_profiler;
}

bool QueryProfiler::IsDetailedEnabled() const {
	return is_explain_analyze ? false : ClientConfig::GetConfig(context).enable_detailed_profiling;
}

ProfilerPrintFormat QueryProfiler::GetPrintFormat() const {
	return ClientConfig::GetConfig(context).profiler_print_format;
}

bool QueryProfiler::PrintOptimizerOutput() const {
	return GetPrintFormat() == ProfilerPrintFormat::QUERY_TREE_OPTIMIZER || IsDetailedEnabled();
}

string QueryProfiler::GetSaveLocation() const {
	return is_explain_analyze ? string() : ClientConfig::GetConfig(context).profiler_save_location;
}

QueryProfiler &QueryProfiler::Get(ClientContext &context) {
	return *ClientData::Get(context).profiler;
}

void QueryProfiler::StartQuery(string query, bool is_explain_analyze, bool start_at_optimizer) {
	if (is_explain_analyze) {
		StartExplainAnalyze();
	}
	if (!IsEnabled()) {
		return;
	}
	if (start_at_optimizer && !PrintOptimizerOutput()) {
		// This is the StartQuery call before the optimizer, but we don't have to print optimizer output
		return;
	}
	if (running) {
		// Called while already running: this should only happen when we print optimizer output
		D_ASSERT(PrintOptimizerOutput());
		return;
	}
	this->running = true;
	this->query = std::move(query);
	tree_map.clear();
	root = nullptr;
	phase_timings.clear();
	phase_stack.clear();

	main_query.Start();
}

bool QueryProfiler::OperatorRequiresProfiling(PhysicalOperatorType op_type) {
	switch (op_type) {
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
	case PhysicalOperatorType::STREAMING_SAMPLE:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::LIMIT_PERCENT:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::TOP_N:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::UNNEST:
	case PhysicalOperatorType::UNGROUPED_AGGREGATE:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::COPY_TO_FILE:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::EXPRESSION_SCAN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::IE_JOIN:
	case PhysicalOperatorType::DELIM_JOIN:
	case PhysicalOperatorType::UNION:
	case PhysicalOperatorType::RECURSIVE_CTE:
	case PhysicalOperatorType::EMPTY_RESULT:
		return true;
	default:
		return false;
	}
}

void QueryProfiler::Finalize(TreeNode &node) {
	for (auto &child : node.children) {
		Finalize(*child);
		if (node.type == PhysicalOperatorType::UNION) {
			node.info.elements += child->info.elements;
		}
	}
}

void QueryProfiler::StartExplainAnalyze() {
	this->is_explain_analyze = true;
}

void QueryProfiler::EndQuery() {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}

	main_query.End();
	if (root) {
		Finalize(*root);
	}
	this->running = false;
	// print or output the query profiling after termination
	// EXPLAIN ANALYSE should not be outputted by the profiler
	if (IsEnabled() && !is_explain_analyze) {
		string query_info = ToString();
		auto save_location = GetSaveLocation();
		if (!ClientConfig::GetConfig(context).emit_profiler_output) {
			// disable output
		} else if (save_location.empty()) {
			Printer::Print(query_info);
			Printer::Print("\n");
		} else {
			WriteToFile(save_location.c_str(), query_info);
		}
	}
	this->is_explain_analyze = false;
}
string QueryProfiler::ToString() const {
	const auto format = GetPrintFormat();
	switch (format) {
	case ProfilerPrintFormat::QUERY_TREE:
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return QueryTreeToString();
	case ProfilerPrintFormat::JSON:
		return ToJSON();
	default:
		throw InternalException("Unknown ProfilerPrintFormat \"%s\"", format);
	}
}

void QueryProfiler::StartPhase(string new_phase) {
	if (!IsEnabled() || !running) {
		return;
	}

	if (!phase_stack.empty()) {
		// there are active phases
		phase_profiler.End();
		// add the timing to all phases prior to this one
		string prefix = "";
		for (auto &phase : phase_stack) {
			phase_timings[phase] += phase_profiler.Elapsed();
			prefix += phase + " > ";
		}
		// when there are previous phases, we prefix the current phase with those phases
		new_phase = prefix + new_phase;
	}

	// start a new phase
	phase_stack.push_back(new_phase);
	// restart the timer
	phase_profiler.Start();
}

void QueryProfiler::EndPhase() {
	if (!IsEnabled() || !running) {
		return;
	}
	D_ASSERT(phase_stack.size() > 0);

	// end the timer
	phase_profiler.End();
	// add the timing to all currently active phases
	for (auto &phase : phase_stack) {
		phase_timings[phase] += phase_profiler.Elapsed();
	}
	// now remove the last added phase
	phase_stack.pop_back();

	if (!phase_stack.empty()) {
		phase_profiler.Start();
	}
}

void QueryProfiler::Initialize(const PhysicalOperator &root_op) {
	if (!IsEnabled() || !running) {
		return;
	}
	this->query_requires_profiling = false;
	this->root = CreateTree(root_op);
	if (!query_requires_profiling) {
		// query does not require profiling: disable profiling for this query
		this->running = false;
		tree_map.clear();
		root = nullptr;
		phase_timings.clear();
		phase_stack.clear();
	}
}

OperatorProfiler::OperatorProfiler(bool enabled_p) : enabled(enabled_p), active_operator(nullptr) {
}

void OperatorProfiler::StartOperator(optional_ptr<const PhysicalOperator> phys_op) {
	if (!enabled) {
		return;
	}

	if (active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call StartOperator while another operator is active");
	}

	active_operator = phys_op;

	// start timing for current element
	op.Start();
}

void OperatorProfiler::EndOperator(optional_ptr<DataChunk> chunk) {
	if (!enabled) {
		return;
	}

	if (!active_operator) {
		throw InternalException("OperatorProfiler: Attempting to call EndOperator while another operator is active");
	}

	// finish timing for the current element
	op.End();

	AddTiming(*active_operator, op.Elapsed(), chunk ? chunk->size() : 0);
	active_operator = nullptr;
}

void OperatorProfiler::AddTiming(const PhysicalOperator &op, double time, idx_t elements) {
	if (!enabled) {
		return;
	}
	if (!Value::DoubleIsFinite(time)) {
		return;
	}
	auto entry = timings.find(op);
	if (entry == timings.end()) {
		// add new entry
		timings[op] = OperatorInformation(time, elements);
	} else {
		// add to existing entry
		entry->second.time += time;
		entry->second.elements += elements;
	}
}
void OperatorProfiler::Flush(const PhysicalOperator &phys_op, ExpressionExecutor &expression_executor,
                             const string &name, int id) {
	auto entry = timings.find(phys_op);
	if (entry == timings.end()) {
		return;
	}
	auto &operator_timing = timings.find(phys_op)->second;
	if (int(operator_timing.executors_info.size()) <= id) {
		operator_timing.executors_info.resize(id + 1);
	}
	operator_timing.executors_info[id] = make_uniq<ExpressionExecutorInfo>(expression_executor, name, id);
	operator_timing.name = phys_op.GetName();
}

void QueryProfiler::Flush(OperatorProfiler &profiler) {
	lock_guard<mutex> guard(flush_lock);
	if (!IsEnabled() || !running) {
		return;
	}
	for (auto &node : profiler.timings) {
		auto &op = node.first.get();
		auto entry = tree_map.find(op);
		D_ASSERT(entry != tree_map.end());
		auto &tree_node = entry->second.get();

		tree_node.info.time += node.second.time;
		tree_node.info.elements += node.second.elements;
		if (!IsDetailedEnabled()) {
			continue;
		}
		for (auto &info : node.second.executors_info) {
			if (!info) {
				continue;
			}
			auto info_id = info->id;
			if (int32_t(tree_node.info.executors_info.size()) <= info_id) {
				tree_node.info.executors_info.resize(info_id + 1);
			}
			tree_node.info.executors_info[info_id] = std::move(info);
		}
	}
	profiler.timings.clear();
}

static string DrawPadded(const string &str, idx_t width) {
	if (str.size() > width) {
		return str.substr(0, width);
	} else {
		width -= str.size();
		int half_spaces = width / 2;
		int extra_left_space = width % 2 != 0 ? 1 : 0;
		return string(half_spaces + extra_left_space, ' ') + str + string(half_spaces, ' ');
	}
}

static string RenderTitleCase(string str) {
	str = StringUtil::Lower(str);
	str[0] = toupper(str[0]);
	for (idx_t i = 0; i < str.size(); i++) {
		if (str[i] == '_') {
			str[i] = ' ';
			if (i + 1 < str.size()) {
				str[i + 1] = toupper(str[i + 1]);
			}
		}
	}
	return str;
}

static string RenderTiming(double timing) {
	string timing_s;
	if (timing >= 1) {
		timing_s = StringUtil::Format("%.2f", timing);
	} else if (timing >= 0.1) {
		timing_s = StringUtil::Format("%.3f", timing);
	} else {
		timing_s = StringUtil::Format("%.4f", timing);
	}
	return timing_s + "s";
}

string QueryProfiler::QueryTreeToString() const {
	std::stringstream str;
	QueryTreeToStream(str);
	return str.str();
}

void QueryProfiler::QueryTreeToStream(std::ostream &ss) const {
	if (!IsEnabled()) {
		ss << "Query profiling is disabled. Call "
		      "Connection::EnableProfiling() to enable profiling!";
		return;
	}
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	ss << "││    Query Profiling Information    ││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	ss << StringUtil::Replace(query, "\n", " ") + "\n";

	// checking the tree to ensure the query is really empty
	// the query string is empty when a logical plan is deserialized
	if (query.empty() && !root) {
		return;
	}

	if (context.client_data->http_state && !context.client_data->http_state->IsEmpty()) {
		string read =
		    "in: " + StringUtil::BytesToHumanReadableString(context.client_data->http_state->total_bytes_received);
		string written =
		    "out: " + StringUtil::BytesToHumanReadableString(context.client_data->http_state->total_bytes_sent);
		string head = "#HEAD: " + to_string(context.client_data->http_state->head_count);
		string get = "#GET: " + to_string(context.client_data->http_state->get_count);
		string put = "#PUT: " + to_string(context.client_data->http_state->put_count);
		string post = "#POST: " + to_string(context.client_data->http_state->post_count);

		constexpr idx_t TOTAL_BOX_WIDTH = 39;
		ss << "┌─────────────────────────────────────┐\n";
		ss << "│┌───────────────────────────────────┐│\n";
		ss << "││            HTTP Stats:            ││\n";
		ss << "││                                   ││\n";
		ss << "││" + DrawPadded(read, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(written, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(head, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(get, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(put, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "││" + DrawPadded(post, TOTAL_BOX_WIDTH - 4) + "││\n";
		ss << "│└───────────────────────────────────┘│\n";
		ss << "└─────────────────────────────────────┘\n";
	}

	constexpr idx_t TOTAL_BOX_WIDTH = 39;
	ss << "┌─────────────────────────────────────┐\n";
	ss << "│┌───────────────────────────────────┐│\n";
	string total_time = "Total Time: " + RenderTiming(main_query.Elapsed());
	ss << "││" + DrawPadded(total_time, TOTAL_BOX_WIDTH - 4) + "││\n";
	ss << "│└───────────────────────────────────┘│\n";
	ss << "└─────────────────────────────────────┘\n";
	// print phase timings
	if (PrintOptimizerOutput()) {
		bool has_previous_phase = false;
		for (const auto &entry : GetOrderedPhaseTimings()) {
			if (!StringUtil::Contains(entry.first, " > ")) {
				// primary phase!
				if (has_previous_phase) {
					ss << "│└───────────────────────────────────┘│\n";
					ss << "└─────────────────────────────────────┘\n";
				}
				ss << "┌─────────────────────────────────────┐\n";
				ss << "│" +
				          DrawPadded(RenderTitleCase(entry.first) + ": " + RenderTiming(entry.second),
				                     TOTAL_BOX_WIDTH - 2) +
				          "│\n";
				ss << "│┌───────────────────────────────────┐│\n";
				has_previous_phase = true;
			} else {
				string entry_name = StringUtil::Split(entry.first, " > ")[1];
				ss << "││" +
				          DrawPadded(RenderTitleCase(entry_name) + ": " + RenderTiming(entry.second),
				                     TOTAL_BOX_WIDTH - 4) +
				          "││\n";
			}
		}
		if (has_previous_phase) {
			ss << "│└───────────────────────────────────┘│\n";
			ss << "└─────────────────────────────────────┘\n";
		}
	}
	// render the main operator tree
	if (root) {
		Render(*root, ss);
	}
}

static string JSONSanitize(const string &text) {
	string result;
	result.reserve(text.size());
	for (idx_t i = 0; i < text.size(); i++) {
		switch (text[i]) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += text[i];
			break;
		}
	}
	return result;
}

// Print a row
static void PrintRow(std::ostream &ss, const string &annotation, int id, const string &name, double time,
                     int sample_counter, int tuple_counter, const string &extra_info, int depth) {
	ss << string(depth * 3, ' ') << " {\n";
	ss << string(depth * 3, ' ') << "   \"annotation\": \"" + JSONSanitize(annotation) + "\",\n";
	ss << string(depth * 3, ' ') << "   \"id\": " + to_string(id) + ",\n";
	ss << string(depth * 3, ' ') << "   \"name\": \"" + JSONSanitize(name) + "\",\n";
#if defined(RDTSC)
	ss << string(depth * 3, ' ') << "   \"timing\": \"NULL\" ,\n";
	ss << string(depth * 3, ' ') << "   \"cycles_per_tuple\": " + StringUtil::Format("%.4f", time) + ",\n";
#else
	ss << string(depth * 3, ' ') << "   \"timing\":" + to_string(time) + ",\n";
	ss << string(depth * 3, ' ') << "   \"cycles_per_tuple\": \"NULL\" ,\n";
#endif
	ss << string(depth * 3, ' ') << "   \"sample_size\": " << to_string(sample_counter) + ",\n";
	ss << string(depth * 3, ' ') << "   \"input_size\": " << to_string(tuple_counter) + ",\n";
	ss << string(depth * 3, ' ') << "   \"extra_info\": \"" << JSONSanitize(extra_info) + "\"\n";
	ss << string(depth * 3, ' ') << " },\n";
}

static void ExtractFunctions(std::ostream &ss, ExpressionInfo &info, int &fun_id, int depth) {
	if (info.hasfunction) {
		double time = info.sample_tuples_count == 0 ? 0 : int(info.function_time) / double(info.sample_tuples_count);
		PrintRow(ss, "Function", fun_id++, info.function_name, time, info.sample_tuples_count, info.tuples_count, "",
		         depth);
	}
	if (info.children.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : info.children) {
		ExtractFunctions(ss, *child, fun_id, depth);
	}
}

static void ToJSONRecursive(QueryProfiler::TreeNode &node, std::ostream &ss, int depth = 1) {
	ss << string(depth * 3, ' ') << " {\n";
	ss << string(depth * 3, ' ') << "   \"name\": \"" + JSONSanitize(node.name) + "\",\n";
	ss << string(depth * 3, ' ') << "   \"timing\":" + to_string(node.info.time) + ",\n";
	ss << string(depth * 3, ' ') << "   \"cardinality\":" + to_string(node.info.elements) + ",\n";
	ss << string(depth * 3, ' ') << "   \"extra_info\": \"" + JSONSanitize(node.extra_info) + "\",\n";
	ss << string(depth * 3, ' ') << "   \"timings\": [";
	int32_t function_counter = 1;
	int32_t expression_counter = 1;
	ss << "\n ";
	for (auto &expr_executor : node.info.executors_info) {
		// For each Expression tree
		if (!expr_executor) {
			continue;
		}
		for (auto &expr_timer : expr_executor->roots) {
			double time = expr_timer->sample_tuples_count == 0
			                  ? 0
			                  : double(expr_timer->time) / double(expr_timer->sample_tuples_count);
			PrintRow(ss, "ExpressionRoot", expression_counter++, expr_timer->name, time,
			         expr_timer->sample_tuples_count, expr_timer->tuples_count, expr_timer->extra_info, depth + 1);
			// Extract all functions inside the tree
			ExtractFunctions(ss, *expr_timer->root, function_counter, depth + 1);
		}
	}
	ss.seekp(-2, ss.cur);
	ss << "\n";
	ss << string(depth * 3, ' ') << "   ],\n";
	ss << string(depth * 3, ' ') << "   \"children\": [\n";
	if (node.children.empty()) {
		ss << string(depth * 3, ' ') << "   ]\n";
	} else {
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (i > 0) {
				ss << ",\n";
			}
			ToJSONRecursive(*node.children[i], ss, depth + 1);
		}
		ss << string(depth * 3, ' ') << "   ]\n";
	}
	ss << string(depth * 3, ' ') << " }\n";
}

string QueryProfiler::ToJSON() const {
	if (!IsEnabled()) {
		return "{ \"result\": \"disabled\" }\n";
	}
	if (query.empty() && !root) {
		return "{ \"result\": \"empty\" }\n";
	}
	if (!root) {
		return "{ \"result\": \"error\" }\n";
	}
	std::stringstream ss;
	ss << "{\n";
	ss << "   \"name\":  \"Query\", \n";
	ss << "   \"result\": " + to_string(main_query.Elapsed()) + ",\n";
	ss << "   \"timing\": " + to_string(main_query.Elapsed()) + ",\n";
	ss << "   \"cardinality\": " + to_string(root->info.elements) + ",\n";
	// JSON cannot have literal control characters in string literals
	string extra_info = JSONSanitize(query);
	ss << "   \"extra-info\": \"" + extra_info + "\", \n";
	// print the phase timings
	ss << "   \"timings\": [\n";
	const auto &ordered_phase_timings = GetOrderedPhaseTimings();
	for (idx_t i = 0; i < ordered_phase_timings.size(); i++) {
		if (i > 0) {
			ss << ",\n";
		}
		ss << "   {\n";
		ss << "   \"annotation\": \"" + ordered_phase_timings[i].first + "\", \n";
		ss << "   \"timing\": " + to_string(ordered_phase_timings[i].second) + "\n";
		ss << "   }";
	}
	ss << "\n";
	ss << "   ],\n";
	// recursively print the physical operator tree
	ss << "   \"children\": [\n";
	ToJSONRecursive(*root, ss);
	ss << "   ]\n";
	ss << "}";
	return ss.str();
}

void QueryProfiler::WriteToFile(const char *path, string &info) const {
	ofstream out(path);
	out << info;
	out.close();
	// throw an IO exception if it fails to write the file
	if (out.fail()) {
		throw IOException(strerror(errno));
	}
}

unique_ptr<QueryProfiler::TreeNode> QueryProfiler::CreateTree(const PhysicalOperator &root, idx_t depth) {
	if (OperatorRequiresProfiling(root.type)) {
		this->query_requires_profiling = true;
	}
	auto node = make_uniq<QueryProfiler::TreeNode>();
	node->type = root.type;
	node->name = root.GetName();
	node->extra_info = root.ParamsToString();
	node->depth = depth;
	tree_map.insert(make_pair(reference<const PhysicalOperator>(root), reference<QueryProfiler::TreeNode>(*node)));
	auto children = root.GetChildren();
	for (auto &child : children) {
		auto child_node = CreateTree(child.get(), depth + 1);
		node->children.push_back(std::move(child_node));
	}
	return node;
}

void QueryProfiler::Render(const QueryProfiler::TreeNode &node, std::ostream &ss) const {
	TreeRenderer renderer;
	if (IsDetailedEnabled()) {
		renderer.EnableDetailed();
	} else {
		renderer.EnableStandard();
	}
	renderer.Render(node, ss);
}

void QueryProfiler::Print() {
	Printer::Print(QueryTreeToString());
}

vector<QueryProfiler::PhaseTimingItem> QueryProfiler::GetOrderedPhaseTimings() const {
	vector<PhaseTimingItem> result;
	// first sort the phases alphabetically
	vector<string> phases;
	for (auto &entry : phase_timings) {
		phases.push_back(entry.first);
	}
	std::sort(phases.begin(), phases.end());
	for (const auto &phase : phases) {
		auto entry = phase_timings.find(phase);
		D_ASSERT(entry != phase_timings.end());
		result.emplace_back(entry->first, entry->second);
	}
	return result;
}
void QueryProfiler::Propagate(QueryProfiler &qp) {
}

void ExpressionInfo::ExtractExpressionsRecursive(unique_ptr<ExpressionState> &state) {
	if (state->child_states.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : state->child_states) {
		auto expr_info = make_uniq<ExpressionInfo>();
		if (child->expr.expression_class == ExpressionClass::BOUND_FUNCTION) {
			expr_info->hasfunction = true;
			expr_info->function_name = child->expr.Cast<BoundFunctionExpression>().function.ToString();
			expr_info->function_time = child->profiler.time;
			expr_info->sample_tuples_count = child->profiler.sample_tuples_count;
			expr_info->tuples_count = child->profiler.tuples_count;
		}
		expr_info->ExtractExpressionsRecursive(child);
		children.push_back(std::move(expr_info));
	}
	return;
}

ExpressionExecutorInfo::ExpressionExecutorInfo(ExpressionExecutor &executor, const string &name, int id) : id(id) {
	// Extract Expression Root Information from ExpressionExecutorStats
	for (auto &state : executor.GetStates()) {
		roots.push_back(make_uniq<ExpressionRootInfo>(*state, name));
	}
}

ExpressionRootInfo::ExpressionRootInfo(ExpressionExecutorState &state, string name)
    : current_count(state.profiler.current_count), sample_count(state.profiler.sample_count),
      sample_tuples_count(state.profiler.sample_tuples_count), tuples_count(state.profiler.tuples_count),
      name("expression"), time(state.profiler.time) {
	// Use the name of expression-tree as extra-info
	extra_info = std::move(name);
	auto expression_info_p = make_uniq<ExpressionInfo>();
	// Maybe root has a function
	if (state.root_state->expr.expression_class == ExpressionClass::BOUND_FUNCTION) {
		expression_info_p->hasfunction = true;
		expression_info_p->function_name = (state.root_state->expr.Cast<BoundFunctionExpression>()).function.name;
		expression_info_p->function_time = state.root_state->profiler.time;
		expression_info_p->sample_tuples_count = state.root_state->profiler.sample_tuples_count;
		expression_info_p->tuples_count = state.root_state->profiler.tuples_count;
	}
	expression_info_p->ExtractExpressionsRecursive(state.root_state);
	root = std::move(expression_info_p);
}
} // namespace duckdb






namespace duckdb {

BaseQueryResult::BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties_p,
                                 vector<LogicalType> types_p, vector<string> names_p)
    : type(type), statement_type(statement_type), properties(std::move(properties_p)), types(std::move(types_p)),
      names(std::move(names_p)), success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(QueryResultType type, PreservedError error)
    : type(type), success(false), error(std::move(error)) {
}

BaseQueryResult::~BaseQueryResult() {
}

void BaseQueryResult::ThrowError(const string &prepended_message) const {
	D_ASSERT(HasError());
	error.Throw(prepended_message);
}

void BaseQueryResult::SetError(PreservedError error) {
	success = !error;
	this->error = std::move(error);
}

bool BaseQueryResult::HasError() const {
	D_ASSERT((bool)error == !success);
	return !success;
}

const ExceptionType &BaseQueryResult::GetErrorType() const {
	return error.Type();
}

const std::string &BaseQueryResult::GetError() {
	D_ASSERT(HasError());
	return error.Message();
}

PreservedError &BaseQueryResult::GetErrorObject() {
	return error;
}

idx_t BaseQueryResult::ColumnCount() {
	return types.size();
}

QueryResult::QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
                         vector<LogicalType> types_p, vector<string> names_p, ClientProperties client_properties_p)
    : BaseQueryResult(type, statement_type, std::move(properties), std::move(types_p), std::move(names_p)),
      client_properties(std::move(client_properties_p)) {
}

QueryResult::QueryResult(QueryResultType type, PreservedError error)
    : BaseQueryResult(type, std::move(error)), client_properties("UTC", ArrowOffsetSize::REGULAR) {
}

QueryResult::~QueryResult() {
}

const string &QueryResult::ColumnName(idx_t index) const {
	D_ASSERT(index < names.size());
	return names[index];
}

string QueryResult::ToBox(ClientContext &context, const BoxRendererConfig &config) {
	return ToString();
}

unique_ptr<DataChunk> QueryResult::Fetch() {
	auto chunk = FetchRaw();
	if (!chunk) {
		return nullptr;
	}
	chunk->Flatten();
	return chunk;
}

bool QueryResult::Equals(QueryResult &other) { // LCOV_EXCL_START
	// first compare the success state of the results
	if (success != other.success) {
		return false;
	}
	if (!success) {
		return error == other.error;
	}
	// compare names
	if (names != other.names) {
		return false;
	}
	// compare types
	if (types != other.types) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	unique_ptr<DataChunk> lchunk, rchunk;
	idx_t lindex = 0, rindex = 0;
	while (true) {
		if (!lchunk || lindex == lchunk->size()) {
			lchunk = Fetch();
			lindex = 0;
		}
		if (!rchunk || rindex == rchunk->size()) {
			rchunk = other.Fetch();
			rindex = 0;
		}
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (; lindex < lchunk->size() && rindex < rchunk->size(); lindex++, rindex++) {
			for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
				auto lvalue = lchunk->GetValue(col, lindex);
				auto rvalue = rchunk->GetValue(col, rindex);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue.IsNull() != rvalue.IsNull()) {
					return false;
				}
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
} // LCOV_EXCL_STOP

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : names) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : types) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}

} // namespace duckdb





namespace duckdb {

AggregateRelation::AggregateRelation(shared_ptr<Relation> child_p,
                                     vector<unique_ptr<ParsedExpression>> parsed_expressions)
    : Relation(child_p->context, RelationType::AGGREGATE_RELATION), expressions(std::move(parsed_expressions)),
      child(std::move(child_p)) {
	// bind the expressions
	context.GetContext()->TryBindRelation(*this, this->columns);
}

AggregateRelation::AggregateRelation(shared_ptr<Relation> child_p,
                                     vector<unique_ptr<ParsedExpression>> parsed_expressions, GroupByNode groups_p)
    : Relation(child_p->context, RelationType::AGGREGATE_RELATION), expressions(std::move(parsed_expressions)),
      groups(std::move(groups_p)), child(std::move(child_p)) {
	// bind the expressions
	context.GetContext()->TryBindRelation(*this, this->columns);
}

AggregateRelation::AggregateRelation(shared_ptr<Relation> child_p,
                                     vector<unique_ptr<ParsedExpression>> parsed_expressions,
                                     vector<unique_ptr<ParsedExpression>> groups_p)
    : Relation(child_p->context, RelationType::AGGREGATE_RELATION), expressions(std::move(parsed_expressions)),
      child(std::move(child_p)) {
	if (!groups_p.empty()) {
		// explicit groups provided: use standard handling
		GroupingSet grouping_set;
		for (idx_t i = 0; i < groups_p.size(); i++) {
			groups.group_expressions.push_back(std::move(groups_p[i]));
			grouping_set.insert(i);
		}
		groups.grouping_sets.push_back(std::move(grouping_set));
	}
	// bind the expressions
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> AggregateRelation::GetQueryNode() {
	auto child_ptr = child.get();
	while (child_ptr->InheritsColumnBindings()) {
		child_ptr = child_ptr->ChildRelation();
	}
	unique_ptr<QueryNode> result;
	if (child_ptr->type == RelationType::JOIN_RELATION) {
		// child node is a join: push projection into the child query node
		result = child->GetQueryNode();
	} else {
		// child node is not a join: create a new select node and push the child as a table reference
		auto select = make_uniq<SelectNode>();
		select->from_table = child->GetTableRef();
		result = std::move(select);
	}
	D_ASSERT(result->type == QueryNodeType::SELECT_NODE);
	auto &select_node = result->Cast<SelectNode>();
	if (!groups.group_expressions.empty()) {
		select_node.aggregate_handling = AggregateHandling::STANDARD_HANDLING;
		select_node.groups = groups.Copy();
	} else {
		// no groups provided: automatically figure out groups (if any)
		select_node.aggregate_handling = AggregateHandling::FORCE_AGGREGATES;
	}
	select_node.select_list.clear();
	for (auto &expr : expressions) {
		select_node.select_list.push_back(expr->Copy());
	}
	return result;
}

string AggregateRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &AggregateRelation::Columns() {
	return columns;
}

string AggregateRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Aggregate [";
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += expressions[i]->ToString();
	}
	str += "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

CreateTableRelation::CreateTableRelation(shared_ptr<Relation> child_p, string schema_name, string table_name)
    : Relation(child_p->context, RelationType::CREATE_TABLE_RELATION), child(std::move(child_p)),
      schema_name(std::move(schema_name)), table_name(std::move(table_name)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement CreateTableRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();

	CreateStatement stmt;
	auto info = make_uniq<CreateTableInfo>();
	info->schema = schema_name;
	info->table = table_name;
	info->query = std::move(select);
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	stmt.info = std::move(info);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &CreateTableRelation::Columns() {
	return columns;
}

string CreateTableRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Create Table\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb





namespace duckdb {

CreateViewRelation::CreateViewRelation(shared_ptr<Relation> child_p, string view_name_p, bool replace_p,
                                       bool temporary_p)
    : Relation(child_p->context, RelationType::CREATE_VIEW_RELATION), child(std::move(child_p)),
      view_name(std::move(view_name_p)), replace(replace_p), temporary(temporary_p) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

CreateViewRelation::CreateViewRelation(shared_ptr<Relation> child_p, string schema_name_p, string view_name_p,
                                       bool replace_p, bool temporary_p)
    : Relation(child_p->context, RelationType::CREATE_VIEW_RELATION), child(std::move(child_p)),
      schema_name(std::move(schema_name_p)), view_name(std::move(view_name_p)), replace(replace_p),
      temporary(temporary_p) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement CreateViewRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();

	CreateStatement stmt;
	auto info = make_uniq<CreateViewInfo>();
	info->query = std::move(select);
	info->view_name = view_name;
	info->temporary = temporary;
	info->schema = schema_name;
	info->on_conflict = replace ? OnCreateConflict::REPLACE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	stmt.info = std::move(info);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &CreateViewRelation::Columns() {
	return columns;
}

string CreateViewRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Create View\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

CrossProductRelation::CrossProductRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                                           JoinRefType ref_type)
    : Relation(left_p->context, RelationType::CROSS_PRODUCT_RELATION), left(std::move(left_p)),
      right(std::move(right_p)), ref_type(ref_type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> CrossProductRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> CrossProductRelation::GetTableRef() {
	auto cross_product_ref = make_uniq<JoinRef>(ref_type);
	cross_product_ref->left = left->GetTableRef();
	cross_product_ref->right = right->GetTableRef();
	return std::move(cross_product_ref);
}

const vector<ColumnDefinition> &CrossProductRelation::Columns() {
	return this->columns;
}

string CrossProductRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str = "Cross Product";
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

DeleteRelation::DeleteRelation(ClientContextWrapper &context, unique_ptr<ParsedExpression> condition_p,
                               string schema_name_p, string table_name_p)
    : Relation(context, RelationType::DELETE_RELATION), condition(std::move(condition_p)),
      schema_name(std::move(schema_name_p)), table_name(std::move(table_name_p)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement DeleteRelation::Bind(Binder &binder) {
	auto basetable = make_uniq<BaseTableRef>();
	basetable->schema_name = schema_name;
	basetable->table_name = table_name;

	DeleteStatement stmt;
	stmt.condition = condition ? condition->Copy() : nullptr;
	stmt.table = std::move(basetable);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &DeleteRelation::Columns() {
	return columns;
}

string DeleteRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "DELETE FROM " + table_name;
	if (condition) {
		str += " WHERE " + condition->ToString();
	}
	return str;
}

} // namespace duckdb




namespace duckdb {

DistinctRelation::DistinctRelation(shared_ptr<Relation> child_p)
    : Relation(child_p->context, RelationType::DISTINCT_RELATION), child(std::move(child_p)) {
	D_ASSERT(child.get() != this);
	vector<ColumnDefinition> dummy_columns;
	context.GetContext()->TryBindRelation(*this, dummy_columns);
}

unique_ptr<QueryNode> DistinctRelation::GetQueryNode() {
	auto child_node = child->GetQueryNode();
	child_node->AddDistinct();
	return child_node;
}

string DistinctRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &DistinctRelation::Columns() {
	return child->Columns();
}

string DistinctRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Distinct\n";
	return str + child->ToString(depth + 1);
	;
}

} // namespace duckdb







namespace duckdb {

ExplainRelation::ExplainRelation(shared_ptr<Relation> child_p, ExplainType type)
    : Relation(child_p->context, RelationType::EXPLAIN_RELATION), child(std::move(child_p)), type(type) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement ExplainRelation::Bind(Binder &binder) {
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();
	ExplainStatement explain(std::move(select), type);
	return binder.Bind(explain.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &ExplainRelation::Columns() {
	return columns;
}

string ExplainRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Explain\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb







namespace duckdb {

FilterRelation::FilterRelation(shared_ptr<Relation> child_p, unique_ptr<ParsedExpression> condition_p)
    : Relation(child_p->context, RelationType::FILTER_RELATION), condition(std::move(condition_p)),
      child(std::move(child_p)) {
	D_ASSERT(child.get() != this);
	vector<ColumnDefinition> dummy_columns;
	context.GetContext()->TryBindRelation(*this, dummy_columns);
}

unique_ptr<QueryNode> FilterRelation::GetQueryNode() {
	auto child_ptr = child.get();
	while (child_ptr->InheritsColumnBindings()) {
		child_ptr = child_ptr->ChildRelation();
	}
	if (child_ptr->type == RelationType::JOIN_RELATION) {
		// child node is a join: push filter into WHERE clause of select node
		auto child_node = child->GetQueryNode();
		D_ASSERT(child_node->type == QueryNodeType::SELECT_NODE);
		auto &select_node = child_node->Cast<SelectNode>();
		if (!select_node.where_clause) {
			select_node.where_clause = condition->Copy();
		} else {
			select_node.where_clause = make_uniq<ConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(select_node.where_clause), condition->Copy());
		}
		return child_node;
	} else {
		auto result = make_uniq<SelectNode>();
		result->select_list.push_back(make_uniq<StarExpression>());
		result->from_table = child->GetTableRef();
		result->where_clause = condition->Copy();
		return std::move(result);
	}
}

string FilterRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &FilterRelation::Columns() {
	return child->Columns();
}

string FilterRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Filter [" + condition->ToString() + "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb







namespace duckdb {

InsertRelation::InsertRelation(shared_ptr<Relation> child_p, string schema_name, string table_name)
    : Relation(child_p->context, RelationType::INSERT_RELATION), child(std::move(child_p)),
      schema_name(std::move(schema_name)), table_name(std::move(table_name)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement InsertRelation::Bind(Binder &binder) {
	InsertStatement stmt;
	auto select = make_uniq<SelectStatement>();
	select->node = child->GetQueryNode();

	stmt.schema = schema_name;
	stmt.table = table_name;
	stmt.select_statement = std::move(select);
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &InsertRelation::Columns() {
	return columns;
}

string InsertRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Insert\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb







namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      using_columns(std::move(using_columns_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> JoinRelation::GetTableRef() {
	auto join_ref = make_uniq<JoinRef>(join_ref_type);
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->using_columns = using_columns;
	join_ref->type = join_type;
	return std::move(join_ref);
}

const vector<ColumnDefinition> &JoinRelation::Columns() {
	return this->columns;
}

string JoinRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str += "Join " + EnumUtil::ToString(join_ref_type) + " " + EnumUtil::ToString(join_type);
	if (condition) {
		str += " " + condition->GetName();
	}

	return str + "\n" + left->ToString(depth + 1) + "\n" + right->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

LimitRelation::LimitRelation(shared_ptr<Relation> child_p, int64_t limit, int64_t offset)
    : Relation(child_p->context, RelationType::PROJECTION_RELATION), limit(limit), offset(offset),
      child(std::move(child_p)) {
	D_ASSERT(child.get() != this);
}

unique_ptr<QueryNode> LimitRelation::GetQueryNode() {
	auto child_node = child->GetQueryNode();
	auto limit_node = make_uniq<LimitModifier>();
	if (limit >= 0) {
		limit_node->limit = make_uniq<ConstantExpression>(Value::BIGINT(limit));
	}
	if (offset > 0) {
		limit_node->offset = make_uniq<ConstantExpression>(Value::BIGINT(offset));
	}

	child_node->modifiers.push_back(std::move(limit_node));
	return child_node;
}

string LimitRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &LimitRelation::Columns() {
	return child->Columns();
}

string LimitRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Limit " + to_string(limit);
	if (offset > 0) {
		str += " Offset " + to_string(offset);
	}
	str += "\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

OrderRelation::OrderRelation(shared_ptr<Relation> child_p, vector<OrderByNode> orders)
    : Relation(child_p->context, RelationType::ORDER_RELATION), orders(std::move(orders)), child(std::move(child_p)) {
	D_ASSERT(child.get() != this);
	// bind the expressions
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> OrderRelation::GetQueryNode() {
	auto select = make_uniq<SelectNode>();
	select->from_table = child->GetTableRef();
	select->select_list.push_back(make_uniq<StarExpression>());
	auto order_node = make_uniq<OrderModifier>();
	for (idx_t i = 0; i < orders.size(); i++) {
		order_node->orders.emplace_back(orders[i].type, orders[i].null_order, orders[i].expression->Copy());
	}
	select->modifiers.push_back(std::move(order_node));
	return std::move(select);
}

string OrderRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &OrderRelation::Columns() {
	return columns;
}

string OrderRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Order [";
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += orders[i].expression->ToString() + (orders[i].type == OrderType::ASCENDING ? " ASC" : " DESC");
	}
	str += "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb





namespace duckdb {

ProjectionRelation::ProjectionRelation(shared_ptr<Relation> child_p,
                                       vector<unique_ptr<ParsedExpression>> parsed_expressions, vector<string> aliases)
    : Relation(child_p->context, RelationType::PROJECTION_RELATION), expressions(std::move(parsed_expressions)),
      child(std::move(child_p)) {
	if (!aliases.empty()) {
		if (aliases.size() != expressions.size()) {
			throw ParserException("Aliases list length must match expression list length!");
		}
		for (idx_t i = 0; i < aliases.size(); i++) {
			expressions[i]->alias = aliases[i];
		}
	}
	// bind the expressions
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> ProjectionRelation::GetQueryNode() {
	auto child_ptr = child.get();
	while (child_ptr->InheritsColumnBindings()) {
		child_ptr = child_ptr->ChildRelation();
	}
	unique_ptr<QueryNode> result;
	if (child_ptr->type == RelationType::JOIN_RELATION) {
		// child node is a join: push projection into the child query node
		result = child->GetQueryNode();
	} else {
		// child node is not a join: create a new select node and push the child as a table reference
		auto select = make_uniq<SelectNode>();
		select->from_table = child->GetTableRef();
		result = std::move(select);
	}
	D_ASSERT(result->type == QueryNodeType::SELECT_NODE);
	auto &select_node = result->Cast<SelectNode>();
	select_node.aggregate_handling = AggregateHandling::NO_AGGREGATES_ALLOWED;
	select_node.select_list.clear();
	for (auto &expr : expressions) {
		select_node.select_list.push_back(expr->Copy());
	}
	return result;
}

string ProjectionRelation::GetAlias() {
	return child->GetAlias();
}

const vector<ColumnDefinition> &ProjectionRelation::Columns() {
	return columns;
}

string ProjectionRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Projection [";
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i != 0) {
			str += ", ";
		}
		str += expressions[i]->ToString() + " as " + expressions[i]->alias;
	}
	str += "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

QueryRelation::QueryRelation(const std::shared_ptr<ClientContext> &context, unique_ptr<SelectStatement> select_stmt_p,
                             string alias_p)
    : Relation(context, RelationType::QUERY_RELATION), select_stmt(std::move(select_stmt_p)),
      alias(std::move(alias_p)) {
	context->TryBindRelation(*this, this->columns);
}

QueryRelation::~QueryRelation() {
}

unique_ptr<SelectStatement> QueryRelation::ParseStatement(ClientContext &context, const string &query,
                                                          const string &error) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query);
	if (parser.statements.size() != 1) {
		throw ParserException(error);
	}
	if (parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException(error);
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

unique_ptr<SelectStatement> QueryRelation::GetSelectStatement() {
	return unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
}

unique_ptr<QueryNode> QueryRelation::GetQueryNode() {
	auto select = GetSelectStatement();
	return std::move(select->node);
}

unique_ptr<TableRef> QueryRelation::GetTableRef() {
	auto subquery_ref = make_uniq<SubqueryRef>(GetSelectStatement(), GetAlias());
	return std::move(subquery_ref);
}

string QueryRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &QueryRelation::Columns() {
	return columns;
}

string QueryRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Subquery";
}

} // namespace duckdb

















namespace duckdb {

ReadCSVRelation::ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file,
                                 vector<ColumnDefinition> columns_p, string alias_p)
    : TableFunctionRelation(context, "read_csv", {Value(csv_file)}, nullptr, false), alias(std::move(alias_p)),
      auto_detect(false) {

	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}

	columns = std::move(columns_p);

	child_list_t<Value> column_names;
	for (idx_t i = 0; i < columns.size(); i++) {
		column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
	}

	AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
}

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, const string &csv_file,
                                 named_parameter_map_t &&options, string alias_p)
    : TableFunctionRelation(context, "read_csv_auto", {Value(csv_file)}, nullptr, false), alias(std::move(alias_p)),
      auto_detect(true) {

	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}

	auto files = MultiFileReader::GetFileList(*context, csv_file, "CSV");
	D_ASSERT(!files.empty());

	auto &file_name = files[0];
	options["auto_detect"] = Value::BOOLEAN(true);
	CSVReaderOptions csv_options;
	csv_options.file_path = file_name;
	vector<string> empty;

	vector<LogicalType> unused_types;
	vector<string> unused_names;
	csv_options.FromNamedParameters(options, *context, unused_types, unused_names);
	// Run the auto-detect, populating the options with the detected settings

	auto bm_file_handle = BaseCSVReader::OpenCSV(*context, csv_options);
	auto buffer_manager = make_shared<CSVBufferManager>(*context, std::move(bm_file_handle), csv_options);
	CSVStateMachineCache state_machine_cache;
	CSVSniffer sniffer(csv_options, buffer_manager, state_machine_cache);
	auto sniffer_result = sniffer.SniffCSV();
	auto &types = sniffer_result.return_types;
	auto &names = sniffer_result.names;
	for (idx_t i = 0; i < types.size(); i++) {
		columns.emplace_back(names[i], types[i]);
	}

	//! Capture the options potentially set/altered by the auto detection phase
	csv_options.ToNamedParameters(options);

	// No need to auto-detect again
	options["auto_detect"] = Value::BOOLEAN(false);
	SetNamedParameters(std::move(options));
}

string ReadCSVRelation::GetAlias() {
	return alias;
}

} // namespace duckdb


namespace duckdb {

ReadJSONRelation::ReadJSONRelation(const shared_ptr<ClientContext> &context, string json_file_p,
                                   named_parameter_map_t options, bool auto_detect, string alias_p)
    : TableFunctionRelation(context, auto_detect ? "read_json_auto" : "read_json", {Value(json_file_p)},
                            std::move(options)),
      json_file(std::move(json_file_p)), alias(std::move(alias_p)) {

	if (alias.empty()) {
		alias = StringUtil::Split(json_file, ".")[0];
	}
}

string ReadJSONRelation::GetAlias() {
	return alias;
}

} // namespace duckdb





namespace duckdb {

SetOpRelation::SetOpRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, SetOperationType setop_type_p)
    : Relation(left_p->context, RelationType::SET_OPERATION_RELATION), left(std::move(left_p)),
      right(std::move(right_p)), setop_type(setop_type_p) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw Exception("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> SetOpRelation::GetQueryNode() {
	auto result = make_uniq<SetOperationNode>();
	if (setop_type == SetOperationType::EXCEPT || setop_type == SetOperationType::INTERSECT) {
		result->modifiers.push_back(make_uniq<DistinctModifier>());
	}
	result->left = left->GetQueryNode();
	result->right = right->GetQueryNode();
	result->setop_type = setop_type;
	return std::move(result);
}

string SetOpRelation::GetAlias() {
	return left->GetAlias();
}

const vector<ColumnDefinition> &SetOpRelation::Columns() {
	return this->columns;
}

string SetOpRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	switch (setop_type) {
	case SetOperationType::UNION:
		str += "Union";
		break;
	case SetOperationType::EXCEPT:
		str += "Except";
		break;
	case SetOperationType::INTERSECT:
		str += "Intersect";
		break;
	default:
		throw InternalException("Unknown setop type");
	}
	return str + "\n" + left->ToString(depth + 1) + right->ToString(depth + 1);
}

} // namespace duckdb




namespace duckdb {

SubqueryRelation::SubqueryRelation(shared_ptr<Relation> child_p, string alias_p)
    : Relation(child_p->context, RelationType::SUBQUERY_RELATION), child(std::move(child_p)),
      alias(std::move(alias_p)) {
	D_ASSERT(child.get() != this);
	vector<ColumnDefinition> dummy_columns;
	context.GetContext()->TryBindRelation(*this, dummy_columns);
}

unique_ptr<QueryNode> SubqueryRelation::GetQueryNode() {
	return child->GetQueryNode();
}

string SubqueryRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &SubqueryRelation::Columns() {
	return child->Columns();
}

string SubqueryRelation::ToString(idx_t depth) {
	return child->ToString(depth);
}

} // namespace duckdb













namespace duckdb {

void TableFunctionRelation::AddNamedParameter(const string &name, Value argument) {
	named_parameters[name] = std::move(argument);
}

void TableFunctionRelation::SetNamedParameters(named_parameter_map_t &&options) {
	D_ASSERT(named_parameters.empty());
	named_parameters = std::move(options);
}

TableFunctionRelation::TableFunctionRelation(const shared_ptr<ClientContext> &context, string name_p,
                                             vector<Value> parameters_p, named_parameter_map_t named_parameters,
                                             shared_ptr<Relation> input_relation_p, bool auto_init)
    : Relation(context, RelationType::TABLE_FUNCTION_RELATION), name(std::move(name_p)),
      parameters(std::move(parameters_p)), named_parameters(std::move(named_parameters)),
      input_relation(std::move(input_relation_p)), auto_initialize(auto_init) {
	InitializeColumns();
}

TableFunctionRelation::TableFunctionRelation(const shared_ptr<ClientContext> &context, string name_p,
                                             vector<Value> parameters_p, shared_ptr<Relation> input_relation_p,
                                             bool auto_init)
    : Relation(context, RelationType::TABLE_FUNCTION_RELATION), name(std::move(name_p)),
      parameters(std::move(parameters_p)), input_relation(std::move(input_relation_p)), auto_initialize(auto_init) {
	InitializeColumns();
}

void TableFunctionRelation::InitializeColumns() {
	if (!auto_initialize) {
		return;
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> TableFunctionRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> TableFunctionRelation::GetTableRef() {
	vector<unique_ptr<ParsedExpression>> children;
	if (input_relation) { // input relation becomes first parameter if present, always
		auto subquery = make_uniq<SubqueryExpression>();
		subquery->subquery = make_uniq<SelectStatement>();
		subquery->subquery->node = input_relation->GetQueryNode();
		subquery->subquery_type = SubqueryType::SCALAR;
		children.push_back(std::move(subquery));
	}
	for (auto &parameter : parameters) {
		children.push_back(make_uniq<ConstantExpression>(parameter));
	}

	for (auto &parameter : named_parameters) {
		// Hackity-hack some comparisons with column refs
		// This is all but pretty, basically the named parameter is the column, the table is empty because that's what
		// the function binder likes
		auto column_ref = make_uniq<ColumnRefExpression>(parameter.first);
		auto constant_value = make_uniq<ConstantExpression>(parameter.second);
		auto comparison = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(column_ref),
		                                                  std::move(constant_value));
		children.push_back(std::move(comparison));
	}

	auto table_function = make_uniq<TableFunctionRef>();
	auto function = make_uniq<FunctionExpression>(name, std::move(children));
	table_function->function = std::move(function);
	return std::move(table_function);
}

string TableFunctionRelation::GetAlias() {
	return name;
}

const vector<ColumnDefinition> &TableFunctionRelation::Columns() {
	return columns;
}

string TableFunctionRelation::ToString(idx_t depth) {
	string function_call = name + "(";
	for (idx_t i = 0; i < parameters.size(); i++) {
		if (i > 0) {
			function_call += ", ";
		}
		function_call += parameters[i].ToString();
	}
	function_call += ")";
	return RenderWhitespace(depth) + function_call;
}

} // namespace duckdb









namespace duckdb {

TableRelation::TableRelation(const std::shared_ptr<ClientContext> &context, unique_ptr<TableDescription> description)
    : Relation(context, RelationType::TABLE_RELATION), description(std::move(description)) {
}

unique_ptr<QueryNode> TableRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> TableRelation::GetTableRef() {
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->schema_name = description->schema;
	table_ref->table_name = description->table;
	return std::move(table_ref);
}

string TableRelation::GetAlias() {
	return description->table;
}

const vector<ColumnDefinition> &TableRelation::Columns() {
	return description->columns;
}

string TableRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Scan Table [" + description->table + "]";
}

static unique_ptr<ParsedExpression> ParseCondition(ClientContext &context, const string &condition) {
	if (!condition.empty()) {
		auto expression_list = Parser::ParseExpressionList(condition, context.GetParserOptions());
		if (expression_list.size() != 1) {
			throw ParserException("Expected a single expression as filter condition");
		}
		return std::move(expression_list[0]);
	} else {
		return nullptr;
	}
}

void TableRelation::Update(const string &update_list, const string &condition) {
	vector<string> update_columns;
	vector<unique_ptr<ParsedExpression>> expressions;
	auto cond = ParseCondition(*context.GetContext(), condition);
	Parser::ParseUpdateList(update_list, update_columns, expressions, context.GetContext()->GetParserOptions());
	auto update = make_shared<UpdateRelation>(context, std::move(cond), description->schema, description->table,
	                                          std::move(update_columns), std::move(expressions));
	update->Execute();
}

void TableRelation::Delete(const string &condition) {
	auto cond = ParseCondition(*context.GetContext(), condition);
	auto del = make_shared<DeleteRelation>(context, std::move(cond), description->schema, description->table);
	del->Execute();
}

} // namespace duckdb






namespace duckdb {

UpdateRelation::UpdateRelation(ClientContextWrapper &context, unique_ptr<ParsedExpression> condition_p,
                               string schema_name_p, string table_name_p, vector<string> update_columns_p,
                               vector<unique_ptr<ParsedExpression>> expressions_p)
    : Relation(context, RelationType::UPDATE_RELATION), condition(std::move(condition_p)),
      schema_name(std::move(schema_name_p)), table_name(std::move(table_name_p)),
      update_columns(std::move(update_columns_p)), expressions(std::move(expressions_p)) {
	D_ASSERT(update_columns.size() == expressions.size());
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement UpdateRelation::Bind(Binder &binder) {
	auto basetable = make_uniq<BaseTableRef>();
	basetable->schema_name = schema_name;
	basetable->table_name = table_name;

	UpdateStatement stmt;
	stmt.set_info = make_uniq<UpdateSetInfo>();

	stmt.set_info->condition = condition ? condition->Copy() : nullptr;
	stmt.table = std::move(basetable);
	stmt.set_info->columns = update_columns;
	for (auto &expr : expressions) {
		stmt.set_info->expressions.push_back(expr->Copy());
	}
	return binder.Bind(stmt.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &UpdateRelation::Columns() {
	return columns;
}

string UpdateRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "UPDATE " + table_name + " SET\n";
	for (idx_t i = 0; i < expressions.size(); i++) {
		str += update_columns[i] + " = " + expressions[i]->ToString() + "\n";
	}
	if (condition) {
		str += "WHERE " + condition->ToString() + "\n";
	}
	return str;
}

} // namespace duckdb








namespace duckdb {

ValueRelation::ValueRelation(const std::shared_ptr<ClientContext> &context, const vector<vector<Value>> &values,
                             vector<string> names_p, string alias_p)
    : Relation(context, RelationType::VALUE_LIST_RELATION), names(std::move(names_p)), alias(std::move(alias_p)) {
	// create constant expressions for the values
	for (idx_t row_idx = 0; row_idx < values.size(); row_idx++) {
		auto &list = values[row_idx];
		vector<unique_ptr<ParsedExpression>> expressions;
		for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			expressions.push_back(make_uniq<ConstantExpression>(list[col_idx]));
		}
		this->expressions.push_back(std::move(expressions));
	}
	context->TryBindRelation(*this, this->columns);
}

ValueRelation::ValueRelation(const std::shared_ptr<ClientContext> &context, const string &values_list,
                             vector<string> names_p, string alias_p)
    : Relation(context, RelationType::VALUE_LIST_RELATION), names(std::move(names_p)), alias(std::move(alias_p)) {
	this->expressions = Parser::ParseValuesList(values_list, context->GetParserOptions());
	context->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> ValueRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> ValueRelation::GetTableRef() {
	auto table_ref = make_uniq<ExpressionListRef>();
	// set the expected types/names
	if (columns.empty()) {
		// no columns yet: only set up names
		for (idx_t i = 0; i < names.size(); i++) {
			table_ref->expected_names.push_back(names[i]);
		}
	} else {
		for (idx_t i = 0; i < columns.size(); i++) {
			table_ref->expected_names.push_back(columns[i].Name());
			table_ref->expected_types.push_back(columns[i].Type());
			D_ASSERT(names.size() == 0 || columns[i].Name() == names[i]);
		}
	}
	// copy the expressions
	for (auto &expr_list : expressions) {
		vector<unique_ptr<ParsedExpression>> copied_list;
		copied_list.reserve(expr_list.size());
		for (auto &expr : expr_list) {
			copied_list.push_back(expr->Copy());
		}
		table_ref->values.push_back(std::move(copied_list));
	}
	table_ref->alias = GetAlias();
	return std::move(table_ref);
}

string ValueRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &ValueRelation::Columns() {
	return columns;
}

string ValueRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Values ";
	for (idx_t row_idx = 0; row_idx < expressions.size(); row_idx++) {
		auto &list = expressions[row_idx];
		str += row_idx > 0 ? ", (" : "(";
		for (idx_t col_idx = 0; col_idx < list.size(); col_idx++) {
			str += col_idx > 0 ? ", " : "";
			str += list[col_idx]->ToString();
		}
		str += ")";
	}
	str += "\n";
	return str;
}

} // namespace duckdb







namespace duckdb {

ViewRelation::ViewRelation(const std::shared_ptr<ClientContext> &context, string schema_name_p, string view_name_p)
    : Relation(context, RelationType::VIEW_RELATION), schema_name(std::move(schema_name_p)),
      view_name(std::move(view_name_p)) {
	context->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> ViewRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> ViewRelation::GetTableRef() {
	auto table_ref = make_uniq<BaseTableRef>();
	table_ref->schema_name = schema_name;
	table_ref->table_name = view_name;
	return std::move(table_ref);
}

string ViewRelation::GetAlias() {
	return view_name;
}

const vector<ColumnDefinition> &ViewRelation::Columns() {
	return columns;
}

string ViewRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "View [" + view_name + "]";
}

} // namespace duckdb






namespace duckdb {

WriteCSVRelation::WriteCSVRelation(shared_ptr<Relation> child_p, string csv_file_p,
                                   case_insensitive_map_t<vector<Value>> options_p)
    : Relation(child_p->context, RelationType::WRITE_CSV_RELATION), child(std::move(child_p)),
      csv_file(std::move(csv_file_p)), options(std::move(options_p)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement WriteCSVRelation::Bind(Binder &binder) {
	CopyStatement copy;
	copy.select_statement = child->GetQueryNode();
	auto info = make_uniq<CopyInfo>();
	info->is_from = false;
	info->file_path = csv_file;
	info->format = "csv";
	info->options = options;
	copy.info = std::move(info);
	return binder.Bind(copy.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &WriteCSVRelation::Columns() {
	return columns;
}

string WriteCSVRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Write To CSV [" + csv_file + "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb






namespace duckdb {

WriteParquetRelation::WriteParquetRelation(shared_ptr<Relation> child_p, string parquet_file_p,
                                           case_insensitive_map_t<vector<Value>> options_p)
    : Relation(child_p->context, RelationType::WRITE_PARQUET_RELATION), child(std::move(child_p)),
      parquet_file(std::move(parquet_file_p)), options(std::move(options_p)) {
	context.GetContext()->TryBindRelation(*this, this->columns);
}

BoundStatement WriteParquetRelation::Bind(Binder &binder) {
	CopyStatement copy;
	copy.select_statement = child->GetQueryNode();
	auto info = make_uniq<CopyInfo>();
	info->is_from = false;
	info->file_path = parquet_file;
	info->format = "parquet";
	info->options = options;
	copy.info = std::move(info);
	return binder.Bind(copy.Cast<SQLStatement>());
}

const vector<ColumnDefinition> &WriteParquetRelation::Columns() {
	return columns;
}

string WriteParquetRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth) + "Write To Parquet [" + parquet_file + "]\n";
	return str + child->ToString(depth + 1);
}

} // namespace duckdb





























namespace duckdb {

shared_ptr<Relation> Relation::Project(const string &select_list) {
	return Project(select_list, vector<string>());
}

shared_ptr<Relation> Relation::Project(const string &expression, const string &alias) {
	return Project(expression, vector<string>({alias}));
}

shared_ptr<Relation> Relation::Project(const string &select_list, const vector<string> &aliases) {
	auto expressions = Parser::ParseExpressionList(select_list, context.GetContext()->GetParserOptions());
	return make_shared<ProjectionRelation>(shared_from_this(), std::move(expressions), aliases);
}

shared_ptr<Relation> Relation::Project(const vector<string> &expressions) {
	vector<string> aliases;
	return Project(expressions, aliases);
}

shared_ptr<Relation> Relation::Project(vector<unique_ptr<ParsedExpression>> expressions,
                                       const vector<string> &aliases) {
	return make_shared<ProjectionRelation>(shared_from_this(), std::move(expressions), aliases);
}

static vector<unique_ptr<ParsedExpression>> StringListToExpressionList(ClientContext &context,
                                                                       const vector<string> &expressions) {
	if (expressions.empty()) {
		throw ParserException("Zero expressions provided");
	}
	vector<unique_ptr<ParsedExpression>> result_list;
	for (auto &expr : expressions) {
		auto expression_list = Parser::ParseExpressionList(expr, context.GetParserOptions());
		if (expression_list.size() != 1) {
			throw ParserException("Expected a single expression in the expression list");
		}
		result_list.push_back(std::move(expression_list[0]));
	}
	return result_list;
}

shared_ptr<Relation> Relation::Project(const vector<string> &expressions, const vector<string> &aliases) {
	auto result_list = StringListToExpressionList(*context.GetContext(), expressions);
	return make_shared<ProjectionRelation>(shared_from_this(), std::move(result_list), aliases);
}

shared_ptr<Relation> Relation::Filter(const string &expression) {
	auto expression_list = Parser::ParseExpressionList(expression, context.GetContext()->GetParserOptions());
	if (expression_list.size() != 1) {
		throw ParserException("Expected a single expression as filter condition");
	}
	return Filter(std::move(expression_list[0]));
}

shared_ptr<Relation> Relation::Filter(unique_ptr<ParsedExpression> expression) {
	return make_shared<FilterRelation>(shared_from_this(), std::move(expression));
}

shared_ptr<Relation> Relation::Filter(const vector<string> &expressions) {
	// if there are multiple expressions, we AND them together
	auto expression_list = StringListToExpressionList(*context.GetContext(), expressions);
	D_ASSERT(!expression_list.empty());

	auto expr = std::move(expression_list[0]);
	for (idx_t i = 1; i < expression_list.size(); i++) {
		expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
		                                        std::move(expression_list[i]));
	}
	return make_shared<FilterRelation>(shared_from_this(), std::move(expr));
}

shared_ptr<Relation> Relation::Limit(int64_t limit, int64_t offset) {
	return make_shared<LimitRelation>(shared_from_this(), limit, offset);
}

shared_ptr<Relation> Relation::Order(const string &expression) {
	auto order_list = Parser::ParseOrderList(expression, context.GetContext()->GetParserOptions());
	return Order(std::move(order_list));
}

shared_ptr<Relation> Relation::Order(vector<OrderByNode> order_list) {
	return make_shared<OrderRelation>(shared_from_this(), std::move(order_list));
}

shared_ptr<Relation> Relation::Order(const vector<string> &expressions) {
	if (expressions.empty()) {
		throw ParserException("Zero ORDER BY expressions provided");
	}
	vector<OrderByNode> order_list;
	for (auto &expression : expressions) {
		auto inner_list = Parser::ParseOrderList(expression, context.GetContext()->GetParserOptions());
		if (inner_list.size() != 1) {
			throw ParserException("Expected a single ORDER BY expression in the expression list");
		}
		order_list.push_back(std::move(inner_list[0]));
	}
	return Order(std::move(order_list));
}

shared_ptr<Relation> Relation::Join(const shared_ptr<Relation> &other, const string &condition, JoinType type,
                                    JoinRefType ref_type) {
	auto expression_list = Parser::ParseExpressionList(condition, context.GetContext()->GetParserOptions());
	D_ASSERT(!expression_list.empty());
	return Join(other, std::move(expression_list), type, ref_type);
}

shared_ptr<Relation> Relation::Join(const shared_ptr<Relation> &other,
                                    vector<unique_ptr<ParsedExpression>> expression_list, JoinType type,
                                    JoinRefType ref_type) {
	if (expression_list.size() > 1 || expression_list[0]->type == ExpressionType::COLUMN_REF) {
		// multiple columns or single column ref: the condition is a USING list
		vector<string> using_columns;
		for (auto &expr : expression_list) {
			if (expr->type != ExpressionType::COLUMN_REF) {
				throw ParserException("Expected a single expression as join condition");
			}
			auto &colref = expr->Cast<ColumnRefExpression>();
			if (colref.IsQualified()) {
				throw ParserException("Expected unqualified column for column in USING clause");
			}
			using_columns.push_back(colref.column_names[0]);
		}
		return make_shared<JoinRelation>(shared_from_this(), other, std::move(using_columns), type, ref_type);
	} else {
		// single expression that is not a column reference: use the expression as a join condition
		return make_shared<JoinRelation>(shared_from_this(), other, std::move(expression_list[0]), type, ref_type);
	}
}

shared_ptr<Relation> Relation::CrossProduct(const shared_ptr<Relation> &other, JoinRefType join_ref_type) {
	return make_shared<CrossProductRelation>(shared_from_this(), other, join_ref_type);
}

shared_ptr<Relation> Relation::Union(const shared_ptr<Relation> &other) {
	return make_shared<SetOpRelation>(shared_from_this(), other, SetOperationType::UNION);
}

shared_ptr<Relation> Relation::Except(const shared_ptr<Relation> &other) {
	return make_shared<SetOpRelation>(shared_from_this(), other, SetOperationType::EXCEPT);
}

shared_ptr<Relation> Relation::Intersect(const shared_ptr<Relation> &other) {
	return make_shared<SetOpRelation>(shared_from_this(), other, SetOperationType::INTERSECT);
}

shared_ptr<Relation> Relation::Distinct() {
	return make_shared<DistinctRelation>(shared_from_this());
}

shared_ptr<Relation> Relation::Alias(const string &alias) {
	return make_shared<SubqueryRelation>(shared_from_this(), alias);
}

shared_ptr<Relation> Relation::Aggregate(const string &aggregate_list) {
	auto expression_list = Parser::ParseExpressionList(aggregate_list, context.GetContext()->GetParserOptions());
	return make_shared<AggregateRelation>(shared_from_this(), std::move(expression_list));
}

shared_ptr<Relation> Relation::Aggregate(const string &aggregate_list, const string &group_list) {
	auto expression_list = Parser::ParseExpressionList(aggregate_list, context.GetContext()->GetParserOptions());
	auto groups = Parser::ParseGroupByList(group_list, context.GetContext()->GetParserOptions());
	return make_shared<AggregateRelation>(shared_from_this(), std::move(expression_list), std::move(groups));
}

shared_ptr<Relation> Relation::Aggregate(const vector<string> &aggregates) {
	auto aggregate_list = StringListToExpressionList(*context.GetContext(), aggregates);
	return make_shared<AggregateRelation>(shared_from_this(), std::move(aggregate_list));
}

shared_ptr<Relation> Relation::Aggregate(const vector<string> &aggregates, const vector<string> &groups) {
	auto aggregate_list = StringUtil::Join(aggregates, ", ");
	auto group_list = StringUtil::Join(groups, ", ");
	return this->Aggregate(aggregate_list, group_list);
}

shared_ptr<Relation> Relation::Aggregate(vector<unique_ptr<ParsedExpression>> expressions, const string &group_list) {
	auto groups = Parser::ParseGroupByList(group_list, context.GetContext()->GetParserOptions());
	return make_shared<AggregateRelation>(shared_from_this(), std::move(expressions), std::move(groups));
}

string Relation::GetAlias() {
	return "relation";
}

unique_ptr<TableRef> Relation::GetTableRef() {
	auto select = make_uniq<SelectStatement>();
	select->node = GetQueryNode();
	return make_uniq<SubqueryRef>(std::move(select), GetAlias());
}

unique_ptr<QueryResult> Relation::Execute() {
	return context.GetContext()->Execute(shared_from_this());
}

unique_ptr<QueryResult> Relation::ExecuteOrThrow() {
	auto res = Execute();
	D_ASSERT(res);
	if (res->HasError()) {
		res->ThrowError();
	}
	return res;
}

BoundStatement Relation::Bind(Binder &binder) {
	SelectStatement stmt;
	stmt.node = GetQueryNode();
	return binder.Bind(stmt.Cast<SQLStatement>());
}

shared_ptr<Relation> Relation::InsertRel(const string &schema_name, const string &table_name) {
	return make_shared<InsertRelation>(shared_from_this(), schema_name, table_name);
}

void Relation::Insert(const string &table_name) {
	Insert(INVALID_SCHEMA, table_name);
}

void Relation::Insert(const string &schema_name, const string &table_name) {
	auto insert = InsertRel(schema_name, table_name);
	auto res = insert->Execute();
	if (res->HasError()) {
		const string prepended_message = "Failed to insert into table '" + table_name + "': ";
		res->ThrowError(prepended_message);
	}
}

void Relation::Insert(const vector<vector<Value>> &values) {
	vector<string> column_names;
	auto rel = make_shared<ValueRelation>(context.GetContext(), values, std::move(column_names), "values");
	rel->Insert(GetAlias());
}

shared_ptr<Relation> Relation::CreateRel(const string &schema_name, const string &table_name) {
	return make_shared<CreateTableRelation>(shared_from_this(), schema_name, table_name);
}

void Relation::Create(const string &table_name) {
	Create(INVALID_SCHEMA, table_name);
}

void Relation::Create(const string &schema_name, const string &table_name) {
	auto create = CreateRel(schema_name, table_name);
	auto res = create->Execute();
	if (res->HasError()) {
		const string prepended_message = "Failed to create table '" + table_name + "': ";
		res->ThrowError(prepended_message);
	}
}

shared_ptr<Relation> Relation::WriteCSVRel(const string &csv_file, case_insensitive_map_t<vector<Value>> options) {
	return std::make_shared<duckdb::WriteCSVRelation>(shared_from_this(), csv_file, std::move(options));
}

void Relation::WriteCSV(const string &csv_file, case_insensitive_map_t<vector<Value>> options) {
	auto write_csv = WriteCSVRel(csv_file, std::move(options));
	auto res = write_csv->Execute();
	if (res->HasError()) {
		const string prepended_message = "Failed to write '" + csv_file + "': ";
		res->ThrowError(prepended_message);
	}
}

shared_ptr<Relation> Relation::WriteParquetRel(const string &parquet_file,
                                               case_insensitive_map_t<vector<Value>> options) {
	auto write_parquet =
	    std::make_shared<duckdb::WriteParquetRelation>(shared_from_this(), parquet_file, std::move(options));
	return std::move(write_parquet);
}

void Relation::WriteParquet(const string &parquet_file, case_insensitive_map_t<vector<Value>> options) {
	auto write_parquet = WriteParquetRel(parquet_file, std::move(options));
	auto res = write_parquet->Execute();
	if (res->HasError()) {
		const string prepended_message = "Failed to write '" + parquet_file + "': ";
		res->ThrowError(prepended_message);
	}
}

shared_ptr<Relation> Relation::CreateView(const string &name, bool replace, bool temporary) {
	return CreateView(INVALID_SCHEMA, name, replace, temporary);
}

shared_ptr<Relation> Relation::CreateView(const string &schema_name, const string &name, bool replace, bool temporary) {
	auto view = make_shared<CreateViewRelation>(shared_from_this(), schema_name, name, replace, temporary);
	auto res = view->Execute();
	if (res->HasError()) {
		const string prepended_message = "Failed to create view '" + name + "': ";
		res->ThrowError(prepended_message);
	}
	return shared_from_this();
}

unique_ptr<QueryResult> Relation::Query(const string &sql) {
	return context.GetContext()->Query(sql, false);
}

unique_ptr<QueryResult> Relation::Query(const string &name, const string &sql) {
	CreateView(name);
	return Query(sql);
}

unique_ptr<QueryResult> Relation::Explain(ExplainType type) {
	auto explain = make_shared<ExplainRelation>(shared_from_this(), type);
	return explain->Execute();
}

void Relation::Update(const string &update, const string &condition) {
	throw Exception("UPDATE can only be used on base tables!");
}

void Relation::Delete(const string &condition) {
	throw Exception("DELETE can only be used on base tables!");
}

shared_ptr<Relation> Relation::TableFunction(const std::string &fname, const vector<Value> &values,
                                             const named_parameter_map_t &named_parameters) {
	return make_shared<TableFunctionRelation>(context.GetContext(), fname, values, named_parameters,
	                                          shared_from_this());
}

shared_ptr<Relation> Relation::TableFunction(const std::string &fname, const vector<Value> &values) {
	return make_shared<TableFunctionRelation>(context.GetContext(), fname, values, shared_from_this());
}

string Relation::ToString() {
	string str;
	str += "---------------------\n";
	str += "--- Relation Tree ---\n";
	str += "---------------------\n";
	str += ToString(0);
	str += "\n\n";
	str += "---------------------\n";
	str += "-- Result Columns  --\n";
	str += "---------------------\n";
	auto &cols = Columns();
	for (idx_t i = 0; i < cols.size(); i++) {
		str += "- " + cols[i].Name() + " (" + cols[i].Type().ToString() + ")\n";
	}
	return str;
}

// LCOV_EXCL_START
unique_ptr<QueryNode> Relation::GetQueryNode() {
	throw InternalException("Cannot create a query node from this node type");
}

void Relation::Head(idx_t limit) {
	auto limit_node = Limit(limit);
	limit_node->Execute()->Print();
}
// LCOV_EXCL_STOP

void Relation::Print() {
	Printer::Print(ToString());
}

string Relation::RenderWhitespace(idx_t depth) {
	return string(depth * 2, ' ');
}

vector<shared_ptr<ExternalDependency>> Relation::GetAllDependencies() {
	vector<shared_ptr<ExternalDependency>> all_dependencies;
	Relation *cur = this;
	while (cur) {
		if (cur->extra_dependencies) {
			all_dependencies.push_back(cur->extra_dependencies);
		}
		cur = cur->ChildRelation();
	}
	return all_dependencies;
}

} // namespace duckdb

















namespace duckdb {

//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void AccessModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (db) {
		throw InvalidInputException("Cannot change access_mode setting while database is running - it must be set when "
		                            "opening or attaching the database");
	}
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "automatic") {
		config.options.access_mode = AccessMode::AUTOMATIC;
	} else if (parameter == "read_only") {
		config.options.access_mode = AccessMode::READ_ONLY;
	} else if (parameter == "read_write") {
		config.options.access_mode = AccessMode::READ_WRITE;
	} else {
		throw InvalidInputException(
		    "Unrecognized parameter for option ACCESS_MODE \"%s\". Expected READ_ONLY or READ_WRITE.", parameter);
	}
}

void AccessModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.access_mode = DBConfig().options.access_mode;
}

Value AccessModeSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.access_mode) {
	case AccessMode::AUTOMATIC:
		return "automatic";
	case AccessMode::READ_ONLY:
		return "read_only";
	case AccessMode::READ_WRITE:
		return "read_write";
	default:
		throw InternalException("Unknown access mode setting");
	}
}

//===--------------------------------------------------------------------===//
// Checkpoint Threshold
//===--------------------------------------------------------------------===//
void CheckpointThresholdSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	idx_t new_limit = DBConfig::ParseMemoryLimit(input.ToString());
	config.options.checkpoint_wal_size = new_limit;
}

void CheckpointThresholdSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.checkpoint_wal_size = DBConfig().options.checkpoint_wal_size;
}

Value CheckpointThresholdSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.checkpoint_wal_size));
}

//===--------------------------------------------------------------------===//
// Debug Checkpoint Abort
//===--------------------------------------------------------------------===//
void DebugCheckpointAbort::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto checkpoint_abort = StringUtil::Lower(input.ToString());
	if (checkpoint_abort == "none") {
		config.options.checkpoint_abort = CheckpointAbort::NO_ABORT;
	} else if (checkpoint_abort == "before_truncate") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE;
	} else if (checkpoint_abort == "before_header") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER;
	} else if (checkpoint_abort == "after_free_list_write") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE;
	} else {
		throw ParserException(
		    "Unrecognized option for PRAGMA debug_checkpoint_abort, expected none, before_truncate or before_header");
	}
}

void DebugCheckpointAbort::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.checkpoint_abort = DBConfig().options.checkpoint_abort;
}

Value DebugCheckpointAbort::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	auto setting = config.options.checkpoint_abort;
	switch (setting) {
	case CheckpointAbort::NO_ABORT:
		return "none";
	case CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE:
		return "before_truncate";
	case CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER:
		return "before_header";
	case CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE:
		return "after_free_list_write";
	default:
		throw InternalException("Type not implemented for CheckpointAbort");
	}
}

//===--------------------------------------------------------------------===//
// Debug Force External
//===--------------------------------------------------------------------===//
void DebugForceExternal::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_external = ClientConfig().force_external;
}

void DebugForceExternal::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_external = input.GetValue<bool>();
}

Value DebugForceExternal::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_external);
}

//===--------------------------------------------------------------------===//
// Debug Force NoCrossProduct
//===--------------------------------------------------------------------===//
void DebugForceNoCrossProduct::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_no_cross_product = ClientConfig().force_no_cross_product;
}

void DebugForceNoCrossProduct::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_no_cross_product = input.GetValue<bool>();
}

Value DebugForceNoCrossProduct::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_no_cross_product);
}

//===--------------------------------------------------------------------===//
// Ordered Aggregate Threshold
//===--------------------------------------------------------------------===//
void OrderedAggregateThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).ordered_aggregate_threshold = ClientConfig().ordered_aggregate_threshold;
}

void OrderedAggregateThreshold::SetLocal(ClientContext &context, const Value &input) {
	const auto param = input.GetValue<uint64_t>();
	if (param <= 0) {
		throw ParserException("Invalid option for PRAGMA ordered_aggregate_threshold, value must be positive");
	}
	ClientConfig::GetConfig(context).ordered_aggregate_threshold = param;
}

Value OrderedAggregateThreshold::GetSetting(ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).ordered_aggregate_threshold);
}

//===--------------------------------------------------------------------===//
// Debug Window Mode
//===--------------------------------------------------------------------===//
void DebugWindowMode::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto param = StringUtil::Lower(input.ToString());
	if (param == "window") {
		config.options.window_mode = WindowAggregationMode::WINDOW;
	} else if (param == "combine") {
		config.options.window_mode = WindowAggregationMode::COMBINE;
	} else if (param == "separate") {
		config.options.window_mode = WindowAggregationMode::SEPARATE;
	} else {
		throw ParserException("Unrecognized option for PRAGMA debug_window_mode, expected window, combine or separate");
	}
}

void DebugWindowMode::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.window_mode = DBConfig().options.window_mode;
}

Value DebugWindowMode::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Debug AsOf Join
//===--------------------------------------------------------------------===//
void DebugAsOfIEJoin::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_asof_iejoin = ClientConfig().force_asof_iejoin;
}

void DebugAsOfIEJoin::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_asof_iejoin = input.GetValue<bool>();
}

Value DebugAsOfIEJoin::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_asof_iejoin);
}

//===--------------------------------------------------------------------===//
// Prefer Range Joins
//===--------------------------------------------------------------------===//
void PreferRangeJoins::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).prefer_range_joins = ClientConfig().prefer_range_joins;
}

void PreferRangeJoins::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).prefer_range_joins = input.GetValue<bool>();
}

Value PreferRangeJoins::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).prefer_range_joins);
}

//===--------------------------------------------------------------------===//
// Default Collation
//===--------------------------------------------------------------------===//
void DefaultCollationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	config.options.collation = parameter;
}

void DefaultCollationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.collation = DBConfig().options.collation;
}

void DefaultCollationSetting::ResetLocal(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	config.options.collation = DBConfig().options.collation;
}

void DefaultCollationSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	// bind the collation to verify that it exists
	ExpressionBinder::TestCollation(context, parameter);
	auto &config = DBConfig::GetConfig(context);
	config.options.collation = parameter;
}

Value DefaultCollationSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.collation);
}

//===--------------------------------------------------------------------===//
// Default Order
//===--------------------------------------------------------------------===//
void DefaultOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "ascending" || parameter == "asc") {
		config.options.default_order_type = OrderType::ASCENDING;
	} else if (parameter == "descending" || parameter == "desc") {
		config.options.default_order_type = OrderType::DESCENDING;
	} else {
		throw InvalidInputException("Unrecognized parameter for option DEFAULT_ORDER \"%s\". Expected ASC or DESC.",
		                            parameter);
	}
}

void DefaultOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_order_type = DBConfig().options.default_order_type;
}

Value DefaultOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.default_order_type) {
	case OrderType::ASCENDING:
		return "asc";
	case OrderType::DESCENDING:
		return "desc";
	default:
		throw InternalException("Unknown order type setting");
	}
}

//===--------------------------------------------------------------------===//
// Default Null Order
//===--------------------------------------------------------------------===//
void DefaultNullOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());

	if (parameter == "nulls_first" || parameter == "nulls first" || parameter == "null first" || parameter == "first") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_FIRST;
	} else if (parameter == "nulls_last" || parameter == "nulls last" || parameter == "null last" ||
	           parameter == "last") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_LAST;
	} else if (parameter == "nulls_first_on_asc_last_on_desc" || parameter == "sqlite" || parameter == "mysql") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC;
	} else if (parameter == "nulls_last_on_asc_first_on_desc" || parameter == "postgres") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC;
	} else {
		throw ParserException("Unrecognized parameter for option NULL_ORDER \"%s\", expected either NULLS FIRST, NULLS "
		                      "LAST, SQLite, MySQL or Postgres",
		                      parameter);
	}
}

void DefaultNullOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_null_order = DBConfig().options.default_null_order;
}

Value DefaultNullOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.default_null_order) {
	case DefaultOrderByNullType::NULLS_FIRST:
		return "nulls_first";
	case DefaultOrderByNullType::NULLS_LAST:
		return "nulls_last";
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		return "nulls_first_on_asc_last_on_desc";
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		return "nulls_last_on_asc_first_on_desc";
	default:
		throw InternalException("Unknown null order setting");
	}
}

//===--------------------------------------------------------------------===//
// Disabled File Systems
//===--------------------------------------------------------------------===//
void DisabledFileSystemsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (!db) {
		throw InternalException("disabled_filesystems can only be set in an active database");
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	auto list = StringUtil::Split(input.ToString(), ",");
	fs.SetDisabledFileSystems(list);
}

void DisabledFileSystemsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (!db) {
		throw InternalException("disabled_filesystems can only be set in an active database");
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	fs.SetDisabledFileSystems(vector<string>());
}

Value DisabledFileSystemsSetting::GetSetting(ClientContext &context) {
	return Value("");
}

//===--------------------------------------------------------------------===//
// Disabled Optimizer
//===--------------------------------------------------------------------===//
void DisabledOptimizersSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto list = StringUtil::Split(input.ToString(), ",");
	set<OptimizerType> disabled_optimizers;
	for (auto &entry : list) {
		auto param = StringUtil::Lower(entry);
		StringUtil::Trim(param);
		if (param.empty()) {
			continue;
		}
		disabled_optimizers.insert(OptimizerTypeFromString(param));
	}
	config.options.disabled_optimizers = std::move(disabled_optimizers);
}

void DisabledOptimizersSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.disabled_optimizers = DBConfig().options.disabled_optimizers;
}

Value DisabledOptimizersSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	string result;
	for (auto &optimizer : config.options.disabled_optimizers) {
		if (!result.empty()) {
			result += ",";
		}
		result += OptimizerTypeToString(optimizer);
	}
	return Value(result);
}

//===--------------------------------------------------------------------===//
// Enable External Access
//===--------------------------------------------------------------------===//
void EnableExternalAccessSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	if (db && new_value) {
		throw InvalidInputException("Cannot change enable_external_access setting while database is running");
	}
	config.options.enable_external_access = new_value;
}

void EnableExternalAccessSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change enable_external_access setting while database is running");
	}
	config.options.enable_external_access = DBConfig().options.enable_external_access;
}

Value EnableExternalAccessSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_external_access);
}

//===--------------------------------------------------------------------===//
// Enable FSST Vectors
//===--------------------------------------------------------------------===//
void EnableFSSTVectors::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_fsst_vectors = input.GetValue<bool>();
}

void EnableFSSTVectors::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_fsst_vectors = DBConfig().options.enable_fsst_vectors;
}

Value EnableFSSTVectors::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_fsst_vectors);
}

//===--------------------------------------------------------------------===//
// Allow Unsigned Extensions
//===--------------------------------------------------------------------===//
void AllowUnsignedExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	if (db && new_value) {
		throw InvalidInputException("Cannot change allow_unsigned_extensions setting while database is running");
	}
	config.options.allow_unsigned_extensions = new_value;
}

void AllowUnsignedExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change allow_unsigned_extensions setting while database is running");
	}
	config.options.allow_unsigned_extensions = DBConfig().options.allow_unsigned_extensions;
}

Value AllowUnsignedExtensionsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_unsigned_extensions);
}

//===--------------------------------------------------------------------===//
// Enable Object Cache
//===--------------------------------------------------------------------===//
void EnableObjectCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.object_cache_enable = input.GetValue<bool>();
}

void EnableObjectCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.object_cache_enable = DBConfig().options.object_cache_enable;
}

Value EnableObjectCacheSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.object_cache_enable);
}

//===--------------------------------------------------------------------===//
// Enable HTTP Metadata Cache
//===--------------------------------------------------------------------===//
void EnableHTTPMetadataCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.http_metadata_cache_enable = input.GetValue<bool>();
}

void EnableHTTPMetadataCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.http_metadata_cache_enable = DBConfig().options.http_metadata_cache_enable;
}

Value EnableHTTPMetadataCacheSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.http_metadata_cache_enable);
}

//===--------------------------------------------------------------------===//
// Enable Profiling
//===--------------------------------------------------------------------===//
void EnableProfilingSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.profiler_print_format = ClientConfig().profiler_print_format;
	config.enable_profiler = ClientConfig().enable_profiler;
	config.emit_profiler_output = ClientConfig().emit_profiler_output;
}

void EnableProfilingSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());

	auto &config = ClientConfig::GetConfig(context);
	if (parameter == "json") {
		config.profiler_print_format = ProfilerPrintFormat::JSON;
	} else if (parameter == "query_tree") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else if (parameter == "query_tree_optimizer") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;
	} else {
		throw ParserException(
		    "Unrecognized print format %s, supported formats: [json, query_tree, query_tree_optimizer]", parameter);
	}
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

Value EnableProfilingSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_profiler) {
		return Value();
	}
	switch (config.profiler_print_format) {
	case ProfilerPrintFormat::JSON:
		return Value("json");
	case ProfilerPrintFormat::QUERY_TREE:
		return Value("query_tree");
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return Value("query_tree_optimizer");
	default:
		throw InternalException("Unsupported profiler print format");
	}
}

//===--------------------------------------------------------------------===//
// Custom Extension Repository
//===--------------------------------------------------------------------===//
void CustomExtensionRepository::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).custom_extension_repo = ClientConfig().custom_extension_repo;
}

void CustomExtensionRepository::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).custom_extension_repo = StringUtil::Lower(input.ToString());
}

Value CustomExtensionRepository::GetSetting(ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).custom_extension_repo);
}

//===--------------------------------------------------------------------===//
// Autoload Extension Repository
//===--------------------------------------------------------------------===//
void AutoloadExtensionRepository::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).autoinstall_extension_repo = ClientConfig().autoinstall_extension_repo;
}

void AutoloadExtensionRepository::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).autoinstall_extension_repo = StringUtil::Lower(input.ToString());
}

Value AutoloadExtensionRepository::GetSetting(ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).autoinstall_extension_repo);
}

//===--------------------------------------------------------------------===//
// Autoinstall Known Extensions
//===--------------------------------------------------------------------===//
void AutoinstallKnownExtensions::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoinstall_known_extensions = input.GetValue<bool>();
}

void AutoinstallKnownExtensions::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoinstall_known_extensions = DBConfig().options.autoinstall_known_extensions;
}

Value AutoinstallKnownExtensions::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.autoinstall_known_extensions);
}
//===--------------------------------------------------------------------===//
// Autoload Known Extensions
//===--------------------------------------------------------------------===//
void AutoloadKnownExtensions::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoload_known_extensions = input.GetValue<bool>();
}

void AutoloadKnownExtensions::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoload_known_extensions = DBConfig().options.autoload_known_extensions;
}

Value AutoloadKnownExtensions::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.autoload_known_extensions);
}

//===--------------------------------------------------------------------===//
// Enable Progress Bar
//===--------------------------------------------------------------------===//
void EnableProgressBarSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.enable_progress_bar = ClientConfig().enable_progress_bar;
}

void EnableProgressBarSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.enable_progress_bar = input.GetValue<bool>();
}

Value EnableProgressBarSetting::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).enable_progress_bar);
}

//===--------------------------------------------------------------------===//
// Enable Progress Bar Print
//===--------------------------------------------------------------------===//
void EnableProgressBarPrintSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.print_progress_bar = input.GetValue<bool>();
}

void EnableProgressBarPrintSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.print_progress_bar = ClientConfig().print_progress_bar;
}

Value EnableProgressBarPrintSetting::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).print_progress_bar);
}

//===--------------------------------------------------------------------===//
// Explain Output
//===--------------------------------------------------------------------===//
void ExplainOutputSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).explain_output_type = ClientConfig().explain_output_type;
}

void ExplainOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "all") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::ALL;
	} else if (parameter == "optimized_only") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::OPTIMIZED_ONLY;
	} else if (parameter == "physical_only") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::PHYSICAL_ONLY;
	} else {
		throw ParserException("Unrecognized output type \"%s\", expected either ALL, OPTIMIZED_ONLY or PHYSICAL_ONLY",
		                      parameter);
	}
}

Value ExplainOutputSetting::GetSetting(ClientContext &context) {
	switch (ClientConfig::GetConfig(context).explain_output_type) {
	case ExplainOutputType::ALL:
		return "all";
	case ExplainOutputType::OPTIMIZED_ONLY:
		return "optimized_only";
	case ExplainOutputType::PHYSICAL_ONLY:
		return "physical_only";
	default:
		throw InternalException("Unrecognized explain output type");
	}
}

//===--------------------------------------------------------------------===//
// Extension Directory Setting
//===--------------------------------------------------------------------===//
void ExtensionDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_directory = input.ToString();
	config.options.extension_directory = input.ToString();
}

void ExtensionDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.extension_directory = DBConfig().options.extension_directory;
}

Value ExtensionDirectorySetting::GetSetting(ClientContext &context) {
	return Value(DBConfig::GetConfig(context).options.extension_directory);
}

//===--------------------------------------------------------------------===//
// External Threads Setting
//===--------------------------------------------------------------------===//
void ExternalThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.external_threads = input.GetValue<int64_t>();
}

void ExternalThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.external_threads = DBConfig().options.external_threads;
}

Value ExternalThreadsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(config.options.external_threads);
}

//===--------------------------------------------------------------------===//
// File Search Path
//===--------------------------------------------------------------------===//
void FileSearchPathSetting::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	client_data.file_search_path.clear();
}

void FileSearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.file_search_path = parameter;
}

Value FileSearchPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return Value(client_data.file_search_path);
}

//===--------------------------------------------------------------------===//
// Force Compression
//===--------------------------------------------------------------------===//
void ForceCompressionSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto compression = StringUtil::Lower(input.ToString());
	if (compression == "none" || compression == "auto") {
		config.options.force_compression = CompressionType::COMPRESSION_AUTO;
	} else {
		auto compression_type = CompressionTypeFromString(compression);
		if (compression_type == CompressionType::COMPRESSION_AUTO) {
			auto compression_types = StringUtil::Join(ListCompressionTypes(), ", ");
			throw ParserException("Unrecognized option for PRAGMA force_compression, expected %s", compression_types);
		}
		config.options.force_compression = compression_type;
	}
}

void ForceCompressionSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_compression = DBConfig().options.force_compression;
}

Value ForceCompressionSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CompressionTypeToString(config.options.force_compression);
}

//===--------------------------------------------------------------------===//
// Force Bitpacking mode
//===--------------------------------------------------------------------===//
void ForceBitpackingModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto mode_str = StringUtil::Lower(input.ToString());
	auto mode = BitpackingModeFromString(mode_str);
	if (mode == BitpackingMode::INVALID) {
		throw ParserException("Unrecognized option for force_bitpacking_mode, expected none, constant, constant_delta, "
		                      "delta_for, or for");
	}
	config.options.force_bitpacking_mode = mode;
}

void ForceBitpackingModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_bitpacking_mode = DBConfig().options.force_bitpacking_mode;
}

Value ForceBitpackingModeSetting::GetSetting(ClientContext &context) {
	return Value(BitpackingModeToString(context.db->config.options.force_bitpacking_mode));
}

//===--------------------------------------------------------------------===//
// Home Directory
//===--------------------------------------------------------------------===//
void HomeDirectorySetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).home_directory = ClientConfig().home_directory;
}

void HomeDirectorySetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.home_directory = input.IsNull() ? string() : input.ToString();
}

Value HomeDirectorySetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.home_directory);
}

//===--------------------------------------------------------------------===//
// Integer Division
//===--------------------------------------------------------------------===//
void IntegerDivisionSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).integer_division = ClientConfig().integer_division;
}

void IntegerDivisionSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.integer_division = input.GetValue<bool>();
}

Value IntegerDivisionSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.integer_division);
}

//===--------------------------------------------------------------------===//
// Log Query Path
//===--------------------------------------------------------------------===//
void LogQueryPathSetting::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	// TODO: verify that this does the right thing
	client_data.log_query_writer = std::move(ClientData(context).log_query_writer);
}

void LogQueryPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &client_data = ClientData::Get(context);
	auto path = input.ToString();
	if (path.empty()) {
		// empty path: clean up query writer
		client_data.log_query_writer = nullptr;
	} else {
		client_data.log_query_writer = make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(context), path,
		                                                             BufferedFileWriter::DEFAULT_OPEN_FLAGS);
	}
}

Value LogQueryPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return client_data.log_query_writer ? Value(client_data.log_query_writer->path) : Value();
}

//===--------------------------------------------------------------------===//
// Lock Configuration
//===--------------------------------------------------------------------===//
void LockConfigurationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	config.options.lock_configuration = new_value;
}

void LockConfigurationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.lock_configuration = DBConfig().options.lock_configuration;
}

Value LockConfigurationSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.lock_configuration);
}

//===--------------------------------------------------------------------===//
// Immediate Transaction Mode
//===--------------------------------------------------------------------===//
void ImmediateTransactionModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.immediate_transaction_mode = BooleanValue::Get(input);
}

void ImmediateTransactionModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.immediate_transaction_mode = DBConfig().options.immediate_transaction_mode;
}

Value ImmediateTransactionModeSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.immediate_transaction_mode);
}

//===--------------------------------------------------------------------===//
// Maximum Expression Depth
//===--------------------------------------------------------------------===//
void MaximumExpressionDepthSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).max_expression_depth = ClientConfig().max_expression_depth;
}

void MaximumExpressionDepthSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).max_expression_depth = input.GetValue<uint64_t>();
}

Value MaximumExpressionDepthSetting::GetSetting(ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).max_expression_depth);
}

//===--------------------------------------------------------------------===//
// Maximum Memory
//===--------------------------------------------------------------------===//
void MaximumMemorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.maximum_memory = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		BufferManager::GetBufferManager(*db).SetLimit(config.options.maximum_memory);
	}
}

void MaximumMemorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.SetDefaultMaxMemory();
}

Value MaximumMemorySetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.maximum_memory));
}

//===--------------------------------------------------------------------===//
// Password Setting
//===--------------------------------------------------------------------===//
void PasswordSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// nop
}

void PasswordSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// nop
}

Value PasswordSetting::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Perfect Hash Threshold
//===--------------------------------------------------------------------===//
void PerfectHashThresholdSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).perfect_ht_threshold = ClientConfig().perfect_ht_threshold;
}

void PerfectHashThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto bits = input.GetValue<int64_t>();
	if (bits < 0 || bits > 32) {
		throw ParserException("Perfect HT threshold out of range: should be within range 0 - 32");
	}
	ClientConfig::GetConfig(context).perfect_ht_threshold = bits;
}

Value PerfectHashThresholdSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).perfect_ht_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Filter Threshold
//===--------------------------------------------------------------------===//
void PivotFilterThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).pivot_filter_threshold = ClientConfig().pivot_filter_threshold;
}

void PivotFilterThreshold::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).pivot_filter_threshold = input.GetValue<uint64_t>();
}

Value PivotFilterThreshold::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).pivot_filter_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Limit
//===--------------------------------------------------------------------===//
void PivotLimitSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).pivot_limit = ClientConfig().pivot_limit;
}

void PivotLimitSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).pivot_limit = input.GetValue<uint64_t>();
}

Value PivotLimitSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).pivot_limit);
}

//===--------------------------------------------------------------------===//
// PreserveIdentifierCase
//===--------------------------------------------------------------------===//
void PreserveIdentifierCase::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).preserve_identifier_case = ClientConfig().preserve_identifier_case;
}

void PreserveIdentifierCase::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).preserve_identifier_case = input.GetValue<bool>();
}

Value PreserveIdentifierCase::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).preserve_identifier_case);
}

//===--------------------------------------------------------------------===//
// PreserveInsertionOrder
//===--------------------------------------------------------------------===//
void PreserveInsertionOrder::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.preserve_insertion_order = input.GetValue<bool>();
}

void PreserveInsertionOrder::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.preserve_insertion_order = DBConfig().options.preserve_insertion_order;
}

Value PreserveInsertionOrder::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.preserve_insertion_order);
}

//===--------------------------------------------------------------------===//
// ExportLargeBufferArrow
//===--------------------------------------------------------------------===//
void ExportLargeBufferArrow::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto export_large_buffers_arrow = input.GetValue<bool>();

	config.options.arrow_offset_size = export_large_buffers_arrow ? ArrowOffsetSize::LARGE : ArrowOffsetSize::REGULAR;
}

void ExportLargeBufferArrow::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_offset_size = DBConfig().options.arrow_offset_size;
}

Value ExportLargeBufferArrow::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	bool export_large_buffers_arrow = config.options.arrow_offset_size == ArrowOffsetSize::LARGE;
	return Value::BOOLEAN(export_large_buffers_arrow);
}

//===--------------------------------------------------------------------===//
// Profiler History Size
//===--------------------------------------------------------------------===//
void ProfilerHistorySize::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	client_data.query_profiler_history->ResetProfilerHistorySize();
}

void ProfilerHistorySize::SetLocal(ClientContext &context, const Value &input) {
	auto size = input.GetValue<int64_t>();
	if (size <= 0) {
		throw ParserException("Size should be >= 0");
	}
	auto &client_data = ClientData::Get(context);
	client_data.query_profiler_history->SetProfilerHistorySize(size);
}

Value ProfilerHistorySize::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Profile Output
//===--------------------------------------------------------------------===//
void ProfileOutputSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).profiler_save_location = ClientConfig().profiler_save_location;
}

void ProfileOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	auto parameter = input.ToString();
	config.profiler_save_location = parameter;
}

Value ProfileOutputSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.profiler_save_location);
}

//===--------------------------------------------------------------------===//
// Profiling Mode
//===--------------------------------------------------------------------===//
void ProfilingModeSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).enable_profiler = ClientConfig().enable_profiler;
	ClientConfig::GetConfig(context).enable_detailed_profiling = ClientConfig().enable_detailed_profiling;
	ClientConfig::GetConfig(context).emit_profiler_output = ClientConfig().emit_profiler_output;
}

void ProfilingModeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	auto &config = ClientConfig::GetConfig(context);
	if (parameter == "standard") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = false;
		config.emit_profiler_output = true;
	} else if (parameter == "detailed") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = true;
		config.emit_profiler_output = true;
	} else {
		throw ParserException("Unrecognized profiling mode \"%s\", supported formats: [standard, detailed]", parameter);
	}
}

Value ProfilingModeSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_profiler) {
		return Value();
	}
	return Value(config.enable_detailed_profiling ? "detailed" : "standard");
}

//===--------------------------------------------------------------------===//
// Progress Bar Time
//===--------------------------------------------------------------------===//
void ProgressBarTimeSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.wait_time = ClientConfig().wait_time;
	config.enable_progress_bar = ClientConfig().enable_progress_bar;
}

void ProgressBarTimeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.wait_time = input.GetValue<int32_t>();
	config.enable_progress_bar = true;
}

Value ProgressBarTimeSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).wait_time);
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void SchemaSetting::ResetLocal(ClientContext &context) {
	// FIXME: catalog_search_path is controlled by both SchemaSetting and SearchPathSetting
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Reset();
}

void SchemaSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Set(CatalogSearchEntry::Parse(parameter), CatalogSetPathType::SET_SCHEMA);
}

Value SchemaSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return client_data.catalog_search_path->GetDefault().schema;
}

//===--------------------------------------------------------------------===//
// Search Path
//===--------------------------------------------------------------------===//
void SearchPathSetting::ResetLocal(ClientContext &context) {
	// FIXME: catalog_search_path is controlled by both SchemaSetting and SearchPathSetting
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Reset();
}

void SearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Set(CatalogSearchEntry::ParseList(parameter), CatalogSetPathType::SET_SCHEMAS);
}

Value SearchPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	auto &set_paths = client_data.catalog_search_path->GetSetPaths();
	return Value(CatalogSearchEntry::ListToString(set_paths));
}

//===--------------------------------------------------------------------===//
// Temp Directory
//===--------------------------------------------------------------------===//
void TempDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.temporary_directory = input.ToString();
	config.options.use_temporary_directory = !config.options.temporary_directory.empty();
	if (db) {
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		buffer_manager.SetTemporaryDirectory(config.options.temporary_directory);
	}
}

void TempDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.temporary_directory = DBConfig().options.temporary_directory;
	config.options.use_temporary_directory = DBConfig().options.use_temporary_directory;
	if (db) {
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		buffer_manager.SetTemporaryDirectory(config.options.temporary_directory);
	}
}

Value TempDirectorySetting::GetSetting(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	return Value(buffer_manager.GetTemporaryDirectory());
}

//===--------------------------------------------------------------------===//
// Threads Setting
//===--------------------------------------------------------------------===//
void ThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.maximum_threads = input.GetValue<int64_t>();
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(config.options.maximum_threads);
	}
}

void ThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.SetDefaultMaxThreads();
}

Value ThreadsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(config.options.maximum_threads);
}

//===--------------------------------------------------------------------===//
// Username Setting
//===--------------------------------------------------------------------===//
void UsernameSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// nop
}

void UsernameSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// nop
}

Value UsernameSetting::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Allocator Flush Threshold
//===--------------------------------------------------------------------===//
void FlushAllocatorSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_flush_threshold = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

void FlushAllocatorSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_flush_threshold = DBConfig().options.allocator_flush_threshold;
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

Value FlushAllocatorSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.allocator_flush_threshold));
}

} // namespace duckdb






namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, StatementProperties properties,
                                     shared_ptr<ClientContext> context_p, vector<LogicalType> types,
                                     vector<string> names)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, std::move(properties), std::move(types),
                  std::move(names), context_p->GetClientProperties()),
      context(std::move(context_p)) {
	D_ASSERT(context);
}

StreamQueryResult::~StreamQueryResult() {
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = GetError() + "\n";
	}
	return result;
}

unique_ptr<ClientContextLock> StreamQueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

void StreamQueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed pending query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
	unique_ptr<DataChunk> chunk;
	{
		auto lock = LockContext();
		CheckExecutableInternal(*lock);
		chunk = context->Fetch(*lock, *this);
	}
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		Close();
		return nullptr;
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (HasError() || !context) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);

	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);
	while (true) {
		auto chunk = Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		collection->Append(append_state, *chunk);
	}
	auto result =
	    make_uniq<MaterializedQueryResult>(statement_type, properties, names, std::move(collection), client_properties);
	if (HasError()) {
		return make_uniq<MaterializedQueryResult>(GetErrorObject());
	}
	return result;
}

bool StreamQueryResult::IsOpenInternal(ClientContextLock &lock) {
	bool invalidated = !success || !context;
	if (!invalidated) {
		invalidated = !context->IsActiveResult(lock, this);
	}
	return !invalidated;
}

bool StreamQueryResult::IsOpen() {
	if (!success || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

void StreamQueryResult::Close() {
	context.reset();
}

} // namespace duckdb


namespace duckdb {

ValidChecker::ValidChecker() : is_invalidated(false) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	this->is_invalidated = true;
	this->invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	return this->is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}
} // namespace duckdb




namespace duckdb {

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding)
    : old_binding(old_binding), new_binding(new_binding), replace_type(false) {
}

ReplacementBinding::ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type)
    : old_binding(old_binding), new_binding(new_binding), replace_type(true), new_type(std::move(new_type)) {
}

ColumnBindingReplacer::ColumnBindingReplacer() {
}

void ColumnBindingReplacer::VisitOperator(LogicalOperator &op) {
	if (stop_operator && stop_operator.get() == &op) {
		return;
	}
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void ColumnBindingReplacer::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = *expression;
	if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr->Cast<BoundColumnRefExpression>();
		for (const auto &replace_binding : replacement_bindings) {
			if (bound_column_ref.binding == replace_binding.old_binding) {
				bound_column_ref.binding = replace_binding.new_binding;
				if (replace_binding.replace_type) {
					bound_column_ref.return_type = replace_binding.new_type;
				}
			}
		}
	}

	VisitExpressionChildren(**expression);
}

} // namespace duckdb





namespace duckdb {

void ColumnLifetimeAnalyzer::ExtractUnusedColumnBindings(vector<ColumnBinding> bindings,
                                                         column_binding_set_t &unused_bindings) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (column_references.find(bindings[i]) == column_references.end()) {
			unused_bindings.insert(bindings[i]);
		}
	}
}

void ColumnLifetimeAnalyzer::GenerateProjectionMap(vector<ColumnBinding> bindings,
                                                   column_binding_set_t &unused_bindings,
                                                   vector<idx_t> &projection_map) {
	projection_map.clear();
	if (unused_bindings.empty()) {
		return;
	}
	// now iterate over the result bindings of the child
	for (idx_t i = 0; i < bindings.size(); i++) {
		// if this binding does not belong to the unused bindings, add it to the projection map
		if (unused_bindings.find(bindings[i]) == unused_bindings.end()) {
			projection_map.push_back(i);
		}
	}
	if (projection_map.size() == bindings.size()) {
		projection_map.clear();
	}
}

void ColumnLifetimeAnalyzer::StandardVisitOperator(LogicalOperator &op) {
	LogicalOperatorVisitor::VisitOperatorExpressions(op);
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		// visit the duplicate eliminated columns on the LHS, if any
		auto &delim_join = op.Cast<LogicalComparisonJoin>();
		for (auto &expr : delim_join.duplicate_eliminated_columns) {
			VisitExpression(&expr);
		}
	}
	LogicalOperatorVisitor::VisitOperatorChildren(op);
}

void ColumnLifetimeAnalyzer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// FIXME: groups that are not referenced can be removed from projection
		// recurse into the children of the aggregate
		ColumnLifetimeAnalyzer analyzer;
		analyzer.VisitOperatorExpressions(op);
		analyzer.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (everything_referenced) {
			break;
		}
		auto &comp_join = op.Cast<LogicalComparisonJoin>();
		if (comp_join.join_type == JoinType::MARK || comp_join.join_type == JoinType::SEMI ||
		    comp_join.join_type == JoinType::ANTI) {
			break;
		}
		// FIXME for now, we only push into the projection map for equality (hash) joins
		// FIXME: add projection to LHS as well
		bool has_equality = false;
		for (auto &cond : comp_join.conditions) {
			if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
				has_equality = true;
				break;
			}
		}
		if (!has_equality) {
			break;
		}
		// visit current operator expressions so they are added to the referenced_columns
		LogicalOperatorVisitor::VisitOperatorExpressions(op);

		column_binding_set_t unused_bindings;
		auto old_op_bindings = op.GetColumnBindings();
		ExtractUnusedColumnBindings(op.children[1]->GetColumnBindings(), unused_bindings);

		// now recurse into the filter and its children
		LogicalOperatorVisitor::VisitOperatorChildren(op);

		// then generate the projection map
		GenerateProjectionMap(op.children[1]->GetColumnBindings(), unused_bindings, comp_join.right_projection_map);
		return;
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		// for set operations we don't remove anything, just recursively visit the children
		// FIXME: for UNION we can remove unreferenced columns as long as everything_referenced is false (i.e. we
		// encounter a UNION node that is not preceded by a DISTINCT)
		for (auto &child : op.children) {
			ColumnLifetimeAnalyzer analyzer(true);
			analyzer.VisitOperator(*child);
		}
		return;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// then recurse into the children of this projection
		ColumnLifetimeAnalyzer analyzer;
		analyzer.VisitOperatorExpressions(op);
		analyzer.VisitOperator(*op.children[0]);
		return;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		// distinct, all projected columns are used for the DISTINCT computation
		// mark all columns as used and continue to the children
		// FIXME: DISTINCT with expression list does not implicitly reference everything
		everything_referenced = true;
		break;
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto &filter = op.Cast<LogicalFilter>();
		if (everything_referenced) {
			break;
		}
		// first visit operator expressions to populate referenced columns
		LogicalOperatorVisitor::VisitOperatorExpressions(op);
		// filter, figure out which columns are not needed after the filter
		column_binding_set_t unused_bindings;
		ExtractUnusedColumnBindings(op.children[0]->GetColumnBindings(), unused_bindings);

		// now recurse into the filter and its children
		LogicalOperatorVisitor::VisitOperatorChildren(op);

		// then generate the projection map
		GenerateProjectionMap(op.children[0]->GetColumnBindings(), unused_bindings, filter.projection_map);
		return;
	}
	default:
		break;
	}
	StandardVisitOperator(op);
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	column_references.insert(expr.binding);
	return nullptr;
}

unique_ptr<Expression> ColumnLifetimeAnalyzer::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	// BoundReferenceExpression should not be used here yet, they only belong in the physical plan
	throw InternalException("BoundReferenceExpression should not be used here yet!");
}

} // namespace duckdb







namespace duckdb {

void CommonAggregateOptimizer::VisitOperator(LogicalOperator &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		ExtractCommonAggregates(op.Cast<LogicalAggregate>());
		break;
	default:
		break;
	}
}

unique_ptr<Expression> CommonAggregateOptimizer::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	// check if this column ref points to an aggregate that was remapped; if it does we remap it
	auto entry = aggregate_map.find(expr.binding);
	if (entry != aggregate_map.end()) {
		expr.binding = entry->second;
	}
	return nullptr;
}

void CommonAggregateOptimizer::ExtractCommonAggregates(LogicalAggregate &aggr) {
	expression_map_t<idx_t> aggregate_remap;
	idx_t total_erased = 0;
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		idx_t original_index = i + total_erased;
		auto entry = aggregate_remap.find(*aggr.expressions[i]);
		if (entry == aggregate_remap.end()) {
			// aggregate does not exist yet: add it to the map
			aggregate_remap[*aggr.expressions[i]] = i;
			if (i != original_index) {
				// this aggregate is not erased, however an agregate BEFORE it has been erased
				// so we need to remap this aggregaet
				ColumnBinding original_binding(aggr.aggregate_index, original_index);
				ColumnBinding new_binding(aggr.aggregate_index, i);
				aggregate_map[original_binding] = new_binding;
			}
		} else {
			// aggregate already exists! we can remove this entry
			total_erased++;
			aggr.expressions.erase(aggr.expressions.begin() + i);
			i--;
			// we need to remap any references to this aggregate so they point to the other aggregate
			ColumnBinding original_binding(aggr.aggregate_index, original_index);
			ColumnBinding new_binding(aggr.aggregate_index, entry->second);
			aggregate_map[original_binding] = new_binding;
		}
	}
}

} // namespace duckdb





namespace duckdb {

void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> &op) {
	auto &aggregate = op->Cast<LogicalAggregate>();
	auto &groups = aggregate.groups;
	column_binding_set_t group_binding_set;
	for (const auto &group : groups) {
		if (group->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = group->Cast<BoundColumnRefExpression>();
		if (group_binding_set.find(colref.binding) != group_binding_set.end()) {
			return; // Duplicate group - don't compress
		}
		group_binding_set.insert(colref.binding);
	}
	auto &group_stats = aggregate.group_stats;

	// No need to compress if there are no groups/stats
	if (groups.empty() || group_stats.empty()) {
		return;
	}
	D_ASSERT(groups.size() == group_stats.size());

	// Find all bindings referenced by non-colref expressions in the groups
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	column_binding_set_t referenced_bindings;
	vector<ColumnBinding> group_bindings(groups.size(), ColumnBinding());
	vector<bool> needs_decompression(groups.size(), false);
	vector<unique_ptr<BaseStatistics>> stored_group_stats;
	stored_group_stats.resize(groups.size());
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group_expr = *groups[group_idx];
		if (group_expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = group_expr.Cast<BoundColumnRefExpression>();
			group_bindings[group_idx] = colref.binding;
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(group_expr, referenced_bindings);

		// The non-colref expression won't be compressed generically, so try to compress it here
		if (!group_stats[group_idx]) {
			continue; // Can't compress without stats
		}

		// Try to compress, if successful, replace the expression
		auto compress_expr = GetCompressExpression(group_expr.Copy(), *group_stats[group_idx]);
		if (compress_expr) {
			needs_decompression[group_idx] = true;
			stored_group_stats[group_idx] = std::move(group_stats[group_idx]);
			groups[group_idx] = std::move(compress_expr->expression);
			group_stats[group_idx] = std::move(compress_expr->stats);
		}
	}

	// Anything referenced in the aggregate functions is also excluded
	for (idx_t expr_idx = 0; expr_idx < aggregate.expressions.size(); expr_idx++) {
		const auto &expr = *aggregate.expressions[expr_idx];
		D_ASSERT(expr.type == ExpressionType::BOUND_AGGREGATE);
		const auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (const auto &child : aggr_expr.children) {
			GetReferencedBindings(*child, referenced_bindings);
		}
		if (aggr_expr.filter) {
			GetReferencedBindings(*aggr_expr.filter, referenced_bindings);
		}
		if (aggr_expr.order_bys) {
			for (const auto &order : aggr_expr.order_bys->orders) {
				const auto &order_expr = *order.expression;
				if (order_expr.type != ExpressionType::BOUND_COLUMN_REF) {
					GetReferencedBindings(order_expr, referenced_bindings);
				}
			}
		}
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings_out = aggregate.GetColumnBindings();
	const auto &types = aggregate.types;
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		// Aggregate changes bindings as it has a table idx
		CMBindingInfo binding_info(bindings_out[group_idx], types[group_idx]);
		binding_info.needs_decompression = needs_decompression[group_idx];
		if (needs_decompression[group_idx]) {
			// Compressed non-generically
			auto entry = info.binding_map.emplace(bindings_out[group_idx], std::move(binding_info));
			entry.first->second.stats = std::move(stored_group_stats[group_idx]);
		} else if (group_bindings[group_idx] != ColumnBinding()) {
			info.binding_map.emplace(group_bindings[group_idx], std::move(binding_info));
		}
	}

	// Now try to compress
	CreateProjections(op, info);

	// Update aggregate statistics
	UpdateAggregateStats(op);
}

void CompressedMaterialization::UpdateAggregateStats(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// Update aggregate group stats if compressed
	auto &compressed_aggregate = op->children[0]->Cast<LogicalAggregate>();
	auto &groups = compressed_aggregate.groups;
	auto &group_stats = compressed_aggregate.group_stats;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group_expr = *groups[group_idx];
		if (group_expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = group_expr.Cast<BoundColumnRefExpression>();
		if (!group_stats[group_idx]) {
			continue;
		}
		if (colref.return_type == group_stats[group_idx]->GetType()) {
			continue;
		}
		auto it = statistics_map.find(colref.binding);
		if (it != statistics_map.end() && it->second) {
			group_stats[group_idx] = it->second->ToUnique();
		}
	}
}

} // namespace duckdb




namespace duckdb {

void CompressedMaterialization::CompressDistinct(unique_ptr<LogicalOperator> &op) {
	auto &distinct = op->Cast<LogicalDistinct>();
	auto &distinct_targets = distinct.distinct_targets;

	column_binding_set_t referenced_bindings;
	for (auto &target : distinct_targets) {
		if (target->type != ExpressionType::BOUND_COLUMN_REF) { // LCOV_EXCL_START
			GetReferencedBindings(*target, referenced_bindings);
		} // LCOV_EXCL_STOP
	}

	if (distinct.order_by) {
		for (auto &order : distinct.order_by->orders) {
			if (order.expression->type != ExpressionType::BOUND_COLUMN_REF) { // LCOV_EXCL_START
				GetReferencedBindings(*order.expression, referenced_bindings);
			} // LCOV_EXCL_STOP
		}
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings = distinct.GetColumnBindings();
	const auto &types = distinct.types;
	D_ASSERT(bindings.size() == types.size());
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		// Distinct does not change bindings, input binding is output binding
		info.binding_map.emplace(bindings[col_idx], CMBindingInfo(bindings[col_idx], types[col_idx]));
	}

	// Now try to compress
	CreateProjections(op, info);
}

} // namespace duckdb




namespace duckdb {

void CompressedMaterialization::CompressOrder(unique_ptr<LogicalOperator> &op) {
	auto &order = op->Cast<LogicalOrder>();

	// Find all bindings referenced by non-colref expressions in the order nodes
	// These are excluded from compression by projection
	// But we can try to compress the expression directly
	column_binding_set_t referenced_bindings;
	for (idx_t order_node_idx = 0; order_node_idx < order.orders.size(); order_node_idx++) {
		auto &bound_order = order.orders[order_node_idx];
		auto &order_expression = *bound_order.expression;
		if (order_expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			continue; // Will be compressed generically
		}

		// Mark the bindings referenced by the non-colref expression so they won't be modified
		GetReferencedBindings(order_expression, referenced_bindings);
	}

	// Create info for compression
	CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

	// Create binding mapping
	const auto bindings = order.GetColumnBindings();
	const auto &types = order.types;
	D_ASSERT(bindings.size() == types.size());
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		// Order does not change bindings, input binding is output binding
		info.binding_map.emplace(bindings[col_idx], CMBindingInfo(bindings[col_idx], types[col_idx]));
	}

	// Now try to compress
	CreateProjections(op, info);

	// Update order statistics
	UpdateOrderStats(op);
}

void CompressedMaterialization::UpdateOrderStats(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}

	// Update order stats if compressed
	auto &compressed_order = op->children[0]->Cast<LogicalOrder>();
	for (idx_t order_node_idx = 0; order_node_idx < compressed_order.orders.size(); order_node_idx++) {
		auto &bound_order = compressed_order.orders[order_node_idx];
		auto &order_expression = *bound_order.expression;
		if (order_expression.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &colref = order_expression.Cast<BoundColumnRefExpression>();
		auto it = statistics_map.find(colref.binding);
		if (it != statistics_map.end() && it->second) {
			bound_order.stats = it->second->ToUnique();
		}
	}
}

} // namespace duckdb













namespace duckdb {

CMChildInfo::CMChildInfo(LogicalOperator &op, const column_binding_set_t &referenced_bindings)
    : bindings_before(op.GetColumnBindings()), types(op.types), can_compress(bindings_before.size(), true) {
	for (const auto &binding : referenced_bindings) {
		for (idx_t binding_idx = 0; binding_idx < bindings_before.size(); binding_idx++) {
			if (binding == bindings_before[binding_idx]) {
				can_compress[binding_idx] = false;
			}
		}
	}
}

CMBindingInfo::CMBindingInfo(ColumnBinding binding_p, const LogicalType &type_p)
    : binding(binding_p), type(type_p), needs_decompression(false) {
}

CompressedMaterializationInfo::CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs_p,
                                                             const column_binding_set_t &referenced_bindings)
    : child_idxs(child_idxs_p) {
	child_info.reserve(child_idxs.size());
	for (const auto &child_idx : child_idxs) {
		child_info.emplace_back(*op.children[child_idx], referenced_bindings);
	}
}

CompressExpression::CompressExpression(unique_ptr<Expression> expression_p, unique_ptr<BaseStatistics> stats_p)
    : expression(std::move(expression_p)), stats(std::move(stats_p)) {
}

CompressedMaterialization::CompressedMaterialization(ClientContext &context_p, Binder &binder_p,
                                                     statistics_map_t &&statistics_map_p)
    : context(context_p), binder(binder_p), statistics_map(std::move(statistics_map_p)) {
}

void CompressedMaterialization::GetReferencedBindings(const Expression &expression,
                                                      column_binding_set_t &referenced_bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		const auto &col_ref = expression.Cast<BoundColumnRefExpression>();
		referenced_bindings.insert(col_ref.binding);
	} else {
		ExpressionIterator::EnumerateChildren(
		    expression, [&](const Expression &child) { GetReferencedBindings(child, referenced_bindings); });
	}
}

void CompressedMaterialization::UpdateBindingInfo(CompressedMaterializationInfo &info, const ColumnBinding &binding,
                                                  bool needs_decompression) {
	auto &binding_map = info.binding_map;
	auto binding_it = binding_map.find(binding);
	if (binding_it == binding_map.end()) {
		return;
	}

	auto &binding_info = binding_it->second;
	binding_info.needs_decompression = needs_decompression;
	auto stats_it = statistics_map.find(binding);
	if (stats_it != statistics_map.end()) {
		binding_info.stats = statistics_map[binding]->ToUnique();
	}
}

void CompressedMaterialization::Compress(unique_ptr<LogicalOperator> &op) {
	root = op.get();
	root->ResolveOperatorTypes();

	CompressInternal(op);
}

void CompressedMaterialization::CompressInternal(unique_ptr<LogicalOperator> &op) {
	if (TopN::CanOptimize(*op)) { // Let's not mess with the TopN optimizer
		CompressInternal(op->children[0]->children[0]);
		return;
	}

	for (auto &child : op->children) {
		CompressInternal(child);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		CompressAggregate(op);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		CompressDistinct(op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		CompressOrder(op);
		break;
	default:
		return;
	}
}

void CompressedMaterialization::CreateProjections(unique_ptr<LogicalOperator> &op,
                                                  CompressedMaterializationInfo &info) {
	auto &materializing_op = *op;

	bool compressed_anything = false;
	for (idx_t i = 0; i < info.child_idxs.size(); i++) {
		auto &child_info = info.child_info[i];
		vector<unique_ptr<CompressExpression>> compress_exprs;
		if (TryCompressChild(info, child_info, compress_exprs)) {
			// We can compress: Create a projection on top of the child operator
			const auto child_idx = info.child_idxs[i];
			CreateCompressProjection(materializing_op.children[child_idx], std::move(compress_exprs), info, child_info);
			compressed_anything = true;
		}
	}

	if (compressed_anything) {
		CreateDecompressProjection(op, info);
	}
}

bool CompressedMaterialization::TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
                                                 vector<unique_ptr<CompressExpression>> &compress_exprs) {
	// Try to compress each of the column bindings of the child
	bool compressed_anything = false;
	for (idx_t child_i = 0; child_i < child_info.bindings_before.size(); child_i++) {
		const auto child_binding = child_info.bindings_before[child_i];
		const auto &child_type = child_info.types[child_i];
		const auto &can_compress = child_info.can_compress[child_i];
		auto compress_expr = GetCompressExpression(child_binding, child_type, can_compress);
		bool compressed = false;
		if (compress_expr) { // We compressed, mark the outgoing binding in need of decompression
			compress_exprs.emplace_back(std::move(compress_expr));
			compressed = true;
		} else { // We did not compress, just push a colref
			auto colref_expr = make_uniq<BoundColumnRefExpression>(child_type, child_binding);
			auto it = statistics_map.find(colref_expr->binding);
			unique_ptr<BaseStatistics> colref_stats = it != statistics_map.end() ? it->second->ToUnique() : nullptr;
			compress_exprs.emplace_back(make_uniq<CompressExpression>(std::move(colref_expr), std::move(colref_stats)));
		}
		UpdateBindingInfo(info, child_binding, compressed);
		compressed_anything = compressed_anything || compressed;
	}
	if (!compressed_anything) {
		// If we compressed anything non-generically, we still need to decompress
		for (const auto &entry : info.binding_map) {
			compressed_anything = compressed_anything || entry.second.needs_decompression;
		}
	}
	return compressed_anything;
}

void CompressedMaterialization::CreateCompressProjection(unique_ptr<LogicalOperator> &child_op,
                                                         vector<unique_ptr<CompressExpression>> &&compress_exprs,
                                                         CompressedMaterializationInfo &info, CMChildInfo &child_info) {
	// Replace child op with a projection
	vector<unique_ptr<Expression>> projections;
	projections.reserve(compress_exprs.size());
	for (auto &compress_expr : compress_exprs) {
		projections.emplace_back(std::move(compress_expr->expression));
	}
	const auto table_index = binder.GenerateTableIndex();
	auto compress_projection = make_uniq<LogicalProjection>(table_index, std::move(projections));
	compression_table_indices.insert(table_index);
	compress_projection->ResolveOperatorTypes();

	compress_projection->children.emplace_back(std::move(child_op));
	child_op = std::move(compress_projection);

	// Get the new bindings and types
	child_info.bindings_after = child_op->GetColumnBindings();
	const auto &new_types = child_op->types;

	// Initialize a ColumnBindingReplacer with the new bindings and types
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (idx_t col_idx = 0; col_idx < child_info.bindings_before.size(); col_idx++) {
		const auto &old_binding = child_info.bindings_before[col_idx];
		const auto &new_binding = child_info.bindings_after[col_idx];
		const auto &new_type = new_types[col_idx];
		replacement_bindings.emplace_back(old_binding, new_binding, new_type);

		// Remove the old binding from the statistics map
		statistics_map.erase(old_binding);
	}

	// Make sure we skip the compress operator when replacing bindings
	replacer.stop_operator = child_op.get();

	// Make the plan consistent again
	replacer.VisitOperator(*root);

	// Replace in/out exprs in the binding map too
	auto &binding_map = info.binding_map;
	for (auto &replacement_binding : replacement_bindings) {
		auto it = binding_map.find(replacement_binding.old_binding);
		if (it == binding_map.end()) {
			continue;
		}
		auto &binding_info = it->second;
		if (binding_info.binding == replacement_binding.old_binding) {
			binding_info.binding = replacement_binding.new_binding;
		}

		if (it->first == replacement_binding.old_binding) {
			auto binding_info_local = std::move(binding_info);
			binding_map.erase(it);
			binding_map.emplace(replacement_binding.new_binding, std::move(binding_info_local));
		}
	}

	// Add projection stats to statistics map
	for (idx_t col_idx = 0; col_idx < child_info.bindings_after.size(); col_idx++) {
		const auto &binding = child_info.bindings_after[col_idx];
		auto &stats = compress_exprs[col_idx]->stats;
		statistics_map.emplace(binding, std::move(stats));
	}
}

void CompressedMaterialization::CreateDecompressProjection(unique_ptr<LogicalOperator> &op,
                                                           CompressedMaterializationInfo &info) {
	const auto bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	const auto &types = op->types;

	// Create decompress expressions for everything we compressed
	auto &binding_map = info.binding_map;
	vector<unique_ptr<Expression>> decompress_exprs;
	vector<optional_ptr<BaseStatistics>> statistics;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &binding = bindings[col_idx];
		auto decompress_expr = make_uniq_base<Expression, BoundColumnRefExpression>(types[col_idx], binding);
		optional_ptr<BaseStatistics> stats;
		for (auto &entry : binding_map) {
			auto &binding_info = entry.second;
			if (binding_info.binding != binding) {
				continue;
			}
			stats = binding_info.stats.get();
			if (binding_info.needs_decompression) {
				decompress_expr = GetDecompressExpression(std::move(decompress_expr), binding_info.type, *stats);
			}
		}
		statistics.push_back(stats);
		decompress_exprs.emplace_back(std::move(decompress_expr));
	}

	// Replace op with a projection
	const auto table_index = binder.GenerateTableIndex();
	auto decompress_projection = make_uniq<LogicalProjection>(table_index, std::move(decompress_exprs));
	decompression_table_indices.insert(table_index);

	decompress_projection->children.emplace_back(std::move(op));
	op = std::move(decompress_projection);

	// Check if we're placing a projection on top of the root
	if (op->children[0].get() == root.get()) {
		root = op.get();
		return;
	}

	// Get the new bindings and types
	auto new_bindings = op->GetColumnBindings();
	op->ResolveOperatorTypes();
	auto &new_types = op->types;

	// Initialize a ColumnBindingReplacer with the new bindings and types
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
		const auto &old_binding = bindings[col_idx];
		const auto &new_binding = new_bindings[col_idx];
		const auto &new_type = new_types[col_idx];
		replacement_bindings.emplace_back(old_binding, new_binding, new_type);

		if (statistics[col_idx]) {
			statistics_map[new_binding] = statistics[col_idx]->ToUnique();
		}
	}

	// Make sure we skip the decompress operator when replacing bindings
	replacer.stop_operator = op.get();

	// Make the plan consistent again
	replacer.VisitOperator(*root);
}

unique_ptr<CompressExpression> CompressedMaterialization::GetCompressExpression(const ColumnBinding &binding,
                                                                                const LogicalType &type,
                                                                                const bool &can_compress) {
	auto it = statistics_map.find(binding);
	if (can_compress && it != statistics_map.end() && it->second) {
		auto input = make_uniq<BoundColumnRefExpression>(type, binding);
		const auto &stats = *it->second;
		return GetCompressExpression(std::move(input), stats);
	}
	return nullptr;
}

unique_ptr<CompressExpression> CompressedMaterialization::GetCompressExpression(unique_ptr<Expression> input,
                                                                                const BaseStatistics &stats) {
	const auto &type = input->return_type;
	if (type != stats.GetType()) { // LCOV_EXCL_START
		return nullptr;
	} // LCOV_EXCL_STOP
	if (type.IsIntegral()) {
		return GetIntegralCompress(std::move(input), stats);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringCompress(std::move(input), stats);
	}
	return nullptr;
}

static Value GetIntegralRangeValue(ClientContext &context, const LogicalType &type, const BaseStatistics &stats) {
	auto min = NumericStats::Min(stats);
	auto max = NumericStats::Max(stats);

	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(make_uniq<BoundConstantExpression>(max));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(min));
	BoundFunctionExpression sub(type, SubtractFun::GetFunction(type, type), std::move(arguments), nullptr);

	Value result;
	if (ExpressionExecutor::TryEvaluateScalar(context, sub, result)) {
		return result;
	} else {
		// Couldn't evaluate: Return max hugeint as range so GetIntegralCompress will return nullptr
		return Value::HUGEINT(NumericLimits<hugeint_t>::Maximum());
	}
}

unique_ptr<CompressExpression> CompressedMaterialization::GetIntegralCompress(unique_ptr<Expression> input,
                                                                              const BaseStatistics &stats) {
	const auto &type = input->return_type;
	if (GetTypeIdSize(type.InternalType()) == 1 || !NumericStats::HasMinMax(stats)) {
		return nullptr;
	}

	// Get range and cast to UBIGINT (might fail for HUGEINT, in which case we just return)
	Value range_value = GetIntegralRangeValue(context, type, stats);
	if (!range_value.DefaultTryCastAs(LogicalType::UBIGINT)) {
		return nullptr;
	}

	// Get the smallest type that the range can fit into
	const auto range = UBigIntValue::Get(range_value);
	LogicalType cast_type;
	if (range <= NumericLimits<uint8_t>().Maximum()) {
		cast_type = LogicalType::UTINYINT;
	} else if (range <= NumericLimits<uint16_t>().Maximum()) {
		cast_type = LogicalType::USMALLINT;
	} else if (range <= NumericLimits<uint32_t>().Maximum()) {
		cast_type = LogicalType::UINTEGER;
	} else {
		D_ASSERT(range <= NumericLimits<uint64_t>().Maximum());
		cast_type = LogicalType::UBIGINT;
	}

	// Check if type that fits the range is smaller than the input type
	if (GetTypeIdSize(cast_type.InternalType()) == GetTypeIdSize(type.InternalType())) {
		return nullptr;
	}
	D_ASSERT(GetTypeIdSize(cast_type.InternalType()) < GetTypeIdSize(type.InternalType()));

	// Compressing will yield a benefit
	auto compress_function = CMIntegralCompressFun::GetFunction(type, cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	auto compress_expr =
	    make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);

	auto compress_stats = BaseStatistics::CreateEmpty(cast_type);
	compress_stats.CopyBase(stats);
	NumericStats::SetMin(compress_stats, Value(0).DefaultCastAs(cast_type));
	NumericStats::SetMax(compress_stats, range_value.DefaultCastAs(cast_type));

	return make_uniq<CompressExpression>(std::move(compress_expr), compress_stats.ToUnique());
}

unique_ptr<CompressExpression> CompressedMaterialization::GetStringCompress(unique_ptr<Expression> input,
                                                                            const BaseStatistics &stats) {
	if (!StringStats::HasMaxStringLength(stats)) {
		return nullptr;
	}

	const auto max_string_length = StringStats::MaxStringLength(stats);
	LogicalType cast_type = LogicalType::INVALID;
	for (const auto &compressed_type : CompressedMaterializationFunctions::StringTypes()) {
		if (max_string_length < GetTypeIdSize(compressed_type.InternalType())) {
			cast_type = compressed_type;
			break;
		}
	}
	if (cast_type == LogicalType::INVALID) {
		return nullptr;
	}

	auto compress_stats = BaseStatistics::CreateEmpty(cast_type);
	compress_stats.CopyBase(stats);
	if (cast_type.id() == LogicalTypeId::USMALLINT) {
		auto min_string = StringStats::Min(stats);
		auto max_string = StringStats::Max(stats);

		uint8_t min_numeric = 0;
		if (max_string_length != 0 && min_string.length() != 0) {
			min_numeric = *reinterpret_cast<const uint8_t *>(min_string.c_str());
		}
		uint8_t max_numeric = 0;
		if (max_string_length != 0 && max_string.length() != 0) {
			max_numeric = *reinterpret_cast<const uint8_t *>(max_string.c_str());
		}

		Value min_val = Value::USMALLINT(min_numeric);
		Value max_val = Value::USMALLINT(max_numeric + 1);
		if (max_numeric < NumericLimits<uint8_t>::Maximum()) {
			cast_type = LogicalType::UTINYINT;
			compress_stats = BaseStatistics::CreateEmpty(cast_type);
			compress_stats.CopyBase(stats);
			min_val = Value::UTINYINT(min_numeric);
			max_val = Value::UTINYINT(max_numeric + 1);
		}

		NumericStats::SetMin(compress_stats, min_val);
		NumericStats::SetMax(compress_stats, max_val);
	}

	auto compress_function = CMStringCompressFun::GetFunction(cast_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	auto compress_expr =
	    make_uniq<BoundFunctionExpression>(cast_type, compress_function, std::move(arguments), nullptr);
	return make_uniq<CompressExpression>(std::move(compress_expr), compress_stats.ToUnique());
}

unique_ptr<Expression> CompressedMaterialization::GetDecompressExpression(unique_ptr<Expression> input,
                                                                          const LogicalType &result_type,
                                                                          const BaseStatistics &stats) {
	const auto &type = result_type;
	if (TypeIsIntegral(type.InternalType())) {
		return GetIntegralDecompress(std::move(input), result_type, stats);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		return GetStringDecompress(std::move(input), stats);
	} else {
		throw InternalException("Type other than integral/string marked for decompression!");
	}
}

unique_ptr<Expression> CompressedMaterialization::GetIntegralDecompress(unique_ptr<Expression> input,
                                                                        const LogicalType &result_type,
                                                                        const BaseStatistics &stats) {
	D_ASSERT(NumericStats::HasMinMax(stats));
	auto decompress_function = CMIntegralDecompressFun::GetFunction(input->return_type, result_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	arguments.emplace_back(make_uniq<BoundConstantExpression>(NumericStats::Min(stats)));
	return make_uniq<BoundFunctionExpression>(result_type, decompress_function, std::move(arguments), nullptr);
}

unique_ptr<Expression> CompressedMaterialization::GetStringDecompress(unique_ptr<Expression> input,
                                                                      const BaseStatistics &stats) {
	D_ASSERT(StringStats::HasMaxStringLength(stats));
	auto decompress_function = CMStringDecompressFun::GetFunction(input->return_type);
	vector<unique_ptr<Expression>> arguments;
	arguments.emplace_back(std::move(input));
	return make_uniq<BoundFunctionExpression>(decompress_function.return_type, decompress_function,
	                                          std::move(arguments), nullptr);
}

} // namespace duckdb









namespace duckdb {

//! The CSENode contains information about a common subexpression; how many times it occurs, and the column index in the
//! underlying projection
struct CSENode {
	idx_t count;
	idx_t column_index;

	CSENode() : count(1), column_index(DConstants::INVALID_INDEX) {
	}
};

//! The CSEReplacementState
struct CSEReplacementState {
	//! The projection index of the new projection
	idx_t projection_index;
	//! Map of expression -> CSENode
	expression_map_t<CSENode> expression_count;
	//! Map of column bindings to column indexes in the projection expression list
	column_binding_map_t<idx_t> column_map;
	//! The set of expressions of the resulting projection
	vector<unique_ptr<Expression>> expressions;
	//! Cached expressions that are kept around so the expression_map always contains valid expressions
	vector<unique_ptr<Expression>> cached_expressions;
};

void CommonSubExpressionOptimizer::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		ExtractCommonSubExpresions(op);
		break;
	default:
		break;
	}
	LogicalOperatorVisitor::VisitOperator(op);
}

void CommonSubExpressionOptimizer::CountExpressions(Expression &expr, CSEReplacementState &state) {
	// we only consider expressions with children for CSE elimination
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
	// skip conjunctions and case, since short-circuiting might be incorrectly disabled otherwise
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CASE:
		return;
	default:
		break;
	}
	if (expr.expression_class != ExpressionClass::BOUND_AGGREGATE && !expr.HasSideEffects()) {
		// we can't move aggregates to a projection, so we only consider the children of the aggregate
		auto node = state.expression_count.find(expr);
		if (node == state.expression_count.end()) {
			// first time we encounter this expression, insert this node with [count = 1]
			state.expression_count[expr] = CSENode();
		} else {
			// we encountered this expression before, increment the occurrence count
			node->second.count++;
		}
	}
	// recursively count the children
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CountExpressions(child, state); });
}

void CommonSubExpressionOptimizer::PerformCSEReplacement(unique_ptr<Expression> &expr_ptr, CSEReplacementState &state) {
	Expression &expr = *expr_ptr;
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &bound_column_ref = expr.Cast<BoundColumnRefExpression>();
		// bound column ref, check if this one has already been recorded in the expression list
		auto column_entry = state.column_map.find(bound_column_ref.binding);
		if (column_entry == state.column_map.end()) {
			// not there yet: push the expression
			idx_t new_column_index = state.expressions.size();
			state.column_map[bound_column_ref.binding] = new_column_index;
			state.expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    bound_column_ref.alias, bound_column_ref.return_type, bound_column_ref.binding));
			bound_column_ref.binding = ColumnBinding(state.projection_index, new_column_index);
		} else {
			// else: just update the column binding!
			bound_column_ref.binding = ColumnBinding(state.projection_index, column_entry->second);
		}
		return;
	}
	// check if this child is eligible for CSE elimination
	bool can_cse = expr.expression_class != ExpressionClass::BOUND_CONJUNCTION &&
	               expr.expression_class != ExpressionClass::BOUND_CASE;
	if (can_cse && state.expression_count.find(expr) != state.expression_count.end()) {
		auto &node = state.expression_count[expr];
		if (node.count > 1) {
			// this expression occurs more than once! push it into the projection
			// check if it has already been pushed into the projection
			auto alias = expr.alias;
			auto type = expr.return_type;
			if (node.column_index == DConstants::INVALID_INDEX) {
				// has not been pushed yet: push it
				node.column_index = state.expressions.size();
				state.expressions.push_back(std::move(expr_ptr));
			} else {
				state.cached_expressions.push_back(std::move(expr_ptr));
			}
			// replace the original expression with a bound column ref
			expr_ptr = make_uniq<BoundColumnRefExpression>(alias, type,
			                                               ColumnBinding(state.projection_index, node.column_index));
			return;
		}
	}
	// this expression only occurs once, we can't perform CSE elimination
	// look into the children to see if we can replace them
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](unique_ptr<Expression> &child) { PerformCSEReplacement(child, state); });
}

void CommonSubExpressionOptimizer::ExtractCommonSubExpresions(LogicalOperator &op) {
	D_ASSERT(op.children.size() == 1);

	// first we count for each expression with children how many types it occurs
	CSEReplacementState state;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *child) { CountExpressions(**child, state); });
	// check if there are any expressions to extract
	bool perform_replacement = false;
	for (auto &expr : state.expression_count) {
		if (expr.second.count > 1) {
			perform_replacement = true;
			break;
		}
	}
	if (!perform_replacement) {
		// no CSEs to extract
		return;
	}
	state.projection_index = binder.GenerateTableIndex();
	// we found common subexpressions to extract
	// now we iterate over all the expressions and perform the actual CSE elimination

	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *child) { PerformCSEReplacement(*child, state); });
	D_ASSERT(state.expressions.size() > 0);
	// create a projection node as the child of this node
	auto projection = make_uniq<LogicalProjection>(state.projection_index, std::move(state.expressions));
	projection->children.push_back(std::move(op.children[0]));
	op.children[0] = std::move(projection);
}

} // namespace duckdb













namespace duckdb {

struct DelimCandidate {
public:
	explicit DelimCandidate(unique_ptr<LogicalOperator> &op, LogicalComparisonJoin &delim_join)
	    : op(op), delim_join(delim_join), delim_get_count(0) {
	}

public:
	unique_ptr<LogicalOperator> &op;
	LogicalComparisonJoin &delim_join;
	vector<reference<unique_ptr<LogicalOperator>>> joins;
	idx_t delim_get_count;
};

static bool IsEqualityJoinCondition(const JoinCondition &cond) {
	switch (cond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return true;
	default:
		return false;
	}
}

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	root = op;

	vector<DelimCandidate> candidates;
	FindCandidates(op, candidates);

	for (auto &candidate : candidates) {
		auto &delim_join = candidate.delim_join;

		bool all_removed = true;
		bool all_equality_conditions = true;
		for (auto &join : candidate.joins) {
			all_removed =
			    RemoveJoinWithDelimGet(delim_join, candidate.delim_get_count, join, all_equality_conditions) &&
			    all_removed;
		}

		// Change type if there are no more duplicate-eliminated columns
		if (candidate.joins.size() == candidate.delim_get_count && all_removed) {
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
			delim_join.duplicate_eliminated_columns.clear();
			if (all_equality_conditions) {
				for (auto &cond : delim_join.conditions) {
					if (IsEqualityJoinCondition(cond)) {
						cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
					}
				}
			}
		}
	}

	return op;
}

void Deliminator::FindCandidates(unique_ptr<LogicalOperator> &op, vector<DelimCandidate> &candidates) {
	// Search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(child, candidates);
	}

	if (op->type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}

	candidates.emplace_back(op, op->Cast<LogicalComparisonJoin>());
	auto &candidate = candidates.back();

	// DelimGets are in the RHS
	FindJoinWithDelimGet(op->children[1], candidate);
}

static bool OperatorIsDelimGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER &&
	    op.children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	return false;
}

void Deliminator::FindJoinWithDelimGet(unique_ptr<LogicalOperator> &op, DelimCandidate &candidate) {
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		FindJoinWithDelimGet(op->children[0], candidate);
	} else if (op->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidate.delim_get_count++;
	} else {
		for (auto &child : op->children) {
			FindJoinWithDelimGet(child, candidate);
		}
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    (OperatorIsDelimGet(*op->children[0]) || OperatorIsDelimGet(*op->children[1]))) {
		candidate.joins.emplace_back(op);
	}
}

static bool ChildJoinTypeCanBeDeliminated(JoinType &join_type) {
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::SEMI:
		return true;
	default:
		return false;
	}
}

bool Deliminator::RemoveJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
                                         unique_ptr<LogicalOperator> &join, bool &all_equality_conditions) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	if (!ChildJoinTypeCanBeDeliminated(comparison_join.join_type)) {
		return false;
	}

	// Get the index (left or right) of the DelimGet side of the join
	const idx_t delim_idx = OperatorIsDelimGet(*join->children[0]) ? 0 : 1;

	// Get the filter (if any)
	optional_ptr<LogicalFilter> filter;
	vector<unique_ptr<Expression>> filter_expressions;
	if (join->children[delim_idx]->type == LogicalOperatorType::LOGICAL_FILTER) {
		filter = &join->children[delim_idx]->Cast<LogicalFilter>();
		for (auto &expr : filter->expressions) {
			filter_expressions.emplace_back(expr->Copy());
		}
	}

	auto &delim_get = (filter ? filter->children[0] : join->children[delim_idx])->Cast<LogicalDelimGet>();
	if (comparison_join.conditions.size() != delim_get.chunk_types.size()) {
		return false; // Joining with DelimGet adds new information
	}

	// Check if joining with the DelimGet is redundant, and collect relevant column information
	ColumnBindingReplacer replacer;
	auto &replacement_bindings = replacer.replacement_bindings;
	for (auto &cond : comparison_join.conditions) {
		all_equality_conditions = all_equality_conditions && IsEqualityJoinCondition(cond);
		auto &delim_side = delim_idx == 0 ? *cond.left : *cond.right;
		auto &other_side = delim_idx == 0 ? *cond.right : *cond.left;
		if (delim_side.type != ExpressionType::BOUND_COLUMN_REF ||
		    other_side.type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &delim_colref = delim_side.Cast<BoundColumnRefExpression>();
		auto &other_colref = other_side.Cast<BoundColumnRefExpression>();
		replacement_bindings.emplace_back(delim_colref.binding, other_colref.binding);

		if (cond.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			auto is_not_null_expr =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			is_not_null_expr->children.push_back(other_side.Copy());
			filter_expressions.push_back(std::move(is_not_null_expr));
		}
	}

	if (!all_equality_conditions &&
	    !RemoveInequalityJoinWithDelimGet(delim_join, delim_get_count, join, replacement_bindings)) {
		return false;
	}

	unique_ptr<LogicalOperator> replacement_op = std::move(comparison_join.children[1 - delim_idx]);
	if (!filter_expressions.empty()) { // Create filter if necessary
		auto new_filter = make_uniq<LogicalFilter>();
		new_filter->expressions = std::move(filter_expressions);
		new_filter->children.emplace_back(std::move(replacement_op));
		replacement_op = std::move(new_filter);
	}

	join = std::move(replacement_op);

	// TODO: Maybe go from delim join instead to save work
	replacer.VisitOperator(*root);
	return true;
}

static bool InequalityDelimJoinCanBeEliminated(JoinType &join_type) {
	return join_type == JoinType::ANTI || join_type == JoinType::MARK || join_type == JoinType::SEMI ||
	       join_type == JoinType::SINGLE;
}

bool FindAndReplaceBindings(vector<ColumnBinding> &traced_bindings, const vector<unique_ptr<Expression>> &expressions,
                            const vector<ColumnBinding> &current_bindings) {
	for (auto &binding : traced_bindings) {
		idx_t current_idx;
		for (current_idx = 0; current_idx < expressions.size(); current_idx++) {
			if (binding == current_bindings[current_idx]) {
				break;
			}
		}

		if (current_idx == expressions.size() || expressions[current_idx]->type != ExpressionType::BOUND_COLUMN_REF) {
			return false; // Didn't find / can't deal with non-colref
		}

		auto &colref = expressions[current_idx]->Cast<BoundColumnRefExpression>();
		binding = colref.binding;
	}
	return true;
}

bool Deliminator::RemoveInequalityJoinWithDelimGet(LogicalComparisonJoin &delim_join, const idx_t delim_get_count,
                                                   unique_ptr<LogicalOperator> &join,
                                                   const vector<ReplacementBinding> &replacement_bindings) {
	auto &comparison_join = join->Cast<LogicalComparisonJoin>();
	auto &delim_conditions = delim_join.conditions;
	const auto &join_conditions = comparison_join.conditions;
	if (delim_get_count != 1 || !InequalityDelimJoinCanBeEliminated(delim_join.join_type) ||
	    delim_conditions.size() != join_conditions.size()) {
		return false;
	}

	// TODO: we cannot perform the optimization here because our pure inequality joins don't implement
	//  JoinType::SINGLE yet
	if (delim_join.join_type == JoinType::SINGLE) {
		bool has_one_equality = false;
		for (auto &cond : join_conditions) {
			has_one_equality = has_one_equality || IsEqualityJoinCondition(cond);
		}
		if (!has_one_equality) {
			return false;
		}
	}

	// We only support colref's
	vector<ColumnBinding> traced_bindings;
	for (const auto &cond : delim_conditions) {
		if (cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &colref = cond.right->Cast<BoundColumnRefExpression>();
		traced_bindings.emplace_back(colref.binding);
	}

	// Now we trace down the bindings to the join (for now, we only trace it through a few operators)
	reference<LogicalOperator> current_op = *delim_join.children[1];
	while (&current_op.get() != join.get()) {
		if (current_op.get().children.size() != 1) {
			return false;
		}

		switch (current_op.get().type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			FindAndReplaceBindings(traced_bindings, current_op.get().expressions, current_op.get().GetColumnBindings());
			break;
		case LogicalOperatorType::LOGICAL_FILTER:
			break; // Doesn't change bindings
		default:
			return false;
		}
		current_op = *current_op.get().children[0];
	}

	// Get the index (left or right) of the DelimGet side of the join
	const idx_t delim_idx = OperatorIsDelimGet(*join->children[0]) ? 0 : 1;

	bool found_all = true;
	for (idx_t cond_idx = 0; cond_idx < delim_conditions.size(); cond_idx++) {
		auto &delim_condition = delim_conditions[cond_idx];
		const auto &traced_binding = traced_bindings[cond_idx];

		bool found = false;
		for (auto &join_condition : join_conditions) {
			auto &delim_side = delim_idx == 0 ? *join_condition.left : *join_condition.right;
			auto &colref = delim_side.Cast<BoundColumnRefExpression>();
			if (colref.binding == traced_binding) {
				delim_condition.comparison = FlipComparisonExpression(join_condition.comparison);
				found = true;
				break;
			}
		}
		found_all = found_all && found;
	}

	return found_all;
}

} // namespace duckdb



namespace duckdb {

unique_ptr<LogicalOperator> ExpressionHeuristics::Rewrite(unique_ptr<LogicalOperator> op) {
	VisitOperator(*op);
	return op;
}

void ExpressionHeuristics::VisitOperator(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		// reorder all filter expressions
		if (op.expressions.size() > 1) {
			ReorderExpressions(op.expressions);
		}
	}

	// traverse recursively through the operator tree
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> ExpressionHeuristics::VisitReplace(BoundConjunctionExpression &expr,
                                                          unique_ptr<Expression> *expr_ptr) {
	ReorderExpressions(expr.children);
	return nullptr;
}

void ExpressionHeuristics::ReorderExpressions(vector<unique_ptr<Expression>> &expressions) {

	struct ExpressionCosts {
		unique_ptr<Expression> expr;
		idx_t cost;

		bool operator==(const ExpressionCosts &p) const {
			return cost == p.cost;
		}
		bool operator<(const ExpressionCosts &p) const {
			return cost < p.cost;
		}
	};

	vector<ExpressionCosts> expression_costs;
	expression_costs.reserve(expressions.size());
	// iterate expressions, get cost for each one
	for (idx_t i = 0; i < expressions.size(); i++) {
		idx_t cost = Cost(*expressions[i]);
		expression_costs.push_back({std::move(expressions[i]), cost});
	}

	// sort by cost and put back in place
	sort(expression_costs.begin(), expression_costs.end());
	for (idx_t i = 0; i < expression_costs.size(); i++) {
		expressions[i] = std::move(expression_costs[i].expr);
	}
}

idx_t ExpressionHeuristics::ExpressionCost(BoundBetweenExpression &expr) {
	return Cost(*expr.input) + Cost(*expr.lower) + Cost(*expr.upper) + 10;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundCaseExpression &expr) {
	// CASE WHEN check THEN result_if_true ELSE result_if_false END
	idx_t case_cost = 0;
	for (auto &case_check : expr.case_checks) {
		case_cost += Cost(*case_check.then_expr);
		case_cost += Cost(*case_check.when_expr);
	}
	case_cost += Cost(*expr.else_expr);
	return case_cost;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundCastExpression &expr) {
	// OPERATOR_CAST
	// determine cast cost by comparing cast_expr.source_type and cast_expr_target_type
	idx_t cast_cost = 0;
	if (expr.return_type != expr.source_type()) {
		// if cast from or to varchar
		// TODO: we might want to add more cases
		if (expr.return_type.id() == LogicalTypeId::VARCHAR || expr.source_type().id() == LogicalTypeId::VARCHAR ||
		    expr.return_type.id() == LogicalTypeId::BLOB || expr.source_type().id() == LogicalTypeId::BLOB) {
			cast_cost = 200;
		} else {
			cast_cost = 5;
		}
	}
	return Cost(*expr.child) + cast_cost;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundComparisonExpression &expr) {
	// COMPARE_EQUAL, COMPARE_NOTEQUAL, COMPARE_GREATERTHAN, COMPARE_GREATERTHANOREQUALTO, COMPARE_LESSTHAN,
	// COMPARE_LESSTHANOREQUALTO
	return Cost(*expr.left) + 5 + Cost(*expr.right);
}

idx_t ExpressionHeuristics::ExpressionCost(BoundConjunctionExpression &expr) {
	// CONJUNCTION_AND, CONJUNCTION_OR
	idx_t cost = 5;
	for (auto &child : expr.children) {
		cost += Cost(*child);
	}
	return cost;
}

idx_t ExpressionHeuristics::ExpressionCost(BoundFunctionExpression &expr) {
	idx_t cost_children = 0;
	for (auto &child : expr.children) {
		cost_children += Cost(*child);
	}

	auto cost_function = function_costs.find(expr.function.name);
	if (cost_function != function_costs.end()) {
		return cost_children + cost_function->second;
	} else {
		return cost_children + 1000;
	}
}

idx_t ExpressionHeuristics::ExpressionCost(BoundOperatorExpression &expr, ExpressionType &expr_type) {
	idx_t sum = 0;
	for (auto &child : expr.children) {
		sum += Cost(*child);
	}

	// OPERATOR_IS_NULL, OPERATOR_IS_NOT_NULL
	if (expr_type == ExpressionType::OPERATOR_IS_NULL || expr_type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return sum + 5;
	} else if (expr_type == ExpressionType::COMPARE_IN || expr_type == ExpressionType::COMPARE_NOT_IN) {
		// COMPARE_IN, COMPARE_NOT_IN
		return sum + (expr.children.size() - 1) * 100;
	} else if (expr_type == ExpressionType::OPERATOR_NOT) {
		// OPERATOR_NOT
		return sum + 10; // TODO: evaluate via measured runtimes
	} else {
		return sum + 1000;
	}
}

idx_t ExpressionHeuristics::ExpressionCost(PhysicalType return_type, idx_t multiplier) {
	// TODO: ajust values according to benchmark results
	switch (return_type) {
	case PhysicalType::VARCHAR:
		return 5 * multiplier;
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return 2 * multiplier;
	default:
		return 1 * multiplier;
	}
}

idx_t ExpressionHeuristics::Cost(Expression &expr) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		return ExpressionCost(case_expr);
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = expr.Cast<BoundBetweenExpression>();
		return ExpressionCost(between_expr);
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		return ExpressionCost(cast_expr);
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		return ExpressionCost(comp_expr);
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		return ExpressionCost(conj_expr);
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		return ExpressionCost(func_expr);
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		return ExpressionCost(op_expr, expr.type);
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col_expr = expr.Cast<BoundColumnRefExpression>();
		return ExpressionCost(col_expr.return_type.InternalType(), 8);
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &const_expr = expr.Cast<BoundConstantExpression>();
		return ExpressionCost(const_expr.return_type.InternalType(), 1);
	}
	case ExpressionClass::BOUND_PARAMETER: {
		auto &const_expr = expr.Cast<BoundParameterExpression>();
		return ExpressionCost(const_expr.return_type.InternalType(), 1);
	}
	case ExpressionClass::BOUND_REF: {
		auto &col_expr = expr.Cast<BoundColumnRefExpression>();
		return ExpressionCost(col_expr.return_type.InternalType(), 8);
	}
	default: {
		break;
	}
	}

	// return a very high value if nothing matches
	return 1000;
}

} // namespace duckdb









namespace duckdb {

unique_ptr<Expression> ExpressionRewriter::ApplyRules(LogicalOperator &op, const vector<reference<Rule>> &rules,
                                                      unique_ptr<Expression> expr, bool &changes_made, bool is_root) {
	for (auto &rule : rules) {
		vector<reference<Expression>> bindings;
		if (rule.get().root->Match(*expr, bindings)) {
			// the rule matches! try to apply it
			bool rule_made_change = false;
			auto result = rule.get().Apply(op, bindings, rule_made_change, is_root);
			if (result) {
				changes_made = true;
				// the base node changed: the rule applied changes
				// rerun on the new node
				return ExpressionRewriter::ApplyRules(op, rules, std::move(result), changes_made);
			} else if (rule_made_change) {
				changes_made = true;
				// the base node didn't change, but changes were made, rerun
				return expr;
			}
			// else nothing changed, continue to the next rule
			continue;
		}
	}
	// no changes could be made to this node
	// recursively run on the children of this node
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		child = ExpressionRewriter::ApplyRules(op, rules, std::move(child), changes_made);
	});
	return expr;
}

unique_ptr<Expression> ExpressionRewriter::ConstantOrNull(unique_ptr<Expression> child, Value value) {
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundConstantExpression>(value));
	children.push_back(std::move(child));
	return ConstantOrNull(std::move(children), std::move(value));
}

unique_ptr<Expression> ExpressionRewriter::ConstantOrNull(vector<unique_ptr<Expression>> children, Value value) {
	auto type = value.type();
	children.insert(children.begin(), make_uniq<BoundConstantExpression>(value));
	return make_uniq<BoundFunctionExpression>(type, ConstantOrNull::GetFunction(type), std::move(children),
	                                          ConstantOrNull::Bind(std::move(value)));
}

void ExpressionRewriter::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	this->op = &op;

	to_apply_rules.clear();
	for (auto &rule : rules) {
		if (rule->logical_root && !rule->logical_root->Match(op.type)) {
			// this rule does not apply to this type of LogicalOperator
			continue;
		}
		to_apply_rules.push_back(*rule);
	}
	if (to_apply_rules.empty()) {
		// no rules to apply on this node
		return;
	}

	VisitOperatorExpressions(op);

	// if it is a LogicalFilter, we split up filter conjunctions again
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		filter.SplitPredicates();
	}
}

void ExpressionRewriter::VisitExpression(unique_ptr<Expression> *expression) {
	bool changes_made;
	do {
		changes_made = false;
		*expression = ExpressionRewriter::ApplyRules(*op, to_apply_rules, std::move(*expression), changes_made, true);
	} while (changes_made);
}

ClientContext &Rule::GetContext() const {
	return rewriter.context;
}

} // namespace duckdb

















namespace duckdb {

using ExpressionValueInformation = FilterCombiner::ExpressionValueInformation;

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right);

FilterCombiner::FilterCombiner(ClientContext &context) : context(context) {
}

FilterCombiner::FilterCombiner(Optimizer &optimizer) : FilterCombiner(optimizer.context) {
}

Expression &FilterCombiner::GetNode(Expression &expr) {
	auto entry = stored_expressions.find(expr);
	if (entry != stored_expressions.end()) {
		// expression already exists: return a reference to the stored expression
		return *entry->second;
	}
	// expression does not exist yet: create a copy and store it
	auto copy = expr.Copy();
	auto &copy_ref = *copy;
	D_ASSERT(stored_expressions.find(copy_ref) == stored_expressions.end());
	stored_expressions[copy_ref] = std::move(copy);
	return copy_ref;
}

idx_t FilterCombiner::GetEquivalenceSet(Expression &expr) {
	D_ASSERT(stored_expressions.find(expr) != stored_expressions.end());
	D_ASSERT(stored_expressions.find(expr)->second.get() == &expr);
	auto entry = equivalence_set_map.find(expr);
	if (entry == equivalence_set_map.end()) {
		idx_t index = set_index++;
		equivalence_set_map[expr] = index;
		equivalence_map[index].push_back(expr);
		constant_values.insert(make_pair(index, vector<ExpressionValueInformation>()));
		return index;
	} else {
		return entry->second;
	}
}

FilterResult FilterCombiner::AddConstantComparison(vector<ExpressionValueInformation> &info_list,
                                                   ExpressionValueInformation info) {
	if (info.constant.IsNull()) {
		return FilterResult::UNSATISFIABLE;
	}
	for (idx_t i = 0; i < info_list.size(); i++) {
		auto comparison = CompareValueInformation(info_list[i], info);
		switch (comparison) {
		case ValueComparisonResult::PRUNE_LEFT:
			// prune the entry from the info list
			info_list.erase(info_list.begin() + i);
			i--;
			break;
		case ValueComparisonResult::PRUNE_RIGHT:
			// prune the current info
			return FilterResult::SUCCESS;
		case ValueComparisonResult::UNSATISFIABLE_CONDITION:
			// combination of filters is unsatisfiable: prune the entire branch
			return FilterResult::UNSATISFIABLE;
		default:
			// prune nothing, move to the next condition
			break;
		}
	}
	// finally add the entry to the list
	info_list.push_back(info);
	return FilterResult::SUCCESS;
}

FilterResult FilterCombiner::AddFilter(unique_ptr<Expression> expr) {
	//	LookUpConjunctions(expr.get());
	// try to push the filter into the combiner
	auto result = AddFilter(*expr);
	if (result == FilterResult::UNSUPPORTED) {
		// unsupported filter, push into remaining filters
		remaining_filters.push_back(std::move(expr));
		return FilterResult::SUCCESS;
	}
	return result;
}

void FilterCombiner::GenerateFilters(const std::function<void(unique_ptr<Expression> filter)> &callback) {
	// first loop over the remaining filters
	for (auto &filter : remaining_filters) {
		callback(std::move(filter));
	}
	remaining_filters.clear();
	// now loop over the equivalence sets
	for (auto &entry : equivalence_map) {
		auto equivalence_set = entry.first;
		auto &entries = entry.second;
		auto &constant_list = constant_values.find(equivalence_set)->second;
		// for each entry generate an equality expression comparing to each other
		for (idx_t i = 0; i < entries.size(); i++) {
			for (idx_t k = i + 1; k < entries.size(); k++) {
				auto comparison = make_uniq<BoundComparisonExpression>(
				    ExpressionType::COMPARE_EQUAL, entries[i].get().Copy(), entries[k].get().Copy());
				callback(std::move(comparison));
			}
			// for each entry also create a comparison with each constant
			int lower_index = -1;
			int upper_index = -1;
			bool lower_inclusive = false;
			bool upper_inclusive = false;
			for (idx_t k = 0; k < constant_list.size(); k++) {
				auto &info = constant_list[k];
				if (info.comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
				    info.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					lower_index = k;
					lower_inclusive = info.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				} else if (info.comparison_type == ExpressionType::COMPARE_LESSTHAN ||
				           info.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
					upper_index = k;
					upper_inclusive = info.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
				} else {
					auto constant = make_uniq<BoundConstantExpression>(info.constant);
					auto comparison = make_uniq<BoundComparisonExpression>(
					    info.comparison_type, entries[i].get().Copy(), std::move(constant));
					callback(std::move(comparison));
				}
			}
			if (lower_index >= 0 && upper_index >= 0) {
				// found both lower and upper index, create a BETWEEN expression
				auto lower_constant = make_uniq<BoundConstantExpression>(constant_list[lower_index].constant);
				auto upper_constant = make_uniq<BoundConstantExpression>(constant_list[upper_index].constant);
				auto between =
				    make_uniq<BoundBetweenExpression>(entries[i].get().Copy(), std::move(lower_constant),
				                                      std::move(upper_constant), lower_inclusive, upper_inclusive);
				callback(std::move(between));
			} else if (lower_index >= 0) {
				// only lower index found, create simple comparison expression
				auto constant = make_uniq<BoundConstantExpression>(constant_list[lower_index].constant);
				auto comparison = make_uniq<BoundComparisonExpression>(constant_list[lower_index].comparison_type,
				                                                       entries[i].get().Copy(), std::move(constant));
				callback(std::move(comparison));
			} else if (upper_index >= 0) {
				// only upper index found, create simple comparison expression
				auto constant = make_uniq<BoundConstantExpression>(constant_list[upper_index].constant);
				auto comparison = make_uniq<BoundComparisonExpression>(constant_list[upper_index].comparison_type,
				                                                       entries[i].get().Copy(), std::move(constant));
				callback(std::move(comparison));
			}
		}
	}
	stored_expressions.clear();
	equivalence_set_map.clear();
	constant_values.clear();
	equivalence_map.clear();
}

bool FilterCombiner::HasFilters() {
	bool has_filters = false;
	GenerateFilters([&](unique_ptr<Expression> child) { has_filters = true; });
	return has_filters;
}

// unordered_map<idx_t, std::pair<Value *, Value *>> MergeAnd(unordered_map<idx_t, std::pair<Value *, Value *>> &f_1,
//                                                            unordered_map<idx_t, std::pair<Value *, Value *>> &f_2) {
// 	unordered_map<idx_t, std::pair<Value *, Value *>> result;
// 	for (auto &f : f_1) {
// 		auto it = f_2.find(f.first);
// 		if (it == f_2.end()) {
// 			result[f.first] = f.second;
// 		} else {
// 			Value *min = nullptr, *max = nullptr;
// 			if (it->second.first && f.second.first) {
// 				if (*f.second.first > *it->second.first) {
// 					min = f.second.first;
// 				} else {
// 					min = it->second.first;
// 				}

// 			} else if (it->second.first) {
// 				min = it->second.first;
// 			} else if (f.second.first) {
// 				min = f.second.first;
// 			} else {
// 				min = nullptr;
// 			}
// 			if (it->second.second && f.second.second) {
// 				if (*f.second.second < *it->second.second) {
// 					max = f.second.second;
// 				} else {
// 					max = it->second.second;
// 				}
// 			} else if (it->second.second) {
// 				max = it->second.second;
// 			} else if (f.second.second) {
// 				max = f.second.second;
// 			} else {
// 				max = nullptr;
// 			}
// 			result[f.first] = {min, max};
// 			f_2.erase(f.first);
// 		}
// 	}
// 	for (auto &f : f_2) {
// 		result[f.first] = f.second;
// 	}
// 	return result;
// }

// unordered_map<idx_t, std::pair<Value *, Value *>> MergeOr(unordered_map<idx_t, std::pair<Value *, Value *>> &f_1,
//                                                           unordered_map<idx_t, std::pair<Value *, Value *>> &f_2) {
// 	unordered_map<idx_t, std::pair<Value *, Value *>> result;
// 	for (auto &f : f_1) {
// 		auto it = f_2.find(f.first);
// 		if (it != f_2.end()) {
// 			Value *min = nullptr, *max = nullptr;
// 			if (it->second.first && f.second.first) {
// 				if (*f.second.first < *it->second.first) {
// 					min = f.second.first;
// 				} else {
// 					min = it->second.first;
// 				}
// 			}
// 			if (it->second.second && f.second.second) {
// 				if (*f.second.second > *it->second.second) {
// 					max = f.second.second;
// 				} else {
// 					max = it->second.second;
// 				}
// 			}
// 			result[f.first] = {min, max};
// 			f_2.erase(f.first);
// 		}
// 	}
// 	return result;
// }

// unordered_map<idx_t, std::pair<Value *, Value *>>
// FilterCombiner::FindZonemapChecks(vector<idx_t> &column_ids, unordered_set<idx_t> &not_constants, Expression *filter)
// { 	unordered_map<idx_t, std::pair<Value *, Value *>> checks; 	switch (filter->type) { 	case
// ExpressionType::CONJUNCTION_OR: {
// 		//! For a filter to
// 		auto &or_exp = filter->Cast<BoundConjunctionExpression>();
// 		checks = FindZonemapChecks(column_ids, not_constants, or_exp.children[0].get());
// 		for (size_t i = 1; i < or_exp.children.size(); ++i) {
// 			auto child_check = FindZonemapChecks(column_ids, not_constants, or_exp.children[i].get());
// 			checks = MergeOr(checks, child_check);
// 		}
// 		return checks;
// 	}
// 	case ExpressionType::CONJUNCTION_AND: {
// 		auto &and_exp = filter->Cast<BoundConjunctionExpression>();
// 		checks = FindZonemapChecks(column_ids, not_constants, and_exp.children[0].get());
// 		for (size_t i = 1; i < and_exp.children.size(); ++i) {
// 			auto child_check = FindZonemapChecks(column_ids, not_constants, and_exp.children[i].get());
// 			checks = MergeAnd(checks, child_check);
// 		}
// 		return checks;
// 	}
// 	case ExpressionType::COMPARE_IN: {
// 		auto &comp_in_exp = filter->Cast<BoundOperatorExpression>();
// 		if (comp_in_exp.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
// 			Value *min = nullptr, *max = nullptr;
// 			auto &column_ref = comp_in_exp.children[0]->Cast<BoundColumnRefExpression>();
// 			for (size_t i {1}; i < comp_in_exp.children.size(); i++) {
// 				if (comp_in_exp.children[i]->type != ExpressionType::VALUE_CONSTANT) {
// 					//! This indicates the column has a comparison that is not with a constant
// 					not_constants.insert(column_ids[column_ref.binding.column_index]);
// 					break;
// 				} else {
// 					auto &const_value_expr = comp_in_exp.children[i]->Cast<BoundConstantExpression>();
// 					if (const_value_expr.value.IsNull()) {
// 						return checks;
// 					}
// 					if (!min && !max) {
// 						min = &const_value_expr.value;
// 						max = min;
// 					} else {
// 						if (*min > const_value_expr.value) {
// 							min = &const_value_expr.value;
// 						}
// 						if (*max < const_value_expr.value) {
// 							max = &const_value_expr.value;
// 						}
// 					}
// 				}
// 			}
// 			checks[column_ids[column_ref.binding.column_index]] = {min, max};
// 		}
// 		return checks;
// 	}
// 	case ExpressionType::COMPARE_EQUAL: {
// 		auto &comp_exp = filter->Cast<BoundComparisonExpression>();
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_CONSTANT)) {
// 			auto &column_ref = comp_exp.left->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.right->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {&constant_value_expr.value,
// 			                                                       &constant_value_expr.value};
// 		}
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_CONSTANT &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF)) {
// 			auto &column_ref = comp_exp.right->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.left->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {&constant_value_expr.value,
// 			                                                       &constant_value_expr.value};
// 		}
// 		return checks;
// 	}
// 	case ExpressionType::COMPARE_LESSTHAN:
// 	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
// 		auto &comp_exp = filter->Cast<BoundComparisonExpression>();
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_CONSTANT)) {
// 			auto &column_ref = comp_exp.left->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.right->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {nullptr, &constant_value_expr.value};
// 		}
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_CONSTANT &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF)) {
// 			auto &column_ref = comp_exp.right->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.left->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {&constant_value_expr.value, nullptr};
// 		}
// 		return checks;
// 	}
// 	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
// 	case ExpressionType::COMPARE_GREATERTHAN: {
// 		auto &comp_exp = filter->Cast<BoundComparisonExpression>();
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_CONSTANT)) {
// 			auto &column_ref = comp_exp.left->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.right->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {&constant_value_expr.value, nullptr};
// 		}
// 		if ((comp_exp.left->expression_class == ExpressionClass::BOUND_CONSTANT &&
// 		     comp_exp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF)) {
// 			auto &column_ref = comp_exp.right->Cast<BoundColumnRefExpression>();
// 			auto &constant_value_expr = comp_exp.left->Cast<BoundConstantExpression>();
// 			checks[column_ids[column_ref.binding.column_index]] = {nullptr, &constant_value_expr.value};
// 		}
// 		return checks;
// 	}
// 	default:
// 		return checks;
// 	}
// }

// vector<TableFilter> FilterCombiner::GenerateZonemapChecks(vector<idx_t> &column_ids,
//                                                           vector<TableFilter> &pushed_filters) {
// 	vector<TableFilter> zonemap_checks;
// 	unordered_set<idx_t> not_constants;
// 	//! We go through the remaining filters and capture their min max
// 	if (remaining_filters.empty()) {
// 		return zonemap_checks;
// 	}

// 	auto checks = FindZonemapChecks(column_ids, not_constants, remaining_filters[0].get());
// 	for (size_t i = 1; i < remaining_filters.size(); ++i) {
// 		auto child_check = FindZonemapChecks(column_ids, not_constants, remaining_filters[i].get());
// 		checks = MergeAnd(checks, child_check);
// 	}
// 	//! We construct the equivalent filters
// 	for (auto not_constant : not_constants) {
// 		checks.erase(not_constant);
// 	}
// 	for (const auto &pushed_filter : pushed_filters) {
// 		checks.erase(column_ids[pushed_filter.column_index]);
// 	}
// 	for (const auto &check : checks) {
// 		if (check.second.first) {
// 			zonemap_checks.emplace_back(check.second.first->Copy(), ExpressionType::COMPARE_GREATERTHANOREQUALTO,
// 			                            check.first);
// 		}
// 		if (check.second.second) {
// 			zonemap_checks.emplace_back(check.second.second->Copy(), ExpressionType::COMPARE_LESSTHANOREQUALTO,
// 			                            check.first);
// 		}
// 	}
// 	return zonemap_checks;
// }

TableFilterSet FilterCombiner::GenerateTableScanFilters(vector<idx_t> &column_ids) {
	TableFilterSet table_filters;
	//! First, we figure the filters that have constant expressions that we can push down to the table scan
	for (auto &constant_value : constant_values) {
		if (!constant_value.second.empty()) {
			auto filter_exp = equivalence_map.end();
			if ((constant_value.second[0].comparison_type == ExpressionType::COMPARE_EQUAL ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			     constant_value.second[0].comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) &&
			    (TypeIsNumeric(constant_value.second[0].constant.type().InternalType()) ||
			     constant_value.second[0].constant.type().InternalType() == PhysicalType::VARCHAR ||
			     constant_value.second[0].constant.type().InternalType() == PhysicalType::BOOL)) {
				//! Here we check if these filters are column references
				filter_exp = equivalence_map.find(constant_value.first);
				if (filter_exp->second.size() == 1 &&
				    filter_exp->second[0].get().type == ExpressionType::BOUND_COLUMN_REF) {
					auto &filter_col_exp = filter_exp->second[0].get().Cast<BoundColumnRefExpression>();
					auto column_index = column_ids[filter_col_exp.binding.column_index];
					if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
						break;
					}
					auto equivalence_set = filter_exp->first;
					auto &entries = filter_exp->second;
					auto &constant_list = constant_values.find(equivalence_set)->second;
					// for each entry generate an equality expression comparing to each other
					for (idx_t i = 0; i < entries.size(); i++) {
						// for each entry also create a comparison with each constant
						for (idx_t k = 0; k < constant_list.size(); k++) {
							auto constant_filter = make_uniq<ConstantFilter>(constant_value.second[k].comparison_type,
							                                                 constant_value.second[k].constant);
							table_filters.PushFilter(column_index, std::move(constant_filter));
						}
						table_filters.PushFilter(column_index, make_uniq<IsNotNullFilter>());
					}
					equivalence_map.erase(filter_exp);
				}
			}
		}
	}
	//! Here we look for LIKE or IN filters
	for (idx_t rem_fil_idx = 0; rem_fil_idx < remaining_filters.size(); rem_fil_idx++) {
		auto &remaining_filter = remaining_filters[rem_fil_idx];
		if (remaining_filter->expression_class == ExpressionClass::BOUND_FUNCTION) {
			auto &func = remaining_filter->Cast<BoundFunctionExpression>();
			if (func.function.name == "prefix" &&
			    func.children[0]->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->type == ExpressionType::VALUE_CONSTANT) {
				//! This is a like function.
				auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant_value_expr = func.children[1]->Cast<BoundConstantExpression>();
				auto like_string = StringValue::Get(constant_value_expr.value);
				if (like_string.empty()) {
					continue;
				}
				auto column_index = column_ids[column_ref.binding.column_index];
				//! Here the like must be transformed to a BOUND COMPARISON geq le
				auto lower_bound =
				    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value(like_string));
				like_string[like_string.size() - 1]++;
				auto upper_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, Value(like_string));
				table_filters.PushFilter(column_index, std::move(lower_bound));
				table_filters.PushFilter(column_index, std::move(upper_bound));
				table_filters.PushFilter(column_index, make_uniq<IsNotNullFilter>());
			}
			if (func.function.name == "~~" && func.children[0]->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
			    func.children[1]->type == ExpressionType::VALUE_CONSTANT) {
				//! This is a like function.
				auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
				auto &constant_value_expr = func.children[1]->Cast<BoundConstantExpression>();
				auto &like_string = StringValue::Get(constant_value_expr.value);
				if (like_string[0] == '%' || like_string[0] == '_') {
					//! We have no prefix so nothing to pushdown
					break;
				}
				string prefix;
				bool equality = true;
				for (char const &c : like_string) {
					if (c == '%' || c == '_') {
						equality = false;
						break;
					}
					prefix += c;
				}
				auto column_index = column_ids[column_ref.binding.column_index];
				if (equality) {
					//! Here the like can be transformed to an equality query
					auto equal_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(prefix));
					table_filters.PushFilter(column_index, std::move(equal_filter));
					table_filters.PushFilter(column_index, make_uniq<IsNotNullFilter>());
				} else {
					//! Here the like must be transformed to a BOUND COMPARISON geq le
					auto lower_bound =
					    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value(prefix));
					prefix[prefix.size() - 1]++;
					auto upper_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHAN, Value(prefix));
					table_filters.PushFilter(column_index, std::move(lower_bound));
					table_filters.PushFilter(column_index, std::move(upper_bound));
					table_filters.PushFilter(column_index, make_uniq<IsNotNullFilter>());
				}
			}
		} else if (remaining_filter->type == ExpressionType::COMPARE_IN) {
			auto &func = remaining_filter->Cast<BoundOperatorExpression>();
			vector<hugeint_t> in_values;
			D_ASSERT(func.children.size() > 1);
			if (func.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
				continue;
			}
			auto &column_ref = func.children[0]->Cast<BoundColumnRefExpression>();
			auto column_index = column_ids[column_ref.binding.column_index];
			if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
				break;
			}
			//! check if all children are const expr
			bool children_constant = true;
			for (size_t i {1}; i < func.children.size(); i++) {
				if (func.children[i]->type != ExpressionType::VALUE_CONSTANT) {
					children_constant = false;
				}
			}
			if (!children_constant) {
				continue;
			}
			auto &fst_const_value_expr = func.children[1]->Cast<BoundConstantExpression>();
			auto &type = fst_const_value_expr.value.type();

			//! Check if values are consecutive, if yes transform them to >= <= (only for integers)
			// e.g. if we have x IN (1, 2, 3, 4, 5) we transform this into x >= 1 AND x <= 5
			if (!type.IsIntegral()) {
				continue;
			}

			bool can_simplify_in_clause = true;
			for (idx_t i = 1; i < func.children.size(); i++) {
				auto &const_value_expr = func.children[i]->Cast<BoundConstantExpression>();
				if (const_value_expr.value.IsNull()) {
					can_simplify_in_clause = false;
					break;
				}
				in_values.push_back(const_value_expr.value.GetValue<hugeint_t>());
			}
			if (!can_simplify_in_clause || in_values.empty()) {
				continue;
			}

			sort(in_values.begin(), in_values.end());

			for (idx_t in_val_idx = 1; in_val_idx < in_values.size(); in_val_idx++) {
				if (in_values[in_val_idx] - in_values[in_val_idx - 1] > 1) {
					can_simplify_in_clause = false;
					break;
				}
			}
			if (!can_simplify_in_clause) {
				continue;
			}
			auto lower_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			                                             Value::Numeric(type, in_values.front()));
			auto upper_bound = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
			                                             Value::Numeric(type, in_values.back()));
			table_filters.PushFilter(column_index, std::move(lower_bound));
			table_filters.PushFilter(column_index, std::move(upper_bound));
			table_filters.PushFilter(column_index, make_uniq<IsNotNullFilter>());

			remaining_filters.erase(remaining_filters.begin() + rem_fil_idx);
		}
	}

	//	GenerateORFilters(table_filters, column_ids);

	return table_filters;
}

static bool IsGreaterThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

static bool IsLessThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
}

FilterResult FilterCombiner::AddBoundComparisonFilter(Expression &expr) {
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	if (comparison.type != ExpressionType::COMPARE_LESSTHAN &&
	    comparison.type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
	    comparison.type != ExpressionType::COMPARE_GREATERTHAN &&
	    comparison.type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
	    comparison.type != ExpressionType::COMPARE_EQUAL && comparison.type != ExpressionType::COMPARE_NOTEQUAL) {
		// only support [>, >=, <, <=, ==, !=] expressions
		return FilterResult::UNSUPPORTED;
	}
	// check if one of the sides is a scalar value
	bool left_is_scalar = comparison.left->IsFoldable();
	bool right_is_scalar = comparison.right->IsFoldable();
	if (left_is_scalar || right_is_scalar) {
		// comparison with scalar
		auto &node = GetNode(left_is_scalar ? *comparison.right : *comparison.left);
		idx_t equivalence_set = GetEquivalenceSet(node);
		auto &scalar = left_is_scalar ? comparison.left : comparison.right;
		Value constant_value;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
			return FilterResult::UNSATISFIABLE;
		}
		if (constant_value.IsNull()) {
			// comparisons with null are always null (i.e. will never result in rows)
			return FilterResult::UNSATISFIABLE;
		}

		// create the ExpressionValueInformation
		ExpressionValueInformation info;
		info.comparison_type = left_is_scalar ? FlipComparisonExpression(comparison.type) : comparison.type;
		info.constant = constant_value;

		// get the current bucket of constant values
		D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
		auto &info_list = constant_values.find(equivalence_set)->second;
		D_ASSERT(node.return_type == info.constant.type());
		// check the existing constant comparisons to see if we can do any pruning
		auto ret = AddConstantComparison(info_list, info);

		auto &non_scalar = left_is_scalar ? *comparison.right : *comparison.left;
		auto transitive_filter = FindTransitiveFilter(non_scalar);
		if (transitive_filter != nullptr) {
			// try to add transitive filters
			if (AddTransitiveFilters(transitive_filter->Cast<BoundComparisonExpression>()) ==
			    FilterResult::UNSUPPORTED) {
				// in case of unsuccessful re-add filter into remaining ones
				remaining_filters.push_back(std::move(transitive_filter));
			}
		}
		return ret;
	} else {
		// comparison between two non-scalars
		// only handle comparisons for now
		if (expr.type != ExpressionType::COMPARE_EQUAL) {
			if (IsGreaterThan(expr.type) || IsLessThan(expr.type)) {
				return AddTransitiveFilters(comparison);
			}
			return FilterResult::UNSUPPORTED;
		}
		// get the LHS and RHS nodes
		auto &left_node = GetNode(*comparison.left);
		auto &right_node = GetNode(*comparison.right);
		if (left_node.Equals(right_node)) {
			return FilterResult::UNSUPPORTED;
		}
		// get the equivalence sets of the LHS and RHS
		auto left_equivalence_set = GetEquivalenceSet(left_node);
		auto right_equivalence_set = GetEquivalenceSet(right_node);
		if (left_equivalence_set == right_equivalence_set) {
			// this equality filter already exists, prune it
			return FilterResult::SUCCESS;
		}
		// add the right bucket into the left bucket
		D_ASSERT(equivalence_map.find(left_equivalence_set) != equivalence_map.end());
		D_ASSERT(equivalence_map.find(right_equivalence_set) != equivalence_map.end());

		auto &left_bucket = equivalence_map.find(left_equivalence_set)->second;
		auto &right_bucket = equivalence_map.find(right_equivalence_set)->second;
		for (auto &right_expr : right_bucket) {
			// rewrite the equivalence set mapping for this node
			equivalence_set_map[right_expr] = left_equivalence_set;
			// add the node to the left bucket
			left_bucket.push_back(right_expr);
		}
		// now add all constant values from the right bucket to the left bucket
		D_ASSERT(constant_values.find(left_equivalence_set) != constant_values.end());
		D_ASSERT(constant_values.find(right_equivalence_set) != constant_values.end());
		auto &left_constant_bucket = constant_values.find(left_equivalence_set)->second;
		auto &right_constant_bucket = constant_values.find(right_equivalence_set)->second;
		for (auto &right_constant : right_constant_bucket) {
			if (AddConstantComparison(left_constant_bucket, right_constant) == FilterResult::UNSATISFIABLE) {
				return FilterResult::UNSATISFIABLE;
			}
		}
	}
	return FilterResult::SUCCESS;
}

FilterResult FilterCombiner::AddFilter(Expression &expr) {
	if (expr.HasParameter()) {
		return FilterResult::UNSUPPORTED;
	}
	if (expr.IsFoldable()) {
		// scalar condition, evaluate it
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(context, expr, result)) {
			return FilterResult::UNSUPPORTED;
		}
		result = result.DefaultCastAs(LogicalType::BOOLEAN);
		// check if the filter passes
		if (result.IsNull() || !BooleanValue::Get(result)) {
			// the filter does not pass the scalar test, create an empty result
			return FilterResult::UNSATISFIABLE;
		} else {
			// the filter passes the scalar test, just remove the condition
			return FilterResult::SUCCESS;
		}
	}
	D_ASSERT(!expr.IsFoldable());
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &comparison = expr.Cast<BoundBetweenExpression>();
		//! check if one of the sides is a scalar value
		bool lower_is_scalar = comparison.lower->IsFoldable();
		bool upper_is_scalar = comparison.upper->IsFoldable();
		if (lower_is_scalar || upper_is_scalar) {
			//! comparison with scalar - break apart
			auto &node = GetNode(*comparison.input);
			idx_t equivalence_set = GetEquivalenceSet(node);
			auto result = FilterResult::UNSATISFIABLE;

			if (lower_is_scalar) {
				auto scalar = comparison.lower.get();
				Value constant_value;
				if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
					return FilterResult::UNSUPPORTED;
				}

				// create the ExpressionValueInformation
				ExpressionValueInformation info;
				if (comparison.lower_inclusive) {
					info.comparison_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
				} else {
					info.comparison_type = ExpressionType::COMPARE_GREATERTHAN;
				}
				info.constant = constant_value;

				// get the current bucket of constant values
				D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
				auto &info_list = constant_values.find(equivalence_set)->second;
				// check the existing constant comparisons to see if we can do any pruning
				result = AddConstantComparison(info_list, info);
			} else {
				D_ASSERT(upper_is_scalar);
				const auto type = comparison.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                             : ExpressionType::COMPARE_LESSTHAN;
				auto left = comparison.lower->Copy();
				auto right = comparison.input->Copy();
				auto lower_comp = make_uniq<BoundComparisonExpression>(type, std::move(left), std::move(right));
				result = AddBoundComparisonFilter(*lower_comp);
			}

			//	 Stop if we failed
			if (result != FilterResult::SUCCESS) {
				return result;
			}

			if (upper_is_scalar) {
				auto scalar = comparison.upper.get();
				Value constant_value;
				if (!ExpressionExecutor::TryEvaluateScalar(context, *scalar, constant_value)) {
					return FilterResult::UNSUPPORTED;
				}

				// create the ExpressionValueInformation
				ExpressionValueInformation info;
				if (comparison.upper_inclusive) {
					info.comparison_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
				} else {
					info.comparison_type = ExpressionType::COMPARE_LESSTHAN;
				}
				info.constant = constant_value;

				// get the current bucket of constant values
				D_ASSERT(constant_values.find(equivalence_set) != constant_values.end());
				// check the existing constant comparisons to see if we can do any pruning
				result = AddConstantComparison(constant_values.find(equivalence_set)->second, info);
			} else {
				D_ASSERT(lower_is_scalar);
				const auto type = comparison.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                             : ExpressionType::COMPARE_LESSTHAN;
				auto left = comparison.input->Copy();
				auto right = comparison.upper->Copy();
				auto upper_comp = make_uniq<BoundComparisonExpression>(type, std::move(left), std::move(right));
				result = AddBoundComparisonFilter(*upper_comp);
			}

			return result;
		}
	} else if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		return AddBoundComparisonFilter(expr);
	}
	// only comparisons supported for now
	return FilterResult::UNSUPPORTED;
}

/*
 * Create and add new transitive filters from a two non-scalar filter such as j > i, j >= i, j < i, and j <= i
 * It's missing to create another method to add transitive filters from scalar filters, e.g, i > 10
 */
FilterResult FilterCombiner::AddTransitiveFilters(BoundComparisonExpression &comparison) {
	D_ASSERT(IsGreaterThan(comparison.type) || IsLessThan(comparison.type));
	// get the LHS and RHS nodes
	auto &left_node = GetNode(*comparison.left);
	reference<Expression> right_node = GetNode(*comparison.right);
	// In case with filters like CAST(i) = j and i = 5 we replace the COLUMN_REF i with the constant 5
	if (right_node.get().type == ExpressionType::OPERATOR_CAST) {
		auto &bound_cast_expr = right_node.get().Cast<BoundCastExpression>();
		if (bound_cast_expr.child->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &col_ref = bound_cast_expr.child->Cast<BoundColumnRefExpression>();
			for (auto &stored_exp : stored_expressions) {
				if (stored_exp.first.get().type == ExpressionType::BOUND_COLUMN_REF) {
					auto &st_col_ref = stored_exp.second->Cast<BoundColumnRefExpression>();
					if (st_col_ref.binding == col_ref.binding &&
					    bound_cast_expr.return_type == stored_exp.second->return_type) {
						bound_cast_expr.child = stored_exp.second->Copy();
						right_node = GetNode(*bound_cast_expr.child);
						break;
					}
				}
			}
		}
	}

	if (left_node.Equals(right_node)) {
		return FilterResult::UNSUPPORTED;
	}
	// get the equivalence sets of the LHS and RHS
	idx_t left_equivalence_set = GetEquivalenceSet(left_node);
	idx_t right_equivalence_set = GetEquivalenceSet(right_node);
	if (left_equivalence_set == right_equivalence_set) {
		// this equality filter already exists, prune it
		return FilterResult::SUCCESS;
	}

	vector<ExpressionValueInformation> &left_constants = constant_values.find(left_equivalence_set)->second;
	vector<ExpressionValueInformation> &right_constants = constant_values.find(right_equivalence_set)->second;
	bool is_successful = false;
	bool is_inserted = false;
	// read every constant filters already inserted for the right scalar variable
	// and see if we can create new transitive filters, e.g., there is already a filter i > 10,
	// suppose that we have now the j >= i, then we can infer a new filter j > 10
	for (const auto &right_constant : right_constants) {
		ExpressionValueInformation info;
		info.constant = right_constant.constant;
		// there is already an equality filter, e.g., i = 10
		if (right_constant.comparison_type == ExpressionType::COMPARE_EQUAL) {
			// create filter j [>, >=, <, <=] 10
			// suppose the new comparison is j >= i and we have already a filter i = 10,
			// then we create a new filter j >= 10
			// and the filter j >= i can be pruned by not adding it into the remaining filters
			info.comparison_type = comparison.type;
		} else if ((comparison.type == ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
		            IsGreaterThan(right_constant.comparison_type)) ||
		           (comparison.type == ExpressionType::COMPARE_LESSTHANOREQUALTO &&
		            IsLessThan(right_constant.comparison_type))) {
			// filters (j >= i AND i [>, >=] 10) OR (j <= i AND i [<, <=] 10)
			// create filter j [>, >=] 10 and add the filter j [>=, <=] i into the remaining filters
			info.comparison_type = right_constant.comparison_type; // create filter j [>, >=, <, <=] 10
			if (!is_inserted) {
				// Add the filter j >= i in the remaing filters
				auto filter = make_uniq<BoundComparisonExpression>(comparison.type, comparison.left->Copy(),
				                                                   comparison.right->Copy());
				remaining_filters.push_back(std::move(filter));
				is_inserted = true;
			}
		} else if ((comparison.type == ExpressionType::COMPARE_GREATERTHAN &&
		            IsGreaterThan(right_constant.comparison_type)) ||
		           (comparison.type == ExpressionType::COMPARE_LESSTHAN &&
		            IsLessThan(right_constant.comparison_type))) {
			// filters (j > i AND i [>, >=] 10) OR j < i AND i [<, <=] 10
			// create filter j [>, <] 10 and add the filter j [>, <] i into the remaining filters
			// the comparisons j > i and j < i are more restrictive
			info.comparison_type = comparison.type;
			if (!is_inserted) {
				// Add the filter j [>, <] i
				auto filter = make_uniq<BoundComparisonExpression>(comparison.type, comparison.left->Copy(),
				                                                   comparison.right->Copy());
				remaining_filters.push_back(std::move(filter));
				is_inserted = true;
			}
		} else {
			// we cannot add a new filter
			continue;
		}
		// Add the new filer into the left set
		if (AddConstantComparison(left_constants, info) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
		is_successful = true;
	}
	if (is_successful) {
		// now check for remaining trasitive filters from the left column
		auto transitive_filter = FindTransitiveFilter(*comparison.left);
		if (transitive_filter != nullptr) {
			// try to add transitive filters
			if (AddTransitiveFilters(transitive_filter->Cast<BoundComparisonExpression>()) ==
			    FilterResult::UNSUPPORTED) {
				// in case of unsuccessful re-add filter into remaining ones
				remaining_filters.push_back(std::move(transitive_filter));
			}
		}
		return FilterResult::SUCCESS;
	}

	return FilterResult::UNSUPPORTED;
}

/*
 * Find a transitive filter already inserted into the remaining filters
 * Check for a match between the right column of bound comparisons and the expression,
 * then removes the bound comparison from the remaining filters and returns it
 */
unique_ptr<Expression> FilterCombiner::FindTransitiveFilter(Expression &expr) {
	// We only check for bound column ref
	if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	for (idx_t i = 0; i < remaining_filters.size(); i++) {
		if (remaining_filters[i]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = remaining_filters[i]->Cast<BoundComparisonExpression>();
			if (expr.Equals(*comparison.right) && comparison.type != ExpressionType::COMPARE_NOTEQUAL) {
				auto filter = std::move(remaining_filters[i]);
				remaining_filters.erase(remaining_filters.begin() + i);
				return filter;
			}
		}
	}
	return nullptr;
}

ValueComparisonResult InvertValueComparisonResult(ValueComparisonResult result) {
	if (result == ValueComparisonResult::PRUNE_RIGHT) {
		return ValueComparisonResult::PRUNE_LEFT;
	}
	if (result == ValueComparisonResult::PRUNE_LEFT) {
		return ValueComparisonResult::PRUNE_RIGHT;
	}
	return result;
}

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right) {
	if (left.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// left is COMPARE_EQUAL, we can either
		// (1) prune the right side or
		// (2) return UNSATISFIABLE
		bool prune_right_side = false;
		switch (right.comparison_type) {
		case ExpressionType::COMPARE_LESSTHAN:
			prune_right_side = left.constant < right.constant;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			prune_right_side = left.constant <= right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			prune_right_side = left.constant > right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			prune_right_side = left.constant >= right.constant;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			prune_right_side = left.constant != right.constant;
			break;
		default:
			D_ASSERT(right.comparison_type == ExpressionType::COMPARE_EQUAL);
			prune_right_side = left.constant == right.constant;
			break;
		}
		if (prune_right_side) {
			return ValueComparisonResult::PRUNE_RIGHT;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else if (right.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// right is COMPARE_EQUAL
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	} else if (left.comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
		// left is COMPARE_NOTEQUAL, we can either
		// (1) prune the left side or
		// (2) not prune anything
		bool prune_left_side = false;
		switch (right.comparison_type) {
		case ExpressionType::COMPARE_LESSTHAN:
			prune_left_side = left.constant >= right.constant;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			prune_left_side = left.constant > right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			prune_left_side = left.constant <= right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			prune_left_side = left.constant < right.constant;
			break;
		default:
			D_ASSERT(right.comparison_type == ExpressionType::COMPARE_NOTEQUAL);
			prune_left_side = left.constant == right.constant;
			break;
		}
		if (prune_left_side) {
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			return ValueComparisonResult::PRUNE_NOTHING;
		}
	} else if (right.comparison_type == ExpressionType::COMPARE_NOTEQUAL) {
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	} else if (IsGreaterThan(left.comparison_type) && IsGreaterThan(right.comparison_type)) {
		// both comparisons are [>], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant > right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant < right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [>=] and the other is [>]
			// we want to prune the [>=] because [>] is more selective
			// if left is [>=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type) && IsLessThan(right.comparison_type)) {
		// both comparisons are [<], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant < right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant > right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [<=] and the other is [<]
			// we want to prune the [<=] because [<] is more selective
			// if left is [<=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type)) {
		D_ASSERT(IsGreaterThan(right.comparison_type));
		// left is [<] and right is [>], in this case we can either
		// (1) prune nothing or
		// (2) return UNSATISFIABLE
		// the SMALLER THAN constant has to be greater than the BIGGER THAN constant
		if (left.constant >= right.constant) {
			return ValueComparisonResult::PRUNE_NOTHING;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else {
		// left is [>] and right is [<] or [!=]
		D_ASSERT(IsLessThan(right.comparison_type) && IsGreaterThan(left.comparison_type));
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	}
}
//
// void FilterCombiner::LookUpConjunctions(Expression *expr) {
//	if (expr->GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
//		auto root_or_expr = (BoundConjunctionExpression *)expr;
//		for (const auto &entry : map_col_conjunctions) {
//			for (const auto &conjs_to_push : entry.second) {
//				if (conjs_to_push->root_or->Equals(root_or_expr)) {
//					return;
//				}
//			}
//		}
//
//		cur_root_or = root_or_expr;
//		cur_conjunction = root_or_expr;
//		cur_colref_to_push = nullptr;
//		if (!BFSLookUpConjunctions(cur_root_or)) {
//			if (cur_colref_to_push) {
//				auto entry = map_col_conjunctions.find(cur_colref_to_push);
//				auto &vec_conjs_to_push = entry->second;
//				if (vec_conjs_to_push.size() == 1) {
//					map_col_conjunctions.erase(entry);
//					return;
//				}
//				vec_conjs_to_push.pop_back();
//			}
//		}
//		return;
//	}
//
//	// Verify if the expression has a column already pushed down by other OR expression
//	VerifyOrsToPush(*expr);
//}
//
// bool FilterCombiner::BFSLookUpConjunctions(BoundConjunctionExpression *conjunction) {
//	vector<BoundConjunctionExpression *> conjunctions_to_visit;
//
//	for (auto &child : conjunction->children) {
//		switch (child->GetExpressionClass()) {
//		case ExpressionClass::BOUND_CONJUNCTION: {
//			auto child_conjunction = (BoundConjunctionExpression *)child.get();
//			conjunctions_to_visit.emplace_back(child_conjunction);
//			break;
//		}
//		case ExpressionClass::BOUND_COMPARISON: {
//			if (!UpdateConjunctionFilter((BoundComparisonExpression *)child.get())) {
//				return false;
//			}
//			break;
//		}
//		default: {
//			return false;
//		}
//		}
//	}
//
//	for (auto child_conjunction : conjunctions_to_visit) {
//		cur_conjunction = child_conjunction;
//		// traverse child conjuction
//		if (!BFSLookUpConjunctions(child_conjunction)) {
//			return false;
//		}
//	}
//	return true;
//}
//
// void FilterCombiner::VerifyOrsToPush(Expression &expr) {
//	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
//		auto colref = (BoundColumnRefExpression *)&expr;
//		auto entry = map_col_conjunctions.find(colref);
//		if (entry == map_col_conjunctions.end()) {
//			return;
//		}
//		map_col_conjunctions.erase(entry);
//	}
//	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { VerifyOrsToPush(child); });
//}
//
// bool FilterCombiner::UpdateConjunctionFilter(BoundComparisonExpression *comparison_expr) {
//	bool left_is_scalar = comparison_expr->left->IsFoldable();
//	bool right_is_scalar = comparison_expr->right->IsFoldable();
//
//	Expression *non_scalar_expr;
//	if (left_is_scalar || right_is_scalar) {
//		// only support comparison with scalar
//		non_scalar_expr = left_is_scalar ? comparison_expr->right.get() : comparison_expr->left.get();
//
//		if (non_scalar_expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
//			return UpdateFilterByColumn((BoundColumnRefExpression *)non_scalar_expr, comparison_expr);
//		}
//	}
//
//	return false;
//}
//
// bool FilterCombiner::UpdateFilterByColumn(BoundColumnRefExpression *column_ref,
//                                          BoundComparisonExpression *comparison_expr) {
//	if (cur_colref_to_push == nullptr) {
//		cur_colref_to_push = column_ref;
//
//		auto or_conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR);
//		or_conjunction->children.emplace_back(comparison_expr->Copy());
//
//		unique_ptr<ConjunctionsToPush> conjs_to_push = make_uniq<ConjunctionsToPush>();
//		conjs_to_push->conjunctions.emplace_back(std::move(or_conjunction));
//		conjs_to_push->root_or = cur_root_or;
//
//		auto &&vec_col_conjs = map_col_conjunctions[column_ref];
//		vec_col_conjs.emplace_back(std::move(conjs_to_push));
//		vec_colref_insertion_order.emplace_back(column_ref);
//		return true;
//	}
//
//	auto entry = map_col_conjunctions.find(cur_colref_to_push);
//	D_ASSERT(entry != map_col_conjunctions.end());
//	auto &conjunctions_to_push = entry->second.back();
//
//	if (!cur_colref_to_push->Equals(column_ref)) {
//		// check for multiple colunms in the same root OR node
//		if (cur_root_or == cur_conjunction) {
//			return false;
//		}
//		// found an AND using a different column, we should stop the look up
//		if (cur_conjunction->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
//			return false;
//		}
//
//		// found a different column, AND conditions cannot be preserved anymore
//		conjunctions_to_push->preserve_and = false;
//		return true;
//	}
//
//	auto &last_conjunction = conjunctions_to_push->conjunctions.back();
//	if (cur_conjunction->GetExpressionType() == last_conjunction->GetExpressionType()) {
//		last_conjunction->children.emplace_back(comparison_expr->Copy());
//	} else {
//		auto new_conjunction = make_uniq<BoundConjunctionExpression>(cur_conjunction->GetExpressionType());
//		new_conjunction->children.emplace_back(comparison_expr->Copy());
//		conjunctions_to_push->conjunctions.emplace_back(std::move(new_conjunction));
//	}
//	return true;
//}
//
// void FilterCombiner::GenerateORFilters(TableFilterSet &table_filter, vector<idx_t> &column_ids) {
//	for (const auto colref : vec_colref_insertion_order) {
//		auto column_index = column_ids[colref->binding.column_index];
//		if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
//			break;
//		}
//
//		for (const auto &conjunctions_to_push : map_col_conjunctions[colref]) {
//			// root OR filter to push into the TableFilter
//			auto root_or_filter = make_uniq<ConjunctionOrFilter>();
//			// variable to hold the last conjuntion filter pointer
//			// the next filter will be added into it, i.e., we create a chain of conjunction filters
//			ConjunctionFilter *last_conj_filter = root_or_filter.get();
//
//			for (auto &conjunction : conjunctions_to_push->conjunctions) {
//				if (conjunction->GetExpressionType() == ExpressionType::CONJUNCTION_AND &&
//				    conjunctions_to_push->preserve_and) {
//					GenerateConjunctionFilter<ConjunctionAndFilter>(conjunction.get(), last_conj_filter);
//				} else {
//					GenerateConjunctionFilter<ConjunctionOrFilter>(conjunction.get(), last_conj_filter);
//				}
//			}
//			table_filter.PushFilter(column_index, std::move(root_or_filter));
//		}
//	}
//	map_col_conjunctions.clear();
//	vec_colref_insertion_order.clear();
//}

} // namespace duckdb



namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return PullupFilter(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PullupProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PullupCrossProduct(std::move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return PullupJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return PullupSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just pull directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	default:
		return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();

	switch (join.join_type) {
	case JoinType::INNER:
		return PullupInnerJoin(std::move(op));
	case JoinType::LEFT:
	case JoinType::ANTI:
	case JoinType::SEMI: {
		return PullupFromLeft(std::move(op));
	}
	default:
		// unsupported join type: call children pull up
		return FinishPullup(std::move(op));
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupInnerJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->Cast<LogicalJoin>().join_type == JoinType::INNER);
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return op;
	}
	return PullupBothSide(std::move(op));
}

unique_ptr<LogicalOperator> FilterPullup::PullupCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	return PullupBothSide(std::move(op));
}

unique_ptr<LogicalOperator> FilterPullup::GeneratePullupFilter(unique_ptr<LogicalOperator> child,
                                                               vector<unique_ptr<Expression>> &expressions) {
	unique_ptr<LogicalFilter> filter = make_uniq<LogicalFilter>();
	for (idx_t i = 0; i < expressions.size(); ++i) {
		filter->expressions.push_back(std::move(expressions[i]));
	}
	expressions.clear();
	filter->children.push_back(std::move(child));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPullup::FinishPullup(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		FilterPullup pullup;
		op->children[i] = pullup.Rewrite(std::move(op->children[i]));
	}
	// now pull up any existing filters
	if (filters_expr_pullup.empty()) {
		// no filters to pull up
		return op;
	}
	return GeneratePullupFilter(std::move(op), filters_expr_pullup);
}

} // namespace duckdb







namespace duckdb {

using Filter = FilterPushdown::Filter;

FilterPushdown::FilterPushdown(Optimizer &optimizer) : optimizer(optimizer), combiner(optimizer.context) {
}

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	D_ASSERT(!combiner.HasFilters());
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return PushdownAggregate(std::move(op));
	case LogicalOperatorType::LOGICAL_FILTER:
		return PushdownFilter(std::move(op));
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return PushdownCrossProduct(std::move(op));
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PushdownJoin(std::move(op));
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return PushdownProjection(std::move(op));
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_UNION:
		return PushdownSetOperation(std::move(op));
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// we can just push directly through these operations without any rewriting
		op->children[0] = Rewrite(std::move(op->children[0]));
		return op;
	}
	case LogicalOperatorType::LOGICAL_GET:
		return PushdownGet(std::move(op));
	case LogicalOperatorType::LOGICAL_LIMIT:
		return PushdownLimit(std::move(op));
	default:
		return FinishPushdown(std::move(op));
	}
}

ClientContext &FilterPushdown::GetContext() {
	return optimizer.GetContext();
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);
	auto &join = op->Cast<LogicalJoin>();
	if (!join.left_projection_map.empty() || !join.right_projection_map.empty()) {
		// cannot push down further otherwise the projection maps won't be preserved
		return FinishPushdown(std::move(op));
	}

	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	switch (join.join_type) {
	case JoinType::INNER:
		return PushdownInnerJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(std::move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(std::move(op), left_bindings, right_bindings);
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(std::move(op));
	}
}
void FilterPushdown::PushFilters() {
	for (auto &f : filters) {
		auto result = combiner.AddFilter(std::move(f->filter));
		D_ASSERT(result != FilterResult::UNSUPPORTED);
		(void)result;
	}
	filters.clear();
}

FilterResult FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	PushFilters();
	// split up the filters by AND predicate
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(expr));
	LogicalFilter::SplitPredicates(expressions);
	// push the filters into the combiner
	for (auto &child_expr : expressions) {
		if (combiner.AddFilter(std::move(child_expr)) == FilterResult::UNSATISFIABLE) {
			return FilterResult::UNSATISFIABLE;
		}
	}
	return FilterResult::SUCCESS;
}

void FilterPushdown::GenerateFilters() {
	if (!filters.empty()) {
		D_ASSERT(!combiner.HasFilters());
		return;
	}
	combiner.GenerateFilters([&](unique_ptr<Expression> filter) {
		auto f = make_uniq<Filter>();
		f->filter = std::move(filter);
		f->ExtractBindings();
		filters.push_back(std::move(f));
	});
}

unique_ptr<LogicalOperator> FilterPushdown::AddLogicalFilter(unique_ptr<LogicalOperator> op,
                                                             vector<unique_ptr<Expression>> expressions) {
	if (expressions.empty()) {
		// No left expressions, so needn't to add an extra filter operator.
		return op;
	}
	auto filter = make_uniq<LogicalFilter>();
	filter->expressions = std::move(expressions);
	filter->children.push_back(std::move(op));
	return std::move(filter);
}

unique_ptr<LogicalOperator> FilterPushdown::PushFinalFilters(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<Expression>> expressions;
	for (auto &f : filters) {
		expressions.push_back(std::move(f->filter));
	}

	return AddLogicalFilter(std::move(op), std::move(expressions));
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (auto &child : op->children) {
		FilterPushdown pushdown(optimizer);
		child = pushdown.Rewrite(std::move(child));
	}
	// now push any existing filters
	return PushFinalFilters(std::move(op));
}

void FilterPushdown::Filter::ExtractBindings() {
	bindings.clear();
	LogicalJoin::GetExpressionBindings(*filter, bindings);
}

} // namespace duckdb










namespace duckdb {

unique_ptr<LogicalOperator> InClauseRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->children.size() == 1) {
		root = std::move(op->children[0]);
		VisitOperatorExpressions(*op);
		op->children[0] = std::move(root);
	}

	for (auto &child : op->children) {
		child = Rewrite(std::move(child));
	}
	return op;
}

unique_ptr<Expression> InClauseRewriter::VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) {
	if (expr.type != ExpressionType::COMPARE_IN && expr.type != ExpressionType::COMPARE_NOT_IN) {
		return nullptr;
	}
	D_ASSERT(root);
	auto in_type = expr.children[0]->return_type;
	bool is_regular_in = expr.type == ExpressionType::COMPARE_IN;
	bool all_scalar = true;
	// IN clause with many children: try to generate a mark join that replaces this IN expression
	// we can only do this if the expressions in the expression list are scalar
	for (idx_t i = 1; i < expr.children.size(); i++) {
		if (!expr.children[i]->IsFoldable()) {
			// non-scalar expression
			all_scalar = false;
		}
	}
	if (expr.children.size() == 2) {
		// only one child
		// IN: turn into X = 1
		// NOT IN: turn into X <> 1
		return make_uniq<BoundComparisonExpression>(is_regular_in ? ExpressionType::COMPARE_EQUAL
		                                                          : ExpressionType::COMPARE_NOTEQUAL,
		                                            std::move(expr.children[0]), std::move(expr.children[1]));
	}
	if (expr.children.size() < 6 || !all_scalar) {
		// low amount of children or not all scalar
		// IN: turn into (X = 1 OR X = 2 OR X = 3...)
		// NOT IN: turn into (X <> 1 AND X <> 2 AND X <> 3 ...)
		auto conjunction = make_uniq<BoundConjunctionExpression>(is_regular_in ? ExpressionType::CONJUNCTION_OR
		                                                                       : ExpressionType::CONJUNCTION_AND);
		for (idx_t i = 1; i < expr.children.size(); i++) {
			conjunction->children.push_back(make_uniq<BoundComparisonExpression>(
			    is_regular_in ? ExpressionType::COMPARE_EQUAL : ExpressionType::COMPARE_NOTEQUAL,
			    expr.children[0]->Copy(), std::move(expr.children[i])));
		}
		return std::move(conjunction);
	}
	// IN clause with many constant children
	// generate a mark join that replaces this IN expression
	// first generate a ColumnDataCollection from the set of expressions
	vector<LogicalType> types = {in_type};
	auto collection = make_uniq<ColumnDataCollection>(context, types);
	ColumnDataAppendState append_state;
	collection->InitializeAppend(append_state);

	DataChunk chunk;
	chunk.Initialize(context, types);
	for (idx_t i = 1; i < expr.children.size(); i++) {
		// resolve this expression to a constant
		auto value = ExpressionExecutor::EvaluateScalar(context, *expr.children[i]);
		idx_t index = chunk.size();
		chunk.SetCardinality(chunk.size() + 1);
		chunk.SetValue(0, index, value);
		if (chunk.size() == STANDARD_VECTOR_SIZE || i + 1 == expr.children.size()) {
			// chunk full: append to chunk collection
			collection->Append(append_state, chunk);
			chunk.Reset();
		}
	}
	// now generate a ChunkGet that scans this collection
	auto chunk_index = optimizer.binder.GenerateTableIndex();
	auto chunk_scan = make_uniq<LogicalColumnDataGet>(chunk_index, types, std::move(collection));

	// then we generate the MARK join with the chunk scan on the RHS
	auto join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
	join->mark_index = chunk_index;
	join->AddChild(std::move(root));
	join->AddChild(std::move(chunk_scan));
	// create the JOIN condition
	JoinCondition cond;
	cond.left = std::move(expr.children[0]);

	cond.right = make_uniq<BoundColumnRefExpression>(in_type, ColumnBinding(chunk_index, 0));
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	join->conditions.push_back(std::move(cond));
	root = std::move(join);

	// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
	unique_ptr<Expression> result =
	    make_uniq<BoundColumnRefExpression>("IN (...)", LogicalType::BOOLEAN, ColumnBinding(chunk_index, 0));
	if (!is_regular_in) {
		// NOT IN: invert
		auto invert = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
		invert->children.push_back(std::move(result));
		result = std::move(invert);
	}
	return result;
}

} // namespace duckdb











namespace duckdb {

// The filter was made on top of a logical sample or other projection,
// but no specific columns are referenced. See issue 4978 number 4.
bool CardinalityEstimator::EmptyFilter(FilterInfo &filter_info) {
	if (!filter_info.left_set && !filter_info.right_set) {
		return true;
	}
	return false;
}

void CardinalityEstimator::AddRelationTdom(FilterInfo &filter_info) {
	D_ASSERT(filter_info.set.count >= 1);
	for (const RelationsToTDom &r2tdom : relations_to_tdoms) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(filter_info.left_binding) != i_set.end()) {
			// found an equivalent filter
			return;
		}
	}

	auto key = ColumnBinding(filter_info.left_binding.table_index, filter_info.left_binding.column_index);
	RelationsToTDom new_r2tdom(column_binding_set_t({key}));

	relations_to_tdoms.emplace_back(new_r2tdom);
}

bool CardinalityEstimator::SingleColumnFilter(FilterInfo &filter_info) {
	if (filter_info.left_set && filter_info.right_set) {
		// Both set
		return false;
	}
	if (EmptyFilter(filter_info)) {
		return false;
	}
	return true;
}

vector<idx_t> CardinalityEstimator::DetermineMatchingEquivalentSets(FilterInfo *filter_info) {
	vector<idx_t> matching_equivalent_sets;
	auto equivalent_relation_index = 0;

	for (const RelationsToTDom &r2tdom : relations_to_tdoms) {
		auto &i_set = r2tdom.equivalent_relations;
		if (i_set.find(filter_info->left_binding) != i_set.end()) {
			matching_equivalent_sets.push_back(equivalent_relation_index);
		} else if (i_set.find(filter_info->right_binding) != i_set.end()) {
			// don't add both left and right to the matching_equivalent_sets
			// since both left and right get added to that index anyway.
			matching_equivalent_sets.push_back(equivalent_relation_index);
		}
		equivalent_relation_index++;
	}
	return matching_equivalent_sets;
}

void CardinalityEstimator::AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets) {
	D_ASSERT(matching_equivalent_sets.size() <= 2);
	if (matching_equivalent_sets.size() > 1) {
		// an equivalence relation is connecting two sets of equivalence relations
		// so push all relations from the second set into the first. Later we will delete
		// the second set.
		for (ColumnBinding i : relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations) {
			relations_to_tdoms.at(matching_equivalent_sets[0]).equivalent_relations.insert(i);
		}
		for (auto &column_name : relations_to_tdoms.at(matching_equivalent_sets[1]).column_names) {
			relations_to_tdoms.at(matching_equivalent_sets[0]).column_names.push_back(column_name);
		}
		relations_to_tdoms.at(matching_equivalent_sets[1]).equivalent_relations.clear();
		relations_to_tdoms.at(matching_equivalent_sets[1]).column_names.clear();
		relations_to_tdoms.at(matching_equivalent_sets[0]).filters.push_back(filter_info);
		// add all values of one set to the other, delete the empty one
	} else if (matching_equivalent_sets.size() == 1) {
		auto &tdom_i = relations_to_tdoms.at(matching_equivalent_sets.at(0));
		tdom_i.equivalent_relations.insert(filter_info->left_binding);
		tdom_i.equivalent_relations.insert(filter_info->right_binding);
		tdom_i.filters.push_back(filter_info);
	} else if (matching_equivalent_sets.empty()) {
		column_binding_set_t tmp;
		tmp.insert(filter_info->left_binding);
		tmp.insert(filter_info->right_binding);
		relations_to_tdoms.emplace_back(tmp);
		relations_to_tdoms.back().filters.push_back(filter_info);
	}
}

void CardinalityEstimator::InitEquivalentRelations(const vector<unique_ptr<FilterInfo>> &filter_infos) {
	// For each filter, we fill keep track of the index of the equivalent relation set
	// the left and right relation needs to be added to.
	for (auto &filter : filter_infos) {
		if (SingleColumnFilter(*filter)) {
			// Filter on one relation, (i.e string or range filter on a column).
			// Grab the first relation and add it to  the equivalence_relations
			AddRelationTdom(*filter);
			continue;
		} else if (EmptyFilter(*filter)) {
			continue;
		}
		D_ASSERT(filter->left_set->count >= 1);
		D_ASSERT(filter->right_set->count >= 1);

		auto matching_equivalent_sets = DetermineMatchingEquivalentSets(filter.get());
		AddToEquivalenceSets(filter.get(), matching_equivalent_sets);
	}
	RemoveEmptyTotalDomains();
}

void CardinalityEstimator::RemoveEmptyTotalDomains() {
	auto remove_start = std::remove_if(relations_to_tdoms.begin(), relations_to_tdoms.end(),
	                                   [](RelationsToTDom &r_2_tdom) { return r_2_tdom.equivalent_relations.empty(); });
	relations_to_tdoms.erase(remove_start, relations_to_tdoms.end());
}

void UpdateDenom(Subgraph2Denominator &relation_2_denom, RelationsToTDom &relation_to_tdom) {
	relation_2_denom.denom *= relation_to_tdom.has_tdom_hll ? relation_to_tdom.tdom_hll : relation_to_tdom.tdom_no_hll;
}

void FindSubgraphMatchAndMerge(Subgraph2Denominator &merge_to, idx_t find_me,
                               vector<Subgraph2Denominator>::iterator subgraph,
                               vector<Subgraph2Denominator>::iterator end) {
	for (; subgraph != end; subgraph++) {
		if (subgraph->relations.count(find_me) >= 1) {
			for (auto &relation : subgraph->relations) {
				merge_to.relations.insert(relation);
			}
			subgraph->relations.clear();
			merge_to.denom *= subgraph->denom;
			return;
		}
	}
}

template <>
double CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {

	if (relation_set_2_cardinality.find(new_set.ToString()) != relation_set_2_cardinality.end()) {
		return relation_set_2_cardinality[new_set.ToString()].cardinality_before_filters;
	}
	double numerator = 1;
	unordered_set<idx_t> actual_set;

	for (idx_t i = 0; i < new_set.count; i++) {
		auto &single_node_set = set_manager.GetJoinRelation(new_set.relations[i]);
		auto card_helper = relation_set_2_cardinality[single_node_set.ToString()];
		numerator *= card_helper.cardinality_before_filters;
		actual_set.insert(new_set.relations[i]);
	}

	vector<Subgraph2Denominator> subgraphs;
	bool done = false;
	bool found_match = false;

	// Finding the denominator is tricky. You need to go through the tdoms in decreasing order
	// Then loop through all filters in the equivalence set of the tdom to see if both the
	// left and right relations are in the new set, if so you can use that filter.
	// You must also make sure that the filters all relations in the given set, so we use subgraphs
	// that should eventually merge into one connected graph that joins all the relations
	// TODO: Implement a method to cache subgraphs so you don't have to build them up every
	// time the cardinality of a new set is requested

	// relations_to_tdoms has already been sorted.
	for (auto &relation_2_tdom : relations_to_tdoms) {
		// loop through each filter in the tdom.
		if (done) {
			break;
		}
		for (auto &filter : relation_2_tdom.filters) {
			if (actual_set.count(filter->left_binding.table_index) == 0 ||
			    actual_set.count(filter->right_binding.table_index) == 0) {
				continue;
			}
			// the join filter is on relations in the new set.
			found_match = false;
			vector<Subgraph2Denominator>::iterator it;
			for (it = subgraphs.begin(); it != subgraphs.end(); it++) {
				auto left_in = it->relations.count(filter->left_binding.table_index);
				auto right_in = it->relations.count(filter->right_binding.table_index);
				if (left_in && right_in) {
					// if both left and right bindings are in the subgraph, continue.
					// This means another filter is connecting relations already in the
					// subgraph it, but it has a tdom that is less, and we don't care.
					found_match = true;
					continue;
				}
				if (!left_in && !right_in) {
					// if both left and right bindings are *not* in the subgraph, continue
					// without finding a match. This will trigger the process to add a new
					// subgraph
					continue;
				}
				idx_t find_table;
				if (left_in) {
					find_table = filter->right_binding.table_index;
				} else {
					D_ASSERT(right_in);
					find_table = filter->left_binding.table_index;
				}
				auto next_subgraph = it + 1;
				// iterate through other subgraphs and merge.
				FindSubgraphMatchAndMerge(*it, find_table, next_subgraph, subgraphs.end());
				// Now insert the right binding and update denominator with the
				// tdom of the filter
				it->relations.insert(find_table);
				UpdateDenom(*it, relation_2_tdom);
				found_match = true;
				break;
			}
			// means that the filter joins relations in the given set, but there is no
			// connection to any subgraph in subgraphs. Add a new subgraph, and maybe later there will be
			// a connection.
			if (!found_match) {
				subgraphs.emplace_back();
				auto &subgraph = subgraphs.back();
				subgraph.relations.insert(filter->left_binding.table_index);
				subgraph.relations.insert(filter->right_binding.table_index);
				UpdateDenom(subgraph, relation_2_tdom);
			}
			auto remove_start = std::remove_if(subgraphs.begin(), subgraphs.end(),
			                                   [](Subgraph2Denominator &s) { return s.relations.empty(); });
			subgraphs.erase(remove_start, subgraphs.end());

			if (subgraphs.size() == 1 && subgraphs.at(0).relations.size() == new_set.count) {
				// You have found enough filters to connect the relations. These are guaranteed
				// to be the filters with the highest Tdoms.
				done = true;
				break;
			}
		}
	}
	double denom = 1;
	// TODO: It's possible cross-products were added and are not present in the filters in the relation_2_tdom
	//       structures. When that's the case, multiply the denom structures that have no intersection
	for (auto &match : subgraphs) {
		denom *= match.denom;
	}
	// can happen if a table has cardinality 0, or a tdom is set to 0
	if (denom == 0) {
		denom = 1;
	}
	auto result = numerator / denom;
	auto new_entry = CardinalityHelper((double)result, 1);
	relation_set_2_cardinality[new_set.ToString()] = new_entry;
	return result;
}

template <>
idx_t CardinalityEstimator::EstimateCardinalityWithSet(JoinRelationSet &new_set) {
	auto cardinality_as_double = EstimateCardinalityWithSet<double>(new_set);
	auto max = NumericLimits<idx_t>::Maximum();
	if (cardinality_as_double > max) {
		return max;
	}
	return (idx_t)cardinality_as_double;
}

bool SortTdoms(const RelationsToTDom &a, const RelationsToTDom &b) {
	if (a.has_tdom_hll && b.has_tdom_hll) {
		return a.tdom_hll > b.tdom_hll;
	}
	if (a.has_tdom_hll) {
		return a.tdom_hll > b.tdom_no_hll;
	}
	if (b.has_tdom_hll) {
		return a.tdom_no_hll > b.tdom_hll;
	}
	return a.tdom_no_hll > b.tdom_no_hll;
}

void CardinalityEstimator::InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	// Get the join relation set
	D_ASSERT(stats.stats_initialized);
	auto relation_cardinality = stats.cardinality;
	auto relation_filter = stats.filter_strength;

	auto card_helper = CardinalityHelper(relation_cardinality, relation_filter);
	relation_set_2_cardinality[set->ToString()] = card_helper;

	UpdateTotalDomains(set, stats);

	// sort relations from greatest tdom to lowest tdom.
	std::sort(relations_to_tdoms.begin(), relations_to_tdoms.end(), SortTdoms);
}

void CardinalityEstimator::UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats) {
	D_ASSERT(set->count == 1);
	auto relation_id = set->relations[0];
	//! Initialize the distinct count for all columns used in joins with the current relation.
	//	D_ASSERT(stats.column_distinct_count.size() >= 1);

	for (idx_t i = 0; i < stats.column_distinct_count.size(); i++) {
		//! for every column used in a filter in the relation, get the distinct count via HLL, or assume it to be
		//! the cardinality
		// Update the relation_to_tdom set with the estimated distinct count (or tdom) calculated above
		auto key = ColumnBinding(relation_id, i);
		for (auto &relation_to_tdom : relations_to_tdoms) {
			column_binding_set_t i_set = relation_to_tdom.equivalent_relations;
			if (i_set.find(key) == i_set.end()) {
				continue;
			}
			auto distinct_count = stats.column_distinct_count.at(i);
			if (distinct_count.from_hll && relation_to_tdom.has_tdom_hll) {
				relation_to_tdom.tdom_hll = MaxValue(relation_to_tdom.tdom_hll, distinct_count.distinct_count);
			} else if (distinct_count.from_hll && !relation_to_tdom.has_tdom_hll) {
				relation_to_tdom.has_tdom_hll = true;
				relation_to_tdom.tdom_hll = distinct_count.distinct_count;
			} else {
				relation_to_tdom.tdom_no_hll = MinValue(distinct_count.distinct_count, relation_to_tdom.tdom_no_hll);
			}
			break;
		}
	}
}

// LCOV_EXCL_START

void CardinalityEstimator::AddRelationNamesToTdoms(vector<RelationStats> &stats) {
#ifdef DEBUG
	for (auto &total_domain : relations_to_tdoms) {
		for (auto &binding : total_domain.equivalent_relations) {
			D_ASSERT(binding.table_index < stats.size());
			D_ASSERT(binding.column_index < stats.at(binding.table_index).column_names.size());
			string column_name = stats.at(binding.table_index).column_names.at(binding.column_index);
			total_domain.column_names.push_back(column_name);
		}
	}
#endif
}

void CardinalityEstimator::PrintRelationToTdomInfo() {
	for (auto &total_domain : relations_to_tdoms) {
		string domain = "Following columns have the same distinct count: ";
		for (auto &column_name : total_domain.column_names) {
			domain += column_name + ", ";
		}
		bool have_hll = total_domain.has_tdom_hll;
		domain += "\n TOTAL DOMAIN = " + to_string(have_hll ? total_domain.tdom_hll : total_domain.tdom_no_hll);
		Printer::Print(domain);
	}
}

// LCOV_EXCL_STOP

} // namespace duckdb



#include <cmath>

namespace duckdb {

CostModel::CostModel(QueryGraphManager &query_graph_manager)
    : query_graph_manager(query_graph_manager), cardinality_estimator() {
}

double CostModel::ComputeCost(JoinNode &left, JoinNode &right) {
	auto &combination = query_graph_manager.set_manager.Union(left.set, right.set);
	auto join_card = cardinality_estimator.EstimateCardinalityWithSet<double>(combination);
	auto join_cost = join_card;
	return join_cost + left.cost + right.cost;
}

} // namespace duckdb



namespace duckdb {

template <>
double EstimatedProperties::GetCardinality() const {
	return cardinality;
}

template <>
idx_t EstimatedProperties::GetCardinality() const {
	auto max_idx_t = NumericLimits<idx_t>::Maximum() - 10000;
	return MinValue<double>(cardinality, max_idx_t);
}

template <>
double EstimatedProperties::GetCost() const {
	return cost;
}

template <>
idx_t EstimatedProperties::GetCost() const {
	auto max_idx_t = NumericLimits<idx_t>::Maximum() - 10000;
	return MinValue<double>(cost, max_idx_t);
}

void EstimatedProperties::SetCardinality(double new_card) {
	cardinality = new_card;
}

void EstimatedProperties::SetCost(double new_cost) {
	cost = new_cost;
}

} // namespace duckdb






namespace duckdb {

JoinNode::JoinNode(JoinRelationSet &set) : set(set) {
}

JoinNode::JoinNode(JoinRelationSet &set, optional_ptr<NeighborInfo> info, JoinNode &left, JoinNode &right, double cost)
    : set(set), info(info), left(&left), right(&right), cost(cost) {
}

unique_ptr<EstimatedProperties> EstimatedProperties::Copy() {
	auto result = make_uniq<EstimatedProperties>(cardinality, cost);
	return result;
}

string JoinNode::ToString() {
	string result = "-------------------------------\n";
	result += set.ToString() + "\n";
	result += "cost = " + to_string(cost) + "\n";
	result += "left = \n";
	if (left) {
		result += left->ToString();
	}
	result += "right = \n";
	if (right) {
		result += right->ToString();
	}
	return result;
}

} // namespace duckdb










#include <algorithm>
#include <cmath>

namespace duckdb {

static bool HasJoin(LogicalOperator *op) {
	while (!op->children.empty()) {
		if (op->children.size() == 1) {
			op = op->children[0].get();
		}
		if (op->children.size() == 2) {
			return true;
		}
	}
	return false;
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan,
                                                         optional_ptr<RelationStats> stats) {

	// make sure query graph manager has not extracted a relation graph already
	LogicalOperator *op = plan.get();

	// extract the relations that go into the hyper graph.
	// We optimize the children of any non-reorderable operations we come across.
	bool reorderable = query_graph_manager.Build(*op);

	// get relation_stats here since the reconstruction process will move all of the relations.
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();
	unique_ptr<LogicalOperator> new_logical_plan = nullptr;

	if (reorderable) {
		// query graph now has filters and relations
		auto cost_model = CostModel(query_graph_manager);

		// Initialize a plan enumerator.
		auto plan_enumerator =
		    PlanEnumerator(query_graph_manager, cost_model, query_graph_manager.GetQueryGraphEdges());

		// Initialize the leaf/single node plans
		plan_enumerator.InitLeafPlans();

		// Ask the plan enumerator to enumerate a number of join orders
		auto final_plan = plan_enumerator.SolveJoinOrder();
		// TODO: add in the check that if no plan exists, you have to add a cross product.

		// now reconstruct a logical plan from the query graph plan
		new_logical_plan = query_graph_manager.Reconstruct(std::move(plan), *final_plan);
	} else {
		new_logical_plan = std::move(plan);
		if (relation_stats.size() == 1) {
			new_logical_plan->estimated_cardinality = relation_stats.at(0).cardinality;
		}
	}

	// only perform left right optimizations when stats is null (means we have the top level optimize call)
	// Don't check reorderability because non-reorderable joins will result in 1 relation, but we can
	// still switch the children.
	// TODO: put this in a different optimizer maybe?
	if (stats == nullptr && HasJoin(new_logical_plan.get())) {
		new_logical_plan = query_graph_manager.LeftRightOptimizations(std::move(new_logical_plan));
	}

	// Propagate up a stats object from the top of the new_logical_plan if stats exist.
	if (stats) {
		auto cardinality = new_logical_plan->EstimateCardinality(context);
		auto bindings = new_logical_plan->GetColumnBindings();
		auto new_stats = RelationStatisticsHelper::CombineStatsOfReorderableOperator(bindings, relation_stats);
		new_stats.cardinality = MaxValue(cardinality, new_stats.cardinality);
		RelationStatisticsHelper::CopyRelationStats(*stats, new_stats);
	}

	return new_logical_plan;
}

} // namespace duckdb




#include <algorithm>

namespace duckdb {

using JoinRelationTreeNode = JoinRelationSetManager::JoinRelationTreeNode;

// LCOV_EXCL_START
string JoinRelationSet::ToString() const {
	string result = "[";
	result += StringUtil::Join(relations, count, ", ", [](const idx_t &relation) { return to_string(relation); });
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

//! Returns true if sub is a subset of super
bool JoinRelationSet::IsSubset(JoinRelationSet &super, JoinRelationSet &sub) {
	D_ASSERT(sub.count > 0);
	if (sub.count > super.count) {
		return false;
	}
	idx_t j = 0;
	for (idx_t i = 0; i < super.count; i++) {
		if (sub.relations[j] == super.relations[i]) {
			j++;
			if (j == sub.count) {
				return true;
			}
		}
	}
	return false;
}

JoinRelationSet &JoinRelationSetManager::GetJoinRelation(unsafe_unique_array<idx_t> relations, idx_t count) {
	// now look it up in the tree
	reference<JoinRelationTreeNode> info(root);
	for (idx_t i = 0; i < count; i++) {
		auto entry = info.get().children.find(relations[i]);
		if (entry == info.get().children.end()) {
			// node not found, create it
			auto insert_it = info.get().children.insert(make_pair(relations[i], make_uniq<JoinRelationTreeNode>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = *entry->second;
	}
	// now check if the JoinRelationSet has already been created
	if (!info.get().relation) {
		// if it hasn't we need to create it
		info.get().relation = make_uniq<JoinRelationSet>(std::move(relations), count);
	}
	return *info.get().relation;
}

//! Create or get a JoinRelationSet from a single node with the given index
JoinRelationSet &JoinRelationSetManager::GetJoinRelation(idx_t index) {
	// create a sorted vector of the relations
	auto relations = make_unsafe_uniq_array<idx_t>(1);
	relations[0] = index;
	idx_t count = 1;
	return GetJoinRelation(std::move(relations), count);
}

JoinRelationSet &JoinRelationSetManager::GetJoinRelation(const unordered_set<idx_t> &bindings) {
	// create a sorted vector of the relations
	unsafe_unique_array<idx_t> relations = bindings.empty() ? nullptr : make_unsafe_uniq_array<idx_t>(bindings.size());
	idx_t count = 0;
	for (auto &entry : bindings) {
		relations[count++] = entry;
	}
	std::sort(relations.get(), relations.get() + count);
	return GetJoinRelation(std::move(relations), count);
}

JoinRelationSet &JoinRelationSetManager::Union(JoinRelationSet &left, JoinRelationSet &right) {
	auto relations = make_unsafe_uniq_array<idx_t>(left.count + right.count);
	idx_t count = 0;
	// move through the left and right relations, eliminating duplicates
	idx_t i = 0, j = 0;
	while (true) {
		if (i == left.count) {
			// exhausted left relation, add remaining of right relation
			for (; j < right.count; j++) {
				relations[count++] = right.relations[j];
			}
			break;
		} else if (j == right.count) {
			// exhausted right relation, add remaining of left
			for (; i < left.count; i++) {
				relations[count++] = left.relations[i];
			}
			break;
		} else if (left.relations[i] < right.relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left.relations[i];
			i++;
		} else {
			D_ASSERT(left.relations[i] > right.relations[j]);
			// right is smaller, progress right and add it to the set
			relations[count++] = right.relations[j];
			j++;
		}
	}
	return GetJoinRelation(std::move(relations), count);
}

// JoinRelationSet *JoinRelationSetManager::Difference(JoinRelationSet *left, JoinRelationSet *right) {
// 	auto relations = unsafe_unique_array<idx_t>(new idx_t[left->count]);
// 	idx_t count = 0;
// 	// move through the left and right relations
// 	idx_t i = 0, j = 0;
// 	while (true) {
// 		if (i == left->count) {
// 			// exhausted left relation, we are done
// 			break;
// 		} else if (j == right->count) {
// 			// exhausted right relation, add remaining of left
// 			for (; i < left->count; i++) {
// 				relations[count++] = left->relations[i];
// 			}
// 			break;
// 		} else if (left->relations[i] == right->relations[j]) {
// 			// equivalent, add nothing
// 			i++;
// 			j++;
// 		} else if (left->relations[i] < right->relations[j]) {
// 			// left is smaller, progress left and add it to the set
// 			relations[count++] = left->relations[i];
// 			i++;
// 		} else {
// 			// right is smaller, progress right
// 			j++;
// 		}
// 	}
// 	return GetJoinRelation(std::move(relations), count);
// }

} // namespace duckdb





namespace duckdb {

bool PlanEnumerator::NodeInFullPlan(JoinNode &node) {
	return join_nodes_in_full_plan.find(node.set.ToString()) != join_nodes_in_full_plan.end();
}

void PlanEnumerator::UpdateJoinNodesInFullPlan(JoinNode &node) {
	if (node.set.count == query_graph_manager.relation_manager.NumRelations()) {
		join_nodes_in_full_plan.clear();
	}
	if (node.set.count < query_graph_manager.relation_manager.NumRelations()) {
		join_nodes_in_full_plan.insert(node.set.ToString());
	}
	if (node.left) {
		UpdateJoinNodesInFullPlan(*node.left);
	}
	if (node.right) {
		UpdateJoinNodesInFullPlan(*node.right);
	}
}

static vector<unordered_set<idx_t>> AddSuperSets(const vector<unordered_set<idx_t>> &current,
                                                 const vector<idx_t> &all_neighbors) {
	vector<unordered_set<idx_t>> ret;

	for (const auto &neighbor_set : current) {
		auto max_val = std::max_element(neighbor_set.begin(), neighbor_set.end());
		for (const auto &neighbor : all_neighbors) {
			if (*max_val >= neighbor) {
				continue;
			}
			if (neighbor_set.count(neighbor) == 0) {
				unordered_set<idx_t> new_set;
				for (auto &n : neighbor_set) {
					new_set.insert(n);
				}
				new_set.insert(neighbor);
				ret.push_back(new_set);
			}
		}
	}

	return ret;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(optional_ptr<JoinRelationSet> node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

// works by first creating all sets with cardinality 1
// then iterates over each previously created group of subsets and will only add a neighbor if the neighbor
// is greater than all relations in the set.
static vector<unordered_set<idx_t>> GetAllNeighborSets(vector<idx_t> neighbors) {
	vector<unordered_set<idx_t>> ret;
	sort(neighbors.begin(), neighbors.end());
	vector<unordered_set<idx_t>> added;
	for (auto &neighbor : neighbors) {
		added.push_back(unordered_set<idx_t>({neighbor}));
		ret.push_back(unordered_set<idx_t>({neighbor}));
	}
	do {
		added = AddSuperSets(added, neighbors);
		for (auto &d : added) {
			ret.push_back(d);
		}
	} while (!added.empty());
#if DEBUG
	// drive by test to make sure we have an accurate amount of
	// subsets, and that each neighbor is in a correct amount
	// of those subsets.
	D_ASSERT(ret.size() == pow(2, neighbors.size()) - 1);
	for (auto &n : neighbors) {
		idx_t count = 0;
		for (auto &set : ret) {
			if (set.count(n) >= 1) {
				count += 1;
			}
		}
		D_ASSERT(count == pow(2, neighbors.size() - 1));
	}
#endif
	return ret;
}

void PlanEnumerator::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		auto &left = query_graph_manager.set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < query_graph_manager.relation_manager.NumRelations(); j++) {
			if (i != j) {
				auto &right = query_graph_manager.set_manager.GetJoinRelation(j);
				query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			}
		}
	}
	// Now that the query graph has new edges, we need to re-initialize our query graph.
	// TODO: do we need to initialize our qyery graph again?
	// query_graph = query_graph_manager.GetQueryGraph();
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> PlanEnumerator::CreateJoinTree(JoinRelationSet &set,
                                                    const vector<reference<NeighborInfo>> &possible_connections,
                                                    JoinNode &left, JoinNode &right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	optional_ptr<NeighborInfo> best_connection = nullptr;

	// cross products are techincally still connections, but the filter expression is a null_ptr
	if (!possible_connections.empty()) {
		best_connection = &possible_connections.back().get();
	}

	auto cost = cost_model.ComputeCost(left, right);
	auto result = make_uniq<JoinNode>(set, best_connection, left, right, cost);
	result->cardinality = cost_model.cardinality_estimator.EstimateCardinalityWithSet<idx_t>(set);
	return result;
}

JoinNode &PlanEnumerator::EmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                   const vector<reference<NeighborInfo>> &info) {
	// get the left and right join plans
	auto left_plan = plans.find(left);
	auto right_plan = plans.find(right);
	if (left_plan == plans.end() || right_plan == plans.end()) {
		throw InternalException("No left or right plan: internal error in join order optimizer");
	}
	auto &new_set = query_graph_manager.set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, *left_plan->second, *right_plan->second);
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
	auto new_cost = new_plan->cost;
	double old_cost = NumericLimits<double>::Maximum();
	if (entry != plans.end()) {
		old_cost = entry->second->cost;
	}
	if (entry == plans.end() || new_cost < old_cost) {
		// the new plan costs less than the old plan. Update our DP tree and cost tree
		auto &result = *new_plan;

		if (full_plan_found &&
		    join_nodes_in_full_plan.find(new_plan->set.ToString()) != join_nodes_in_full_plan.end()) {
			must_update_full_plan = true;
		}
		if (new_set.count == query_graph_manager.relation_manager.NumRelations()) {
			full_plan_found = true;
			// If we find a full plan, we need to keep track of which nodes are in the full plan.
			// It's possible the DP algorithm updates a node in the current full plan, then moves on
			// to the SolveApproximately. SolveApproximately may find a full plan with a higher cost than
			// what SolveExactly found. In this case, we revert to the SolveExactly plan, but it is
			// possible to get use-after-free errors if the SolveApproximately algorithm updated some (but not all)
			// nodes in the SolveExactly plan
			// If we know a node in the full plan is updated, we can prevent ourselves from exiting the
			// DP algorithm until the last plan updated is a full plan
			UpdateJoinNodesInFullPlan(result);
			if (must_update_full_plan) {
				must_update_full_plan = false;
			}
		}

		D_ASSERT(new_plan);
		plans[new_set] = std::move(new_plan);
		return result;
	}
	return *entry->second;
}

bool PlanEnumerator::TryEmitPair(JoinRelationSet &left, JoinRelationSet &right,
                                 const vector<reference<NeighborInfo>> &info) {
	pairs++;
	// If a full plan is created, it's possible a node in the plan gets updated. When this happens, make sure you keep
	// emitting pairs until you emit another final plan. Another final plan is guaranteed to be produced because of
	// our symmetry guarantees.
	if (pairs >= 10000 && !must_update_full_plan) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool PlanEnumerator::EmitCSG(JoinRelationSet &node) {
	if (node.count == query_graph_manager.relation_manager.NumRelations()) {
		return true;
	}
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node.relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(&node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	//! Neighbors should be reversed when iterating over them.
	std::sort(neighbors.begin(), neighbors.end(), std::greater_equal<idx_t>());
	for (idx_t i = 0; i < neighbors.size() - 1; i++) {
		D_ASSERT(neighbors[i] > neighbors[i + 1]);
	}

	// Dphyp paper missiing this.
	// Because we are traversing in reverse order, we need to add neighbors whose number is smaller than the current
	// node to exclusion_set
	// This avoids duplicated enumeration
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); ++i) {
		D_ASSERT(new_exclusion_set.find(neighbors[i]) == new_exclusion_set.end());
		new_exclusion_set.insert(neighbors[i]);
	}

	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto &neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto connections = query_graph.GetConnections(node, neighbor_relation);
		if (!connections.empty()) {
			if (!TryEmitPair(node, neighbor_relation, connections)) {
				return false;
			}
		}

		if (!EnumerateCmpRecursive(node, neighbor_relation, new_exclusion_set)) {
			return false;
		}

		new_exclusion_set.erase(neighbor);
	}
	return true;
}

bool PlanEnumerator::EnumerateCmpRecursive(JoinRelationSet &left, JoinRelationSet &right,
                                           unordered_set<idx_t> &exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto &combined_set = query_graph_manager.set_manager.Union(right, neighbor);
		// If combined_set.count == right.count, This means we found a neighbor that has been present before
		// This means we didn't set exclusion_set correctly.
		D_ASSERT(combined_set.count > right.count);
		if (plans.find(combined_set) != plans.end()) {
			auto connections = query_graph.GetConnections(left, combined_set);
			if (!connections.empty()) {
				if (!TryEmitPair(left, combined_set, connections)) {
					return false;
				}
			}
		}
		union_sets.push_back(combined_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool PlanEnumerator::EnumerateCSGRecursive(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.empty()) {
		return true;
	}

	auto all_subset = GetAllNeighborSets(neighbors);
	vector<reference<JoinRelationSet>> union_sets;
	union_sets.reserve(all_subset.size());
	for (const auto &rel_set : all_subset) {
		auto &neighbor = query_graph_manager.set_manager.GetJoinRelation(rel_set);
		// emit the combinations of this node and its neighbors
		auto &new_set = query_graph_manager.set_manager.Union(node, neighbor);
		D_ASSERT(new_set.count > node.count);
		if (plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets.push_back(new_set);
	}

	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (const auto &neighbor : neighbors) {
		new_exclusion_set.insert(neighbor);
	}

	// recursively enumerate the sets
	for (idx_t i = 0; i < union_sets.size(); i++) {
		// updated the set of excluded entries with this neighbor
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool PlanEnumerator::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = query_graph_manager.relation_manager.NumRelations(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto &start_node = query_graph_manager.set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

void PlanEnumerator::UpdateDPTree(JoinNode &new_plan) {
	if (!NodeInFullPlan(new_plan)) {
		// if the new node is not in the full plan, feel free to return
		// because you won't be updating the full plan.
		return;
	}
	auto &new_set = new_plan.set;
	// now update every plan that uses this plan
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < new_set.count; i++) {
		exclusion_set.insert(new_set.relations[i]);
	}
	auto neighbors = query_graph.GetNeighbors(new_set, exclusion_set);
	auto all_neighbors = GetAllNeighborSets(neighbors);
	for (const auto &neighbor : all_neighbors) {
		auto &neighbor_relation = query_graph_manager.set_manager.GetJoinRelation(neighbor);
		auto &combined_set = query_graph_manager.set_manager.Union(new_set, neighbor_relation);

		auto combined_set_plan = plans.find(combined_set);
		if (combined_set_plan == plans.end()) {
			continue;
		}

		double combined_set_plan_cost = combined_set_plan->second->cost; // combined_set_plan->second->GetCost();
		auto connections = query_graph.GetConnections(new_set, neighbor_relation);
		// recurse and update up the tree if the combined set produces a plan with a lower cost
		// only recurse on neighbor relations that have plans.
		auto right_plan = plans.find(neighbor_relation);
		if (right_plan == plans.end()) {
			continue;
		}
		auto &updated_plan = EmitPair(new_set, neighbor_relation, connections);
		// <= because the child node has already been replaced. You need to
		// replace the parent node as well in this case
		if (updated_plan.cost < combined_set_plan_cost) {
			UpdateDPTree(updated_plan);
		}
	}
}

void PlanEnumerator::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<reference<JoinRelationSet>> join_relations; // T in the paper
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		join_relations.push_back(query_graph_manager.set_manager.GetJoinRelation(i));
	}
	while (join_relations.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has the
		// smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations to-be-joined
		// by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		optional_ptr<JoinNode> best_connection;
		for (idx_t i = 0; i < join_relations.size(); i++) {
			auto left = join_relations[i];
			for (idx_t j = i + 1; j < join_relations.size(); j++) {
				auto right = join_relations[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnections(left, right);
				if (!connection.empty()) {
					// we can check the cost of this connection
					auto &node = EmitPair(left, right, connection);

					// update the DP tree in case a plan created by the DP algorithm uses the node
					// that was potentially just updated by EmitPair. You will get a use-after-free
					// error if future plans rely on the old node that was just replaced.
					// if node in FullPath, then updateDP tree.
					UpdateDPTree(node);

					if (!best_connection || node.cost < best_connection->cost) {
						// best pair found so far
						best_connection = &node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			optional_ptr<JoinNode> smallest_plans[2];
			idx_t smallest_index[2];
			D_ASSERT(join_relations.size() >= 2);

			// first just add the first two join relations. It doesn't matter the cost as the JOO
			// will swap them on estimated cardinality anyway.
			for (idx_t i = 0; i < 2; i++) {
				auto current_plan = plans[join_relations[i]].get();
				smallest_plans[i] = current_plan;
				smallest_index[i] = i;
			}

			// if there are any other join relations that don't have connections
			// add them if they have lower estimated cardinality.
			for (idx_t i = 2; i < join_relations.size(); i++) {
				// get the plan for this relation
				auto current_plan = plans[join_relations[i].get()].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] || smallest_plans[j]->cost > current_plan->cost) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			if (!smallest_plans[0] || !smallest_plans[1]) {
				throw InternalException("Internal error in join order optimizer");
			}
			D_ASSERT(smallest_plans[0] && smallest_plans[1]);
			D_ASSERT(smallest_index[0] != smallest_index[1]);
			auto &left = smallest_plans[0]->set;
			auto &right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph_manager.CreateQueryGraphCrossProduct(left, right);
			// now emit the pair and continue with the algorithm
			auto connections = query_graph.GetConnections(left, right);
			D_ASSERT(!connections.empty());

			best_connection = &EmitPair(left, right, connections);
			best_left = smallest_index[0];
			best_right = smallest_index[1];

			UpdateDPTree(*best_connection);
			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				std::swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		D_ASSERT(best_right > best_left);
		join_relations.erase(join_relations.begin() + best_right);
		join_relations.erase(join_relations.begin() + best_left);
		join_relations.push_back(best_connection->set);
	}
}

void PlanEnumerator::InitLeafPlans() {
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the leaf
	// nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
	// function ensures that a unique combination of relations will have a unique JoinRelationSet object.
	// first initialize equivalent relations based on the filters
	auto relation_stats = query_graph_manager.relation_manager.GetRelationStats();

	cost_model.cardinality_estimator.InitEquivalentRelations(query_graph_manager.GetFilterBindings());
	cost_model.cardinality_estimator.AddRelationNamesToTdoms(relation_stats);

	// then update the total domains based on the cardinalities of each relation.
	for (idx_t i = 0; i < relation_stats.size(); i++) {
		auto stats = relation_stats.at(i);
		auto &relation_set = query_graph_manager.set_manager.GetJoinRelation(i);
		auto join_node = make_uniq<JoinNode>(relation_set);
		join_node->cost = 0;
		join_node->cardinality = stats.cardinality;
		plans[relation_set] = std::move(join_node);
		cost_model.cardinality_estimator.InitCardinalityEstimatorProps(&relation_set, stats);
	}
}

// the plan enumeration is a straight implementation of the paper "Dynamic Programming Strikes Back" by Guido
// Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
unique_ptr<JoinNode> PlanEnumerator::SolveJoinOrder() {
	bool force_no_cross_product = query_graph_manager.context.config.force_no_cross_product;
	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}

	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < query_graph_manager.relation_manager.NumRelations(); i++) {
		bindings.insert(i);
	}
	auto &total_relation = query_graph_manager.set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		if (force_no_cross_product) {
			throw InvalidInputException(
			    "Query requires a cross-product, but 'force_no_cross_product' PRAGMA is enabled");
		}
		GenerateCrossProducts();
		//! solve the join order again, returning the final plan
		return SolveJoinOrder();
	}
	return std::move(final_plan->second);
}

} // namespace duckdb






namespace duckdb {

using QueryEdge = QueryGraphEdges::QueryEdge;

// LCOV_EXCL_START
static string QueryEdgeToString(const QueryEdge *info, vector<idx_t> prefix) {
	string result = "";
	string source = "[";
	for (idx_t i = 0; i < prefix.size(); i++) {
		source += to_string(prefix[i]) + (i < prefix.size() - 1 ? ", " : "");
	}
	source += "]";
	for (auto &entry : info->neighbors) {
		result += StringUtil::Format("%s -> %s\n", source.c_str(), entry->neighbor->ToString().c_str());
	}
	for (auto &entry : info->children) {
		vector<idx_t> new_prefix = prefix;
		new_prefix.push_back(entry.first);
		result += QueryEdgeToString(entry.second.get(), new_prefix);
	}
	return result;
}

string QueryGraphEdges::ToString() const {
	return QueryEdgeToString(&root, {});
}

void QueryGraphEdges::Print() {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

optional_ptr<QueryEdge> QueryGraphEdges::GetQueryEdge(JoinRelationSet &left) {
	D_ASSERT(left.count > 0);
	// find the EdgeInfo corresponding to the left set
	optional_ptr<QueryEdge> info(&root);
	for (idx_t i = 0; i < left.count; i++) {
		auto entry = info.get()->children.find(left.relations[i]);
		if (entry == info.get()->children.end()) {
			// node not found, create it
			auto insert_it = info.get()->children.insert(make_pair(left.relations[i], make_uniq<QueryEdge>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = entry->second;
	}
	return info;
}

void QueryGraphEdges::CreateEdge(JoinRelationSet &left, JoinRelationSet &right, optional_ptr<FilterInfo> filter_info) {
	D_ASSERT(left.count > 0 && right.count > 0);
	// find the EdgeInfo corresponding to the left set
	auto info = GetQueryEdge(left);
	// now insert the edge to the right relation, if it does not exist
	for (idx_t i = 0; i < info->neighbors.size(); i++) {
		if (info->neighbors[i]->neighbor == &right) {
			if (filter_info) {
				// neighbor already exists just add the filter, if we have any
				info->neighbors[i]->filters.push_back(filter_info);
			}
			return;
		}
	}
	// neighbor does not exist, create it
	auto n = make_uniq<NeighborInfo>(&right);
	// if the edge represents a cross product, filter_info is null. The easiest way then to determine
	// if an edge is for a cross product is if the filters are empty
	if (info && filter_info) {
		n->filters.push_back(filter_info);
	}
	info->neighbors.push_back(std::move(n));
}

void QueryGraphEdges::EnumerateNeighborsDFS(JoinRelationSet &node, reference<QueryEdge> info, idx_t index,
                                            const std::function<bool(NeighborInfo &)> &callback) const {

	for (auto &neighbor : info.get().neighbors) {
		if (callback(*neighbor)) {
			return;
		}
	}

	for (idx_t node_index = index; node_index < node.count; ++node_index) {
		auto iter = info.get().children.find(node.relations[node_index]);
		if (iter != info.get().children.end()) {
			reference<QueryEdge> new_info = *iter->second;
			EnumerateNeighborsDFS(node, new_info, node_index + 1, callback);
		}
	}
}

void QueryGraphEdges::EnumerateNeighbors(JoinRelationSet &node,
                                         const std::function<bool(NeighborInfo &)> &callback) const {
	for (idx_t j = 0; j < node.count; j++) {
		auto iter = root.children.find(node.relations[j]);
		if (iter != root.children.end()) {
			reference<QueryEdge> new_info = *iter->second;
			EnumerateNeighborsDFS(node, new_info, j + 1, callback);
		}
	}
}

//! Returns true if a JoinRelationSet is banned by the list of exclusion_set, false otherwise
static bool JoinRelationSetIsExcluded(optional_ptr<JoinRelationSet> node, unordered_set<idx_t> &exclusion_set) {
	return exclusion_set.find(node->relations[0]) != exclusion_set.end();
}

const vector<idx_t> QueryGraphEdges::GetNeighbors(JoinRelationSet &node, unordered_set<idx_t> &exclusion_set) const {
	unordered_set<idx_t> result;
	EnumerateNeighbors(node, [&](NeighborInfo &info) -> bool {
		if (!JoinRelationSetIsExcluded(info.neighbor, exclusion_set)) {
			// add the smallest node of the neighbor to the set
			result.insert(info.neighbor->relations[0]);
		}
		return false;
	});
	vector<idx_t> neighbors;
	neighbors.insert(neighbors.end(), result.begin(), result.end());
	return neighbors;
}

const vector<reference<NeighborInfo>> QueryGraphEdges::GetConnections(JoinRelationSet &node,
                                                                      JoinRelationSet &other) const {
	vector<reference<NeighborInfo>> connections;
	EnumerateNeighbors(node, [&](NeighborInfo &info) -> bool {
		if (JoinRelationSet::IsSubset(other, *info.neighbor)) {
			connections.push_back(info);
		}
		return false;
	});
	return connections;
}

} // namespace duckdb











namespace duckdb {

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool Disjoint(const unordered_set<T> &a, const unordered_set<T> &b) {
	return std::all_of(a.begin(), a.end(), [&b](typename std::unordered_set<T>::const_reference entry) {
		return b.find(entry) == b.end();
	});
}

bool QueryGraphManager::Build(LogicalOperator &op) {
	vector<reference<LogicalOperator>> filter_operators;
	// have the relation manager extract the join relations and create a reference list of all the
	// filter operators.
	auto can_reorder = relation_manager.ExtractJoinRelations(op, filter_operators);
	auto num_relations = relation_manager.NumRelations();
	if (num_relations <= 1 || !can_reorder) {
		// nothing to optimize/reorder
		return false;
	}
	// extract the edges of the hypergraph, creating a list of filters and their associated bindings.
	filters_and_bindings = relation_manager.ExtractEdges(op, filter_operators, set_manager);
	// Create the query_graph hyper edges
	CreateHyperGraphEdges();
	return true;
}

void QueryGraphManager::GetColumnBinding(Expression &expression, ColumnBinding &binding) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		// Here you have a filter on a single column in a table. Return a binding for the column
		// being filtered on so the filter estimator knows what HLL count to pull
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		D_ASSERT(relation_manager.relation_mapping.find(colref.binding.table_index) !=
		         relation_manager.relation_mapping.end());
		binding =
		    ColumnBinding(relation_manager.relation_mapping[colref.binding.table_index], colref.binding.column_index);
	}
	// TODO: handle inequality filters with functions.
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) { GetColumnBinding(expr, binding); });
}

const vector<unique_ptr<FilterInfo>> &QueryGraphManager::GetFilterBindings() const {
	return filters_and_bindings;
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
		// we don't, we need to create one
		auto filter = make_uniq<LogicalFilter>();
		filter->children.push_back(std::move(node));
		node = std::move(filter);
	}
	// push the filter into the LogicalFilter
	D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = node->Cast<LogicalFilter>();
	filter.expressions.push_back(std::move(expr));
	return node;
}

void QueryGraphManager::CreateHyperGraphEdges() {
	// create potential edges from the comparisons
	for (auto &filter_info : filters_and_bindings) {
		auto &filter = filter_info->filter;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto &comparison = filter->Cast<BoundComparisonExpression>();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<idx_t> left_bindings, right_bindings;
			relation_manager.ExtractBindings(*comparison.left, left_bindings);
			relation_manager.ExtractBindings(*comparison.right, right_bindings);
			GetColumnBinding(*comparison.left, filter_info->left_binding);
			GetColumnBinding(*comparison.right, filter_info->right_binding);
			if (!left_bindings.empty() && !right_bindings.empty()) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = &set_manager.GetJoinRelation(left_bindings);
				filter_info->right_set = &set_manager.GetJoinRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(*filter_info->left_set, *filter_info->right_set, filter_info);
						query_graph.CreateEdge(*filter_info->right_set, *filter_info->left_set, filter_info);
					} else {
						continue;
					}
					continue;
				}
			}
		}
	}
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(unique_ptr<SingleJoinRelation> &rel) {
	auto &children = rel->parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == &rel->op) {
			// found it! take ownership o/**/f it from the parent
			auto result = std::move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

unique_ptr<LogicalOperator> QueryGraphManager::Reconstruct(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	return RewritePlan(std::move(plan), node);
}

GenerateJoinRelation QueryGraphManager::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
                                                      JoinNode &node) {
	optional_ptr<JoinRelationSet> left_node;
	optional_ptr<JoinRelationSet> right_node;
	optional_ptr<JoinRelationSet> result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node.left && node.right && node.info) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, *node.left);
		auto right = GenerateJoins(extracted_relations, *node.right);

		if (node.info->filters.empty()) {
			// no filters, create a cross product
			result_operator = LogicalCrossProduct::Create(std::move(left.op), std::move(right.op));
		} else {
			// we have filters, create a join node
			auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			// Here we optimize build side probe side. Our build side is the right side
			// So the right plans should have lower cardinalities.
			join->children.push_back(std::move(left.op));
			join->children.push_back(std::move(right.op));

			// set the join conditions from the join node
			for (auto &filter_ref : node.info->filters) {
				auto f = filter_ref.get();
				// extract the filter from the operator it originally belonged to
				D_ASSERT(filters_and_bindings[f->filter_index]->filter);
				auto &filter_and_binding = filters_and_bindings.at(f->filter_index);
				auto condition = std::move(filter_and_binding->filter);
				// now create the actual join condition
				D_ASSERT((JoinRelationSet::IsSubset(*left.set, *f->left_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->right_set)) ||
				         (JoinRelationSet::IsSubset(*left.set, *f->right_set) &&
				          JoinRelationSet::IsSubset(*right.set, *f->left_set)));
				JoinCondition cond;
				D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = condition->Cast<BoundComparisonExpression>();

				// we need to figure out which side is which by looking at the relations available to us
				bool invert = !JoinRelationSet::IsSubset(*left.set, *f->left_set);
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = condition->type;

				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(cond.comparison);
				}
				join->conditions.push_back(std::move(cond));
			}
			D_ASSERT(!join->conditions.empty());
			result_operator = std::move(join);
		}
		left_node = left.set;
		right_node = right.set;
		result_relation = &set_manager.Union(*left.set, *right.set);
	} else {
		// base node, get the entry from the list of extracted relations
		D_ASSERT(node.set.count == 1);
		D_ASSERT(extracted_relations[node.set.relations[0]]);
		result_relation = &node.set;
		result_operator = std::move(extracted_relations[node.set.relations[0]]);
	}
	// TODO: this is where estimated properties start coming into play.
	//  when creating the result operator, we should ask the cost model and cardinality estimator what
	//  the cost and cardinality are
	//	result_operator->estimated_props = node.estimated_props->Copy();
	result_operator->estimated_cardinality = node.cardinality;
	result_operator->has_estimated_cardinality = true;
	if (result_operator->type == LogicalOperatorType::LOGICAL_FILTER &&
	    result_operator->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
		// FILTER on top of GET, add estimated properties to both
		// auto &filter_props = *result_operator->estimated_props;
		auto &child_operator = *result_operator->children[0];
		child_operator.estimated_cardinality = node.cardinality;
		child_operator.has_estimated_cardinality = true;
	}
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (auto &filter_info : filters_and_bindings) {
		// check if the filter has already been extracted
		auto &info = *filter_info;
		if (filters_and_bindings[info.filter_index]->filter) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info.set.count > 0 && JoinRelationSet::IsSubset(*result_relation, info.set)) {
				auto &filter_and_binding = filters_and_bindings[info.filter_index];
				auto filter = std::move(filter_and_binding->filter);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info.left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(*left_node, *info.left_set) &&
				    JoinRelationSet::IsSubset(*right_node, *info.right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(*right_node, *info.left_set) &&
				           JoinRelationSet::IsSubset(*left_node, *info.right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(std::move(result_operator), std::move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = filter->Cast<BoundComparisonExpression>();
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? std::move(comparison.left) : std::move(comparison.right);
				cond.right = !invert ? std::move(comparison.right) : std::move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisonExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(std::move(node->children[0]));
					comp_join->children.push_back(std::move(node->children[1]));
					comp_join->conditions.push_back(std::move(cond));
					if (node == result_operator.get()) {
						result_operator = std::move(comp_join);
					} else {
						D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
						result_operator->children[0] = std::move(comp_join);
					}
				} else {
					D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
					         node->type == LogicalOperatorType::LOGICAL_ASOF_JOIN);
					auto &comp_join = node->Cast<LogicalComparisonJoin>();
					comp_join.conditions.push_back(std::move(cond));
				}
			}
		}
	}
	auto result = GenerateJoinRelation(result_relation, std::move(result_operator));
	return result;
}

const QueryGraphEdges &QueryGraphManager::GetQueryGraphEdges() const {
	return query_graph;
}

void QueryGraphManager::CreateQueryGraphCrossProduct(JoinRelationSet &left, JoinRelationSet &right) {
	query_graph.CreateEdge(left, right, nullptr);
	query_graph.CreateEdge(right, left, nullptr);
}

unique_ptr<LogicalOperator> QueryGraphManager::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	extracted_relations.reserve(relation_manager.NumRelations());
	for (auto &relation : relation_manager.GetRelations()) {
		extracted_relations.push_back(ExtractJoinRelation(relation));
	}

	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (auto &filter : filters_and_bindings) {
		// check if the filter has already been extracted
		if (filter->filter) {
			// if not we need to push it
			join_tree.op = PushFilter(std::move(join_tree.op), std::move(filter->filter));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return std::move(join_tree.op);
	}
	D_ASSERT(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	       op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	       op->type != LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		D_ASSERT(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = std::move(join_tree.op);
	return plan;
}

bool QueryGraphManager::LeftCardLessThanRight(LogicalOperator &op) {
	D_ASSERT(op.children.size() == 2);
	if (op.children[0]->has_estimated_cardinality && op.children[1]->has_estimated_cardinality) {
		return op.children[0]->estimated_cardinality < op.children[1]->estimated_cardinality;
	}
	return op.children[0]->EstimateCardinality(context) < op.children[1]->EstimateCardinality(context);
}

unique_ptr<LogicalOperator> QueryGraphManager::LeftRightOptimizations(unique_ptr<LogicalOperator> input_op) {
	auto op = input_op.get();
	// pass through single child operators
	while (!op->children.empty()) {
		if (op->children.size() == 2) {
			switch (op->type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
				auto &join = op->Cast<LogicalComparisonJoin>();
				if (join.join_type == JoinType::INNER) {
					if (LeftCardLessThanRight(*op)) {
						std::swap(op->children[0], op->children[1]);
						for (auto &cond : join.conditions) {
							std::swap(cond.left, cond.right);
							cond.comparison = FlipComparisonExpression(cond.comparison);
						}
					}
				} else if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
					auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
					auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
					if (rhs_cardinality > lhs_cardinality * 2) {
						join.join_type = JoinType::RIGHT;
						std::swap(join.children[0], join.children[1]);
						for (auto &cond : join.conditions) {
							std::swap(cond.left, cond.right);
							cond.comparison = FlipComparisonExpression(cond.comparison);
						}
					}
				}
				break;
			}
			case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
				if (LeftCardLessThanRight(*op)) {
					std::swap(op->children[0], op->children[1]);
				}
				break;
			}
			case LogicalOperatorType::LOGICAL_ANY_JOIN: {
				auto &join = op->Cast<LogicalAnyJoin>();
				if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
					auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
					auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
					if (rhs_cardinality > lhs_cardinality * 2) {
						join.join_type = JoinType::RIGHT;
						std::swap(join.children[0], join.children[1]);
					}
				} else if (join.join_type == JoinType::INNER && LeftCardLessThanRight(*op)) {
					std::swap(join.children[0], join.children[1]);
				}
				break;
			}
			default:
				break;
			}
			op->children[0] = LeftRightOptimizations(std::move(op->children[0]));
			op->children[1] = LeftRightOptimizations(std::move(op->children[1]));
			// break from while loop
			break;
		}
		if (op->children.size() == 1) {
			op = op->children[0].get();
		}
	}
	return input_op;
}

} // namespace duckdb










#include <cmath>

namespace duckdb {

const vector<RelationStats> RelationManager::GetRelationStats() {
	vector<RelationStats> ret;
	for (idx_t i = 0; i < relations.size(); i++) {
		ret.push_back(relations[i]->stats);
	}
	return ret;
}

vector<unique_ptr<SingleJoinRelation>> RelationManager::GetRelations() {
	return std::move(relations);
}

idx_t RelationManager::NumRelations() {
	return relations.size();
}

void RelationManager::AddAggregateRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                           const RelationStats &stats) {
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	for (auto &index : table_indexes) {
		D_ASSERT(relation_mapping.find(index) == relation_mapping.end());
		relation_mapping[index] = relation_id;
	}
	relations.push_back(std::move(relation));
}

void RelationManager::AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
                                  const RelationStats &stats) {

	// if parent is null, then this is a root relation
	// if parent is not null, it should have multiple children
	D_ASSERT(!parent || parent->children.size() >= 2);
	auto relation = make_uniq<SingleJoinRelation>(op, parent, stats);
	auto relation_id = relations.size();

	auto table_indexes = op.GetTableIndex();
	if (table_indexes.empty()) {
		// relation represents a non-reorderable relation, most likely a join relation
		// Get the tables referenced in the non-reorderable relation and add them to the relation mapping
		// This should all table references, even if there are nested non-reorderable joins.
		unordered_set<idx_t> table_references;
		LogicalJoin::GetTableReferences(op, table_references);
		D_ASSERT(table_references.size() > 0);
		for (auto &reference : table_references) {
			D_ASSERT(relation_mapping.find(reference) == relation_mapping.end());
			relation_mapping[reference] = relation_id;
		}
	} else {
		// Relations should never return more than 1 table index
		D_ASSERT(table_indexes.size() == 1);
		idx_t table_index = table_indexes.at(0);
		D_ASSERT(relation_mapping.find(table_index) == relation_mapping.end());
		relation_mapping[table_index] = relation_id;
	}
	relations.push_back(std::move(relation));
}

static bool OperatorNeedsRelation(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
		return true;
	default:
		return false;
	}
}

static bool OperatorIsNonReorderable(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return true;
	default:
		return false;
	}
}

static bool HasNonReorderableChild(LogicalOperator &op) {
	LogicalOperator *tmp = &op;
	while (tmp->children.size() == 1) {
		if (OperatorNeedsRelation(tmp->type) || OperatorIsNonReorderable(tmp->type)) {
			return true;
		}
		tmp = tmp->children[0].get();
		if (tmp->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = tmp->Cast<LogicalComparisonJoin>();
			if (join.join_type != JoinType::INNER) {
				return true;
			}
		}
	}
	return tmp->children.empty();
}

bool RelationManager::ExtractJoinRelations(LogicalOperator &input_op,
                                           vector<reference<LogicalOperator>> &filter_operators,
                                           optional_ptr<LogicalOperator> parent) {
	LogicalOperator *op = &input_op;
	vector<reference<LogicalOperator>> datasource_filters;
	// pass through single child operators
	while (op->children.size() == 1 && !OperatorNeedsRelation(op->type)) {
		if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (HasNonReorderableChild(*op)) {
				datasource_filters.push_back(*op);
			}
			filter_operators.push_back(*op);
		}
		if (op->type == LogicalOperatorType::LOGICAL_SHOW) {
			return false;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (OperatorIsNonReorderable(op->type)) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(*op);
		} else {
			non_reorderable_operation = true;
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		// stats.cardinality will be initiated to highest cardinality of the children.
		vector<RelationStats> children_stats;
		for (auto &child : op->children) {
			auto stats = RelationStats();
			JoinOrderOptimizer optimizer(context);
			child = optimizer.Optimize(std::move(child), &stats);
			children_stats.push_back(stats);
		}

		auto combined_stats = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(*op, children_stats);
		if (!datasource_filters.empty()) {
			combined_stats.cardinality =
			    (idx_t)MaxValue(combined_stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, combined_stats);
		return true;
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// optimize children
		RelationStats child_stats;
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &aggr = op->Cast<LogicalAggregate>();
		auto operator_stats = RelationStatisticsHelper::ExtractAggregationStats(aggr, child_stats);
		AddAggregateRelation(input_op, parent, operator_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		// optimize children
		RelationStats child_stats;
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &window = op->Cast<LogicalWindow>();
		auto operator_stats = RelationStatisticsHelper::ExtractWindowStats(window, child_stats);
		AddAggregateRelation(input_op, parent, operator_stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Adding relations to the current join order optimizer
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	}
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN: {
		auto &dummy_scan = op->Cast<LogicalDummyScan>();
		auto stats = RelationStatisticsHelper::ExtractDummyScanStats(dummy_scan, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// base table scan, add to set of relations.
		// create empty stats for dummy scan or logical expression get
		auto &expression_get = op->Cast<LogicalExpressionGet>();
		auto stats = RelationStatisticsHelper::ExtractExpressionGetStats(expression_get, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// TODO: Get stats from a logical GET
		auto &get = op->Cast<LogicalGet>();
		auto stats = RelationStatisticsHelper::ExtractGetStats(get, context);
		// if there is another logical filter that could not be pushed down into the
		// table scan, apply another selectivity.
		if (!datasource_filters.empty()) {
			stats.cardinality =
			    (idx_t)MaxValue(stats.cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, (double)1);
		}
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		auto &delim_get = op->Cast<LogicalDelimGet>();
		auto stats = RelationStatisticsHelper::ExtractDelimGetStats(delim_get, context);
		AddRelation(input_op, parent, stats);
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto child_stats = RelationStats();
		// optimize the child and copy the stats
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(std::move(op->children[0]), &child_stats);
		auto &proj = op->Cast<LogicalProjection>();
		// Projection can create columns so we need to add them here
		auto proj_stats = RelationStatisticsHelper::ExtractProjectionStats(proj, child_stats);
		AddRelation(input_op, parent, proj_stats);
		return true;
	}
	default:
		return false;
	}
}

bool RelationManager::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		D_ASSERT(colref.binding.table_index != DConstants::INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		if (expression.alias == "SUBQUERY" &&
		    relation_mapping.find(colref.binding.table_index) == relation_mapping.end()) {
			// most likely a BoundSubqueryExpression that was created from an uncorrelated subquery
			// Here we return true and don't fill the bindings, the expression can be reordered.
			// A filter will be created using this expression, and pushed back on top of the parent
			// operator during plan reconstruction
			return true;
		}
		D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		bindings.insert(relation_mapping[colref.binding.table_index]);
	}
	if (expression.type == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	D_ASSERT(expression.type != ExpressionType::SUBQUERY);
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
		if (!ExtractBindings(expr, bindings)) {
			can_reorder = false;
			return;
		}
	});
	return can_reorder;
}

vector<unique_ptr<FilterInfo>> RelationManager::ExtractEdges(LogicalOperator &op,
                                                             vector<reference<LogicalOperator>> &filter_operators,
                                                             JoinRelationSetManager &set_manager) {
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	vector<unique_ptr<FilterInfo>> filters_and_bindings;
	expression_set_t filter_set;
	for (auto &filter_op : filter_operators) {
		auto &f_op = filter_op.get();
		if (f_op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    f_op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			auto &join = f_op.Cast<LogicalComparisonJoin>();
			D_ASSERT(join.join_type == JoinType::INNER);
			D_ASSERT(join.expressions.empty());
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
				if (filter_set.find(*comparison) == filter_set.end()) {
					filter_set.insert(*comparison);
					unordered_set<idx_t> bindings;
					ExtractBindings(*comparison, bindings);
					auto &set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(comparison), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			join.conditions.clear();
		} else {
			for (auto &expression : f_op.expressions) {
				if (filter_set.find(*expression) == filter_set.end()) {
					filter_set.insert(*expression);
					unordered_set<idx_t> bindings;
					ExtractBindings(*expression, bindings);
					auto &set = set_manager.GetJoinRelation(bindings);
					auto filter_info = make_uniq<FilterInfo>(std::move(expression), set, filters_and_bindings.size());
					filters_and_bindings.push_back(std::move(filter_info));
				}
			}
			f_op.expressions.clear();
		}
	}

	return filters_and_bindings;
}

// LCOV_EXCL_START

void RelationManager::PrintRelationStats() {
#ifdef DEBUG
	string to_print;
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &relation = relations.at(i);
		auto &stats = relation->stats;
		D_ASSERT(stats.column_names.size() == stats.column_distinct_count.size());
		for (idx_t i = 0; i < stats.column_names.size(); i++) {
			to_print = stats.column_names.at(i) + " has estimated distinct count " +
			           to_string(stats.column_distinct_count.at(i).distinct_count);
			Printer::Print(to_print);
		}
		to_print = stats.table_name + " has estimated cardinality " + to_string(stats.cardinality);
		to_print += " and relation id " + to_string(i) + "\n";
		Printer::Print(to_print);
	}
#endif
}

// LCOV_EXCL_STOP

} // namespace duckdb











namespace duckdb {

static ExpressionBinding GetChildColumnBinding(Expression &expr) {
	auto ret = ExpressionBinding();
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_FUNCTION: {
		// TODO: Other expression classes that can have 0 children?
		auto &func = expr.Cast<BoundFunctionExpression>();
		// no children some sort of gen_random_uuid() or equivalent.
		if (func.children.empty()) {
			ret.found_expression = true;
			ret.expression_is_constant = true;
			return ret;
		}
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF: {
		ret.found_expression = true;
		auto &new_col_ref = expr.Cast<BoundColumnRefExpression>();
		ret.child_binding = ColumnBinding(new_col_ref.binding.table_index, new_col_ref.binding.column_index);
		return ret;
	}
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		ret.found_expression = true;
		ret.expression_is_constant = true;
		return ret;
	default:
		break;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		auto recursive_result = GetChildColumnBinding(*child);
		if (recursive_result.found_expression) {
			ret = recursive_result;
		}
	});
	// we didn't find a Bound Column Ref
	return ret;
}

RelationStats RelationStatisticsHelper::ExtractGetStats(LogicalGet &get, ClientContext &context) {
	auto return_stats = RelationStats();

	auto base_table_cardinality = get.EstimateCardinality(context);
	auto cardinality_after_filters = base_table_cardinality;
	unique_ptr<BaseStatistics> column_statistics;

	auto table_thing = get.GetTable();
	auto name = string("some table");
	if (table_thing) {
		name = table_thing->name;
		return_stats.table_name = name;
	}

	// if we can get the catalog table, then our column statistics will be accurate
	// parquet readers etc. will still return statistics, but they initialize distinct column
	// counts to 0.
	// TODO: fix this, some file formats can encode distinct counts, we don't want to rely on
	//  getting a catalog table to know that we can use statistics.
	bool have_catalog_table_statistics = false;
	if (get.GetTable()) {
		have_catalog_table_statistics = true;
	}

	// first push back basic distinct counts for each column (if we have them).
	for (idx_t i = 0; i < get.column_ids.size(); i++) {
		bool have_distinct_count_stats = false;
		if (get.function.statistics) {
			column_statistics = get.function.statistics(context, get.bind_data.get(), get.column_ids[i]);
			if (column_statistics && have_catalog_table_statistics) {
				auto column_distinct_count = DistinctCount({column_statistics->GetDistinctCount(), true});
				return_stats.column_distinct_count.push_back(column_distinct_count);
				return_stats.column_names.push_back(name + "." + get.names.at(get.column_ids.at(i)));
				have_distinct_count_stats = true;
			}
		}
		if (!have_distinct_count_stats) {
			// currently treating the cardinality as the distinct count.
			// the cardinality estimator will update these distinct counts based
			// on the extra columns that are joined on.
			auto column_distinct_count = DistinctCount({cardinality_after_filters, false});
			return_stats.column_distinct_count.push_back(column_distinct_count);
			auto column_name = string("column");
			if (get.column_ids.at(i) < get.names.size()) {
				column_name = get.names.at(get.column_ids.at(i));
			}
			return_stats.column_names.push_back(get.GetName() + "." + column_name);
		}
	}

	if (!get.table_filters.filters.empty()) {
		column_statistics = nullptr;
		for (auto &it : get.table_filters.filters) {
			if (get.bind_data && get.function.name.compare("seq_scan") == 0) {
				auto &table_scan_bind_data = get.bind_data->Cast<TableScanBindData>();
				column_statistics = get.function.statistics(context, &table_scan_bind_data, it.first);
			}

			if (column_statistics && it.second->filter_type == TableFilterType::CONJUNCTION_AND) {
				auto &filter = it.second->Cast<ConjunctionAndFilter>();
				idx_t cardinality_with_and_filter = RelationStatisticsHelper::InspectConjunctionAND(
				    base_table_cardinality, it.first, filter, *column_statistics);
				cardinality_after_filters = MinValue(cardinality_after_filters, cardinality_with_and_filter);
			}
		}
		// if the above code didn't find an equality filter (i.e country_code = "[us]")
		// and there are other table filters (i.e cost > 50), use default selectivity.
		bool has_equality_filter = (cardinality_after_filters != base_table_cardinality);
		if (!has_equality_filter && !get.table_filters.filters.empty()) {
			cardinality_after_filters =
			    MaxValue<idx_t>(base_table_cardinality * RelationStatisticsHelper::DEFAULT_SELECTIVITY, 1);
		}
		if (base_table_cardinality == 0) {
			cardinality_after_filters = 0;
		}
	}
	return_stats.cardinality = cardinality_after_filters;
	// update the estimated cardinality of the get as well.
	// This is not updated during plan reconstruction.
	get.estimated_cardinality = cardinality_after_filters;
	get.has_estimated_cardinality = true;
	D_ASSERT(base_table_cardinality >= cardinality_after_filters);
	return_stats.stats_initialized = true;
	return return_stats;
}

RelationStats RelationStatisticsHelper::ExtractDelimGetStats(LogicalDelimGet &delim_get, ClientContext &context) {
	RelationStats stats;
	stats.table_name = delim_get.GetName();
	idx_t card = delim_get.EstimateCardinality(context);
	stats.cardinality = card;
	stats.stats_initialized = true;
	for (auto &binding : delim_get.GetColumnBindings()) {
		stats.column_distinct_count.push_back(DistinctCount({1, false}));
		stats.column_names.push_back("column" + to_string(binding.column_index));
	}
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractProjectionStats(LogicalProjection &proj, RelationStats &child_stats) {
	auto proj_stats = RelationStats();
	proj_stats.cardinality = child_stats.cardinality;
	proj_stats.table_name = proj.GetName();
	for (auto &expr : proj.expressions) {
		proj_stats.column_names.push_back(expr->GetName());
		auto res = GetChildColumnBinding(*expr);
		D_ASSERT(res.found_expression);
		if (res.expression_is_constant) {
			proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
		} else {
			auto column_index = res.child_binding.column_index;
			if (column_index >= child_stats.column_distinct_count.size() && expr->ToString() == "count_star()") {
				// only one value for a count star
				proj_stats.column_distinct_count.push_back(DistinctCount({1, true}));
			} else {
				// TODO: add this back in
				//	D_ASSERT(column_index < stats.column_distinct_count.size());
				if (column_index < child_stats.column_distinct_count.size()) {
					proj_stats.column_distinct_count.push_back(child_stats.column_distinct_count.at(column_index));
				} else {
					proj_stats.column_distinct_count.push_back(DistinctCount({proj_stats.cardinality, false}));
				}
			}
		}
	}
	proj_stats.stats_initialized = true;
	return proj_stats;
}

RelationStats RelationStatisticsHelper::ExtractDummyScanStats(LogicalDummyScan &dummy_scan, ClientContext &context) {
	auto stats = RelationStats();
	idx_t card = dummy_scan.EstimateCardinality(context);
	stats.cardinality = card;
	for (idx_t i = 0; i < dummy_scan.GetColumnBindings().size(); i++) {
		stats.column_distinct_count.push_back(DistinctCount({card, false}));
		stats.column_names.push_back("dummy_scan_column");
	}
	stats.stats_initialized = true;
	stats.table_name = "dummy scan";
	return stats;
}

void RelationStatisticsHelper::CopyRelationStats(RelationStats &to, const RelationStats &from) {
	to.column_distinct_count = from.column_distinct_count;
	to.column_names = from.column_names;
	to.cardinality = from.cardinality;
	to.table_name = from.table_name;
	to.stats_initialized = from.stats_initialized;
}

RelationStats RelationStatisticsHelper::CombineStatsOfReorderableOperator(vector<ColumnBinding> &bindings,
                                                                          vector<RelationStats> relation_stats) {
	RelationStats stats;
	idx_t max_card = 0;
	for (auto &child_stats : relation_stats) {
		for (idx_t i = 0; i < child_stats.column_distinct_count.size(); i++) {
			stats.column_distinct_count.push_back(child_stats.column_distinct_count.at(i));
			stats.column_names.push_back(child_stats.column_names.at(i));
		}
		stats.table_name += "joined with " + child_stats.table_name;
		max_card = MaxValue(max_card, child_stats.cardinality);
	}
	stats.stats_initialized = true;
	stats.cardinality = max_card;
	return stats;
}

RelationStats RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(LogicalOperator &op,
                                                                             vector<RelationStats> child_stats) {
	D_ASSERT(child_stats.size() == 2);
	RelationStats ret;
	idx_t child_1_card = child_stats[0].stats_initialized ? child_stats[0].cardinality : 0;
	idx_t child_2_card = child_stats[1].stats_initialized ? child_stats[1].cardinality : 0;
	ret.cardinality = MaxValue(child_1_card, child_2_card);
	ret.stats_initialized = true;
	ret.filter_strength = 1;
	ret.table_name = child_stats[0].table_name + " joined with " + child_stats[1].table_name;
	for (auto &stats : child_stats) {
		// MARK joins are nonreorderable. They won't return initialized stats
		// continue in this case.
		if (!stats.stats_initialized) {
			continue;
		}
		for (auto &distinct_count : stats.column_distinct_count) {
			ret.column_distinct_count.push_back(distinct_count);
		}
		for (auto &column_name : stats.column_names) {
			ret.column_names.push_back(column_name);
		}
	}
	return ret;
}

RelationStats RelationStatisticsHelper::ExtractExpressionGetStats(LogicalExpressionGet &expression_get,
                                                                  ClientContext &context) {
	auto stats = RelationStats();
	idx_t card = expression_get.EstimateCardinality(context);
	stats.cardinality = card;
	for (idx_t i = 0; i < expression_get.GetColumnBindings().size(); i++) {
		stats.column_distinct_count.push_back(DistinctCount({card, false}));
		stats.column_names.push_back("expression_get_column");
	}
	stats.stats_initialized = true;
	stats.table_name = "expression_get";
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractWindowStats(LogicalWindow &window, RelationStats &child_stats) {
	RelationStats stats;
	stats.cardinality = child_stats.cardinality;
	stats.column_distinct_count = child_stats.column_distinct_count;
	stats.column_names = child_stats.column_names;
	stats.stats_initialized = true;
	auto num_child_columns = window.GetColumnBindings().size();

	for (idx_t column_index = child_stats.column_distinct_count.size(); column_index < num_child_columns;
	     column_index++) {
		stats.column_distinct_count.push_back(DistinctCount({child_stats.cardinality, false}));
		stats.column_names.push_back("window");
	}
	return stats;
}

RelationStats RelationStatisticsHelper::ExtractAggregationStats(LogicalAggregate &aggr, RelationStats &child_stats) {
	RelationStats stats;
	// TODO: look at child distinct count to better estimate cardinality.
	stats.cardinality = child_stats.cardinality;
	stats.column_distinct_count = child_stats.column_distinct_count;
	stats.column_names = child_stats.column_names;
	stats.stats_initialized = true;
	auto num_child_columns = aggr.GetColumnBindings().size();

	for (idx_t column_index = child_stats.column_distinct_count.size(); column_index < num_child_columns;
	     column_index++) {
		stats.column_distinct_count.push_back(DistinctCount({child_stats.cardinality, false}));
		stats.column_names.push_back("aggregate");
	}
	return stats;
}

idx_t RelationStatisticsHelper::InspectConjunctionAND(idx_t cardinality, idx_t column_index,
                                                      ConjunctionAndFilter &filter, BaseStatistics &base_stats) {
	auto cardinality_after_filters = cardinality;
	for (auto &child_filter : filter.child_filters) {
		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
			continue;
		}
		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
		if (comparison_filter.comparison_type != ExpressionType::COMPARE_EQUAL) {
			continue;
		}
		auto column_count = base_stats.GetDistinctCount();
		auto filtered_card = cardinality;
		// column_count = 0 when there is no column count (i.e parquet scans)
		if (column_count > 0) {
			// we want the ceil of cardinality/column_count. We also want to avoid compiler errors
			filtered_card = (cardinality + column_count - 1) / column_count;
			cardinality_after_filters = filtered_card;
		}
	}
	return cardinality_after_filters;
}

// TODO: Currently only simple AND filters are pushed into table scans.
//  When OR filters are pushed this function can be added
// idx_t RelationStatisticsHelper::InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter
// &filter,
//                                                     BaseStatistics &base_stats) {
//	auto has_equality_filter = false;
//	auto cardinality_after_filters = cardinality;
//	for (auto &child_filter : filter.child_filters) {
//		if (child_filter->filter_type != TableFilterType::CONSTANT_COMPARISON) {
//			continue;
//		}
//		auto &comparison_filter = child_filter->Cast<ConstantFilter>();
//		if (comparison_filter.comparison_type == ExpressionType::COMPARE_EQUAL) {
//			auto column_count = base_stats.GetDistinctCount();
//			auto increment = MaxValue<idx_t>(((cardinality + column_count - 1) / column_count), 1);
//			if (has_equality_filter) {
//				cardinality_after_filters += increment;
//			} else {
//				cardinality_after_filters = increment;
//			}
//			has_equality_filter = true;
//		}
//		if (child_filter->filter_type == TableFilterType::CONJUNCTION_AND) {
//			auto &and_filter = child_filter->Cast<ConjunctionAndFilter>();
//			cardinality_after_filters = RelationStatisticsHelper::InspectConjunctionAND(
//			    cardinality_after_filters, column_index, and_filter, base_stats);
//			continue;
//		}
//	}
//	D_ASSERT(cardinality_after_filters > 0);
//	return cardinality_after_filters;
//}

} // namespace duckdb




namespace duckdb {

bool ExpressionMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	if (type && !type->Match(expr.return_type)) {
		return false;
	}
	if (expr_type && !expr_type->Match(expr.type)) {
		return false;
	}
	if (expr_class != ExpressionClass::INVALID && expr_class != expr.GetExpressionClass()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool ExpressionEqualityMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	if (!expr.Equals(expression)) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

bool CaseExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	return true;
}

bool ComparisonExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundComparisonExpression>();
	vector<reference<Expression>> expressions;
	expressions.push_back(*expr.left);
	expressions.push_back(*expr.right);
	return SetMatcher::Match(matchers, expressions, bindings, policy);
}

bool CastExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	if (!matcher) {
		return true;
	}
	auto &expr = expr_p.Cast<BoundCastExpression>();
	return matcher->Match(*expr.child, bindings);
}

bool InClauseExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundOperatorExpression>();
	if (expr.type != ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
		return false;
	}
	return SetMatcher::Match(matchers, expr.children, bindings, policy);
}

bool ConjunctionExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundConjunctionExpression>();
	if (!SetMatcher::Match(matchers, expr.children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FunctionExpressionMatcher::Match(Expression &expr_p, vector<reference<Expression>> &bindings) {
	if (!ExpressionMatcher::Match(expr_p, bindings)) {
		return false;
	}
	auto &expr = expr_p.Cast<BoundFunctionExpression>();
	if (!FunctionMatcher::Match(function, expr.function.name)) {
		return false;
	}
	if (!SetMatcher::Match(matchers, expr.children, bindings, policy)) {
		return false;
	}
	return true;
}

bool FoldableConstantMatcher::Match(Expression &expr, vector<reference<Expression>> &bindings) {
	// we match on ANY expression that is a scalar expression
	if (!expr.IsFoldable()) {
		return false;
	}
	bindings.push_back(expr);
	return true;
}

} // namespace duckdb




























namespace duckdb {

Optimizer::Optimizer(Binder &binder, ClientContext &context) : context(context), binder(binder), rewriter(context) {
	rewriter.rules.push_back(make_uniq<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_uniq<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ArithmeticSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<CaseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ConjunctionSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<DatePartSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<ComparisonSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<InClauseSimplificationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EqualOrNullSimplification>(rewriter));
	rewriter.rules.push_back(make_uniq<MoveConstantsRule>(rewriter));
	rewriter.rules.push_back(make_uniq<LikeOptimizationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<OrderedAggregateOptimizer>(rewriter));
	rewriter.rules.push_back(make_uniq<RegexOptimizationRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EmptyNeedleRemovalRule>(rewriter));
	rewriter.rules.push_back(make_uniq<EnumComparisonRule>(rewriter));

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		D_ASSERT(rule->root);
	}
#endif
}

ClientContext &Optimizer::GetContext() {
	return context;
}

void Optimizer::RunOptimizer(OptimizerType type, const std::function<void()> &callback) {
	auto &config = DBConfig::GetConfig(context);
	if (config.options.disabled_optimizers.find(type) != config.options.disabled_optimizers.end()) {
		// optimizer is marked as disabled: skip
		return;
	}
	auto &profiler = QueryProfiler::Get(context);
	profiler.StartPhase(OptimizerTypeToString(type));
	callback();
	profiler.EndPhase();
	if (plan) {
		Verify(*plan);
	}
}

void Optimizer::Verify(LogicalOperator &op) {
	ColumnBindingResolver::Verify(op);
}

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan_p) {
	Verify(*plan_p);

	switch (plan_p->type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return plan_p; // skip optimizing simple & often-occurring plans unaffected by rewrites
	default:
		break;
	}

	this->plan = std::move(plan_p);
	// first we perform expression rewrites using the ExpressionRewriter
	// this does not change the logical plan structure, but only simplifies the expression trees
	RunOptimizer(OptimizerType::EXPRESSION_REWRITER, [&]() { rewriter.VisitOperator(*plan); });

	// perform filter pullup
	RunOptimizer(OptimizerType::FILTER_PULLUP, [&]() {
		FilterPullup filter_pullup;
		plan = filter_pullup.Rewrite(std::move(plan));
	});

	// perform filter pushdown
	RunOptimizer(OptimizerType::FILTER_PUSHDOWN, [&]() {
		FilterPushdown filter_pushdown(*this);
		plan = filter_pushdown.Rewrite(std::move(plan));
	});

	RunOptimizer(OptimizerType::REGEX_RANGE, [&]() {
		RegexRangeFilter regex_opt;
		plan = regex_opt.Rewrite(std::move(plan));
	});

	RunOptimizer(OptimizerType::IN_CLAUSE, [&]() {
		InClauseRewriter ic_rewriter(context, *this);
		plan = ic_rewriter.Rewrite(std::move(plan));
	});

	// removes any redundant DelimGets/DelimJoins
	RunOptimizer(OptimizerType::DELIMINATOR, [&]() {
		Deliminator deliminator;
		plan = deliminator.Optimize(std::move(plan));
	});

	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	RunOptimizer(OptimizerType::JOIN_ORDER, [&]() {
		JoinOrderOptimizer optimizer(context);
		plan = optimizer.Optimize(std::move(plan));
	});

	// rewrites UNNESTs in DelimJoins by moving them to the projection
	RunOptimizer(OptimizerType::UNNEST_REWRITER, [&]() {
		UnnestRewriter unnest_rewriter;
		plan = unnest_rewriter.Optimize(std::move(plan));
	});

	// removes unused columns
	RunOptimizer(OptimizerType::UNUSED_COLUMNS, [&]() {
		RemoveUnusedColumns unused(binder, context, true);
		unused.VisitOperator(*plan);
	});

	// Remove duplicate groups from aggregates
	RunOptimizer(OptimizerType::DUPLICATE_GROUPS, [&]() {
		RemoveDuplicateGroups remove;
		remove.VisitOperator(*plan);
	});

	// then we extract common subexpressions inside the different operators
	RunOptimizer(OptimizerType::COMMON_SUBEXPRESSIONS, [&]() {
		CommonSubExpressionOptimizer cse_optimizer(binder);
		cse_optimizer.VisitOperator(*plan);
	});

	// creates projection maps so unused columns are projected out early
	RunOptimizer(OptimizerType::COLUMN_LIFETIME, [&]() {
		ColumnLifetimeAnalyzer column_lifetime(true);
		column_lifetime.VisitOperator(*plan);
	});

	// perform statistics propagation
	column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;
	RunOptimizer(OptimizerType::STATISTICS_PROPAGATION, [&]() {
		StatisticsPropagator propagator(*this);
		propagator.PropagateStatistics(plan);
		statistics_map = propagator.GetStatisticsMap();
	});

	// remove duplicate aggregates
	RunOptimizer(OptimizerType::COMMON_AGGREGATE, [&]() {
		CommonAggregateOptimizer common_aggregate;
		common_aggregate.VisitOperator(*plan);
	});

	// creates projection maps so unused columns are projected out early
	RunOptimizer(OptimizerType::COLUMN_LIFETIME, [&]() {
		ColumnLifetimeAnalyzer column_lifetime(true);
		column_lifetime.VisitOperator(*plan);
	});

	// compress data based on statistics for materializing operators
	RunOptimizer(OptimizerType::COMPRESSED_MATERIALIZATION, [&]() {
		CompressedMaterialization compressed_materialization(context, binder, std::move(statistics_map));
		compressed_materialization.Compress(plan);
	});

	// transform ORDER BY + LIMIT to TopN
	RunOptimizer(OptimizerType::TOP_N, [&]() {
		TopN topn;
		plan = topn.Optimize(std::move(plan));
	});

	// apply simple expression heuristics to get an initial reordering
	RunOptimizer(OptimizerType::REORDER_FILTER, [&]() {
		ExpressionHeuristics expression_heuristics(*this);
		plan = expression_heuristics.Rewrite(std::move(plan));
	});

	for (auto &optimizer_extension : DBConfig::GetConfig(context).optimizer_extensions) {
		RunOptimizer(OptimizerType::EXTENSION, [&]() {
			optimizer_extension.optimize_function(context, optimizer_extension.optimizer_info.get(), plan);
		});
	}

	Planner::VerifyPlan(context, plan);

	return std::move(plan);
}

} // namespace duckdb


namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::PullupBothSide(unique_ptr<LogicalOperator> op) {
	FilterPullup left_pullup(true, can_add_column);
	FilterPullup right_pullup(true, can_add_column);
	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));
	D_ASSERT(left_pullup.can_add_column == can_add_column);
	D_ASSERT(right_pullup.can_add_column == can_add_column);

	// merging filter expressions
	for (idx_t i = 0; i < right_pullup.filters_expr_pullup.size(); ++i) {
		left_pullup.filters_expr_pullup.push_back(std::move(right_pullup.filters_expr_pullup[i]));
	}

	if (!left_pullup.filters_expr_pullup.empty()) {
		return GeneratePullupFilter(std::move(op), left_pullup.filters_expr_pullup);
	}
	return op;
}

} // namespace duckdb






namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::PullupFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);

	auto &filter = op->Cast<LogicalFilter>();
	if (can_pullup && filter.projection_map.empty()) {
		unique_ptr<LogicalOperator> child = std::move(op->children[0]);
		child = Rewrite(std::move(child));
		// moving filter's expressions
		for (idx_t i = 0; i < op->expressions.size(); ++i) {
			filters_expr_pullup.push_back(std::move(op->expressions[i]));
		}
		return child;
	}
	op->children[0] = Rewrite(std::move(op->children[0]));
	return op;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<LogicalOperator> FilterPullup::PullupFromLeft(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	         op->type == LogicalOperatorType::LOGICAL_EXCEPT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN);

	FilterPullup left_pullup(true, can_add_column);
	FilterPullup right_pullup(false, can_add_column);

	op->children[0] = left_pullup.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pullup.Rewrite(std::move(op->children[1]));

	// check only for filters from the LHS
	if (!left_pullup.filters_expr_pullup.empty() && right_pullup.filters_expr_pullup.empty()) {
		return GeneratePullupFilter(std::move(op), left_pullup.filters_expr_pullup);
	}
	return op;
}

} // namespace duckdb







namespace duckdb {

static void RevertFilterPullup(LogicalProjection &proj, vector<unique_ptr<Expression>> &expressions) {
	unique_ptr<LogicalFilter> filter = make_uniq<LogicalFilter>();
	for (idx_t i = 0; i < expressions.size(); ++i) {
		filter->expressions.push_back(std::move(expressions[i]));
	}
	expressions.clear();
	filter->children.push_back(std::move(proj.children[0]));
	proj.children[0] = std::move(filter);
}

static void ReplaceExpressionBinding(vector<unique_ptr<Expression>> &proj_expressions, Expression &expr,
                                     idx_t proj_table_idx) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		bool found_proj_col = false;
		BoundColumnRefExpression &colref = expr.Cast<BoundColumnRefExpression>();
		// find the corresponding column index in the projection expressions
		for (idx_t proj_idx = 0; proj_idx < proj_expressions.size(); proj_idx++) {
			auto &proj_expr = *proj_expressions[proj_idx];
			if (proj_expr.type == ExpressionType::BOUND_COLUMN_REF) {
				if (colref.Equals(proj_expr)) {
					colref.binding.table_index = proj_table_idx;
					colref.binding.column_index = proj_idx;
					found_proj_col = true;
					break;
				}
			}
		}
		if (!found_proj_col) {
			// Project a new column
			auto new_colref = colref.Copy();
			colref.binding.table_index = proj_table_idx;
			colref.binding.column_index = proj_expressions.size();
			proj_expressions.push_back(std::move(new_colref));
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { return ReplaceExpressionBinding(proj_expressions, child, proj_table_idx); });
}

void FilterPullup::ProjectSetOperation(LogicalProjection &proj) {
	vector<unique_ptr<Expression>> copy_proj_expressions;
	// copying the project expressions, it's useful whether we should revert the filter pullup
	for (idx_t i = 0; i < proj.expressions.size(); ++i) {
		copy_proj_expressions.push_back(proj.expressions[i]->Copy());
	}

	// Replace filter expression bindings, when need we add new columns into the copied projection expression
	vector<unique_ptr<Expression>> changed_filter_expressions;
	for (idx_t i = 0; i < filters_expr_pullup.size(); ++i) {
		auto copy_filter_expr = filters_expr_pullup[i]->Copy();
		ReplaceExpressionBinding(copy_proj_expressions, (Expression &)*copy_filter_expr, proj.table_index);
		changed_filter_expressions.push_back(std::move(copy_filter_expr));
	}

	/// Case new columns were added into the projection
	// we must skip filter pullup because adding new columns to these operators will change the result
	if (copy_proj_expressions.size() > proj.expressions.size()) {
		RevertFilterPullup(proj, filters_expr_pullup);
		return;
	}

	// now we must replace the filter bindings
	D_ASSERT(filters_expr_pullup.size() == changed_filter_expressions.size());
	for (idx_t i = 0; i < filters_expr_pullup.size(); ++i) {
		filters_expr_pullup[i] = std::move(changed_filter_expressions[i]);
	}
}

unique_ptr<LogicalOperator> FilterPullup::PullupProjection(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_PROJECTION);
	op->children[0] = Rewrite(std::move(op->children[0]));
	if (!filters_expr_pullup.empty()) {
		auto &proj = op->Cast<LogicalProjection>();
		// INTERSECT, EXCEPT, and DISTINCT
		if (!can_add_column) {
			// special treatment for operators that cannot add columns, e.g., INTERSECT, EXCEPT, and DISTINCT
			ProjectSetOperation(proj);
			return op;
		}

		for (idx_t i = 0; i < filters_expr_pullup.size(); ++i) {
			auto &expr = (Expression &)*filters_expr_pullup[i];
			ReplaceExpressionBinding(proj.expressions, expr, proj.table_index);
		}
	}
	return op;
}

} // namespace duckdb





namespace duckdb {

static void ReplaceFilterTableIndex(Expression &expr, LogicalSetOperation &setop) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);

		colref.binding.table_index = setop.table_index;
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ReplaceFilterTableIndex(child, setop); });
}

unique_ptr<LogicalOperator> FilterPullup::PullupSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT);
	can_add_column = false;
	can_pullup = true;
	if (op->type == LogicalOperatorType::LOGICAL_INTERSECT) {
		op = PullupBothSide(std::move(op));
	} else {
		// EXCEPT only pull ups from LHS
		op = PullupFromLeft(std::move(op));
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		auto &setop = filter.children[0]->Cast<LogicalSetOperation>();
		for (idx_t i = 0; i < filter.expressions.size(); ++i) {
			ReplaceFilterTableIndex(*filter.expressions[i], setop);
		}
	}
	return op;
}

} // namespace duckdb







namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ExtractFilterBindings(Expression &expr, vector<ColumnBinding> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		bindings.push_back(colref.binding);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractFilterBindings(child, bindings); });
}

static unique_ptr<Expression> ReplaceGroupBindings(LogicalAggregate &proj, unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.binding.table_index == proj.group_index);
		D_ASSERT(colref.binding.column_index < proj.groups.size());
		D_ASSERT(colref.depth == 0);
		// replace the binding with a copy to the expression at the referenced index
		return proj.groups[colref.binding.column_index]->Copy();
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { child = ReplaceGroupBindings(proj, std::move(child)); });
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownAggregate(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	auto &aggr = op->Cast<LogicalAggregate>();

	// pushdown into AGGREGATE and GROUP BY
	// we cannot push expressions that refer to the aggregate
	FilterPushdown child_pushdown(optimizer);
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		if (f.bindings.find(aggr.aggregate_index) != f.bindings.end()) {
			// filter on aggregate: cannot pushdown
			continue;
		}
		if (f.bindings.find(aggr.groupings_index) != f.bindings.end()) {
			// filter on GROUPINGS function: cannot pushdown
			continue;
		}
		// no aggregate! we are filtering on a group
		// we can only push this down if the filter is in all grouping sets
		vector<ColumnBinding> bindings;
		ExtractFilterBindings(*f.filter, bindings);

		bool can_pushdown_filter = true;
		if (aggr.grouping_sets.empty()) {
			// empty grouping set - we cannot pushdown the filter
			can_pushdown_filter = false;
		}
		for (auto &grp : aggr.grouping_sets) {
			// check for each of the grouping sets if they contain all groups
			if (bindings.empty()) {
				// we can never push down empty grouping sets
				can_pushdown_filter = false;
				break;
			}
			for (auto &binding : bindings) {
				if (grp.find(binding.column_index) == grp.end()) {
					can_pushdown_filter = false;
					break;
				}
			}
			if (!can_pushdown_filter) {
				break;
			}
		}
		if (!can_pushdown_filter) {
			continue;
		}
		// no aggregate! we can push this down
		// rewrite any group bindings within the filter
		f.filter = ReplaceGroupBindings(aggr, std::move(f.filter));
		// add the filter to the child node
		if (child_pushdown.AddFilter(std::move(f.filter)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
		// erase the filter from here
		filters.erase(filters.begin() + i);
		i--;
	}
	child_pushdown.GenerateFilters();

	op->children[0] = child_pushdown.Rewrite(std::move(op->children[0]));
	return FinishPushdown(std::move(op));
}

} // namespace duckdb




namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	FilterPushdown left_pushdown(optimizer), right_pushdown(optimizer);
	vector<unique_ptr<Expression>> join_expressions;
	unordered_set<idx_t> left_bindings, right_bindings;
	if (!filters.empty()) {
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
		LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
		// now check the set of filters
		for (auto &f : filters) {
			auto side = JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT) {
				// bindings match left side: push into left
				left_pushdown.filters.push_back(std::move(f));
			} else if (side == JoinSide::RIGHT) {
				// bindings match right side: push into right
				right_pushdown.filters.push_back(std::move(f));
			} else {
				D_ASSERT(side == JoinSide::BOTH || side == JoinSide::NONE);
				// bindings match both: turn into join condition
				join_expressions.push_back(std::move(f->filter));
			}
		}
	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	if (!join_expressions.empty()) {
		// join conditions found: turn into inner join
		// extract join conditions
		vector<JoinCondition> conditions;
		vector<unique_ptr<Expression>> arbitrary_expressions;
		auto join_type = JoinType::INNER;
		LogicalComparisonJoin::ExtractJoinConditions(GetContext(), join_type, op->children[0], op->children[1],
		                                             left_bindings, right_bindings, join_expressions, conditions,
		                                             arbitrary_expressions);
		// create the join from the join conditions
		return LogicalComparisonJoin::CreateJoin(GetContext(), JoinType::INNER, JoinRefType::REGULAR,
		                                         std::move(op->children[0]), std::move(op->children[1]),
		                                         std::move(conditions), std::move(arbitrary_expressions));
	} else {
		// no join conditions found: keep as cross product
		return op;
	}
}

} // namespace duckdb




namespace duckdb {

using Filter = FilterPushdown::Filter;

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &filter = op->Cast<LogicalFilter>();
	if (!filter.projection_map.empty()) {
		return FinishPushdown(std::move(op));
	}
	// filter: gather the filters and remove the filter from the set of operations
	for (auto &expression : filter.expressions) {
		if (AddFilter(std::move(expression)) == FilterResult::UNSATISFIABLE) {
			// filter statically evaluates to false, strip tree
			return make_uniq<LogicalEmptyResult>(std::move(op));
		}
	}
	GenerateFilters();
	return Rewrite(std::move(filter.children[0]));
}

} // namespace duckdb







namespace duckdb {

unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op->Cast<LogicalGet>();

	if (get.function.pushdown_complex_filter || get.function.filter_pushdown) {
		// this scan supports some form of filter push-down
		// check if there are any parameters
		// if there are, invalidate them to force a re-bind on execution
		for (auto &filter : filters) {
			if (filter->filter->HasParameter()) {
				// there is a parameter in the filters! invalidate it
				BoundParameterExpression::InvalidateRecursive(*filter->filter);
			}
		}
	}
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(filters.size());
		for (auto &filter : filters) {
			expressions.push_back(std::move(filter->filter));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.empty()) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_uniq<Filter>();
			f->filter = std::move(expr);
			f->ExtractBindings();
			filters.push_back(std::move(f));
		}
	}

	if (!get.table_filters.filters.empty() || !get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(std::move(op));
	}
	PushFilters();

	//! We generate the table filters that will be executed during the table scan
	//! Right now this only executes simple AND filters
	get.table_filters = combiner.GenerateTableScanFilters(get.column_ids);

	// //! For more complex filters if all filters to a column are constants we generate a min max boundary used to
	// check
	// //! the zonemaps.
	// auto zonemap_checks = combiner.GenerateZonemapChecks(get.column_ids, get.table_filters);

	// for (auto &f : get.table_filters) {
	// 	f.column_index = get.column_ids[f.column_index];
	// }

	// //! Use zonemap checks as table filters for pre-processing
	// for (auto &zonemap_check : zonemap_checks) {
	// 	if (zonemap_check.column_index != COLUMN_IDENTIFIER_ROW_ID) {
	// 		get.table_filters.push_back(zonemap_check);
	// 	}
	// }

	GenerateFilters();

	//! Now we try to pushdown the remaining filters to perform zonemap checking
	return FinishPushdown(std::move(op));
}

} // namespace duckdb

#endif
