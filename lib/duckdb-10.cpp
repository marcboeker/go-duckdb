#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif







namespace duckdb {

Index::Index(AttachedDatabase &db, IndexType type, TableIOManager &table_io_manager,
             const vector<column_t> &column_ids_p, const vector<unique_ptr<Expression>> &unbound_expressions,
             IndexConstraintType constraint_type_p)

    : type(type), table_io_manager(table_io_manager), column_ids(column_ids_p), constraint_type(constraint_type_p),
      db(db) {

	for (auto &expr : unbound_expressions) {
		types.push_back(expr->return_type.InternalType());
		logical_types.push_back(expr->return_type);
		auto unbound_expression = expr->Copy();
		bound_expressions.push_back(BindExpression(unbound_expression->Copy()));
		this->unbound_expressions.emplace_back(std::move(unbound_expression));
	}
	for (auto &bound_expr : bound_expressions) {
		executor.AddExpression(*bound_expr);
	}

	// create the column id set
	column_id_set.insert(column_ids.begin(), column_ids.end());
}

void Index::InitializeLock(IndexLock &state) {
	state.index_lock = unique_lock<mutex>(lock);
}

PreservedError Index::Append(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	return Append(state, entries, row_identifiers);
}

void Index::CommitDrop() {
	IndexLock index_lock;
	InitializeLock(index_lock);
	CommitDrop(index_lock);
}

void Index::Delete(DataChunk &entries, Vector &row_identifiers) {
	IndexLock state;
	InitializeLock(state);
	Delete(state, entries, row_identifiers);
}

bool Index::MergeIndexes(Index &other_index) {
	IndexLock state;
	InitializeLock(state);
	return MergeIndexes(state, other_index);
}

string Index::VerifyAndToString(const bool only_verify) {
	IndexLock state;
	InitializeLock(state);
	return VerifyAndToString(state, only_verify);
}

void Index::Vacuum() {
	IndexLock state;
	InitializeLock(state);
	Vacuum(state);
}

void Index::ExecuteExpressions(DataChunk &input, DataChunk &result) {
	executor.Execute(input, result);
}

unique_ptr<Expression> Index::BindExpression(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr->Cast<BoundColumnRefExpression>();
		return make_uniq<BoundReferenceExpression>(expr->return_type, column_ids[bound_colref.binding.column_index]);
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [this](unique_ptr<Expression> &expr) { expr = BindExpression(std::move(expr)); });
	return expr;
}

bool Index::IndexIsUpdated(const vector<PhysicalIndex> &column_ids) const {
	for (auto &column : column_ids) {
		if (column_id_set.find(column.index) != column_id_set.end()) {
			return true;
		}
	}
	return false;
}

BlockPointer Index::Serialize(MetadataWriter &writer) {
	throw NotImplementedException("The implementation of this index serialization does not exist.");
}

string Index::AppendRowError(DataChunk &input, idx_t index) {
	string error;
	for (idx_t c = 0; c < input.ColumnCount(); c++) {
		if (c > 0) {
			error += ", ";
		}
		error += input.GetValue(c, index).ToString();
	}
	return error;
}

} // namespace duckdb














namespace duckdb {

LocalTableStorage::LocalTableStorage(DataTable &table)
    : table_ref(table), allocator(Allocator::Get(table.db)), deleted_rows(0), optimistic_writer(table),
      merged_storage(false) {
	auto types = table.GetTypes();
	row_groups = make_shared<RowGroupCollection>(table.info, TableIOManager::Get(table).GetBlockManagerForRowData(),
	                                             types, MAX_ROW_ID, 0);
	row_groups->InitializeEmpty();

	table.info->indexes.Scan([&](Index &index) {
		D_ASSERT(index.type == IndexType::ART);
		auto &art = index.Cast<ART>();
		if (art.constraint_type != IndexConstraintType::NONE) {
			// unique index: create a local ART index that maintains the same unique constraint
			vector<unique_ptr<Expression>> unbound_expressions;
			unbound_expressions.reserve(art.unbound_expressions.size());
			for (auto &expr : art.unbound_expressions) {
				unbound_expressions.push_back(expr->Copy());
			}
			indexes.AddIndex(make_uniq<ART>(art.column_ids, art.table_io_manager, std::move(unbound_expressions),
			                                art.constraint_type, art.db));
		}
		return false;
	});
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     idx_t changed_idx, const LogicalType &target_type,
                                     const vector<column_t> &bound_columns, Expression &cast_expr)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(new_dt, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)),
      merged_storage(parent.merged_storage) {
	row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(DataTable &new_dt, LocalTableStorage &parent, idx_t drop_idx)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(new_dt, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)),
      merged_storage(parent.merged_storage) {
	row_groups = parent.row_groups->RemoveColumn(drop_idx);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::LocalTableStorage(ClientContext &context, DataTable &new_dt, LocalTableStorage &parent,
                                     ColumnDefinition &new_column, Expression &default_value)
    : table_ref(new_dt), allocator(Allocator::Get(new_dt.db)), deleted_rows(parent.deleted_rows),
      optimistic_writer(new_dt, parent.optimistic_writer), optimistic_writers(std::move(parent.optimistic_writers)),
      merged_storage(parent.merged_storage) {
	row_groups = parent.row_groups->AddColumn(context, new_column, default_value);
	parent.row_groups.reset();
	indexes.Move(parent.indexes);
}

LocalTableStorage::~LocalTableStorage() {
}

void LocalTableStorage::InitializeScan(CollectionScanState &state, optional_ptr<TableFilterSet> table_filters) {
	if (row_groups->GetTotalRows() == 0) {
		throw InternalException("No rows in LocalTableStorage row group for scan");
	}
	row_groups->InitializeScan(state, state.GetColumnIds(), table_filters.get());
}

idx_t LocalTableStorage::EstimatedSize() {
	idx_t appended_rows = row_groups->GetTotalRows() - deleted_rows;
	idx_t row_size = 0;
	auto &types = row_groups->GetTypes();
	for (auto &type : types) {
		row_size += GetTypeIdSize(type.InternalType());
	}
	return appended_rows * row_size;
}

void LocalTableStorage::WriteNewRowGroup() {
	if (deleted_rows != 0) {
		// we have deletes - we cannot merge row groups
		return;
	}
	optimistic_writer.WriteNewRowGroup(*row_groups);
}

void LocalTableStorage::FlushBlocks() {
	if (!merged_storage && row_groups->GetTotalRows() > Storage::ROW_GROUP_SIZE) {
		optimistic_writer.WriteLastRowGroup(*row_groups);
	}
	optimistic_writer.FinalFlush();
}

PreservedError LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source,
                                                  TableIndexList &index_list, const vector<LogicalType> &table_types,
                                                  row_t &start_row) {
	// only need to scan for index append
	// figure out which columns we need to scan for the set of indexes
	auto columns = index_list.GetRequiredColumns();
	// create an empty mock chunk that contains all the correct types for the table
	DataChunk mock_chunk;
	mock_chunk.InitializeEmpty(table_types);
	PreservedError error;
	source.Scan(transaction, columns, [&](DataChunk &chunk) -> bool {
		// construct the mock chunk by referencing the required columns
		for (idx_t i = 0; i < columns.size(); i++) {
			mock_chunk.data[columns[i]].Reference(chunk.data[i]);
		}
		mock_chunk.SetCardinality(chunk);
		// append this chunk to the indexes of the table
		error = DataTable::AppendToIndexes(index_list, mock_chunk, start_row);
		if (error) {
			return false;
		}
		start_row += chunk.size();
		return true;
	});
	return error;
}

void LocalTableStorage::AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state,
                                        idx_t append_count, bool append_to_table) {
	auto &table = table_ref.get();
	if (append_to_table) {
		table.InitializeAppend(transaction, append_state, append_count);
	}
	PreservedError error;
	if (append_to_table) {
		// appending: need to scan entire
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			error = table.AppendToIndexes(chunk, append_state.current_row);
			if (error) {
				return false;
			}
			// append to base table
			table.Append(chunk, append_state);
			return true;
		});
	} else {
		error =
		    AppendToIndexes(transaction, *row_groups, table.info->indexes, table.GetTypes(), append_state.current_row);
	}
	if (error) {
		// need to revert all appended row ids
		row_t current_row = append_state.row_start;
		// remove the data from the indexes, if there are any indexes
		row_groups->Scan(transaction, [&](DataChunk &chunk) -> bool {
			// append this chunk to the indexes of the table
			try {
				table.RemoveFromIndexes(append_state, chunk, current_row);
			} catch (Exception &ex) {
				error = PreservedError(ex);
				return false;
			} catch (std::exception &ex) { // LCOV_EXCL_START
				error = PreservedError(ex);
				return false;
			} // LCOV_EXCL_STOP

			current_row += chunk.size();
			if (current_row >= append_state.current_row) {
				// finished deleting all rows from the index: abort now
				return false;
			}
			return true;
		});
		if (append_to_table) {
			table.RevertAppendInternal(append_state.row_start);
		}

		// we need to vacuum the indexes to remove any buffers that are now empty
		// due to reverting the appends
		table.info->indexes.Scan([&](Index &index) {
			index.Vacuum();
			return false;
		});
		error.Throw();
	}
}

OptimisticDataWriter &LocalTableStorage::CreateOptimisticWriter() {
	auto writer = make_uniq<OptimisticDataWriter>(table_ref.get());
	optimistic_writers.push_back(std::move(writer));
	return *optimistic_writers.back();
}

void LocalTableStorage::FinalizeOptimisticWriter(OptimisticDataWriter &writer) {
	// remove the writer from the set of optimistic writers
	unique_ptr<OptimisticDataWriter> owned_writer;
	for (idx_t i = 0; i < optimistic_writers.size(); i++) {
		if (optimistic_writers[i].get() == &writer) {
			owned_writer = std::move(optimistic_writers[i]);
			optimistic_writers.erase(optimistic_writers.begin() + i);
			break;
		}
	}
	if (!owned_writer) {
		throw InternalException("Error in FinalizeOptimisticWriter - could not find writer");
	}
	optimistic_writer.Merge(*owned_writer);
}

void LocalTableStorage::Rollback() {
	for (auto &writer : optimistic_writers) {
		writer->Rollback();
	}
	optimistic_writers.clear();
	optimistic_writer.Rollback();
}

//===--------------------------------------------------------------------===//
// LocalTableManager
//===--------------------------------------------------------------------===//
optional_ptr<LocalTableStorage> LocalTableManager::GetStorage(DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	return entry == table_storage.end() ? nullptr : entry->second.get();
}

LocalTableStorage &LocalTableManager::GetOrCreateStorage(DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		auto new_storage = make_shared<LocalTableStorage>(table);
		auto storage = new_storage.get();
		table_storage.insert(make_pair(reference<DataTable>(table), std::move(new_storage)));
		return *storage;
	} else {
		return *entry->second.get();
	}
}

bool LocalTableManager::IsEmpty() {
	lock_guard<mutex> l(table_storage_lock);
	return table_storage.empty();
}

shared_ptr<LocalTableStorage> LocalTableManager::MoveEntry(DataTable &table) {
	lock_guard<mutex> l(table_storage_lock);
	auto entry = table_storage.find(table);
	if (entry == table_storage.end()) {
		return nullptr;
	}
	auto storage_entry = std::move(entry->second);
	table_storage.erase(entry);
	return storage_entry;
}

reference_map_t<DataTable, shared_ptr<LocalTableStorage>> LocalTableManager::MoveEntries() {
	lock_guard<mutex> l(table_storage_lock);
	return std::move(table_storage);
}

idx_t LocalTableManager::EstimatedSize() {
	lock_guard<mutex> l(table_storage_lock);
	idx_t estimated_size = 0;
	for (auto &storage : table_storage) {
		estimated_size += storage.second->EstimatedSize();
	}
	return estimated_size;
}

void LocalTableManager::InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry) {
	lock_guard<mutex> l(table_storage_lock);
	D_ASSERT(table_storage.find(table) == table_storage.end());
	table_storage[table] = std::move(entry);
}

//===--------------------------------------------------------------------===//
// LocalStorage
//===--------------------------------------------------------------------===//
LocalStorage::LocalStorage(ClientContext &context, DuckTransaction &transaction)
    : context(context), transaction(transaction) {
}

LocalStorage::CommitState::CommitState() {
}

LocalStorage::CommitState::~CommitState() {
}

LocalStorage &LocalStorage::Get(DuckTransaction &transaction) {
	return transaction.GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db).GetLocalStorage();
}

LocalStorage &LocalStorage::Get(ClientContext &context, Catalog &catalog) {
	return LocalStorage::Get(context, catalog.GetAttached());
}

void LocalStorage::InitializeScan(DataTable &table, CollectionScanState &state,
                                  optional_ptr<TableFilterSet> table_filters) {
	auto storage = table_manager.GetStorage(table);
	if (storage == nullptr) {
		return;
	}
	storage->InitializeScan(state, table_filters);
}

void LocalStorage::Scan(CollectionScanState &state, const vector<storage_t> &column_ids, DataChunk &result) {
	state.Scan(transaction, result);
}

void LocalStorage::InitializeParallelScan(DataTable &table, ParallelCollectionScanState &state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		state.max_row = 0;
		state.vector_index = 0;
		state.current_row_group = nullptr;
	} else {
		storage->row_groups->InitializeParallelScan(state);
	}
}

bool LocalStorage::NextParallelScan(ClientContext &context, DataTable &table, ParallelCollectionScanState &state,
                                    CollectionScanState &scan_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return false;
	}
	return storage->row_groups->NextParallelScan(context, state, scan_state);
}

void LocalStorage::InitializeAppend(LocalAppendState &state, DataTable &table) {
	state.storage = &table_manager.GetOrCreateStorage(table);
	state.storage->row_groups->InitializeAppend(TransactionData(transaction), state.append_state, 0);
}

void LocalStorage::Append(LocalAppendState &state, DataChunk &chunk) {
	// append to unique indices (if any)
	auto storage = state.storage;
	idx_t base_id = MAX_ROW_ID + storage->row_groups->GetTotalRows() + state.append_state.total_append_count;
	auto error = DataTable::AppendToIndexes(storage->indexes, chunk, base_id);
	if (error) {
		error.Throw();
	}

	//! Append the chunk to the local storage
	auto new_row_group = storage->row_groups->Append(chunk, state.append_state);
	//! Check if we should pre-emptively flush blocks to disk
	if (new_row_group) {
		storage->WriteNewRowGroup();
	}
}

void LocalStorage::FinalizeAppend(LocalAppendState &state) {
	state.storage->row_groups->FinalizeAppend(state.append_state.transaction, state.append_state);
}

void LocalStorage::LocalMerge(DataTable &table, RowGroupCollection &collection) {
	auto &storage = table_manager.GetOrCreateStorage(table);
	if (!storage.indexes.Empty()) {
		// append data to indexes if required
		row_t base_id = MAX_ROW_ID + storage.row_groups->GetTotalRows();
		auto error = storage.AppendToIndexes(transaction, collection, storage.indexes, table.GetTypes(), base_id);
		if (error) {
			error.Throw();
		}
	}
	storage.row_groups->MergeStorage(collection);
	storage.merged_storage = true;
}

OptimisticDataWriter &LocalStorage::CreateOptimisticWriter(DataTable &table) {
	auto &storage = table_manager.GetOrCreateStorage(table);
	return storage.CreateOptimisticWriter();
}

void LocalStorage::FinalizeOptimisticWriter(DataTable &table, OptimisticDataWriter &writer) {
	auto &storage = table_manager.GetOrCreateStorage(table);
	storage.FinalizeOptimisticWriter(writer);
}

bool LocalStorage::ChangesMade() noexcept {
	return !table_manager.IsEmpty();
}

bool LocalStorage::Find(DataTable &table) {
	return table_manager.GetStorage(table) != nullptr;
}

idx_t LocalStorage::EstimatedSize() {
	return table_manager.EstimatedSize();
}

idx_t LocalStorage::Delete(DataTable &table, Vector &row_ids, idx_t count) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	// delete from unique indices (if any)
	if (!storage->indexes.Empty()) {
		storage->row_groups->RemoveFromIndexes(storage->indexes, row_ids, count);
	}

	auto ids = FlatVector::GetData<row_t>(row_ids);
	idx_t delete_count = storage->row_groups->Delete(TransactionData(0, 0), table, ids, count);
	storage->deleted_rows += delete_count;
	return delete_count;
}

void LocalStorage::Update(DataTable &table, Vector &row_ids, const vector<PhysicalIndex> &column_ids,
                          DataChunk &updates) {
	auto storage = table_manager.GetStorage(table);
	D_ASSERT(storage);

	auto ids = FlatVector::GetData<row_t>(row_ids);
	storage->row_groups->Update(TransactionData(0, 0), ids, column_ids, updates);
}

void LocalStorage::Flush(DataTable &table, LocalTableStorage &storage) {
	if (storage.row_groups->GetTotalRows() <= storage.deleted_rows) {
		return;
	}
	idx_t append_count = storage.row_groups->GetTotalRows() - storage.deleted_rows;

	TableAppendState append_state;
	table.AppendLock(append_state);
	transaction.PushAppend(table, append_state.row_start, append_count);
	if ((append_state.row_start == 0 || storage.row_groups->GetTotalRows() >= MERGE_THRESHOLD) &&
	    storage.deleted_rows == 0) {
		// table is currently empty OR we are bulk appending: move over the storage directly
		// first flush any outstanding blocks
		storage.FlushBlocks();
		// now append to the indexes (if there are any)
		// FIXME: we should be able to merge the transaction-local index directly into the main table index
		// as long we just rewrite some row-ids
		if (!table.info->indexes.Empty()) {
			storage.AppendToIndexes(transaction, append_state, append_count, false);
		}
		// finally move over the row groups
		table.MergeStorage(*storage.row_groups, storage.indexes);
	} else {
		// check if we have written data
		// if we have, we cannot merge to disk after all
		// so we need to revert the data we have already written
		storage.Rollback();
		// append to the indexes and append to the base table
		storage.AppendToIndexes(transaction, append_state, append_count, true);
	}

	// possibly vacuum any excess index data
	table.info->indexes.Scan([&](Index &index) {
		index.Vacuum();
		return false;
	});
}

void LocalStorage::Commit(LocalStorage::CommitState &commit_state, DuckTransaction &transaction) {
	// commit local storage
	// iterate over all entries in the table storage map and commit them
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto table = entry.first;
		auto storage = entry.second.get();
		Flush(table, *storage);
		entry.second.reset();
	}
}

void LocalStorage::Rollback() {
	// rollback local storage
	// after this, the local storage is no longer required and can be cleared
	auto table_storage = table_manager.MoveEntries();
	for (auto &entry : table_storage) {
		auto storage = entry.second.get();
		if (!storage) {
			continue;
		}
		storage->Rollback();

		entry.second.reset();
	}
}

idx_t LocalStorage::AddedRows(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		return 0;
	}
	return storage->row_groups->GetTotalRows() - storage->deleted_rows;
}

void LocalStorage::MoveStorage(DataTable &old_dt, DataTable &new_dt) {
	// check if there are any pending appends for the old version of the table
	auto new_storage = table_manager.MoveEntry(old_dt);
	if (!new_storage) {
		return;
	}
	// take over the storage from the old entry
	new_storage->table_ref = new_dt;
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::AddColumn(DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
                             Expression &default_value) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared<LocalTableStorage>(context, new_dt, *storage, new_column, default_value);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::DropColumn(DataTable &old_dt, DataTable &new_dt, idx_t removed_column) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage = make_shared<LocalTableStorage>(new_dt, *storage, removed_column);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::ChangeType(DataTable &old_dt, DataTable &new_dt, idx_t changed_idx, const LogicalType &target_type,
                              const vector<column_t> &bound_columns, Expression &cast_expr) {
	// check if there are any pending appends for the old version of the table
	auto storage = table_manager.MoveEntry(old_dt);
	if (!storage) {
		return;
	}
	auto new_storage =
	    make_shared<LocalTableStorage>(context, new_dt, *storage, changed_idx, target_type, bound_columns, cast_expr);
	table_manager.InsertEntry(new_dt, std::move(new_storage));
}

void LocalStorage::FetchChunk(DataTable &table, Vector &row_ids, idx_t count, const vector<column_t> &col_ids,
                              DataChunk &chunk, ColumnFetchState &fetch_state) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::FetchChunk - local storage not found");
	}

	storage->row_groups->Fetch(transaction, chunk, col_ids, row_ids, count, fetch_state);
}

TableIndexList &LocalStorage::GetIndexes(DataTable &table) {
	auto storage = table_manager.GetStorage(table);
	if (!storage) {
		throw InternalException("LocalStorage::GetIndexes - local storage not found");
	}
	return storage->indexes;
}

void LocalStorage::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	auto storage = table_manager.GetStorage(parent);
	if (!storage) {
		return;
	}
	storage->row_groups->VerifyNewConstraint(parent, constraint);
}

} // namespace duckdb




namespace duckdb {

DataFileType MagicBytes::CheckMagicBytes(FileSystem *fs_p, const string &path) {
	LocalFileSystem lfs;
	FileSystem &fs = fs_p ? *fs_p : lfs;
	if (!fs.FileExists(path)) {
		return DataFileType::FILE_DOES_NOT_EXIST;
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);

	constexpr const idx_t MAGIC_BYTES_READ_SIZE = 16;
	char buffer[MAGIC_BYTES_READ_SIZE];

	handle->Read(buffer, MAGIC_BYTES_READ_SIZE);
	if (memcmp(buffer, "SQLite format 3\0", 16) == 0) {
		return DataFileType::SQLITE_FILE;
	}
	if (memcmp(buffer, "PAR1", 4) == 0) {
		return DataFileType::PARQUET_FILE;
	}
	if (memcmp(buffer + MainHeader::MAGIC_BYTE_OFFSET, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) == 0) {
		return DataFileType::DUCKDB_FILE;
	}
	return DataFileType::FILE_DOES_NOT_EXIST;
}

} // namespace duckdb







namespace duckdb {

MetadataManager::MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager)
    : block_manager(block_manager), buffer_manager(buffer_manager) {
}

MetadataManager::~MetadataManager() {
}

MetadataHandle MetadataManager::AllocateHandle() {
	// check if there is any free space left in an existing block
	// if not allocate a new block
	block_id_t free_block = INVALID_BLOCK;
	for (auto &kv : blocks) {
		auto &block = kv.second;
		D_ASSERT(kv.first == block.block_id);
		if (!block.free_blocks.empty()) {
			free_block = kv.first;
			break;
		}
	}
	if (free_block == INVALID_BLOCK) {
		free_block = AllocateNewBlock();
	}
	D_ASSERT(free_block != INVALID_BLOCK);

	// select the first free metadata block we can find
	MetadataPointer pointer;
	pointer.block_index = free_block;
	auto &block = blocks[free_block];
	if (block.block->BlockId() < MAXIMUM_BLOCK) {
		// this block is a disk-backed block, yet we are planning to write to it
		// we need to convert it into a transient block before we can write to it
		ConvertToTransient(block);
		D_ASSERT(block.block->BlockId() >= MAXIMUM_BLOCK);
	}
	D_ASSERT(!block.free_blocks.empty());
	pointer.index = block.free_blocks.back();
	// mark the block as used
	block.free_blocks.pop_back();
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	// pin the block
	return Pin(pointer);
}

MetadataHandle MetadataManager::Pin(MetadataPointer pointer) {
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	auto &block = blocks[pointer.block_index];

	MetadataHandle handle;
	handle.pointer.block_index = pointer.block_index;
	handle.pointer.index = pointer.index;
	handle.handle = buffer_manager.Pin(block.block);
	return handle;
}

void MetadataManager::ConvertToTransient(MetadataBlock &block) {
	// pin the old block
	auto old_buffer = buffer_manager.Pin(block.block);

	// allocate a new transient block to replace it
	shared_ptr<BlockHandle> new_block;
	auto new_buffer = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block);

	// copy the data to the transient block
	memcpy(new_buffer.Ptr(), old_buffer.Ptr(), Storage::BLOCK_SIZE);

	block.block = std::move(new_block);

	// unregister the old block
	block_manager.UnregisterBlock(block.block_id, false);
}

block_id_t MetadataManager::AllocateNewBlock() {
	auto new_block_id = GetNextBlockId();

	MetadataBlock new_block;
	auto handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block.block);
	new_block.block_id = new_block_id;
	for (idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(METADATA_BLOCK_COUNT - i - 1);
	}
	// zero-initialize the handle
	memset(handle.Ptr(), 0, Storage::BLOCK_SIZE);
	AddBlock(std::move(new_block));
	return new_block_id;
}

void MetadataManager::AddBlock(MetadataBlock new_block, bool if_exists) {
	if (blocks.find(new_block.block_id) != blocks.end()) {
		if (if_exists) {
			return;
		}
		throw InternalException("Block id with id %llu already exists", new_block.block_id);
	}
	blocks[new_block.block_id] = std::move(new_block);
}

void MetadataManager::AddAndRegisterBlock(MetadataBlock block) {
	if (block.block) {
		throw InternalException("Calling AddAndRegisterBlock on block that already exists");
	}
	block.block = block_manager.RegisterBlock(block.block_id);
	AddBlock(std::move(block), true);
}

MetaBlockPointer MetadataManager::GetDiskPointer(MetadataPointer pointer, uint32_t offset) {
	idx_t block_pointer = idx_t(pointer.block_index);
	block_pointer |= idx_t(pointer.index) << 56ULL;
	return MetaBlockPointer(block_pointer, offset);
}

block_id_t MetaBlockPointer::GetBlockId() const {
	return block_id_t(block_pointer & ~(idx_t(0xFF) << 56ULL));
}

uint32_t MetaBlockPointer::GetBlockIndex() const {
	return block_pointer >> 56ULL;
}

MetadataPointer MetadataManager::FromDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	auto index = pointer.GetBlockIndex();
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) { // LCOV_EXCL_START
		throw InternalException("Failed to load metadata pointer (id %llu, idx %llu, ptr %llu)\n", block_id, index,
		                        pointer.block_pointer);
	} // LCOV_EXCL_STOP
	MetadataPointer result;
	result.block_index = block_id;
	result.index = index;
	return result;
}

MetadataPointer MetadataManager::RegisterDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	MetadataBlock block;
	block.block_id = block_id;
	AddAndRegisterBlock(block);
	return FromDiskPointer(pointer);
}

BlockPointer MetadataManager::ToBlockPointer(MetaBlockPointer meta_pointer) {
	BlockPointer result;
	result.block_id = meta_pointer.GetBlockId();
	result.offset = meta_pointer.GetBlockIndex() * MetadataManager::METADATA_BLOCK_SIZE + meta_pointer.offset;
	D_ASSERT(result.offset < MetadataManager::METADATA_BLOCK_SIZE * MetadataManager::METADATA_BLOCK_COUNT);
	return result;
}

MetaBlockPointer MetadataManager::FromBlockPointer(BlockPointer block_pointer) {
	if (!block_pointer.IsValid()) {
		return MetaBlockPointer();
	}
	idx_t index = block_pointer.offset / MetadataManager::METADATA_BLOCK_SIZE;
	auto offset = block_pointer.offset % MetadataManager::METADATA_BLOCK_SIZE;
	D_ASSERT(index < MetadataManager::METADATA_BLOCK_COUNT);
	D_ASSERT(offset < MetadataManager::METADATA_BLOCK_SIZE);
	MetaBlockPointer result;
	result.block_pointer = idx_t(block_pointer.block_id) | index << 56ULL;
	result.offset = offset;
	return result;
}

idx_t MetadataManager::BlockCount() {
	return blocks.size();
}

void MetadataManager::Flush() {
	const idx_t total_metadata_size = MetadataManager::METADATA_BLOCK_SIZE * MetadataManager::METADATA_BLOCK_COUNT;
	// write the blocks of the metadata manager to disk
	for (auto &kv : blocks) {
		auto &block = kv.second;
		auto handle = buffer_manager.Pin(block.block);
		// there are a few bytes left-over at the end of the block, zero-initialize them
		memset(handle.Ptr() + total_metadata_size, 0, Storage::BLOCK_SIZE - total_metadata_size);
		D_ASSERT(kv.first == block.block_id);
		if (block.block->BlockId() >= MAXIMUM_BLOCK) {
			// temporary block - convert to persistent
			block.block = block_manager.ConvertToPersistent(kv.first, std::move(block.block));
		} else {
			// already a persistent block - only need to write it
			D_ASSERT(block.block->BlockId() == block.block_id);
			block_manager.Write(handle.GetFileBuffer(), block.block_id);
		}
	}
}

void MetadataManager::Write(WriteStream &sink) {
	sink.Write<uint64_t>(blocks.size());
	for (auto &kv : blocks) {
		kv.second.Write(sink);
	}
}

void MetadataManager::Read(ReadStream &source) {
	auto block_count = source.Read<uint64_t>();
	for (idx_t i = 0; i < block_count; i++) {
		auto block = MetadataBlock::Read(source);
		auto entry = blocks.find(block.block_id);
		if (entry == blocks.end()) {
			// block does not exist yet
			AddAndRegisterBlock(std::move(block));
		} else {
			// block was already created - only copy over the free list
			entry->second.free_blocks = std::move(block.free_blocks);
		}
	}
}

void MetadataBlock::Write(WriteStream &sink) {
	sink.Write<block_id_t>(block_id);
	sink.Write<idx_t>(FreeBlocksToInteger());
}

MetadataBlock MetadataBlock::Read(ReadStream &source) {
	MetadataBlock result;
	result.block_id = source.Read<block_id_t>();
	auto free_list = source.Read<idx_t>();
	result.FreeBlocksFromInteger(free_list);
	return result;
}

idx_t MetadataBlock::FreeBlocksToInteger() {
	idx_t result = 0;
	for (idx_t i = 0; i < free_blocks.size(); i++) {
		D_ASSERT(free_blocks[i] < idx_t(64));
		idx_t mask = idx_t(1) << idx_t(free_blocks[i]);
		result |= mask;
	}
	return result;
}

void MetadataBlock::FreeBlocksFromInteger(idx_t free_list) {
	free_blocks.clear();
	if (free_list == 0) {
		return;
	}
	for (idx_t i = 64; i > 0; i--) {
		auto index = i - 1;
		idx_t mask = idx_t(1) << index;
		if (free_list & mask) {
			free_blocks.push_back(index);
		}
	}
}

void MetadataManager::MarkBlocksAsModified() {
	// for any blocks that were modified in the last checkpoint - set them to free blocks currently
	for (auto &kv : modified_blocks) {
		auto block_id = kv.first;
		idx_t modified_list = kv.second;
		auto entry = blocks.find(block_id);
		D_ASSERT(entry != blocks.end());
		auto &block = entry->second;
		idx_t current_free_blocks = block.FreeBlocksToInteger();
		// merge the current set of free blocks with the modified blocks
		idx_t new_free_blocks = current_free_blocks | modified_list;
		if (new_free_blocks == NumericLimits<idx_t>::Maximum()) {
			// if new free_blocks is all blocks - mark entire block as modified
			blocks.erase(entry);
			block_manager.MarkBlockAsModified(block_id);
		} else {
			// set the new set of free blocks
			block.FreeBlocksFromInteger(new_free_blocks);
		}
	}

	modified_blocks.clear();
	for (auto &kv : blocks) {
		auto &block = kv.second;
		idx_t free_list = block.FreeBlocksToInteger();
		idx_t occupied_list = ~free_list;
		modified_blocks[block.block_id] = occupied_list;
	}
}

void MetadataManager::ClearModifiedBlocks(const vector<MetaBlockPointer> &pointers) {
	for (auto &pointer : pointers) {
		auto block_id = pointer.GetBlockId();
		auto block_index = pointer.GetBlockIndex();
		auto entry = modified_blocks.find(block_id);
		if (entry == modified_blocks.end()) {
			throw InternalException("ClearModifiedBlocks - Block id %llu not found in modified_blocks", block_id);
		}
		auto &modified_list = entry->second;
		// verify the block has been modified
		D_ASSERT(modified_list && (1ULL << block_index));
		// unset the bit
		modified_list &= ~(1ULL << block_index);
	}
}

vector<MetadataBlockInfo> MetadataManager::GetMetadataInfo() const {
	vector<MetadataBlockInfo> result;
	for (auto &block : blocks) {
		MetadataBlockInfo block_info;
		block_info.block_id = block.second.block_id;
		block_info.total_blocks = MetadataManager::METADATA_BLOCK_COUNT;
		for (auto free_block : block.second.free_blocks) {
			block_info.free_list.push_back(free_block);
		}
		std::sort(block_info.free_list.begin(), block_info.free_list.end());
		result.push_back(std::move(block_info));
	}
	std::sort(result.begin(), result.end(),
	          [](const MetadataBlockInfo &a, const MetadataBlockInfo &b) { return a.block_id < b.block_id; });
	return result;
}

block_id_t MetadataManager::GetNextBlockId() {
	return block_manager.GetFreeBlockId();
}

} // namespace duckdb


namespace duckdb {

MetadataReader::MetadataReader(MetadataManager &manager, MetaBlockPointer pointer,
                               optional_ptr<vector<MetaBlockPointer>> read_pointers_p, BlockReaderType type)
    : manager(manager), type(type), next_pointer(FromDiskPointer(pointer)), has_next_block(true),
      read_pointers(read_pointers_p), index(0), offset(0), next_offset(pointer.offset), capacity(0) {
	if (read_pointers) {
		D_ASSERT(read_pointers->empty());
		read_pointers->push_back(pointer);
	}
}

MetadataReader::MetadataReader(MetadataManager &manager, BlockPointer pointer)
    : MetadataReader(manager, MetadataManager::FromBlockPointer(pointer)) {
}

MetadataPointer MetadataReader::FromDiskPointer(MetaBlockPointer pointer) {
	if (type == BlockReaderType::EXISTING_BLOCKS) {
		return manager.FromDiskPointer(pointer);
	} else {
		return manager.RegisterDiskPointer(pointer);
	}
}

MetadataReader::~MetadataReader() {
}

void MetadataReader::ReadData(data_ptr_t buffer, idx_t read_size) {
	while (offset + read_size > capacity) {
		// cannot read entire entry from block
		// first read what we can from this block
		idx_t to_read = capacity - offset;
		if (to_read > 0) {
			memcpy(buffer, Ptr(), to_read);
			read_size -= to_read;
			buffer += to_read;
			offset += read_size;
		}
		// then move to the next block
		ReadNextBlock();
	}
	// we have enough left in this block to read from the buffer
	memcpy(buffer, Ptr(), read_size);
	offset += read_size;
}

MetaBlockPointer MetadataReader::GetMetaBlockPointer() {
	return manager.GetDiskPointer(block.pointer, offset);
}

void MetadataReader::ReadNextBlock() {
	if (!has_next_block) {
		throw IOException("No more data remaining in MetadataReader");
	}
	block = manager.Pin(next_pointer);
	index = next_pointer.index;

	idx_t next_block = Load<idx_t>(BasePtr());
	if (next_block == idx_t(-1)) {
		has_next_block = false;
	} else {
		next_pointer = FromDiskPointer(MetaBlockPointer(next_block, 0));
		MetaBlockPointer next_block_pointer(next_block, 0);
		if (read_pointers) {
			read_pointers->push_back(next_block_pointer);
		}
	}
	if (next_offset < sizeof(block_id_t)) {
		next_offset = sizeof(block_id_t);
	}
	if (next_offset > MetadataManager::METADATA_BLOCK_SIZE) {
		throw InternalException("next_offset cannot be bigger than block size");
	}
	offset = next_offset;
	next_offset = sizeof(block_id_t);
	capacity = MetadataManager::METADATA_BLOCK_SIZE;
}

data_ptr_t MetadataReader::BasePtr() {
	return block.handle.Ptr() + index * MetadataManager::METADATA_BLOCK_SIZE;
}

data_ptr_t MetadataReader::Ptr() {
	return BasePtr() + offset;
}

} // namespace duckdb



namespace duckdb {

MetadataWriter::MetadataWriter(MetadataManager &manager, optional_ptr<vector<MetaBlockPointer>> written_pointers_p)
    : manager(manager), written_pointers(written_pointers_p), capacity(0), offset(0) {
	D_ASSERT(!written_pointers || written_pointers->empty());
}

MetadataWriter::~MetadataWriter() {
	// If there's an exception during checkpoint, this can get destroyed without
	// flushing the data...which is fine, because none of the unwritten data
	// will be referenced.
	//
	// Otherwise, we should have explicitly flushed (and thereby nulled the block).
	D_ASSERT(!block.handle.IsValid() || Exception::UncaughtException());
}

BlockPointer MetadataWriter::GetBlockPointer() {
	return MetadataManager::ToBlockPointer(GetMetaBlockPointer());
}

MetaBlockPointer MetadataWriter::GetMetaBlockPointer() {
	if (offset >= capacity) {
		// at the end of the block - fetch the next block
		NextBlock();
		D_ASSERT(capacity > 0);
	}
	return manager.GetDiskPointer(block.pointer, offset);
}

MetadataHandle MetadataWriter::NextHandle() {
	return manager.AllocateHandle();
}

void MetadataWriter::NextBlock() {
	// now we need to get a new block id
	auto new_handle = NextHandle();

	// write the block id of the new block to the start of the current block
	if (capacity > 0) {
		auto disk_block = manager.GetDiskPointer(new_handle.pointer);
		Store<idx_t>(disk_block.block_pointer, BasePtr());
	}
	// now update the block id of the block
	block = std::move(new_handle);
	current_pointer = block.pointer;
	offset = sizeof(idx_t);
	capacity = MetadataManager::METADATA_BLOCK_SIZE;
	Store<idx_t>(-1, BasePtr());
	if (written_pointers) {
		written_pointers->push_back(manager.GetDiskPointer(current_pointer));
	}
}

void MetadataWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	while (offset + write_size > capacity) {
		// we need to make a new block
		// first copy what we can
		D_ASSERT(offset <= capacity);
		idx_t copy_amount = capacity - offset;
		if (copy_amount > 0) {
			memcpy(Ptr(), buffer, copy_amount);
			buffer += copy_amount;
			offset += copy_amount;
			write_size -= copy_amount;
		}
		// move forward to the next block
		NextBlock();
	}
	memcpy(Ptr(), buffer, write_size);
	offset += write_size;
}

void MetadataWriter::Flush() {
	if (offset < capacity) {
		// clear remaining bytes of block (if any)
		memset(Ptr(), 0, capacity - offset);
	}
	block.handle.Destroy();
}

data_ptr_t MetadataWriter::BasePtr() {
	return block.handle.Ptr() + current_pointer.index * MetadataManager::METADATA_BLOCK_SIZE;
}

data_ptr_t MetadataWriter::Ptr() {
	return BasePtr() + offset;
}

} // namespace duckdb





namespace duckdb {

OptimisticDataWriter::OptimisticDataWriter(DataTable &table) : table(table) {
}

OptimisticDataWriter::OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent) : table(table) {
	if (parent.partial_manager) {
		parent.partial_manager->ClearBlocks();
	}
}

OptimisticDataWriter::~OptimisticDataWriter() {
}

bool OptimisticDataWriter::PrepareWrite() {
	// check if we should pre-emptively write the table to disk
	if (table.info->IsTemporary() || StorageManager::Get(table.info->db).InMemory()) {
		return false;
	}
	// we should! write the second-to-last row group to disk
	// allocate the partial block-manager if none is allocated yet
	if (!partial_manager) {
		auto &block_manager = table.info->table_io_manager->GetBlockManagerForRowData();
		partial_manager = make_uniq<PartialBlockManager>(block_manager, CheckpointType::APPEND_TO_TABLE);
	}
	return true;
}

void OptimisticDataWriter::WriteNewRowGroup(RowGroupCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.GetRowGroup(-2);
	FlushToDisk(row_group);
}

void OptimisticDataWriter::WriteLastRowGroup(RowGroupCollection &row_groups) {
	// we finished writing a complete row group
	if (!PrepareWrite()) {
		return;
	}
	// flush second-to-last row group
	auto row_group = row_groups.GetRowGroup(-1);
	if (!row_group) {
		return;
	}
	FlushToDisk(row_group);
}

void OptimisticDataWriter::FlushToDisk(RowGroup *row_group) {
	if (!row_group) {
		throw InternalException("FlushToDisk called without a RowGroup");
	}
	//! The set of column compression types (if any)
	vector<CompressionType> compression_types;
	D_ASSERT(compression_types.empty());
	for (auto &column : table.column_definitions) {
		compression_types.push_back(column.CompressionType());
	}
	row_group->WriteToDisk(*partial_manager, compression_types);
}

void OptimisticDataWriter::Merge(OptimisticDataWriter &other) {
	if (!other.partial_manager) {
		return;
	}
	if (!partial_manager) {
		partial_manager = std::move(other.partial_manager);
		return;
	}
	partial_manager->Merge(*other.partial_manager);
	other.partial_manager.reset();
}

void OptimisticDataWriter::FinalFlush() {
	if (partial_manager) {
		partial_manager->FlushPartialBlocks();
		partial_manager.reset();
	}
}

void OptimisticDataWriter::Rollback() {
	if (partial_manager) {
		partial_manager->Rollback();
		partial_manager.reset();
	}
}

} // namespace duckdb


namespace duckdb {

//===--------------------------------------------------------------------===//
// PartialBlock
//===--------------------------------------------------------------------===//

PartialBlock::PartialBlock(PartialBlockState state, BlockManager &block_manager,
                           const shared_ptr<BlockHandle> &block_handle)
    : state(state), block_manager(block_manager), block_handle(block_handle) {
}

void PartialBlock::AddUninitializedRegion(idx_t start, idx_t end) {
	uninitialized_regions.push_back({start, end});
}

void PartialBlock::FlushInternal(const idx_t free_space_left) {

	// ensure that we do not leak any data
	if (free_space_left > 0 || !uninitialized_regions.empty()) {
		auto buffer_handle = block_manager.buffer_manager.Pin(block_handle);

		// memset any uninitialized regions
		for (auto &uninitialized : uninitialized_regions) {
			memset(buffer_handle.Ptr() + uninitialized.start, 0, uninitialized.end - uninitialized.start);
		}
		// memset any free space at the end of the block to 0 prior to writing to disk
		memset(buffer_handle.Ptr() + Storage::BLOCK_SIZE - free_space_left, 0, free_space_left);
	}
}

//===--------------------------------------------------------------------===//
// PartialBlockManager
//===--------------------------------------------------------------------===//

PartialBlockManager::PartialBlockManager(BlockManager &block_manager, CheckpointType checkpoint_type,
                                         uint32_t max_partial_block_size, uint32_t max_use_count)
    : block_manager(block_manager), checkpoint_type(checkpoint_type), max_partial_block_size(max_partial_block_size),
      max_use_count(max_use_count) {
}
PartialBlockManager::~PartialBlockManager() {
}

PartialBlockAllocation PartialBlockManager::GetBlockAllocation(uint32_t segment_size) {
	PartialBlockAllocation allocation;
	allocation.block_manager = &block_manager;
	allocation.allocation_size = segment_size;

	// if the block is less than 80% full, we consider it a "partial block"
	// which means we will try to fit it with other blocks
	// check if there is a partial block available we can write to
	if (segment_size <= max_partial_block_size && GetPartialBlock(segment_size, allocation.partial_block)) {
		//! there is! increase the reference count of this block
		allocation.partial_block->state.block_use_count += 1;
		allocation.state = allocation.partial_block->state;
		if (checkpoint_type == CheckpointType::FULL_CHECKPOINT) {
			block_manager.IncreaseBlockReferenceCount(allocation.state.block_id);
		}
	} else {
		// full block: get a free block to write to
		AllocateBlock(allocation.state, segment_size);
	}
	return allocation;
}

bool PartialBlockManager::HasBlockAllocation(uint32_t segment_size) {
	return segment_size <= max_partial_block_size &&
	       partially_filled_blocks.lower_bound(segment_size) != partially_filled_blocks.end();
}

void PartialBlockManager::AllocateBlock(PartialBlockState &state, uint32_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	if (checkpoint_type == CheckpointType::FULL_CHECKPOINT) {
		state.block_id = block_manager.GetFreeBlockId();
	} else {
		state.block_id = INVALID_BLOCK;
	}
	state.block_size = Storage::BLOCK_SIZE;
	state.offset = 0;
	state.block_use_count = 1;
}

bool PartialBlockManager::GetPartialBlock(idx_t segment_size, unique_ptr<PartialBlock> &partial_block) {
	auto entry = partially_filled_blocks.lower_bound(segment_size);
	if (entry == partially_filled_blocks.end()) {
		return false;
	}
	// found a partially filled block! fill in the info
	partial_block = std::move(entry->second);
	partially_filled_blocks.erase(entry);

	D_ASSERT(partial_block->state.offset > 0);
	D_ASSERT(ValueIsAligned(partial_block->state.offset));
	return true;
}

void PartialBlockManager::RegisterPartialBlock(PartialBlockAllocation &&allocation) {
	auto &state = allocation.partial_block->state;
	D_ASSERT(checkpoint_type != CheckpointType::FULL_CHECKPOINT || state.block_id >= 0);
	if (state.block_use_count < max_use_count) {
		auto unaligned_size = allocation.allocation_size + state.offset;
		auto new_size = AlignValue(unaligned_size);
		if (new_size != unaligned_size) {
			// register the uninitialized region so we can correctly initialize it before writing to disk
			allocation.partial_block->AddUninitializedRegion(unaligned_size, new_size);
		}
		state.offset = new_size;
		auto new_space_left = state.block_size - new_size;
		// check if the block is STILL partially filled after adding the segment_size
		if (new_space_left >= Storage::BLOCK_SIZE - max_partial_block_size) {
			// the block is still partially filled: add it to the partially_filled_blocks list
			partially_filled_blocks.insert(make_pair(new_space_left, std::move(allocation.partial_block)));
		}
	}
	idx_t free_space = state.block_size - state.offset;
	auto block_to_free = std::move(allocation.partial_block);
	if (!block_to_free && partially_filled_blocks.size() > MAX_BLOCK_MAP_SIZE) {
		// Free the page with the least space free.
		auto itr = partially_filled_blocks.begin();
		block_to_free = std::move(itr->second);
		free_space = state.block_size - itr->first;
		partially_filled_blocks.erase(itr);
	}
	// Flush any block that we're not going to reuse.
	if (block_to_free) {
		block_to_free->Flush(free_space);
		AddWrittenBlock(block_to_free->state.block_id);
	}
}

void PartialBlockManager::Merge(PartialBlockManager &other) {
	if (&other == this) {
		throw InternalException("Cannot merge into itself");
	}
	// for each partially filled block in the other manager, check if we can merge it into an existing block in this
	// manager
	for (auto &e : other.partially_filled_blocks) {
		if (!e.second) {
			throw InternalException("Empty partially filled block found");
		}
		auto used_space = Storage::BLOCK_SIZE - e.first;
		if (HasBlockAllocation(used_space)) {
			// we can merge this block into an existing block - merge them
			// merge blocks
			auto allocation = GetBlockAllocation(used_space);
			allocation.partial_block->Merge(*e.second, allocation.state.offset, used_space);

			// re-register the partial block
			allocation.state.offset += used_space;
			RegisterPartialBlock(std::move(allocation));
		} else {
			// we cannot merge this block - append it directly to the current block manager
			partially_filled_blocks.insert(make_pair(e.first, std::move(e.second)));
		}
	}
	// copy over the written blocks
	for (auto &block_id : other.written_blocks) {
		AddWrittenBlock(block_id);
	}
	other.written_blocks.clear();
	other.partially_filled_blocks.clear();
}

void PartialBlockManager::AddWrittenBlock(block_id_t block) {
	auto entry = written_blocks.insert(block);
	if (!entry.second) {
		throw InternalException("Written block already exists");
	}
}

void PartialBlockManager::ClearBlocks() {
	for (auto &e : partially_filled_blocks) {
		e.second->Clear();
	}
	partially_filled_blocks.clear();
}

void PartialBlockManager::FlushPartialBlocks() {
	for (auto &e : partially_filled_blocks) {
		e.second->Flush(e.first);
	}
	partially_filled_blocks.clear();
}

void PartialBlockManager::Rollback() {
	ClearBlocks();
	for (auto &block_id : written_blocks) {
		block_manager.MarkBlockAsFree(block_id);
	}
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void Constraint::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ConstraintType>(100, "type", type);
}

unique_ptr<Constraint> Constraint::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<ConstraintType>(100, "type");
	unique_ptr<Constraint> result;
	switch (type) {
	case ConstraintType::CHECK:
		result = CheckConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::FOREIGN_KEY:
		result = ForeignKeyConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::NOT_NULL:
		result = NotNullConstraint::Deserialize(deserializer);
		break;
	case ConstraintType::UNIQUE:
		result = UniqueConstraint::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of Constraint!");
	}
	return result;
}

void CheckConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression", expression);
}

unique_ptr<Constraint> CheckConstraint::Deserialize(Deserializer &deserializer) {
	auto expression = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression");
	auto result = duckdb::unique_ptr<CheckConstraint>(new CheckConstraint(std::move(expression)));
	return std::move(result);
}

void ForeignKeyConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<string>>(200, "pk_columns", pk_columns);
	serializer.WritePropertyWithDefault<vector<string>>(201, "fk_columns", fk_columns);
	serializer.WriteProperty<ForeignKeyType>(202, "fk_type", info.type);
	serializer.WritePropertyWithDefault<string>(203, "schema", info.schema);
	serializer.WritePropertyWithDefault<string>(204, "table", info.table);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(205, "pk_keys", info.pk_keys);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(206, "fk_keys", info.fk_keys);
}

unique_ptr<Constraint> ForeignKeyConstraint::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ForeignKeyConstraint>(new ForeignKeyConstraint());
	deserializer.ReadPropertyWithDefault<vector<string>>(200, "pk_columns", result->pk_columns);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "fk_columns", result->fk_columns);
	deserializer.ReadProperty<ForeignKeyType>(202, "fk_type", result->info.type);
	deserializer.ReadPropertyWithDefault<string>(203, "schema", result->info.schema);
	deserializer.ReadPropertyWithDefault<string>(204, "table", result->info.table);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(205, "pk_keys", result->info.pk_keys);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(206, "fk_keys", result->info.fk_keys);
	return std::move(result);
}

void NotNullConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WriteProperty<LogicalIndex>(200, "index", index);
}

unique_ptr<Constraint> NotNullConstraint::Deserialize(Deserializer &deserializer) {
	auto index = deserializer.ReadProperty<LogicalIndex>(200, "index");
	auto result = duckdb::unique_ptr<NotNullConstraint>(new NotNullConstraint(index));
	return std::move(result);
}

void UniqueConstraint::Serialize(Serializer &serializer) const {
	Constraint::Serialize(serializer);
	serializer.WritePropertyWithDefault<bool>(200, "is_primary_key", is_primary_key);
	serializer.WriteProperty<LogicalIndex>(201, "index", index);
	serializer.WritePropertyWithDefault<vector<string>>(202, "columns", columns);
}

unique_ptr<Constraint> UniqueConstraint::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<UniqueConstraint>(new UniqueConstraint());
	deserializer.ReadPropertyWithDefault<bool>(200, "is_primary_key", result->is_primary_key);
	deserializer.ReadProperty<LogicalIndex>(201, "index", result->index);
	deserializer.ReadPropertyWithDefault<vector<string>>(202, "columns", result->columns);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//












namespace duckdb {

void CreateInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<CatalogType>(100, "type", type);
	serializer.WritePropertyWithDefault<string>(101, "catalog", catalog);
	serializer.WritePropertyWithDefault<string>(102, "schema", schema);
	serializer.WritePropertyWithDefault<bool>(103, "temporary", temporary);
	serializer.WritePropertyWithDefault<bool>(104, "internal", internal);
	serializer.WriteProperty<OnCreateConflict>(105, "on_conflict", on_conflict);
	serializer.WritePropertyWithDefault<string>(106, "sql", sql);
}

unique_ptr<CreateInfo> CreateInfo::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<CatalogType>(100, "type");
	auto catalog = deserializer.ReadPropertyWithDefault<string>(101, "catalog");
	auto schema = deserializer.ReadPropertyWithDefault<string>(102, "schema");
	auto temporary = deserializer.ReadPropertyWithDefault<bool>(103, "temporary");
	auto internal = deserializer.ReadPropertyWithDefault<bool>(104, "internal");
	auto on_conflict = deserializer.ReadProperty<OnCreateConflict>(105, "on_conflict");
	auto sql = deserializer.ReadPropertyWithDefault<string>(106, "sql");
	deserializer.Set<CatalogType>(type);
	unique_ptr<CreateInfo> result;
	switch (type) {
	case CatalogType::INDEX_ENTRY:
		result = CreateIndexInfo::Deserialize(deserializer);
		break;
	case CatalogType::MACRO_ENTRY:
		result = CreateMacroInfo::Deserialize(deserializer);
		break;
	case CatalogType::SCHEMA_ENTRY:
		result = CreateSchemaInfo::Deserialize(deserializer);
		break;
	case CatalogType::SEQUENCE_ENTRY:
		result = CreateSequenceInfo::Deserialize(deserializer);
		break;
	case CatalogType::TABLE_ENTRY:
		result = CreateTableInfo::Deserialize(deserializer);
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		result = CreateMacroInfo::Deserialize(deserializer);
		break;
	case CatalogType::TYPE_ENTRY:
		result = CreateTypeInfo::Deserialize(deserializer);
		break;
	case CatalogType::VIEW_ENTRY:
		result = CreateViewInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of CreateInfo!");
	}
	deserializer.Unset<CatalogType>();
	result->catalog = std::move(catalog);
	result->schema = std::move(schema);
	result->temporary = temporary;
	result->internal = internal;
	result->on_conflict = on_conflict;
	result->sql = std::move(sql);
	return result;
}

void CreateIndexInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", index_name);
	serializer.WritePropertyWithDefault<string>(201, "table", table);
	serializer.WriteProperty<IndexType>(202, "index_type", index_type);
	serializer.WriteProperty<IndexConstraintType>(203, "constraint_type", constraint_type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "parsed_expressions", parsed_expressions);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(205, "scan_types", scan_types);
	serializer.WritePropertyWithDefault<vector<string>>(206, "names", names);
	serializer.WritePropertyWithDefault<vector<column_t>>(207, "column_ids", column_ids);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<Value>>(208, "options", options);
	serializer.WritePropertyWithDefault<string>(209, "index_type_name", index_type_name);
}

unique_ptr<CreateInfo> CreateIndexInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateIndexInfo>(new CreateIndexInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->index_name);
	deserializer.ReadPropertyWithDefault<string>(201, "table", result->table);
	deserializer.ReadProperty<IndexType>(202, "index_type", result->index_type);
	deserializer.ReadProperty<IndexConstraintType>(203, "constraint_type", result->constraint_type);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "parsed_expressions", result->parsed_expressions);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(205, "scan_types", result->scan_types);
	deserializer.ReadPropertyWithDefault<vector<string>>(206, "names", result->names);
	deserializer.ReadPropertyWithDefault<vector<column_t>>(207, "column_ids", result->column_ids);
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<Value>>(208, "options", result->options);
	deserializer.ReadPropertyWithDefault<string>(209, "index_type_name", result->index_type_name);
	return std::move(result);
}

void CreateMacroInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WritePropertyWithDefault<unique_ptr<MacroFunction>>(201, "function", function);
}

unique_ptr<CreateInfo> CreateMacroInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateMacroInfo>(new CreateMacroInfo(deserializer.Get<CatalogType>()));
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadPropertyWithDefault<unique_ptr<MacroFunction>>(201, "function", result->function);
	return std::move(result);
}

void CreateSchemaInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
}

unique_ptr<CreateInfo> CreateSchemaInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateSchemaInfo>(new CreateSchemaInfo());
	return std::move(result);
}

void CreateSequenceInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WritePropertyWithDefault<uint64_t>(201, "usage_count", usage_count);
	serializer.WritePropertyWithDefault<int64_t>(202, "increment", increment);
	serializer.WritePropertyWithDefault<int64_t>(203, "min_value", min_value);
	serializer.WritePropertyWithDefault<int64_t>(204, "max_value", max_value);
	serializer.WritePropertyWithDefault<int64_t>(205, "start_value", start_value);
	serializer.WritePropertyWithDefault<bool>(206, "cycle", cycle);
}

unique_ptr<CreateInfo> CreateSequenceInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateSequenceInfo>(new CreateSequenceInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadPropertyWithDefault<uint64_t>(201, "usage_count", result->usage_count);
	deserializer.ReadPropertyWithDefault<int64_t>(202, "increment", result->increment);
	deserializer.ReadPropertyWithDefault<int64_t>(203, "min_value", result->min_value);
	deserializer.ReadPropertyWithDefault<int64_t>(204, "max_value", result->max_value);
	deserializer.ReadPropertyWithDefault<int64_t>(205, "start_value", result->start_value);
	deserializer.ReadPropertyWithDefault<bool>(206, "cycle", result->cycle);
	return std::move(result);
}

void CreateTableInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "table", table);
	serializer.WriteProperty<ColumnList>(201, "columns", columns);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Constraint>>>(202, "constraints", constraints);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", query);
}

unique_ptr<CreateInfo> CreateTableInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateTableInfo>(new CreateTableInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "table", result->table);
	deserializer.ReadProperty<ColumnList>(201, "columns", result->columns);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Constraint>>>(202, "constraints", result->constraints);
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", result->query);
	return std::move(result);
}

void CreateTypeInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WriteProperty<LogicalType>(201, "logical_type", type);
}

unique_ptr<CreateInfo> CreateTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateTypeInfo>(new CreateTypeInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadProperty<LogicalType>(201, "logical_type", result->type);
	return std::move(result);
}

void CreateViewInfo::Serialize(Serializer &serializer) const {
	CreateInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "view_name", view_name);
	serializer.WritePropertyWithDefault<vector<string>>(201, "aliases", aliases);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(202, "types", types);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", query);
}

unique_ptr<CreateInfo> CreateViewInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CreateViewInfo>(new CreateViewInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "view_name", result->view_name);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "aliases", result->aliases);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(202, "types", result->types);
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(203, "query", result->query);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void Expression::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ExpressionClass>(100, "expression_class", expression_class);
	serializer.WriteProperty<ExpressionType>(101, "type", type);
	serializer.WritePropertyWithDefault<string>(102, "alias", alias);
}

unique_ptr<Expression> Expression::Deserialize(Deserializer &deserializer) {
	auto expression_class = deserializer.ReadProperty<ExpressionClass>(100, "expression_class");
	auto type = deserializer.ReadProperty<ExpressionType>(101, "type");
	auto alias = deserializer.ReadPropertyWithDefault<string>(102, "alias");
	deserializer.Set<ExpressionType>(type);
	unique_ptr<Expression> result;
	switch (expression_class) {
	case ExpressionClass::BOUND_AGGREGATE:
		result = BoundAggregateExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_BETWEEN:
		result = BoundBetweenExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_CASE:
		result = BoundCaseExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_CAST:
		result = BoundCastExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = BoundColumnRefExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = BoundComparisonExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = BoundConjunctionExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = BoundConstantExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = BoundDefaultExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = BoundFunctionExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_LAMBDA:
		result = BoundLambdaExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_LAMBDA_REF:
		result = BoundLambdaRefExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = BoundOperatorExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = BoundParameterExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_REF:
		result = BoundReferenceExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_UNNEST:
		result = BoundUnnestExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::BOUND_WINDOW:
		result = BoundWindowExpression::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of Expression!");
	}
	deserializer.Unset<ExpressionType>();
	result->alias = std::move(alias);
	return result;
}

void BoundBetweenExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "input", input);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(201, "lower", lower);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(202, "upper", upper);
	serializer.WritePropertyWithDefault<bool>(203, "lower_inclusive", lower_inclusive);
	serializer.WritePropertyWithDefault<bool>(204, "upper_inclusive", upper_inclusive);
}

unique_ptr<Expression> BoundBetweenExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<BoundBetweenExpression>(new BoundBetweenExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "input", result->input);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(201, "lower", result->lower);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(202, "upper", result->upper);
	deserializer.ReadPropertyWithDefault<bool>(203, "lower_inclusive", result->lower_inclusive);
	deserializer.ReadPropertyWithDefault<bool>(204, "upper_inclusive", result->upper_inclusive);
	return std::move(result);
}

void BoundCaseExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WritePropertyWithDefault<vector<BoundCaseCheck>>(201, "case_checks", case_checks);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(202, "else_expr", else_expr);
}

unique_ptr<Expression> BoundCaseExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto result = duckdb::unique_ptr<BoundCaseExpression>(new BoundCaseExpression(std::move(return_type)));
	deserializer.ReadPropertyWithDefault<vector<BoundCaseCheck>>(201, "case_checks", result->case_checks);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(202, "else_expr", result->else_expr);
	return std::move(result);
}

void BoundCastExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "child", child);
	serializer.WriteProperty<LogicalType>(201, "return_type", return_type);
	serializer.WritePropertyWithDefault<bool>(202, "try_cast", try_cast);
}

unique_ptr<Expression> BoundCastExpression::Deserialize(Deserializer &deserializer) {
	auto child = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "child");
	auto return_type = deserializer.ReadProperty<LogicalType>(201, "return_type");
	auto result = duckdb::unique_ptr<BoundCastExpression>(new BoundCastExpression(deserializer.Get<ClientContext &>(), std::move(child), std::move(return_type)));
	deserializer.ReadPropertyWithDefault<bool>(202, "try_cast", result->try_cast);
	return std::move(result);
}

void BoundColumnRefExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WriteProperty<ColumnBinding>(201, "binding", binding);
	serializer.WritePropertyWithDefault<idx_t>(202, "depth", depth);
}

unique_ptr<Expression> BoundColumnRefExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto binding = deserializer.ReadProperty<ColumnBinding>(201, "binding");
	auto depth = deserializer.ReadPropertyWithDefault<idx_t>(202, "depth");
	auto result = duckdb::unique_ptr<BoundColumnRefExpression>(new BoundColumnRefExpression(std::move(return_type), binding, depth));
	return std::move(result);
}

void BoundComparisonExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(201, "right", right);
}

unique_ptr<Expression> BoundComparisonExpression::Deserialize(Deserializer &deserializer) {
	auto left = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "left");
	auto right = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(201, "right");
	auto result = duckdb::unique_ptr<BoundComparisonExpression>(new BoundComparisonExpression(deserializer.Get<ExpressionType>(), std::move(left), std::move(right)));
	return std::move(result);
}

void BoundConjunctionExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(200, "children", children);
}

unique_ptr<Expression> BoundConjunctionExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<BoundConjunctionExpression>(new BoundConjunctionExpression(deserializer.Get<ExpressionType>()));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(200, "children", result->children);
	return std::move(result);
}

void BoundConstantExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<Value>(200, "value", value);
}

unique_ptr<Expression> BoundConstantExpression::Deserialize(Deserializer &deserializer) {
	auto value = deserializer.ReadProperty<Value>(200, "value");
	auto result = duckdb::unique_ptr<BoundConstantExpression>(new BoundConstantExpression(value));
	return std::move(result);
}

void BoundDefaultExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
}

unique_ptr<Expression> BoundDefaultExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto result = duckdb::unique_ptr<BoundDefaultExpression>(new BoundDefaultExpression(std::move(return_type)));
	return std::move(result);
}

void BoundLambdaExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(201, "lambda_expr", lambda_expr);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(202, "captures", captures);
	serializer.WritePropertyWithDefault<idx_t>(203, "parameter_count", parameter_count);
}

unique_ptr<Expression> BoundLambdaExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto lambda_expr = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(201, "lambda_expr");
	auto captures = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(202, "captures");
	auto parameter_count = deserializer.ReadPropertyWithDefault<idx_t>(203, "parameter_count");
	auto result = duckdb::unique_ptr<BoundLambdaExpression>(new BoundLambdaExpression(deserializer.Get<ExpressionType>(), std::move(return_type), std::move(lambda_expr), parameter_count));
	result->captures = std::move(captures);
	return std::move(result);
}

void BoundLambdaRefExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WriteProperty<ColumnBinding>(201, "binding", binding);
	serializer.WritePropertyWithDefault<idx_t>(202, "lambda_index", lambda_index);
	serializer.WritePropertyWithDefault<idx_t>(203, "depth", depth);
}

unique_ptr<Expression> BoundLambdaRefExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto binding = deserializer.ReadProperty<ColumnBinding>(201, "binding");
	auto lambda_index = deserializer.ReadPropertyWithDefault<idx_t>(202, "lambda_index");
	auto depth = deserializer.ReadPropertyWithDefault<idx_t>(203, "depth");
	auto result = duckdb::unique_ptr<BoundLambdaRefExpression>(new BoundLambdaRefExpression(std::move(return_type), binding, lambda_index, depth));
	return std::move(result);
}

void BoundOperatorExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "children", children);
}

unique_ptr<Expression> BoundOperatorExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto result = duckdb::unique_ptr<BoundOperatorExpression>(new BoundOperatorExpression(deserializer.Get<ExpressionType>(), std::move(return_type)));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "children", result->children);
	return std::move(result);
}

void BoundParameterExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "identifier", identifier);
	serializer.WriteProperty<LogicalType>(201, "return_type", return_type);
	serializer.WritePropertyWithDefault<shared_ptr<BoundParameterData>>(202, "parameter_data", parameter_data);
}

unique_ptr<Expression> BoundParameterExpression::Deserialize(Deserializer &deserializer) {
	auto identifier = deserializer.ReadPropertyWithDefault<string>(200, "identifier");
	auto return_type = deserializer.ReadProperty<LogicalType>(201, "return_type");
	auto parameter_data = deserializer.ReadPropertyWithDefault<shared_ptr<BoundParameterData>>(202, "parameter_data");
	auto result = duckdb::unique_ptr<BoundParameterExpression>(new BoundParameterExpression(deserializer.Get<bound_parameter_map_t &>(), std::move(identifier), std::move(return_type), std::move(parameter_data)));
	return std::move(result);
}

void BoundReferenceExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WritePropertyWithDefault<idx_t>(201, "index", index);
}

unique_ptr<Expression> BoundReferenceExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto index = deserializer.ReadPropertyWithDefault<idx_t>(201, "index");
	auto result = duckdb::unique_ptr<BoundReferenceExpression>(new BoundReferenceExpression(std::move(return_type), index));
	return std::move(result);
}

void BoundUnnestExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "return_type", return_type);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(201, "child", child);
}

unique_ptr<Expression> BoundUnnestExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto result = duckdb::unique_ptr<BoundUnnestExpression>(new BoundUnnestExpression(std::move(return_type)));
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(201, "child", result->child);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//







namespace duckdb {

void LogicalOperator::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<LogicalOperatorType>(100, "type", type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<LogicalOperator>>>(101, "children", children);
}

unique_ptr<LogicalOperator> LogicalOperator::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<LogicalOperatorType>(100, "type");
	auto children = deserializer.ReadPropertyWithDefault<vector<unique_ptr<LogicalOperator>>>(101, "children");
	deserializer.Set<LogicalOperatorType>(type);
	unique_ptr<LogicalOperator> result;
	switch (type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		result = LogicalAggregate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_ALTER:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		result = LogicalAnyJoin::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		result = LogicalComparisonJoin::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_ATTACH:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		result = LogicalColumnDataGet::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		result = LogicalComparisonJoin::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		result = LogicalCopyToFile::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		result = LogicalCreateIndex::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
		result = LogicalCreate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
		result = LogicalCreate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
		result = LogicalCreate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		result = LogicalCreateTable::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		result = LogicalCreate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
		result = LogicalCreate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		result = LogicalCrossProduct::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
		result = LogicalCTERef::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		result = LogicalDelete::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		result = LogicalDelimGet::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		result = LogicalComparisonJoin::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DETACH:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		result = LogicalDistinct::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DROP:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		result = LogicalDummyScan::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		result = LogicalEmptyResult::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_EXCEPT:
		result = LogicalSetOperation::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		result = LogicalExplain::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		result = LogicalExpressionGet::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		result = LogicalExtensionOperator::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		result = LogicalFilter::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_GET:
		result = LogicalGet::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		result = LogicalInsert::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
		result = LogicalSetOperation::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		result = LogicalLimit::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		result = LogicalLimitPercent::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_LOAD:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		result = LogicalMaterializedCTE::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		result = LogicalOrder::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_PIVOT:
		result = LogicalPivot::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		result = LogicalPositionalJoin::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		result = LogicalProjection::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		result = LogicalRecursiveCTE::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_RESET:
		result = LogicalReset::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_SAMPLE:
		result = LogicalSample::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_SET:
		result = LogicalSet::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_SHOW:
		result = LogicalShow::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_TOP_N:
		result = LogicalTopN::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
		result = LogicalSetOperation::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		result = LogicalUnnest::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		result = LogicalUpdate::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_VACUUM:
		result = LogicalSimple::Deserialize(deserializer);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		result = LogicalWindow::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of LogicalOperator!");
	}
	deserializer.Unset<LogicalOperatorType>();
	result->children = std::move(children);
	return result;
}

void LogicalAggregate::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(200, "expressions", expressions);
	serializer.WritePropertyWithDefault<idx_t>(201, "group_index", group_index);
	serializer.WritePropertyWithDefault<idx_t>(202, "aggregate_index", aggregate_index);
	serializer.WritePropertyWithDefault<idx_t>(203, "groupings_index", groupings_index);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(204, "groups", groups);
	serializer.WritePropertyWithDefault<vector<GroupingSet>>(205, "grouping_sets", grouping_sets);
	serializer.WritePropertyWithDefault<vector<unsafe_vector<idx_t>>>(206, "grouping_functions", grouping_functions);
}

unique_ptr<LogicalOperator> LogicalAggregate::Deserialize(Deserializer &deserializer) {
	auto expressions = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(200, "expressions");
	auto group_index = deserializer.ReadPropertyWithDefault<idx_t>(201, "group_index");
	auto aggregate_index = deserializer.ReadPropertyWithDefault<idx_t>(202, "aggregate_index");
	auto result = duckdb::unique_ptr<LogicalAggregate>(new LogicalAggregate(group_index, aggregate_index, std::move(expressions)));
	deserializer.ReadPropertyWithDefault<idx_t>(203, "groupings_index", result->groupings_index);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(204, "groups", result->groups);
	deserializer.ReadPropertyWithDefault<vector<GroupingSet>>(205, "grouping_sets", result->grouping_sets);
	deserializer.ReadPropertyWithDefault<vector<unsafe_vector<idx_t>>>(206, "grouping_functions", result->grouping_functions);
	return std::move(result);
}

void LogicalAnyJoin::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty<JoinType>(200, "join_type", join_type);
	serializer.WritePropertyWithDefault<idx_t>(201, "mark_index", mark_index);
	serializer.WritePropertyWithDefault<vector<idx_t>>(202, "left_projection_map", left_projection_map);
	serializer.WritePropertyWithDefault<vector<idx_t>>(203, "right_projection_map", right_projection_map);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(204, "condition", condition);
}

unique_ptr<LogicalOperator> LogicalAnyJoin::Deserialize(Deserializer &deserializer) {
	auto join_type = deserializer.ReadProperty<JoinType>(200, "join_type");
	auto result = duckdb::unique_ptr<LogicalAnyJoin>(new LogicalAnyJoin(join_type));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "mark_index", result->mark_index);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(202, "left_projection_map", result->left_projection_map);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(203, "right_projection_map", result->right_projection_map);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(204, "condition", result->condition);
	return std::move(result);
}

void LogicalCTERef::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<idx_t>(201, "cte_index", cte_index);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(202, "chunk_types", chunk_types);
	serializer.WritePropertyWithDefault<vector<string>>(203, "bound_columns", bound_columns);
	serializer.WriteProperty<CTEMaterialize>(204, "materialized_cte", materialized_cte);
}

unique_ptr<LogicalOperator> LogicalCTERef::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto cte_index = deserializer.ReadPropertyWithDefault<idx_t>(201, "cte_index");
	auto chunk_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(202, "chunk_types");
	auto bound_columns = deserializer.ReadPropertyWithDefault<vector<string>>(203, "bound_columns");
	auto materialized_cte = deserializer.ReadProperty<CTEMaterialize>(204, "materialized_cte");
	auto result = duckdb::unique_ptr<LogicalCTERef>(new LogicalCTERef(table_index, cte_index, std::move(chunk_types), std::move(bound_columns), materialized_cte));
	return std::move(result);
}

void LogicalColumnDataGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(201, "chunk_types", chunk_types);
	serializer.WritePropertyWithDefault<unique_ptr<ColumnDataCollection>>(202, "collection", collection);
}

unique_ptr<LogicalOperator> LogicalColumnDataGet::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto chunk_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(201, "chunk_types");
	auto collection = deserializer.ReadPropertyWithDefault<unique_ptr<ColumnDataCollection>>(202, "collection");
	auto result = duckdb::unique_ptr<LogicalColumnDataGet>(new LogicalColumnDataGet(table_index, std::move(chunk_types), std::move(collection)));
	return std::move(result);
}

void LogicalComparisonJoin::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty<JoinType>(200, "join_type", join_type);
	serializer.WritePropertyWithDefault<idx_t>(201, "mark_index", mark_index);
	serializer.WritePropertyWithDefault<vector<idx_t>>(202, "left_projection_map", left_projection_map);
	serializer.WritePropertyWithDefault<vector<idx_t>>(203, "right_projection_map", right_projection_map);
	serializer.WritePropertyWithDefault<vector<JoinCondition>>(204, "conditions", conditions);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(205, "mark_types", mark_types);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(206, "duplicate_eliminated_columns", duplicate_eliminated_columns);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::Deserialize(Deserializer &deserializer) {
	auto join_type = deserializer.ReadProperty<JoinType>(200, "join_type");
	auto result = duckdb::unique_ptr<LogicalComparisonJoin>(new LogicalComparisonJoin(join_type, deserializer.Get<LogicalOperatorType>()));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "mark_index", result->mark_index);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(202, "left_projection_map", result->left_projection_map);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(203, "right_projection_map", result->right_projection_map);
	deserializer.ReadPropertyWithDefault<vector<JoinCondition>>(204, "conditions", result->conditions);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(205, "mark_types", result->mark_types);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(206, "duplicate_eliminated_columns", result->duplicate_eliminated_columns);
	return std::move(result);
}

void LogicalCreate::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateInfo>>(200, "info", info);
}

unique_ptr<LogicalOperator> LogicalCreate::Deserialize(Deserializer &deserializer) {
	auto info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "info");
	auto result = duckdb::unique_ptr<LogicalCreate>(new LogicalCreate(deserializer.Get<LogicalOperatorType>(), deserializer.Get<ClientContext &>(), std::move(info)));
	return std::move(result);
}

void LogicalCreateIndex::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateIndexInfo>>(200, "info", info);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "unbound_expressions", unbound_expressions);
}

unique_ptr<LogicalOperator> LogicalCreateIndex::Deserialize(Deserializer &deserializer) {
	auto info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "info");
	auto unbound_expressions = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "unbound_expressions");
	auto result = duckdb::unique_ptr<LogicalCreateIndex>(new LogicalCreateIndex(deserializer.Get<ClientContext &>(), std::move(info), std::move(unbound_expressions)));
	return std::move(result);
}

void LogicalCreateTable::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateInfo>>(200, "info", info->base);
}

unique_ptr<LogicalOperator> LogicalCreateTable::Deserialize(Deserializer &deserializer) {
	auto info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "info");
	auto result = duckdb::unique_ptr<LogicalCreateTable>(new LogicalCreateTable(deserializer.Get<ClientContext &>(), std::move(info)));
	return std::move(result);
}

void LogicalCrossProduct::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalCrossProduct>(new LogicalCrossProduct());
	return std::move(result);
}

void LogicalDelete::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info", table.GetInfo());
	serializer.WritePropertyWithDefault<idx_t>(201, "table_index", table_index);
	serializer.WritePropertyWithDefault<bool>(202, "return_chunk", return_chunk);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(203, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalDelete::Deserialize(Deserializer &deserializer) {
	auto table_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info");
	auto result = duckdb::unique_ptr<LogicalDelete>(new LogicalDelete(deserializer.Get<ClientContext &>(), table_info));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "table_index", result->table_index);
	deserializer.ReadPropertyWithDefault<bool>(202, "return_chunk", result->return_chunk);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(203, "expressions", result->expressions);
	return std::move(result);
}

void LogicalDelimGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(201, "chunk_types", chunk_types);
}

unique_ptr<LogicalOperator> LogicalDelimGet::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto chunk_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(201, "chunk_types");
	auto result = duckdb::unique_ptr<LogicalDelimGet>(new LogicalDelimGet(table_index, std::move(chunk_types)));
	return std::move(result);
}

void LogicalDistinct::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty<DistinctType>(200, "distinct_type", distinct_type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "distinct_targets", distinct_targets);
	serializer.WritePropertyWithDefault<unique_ptr<BoundOrderModifier>>(202, "order_by", order_by);
}

unique_ptr<LogicalOperator> LogicalDistinct::Deserialize(Deserializer &deserializer) {
	auto distinct_type = deserializer.ReadProperty<DistinctType>(200, "distinct_type");
	auto distinct_targets = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "distinct_targets");
	auto result = duckdb::unique_ptr<LogicalDistinct>(new LogicalDistinct(std::move(distinct_targets), distinct_type));
	deserializer.ReadPropertyWithDefault<unique_ptr<BoundOrderModifier>>(202, "order_by", result->order_by);
	return std::move(result);
}

void LogicalDummyScan::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
}

unique_ptr<LogicalOperator> LogicalDummyScan::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto result = duckdb::unique_ptr<LogicalDummyScan>(new LogicalDummyScan(table_index));
	return std::move(result);
}

void LogicalEmptyResult::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(200, "return_types", return_types);
	serializer.WritePropertyWithDefault<vector<ColumnBinding>>(201, "bindings", bindings);
}

unique_ptr<LogicalOperator> LogicalEmptyResult::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalEmptyResult>(new LogicalEmptyResult());
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(200, "return_types", result->return_types);
	deserializer.ReadPropertyWithDefault<vector<ColumnBinding>>(201, "bindings", result->bindings);
	return std::move(result);
}

void LogicalExplain::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty<ExplainType>(200, "explain_type", explain_type);
	serializer.WritePropertyWithDefault<string>(201, "physical_plan", physical_plan);
	serializer.WritePropertyWithDefault<string>(202, "logical_plan_unopt", logical_plan_unopt);
	serializer.WritePropertyWithDefault<string>(203, "logical_plan_opt", logical_plan_opt);
}

unique_ptr<LogicalOperator> LogicalExplain::Deserialize(Deserializer &deserializer) {
	auto explain_type = deserializer.ReadProperty<ExplainType>(200, "explain_type");
	auto result = duckdb::unique_ptr<LogicalExplain>(new LogicalExplain(explain_type));
	deserializer.ReadPropertyWithDefault<string>(201, "physical_plan", result->physical_plan);
	deserializer.ReadPropertyWithDefault<string>(202, "logical_plan_unopt", result->logical_plan_unopt);
	deserializer.ReadPropertyWithDefault<string>(203, "logical_plan_opt", result->logical_plan_opt);
	return std::move(result);
}

void LogicalExpressionGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(201, "expr_types", expr_types);
	serializer.WritePropertyWithDefault<vector<vector<unique_ptr<Expression>>>>(202, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalExpressionGet::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto expr_types = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(201, "expr_types");
	auto expressions = deserializer.ReadPropertyWithDefault<vector<vector<unique_ptr<Expression>>>>(202, "expressions");
	auto result = duckdb::unique_ptr<LogicalExpressionGet>(new LogicalExpressionGet(table_index, std::move(expr_types), std::move(expressions)));
	return std::move(result);
}

void LogicalFilter::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(200, "expressions", expressions);
	serializer.WritePropertyWithDefault<vector<idx_t>>(201, "projection_map", projection_map);
}

unique_ptr<LogicalOperator> LogicalFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalFilter>(new LogicalFilter());
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(200, "expressions", result->expressions);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(201, "projection_map", result->projection_map);
	return std::move(result);
}

void LogicalInsert::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info", table.GetInfo());
	serializer.WritePropertyWithDefault<vector<vector<unique_ptr<Expression>>>>(201, "insert_values", insert_values);
	serializer.WriteProperty<IndexVector<idx_t, PhysicalIndex>>(202, "column_index_map", column_index_map);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(203, "expected_types", expected_types);
	serializer.WritePropertyWithDefault<idx_t>(204, "table_index", table_index);
	serializer.WritePropertyWithDefault<bool>(205, "return_chunk", return_chunk);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(206, "bound_defaults", bound_defaults);
	serializer.WriteProperty<OnConflictAction>(207, "action_type", action_type);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(208, "expected_set_types", expected_set_types);
	serializer.WritePropertyWithDefault<unordered_set<idx_t>>(209, "on_conflict_filter", on_conflict_filter);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(210, "on_conflict_condition", on_conflict_condition);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(211, "do_update_condition", do_update_condition);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(212, "set_columns", set_columns);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(213, "set_types", set_types);
	serializer.WritePropertyWithDefault<idx_t>(214, "excluded_table_index", excluded_table_index);
	serializer.WritePropertyWithDefault<vector<column_t>>(215, "columns_to_fetch", columns_to_fetch);
	serializer.WritePropertyWithDefault<vector<column_t>>(216, "source_columns", source_columns);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(217, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalInsert::Deserialize(Deserializer &deserializer) {
	auto table_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info");
	auto result = duckdb::unique_ptr<LogicalInsert>(new LogicalInsert(deserializer.Get<ClientContext &>(), std::move(table_info)));
	deserializer.ReadPropertyWithDefault<vector<vector<unique_ptr<Expression>>>>(201, "insert_values", result->insert_values);
	deserializer.ReadProperty<IndexVector<idx_t, PhysicalIndex>>(202, "column_index_map", result->column_index_map);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(203, "expected_types", result->expected_types);
	deserializer.ReadPropertyWithDefault<idx_t>(204, "table_index", result->table_index);
	deserializer.ReadPropertyWithDefault<bool>(205, "return_chunk", result->return_chunk);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(206, "bound_defaults", result->bound_defaults);
	deserializer.ReadProperty<OnConflictAction>(207, "action_type", result->action_type);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(208, "expected_set_types", result->expected_set_types);
	deserializer.ReadPropertyWithDefault<unordered_set<idx_t>>(209, "on_conflict_filter", result->on_conflict_filter);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(210, "on_conflict_condition", result->on_conflict_condition);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(211, "do_update_condition", result->do_update_condition);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(212, "set_columns", result->set_columns);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(213, "set_types", result->set_types);
	deserializer.ReadPropertyWithDefault<idx_t>(214, "excluded_table_index", result->excluded_table_index);
	deserializer.ReadPropertyWithDefault<vector<column_t>>(215, "columns_to_fetch", result->columns_to_fetch);
	deserializer.ReadPropertyWithDefault<vector<column_t>>(216, "source_columns", result->source_columns);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(217, "expressions", result->expressions);
	return std::move(result);
}

void LogicalLimit::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<int64_t>(200, "limit_val", limit_val);
	serializer.WritePropertyWithDefault<int64_t>(201, "offset_val", offset_val);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(202, "limit", limit);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(203, "offset", offset);
}

unique_ptr<LogicalOperator> LogicalLimit::Deserialize(Deserializer &deserializer) {
	auto limit_val = deserializer.ReadPropertyWithDefault<int64_t>(200, "limit_val");
	auto offset_val = deserializer.ReadPropertyWithDefault<int64_t>(201, "offset_val");
	auto limit = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(202, "limit");
	auto offset = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(203, "offset");
	auto result = duckdb::unique_ptr<LogicalLimit>(new LogicalLimit(limit_val, offset_val, std::move(limit), std::move(offset)));
	return std::move(result);
}

void LogicalLimitPercent::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty<double>(200, "limit_percent", limit_percent);
	serializer.WritePropertyWithDefault<int64_t>(201, "offset_val", offset_val);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(202, "limit", limit);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(203, "offset", offset);
}

unique_ptr<LogicalOperator> LogicalLimitPercent::Deserialize(Deserializer &deserializer) {
	auto limit_percent = deserializer.ReadProperty<double>(200, "limit_percent");
	auto offset_val = deserializer.ReadPropertyWithDefault<int64_t>(201, "offset_val");
	auto limit = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(202, "limit");
	auto offset = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(203, "offset");
	auto result = duckdb::unique_ptr<LogicalLimitPercent>(new LogicalLimitPercent(limit_percent, offset_val, std::move(limit), std::move(offset)));
	return std::move(result);
}

void LogicalMaterializedCTE::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<idx_t>(201, "column_count", column_count);
	serializer.WritePropertyWithDefault<string>(202, "ctename", ctename);
}

unique_ptr<LogicalOperator> LogicalMaterializedCTE::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalMaterializedCTE>(new LogicalMaterializedCTE());
	deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index", result->table_index);
	deserializer.ReadPropertyWithDefault<idx_t>(201, "column_count", result->column_count);
	deserializer.ReadPropertyWithDefault<string>(202, "ctename", result->ctename);
	return std::move(result);
}

void LogicalOrder::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<BoundOrderByNode>>(200, "orders", orders);
	serializer.WritePropertyWithDefault<vector<idx_t>>(201, "projections", projections);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(Deserializer &deserializer) {
	auto orders = deserializer.ReadPropertyWithDefault<vector<BoundOrderByNode>>(200, "orders");
	auto result = duckdb::unique_ptr<LogicalOrder>(new LogicalOrder(std::move(orders)));
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(201, "projections", result->projections);
	return std::move(result);
}

void LogicalPivot::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "pivot_index", pivot_index);
	serializer.WriteProperty<BoundPivotInfo>(201, "bound_pivot", bound_pivot);
}

unique_ptr<LogicalOperator> LogicalPivot::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalPivot>(new LogicalPivot());
	deserializer.ReadPropertyWithDefault<idx_t>(200, "pivot_index", result->pivot_index);
	deserializer.ReadProperty<BoundPivotInfo>(201, "bound_pivot", result->bound_pivot);
	return std::move(result);
}

void LogicalPositionalJoin::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalPositionalJoin>(new LogicalPositionalJoin());
	return std::move(result);
}

void LogicalProjection::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalProjection::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto expressions = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions");
	auto result = duckdb::unique_ptr<LogicalProjection>(new LogicalProjection(table_index, std::move(expressions)));
	return std::move(result);
}

void LogicalRecursiveCTE::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<bool>(200, "union_all", union_all);
	serializer.WritePropertyWithDefault<string>(201, "ctename", ctename);
	serializer.WritePropertyWithDefault<idx_t>(202, "table_index", table_index);
	serializer.WritePropertyWithDefault<idx_t>(203, "column_count", column_count);
}

unique_ptr<LogicalOperator> LogicalRecursiveCTE::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalRecursiveCTE>(new LogicalRecursiveCTE());
	deserializer.ReadPropertyWithDefault<bool>(200, "union_all", result->union_all);
	deserializer.ReadPropertyWithDefault<string>(201, "ctename", result->ctename);
	deserializer.ReadPropertyWithDefault<idx_t>(202, "table_index", result->table_index);
	deserializer.ReadPropertyWithDefault<idx_t>(203, "column_count", result->column_count);
	return std::move(result);
}

void LogicalReset::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WriteProperty<SetScope>(201, "scope", scope);
}

unique_ptr<LogicalOperator> LogicalReset::Deserialize(Deserializer &deserializer) {
	auto name = deserializer.ReadPropertyWithDefault<string>(200, "name");
	auto scope = deserializer.ReadProperty<SetScope>(201, "scope");
	auto result = duckdb::unique_ptr<LogicalReset>(new LogicalReset(std::move(name), scope));
	return std::move(result);
}

void LogicalSample::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<SampleOptions>>(200, "sample_options", sample_options);
}

unique_ptr<LogicalOperator> LogicalSample::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalSample>(new LogicalSample());
	deserializer.ReadPropertyWithDefault<unique_ptr<SampleOptions>>(200, "sample_options", result->sample_options);
	return std::move(result);
}

void LogicalSet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WriteProperty<Value>(201, "value", value);
	serializer.WriteProperty<SetScope>(202, "scope", scope);
}

unique_ptr<LogicalOperator> LogicalSet::Deserialize(Deserializer &deserializer) {
	auto name = deserializer.ReadPropertyWithDefault<string>(200, "name");
	auto value = deserializer.ReadProperty<Value>(201, "value");
	auto scope = deserializer.ReadProperty<SetScope>(202, "scope");
	auto result = duckdb::unique_ptr<LogicalSet>(new LogicalSet(std::move(name), value, scope));
	return std::move(result);
}

void LogicalSetOperation::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "table_index", table_index);
	serializer.WritePropertyWithDefault<idx_t>(201, "column_count", column_count);
}

unique_ptr<LogicalOperator> LogicalSetOperation::Deserialize(Deserializer &deserializer) {
	auto table_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "table_index");
	auto column_count = deserializer.ReadPropertyWithDefault<idx_t>(201, "column_count");
	auto result = duckdb::unique_ptr<LogicalSetOperation>(new LogicalSetOperation(table_index, column_count, deserializer.Get<LogicalOperatorType>()));
	return std::move(result);
}

void LogicalShow::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(200, "types_select", types_select);
	serializer.WritePropertyWithDefault<vector<string>>(201, "aliases", aliases);
}

unique_ptr<LogicalOperator> LogicalShow::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LogicalShow>(new LogicalShow());
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(200, "types_select", result->types_select);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "aliases", result->aliases);
	return std::move(result);
}

void LogicalSimple::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParseInfo>>(200, "info", info);
}

unique_ptr<LogicalOperator> LogicalSimple::Deserialize(Deserializer &deserializer) {
	auto info = deserializer.ReadPropertyWithDefault<unique_ptr<ParseInfo>>(200, "info");
	auto result = duckdb::unique_ptr<LogicalSimple>(new LogicalSimple(deserializer.Get<LogicalOperatorType>(), std::move(info)));
	return std::move(result);
}

void LogicalTopN::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<BoundOrderByNode>>(200, "orders", orders);
	serializer.WritePropertyWithDefault<idx_t>(201, "limit", limit);
	serializer.WritePropertyWithDefault<idx_t>(202, "offset", offset);
}

unique_ptr<LogicalOperator> LogicalTopN::Deserialize(Deserializer &deserializer) {
	auto orders = deserializer.ReadPropertyWithDefault<vector<BoundOrderByNode>>(200, "orders");
	auto limit = deserializer.ReadPropertyWithDefault<idx_t>(201, "limit");
	auto offset = deserializer.ReadPropertyWithDefault<idx_t>(202, "offset");
	auto result = duckdb::unique_ptr<LogicalTopN>(new LogicalTopN(std::move(orders), limit, offset));
	return std::move(result);
}

void LogicalUnnest::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "unnest_index", unnest_index);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalUnnest::Deserialize(Deserializer &deserializer) {
	auto unnest_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "unnest_index");
	auto result = duckdb::unique_ptr<LogicalUnnest>(new LogicalUnnest(unnest_index));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions", result->expressions);
	return std::move(result);
}

void LogicalUpdate::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info", table.GetInfo());
	serializer.WritePropertyWithDefault<idx_t>(201, "table_index", table_index);
	serializer.WritePropertyWithDefault<bool>(202, "return_chunk", return_chunk);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(203, "expressions", expressions);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(204, "columns", columns);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(205, "bound_defaults", bound_defaults);
	serializer.WritePropertyWithDefault<bool>(206, "update_is_del_and_insert", update_is_del_and_insert);
}

unique_ptr<LogicalOperator> LogicalUpdate::Deserialize(Deserializer &deserializer) {
	auto table_info = deserializer.ReadPropertyWithDefault<unique_ptr<CreateInfo>>(200, "table_info");
	auto result = duckdb::unique_ptr<LogicalUpdate>(new LogicalUpdate(deserializer.Get<ClientContext &>(), table_info));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "table_index", result->table_index);
	deserializer.ReadPropertyWithDefault<bool>(202, "return_chunk", result->return_chunk);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(203, "expressions", result->expressions);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(204, "columns", result->columns);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(205, "bound_defaults", result->bound_defaults);
	deserializer.ReadPropertyWithDefault<bool>(206, "update_is_del_and_insert", result->update_is_del_and_insert);
	return std::move(result);
}

void LogicalWindow::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "window_index", window_index);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions", expressions);
}

unique_ptr<LogicalOperator> LogicalWindow::Deserialize(Deserializer &deserializer) {
	auto window_index = deserializer.ReadPropertyWithDefault<idx_t>(200, "window_index");
	auto result = duckdb::unique_ptr<LogicalWindow>(new LogicalWindow(window_index));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(201, "expressions", result->expressions);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//







namespace duckdb {

void MacroFunction::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<MacroType>(100, "type", type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(101, "parameters", parameters);
	serializer.WritePropertyWithDefault<unordered_map<string, unique_ptr<ParsedExpression>>>(102, "default_parameters", default_parameters);
}

unique_ptr<MacroFunction> MacroFunction::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<MacroType>(100, "type");
	auto parameters = deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(101, "parameters");
	auto default_parameters = deserializer.ReadPropertyWithDefault<unordered_map<string, unique_ptr<ParsedExpression>>>(102, "default_parameters");
	unique_ptr<MacroFunction> result;
	switch (type) {
	case MacroType::SCALAR_MACRO:
		result = ScalarMacroFunction::Deserialize(deserializer);
		break;
	case MacroType::TABLE_MACRO:
		result = TableMacroFunction::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of MacroFunction!");
	}
	result->parameters = std::move(parameters);
	result->default_parameters = std::move(default_parameters);
	return result;
}

void ScalarMacroFunction::Serialize(Serializer &serializer) const {
	MacroFunction::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression", expression);
}

unique_ptr<MacroFunction> ScalarMacroFunction::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ScalarMacroFunction>(new ScalarMacroFunction());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "expression", result->expression);
	return std::move(result);
}

void TableMacroFunction::Serialize(Serializer &serializer) const {
	MacroFunction::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(200, "query_node", query_node);
}

unique_ptr<MacroFunction> TableMacroFunction::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<TableMacroFunction>(new TableMacroFunction());
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(200, "query_node", result->query_node);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//




























namespace duckdb {

void BoundCaseCheck::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(100, "when_expr", when_expr);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(101, "then_expr", then_expr);
}

BoundCaseCheck BoundCaseCheck::Deserialize(Deserializer &deserializer) {
	BoundCaseCheck result;
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(100, "when_expr", result.when_expr);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "then_expr", result.then_expr);
	return result;
}

void BoundOrderByNode::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<OrderType>(100, "type", type);
	serializer.WriteProperty<OrderByNullType>(101, "null_order", null_order);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(102, "expression", expression);
}

BoundOrderByNode BoundOrderByNode::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<OrderType>(100, "type");
	auto null_order = deserializer.ReadProperty<OrderByNullType>(101, "null_order");
	auto expression = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(102, "expression");
	BoundOrderByNode result(type, null_order, std::move(expression));
	return result;
}

void BoundParameterData::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<Value>(100, "value", value);
	serializer.WriteProperty<LogicalType>(101, "return_type", return_type);
}

shared_ptr<BoundParameterData> BoundParameterData::Deserialize(Deserializer &deserializer) {
	auto value = deserializer.ReadProperty<Value>(100, "value");
	auto result = duckdb::shared_ptr<BoundParameterData>(new BoundParameterData(value));
	deserializer.ReadProperty<LogicalType>(101, "return_type", result->return_type);
	return result;
}

void BoundPivotInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<idx_t>(100, "group_count", group_count);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(101, "types", types);
	serializer.WritePropertyWithDefault<vector<string>>(102, "pivot_values", pivot_values);
	serializer.WritePropertyWithDefault<vector<unique_ptr<Expression>>>(103, "aggregates", aggregates);
}

BoundPivotInfo BoundPivotInfo::Deserialize(Deserializer &deserializer) {
	BoundPivotInfo result;
	deserializer.ReadPropertyWithDefault<idx_t>(100, "group_count", result.group_count);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(101, "types", result.types);
	deserializer.ReadPropertyWithDefault<vector<string>>(102, "pivot_values", result.pivot_values);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(103, "aggregates", result.aggregates);
	return result;
}

void CSVReaderOptions::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<bool>(100, "has_delimiter", has_delimiter);
	serializer.WritePropertyWithDefault<bool>(101, "has_quote", has_quote);
	serializer.WritePropertyWithDefault<bool>(102, "has_escape", has_escape);
	serializer.WritePropertyWithDefault<bool>(103, "has_header", has_header);
	serializer.WritePropertyWithDefault<bool>(104, "ignore_errors", ignore_errors);
	serializer.WritePropertyWithDefault<idx_t>(105, "buffer_sample_size", buffer_sample_size);
	serializer.WritePropertyWithDefault<string>(106, "null_str", null_str);
	serializer.WriteProperty<FileCompressionType>(107, "compression", compression);
	serializer.WritePropertyWithDefault<bool>(108, "allow_quoted_nulls", allow_quoted_nulls);
	serializer.WritePropertyWithDefault<bool>(109, "skip_rows_set", skip_rows_set);
	serializer.WritePropertyWithDefault<idx_t>(110, "maximum_line_size", maximum_line_size);
	serializer.WritePropertyWithDefault<bool>(111, "normalize_names", normalize_names);
	serializer.WritePropertyWithDefault<vector<bool>>(112, "force_not_null", force_not_null);
	serializer.WritePropertyWithDefault<bool>(113, "all_varchar", all_varchar);
	serializer.WritePropertyWithDefault<idx_t>(114, "sample_size_chunks", sample_size_chunks);
	serializer.WritePropertyWithDefault<bool>(115, "auto_detect", auto_detect);
	serializer.WritePropertyWithDefault<string>(116, "file_path", file_path);
	serializer.WritePropertyWithDefault<string>(117, "decimal_separator", decimal_separator);
	serializer.WritePropertyWithDefault<bool>(118, "null_padding", null_padding);
	serializer.WritePropertyWithDefault<idx_t>(119, "buffer_size", buffer_size);
	serializer.WriteProperty<MultiFileReaderOptions>(120, "file_options", file_options);
	serializer.WritePropertyWithDefault<vector<bool>>(121, "force_quote", force_quote);
	serializer.WritePropertyWithDefault<string>(122, "rejects_table_name", rejects_table_name);
	serializer.WritePropertyWithDefault<idx_t>(123, "rejects_limit", rejects_limit);
	serializer.WritePropertyWithDefault<vector<string>>(124, "rejects_recovery_columns", rejects_recovery_columns);
	serializer.WritePropertyWithDefault<vector<idx_t>>(125, "rejects_recovery_column_ids", rejects_recovery_column_ids);
	serializer.WriteProperty<char>(126, "dialect_options.state_machine_options.delimiter", dialect_options.state_machine_options.delimiter);
	serializer.WriteProperty<char>(127, "dialect_options.state_machine_options.quote", dialect_options.state_machine_options.quote);
	serializer.WriteProperty<char>(128, "dialect_options.state_machine_options.escape", dialect_options.state_machine_options.escape);
	serializer.WritePropertyWithDefault<bool>(129, "dialect_options.header", dialect_options.header);
	serializer.WritePropertyWithDefault<idx_t>(130, "dialect_options.num_cols", dialect_options.num_cols);
	serializer.WriteProperty<NewLineIdentifier>(131, "dialect_options.new_line", dialect_options.new_line);
	serializer.WritePropertyWithDefault<idx_t>(132, "dialect_options.skip_rows", dialect_options.skip_rows);
	serializer.WritePropertyWithDefault<map<LogicalTypeId, StrpTimeFormat>>(133, "dialect_options.date_format", dialect_options.date_format);
	serializer.WritePropertyWithDefault<map<LogicalTypeId, bool>>(134, "dialect_options.has_format", dialect_options.has_format);
}

CSVReaderOptions CSVReaderOptions::Deserialize(Deserializer &deserializer) {
	CSVReaderOptions result;
	deserializer.ReadPropertyWithDefault<bool>(100, "has_delimiter", result.has_delimiter);
	deserializer.ReadPropertyWithDefault<bool>(101, "has_quote", result.has_quote);
	deserializer.ReadPropertyWithDefault<bool>(102, "has_escape", result.has_escape);
	deserializer.ReadPropertyWithDefault<bool>(103, "has_header", result.has_header);
	deserializer.ReadPropertyWithDefault<bool>(104, "ignore_errors", result.ignore_errors);
	deserializer.ReadPropertyWithDefault<idx_t>(105, "buffer_sample_size", result.buffer_sample_size);
	deserializer.ReadPropertyWithDefault<string>(106, "null_str", result.null_str);
	deserializer.ReadProperty<FileCompressionType>(107, "compression", result.compression);
	deserializer.ReadPropertyWithDefault<bool>(108, "allow_quoted_nulls", result.allow_quoted_nulls);
	deserializer.ReadPropertyWithDefault<bool>(109, "skip_rows_set", result.skip_rows_set);
	deserializer.ReadPropertyWithDefault<idx_t>(110, "maximum_line_size", result.maximum_line_size);
	deserializer.ReadPropertyWithDefault<bool>(111, "normalize_names", result.normalize_names);
	deserializer.ReadPropertyWithDefault<vector<bool>>(112, "force_not_null", result.force_not_null);
	deserializer.ReadPropertyWithDefault<bool>(113, "all_varchar", result.all_varchar);
	deserializer.ReadPropertyWithDefault<idx_t>(114, "sample_size_chunks", result.sample_size_chunks);
	deserializer.ReadPropertyWithDefault<bool>(115, "auto_detect", result.auto_detect);
	deserializer.ReadPropertyWithDefault<string>(116, "file_path", result.file_path);
	deserializer.ReadPropertyWithDefault<string>(117, "decimal_separator", result.decimal_separator);
	deserializer.ReadPropertyWithDefault<bool>(118, "null_padding", result.null_padding);
	deserializer.ReadPropertyWithDefault<idx_t>(119, "buffer_size", result.buffer_size);
	deserializer.ReadProperty<MultiFileReaderOptions>(120, "file_options", result.file_options);
	deserializer.ReadPropertyWithDefault<vector<bool>>(121, "force_quote", result.force_quote);
	deserializer.ReadPropertyWithDefault<string>(122, "rejects_table_name", result.rejects_table_name);
	deserializer.ReadPropertyWithDefault<idx_t>(123, "rejects_limit", result.rejects_limit);
	deserializer.ReadPropertyWithDefault<vector<string>>(124, "rejects_recovery_columns", result.rejects_recovery_columns);
	deserializer.ReadPropertyWithDefault<vector<idx_t>>(125, "rejects_recovery_column_ids", result.rejects_recovery_column_ids);
	deserializer.ReadProperty<char>(126, "dialect_options.state_machine_options.delimiter", result.dialect_options.state_machine_options.delimiter);
	deserializer.ReadProperty<char>(127, "dialect_options.state_machine_options.quote", result.dialect_options.state_machine_options.quote);
	deserializer.ReadProperty<char>(128, "dialect_options.state_machine_options.escape", result.dialect_options.state_machine_options.escape);
	deserializer.ReadPropertyWithDefault<bool>(129, "dialect_options.header", result.dialect_options.header);
	deserializer.ReadPropertyWithDefault<idx_t>(130, "dialect_options.num_cols", result.dialect_options.num_cols);
	deserializer.ReadProperty<NewLineIdentifier>(131, "dialect_options.new_line", result.dialect_options.new_line);
	deserializer.ReadPropertyWithDefault<idx_t>(132, "dialect_options.skip_rows", result.dialect_options.skip_rows);
	deserializer.ReadPropertyWithDefault<map<LogicalTypeId, StrpTimeFormat>>(133, "dialect_options.date_format", result.dialect_options.date_format);
	deserializer.ReadPropertyWithDefault<map<LogicalTypeId, bool>>(134, "dialect_options.has_format", result.dialect_options.has_format);
	return result;
}

void CaseCheck::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(100, "when_expr", when_expr);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(101, "then_expr", then_expr);
}

CaseCheck CaseCheck::Deserialize(Deserializer &deserializer) {
	CaseCheck result;
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(100, "when_expr", result.when_expr);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(101, "then_expr", result.then_expr);
	return result;
}

void ColumnBinding::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<idx_t>(100, "table_index", table_index);
	serializer.WritePropertyWithDefault<idx_t>(101, "column_index", column_index);
}

ColumnBinding ColumnBinding::Deserialize(Deserializer &deserializer) {
	ColumnBinding result;
	deserializer.ReadPropertyWithDefault<idx_t>(100, "table_index", result.table_index);
	deserializer.ReadPropertyWithDefault<idx_t>(101, "column_index", result.column_index);
	return result;
}

void ColumnDefinition::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<string>(100, "name", name);
	serializer.WriteProperty<LogicalType>(101, "type", type);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(102, "expression", expression);
	serializer.WriteProperty<TableColumnType>(103, "category", category);
	serializer.WriteProperty<duckdb::CompressionType>(104, "compression_type", compression_type);
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &deserializer) {
	auto name = deserializer.ReadPropertyWithDefault<string>(100, "name");
	auto type = deserializer.ReadProperty<LogicalType>(101, "type");
	auto expression = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(102, "expression");
	auto category = deserializer.ReadProperty<TableColumnType>(103, "category");
	ColumnDefinition result(std::move(name), std::move(type), std::move(expression), category);
	deserializer.ReadProperty<duckdb::CompressionType>(104, "compression_type", result.compression_type);
	return result;
}

void ColumnInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<string>>(100, "names", names);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(101, "types", types);
}

ColumnInfo ColumnInfo::Deserialize(Deserializer &deserializer) {
	ColumnInfo result;
	deserializer.ReadPropertyWithDefault<vector<string>>(100, "names", result.names);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(101, "types", result.types);
	return result;
}

void ColumnList::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<ColumnDefinition>>(100, "columns", columns);
}

ColumnList ColumnList::Deserialize(Deserializer &deserializer) {
	auto columns = deserializer.ReadPropertyWithDefault<vector<ColumnDefinition>>(100, "columns");
	ColumnList result(std::move(columns));
	return result;
}

void CommonTableExpressionInfo::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<string>>(100, "aliases", aliases);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(101, "query", query);
	serializer.WriteProperty<CTEMaterialize>(102, "materialized", materialized);
}

unique_ptr<CommonTableExpressionInfo> CommonTableExpressionInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CommonTableExpressionInfo>(new CommonTableExpressionInfo());
	deserializer.ReadPropertyWithDefault<vector<string>>(100, "aliases", result->aliases);
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(101, "query", result->query);
	deserializer.ReadProperty<CTEMaterialize>(102, "materialized", result->materialized);
	return result;
}

void CommonTableExpressionMap::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<case_insensitive_map_t<unique_ptr<CommonTableExpressionInfo>>>(100, "map", map);
}

CommonTableExpressionMap CommonTableExpressionMap::Deserialize(Deserializer &deserializer) {
	CommonTableExpressionMap result;
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<unique_ptr<CommonTableExpressionInfo>>>(100, "map", result.map);
	return result;
}

void HivePartitioningIndex::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<string>(100, "value", value);
	serializer.WritePropertyWithDefault<idx_t>(101, "index", index);
}

HivePartitioningIndex HivePartitioningIndex::Deserialize(Deserializer &deserializer) {
	auto value = deserializer.ReadPropertyWithDefault<string>(100, "value");
	auto index = deserializer.ReadPropertyWithDefault<idx_t>(101, "index");
	HivePartitioningIndex result(std::move(value), index);
	return result;
}

void JoinCondition::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(100, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(101, "right", right);
	serializer.WriteProperty<ExpressionType>(102, "comparison", comparison);
}

JoinCondition JoinCondition::Deserialize(Deserializer &deserializer) {
	JoinCondition result;
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(100, "left", result.left);
	deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(101, "right", result.right);
	deserializer.ReadProperty<ExpressionType>(102, "comparison", result.comparison);
	return result;
}

void LogicalType::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<LogicalTypeId>(100, "id", id_);
	serializer.WritePropertyWithDefault<shared_ptr<ExtraTypeInfo>>(101, "type_info", type_info_);
}

LogicalType LogicalType::Deserialize(Deserializer &deserializer) {
	auto id = deserializer.ReadProperty<LogicalTypeId>(100, "id");
	auto type_info = deserializer.ReadPropertyWithDefault<shared_ptr<ExtraTypeInfo>>(101, "type_info");
	LogicalType result(id, std::move(type_info));
	return result;
}

void MultiFileReaderBindData::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<idx_t>(100, "filename_idx", filename_idx);
	serializer.WritePropertyWithDefault<vector<HivePartitioningIndex>>(101, "hive_partitioning_indexes", hive_partitioning_indexes);
}

MultiFileReaderBindData MultiFileReaderBindData::Deserialize(Deserializer &deserializer) {
	MultiFileReaderBindData result;
	deserializer.ReadPropertyWithDefault<idx_t>(100, "filename_idx", result.filename_idx);
	deserializer.ReadPropertyWithDefault<vector<HivePartitioningIndex>>(101, "hive_partitioning_indexes", result.hive_partitioning_indexes);
	return result;
}

void MultiFileReaderOptions::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<bool>(100, "filename", filename);
	serializer.WritePropertyWithDefault<bool>(101, "hive_partitioning", hive_partitioning);
	serializer.WritePropertyWithDefault<bool>(102, "auto_detect_hive_partitioning", auto_detect_hive_partitioning);
	serializer.WritePropertyWithDefault<bool>(103, "union_by_name", union_by_name);
	serializer.WritePropertyWithDefault<bool>(104, "hive_types_autocast", hive_types_autocast);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<LogicalType>>(105, "hive_types_schema", hive_types_schema);
}

MultiFileReaderOptions MultiFileReaderOptions::Deserialize(Deserializer &deserializer) {
	MultiFileReaderOptions result;
	deserializer.ReadPropertyWithDefault<bool>(100, "filename", result.filename);
	deserializer.ReadPropertyWithDefault<bool>(101, "hive_partitioning", result.hive_partitioning);
	deserializer.ReadPropertyWithDefault<bool>(102, "auto_detect_hive_partitioning", result.auto_detect_hive_partitioning);
	deserializer.ReadPropertyWithDefault<bool>(103, "union_by_name", result.union_by_name);
	deserializer.ReadPropertyWithDefault<bool>(104, "hive_types_autocast", result.hive_types_autocast);
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<LogicalType>>(105, "hive_types_schema", result.hive_types_schema);
	return result;
}

void OrderByNode::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<OrderType>(100, "type", type);
	serializer.WriteProperty<OrderByNullType>(101, "null_order", null_order);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(102, "expression", expression);
}

OrderByNode OrderByNode::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<OrderType>(100, "type");
	auto null_order = deserializer.ReadProperty<OrderByNullType>(101, "null_order");
	auto expression = deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(102, "expression");
	OrderByNode result(type, null_order, std::move(expression));
	return result;
}

void PivotColumn::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(100, "pivot_expressions", pivot_expressions);
	serializer.WritePropertyWithDefault<vector<string>>(101, "unpivot_names", unpivot_names);
	serializer.WritePropertyWithDefault<vector<PivotColumnEntry>>(102, "entries", entries);
	serializer.WritePropertyWithDefault<string>(103, "pivot_enum", pivot_enum);
}

PivotColumn PivotColumn::Deserialize(Deserializer &deserializer) {
	PivotColumn result;
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(100, "pivot_expressions", result.pivot_expressions);
	deserializer.ReadPropertyWithDefault<vector<string>>(101, "unpivot_names", result.unpivot_names);
	deserializer.ReadPropertyWithDefault<vector<PivotColumnEntry>>(102, "entries", result.entries);
	deserializer.ReadPropertyWithDefault<string>(103, "pivot_enum", result.pivot_enum);
	return result;
}

void PivotColumnEntry::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<Value>>(100, "values", values);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(101, "star_expr", star_expr);
	serializer.WritePropertyWithDefault<string>(102, "alias", alias);
}

PivotColumnEntry PivotColumnEntry::Deserialize(Deserializer &deserializer) {
	PivotColumnEntry result;
	deserializer.ReadPropertyWithDefault<vector<Value>>(100, "values", result.values);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(101, "star_expr", result.star_expr);
	deserializer.ReadPropertyWithDefault<string>(102, "alias", result.alias);
	return result;
}

void ReadCSVData::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<string>>(100, "files", files);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(101, "csv_types", csv_types);
	serializer.WritePropertyWithDefault<vector<string>>(102, "csv_names", csv_names);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(103, "return_types", return_types);
	serializer.WritePropertyWithDefault<vector<string>>(104, "return_names", return_names);
	serializer.WritePropertyWithDefault<idx_t>(105, "filename_col_idx", filename_col_idx);
	serializer.WriteProperty<CSVReaderOptions>(106, "options", options);
	serializer.WritePropertyWithDefault<bool>(107, "single_threaded", single_threaded);
	serializer.WriteProperty<MultiFileReaderBindData>(108, "reader_bind", reader_bind);
	serializer.WritePropertyWithDefault<vector<ColumnInfo>>(109, "column_info", column_info);
}

unique_ptr<ReadCSVData> ReadCSVData::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ReadCSVData>(new ReadCSVData());
	deserializer.ReadPropertyWithDefault<vector<string>>(100, "files", result->files);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(101, "csv_types", result->csv_types);
	deserializer.ReadPropertyWithDefault<vector<string>>(102, "csv_names", result->csv_names);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(103, "return_types", result->return_types);
	deserializer.ReadPropertyWithDefault<vector<string>>(104, "return_names", result->return_names);
	deserializer.ReadPropertyWithDefault<idx_t>(105, "filename_col_idx", result->filename_col_idx);
	deserializer.ReadProperty<CSVReaderOptions>(106, "options", result->options);
	deserializer.ReadPropertyWithDefault<bool>(107, "single_threaded", result->single_threaded);
	deserializer.ReadProperty<MultiFileReaderBindData>(108, "reader_bind", result->reader_bind);
	deserializer.ReadPropertyWithDefault<vector<ColumnInfo>>(109, "column_info", result->column_info);
	return result;
}

void SampleOptions::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<Value>(100, "sample_size", sample_size);
	serializer.WritePropertyWithDefault<bool>(101, "is_percentage", is_percentage);
	serializer.WriteProperty<SampleMethod>(102, "method", method);
	serializer.WritePropertyWithDefault<int64_t>(103, "seed", seed);
}

unique_ptr<SampleOptions> SampleOptions::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SampleOptions>(new SampleOptions());
	deserializer.ReadProperty<Value>(100, "sample_size", result->sample_size);
	deserializer.ReadPropertyWithDefault<bool>(101, "is_percentage", result->is_percentage);
	deserializer.ReadProperty<SampleMethod>(102, "method", result->method);
	deserializer.ReadPropertyWithDefault<int64_t>(103, "seed", result->seed);
	return result;
}

void StrpTimeFormat::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<string>(100, "format_specifier", format_specifier);
}

StrpTimeFormat StrpTimeFormat::Deserialize(Deserializer &deserializer) {
	auto format_specifier = deserializer.ReadPropertyWithDefault<string>(100, "format_specifier");
	StrpTimeFormat result(format_specifier);
	return result;
}

void TableFilterSet::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unordered_map<idx_t, unique_ptr<TableFilter>>>(100, "filters", filters);
}

TableFilterSet TableFilterSet::Deserialize(Deserializer &deserializer) {
	TableFilterSet result;
	deserializer.ReadPropertyWithDefault<unordered_map<idx_t, unique_ptr<TableFilter>>>(100, "filters", result.filters);
	return result;
}

void VacuumOptions::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<bool>(100, "vacuum", vacuum);
	serializer.WritePropertyWithDefault<bool>(101, "analyze", analyze);
}

VacuumOptions VacuumOptions::Deserialize(Deserializer &deserializer) {
	VacuumOptions result;
	deserializer.ReadPropertyWithDefault<bool>(100, "vacuum", result.vacuum);
	deserializer.ReadPropertyWithDefault<bool>(101, "analyze", result.analyze);
	return result;
}

void interval_t::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<int32_t>(1, "months", months);
	serializer.WritePropertyWithDefault<int32_t>(2, "days", days);
	serializer.WritePropertyWithDefault<int64_t>(3, "micros", micros);
}

interval_t interval_t::Deserialize(Deserializer &deserializer) {
	interval_t result;
	deserializer.ReadPropertyWithDefault<int32_t>(1, "months", result.months);
	deserializer.ReadPropertyWithDefault<int32_t>(2, "days", result.days);
	deserializer.ReadPropertyWithDefault<int64_t>(3, "micros", result.micros);
	return result;
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//















namespace duckdb {

void ParseInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ParseInfoType>(100, "info_type", info_type);
}

unique_ptr<ParseInfo> ParseInfo::Deserialize(Deserializer &deserializer) {
	auto info_type = deserializer.ReadProperty<ParseInfoType>(100, "info_type");
	unique_ptr<ParseInfo> result;
	switch (info_type) {
	case ParseInfoType::ALTER_INFO:
		result = AlterInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::ATTACH_INFO:
		result = AttachInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::COPY_INFO:
		result = CopyInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::DETACH_INFO:
		result = DetachInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::DROP_INFO:
		result = DropInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::LOAD_INFO:
		result = LoadInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::PRAGMA_INFO:
		result = PragmaInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::TRANSACTION_INFO:
		result = TransactionInfo::Deserialize(deserializer);
		break;
	case ParseInfoType::VACUUM_INFO:
		result = VacuumInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of ParseInfo!");
	}
	return result;
}

void AlterInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WriteProperty<AlterType>(200, "type", type);
	serializer.WritePropertyWithDefault<string>(201, "catalog", catalog);
	serializer.WritePropertyWithDefault<string>(202, "schema", schema);
	serializer.WritePropertyWithDefault<string>(203, "name", name);
	serializer.WriteProperty<OnEntryNotFound>(204, "if_not_found", if_not_found);
	serializer.WritePropertyWithDefault<bool>(205, "allow_internal", allow_internal);
}

unique_ptr<ParseInfo> AlterInfo::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<AlterType>(200, "type");
	auto catalog = deserializer.ReadPropertyWithDefault<string>(201, "catalog");
	auto schema = deserializer.ReadPropertyWithDefault<string>(202, "schema");
	auto name = deserializer.ReadPropertyWithDefault<string>(203, "name");
	auto if_not_found = deserializer.ReadProperty<OnEntryNotFound>(204, "if_not_found");
	auto allow_internal = deserializer.ReadPropertyWithDefault<bool>(205, "allow_internal");
	unique_ptr<AlterInfo> result;
	switch (type) {
	case AlterType::ALTER_TABLE:
		result = AlterTableInfo::Deserialize(deserializer);
		break;
	case AlterType::ALTER_VIEW:
		result = AlterViewInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterInfo!");
	}
	result->catalog = std::move(catalog);
	result->schema = std::move(schema);
	result->name = std::move(name);
	result->if_not_found = if_not_found;
	result->allow_internal = allow_internal;
	return std::move(result);
}

void AlterTableInfo::Serialize(Serializer &serializer) const {
	AlterInfo::Serialize(serializer);
	serializer.WriteProperty<AlterTableType>(300, "alter_table_type", alter_table_type);
}

unique_ptr<AlterInfo> AlterTableInfo::Deserialize(Deserializer &deserializer) {
	auto alter_table_type = deserializer.ReadProperty<AlterTableType>(300, "alter_table_type");
	unique_ptr<AlterTableInfo> result;
	switch (alter_table_type) {
	case AlterTableType::ADD_COLUMN:
		result = AddColumnInfo::Deserialize(deserializer);
		break;
	case AlterTableType::ALTER_COLUMN_TYPE:
		result = ChangeColumnTypeInfo::Deserialize(deserializer);
		break;
	case AlterTableType::DROP_NOT_NULL:
		result = DropNotNullInfo::Deserialize(deserializer);
		break;
	case AlterTableType::FOREIGN_KEY_CONSTRAINT:
		result = AlterForeignKeyInfo::Deserialize(deserializer);
		break;
	case AlterTableType::REMOVE_COLUMN:
		result = RemoveColumnInfo::Deserialize(deserializer);
		break;
	case AlterTableType::RENAME_COLUMN:
		result = RenameColumnInfo::Deserialize(deserializer);
		break;
	case AlterTableType::RENAME_TABLE:
		result = RenameTableInfo::Deserialize(deserializer);
		break;
	case AlterTableType::SET_DEFAULT:
		result = SetDefaultInfo::Deserialize(deserializer);
		break;
	case AlterTableType::SET_NOT_NULL:
		result = SetNotNullInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterTableInfo!");
	}
	return std::move(result);
}

void AlterViewInfo::Serialize(Serializer &serializer) const {
	AlterInfo::Serialize(serializer);
	serializer.WriteProperty<AlterViewType>(300, "alter_view_type", alter_view_type);
}

unique_ptr<AlterInfo> AlterViewInfo::Deserialize(Deserializer &deserializer) {
	auto alter_view_type = deserializer.ReadProperty<AlterViewType>(300, "alter_view_type");
	unique_ptr<AlterViewInfo> result;
	switch (alter_view_type) {
	case AlterViewType::RENAME_VIEW:
		result = RenameViewInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of AlterViewInfo!");
	}
	return std::move(result);
}

void AddColumnInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteProperty<ColumnDefinition>(400, "new_column", new_column);
	serializer.WritePropertyWithDefault<bool>(401, "if_column_not_exists", if_column_not_exists);
}

unique_ptr<AlterTableInfo> AddColumnInfo::Deserialize(Deserializer &deserializer) {
	auto new_column = deserializer.ReadProperty<ColumnDefinition>(400, "new_column");
	auto result = duckdb::unique_ptr<AddColumnInfo>(new AddColumnInfo(std::move(new_column)));
	deserializer.ReadPropertyWithDefault<bool>(401, "if_column_not_exists", result->if_column_not_exists);
	return std::move(result);
}

void AlterForeignKeyInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "fk_table", fk_table);
	serializer.WritePropertyWithDefault<vector<string>>(401, "pk_columns", pk_columns);
	serializer.WritePropertyWithDefault<vector<string>>(402, "fk_columns", fk_columns);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(403, "pk_keys", pk_keys);
	serializer.WritePropertyWithDefault<vector<PhysicalIndex>>(404, "fk_keys", fk_keys);
	serializer.WriteProperty<AlterForeignKeyType>(405, "alter_fk_type", type);
}

unique_ptr<AlterTableInfo> AlterForeignKeyInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<AlterForeignKeyInfo>(new AlterForeignKeyInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "fk_table", result->fk_table);
	deserializer.ReadPropertyWithDefault<vector<string>>(401, "pk_columns", result->pk_columns);
	deserializer.ReadPropertyWithDefault<vector<string>>(402, "fk_columns", result->fk_columns);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(403, "pk_keys", result->pk_keys);
	deserializer.ReadPropertyWithDefault<vector<PhysicalIndex>>(404, "fk_keys", result->fk_keys);
	deserializer.ReadProperty<AlterForeignKeyType>(405, "alter_fk_type", result->type);
	return std::move(result);
}

void AttachInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WritePropertyWithDefault<string>(201, "path", path);
	serializer.WritePropertyWithDefault<unordered_map<string, Value>>(202, "options", options);
}

unique_ptr<ParseInfo> AttachInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<AttachInfo>(new AttachInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadPropertyWithDefault<string>(201, "path", result->path);
	deserializer.ReadPropertyWithDefault<unordered_map<string, Value>>(202, "options", result->options);
	return std::move(result);
}

void ChangeColumnTypeInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "column_name", column_name);
	serializer.WriteProperty<LogicalType>(401, "target_type", target_type);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(402, "expression", expression);
}

unique_ptr<AlterTableInfo> ChangeColumnTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ChangeColumnTypeInfo>(new ChangeColumnTypeInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "column_name", result->column_name);
	deserializer.ReadProperty<LogicalType>(401, "target_type", result->target_type);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(402, "expression", result->expression);
	return std::move(result);
}

void CopyInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "catalog", catalog);
	serializer.WritePropertyWithDefault<string>(201, "schema", schema);
	serializer.WritePropertyWithDefault<string>(202, "table", table);
	serializer.WritePropertyWithDefault<vector<string>>(203, "select_list", select_list);
	serializer.WritePropertyWithDefault<bool>(204, "is_from", is_from);
	serializer.WritePropertyWithDefault<string>(205, "format", format);
	serializer.WritePropertyWithDefault<string>(206, "file_path", file_path);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<vector<Value>>>(207, "options", options);
}

unique_ptr<ParseInfo> CopyInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CopyInfo>(new CopyInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "catalog", result->catalog);
	deserializer.ReadPropertyWithDefault<string>(201, "schema", result->schema);
	deserializer.ReadPropertyWithDefault<string>(202, "table", result->table);
	deserializer.ReadPropertyWithDefault<vector<string>>(203, "select_list", result->select_list);
	deserializer.ReadPropertyWithDefault<bool>(204, "is_from", result->is_from);
	deserializer.ReadPropertyWithDefault<string>(205, "format", result->format);
	deserializer.ReadPropertyWithDefault<string>(206, "file_path", result->file_path);
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<vector<Value>>>(207, "options", result->options);
	return std::move(result);
}

void DetachInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WriteProperty<OnEntryNotFound>(201, "if_not_found", if_not_found);
}

unique_ptr<ParseInfo> DetachInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DetachInfo>(new DetachInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadProperty<OnEntryNotFound>(201, "if_not_found", result->if_not_found);
	return std::move(result);
}

void DropInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WriteProperty<CatalogType>(200, "type", type);
	serializer.WritePropertyWithDefault<string>(201, "catalog", catalog);
	serializer.WritePropertyWithDefault<string>(202, "schema", schema);
	serializer.WritePropertyWithDefault<string>(203, "name", name);
	serializer.WriteProperty<OnEntryNotFound>(204, "if_not_found", if_not_found);
	serializer.WritePropertyWithDefault<bool>(205, "cascade", cascade);
	serializer.WritePropertyWithDefault<bool>(206, "allow_drop_internal", allow_drop_internal);
}

unique_ptr<ParseInfo> DropInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DropInfo>(new DropInfo());
	deserializer.ReadProperty<CatalogType>(200, "type", result->type);
	deserializer.ReadPropertyWithDefault<string>(201, "catalog", result->catalog);
	deserializer.ReadPropertyWithDefault<string>(202, "schema", result->schema);
	deserializer.ReadPropertyWithDefault<string>(203, "name", result->name);
	deserializer.ReadProperty<OnEntryNotFound>(204, "if_not_found", result->if_not_found);
	deserializer.ReadPropertyWithDefault<bool>(205, "cascade", result->cascade);
	deserializer.ReadPropertyWithDefault<bool>(206, "allow_drop_internal", result->allow_drop_internal);
	return std::move(result);
}

void DropNotNullInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "column_name", column_name);
}

unique_ptr<AlterTableInfo> DropNotNullInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DropNotNullInfo>(new DropNotNullInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "column_name", result->column_name);
	return std::move(result);
}

void LoadInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "filename", filename);
	serializer.WriteProperty<LoadType>(201, "load_type", load_type);
	serializer.WritePropertyWithDefault<string>(202, "repository", repository);
}

unique_ptr<ParseInfo> LoadInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LoadInfo>(new LoadInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "filename", result->filename);
	deserializer.ReadProperty<LoadType>(201, "load_type", result->load_type);
	deserializer.ReadPropertyWithDefault<string>(202, "repository", result->repository);
	return std::move(result);
}

void PragmaInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "name", name);
	serializer.WritePropertyWithDefault<vector<Value>>(201, "parameters", parameters);
	serializer.WriteProperty<named_parameter_map_t>(202, "named_parameters", named_parameters);
}

unique_ptr<ParseInfo> PragmaInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<PragmaInfo>(new PragmaInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "name", result->name);
	deserializer.ReadPropertyWithDefault<vector<Value>>(201, "parameters", result->parameters);
	deserializer.ReadProperty<named_parameter_map_t>(202, "named_parameters", result->named_parameters);
	return std::move(result);
}

void RemoveColumnInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "removed_column", removed_column);
	serializer.WritePropertyWithDefault<bool>(401, "if_column_exists", if_column_exists);
	serializer.WritePropertyWithDefault<bool>(402, "cascade", cascade);
}

unique_ptr<AlterTableInfo> RemoveColumnInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<RemoveColumnInfo>(new RemoveColumnInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "removed_column", result->removed_column);
	deserializer.ReadPropertyWithDefault<bool>(401, "if_column_exists", result->if_column_exists);
	deserializer.ReadPropertyWithDefault<bool>(402, "cascade", result->cascade);
	return std::move(result);
}

void RenameColumnInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "old_name", old_name);
	serializer.WritePropertyWithDefault<string>(401, "new_name", new_name);
}

unique_ptr<AlterTableInfo> RenameColumnInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameColumnInfo>(new RenameColumnInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "old_name", result->old_name);
	deserializer.ReadPropertyWithDefault<string>(401, "new_name", result->new_name);
	return std::move(result);
}

void RenameTableInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "new_table_name", new_table_name);
}

unique_ptr<AlterTableInfo> RenameTableInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameTableInfo>(new RenameTableInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "new_table_name", result->new_table_name);
	return std::move(result);
}

void RenameViewInfo::Serialize(Serializer &serializer) const {
	AlterViewInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "new_view_name", new_view_name);
}

unique_ptr<AlterViewInfo> RenameViewInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<RenameViewInfo>(new RenameViewInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "new_view_name", result->new_view_name);
	return std::move(result);
}

void SetDefaultInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "column_name", column_name);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(401, "expression", expression);
}

unique_ptr<AlterTableInfo> SetDefaultInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SetDefaultInfo>(new SetDefaultInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "column_name", result->column_name);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(401, "expression", result->expression);
	return std::move(result);
}

void SetNotNullInfo::Serialize(Serializer &serializer) const {
	AlterTableInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(400, "column_name", column_name);
}

unique_ptr<AlterTableInfo> SetNotNullInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SetNotNullInfo>(new SetNotNullInfo());
	deserializer.ReadPropertyWithDefault<string>(400, "column_name", result->column_name);
	return std::move(result);
}

void TransactionInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WriteProperty<TransactionType>(200, "type", type);
}

unique_ptr<ParseInfo> TransactionInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<TransactionInfo>(new TransactionInfo());
	deserializer.ReadProperty<TransactionType>(200, "type", result->type);
	return std::move(result);
}

void VacuumInfo::Serialize(Serializer &serializer) const {
	ParseInfo::Serialize(serializer);
	serializer.WriteProperty<VacuumOptions>(200, "options", options);
}

unique_ptr<ParseInfo> VacuumInfo::Deserialize(Deserializer &deserializer) {
	auto options = deserializer.ReadProperty<VacuumOptions>(200, "options");
	auto result = duckdb::unique_ptr<VacuumInfo>(new VacuumInfo(options));
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void ParsedExpression::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ExpressionClass>(100, "class", expression_class);
	serializer.WriteProperty<ExpressionType>(101, "type", type);
	serializer.WritePropertyWithDefault<string>(102, "alias", alias);
}

unique_ptr<ParsedExpression> ParsedExpression::Deserialize(Deserializer &deserializer) {
	auto expression_class = deserializer.ReadProperty<ExpressionClass>(100, "class");
	auto type = deserializer.ReadProperty<ExpressionType>(101, "type");
	auto alias = deserializer.ReadPropertyWithDefault<string>(102, "alias");
	deserializer.Set<ExpressionType>(type);
	unique_ptr<ParsedExpression> result;
	switch (expression_class) {
	case ExpressionClass::BETWEEN:
		result = BetweenExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::CASE:
		result = CaseExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::CAST:
		result = CastExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::COLLATE:
		result = CollateExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::COLUMN_REF:
		result = ColumnRefExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::COMPARISON:
		result = ComparisonExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::CONJUNCTION:
		result = ConjunctionExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::CONSTANT:
		result = ConstantExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::DEFAULT:
		result = DefaultExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::FUNCTION:
		result = FunctionExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::LAMBDA:
		result = LambdaExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::OPERATOR:
		result = OperatorExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::PARAMETER:
		result = ParameterExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::POSITIONAL_REFERENCE:
		result = PositionalReferenceExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::STAR:
		result = StarExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::SUBQUERY:
		result = SubqueryExpression::Deserialize(deserializer);
		break;
	case ExpressionClass::WINDOW:
		result = WindowExpression::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of ParsedExpression!");
	}
	deserializer.Unset<ExpressionType>();
	result->alias = std::move(alias);
	return result;
}

void BetweenExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "input", input);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "lower", lower);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(202, "upper", upper);
}

unique_ptr<ParsedExpression> BetweenExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<BetweenExpression>(new BetweenExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "input", result->input);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "lower", result->lower);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(202, "upper", result->upper);
	return std::move(result);
}

void CaseExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<CaseCheck>>(200, "case_checks", case_checks);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "else_expr", else_expr);
}

unique_ptr<ParsedExpression> CaseExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CaseExpression>(new CaseExpression());
	deserializer.ReadPropertyWithDefault<vector<CaseCheck>>(200, "case_checks", result->case_checks);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "else_expr", result->else_expr);
	return std::move(result);
}

void CastExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", child);
	serializer.WriteProperty<LogicalType>(201, "cast_type", cast_type);
	serializer.WritePropertyWithDefault<bool>(202, "try_cast", try_cast);
}

unique_ptr<ParsedExpression> CastExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CastExpression>(new CastExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", result->child);
	deserializer.ReadProperty<LogicalType>(201, "cast_type", result->cast_type);
	deserializer.ReadPropertyWithDefault<bool>(202, "try_cast", result->try_cast);
	return std::move(result);
}

void CollateExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", child);
	serializer.WritePropertyWithDefault<string>(201, "collation", collation);
}

unique_ptr<ParsedExpression> CollateExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CollateExpression>(new CollateExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "child", result->child);
	deserializer.ReadPropertyWithDefault<string>(201, "collation", result->collation);
	return std::move(result);
}

void ColumnRefExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<string>>(200, "column_names", column_names);
}

unique_ptr<ParsedExpression> ColumnRefExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ColumnRefExpression>(new ColumnRefExpression());
	deserializer.ReadPropertyWithDefault<vector<string>>(200, "column_names", result->column_names);
	return std::move(result);
}

void ComparisonExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "right", right);
}

unique_ptr<ParsedExpression> ComparisonExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ComparisonExpression>(new ComparisonExpression(deserializer.Get<ExpressionType>()));
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "left", result->left);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "right", result->right);
	return std::move(result);
}

void ConjunctionExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "children", children);
}

unique_ptr<ParsedExpression> ConjunctionExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ConjunctionExpression>(new ConjunctionExpression(deserializer.Get<ExpressionType>()));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "children", result->children);
	return std::move(result);
}

void ConstantExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WriteProperty<Value>(200, "value", value);
}

unique_ptr<ParsedExpression> ConstantExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ConstantExpression>(new ConstantExpression());
	deserializer.ReadProperty<Value>(200, "value", result->value);
	return std::move(result);
}

void DefaultExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
}

unique_ptr<ParsedExpression> DefaultExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DefaultExpression>(new DefaultExpression());
	return std::move(result);
}

void FunctionExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "function_name", function_name);
	serializer.WritePropertyWithDefault<string>(201, "schema", schema);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(202, "children", children);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(203, "filter", filter);
	serializer.WritePropertyWithDefault<unique_ptr<OrderModifier>>(204, "order_bys", order_bys);
	serializer.WritePropertyWithDefault<bool>(205, "distinct", distinct);
	serializer.WritePropertyWithDefault<bool>(206, "is_operator", is_operator);
	serializer.WritePropertyWithDefault<bool>(207, "export_state", export_state);
	serializer.WritePropertyWithDefault<string>(208, "catalog", catalog);
}

unique_ptr<ParsedExpression> FunctionExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<FunctionExpression>(new FunctionExpression());
	deserializer.ReadPropertyWithDefault<string>(200, "function_name", result->function_name);
	deserializer.ReadPropertyWithDefault<string>(201, "schema", result->schema);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(202, "children", result->children);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(203, "filter", result->filter);
	auto order_bys = deserializer.ReadPropertyWithDefault<unique_ptr<ResultModifier>>(204, "order_bys");
	result->order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(std::move(order_bys));
	deserializer.ReadPropertyWithDefault<bool>(205, "distinct", result->distinct);
	deserializer.ReadPropertyWithDefault<bool>(206, "is_operator", result->is_operator);
	deserializer.ReadPropertyWithDefault<bool>(207, "export_state", result->export_state);
	deserializer.ReadPropertyWithDefault<string>(208, "catalog", result->catalog);
	return std::move(result);
}

void LambdaExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "lhs", lhs);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "expr", expr);
}

unique_ptr<ParsedExpression> LambdaExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LambdaExpression>(new LambdaExpression());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "lhs", result->lhs);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "expr", result->expr);
	return std::move(result);
}

void OperatorExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "children", children);
}

unique_ptr<ParsedExpression> OperatorExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<OperatorExpression>(new OperatorExpression(deserializer.Get<ExpressionType>()));
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "children", result->children);
	return std::move(result);
}

void ParameterExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "identifier", identifier);
}

unique_ptr<ParsedExpression> ParameterExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ParameterExpression>(new ParameterExpression());
	deserializer.ReadPropertyWithDefault<string>(200, "identifier", result->identifier);
	return std::move(result);
}

void PositionalReferenceExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "index", index);
}

unique_ptr<ParsedExpression> PositionalReferenceExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<PositionalReferenceExpression>(new PositionalReferenceExpression());
	deserializer.ReadPropertyWithDefault<idx_t>(200, "index", result->index);
	return std::move(result);
}

void StarExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "relation_name", relation_name);
	serializer.WriteProperty<case_insensitive_set_t>(201, "exclude_list", exclude_list);
	serializer.WritePropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(202, "replace_list", replace_list);
	serializer.WritePropertyWithDefault<bool>(203, "columns", columns);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(204, "expr", expr);
}

unique_ptr<ParsedExpression> StarExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<StarExpression>(new StarExpression());
	deserializer.ReadPropertyWithDefault<string>(200, "relation_name", result->relation_name);
	deserializer.ReadProperty<case_insensitive_set_t>(201, "exclude_list", result->exclude_list);
	deserializer.ReadPropertyWithDefault<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(202, "replace_list", result->replace_list);
	deserializer.ReadPropertyWithDefault<bool>(203, "columns", result->columns);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(204, "expr", result->expr);
	return std::move(result);
}

void SubqueryExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WriteProperty<SubqueryType>(200, "subquery_type", subquery_type);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(201, "subquery", subquery);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(202, "child", child);
	serializer.WriteProperty<ExpressionType>(203, "comparison_type", comparison_type);
}

unique_ptr<ParsedExpression> SubqueryExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SubqueryExpression>(new SubqueryExpression());
	deserializer.ReadProperty<SubqueryType>(200, "subquery_type", result->subquery_type);
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(201, "subquery", result->subquery);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(202, "child", result->child);
	deserializer.ReadProperty<ExpressionType>(203, "comparison_type", result->comparison_type);
	return std::move(result);
}

void WindowExpression::Serialize(Serializer &serializer) const {
	ParsedExpression::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "function_name", function_name);
	serializer.WritePropertyWithDefault<string>(201, "schema", schema);
	serializer.WritePropertyWithDefault<string>(202, "catalog", catalog);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(203, "children", children);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "partitions", partitions);
	serializer.WritePropertyWithDefault<vector<OrderByNode>>(205, "orders", orders);
	serializer.WriteProperty<WindowBoundary>(206, "start", start);
	serializer.WriteProperty<WindowBoundary>(207, "end", end);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(208, "start_expr", start_expr);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(209, "end_expr", end_expr);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(210, "offset_expr", offset_expr);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(211, "default_expr", default_expr);
	serializer.WritePropertyWithDefault<bool>(212, "ignore_nulls", ignore_nulls);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(213, "filter_expr", filter_expr);
}

unique_ptr<ParsedExpression> WindowExpression::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<WindowExpression>(new WindowExpression(deserializer.Get<ExpressionType>()));
	deserializer.ReadPropertyWithDefault<string>(200, "function_name", result->function_name);
	deserializer.ReadPropertyWithDefault<string>(201, "schema", result->schema);
	deserializer.ReadPropertyWithDefault<string>(202, "catalog", result->catalog);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(203, "children", result->children);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(204, "partitions", result->partitions);
	deserializer.ReadPropertyWithDefault<vector<OrderByNode>>(205, "orders", result->orders);
	deserializer.ReadProperty<WindowBoundary>(206, "start", result->start);
	deserializer.ReadProperty<WindowBoundary>(207, "end", result->end);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(208, "start_expr", result->start_expr);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(209, "end_expr", result->end_expr);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(210, "offset_expr", result->offset_expr);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(211, "default_expr", result->default_expr);
	deserializer.ReadPropertyWithDefault<bool>(212, "ignore_nulls", result->ignore_nulls);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(213, "filter_expr", result->filter_expr);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void QueryNode::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<QueryNodeType>(100, "type", type);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ResultModifier>>>(101, "modifiers", modifiers);
	serializer.WriteProperty<CommonTableExpressionMap>(102, "cte_map", cte_map);
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<QueryNodeType>(100, "type");
	auto modifiers = deserializer.ReadPropertyWithDefault<vector<unique_ptr<ResultModifier>>>(101, "modifiers");
	auto cte_map = deserializer.ReadProperty<CommonTableExpressionMap>(102, "cte_map");
	unique_ptr<QueryNode> result;
	switch (type) {
	case QueryNodeType::CTE_NODE:
		result = CTENode::Deserialize(deserializer);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = RecursiveCTENode::Deserialize(deserializer);
		break;
	case QueryNodeType::SELECT_NODE:
		result = SelectNode::Deserialize(deserializer);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = SetOperationNode::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of QueryNode!");
	}
	result->modifiers = std::move(modifiers);
	result->cte_map = std::move(cte_map);
	return result;
}

void CTENode::Serialize(Serializer &serializer) const {
	QueryNode::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "cte_name", ctename);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(201, "query", query);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(202, "child", child);
	serializer.WritePropertyWithDefault<vector<string>>(203, "aliases", aliases);
}

unique_ptr<QueryNode> CTENode::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<CTENode>(new CTENode());
	deserializer.ReadPropertyWithDefault<string>(200, "cte_name", result->ctename);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(201, "query", result->query);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(202, "child", result->child);
	deserializer.ReadPropertyWithDefault<vector<string>>(203, "aliases", result->aliases);
	return std::move(result);
}

void RecursiveCTENode::Serialize(Serializer &serializer) const {
	QueryNode::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "cte_name", ctename);
	serializer.WritePropertyWithDefault<bool>(201, "union_all", union_all, false);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(202, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(203, "right", right);
	serializer.WritePropertyWithDefault<vector<string>>(204, "aliases", aliases);
}

unique_ptr<QueryNode> RecursiveCTENode::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<RecursiveCTENode>(new RecursiveCTENode());
	deserializer.ReadPropertyWithDefault<string>(200, "cte_name", result->ctename);
	deserializer.ReadPropertyWithDefault<bool>(201, "union_all", result->union_all, false);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(202, "left", result->left);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(203, "right", result->right);
	deserializer.ReadPropertyWithDefault<vector<string>>(204, "aliases", result->aliases);
	return std::move(result);
}

void SelectNode::Serialize(Serializer &serializer) const {
	QueryNode::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "select_list", select_list);
	serializer.WritePropertyWithDefault<unique_ptr<TableRef>>(201, "from_table", from_table);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(202, "where_clause", where_clause);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(203, "group_expressions", groups.group_expressions);
	serializer.WritePropertyWithDefault<vector<GroupingSet>>(204, "group_sets", groups.grouping_sets);
	serializer.WriteProperty<AggregateHandling>(205, "aggregate_handling", aggregate_handling);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(206, "having", having);
	serializer.WritePropertyWithDefault<unique_ptr<SampleOptions>>(207, "sample", sample);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(208, "qualify", qualify);
}

unique_ptr<QueryNode> SelectNode::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SelectNode>(new SelectNode());
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "select_list", result->select_list);
	deserializer.ReadPropertyWithDefault<unique_ptr<TableRef>>(201, "from_table", result->from_table);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(202, "where_clause", result->where_clause);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(203, "group_expressions", result->groups.group_expressions);
	deserializer.ReadPropertyWithDefault<vector<GroupingSet>>(204, "group_sets", result->groups.grouping_sets);
	deserializer.ReadProperty<AggregateHandling>(205, "aggregate_handling", result->aggregate_handling);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(206, "having", result->having);
	deserializer.ReadPropertyWithDefault<unique_ptr<SampleOptions>>(207, "sample", result->sample);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(208, "qualify", result->qualify);
	return std::move(result);
}

void SetOperationNode::Serialize(Serializer &serializer) const {
	QueryNode::Serialize(serializer);
	serializer.WriteProperty<SetOperationType>(200, "setop_type", setop_type);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(201, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(202, "right", right);
}

unique_ptr<QueryNode> SetOperationNode::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SetOperationNode>(new SetOperationNode());
	deserializer.ReadProperty<SetOperationType>(200, "setop_type", result->setop_type);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(201, "left", result->left);
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(202, "right", result->right);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//






namespace duckdb {

void ResultModifier::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ResultModifierType>(100, "type", type);
}

unique_ptr<ResultModifier> ResultModifier::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<ResultModifierType>(100, "type");
	unique_ptr<ResultModifier> result;
	switch (type) {
	case ResultModifierType::DISTINCT_MODIFIER:
		result = DistinctModifier::Deserialize(deserializer);
		break;
	case ResultModifierType::LIMIT_MODIFIER:
		result = LimitModifier::Deserialize(deserializer);
		break;
	case ResultModifierType::LIMIT_PERCENT_MODIFIER:
		result = LimitPercentModifier::Deserialize(deserializer);
		break;
	case ResultModifierType::ORDER_MODIFIER:
		result = OrderModifier::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of ResultModifier!");
	}
	return result;
}

void BoundOrderModifier::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<vector<BoundOrderByNode>>(100, "orders", orders);
}

unique_ptr<BoundOrderModifier> BoundOrderModifier::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<BoundOrderModifier>(new BoundOrderModifier());
	deserializer.ReadPropertyWithDefault<vector<BoundOrderByNode>>(100, "orders", result->orders);
	return result;
}

void DistinctModifier::Serialize(Serializer &serializer) const {
	ResultModifier::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "distinct_on_targets", distinct_on_targets);
}

unique_ptr<ResultModifier> DistinctModifier::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<DistinctModifier>(new DistinctModifier());
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(200, "distinct_on_targets", result->distinct_on_targets);
	return std::move(result);
}

void LimitModifier::Serialize(Serializer &serializer) const {
	ResultModifier::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "limit", limit);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "offset", offset);
}

unique_ptr<ResultModifier> LimitModifier::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LimitModifier>(new LimitModifier());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "limit", result->limit);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "offset", result->offset);
	return std::move(result);
}

void LimitPercentModifier::Serialize(Serializer &serializer) const {
	ResultModifier::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "limit", limit);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(201, "offset", offset);
}

unique_ptr<ResultModifier> LimitPercentModifier::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<LimitPercentModifier>(new LimitPercentModifier());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "limit", result->limit);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(201, "offset", result->offset);
	return std::move(result);
}

void OrderModifier::Serialize(Serializer &serializer) const {
	ResultModifier::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<OrderByNode>>(200, "orders", orders);
}

unique_ptr<ResultModifier> OrderModifier::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<OrderModifier>(new OrderModifier());
	deserializer.ReadPropertyWithDefault<vector<OrderByNode>>(200, "orders", result->orders);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void SelectStatement::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<QueryNode>>(100, "node", node);
}

unique_ptr<SelectStatement> SelectStatement::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SelectStatement>(new SelectStatement());
	deserializer.ReadPropertyWithDefault<unique_ptr<QueryNode>>(100, "node", result->node);
	return result;
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//







namespace duckdb {

void BlockPointer::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<block_id_t>(100, "block_id", block_id);
	serializer.WritePropertyWithDefault<uint32_t>(101, "offset", offset);
}

BlockPointer BlockPointer::Deserialize(Deserializer &deserializer) {
	auto block_id = deserializer.ReadProperty<block_id_t>(100, "block_id");
	auto offset = deserializer.ReadPropertyWithDefault<uint32_t>(101, "offset");
	BlockPointer result(block_id, offset);
	return result;
}

void DataPointer::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<uint64_t>(100, "row_start", row_start);
	serializer.WritePropertyWithDefault<uint64_t>(101, "tuple_count", tuple_count);
	serializer.WriteProperty<BlockPointer>(102, "block_pointer", block_pointer);
	serializer.WriteProperty<CompressionType>(103, "compression_type", compression_type);
	serializer.WriteProperty<BaseStatistics>(104, "statistics", statistics);
	serializer.WritePropertyWithDefault<unique_ptr<ColumnSegmentState>>(105, "segment_state", segment_state);
}

DataPointer DataPointer::Deserialize(Deserializer &deserializer) {
	auto row_start = deserializer.ReadPropertyWithDefault<uint64_t>(100, "row_start");
	auto tuple_count = deserializer.ReadPropertyWithDefault<uint64_t>(101, "tuple_count");
	auto block_pointer = deserializer.ReadProperty<BlockPointer>(102, "block_pointer");
	auto compression_type = deserializer.ReadProperty<CompressionType>(103, "compression_type");
	auto statistics = deserializer.ReadProperty<BaseStatistics>(104, "statistics");
	DataPointer result(std::move(statistics));
	result.row_start = row_start;
	result.tuple_count = tuple_count;
	result.block_pointer = block_pointer;
	result.compression_type = compression_type;
	deserializer.Set<CompressionType>(compression_type);
	deserializer.ReadPropertyWithDefault<unique_ptr<ColumnSegmentState>>(105, "segment_state", result.segment_state);
	deserializer.Unset<CompressionType>();
	return result;
}

void DistinctStatistics::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<idx_t>(100, "sample_count", sample_count);
	serializer.WritePropertyWithDefault<idx_t>(101, "total_count", total_count);
	serializer.WritePropertyWithDefault<unique_ptr<HyperLogLog>>(102, "log", log);
}

unique_ptr<DistinctStatistics> DistinctStatistics::Deserialize(Deserializer &deserializer) {
	auto sample_count = deserializer.ReadPropertyWithDefault<idx_t>(100, "sample_count");
	auto total_count = deserializer.ReadPropertyWithDefault<idx_t>(101, "total_count");
	auto log = deserializer.ReadPropertyWithDefault<unique_ptr<HyperLogLog>>(102, "log");
	auto result = duckdb::unique_ptr<DistinctStatistics>(new DistinctStatistics(std::move(log), sample_count, total_count));
	return result;
}

void MetaBlockPointer::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<idx_t>(100, "block_pointer", block_pointer);
	serializer.WritePropertyWithDefault<uint32_t>(101, "offset", offset);
}

MetaBlockPointer MetaBlockPointer::Deserialize(Deserializer &deserializer) {
	auto block_pointer = deserializer.ReadPropertyWithDefault<idx_t>(100, "block_pointer");
	auto offset = deserializer.ReadPropertyWithDefault<uint32_t>(101, "offset");
	MetaBlockPointer result(block_pointer, offset);
	return result;
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//








namespace duckdb {

void TableFilter::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<TableFilterType>(100, "filter_type", filter_type);
}

unique_ptr<TableFilter> TableFilter::Deserialize(Deserializer &deserializer) {
	auto filter_type = deserializer.ReadProperty<TableFilterType>(100, "filter_type");
	unique_ptr<TableFilter> result;
	switch (filter_type) {
	case TableFilterType::CONJUNCTION_AND:
		result = ConjunctionAndFilter::Deserialize(deserializer);
		break;
	case TableFilterType::CONJUNCTION_OR:
		result = ConjunctionOrFilter::Deserialize(deserializer);
		break;
	case TableFilterType::CONSTANT_COMPARISON:
		result = ConstantFilter::Deserialize(deserializer);
		break;
	case TableFilterType::IS_NOT_NULL:
		result = IsNotNullFilter::Deserialize(deserializer);
		break;
	case TableFilterType::IS_NULL:
		result = IsNullFilter::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of TableFilter!");
	}
	return result;
}

void ConjunctionAndFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<TableFilter>>>(200, "child_filters", child_filters);
}

unique_ptr<TableFilter> ConjunctionAndFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ConjunctionAndFilter>(new ConjunctionAndFilter());
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<TableFilter>>>(200, "child_filters", result->child_filters);
	return std::move(result);
}

void ConjunctionOrFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<unique_ptr<TableFilter>>>(200, "child_filters", child_filters);
}

unique_ptr<TableFilter> ConjunctionOrFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ConjunctionOrFilter>(new ConjunctionOrFilter());
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<TableFilter>>>(200, "child_filters", result->child_filters);
	return std::move(result);
}

void ConstantFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty<ExpressionType>(200, "comparison_type", comparison_type);
	serializer.WriteProperty<Value>(201, "constant", constant);
}

unique_ptr<TableFilter> ConstantFilter::Deserialize(Deserializer &deserializer) {
	auto comparison_type = deserializer.ReadProperty<ExpressionType>(200, "comparison_type");
	auto constant = deserializer.ReadProperty<Value>(201, "constant");
	auto result = duckdb::unique_ptr<ConstantFilter>(new ConstantFilter(comparison_type, constant));
	return std::move(result);
}

void IsNotNullFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
}

unique_ptr<TableFilter> IsNotNullFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<IsNotNullFilter>(new IsNotNullFilter());
	return std::move(result);
}

void IsNullFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
}

unique_ptr<TableFilter> IsNullFilter::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<IsNullFilter>(new IsNullFilter());
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void TableRef::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<TableReferenceType>(100, "type", type);
	serializer.WritePropertyWithDefault<string>(101, "alias", alias);
	serializer.WritePropertyWithDefault<unique_ptr<SampleOptions>>(102, "sample", sample);
}

unique_ptr<TableRef> TableRef::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<TableReferenceType>(100, "type");
	auto alias = deserializer.ReadPropertyWithDefault<string>(101, "alias");
	auto sample = deserializer.ReadPropertyWithDefault<unique_ptr<SampleOptions>>(102, "sample");
	unique_ptr<TableRef> result;
	switch (type) {
	case TableReferenceType::BASE_TABLE:
		result = BaseTableRef::Deserialize(deserializer);
		break;
	case TableReferenceType::EMPTY:
		result = EmptyTableRef::Deserialize(deserializer);
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = ExpressionListRef::Deserialize(deserializer);
		break;
	case TableReferenceType::JOIN:
		result = JoinRef::Deserialize(deserializer);
		break;
	case TableReferenceType::PIVOT:
		result = PivotRef::Deserialize(deserializer);
		break;
	case TableReferenceType::SUBQUERY:
		result = SubqueryRef::Deserialize(deserializer);
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = TableFunctionRef::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of TableRef!");
	}
	result->alias = std::move(alias);
	result->sample = std::move(sample);
	return result;
}

void BaseTableRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "schema_name", schema_name);
	serializer.WritePropertyWithDefault<string>(201, "table_name", table_name);
	serializer.WritePropertyWithDefault<vector<string>>(202, "column_name_alias", column_name_alias);
	serializer.WritePropertyWithDefault<string>(203, "catalog_name", catalog_name);
}

unique_ptr<TableRef> BaseTableRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<BaseTableRef>(new BaseTableRef());
	deserializer.ReadPropertyWithDefault<string>(200, "schema_name", result->schema_name);
	deserializer.ReadPropertyWithDefault<string>(201, "table_name", result->table_name);
	deserializer.ReadPropertyWithDefault<vector<string>>(202, "column_name_alias", result->column_name_alias);
	deserializer.ReadPropertyWithDefault<string>(203, "catalog_name", result->catalog_name);
	return std::move(result);
}

void EmptyTableRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
}

unique_ptr<TableRef> EmptyTableRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<EmptyTableRef>(new EmptyTableRef());
	return std::move(result);
}

void ExpressionListRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<vector<string>>(200, "expected_names", expected_names);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(201, "expected_types", expected_types);
	serializer.WritePropertyWithDefault<vector<vector<unique_ptr<ParsedExpression>>>>(202, "values", values);
}

unique_ptr<TableRef> ExpressionListRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<ExpressionListRef>(new ExpressionListRef());
	deserializer.ReadPropertyWithDefault<vector<string>>(200, "expected_names", result->expected_names);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(201, "expected_types", result->expected_types);
	deserializer.ReadPropertyWithDefault<vector<vector<unique_ptr<ParsedExpression>>>>(202, "values", result->values);
	return std::move(result);
}

void JoinRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<TableRef>>(200, "left", left);
	serializer.WritePropertyWithDefault<unique_ptr<TableRef>>(201, "right", right);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(202, "condition", condition);
	serializer.WriteProperty<JoinType>(203, "join_type", type);
	serializer.WriteProperty<JoinRefType>(204, "ref_type", ref_type);
	serializer.WritePropertyWithDefault<vector<string>>(205, "using_columns", using_columns);
}

unique_ptr<TableRef> JoinRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<JoinRef>(new JoinRef());
	deserializer.ReadPropertyWithDefault<unique_ptr<TableRef>>(200, "left", result->left);
	deserializer.ReadPropertyWithDefault<unique_ptr<TableRef>>(201, "right", result->right);
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(202, "condition", result->condition);
	deserializer.ReadProperty<JoinType>(203, "join_type", result->type);
	deserializer.ReadProperty<JoinRefType>(204, "ref_type", result->ref_type);
	deserializer.ReadPropertyWithDefault<vector<string>>(205, "using_columns", result->using_columns);
	return std::move(result);
}

void PivotRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<TableRef>>(200, "source", source);
	serializer.WritePropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(201, "aggregates", aggregates);
	serializer.WritePropertyWithDefault<vector<string>>(202, "unpivot_names", unpivot_names);
	serializer.WritePropertyWithDefault<vector<PivotColumn>>(203, "pivots", pivots);
	serializer.WritePropertyWithDefault<vector<string>>(204, "groups", groups);
	serializer.WritePropertyWithDefault<vector<string>>(205, "column_name_alias", column_name_alias);
	serializer.WritePropertyWithDefault<bool>(206, "include_nulls", include_nulls);
}

unique_ptr<TableRef> PivotRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<PivotRef>(new PivotRef());
	deserializer.ReadPropertyWithDefault<unique_ptr<TableRef>>(200, "source", result->source);
	deserializer.ReadPropertyWithDefault<vector<unique_ptr<ParsedExpression>>>(201, "aggregates", result->aggregates);
	deserializer.ReadPropertyWithDefault<vector<string>>(202, "unpivot_names", result->unpivot_names);
	deserializer.ReadPropertyWithDefault<vector<PivotColumn>>(203, "pivots", result->pivots);
	deserializer.ReadPropertyWithDefault<vector<string>>(204, "groups", result->groups);
	deserializer.ReadPropertyWithDefault<vector<string>>(205, "column_name_alias", result->column_name_alias);
	deserializer.ReadPropertyWithDefault<bool>(206, "include_nulls", result->include_nulls);
	return std::move(result);
}

void SubqueryRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<SelectStatement>>(200, "subquery", subquery);
	serializer.WritePropertyWithDefault<vector<string>>(201, "column_name_alias", column_name_alias);
}

unique_ptr<TableRef> SubqueryRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<SubqueryRef>(new SubqueryRef());
	deserializer.ReadPropertyWithDefault<unique_ptr<SelectStatement>>(200, "subquery", result->subquery);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "column_name_alias", result->column_name_alias);
	return std::move(result);
}

void TableFunctionRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WritePropertyWithDefault<unique_ptr<ParsedExpression>>(200, "function", function);
	serializer.WritePropertyWithDefault<vector<string>>(201, "column_name_alias", column_name_alias);
}

unique_ptr<TableRef> TableFunctionRef::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::unique_ptr<TableFunctionRef>(new TableFunctionRef());
	deserializer.ReadPropertyWithDefault<unique_ptr<ParsedExpression>>(200, "function", result->function);
	deserializer.ReadPropertyWithDefault<vector<string>>(201, "column_name_alias", result->column_name_alias);
	return std::move(result);
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_serialization.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//





namespace duckdb {

void ExtraTypeInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<ExtraTypeInfoType>(100, "type", type);
	serializer.WritePropertyWithDefault<string>(101, "alias", alias);
}

shared_ptr<ExtraTypeInfo> ExtraTypeInfo::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<ExtraTypeInfoType>(100, "type");
	auto alias = deserializer.ReadPropertyWithDefault<string>(101, "alias");
	shared_ptr<ExtraTypeInfo> result;
	switch (type) {
	case ExtraTypeInfoType::AGGREGATE_STATE_TYPE_INFO:
		result = AggregateStateTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::DECIMAL_TYPE_INFO:
		result = DecimalTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::ENUM_TYPE_INFO:
		result = EnumTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::GENERIC_TYPE_INFO:
		result = make_shared<ExtraTypeInfo>(type);
		break;
	case ExtraTypeInfoType::INVALID_TYPE_INFO:
		return nullptr;
	case ExtraTypeInfoType::LIST_TYPE_INFO:
		result = ListTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::STRING_TYPE_INFO:
		result = StringTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::STRUCT_TYPE_INFO:
		result = StructTypeInfo::Deserialize(deserializer);
		break;
	case ExtraTypeInfoType::USER_TYPE_INFO:
		result = UserTypeInfo::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of ExtraTypeInfo!");
	}
	result->alias = std::move(alias);
	return result;
}

void AggregateStateTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "function_name", state_type.function_name);
	serializer.WriteProperty<LogicalType>(201, "return_type", state_type.return_type);
	serializer.WritePropertyWithDefault<vector<LogicalType>>(202, "bound_argument_types", state_type.bound_argument_types);
}

shared_ptr<ExtraTypeInfo> AggregateStateTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<AggregateStateTypeInfo>(new AggregateStateTypeInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "function_name", result->state_type.function_name);
	deserializer.ReadProperty<LogicalType>(201, "return_type", result->state_type.return_type);
	deserializer.ReadPropertyWithDefault<vector<LogicalType>>(202, "bound_argument_types", result->state_type.bound_argument_types);
	return std::move(result);
}

void DecimalTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<uint8_t>(200, "width", width);
	serializer.WritePropertyWithDefault<uint8_t>(201, "scale", scale);
}

shared_ptr<ExtraTypeInfo> DecimalTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<DecimalTypeInfo>(new DecimalTypeInfo());
	deserializer.ReadPropertyWithDefault<uint8_t>(200, "width", result->width);
	deserializer.ReadPropertyWithDefault<uint8_t>(201, "scale", result->scale);
	return std::move(result);
}

void ListTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WriteProperty<LogicalType>(200, "child_type", child_type);
}

shared_ptr<ExtraTypeInfo> ListTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<ListTypeInfo>(new ListTypeInfo());
	deserializer.ReadProperty<LogicalType>(200, "child_type", result->child_type);
	return std::move(result);
}

void StringTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "collation", collation);
}

shared_ptr<ExtraTypeInfo> StringTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<StringTypeInfo>(new StringTypeInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "collation", result->collation);
	return std::move(result);
}

void StructTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<child_list_t<LogicalType>>(200, "child_types", child_types);
}

shared_ptr<ExtraTypeInfo> StructTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<StructTypeInfo>(new StructTypeInfo());
	deserializer.ReadPropertyWithDefault<child_list_t<LogicalType>>(200, "child_types", result->child_types);
	return std::move(result);
}

void UserTypeInfo::Serialize(Serializer &serializer) const {
	ExtraTypeInfo::Serialize(serializer);
	serializer.WritePropertyWithDefault<string>(200, "user_type_name", user_type_name);
}

shared_ptr<ExtraTypeInfo> UserTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = duckdb::shared_ptr<UserTypeInfo>(new UserTypeInfo());
	deserializer.ReadPropertyWithDefault<string>(200, "user_type_name", result->user_type_name);
	return std::move(result);
}

} // namespace duckdb












#include <algorithm>
#include <cstring>

namespace duckdb {

const char MainHeader::MAGIC_BYTES[] = "DUCK";

void SerializeVersionNumber(WriteStream &ser, const string &version_str) {
	constexpr const idx_t MAX_VERSION_SIZE = 32;
	data_t version[MAX_VERSION_SIZE];
	memset(version, 0, MAX_VERSION_SIZE);
	memcpy(version, version_str.c_str(), MinValue<idx_t>(version_str.size(), MAX_VERSION_SIZE));
	ser.WriteData(version, MAX_VERSION_SIZE);
}

void MainHeader::Write(WriteStream &ser) {
	ser.WriteData(const_data_ptr_cast(MAGIC_BYTES), MAGIC_BYTE_SIZE);
	ser.Write<uint64_t>(version_number);
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		ser.Write<uint64_t>(flags[i]);
	}
	SerializeVersionNumber(ser, DuckDB::LibraryVersion());
	SerializeVersionNumber(ser, DuckDB::SourceID());
}

void MainHeader::CheckMagicBytes(FileHandle &handle) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	if (handle.GetFileSize() < MainHeader::MAGIC_BYTE_SIZE + MainHeader::MAGIC_BYTE_OFFSET) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
	handle.Read(magic_bytes, MainHeader::MAGIC_BYTE_SIZE, MainHeader::MAGIC_BYTE_OFFSET);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file \"%s\" exists, but it is not a valid DuckDB database file!", handle.path);
	}
}

MainHeader MainHeader::Read(ReadStream &source) {
	data_t magic_bytes[MAGIC_BYTE_SIZE];
	MainHeader header;
	source.ReadData(magic_bytes, MainHeader::MAGIC_BYTE_SIZE);
	if (memcmp(magic_bytes, MainHeader::MAGIC_BYTES, MainHeader::MAGIC_BYTE_SIZE) != 0) {
		throw IOException("The file is not a valid DuckDB database file!");
	}
	header.version_number = source.Read<uint64_t>();
	// check the version number
	if (header.version_number != VERSION_NUMBER) {
		auto version = GetDuckDBVersion(header.version_number);
		string version_text;
		if (version) {
			// known version
			version_text = "DuckDB version " + string(version);
		} else {
			version_text = string("an ") + (VERSION_NUMBER > header.version_number ? "older development" : "newer") +
			               string(" version of DuckDB");
		}
		throw IOException(
		    "Trying to read a database file with version number %lld, but we can only read version %lld.\n"
		    "The database file was created with %s.\n\n"
		    "The storage of DuckDB is not yet stable; newer versions of DuckDB cannot read old database files and "
		    "vice versa.\n"
		    "The storage will be stabilized when version 1.0 releases.\n\n"
		    "For now, we recommend that you load the database file in a supported version of DuckDB, and use the "
		    "EXPORT DATABASE command "
		    "followed by IMPORT DATABASE on the current version of DuckDB.\n\n"
		    "See the storage page for more information: https://duckdb.org/internals/storage",
		    header.version_number, VERSION_NUMBER, version_text);
	}
	// read the flags
	for (idx_t i = 0; i < FLAG_COUNT; i++) {
		header.flags[i] = source.Read<uint64_t>();
	}
	return header;
}

void DatabaseHeader::Write(WriteStream &ser) {
	ser.Write<uint64_t>(iteration);
	ser.Write<idx_t>(meta_block);
	ser.Write<idx_t>(free_list);
	ser.Write<uint64_t>(block_count);
}

DatabaseHeader DatabaseHeader::Read(ReadStream &source) {
	DatabaseHeader header;
	header.iteration = source.Read<uint64_t>();
	header.meta_block = source.Read<idx_t>();
	header.free_list = source.Read<idx_t>();
	header.block_count = source.Read<uint64_t>();
	return header;
}

template <class T>
void SerializeHeaderStructure(T header, data_ptr_t ptr) {
	MemoryStream ser(ptr, Storage::FILE_HEADER_SIZE);
	header.Write(ser);
}

template <class T>
T DeserializeHeaderStructure(data_ptr_t ptr) {
	MemoryStream source(ptr, Storage::FILE_HEADER_SIZE);
	return T::Read(source);
}

SingleFileBlockManager::SingleFileBlockManager(AttachedDatabase &db, string path_p, StorageManagerOptions options)
    : BlockManager(BufferManager::GetBufferManager(db)), db(db), path(std::move(path_p)),
      header_buffer(Allocator::Get(db), FileBufferType::MANAGED_BUFFER,
                    Storage::FILE_HEADER_SIZE - Storage::BLOCK_HEADER_SIZE),
      iteration_count(0), options(options) {
}

void SingleFileBlockManager::GetFileFlags(uint8_t &flags, FileLockType &lock, bool create_new) {
	if (options.read_only) {
		D_ASSERT(!create_new);
		flags = FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::READ_LOCK;
	} else {
		flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_READ;
		lock = FileLockType::WRITE_LOCK;
		if (create_new) {
			flags |= FileFlags::FILE_FLAGS_FILE_CREATE;
		}
	}
	if (options.use_direct_io) {
		flags |= FileFlags::FILE_FLAGS_DIRECT_IO;
	}
}

void SingleFileBlockManager::CreateNewDatabase() {
	uint8_t flags;
	FileLockType lock;
	GetFileFlags(flags, lock, true);

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags, lock);

	// if we create a new file, we fill the metadata of the file
	// first fill in the new header
	header_buffer.Clear();

	MainHeader main_header;
	main_header.version_number = VERSION_NUMBER;
	memset(main_header.flags, 0, sizeof(uint64_t) * 4);

	SerializeHeaderStructure<MainHeader>(main_header, header_buffer.buffer);
	// now write the header to the file
	ChecksumAndWrite(header_buffer, 0);
	header_buffer.Clear();

	// write the database headers
	// initialize meta_block and free_list to INVALID_BLOCK because the database file does not contain any actual
	// content yet
	DatabaseHeader h1, h2;
	// header 1
	h1.iteration = 0;
	h1.meta_block = INVALID_BLOCK;
	h1.free_list = INVALID_BLOCK;
	h1.block_count = 0;
	SerializeHeaderStructure<DatabaseHeader>(h1, header_buffer.buffer);
	ChecksumAndWrite(header_buffer, Storage::FILE_HEADER_SIZE);
	// header 2
	h2.iteration = 0;
	h2.meta_block = INVALID_BLOCK;
	h2.free_list = INVALID_BLOCK;
	h2.block_count = 0;
	SerializeHeaderStructure<DatabaseHeader>(h2, header_buffer.buffer);
	ChecksumAndWrite(header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);
	// ensure that writing to disk is completed before returning
	handle->Sync();
	// we start with h2 as active_header, this way our initial write will be in h1
	iteration_count = 0;
	active_header = 1;
	max_block = 0;
}

void SingleFileBlockManager::LoadExistingDatabase() {
	uint8_t flags;
	FileLockType lock;
	GetFileFlags(flags, lock, false);

	// open the RDBMS handle
	auto &fs = FileSystem::Get(db);
	handle = fs.OpenFile(path, flags, lock);

	MainHeader::CheckMagicBytes(*handle);
	// otherwise, we check the metadata of the file
	ReadAndChecksum(header_buffer, 0);
	DeserializeHeaderStructure<MainHeader>(header_buffer.buffer);

	// read the database headers from disk
	DatabaseHeader h1, h2;
	ReadAndChecksum(header_buffer, Storage::FILE_HEADER_SIZE);
	h1 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
	ReadAndChecksum(header_buffer, Storage::FILE_HEADER_SIZE * 2ULL);
	h2 = DeserializeHeaderStructure<DatabaseHeader>(header_buffer.buffer);
	// check the header with the highest iteration count
	if (h1.iteration > h2.iteration) {
		// h1 is active header
		active_header = 0;
		Initialize(h1);
	} else {
		// h2 is active header
		active_header = 1;
		Initialize(h2);
	}
	LoadFreeList();
}

void SingleFileBlockManager::ReadAndChecksum(FileBuffer &block, uint64_t location) const {
	// read the buffer from disk
	block.Read(*handle, location);
	// compute the checksum
	auto stored_checksum = Load<uint64_t>(block.InternalBuffer());
	uint64_t computed_checksum = Checksum(block.buffer, block.size);
	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block",
		                  computed_checksum, stored_checksum);
	}
}

void SingleFileBlockManager::ChecksumAndWrite(FileBuffer &block, uint64_t location) const {
	// compute the checksum and write it to the start of the buffer (if not temp buffer)
	uint64_t checksum = Checksum(block.buffer, block.size);
	Store<uint64_t>(checksum, block.InternalBuffer());
	// now write the buffer
	block.Write(*handle, location);
}

void SingleFileBlockManager::Initialize(DatabaseHeader &header) {
	free_list_id = header.free_list;
	meta_block = header.meta_block;
	iteration_count = header.iteration;
	max_block = header.block_count;
}

void SingleFileBlockManager::LoadFreeList() {
	MetaBlockPointer free_pointer(free_list_id, 0);
	if (!free_pointer.IsValid()) {
		// no free list
		return;
	}
	MetadataReader reader(GetMetadataManager(), free_pointer, nullptr, BlockReaderType::REGISTER_BLOCKS);
	auto free_list_count = reader.Read<uint64_t>();
	free_list.clear();
	for (idx_t i = 0; i < free_list_count; i++) {
		free_list.insert(reader.Read<block_id_t>());
	}
	auto multi_use_blocks_count = reader.Read<uint64_t>();
	multi_use_blocks.clear();
	for (idx_t i = 0; i < multi_use_blocks_count; i++) {
		auto block_id = reader.Read<block_id_t>();
		auto usage_count = reader.Read<uint32_t>();
		multi_use_blocks[block_id] = usage_count;
	}
	GetMetadataManager().Read(reader);
	GetMetadataManager().MarkBlocksAsModified();
}

bool SingleFileBlockManager::IsRootBlock(MetaBlockPointer root) {
	return root.block_pointer == meta_block;
}

block_id_t SingleFileBlockManager::GetFreeBlockId() {
	lock_guard<mutex> lock(block_lock);
	block_id_t block;
	if (!free_list.empty()) {
		// free list is non empty
		// take an entry from the free list
		block = *free_list.begin();
		// erase the entry from the free list again
		free_list.erase(free_list.begin());
	} else {
		block = max_block++;
	}
	return block;
}

void SingleFileBlockManager::MarkBlockAsFree(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	if (free_list.find(block_id) != free_list.end()) {
		throw InternalException("MarkBlockAsFree called but block %llu was already freed!", block_id);
	}
	multi_use_blocks.erase(block_id);
	free_list.insert(block_id);
}

void SingleFileBlockManager::MarkBlockAsModified(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);

	// check if the block is a multi-use block
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		// it is! reduce the reference count of the block
		entry->second--;
		// check the reference count: is the block still a multi-use block?
		if (entry->second <= 1) {
			// no longer a multi-use block!
			multi_use_blocks.erase(entry);
		}
		return;
	}
	// Check for multi-free
	// TODO: Fix the bug that causes this assert to fire, then uncomment it.
	// D_ASSERT(modified_blocks.find(block_id) == modified_blocks.end());
	D_ASSERT(free_list.find(block_id) == free_list.end());
	modified_blocks.insert(block_id);
}

void SingleFileBlockManager::IncreaseBlockReferenceCount(block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	D_ASSERT(block_id >= 0);
	D_ASSERT(block_id < max_block);
	D_ASSERT(free_list.find(block_id) == free_list.end());
	auto entry = multi_use_blocks.find(block_id);
	if (entry != multi_use_blocks.end()) {
		entry->second++;
	} else {
		multi_use_blocks[block_id] = 2;
	}
}

idx_t SingleFileBlockManager::GetMetaBlock() {
	return meta_block;
}

idx_t SingleFileBlockManager::TotalBlocks() {
	lock_guard<mutex> lock(block_lock);
	return max_block;
}

idx_t SingleFileBlockManager::FreeBlocks() {
	lock_guard<mutex> lock(block_lock);
	return free_list.size();
}

unique_ptr<Block> SingleFileBlockManager::ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) {
	D_ASSERT(source_buffer.AllocSize() == Storage::BLOCK_ALLOC_SIZE);
	return make_uniq<Block>(source_buffer, block_id);
}

unique_ptr<Block> SingleFileBlockManager::CreateBlock(block_id_t block_id, FileBuffer *source_buffer) {
	unique_ptr<Block> result;
	if (source_buffer) {
		result = ConvertBlock(block_id, *source_buffer);
	} else {
		result = make_uniq<Block>(Allocator::Get(db), block_id);
	}
	result->Initialize(options.debug_initialize);
	return result;
}

void SingleFileBlockManager::Read(Block &block) {
	D_ASSERT(block.id >= 0);
	D_ASSERT(std::find(free_list.begin(), free_list.end(), block.id) == free_list.end());
	ReadAndChecksum(block, BLOCK_START + block.id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::Write(FileBuffer &buffer, block_id_t block_id) {
	D_ASSERT(block_id >= 0);
	ChecksumAndWrite(buffer, BLOCK_START + block_id * Storage::BLOCK_ALLOC_SIZE);
}

void SingleFileBlockManager::Truncate() {
	BlockManager::Truncate();
	idx_t blocks_to_truncate = 0;
	// reverse iterate over the free-list
	for (auto entry = free_list.rbegin(); entry != free_list.rend(); entry++) {
		auto block_id = *entry;
		if (block_id + 1 != max_block) {
			break;
		}
		blocks_to_truncate++;
		max_block--;
	}
	if (blocks_to_truncate == 0) {
		// nothing to truncate
		return;
	}
	// truncate the file
	for (idx_t i = 0; i < blocks_to_truncate; i++) {
		free_list.erase(max_block + i);
	}
	handle->Truncate(BLOCK_START + max_block * Storage::BLOCK_ALLOC_SIZE);
}

vector<MetadataHandle> SingleFileBlockManager::GetFreeListBlocks() {
	vector<MetadataHandle> free_list_blocks;

	// reserve all blocks that we are going to write the free list to
	// since these blocks are no longer free we cannot just include them in the free list!
	auto block_size = MetadataManager::METADATA_BLOCK_SIZE - sizeof(idx_t);
	idx_t allocated_size = 0;
	while (true) {
		auto free_list_size = sizeof(uint64_t) + sizeof(block_id_t) * (free_list.size() + modified_blocks.size());
		auto multi_use_blocks_size =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(uint32_t)) * multi_use_blocks.size();
		auto metadata_blocks =
		    sizeof(uint64_t) + (sizeof(block_id_t) + sizeof(idx_t)) * GetMetadataManager().BlockCount();
		auto total_size = free_list_size + multi_use_blocks_size + metadata_blocks;
		if (total_size < allocated_size) {
			break;
		}
		auto free_list_handle = GetMetadataManager().AllocateHandle();
		free_list_blocks.push_back(std::move(free_list_handle));
		allocated_size += block_size;
	}

	return free_list_blocks;
}

class FreeListBlockWriter : public MetadataWriter {
public:
	FreeListBlockWriter(MetadataManager &manager, vector<MetadataHandle> free_list_blocks_p)
	    : MetadataWriter(manager), free_list_blocks(std::move(free_list_blocks_p)), index(0) {
	}

	vector<MetadataHandle> free_list_blocks;
	idx_t index;

protected:
	MetadataHandle NextHandle() override {
		if (index >= free_list_blocks.size()) {
			throw InternalException(
			    "Free List Block Writer ran out of blocks, this means not enough blocks were allocated up front");
		}
		return std::move(free_list_blocks[index++]);
	}
};

void SingleFileBlockManager::WriteHeader(DatabaseHeader header) {
	// set the iteration count
	header.iteration = ++iteration_count;

	auto free_list_blocks = GetFreeListBlocks();

	// now handle the free list
	auto &metadata_manager = GetMetadataManager();
	// add all modified blocks to the free list: they can now be written to again
	metadata_manager.MarkBlocksAsModified();
	for (auto &block : modified_blocks) {
		free_list.insert(block);
	}
	modified_blocks.clear();

	if (!free_list_blocks.empty()) {
		// there are blocks to write, either in the free_list or in the modified_blocks
		// we write these blocks specifically to the free_list_blocks
		// a normal MetadataWriter will fetch blocks to use from the free_list
		// but since we are WRITING the free_list, this behavior is sub-optimal
		FreeListBlockWriter writer(metadata_manager, std::move(free_list_blocks));

		auto ptr = writer.GetMetaBlockPointer();
		header.free_list = ptr.block_pointer;

		writer.Write<uint64_t>(free_list.size());
		for (auto &block_id : free_list) {
			writer.Write<block_id_t>(block_id);
		}
		writer.Write<uint64_t>(multi_use_blocks.size());
		for (auto &entry : multi_use_blocks) {
			writer.Write<block_id_t>(entry.first);
			writer.Write<uint32_t>(entry.second);
		}
		GetMetadataManager().Write(writer);
		writer.Flush();
	} else {
		// no blocks in the free list
		header.free_list = DConstants::INVALID_INDEX;
	}
	metadata_manager.Flush();
	header.block_count = max_block;

	auto &config = DBConfig::Get(db);
	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE) {
		throw FatalException("Checkpoint aborted after free list write because of PRAGMA checkpoint_abort flag");
	}

	if (!options.use_direct_io) {
		// if we are not using Direct IO we need to fsync BEFORE we write the header to ensure that all the previous
		// blocks are written as well
		handle->Sync();
	}
	// set the header inside the buffer
	header_buffer.Clear();
	MemoryStream serializer;
	header.Write(serializer);
	memcpy(header_buffer.buffer, serializer.GetData(), serializer.GetPosition());
	// now write the header to the file, active_header determines whether we write to h1 or h2
	// note that if active_header is h1 we write to h2, and vice versa
	ChecksumAndWrite(header_buffer, active_header == 1 ? Storage::FILE_HEADER_SIZE : Storage::FILE_HEADER_SIZE * 2);
	// switch active header to the other header
	active_header = 1 - active_header;
	//! Ensure the header write ends up on disk
	handle->Sync();
}

} // namespace duckdb











namespace duckdb {

struct BufferAllocatorData : PrivateAllocatorData {
	explicit BufferAllocatorData(StandardBufferManager &manager) : manager(manager) {
	}

	StandardBufferManager &manager;
};

unique_ptr<FileBuffer> StandardBufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                                     FileBufferType type) {
	unique_ptr<FileBuffer> result;
	if (source) {
		auto tmp = std::move(source);
		D_ASSERT(tmp->AllocSize() == BufferManager::GetAllocSize(size));
		result = make_uniq<FileBuffer>(*tmp, type);
	} else {
		// no re-usable buffer: allocate a new buffer
		result = make_uniq<FileBuffer>(Allocator::Get(db), type, size);
	}
	result->Initialize(DBConfig::GetConfig(db).options.debug_initialize);
	return result;
}

class TemporaryFileManager;

class TemporaryDirectoryHandle {
public:
	TemporaryDirectoryHandle(DatabaseInstance &db, string path_p);
	~TemporaryDirectoryHandle();

	TemporaryFileManager &GetTempFile();

private:
	DatabaseInstance &db;
	string temp_directory;
	bool created_directory = false;
	unique_ptr<TemporaryFileManager> temp_file;
};

void StandardBufferManager::SetTemporaryDirectory(const string &new_dir) {
	if (temp_directory_handle) {
		throw NotImplementedException("Cannot switch temporary directory after the current one has been used");
	}
	this->temp_directory = new_dir;
}

StandardBufferManager::StandardBufferManager(DatabaseInstance &db, string tmp)
    : BufferManager(), db(db), buffer_pool(db.GetBufferPool()), temp_directory(std::move(tmp)),
      temporary_id(MAXIMUM_BLOCK), buffer_allocator(BufferAllocatorAllocate, BufferAllocatorFree,
                                                    BufferAllocatorRealloc, make_uniq<BufferAllocatorData>(*this)) {
	temp_block_manager = make_uniq<InMemoryBlockManager>(*this);
}

StandardBufferManager::~StandardBufferManager() {
}

BufferPool &StandardBufferManager::GetBufferPool() {
	return buffer_pool;
}

idx_t StandardBufferManager::GetUsedMemory() const {
	return buffer_pool.GetUsedMemory();
}
idx_t StandardBufferManager::GetMaxMemory() const {
	return buffer_pool.GetMaxMemory();
}

template <typename... ARGS>
TempBufferPoolReservation StandardBufferManager::EvictBlocksOrThrow(idx_t memory_delta, unique_ptr<FileBuffer> *buffer,
                                                                    ARGS... args) {
	auto r = buffer_pool.EvictBlocks(memory_delta, buffer_pool.maximum_memory, buffer);
	if (!r.success) {
		string extra_text = StringUtil::Format(" (%s/%s used)", StringUtil::BytesToHumanReadableString(GetUsedMemory()),
		                                       StringUtil::BytesToHumanReadableString(GetMaxMemory()));
		extra_text += InMemoryWarning();
		throw OutOfMemoryException(args..., extra_text);
	}
	return std::move(r.reservation);
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterSmallMemory(idx_t block_size) {
	D_ASSERT(block_size < Storage::BLOCK_SIZE);
	auto res = EvictBlocksOrThrow(block_size, nullptr, "could not allocate block of size %s%s",
	                              StringUtil::BytesToHumanReadableString(block_size));

	auto buffer = ConstructManagedBuffer(block_size, nullptr, FileBufferType::TINY_BUFFER);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, std::move(buffer), false, block_size,
	                                std::move(res));
}

shared_ptr<BlockHandle> StandardBufferManager::RegisterMemory(idx_t block_size, bool can_destroy) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	auto alloc_size = GetAllocSize(block_size);
	// first evict blocks until we have enough memory to store this buffer
	unique_ptr<FileBuffer> reusable_buffer;
	auto res = EvictBlocksOrThrow(alloc_size, &reusable_buffer, "could not allocate block of size %s%s",
	                              StringUtil::BytesToHumanReadableString(alloc_size));

	auto buffer = ConstructManagedBuffer(block_size, std::move(reusable_buffer));

	// create a new block pointer for this block
	return make_shared<BlockHandle>(*temp_block_manager, ++temporary_id, std::move(buffer), can_destroy, alloc_size,
	                                std::move(res));
}

BufferHandle StandardBufferManager::Allocate(idx_t block_size, bool can_destroy, shared_ptr<BlockHandle> *block) {
	shared_ptr<BlockHandle> local_block;
	auto block_ptr = block ? block : &local_block;
	*block_ptr = RegisterMemory(block_size, can_destroy);
	return Pin(*block_ptr);
}

void StandardBufferManager::ReAllocate(shared_ptr<BlockHandle> &handle, idx_t block_size) {
	D_ASSERT(block_size >= Storage::BLOCK_SIZE);
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->state == BlockState::BLOCK_LOADED);
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	D_ASSERT(handle->memory_usage == handle->memory_charge.size);

	auto req = handle->buffer->CalculateMemory(block_size);
	int64_t memory_delta = (int64_t)req.alloc_size - handle->memory_usage;

	if (memory_delta == 0) {
		return;
	} else if (memory_delta > 0) {
		// evict blocks until we have space to resize this block
		auto reservation = EvictBlocksOrThrow(memory_delta, nullptr, "failed to resize block from %s to %s%s",
		                                      StringUtil::BytesToHumanReadableString(handle->memory_usage),
		                                      StringUtil::BytesToHumanReadableString(req.alloc_size));
		// EvictBlocks decrements 'current_memory' for us.
		handle->memory_charge.Merge(std::move(reservation));
	} else {
		// no need to evict blocks, but we do need to decrement 'current_memory'.
		handle->memory_charge.Resize(req.alloc_size);
	}

	handle->ResizeBuffer(block_size, memory_delta);
}

BufferHandle StandardBufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	idx_t required_memory;
	{
		// lock the block
		lock_guard<mutex> lock(handle->lock);
		// check if the block is already loaded
		if (handle->state == BlockState::BLOCK_LOADED) {
			// the block is loaded, increment the reader count and return a pointer to the handle
			handle->readers++;
			return handle->Load(handle);
		}
		required_memory = handle->memory_usage;
	}
	// evict blocks until we have space for the current block
	unique_ptr<FileBuffer> reusable_buffer;
	auto reservation = EvictBlocksOrThrow(required_memory, &reusable_buffer, "failed to pin block of size %s%s",
	                                      StringUtil::BytesToHumanReadableString(required_memory));
	// lock the handle again and repeat the check (in case anybody loaded in the mean time)
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		reservation.Resize(0);
		return handle->Load(handle);
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	auto buf = handle->Load(handle, std::move(reusable_buffer));
	handle->memory_charge = std::move(reservation);
	// In the case of a variable sized block, the buffer may be smaller than a full block.
	int64_t delta = handle->buffer->AllocSize() - handle->memory_usage;
	if (delta) {
		D_ASSERT(delta < 0);
		handle->memory_usage += delta;
		handle->memory_charge.Resize(handle->memory_usage);
	}
	D_ASSERT(handle->memory_usage == handle->buffer->AllocSize());
	return buf;
}

void StandardBufferManager::PurgeQueue() {
	buffer_pool.PurgeQueue();
}

void StandardBufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	buffer_pool.AddToEvictionQueue(handle);
}

void StandardBufferManager::VerifyZeroReaders(shared_ptr<BlockHandle> &handle) {
#ifdef DUCKDB_DEBUG_DESTROY_BLOCKS
	auto replacement_buffer = make_uniq<FileBuffer>(Allocator::Get(db), handle->buffer->type,
	                                                handle->memory_usage - Storage::BLOCK_HEADER_SIZE);
	memcpy(replacement_buffer->buffer, handle->buffer->buffer, handle->buffer->size);
	memset(handle->buffer->buffer, 0xa5, handle->buffer->size); // 0xa5 is default memory in debug mode
	handle->buffer = std::move(replacement_buffer);
#endif
}

void StandardBufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	if (!handle->buffer || handle->buffer->type == FileBufferType::TINY_BUFFER) {
		return;
	}
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	if (handle->readers == 0) {
		VerifyZeroReaders(handle);
		buffer_pool.AddToEvictionQueue(handle);
	}
}

void StandardBufferManager::SetLimit(idx_t limit) {
	buffer_pool.SetLimit(limit, InMemoryWarning());
}

//===--------------------------------------------------------------------===//
// Temporary File Management
//===--------------------------------------------------------------------===//
unique_ptr<FileBuffer> ReadTemporaryBufferInternal(BufferManager &buffer_manager, FileHandle &handle, idx_t position,
                                                   idx_t size, block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
	auto buffer = buffer_manager.ConstructManagedBuffer(size, std::move(reusable_buffer));
	buffer->Read(handle, position);
	return buffer;
}

struct TemporaryFileIndex {
	explicit TemporaryFileIndex(idx_t file_index = DConstants::INVALID_INDEX,
	                            idx_t block_index = DConstants::INVALID_INDEX)
	    : file_index(file_index), block_index(block_index) {
	}

	idx_t file_index;
	idx_t block_index;

public:
	bool IsValid() {
		return block_index != DConstants::INVALID_INDEX;
	}
};

struct BlockIndexManager {
	BlockIndexManager() : max_index(0) {
	}

public:
	//! Obtains a new block index from the index manager
	idx_t GetNewBlockIndex() {
		auto index = GetNewBlockIndexInternal();
		indexes_in_use.insert(index);
		return index;
	}

	//! Removes an index from the block manager
	//! Returns true if the max_index has been altered
	bool RemoveIndex(idx_t index) {
		// remove this block from the set of blocks
		auto entry = indexes_in_use.find(index);
		if (entry == indexes_in_use.end()) {
			throw InternalException("RemoveIndex - index %llu not found in indexes_in_use", index);
		}
		indexes_in_use.erase(entry);
		free_indexes.insert(index);
		// check if we can truncate the file

		// get the max_index in use right now
		auto max_index_in_use = indexes_in_use.empty() ? 0 : *indexes_in_use.rbegin();
		if (max_index_in_use < max_index) {
			// max index in use is lower than the max_index
			// reduce the max_index
			max_index = indexes_in_use.empty() ? 0 : max_index_in_use + 1;
			// we can remove any free_indexes that are larger than the current max_index
			while (!free_indexes.empty()) {
				auto max_entry = *free_indexes.rbegin();
				if (max_entry < max_index) {
					break;
				}
				free_indexes.erase(max_entry);
			}
			return true;
		}
		return false;
	}

	idx_t GetMaxIndex() {
		return max_index;
	}

	bool HasFreeBlocks() {
		return !free_indexes.empty();
	}

private:
	idx_t GetNewBlockIndexInternal() {
		if (free_indexes.empty()) {
			return max_index++;
		}
		auto entry = free_indexes.begin();
		auto index = *entry;
		free_indexes.erase(entry);
		return index;
	}

	idx_t max_index;
	set<idx_t> free_indexes;
	set<idx_t> indexes_in_use;
};

class TemporaryFileHandle {
	constexpr static idx_t MAX_ALLOWED_INDEX_BASE = 4000;

public:
	TemporaryFileHandle(idx_t temp_file_count, DatabaseInstance &db, const string &temp_directory, idx_t index)
	    : max_allowed_index((1 << temp_file_count) * MAX_ALLOWED_INDEX_BASE), db(db), file_index(index),
	      path(FileSystem::GetFileSystem(db).JoinPath(temp_directory,
	                                                  "duckdb_temp_storage-" + to_string(index) + ".tmp")) {
	}

public:
	struct TemporaryFileLock {
		explicit TemporaryFileLock(mutex &mutex) : lock(mutex) {
		}

		lock_guard<mutex> lock;
	};

public:
	TemporaryFileIndex TryGetBlockIndex() {
		TemporaryFileLock lock(file_lock);
		if (index_manager.GetMaxIndex() >= max_allowed_index && index_manager.HasFreeBlocks()) {
			// file is at capacity
			return TemporaryFileIndex();
		}
		// open the file handle if it does not yet exist
		CreateFileIfNotExists(lock);
		// fetch a new block index to write to
		auto block_index = index_manager.GetNewBlockIndex();
		return TemporaryFileIndex(file_index, block_index);
	}

	void WriteTemporaryFile(FileBuffer &buffer, TemporaryFileIndex index) {
		D_ASSERT(buffer.size == Storage::BLOCK_SIZE);
		buffer.Write(*handle, GetPositionInFile(index.block_index));
	}

	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, idx_t block_index,
	                                           unique_ptr<FileBuffer> reusable_buffer) {
		return ReadTemporaryBufferInternal(BufferManager::GetBufferManager(db), *handle, GetPositionInFile(block_index),
		                                   Storage::BLOCK_SIZE, id, std::move(reusable_buffer));
	}

	void EraseBlockIndex(block_id_t block_index) {
		// remove the block (and potentially truncate the temp file)
		TemporaryFileLock lock(file_lock);
		D_ASSERT(handle);
		RemoveTempBlockIndex(lock, block_index);
	}

	bool DeleteIfEmpty() {
		TemporaryFileLock lock(file_lock);
		if (index_manager.GetMaxIndex() > 0) {
			// there are still blocks in this file
			return false;
		}
		// the file is empty: delete it
		handle.reset();
		auto &fs = FileSystem::GetFileSystem(db);
		fs.RemoveFile(path);
		return true;
	}

	TemporaryFileInformation GetTemporaryFile() {
		TemporaryFileLock lock(file_lock);
		TemporaryFileInformation info;
		info.path = path;
		info.size = GetPositionInFile(index_manager.GetMaxIndex());
		return info;
	}

private:
	void CreateFileIfNotExists(TemporaryFileLock &) {
		if (handle) {
			return;
		}
		auto &fs = FileSystem::GetFileSystem(db);
		handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_WRITE |
		                               FileFlags::FILE_FLAGS_FILE_CREATE);
	}

	void RemoveTempBlockIndex(TemporaryFileLock &, idx_t index) {
		// remove the block index from the index manager
		if (index_manager.RemoveIndex(index)) {
			// the max_index that is currently in use has decreased
			// as a result we can truncate the file
#ifndef WIN32 // this ended up causing issues when sorting
			auto max_index = index_manager.GetMaxIndex();
			auto &fs = FileSystem::GetFileSystem(db);
			fs.Truncate(*handle, GetPositionInFile(max_index + 1));
#endif
		}
	}

	idx_t GetPositionInFile(idx_t index) {
		return index * Storage::BLOCK_ALLOC_SIZE;
	}

private:
	const idx_t max_allowed_index;
	DatabaseInstance &db;
	unique_ptr<FileHandle> handle;
	idx_t file_index;
	string path;
	mutex file_lock;
	BlockIndexManager index_manager;
};

class TemporaryFileManager {
public:
	TemporaryFileManager(DatabaseInstance &db, const string &temp_directory_p)
	    : db(db), temp_directory(temp_directory_p) {
	}

public:
	struct TemporaryManagerLock {
		explicit TemporaryManagerLock(mutex &mutex) : lock(mutex) {
		}

		lock_guard<mutex> lock;
	};

	void WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
		D_ASSERT(buffer.size == Storage::BLOCK_SIZE);
		TemporaryFileIndex index;
		TemporaryFileHandle *handle = nullptr;

		{
			TemporaryManagerLock lock(manager_lock);
			// first check if we can write to an open existing file
			for (auto &entry : files) {
				auto &temp_file = entry.second;
				index = temp_file->TryGetBlockIndex();
				if (index.IsValid()) {
					handle = entry.second.get();
					break;
				}
			}
			if (!handle) {
				// no existing handle to write to; we need to create & open a new file
				auto new_file_index = index_manager.GetNewBlockIndex();
				auto new_file = make_uniq<TemporaryFileHandle>(files.size(), db, temp_directory, new_file_index);
				handle = new_file.get();
				files[new_file_index] = std::move(new_file);

				index = handle->TryGetBlockIndex();
			}
			D_ASSERT(used_blocks.find(block_id) == used_blocks.end());
			used_blocks[block_id] = index;
		}
		D_ASSERT(handle);
		D_ASSERT(index.IsValid());
		handle->WriteTemporaryFile(buffer, index);
	}

	bool HasTemporaryBuffer(block_id_t block_id) {
		lock_guard<mutex> lock(manager_lock);
		return used_blocks.find(block_id) != used_blocks.end();
	}

	unique_ptr<FileBuffer> ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> reusable_buffer) {
		TemporaryFileIndex index;
		TemporaryFileHandle *handle;
		{
			TemporaryManagerLock lock(manager_lock);
			index = GetTempBlockIndex(lock, id);
			handle = GetFileHandle(lock, index.file_index);
		}
		auto buffer = handle->ReadTemporaryBuffer(id, index.block_index, std::move(reusable_buffer));
		{
			// remove the block (and potentially erase the temp file)
			TemporaryManagerLock lock(manager_lock);
			EraseUsedBlock(lock, id, handle, index);
		}
		return buffer;
	}

	void DeleteTemporaryBuffer(block_id_t id) {
		TemporaryManagerLock lock(manager_lock);
		auto index = GetTempBlockIndex(lock, id);
		auto handle = GetFileHandle(lock, index.file_index);
		EraseUsedBlock(lock, id, handle, index);
	}

	vector<TemporaryFileInformation> GetTemporaryFiles() {
		lock_guard<mutex> lock(manager_lock);
		vector<TemporaryFileInformation> result;
		for (auto &file : files) {
			result.push_back(file.second->GetTemporaryFile());
		}
		return result;
	}

private:
	void EraseUsedBlock(TemporaryManagerLock &lock, block_id_t id, TemporaryFileHandle *handle,
	                    TemporaryFileIndex index) {
		auto entry = used_blocks.find(id);
		if (entry == used_blocks.end()) {
			throw InternalException("EraseUsedBlock - Block %llu not found in used blocks", id);
		}
		used_blocks.erase(entry);
		handle->EraseBlockIndex(index.block_index);
		if (handle->DeleteIfEmpty()) {
			EraseFileHandle(lock, index.file_index);
		}
	}

	TemporaryFileHandle *GetFileHandle(TemporaryManagerLock &, idx_t index) {
		return files[index].get();
	}

	TemporaryFileIndex GetTempBlockIndex(TemporaryManagerLock &, block_id_t id) {
		D_ASSERT(used_blocks.find(id) != used_blocks.end());
		return used_blocks[id];
	}

	void EraseFileHandle(TemporaryManagerLock &, idx_t file_index) {
		files.erase(file_index);
		index_manager.RemoveIndex(file_index);
	}

private:
	DatabaseInstance &db;
	mutex manager_lock;
	//! The temporary directory
	string temp_directory;
	//! The set of active temporary file handles
	unordered_map<idx_t, unique_ptr<TemporaryFileHandle>> files;
	//! map of block_id -> temporary file position
	unordered_map<block_id_t, TemporaryFileIndex> used_blocks;
	//! Manager of in-use temporary file indexes
	BlockIndexManager index_manager;
};

TemporaryDirectoryHandle::TemporaryDirectoryHandle(DatabaseInstance &db, string path_p)
    : db(db), temp_directory(std::move(path_p)), temp_file(make_uniq<TemporaryFileManager>(db, temp_directory)) {
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		if (!fs.DirectoryExists(temp_directory)) {
			fs.CreateDirectory(temp_directory);
			created_directory = true;
		}
	}
}
TemporaryDirectoryHandle::~TemporaryDirectoryHandle() {
	// first release any temporary files
	temp_file.reset();
	// then delete the temporary file directory
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		bool delete_directory = created_directory;
		vector<string> files_to_delete;
		if (!created_directory) {
			bool deleted_everything = true;
			fs.ListFiles(temp_directory, [&](const string &path, bool isdir) {
				if (isdir) {
					deleted_everything = false;
					return;
				}
				if (!StringUtil::StartsWith(path, "duckdb_temp_")) {
					deleted_everything = false;
					return;
				}
				files_to_delete.push_back(path);
			});
		}
		if (delete_directory) {
			// we want to remove all files in the directory
			fs.RemoveDirectory(temp_directory);
		} else {
			for (auto &file : files_to_delete) {
				fs.RemoveFile(fs.JoinPath(temp_directory, file));
			}
		}
	}
}

TemporaryFileManager &TemporaryDirectoryHandle::GetTempFile() {
	return *temp_file;
}

string StandardBufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temp_directory, "duckdb_temp_block-" + to_string(id) + ".block");
}

void StandardBufferManager::RequireTemporaryDirectory() {
	if (temp_directory.empty()) {
		throw Exception(
		    "Out-of-memory: cannot write buffer because no temporary directory is specified!\nTo enable "
		    "temporary buffer eviction set a temporary directory using PRAGMA temp_directory='/path/to/tmp.tmp'");
	}
	lock_guard<mutex> temp_handle_guard(temp_handle_lock);
	if (!temp_directory_handle) {
		// temp directory has not been created yet: initialize it
		temp_directory_handle = make_uniq<TemporaryDirectoryHandle>(db, temp_directory);
	}
}

void StandardBufferManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	RequireTemporaryDirectory();
	if (buffer.size == Storage::BLOCK_SIZE) {
		temp_directory_handle->GetTempFile().WriteTemporaryBuffer(block_id, buffer);
		return;
	}
	// get the path to write to
	auto path = GetTemporaryPath(block_id);
	D_ASSERT(buffer.size > Storage::BLOCK_SIZE);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> StandardBufferManager::ReadTemporaryBuffer(block_id_t id,
                                                                  unique_ptr<FileBuffer> reusable_buffer) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(temp_directory_handle.get());
	if (temp_directory_handle->GetTempFile().HasTemporaryBuffer(id)) {
		return temp_directory_handle->GetTempFile().ReadTemporaryBuffer(id, std::move(reusable_buffer));
	}
	idx_t block_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&block_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer =
	    ReadTemporaryBufferInternal(*this, *handle, sizeof(idx_t), block_size, id, std::move(reusable_buffer));

	handle.reset();
	DeleteTemporaryFile(id);
	return buffer;
}

void StandardBufferManager::DeleteTemporaryFile(block_id_t id) {
	if (temp_directory.empty()) {
		// no temporary directory specified: nothing to delete
		return;
	}
	{
		lock_guard<mutex> temp_handle_guard(temp_handle_lock);
		if (!temp_directory_handle) {
			// temporary directory was not initialized yet: nothing to delete
			return;
		}
	}
	// check if we should delete the file from the shared pool of files, or from the general file system
	if (temp_directory_handle->GetTempFile().HasTemporaryBuffer(id)) {
		temp_directory_handle->GetTempFile().DeleteTemporaryBuffer(id);
		return;
	}
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

bool StandardBufferManager::HasTemporaryDirectory() const {
	return !temp_directory.empty();
}

vector<TemporaryFileInformation> StandardBufferManager::GetTemporaryFiles() {
	vector<TemporaryFileInformation> result;
	if (temp_directory.empty()) {
		return result;
	}
	{
		lock_guard<mutex> temp_handle_guard(temp_handle_lock);
		if (temp_directory_handle) {
			result = temp_directory_handle->GetTempFile().GetTemporaryFiles();
		}
	}
	auto &fs = FileSystem::GetFileSystem(db);
	fs.ListFiles(temp_directory, [&](const string &name, bool is_dir) {
		if (is_dir) {
			return;
		}
		if (!StringUtil::EndsWith(name, ".block")) {
			return;
		}
		TemporaryFileInformation info;
		info.path = name;
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ);
		info.size = fs.GetFileSize(*handle);
		handle.reset();
		result.push_back(info);
	});
	return result;
}

const char *StandardBufferManager::InMemoryWarning() {
	if (!temp_directory.empty()) {
		return "";
	}
	return "\nDatabase is launched in in-memory mode and no temporary directory is specified."
	       "\nUnused blocks cannot be offloaded to disk."
	       "\n\nLaunch the database with a persistent storage back-end"
	       "\nOr set PRAGMA temp_directory='/path/to/tmp.tmp'";
}

void StandardBufferManager::ReserveMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	auto reservation = EvictBlocksOrThrow(size, nullptr, "failed to reserve memory data of size %s%s",
	                                      StringUtil::BytesToHumanReadableString(size));
	reservation.size = 0;
}

void StandardBufferManager::FreeReservedMemory(idx_t size) {
	if (size == 0) {
		return;
	}
	buffer_pool.current_memory -= size;
}

//===--------------------------------------------------------------------===//
// Buffer Allocator
//===--------------------------------------------------------------------===//
data_ptr_t StandardBufferManager::BufferAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &data = private_data->Cast<BufferAllocatorData>();
	auto reservation = data.manager.EvictBlocksOrThrow(size, nullptr, "failed to allocate data of size %s%s",
	                                                   StringUtil::BytesToHumanReadableString(size));
	// We rely on manual tracking of this one. :(
	reservation.size = 0;
	return Allocator::Get(data.manager.db).AllocateData(size);
}

void StandardBufferManager::BufferAllocatorFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	auto &data = private_data->Cast<BufferAllocatorData>();
	BufferPoolReservation r(data.manager.GetBufferPool());
	r.size = size;
	r.Resize(0);
	return Allocator::Get(data.manager.db).FreeData(pointer, size);
}

data_ptr_t StandardBufferManager::BufferAllocatorRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer,
                                                         idx_t old_size, idx_t size) {
	if (old_size == size) {
		return pointer;
	}
	auto &data = private_data->Cast<BufferAllocatorData>();
	BufferPoolReservation r(data.manager.GetBufferPool());
	r.size = old_size;
	r.Resize(size);
	r.size = 0;
	return Allocator::Get(data.manager.db).ReallocateData(pointer, old_size, size);
}

Allocator &BufferAllocator::Get(ClientContext &context) {
	auto &manager = StandardBufferManager::GetBufferManager(context);
	return manager.GetBufferAllocator();
}

Allocator &BufferAllocator::Get(DatabaseInstance &db) {
	return StandardBufferManager::GetBufferManager(db).GetBufferAllocator();
}

Allocator &BufferAllocator::Get(AttachedDatabase &db) {
	return BufferAllocator::Get(db.GetDatabase());
}

Allocator &StandardBufferManager::GetBufferAllocator() {
	return buffer_allocator;
}

} // namespace duckdb










namespace duckdb {

BaseStatistics::BaseStatistics() : type(LogicalType::INVALID) {
}

BaseStatistics::BaseStatistics(LogicalType type) {
	Construct(*this, std::move(type));
}

void BaseStatistics::Construct(BaseStatistics &stats, LogicalType type) {
	stats.distinct_count = 0;
	stats.type = std::move(type);
	switch (GetStatsType(stats.type)) {
	case StatisticsType::LIST_STATS:
		ListStats::Construct(stats);
		break;
	case StatisticsType::STRUCT_STATS:
		StructStats::Construct(stats);
		break;
	default:
		break;
	}
}

BaseStatistics::~BaseStatistics() {
}

BaseStatistics::BaseStatistics(BaseStatistics &&other) noexcept {
	std::swap(type, other.type);
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
	stats_union = other.stats_union;
	std::swap(child_stats, other.child_stats);
}

BaseStatistics &BaseStatistics::operator=(BaseStatistics &&other) noexcept {
	std::swap(type, other.type);
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
	stats_union = other.stats_union;
	std::swap(child_stats, other.child_stats);
	return *this;
}

StatisticsType BaseStatistics::GetStatsType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::SQLNULL) {
		return StatisticsType::BASE_STATS;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return StatisticsType::NUMERIC_STATS;
	case PhysicalType::VARCHAR:
		return StatisticsType::STRING_STATS;
	case PhysicalType::STRUCT:
		return StatisticsType::STRUCT_STATS;
	case PhysicalType::LIST:
		return StatisticsType::LIST_STATS;
	case PhysicalType::BIT:
	case PhysicalType::INTERVAL:
	default:
		return StatisticsType::BASE_STATS;
	}
}

StatisticsType BaseStatistics::GetStatsType() const {
	return GetStatsType(GetType());
}

void BaseStatistics::InitializeUnknown() {
	has_null = true;
	has_no_null = true;
}

void BaseStatistics::InitializeEmpty() {
	has_null = false;
	has_no_null = true;
}

bool BaseStatistics::CanHaveNull() const {
	return has_null;
}

bool BaseStatistics::CanHaveNoNull() const {
	return has_no_null;
}

bool BaseStatistics::IsConstant() const {
	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity mask
		if (CanHaveNull() && !CanHaveNoNull()) {
			return true;
		}
		if (!CanHaveNull() && CanHaveNoNull()) {
			return true;
		}
		return false;
	}
	switch (GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		return NumericStats::IsConstant(*this);
	default:
		break;
	}
	return false;
}

void BaseStatistics::Merge(const BaseStatistics &other) {
	has_null = has_null || other.has_null;
	has_no_null = has_no_null || other.has_no_null;
	switch (GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		NumericStats::Merge(*this, other);
		break;
	case StatisticsType::STRING_STATS:
		StringStats::Merge(*this, other);
		break;
	case StatisticsType::LIST_STATS:
		ListStats::Merge(*this, other);
		break;
	case StatisticsType::STRUCT_STATS:
		StructStats::Merge(*this, other);
		break;
	default:
		break;
	}
}

idx_t BaseStatistics::GetDistinctCount() {
	return distinct_count;
}

BaseStatistics BaseStatistics::CreateUnknownType(LogicalType type) {
	switch (GetStatsType(type)) {
	case StatisticsType::NUMERIC_STATS:
		return NumericStats::CreateUnknown(std::move(type));
	case StatisticsType::STRING_STATS:
		return StringStats::CreateUnknown(std::move(type));
	case StatisticsType::LIST_STATS:
		return ListStats::CreateUnknown(std::move(type));
	case StatisticsType::STRUCT_STATS:
		return StructStats::CreateUnknown(std::move(type));
	default:
		return BaseStatistics(std::move(type));
	}
}

BaseStatistics BaseStatistics::CreateEmptyType(LogicalType type) {
	switch (GetStatsType(type)) {
	case StatisticsType::NUMERIC_STATS:
		return NumericStats::CreateEmpty(std::move(type));
	case StatisticsType::STRING_STATS:
		return StringStats::CreateEmpty(std::move(type));
	case StatisticsType::LIST_STATS:
		return ListStats::CreateEmpty(std::move(type));
	case StatisticsType::STRUCT_STATS:
		return StructStats::CreateEmpty(std::move(type));
	default:
		return BaseStatistics(std::move(type));
	}
}

BaseStatistics BaseStatistics::CreateUnknown(LogicalType type) {
	auto result = CreateUnknownType(std::move(type));
	result.InitializeUnknown();
	return result;
}

BaseStatistics BaseStatistics::CreateEmpty(LogicalType type) {
	if (type.InternalType() == PhysicalType::BIT) {
		// FIXME: this special case should not be necessary
		// but currently InitializeEmpty sets StatsInfo::CAN_HAVE_VALID_VALUES
		BaseStatistics result(std::move(type));
		result.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		result.Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
		return result;
	}
	auto result = CreateEmptyType(std::move(type));
	result.InitializeEmpty();
	return result;
}

void BaseStatistics::Copy(const BaseStatistics &other) {
	D_ASSERT(GetType() == other.GetType());
	CopyBase(other);
	stats_union = other.stats_union;
	switch (GetStatsType()) {
	case StatisticsType::LIST_STATS:
		ListStats::Copy(*this, other);
		break;
	case StatisticsType::STRUCT_STATS:
		StructStats::Copy(*this, other);
		break;
	default:
		break;
	}
}

BaseStatistics BaseStatistics::Copy() const {
	BaseStatistics result(type);
	result.Copy(*this);
	return result;
}

unique_ptr<BaseStatistics> BaseStatistics::ToUnique() const {
	auto result = unique_ptr<BaseStatistics>(new BaseStatistics(type));
	result->Copy(*this);
	return result;
}

void BaseStatistics::CopyBase(const BaseStatistics &other) {
	has_null = other.has_null;
	has_no_null = other.has_no_null;
	distinct_count = other.distinct_count;
}

void BaseStatistics::Set(StatsInfo info) {
	switch (info) {
	case StatsInfo::CAN_HAVE_NULL_VALUES:
		has_null = true;
		break;
	case StatsInfo::CANNOT_HAVE_NULL_VALUES:
		has_null = false;
		break;
	case StatsInfo::CAN_HAVE_VALID_VALUES:
		has_no_null = true;
		break;
	case StatsInfo::CANNOT_HAVE_VALID_VALUES:
		has_no_null = false;
		break;
	case StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES:
		has_null = true;
		has_no_null = true;
		break;
	default:
		throw InternalException("Unrecognized StatsInfo for BaseStatistics::Set");
	}
}

void BaseStatistics::CombineValidity(BaseStatistics &left, BaseStatistics &right) {
	has_null = left.has_null || right.has_null;
	has_no_null = left.has_no_null || right.has_no_null;
}

void BaseStatistics::CopyValidity(BaseStatistics &stats) {
	has_null = stats.has_null;
	has_no_null = stats.has_no_null;
}

void BaseStatistics::SetDistinctCount(idx_t count) {
	this->distinct_count = count;
}

void BaseStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "has_null", has_null);
	serializer.WriteProperty(101, "has_no_null", has_no_null);
	serializer.WriteProperty(102, "distinct_count", distinct_count);
	serializer.WriteObject(103, "type_stats", [&](Serializer &serializer) {
		switch (GetStatsType()) {
		case StatisticsType::NUMERIC_STATS:
			NumericStats::Serialize(*this, serializer);
			break;
		case StatisticsType::STRING_STATS:
			StringStats::Serialize(*this, serializer);
			break;
		case StatisticsType::LIST_STATS:
			ListStats::Serialize(*this, serializer);
			break;
		case StatisticsType::STRUCT_STATS:
			StructStats::Serialize(*this, serializer);
			break;
		default:
			break;
		}
	});
}

BaseStatistics BaseStatistics::Deserialize(Deserializer &deserializer) {
	auto has_null = deserializer.ReadProperty<bool>(100, "has_null");
	auto has_no_null = deserializer.ReadProperty<bool>(101, "has_no_null");
	auto distinct_count = deserializer.ReadProperty<idx_t>(102, "distinct_count");

	// Get the logical type from the deserializer context.
	auto type = deserializer.Get<LogicalType &>();

	auto stats_type = GetStatsType(type);

	BaseStatistics stats(std::move(type));

	stats.has_null = has_null;
	stats.has_no_null = has_no_null;
	stats.distinct_count = distinct_count;

	deserializer.ReadObject(103, "type_stats", [&](Deserializer &obj) {
		switch (stats_type) {
		case StatisticsType::NUMERIC_STATS:
			NumericStats::Deserialize(obj, stats);
			break;
		case StatisticsType::STRING_STATS:
			StringStats::Deserialize(obj, stats);
			break;
		case StatisticsType::LIST_STATS:
			ListStats::Deserialize(obj, stats);
			break;
		case StatisticsType::STRUCT_STATS:
			StructStats::Deserialize(obj, stats);
			break;
		default:
			break;
		}
	});

	return stats;
}

string BaseStatistics::ToString() const {
	auto has_n = has_null ? "true" : "false";
	auto has_n_n = has_no_null ? "true" : "false";
	string result =
	    StringUtil::Format("%s%s", StringUtil::Format("[Has Null: %s, Has No Null: %s]", has_n, has_n_n),
	                       distinct_count > 0 ? StringUtil::Format("[Approx Unique: %lld]", distinct_count) : "");
	switch (GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		result = NumericStats::ToString(*this) + result;
		break;
	case StatisticsType::STRING_STATS:
		result = StringStats::ToString(*this) + result;
		break;
	case StatisticsType::LIST_STATS:
		result = ListStats::ToString(*this) + result;
		break;
	case StatisticsType::STRUCT_STATS:
		result = StructStats::ToString(*this) + result;
		break;
	default:
		break;
	}
	return result;
}

void BaseStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector.GetType() == this->type);
	switch (GetStatsType()) {
	case StatisticsType::NUMERIC_STATS:
		NumericStats::Verify(*this, vector, sel, count);
		break;
	case StatisticsType::STRING_STATS:
		StringStats::Verify(*this, vector, sel, count);
		break;
	case StatisticsType::LIST_STATS:
		ListStats::Verify(*this, vector, sel, count);
		break;
	case StatisticsType::STRUCT_STATS:
		StructStats::Verify(*this, vector, sel, count);
		break;
	default:
		break;
	}
	if (has_null && has_no_null) {
		// nothing to verify
		return;
	}
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		bool row_is_valid = vdata.validity.RowIsValid(index);
		if (row_is_valid && !has_no_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as having only NULL values, but vector contains valid values: %s",
			    vector.ToString(count));
		}
		if (!row_is_valid && !has_null) {
			throw InternalException(
			    "Statistics mismatch: vector labeled as not having NULL values, but vector contains null values: %s",
			    vector.ToString(count));
		}
	}
}

void BaseStatistics::Verify(Vector &vector, idx_t count) const {
	auto sel = FlatVector::IncrementalSelectionVector();
	Verify(vector, *sel, count);
}

BaseStatistics BaseStatistics::FromConstantType(const Value &input) {
	switch (GetStatsType(input.type())) {
	case StatisticsType::NUMERIC_STATS: {
		auto result = NumericStats::CreateEmpty(input.type());
		NumericStats::SetMin(result, input);
		NumericStats::SetMax(result, input);
		return result;
	}
	case StatisticsType::STRING_STATS: {
		auto result = StringStats::CreateEmpty(input.type());
		if (!input.IsNull()) {
			auto &string_value = StringValue::Get(input);
			StringStats::Update(result, string_t(string_value));
		}
		return result;
	}
	case StatisticsType::LIST_STATS: {
		auto result = ListStats::CreateEmpty(input.type());
		auto &child_stats = ListStats::GetChildStats(result);
		if (!input.IsNull()) {
			auto &list_children = ListValue::GetChildren(input);
			for (auto &child_element : list_children) {
				child_stats.Merge(FromConstant(child_element));
			}
		}
		return result;
	}
	case StatisticsType::STRUCT_STATS: {
		auto result = StructStats::CreateEmpty(input.type());
		auto &child_types = StructType::GetChildTypes(input.type());
		if (input.IsNull()) {
			for (idx_t i = 0; i < child_types.size(); i++) {
				StructStats::SetChildStats(result, i, FromConstant(Value(child_types[i].second)));
			}
		} else {
			auto &struct_children = StructValue::GetChildren(input);
			for (idx_t i = 0; i < child_types.size(); i++) {
				StructStats::SetChildStats(result, i, FromConstant(struct_children[i]));
			}
		}
		return result;
	}
	default:
		return BaseStatistics(input.type());
	}
}

BaseStatistics BaseStatistics::FromConstant(const Value &input) {
	auto result = FromConstantType(input);
	result.SetDistinctCount(1);
	if (input.IsNull()) {
		result.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
		result.Set(StatsInfo::CANNOT_HAVE_VALID_VALUES);
	} else {
		result.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		result.Set(StatsInfo::CAN_HAVE_VALID_VALUES);
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

ColumnStatistics::ColumnStatistics(BaseStatistics stats_p) : stats(std::move(stats_p)) {
	if (DistinctStatistics::TypeIsSupported(stats.GetType())) {
		distinct_stats = make_uniq<DistinctStatistics>();
	}
}
ColumnStatistics::ColumnStatistics(BaseStatistics stats_p, unique_ptr<DistinctStatistics> distinct_stats_p)
    : stats(std::move(stats_p)), distinct_stats(std::move(distinct_stats_p)) {
}

shared_ptr<ColumnStatistics> ColumnStatistics::CreateEmptyStats(const LogicalType &type) {
	return make_shared<ColumnStatistics>(BaseStatistics::CreateEmpty(type));
}

void ColumnStatistics::Merge(ColumnStatistics &other) {
	stats.Merge(other.stats);
	if (distinct_stats) {
		distinct_stats->Merge(*other.distinct_stats);
	}
}

BaseStatistics &ColumnStatistics::Statistics() {
	return stats;
}

bool ColumnStatistics::HasDistinctStats() {
	return distinct_stats.get();
}

DistinctStatistics &ColumnStatistics::DistinctStats() {
	if (!distinct_stats) {
		throw InternalException("DistinctStats called without distinct_stats");
	}
	return *distinct_stats;
}

void ColumnStatistics::SetDistinct(unique_ptr<DistinctStatistics> distinct) {
	this->distinct_stats = std::move(distinct);
}

void ColumnStatistics::UpdateDistinctStatistics(Vector &v, idx_t count) {
	if (!distinct_stats) {
		return;
	}
	auto &d_stats = (DistinctStatistics &)*distinct_stats;
	d_stats.Update(v, count);
}

shared_ptr<ColumnStatistics> ColumnStatistics::Copy() const {
	return make_shared<ColumnStatistics>(stats.Copy(), distinct_stats ? distinct_stats->Copy() : nullptr);
}

void ColumnStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "statistics", stats);
	serializer.WritePropertyWithDefault(101, "distinct", distinct_stats, unique_ptr<DistinctStatistics>());
}

shared_ptr<ColumnStatistics> ColumnStatistics::Deserialize(Deserializer &deserializer) {
	auto stats = deserializer.ReadProperty<BaseStatistics>(100, "statistics");
	auto distinct_stats = deserializer.ReadPropertyWithDefault<unique_ptr<DistinctStatistics>>(
	    101, "distinct", unique_ptr<DistinctStatistics>());
	return make_shared<ColumnStatistics>(std::move(stats), std::move(distinct_stats));
}

} // namespace duckdb




#include <math.h>

namespace duckdb {

DistinctStatistics::DistinctStatistics() : log(make_uniq<HyperLogLog>()), sample_count(0), total_count(0) {
}

DistinctStatistics::DistinctStatistics(unique_ptr<HyperLogLog> log, idx_t sample_count, idx_t total_count)
    : log(std::move(log)), sample_count(sample_count), total_count(total_count) {
}

unique_ptr<DistinctStatistics> DistinctStatistics::Copy() const {
	return make_uniq<DistinctStatistics>(log->Copy(), sample_count, total_count);
}

void DistinctStatistics::Merge(const DistinctStatistics &other) {
	log = log->Merge(*other.log);
	sample_count += other.sample_count;
	total_count += other.total_count;
}

void DistinctStatistics::Update(Vector &v, idx_t count, bool sample) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(count, vdata);
	Update(vdata, v.GetType(), count, sample);
}

void DistinctStatistics::Update(UnifiedVectorFormat &vdata, const LogicalType &type, idx_t count, bool sample) {
	if (count == 0) {
		return;
	}

	total_count += count;
	if (sample) {
		count = MinValue<idx_t>(idx_t(SAMPLE_RATE * MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count)), count);
	}
	sample_count += count;

	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];

	HyperLogLog::ProcessEntries(vdata, type, indices, counts, count);
	log->AddToLog(vdata, count, indices, counts);
}

string DistinctStatistics::ToString() const {
	return StringUtil::Format("[Approx Unique: %s]", to_string(GetCount()));
}

idx_t DistinctStatistics::GetCount() const {
	if (sample_count == 0 || total_count == 0) {
		return 0;
	}

	double u = MinValue<idx_t>(log->Count(), sample_count);
	double s = sample_count;
	double n = total_count;

	// Assume this proportion of the the sampled values occurred only once
	double u1 = pow(u / s, 2) * u;

	// Estimate total uniques using Good Turing Estimation
	idx_t estimate = u + u1 / s * (n - s);
	return MinValue<idx_t>(estimate, total_count);
}

bool DistinctStatistics::TypeIsSupported(const LogicalType &type) {
	return type.InternalType() != PhysicalType::LIST && type.InternalType() != PhysicalType::STRUCT;
}

} // namespace duckdb








namespace duckdb {

void ListStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[1]);
	BaseStatistics::Construct(stats.child_stats[0], ListType::GetChildType(stats.GetType()));
}

BaseStatistics ListStats::CreateUnknown(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(child_type));
	return result;
}

BaseStatistics ListStats::CreateEmpty(LogicalType type) {
	auto &child_type = ListType::GetChildType(type);
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(child_type));
	return result;
}

void ListStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	D_ASSERT(stats.child_stats);
	D_ASSERT(other.child_stats);
	stats.child_stats[0].Copy(other.child_stats[0]);
}

const BaseStatistics &ListStats::GetChildStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}
BaseStatistics &ListStats::GetChildStats(BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::LIST_STATS) {
		throw InternalException("ListStats::GetChildStats called on stats that is not a list");
	}
	D_ASSERT(stats.child_stats);
	return stats.child_stats[0];
}

void ListStats::SetChildStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	if (!new_stats) {
		stats.child_stats[0].Copy(BaseStatistics::CreateUnknown(ListType::GetChildType(stats.GetType())));
	} else {
		stats.child_stats[0].Copy(*new_stats);
	}
}

void ListStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}

	auto &child_stats = ListStats::GetChildStats(stats);
	auto &other_child_stats = ListStats::GetChildStats(other);
	child_stats.Merge(other_child_stats);
}

void ListStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &child_stats = ListStats::GetChildStats(stats);
	serializer.WriteProperty(200, "child_stats", child_stats);
}

void ListStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);

	// Push the logical type of the child type to the deserialization context
	deserializer.Set<LogicalType &>(const_cast<LogicalType &>(child_type));
	base.child_stats[0].Copy(deserializer.ReadProperty<BaseStatistics>(200, "child_stats"));
	deserializer.Unset<LogicalType>();
}

string ListStats::ToString(const BaseStatistics &stats) {
	auto &child_stats = ListStats::GetChildStats(stats);
	return StringUtil::Format("[%s]", child_stats.ToString());
}

void ListStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_stats = ListStats::GetChildStats(stats);
	auto &child_entry = ListVector::GetEntry(vector);
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
	idx_t total_list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				total_list_count++;
			}
		}
	}
	SelectionVector list_sel(total_list_count);
	idx_t list_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		auto list = list_data[index];
		if (vdata.validity.RowIsValid(index)) {
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				list_sel.set_index(list_count++, list.offset + list_idx);
			}
		}
	}

	child_stats.Verify(child_entry, list_sel, list_count);
}

} // namespace duckdb








namespace duckdb {

template <>
void NumericStats::Update<interval_t>(BaseStatistics &stats, interval_t new_value) {
}

template <>
void NumericStats::Update<list_entry_t>(BaseStatistics &stats, list_entry_t new_value) {
}

//===--------------------------------------------------------------------===//
// NumericStats
//===--------------------------------------------------------------------===//
BaseStatistics NumericStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	SetMin(result, Value(result.GetType()));
	SetMax(result, Value(result.GetType()));
	return result;
}

BaseStatistics NumericStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	SetMin(result, Value::MaximumValue(result.GetType()));
	SetMax(result, Value::MinimumValue(result.GetType()));
	return result;
}

NumericStatsData &NumericStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::NUMERIC_STATS);
	return stats.stats_union.numeric_data;
}

const NumericStatsData &NumericStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::NUMERIC_STATS);
	return stats.stats_union.numeric_data;
}

void NumericStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	D_ASSERT(stats.GetType() == other.GetType());
	if (NumericStats::HasMin(other) && NumericStats::HasMin(stats)) {
		auto other_min = NumericStats::Min(other);
		if (other_min < NumericStats::Min(stats)) {
			NumericStats::SetMin(stats, other_min);
		}
	} else {
		NumericStats::SetMin(stats, Value());
	}
	if (NumericStats::HasMax(other) && NumericStats::HasMax(stats)) {
		auto other_max = NumericStats::Max(other);
		if (other_max > NumericStats::Max(stats)) {
			NumericStats::SetMax(stats, other_max);
		}
	} else {
		NumericStats::SetMax(stats, Value());
	}
}

struct GetNumericValueUnion {
	template <class T>
	static T Operation(const NumericValueUnion &v);
};

template <>
int8_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.tinyint;
}

template <>
int16_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.smallint;
}

template <>
int32_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.integer;
}

template <>
int64_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.bigint;
}

template <>
hugeint_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.hugeint;
}

template <>
uint8_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.utinyint;
}

template <>
uint16_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.usmallint;
}

template <>
uint32_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.uinteger;
}

template <>
uint64_t GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.ubigint;
}

template <>
float GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.float_;
}

template <>
double GetNumericValueUnion::Operation(const NumericValueUnion &v) {
	return v.value_.double_;
}

template <class T>
T NumericStats::GetMinUnsafe(const BaseStatistics &stats) {
	return GetNumericValueUnion::Operation<T>(NumericStats::GetDataUnsafe(stats).min);
}

template <class T>
T NumericStats::GetMaxUnsafe(const BaseStatistics &stats) {
	return GetNumericValueUnion::Operation<T>(NumericStats::GetDataUnsafe(stats).max);
}

template <class T>
bool ConstantExactRange(T min, T max, T constant) {
	return Equals::Operation(constant, min) && Equals::Operation(constant, max);
}

template <class T>
bool ConstantValueInRange(T min, T max, T constant) {
	return !(LessThan::Operation(constant, min) || GreaterThan::Operation(constant, max));
}

template <class T>
FilterPropagateResult CheckZonemapTemplated(const BaseStatistics &stats, ExpressionType comparison_type,
                                            const Value &constant_value) {
	T min_value = NumericStats::GetMinUnsafe<T>(stats);
	T max_value = NumericStats::GetMaxUnsafe<T>(stats);
	T constant = constant_value.GetValueUnsafe<T>();
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (ConstantExactRange(min_value, max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		if (ConstantValueInRange(min_value, max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	case ExpressionType::COMPARE_NOTEQUAL:
		if (!ConstantValueInRange(min_value, max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (ConstantExactRange(min_value, max_value, constant)) {
			// corner case of a cluster with one numeric equal to the target constant
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// GreaterThanEquals::Operation(X, C)
		// this can be true only if max(X) >= C
		// if min(X) >= C, then this is always true
		if (GreaterThanEquals::Operation(min_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (GreaterThanEquals::Operation(max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// GreaterThan::Operation(X, C)
		// this can be true only if max(X) > C
		// if min(X) > C, then this is always true
		if (GreaterThan::Operation(min_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (GreaterThan::Operation(max_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// LessThanEquals::Operation(X, C)
		// this can be true only if min(X) <= C
		// if max(X) <= C, then this is always true
		if (LessThanEquals::Operation(max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (LessThanEquals::Operation(min_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
		// LessThan::Operation(X, C)
		// this can be true only if min(X) < C
		// if max(X) < C, then this is always true
		if (LessThan::Operation(max_value, constant)) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (LessThan::Operation(min_value, constant)) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type in zonemap check not implemented");
	}
}

FilterPropagateResult NumericStats::CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
                                                 const Value &constant) {
	D_ASSERT(constant.type() == stats.GetType());
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch (stats.GetType().InternalType()) {
	case PhysicalType::INT8:
		return CheckZonemapTemplated<int8_t>(stats, comparison_type, constant);
	case PhysicalType::INT16:
		return CheckZonemapTemplated<int16_t>(stats, comparison_type, constant);
	case PhysicalType::INT32:
		return CheckZonemapTemplated<int32_t>(stats, comparison_type, constant);
	case PhysicalType::INT64:
		return CheckZonemapTemplated<int64_t>(stats, comparison_type, constant);
	case PhysicalType::UINT8:
		return CheckZonemapTemplated<uint8_t>(stats, comparison_type, constant);
	case PhysicalType::UINT16:
		return CheckZonemapTemplated<uint16_t>(stats, comparison_type, constant);
	case PhysicalType::UINT32:
		return CheckZonemapTemplated<uint32_t>(stats, comparison_type, constant);
	case PhysicalType::UINT64:
		return CheckZonemapTemplated<uint64_t>(stats, comparison_type, constant);
	case PhysicalType::INT128:
		return CheckZonemapTemplated<hugeint_t>(stats, comparison_type, constant);
	case PhysicalType::FLOAT:
		return CheckZonemapTemplated<float>(stats, comparison_type, constant);
	case PhysicalType::DOUBLE:
		return CheckZonemapTemplated<double>(stats, comparison_type, constant);
	default:
		throw InternalException("Unsupported type for NumericStats::CheckZonemap");
	}
}

bool NumericStats::IsConstant(const BaseStatistics &stats) {
	return NumericStats::Max(stats) <= NumericStats::Min(stats);
}

void SetNumericValueInternal(const Value &input, const LogicalType &type, NumericValueUnion &val, bool &has_val) {
	if (input.IsNull()) {
		has_val = false;
		return;
	}
	if (input.type().InternalType() != type.InternalType()) {
		throw InternalException("SetMin or SetMax called with Value that does not match statistics' column value");
	}
	has_val = true;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		val.value_.boolean = BooleanValue::Get(input);
		break;
	case PhysicalType::INT8:
		val.value_.tinyint = TinyIntValue::Get(input);
		break;
	case PhysicalType::INT16:
		val.value_.smallint = SmallIntValue::Get(input);
		break;
	case PhysicalType::INT32:
		val.value_.integer = IntegerValue::Get(input);
		break;
	case PhysicalType::INT64:
		val.value_.bigint = BigIntValue::Get(input);
		break;
	case PhysicalType::UINT8:
		val.value_.utinyint = UTinyIntValue::Get(input);
		break;
	case PhysicalType::UINT16:
		val.value_.usmallint = USmallIntValue::Get(input);
		break;
	case PhysicalType::UINT32:
		val.value_.uinteger = UIntegerValue::Get(input);
		break;
	case PhysicalType::UINT64:
		val.value_.ubigint = UBigIntValue::Get(input);
		break;
	case PhysicalType::INT128:
		val.value_.hugeint = HugeIntValue::Get(input);
		break;
	case PhysicalType::FLOAT:
		val.value_.float_ = FloatValue::Get(input);
		break;
	case PhysicalType::DOUBLE:
		val.value_.double_ = DoubleValue::Get(input);
		break;
	default:
		throw InternalException("Unsupported type for NumericStatistics::SetValueInternal");
	}
}

void NumericStats::SetMin(BaseStatistics &stats, const Value &new_min) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	SetNumericValueInternal(new_min, stats.GetType(), data.min, data.has_min);
}

void NumericStats::SetMax(BaseStatistics &stats, const Value &new_max) {
	auto &data = NumericStats::GetDataUnsafe(stats);
	SetNumericValueInternal(new_max, stats.GetType(), data.max, data.has_max);
}

Value NumericValueUnionToValueInternal(const LogicalType &type, const NumericValueUnion &val) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return Value::BOOLEAN(val.value_.boolean);
	case PhysicalType::INT8:
		return Value::TINYINT(val.value_.tinyint);
	case PhysicalType::INT16:
		return Value::SMALLINT(val.value_.smallint);
	case PhysicalType::INT32:
		return Value::INTEGER(val.value_.integer);
	case PhysicalType::INT64:
		return Value::BIGINT(val.value_.bigint);
	case PhysicalType::UINT8:
		return Value::UTINYINT(val.value_.utinyint);
	case PhysicalType::UINT16:
		return Value::USMALLINT(val.value_.usmallint);
	case PhysicalType::UINT32:
		return Value::UINTEGER(val.value_.uinteger);
	case PhysicalType::UINT64:
		return Value::UBIGINT(val.value_.ubigint);
	case PhysicalType::INT128:
		return Value::HUGEINT(val.value_.hugeint);
	case PhysicalType::FLOAT:
		return Value::FLOAT(val.value_.float_);
	case PhysicalType::DOUBLE:
		return Value::DOUBLE(val.value_.double_);
	default:
		throw InternalException("Unsupported type for NumericValueUnionToValue");
	}
}

Value NumericValueUnionToValue(const LogicalType &type, const NumericValueUnion &val) {
	Value result = NumericValueUnionToValueInternal(type, val);
	result.GetTypeMutable() = type;
	return result;
}

bool NumericStats::HasMinMax(const BaseStatistics &stats) {
	return NumericStats::HasMin(stats) && NumericStats::HasMax(stats);
}

bool NumericStats::HasMin(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return NumericStats::GetDataUnsafe(stats).has_min;
}

bool NumericStats::HasMax(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return NumericStats::GetDataUnsafe(stats).has_max;
}

Value NumericStats::Min(const BaseStatistics &stats) {
	if (!NumericStats::HasMin(stats)) {
		throw InternalException("Min() called on statistics that does not have min");
	}
	return NumericValueUnionToValue(stats.GetType(), NumericStats::GetDataUnsafe(stats).min);
}

Value NumericStats::Max(const BaseStatistics &stats) {
	if (!NumericStats::HasMax(stats)) {
		throw InternalException("Max() called on statistics that does not have max");
	}
	return NumericValueUnionToValue(stats.GetType(), NumericStats::GetDataUnsafe(stats).max);
}

Value NumericStats::MinOrNull(const BaseStatistics &stats) {
	if (!NumericStats::HasMin(stats)) {
		return Value(stats.GetType());
	}
	return NumericStats::Min(stats);
}

Value NumericStats::MaxOrNull(const BaseStatistics &stats) {
	if (!NumericStats::HasMax(stats)) {
		return Value(stats.GetType());
	}
	return NumericStats::Max(stats);
}

static void SerializeNumericStatsValue(const LogicalType &type, NumericValueUnion val, bool has_value,
                                       Serializer &serializer) {
	serializer.WriteProperty(100, "has_value", has_value);
	if (!has_value) {
		return;
	}
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		serializer.WriteProperty(101, "value", val.value_.boolean);
		break;
	case PhysicalType::INT8:
		serializer.WriteProperty(101, "value", val.value_.tinyint);
		break;
	case PhysicalType::INT16:
		serializer.WriteProperty(101, "value", val.value_.smallint);
		break;
	case PhysicalType::INT32:
		serializer.WriteProperty(101, "value", val.value_.integer);
		break;
	case PhysicalType::INT64:
		serializer.WriteProperty(101, "value", val.value_.bigint);
		break;
	case PhysicalType::UINT8:
		serializer.WriteProperty(101, "value", val.value_.utinyint);
		break;
	case PhysicalType::UINT16:
		serializer.WriteProperty(101, "value", val.value_.usmallint);
		break;
	case PhysicalType::UINT32:
		serializer.WriteProperty(101, "value", val.value_.uinteger);
		break;
	case PhysicalType::UINT64:
		serializer.WriteProperty(101, "value", val.value_.ubigint);
		break;
	case PhysicalType::INT128:
		serializer.WriteProperty(101, "value", val.value_.hugeint);
		break;
	case PhysicalType::FLOAT:
		serializer.WriteProperty(101, "value", val.value_.float_);
		break;
	case PhysicalType::DOUBLE:
		serializer.WriteProperty(101, "value", val.value_.double_);
		break;
	default:
		throw InternalException("Unsupported type for serializing numeric statistics");
	}
}

static void DeserializeNumericStatsValue(const LogicalType &type, NumericValueUnion &result, bool &has_stats,
                                         Deserializer &deserializer) {
	auto has_value = deserializer.ReadProperty<bool>(100, "has_value");
	if (!has_value) {
		has_stats = false;
		return;
	}
	has_stats = true;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.value_.boolean = deserializer.ReadProperty<bool>(101, "value");
		break;
	case PhysicalType::INT8:
		result.value_.tinyint = deserializer.ReadProperty<int8_t>(101, "value");
		break;
	case PhysicalType::INT16:
		result.value_.smallint = deserializer.ReadProperty<int16_t>(101, "value");
		break;
	case PhysicalType::INT32:
		result.value_.integer = deserializer.ReadProperty<int32_t>(101, "value");
		break;
	case PhysicalType::INT64:
		result.value_.bigint = deserializer.ReadProperty<int64_t>(101, "value");
		break;
	case PhysicalType::UINT8:
		result.value_.utinyint = deserializer.ReadProperty<uint8_t>(101, "value");
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = deserializer.ReadProperty<uint16_t>(101, "value");
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = deserializer.ReadProperty<uint32_t>(101, "value");
		break;
	case PhysicalType::UINT64:
		result.value_.ubigint = deserializer.ReadProperty<uint64_t>(101, "value");
		break;
	case PhysicalType::INT128:
		result.value_.hugeint = deserializer.ReadProperty<hugeint_t>(101, "value");
		break;
	case PhysicalType::FLOAT:
		result.value_.float_ = deserializer.ReadProperty<float>(101, "value");
		break;
	case PhysicalType::DOUBLE:
		result.value_.double_ = deserializer.ReadProperty<double>(101, "value");
		break;
	default:
		throw InternalException("Unsupported type for serializing numeric statistics");
	}
}

void NumericStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &numeric_stats = NumericStats::GetDataUnsafe(stats);
	serializer.WriteObject(200, "max", [&](Serializer &object) {
		SerializeNumericStatsValue(stats.GetType(), numeric_stats.min, numeric_stats.has_min, object);
	});
	serializer.WriteObject(201, "min", [&](Serializer &object) {
		SerializeNumericStatsValue(stats.GetType(), numeric_stats.max, numeric_stats.has_max, object);
	});
}

void NumericStats::Deserialize(Deserializer &deserializer, BaseStatistics &result) {
	auto &numeric_stats = NumericStats::GetDataUnsafe(result);

	deserializer.ReadObject(200, "max", [&](Deserializer &object) {
		DeserializeNumericStatsValue(result.GetType(), numeric_stats.min, numeric_stats.has_min, object);
	});
	deserializer.ReadObject(201, "min", [&](Deserializer &object) {
		DeserializeNumericStatsValue(result.GetType(), numeric_stats.max, numeric_stats.has_max, object);
	});
}

string NumericStats::ToString(const BaseStatistics &stats) {
	return StringUtil::Format("[Min: %s, Max: %s]", NumericStats::MinOrNull(stats).ToString(),
	                          NumericStats::MaxOrNull(stats).ToString());
}

template <class T>
void NumericStats::TemplatedVerify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel,
                                   idx_t count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	auto min_value = NumericStats::MinOrNull(stats);
	auto max_value = NumericStats::MaxOrNull(stats);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min_value.IsNull() && LessThan::Operation(data[index], min_value.GetValueUnsafe<T>())) { // LCOV_EXCL_START
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		} // LCOV_EXCL_STOP
		if (!max_value.IsNull() && GreaterThan::Operation(data[index], max_value.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
	}
}

void NumericStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &type = stats.GetType();
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedVerify<uint8_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedVerify<uint16_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedVerify<uint32_t>(stats, vector, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedVerify<uint64_t>(stats, vector, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(stats, vector, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(stats, vector, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(stats, vector, sel, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

} // namespace duckdb


namespace duckdb {

template <>
bool &NumericValueUnion::GetReferenceUnsafe() {
	return value_.boolean;
}

template <>
int8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.tinyint;
}

template <>
int16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.smallint;
}

template <>
int32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.integer;
}

template <>
int64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.bigint;
}

template <>
hugeint_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.hugeint;
}

template <>
uint8_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.utinyint;
}

template <>
uint16_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.usmallint;
}

template <>
uint32_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.uinteger;
}

template <>
uint64_t &NumericValueUnion::GetReferenceUnsafe() {
	return value_.ubigint;
}

template <>
float &NumericValueUnion::GetReferenceUnsafe() {
	return value_.float_;
}

template <>
double &NumericValueUnion::GetReferenceUnsafe() {
	return value_.double_;
}

} // namespace duckdb




namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type) : statistics(BaseStatistics::CreateEmpty(std::move(type))) {
}

SegmentStatistics::SegmentStatistics(BaseStatistics stats) : statistics(std::move(stats)) {
}

} // namespace duckdb











namespace duckdb {

BaseStatistics StringStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	auto &string_data = StringStats::GetDataUnsafe(result);
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		string_data.min[i] = 0;
		string_data.max[i] = 0xFF;
	}
	string_data.max_string_length = 0;
	string_data.has_max_string_length = false;
	string_data.has_unicode = true;
	return result;
}

BaseStatistics StringStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	auto &string_data = StringStats::GetDataUnsafe(result);
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		string_data.min[i] = 0xFF;
		string_data.max[i] = 0;
	}
	string_data.max_string_length = 0;
	string_data.has_max_string_length = true;
	string_data.has_unicode = false;
	return result;
}

StringStatsData &StringStats::GetDataUnsafe(BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRING_STATS);
	return stats.stats_union.string_data;
}

const StringStatsData &StringStats::GetDataUnsafe(const BaseStatistics &stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRING_STATS);
	return stats.stats_union.string_data;
}

bool StringStats::HasMaxStringLength(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return false;
	}
	return StringStats::GetDataUnsafe(stats).has_max_string_length;
}

uint32_t StringStats::MaxStringLength(const BaseStatistics &stats) {
	if (!HasMaxStringLength(stats)) {
		throw InternalException("MaxStringLength called on statistics that does not have a max string length");
	}
	return StringStats::GetDataUnsafe(stats).max_string_length;
}

bool StringStats::CanContainUnicode(const BaseStatistics &stats) {
	if (stats.GetType().id() == LogicalTypeId::SQLNULL) {
		return true;
	}
	return StringStats::GetDataUnsafe(stats).has_unicode;
}

string GetStringMinMaxValue(const data_t data[]) {
	idx_t len;
	for (len = 0; len < StringStatsData::MAX_STRING_MINMAX_SIZE; len++) {
		if (!data[len]) {
			break;
		}
	}
	return string(const_char_ptr_cast(data), len);
}

string StringStats::Min(const BaseStatistics &stats) {
	return GetStringMinMaxValue(StringStats::GetDataUnsafe(stats).min);
}

string StringStats::Max(const BaseStatistics &stats) {
	return GetStringMinMaxValue(StringStats::GetDataUnsafe(stats).max);
}

void StringStats::ResetMaxStringLength(BaseStatistics &stats) {
	StringStats::GetDataUnsafe(stats).has_max_string_length = false;
}

void StringStats::SetContainsUnicode(BaseStatistics &stats) {
	StringStats::GetDataUnsafe(stats).has_unicode = true;
}

void StringStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	serializer.WriteProperty(200, "min", string_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(201, "max", string_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	serializer.WriteProperty(202, "has_unicode", string_data.has_unicode);
	serializer.WriteProperty(203, "has_max_string_length", string_data.has_max_string_length);
	serializer.WriteProperty(204, "max_string_length", string_data.max_string_length);
}

void StringStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &string_data = StringStats::GetDataUnsafe(base);
	deserializer.ReadProperty(200, "min", string_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(201, "max", string_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	deserializer.ReadProperty(202, "has_unicode", string_data.has_unicode);
	deserializer.ReadProperty(203, "has_max_string_length", string_data.has_max_string_length);
	deserializer.ReadProperty(204, "max_string_length", string_data.max_string_length);
}

static int StringValueComparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
	D_ASSERT(len <= StringStatsData::MAX_STRING_MINMAX_SIZE);
	for (idx_t i = 0; i < len; i++) {
		if (data[i] < comparison[i]) {
			return -1;
		} else if (data[i] > comparison[i]) {
			return 1;
		}
	}
	return 0;
}

static void ConstructValue(const_data_ptr_t data, idx_t size, data_t target[]) {
	idx_t value_size = size > StringStatsData::MAX_STRING_MINMAX_SIZE ? StringStatsData::MAX_STRING_MINMAX_SIZE : size;
	memcpy(target, data, value_size);
	for (idx_t i = value_size; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		target[i] = '\0';
	}
}

void StringStats::Update(BaseStatistics &stats, const string_t &value) {
	auto data = const_data_ptr_cast(value.GetData());
	auto size = value.GetSize();

	//! we can only fit 8 bytes, so we might need to trim our string
	// construct the value
	data_t target[StringStatsData::MAX_STRING_MINMAX_SIZE];
	ConstructValue(data, size, target);

	// update the min and max
	auto &string_data = StringStats::GetDataUnsafe(stats);
	if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.min) < 0) {
		memcpy(string_data.min, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.max) > 0) {
		memcpy(string_data.max, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (size > string_data.max_string_length) {
		string_data.max_string_length = size;
	}
	if (stats.GetType().id() == LogicalTypeId::VARCHAR && !string_data.has_unicode) {
		auto unicode = Utf8Proc::Analyze(const_char_ptr_cast(data), size);
		if (unicode == UnicodeType::UNICODE) {
			string_data.has_unicode = true;
		} else if (unicode == UnicodeType::INVALID) {
			throw InvalidInputException(ErrorManager::InvalidUnicodeError(string(const_char_ptr_cast(data), size),
			                                                              "segment statistics update"));
		}
	}
}

void StringStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	auto &string_data = StringStats::GetDataUnsafe(stats);
	auto &other_data = StringStats::GetDataUnsafe(other);
	if (StringValueComparison(other_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.min) < 0) {
		memcpy(string_data.min, other_data.min, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	if (StringValueComparison(other_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE, string_data.max) > 0) {
		memcpy(string_data.max, other_data.max, StringStatsData::MAX_STRING_MINMAX_SIZE);
	}
	string_data.has_unicode = string_data.has_unicode || other_data.has_unicode;
	string_data.has_max_string_length = string_data.has_max_string_length && other_data.has_max_string_length;
	string_data.max_string_length = MaxValue<uint32_t>(string_data.max_string_length, other_data.max_string_length);
}

FilterPropagateResult StringStats::CheckZonemap(const BaseStatistics &stats, ExpressionType comparison_type,
                                                const string &constant) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	auto data = const_data_ptr_cast(constant.c_str());
	auto size = constant.size();

	idx_t value_size = size > StringStatsData::MAX_STRING_MINMAX_SIZE ? StringStatsData::MAX_STRING_MINMAX_SIZE : size;
	int min_comp = StringValueComparison(data, value_size, string_data.min);
	int max_comp = StringValueComparison(data, value_size, string_data.max);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (min_comp >= 0 && max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
		if (min_comp < 0 || max_comp > 0) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
		if (max_comp <= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (min_comp >= 0) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type not implemented for string statistics zone map");
	}
}

static idx_t GetValidMinMaxSubstring(const_data_ptr_t data) {
	for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
		if (data[i] == '\0') {
			return i;
		}
		if ((data[i] & 0x80) != 0) {
			return i;
		}
	}
	return StringStatsData::MAX_STRING_MINMAX_SIZE;
}

string StringStats::ToString(const BaseStatistics &stats) {
	auto &string_data = StringStats::GetDataUnsafe(stats);
	idx_t min_len = GetValidMinMaxSubstring(string_data.min);
	idx_t max_len = GetValidMinMaxSubstring(string_data.max);
	return StringUtil::Format("[Min: %s, Max: %s, Has Unicode: %s, Max String Length: %s]",
	                          string(const_char_ptr_cast(string_data.min), min_len),
	                          string(const_char_ptr_cast(string_data.max), max_len),
	                          string_data.has_unicode ? "true" : "false",
	                          string_data.has_max_string_length ? to_string(string_data.max_string_length) : "?");
}

void StringStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &string_data = StringStats::GetDataUnsafe(stats);

	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		auto value = data[index];
		auto data = value.GetData();
		auto len = value.GetSize();
		// LCOV_EXCL_START
		if (string_data.has_max_string_length && len > string_data.max_string_length) {
			throw InternalException(
			    "Statistics mismatch: string value exceeds maximum string length.\nStatistics: %s\nVector: %s",
			    stats.ToString(), vector.ToString(count));
		}
		if (stats.GetType().id() == LogicalTypeId::VARCHAR && !string_data.has_unicode) {
			auto unicode = Utf8Proc::Analyze(data, len);
			if (unicode == UnicodeType::UNICODE) {
				throw InternalException("Statistics mismatch: string value contains unicode, but statistics says it "
				                        "shouldn't.\nStatistics: %s\nVector: %s",
				                        stats.ToString(), vector.ToString(count));
			} else if (unicode == UnicodeType::INVALID) {
				throw InternalException("Invalid unicode detected in vector: %s", vector.ToString(count));
			}
		}
		if (StringValueComparison(const_data_ptr_cast(data),
		                          MinValue<idx_t>(len, StringStatsData::MAX_STRING_MINMAX_SIZE), string_data.min) < 0) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
		if (StringValueComparison(const_data_ptr_cast(data),
		                          MinValue<idx_t>(len, StringStatsData::MAX_STRING_MINMAX_SIZE), string_data.max) > 0) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        stats.ToString(), vector.ToString(count));
		}
		// LCOV_EXCL_STOP
	}
}

} // namespace duckdb







namespace duckdb {

void StructStats::Construct(BaseStatistics &stats) {
	auto &child_types = StructType::GetChildTypes(stats.GetType());
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[child_types.size()]);
	for (idx_t i = 0; i < child_types.size(); i++) {
		BaseStatistics::Construct(stats.child_stats[i], child_types[i].second);
	}
}

BaseStatistics StructStats::CreateUnknown(LogicalType type) {
	auto &child_types = StructType::GetChildTypes(type);
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	for (idx_t i = 0; i < child_types.size(); i++) {
		result.child_stats[i].Copy(BaseStatistics::CreateUnknown(child_types[i].second));
	}
	return result;
}

BaseStatistics StructStats::CreateEmpty(LogicalType type) {
	auto &child_types = StructType::GetChildTypes(type);
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	for (idx_t i = 0; i < child_types.size(); i++) {
		result.child_stats[i].Copy(BaseStatistics::CreateEmpty(child_types[i].second));
	}
	return result;
}

const BaseStatistics *StructStats::GetChildStats(const BaseStatistics &stats) {
	if (stats.GetStatsType() != StatisticsType::STRUCT_STATS) {
		throw InternalException("Calling StructStats::GetChildStats on stats that is not a struct");
	}
	return stats.child_stats.get();
}

const BaseStatistics &StructStats::GetChildStats(const BaseStatistics &stats, idx_t i) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (i >= StructType::GetChildCount(stats.GetType())) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return stats.child_stats[i];
}

BaseStatistics &StructStats::GetChildStats(BaseStatistics &stats, idx_t i) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (i >= StructType::GetChildCount(stats.GetType())) {
		throw InternalException("Calling StructStats::GetChildStats but there are no stats for this index");
	}
	return stats.child_stats[i];
}

void StructStats::SetChildStats(BaseStatistics &stats, idx_t i, const BaseStatistics &new_stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	D_ASSERT(i < StructType::GetChildCount(stats.GetType()));
	stats.child_stats[i].Copy(new_stats);
}

void StructStats::SetChildStats(BaseStatistics &stats, idx_t i, unique_ptr<BaseStatistics> new_stats) {
	D_ASSERT(stats.GetStatsType() == StatisticsType::STRUCT_STATS);
	if (!new_stats) {
		StructStats::SetChildStats(stats, i,
		                           BaseStatistics::CreateUnknown(StructType::GetChildType(stats.GetType(), i)));
	} else {
		StructStats::SetChildStats(stats, i, *new_stats);
	}
}

void StructStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	auto count = StructType::GetChildCount(stats.GetType());
	for (idx_t i = 0; i < count; i++) {
		stats.child_stats[i].Copy(other.child_stats[i]);
	}
}

void StructStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	D_ASSERT(stats.GetType() == other.GetType());
	auto child_count = StructType::GetChildCount(stats.GetType());
	for (idx_t i = 0; i < child_count; i++) {
		stats.child_stats[i].Merge(other.child_stats[i]);
	}
}

void StructStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto child_stats = StructStats::GetChildStats(stats);
	auto child_count = StructType::GetChildCount(stats.GetType());

	serializer.WriteList(200, "child_stats", child_count,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(child_stats[i]); });
}

void StructStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);

	auto &child_types = StructType::GetChildTypes(type);

	deserializer.ReadList(200, "child_stats", [&](Deserializer::List &list, idx_t i) {
		deserializer.Set<LogicalType &>(const_cast<LogicalType &>(child_types[i].second));
		auto stat = list.ReadElement<BaseStatistics>();
		base.child_stats[i].Copy(stat);
		deserializer.Unset<LogicalType>();
	});
}

string StructStats::ToString(const BaseStatistics &stats) {
	string result;
	result += " {";
	auto &child_types = StructType::GetChildTypes(stats.GetType());
	for (idx_t i = 0; i < child_types.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += child_types[i].first + ": " + stats.child_stats[i].ToString();
	}
	result += "}";
	return result;
}

void StructStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		stats.child_stats[i].Verify(*child_entries[i], sel, count);
	}
}

} // namespace duckdb


namespace duckdb {

const uint64_t VERSION_NUMBER = 64;

struct StorageVersionInfo {
	const char *version_name;
	idx_t storage_version;
};

static StorageVersionInfo storage_version_info[] = {{"v0.8.0 or v0.8.1", 51},
                                                    {"v0.7.0 or v0.7.1", 43},
                                                    {"v0.6.0 or v0.6.1", 39},
                                                    {"v0.5.0 or v0.5.1", 38},
                                                    {"v0.3.3, v0.3.4 or v0.4.0", 33},
                                                    {"v0.3.2", 31},
                                                    {"v0.3.1", 27},
                                                    {"v0.3.0", 25},
                                                    {"v0.2.9", 21},
                                                    {"v0.2.8", 18},
                                                    {"v0.2.7", 17},
                                                    {"v0.2.6", 15},
                                                    {"v0.2.5", 13},
                                                    {"v0.2.4", 11},
                                                    {"v0.2.3", 6},
                                                    {"v0.2.2", 4},
                                                    {"v0.2.1 and prior", 1},
                                                    {nullptr, 0}};

const char *GetDuckDBVersion(idx_t version_number) {
	for (idx_t i = 0; storage_version_info[i].version_name; i++) {
		if (version_number == storage_version_info[i].storage_version) {
			return storage_version_info[i].version_name;
		}
	}
	return nullptr;
}

} // namespace duckdb




namespace duckdb {

StorageLockKey::StorageLockKey(StorageLock &lock, StorageLockType type) : lock(lock), type(type) {
}

StorageLockKey::~StorageLockKey() {
	if (type == StorageLockType::EXCLUSIVE) {
		lock.ReleaseExclusiveLock();
	} else {
		D_ASSERT(type == StorageLockType::SHARED);
		lock.ReleaseSharedLock();
	}
}

StorageLock::StorageLock() : read_count(0) {
}

unique_ptr<StorageLockKey> StorageLock::GetExclusiveLock() {
	exclusive_lock.lock();
	while (read_count != 0) {
	}
	return make_uniq<StorageLockKey>(*this, StorageLockType::EXCLUSIVE);
}

unique_ptr<StorageLockKey> StorageLock::GetSharedLock() {
	exclusive_lock.lock();
	read_count++;
	exclusive_lock.unlock();
	return make_uniq<StorageLockKey>(*this, StorageLockType::SHARED);
}

void StorageLock::ReleaseExclusiveLock() {
	exclusive_lock.unlock();
}

void StorageLock::ReleaseSharedLock() {
	read_count--;
}

} // namespace duckdb
















namespace duckdb {

StorageManager::StorageManager(AttachedDatabase &db, string path_p, bool read_only)
    : db(db), path(std::move(path_p)), read_only(read_only) {
	if (path.empty()) {
		path = ":memory:";
	} else {
		auto &fs = FileSystem::Get(db);
		this->path = fs.ExpandPath(path);
	}
}

StorageManager::~StorageManager() {
}

StorageManager &StorageManager::Get(AttachedDatabase &db) {
	return db.GetStorageManager();
}
StorageManager &StorageManager::Get(Catalog &catalog) {
	return StorageManager::Get(catalog.GetAttached());
}

DatabaseInstance &StorageManager::GetDatabase() {
	return db.GetDatabase();
}

BufferManager &BufferManager::GetBufferManager(ClientContext &context) {
	return BufferManager::GetBufferManager(*context.db);
}

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

bool ObjectCache::ObjectCacheEnabled(ClientContext &context) {
	return context.db->config.options.object_cache_enable;
}

bool StorageManager::InMemory() {
	D_ASSERT(!path.empty());
	return path == ":memory:";
}

void StorageManager::Initialize() {
	bool in_memory = InMemory();
	if (in_memory && read_only) {
		throw CatalogException("Cannot launch in-memory database in read-only mode!");
	}

	// create or load the database from disk, if not in-memory mode
	LoadDatabase();
}

///////////////////////////////////////////////////////////////////////////
class SingleFileTableIOManager : public TableIOManager {
public:
	explicit SingleFileTableIOManager(BlockManager &block_manager) : block_manager(block_manager) {
	}

	BlockManager &block_manager;

public:
	BlockManager &GetIndexBlockManager() override {
		return block_manager;
	}
	BlockManager &GetBlockManagerForRowData() override {
		return block_manager;
	}
	MetadataManager &GetMetadataManager() override {
		return block_manager.GetMetadataManager();
	}
};

SingleFileStorageManager::SingleFileStorageManager(AttachedDatabase &db, string path, bool read_only)
    : StorageManager(db, std::move(path), read_only) {
}

void SingleFileStorageManager::LoadDatabase() {
	if (InMemory()) {
		block_manager = make_uniq<InMemoryBlockManager>(BufferManager::GetBufferManager(db));
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);
		return;
	}
	std::size_t question_mark_pos = path.find('?');
	auto wal_path = path;
	if (question_mark_pos != std::string::npos) {
		wal_path.insert(question_mark_pos, ".wal");
	} else {
		wal_path += ".wal";
	}
	auto &fs = FileSystem::Get(db);
	auto &config = DBConfig::Get(db);
	bool truncate_wal = false;
	if (!config.options.enable_external_access) {
		if (!db.IsInitialDatabase()) {
			throw PermissionException("Attaching on-disk databases is disabled through configuration");
		}
	}

	StorageManagerOptions options;
	options.read_only = read_only;
	options.use_direct_io = config.options.use_direct_io;
	options.debug_initialize = config.options.debug_initialize;
	// first check if the database exists
	if (!fs.FileExists(path)) {
		if (read_only) {
			throw CatalogException("Cannot open database \"%s\" in read-only mode: database does not exist", path);
		}
		// check if the WAL exists
		if (fs.FileExists(wal_path)) {
			// WAL file exists but database file does not
			// remove the WAL
			fs.RemoveFile(wal_path);
		}
		// initialize the block manager while creating a new db file
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->CreateNewDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);
	} else {
		// initialize the block manager while loading the current db file
		auto sf_block_manager = make_uniq<SingleFileBlockManager>(db, path, options);
		sf_block_manager->LoadExistingDatabase();
		block_manager = std::move(sf_block_manager);
		table_io_manager = make_uniq<SingleFileTableIOManager>(*block_manager);

		//! Load from storage
		auto checkpointer = SingleFileCheckpointReader(*this);
		checkpointer.LoadFromStorage();
		// check if the WAL file exists
		if (fs.FileExists(wal_path)) {
			// replay the WAL
			truncate_wal = WriteAheadLog::Replay(db, wal_path);
		}
	}
	// initialize the WAL file
	if (!read_only) {
		wal = make_uniq<WriteAheadLog>(db, wal_path);
		if (truncate_wal) {
			wal->Truncate(0);
		}
	}
}

///////////////////////////////////////////////////////////////////////////////

class SingleFileStorageCommitState : public StorageCommitState {
	idx_t initial_wal_size = 0;
	idx_t initial_written = 0;
	optional_ptr<WriteAheadLog> log;
	bool checkpoint;

public:
	SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint);
	~SingleFileStorageCommitState() override {
		// If log is non-null, then commit threw an exception before flushing.
		if (log) {
			auto &wal = *log.get();
			wal.skip_writing = false;
			if (wal.GetTotalWritten() > initial_written) {
				// remove any entries written into the WAL by truncating it
				wal.Truncate(initial_wal_size);
			}
		}
	}

	// Make the commit persistent
	void FlushCommit() override;
};

SingleFileStorageCommitState::SingleFileStorageCommitState(StorageManager &storage_manager, bool checkpoint)
    : checkpoint(checkpoint) {
	log = storage_manager.GetWriteAheadLog();
	if (log) {
		auto initial_size = log->GetWALSize();
		initial_written = log->GetTotalWritten();
		initial_wal_size = initial_size < 0 ? 0 : idx_t(initial_size);

		if (checkpoint) {
			// check if we are checkpointing after this commit
			// if we are checkpointing, we don't need to write anything to the WAL
			// this saves us a lot of unnecessary writes to disk in the case of large commits
			log->skip_writing = true;
		}
	} else {
		D_ASSERT(!checkpoint);
	}
}

// Make the commit persistent
void SingleFileStorageCommitState::FlushCommit() {
	if (log) {
		// flush the WAL if any changes were made
		if (log->GetTotalWritten() > initial_written) {
			(void)checkpoint;
			D_ASSERT(!checkpoint);
			D_ASSERT(!log->skip_writing);
			log->Flush();
		}
		log->skip_writing = false;
	}
	// Null so that the destructor will not truncate the log.
	log = nullptr;
}

unique_ptr<StorageCommitState> SingleFileStorageManager::GenStorageCommitState(Transaction &transaction,
                                                                               bool checkpoint) {
	return make_uniq<SingleFileStorageCommitState>(*this, checkpoint);
}

bool SingleFileStorageManager::IsCheckpointClean(MetaBlockPointer checkpoint_id) {
	return block_manager->IsRootBlock(checkpoint_id);
}

void SingleFileStorageManager::CreateCheckpoint(bool delete_wal, bool force_checkpoint) {
	if (InMemory() || read_only || !wal) {
		return;
	}
	auto &config = DBConfig::Get(db);
	if (wal->GetWALSize() > 0 || config.options.force_checkpoint || force_checkpoint) {
		// we only need to checkpoint if there is anything in the WAL
		try {
			SingleFileCheckpointWriter checkpointer(db, *block_manager);
			checkpointer.CreateCheckpoint();
		} catch (std::exception &ex) {
			throw FatalException("Failed to create checkpoint because of error: %s", ex.what());
		}
	}
	if (delete_wal) {
		wal->Delete();
		wal.reset();
	}
}

DatabaseSize SingleFileStorageManager::GetDatabaseSize() {
	// All members default to zero
	DatabaseSize ds;
	if (!InMemory()) {
		ds.total_blocks = block_manager->TotalBlocks();
		ds.block_size = Storage::BLOCK_ALLOC_SIZE;
		ds.free_blocks = block_manager->FreeBlocks();
		ds.used_blocks = ds.total_blocks - ds.free_blocks;
		ds.bytes = (ds.total_blocks * ds.block_size);
		if (auto wal = GetWriteAheadLog()) {
			ds.wal_size = wal->GetWALSize();
		}
	}
	return ds;
}

vector<MetadataBlockInfo> SingleFileStorageManager::GetMetadataInfo() {
	auto &metadata_manager = block_manager->GetMetadataManager();
	return metadata_manager.GetMetadataInfo();
}

bool SingleFileStorageManager::AutomaticCheckpoint(idx_t estimated_wal_bytes) {
	auto log = GetWriteAheadLog();
	if (!log) {
		return false;
	}

	auto &config = DBConfig::Get(db);
	auto initial_size = log->GetWALSize();
	idx_t expected_wal_size = initial_size + estimated_wal_bytes;
	return expected_wal_size > config.options.checkpoint_wal_size;
}

shared_ptr<TableIOManager> SingleFileStorageManager::GetTableIOManager(BoundCreateTableInfo *info /*info*/) {
	// This is an unmanaged reference. No ref/deref overhead. Lifetime of the
	// TableIoManager follows lifetime of the StorageManager (this).
	return shared_ptr<TableIOManager>(shared_ptr<char>(nullptr), table_io_manager.get());
}

} // namespace duckdb






namespace duckdb {

struct TransactionVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return id < start_time || id == transaction_id;
	}

	static bool UseDeletedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return !UseInsertedVersion(start_time, transaction_id, id);
	}
};

struct CommittedVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return true;
	}

	static bool UseDeletedVersion(transaction_t min_start_time, transaction_t min_transaction_id, transaction_t id) {
		return (id >= min_start_time && id < TRANSACTION_ID_START) || (id >= min_transaction_id);
	}
};

static bool UseVersion(TransactionData transaction, transaction_t id) {
	return TransactionVersionOperator::UseInsertedVersion(transaction.start_time, transaction.transaction_id, id);
}

void ChunkInfo::Write(WriteStream &writer) const {
	writer.Write<ChunkInfoType>(type);
}

unique_ptr<ChunkInfo> ChunkInfo::Read(ReadStream &reader) {
	auto type = reader.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO:
		return ChunkConstantInfo::Read(reader);
	case ChunkInfoType::VECTOR_INFO:
		return ChunkVectorInfo::Read(reader);
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

//===--------------------------------------------------------------------===//
// Constant info
//===--------------------------------------------------------------------===//
ChunkConstantInfo::ChunkConstantInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::CONSTANT_INFO), insert_id(0), delete_id(NOT_DELETED_ID) {
}

template <class OP>
idx_t ChunkConstantInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) const {
	if (OP::UseInsertedVersion(start_time, transaction_id, insert_id) &&
	    OP::UseDeletedVersion(start_time, transaction_id, delete_id)) {
		return max_count;
	}
	return 0;
}

idx_t ChunkConstantInfo::GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<TransactionVersionOperator>(transaction.start_time, transaction.transaction_id,
	                                                         sel_vector, max_count);
}

idx_t ChunkConstantInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

bool ChunkConstantInfo::Fetch(TransactionData transaction, row_t row) {
	return UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id);
}

void ChunkConstantInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	D_ASSERT(start == 0 && end == STANDARD_VECTOR_SIZE);
	insert_id = commit_id;
}

bool ChunkConstantInfo::HasDeletes() const {
	bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id < TRANSACTION_ID_START;
	return is_deleted;
}

idx_t ChunkConstantInfo::GetCommittedDeletedCount(idx_t max_count) {
	return delete_id < TRANSACTION_ID_START ? max_count : 0;
}

void ChunkConstantInfo::Write(WriteStream &writer) const {
	D_ASSERT(HasDeletes());
	ChunkInfo::Write(writer);
	writer.Write<idx_t>(start);
}

unique_ptr<ChunkInfo> ChunkConstantInfo::Read(ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto info = make_uniq<ChunkConstantInfo>(start);
	info->insert_id = 0;
	info->delete_id = 0;
	return std::move(info);
}

//===--------------------------------------------------------------------===//
// Vector info
//===--------------------------------------------------------------------===//
ChunkVectorInfo::ChunkVectorInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::VECTOR_INFO), insert_id(0), same_inserted_id(true), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = 0;
		deleted[i] = NOT_DELETED_ID;
	}
}

template <class OP>
idx_t ChunkVectorInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) const {
	idx_t count = 0;
	if (same_inserted_id && !any_deleted) {
		// all tuples have the same inserted id: and no tuples were deleted
		if (OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return max_count;
		} else {
			return 0;
		}
	} else if (same_inserted_id) {
		if (!OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return 0;
		}
		// have to check deleted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else if (!any_deleted) {
		// have to check inserted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else {
		// have to check both flags
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i]) &&
			    OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	}
	return count;
}

idx_t ChunkVectorInfo::GetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
                                    idx_t max_count) const {
	return TemplatedGetSelVector<TransactionVersionOperator>(start_time, transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) {
	return GetSelVector(transaction.start_time, transaction.transaction_id, sel_vector, max_count);
}

bool ChunkVectorInfo::Fetch(TransactionData transaction, row_t row) {
	return UseVersion(transaction, inserted[row]) && !UseVersion(transaction, deleted[row]);
}

idx_t ChunkVectorInfo::Delete(transaction_t transaction_id, row_t rows[], idx_t count) {
	any_deleted = true;

	idx_t deleted_tuples = 0;
	for (idx_t i = 0; i < count; i++) {
		if (deleted[rows[i]] == transaction_id) {
			continue;
		}
		// first check the chunk for conflicts
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction
			throw TransactionException("Conflict on tuple deletion!");
		}
		// after verifying that there are no conflicts we mark the tuple as deleted
		deleted[rows[i]] = transaction_id;
		rows[deleted_tuples] = rows[i];
		deleted_tuples++;
	}
	return deleted_tuples;
}

void ChunkVectorInfo::CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		deleted[rows[i]] = commit_id;
	}
}

void ChunkVectorInfo::Append(idx_t start, idx_t end, transaction_t commit_id) {
	if (start == 0) {
		insert_id = commit_id;
	} else if (insert_id != commit_id) {
		same_inserted_id = false;
		insert_id = NOT_DELETED_ID;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

void ChunkVectorInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	if (same_inserted_id) {
		insert_id = commit_id;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

bool ChunkVectorInfo::HasDeletes() const {
	return any_deleted;
}

idx_t ChunkVectorInfo::GetCommittedDeletedCount(idx_t max_count) {
	if (!any_deleted) {
		return 0;
	}
	idx_t delete_count = 0;
	for (idx_t i = 0; i < max_count; i++) {
		if (deleted[i] < TRANSACTION_ID_START) {
			delete_count++;
		}
	}
	return delete_count;
}

void ChunkVectorInfo::Write(WriteStream &writer) const {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t start_time = TRANSACTION_ID_START - 1;
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(start_time, transaction_id, sel, STANDARD_VECTOR_SIZE);
	if (count == STANDARD_VECTOR_SIZE) {
		// nothing is deleted: skip writing anything
		writer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	if (count == 0) {
		// everything is deleted: write a constant vector
		writer.Write<ChunkInfoType>(ChunkInfoType::CONSTANT_INFO);
		writer.Write<idx_t>(start);
		return;
	}
	// write a boolean vector
	ChunkInfo::Write(writer);
	writer.Write<idx_t>(start);
	ValidityMask mask(STANDARD_VECTOR_SIZE);
	mask.Initialize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		mask.SetInvalid(sel.get_index(i));
	}
	mask.Write(writer, STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkInfo> ChunkVectorInfo::Read(ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto result = make_uniq<ChunkVectorInfo>(start);
	result->any_deleted = true;
	ValidityMask mask;
	mask.Read(reader, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (mask.RowIsValid(i)) {
			result->deleted[i] = 0;
		}
	}
	return std::move(result);
}

} // namespace duckdb









namespace duckdb {

ColumnCheckpointState::ColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
                                             PartialBlockManager &partial_block_manager)
    : row_group(row_group), column_data(column_data), partial_block_manager(partial_block_manager) {
}

ColumnCheckpointState::~ColumnCheckpointState() {
}

unique_ptr<BaseStatistics> ColumnCheckpointState::GetStatistics() {
	D_ASSERT(global_stats);
	return std::move(global_stats);
}

PartialBlockForCheckpoint::PartialBlockForCheckpoint(ColumnData &data, ColumnSegment &segment, PartialBlockState state,
                                                     BlockManager &block_manager)
    : PartialBlock(state, block_manager, segment.block) {
	AddSegmentToTail(data, segment, 0);
}

PartialBlockForCheckpoint::~PartialBlockForCheckpoint() {
	D_ASSERT(IsFlushed() || Exception::UncaughtException());
}

bool PartialBlockForCheckpoint::IsFlushed() {
	// segments are cleared on Flush
	return segments.empty();
}

void PartialBlockForCheckpoint::Flush(const idx_t free_space_left) {

	if (IsFlushed()) {
		throw InternalException("Flush called on partial block that was already flushed");
	}

	// zero-initialize unused memory
	FlushInternal(free_space_left);

	// At this point, we've already copied all data from tail_segments
	// into the page owned by first_segment. We flush all segment data to
	// disk with the following call.
	// persist the first segment to disk and point the remaining segments to the same block
	bool fetch_new_block = state.block_id == INVALID_BLOCK;
	if (fetch_new_block) {
		state.block_id = block_manager.GetFreeBlockId();
	}

	for (idx_t i = 0; i < segments.size(); i++) {
		auto &segment = segments[i];
		segment.data.IncrementVersion();
		if (i == 0) {
			// the first segment is converted to persistent - this writes the data for ALL segments to disk
			D_ASSERT(segment.offset_in_block == 0);
			segment.segment.ConvertToPersistent(&block_manager, state.block_id);
			// update the block after it has been converted to a persistent segment
			block_handle = segment.segment.block;
		} else {
			// subsequent segments are MARKED as persistent - they don't need to be rewritten
			segment.segment.MarkAsPersistent(block_handle, segment.offset_in_block);
			if (fetch_new_block) {
				// if we fetched a new block we need to increase the reference count to the block
				block_manager.IncreaseBlockReferenceCount(state.block_id);
			}
		}
	}

	Clear();
}

void PartialBlockForCheckpoint::Merge(PartialBlock &other_p, idx_t offset, idx_t other_size) {
	auto &other = other_p.Cast<PartialBlockForCheckpoint>();

	auto &buffer_manager = block_manager.buffer_manager;
	// pin the source block
	auto old_handle = buffer_manager.Pin(other.block_handle);
	// pin the target block
	auto new_handle = buffer_manager.Pin(block_handle);
	// memcpy the contents of the old block to the new block
	memcpy(new_handle.Ptr() + offset, old_handle.Ptr(), other_size);

	// now copy over all segments to the new block
	// move over the uninitialized regions
	for (auto &region : other.uninitialized_regions) {
		region.start += offset;
		region.end += offset;
		uninitialized_regions.push_back(region);
	}

	// move over the segments
	for (auto &segment : other.segments) {
		AddSegmentToTail(segment.data, segment.segment, segment.offset_in_block + offset);
	}

	other.Clear();
}

void PartialBlockForCheckpoint::AddSegmentToTail(ColumnData &data, ColumnSegment &segment, uint32_t offset_in_block) {
	segments.emplace_back(data, segment, offset_in_block);
}

void PartialBlockForCheckpoint::Clear() {
	uninitialized_regions.clear();
	block_handle.reset();
	segments.clear();
}

void ColumnCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, idx_t segment_size) {
	D_ASSERT(segment_size <= Storage::BLOCK_SIZE);
	auto tuple_count = segment->count.load();
	if (tuple_count == 0) { // LCOV_EXCL_START
		return;
	} // LCOV_EXCL_STOP

	// merge the segment stats into the global stats
	global_stats->Merge(segment->stats.statistics);

	// get the buffer of the segment and pin it
	auto &db = column_data.GetDatabase();
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	block_id_t block_id = INVALID_BLOCK;
	uint32_t offset_in_block = 0;

	if (!segment->stats.statistics.IsConstant()) {
		// non-constant block
		PartialBlockAllocation allocation = partial_block_manager.GetBlockAllocation(segment_size);
		block_id = allocation.state.block_id;
		offset_in_block = allocation.state.offset;

		if (allocation.partial_block) {
			// Use an existing block.
			D_ASSERT(offset_in_block > 0);
			auto &pstate = allocation.partial_block->Cast<PartialBlockForCheckpoint>();
			// pin the source block
			auto old_handle = buffer_manager.Pin(segment->block);
			// pin the target block
			auto new_handle = buffer_manager.Pin(pstate.block_handle);
			// memcpy the contents of the old block to the new block
			memcpy(new_handle.Ptr() + offset_in_block, old_handle.Ptr(), segment_size);
			pstate.AddSegmentToTail(column_data, *segment, offset_in_block);
		} else {
			// Create a new block for future reuse.
			if (segment->SegmentSize() != Storage::BLOCK_SIZE) {
				// the segment is smaller than the block size
				// allocate a new block and copy the data over
				D_ASSERT(segment->SegmentSize() < Storage::BLOCK_SIZE);
				segment->Resize(Storage::BLOCK_SIZE);
			}
			D_ASSERT(offset_in_block == 0);
			allocation.partial_block = make_uniq<PartialBlockForCheckpoint>(column_data, *segment, allocation.state,
			                                                                *allocation.block_manager);
		}
		// Writer will decide whether to reuse this block.
		partial_block_manager.RegisterPartialBlock(std::move(allocation));
	} else {
		// constant block: no need to write anything to disk besides the stats
		// set up the compression function to constant
		auto &config = DBConfig::GetConfig(db);
		segment->function =
		    *config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, segment->type.InternalType());
		segment->ConvertToPersistent(nullptr, INVALID_BLOCK);
	}

	// construct the data pointer
	DataPointer data_pointer(segment->stats.statistics.Copy());
	data_pointer.block_pointer.block_id = block_id;
	data_pointer.block_pointer.offset = offset_in_block;
	data_pointer.row_start = row_group.start;
	if (!data_pointers.empty()) {
		auto &last_pointer = data_pointers.back();
		data_pointer.row_start = last_pointer.row_start + last_pointer.tuple_count;
	}
	data_pointer.tuple_count = tuple_count;
	data_pointer.compression_type = segment->function.get().type;
	if (segment->function.get().serialize_state) {
		data_pointer.segment_state = segment->function.get().serialize_state(*segment);
	}

	// append the segment to the new segment tree
	new_tree.AppendSegment(std::move(segment));
	data_pointers.push_back(std::move(data_pointer));
}

void ColumnCheckpointState::WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) {
	writer.WriteColumnDataPointers(*this, serializer);
}

} // namespace duckdb




















namespace duckdb {

ColumnData::ColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                       LogicalType type_p, optional_ptr<ColumnData> parent)
    : start(start_row), count(0), block_manager(block_manager), info(info), column_index(column_index),
      type(std::move(type_p)), parent(parent), version(0) {
	if (!parent) {
		stats = make_uniq<SegmentStatistics>(type);
	}
}

ColumnData::~ColumnData() {
}

void ColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	idx_t offset = 0;
	for (auto &segment : data.Segments()) {
		segment.start = start + offset;
		offset += segment.count;
	}
	data.Reinitialize();
}

DatabaseInstance &ColumnData::GetDatabase() const {
	return info.db.GetDatabase();
}

DataTableInfo &ColumnData::GetTableInfo() const {
	return info;
}

const LogicalType &ColumnData::RootType() const {
	if (parent) {
		return parent->RootType();
	}
	return type;
}

void ColumnData::IncrementVersion() {
	version++;
}

idx_t ColumnData::GetMaxEntry() {
	return count;
}

void ColumnData::InitializeScan(ColumnScanState &state) {
	state.current = data.GetRootSegment();
	state.segment_tree = &data;
	state.row_index = state.current ? state.current->start : 0;
	state.internal_index = state.row_index;
	state.initialized = false;
	state.version = version;
	state.scan_state.reset();
	state.last_offset = 0;
}

void ColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	state.current = data.GetSegment(row_idx);
	state.segment_tree = &data;
	state.row_index = row_idx;
	state.internal_index = state.current->start;
	state.initialized = false;
	state.version = version;
	state.scan_state.reset();
	state.last_offset = 0;
}

idx_t ColumnData::ScanVector(ColumnScanState &state, Vector &result, idx_t remaining, bool has_updates) {
	state.previous_states.clear();
	if (state.version != version) {
		InitializeScanWithOffset(state, state.row_index);
		state.current->InitializeScan(state);
		state.initialized = true;
	} else if (!state.initialized) {
		D_ASSERT(state.current);
		state.current->InitializeScan(state);
		state.internal_index = state.current->start;
		state.initialized = true;
	}
	D_ASSERT(data.HasSegment(state.current));
	D_ASSERT(state.version == version);
	D_ASSERT(state.internal_index <= state.row_index);
	if (state.internal_index < state.row_index) {
		state.current->Skip(state);
	}
	D_ASSERT(state.current->type == type);
	idx_t initial_remaining = remaining;
	while (remaining > 0) {
		D_ASSERT(state.row_index >= state.current->start &&
		         state.row_index <= state.current->start + state.current->count);
		idx_t scan_count = MinValue<idx_t>(remaining, state.current->start + state.current->count - state.row_index);
		idx_t result_offset = initial_remaining - remaining;
		if (scan_count > 0) {
			state.current->Scan(state, scan_count, result, result_offset,
			                    !has_updates && scan_count == initial_remaining);

			state.row_index += scan_count;
			remaining -= scan_count;
		}

		if (remaining > 0) {
			auto next = data.GetNextSegment(state.current);
			if (!next) {
				break;
			}
			state.previous_states.emplace_back(std::move(state.scan_state));
			state.current = next;
			state.current->InitializeScan(state);
			state.segment_checked = false;
			D_ASSERT(state.row_index >= state.current->start &&
			         state.row_index <= state.current->start + state.current->count);
		}
	}
	state.internal_index = state.row_index;
	return initial_remaining - remaining;
}

template <bool SCAN_COMMITTED, bool ALLOW_UPDATES>
idx_t ColumnData::ScanVector(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	bool has_updates;
	{
		lock_guard<mutex> update_guard(update_lock);
		has_updates = updates ? true : false;
	}
	auto scan_count = ScanVector(state, result, STANDARD_VECTOR_SIZE, has_updates);
	if (has_updates) {
		lock_guard<mutex> update_guard(update_lock);
		if (!ALLOW_UPDATES && updates->HasUncommittedUpdates(vector_index)) {
			throw TransactionException("Cannot create index with outstanding updates");
		}
		result.Flatten(scan_count);
		if (SCAN_COMMITTED) {
			updates->FetchCommitted(vector_index, result);
		} else {
			updates->FetchUpdates(transaction, vector_index, result);
		}
	}
	return scan_count;
}

template idx_t ColumnData::ScanVector<false, false>(TransactionData transaction, idx_t vector_index,
                                                    ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, false>(TransactionData transaction, idx_t vector_index,
                                                   ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<false, true>(TransactionData transaction, idx_t vector_index,
                                                   ColumnScanState &state, Vector &result);
template idx_t ColumnData::ScanVector<true, true>(TransactionData transaction, idx_t vector_index,
                                                  ColumnScanState &state, Vector &result);

idx_t ColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanVector<false, true>(transaction, vector_index, state, result);
}

idx_t ColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	if (allow_updates) {
		return ScanVector<true, true>(TransactionData(0, 0), vector_index, state, result);
	} else {
		return ScanVector<true, false>(TransactionData(0, 0), vector_index, state, result);
	}
}

void ColumnData::ScanCommittedRange(idx_t row_group_start, idx_t offset_in_row_group, idx_t count, Vector &result) {
	ColumnScanState child_state;
	InitializeScanWithOffset(child_state, row_group_start + offset_in_row_group);
	auto scan_count = ScanVector(child_state, result, count, updates ? true : false);
	if (updates) {
		result.Flatten(scan_count);
		updates->FetchCommittedRange(offset_in_row_group, count, result);
	}
}

idx_t ColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// ScanCount can only be used if there are no updates
	D_ASSERT(!updates);
	return ScanVector(state, result, count, false);
}

void ColumnData::Select(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                        SelectionVector &sel, idx_t &count, const TableFilter &filter) {
	idx_t scan_count = Scan(transaction, vector_index, state, result);
	result.Flatten(scan_count);
	ColumnSegment::FilterSelection(sel, result, filter, count, FlatVector::Validity(result));
}

void ColumnData::FilterScan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result,
                            SelectionVector &sel, idx_t count) {
	Scan(transaction, vector_index, state, result);
	result.Slice(sel, count);
}

void ColumnData::FilterScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, SelectionVector &sel,
                                     idx_t count, bool allow_updates) {
	ScanCommitted(vector_index, state, result, allow_updates);
	result.Slice(sel, count);
}

void ColumnData::Skip(ColumnScanState &state, idx_t count) {
	state.Next(count);
}

void ColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	AppendData(stats, state, vdata, count);
}

void ColumnData::Append(ColumnAppendState &state, Vector &vector, idx_t count) {
	if (parent || !stats) {
		throw InternalException("ColumnData::Append called on a column with a parent or without stats");
	}
	Append(stats->statistics, state, vector, count);
}

bool ColumnData::CheckZonemap(TableFilter &filter) {
	if (!stats) {
		throw InternalException("ColumnData::CheckZonemap called on a column without stats");
	}
	auto propagate_result = filter.CheckStatistics(stats->statistics);
	if (propagate_result == FilterPropagateResult::FILTER_ALWAYS_FALSE ||
	    propagate_result == FilterPropagateResult::FILTER_FALSE_OR_NULL) {
		return false;
	}
	return true;
}

unique_ptr<BaseStatistics> ColumnData::GetStatistics() {
	if (!stats) {
		throw InternalException("ColumnData::GetStatistics called on a column without stats");
	}
	return stats->statistics.ToUnique();
}

void ColumnData::MergeStatistics(const BaseStatistics &other) {
	if (!stats) {
		throw InternalException("ColumnData::MergeStatistics called on a column without stats");
	}
	return stats->statistics.Merge(other);
}

void ColumnData::MergeIntoStatistics(BaseStatistics &other) {
	if (!stats) {
		throw InternalException("ColumnData::MergeIntoStatistics called on a column without stats");
	}
	return other.Merge(stats->statistics);
}

void ColumnData::InitializeAppend(ColumnAppendState &state) {
	auto l = data.Lock();
	if (data.IsEmpty(l)) {
		// no segments yet, append an empty segment
		AppendTransientSegment(l, start);
	}
	auto segment = data.GetLastSegment(l);
	if (segment->segment_type == ColumnSegmentType::PERSISTENT || !segment->function.get().init_append) {
		// we cannot append to this segment - append a new segment
		auto total_rows = segment->start + segment->count;
		AppendTransientSegment(l, total_rows);
		state.current = data.GetLastSegment(l);
	} else {
		state.current = segment;
	}

	D_ASSERT(state.current->segment_type == ColumnSegmentType::TRANSIENT);
	state.current->InitializeAppend(state);
	D_ASSERT(state.current->function.get().append);
}

void ColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata, idx_t count) {
	idx_t offset = 0;
	this->count += count;
	while (true) {
		// append the data from the vector
		idx_t copied_elements = state.current->Append(state, vdata, offset, count);
		stats.Merge(state.current->stats.statistics);
		if (copied_elements == count) {
			// finished copying everything
			break;
		}

		// we couldn't fit everything we wanted in the current column segment, create a new one
		{
			auto l = data.Lock();
			AppendTransientSegment(l, state.current->start + state.current->count);
			state.current = data.GetLastSegment(l);
			state.current->InitializeAppend(state);
		}
		offset += copied_elements;
		count -= copied_elements;
	}
}

void ColumnData::RevertAppend(row_t start_row) {
	auto l = data.Lock();
	// check if this row is in the segment tree at all
	auto last_segment = data.GetLastSegment(l);
	if (idx_t(start_row) >= last_segment->start + last_segment->count) {
		// the start row is equal to the final portion of the column data: nothing was ever appended here
		D_ASSERT(idx_t(start_row) == last_segment->start + last_segment->count);
		return;
	}
	// find the segment index that the current row belongs to
	idx_t segment_index = data.GetSegmentIndex(l, start_row);
	auto segment = data.GetSegmentByIndex(l, segment_index);
	auto &transient = *segment;
	D_ASSERT(transient.segment_type == ColumnSegmentType::TRANSIENT);

	// remove any segments AFTER this segment: they should be deleted entirely
	data.EraseSegments(l, segment_index);

	this->count = start_row - this->start;
	segment->next = nullptr;
	transient.RevertAppend(start_row);
}

idx_t ColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	D_ASSERT(row_id >= 0);
	D_ASSERT(idx_t(row_id) >= start);
	// perform the fetch within the segment
	state.row_index = start + ((row_id - start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
	state.current = data.GetSegment(state.row_index);
	state.internal_index = state.current->start;
	return ScanVector(state, result, STANDARD_VECTOR_SIZE, false);
}

void ColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                          idx_t result_idx) {
	auto segment = data.GetSegment(row_id);

	// now perform the fetch within the segment
	segment->FetchRow(state, row_id, result, result_idx);
	// merge any updates made to this row
	lock_guard<mutex> update_guard(update_lock);
	if (updates) {
		updates->FetchRow(transaction, row_id, result, result_idx);
	}
}

void ColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                        idx_t update_count) {
	lock_guard<mutex> update_guard(update_lock);
	if (!updates) {
		updates = make_uniq<UpdateSegment>(*this);
	}
	Vector base_vector(type);
	ColumnScanState state;
	auto fetch_count = Fetch(state, row_ids[0], base_vector);

	base_vector.Flatten(fetch_count);
	updates->Update(transaction, column_index, update_vector, row_ids, update_count, base_vector);
}

void ColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path, Vector &update_vector,
                              row_t *row_ids, idx_t update_count, idx_t depth) {
	// this method should only be called at the end of the path in the base column case
	D_ASSERT(depth >= column_path.size());
	ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
}

unique_ptr<BaseStatistics> ColumnData::GetUpdateStatistics() {
	lock_guard<mutex> update_guard(update_lock);
	return updates ? updates->GetStatistics() : nullptr;
}

void ColumnData::AppendTransientSegment(SegmentLock &l, idx_t start_row) {
	idx_t segment_size = Storage::BLOCK_SIZE;
	if (start_row == idx_t(MAX_ROW_ID)) {
#if STANDARD_VECTOR_SIZE < 1024
		segment_size = 1024 * GetTypeIdSize(type.InternalType());
#else
		segment_size = STANDARD_VECTOR_SIZE * GetTypeIdSize(type.InternalType());
#endif
	}
	auto new_segment = ColumnSegment::CreateTransientSegment(GetDatabase(), type, start_row, segment_size);
	data.AppendSegment(l, std::move(new_segment));
}

void ColumnData::CommitDropColumn() {
	for (auto &segment_p : data.Segments()) {
		auto &segment = segment_p;
		segment.CommitDropSegment();
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                    PartialBlockManager &partial_block_manager) {
	return make_uniq<ColumnCheckpointState>(row_group, *this, partial_block_manager);
}

void ColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start, idx_t count,
                                Vector &scan_vector) {
	segment.Scan(state, count, scan_vector, 0, true);
	if (updates) {
		scan_vector.Flatten(count);
		updates->FetchCommittedRange(state.row_index - row_group_start, count, scan_vector);
	}
}

unique_ptr<ColumnCheckpointState> ColumnData::Checkpoint(RowGroup &row_group,
                                                         PartialBlockManager &partial_block_manager,
                                                         ColumnCheckpointInfo &checkpoint_info) {
	// scan the segments of the column data
	// set up the checkpoint state
	auto checkpoint_state = CreateCheckpointState(row_group, partial_block_manager);
	checkpoint_state->global_stats = BaseStatistics::CreateEmpty(type).ToUnique();

	auto l = data.Lock();
	auto nodes = data.MoveSegments(l);
	if (nodes.empty()) {
		// empty table: flush the empty list
		return checkpoint_state;
	}
	lock_guard<mutex> update_guard(update_lock);

	ColumnDataCheckpointer checkpointer(*this, row_group, *checkpoint_state, checkpoint_info);
	checkpointer.Checkpoint(std::move(nodes));

	// replace the old tree with the new one
	data.Replace(l, checkpoint_state->new_tree);
	version++;

	return checkpoint_state;
}

void ColumnData::DeserializeColumn(Deserializer &deserializer) {
	// load the data pointers for the column
	deserializer.Set<DatabaseInstance &>(info.db.GetDatabase());
	deserializer.Set<LogicalType &>(type);

	vector<DataPointer> data_pointers;
	deserializer.ReadProperty(100, "data_pointers", data_pointers);

	deserializer.Unset<DatabaseInstance>();
	deserializer.Unset<LogicalType>();

	// construct the segments based on the data pointers
	this->count = 0;
	for (auto &data_pointer : data_pointers) {
		// Update the count and statistics
		this->count += data_pointer.tuple_count;
		if (stats) {
			stats->statistics.Merge(data_pointer.statistics);
		}

		// create a persistent segment
		auto segment = ColumnSegment::CreatePersistentSegment(
		    GetDatabase(), block_manager, data_pointer.block_pointer.block_id, data_pointer.block_pointer.offset, type,
		    data_pointer.row_start, data_pointer.tuple_count, data_pointer.compression_type,
		    std::move(data_pointer.statistics), std::move(data_pointer.segment_state));

		data.AppendSegment(std::move(segment));
	}
}

shared_ptr<ColumnData> ColumnData::Deserialize(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                               idx_t start_row, ReadStream &source, const LogicalType &type,
                                               optional_ptr<ColumnData> parent) {
	auto entry = ColumnData::CreateColumn(block_manager, info, column_index, start_row, type, parent);
	BinaryDeserializer deserializer(source);
	deserializer.Begin();
	entry->DeserializeColumn(deserializer);
	deserializer.End();
	return entry;
}

void ColumnData::GetColumnSegmentInfo(idx_t row_group_index, vector<idx_t> col_path,
                                      vector<ColumnSegmentInfo> &result) {
	D_ASSERT(!col_path.empty());

	// convert the column path to a string
	string col_path_str = "[";
	for (idx_t i = 0; i < col_path.size(); i++) {
		if (i > 0) {
			col_path_str += ", ";
		}
		col_path_str += to_string(col_path[i]);
	}
	col_path_str += "]";

	// iterate over the segments
	idx_t segment_idx = 0;
	auto segment = (ColumnSegment *)data.GetRootSegment();
	while (segment) {
		ColumnSegmentInfo column_info;
		column_info.row_group_index = row_group_index;
		column_info.column_id = col_path[0];
		column_info.column_path = col_path_str;
		column_info.segment_idx = segment_idx;
		column_info.segment_type = type.ToString();
		column_info.segment_start = segment->start;
		column_info.segment_count = segment->count;
		column_info.compression_type = CompressionTypeToString(segment->function.get().type);
		column_info.segment_stats = segment->stats.statistics.ToString();
		{
			lock_guard<mutex> ulock(update_lock);
			column_info.has_updates = updates ? true : false;
		}
		// persistent
		// block_id
		// block_offset
		if (segment->segment_type == ColumnSegmentType::PERSISTENT) {
			column_info.persistent = true;
			column_info.block_id = segment->GetBlockId();
			column_info.block_offset = segment->GetBlockOffset();
		} else {
			column_info.persistent = false;
		}
		auto segment_state = segment->GetSegmentState();
		if (segment_state) {
			column_info.segment_info = segment_state->GetSegmentInfo();
		}
		result.emplace_back(column_info);

		segment_idx++;
		segment = data.GetNextSegment(segment);
	}
}

void ColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	D_ASSERT(this->start == parent.start);
	data.Verify();
	if (type.InternalType() == PhysicalType::STRUCT) {
		// structs don't have segments
		D_ASSERT(!data.GetRootSegment());
		return;
	}
	idx_t current_index = 0;
	idx_t current_start = this->start;
	idx_t total_count = 0;
	for (auto &segment : data.Segments()) {
		D_ASSERT(segment.index == current_index);
		D_ASSERT(segment.start == current_start);
		current_start += segment.count;
		total_count += segment.count;
		current_index++;
	}
	D_ASSERT(this->count == total_count);
#endif
}

template <class RET, class OP>
static RET CreateColumnInternal(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                                const LogicalType &type, optional_ptr<ColumnData> parent) {
	if (type.InternalType() == PhysicalType::STRUCT) {
		return OP::template Create<StructColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.InternalType() == PhysicalType::LIST) {
		return OP::template Create<ListColumnData>(block_manager, info, column_index, start_row, type, parent);
	} else if (type.id() == LogicalTypeId::VALIDITY) {
		return OP::template Create<ValidityColumnData>(block_manager, info, column_index, start_row, *parent);
	}
	return OP::template Create<StandardColumnData>(block_manager, info, column_index, start_row, type, parent);
}

shared_ptr<ColumnData> ColumnData::CreateColumn(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                                idx_t start_row, const LogicalType &type,
                                                optional_ptr<ColumnData> parent) {
	return CreateColumnInternal<shared_ptr<ColumnData>, SharedConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

unique_ptr<ColumnData> ColumnData::CreateColumnUnique(BlockManager &block_manager, DataTableInfo &info,
                                                      idx_t column_index, idx_t start_row, const LogicalType &type,
                                                      optional_ptr<ColumnData> parent) {
	return CreateColumnInternal<unique_ptr<ColumnData>, UniqueConstructor>(block_manager, info, column_index, start_row,
	                                                                       type, parent);
}

} // namespace duckdb







namespace duckdb {

ColumnDataCheckpointer::ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p,
                                               ColumnCheckpointState &state_p, ColumnCheckpointInfo &checkpoint_info_p)
    : col_data(col_data_p), row_group(row_group_p), state(state_p),
      is_validity(GetType().id() == LogicalTypeId::VALIDITY),
      intermediate(is_validity ? LogicalType::BOOLEAN : GetType(), true, is_validity),
      checkpoint_info(checkpoint_info_p) {
	auto &config = DBConfig::GetConfig(GetDatabase());
	auto functions = config.GetCompressionFunctions(GetType().InternalType());
	for (auto &func : functions) {
		compression_functions.push_back(&func.get());
	}
}

DatabaseInstance &ColumnDataCheckpointer::GetDatabase() {
	return col_data.GetDatabase();
}

const LogicalType &ColumnDataCheckpointer::GetType() const {
	return col_data.type;
}

ColumnData &ColumnDataCheckpointer::GetColumnData() {
	return col_data;
}

RowGroup &ColumnDataCheckpointer::GetRowGroup() {
	return row_group;
}

ColumnCheckpointState &ColumnDataCheckpointer::GetCheckpointState() {
	return state;
}

void ColumnDataCheckpointer::ScanSegments(const std::function<void(Vector &, idx_t)> &callback) {
	Vector scan_vector(intermediate.GetType(), nullptr);
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto &segment = *nodes[segment_idx].node;
		ColumnScanState scan_state;
		scan_state.current = &segment;
		segment.InitializeScan(scan_state);

		for (idx_t base_row_index = 0; base_row_index < segment.count; base_row_index += STANDARD_VECTOR_SIZE) {
			scan_vector.Reference(intermediate);

			idx_t count = MinValue<idx_t>(segment.count - base_row_index, STANDARD_VECTOR_SIZE);
			scan_state.row_index = segment.start + base_row_index;

			col_data.CheckpointScan(segment, scan_state, row_group.start, count, scan_vector);

			callback(scan_vector, count);
		}
	}
}

CompressionType ForceCompression(vector<optional_ptr<CompressionFunction>> &compression_functions,
                                 CompressionType compression_type) {
	// On of the force_compression flags has been set
	// check if this compression method is available
	bool found = false;
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		auto &compression_function = *compression_functions[i];
		if (compression_function.type == compression_type) {
			found = true;
			break;
		}
	}
	if (found) {
		// the force_compression method is available
		// clear all other compression methods
		// except the uncompressed method, so we can fall back on that
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			auto &compression_function = *compression_functions[i];
			if (compression_function.type == CompressionType::COMPRESSION_UNCOMPRESSED) {
				continue;
			}
			if (compression_function.type != compression_type) {
				compression_functions[i] = nullptr;
			}
		}
	}
	return found ? compression_type : CompressionType::COMPRESSION_AUTO;
}

unique_ptr<AnalyzeState> ColumnDataCheckpointer::DetectBestCompressionMethod(idx_t &compression_idx) {
	D_ASSERT(!compression_functions.empty());
	auto &config = DBConfig::GetConfig(GetDatabase());
	CompressionType forced_method = CompressionType::COMPRESSION_AUTO;

	auto compression_type = checkpoint_info.compression_type;
	if (compression_type != CompressionType::COMPRESSION_AUTO) {
		forced_method = ForceCompression(compression_functions, compression_type);
	}
	if (compression_type == CompressionType::COMPRESSION_AUTO &&
	    config.options.force_compression != CompressionType::COMPRESSION_AUTO) {
		forced_method = ForceCompression(compression_functions, config.options.force_compression);
	}
	// set up the analyze states for each compression method
	vector<unique_ptr<AnalyzeState>> analyze_states;
	analyze_states.reserve(compression_functions.size());
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			analyze_states.push_back(nullptr);
			continue;
		}
		analyze_states.push_back(compression_functions[i]->init_analyze(col_data, col_data.type.InternalType()));
	}

	// scan over all the segments and run the analyze step
	ScanSegments([&](Vector &scan_vector, idx_t count) {
		for (idx_t i = 0; i < compression_functions.size(); i++) {
			if (!compression_functions[i]) {
				continue;
			}
			auto success = compression_functions[i]->analyze(*analyze_states[i], scan_vector, count);
			if (!success) {
				// could not use this compression function on this data set
				// erase it
				compression_functions[i] = nullptr;
				analyze_states[i].reset();
			}
		}
	});

	// now that we have passed over all the data, we need to figure out the best method
	// we do this using the final_analyze method
	unique_ptr<AnalyzeState> state;
	compression_idx = DConstants::INVALID_INDEX;
	idx_t best_score = NumericLimits<idx_t>::Maximum();
	for (idx_t i = 0; i < compression_functions.size(); i++) {
		if (!compression_functions[i]) {
			continue;
		}
		//! Check if the method type is the forced method (if forced is used)
		bool forced_method_found = compression_functions[i]->type == forced_method;
		auto score = compression_functions[i]->final_analyze(*analyze_states[i]);

		//! The finalize method can return this value from final_analyze to indicate it should not be used.
		if (score == DConstants::INVALID_INDEX) {
			continue;
		}

		if (score < best_score || forced_method_found) {
			compression_idx = i;
			best_score = score;
			state = std::move(analyze_states[i]);
		}
		//! If we have found the forced method, we're done
		if (forced_method_found) {
			break;
		}
	}
	return state;
}

void ColumnDataCheckpointer::WriteToDisk() {
	// there were changes or transient segments
	// we need to rewrite the column segments to disk

	// first we check the current segments
	// if there are any persistent segments, we will mark their old block ids as modified
	// since the segments will be rewritten their old on disk data is no longer required
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		segment->CommitDropSegment();
	}

	// now we need to write our segment
	// we will first run an analyze step that determines which compression function to use
	idx_t compression_idx;
	auto analyze_state = DetectBestCompressionMethod(compression_idx);

	if (!analyze_state) {
		throw FatalException("No suitable compression/storage method found to store column");
	}

	// now that we have analyzed the compression functions we can start writing to disk
	auto best_function = compression_functions[compression_idx];
	auto compress_state = best_function->init_compression(*this, std::move(analyze_state));
	ScanSegments(
	    [&](Vector &scan_vector, idx_t count) { best_function->compress(*compress_state, scan_vector, count); });
	best_function->compress_finalize(*compress_state);

	nodes.clear();
}

bool ColumnDataCheckpointer::HasChanges() {
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		if (segment->segment_type == ColumnSegmentType::TRANSIENT) {
			// transient segment: always need to write to disk
			return true;
		} else {
			// persistent segment; check if there were any updates or deletions in this segment
			idx_t start_row_idx = segment->start - row_group.start;
			idx_t end_row_idx = start_row_idx + segment->count;
			if (col_data.updates && col_data.updates->HasUpdates(start_row_idx, end_row_idx)) {
				return true;
			}
		}
	}
	return false;
}

void ColumnDataCheckpointer::WritePersistentSegments() {
	// all segments are persistent and there are no updates
	// we only need to write the metadata
	for (idx_t segment_idx = 0; segment_idx < nodes.size(); segment_idx++) {
		auto segment = nodes[segment_idx].node.get();
		D_ASSERT(segment->segment_type == ColumnSegmentType::PERSISTENT);

		// set up the data pointer directly using the data from the persistent segment
		DataPointer pointer(segment->stats.statistics.Copy());
		pointer.block_pointer.block_id = segment->GetBlockId();
		pointer.block_pointer.offset = segment->GetBlockOffset();
		pointer.row_start = segment->start;
		pointer.tuple_count = segment->count;
		pointer.compression_type = segment->function.get().type;
		if (segment->function.get().serialize_state) {
			pointer.segment_state = segment->function.get().serialize_state(*segment);
		}

		// merge the persistent stats into the global column stats
		state.global_stats->Merge(segment->stats.statistics);

		// directly append the current segment to the new tree
		state.new_tree.AppendSegment(std::move(nodes[segment_idx].node));

		state.data_pointers.push_back(std::move(pointer));
	}
}

void ColumnDataCheckpointer::Checkpoint(vector<SegmentNode<ColumnSegment>> nodes_p) {
	D_ASSERT(!nodes_p.empty());
	this->nodes = std::move(nodes_p);
	// first check if any of the segments have changes
	if (!HasChanges()) {
		// no changes: only need to write the metadata for this column
		WritePersistentSegments();
	} else {
		// there are changes: rewrite the set of columns);
		WriteToDisk();
	}
}

CompressionFunction &ColumnDataCheckpointer::GetCompressionFunction(CompressionType compression_type) {
	auto &db = GetDatabase();
	auto &column_type = GetType();
	auto &config = DBConfig::GetConfig(db);
	return *config.GetCompressionFunction(compression_type, column_type.InternalType());
}

} // namespace duckdb













#include <cstring>

namespace duckdb {

unique_ptr<ColumnSegment> ColumnSegment::CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
                                                                 block_id_t block_id, idx_t offset,
                                                                 const LogicalType &type, idx_t start, idx_t count,
                                                                 CompressionType compression_type,
                                                                 BaseStatistics statistics,
                                                                 unique_ptr<ColumnSegmentState> segment_state) {
	auto &config = DBConfig::GetConfig(db);
	optional_ptr<CompressionFunction> function;
	shared_ptr<BlockHandle> block;
	if (block_id == INVALID_BLOCK) {
		// constant segment, no need to allocate an actual block
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CONSTANT, type.InternalType());
	} else {
		function = config.GetCompressionFunction(compression_type, type.InternalType());
		block = block_manager.RegisterBlock(block_id);
	}
	auto segment_size = Storage::BLOCK_SIZE;
	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::PERSISTENT, start, count, *function,
	                                std::move(statistics), block_id, offset, segment_size, std::move(segment_state));
}

unique_ptr<ColumnSegment> ColumnSegment::CreateTransientSegment(DatabaseInstance &db, const LogicalType &type,
                                                                idx_t start, idx_t segment_size) {
	auto &config = DBConfig::GetConfig(db);
	auto function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	shared_ptr<BlockHandle> block;
	// transient: allocate a buffer for the uncompressed segment
	if (segment_size < Storage::BLOCK_SIZE) {
		block = buffer_manager.RegisterSmallMemory(segment_size);
	} else {
		buffer_manager.Allocate(segment_size, false, &block);
	}
	return make_uniq<ColumnSegment>(db, std::move(block), type, ColumnSegmentType::TRANSIENT, start, 0, *function,
	                                BaseStatistics::CreateEmpty(type), INVALID_BLOCK, 0, segment_size);
}

unique_ptr<ColumnSegment> ColumnSegment::CreateSegment(ColumnSegment &other, idx_t start) {
	return make_uniq<ColumnSegment>(other, start);
}

ColumnSegment::ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block, LogicalType type_p,
                             ColumnSegmentType segment_type, idx_t start, idx_t count, CompressionFunction &function_p,
                             BaseStatistics statistics, block_id_t block_id_p, idx_t offset_p, idx_t segment_size_p,
                             unique_ptr<ColumnSegmentState> segment_state)
    : SegmentBase<ColumnSegment>(start, count), db(db), type(std::move(type_p)),
      type_size(GetTypeIdSize(type.InternalType())), segment_type(segment_type), function(function_p),
      stats(std::move(statistics)), block(std::move(block)), block_id(block_id_p), offset(offset_p),
      segment_size(segment_size_p) {
	if (function.get().init_segment) {
		this->segment_state = function.get().init_segment(*this, block_id, segment_state.get());
	}
}

ColumnSegment::ColumnSegment(ColumnSegment &other, idx_t start)
    : SegmentBase<ColumnSegment>(start, other.count.load()), db(other.db), type(std::move(other.type)),
      type_size(other.type_size), segment_type(other.segment_type), function(other.function),
      stats(std::move(other.stats)), block(std::move(other.block)), block_id(other.block_id), offset(other.offset),
      segment_size(other.segment_size), segment_state(std::move(other.segment_state)) {
}

ColumnSegment::~ColumnSegment() {
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnSegment::InitializeScan(ColumnScanState &state) {
	state.scan_state = function.get().init_scan(*this);
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset,
                         bool entire_vector) {
	if (entire_vector) {
		D_ASSERT(result_offset == 0);
		Scan(state, scan_count, result);
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		ScanPartial(state, scan_count, result, result_offset);
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	}
}

void ColumnSegment::Skip(ColumnScanState &state) {
	function.get().skip(*this, state, state.row_index - state.internal_index);
	state.internal_index = state.row_index;
}

void ColumnSegment::Scan(ColumnScanState &state, idx_t scan_count, Vector &result) {
	function.get().scan_vector(*this, state, scan_count, result);
}

void ColumnSegment::ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	function.get().scan_partial(*this, state, scan_count, result, result_offset);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ColumnSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	function.get().fetch_row(*this, state, row_id - this->start, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t ColumnSegment::SegmentSize() const {
	return segment_size;
}

void ColumnSegment::Resize(idx_t new_size) {
	D_ASSERT(new_size > this->segment_size);
	D_ASSERT(offset == 0);
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto old_handle = buffer_manager.Pin(block);
	shared_ptr<BlockHandle> new_block;
	auto new_handle = buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block);
	memcpy(new_handle.Ptr(), old_handle.Ptr(), segment_size);
	this->block_id = new_block->BlockId();
	this->block = std::move(new_block);
	this->segment_size = new_size;
}

void ColumnSegment::InitializeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().init_append) {
		throw InternalException("Attempting to init append to a segment without init_append method");
	}
	state.append_state = function.get().init_append(*this);
}

idx_t ColumnSegment::Append(ColumnAppendState &state, UnifiedVectorFormat &append_data, idx_t offset, idx_t count) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().append) {
		throw InternalException("Attempting to append to a segment without append method");
	}
	return function.get().append(*state.append_state, *this, stats, append_data, offset, count);
}

idx_t ColumnSegment::FinalizeAppend(ColumnAppendState &state) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (!function.get().finalize_append) {
		throw InternalException("Attempting to call FinalizeAppend on a segment without a finalize_append method");
	}
	auto result_count = function.get().finalize_append(*this, stats);
	state.append_state.reset();
	return result_count;
}

void ColumnSegment::RevertAppend(idx_t start_row) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	if (function.get().revert_append) {
		function.get().revert_append(*this, start_row);
	}
	this->count = start_row - this->start;
}

//===--------------------------------------------------------------------===//
// Convert To Persistent
//===--------------------------------------------------------------------===//
void ColumnSegment::ConvertToPersistent(optional_ptr<BlockManager> block_manager, block_id_t block_id_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_id_p;
	offset = 0;

	if (block_id == INVALID_BLOCK) {
		// constant block: reset the block buffer
		D_ASSERT(stats.statistics.IsConstant());
		block.reset();
	} else {
		D_ASSERT(!stats.statistics.IsConstant());
		// non-constant block: write the block to disk
		// the data for the block already exists in-memory of our block
		// instead of copying the data we alter some metadata so the buffer points to an on-disk block
		block = block_manager->ConvertToPersistent(block_id, std::move(block));
	}
}

void ColumnSegment::MarkAsPersistent(shared_ptr<BlockHandle> block_p, uint32_t offset_p) {
	D_ASSERT(segment_type == ColumnSegmentType::TRANSIENT);
	segment_type = ColumnSegmentType::PERSISTENT;

	block_id = block_p->BlockId();
	offset = offset_p;
	block = std::move(block_p);
}

//===--------------------------------------------------------------------===//
// Drop Segment
//===--------------------------------------------------------------------===//
void ColumnSegment::CommitDropSegment() {
	if (segment_type != ColumnSegmentType::PERSISTENT) {
		// not persistent
		return;
	}
	if (block_id != INVALID_BLOCK) {
		GetBlockManager().MarkBlockAsModified(block_id);
	}
	if (function.get().cleanup_state) {
		function.get().cleanup_state(*this);
	}
}

//===--------------------------------------------------------------------===//
// Filter Selection
//===--------------------------------------------------------------------===//
template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(T *vec, T predicate, SelectionVector &sel, idx_t approved_tuple_count,
                                      ValidityMask &mask, SelectionVector &result_sel) {
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		auto idx = sel.get_index(i);
		if ((!HAS_NULL || mask.RowIsValid(idx)) && OP::Operation(vec[idx], predicate)) {
			result_sel.set_index(result_count++, idx);
		}
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(T *vec, T predicate, SelectionVector &sel, idx_t &approved_tuple_count,
                                  ExpressionType comparison_type, ValidityMask &mask) {
	SelectionVector new_sel(approved_tuple_count);
	// the inplace loops take the result as the last parameter
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_NOTEQUAL: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, false>(vec, predicate, sel,
			                                                                       approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThan, true>(vec, predicate, sel,
			                                                                      approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
		if (mask.AllValid()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(
			    vec, predicate, sel, approved_tuple_count, mask, new_sel);
		}
		break;
	}
	default:
		throw NotImplementedException("Unknown comparison type for filter pushed down to table!");
	}
	sel.Initialize(new_sel);
}

template <bool IS_NULL>
static idx_t TemplatedNullSelection(SelectionVector &sel, idx_t &approved_tuple_count, ValidityMask &mask) {
	if (mask.AllValid()) {
		// no NULL values
		if (IS_NULL) {
			approved_tuple_count = 0;
			return 0;
		} else {
			return approved_tuple_count;
		}
	} else {
		SelectionVector result_sel(approved_tuple_count);
		idx_t result_count = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto idx = sel.get_index(i);
			if (mask.RowIsValid(idx) != IS_NULL) {
				result_sel.set_index(result_count++, idx);
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = result_count;
		return result_count;
	}
}

idx_t ColumnSegment::FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
                                     idx_t &approved_tuple_count, ValidityMask &mask) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_OR: {
		// similar to the CONJUNCTION_AND, but we need to take care of the SelectionVectors (OR all of them)
		idx_t count_total = 0;
		SelectionVector result_sel(approved_tuple_count);
		auto &conjunction_or = filter.Cast<ConjunctionOrFilter>();
		for (auto &child_filter : conjunction_or.child_filters) {
			SelectionVector temp_sel;
			temp_sel.Initialize(sel);
			idx_t temp_tuple_count = approved_tuple_count;
			idx_t temp_count = FilterSelection(temp_sel, result, *child_filter, temp_tuple_count, mask);
			// tuples passed, move them into the actual result vector
			for (idx_t i = 0; i < temp_count; i++) {
				auto new_idx = temp_sel.get_index(i);
				bool is_new_idx = true;
				for (idx_t res_idx = 0; res_idx < count_total; res_idx++) {
					if (result_sel.get_index(res_idx) == new_idx) {
						is_new_idx = false;
						break;
					}
				}
				if (is_new_idx) {
					result_sel.set_index(count_total++, new_idx);
				}
			}
		}
		sel.Initialize(result_sel);
		approved_tuple_count = count_total;
		return approved_tuple_count;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction_and.child_filters) {
			FilterSelection(sel, result, *child_filter, approved_tuple_count, mask);
		}
		return approved_tuple_count;
	}
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		// the inplace loops take the result as the last parameter
		switch (result.GetType().InternalType()) {
		case PhysicalType::UINT8: {
			auto result_flat = FlatVector::GetData<uint8_t>(result);
			auto predicate = UTinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint8_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT16: {
			auto result_flat = FlatVector::GetData<uint16_t>(result);
			auto predicate = USmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint16_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT32: {
			auto result_flat = FlatVector::GetData<uint32_t>(result);
			auto predicate = UIntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint32_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::UINT64: {
			auto result_flat = FlatVector::GetData<uint64_t>(result);
			auto predicate = UBigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<uint64_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT8: {
			auto result_flat = FlatVector::GetData<int8_t>(result);
			auto predicate = TinyIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int8_t>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT16: {
			auto result_flat = FlatVector::GetData<int16_t>(result);
			auto predicate = SmallIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int16_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT32: {
			auto result_flat = FlatVector::GetData<int32_t>(result);
			auto predicate = IntegerValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int32_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT64: {
			auto result_flat = FlatVector::GetData<int64_t>(result);
			auto predicate = BigIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<int64_t>(result_flat, predicate, sel, approved_tuple_count,
			                               constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::INT128: {
			auto result_flat = FlatVector::GetData<hugeint_t>(result);
			auto predicate = HugeIntValue::Get(constant_filter.constant);
			FilterSelectionSwitch<hugeint_t>(result_flat, predicate, sel, approved_tuple_count,
			                                 constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::FLOAT: {
			auto result_flat = FlatVector::GetData<float>(result);
			auto predicate = FloatValue::Get(constant_filter.constant);
			FilterSelectionSwitch<float>(result_flat, predicate, sel, approved_tuple_count,
			                             constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::DOUBLE: {
			auto result_flat = FlatVector::GetData<double>(result);
			auto predicate = DoubleValue::Get(constant_filter.constant);
			FilterSelectionSwitch<double>(result_flat, predicate, sel, approved_tuple_count,
			                              constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::VARCHAR: {
			auto result_flat = FlatVector::GetData<string_t>(result);
			auto predicate = string_t(StringValue::Get(constant_filter.constant));
			FilterSelectionSwitch<string_t>(result_flat, predicate, sel, approved_tuple_count,
			                                constant_filter.comparison_type, mask);
			break;
		}
		case PhysicalType::BOOL: {
			auto result_flat = FlatVector::GetData<bool>(result);
			auto predicate = BooleanValue::Get(constant_filter.constant);
			FilterSelectionSwitch<bool>(result_flat, predicate, sel, approved_tuple_count,
			                            constant_filter.comparison_type, mask);
			break;
		}
		default:
			throw InvalidTypeException(result.GetType(), "Invalid type for filter pushed down to table comparison");
		}
		return approved_tuple_count;
	}
	case TableFilterType::IS_NULL:
		return TemplatedNullSelection<true>(sel, approved_tuple_count, mask);
	case TableFilterType::IS_NOT_NULL:
		return TemplatedNullSelection<false>(sel, approved_tuple_count, mask);
	default:
		throw InternalException("FIXME: unsupported type for filter selection");
	}
}

} // namespace duckdb








namespace duckdb {

ListColumnData::ListColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, idx_t start_row,
                               LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	auto &child_type = ListType::GetChildType(type);
	// the child column, with column index 1 (0 is the validity mask)
	child_column = ColumnData::CreateColumnUnique(block_manager, info, 1, start_row, child_type, this);
}

void ListColumnData::SetStart(idx_t new_start) {
	ColumnData::SetStart(new_start);
	child_column->SetStart(new_start);
	validity.SetStart(new_start);
}

bool ListColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for list columns
	return false;
}

void ListColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 2);
	validity.InitializeScan(state.child_states[0]);

	// initialize the child scan
	child_column->InitializeScan(state.child_states[1]);
}

uint64_t ListColumnData::FetchListOffset(idx_t row_idx) {
	auto segment = data.GetSegment(row_idx);
	ColumnFetchState fetch_state;
	Vector result(type, 1);
	segment->FetchRow(fetch_state, row_idx, result, 0);

	// initialize the child scan with the required offset
	return FlatVector::GetData<uint64_t>(result)[0];
}

void ListColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	if (row_idx == 0) {
		InitializeScan(state);
		return;
	}
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 2);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	// we need to read the list at position row_idx to get the correct row offset of the child
	auto child_offset = row_idx == start ? 0 : FetchListOffset(row_idx - 1);
	D_ASSERT(child_offset <= child_column->GetMaxEntry());
	if (child_offset < child_column->GetMaxEntry()) {
		child_column->InitializeScanWithOffset(state.child_states[1], start + child_offset);
	}
	state.last_offset = child_offset;
}

idx_t ListColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	return ScanCount(state, result, STANDARD_VECTOR_SIZE);
}

idx_t ListColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	if (count == 0) {
		return 0;
	}
	// updates not supported for lists
	D_ASSERT(!updates);

	Vector offset_vector(LogicalType::UBIGINT, count);
	idx_t scan_count = ScanVector(state, offset_vector, count, false);
	D_ASSERT(scan_count > 0);
	validity.ScanCount(state.child_states[0], result, count);

	UnifiedVectorFormat offsets;
	offset_vector.ToUnifiedFormat(scan_count, offsets);
	auto data = UnifiedVectorFormat::GetData<uint64_t>(offsets);
	auto last_entry = data[offsets.sel->get_index(scan_count - 1)];

	// shift all offsets so they are 0 at the first entry
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto base_offset = state.last_offset;
	idx_t current_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		auto offset_index = offsets.sel->get_index(i);
		result_data[i].offset = current_offset;
		result_data[i].length = data[offset_index] - current_offset - base_offset;
		current_offset += result_data[i].length;
	}

	D_ASSERT(last_entry >= base_offset);
	idx_t child_scan_count = last_entry - base_offset;
	ListVector::Reserve(result, child_scan_count);

	if (child_scan_count > 0) {
		auto &child_entry = ListVector::GetEntry(result);
		if (child_entry.GetType().InternalType() != PhysicalType::STRUCT &&
		    state.child_states[1].row_index + child_scan_count > child_column->start + child_column->GetMaxEntry()) {
			throw InternalException("ListColumnData::ScanCount - internal list scan offset is out of range");
		}
		child_column->ScanCount(state.child_states[1], child_entry, child_scan_count);
	}
	state.last_offset = last_entry;

	ListVector::SetListSize(result, child_scan_count);
	return scan_count;
}

void ListColumnData::Skip(ColumnScanState &state, idx_t count) {
	// skip inside the validity segment
	validity.Skip(state.child_states[0], count);

	// we need to read the list entries/offsets to figure out how much to skip
	// note that we only need to read the first and last entry
	// however, let's just read all "count" entries for now
	Vector result(LogicalType::UBIGINT, count);
	idx_t scan_count = ScanVector(state, result, count, false);
	if (scan_count == 0) {
		return;
	}

	auto data = FlatVector::GetData<uint64_t>(result);
	auto last_entry = data[scan_count - 1];
	idx_t child_scan_count = last_entry - state.last_offset;
	if (child_scan_count == 0) {
		return;
	}
	state.last_offset = last_entry;

	// skip the child state forward by the child_scan_count
	child_column->Skip(state.child_states[1], child_scan_count);
}

void ListColumnData::InitializeAppend(ColumnAppendState &state) {
	// initialize the list offset append
	ColumnData::InitializeAppend(state);

	// initialize the validity append
	ColumnAppendState validity_append_state;
	validity.InitializeAppend(validity_append_state);
	state.child_appends.push_back(std::move(validity_append_state));

	// initialize the child column append
	ColumnAppendState child_append_state;
	child_column->InitializeAppend(child_append_state);
	state.child_appends.push_back(std::move(child_append_state));
}

void ListColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	D_ASSERT(count > 0);
	UnifiedVectorFormat list_data;
	vector.ToUnifiedFormat(count, list_data);
	auto &list_validity = list_data.validity;

	// construct the list_entry_t entries to append to the column data
	auto input_offsets = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto start_offset = child_column->GetMaxEntry();
	idx_t child_count = 0;

	ValidityMask append_mask(count);
	auto append_offsets = unique_ptr<uint64_t[]>(new uint64_t[count]);
	bool child_contiguous = true;
	for (idx_t i = 0; i < count; i++) {
		auto input_idx = list_data.sel->get_index(i);
		if (list_validity.RowIsValid(input_idx)) {
			auto &input_list = input_offsets[input_idx];
			if (input_list.offset != child_count) {
				child_contiguous = false;
			}
			append_offsets[i] = start_offset + child_count + input_list.length;
			child_count += input_list.length;
		} else {
			append_mask.SetInvalid(i);
			append_offsets[i] = start_offset + child_count;
		}
	}
	auto &list_child = ListVector::GetEntry(vector);
	Vector child_vector(list_child);
	if (!child_contiguous) {
		// if the child of the list vector is a non-contiguous vector (i.e. list elements are repeating or have gaps)
		// we first push a selection vector and flatten the child vector to turn it into a contiguous vector
		SelectionVector child_sel(child_count);
		idx_t current_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto input_idx = list_data.sel->get_index(i);
			if (list_validity.RowIsValid(input_idx)) {
				auto &input_list = input_offsets[input_idx];
				for (idx_t list_idx = 0; list_idx < input_list.length; list_idx++) {
					child_sel.set_index(current_count++, input_list.offset + list_idx);
				}
			}
		}
		D_ASSERT(current_count == child_count);
		child_vector.Slice(list_child, child_sel, child_count);
	}

	UnifiedVectorFormat vdata;
	vdata.sel = FlatVector::IncrementalSelectionVector();
	vdata.data = data_ptr_cast(append_offsets.get());

	// append the list offsets
	ColumnData::AppendData(stats, state, vdata, count);
	// append the validity data
	vdata.validity = append_mask;
	validity.AppendData(stats, state.child_appends[0], vdata, count);
	// append the child vector
	if (child_count > 0) {
		child_column->Append(ListStats::GetChildStats(stats), state.child_appends[1], child_vector, child_count);
	}
}

void ListColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);
	validity.RevertAppend(start_row);
	auto column_count = GetMaxEntry();
	if (column_count > start) {
		// revert append in the child column
		auto list_offset = FetchListOffset(column_count - 1);
		child_column->RevertAppend(list_offset);
	}
}

idx_t ListColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	throw NotImplementedException("List Fetch");
}

void ListColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                            idx_t update_count) {
	throw NotImplementedException("List Update is not supported.");
}

void ListColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                  Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	throw NotImplementedException("List Update Column is not supported");
}

unique_ptr<BaseStatistics> ListColumnData::GetUpdateStatistics() {
	return nullptr;
}

void ListColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	// insert any child states that are required
	// we need two (validity & list child)
	// note that we need a scan state for the child vector
	// this is because we will (potentially) fetch more than one tuple from the list child
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}

	// now perform the fetch within the segment
	auto start_offset = idx_t(row_id) == this->start ? 0 : FetchListOffset(row_id - 1);
	auto end_offset = FetchListOffset(row_id);
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);

	auto &validity = FlatVector::Validity(result);
	auto list_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = list_data[result_idx];
	// set the list entry offset to the size of the current list
	list_entry.offset = ListVector::GetListSize(result);
	list_entry.length = end_offset - start_offset;
	if (!validity.RowIsValid(result_idx)) {
		// the list is NULL! no need to fetch the child
		D_ASSERT(list_entry.length == 0);
		return;
	}

	// now we need to read from the child all the elements between [offset...length]
	auto child_scan_count = list_entry.length;
	if (child_scan_count > 0) {
		auto child_state = make_uniq<ColumnScanState>();
		auto &child_type = ListType::GetChildType(result.GetType());
		Vector child_scan(child_type, child_scan_count);
		// seek the scan towards the specified position and read [length] entries
		child_state->Initialize(child_type);
		child_column->InitializeScanWithOffset(*child_state, start + start_offset);
		D_ASSERT(child_type.InternalType() == PhysicalType::STRUCT ||
		         child_state->row_index + child_scan_count - this->start <= child_column->GetMaxEntry());
		child_column->ScanCount(*child_state, child_scan, child_scan_count);

		ListVector::Append(result, child_scan, child_scan_count);
	}
}

void ListColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	child_column->CommitDropColumn();
}

struct ListColumnCheckpointState : public ColumnCheckpointState {
	ListColumnCheckpointState(RowGroup &row_group, ColumnData &column_data, PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = ListStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	unique_ptr<ColumnCheckpointState> child_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = global_stats->Copy();
		ListStats::SetChildStats(stats, child_state->GetStatistics());
		return stats.ToUnique();
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		ColumnCheckpointState::WriteDataPointers(writer, serializer);
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
		serializer.WriteObject(102, "child_column",
		                       [&](Serializer &serializer) { child_state->WriteDataPointers(writer, serializer); });
	}
};

unique_ptr<ColumnCheckpointState> ListColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                        PartialBlockManager &partial_block_manager) {
	return make_uniq<ListColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> ListColumnData::Checkpoint(RowGroup &row_group,
                                                             PartialBlockManager &partial_block_manager,
                                                             ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto child_state = child_column->Checkpoint(row_group, partial_block_manager, checkpoint_info);

	auto &checkpoint_state = base_state->Cast<ListColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_state);
	checkpoint_state.child_state = std::move(child_state);
	return base_state;
}

void ListColumnData::DeserializeColumn(Deserializer &deserializer) {
	ColumnData::DeserializeColumn(deserializer);

	deserializer.ReadObject(101, "validity",
	                        [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer); });

	deserializer.ReadObject(102, "child_column",
	                        [&](Deserializer &deserializer) { child_column->DeserializeColumn(deserializer); });
}

void ListColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                          vector<duckdb::ColumnSegmentInfo> &result) {
	ColumnData::GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.back() = 1;
	child_column->GetColumnSegmentInfo(row_group_index, col_path, result);
}

} // namespace duckdb



namespace duckdb {

PersistentTableData::PersistentTableData(idx_t column_count) : total_rows(0), row_group_count(0) {
}

PersistentTableData::~PersistentTableData() {
}

} // namespace duckdb























namespace duckdb {

RowGroup::RowGroup(RowGroupCollection &collection, idx_t start, idx_t count)
    : SegmentBase<RowGroup>(start, count), collection(collection) {
	Verify();
}

RowGroup::RowGroup(RowGroupCollection &collection, RowGroupPointer &&pointer)
    : SegmentBase<RowGroup>(pointer.row_start, pointer.tuple_count), collection(collection) {
	// deserialize the columns
	if (pointer.data_pointers.size() != collection.GetTypes().size()) {
		throw IOException("Row group column count is unaligned with table column count. Corrupt file?");
	}
	this->column_pointers = std::move(pointer.data_pointers);
	this->columns.resize(column_pointers.size());
	this->is_loaded = unique_ptr<atomic<bool>[]>(new atomic<bool>[columns.size()]);
	for (idx_t c = 0; c < columns.size(); c++) {
		this->is_loaded[c] = false;
	}
	this->deletes_pointers = std::move(pointer.deletes_pointers);
	this->deletes_is_loaded = false;

	Verify();
}

void RowGroup::MoveToCollection(RowGroupCollection &collection, idx_t new_start) {
	this->collection = collection;
	this->start = new_start;
	for (auto &column : GetColumns()) {
		column->SetStart(new_start);
	}
	if (!HasUnloadedDeletes()) {
		auto &vinfo = GetVersionInfo();
		if (vinfo) {
			vinfo->SetStart(new_start);
		}
	}
}

RowGroup::~RowGroup() {
}

vector<shared_ptr<ColumnData>> &RowGroup::GetColumns() {
	// ensure all columns are loaded
	for (idx_t c = 0; c < GetColumnCount(); c++) {
		GetColumn(c);
	}
	return columns;
}

idx_t RowGroup::GetColumnCount() const {
	return columns.size();
}

ColumnData &RowGroup::GetColumn(storage_t c) {
	D_ASSERT(c < columns.size());
	if (!is_loaded) {
		// not being lazy loaded
		D_ASSERT(columns[c]);
		return *columns[c];
	}
	if (is_loaded[c]) {
		D_ASSERT(columns[c]);
		return *columns[c];
	}
	lock_guard<mutex> l(row_group_lock);
	if (columns[c]) {
		D_ASSERT(is_loaded[c]);
		return *columns[c];
	}
	if (column_pointers.size() != columns.size()) {
		throw InternalException("Lazy loading a column but the pointer was not set");
	}
	auto &metadata_manager = GetCollection().GetMetadataManager();
	auto &types = GetCollection().GetTypes();
	auto &block_pointer = column_pointers[c];
	MetadataReader column_data_reader(metadata_manager, block_pointer);
	this->columns[c] =
	    ColumnData::Deserialize(GetBlockManager(), GetTableInfo(), c, start, column_data_reader, types[c], nullptr);
	is_loaded[c] = true;
	if (this->columns[c]->count != this->count) {
		throw InternalException("Corrupted database - loaded column with index %llu at row start %llu, count %llu did "
		                        "not match count of row group %llu",
		                        c, start, this->columns[c]->count, this->count.load());
	}
	return *columns[c];
}

BlockManager &RowGroup::GetBlockManager() {
	return GetCollection().GetBlockManager();
}
DataTableInfo &RowGroup::GetTableInfo() {
	return GetCollection().GetTableInfo();
}

void RowGroup::InitializeEmpty(const vector<LogicalType> &types) {
	// set up the segment trees for the column segments
	D_ASSERT(columns.empty());
	for (idx_t i = 0; i < types.size(); i++) {
		auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), i, start, types[i]);
		columns.push_back(std::move(column_data));
	}
}

void ColumnScanState::Initialize(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VALIDITY) {
		// validity - nothing to initialize
		return;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		// validity + struct children
		auto &struct_children = StructType::GetChildTypes(type);
		child_states.resize(struct_children.size() + 1);
		for (idx_t i = 0; i < struct_children.size(); i++) {
			child_states[i + 1].Initialize(struct_children[i].second);
		}
	} else if (type.InternalType() == PhysicalType::LIST) {
		// validity + list child
		child_states.resize(2);
		child_states[1].Initialize(ListType::GetChildType(type));
	} else {
		// validity
		child_states.resize(1);
	}
}

void CollectionScanState::Initialize(const vector<LogicalType> &types) {
	auto &column_ids = GetColumnIds();
	column_scans = make_unsafe_uniq_array<ColumnScanState>(column_ids.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		column_scans[i].Initialize(types[column_ids[i]]);
	}
}

bool RowGroup::InitializeScanWithOffset(CollectionScanState &state, idx_t vector_offset) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (filters) {
		if (!CheckZonemap(*filters, column_ids)) {
			return false;
		}
	}

	state.row_group = this;
	state.vector_index = vector_offset;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
	D_ASSERT(state.column_scans);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			auto &column_data = GetColumn(column);
			column_data.InitializeScanWithOffset(state.column_scans[i], start + vector_offset * STANDARD_VECTOR_SIZE);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

bool RowGroup::InitializeScan(CollectionScanState &state) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (filters) {
		if (!CheckZonemap(*filters, column_ids)) {
			return false;
		}
	}
	state.row_group = this;
	state.vector_index = 0;
	state.max_row_group_row =
	    this->start > state.max_row ? 0 : MinValue<idx_t>(this->count, state.max_row - this->start);
	if (state.max_row_group_row == 0) {
		return false;
	}
	D_ASSERT(state.column_scans);
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		if (column != COLUMN_IDENTIFIER_ROW_ID) {
			auto &column_data = GetColumn(column);
			column_data.InitializeScan(state.column_scans[i]);
		} else {
			state.column_scans[i].current = nullptr;
		}
	}
	return true;
}

unique_ptr<RowGroup> RowGroup::AlterType(RowGroupCollection &new_collection, const LogicalType &target_type,
                                         idx_t changed_idx, ExpressionExecutor &executor,
                                         CollectionScanState &scan_state, DataChunk &scan_chunk) {
	Verify();

	// construct a new column data for this type
	auto column_data = ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), changed_idx, start, target_type);

	ColumnAppendState append_state;
	column_data->InitializeAppend(append_state);

	// scan the original table, and fill the new column with the transformed value
	scan_state.Initialize(GetCollection().GetTypes());
	InitializeScan(scan_state);

	DataChunk append_chunk;
	vector<LogicalType> append_types;
	append_types.push_back(target_type);
	append_chunk.Initialize(Allocator::DefaultAllocator(), append_types);
	auto &append_vector = append_chunk.data[0];
	while (true) {
		// scan the table
		scan_chunk.Reset();
		ScanCommitted(scan_state, scan_chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (scan_chunk.size() == 0) {
			break;
		}
		// execute the expression
		append_chunk.Reset();
		executor.ExecuteExpression(scan_chunk, append_vector);
		column_data->Append(append_state, append_vector, scan_chunk.size());
	}

	// set up the row_group based on this row_group
	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
	auto &cols = GetColumns();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i == changed_idx) {
			// this is the altered column: use the new column
			row_group->columns.push_back(std::move(column_data));
		} else {
			// this column was not altered: use the data directly
			row_group->columns.push_back(cols[i]);
		}
	}
	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::AddColumn(RowGroupCollection &new_collection, ColumnDefinition &new_column,
                                         ExpressionExecutor &executor, Expression &default_value, Vector &result) {
	Verify();

	// construct a new column data for the new column
	auto added_column =
	    ColumnData::CreateColumn(GetBlockManager(), GetTableInfo(), GetColumnCount(), start, new_column.Type());

	idx_t rows_to_write = this->count;
	if (rows_to_write > 0) {
		DataChunk dummy_chunk;

		ColumnAppendState state;
		added_column->InitializeAppend(state);
		for (idx_t i = 0; i < rows_to_write; i += STANDARD_VECTOR_SIZE) {
			idx_t rows_in_this_vector = MinValue<idx_t>(rows_to_write - i, STANDARD_VECTOR_SIZE);
			dummy_chunk.SetCardinality(rows_in_this_vector);
			executor.ExecuteExpression(dummy_chunk, result);
			added_column->Append(state, result, rows_in_this_vector);
		}
	}

	// set up the row_group based on this row_group
	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
	row_group->columns = GetColumns();
	// now add the new column
	row_group->columns.push_back(std::move(added_column));

	row_group->Verify();
	return row_group;
}

unique_ptr<RowGroup> RowGroup::RemoveColumn(RowGroupCollection &new_collection, idx_t removed_column) {
	Verify();

	D_ASSERT(removed_column < columns.size());

	auto row_group = make_uniq<RowGroup>(new_collection, this->start, this->count);
	row_group->version_info = GetOrCreateVersionInfoPtr();
	// copy over all columns except for the removed one
	auto &cols = GetColumns();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i != removed_column) {
			row_group->columns.push_back(cols[i]);
		}
	}

	row_group->Verify();
	return row_group;
}

void RowGroup::CommitDrop() {
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		CommitDropColumn(column_idx);
	}
}

void RowGroup::CommitDropColumn(idx_t column_idx) {
	GetColumn(column_idx).CommitDropColumn();
}

void RowGroup::NextVector(CollectionScanState &state) {
	state.vector_index++;
	const auto &column_ids = state.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const auto &column = column_ids[i];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			continue;
		}
		D_ASSERT(column < columns.size());
		GetColumn(column).Skip(state.column_scans[i]);
	}
}

bool RowGroup::CheckZonemap(TableFilterSet &filters, const vector<storage_t> &column_ids) {
	for (auto &entry : filters.filters) {
		auto column_index = entry.first;
		auto &filter = entry.second;
		const auto &base_column_index = column_ids[column_index];
		if (!GetColumn(base_column_index).CheckZonemap(*filter)) {
			return false;
		}
	}
	return true;
}

bool RowGroup::CheckZonemapSegments(CollectionScanState &state) {
	auto &column_ids = state.GetColumnIds();
	auto filters = state.GetFilters();
	if (!filters) {
		return true;
	}
	for (auto &entry : filters->filters) {
		D_ASSERT(entry.first < column_ids.size());
		auto column_idx = entry.first;
		const auto &base_column_idx = column_ids[column_idx];
		bool read_segment = GetColumn(base_column_idx).CheckZonemap(state.column_scans[column_idx], *entry.second);
		if (!read_segment) {
			idx_t target_row =
			    state.column_scans[column_idx].current->start + state.column_scans[column_idx].current->count;
			D_ASSERT(target_row >= this->start);
			D_ASSERT(target_row <= this->start + this->count);
			idx_t target_vector_index = (target_row - this->start) / STANDARD_VECTOR_SIZE;
			if (state.vector_index == target_vector_index) {
				// we can't skip any full vectors because this segment contains less than a full vector
				// for now we just bail-out
				// FIXME: we could check if we can ALSO skip the next segments, in which case skipping a full vector
				// might be possible
				// we don't care that much though, since a single segment that fits less than a full vector is
				// exceedingly rare
				return true;
			}
			while (state.vector_index < target_vector_index) {
				NextVector(state);
			}
			return false;
		}
	}

	return true;
}

template <TableScanType TYPE>
void RowGroup::TemplatedScan(TransactionData transaction, CollectionScanState &state, DataChunk &result) {
	const bool ALLOW_UPDATES = TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES &&
	                           TYPE != TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED;
	auto table_filters = state.GetFilters();
	const auto &column_ids = state.GetColumnIds();
	auto adaptive_filter = state.GetAdaptiveFilter();
	while (true) {
		if (state.vector_index * STANDARD_VECTOR_SIZE >= state.max_row_group_row) {
			// exceeded the amount of rows to scan
			return;
		}
		idx_t current_row = state.vector_index * STANDARD_VECTOR_SIZE;
		auto max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.max_row_group_row - current_row);

		//! first check the zonemap if we have to scan this partition
		if (!CheckZonemapSegments(state)) {
			continue;
		}
		// second, scan the version chunk manager to figure out which tuples to load for this transaction
		idx_t count;
		SelectionVector valid_sel(STANDARD_VECTOR_SIZE);
		if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
			count = state.row_group->GetSelVector(transaction, state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else if (TYPE == TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED) {
			count = state.row_group->GetCommittedSelVector(transaction.start_time, transaction.transaction_id,
			                                               state.vector_index, valid_sel, max_count);
			if (count == 0) {
				// nothing to scan for this vector, skip the entire vector
				NextVector(state);
				continue;
			}
		} else {
			count = max_count;
		}
		if (count == max_count && !table_filters) {
			// scan all vectors completely: full scan without deletions or table filters
			for (idx_t i = 0; i < column_ids.size(); i++) {
				const auto &column = column_ids[i];
				if (column == COLUMN_IDENTIFIER_ROW_ID) {
					// scan row id
					D_ASSERT(result.data[i].GetType().InternalType() == ROW_TYPE);
					result.data[i].Sequence(this->start + current_row, 1, count);
				} else {
					auto &col_data = GetColumn(column);
					if (TYPE != TableScanType::TABLE_SCAN_REGULAR) {
						col_data.ScanCommitted(state.vector_index, state.column_scans[i], result.data[i],
						                       ALLOW_UPDATES);
					} else {
						col_data.Scan(transaction, state.vector_index, state.column_scans[i], result.data[i]);
					}
				}
			}
		} else {
			// partial scan: we have deletions or table filters
			idx_t approved_tuple_count = count;
			SelectionVector sel;
			if (count != max_count) {
				sel.Initialize(valid_sel);
			} else {
				sel.Initialize(nullptr);
			}
			//! first, we scan the columns with filters, fetch their data and generate a selection vector.
			//! get runtime statistics
			auto start_time = high_resolution_clock::now();
			if (table_filters) {
				D_ASSERT(adaptive_filter);
				D_ASSERT(ALLOW_UPDATES);
				for (idx_t i = 0; i < table_filters->filters.size(); i++) {
					auto tf_idx = adaptive_filter->permutation[i];
					auto col_idx = column_ids[tf_idx];
					auto &col_data = GetColumn(col_idx);
					col_data.Select(transaction, state.vector_index, state.column_scans[tf_idx], result.data[tf_idx],
					                sel, approved_tuple_count, *table_filters->filters[tf_idx]);
				}
				for (auto &table_filter : table_filters->filters) {
					result.data[table_filter.first].Slice(sel, approved_tuple_count);
				}
			}
			if (approved_tuple_count == 0) {
				// all rows were filtered out by the table filters
				// skip this vector in all the scans that were not scanned yet
				D_ASSERT(table_filters);
				result.Reset();
				for (idx_t i = 0; i < column_ids.size(); i++) {
					auto col_idx = column_ids[i];
					if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
						continue;
					}
					if (table_filters->filters.find(i) == table_filters->filters.end()) {
						auto &col_data = GetColumn(col_idx);
						col_data.Skip(state.column_scans[i]);
					}
				}
				state.vector_index++;
				continue;
			}
			//! Now we use the selection vector to fetch data for the other columns.
			for (idx_t i = 0; i < column_ids.size(); i++) {
				if (!table_filters || table_filters->filters.find(i) == table_filters->filters.end()) {
					auto column = column_ids[i];
					if (column == COLUMN_IDENTIFIER_ROW_ID) {
						D_ASSERT(result.data[i].GetType().InternalType() == PhysicalType::INT64);
						result.data[i].SetVectorType(VectorType::FLAT_VECTOR);
						auto result_data = FlatVector::GetData<int64_t>(result.data[i]);
						for (size_t sel_idx = 0; sel_idx < approved_tuple_count; sel_idx++) {
							result_data[sel_idx] = this->start + current_row + sel.get_index(sel_idx);
						}
					} else {
						auto &col_data = GetColumn(column);
						if (TYPE == TableScanType::TABLE_SCAN_REGULAR) {
							col_data.FilterScan(transaction, state.vector_index, state.column_scans[i], result.data[i],
							                    sel, approved_tuple_count);
						} else {
							col_data.FilterScanCommitted(state.vector_index, state.column_scans[i], result.data[i], sel,
							                             approved_tuple_count, ALLOW_UPDATES);
						}
					}
				}
			}
			auto end_time = high_resolution_clock::now();
			if (adaptive_filter && table_filters->filters.size() > 1) {
				adaptive_filter->AdaptRuntimeStatistics(duration_cast<duration<double>>(end_time - start_time).count());
			}
			D_ASSERT(approved_tuple_count > 0);
			count = approved_tuple_count;
		}
		result.SetCardinality(count);
		state.vector_index++;
		break;
	}
}

void RowGroup::Scan(TransactionData transaction, CollectionScanState &state, DataChunk &result) {
	TemplatedScan<TableScanType::TABLE_SCAN_REGULAR>(transaction, state, result);
}

void RowGroup::ScanCommitted(CollectionScanState &state, DataChunk &result, TableScanType type) {
	auto &transaction_manager = DuckTransactionManager::Get(GetCollection().GetAttached());

	auto lowest_active_start = transaction_manager.LowestActiveStart();
	auto lowest_active_id = transaction_manager.LowestActiveId();
	TransactionData data(lowest_active_id, lowest_active_start);
	switch (type) {
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_DISALLOW_UPDATES>(data, state, result);
		break;
	case TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED:
		TemplatedScan<TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED>(data, state, result);
		break;
	default:
		throw InternalException("Unrecognized table scan type");
	}
}

shared_ptr<RowVersionManager> &RowGroup::GetVersionInfo() {
	if (!HasUnloadedDeletes()) {
		// deletes are loaded - return the version info
		return version_info;
	}
	lock_guard<mutex> lock(row_group_lock);
	// double-check after obtaining the lock whether or not deletes are still not loaded to avoid double load
	if (HasUnloadedDeletes()) {
		// deletes are not loaded - reload
		auto root_delete = deletes_pointers[0];
		version_info = RowVersionManager::Deserialize(root_delete, GetBlockManager().GetMetadataManager(), start);
		deletes_is_loaded = true;
	}
	return version_info;
}

shared_ptr<RowVersionManager> &RowGroup::GetOrCreateVersionInfoPtr() {
	auto vinfo = GetVersionInfo();
	if (!vinfo) {
		lock_guard<mutex> lock(row_group_lock);
		if (!version_info) {
			version_info = make_shared<RowVersionManager>(start);
		}
	}
	return version_info;
}

RowVersionManager &RowGroup::GetOrCreateVersionInfo() {
	return *GetOrCreateVersionInfoPtr();
}

idx_t RowGroup::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                             idx_t max_count) {
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetSelVector(transaction, vector_idx, sel_vector, max_count);
}

idx_t RowGroup::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                      SelectionVector &sel_vector, idx_t max_count) {
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return max_count;
	}
	return vinfo->GetCommittedSelVector(start_time, transaction_id, vector_idx, sel_vector, max_count);
}

bool RowGroup::Fetch(TransactionData transaction, idx_t row) {
	D_ASSERT(row < this->count);
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return true;
	}
	return vinfo->Fetch(transaction, row);
}

void RowGroup::FetchRow(TransactionData transaction, ColumnFetchState &state, const vector<column_t> &column_ids,
                        row_t row_id, DataChunk &result, idx_t result_idx) {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		auto column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			D_ASSERT(result.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
			result.data[col_idx].SetVectorType(VectorType::FLAT_VECTOR);
			auto data = FlatVector::GetData<row_t>(result.data[col_idx]);
			data[result_idx] = row_id;
		} else {
			// regular column: fetch data from the base column
			auto &col_data = GetColumn(column);
			col_data.FetchRow(transaction, state, row_id, result.data[col_idx], result_idx);
		}
	}
}

void RowGroup::AppendVersionInfo(TransactionData transaction, idx_t count) {
	idx_t row_group_start = this->count.load();
	idx_t row_group_end = row_group_start + count;
	if (row_group_end > Storage::ROW_GROUP_SIZE) {
		row_group_end = Storage::ROW_GROUP_SIZE;
	}
	// create the version_info if it doesn't exist yet
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.AppendVersionInfo(transaction, count, row_group_start, row_group_end);
	this->count = row_group_end;
}

void RowGroup::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.CommitAppend(commit_id, row_group_start, count);
}

void RowGroup::RevertAppend(idx_t row_group_start) {
	auto &vinfo = GetOrCreateVersionInfo();
	vinfo.RevertAppend(row_group_start - this->start);
	for (auto &column : columns) {
		column->RevertAppend(row_group_start);
	}
	this->count = MinValue<idx_t>(row_group_start - this->start, this->count);
	Verify();
}

void RowGroup::InitializeAppend(RowGroupAppendState &append_state) {
	append_state.row_group = this;
	append_state.offset_in_row_group = this->count;
	// for each column, initialize the append state
	append_state.states = make_unsafe_uniq_array<ColumnAppendState>(GetColumnCount());
	for (idx_t i = 0; i < GetColumnCount(); i++) {
		auto &col_data = GetColumn(i);
		col_data.InitializeAppend(append_state.states[i]);
	}
}

void RowGroup::Append(RowGroupAppendState &state, DataChunk &chunk, idx_t append_count) {
	// append to the current row_group
	for (idx_t i = 0; i < GetColumnCount(); i++) {
		auto &col_data = GetColumn(i);
		col_data.Append(state.states[i], chunk.data[i], append_count);
	}
	state.offset_in_row_group += append_count;
}

void RowGroup::Update(TransactionData transaction, DataChunk &update_chunk, row_t *ids, idx_t offset, idx_t count,
                      const vector<PhysicalIndex> &column_ids) {
#ifdef DEBUG
	for (size_t i = offset; i < offset + count; i++) {
		D_ASSERT(ids[i] >= row_t(this->start) && ids[i] < row_t(this->start + this->count));
	}
#endif
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column = column_ids[i];
		D_ASSERT(column.index != COLUMN_IDENTIFIER_ROW_ID);
		auto &col_data = GetColumn(column.index);
		D_ASSERT(col_data.type.id() == update_chunk.data[i].GetType().id());
		if (offset > 0) {
			Vector sliced_vector(update_chunk.data[i], offset, offset + count);
			sliced_vector.Flatten(count);
			col_data.Update(transaction, column.index, sliced_vector, ids + offset, count);
		} else {
			col_data.Update(transaction, column.index, update_chunk.data[i], ids, count);
		}
		MergeStatistics(column.index, *col_data.GetUpdateStatistics());
	}
}

void RowGroup::UpdateColumn(TransactionData transaction, DataChunk &updates, Vector &row_ids,
                            const vector<column_t> &column_path) {
	D_ASSERT(updates.ColumnCount() == 1);
	auto ids = FlatVector::GetData<row_t>(row_ids);

	auto primary_column_idx = column_path[0];
	D_ASSERT(primary_column_idx != COLUMN_IDENTIFIER_ROW_ID);
	D_ASSERT(primary_column_idx < columns.size());
	auto &col_data = GetColumn(primary_column_idx);
	col_data.UpdateColumn(transaction, column_path, updates.data[0], ids, updates.size(), 1);
	MergeStatistics(primary_column_idx, *col_data.GetUpdateStatistics());
}

unique_ptr<BaseStatistics> RowGroup::GetStatistics(idx_t column_idx) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	return col_data.GetStatistics();
}

void RowGroup::MergeStatistics(idx_t column_idx, const BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	col_data.MergeStatistics(other);
}

void RowGroup::MergeIntoStatistics(idx_t column_idx, BaseStatistics &other) {
	auto &col_data = GetColumn(column_idx);
	lock_guard<mutex> slock(stats_lock);
	col_data.MergeIntoStatistics(other);
}

RowGroupWriteData RowGroup::WriteToDisk(PartialBlockManager &manager,
                                        const vector<CompressionType> &compression_types) {
	RowGroupWriteData result;
	result.states.reserve(columns.size());
	result.statistics.reserve(columns.size());

	// Checkpoint the individual columns of the row group
	// Here we're iterating over columns. Each column can have multiple segments.
	// (Some columns will be wider than others, and require different numbers
	// of blocks to encode.) Segments cannot span blocks.
	//
	// Some of these columns are composite (list, struct). The data is written
	// first sequentially, and the pointers are written later, so that the
	// pointers all end up densely packed, and thus more cache-friendly.
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		auto &column = GetColumn(column_idx);
		ColumnCheckpointInfo checkpoint_info {compression_types[column_idx]};
		auto checkpoint_state = column.Checkpoint(*this, manager, checkpoint_info);
		D_ASSERT(checkpoint_state);

		auto stats = checkpoint_state->GetStatistics();
		D_ASSERT(stats);

		result.statistics.push_back(stats->Copy());
		result.states.push_back(std::move(checkpoint_state));
	}
	D_ASSERT(result.states.size() == result.statistics.size());
	return result;
}

bool RowGroup::AllDeleted() {
	if (HasUnloadedDeletes()) {
		// deletes aren't loaded yet - we know not everything is deleted
		return false;
	}
	auto &vinfo = GetVersionInfo();
	if (!vinfo) {
		return false;
	}
	return vinfo->GetCommittedDeletedCount(count) == count;
}

bool RowGroup::HasUnloadedDeletes() const {
	if (deletes_pointers.empty()) {
		// no stored deletes at all
		return false;
	}
	// return whether or not the deletes have been loaded
	return !deletes_is_loaded;
}

RowGroupPointer RowGroup::Checkpoint(RowGroupWriter &writer, TableStatistics &global_stats) {
	RowGroupPointer row_group_pointer;

	vector<CompressionType> compression_types;
	compression_types.reserve(columns.size());
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		auto &column = GetColumn(column_idx);
		if (column.count != this->count) {
			throw InternalException("Corrupted in-memory column - column with index %llu has misaligned count (row "
			                        "group has %llu rows, column has %llu)",
			                        column_idx, this->count.load(), column.count);
		}
		compression_types.push_back(writer.GetColumnCompressionType(column_idx));
	}
	auto result = WriteToDisk(writer.GetPartialBlockManager(), compression_types);
	for (idx_t column_idx = 0; column_idx < GetColumnCount(); column_idx++) {
		global_stats.GetStats(column_idx).Statistics().Merge(result.statistics[column_idx]);
	}

	// construct the row group pointer and write the column meta data to disk
	D_ASSERT(result.states.size() == columns.size());
	row_group_pointer.row_start = start;
	row_group_pointer.tuple_count = count;
	for (auto &state : result.states) {
		// get the current position of the table data writer
		auto &data_writer = writer.GetPayloadWriter();
		auto pointer = data_writer.GetMetaBlockPointer();

		// store the stats and the data pointers in the row group pointers
		row_group_pointer.data_pointers.push_back(pointer);

		// Write pointers to the column segments.
		//
		// Just as above, the state can refer to many other states, so this
		// can cascade recursively into more pointer writes.
		BinarySerializer serializer(data_writer);
		serializer.Begin();
		state->WriteDataPointers(writer, serializer);
		serializer.End();
	}
	row_group_pointer.deletes_pointers = CheckpointDeletes(writer.GetPayloadWriter().GetManager());
	Verify();
	return row_group_pointer;
}

vector<MetaBlockPointer> RowGroup::CheckpointDeletes(MetadataManager &manager) {
	if (HasUnloadedDeletes()) {
		// deletes were not loaded so they cannot be changed
		// re-use them as-is
		manager.ClearModifiedBlocks(deletes_pointers);
		return deletes_pointers;
	}
	if (!version_info) {
		// no version information: write nothing
		return vector<MetaBlockPointer>();
	}
	return version_info->Checkpoint(manager);
}

void RowGroup::Serialize(RowGroupPointer &pointer, Serializer &serializer) {
	serializer.WriteProperty(100, "row_start", pointer.row_start);
	serializer.WriteProperty(101, "tuple_count", pointer.tuple_count);
	serializer.WriteProperty(102, "data_pointers", pointer.data_pointers);
	serializer.WriteProperty(103, "delete_pointers", pointer.deletes_pointers);
}

RowGroupPointer RowGroup::Deserialize(Deserializer &deserializer) {
	RowGroupPointer result;
	result.row_start = deserializer.ReadProperty<uint64_t>(100, "row_start");
	result.tuple_count = deserializer.ReadProperty<uint64_t>(101, "tuple_count");
	result.data_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(102, "data_pointers");
	result.deletes_pointers = deserializer.ReadProperty<vector<MetaBlockPointer>>(103, "delete_pointers");
	return result;
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
void RowGroup::GetColumnSegmentInfo(idx_t row_group_index, vector<ColumnSegmentInfo> &result) {
	for (idx_t col_idx = 0; col_idx < GetColumnCount(); col_idx++) {
		auto &col_data = GetColumn(col_idx);
		col_data.GetColumnSegmentInfo(row_group_index, {col_idx}, result);
	}
}

//===--------------------------------------------------------------------===//
// Version Delete Information
//===--------------------------------------------------------------------===//
class VersionDeleteState {
public:
	VersionDeleteState(RowGroup &info, TransactionData transaction, DataTable &table, idx_t base_row)
	    : info(info), transaction(transaction), table(table), current_chunk(DConstants::INVALID_INDEX), count(0),
	      base_row(base_row), delete_count(0) {
	}

	RowGroup &info;
	TransactionData transaction;
	DataTable &table;
	idx_t current_chunk;
	row_t rows[STANDARD_VECTOR_SIZE];
	idx_t count;
	idx_t base_row;
	idx_t chunk_row;
	idx_t delete_count;

public:
	void Delete(row_t row_id);
	void Flush();
};

idx_t RowGroup::Delete(TransactionData transaction, DataTable &table, row_t *ids, idx_t count) {
	VersionDeleteState del_state(*this, transaction, table, this->start);

	// obtain a write lock
	for (idx_t i = 0; i < count; i++) {
		D_ASSERT(ids[i] >= 0);
		D_ASSERT(idx_t(ids[i]) >= this->start && idx_t(ids[i]) < this->start + this->count);
		del_state.Delete(ids[i] - this->start);
	}
	del_state.Flush();
	return del_state.delete_count;
}

void RowGroup::Verify() {
#ifdef DEBUG
	for (auto &column : GetColumns()) {
		column->Verify(*this);
	}
#endif
}

idx_t RowGroup::DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count) {
	return GetOrCreateVersionInfo().DeleteRows(vector_idx, transaction_id, rows, count);
}

void VersionDeleteState::Delete(row_t row_id) {
	D_ASSERT(row_id >= 0);
	idx_t vector_idx = row_id / STANDARD_VECTOR_SIZE;
	idx_t idx_in_vector = row_id - vector_idx * STANDARD_VECTOR_SIZE;
	if (current_chunk != vector_idx) {
		Flush();

		current_chunk = vector_idx;
		chunk_row = vector_idx * STANDARD_VECTOR_SIZE;
	}
	rows[count++] = idx_in_vector;
}

void VersionDeleteState::Flush() {
	if (count == 0) {
		return;
	}
	// it is possible for delete statements to delete the same tuple multiple times when combined with a USING clause
	// in the current_info->Delete, we check which tuples are actually deleted (excluding duplicate deletions)
	// this is returned in the actual_delete_count
	auto actual_delete_count = info.DeleteRows(current_chunk, transaction.transaction_id, rows, count);
	delete_count += actual_delete_count;
	if (transaction.transaction && actual_delete_count > 0) {
		// now push the delete into the undo buffer, but only if any deletes were actually performed
		transaction.transaction->PushDelete(table, info.GetOrCreateVersionInfo(), current_chunk, rows,
		                                    actual_delete_count, base_row + chunk_row);
	}
	count = 0;
}

} // namespace duckdb














namespace duckdb {

//===--------------------------------------------------------------------===//
// Row Group Segment Tree
//===--------------------------------------------------------------------===//
RowGroupSegmentTree::RowGroupSegmentTree(RowGroupCollection &collection)
    : SegmentTree<RowGroup, true>(), collection(collection), current_row_group(0), max_row_group(0) {
}
RowGroupSegmentTree::~RowGroupSegmentTree() {
}

void RowGroupSegmentTree::Initialize(PersistentTableData &data) {
	D_ASSERT(data.row_group_count > 0);
	current_row_group = 0;
	max_row_group = data.row_group_count;
	finished_loading = false;
	reader = make_uniq<MetadataReader>(collection.GetMetadataManager(), data.block_pointer);
}

unique_ptr<RowGroup> RowGroupSegmentTree::LoadSegment() {
	if (current_row_group >= max_row_group) {
		reader.reset();
		finished_loading = true;
		return nullptr;
	}
	BinaryDeserializer deserializer(*reader);
	deserializer.Begin();
	auto row_group_pointer = RowGroup::Deserialize(deserializer);
	deserializer.End();
	current_row_group++;
	return make_uniq<RowGroup>(collection, std::move(row_group_pointer));
}

//===--------------------------------------------------------------------===//
// Row Group Collection
//===--------------------------------------------------------------------===//
RowGroupCollection::RowGroupCollection(shared_ptr<DataTableInfo> info_p, BlockManager &block_manager,
                                       vector<LogicalType> types_p, idx_t row_start_p, idx_t total_rows_p)
    : block_manager(block_manager), total_rows(total_rows_p), info(std::move(info_p)), types(std::move(types_p)),
      row_start(row_start_p) {
	row_groups = make_shared<RowGroupSegmentTree>(*this);
}

idx_t RowGroupCollection::GetTotalRows() const {
	return total_rows.load();
}

const vector<LogicalType> &RowGroupCollection::GetTypes() const {
	return types;
}

Allocator &RowGroupCollection::GetAllocator() const {
	return Allocator::Get(info->db);
}

AttachedDatabase &RowGroupCollection::GetAttached() {
	return GetTableInfo().db;
}

MetadataManager &RowGroupCollection::GetMetadataManager() {
	return GetBlockManager().GetMetadataManager();
}

//===--------------------------------------------------------------------===//
// Initialize
//===--------------------------------------------------------------------===//
void RowGroupCollection::Initialize(PersistentTableData &data) {
	D_ASSERT(this->row_start == 0);
	auto l = row_groups->Lock();
	this->total_rows = data.total_rows;
	row_groups->Initialize(data);
	stats.Initialize(types, data);
}

void RowGroupCollection::InitializeEmpty() {
	stats.InitializeEmpty(types);
}

void RowGroupCollection::AppendRowGroup(SegmentLock &l, idx_t start_row) {
	D_ASSERT(start_row >= row_start);
	auto new_row_group = make_uniq<RowGroup>(*this, start_row, 0);
	new_row_group->InitializeEmpty(types);
	row_groups->AppendSegment(l, std::move(new_row_group));
}

RowGroup *RowGroupCollection::GetRowGroup(int64_t index) {
	return (RowGroup *)row_groups->GetSegmentByIndex(index);
}

void RowGroupCollection::Verify() {
#ifdef DEBUG
	idx_t current_total_rows = 0;
	row_groups->Verify();
	for (auto &row_group : row_groups->Segments()) {
		row_group.Verify();
		D_ASSERT(&row_group.GetCollection() == this);
		D_ASSERT(row_group.start == this->row_start + current_total_rows);
		current_total_rows += row_group.count;
	}
	D_ASSERT(current_total_rows == total_rows.load());
#endif
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void RowGroupCollection::InitializeScan(CollectionScanState &state, const vector<column_t> &column_ids,
                                        TableFilterSet *table_filters) {
	auto row_group = row_groups->GetRootSegment();
	D_ASSERT(row_group);
	state.row_groups = row_groups.get();
	state.max_row = row_start + total_rows;
	state.Initialize(GetTypes());
	while (row_group && !row_group->InitializeScan(state)) {
		row_group = row_groups->GetNextSegment(row_group);
	}
}

void RowGroupCollection::InitializeCreateIndexScan(CreateIndexScanState &state) {
	state.segment_lock = row_groups->Lock();
}

void RowGroupCollection::InitializeScanWithOffset(CollectionScanState &state, const vector<column_t> &column_ids,
                                                  idx_t start_row, idx_t end_row) {
	auto row_group = row_groups->GetSegment(start_row);
	D_ASSERT(row_group);
	state.row_groups = row_groups.get();
	state.max_row = end_row;
	state.Initialize(GetTypes());
	idx_t start_vector = (start_row - row_group->start) / STANDARD_VECTOR_SIZE;
	if (!row_group->InitializeScanWithOffset(state, start_vector)) {
		throw InternalException("Failed to initialize row group scan with offset");
	}
}

bool RowGroupCollection::InitializeScanInRowGroup(CollectionScanState &state, RowGroupCollection &collection,
                                                  RowGroup &row_group, idx_t vector_index, idx_t max_row) {
	state.max_row = max_row;
	state.row_groups = collection.row_groups.get();
	if (!state.column_scans) {
		// initialize the scan state
		state.Initialize(collection.GetTypes());
	}
	return row_group.InitializeScanWithOffset(state, vector_index);
}

void RowGroupCollection::InitializeParallelScan(ParallelCollectionScanState &state) {
	state.collection = this;
	state.current_row_group = row_groups->GetRootSegment();
	state.vector_index = 0;
	state.max_row = row_start + total_rows;
	state.batch_index = 0;
	state.processed_rows = 0;
}

bool RowGroupCollection::NextParallelScan(ClientContext &context, ParallelCollectionScanState &state,
                                          CollectionScanState &scan_state) {
	while (true) {
		idx_t vector_index;
		idx_t max_row;
		RowGroupCollection *collection;
		RowGroup *row_group;
		{
			// select the next row group to scan from the parallel state
			lock_guard<mutex> l(state.lock);
			if (!state.current_row_group || state.current_row_group->count == 0) {
				// no more data left to scan
				break;
			}
			collection = state.collection;
			row_group = state.current_row_group;
			if (ClientConfig::GetConfig(context).verify_parallelism) {
				vector_index = state.vector_index;
				max_row = state.current_row_group->start +
				          MinValue<idx_t>(state.current_row_group->count,
				                          STANDARD_VECTOR_SIZE * state.vector_index + STANDARD_VECTOR_SIZE);
				D_ASSERT(vector_index * STANDARD_VECTOR_SIZE < state.current_row_group->count);
				state.vector_index++;
				if (state.vector_index * STANDARD_VECTOR_SIZE >= state.current_row_group->count) {
					state.current_row_group = row_groups->GetNextSegment(state.current_row_group);
					state.vector_index = 0;
				}
			} else {
				state.processed_rows += state.current_row_group->count;
				vector_index = 0;
				max_row = state.current_row_group->start + state.current_row_group->count;
				state.current_row_group = row_groups->GetNextSegment(state.current_row_group);
			}
			max_row = MinValue<idx_t>(max_row, state.max_row);
			scan_state.batch_index = ++state.batch_index;
		}
		D_ASSERT(collection);
		D_ASSERT(row_group);

		// initialize the scan for this row group
		bool need_to_scan = InitializeScanInRowGroup(scan_state, *collection, *row_group, vector_index, max_row);
		if (!need_to_scan) {
			// skip this row group
			continue;
		}
		return true;
	}
	return false;
}

bool RowGroupCollection::Scan(DuckTransaction &transaction, const vector<column_t> &column_ids,
                              const std::function<bool(DataChunk &chunk)> &fun) {
	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		scan_types.push_back(types[column_ids[i]]);
	}
	DataChunk chunk;
	chunk.Initialize(GetAllocator(), scan_types);

	// initialize the scan
	TableScanState state;
	state.Initialize(column_ids, nullptr);
	InitializeScan(state.local_state, column_ids, nullptr);

	while (true) {
		chunk.Reset();
		state.local_state.Scan(transaction, chunk);
		if (chunk.size() == 0) {
			return true;
		}
		if (!fun(chunk)) {
			return false;
		}
	}
}

bool RowGroupCollection::Scan(DuckTransaction &transaction, const std::function<bool(DataChunk &chunk)> &fun) {
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	return Scan(transaction, column_ids, fun);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void RowGroupCollection::Fetch(TransactionData transaction, DataChunk &result, const vector<column_t> &column_ids,
                               const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	// figure out which row_group to fetch from
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);
	idx_t count = 0;
	for (idx_t i = 0; i < fetch_count; i++) {
		auto row_id = row_ids[i];
		RowGroup *row_group;
		{
			idx_t segment_index;
			auto l = row_groups->Lock();
			if (!row_groups->TryGetSegmentIndex(l, row_id, segment_index)) {
				// in parallel append scenarios it is possible for the row_id
				continue;
			}
			row_group = row_groups->GetSegmentByIndex(l, segment_index);
		}
		if (!row_group->Fetch(transaction, row_id - row_group->start)) {
			continue;
		}
		row_group->FetchRow(transaction, state, column_ids, row_id, result, count);
		count++;
	}
	result.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
TableAppendState::TableAppendState()
    : row_group_append_state(*this), total_append_count(0), start_row_group(nullptr), transaction(0, 0), remaining(0) {
}

TableAppendState::~TableAppendState() {
	D_ASSERT(Exception::UncaughtException() || remaining == 0);
}

bool RowGroupCollection::IsEmpty() const {
	auto l = row_groups->Lock();
	return IsEmpty(l);
}

bool RowGroupCollection::IsEmpty(SegmentLock &l) const {
	return row_groups->IsEmpty(l);
}

void RowGroupCollection::InitializeAppend(TransactionData transaction, TableAppendState &state, idx_t append_count) {
	state.row_start = total_rows;
	state.current_row = state.row_start;
	state.total_append_count = 0;

	// start writing to the row_groups
	auto l = row_groups->Lock();
	if (IsEmpty(l)) {
		// empty row group collection: empty first row group
		AppendRowGroup(l, row_start);
	}
	state.start_row_group = row_groups->GetLastSegment(l);
	D_ASSERT(this->row_start + total_rows == state.start_row_group->start + state.start_row_group->count);
	state.start_row_group->InitializeAppend(state.row_group_append_state);
	state.remaining = append_count;
	state.transaction = transaction;
	if (state.remaining > 0) {
		state.start_row_group->AppendVersionInfo(transaction, state.remaining);
		total_rows += state.remaining;
	}
}

void RowGroupCollection::InitializeAppend(TableAppendState &state) {
	TransactionData tdata(0, 0);
	InitializeAppend(tdata, state, 0);
}

bool RowGroupCollection::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(chunk.ColumnCount() == types.size());
	chunk.Verify();

	bool new_row_group = false;
	idx_t append_count = chunk.size();
	idx_t remaining = chunk.size();
	state.total_append_count += append_count;
	while (true) {
		auto current_row_group = state.row_group_append_state.row_group;
		// check how much we can fit into the current row_group
		idx_t append_count =
		    MinValue<idx_t>(remaining, Storage::ROW_GROUP_SIZE - state.row_group_append_state.offset_in_row_group);
		if (append_count > 0) {
			current_row_group->Append(state.row_group_append_state, chunk, append_count);
			// merge the stats
			auto stats_lock = stats.GetLock();
			for (idx_t i = 0; i < types.size(); i++) {
				current_row_group->MergeIntoStatistics(i, stats.GetStats(i).Statistics());
			}
		}
		remaining -= append_count;
		if (state.remaining > 0) {
			state.remaining -= append_count;
		}
		if (remaining > 0) {
			// we expect max 1 iteration of this loop (i.e. a single chunk should never overflow more than one
			// row_group)
			D_ASSERT(chunk.size() == remaining + append_count);
			// slice the input chunk
			if (remaining < chunk.size()) {
				SelectionVector sel(remaining);
				for (idx_t i = 0; i < remaining; i++) {
					sel.set_index(i, append_count + i);
				}
				chunk.Slice(sel, remaining);
			}
			// append a new row_group
			new_row_group = true;
			auto next_start = current_row_group->start + state.row_group_append_state.offset_in_row_group;

			auto l = row_groups->Lock();
			AppendRowGroup(l, next_start);
			// set up the append state for this row_group
			auto last_row_group = row_groups->GetLastSegment(l);
			last_row_group->InitializeAppend(state.row_group_append_state);
			if (state.remaining > 0) {
				last_row_group->AppendVersionInfo(state.transaction, state.remaining);
			}
			continue;
		} else {
			break;
		}
	}
	state.current_row += append_count;
	auto stats_lock = stats.GetLock();
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		stats.GetStats(col_idx).UpdateDistinctStatistics(chunk.data[col_idx], chunk.size());
	}
	return new_row_group;
}

void RowGroupCollection::FinalizeAppend(TransactionData transaction, TableAppendState &state) {
	auto remaining = state.total_append_count;
	auto row_group = state.start_row_group;
	while (remaining > 0) {
		auto append_count = MinValue<idx_t>(remaining, Storage::ROW_GROUP_SIZE - row_group->count);
		row_group->AppendVersionInfo(transaction, append_count);
		remaining -= append_count;
		row_group = row_groups->GetNextSegment(row_group);
	}
	total_rows += state.total_append_count;

	state.total_append_count = 0;
	state.start_row_group = nullptr;

	Verify();
}

void RowGroupCollection::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	auto row_group = row_groups->GetSegment(row_start);
	D_ASSERT(row_group);
	idx_t current_row = row_start;
	idx_t remaining = count;
	while (true) {
		idx_t start_in_row_group = current_row - row_group->start;
		idx_t append_count = MinValue<idx_t>(row_group->count - start_in_row_group, remaining);

		row_group->CommitAppend(commit_id, start_in_row_group, append_count);

		current_row += append_count;
		remaining -= append_count;
		if (remaining == 0) {
			break;
		}
		row_group = row_groups->GetNextSegment(row_group);
	}
}

void RowGroupCollection::RevertAppendInternal(idx_t start_row) {
	if (total_rows <= start_row) {
		return;
	}
	total_rows = start_row;

	auto l = row_groups->Lock();
	// find the segment index that the current row belongs to
	idx_t segment_index = row_groups->GetSegmentIndex(l, start_row);
	auto segment = row_groups->GetSegmentByIndex(l, segment_index);
	auto &info = *segment;

	// remove any segments AFTER this segment: they should be deleted entirely
	row_groups->EraseSegments(l, segment_index);

	info.next = nullptr;
	info.RevertAppend(start_row);
}

void RowGroupCollection::MergeStorage(RowGroupCollection &data) {
	D_ASSERT(data.types == types);
	auto index = row_start + total_rows.load();
	auto segments = data.row_groups->MoveSegments();
	for (auto &entry : segments) {
		auto &row_group = entry.node;
		row_group->MoveToCollection(*this, index);
		index += row_group->count;
		row_groups->AppendSegment(std::move(row_group));
	}
	stats.MergeStats(data.stats);
	total_rows += data.total_rows.load();
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
idx_t RowGroupCollection::Delete(TransactionData transaction, DataTable &table, row_t *ids, idx_t count) {
	idx_t delete_count = 0;
	// delete is in the row groups
	// we need to figure out for each id to which row group it belongs
	// usually all (or many) ids belong to the same row group
	// we iterate over the ids and check for every id if it belongs to the same row group as their predecessor
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(ids[start]);
		for (pos++; pos < count; pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this row group
			if (idx_t(ids[pos]) < row_group->start) {
				// id is before row_group start -> it does not
				break;
			}
			if (idx_t(ids[pos]) >= row_group->start + row_group->count) {
				// id is after row group end -> it does not
				break;
			}
		}
		delete_count += row_group->Delete(transaction, table, ids + start, pos - start);
	} while (pos < count);
	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
void RowGroupCollection::Update(TransactionData transaction, row_t *ids, const vector<PhysicalIndex> &column_ids,
                                DataChunk &updates) {
	idx_t pos = 0;
	do {
		idx_t start = pos;
		auto row_group = row_groups->GetSegment(ids[pos]);
		row_t base_id =
		    row_group->start + ((ids[pos] - row_group->start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE);
		row_t max_id = MinValue<row_t>(base_id + STANDARD_VECTOR_SIZE, row_group->start + row_group->count);
		for (pos++; pos < updates.size(); pos++) {
			D_ASSERT(ids[pos] >= 0);
			// check if this id still belongs to this vector in this row group
			if (ids[pos] < base_id) {
				// id is before vector start -> it does not
				break;
			}
			if (ids[pos] >= max_id) {
				// id is after the maximum id in this vector -> it does not
				break;
			}
		}
		row_group->Update(transaction, updates, ids, start, pos - start, column_ids);

		auto l = stats.GetLock();
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto column_id = column_ids[i];
			stats.MergeStats(*l, column_id.index, *row_group->GetStatistics(column_id.index));
		}
	} while (pos < updates.size());
}

void RowGroupCollection::RemoveFromIndexes(TableIndexList &indexes, Vector &row_identifiers, idx_t count) {
	auto row_ids = FlatVector::GetData<row_t>(row_identifiers);

	// initialize the fetch state
	// FIXME: we do not need to fetch all columns, only the columns required by the indices!
	TableScanState state;
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	state.Initialize(std::move(column_ids));
	state.table_state.max_row = row_start + total_rows;

	// initialize the fetch chunk
	DataChunk result;
	result.Initialize(GetAllocator(), types);

	SelectionVector sel(STANDARD_VECTOR_SIZE);
	// now iterate over the row ids
	for (idx_t r = 0; r < count;) {
		result.Reset();
		// figure out which row_group to fetch from
		auto row_id = row_ids[r];
		auto row_group = row_groups->GetSegment(row_id);
		auto row_group_vector_idx = (row_id - row_group->start) / STANDARD_VECTOR_SIZE;
		auto base_row_id = row_group_vector_idx * STANDARD_VECTOR_SIZE + row_group->start;

		// fetch the current vector
		state.table_state.Initialize(GetTypes());
		row_group->InitializeScanWithOffset(state.table_state, row_group_vector_idx);
		row_group->ScanCommitted(state.table_state, result, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		result.Verify();

		// check for any remaining row ids if they also fall into this vector
		// we try to fetch handle as many rows as possible at the same time
		idx_t sel_count = 0;
		for (; r < count; r++) {
			idx_t current_row = idx_t(row_ids[r]);
			if (current_row < base_row_id || current_row >= base_row_id + result.size()) {
				// this row-id does not fall into the current chunk - break
				break;
			}
			auto row_in_vector = current_row - base_row_id;
			D_ASSERT(row_in_vector < result.size());
			sel.set_index(sel_count++, row_in_vector);
		}
		D_ASSERT(sel_count > 0);
		// slice the vector with all rows that are present in this vector and erase from the index
		result.Slice(sel, sel_count);

		indexes.Scan([&](Index &index) {
			index.Delete(result, row_identifiers);
			return false;
		});
	}
}

void RowGroupCollection::UpdateColumn(TransactionData transaction, Vector &row_ids, const vector<column_t> &column_path,
                                      DataChunk &updates) {
	auto first_id = FlatVector::GetValue<row_t>(row_ids, 0);
	if (first_id >= MAX_ROW_ID) {
		throw NotImplementedException("Cannot update a column-path on transaction local data");
	}
	// find the row_group this id belongs to
	auto primary_column_idx = column_path[0];
	auto row_group = row_groups->GetSegment(first_id);
	row_group->UpdateColumn(transaction, updates, row_ids, column_path);

	row_group->MergeIntoStatistics(primary_column_idx, stats.GetStats(primary_column_idx).Statistics());
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void RowGroupCollection::Checkpoint(TableDataWriter &writer, TableStatistics &global_stats) {
	bool can_vacuum_deletes = info->indexes.Empty();
	idx_t start = this->row_start;
	auto segments = row_groups->MoveSegments();
	auto l = row_groups->Lock();
	for (auto &entry : segments) {
		auto &row_group = *entry.node;
		if (can_vacuum_deletes && row_group.AllDeleted()) {
			row_group.CommitDrop();
			continue;
		}
		row_group.MoveToCollection(*this, start);
		auto row_group_writer = writer.GetRowGroupWriter(row_group);
		auto pointer = row_group.Checkpoint(*row_group_writer, global_stats);
		writer.AddRowGroup(std::move(pointer), std::move(row_group_writer));
		row_groups->AppendSegment(l, std::move(entry.node));
		start += row_group.count;
	}
	total_rows = start;
}

//===--------------------------------------------------------------------===//
// CommitDrop
//===--------------------------------------------------------------------===//
void RowGroupCollection::CommitDropColumn(idx_t index) {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDropColumn(index);
	}
}

void RowGroupCollection::CommitDropTable() {
	for (auto &row_group : row_groups->Segments()) {
		row_group.CommitDrop();
	}
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> RowGroupCollection::GetColumnSegmentInfo() {
	vector<ColumnSegmentInfo> result;
	for (auto &row_group : row_groups->Segments()) {
		row_group.GetColumnSegmentInfo(row_group.index, result);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Alter
//===--------------------------------------------------------------------===//
shared_ptr<RowGroupCollection> RowGroupCollection::AddColumn(ClientContext &context, ColumnDefinition &new_column,
                                                             Expression &default_value) {
	idx_t new_column_idx = types.size();
	auto new_types = types;
	new_types.push_back(new_column.GetType());
	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());

	ExpressionExecutor executor(context);
	DataChunk dummy_chunk;
	Vector default_vector(new_column.GetType());
	executor.AddExpression(default_value);

	result->stats.InitializeAddColumn(stats, new_column.GetType());
	auto &new_column_stats = result->stats.GetStats(new_column_idx);

	// fill the column with its DEFAULT value, or NULL if none is specified
	auto new_stats = make_uniq<SegmentStatistics>(new_column.GetType());
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AddColumn(*result, new_column, executor, default_value, default_vector);
		// merge in the statistics
		new_row_group->MergeIntoStatistics(new_column_idx, new_column_stats.Statistics());

		result->row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::RemoveColumn(idx_t col_idx) {
	D_ASSERT(col_idx < types.size());
	auto new_types = types;
	new_types.erase(new_types.begin() + col_idx);

	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());
	result->stats.InitializeRemoveColumn(stats, col_idx);

	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.RemoveColumn(*result, col_idx);
		result->row_groups->AppendSegment(std::move(new_row_group));
	}
	return result;
}

shared_ptr<RowGroupCollection> RowGroupCollection::AlterType(ClientContext &context, idx_t changed_idx,
                                                             const LogicalType &target_type,
                                                             vector<column_t> bound_columns, Expression &cast_expr) {
	D_ASSERT(changed_idx < types.size());
	auto new_types = types;
	new_types[changed_idx] = target_type;

	auto result =
	    make_shared<RowGroupCollection>(info, block_manager, std::move(new_types), row_start, total_rows.load());
	result->stats.InitializeAlterType(stats, changed_idx, target_type);

	vector<LogicalType> scan_types;
	for (idx_t i = 0; i < bound_columns.size(); i++) {
		if (bound_columns[i] == COLUMN_IDENTIFIER_ROW_ID) {
			scan_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			scan_types.push_back(types[bound_columns[i]]);
		}
	}
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	ExpressionExecutor executor(context);
	executor.AddExpression(cast_expr);

	TableScanState scan_state;
	scan_state.Initialize(bound_columns);
	scan_state.table_state.max_row = row_start + total_rows;

	// now alter the type of the column within all of the row_groups individually
	auto &changed_stats = result->stats.GetStats(changed_idx);
	for (auto &current_row_group : row_groups->Segments()) {
		auto new_row_group = current_row_group.AlterType(*result, target_type, changed_idx, executor,
		                                                 scan_state.table_state, scan_chunk);
		new_row_group->MergeIntoStatistics(changed_idx, changed_stats.Statistics());
		result->row_groups->AppendSegment(std::move(new_row_group));
	}

	return result;
}

void RowGroupCollection::VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint) {
	if (total_rows == 0) {
		return;
	}
	// scan the original table, check if there's any null value
	auto &not_null_constraint = constraint.Cast<BoundNotNullConstraint>();
	vector<LogicalType> scan_types;
	auto physical_index = not_null_constraint.index.index;
	D_ASSERT(physical_index < types.size());
	scan_types.push_back(types[physical_index]);
	DataChunk scan_chunk;
	scan_chunk.Initialize(GetAllocator(), scan_types);

	CreateIndexScanState state;
	vector<column_t> cids;
	cids.push_back(physical_index);
	// Use ScanCommitted to scan the latest committed data
	state.Initialize(cids, nullptr);
	InitializeScan(state.table_state, cids, nullptr);
	InitializeCreateIndexScan(state);
	while (true) {
		scan_chunk.Reset();
		state.table_state.ScanCommitted(scan_chunk, state.segment_lock,
		                                TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		if (scan_chunk.size() == 0) {
			break;
		}
		// Check constraint
		if (VectorOperations::HasNull(scan_chunk.data[0], scan_chunk.size())) {
			throw ConstraintException("NOT NULL constraint failed: %s.%s", info->table,
			                          parent.column_definitions[physical_index].GetName());
		}
	}
}

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//
void RowGroupCollection::CopyStats(TableStatistics &other_stats) {
	stats.CopyStats(other_stats);
}

unique_ptr<BaseStatistics> RowGroupCollection::CopyStats(column_t column_id) {
	return stats.CopyStats(column_id);
}

void RowGroupCollection::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	auto stats_guard = stats.GetLock();
	stats.GetStats(column_id).SetDistinct(std::move(distinct_stats));
}

} // namespace duckdb







namespace duckdb {

RowVersionManager::RowVersionManager(idx_t start) : start(start), has_changes(false) {
}

void RowVersionManager::SetStart(idx_t new_start) {
	lock_guard<mutex> l(version_lock);
	this->start = new_start;
	idx_t current_start = start;
	for (idx_t i = 0; i < Storage::ROW_GROUP_VECTOR_COUNT; i++) {
		if (vector_info[i]) {
			vector_info[i]->start = current_start;
		}
		current_start += STANDARD_VECTOR_SIZE;
	}
}

idx_t RowVersionManager::GetCommittedDeletedCount(idx_t count) {
	lock_guard<mutex> l(version_lock);
	idx_t deleted_count = 0;
	for (idx_t r = 0, i = 0; r < count; r += STANDARD_VECTOR_SIZE, i++) {
		if (!vector_info[i]) {
			continue;
		}
		idx_t max_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - r);
		if (max_count == 0) {
			break;
		}
		deleted_count += vector_info[i]->GetCommittedDeletedCount(max_count);
	}
	return deleted_count;
}

optional_ptr<ChunkInfo> RowVersionManager::GetChunkInfo(idx_t vector_idx) {
	return vector_info[vector_idx].get();
}

idx_t RowVersionManager::GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector,
                                      idx_t max_count) {
	lock_guard<mutex> l(version_lock);
	auto chunk_info = GetChunkInfo(vector_idx);
	if (!chunk_info) {
		return max_count;
	}
	return chunk_info->GetSelVector(transaction, sel_vector, max_count);
}

idx_t RowVersionManager::GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
                                               SelectionVector &sel_vector, idx_t max_count) {
	lock_guard<mutex> l(version_lock);
	auto info = GetChunkInfo(vector_idx);
	if (!info) {
		return max_count;
	}
	return info->GetCommittedSelVector(start_time, transaction_id, sel_vector, max_count);
}

bool RowVersionManager::Fetch(TransactionData transaction, idx_t row) {
	lock_guard<mutex> lock(version_lock);
	idx_t vector_index = row / STANDARD_VECTOR_SIZE;
	auto info = GetChunkInfo(vector_index);
	if (!info) {
		return true;
	}
	return info->Fetch(transaction, row - vector_index * STANDARD_VECTOR_SIZE);
}

void RowVersionManager::AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start,
                                          idx_t row_group_end) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t vector_start =
		    vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t vector_end =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		if (vector_start == 0 && vector_end == STANDARD_VECTOR_SIZE) {
			// entire vector is encapsulated by append: append a single constant
			auto constant_info = make_uniq<ChunkConstantInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
			constant_info->insert_id = transaction.transaction_id;
			constant_info->delete_id = NOT_DELETED_ID;
			vector_info[vector_idx] = std::move(constant_info);
		} else {
			// part of a vector is encapsulated: append to that part
			optional_ptr<ChunkVectorInfo> new_info;
			if (!vector_info[vector_idx]) {
				// first time appending to this vector: create new info
				auto insert_info = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
				new_info = insert_info.get();
				vector_info[vector_idx] = std::move(insert_info);
			} else if (vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO) {
				// use existing vector
				new_info = &vector_info[vector_idx]->Cast<ChunkVectorInfo>();
			} else {
				throw InternalException("Error in RowVersionManager::AppendVersionInfo - expected either a "
				                        "ChunkVectorInfo or no version info");
			}
			new_info->Append(vector_start, vector_end, transaction.transaction_id);
		}
	}
}

void RowVersionManager::CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) {
	idx_t row_group_end = row_group_start + count;

	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = row_group_start / STANDARD_VECTOR_SIZE;
	idx_t end_vector_idx = (row_group_end - 1) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx <= end_vector_idx; vector_idx++) {
		idx_t vstart = vector_idx == start_vector_idx ? row_group_start - start_vector_idx * STANDARD_VECTOR_SIZE : 0;
		idx_t vend =
		    vector_idx == end_vector_idx ? row_group_end - end_vector_idx * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;

		auto info = vector_info[vector_idx].get();
		info->CommitAppend(commit_id, vstart, vend);
	}
}

void RowVersionManager::RevertAppend(idx_t start_row) {
	lock_guard<mutex> lock(version_lock);
	idx_t start_vector_idx = (start_row + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE;
	for (idx_t vector_idx = start_vector_idx; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		vector_info[vector_idx].reset();
	}
}

ChunkVectorInfo &RowVersionManager::GetVectorInfo(idx_t vector_idx) {
	if (!vector_info[vector_idx]) {
		// no info yet: create it
		vector_info[vector_idx] = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
	} else if (vector_info[vector_idx]->type == ChunkInfoType::CONSTANT_INFO) {
		auto &constant = vector_info[vector_idx]->Cast<ChunkConstantInfo>();
		// info exists but it's a constant info: convert to a vector info
		auto new_info = make_uniq<ChunkVectorInfo>(start + vector_idx * STANDARD_VECTOR_SIZE);
		new_info->insert_id = constant.insert_id;
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			new_info->inserted[i] = constant.insert_id;
		}
		vector_info[vector_idx] = std::move(new_info);
	}
	D_ASSERT(vector_info[vector_idx]->type == ChunkInfoType::VECTOR_INFO);
	return vector_info[vector_idx]->Cast<ChunkVectorInfo>();
}

idx_t RowVersionManager::DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	return GetVectorInfo(vector_idx).Delete(transaction_id, rows, count);
}

void RowVersionManager::CommitDelete(idx_t vector_idx, transaction_t commit_id, row_t rows[], idx_t count) {
	lock_guard<mutex> lock(version_lock);
	has_changes = true;
	GetVectorInfo(vector_idx).CommitDelete(commit_id, rows, count);
}

vector<MetaBlockPointer> RowVersionManager::Checkpoint(MetadataManager &manager) {
	if (!has_changes && !storage_pointers.empty()) {
		// the row version manager already exists on disk and no changes were made
		// we can write the current pointer as-is
		// ensure the blocks we are pointing to are not marked as free
		manager.ClearModifiedBlocks(storage_pointers);
		// return the root pointer
		return storage_pointers;
	}
	// first count how many ChunkInfo's we need to deserialize
	vector<pair<idx_t, reference<ChunkInfo>>> to_serialize;
	for (idx_t vector_idx = 0; vector_idx < Storage::ROW_GROUP_VECTOR_COUNT; vector_idx++) {
		auto chunk_info = vector_info[vector_idx].get();
		if (!chunk_info) {
			continue;
		}
		if (!chunk_info->HasDeletes()) {
			continue;
		}
		to_serialize.emplace_back(vector_idx, *chunk_info);
	}
	if (to_serialize.empty()) {
		return vector<MetaBlockPointer>();
	}

	storage_pointers.clear();

	MetadataWriter writer(manager, &storage_pointers);
	// now serialize the actual version information
	writer.Write<idx_t>(to_serialize.size());
	for (auto &entry : to_serialize) {
		auto &vector_idx = entry.first;
		auto &chunk_info = entry.second.get();
		writer.Write<idx_t>(vector_idx);
		chunk_info.Write(writer);
	}
	writer.Flush();

	has_changes = false;
	return storage_pointers;
}

shared_ptr<RowVersionManager> RowVersionManager::Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager,
                                                             idx_t start) {
	if (!delete_pointer.IsValid()) {
		return nullptr;
	}
	auto version_info = make_shared<RowVersionManager>(start);
	MetadataReader source(manager, delete_pointer, &version_info->storage_pointers);
	auto chunk_count = source.Read<idx_t>();
	D_ASSERT(chunk_count > 0);
	for (idx_t i = 0; i < chunk_count; i++) {
		idx_t vector_index = source.Read<idx_t>();
		if (vector_index >= Storage::ROW_GROUP_VECTOR_COUNT) {
			throw Exception("In DeserializeDeletes, vector_index is out of range for the row group. Corrupted file?");
		}
		version_info->vector_info[vector_index] = ChunkInfo::Read(source);
	}
	version_info->has_changes = false;
	return version_info;
}

} // namespace duckdb








namespace duckdb {

void TableScanState::Initialize(vector<column_t> column_ids, TableFilterSet *table_filters) {
	this->column_ids = std::move(column_ids);
	this->table_filters = table_filters;
	if (table_filters) {
		D_ASSERT(table_filters->filters.size() > 0);
		this->adaptive_filter = make_uniq<AdaptiveFilter>(table_filters);
	}
}

const vector<column_t> &TableScanState::GetColumnIds() {
	D_ASSERT(!column_ids.empty());
	return column_ids;
}

TableFilterSet *TableScanState::GetFilters() {
	D_ASSERT(!table_filters || adaptive_filter.get());
	return table_filters;
}

AdaptiveFilter *TableScanState::GetAdaptiveFilter() {
	return adaptive_filter.get();
}

void ColumnScanState::NextInternal(idx_t count) {
	if (!current) {
		//! There is no column segment
		return;
	}
	row_index += count;
	while (row_index >= current->start + current->count) {
		current = segment_tree->GetNextSegment(current);
		initialized = false;
		segment_checked = false;
		if (!current) {
			break;
		}
	}
	D_ASSERT(!current || (row_index >= current->start && row_index < current->start + current->count));
}

void ColumnScanState::Next(idx_t count) {
	NextInternal(count);
	for (auto &child_state : child_states) {
		child_state.Next(count);
	}
}

const vector<storage_t> &CollectionScanState::GetColumnIds() {
	return parent.GetColumnIds();
}

TableFilterSet *CollectionScanState::GetFilters() {
	return parent.GetFilters();
}

AdaptiveFilter *CollectionScanState::GetAdaptiveFilter() {
	return parent.GetAdaptiveFilter();
}

ParallelCollectionScanState::ParallelCollectionScanState()
    : collection(nullptr), current_row_group(nullptr), processed_rows(0) {
}

CollectionScanState::CollectionScanState(TableScanState &parent_p)
    : row_group(nullptr), vector_index(0), max_row_group_row(0), row_groups(nullptr), max_row(0), batch_index(0),
      parent(parent_p) {
}

bool CollectionScanState::Scan(DuckTransaction &transaction, DataChunk &result) {
	while (row_group) {
		row_group->Scan(transaction, *this, result);
		if (result.size() > 0) {
			return true;
		} else if (max_row <= row_group->start + row_group->count) {
			row_group = nullptr;
			return false;
		} else {
			do {
				row_group = row_groups->GetNextSegment(row_group);
				if (row_group) {
					if (row_group->start >= max_row) {
						row_group = nullptr;
						break;
					}
					bool scan_row_group = row_group->InitializeScan(*this);
					if (scan_row_group) {
						// scan this row group
						break;
					}
				}
			} while (row_group);
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, SegmentLock &l, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(l, row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

bool CollectionScanState::ScanCommitted(DataChunk &result, TableScanType type) {
	while (row_group) {
		row_group->ScanCommitted(*this, result, type);
		if (result.size() > 0) {
			return true;
		} else {
			row_group = row_groups->GetNextSegment(row_group);
			if (row_group) {
				row_group->InitializeScan(*this);
			}
		}
	}
	return false;
}

} // namespace duckdb










namespace duckdb {

StandardColumnData::StandardColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, LogicalType type, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type), parent),
      validity(block_manager, info, 0, start_row, *this) {
}

void StandardColumnData::SetStart(idx_t new_start) {
	ColumnData::SetStart(new_start);
	validity.SetStart(new_start);
}

bool StandardColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	if (!state.segment_checked) {
		if (!state.current) {
			return true;
		}
		state.segment_checked = true;
		auto prune_result = filter.CheckStatistics(state.current->stats.statistics);
		if (prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return true;
		}
		if (updates) {
			auto update_stats = updates->GetStatistics();
			prune_result = filter.CheckStatistics(*update_stats);
			return prune_result != FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else {
			return false;
		}
	} else {
		return true;
	}
}

void StandardColumnData::InitializeScan(ColumnScanState &state) {
	ColumnData::InitializeScan(state);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScan(state.child_states[0]);
}

void StandardColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	ColumnData::InitializeScanWithOffset(state, row_idx);

	// initialize the validity segment
	D_ASSERT(state.child_states.size() == 1);
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);
}

idx_t StandardColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state,
                               Vector &result) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::Scan(transaction, vector_index, state, result);
	validity.Scan(transaction, vector_index, state.child_states[0], result);
	return scan_count;
}

idx_t StandardColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result,
                                        bool allow_updates) {
	D_ASSERT(state.row_index == state.child_states[0].row_index);
	auto scan_count = ColumnData::ScanCommitted(vector_index, state, result, allow_updates);
	validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	return scan_count;
}

idx_t StandardColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = ColumnData::ScanCount(state, result, count);
	validity.ScanCount(state.child_states[0], result, count);
	return scan_count;
}

void StandardColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnData::InitializeAppend(state);

	ColumnAppendState child_append;
	validity.InitializeAppend(child_append);
	state.child_appends.push_back(std::move(child_append));
}

void StandardColumnData::AppendData(BaseStatistics &stats, ColumnAppendState &state, UnifiedVectorFormat &vdata,
                                    idx_t count) {
	ColumnData::AppendData(stats, state, vdata, count);
	validity.AppendData(stats, state.child_appends[0], vdata, count);
}

void StandardColumnData::RevertAppend(row_t start_row) {
	ColumnData::RevertAppend(start_row);

	validity.RevertAppend(start_row);
}

idx_t StandardColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	if (state.child_states.empty()) {
		ColumnScanState child_state;
		state.child_states.push_back(std::move(child_state));
	}
	auto scan_count = ColumnData::Fetch(state, row_id, result);
	validity.Fetch(state.child_states[0], row_id, result);
	return scan_count;
}

void StandardColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                                idx_t update_count) {
	ColumnData::Update(transaction, column_index, update_vector, row_ids, update_count);
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
}

void StandardColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                      Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	if (depth >= column_path.size()) {
		// update this column
		ColumnData::Update(transaction, column_path[0], update_vector, row_ids, update_count);
	} else {
		// update the child column (i.e. the validity column)
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	}
}

unique_ptr<BaseStatistics> StandardColumnData::GetUpdateStatistics() {
	auto stats = updates ? updates->GetStatistics() : nullptr;
	auto validity_stats = validity.GetUpdateStatistics();
	if (!stats && !validity_stats) {
		return nullptr;
	}
	if (!stats) {
		stats = BaseStatistics::CreateEmpty(type).ToUnique();
	}
	if (validity_stats) {
		stats->Merge(*validity_stats);
	}
	return stats;
}

void StandardColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                  idx_t result_idx) {
	// find the segment the row belongs to
	if (state.child_states.empty()) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	ColumnData::FetchRow(transaction, state, row_id, result, result_idx);
}

void StandardColumnData::CommitDropColumn() {
	ColumnData::CommitDropColumn();
	validity.CommitDropColumn();
}

struct StandardColumnCheckpointState : public ColumnCheckpointState {
	StandardColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                              PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
	}

	unique_ptr<ColumnCheckpointState> validity_state;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		D_ASSERT(global_stats);
		return std::move(global_stats);
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		ColumnCheckpointState::WriteDataPointers(writer, serializer);
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
	}
};

unique_ptr<ColumnCheckpointState>
StandardColumnData::CreateCheckpointState(RowGroup &row_group, PartialBlockManager &partial_block_manager) {
	return make_uniq<StandardColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> StandardColumnData::Checkpoint(RowGroup &row_group,
                                                                 PartialBlockManager &partial_block_manager,
                                                                 ColumnCheckpointInfo &checkpoint_info) {
	auto validity_state = validity.Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto base_state = ColumnData::Checkpoint(row_group, partial_block_manager, checkpoint_info);
	auto &checkpoint_state = base_state->Cast<StandardColumnCheckpointState>();
	checkpoint_state.validity_state = std::move(validity_state);
	return base_state;
}

void StandardColumnData::CheckpointScan(ColumnSegment &segment, ColumnScanState &state, idx_t row_group_start,
                                        idx_t count, Vector &scan_vector) {
	ColumnData::CheckpointScan(segment, state, row_group_start, count, scan_vector);

	idx_t offset_in_row_group = state.row_index - row_group_start;
	validity.ScanCommittedRange(row_group_start, offset_in_row_group, count, scan_vector);
}

void StandardColumnData::DeserializeColumn(Deserializer &deserializer) {
	ColumnData::DeserializeColumn(deserializer);
	deserializer.ReadObject(101, "validity",
	                        [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer); });
}

void StandardColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                              vector<duckdb::ColumnSegmentInfo> &result) {
	ColumnData::GetColumnSegmentInfo(row_group_index, col_path, result);
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, std::move(col_path), result);
}

void StandardColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
#endif
}

} // namespace duckdb








namespace duckdb {

StructColumnData::StructColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                   idx_t start_row, LogicalType type_p, optional_ptr<ColumnData> parent)
    : ColumnData(block_manager, info, column_index, start_row, std::move(type_p), parent),
      validity(block_manager, info, 0, start_row, *this) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(child_types.size() > 0);
	if (type.id() != LogicalTypeId::UNION && StructType::IsUnnamed(type)) {
		throw InvalidInputException("A table cannot be created from an unnamed struct");
	}
	// the sub column index, starting at 1 (0 is the validity mask)
	idx_t sub_column_index = 1;
	for (auto &child_type : child_types) {
		sub_columns.push_back(
		    ColumnData::CreateColumnUnique(block_manager, info, sub_column_index, start_row, child_type.second, this));
		sub_column_index++;
	}
}

void StructColumnData::SetStart(idx_t new_start) {
	this->start = new_start;
	for (auto &sub_column : sub_columns) {
		sub_column->SetStart(new_start);
	}
	validity.SetStart(new_start);
}

bool StructColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	// table filters are not supported yet for struct columns
	return false;
}

idx_t StructColumnData::GetMaxEntry() {
	return sub_columns[0]->GetMaxEntry();
}

void StructColumnData::InitializeScan(ColumnScanState &state) {
	D_ASSERT(state.child_states.size() == sub_columns.size() + 1);
	state.row_index = 0;
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScan(state.child_states[0]);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->InitializeScan(state.child_states[i + 1]);
	}
}

void StructColumnData::InitializeScanWithOffset(ColumnScanState &state, idx_t row_idx) {
	D_ASSERT(state.child_states.size() == sub_columns.size() + 1);
	state.row_index = row_idx;
	state.current = nullptr;

	// initialize the validity segment
	validity.InitializeScanWithOffset(state.child_states[0], row_idx);

	// initialize the sub-columns
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->InitializeScanWithOffset(state.child_states[i + 1], row_idx);
	}
}

idx_t StructColumnData::Scan(TransactionData transaction, idx_t vector_index, ColumnScanState &state, Vector &result) {
	auto scan_count = validity.Scan(transaction, vector_index, state.child_states[0], result);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->Scan(transaction, vector_index, state.child_states[i + 1], *child_entries[i]);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCommitted(idx_t vector_index, ColumnScanState &state, Vector &result, bool allow_updates) {
	auto scan_count = validity.ScanCommitted(vector_index, state.child_states[0], result, allow_updates);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCommitted(vector_index, state.child_states[i + 1], *child_entries[i], allow_updates);
	}
	return scan_count;
}

idx_t StructColumnData::ScanCount(ColumnScanState &state, Vector &result, idx_t count) {
	auto scan_count = validity.ScanCount(state.child_states[0], result, count);
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		sub_columns[i]->ScanCount(state.child_states[i + 1], *child_entries[i], count);
	}
	return scan_count;
}

void StructColumnData::Skip(ColumnScanState &state, idx_t count) {
	validity.Skip(state.child_states[0], count);

	// skip inside the sub-columns
	for (idx_t child_idx = 0; child_idx < sub_columns.size(); child_idx++) {
		sub_columns[child_idx]->Skip(state.child_states[child_idx + 1], count);
	}
}

void StructColumnData::InitializeAppend(ColumnAppendState &state) {
	ColumnAppendState validity_append;
	validity.InitializeAppend(validity_append);
	state.child_appends.push_back(std::move(validity_append));

	for (auto &sub_column : sub_columns) {
		ColumnAppendState child_append;
		sub_column->InitializeAppend(child_append);
		state.child_appends.push_back(std::move(child_append));
	}
}

void StructColumnData::Append(BaseStatistics &stats, ColumnAppendState &state, Vector &vector, idx_t count) {
	vector.Flatten(count);

	// append the null values
	validity.Append(stats, state.child_appends[0], vector, count);

	auto &child_entries = StructVector::GetEntries(vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Append(StructStats::GetChildStats(stats, i), state.child_appends[i + 1], *child_entries[i],
		                       count);
	}
	this->count += count;
}

void StructColumnData::RevertAppend(row_t start_row) {
	validity.RevertAppend(start_row);
	for (auto &sub_column : sub_columns) {
		sub_column->RevertAppend(start_row);
	}
	this->count = start_row - this->start;
}

idx_t StructColumnData::Fetch(ColumnScanState &state, row_t row_id, Vector &result) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		ColumnScanState child_state;
		state.child_states.push_back(std::move(child_state));
	}
	// fetch the validity state
	idx_t scan_count = validity.Fetch(state.child_states[0], row_id, result);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Fetch(state.child_states[i + 1], row_id, *child_entries[i]);
	}
	return scan_count;
}

void StructColumnData::Update(TransactionData transaction, idx_t column_index, Vector &update_vector, row_t *row_ids,
                              idx_t update_count) {
	validity.Update(transaction, column_index, update_vector, row_ids, update_count);
	auto &child_entries = StructVector::GetEntries(update_vector);
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->Update(transaction, column_index, *child_entries[i], row_ids, update_count);
	}
}

void StructColumnData::UpdateColumn(TransactionData transaction, const vector<column_t> &column_path,
                                    Vector &update_vector, row_t *row_ids, idx_t update_count, idx_t depth) {
	// we can never DIRECTLY update a struct column
	if (depth >= column_path.size()) {
		throw InternalException("Attempting to directly update a struct column - this should not be possible");
	}
	auto update_column = column_path[depth];
	if (update_column == 0) {
		// update the validity column
		validity.UpdateColumn(transaction, column_path, update_vector, row_ids, update_count, depth + 1);
	} else {
		if (update_column > sub_columns.size()) {
			throw InternalException("Update column_path out of range");
		}
		sub_columns[update_column - 1]->UpdateColumn(transaction, column_path, update_vector, row_ids, update_count,
		                                             depth + 1);
	}
}

unique_ptr<BaseStatistics> StructColumnData::GetUpdateStatistics() {
	// check if any child column has updates
	auto stats = BaseStatistics::CreateEmpty(type);
	auto validity_stats = validity.GetUpdateStatistics();
	if (validity_stats) {
		stats.Merge(*validity_stats);
	}
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		auto child_stats = sub_columns[i]->GetUpdateStatistics();
		if (child_stats) {
			StructStats::SetChildStats(stats, i, std::move(child_stats));
		}
	}
	return stats.ToUnique();
}

void StructColumnData::FetchRow(TransactionData transaction, ColumnFetchState &state, row_t row_id, Vector &result,
                                idx_t result_idx) {
	// fetch validity mask
	auto &child_entries = StructVector::GetEntries(result);
	// insert any child states that are required
	for (idx_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
		auto child_state = make_uniq<ColumnFetchState>();
		state.child_states.push_back(std::move(child_state));
	}
	// fetch the validity state
	validity.FetchRow(transaction, *state.child_states[0], row_id, result, result_idx);
	// fetch the sub-column states
	for (idx_t i = 0; i < child_entries.size(); i++) {
		sub_columns[i]->FetchRow(transaction, *state.child_states[i + 1], row_id, *child_entries[i], result_idx);
	}
}

void StructColumnData::CommitDropColumn() {
	validity.CommitDropColumn();
	for (auto &sub_column : sub_columns) {
		sub_column->CommitDropColumn();
	}
}

struct StructColumnCheckpointState : public ColumnCheckpointState {
	StructColumnCheckpointState(RowGroup &row_group, ColumnData &column_data,
	                            PartialBlockManager &partial_block_manager)
	    : ColumnCheckpointState(row_group, column_data, partial_block_manager) {
		global_stats = StructStats::CreateEmpty(column_data.type).ToUnique();
	}

	unique_ptr<ColumnCheckpointState> validity_state;
	vector<unique_ptr<ColumnCheckpointState>> child_states;

public:
	unique_ptr<BaseStatistics> GetStatistics() override {
		auto stats = StructStats::CreateEmpty(column_data.type);
		for (idx_t i = 0; i < child_states.size(); i++) {
			StructStats::SetChildStats(stats, i, child_states[i]->GetStatistics());
		}
		return stats.ToUnique();
	}

	void WriteDataPointers(RowGroupWriter &writer, Serializer &serializer) override {
		serializer.WriteObject(101, "validity",
		                       [&](Serializer &serializer) { validity_state->WriteDataPointers(writer, serializer); });
		serializer.WriteList(102, "sub_columns", child_states.size(), [&](Serializer::List &list, idx_t i) {
			auto &state = child_states[i];
			list.WriteObject([&](Serializer &serializer) { state->WriteDataPointers(writer, serializer); });
		});
	}
};

unique_ptr<ColumnCheckpointState> StructColumnData::CreateCheckpointState(RowGroup &row_group,
                                                                          PartialBlockManager &partial_block_manager) {
	return make_uniq<StructColumnCheckpointState>(row_group, *this, partial_block_manager);
}

unique_ptr<ColumnCheckpointState> StructColumnData::Checkpoint(RowGroup &row_group,
                                                               PartialBlockManager &partial_block_manager,
                                                               ColumnCheckpointInfo &checkpoint_info) {
	auto checkpoint_state = make_uniq<StructColumnCheckpointState>(row_group, *this, partial_block_manager);
	checkpoint_state->validity_state = validity.Checkpoint(row_group, partial_block_manager, checkpoint_info);
	for (auto &sub_column : sub_columns) {
		checkpoint_state->child_states.push_back(
		    sub_column->Checkpoint(row_group, partial_block_manager, checkpoint_info));
	}
	return std::move(checkpoint_state);
}

void StructColumnData::DeserializeColumn(Deserializer &deserializer) {
	deserializer.ReadObject(101, "validity",
	                        [&](Deserializer &deserializer) { validity.DeserializeColumn(deserializer); });

	deserializer.ReadList(102, "sub_columns", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &item) { sub_columns[i]->DeserializeColumn(item); });
	});

	this->count = validity.count;
}

void StructColumnData::GetColumnSegmentInfo(duckdb::idx_t row_group_index, vector<duckdb::idx_t> col_path,
                                            vector<duckdb::ColumnSegmentInfo> &result) {
	col_path.push_back(0);
	validity.GetColumnSegmentInfo(row_group_index, col_path, result);
	for (idx_t i = 0; i < sub_columns.size(); i++) {
		col_path.back() = i + 1;
		sub_columns[i]->GetColumnSegmentInfo(row_group_index, col_path, result);
	}
}

void StructColumnData::Verify(RowGroup &parent) {
#ifdef DEBUG
	ColumnData::Verify(parent);
	validity.Verify(parent);
	for (auto &sub_column : sub_columns) {
		sub_column->Verify(parent);
	}
#endif
}

} // namespace duckdb





namespace duckdb {

void TableStatistics::Initialize(const vector<LogicalType> &types, PersistentTableData &data) {
	D_ASSERT(Empty());

	column_stats = std::move(data.table_stats.column_stats);
	if (column_stats.size() != types.size()) { // LCOV_EXCL_START
		throw IOException("Table statistics column count is not aligned with table column count. Corrupt file?");
	} // LCOV_EXCL_STOP
}

void TableStatistics::InitializeEmpty(const vector<LogicalType> &types) {
	D_ASSERT(Empty());

	for (auto &type : types) {
		column_stats.push_back(ColumnStatistics::CreateEmptyStats(type));
	}
}

void TableStatistics::InitializeAddColumn(TableStatistics &parent, const LogicalType &new_column_type) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
	column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_column_type));
}

void TableStatistics::InitializeRemoveColumn(TableStatistics &parent, idx_t removed_column) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i != removed_column) {
			column_stats.push_back(parent.column_stats[i]);
		}
	}
}

void TableStatistics::InitializeAlterType(TableStatistics &parent, idx_t changed_idx, const LogicalType &new_type) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		if (i == changed_idx) {
			column_stats.push_back(ColumnStatistics::CreateEmptyStats(new_type));
		} else {
			column_stats.push_back(parent.column_stats[i]);
		}
	}
}

void TableStatistics::InitializeAddConstraint(TableStatistics &parent) {
	D_ASSERT(Empty());

	lock_guard<mutex> stats_lock(parent.stats_lock);
	for (idx_t i = 0; i < parent.column_stats.size(); i++) {
		column_stats.push_back(parent.column_stats[i]);
	}
}

void TableStatistics::MergeStats(TableStatistics &other) {
	auto l = GetLock();
	D_ASSERT(column_stats.size() == other.column_stats.size());
	for (idx_t i = 0; i < column_stats.size(); i++) {
		column_stats[i]->Merge(*other.column_stats[i]);
	}
}

void TableStatistics::MergeStats(idx_t i, BaseStatistics &stats) {
	auto l = GetLock();
	MergeStats(*l, i, stats);
}

void TableStatistics::MergeStats(TableStatisticsLock &lock, idx_t i, BaseStatistics &stats) {
	column_stats[i]->Statistics().Merge(stats);
}

ColumnStatistics &TableStatistics::GetStats(idx_t i) {
	return *column_stats[i];
}

unique_ptr<BaseStatistics> TableStatistics::CopyStats(idx_t i) {
	lock_guard<mutex> l(stats_lock);
	auto result = column_stats[i]->Statistics().Copy();
	if (column_stats[i]->HasDistinctStats()) {
		result.SetDistinctCount(column_stats[i]->DistinctStats().GetCount());
	}
	return result.ToUnique();
}

void TableStatistics::CopyStats(TableStatistics &other) {
	for (auto &stats : column_stats) {
		other.column_stats.push_back(stats->Copy());
	}
}

void TableStatistics::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "column_stats", column_stats);
}

void TableStatistics::Deserialize(Deserializer &deserializer, ColumnList &columns) {
	auto physical_columns = columns.Physical();

	auto iter = physical_columns.begin();
	deserializer.ReadList(100, "column_stats", [&](Deserializer::List &list, idx_t i) {
		auto &col = *iter;
		iter.operator++();

		auto type = col.GetType();
		deserializer.Set<LogicalType &>(type);

		column_stats.push_back(list.ReadElement<shared_ptr<ColumnStatistics>>());

		deserializer.Unset<LogicalType>();
	});
}

unique_ptr<TableStatisticsLock> TableStatistics::GetLock() {
	return make_uniq<TableStatisticsLock>(stats_lock);
}

bool TableStatistics::Empty() {
	return column_stats.empty();
}

} // namespace duckdb









#include <algorithm>

namespace duckdb {

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type);
static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type);

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type);
static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type);
static UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type);
static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type);

UpdateSegment::UpdateSegment(ColumnData &column_data)
    : column_data(column_data), stats(column_data.type), heap(BufferAllocator::Get(column_data.GetDatabase())) {
	auto physical_type = column_data.type.InternalType();

	this->type_size = GetTypeIdSize(physical_type);

	this->initialize_update_function = GetInitializeUpdateFunction(physical_type);
	this->fetch_update_function = GetFetchUpdateFunction(physical_type);
	this->fetch_committed_function = GetFetchCommittedFunction(physical_type);
	this->fetch_committed_range = GetFetchCommittedRangeFunction(physical_type);
	this->fetch_row_function = GetFetchRowFunction(physical_type);
	this->merge_update_function = GetMergeUpdateFunction(physical_type);
	this->rollback_update_function = GetRollbackUpdateFunction(physical_type);
	this->statistics_update_function = GetStatisticsUpdateFunction(physical_type);
}

UpdateSegment::~UpdateSegment() {
}

//===--------------------------------------------------------------------===//
// Update Info Helpers
//===--------------------------------------------------------------------===//
Value UpdateInfo::GetValue(idx_t index) {
	auto &type = segment->column_data.type;

	switch (type.id()) {
	case LogicalTypeId::VALIDITY:
		return Value::BOOLEAN(reinterpret_cast<bool *>(tuple_data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(reinterpret_cast<int32_t *>(tuple_data)[index]);
	default:
		throw NotImplementedException("Unimplemented type for UpdateInfo::GetValue");
	}
}

void UpdateInfo::Print() {
	Printer::Print(ToString());
}

string UpdateInfo::ToString() {
	auto &type = segment->column_data.type;
	string result = "Update Info [" + type.ToString() + ", Count: " + to_string(N) +
	                ", Transaction Id: " + to_string(version_number) + "]\n";
	for (idx_t i = 0; i < N; i++) {
		result += to_string(tuples[i]) + ": " + GetValue(i).ToString() + "\n";
	}
	if (next) {
		result += "\nChild Segment: " + next->ToString();
	}
	return result;
}

void UpdateInfo::Verify() {
#ifdef DEBUG
	for (idx_t i = 1; i < N; i++) {
		D_ASSERT(tuples[i] > tuples[i - 1] && tuples[i] < STANDARD_VECTOR_SIZE);
	}
#endif
}

//===--------------------------------------------------------------------===//
// Update Fetch
//===--------------------------------------------------------------------===//
static void MergeValidityInfo(UpdateInfo *current, ValidityMask &result_mask) {
	auto info_data = reinterpret_cast<bool *>(current->tuple_data);
	for (idx_t i = 0; i < current->N; i++) {
		result_mask.Set(current->tuples[i], info_data[i]);
	}
}

static void UpdateMergeValidity(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info,
                                Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id,
	                                  [&](UpdateInfo *current) { MergeValidityInfo(current, result_mask); });
}

template <class T>
static void MergeUpdateInfo(UpdateInfo *current, T *result_data) {
	auto info_data = reinterpret_cast<T *>(current->tuple_data);
	if (current->N == STANDARD_VECTOR_SIZE) {
		// special case: update touches ALL tuples of this vector
		// in this case we can just memcpy the data
		// since the layout of the update info is guaranteed to be [0, 1, 2, 3, ...]
		memcpy(result_data, info_data, sizeof(T) * current->N);
	} else {
		for (idx_t i = 0; i < current->N; i++) {
			result_data[current->tuples[i]] = info_data[i];
		}
	}
}

template <class T>
static void UpdateMergeFetch(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id,
	                                  [&](UpdateInfo *current) { MergeUpdateInfo<T>(current, result_data); });
}

static UpdateSegment::fetch_update_function_t GetFetchUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return UpdateMergeValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return UpdateMergeFetch<int8_t>;
	case PhysicalType::INT16:
		return UpdateMergeFetch<int16_t>;
	case PhysicalType::INT32:
		return UpdateMergeFetch<int32_t>;
	case PhysicalType::INT64:
		return UpdateMergeFetch<int64_t>;
	case PhysicalType::UINT8:
		return UpdateMergeFetch<uint8_t>;
	case PhysicalType::UINT16:
		return UpdateMergeFetch<uint16_t>;
	case PhysicalType::UINT32:
		return UpdateMergeFetch<uint32_t>;
	case PhysicalType::UINT64:
		return UpdateMergeFetch<uint64_t>;
	case PhysicalType::INT128:
		return UpdateMergeFetch<hugeint_t>;
	case PhysicalType::FLOAT:
		return UpdateMergeFetch<float>;
	case PhysicalType::DOUBLE:
		return UpdateMergeFetch<double>;
	case PhysicalType::INTERVAL:
		return UpdateMergeFetch<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateMergeFetch<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchUpdates(TransactionData transaction, idx_t vector_index, Vector &result) {
	auto lock_handle = lock.GetSharedLock();
	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_update_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                      result);
}

//===--------------------------------------------------------------------===//
// Fetch Committed
//===--------------------------------------------------------------------===//
static void FetchCommittedValidity(UpdateInfo *info, Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	MergeValidityInfo(info, result_mask);
}

template <class T>
static void TemplatedFetchCommitted(UpdateInfo *info, Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	MergeUpdateInfo<T>(info, result_data);
}

static UpdateSegment::fetch_committed_function_t GetFetchCommittedFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchCommittedValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommitted<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommitted<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommitted<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommitted<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommitted<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommitted<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommitted<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommitted<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommitted<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommitted<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommitted<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommitted<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommitted<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommitted(idx_t vector_index, Vector &result) {
	auto lock_handle = lock.GetSharedLock();

	if (!root) {
		return;
	}
	if (!root->info[vector_index]) {
		return;
	}
	// FIXME: normalify if this is not the case... need to pass in count?
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	fetch_committed_function(root->info[vector_index]->info.get(), result);
}

//===--------------------------------------------------------------------===//
// Fetch Range
//===--------------------------------------------------------------------===//
static void MergeUpdateInfoRangeValidity(UpdateInfo *current, idx_t start, idx_t end, idx_t result_offset,
                                         ValidityMask &result_mask) {
	auto info_data = reinterpret_cast<bool *>(current->tuple_data);
	for (idx_t i = 0; i < current->N; i++) {
		auto tuple_idx = current->tuples[i];
		if (tuple_idx < start) {
			continue;
		} else if (tuple_idx >= end) {
			break;
		}
		auto result_idx = result_offset + tuple_idx - start;
		result_mask.Set(result_idx, info_data[i]);
	}
}

static void FetchCommittedRangeValidity(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset, Vector &result) {
	auto &result_mask = FlatVector::Validity(result);
	MergeUpdateInfoRangeValidity(info, start, end, result_offset, result_mask);
}

template <class T>
static void MergeUpdateInfoRange(UpdateInfo *current, idx_t start, idx_t end, idx_t result_offset, T *result_data) {
	auto info_data = reinterpret_cast<T *>(current->tuple_data);
	for (idx_t i = 0; i < current->N; i++) {
		auto tuple_idx = current->tuples[i];
		if (tuple_idx < start) {
			continue;
		} else if (tuple_idx >= end) {
			break;
		}
		auto result_idx = result_offset + tuple_idx - start;
		result_data[result_idx] = info_data[i];
	}
}

template <class T>
static void TemplatedFetchCommittedRange(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset,
                                         Vector &result) {
	auto result_data = FlatVector::GetData<T>(result);
	MergeUpdateInfoRange<T>(info, start, end, result_offset, result_data);
}

static UpdateSegment::fetch_committed_range_function_t GetFetchCommittedRangeFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchCommittedRangeValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchCommittedRange<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchCommittedRange<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchCommittedRange<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchCommittedRange<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchCommittedRange<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchCommittedRange<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchCommittedRange<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchCommittedRange<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchCommittedRange<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchCommittedRange<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchCommittedRange<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchCommittedRange<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchCommittedRange<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

void UpdateSegment::FetchCommittedRange(idx_t start_row, idx_t count, Vector &result) {
	D_ASSERT(count > 0);
	if (!root) {
		return;
	}
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);

	idx_t end_row = start_row + count;
	idx_t start_vector = start_row / STANDARD_VECTOR_SIZE;
	idx_t end_vector = (end_row - 1) / STANDARD_VECTOR_SIZE;
	D_ASSERT(start_vector <= end_vector);
	D_ASSERT(end_vector < Storage::ROW_GROUP_VECTOR_COUNT);

	for (idx_t vector_idx = start_vector; vector_idx <= end_vector; vector_idx++) {
		if (!root->info[vector_idx]) {
			continue;
		}
		idx_t start_in_vector = vector_idx == start_vector ? start_row - start_vector * STANDARD_VECTOR_SIZE : 0;
		idx_t end_in_vector =
		    vector_idx == end_vector ? end_row - end_vector * STANDARD_VECTOR_SIZE : STANDARD_VECTOR_SIZE;
		D_ASSERT(start_in_vector < end_in_vector);
		D_ASSERT(end_in_vector > 0 && end_in_vector <= STANDARD_VECTOR_SIZE);
		idx_t result_offset = ((vector_idx * STANDARD_VECTOR_SIZE) + start_in_vector) - start_row;
		fetch_committed_range(root->info[vector_idx]->info.get(), start_in_vector, end_in_vector, result_offset,
		                      result);
	}
}

//===--------------------------------------------------------------------===//
// Fetch Row
//===--------------------------------------------------------------------===//
static void FetchRowValidity(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, idx_t row_idx,
                             Vector &result, idx_t result_idx) {
	auto &result_mask = FlatVector::Validity(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		auto info_data = reinterpret_cast<bool *>(current->tuple_data);
		// FIXME: we could do a binary search in here
		for (idx_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_idx) {
				result_mask.Set(result_idx, info_data[i]);
				break;
			} else if (current->tuples[i] > row_idx) {
				break;
			}
		}
	});
}

template <class T>
static void TemplatedFetchRow(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, idx_t row_idx,
                              Vector &result, idx_t result_idx) {
	auto result_data = FlatVector::GetData<T>(result);
	UpdateInfo::UpdatesForTransaction(info, start_time, transaction_id, [&](UpdateInfo *current) {
		auto info_data = (T *)current->tuple_data;
		// FIXME: we could do a binary search in here
		for (idx_t i = 0; i < current->N; i++) {
			if (current->tuples[i] == row_idx) {
				result_data[result_idx] = info_data[i];
				break;
			} else if (current->tuples[i] > row_idx) {
				break;
			}
		}
	});
}

static UpdateSegment::fetch_row_function_t GetFetchRowFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return FetchRowValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedFetchRow<int8_t>;
	case PhysicalType::INT16:
		return TemplatedFetchRow<int16_t>;
	case PhysicalType::INT32:
		return TemplatedFetchRow<int32_t>;
	case PhysicalType::INT64:
		return TemplatedFetchRow<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedFetchRow<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedFetchRow<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedFetchRow<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedFetchRow<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedFetchRow<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedFetchRow<float>;
	case PhysicalType::DOUBLE:
		return TemplatedFetchRow<double>;
	case PhysicalType::INTERVAL:
		return TemplatedFetchRow<interval_t>;
	case PhysicalType::VARCHAR:
		return TemplatedFetchRow<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment fetch row");
	}
}

void UpdateSegment::FetchRow(TransactionData transaction, idx_t row_id, Vector &result, idx_t result_idx) {
	if (!root) {
		return;
	}
	idx_t vector_index = (row_id - column_data.start) / STANDARD_VECTOR_SIZE;
	if (!root->info[vector_index]) {
		return;
	}
	idx_t row_in_vector = (row_id - column_data.start) - vector_index * STANDARD_VECTOR_SIZE;
	fetch_row_function(transaction.start_time, transaction.transaction_id, root->info[vector_index]->info.get(),
	                   row_in_vector, result, result_idx);
}

//===--------------------------------------------------------------------===//
// Rollback update
//===--------------------------------------------------------------------===//
template <class T>
static void RollbackUpdate(UpdateInfo &base_info, UpdateInfo &rollback_info) {
	auto base_data = (T *)base_info.tuple_data;
	auto rollback_data = (T *)rollback_info.tuple_data;
	idx_t base_offset = 0;
	for (idx_t i = 0; i < rollback_info.N; i++) {
		auto id = rollback_info.tuples[i];
		while (base_info.tuples[base_offset] < id) {
			base_offset++;
			D_ASSERT(base_offset < base_info.N);
		}
		base_data[base_offset] = rollback_data[i];
	}
}

static UpdateSegment::rollback_update_function_t GetRollbackUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return RollbackUpdate<bool>;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return RollbackUpdate<int8_t>;
	case PhysicalType::INT16:
		return RollbackUpdate<int16_t>;
	case PhysicalType::INT32:
		return RollbackUpdate<int32_t>;
	case PhysicalType::INT64:
		return RollbackUpdate<int64_t>;
	case PhysicalType::UINT8:
		return RollbackUpdate<uint8_t>;
	case PhysicalType::UINT16:
		return RollbackUpdate<uint16_t>;
	case PhysicalType::UINT32:
		return RollbackUpdate<uint32_t>;
	case PhysicalType::UINT64:
		return RollbackUpdate<uint64_t>;
	case PhysicalType::INT128:
		return RollbackUpdate<hugeint_t>;
	case PhysicalType::FLOAT:
		return RollbackUpdate<float>;
	case PhysicalType::DOUBLE:
		return RollbackUpdate<double>;
	case PhysicalType::INTERVAL:
		return RollbackUpdate<interval_t>;
	case PhysicalType::VARCHAR:
		return RollbackUpdate<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

void UpdateSegment::RollbackUpdate(UpdateInfo &info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();

	// move the data from the UpdateInfo back into the base info
	D_ASSERT(root->info[info.vector_index]);
	rollback_update_function(*root->info[info.vector_index]->info, info);

	// clean up the update chain
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Cleanup Update
//===--------------------------------------------------------------------===//
void UpdateSegment::CleanupUpdateInternal(const StorageLockKey &lock, UpdateInfo &info) {
	D_ASSERT(info.prev);
	auto prev = info.prev;
	prev->next = info.next;
	if (prev->next) {
		prev->next->prev = prev;
	}
}

void UpdateSegment::CleanupUpdate(UpdateInfo &info) {
	// obtain an exclusive lock
	auto lock_handle = lock.GetExclusiveLock();
	CleanupUpdateInternal(*lock_handle, info);
}

//===--------------------------------------------------------------------===//
// Check for conflicts in update
//===--------------------------------------------------------------------===//
static void CheckForConflicts(UpdateInfo *info, TransactionData transaction, row_t *ids, const SelectionVector &sel,
                              idx_t count, row_t offset, UpdateInfo *&node) {
	if (!info) {
		return;
	}
	if (info->version_number == transaction.transaction_id) {
		// this UpdateInfo belongs to the current transaction, set it in the node
		node = info;
	} else if (info->version_number > transaction.start_time) {
		// potential conflict, check that tuple ids do not conflict
		// as both ids and info->tuples are sorted, this is similar to a merge join
		idx_t i = 0, j = 0;
		while (true) {
			auto id = ids[sel.get_index(i)] - offset;
			if (id == info->tuples[j]) {
				throw TransactionException("Conflict on update!");
			} else if (id < info->tuples[j]) {
				// id < the current tuple in info, move to next id
				i++;
				if (i == count) {
					break;
				}
			} else {
				// id > the current tuple, move to next tuple in info
				j++;
				if (j == info->N) {
					break;
				}
			}
		}
	}
	CheckForConflicts(info->next, transaction, ids, sel, count, offset, node);
}

//===--------------------------------------------------------------------===//
// Initialize update info
//===--------------------------------------------------------------------===//
void UpdateSegment::InitializeUpdateInfo(UpdateInfo &info, row_t *ids, const SelectionVector &sel, idx_t count,
                                         idx_t vector_index, idx_t vector_offset) {
	info.segment = this;
	info.vector_index = vector_index;
	info.prev = nullptr;
	info.next = nullptr;

	// set up the tuple ids
	info.N = count;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto id = ids[idx];
		D_ASSERT(idx_t(id) >= vector_offset && idx_t(id) < vector_offset + STANDARD_VECTOR_SIZE);
		info.tuples[i] = id - vector_offset;
	};
}

static void InitializeUpdateValidity(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                                     const SelectionVector &sel) {
	auto &update_mask = FlatVector::Validity(update);
	auto tuple_data = reinterpret_cast<bool *>(update_info->tuple_data);

	if (!update_mask.AllValid()) {
		for (idx_t i = 0; i < update_info->N; i++) {
			auto idx = sel.get_index(i);
			tuple_data[i] = update_mask.RowIsValidUnsafe(idx);
		}
	} else {
		for (idx_t i = 0; i < update_info->N; i++) {
			tuple_data[i] = true;
		}
	}

	auto &base_mask = FlatVector::Validity(base_data);
	auto base_tuple_data = reinterpret_cast<bool *>(base_info->tuple_data);
	if (!base_mask.AllValid()) {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = base_mask.RowIsValidUnsafe(base_info->tuples[i]);
		}
	} else {
		for (idx_t i = 0; i < base_info->N; i++) {
			base_tuple_data[i] = true;
		}
	}
}

struct UpdateSelectElement {
	template <class T>
	static T Operation(UpdateSegment *segment, T element) {
		return element;
	}
};

template <>
string_t UpdateSelectElement::Operation(UpdateSegment *segment, string_t element) {
	return element.IsInlined() ? element : segment->GetStringHeap().AddBlob(element);
}

template <class T>
static void InitializeUpdateData(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                                 const SelectionVector &sel) {
	auto update_data = FlatVector::GetData<T>(update);
	auto tuple_data = (T *)update_info->tuple_data;

	for (idx_t i = 0; i < update_info->N; i++) {
		auto idx = sel.get_index(i);
		tuple_data[i] = update_data[idx];
	}

	auto base_array_data = FlatVector::GetData<T>(base_data);
	auto &base_validity = FlatVector::Validity(base_data);
	auto base_tuple_data = (T *)base_info->tuple_data;
	for (idx_t i = 0; i < base_info->N; i++) {
		auto base_idx = base_info->tuples[i];
		if (!base_validity.RowIsValid(base_idx)) {
			continue;
		}
		base_tuple_data[i] = UpdateSelectElement::Operation<T>(base_info->segment, base_array_data[base_idx]);
	}
}

static UpdateSegment::initialize_update_function_t GetInitializeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return InitializeUpdateValidity;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return InitializeUpdateData<int8_t>;
	case PhysicalType::INT16:
		return InitializeUpdateData<int16_t>;
	case PhysicalType::INT32:
		return InitializeUpdateData<int32_t>;
	case PhysicalType::INT64:
		return InitializeUpdateData<int64_t>;
	case PhysicalType::UINT8:
		return InitializeUpdateData<uint8_t>;
	case PhysicalType::UINT16:
		return InitializeUpdateData<uint16_t>;
	case PhysicalType::UINT32:
		return InitializeUpdateData<uint32_t>;
	case PhysicalType::UINT64:
		return InitializeUpdateData<uint64_t>;
	case PhysicalType::INT128:
		return InitializeUpdateData<hugeint_t>;
	case PhysicalType::FLOAT:
		return InitializeUpdateData<float>;
	case PhysicalType::DOUBLE:
		return InitializeUpdateData<double>;
	case PhysicalType::INTERVAL:
		return InitializeUpdateData<interval_t>;
	case PhysicalType::VARCHAR:
		return InitializeUpdateData<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for update segment");
	}
}

//===--------------------------------------------------------------------===//
// Merge update info
//===--------------------------------------------------------------------===//
template <class F1, class F2, class F3>
static idx_t MergeLoop(row_t a[], sel_t b[], idx_t acount, idx_t bcount, idx_t aoffset, F1 merge, F2 pick_a, F3 pick_b,
                       const SelectionVector &asel) {
	idx_t aidx = 0, bidx = 0;
	idx_t count = 0;
	while (aidx < acount && bidx < bcount) {
		auto a_index = asel.get_index(aidx);
		auto a_id = a[a_index] - aoffset;
		auto b_id = b[bidx];
		if (a_id == b_id) {
			merge(a_id, a_index, bidx, count);
			aidx++;
			bidx++;
			count++;
		} else if (a_id < b_id) {
			pick_a(a_id, a_index, count);
			aidx++;
			count++;
		} else {
			pick_b(b_id, bidx, count);
			bidx++;
			count++;
		}
	}
	for (; aidx < acount; aidx++) {
		auto a_index = asel.get_index(aidx);
		pick_a(a[a_index] - aoffset, a_index, count);
		count++;
	}
	for (; bidx < bcount; bidx++) {
		pick_b(b[bidx], bidx, count);
		count++;
	}
	return count;
}

struct ExtractStandardEntry {
	template <class T, class V>
	static T Extract(V *data, idx_t entry) {
		return data[entry];
	}
};

struct ExtractValidityEntry {
	template <class T, class V>
	static T Extract(V *data, idx_t entry) {
		return data->RowIsValid(entry);
	}
};

template <class T, class V, class OP = ExtractStandardEntry>
static void MergeUpdateLoopInternal(UpdateInfo *base_info, V *base_table_data, UpdateInfo *update_info,
                                    V *update_vector_data, row_t *ids, idx_t count, const SelectionVector &sel) {
	auto base_id = base_info->segment->column_data.start + base_info->vector_index * STANDARD_VECTOR_SIZE;
#ifdef DEBUG
	// all of these should be sorted, otherwise the below algorithm does not work
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sel.get_index(i - 1);
		auto idx = sel.get_index(i);
		D_ASSERT(ids[idx] > ids[prev_idx] && ids[idx] >= row_t(base_id) &&
		         ids[idx] < row_t(base_id + STANDARD_VECTOR_SIZE));
	}
#endif

	// we have a new batch of updates (update, ids, count)
	// we already have existing updates (base_info)
	// and potentially, this transaction already has updates present (update_info)
	// we need to merge these all together so that the latest updates get merged into base_info
	// and the "old" values (fetched from EITHER base_info OR from base_data) get placed into update_info
	auto base_info_data = (T *)base_info->tuple_data;
	auto update_info_data = (T *)update_info->tuple_data;

	// we first do the merging of the old values
	// what we are trying to do here is update the "update_info" of this transaction with all the old data we require
	// this means we need to merge (1) any previously updated values (stored in update_info->tuples)
	// together with (2)
	// to simplify this, we create new arrays here
	// we memcpy these over afterwards
	T result_values[STANDARD_VECTOR_SIZE];
	sel_t result_ids[STANDARD_VECTOR_SIZE];

	idx_t base_info_offset = 0;
	idx_t update_info_offset = 0;
	idx_t result_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		// we have to merge the info for "ids[i]"
		auto update_id = ids[idx] - base_id;

		while (update_info_offset < update_info->N && update_info->tuples[update_info_offset] < update_id) {
			// old id comes before the current id: write it
			result_values[result_offset] = update_info_data[update_info_offset];
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
		}
		// write the new id
		if (update_info_offset < update_info->N && update_info->tuples[update_info_offset] == update_id) {
			// we have an id that is equivalent in the current update info: write the update info
			result_values[result_offset] = update_info_data[update_info_offset];
			result_ids[result_offset++] = update_info->tuples[update_info_offset];
			update_info_offset++;
			continue;
		}

		/// now check if we have the current update_id in the base_info, or if we should fetch it from the base data
		while (base_info_offset < base_info->N && base_info->tuples[base_info_offset] < update_id) {
			base_info_offset++;
		}
		if (base_info_offset < base_info->N && base_info->tuples[base_info_offset] == update_id) {
			// it is! we have to move the tuple from base_info->ids[base_info_offset] to update_info
			result_values[result_offset] = base_info_data[base_info_offset];
		} else {
			// it is not! we have to move base_table_data[update_id] to update_info
			result_values[result_offset] = UpdateSelectElement::Operation<T>(
			    base_info->segment, OP::template Extract<T, V>(base_table_data, update_id));
		}
		result_ids[result_offset++] = update_id;
	}
	// write any remaining entries from the old updates
	while (update_info_offset < update_info->N) {
		result_values[result_offset] = update_info_data[update_info_offset];
		result_ids[result_offset++] = update_info->tuples[update_info_offset];
		update_info_offset++;
	}
	// now copy them back
	update_info->N = result_offset;
	memcpy(update_info_data, result_values, result_offset * sizeof(T));
	memcpy(update_info->tuples, result_ids, result_offset * sizeof(sel_t));

	// now we merge the new values into the base_info
	result_offset = 0;
	auto pick_new = [&](idx_t id, idx_t aidx, idx_t count) {
		result_values[result_offset] = OP::template Extract<T, V>(update_vector_data, aidx);
		result_ids[result_offset] = id;
		result_offset++;
	};
	auto pick_old = [&](idx_t id, idx_t bidx, idx_t count) {
		result_values[result_offset] = base_info_data[bidx];
		result_ids[result_offset] = id;
		result_offset++;
	};
	// now we perform a merge of the new ids with the old ids
	auto merge = [&](idx_t id, idx_t aidx, idx_t bidx, idx_t count) {
		pick_new(id, aidx, count);
	};
	MergeLoop(ids, base_info->tuples, count, base_info->N, base_id, merge, pick_new, pick_old, sel);

	base_info->N = result_offset;
	memcpy(base_info_data, result_values, result_offset * sizeof(T));
	memcpy(base_info->tuples, result_ids, result_offset * sizeof(sel_t));
}

static void MergeValidityLoop(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                              row_t *ids, idx_t count, const SelectionVector &sel) {
	auto &base_validity = FlatVector::Validity(base_data);
	auto &update_validity = FlatVector::Validity(update);
	MergeUpdateLoopInternal<bool, ValidityMask, ExtractValidityEntry>(base_info, &base_validity, update_info,
	                                                                  &update_validity, ids, count, sel);
}

template <class T>
static void MergeUpdateLoop(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info, Vector &update,
                            row_t *ids, idx_t count, const SelectionVector &sel) {
	auto base_table_data = FlatVector::GetData<T>(base_data);
	auto update_vector_data = FlatVector::GetData<T>(update);
	MergeUpdateLoopInternal<T, T>(base_info, base_table_data, update_info, update_vector_data, ids, count, sel);
}

static UpdateSegment::merge_update_function_t GetMergeUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return MergeValidityLoop;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return MergeUpdateLoop<int8_t>;
	case PhysicalType::INT16:
		return MergeUpdateLoop<int16_t>;
	case PhysicalType::INT32:
		return MergeUpdateLoop<int32_t>;
	case PhysicalType::INT64:
		return MergeUpdateLoop<int64_t>;
	case PhysicalType::UINT8:
		return MergeUpdateLoop<uint8_t>;
	case PhysicalType::UINT16:
		return MergeUpdateLoop<uint16_t>;
	case PhysicalType::UINT32:
		return MergeUpdateLoop<uint32_t>;
	case PhysicalType::UINT64:
		return MergeUpdateLoop<uint64_t>;
	case PhysicalType::INT128:
		return MergeUpdateLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return MergeUpdateLoop<float>;
	case PhysicalType::DOUBLE:
		return MergeUpdateLoop<double>;
	case PhysicalType::INTERVAL:
		return MergeUpdateLoop<interval_t>;
	case PhysicalType::VARCHAR:
		return MergeUpdateLoop<string_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update statistics
//===--------------------------------------------------------------------===//
unique_ptr<BaseStatistics> UpdateSegment::GetStatistics() {
	lock_guard<mutex> stats_guard(stats_lock);
	return stats.statistics.ToUnique();
}

idx_t UpdateValidityStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                               SelectionVector &sel) {
	auto &mask = FlatVector::Validity(update);
	auto &validity = stats.statistics;
	if (!mask.AllValid() && !validity.CanHaveNull()) {
		for (idx_t i = 0; i < count; i++) {
			if (!mask.RowIsValid(i)) {
				validity.SetHasNull();
				break;
			}
		}
	}
	sel.Initialize(nullptr);
	return count;
}

template <class T>
idx_t TemplatedUpdateNumericStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                                       SelectionVector &sel) {
	auto update_data = FlatVector::GetData<T>(update);
	auto &mask = FlatVector::Validity(update);

	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			NumericStats::Update<T>(stats.statistics, update_data[i]);
		}
		sel.Initialize(nullptr);
		return count;
	} else {
		idx_t not_null_count = 0;
		sel.Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				sel.set_index(not_null_count++, i);
				NumericStats::Update<T>(stats.statistics, update_data[i]);
			}
		}
		return not_null_count;
	}
}

idx_t UpdateStringStatistics(UpdateSegment *segment, SegmentStatistics &stats, Vector &update, idx_t count,
                             SelectionVector &sel) {
	auto update_data = FlatVector::GetData<string_t>(update);
	auto &mask = FlatVector::Validity(update);
	if (mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			StringStats::Update(stats.statistics, update_data[i]);
			if (!update_data[i].IsInlined()) {
				update_data[i] = segment->GetStringHeap().AddBlob(update_data[i]);
			}
		}
		sel.Initialize(nullptr);
		return count;
	} else {
		idx_t not_null_count = 0;
		sel.Initialize(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < count; i++) {
			if (mask.RowIsValid(i)) {
				sel.set_index(not_null_count++, i);
				StringStats::Update(stats.statistics, update_data[i]);
				if (!update_data[i].IsInlined()) {
					update_data[i] = segment->GetStringHeap().AddBlob(update_data[i]);
				}
			}
		}
		return not_null_count;
	}
}

UpdateSegment::statistics_update_function_t GetStatisticsUpdateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
		return UpdateValidityStatistics;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedUpdateNumericStatistics<int8_t>;
	case PhysicalType::INT16:
		return TemplatedUpdateNumericStatistics<int16_t>;
	case PhysicalType::INT32:
		return TemplatedUpdateNumericStatistics<int32_t>;
	case PhysicalType::INT64:
		return TemplatedUpdateNumericStatistics<int64_t>;
	case PhysicalType::UINT8:
		return TemplatedUpdateNumericStatistics<uint8_t>;
	case PhysicalType::UINT16:
		return TemplatedUpdateNumericStatistics<uint16_t>;
	case PhysicalType::UINT32:
		return TemplatedUpdateNumericStatistics<uint32_t>;
	case PhysicalType::UINT64:
		return TemplatedUpdateNumericStatistics<uint64_t>;
	case PhysicalType::INT128:
		return TemplatedUpdateNumericStatistics<hugeint_t>;
	case PhysicalType::FLOAT:
		return TemplatedUpdateNumericStatistics<float>;
	case PhysicalType::DOUBLE:
		return TemplatedUpdateNumericStatistics<double>;
	case PhysicalType::INTERVAL:
		return TemplatedUpdateNumericStatistics<interval_t>;
	case PhysicalType::VARCHAR:
		return UpdateStringStatistics;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static idx_t SortSelectionVector(SelectionVector &sel, idx_t count, row_t *ids) {
	D_ASSERT(count > 0);

	bool is_sorted = true;
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sel.get_index(i - 1);
		auto idx = sel.get_index(i);
		if (ids[idx] <= ids[prev_idx]) {
			is_sorted = false;
			break;
		}
	}
	if (is_sorted) {
		// already sorted: bailout
		return count;
	}
	// not sorted: need to sort the selection vector
	SelectionVector sorted_sel(count);
	for (idx_t i = 0; i < count; i++) {
		sorted_sel.set_index(i, sel.get_index(i));
	}
	std::sort(sorted_sel.data(), sorted_sel.data() + count, [&](sel_t l, sel_t r) { return ids[l] < ids[r]; });
	// eliminate any duplicates
	idx_t pos = 1;
	for (idx_t i = 1; i < count; i++) {
		auto prev_idx = sorted_sel.get_index(i - 1);
		auto idx = sorted_sel.get_index(i);
		D_ASSERT(ids[idx] >= ids[prev_idx]);
		if (ids[prev_idx] != ids[idx]) {
			sorted_sel.set_index(pos++, idx);
		}
	}
#ifdef DEBUG
	for (idx_t i = 1; i < pos; i++) {
		auto prev_idx = sorted_sel.get_index(i - 1);
		auto idx = sorted_sel.get_index(i);
		D_ASSERT(ids[idx] > ids[prev_idx]);
	}
#endif

	sel.Initialize(sorted_sel);
	D_ASSERT(pos > 0);
	return pos;
}

UpdateInfo *CreateEmptyUpdateInfo(TransactionData transaction, idx_t type_size, idx_t count,
                                  unsafe_unique_array<char> &data) {
	data = make_unsafe_uniq_array<char>(sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * STANDARD_VECTOR_SIZE);
	auto update_info = reinterpret_cast<UpdateInfo *>(data.get());
	update_info->max = STANDARD_VECTOR_SIZE;
	update_info->tuples = reinterpret_cast<sel_t *>((data_ptr_cast(update_info)) + sizeof(UpdateInfo));
	update_info->tuple_data = (data_ptr_cast(update_info)) + sizeof(UpdateInfo) + sizeof(sel_t) * update_info->max;
	update_info->version_number = transaction.transaction_id;
	return update_info;
}

void UpdateSegment::Update(TransactionData transaction, idx_t column_index, Vector &update, row_t *ids, idx_t count,
                           Vector &base_data) {
	// obtain an exclusive lock
	auto write_lock = lock.GetExclusiveLock();

	update.Flatten(count);

	// update statistics
	SelectionVector sel;
	{
		lock_guard<mutex> stats_guard(stats_lock);
		count = statistics_update_function(this, stats, update, count, sel);
	}
	if (count == 0) {
		return;
	}

	// subsequent algorithms used by the update require row ids to be (1) sorted, and (2) unique
	// this is usually the case for "standard" queries (e.g. UPDATE tbl SET x=bla WHERE cond)
	// however, for more exotic queries involving e.g. cross products/joins this might not be the case
	// hence we explicitly check here if the ids are sorted and, if not, sort + duplicate eliminate them
	count = SortSelectionVector(sel, count, ids);
	D_ASSERT(count > 0);

	// create the versions for this segment, if there are none yet
	if (!root) {
		root = make_uniq<UpdateNode>();
	}

	// get the vector index based on the first id
	// we assert that all updates must be part of the same vector
	auto first_id = ids[sel.get_index(0)];
	idx_t vector_index = (first_id - column_data.start) / STANDARD_VECTOR_SIZE;
	idx_t vector_offset = column_data.start + vector_index * STANDARD_VECTOR_SIZE;

	D_ASSERT(idx_t(first_id) >= column_data.start);
	D_ASSERT(vector_index < Storage::ROW_GROUP_VECTOR_COUNT);

	// first check the version chain
	UpdateInfo *node = nullptr;

	if (root->info[vector_index]) {
		// there is already a version here, check if there are any conflicts and search for the node that belongs to
		// this transaction in the version chain
		auto base_info = root->info[vector_index]->info.get();
		CheckForConflicts(base_info->next, transaction, ids, sel, count, vector_offset, node);

		// there are no conflicts
		// first, check if this thread has already done any updates
		auto node = base_info->next;
		while (node) {
			if (node->version_number == transaction.transaction_id) {
				// it has! use this node
				break;
			}
			node = node->next;
		}
		unsafe_unique_array<char> update_info_data;
		if (!node) {
			// no updates made yet by this transaction: initially the update info to empty
			if (transaction.transaction) {
				auto &dtransaction = transaction.transaction->Cast<DuckTransaction>();
				node = dtransaction.CreateUpdateInfo(type_size, count);
			} else {
				node = CreateEmptyUpdateInfo(transaction, type_size, count, update_info_data);
			}
			node->segment = this;
			node->vector_index = vector_index;
			node->N = 0;
			node->column_index = column_index;

			// insert the new node into the chain
			node->next = base_info->next;
			if (node->next) {
				node->next->prev = node;
			}
			node->prev = base_info;
			base_info->next = transaction.transaction ? node : nullptr;
		}
		base_info->Verify();
		node->Verify();

		// now we are going to perform the merge
		merge_update_function(base_info, base_data, node, update, ids, count, sel);

		base_info->Verify();
		node->Verify();
	} else {
		// there is no version info yet: create the top level update info and fill it with the updates
		auto result = make_uniq<UpdateNodeData>();

		result->info = make_uniq<UpdateInfo>();
		result->tuples = make_unsafe_uniq_array<sel_t>(STANDARD_VECTOR_SIZE);
		result->tuple_data = make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * type_size);
		result->info->tuples = result->tuples.get();
		result->info->tuple_data = result->tuple_data.get();
		result->info->version_number = TRANSACTION_ID_START - 1;
		result->info->column_index = column_index;
		InitializeUpdateInfo(*result->info, ids, sel, count, vector_index, vector_offset);

		// now create the transaction level update info in the undo log
		unsafe_unique_array<char> update_info_data;
		UpdateInfo *transaction_node;
		if (transaction.transaction) {
			transaction_node = transaction.transaction->CreateUpdateInfo(type_size, count);
		} else {
			transaction_node = CreateEmptyUpdateInfo(transaction, type_size, count, update_info_data);
		}

		InitializeUpdateInfo(*transaction_node, ids, sel, count, vector_index, vector_offset);

		// we write the updates in the update node data, and write the updates in the info
		initialize_update_function(transaction_node, base_data, result->info.get(), update, sel);

		result->info->next = transaction.transaction ? transaction_node : nullptr;
		result->info->prev = nullptr;
		transaction_node->next = nullptr;
		transaction_node->prev = result->info.get();
		transaction_node->column_index = column_index;

		transaction_node->Verify();
		result->info->Verify();

		root->info[vector_index] = std::move(result);
	}
}

bool UpdateSegment::HasUpdates() const {
	return root.get() != nullptr;
}

bool UpdateSegment::HasUpdates(idx_t vector_index) const {
	if (!HasUpdates()) {
		return false;
	}
	return root->info[vector_index].get();
}

bool UpdateSegment::HasUncommittedUpdates(idx_t vector_index) {
	if (!HasUpdates(vector_index)) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	auto entry = root->info[vector_index].get();
	if (entry->info->next) {
		return true;
	}
	return false;
}

bool UpdateSegment::HasUpdates(idx_t start_row_index, idx_t end_row_index) {
	if (!HasUpdates()) {
		return false;
	}
	auto read_lock = lock.GetSharedLock();
	idx_t base_vector_index = start_row_index / STANDARD_VECTOR_SIZE;
	idx_t end_vector_index = end_row_index / STANDARD_VECTOR_SIZE;
	for (idx_t i = base_vector_index; i <= end_vector_index; i++) {
		if (root->info[i]) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb




namespace duckdb {

ValidityColumnData::ValidityColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
                                       idx_t start_row, ColumnData &parent)
    : ColumnData(block_manager, info, column_index, start_row, LogicalType(LogicalTypeId::VALIDITY), &parent) {
}

bool ValidityColumnData::CheckZonemap(ColumnScanState &state, TableFilter &filter) {
	return true;
}

} // namespace duckdb





namespace duckdb {
void TableIndexList::AddIndex(unique_ptr<Index> index) {
	D_ASSERT(index);
	lock_guard<mutex> lock(indexes_lock);
	indexes.push_back(std::move(index));
}

void TableIndexList::RemoveIndex(Index &index) {
	lock_guard<mutex> lock(indexes_lock);

	for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
		auto &index_entry = indexes[index_idx];
		if (index_entry.get() == &index) {
			indexes.erase(indexes.begin() + index_idx);
			break;
		}
	}
}

bool TableIndexList::Empty() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.empty();
}

idx_t TableIndexList::Count() {
	lock_guard<mutex> lock(indexes_lock);
	return indexes.size();
}

void TableIndexList::Move(TableIndexList &other) {
	D_ASSERT(indexes.empty());
	indexes = std::move(other.indexes);
}

Index *TableIndexList::FindForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, ForeignKeyType fk_type) {
	Index *result = nullptr;
	Scan([&](Index &index) {
		if (DataTable::IsForeignKeyIndex(fk_keys, index, fk_type)) {
			result = &index;
		}
		return false;
	});
	return result;
}

void TableIndexList::VerifyForeignKey(const vector<PhysicalIndex> &fk_keys, DataChunk &chunk,
                                      ConflictManager &conflict_manager) {
	auto fk_type = conflict_manager.LookupType() == VerifyExistenceType::APPEND_FK
	                   ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE
	                   : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	// check whether the chunk can be inserted or deleted into the referenced table storage
	auto index = FindForeignKeyIndex(fk_keys, fk_type);
	if (!index) {
		throw InternalException("Internal Foreign Key error: could not find index to verify...");
	}
	conflict_manager.SetIndexCount(1);
	index->CheckConstraintsForChunk(chunk, conflict_manager);
}

vector<column_t> TableIndexList::GetRequiredColumns() {
	lock_guard<mutex> lock(indexes_lock);
	set<column_t> unique_indexes;
	for (auto &index : indexes) {
		for (auto col_index : index->column_ids) {
			unique_indexes.insert(col_index);
		}
	}
	vector<column_t> result;
	result.reserve(unique_indexes.size());
	for (auto column_index : unique_indexes) {
		result.emplace_back(column_index);
	}
	return result;
}

vector<BlockPointer> TableIndexList::SerializeIndexes(duckdb::MetadataWriter &writer) {
	vector<BlockPointer> blocks_info;
	for (auto &index : indexes) {
		blocks_info.emplace_back(index->Serialize(writer));
	}
	return blocks_info;
}

} // namespace duckdb
























namespace duckdb {

bool WriteAheadLog::Replay(AttachedDatabase &database, string &path) {
	Connection con(database.GetDatabase());
	auto initial_source = make_uniq<BufferedFileReader>(FileSystem::Get(database), path.c_str());
	if (initial_source->Finished()) {
		// WAL is empty
		return false;
	}

	con.BeginTransaction();

	// first deserialize the WAL to look for a checkpoint flag
	// if there is a checkpoint flag, we might have already flushed the contents of the WAL to disk
	ReplayState checkpoint_state(database, *con.context);
	checkpoint_state.deserialize_only = true;
	try {
		while (true) {
			// read the current entry
			BinaryDeserializer deserializer(*initial_source);
			deserializer.Begin();
			auto entry_type = deserializer.ReadProperty<WALType>(100, "wal_type");
			if (entry_type == WALType::WAL_FLUSH) {
				deserializer.End();
				// check if the file is exhausted
				if (initial_source->Finished()) {
					// we finished reading the file: break
					break;
				}
			} else {
				// replay the entry
				checkpoint_state.ReplayEntry(entry_type, deserializer);
				deserializer.End();
			}
		}
	} catch (SerializationException &ex) { // LCOV_EXCL_START
		                                   // serialization exception - torn WAL
		                                   // continue reading
	} catch (std::exception &ex) {
		Printer::PrintF("Exception in WAL playback during initial read: %s\n", ex.what());
		return false;
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback during initial read");
		return false;
	} // LCOV_EXCL_STOP
	initial_source.reset();
	if (checkpoint_state.checkpoint_id.IsValid()) {
		// there is a checkpoint flag: check if we need to deserialize the WAL
		auto &manager = database.GetStorageManager();
		if (manager.IsCheckpointClean(checkpoint_state.checkpoint_id)) {
			// the contents of the WAL have already been checkpointed
			// we can safely truncate the WAL and ignore its contents
			return true;
		}
	}

	// we need to recover from the WAL: actually set up the replay state
	BufferedFileReader reader(FileSystem::Get(database), path.c_str());
	ReplayState state(database, *con.context);

	// replay the WAL
	// note that everything is wrapped inside a try/catch block here
	// there can be errors in WAL replay because of a corrupt WAL file
	// in this case we should throw a warning but startup anyway
	try {
		while (true) {
			// read the current entry
			BinaryDeserializer deserializer(reader);
			deserializer.Begin();
			auto entry_type = deserializer.ReadProperty<WALType>(100, "wal_type");
			if (entry_type == WALType::WAL_FLUSH) {
				deserializer.End();
				con.Commit();
				// check if the file is exhausted
				if (reader.Finished()) {
					// we finished reading the file: break
					break;
				}
				con.BeginTransaction();
			} else {
				// replay the entry
				state.ReplayEntry(entry_type, deserializer);
				deserializer.End();
			}
		}
	} catch (SerializationException &ex) { // LCOV_EXCL_START
		// serialization error during WAL replay: rollback
		con.Rollback();
	} catch (std::exception &ex) {
		// FIXME: this should report a proper warning in the connection
		Printer::PrintF("Exception in WAL playback: %s\n", ex.what());
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} catch (...) {
		Printer::Print("Unknown Exception in WAL playback: %s\n");
		// exception thrown in WAL replay: rollback
		con.Rollback();
	} // LCOV_EXCL_STOP
	return false;
}

//===--------------------------------------------------------------------===//
// Replay Entries
//===--------------------------------------------------------------------===//
void ReplayState::ReplayEntry(WALType entry_type, BinaryDeserializer &deserializer) {
	switch (entry_type) {
	case WALType::CREATE_TABLE:
		ReplayCreateTable(deserializer);
		break;
	case WALType::DROP_TABLE:
		ReplayDropTable(deserializer);
		break;
	case WALType::ALTER_INFO:
		ReplayAlter(deserializer);
		break;
	case WALType::CREATE_VIEW:
		ReplayCreateView(deserializer);
		break;
	case WALType::DROP_VIEW:
		ReplayDropView(deserializer);
		break;
	case WALType::CREATE_SCHEMA:
		ReplayCreateSchema(deserializer);
		break;
	case WALType::DROP_SCHEMA:
		ReplayDropSchema(deserializer);
		break;
	case WALType::CREATE_SEQUENCE:
		ReplayCreateSequence(deserializer);
		break;
	case WALType::DROP_SEQUENCE:
		ReplayDropSequence(deserializer);
		break;
	case WALType::SEQUENCE_VALUE:
		ReplaySequenceValue(deserializer);
		break;
	case WALType::CREATE_MACRO:
		ReplayCreateMacro(deserializer);
		break;
	case WALType::DROP_MACRO:
		ReplayDropMacro(deserializer);
		break;
	case WALType::CREATE_TABLE_MACRO:
		ReplayCreateTableMacro(deserializer);
		break;
	case WALType::DROP_TABLE_MACRO:
		ReplayDropTableMacro(deserializer);
		break;
	case WALType::CREATE_INDEX:
		ReplayCreateIndex(deserializer);
		break;
	case WALType::DROP_INDEX:
		ReplayDropIndex(deserializer);
		break;
	case WALType::USE_TABLE:
		ReplayUseTable(deserializer);
		break;
	case WALType::INSERT_TUPLE:
		ReplayInsert(deserializer);
		break;
	case WALType::DELETE_TUPLE:
		ReplayDelete(deserializer);
		break;
	case WALType::UPDATE_TUPLE:
		ReplayUpdate(deserializer);
		break;
	case WALType::CHECKPOINT:
		ReplayCheckpoint(deserializer);
		break;
	case WALType::CREATE_TYPE:
		ReplayCreateType(deserializer);
		break;
	case WALType::DROP_TYPE:
		ReplayDropType(deserializer);
		break;
	default:
		throw InternalException("Invalid WAL entry type!");
	}
}

//===--------------------------------------------------------------------===//
// Replay Table
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTable(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table");
	if (deserialize_only) {
		return;
	}
	// bind the constraints to the table again
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	catalog.CreateTable(context, *bound_info);
}

void ReplayState::ReplayDropTable(BinaryDeserializer &deserializer) {

	DropInfo info;

	info.type = CatalogType::TABLE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

void ReplayState::ReplayAlter(BinaryDeserializer &deserializer) {

	auto info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "info");
	auto &alter_info = info->Cast<AlterInfo>();
	if (deserialize_only) {
		return;
	}
	catalog.Alter(context, alter_info);
}

//===--------------------------------------------------------------------===//
// Replay View
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateView(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "view");
	if (deserialize_only) {
		return;
	}
	catalog.CreateView(context, entry->Cast<CreateViewInfo>());
}

void ReplayState::ReplayDropView(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::VIEW_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}
	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Schema
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSchema(BinaryDeserializer &deserializer) {
	CreateSchemaInfo info;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	if (deserialize_only) {
		return;
	}

	catalog.CreateSchema(context, info);
}

void ReplayState::ReplayDropSchema(BinaryDeserializer &deserializer) {
	DropInfo info;

	info.type = CatalogType::SCHEMA_ENTRY;
	info.name = deserializer.ReadProperty<string>(101, "schema");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Custom Type
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateType(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "type");
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateType(context, info->Cast<CreateTypeInfo>());
}

void ReplayState::ReplayDropType(BinaryDeserializer &deserializer) {
	DropInfo info;

	info.type = CatalogType::TYPE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Sequence
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateSequence(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "sequence");
	if (deserialize_only) {
		return;
	}

	catalog.CreateSequence(context, entry->Cast<CreateSequenceInfo>());
}

void ReplayState::ReplayDropSequence(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::SEQUENCE_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

void ReplayState::ReplaySequenceValue(BinaryDeserializer &deserializer) {
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto name = deserializer.ReadProperty<string>(102, "name");
	auto usage_count = deserializer.ReadProperty<uint64_t>(103, "usage_count");
	auto counter = deserializer.ReadProperty<int64_t>(104, "counter");
	if (deserialize_only) {
		return;
	}

	// fetch the sequence from the catalog
	auto &seq = catalog.GetEntry<SequenceCatalogEntry>(context, schema, name);
	if (usage_count > seq.usage_count) {
		seq.usage_count = usage_count;
		seq.counter = counter;
	}
}

//===--------------------------------------------------------------------===//
// Replay Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateMacro(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "macro");
	if (deserialize_only) {
		return;
	}

	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void ReplayState::ReplayDropMacro(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Table Macro
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateTableMacro(BinaryDeserializer &deserializer) {
	auto entry = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "table_macro");
	if (deserialize_only) {
		return;
	}
	catalog.CreateFunction(context, entry->Cast<CreateMacroInfo>());
}

void ReplayState::ReplayDropTableMacro(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::TABLE_MACRO_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Index
//===--------------------------------------------------------------------===//
void ReplayState::ReplayCreateIndex(BinaryDeserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(101, "index");
	if (deserialize_only) {
		return;
	}
	auto &index_info = info->Cast<CreateIndexInfo>();

	// get the physical table to which we'll add the index
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema, index_info.table);
	auto &data_table = table.GetStorage();

	// bind the parsed expressions
	if (index_info.expressions.empty()) {
		for (auto &parsed_expr : index_info.parsed_expressions) {
			index_info.expressions.push_back(parsed_expr->Copy());
		}
	}
	auto binder = Binder::CreateBinder(context);
	auto expressions = binder->BindCreateIndexExpressions(table, index_info);

	// create the empty index
	unique_ptr<Index> index;
	switch (index_info.index_type) {
	case IndexType::ART: {
		index = make_uniq<ART>(index_info.column_ids, TableIOManager::Get(data_table), expressions,
		                       index_info.constraint_type, data_table.db);
		break;
	}
	default:
		throw InternalException("Unimplemented index type");
	}

	// add the index to the catalog
	auto &index_entry = catalog.CreateIndex(context, index_info)->Cast<DuckIndexEntry>();
	index_entry.index = index.get();
	index_entry.info = data_table.info;
	for (auto &parsed_expr : index_info.parsed_expressions) {
		index_entry.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// physically add the index to the data table storage
	data_table.WALAddIndex(context, std::move(index), expressions);
}

void ReplayState::ReplayDropIndex(BinaryDeserializer &deserializer) {
	DropInfo info;
	info.type = CatalogType::INDEX_ENTRY;
	info.schema = deserializer.ReadProperty<string>(101, "schema");
	info.name = deserializer.ReadProperty<string>(102, "name");
	if (deserialize_only) {
		return;
	}

	catalog.DropEntry(context, info);
}

//===--------------------------------------------------------------------===//
// Replay Data
//===--------------------------------------------------------------------===//
void ReplayState::ReplayUseTable(BinaryDeserializer &deserializer) {
	auto schema_name = deserializer.ReadProperty<string>(101, "schema");
	auto table_name = deserializer.ReadProperty<string>(102, "table");
	if (deserialize_only) {
		return;
	}
	current_table = &catalog.GetEntry<TableCatalogEntry>(context, schema_name, table_name);
}

void ReplayState::ReplayInsert(BinaryDeserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw Exception("Corrupt WAL: insert without table");
	}

	// append to the current table
	current_table->GetStorage().LocalAppend(*current_table, context, chunk);
}

void ReplayState::ReplayDelete(BinaryDeserializer &deserializer) {
	DataChunk chunk;
	deserializer.ReadObject(101, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });
	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: delete without table");
	}

	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	row_t row_ids[1];
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_ids));

	auto source_ids = FlatVector::GetData<row_t>(chunk.data[0]);
	// delete the tuples from the current table
	for (idx_t i = 0; i < chunk.size(); i++) {
		row_ids[0] = source_ids[i];
		current_table->GetStorage().Delete(*current_table, context, row_identifiers, 1);
	}
}

void ReplayState::ReplayUpdate(BinaryDeserializer &deserializer) {
	auto column_path = deserializer.ReadProperty<vector<column_t>>(101, "column_indexes");

	DataChunk chunk;
	deserializer.ReadObject(102, "chunk", [&](Deserializer &object) { chunk.Deserialize(object); });

	if (deserialize_only) {
		return;
	}
	if (!current_table) {
		throw InternalException("Corrupt WAL: update without table");
	}

	if (column_path[0] >= current_table->GetColumns().PhysicalColumnCount()) {
		throw InternalException("Corrupt WAL: column index for update out of bounds");
	}

	// remove the row id vector from the chunk
	auto row_ids = std::move(chunk.data.back());
	chunk.data.pop_back();

	// now perform the update
	current_table->GetStorage().UpdateColumn(*current_table, context, row_ids, column_path, chunk);
}

void ReplayState::ReplayCheckpoint(BinaryDeserializer &deserializer) {
	checkpoint_id = deserializer.ReadProperty<MetaBlockPointer>(101, "meta_block");
}

} // namespace duckdb










#include <cstring>

namespace duckdb {

WriteAheadLog::WriteAheadLog(AttachedDatabase &database, const string &path) : skip_writing(false), database(database) {
	wal_path = path;
	writer = make_uniq<BufferedFileWriter>(FileSystem::Get(database), path.c_str(),
	                                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
	                                           FileFlags::FILE_FLAGS_APPEND);
}

WriteAheadLog::~WriteAheadLog() {
}

int64_t WriteAheadLog::GetWALSize() {
	D_ASSERT(writer);
	return writer->GetFileSize();
}

idx_t WriteAheadLog::GetTotalWritten() {
	D_ASSERT(writer);
	return writer->GetTotalWritten();
}

void WriteAheadLog::Truncate(int64_t size) {
	writer->Truncate(size);
}

void WriteAheadLog::Delete() {
	if (!writer) {
		return;
	}
	writer.reset();

	auto &fs = FileSystem::Get(database);
	fs.RemoveFile(wal_path);
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCheckpoint(MetaBlockPointer meta_block) {
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CHECKPOINT);
	serializer.WriteProperty(101, "meta_block", meta_block);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(const TableCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TABLE);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(const TableCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TABLE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(const SchemaCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(const SequenceCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_SEQUENCE);
	serializer.WriteProperty(101, "sequence", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropSequence(const SequenceCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_SEQUENCE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteSequenceValue(const SequenceCatalogEntry &entry, SequenceValue val) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::SEQUENCE_VALUE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.WriteProperty(103, "usage_count", val.usage_count);
	serializer.WriteProperty(104, "counter", val.counter);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// MACROS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateMacro(const ScalarMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_MACRO);
	serializer.WriteProperty(101, "macro", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropMacro(const ScalarMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

void WriteAheadLog::WriteCreateTableMacro(const TableMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TABLE_MACRO);
	serializer.WriteProperty(101, "table", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropTableMacro(const TableMacroCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TABLE_MACRO);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_INDEX);
	serializer.WriteProperty(101, "index", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropIndex(const IndexCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_INDEX);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(const TypeCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_TYPE);
	serializer.WriteProperty(101, "type", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropType(const TypeCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_TYPE);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(const ViewCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::CREATE_VIEW);
	serializer.WriteProperty(101, "view", &entry);
	serializer.End();
}

void WriteAheadLog::WriteDropView(const ViewCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_VIEW);
	serializer.WriteProperty(101, "schema", entry.schema.name);
	serializer.WriteProperty(102, "name", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(const SchemaCatalogEntry &entry) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DROP_SCHEMA);
	serializer.WriteProperty(101, "schema", entry.name);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(string &schema, string &table) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::USE_TABLE);
	serializer.WriteProperty(101, "schema", schema);
	serializer.WriteProperty(102, "table", table);
	serializer.End();
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::INSERT_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::DELETE_TUPLE);
	serializer.WriteProperty(101, "chunk", chunk);
	serializer.End();
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	if (skip_writing) {
		return;
	}
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify();

	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::UPDATE_TUPLE);
	serializer.WriteProperty(101, "column_indexes", column_indexes);
	serializer.WriteProperty(102, "chunk", chunk);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Write ALTER Statement
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteAlter(const AlterInfo &info) {
	if (skip_writing) {
		return;
	}
	BinarySerializer serializer(*writer);
	serializer.Begin();
	serializer.WriteProperty(100, "wal_type", WALType::ALTER_INFO);
	serializer.WriteProperty(101, "info", &info);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	if (skip_writing) {
		return;
	}

	BinarySerializer serializer(*writer);
	serializer.Begin();
	// write an empty entry
	serializer.WriteProperty(100, "wal_type", WALType::WAL_FLUSH);
	serializer.End();

	// flushes all changes made to the WAL to disk
	writer->Sync();
}

} // namespace duckdb












namespace duckdb {

CleanupState::CleanupState() : current_table(nullptr), count(0) {
}

CleanupState::~CleanupState() {
	Flush();
}

void CleanupState::CleanupEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->CleanupEntry(*catalog_entry);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = reinterpret_cast<DeleteInfo *>(data);
		CleanupDelete(*info);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = reinterpret_cast<UpdateInfo *>(data);
		CleanupUpdate(*info);
		break;
	}
	default:
		break;
	}
}

void CleanupState::CleanupUpdate(UpdateInfo &info) {
	// remove the update info from the update chain
	// first obtain an exclusive lock on the segment
	info.segment->CleanupUpdate(info);
}

void CleanupState::CleanupDelete(DeleteInfo &info) {
	auto version_table = info.table;
	D_ASSERT(version_table->info->cardinality >= info.count);
	version_table->info->cardinality -= info.count;

	if (version_table->info->indexes.Empty()) {
		// this table has no indexes: no cleanup to be done
		return;
	}

	if (current_table != version_table) {
		// table for this entry differs from previous table: flush and switch to the new table
		Flush();
		current_table = version_table;
	}

	// possibly vacuum any indexes in this table later
	indexed_tables[current_table->info->table] = current_table;

	count = 0;
	for (idx_t i = 0; i < info.count; i++) {
		row_numbers[count++] = info.base_row + info.rows[i];
	}
	Flush();
}

void CleanupState::Flush() {
	if (count == 0) {
		return;
	}

	// set up the row identifiers vector
	Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_numbers));

	// delete the tuples from all the indexes
	try {
		current_table->RemoveFromIndexes(row_identifiers, count);
	} catch (...) {
	}

	count = 0;
}

} // namespace duckdb






















namespace duckdb {

CommitState::CommitState(transaction_t commit_id, optional_ptr<WriteAheadLog> log)
    : log(log), commit_id(commit_id), current_table_info(nullptr) {
}

void CommitState::SwitchTable(DataTableInfo *table_info, UndoFlags new_op) {
	if (current_table_info != table_info) {
		// write the current table to the log
		log->WriteSetTable(table_info->schema, table_info->table);
		current_table_info = table_info;
	}
}

void CommitState::WriteCatalogEntry(CatalogEntry &entry, data_ptr_t dataptr) {
	if (entry.temporary || entry.parent->temporary) {
		return;
	}
	D_ASSERT(log);
	// look at the type of the parent entry
	auto parent = entry.parent;
	switch (parent->type) {
	case CatalogType::TABLE_ENTRY:
		if (entry.type == CatalogType::TABLE_ENTRY) {
			auto &table_entry = entry.Cast<DuckTableEntry>();
			D_ASSERT(table_entry.IsDuckTable());
			// ALTER TABLE statement, read the extra data after the entry

			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = data_ptr_cast(dataptr + sizeof(idx_t));

			MemoryStream source(extra_data, extra_data_size);
			BinaryDeserializer deserializer(source);
			deserializer.Begin();
			auto column_name = deserializer.ReadProperty<string>(100, "column_name");
			auto parse_info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "alter_info");
			deserializer.End();

			if (!column_name.empty()) {
				// write the alter table in the log
				table_entry.CommitAlter(column_name);
			}
			auto &alter_info = parse_info->Cast<AlterInfo>();
			log->WriteAlter(alter_info);
		} else {
			// CREATE TABLE statement
			log->WriteCreateTable(parent->Cast<TableCatalogEntry>());
		}
		break;
	case CatalogType::SCHEMA_ENTRY:
		if (entry.type == CatalogType::SCHEMA_ENTRY) {
			// ALTER TABLE statement, skip it
			return;
		}
		log->WriteCreateSchema(parent->Cast<SchemaCatalogEntry>());
		break;
	case CatalogType::VIEW_ENTRY:
		if (entry.type == CatalogType::VIEW_ENTRY) {
			// ALTER TABLE statement, read the extra data after the entry
			auto extra_data_size = Load<idx_t>(dataptr);
			auto extra_data = data_ptr_cast(dataptr + sizeof(idx_t));
			// deserialize it
			MemoryStream source(extra_data, extra_data_size);
			BinaryDeserializer deserializer(source);
			deserializer.Begin();
			auto column_name = deserializer.ReadProperty<string>(100, "column_name");
			auto parse_info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(101, "alter_info");
			deserializer.End();

			(void)column_name;

			// write the alter table in the log
			auto &alter_info = parse_info->Cast<AlterInfo>();
			log->WriteAlter(alter_info);
		} else {
			log->WriteCreateView(parent->Cast<ViewCatalogEntry>());
		}
		break;
	case CatalogType::SEQUENCE_ENTRY:
		log->WriteCreateSequence(parent->Cast<SequenceCatalogEntry>());
		break;
	case CatalogType::MACRO_ENTRY:
		log->WriteCreateMacro(parent->Cast<ScalarMacroCatalogEntry>());
		break;
	case CatalogType::TABLE_MACRO_ENTRY:
		log->WriteCreateTableMacro(parent->Cast<TableMacroCatalogEntry>());
		break;
	case CatalogType::INDEX_ENTRY:
		log->WriteCreateIndex(parent->Cast<IndexCatalogEntry>());
		break;
	case CatalogType::TYPE_ENTRY:
		log->WriteCreateType(parent->Cast<TypeCatalogEntry>());
		break;
	case CatalogType::DELETED_ENTRY:
		switch (entry.type) {
		case CatalogType::TABLE_ENTRY: {
			auto &table_entry = entry.Cast<DuckTableEntry>();
			D_ASSERT(table_entry.IsDuckTable());
			table_entry.CommitDrop();
			log->WriteDropTable(table_entry);
			break;
		}
		case CatalogType::SCHEMA_ENTRY:
			log->WriteDropSchema(entry.Cast<SchemaCatalogEntry>());
			break;
		case CatalogType::VIEW_ENTRY:
			log->WriteDropView(entry.Cast<ViewCatalogEntry>());
			break;
		case CatalogType::SEQUENCE_ENTRY:
			log->WriteDropSequence(entry.Cast<SequenceCatalogEntry>());
			break;
		case CatalogType::MACRO_ENTRY:
			log->WriteDropMacro(entry.Cast<ScalarMacroCatalogEntry>());
			break;
		case CatalogType::TABLE_MACRO_ENTRY:
			log->WriteDropTableMacro(entry.Cast<TableMacroCatalogEntry>());
			break;
		case CatalogType::TYPE_ENTRY:
			log->WriteDropType(entry.Cast<TypeCatalogEntry>());
			break;
		case CatalogType::INDEX_ENTRY: {
			auto &index_entry = entry.Cast<DuckIndexEntry>();
			index_entry.CommitDrop();
			log->WriteDropIndex(entry.Cast<IndexCatalogEntry>());
			break;
		}
		case CatalogType::PREPARED_STATEMENT:
		case CatalogType::SCALAR_FUNCTION_ENTRY:
			// do nothing, indexes/prepared statements/functions aren't persisted to disk
			break;
		default:
			throw InternalException("Don't know how to drop this type!");
		}
		break;
	case CatalogType::PREPARED_STATEMENT:
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::COPY_FUNCTION_ENTRY:
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
	case CatalogType::COLLATION_ENTRY:
		// do nothing, these entries are not persisted to disk
		break;
	default:
		throw InternalException("UndoBuffer - don't know how to write this entry to the WAL");
	}
}

void CommitState::WriteDelete(DeleteInfo &info) {
	D_ASSERT(log);
	// switch to the current table, if necessary
	SwitchTable(info.table->info.get(), UndoFlags::DELETE_TUPLE);

	if (!delete_chunk) {
		delete_chunk = make_uniq<DataChunk>();
		vector<LogicalType> delete_types = {LogicalType::ROW_TYPE};
		delete_chunk->Initialize(Allocator::DefaultAllocator(), delete_types);
	}
	auto rows = FlatVector::GetData<row_t>(delete_chunk->data[0]);
	for (idx_t i = 0; i < info.count; i++) {
		rows[i] = info.base_row + info.rows[i];
	}
	delete_chunk->SetCardinality(info.count);
	log->WriteDelete(*delete_chunk);
}

void CommitState::WriteUpdate(UpdateInfo &info) {
	D_ASSERT(log);
	// switch to the current table, if necessary
	auto &column_data = info.segment->column_data;
	auto &table_info = column_data.GetTableInfo();

	SwitchTable(&table_info, UndoFlags::UPDATE_TUPLE);

	// initialize the update chunk
	vector<LogicalType> update_types;
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		update_types.emplace_back(LogicalType::BOOLEAN);
	} else {
		update_types.push_back(column_data.type);
	}
	update_types.emplace_back(LogicalType::ROW_TYPE);

	update_chunk = make_uniq<DataChunk>();
	update_chunk->Initialize(Allocator::DefaultAllocator(), update_types);

	// fetch the updated values from the base segment
	info.segment->FetchCommitted(info.vector_index, update_chunk->data[0]);

	// write the row ids into the chunk
	auto row_ids = FlatVector::GetData<row_t>(update_chunk->data[1]);
	idx_t start = column_data.start + info.vector_index * STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < info.N; i++) {
		row_ids[info.tuples[i]] = start + info.tuples[i];
	}
	if (column_data.type.id() == LogicalTypeId::VALIDITY) {
		// zero-initialize the booleans
		// FIXME: this is only required because of NullValue<T> in Vector::Serialize...
		auto booleans = FlatVector::GetData<bool>(update_chunk->data[0]);
		for (idx_t i = 0; i < info.N; i++) {
			auto idx = info.tuples[i];
			booleans[idx] = false;
		}
	}
	SelectionVector sel(info.tuples);
	update_chunk->Slice(sel, info.N);

	// construct the column index path
	vector<column_t> column_indexes;
	reference<ColumnData> current_column_data = column_data;
	while (current_column_data.get().parent) {
		column_indexes.push_back(current_column_data.get().column_index);
		current_column_data = *current_column_data.get().parent;
	}
	column_indexes.push_back(info.column_index);
	std::reverse(column_indexes.begin(), column_indexes.end());

	log->WriteUpdate(*update_chunk, column_indexes);
}

template <bool HAS_LOG>
void CommitState::CommitEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->parent);

		auto &catalog = catalog_entry->ParentCatalog();
		D_ASSERT(catalog.IsDuckCatalog());

		// Grab a write lock on the catalog
		auto &duck_catalog = catalog.Cast<DuckCatalog>();
		lock_guard<mutex> write_lock(duck_catalog.GetWriteLock());
		catalog_entry->set->UpdateTimestamp(*catalog_entry->parent, commit_id);
		if (catalog_entry->name != catalog_entry->parent->name) {
			catalog_entry->set->UpdateTimestamp(*catalog_entry, commit_id);
		}
		if (HAS_LOG) {
			// push the catalog update to the WAL
			WriteCatalogEntry(*catalog_entry, data + sizeof(CatalogEntry *));
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		// append:
		auto info = reinterpret_cast<AppendInfo *>(data);
		if (HAS_LOG && !info->table->info->IsTemporary()) {
			info->table->WriteToLog(*log, info->start_row, info->count);
		}
		// mark the tuples as committed
		info->table->CommitAppend(commit_id, info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		if (HAS_LOG && !info->table->info->IsTemporary()) {
			WriteDelete(*info);
		}
		// mark the tuples as committed
		info->version_info->CommitDelete(info->vector_idx, commit_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		if (HAS_LOG && !info->segment->column_data.GetTableInfo().IsTemporary()) {
			WriteUpdate(*info);
		}
		info->version_number = commit_id;
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CommitState::RevertCommit(UndoFlags type, data_ptr_t data) {
	transaction_t transaction_id = commit_id;
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// set the commit timestamp of the catalog entry to the given id
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->parent);
		catalog_entry->set->UpdateTimestamp(*catalog_entry->parent, transaction_id);
		if (catalog_entry->name != catalog_entry->parent->name) {
			catalog_entry->set->UpdateTimestamp(*catalog_entry, transaction_id);
		}
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// revert this append
		info->table->RevertAppend(info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		// deletion:
		auto info = reinterpret_cast<DeleteInfo *>(data);
		info->table->info->cardinality += info->count;
		// revert the commit by writing the (uncommitted) transaction_id back into the version info
		info->version_info->CommitDelete(info->vector_idx, transaction_id, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		// update:
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->version_number = transaction_id;
		break;
	}
	default:
		throw InternalException("UndoBuffer - don't know how to revert commit of this type!");
	}
}

template void CommitState::CommitEntry<true>(UndoFlags type, data_ptr_t data);
template void CommitState::CommitEntry<false>(UndoFlags type, data_ptr_t data);

} // namespace duckdb



















namespace duckdb {

TransactionData::TransactionData(DuckTransaction &transaction_p) // NOLINT
    : transaction(&transaction_p), transaction_id(transaction_p.transaction_id), start_time(transaction_p.start_time) {
}
TransactionData::TransactionData(transaction_t transaction_id_p, transaction_t start_time_p)
    : transaction(nullptr), transaction_id(transaction_id_p), start_time(start_time_p) {
}

DuckTransaction::DuckTransaction(TransactionManager &manager, ClientContext &context_p, transaction_t start_time,
                                 transaction_t transaction_id)
    : Transaction(manager, context_p), start_time(start_time), transaction_id(transaction_id), commit_id(0),
      highest_active_query(0), undo_buffer(context_p), storage(make_uniq<LocalStorage>(context_p, *this)) {
}

DuckTransaction::~DuckTransaction() {
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, AttachedDatabase &db) {
	return DuckTransaction::Get(context, db.GetCatalog());
}

DuckTransaction &DuckTransaction::Get(ClientContext &context, Catalog &catalog) {
	auto &transaction = Transaction::Get(context, catalog);
	if (!transaction.IsDuckTransaction()) {
		throw InternalException("DuckTransaction::Get called on non-DuckDB transaction");
	}
	return transaction.Cast<DuckTransaction>();
}

LocalStorage &DuckTransaction::GetLocalStorage() {
	return *storage;
}

void DuckTransaction::PushCatalogEntry(CatalogEntry &entry, data_ptr_t extra_data, idx_t extra_data_size) {
	idx_t alloc_size = sizeof(CatalogEntry *);
	if (extra_data_size > 0) {
		alloc_size += extra_data_size + sizeof(idx_t);
	}
	auto baseptr = undo_buffer.CreateEntry(UndoFlags::CATALOG_ENTRY, alloc_size);
	// store the pointer to the catalog entry
	Store<CatalogEntry *>(&entry, baseptr);
	if (extra_data_size > 0) {
		// copy the extra data behind the catalog entry pointer (if any)
		baseptr += sizeof(CatalogEntry *);
		// first store the extra data size
		Store<idx_t>(extra_data_size, baseptr);
		baseptr += sizeof(idx_t);
		// then copy over the actual data
		memcpy(baseptr, extra_data, extra_data_size);
	}
}

void DuckTransaction::PushDelete(DataTable &table, RowVersionManager &info, idx_t vector_idx, row_t rows[], idx_t count,
                                 idx_t base_row) {
	auto delete_info = reinterpret_cast<DeleteInfo *>(
	    undo_buffer.CreateEntry(UndoFlags::DELETE_TUPLE, sizeof(DeleteInfo) + sizeof(row_t) * count));
	delete_info->version_info = &info;
	delete_info->vector_idx = vector_idx;
	delete_info->table = &table;
	delete_info->count = count;
	delete_info->base_row = base_row;
	memcpy(delete_info->rows, rows, sizeof(row_t) * count);
}

void DuckTransaction::PushAppend(DataTable &table, idx_t start_row, idx_t row_count) {
	auto append_info =
	    reinterpret_cast<AppendInfo *>(undo_buffer.CreateEntry(UndoFlags::INSERT_TUPLE, sizeof(AppendInfo)));
	append_info->table = &table;
	append_info->start_row = start_row;
	append_info->count = row_count;
}

UpdateInfo *DuckTransaction::CreateUpdateInfo(idx_t type_size, idx_t entries) {
	data_ptr_t base_info = undo_buffer.CreateEntry(
	    UndoFlags::UPDATE_TUPLE, sizeof(UpdateInfo) + (sizeof(sel_t) + type_size) * STANDARD_VECTOR_SIZE);
	auto update_info = reinterpret_cast<UpdateInfo *>(base_info);
	update_info->max = STANDARD_VECTOR_SIZE;
	update_info->tuples = reinterpret_cast<sel_t *>(base_info + sizeof(UpdateInfo));
	update_info->tuple_data = base_info + sizeof(UpdateInfo) + sizeof(sel_t) * update_info->max;
	update_info->version_number = transaction_id;
	return update_info;
}

bool DuckTransaction::ChangesMade() {
	return undo_buffer.ChangesMade() || storage->ChangesMade();
}

bool DuckTransaction::AutomaticCheckpoint(AttachedDatabase &db) {
	auto &storage_manager = db.GetStorageManager();
	return storage_manager.AutomaticCheckpoint(storage->EstimatedSize() + undo_buffer.EstimatedSize());
}

string DuckTransaction::Commit(AttachedDatabase &db, transaction_t commit_id, bool checkpoint) noexcept {
	// "checkpoint" parameter indicates if the caller will checkpoint. If checkpoint ==
	//    true: Then this function will NOT write to the WAL or flush/persist.
	//          This method only makes commit in memory, expecting caller to checkpoint/flush.
	//    false: Then this function WILL write to the WAL and Flush/Persist it.
	this->commit_id = commit_id;

	UndoBuffer::IteratorState iterator_state;
	LocalStorage::CommitState commit_state;
	unique_ptr<StorageCommitState> storage_commit_state;
	optional_ptr<WriteAheadLog> log;
	if (!db.IsSystem()) {
		auto &storage_manager = db.GetStorageManager();
		log = storage_manager.GetWriteAheadLog();
		storage_commit_state = storage_manager.GenStorageCommitState(*this, checkpoint);
	} else {
		log = nullptr;
	}
	try {
		storage->Commit(commit_state, *this);
		undo_buffer.Commit(iterator_state, log, commit_id);
		if (log) {
			// commit any sequences that were used to the WAL
			for (auto &entry : sequence_usage) {
				log->WriteSequenceValue(*entry.first, entry.second);
			}
		}
		if (storage_commit_state) {
			storage_commit_state->FlushCommit();
		}
		return string();
	} catch (std::exception &ex) {
		undo_buffer.RevertCommit(iterator_state, this->transaction_id);
		return ex.what();
	}
}

void DuckTransaction::Rollback() noexcept {
	storage->Rollback();
	undo_buffer.Rollback();
}

void DuckTransaction::Cleanup() {
	undo_buffer.Cleanup();
}

} // namespace duckdb















namespace duckdb {

struct CheckpointLock {
	explicit CheckpointLock(DuckTransactionManager &manager) : manager(manager), is_locked(false) {
	}
	~CheckpointLock() {
		Unlock();
	}

	DuckTransactionManager &manager;
	bool is_locked;

	void Lock() {
		D_ASSERT(!manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = true;
		is_locked = true;
	}
	void Unlock() {
		if (!is_locked) {
			return;
		}
		D_ASSERT(manager.thread_is_checkpointing);
		manager.thread_is_checkpointing = false;
		is_locked = false;
	}
};

DuckTransactionManager::DuckTransactionManager(AttachedDatabase &db)
    : TransactionManager(db), thread_is_checkpointing(false) {
	// start timestamp starts at two
	current_start_timestamp = 2;
	// transaction ID starts very high:
	// it should be much higher than the current start timestamp
	// if transaction_id < start_timestamp for any set of active transactions
	// uncommited data could be read by
	current_transaction_id = TRANSACTION_ID_START;
	lowest_active_id = TRANSACTION_ID_START;
	lowest_active_start = MAX_TRANSACTION_ID;
}

DuckTransactionManager::~DuckTransactionManager() {
}

DuckTransactionManager &DuckTransactionManager::Get(AttachedDatabase &db) {
	auto &transaction_manager = TransactionManager::Get(db);
	if (!transaction_manager.IsDuckTransactionManager()) {
		throw InternalException("Calling DuckTransactionManager::Get on non-DuckDB transaction manager");
	}
	return reinterpret_cast<DuckTransactionManager &>(transaction_manager);
}

Transaction *DuckTransactionManager::StartTransaction(ClientContext &context) {
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);
	if (current_start_timestamp >= TRANSACTION_ID_START) { // LCOV_EXCL_START
		throw InternalException("Cannot start more transactions, ran out of "
		                        "transaction identifiers!");
	} // LCOV_EXCL_STOP

	// obtain the start time and transaction ID of this transaction
	transaction_t start_time = current_start_timestamp++;
	transaction_t transaction_id = current_transaction_id++;
	if (active_transactions.empty()) {
		lowest_active_start = start_time;
		lowest_active_id = transaction_id;
	}

	// create the actual transaction
	auto transaction = make_uniq<DuckTransaction>(*this, context, start_time, transaction_id);
	auto transaction_ptr = transaction.get();

	// store it in the set of active transactions
	active_transactions.push_back(std::move(transaction));
	return transaction_ptr;
}

struct ClientLockWrapper {
	ClientLockWrapper(mutex &client_lock, shared_ptr<ClientContext> connection)
	    : connection(std::move(connection)), connection_lock(make_uniq<lock_guard<mutex>>(client_lock)) {
	}

	shared_ptr<ClientContext> connection;
	unique_ptr<lock_guard<mutex>> connection_lock;
};

void DuckTransactionManager::LockClients(vector<ClientLockWrapper> &client_locks, ClientContext &context) {
	auto &connection_manager = ConnectionManager::Get(context);
	client_locks.emplace_back(connection_manager.connections_lock, nullptr);
	auto connection_list = connection_manager.GetConnectionList();
	for (auto &con : connection_list) {
		if (con.get() == &context) {
			continue;
		}
		auto &context_lock = con->context_lock;
		client_locks.emplace_back(context_lock, std::move(con));
	}
}

void DuckTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return;
	}

	// first check if no other thread is checkpointing right now
	auto lock = unique_lock<mutex>(transaction_lock);
	if (thread_is_checkpointing) {
		throw TransactionException("Cannot CHECKPOINT: another thread is checkpointing right now");
	}
	CheckpointLock checkpoint_lock(*this);
	checkpoint_lock.Lock();
	lock.unlock();

	// lock all the clients AND the connection manager now
	// this ensures no new queries can be started, and no new connections to the database can be made
	// to avoid deadlock we release the transaction lock while locking the clients
	vector<ClientLockWrapper> client_locks;
	LockClients(client_locks, context);

	auto current = &DuckTransaction::Get(context, db);
	lock.lock();
	if (current->ChangesMade()) {
		throw TransactionException("Cannot CHECKPOINT: the current transaction has transaction local changes");
	}
	if (!force) {
		if (!CanCheckpoint(current)) {
			throw TransactionException("Cannot CHECKPOINT: there are other transactions. Use FORCE CHECKPOINT to abort "
			                           "the other transactions and force a checkpoint");
		}
	} else {
		if (!CanCheckpoint(current)) {
			for (size_t i = 0; i < active_transactions.size(); i++) {
				auto &transaction = active_transactions[i];
				// rollback the transaction
				transaction->Rollback();
				auto transaction_context = transaction->context.lock();

				// remove the transaction id from the list of active transactions
				// potentially resulting in garbage collection
				RemoveTransaction(*transaction);
				if (transaction_context) {
					transaction_context->transaction.ClearTransaction();
				}
				i--;
			}
			D_ASSERT(CanCheckpoint(nullptr));
		}
	}
	storage_manager.CreateCheckpoint();
}

bool DuckTransactionManager::CanCheckpoint(optional_ptr<DuckTransaction> current) {
	if (db.IsSystem()) {
		return false;
	}
	auto &storage_manager = db.GetStorageManager();
	if (storage_manager.InMemory()) {
		return false;
	}
	if (!recently_committed_transactions.empty() || !old_transactions.empty()) {
		return false;
	}
	for (auto &transaction : active_transactions) {
		if (transaction.get() != current.get()) {
			return false;
		}
	}
	return true;
}

string DuckTransactionManager::CommitTransaction(ClientContext &context, Transaction *transaction_p) {
	auto &transaction = transaction_p->Cast<DuckTransaction>();
	vector<ClientLockWrapper> client_locks;
	auto lock = make_uniq<lock_guard<mutex>>(transaction_lock);
	CheckpointLock checkpoint_lock(*this);
	// check if we can checkpoint
	bool checkpoint = thread_is_checkpointing ? false : CanCheckpoint(&transaction);
	if (checkpoint) {
		if (transaction.AutomaticCheckpoint(db)) {
			checkpoint_lock.Lock();
			// we might be able to checkpoint: lock all clients
			// to avoid deadlock we release the transaction lock while locking the clients
			lock.reset();

			LockClients(client_locks, context);

			lock = make_uniq<lock_guard<mutex>>(transaction_lock);
			checkpoint = CanCheckpoint(&transaction);
			if (!checkpoint) {
				checkpoint_lock.Unlock();
				client_locks.clear();
			}
		} else {
			checkpoint = false;
		}
	}
	// obtain a commit id for the transaction
	transaction_t commit_id = current_start_timestamp++;
	// commit the UndoBuffer of the transaction
	string error = transaction.Commit(db, commit_id, checkpoint);
	if (!error.empty()) {
		// commit unsuccessful: rollback the transaction instead
		checkpoint = false;
		transaction.commit_id = 0;
		transaction.Rollback();
	}
	if (!checkpoint) {
		// we won't checkpoint after all: unlock the clients again
		checkpoint_lock.Unlock();
		client_locks.clear();
	}

	// commit successful: remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
	// now perform a checkpoint if (1) we are able to checkpoint, and (2) the WAL has reached sufficient size to
	// checkpoint
	if (checkpoint) {
		// checkpoint the database to disk
		auto &storage_manager = db.GetStorageManager();
		storage_manager.CreateCheckpoint(false, true);
	}
	return error;
}

void DuckTransactionManager::RollbackTransaction(Transaction *transaction_p) {
	auto &transaction = transaction_p->Cast<DuckTransaction>();
	// obtain the transaction lock during this function
	lock_guard<mutex> lock(transaction_lock);

	// rollback the transaction
	transaction.Rollback();

	// remove the transaction id from the list of active transactions
	// potentially resulting in garbage collection
	RemoveTransaction(transaction);
}

void DuckTransactionManager::RemoveTransaction(DuckTransaction &transaction) noexcept {
	// remove the transaction from the list of active transactions
	idx_t t_index = active_transactions.size();
	// check for the lowest and highest start time in the list of transactions
	transaction_t lowest_start_time = TRANSACTION_ID_START;
	transaction_t lowest_transaction_id = MAX_TRANSACTION_ID;
	transaction_t lowest_active_query = MAXIMUM_QUERY_ID;
	for (idx_t i = 0; i < active_transactions.size(); i++) {
		if (active_transactions[i].get() == &transaction) {
			t_index = i;
		} else {
			transaction_t active_query = active_transactions[i]->active_query;
			lowest_start_time = MinValue(lowest_start_time, active_transactions[i]->start_time);
			lowest_active_query = MinValue(lowest_active_query, active_query);
			lowest_transaction_id = MinValue(lowest_transaction_id, active_transactions[i]->transaction_id);
		}
	}
	lowest_active_start = lowest_start_time;
	lowest_active_id = lowest_transaction_id;

	transaction_t lowest_stored_query = lowest_start_time;
	D_ASSERT(t_index != active_transactions.size());
	auto current_transaction = std::move(active_transactions[t_index]);
	auto current_query = DatabaseManager::Get(db).ActiveQueryNumber();
	if (transaction.commit_id != 0) {
		// the transaction was committed, add it to the list of recently
		// committed transactions
		recently_committed_transactions.push_back(std::move(current_transaction));
	} else {
		// the transaction was aborted, but we might still need its information
		// add it to the set of transactions awaiting GC
		current_transaction->highest_active_query = current_query;
		old_transactions.push_back(std::move(current_transaction));
	}
	// remove the transaction from the set of currently active transactions
	active_transactions.erase(active_transactions.begin() + t_index);
	// traverse the recently_committed transactions to see if we can remove any
	idx_t i = 0;
	for (; i < recently_committed_transactions.size(); i++) {
		D_ASSERT(recently_committed_transactions[i]);
		lowest_stored_query = MinValue(recently_committed_transactions[i]->start_time, lowest_stored_query);
		if (recently_committed_transactions[i]->commit_id < lowest_start_time) {
			// changes made BEFORE this transaction are no longer relevant
			// we can cleanup the undo buffer

			// HOWEVER: any currently running QUERY can still be using
			// the version information after the cleanup!

			// if we remove the UndoBuffer immediately, we have a race
			// condition

			// we can only safely do the actual memory cleanup when all the
			// currently active queries have finished running! (actually,
			// when all the currently active scans have finished running...)
			recently_committed_transactions[i]->Cleanup();
			// store the current highest active query
			recently_committed_transactions[i]->highest_active_query = current_query;
			// move it to the list of transactions awaiting GC
			old_transactions.push_back(std::move(recently_committed_transactions[i]));
		} else {
			// recently_committed_transactions is ordered on commit_id
			// implicitly thus if the current one is bigger than
			// lowest_start_time any subsequent ones are also bigger
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		recently_committed_transactions.erase(recently_committed_transactions.begin(),
		                                      recently_committed_transactions.begin() + i);
	}
	// check if we can free the memory of any old transactions
	i = active_transactions.empty() ? old_transactions.size() : 0;
	for (; i < old_transactions.size(); i++) {
		D_ASSERT(old_transactions[i]);
		D_ASSERT(old_transactions[i]->highest_active_query > 0);
		if (old_transactions[i]->highest_active_query >= lowest_active_query) {
			// there is still a query running that could be using
			// this transactions' data
			break;
		}
	}
	if (i > 0) {
		// we garbage collected transactions: remove them from the list
		old_transactions.erase(old_transactions.begin(), old_transactions.begin() + i);
	}
}

} // namespace duckdb





namespace duckdb {

MetaTransaction::MetaTransaction(ClientContext &context_p, timestamp_t start_timestamp_p, idx_t catalog_version_p)
    : context(context_p), start_timestamp(start_timestamp_p), catalog_version(catalog_version_p), read_only(true),
      active_query(MAXIMUM_QUERY_ID), modified_database(nullptr) {
}

MetaTransaction &MetaTransaction::Get(ClientContext &context) {
	return context.transaction.ActiveTransaction();
}

ValidChecker &ValidChecker::Get(MetaTransaction &transaction) {
	return transaction.transaction_validity;
}

Transaction &Transaction::Get(ClientContext &context, AttachedDatabase &db) {
	auto &meta_transaction = MetaTransaction::Get(context);
	return meta_transaction.GetTransaction(db);
}

Transaction &MetaTransaction::GetTransaction(AttachedDatabase &db) {
	auto entry = transactions.find(&db);
	if (entry == transactions.end()) {
		auto new_transaction = db.GetTransactionManager().StartTransaction(context);
		if (!new_transaction) {
			throw InternalException("StartTransaction did not return a valid transaction");
		}
		new_transaction->active_query = active_query;
		all_transactions.push_back(&db);
		transactions[&db] = new_transaction;
		return *new_transaction;
	} else {
		D_ASSERT(entry->second->active_query == active_query);
		return *entry->second;
	}
}

Transaction &Transaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog.GetAttached());
}

string MetaTransaction::Commit() {
	string error;
	// commit transactions in reverse order
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto db = all_transactions[i - 1];
		auto entry = transactions.find(db.get());
		if (entry == transactions.end()) {
			throw InternalException("Could not find transaction corresponding to database in MetaTransaction");
		}
		auto &transaction_manager = db->GetTransactionManager();
		auto transaction = entry->second;
		if (error.empty()) {
			// commit
			error = transaction_manager.CommitTransaction(context, transaction);
		} else {
			// we have encountered an error previously - roll back subsequent entries
			transaction_manager.RollbackTransaction(transaction);
		}
	}
	return error;
}

void MetaTransaction::Rollback() {
	// rollback transactions in reverse order
	for (idx_t i = all_transactions.size(); i > 0; i--) {
		auto db = all_transactions[i - 1];
		auto &transaction_manager = db->GetTransactionManager();
		auto entry = transactions.find(db.get());
		D_ASSERT(entry != transactions.end());
		auto transaction = entry->second;
		transaction_manager.RollbackTransaction(transaction);
	}
}

idx_t MetaTransaction::GetActiveQuery() {
	return active_query;
}

void MetaTransaction::SetActiveQuery(transaction_t query_number) {
	active_query = query_number;
	for (auto &entry : transactions) {
		entry.second->active_query = query_number;
	}
}

void MetaTransaction::ModifyDatabase(AttachedDatabase &db) {
	if (db.IsSystem() || db.IsTemporary()) {
		// we can always modify the system and temp databases
		return;
	}
	if (!modified_database) {
		modified_database = &db;
		return;
	}
	if (&db != modified_database.get()) {
		throw TransactionException(
		    "Attempting to write to database \"%s\" in a transaction that has already modified database \"%s\" - a "
		    "single transaction can only write to a single attached database.",
		    db.GetName(), modified_database->GetName());
	}
}

} // namespace duckdb













namespace duckdb {

void RollbackState::RollbackEntry(UndoFlags type, data_ptr_t data) {
	switch (type) {
	case UndoFlags::CATALOG_ENTRY: {
		// undo this catalog entry
		auto catalog_entry = Load<CatalogEntry *>(data);
		D_ASSERT(catalog_entry->set);
		catalog_entry->set->Undo(*catalog_entry);
		break;
	}
	case UndoFlags::INSERT_TUPLE: {
		auto info = reinterpret_cast<AppendInfo *>(data);
		// revert the append in the base table
		info->table->RevertAppend(info->start_row, info->count);
		break;
	}
	case UndoFlags::DELETE_TUPLE: {
		auto info = reinterpret_cast<DeleteInfo *>(data);
		// reset the deleted flag on rollback
		info->version_info->CommitDelete(info->vector_idx, NOT_DELETED_ID, info->rows, info->count);
		break;
	}
	case UndoFlags::UPDATE_TUPLE: {
		auto info = reinterpret_cast<UpdateInfo *>(data);
		info->segment->RollbackUpdate(*info);
		break;
	}
	default: // LCOV_EXCL_START
		D_ASSERT(type == UndoFlags::EMPTY_ENTRY);
		break;
	} // LCOV_EXCL_STOP
}

} // namespace duckdb





namespace duckdb {

Transaction::Transaction(TransactionManager &manager_p, ClientContext &context_p)
    : manager(manager_p), context(context_p.shared_from_this()), active_query(MAXIMUM_QUERY_ID) {
}

Transaction::~Transaction() {
}

bool Transaction::IsReadOnly() {
	auto ctxt = context.lock();
	if (!ctxt) {
		throw InternalException("Transaction::IsReadOnly() called after client context has been destroyed");
	}
	auto &db = manager.GetDB();
	return MetaTransaction::Get(*ctxt).ModifiedDatabase().get() != &db;
}

} // namespace duckdb








namespace duckdb {

TransactionContext::TransactionContext(ClientContext &context)
    : context(context), auto_commit(true), current_transaction(nullptr) {
}

TransactionContext::~TransactionContext() {
	if (current_transaction) {
		try {
			Rollback();
		} catch (...) {
		}
	}
}

void TransactionContext::BeginTransaction() {
	if (current_transaction) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	auto start_timestamp = Timestamp::GetCurrentTimestamp();
	auto catalog_version = Catalog::GetSystemCatalog(context).GetCatalogVersion();
	current_transaction = make_uniq<MetaTransaction>(context, start_timestamp, catalog_version);

	auto &config = DBConfig::GetConfig(context);
	if (config.options.immediate_transaction_mode) {
		// if immediate transaction mode is enabled then start all transactions immediately
		auto databases = DatabaseManager::Get(context).GetDatabases(context);
		for (auto db : databases) {
			current_transaction->GetTransaction(db.get());
		}
	}
}

void TransactionContext::Commit() {
	if (!current_transaction) {
		throw TransactionException("failed to commit: no transaction active");
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	string error = transaction->Commit();
	if (!error.empty()) {
		throw TransactionException("Failed to commit: %s", error);
	}
}

void TransactionContext::SetAutoCommit(bool value) {
	auto_commit = value;
	if (!auto_commit && !current_transaction) {
		BeginTransaction();
	}
}

void TransactionContext::Rollback() {
	if (!current_transaction) {
		throw TransactionException("failed to rollback: no transaction active");
	}
	auto transaction = std::move(current_transaction);
	ClearTransaction();
	transaction->Rollback();
}

void TransactionContext::ClearTransaction() {
	SetAutoCommit(true);
	current_transaction = nullptr;
}

idx_t TransactionContext::GetActiveQuery() {
	if (!current_transaction) {
		throw InternalException("GetActiveQuery called without active transaction");
	}
	return current_transaction->GetActiveQuery();
}

void TransactionContext::ResetActiveQuery() {
	if (current_transaction) {
		SetActiveQuery(MAXIMUM_QUERY_ID);
	}
}

void TransactionContext::SetActiveQuery(transaction_t query_number) {
	if (!current_transaction) {
		throw InternalException("SetActiveQuery called without active transaction");
	}
	current_transaction->SetActiveQuery(query_number);
}

} // namespace duckdb


namespace duckdb {

TransactionManager::TransactionManager(AttachedDatabase &db) : db(db) {
}

TransactionManager::~TransactionManager() {
}

} // namespace duckdb













namespace duckdb {
constexpr uint32_t UNDO_ENTRY_HEADER_SIZE = sizeof(UndoFlags) + sizeof(uint32_t);

UndoBuffer::UndoBuffer(ClientContext &context_p) : allocator(BufferAllocator::Get(context_p)) {
}

data_ptr_t UndoBuffer::CreateEntry(UndoFlags type, idx_t len) {
	D_ASSERT(len <= NumericLimits<uint32_t>::Maximum());
	len = AlignValue(len);
	idx_t needed_space = len + UNDO_ENTRY_HEADER_SIZE;
	auto data = allocator.Allocate(needed_space);
	Store<UndoFlags>(type, data);
	data += sizeof(UndoFlags);
	Store<uint32_t>(len, data);
	data += sizeof(uint32_t);
	return data;
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = allocator.GetTail();
	while (state.current) {
		state.start = state.current->data.get();
		state.end = state.start + state.current->current_position;
		while (state.start < state.end) {
			UndoFlags type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);

			uint32_t len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::IterateEntries(UndoBuffer::IteratorState &state, UndoBuffer::IteratorState &end_state, T &&callback) {
	// iterate in insertion order: start with the tail
	state.current = allocator.GetTail();
	while (state.current) {
		state.start = state.current->data.get();
		state.end =
		    state.current == end_state.current ? end_state.start : state.start + state.current->current_position;
		while (state.start < state.end) {
			auto type = Load<UndoFlags>(state.start);
			state.start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(state.start);
			state.start += sizeof(uint32_t);
			callback(type, state.start);
			state.start += len;
		}
		if (state.current == end_state.current) {
			// finished executing until the current end state
			return;
		}
		state.current = state.current->prev;
	}
}

template <class T>
void UndoBuffer::ReverseIterateEntries(T &&callback) {
	// iterate in reverse insertion order: start with the head
	auto current = allocator.GetHead();
	while (current) {
		data_ptr_t start = current->data.get();
		data_ptr_t end = start + current->current_position;
		// create a vector with all nodes in this chunk
		vector<pair<UndoFlags, data_ptr_t>> nodes;
		while (start < end) {
			auto type = Load<UndoFlags>(start);
			start += sizeof(UndoFlags);
			auto len = Load<uint32_t>(start);
			start += sizeof(uint32_t);
			nodes.emplace_back(type, start);
			start += len;
		}
		// iterate over it in reverse order
		for (idx_t i = nodes.size(); i > 0; i--) {
			callback(nodes[i - 1].first, nodes[i - 1].second);
		}
		current = current->next.get();
	}
}

bool UndoBuffer::ChangesMade() {
	return !allocator.IsEmpty();
}

idx_t UndoBuffer::EstimatedSize() {
	idx_t estimated_size = 0;
	auto node = allocator.GetHead();
	while (node) {
		estimated_size += node->current_position;
		node = node->next.get();
	}
	return estimated_size;
}

void UndoBuffer::Cleanup() {
	// garbage collect everything in the Undo Chunk
	// this should only happen if
	//  (1) the transaction this UndoBuffer belongs to has successfully
	//  committed
	//      (on Rollback the Rollback() function should be called, that clears
	//      the chunks)
	//  (2) there is no active transaction with start_id < commit_id of this
	//  transaction
	CleanupState state;
	UndoBuffer::IteratorState iterator_state;
	IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CleanupEntry(type, data); });

	// possibly vacuum indexes
	for (const auto &table : state.indexed_tables) {
		table.second->info->indexes.Scan([&](Index &index) {
			index.Vacuum();
			return false;
		});
	}
}

void UndoBuffer::Commit(UndoBuffer::IteratorState &iterator_state, optional_ptr<WriteAheadLog> log,
                        transaction_t commit_id) {
	CommitState state(commit_id, log);
	if (log) {
		// commit WITH write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<true>(type, data); });
	} else {
		// commit WITHOUT write ahead log
		IterateEntries(iterator_state, [&](UndoFlags type, data_ptr_t data) { state.CommitEntry<false>(type, data); });
	}
}

void UndoBuffer::RevertCommit(UndoBuffer::IteratorState &end_state, transaction_t transaction_id) {
	CommitState state(transaction_id, nullptr);
	UndoBuffer::IteratorState start_state;
	IterateEntries(start_state, end_state, [&](UndoFlags type, data_ptr_t data) { state.RevertCommit(type, data); });
}

void UndoBuffer::Rollback() noexcept {
	// rollback needs to be performed in reverse
	RollbackState state;
	ReverseIterateEntries([&](UndoFlags type, data_ptr_t data) { state.RollbackEntry(type, data); });
}
} // namespace duckdb


namespace duckdb {

CopiedStatementVerifier::CopiedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::COPIED, "Copied", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> CopiedStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<CopiedStatementVerifier>(statement.Copy());
}

} // namespace duckdb





namespace duckdb {

DeserializedStatementVerifier::DeserializedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::DESERIALIZED, "Deserialized", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> DeserializedStatementVerifier::Create(const SQLStatement &statement) {

	auto &select_stmt = statement.Cast<SelectStatement>();

	MemoryStream stream;
	BinarySerializer::Serialize(select_stmt, stream);
	stream.Rewind();
	auto result = BinaryDeserializer::Deserialize<SelectStatement>(stream);

	return make_uniq<DeserializedStatementVerifier>(std::move(result));
}

} // namespace duckdb


namespace duckdb {

ExternalStatementVerifier::ExternalStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::EXTERNAL, "External", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> ExternalStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<ExternalStatementVerifier>(statement.Copy());
}

} // namespace duckdb


namespace duckdb {

NoOperatorCachingVerifier::NoOperatorCachingVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::NO_OPERATOR_CACHING, "No operator caching", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> NoOperatorCachingVerifier::Create(const SQLStatement &statement_p) {
	return make_uniq<NoOperatorCachingVerifier>(statement_p.Copy());
}

} // namespace duckdb




namespace duckdb {

ParsedStatementVerifier::ParsedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::PARSED, "Parsed", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> ParsedStatementVerifier::Create(const SQLStatement &statement) {
	auto query_str = statement.ToString();
	Parser parser;
	try {
		parser.ParseQuery(query_str);
	} catch (std::exception &ex) {
		throw InternalException("Parsed statement verification failed. Query:\n%s\n\nError: %s", query_str, ex.what());
	}
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	return make_uniq<ParsedStatementVerifier>(std::move(parser.statements[0]));
}

} // namespace duckdb









namespace duckdb {

PreparedStatementVerifier::PreparedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::PREPARED, "Prepared", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> PreparedStatementVerifier::Create(const SQLStatement &statement) {
	return make_uniq<PreparedStatementVerifier>(statement.Copy());
}

void PreparedStatementVerifier::Extract() {
	auto &select = *statement;
	// replace all the constants from the select statement and replace them with parameter expressions
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *select.node, [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
	statement->n_param = values.size();
	for (auto &kv : values) {
		statement->named_param_map[kv.first] = 0;
	}
	// create the PREPARE and EXECUTE statements
	string name = "__duckdb_verification_prepared_statement";
	auto prepare = make_uniq<PrepareStatement>();
	prepare->name = name;
	prepare->statement = std::move(statement);

	auto execute = make_uniq<ExecuteStatement>();
	execute->name = name;
	execute->named_values = std::move(values);

	auto dealloc = make_uniq<DropStatement>();
	dealloc->info->type = CatalogType::PREPARED_STATEMENT;
	dealloc->info->name = string(name);

	prepare_statement = std::move(prepare);
	execute_statement = std::move(execute);
	dealloc_statement = std::move(dealloc);
}

void PreparedStatementVerifier::ConvertConstants(unique_ptr<ParsedExpression> &child) {
	if (child->type == ExpressionType::VALUE_CONSTANT) {
		// constant: extract the constant value
		auto alias = child->alias;
		child->alias = string();
		// check if the value already exists
		idx_t index = values.size();
		auto identifier = std::to_string(index + 1);
		const auto predicate = [&](const std::pair<const string, unique_ptr<ParsedExpression>> &pair) {
			return pair.second->Equals(*child.get());
		};
		auto result = std::find_if(values.begin(), values.end(), predicate);
		if (result == values.end()) {
			// If it doesn't exist yet, add it
			values[identifier] = std::move(child);
		} else {
			identifier = result->first;
		}

		// replace it with an expression
		auto parameter = make_uniq<ParameterExpression>();
		parameter->identifier = identifier;
		parameter->alias = alias;
		child = std::move(parameter);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*child,
	                                            [&](unique_ptr<ParsedExpression> &child) { ConvertConstants(child); });
}

bool PreparedStatementVerifier::Run(
    ClientContext &context, const string &query,
    const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>)> &run) {
	bool failed = false;
	// verify that we can extract all constants from the query and run the query as a prepared statement
	// create the PREPARE and EXECUTE statements
	Extract();
	// execute the prepared statements
	try {
		auto prepare_result = run(string(), std::move(prepare_statement));
		if (prepare_result->HasError()) {
			prepare_result->ThrowError("Failed prepare during verify: ");
		}
		auto execute_result = run(string(), std::move(execute_statement));
		if (execute_result->HasError()) {
			execute_result->ThrowError("Failed execute during verify: ");
		}
		materialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(execute_result));
	} catch (const Exception &ex) {
		if (ex.type != ExceptionType::PARAMETER_NOT_ALLOWED) {
			materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
		}
		failed = true;
	} catch (std::exception &ex) {
		materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
		failed = true;
	}
	run(string(), std::move(dealloc_statement));
	context.interrupted = false;

	return failed;
}

} // namespace duckdb













namespace duckdb {

StatementVerifier::StatementVerifier(VerificationType type, string name, unique_ptr<SQLStatement> statement_p)
    : type(type), name(std::move(name)),
      statement(unique_ptr_cast<SQLStatement, SelectStatement>(std::move(statement_p))),
      select_list(statement->node->GetSelectList()) {
}

StatementVerifier::StatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::ORIGINAL, "Original", std::move(statement_p)) {
}

StatementVerifier::~StatementVerifier() noexcept {
}

unique_ptr<StatementVerifier> StatementVerifier::Create(VerificationType type, const SQLStatement &statement_p) {
	switch (type) {
	case VerificationType::COPIED:
		return CopiedStatementVerifier::Create(statement_p);
	case VerificationType::DESERIALIZED:
		return DeserializedStatementVerifier::Create(statement_p);
	case VerificationType::PARSED:
		return ParsedStatementVerifier::Create(statement_p);
	case VerificationType::UNOPTIMIZED:
		return UnoptimizedStatementVerifier::Create(statement_p);
	case VerificationType::NO_OPERATOR_CACHING:
		return NoOperatorCachingVerifier::Create(statement_p);
	case VerificationType::PREPARED:
		return PreparedStatementVerifier::Create(statement_p);
	case VerificationType::EXTERNAL:
		return ExternalStatementVerifier::Create(statement_p);
	case VerificationType::INVALID:
	default:
		throw InternalException("Invalid statement verification type!");
	}
}

void StatementVerifier::CheckExpressions(const StatementVerifier &other) const {
	// Only the original statement should check other statements
	D_ASSERT(type == VerificationType::ORIGINAL);

	// Check equality
	if (other.RequireEquality()) {
		D_ASSERT(statement->Equals(*other.statement));
	}

#ifdef DEBUG
	// Now perform checking on the expressions
	D_ASSERT(select_list.size() == other.select_list.size());
	const auto expr_count = select_list.size();
	if (other.RequireEquality()) {
		for (idx_t i = 0; i < expr_count; i++) {
			// Run the ToString, to verify that it doesn't crash
			select_list[i]->ToString();

			if (select_list[i]->HasSubquery()) {
				continue;
			}

			// Check that the expressions are equivalent
			D_ASSERT(select_list[i]->Equals(*other.select_list[i]));
			// Check that the hashes are equivalent too
			D_ASSERT(select_list[i]->Hash() == other.select_list[i]->Hash());

			other.select_list[i]->Verify();
		}
	}
#endif
}

void StatementVerifier::CheckExpressions() const {
#ifdef DEBUG
	D_ASSERT(type == VerificationType::ORIGINAL);
	// Perform additional checking within the expressions
	const auto expr_count = select_list.size();
	for (idx_t outer_idx = 0; outer_idx < expr_count; outer_idx++) {
		auto hash = select_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < expr_count; inner_idx++) {
			auto hash2 = select_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				D_ASSERT(!select_list[outer_idx]->Equals(*select_list[inner_idx]));
			}
		}
	}
#endif
}

bool StatementVerifier::Run(
    ClientContext &context, const string &query,
    const std::function<unique_ptr<QueryResult>(const string &, unique_ptr<SQLStatement>)> &run) {
	bool failed = false;

	context.interrupted = false;
	context.config.enable_optimizer = !DisableOptimizer();
	context.config.enable_caching_operators = !DisableOperatorCaching();
	context.config.force_external = ForceExternal();
	try {
		auto result = run(query, std::move(statement));
		if (result->HasError()) {
			failed = true;
		}
		materialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	} catch (const Exception &ex) {
		failed = true;
		materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
	} catch (std::exception &ex) {
		failed = true;
		materialized_result = make_uniq<MaterializedQueryResult>(PreservedError(ex));
	}
	context.interrupted = false;

	return failed;
}

string StatementVerifier::CompareResults(const StatementVerifier &other) {
	D_ASSERT(type == VerificationType::ORIGINAL);
	string error;
	if (materialized_result->HasError() != other.materialized_result->HasError()) { // LCOV_EXCL_START
		string result = other.name + " statement differs from original result!\n";
		result += "Original Result:\n" + materialized_result->ToString();
		result += other.name + ":\n" + other.materialized_result->ToString();
		return result;
	} // LCOV_EXCL_STOP
	if (materialized_result->HasError()) {
		return "";
	}
	if (!ColumnDataCollection::ResultEquals(materialized_result->Collection(), other.materialized_result->Collection(),
	                                        error)) { // LCOV_EXCL_START
		string result = other.name + " statement differs from original result!\n";
		result += "Original Result:\n" + materialized_result->ToString();
		result += other.name + ":\n" + other.materialized_result->ToString();
		result += "\n\n---------------------------------\n" + error;
		return result;
	} // LCOV_EXCL_STOP

	return "";
}

} // namespace duckdb


namespace duckdb {

UnoptimizedStatementVerifier::UnoptimizedStatementVerifier(unique_ptr<SQLStatement> statement_p)
    : StatementVerifier(VerificationType::UNOPTIMIZED, "Unoptimized", std::move(statement_p)) {
}

unique_ptr<StatementVerifier> UnoptimizedStatementVerifier::Create(const SQLStatement &statement_p) {
	return make_uniq<UnoptimizedStatementVerifier>(statement_p.Copy());
}

} // namespace duckdb

#endif
