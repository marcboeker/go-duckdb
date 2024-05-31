//#include "capi_tester.hpp"
//
//using namespace duckdb;
//using namespace std;
//
//void AddNumbersTogether(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
//	// get the total number of rows in this chunk
//	idx_t input_size = duckdb_data_chunk_get_size(input);

//	// extract the two input vectors
//	duckdb_vector a = duckdb_data_chunk_get_vector(input, 0);
//	duckdb_vector b = duckdb_data_chunk_get_vector(input, 1);

//	// get the data pointers for the input vectors (both int64 as specified by the parameter types)
//	auto a_data = (int64_t *)duckdb_vector_get_data(a);
//	auto b_data = (int64_t *)duckdb_vector_get_data(b);
//	auto result_data = (int64_t *)duckdb_vector_get_data(output);

//	// get the validity vectors
//	auto a_validity = duckdb_vector_get_validity(a);
//	auto b_validity = duckdb_vector_get_validity(b);
//	if (a_validity || b_validity) {
//		// if either a_validity or b_validity is defined there might be NULL values
//		duckdb_vector_ensure_validity_writable(output);
//		auto result_validity = duckdb_vector_get_validity(output);
//		for (idx_t row = 0; row < input_size; row++) {
//			if (duckdb_validity_row_is_valid(a_validity, row) && duckdb_validity_row_is_valid(b_validity, row)) {
//				// not null - do the addition
//				result_data[row] = a_data[row] + b_data[row];
//			} else {
//				// either a or b is NULL - set the result row to NULL
//				duckdb_validity_set_row_invalid(result_validity, row);
//			}
//		}
//	} else {
//		// no NULL values - iterate and do the operation directly
//		for (idx_t row = 0; row < input_size; row++) {
//			result_data[row] = a_data[row] + b_data[row];
//		}
//	}
//}
//

//
//TEST_CASE("Test Scalar Functions C API", "[capi]") {
//	CAPITester tester;
//	duckdb::unique_ptr<CAPIResult> result;
//
//	REQUIRE(tester.OpenDatabase(nullptr));
//	CAPIRegisterAddition(tester.connection, "my_addition", DuckDBSuccess);
//	// try to register it again - this should be an error
//	CAPIRegisterAddition(tester.connection, "my_addition", DuckDBError);
//
//	// now call it
//	result = tester.Query("SELECT my_addition(40, 2)");
//	REQUIRE_NO_FAIL(*result);
//	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
//
//	result = tester.Query("SELECT my_addition(40, NULL)");
//	REQUIRE_NO_FAIL(*result);
//	REQUIRE(result->IsNull(0, 0));
//
//	result = tester.Query("SELECT my_addition(NULL, 2)");
//	REQUIRE_NO_FAIL(*result);
//	REQUIRE(result->IsNull(0, 0));
//
//	// call it over a vector of values
//	result = tester.Query("SELECT my_addition(1000000, i) FROM range(10000) t(i)");
//	REQUIRE_NO_FAIL(*result);
//	for (idx_t row = 0; row < 10000; row++) {
//		REQUIRE(result->Fetch<int64_t>(0, row) == static_cast<int64_t>(1000000 + row));
//	}
//}
//
//void ReturnStringInfo(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
//	auto extra_info = string((const char *)info);
//	// get the total number of rows in this chunk
//	idx_t input_size = duckdb_data_chunk_get_size(input);
//	// extract the two input vectors
//	duckdb_vector input_vector = duckdb_data_chunk_get_vector(input, 0);
//	// get the data pointers for the input vectors (both int64 as specified by the parameter types)
//	auto input_data = (duckdb_string_t *)duckdb_vector_get_data(input_vector);
//	// get the validity vectors
//	auto input_validity = duckdb_vector_get_validity(input_vector);
//	duckdb_vector_ensure_validity_writable(output);
//	auto result_validity = duckdb_vector_get_validity(output);
//	for (idx_t row = 0; row < input_size; row++) {
//		if (duckdb_validity_row_is_valid(input_validity, row)) {
//			// not null - do the operation
//			auto input_string = input_data[row];
//			string result = extra_info + "_";
//			if (duckdb_string_is_inlined(input_string)) {
//				result += string(input_string.value.inlined.inlined, input_string.value.inlined.length);
//			} else {
//				result += input_string.value.pointer.ptr;
//			}
//			duckdb_vector_assign_string_element_len(output, row, result.c_str(), result.size());
//		} else {
//			// either a or b is NULL - set the result row to NULL
//			duckdb_validity_set_row_invalid(result_validity, row);
//		}
//	}
//}
//

