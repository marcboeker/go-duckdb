#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif

























#include <cctype>
#include <cmath>
#include <cstdlib>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(bool input, bool &result, bool strict) {
	return NumericTryCast::Operation<bool, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<bool, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, float &result, bool strict) {
	return NumericTryCast::Operation<bool, float>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, double &result, bool strict) {
	return NumericTryCast::Operation<bool, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(float input, bool &result, bool strict) {
	return NumericTryCast::Operation<float, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<float, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<float, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<float, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<float, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<float, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, float &result, bool strict) {
	return NumericTryCast::Operation<float, float>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, double &result, bool strict) {
	return NumericTryCast::Operation<float, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(double input, bool &result, bool strict) {
	return NumericTryCast::Operation<double, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<double, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<double, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<double, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<double, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<double, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, float &result, bool strict) {
	return NumericTryCast::Operation<double, float>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, double &result, bool strict) {
	return NumericTryCast::Operation<double, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <typename T>
struct IntegerCastData {
	using Result = T;
	Result result;
	bool seen_decimal;
};

struct IntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		using result_t = typename T::Result;
		if (NEGATIVE) {
			if (state.result < (NumericLimits<result_t>::Minimum() + digit) / 10) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 10) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		using result_t = typename T::Result;
		if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 16) {
			return false;
		}
		state.result = state.result * 16 + digit;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		using result_t = typename T::Result;
		if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 2) {
			return false;
		}
		state.result = state.result * 2 + digit;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		using result_t = typename T::Result;
		double dbl_res = state.result * std::pow(10.0L, exponent);
		if (dbl_res < (double)NumericLimits<result_t>::Minimum() ||
		    dbl_res > (double)NumericLimits<result_t>::Maximum()) {
			return false;
		}
		state.result = (result_t)std::nearbyint(dbl_res);
		return true;
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.seen_decimal) {
			return true;
		}
		state.seen_decimal = true;
		// round the integer based on what is after the decimal point
		// if digit >= 5, then we round up (or down in case of negative numbers)
		auto increment = digit >= 5;
		if (!increment) {
			return true;
		}
		if (NEGATIVE) {
			if (state.result == NumericLimits<typename T::Result>::Minimum()) {
				return false;
			}
			state.result--;
		} else {
			if (state.result == NumericLimits<typename T::Result>::Maximum()) {
				return false;
			}
			state.result++;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		return true;
	}
};

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation, char decimal_separator = '.'>
static bool IntegerCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	idx_t start_pos;
	if (NEGATIVE) {
		start_pos = 1;
	} else {
		if (*buf == '+') {
			if (strict) {
				// leading plus is not allowed in strict mode
				return false;
			}
			start_pos = 1;
		} else {
			start_pos = 0;
		}
	}
	idx_t pos = start_pos;
	while (pos < len) {
		if (!StringUtil::CharacterIsDigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == decimal_separator) {
				if (strict) {
					return false;
				}
				bool number_before_period = pos > start_pos;
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				idx_t start_digit = pos;
				while (pos < len) {
					if (!StringUtil::CharacterIsDigit(buf[pos])) {
						break;
					}
					if (!OP::template HandleDecimal<T, NEGATIVE, ALLOW_EXPONENT>(result, buf[pos] - '0')) {
						return false;
					}
					pos++;
				}
				// make sure there is either (1) one number after the period, or (2) one number before the period
				// i.e. we accept "1." and ".1" as valid numbers, but not "."
				if (!(number_before_period || pos > start_digit)) {
					return false;
				}
				if (pos >= len) {
					break;
				}
			}
			if (StringUtil::CharacterIsSpace(buf[pos])) {
				// skip any trailing spaces
				while (++pos < len) {
					if (!StringUtil::CharacterIsSpace(buf[pos])) {
						return false;
					}
				}
				break;
			}
			if (ALLOW_EXPONENT) {
				if (buf[pos] == 'e' || buf[pos] == 'E') {
					if (pos == start_pos) {
						return false;
					}
					pos++;
					if (pos >= len) {
						return false;
					}
					using ExponentData = IntegerCastData<int32_t>;
					ExponentData exponent {0, false};
					int negative = buf[pos] == '-';
					if (negative) {
						if (!IntegerCastLoop<ExponentData, true, false, IntegerCastOperation, decimal_separator>(
						        buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<ExponentData, false, false, IntegerCastOperation, decimal_separator>(
						        buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					}
					return OP::template HandleExponent<T, NEGATIVE>(result, exponent.result);
				}
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		if (!OP::template HandleDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerHexCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	if (ALLOW_EXPONENT || NEGATIVE) {
		return false;
	}
	idx_t start_pos = 1;
	idx_t pos = start_pos;
	char current_char;
	while (pos < len) {
		current_char = StringUtil::CharacterToLower(buf[pos]);
		if (!StringUtil::CharacterIsHex(current_char)) {
			return false;
		}
		uint8_t digit;
		if (current_char >= 'a') {
			digit = current_char - 'a' + 10;
		} else {
			digit = current_char - '0';
		}
		pos++;
		if (!OP::template HandleHexDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerBinaryCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	if (ALLOW_EXPONENT || NEGATIVE) {
		return false;
	}
	idx_t start_pos = 1;
	idx_t pos = start_pos;
	uint8_t digit;
	char current_char;
	while (pos < len) {
		current_char = buf[pos];
		if (current_char == '_' && pos > start_pos) {
			// skip underscore, if it is not the first character
			pos++;
			if (pos == len) {
				// we cant end on an underscore either
				return false;
			}
			continue;
		} else if (current_char == '0') {
			digit = 0;
		} else if (current_char == '1') {
			digit = 1;
		} else {
			return false;
		}
		pos++;
		if (!OP::template HandleBinaryDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T, NEGATIVE>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool IS_SIGNED = true, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation,
          bool ZERO_INITIALIZE = true, char decimal_separator = '.'>
static bool TryIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	if (ZERO_INITIALIZE) {
		memset(&result, 0, sizeof(T));
	}
	// if the number is negative, we set the negative flag and skip the negative sign
	if (*buf == '-') {
		if (!IS_SIGNED) {
			// Need to check if its not -0
			idx_t pos = 1;
			while (pos < len) {
				if (buf[pos++] != '0') {
					return false;
				}
			}
		}
		return IntegerCastLoop<T, true, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
	}
	if (len > 1 && *buf == '0') {
		if (buf[1] == 'x' || buf[1] == 'X') {
			// If it starts with 0x or 0X, we parse it as a hex value
			buf++;
			len--;
			return IntegerHexCastLoop<T, false, false, OP>(buf, len, result, strict);
		} else if (buf[1] == 'b' || buf[1] == 'B') {
			// If it starts with 0b or 0B, we parse it as a binary value
			buf++;
			len--;
			return IntegerBinaryCastLoop<T, false, false, OP>(buf, len, result, strict);
		} else if (strict && StringUtil::CharacterIsDigit(buf[1])) {
			// leading zeros are not allowed in strict mode
			return false;
		}
	}
	return IntegerCastLoop<T, false, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
}

template <typename T, bool IS_SIGNED = true>
static inline bool TrySimpleIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	IntegerCastData<T> data;
	if (TryIntegerCast<IntegerCastData<T>, IS_SIGNED>(buf, len, data, strict)) {
		result = data.result;
		return true;
	}
	return false;
}

template <>
bool TryCast::Operation(string_t input, bool &result, bool strict) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();

	switch (input_size) {
	case 1: {
		char c = std::tolower(*input_data);
		if (c == 't' || (!strict && c == '1')) {
			result = true;
			return true;
		} else if (c == 'f' || (!strict && c == '0')) {
			result = false;
			return true;
		}
		return false;
	}
	case 4: {
		char t = std::tolower(input_data[0]);
		char r = std::tolower(input_data[1]);
		char u = std::tolower(input_data[2]);
		char e = std::tolower(input_data[3]);
		if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
			result = true;
			return true;
		}
		return false;
	}
	case 5: {
		char f = std::tolower(input_data[0]);
		char a = std::tolower(input_data[1]);
		char l = std::tolower(input_data[2]);
		char s = std::tolower(input_data[3]);
		char e = std::tolower(input_data[4]);
		if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
			result = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}
template <>
bool TryCast::Operation(string_t input, int8_t &result, bool strict) {
	return TrySimpleIntegerCast<int8_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int16_t &result, bool strict) {
	return TrySimpleIntegerCast<int16_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int32_t &result, bool strict) {
	return TrySimpleIntegerCast<int32_t>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int64_t &result, bool strict) {
	return TrySimpleIntegerCast<int64_t>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, uint8_t &result, bool strict) {
	return TrySimpleIntegerCast<uint8_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict) {
	return TrySimpleIntegerCast<uint16_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict) {
	return TrySimpleIntegerCast<uint32_t, false>(input.GetData(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict) {
	return TrySimpleIntegerCast<uint64_t, false>(input.GetData(), input.GetSize(), result, strict);
}

template <class T, char decimal_separator = '.'>
static bool TryDoubleCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	if (*buf == '+') {
		if (strict) {
			// plus is not allowed in strict mode
			return false;
		}
		buf++;
		len--;
	}
	if (strict && len >= 2) {
		if (buf[0] == '0' && StringUtil::CharacterIsDigit(buf[1])) {
			// leading zeros are not allowed in strict mode
			return false;
		}
	}
	auto endptr = buf + len;
	auto parse_result = duckdb_fast_float::from_chars(buf, buf + len, result, decimal_separator);
	if (parse_result.ec != std::errc()) {
		return false;
	}
	auto current_end = parse_result.ptr;
	if (!strict) {
		while (current_end < endptr && StringUtil::CharacterIsSpace(*current_end)) {
			current_end++;
		}
	}
	return current_end == endptr;
}

template <>
bool TryCast::Operation(string_t input, float &result, bool strict) {
	return TryDoubleCast<float>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, double &result, bool strict) {
	return TryDoubleCast<double>(input.GetData(), input.GetSize(), result, strict);
}

template <>
bool TryCastErrorMessageCommaSeparated::Operation(string_t input, float &result, string *error_message, bool strict) {
	if (!TryDoubleCast<float, ','>(input.GetData(), input.GetSize(), result, strict)) {
		HandleCastError::AssignError(StringUtil::Format("Could not cast string to float: \"%s\"", input.GetString()),
		                             error_message);
		return false;
	}
	return true;
}

template <>
bool TryCastErrorMessageCommaSeparated::Operation(string_t input, double &result, string *error_message, bool strict) {
	if (!TryDoubleCast<double, ','>(input.GetData(), input.GetSize(), result, strict)) {
		HandleCastError::AssignError(StringUtil::Format("Could not cast string to double: \"%s\"", input.GetString()),
		                             error_message);
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(date_t input, date_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(date_t input, timestamp_t &result, bool strict) {
	if (input == date_t::infinity()) {
		result = timestamp_t::infinity();
		return true;
	} else if (input == date_t::ninfinity()) {
		result = timestamp_t::ninfinity();
		return true;
	}
	return Timestamp::TryFromDatetime(input, Time::FromTime(0, 0, 0), result);
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(dtime_t input, dtime_tz_t &result, bool strict) {
	result = dtime_tz_t(input, 0);
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Time With Time Zone (Offset)
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_tz_t input, dtime_tz_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(dtime_tz_t input, dtime_t &result, bool strict) {
	result = input.time();
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(timestamp_t input, date_t &result, bool strict) {
	result = Timestamp::GetDate(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict) {
	if (!Timestamp::IsFinite(input)) {
		return false;
	}
	result = Timestamp::GetTime(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_tz_t &result, bool strict) {
	if (!Timestamp::IsFinite(input)) {
		return false;
	}
	result = dtime_tz_t(Timestamp::GetTime(input), 0);
	return true;
}

//===--------------------------------------------------------------------===//
// Cast from Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(interval_t input, interval_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps
//===--------------------------------------------------------------------===//
template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochNanoSeconds(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochMs(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochSeconds(input.value), result);
}

template <>
timestamp_t CastTimestampUsToMs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochMs(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToNs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochNanoSeconds(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToSec::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochSeconds(input));
	return cast_timestamp;
}
template <>
timestamp_t CastTimestampMsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochMs(input.value);
}

template <>
timestamp_t CastTimestampMsToNs::Operation(timestamp_t input) {
	auto us = CastTimestampMsToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToNs::Operation<timestamp_t, timestamp_t>(us);
}

template <>
timestamp_t CastTimestampNsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochNanoSeconds(input.value);
}

template <>
timestamp_t CastTimestampSecToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochSeconds(input.value);
}

template <>
timestamp_t CastTimestampSecToMs::Operation(timestamp_t input) {
	auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToMs::Operation<timestamp_t, timestamp_t>(us);
}

template <>
timestamp_t CastTimestampSecToNs::Operation(timestamp_t input) {
	auto us = CastTimestampSecToUs::Operation<timestamp_t, timestamp_t>(input);
	return CastTimestampUsToNs::Operation<timestamp_t, timestamp_t>(us);
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochNanoSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochMs(result);
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampNS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!TryMultiplyOperator::Operation(result.value, Interval::NANOS_PER_MICRO, result.value)) {
		return false;
	}
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result.value /= Interval::MICROS_PER_MSEC;
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result.value /= Interval::MICROS_PER_MSEC * Interval::MSECS_PER_SEC;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Blob
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBlob::Operation(string_t input, Vector &vector) {
	idx_t result_size = Blob::GetStringSize(input);

	string_t result = StringVector::EmptyString(vector, result_size);
	Blob::ToString(input, result.GetDataWriteable());
	result.Finalize();

	return result;
}

template <>
string_t CastFromBlobToBit::Operation(string_t input, Vector &vector) {
	idx_t result_size = input.GetSize() + 1;
	if (result_size <= 1) {
		throw ConversionException("Cannot cast empty BLOB to BIT");
	}
	return StringVector::AddStringOrBlob(vector, Bit::BlobToBit(input));
}

//===--------------------------------------------------------------------===//
// Cast From Bit
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBitToString::Operation(string_t input, Vector &vector) {

	idx_t result_size = Bit::BitLength(input);
	string_t result = StringVector::EmptyString(vector, result_size);
	Bit::ToString(input, result.GetDataWriteable());
	result.Finalize();

	return result;
}

//===--------------------------------------------------------------------===//
// Cast From Pointer
//===--------------------------------------------------------------------===//
template <>
string_t CastFromPointer::Operation(uintptr_t input, Vector &vector) {
	std::string s = duckdb_fmt::format("0x{:x}", input);
	return StringVector::AddString(vector, s);
}

//===--------------------------------------------------------------------===//
// Cast To Blob
//===--------------------------------------------------------------------===//
template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message,
                              bool strict) {
	idx_t result_size;
	if (!Blob::TryGetBlobSize(input, result_size, error_message)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Blob::ToBlob(input, data_ptr_cast(result.GetDataWriteable()));
	result.Finalize();
	return true;
}

//===--------------------------------------------------------------------===//
// Cast To Bit
//===--------------------------------------------------------------------===//
template <>
bool TryCastToBit::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message,
                             bool strict) {
	idx_t result_size;
	if (!Bit::TryGetBitStringSize(input, result_size, error_message)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Bit::ToBit(input, result);
	result.Finalize();
	return true;
}

template <>
bool CastFromBitToNumeric::Operation(string_t input, bool &result, bool strict) {
	D_ASSERT(input.GetSize() > 1);

	uint8_t value;
	bool success = CastFromBitToNumeric::Operation(input, value, strict);
	result = (value > 0);
	return (success);
}

template <>
bool CastFromBitToNumeric::Operation(string_t input, hugeint_t &result, bool strict) {
	D_ASSERT(input.GetSize() > 1);

	if (input.GetSize() - 1 > sizeof(hugeint_t)) {
		throw ConversionException("Bitstring doesn't fit inside of %s", GetTypeId<hugeint_t>());
	}
	Bit::BitToNumeric(input, result);
	if (result < NumericLimits<hugeint_t>::Minimum()) {
		throw ConversionException("Minimum limit for HUGEINT is %s", NumericLimits<hugeint_t>::Minimum().ToString());
	}
	return (true);
}

//===--------------------------------------------------------------------===//
// Cast From UUID
//===--------------------------------------------------------------------===//
template <>
string_t CastFromUUID::Operation(hugeint_t input, Vector &vector) {
	string_t result = StringVector::EmptyString(vector, 36);
	UUID::ToString(input, result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To UUID
//===--------------------------------------------------------------------===//
template <>
bool TryCastToUUID::Operation(string_t input, hugeint_t &result, Vector &result_vector, string *error_message,
                              bool strict) {
	return UUID::FromString(input.GetString(), result);
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, date_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, date_t>(input, result, strict)) {
		HandleCastError::AssignError(Date::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, date_t &result, bool strict) {
	idx_t pos;
	bool special = false;
	return Date::TryConvertDate(input.GetData(), input.GetSize(), pos, result, special, strict);
}

template <>
date_t Cast::Operation(string_t input) {
	return Date::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, dtime_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, dtime_t>(input, result, strict)) {
		HandleCastError::AssignError(Time::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, dtime_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTime(input.GetData(), input.GetSize(), pos, result, strict);
}

template <>
dtime_t Cast::Operation(string_t input) {
	return Time::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To TimeTZ
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, dtime_tz_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, dtime_tz_t>(input, result, strict)) {
		HandleCastError::AssignError(Time::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, dtime_tz_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTimeTZ(input.GetData(), input.GetSize(), pos, result, strict);
}

template <>
dtime_tz_t Cast::Operation(string_t input) {
	dtime_tz_t result;
	if (!TryCast::Operation(input, result, false)) {
		throw ConversionException(Time::ConversionError(input));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, timestamp_t &result, string *error_message, bool strict) {
	auto cast_result = Timestamp::TryConvertTimestamp(input.GetData(), input.GetSize(), result);
	if (cast_result == TimestampCastResult::SUCCESS) {
		return true;
	}
	if (cast_result == TimestampCastResult::ERROR_INCORRECT_FORMAT) {
		HandleCastError::AssignError(Timestamp::ConversionError(input), error_message);
	} else {
		HandleCastError::AssignError(Timestamp::UnsupportedTimezoneError(input), error_message);
	}
	return false;
}

template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict) {
	return Timestamp::TryConvertTimestamp(input.GetData(), input.GetSize(), result) == TimestampCastResult::SUCCESS;
}

template <>
timestamp_t Cast::Operation(string_t input) {
	return Timestamp::FromCString(input.GetData(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast From Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, interval_t &result, string *error_message, bool strict) {
	return Interval::FromCString(input.GetData(), input.GetSize(), result, error_message, strict);
}

//===--------------------------------------------------------------------===//
// Cast From Hugeint
//===--------------------------------------------------------------------===//
// parsing hugeint from string is done a bit differently for performance reasons
// for other integer types we keep track of a single value
// and multiply that value by 10 for every digit we read
// however, for hugeints, multiplication is very expensive (>20X as expensive as for int64)
// for that reason, we parse numbers first into an int64 value
// when that value is full, we perform a HUGEINT multiplication to flush it into the hugeint
// this takes the number of HUGEINT multiplications down from [0-38] to [0-2]
struct HugeIntCastData {
	hugeint_t hugeint;
	int64_t intermediate;
	uint8_t digits;
	bool decimal;

	bool Flush() {
		if (digits == 0 && intermediate == 0) {
			return true;
		}
		if (hugeint.lower != 0 || hugeint.upper != 0) {
			if (digits > 38) {
				return false;
			}
			if (!Hugeint::TryMultiply(hugeint, Hugeint::POWERS_OF_TEN[digits], hugeint)) {
				return false;
			}
		}
		if (!Hugeint::AddInPlace(hugeint, hugeint_t(intermediate))) {
			return false;
		}
		digits = 0;
		intermediate = 0;
		return true;
	}
};

struct HugeIntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &result, uint8_t digit) {
		if (NEGATIVE) {
			if (result.intermediate < (NumericLimits<int64_t>::Minimum() + digit) / 10) {
				// intermediate is full: need to flush it
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 - digit;
		} else {
			if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10) {
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 + digit;
		}
		result.digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &result, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &result, uint8_t digit) {
		if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 2) {
			// intermediate is full: need to flush it
			if (!result.Flush()) {
				return false;
			}
		}
		result.intermediate = result.intermediate * 2 + digit;
		result.digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &result, int32_t exponent) {
		if (!result.Flush()) {
			return false;
		}
		if (exponent < -38 || exponent > 38) {
			// out of range for exact exponent: use double and convert
			double dbl_res = Hugeint::Cast<double>(result.hugeint) * std::pow(10.0L, exponent);
			if (dbl_res < Hugeint::Cast<double>(NumericLimits<hugeint_t>::Minimum()) ||
			    dbl_res > Hugeint::Cast<double>(NumericLimits<hugeint_t>::Maximum())) {
				return false;
			}
			result.hugeint = Hugeint::Convert(dbl_res);
			return true;
		}
		if (exponent < 0) {
			// negative exponent: divide by power of 10
			result.hugeint = Hugeint::Divide(result.hugeint, Hugeint::POWERS_OF_TEN[-exponent]);
			return true;
		} else {
			// positive exponent: multiply by power of 10
			return Hugeint::TryMultiply(result.hugeint, Hugeint::POWERS_OF_TEN[exponent], result.hugeint);
		}
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &result, uint8_t digit) {
		// Integer casts round
		if (!result.decimal) {
			if (!result.Flush()) {
				return false;
			}
			if (NEGATIVE) {
				result.intermediate = -(digit >= 5);
			} else {
				result.intermediate = (digit >= 5);
			}
		}
		result.decimal = true;

		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &result) {
		return result.Flush();
	}
};

template <>
bool TryCast::Operation(string_t input, hugeint_t &result, bool strict) {
	HugeIntCastData data;
	if (!TryIntegerCast<HugeIntCastData, true, true, HugeIntegerCastOperation>(input.GetData(), input.GetSize(), data,
	                                                                           strict)) {
		return false;
	}
	result = data.hugeint;
	return true;
}

//===--------------------------------------------------------------------===//
// Decimal String Cast
//===--------------------------------------------------------------------===//

template <class TYPE>
struct DecimalCastData {
	typedef TYPE type_t;
	TYPE result;
	uint8_t width;
	uint8_t scale;
	uint8_t digit_count;
	uint8_t decimal_count;
	//! Whether we have determined if the result should be rounded
	bool round_set;
	//! If the result should be rounded
	bool should_round;
	//! Only set when ALLOW_EXPONENT is enabled
	enum class ExponentType : uint8_t { NONE, POSITIVE, NEGATIVE };
	uint8_t excessive_decimals;
	ExponentType exponent_type;
};

struct DecimalCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// leading zero's don't count towards the digit count
			return true;
		}
		if (state.digit_count == state.width - state.scale) {
			// width of decimal type is exceeded!
			return false;
		}
		state.digit_count++;
		if (NEGATIVE) {
			if (state.result < (NumericLimits<typename T::type_t>::Minimum() / 10)) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (state.result > (NumericLimits<typename T::type_t>::Maximum() / 10)) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleHexDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static bool HandleBinaryDigit(T &state, uint8_t digit) {
		return false;
	}

	template <class T, bool NEGATIVE>
	static void RoundUpResult(T &state) {
		if (NEGATIVE) {
			state.result -= 1;
		} else {
			state.result += 1;
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		auto decimal_excess = (state.decimal_count > state.scale) ? state.decimal_count - state.scale : 0;
		if (exponent > 0) {
			state.exponent_type = T::ExponentType::POSITIVE;
			// Positive exponents need up to 'exponent' amount of digits
			// Everything beyond that amount needs to be truncated
			if (decimal_excess > exponent) {
				// We've allowed too many decimals
				state.excessive_decimals = decimal_excess - exponent;
				exponent = 0;
			} else {
				exponent -= decimal_excess;
			}
			D_ASSERT(exponent >= 0);
		} else if (exponent < 0) {
			state.exponent_type = T::ExponentType::NEGATIVE;
		}
		if (!Finalize<T, NEGATIVE>(state)) {
			return false;
		}
		if (exponent < 0) {
			bool round_up = false;
			for (idx_t i = 0; i < idx_t(-int64_t(exponent)); i++) {
				auto mod = state.result % 10;
				round_up = NEGATIVE ? mod <= -5 : mod >= 5;
				state.result /= 10;
				if (state.result == 0) {
					break;
				}
			}
			if (round_up) {
				RoundUpResult<T, NEGATIVE>(state);
			}
			return true;
		} else {
			// positive exponent: append 0's
			for (idx_t i = 0; i < idx_t(exponent); i++) {
				if (!HandleDigit<T, NEGATIVE>(state, 0)) {
					return false;
				}
			}
			return true;
		}
	}

	template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.decimal_count == state.scale && !state.round_set) {
			// Determine whether the last registered decimal should be rounded or not
			state.round_set = true;
			state.should_round = digit >= 5;
		}
		if (!ALLOW_EXPONENT && state.decimal_count == state.scale) {
			// we exceeded the amount of supported decimals
			// however, we don't throw an error here
			// we just truncate the decimal
			return true;
		}
		//! If we expect an exponent, we need to preserve the decimals
		//! But we don't want to overflow, so we prevent overflowing the result with this check
		if (state.digit_count + state.decimal_count >= DecimalWidth<decltype(state.result)>::max) {
			return true;
		}
		state.decimal_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool TruncateExcessiveDecimals(T &state) {
		D_ASSERT(state.excessive_decimals);
		bool round_up = false;
		for (idx_t i = 0; i < state.excessive_decimals; i++) {
			auto mod = state.result % 10;
			round_up = NEGATIVE ? mod <= -5 : mod >= 5;
			state.result /= 10.0;
		}
		//! Only round up when exponents are involved
		if (state.exponent_type == T::ExponentType::POSITIVE && round_up) {
			RoundUpResult<T, NEGATIVE>(state);
		}
		D_ASSERT(state.decimal_count > state.scale);
		state.decimal_count = state.scale;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool Finalize(T &state) {
		if (state.exponent_type != T::ExponentType::POSITIVE && state.decimal_count > state.scale) {
			//! Did not encounter an exponent, but ALLOW_EXPONENT was on
			state.excessive_decimals = state.decimal_count - state.scale;
		}
		if (state.excessive_decimals && !TruncateExcessiveDecimals<T, NEGATIVE>(state)) {
			return false;
		}
		if (state.exponent_type == T::ExponentType::NONE && state.round_set && state.should_round) {
			RoundUpResult<T, NEGATIVE>(state);
		}
		//  if we have not gotten exactly "scale" decimals, we need to multiply the result
		//  e.g. if we have a string "1.0" that is cast to a DECIMAL(9,3), the value needs to be 1000
		//  but we have only gotten the value "10" so far, so we multiply by 1000
		for (uint8_t i = state.decimal_count; i < state.scale; i++) {
			state.result *= 10;
		}
		return true;
	}
};

template <class T, char decimal_separator = '.'>
bool TryDecimalStringCast(string_t input, T &result, string *error_message, uint8_t width, uint8_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.decimal_count = 0;
	state.excessive_decimals = 0;
	state.exponent_type = DecimalCastData<T>::ExponentType::NONE;
	state.round_set = false;
	state.should_round = false;
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false, decimal_separator>(
	        input.GetData(), input.GetSize(), state, false)) {
		string error = StringUtil::Format("Could not convert string \"%s\" to DECIMAL(%d,%d)", input.GetString(),
		                                  (int)width, (int)scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = state.result;
	return true;
}

template <>
bool TryCastToDecimal::Operation(string_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<hugeint_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int16_t &result, string *error_message, uint8_t width,
                                               uint8_t scale) {
	return TryDecimalStringCast<int16_t, ','>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int32_t &result, string *error_message, uint8_t width,
                                               uint8_t scale) {
	return TryDecimalStringCast<int32_t, ','>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, int64_t &result, string *error_message, uint8_t width,
                                               uint8_t scale) {
	return TryDecimalStringCast<int64_t, ','>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimalCommaSeparated::Operation(string_t input, hugeint_t &result, string *error_message, uint8_t width,
                                               uint8_t scale) {
	return TryDecimalStringCast<hugeint_t, ','>(input, result, error_message, width, scale);
}

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int16_t, uint16_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int32_t, uint32_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int64_t, uint64_t>(input, width, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result) {
	return HugeintToStringCast::FormatDecimal(input, width, scale, result);
}

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
// Decimal <-> Bool
//===--------------------------------------------------------------------===//
template <class T, class OP = NumericHelper>
bool TryCastBoolToDecimal(bool input, T &result, string *error_message, uint8_t width, uint8_t scale) {
	if (width > scale) {
		result = input ? OP::POWERS_OF_TEN[scale] : 0;
		return true;
	} else {
		return TryCast::Operation<bool, T>(input, result);
	}
}

template <>
bool TryCastToDecimal::Operation(bool input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<hugeint_t, Hugeint>(input, result, error_message, width, scale);
}

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int16_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int32_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int64_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<hugeint_t, bool>(input, result);
}

//===--------------------------------------------------------------------===//
// Numeric -> Decimal Cast
//===--------------------------------------------------------------------===//
struct SignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return int64_t(input) >= int64_t(max_width) || int64_t(input) <= int64_t(-max_width);
	}
};

struct UnsignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return uint64_t(input) >= uint64_t(max_width);
	}
};

template <class SRC, class DST, class OP = SignedToDecimalOperator>
bool StandardNumericToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	DST max_width = NumericHelper::POWERS_OF_TEN[width - scale];
	if (OP::template Operation<SRC, DST>(input, max_width)) {
		string error = StringUtil::Format("Could not cast value %d to DECIMAL(%d,%d)", input, width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = DST(input) * NumericHelper::POWERS_OF_TEN[scale];
	return true;
}

template <class SRC>
bool NumericToHugeDecimalCast(SRC input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	hugeint_t hinput = Hugeint::Convert(input);
	if (hinput >= max_width || hinput <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", hinput.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = hinput * Hugeint::POWERS_OF_TEN[scale];
	return true;
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int8_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int8_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int16_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int32_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int64_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint8_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint8_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint16_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint16_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint32_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint32_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint64_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint64_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Hugeint -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class DST>
bool HugeintToDecimalCast(hugeint_t input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width || input <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Hugeint::Cast<DST>(input * Hugeint::POWERS_OF_TEN[scale]);
	return true;
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Float/Double -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool DoubleToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	double value = input * NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
	// Add the sign (-1, 0, 1) times a tiny value to fix floating point issues (issue 3091)
	double sign = (double(0) < value) - (value < double(0));
	value += 1e-9 * sign;
	if (value <= -NumericHelper::DOUBLE_POWERS_OF_TEN[width] || value >= NumericHelper::DOUBLE_POWERS_OF_TEN[width]) {
		string error = StringUtil::Format("Could not cast value %f to DECIMAL(%d,%d)", value, width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Cast::Operation<SRC, DST>(value);
	return true;
}

template <>
bool TryCastToDecimal::Operation(float input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, hugeint_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Numeric Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToNumeric(SRC input, DST &result, string *error_message, uint8_t scale) {
	// Round away from 0.
	const auto power = NumericHelper::POWERS_OF_TEN[scale];
	// https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
	const auto fNegate = int64_t(input < 0);
	const auto rounding = ((power ^ -fNegate) + fNegate) / 2;
	const auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<SRC, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %d to type %s", scaled_value, GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

template <class DST>
bool TryCastHugeDecimalToNumeric(hugeint_t input, DST &result, string *error_message, uint8_t scale) {
	const auto power = Hugeint::POWERS_OF_TEN[scale];
	const auto rounding = ((input < 0) ? -power : power) / 2;
	auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<hugeint_t, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %s to type %s",
		                                  ConvertToString::Operation(scaled_value), GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int8_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int16_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int32_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int64_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint8_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint16_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint32_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint64_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> hugeint_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<hugeint_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Float/Double Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToFloatingPoint(SRC input, DST &result, uint8_t scale) {
	result = Cast::Operation<SRC, DST>(input) / DST(NumericHelper::DOUBLE_POWERS_OF_TEN[scale]);
	return true;
}

// DECIMAL -> FLOAT
template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, float>(input, result, scale);
}

// DECIMAL -> DOUBLE
template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, double>(input, result, scale);
}

} // namespace duckdb




namespace duckdb {

template <class T>
string StandardStringCast(T input) {
	Vector v(LogicalType::VARCHAR);
	return StringCast::Operation(input, v).GetString();
}

template <>
string ConvertToString::Operation(bool input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(hugeint_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(float input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(double input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(interval_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(date_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(dtime_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(timestamp_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(string_t input) {
	return input.GetString();
}

} // namespace duckdb










namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <>
string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return StringVector::AddString(vector, "true", 4);
	} else {
		return StringVector::AddString(vector, "false", 5);
	}
}

template <>
string_t StringCast::Operation(int8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int8_t, uint8_t>(input, vector);
}

template <>
string_t StringCast::Operation(int16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int16_t, uint16_t>(input, vector);
}
template <>
string_t StringCast::Operation(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

template <>
string_t StringCast::Operation(int64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int64_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint8_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint16_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint32_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint64_t, uint64_t>(input, vector);
}

template <>
string_t StringCast::Operation(float input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(double input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(interval_t input, Vector &vector) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(input, buffer);
	return StringVector::AddString(vector, buffer, length);
}

template <>
duckdb::string_t StringCast::Operation(hugeint_t input, Vector &vector) {
	return HugeintToStringCast::FormatSigned(input, vector);
}

template <>
duckdb::string_t StringCast::Operation(date_t input, Vector &vector) {
	if (input == date_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == date_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	int32_t date[3];
	Date::Convert(input, date[0], date[1], date[2]);

	idx_t year_length;
	bool add_bc;
	idx_t length = DateToStringCast::Length(date, year_length, add_bc);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	char micro_buffer[10];
	idx_t length = TimeToStringCast::Length(time, micro_buffer);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	TimeToStringCast::Format(data, length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamp is DATE TIME (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	idx_t length = date_length + time_length + 1;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);
	data[date_length] = ' ';
	TimeToStringCast::Format(data + date_length + 1, time_length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(duckdb::string_t input, Vector &result) {
	return StringVector::AddStringOrBlob(result, input);
}

template <>
string_t StringCastTZ::Operation(dtime_tz_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input.time(), time[0], time[1], time[2], time[3]);

	char micro_buffer[10];
	const auto time_length = TimeToStringCast::Length(time, micro_buffer);
	idx_t length = time_length;

	const auto offset = input.offset();
	const bool negative = (offset < 0);
	++length;

	auto ss = std::abs(offset);
	const auto hh = ss / Interval::SECS_PER_HOUR;

	const auto hh_length = (hh < 100) ? 2 : NumericHelper::UnsignedLength(uint32_t(hh));
	length += hh_length;

	ss %= Interval::SECS_PER_HOUR;
	const auto mm = ss / Interval::SECS_PER_MINUTE;
	if (mm) {
		length += 3;
	}

	ss %= Interval::SECS_PER_MINUTE;
	if (ss) {
		length += 3;
	}

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	TimeToStringCast::Format(data + pos, time_length, time, micro_buffer);
	pos += time_length;

	data[pos++] = negative ? '-' : '+';
	if (hh < 100) {
		TimeToStringCast::FormatTwoDigits(data + pos, hh);
	} else {
		NumericHelper::FormatUnsigned(hh, data + pos + hh_length);
	}
	pos += hh_length;

	if (mm) {
		data[pos++] = ':';
		TimeToStringCast::FormatTwoDigits(data + pos, mm);
		pos += 2;
	}

	if (ss) {
		data[pos++] = ':';
		TimeToStringCast::FormatTwoDigits(data + pos, ss);
		pos += 2;
	}

	result.Finalize();
	return result;
}

template <>
string_t StringCastTZ::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamptz is DATE TIME+00 (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	const idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	const idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	const idx_t length = date_length + 1 + time_length + 3;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	DateToStringCast::Format(data + pos, date, year_length, add_bc);
	pos += date_length;
	data[pos++] = ' ';
	TimeToStringCast::Format(data + pos, time_length, time, micro_buffer);
	pos += time_length;
	data[pos++] = '+';
	data[pos++] = '0';
	data[pos++] = '0';

	result.Finalize();
	return result;
}

} // namespace duckdb





namespace duckdb {
class PipeFile : public FileHandle {
public:
	PipeFile(unique_ptr<FileHandle> child_handle_p, const string &path)
	    : FileHandle(pipe_fs, path), child_handle(std::move(child_handle_p)) {
	}

	PipeFileSystem pipe_fs;
	unique_ptr<FileHandle> child_handle;

public:
	int64_t ReadChunk(void *buffer, int64_t nr_bytes);
	int64_t WriteChunk(void *buffer, int64_t nr_bytes);

	void Close() override {
	}
};

int64_t PipeFile::ReadChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Read(buffer, nr_bytes);
}
int64_t PipeFile::WriteChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Write(buffer, nr_bytes);
}

void PipeFileSystem::Reset(FileHandle &handle) {
	throw InternalException("Cannot reset pipe file system");
}

int64_t PipeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = handle.Cast<PipeFile>();
	return pipe.ReadChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = handle.Cast<PipeFile>();
	return pipe.WriteChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::GetFileSize(FileHandle &handle) {
	return 0;
}

void PipeFileSystem::FileSync(FileHandle &handle) {
}

unique_ptr<FileHandle> PipeFileSystem::OpenPipe(unique_ptr<FileHandle> handle) {
	auto path = handle->path;
	return make_uniq<PipeFile>(std::move(handle), path);
}

} // namespace duckdb







namespace duckdb {

PreservedError::PreservedError() : initialized(false), exception_instance(nullptr) {
}

PreservedError::PreservedError(const Exception &exception)
    : initialized(true), type(exception.type), raw_message(SanitizeErrorMessage(exception.RawMessage())),
      exception_instance(exception.Copy()) {
}

PreservedError::PreservedError(const string &message)
    : initialized(true), type(ExceptionType::INVALID), raw_message(SanitizeErrorMessage(message)),
      exception_instance(nullptr) {
}

const string &PreservedError::Message() {
	if (final_message.empty()) {
		final_message = Exception::ExceptionTypeToString(type) + " Error: " + raw_message;
	}
	return final_message;
}

string PreservedError::SanitizeErrorMessage(string error) {
	return StringUtil::Replace(std::move(error), string("\0", 1), "\\0");
}

void PreservedError::Throw(const string &prepended_message) const {
	D_ASSERT(initialized);
	if (!prepended_message.empty()) {
		string new_message = prepended_message + raw_message;
		Exception::ThrowAsTypeWithMessage(type, new_message, exception_instance);
	}
	Exception::ThrowAsTypeWithMessage(type, raw_message, exception_instance);
}

const ExceptionType &PreservedError::Type() const {
	D_ASSERT(initialized);
	return this->type;
}

PreservedError &PreservedError::AddToMessage(const string &prepended_message) {
	raw_message = prepended_message + raw_message;
	return *this;
}

PreservedError::operator bool() const {
	return initialized;
}

bool PreservedError::operator==(const PreservedError &other) const {
	if (initialized != other.initialized) {
		return false;
	}
	if (type != other.type) {
		return false;
	}
	return raw_message == other.raw_message;
}

} // namespace duckdb




#include <stdio.h>

#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
#include <io.h>
#else
#include <sys/ioctl.h>
#include <stdio.h>
#include <unistd.h>
#endif
#endif

namespace duckdb {

void Printer::RawPrint(OutputStream stream, const string &str) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	if (IsTerminal(stream)) {
		// print utf8 to terminal
		auto unicode = WindowsUtil::UTF8ToMBCS(str.c_str());
		fprintf(stream == OutputStream::STREAM_STDERR ? stderr : stdout, "%s", unicode.c_str());
		return;
	}
#endif
	fprintf(stream == OutputStream::STREAM_STDERR ? stderr : stdout, "%s", str.c_str());
#endif
}

// LCOV_EXCL_START
void Printer::Print(OutputStream stream, const string &str) {
	Printer::RawPrint(stream, str);
	Printer::RawPrint(stream, "\n");
}
void Printer::Flush(OutputStream stream) {
#ifndef DUCKDB_DISABLE_PRINT
	fflush(stream == OutputStream::STREAM_STDERR ? stderr : stdout);
#endif
}

void Printer::Print(const string &str) {
	Printer::Print(OutputStream::STREAM_STDERR, str);
}

bool Printer::IsTerminal(OutputStream stream) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	auto stream_handle = stream == OutputStream::STREAM_STDERR ? STD_ERROR_HANDLE : STD_OUTPUT_HANDLE;
	return GetFileType(GetStdHandle(stream_handle)) == FILE_TYPE_CHAR;
#else
	return isatty(stream == OutputStream::STREAM_STDERR ? 2 : 1);
#endif
#else
	throw InternalException("IsTerminal called while printing is disabled");
#endif
}

idx_t Printer::TerminalWidth() {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	CONSOLE_SCREEN_BUFFER_INFO csbi;
	int columns, rows;

	GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi);
	rows = csbi.srWindow.Right - csbi.srWindow.Left + 1;
	return rows;
#else
	struct winsize w;
	ioctl(0, TIOCGWINSZ, &w);
	return w.ws_col;
#endif
#else
	throw InternalException("TerminalWidth called while printing is disabled");
#endif
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

void ProgressBar::SystemOverrideCheck(ClientConfig &config) {
	if (config.system_progress_bar_disable_reason != nullptr) {
		throw InvalidInputException("Could not change the progress bar setting because: '%s'",
		                            config.system_progress_bar_disable_reason);
	}
}

unique_ptr<ProgressBarDisplay> ProgressBar::DefaultProgressBarDisplay() {
	return make_uniq<TerminalProgressBarDisplay>();
}

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after,
                         progress_bar_display_create_func_t create_display_func)
    : executor(executor), show_progress_after(show_progress_after), current_percentage(-1) {
	if (create_display_func) {
		display = create_display_func();
	}
}

double ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
	profiler.Start();
	current_percentage = 0;
	supported = true;
}

bool ProgressBar::PrintEnabled() const {
	return display != nullptr;
}

bool ProgressBar::ShouldPrint(bool final) const {
	if (!PrintEnabled()) {
		// Don't print progress at all
		return false;
	}
	// FIXME - do we need to check supported before running `profiler.Elapsed()` ?
	auto sufficient_time_elapsed = profiler.Elapsed() > show_progress_after / 1000.0;
	if (!sufficient_time_elapsed) {
		// Don't print yet
		return false;
	}
	if (final) {
		// Print the last completed bar
		return true;
	}
	if (!supported) {
		return false;
	}
	return current_percentage > -1;
}

void ProgressBar::Update(bool final) {
	if (!final && !supported) {
		return;
	}
	double new_percentage;
	supported = executor.GetPipelinesProgress(new_percentage);
	if (!final && !supported) {
		return;
	}
	if (new_percentage > current_percentage) {
		current_percentage = new_percentage;
	}
	if (ShouldPrint(final)) {
#ifndef DUCKDB_DISABLE_PRINT
		if (final) {
			FinishProgressBarPrint();
		} else {
			PrintProgress(current_percentage);
		}
#endif
	}
}

void ProgressBar::PrintProgress(int current_percentage) {
	D_ASSERT(display);
	display->Update(current_percentage);
}

void ProgressBar::FinishProgressBarPrint() {
	if (finished) {
		return;
	}
	D_ASSERT(display);
	display->Finish();
	finished = true;
}

} // namespace duckdb




namespace duckdb {

void TerminalProgressBarDisplay::PrintProgressInternal(int percentage) {
	if (percentage > 100) {
		percentage = 100;
	}
	if (percentage < 0) {
		percentage = 0;
	}
	string result;
	// we divide the number of blocks by the percentage
	// 0%   = 0
	// 100% = PROGRESS_BAR_WIDTH
	// the percentage determines how many blocks we need to draw
	double blocks_to_draw = PROGRESS_BAR_WIDTH * (percentage / 100.0);
	// because of the power of unicode, we can also draw partial blocks

	// render the percentage with some padding to ensure everything stays nicely aligned
	result = "\r";
	if (percentage < 100) {
		result += " ";
	}
	if (percentage < 10) {
		result += " ";
	}
	result += to_string(percentage) + "%";
	result += " ";
	result += PROGRESS_START;
	idx_t i;
	for (i = 0; i < idx_t(blocks_to_draw); i++) {
		result += PROGRESS_BLOCK;
	}
	if (i < PROGRESS_BAR_WIDTH) {
		// print a partial block based on the percentage of the progress bar remaining
		idx_t index = idx_t((blocks_to_draw - idx_t(blocks_to_draw)) * PARTIAL_BLOCK_COUNT);
		if (index >= PARTIAL_BLOCK_COUNT) {
			index = PARTIAL_BLOCK_COUNT - 1;
		}
		result += PROGRESS_PARTIAL[index];
		i++;
	}
	for (; i < PROGRESS_BAR_WIDTH; i++) {
		result += PROGRESS_EMPTY;
	}
	result += PROGRESS_END;
	result += " ";

	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

void TerminalProgressBarDisplay::Update(double percentage) {
	PrintProgressInternal(percentage);
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

void TerminalProgressBarDisplay::Finish() {
	PrintProgressInternal(100);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, "\n");
	Printer::Flush(OutputStream::STREAM_STDOUT);
}

} // namespace duckdb








namespace duckdb {

//! Templated radix partitioning constants, can be templated to the number of radix bits
template <idx_t radix_bits>
struct RadixPartitioningConstants {
public:
	//! Bitmask of the upper bits starting at the 5th byte
	static constexpr const idx_t NUM_PARTITIONS = RadixPartitioning::NumberOfPartitions(radix_bits);
	static constexpr const idx_t SHIFT = RadixPartitioning::Shift(radix_bits);
	static constexpr const hash_t MASK = RadixPartitioning::Mask(radix_bits);

public:
	//! Apply bitmask and right shift to get a number between 0 and NUM_PARTITIONS
	static inline hash_t ApplyMask(hash_t hash) {
		D_ASSERT((hash & MASK) >> SHIFT < NUM_PARTITIONS);
		return (hash & MASK) >> SHIFT;
	}
};

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE RadixBitsSwitch(idx_t radix_bits, ARGS &&... args) {
	D_ASSERT(radix_bits <= RadixPartitioning::MAX_RADIX_BITS);
	switch (radix_bits) {
	case 0:
		return OP::template Operation<0>(std::forward<ARGS>(args)...);
	case 1:
		return OP::template Operation<1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<4>(std::forward<ARGS>(args)...);
	case 5: // LCOV_EXCL_START
		return OP::template Operation<5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	case 11:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	case 12:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException(
		    "radix_bits higher than RadixPartitioning::MAX_RADIX_BITS encountered in RadixBitsSwitch");
	} // LCOV_EXCL_STOP
}

template <idx_t radix_bits>
struct RadixLessThan {
	static inline bool Operation(hash_t hash, hash_t cutoff) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;
		return CONSTANTS::ApplyMask(hash) < cutoff;
	}
};

struct SelectFunctor {
	template <idx_t radix_bits>
	static idx_t Operation(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t cutoff,
	                       SelectionVector *true_sel, SelectionVector *false_sel) {
		Vector cutoff_vector(Value::HASH(cutoff));
		return BinaryExecutor::Select<hash_t, hash_t, RadixLessThan<radix_bits>>(hashes, cutoff_vector, sel, count,
		                                                                         true_sel, false_sel);
	}
};

idx_t RadixPartitioning::Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return RadixBitsSwitch<SelectFunctor, idx_t>(radix_bits, hashes, sel, count, cutoff, true_sel, false_sel);
}

struct ComputePartitionIndicesFunctor {
	template <idx_t radix_bits>
	static void Operation(Vector &hashes, Vector &partition_indices, idx_t count) {
		UnaryExecutor::Execute<hash_t, hash_t>(hashes, partition_indices, count, [&](hash_t hash) {
			using CONSTANTS = RadixPartitioningConstants<radix_bits>;
			return CONSTANTS::ApplyMask(hash);
		});
	}
};

//===--------------------------------------------------------------------===//
// Column Data Partitioning
//===--------------------------------------------------------------------===//
RadixPartitionedColumnData::RadixPartitionedColumnData(ClientContext &context_p, vector<LogicalType> types_p,
                                                       idx_t radix_bits_p, idx_t hash_col_idx_p)
    : PartitionedColumnData(PartitionedColumnDataType::RADIX, context_p, std::move(types_p)), radix_bits(radix_bits_p),
      hash_col_idx(hash_col_idx_p) {
	D_ASSERT(radix_bits <= RadixPartitioning::MAX_RADIX_BITS);
	D_ASSERT(hash_col_idx < types.size());
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	allocators->allocators.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		CreateAllocator();
	}
	D_ASSERT(allocators->allocators.size() == num_partitions);
}

RadixPartitionedColumnData::RadixPartitionedColumnData(const RadixPartitionedColumnData &other)
    : PartitionedColumnData(other), radix_bits(other.radix_bits), hash_col_idx(other.hash_col_idx) {
	for (idx_t i = 0; i < RadixPartitioning::NumberOfPartitions(radix_bits); i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
	}
}

RadixPartitionedColumnData::~RadixPartitionedColumnData() {
}

void RadixPartitionedColumnData::InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	state.partition_append_states.reserve(num_partitions);
	state.partition_buffers.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		state.partition_append_states.emplace_back(make_uniq<ColumnDataAppendState>());
		partitions[i]->InitializeAppend(*state.partition_append_states[i]);
		state.partition_buffers.emplace_back(CreatePartitionBuffer());
	}
}

void RadixPartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
	D_ASSERT(partitions.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	D_ASSERT(state.partition_buffers.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, input.data[hash_col_idx], state.partition_indices,
	                                                      input.size());
}

//===--------------------------------------------------------------------===//
// Tuple Data Partitioning
//===--------------------------------------------------------------------===//
RadixPartitionedTupleData::RadixPartitionedTupleData(BufferManager &buffer_manager, const TupleDataLayout &layout_p,
                                                     idx_t radix_bits_p, idx_t hash_col_idx_p)
    : PartitionedTupleData(PartitionedTupleDataType::RADIX, buffer_manager, layout_p.Copy()), radix_bits(radix_bits_p),
      hash_col_idx(hash_col_idx_p) {
	D_ASSERT(radix_bits <= RadixPartitioning::MAX_RADIX_BITS);
	D_ASSERT(hash_col_idx < layout.GetTypes().size());
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	allocators->allocators.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		CreateAllocator();
	}
	D_ASSERT(allocators->allocators.size() == num_partitions);
	Initialize();
}

RadixPartitionedTupleData::RadixPartitionedTupleData(const RadixPartitionedTupleData &other)
    : PartitionedTupleData(other), radix_bits(other.radix_bits), hash_col_idx(other.hash_col_idx) {
	Initialize();
}

RadixPartitionedTupleData::~RadixPartitionedTupleData() {
}

void RadixPartitionedTupleData::Initialize() {
	for (idx_t i = 0; i < RadixPartitioning::NumberOfPartitions(radix_bits); i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
	}
}

void RadixPartitionedTupleData::InitializeAppendStateInternal(PartitionedTupleDataAppendState &state,
                                                              TupleDataPinProperties properties) const {
	// Init pin state per partition
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	state.partition_pin_states.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		state.partition_pin_states.emplace_back(make_uniq<TupleDataPinState>());
		partitions[i]->InitializeAppend(*state.partition_pin_states[i], properties);
	}

	// Init single chunk state
	auto column_count = layout.ColumnCount();
	vector<column_t> column_ids;
	column_ids.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	partitions[0]->InitializeChunkState(state.chunk_state, std::move(column_ids));

	// Initialize fixed-size map
	state.fixed_partition_entries.resize(RadixPartitioning::NumberOfPartitions(radix_bits));
}

void RadixPartitionedTupleData::ComputePartitionIndices(PartitionedTupleDataAppendState &state, DataChunk &input) {
	D_ASSERT(partitions.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, input.data[hash_col_idx], state.partition_indices,
	                                                      input.size());
}

void RadixPartitionedTupleData::ComputePartitionIndices(Vector &row_locations, idx_t count,
                                                        Vector &partition_indices) const {
	Vector intermediate(LogicalType::HASH);
	partitions[0]->Gather(row_locations, *FlatVector::IncrementalSelectionVector(), count, hash_col_idx, intermediate,
	                      *FlatVector::IncrementalSelectionVector());
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, intermediate, partition_indices, count);
}

void RadixPartitionedTupleData::RepartitionFinalizeStates(PartitionedTupleData &old_partitioned_data,
                                                          PartitionedTupleData &new_partitioned_data,
                                                          PartitionedTupleDataAppendState &state,
                                                          idx_t finished_partition_idx) const {
	D_ASSERT(old_partitioned_data.GetType() == PartitionedTupleDataType::RADIX &&
	         new_partitioned_data.GetType() == PartitionedTupleDataType::RADIX);
	const auto &old_radix_partitions = old_partitioned_data.Cast<RadixPartitionedTupleData>();
	const auto &new_radix_partitions = new_partitioned_data.Cast<RadixPartitionedTupleData>();
	const auto old_radix_bits = old_radix_partitions.GetRadixBits();
	const auto new_radix_bits = new_radix_partitions.GetRadixBits();
	D_ASSERT(new_radix_bits > old_radix_bits);

	// We take the most significant digits as the partition index
	// When repartitioning, e.g., partition 0 from "old" goes into the first N partitions in "new"
	// When partition 0 is done, we can already finalize the append states, unpinning blocks
	const auto multiplier = RadixPartitioning::NumberOfPartitions(new_radix_bits - old_radix_bits);
	const auto from_idx = finished_partition_idx * multiplier;
	const auto to_idx = from_idx + multiplier;
	auto &partitions = new_partitioned_data.GetPartitions();
	for (idx_t partition_index = from_idx; partition_index < to_idx; partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

} // namespace duckdb


#include <random>

namespace duckdb {

struct RandomState {
	RandomState() {
	}

	pcg32 pcg;
};

RandomEngine::RandomEngine(int64_t seed) : random_state(make_uniq<RandomState>()) {
	if (seed < 0) {
		random_state->pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
	} else {
		random_state->pcg.seed(seed);
	}
}

RandomEngine::~RandomEngine() {
}

double RandomEngine::NextRandom(double min, double max) {
	D_ASSERT(max >= min);
	return min + (NextRandom() * (max - min));
}

double RandomEngine::NextRandom() {
	return std::ldexp(random_state->pcg(), -32);
}
uint32_t RandomEngine::NextRandomInteger() {
	return random_state->pcg();
}

void RandomEngine::SetSeed(uint32_t seed) {
	random_state->pcg.seed(seed);
}

} // namespace duckdb

#include <memory>




namespace duckdb_re2 {

Regex::Regex(const std::string &pattern, RegexOptions options) {
	RE2::Options o;
	o.set_case_sensitive(options == RegexOptions::CASE_INSENSITIVE);
	regex = std::make_shared<duckdb_re2::RE2>(StringPiece(pattern), o);
}

bool RegexSearchInternal(const char *input, Match &match, const Regex &r, RE2::Anchor anchor, size_t start,
                         size_t end) {
	auto &regex = r.GetRegex();
	duckdb::vector<StringPiece> target_groups;
	auto group_count = regex.NumberOfCapturingGroups() + 1;
	target_groups.resize(group_count);
	match.groups.clear();
	if (!regex.Match(StringPiece(input), start, end, anchor, target_groups.data(), group_count)) {
		return false;
	}
	for (auto &group : target_groups) {
		GroupMatch group_match;
		group_match.text = group.ToString();
		group_match.position = group.data() - input;
		match.groups.emplace_back(group_match);
	}
	return true;
}

bool RegexSearch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, 0, input.size());
}

bool RegexMatch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex) {
	return RegexSearchInternal(start, match, regex, RE2::ANCHOR_BOTH, 0, end - start);
}

bool RegexMatch(const std::string &input, const Regex &regex) {
	Match nop_match;
	return RegexSearchInternal(input.c_str(), nop_match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

duckdb::vector<Match> RegexFindAll(const std::string &input, const Regex &regex) {
	duckdb::vector<Match> matches;
	size_t position = 0;
	Match match;
	while (RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, position, input.size())) {
		position += match.position(0) + match.length(0);
		matches.emplace_back(match);
	}
	return matches;
}

} // namespace duckdb_re2
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_aggregate.cpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

void RowOperations::InitializeStates(TupleDataLayout &layout, Vector &addresses, const SelectionVector &sel,
                                     idx_t count) {
	if (count == 0) {
		return;
	}
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto &offsets = layout.GetOffsets();
	auto aggr_idx = layout.ColumnCount();

	for (const auto &aggr : layout.GetAggregates()) {
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = sel.get_index(i);
			auto row = pointers[row_idx];
			aggr.function.initialize(row + offsets[aggr_idx]);
		}
		++aggr_idx;
	}
}

void RowOperations::DestroyStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses, idx_t count) {
	if (count == 0) {
		return;
	}
	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), count);
	for (const auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
			aggr.function.destructor(addresses, aggr_input_data, count);
		}
		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses, aggr.payload_size, count);
	}
}

void RowOperations::UpdateStates(RowOperationsState &state, AggregateObject &aggr, Vector &addresses,
                                 DataChunk &payload, idx_t arg_idx, idx_t count) {
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
	aggr.function.update(aggr.child_count == 0 ? nullptr : &payload.data[arg_idx], aggr_input_data, aggr.child_count,
	                     addresses, count);
}

void RowOperations::UpdateFilteredStates(RowOperationsState &state, AggregateFilterData &filter_data,
                                         AggregateObject &aggr, Vector &addresses, DataChunk &payload, idx_t arg_idx) {
	idx_t count = filter_data.ApplyFilter(payload);
	if (count == 0) {
		return;
	}

	Vector filtered_addresses(addresses, filter_data.true_sel, count);
	filtered_addresses.Flatten(count);

	UpdateStates(state, aggr, filtered_addresses, filter_data.filtered_payload, arg_idx, count);
}

void RowOperations::CombineStates(RowOperationsState &state, TupleDataLayout &layout, Vector &sources, Vector &targets,
                                  idx_t count) {
	if (count == 0) {
		return;
	}

	//	Move to the first aggregate states
	VectorOperations::AddInPlace(sources, layout.GetAggrOffset(), count);
	VectorOperations::AddInPlace(targets, layout.GetAggrOffset(), count);

	// Keep track of the offset
	idx_t offset = layout.GetAggrOffset();

	for (auto &aggr : layout.GetAggregates()) {
		D_ASSERT(aggr.function.combine);
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
		aggr.function.combine(sources, targets, aggr_input_data, count);

		// Move to the next aggregate states
		VectorOperations::AddInPlace(sources, aggr.payload_size, count);
		VectorOperations::AddInPlace(targets, aggr.payload_size, count);

		// Increment the offset
		offset += aggr.payload_size;
	}

	// Now subtract the offset to get back to the original position
	VectorOperations::AddInPlace(sources, -offset, count);
	VectorOperations::AddInPlace(targets, -offset, count);
}

void RowOperations::FinalizeStates(RowOperationsState &state, TupleDataLayout &layout, Vector &addresses,
                                   DataChunk &result, idx_t aggr_idx) {
	// Copy the addresses
	Vector addresses_copy(LogicalType::POINTER);
	VectorOperations::Copy(addresses, addresses_copy, result.size(), 0, 0);

	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses_copy, layout.GetAggrOffset(), result.size());

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &target = result.data[aggr_idx + i];
		auto &aggr = aggregates[i];
		AggregateInputData aggr_input_data(aggr.GetFunctionData(), state.allocator);
		aggr.function.finalize(addresses_copy, aggr_input_data, target, result.size(), 0);

		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses_copy, aggr.payload_size, result.size());
	}
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_external.cpp
//
//
//===----------------------------------------------------------------------===//



namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

void RowOperations::SwizzleColumns(const RowLayout &layout, const data_ptr_t base_row_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Load heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = Load<data_ptr_t>(heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + string_t::HEADER_SIZE;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string pointer with the within-row offset (if not inlined)
						Store<idx_t>(Load<data_ptr_t>(string_ptr) - heap_row_ptrs[i], string_ptr);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data pointer with the within-row offset
					Store<idx_t>(Load<data_ptr_t>(col_ptr) - heap_row_ptrs[i], col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

void RowOperations::SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
                                       const idx_t count, const idx_t base_offset) {
	const idx_t row_width = layout.GetRowWidth();
	row_ptr += layout.GetHeapOffset();
	idx_t cumulative_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(base_offset + cumulative_offset, row_ptr);
		cumulative_offset += Load<uint32_t>(heap_base_ptr + cumulative_offset);
		row_ptr += row_width;
	}
}

void RowOperations::CopyHeapAndSwizzle(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
                                       data_ptr_t heap_ptr, const idx_t count) {
	const auto row_width = layout.GetRowWidth();
	const auto heap_offset = layout.GetHeapOffset();
	for (idx_t i = 0; i < count; i++) {
		// Figure out source and size
		const auto source_heap_ptr = Load<data_ptr_t>(row_ptr + heap_offset);
		const auto size = Load<uint32_t>(source_heap_ptr);
		D_ASSERT(size >= sizeof(uint32_t));

		// Copy and swizzle
		memcpy(heap_ptr, source_heap_ptr, size);
		Store<idx_t>(heap_ptr - heap_base_ptr, row_ptr + heap_offset);

		// Increment for next iteration
		row_ptr += row_width;
		heap_ptr += size;
	}
}

void RowOperations::UnswizzleHeapPointer(const RowLayout &layout, const data_ptr_t base_row_ptr,
                                         const data_ptr_t base_heap_ptr, const idx_t count) {
	const auto row_width = layout.GetRowWidth();
	data_ptr_t heap_ptr_ptr = base_row_ptr + layout.GetHeapOffset();
	for (idx_t i = 0; i < count; i++) {
		Store<data_ptr_t>(base_heap_ptr + Load<idx_t>(heap_ptr_ptr), heap_ptr_ptr);
		heap_ptr_ptr += row_width;
	}
}

static inline void VerifyUnswizzledString(const RowLayout &layout, const idx_t &col_idx, const data_ptr_t &row_ptr) {
#ifdef DEBUG
	if (layout.GetTypes()[col_idx].id() != LogicalTypeId::VARCHAR) {
		return;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	ValidityBytes row_mask(row_ptr);
	if (row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
		auto str = Load<string_t>(row_ptr + layout.GetOffsets()[col_idx]);
		str.Verify();
	}
#endif
}

void RowOperations::UnswizzlePointers(const RowLayout &layout, const data_ptr_t base_row_ptr,
                                      const data_ptr_t base_heap_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Restore heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			Store<data_ptr_t>(heap_row_ptrs[i], heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + string_t::HEADER_SIZE;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string offset with the pointer (if not inlined)
						Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(string_ptr), string_ptr);
						VerifyUnswizzledString(layout, col_idx, row_ptr + i * row_width);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data offset with the pointer
					Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(col_ptr), col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// row_gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//








namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedGatherLoop(Vector &rows, const SelectionVector &row_sel, Vector &col,
                                const SelectionVector &col_sel, idx_t count, const RowLayout &layout, idx_t col_no,
                                idx_t build_size) {
	// Precompute mask indexes
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		data[col_idx] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		}
	}
}

static void GatherVarchar(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                          idx_t count, const RowLayout &layout, idx_t col_no, idx_t build_size,
                          data_ptr_t base_heap_ptr) {
	// Precompute mask indexes
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	const auto heap_offset = layout.GetHeapOffset();
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<string_t>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		auto col_ptr = row + col_offset;
		data[col_idx] = Load<string_t>(col_ptr);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		} else if (base_heap_ptr && Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
			//	Not inline, so unswizzle the copied pointer the pointer
			auto heap_ptr_ptr = row + heap_offset;
			auto heap_row_ptr = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			auto string_ptr = data_ptr_t(data + col_idx) + string_t::HEADER_SIZE;
			Store<data_ptr_t>(heap_row_ptr + Load<idx_t>(string_ptr), string_ptr);
#ifdef DEBUG
			data[col_idx].Verify();
#endif
		}
	}
}

static void GatherNestedVector(Vector &rows, const SelectionVector &row_sel, Vector &col,
                               const SelectionVector &col_sel, idx_t count, const RowLayout &layout, idx_t col_no,
                               data_ptr_t base_heap_ptr) {
	const auto &offsets = layout.GetOffsets();
	const auto col_offset = offsets[col_no];
	const auto heap_offset = layout.GetHeapOffset();
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	// Build the gather locations
	auto data_locations = make_unsafe_uniq_array<data_ptr_t>(count);
	auto mask_locations = make_unsafe_uniq_array<data_ptr_t>(count);
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		mask_locations[i] = row;
		auto col_ptr = ptrs[row_idx] + col_offset;
		if (base_heap_ptr) {
			auto heap_ptr_ptr = row + heap_offset;
			auto heap_row_ptr = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			data_locations[i] = heap_row_ptr + Load<idx_t>(col_ptr);
		} else {
			data_locations[i] = Load<data_ptr_t>(col_ptr);
		}
	}

	// Deserialise into the selected locations
	RowOperations::HeapGather(col, count, col_sel, col_no, data_locations.get(), mask_locations.get());
}

void RowOperations::Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                           const idx_t count, const RowLayout &layout, const idx_t col_no, const idx_t build_size,
                           data_ptr_t heap_ptr) {
	D_ASSERT(rows.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(rows.GetType().id() == LogicalTypeId::POINTER); // "Cannot gather from non-pointer type!"

	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedGatherLoop<uint8_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherLoop<uint16_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherLoop<uint32_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherLoop<uint64_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherLoop<int8_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT16:
		TemplatedGatherLoop<int16_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT32:
		TemplatedGatherLoop<int32_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT64:
		TemplatedGatherLoop<int64_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INT128:
		TemplatedGatherLoop<hugeint_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherLoop<float>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherLoop<double>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherLoop<interval_t>(rows, row_sel, col, col_sel, count, layout, col_no, build_size);
		break;
	case PhysicalType::VARCHAR:
		GatherVarchar(rows, row_sel, col, col_sel, count, layout, col_no, build_size, heap_ptr);
		break;
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
		GatherNestedVector(rows, row_sel, col, col_sel, count, layout, col_no, heap_ptr);
		break;
	default:
		throw InternalException("Unimplemented type for RowOperations::Gather");
	}
}

template <class T>
static void TemplatedFullScanLoop(Vector &rows, Vector &col, idx_t count, idx_t col_offset, idx_t col_no) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	//	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row = ptrs[i];
		data[i] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			throw InternalException("Null value comparisons not implemented for perfect hash table yet");
			//			col_mask.SetInvalid(i);
		}
	}
}

void RowOperations::FullScanColumn(const TupleDataLayout &layout, Vector &rows, Vector &col, idx_t count,
                                   idx_t col_no) {
	const auto col_offset = layout.GetOffsets()[col_no];
	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedFullScanLoop<uint8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT16:
		TemplatedFullScanLoop<uint16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT32:
		TemplatedFullScanLoop<uint32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT64:
		TemplatedFullScanLoop<uint64_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT8:
		TemplatedFullScanLoop<int8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT16:
		TemplatedFullScanLoop<int16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT32:
		TemplatedFullScanLoop<int32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT64:
		TemplatedFullScanLoop<int64_t>(rows, col, count, col_offset, col_no);
		break;
	default:
		throw NotImplementedException("Unimplemented type for RowOperations::FullScanColumn");
	}
}

} // namespace duckdb




namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

template <class T>
static void TemplatedHeapGather(Vector &v, const idx_t count, const SelectionVector &sel, data_ptr_t *key_locations) {
	auto target = FlatVector::GetData<T>(v);

	for (idx_t i = 0; i < count; ++i) {
		const auto col_idx = sel.get_index(i);
		target[col_idx] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
	}
}

static void HeapGatherStringVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);
	auto target = FlatVector::GetData<string_t>(v);

	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		auto len = Load<uint32_t>(key_locations[i]);
		key_locations[i] += sizeof(uint32_t);
		target[col_idx] = StringVector::AddStringOrBlob(v, string_t(const_char_ptr_cast(key_locations[i]), len));
		key_locations[i] += len;
	}
}

static void HeapGatherStructVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	// struct must have a validitymask for its fields
	auto &child_types = StructType::GetChildTypes(v.GetType());
	const idx_t struct_validitymask_size = (child_types.size() + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < vcount; i++) {
		// use key_locations as the validitymask, and create struct_key_locations
		struct_validitymask_locations[i] = key_locations[i];
		key_locations[i] += struct_validitymask_size;
	}

	// now deserialize into the struct vectors
	auto &children = StructVector::GetEntries(v);
	for (idx_t i = 0; i < child_types.size(); i++) {
		RowOperations::HeapGather(*children[i], vcount, sel, i, key_locations, struct_validitymask_locations);
	}
}

static void HeapGatherListVector(Vector &v, const idx_t vcount, const SelectionVector &sel, data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);

	auto child_type = ListType::GetChildType(v.GetType());
	auto list_data = ListVector::GetData(v);
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	uint64_t entry_offset = ListVector::GetListSize(v);
	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		// read list length
		auto entry_remaining = Load<uint64_t>(key_locations[i]);
		key_locations[i] += sizeof(uint64_t);
		// set list entry attributes
		list_data[col_idx].length = entry_remaining;
		list_data[col_idx].offset = entry_offset;
		// skip over the validity mask
		data_ptr_t validitymask_location = key_locations[i];
		idx_t offset_in_byte = 0;
		key_locations[i] += (entry_remaining + 7) / 8;
		// entry sizes
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type.InternalType())) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += entry_remaining * sizeof(idx_t);
		}

		// now read the list data
		while (entry_remaining > 0) {
			auto next = MinValue(entry_remaining, (idx_t)STANDARD_VECTOR_SIZE);

			// initialize a new vector to append
			Vector append_vector(v.GetType());
			append_vector.SetVectorType(v.GetVectorType());

			auto &list_vec_to_append = ListVector::GetEntry(append_vector);

			// set validity
			//! Since we are constructing the vector, this will always be a flat vector.
			auto &append_validity = FlatVector::Validity(list_vec_to_append);
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				append_validity.Set(entry_idx, *(validitymask_location) & (1 << offset_in_byte));
				if (++offset_in_byte == 8) {
					validitymask_location++;
					offset_in_byte = 0;
				}
			}

			// compute entry sizes and set locations where the list entries are
			if (TypeIsConstantSize(child_type.InternalType())) {
				// constant size list entries
				const idx_t type_size = GetTypeIdSize(child_type.InternalType());
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += Load<idx_t>(var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now deserialize and add to listvector
			RowOperations::HeapGather(list_vec_to_append, next, *FlatVector::IncrementalSelectionVector(), 0,
			                          list_entry_locations, nullptr);
			ListVector::Append(v, list_vec_to_append, next);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

void RowOperations::HeapGather(Vector &v, const idx_t &vcount, const SelectionVector &sel, const idx_t &col_no,
                               data_ptr_t *key_locations, data_ptr_t *validitymask_locations) {
	v.SetVectorType(VectorType::FLAT_VECTOR);

	auto &validity = FlatVector::Validity(v);
	if (validitymask_locations) {
		// Precompute mask indexes
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

		for (idx_t i = 0; i < vcount; i++) {
			ValidityBytes row_mask(validitymask_locations[i]);
			const auto valid = row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);
			const auto col_idx = sel.get_index(i);
			validity.Set(col_idx, valid);
		}
	}

	auto type = v.GetType().InternalType();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapGather<int8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT16:
		TemplatedHeapGather<int16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT32:
		TemplatedHeapGather<int32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT64:
		TemplatedHeapGather<int64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapGather<uint8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapGather<uint16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapGather<uint32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapGather<uint64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT128:
		TemplatedHeapGather<hugeint_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapGather<float>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapGather<double>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapGather<interval_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::VARCHAR:
		HeapGatherStringVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::STRUCT:
		HeapGatherStructVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::LIST:
		HeapGatherListVector(v, vcount, sel, key_locations);
		break;
	default:
		throw NotImplementedException("Unimplemented deserialize from row-format");
	}
}

} // namespace duckdb




namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

static void ComputeStringEntrySizes(UnifiedVectorFormat &vdata, idx_t entry_sizes[], const idx_t ser_count,
                                    const SelectionVector &sel, const idx_t offset) {
	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto str_idx = vdata.sel->get_index(idx + offset);
		if (vdata.validity.RowIsValid(str_idx)) {
			entry_sizes[i] += sizeof(uint32_t) + strings[str_idx].GetSize();
		}
	}
}

static void ComputeStructEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                    const SelectionVector &sel, idx_t offset) {
	// obtain child vectors
	idx_t num_children;
	auto &children = StructVector::GetEntries(v);
	num_children = children.size();
	// add struct validitymask size
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	for (idx_t i = 0; i < ser_count; i++) {
		entry_sizes[i] += struct_validitymask_size;
	}
	// compute size of child vectors
	for (auto &struct_vector : children) {
		RowOperations::ComputeEntrySizes(*struct_vector, entry_sizes, vcount, ser_count, sel, offset);
	}
}

static void ComputeListEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t ser_count,
                                  const SelectionVector &sel, idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx + offset);
		if (vdata.validity.RowIsValid(source_idx)) {
			auto list_entry = list_data[source_idx];

			// make room for list length, list validitymask
			entry_sizes[i] += sizeof(list_entry.length);
			entry_sizes[i] += (list_entry.length + 7) / 8;

			// serialize size of each entry (if non-constant size)
			if (!TypeIsConstantSize(ListType::GetChildType(v.GetType()).InternalType())) {
				entry_sizes[i] += list_entry.length * sizeof(list_entry.length);
			}

			// compute size of each the elements in list_entry and sum them
			auto entry_remaining = list_entry.length;
			auto entry_offset = list_entry.offset;
			while (entry_remaining > 0) {
				// the list entry can span multiple vectors
				auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

				// compute and add to the total
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t list_idx = 0; list_idx < next; list_idx++) {
					entry_sizes[i] += list_entry_sizes[list_idx];
				}

				// update for next iteration
				entry_remaining -= next;
				entry_offset += next;
			}
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t vcount,
                                      idx_t ser_count, const SelectionVector &sel, idx_t offset) {
	const auto physical_type = v.GetType().InternalType();
	if (TypeIsConstantSize(physical_type)) {
		const auto type_size = GetTypeIdSize(physical_type);
		for (idx_t i = 0; i < ser_count; i++) {
			entry_sizes[i] += type_size;
		}
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR:
			ComputeStringEntrySizes(vdata, entry_sizes, ser_count, sel, offset);
			break;
		case PhysicalType::STRUCT:
			ComputeStructEntrySizes(v, entry_sizes, vcount, ser_count, sel, offset);
			break;
		case PhysicalType::LIST:
			ComputeListEntrySizes(v, vdata, entry_sizes, ser_count, sel, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Column with variable size type %s cannot be serialized to row-format",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                      const SelectionVector &sel, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);
	ComputeEntrySizes(v, vdata, entry_sizes, vcount, ser_count, sel, offset);
}

template <class T>
static void TemplatedHeapScatter(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count, idx_t col_idx,
                                 data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<T>(vdata);
	if (!validitymask_locations) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], data_ptr_cast(target));
			key_locations[i] += sizeof(T);
		}
	} else {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
		const auto bit = ~(1UL << idx_in_entry);
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], data_ptr_cast(target));
			key_locations[i] += sizeof(T);

			// set the validitymask
			if (!vdata.validity.RowIsValid(source_idx)) {
				*(validitymask_locations[i] + entry_idx) &= bit;
			}
		}
	}
}

static void HeapScatterStringVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	if (!validitymask_locations) {
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(string_entry.GetSize(), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetData(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			}
		}
	} else {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
		const auto bit = ~(1UL << idx_in_entry);
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(string_entry.GetSize(), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetData(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			} else {
				// set the validitymask
				*(validitymask_locations[i] + entry_idx) &= bit;
			}
		}
	}
}

static void HeapScatterStructVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	auto &children = StructVector::GetEntries(v);
	idx_t num_children = children.size();

	// the whole struct itself can be NULL
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	const auto bit = ~(1UL << idx_in_entry);

	// struct must have a validitymask for its fields
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		// initialize the struct validity mask
		struct_validitymask_locations[i] = key_locations[i];
		memset(struct_validitymask_locations[i], -1, struct_validitymask_size);
		key_locations[i] += struct_validitymask_size;

		// set whether the whole struct is null
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx) + offset;
		if (validitymask_locations && !vdata.validity.RowIsValid(source_idx)) {
			*(validitymask_locations[i] + entry_idx) &= bit;
		}
	}

	// now serialize the struct vectors
	for (idx_t i = 0; i < children.size(); i++) {
		auto &struct_vector = *children[i];
		RowOperations::HeapScatter(struct_vector, vcount, sel, ser_count, i, key_locations,
		                           struct_validitymask_locations, offset);
	}
}

static void HeapScatterListVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_no,
                                  data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto list_data = ListVector::GetData(v);

	auto &child_vector = ListVector::GetEntry(v);

	UnifiedVectorFormat list_vdata;
	child_vector.ToUnifiedFormat(ListVector::GetListSize(v), list_vdata);
	auto child_type = ListType::GetChildType(v.GetType()).InternalType();

	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx + offset);
		if (!vdata.validity.RowIsValid(source_idx)) {
			if (validitymask_locations) {
				// set the row validitymask for this column to invalid
				ValidityBytes row_mask(validitymask_locations[i]);
				row_mask.SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
			continue;
		}
		auto list_entry = list_data[source_idx];

		// store list length
		Store<uint64_t>(list_entry.length, key_locations[i]);
		key_locations[i] += sizeof(list_entry.length);

		// make room for the validitymask
		data_ptr_t list_validitymask_location = key_locations[i];
		idx_t entry_offset_in_byte = 0;
		idx_t validitymask_size = (list_entry.length + 7) / 8;
		memset(list_validitymask_location, -1, validitymask_size);
		key_locations[i] += validitymask_size;

		// serialize size of each entry (if non-constant size)
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type)) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += list_entry.length * sizeof(idx_t);
		}

		auto entry_remaining = list_entry.length;
		auto entry_offset = list_entry.offset;
		while (entry_remaining > 0) {
			// the list entry can span multiple vectors
			auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

			// serialize list validity
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				auto list_idx = list_vdata.sel->get_index(entry_idx + entry_offset);
				if (!list_vdata.validity.RowIsValid(list_idx)) {
					*(list_validitymask_location) &= ~(1UL << entry_offset_in_byte);
				}
				if (++entry_offset_in_byte == 8) {
					list_validitymask_location++;
					entry_offset_in_byte = 0;
				}
			}

			if (TypeIsConstantSize(child_type)) {
				// constant size list entries: set list entry locations
				const idx_t type_size = GetTypeIdSize(child_type);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries: compute entry sizes and set list entry locations
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += list_entry_sizes[entry_idx];
					Store<idx_t>(list_entry_sizes[entry_idx], var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now serialize to the locations
			RowOperations::HeapScatter(child_vector, ListVector::GetListSize(v),
			                           *FlatVector::IncrementalSelectionVector(), next, 0, list_entry_locations,
			                           nullptr, entry_offset);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

void RowOperations::HeapScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	if (TypeIsConstantSize(v.GetType().InternalType())) {
		UnifiedVectorFormat vdata;
		v.ToUnifiedFormat(vcount, vdata);
		RowOperations::HeapScatterVData(vdata, v.GetType().InternalType(), sel, ser_count, col_idx, key_locations,
		                                validitymask_locations, offset);
	} else {
		switch (v.GetType().InternalType()) {
		case PhysicalType::VARCHAR:
			HeapScatterStringVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		case PhysicalType::STRUCT:
			HeapScatterStructVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		case PhysicalType::LIST:
			HeapScatterListVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Serialization of variable length vector with type %s",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::HeapScatterVData(UnifiedVectorFormat &vdata, PhysicalType type, const SelectionVector &sel,
                                     idx_t ser_count, idx_t col_idx, data_ptr_t *key_locations,
                                     data_ptr_t *validitymask_locations, idx_t offset) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapScatter<int8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT16:
		TemplatedHeapScatter<int16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT32:
		TemplatedHeapScatter<int32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT64:
		TemplatedHeapScatter<int64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapScatter<uint8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapScatter<uint16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapScatter<uint32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapScatter<uint64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT128:
		TemplatedHeapScatter<hugeint_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapScatter<float>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapScatter<double>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapScatter<interval_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	default:
		throw NotImplementedException("FIXME: Serialize to of constant type column to row-format");
	}
}

} // namespace duckdb






namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

template <bool NO_MATCH_SEL, class T, class OP>
static idx_t TemplatedMatch(Vector &, const TupleDataVectorFormat &lhs_format, SelectionVector &sel, const idx_t count,
                            const TupleDataLayout &rhs_layout, Vector &rhs_row_locations, const idx_t col_idx,
                            const vector<MatchFunction> &, SelectionVector *no_match_sel, idx_t &no_match_count) {
	using COMPARISON_OP = ComparisonOperationWrapper<OP>;

	// LHS
	const auto &lhs_sel = *lhs_format.unified.sel;
	const auto lhs_data = UnifiedVectorFormat::GetData<T>(lhs_format.unified);
	const auto &lhs_validity = lhs_format.unified.validity;

	// RHS
	const auto rhs_locations = FlatVector::GetData<data_ptr_t>(rhs_row_locations);
	const auto rhs_offset_in_row = rhs_layout.GetOffsets()[col_idx];
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	idx_t match_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto idx = sel.get_index(i);

		const auto lhs_idx = lhs_sel.get_index(idx);
		const auto lhs_null = lhs_validity.AllValid() ? false : !lhs_validity.RowIsValid(lhs_idx);

		const auto &rhs_location = rhs_locations[idx];
		const ValidityBytes rhs_mask(rhs_location);
		const auto rhs_null = !rhs_mask.RowIsValid(rhs_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry);

		if (COMPARISON_OP::template Operation<T>(lhs_data[lhs_idx], Load<T>(rhs_location + rhs_offset_in_row), lhs_null,
		                                         rhs_null)) {
			sel.set_index(match_count++, idx);
		} else if (NO_MATCH_SEL) {
			no_match_sel->set_index(no_match_count++, idx);
		}
	}
	return match_count;
}

template <bool NO_MATCH_SEL, class OP>
static idx_t StructMatchEquality(Vector &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                                 const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                                 const idx_t col_idx, const vector<MatchFunction> &child_functions,
                                 SelectionVector *no_match_sel, idx_t &no_match_count) {
	using COMPARISON_OP = ComparisonOperationWrapper<OP>;

	// LHS
	const auto &lhs_sel = *lhs_format.unified.sel;
	const auto &lhs_validity = lhs_format.unified.validity;

	// RHS
	const auto rhs_locations = FlatVector::GetData<data_ptr_t>(rhs_row_locations);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	idx_t match_count = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto idx = sel.get_index(i);

		const auto lhs_idx = lhs_sel.get_index(idx);
		const auto lhs_null = lhs_validity.AllValid() ? false : !lhs_validity.RowIsValid(lhs_idx);

		const auto &rhs_location = rhs_locations[idx];
		const ValidityBytes rhs_mask(rhs_location);
		const auto rhs_null = !rhs_mask.RowIsValid(rhs_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry);

		// For structs there is no value to compare, here we match NULLs and let recursion do the rest
		// So we use the comparison only if rhs or LHS is NULL and COMPARE_NULL is true
		if (!(lhs_null || rhs_null) ||
		    (COMPARISON_OP::COMPARE_NULL && COMPARISON_OP::template Operation<uint32_t>(0, 0, lhs_null, rhs_null))) {
			sel.set_index(match_count++, idx);
		} else if (NO_MATCH_SEL) {
			no_match_sel->set_index(no_match_count++, idx);
		}
	}

	// Create a Vector of pointers to the start of the TupleDataLayout of the STRUCT
	Vector rhs_struct_row_locations(LogicalType::POINTER);
	const auto rhs_offset_in_row = rhs_layout.GetOffsets()[col_idx];
	auto rhs_struct_locations = FlatVector::GetData<data_ptr_t>(rhs_struct_row_locations);
	for (idx_t i = 0; i < match_count; i++) {
		const auto idx = sel.get_index(i);
		rhs_struct_locations[idx] = rhs_locations[idx] + rhs_offset_in_row;
	}

	// Get the struct layout and struct entries
	const auto &rhs_struct_layout = rhs_layout.GetStructLayout(col_idx);
	auto &lhs_struct_vectors = StructVector::GetEntries(lhs_vector);
	D_ASSERT(rhs_struct_layout.ColumnCount() == lhs_struct_vectors.size());

	for (idx_t struct_col_idx = 0; struct_col_idx < rhs_struct_layout.ColumnCount(); struct_col_idx++) {
		auto &lhs_struct_vector = *lhs_struct_vectors[struct_col_idx];
		auto &lhs_struct_format = lhs_format.children[struct_col_idx];
		const auto &child_function = child_functions[struct_col_idx];
		match_count = child_function.function(lhs_struct_vector, lhs_struct_format, sel, match_count, rhs_struct_layout,
		                                      rhs_struct_row_locations, struct_col_idx, child_function.child_functions,
		                                      no_match_sel, no_match_count);
	}

	return match_count;
}

template <typename OP>
static idx_t SelectComparison(Vector &, Vector &, const SelectionVector &, idx_t, SelectionVector *,
                              SelectionVector *) {
	throw NotImplementedException("Unsupported list comparison operand for RowMatcher::GetMatchFunction");
}

template <>
idx_t SelectComparison<Equals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<NotEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                     SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctFrom(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NotDistinctFrom(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <bool NO_MATCH_SEL, class OP>
static idx_t GenericNestedMatch(Vector &lhs_vector, const TupleDataVectorFormat &, SelectionVector &sel,
                                const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                                const idx_t col_idx, const vector<MatchFunction> &, SelectionVector *no_match_sel,
                                idx_t &no_match_count) {
	const auto &type = rhs_layout.GetTypes()[col_idx];

	// Gather a dense Vector containing the column values being matched
	Vector key(type);
	const auto gather_function = TupleDataCollection::GetGatherFunction(type);
	gather_function.function(rhs_layout, rhs_row_locations, col_idx, sel, count, key,
	                         *FlatVector::IncrementalSelectionVector(), key, gather_function.child_functions);

	// Densify the input column
	Vector sliced(lhs_vector, sel, count);

	if (NO_MATCH_SEL) {
		SelectionVector no_match_sel_offset(no_match_sel->data() + no_match_count);
		auto match_count = SelectComparison<OP>(sliced, key, sel, count, &sel, &no_match_sel_offset);
		no_match_count += count - match_count;
		return match_count;
	}
	return SelectComparison<OP>(sliced, key, sel, count, &sel, nullptr);
}

void RowMatcher::Initialize(const bool no_match_sel, const TupleDataLayout &layout, const Predicates &predicates) {
	match_functions.reserve(predicates.size());
	for (idx_t col_idx = 0; col_idx < predicates.size(); col_idx++) {
		match_functions.push_back(GetMatchFunction(no_match_sel, layout.GetTypes()[col_idx], predicates[col_idx]));
	}
}

idx_t RowMatcher::Match(DataChunk &lhs, const vector<TupleDataVectorFormat> &lhs_formats, SelectionVector &sel,
                        idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                        SelectionVector *no_match_sel, idx_t &no_match_count) {
	D_ASSERT(!match_functions.empty());
	for (idx_t col_idx = 0; col_idx < match_functions.size(); col_idx++) {
		const auto &match_function = match_functions[col_idx];
		count =
		    match_function.function(lhs.data[col_idx], lhs_formats[col_idx], sel, count, rhs_layout, rhs_row_locations,
		                            col_idx, match_function.child_functions, no_match_sel, no_match_count);
	}
	return count;
}

MatchFunction RowMatcher::GetMatchFunction(const bool no_match_sel, const LogicalType &type,
                                           const ExpressionType predicate) {
	return no_match_sel ? GetMatchFunction<true>(type, predicate) : GetMatchFunction<false>(type, predicate);
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetMatchFunction(const LogicalType &type, const ExpressionType predicate) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetMatchFunction<NO_MATCH_SEL, bool>(predicate);
	case PhysicalType::INT8:
		return GetMatchFunction<NO_MATCH_SEL, int8_t>(predicate);
	case PhysicalType::INT16:
		return GetMatchFunction<NO_MATCH_SEL, int16_t>(predicate);
	case PhysicalType::INT32:
		return GetMatchFunction<NO_MATCH_SEL, int32_t>(predicate);
	case PhysicalType::INT64:
		return GetMatchFunction<NO_MATCH_SEL, int64_t>(predicate);
	case PhysicalType::INT128:
		return GetMatchFunction<NO_MATCH_SEL, hugeint_t>(predicate);
	case PhysicalType::UINT8:
		return GetMatchFunction<NO_MATCH_SEL, uint8_t>(predicate);
	case PhysicalType::UINT16:
		return GetMatchFunction<NO_MATCH_SEL, uint16_t>(predicate);
	case PhysicalType::UINT32:
		return GetMatchFunction<NO_MATCH_SEL, uint32_t>(predicate);
	case PhysicalType::UINT64:
		return GetMatchFunction<NO_MATCH_SEL, uint64_t>(predicate);
	case PhysicalType::FLOAT:
		return GetMatchFunction<NO_MATCH_SEL, float>(predicate);
	case PhysicalType::DOUBLE:
		return GetMatchFunction<NO_MATCH_SEL, double>(predicate);
	case PhysicalType::INTERVAL:
		return GetMatchFunction<NO_MATCH_SEL, interval_t>(predicate);
	case PhysicalType::VARCHAR:
		return GetMatchFunction<NO_MATCH_SEL, string_t>(predicate);
	case PhysicalType::STRUCT:
		return GetStructMatchFunction<NO_MATCH_SEL>(type, predicate);
	case PhysicalType::LIST:
		return GetListMatchFunction<NO_MATCH_SEL>(predicate);
	default:
		throw InternalException("Unsupported PhysicalType for RowMatcher::GetMatchFunction: %s",
		                        EnumUtil::ToString(type.InternalType()));
	}
}

template <bool NO_MATCH_SEL, class T>
MatchFunction RowMatcher::GetMatchFunction(const ExpressionType predicate) {
	MatchFunction result;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, Equals>;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, NotEquals>;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, DistinctFrom>;
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, NotDistinctFrom>;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, GreaterThan>;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, GreaterThanEquals>;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, LessThan>;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = TemplatedMatch<NO_MATCH_SEL, T, LessThanEquals>;
		break;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}
	return result;
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetStructMatchFunction(const LogicalType &type, const ExpressionType predicate) {
	// We perform equality conditions like it's just a row, but we cannot perform inequality conditions like a row,
	// because for equality conditions we need to always loop through all columns, but for inequality conditions,
	// we need to find the first inequality, so the loop looks very different
	MatchFunction result;
	ExpressionType child_predicate = predicate;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = StructMatchEquality<NO_MATCH_SEL, Equals>;
		child_predicate = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = GenericNestedMatch<NO_MATCH_SEL, NotEquals>;
		return result;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = GenericNestedMatch<NO_MATCH_SEL, DistinctFrom>;
		return result;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = StructMatchEquality<NO_MATCH_SEL, NotDistinctFrom>;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = GenericNestedMatch<NO_MATCH_SEL, GreaterThan>;
		return result;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = GenericNestedMatch<NO_MATCH_SEL, GreaterThanEquals>;
		return result;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = GenericNestedMatch<NO_MATCH_SEL, LessThan>;
		return result;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = GenericNestedMatch<NO_MATCH_SEL, LessThanEquals>;
		return result;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetStructMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}

	result.child_functions.reserve(StructType::GetChildCount(type));
	for (const auto &child_type : StructType::GetChildTypes(type)) {
		result.child_functions.push_back(GetMatchFunction<NO_MATCH_SEL>(child_type.second, child_predicate));
	}

	return result;
}

template <bool NO_MATCH_SEL>
MatchFunction RowMatcher::GetListMatchFunction(const ExpressionType predicate) {
	MatchFunction result;
	switch (predicate) {
	case ExpressionType::COMPARE_EQUAL:
		result.function = GenericNestedMatch<NO_MATCH_SEL, Equals>;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		result.function = GenericNestedMatch<NO_MATCH_SEL, NotEquals>;
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		result.function = GenericNestedMatch<NO_MATCH_SEL, DistinctFrom>;
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		result.function = GenericNestedMatch<NO_MATCH_SEL, NotDistinctFrom>;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		result.function = GenericNestedMatch<NO_MATCH_SEL, GreaterThan>;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		result.function = GenericNestedMatch<NO_MATCH_SEL, GreaterThanEquals>;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		result.function = GenericNestedMatch<NO_MATCH_SEL, LessThan>;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		result.function = GenericNestedMatch<NO_MATCH_SEL, LessThanEquals>;
		break;
	default:
		throw InternalException("Unsupported ExpressionType for RowMatcher::GetListMatchFunction: %s",
		                        EnumUtil::ToString(predicate));
	}
	return result;
}

} // namespace duckdb





namespace duckdb {

template <class T>
void TemplatedRadixScatter(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                           data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                           const idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<T>(vdata);
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeData<T>(key_locations[i] + 1, source[source_idx]);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < sizeof(T) + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', sizeof(T));
			}
			key_locations[i] += sizeof(T) + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeData<T>(key_locations[i], source[source_idx]);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < sizeof(T); s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += sizeof(T);
		}
	}
}

void RadixScatterStringVector(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                              data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                              const idx_t prefix_len, idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<string_t>(vdata);
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeStringDataPrefix(key_locations[i] + 1, source[source_idx], prefix_len);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < prefix_len + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', prefix_len);
			}
			key_locations[i] += prefix_len + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeStringDataPrefix(key_locations[i], source[source_idx], prefix_len);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < prefix_len; s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += prefix_len;
		}
	}
}

void RadixScatterListVector(Vector &v, UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                            data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                            const idx_t prefix_len, const idx_t width, const idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	auto list_size = ListVector::GetListSize(v);
	child_vector.Flatten(list_size);

	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			data_ptr_t key_location = key_locations[i] + 1;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				key_locations[i]++;
				auto &list_entry = list_data[source_idx];
				if (list_entry.length > 0) {
					// denote that the list is not empty with a 1
					key_locations[i][0] = 1;
					key_locations[i]++;
					RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
					                            key_locations + i, false, true, false, prefix_len, width - 1,
					                            list_entry.offset);
				} else {
					// denote that the list is empty with a 0
					key_locations[i][0] = 0;
					key_locations[i]++;
					memset(key_locations[i], '\0', width - 2);
				}
				// invert bits if desc
				if (desc) {
					for (idx_t s = 0; s < width - 1; s++) {
						*(key_location + s) = ~*(key_location + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', width - 1);
				key_locations[i] += width;
			}
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			auto &list_entry = list_data[source_idx];
			data_ptr_t key_location = key_locations[i];
			if (list_entry.length > 0) {
				// denote that the list is not empty with a 1
				key_locations[i][0] = 1;
				key_locations[i]++;
				RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
				                            key_locations + i, false, true, false, prefix_len, width - 1,
				                            list_entry.offset);
			} else {
				// denote that the list is empty with a 0
				key_locations[i][0] = 0;
				key_locations[i]++;
				memset(key_locations[i], '\0', width - 1);
			}
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < width; s++) {
					*(key_location + s) = ~*(key_location + s);
				}
			}
		}
	}
}

void RadixScatterStructVector(Vector &v, UnifiedVectorFormat &vdata, idx_t vcount, const SelectionVector &sel,
                              idx_t add_count, data_ptr_t *key_locations, const bool desc, const bool has_null,
                              const bool nulls_first, const idx_t prefix_len, idx_t width, const idx_t offset) {
	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
			} else {
				key_locations[i][0] = invalid;
			}
			key_locations[i]++;
		}
		width--;
	}
	// serialize the struct
	auto &child_vector = *StructVector::GetEntries(v)[0];
	RowOperations::RadixScatter(child_vector, vcount, *FlatVector::IncrementalSelectionVector(), add_count,
	                            key_locations, false, true, false, prefix_len, width, offset);
	// invert bits if desc
	if (desc) {
		for (idx_t i = 0; i < add_count; i++) {
			for (idx_t s = 0; s < width; s++) {
				*(key_locations[i] - width + s) = ~*(key_locations[i] - width + s);
			}
		}
	}
}

void RowOperations::RadixScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                 data_ptr_t *key_locations, bool desc, bool has_null, bool nulls_first,
                                 idx_t prefix_len, idx_t width, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);
	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedRadixScatter<int8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT16:
		TemplatedRadixScatter<int16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT32:
		TemplatedRadixScatter<int32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT64:
		TemplatedRadixScatter<int64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedRadixScatter<uint8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedRadixScatter<uint16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedRadixScatter<uint32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedRadixScatter<uint64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT128:
		TemplatedRadixScatter<hugeint_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedRadixScatter<float>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedRadixScatter<double>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedRadixScatter<interval_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::VARCHAR:
		RadixScatterStringVector(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, offset);
		break;
	case PhysicalType::LIST:
		RadixScatterListVector(v, vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, width,
		                       offset);
		break;
	case PhysicalType::STRUCT:
		RadixScatterStructVector(v, vdata, vcount, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                         prefix_len, width, offset);
		break;
	default:
		throw NotImplementedException("Cannot ORDER BY column with type %s", v.GetType().ToString());
	}
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// row_scatter.cpp
// Description: This file contains the implementation of the row scattering
//              operators
//===--------------------------------------------------------------------===//










namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedScatter(UnifiedVectorFormat &col, Vector &rows, const SelectionVector &sel, const idx_t count,
                             const idx_t col_offset, const idx_t col_no) {
	auto data = UnifiedVectorFormat::GetData<T>(col);
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	if (!col.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			auto isnull = !col.validity.RowIsValid(col_idx);
			T store_value = isnull ? NullValue<T>() : data[col_idx];
			Store<T>(store_value, row + col_offset);
			if (isnull) {
				ValidityBytes col_mask(ptrs[idx]);
				col_mask.SetInvalidUnsafe(col_no);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			Store<T>(data[col_idx], row + col_offset);
		}
	}
}

static void ComputeStringEntrySizes(const UnifiedVectorFormat &col, idx_t entry_sizes[], const SelectionVector &sel,
                                    const idx_t count, const idx_t offset = 0) {
	auto data = UnifiedVectorFormat::GetData<string_t>(col);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx) + offset;
		const auto &str = data[col_idx];
		if (col.validity.RowIsValid(col_idx) && !str.IsInlined()) {
			entry_sizes[i] += str.GetSize();
		}
	}
}

static void ScatterStringVector(UnifiedVectorFormat &col, Vector &rows, data_ptr_t str_locations[],
                                const SelectionVector &sel, const idx_t count, const idx_t col_offset,
                                const idx_t col_no) {
	auto string_data = UnifiedVectorFormat::GetData<string_t>(col);
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	// Write out zero length to avoid swizzling problems.
	const string_t null(nullptr, 0);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx);
		auto row = ptrs[idx];
		if (!col.validity.RowIsValid(col_idx)) {
			ValidityBytes col_mask(row);
			col_mask.SetInvalidUnsafe(col_no);
			Store<string_t>(null, row + col_offset);
		} else if (string_data[col_idx].IsInlined()) {
			Store<string_t>(string_data[col_idx], row + col_offset);
		} else {
			const auto &str = string_data[col_idx];
			string_t inserted(const_char_ptr_cast(str_locations[i]), str.GetSize());
			memcpy(inserted.GetDataWriteable(), str.GetData(), str.GetSize());
			str_locations[i] += str.GetSize();
			inserted.Finalize();
			Store<string_t>(inserted, row + col_offset);
		}
	}
}

static void ScatterNestedVector(Vector &vec, UnifiedVectorFormat &col, Vector &rows, data_ptr_t data_locations[],
                                const SelectionVector &sel, const idx_t count, const idx_t col_offset,
                                const idx_t col_no, const idx_t vcount) {
	// Store pointers to the data in the row
	// Do this first because SerializeVector destroys the locations
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto row = ptrs[idx];
		validitymask_locations[i] = row;

		Store<data_ptr_t>(data_locations[i], row + col_offset);
	}

	// Serialise the data
	RowOperations::HeapScatter(vec, vcount, sel, count, col_no, data_locations, validitymask_locations);
}

void RowOperations::Scatter(DataChunk &columns, UnifiedVectorFormat col_data[], const RowLayout &layout, Vector &rows,
                            RowDataCollection &string_heap, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}

	// Set the validity mask for each row before inserting data
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	for (idx_t i = 0; i < count; ++i) {
		auto row_idx = sel.get_index(i);
		auto row = ptrs[row_idx];
		ValidityBytes(row).SetAllValid(layout.ColumnCount());
	}

	const auto vcount = columns.size();
	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();

	// Compute the entry size of the variable size columns
	vector<BufferHandle> handles;
	data_ptr_t data_locations[STANDARD_VECTOR_SIZE];
	if (!layout.AllConstant()) {
		idx_t entry_sizes[STANDARD_VECTOR_SIZE];
		std::fill_n(entry_sizes, count, sizeof(uint32_t));
		for (idx_t col_no = 0; col_no < types.size(); col_no++) {
			if (TypeIsConstantSize(types[col_no].InternalType())) {
				continue;
			}

			auto &vec = columns.data[col_no];
			auto &col = col_data[col_no];
			switch (types[col_no].InternalType()) {
			case PhysicalType::VARCHAR:
				ComputeStringEntrySizes(col, entry_sizes, sel, count);
				break;
			case PhysicalType::LIST:
			case PhysicalType::STRUCT:
				RowOperations::ComputeEntrySizes(vec, col, entry_sizes, vcount, count, sel);
				break;
			default:
				throw InternalException("Unsupported type for RowOperations::Scatter");
			}
		}

		// Build out the buffer space
		handles = string_heap.Build(count, data_locations, entry_sizes);

		// Serialize information that is needed for swizzling if the computation goes out-of-core
		const idx_t heap_pointer_offset = layout.GetHeapOffset();
		for (idx_t i = 0; i < count; i++) {
			auto row_idx = sel.get_index(i);
			auto row = ptrs[row_idx];
			// Pointer to this row in the heap block
			Store<data_ptr_t>(data_locations[i], row + heap_pointer_offset);
			// Row size is stored in the heap in front of each row
			Store<uint32_t>(entry_sizes[i], data_locations[i]);
			data_locations[i] += sizeof(uint32_t);
		}
	}

	for (idx_t col_no = 0; col_no < types.size(); col_no++) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		auto col_offset = offsets[col_no];

		switch (types[col_no].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::VARCHAR:
			ScatterStringVector(col, rows, data_locations, sel, count, col_offset, col_no);
			break;
		case PhysicalType::LIST:
		case PhysicalType::STRUCT:
			ScatterNestedVector(vec, col, rows, data_locations, sel, count, col_offset, col_no, vcount);
			break;
		default:
			throw InternalException("Unsupported type for RowOperations::Scatter");
		}
	}
}

} // namespace duckdb


namespace duckdb {

//-------------------------------------------------------------------------
// Nested Type Hooks
//-------------------------------------------------------------------------
void BinaryDeserializer::OnPropertyBegin(const field_id_t field_id, const char *) {
	auto field = NextField();
	if (field != field_id) {
		throw InternalException("Failed to deserialize: field id mismatch, expected: %d, got: %d", field_id, field);
	}
}

void BinaryDeserializer::OnPropertyEnd() {
}

bool BinaryDeserializer::OnOptionalPropertyBegin(const field_id_t field_id, const char *s) {
	auto next_field = PeekField();
	auto present = next_field == field_id;
	if (present) {
		ConsumeField();
	}
	return present;
}

void BinaryDeserializer::OnOptionalPropertyEnd(bool present) {
}

void BinaryDeserializer::OnObjectBegin() {
	nesting_level++;
}

void BinaryDeserializer::OnObjectEnd() {
	auto next_field = NextField();
	if (next_field != MESSAGE_TERMINATOR_FIELD_ID) {
		throw InternalException("Failed to deserialize: expected end of object, but found field id: %d", next_field);
	}
	nesting_level--;
}

idx_t BinaryDeserializer::OnListBegin() {
	return VarIntDecode<idx_t>();
}

void BinaryDeserializer::OnListEnd() {
}

bool BinaryDeserializer::OnNullableBegin() {
	return ReadBool();
}

void BinaryDeserializer::OnNullableEnd() {
}

//-------------------------------------------------------------------------
// Primitive Types
//-------------------------------------------------------------------------
bool BinaryDeserializer::ReadBool() {
	return static_cast<bool>(ReadPrimitive<uint8_t>());
}

char BinaryDeserializer::ReadChar() {
	return ReadPrimitive<char>();
}

int8_t BinaryDeserializer::ReadSignedInt8() {
	return VarIntDecode<int8_t>();
}

uint8_t BinaryDeserializer::ReadUnsignedInt8() {
	return VarIntDecode<uint8_t>();
}

int16_t BinaryDeserializer::ReadSignedInt16() {
	return VarIntDecode<int16_t>();
}

uint16_t BinaryDeserializer::ReadUnsignedInt16() {
	return VarIntDecode<uint16_t>();
}

int32_t BinaryDeserializer::ReadSignedInt32() {
	return VarIntDecode<int32_t>();
}

uint32_t BinaryDeserializer::ReadUnsignedInt32() {
	return VarIntDecode<uint32_t>();
}

int64_t BinaryDeserializer::ReadSignedInt64() {
	return VarIntDecode<int64_t>();
}

uint64_t BinaryDeserializer::ReadUnsignedInt64() {
	return VarIntDecode<uint64_t>();
}

float BinaryDeserializer::ReadFloat() {
	auto value = ReadPrimitive<float>();
	return value;
}

double BinaryDeserializer::ReadDouble() {
	auto value = ReadPrimitive<double>();
	return value;
}

string BinaryDeserializer::ReadString() {
	auto len = VarIntDecode<uint32_t>();
	if (len == 0) {
		return string();
	}
	auto buffer = make_unsafe_uniq_array<data_t>(len);
	ReadData(buffer.get(), len);
	return string(const_char_ptr_cast(buffer.get()), len);
}

hugeint_t BinaryDeserializer::ReadHugeInt() {
	auto upper = VarIntDecode<int64_t>();
	auto lower = VarIntDecode<uint64_t>();
	return hugeint_t(upper, lower);
}

void BinaryDeserializer::ReadDataPtr(data_ptr_t &ptr_p, idx_t count) {
	auto len = VarIntDecode<uint64_t>();
	if (len != count) {
		throw SerializationException("Tried to read blob of %d size, but only %d elements are available", count, len);
	}
	ReadData(ptr_p, count);
}

} // namespace duckdb


#ifdef DEBUG

#endif

namespace duckdb {

void BinarySerializer::OnPropertyBegin(const field_id_t field_id, const char *tag) {
	// Just write the field id straight up
	Write<field_id_t>(field_id);
#ifdef DEBUG
	// First of check that we are inside an object
	if (debug_stack.empty()) {
		throw InternalException("OnPropertyBegin called outside of object");
	}

	// Check that the tag is unique
	auto &state = debug_stack.back();
	auto &seen_field_ids = state.seen_field_ids;
	auto &seen_field_tags = state.seen_field_tags;
	auto &seen_fields = state.seen_fields;

	if (seen_field_ids.find(field_id) != seen_field_ids.end() || seen_field_tags.find(tag) != seen_field_tags.end()) {
		string all_fields;
		for (auto &field : seen_fields) {
			all_fields += StringUtil::Format("\"%s\":%d ", field.first, field.second);
		}
		throw InternalException("Duplicate field id/tag in field: \"%s\":%d, other fields: %s", tag, field_id,
		                        all_fields);
	}

	seen_field_ids.insert(field_id);
	seen_field_tags.insert(tag);
	seen_fields.emplace_back(tag, field_id);
#else
	(void)tag;
#endif
}

void BinarySerializer::OnPropertyEnd() {
	// Nothing to do here
}

void BinarySerializer::OnOptionalPropertyBegin(const field_id_t field_id, const char *tag, bool present) {
	// Dont write anything at all if the property is not present
	if (present) {
		OnPropertyBegin(field_id, tag);
	}
}

void BinarySerializer::OnOptionalPropertyEnd(bool present) {
	// Nothing to do here
}

//-------------------------------------------------------------------------
// Nested Type Hooks
//-------------------------------------------------------------------------
void BinarySerializer::OnObjectBegin() {
#ifdef DEBUG
	debug_stack.emplace_back();
#endif
}

void BinarySerializer::OnObjectEnd() {
#ifdef DEBUG
	debug_stack.pop_back();
#endif
	// Write object terminator
	Write<field_id_t>(MESSAGE_TERMINATOR_FIELD_ID);
}

void BinarySerializer::OnListBegin(idx_t count) {
	VarIntEncode(count);
}

void BinarySerializer::OnListEnd() {
}

void BinarySerializer::OnNullableBegin(bool present) {
	WriteValue(present);
}

void BinarySerializer::OnNullableEnd() {
}

//-------------------------------------------------------------------------
// Primitive Types
//-------------------------------------------------------------------------
void BinarySerializer::WriteNull() {
	// This should never be called, optional writes should be handled by OnOptionalBegin
}

void BinarySerializer::WriteValue(bool value) {
	Write<uint8_t>(value);
}

void BinarySerializer::WriteValue(uint8_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(char value) {
	Write(value);
}

void BinarySerializer::WriteValue(int8_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint16_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int16_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint32_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int32_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(uint64_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(int64_t value) {
	VarIntEncode(value);
}

void BinarySerializer::WriteValue(hugeint_t value) {
	VarIntEncode(value.upper);
	VarIntEncode(value.lower);
}

void BinarySerializer::WriteValue(float value) {
	Write(value);
}

void BinarySerializer::WriteValue(double value) {
	Write(value);
}

void BinarySerializer::WriteValue(const string &value) {
	uint32_t len = value.length();
	VarIntEncode(len);
	WriteData(value.c_str(), len);
}

void BinarySerializer::WriteValue(const string_t value) {
	uint32_t len = value.GetSize();
	VarIntEncode(len);
	WriteData(value.GetDataUnsafe(), len);
}

void BinarySerializer::WriteValue(const char *value) {
	uint32_t len = strlen(value);
	VarIntEncode(len);
	WriteData(value, len);
}

void BinarySerializer::WriteDataPtr(const_data_ptr_t ptr, idx_t count) {
	VarIntEncode(static_cast<uint64_t>(count));
	WriteData(ptr, count);
}

} // namespace duckdb




#include <cstring>
#include <algorithm>

namespace duckdb {

BufferedFileReader::BufferedFileReader(FileSystem &fs, const char *path, FileLockType lock_type,
                                       optional_ptr<FileOpener> opener)
    : fs(fs), data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), offset(0), read_data(0), total_read(0) {
	handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, lock_type, FileSystem::DEFAULT_COMPRESSION, opener.get());
	file_size = fs.GetFileSize(*handle);
}

void BufferedFileReader::ReadData(data_ptr_t target_buffer, uint64_t read_size) {
	// first copy anything we can from the buffer
	data_ptr_t end_ptr = target_buffer + read_size;
	while (true) {
		idx_t to_read = MinValue<idx_t>(end_ptr - target_buffer, read_data - offset);
		if (to_read > 0) {
			memcpy(target_buffer, data.get() + offset, to_read);
			offset += to_read;
			target_buffer += to_read;
		}
		if (target_buffer < end_ptr) {
			D_ASSERT(offset == read_data);
			total_read += read_data;
			// did not finish reading yet but exhausted buffer
			// read data into buffer
			offset = 0;
			read_data = fs.Read(*handle, data.get(), FILE_BUFFER_SIZE);
			if (read_data == 0) {
				throw SerializationException("not enough data in file to deserialize result");
			}
		} else {
			return;
		}
	}
}

bool BufferedFileReader::Finished() {
	return total_read + offset == file_size;
}

void BufferedFileReader::Seek(uint64_t location) {
	D_ASSERT(location <= file_size);
	handle->Seek(location);
	total_read = location;
	read_data = offset = 0;
}

uint64_t BufferedFileReader::CurrentOffset() {
	return total_read + offset;
}

} // namespace duckdb



#include <cstring>

namespace duckdb {

// Remove this when we switch C++17: https://stackoverflow.com/a/53350948
constexpr uint8_t BufferedFileWriter::DEFAULT_OPEN_FLAGS;

BufferedFileWriter::BufferedFileWriter(FileSystem &fs, const string &path_p, uint8_t open_flags)
    : fs(fs), path(path_p), data(make_unsafe_uniq_array<data_t>(FILE_BUFFER_SIZE)), offset(0), total_written(0) {
	handle = fs.OpenFile(path, open_flags, FileLockType::WRITE_LOCK);
}

int64_t BufferedFileWriter::GetFileSize() {
	return fs.GetFileSize(*handle) + offset;
}

idx_t BufferedFileWriter::GetTotalWritten() {
	return total_written + offset;
}

void BufferedFileWriter::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	// first copy anything we can from the buffer
	const_data_ptr_t end_ptr = buffer + write_size;
	while (buffer < end_ptr) {
		idx_t to_write = MinValue<idx_t>((end_ptr - buffer), FILE_BUFFER_SIZE - offset);
		D_ASSERT(to_write > 0);
		memcpy(data.get() + offset, buffer, to_write);
		offset += to_write;
		buffer += to_write;
		if (offset == FILE_BUFFER_SIZE) {
			Flush();
		}
	}
}

void BufferedFileWriter::Flush() {
	if (offset == 0) {
		return;
	}
	fs.Write(*handle, data.get(), offset);
	total_written += offset;
	offset = 0;
}

void BufferedFileWriter::Sync() {
	Flush();
	handle->Sync();
}

void BufferedFileWriter::Truncate(int64_t size) {
	uint64_t persistent = fs.GetFileSize(*handle);
	D_ASSERT((uint64_t)size <= persistent + offset);
	if (persistent <= (uint64_t)size) {
		// truncating into the pending write buffer.
		offset = size - persistent;
	} else {
		// truncate the physical file on disk
		handle->Truncate(size);
		// reset anything written in the buffer
		offset = 0;
	}
}

} // namespace duckdb


namespace duckdb {

MemoryStream::MemoryStream(idx_t capacity)
    : position(0), capacity(capacity), owns_data(true), data(static_cast<data_ptr_t>(malloc(capacity))) {
}

MemoryStream::MemoryStream(data_ptr_t buffer, idx_t capacity)
    : position(0), capacity(capacity), owns_data(false), data(buffer) {
}

MemoryStream::~MemoryStream() {
	if (owns_data) {
		free(data);
	}
}

void MemoryStream::WriteData(const_data_ptr_t source, idx_t write_size) {
	while (position + write_size > capacity) {
		if (owns_data) {
			capacity *= 2;
			data = static_cast<data_ptr_t>(realloc(data, capacity));
		} else {
			throw SerializationException("Failed to serialize: not enough space in buffer to fulfill write request");
		}
	}

	memcpy(data + position, source, write_size);
	position += write_size;
}

void MemoryStream::ReadData(data_ptr_t destination, idx_t read_size) {
	if (position + read_size > capacity) {
		throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
	}
	memcpy(destination, data + position, read_size);
	position += read_size;
}

void MemoryStream::Rewind() {
	position = 0;
}

void MemoryStream::Release() {
	owns_data = false;
}

data_ptr_t MemoryStream::GetData() const {
	return data;
}

idx_t MemoryStream::GetPosition() const {
	return position;
}

idx_t MemoryStream::GetCapacity() const {
	return capacity;
}

} // namespace duckdb


namespace duckdb {

template <>
void Serializer::WriteValue(const vector<bool> &vec) {
	auto count = vec.size();
	OnListBegin(count);
	for (auto item : vec) {
		WriteValue(item);
	}
	OnListEnd();
}

} // namespace duckdb





namespace duckdb {

bool Comparators::TieIsBreakable(const idx_t &tie_col, const data_ptr_t &row_ptr, const SortLayout &sort_layout) {
	const auto &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	// Check if the blob is NULL
	ValidityBytes row_mask(row_ptr);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
		// Can't break a NULL tie
		return false;
	}
	auto &row_layout = sort_layout.blob_layout;
	if (row_layout.GetTypes()[col_idx].InternalType() != PhysicalType::VARCHAR) {
		// Nested type, must be broken
		return true;
	}
	const auto &tie_col_offset = row_layout.GetOffsets()[col_idx];
	auto tie_string = Load<string_t>(row_ptr + tie_col_offset);
	if (tie_string.GetSize() < sort_layout.prefix_lengths[tie_col]) {
		// No need to break the tie - we already compared the full string
		return false;
	}
	return true;
}

int Comparators::CompareTuple(const SBScanState &left, const SBScanState &right, const data_ptr_t &l_ptr,
                              const data_ptr_t &r_ptr, const SortLayout &sort_layout, const bool &external_sort) {
	// Compare the sorting columns one by one
	int comp_res = 0;
	data_ptr_t l_ptr_offset = l_ptr;
	data_ptr_t r_ptr_offset = r_ptr;
	for (idx_t col_idx = 0; col_idx < sort_layout.column_count; col_idx++) {
		comp_res = FastMemcmp(l_ptr_offset, r_ptr_offset, sort_layout.column_sizes[col_idx]);
		if (comp_res == 0 && !sort_layout.constant_size[col_idx]) {
			comp_res = BreakBlobTie(col_idx, left, right, sort_layout, external_sort);
		}
		if (comp_res != 0) {
			break;
		}
		l_ptr_offset += sort_layout.column_sizes[col_idx];
		r_ptr_offset += sort_layout.column_sizes[col_idx];
	}
	return comp_res;
}

int Comparators::CompareVal(const data_ptr_t l_ptr, const data_ptr_t r_ptr, const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR:
		return TemplatedCompareVal<string_t>(l_ptr, r_ptr);
	case PhysicalType::LIST:
	case PhysicalType::STRUCT: {
		auto l_nested_ptr = Load<data_ptr_t>(l_ptr);
		auto r_nested_ptr = Load<data_ptr_t>(r_ptr);
		return CompareValAndAdvance(l_nested_ptr, r_nested_ptr, type, true);
	}
	default:
		throw NotImplementedException("Unimplemented CompareVal for type %s", type.ToString());
	}
}

int Comparators::BreakBlobTie(const idx_t &tie_col, const SBScanState &left, const SBScanState &right,
                              const SortLayout &sort_layout, const bool &external) {
	data_ptr_t l_data_ptr = left.DataPtr(*left.sb->blob_sorting_data);
	data_ptr_t r_data_ptr = right.DataPtr(*right.sb->blob_sorting_data);
	if (!TieIsBreakable(tie_col, l_data_ptr, sort_layout)) {
		// Quick check to see if ties can be broken
		return 0;
	}
	// Align the pointers
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	l_data_ptr += tie_col_offset;
	r_data_ptr += tie_col_offset;
	// Do the comparison
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &type = sort_layout.blob_layout.GetTypes()[col_idx];
	int result;
	if (external) {
		// Store heap pointers
		data_ptr_t l_heap_ptr = left.HeapPtr(*left.sb->blob_sorting_data);
		data_ptr_t r_heap_ptr = right.HeapPtr(*right.sb->blob_sorting_data);
		// Unswizzle offset to pointer
		UnswizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		UnswizzleSingleValue(r_data_ptr, r_heap_ptr, type);
		// Compare
		result = CompareVal(l_data_ptr, r_data_ptr, type);
		// Swizzle the pointers back to offsets
		SwizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		SwizzleSingleValue(r_data_ptr, r_heap_ptr, type);
	} else {
		result = CompareVal(l_data_ptr, r_data_ptr, type);
	}
	return order * result;
}

template <class T>
int Comparators::TemplatedCompareVal(const data_ptr_t &left_ptr, const data_ptr_t &right_ptr) {
	const auto left_val = Load<T>(left_ptr);
	const auto right_val = Load<T>(right_ptr);
	if (Equals::Operation<T>(left_val, right_val)) {
		return 0;
	} else if (LessThan::Operation<T>(left_val, right_val)) {
		return -1;
	} else {
		return 1;
	}
}

int Comparators::CompareValAndAdvance(data_ptr_t &l_ptr, data_ptr_t &r_ptr, const LogicalType &type, bool valid) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedCompareAndAdvance<int8_t>(l_ptr, r_ptr);
	case PhysicalType::INT16:
		return TemplatedCompareAndAdvance<int16_t>(l_ptr, r_ptr);
	case PhysicalType::INT32:
		return TemplatedCompareAndAdvance<int32_t>(l_ptr, r_ptr);
	case PhysicalType::INT64:
		return TemplatedCompareAndAdvance<int64_t>(l_ptr, r_ptr);
	case PhysicalType::UINT8:
		return TemplatedCompareAndAdvance<uint8_t>(l_ptr, r_ptr);
	case PhysicalType::UINT16:
		return TemplatedCompareAndAdvance<uint16_t>(l_ptr, r_ptr);
	case PhysicalType::UINT32:
		return TemplatedCompareAndAdvance<uint32_t>(l_ptr, r_ptr);
	case PhysicalType::UINT64:
		return TemplatedCompareAndAdvance<uint64_t>(l_ptr, r_ptr);
	case PhysicalType::INT128:
		return TemplatedCompareAndAdvance<hugeint_t>(l_ptr, r_ptr);
	case PhysicalType::FLOAT:
		return TemplatedCompareAndAdvance<float>(l_ptr, r_ptr);
	case PhysicalType::DOUBLE:
		return TemplatedCompareAndAdvance<double>(l_ptr, r_ptr);
	case PhysicalType::INTERVAL:
		return TemplatedCompareAndAdvance<interval_t>(l_ptr, r_ptr);
	case PhysicalType::VARCHAR:
		return CompareStringAndAdvance(l_ptr, r_ptr, valid);
	case PhysicalType::LIST:
		return CompareListAndAdvance(l_ptr, r_ptr, ListType::GetChildType(type), valid);
	case PhysicalType::STRUCT:
		return CompareStructAndAdvance(l_ptr, r_ptr, StructType::GetChildTypes(type), valid);
	default:
		throw NotImplementedException("Unimplemented CompareValAndAdvance for type %s", type.ToString());
	}
}

template <class T>
int Comparators::TemplatedCompareAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr) {
	auto result = TemplatedCompareVal<T>(left_ptr, right_ptr);
	left_ptr += sizeof(T);
	right_ptr += sizeof(T);
	return result;
}

int Comparators::CompareStringAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, bool valid) {
	if (!valid) {
		return 0;
	}
	uint32_t left_string_size = Load<uint32_t>(left_ptr);
	uint32_t right_string_size = Load<uint32_t>(right_ptr);
	left_ptr += sizeof(uint32_t);
	right_ptr += sizeof(uint32_t);
	auto memcmp_res = memcmp(const_char_ptr_cast(left_ptr), const_char_ptr_cast(right_ptr),
	                         std::min<uint32_t>(left_string_size, right_string_size));

	left_ptr += left_string_size;
	right_ptr += right_string_size;

	if (memcmp_res != 0) {
		return memcmp_res;
	}
	if (left_string_size == right_string_size) {
		return 0;
	}
	if (left_string_size < right_string_size) {
		return -1;
	}
	return 1;
}

int Comparators::CompareStructAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                         const child_list_t<LogicalType> &types, bool valid) {
	idx_t count = types.size();
	// Load validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (count + 7) / 8;
	right_ptr += (count + 7) / 8;
	// Initialize variables
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	// Compare
	int comp_res = 0;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		auto &type = types[i].second;
		if ((left_valid == right_valid) || TypeIsConstantSize(type.InternalType())) {
			comp_res = CompareValAndAdvance(left_ptr, right_ptr, types[i].second, left_valid && valid);
		}
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

int Comparators::CompareListAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const LogicalType &type,
                                       bool valid) {
	if (!valid) {
		return 0;
	}
	// Load list lengths
	auto left_len = Load<idx_t>(left_ptr);
	auto right_len = Load<idx_t>(right_ptr);
	left_ptr += sizeof(idx_t);
	right_ptr += sizeof(idx_t);
	// Load list validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (left_len + 7) / 8;
	right_ptr += (right_len + 7) / 8;
	// Compare
	int comp_res = 0;
	idx_t count = MinValue(left_len, right_len);
	if (TypeIsConstantSize(type.InternalType())) {
		// Templated code for fixed-size types
		switch (type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			comp_res = TemplatedCompareListLoop<int8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT16:
			comp_res = TemplatedCompareListLoop<int16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT32:
			comp_res = TemplatedCompareListLoop<int32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT64:
			comp_res = TemplatedCompareListLoop<int64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT8:
			comp_res = TemplatedCompareListLoop<uint8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT16:
			comp_res = TemplatedCompareListLoop<uint16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT32:
			comp_res = TemplatedCompareListLoop<uint32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT64:
			comp_res = TemplatedCompareListLoop<uint64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT128:
			comp_res = TemplatedCompareListLoop<hugeint_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::FLOAT:
			comp_res = TemplatedCompareListLoop<float>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::DOUBLE:
			comp_res = TemplatedCompareListLoop<double>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INTERVAL:
			comp_res = TemplatedCompareListLoop<interval_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		default:
			throw NotImplementedException("CompareListAndAdvance for fixed-size type %s", type.ToString());
		}
	} else {
		// Variable-sized list entries
		bool left_valid;
		bool right_valid;
		idx_t entry_idx;
		idx_t idx_in_entry;
		// Size (in bytes) of all variable-sizes entries is stored before the entries begin,
		// to make deserialization easier. We need to skip over them
		left_ptr += left_len * sizeof(idx_t);
		right_ptr += right_len * sizeof(idx_t);
		for (idx_t i = 0; i < count; i++) {
			ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
			left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
			right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
			if (left_valid && right_valid) {
				switch (type.InternalType()) {
				case PhysicalType::LIST:
					comp_res = CompareListAndAdvance(left_ptr, right_ptr, ListType::GetChildType(type), left_valid);
					break;
				case PhysicalType::VARCHAR:
					comp_res = CompareStringAndAdvance(left_ptr, right_ptr, left_valid);
					break;
				case PhysicalType::STRUCT:
					comp_res =
					    CompareStructAndAdvance(left_ptr, right_ptr, StructType::GetChildTypes(type), left_valid);
					break;
				default:
					throw NotImplementedException("CompareListAndAdvance for variable-size type %s", type.ToString());
				}
			} else if (!left_valid && !right_valid) {
				comp_res = 0;
			} else if (left_valid) {
				comp_res = -1;
			} else {
				comp_res = 1;
			}
			if (comp_res != 0) {
				break;
			}
		}
	}
	// All values that we looped over were equal
	if (comp_res == 0 && left_len != right_len) {
		// Smaller lists first
		if (left_len < right_len) {
			comp_res = -1;
		} else {
			comp_res = 1;
		}
	}
	return comp_res;
}

template <class T>
int Comparators::TemplatedCompareListLoop(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                          const ValidityBytes &left_validity, const ValidityBytes &right_validity,
                                          const idx_t &count) {
	int comp_res = 0;
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		comp_res = TemplatedCompareAndAdvance<T>(left_ptr, right_ptr);
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

void Comparators::UnswizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += string_t::HEADER_SIZE;
	}
	Store<data_ptr_t>(heap_ptr + Load<idx_t>(data_ptr), data_ptr);
}

void Comparators::SwizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += string_t::HEADER_SIZE;
	}
	Store<idx_t>(Load<data_ptr_t>(data_ptr) - heap_ptr, data_ptr);
}

} // namespace duckdb




namespace duckdb {

MergeSorter::MergeSorter(GlobalSortState &state, BufferManager &buffer_manager)
    : state(state), buffer_manager(buffer_manager), sort_layout(state.sort_layout) {
}

void MergeSorter::PerformInMergeRound() {
	while (true) {
		{
			lock_guard<mutex> pair_guard(state.lock);
			if (state.pair_idx == state.num_pairs) {
				break;
			}
			GetNextPartition();
		}
		MergePartition();
	}
}

void MergeSorter::MergePartition() {
	auto &left_block = *left->sb;
	auto &right_block = *right->sb;
#ifdef DEBUG
	D_ASSERT(left_block.radix_sorting_data.size() == left_block.payload_data->data_blocks.size());
	D_ASSERT(right_block.radix_sorting_data.size() == right_block.payload_data->data_blocks.size());
	if (!state.payload_layout.AllConstant() && state.external) {
		D_ASSERT(left_block.payload_data->data_blocks.size() == left_block.payload_data->heap_blocks.size());
		D_ASSERT(right_block.payload_data->data_blocks.size() == right_block.payload_data->heap_blocks.size());
	}
	if (!sort_layout.all_constant) {
		D_ASSERT(left_block.radix_sorting_data.size() == left_block.blob_sorting_data->data_blocks.size());
		D_ASSERT(right_block.radix_sorting_data.size() == right_block.blob_sorting_data->data_blocks.size());
		if (state.external) {
			D_ASSERT(left_block.blob_sorting_data->data_blocks.size() ==
			         left_block.blob_sorting_data->heap_blocks.size());
			D_ASSERT(right_block.blob_sorting_data->data_blocks.size() ==
			         right_block.blob_sorting_data->heap_blocks.size());
		}
	}
#endif
	// Set up the write block
	// Each merge task produces a SortedBlock with exactly state.block_capacity rows or less
	result->InitializeWrite();
	// Initialize arrays to store merge data
	bool left_smaller[STANDARD_VECTOR_SIZE];
	idx_t next_entry_sizes[STANDARD_VECTOR_SIZE];
	// Merge loop
#ifdef DEBUG
	auto l_count = left->Remaining();
	auto r_count = right->Remaining();
#endif
	while (true) {
		auto l_remaining = left->Remaining();
		auto r_remaining = right->Remaining();
		if (l_remaining + r_remaining == 0) {
			// Done
			break;
		}
		const idx_t next = MinValue(l_remaining + r_remaining, (idx_t)STANDARD_VECTOR_SIZE);
		if (l_remaining != 0 && r_remaining != 0) {
			// Compute the merge (not needed if one side is exhausted)
			ComputeMerge(next, left_smaller);
		}
		// Actually merge the data (radix, blob, and payload)
		MergeRadix(next, left_smaller);
		if (!sort_layout.all_constant) {
			MergeData(*result->blob_sorting_data, *left_block.blob_sorting_data, *right_block.blob_sorting_data, next,
			          left_smaller, next_entry_sizes, true);
			D_ASSERT(result->radix_sorting_data.size() == result->blob_sorting_data->data_blocks.size());
		}
		MergeData(*result->payload_data, *left_block.payload_data, *right_block.payload_data, next, left_smaller,
		          next_entry_sizes, false);
		D_ASSERT(result->radix_sorting_data.size() == result->payload_data->data_blocks.size());
	}
#ifdef DEBUG
	D_ASSERT(result->Count() == l_count + r_count);
#endif
}

void MergeSorter::GetNextPartition() {
	// Create result block
	state.sorted_blocks_temp[state.pair_idx].push_back(make_uniq<SortedBlock>(buffer_manager, state));
	result = state.sorted_blocks_temp[state.pair_idx].back().get();
	// Determine which blocks must be merged
	auto &left_block = *state.sorted_blocks[state.pair_idx * 2];
	auto &right_block = *state.sorted_blocks[state.pair_idx * 2 + 1];
	const idx_t l_count = left_block.Count();
	const idx_t r_count = right_block.Count();
	// Initialize left and right reader
	left = make_uniq<SBScanState>(buffer_manager, state);
	right = make_uniq<SBScanState>(buffer_manager, state);
	// Compute the work that this thread must do using Merge Path
	idx_t l_end;
	idx_t r_end;
	if (state.l_start + state.r_start + state.block_capacity < l_count + r_count) {
		left->sb = state.sorted_blocks[state.pair_idx * 2].get();
		right->sb = state.sorted_blocks[state.pair_idx * 2 + 1].get();
		const idx_t intersection = state.l_start + state.r_start + state.block_capacity;
		GetIntersection(intersection, l_end, r_end);
		D_ASSERT(l_end <= l_count);
		D_ASSERT(r_end <= r_count);
		D_ASSERT(intersection == l_end + r_end);
	} else {
		l_end = l_count;
		r_end = r_count;
	}
	// Create slices of the data that this thread must merge
	left->SetIndices(0, 0);
	right->SetIndices(0, 0);
	left_input = left_block.CreateSlice(state.l_start, l_end, left->entry_idx);
	right_input = right_block.CreateSlice(state.r_start, r_end, right->entry_idx);
	left->sb = left_input.get();
	right->sb = right_input.get();
	state.l_start = l_end;
	state.r_start = r_end;
	D_ASSERT(left->Remaining() + right->Remaining() == state.block_capacity || (l_end == l_count && r_end == r_count));
	// Update global state
	if (state.l_start == l_count && state.r_start == r_count) {
		// Delete references to previous pair
		state.sorted_blocks[state.pair_idx * 2] = nullptr;
		state.sorted_blocks[state.pair_idx * 2 + 1] = nullptr;
		// Advance pair
		state.pair_idx++;
		state.l_start = 0;
		state.r_start = 0;
	}
}

int MergeSorter::CompareUsingGlobalIndex(SBScanState &l, SBScanState &r, const idx_t l_idx, const idx_t r_idx) {
	D_ASSERT(l_idx < l.sb->Count());
	D_ASSERT(r_idx < r.sb->Count());

	// Easy comparison using the previous result (intersections must increase monotonically)
	if (l_idx < state.l_start) {
		return -1;
	}
	if (r_idx < state.r_start) {
		return 1;
	}

	l.sb->GlobalToLocalIndex(l_idx, l.block_idx, l.entry_idx);
	r.sb->GlobalToLocalIndex(r_idx, r.block_idx, r.entry_idx);

	l.PinRadix(l.block_idx);
	r.PinRadix(r.block_idx);
	data_ptr_t l_ptr = l.radix_handle.Ptr() + l.entry_idx * sort_layout.entry_size;
	data_ptr_t r_ptr = r.radix_handle.Ptr() + r.entry_idx * sort_layout.entry_size;

	int comp_res;
	if (sort_layout.all_constant) {
		comp_res = FastMemcmp(l_ptr, r_ptr, sort_layout.comparison_size);
	} else {
		l.PinData(*l.sb->blob_sorting_data);
		r.PinData(*r.sb->blob_sorting_data);
		comp_res = Comparators::CompareTuple(l, r, l_ptr, r_ptr, sort_layout, state.external);
	}
	return comp_res;
}

void MergeSorter::GetIntersection(const idx_t diagonal, idx_t &l_idx, idx_t &r_idx) {
	const idx_t l_count = left->sb->Count();
	const idx_t r_count = right->sb->Count();
	// Cover some edge cases
	// Code coverage off because these edge cases cannot happen unless other code changes
	// Edge cases have been tested extensively while developing Merge Path in a script
	// LCOV_EXCL_START
	if (diagonal >= l_count + r_count) {
		l_idx = l_count;
		r_idx = r_count;
		return;
	} else if (diagonal == 0) {
		l_idx = 0;
		r_idx = 0;
		return;
	} else if (l_count == 0) {
		l_idx = 0;
		r_idx = diagonal;
		return;
	} else if (r_count == 0) {
		r_idx = 0;
		l_idx = diagonal;
		return;
	}
	// LCOV_EXCL_STOP
	// Determine offsets for the binary search
	const idx_t l_offset = MinValue(l_count, diagonal);
	const idx_t r_offset = diagonal > l_count ? diagonal - l_count : 0;
	D_ASSERT(l_offset + r_offset == diagonal);
	const idx_t search_space = diagonal > MaxValue(l_count, r_count) ? l_count + r_count - diagonal
	                                                                 : MinValue(diagonal, MinValue(l_count, r_count));
	// Double binary search
	idx_t li = 0;
	idx_t ri = search_space - 1;
	idx_t middle;
	int comp_res;
	while (li <= ri) {
		middle = (li + ri) / 2;
		l_idx = l_offset - middle;
		r_idx = r_offset + middle;
		if (l_idx == l_count || r_idx == 0) {
			comp_res = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
			if (comp_res > 0) {
				l_idx--;
				r_idx++;
			} else {
				return;
			}
			if (l_idx == 0 || r_idx == r_count) {
				// This case is incredibly difficult to cover as it is dependent on parallelism randomness
				// But it has been tested extensively during development in a script
				// LCOV_EXCL_START
				return;
				// LCOV_EXCL_STOP
			} else {
				break;
			}
		}
		comp_res = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx);
		if (comp_res > 0) {
			li = middle + 1;
		} else {
			ri = middle - 1;
		}
	}
	int l_r_min1 = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx - 1);
	int l_min1_r = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
	if (l_r_min1 > 0 && l_min1_r < 0) {
		return;
	} else if (l_r_min1 > 0) {
		l_idx--;
		r_idx++;
	} else if (l_min1_r < 0) {
		l_idx++;
		r_idx--;
	}
}

void MergeSorter::ComputeMerge(const idx_t &count, bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	auto &l_sorted_block = *l.sb;
	auto &r_sorted_block = *r.sb;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;
	// Data pointers for both sides
	data_ptr_t l_radix_ptr;
	data_ptr_t r_radix_ptr;
	// Compute the merge of the next 'count' tuples
	idx_t compared = 0;
	while (compared < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_sorted_block.radix_sorting_data.size() &&
		    l.entry_idx == l_sorted_block.radix_sorting_data[l.block_idx]->count) {
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_sorted_block.radix_sorting_data.size() &&
		    r.entry_idx == r_sorted_block.radix_sorting_data[r.block_idx]->count) {
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_sorted_block.radix_sorting_data.size();
		const bool r_done = r.block_idx == r_sorted_block.radix_sorting_data.size();
		if (l_done || r_done) {
			// One of the sides is exhausted, no need to compare
			break;
		}
		// Pin the radix sorting data
		left->PinRadix(l.block_idx);
		l_radix_ptr = left->RadixPtr();
		right->PinRadix(r.block_idx);
		r_radix_ptr = right->RadixPtr();

		const idx_t l_count = l_sorted_block.radix_sorting_data[l.block_idx]->count;
		const idx_t r_count = r_sorted_block.radix_sorting_data[r.block_idx]->count;
		// Compute the merge
		if (sort_layout.all_constant) {
			// All sorting columns are constant size
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] = FastMemcmp(l_radix_ptr, r_radix_ptr, sort_layout.comparison_size) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		} else {
			// Pin the blob data
			left->PinData(*l_sorted_block.blob_sorting_data);
			right->PinData(*r_sorted_block.blob_sorting_data);
			// Merge with variable size sorting columns
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] =
				    Comparators::CompareTuple(*left, *right, l_radix_ptr, r_radix_ptr, sort_layout, state.external) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeRadix(const idx_t &count, const bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	auto &l_blocks = l.sb->radix_sorting_data;
	auto &r_blocks = r.sb->radix_sorting_data;
	RowDataBlock *l_block = nullptr;
	RowDataBlock *r_block = nullptr;

	data_ptr_t l_ptr;
	data_ptr_t r_ptr;

	RowDataBlock *result_block = result->radix_sorting_data.back().get();
	auto result_handle = buffer_manager.Pin(result_block->block);
	data_ptr_t result_ptr = result_handle.Ptr() + result_block->count * sort_layout.entry_size;

	idx_t copied = 0;
	while (copied < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_blocks.size() && l.entry_idx == l_blocks[l.block_idx]->count) {
			// Delete reference to previous block
			l_blocks[l.block_idx]->block = nullptr;
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_blocks.size() && r.entry_idx == r_blocks[r.block_idx]->count) {
			// Delete reference to previous block
			r_blocks[r.block_idx]->block = nullptr;
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_blocks.size();
		const bool r_done = r.block_idx == r_blocks.size();
		// Pin the radix sortable blocks
		idx_t l_count;
		if (!l_done) {
			l_block = l_blocks[l.block_idx].get();
			left->PinRadix(l.block_idx);
			l_ptr = l.RadixPtr();
			l_count = l_block->count;
		} else {
			l_count = 0;
		}
		idx_t r_count;
		if (!r_done) {
			r_block = r_blocks[r.block_idx].get();
			r.PinRadix(r.block_idx);
			r_ptr = r.RadixPtr();
			r_count = r_block->count;
		} else {
			r_count = 0;
		}
		// Copy using computed merge
		if (!l_done && !r_done) {
			// Both sides have data - merge
			MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, *result_block, result_ptr,
			          sort_layout.entry_size, left_smaller, copied, count);
		} else if (r_done) {
			// Right side is exhausted
			FlushRows(l_ptr, l.entry_idx, l_count, *result_block, result_ptr, sort_layout.entry_size, copied, count);
		} else {
			// Left side is exhausted
			FlushRows(r_ptr, r.entry_idx, r_count, *result_block, result_ptr, sort_layout.entry_size, copied, count);
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
                            const bool left_smaller[], idx_t next_entry_sizes[], bool reset_indices) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	const auto &layout = result_data.layout;
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapOffset();

	// Left and right row data to merge
	data_ptr_t l_ptr;
	data_ptr_t r_ptr;
	// Accompanying left and right heap data (if needed)
	data_ptr_t l_heap_ptr;
	data_ptr_t r_heap_ptr;

	// Result rows to write to
	RowDataBlock *result_data_block = result_data.data_blocks.back().get();
	auto result_data_handle = buffer_manager.Pin(result_data_block->block);
	data_ptr_t result_data_ptr = result_data_handle.Ptr() + result_data_block->count * row_width;
	// Result heap to write to (if needed)
	RowDataBlock *result_heap_block = nullptr;
	BufferHandle result_heap_handle;
	data_ptr_t result_heap_ptr;
	if (!layout.AllConstant() && state.external) {
		result_heap_block = result_data.heap_blocks.back().get();
		result_heap_handle = buffer_manager.Pin(result_heap_block->block);
		result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
	}

	idx_t copied = 0;
	while (copied < count) {
		// Move to new data blocks (if needed)
		if (l.block_idx < l_data.data_blocks.size() && l.entry_idx == l_data.data_blocks[l.block_idx]->count) {
			// Delete reference to previous block
			l_data.data_blocks[l.block_idx]->block = nullptr;
			if (!layout.AllConstant() && state.external) {
				l_data.heap_blocks[l.block_idx]->block = nullptr;
			}
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_data.data_blocks.size() && r.entry_idx == r_data.data_blocks[r.block_idx]->count) {
			// Delete reference to previous block
			r_data.data_blocks[r.block_idx]->block = nullptr;
			if (!layout.AllConstant() && state.external) {
				r_data.heap_blocks[r.block_idx]->block = nullptr;
			}
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_data.data_blocks.size();
		const bool r_done = r.block_idx == r_data.data_blocks.size();
		// Pin the row data blocks
		if (!l_done) {
			l.PinData(l_data);
			l_ptr = l.DataPtr(l_data);
		}
		if (!r_done) {
			r.PinData(r_data);
			r_ptr = r.DataPtr(r_data);
		}
		const idx_t &l_count = !l_done ? l_data.data_blocks[l.block_idx]->count : 0;
		const idx_t &r_count = !r_done ? r_data.data_blocks[r.block_idx]->count : 0;
		// Perform the merge
		if (layout.AllConstant() || !state.external) {
			// If all constant size, or if we are doing an in-memory sort, we do not need to touch the heap
			if (!l_done && !r_done) {
				// Both sides have data - merge
				MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, *result_data_block, result_data_ptr,
				          row_width, left_smaller, copied, count);
			} else if (r_done) {
				// Right side is exhausted
				FlushRows(l_ptr, l.entry_idx, l_count, *result_data_block, result_data_ptr, row_width, copied, count);
			} else {
				// Left side is exhausted
				FlushRows(r_ptr, r.entry_idx, r_count, *result_data_block, result_data_ptr, row_width, copied, count);
			}
		} else {
			// External sorting with variable size data. Pin the heap blocks too
			if (!l_done) {
				l_heap_ptr = l.BaseHeapPtr(l_data) + Load<idx_t>(l_ptr + heap_pointer_offset);
				D_ASSERT(l_heap_ptr - l.BaseHeapPtr(l_data) >= 0);
				D_ASSERT((idx_t)(l_heap_ptr - l.BaseHeapPtr(l_data)) < l_data.heap_blocks[l.block_idx]->byte_offset);
			}
			if (!r_done) {
				r_heap_ptr = r.BaseHeapPtr(r_data) + Load<idx_t>(r_ptr + heap_pointer_offset);
				D_ASSERT(r_heap_ptr - r.BaseHeapPtr(r_data) >= 0);
				D_ASSERT((idx_t)(r_heap_ptr - r.BaseHeapPtr(r_data)) < r_data.heap_blocks[r.block_idx]->byte_offset);
			}
			// Both the row and heap data need to be dealt with
			if (!l_done && !r_done) {
				// Both sides have data - merge
				idx_t l_idx_copy = l.entry_idx;
				idx_t r_idx_copy = r.entry_idx;
				data_ptr_t result_data_ptr_copy = result_data_ptr;
				idx_t copied_copy = copied;
				// Merge row data
				MergeRows(l_ptr, l_idx_copy, l_count, r_ptr, r_idx_copy, r_count, *result_data_block,
				          result_data_ptr_copy, row_width, left_smaller, copied_copy, count);
				const idx_t merged = copied_copy - copied;
				// Compute the entry sizes and number of heap bytes that will be copied
				idx_t copy_bytes = 0;
				data_ptr_t l_heap_ptr_copy = l_heap_ptr;
				data_ptr_t r_heap_ptr_copy = r_heap_ptr;
				for (idx_t i = 0; i < merged; i++) {
					// Store base heap offset in the row data
					Store<idx_t>(result_heap_block->byte_offset + copy_bytes, result_data_ptr + heap_pointer_offset);
					result_data_ptr += row_width;
					// Compute entry size and add to total
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					auto &entry_size = next_entry_sizes[copied + i];
					entry_size =
					    l_smaller * Load<uint32_t>(l_heap_ptr_copy) + r_smaller * Load<uint32_t>(r_heap_ptr_copy);
					D_ASSERT(entry_size >= sizeof(uint32_t));
					D_ASSERT(l_heap_ptr_copy - l.BaseHeapPtr(l_data) + l_smaller * entry_size <=
					         l_data.heap_blocks[l.block_idx]->byte_offset);
					D_ASSERT(r_heap_ptr_copy - r.BaseHeapPtr(r_data) + r_smaller * entry_size <=
					         r_data.heap_blocks[r.block_idx]->byte_offset);
					l_heap_ptr_copy += l_smaller * entry_size;
					r_heap_ptr_copy += r_smaller * entry_size;
					copy_bytes += entry_size;
				}
				// Reallocate result heap block size (if needed)
				if (result_heap_block->byte_offset + copy_bytes > result_heap_block->capacity) {
					idx_t new_capacity = result_heap_block->byte_offset + copy_bytes;
					buffer_manager.ReAllocate(result_heap_block->block, new_capacity);
					result_heap_block->capacity = new_capacity;
					result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
				}
				D_ASSERT(result_heap_block->byte_offset + copy_bytes <= result_heap_block->capacity);
				// Now copy the heap data
				for (idx_t i = 0; i < merged; i++) {
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					const auto &entry_size = next_entry_sizes[copied + i];
					memcpy(result_heap_ptr,
					       reinterpret_cast<data_ptr_t>(l_smaller * CastPointerToValue(l_heap_ptr) +
					                                    r_smaller * CastPointerToValue(r_heap_ptr)),
					       entry_size);
					D_ASSERT(Load<uint32_t>(result_heap_ptr) == entry_size);
					result_heap_ptr += entry_size;
					l_heap_ptr += l_smaller * entry_size;
					r_heap_ptr += r_smaller * entry_size;
					l.entry_idx += l_smaller;
					r.entry_idx += r_smaller;
				}
				// Update result indices and pointers
				result_heap_block->count += merged;
				result_heap_block->byte_offset += copy_bytes;
				copied += merged;
			} else if (r_done) {
				// Right side is exhausted - flush left
				FlushBlobs(layout, l_count, l_ptr, l.entry_idx, l_heap_ptr, *result_data_block, result_data_ptr,
				           *result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			} else {
				// Left side is exhausted - flush right
				FlushBlobs(layout, r_count, r_ptr, r.entry_idx, r_heap_ptr, *result_data_block, result_data_ptr,
				           *result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			}
			D_ASSERT(result_data_block->count == result_heap_block->count);
		}
	}
	if (reset_indices) {
		left->SetIndices(l_block_idx_before, l_entry_idx_before);
		right->SetIndices(r_block_idx_before, r_entry_idx_before);
	}
}

void MergeSorter::MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr,
                            idx_t &r_entry_idx, const idx_t &r_count, RowDataBlock &target_block,
                            data_ptr_t &target_ptr, const idx_t &entry_size, const bool left_smaller[], idx_t &copied,
                            const idx_t &count) {
	const idx_t next = MinValue(count - copied, target_block.capacity - target_block.count);
	idx_t i;
	for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
		const bool &l_smaller = left_smaller[copied + i];
		const bool r_smaller = !l_smaller;
		// Use comparison bool (0 or 1) to copy an entry from either side
		FastMemcpy(
		    target_ptr,
		    reinterpret_cast<data_ptr_t>(l_smaller * CastPointerToValue(l_ptr) + r_smaller * CastPointerToValue(r_ptr)),
		    entry_size);
		target_ptr += entry_size;
		// Use the comparison bool to increment entries and pointers
		l_entry_idx += l_smaller;
		r_entry_idx += r_smaller;
		l_ptr += l_smaller * entry_size;
		r_ptr += r_smaller * entry_size;
	}
	// Update counts
	target_block.count += i;
	copied += i;
}

void MergeSorter::FlushRows(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
                            RowDataBlock &target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
                            const idx_t &count) {
	// Compute how many entries we can fit
	idx_t next = MinValue(count - copied, target_block.capacity - target_block.count);
	next = MinValue(next, source_count - source_entry_idx);
	// Copy them all in a single memcpy
	const idx_t copy_bytes = next * entry_size;
	memcpy(target_ptr, source_ptr, copy_bytes);
	target_ptr += copy_bytes;
	source_ptr += copy_bytes;
	// Update counts
	source_entry_idx += next;
	target_block.count += next;
	copied += next;
}

void MergeSorter::FlushBlobs(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
                             idx_t &source_entry_idx, data_ptr_t &source_heap_ptr, RowDataBlock &target_data_block,
                             data_ptr_t &target_data_ptr, RowDataBlock &target_heap_block,
                             BufferHandle &target_heap_handle, data_ptr_t &target_heap_ptr, idx_t &copied,
                             const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapOffset();
	idx_t source_entry_idx_copy = source_entry_idx;
	data_ptr_t target_data_ptr_copy = target_data_ptr;
	idx_t copied_copy = copied;
	// Flush row data
	FlushRows(source_data_ptr, source_entry_idx_copy, source_count, target_data_block, target_data_ptr_copy, row_width,
	          copied_copy, count);
	const idx_t flushed = copied_copy - copied;
	// Compute the entry sizes and number of heap bytes that will be copied
	idx_t copy_bytes = 0;
	data_ptr_t source_heap_ptr_copy = source_heap_ptr;
	for (idx_t i = 0; i < flushed; i++) {
		// Store base heap offset in the row data
		Store<idx_t>(target_heap_block.byte_offset + copy_bytes, target_data_ptr + heap_pointer_offset);
		target_data_ptr += row_width;
		// Compute entry size and add to total
		auto entry_size = Load<uint32_t>(source_heap_ptr_copy);
		D_ASSERT(entry_size >= sizeof(uint32_t));
		source_heap_ptr_copy += entry_size;
		copy_bytes += entry_size;
	}
	// Reallocate result heap block size (if needed)
	if (target_heap_block.byte_offset + copy_bytes > target_heap_block.capacity) {
		idx_t new_capacity = target_heap_block.byte_offset + copy_bytes;
		buffer_manager.ReAllocate(target_heap_block.block, new_capacity);
		target_heap_block.capacity = new_capacity;
		target_heap_ptr = target_heap_handle.Ptr() + target_heap_block.byte_offset;
	}
	D_ASSERT(target_heap_block.byte_offset + copy_bytes <= target_heap_block.capacity);
	// Copy the heap data in one go
	memcpy(target_heap_ptr, source_heap_ptr, copy_bytes);
	target_heap_ptr += copy_bytes;
	source_heap_ptr += copy_bytes;
	source_entry_idx += flushed;
	copied += flushed;
	// Update result indices and pointers
	target_heap_block.count += flushed;
	target_heap_block.byte_offset += copy_bytes;
	D_ASSERT(target_heap_block.byte_offset <= target_heap_block.capacity);
}

} // namespace duckdb







#include <numeric>

namespace duckdb {

PartitionGlobalHashGroup::PartitionGlobalHashGroup(BufferManager &buffer_manager, const Orders &partitions,
                                                   const Orders &orders, const Types &payload_types, bool external)
    : count(0), batch_base(0) {

	RowLayout payload_layout;
	payload_layout.Initialize(payload_types);
	global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
	global_sort->external = external;

	//	Set up a comparator for the partition subset
	partition_layout = global_sort->sort_layout.GetPrefixComparisonLayout(partitions.size());
}

int PartitionGlobalHashGroup::ComparePartitions(const SBIterator &left, const SBIterator &right) const {
	int part_cmp = 0;
	if (partition_layout.all_constant) {
		part_cmp = FastMemcmp(left.entry_ptr, right.entry_ptr, partition_layout.comparison_size);
	} else {
		part_cmp = Comparators::CompareTuple(left.scan, right.scan, left.entry_ptr, right.entry_ptr, partition_layout,
		                                     left.external);
	}
	return part_cmp;
}

void PartitionGlobalHashGroup::ComputeMasks(ValidityMask &partition_mask, ValidityMask &order_mask) {
	D_ASSERT(count > 0);

	SBIterator prev(*global_sort, ExpressionType::COMPARE_LESSTHAN);
	SBIterator curr(*global_sort, ExpressionType::COMPARE_LESSTHAN);

	partition_mask.SetValidUnsafe(0);
	order_mask.SetValidUnsafe(0);
	for (++curr; curr.GetIndex() < count; ++curr) {
		//	Compare the partition subset first because if that differs, then so does the full ordering
		const auto part_cmp = ComparePartitions(prev, curr);
		;

		if (part_cmp) {
			partition_mask.SetValidUnsafe(curr.GetIndex());
			order_mask.SetValidUnsafe(curr.GetIndex());
		} else if (prev.Compare(curr)) {
			order_mask.SetValidUnsafe(curr.GetIndex());
		}
		++prev;
	}
}

void PartitionGlobalSinkState::GenerateOrderings(Orders &partitions, Orders &orders,
                                                 const vector<unique_ptr<Expression>> &partition_bys,
                                                 const Orders &order_bys,
                                                 const vector<unique_ptr<BaseStatistics>> &partition_stats) {

	// we sort by both 1) partition by expression list and 2) order by expressions
	const auto partition_cols = partition_bys.size();
	for (idx_t prt_idx = 0; prt_idx < partition_cols; prt_idx++) {
		auto &pexpr = partition_bys[prt_idx];

		if (partition_stats.empty() || !partition_stats[prt_idx]) {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(), nullptr);
		} else {
			orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(),
			                    partition_stats[prt_idx]->ToUnique());
		}
		partitions.emplace_back(orders.back().Copy());
	}

	for (const auto &order : order_bys) {
		orders.emplace_back(order.Copy());
	}
}

PartitionGlobalSinkState::PartitionGlobalSinkState(ClientContext &context,
                                                   const vector<unique_ptr<Expression>> &partition_bys,
                                                   const vector<BoundOrderByNode> &order_bys,
                                                   const Types &payload_types,
                                                   const vector<unique_ptr<BaseStatistics>> &partition_stats,
                                                   idx_t estimated_cardinality)
    : context(context), buffer_manager(BufferManager::GetBufferManager(context)), allocator(Allocator::Get(context)),
      fixed_bits(0), payload_types(payload_types), memory_per_thread(0), max_bits(1), count(0) {

	GenerateOrderings(partitions, orders, partition_bys, order_bys, partition_stats);

	memory_per_thread = PhysicalOperator::GetMaxThreadMemory(context);
	external = ClientConfig::GetConfig(context).force_external;

	const auto thread_pages = PreviousPowerOfTwo(memory_per_thread / (4 * idx_t(Storage::BLOCK_ALLOC_SIZE)));
	while (max_bits < 10 && (thread_pages >> max_bits) > 1) {
		++max_bits;
	}

	if (!orders.empty()) {
		if (partitions.empty()) {
			//	Sort early into a dedicated hash group if we only sort.
			grouping_types.Initialize(payload_types);
			auto new_group =
			    make_uniq<PartitionGlobalHashGroup>(buffer_manager, partitions, orders, payload_types, external);
			hash_groups.emplace_back(std::move(new_group));
		} else {
			auto types = payload_types;
			types.push_back(LogicalType::HASH);
			grouping_types.Initialize(types);
			ResizeGroupingData(estimated_cardinality);
		}
	}
}

bool PartitionGlobalSinkState::HasMergeTasks() const {
	if (grouping_data) {
		auto &groups = grouping_data->GetPartitions();
		return !groups.empty();
	} else if (!hash_groups.empty()) {
		D_ASSERT(hash_groups.size() == 1);
		return hash_groups[0]->count > 0;
	} else {
		return false;
	}
}

void PartitionGlobalSinkState::SyncPartitioning(const PartitionGlobalSinkState &other) {
	fixed_bits = other.grouping_data ? other.grouping_data->GetRadixBits() : 0;

	const auto old_bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	if (fixed_bits != old_bits) {
		const auto hash_col_idx = payload_types.size();
		grouping_data = make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types, fixed_bits, hash_col_idx);
	}
}

unique_ptr<RadixPartitionedTupleData> PartitionGlobalSinkState::CreatePartition(idx_t new_bits) const {
	const auto hash_col_idx = payload_types.size();
	return make_uniq<RadixPartitionedTupleData>(buffer_manager, grouping_types, new_bits, hash_col_idx);
}

void PartitionGlobalSinkState::ResizeGroupingData(idx_t cardinality) {
	//	Have we started to combine? Then just live with it.
	if (fixed_bits || (grouping_data && !grouping_data->GetPartitions().empty())) {
		return;
	}
	//	Is the average partition size too large?
	const idx_t partition_size = STANDARD_ROW_GROUPS_SIZE;
	const auto bits = grouping_data ? grouping_data->GetRadixBits() : 0;
	auto new_bits = bits ? bits : 4;
	while (new_bits < max_bits && (cardinality / RadixPartitioning::NumberOfPartitions(new_bits)) > partition_size) {
		++new_bits;
	}

	// Repartition the grouping data
	if (new_bits != bits) {
		grouping_data = CreatePartition(new_bits);
	}
}

void PartitionGlobalSinkState::SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// We are done if the local_partition is right sized.
	auto &local_radix = local_partition->Cast<RadixPartitionedTupleData>();
	const auto new_bits = grouping_data->GetRadixBits();
	if (local_radix.GetRadixBits() == new_bits) {
		return;
	}

	// If the local partition is now too small, flush it and reallocate
	auto new_partition = CreatePartition(new_bits);
	local_partition->FlushAppendState(*local_append);
	local_partition->Repartition(*new_partition);

	local_partition = std::move(new_partition);
	local_append = make_uniq<PartitionedTupleDataAppendState>();
	local_partition->InitializeAppendState(*local_append);
}

void PartitionGlobalSinkState::UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	// Make sure grouping_data doesn't change under us.
	lock_guard<mutex> guard(lock);

	if (!local_partition) {
		local_partition = CreatePartition(grouping_data->GetRadixBits());
		local_append = make_uniq<PartitionedTupleDataAppendState>();
		local_partition->InitializeAppendState(*local_append);
		return;
	}

	// 	Grow the groups if they are too big
	ResizeGroupingData(count);

	//	Sync local partition to have the same bit count
	SyncLocalPartition(local_partition, local_append);
}

void PartitionGlobalSinkState::CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append) {
	if (!local_partition) {
		return;
	}
	local_partition->FlushAppendState(*local_append);

	// Make sure grouping_data doesn't change under us.
	// Combine has an internal mutex, so this is single-threaded anyway.
	lock_guard<mutex> guard(lock);
	SyncLocalPartition(local_partition, local_append);
	grouping_data->Combine(*local_partition);
}

PartitionLocalMergeState::PartitionLocalMergeState(PartitionGlobalSinkState &gstate)
    : merge_state(nullptr), stage(PartitionSortStage::INIT), finished(true), executor(gstate.context) {

	//	 Set up the sort expression computation.
	vector<LogicalType> sort_types;
	for (auto &order : gstate.orders) {
		auto &oexpr = order.expression;
		sort_types.emplace_back(oexpr->return_type);
		executor.AddExpression(*oexpr);
	}
	sort_chunk.Initialize(gstate.allocator, sort_types);
	payload_chunk.Initialize(gstate.allocator, gstate.payload_types);
}

void PartitionLocalMergeState::Scan() {
	if (!merge_state->group_data) {
		//	OVER(ORDER BY...)
		//	Already sorted
		return;
	}

	auto &group_data = *merge_state->group_data;
	auto &hash_group = *merge_state->hash_group;
	auto &chunk_state = merge_state->chunk_state;
	// Copy the data from the group into the sort code.
	auto &global_sort = *hash_group.global_sort;
	LocalSortState local_sort;
	local_sort.Initialize(global_sort, global_sort.buffer_manager);

	TupleDataScanState local_scan;
	group_data.InitializeScan(local_scan, merge_state->column_ids);
	while (group_data.Scan(chunk_state, local_scan, payload_chunk)) {
		sort_chunk.Reset();
		executor.Execute(payload_chunk, sort_chunk);

		local_sort.SinkChunk(sort_chunk, payload_chunk);
		if (local_sort.SizeInBytes() > merge_state->memory_per_thread) {
			local_sort.Sort(global_sort, true);
		}
		hash_group.count += payload_chunk.size();
	}

	global_sort.AddLocalState(local_sort);
}

//	Per-thread sink state
PartitionLocalSinkState::PartitionLocalSinkState(ClientContext &context, PartitionGlobalSinkState &gstate_p)
    : gstate(gstate_p), allocator(Allocator::Get(context)), executor(context) {

	vector<LogicalType> group_types;
	for (idx_t prt_idx = 0; prt_idx < gstate.partitions.size(); prt_idx++) {
		auto &pexpr = *gstate.partitions[prt_idx].expression.get();
		group_types.push_back(pexpr.return_type);
		executor.AddExpression(pexpr);
	}
	sort_cols = gstate.orders.size() + group_types.size();

	if (sort_cols) {
		auto payload_types = gstate.payload_types;
		if (!group_types.empty()) {
			// OVER(PARTITION BY...)
			group_chunk.Initialize(allocator, group_types);
			payload_types.emplace_back(LogicalType::HASH);
		} else {
			// OVER(ORDER BY...)
			for (idx_t ord_idx = 0; ord_idx < gstate.orders.size(); ord_idx++) {
				auto &pexpr = *gstate.orders[ord_idx].expression.get();
				group_types.push_back(pexpr.return_type);
				executor.AddExpression(pexpr);
			}
			group_chunk.Initialize(allocator, group_types);

			//	Single partition
			auto &global_sort = *gstate.hash_groups[0]->global_sort;
			local_sort = make_uniq<LocalSortState>();
			local_sort->Initialize(global_sort, global_sort.buffer_manager);
		}
		// OVER(...)
		payload_chunk.Initialize(allocator, payload_types);
	} else {
		// OVER()
		payload_layout.Initialize(gstate.payload_types);
	}
}

void PartitionLocalSinkState::Hash(DataChunk &input_chunk, Vector &hash_vector) {
	const auto count = input_chunk.size();
	D_ASSERT(group_chunk.ColumnCount() > 0);

	// OVER(PARTITION BY...) (hash grouping)
	group_chunk.Reset();
	executor.Execute(input_chunk, group_chunk);
	VectorOperations::Hash(group_chunk.data[0], hash_vector, count);
	for (idx_t prt_idx = 1; prt_idx < group_chunk.ColumnCount(); ++prt_idx) {
		VectorOperations::CombineHash(hash_vector, group_chunk.data[prt_idx], count);
	}
}

void PartitionLocalSinkState::Sink(DataChunk &input_chunk) {
	gstate.count += input_chunk.size();

	// OVER()
	if (sort_cols == 0) {
		//	No sorts, so build paged row chunks
		if (!rows) {
			const auto entry_size = payload_layout.GetRowWidth();
			const auto capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_SIZE / entry_size) + 1);
			rows = make_uniq<RowDataCollection>(gstate.buffer_manager, capacity, entry_size);
			strings = make_uniq<RowDataCollection>(gstate.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
		}
		const auto row_count = input_chunk.size();
		const auto row_sel = FlatVector::IncrementalSelectionVector();
		Vector addresses(LogicalType::POINTER);
		auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
		const auto prev_rows_blocks = rows->blocks.size();
		auto handles = rows->Build(row_count, key_locations, nullptr, row_sel);
		auto input_data = input_chunk.ToUnifiedFormat();
		RowOperations::Scatter(input_chunk, input_data.get(), payload_layout, addresses, *strings, *row_sel, row_count);
		// Mark that row blocks contain pointers (heap blocks are pinned)
		if (!payload_layout.AllConstant()) {
			D_ASSERT(strings->keep_pinned);
			for (size_t i = prev_rows_blocks; i < rows->blocks.size(); ++i) {
				rows->blocks[i]->block->SetSwizzling("PartitionLocalSinkState::Sink");
			}
		}
		return;
	}

	if (local_sort) {
		//	OVER(ORDER BY...)
		group_chunk.Reset();
		executor.Execute(input_chunk, group_chunk);
		local_sort->SinkChunk(group_chunk, input_chunk);

		auto &hash_group = *gstate.hash_groups[0];
		hash_group.count += input_chunk.size();

		if (local_sort->SizeInBytes() > gstate.memory_per_thread) {
			auto &global_sort = *hash_group.global_sort;
			local_sort->Sort(global_sort, true);
		}
		return;
	}

	// OVER(...)
	payload_chunk.Reset();
	auto &hash_vector = payload_chunk.data.back();
	Hash(input_chunk, hash_vector);
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}
	payload_chunk.SetCardinality(input_chunk);

	gstate.UpdateLocalPartition(local_partition, local_append);
	local_partition->Append(*local_append, payload_chunk);
}

void PartitionLocalSinkState::Combine() {
	// OVER()
	if (sort_cols == 0) {
		// Only one partition again, so need a global lock.
		lock_guard<mutex> glock(gstate.lock);
		if (gstate.rows) {
			if (rows) {
				gstate.rows->Merge(*rows);
				gstate.strings->Merge(*strings);
				rows.reset();
				strings.reset();
			}
		} else {
			gstate.rows = std::move(rows);
			gstate.strings = std::move(strings);
		}
		return;
	}

	if (local_sort) {
		//	OVER(ORDER BY...)
		auto &hash_group = *gstate.hash_groups[0];
		auto &global_sort = *hash_group.global_sort;
		global_sort.AddLocalState(*local_sort);
		local_sort.reset();
		return;
	}

	// OVER(...)
	gstate.CombineLocalPartition(local_partition, local_append);
}

PartitionGlobalMergeState::PartitionGlobalMergeState(PartitionGlobalSinkState &sink, GroupDataPtr group_data_p,
                                                     hash_t hash_bin)
    : sink(sink), group_data(std::move(group_data_p)), memory_per_thread(sink.memory_per_thread),
      num_threads(TaskScheduler::GetScheduler(sink.context).NumberOfThreads()), stage(PartitionSortStage::INIT),
      total_tasks(0), tasks_assigned(0), tasks_completed(0) {

	const auto group_idx = sink.hash_groups.size();
	auto new_group = make_uniq<PartitionGlobalHashGroup>(sink.buffer_manager, sink.partitions, sink.orders,
	                                                     sink.payload_types, sink.external);
	sink.hash_groups.emplace_back(std::move(new_group));

	hash_group = sink.hash_groups[group_idx].get();
	global_sort = sink.hash_groups[group_idx]->global_sort.get();

	sink.bin_groups[hash_bin] = group_idx;

	column_ids.reserve(sink.payload_types.size());
	for (column_t i = 0; i < sink.payload_types.size(); ++i) {
		column_ids.emplace_back(i);
	}
	group_data->InitializeScan(chunk_state, column_ids);
}

PartitionGlobalMergeState::PartitionGlobalMergeState(PartitionGlobalSinkState &sink)
    : sink(sink), memory_per_thread(sink.memory_per_thread),
      num_threads(TaskScheduler::GetScheduler(sink.context).NumberOfThreads()), stage(PartitionSortStage::INIT),
      total_tasks(0), tasks_assigned(0), tasks_completed(0) {

	const hash_t hash_bin = 0;
	const size_t group_idx = 0;
	hash_group = sink.hash_groups[group_idx].get();
	global_sort = sink.hash_groups[group_idx]->global_sort.get();

	sink.bin_groups[hash_bin] = group_idx;
}

void PartitionLocalMergeState::Prepare() {
	merge_state->group_data.reset();

	auto &global_sort = *merge_state->global_sort;
	global_sort.PrepareMergePhase();
}

void PartitionLocalMergeState::Merge() {
	auto &global_sort = *merge_state->global_sort;
	MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
	merge_sorter.PerformInMergeRound();
}

void PartitionLocalMergeState::ExecuteTask() {
	switch (stage) {
	case PartitionSortStage::SCAN:
		Scan();
		break;
	case PartitionSortStage::PREPARE:
		Prepare();
		break;
	case PartitionSortStage::MERGE:
		Merge();
		break;
	default:
		throw InternalException("Unexpected PartitionSortStage in ExecuteTask!");
	}

	merge_state->CompleteTask();
	finished = true;
}

bool PartitionGlobalMergeState::AssignTask(PartitionLocalMergeState &local_state) {
	lock_guard<mutex> guard(lock);

	if (tasks_assigned >= total_tasks) {
		return false;
	}

	local_state.merge_state = this;
	local_state.stage = stage;
	local_state.finished = false;
	tasks_assigned++;

	return true;
}

void PartitionGlobalMergeState::CompleteTask() {
	lock_guard<mutex> guard(lock);

	++tasks_completed;
}

bool PartitionGlobalMergeState::TryPrepareNextStage() {
	lock_guard<mutex> guard(lock);

	if (tasks_completed < total_tasks) {
		return false;
	}

	tasks_assigned = tasks_completed = 0;

	switch (stage) {
	case PartitionSortStage::INIT:
		//	If the partitions are unordered, don't scan in parallel
		//	because it produces non-deterministic orderings.
		//	This can theoretically happen with ORDER BY,
		//	but that is something the query should be explicit about.
		total_tasks = sink.orders.size() > sink.partitions.size() ? num_threads : 1;
		stage = PartitionSortStage::SCAN;
		return true;

	case PartitionSortStage::SCAN:
		total_tasks = 1;
		stage = PartitionSortStage::PREPARE;
		return true;

	case PartitionSortStage::PREPARE:
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		stage = PartitionSortStage::MERGE;
		global_sort->InitializeMergeRound();
		return true;

	case PartitionSortStage::MERGE:
		global_sort->CompleteMergeRound(true);
		total_tasks = global_sort->sorted_blocks.size() / 2;
		if (!total_tasks) {
			break;
		}
		global_sort->InitializeMergeRound();
		return true;

	case PartitionSortStage::SORTED:
		break;
	}

	stage = PartitionSortStage::SORTED;

	return false;
}

PartitionGlobalMergeStates::PartitionGlobalMergeStates(PartitionGlobalSinkState &sink) {
	// Schedule all the sorts for maximum thread utilisation
	if (sink.grouping_data) {
		auto &partitions = sink.grouping_data->GetPartitions();
		sink.bin_groups.resize(partitions.size(), partitions.size());
		for (hash_t hash_bin = 0; hash_bin < partitions.size(); ++hash_bin) {
			auto &group_data = partitions[hash_bin];
			// Prepare for merge sort phase
			if (group_data->Count()) {
				auto state = make_uniq<PartitionGlobalMergeState>(sink, std::move(group_data), hash_bin);
				states.emplace_back(std::move(state));
			}
		}
	} else {
		//	OVER(ORDER BY...)
		//	Already sunk into the single global sort, so set up single merge with no data
		sink.bin_groups.resize(1, 1);
		auto state = make_uniq<PartitionGlobalMergeState>(sink);
		states.emplace_back(std::move(state));
	}
}

class PartitionMergeTask : public ExecutorTask {
public:
	PartitionMergeTask(shared_ptr<Event> event_p, ClientContext &context_p, PartitionGlobalMergeStates &hash_groups_p,
	                   PartitionGlobalSinkState &gstate)
	    : ExecutorTask(context_p), event(std::move(event_p)), local_state(gstate), hash_groups(hash_groups_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	struct ExecutorCallback : public PartitionGlobalMergeStates::Callback {
		explicit ExecutorCallback(Executor &executor) : executor(executor) {
		}

		bool HasError() const override {
			return executor.HasError();
		}

		Executor &executor;
	};

	shared_ptr<Event> event;
	PartitionLocalMergeState local_state;
	PartitionGlobalMergeStates &hash_groups;
};

bool PartitionGlobalMergeStates::ExecuteTask(PartitionLocalMergeState &local_state, Callback &callback) {
	// Loop until all hash groups are done
	size_t sorted = 0;
	while (sorted < states.size()) {
		// First check if there is an unfinished task for this thread
		if (callback.HasError()) {
			return false;
		}
		if (!local_state.TaskFinished()) {
			local_state.ExecuteTask();
			continue;
		}

		// Thread is done with its assigned task, try to fetch new work
		for (auto group = sorted; group < states.size(); ++group) {
			auto &global_state = states[group];
			if (global_state->IsSorted()) {
				// This hash group is done
				// Update the high water mark of densely completed groups
				if (sorted == group) {
					++sorted;
				}
				continue;
			}

			// Try to assign work for this hash group to this thread
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// Hash group global state couldn't assign a task to this thread
			// Try to prepare the next stage
			if (!global_state->TryPrepareNextStage()) {
				// This current hash group is not yet done
				// But we were not able to assign a task for it to this thread
				// See if the next hash group is better
				continue;
			}

			// We were able to prepare the next stage for this hash group!
			// Try to assign a task once more
			if (global_state->AssignTask(local_state)) {
				// We assigned a task to this thread!
				// Break out of this loop to re-enter the top-level loop and execute the task
				break;
			}

			// We were able to prepare the next merge round,
			// but we were not able to assign a task for it to this thread
			// The tasks were assigned to other threads while this thread waited for the lock
			// Go to the next iteration to see if another hash group has a task
		}
	}

	return true;
}

TaskExecutionResult PartitionMergeTask::ExecuteTask(TaskExecutionMode mode) {
	ExecutorCallback callback(executor);

	if (!hash_groups.ExecuteTask(local_state, callback)) {
		return TaskExecutionResult::TASK_ERROR;
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void PartitionMergeEvent::Schedule() {
	auto &context = pipeline->GetClientContext();

	// Schedule tasks equal to the number of threads, which will each merge multiple partitions
	auto &ts = TaskScheduler::GetScheduler(context);
	idx_t num_threads = ts.NumberOfThreads();

	vector<shared_ptr<Task>> merge_tasks;
	for (idx_t tnum = 0; tnum < num_threads; tnum++) {
		merge_tasks.emplace_back(make_uniq<PartitionMergeTask>(shared_from_this(), context, merge_states, gstate));
	}
	SetTasks(std::move(merge_tasks));
}

} // namespace duckdb





namespace duckdb {

//! Calls std::sort on strings that are tied by their prefix after the radix sort
static void SortTiedBlobs(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start, const idx_t &end,
                          const idx_t &tie_col, bool *ties, const data_ptr_t blob_ptr, const SortLayout &sort_layout) {
	const auto row_width = sort_layout.blob_layout.GetRowWidth();
	// Locate the first blob row in question
	data_ptr_t row_ptr = dataptr + start * sort_layout.entry_size;
	data_ptr_t blob_row_ptr = blob_ptr + Load<uint32_t>(row_ptr + sort_layout.comparison_size) * row_width;
	if (!Comparators::TieIsBreakable(tie_col, blob_row_ptr, sort_layout)) {
		// Quick check to see if ties can be broken
		return;
	}
	// Fill pointer array for sorting
	auto ptr_block = make_unsafe_uniq_array<data_ptr_t>(end - start);
	auto entry_ptrs = (data_ptr_t *)ptr_block.get();
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = row_ptr;
		row_ptr += sort_layout.entry_size;
	}
	// Slow pointer-based sorting
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	auto logical_type = sort_layout.blob_layout.GetTypes()[col_idx];
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&blob_ptr, &order, &sort_layout, &tie_col_offset, &row_width, &logical_type](const data_ptr_t l,
	                                                                                        const data_ptr_t r) {
		          idx_t left_idx = Load<uint32_t>(l + sort_layout.comparison_size);
		          idx_t right_idx = Load<uint32_t>(r + sort_layout.comparison_size);
		          data_ptr_t left_ptr = blob_ptr + left_idx * row_width + tie_col_offset;
		          data_ptr_t right_ptr = blob_ptr + right_idx * row_width + tie_col_offset;
		          return order * Comparators::CompareVal(left_ptr, right_ptr, logical_type) < 0;
	          });
	// Re-order
	auto temp_block = buffer_manager.GetBufferAllocator().Allocate((end - start) * sort_layout.entry_size);
	data_ptr_t temp_ptr = temp_block.get();
	for (idx_t i = 0; i < end - start; i++) {
		FastMemcpy(temp_ptr, entry_ptrs[i], sort_layout.entry_size);
		temp_ptr += sort_layout.entry_size;
	}
	memcpy(dataptr + start * sort_layout.entry_size, temp_block.get(), (end - start) * sort_layout.entry_size);
	// Determine if there are still ties (if this is not the last column)
	if (tie_col < sort_layout.column_count - 1) {
		data_ptr_t idx_ptr = dataptr + start * sort_layout.entry_size + sort_layout.comparison_size;
		// Load current entry
		data_ptr_t current_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
		for (idx_t i = 0; i < end - start - 1; i++) {
			// Load next entry and compare
			idx_ptr += sort_layout.entry_size;
			data_ptr_t next_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
			ties[start + i] = Comparators::CompareVal(current_ptr, next_ptr, logical_type) == 0;
			current_ptr = next_ptr;
		}
	}
}

//! Identifies sequences of rows that are tied by the prefix of a blob column, and sorts them
static void SortTiedBlobs(BufferManager &buffer_manager, SortedBlock &sb, bool *ties, data_ptr_t dataptr,
                          const idx_t &count, const idx_t &tie_col, const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	auto &blob_block = *sb.blob_sorting_data->data_blocks.back();
	auto blob_handle = buffer_manager.Pin(blob_block.block);
	const data_ptr_t blob_ptr = blob_handle.Ptr();

	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		SortTiedBlobs(buffer_manager, dataptr, i, j + 1, tie_col, ties, blob_ptr, sort_layout);
		i = j;
	}
}

//! Returns whether there are any 'true' values in the ties[] array
static bool AnyTies(bool ties[], const idx_t &count) {
	D_ASSERT(!ties[count - 1]);
	bool any_ties = false;
	for (idx_t i = 0; i < count - 1; i++) {
		any_ties = any_ties || ties[i];
	}
	return any_ties;
}

//! Compares subsequent rows to check for ties
static void ComputeTies(data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset, const idx_t &tie_size,
                        bool ties[], const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	D_ASSERT(col_offset + tie_size <= sort_layout.comparison_size);
	// Align dataptr
	dataptr += col_offset;
	for (idx_t i = 0; i < count - 1; i++) {
		ties[i] = ties[i] && FastMemcmp(dataptr, dataptr + sort_layout.entry_size, tie_size) == 0;
		dataptr += sort_layout.entry_size;
	}
}

//! Textbook LSD radix sort
void RadixSortLSD(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &sorting_size) {
	auto temp_block = buffer_manager.GetBufferAllocator().Allocate(count * row_width);
	bool swap = false;

	idx_t counts[SortConstants::VALUES_PER_RADIX];
	for (idx_t r = 1; r <= sorting_size; r++) {
		// Init counts to 0
		memset(counts, 0, sizeof(counts));
		// Const some values for convenience
		const data_ptr_t source_ptr = swap ? temp_block.get() : dataptr;
		const data_ptr_t target_ptr = swap ? dataptr : temp_block.get();
		const idx_t offset = col_offset + sorting_size - r;
		// Collect counts
		data_ptr_t offset_ptr = source_ptr + offset;
		for (idx_t i = 0; i < count; i++) {
			counts[*offset_ptr]++;
			offset_ptr += row_width;
		}
		// Compute offsets from counts
		idx_t max_count = counts[0];
		for (idx_t val = 1; val < SortConstants::VALUES_PER_RADIX; val++) {
			max_count = MaxValue<idx_t>(max_count, counts[val]);
			counts[val] = counts[val] + counts[val - 1];
		}
		if (max_count == count) {
			continue;
		}
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr + (count - 1) * row_width;
		for (idx_t i = 0; i < count; i++) {
			idx_t &radix_offset = --counts[*(row_ptr + offset)];
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr -= row_width;
		}
		swap = !swap;
	}
	// Move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(dataptr, temp_block.get(), count * row_width);
	}
}

//! Insertion sort, used when count of values is low
inline void InsertionSort(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count,
                          const idx_t &col_offset, const idx_t &row_width, const idx_t &total_comp_width,
                          const idx_t &offset, bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	if (count > 1) {
		const idx_t total_offset = col_offset + offset;
		auto temp_val = make_unsafe_uniq_array<data_t>(row_width);
		const data_ptr_t val = temp_val.get();
		const auto comp_width = total_comp_width - offset;
		for (idx_t i = 1; i < count; i++) {
			FastMemcpy(val, source_ptr + i * row_width, row_width);
			idx_t j = i;
			while (j > 0 &&
			       FastMemcmp(source_ptr + (j - 1) * row_width + total_offset, val + total_offset, comp_width) > 0) {
				FastMemcpy(source_ptr + j * row_width, source_ptr + (j - 1) * row_width, row_width);
				j--;
			}
			FastMemcpy(source_ptr + j * row_width, val, row_width);
		}
	}
	if (swap) {
		memcpy(target_ptr, source_ptr, count * row_width);
	}
}

//! MSD radix sort that switches to insertion sort with low bucket sizes
void RadixSortMSD(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &comp_width, const idx_t &offset, idx_t locations[], bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	// Init counts to 0
	memset(locations, 0, SortConstants::MSD_RADIX_LOCATIONS * sizeof(idx_t));
	idx_t *counts = locations + 1;
	// Collect counts
	const idx_t total_offset = col_offset + offset;
	data_ptr_t offset_ptr = source_ptr + total_offset;
	for (idx_t i = 0; i < count; i++) {
		counts[*offset_ptr]++;
		offset_ptr += row_width;
	}
	// Compute locations from counts
	idx_t max_count = 0;
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		max_count = MaxValue<idx_t>(max_count, counts[radix]);
		counts[radix] += locations[radix];
	}
	if (max_count != count) {
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr;
		for (idx_t i = 0; i < count; i++) {
			const idx_t &radix_offset = locations[*(row_ptr + total_offset)]++;
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr += row_width;
		}
		swap = !swap;
	}
	// Check if done
	if (offset == comp_width - 1) {
		if (swap) {
			memcpy(orig_ptr, temp_ptr, count * row_width);
		}
		return;
	}
	if (max_count == count) {
		RadixSortMSD(orig_ptr, temp_ptr, count, col_offset, row_width, comp_width, offset + 1,
		             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		return;
	}
	// Recurse
	idx_t radix_count = locations[0];
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		const idx_t loc = (locations[radix] - radix_count) * row_width;
		if (radix_count > SortConstants::INSERTION_SORT_THRESHOLD) {
			RadixSortMSD(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		} else if (radix_count != 0) {
			InsertionSort(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			              swap);
		}
		radix_count = locations[radix + 1] - locations[radix];
	}
}

//! Calls different sort functions, depending on the count and sorting sizes
void RadixSort(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
               const idx_t &sorting_size, const SortLayout &sort_layout, bool contains_string) {
	if (contains_string) {
		auto begin = duckdb_pdqsort::PDQIterator(dataptr, sort_layout.entry_size);
		auto end = begin + count;
		duckdb_pdqsort::PDQConstants constants(sort_layout.entry_size, col_offset, sorting_size, *end);
		duckdb_pdqsort::pdqsort_branchless(begin, begin + count, constants);
	} else if (count <= SortConstants::INSERTION_SORT_THRESHOLD) {
		InsertionSort(dataptr, nullptr, count, 0, sort_layout.entry_size, sort_layout.comparison_size, 0, false);
	} else if (sorting_size <= SortConstants::MSD_RADIX_SORT_SIZE_THRESHOLD) {
		RadixSortLSD(buffer_manager, dataptr, count, col_offset, sort_layout.entry_size, sorting_size);
	} else {
		auto temp_block = buffer_manager.Allocate(MaxValue(count * sort_layout.entry_size, (idx_t)Storage::BLOCK_SIZE));
		auto preallocated_array = make_unsafe_uniq_array<idx_t>(sorting_size * SortConstants::MSD_RADIX_LOCATIONS);
		RadixSortMSD(dataptr, temp_block.Ptr(), count, col_offset, sort_layout.entry_size, sorting_size, 0,
		             preallocated_array.get(), false);
	}
}

//! Identifies sequences of rows that are tied, and calls radix sort on these
static void SubSortTiedTuples(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &count,
                              const idx_t &col_offset, const idx_t &sorting_size, bool ties[],
                              const SortLayout &sort_layout, bool contains_string) {
	D_ASSERT(!ties[count - 1]);
	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i + 1; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		RadixSort(buffer_manager, dataptr + i * sort_layout.entry_size, j - i + 1, col_offset, sorting_size,
		          sort_layout, contains_string);
		i = j;
	}
}

void LocalSortState::SortInMemory() {
	auto &sb = *sorted_blocks.back();
	auto &block = *sb.radix_sorting_data.back();
	const auto &count = block.count;
	auto handle = buffer_manager->Pin(block.block);
	const auto dataptr = handle.Ptr();
	// Assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sort_layout->comparison_size;
	for (uint32_t i = 0; i < count; i++) {
		Store<uint32_t>(i, idx_dataptr);
		idx_dataptr += sort_layout->entry_size;
	}
	// Radix sort and break ties until no more ties, or until all columns are sorted
	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unsafe_unique_array<bool> ties_ptr;
	bool *ties = nullptr;
	bool contains_string = false;
	for (idx_t i = 0; i < sort_layout->column_count; i++) {
		sorting_size += sort_layout->column_sizes[i];
		contains_string = contains_string || sort_layout->logical_types[i].InternalType() == PhysicalType::VARCHAR;
		if (sort_layout->constant_size[i] && i < sort_layout->column_count - 1) {
			// Add columns to the sorting size until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// This is the first sort
			RadixSort(*buffer_manager, dataptr, count, col_offset, sorting_size, *sort_layout, contains_string);
			ties_ptr = make_unsafe_uniq_array<bool>(count);
			ties = ties_ptr.get();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// For subsequent sorts, we only have to subsort the tied tuples
			SubSortTiedTuples(*buffer_manager, dataptr, count, col_offset, sorting_size, ties, *sort_layout,
			                  contains_string);
		}

		contains_string = false;

		if (sort_layout->constant_size[i] && i == sort_layout->column_count - 1) {
			// All columns are sorted, no ties to break because last column is constant size
			break;
		}

		ComputeTies(dataptr, count, col_offset, sorting_size, ties, *sort_layout);
		if (!AnyTies(ties, count)) {
			// No ties, stop sorting
			break;
		}

		if (!sort_layout->constant_size[i]) {
			SortTiedBlobs(*buffer_manager, sb, ties, dataptr, count, i, *sort_layout);
			if (!AnyTies(ties, count)) {
				// No more ties after tie-breaking, stop
				break;
			}
		}

		col_offset += sorting_size;
		sorting_size = 0;
	}
}

} // namespace duckdb






#include <algorithm>
#include <numeric>

namespace duckdb {

idx_t GetNestedSortingColSize(idx_t &col_size, const LogicalType &type) {
	auto physical_type = type.InternalType();
	if (TypeIsConstantSize(physical_type)) {
		col_size += GetTypeIdSize(physical_type);
		return 0;
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR: {
			// Nested strings are between 4 and 11 chars long for alignment
			auto size_before_str = col_size;
			col_size += 11;
			col_size -= (col_size - 12) % 8;
			return col_size - size_before_str;
		}
		case PhysicalType::LIST:
			// Lists get 2 bytes (null and empty list)
			col_size += 2;
			return GetNestedSortingColSize(col_size, ListType::GetChildType(type));
		case PhysicalType::STRUCT:
			// Structs get 1 bytes (null)
			col_size++;
			return GetNestedSortingColSize(col_size, StructType::GetChildType(type, 0));
		default:
			throw NotImplementedException("Unable to order column with type %s", type.ToString());
		}
	}
}

SortLayout::SortLayout(const vector<BoundOrderByNode> &orders)
    : column_count(orders.size()), all_constant(true), comparison_size(0), entry_size(0) {
	vector<LogicalType> blob_layout_types;
	for (idx_t i = 0; i < column_count; i++) {
		const auto &order = orders[i];

		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		auto &expr = *order.expression;
		logical_types.push_back(expr.return_type);

		auto physical_type = expr.return_type.InternalType();
		constant_size.push_back(TypeIsConstantSize(physical_type));

		if (order.stats) {
			stats.push_back(order.stats.get());
			has_null.push_back(stats.back()->CanHaveNull());
		} else {
			stats.push_back(nullptr);
			has_null.push_back(true);
		}

		idx_t col_size = has_null.back() ? 1 : 0;
		prefix_lengths.push_back(0);
		if (!TypeIsConstantSize(physical_type) && physical_type != PhysicalType::VARCHAR) {
			prefix_lengths.back() = GetNestedSortingColSize(col_size, expr.return_type);
		} else if (physical_type == PhysicalType::VARCHAR) {
			idx_t size_before = col_size;
			if (stats.back() && StringStats::HasMaxStringLength(*stats.back())) {
				col_size += StringStats::MaxStringLength(*stats.back());
				if (col_size > 12) {
					col_size = 12;
				} else {
					constant_size.back() = true;
				}
			} else {
				col_size = 12;
			}
			prefix_lengths.back() = col_size - size_before;
		} else {
			col_size += GetTypeIdSize(physical_type);
		}

		comparison_size += col_size;
		column_sizes.push_back(col_size);
	}
	entry_size = comparison_size + sizeof(uint32_t);

	// 8-byte alignment
	if (entry_size % 8 != 0) {
		// First assign more bytes to strings instead of aligning
		idx_t bytes_to_fill = 8 - (entry_size % 8);
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			if (bytes_to_fill == 0) {
				break;
			}
			if (logical_types[col_idx].InternalType() == PhysicalType::VARCHAR && stats[col_idx] &&
			    StringStats::HasMaxStringLength(*stats[col_idx])) {
				idx_t diff = StringStats::MaxStringLength(*stats[col_idx]) - prefix_lengths[col_idx];
				if (diff > 0) {
					// Increase all sizes accordingly
					idx_t increase = MinValue(bytes_to_fill, diff);
					column_sizes[col_idx] += increase;
					prefix_lengths[col_idx] += increase;
					constant_size[col_idx] = increase == diff;
					comparison_size += increase;
					entry_size += increase;
					bytes_to_fill -= increase;
				}
			}
		}
		entry_size = AlignValue(entry_size);
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		all_constant = all_constant && constant_size[col_idx];
		if (!constant_size[col_idx]) {
			sorting_to_blob_col[col_idx] = blob_layout_types.size();
			blob_layout_types.push_back(logical_types[col_idx]);
		}
	}

	blob_layout.Initialize(blob_layout_types);
}

SortLayout SortLayout::GetPrefixComparisonLayout(idx_t num_prefix_cols) const {
	SortLayout result;
	result.column_count = num_prefix_cols;
	result.all_constant = true;
	result.comparison_size = 0;
	for (idx_t col_idx = 0; col_idx < num_prefix_cols; col_idx++) {
		result.order_types.push_back(order_types[col_idx]);
		result.order_by_null_types.push_back(order_by_null_types[col_idx]);
		result.logical_types.push_back(logical_types[col_idx]);

		result.all_constant = result.all_constant && constant_size[col_idx];
		result.constant_size.push_back(constant_size[col_idx]);

		result.comparison_size += column_sizes[col_idx];
		result.column_sizes.push_back(column_sizes[col_idx]);

		result.prefix_lengths.push_back(prefix_lengths[col_idx]);
		result.stats.push_back(stats[col_idx]);
		result.has_null.push_back(has_null[col_idx]);
	}
	result.entry_size = entry_size;
	result.blob_layout = blob_layout;
	result.sorting_to_blob_col = sorting_to_blob_col;
	return result;
}

LocalSortState::LocalSortState() : initialized(false) {
	if (!Radix::IsLittleEndian()) {
		throw NotImplementedException("Sorting is not supported on big endian architectures");
	}
}

void LocalSortState::Initialize(GlobalSortState &global_sort_state, BufferManager &buffer_manager_p) {
	sort_layout = &global_sort_state.sort_layout;
	payload_layout = &global_sort_state.payload_layout;
	buffer_manager = &buffer_manager_p;
	// Radix sorting data
	radix_sorting_data = make_uniq<RowDataCollection>(
	    *buffer_manager, RowDataCollection::EntriesPerBlock(sort_layout->entry_size), sort_layout->entry_size);
	// Blob sorting data
	if (!sort_layout->all_constant) {
		auto blob_row_width = sort_layout->blob_layout.GetRowWidth();
		blob_sorting_data = make_uniq<RowDataCollection>(
		    *buffer_manager, RowDataCollection::EntriesPerBlock(blob_row_width), blob_row_width);
		blob_sorting_heap = make_uniq<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	// Payload data
	auto payload_row_width = payload_layout->GetRowWidth();
	payload_data = make_uniq<RowDataCollection>(*buffer_manager, RowDataCollection::EntriesPerBlock(payload_row_width),
	                                            payload_row_width);
	payload_heap = make_uniq<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	// Init done
	initialized = true;
}

void LocalSortState::SinkChunk(DataChunk &sort, DataChunk &payload) {
	D_ASSERT(sort.size() == payload.size());
	// Build and serialize sorting data to radix sortable rows
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto handles = radix_sorting_data->Build(sort.size(), data_pointers, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sort_layout->has_null[sort_col];
		bool nulls_first = sort_layout->order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sort_layout->order_types[sort_col] == OrderType::DESCENDING;
		RowOperations::RadixScatter(sort.data[sort_col], sort.size(), sel_ptr, sort.size(), data_pointers, desc,
		                            has_null, nulls_first, sort_layout->prefix_lengths[sort_col],
		                            sort_layout->column_sizes[sort_col]);
	}

	// Also fully serialize blob sorting columns (to be able to break ties
	if (!sort_layout->all_constant) {
		DataChunk blob_chunk;
		blob_chunk.SetCardinality(sort.size());
		for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
			if (!sort_layout->constant_size[sort_col]) {
				blob_chunk.data.emplace_back(sort.data[sort_col]);
			}
		}
		handles = blob_sorting_data->Build(blob_chunk.size(), data_pointers, nullptr);
		auto blob_data = blob_chunk.ToUnifiedFormat();
		RowOperations::Scatter(blob_chunk, blob_data.get(), sort_layout->blob_layout, addresses, *blob_sorting_heap,
		                       sel_ptr, blob_chunk.size());
		D_ASSERT(blob_sorting_heap->keep_pinned);
	}

	// Finally, serialize payload data
	handles = payload_data->Build(payload.size(), data_pointers, nullptr);
	auto input_data = payload.ToUnifiedFormat();
	RowOperations::Scatter(payload, input_data.get(), *payload_layout, addresses, *payload_heap, sel_ptr,
	                       payload.size());
	D_ASSERT(payload_heap->keep_pinned);
}

idx_t LocalSortState::SizeInBytes() const {
	idx_t size_in_bytes = radix_sorting_data->SizeInBytes() + payload_data->SizeInBytes();
	if (!sort_layout->all_constant) {
		size_in_bytes += blob_sorting_data->SizeInBytes() + blob_sorting_heap->SizeInBytes();
	}
	if (!payload_layout->AllConstant()) {
		size_in_bytes += payload_heap->SizeInBytes();
	}
	return size_in_bytes;
}

void LocalSortState::Sort(GlobalSortState &global_sort_state, bool reorder_heap) {
	D_ASSERT(radix_sorting_data->count == payload_data->count);
	if (radix_sorting_data->count == 0) {
		return;
	}
	// Move all data to a single SortedBlock
	sorted_blocks.emplace_back(make_uniq<SortedBlock>(*buffer_manager, global_sort_state));
	auto &sb = *sorted_blocks.back();
	// Fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(*radix_sorting_data);
	sb.radix_sorting_data.push_back(std::move(sorting_block));
	// Variable-size sorting data
	if (!sort_layout->all_constant) {
		auto &blob_data = *blob_sorting_data;
		auto new_block = ConcatenateBlocks(blob_data);
		sb.blob_sorting_data->data_blocks.push_back(std::move(new_block));
	}
	// Payload data
	auto payload_block = ConcatenateBlocks(*payload_data);
	sb.payload_data->data_blocks.push_back(std::move(payload_block));
	// Now perform the actual sort
	SortInMemory();
	// Re-order before the merge sort
	ReOrder(global_sort_state, reorder_heap);
}

unique_ptr<RowDataBlock> LocalSortState::ConcatenateBlocks(RowDataCollection &row_data) {
	//	Don't copy and delete if there is only one block.
	if (row_data.blocks.size() == 1) {
		auto new_block = std::move(row_data.blocks[0]);
		row_data.blocks.clear();
		row_data.count = 0;
		return new_block;
	}
	// Create block with the correct capacity
	auto buffer_manager = &row_data.buffer_manager;
	const idx_t &entry_size = row_data.entry_size;
	idx_t capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + entry_size - 1) / entry_size, row_data.count);
	auto new_block = make_uniq<RowDataBlock>(*buffer_manager, capacity, entry_size);
	new_block->count = row_data.count;
	auto new_block_handle = buffer_manager->Pin(new_block->block);
	data_ptr_t new_block_ptr = new_block_handle.Ptr();
	// Copy the data of the blocks into a single block
	for (idx_t i = 0; i < row_data.blocks.size(); i++) {
		auto &block = row_data.blocks[i];
		auto block_handle = buffer_manager->Pin(block->block);
		memcpy(new_block_ptr, block_handle.Ptr(), block->count * entry_size);
		new_block_ptr += block->count * entry_size;
		block.reset();
	}
	row_data.blocks.clear();
	row_data.count = 0;
	return new_block;
}

void LocalSortState::ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate,
                             bool reorder_heap) {
	sd.swizzled = reorder_heap;
	auto &unordered_data_block = sd.data_blocks.back();
	const idx_t count = unordered_data_block->count;
	auto unordered_data_handle = buffer_manager->Pin(unordered_data_block->block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle.Ptr();
	// Create new block that will hold re-ordered row data
	auto ordered_data_block =
	    make_uniq<RowDataBlock>(*buffer_manager, unordered_data_block->capacity, unordered_data_block->entry_size);
	ordered_data_block->count = count;
	auto ordered_data_handle = buffer_manager->Pin(ordered_data_block->block);
	data_ptr_t ordered_data_ptr = ordered_data_handle.Ptr();
	// Re-order fixed-size row layout
	const idx_t row_width = sd.layout.GetRowWidth();
	const idx_t sorting_entry_size = gstate.sort_layout.entry_size;
	for (idx_t i = 0; i < count; i++) {
		auto index = Load<uint32_t>(sorting_ptr);
		FastMemcpy(ordered_data_ptr, unordered_data_ptr + index * row_width, row_width);
		ordered_data_ptr += row_width;
		sorting_ptr += sorting_entry_size;
	}
	ordered_data_block->block->SetSwizzling(
	    sd.layout.AllConstant() || !sd.swizzled ? nullptr : "LocalSortState::ReOrder.ordered_data");
	// Replace the unordered data block with the re-ordered data block
	sd.data_blocks.clear();
	sd.data_blocks.push_back(std::move(ordered_data_block));
	// Deal with the heap (if necessary)
	if (!sd.layout.AllConstant() && reorder_heap) {
		// Swizzle the column pointers to offsets
		RowOperations::SwizzleColumns(sd.layout, ordered_data_handle.Ptr(), count);
		sd.data_blocks.back()->block->SetSwizzling(nullptr);
		// Create a single heap block to store the ordered heap
		idx_t total_byte_offset =
		    std::accumulate(heap.blocks.begin(), heap.blocks.end(), (idx_t)0,
		                    [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->byte_offset; });
		idx_t heap_block_size = MaxValue(total_byte_offset, (idx_t)Storage::BLOCK_SIZE);
		auto ordered_heap_block = make_uniq<RowDataBlock>(*buffer_manager, heap_block_size, 1);
		ordered_heap_block->count = count;
		ordered_heap_block->byte_offset = total_byte_offset;
		auto ordered_heap_handle = buffer_manager->Pin(ordered_heap_block->block);
		data_ptr_t ordered_heap_ptr = ordered_heap_handle.Ptr();
		// Fill the heap in order
		ordered_data_ptr = ordered_data_handle.Ptr();
		const idx_t heap_pointer_offset = sd.layout.GetHeapOffset();
		for (idx_t i = 0; i < count; i++) {
			auto heap_row_ptr = Load<data_ptr_t>(ordered_data_ptr + heap_pointer_offset);
			auto heap_row_size = Load<uint32_t>(heap_row_ptr);
			memcpy(ordered_heap_ptr, heap_row_ptr, heap_row_size);
			ordered_heap_ptr += heap_row_size;
			ordered_data_ptr += row_width;
		}
		// Swizzle the base pointer to the offset of each row in the heap
		RowOperations::SwizzleHeapPointer(sd.layout, ordered_data_handle.Ptr(), ordered_heap_handle.Ptr(), count);
		// Move the re-ordered heap to the SortedData, and clear the local heap
		sd.heap_blocks.push_back(std::move(ordered_heap_block));
		heap.pinned_blocks.clear();
		heap.blocks.clear();
		heap.count = 0;
	}
}

void LocalSortState::ReOrder(GlobalSortState &gstate, bool reorder_heap) {
	auto &sb = *sorted_blocks.back();
	auto sorting_handle = buffer_manager->Pin(sb.radix_sorting_data.back()->block);
	const data_ptr_t sorting_ptr = sorting_handle.Ptr() + gstate.sort_layout.comparison_size;
	// Re-order variable size sorting columns
	if (!gstate.sort_layout.all_constant) {
		ReOrder(*sb.blob_sorting_data, sorting_ptr, *blob_sorting_heap, gstate, reorder_heap);
	}
	// And the payload
	ReOrder(*sb.payload_data, sorting_ptr, *payload_heap, gstate, reorder_heap);
}

GlobalSortState::GlobalSortState(BufferManager &buffer_manager, const vector<BoundOrderByNode> &orders,
                                 RowLayout &payload_layout)
    : buffer_manager(buffer_manager), sort_layout(SortLayout(orders)), payload_layout(payload_layout),
      block_capacity(0), external(false) {
}

void GlobalSortState::AddLocalState(LocalSortState &local_sort_state) {
	if (!local_sort_state.radix_sorting_data) {
		return;
	}

	// Sort accumulated data
	// we only re-order the heap when the data is expected to not fit in memory
	// re-ordering the heap avoids random access when reading/merging but incurs a significant cost of shuffling data
	// when data fits in memory, doing random access on reads is cheaper than re-shuffling
	local_sort_state.Sort(*this, external || !local_sort_state.sorted_blocks.empty());

	// Append local state sorted data to this global state
	lock_guard<mutex> append_guard(lock);
	for (auto &sb : local_sort_state.sorted_blocks) {
		sorted_blocks.push_back(std::move(sb));
	}
	auto &payload_heap = local_sort_state.payload_heap;
	for (idx_t i = 0; i < payload_heap->blocks.size(); i++) {
		heap_blocks.push_back(std::move(payload_heap->blocks[i]));
		pinned_blocks.push_back(std::move(payload_heap->pinned_blocks[i]));
	}
	if (!sort_layout.all_constant) {
		auto &blob_heap = local_sort_state.blob_sorting_heap;
		for (idx_t i = 0; i < blob_heap->blocks.size(); i++) {
			heap_blocks.push_back(std::move(blob_heap->blocks[i]));
			pinned_blocks.push_back(std::move(blob_heap->pinned_blocks[i]));
		}
	}
}

void GlobalSortState::PrepareMergePhase() {
	// Determine if we need to use do an external sort
	idx_t total_heap_size =
	    std::accumulate(sorted_blocks.begin(), sorted_blocks.end(), (idx_t)0,
	                    [](idx_t a, const unique_ptr<SortedBlock> &b) { return a + b->HeapSize(); });
	if (external || (pinned_blocks.empty() && total_heap_size > 0.25 * buffer_manager.GetMaxMemory())) {
		external = true;
	}
	// Use the data that we have to determine which partition size to use during the merge
	if (external && total_heap_size > 0) {
		// If we have variable size data we need to be conservative, as there might be skew
		idx_t max_block_size = 0;
		for (auto &sb : sorted_blocks) {
			idx_t size_in_bytes = sb->SizeInBytes();
			if (size_in_bytes > max_block_size) {
				max_block_size = size_in_bytes;
				block_capacity = sb->Count();
			}
		}
	} else {
		for (auto &sb : sorted_blocks) {
			block_capacity = MaxValue(block_capacity, sb->Count());
		}
	}
	// Unswizzle and pin heap blocks if we can fit everything in memory
	if (!external) {
		for (auto &sb : sorted_blocks) {
			sb->blob_sorting_data->Unswizzle();
			sb->payload_data->Unswizzle();
		}
	}
}

void GlobalSortState::InitializeMergeRound() {
	D_ASSERT(sorted_blocks_temp.empty());
	// If we reverse this list, the blocks that were merged last will be merged first in the next round
	// These are still in memory, therefore this reduces the amount of read/write to disk!
	std::reverse(sorted_blocks.begin(), sorted_blocks.end());
	// Uneven number of blocks - keep one on the side
	if (sorted_blocks.size() % 2 == 1) {
		odd_one_out = std::move(sorted_blocks.back());
		sorted_blocks.pop_back();
	}
	// Init merge path path indices
	pair_idx = 0;
	num_pairs = sorted_blocks.size() / 2;
	l_start = 0;
	r_start = 0;
	// Allocate room for merge results
	for (idx_t p_idx = 0; p_idx < num_pairs; p_idx++) {
		sorted_blocks_temp.emplace_back();
	}
}

void GlobalSortState::CompleteMergeRound(bool keep_radix_data) {
	sorted_blocks.clear();
	for (auto &sorted_block_vector : sorted_blocks_temp) {
		sorted_blocks.push_back(make_uniq<SortedBlock>(buffer_manager, *this));
		sorted_blocks.back()->AppendSortedBlocks(sorted_block_vector);
	}
	sorted_blocks_temp.clear();
	if (odd_one_out) {
		sorted_blocks.push_back(std::move(odd_one_out));
		odd_one_out = nullptr;
	}
	// Only one block left: Done!
	if (sorted_blocks.size() == 1 && !keep_radix_data) {
		sorted_blocks[0]->radix_sorting_data.clear();
		sorted_blocks[0]->blob_sorting_data = nullptr;
	}
}
void GlobalSortState::Print() {
	PayloadScanner scanner(*this, false);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), scanner.GetPayloadTypes());
	for (;;) {
		scanner.Scan(chunk);
		const auto count = chunk.size();
		if (!count) {
			break;
		}
		chunk.Print();
	}
}

} // namespace duckdb







#include <numeric>

namespace duckdb {

SortedData::SortedData(SortedDataType type, const RowLayout &layout, BufferManager &buffer_manager,
                       GlobalSortState &state)
    : type(type), layout(layout), swizzled(state.external), buffer_manager(buffer_manager), state(state) {
}

idx_t SortedData::Count() {
	idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), (idx_t)0,
	                              [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; });
	if (!layout.AllConstant() && state.external) {
		D_ASSERT(count == std::accumulate(heap_blocks.begin(), heap_blocks.end(), (idx_t)0,
		                                  [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; }));
	}
	return count;
}

void SortedData::CreateBlock() {
	auto capacity =
	    MaxValue(((idx_t)Storage::BLOCK_SIZE + layout.GetRowWidth() - 1) / layout.GetRowWidth(), state.block_capacity);
	data_blocks.push_back(make_uniq<RowDataBlock>(buffer_manager, capacity, layout.GetRowWidth()));
	if (!layout.AllConstant() && state.external) {
		heap_blocks.push_back(make_uniq<RowDataBlock>(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1));
		D_ASSERT(data_blocks.size() == heap_blocks.size());
	}
}

unique_ptr<SortedData> SortedData::CreateSlice(idx_t start_block_index, idx_t end_block_index, idx_t end_entry_index) {
	// Add the corresponding blocks to the result
	auto result = make_uniq<SortedData>(type, layout, buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->data_blocks.push_back(data_blocks[i]->Copy());
		if (!layout.AllConstant() && state.external) {
			result->heap_blocks.push_back(heap_blocks[i]->Copy());
		}
	}
	// All of the blocks that come before block with idx = start_block_idx can be reset (other references exist)
	for (idx_t i = 0; i < start_block_index; i++) {
		data_blocks[i]->block = nullptr;
		if (!layout.AllConstant() && state.external) {
			heap_blocks[i]->block = nullptr;
		}
	}
	// Use start and end entry indices to set the boundaries
	D_ASSERT(end_entry_index <= result->data_blocks.back()->count);
	result->data_blocks.back()->count = end_entry_index;
	if (!layout.AllConstant() && state.external) {
		result->heap_blocks.back()->count = end_entry_index;
	}
	return result;
}

void SortedData::Unswizzle() {
	if (layout.AllConstant() || !swizzled) {
		return;
	}
	for (idx_t i = 0; i < data_blocks.size(); i++) {
		auto &data_block = data_blocks[i];
		auto &heap_block = heap_blocks[i];
		D_ASSERT(data_block->block->IsSwizzled());
		auto data_handle_p = buffer_manager.Pin(data_block->block);
		auto heap_handle_p = buffer_manager.Pin(heap_block->block);
		RowOperations::UnswizzlePointers(layout, data_handle_p.Ptr(), heap_handle_p.Ptr(), data_block->count);
		state.heap_blocks.push_back(std::move(heap_block));
		state.pinned_blocks.push_back(std::move(heap_handle_p));
	}
	swizzled = false;
	heap_blocks.clear();
}

SortedBlock::SortedBlock(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), state(state), sort_layout(state.sort_layout),
      payload_layout(state.payload_layout) {
	blob_sorting_data = make_uniq<SortedData>(SortedDataType::BLOB, sort_layout.blob_layout, buffer_manager, state);
	payload_data = make_uniq<SortedData>(SortedDataType::PAYLOAD, payload_layout, buffer_manager, state);
}

idx_t SortedBlock::Count() const {
	idx_t count = std::accumulate(radix_sorting_data.begin(), radix_sorting_data.end(), (idx_t)0,
	                              [](idx_t a, const unique_ptr<RowDataBlock> &b) { return a + b->count; });
	if (!sort_layout.all_constant) {
		D_ASSERT(count == blob_sorting_data->Count());
	}
	D_ASSERT(count == payload_data->Count());
	return count;
}

void SortedBlock::InitializeWrite() {
	CreateBlock();
	if (!sort_layout.all_constant) {
		blob_sorting_data->CreateBlock();
	}
	payload_data->CreateBlock();
}

void SortedBlock::CreateBlock() {
	auto capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + sort_layout.entry_size - 1) / sort_layout.entry_size,
	                         state.block_capacity);
	radix_sorting_data.push_back(make_uniq<RowDataBlock>(buffer_manager, capacity, sort_layout.entry_size));
}

void SortedBlock::AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks) {
	D_ASSERT(Count() == 0);
	for (auto &sb : sorted_blocks) {
		for (auto &radix_block : sb->radix_sorting_data) {
			radix_sorting_data.push_back(std::move(radix_block));
		}
		if (!sort_layout.all_constant) {
			for (auto &blob_block : sb->blob_sorting_data->data_blocks) {
				blob_sorting_data->data_blocks.push_back(std::move(blob_block));
			}
			for (auto &heap_block : sb->blob_sorting_data->heap_blocks) {
				blob_sorting_data->heap_blocks.push_back(std::move(heap_block));
			}
		}
		for (auto &payload_data_block : sb->payload_data->data_blocks) {
			payload_data->data_blocks.push_back(std::move(payload_data_block));
		}
		if (!payload_data->layout.AllConstant()) {
			for (auto &payload_heap_block : sb->payload_data->heap_blocks) {
				payload_data->heap_blocks.push_back(std::move(payload_heap_block));
			}
		}
	}
}

void SortedBlock::GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index) {
	if (global_idx == Count()) {
		local_block_index = radix_sorting_data.size() - 1;
		local_entry_index = radix_sorting_data.back()->count;
		return;
	}
	D_ASSERT(global_idx < Count());
	local_entry_index = global_idx;
	for (local_block_index = 0; local_block_index < radix_sorting_data.size(); local_block_index++) {
		const idx_t &block_count = radix_sorting_data[local_block_index]->count;
		if (local_entry_index >= block_count) {
			local_entry_index -= block_count;
		} else {
			break;
		}
	}
	D_ASSERT(local_entry_index < radix_sorting_data[local_block_index]->count);
}

unique_ptr<SortedBlock> SortedBlock::CreateSlice(const idx_t start, const idx_t end, idx_t &entry_idx) {
	// Identify blocks/entry indices of this slice
	idx_t start_block_index;
	idx_t start_entry_index;
	GlobalToLocalIndex(start, start_block_index, start_entry_index);
	idx_t end_block_index;
	idx_t end_entry_index;
	GlobalToLocalIndex(end, end_block_index, end_entry_index);
	// Add the corresponding blocks to the result
	auto result = make_uniq<SortedBlock>(buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->radix_sorting_data.push_back(radix_sorting_data[i]->Copy());
	}
	// Reset all blocks that come before block with idx = start_block_idx (slice holds new reference)
	for (idx_t i = 0; i < start_block_index; i++) {
		radix_sorting_data[i]->block = nullptr;
	}
	// Use start and end entry indices to set the boundaries
	entry_idx = start_entry_index;
	D_ASSERT(end_entry_index <= result->radix_sorting_data.back()->count);
	result->radix_sorting_data.back()->count = end_entry_index;
	// Same for the var size sorting data
	if (!sort_layout.all_constant) {
		result->blob_sorting_data = blob_sorting_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	}
	// And the payload data
	result->payload_data = payload_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	return result;
}

idx_t SortedBlock::HeapSize() const {
	idx_t result = 0;
	if (!sort_layout.all_constant) {
		for (auto &block : blob_sorting_data->heap_blocks) {
			result += block->capacity;
		}
	}
	if (!payload_layout.AllConstant()) {
		for (auto &block : payload_data->heap_blocks) {
			result += block->capacity;
		}
	}
	return result;
}

idx_t SortedBlock::SizeInBytes() const {
	idx_t bytes = 0;
	for (idx_t i = 0; i < radix_sorting_data.size(); i++) {
		bytes += radix_sorting_data[i]->capacity * sort_layout.entry_size;
		if (!sort_layout.all_constant) {
			bytes += blob_sorting_data->data_blocks[i]->capacity * sort_layout.blob_layout.GetRowWidth();
			bytes += blob_sorting_data->heap_blocks[i]->capacity;
		}
		bytes += payload_data->data_blocks[i]->capacity * payload_layout.GetRowWidth();
		if (!payload_layout.AllConstant()) {
			bytes += payload_data->heap_blocks[i]->capacity;
		}
	}
	return bytes;
}

SBScanState::SBScanState(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), sort_layout(state.sort_layout), state(state), block_idx(0), entry_idx(0) {
}

void SBScanState::PinRadix(idx_t block_idx_to) {
	auto &radix_sorting_data = sb->radix_sorting_data;
	D_ASSERT(block_idx_to < radix_sorting_data.size());
	auto &block = radix_sorting_data[block_idx_to];
	if (!radix_handle.IsValid() || radix_handle.GetBlockHandle() != block->block) {
		radix_handle = buffer_manager.Pin(block->block);
	}
}

void SBScanState::PinData(SortedData &sd) {
	D_ASSERT(block_idx < sd.data_blocks.size());
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;

	auto &data_block = sd.data_blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockHandle() != data_block->block) {
		data_handle = buffer_manager.Pin(data_block->block);
	}
	if (sd.layout.AllConstant() || !state.external) {
		return;
	}
	auto &heap_block = sd.heap_blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockHandle() != heap_block->block) {
		heap_handle = buffer_manager.Pin(heap_block->block);
	}
}

data_ptr_t SBScanState::RadixPtr() const {
	return radix_handle.Ptr() + entry_idx * sort_layout.entry_size;
}

data_ptr_t SBScanState::DataPtr(SortedData &sd) const {
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	D_ASSERT(sd.data_blocks[block_idx]->block->Readers() != 0 &&
	         data_handle.GetBlockHandle() == sd.data_blocks[block_idx]->block);
	return data_handle.Ptr() + entry_idx * sd.layout.GetRowWidth();
}

data_ptr_t SBScanState::HeapPtr(SortedData &sd) const {
	return BaseHeapPtr(sd) + Load<idx_t>(DataPtr(sd) + sd.layout.GetHeapOffset());
}

data_ptr_t SBScanState::BaseHeapPtr(SortedData &sd) const {
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;
	D_ASSERT(!sd.layout.AllConstant() && state.external);
	D_ASSERT(sd.heap_blocks[block_idx]->block->Readers() != 0 &&
	         heap_handle.GetBlockHandle() == sd.heap_blocks[block_idx]->block);
	return heap_handle.Ptr();
}

idx_t SBScanState::Remaining() const {
	const auto &blocks = sb->radix_sorting_data;
	idx_t remaining = 0;
	if (block_idx < blocks.size()) {
		remaining += blocks[block_idx]->count - entry_idx;
		for (idx_t i = block_idx + 1; i < blocks.size(); i++) {
			remaining += blocks[i]->count;
		}
	}
	return remaining;
}

void SBScanState::SetIndices(idx_t block_idx_to, idx_t entry_idx_to) {
	block_idx = block_idx_to;
	entry_idx = entry_idx_to;
}

PayloadScanner::PayloadScanner(SortedData &sorted_data, GlobalSortState &global_sort_state, bool flush_p) {
	auto count = sorted_data.Count();
	auto &layout = sorted_data.layout;

	// Create collections to put the data into so we can use RowDataCollectionScanner
	rows = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	rows->count = count;

	heap = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (!sorted_data.layout.AllConstant()) {
		heap->count = count;
	}

	if (flush_p) {
		// If we are flushing, we can just move the data
		rows->blocks = std::move(sorted_data.data_blocks);
		if (!layout.AllConstant()) {
			heap->blocks = std::move(sorted_data.heap_blocks);
		}
	} else {
		// Not flushing, create references to the blocks
		for (auto &block : sorted_data.data_blocks) {
			rows->blocks.emplace_back(block->Copy());
		}
		if (!layout.AllConstant()) {
			for (auto &block : sorted_data.heap_blocks) {
				heap->blocks.emplace_back(block->Copy());
			}
		}
	}

	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, global_sort_state.external, flush_p);
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, bool flush_p)
    : PayloadScanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state, flush_p) {
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, idx_t block_idx, bool flush_p) {
	auto &sorted_data = *global_sort_state.sorted_blocks[0]->payload_data;
	auto count = sorted_data.data_blocks[block_idx]->count;
	auto &layout = sorted_data.layout;

	// Create collections to put the data into so we can use RowDataCollectionScanner
	rows = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (flush_p) {
		rows->blocks.emplace_back(std::move(sorted_data.data_blocks[block_idx]));
	} else {
		rows->blocks.emplace_back(sorted_data.data_blocks[block_idx]->Copy());
	}
	rows->count = count;

	heap = make_uniq<RowDataCollection>(global_sort_state.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
	if (!sorted_data.layout.AllConstant() && sorted_data.swizzled) {
		if (flush_p) {
			heap->blocks.emplace_back(std::move(sorted_data.heap_blocks[block_idx]));
		} else {
			heap->blocks.emplace_back(sorted_data.heap_blocks[block_idx]->Copy());
		}
		heap->count = count;
	}

	scanner = make_uniq<RowDataCollectionScanner>(*rows, *heap, layout, global_sort_state.external, flush_p);
}

void PayloadScanner::Scan(DataChunk &chunk) {
	scanner->Scan(chunk);
}

int SBIterator::ComparisonValue(ExpressionType comparison) {
	switch (comparison) {
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return -1;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0;
	default:
		throw InternalException("Unimplemented comparison type for IEJoin!");
	}
}

static idx_t GetBlockCountWithEmptyCheck(const GlobalSortState &gss) {
	D_ASSERT(!gss.sorted_blocks.empty());
	return gss.sorted_blocks[0]->radix_sorting_data.size();
}

SBIterator::SBIterator(GlobalSortState &gss, ExpressionType comparison, idx_t entry_idx_p)
    : sort_layout(gss.sort_layout), block_count(GetBlockCountWithEmptyCheck(gss)), block_capacity(gss.block_capacity),
      cmp_size(sort_layout.comparison_size), entry_size(sort_layout.entry_size), all_constant(sort_layout.all_constant),
      external(gss.external), cmp(ComparisonValue(comparison)), scan(gss.buffer_manager, gss), block_ptr(nullptr),
      entry_ptr(nullptr) {

	scan.sb = gss.sorted_blocks[0].get();
	scan.block_idx = block_count;
	SetIndex(entry_idx_p);
}

} // namespace duckdb







#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>
#include <random>

namespace duckdb {

string StringUtil::GenerateRandomName(idx_t length) {
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(0, 15);

	std::stringstream ss;
	ss << std::hex;
	for (idx_t i = 0; i < length; i++) {
		ss << dis(gen);
	}
	return ss.str();
}

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

void StringUtil::LTrim(string &str) {
	auto it = str.begin();
	while (it != str.end() && CharacterIsSpace(*it)) {
		it++;
	}
	str.erase(str.begin(), it);
}

// Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
void StringUtil::RTrim(string &str) {
	str.erase(find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !CharacterIsSpace(ch); }).base(),
	          str.end());
}

void StringUtil::RTrim(string &str, const string &chars_to_trim) {
	str.erase(find_if(str.rbegin(), str.rend(),
	                  [&chars_to_trim](int ch) { return ch > 0 && chars_to_trim.find(ch) == string::npos; })
	              .base(),
	          str.end());
}

void StringUtil::Trim(string &str) {
	StringUtil::LTrim(str);
	StringUtil::RTrim(str);
}

bool StringUtil::StartsWith(string str, string prefix) {
	if (prefix.size() > str.size()) {
		return false;
	}
	return equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const string &str, const string &suffix) {
	if (suffix.size() > str.size()) {
		return false;
	}
	return equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

string StringUtil::Repeat(const string &str, idx_t n) {
	std::ostringstream os;
	for (idx_t i = 0; i < n; i++) {
		os << str;
	}
	return (os.str());
}

vector<string> StringUtil::Split(const string &str, char delimiter) {
	std::stringstream ss(str);
	vector<string> lines;
	string temp;
	while (getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	}
	return (lines);
}

namespace string_util_internal {

inline void SkipSpaces(const string &str, idx_t &index) {
	while (index < str.size() && std::isspace(str[index])) {
		index++;
	}
}

inline void ConsumeLetter(const string &str, idx_t &index, char expected) {
	if (index >= str.size() || str[index] != expected) {
		throw ParserException("Invalid quoted list: %s", str);
	}

	index++;
}

template <typename F>
inline void TakeWhile(const string &str, idx_t &index, const F &cond, string &taker) {
	while (index < str.size() && cond(str[index])) {
		taker.push_back(str[index]);
		index++;
	}
}

inline string TakePossiblyQuotedItem(const string &str, idx_t &index, char delimiter, char quote) {
	string entry;

	if (str[index] == quote) {
		index++;
		TakeWhile(
		    str, index, [quote](char c) { return c != quote; }, entry);
		ConsumeLetter(str, index, quote);
	} else {
		TakeWhile(
		    str, index, [delimiter, quote](char c) { return c != delimiter && c != quote && !std::isspace(c); }, entry);
	}

	return entry;
}

} // namespace string_util_internal

vector<string> StringUtil::SplitWithQuote(const string &str, char delimiter, char quote) {
	vector<string> entries;
	idx_t i = 0;

	string_util_internal::SkipSpaces(str, i);
	while (i < str.size()) {
		if (!entries.empty()) {
			string_util_internal::ConsumeLetter(str, i, delimiter);
		}

		entries.emplace_back(string_util_internal::TakePossiblyQuotedItem(str, i, delimiter, quote));
		string_util_internal::SkipSpaces(str, i);
	}

	return entries;
}

string StringUtil::Join(const vector<string> &input, const string &separator) {
	return StringUtil::Join(input, input.size(), separator, [](const string &s) { return s; });
}

string StringUtil::BytesToHumanReadableString(idx_t bytes) {
	string db_size;
	auto kilobytes = bytes / 1000;
	auto megabytes = kilobytes / 1000;
	kilobytes -= megabytes * 1000;
	auto gigabytes = megabytes / 1000;
	megabytes -= gigabytes * 1000;
	auto terabytes = gigabytes / 1000;
	gigabytes -= terabytes * 1000;
	auto petabytes = terabytes / 1000;
	terabytes -= petabytes * 1000;
	if (petabytes > 0) {
		return to_string(petabytes) + "." + to_string(terabytes / 100) + "PB";
	}
	if (terabytes > 0) {
		return to_string(terabytes) + "." + to_string(gigabytes / 100) + "TB";
	} else if (gigabytes > 0) {
		return to_string(gigabytes) + "." + to_string(megabytes / 100) + "GB";
	} else if (megabytes > 0) {
		return to_string(megabytes) + "." + to_string(kilobytes / 100) + "MB";
	} else if (kilobytes > 0) {
		return to_string(kilobytes) + "KB";
	} else {
		return to_string(bytes) + (bytes == 1 ? " byte" : " bytes");
	}
}

string StringUtil::Upper(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::toupper(c); });
	return (copy);
}

string StringUtil::Lower(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return StringUtil::CharacterToLower(c); });
	return (copy);
}

bool StringUtil::IsLower(const string &str) {
	return str == Lower(str);
}

// Jenkins hash function: https://en.wikipedia.org/wiki/Jenkins_hash_function
uint64_t StringUtil::CIHash(const string &str) {
	uint32_t hash = 0;
	for (auto c : str) {
		hash += StringUtil::CharacterToLower(c);
		hash += hash << 10;
		hash ^= hash >> 6;
	}
	hash += hash << 3;
	hash ^= hash >> 11;
	hash += hash << 15;
	return hash;
}

bool StringUtil::CIEquals(const string &l1, const string &l2) {
	if (l1.size() != l2.size()) {
		return false;
	}
	for (idx_t c = 0; c < l1.size(); c++) {
		if (StringUtil::CharacterToLower(l1[c]) != StringUtil::CharacterToLower(l2[c])) {
			return false;
		}
	}
	return true;
}

vector<string> StringUtil::Split(const string &input, const string &split) {
	vector<string> splits;

	idx_t last = 0;
	idx_t input_len = input.size();
	idx_t split_len = split.size();
	while (last <= input_len) {
		idx_t next = input.find(split, last);
		if (next == string::npos) {
			next = input_len;
		}

		// Push the substring [last, next) on to splits
		string substr = input.substr(last, next - last);
		if (!substr.empty()) {
			splits.push_back(substr);
		}
		last = next + split_len;
	}
	if (splits.empty()) {
		splits.push_back(input);
	}
	return splits;
}

string StringUtil::Replace(string source, const string &from, const string &to) {
	if (from.empty()) {
		throw InternalException("Invalid argument to StringUtil::Replace - empty FROM");
	}
	idx_t start_pos = 0;
	while ((start_pos = source.find(from, start_pos)) != string::npos) {
		source.replace(start_pos, from.length(), to);
		start_pos += to.length(); // In case 'to' contains 'from', like
		                          // replacing 'x' with 'yx'
	}
	return source;
}

vector<string> StringUtil::TopNStrings(vector<pair<string, idx_t>> scores, idx_t n, idx_t threshold) {
	if (scores.empty()) {
		return vector<string>();
	}
	sort(scores.begin(), scores.end(), [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) -> bool {
		return a.second < b.second || (a.second == b.second && a.first.size() < b.first.size());
	});
	vector<string> result;
	result.push_back(scores[0].first);
	for (idx_t i = 1; i < MinValue<idx_t>(scores.size(), n); i++) {
		if (scores[i].second > threshold) {
			break;
		}
		result.push_back(scores[i].first);
	}
	return result;
}

struct LevenshteinArray {
	LevenshteinArray(idx_t len1, idx_t len2) : len1(len1) {
		dist = make_unsafe_uniq_array<idx_t>(len1 * len2);
	}

	idx_t &Score(idx_t i, idx_t j) {
		return dist[GetIndex(i, j)];
	}

private:
	idx_t len1;
	unsafe_unique_array<idx_t> dist;

	idx_t GetIndex(idx_t i, idx_t j) {
		return j * len1 + i;
	}
};

// adapted from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C++
idx_t StringUtil::LevenshteinDistance(const string &s1_p, const string &s2_p, idx_t not_equal_penalty) {
	auto s1 = StringUtil::Lower(s1_p);
	auto s2 = StringUtil::Lower(s2_p);
	idx_t len1 = s1.size();
	idx_t len2 = s2.size();
	if (len1 == 0) {
		return len2;
	}
	if (len2 == 0) {
		return len1;
	}
	LevenshteinArray array(len1 + 1, len2 + 1);
	array.Score(0, 0) = 0;
	for (idx_t i = 0; i <= len1; i++) {
		array.Score(i, 0) = i;
	}
	for (idx_t j = 0; j <= len2; j++) {
		array.Score(0, j) = j;
	}
	for (idx_t i = 1; i <= len1; i++) {
		for (idx_t j = 1; j <= len2; j++) {
			// d[i][j] = std::min({ d[i - 1][j] + 1,
			//                      d[i][j - 1] + 1,
			//                      d[i - 1][j - 1] + (s1[i - 1] == s2[j - 1] ? 0 : 1) });
			int equal = s1[i - 1] == s2[j - 1] ? 0 : not_equal_penalty;
			idx_t adjacent_score1 = array.Score(i - 1, j) + 1;
			idx_t adjacent_score2 = array.Score(i, j - 1) + 1;
			idx_t adjacent_score3 = array.Score(i - 1, j - 1) + equal;

			idx_t t = MinValue<idx_t>(adjacent_score1, adjacent_score2);
			array.Score(i, j) = MinValue<idx_t>(t, adjacent_score3);
		}
	}
	return array.Score(len1, len2);
}

idx_t StringUtil::SimilarityScore(const string &s1, const string &s2) {
	return LevenshteinDistance(s1, s2, 3);
}

vector<string> StringUtil::TopNLevenshtein(const vector<string> &strings, const string &target, idx_t n,
                                           idx_t threshold) {
	vector<pair<string, idx_t>> scores;
	scores.reserve(strings.size());
	for (auto &str : strings) {
		if (target.size() < str.size()) {
			scores.emplace_back(str, SimilarityScore(str.substr(0, target.size()), target));
		} else {
			scores.emplace_back(str, SimilarityScore(str, target));
		}
	}
	return TopNStrings(scores, n, threshold);
}

string StringUtil::CandidatesMessage(const vector<string> &candidates, const string &candidate) {
	string result_str;
	if (!candidates.empty()) {
		result_str = "\n" + candidate + ": ";
		for (idx_t i = 0; i < candidates.size(); i++) {
			if (i > 0) {
				result_str += ", ";
			}
			result_str += "\"" + candidates[i] + "\"";
		}
	}
	return result_str;
}

string StringUtil::CandidatesErrorMessage(const vector<string> &strings, const string &target,
                                          const string &message_prefix, idx_t n) {
	auto closest_strings = StringUtil::TopNLevenshtein(strings, target, n);
	return StringUtil::CandidatesMessage(closest_strings, message_prefix);
}

} // namespace duckdb











#include <sstream>

namespace duckdb {

RenderTree::RenderTree(idx_t width_p, idx_t height_p) : width(width_p), height(height_p) {
	nodes = unique_ptr<unique_ptr<RenderTreeNode>[]>(new unique_ptr<RenderTreeNode>[(width + 1) * (height + 1)]);
}

RenderTreeNode *RenderTree::GetNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return nullptr;
	}
	return nodes[GetPosition(x, y)].get();
}

bool RenderTree::HasNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return false;
	}
	return nodes[GetPosition(x, y)].get() != nullptr;
}

idx_t RenderTree::GetPosition(idx_t x, idx_t y) {
	return y * width + x;
}

void RenderTree::SetNode(idx_t x, idx_t y, unique_ptr<RenderTreeNode> node) {
	nodes[GetPosition(x, y)] = std::move(node);
}

void TreeRenderer::RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x < root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LTCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (y == 0) {
				// top level node: no node above this one
				ss << config.HORIZONTAL;
			} else {
				// render connection to node above this one
				ss << config.DMIDDLE;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RTCORNER;
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

void TreeRenderer::RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x <= root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LDCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (root.HasNode(x, y + 1)) {
				// node below this one: connect to that one
				ss << config.TMIDDLE;
			} else {
				// no node below this one: end the box
				ss << config.HORIZONTAL;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RDCORNER;
		} else if (root.HasNode(x, y + 1)) {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
			ss << config.VERTICAL;
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

string AdjustTextForRendering(string source, idx_t max_render_width) {
	idx_t cpos = 0;
	idx_t render_width = 0;
	vector<pair<idx_t, idx_t>> render_widths;
	while (cpos < source.size()) {
		idx_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		render_width += char_render_width;
		render_widths.emplace_back(cpos, render_width);
		if (render_width > max_render_width) {
			break;
		}
	}
	if (render_width > max_render_width) {
		// need to find a position to truncate
		for (idx_t pos = render_widths.size(); pos > 0; pos--) {
			if (render_widths[pos - 1].second < max_render_width - 4) {
				return source.substr(0, render_widths[pos - 1].first) + "..." +
				       string(max_render_width - render_widths[pos - 1].second - 3, ' ');
			}
		}
		source = "...";
	}
	// need to pad with spaces
	idx_t total_spaces = max_render_width - render_width;
	idx_t half_spaces = total_spaces / 2;
	idx_t extra_left_space = total_spaces % 2 == 0 ? 0 : 1;
	return string(half_spaces + extra_left_space, ' ') + source + string(half_spaces, ' ');
}

static bool NodeHasMultipleChildren(RenderTree &root, idx_t x, idx_t y) {
	for (; x < root.width && !root.HasNode(x + 1, y); x++) {
		if (root.HasNode(x + 1, y + 1)) {
			return true;
		}
	}
	return false;
}

void TreeRenderer::RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y) {
	// we first need to figure out how high our boxes are going to be
	vector<vector<string>> extra_info;
	idx_t extra_height = 0;
	extra_info.resize(root.width);
	for (idx_t x = 0; x < root.width; x++) {
		auto node = root.GetNode(x, y);
		if (node) {
			SplitUpExtraInfo(node->extra_text, extra_info[x]);
			if (extra_info[x].size() > extra_height) {
				extra_height = extra_info[x].size();
			}
		}
	}
	extra_height = MinValue<idx_t>(extra_height, config.MAX_EXTRA_LINES);
	idx_t halfway_point = (extra_height + 1) / 2;
	// now we render the actual node
	for (idx_t render_y = 0; render_y <= extra_height; render_y++) {
		for (idx_t x = 0; x < root.width; x++) {
			if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
				break;
			}
			auto node = root.GetNode(x, y);
			if (!node) {
				if (render_y == halfway_point) {
					bool has_child_to_the_right = NodeHasMultipleChildren(root, x, y);
					if (root.HasNode(x, y + 1)) {
						// node right below this one
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						ss << config.RTCORNER;
						if (has_child_to_the_right) {
							// but we have another child to the right! keep rendering the line
							ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						} else {
							// only a child below this one: fill the rest with spaces
							ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						}
					} else if (has_child_to_the_right) {
						// child to the right, but no child right below this one: render a full line
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else if (render_y >= halfway_point) {
					if (root.HasNode(x, y + 1)) {
						// we have a node below this empty spot: render a vertical line
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						ss << config.VERTICAL;
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else {
					// empty spot: render spaces
					ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
				}
			} else {
				ss << config.VERTICAL;
				// figure out what to render
				string render_text;
				if (render_y == 0) {
					render_text = node->name;
				} else {
					if (render_y <= extra_info[x].size()) {
						render_text = extra_info[x][render_y - 1];
					}
				}
				render_text = AdjustTextForRendering(render_text, config.NODE_RENDER_WIDTH - 2);
				ss << render_text;

				if (render_y == halfway_point && NodeHasMultipleChildren(root, x, y)) {
					ss << config.LMIDDLE;
				} else {
					ss << config.VERTICAL;
				}
			}
		}
		ss << std::endl;
	}
}

string TreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const QueryProfiler::TreeNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void TreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const QueryProfiler::TreeNode &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	while (root.width * config.NODE_RENDER_WIDTH > config.MAXIMUM_RENDER_WIDTH) {
		if (config.NODE_RENDER_WIDTH - 2 < config.MINIMUM_RENDER_WIDTH) {
			break;
		}
		config.NODE_RENDER_WIDTH -= 2;
	}

	for (idx_t y = 0; y < root.height; y++) {
		// start by rendering the top layer
		RenderTopLayer(root, ss, y);
		// now we render the content of the boxes
		RenderBoxContent(root, ss, y);
		// render the bottom layer of each of the boxes
		RenderBottomLayer(root, ss, y);
	}
}

bool TreeRenderer::CanSplitOnThisChar(char l) {
	return (l < '0' || (l > '9' && l < 'A') || (l > 'Z' && l < 'a')) && l != '_';
}

bool TreeRenderer::IsPadding(char l) {
	return l == ' ' || l == '\t' || l == '\n' || l == '\r';
}

string TreeRenderer::RemovePadding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && IsPadding(l[start])) {
		start++;
	}
	while (end > 0 && IsPadding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

void TreeRenderer::SplitStringBuffer(const string &source, vector<string> &result) {
	D_ASSERT(Utf8Proc::IsValid(source.c_str(), source.size()));
	idx_t max_line_render_size = config.NODE_RENDER_WIDTH - 2;
	// utf8 in prompt, get render width
	idx_t cpos = 0;
	idx_t start_pos = 0;
	idx_t render_width = 0;
	idx_t last_possible_split = 0;
	while (cpos < source.size()) {
		// check if we can split on this character
		if (CanSplitOnThisChar(source[cpos])) {
			last_possible_split = cpos;
		}
		size_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		idx_t next_cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		if (render_width + char_render_width > max_line_render_size) {
			if (last_possible_split <= start_pos + 8) {
				last_possible_split = cpos;
			}
			result.push_back(source.substr(start_pos, last_possible_split - start_pos));
			start_pos = last_possible_split;
			cpos = last_possible_split;
			render_width = 0;
		}
		cpos = next_cpos;
		render_width += char_render_width;
	}
	if (source.size() > start_pos) {
		result.push_back(source.substr(start_pos, source.size() - start_pos));
	}
}

void TreeRenderer::SplitUpExtraInfo(const string &extra_info, vector<string> &result) {
	if (extra_info.empty()) {
		return;
	}
	if (!Utf8Proc::IsValid(extra_info.c_str(), extra_info.size())) {
		return;
	}
	auto splits = StringUtil::Split(extra_info, "\n");
	if (!splits.empty() && splits[0] != "[INFOSEPARATOR]") {
		result.push_back(ExtraInfoSeparator());
	}
	for (auto &split : splits) {
		if (split == "[INFOSEPARATOR]") {
			result.push_back(ExtraInfoSeparator());
			continue;
		}
		string str = RemovePadding(split);
		if (str.empty()) {
			continue;
		}
		SplitStringBuffer(str, result);
	}
}

string TreeRenderer::ExtraInfoSeparator() {
	return StringUtil::Repeat(string(config.HORIZONTAL) + " ", (config.NODE_RENDER_WIDTH - 7) / 2);
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateRenderNode(string name, string extra_info) {
	auto result = make_uniq<RenderTreeNode>();
	result->name = std::move(name);
	result->extra_text = std::move(extra_info);
	return result;
}

class TreeChildrenIterator {
public:
	template <class T>
	static bool HasChildren(const T &op) {
		return !op.children.empty();
	}
	template <class T>
	static void Iterate(const T &op, const std::function<void(const T &child)> &callback) {
		for (auto &child : op.children) {
			callback(*child);
		}
	}
};

template <>
bool TreeChildrenIterator::HasChildren(const PhysicalOperator &op) {
	switch (op.type) {
	case PhysicalOperatorType::DELIM_JOIN:
	case PhysicalOperatorType::POSITIONAL_SCAN:
		return true;
	default:
		return !op.children.empty();
	}
}
template <>
void TreeChildrenIterator::Iterate(const PhysicalOperator &op,
                                   const std::function<void(const PhysicalOperator &child)> &callback) {
	for (auto &child : op.children) {
		callback(*child);
	}
	if (op.type == PhysicalOperatorType::DELIM_JOIN) {
		auto &delim = op.Cast<PhysicalDelimJoin>();
		callback(*delim.join);
	} else if ((op.type == PhysicalOperatorType::POSITIONAL_SCAN)) {
		auto &pscan = op.Cast<PhysicalPositionalScan>();
		for (auto &table : pscan.child_tables) {
			callback(*table);
		}
	}
}

struct PipelineRenderNode {
	explicit PipelineRenderNode(const PhysicalOperator &op) : op(op) {
	}

	const PhysicalOperator &op;
	unique_ptr<PipelineRenderNode> child;
};

template <>
bool TreeChildrenIterator::HasChildren(const PipelineRenderNode &op) {
	return op.child.get();
}

template <>
void TreeChildrenIterator::Iterate(const PipelineRenderNode &op,
                                   const std::function<void(const PipelineRenderNode &child)> &callback) {
	if (op.child) {
		callback(*op.child);
	}
}

template <class T>
static void GetTreeWidthHeight(const T &op, idx_t &width, idx_t &height) {
	if (!TreeChildrenIterator::HasChildren(op)) {
		width = 1;
		height = 1;
		return;
	}
	width = 0;
	height = 0;

	TreeChildrenIterator::Iterate<T>(op, [&](const T &child) {
		idx_t child_width, child_height;
		GetTreeWidthHeight<T>(child, child_width, child_height);
		width += child_width;
		height = MaxValue<idx_t>(height, child_height);
	});
	height++;
}

template <class T>
idx_t TreeRenderer::CreateRenderTreeRecursive(RenderTree &result, const T &op, idx_t x, idx_t y) {
	auto node = TreeRenderer::CreateNode(op);
	result.SetNode(x, y, std::move(node));

	if (!TreeChildrenIterator::HasChildren(op)) {
		return 1;
	}
	idx_t width = 0;
	// render the children of this node
	TreeChildrenIterator::Iterate<T>(
	    op, [&](const T &child) { width += CreateRenderTreeRecursive<T>(result, child, x + width, y + 1); });
	return width;
}

template <class T>
unique_ptr<RenderTree> TreeRenderer::CreateRenderTree(const T &op) {
	idx_t width, height;
	GetTreeWidthHeight<T>(op, width, height);

	auto result = make_uniq<RenderTree>(width, height);

	// now fill in the tree
	CreateRenderTreeRecursive<T>(*result, op, 0, 0);
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const LogicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PhysicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PipelineRenderNode &op) {
	return CreateNode(op.op);
}

string TreeRenderer::ExtractExpressionsRecursive(ExpressionInfo &state) {
	string result = "\n[INFOSEPARATOR]";
	result += "\n" + state.function_name;
	result += "\n" + StringUtil::Format("%.9f", double(state.function_time));
	if (state.children.empty()) {
		return result;
	}
	// render the children of this node
	for (auto &child : state.children) {
		result += ExtractExpressionsRecursive(*child);
	}
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const QueryProfiler::TreeNode &op) {
	auto result = TreeRenderer::CreateRenderNode(op.name, op.extra_info);
	result->extra_text += "\n[INFOSEPARATOR]";
	result->extra_text += "\n" + to_string(op.info.elements);
	string timing = StringUtil::Format("%.2f", op.info.time);
	result->extra_text += "\n(" + timing + "s)";
	if (config.detailed) {
		for (auto &info : op.info.executors_info) {
			if (!info) {
				continue;
			}
			for (auto &executor_info : info->roots) {
				string sample_count = to_string(executor_info->sample_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_count: " + sample_count;
				string sample_tuples_count = to_string(executor_info->sample_tuples_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_tuples_count: " + sample_tuples_count;
				string total_count = to_string(executor_info->total_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\ntotal_count: " + total_count;
				for (auto &state : executor_info->root->children) {
					result->extra_text += ExtractExpressionsRecursive(*state);
				}
			}
		}
	}
	return result;
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const LogicalOperator &op) {
	return CreateRenderTree<LogicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const PhysicalOperator &op) {
	return CreateRenderTree<PhysicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const QueryProfiler::TreeNode &op) {
	return CreateRenderTree<QueryProfiler::TreeNode>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const Pipeline &op) {
	auto operators = op.GetOperators();
	D_ASSERT(!operators.empty());
	unique_ptr<PipelineRenderNode> node;
	for (auto &op : operators) {
		auto new_node = make_uniq<PipelineRenderNode>(op.get());
		new_node->child = std::move(node);
		node = std::move(new_node);
	}
	return CreateRenderTree<PipelineRenderNode>(*node);
}

} // namespace duckdb






namespace duckdb {

BatchedDataCollection::BatchedDataCollection(ClientContext &context_p, vector<LogicalType> types_p,
                                             bool buffer_managed_p)
    : context(context_p), types(std::move(types_p)), buffer_managed(buffer_managed_p) {
}

void BatchedDataCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	optional_ptr<ColumnDataCollection> collection;
	if (last_collection.collection && last_collection.batch_index == batch_index) {
		// we are inserting into the same collection as before: use it directly
		collection = last_collection.collection;
	} else {
		// new collection: check if there is already an entry
		D_ASSERT(data.find(batch_index) == data.end());
		unique_ptr<ColumnDataCollection> new_collection;
		if (last_collection.collection) {
			new_collection = make_uniq<ColumnDataCollection>(*last_collection.collection);
		} else if (buffer_managed) {
			new_collection = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), types);
		} else {
			new_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
		}
		last_collection.collection = new_collection.get();
		last_collection.batch_index = batch_index;
		new_collection->InitializeAppend(last_collection.append_state);
		collection = new_collection.get();
		data.insert(make_pair(batch_index, std::move(new_collection)));
	}
	collection->Append(last_collection.append_state, input);
}

void BatchedDataCollection::Merge(BatchedDataCollection &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchedDataCollection::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = std::move(entry.second);
	}
	other.data.clear();
}

void BatchedDataCollection::InitializeScan(BatchedChunkScanState &state) {
	state.iterator = data.begin();
	if (state.iterator == data.end()) {
		return;
	}
	state.iterator->second->InitializeScan(state.scan_state);
}

void BatchedDataCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
	while (state.iterator != data.end()) {
		// check if there is a chunk remaining in this collection
		auto collection = state.iterator->second.get();
		collection->Scan(state.scan_state, output);
		if (output.size() > 0) {
			return;
		}
		// there isn't! move to the next collection
		state.iterator++;
		if (state.iterator == data.end()) {
			return;
		}
		state.iterator->second->InitializeScan(state.scan_state);
	}
}

unique_ptr<ColumnDataCollection> BatchedDataCollection::FetchCollection() {
	unique_ptr<ColumnDataCollection> result;
	for (auto &entry : data) {
		if (!result) {
			result = std::move(entry.second);
		} else {
			result->Combine(*entry.second);
		}
	}
	data.clear();
	if (!result) {
		// empty result
		return make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	}
	return result;
}

string BatchedDataCollection::ToString() const {
	string result;
	result += "Batched Data Collection\n";
	for (auto &entry : data) {
		result += "Batch Index - " + to_string(entry.first) + "\n";
		result += entry.second->ToString() + "\n\n";
	}
	return result;
}

void BatchedDataCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb






namespace duckdb {

// **** helper functions ****
static char ComputePadding(idx_t len) {
	return (8 - (len % 8)) % 8;
}

idx_t Bit::ComputeBitstringLen(idx_t len) {
	idx_t result = len / 8;
	if (len % 8 != 0) {
		result++;
	}
	// additional first byte to store info on zero padding
	result++;
	return result;
}

static inline idx_t GetBitPadding(const string_t &bit_string) {
	auto data = const_data_ptr_cast(bit_string.GetData());
	D_ASSERT(idx_t(data[0]) <= 8);
	return data[0];
}

static inline idx_t GetBitSize(const string_t &str) {
	string error_message;
	idx_t str_len;
	if (!Bit::TryGetBitStringSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

uint8_t Bit::GetFirstByte(const string_t &str) {
	D_ASSERT(str.GetSize() > 1);

	auto data = const_data_ptr_cast(str.GetData());
	return data[1] & ((1 << (8 - data[0])) - 1);
}

void Bit::Finalize(string_t &str) {
	// bit strings require all padding bits to be set to 1
	// this method sets all padding bits to 1
	auto padding = GetBitPadding(str);
	for (idx_t i = 0; i < idx_t(padding); i++) {
		Bit::SetBitInternal(str, i, 1);
	}
	Bit::Verify(str);
}

void Bit::SetEmptyBitString(string_t &target, string_t &input) {
	char *res_buf = target.GetDataWriteable();
	const char *buf = input.GetData();
	memset(res_buf, 0, input.GetSize());
	res_buf[0] = buf[0];
	Bit::Finalize(target);
}

void Bit::SetEmptyBitString(string_t &target, idx_t len) {
	char *res_buf = target.GetDataWriteable();
	memset(res_buf, 0, target.GetSize());
	res_buf[0] = ComputePadding(len);
	Bit::Finalize(target);
}

// **** casting functions ****
void Bit::ToString(string_t bits, char *output) {
	auto data = const_data_ptr_cast(bits.GetData());
	auto len = bits.GetSize();

	idx_t padding = GetBitPadding(bits);
	idx_t output_idx = 0;
	for (idx_t bit_idx = padding; bit_idx < 8; bit_idx++) {
		output[output_idx++] = data[1] & (1 << (7 - bit_idx)) ? '1' : '0';
	}
	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			output[output_idx++] = data[byte_idx] & (1 << (7 - bit_idx)) ? '1' : '0';
		}
	}
}

string Bit::ToString(string_t str) {
	auto len = BitLength(str);
	auto buffer = make_unsafe_uniq_array<char>(len);
	ToString(str, buffer.get());
	return string(buffer.get(), len);
}

bool Bit::TryGetBitStringSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '0' || data[i] == '1') {
			str_len++;
		} else {
			string error = StringUtil::Format("Invalid character encountered in string -> bit conversion: '%s'",
			                                  string(const_char_ptr_cast(data) + i, 1));
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	if (str_len == 0) {
		string error = "Cannot cast empty string to BIT";
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	str_len = ComputeBitstringLen(str_len);
	return true;
}

void Bit::ToBit(string_t str, string_t &output_str) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	auto output = output_str.GetDataWriteable();

	char byte = 0;
	idx_t padded_byte = len % 8;
	for (idx_t i = 0; i < padded_byte; i++) {
		byte <<= 1;
		if (data[i] == '1') {
			byte |= 1;
		}
	}
	if (padded_byte != 0) {
		*(output++) = (8 - padded_byte); // the first byte contains the number of padded zeroes
	}
	*(output++) = byte;

	for (idx_t byte_idx = padded_byte; byte_idx < len; byte_idx += 8) {
		byte = 0;
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			byte <<= 1;
			if (data[byte_idx + bit_idx] == '1') {
				byte |= 1;
			}
		}
		*(output++) = byte;
	}
	Bit::Finalize(output_str);
	Bit::Verify(output_str);
}

string Bit::ToBit(string_t str) {
	auto bit_len = GetBitSize(str);
	auto buffer = make_unsafe_uniq_array<char>(bit_len);
	string_t output_str(buffer.get(), bit_len);
	Bit::ToBit(str, output_str);
	return output_str.GetString();
}

void Bit::BlobToBit(string_t blob, string_t &output_str) {
	auto data = const_data_ptr_cast(blob.GetData());
	auto output = output_str.GetDataWriteable();
	idx_t size = blob.GetSize();

	*output = 0; // No padding
	memcpy(output + 1, data, size);
}

string Bit::BlobToBit(string_t blob) {
	auto buffer = make_unsafe_uniq_array<char>(blob.GetSize() + 1);
	string_t output_str(buffer.get(), blob.GetSize() + 1);
	Bit::BlobToBit(blob, output_str);
	return output_str.GetString();
}

void Bit::BitToBlob(string_t bit, string_t &output_blob) {
	D_ASSERT(bit.GetSize() == output_blob.GetSize() + 1);

	auto data = const_data_ptr_cast(bit.GetData());
	auto output = output_blob.GetDataWriteable();
	idx_t size = output_blob.GetSize();

	output[0] = GetFirstByte(bit);
	if (size > 2) {
		++output;
		// First byte in bitstring contains amount of padded bits,
		// second byte in bitstring is the padded byte,
		// therefore the rest of the data starts at data + 2 (third byte)
		memcpy(output, data + 2, size - 1);
	}
}

string Bit::BitToBlob(string_t bit) {
	D_ASSERT(bit.GetSize() > 1);

	auto buffer = make_unsafe_uniq_array<char>(bit.GetSize() - 1);
	string_t output_str(buffer.get(), bit.GetSize() - 1);
	Bit::BitToBlob(bit, output_str);
	return output_str.GetString();
}

// **** scalar functions ****
void Bit::BitString(const string_t &input, const idx_t &bit_length, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = input.GetData();

	auto padding = ComputePadding(bit_length);
	res_buf[0] = padding;
	for (idx_t i = 0; i < bit_length; i++) {
		if (i < bit_length - input.GetSize()) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = buf[i - (bit_length - input.GetSize())] == '1' ? 1 : 0;
			Bit::SetBit(result, i, bit);
		}
	}
	Bit::Finalize(result);
}

idx_t Bit::BitLength(string_t bits) {
	return ((bits.GetSize() - 1) * 8) - GetBitPadding(bits);
}

idx_t Bit::OctetLength(string_t bits) {
	return bits.GetSize() - 1;
}

idx_t Bit::BitCount(string_t bits) {
	idx_t count = 0;
	const char *buf = bits.GetData();
	for (idx_t byte_idx = 1; byte_idx < OctetLength(bits) + 1; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			count += (buf[byte_idx] & (1 << bit_idx)) ? 1 : 0;
		}
	}
	return count - GetBitPadding(bits);
}

idx_t Bit::BitPosition(string_t substring, string_t bits) {
	const char *buf = bits.GetData();
	auto len = bits.GetSize();
	auto substr_len = BitLength(substring);
	idx_t substr_idx = 0;

	for (idx_t bit_idx = GetBitPadding(bits); bit_idx < 8; bit_idx++) {
		idx_t bit = buf[1] & (1 << (7 - bit_idx)) ? 1 : 0;
		if (bit == GetBit(substring, substr_idx)) {
			substr_idx++;
			if (substr_idx == substr_len) {
				return (bit_idx - GetBitPadding(bits)) - substr_len + 2;
			}
		} else {
			substr_idx = 0;
		}
	}

	for (idx_t byte_idx = 2; byte_idx < len; byte_idx++) {
		for (idx_t bit_idx = 0; bit_idx < 8; bit_idx++) {
			idx_t bit = buf[byte_idx] & (1 << (7 - bit_idx)) ? 1 : 0;
			if (bit == GetBit(substring, substr_idx)) {
				substr_idx++;
				if (substr_idx == substr_len) {
					return (((byte_idx - 1) * 8) + bit_idx - GetBitPadding(bits)) - substr_len + 2;
				}
			} else {
				substr_idx = 0;
			}
		}
	}
	return 0;
}

idx_t Bit::GetBit(string_t bit_string, idx_t n) {
	return Bit::GetBitInternal(bit_string, n + GetBitPadding(bit_string));
}

idx_t Bit::GetBitIndex(idx_t n) {
	return n / 8 + 1;
}

idx_t Bit::GetBitInternal(string_t bit_string, idx_t n) {
	const char *buf = bit_string.GetData();
	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	char byte = buf[idx] >> (7 - (n % 8));
	return (byte & 1 ? 1 : 0);
}

void Bit::SetBit(string_t &bit_string, idx_t n, idx_t new_value) {
	SetBitInternal(bit_string, n + GetBitPadding(bit_string), new_value);
}

void Bit::SetBitInternal(string_t &bit_string, idx_t n, idx_t new_value) {
	char *buf = bit_string.GetDataWriteable();

	auto idx = Bit::GetBitIndex(n);
	D_ASSERT(idx < bit_string.GetSize());
	char shift_byte = 1 << (7 - (n % 8));
	if (new_value == 0) {
		shift_byte = ~shift_byte;
		buf[idx] &= shift_byte;
	} else {
		buf[idx] |= shift_byte;
	}
}

// **** BITWISE operators ****
void Bit::RightShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetData();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(result); i++) {
		if (i < shift) {
			Bit::SetBit(result, i, 0);
		} else {
			idx_t bit = Bit::GetBit(bit_string, i - shift);
			Bit::SetBit(result, i, bit);
		}
	}
	Bit::Finalize(result);
}

void Bit::LeftShift(const string_t &bit_string, const idx_t &shift, string_t &result) {
	char *res_buf = result.GetDataWriteable();
	const char *buf = bit_string.GetData();
	res_buf[0] = buf[0];
	for (idx_t i = 0; i < Bit::BitLength(bit_string); i++) {
		if (i < (Bit::BitLength(bit_string) - shift)) {
			idx_t bit = Bit::GetBit(bit_string, shift + i);
			Bit::SetBit(result, i, bit);
		} else {
			Bit::SetBit(result, i, 0);
		}
	}
	Bit::Finalize(result);
	Bit::Verify(result);
}

void Bit::BitwiseAnd(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot AND bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] & r_buf[i];
	}
	// and should preserve padding bits
	Bit::Verify(result);
}

void Bit::BitwiseOr(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot OR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] | r_buf[i];
	}
	// or should preserve padding bits
	Bit::Verify(result);
}

void Bit::BitwiseXor(const string_t &rhs, const string_t &lhs, string_t &result) {
	if (Bit::BitLength(lhs) != Bit::BitLength(rhs)) {
		throw InvalidInputException("Cannot XOR bit strings of different sizes");
	}

	char *buf = result.GetDataWriteable();
	const char *r_buf = rhs.GetData();
	const char *l_buf = lhs.GetData();

	buf[0] = l_buf[0];
	for (idx_t i = 1; i < lhs.GetSize(); i++) {
		buf[i] = l_buf[i] ^ r_buf[i];
	}
	Bit::Finalize(result);
}

void Bit::BitwiseNot(const string_t &input, string_t &result) {
	char *result_buf = result.GetDataWriteable();
	const char *buf = input.GetData();

	result_buf[0] = buf[0];
	for (idx_t i = 1; i < input.GetSize(); i++) {
		result_buf[i] = ~buf[i];
	}
	Bit::Finalize(result);
}

void Bit::Verify(const string_t &input) {
#ifdef DEBUG
	// bit strings require all padding bits to be set to 1
	auto padding = GetBitPadding(input);
	for (idx_t i = 0; i < padding; i++) {
		D_ASSERT(Bit::GetBitInternal(input, i));
	}
#endif
}

} // namespace duckdb







namespace duckdb {

constexpr const char *Blob::HEX_TABLE;
const int Blob::HEX_MAP[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
    -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

bool IsRegularCharacter(data_t c) {
	return c >= 32 && c <= 126 && c != '\\' && c != '\'' && c != '"';
}

idx_t Blob::GetStringSize(string_t blob) {
	auto data = const_data_ptr_cast(blob.GetData());
	auto len = blob.GetSize();
	idx_t str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (IsRegularCharacter(data[i])) {
			// ascii characters are rendered as-is
			str_len++;
		} else {
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			str_len += 4;
		}
	}
	return str_len;
}

void Blob::ToString(string_t blob, char *output) {
	auto data = const_data_ptr_cast(blob.GetData());
	auto len = blob.GetSize();
	idx_t str_idx = 0;
	for (idx_t i = 0; i < len; i++) {
		if (IsRegularCharacter(data[i])) {
			// ascii characters are rendered as-is
			output[str_idx++] = data[i];
		} else {
			auto byte_a = data[i] >> 4;
			auto byte_b = data[i] & 0x0F;
			D_ASSERT(byte_a >= 0 && byte_a < 16);
			D_ASSERT(byte_b >= 0 && byte_b < 16);
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			output[str_idx++] = '\\';
			output[str_idx++] = 'x';
			output[str_idx++] = Blob::HEX_TABLE[byte_a];
			output[str_idx++] = Blob::HEX_TABLE[byte_b];
		}
	}
	D_ASSERT(str_idx == GetStringSize(blob));
}

string Blob::ToString(string_t blob) {
	auto str_len = GetStringSize(blob);
	auto buffer = make_unsafe_uniq_array<char>(str_len);
	Blob::ToString(blob, buffer.get());
	return string(buffer.get(), str_len);
}

bool Blob::TryGetBlobSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			if (i + 3 >= len) {
				string error = "Invalid hex escape code encountered in string -> blob conversion: "
				               "unterminated escape code at end of blob";
				HandleCastError::AssignError(error, error_message);
				return false;
			}
			if (data[i + 1] != 'x' || Blob::HEX_MAP[data[i + 2]] < 0 || Blob::HEX_MAP[data[i + 3]] < 0) {
				string error =
				    StringUtil::Format("Invalid hex escape code encountered in string -> blob conversion: %s",
				                       string(const_char_ptr_cast(data) + i, 4));
				HandleCastError::AssignError(error, error_message);
				return false;
			}
			str_len++;
			i += 3;
		} else if (data[i] <= 127) {
			str_len++;
		} else {
			string error = "Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters "
			               "must be escaped with hex codes (e.g. \\xAA)";
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	return true;
}

idx_t Blob::GetBlobSize(string_t str) {
	string error_message;
	idx_t str_len;
	if (!Blob::TryGetBlobSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

void Blob::ToBlob(string_t str, data_ptr_t output) {
	auto data = const_data_ptr_cast(str.GetData());
	auto len = str.GetSize();
	idx_t blob_idx = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			int byte_a = Blob::HEX_MAP[data[i + 2]];
			int byte_b = Blob::HEX_MAP[data[i + 3]];
			D_ASSERT(i + 3 < len);
			D_ASSERT(byte_a >= 0 && byte_b >= 0);
			D_ASSERT(data[i + 1] == 'x');
			output[blob_idx++] = (byte_a << 4) + byte_b;
			i += 3;
		} else if (data[i] <= 127) {
			output[blob_idx++] = data_t(data[i]);
		} else {
			throw ConversionException("Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters "
			                          "must be escaped with hex codes (e.g. \\xAA)");
		}
	}
	D_ASSERT(blob_idx == GetBlobSize(str));
}

string Blob::ToBlob(string_t str) {
	auto blob_len = GetBlobSize(str);
	auto buffer = make_unsafe_uniq_array<char>(blob_len);
	Blob::ToBlob(str, data_ptr_cast(buffer.get()));
	return string(buffer.get(), blob_len);
}

// base64 functions are adapted from https://gist.github.com/tomykaira/f0fd86b6c73063283afe550bc5d77594
idx_t Blob::ToBase64Size(string_t blob) {
	// every 4 characters in base64 encode 3 bytes, plus (potential) padding at the end
	auto input_size = blob.GetSize();
	return ((input_size + 2) / 3) * 4;
}

void Blob::ToBase64(string_t blob, char *output) {
	auto input_data = const_data_ptr_cast(blob.GetData());
	auto input_size = blob.GetSize();
	idx_t out_idx = 0;
	idx_t i;
	// convert the bulk of the string to base64
	// this happens in steps of 3 bytes -> 4 output bytes
	for (i = 0; i + 2 < input_size; i += 3) {
		output[out_idx++] = Blob::BASE64_MAP[(input_data[i] >> 2) & 0x3F];
		output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4) | ((input_data[i + 1] & 0xF0) >> 4)];
		output[out_idx++] = Blob::BASE64_MAP[((input_data[i + 1] & 0xF) << 2) | ((input_data[i + 2] & 0xC0) >> 6)];
		output[out_idx++] = Blob::BASE64_MAP[input_data[i + 2] & 0x3F];
	}

	if (i < input_size) {
		// there are one or two bytes left over: we have to insert padding
		// first write the first 6 bits of the first byte
		output[out_idx++] = Blob::BASE64_MAP[(input_data[i] >> 2) & 0x3F];
		// now check the character count
		if (i == input_size - 1) {
			// single byte left over: convert the remainder of that byte and insert padding
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4)];
			output[out_idx++] = Blob::BASE64_PADDING;
		} else {
			// two bytes left over: convert the second byte as well
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4) | ((input_data[i + 1] & 0xF0) >> 4)];
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i + 1] & 0xF) << 2)];
		}
		output[out_idx++] = Blob::BASE64_PADDING;
	}
}

static constexpr int BASE64_DECODING_TABLE[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
    -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
    45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

idx_t Blob::FromBase64Size(string_t str) {
	auto input_data = str.GetData();
	auto input_size = str.GetSize();
	if (input_size % 4 != 0) {
		// valid base64 needs to always be cleanly divisible by 4
		throw ConversionException("Could not decode string \"%s\" as base64: length must be a multiple of 4",
		                          str.GetString());
	}
	if (input_size < 4) {
		// empty string
		return 0;
	}
	auto base_size = input_size / 4 * 3;
	// check for padding to figure out the length
	if (input_data[input_size - 2] == Blob::BASE64_PADDING) {
		// two bytes of padding
		return base_size - 2;
	}
	if (input_data[input_size - 1] == Blob::BASE64_PADDING) {
		// one byte of padding
		return base_size - 1;
	}
	// no padding
	return base_size;
}

template <bool ALLOW_PADDING>
uint32_t DecodeBase64Bytes(const string_t &str, const_data_ptr_t input_data, idx_t base_idx) {
	int decoded_bytes[4];
	for (idx_t decode_idx = 0; decode_idx < 4; decode_idx++) {
		if (ALLOW_PADDING && decode_idx >= 2 && input_data[base_idx + decode_idx] == Blob::BASE64_PADDING) {
			// the last two bytes of a base64 string can have padding: in this case we set the byte to 0
			decoded_bytes[decode_idx] = 0;
		} else {
			decoded_bytes[decode_idx] = BASE64_DECODING_TABLE[input_data[base_idx + decode_idx]];
		}
		if (decoded_bytes[decode_idx] < 0) {
			throw ConversionException(
			    "Could not decode string \"%s\" as base64: invalid byte value '%d' at position %d", str.GetString(),
			    input_data[base_idx + decode_idx], base_idx + decode_idx);
		}
	}
	return (decoded_bytes[0] << 3 * 6) + (decoded_bytes[1] << 2 * 6) + (decoded_bytes[2] << 1 * 6) +
	       (decoded_bytes[3] << 0 * 6);
}

void Blob::FromBase64(string_t str, data_ptr_t output, idx_t output_size) {
	D_ASSERT(output_size == FromBase64Size(str));
	auto input_data = const_data_ptr_cast(str.GetData());
	auto input_size = str.GetSize();
	if (input_size == 0) {
		return;
	}
	idx_t out_idx = 0;
	idx_t i = 0;
	for (i = 0; i + 4 < input_size; i += 4) {
		auto combined = DecodeBase64Bytes<false>(str, input_data, i);
		output[out_idx++] = (combined >> 2 * 8) & 0xFF;
		output[out_idx++] = (combined >> 1 * 8) & 0xFF;
		output[out_idx++] = (combined >> 0 * 8) & 0xFF;
	}
	// decode the final four bytes: padding is allowed here
	auto combined = DecodeBase64Bytes<true>(str, input_data, i);
	output[out_idx++] = (combined >> 2 * 8) & 0xFF;
	if (out_idx < output_size) {
		output[out_idx++] = (combined >> 1 * 8) & 0xFF;
	}
	if (out_idx < output_size) {
		output[out_idx++] = (combined >> 0 * 8) & 0xFF;
	}
}

} // namespace duckdb



namespace duckdb {

const int64_t NumericHelper::POWERS_OF_TEN[] {1,
                                              10,
                                              100,
                                              1000,
                                              10000,
                                              100000,
                                              1000000,
                                              10000000,
                                              100000000,
                                              1000000000,
                                              10000000000,
                                              100000000000,
                                              1000000000000,
                                              10000000000000,
                                              100000000000000,
                                              1000000000000000,
                                              10000000000000000,
                                              100000000000000000,
                                              1000000000000000000};

const double NumericHelper::DOUBLE_POWERS_OF_TEN[] {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
                                                    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
                                                    1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29,
                                                    1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39};

template <>
int NumericHelper::UnsignedLength(uint8_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint16_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	length += value >= 1000;
	length += value >= 10000;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint32_t value) {
	if (value >= 10000) {
		int length = 5;
		length += value >= 100000;
		length += value >= 1000000;
		length += value >= 10000000;
		length += value >= 100000000;
		length += value >= 1000000000;
		return length;
	} else {
		int length = 1;
		length += value >= 10;
		length += value >= 100;
		length += value >= 1000;
		return length;
	}
}

template <>
int NumericHelper::UnsignedLength(uint64_t value) {
	if (value >= 10000000000ULL) {
		if (value >= 1000000000000000ULL) {
			int length = 16;
			length += value >= 10000000000000000ULL;
			length += value >= 100000000000000000ULL;
			length += value >= 1000000000000000000ULL;
			length += value >= 10000000000000000000ULL;
			return length;
		} else {
			int length = 11;
			length += value >= 100000000000ULL;
			length += value >= 1000000000000ULL;
			length += value >= 10000000000000ULL;
			length += value >= 100000000000000ULL;
			return length;
		}
	} else {
		if (value >= 100000ULL) {
			int length = 6;
			length += value >= 1000000ULL;
			length += value >= 10000000ULL;
			length += value >= 100000000ULL;
			length += value >= 1000000000ULL;
			return length;
		} else {
			int length = 1;
			length += value >= 10ULL;
			length += value >= 100ULL;
			length += value >= 1000ULL;
			length += value >= 10000ULL;
			return length;
		}
	}
}

template <>
std::string NumericHelper::ToString(hugeint_t value) {
	return Hugeint::ToString(value);
}

} // namespace duckdb










#include <algorithm>
#include <cstring>

namespace duckdb {

ChunkCollection::ChunkCollection(Allocator &allocator) : allocator(allocator), count(0) {
}

ChunkCollection::ChunkCollection(ClientContext &context) : ChunkCollection(Allocator::Get(context)) {
}

void ChunkCollection::Verify() {
#ifdef DEBUG
	for (auto &chunk : chunks) {
		chunk->Verify();
	}
#endif
}

void ChunkCollection::Append(ChunkCollection &other) {
	for (auto &chunk : other.chunks) {
		Append(*chunk);
	}
}

void ChunkCollection::Merge(ChunkCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (count == 0) {
		chunks = std::move(other.chunks);
		types = std::move(other.types);
		count = other.count;
		return;
	}
	unique_ptr<DataChunk> old_back;
	if (!chunks.empty() && chunks.back()->size() != STANDARD_VECTOR_SIZE) {
		old_back = std::move(chunks.back());
		chunks.pop_back();
		count -= old_back->size();
	}
	for (auto &chunk : other.chunks) {
		chunks.push_back(std::move(chunk));
	}
	count += other.count;
	if (old_back) {
		Append(*old_back);
	}
	Verify();
}

void ChunkCollection::Append(DataChunk &new_chunk) {
	if (new_chunk.size() == 0) {
		return;
	}
	new_chunk.Verify();

	// we have to ensure that every chunk in the ChunkCollection is completely
	// filled, otherwise our O(1) lookup in GetValue and SetValue does not work
	// first fill the latest chunk, if it exists
	count += new_chunk.size();

	idx_t remaining_data = new_chunk.size();
	idx_t offset = 0;
	if (chunks.empty()) {
		// first chunk
		types = new_chunk.GetTypes();
	} else {
		// the types of the new chunk should match the types of the previous one
		D_ASSERT(types.size() == new_chunk.ColumnCount());
		auto new_types = new_chunk.GetTypes();
		for (idx_t i = 0; i < types.size(); i++) {
			if (new_types[i] != types[i]) {
				throw TypeMismatchException(new_types[i], types[i], "Type mismatch when combining rows");
			}
			if (types[i].InternalType() == PhysicalType::LIST) {
				// need to check all the chunks because they can have only-null list entries
				for (auto &chunk : chunks) {
					auto &chunk_vec = chunk->data[i];
					auto &new_vec = new_chunk.data[i];
					auto &chunk_type = chunk_vec.GetType();
					auto &new_type = new_vec.GetType();
					if (chunk_type != new_type) {
						throw TypeMismatchException(chunk_type, new_type, "Type mismatch when combining lists");
					}
				}
			}
			// TODO check structs, too
		}

		// first append data to the current chunk
		DataChunk &last_chunk = *chunks.back();
		idx_t added_data = MinValue<idx_t>(remaining_data, STANDARD_VECTOR_SIZE - last_chunk.size());
		if (added_data > 0) {
			// copy <added_data> elements to the last chunk
			new_chunk.Flatten();
			// have to be careful here: setting the cardinality without calling normalify can cause incorrect partial
			// decompression
			idx_t old_count = new_chunk.size();
			new_chunk.SetCardinality(added_data);

			last_chunk.Append(new_chunk);
			remaining_data -= added_data;
			// reset the chunk to the old data
			new_chunk.SetCardinality(old_count);
			offset = added_data;
		}
	}

	if (remaining_data > 0) {
		// create a new chunk and fill it with the remainder
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(allocator, types);
		new_chunk.Copy(*chunk, offset);
		chunks.push_back(std::move(chunk));
	}
}

void ChunkCollection::Append(unique_ptr<DataChunk> new_chunk) {
	if (types.empty()) {
		types = new_chunk->GetTypes();
	}
	D_ASSERT(types == new_chunk->GetTypes());
	count += new_chunk->size();
	chunks.push_back(std::move(new_chunk));
}

void ChunkCollection::Fuse(ChunkCollection &other) {
	if (count == 0) {
		chunks.reserve(other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < other.ChunkCount(); ++chunk_idx) {
			auto lhs = make_uniq<DataChunk>();
			auto &rhs = other.GetChunk(chunk_idx);
			lhs->data.reserve(rhs.data.size());
			for (auto &v : rhs.data) {
				lhs->data.emplace_back(v);
			}
			lhs->SetCardinality(rhs.size());
			chunks.push_back(std::move(lhs));
		}
		count = other.Count();
	} else {
		D_ASSERT(this->ChunkCount() == other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < ChunkCount(); ++chunk_idx) {
			auto &lhs = this->GetChunk(chunk_idx);
			auto &rhs = other.GetChunk(chunk_idx);
			D_ASSERT(lhs.size() == rhs.size());
			for (auto &v : rhs.data) {
				lhs.data.emplace_back(v);
			}
		}
	}
	types.insert(types.end(), other.types.begin(), other.types.end());
}

Value ChunkCollection::GetValue(idx_t column, idx_t index) {
	return chunks[LocateChunk(index)]->GetValue(column, index % STANDARD_VECTOR_SIZE);
}

void ChunkCollection::SetValue(idx_t column, idx_t index, const Value &value) {
	chunks[LocateChunk(index)]->SetValue(column, index % STANDARD_VECTOR_SIZE, value);
}

void ChunkCollection::CopyCell(idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	auto &chunk = GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	VectorOperations::Copy(source, target, source_offset + 1, source_offset, target_offset);
}

string ChunkCollection::ToString() const {
	return chunks.empty() ? "ChunkCollection [ 0 ]"
	                      : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
}

void ChunkCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb






namespace duckdb {

ColumnDataAllocator::ColumnDataAllocator(Allocator &allocator) : type(ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
	alloc.allocator = &allocator;
}

ColumnDataAllocator::ColumnDataAllocator(BufferManager &buffer_manager)
    : type(ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
	alloc.buffer_manager = &buffer_manager;
}

ColumnDataAllocator::ColumnDataAllocator(ClientContext &context, ColumnDataAllocatorType allocator_type)
    : type(allocator_type) {
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
	case ColumnDataAllocatorType::HYBRID:
		alloc.buffer_manager = &BufferManager::GetBufferManager(context);
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		alloc.allocator = &Allocator::Get(context);
		break;
	default:
		throw InternalException("Unrecognized column data allocator type");
	}
}

ColumnDataAllocator::ColumnDataAllocator(ColumnDataAllocator &other) {
	type = other.GetType();
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
	case ColumnDataAllocatorType::HYBRID:
		alloc.allocator = other.alloc.allocator;
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		alloc.buffer_manager = other.alloc.buffer_manager;
		break;
	default:
		throw InternalException("Unrecognized column data allocator type");
	}
}

BufferHandle ColumnDataAllocator::Pin(uint32_t block_id) {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR || type == ColumnDataAllocatorType::HYBRID);
	shared_ptr<BlockHandle> handle;
	if (shared) {
		// we only need to grab the lock when accessing the vector, because vector access is not thread-safe:
		// the vector can be resized by another thread while we try to access it
		lock_guard<mutex> guard(lock);
		handle = blocks[block_id].handle;
	} else {
		handle = blocks[block_id].handle;
	}
	return alloc.buffer_manager->Pin(handle);
}

BufferHandle ColumnDataAllocator::AllocateBlock(idx_t size) {
	D_ASSERT(type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR || type == ColumnDataAllocatorType::HYBRID);
	auto block_size = MaxValue<idx_t>(size, Storage::BLOCK_SIZE);
	BlockMetaData data;
	data.size = 0;
	data.capacity = block_size;
	auto pin = alloc.buffer_manager->Allocate(block_size, false, &data.handle);
	blocks.push_back(std::move(data));
	return pin;
}

void ColumnDataAllocator::AllocateEmptyBlock(idx_t size) {
	auto allocation_amount = MaxValue<idx_t>(NextPowerOfTwo(size), 4096);
	if (!blocks.empty()) {
		idx_t last_capacity = blocks.back().capacity;
		auto next_capacity = MinValue<idx_t>(last_capacity * 2, last_capacity + Storage::BLOCK_SIZE);
		allocation_amount = MaxValue<idx_t>(next_capacity, allocation_amount);
	}
	D_ASSERT(type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR);
	BlockMetaData data;
	data.size = 0;
	data.capacity = allocation_amount;
	data.handle = nullptr;
	blocks.push_back(std::move(data));
}

void ColumnDataAllocator::AssignPointer(uint32_t &block_id, uint32_t &offset, data_ptr_t pointer) {
	auto pointer_value = uintptr_t(pointer);
	if (sizeof(uintptr_t) == sizeof(uint32_t)) {
		block_id = uint32_t(pointer_value);
	} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
		block_id = uint32_t(pointer_value & 0xFFFFFFFF);
		offset = uint32_t(pointer_value >> 32);
	} else {
		throw InternalException("ColumnDataCollection: Architecture not supported!?");
	}
}

void ColumnDataAllocator::AllocateBuffer(idx_t size, uint32_t &block_id, uint32_t &offset,
                                         ChunkManagementState *chunk_state) {
	D_ASSERT(allocated_data.empty());
	if (blocks.empty() || blocks.back().Capacity() < size) {
		auto pinned_block = AllocateBlock(size);
		if (chunk_state) {
			D_ASSERT(!blocks.empty());
			auto new_block_id = blocks.size() - 1;
			chunk_state->handles[new_block_id] = std::move(pinned_block);
		}
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = blocks.size() - 1;
	if (chunk_state && chunk_state->handles.find(block_id) == chunk_state->handles.end()) {
		// not guaranteed to be pinned already by this thread (if shared allocator)
		chunk_state->handles[block_id] = alloc.buffer_manager->Pin(blocks[block_id].handle);
	}
	offset = block.size;
	block.size += size;
}

void ColumnDataAllocator::AllocateMemory(idx_t size, uint32_t &block_id, uint32_t &offset,
                                         ChunkManagementState *chunk_state) {
	D_ASSERT(blocks.size() == allocated_data.size());
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateEmptyBlock(size);
		auto &last_block = blocks.back();
		auto allocated = alloc.allocator->Allocate(last_block.capacity);
		allocated_data.push_back(std::move(allocated));
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	AssignPointer(block_id, offset, allocated_data.back().get() + block.size);
	block.size += size;
}

void ColumnDataAllocator::AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset,
                                       ChunkManagementState *chunk_state) {
	switch (type) {
	case ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR:
	case ColumnDataAllocatorType::HYBRID:
		if (shared) {
			lock_guard<mutex> guard(lock);
			AllocateBuffer(size, block_id, offset, chunk_state);
		} else {
			AllocateBuffer(size, block_id, offset, chunk_state);
		}
		break;
	case ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR:
		D_ASSERT(!shared);
		AllocateMemory(size, block_id, offset, chunk_state);
		break;
	default:
		throw InternalException("Unrecognized allocator type");
	}
}

void ColumnDataAllocator::Initialize(ColumnDataAllocator &other) {
	D_ASSERT(other.HasBlocks());
	blocks.push_back(other.blocks.back());
}

data_ptr_t ColumnDataAllocator::GetDataPointer(ChunkManagementState &state, uint32_t block_id, uint32_t offset) {
	if (type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR) {
		// in-memory allocator: construct pointer from block_id and offset
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			uintptr_t pointer_value = uintptr_t(block_id);
			return (data_ptr_t)pointer_value; // NOLINT - convert from pointer value back to pointer
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			uintptr_t pointer_value = (uintptr_t(offset) << 32) | uintptr_t(block_id);
			return (data_ptr_t)pointer_value; // NOLINT - convert from pointer value back to pointer
		} else {
			throw InternalException("ColumnDataCollection: Architecture not supported!?");
		}
	}
	D_ASSERT(state.handles.find(block_id) != state.handles.end());
	return state.handles[block_id].Ptr() + offset;
}

void ColumnDataAllocator::UnswizzlePointers(ChunkManagementState &state, Vector &result, idx_t v_offset, uint16_t count,
                                            uint32_t block_id, uint32_t offset) {
	D_ASSERT(result.GetType().InternalType() == PhysicalType::VARCHAR);
	lock_guard<mutex> guard(lock);

	auto &validity = FlatVector::Validity(result);
	auto strings = FlatVector::GetData<string_t>(result);

	// find first non-inlined string
	uint32_t i = v_offset;
	const uint32_t end = v_offset + count;
	for (; i < end; i++) {
		if (!validity.RowIsValid(i)) {
			continue;
		}
		if (!strings[i].IsInlined()) {
			break;
		}
	}
	// at least one string must be non-inlined, otherwise this function should not be called
	D_ASSERT(i < end);

	auto base_ptr = char_ptr_cast(GetDataPointer(state, block_id, offset));
	if (strings[i].GetData() == base_ptr) {
		// pointers are still valid
		return;
	}

	// pointer mismatch! pointers are invalid, set them correctly
	for (; i < end; i++) {
		if (!validity.RowIsValid(i)) {
			continue;
		}
		if (strings[i].IsInlined()) {
			continue;
		}
		strings[i].SetPointer(base_ptr);
		base_ptr += strings[i].GetSize();
	}
}

void ColumnDataAllocator::DeleteBlock(uint32_t block_id) {
	blocks[block_id].handle->SetCanDestroy(true);
}

Allocator &ColumnDataAllocator::GetAllocator() {
	return type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR ? *alloc.allocator
	                                                            : alloc.buffer_manager->GetBufferAllocator();
}

void ColumnDataAllocator::InitializeChunkState(ChunkManagementState &state, ChunkMetaData &chunk) {
	if (type != ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR && type != ColumnDataAllocatorType::HYBRID) {
		// nothing to pin
		return;
	}
	// release any handles that are no longer required
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = state.handles.begin(); it != state.handles.end(); it++) {
			if (chunk.block_ids.find(it->first) != chunk.block_ids.end()) {
				// still required: do not release
				continue;
			}
			state.handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);

	// grab any handles that are now required
	for (auto &block_id : chunk.block_ids) {
		if (state.handles.find(block_id) != state.handles.end()) {
			// already pinned: don't need to do anything
			continue;
		}
		state.handles[block_id] = Pin(block_id);
	}
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

} // namespace duckdb











namespace duckdb {

struct ColumnDataMetaData;

typedef void (*column_data_copy_function_t)(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data,
                                            Vector &source, idx_t offset, idx_t copy_count);

struct ColumnDataCopyFunction {
	column_data_copy_function_t function;
	vector<ColumnDataCopyFunction> child_functions;
};

struct ColumnDataMetaData {
	ColumnDataMetaData(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
	                   ColumnDataAppendState &state, ChunkMetaData &chunk_data, VectorDataIndex vector_data_index)
	    : copy_function(copy_function), segment(segment), state(state), chunk_data(chunk_data),
	      vector_data_index(vector_data_index) {
	}
	ColumnDataMetaData(ColumnDataCopyFunction &copy_function, ColumnDataMetaData &parent,
	                   VectorDataIndex vector_data_index)
	    : copy_function(copy_function), segment(parent.segment), state(parent.state), chunk_data(parent.chunk_data),
	      vector_data_index(vector_data_index) {
	}

	ColumnDataCopyFunction &copy_function;
	ColumnDataCollectionSegment &segment;
	ColumnDataAppendState &state;
	ChunkMetaData &chunk_data;
	VectorDataIndex vector_data_index;
	idx_t child_list_size = DConstants::INVALID_INDEX;

	VectorMetaData &GetVectorMetaData() {
		return segment.GetVectorData(vector_data_index);
	}
};

//! Explicitly initialized without types
ColumnDataCollection::ColumnDataCollection(Allocator &allocator_p) {
	types.clear();
	count = 0;
	this->finished_append = false;
	allocator = make_shared<ColumnDataAllocator>(allocator_p);
}

ColumnDataCollection::ColumnDataCollection(Allocator &allocator_p, vector<LogicalType> types_p) {
	Initialize(std::move(types_p));
	allocator = make_shared<ColumnDataAllocator>(allocator_p);
}

ColumnDataCollection::ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types_p) {
	Initialize(std::move(types_p));
	allocator = make_shared<ColumnDataAllocator>(buffer_manager);
}

ColumnDataCollection::ColumnDataCollection(shared_ptr<ColumnDataAllocator> allocator_p, vector<LogicalType> types_p) {
	Initialize(std::move(types_p));
	this->allocator = std::move(allocator_p);
}

ColumnDataCollection::ColumnDataCollection(ClientContext &context, vector<LogicalType> types_p,
                                           ColumnDataAllocatorType type)
    : ColumnDataCollection(make_shared<ColumnDataAllocator>(context, type), std::move(types_p)) {
	D_ASSERT(!types.empty());
}

ColumnDataCollection::ColumnDataCollection(ColumnDataCollection &other)
    : ColumnDataCollection(other.allocator, other.types) {
	other.finished_append = true;
	D_ASSERT(!types.empty());
}

ColumnDataCollection::~ColumnDataCollection() {
}

void ColumnDataCollection::Initialize(vector<LogicalType> types_p) {
	this->types = std::move(types_p);
	this->count = 0;
	this->finished_append = false;
	D_ASSERT(!types.empty());
	copy_functions.reserve(types.size());
	for (auto &type : types) {
		copy_functions.push_back(GetCopyFunction(type));
	}
}

void ColumnDataCollection::CreateSegment() {
	segments.emplace_back(make_uniq<ColumnDataCollectionSegment>(allocator, types));
}

Allocator &ColumnDataCollection::GetAllocator() const {
	return allocator->GetAllocator();
}

idx_t ColumnDataCollection::SizeInBytes() const {
	idx_t total_size = 0;
	for (const auto &segment : segments) {
		total_size += segment->SizeInBytes();
	}
	return total_size;
}

//===--------------------------------------------------------------------===//
// ColumnDataRow
//===--------------------------------------------------------------------===//
ColumnDataRow::ColumnDataRow(DataChunk &chunk_p, idx_t row_index, idx_t base_index)
    : chunk(chunk_p), row_index(row_index), base_index(base_index) {
}

Value ColumnDataRow::GetValue(idx_t column_index) const {
	D_ASSERT(column_index < chunk.ColumnCount());
	D_ASSERT(row_index < chunk.size());
	return chunk.data[column_index].GetValue(row_index);
}

idx_t ColumnDataRow::RowIndex() const {
	return base_index + row_index;
}

//===--------------------------------------------------------------------===//
// ColumnDataRowCollection
//===--------------------------------------------------------------------===//
ColumnDataRowCollection::ColumnDataRowCollection(const ColumnDataCollection &collection) {
	if (collection.Count() == 0) {
		return;
	}
	// read all the chunks
	ColumnDataScanState temp_scan_state;
	collection.InitializeScan(temp_scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	while (true) {
		auto chunk = make_uniq<DataChunk>();
		collection.InitializeScanChunk(*chunk);
		if (!collection.Scan(temp_scan_state, *chunk)) {
			break;
		}
		chunks.push_back(std::move(chunk));
	}
	// now create all of the column data rows
	rows.reserve(collection.Count());
	idx_t base_row = 0;
	for (auto &chunk : chunks) {
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			rows.emplace_back(*chunk, row_idx, base_row);
		}
		base_row += chunk->size();
	}
}

ColumnDataRow &ColumnDataRowCollection::operator[](idx_t i) {
	return rows[i];
}

const ColumnDataRow &ColumnDataRowCollection::operator[](idx_t i) const {
	return rows[i];
}

Value ColumnDataRowCollection::GetValue(idx_t column, idx_t index) const {
	return rows[index].GetValue(column);
}

//===--------------------------------------------------------------------===//
// ColumnDataChunkIterator
//===--------------------------------------------------------------------===//
ColumnDataChunkIterationHelper ColumnDataCollection::Chunks() const {
	vector<column_t> column_ids;
	for (idx_t i = 0; i < ColumnCount(); i++) {
		column_ids.push_back(i);
	}
	return Chunks(column_ids);
}

ColumnDataChunkIterationHelper ColumnDataCollection::Chunks(vector<column_t> column_ids) const {
	return ColumnDataChunkIterationHelper(*this, std::move(column_ids));
}

ColumnDataChunkIterationHelper::ColumnDataChunkIterationHelper(const ColumnDataCollection &collection_p,
                                                               vector<column_t> column_ids_p)
    : collection(collection_p), column_ids(std::move(column_ids_p)) {
}

ColumnDataChunkIterationHelper::ColumnDataChunkIterator::ColumnDataChunkIterator(
    const ColumnDataCollection *collection_p, vector<column_t> column_ids_p)
    : collection(collection_p), scan_chunk(make_shared<DataChunk>()), row_index(0) {
	if (!collection) {
		return;
	}
	collection->InitializeScan(scan_state, std::move(column_ids_p));
	collection->InitializeScanChunk(scan_state, *scan_chunk);
	collection->Scan(scan_state, *scan_chunk);
}

void ColumnDataChunkIterationHelper::ColumnDataChunkIterator::Next() {
	if (!collection) {
		return;
	}
	if (!collection->Scan(scan_state, *scan_chunk)) {
		collection = nullptr;
		row_index = 0;
	} else {
		row_index += scan_chunk->size();
	}
}

ColumnDataChunkIterationHelper::ColumnDataChunkIterator &
ColumnDataChunkIterationHelper::ColumnDataChunkIterator::operator++() {
	Next();
	return *this;
}

bool ColumnDataChunkIterationHelper::ColumnDataChunkIterator::operator!=(const ColumnDataChunkIterator &other) const {
	return collection != other.collection || row_index != other.row_index;
}

DataChunk &ColumnDataChunkIterationHelper::ColumnDataChunkIterator::operator*() const {
	return *scan_chunk;
}

//===--------------------------------------------------------------------===//
// ColumnDataRowIterator
//===--------------------------------------------------------------------===//
ColumnDataRowIterationHelper ColumnDataCollection::Rows() const {
	return ColumnDataRowIterationHelper(*this);
}

ColumnDataRowIterationHelper::ColumnDataRowIterationHelper(const ColumnDataCollection &collection_p)
    : collection(collection_p) {
}

ColumnDataRowIterationHelper::ColumnDataRowIterator::ColumnDataRowIterator(const ColumnDataCollection *collection_p)
    : collection(collection_p), scan_chunk(make_shared<DataChunk>()), current_row(*scan_chunk, 0, 0) {
	if (!collection) {
		return;
	}
	collection->InitializeScan(scan_state);
	collection->InitializeScanChunk(*scan_chunk);
	collection->Scan(scan_state, *scan_chunk);
}

void ColumnDataRowIterationHelper::ColumnDataRowIterator::Next() {
	if (!collection) {
		return;
	}
	current_row.row_index++;
	if (current_row.row_index >= scan_chunk->size()) {
		current_row.base_index += scan_chunk->size();
		current_row.row_index = 0;
		if (!collection->Scan(scan_state, *scan_chunk)) {
			// exhausted collection: move iterator to nop state
			current_row.base_index = 0;
			collection = nullptr;
		}
	}
}

ColumnDataRowIterationHelper::ColumnDataRowIterator ColumnDataRowIterationHelper::begin() { // NOLINT
	return ColumnDataRowIterationHelper::ColumnDataRowIterator(collection.Count() == 0 ? nullptr : &collection);
}
ColumnDataRowIterationHelper::ColumnDataRowIterator ColumnDataRowIterationHelper::end() { // NOLINT
	return ColumnDataRowIterationHelper::ColumnDataRowIterator(nullptr);
}

ColumnDataRowIterationHelper::ColumnDataRowIterator &ColumnDataRowIterationHelper::ColumnDataRowIterator::operator++() {
	Next();
	return *this;
}

bool ColumnDataRowIterationHelper::ColumnDataRowIterator::operator!=(const ColumnDataRowIterator &other) const {
	return collection != other.collection || current_row.row_index != other.current_row.row_index ||
	       current_row.base_index != other.current_row.base_index;
}

const ColumnDataRow &ColumnDataRowIterationHelper::ColumnDataRowIterator::operator*() const {
	return current_row;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
void ColumnDataCollection::InitializeAppend(ColumnDataAppendState &state) {
	D_ASSERT(!finished_append);
	state.vector_data.resize(types.size());
	if (segments.empty()) {
		CreateSegment();
	}
	auto &segment = *segments.back();
	if (segment.chunk_data.empty()) {
		segment.AllocateNewChunk();
	}
	segment.InitializeChunkState(segment.chunk_data.size() - 1, state.current_chunk_state);
}

void ColumnDataCopyValidity(const UnifiedVectorFormat &source_data, validity_t *target, idx_t source_offset,
                            idx_t target_offset, idx_t copy_count) {
	ValidityMask validity(target);
	if (target_offset == 0) {
		// first time appending to this vector
		// all data here is still uninitialized
		// initialize the validity mask to set all to valid
		validity.SetAllValid(STANDARD_VECTOR_SIZE);
	}
	// FIXME: we can do something more optimized here using bitshifts & bitwise ors
	if (!source_data.validity.AllValid()) {
		for (idx_t i = 0; i < copy_count; i++) {
			auto idx = source_data.sel->get_index(source_offset + i);
			if (!source_data.validity.RowIsValid(idx)) {
				validity.SetInvalid(target_offset + i);
			}
		}
	}
}

template <class T>
struct BaseValueCopy {
	static idx_t TypeSize() {
		return sizeof(T);
	}

	template <class OP>
	static void Assign(ColumnDataMetaData &meta_data, data_ptr_t target, data_ptr_t source, idx_t target_idx,
	                   idx_t source_idx) {
		auto result_data = (T *)target;
		auto source_data = (T *)source;
		result_data[target_idx] = OP::Operation(meta_data, source_data[source_idx]);
	}
};

template <class T>
struct StandardValueCopy : public BaseValueCopy<T> {
	static T Operation(ColumnDataMetaData &, T input) {
		return input;
	}
};

struct StringValueCopy : public BaseValueCopy<string_t> {
	static string_t Operation(ColumnDataMetaData &meta_data, string_t input) {
		return input.IsInlined() ? input : meta_data.segment.heap->AddBlob(input);
	}
};

struct ConstListValueCopy : public BaseValueCopy<list_entry_t> {
	using TYPE = list_entry_t;

	static TYPE Operation(ColumnDataMetaData &meta_data, TYPE input) {
		input.offset = meta_data.child_list_size;
		return input;
	}
};

struct ListValueCopy : public BaseValueCopy<list_entry_t> {
	using TYPE = list_entry_t;

	static TYPE Operation(ColumnDataMetaData &meta_data, TYPE input) {
		input.offset = meta_data.child_list_size;
		meta_data.child_list_size += input.length;
		return input;
	}
};

struct StructValueCopy {
	static idx_t TypeSize() {
		return 0;
	}

	template <class OP>
	static void Assign(ColumnDataMetaData &meta_data, data_ptr_t target, data_ptr_t source, idx_t target_idx,
	                   idx_t source_idx) {
	}
};

template <class OP>
static void TemplatedColumnDataCopy(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data,
                                    Vector &source, idx_t offset, idx_t count) {
	auto &segment = meta_data.segment;
	auto &append_state = meta_data.state;

	auto current_index = meta_data.vector_data_index;
	idx_t remaining = count;
	while (remaining > 0) {
		auto &current_segment = segment.GetVectorData(current_index);
		idx_t append_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE - current_segment.count, remaining);

		auto base_ptr = segment.allocator->GetDataPointer(append_state.current_chunk_state, current_segment.block_id,
		                                                  current_segment.offset);
		auto validity_data = ColumnDataCollectionSegment::GetValidityPointer(base_ptr, OP::TypeSize());

		ValidityMask result_validity(validity_data);
		if (current_segment.count == 0) {
			// first time appending to this vector
			// all data here is still uninitialized
			// initialize the validity mask to set all to valid
			result_validity.SetAllValid(STANDARD_VECTOR_SIZE);
		}
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_data.sel->get_index(offset + i);
			if (source_data.validity.RowIsValid(source_idx)) {
				OP::template Assign<OP>(meta_data, base_ptr, source_data.data, current_segment.count + i, source_idx);
			} else {
				result_validity.SetInvalid(current_segment.count + i);
			}
		}
		current_segment.count += append_count;
		offset += append_count;
		remaining -= append_count;
		if (remaining > 0) {
			// need to append more, check if we need to allocate a new vector or not
			if (!current_segment.next_data.IsValid()) {
				segment.AllocateVector(source.GetType(), meta_data.chunk_data, append_state, current_index);
			}
			D_ASSERT(segment.GetVectorData(current_index).next_data.IsValid());
			current_index = segment.GetVectorData(current_index).next_data;
		}
	}
}

template <class T>
static void ColumnDataCopy(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data, Vector &source,
                           idx_t offset, idx_t copy_count) {
	TemplatedColumnDataCopy<StandardValueCopy<T>>(meta_data, source_data, source, offset, copy_count);
}

template <>
void ColumnDataCopy<string_t>(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data, Vector &source,
                              idx_t offset, idx_t copy_count) {

	const auto &allocator_type = meta_data.segment.allocator->GetType();
	if (allocator_type == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR ||
	    allocator_type == ColumnDataAllocatorType::HYBRID) {
		// strings cannot be spilled to disk - use StringHeap
		TemplatedColumnDataCopy<StringValueCopy>(meta_data, source_data, source, offset, copy_count);
		return;
	}
	D_ASSERT(allocator_type == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);

	auto &segment = meta_data.segment;
	auto &append_state = meta_data.state;

	VectorDataIndex child_index;
	if (meta_data.GetVectorMetaData().child_index.IsValid()) {
		// find the last child index
		child_index = segment.GetChildIndex(meta_data.GetVectorMetaData().child_index);
		auto next_child_index = segment.GetVectorData(child_index).next_data;
		while (next_child_index.IsValid()) {
			child_index = next_child_index;
			next_child_index = segment.GetVectorData(child_index).next_data;
		}
	}

	auto current_index = meta_data.vector_data_index;
	idx_t remaining = copy_count;
	while (remaining > 0) {
		// how many values fit in the current string vector
		idx_t vector_remaining =
		    MinValue<idx_t>(STANDARD_VECTOR_SIZE - segment.GetVectorData(current_index).count, remaining);

		// 'append_count' is less if we cannot fit that amount of non-inlined strings on one buffer-managed block
		idx_t append_count;
		idx_t heap_size = 0;
		const auto source_entries = UnifiedVectorFormat::GetData<string_t>(source_data);
		for (append_count = 0; append_count < vector_remaining; append_count++) {
			auto source_idx = source_data.sel->get_index(offset + append_count);
			if (!source_data.validity.RowIsValid(source_idx)) {
				continue;
			}
			const auto &entry = source_entries[source_idx];
			if (entry.IsInlined()) {
				continue;
			}
			if (heap_size + entry.GetSize() > Storage::BLOCK_SIZE) {
				break;
			}
			heap_size += entry.GetSize();
		}

		if (vector_remaining != 0 && append_count == 0) {
			// single string is longer than Storage::BLOCK_SIZE
			// we allocate one block at a time for long strings
			auto source_idx = source_data.sel->get_index(offset + append_count);
			D_ASSERT(source_data.validity.RowIsValid(source_idx));
			D_ASSERT(!source_entries[source_idx].IsInlined());
			D_ASSERT(source_entries[source_idx].GetSize() > Storage::BLOCK_SIZE);
			heap_size += source_entries[source_idx].GetSize();
			append_count++;
		}

		// allocate string heap for the next 'append_count' strings
		data_ptr_t heap_ptr = nullptr;
		if (heap_size != 0) {
			child_index = segment.AllocateStringHeap(heap_size, meta_data.chunk_data, append_state, child_index);
			if (!meta_data.GetVectorMetaData().child_index.IsValid()) {
				meta_data.GetVectorMetaData().child_index = meta_data.segment.AddChildIndex(child_index);
			}
			auto &child_segment = segment.GetVectorData(child_index);
			heap_ptr = segment.allocator->GetDataPointer(append_state.current_chunk_state, child_segment.block_id,
			                                             child_segment.offset);
		}

		auto &current_segment = segment.GetVectorData(current_index);
		auto base_ptr = segment.allocator->GetDataPointer(append_state.current_chunk_state, current_segment.block_id,
		                                                  current_segment.offset);
		auto validity_data = ColumnDataCollectionSegment::GetValidityPointer(base_ptr, sizeof(string_t));
		ValidityMask target_validity(validity_data);
		if (current_segment.count == 0) {
			// first time appending to this vector
			// all data here is still uninitialized
			// initialize the validity mask to set all to valid
			target_validity.SetAllValid(STANDARD_VECTOR_SIZE);
		}

		auto target_entries = reinterpret_cast<string_t *>(base_ptr);
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_data.sel->get_index(offset + i);
			auto target_idx = current_segment.count + i;
			if (!source_data.validity.RowIsValid(source_idx)) {
				target_validity.SetInvalid(target_idx);
				continue;
			}
			const auto &source_entry = source_entries[source_idx];
			auto &target_entry = target_entries[target_idx];
			if (source_entry.IsInlined()) {
				target_entry = source_entry;
			} else {
				D_ASSERT(heap_ptr != nullptr);
				memcpy(heap_ptr, source_entry.GetData(), source_entry.GetSize());
				target_entry = string_t(const_char_ptr_cast(heap_ptr), source_entry.GetSize());
				heap_ptr += source_entry.GetSize();
			}
		}

		if (heap_size != 0) {
			current_segment.swizzle_data.emplace_back(child_index, current_segment.count, append_count);
		}

		current_segment.count += append_count;
		offset += append_count;
		remaining -= append_count;

		if (vector_remaining - append_count == 0) {
			// need to append more, check if we need to allocate a new vector or not
			if (!current_segment.next_data.IsValid()) {
				segment.AllocateVector(source.GetType(), meta_data.chunk_data, append_state, current_index);
			}
			D_ASSERT(segment.GetVectorData(current_index).next_data.IsValid());
			current_index = segment.GetVectorData(current_index).next_data;
		}
	}
}

template <>
void ColumnDataCopy<list_entry_t>(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data, Vector &source,
                                  idx_t offset, idx_t copy_count) {

	auto &segment = meta_data.segment;

	auto &child_vector = ListVector::GetEntry(source);
	auto &child_type = child_vector.GetType();

	if (!meta_data.GetVectorMetaData().child_index.IsValid()) {
		auto child_index = segment.AllocateVector(child_type, meta_data.chunk_data, meta_data.state);
		meta_data.GetVectorMetaData().child_index = meta_data.segment.AddChildIndex(child_index);
	}

	auto &child_function = meta_data.copy_function.child_functions[0];
	auto child_index = segment.GetChildIndex(meta_data.GetVectorMetaData().child_index);

	// figure out the current list size by traversing the set of child entries
	idx_t current_list_size = 0;
	auto current_child_index = child_index;
	while (current_child_index.IsValid()) {
		auto &child_vdata = segment.GetVectorData(current_child_index);
		current_list_size += child_vdata.count;
		current_child_index = child_vdata.next_data;
	}

	// set the child vector
	UnifiedVectorFormat child_vector_data;
	ColumnDataMetaData child_meta_data(child_function, meta_data, child_index);
	auto info = ListVector::GetConsecutiveChildListInfo(source, offset, copy_count);

	if (info.needs_slicing) {
		SelectionVector sel(info.child_list_info.length);
		ListVector::GetConsecutiveChildSelVector(source, sel, offset, copy_count);

		auto sliced_child_vector = Vector(child_vector, sel, info.child_list_info.length);
		sliced_child_vector.Flatten(info.child_list_info.length);
		info.child_list_info.offset = 0;

		sliced_child_vector.ToUnifiedFormat(info.child_list_info.length, child_vector_data);
		child_function.function(child_meta_data, child_vector_data, sliced_child_vector, info.child_list_info.offset,
		                        info.child_list_info.length);

	} else {
		child_vector.ToUnifiedFormat(info.child_list_info.length, child_vector_data);
		child_function.function(child_meta_data, child_vector_data, child_vector, info.child_list_info.offset,
		                        info.child_list_info.length);
	}

	// now copy the list entries
	meta_data.child_list_size = current_list_size;
	if (info.is_constant) {
		TemplatedColumnDataCopy<ConstListValueCopy>(meta_data, source_data, source, offset, copy_count);
	} else {
		TemplatedColumnDataCopy<ListValueCopy>(meta_data, source_data, source, offset, copy_count);
	}
}

void ColumnDataCopyStruct(ColumnDataMetaData &meta_data, const UnifiedVectorFormat &source_data, Vector &source,
                          idx_t offset, idx_t copy_count) {
	auto &segment = meta_data.segment;

	// copy the NULL values for the main struct vector
	TemplatedColumnDataCopy<StructValueCopy>(meta_data, source_data, source, offset, copy_count);

	auto &child_types = StructType::GetChildTypes(source.GetType());
	// now copy all the child vectors
	D_ASSERT(meta_data.GetVectorMetaData().child_index.IsValid());
	auto &child_vectors = StructVector::GetEntries(source);
	for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
		auto &child_function = meta_data.copy_function.child_functions[child_idx];
		auto child_index = segment.GetChildIndex(meta_data.GetVectorMetaData().child_index, child_idx);
		ColumnDataMetaData child_meta_data(child_function, meta_data, child_index);

		UnifiedVectorFormat child_data;
		child_vectors[child_idx]->ToUnifiedFormat(copy_count, child_data);

		child_function.function(child_meta_data, child_data, *child_vectors[child_idx], offset, copy_count);
	}
}

ColumnDataCopyFunction ColumnDataCollection::GetCopyFunction(const LogicalType &type) {
	ColumnDataCopyFunction result;
	column_data_copy_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = ColumnDataCopy<bool>;
		break;
	case PhysicalType::INT8:
		function = ColumnDataCopy<int8_t>;
		break;
	case PhysicalType::INT16:
		function = ColumnDataCopy<int16_t>;
		break;
	case PhysicalType::INT32:
		function = ColumnDataCopy<int32_t>;
		break;
	case PhysicalType::INT64:
		function = ColumnDataCopy<int64_t>;
		break;
	case PhysicalType::INT128:
		function = ColumnDataCopy<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = ColumnDataCopy<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = ColumnDataCopy<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = ColumnDataCopy<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = ColumnDataCopy<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = ColumnDataCopy<float>;
		break;
	case PhysicalType::DOUBLE:
		function = ColumnDataCopy<double>;
		break;
	case PhysicalType::INTERVAL:
		function = ColumnDataCopy<interval_t>;
		break;
	case PhysicalType::VARCHAR:
		function = ColumnDataCopy<string_t>;
		break;
	case PhysicalType::STRUCT: {
		function = ColumnDataCopyStruct;
		auto &child_types = StructType::GetChildTypes(type);
		for (auto &kv : child_types) {
			result.child_functions.push_back(GetCopyFunction(kv.second));
		}
		break;
	}
	case PhysicalType::LIST: {
		function = ColumnDataCopy<list_entry_t>;
		auto child_function = GetCopyFunction(ListType::GetChildType(type));
		result.child_functions.push_back(child_function);
		break;
	}
	default:
		throw InternalException("Unsupported type for ColumnDataCollection::GetCopyFunction");
	}
	result.function = function;
	return result;
}

static bool IsComplexType(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		return true;
	default:
		return false;
	};
}

void ColumnDataCollection::Append(ColumnDataAppendState &state, DataChunk &input) {
	D_ASSERT(!finished_append);
	D_ASSERT(types == input.GetTypes());

	auto &segment = *segments.back();
	for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
		if (IsComplexType(input.data[vector_idx].GetType())) {
			input.data[vector_idx].Flatten(input.size());
		}
		input.data[vector_idx].ToUnifiedFormat(input.size(), state.vector_data[vector_idx]);
	}

	idx_t remaining = input.size();
	while (remaining > 0) {
		auto &chunk_data = segment.chunk_data.back();
		idx_t append_amount = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE - chunk_data.count);
		if (append_amount > 0) {
			idx_t offset = input.size() - remaining;
			for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
				ColumnDataMetaData meta_data(copy_functions[vector_idx], segment, state, chunk_data,
				                             chunk_data.vector_data[vector_idx]);
				copy_functions[vector_idx].function(meta_data, state.vector_data[vector_idx], input.data[vector_idx],
				                                    offset, append_amount);
			}
			chunk_data.count += append_amount;
		}
		remaining -= append_amount;
		if (remaining > 0) {
			// more to do
			// allocate a new chunk
			segment.AllocateNewChunk();
			segment.InitializeChunkState(segment.chunk_data.size() - 1, state.current_chunk_state);
		}
	}
	segment.count += input.size();
	count += input.size();
}

void ColumnDataCollection::Append(DataChunk &input) {
	ColumnDataAppendState state;
	InitializeAppend(state);
	Append(state, input);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void ColumnDataCollection::InitializeScan(ColumnDataScanState &state, ColumnDataScanProperties properties) const {
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	InitializeScan(state, std::move(column_ids), properties);
}

void ColumnDataCollection::InitializeScan(ColumnDataScanState &state, vector<column_t> column_ids,
                                          ColumnDataScanProperties properties) const {
	state.chunk_index = 0;
	state.segment_index = 0;
	state.current_row_index = 0;
	state.next_row_index = 0;
	state.current_chunk_state.handles.clear();
	state.properties = properties;
	state.column_ids = std::move(column_ids);
}

void ColumnDataCollection::InitializeScan(ColumnDataParallelScanState &state,
                                          ColumnDataScanProperties properties) const {
	InitializeScan(state.scan_state, properties);
}

void ColumnDataCollection::InitializeScan(ColumnDataParallelScanState &state, vector<column_t> column_ids,
                                          ColumnDataScanProperties properties) const {
	InitializeScan(state.scan_state, std::move(column_ids), properties);
}

bool ColumnDataCollection::Scan(ColumnDataParallelScanState &state, ColumnDataLocalScanState &lstate,
                                DataChunk &result) const {
	result.Reset();

	idx_t chunk_index;
	idx_t segment_index;
	idx_t row_index;
	{
		lock_guard<mutex> l(state.lock);
		if (!NextScanIndex(state.scan_state, chunk_index, segment_index, row_index)) {
			return false;
		}
	}
	ScanAtIndex(state, lstate, result, chunk_index, segment_index, row_index);
	return true;
}

void ColumnDataCollection::InitializeScanChunk(DataChunk &chunk) const {
	chunk.Initialize(allocator->GetAllocator(), types);
}

void ColumnDataCollection::InitializeScanChunk(ColumnDataScanState &state, DataChunk &chunk) const {
	D_ASSERT(!state.column_ids.empty());
	vector<LogicalType> chunk_types;
	chunk_types.reserve(state.column_ids.size());
	for (idx_t i = 0; i < state.column_ids.size(); i++) {
		auto column_idx = state.column_ids[i];
		D_ASSERT(column_idx < types.size());
		chunk_types.push_back(types[column_idx]);
	}
	chunk.Initialize(allocator->GetAllocator(), chunk_types);
}

bool ColumnDataCollection::NextScanIndex(ColumnDataScanState &state, idx_t &chunk_index, idx_t &segment_index,
                                         idx_t &row_index) const {
	row_index = state.current_row_index = state.next_row_index;
	// check if we still have collections to scan
	if (state.segment_index >= segments.size()) {
		// no more data left in the scan
		return false;
	}
	// check within the current collection if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index]->chunk_data.size()) {
		// exhausted all chunks for this internal data structure: move to the next one
		state.chunk_index = 0;
		state.segment_index++;
		state.current_chunk_state.handles.clear();
		if (state.segment_index >= segments.size()) {
			return false;
		}
	}
	state.next_row_index += segments[state.segment_index]->chunk_data[state.chunk_index].count;
	segment_index = state.segment_index;
	chunk_index = state.chunk_index++;
	return true;
}

void ColumnDataCollection::ScanAtIndex(ColumnDataParallelScanState &state, ColumnDataLocalScanState &lstate,
                                       DataChunk &result, idx_t chunk_index, idx_t segment_index,
                                       idx_t row_index) const {
	if (segment_index != lstate.current_segment_index) {
		lstate.current_chunk_state.handles.clear();
		lstate.current_segment_index = segment_index;
	}
	auto &segment = *segments[segment_index];
	lstate.current_chunk_state.properties = state.scan_state.properties;
	segment.ReadChunk(chunk_index, lstate.current_chunk_state, result, state.scan_state.column_ids);
	lstate.current_row_index = row_index;
	result.Verify();
}

bool ColumnDataCollection::Scan(ColumnDataScanState &state, DataChunk &result) const {
	result.Reset();

	idx_t chunk_index;
	idx_t segment_index;
	idx_t row_index;
	if (!NextScanIndex(state, chunk_index, segment_index, row_index)) {
		return false;
	}

	// found a chunk to scan -> scan it
	auto &segment = *segments[segment_index];
	state.current_chunk_state.properties = state.properties;
	segment.ReadChunk(chunk_index, state.current_chunk_state, result, state.column_ids);
	result.Verify();
	return true;
}

ColumnDataRowCollection ColumnDataCollection::GetRows() const {
	return ColumnDataRowCollection(*this);
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void ColumnDataCollection::Combine(ColumnDataCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (types != other.types) {
		throw InternalException("Attempting to combine ColumnDataCollections with mismatching types");
	}
	this->count += other.count;
	this->segments.reserve(segments.size() + other.segments.size());
	for (auto &other_seg : other.segments) {
		segments.push_back(std::move(other_seg));
	}
	other.Reset();
	Verify();
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
idx_t ColumnDataCollection::ChunkCount() const {
	idx_t chunk_count = 0;
	for (auto &segment : segments) {
		chunk_count += segment->ChunkCount();
	}
	return chunk_count;
}

void ColumnDataCollection::FetchChunk(idx_t chunk_idx, DataChunk &result) const {
	D_ASSERT(chunk_idx < ChunkCount());
	for (auto &segment : segments) {
		if (chunk_idx >= segment->ChunkCount()) {
			chunk_idx -= segment->ChunkCount();
		} else {
			segment->FetchChunk(chunk_idx, result);
			return;
		}
	}
	throw InternalException("Failed to find chunk in ColumnDataCollection");
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
void ColumnDataCollection::Verify() {
#ifdef DEBUG
	// verify counts
	idx_t total_segment_count = 0;
	for (auto &segment : segments) {
		segment->Verify();
		total_segment_count += segment->count;
	}
	D_ASSERT(total_segment_count == this->count);
#endif
}

// LCOV_EXCL_START
string ColumnDataCollection::ToString() const {
	DataChunk chunk;
	InitializeScanChunk(chunk);

	ColumnDataScanState scan_state;
	InitializeScan(scan_state);

	string result = StringUtil::Format("ColumnDataCollection - [%llu Chunks, %llu Rows]\n", ChunkCount(), Count());
	idx_t chunk_idx = 0;
	idx_t row_count = 0;
	while (Scan(scan_state, chunk)) {
		result +=
		    StringUtil::Format("Chunk %llu - [Rows %llu - %llu]\n", chunk_idx, row_count, row_count + chunk.size()) +
		    chunk.ToString();
		chunk_idx++;
		row_count += chunk.size();
	}

	return result;
}
// LCOV_EXCL_STOP

void ColumnDataCollection::Print() const {
	Printer::Print(ToString());
}

void ColumnDataCollection::Reset() {
	count = 0;
	segments.clear();

	// Refreshes the ColumnDataAllocator to prevent holding on to allocated data unnecessarily
	allocator = make_shared<ColumnDataAllocator>(*allocator);
}

struct ValueResultEquals {
	bool operator()(const Value &a, const Value &b) const {
		return Value::DefaultValuesAreEqual(a, b);
	}
};

bool ColumnDataCollection::ResultEquals(const ColumnDataCollection &left, const ColumnDataCollection &right,
                                        string &error_message, bool ordered) {
	if (left.ColumnCount() != right.ColumnCount()) {
		error_message = "Column count mismatch";
		return false;
	}
	if (left.Count() != right.Count()) {
		error_message = "Row count mismatch";
		return false;
	}
	auto left_rows = left.GetRows();
	auto right_rows = right.GetRows();
	for (idx_t r = 0; r < left.Count(); r++) {
		for (idx_t c = 0; c < left.ColumnCount(); c++) {
			auto lvalue = left_rows.GetValue(c, r);
			auto rvalue = right_rows.GetValue(c, r);

			if (!Value::DefaultValuesAreEqual(lvalue, rvalue)) {
				error_message =
				    StringUtil::Format("%s <> %s (row: %lld, col: %lld)\n", lvalue.ToString(), rvalue.ToString(), r, c);
				break;
			}
		}
		if (!error_message.empty()) {
			if (ordered) {
				return false;
			} else {
				break;
			}
		}
	}
	if (!error_message.empty()) {
		// do an unordered comparison
		bool found_all = true;
		for (idx_t c = 0; c < left.ColumnCount(); c++) {
			std::unordered_multiset<Value, ValueHashFunction, ValueResultEquals> lvalues;
			for (idx_t r = 0; r < left.Count(); r++) {
				auto lvalue = left_rows.GetValue(c, r);
				lvalues.insert(lvalue);
			}
			for (idx_t r = 0; r < right.Count(); r++) {
				auto rvalue = right_rows.GetValue(c, r);
				auto entry = lvalues.find(rvalue);
				if (entry == lvalues.end()) {
					found_all = false;
					break;
				}
				lvalues.erase(entry);
			}
			if (!found_all) {
				break;
			}
		}
		if (!found_all) {
			return false;
		}
		error_message = string();
	}
	return true;
}

vector<shared_ptr<StringHeap>> ColumnDataCollection::GetHeapReferences() {
	vector<shared_ptr<StringHeap>> result(segments.size(), nullptr);
	for (idx_t segment_idx = 0; segment_idx < segments.size(); segment_idx++) {
		result[segment_idx] = segments[segment_idx]->heap;
	}
	return result;
}

ColumnDataAllocatorType ColumnDataCollection::GetAllocatorType() const {
	return allocator->GetType();
}

const vector<unique_ptr<ColumnDataCollectionSegment>> &ColumnDataCollection::GetSegments() const {
	return segments;
}

void ColumnDataCollection::Serialize(Serializer &serializer) const {
	vector<vector<Value>> values;
	values.resize(ColumnCount());
	for (auto &chunk : Chunks()) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				values[c].push_back(chunk.GetValue(c, r));
			}
		}
	}
	serializer.WriteProperty(100, "types", types);
	serializer.WriteProperty(101, "values", values);
}

unique_ptr<ColumnDataCollection> ColumnDataCollection::Deserialize(Deserializer &deserializer) {
	auto types = deserializer.ReadProperty<vector<LogicalType>>(100, "types");
	auto values = deserializer.ReadProperty<vector<vector<Value>>>(101, "values");

	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	if (values.empty()) {
		return collection;
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);

	for (idx_t r = 0; r < values[0].size(); r++) {
		for (idx_t c = 0; c < types.size(); c++) {
			chunk.SetValue(c, chunk.size(), values[c][r]);
		}
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
	}
	if (chunk.size() > 0) {
		collection->Append(chunk);
	}
	return collection;
}

} // namespace duckdb




namespace duckdb {

ColumnDataCollectionSegment::ColumnDataCollectionSegment(shared_ptr<ColumnDataAllocator> allocator_p,
                                                         vector<LogicalType> types_p)
    : allocator(std::move(allocator_p)), types(std::move(types_p)), count(0),
      heap(make_shared<StringHeap>(allocator->GetAllocator())) {
}

idx_t ColumnDataCollectionSegment::GetDataSize(idx_t type_size) {
	return AlignValue(type_size * STANDARD_VECTOR_SIZE);
}

validity_t *ColumnDataCollectionSegment::GetValidityPointer(data_ptr_t base_ptr, idx_t type_size) {
	return reinterpret_cast<validity_t *>(base_ptr + GetDataSize(type_size));
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVectorInternal(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                                    ChunkManagementState *chunk_state) {
	VectorMetaData meta_data;
	meta_data.count = 0;

	auto internal_type = type.InternalType();
	auto type_size = internal_type == PhysicalType::STRUCT ? 0 : GetTypeIdSize(internal_type);
	allocator->AllocateData(GetDataSize(type_size) + ValidityMask::STANDARD_MASK_SIZE, meta_data.block_id,
	                        meta_data.offset, chunk_state);
	if (allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR ||
	    allocator->GetType() == ColumnDataAllocatorType::HYBRID) {
		chunk_meta.block_ids.insert(meta_data.block_id);
	}

	auto index = vector_data.size();
	vector_data.push_back(meta_data);
	return VectorDataIndex(index);
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                            ChunkManagementState *chunk_state,
                                                            VectorDataIndex prev_index) {
	auto index = AllocateVectorInternal(type, chunk_meta, chunk_state);
	if (prev_index.IsValid()) {
		GetVectorData(prev_index).next_data = index;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		// initialize the struct children
		auto &child_types = StructType::GetChildTypes(type);
		auto base_child_index = ReserveChildren(child_types.size());
		for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
			VectorDataIndex prev_child_index;
			if (prev_index.IsValid()) {
				prev_child_index = GetChildIndex(GetVectorData(prev_index).child_index, child_idx);
			}
			auto child_index = AllocateVector(child_types[child_idx].second, chunk_meta, chunk_state, prev_child_index);
			SetChildIndex(base_child_index, child_idx, child_index);
		}
		GetVectorData(index).child_index = base_child_index;
	}
	return index;
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                            ColumnDataAppendState &append_state,
                                                            VectorDataIndex prev_index) {
	return AllocateVector(type, chunk_meta, &append_state.current_chunk_state, prev_index);
}

VectorDataIndex ColumnDataCollectionSegment::AllocateStringHeap(idx_t size, ChunkMetaData &chunk_meta,
                                                                ColumnDataAppendState &append_state,
                                                                VectorDataIndex prev_index) {
	D_ASSERT(allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	D_ASSERT(size != 0);

	VectorMetaData meta_data;
	meta_data.count = 0;

	allocator->AllocateData(AlignValue(size), meta_data.block_id, meta_data.offset, &append_state.current_chunk_state);
	chunk_meta.block_ids.insert(meta_data.block_id);

	VectorDataIndex index(vector_data.size());
	vector_data.push_back(meta_data);

	if (prev_index.IsValid()) {
		GetVectorData(prev_index).next_data = index;
	}

	return index;
}

void ColumnDataCollectionSegment::AllocateNewChunk() {
	ChunkMetaData meta_data;
	meta_data.count = 0;
	meta_data.vector_data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		auto vector_idx = AllocateVector(types[i], meta_data);
		meta_data.vector_data.push_back(vector_idx);
	}
	chunk_data.push_back(std::move(meta_data));
}

void ColumnDataCollectionSegment::InitializeChunkState(idx_t chunk_index, ChunkManagementState &state) {
	auto &chunk = chunk_data[chunk_index];
	allocator->InitializeChunkState(state, chunk);
}

VectorDataIndex ColumnDataCollectionSegment::GetChildIndex(VectorChildIndex index, idx_t child_entry) {
	D_ASSERT(index.IsValid());
	D_ASSERT(index.index + child_entry < child_indices.size());
	return VectorDataIndex(child_indices[index.index + child_entry]);
}

VectorChildIndex ColumnDataCollectionSegment::AddChildIndex(VectorDataIndex index) {
	auto result = child_indices.size();
	child_indices.push_back(index);
	return VectorChildIndex(result);
}

VectorChildIndex ColumnDataCollectionSegment::ReserveChildren(idx_t child_count) {
	auto result = child_indices.size();
	for (idx_t i = 0; i < child_count; i++) {
		child_indices.emplace_back();
	}
	return VectorChildIndex(result);
}

void ColumnDataCollectionSegment::SetChildIndex(VectorChildIndex base_idx, idx_t child_number, VectorDataIndex index) {
	D_ASSERT(base_idx.IsValid());
	D_ASSERT(index.IsValid());
	D_ASSERT(base_idx.index + child_number < child_indices.size());
	child_indices[base_idx.index + child_number] = index;
}

idx_t ColumnDataCollectionSegment::ReadVectorInternal(ChunkManagementState &state, VectorDataIndex vector_index,
                                                      Vector &result) {
	auto &vector_type = result.GetType();
	auto internal_type = vector_type.InternalType();
	auto type_size = GetTypeIdSize(internal_type);
	auto &vdata = GetVectorData(vector_index);

	auto base_ptr = allocator->GetDataPointer(state, vdata.block_id, vdata.offset);
	auto validity_data = GetValidityPointer(base_ptr, type_size);
	if (!vdata.next_data.IsValid() && state.properties != ColumnDataScanProperties::DISALLOW_ZERO_COPY) {
		// no next data, we can do a zero-copy read of this vector
		FlatVector::SetData(result, base_ptr);
		FlatVector::Validity(result).Initialize(validity_data);
		return vdata.count;
	}

	// the data for this vector is spread over multiple vector data entries
	// we need to copy over the data for each of the vectors
	// first figure out how many rows we need to copy by looping over all of the child vector indexes
	idx_t vector_count = 0;
	auto next_index = vector_index;
	while (next_index.IsValid()) {
		auto &current_vdata = GetVectorData(next_index);
		vector_count += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	// resize the result vector
	result.Resize(0, vector_count);
	next_index = vector_index;
	// now perform the copy of each of the vectors
	auto target_data = FlatVector::GetData(result);
	auto &target_validity = FlatVector::Validity(result);
	idx_t current_offset = 0;
	while (next_index.IsValid()) {
		auto &current_vdata = GetVectorData(next_index);
		base_ptr = allocator->GetDataPointer(state, current_vdata.block_id, current_vdata.offset);
		validity_data = GetValidityPointer(base_ptr, type_size);
		if (type_size > 0) {
			memcpy(target_data + current_offset * type_size, base_ptr, current_vdata.count * type_size);
		}
		ValidityMask current_validity(validity_data);
		target_validity.SliceInPlace(current_validity, current_offset, 0, current_vdata.count);
		current_offset += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	return vector_count;
}

idx_t ColumnDataCollectionSegment::ReadVector(ChunkManagementState &state, VectorDataIndex vector_index,
                                              Vector &result) {
	auto &vector_type = result.GetType();
	auto internal_type = vector_type.InternalType();
	auto &vdata = GetVectorData(vector_index);
	if (vdata.count == 0) {
		return 0;
	}
	auto vcount = ReadVectorInternal(state, vector_index, result);
	if (internal_type == PhysicalType::LIST) {
		// list: copy child
		auto &child_vector = ListVector::GetEntry(result);
		auto child_count = ReadVector(state, GetChildIndex(vdata.child_index), child_vector);
		ListVector::SetListSize(result, child_count);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &child_vectors = StructVector::GetEntries(result);
		for (idx_t child_idx = 0; child_idx < child_vectors.size(); child_idx++) {
			auto child_count =
			    ReadVector(state, GetChildIndex(vdata.child_index, child_idx), *child_vectors[child_idx]);
			if (child_count != vcount) {
				throw InternalException("Column Data Collection: mismatch in struct child sizes");
			}
		}
	} else if (internal_type == PhysicalType::VARCHAR) {
		if (allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
			auto next_index = vector_index;
			idx_t offset = 0;
			while (next_index.IsValid()) {
				auto &current_vdata = GetVectorData(next_index);
				for (auto &swizzle_segment : current_vdata.swizzle_data) {
					auto &string_heap_segment = GetVectorData(swizzle_segment.child_index);
					allocator->UnswizzlePointers(state, result, offset + swizzle_segment.offset, swizzle_segment.count,
					                             string_heap_segment.block_id, string_heap_segment.offset);
				}
				offset += current_vdata.count;
				next_index = current_vdata.next_data;
			}
		}
		if (state.properties == ColumnDataScanProperties::DISALLOW_ZERO_COPY) {
			VectorOperations::Copy(result, result, vdata.count, 0, 0);
		}
	}
	return vcount;
}

void ColumnDataCollectionSegment::ReadChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk,
                                            const vector<column_t> &column_ids) {
	D_ASSERT(chunk.ColumnCount() == column_ids.size());
	D_ASSERT(state.properties != ColumnDataScanProperties::INVALID);
	InitializeChunkState(chunk_index, state);
	auto &chunk_meta = chunk_data[chunk_index];
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto vector_idx = column_ids[i];
		D_ASSERT(vector_idx < chunk_meta.vector_data.size());
		ReadVector(state, chunk_meta.vector_data[vector_idx], chunk.data[i]);
	}
	chunk.SetCardinality(chunk_meta.count);
}

idx_t ColumnDataCollectionSegment::ChunkCount() const {
	return chunk_data.size();
}

idx_t ColumnDataCollectionSegment::SizeInBytes() const {
	D_ASSERT(!allocator->IsShared());
	return allocator->SizeInBytes() + heap->SizeInBytes();
}

void ColumnDataCollectionSegment::FetchChunk(idx_t chunk_idx, DataChunk &result) {
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	FetchChunk(chunk_idx, result, column_ids);
}

void ColumnDataCollectionSegment::FetchChunk(idx_t chunk_idx, DataChunk &result, const vector<column_t> &column_ids) {
	D_ASSERT(chunk_idx < chunk_data.size());
	ChunkManagementState state;
	state.properties = ColumnDataScanProperties::DISALLOW_ZERO_COPY;
	ReadChunk(chunk_idx, state, result, column_ids);
}

void ColumnDataCollectionSegment::Verify() {
#ifdef DEBUG
	idx_t total_count = 0;
	for (idx_t i = 0; i < chunk_data.size(); i++) {
		total_count += chunk_data[i].count;
	}
	D_ASSERT(total_count == this->count);
#endif
}

} // namespace duckdb


#include <algorithm>

namespace duckdb {

using ChunkReference = ColumnDataConsumer::ChunkReference;

ChunkReference::ChunkReference(ColumnDataCollectionSegment *segment_p, uint32_t chunk_index_p)
    : segment(segment_p), chunk_index_in_segment(chunk_index_p) {
}

uint32_t ChunkReference::GetMinimumBlockID() const {
	const auto &block_ids = segment->chunk_data[chunk_index_in_segment].block_ids;
	return *std::min_element(block_ids.begin(), block_ids.end());
}

ColumnDataConsumer::ColumnDataConsumer(ColumnDataCollection &collection_p, vector<column_t> column_ids)
    : collection(collection_p), column_ids(std::move(column_ids)) {
}

void ColumnDataConsumer::InitializeScan() {
	chunk_count = collection.ChunkCount();
	current_chunk_index = 0;
	chunk_delete_index = DConstants::INVALID_INDEX;

	// Initialize chunk references and sort them, so we can scan them in a sane order, regardless of how it was created
	chunk_references.reserve(chunk_count);
	for (auto &segment : collection.GetSegments()) {
		for (idx_t chunk_index = 0; chunk_index < segment->chunk_data.size(); chunk_index++) {
			chunk_references.emplace_back(segment.get(), chunk_index);
		}
	}
	std::sort(chunk_references.begin(), chunk_references.end());
}

bool ColumnDataConsumer::AssignChunk(ColumnDataConsumerScanState &state) {
	lock_guard<mutex> guard(lock);
	if (current_chunk_index == chunk_count) {
		// All chunks have been assigned
		state.current_chunk_state.handles.clear();
		state.chunk_index = DConstants::INVALID_INDEX;
		return false;
	}
	// Assign chunk index
	state.chunk_index = current_chunk_index++;
	D_ASSERT(chunks_in_progress.find(state.chunk_index) == chunks_in_progress.end());
	chunks_in_progress.insert(state.chunk_index);
	return true;
}

void ColumnDataConsumer::ScanChunk(ColumnDataConsumerScanState &state, DataChunk &chunk) const {
	D_ASSERT(state.chunk_index < chunk_count);
	auto &chunk_ref = chunk_references[state.chunk_index];
	if (state.allocator != chunk_ref.segment->allocator.get()) {
		// Previously scanned a chunk from a different allocator, reset the handles
		state.allocator = chunk_ref.segment->allocator.get();
		state.current_chunk_state.handles.clear();
	}
	chunk_ref.segment->ReadChunk(chunk_ref.chunk_index_in_segment, state.current_chunk_state, chunk, column_ids);
}

void ColumnDataConsumer::FinishChunk(ColumnDataConsumerScanState &state) {
	D_ASSERT(state.chunk_index < chunk_count);
	idx_t delete_index_start;
	idx_t delete_index_end;
	{
		lock_guard<mutex> guard(lock);
		D_ASSERT(chunks_in_progress.find(state.chunk_index) != chunks_in_progress.end());
		delete_index_start = chunk_delete_index;
		delete_index_end = *std::min_element(chunks_in_progress.begin(), chunks_in_progress.end());
		chunks_in_progress.erase(state.chunk_index);
		chunk_delete_index = delete_index_end;
	}
	ConsumeChunks(delete_index_start, delete_index_end);
}
void ColumnDataConsumer::ConsumeChunks(idx_t delete_index_start, idx_t delete_index_end) {
	for (idx_t chunk_index = delete_index_start; chunk_index < delete_index_end; chunk_index++) {
		if (chunk_index == 0) {
			continue;
		}
		auto &prev_chunk_ref = chunk_references[chunk_index - 1];
		auto &curr_chunk_ref = chunk_references[chunk_index];
		auto prev_allocator = prev_chunk_ref.segment->allocator.get();
		auto curr_allocator = curr_chunk_ref.segment->allocator.get();
		auto prev_min_block_id = prev_chunk_ref.GetMinimumBlockID();
		auto curr_min_block_id = curr_chunk_ref.GetMinimumBlockID();
		if (prev_allocator != curr_allocator) {
			// Moved to the next allocator, delete all remaining blocks in the previous one
			for (uint32_t block_id = prev_min_block_id; block_id < prev_allocator->BlockCount(); block_id++) {
				prev_allocator->DeleteBlock(block_id);
			}
			continue;
		}
		// Same allocator, see if we can delete blocks
		for (uint32_t block_id = prev_min_block_id; block_id < curr_min_block_id; block_id++) {
			prev_allocator->DeleteBlock(block_id);
		}
	}
}

} // namespace duckdb






namespace duckdb {

PartitionedColumnData::PartitionedColumnData(PartitionedColumnDataType type_p, ClientContext &context_p,
                                             vector<LogicalType> types_p)
    : type(type_p), context(context_p), types(std::move(types_p)),
      allocators(make_shared<PartitionColumnDataAllocators>()) {
}

PartitionedColumnData::PartitionedColumnData(const PartitionedColumnData &other)
    : type(other.type), context(other.context), types(other.types), allocators(other.allocators) {
}

unique_ptr<PartitionedColumnData> PartitionedColumnData::CreateShared() {
	switch (type) {
	case PartitionedColumnDataType::RADIX:
		return make_uniq<RadixPartitionedColumnData>(Cast<RadixPartitionedColumnData>());
	case PartitionedColumnDataType::HIVE:
		return make_uniq<HivePartitionedColumnData>(Cast<HivePartitionedColumnData>());
	default:
		throw NotImplementedException("CreateShared for this type of PartitionedColumnData");
	}
}

PartitionedColumnData::~PartitionedColumnData() {
}

void PartitionedColumnData::InitializeAppendState(PartitionedColumnDataAppendState &state) const {
	state.partition_sel.Initialize();
	state.slice_chunk.Initialize(BufferAllocator::Get(context), types);
	InitializeAppendStateInternal(state);
}

unique_ptr<DataChunk> PartitionedColumnData::CreatePartitionBuffer() const {
	auto result = make_uniq<DataChunk>();
	result->Initialize(BufferAllocator::Get(context), types, BufferSize());
	return result;
}

void PartitionedColumnData::Append(PartitionedColumnDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Compute the counts per partition
	const auto count = input.size();
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	auto &partition_entries = state.partition_entries;
	partition_entries.clear();
	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			const auto &partition_index = partition_indices[i];
			auto partition_entry = partition_entries.find(partition_index);
			if (partition_entry == partition_entries.end()) {
				partition_entries[partition_index] = list_entry_t(0, 1);
			} else {
				partition_entry->second.length++;
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedColumnData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_append_state = *state.partition_append_states[partition_index];
		partition.Append(partition_append_state, input);
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	for (auto &pc : partition_entries) {
		auto &partition_entry = pc.second;
		partition_entry.offset = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &all_partitions_sel = state.partition_sel;
	for (idx_t i = 0; i < count; i++) {
		const auto &partition_index = partition_indices[i];
		auto &partition_offset = partition_entries[partition_index].offset;
		all_partitions_sel[partition_offset++] = i;
	}

	// Loop through the partitions to append the new data to the partition buffers, and flush the buffers if necessary
	SelectionVector partition_sel;
	for (auto &pc : partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, buffer, and append state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_buffer = *state.partition_buffers[partition_index];
		auto &partition_append_state = *state.partition_append_states[partition_index];

		// Length and offset into the selection vector for this chunk, for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Create a selection vector for this partition using the offset into the single selection vector
		partition_sel.Initialize(all_partitions_sel.data() + partition_offset);

		if (partition_length >= HalfBufferSize()) {
			// Slice the input chunk using the selection vector
			state.slice_chunk.Reset();
			state.slice_chunk.Slice(input, partition_sel, partition_length);

			// Append it to the partition directly
			partition.Append(partition_append_state, state.slice_chunk);
		} else {
			// Append the input chunk to the partition buffer using the selection vector
			partition_buffer.Append(input, false, &partition_sel, partition_length);

			if (partition_buffer.size() >= HalfBufferSize()) {
				// Next batch won't fit in the buffer, flush it to the partition
				partition.Append(partition_append_state, partition_buffer);
				partition_buffer.Reset();
				partition_buffer.SetCapacity(BufferSize());
			}
		}
	}
}

void PartitionedColumnData::FlushAppendState(PartitionedColumnDataAppendState &state) {
	for (idx_t i = 0; i < state.partition_buffers.size(); i++) {
		auto &partition_buffer = *state.partition_buffers[i];
		if (partition_buffer.size() > 0) {
			partitions[i]->Append(partition_buffer);
			partition_buffer.Reset();
		}
	}
}

void PartitionedColumnData::Combine(PartitionedColumnData &other) {
	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);

	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = std::move(other.partitions);
	} else {
		D_ASSERT(partitions.size() == other.partitions.size());
		// Combine the append state's partitions into this PartitionedColumnData
		for (idx_t i = 0; i < other.partitions.size(); i++) {
			partitions[i]->Combine(*other.partitions[i]);
		}
	}
}

vector<unique_ptr<ColumnDataCollection>> &PartitionedColumnData::GetPartitions() {
	return partitions;
}

void PartitionedColumnData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared<ColumnDataAllocator>(BufferManager::GetBufferManager(context)));
	allocators->allocators.back()->MakeShared();
}

} // namespace duckdb



namespace duckdb {

bool ConflictInfo::ConflictTargetMatches(Index &index) const {
	if (only_check_unique && !index.IsUnique()) {
		// We only support checking ON CONFLICT for Unique/Primary key constraints
		return false;
	}
	if (column_ids.empty()) {
		return true;
	}
	// Check whether the column ids match
	return column_ids == index.column_id_set;
}

} // namespace duckdb





namespace duckdb {

ConflictManager::ConflictManager(VerifyExistenceType lookup_type, idx_t input_size,
                                 optional_ptr<ConflictInfo> conflict_info)
    : lookup_type(lookup_type), input_size(input_size), conflict_info(conflict_info), conflicts(input_size, false),
      mode(ConflictManagerMode::THROW) {
}

ManagedSelection &ConflictManager::InternalSelection() {
	if (!conflicts.Initialized()) {
		conflicts.Initialize(input_size);
	}
	return conflicts;
}

const unordered_set<idx_t> &ConflictManager::InternalConflictSet() const {
	D_ASSERT(conflict_set);
	return *conflict_set;
}

Vector &ConflictManager::InternalRowIds() {
	if (!row_ids) {
		row_ids = make_uniq<Vector>(LogicalType::ROW_TYPE, input_size);
	}
	return *row_ids;
}

Vector &ConflictManager::InternalIntermediate() {
	if (!intermediate_vector) {
		intermediate_vector = make_uniq<Vector>(LogicalType::BOOLEAN, true, true, input_size);
	}
	return *intermediate_vector;
}

const ConflictInfo &ConflictManager::GetConflictInfo() const {
	D_ASSERT(conflict_info);
	return *conflict_info;
}

void ConflictManager::FinishLookup() {
	if (mode == ConflictManagerMode::THROW) {
		return;
	}
	if (!SingleIndexTarget()) {
		return;
	}
	if (conflicts.Count() != 0) {
		// We have recorded conflicts from the one index we're interested in
		// We set this so we don't duplicate the conflicts when there are duplicate indexes
		// that also match our conflict target
		single_index_finished = true;
	}
}

void ConflictManager::SetMode(ConflictManagerMode mode) {
	// Only allow SCAN when we have conflict info
	D_ASSERT(mode != ConflictManagerMode::SCAN || conflict_info != nullptr);
	this->mode = mode;
}

void ConflictManager::AddToConflictSet(idx_t chunk_index) {
	if (!conflict_set) {
		conflict_set = make_uniq<unordered_set<idx_t>>();
	}
	auto &set = *conflict_set;
	set.insert(chunk_index);
}

void ConflictManager::AddConflictInternal(idx_t chunk_index, row_t row_id) {
	D_ASSERT(mode == ConflictManagerMode::SCAN);

	// Only when we should not throw on conflict should we get here
	D_ASSERT(!ShouldThrow(chunk_index));
	AddToConflictSet(chunk_index);
	if (SingleIndexTarget()) {
		// If we have identical indexes, only the conflicts of the first index should be recorded
		// as the other index(es) would produce the exact same conflicts anyways
		if (single_index_finished) {
			return;
		}

		// We can be more efficient because we don't need to merge conflicts of multiple indexes
		auto &selection = InternalSelection();
		auto &row_ids = InternalRowIds();
		auto data = FlatVector::GetData<row_t>(row_ids);
		data[selection.Count()] = row_id;
		selection.Append(chunk_index);
	} else {
		auto &intermediate = InternalIntermediate();
		auto data = FlatVector::GetData<bool>(intermediate);
		// Mark this index in the chunk as producing a conflict
		data[chunk_index] = true;
		if (row_id_map.empty()) {
			row_id_map.resize(input_size);
		}
		row_id_map[chunk_index] = row_id;
	}
}

bool ConflictManager::IsConflict(LookupResultType type) {
	switch (type) {
	case LookupResultType::LOOKUP_NULL: {
		if (ShouldIgnoreNulls()) {
			return false;
		}
		// If nulls are not ignored, treat this as a hit instead
		return IsConflict(LookupResultType::LOOKUP_HIT);
	}
	case LookupResultType::LOOKUP_HIT: {
		return true;
	}
	case LookupResultType::LOOKUP_MISS: {
		// FIXME: If we record a miss as a conflict when the verify type is APPEND_FK, then we can simplify the checks
		// in VerifyForeignKeyConstraint This also means we should not record a hit as a conflict when the verify type
		// is APPEND_FK
		return false;
	}
	default: {
		throw NotImplementedException("Type not implemented for LookupResultType");
	}
	}
}

bool ConflictManager::AddHit(idx_t chunk_index, row_t row_id) {
	D_ASSERT(chunk_index < input_size);
	// First check if this causes a conflict
	if (!IsConflict(LookupResultType::LOOKUP_HIT)) {
		return false;
	}

	// Then check if we should throw on a conflict
	if (ShouldThrow(chunk_index)) {
		return true;
	}
	if (mode == ConflictManagerMode::THROW) {
		// When our mode is THROW, and the chunk index is part of the previously scanned conflicts
		// then we ignore the conflict instead
		D_ASSERT(!ShouldThrow(chunk_index));
		return false;
	}
	D_ASSERT(conflict_info);
	// Because we don't throw, we need to register the conflict
	AddConflictInternal(chunk_index, row_id);
	return false;
}

bool ConflictManager::AddMiss(idx_t chunk_index) {
	D_ASSERT(chunk_index < input_size);
	return IsConflict(LookupResultType::LOOKUP_MISS);
}

bool ConflictManager::AddNull(idx_t chunk_index) {
	D_ASSERT(chunk_index < input_size);
	if (!IsConflict(LookupResultType::LOOKUP_NULL)) {
		return false;
	}
	return AddHit(chunk_index, DConstants::INVALID_INDEX);
}

bool ConflictManager::SingleIndexTarget() const {
	D_ASSERT(conflict_info);
	// We are only interested in a specific index
	return !conflict_info->column_ids.empty();
}

bool ConflictManager::ShouldThrow(idx_t chunk_index) const {
	if (mode == ConflictManagerMode::SCAN) {
		return false;
	}
	D_ASSERT(mode == ConflictManagerMode::THROW);
	if (conflict_set == nullptr) {
		// No conflicts were scanned, so this conflict is not in the set
		return true;
	}
	auto &set = InternalConflictSet();
	if (set.count(chunk_index)) {
		return false;
	}
	// None of the scanned conflicts arose from this insert tuple
	return true;
}

bool ConflictManager::ShouldIgnoreNulls() const {
	switch (lookup_type) {
	case VerifyExistenceType::APPEND:
		return true;
	case VerifyExistenceType::APPEND_FK:
		return false;
	case VerifyExistenceType::DELETE_FK:
		return true;
	default:
		throw InternalException("Type not implemented for VerifyExistenceType");
	}
}

Vector &ConflictManager::RowIds() {
	D_ASSERT(finalized);
	return *row_ids;
}

const ManagedSelection &ConflictManager::Conflicts() const {
	D_ASSERT(finalized);
	return conflicts;
}

idx_t ConflictManager::ConflictCount() const {
	return conflicts.Count();
}

void ConflictManager::Finalize() {
	D_ASSERT(!finalized);
	if (SingleIndexTarget()) {
		// Selection vector has been directly populated already, no need to finalize
		finalized = true;
		return;
	}
	finalized = true;
	if (!intermediate_vector) {
		// No conflicts were found, we're done
		return;
	}
	auto &intermediate = InternalIntermediate();
	auto data = FlatVector::GetData<bool>(intermediate);
	auto &selection = InternalSelection();
	// Create the selection vector from the encountered conflicts
	for (idx_t i = 0; i < input_size; i++) {
		if (data[i]) {
			selection.Append(i);
		}
	}
	// Now create the row_ids Vector, aligned with the selection vector
	auto &row_ids = InternalRowIds();
	auto row_id_data = FlatVector::GetData<row_t>(row_ids);

	for (idx_t i = 0; i < selection.Count(); i++) {
		D_ASSERT(!row_id_map.empty());
		auto index = selection[i];
		D_ASSERT(index < row_id_map.size());
		auto row_id = row_id_map[index];
		row_id_data[i] = row_id;
	}
	intermediate_vector.reset();
}

VerifyExistenceType ConflictManager::LookupType() const {
	return this->lookup_type;
}

void ConflictManager::SetIndexCount(idx_t count) {
	index_count = count;
}

} // namespace duckdb



















namespace duckdb {

DataChunk::DataChunk() : count(0), capacity(STANDARD_VECTOR_SIZE) {
}

DataChunk::~DataChunk() {
}

void DataChunk::InitializeEmpty(const vector<LogicalType> &types) {
	InitializeEmpty(types.begin(), types.end());
}

void DataChunk::Initialize(Allocator &allocator, const vector<LogicalType> &types, idx_t capacity_p) {
	Initialize(allocator, types.begin(), types.end(), capacity_p);
}

void DataChunk::Initialize(ClientContext &context, const vector<LogicalType> &types, idx_t capacity_p) {
	Initialize(Allocator::Get(context), types, capacity_p);
}

void DataChunk::Initialize(Allocator &allocator, vector<LogicalType>::const_iterator begin,
                           vector<LogicalType>::const_iterator end, idx_t capacity_p) {
	D_ASSERT(data.empty());                   // can only be initialized once
	D_ASSERT(std::distance(begin, end) != 0); // empty chunk not allowed
	capacity = capacity_p;
	for (; begin != end; begin++) {
		VectorCache cache(allocator, *begin, capacity);
		data.emplace_back(cache);
		vector_caches.push_back(std::move(cache));
	}
}

void DataChunk::Initialize(ClientContext &context, vector<LogicalType>::const_iterator begin,
                           vector<LogicalType>::const_iterator end, idx_t capacity_p) {
	Initialize(Allocator::Get(context), begin, end, capacity_p);
}

void DataChunk::InitializeEmpty(vector<LogicalType>::const_iterator begin, vector<LogicalType>::const_iterator end) {
	capacity = STANDARD_VECTOR_SIZE;
	D_ASSERT(data.empty());                   // can only be initialized once
	D_ASSERT(std::distance(begin, end) != 0); // empty chunk not allowed
	for (; begin != end; begin++) {
		data.emplace_back(*begin, nullptr);
	}
}

void DataChunk::Reset() {
	if (data.empty()) {
		return;
	}
	if (vector_caches.size() != data.size()) {
		throw InternalException("VectorCache and column count mismatch in DataChunk::Reset");
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].ResetFromCache(vector_caches[i]);
	}
	capacity = STANDARD_VECTOR_SIZE;
	SetCardinality(0);
}

void DataChunk::Destroy() {
	data.clear();
	vector_caches.clear();
	capacity = 0;
	SetCardinality(0);
}

Value DataChunk::GetValue(idx_t col_idx, idx_t index) const {
	D_ASSERT(index < size());
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, const Value &val) {
	data[col_idx].SetValue(index, val);
}

bool DataChunk::AllConstant() const {
	for (auto &v : data) {
		if (v.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			return false;
		}
	}
	return true;
}

void DataChunk::Reference(DataChunk &chunk) {
	D_ASSERT(chunk.ColumnCount() <= ColumnCount());
	SetCapacity(chunk);
	SetCardinality(chunk);
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		data[i].Reference(chunk.data[i]);
	}
}

void DataChunk::Move(DataChunk &chunk) {
	SetCardinality(chunk);
	SetCapacity(chunk);
	data = std::move(chunk.data);
	vector_caches = std::move(chunk.vector_caches);

	chunk.Destroy();
}

void DataChunk::Copy(DataChunk &other, idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset, 0);
	}
	other.SetCardinality(size() - offset);
}

void DataChunk::Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count, const idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);
	D_ASSERT((offset + source_count) <= size());

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], sel, source_count, offset, 0);
	}
	other.SetCardinality(source_count - offset);
}

void DataChunk::Split(DataChunk &other, idx_t split_idx) {
	D_ASSERT(other.size() == 0);
	D_ASSERT(other.data.empty());
	D_ASSERT(split_idx < data.size());
	const idx_t num_cols = data.size();
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		other.data.push_back(std::move(data[col_idx]));
		other.vector_caches.push_back(std::move(vector_caches[col_idx]));
	}
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		data.pop_back();
		vector_caches.pop_back();
	}
	other.SetCapacity(*this);
	other.SetCardinality(*this);
}

void DataChunk::Fuse(DataChunk &other) {
	D_ASSERT(other.size() == size());
	const idx_t num_cols = other.data.size();
	for (idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		data.emplace_back(std::move(other.data[col_idx]));
		vector_caches.emplace_back(std::move(other.vector_caches[col_idx]));
	}
	other.Destroy();
}

void DataChunk::ReferenceColumns(DataChunk &other, const vector<column_t> &column_ids) {
	D_ASSERT(ColumnCount() == column_ids.size());
	Reset();
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		auto &other_col = other.data[column_ids[col_idx]];
		auto &this_col = data[col_idx];
		D_ASSERT(other_col.GetType() == this_col.GetType());
		this_col.Reference(other_col);
	}
	SetCardinality(other.size());
}

void DataChunk::Append(const DataChunk &other, bool resize, SelectionVector *sel, idx_t sel_count) {
	idx_t new_size = sel ? size() + sel_count : size() + other.size();
	if (other.size() == 0) {
		return;
	}
	if (ColumnCount() != other.ColumnCount()) {
		throw InternalException("Column counts of appending chunk doesn't match!");
	}
	if (new_size > capacity) {
		if (resize) {
			auto new_capacity = NextPowerOfTwo(new_size);
			for (idx_t i = 0; i < ColumnCount(); i++) {
				data[i].Resize(size(), new_capacity);
			}
			capacity = new_capacity;
		} else {
			throw InternalException("Can't append chunk to other chunk without resizing");
		}
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		if (sel) {
			VectorOperations::Copy(other.data[i], data[i], *sel, sel_count, 0, size());
		} else {
			VectorOperations::Copy(other.data[i], data[i], other.size(), 0, size());
		}
	}
	SetCardinality(new_size);
}

void DataChunk::Flatten() {
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Flatten(size());
	}
}

vector<LogicalType> DataChunk::GetTypes() {
	vector<LogicalType> types;
	for (idx_t i = 0; i < ColumnCount(); i++) {
		types.push_back(data[i].GetType());
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(ColumnCount()) + " Columns]\n";
	for (idx_t i = 0; i < ColumnCount(); i++) {
		retval += "- " + data[i].ToString(size()) + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) const {

	// write the count
	auto row_count = size();
	serializer.WriteProperty<sel_t>(100, "rows", row_count);

	// we should never try to serialize empty data chunks
	auto column_count = ColumnCount();
	D_ASSERT(column_count);

	// write the types
	serializer.WriteList(101, "types", column_count,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(data[i].GetType()); });

	// write the data
	serializer.WriteList(102, "columns", column_count, [&](Serializer::List &list, idx_t i) {
		list.WriteObject([&](Serializer &object) {
			// Reference the vector to avoid potentially mutating it during serialization
			Vector serialized_vector(data[i].GetType());
			serialized_vector.Reference(data[i]);
			serialized_vector.Serialize(object, row_count);
		});
	});
}

void DataChunk::Deserialize(Deserializer &deserializer) {

	// read and set the row count
	auto row_count = deserializer.ReadProperty<sel_t>(100, "rows");

	// read the types
	vector<LogicalType> types;
	deserializer.ReadList(101, "types", [&](Deserializer::List &list, idx_t i) {
		auto type = list.ReadElement<LogicalType>();
		types.push_back(type);
	});

	// initialize the data chunk
	D_ASSERT(!types.empty());
	Initialize(Allocator::DefaultAllocator(), types);
	SetCardinality(row_count);

	// read the data
	deserializer.ReadList(102, "columns", [&](Deserializer::List &list, idx_t i) {
		list.ReadObject([&](Deserializer &object) { data[i].Deserialize(object, row_count); });
	});
}

void DataChunk::Slice(const SelectionVector &sel_vector, idx_t count_p) {
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < ColumnCount(); c++) {
		data[c].Slice(sel_vector, count_p, merge_cache);
	}
}

void DataChunk::Slice(DataChunk &other, const SelectionVector &sel, idx_t count_p, idx_t col_offset) {
	D_ASSERT(other.ColumnCount() <= col_offset + ColumnCount());
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < other.ColumnCount(); c++) {
		if (other.data[c].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			data[col_offset + c].Reference(other.data[c]);
			data[col_offset + c].Slice(sel, count_p, merge_cache);
		} else {
			data[col_offset + c].Slice(other.data[c], sel, count_p);
		}
	}
}

unsafe_unique_array<UnifiedVectorFormat> DataChunk::ToUnifiedFormat() {
	auto unified_data = make_unsafe_uniq_array<UnifiedVectorFormat>(ColumnCount());
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].ToUnifiedFormat(size(), unified_data[col_idx]);
	}
	return unified_data;
}

void DataChunk::Hash(Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	VectorOperations::Hash(data[0], result, size());
	for (idx_t i = 1; i < ColumnCount(); i++) {
		VectorOperations::CombineHash(result, data[i], size());
	}
}

void DataChunk::Hash(vector<idx_t> &column_ids, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	D_ASSERT(!column_ids.empty());

	VectorOperations::Hash(data[column_ids[0]], result, size());
	for (idx_t i = 1; i < column_ids.size(); i++) {
		VectorOperations::CombineHash(result, data[column_ids[i]], size());
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	D_ASSERT(size() <= capacity);

	// verify that all vectors in this chunk have the chunk selection vector
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Verify(size());
	}

	if (!ColumnCount()) {
		// don't try to round-trip dummy data chunks with no data
		// e.g., these exist in queries like 'SELECT distinct(col0, col1) FROM tbl', where we have groups, but no
		// payload so the payload will be such an empty data chunk
		return;
	}

	// verify that we can round-trip chunk serialization
	MemoryStream mem_stream;
	BinarySerializer serializer(mem_stream);

	serializer.Begin();
	Serialize(serializer);
	serializer.End();

	mem_stream.Rewind();

	BinaryDeserializer deserializer(mem_stream);
	DataChunk new_chunk;

	deserializer.Begin();
	new_chunk.Deserialize(deserializer);
	deserializer.End();

	D_ASSERT(size() == new_chunk.size());
#endif
}

void DataChunk::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb










#include <cstring>
#include <cctype>
#include <algorithm>

namespace duckdb {

static_assert(sizeof(date_t) == sizeof(int32_t), "date_t was padded");

const char *Date::PINF = "infinity";  // NOLINT
const char *Date::NINF = "-infinity"; // NOLINT
const char *Date::EPOCH = "epoch";    // NOLINT

const string_t Date::MONTH_NAMES_ABBREVIATED[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
const string_t Date::MONTH_NAMES[] = {"January", "February", "March",     "April",   "May",      "June",
                                      "July",    "August",   "September", "October", "November", "December"};
const string_t Date::DAY_NAMES[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
const string_t Date::DAY_NAMES_ABBREVIATED[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

const int32_t Date::NORMAL_DAYS[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_DAYS[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
const int32_t Date::LEAP_DAYS[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
const int32_t Date::CUMULATIVE_LEAP_DAYS[] = {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
const int8_t Date::MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int8_t Date::LEAP_MONTH_PER_DAY_OF_YEAR[] = {
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12,
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12};
const int32_t Date::CUMULATIVE_YEAR_DAYS[] = {
    0,      365,    730,    1096,   1461,   1826,   2191,   2557,   2922,   3287,   3652,   4018,   4383,   4748,
    5113,   5479,   5844,   6209,   6574,   6940,   7305,   7670,   8035,   8401,   8766,   9131,   9496,   9862,
    10227,  10592,  10957,  11323,  11688,  12053,  12418,  12784,  13149,  13514,  13879,  14245,  14610,  14975,
    15340,  15706,  16071,  16436,  16801,  17167,  17532,  17897,  18262,  18628,  18993,  19358,  19723,  20089,
    20454,  20819,  21184,  21550,  21915,  22280,  22645,  23011,  23376,  23741,  24106,  24472,  24837,  25202,
    25567,  25933,  26298,  26663,  27028,  27394,  27759,  28124,  28489,  28855,  29220,  29585,  29950,  30316,
    30681,  31046,  31411,  31777,  32142,  32507,  32872,  33238,  33603,  33968,  34333,  34699,  35064,  35429,
    35794,  36160,  36525,  36890,  37255,  37621,  37986,  38351,  38716,  39082,  39447,  39812,  40177,  40543,
    40908,  41273,  41638,  42004,  42369,  42734,  43099,  43465,  43830,  44195,  44560,  44926,  45291,  45656,
    46021,  46387,  46752,  47117,  47482,  47847,  48212,  48577,  48942,  49308,  49673,  50038,  50403,  50769,
    51134,  51499,  51864,  52230,  52595,  52960,  53325,  53691,  54056,  54421,  54786,  55152,  55517,  55882,
    56247,  56613,  56978,  57343,  57708,  58074,  58439,  58804,  59169,  59535,  59900,  60265,  60630,  60996,
    61361,  61726,  62091,  62457,  62822,  63187,  63552,  63918,  64283,  64648,  65013,  65379,  65744,  66109,
    66474,  66840,  67205,  67570,  67935,  68301,  68666,  69031,  69396,  69762,  70127,  70492,  70857,  71223,
    71588,  71953,  72318,  72684,  73049,  73414,  73779,  74145,  74510,  74875,  75240,  75606,  75971,  76336,
    76701,  77067,  77432,  77797,  78162,  78528,  78893,  79258,  79623,  79989,  80354,  80719,  81084,  81450,
    81815,  82180,  82545,  82911,  83276,  83641,  84006,  84371,  84736,  85101,  85466,  85832,  86197,  86562,
    86927,  87293,  87658,  88023,  88388,  88754,  89119,  89484,  89849,  90215,  90580,  90945,  91310,  91676,
    92041,  92406,  92771,  93137,  93502,  93867,  94232,  94598,  94963,  95328,  95693,  96059,  96424,  96789,
    97154,  97520,  97885,  98250,  98615,  98981,  99346,  99711,  100076, 100442, 100807, 101172, 101537, 101903,
    102268, 102633, 102998, 103364, 103729, 104094, 104459, 104825, 105190, 105555, 105920, 106286, 106651, 107016,
    107381, 107747, 108112, 108477, 108842, 109208, 109573, 109938, 110303, 110669, 111034, 111399, 111764, 112130,
    112495, 112860, 113225, 113591, 113956, 114321, 114686, 115052, 115417, 115782, 116147, 116513, 116878, 117243,
    117608, 117974, 118339, 118704, 119069, 119435, 119800, 120165, 120530, 120895, 121260, 121625, 121990, 122356,
    122721, 123086, 123451, 123817, 124182, 124547, 124912, 125278, 125643, 126008, 126373, 126739, 127104, 127469,
    127834, 128200, 128565, 128930, 129295, 129661, 130026, 130391, 130756, 131122, 131487, 131852, 132217, 132583,
    132948, 133313, 133678, 134044, 134409, 134774, 135139, 135505, 135870, 136235, 136600, 136966, 137331, 137696,
    138061, 138427, 138792, 139157, 139522, 139888, 140253, 140618, 140983, 141349, 141714, 142079, 142444, 142810,
    143175, 143540, 143905, 144271, 144636, 145001, 145366, 145732, 146097};

void Date::ExtractYearOffset(int32_t &n, int32_t &year, int32_t &year_offset) {
	year = Date::EPOCH_YEAR;
	// first we normalize n to be in the year range [1970, 2370]
	// since leap years repeat every 400 years, we can safely normalize just by "shifting" the CumulativeYearDays array
	while (n < 0) {
		n += Date::DAYS_PER_YEAR_INTERVAL;
		year -= Date::YEAR_INTERVAL;
	}
	while (n >= Date::DAYS_PER_YEAR_INTERVAL) {
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		year += Date::YEAR_INTERVAL;
	}
	// interpolation search
	// we can find an upper bound of the year by assuming each year has 365 days
	year_offset = n / 365;
	// because of leap years we might be off by a little bit: compensate by decrementing the year offset until we find
	// our year
	while (n < Date::CUMULATIVE_YEAR_DAYS[year_offset]) {
		year_offset--;
		D_ASSERT(year_offset >= 0);
	}
	year += year_offset;
	D_ASSERT(n >= Date::CUMULATIVE_YEAR_DAYS[year_offset]);
}

void Date::Convert(date_t d, int32_t &year, int32_t &month, int32_t &day) {
	auto n = d.days;
	int32_t year_offset;
	Date::ExtractYearOffset(n, year, year_offset);

	day = n - Date::CUMULATIVE_YEAR_DAYS[year_offset];
	D_ASSERT(day >= 0 && day <= 365);

	bool is_leap_year = (Date::CUMULATIVE_YEAR_DAYS[year_offset + 1] - Date::CUMULATIVE_YEAR_DAYS[year_offset]) == 366;
	if (is_leap_year) {
		month = Date::LEAP_MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_LEAP_DAYS[month - 1];
	} else {
		month = Date::MONTH_PER_DAY_OF_YEAR[day];
		day -= Date::CUMULATIVE_DAYS[month - 1];
	}
	day++;
	D_ASSERT(day > 0 && day <= (is_leap_year ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month]));
	D_ASSERT(month > 0 && month <= 12);
}

bool Date::TryFromDate(int32_t year, int32_t month, int32_t day, date_t &result) {
	int32_t n = 0;
	if (!Date::IsValid(year, month, day)) {
		return false;
	}
	n += Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month - 1] : Date::CUMULATIVE_DAYS[month - 1];
	n += day - 1;
	if (year < 1970) {
		int32_t diff_from_base = 1970 - year;
		int32_t year_index = 400 - (diff_from_base % 400);
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n -= Date::DAYS_PER_YEAR_INTERVAL;
		n -= fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else if (year >= 2370) {
		int32_t diff_from_base = year - 2370;
		int32_t year_index = diff_from_base % 400;
		int32_t fractions = diff_from_base / 400;
		n += Date::CUMULATIVE_YEAR_DAYS[year_index];
		n += Date::DAYS_PER_YEAR_INTERVAL;
		n += fractions * Date::DAYS_PER_YEAR_INTERVAL;
	} else {
		n += Date::CUMULATIVE_YEAR_DAYS[year - 1970];
	}
#ifdef DEBUG
	int32_t y, m, d;
	Date::Convert(date_t(n), y, m, d);
	D_ASSERT(year == y);
	D_ASSERT(month == m);
	D_ASSERT(day == d);
#endif
	result = date_t(n);
	return true;
}

date_t Date::FromDate(int32_t year, int32_t month, int32_t day) {
	date_t result;
	if (!Date::TryFromDate(year, month, day, result)) {
		throw ConversionException("Date out of range: %d-%d-%d", year, month, day);
	}
	return result;
}

bool Date::ParseDoubleDigit(const char *buf, idx_t len, idx_t &pos, int32_t &result) {
	if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
		result = buf[pos++] - '0';
		if (pos < len && StringUtil::CharacterIsDigit(buf[pos])) {
			result = (buf[pos++] - '0') + result * 10;
		}
		return true;
	}
	return false;
}

static bool TryConvertDateSpecial(const char *buf, idx_t len, idx_t &pos, const char *special) {
	auto p = pos;
	for (; p < len && *special; ++p) {
		const auto s = *special++;
		if (!s || StringUtil::CharacterToLower(buf[p]) != s) {
			return false;
		}
	}
	if (*special) {
		return false;
	}
	pos = p;
	return true;
}

bool Date::TryConvertDate(const char *buf, idx_t len, idx_t &pos, date_t &result, bool &special, bool strict) {
	special = false;
	pos = 0;
	if (len == 0) {
		return false;
	}

	int32_t day = 0;
	int32_t month = -1;
	int32_t year = 0;
	bool yearneg = false;
	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}
	if (buf[pos] == '-') {
		yearneg = true;
		pos++;
		if (pos >= len) {
			return false;
		}
	}
	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		// Check for special values
		if (TryConvertDateSpecial(buf, len, pos, PINF)) {
			result = yearneg ? date_t::ninfinity() : date_t::infinity();
		} else if (TryConvertDateSpecial(buf, len, pos, EPOCH)) {
			result = date_t::epoch();
		} else {
			return false;
		}
		// skip trailing spaces - parsing must be strict here
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		special = true;
		return pos == len;
	}
	// first parse the year
	for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++) {
		if (year >= 100000000) {
			return false;
		}
		year = (buf[pos] - '0') + year * 10;
	}
	if (yearneg) {
		year = -year;
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
		// invalid separator
		return false;
	}

	// parse the month
	if (!Date::ParseDoubleDigit(buf, len, pos, month)) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// now parse the day
	if (!Date::ParseDoubleDigit(buf, len, pos, day)) {
		return false;
	}

	// check for an optional trailing " (BC)""
	if (len - pos >= 5 && StringUtil::CharacterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
	    StringUtil::CharacterToLower(buf[pos + 2]) == 'b' && StringUtil::CharacterToLower(buf[pos + 3]) == 'c' &&
	    buf[pos + 4] == ')') {
		if (yearneg || year == 0) {
			return false;
		}
		year = -year + 1;
		pos += 5;
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace((unsigned char)buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	} else {
		// in non-strict mode, check for any direct trailing digits
		if (pos < len && StringUtil::CharacterIsDigit((unsigned char)buf[pos])) {
			return false;
		}
	}

	return Date::TryFromDate(year, month, day, result);
}

string Date::ConversionError(const string &str) {
	return StringUtil::Format("date field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD)",
	                          str);
}

string Date::ConversionError(string_t str) {
	return ConversionError(str.GetString());
}

date_t Date::FromCString(const char *buf, idx_t len, bool strict) {
	date_t result;
	idx_t pos;
	bool special = false;
	if (!TryConvertDate(buf, len, pos, result, special, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

date_t Date::FromString(const string &str, bool strict) {
	return Date::FromCString(str.c_str(), str.size(), strict);
}

string Date::ToString(date_t date) {
	// PG displays temporal infinities in lowercase,
	// but numerics in Titlecase.
	if (date == date_t::infinity()) {
		return PINF;
	} else if (date == date_t::ninfinity()) {
		return NINF;
	}
	int32_t date_units[3];
	idx_t year_length;
	bool add_bc;
	Date::Convert(date, date_units[0], date_units[1], date_units[2]);

	auto length = DateToStringCast::Length(date_units, year_length, add_bc);
	auto buffer = make_unsafe_uniq_array<char>(length);
	DateToStringCast::Format(buffer.get(), date_units, year_length, add_bc);
	return string(buffer.get(), length);
}

string Date::Format(int32_t year, int32_t month, int32_t day) {
	return ToString(Date::FromDate(year, month, day));
}

bool Date::IsLeapYear(int32_t year) {
	return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool Date::IsValid(int32_t year, int32_t month, int32_t day) {
	if (month < 1 || month > 12) {
		return false;
	}
	if (day < 1) {
		return false;
	}
	if (year <= DATE_MIN_YEAR) {
		if (year < DATE_MIN_YEAR) {
			return false;
		} else if (year == DATE_MIN_YEAR) {
			if (month < DATE_MIN_MONTH || (month == DATE_MIN_MONTH && day < DATE_MIN_DAY)) {
				return false;
			}
		}
	}
	if (year >= DATE_MAX_YEAR) {
		if (year > DATE_MAX_YEAR) {
			return false;
		} else if (year == DATE_MAX_YEAR) {
			if (month > DATE_MAX_MONTH || (month == DATE_MAX_MONTH && day > DATE_MAX_DAY)) {
				return false;
			}
		}
	}
	return Date::IsLeapYear(year) ? day <= Date::LEAP_DAYS[month] : day <= Date::NORMAL_DAYS[month];
}

int32_t Date::MonthDays(int32_t year, int32_t month) {
	D_ASSERT(month >= 1 && month <= 12);
	return Date::IsLeapYear(year) ? Date::LEAP_DAYS[month] : Date::NORMAL_DAYS[month];
}

date_t Date::EpochDaysToDate(int32_t epoch) {
	return (date_t)epoch;
}

int32_t Date::EpochDays(date_t date) {
	return date.days;
}

date_t Date::EpochToDate(int64_t epoch) {
	return date_t(epoch / Interval::SECS_PER_DAY);
}

int64_t Date::Epoch(date_t date) {
	return ((int64_t)date.days) * Interval::SECS_PER_DAY;
}

int64_t Date::EpochNanoseconds(date_t date) {
	int64_t result;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, Interval::MICROS_PER_DAY * 1000,
	                                                               result)) {
		throw ConversionException("Could not convert DATE (%s) to nanoseconds", Date::ToString(date));
	}
	return result;
}

int64_t Date::EpochMicroseconds(date_t date) {
	int64_t result;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, Interval::MICROS_PER_DAY, result)) {
		throw ConversionException("Could not convert DATE (%s) to microseconds", Date::ToString(date));
	}
	return result;
}

int64_t Date::EpochMilliseconds(date_t date) {
	int64_t result;
	const auto MILLIS_PER_DAY = Interval::MICROS_PER_DAY / Interval::MICROS_PER_MSEC;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, MILLIS_PER_DAY, result)) {
		throw ConversionException("Could not convert DATE (%s) to milliseconds", Date::ToString(date));
	}
	return result;
}

int32_t Date::ExtractYear(date_t d, int32_t *last_year) {
	auto n = d.days;
	// cached look up: check if year of this date is the same as the last one we looked up
	// note that this only works for years in the range [1970, 2370]
	if (n >= Date::CUMULATIVE_YEAR_DAYS[*last_year] && n < Date::CUMULATIVE_YEAR_DAYS[*last_year + 1]) {
		return Date::EPOCH_YEAR + *last_year;
	}
	int32_t year;
	Date::ExtractYearOffset(n, year, *last_year);
	return year;
}

int32_t Date::ExtractYear(timestamp_t ts, int32_t *last_year) {
	return Date::ExtractYear(Timestamp::GetDate(ts), last_year);
}

int32_t Date::ExtractYear(date_t d) {
	int32_t year, year_offset;
	Date::ExtractYearOffset(d.days, year, year_offset);
	return year;
}

int32_t Date::ExtractMonth(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_month;
}

int32_t Date::ExtractDay(date_t date) {
	int32_t out_year, out_month, out_day;
	Date::Convert(date, out_year, out_month, out_day);
	return out_day;
}

int32_t Date::ExtractDayOfTheYear(date_t date) {
	int32_t year, year_offset;
	Date::ExtractYearOffset(date.days, year, year_offset);
	return date.days - Date::CUMULATIVE_YEAR_DAYS[year_offset] + 1;
}

int64_t Date::ExtractJulianDay(date_t date) {
	// Julian Day 0 is (-4713, 11, 24) in the proleptic Gregorian calendar.
	static const int64_t JULIAN_EPOCH = -2440588;
	return date.days - JULIAN_EPOCH;
}

int32_t Date::ExtractISODayOfTheWeek(date_t date) {
	// date of 0 is 1970-01-01, which was a Thursday (4)
	// -7 = 4
	// -6 = 5
	// -5 = 6
	// -4 = 7
	// -3 = 1
	// -2 = 2
	// -1 = 3
	// 0  = 4
	// 1  = 5
	// 2  = 6
	// 3  = 7
	// 4  = 1
	// 5  = 2
	// 6  = 3
	// 7  = 4
	if (date.days < 0) {
		// negative date: start off at 4 and cycle downwards
		return (7 - ((-int64_t(date.days) + 3) % 7));
	} else {
		// positive date: start off at 4 and cycle upwards
		return ((int64_t(date.days) + 3) % 7) + 1;
	}
}

template <typename T>
static T PythonDivMod(const T &x, const T &y, T &r) {
	// D_ASSERT(y > 0);
	T quo = x / y;
	r = x - quo * y;
	if (r < 0) {
		--quo;
		r += y;
	}
	// D_ASSERT(0 <= r && r < y);
	return quo;
}

static date_t GetISOWeekOne(int32_t year) {
	const auto first_day = Date::FromDate(year, 1, 1); /* ord of 1/1 */
	/* 0 if 1/1 is a Monday, 1 if a Tue, etc. */
	const auto first_weekday = Date::ExtractISODayOfTheWeek(first_day) - 1;
	/* ordinal of closest Monday at or before 1/1 */
	auto week1_monday = first_day - first_weekday;

	if (first_weekday > 3) { /* if 1/1 was Fri, Sat, Sun */
		week1_monday += 7;
	}

	return week1_monday;
}

static int32_t GetISOYearWeek(const date_t date, int32_t &year) {
	int32_t month, day;
	Date::Convert(date, year, month, day);
	auto week1_monday = GetISOWeekOne(year);
	auto week = PythonDivMod((date.days - week1_monday.days), 7, day);
	if (week < 0) {
		week1_monday = GetISOWeekOne(--year);
		week = PythonDivMod((date.days - week1_monday.days), 7, day);
	} else if (week >= 52 && date >= GetISOWeekOne(year + 1)) {
		++year;
		week = 0;
	}

	return week + 1;
}

void Date::ExtractISOYearWeek(date_t date, int32_t &year, int32_t &week) {
	week = GetISOYearWeek(date, year);
}

int32_t Date::ExtractISOWeekNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return week;
}

int32_t Date::ExtractISOYearNumber(date_t date) {
	int32_t year, week;
	ExtractISOYearWeek(date, year, week);
	return year;
}

int32_t Date::ExtractWeekNumberRegular(date_t date, bool monday_first) {
	int32_t year, month, day;
	Date::Convert(date, year, month, day);
	month -= 1;
	day -= 1;
	// get the day of the year
	auto day_of_the_year =
	    (Date::IsLeapYear(year) ? Date::CUMULATIVE_LEAP_DAYS[month] : Date::CUMULATIVE_DAYS[month]) + day;
	// now figure out the first monday or sunday of the year
	// what day is January 1st?
	auto day_of_jan_first = Date::ExtractISODayOfTheWeek(Date::FromDate(year, 1, 1));
	// monday = 1, sunday = 7
	int32_t first_week_start;
	if (monday_first) {
		// have to find next "1"
		if (day_of_jan_first == 1) {
			// jan 1 is monday: starts immediately
			first_week_start = 0;
		} else {
			// jan 1 is not monday: count days until next monday
			first_week_start = 8 - day_of_jan_first;
		}
	} else {
		first_week_start = 7 - day_of_jan_first;
	}
	if (day_of_the_year < first_week_start) {
		// day occurs before first week starts: week 0
		return 0;
	}
	return ((day_of_the_year - first_week_start) / 7) + 1;
}

// Returns the date of the monday of the current week.
date_t Date::GetMondayOfCurrentWeek(date_t date) {
	int32_t dotw = Date::ExtractISODayOfTheWeek(date);
	return date - (dotw - 1);
}

} // namespace duckdb



namespace duckdb {

template <class SIGNED, class UNSIGNED>
string TemplatedDecimalToString(SIGNED value, uint8_t width, uint8_t scale) {
	auto len = DecimalToString::DecimalLength<SIGNED, UNSIGNED>(value, width, scale);
	auto data = make_unsafe_uniq_array<char>(len + 1);
	DecimalToString::FormatDecimal<SIGNED, UNSIGNED>(value, width, scale, data.get(), len);
	return string(data.get(), len);
}

string Decimal::ToString(int16_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int16_t, uint16_t>(value, width, scale);
}

string Decimal::ToString(int32_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int32_t, uint32_t>(value, width, scale);
}

string Decimal::ToString(int64_t value, uint8_t width, uint8_t scale) {
	return TemplatedDecimalToString<int64_t, uint64_t>(value, width, scale);
}

string Decimal::ToString(hugeint_t value, uint8_t width, uint8_t scale) {
	auto len = HugeintToStringCast::DecimalLength(value, width, scale);
	auto data = make_unsafe_uniq_array<char>(len + 1);
	HugeintToStringCast::FormatDecimal(value, width, scale, data.get(), len);
	return string(data.get(), len);
}

} // namespace duckdb






#include <functional>
#include <cmath>

namespace duckdb {

template <>
hash_t Hash(uint64_t val) {
	return murmurhash64(val);
}

template <>
hash_t Hash(int64_t val) {
	return murmurhash64((uint64_t)val);
}

template <>
hash_t Hash(hugeint_t val) {
	return murmurhash64(val.lower) ^ murmurhash64(val.upper);
}

template <class T>
struct FloatingPointEqualityTransform {
	static void OP(T &val) {
		if (val == (T)0.0) {
			// Turn negative zero into positive zero
			val = (T)0.0;
		} else if (std::isnan(val)) {
			val = std::numeric_limits<T>::quiet_NaN();
		}
	}
};

template <>
hash_t Hash(float val) {
	static_assert(sizeof(float) == sizeof(uint32_t), "");
	FloatingPointEqualityTransform<float>::OP(val);
	uint32_t uval = Load<uint32_t>(const_data_ptr_cast(&val));
	return murmurhash64(uval);
}

template <>
hash_t Hash(double val) {
	static_assert(sizeof(double) == sizeof(uint64_t), "");
	FloatingPointEqualityTransform<double>::OP(val);
	uint64_t uval = Load<uint64_t>(const_data_ptr_cast(&val));
	return murmurhash64(uval);
}

template <>
hash_t Hash(interval_t val) {
	return Hash(val.days) ^ Hash(val.months) ^ Hash(val.micros);
}

template <>
hash_t Hash(const char *str) {
	return Hash(str, strlen(str));
}

template <>
hash_t Hash(string_t val) {
	return Hash(val.GetData(), val.GetSize());
}

template <>
hash_t Hash(char *val) {
	return Hash<const char *>(val);
}

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
hash_t HashBytes(void *ptr, size_t len) noexcept {
	static constexpr uint64_t M = UINT64_C(0xc6a4a7935bd1e995);
	static constexpr uint64_t SEED = UINT64_C(0xe17a1465);
	static constexpr unsigned int R = 47;

	auto const *const data64 = static_cast<uint64_t const *>(ptr);
	uint64_t h = SEED ^ (len * M);

	size_t const n_blocks = len / 8;
	for (size_t i = 0; i < n_blocks; ++i) {
		auto k = Load<uint64_t>(reinterpret_cast<const_data_ptr_t>(data64 + i));

		k *= M;
		k ^= k >> R;
		k *= M;

		h ^= k;
		h *= M;
	}

	auto const *const data8 = reinterpret_cast<uint8_t const *>(data64 + n_blocks);
	switch (len & 7U) {
	case 7:
		h ^= static_cast<uint64_t>(data8[6]) << 48U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 6:
		h ^= static_cast<uint64_t>(data8[5]) << 40U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 5:
		h ^= static_cast<uint64_t>(data8[4]) << 32U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 4:
		h ^= static_cast<uint64_t>(data8[3]) << 24U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 3:
		h ^= static_cast<uint64_t>(data8[2]) << 16U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 2:
		h ^= static_cast<uint64_t>(data8[1]) << 8U;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	case 1:
		h ^= static_cast<uint64_t>(data8[0]);
		h *= M;
		DUCKDB_EXPLICIT_FALLTHROUGH;
	default:
		break;
	}
	h ^= h >> R;
	h *= M;
	h ^= h >> R;
	return static_cast<hash_t>(h);
}

hash_t Hash(const char *val, size_t size) {
	return HashBytes((void *)val, size);
}

hash_t Hash(uint8_t *val, size_t size) {
	return HashBytes((void *)val, size);
}

} // namespace duckdb









#include <cmath>
#include <limits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// String Conversion
//===--------------------------------------------------------------------===//
const hugeint_t Hugeint::POWERS_OF_TEN[] {
    hugeint_t(1),
    hugeint_t(10),
    hugeint_t(100),
    hugeint_t(1000),
    hugeint_t(10000),
    hugeint_t(100000),
    hugeint_t(1000000),
    hugeint_t(10000000),
    hugeint_t(100000000),
    hugeint_t(1000000000),
    hugeint_t(10000000000),
    hugeint_t(100000000000),
    hugeint_t(1000000000000),
    hugeint_t(10000000000000),
    hugeint_t(100000000000000),
    hugeint_t(1000000000000000),
    hugeint_t(10000000000000000),
    hugeint_t(100000000000000000),
    hugeint_t(1000000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10),
    hugeint_t(1000000000000000000) * hugeint_t(100),
    hugeint_t(1000000000000000000) * hugeint_t(1000),
    hugeint_t(1000000000000000000) * hugeint_t(10000),
    hugeint_t(1000000000000000000) * hugeint_t(100000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(10000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(100000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(10),
    hugeint_t(1000000000000000000) * hugeint_t(1000000000000000000) * hugeint_t(100)};

static uint8_t PositiveHugeintHighestBit(hugeint_t bits) {
	uint8_t out = 0;
	if (bits.upper) {
		out = 64;
		uint64_t up = bits.upper;
		while (up) {
			up >>= 1;
			out++;
		}
	} else {
		uint64_t low = bits.lower;
		while (low) {
			low >>= 1;
			out++;
		}
	}
	return out;
}

static bool PositiveHugeintIsBitSet(hugeint_t lhs, uint8_t bit_position) {
	if (bit_position < 64) {
		return lhs.lower & (uint64_t(1) << uint64_t(bit_position));
	} else {
		return lhs.upper & (uint64_t(1) << uint64_t(bit_position - 64));
	}
}

hugeint_t PositiveHugeintLeftShift(hugeint_t lhs, uint32_t amount) {
	D_ASSERT(amount > 0 && amount < 64);
	hugeint_t result;
	result.lower = lhs.lower << amount;
	result.upper = (lhs.upper << amount) + (lhs.lower >> (64 - amount));
	return result;
}

hugeint_t Hugeint::DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t &remainder) {
	D_ASSERT(lhs.upper >= 0);
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder = 0;

	uint8_t highest_bit_set = PositiveHugeintHighestBit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for (uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = PositiveHugeintLeftShift(div_result, 1);
		remainder <<= 1;
		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (PositiveHugeintIsBitSet(lhs, x - 1)) {
			// increment the remainder
			remainder++;
		}
		if (remainder >= rhs) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder -= rhs;
			div_result.lower++;
			if (div_result.lower == 0) {
				// overflow
				div_result.upper++;
			}
		}
	}
	return div_result;
}

string Hugeint::ToString(hugeint_t input) {
	uint64_t remainder;
	string result;
	bool negative = input.upper < 0;
	if (negative) {
		NegateInPlace(input);
	}
	while (true) {
		if (!input.lower && !input.upper) {
			break;
		}
		input = Hugeint::DivModPositive(input, 10, remainder);
		result = string(1, '0' + remainder) + result; // NOLINT
	}
	if (result.empty()) {
		// value is zero
		return "0";
	}
	return negative ? "-" + result : result;
}

//===--------------------------------------------------------------------===//
// Multiply
//===--------------------------------------------------------------------===//
bool Hugeint::TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t &result) {
	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		NegateInPlace(lhs);
	}
	if (rhs_negative) {
		NegateInPlace(rhs);
	}
#if ((__GNUC__ >= 5) || defined(__clang__)) && defined(__SIZEOF_INT128__)
	__uint128_t left = __uint128_t(lhs.lower) + (__uint128_t(lhs.upper) << 64);
	__uint128_t right = __uint128_t(rhs.lower) + (__uint128_t(rhs.upper) << 64);
	__uint128_t result_i128;
	if (__builtin_mul_overflow(left, right, &result_i128)) {
		return false;
	}
	uint64_t upper = uint64_t(result_i128 >> 64);
	if (upper & 0x8000000000000000) {
		return false;
	}
	result.upper = int64_t(upper);
	result.lower = uint64_t(result_i128 & 0xffffffffffffffff);
#else
	// Multiply code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// split values into 4 32-bit parts
	uint64_t top[4] = {uint64_t(lhs.upper) >> 32, uint64_t(lhs.upper) & 0xffffffff, lhs.lower >> 32,
	                   lhs.lower & 0xffffffff};
	uint64_t bottom[4] = {uint64_t(rhs.upper) >> 32, uint64_t(rhs.upper) & 0xffffffff, rhs.lower >> 32,
	                      rhs.lower & 0xffffffff};
	uint64_t products[4][4];

	// multiply each component of the values
	for (auto x = 0; x < 4; x++) {
		for (auto y = 0; y < 4; y++) {
			products[x][y] = top[x] * bottom[y];
		}
	}

	// if any of these products are set to a non-zero value, there is always an overflow
	if (products[0][0] || products[0][1] || products[0][2] || products[1][0] || products[2][0] || products[1][1]) {
		return false;
	}
	// if the high bits of any of these are set, there is always an overflow
	if ((products[0][3] & 0xffffffff80000000) || (products[1][2] & 0xffffffff80000000) ||
	    (products[2][1] & 0xffffffff80000000) || (products[3][0] & 0xffffffff80000000)) {
		return false;
	}

	// otherwise we merge the result of the different products together in-order

	// first row
	uint64_t fourth32 = (products[3][3] & 0xffffffff);
	uint64_t third32 = (products[3][2] & 0xffffffff) + (products[3][3] >> 32);
	uint64_t second32 = (products[3][1] & 0xffffffff) + (products[3][2] >> 32);
	uint64_t first32 = (products[3][0] & 0xffffffff) + (products[3][1] >> 32);

	// second row
	third32 += (products[2][3] & 0xffffffff);
	second32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);
	first32 += (products[2][1] & 0xffffffff) + (products[2][2] >> 32);

	// third row
	second32 += (products[1][3] & 0xffffffff);
	first32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);

	// fourth row
	first32 += (products[0][3] & 0xffffffff);

	// move carry to next digit
	third32 += fourth32 >> 32;
	second32 += third32 >> 32;
	first32 += second32 >> 32;

	// check if the combination of the different products resulted in an overflow
	if (first32 & 0xffffff80000000) {
		return false;
	}

	// remove carry from current digit
	fourth32 &= 0xffffffff;
	third32 &= 0xffffffff;
	second32 &= 0xffffffff;
	first32 &= 0xffffffff;

	// combine components
	result.lower = (third32 << 32) | fourth32;
	result.upper = (first32 << 32) | second32;
#endif
	if (lhs_negative ^ rhs_negative) {
		NegateInPlace(result);
	}
	return true;
}

hugeint_t Hugeint::Multiply(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t result;
	if (!TryMultiply(lhs, rhs, result)) {
		throw OutOfRangeException("Overflow in HUGEINT multiplication!");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Divide
//===--------------------------------------------------------------------===//
hugeint_t Hugeint::DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t &remainder) {
	// division by zero not allowed
	D_ASSERT(!(rhs.upper == 0 && rhs.lower == 0));

	bool lhs_negative = lhs.upper < 0;
	bool rhs_negative = rhs.upper < 0;
	if (lhs_negative) {
		Hugeint::NegateInPlace(lhs);
	}
	if (rhs_negative) {
		Hugeint::NegateInPlace(rhs);
	}
	// DivMod code adapted from:
	// https://github.com/calccrypto/uint128_t/blob/master/uint128_t.cpp

	// initialize the result and remainder to 0
	hugeint_t div_result;
	div_result.lower = 0;
	div_result.upper = 0;
	remainder.lower = 0;
	remainder.upper = 0;

	uint8_t highest_bit_set = PositiveHugeintHighestBit(lhs);
	// now iterate over the amount of bits that are set in the LHS
	for (uint8_t x = highest_bit_set; x > 0; x--) {
		// left-shift the current result and remainder by 1
		div_result = PositiveHugeintLeftShift(div_result, 1);
		remainder = PositiveHugeintLeftShift(remainder, 1);

		// we get the value of the bit at position X, where position 0 is the least-significant bit
		if (PositiveHugeintIsBitSet(lhs, x - 1)) {
			// increment the remainder
			Hugeint::AddInPlace(remainder, 1);
		}
		if (Hugeint::GreaterThanEquals(remainder, rhs)) {
			// the remainder has passed the division multiplier: add one to the divide result
			remainder = Hugeint::Subtract(remainder, rhs);
			Hugeint::AddInPlace(div_result, 1);
		}
	}
	if (lhs_negative ^ rhs_negative) {
		Hugeint::NegateInPlace(div_result);
	}
	if (lhs_negative) {
		Hugeint::NegateInPlace(remainder);
	}
	return div_result;
}

hugeint_t Hugeint::Divide(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	return Hugeint::DivMod(lhs, rhs, remainder);
}

hugeint_t Hugeint::Modulo(hugeint_t lhs, hugeint_t rhs) {
	hugeint_t remainder;
	Hugeint::DivMod(lhs, rhs, remainder);
	return remainder;
}

//===--------------------------------------------------------------------===//
// Add/Subtract
//===--------------------------------------------------------------------===//
bool Hugeint::AddInPlace(hugeint_t &lhs, hugeint_t rhs) {
	int overflow = lhs.lower + rhs.lower < lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for overflow
		if (lhs.upper > (std::numeric_limits<int64_t>::max() - rhs.upper - overflow)) {
			return false;
		}
		lhs.upper = lhs.upper + overflow + rhs.upper;
	} else {
		// RHS is negative: check for underflow
		if (lhs.upper < std::numeric_limits<int64_t>::min() - rhs.upper - overflow) {
			return false;
		}
		lhs.upper = lhs.upper + (overflow + rhs.upper);
	}
	lhs.lower += rhs.lower;
	if (lhs.upper == std::numeric_limits<int64_t>::min() && lhs.lower == 0) {
		return false;
	}
	return true;
}

bool Hugeint::SubtractInPlace(hugeint_t &lhs, hugeint_t rhs) {
	// underflow
	int underflow = lhs.lower - rhs.lower > lhs.lower;
	if (rhs.upper >= 0) {
		// RHS is positive: check for underflow
		if (lhs.upper < (std::numeric_limits<int64_t>::min() + rhs.upper + underflow)) {
			return false;
		}
		lhs.upper = (lhs.upper - rhs.upper) - underflow;
	} else {
		// RHS is negative: check for overflow
		if (lhs.upper > std::numeric_limits<int64_t>::min() &&
		    lhs.upper - 1 >= (std::numeric_limits<int64_t>::max() + rhs.upper + underflow)) {
			return false;
		}
		lhs.upper = lhs.upper - (rhs.upper + underflow);
	}
	lhs.lower -= rhs.lower;
	if (lhs.upper == std::numeric_limits<int64_t>::min() && lhs.lower == 0) {
		return false;
	}
	return true;
}

hugeint_t Hugeint::Add(hugeint_t lhs, hugeint_t rhs) {
	if (!AddInPlace(lhs, rhs)) {
		throw OutOfRangeException("Overflow in HUGEINT addition");
	}
	return lhs;
}

hugeint_t Hugeint::Subtract(hugeint_t lhs, hugeint_t rhs) {
	if (!SubtractInPlace(lhs, rhs)) {
		throw OutOfRangeException("Underflow in HUGEINT addition");
	}
	return lhs;
}

//===--------------------------------------------------------------------===//
// Hugeint Cast/Conversion
//===--------------------------------------------------------------------===//
template <class DST, bool SIGNED = true>
bool HugeintTryCastInteger(hugeint_t input, DST &result) {
	switch (input.upper) {
	case 0:
		// positive number: check if the positive number is in range
		if (input.lower <= uint64_t(NumericLimits<DST>::Maximum())) {
			result = DST(input.lower);
			return true;
		}
		break;
	case -1:
		if (!SIGNED) {
			return false;
		}
		// negative number: check if the negative number is in range
		if (input.lower >= NumericLimits<uint64_t>::Maximum() - uint64_t(NumericLimits<DST>::Maximum())) {
			result = -DST(NumericLimits<uint64_t>::Maximum() - input.lower) - 1;
			return true;
		}
		break;
	default:
		break;
	}
	return false;
}

template <>
bool Hugeint::TryCast(hugeint_t input, int8_t &result) {
	return HugeintTryCastInteger<int8_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int16_t &result) {
	return HugeintTryCastInteger<int16_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int32_t &result) {
	return HugeintTryCastInteger<int32_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, int64_t &result) {
	return HugeintTryCastInteger<int64_t>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint8_t &result) {
	return HugeintTryCastInteger<uint8_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint16_t &result) {
	return HugeintTryCastInteger<uint16_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint32_t &result) {
	return HugeintTryCastInteger<uint32_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, uint64_t &result) {
	return HugeintTryCastInteger<uint64_t, false>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, hugeint_t &result) {
	result = input;
	return true;
}

template <>
bool Hugeint::TryCast(hugeint_t input, float &result) {
	double dbl_result;
	Hugeint::TryCast(input, dbl_result);
	result = (float)dbl_result;
	return true;
}

template <class REAL_T>
bool CastBigintToFloating(hugeint_t input, REAL_T &result) {
	switch (input.upper) {
	case -1:
		// special case for upper = -1 to avoid rounding issues in small negative numbers
		result = -REAL_T(NumericLimits<uint64_t>::Maximum() - input.lower) - 1;
		break;
	default:
		result = REAL_T(input.lower) + REAL_T(input.upper) * REAL_T(NumericLimits<uint64_t>::Maximum());
		break;
	}
	return true;
}

template <>
bool Hugeint::TryCast(hugeint_t input, double &result) {
	return CastBigintToFloating<double>(input, result);
}

template <>
bool Hugeint::TryCast(hugeint_t input, long double &result) {
	return CastBigintToFloating<long double>(input, result);
}

template <class DST>
hugeint_t HugeintConvertInteger(DST input) {
	hugeint_t result;
	result.lower = (uint64_t)input;
	result.upper = (input < 0) * -1;
	return result;
}

template <>
bool Hugeint::TryConvert(int8_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int8_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(const char *value, hugeint_t &result) {
	auto len = strlen(value);
	string_t string_val(value, len);
	return TryCast::Operation<string_t, hugeint_t>(string_val, result, true);
}

template <>
bool Hugeint::TryConvert(int16_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int16_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(int32_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int32_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(int64_t value, hugeint_t &result) {
	result = HugeintConvertInteger<int64_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint8_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint8_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint16_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint16_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint32_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint32_t>(value);
	return true;
}
template <>
bool Hugeint::TryConvert(uint64_t value, hugeint_t &result) {
	result = HugeintConvertInteger<uint64_t>(value);
	return true;
}

template <>
bool Hugeint::TryConvert(hugeint_t value, hugeint_t &result) {
	result = value;
	return true;
}

template <>
bool Hugeint::TryConvert(float value, hugeint_t &result) {
	return Hugeint::TryConvert(double(value), result);
}

template <class REAL_T>
bool ConvertFloatingToBigint(REAL_T value, hugeint_t &result) {
	if (!Value::IsFinite<REAL_T>(value)) {
		return false;
	}
	if (value <= -170141183460469231731687303715884105728.0 || value >= 170141183460469231731687303715884105727.0) {
		return false;
	}
	bool negative = value < 0;
	if (negative) {
		value = -value;
	}
	result.lower = (uint64_t)fmod(value, REAL_T(NumericLimits<uint64_t>::Maximum()));
	result.upper = (uint64_t)(value / REAL_T(NumericLimits<uint64_t>::Maximum()));
	if (negative) {
		Hugeint::NegateInPlace(result);
	}
	return true;
}

template <>
bool Hugeint::TryConvert(double value, hugeint_t &result) {
	return ConvertFloatingToBigint<double>(value, result);
}

template <>
bool Hugeint::TryConvert(long double value, hugeint_t &result) {
	return ConvertFloatingToBigint<long double>(value, result);
}

//===--------------------------------------------------------------------===//
// hugeint_t operators
//===--------------------------------------------------------------------===//
hugeint_t::hugeint_t(int64_t value) {
	auto result = Hugeint::Convert(value);
	this->lower = result.lower;
	this->upper = result.upper;
}

bool hugeint_t::operator==(const hugeint_t &rhs) const {
	return Hugeint::Equals(*this, rhs);
}

bool hugeint_t::operator!=(const hugeint_t &rhs) const {
	return Hugeint::NotEquals(*this, rhs);
}

bool hugeint_t::operator<(const hugeint_t &rhs) const {
	return Hugeint::LessThan(*this, rhs);
}

bool hugeint_t::operator<=(const hugeint_t &rhs) const {
	return Hugeint::LessThanEquals(*this, rhs);
}

bool hugeint_t::operator>(const hugeint_t &rhs) const {
	return Hugeint::GreaterThan(*this, rhs);
}

bool hugeint_t::operator>=(const hugeint_t &rhs) const {
	return Hugeint::GreaterThanEquals(*this, rhs);
}

hugeint_t hugeint_t::operator+(const hugeint_t &rhs) const {
	return Hugeint::Add(*this, rhs);
}

hugeint_t hugeint_t::operator-(const hugeint_t &rhs) const {
	return Hugeint::Subtract(*this, rhs);
}

hugeint_t hugeint_t::operator*(const hugeint_t &rhs) const {
	return Hugeint::Multiply(*this, rhs);
}

hugeint_t hugeint_t::operator/(const hugeint_t &rhs) const {
	return Hugeint::Divide(*this, rhs);
}

hugeint_t hugeint_t::operator%(const hugeint_t &rhs) const {
	return Hugeint::Modulo(*this, rhs);
}

hugeint_t hugeint_t::operator-() const {
	return Hugeint::Negate(*this);
}

hugeint_t hugeint_t::operator>>(const hugeint_t &rhs) const {
	hugeint_t result;
	uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return hugeint_t(0);
	} else if (shift == 0) {
		return *this;
	} else if (shift == 64) {
		result.upper = (upper < 0) ? -1 : 0;
		result.lower = upper;
	} else if (shift < 64) {
		// perform lower shift in unsigned integer, and mask away the most significant bit
		result.lower = (uint64_t(upper) << (64 - shift)) | (lower >> shift);
		result.upper = upper >> shift;
	} else {
		D_ASSERT(shift < 128);
		result.lower = upper >> (shift - 64);
		result.upper = (upper < 0) ? -1 : 0;
	}
	return result;
}

hugeint_t hugeint_t::operator<<(const hugeint_t &rhs) const {
	if (upper < 0) {
		return hugeint_t(0);
	}
	hugeint_t result;
	uint64_t shift = rhs.lower;
	if (rhs.upper != 0 || shift >= 128) {
		return hugeint_t(0);
	} else if (shift == 64) {
		result.upper = lower;
		result.lower = 0;
	} else if (shift == 0) {
		return *this;
	} else if (shift < 64) {
		// perform upper shift in unsigned integer, and mask away the most significant bit
		uint64_t upper_shift = ((uint64_t(upper) << shift) + (lower >> (64 - shift))) & 0x7FFFFFFFFFFFFFFF;
		result.lower = lower << shift;
		result.upper = upper_shift;
	} else {
		D_ASSERT(shift < 128);
		result.lower = 0;
		result.upper = (lower << (shift - 64)) & 0x7FFFFFFFFFFFFFFF;
	}
	return result;
}

hugeint_t hugeint_t::operator&(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower & rhs.lower;
	result.upper = upper & rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator|(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower | rhs.lower;
	result.upper = upper | rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator^(const hugeint_t &rhs) const {
	hugeint_t result;
	result.lower = lower ^ rhs.lower;
	result.upper = upper ^ rhs.upper;
	return result;
}

hugeint_t hugeint_t::operator~() const {
	hugeint_t result;
	result.lower = ~lower;
	result.upper = ~upper;
	return result;
}

hugeint_t &hugeint_t::operator+=(const hugeint_t &rhs) {
	Hugeint::AddInPlace(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator-=(const hugeint_t &rhs) {
	Hugeint::SubtractInPlace(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator*=(const hugeint_t &rhs) {
	*this = Hugeint::Multiply(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator/=(const hugeint_t &rhs) {
	*this = Hugeint::Divide(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator%=(const hugeint_t &rhs) {
	*this = Hugeint::Modulo(*this, rhs);
	return *this;
}
hugeint_t &hugeint_t::operator>>=(const hugeint_t &rhs) {
	*this = *this >> rhs;
	return *this;
}
hugeint_t &hugeint_t::operator<<=(const hugeint_t &rhs) {
	*this = *this << rhs;
	return *this;
}
hugeint_t &hugeint_t::operator&=(const hugeint_t &rhs) {
	lower &= rhs.lower;
	upper &= rhs.upper;
	return *this;
}
hugeint_t &hugeint_t::operator|=(const hugeint_t &rhs) {
	lower |= rhs.lower;
	upper |= rhs.upper;
	return *this;
}
hugeint_t &hugeint_t::operator^=(const hugeint_t &rhs) {
	lower ^= rhs.lower;
	upper ^= rhs.upper;
	return *this;
}

bool hugeint_t::operator!() const {
	return *this == 0;
}

hugeint_t::operator bool() const {
	return *this != 0;
}

template <class T>
static T NarrowCast(const hugeint_t &input) {
	// NarrowCast is supposed to truncate (take lower)
	return static_cast<T>(input.lower);
}

hugeint_t::operator uint8_t() const {
	return NarrowCast<uint8_t>(*this);
}
hugeint_t::operator uint16_t() const {
	return NarrowCast<uint16_t>(*this);
}
hugeint_t::operator uint32_t() const {
	return NarrowCast<uint32_t>(*this);
}
hugeint_t::operator uint64_t() const {
	return NarrowCast<uint64_t>(*this);
}
hugeint_t::operator int8_t() const {
	return NarrowCast<int8_t>(*this);
}
hugeint_t::operator int16_t() const {
	return NarrowCast<int16_t>(*this);
}
hugeint_t::operator int32_t() const {
	return NarrowCast<int32_t>(*this);
}
hugeint_t::operator int64_t() const {
	return NarrowCast<int64_t>(*this);
}

string hugeint_t::ToString() const {
	return Hugeint::ToString(*this);
}

} // namespace duckdb








namespace duckdb {

HyperLogLog::HyperLogLog() : hll(nullptr) {
	hll = duckdb_hll::hll_create();
	// Insert into a dense hll can be vectorized, sparse cannot, so we immediately convert
	duckdb_hll::hllSparseToDense(hll);
}

HyperLogLog::HyperLogLog(duckdb_hll::robj *hll) : hll(hll) {
}

HyperLogLog::~HyperLogLog() {
	duckdb_hll::hll_destroy(hll);
}

void HyperLogLog::Add(data_ptr_t element, idx_t size) {
	if (duckdb_hll::hll_add(hll, element, size) == HLL_C_ERR) {
		throw InternalException("Could not add to HLL?");
	}
}

idx_t HyperLogLog::Count() const {
	// exception from size_t ban
	size_t result;

	if (duckdb_hll::hll_count(hll, &result) != HLL_C_OK) {
		throw InternalException("Could not count HLL?");
	}
	return result;
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = hll;
	hlls[1] = other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog(new_hll));
}

HyperLogLog *HyperLogLog::MergePointer(HyperLogLog &other) {
	duckdb_hll::robj *hlls[2];
	hlls[0] = hll;
	hlls[1] = other.hll;
	auto new_hll = duckdb_hll::hll_merge(hlls, 2);
	if (!new_hll) {
		throw Exception("Could not merge HLLs");
	}
	return new HyperLogLog(new_hll);
}

unique_ptr<HyperLogLog> HyperLogLog::Merge(HyperLogLog logs[], idx_t count) {
	auto hlls_uptr = unique_ptr<duckdb_hll::robj *[]> {
		new duckdb_hll::robj *[count]
	};
	auto hlls = hlls_uptr.get();
	for (idx_t i = 0; i < count; i++) {
		hlls[i] = logs[i].hll;
	}
	auto new_hll = duckdb_hll::hll_merge(hlls, count);
	if (!new_hll) {
		throw InternalException("Could not merge HLLs");
	}
	return unique_ptr<HyperLogLog>(new HyperLogLog(new_hll));
}

idx_t HyperLogLog::GetSize() {
	return duckdb_hll::get_size();
}

data_ptr_t HyperLogLog::GetPtr() const {
	return data_ptr_cast((hll)->ptr);
}

unique_ptr<HyperLogLog> HyperLogLog::Copy() {
	auto result = make_uniq<HyperLogLog>();
	lock_guard<mutex> guard(lock);
	memcpy(result->GetPtr(), GetPtr(), GetSize());
	D_ASSERT(result->Count() == Count());
	return result;
}

void HyperLogLog::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", HLLStorageType::UNCOMPRESSED);
	serializer.WriteProperty(101, "data", GetPtr(), GetSize());
}

unique_ptr<HyperLogLog> HyperLogLog::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<HyperLogLog>();
	auto storage_type = deserializer.ReadProperty<HLLStorageType>(100, "type");
	switch (storage_type) {
	case HLLStorageType::UNCOMPRESSED:
		deserializer.ReadProperty(101, "data", result->GetPtr(), GetSize());
		break;
	default:
		throw SerializationException("Unknown HyperLogLog storage type!");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Vectorized HLL implementation
//===--------------------------------------------------------------------===//
//! Taken from https://nullprogram.com/blog/2018/07/31/
template <class T>
inline uint64_t TemplatedHash(const T &elem) {
	uint64_t x = elem;
	x ^= x >> 30;
	x *= UINT64_C(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64_C(0x94d049bb133111eb);
	x ^= x >> 31;
	return x;
}

template <>
inline uint64_t TemplatedHash(const hugeint_t &elem) {
	return TemplatedHash<uint64_t>(Load<uint64_t>(const_data_ptr_cast(&elem.upper))) ^
	       TemplatedHash<uint64_t>(elem.lower);
}

template <idx_t rest>
inline void CreateIntegerRecursive(const_data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[rest - 1] << ((rest - 1) * 8);
	return CreateIntegerRecursive<rest - 1>(data, x);
}

template <>
inline void CreateIntegerRecursive<1>(const_data_ptr_t &data, uint64_t &x) {
	x ^= (uint64_t)data[0];
}

inline uint64_t HashOtherSize(const_data_ptr_t &data, const idx_t &len) {
	uint64_t x = 0;
	switch (len & 7) {
	case 7:
		CreateIntegerRecursive<7>(data, x);
		break;
	case 6:
		CreateIntegerRecursive<6>(data, x);
		break;
	case 5:
		CreateIntegerRecursive<5>(data, x);
		break;
	case 4:
		CreateIntegerRecursive<4>(data, x);
		break;
	case 3:
		CreateIntegerRecursive<3>(data, x);
		break;
	case 2:
		CreateIntegerRecursive<2>(data, x);
		break;
	case 1:
		CreateIntegerRecursive<1>(data, x);
		break;
	case 0:
		break;
	}
	return TemplatedHash<uint64_t>(x);
}

template <>
inline uint64_t TemplatedHash(const string_t &elem) {
	auto data = const_data_ptr_cast(elem.GetData());
	const auto &len = elem.GetSize();
	uint64_t h = 0;
	for (idx_t i = 0; i + sizeof(uint64_t) <= len; i += sizeof(uint64_t)) {
		h ^= TemplatedHash<uint64_t>(Load<uint64_t>(data));
		data += sizeof(uint64_t);
	}
	switch (len & (sizeof(uint64_t) - 1)) {
	case 4:
		h ^= TemplatedHash<uint32_t>(Load<uint32_t>(data));
		break;
	case 2:
		h ^= TemplatedHash<uint16_t>(Load<uint16_t>(data));
		break;
	case 1:
		h ^= TemplatedHash<uint8_t>(Load<uint8_t>(data));
		break;
	default:
		h ^= HashOtherSize(data, len);
	}
	return h;
}

template <class T>
void TemplatedComputeHashes(UnifiedVectorFormat &vdata, const idx_t &count, uint64_t hashes[]) {
	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			hashes[i] = TemplatedHash<T>(data[idx]);
		} else {
			hashes[i] = 0;
		}
	}
}

static void ComputeHashes(UnifiedVectorFormat &vdata, const LogicalType &type, uint64_t hashes[], idx_t count) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return TemplatedComputeHashes<uint8_t>(vdata, count, hashes);
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return TemplatedComputeHashes<uint16_t>(vdata, count, hashes);
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
	case PhysicalType::FLOAT:
		return TemplatedComputeHashes<uint32_t>(vdata, count, hashes);
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
	case PhysicalType::DOUBLE:
		return TemplatedComputeHashes<uint64_t>(vdata, count, hashes);
	case PhysicalType::INT128:
	case PhysicalType::INTERVAL:
		static_assert(sizeof(hugeint_t) == sizeof(interval_t), "ComputeHashes assumes these are the same size!");
		return TemplatedComputeHashes<hugeint_t>(vdata, count, hashes);
	case PhysicalType::VARCHAR:
		return TemplatedComputeHashes<string_t>(vdata, count, hashes);
	default:
		throw InternalException("Unimplemented type for HyperLogLog::ComputeHashes");
	}
}

//! Taken from https://stackoverflow.com/a/72088344
static inline uint8_t CountTrailingZeros(uint64_t &x) {
	static constexpr const uint64_t DEBRUIJN = 0x03f79d71b4cb0a89;
	static constexpr const uint8_t LOOKUP[] = {0,  47, 1,  56, 48, 27, 2,  60, 57, 49, 41, 37, 28, 16, 3,  61,
	                                           54, 58, 35, 52, 50, 42, 21, 44, 38, 32, 29, 23, 17, 11, 4,  62,
	                                           46, 55, 26, 59, 40, 36, 15, 53, 34, 51, 20, 43, 31, 22, 10, 45,
	                                           25, 39, 14, 33, 19, 30, 9,  24, 13, 18, 8,  12, 7,  6,  5,  63};
	return LOOKUP[(DEBRUIJN * (x ^ (x - 1))) >> 58];
}

static inline void ComputeIndexAndCount(uint64_t &hash, uint8_t &prefix) {
	uint64_t index = hash & ((1 << 12) - 1); /* Register index. */
	hash >>= 12;                             /* Remove bits used to address the register. */
	hash |= ((uint64_t)1 << (64 - 12));      /* Make sure the count will be <= Q+1. */

	prefix = CountTrailingZeros(hash) + 1; /* Add 1 since we count the "00000...1" pattern. */
	hash = index;
}

void HyperLogLog::ProcessEntries(UnifiedVectorFormat &vdata, const LogicalType &type, uint64_t hashes[],
                                 uint8_t counts[], idx_t count) {
	ComputeHashes(vdata, type, hashes, count);
	for (idx_t i = 0; i < count; i++) {
		ComputeIndexAndCount(hashes[i], counts[i]);
	}
}

void HyperLogLog::AddToLogs(UnifiedVectorFormat &vdata, idx_t count, uint64_t indices[], uint8_t counts[],
                            HyperLogLog **logs[], const SelectionVector *log_sel) {
	AddToLogsInternal(vdata, count, indices, counts, reinterpret_cast<void ****>(logs), log_sel);
}

void HyperLogLog::AddToLog(UnifiedVectorFormat &vdata, idx_t count, uint64_t indices[], uint8_t counts[]) {
	lock_guard<mutex> guard(lock);
	AddToSingleLogInternal(vdata, count, indices, counts, hll);
}

} // namespace duckdb
















namespace duckdb {

bool Interval::FromString(const string &str, interval_t &result) {
	string error_message;
	return Interval::FromCString(str.c_str(), str.size(), result, &error_message, false);
}

template <class T>
void IntervalTryAddition(T &target, int64_t input, int64_t multiplier) {
	int64_t addition;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, multiplier, addition)) {
		throw OutOfRangeException("interval value is out of range");
	}
	T addition_base = Cast::Operation<int64_t, T>(addition);
	if (!TryAddOperator::Operation<T, T, T>(target, addition_base, target)) {
		throw OutOfRangeException("interval value is out of range");
	}
}

bool Interval::FromCString(const char *str, idx_t len, interval_t &result, string *error_message, bool strict) {
	idx_t pos = 0;
	idx_t start_pos;
	bool negative;
	bool found_any = false;
	int64_t number;
	DatePartSpecifier specifier;
	string specifier_str;

	result.days = 0;
	result.micros = 0;
	result.months = 0;

	if (len == 0) {
		return false;
	}

	switch (str[pos]) {
	case '@':
		pos++;
		goto standard_interval;
	case 'P':
	case 'p':
		pos++;
		goto posix_interval;
	default:
		goto standard_interval;
	}
standard_interval:
	// start parsing a standard interval (e.g. 2 years 3 months...)
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces
			continue;
		} else if (c >= '0' && c <= '9') {
			// start parsing a positive number
			negative = false;
			goto interval_parse_number;
		} else if (c == '-') {
			// negative number
			negative = true;
			pos++;
			goto interval_parse_number;
		} else if (c == 'a' || c == 'A') {
			// parse the word "ago" as the final specifier
			goto interval_parse_ago;
		} else {
			// unrecognized character, expected a number or end of string
			return false;
		}
	}
	goto end_of_string;
interval_parse_number:
	start_pos = pos;
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c >= '0' && c <= '9') {
			// the number continues
			continue;
		} else if (c == ':') {
			// colon: we are parsing a time
			goto interval_parse_time;
		} else {
			if (pos == start_pos) {
				return false;
			}
			// finished the number, parse it from the string
			string_t nr_string(str + start_pos, pos - start_pos);
			number = Cast::Operation<string_t, int64_t>(nr_string);
			if (negative) {
				number = -number;
			}
			goto interval_parse_identifier;
		}
	}
	goto end_of_string;
interval_parse_time : {
	// parse the remainder of the time as a Time type
	dtime_t time;
	idx_t pos;
	if (!Time::TryConvertTime(str + start_pos, len - start_pos, pos, time)) {
		return false;
	}
	result.micros += time.micros;
	found_any = true;
	if (negative) {
		result.micros = -result.micros;
	}
	goto end_of_string;
}
interval_parse_identifier:
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			// skip spaces at the start
			continue;
		} else {
			break;
		}
	}
	// now parse the identifier
	start_pos = pos;
	for (; pos < len; pos++) {
		char c = str[pos];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			// keep parsing the string
			continue;
		} else {
			break;
		}
	}
	specifier_str = string(str + start_pos, pos - start_pos);
	if (!TryGetDatePartSpecifier(specifier_str, specifier)) {
		HandleCastError::AssignError(StringUtil::Format("extract specifier \"%s\" not recognized", specifier_str),
		                             error_message);
		return false;
	}
	// add the specifier to the interval
	switch (specifier) {
	case DatePartSpecifier::MILLENNIUM:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_MILLENIUM);
		break;
	case DatePartSpecifier::CENTURY:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_CENTURY);
		break;
	case DatePartSpecifier::DECADE:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_DECADE);
		break;
	case DatePartSpecifier::YEAR:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_YEAR);
		break;
	case DatePartSpecifier::QUARTER:
		IntervalTryAddition<int32_t>(result.months, number, MONTHS_PER_QUARTER);
		break;
	case DatePartSpecifier::MONTH:
		IntervalTryAddition<int32_t>(result.months, number, 1);
		break;
	case DatePartSpecifier::DAY:
		IntervalTryAddition<int32_t>(result.days, number, 1);
		break;
	case DatePartSpecifier::WEEK:
		IntervalTryAddition<int32_t>(result.days, number, DAYS_PER_WEEK);
		break;
	case DatePartSpecifier::MICROSECONDS:
		IntervalTryAddition<int64_t>(result.micros, number, 1);
		break;
	case DatePartSpecifier::MILLISECONDS:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_MSEC);
		break;
	case DatePartSpecifier::SECOND:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_SEC);
		break;
	case DatePartSpecifier::MINUTE:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_MINUTE);
		break;
	case DatePartSpecifier::HOUR:
		IntervalTryAddition<int64_t>(result.micros, number, MICROS_PER_HOUR);
		break;
	default:
		HandleCastError::AssignError(
		    StringUtil::Format("extract specifier \"%s\" not supported for interval", specifier_str), error_message);
		return false;
	}
	found_any = true;
	goto standard_interval;
interval_parse_ago:
	D_ASSERT(str[pos] == 'a' || str[pos] == 'A');
	// parse the "ago" string at the end of the interval
	if (len - pos < 3) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'g' || str[pos] == 'G')) {
		return false;
	}
	pos++;
	if (!(str[pos] == 'o' || str[pos] == 'O')) {
		return false;
	}
	pos++;
	// parse any trailing whitespace
	for (; pos < len; pos++) {
		char c = str[pos];
		if (c == ' ' || c == '\t' || c == '\n') {
			continue;
		} else {
			return false;
		}
	}
	// invert all the values
	result.months = -result.months;
	result.days = -result.days;
	result.micros = -result.micros;
	goto end_of_string;
end_of_string:
	if (!found_any) {
		// end of string and no identifiers were found: cannot convert empty interval
		return false;
	}
	return true;
posix_interval:
	return false;
}

string Interval::ToString(const interval_t &interval) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(interval, buffer);
	return string(buffer, length);
}

int64_t Interval::GetMilli(const interval_t &val) {
	int64_t milli_month, milli_day, milli;
	if (!TryMultiplyOperator::Operation((int64_t)val.months, Interval::MICROS_PER_MONTH / 1000, milli_month)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	if (!TryMultiplyOperator::Operation((int64_t)val.days, Interval::MICROS_PER_DAY / 1000, milli_day)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	milli = val.micros / 1000;
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(milli, milli_month, milli)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(milli, milli_day, milli)) {
		throw ConversionException("Could not convert Interval to Milliseconds");
	}
	return milli;
}

int64_t Interval::GetMicro(const interval_t &val) {
	int64_t micro_month, micro_day, micro_total;
	micro_total = val.micros;
	if (!TryMultiplyOperator::Operation((int64_t)val.months, MICROS_PER_MONTH, micro_month)) {
		throw ConversionException("Could not convert Month to Microseconds");
	}
	if (!TryMultiplyOperator::Operation((int64_t)val.days, MICROS_PER_DAY, micro_day)) {
		throw ConversionException("Could not convert Day to Microseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(micro_total, micro_month, micro_total)) {
		throw ConversionException("Could not convert Interval to Microseconds");
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(micro_total, micro_day, micro_total)) {
		throw ConversionException("Could not convert Interval to Microseconds");
	}

	return micro_total;
}

int64_t Interval::GetNanoseconds(const interval_t &val) {
	int64_t nano;
	const auto micro_total = GetMicro(val);
	if (!TryMultiplyOperator::Operation(micro_total, NANOS_PER_MICRO, nano)) {
		throw ConversionException("Could not convert Interval to Nanoseconds");
	}

	return nano;
}

interval_t Interval::GetAge(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	D_ASSERT(Timestamp::IsFinite(timestamp_1) && Timestamp::IsFinite(timestamp_2));
	date_t date1, date2;
	dtime_t time1, time2;

	Timestamp::Convert(timestamp_1, date1, time1);
	Timestamp::Convert(timestamp_2, date2, time2);

	// and from date extract the years, months and days
	int32_t year1, month1, day1;
	int32_t year2, month2, day2;
	Date::Convert(date1, year1, month1, day1);
	Date::Convert(date2, year2, month2, day2);
	// finally perform the differences
	auto year_diff = year1 - year2;
	auto month_diff = month1 - month2;
	auto day_diff = day1 - day2;

	// and from time extract hours, minutes, seconds and milliseconds
	int32_t hour1, min1, sec1, micros1;
	int32_t hour2, min2, sec2, micros2;
	Time::Convert(time1, hour1, min1, sec1, micros1);
	Time::Convert(time2, hour2, min2, sec2, micros2);
	// finally perform the differences
	auto hour_diff = hour1 - hour2;
	auto min_diff = min1 - min2;
	auto sec_diff = sec1 - sec2;
	auto micros_diff = micros1 - micros2;

	// flip sign if necessary
	bool sign_flipped = false;
	if (timestamp_1 < timestamp_2) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		micros_diff = -micros_diff;
		sign_flipped = true;
	}
	// now propagate any negative field into the next higher field
	while (micros_diff < 0) {
		micros_diff += MICROS_PER_SEC;
		sec_diff--;
	}
	while (sec_diff < 0) {
		sec_diff += SECS_PER_MINUTE;
		min_diff--;
	}
	while (min_diff < 0) {
		min_diff += MINS_PER_HOUR;
		hour_diff--;
	}
	while (hour_diff < 0) {
		hour_diff += HOURS_PER_DAY;
		day_diff--;
	}
	while (day_diff < 0) {
		if (timestamp_1 < timestamp_2) {
			day_diff += Date::IsLeapYear(year1) ? Date::LEAP_DAYS[month1] : Date::NORMAL_DAYS[month1];
			month_diff--;
		} else {
			day_diff += Date::IsLeapYear(year2) ? Date::LEAP_DAYS[month2] : Date::NORMAL_DAYS[month2];
			month_diff--;
		}
	}
	while (month_diff < 0) {
		month_diff += MONTHS_PER_YEAR;
		year_diff--;
	}

	// recover sign if necessary
	if (sign_flipped) {
		year_diff = -year_diff;
		month_diff = -month_diff;
		day_diff = -day_diff;
		hour_diff = -hour_diff;
		min_diff = -min_diff;
		sec_diff = -sec_diff;
		micros_diff = -micros_diff;
	}
	interval_t interval;
	interval.months = year_diff * MONTHS_PER_YEAR + month_diff;
	interval.days = day_diff;
	interval.micros = Time::FromTime(hour_diff, min_diff, sec_diff, micros_diff).micros;

	return interval;
}

interval_t Interval::GetDifference(timestamp_t timestamp_1, timestamp_t timestamp_2) {
	if (!Timestamp::IsFinite(timestamp_1) || !Timestamp::IsFinite(timestamp_2)) {
		throw InvalidInputException("Cannot subtract infinite timestamps");
	}
	const auto us_1 = Timestamp::GetEpochMicroSeconds(timestamp_1);
	const auto us_2 = Timestamp::GetEpochMicroSeconds(timestamp_2);
	int64_t delta_us;
	if (!TrySubtractOperator::Operation(us_1, us_2, delta_us)) {
		throw ConversionException("Timestamp difference is out of bounds");
	}
	return FromMicro(delta_us);
}

interval_t Interval::FromMicro(int64_t delta_us) {
	interval_t result;
	result.months = 0;
	result.days = delta_us / Interval::MICROS_PER_DAY;
	result.micros = delta_us % Interval::MICROS_PER_DAY;

	return result;
}

interval_t Interval::Invert(interval_t interval) {
	interval.days = -interval.days;
	interval.micros = -interval.micros;
	interval.months = -interval.months;
	return interval;
}

date_t Interval::Add(date_t left, interval_t right) {
	if (!Date::IsFinite(left)) {
		return left;
	}
	date_t result;
	if (right.months != 0) {
		int32_t year, month, day;
		Date::Convert(left, year, month, day);
		int32_t year_diff = right.months / Interval::MONTHS_PER_YEAR;
		year += year_diff;
		month += right.months - year_diff * Interval::MONTHS_PER_YEAR;
		if (month > Interval::MONTHS_PER_YEAR) {
			year++;
			month -= Interval::MONTHS_PER_YEAR;
		} else if (month <= 0) {
			year--;
			month += Interval::MONTHS_PER_YEAR;
		}
		day = MinValue<int32_t>(day, Date::MonthDays(year, month));
		result = Date::FromDate(year, month, day);
	} else {
		result = left;
	}
	if (right.days != 0) {
		if (!TryAddOperator::Operation(result.days, right.days, result.days)) {
			throw OutOfRangeException("Date out of range");
		}
	}
	if (right.micros != 0) {
		if (!TryAddOperator::Operation(result.days, int32_t(right.micros / Interval::MICROS_PER_DAY), result.days)) {
			throw OutOfRangeException("Date out of range");
		}
	}
	if (!Date::IsFinite(result)) {
		throw OutOfRangeException("Date out of range");
	}
	return result;
}

dtime_t Interval::Add(dtime_t left, interval_t right, date_t &date) {
	int64_t diff = right.micros - ((right.micros / Interval::MICROS_PER_DAY) * Interval::MICROS_PER_DAY);
	left += diff;
	if (left.micros >= Interval::MICROS_PER_DAY) {
		left.micros -= Interval::MICROS_PER_DAY;
		date.days++;
	} else if (left.micros < 0) {
		left.micros += Interval::MICROS_PER_DAY;
		date.days--;
	}
	return left;
}

timestamp_t Interval::Add(timestamp_t left, interval_t right) {
	if (!Timestamp::IsFinite(left)) {
		return left;
	}
	date_t date;
	dtime_t time;
	Timestamp::Convert(left, date, time);
	auto new_date = Interval::Add(date, right);
	auto new_time = Interval::Add(time, right, new_date);
	return Timestamp::FromDatetime(new_date, new_time);
}

} // namespace duckdb


namespace duckdb {

// forward declarations
//===--------------------------------------------------------------------===//
// Primitives
//===--------------------------------------------------------------------===//
template <class T>
static idx_t GetAllocationSize(uint16_t capacity) {
	return AlignValue(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(T)));
}

template <class T>
static data_ptr_t AllocatePrimitiveData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetAllocationSize<T>(capacity));
}

template <class T>
static T *GetPrimitiveData(ListSegment *segment) {
	return reinterpret_cast<T *>(data_ptr_cast(segment) + sizeof(ListSegment) + segment->capacity * sizeof(bool));
}

template <class T>
static const T *GetPrimitiveData(const ListSegment *segment) {
	return reinterpret_cast<const T *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                   segment->capacity * sizeof(bool));
}

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
static idx_t GetAllocationSizeList(uint16_t capacity) {
	return AlignValue(sizeof(ListSegment) + capacity * (sizeof(bool) + sizeof(uint64_t)) + sizeof(LinkedList));
}

static data_ptr_t AllocateListData(ArenaAllocator &allocator, uint16_t capacity) {
	return allocator.Allocate(GetAllocationSizeList(capacity));
}

static uint64_t *GetListLengthData(ListSegment *segment) {
	return reinterpret_cast<uint64_t *>(data_ptr_cast(segment) + sizeof(ListSegment) +
	                                    segment->capacity * sizeof(bool));
}

static const uint64_t *GetListLengthData(const ListSegment *segment) {
	return reinterpret_cast<const uint64_t *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                          segment->capacity * sizeof(bool));
}

static const LinkedList *GetListChildData(const ListSegment *segment) {
	return reinterpret_cast<const LinkedList *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                            segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

static LinkedList *GetListChildData(ListSegment *segment) {
	return reinterpret_cast<LinkedList *>(data_ptr_cast(segment) + sizeof(ListSegment) +
	                                      segment->capacity * (sizeof(bool) + sizeof(uint64_t)));
}

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
static idx_t GetAllocationSizeStruct(uint16_t capacity, idx_t child_count) {
	return AlignValue(sizeof(ListSegment) + capacity * sizeof(bool) + child_count * sizeof(ListSegment *));
}

static data_ptr_t AllocateStructData(ArenaAllocator &allocator, uint16_t capacity, idx_t child_count) {
	return allocator.Allocate(GetAllocationSizeStruct(capacity, child_count));
}

static ListSegment **GetStructData(ListSegment *segment) {
	return reinterpret_cast<ListSegment **>(data_ptr_cast(segment) + +sizeof(ListSegment) +
	                                        segment->capacity * sizeof(bool));
}

static const ListSegment *const *GetStructData(const ListSegment *segment) {
	return reinterpret_cast<const ListSegment *const *>(const_data_ptr_cast(segment) + sizeof(ListSegment) +
	                                                    segment->capacity * sizeof(bool));
}

static bool *GetNullMask(ListSegment *segment) {
	return reinterpret_cast<bool *>(data_ptr_cast(segment) + sizeof(ListSegment));
}

static const bool *GetNullMask(const ListSegment *segment) {
	return reinterpret_cast<const bool *>(const_data_ptr_cast(segment) + sizeof(ListSegment));
}

static uint16_t GetCapacityForNewSegment(uint16_t capacity) {
	auto next_power_of_two = idx_t(capacity) * 2;
	if (next_power_of_two >= NumericLimits<uint16_t>::Maximum()) {
		return capacity;
	}
	return uint16_t(next_power_of_two);
}

//===--------------------------------------------------------------------===//
// Create
//===--------------------------------------------------------------------===//
template <class T>
static ListSegment *CreatePrimitiveSegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set the header
	auto segment = (ListSegment *)AllocatePrimitiveData<T>(allocator, capacity);
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;
	return segment;
}

static ListSegment *CreateListSegment(const ListSegmentFunctions &, ArenaAllocator &allocator, uint16_t capacity) {
	// allocate data and set the header
	auto segment = reinterpret_cast<ListSegment *>(AllocateListData(allocator, capacity));
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create an empty linked list for the child vector
	auto linked_child_list = GetListChildData(segment);
	LinkedList linked_list(0, nullptr, nullptr);
	Store<LinkedList>(linked_list, data_ptr_cast(linked_child_list));

	return segment;
}

static ListSegment *CreateStructSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                        uint16_t capacity) {
	// allocate data and set header
	auto segment =
	    reinterpret_cast<ListSegment *>(AllocateStructData(allocator, capacity, functions.child_functions.size()));
	segment->capacity = capacity;
	segment->count = 0;
	segment->next = nullptr;

	// create a child ListSegment with exactly the same capacity for each child vector
	auto child_segments = GetStructData(segment);
	for (idx_t i = 0; i < functions.child_functions.size(); i++) {
		auto child_function = functions.child_functions[i];
		auto child_segment = child_function.create_segment(child_function, allocator, capacity);
		Store<ListSegment *>(child_segment, data_ptr_cast(child_segments + i));
	}

	return segment;
}

static ListSegment *GetSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                               LinkedList &linked_list) {
	ListSegment *segment;

	// determine segment
	if (!linked_list.last_segment) {
		// empty linked list, create the first (and last) segment
		auto capacity = ListSegment::INITIAL_CAPACITY;
		segment = functions.create_segment(functions, allocator, capacity);
		linked_list.first_segment = segment;
		linked_list.last_segment = segment;

	} else if (linked_list.last_segment->capacity == linked_list.last_segment->count) {
		// the last segment of the linked list is full, create a new one and append it
		auto capacity = GetCapacityForNewSegment(linked_list.last_segment->capacity);
		segment = functions.create_segment(functions, allocator, capacity);
		linked_list.last_segment->next = segment;
		linked_list.last_segment = segment;
	} else {
		// the last segment of the linked list is not full, append the data to it
		segment = linked_list.last_segment;
	}

	D_ASSERT(segment);
	return segment;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static void WriteDataToPrimitiveSegment(const ListSegmentFunctions &, ArenaAllocator &, ListSegment *segment,
                                        RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {

	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// write value
	if (valid) {
		auto segment_data = GetPrimitiveData<T>(segment);
		auto input_data_ptr = UnifiedVectorFormat::GetData<T>(input_data.unified);
		Store<T>(input_data_ptr[sel_entry_idx], data_ptr_cast(segment_data + segment->count));
	}
}

static void WriteDataToVarcharSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                      ListSegment *segment, RecursiveUnifiedVectorFormat &input_data,
                                      idx_t &entry_idx) {

	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// set the length of this string
	auto str_length_data = GetListLengthData(segment);
	uint64_t str_length = 0;

	// get the string
	string_t str_entry;
	if (valid) {
		str_entry = UnifiedVectorFormat::GetData<string_t>(input_data.unified)[sel_entry_idx];
		str_length = str_entry.GetSize();
	}

	// we can reconstruct the offset from the length
	Store<uint64_t>(str_length, data_ptr_cast(str_length_data + segment->count));
	if (!valid) {
		return;
	}

	// write the characters to the linked list of child segments
	auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
	for (char &c : str_entry.GetString()) {
		auto child_segment = GetSegment(functions.child_functions.back(), allocator, child_segments);
		auto data = GetPrimitiveData<char>(child_segment);
		data[child_segment->count] = c;
		child_segment->count++;
		child_segments.total_capacity++;
	}

	// store the updated linked list
	Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
}

static void WriteDataToListSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                   ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {

	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// set the length of this list
	auto list_length_data = GetListLengthData(segment);
	uint64_t list_length = 0;

	if (valid) {
		// get list entry information
		const auto &list_entry = UnifiedVectorFormat::GetData<list_entry_t>(input_data.unified)[sel_entry_idx];
		list_length = list_entry.length;

		// loop over the child vector entries and recurse on them
		auto child_segments = Load<LinkedList>(data_ptr_cast(GetListChildData(segment)));
		D_ASSERT(functions.child_functions.size() == 1);
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto source_idx_child = list_entry.offset + child_idx;
			functions.child_functions[0].AppendRow(allocator, child_segments, input_data.children.back(),
			                                       source_idx_child);
		}
		// store the updated linked list
		Store<LinkedList>(child_segments, data_ptr_cast(GetListChildData(segment)));
	}

	Store<uint64_t>(list_length, data_ptr_cast(list_length_data + segment->count));
}

static void WriteDataToStructSegment(const ListSegmentFunctions &functions, ArenaAllocator &allocator,
                                     ListSegment *segment, RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) {

	auto sel_entry_idx = input_data.unified.sel->get_index(entry_idx);

	// write null validity
	auto null_mask = GetNullMask(segment);
	auto valid = input_data.unified.validity.RowIsValid(sel_entry_idx);
	null_mask[segment->count] = !valid;

	// write value
	D_ASSERT(input_data.children.size() == functions.child_functions.size());
	auto child_list = GetStructData(segment);

	// write the data of each of the children of the struct
	for (idx_t i = 0; i < input_data.children.size(); i++) {
		auto child_list_segment = Load<ListSegment *>(data_ptr_cast(child_list + i));
		auto &child_function = functions.child_functions[i];
		child_function.write_data(child_function, allocator, child_list_segment, input_data.children[i], entry_idx);
		child_list_segment->count++;
	}
}

void ListSegmentFunctions::AppendRow(ArenaAllocator &allocator, LinkedList &linked_list,
                                     RecursiveUnifiedVectorFormat &input_data, idx_t &entry_idx) const {

	auto &write_data_to_segment = *this;
	auto segment = GetSegment(write_data_to_segment, allocator, linked_list);
	write_data_to_segment.write_data(write_data_to_segment, allocator, segment, input_data, entry_idx);

	linked_list.total_capacity++;
	segment->count++;
}

//===--------------------------------------------------------------------===//
// Read
//===--------------------------------------------------------------------===//
template <class T>
static void ReadDataFromPrimitiveSegment(const ListSegmentFunctions &, const ListSegment *segment, Vector &result,
                                         idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto aggr_vector_data = FlatVector::GetData<T>(result);

	// load values
	for (idx_t i = 0; i < segment->count; i++) {
		if (aggr_vector_validity.RowIsValid(total_count + i)) {
			auto data = GetPrimitiveData<T>(segment);
			aggr_vector_data[total_count + i] = Load<T>(const_data_ptr_cast(data + i));
		}
	}
}

static void ReadDataFromVarcharSegment(const ListSegmentFunctions &, const ListSegment *segment, Vector &result,
                                       idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	// append all the child chars to one string
	string str = "";
	auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
	while (linked_child_list.first_segment) {
		auto child_segment = linked_child_list.first_segment;
		auto data = GetPrimitiveData<char>(child_segment);
		str.append(data, child_segment->count);
		linked_child_list.first_segment = child_segment->next;
	}
	linked_child_list.last_segment = nullptr;

	// use length and (reconstructed) offset to get the correct substrings
	auto aggr_vector_data = FlatVector::GetData<string_t>(result);
	auto str_length_data = GetListLengthData(segment);

	// get the substrings and write them to the result vector
	idx_t offset = 0;
	for (idx_t i = 0; i < segment->count; i++) {
		if (!null_mask[i]) {
			auto str_length = Load<uint64_t>(const_data_ptr_cast(str_length_data + i));
			auto substr = str.substr(offset, str_length);
			auto str_t = StringVector::AddStringOrBlob(result, substr);
			aggr_vector_data[total_count + i] = str_t;
			offset += str_length;
		}
	}
}

static void ReadDataFromListSegment(const ListSegmentFunctions &functions, const ListSegment *segment, Vector &result,
                                    idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto list_vector_data = FlatVector::GetData<list_entry_t>(result);

	// get the starting offset
	idx_t offset = 0;
	if (total_count != 0) {
		offset = list_vector_data[total_count - 1].offset + list_vector_data[total_count - 1].length;
	}
	idx_t starting_offset = offset;

	// set length and offsets
	auto list_length_data = GetListLengthData(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		auto list_length = Load<uint64_t>(const_data_ptr_cast(list_length_data + i));
		list_vector_data[total_count + i].length = list_length;
		list_vector_data[total_count + i].offset = offset;
		offset += list_length;
	}

	auto &child_vector = ListVector::GetEntry(result);
	auto linked_child_list = Load<LinkedList>(const_data_ptr_cast(GetListChildData(segment)));
	ListVector::Reserve(result, offset);

	// recurse into the linked list of child values
	D_ASSERT(functions.child_functions.size() == 1);
	functions.child_functions[0].BuildListVector(linked_child_list, child_vector, starting_offset);
	ListVector::SetListSize(result, offset);
}

static void ReadDataFromStructSegment(const ListSegmentFunctions &functions, const ListSegment *segment, Vector &result,
                                      idx_t &total_count) {

	auto &aggr_vector_validity = FlatVector::Validity(result);

	// set NULLs
	auto null_mask = GetNullMask(segment);
	for (idx_t i = 0; i < segment->count; i++) {
		if (null_mask[i]) {
			aggr_vector_validity.SetInvalid(total_count + i);
		}
	}

	auto &children = StructVector::GetEntries(result);

	// recurse into the child segments of each child of the struct
	D_ASSERT(children.size() == functions.child_functions.size());
	auto struct_children = GetStructData(segment);
	for (idx_t child_count = 0; child_count < children.size(); child_count++) {
		auto struct_children_segment = Load<ListSegment *>(const_data_ptr_cast(struct_children + child_count));
		auto &child_function = functions.child_functions[child_count];
		child_function.read_data(child_function, struct_children_segment, *children[child_count], total_count);
	}
}

void ListSegmentFunctions::BuildListVector(const LinkedList &linked_list, Vector &result,
                                           idx_t &initial_total_count) const {
	auto &read_data_from_segment = *this;
	idx_t total_count = initial_total_count;
	auto segment = linked_list.first_segment;
	while (segment) {
		read_data_from_segment.read_data(read_data_from_segment, segment, result, total_count);

		total_count += segment->count;
		segment = segment->next;
	}
}

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//
template <class T>
void SegmentPrimitiveFunction(ListSegmentFunctions &functions) {
	functions.create_segment = CreatePrimitiveSegment<T>;
	functions.write_data = WriteDataToPrimitiveSegment<T>;
	functions.read_data = ReadDataFromPrimitiveSegment<T>;
}

void GetSegmentDataFunctions(ListSegmentFunctions &functions, const LogicalType &type) {

	auto physical_type = type.InternalType();
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		SegmentPrimitiveFunction<bool>(functions);
		break;
	case PhysicalType::INT8:
		SegmentPrimitiveFunction<int8_t>(functions);
		break;
	case PhysicalType::INT16:
		SegmentPrimitiveFunction<int16_t>(functions);
		break;
	case PhysicalType::INT32:
		SegmentPrimitiveFunction<int32_t>(functions);
		break;
	case PhysicalType::INT64:
		SegmentPrimitiveFunction<int64_t>(functions);
		break;
	case PhysicalType::UINT8:
		SegmentPrimitiveFunction<uint8_t>(functions);
		break;
	case PhysicalType::UINT16:
		SegmentPrimitiveFunction<uint16_t>(functions);
		break;
	case PhysicalType::UINT32:
		SegmentPrimitiveFunction<uint32_t>(functions);
		break;
	case PhysicalType::UINT64:
		SegmentPrimitiveFunction<uint64_t>(functions);
		break;
	case PhysicalType::FLOAT:
		SegmentPrimitiveFunction<float>(functions);
		break;
	case PhysicalType::DOUBLE:
		SegmentPrimitiveFunction<double>(functions);
		break;
	case PhysicalType::INT128:
		SegmentPrimitiveFunction<hugeint_t>(functions);
		break;
	case PhysicalType::INTERVAL:
		SegmentPrimitiveFunction<interval_t>(functions);
		break;
	case PhysicalType::VARCHAR: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToVarcharSegment;
		functions.read_data = ReadDataFromVarcharSegment;

		functions.child_functions.emplace_back();
		SegmentPrimitiveFunction<char>(functions.child_functions.back());
		break;
	}
	case PhysicalType::LIST: {
		functions.create_segment = CreateListSegment;
		functions.write_data = WriteDataToListSegment;
		functions.read_data = ReadDataFromListSegment;

		// recurse
		functions.child_functions.emplace_back();
		GetSegmentDataFunctions(functions.child_functions.back(), ListType::GetChildType(type));
		break;
	}
	case PhysicalType::STRUCT: {
		functions.create_segment = CreateStructSegment;
		functions.write_data = WriteDataToStructSegment;
		functions.read_data = ReadDataFromStructSegment;

		// recurse
		auto child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			functions.child_functions.emplace_back();
			GetSegmentDataFunctions(functions.child_functions.back(), child_types[i].second);
		}
		break;
	}
	default:
		throw InternalException("LIST aggregate not yet implemented for " + type.ToString());
	}
}

} // namespace duckdb






namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, BufferManager &buffer_manager_p,
                                           const TupleDataLayout &layout_p)
    : type(type_p), buffer_manager(buffer_manager_p), layout(layout_p.Copy()), count(0), data_size(0),
      allocators(make_shared<PartitionTupleDataAllocators>()) {
}

PartitionedTupleData::PartitionedTupleData(const PartitionedTupleData &other)
    : type(other.type), buffer_manager(other.buffer_manager), layout(other.layout.Copy()) {
}

PartitionedTupleData::~PartitionedTupleData() {
}

const TupleDataLayout &PartitionedTupleData::GetLayout() const {
	return layout;
}

PartitionedTupleDataType PartitionedTupleData::GetType() const {
	return type;
}

void PartitionedTupleData::InitializeAppendState(PartitionedTupleDataAppendState &state,
                                                 TupleDataPinProperties properties) const {
	state.partition_sel.Initialize();
	state.reverse_partition_sel.Initialize();

	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}

	InitializeAppendStateInternal(state, properties);
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, DataChunk &input,
                                  const SelectionVector &append_sel, const idx_t append_count) {
	TupleDataCollection::ToUnifiedFormat(state.chunk_state, input);
	AppendUnified(state, input, append_sel, append_count);
}

bool PartitionedTupleData::UseFixedSizeMap() const {
	return MaxPartitionIndex() < PartitionedTupleDataAppendState::MAP_THRESHOLD;
}

void PartitionedTupleData::AppendUnified(PartitionedTupleDataAppendState &state, DataChunk &input,
                                         const SelectionVector &append_sel, const idx_t append_count) {
	const idx_t actual_append_count = append_count == DConstants::INVALID_INDEX ? input.size() : append_count;

	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, append_sel, actual_append_count);

	// Early out: check if everything belongs to a single partition
	optional_idx partition_index;
	if (UseFixedSizeMap()) {
		if (state.fixed_partition_entries.size() == 1) {
			partition_index = state.fixed_partition_entries.begin().GetKey();
		}
	} else {
		if (state.partition_entries.size() == 1) {
			partition_index = state.partition_entries.begin()->first;
		}
	}
	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = *state.partition_pin_states[partition_index.GetIndex()];

		const auto size_before = partition.SizeInBytes();
		partition.AppendUnified(partition_pin_state, state.chunk_state, input, append_sel, actual_append_count);
		data_size += partition.SizeInBytes() - size_before;
	} else {
		// Compute the heap sizes for the whole chunk
		if (!layout.AllConstant()) {
			TupleDataCollection::ComputeHeapSizes(state.chunk_state, input, state.partition_sel, actual_append_count);
		}

		// Build the buffer space
		BuildBufferSpace(state);

		// Now scatter everything in one go
		partitions[0]->Scatter(state.chunk_state, input, state.partition_sel, actual_append_count);
	}

	count += actual_append_count;
	Verify();
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, TupleDataChunkState &input,
                                  const idx_t append_count) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(input.row_locations, append_count, state.partition_indices);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, *FlatVector::IncrementalSelectionVector(), append_count);

	// Early out: check if everything belongs to a single partition
	optional_idx partition_index;
	if (UseFixedSizeMap()) {
		if (state.fixed_partition_entries.size() == 1) {
			partition_index = state.fixed_partition_entries.begin().GetKey();
		}
	} else {
		if (state.partition_entries.size() == 1) {
			partition_index = state.partition_entries.begin()->first;
		}
	}

	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = *state.partition_pin_states[partition_index.GetIndex()];

		state.chunk_state.heap_sizes.Reference(input.heap_sizes);

		const auto size_before = partition.SizeInBytes();
		partition.Build(partition_pin_state, state.chunk_state, 0, append_count);
		data_size += partition.SizeInBytes() - size_before;

		partition.CopyRows(state.chunk_state, input, *FlatVector::IncrementalSelectionVector(), append_count);
	} else {
		// Build the buffer space
		state.chunk_state.heap_sizes.Slice(input.heap_sizes, state.partition_sel, append_count);
		state.chunk_state.heap_sizes.Flatten(append_count);
		BuildBufferSpace(state);

		// Copy the rows
		partitions[0]->CopyRows(state.chunk_state, input, state.partition_sel, append_count);
	}

	count += append_count;
	Verify();
}

// LCOV_EXCL_START
template <class MAP_TYPE>
struct UnorderedMapGetter {
	static inline const typename MAP_TYPE::key_type &GetKey(typename MAP_TYPE::iterator &iterator) {
		return iterator->first;
	}

	static inline const typename MAP_TYPE::key_type &GetKey(const typename MAP_TYPE::const_iterator &iterator) {
		return iterator->first;
	}

	static inline typename MAP_TYPE::mapped_type &GetValue(typename MAP_TYPE::iterator &iterator) {
		return iterator->second;
	}

	static inline const typename MAP_TYPE::mapped_type &GetValue(const typename MAP_TYPE::const_iterator &iterator) {
		return iterator->second;
	}
};

template <class T>
struct FixedSizeMapGetter {
	static inline const idx_t &GetKey(fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetKey();
	}

	static inline const idx_t &GetKey(const const_fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetKey();
	}

	static inline T &GetValue(fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetValue();
	}

	static inline const T &GetValue(const const_fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetValue();
	}
};
// LCOV_EXCL_STOP

void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, const SelectionVector &append_sel,
                                             const idx_t append_count) {
	if (UseFixedSizeMap()) {
		BuildPartitionSel<fixed_size_map_t<list_entry_t>, FixedSizeMapGetter<list_entry_t>>(
		    state, state.fixed_partition_entries, append_sel, append_count);
	} else {
		BuildPartitionSel<perfect_map_t<list_entry_t>, UnorderedMapGetter<perfect_map_t<list_entry_t>>>(
		    state, state.partition_entries, append_sel, append_count);
	}
}

template <class MAP_TYPE, class GETTER>
void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, MAP_TYPE &partition_entries,
                                             const SelectionVector &append_sel, const idx_t append_count) {
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	partition_entries.clear();

	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < append_count; i++) {
			const auto index = append_sel.get_index(i);
			const auto &partition_index = partition_indices[index];
			auto partition_entry = partition_entries.find(partition_index);
			if (partition_entry == partition_entries.end()) {
				partition_entries[partition_index] = list_entry_t(0, 1);
			} else {
				GETTER::GetValue(partition_entry).length++;
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, append_count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedTupleData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		// This needs to be initialized, even if we go the short path here
		for (idx_t i = 0; i < append_count; i++) {
			const auto index = append_sel.get_index(i);
			state.reverse_partition_sel[index] = i;
		}
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		auto &partition_entry = GETTER::GetValue(it);
		partition_entry.offset = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &partition_sel = state.partition_sel;
	auto &reverse_partition_sel = state.reverse_partition_sel;
	for (idx_t i = 0; i < append_count; i++) {
		const auto index = append_sel.get_index(i);
		const auto &partition_index = partition_indices[index];
		auto &partition_offset = partition_entries[partition_index].offset;
		reverse_partition_sel[index] = partition_offset;
		partition_sel[partition_offset++] = index;
	}
}

void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state) {
	if (UseFixedSizeMap()) {
		BuildBufferSpace<fixed_size_map_t<list_entry_t>, FixedSizeMapGetter<list_entry_t>>(
		    state, state.fixed_partition_entries);
	} else {
		BuildBufferSpace<perfect_map_t<list_entry_t>, UnorderedMapGetter<perfect_map_t<list_entry_t>>>(
		    state, state.partition_entries);
	}
}

template <class MAP_TYPE, class GETTER>
void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state, const MAP_TYPE &partition_entries) {
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		const auto &partition_index = GETTER::GetKey(it);

		// Partition, pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = GETTER::GetValue(it);
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Build out the buffer space for this partition
		const auto size_before = partition.SizeInBytes();
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
		data_size += partition.SizeInBytes() - size_before;
	}
}

void PartitionedTupleData::FlushAppendState(PartitionedTupleDataAppendState &state) {
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

void PartitionedTupleData::Combine(PartitionedTupleData &other) {
	if (other.Count() == 0) {
		return;
	}

	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);
	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = std::move(other.partitions);
	} else {
		D_ASSERT(partitions.size() == other.partitions.size());
		// Combine the append state's partitions into this PartitionedTupleData
		for (idx_t i = 0; i < other.partitions.size(); i++) {
			partitions[i]->Combine(*other.partitions[i]);
		}
	}
	this->count += other.count;
	this->data_size += other.data_size;
	Verify();
}

void PartitionedTupleData::Reset() {
	for (auto &partition : partitions) {
		partition->Reset();
	}
	this->count = 0;
	this->data_size = 0;
	Verify();
}

void PartitionedTupleData::Repartition(PartitionedTupleData &new_partitioned_data) {
	D_ASSERT(layout.GetTypes() == new_partitioned_data.layout.GetTypes());

	if (partitions.size() == new_partitioned_data.partitions.size()) {
		new_partitioned_data.Combine(*this);
		return;
	}

	PartitionedTupleDataAppendState append_state;
	new_partitioned_data.InitializeAppendState(append_state);

	const auto reverse = RepartitionReverseOrder();
	const idx_t start_idx = reverse ? partitions.size() : 0;
	const idx_t end_idx = reverse ? 0 : partitions.size();
	const int64_t update = reverse ? -1 : 1;
	const int64_t adjustment = reverse ? -1 : 0;

	for (idx_t partition_idx = start_idx; partition_idx != end_idx; partition_idx += update) {
		auto actual_partition_idx = partition_idx + adjustment;
		auto &partition = *partitions[actual_partition_idx];

		if (partition.Count() > 0) {
			TupleDataChunkIterator iterator(partition, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
			auto &chunk_state = iterator.GetChunkState();
			do {
				new_partitioned_data.Append(append_state, chunk_state, iterator.GetCurrentChunkCount());
			} while (iterator.Next());

			RepartitionFinalizeStates(*this, new_partitioned_data, append_state, actual_partition_idx);
		}
		partitions[actual_partition_idx]->Reset();
	}
	new_partitioned_data.FlushAppendState(append_state);

	count = 0;
	data_size = 0;

	Verify();
}

void PartitionedTupleData::Unpin() {
	for (auto &partition : partitions) {
		partition->Unpin();
	}
}

vector<unique_ptr<TupleDataCollection>> &PartitionedTupleData::GetPartitions() {
	return partitions;
}

unique_ptr<TupleDataCollection> PartitionedTupleData::GetUnpartitioned() {
	auto data_collection = std::move(partitions[0]);
	partitions[0] = make_uniq<TupleDataCollection>(buffer_manager, layout);

	for (idx_t i = 1; i < partitions.size(); i++) {
		data_collection->Combine(*partitions[i]);
	}
	count = 0;
	data_size = 0;

	data_collection->Verify();
	Verify();

	return data_collection;
}

idx_t PartitionedTupleData::Count() const {
	return count;
}

idx_t PartitionedTupleData::SizeInBytes() const {
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		total_size += partition->SizeInBytes();
	}
	return total_size;
}

idx_t PartitionedTupleData::PartitionCount() const {
	return partitions.size();
}

void PartitionedTupleData::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		partition->Verify();
		total_count += partition->Count();
		total_size += partition->SizeInBytes();
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

// LCOV_EXCL_START
string PartitionedTupleData::ToString() {
	string result =
	    StringUtil::Format("PartitionedTupleData - [%llu Partitions, %llu Rows]\n", partitions.size(), Count());
	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		result += StringUtil::Format("Partition %llu: ", partition_idx) + partitions[partition_idx]->ToString();
	}
	return result;
}

void PartitionedTupleData::Print() {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

void PartitionedTupleData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared<TupleDataAllocator>(buffer_manager, layout));
}

} // namespace duckdb


namespace duckdb {

RowDataCollection::RowDataCollection(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size,
                                     bool keep_pinned)
    : buffer_manager(buffer_manager), count(0), block_capacity(block_capacity), entry_size(entry_size),
      keep_pinned(keep_pinned) {
	D_ASSERT(block_capacity * entry_size + entry_size > Storage::BLOCK_SIZE);
}

idx_t RowDataCollection::AppendToBlock(RowDataBlock &block, BufferHandle &handle,
                                       vector<BlockAppendEntry> &append_entries, idx_t remaining, idx_t entry_sizes[]) {
	idx_t append_count = 0;
	data_ptr_t dataptr;
	if (entry_sizes) {
		D_ASSERT(entry_size == 1);
		// compute how many entries fit if entry size is variable
		dataptr = handle.Ptr() + block.byte_offset;
		for (idx_t i = 0; i < remaining; i++) {
			if (block.byte_offset + entry_sizes[i] > block.capacity) {
				if (block.count == 0 && append_count == 0 && entry_sizes[i] > block.capacity) {
					// special case: single entry is bigger than block capacity
					// resize current block to fit the entry, append it, and move to the next block
					block.capacity = entry_sizes[i];
					buffer_manager.ReAllocate(block.block, block.capacity);
					dataptr = handle.Ptr();
					append_count++;
					block.byte_offset += entry_sizes[i];
				}
				break;
			}
			append_count++;
			block.byte_offset += entry_sizes[i];
		}
	} else {
		append_count = MinValue<idx_t>(remaining, block.capacity - block.count);
		dataptr = handle.Ptr() + block.count * entry_size;
	}
	append_entries.emplace_back(dataptr, append_count);
	block.count += append_count;
	return append_count;
}

RowDataBlock &RowDataCollection::CreateBlock() {
	blocks.push_back(make_uniq<RowDataBlock>(buffer_manager, block_capacity, entry_size));
	return *blocks.back();
}

vector<BufferHandle> RowDataCollection::Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[],
                                              const SelectionVector *sel) {
	vector<BufferHandle> handles;
	vector<BlockAppendEntry> append_entries;

	// first allocate space of where to serialize the keys and payload columns
	idx_t remaining = added_count;
	{
		// first append to the last block (if any)
		lock_guard<mutex> append_lock(rdc_lock);
		count += added_count;

		if (!blocks.empty()) {
			auto &last_block = *blocks.back();
			if (last_block.count < last_block.capacity) {
				// last block has space: pin the buffer of this block
				auto handle = buffer_manager.Pin(last_block.block);
				// now append to the block
				idx_t append_count = AppendToBlock(last_block, handle, append_entries, remaining, entry_sizes);
				remaining -= append_count;
				handles.push_back(std::move(handle));
			}
		}
		while (remaining > 0) {
			// now for the remaining data, allocate new buffers to store the data and append there
			auto &new_block = CreateBlock();
			auto handle = buffer_manager.Pin(new_block.block);

			// offset the entry sizes array if we have added entries already
			idx_t *offset_entry_sizes = entry_sizes ? entry_sizes + added_count - remaining : nullptr;

			idx_t append_count = AppendToBlock(new_block, handle, append_entries, remaining, offset_entry_sizes);
			D_ASSERT(new_block.count > 0);
			remaining -= append_count;

			if (keep_pinned) {
				pinned_blocks.push_back(std::move(handle));
			} else {
				handles.push_back(std::move(handle));
			}
		}
	}
	// now set up the key_locations based on the append entries
	idx_t append_idx = 0;
	for (auto &append_entry : append_entries) {
		idx_t next = append_idx + append_entry.count;
		if (entry_sizes) {
			for (; append_idx < next; append_idx++) {
				key_locations[append_idx] = append_entry.baseptr;
				append_entry.baseptr += entry_sizes[append_idx];
			}
		} else {
			for (; append_idx < next; append_idx++) {
				auto idx = sel->get_index(append_idx);
				key_locations[idx] = append_entry.baseptr;
				append_entry.baseptr += entry_size;
			}
		}
	}
	// return the unique pointers to the handles because they must stay pinned
	return handles;
}

void RowDataCollection::Merge(RowDataCollection &other) {
	if (other.count == 0) {
		return;
	}
	RowDataCollection temp(buffer_manager, Storage::BLOCK_SIZE, 1);
	{
		//	One lock at a time to avoid deadlocks
		lock_guard<mutex> read_lock(other.rdc_lock);
		temp.count = other.count;
		temp.block_capacity = other.block_capacity;
		temp.entry_size = other.entry_size;
		temp.blocks = std::move(other.blocks);
		temp.pinned_blocks = std::move(other.pinned_blocks);
	}
	other.Clear();

	lock_guard<mutex> write_lock(rdc_lock);
	count += temp.count;
	block_capacity = MaxValue(block_capacity, temp.block_capacity);
	entry_size = MaxValue(entry_size, temp.entry_size);
	for (auto &block : temp.blocks) {
		blocks.emplace_back(std::move(block));
	}
	for (auto &handle : temp.pinned_blocks) {
		pinned_blocks.emplace_back(std::move(handle));
	}
}

} // namespace duckdb






#include <numeric>

namespace duckdb {

void RowDataCollectionScanner::AlignHeapBlocks(RowDataCollection &swizzled_block_collection,
                                               RowDataCollection &swizzled_string_heap,
                                               RowDataCollection &block_collection, RowDataCollection &string_heap,
                                               const RowLayout &layout) {
	if (block_collection.count == 0) {
		return;
	}

	if (layout.AllConstant()) {
		// No heap blocks! Just merge fixed-size data
		swizzled_block_collection.Merge(block_collection);
		return;
	}

	// We create one heap block per data block and swizzle the pointers
	D_ASSERT(string_heap.keep_pinned == swizzled_string_heap.keep_pinned);
	auto &buffer_manager = block_collection.buffer_manager;
	auto &heap_blocks = string_heap.blocks;
	idx_t heap_block_idx = 0;
	idx_t heap_block_remaining = heap_blocks[heap_block_idx]->count;
	for (auto &data_block : block_collection.blocks) {
		if (heap_block_remaining == 0) {
			heap_block_remaining = heap_blocks[++heap_block_idx]->count;
		}

		// Pin the data block and swizzle the pointers within the rows
		auto data_handle = buffer_manager.Pin(data_block->block);
		auto data_ptr = data_handle.Ptr();
		if (!string_heap.keep_pinned) {
			D_ASSERT(!data_block->block->IsSwizzled());
			RowOperations::SwizzleColumns(layout, data_ptr, data_block->count);
			data_block->block->SetSwizzling(nullptr);
		}
		// At this point the data block is pinned and the heap pointer is valid
		// so we can copy heap data as needed

		// We want to copy as little of the heap data as possible, check how the data and heap blocks line up
		if (heap_block_remaining >= data_block->count) {
			// Easy: current heap block contains all strings for this data block, just copy (reference) the block
			swizzled_string_heap.blocks.emplace_back(heap_blocks[heap_block_idx]->Copy());
			swizzled_string_heap.blocks.back()->count = data_block->count;

			// Swizzle the heap pointer if we are not pinning the heap
			auto &heap_block = swizzled_string_heap.blocks.back()->block;
			auto heap_handle = buffer_manager.Pin(heap_block);
			if (!swizzled_string_heap.keep_pinned) {
				auto heap_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
				auto heap_offset = heap_ptr - heap_handle.Ptr();
				RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_ptr, data_block->count, heap_offset);
			} else {
				swizzled_string_heap.pinned_blocks.emplace_back(std::move(heap_handle));
			}

			// Update counter
			heap_block_remaining -= data_block->count;
		} else {
			// Strings for this data block are spread over the current heap block and the next (and possibly more)
			if (string_heap.keep_pinned) {
				// The heap is changing underneath the data block,
				// so swizzle the string pointers to make them portable.
				RowOperations::SwizzleColumns(layout, data_ptr, data_block->count);
			}
			idx_t data_block_remaining = data_block->count;
			vector<std::pair<data_ptr_t, idx_t>> ptrs_and_sizes;
			idx_t total_size = 0;
			const auto base_row_ptr = data_ptr;
			while (data_block_remaining > 0) {
				if (heap_block_remaining == 0) {
					heap_block_remaining = heap_blocks[++heap_block_idx]->count;
				}
				auto next = MinValue<idx_t>(data_block_remaining, heap_block_remaining);

				// Figure out where to start copying strings, and how many bytes we need to copy
				auto heap_start_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
				auto heap_end_ptr =
				    Load<data_ptr_t>(data_ptr + layout.GetHeapOffset() + (next - 1) * layout.GetRowWidth());
				idx_t size = heap_end_ptr - heap_start_ptr + Load<uint32_t>(heap_end_ptr);
				ptrs_and_sizes.emplace_back(heap_start_ptr, size);
				D_ASSERT(size <= heap_blocks[heap_block_idx]->byte_offset);

				// Swizzle the heap pointer
				RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_start_ptr, next, total_size);
				total_size += size;

				// Update where we are in the data and heap blocks
				data_ptr += next * layout.GetRowWidth();
				data_block_remaining -= next;
				heap_block_remaining -= next;
			}

			// Finally, we allocate a new heap block and copy data to it
			swizzled_string_heap.blocks.emplace_back(
			    make_uniq<RowDataBlock>(buffer_manager, MaxValue<idx_t>(total_size, (idx_t)Storage::BLOCK_SIZE), 1));
			auto new_heap_handle = buffer_manager.Pin(swizzled_string_heap.blocks.back()->block);
			auto new_heap_ptr = new_heap_handle.Ptr();
			for (auto &ptr_and_size : ptrs_and_sizes) {
				memcpy(new_heap_ptr, ptr_and_size.first, ptr_and_size.second);
				new_heap_ptr += ptr_and_size.second;
			}
			new_heap_ptr = new_heap_handle.Ptr();
			if (swizzled_string_heap.keep_pinned) {
				// Since the heap blocks are pinned, we can unswizzle the data again.
				swizzled_string_heap.pinned_blocks.emplace_back(std::move(new_heap_handle));
				RowOperations::UnswizzlePointers(layout, base_row_ptr, new_heap_ptr, data_block->count);
				RowOperations::UnswizzleHeapPointer(layout, base_row_ptr, new_heap_ptr, data_block->count);
			}
		}
	}

	// We're done with variable-sized data, now just merge the fixed-size data
	swizzled_block_collection.Merge(block_collection);
	D_ASSERT(swizzled_block_collection.blocks.size() == swizzled_string_heap.blocks.size());

	// Update counts and cleanup
	swizzled_string_heap.count = string_heap.count;
	string_heap.Clear();
}

void RowDataCollectionScanner::ScanState::PinData() {
	auto &rows = scanner.rows;
	D_ASSERT(block_idx < rows.blocks.size());
	auto &data_block = rows.blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockHandle() != data_block->block) {
		data_handle = rows.buffer_manager.Pin(data_block->block);
	}
	if (scanner.layout.AllConstant() || !scanner.external) {
		return;
	}

	auto &heap = scanner.heap;
	D_ASSERT(block_idx < heap.blocks.size());
	auto &heap_block = heap.blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockHandle() != heap_block->block) {
		heap_handle = heap.buffer_manager.Pin(heap_block->block);
	}
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool external_p, bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      external(external_p), flush(flush_p), unswizzling(!layout.AllConstant() && external && !heap.keep_pinned) {

	if (unswizzling) {
		D_ASSERT(rows.blocks.size() == heap.blocks.size());
	}

	ValidateUnscannedBlock();
}

RowDataCollectionScanner::RowDataCollectionScanner(RowDataCollection &rows_p, RowDataCollection &heap_p,
                                                   const RowLayout &layout_p, bool external_p, idx_t block_idx,
                                                   bool flush_p)
    : rows(rows_p), heap(heap_p), layout(layout_p), read_state(*this), total_count(rows.count), total_scanned(0),
      external(external_p), flush(flush_p), unswizzling(!layout.AllConstant() && external && !heap.keep_pinned) {

	if (unswizzling) {
		D_ASSERT(rows.blocks.size() == heap.blocks.size());
	}

	D_ASSERT(block_idx < rows.blocks.size());
	read_state.block_idx = block_idx;
	read_state.entry_idx = 0;

	//	Pretend that we have scanned up to the start block
	//	and will stop at the end
	auto begin = rows.blocks.begin();
	auto end = begin + block_idx;
	total_scanned =
	    std::accumulate(begin, end, idx_t(0), [&](idx_t c, const unique_ptr<RowDataBlock> &b) { return c + b->count; });
	total_count = total_scanned + (*end)->count;

	ValidateUnscannedBlock();
}

void RowDataCollectionScanner::SwizzleBlock(RowDataBlock &data_block, RowDataBlock &heap_block) {
	// Pin the data block and swizzle the pointers within the rows
	D_ASSERT(!data_block.block->IsSwizzled());
	auto data_handle = rows.buffer_manager.Pin(data_block.block);
	auto data_ptr = data_handle.Ptr();
	RowOperations::SwizzleColumns(layout, data_ptr, data_block.count);
	data_block.block->SetSwizzling(nullptr);

	// Swizzle the heap pointers
	auto heap_handle = heap.buffer_manager.Pin(heap_block.block);
	auto heap_ptr = Load<data_ptr_t>(data_ptr + layout.GetHeapOffset());
	auto heap_offset = heap_ptr - heap_handle.Ptr();
	RowOperations::SwizzleHeapPointer(layout, data_ptr, heap_ptr, data_block.count, heap_offset);
}

void RowDataCollectionScanner::ReSwizzle() {
	if (rows.count == 0) {
		return;
	}

	if (!unswizzling) {
		// No swizzled blocks!
		return;
	}

	D_ASSERT(rows.blocks.size() == heap.blocks.size());
	for (idx_t i = 0; i < rows.blocks.size(); ++i) {
		auto &data_block = rows.blocks[i];
		if (data_block->block && !data_block->block->IsSwizzled()) {
			SwizzleBlock(*data_block, *heap.blocks[i]);
		}
	}
}

void RowDataCollectionScanner::ValidateUnscannedBlock() const {
	if (unswizzling && read_state.block_idx < rows.blocks.size() && Remaining()) {
		D_ASSERT(rows.blocks[read_state.block_idx]->block->IsSwizzled());
	}
}

void RowDataCollectionScanner::Scan(DataChunk &chunk) {
	auto count = MinValue((idx_t)STANDARD_VECTOR_SIZE, total_count - total_scanned);
	if (count == 0) {
		chunk.SetCardinality(count);
		return;
	}

	//	Only flush blocks we processed.
	const auto flush_block_idx = read_state.block_idx;

	const idx_t &row_width = layout.GetRowWidth();
	// Set up a batch of pointers to scan data from
	idx_t scanned = 0;
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	// We must pin ALL blocks we are going to gather from
	vector<BufferHandle> pinned_blocks;
	while (scanned < count) {
		read_state.PinData();
		auto &data_block = rows.blocks[read_state.block_idx];
		idx_t next = MinValue(data_block->count - read_state.entry_idx, count - scanned);
		const data_ptr_t data_ptr = read_state.data_handle.Ptr() + read_state.entry_idx * row_width;
		// Set up the next pointers
		data_ptr_t row_ptr = data_ptr;
		for (idx_t i = 0; i < next; i++) {
			data_pointers[scanned + i] = row_ptr;
			row_ptr += row_width;
		}
		// Unswizzle the offsets back to pointers (if needed)
		if (unswizzling) {
			RowOperations::UnswizzlePointers(layout, data_ptr, read_state.heap_handle.Ptr(), next);
			rows.blocks[read_state.block_idx]->block->SetSwizzling("RowDataCollectionScanner::Scan");
		}
		// Update state indices
		read_state.entry_idx += next;
		scanned += next;
		total_scanned += next;
		if (read_state.entry_idx == data_block->count) {
			// Pin completed blocks so we don't lose them
			pinned_blocks.emplace_back(rows.buffer_manager.Pin(data_block->block));
			if (unswizzling) {
				auto &heap_block = heap.blocks[read_state.block_idx];
				pinned_blocks.emplace_back(heap.buffer_manager.Pin(heap_block->block));
			}
			read_state.block_idx++;
			read_state.entry_idx = 0;
			ValidateUnscannedBlock();
		}
	}
	D_ASSERT(scanned == count);
	// Deserialize the payload data
	for (idx_t col_no = 0; col_no < layout.ColumnCount(); col_no++) {
		RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), chunk.data[col_no],
		                      *FlatVector::IncrementalSelectionVector(), count, layout, col_no);
	}
	chunk.SetCardinality(count);
	chunk.Verify();

	//	Switch to a new set of pinned blocks
	read_state.pinned_blocks.swap(pinned_blocks);

	if (flush) {
		// Release blocks we have passed.
		for (idx_t i = flush_block_idx; i < read_state.block_idx; ++i) {
			rows.blocks[i]->block = nullptr;
			if (unswizzling) {
				heap.blocks[i]->block = nullptr;
			}
		}
	} else if (unswizzling) {
		// Reswizzle blocks we have passed so they can be flushed safely.
		for (idx_t i = flush_block_idx; i < read_state.block_idx; ++i) {
			auto &data_block = rows.blocks[i];
			if (data_block->block && !data_block->block->IsSwizzled()) {
				SwizzleBlock(*data_block, *heap.blocks[i]);
			}
		}
	}
}

void RowDataCollectionScanner::Reset(bool flush_p) {
	flush = flush_p;
	total_scanned = 0;

	read_state.block_idx = 0;
	read_state.entry_idx = 0;
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_layout.cpp
//
//
//===----------------------------------------------------------------------===//





namespace duckdb {

RowLayout::RowLayout() : flag_width(0), data_width(0), row_width(0), all_constant(true), heap_pointer_offset(0) {
}

void RowLayout::Initialize(vector<LogicalType> types_p, bool align) {
	offsets.clear();
	types = std::move(types_p);

	// Null mask at the front - 1 bit per value.
	flag_width = ValidityBytes::ValidityMaskSize(types.size());
	row_width = flag_width;

	// Whether all columns are constant size.
	for (const auto &type : types) {
		all_constant = all_constant && TypeIsConstantSize(type.InternalType());
	}

	// This enables pointer swizzling for out-of-core computation.
	if (!all_constant) {
		// When unswizzled, the pointer lives here.
		// When swizzled, the pointer is replaced by an offset.
		heap_pointer_offset = row_width;
		// The 8 byte pointer will be replaced with an 8 byte idx_t when swizzled.
		// However, this cannot be sizeof(data_ptr_t), since 32 bit builds use 4 byte pointers.
		row_width += sizeof(idx_t);
	}

	// Data columns. No alignment required.
	for (const auto &type : types) {
		offsets.push_back(row_width);
		const auto internal_type = type.InternalType();
		if (TypeIsConstantSize(internal_type) || internal_type == PhysicalType::VARCHAR) {
			row_width += GetTypeIdSize(type.InternalType());
		} else {
			// Variable size types use pointers to the actual data (can be swizzled).
			// Again, we would use sizeof(data_ptr_t), but this is not guaranteed to be equal to sizeof(idx_t).
			row_width += sizeof(idx_t);
		}
	}

	data_width = row_width - flag_width;

	// Alignment padding for the next row
	if (align) {
		row_width = AlignValue(row_width);
	}
}

} // namespace duckdb






namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

TupleDataBlock::TupleDataBlock(BufferManager &buffer_manager, idx_t capacity_p) : capacity(capacity_p), size(0) {
	buffer_manager.Allocate(capacity, false, &handle);
}

TupleDataBlock::TupleDataBlock(TupleDataBlock &&other) noexcept {
	std::swap(handle, other.handle);
	std::swap(capacity, other.capacity);
	std::swap(size, other.size);
}

TupleDataBlock &TupleDataBlock::operator=(TupleDataBlock &&other) noexcept {
	std::swap(handle, other.handle);
	std::swap(capacity, other.capacity);
	std::swap(size, other.size);
	return *this;
}

TupleDataAllocator::TupleDataAllocator(BufferManager &buffer_manager, const TupleDataLayout &layout)
    : buffer_manager(buffer_manager), layout(layout.Copy()) {
}

TupleDataAllocator::TupleDataAllocator(TupleDataAllocator &allocator)
    : buffer_manager(allocator.buffer_manager), layout(allocator.layout.Copy()) {
}

BufferManager &TupleDataAllocator::GetBufferManager() {
	return buffer_manager;
}

Allocator &TupleDataAllocator::GetAllocator() {
	return buffer_manager.GetBufferAllocator();
}

const TupleDataLayout &TupleDataAllocator::GetLayout() const {
	return layout;
}

idx_t TupleDataAllocator::RowBlockCount() const {
	return row_blocks.size();
}

idx_t TupleDataAllocator::HeapBlockCount() const {
	return heap_blocks.size();
}

void TupleDataAllocator::Build(TupleDataSegment &segment, TupleDataPinState &pin_state,
                               TupleDataChunkState &chunk_state, const idx_t append_offset, const idx_t append_count) {
	D_ASSERT(this == segment.allocator.get());
	auto &chunks = segment.chunks;
	if (!chunks.empty()) {
		ReleaseOrStoreHandles(pin_state, segment, chunks.back(), true);
	}

	// Build the chunk parts for the incoming data
	chunk_part_indices.clear();
	idx_t offset = 0;
	while (offset != append_count) {
		if (chunks.empty() || chunks.back().count == STANDARD_VECTOR_SIZE) {
			chunks.emplace_back();
		}
		auto &chunk = chunks.back();

		// Build the next part
		auto next = MinValue<idx_t>(append_count - offset, STANDARD_VECTOR_SIZE - chunk.count);
		chunk.AddPart(BuildChunkPart(pin_state, chunk_state, append_offset + offset, next, chunk), layout);
		auto &chunk_part = chunk.parts.back();
		next = chunk_part.count;

		segment.count += next;
		segment.data_size += chunk_part.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			segment.data_size += chunk_part.total_heap_size;
		}

		offset += next;
		chunk_part_indices.emplace_back(chunks.size() - 1, chunk.parts.size() - 1);
	}

	// Now initialize the pointers to write the data to
	chunk_parts.clear();
	for (auto &indices : chunk_part_indices) {
		chunk_parts.emplace_back(segment.chunks[indices.first].parts[indices.second]);
	}
	InitializeChunkStateInternal(pin_state, chunk_state, append_offset, false, true, false, chunk_parts);

	// To reduce metadata, we try to merge chunk parts where possible
	// Due to the way chunk parts are constructed, only the last part of the first chunk is eligible for merging
	segment.chunks[chunk_part_indices[0].first].MergeLastChunkPart(layout);

	segment.Verify();
}

TupleDataChunkPart TupleDataAllocator::BuildChunkPart(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      const idx_t append_offset, const idx_t append_count,
                                                      TupleDataChunk &chunk) {
	D_ASSERT(append_count != 0);
	TupleDataChunkPart result(*chunk.lock);

	// Allocate row block (if needed)
	if (row_blocks.empty() || row_blocks.back().RemainingCapacity() < layout.GetRowWidth()) {
		row_blocks.emplace_back(buffer_manager, (idx_t)Storage::BLOCK_SIZE);
	}
	result.row_block_index = row_blocks.size() - 1;
	auto &row_block = row_blocks[result.row_block_index];
	result.row_block_offset = row_block.size;

	// Set count (might be reduced later when checking heap space)
	result.count = MinValue<idx_t>(row_block.RemainingCapacity(layout.GetRowWidth()), append_count);
	if (!layout.AllConstant()) {
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);

		// Compute total heap size first
		idx_t total_heap_size = 0;
		for (idx_t i = 0; i < result.count; i++) {
			const auto &heap_size = heap_sizes[append_offset + i];
			total_heap_size += heap_size;
		}

		if (total_heap_size == 0) {
			// We don't need a heap at all
			result.heap_block_index = TupleDataChunkPart::INVALID_INDEX;
			result.heap_block_offset = TupleDataChunkPart::INVALID_INDEX;
			result.total_heap_size = 0;
			result.base_heap_ptr = nullptr;
		} else {
			// Allocate heap block (if needed)
			if (heap_blocks.empty() || heap_blocks.back().RemainingCapacity() < heap_sizes[append_offset]) {
				const auto size = MaxValue<idx_t>((idx_t)Storage::BLOCK_SIZE, heap_sizes[append_offset]);
				heap_blocks.emplace_back(buffer_manager, size);
			}
			result.heap_block_index = heap_blocks.size() - 1;
			auto &heap_block = heap_blocks[result.heap_block_index];
			result.heap_block_offset = heap_block.size;

			const auto heap_remaining = heap_block.RemainingCapacity();
			if (total_heap_size <= heap_remaining) {
				// Everything fits
				result.total_heap_size = total_heap_size;
			} else {
				// Not everything fits - determine how many we can read next
				result.total_heap_size = 0;
				for (idx_t i = 0; i < result.count; i++) {
					const auto &heap_size = heap_sizes[append_offset + i];
					if (result.total_heap_size + heap_size > heap_remaining) {
						result.count = i;
						break;
					}
					result.total_heap_size += heap_size;
				}
			}

			// Mark this portion of the heap block as filled and set the pointer
			heap_block.size += result.total_heap_size;
			result.base_heap_ptr = GetBaseHeapPointer(pin_state, result);
		}
	}
	D_ASSERT(result.count != 0 && result.count <= STANDARD_VECTOR_SIZE);

	// Mark this portion of the row block as filled
	row_block.size += result.count * layout.GetRowWidth();

	return result;
}

void TupleDataAllocator::InitializeChunkState(TupleDataSegment &segment, TupleDataPinState &pin_state,
                                              TupleDataChunkState &chunk_state, idx_t chunk_idx, bool init_heap) {
	D_ASSERT(this == segment.allocator.get());
	D_ASSERT(chunk_idx < segment.ChunkCount());
	auto &chunk = segment.chunks[chunk_idx];

	// Release or store any handles that are no longer required:
	// We can't release the heap here if the current chunk's heap_block_ids is empty, because if we are iterating with
	// PinProperties::DESTROY_AFTER_DONE, we might destroy a heap block that is needed by a later chunk, e.g.,
	// when chunk 0 needs heap block 0, chunk 1 does not need any heap blocks, and chunk 2 needs heap block 0 again
	ReleaseOrStoreHandles(pin_state, segment, chunk, !chunk.heap_block_ids.empty());

	unsafe_vector<reference<TupleDataChunkPart>> parts;
	parts.reserve(chunk.parts.size());
	for (auto &part : chunk.parts) {
		parts.emplace_back(part);
	}

	InitializeChunkStateInternal(pin_state, chunk_state, 0, true, init_heap, init_heap, parts);
}

static inline void InitializeHeapSizes(const data_ptr_t row_locations[], idx_t heap_sizes[], const idx_t offset,
                                       const idx_t next, const TupleDataChunkPart &part, const idx_t heap_size_offset) {
	// Read the heap sizes from the rows
	for (idx_t i = 0; i < next; i++) {
		auto idx = offset + i;
		heap_sizes[idx] = Load<uint32_t>(row_locations[idx] + heap_size_offset);
	}

	// Verify total size
#ifdef DEBUG
	idx_t total_heap_size = 0;
	for (idx_t i = 0; i < next; i++) {
		auto idx = offset + i;
		total_heap_size += heap_sizes[idx];
	}
	D_ASSERT(total_heap_size == part.total_heap_size);
#endif
}

void TupleDataAllocator::InitializeChunkStateInternal(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                                      idx_t offset, bool recompute, bool init_heap_pointers,
                                                      bool init_heap_sizes,
                                                      unsafe_vector<reference<TupleDataChunkPart>> &parts) {
	auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);
	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	auto heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);

	for (auto &part_ref : parts) {
		auto &part = part_ref.get();
		const auto next = part.count;

		// Set up row locations for the scan
		const auto row_width = layout.GetRowWidth();
		const auto base_row_ptr = GetRowPointer(pin_state, part);
		for (idx_t i = 0; i < next; i++) {
			row_locations[offset + i] = base_row_ptr + i * row_width;
		}

		if (layout.AllConstant()) { // Can't have a heap
			offset += next;
			continue;
		}

		if (part.total_heap_size == 0) {
			if (init_heap_sizes) { // No heap, but we need the heap sizes
				InitializeHeapSizes(row_locations, heap_sizes, offset, next, part, layout.GetHeapSizeOffset());
			}
			offset += next;
			continue;
		}

		// Check if heap block has changed - re-compute the pointers within each row if so
		if (recompute && pin_state.properties != TupleDataPinProperties::ALREADY_PINNED) {
			const auto new_base_heap_ptr = GetBaseHeapPointer(pin_state, part);
			if (part.base_heap_ptr != new_base_heap_ptr) {
				lock_guard<mutex> guard(part.lock);
				const auto old_base_heap_ptr = part.base_heap_ptr;
				if (old_base_heap_ptr != new_base_heap_ptr) {
					Vector old_heap_ptrs(
					    Value::POINTER(CastPointerToValue(old_base_heap_ptr + part.heap_block_offset)));
					Vector new_heap_ptrs(
					    Value::POINTER(CastPointerToValue(new_base_heap_ptr + part.heap_block_offset)));
					RecomputeHeapPointers(old_heap_ptrs, *ConstantVector::ZeroSelectionVector(), row_locations,
					                      new_heap_ptrs, offset, next, layout, 0);
					part.base_heap_ptr = new_base_heap_ptr;
				}
			}
		}

		if (init_heap_sizes) {
			InitializeHeapSizes(row_locations, heap_sizes, offset, next, part, layout.GetHeapSizeOffset());
		}

		if (init_heap_pointers) {
			// Set the pointers where the heap data will be written (if needed)
			heap_locations[offset] = part.base_heap_ptr + part.heap_block_offset;
			for (idx_t i = 1; i < next; i++) {
				auto idx = offset + i;
				heap_locations[idx] = heap_locations[idx - 1] + heap_sizes[idx - 1];
			}
		}

		offset += next;
	}
	D_ASSERT(offset <= STANDARD_VECTOR_SIZE);
}

static inline void VerifyStrings(const LogicalTypeId type_id, const data_ptr_t row_locations[], const idx_t col_idx,
                                 const idx_t base_col_offset, const idx_t col_offset, const idx_t offset,
                                 const idx_t count) {
#ifdef DEBUG
	if (type_id != LogicalTypeId::VARCHAR) {
		// Make sure we don't verify BLOB / AGGREGATE_STATE
		return;
	}
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	for (idx_t i = 0; i < count; i++) {
		const auto &row_location = row_locations[offset + i] + base_col_offset;
		ValidityBytes row_mask(row_location);
		if (row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			auto recomputed_string = Load<string_t>(row_location + col_offset);
			recomputed_string.Verify();
		}
	}
#endif
}

void TupleDataAllocator::RecomputeHeapPointers(Vector &old_heap_ptrs, const SelectionVector &old_heap_sel,
                                               const data_ptr_t row_locations[], Vector &new_heap_ptrs,
                                               const idx_t offset, const idx_t count, const TupleDataLayout &layout,
                                               const idx_t base_col_offset) {
	const auto old_heap_locations = FlatVector::GetData<data_ptr_t>(old_heap_ptrs);

	UnifiedVectorFormat new_heap_data;
	new_heap_ptrs.ToUnifiedFormat(offset + count, new_heap_data);
	const auto new_heap_locations = UnifiedVectorFormat::GetData<data_ptr_t>(new_heap_data);
	const auto new_heap_sel = *new_heap_data.sel;

	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		const auto &col_offset = layout.GetOffsets()[col_idx];

		// Precompute mask indexes
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

		const auto &type = layout.GetTypes()[col_idx];
		switch (type.InternalType()) {
		case PhysicalType::VARCHAR: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &row_location = row_locations[idx] + base_col_offset;
				ValidityBytes row_mask(row_location);
				if (!row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
					continue;
				}

				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto string_location = row_location + col_offset;
				if (Load<uint32_t>(string_location) > string_t::INLINE_LENGTH) {
					const auto string_ptr_location = string_location + string_t::HEADER_SIZE;
					const auto string_ptr = Load<data_ptr_t>(string_ptr_location);
					const auto diff = string_ptr - old_heap_ptr;
					D_ASSERT(diff >= 0);
					Store<data_ptr_t>(new_heap_ptr + diff, string_ptr_location);
				}
			}
			VerifyStrings(type.id(), row_locations, col_idx, base_col_offset, col_offset, offset, count);
			break;
		}
		case PhysicalType::LIST: {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = offset + i;
				const auto &row_location = row_locations[idx] + base_col_offset;
				ValidityBytes row_mask(row_location);
				if (!row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
					continue;
				}

				const auto &old_heap_ptr = old_heap_locations[old_heap_sel.get_index(idx)];
				const auto &new_heap_ptr = new_heap_locations[new_heap_sel.get_index(idx)];

				const auto &list_ptr_location = row_location + col_offset;
				const auto list_ptr = Load<data_ptr_t>(list_ptr_location);
				const auto diff = list_ptr - old_heap_ptr;
				D_ASSERT(diff >= 0);
				Store<data_ptr_t>(new_heap_ptr + diff, list_ptr_location);
			}
			break;
		}
		case PhysicalType::STRUCT: {
			const auto &struct_layout = layout.GetStructLayout(col_idx);
			if (!struct_layout.AllConstant()) {
				RecomputeHeapPointers(old_heap_ptrs, old_heap_sel, row_locations, new_heap_ptrs, offset, count,
				                      struct_layout, base_col_offset + col_offset);
			}
			break;
		}
		default:
			continue;
		}
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment,
                                               TupleDataChunk &chunk, bool release_heap) {
	D_ASSERT(this == segment.allocator.get());
	ReleaseOrStoreHandlesInternal(segment, segment.pinned_row_handles, pin_state.row_handles, chunk.row_block_ids,
	                              row_blocks, pin_state.properties);
	if (!layout.AllConstant() && release_heap) {
		ReleaseOrStoreHandlesInternal(segment, segment.pinned_heap_handles, pin_state.heap_handles,
		                              chunk.heap_block_ids, heap_blocks, pin_state.properties);
	}
}

void TupleDataAllocator::ReleaseOrStoreHandles(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	static TupleDataChunk DUMMY_CHUNK;
	ReleaseOrStoreHandles(pin_state, segment, DUMMY_CHUNK, true);
}

void TupleDataAllocator::ReleaseOrStoreHandlesInternal(
    TupleDataSegment &segment, unsafe_vector<BufferHandle> &pinned_handles, perfect_map_t<BufferHandle> &handles,
    const perfect_set_t &block_ids, unsafe_vector<TupleDataBlock> &blocks, TupleDataPinProperties properties) {
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = handles.begin(); it != handles.end(); it++) {
			const auto block_id = it->first;
			if (block_ids.find(block_id) != block_ids.end()) {
				// still required: do not release
				continue;
			}
			switch (properties) {
			case TupleDataPinProperties::KEEP_EVERYTHING_PINNED: {
				lock_guard<mutex> guard(segment.pinned_handles_lock);
				const auto block_count = block_id + 1;
				if (block_count > pinned_handles.size()) {
					pinned_handles.resize(block_count);
				}
				pinned_handles[block_id] = std::move(it->second);
				break;
			}
			case TupleDataPinProperties::UNPIN_AFTER_DONE:
			case TupleDataPinProperties::ALREADY_PINNED:
				break;
			case TupleDataPinProperties::DESTROY_AFTER_DONE:
				blocks[block_id].handle = nullptr;
				break;
			default:
				D_ASSERT(properties == TupleDataPinProperties::INVALID);
				throw InternalException("Encountered TupleDataPinProperties::INVALID");
			}
			handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);
}

BufferHandle &TupleDataAllocator::PinRowBlock(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	const auto &row_block_index = part.row_block_index;
	auto it = pin_state.row_handles.find(row_block_index);
	if (it == pin_state.row_handles.end()) {
		D_ASSERT(row_block_index < row_blocks.size());
		auto &row_block = row_blocks[row_block_index];
		D_ASSERT(row_block.handle);
		D_ASSERT(part.row_block_offset < row_block.size);
		D_ASSERT(part.row_block_offset + part.count * layout.GetRowWidth() <= row_block.size);
		it = pin_state.row_handles.emplace(row_block_index, buffer_manager.Pin(row_block.handle)).first;
	}
	return it->second;
}

BufferHandle &TupleDataAllocator::PinHeapBlock(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	const auto &heap_block_index = part.heap_block_index;
	auto it = pin_state.heap_handles.find(heap_block_index);
	if (it == pin_state.heap_handles.end()) {
		D_ASSERT(heap_block_index < heap_blocks.size());
		auto &heap_block = heap_blocks[heap_block_index];
		D_ASSERT(heap_block.handle);
		D_ASSERT(part.heap_block_offset < heap_block.size);
		D_ASSERT(part.heap_block_offset + part.total_heap_size <= heap_block.size);
		it = pin_state.heap_handles.emplace(heap_block_index, buffer_manager.Pin(heap_block.handle)).first;
	}
	return it->second;
}

data_ptr_t TupleDataAllocator::GetRowPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	return PinRowBlock(pin_state, part).Ptr() + part.row_block_offset;
}

data_ptr_t TupleDataAllocator::GetBaseHeapPointer(TupleDataPinState &pin_state, const TupleDataChunkPart &part) {
	return PinHeapBlock(pin_state, part).Ptr();
}

} // namespace duckdb







#include <algorithm>

namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

TupleDataCollection::TupleDataCollection(BufferManager &buffer_manager, const TupleDataLayout &layout_p)
    : layout(layout_p.Copy()), allocator(make_shared<TupleDataAllocator>(buffer_manager, layout)) {
	Initialize();
}

TupleDataCollection::TupleDataCollection(shared_ptr<TupleDataAllocator> allocator)
    : layout(allocator->GetLayout().Copy()), allocator(std::move(allocator)) {
	Initialize();
}

TupleDataCollection::~TupleDataCollection() {
}

void TupleDataCollection::Initialize() {
	D_ASSERT(!layout.GetTypes().empty());
	this->count = 0;
	this->data_size = 0;
	scatter_functions.reserve(layout.ColumnCount());
	gather_functions.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		auto &type = layout.GetTypes()[col_idx];
		scatter_functions.emplace_back(GetScatterFunction(type));
		gather_functions.emplace_back(GetGatherFunction(type));
	}
}

void GetAllColumnIDsInternal(vector<column_t> &column_ids, const idx_t column_count) {
	column_ids.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		column_ids.emplace_back(col_idx);
	}
}

void TupleDataCollection::GetAllColumnIDs(vector<column_t> &column_ids) {
	GetAllColumnIDsInternal(column_ids, layout.ColumnCount());
}

const TupleDataLayout &TupleDataCollection::GetLayout() const {
	return layout;
}

const idx_t &TupleDataCollection::Count() const {
	return count;
}

idx_t TupleDataCollection::ChunkCount() const {
	idx_t total_chunk_count = 0;
	for (const auto &segment : segments) {
		total_chunk_count += segment.ChunkCount();
	}
	return total_chunk_count;
}

idx_t TupleDataCollection::SizeInBytes() const {
	idx_t total_size = 0;
	for (const auto &segment : segments) {
		total_size += segment.SizeInBytes();
	}
	return total_size;
}

void TupleDataCollection::Unpin() {
	for (auto &segment : segments) {
		segment.Unpin();
	}
}

// LCOV_EXCL_START
void VerifyAppendColumns(const TupleDataLayout &layout, const vector<column_t> &column_ids) {
#ifdef DEBUG
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		if (std::find(column_ids.begin(), column_ids.end(), col_idx) != column_ids.end()) {
			continue;
		}
		// This column will not be appended in the first go - verify that it is fixed-size - we cannot resize heap after
		const auto physical_type = layout.GetTypes()[col_idx].InternalType();
		D_ASSERT(physical_type != PhysicalType::VARCHAR && physical_type != PhysicalType::LIST);
		if (physical_type == PhysicalType::STRUCT) {
			const auto &struct_layout = layout.GetStructLayout(col_idx);
			vector<column_t> struct_column_ids;
			struct_column_ids.reserve(struct_layout.ColumnCount());
			for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
				struct_column_ids.emplace_back(struct_col_idx);
			}
			VerifyAppendColumns(struct_layout, struct_column_ids);
		}
	}
#endif
}
// LCOV_EXCL_STOP

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, TupleDataPinProperties properties) {
	vector<column_t> column_ids;
	GetAllColumnIDs(column_ids);
	InitializeAppend(append_state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
                                           TupleDataPinProperties properties) {
	VerifyAppendColumns(layout, column_ids);
	InitializeAppend(append_state.pin_state, properties);
	InitializeChunkState(append_state.chunk_state, std::move(column_ids));
}

void TupleDataCollection::InitializeAppend(TupleDataPinState &pin_state, TupleDataPinProperties properties) {
	pin_state.properties = properties;
	if (segments.empty()) {
		segments.emplace_back(allocator);
	}
}

static void InitializeVectorFormat(vector<TupleDataVectorFormat> &vector_data, const vector<LogicalType> &types) {
	vector_data.resize(types.size());
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			const auto &child_list = StructType::GetChildTypes(type);
			vector<LogicalType> child_types;
			child_types.reserve(child_list.size());
			for (const auto &child_entry : child_list) {
				child_types.emplace_back(child_entry.second);
			}
			InitializeVectorFormat(vector_data[col_idx].children, child_types);
			break;
		}
		case PhysicalType::LIST:
			InitializeVectorFormat(vector_data[col_idx].children, {ListType::GetChildType(type)});
			break;
		default:
			break;
		}
	}
}

void TupleDataCollection::InitializeChunkState(TupleDataChunkState &chunk_state, vector<column_t> column_ids) {
	TupleDataCollection::InitializeChunkState(chunk_state, layout.GetTypes(), std::move(column_ids));
}

void TupleDataCollection::InitializeChunkState(TupleDataChunkState &chunk_state, const vector<LogicalType> &types,
                                               vector<column_t> column_ids) {
	if (column_ids.empty()) {
		GetAllColumnIDsInternal(column_ids, types.size());
	}
	InitializeVectorFormat(chunk_state.vector_data, types);
	chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::Append(DataChunk &new_chunk, const SelectionVector &append_sel, idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state);
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(DataChunk &new_chunk, vector<column_t> column_ids, const SelectionVector &append_sel,
                                 const idx_t append_count) {
	TupleDataAppendState append_state;
	InitializeAppend(append_state, std::move(column_ids));
	Append(append_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, const idx_t append_count) {
	Append(append_state.pin_state, append_state.chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::Append(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
                                 const SelectionVector &append_sel, const idx_t append_count) {
	TupleDataCollection::ToUnifiedFormat(chunk_state, new_chunk);
	AppendUnified(pin_state, chunk_state, new_chunk, append_sel, append_count);
}

void TupleDataCollection::AppendUnified(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                        DataChunk &new_chunk, const SelectionVector &append_sel,
                                        const idx_t append_count) {
	const idx_t actual_append_count = append_count == DConstants::INVALID_INDEX ? new_chunk.size() : append_count;
	if (actual_append_count == 0) {
		return;
	}

	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(chunk_state, new_chunk, append_sel, actual_append_count);
	}

	Build(pin_state, chunk_state, 0, actual_append_count);

#ifdef DEBUG
	Vector heap_locations_copy(LogicalType::POINTER);
	if (!layout.AllConstant()) {
		VectorOperations::Copy(chunk_state.heap_locations, heap_locations_copy, actual_append_count, 0, 0);
	}
#endif

	Scatter(chunk_state, new_chunk, append_sel, actual_append_count);

#ifdef DEBUG
	// Verify that the size of the data written to the heap is the same as the size we computed it would be
	if (!layout.AllConstant()) {
		const auto original_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations_copy);
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
		const auto offset_heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);
		for (idx_t i = 0; i < actual_append_count; i++) {
			D_ASSERT(offset_heap_locations[i] == original_heap_locations[i] + heap_sizes[i]);
		}
	}
#endif
}

static inline void ToUnifiedFormatInternal(TupleDataVectorFormat &format, Vector &vector, const idx_t count) {
	vector.ToUnifiedFormat(count, format.unified);
	format.original_sel = format.unified.sel;
	format.original_owned_sel.Initialize(format.unified.owned_sel);
	switch (vector.GetType().InternalType()) {
	case PhysicalType::STRUCT: {
		auto &entries = StructVector::GetEntries(vector);
		D_ASSERT(format.children.size() == entries.size());
		for (idx_t struct_col_idx = 0; struct_col_idx < entries.size(); struct_col_idx++) {
			ToUnifiedFormatInternal(reinterpret_cast<TupleDataVectorFormat &>(format.children[struct_col_idx]),
			                        *entries[struct_col_idx], count);
		}
		break;
	}
	case PhysicalType::LIST:
		D_ASSERT(format.children.size() == 1);
		ToUnifiedFormatInternal(reinterpret_cast<TupleDataVectorFormat &>(format.children[0]),
		                        ListVector::GetEntry(vector), ListVector::GetListSize(vector));
		break;
	default:
		break;
	}
}

void TupleDataCollection::ToUnifiedFormat(TupleDataChunkState &chunk_state, DataChunk &new_chunk) {
	D_ASSERT(chunk_state.vector_data.size() >= chunk_state.column_ids.size()); // Needs InitializeAppend
	for (const auto &col_idx : chunk_state.column_ids) {
		ToUnifiedFormatInternal(chunk_state.vector_data[col_idx], new_chunk.data[col_idx], new_chunk.size());
	}
}

void TupleDataCollection::GetVectorData(const TupleDataChunkState &chunk_state, UnifiedVectorFormat result[]) {
	const auto &vector_data = chunk_state.vector_data;
	for (idx_t i = 0; i < vector_data.size(); i++) {
		const auto &source = vector_data[i].unified;
		auto &target = result[i];
		target.sel = source.sel;
		target.data = source.data;
		target.validity = source.validity;
	}
}

void TupleDataCollection::Build(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                const idx_t append_offset, const idx_t append_count) {
	auto &segment = segments.back();
	const auto size_before = segment.SizeInBytes();
	segment.allocator->Build(segment, pin_state, chunk_state, append_offset, append_count);
	data_size += segment.SizeInBytes() - size_before;
	count += append_count;
	Verify();
}

// LCOV_EXCL_START
void VerifyHeapSizes(const data_ptr_t source_locations[], const idx_t heap_sizes[], const SelectionVector &append_sel,
                     const idx_t append_count, const idx_t heap_size_offset) {
#ifdef DEBUG
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = append_sel.get_index(i);
		const auto stored_heap_size = Load<uint32_t>(source_locations[idx] + heap_size_offset);
		D_ASSERT(stored_heap_size == heap_sizes[idx]);
	}
#endif
}
// LCOV_EXCL_STOP

void TupleDataCollection::CopyRows(TupleDataChunkState &chunk_state, TupleDataChunkState &input,
                                   const SelectionVector &append_sel, const idx_t append_count) const {
	const auto source_locations = FlatVector::GetData<data_ptr_t>(input.row_locations);
	const auto target_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);

	// Copy rows
	const auto row_width = layout.GetRowWidth();
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = append_sel.get_index(i);
		FastMemcpy(target_locations[i], source_locations[idx], row_width);
	}

	// Copy heap if we need to
	if (!layout.AllConstant()) {
		const auto source_heap_locations = FlatVector::GetData<data_ptr_t>(input.heap_locations);
		const auto target_heap_locations = FlatVector::GetData<data_ptr_t>(chunk_state.heap_locations);
		const auto heap_sizes = FlatVector::GetData<idx_t>(input.heap_sizes);
		VerifyHeapSizes(source_locations, heap_sizes, append_sel, append_count, layout.GetHeapSizeOffset());

		// Check if we need to copy anything at all
		idx_t total_heap_size = 0;
		for (idx_t i = 0; i < append_count; i++) {
			auto idx = append_sel.get_index(i);
			total_heap_size += heap_sizes[idx];
		}
		if (total_heap_size == 0) {
			return;
		}

		// Copy heap
		for (idx_t i = 0; i < append_count; i++) {
			auto idx = append_sel.get_index(i);
			FastMemcpy(target_heap_locations[i], source_heap_locations[idx], heap_sizes[idx]);
		}

		// Recompute pointers after copying the data
		TupleDataAllocator::RecomputeHeapPointers(input.heap_locations, append_sel, target_locations,
		                                          chunk_state.heap_locations, 0, append_count, layout, 0);
	}
}

void TupleDataCollection::Combine(TupleDataCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (this->layout.GetTypes() != other.GetLayout().GetTypes()) {
		throw InternalException("Attempting to combine TupleDataCollection with mismatching types");
	}
	this->segments.reserve(this->segments.size() + other.segments.size());
	for (auto &other_seg : other.segments) {
		AddSegment(std::move(other_seg));
	}
	other.Reset();
}

void TupleDataCollection::AddSegment(TupleDataSegment &&segment) {
	count += segment.count;
	data_size += segment.data_size;
	segments.emplace_back(std::move(segment));
	Verify();
}

void TupleDataCollection::Combine(unique_ptr<TupleDataCollection> other) {
	Combine(*other);
}

void TupleDataCollection::Reset() {
	count = 0;
	data_size = 0;
	segments.clear();

	// Refreshes the TupleDataAllocator to prevent holding on to allocated data unnecessarily
	allocator = make_shared<TupleDataAllocator>(*allocator);
}

void TupleDataCollection::InitializeChunk(DataChunk &chunk) const {
	chunk.Initialize(allocator->GetAllocator(), layout.GetTypes());
}

void TupleDataCollection::InitializeScanChunk(TupleDataScanState &state, DataChunk &chunk) const {
	auto &column_ids = state.chunk_state.column_ids;
	D_ASSERT(!column_ids.empty());
	vector<LogicalType> chunk_types;
	chunk_types.reserve(column_ids.size());
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_idx = column_ids[i];
		D_ASSERT(column_idx < layout.ColumnCount());
		chunk_types.push_back(layout.GetTypes()[column_idx]);
	}
	chunk.Initialize(allocator->GetAllocator(), chunk_types);
}

void TupleDataCollection::InitializeScan(TupleDataScanState &state, TupleDataPinProperties properties) const {
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t i = 0; i < layout.ColumnCount(); i++) {
		column_ids.push_back(i);
	}
	InitializeScan(state, std::move(column_ids), properties);
}

void TupleDataCollection::InitializeScan(TupleDataScanState &state, vector<column_t> column_ids,
                                         TupleDataPinProperties properties) const {
	state.pin_state.row_handles.clear();
	state.pin_state.heap_handles.clear();
	state.pin_state.properties = properties;
	state.segment_index = 0;
	state.chunk_index = 0;
	state.chunk_state.column_ids = std::move(column_ids);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &gstate, TupleDataPinProperties properties) const {
	InitializeScan(gstate.scan_state, properties);
}

void TupleDataCollection::InitializeScan(TupleDataParallelScanState &state, vector<column_t> column_ids,
                                         TupleDataPinProperties properties) const {
	InitializeScan(state.scan_state, std::move(column_ids), properties);
}

bool TupleDataCollection::Scan(TupleDataScanState &state, DataChunk &result) {
	const auto segment_index_before = state.segment_index;
	idx_t segment_index;
	idx_t chunk_index;
	if (!NextScanIndex(state, segment_index, chunk_index)) {
		if (!segments.empty()) {
			FinalizePinState(state.pin_state, segments[segment_index_before]);
		}
		result.SetCardinality(0);
		return false;
	}
	if (segment_index_before != DConstants::INVALID_INDEX && segment_index != segment_index_before) {
		FinalizePinState(state.pin_state, segments[segment_index_before]);
	}
	ScanAtIndex(state.pin_state, state.chunk_state, state.chunk_state.column_ids, segment_index, chunk_index, result);
	return true;
}

bool TupleDataCollection::Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result) {
	lstate.pin_state.properties = gstate.scan_state.pin_state.properties;

	const auto segment_index_before = lstate.segment_index;
	{
		lock_guard<mutex> guard(gstate.lock);
		if (!NextScanIndex(gstate.scan_state, lstate.segment_index, lstate.chunk_index)) {
			if (!segments.empty()) {
				FinalizePinState(lstate.pin_state, segments[segment_index_before]);
			}
			result.SetCardinality(0);
			return false;
		}
	}
	if (segment_index_before != DConstants::INVALID_INDEX && segment_index_before != lstate.segment_index) {
		FinalizePinState(lstate.pin_state, segments[lstate.segment_index]);
	}
	ScanAtIndex(lstate.pin_state, lstate.chunk_state, gstate.scan_state.chunk_state.column_ids, lstate.segment_index,
	            lstate.chunk_index, result);
	return true;
}

bool TupleDataCollection::ScanComplete(const TupleDataScanState &state) const {
	if (Count() == 0) {
		return true;
	}
	return state.segment_index == segments.size() - 1 && state.chunk_index == segments.back().ChunkCount();
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state, TupleDataSegment &segment) {
	segment.allocator->ReleaseOrStoreHandles(pin_state, segment);
}

void TupleDataCollection::FinalizePinState(TupleDataPinState &pin_state) {
	D_ASSERT(!segments.empty());
	FinalizePinState(pin_state, segments.back());
}

bool TupleDataCollection::NextScanIndex(TupleDataScanState &state, idx_t &segment_index, idx_t &chunk_index) {
	// Check if we still have segments to scan
	if (state.segment_index >= segments.size()) {
		// No more data left in the scan
		return false;
	}
	// Check within the current segment if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index].ChunkCount()) {
		// Exhausted all chunks for this segment: Move to the next one
		state.segment_index++;
		state.chunk_index = 0;
		if (state.segment_index >= segments.size()) {
			return false;
		}
	}
	segment_index = state.segment_index;
	chunk_index = state.chunk_index++;
	return true;
}

void TupleDataCollection::ScanAtIndex(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
                                      const vector<column_t> &column_ids, idx_t segment_index, idx_t chunk_index,
                                      DataChunk &result) {
	auto &segment = segments[segment_index];
	auto &chunk = segment.chunks[chunk_index];
	segment.allocator->InitializeChunkState(segment, pin_state, chunk_state, chunk_index, false);
	result.Reset();
	Gather(chunk_state.row_locations, *FlatVector::IncrementalSelectionVector(), chunk.count, column_ids, result,
	       *FlatVector::IncrementalSelectionVector());
	result.SetCardinality(chunk.count);
}

// LCOV_EXCL_START
string TupleDataCollection::ToString() {
	DataChunk chunk;
	InitializeChunk(chunk);

	TupleDataScanState scan_state;
	InitializeScan(scan_state);

	string result = StringUtil::Format("TupleDataCollection - [%llu Chunks, %llu Rows]\n", ChunkCount(), Count());
	idx_t chunk_idx = 0;
	idx_t row_count = 0;
	while (Scan(scan_state, chunk)) {
		result +=
		    StringUtil::Format("Chunk %llu - [Rows %llu - %llu]\n", chunk_idx, row_count, row_count + chunk.size()) +
		    chunk.ToString();
		chunk_idx++;
		row_count += chunk.size();
	}

	return result;
}

void TupleDataCollection::Print() {
	Printer::Print(ToString());
}

void TupleDataCollection::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	idx_t total_size = 0;
	for (const auto &segment : segments) {
		segment.Verify();
		total_count += segment.count;
		total_size += segment.data_size;
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

void TupleDataCollection::VerifyEverythingPinned() const {
#ifdef DEBUG
	for (const auto &segment : segments) {
		segment.VerifyEverythingPinned();
	}
#endif
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties_p,
                                               bool init_heap)
    : TupleDataChunkIterator(collection_p, properties_p, 0, collection_p.ChunkCount(), init_heap) {
}

TupleDataChunkIterator::TupleDataChunkIterator(TupleDataCollection &collection_p, TupleDataPinProperties properties,
                                               idx_t chunk_idx_from, idx_t chunk_idx_to, bool init_heap_p)
    : collection(collection_p), init_heap(init_heap_p) {
	state.pin_state.properties = properties;
	D_ASSERT(chunk_idx_from < chunk_idx_to);
	D_ASSERT(chunk_idx_to <= collection.ChunkCount());
	idx_t overall_chunk_index = 0;
	for (idx_t segment_idx = 0; segment_idx < collection.segments.size(); segment_idx++) {
		const auto &segment = collection.segments[segment_idx];
		if (chunk_idx_from >= overall_chunk_index && chunk_idx_from <= overall_chunk_index + segment.ChunkCount()) {
			// We start in this segment
			start_segment_idx = segment_idx;
			start_chunk_idx = chunk_idx_from - overall_chunk_index;
		}
		if (chunk_idx_to >= overall_chunk_index && chunk_idx_to <= overall_chunk_index + segment.ChunkCount()) {
			// We end in this segment
			end_segment_idx = segment_idx;
			end_chunk_idx = chunk_idx_to - overall_chunk_index;
		}
		overall_chunk_index += segment.ChunkCount();
	}

	Reset();
}

void TupleDataChunkIterator::InitializeCurrentChunk() {
	auto &segment = collection.segments[current_segment_idx];
	segment.allocator->InitializeChunkState(segment, state.pin_state, state.chunk_state, current_chunk_idx, init_heap);
}

bool TupleDataChunkIterator::Done() const {
	return current_segment_idx == end_segment_idx && current_chunk_idx == end_chunk_idx;
}

bool TupleDataChunkIterator::Next() {
	D_ASSERT(!Done()); // Check if called after already done

	// Set the next indices and checks if we're at the end of the collection
	// NextScanIndex can go past this iterators 'end', so we have to check the indices again
	const auto segment_idx_before = current_segment_idx;
	if (!collection.NextScanIndex(state, current_segment_idx, current_chunk_idx) || Done()) {
		// Drop pins / stores them if TupleDataPinProperties::KEEP_EVERYTHING_PINNED
		collection.FinalizePinState(state.pin_state, collection.segments[segment_idx_before]);
		current_segment_idx = end_segment_idx;
		current_chunk_idx = end_chunk_idx;
		return false;
	}

	// Finalize pin state when moving from one segment to the next
	if (current_segment_idx != segment_idx_before) {
		collection.FinalizePinState(state.pin_state, collection.segments[segment_idx_before]);
	}

	InitializeCurrentChunk();
	return true;
}

void TupleDataChunkIterator::Reset() {
	state.segment_index = start_segment_idx;
	state.chunk_index = start_chunk_idx;
	collection.NextScanIndex(state, current_segment_idx, current_chunk_idx);
	InitializeCurrentChunk();
}

idx_t TupleDataChunkIterator::GetCurrentChunkCount() const {
	return collection.segments[current_segment_idx].chunks[current_chunk_idx].count;
}

TupleDataChunkState &TupleDataChunkIterator::GetChunkState() {
	return state.chunk_state;
}

data_ptr_t *TupleDataChunkIterator::GetRowLocations() {
	return FlatVector::GetData<data_ptr_t>(state.chunk_state.row_locations);
}

data_ptr_t *TupleDataChunkIterator::GetHeapLocations() {
	return FlatVector::GetData<data_ptr_t>(state.chunk_state.heap_locations);
}

idx_t *TupleDataChunkIterator::GetHeapSizes() {
	return FlatVector::GetData<idx_t>(state.chunk_state.heap_sizes);
}

} // namespace duckdb




namespace duckdb {

TupleDataLayout::TupleDataLayout()
    : flag_width(0), data_width(0), aggr_width(0), row_width(0), all_constant(true), heap_size_offset(0),
      has_destructor(false) {
}

TupleDataLayout TupleDataLayout::Copy() const {
	TupleDataLayout result;
	result.types = this->types;
	result.aggregates = this->aggregates;
	if (this->struct_layouts) {
		result.struct_layouts = make_uniq<unordered_map<idx_t, TupleDataLayout>>();
		for (const auto &entry : *this->struct_layouts) {
			result.struct_layouts->emplace(entry.first, entry.second.Copy());
		}
	}
	result.flag_width = this->flag_width;
	result.data_width = this->data_width;
	result.aggr_width = this->aggr_width;
	result.row_width = this->row_width;
	result.offsets = this->offsets;
	result.all_constant = this->all_constant;
	result.heap_size_offset = this->heap_size_offset;
	result.has_destructor = this->has_destructor;
	return result;
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, Aggregates aggregates_p, bool align, bool heap_offset_p) {
	offsets.clear();
	types = std::move(types_p);

	// Null mask at the front - 1 bit per value.
	flag_width = ValidityBytes::ValidityMaskSize(types.size());
	row_width = flag_width;

	// Whether all columns are constant size.
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		if (type.InternalType() == PhysicalType::STRUCT) {
			// structs are recursively stored as a TupleDataLayout again
			const auto &child_types = StructType::GetChildTypes(type);
			vector<LogicalType> child_type_vector;
			child_type_vector.reserve(child_types.size());
			for (auto &ct : child_types) {
				child_type_vector.emplace_back(ct.second);
			}
			if (!struct_layouts) {
				struct_layouts = make_uniq<unordered_map<idx_t, TupleDataLayout>>();
			}
			auto struct_entry = struct_layouts->emplace(col_idx, TupleDataLayout());
			struct_entry.first->second.Initialize(std::move(child_type_vector), false, false);
			all_constant = all_constant && struct_entry.first->second.AllConstant();
		} else {
			all_constant = all_constant && TypeIsConstantSize(type.InternalType());
		}
	}

	// This enables pointer swizzling for out-of-core computation.
	if (heap_offset_p && !all_constant) {
		heap_size_offset = row_width;
		row_width += sizeof(uint32_t);
	}

	// Data columns. No alignment required.
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		const auto &type = types[col_idx];
		offsets.push_back(row_width);
		const auto internal_type = type.InternalType();
		if (TypeIsConstantSize(internal_type) || internal_type == PhysicalType::VARCHAR) {
			row_width += GetTypeIdSize(type.InternalType());
		} else if (internal_type == PhysicalType::STRUCT) {
			// Just get the size of the TupleDataLayout of the struct
			row_width += GetStructLayout(col_idx).GetRowWidth();
		} else {
			// Variable size types use pointers to the actual data (can be swizzled).
			// Again, we would use sizeof(data_ptr_t), but this is not guaranteed to be equal to sizeof(idx_t).
			row_width += sizeof(idx_t);
		}
	}

	// Alignment padding for aggregates
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif
	data_width = row_width - flag_width;

	// Aggregate fields.
	aggregates = std::move(aggregates_p);
	for (auto &aggregate : aggregates) {
		offsets.push_back(row_width);
		row_width += aggregate.payload_size;
#ifndef DUCKDB_ALLOW_UNDEFINED
		D_ASSERT(aggregate.payload_size == AlignValue(aggregate.payload_size));
#endif
	}
	aggr_width = row_width - data_width - flag_width;

	// Alignment padding for the next row
#ifndef DUCKDB_ALLOW_UNDEFINED
	if (align) {
		row_width = AlignValue(row_width);
	}
#endif

	has_destructor = false;
	for (auto &aggr : GetAggregates()) {
		if (aggr.function.destructor) {
			has_destructor = true;
			break;
		}
	}
}

void TupleDataLayout::Initialize(vector<LogicalType> types_p, bool align, bool heap_offset_p) {
	Initialize(std::move(types_p), Aggregates(), align, heap_offset_p);
}

void TupleDataLayout::Initialize(Aggregates aggregates_p, bool align, bool heap_offset_p) {
	Initialize(vector<LogicalType>(), std::move(aggregates_p), align, heap_offset_p);
}

} // namespace duckdb





namespace duckdb {

using ValidityBytes = TupleDataLayout::ValidityBytes;

template <class T>
static constexpr idx_t TupleDataWithinListFixedSize() {
	return sizeof(T);
}

template <>
constexpr idx_t TupleDataWithinListFixedSize<string_t>() {
	return sizeof(uint32_t);
}

template <class T>
static inline void TupleDataValueStore(const T &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                       data_ptr_t &heap_location) {
	Store<T>(source, row_location + offset_in_row);
}

template <>
inline void TupleDataValueStore(const string_t &source, const data_ptr_t &row_location, const idx_t offset_in_row,
                                data_ptr_t &heap_location) {
	if (source.IsInlined()) {
		Store<string_t>(source, row_location + offset_in_row);
	} else {
		memcpy(heap_location, source.GetData(), source.GetSize());
		Store<string_t>(string_t(const_char_ptr_cast(heap_location), source.GetSize()), row_location + offset_in_row);
		heap_location += source.GetSize();
	}
}

template <class T>
static inline void TupleDataWithinListValueStore(const T &source, const data_ptr_t &location,
                                                 data_ptr_t &heap_location) {
	Store<T>(source, location);
}

template <>
inline void TupleDataWithinListValueStore(const string_t &source, const data_ptr_t &location,
                                          data_ptr_t &heap_location) {
	Store<uint32_t>(source.GetSize(), location);
	memcpy(heap_location, source.GetData(), source.GetSize());
	heap_location += source.GetSize();
}

template <class T>
static inline T TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &heap_location) {
	return Load<T>(location);
}

template <>
inline string_t TupleDataWithinListValueLoad(const data_ptr_t &location, data_ptr_t &heap_location) {
	const auto size = Load<uint32_t>(location);
	string_t result(const_char_ptr_cast(heap_location), size);
	heap_location += size;
	return result;
}

#ifdef DEBUG
static void ResetCombinedListData(vector<TupleDataVectorFormat> &vector_data) {
	for (auto &vd : vector_data) {
		vd.combined_list_data = nullptr;
		ResetCombinedListData(vd.children);
	}
}
#endif

void TupleDataCollection::ComputeHeapSizes(TupleDataChunkState &chunk_state, const DataChunk &new_chunk,
                                           const SelectionVector &append_sel, const idx_t append_count) {
#ifdef DEBUG
	ResetCombinedListData(chunk_state.vector_data);
#endif

	auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
	std::fill_n(heap_sizes, new_chunk.size(), 0);

	for (idx_t col_idx = 0; col_idx < new_chunk.ColumnCount(); col_idx++) {
		auto &source_v = new_chunk.data[col_idx];
		auto &source_format = chunk_state.vector_data[col_idx];
		TupleDataCollection::ComputeHeapSizes(chunk_state.heap_sizes, source_v, source_format, append_sel,
		                                      append_count);
	}
}

static inline idx_t StringHeapSize(const string_t &val) {
	return val.IsInlined() ? 0 : val.GetSize();
}

void TupleDataCollection::ComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                           TupleDataVectorFormat &source_format, const SelectionVector &append_sel,
                                           const idx_t append_count) {
	const auto type = source_v.GetType().InternalType();
	if (type != PhysicalType::VARCHAR && type != PhysicalType::STRUCT && type != PhysicalType::LIST) {
		return;
	}

	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	const auto &source_vector_data = source_format.unified;
	const auto &source_sel = *source_vector_data.sel;
	const auto &source_validity = source_vector_data.validity;

	switch (type) {
	case PhysicalType::VARCHAR: {
		// Only non-inlined strings are stored in the heap
		const auto source_data = UnifiedVectorFormat::GetData<string_t>(source_vector_data);
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx)) {
				heap_sizes[i] += StringHeapSize(source_data[source_idx]);
			} else {
				heap_sizes[i] += StringHeapSize(NullValue<string_t>());
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		// Recurse through the struct children
		auto &struct_sources = StructVector::GetEntries(source_v);
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			const auto &struct_source = struct_sources[struct_col_idx];
			auto &struct_format = source_format.children[struct_col_idx];
			TupleDataCollection::ComputeHeapSizes(heap_sizes_v, *struct_source, struct_format, append_sel,
			                                      append_count);
		}
		break;
	}
	case PhysicalType::LIST: {
		// Lists are stored entirely in the heap
		for (idx_t i = 0; i < append_count; i++) {
			auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (source_validity.RowIsValid(source_idx)) {
				heap_sizes[i] += sizeof(uint64_t); // Size of the list
			}
		}

		// Recurse
		D_ASSERT(source_format.children.size() == 1);
		auto &child_source_v = ListVector::GetEntry(source_v);
		auto &child_format = source_format.children[0];
		TupleDataCollection::WithinListHeapComputeSizes(heap_sizes_v, child_source_v, child_format, append_sel,
		                                                append_count, source_vector_data);
		break;
	}
	default:
		throw NotImplementedException("ComputeHeapSizes for %s", EnumUtil::ToString(source_v.GetType().id()));
	}
}

void TupleDataCollection::WithinListHeapComputeSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                     TupleDataVectorFormat &source_format,
                                                     const SelectionVector &append_sel, const idx_t append_count,
                                                     const UnifiedVectorFormat &list_data) {
	auto type = source_v.GetType().InternalType();
	if (TypeIsConstantSize(type)) {
		TupleDataCollection::ComputeFixedWithinListHeapSizes(heap_sizes_v, source_v, source_format, append_sel,
		                                                     append_count, list_data);
		return;
	}

	switch (type) {
	case PhysicalType::VARCHAR:
		TupleDataCollection::StringWithinListComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel,
		                                                      append_count, list_data);
		break;
	case PhysicalType::STRUCT:
		TupleDataCollection::StructWithinListComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel,
		                                                      append_count, list_data);
		break;
	case PhysicalType::LIST:
		TupleDataCollection::ListWithinListComputeHeapSizes(heap_sizes_v, source_v, source_format, append_sel,
		                                                    append_count, list_data);
		break;
	default:
		throw NotImplementedException("WithinListHeapComputeSizes for %s", EnumUtil::ToString(source_v.GetType().id()));
	}
}

void TupleDataCollection::ComputeFixedWithinListHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                          TupleDataVectorFormat &source_format,
                                                          const SelectionVector &append_sel, const idx_t append_count,
                                                          const UnifiedVectorFormat &list_data) {
	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	D_ASSERT(TypeIsConstantSize(source_v.GetType().InternalType()));
	const auto type_size = GetTypeIdSize(source_v.GetType().InternalType());
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list length
		const auto &list_length = list_entries[list_idx].length;

		// Size is validity mask and all values
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * type_size;
	}
}

void TupleDataCollection::StringWithinListComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                           TupleDataVectorFormat &source_format,
                                                           const SelectionVector &append_sel, const idx_t append_count,
                                                           const UnifiedVectorFormat &list_data) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<string_t>(source_data);
	const auto &source_validity = source_data.validity;

	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Size is validity mask and all string sizes
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * TupleDataWithinListFixedSize<string_t>();

		// Plus all the actual strings
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (source_validity.RowIsValid(child_source_idx)) {
				heap_size += data[child_source_idx].GetSize();
			}
		}
	}
}

void TupleDataCollection::StructWithinListComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                           TupleDataVectorFormat &source_format,
                                                           const SelectionVector &append_sel, const idx_t append_count,
                                                           const UnifiedVectorFormat &list_data) {
	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list length
		const auto &list_length = list_entries[list_idx].length;

		// Size is just the validity mask
		heap_sizes[i] += ValidityBytes::SizeInBytes(list_length);
	}

	// Recurse
	auto &struct_sources = StructVector::GetEntries(source_v);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];
		auto &struct_format = source_format.children[struct_col_idx];
		TupleDataCollection::WithinListHeapComputeSizes(heap_sizes_v, struct_source, struct_format, append_sel,
		                                                append_count, list_data);
	}
}

static void ApplySliceRecursive(const Vector &source_v, TupleDataVectorFormat &source_format,
                                const SelectionVector &combined_sel, const idx_t count) {
	D_ASSERT(source_format.combined_list_data);
	auto &combined_list_data = *source_format.combined_list_data;

	combined_list_data.selection_data = source_format.original_sel->Slice(combined_sel, count);
	source_format.unified.owned_sel.Initialize(combined_list_data.selection_data);
	source_format.unified.sel = &source_format.unified.owned_sel;

	if (source_v.GetType().InternalType() == PhysicalType::STRUCT) {
		// We have to apply it to the child vectors too
		auto &struct_sources = StructVector::GetEntries(source_v);
		for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
			auto &struct_source = *struct_sources[struct_col_idx];
			auto &struct_format = source_format.children[struct_col_idx];
#ifdef DEBUG
			D_ASSERT(!struct_format.combined_list_data);
#endif
			if (!struct_format.combined_list_data) {
				struct_format.combined_list_data = make_uniq<CombinedListData>();
			}
			ApplySliceRecursive(struct_source, struct_format, *source_format.unified.sel, count);
		}
	}
}

void TupleDataCollection::ListWithinListComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
                                                         TupleDataVectorFormat &source_format,
                                                         const SelectionVector &append_sel, const idx_t append_count,
                                                         const UnifiedVectorFormat &list_data) {
	// List data (of the list Vector that "source_v" is in)
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Child list ("source_v")
	const auto &child_list_data = source_format.unified;
	const auto child_list_sel = *child_list_data.sel;
	const auto child_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_list_data);
	const auto &child_list_validity = child_list_data.validity;

	// Figure out actual child list size (can differ from ListVector::GetListSize if dict/const vector),
	// and we cannot use ConstantVector::ZeroSelectionVector because it may need to be longer than STANDARD_VECTOR_SIZE
	idx_t sum_of_sizes = 0;
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue;
		}
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (!child_list_validity.RowIsValid(child_list_idx)) {
				continue;
			}

			const auto &child_list_entry = child_list_entries[child_list_idx];
			const auto &child_list_length = child_list_entry.length;

			sum_of_sizes += child_list_length;
		}
	}
	const auto child_list_child_count = MaxValue<idx_t>(sum_of_sizes, ListVector::GetListSize(source_v));

	// Target
	auto heap_sizes = FlatVector::GetData<idx_t>(heap_sizes_v);

	// Construct combined list entries and a selection vector for the child list child
	auto &child_format = source_format.children[0];
#ifdef DEBUG
	// In debug mode this should be deleted by ResetCombinedListData
	D_ASSERT(!child_format.combined_list_data);
#endif
	if (!child_format.combined_list_data) {
		child_format.combined_list_data = make_uniq<CombinedListData>();
	}
	auto &combined_list_data = *child_format.combined_list_data;
	auto &combined_list_entries = combined_list_data.combined_list_entries;
	SelectionVector combined_sel(child_list_child_count);
	for (idx_t i = 0; i < child_list_child_count; i++) {
		combined_sel.set_index(i, 0);
	}

	idx_t combined_list_offset = 0;
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child list
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Size is the validity mask and the list sizes
		auto &heap_size = heap_sizes[i];
		heap_size += ValidityBytes::SizeInBytes(list_length);
		heap_size += list_length * sizeof(uint64_t);

		idx_t child_list_size = 0;
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			const auto &child_list_entry = child_list_entries[child_list_idx];
			if (child_list_validity.RowIsValid(child_list_idx)) {
				const auto &child_list_offset = child_list_entry.offset;
				const auto &child_list_length = child_list_entry.length;

				// Add this child's list entries to the combined selection vector
				for (idx_t child_value_i = 0; child_value_i < child_list_length; child_value_i++) {
					auto idx = combined_list_offset + child_list_size + child_value_i;
					auto loc = child_list_offset + child_value_i;
					combined_sel.set_index(idx, loc);
				}

				child_list_size += child_list_length;
			}
		}

		// Combine the child list entries into one
		combined_list_entries[list_idx] = {combined_list_offset, child_list_size};
		combined_list_offset += child_list_size;
	}

	// Create a combined child_list_data to be used as list_data in the recursion
	auto &combined_child_list_data = combined_list_data.combined_data;
	combined_child_list_data.sel = list_data.sel;
	combined_child_list_data.data = data_ptr_cast(combined_list_entries);
	combined_child_list_data.validity = list_data.validity;

	// Combine the selection vectors
	D_ASSERT(source_format.children.size() == 1);
	auto &child_source = ListVector::GetEntry(source_v);
	ApplySliceRecursive(child_source, child_format, combined_sel, child_list_child_count);

	// Recurse
	TupleDataCollection::WithinListHeapComputeSizes(heap_sizes_v, child_source, child_format, append_sel, append_count,
	                                                combined_child_list_data);
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, const DataChunk &new_chunk,
                                  const SelectionVector &append_sel, const idx_t append_count) const {
	const auto row_locations = FlatVector::GetData<data_ptr_t>(chunk_state.row_locations);

	// Set the validity mask for each row before inserting data
	const auto validity_bytes = ValidityBytes::SizeInBytes(layout.ColumnCount());
	for (idx_t i = 0; i < append_count; i++) {
		FastMemset(row_locations[i], ~0, validity_bytes);
	}

	if (!layout.AllConstant()) {
		// Set the heap size for each row
		const auto heap_size_offset = layout.GetHeapSizeOffset();
		const auto heap_sizes = FlatVector::GetData<idx_t>(chunk_state.heap_sizes);
		for (idx_t i = 0; i < append_count; i++) {
			Store<uint32_t>(heap_sizes[i], row_locations[i] + heap_size_offset);
		}
	}

	// Write the data
	for (const auto &col_idx : chunk_state.column_ids) {
		Scatter(chunk_state, new_chunk.data[col_idx], col_idx, append_sel, append_count);
	}
}

void TupleDataCollection::Scatter(TupleDataChunkState &chunk_state, const Vector &source, const column_t column_id,
                                  const SelectionVector &append_sel, const idx_t append_count) const {
	const auto &scatter_function = scatter_functions[column_id];
	scatter_function.function(source, chunk_state.vector_data[column_id], append_sel, append_count, layout,
	                          chunk_state.row_locations, chunk_state.heap_locations, column_id,
	                          chunk_state.vector_data[column_id].unified, scatter_function.child_functions);
}

template <class T>
static void TupleDataTemplatedScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                      const SelectionVector &append_sel, const idx_t append_count,
                                      const TupleDataLayout &layout, const Vector &row_locations,
                                      Vector &heap_locations, const idx_t col_idx, const UnifiedVectorFormat &dummy_arg,
                                      const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<T>(source_data);
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	if (validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
		}
	} else {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (validity.RowIsValid(source_idx)) {
				TupleDataValueStore<T>(data[source_idx], target_locations[i], offset_in_row, target_heap_locations[i]);
			} else {
				TupleDataValueStore<T>(NullValue<T>(), target_locations[i], offset_in_row, target_heap_locations[i]);
				ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
		}
	}
}

static void TupleDataStructScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                   const SelectionVector &append_sel, const idx_t append_count,
                                   const TupleDataLayout &layout, const Vector &row_locations, Vector &heap_locations,
                                   const idx_t col_idx, const UnifiedVectorFormat &dummy_arg,
                                   const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the STRUCT in this layout
	if (!validity.AllValid()) {
		for (idx_t i = 0; i < append_count; i++) {
			const auto source_idx = source_sel.get_index(append_sel.get_index(i));
			if (!validity.RowIsValid(source_idx)) {
				ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
		}
	}

	// Create a Vector of pointers to the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER, append_count);
	auto struct_target_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		struct_target_locations[i] = target_locations[i] + offset_in_row;
	}

	const auto &struct_layout = layout.GetStructLayout(col_idx);
	auto &struct_sources = StructVector::GetEntries(source);
	D_ASSERT(struct_layout.ColumnCount() == struct_sources.size());

	// Set the validity of the entries within the STRUCTs
	const auto validity_bytes = ValidityBytes::SizeInBytes(struct_layout.ColumnCount());
	for (idx_t i = 0; i < append_count; i++) {
		memset(struct_target_locations[i], ~0, validity_bytes);
	}

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];
		const auto &struct_source_format = source_format.children[struct_col_idx];
		const auto &struct_scatter_function = child_functions[struct_col_idx];
		struct_scatter_function.function(struct_source, struct_source_format, append_sel, append_count, struct_layout,
		                                 struct_row_locations, heap_locations, struct_col_idx, dummy_arg,
		                                 struct_scatter_function.child_functions);
	}
}

static void TupleDataListScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                 const SelectionVector &append_sel, const idx_t append_count,
                                 const TupleDataLayout &layout, const Vector &row_locations, Vector &heap_locations,
                                 const idx_t col_idx, const UnifiedVectorFormat &dummy_arg,
                                 const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<list_entry_t>(source_data);
	const auto &validity = source_data.validity;

	// Target
	auto target_locations = FlatVector::GetData<data_ptr_t>(row_locations);
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Set validity of the LIST in this layout, and store pointer to where it's stored
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < append_count; i++) {
		const auto source_idx = source_sel.get_index(append_sel.get_index(i));
		if (validity.RowIsValid(source_idx)) {
			auto &target_heap_location = target_heap_locations[i];
			Store<data_ptr_t>(target_heap_location, target_locations[i] + offset_in_row);

			// Store list length and skip over it
			Store<uint64_t>(data[source_idx].length, target_heap_location);
			target_heap_location += sizeof(uint64_t);
		} else {
			ValidityBytes(target_locations[i]).SetInvalidUnsafe(entry_idx, idx_in_entry);
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	auto &child_source = ListVector::GetEntry(source);
	auto &child_format = source_format.children[0];
	const auto &child_function = child_functions[0];
	child_function.function(child_source, child_format, append_sel, append_count, layout, row_locations, heap_locations,
	                        col_idx, source_format.unified, child_function.child_functions);
}

template <class T>
static void TupleDataTemplatedWithinListScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                                const SelectionVector &append_sel, const idx_t append_count,
                                                const TupleDataLayout &layout, const Vector &row_locations,
                                                Vector &heap_locations, const idx_t col_idx,
                                                const UnifiedVectorFormat &list_data,
                                                const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto data = UnifiedVectorFormat::GetData<T>(source_data);
	const auto &source_validity = source_data.validity;

	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Store the data and validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (source_validity.RowIsValid(child_source_idx)) {
				TupleDataWithinListValueStore<T>(data[child_source_idx],
				                                 child_data_location + child_i * TupleDataWithinListFixedSize<T>(),
				                                 target_heap_location);
			} else {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}
}

static void TupleDataStructWithinListScatter(const Vector &source, const TupleDataVectorFormat &source_format,
                                             const SelectionVector &append_sel, const idx_t append_count,
                                             const TupleDataLayout &layout, const Vector &row_locations,
                                             Vector &heap_locations, const idx_t col_idx,
                                             const UnifiedVectorFormat &list_data,
                                             const vector<TupleDataScatterFunction> &child_functions) {
	// Source
	const auto &source_data = source_format.unified;
	const auto &source_sel = *source_data.sel;
	const auto &source_validity = source_data.validity;

	// List data
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	// Initialize the validity of the STRUCTs
	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip the heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Store the validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_source_idx = source_sel.get_index(list_offset + child_i);
			if (!source_validity.RowIsValid(child_source_idx)) {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}

	// Recurse through the children
	auto &struct_sources = StructVector::GetEntries(source);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_sources.size(); struct_col_idx++) {
		auto &struct_source = *struct_sources[struct_col_idx];
		auto &struct_format = source_format.children[struct_col_idx];
		const auto &struct_scatter_function = child_functions[struct_col_idx];
		struct_scatter_function.function(struct_source, struct_format, append_sel, append_count, layout, row_locations,
		                                 heap_locations, struct_col_idx, list_data,
		                                 struct_scatter_function.child_functions);
	}
}

static void TupleDataListWithinListScatter(const Vector &child_list, const TupleDataVectorFormat &child_list_format,
                                           const SelectionVector &append_sel, const idx_t append_count,
                                           const TupleDataLayout &layout, const Vector &row_locations,
                                           Vector &heap_locations, const idx_t col_idx,
                                           const UnifiedVectorFormat &list_data,
                                           const vector<TupleDataScatterFunction> &child_functions) {
	// List data (of the list Vector that "child_list" is in)
	const auto list_sel = *list_data.sel;
	const auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	const auto &list_validity = list_data.validity;

	// Child list
	const auto &child_list_data = child_list_format.unified;
	const auto child_list_sel = *child_list_data.sel;
	const auto child_list_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_list_data);
	const auto &child_list_validity = child_list_data.validity;

	// Target
	auto target_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);

	for (idx_t i = 0; i < append_count; i++) {
		const auto list_idx = list_sel.get_index(append_sel.get_index(i));
		if (!list_validity.RowIsValid(list_idx)) {
			continue; // Original list entry is invalid - no need to serialize the child list
		}

		// Get the current list entry
		const auto &list_entry = list_entries[list_idx];
		const auto &list_offset = list_entry.offset;
		const auto &list_length = list_entry.length;

		// Initialize validity mask and skip heap pointer over it
		auto &target_heap_location = target_heap_locations[i];
		ValidityBytes child_mask(target_heap_location);
		child_mask.SetAllValid(list_length);
		target_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto child_data_location = target_heap_location;
		target_heap_location += list_length * sizeof(uint64_t);

		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			const auto child_list_idx = child_list_sel.get_index(list_offset + child_i);
			if (child_list_validity.RowIsValid(child_list_idx)) {
				const auto &child_list_length = child_list_entries[child_list_idx].length;
				Store<uint64_t>(child_list_length, child_data_location + child_i * sizeof(uint64_t));
			} else {
				child_mask.SetInvalidUnsafe(child_i);
			}
		}
	}

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	auto &child_vec = ListVector::GetEntry(child_list);
	auto &child_format = child_list_format.children[0];
	auto &combined_child_list_data = child_format.combined_list_data->combined_data;
	const auto &child_function = child_functions[0];
	child_function.function(child_vec, child_format, append_sel, append_count, layout, row_locations, heap_locations,
	                        col_idx, combined_child_list_data, child_function.child_functions);
}

template <class T>
tuple_data_scatter_function_t TupleDataGetScatterFunction(bool within_list) {
	return within_list ? TupleDataTemplatedWithinListScatter<T> : TupleDataTemplatedScatter<T>;
}

TupleDataScatterFunction TupleDataCollection::GetScatterFunction(const LogicalType &type, bool within_list) {
	TupleDataScatterFunction result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.function = TupleDataGetScatterFunction<bool>(within_list);
		break;
	case PhysicalType::INT8:
		result.function = TupleDataGetScatterFunction<int8_t>(within_list);
		break;
	case PhysicalType::INT16:
		result.function = TupleDataGetScatterFunction<int16_t>(within_list);
		break;
	case PhysicalType::INT32:
		result.function = TupleDataGetScatterFunction<int32_t>(within_list);
		break;
	case PhysicalType::INT64:
		result.function = TupleDataGetScatterFunction<int64_t>(within_list);
		break;
	case PhysicalType::INT128:
		result.function = TupleDataGetScatterFunction<hugeint_t>(within_list);
		break;
	case PhysicalType::UINT8:
		result.function = TupleDataGetScatterFunction<uint8_t>(within_list);
		break;
	case PhysicalType::UINT16:
		result.function = TupleDataGetScatterFunction<uint16_t>(within_list);
		break;
	case PhysicalType::UINT32:
		result.function = TupleDataGetScatterFunction<uint32_t>(within_list);
		break;
	case PhysicalType::UINT64:
		result.function = TupleDataGetScatterFunction<uint64_t>(within_list);
		break;
	case PhysicalType::FLOAT:
		result.function = TupleDataGetScatterFunction<float>(within_list);
		break;
	case PhysicalType::DOUBLE:
		result.function = TupleDataGetScatterFunction<double>(within_list);
		break;
	case PhysicalType::INTERVAL:
		result.function = TupleDataGetScatterFunction<interval_t>(within_list);
		break;
	case PhysicalType::VARCHAR:
		result.function = TupleDataGetScatterFunction<string_t>(within_list);
		break;
	case PhysicalType::STRUCT: {
		result.function = within_list ? TupleDataStructWithinListScatter : TupleDataStructScatter;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			result.child_functions.push_back(GetScatterFunction(child_type.second, within_list));
		}
		break;
	}
	case PhysicalType::LIST:
		result.function = within_list ? TupleDataListWithinListScatter : TupleDataListScatter;
		result.child_functions.emplace_back(GetScatterFunction(ListType::GetChildType(type), true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetScatterFunction");
	}
	return result;
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 DataChunk &result, const SelectionVector &target_sel) const {
	D_ASSERT(result.ColumnCount() == layout.ColumnCount());
	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	Gather(row_locations, scan_sel, scan_count, column_ids, result, target_sel);
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const vector<column_t> &column_ids, DataChunk &result,
                                 const SelectionVector &target_sel) const {
	for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
		Gather(row_locations, scan_sel, scan_count, column_ids[col_idx], result.data[col_idx], target_sel);
	}
}

void TupleDataCollection::Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
                                 const column_t column_id, Vector &result, const SelectionVector &target_sel) const {
	const auto &gather_function = gather_functions[column_id];
	gather_function.function(layout, row_locations, column_id, scan_sel, scan_count, result, target_sel, result,
	                         gather_function.child_functions);
}

template <class T>
static void TupleDataTemplatedGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                     const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                     const SelectionVector &target_sel, Vector &dummy_vector,
                                     const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < scan_count; i++) {
		const auto &source_row = source_locations[scan_sel.get_index(i)];
		const auto target_idx = target_sel.get_index(i);
		ValidityBytes row_mask(source_row);
		if (row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			target_data[target_idx] = Load<T>(source_row + offset_in_row);
		} else {
			target_validity.SetInvalid(target_idx);
		}
	}
}

static void TupleDataStructGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                  const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                  const SelectionVector &target_sel, Vector &dummy_vector,
                                  const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Get validity of the struct and create a Vector of pointers to the start of the TupleDataLayout of the STRUCT
	Vector struct_row_locations(LogicalType::POINTER);
	auto struct_source_locations = FlatVector::GetData<data_ptr_t>(struct_row_locations);
	const auto offset_in_row = layout.GetOffsets()[col_idx];
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		const auto &source_row = source_locations[source_idx];

		// Set the validity
		ValidityBytes row_mask(source_row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			const auto target_idx = target_sel.get_index(i);
			target_validity.SetInvalid(target_idx);
		}

		// Set the pointer
		struct_source_locations[source_idx] = source_row + offset_in_row;
	}

	// Get the struct layout and struct entries
	const auto &struct_layout = layout.GetStructLayout(col_idx);
	auto &struct_targets = StructVector::GetEntries(target);
	D_ASSERT(struct_layout.ColumnCount() == struct_targets.size());

	// Recurse through the struct children
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_layout.ColumnCount(); struct_col_idx++) {
		auto &struct_target = *struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[struct_col_idx];
		struct_gather_function.function(struct_layout, struct_row_locations, struct_col_idx, scan_sel, scan_count,
		                                struct_target, target_sel, dummy_vector,
		                                struct_gather_function.child_functions);
	}
}

static void TupleDataListGather(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                const SelectionVector &target_sel, Vector &dummy_vector,
                                const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_locations = FlatVector::GetData<data_ptr_t>(row_locations);

	// Target
	auto target_list_entries = FlatVector::GetData<list_entry_t>(target);
	auto &target_validity = FlatVector::Validity(target);

	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);

	// Load pointers to the data from the row
	Vector heap_locations(LogicalType::POINTER);
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	const auto offset_in_row = layout.GetOffsets()[col_idx];
	uint64_t target_list_offset = 0;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		const auto target_idx = target_sel.get_index(i);

		const auto &source_row = source_locations[source_idx];
		ValidityBytes row_mask(source_row);
		if (row_mask.RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
			auto &source_heap_location = source_heap_locations[source_idx];
			source_heap_location = Load<data_ptr_t>(source_row + offset_in_row);

			// Load list size and skip over
			const auto list_length = Load<uint64_t>(source_heap_location);
			source_heap_location += sizeof(uint64_t);

			// Initialize list entry, and increment offset
			target_list_entries[target_idx] = {target_list_offset, list_length};
			target_list_offset += list_length;
		} else {
			source_heap_validity.SetInvalid(source_idx);
			target_validity.SetInvalid(target_idx);
		}
	}
	auto list_size_before = ListVector::GetListSize(target);
	ListVector::Reserve(target, list_size_before + target_list_offset);
	ListVector::SetListSize(target, list_size_before + target_list_offset);

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(layout, heap_locations, list_size_before, scan_sel, scan_count,
	                        ListVector::GetEntry(target), target_sel, target, child_function.child_functions);
}

template <class T>
static void TupleDataTemplatedWithinListGather(const TupleDataLayout &layout, Vector &heap_locations,
                                               const idx_t list_size_before, const SelectionVector &scan_sel,
                                               const idx_t scan_count, Vector &target,
                                               const SelectionVector &target_sel, Vector &list_vector,
                                               const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	// Target
	auto target_data = FlatVector::GetData<T>(target);
	auto &target_validity = FlatVector::Validity(target);

	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(list_vector);

	uint64_t target_offset = list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		if (!source_heap_validity.RowIsValid(source_idx)) {
			continue;
		}

		const auto &list_length = list_entries[target_sel.get_index(i)].length;

		// Initialize validity mask
		auto &source_heap_location = source_heap_locations[source_idx];
		ValidityBytes source_mask(source_heap_location);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto source_data_location = source_heap_location;
		source_heap_location += list_length * TupleDataWithinListFixedSize<T>();

		// Load the child validity and data belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (source_mask.RowIsValidUnsafe(child_i)) {
				target_data[target_offset + child_i] = TupleDataWithinListValueLoad<T>(
				    source_data_location + child_i * TupleDataWithinListFixedSize<T>(), source_heap_location);
			} else {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}
		target_offset += list_length;
	}
}

static void TupleDataStructWithinListGather(const TupleDataLayout &layout, Vector &heap_locations,
                                            const idx_t list_size_before, const SelectionVector &scan_sel,
                                            const idx_t scan_count, Vector &target, const SelectionVector &target_sel,
                                            Vector &list_vector,
                                            const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	// Target
	auto &target_validity = FlatVector::Validity(target);

	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(list_vector);

	uint64_t target_offset = list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		if (!source_heap_validity.RowIsValid(source_idx)) {
			continue;
		}

		const auto &list_length = list_entries[target_sel.get_index(i)].length;

		// Initialize validity mask and skip over it
		auto &source_heap_location = source_heap_locations[source_idx];
		ValidityBytes source_mask(source_heap_location);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Load the child validity belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (!source_mask.RowIsValidUnsafe(child_i)) {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}
		target_offset += list_length;
	}

	// Recurse
	auto &struct_targets = StructVector::GetEntries(target);
	for (idx_t struct_col_idx = 0; struct_col_idx < struct_targets.size(); struct_col_idx++) {
		auto &struct_target = *struct_targets[struct_col_idx];
		const auto &struct_gather_function = child_functions[struct_col_idx];
		struct_gather_function.function(layout, heap_locations, list_size_before, scan_sel, scan_count, struct_target,
		                                target_sel, list_vector, struct_gather_function.child_functions);
	}
}

static void TupleDataListWithinListGather(const TupleDataLayout &layout, Vector &heap_locations,
                                          const idx_t list_size_before, const SelectionVector &scan_sel,
                                          const idx_t scan_count, Vector &target, const SelectionVector &target_sel,
                                          Vector &list_vector, const vector<TupleDataGatherFunction> &child_functions) {
	// Source
	auto source_heap_locations = FlatVector::GetData<data_ptr_t>(heap_locations);
	auto &source_heap_validity = FlatVector::Validity(heap_locations);

	// Target
	auto target_list_entries = FlatVector::GetData<list_entry_t>(target);
	auto &target_validity = FlatVector::Validity(target);
	const auto child_list_size_before = ListVector::GetListSize(target);

	// List parent
	const auto list_entries = FlatVector::GetData<list_entry_t>(list_vector);

	// We need to create a vector that has the combined list sizes (hugeint_t has same size as list_entry_t)
	Vector combined_list_vector(LogicalType::HUGEINT);
	auto combined_list_entries = FlatVector::GetData<list_entry_t>(combined_list_vector);

	uint64_t target_offset = list_size_before;
	uint64_t target_child_offset = child_list_size_before;
	for (idx_t i = 0; i < scan_count; i++) {
		const auto source_idx = scan_sel.get_index(i);
		if (!source_heap_validity.RowIsValid(source_idx)) {
			continue;
		}

		const auto &list_length = list_entries[target_sel.get_index(i)].length;

		// Initialize validity mask and skip over it
		auto &source_heap_location = source_heap_locations[source_idx];
		ValidityBytes source_mask(source_heap_location);
		source_heap_location += ValidityBytes::SizeInBytes(list_length);

		// Get the start to the fixed-size data and skip the heap pointer over it
		const auto source_data_location = source_heap_location;
		source_heap_location += list_length * sizeof(uint64_t);

		// Set the offset of the combined list entry
		auto &combined_list_entry = combined_list_entries[target_sel.get_index(i)];
		combined_list_entry.offset = target_child_offset;

		// Load the child validity and data belonging to this list entry
		for (idx_t child_i = 0; child_i < list_length; child_i++) {
			if (source_mask.RowIsValidUnsafe(child_i)) {
				auto &target_list_entry = target_list_entries[target_offset + child_i];
				target_list_entry.offset = target_child_offset;
				target_list_entry.length = Load<uint64_t>(source_data_location + child_i * sizeof(uint64_t));
				target_child_offset += target_list_entry.length;
			} else {
				target_validity.SetInvalid(target_offset + child_i);
			}
		}

		// Set the length of the combined list entry
		combined_list_entry.length = target_child_offset - combined_list_entry.offset;

		target_offset += list_length;
	}
	ListVector::Reserve(target, target_child_offset);
	ListVector::SetListSize(target, target_child_offset);

	// Recurse
	D_ASSERT(child_functions.size() == 1);
	const auto &child_function = child_functions[0];
	child_function.function(layout, heap_locations, child_list_size_before, scan_sel, scan_count,
	                        ListVector::GetEntry(target), target_sel, combined_list_vector,
	                        child_function.child_functions);
}

template <class T>
tuple_data_gather_function_t TupleDataGetGatherFunction(bool within_list) {
	return within_list ? TupleDataTemplatedWithinListGather<T> : TupleDataTemplatedGather<T>;
}

TupleDataGatherFunction TupleDataCollection::GetGatherFunction(const LogicalType &type, bool within_list) {
	TupleDataGatherFunction result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result.function = TupleDataGetGatherFunction<bool>(within_list);
		break;
	case PhysicalType::INT8:
		result.function = TupleDataGetGatherFunction<int8_t>(within_list);
		break;
	case PhysicalType::INT16:
		result.function = TupleDataGetGatherFunction<int16_t>(within_list);
		break;
	case PhysicalType::INT32:
		result.function = TupleDataGetGatherFunction<int32_t>(within_list);
		break;
	case PhysicalType::INT64:
		result.function = TupleDataGetGatherFunction<int64_t>(within_list);
		break;
	case PhysicalType::INT128:
		result.function = TupleDataGetGatherFunction<hugeint_t>(within_list);
		break;
	case PhysicalType::UINT8:
		result.function = TupleDataGetGatherFunction<uint8_t>(within_list);
		break;
	case PhysicalType::UINT16:
		result.function = TupleDataGetGatherFunction<uint16_t>(within_list);
		break;
	case PhysicalType::UINT32:
		result.function = TupleDataGetGatherFunction<uint32_t>(within_list);
		break;
	case PhysicalType::UINT64:
		result.function = TupleDataGetGatherFunction<uint64_t>(within_list);
		break;
	case PhysicalType::FLOAT:
		result.function = TupleDataGetGatherFunction<float>(within_list);
		break;
	case PhysicalType::DOUBLE:
		result.function = TupleDataGetGatherFunction<double>(within_list);
		break;
	case PhysicalType::INTERVAL:
		result.function = TupleDataGetGatherFunction<interval_t>(within_list);
		break;
	case PhysicalType::VARCHAR:
		result.function = TupleDataGetGatherFunction<string_t>(within_list);
		break;
	case PhysicalType::STRUCT: {
		result.function = within_list ? TupleDataStructWithinListGather : TupleDataStructGather;
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			result.child_functions.push_back(GetGatherFunction(child_type.second, within_list));
		}
		break;
	}
	case PhysicalType::LIST:
		result.function = within_list ? TupleDataListWithinListGather : TupleDataListGather;
		result.child_functions.push_back(GetGatherFunction(ListType::GetChildType(type), true));
		break;
	default:
		throw InternalException("Unsupported type for TupleDataCollection::GetGatherFunction");
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

TupleDataChunkPart::TupleDataChunkPart(mutex &lock_p) : lock(lock_p) {
}

void SwapTupleDataChunkPart(TupleDataChunkPart &a, TupleDataChunkPart &b) {
	std::swap(a.row_block_index, b.row_block_index);
	std::swap(a.row_block_offset, b.row_block_offset);
	std::swap(a.heap_block_index, b.heap_block_index);
	std::swap(a.heap_block_offset, b.heap_block_offset);
	std::swap(a.base_heap_ptr, b.base_heap_ptr);
	std::swap(a.total_heap_size, b.total_heap_size);
	std::swap(a.count, b.count);
	std::swap(a.lock, b.lock);
}

TupleDataChunkPart::TupleDataChunkPart(TupleDataChunkPart &&other) noexcept : lock((other.lock)) {
	SwapTupleDataChunkPart(*this, other);
}

TupleDataChunkPart &TupleDataChunkPart::operator=(TupleDataChunkPart &&other) noexcept {
	SwapTupleDataChunkPart(*this, other);
	return *this;
}

TupleDataChunk::TupleDataChunk() : count(0), lock(make_unsafe_uniq<mutex>()) {
	parts.reserve(2);
}

static inline void SwapTupleDataChunk(TupleDataChunk &a, TupleDataChunk &b) noexcept {
	std::swap(a.parts, b.parts);
	std::swap(a.row_block_ids, b.row_block_ids);
	std::swap(a.heap_block_ids, b.heap_block_ids);
	std::swap(a.count, b.count);
	std::swap(a.lock, b.lock);
}

TupleDataChunk::TupleDataChunk(TupleDataChunk &&other) noexcept {
	SwapTupleDataChunk(*this, other);
}

TupleDataChunk &TupleDataChunk::operator=(TupleDataChunk &&other) noexcept {
	SwapTupleDataChunk(*this, other);
	return *this;
}

void TupleDataChunk::AddPart(TupleDataChunkPart &&part, const TupleDataLayout &layout) {
	count += part.count;
	row_block_ids.insert(part.row_block_index);
	if (!layout.AllConstant() && part.total_heap_size > 0) {
		heap_block_ids.insert(part.heap_block_index);
	}
	part.lock = *lock;
	parts.emplace_back(std::move(part));
}

void TupleDataChunk::Verify() const {
#ifdef DEBUG
	idx_t total_count = 0;
	for (const auto &part : parts) {
		total_count += part.count;
	}
	D_ASSERT(this->count == total_count);
	D_ASSERT(this->count <= STANDARD_VECTOR_SIZE);
#endif
}

void TupleDataChunk::MergeLastChunkPart(const TupleDataLayout &layout) {
	if (parts.size() < 2) {
		return;
	}

	auto &second_to_last = parts[parts.size() - 2];
	auto &last = parts[parts.size() - 1];

	auto rows_align =
	    last.row_block_index == second_to_last.row_block_index &&
	    last.row_block_offset == second_to_last.row_block_offset + second_to_last.count * layout.GetRowWidth();

	if (!rows_align) { // If rows don't align we can never merge
		return;
	}

	if (layout.AllConstant()) { // No heap and rows align - merge
		second_to_last.count += last.count;
		parts.pop_back();
		return;
	}

	if (last.heap_block_index == second_to_last.heap_block_index &&
	    last.heap_block_offset == second_to_last.heap_block_index + second_to_last.total_heap_size &&
	    last.base_heap_ptr == second_to_last.base_heap_ptr) { // There is a heap and it aligns - merge
		second_to_last.total_heap_size += last.total_heap_size;
		second_to_last.count += last.count;
		parts.pop_back();
	}
}

TupleDataSegment::TupleDataSegment(shared_ptr<TupleDataAllocator> allocator_p)
    : allocator(std::move(allocator_p)), count(0), data_size(0) {
}

TupleDataSegment::~TupleDataSegment() {
	lock_guard<mutex> guard(pinned_handles_lock);
	pinned_row_handles.clear();
	pinned_heap_handles.clear();
	allocator = nullptr;
}

void SwapTupleDataSegment(TupleDataSegment &a, TupleDataSegment &b) {
	std::swap(a.allocator, b.allocator);
	std::swap(a.chunks, b.chunks);
	std::swap(a.count, b.count);
	std::swap(a.data_size, b.data_size);
	std::swap(a.pinned_row_handles, b.pinned_row_handles);
	std::swap(a.pinned_heap_handles, b.pinned_heap_handles);
}

TupleDataSegment::TupleDataSegment(TupleDataSegment &&other) noexcept {
	SwapTupleDataSegment(*this, other);
}

TupleDataSegment &TupleDataSegment::operator=(TupleDataSegment &&other) noexcept {
	SwapTupleDataSegment(*this, other);
	return *this;
}

idx_t TupleDataSegment::ChunkCount() const {
	return chunks.size();
}

idx_t TupleDataSegment::SizeInBytes() const {
	return data_size;
}

void TupleDataSegment::Unpin() {
	lock_guard<mutex> guard(pinned_handles_lock);
	pinned_row_handles.clear();
	pinned_heap_handles.clear();
}

void TupleDataSegment::Verify() const {
#ifdef DEBUG
	const auto &layout = allocator->GetLayout();

	idx_t total_count = 0;
	idx_t total_size = 0;
	for (const auto &chunk : chunks) {
		chunk.Verify();
		total_count += chunk.count;

		total_size += chunk.count * layout.GetRowWidth();
		if (!layout.AllConstant()) {
			for (const auto &part : chunk.parts) {
				total_size += part.total_heap_size;
			}
		}
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

void TupleDataSegment::VerifyEverythingPinned() const {
#ifdef DEBUG
	D_ASSERT(pinned_row_handles.size() == allocator->RowBlockCount());
	D_ASSERT(pinned_heap_handles.size() == allocator->HeapBlockCount());
#endif
}

} // namespace duckdb




namespace duckdb {

SelectionData::SelectionData(idx_t count) {
	owned_data = make_unsafe_uniq_array<sel_t>(count);
#ifdef DEBUG
	for (idx_t i = 0; i < count; i++) {
		owned_data[i] = std::numeric_limits<sel_t>::max();
	}
#endif
}

// LCOV_EXCL_START
string SelectionVector::ToString(idx_t count) const {
	string result = "Selection Vector (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		if (i != 0) {
			result += ", ";
		}
		result += to_string(get_index(i));
	}
	result += "]";
	return result;
}

void SelectionVector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}
// LCOV_EXCL_STOP

buffer_ptr<SelectionData> SelectionVector::Slice(const SelectionVector &sel, idx_t count) const {
	auto data = make_buffer<SelectionData>(count);
	auto result_ptr = data->owned_data.get();
	// for every element, we perform result[i] = target[new[i]]
	for (idx_t i = 0; i < count; i++) {
		auto new_idx = sel.get_index(i);
		auto idx = this->get_index(new_idx);
		result_ptr[i] = idx;
	}
	return data;
}

} // namespace duckdb







#include <cstring>

namespace duckdb {

StringHeap::StringHeap(Allocator &allocator) : allocator(allocator) {
}

void StringHeap::Destroy() {
	allocator.Destroy();
}

void StringHeap::Move(StringHeap &other) {
	other.allocator.Move(allocator);
}

string_t StringHeap::AddString(const char *data, idx_t len) {
	D_ASSERT(Utf8Proc::Analyze(data, len) != UnicodeType::INVALID);
	return AddBlob(data, len);
}

string_t StringHeap::AddString(const char *data) {
	return AddString(data, strlen(data));
}

string_t StringHeap::AddString(const string &data) {
	return AddString(data.c_str(), data.size());
}

string_t StringHeap::AddString(const string_t &data) {
	return AddString(data.GetData(), data.GetSize());
}

string_t StringHeap::AddBlob(const char *data, idx_t len) {
	auto insert_string = EmptyString(len);
	auto insert_pos = insert_string.GetDataWriteable();
	memcpy(insert_pos, data, len);
	insert_string.Finalize();
	return insert_string;
}

string_t StringHeap::AddBlob(const string_t &data) {
	return AddBlob(data.GetData(), data.GetSize());
}

string_t StringHeap::EmptyString(idx_t len) {
	D_ASSERT(len > string_t::INLINE_LENGTH);
	auto insert_pos = const_char_ptr_cast(allocator.Allocate(len));
	return string_t(insert_pos, len);
}

idx_t StringHeap::SizeInBytes() const {
	return allocator.SizeInBytes();
}

} // namespace duckdb






namespace duckdb {

void string_t::Verify() const {
	auto dataptr = GetData();
	(void)dataptr;
	D_ASSERT(dataptr);

#ifdef DEBUG
	auto utf_type = Utf8Proc::Analyze(dataptr, GetSize());
	D_ASSERT(utf_type != UnicodeType::INVALID);
#endif

	// verify that the prefix contains the first four characters of the string
	for (idx_t i = 0; i < MinValue<uint32_t>(PREFIX_LENGTH, GetSize()); i++) {
		D_ASSERT(GetPrefix()[i] == dataptr[i]);
	}
	// verify that for strings with length <= INLINE_LENGTH, the rest of the string is zero
	for (idx_t i = GetSize(); i < INLINE_LENGTH; i++) {
		D_ASSERT(GetData()[i] == '\0');
	}
}

} // namespace duckdb










#include <cctype>
#include <cstring>
#include <sstream>

namespace duckdb {

static_assert(sizeof(dtime_t) == sizeof(int64_t), "dtime_t was padded");

// string format is hh:mm:ss.microsecondsZ
// microseconds and Z are optional
// ISO 8601

bool Time::TryConvertInternal(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	int32_t hour = -1, min = -1, sec = -1, micros = -1;
	pos = 0;

	if (len == 0) {
		return false;
	}

	int sep;

	// skip leading spaces
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	if (pos >= len) {
		return false;
	}

	if (!StringUtil::CharacterIsDigit(buf[pos])) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, hour)) {
		return false;
	}
	if (hour < 0 || hour >= 24) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	// fetch the separator
	sep = buf[pos++];
	if (sep != ':') {
		// invalid separator
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, min)) {
		return false;
	}
	if (min < 0 || min >= 60) {
		return false;
	}

	if (pos >= len) {
		return false;
	}

	if (buf[pos++] != sep) {
		return false;
	}

	if (!Date::ParseDoubleDigit(buf, len, pos, sec)) {
		return false;
	}
	if (sec < 0 || sec >= 60) {
		return false;
	}

	micros = 0;
	if (pos < len && buf[pos] == '.') {
		pos++;
		// we expect some microseconds
		int32_t mult = 100000;
		for (; pos < len && StringUtil::CharacterIsDigit(buf[pos]); pos++, mult /= 10) {
			if (mult > 0) {
				micros += (buf[pos] - '0') * mult;
			}
		}
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	}

	result = Time::FromTime(hour, min, sec, micros);
	return true;
}

bool Time::TryConvertTime(const char *buf, idx_t len, idx_t &pos, dtime_t &result, bool strict) {
	if (!Time::TryConvertInternal(buf, len, pos, result, strict)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = Timestamp::GetTime(timestamp);
				return true;
			}
		}
		return false;
	}
	return true;
}

bool Time::TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int32_t &offset) {
	offset = 0;
	if (pos == len || StringUtil::CharacterIsSpace(str[pos])) {
		return true;
	}

	idx_t curpos = pos;
	// Minimum of 3 characters
	if (curpos + 3 > len) {
		// no characters left to parse
		return false;
	}

	const auto sign_char = str[curpos];
	if (sign_char != '+' && sign_char != '-') {
		// expected either + or -
		return false;
	}
	curpos++;

	int32_t hh = 0;
	idx_t start = curpos;
	for (; curpos < len; ++curpos) {
		const auto c = str[curpos];
		if (!StringUtil::CharacterIsDigit(c)) {
			break;
		}
		hh = hh * 10 + (c - '0');
	}
	//	HH is in [-1559,+1559] and must be at least two digits
	if (curpos - start < 2 || hh > 1559) {
		return false;
	}

	// optional minute specifier: expected ":MM"
	int32_t mm = 0;
	if (curpos + 3 <= len && str[curpos] == ':') {
		++curpos;
		if (!Date::ParseDoubleDigit(str, len, curpos, mm) || mm >= Interval::MINS_PER_HOUR) {
			return false;
		}
	}

	// optional seconds specifier: expected ":SS"
	int32_t ss = 0;
	if (curpos + 3 <= len && str[curpos] == ':') {
		++curpos;
		if (!Date::ParseDoubleDigit(str, len, curpos, ss) || ss >= Interval::SECS_PER_MINUTE) {
			return false;
		}
	}

	//	Assemble the offset now that we know nothing went wrong
	offset += hh * Interval::SECS_PER_HOUR;
	offset += mm * Interval::SECS_PER_MINUTE;
	offset += ss;
	if (sign_char == '-') {
		offset = -offset;
	}

	pos = curpos;

	return true;
}

bool Time::TryConvertTimeTZ(const char *buf, idx_t len, idx_t &pos, dtime_tz_t &result, bool strict) {
	dtime_t time_part;
	if (!Time::TryConvertInternal(buf, len, pos, time_part, false)) {
		if (!strict) {
			// last chance, check if we can parse as timestamp
			timestamp_t timestamp;
			if (Timestamp::TryConvertTimestamp(buf, len, timestamp) == TimestampCastResult::SUCCESS) {
				if (!Timestamp::IsFinite(timestamp)) {
					return false;
				}
				result = dtime_tz_t(Timestamp::GetTime(timestamp), 0);
				return true;
			}
		}
		return false;
	}

	//	We can't use Timestamp::TryParseUTCOffset because the colon is optional there but required here.
	int32_t offset = 0;
	if (!TryParseUTCOffset(buf, pos, len, offset)) {
		return false;
	}

	// in strict mode, check remaining string for non-space characters
	if (strict) {
		// skip trailing spaces
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		// check position. if end was not reached, non-space chars remaining
		if (pos < len) {
			return false;
		}
	}

	result = dtime_tz_t(time_part, offset);

	return true;
}

string Time::ConversionError(const string &str) {
	return StringUtil::Format("time field value out of range: \"%s\", "
	                          "expected format is ([YYYY-MM-DD ]HH:MM:SS[.MS])",
	                          str);
}

string Time::ConversionError(string_t str) {
	return Time::ConversionError(str.GetString());
}

dtime_t Time::FromCString(const char *buf, idx_t len, bool strict) {
	dtime_t result;
	idx_t pos;
	if (!Time::TryConvertTime(buf, len, pos, result, strict)) {
		throw ConversionException(ConversionError(string(buf, len)));
	}
	return result;
}

dtime_t Time::FromString(const string &str, bool strict) {
	return Time::FromCString(str.c_str(), str.size(), strict);
}

string Time::ToString(dtime_t time) {
	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	char micro_buffer[6];
	auto length = TimeToStringCast::Length(time_units, micro_buffer);
	auto buffer = make_unsafe_uniq_array<char>(length);
	TimeToStringCast::Format(buffer.get(), length, time_units, micro_buffer);
	return string(buffer.get(), length);
}

string Time::ToUTCOffset(int hour_offset, int minute_offset) {
	dtime_t time((hour_offset * Interval::MINS_PER_HOUR + minute_offset) * Interval::MICROS_PER_MINUTE);

	char buffer[1 + 2 + 1 + 2];
	idx_t length = 0;
	buffer[length++] = (time.micros < 0 ? '-' : '+');
	time.micros = std::abs(time.micros);

	int32_t time_units[4];
	Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);

	TimeToStringCast::FormatTwoDigits(buffer + length, time_units[0]);
	length += 2;
	if (time_units[1]) {
		buffer[length++] = ':';
		TimeToStringCast::FormatTwoDigits(buffer + length, time_units[1]);
		length += 2;
	}

	return string(buffer, length);
}

dtime_t Time::FromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	int64_t result;
	result = hour;                                             // hours
	result = result * Interval::MINS_PER_HOUR + minute;        // hours -> minutes
	result = result * Interval::SECS_PER_MINUTE + second;      // minutes -> seconds
	result = result * Interval::MICROS_PER_SEC + microseconds; // seconds -> microseconds
	return dtime_t(result);
}

bool Time::IsValidTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
	if (hour < 0 || hour >= 24) {
		return false;
	}
	if (minute < 0 || minute >= 60) {
		return false;
	}
	if (second < 0 || second > 60) {
		return false;
	}
	if (microseconds < 0 || microseconds > 1000000) {
		return false;
	}
	return true;
}

void Time::Convert(dtime_t dtime, int32_t &hour, int32_t &min, int32_t &sec, int32_t &micros) {
	int64_t time = dtime.micros;
	hour = int32_t(time / Interval::MICROS_PER_HOUR);
	time -= int64_t(hour) * Interval::MICROS_PER_HOUR;
	min = int32_t(time / Interval::MICROS_PER_MINUTE);
	time -= int64_t(min) * Interval::MICROS_PER_MINUTE;
	sec = int32_t(time / Interval::MICROS_PER_SEC);
	time -= int64_t(sec) * Interval::MICROS_PER_SEC;
	micros = int32_t(time);
	D_ASSERT(Time::IsValidTime(hour, min, sec, micros));
}

dtime_t Time::FromTimeMs(int64_t time_ms) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(time_ms, Interval::MICROS_PER_MSEC, result)) {
		throw ConversionException("Could not convert Time(MS) to Time(US)");
	}
	return dtime_t(result);
}

dtime_t Time::FromTimeNs(int64_t time_ns) {
	return dtime_t(time_ns / Interval::NANOS_PER_MICRO);
}

} // namespace duckdb

#endif
