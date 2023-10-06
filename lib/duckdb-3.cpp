#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif












#include <ctime>

namespace duckdb {

static_assert(sizeof(timestamp_t) == sizeof(int64_t), "timestamp_t was padded");

// timestamp/datetime uses 64 bits, high 32 bits for date and low 32 bits for time
// string format is YYYY-MM-DDThh:mm:ssZ
// T may be a space
// Z is optional
// ISO 8601

// arithmetic operators
timestamp_t timestamp_t::operator+(const double &value) const {
	timestamp_t result;
	if (!TryAddOperator::Operation(this->value, int64_t(value), result.value)) {
		throw OutOfRangeException("Overflow in timestamp addition");
	}
	return result;
}

int64_t timestamp_t::operator-(const timestamp_t &other) const {
	int64_t result;
	if (!TrySubtractOperator::Operation(value, int64_t(other.value), result)) {
		throw OutOfRangeException("Overflow in timestamp subtraction");
	}
	return result;
}

// in-place operators
timestamp_t &timestamp_t::operator+=(const int64_t &delta) {
	if (!TryAddOperator::Operation(value, delta, value)) {
		throw OutOfRangeException("Overflow in timestamp increment");
	}
	return *this;
}

timestamp_t &timestamp_t::operator-=(const int64_t &delta) {
	if (!TrySubtractOperator::Operation(value, delta, value)) {
		throw OutOfRangeException("Overflow in timestamp decrement");
	}
	return *this;
}

bool Timestamp::TryConvertTimestampTZ(const char *str, idx_t len, timestamp_t &result, bool &has_offset, string_t &tz) {
	idx_t pos;
	date_t date;
	dtime_t time;
	has_offset = false;
	if (!Date::TryConvertDate(str, len, pos, date, has_offset)) {
		return false;
	}
	if (pos == len) {
		// no time: only a date or special
		if (date == date_t::infinity()) {
			result = timestamp_t::infinity();
			return true;
		} else if (date == date_t::ninfinity()) {
			result = timestamp_t::ninfinity();
			return true;
		}
		return Timestamp::TryFromDatetime(date, dtime_t(0), result);
	}
	// try to parse a time field
	if (str[pos] == ' ' || str[pos] == 'T') {
		pos++;
	}
	idx_t time_pos = 0;
	if (!Time::TryConvertTime(str + pos, len - pos, time_pos, time)) {
		return false;
	}
	pos += time_pos;
	if (!Timestamp::TryFromDatetime(date, time, result)) {
		return false;
	}
	if (pos < len) {
		// skip a "Z" at the end (as per the ISO8601 specs)
		int hour_offset, minute_offset;
		if (str[pos] == 'Z') {
			pos++;
			has_offset = true;
		} else if (Timestamp::TryParseUTCOffset(str, pos, len, hour_offset, minute_offset)) {
			const int64_t delta = hour_offset * Interval::MICROS_PER_HOUR + minute_offset * Interval::MICROS_PER_MINUTE;
			if (!TrySubtractOperator::Operation(result.value, delta, result.value)) {
				return false;
			}
			has_offset = true;
		} else {
			// Parse a time zone: / [A-Za-z0-9/_]+/
			if (str[pos++] != ' ') {
				return false;
			}
			auto tz_name = str + pos;
			for (; pos < len && CharacterIsTimeZone(str[pos]); ++pos) {
				continue;
			}
			auto tz_len = str + pos - tz_name;
			if (tz_len) {
				tz = string_t(tz_name, tz_len);
			}
			// Note that the caller must reinterpret the instant we return to the given time zone
		}

		// skip any spaces at the end
		while (pos < len && StringUtil::CharacterIsSpace(str[pos])) {
			pos++;
		}
		if (pos < len) {
			return false;
		}
	}
	return true;
}

TimestampCastResult Timestamp::TryConvertTimestamp(const char *str, idx_t len, timestamp_t &result) {
	string_t tz(nullptr, 0);
	bool has_offset = false;
	// We don't understand TZ without an extension, so fail if one was provided.
	auto success = TryConvertTimestampTZ(str, len, result, has_offset, tz);
	if (!success) {
		return TimestampCastResult::ERROR_INCORRECT_FORMAT;
	}
	if (tz.GetSize() == 0) {
		// no timezone provided - success!
		return TimestampCastResult::SUCCESS;
	}
	if (tz.GetSize() == 3) {
		// we can ONLY handle UTC without ICU being loaded
		auto tz_ptr = tz.GetData();
		if ((tz_ptr[0] == 'u' || tz_ptr[0] == 'U') && (tz_ptr[1] == 't' || tz_ptr[1] == 'T') &&
		    (tz_ptr[2] == 'c' || tz_ptr[2] == 'C')) {
			return TimestampCastResult::SUCCESS;
		}
	}
	return TimestampCastResult::ERROR_NON_UTC_TIMEZONE;
}

string Timestamp::ConversionError(const string &str) {
	return StringUtil::Format("timestamp field value out of range: \"%s\", "
	                          "expected format is (YYYY-MM-DD HH:MM:SS[.US][±HH:MM| ZONE])",
	                          str);
}

string Timestamp::UnsupportedTimezoneError(const string &str) {
	return StringUtil::Format("timestamp field value \"%s\" has a timestamp that is not UTC.\nUse the TIMESTAMPTZ type "
	                          "with the ICU extension loaded to handle non-UTC timestamps.",
	                          str);
}

string Timestamp::ConversionError(string_t str) {
	return Timestamp::ConversionError(str.GetString());
}

string Timestamp::UnsupportedTimezoneError(string_t str) {
	return Timestamp::UnsupportedTimezoneError(str.GetString());
}

timestamp_t Timestamp::FromCString(const char *str, idx_t len) {
	timestamp_t result;
	auto cast_result = Timestamp::TryConvertTimestamp(str, len, result);
	if (cast_result == TimestampCastResult::SUCCESS) {
		return result;
	}
	if (cast_result == TimestampCastResult::ERROR_NON_UTC_TIMEZONE) {
		throw ConversionException(Timestamp::UnsupportedTimezoneError(string(str, len)));
	} else {
		throw ConversionException(Timestamp::ConversionError(string(str, len)));
	}
}

bool Timestamp::TryParseUTCOffset(const char *str, idx_t &pos, idx_t len, int &hour_offset, int &minute_offset) {
	minute_offset = 0;
	idx_t curpos = pos;
	// parse the next 3 characters
	if (curpos + 3 > len) {
		// no characters left to parse
		return false;
	}
	char sign_char = str[curpos];
	if (sign_char != '+' && sign_char != '-') {
		// expected either + or -
		return false;
	}
	curpos++;
	if (!StringUtil::CharacterIsDigit(str[curpos]) || !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// expected +HH or -HH
		return false;
	}
	hour_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		hour_offset = -hour_offset;
	}
	curpos += 2;

	// optional minute specifier: expected either "MM" or ":MM"
	if (curpos >= len) {
		// done, nothing left
		pos = curpos;
		return true;
	}
	if (str[curpos] == ':') {
		curpos++;
	}
	if (curpos + 2 > len || !StringUtil::CharacterIsDigit(str[curpos]) ||
	    !StringUtil::CharacterIsDigit(str[curpos + 1])) {
		// no MM specifier
		pos = curpos;
		return true;
	}
	// we have an MM specifier: parse it
	minute_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
	if (sign_char == '-') {
		minute_offset = -minute_offset;
	}
	pos = curpos + 2;
	return true;
}

timestamp_t Timestamp::FromString(const string &str) {
	return Timestamp::FromCString(str.c_str(), str.size());
}

string Timestamp::ToString(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return Date::PINF;
	} else if (timestamp == timestamp_t::ninfinity()) {
		return Date::NINF;
	}
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp, date, time);
	return Date::ToString(date) + " " + Time::ToString(time);
}

date_t Timestamp::GetDate(timestamp_t timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return date_t::infinity();
	} else if (timestamp == timestamp_t::ninfinity()) {
		return date_t::ninfinity();
	}
	return date_t((timestamp.value + (timestamp.value < 0)) / Interval::MICROS_PER_DAY - (timestamp.value < 0));
}

dtime_t Timestamp::GetTime(timestamp_t timestamp) {
	if (!IsFinite(timestamp)) {
		throw ConversionException("Can't get TIME of infinite TIMESTAMP");
	}
	date_t date = Timestamp::GetDate(timestamp);
	return dtime_t(timestamp.value - (int64_t(date.days) * int64_t(Interval::MICROS_PER_DAY)));
}

bool Timestamp::TryFromDatetime(date_t date, dtime_t time, timestamp_t &result) {
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(date.days, Interval::MICROS_PER_DAY, result.value)) {
		return false;
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(result.value, time.micros, result.value)) {
		return false;
	}
	return Timestamp::IsFinite(result);
}

timestamp_t Timestamp::FromDatetime(date_t date, dtime_t time) {
	timestamp_t result;
	if (!TryFromDatetime(date, time, result)) {
		throw Exception("Overflow exception in date/time -> timestamp conversion");
	}
	return result;
}

void Timestamp::Convert(timestamp_t timestamp, date_t &out_date, dtime_t &out_time) {
	out_date = GetDate(timestamp);
	int64_t days_micros;
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(out_date.days, Interval::MICROS_PER_DAY,
	                                                               days_micros)) {
		throw ConversionException("Date out of range in timestamp conversion");
	}
	out_time = dtime_t(timestamp.value - days_micros);
	D_ASSERT(timestamp == Timestamp::FromDatetime(out_date, out_time));
}

timestamp_t Timestamp::GetCurrentTimestamp() {
	auto now = system_clock::now();
	auto epoch_ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
	return Timestamp::FromEpochMs(epoch_ms);
}

timestamp_t Timestamp::FromEpochSeconds(int64_t sec) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(sec, Interval::MICROS_PER_SEC, result)) {
		throw ConversionException("Could not convert Timestamp(S) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMs(int64_t ms) {
	int64_t result;
	if (!TryMultiplyOperator::Operation(ms, Interval::MICROS_PER_MSEC, result)) {
		throw ConversionException("Could not convert Timestamp(MS) to Timestamp(US)");
	}
	return timestamp_t(result);
}

timestamp_t Timestamp::FromEpochMicroSeconds(int64_t micros) {
	return timestamp_t(micros);
}

timestamp_t Timestamp::FromEpochNanoSeconds(int64_t ns) {
	return timestamp_t(ns / 1000);
}

int64_t Timestamp::GetEpochSeconds(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_SEC;
}

int64_t Timestamp::GetEpochMs(timestamp_t timestamp) {
	return timestamp.value / Interval::MICROS_PER_MSEC;
}

int64_t Timestamp::GetEpochMicroSeconds(timestamp_t timestamp) {
	return timestamp.value;
}

int64_t Timestamp::GetEpochNanoSeconds(timestamp_t timestamp) {
	int64_t result;
	int64_t ns_in_us = 1000;
	if (!TryMultiplyOperator::Operation(timestamp.value, ns_in_us, result)) {
		throw ConversionException("Could not convert Timestamp(US) to Timestamp(NS)");
	}
	return result;
}

double Timestamp::GetJulianDay(timestamp_t timestamp) {
	double result = Timestamp::GetTime(timestamp).micros;
	result /= Interval::MICROS_PER_DAY;
	result += Date::ExtractJulianDay(Timestamp::GetDate(timestamp));
	return result;
}

} // namespace duckdb



namespace duckdb {

bool UUID::FromString(string str, hugeint_t &result) {
	auto hex2char = [](char ch) -> unsigned char {
		if (ch >= '0' && ch <= '9') {
			return ch - '0';
		}
		if (ch >= 'a' && ch <= 'f') {
			return 10 + ch - 'a';
		}
		if (ch >= 'A' && ch <= 'F') {
			return 10 + ch - 'A';
		}
		return 0;
	};
	auto is_hex = [](char ch) -> bool {
		return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
	};

	if (str.empty()) {
		return false;
	}
	int has_braces = 0;
	if (str.front() == '{') {
		has_braces = 1;
	}
	if (has_braces && str.back() != '}') {
		return false;
	}

	result.lower = 0;
	result.upper = 0;
	size_t count = 0;
	for (size_t i = has_braces; i < str.size() - has_braces; ++i) {
		if (str[i] == '-') {
			continue;
		}
		if (count >= 32 || !is_hex(str[i])) {
			return false;
		}
		if (count >= 16) {
			result.lower = (result.lower << 4) | hex2char(str[i]);
		} else {
			result.upper = (result.upper << 4) | hex2char(str[i]);
		}
		count++;
	}
	// Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
	result.upper ^= (uint64_t(1) << 63);
	return count == 32;
}

void UUID::ToString(hugeint_t input, char *buf) {
	auto byte_to_hex = [](char byte_val, char *buf, idx_t &pos) {
		static char const HEX_DIGITS[] = "0123456789abcdef";
		buf[pos++] = HEX_DIGITS[(byte_val >> 4) & 0xf];
		buf[pos++] = HEX_DIGITS[byte_val & 0xf];
	};

	// Flip back before convert to string
	int64_t upper = input.upper ^ (uint64_t(1) << 63);
	idx_t pos = 0;
	byte_to_hex(upper >> 56 & 0xFF, buf, pos);
	byte_to_hex(upper >> 48 & 0xFF, buf, pos);
	byte_to_hex(upper >> 40 & 0xFF, buf, pos);
	byte_to_hex(upper >> 32 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 24 & 0xFF, buf, pos);
	byte_to_hex(upper >> 16 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(upper >> 8 & 0xFF, buf, pos);
	byte_to_hex(upper & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 56 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 48 & 0xFF, buf, pos);
	buf[pos++] = '-';
	byte_to_hex(input.lower >> 40 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 32 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 24 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 16 & 0xFF, buf, pos);
	byte_to_hex(input.lower >> 8 & 0xFF, buf, pos);
	byte_to_hex(input.lower & 0xFF, buf, pos);
}

hugeint_t UUID::GenerateRandomUUID(RandomEngine &engine) {
	uint8_t bytes[16];
	for (int i = 0; i < 16; i += 4) {
		*reinterpret_cast<uint32_t *>(bytes + i) = engine.NextRandomInteger();
	}
	// variant must be 10xxxxxx
	bytes[8] &= 0xBF;
	bytes[8] |= 0x80;
	// version must be 0100xxxx
	bytes[6] &= 0x4F;
	bytes[6] |= 0x40;

	hugeint_t result;
	result.upper = 0;
	result.upper |= ((int64_t)bytes[0] << 56);
	result.upper |= ((int64_t)bytes[1] << 48);
	result.upper |= ((int64_t)bytes[2] << 40);
	result.upper |= ((int64_t)bytes[3] << 32);
	result.upper |= ((int64_t)bytes[4] << 24);
	result.upper |= ((int64_t)bytes[5] << 16);
	result.upper |= ((int64_t)bytes[6] << 8);
	result.upper |= bytes[7];
	result.lower = 0;
	result.lower |= ((uint64_t)bytes[8] << 56);
	result.lower |= ((uint64_t)bytes[9] << 48);
	result.lower |= ((uint64_t)bytes[10] << 40);
	result.lower |= ((uint64_t)bytes[11] << 32);
	result.lower |= ((uint64_t)bytes[12] << 24);
	result.lower |= ((uint64_t)bytes[13] << 16);
	result.lower |= ((uint64_t)bytes[14] << 8);
	result.lower |= bytes[15];
	return result;
}

hugeint_t UUID::GenerateRandomUUID() {
	RandomEngine engine;
	return GenerateRandomUUID(engine);
}

} // namespace duckdb





namespace duckdb {

ValidityData::ValidityData(idx_t count) : TemplatedValidityData(count) {
}
ValidityData::ValidityData(const ValidityMask &original, idx_t count)
    : TemplatedValidityData(original.GetData(), count) {
}

void ValidityMask::Combine(const ValidityMask &other, idx_t count) {
	if (other.AllValid()) {
		// X & 1 = X
		return;
	}
	if (AllValid()) {
		// 1 & Y = Y
		Initialize(other);
		return;
	}
	if (validity_mask == other.validity_mask) {
		// X & X == X
		return;
	}
	// have to merge
	// create a new validity mask that contains the combined mask
	auto owned_data = std::move(validity_data);
	auto data = GetData();
	auto other_data = other.GetData();

	Initialize(count);
	auto result_data = GetData();

	auto entry_count = ValidityData::EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
		result_data[entry_idx] = data[entry_idx] & other_data[entry_idx];
	}
}

// LCOV_EXCL_START
string ValidityMask::ToString(idx_t count) const {
	string result = "Validity Mask (" + to_string(count) + ") [";
	for (idx_t i = 0; i < count; i++) {
		result += RowIsValid(i) ? "." : "X";
	}
	result += "]";
	return result;
}
// LCOV_EXCL_STOP

void ValidityMask::Resize(idx_t old_size, idx_t new_size) {
	D_ASSERT(new_size >= old_size);
	if (validity_mask) {
		auto new_size_count = EntryCount(new_size);
		auto old_size_count = EntryCount(old_size);
		auto new_validity_data = make_buffer<ValidityBuffer>(new_size);
		auto new_owned_data = new_validity_data->owned_data.get();
		for (idx_t entry_idx = 0; entry_idx < old_size_count; entry_idx++) {
			new_owned_data[entry_idx] = validity_mask[entry_idx];
		}
		for (idx_t entry_idx = old_size_count; entry_idx < new_size_count; entry_idx++) {
			new_owned_data[entry_idx] = ValidityData::MAX_ENTRY;
		}
		validity_data = std::move(new_validity_data);
		validity_mask = validity_data->owned_data.get();
	} else {
		Initialize(new_size);
	}
}

void ValidityMask::Slice(const ValidityMask &other, idx_t source_offset, idx_t count) {
	if (other.AllValid()) {
		validity_mask = nullptr;
		validity_data.reset();
		return;
	}
	if (source_offset == 0) {
		Initialize(other);
		return;
	}
	ValidityMask new_mask(count);
	new_mask.SliceInPlace(other, 0, source_offset, count);
	Initialize(new_mask);
}

bool ValidityMask::IsAligned(idx_t count) {
	return count % BITS_PER_VALUE == 0;
}

void ValidityMask::SliceInPlace(const ValidityMask &other, idx_t target_offset, idx_t source_offset, idx_t count) {
	if (IsAligned(source_offset) && IsAligned(target_offset)) {
		auto target_validity = GetData();
		auto source_validity = other.GetData();
		auto source_offset_entries = EntryCount(source_offset);
		auto target_offset_entries = EntryCount(target_offset);
		memcpy(target_validity + target_offset_entries, source_validity + source_offset_entries,
		       sizeof(validity_t) * EntryCount(count));
		return;
	} else if (IsAligned(target_offset)) {
		//	Simple common case where we are shifting into an aligned mask (e.g., 0 in Slice above)
		const idx_t entire_units = count / BITS_PER_VALUE;
		const idx_t ragged = count % BITS_PER_VALUE;
		const idx_t tail = source_offset % BITS_PER_VALUE;
		const idx_t head = BITS_PER_VALUE - tail;
		auto source_validity = other.GetData() + (source_offset / BITS_PER_VALUE);
		auto target_validity = this->GetData() + (target_offset / BITS_PER_VALUE);
		auto src_entry = *source_validity++;
		for (idx_t i = 0; i < entire_units; ++i) {
			//	Start with head of previous src
			validity_t tgt_entry = src_entry >> tail;
			src_entry = *source_validity++;
			// 	Add in tail of current src
			tgt_entry |= (src_entry << head);
			*target_validity++ = tgt_entry;
		}
		//	Finish last ragged entry
		if (ragged) {
			//	Start with head of previous src
			validity_t tgt_entry = (src_entry >> tail);
			//  Add in the tail of the next src, if head was too small
			if (head < ragged) {
				src_entry = *source_validity++;
				tgt_entry |= (src_entry << head);
			}
			//  Mask off the bits that go past the ragged end
			tgt_entry &= (ValidityBuffer::MAX_ENTRY >> (BITS_PER_VALUE - ragged));
			//	Restore the ragged end of the target
			tgt_entry |= *target_validity & (ValidityBuffer::MAX_ENTRY << ragged);
			*target_validity++ = tgt_entry;
		}
		return;
	}

	// FIXME: use bitwise operations here
#if 1
	for (idx_t i = 0; i < count; i++) {
		Set(target_offset + i, other.RowIsValid(source_offset + i));
	}
#else
	// first shift the "whole" units
	idx_t entire_units = offset / BITS_PER_VALUE;
	idx_t sub_units = offset - entire_units * BITS_PER_VALUE;
	if (entire_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + entire_units < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] = other.validity_mask[validity_idx + entire_units];
		}
	}
	// now we shift the remaining sub units
	// this gets a bit more complicated because we have to shift over the borders of the entries
	// e.g. suppose we have 2 entries of length 4 and we left-shift by two
	// 0101|1010
	// a regular left-shift of both gets us:
	// 0100|1000
	// we then OR the overflow (right-shifted by BITS_PER_VALUE - offset) together to get the correct result
	// 0100|1000 ->
	// 0110|1000
	if (sub_units > 0) {
		idx_t validity_idx;
		for (validity_idx = 0; validity_idx + 1 < STANDARD_ENTRY_COUNT; validity_idx++) {
			new_mask.validity_mask[validity_idx] =
			    (other.validity_mask[validity_idx] >> sub_units) |
			    (other.validity_mask[validity_idx + 1] << (BITS_PER_VALUE - sub_units));
		}
		new_mask.validity_mask[validity_idx] >>= sub_units;
	}
#ifdef DEBUG
	for (idx_t i = offset; i < STANDARD_VECTOR_SIZE; i++) {
		D_ASSERT(new_mask.RowIsValid(i - offset) == other.RowIsValid(i));
	}
	Initialize(new_mask);
#endif
#endif
}

enum class ValiditySerialization : uint8_t { BITMASK = 0, VALID_VALUES = 1, INVALID_VALUES = 2 };

void ValidityMask::Write(WriteStream &writer, idx_t count) {
	auto valid_values = CountValid(count);
	auto invalid_values = count - valid_values;
	auto bitmask_bytes = ValidityMask::ValidityMaskSize(count);
	auto need_u32 = count >= NumericLimits<uint16_t>::Maximum();
	auto bytes_per_value = need_u32 ? sizeof(uint32_t) : sizeof(uint16_t);
	auto valid_value_size = bytes_per_value * valid_values + sizeof(uint32_t);
	auto invalid_value_size = bytes_per_value * invalid_values + sizeof(uint32_t);
	if (valid_value_size < bitmask_bytes || invalid_value_size < bitmask_bytes) {
		auto serialize_valid = valid_value_size < invalid_value_size;
		// serialize (in)valid value indexes as [COUNT][V0][V1][...][VN]
		auto flag = serialize_valid ? ValiditySerialization::VALID_VALUES : ValiditySerialization::INVALID_VALUES;
		writer.Write(flag);
		writer.Write<uint32_t>(MinValue<uint32_t>(valid_values, invalid_values));
		for (idx_t i = 0; i < count; i++) {
			if (RowIsValid(i) == serialize_valid) {
				if (need_u32) {
					writer.Write<uint32_t>(i);
				} else {
					writer.Write<uint16_t>(i);
				}
			}
		}
	} else {
		// serialize the entire bitmask
		writer.Write(ValiditySerialization::BITMASK);
		writer.WriteData(const_data_ptr_cast(GetData()), bitmask_bytes);
	}
}

void ValidityMask::Read(ReadStream &reader, idx_t count) {
	Initialize(count);
	// deserialize the storage type
	auto flag = reader.Read<ValiditySerialization>();
	if (flag == ValiditySerialization::BITMASK) {
		// deserialize the bitmask
		reader.ReadData(data_ptr_cast(GetData()), ValidityMask::ValidityMaskSize(count));
		return;
	}
	auto is_u32 = count >= NumericLimits<uint16_t>::Maximum();
	auto is_valid = flag == ValiditySerialization::VALID_VALUES;
	auto serialize_count = reader.Read<uint32_t>();
	if (is_valid) {
		SetAllInvalid(count);
	}
	for (idx_t i = 0; i < serialize_count; i++) {
		idx_t index = is_u32 ? reader.Read<uint32_t>() : reader.Read<uint16_t>();
		Set(index, is_valid);
	}
}

} // namespace duckdb


































#include <utility>
#include <cmath>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extra Value Info
//===--------------------------------------------------------------------===//
enum class ExtraValueInfoType : uint8_t { INVALID_TYPE_INFO = 0, STRING_VALUE_INFO = 1, NESTED_VALUE_INFO = 2 };

struct ExtraValueInfo {
	explicit ExtraValueInfo(ExtraValueInfoType type) : type(type) {
	}
	virtual ~ExtraValueInfo() {
	}

	ExtraValueInfoType type;

public:
	bool Equals(ExtraValueInfo *other_p) const {
		if (!other_p) {
			return false;
		}
		if (type != other_p->type) {
			return false;
		}
		return EqualsInternal(other_p);
	}

	template <class T>
	T &Get() {
		if (type != T::TYPE) {
			throw InternalException("ExtraValueInfo type mismatch");
		}
		return (T &)*this;
	}

protected:
	virtual bool EqualsInternal(ExtraValueInfo *other_p) const {
		return true;
	}
};

//===--------------------------------------------------------------------===//
// String Value Info
//===--------------------------------------------------------------------===//
struct StringValueInfo : public ExtraValueInfo {
	static constexpr const ExtraValueInfoType TYPE = ExtraValueInfoType::STRING_VALUE_INFO;

public:
	explicit StringValueInfo(string str_p)
	    : ExtraValueInfo(ExtraValueInfoType::STRING_VALUE_INFO), str(std::move(str_p)) {
	}

	const string &GetString() {
		return str;
	}

protected:
	bool EqualsInternal(ExtraValueInfo *other_p) const override {
		return other_p->Get<StringValueInfo>().str == str;
	}

	string str;
};

//===--------------------------------------------------------------------===//
// Nested Value Info
//===--------------------------------------------------------------------===//
struct NestedValueInfo : public ExtraValueInfo {
	static constexpr const ExtraValueInfoType TYPE = ExtraValueInfoType::NESTED_VALUE_INFO;

public:
	NestedValueInfo() : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO) {
	}
	explicit NestedValueInfo(vector<Value> values_p)
	    : ExtraValueInfo(ExtraValueInfoType::NESTED_VALUE_INFO), values(std::move(values_p)) {
	}

	const vector<Value> &GetValues() {
		return values;
	}

protected:
	bool EqualsInternal(ExtraValueInfo *other_p) const override {
		return other_p->Get<NestedValueInfo>().values == values;
	}

	vector<Value> values;
};
//===--------------------------------------------------------------------===//
// Value
//===--------------------------------------------------------------------===//
Value::Value(LogicalType type) : type_(std::move(type)), is_null(true) {
}

Value::Value(int32_t val) : type_(LogicalType::INTEGER), is_null(false) {
	value_.integer = val;
}

Value::Value(int64_t val) : type_(LogicalType::BIGINT), is_null(false) {
	value_.bigint = val;
}

Value::Value(float val) : type_(LogicalType::FLOAT), is_null(false) {
	value_.float_ = val;
}

Value::Value(double val) : type_(LogicalType::DOUBLE), is_null(false) {
	value_.double_ = val;
}

Value::Value(const char *val) : Value(val ? string(val) : string()) {
}

Value::Value(std::nullptr_t val) : Value(LogicalType::VARCHAR) {
}

Value::Value(string_t val) : Value(val.GetString()) {
}

Value::Value(string val) : type_(LogicalType::VARCHAR), is_null(false) {
	if (!Value::StringIsValid(val.c_str(), val.size())) {
		throw Exception(ErrorManager::InvalidUnicodeError(val, "value construction"));
	}
	value_info_ = make_shared<StringValueInfo>(std::move(val));
}

Value::~Value() {
}

Value::Value(const Value &other)
    : type_(other.type_), is_null(other.is_null), value_(other.value_), value_info_(other.value_info_) {
}

Value::Value(Value &&other) noexcept
    : type_(std::move(other.type_)), is_null(other.is_null), value_(other.value_),
      value_info_(std::move(other.value_info_)) {
}

Value &Value::operator=(const Value &other) {
	if (this == &other) {
		return *this;
	}
	type_ = other.type_;
	is_null = other.is_null;
	value_ = other.value_;
	value_info_ = other.value_info_;
	return *this;
}

Value &Value::operator=(Value &&other) noexcept {
	type_ = std::move(other.type_);
	is_null = other.is_null;
	value_ = other.value_;
	value_info_ = std::move(other.value_info_);
	return *this;
}

Value Value::MinimumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(false);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Minimum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Minimum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Minimum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Minimum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Minimum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Minimum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Minimum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Minimum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Minimum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MIN_YEAR, Date::DATE_MIN_MONTH, Date::DATE_MIN_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(0));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY),
		                        dtime_t(0));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MinimumValue(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIMESTAMP_MS:
		return MinimumValue(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Minimum()));
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(dtime_tz_t(dtime_t(0), dtime_tz_t::MIN_OFFSET));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(Timestamp::FromDatetime(
		    Date::FromDate(Timestamp::MIN_YEAR, Timestamp::MIN_MONTH, Timestamp::MIN_DAY), dtime_t(0)));
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Minimum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Minimum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(-NumericHelper::POWERS_OF_TEN[width] + 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(-Hugeint::POWERS_OF_TEN[width] + 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(0, type);
	default:
		throw InvalidTypeException(type, "MinimumValue requires numeric type");
	}
}

Value Value::MaximumValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(true);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(NumericLimits<int8_t>::Maximum());
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(NumericLimits<int16_t>::Maximum());
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::SQLNULL:
		return Value::INTEGER(NumericLimits<int32_t>::Maximum());
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(NumericLimits<int64_t>::Maximum());
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UUID:
		return Value::UUID(NumericLimits<hugeint_t>::Maximum());
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(NumericLimits<uint8_t>::Maximum());
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(NumericLimits<uint16_t>::Maximum());
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(NumericLimits<uint32_t>::Maximum());
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(NumericLimits<uint64_t>::Maximum());
	case LogicalTypeId::DATE:
		return Value::DATE(Date::FromDate(Date::DATE_MAX_YEAR, Date::DATE_MAX_MONTH, Date::DATE_MAX_DAY));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_MS:
		return MaximumValue(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::TIMESTAMP_MS);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(NumericLimits<int64_t>::Maximum() - 1));
	case LogicalTypeId::TIMESTAMP_SEC:
		return MaximumValue(LogicalType::TIMESTAMP).DefaultCastAs(LogicalType::TIMESTAMP_S);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(
		    dtime_tz_t(dtime_t(Interval::SECS_PER_DAY * Interval::MICROS_PER_SEC - 1), dtime_tz_t::MAX_OFFSET));
	case LogicalTypeId::TIMESTAMP_TZ:
		return MaximumValue(LogicalType::TIMESTAMP);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(NumericLimits<float>::Maximum());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(NumericLimits<double>::Maximum());
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(int16_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(int32_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(int64_t(NumericHelper::POWERS_OF_TEN[width] - 1), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(Hugeint::POWERS_OF_TEN[width] - 1, width, scale);
		default:
			throw InternalException("Unknown decimal type");
		}
	}
	case LogicalTypeId::ENUM:
		return Value::ENUM(EnumType::GetSize(type) - 1, type);
	default:
		throw InvalidTypeException(type, "MaximumValue requires numeric type");
	}
}

Value Value::Infinity(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return Value::DATE(date_t::infinity());
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t::infinity());
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t::infinity());
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(std::numeric_limits<float>::infinity());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(std::numeric_limits<double>::infinity());
	default:
		throw InvalidTypeException(type, "Infinity requires numeric type");
	}
}

Value Value::NegativeInfinity(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return Value::DATE(date_t::ninfinity());
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t::ninfinity());
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t::ninfinity());
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(-std::numeric_limits<float>::infinity());
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(-std::numeric_limits<double>::infinity());
	default:
		throw InvalidTypeException(type, "NegativeInfinity requires numeric type");
	}
}

Value Value::BOOLEAN(int8_t value) {
	Value result(LogicalType::BOOLEAN);
	result.value_.boolean = bool(value);
	result.is_null = false;
	return result;
}

Value Value::TINYINT(int8_t value) {
	Value result(LogicalType::TINYINT);
	result.value_.tinyint = value;
	result.is_null = false;
	return result;
}

Value Value::SMALLINT(int16_t value) {
	Value result(LogicalType::SMALLINT);
	result.value_.smallint = value;
	result.is_null = false;
	return result;
}

Value Value::INTEGER(int32_t value) {
	Value result(LogicalType::INTEGER);
	result.value_.integer = value;
	result.is_null = false;
	return result;
}

Value Value::BIGINT(int64_t value) {
	Value result(LogicalType::BIGINT);
	result.value_.bigint = value;
	result.is_null = false;
	return result;
}

Value Value::HUGEINT(hugeint_t value) {
	Value result(LogicalType::HUGEINT);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(hugeint_t value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::UUID(const string &value) {
	Value result(LogicalType::UUID);
	result.value_.hugeint = UUID::FromString(value);
	result.is_null = false;
	return result;
}

Value Value::UTINYINT(uint8_t value) {
	Value result(LogicalType::UTINYINT);
	result.value_.utinyint = value;
	result.is_null = false;
	return result;
}

Value Value::USMALLINT(uint16_t value) {
	Value result(LogicalType::USMALLINT);
	result.value_.usmallint = value;
	result.is_null = false;
	return result;
}

Value Value::UINTEGER(uint32_t value) {
	Value result(LogicalType::UINTEGER);
	result.value_.uinteger = value;
	result.is_null = false;
	return result;
}

Value Value::UBIGINT(uint64_t value) {
	Value result(LogicalType::UBIGINT);
	result.value_.ubigint = value;
	result.is_null = false;
	return result;
}

bool Value::FloatIsFinite(float value) {
	return !(std::isnan(value) || std::isinf(value));
}

bool Value::DoubleIsFinite(double value) {
	return !(std::isnan(value) || std::isinf(value));
}

template <>
bool Value::IsNan(float input) {
	return std::isnan(input);
}

template <>
bool Value::IsNan(double input) {
	return std::isnan(input);
}

template <>
bool Value::IsFinite(float input) {
	return Value::FloatIsFinite(input);
}

template <>
bool Value::IsFinite(double input) {
	return Value::DoubleIsFinite(input);
}

template <>
bool Value::IsFinite(date_t input) {
	return Date::IsFinite(input);
}

template <>
bool Value::IsFinite(timestamp_t input) {
	return Timestamp::IsFinite(input);
}

bool Value::StringIsValid(const char *str, idx_t length) {
	auto utf_type = Utf8Proc::Analyze(str, length);
	return utf_type != UnicodeType::INVALID;
}

Value Value::DECIMAL(int16_t value, uint8_t width, uint8_t scale) {
	return Value::DECIMAL(int64_t(value), width, scale);
}

Value Value::DECIMAL(int32_t value, uint8_t width, uint8_t scale) {
	return Value::DECIMAL(int64_t(value), width, scale);
}

Value Value::DECIMAL(int64_t value, uint8_t width, uint8_t scale) {
	auto decimal_type = LogicalType::DECIMAL(width, scale);
	Value result(decimal_type);
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		result.value_.smallint = value;
		break;
	case PhysicalType::INT32:
		result.value_.integer = value;
		break;
	case PhysicalType::INT64:
		result.value_.bigint = value;
		break;
	default:
		result.value_.hugeint = value;
		break;
	}
	result.type_.Verify();
	result.is_null = false;
	return result;
}

Value Value::DECIMAL(hugeint_t value, uint8_t width, uint8_t scale) {
	D_ASSERT(width >= Decimal::MAX_WIDTH_INT64 && width <= Decimal::MAX_WIDTH_INT128);
	Value result(LogicalType::DECIMAL(width, scale));
	result.value_.hugeint = value;
	result.is_null = false;
	return result;
}

Value Value::FLOAT(float value) {
	Value result(LogicalType::FLOAT);
	result.value_.float_ = value;
	result.is_null = false;
	return result;
}

Value Value::DOUBLE(double value) {
	Value result(LogicalType::DOUBLE);
	result.value_.double_ = value;
	result.is_null = false;
	return result;
}

Value Value::HASH(hash_t value) {
	Value result(LogicalType::HASH);
	result.value_.hash = value;
	result.is_null = false;
	return result;
}

Value Value::POINTER(uintptr_t value) {
	Value result(LogicalType::POINTER);
	result.value_.pointer = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(date_t value) {
	Value result(LogicalType::DATE);
	result.value_.date = value;
	result.is_null = false;
	return result;
}

Value Value::DATE(int32_t year, int32_t month, int32_t day) {
	return Value::DATE(Date::FromDate(year, month, day));
}

Value Value::TIME(dtime_t value) {
	Value result(LogicalType::TIME);
	result.value_.time = value;
	result.is_null = false;
	return result;
}

Value Value::TIMETZ(dtime_tz_t value) {
	Value result(LogicalType::TIME_TZ);
	result.value_.timetz = value;
	result.is_null = false;
	return result;
}

Value Value::TIME(int32_t hour, int32_t min, int32_t sec, int32_t micros) {
	return Value::TIME(Time::FromTime(hour, min, sec, micros));
}

Value Value::TIMESTAMP(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPTZ(timestamp_t value) {
	Value result(LogicalType::TIMESTAMP_TZ);
	result.value_.timestamp = value;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPNS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_NS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPMS(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_MS);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMPSEC(timestamp_t timestamp) {
	Value result(LogicalType::TIMESTAMP_S);
	result.value_.timestamp = timestamp;
	result.is_null = false;
	return result;
}

Value Value::TIMESTAMP(date_t date, dtime_t time) {
	return Value::TIMESTAMP(Timestamp::FromDatetime(date, time));
}

Value Value::TIMESTAMP(int32_t year, int32_t month, int32_t day, int32_t hour, int32_t min, int32_t sec,
                       int32_t micros) {
	auto val = Value::TIMESTAMP(Date::FromDate(year, month, day), Time::FromTime(hour, min, sec, micros));
	val.type_ = LogicalType::TIMESTAMP;
	return val;
}

Value Value::STRUCT(child_list_t<Value> values) {
	Value result;
	child_list_t<LogicalType> child_types;
	vector<Value> struct_values;
	for (auto &child : values) {
		child_types.push_back(make_pair(std::move(child.first), child.second.type()));
		struct_values.push_back(std::move(child.second));
	}
	result.value_info_ = make_shared<NestedValueInfo>(std::move(struct_values));
	result.type_ = LogicalType::STRUCT(child_types);
	result.is_null = false;
	return result;
}

Value Value::MAP(const LogicalType &child_type, vector<Value> values) {
	Value result;

	result.type_ = LogicalType::MAP(child_type);
	result.is_null = false;
	for (auto &val : values) {
		D_ASSERT(val.type().InternalType() == PhysicalType::STRUCT);
		auto &children = StructValue::GetChildren(val);

		// Ensure that the field containing the keys is called 'key'
		// and that the field containing the values is called 'value'
		// this is required to make equality checks work
		D_ASSERT(children.size() == 2);
		child_list_t<Value> new_children;
		new_children.reserve(2);
		new_children.push_back(std::make_pair("key", children[0]));
		new_children.push_back(std::make_pair("value", children[1]));
		val = Value::STRUCT(std::move(new_children));
	}
	result.value_info_ = make_shared<NestedValueInfo>(std::move(values));
	return result;
}

Value Value::UNION(child_list_t<LogicalType> members, uint8_t tag, Value value) {
	D_ASSERT(!members.empty());
	D_ASSERT(members.size() <= UnionType::MAX_UNION_MEMBERS);
	D_ASSERT(members.size() > tag);

	D_ASSERT(value.type() == members[tag].second);

	Value result;
	result.is_null = false;
	// add the tag to the front of the struct
	vector<Value> union_values;
	union_values.emplace_back(Value::UTINYINT(tag));
	for (idx_t i = 0; i < members.size(); i++) {
		if (i != tag) {
			union_values.emplace_back(members[i].second);
		} else {
			union_values.emplace_back(nullptr);
		}
	}
	union_values[tag + 1] = std::move(value);
	result.value_info_ = make_shared<NestedValueInfo>(std::move(union_values));
	result.type_ = LogicalType::UNION(std::move(members));
	return result;
}

Value Value::LIST(vector<Value> values) {
	if (values.empty()) {
		throw InternalException("Value::LIST without providing a child-type requires a non-empty list of values. Use "
		                        "Value::LIST(child_type, list) instead.");
	}
#ifdef DEBUG
	for (idx_t i = 1; i < values.size(); i++) {
		D_ASSERT(values[i].type() == values[0].type());
	}
#endif
	Value result;
	result.type_ = LogicalType::LIST(values[0].type());
	result.value_info_ = make_shared<NestedValueInfo>(std::move(values));
	result.is_null = false;
	return result;
}

Value Value::LIST(const LogicalType &child_type, vector<Value> values) {
	if (values.empty()) {
		return Value::EMPTYLIST(child_type);
	}
	for (auto &val : values) {
		val = val.DefaultCastAs(child_type);
	}
	return Value::LIST(std::move(values));
}

Value Value::EMPTYLIST(const LogicalType &child_type) {
	Value result;
	result.type_ = LogicalType::LIST(child_type);
	result.value_info_ = make_shared<NestedValueInfo>();
	result.is_null = false;
	return result;
}

Value Value::BLOB(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.value_info_ = make_shared<StringValueInfo>(string(const_char_ptr_cast(data), len));
	return result;
}

Value Value::BLOB(const string &data) {
	Value result(LogicalType::BLOB);
	result.is_null = false;
	result.value_info_ = make_shared<StringValueInfo>(Blob::ToBlob(string_t(data)));
	return result;
}

Value Value::BIT(const_data_ptr_t data, idx_t len) {
	Value result(LogicalType::BIT);
	result.is_null = false;
	result.value_info_ = make_shared<StringValueInfo>(string(const_char_ptr_cast(data), len));
	return result;
}

Value Value::BIT(const string &data) {
	Value result(LogicalType::BIT);
	result.is_null = false;
	result.value_info_ = make_shared<StringValueInfo>(Bit::ToBit(string_t(data)));
	return result;
}

Value Value::ENUM(uint64_t value, const LogicalType &original_type) {
	D_ASSERT(original_type.id() == LogicalTypeId::ENUM);
	Value result(original_type);
	switch (original_type.InternalType()) {
	case PhysicalType::UINT8:
		result.value_.utinyint = value;
		break;
	case PhysicalType::UINT16:
		result.value_.usmallint = value;
		break;
	case PhysicalType::UINT32:
		result.value_.uinteger = value;
		break;
	default:
		throw InternalException("Incorrect Physical Type for ENUM");
	}
	result.is_null = false;
	return result;
}

Value Value::INTERVAL(int32_t months, int32_t days, int64_t micros) {
	Value result(LogicalType::INTERVAL);
	result.is_null = false;
	result.value_.interval.months = months;
	result.value_.interval.days = days;
	result.value_.interval.micros = micros;
	return result;
}

Value Value::INTERVAL(interval_t interval) {
	return Value::INTERVAL(interval.months, interval.days, interval.micros);
}

//===--------------------------------------------------------------------===//
// CreateValue
//===--------------------------------------------------------------------===//
template <>
Value Value::CreateValue(bool value) {
	return Value::BOOLEAN(value);
}

template <>
Value Value::CreateValue(int8_t value) {
	return Value::TINYINT(value);
}

template <>
Value Value::CreateValue(int16_t value) {
	return Value::SMALLINT(value);
}

template <>
Value Value::CreateValue(int32_t value) {
	return Value::INTEGER(value);
}

template <>
Value Value::CreateValue(int64_t value) {
	return Value::BIGINT(value);
}

template <>
Value Value::CreateValue(uint8_t value) {
	return Value::UTINYINT(value);
}

template <>
Value Value::CreateValue(uint16_t value) {
	return Value::USMALLINT(value);
}

template <>
Value Value::CreateValue(uint32_t value) {
	return Value::UINTEGER(value);
}

template <>
Value Value::CreateValue(uint64_t value) {
	return Value::UBIGINT(value);
}

template <>
Value Value::CreateValue(hugeint_t value) {
	return Value::HUGEINT(value);
}

template <>
Value Value::CreateValue(date_t value) {
	return Value::DATE(value);
}

template <>
Value Value::CreateValue(dtime_t value) {
	return Value::TIME(value);
}

template <>
Value Value::CreateValue(dtime_tz_t value) {
	return Value::TIMETZ(value);
}

template <>
Value Value::CreateValue(timestamp_t value) {
	return Value::TIMESTAMP(value);
}

template <>
Value Value::CreateValue(timestamp_sec_t value) {
	return Value::TIMESTAMPSEC(value);
}

template <>
Value Value::CreateValue(timestamp_ms_t value) {
	return Value::TIMESTAMPMS(value);
}

template <>
Value Value::CreateValue(timestamp_ns_t value) {
	return Value::TIMESTAMPNS(value);
}

template <>
Value Value::CreateValue(timestamp_tz_t value) {
	return Value::TIMESTAMPTZ(value);
}

template <>
Value Value::CreateValue(const char *value) {
	return Value(string(value));
}

template <>
Value Value::CreateValue(string value) { // NOLINT: required for templating
	return Value::BLOB(value);
}

template <>
Value Value::CreateValue(string_t value) {
	return Value(value);
}

template <>
Value Value::CreateValue(float value) {
	return Value::FLOAT(value);
}

template <>
Value Value::CreateValue(double value) {
	return Value::DOUBLE(value);
}

template <>
Value Value::CreateValue(interval_t value) {
	return Value::INTERVAL(value);
}

template <>
Value Value::CreateValue(Value value) {
	return value;
}

//===--------------------------------------------------------------------===//
// GetValue
//===--------------------------------------------------------------------===//
template <class T>
T Value::GetValueInternal() const {
	if (IsNull()) {
		throw InternalException("Calling GetValueInternal on a value that is NULL");
	}
	switch (type_.id()) {
	case LogicalTypeId::BOOLEAN:
		return Cast::Operation<bool, T>(value_.boolean);
	case LogicalTypeId::TINYINT:
		return Cast::Operation<int8_t, T>(value_.tinyint);
	case LogicalTypeId::SMALLINT:
		return Cast::Operation<int16_t, T>(value_.smallint);
	case LogicalTypeId::INTEGER:
		return Cast::Operation<int32_t, T>(value_.integer);
	case LogicalTypeId::BIGINT:
		return Cast::Operation<int64_t, T>(value_.bigint);
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return Cast::Operation<hugeint_t, T>(value_.hugeint);
	case LogicalTypeId::DATE:
		return Cast::Operation<date_t, T>(value_.date);
	case LogicalTypeId::TIME:
		return Cast::Operation<dtime_t, T>(value_.time);
	case LogicalTypeId::TIME_TZ:
		return Cast::Operation<dtime_tz_t, T>(value_.timetz);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return Cast::Operation<timestamp_t, T>(value_.timestamp);
	case LogicalTypeId::UTINYINT:
		return Cast::Operation<uint8_t, T>(value_.utinyint);
	case LogicalTypeId::USMALLINT:
		return Cast::Operation<uint16_t, T>(value_.usmallint);
	case LogicalTypeId::UINTEGER:
		return Cast::Operation<uint32_t, T>(value_.uinteger);
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::UBIGINT:
		return Cast::Operation<uint64_t, T>(value_.ubigint);
	case LogicalTypeId::FLOAT:
		return Cast::Operation<float, T>(value_.float_);
	case LogicalTypeId::DOUBLE:
		return Cast::Operation<double, T>(value_.double_);
	case LogicalTypeId::VARCHAR:
		return Cast::Operation<string_t, T>(StringValue::Get(*this).c_str());
	case LogicalTypeId::INTERVAL:
		return Cast::Operation<interval_t, T>(value_.interval);
	case LogicalTypeId::DECIMAL:
		return DefaultCastAs(LogicalType::DOUBLE).GetValueInternal<T>();
	case LogicalTypeId::ENUM: {
		switch (type_.InternalType()) {
		case PhysicalType::UINT8:
			return Cast::Operation<uint8_t, T>(value_.utinyint);
		case PhysicalType::UINT16:
			return Cast::Operation<uint16_t, T>(value_.usmallint);
		case PhysicalType::UINT32:
			return Cast::Operation<uint32_t, T>(value_.uinteger);
		default:
			throw InternalException("Invalid Internal Type for ENUMs");
		}
	}
	default:
		throw NotImplementedException("Unimplemented type \"%s\" for GetValue()", type_.ToString());
	}
}

template <>
bool Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int8_t Value::GetValue() const {
	return GetValueInternal<int8_t>();
}
template <>
int16_t Value::GetValue() const {
	return GetValueInternal<int16_t>();
}
template <>
int32_t Value::GetValue() const {
	if (type_.id() == LogicalTypeId::DATE) {
		return value_.integer;
	}
	return GetValueInternal<int32_t>();
}
template <>
int64_t Value::GetValue() const {
	if (IsNull()) {
		throw InternalException("Calling GetValue on a value that is NULL");
	}
	switch (type_.id()) {
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_TZ:
		return value_.bigint;
	default:
		return GetValueInternal<int64_t>();
	}
}
template <>
hugeint_t Value::GetValue() const {
	return GetValueInternal<hugeint_t>();
}
template <>
uint8_t Value::GetValue() const {
	return GetValueInternal<uint8_t>();
}
template <>
uint16_t Value::GetValue() const {
	return GetValueInternal<uint16_t>();
}
template <>
uint32_t Value::GetValue() const {
	return GetValueInternal<uint32_t>();
}
template <>
uint64_t Value::GetValue() const {
	return GetValueInternal<uint64_t>();
}
template <>
string Value::GetValue() const {
	return ToString();
}
template <>
float Value::GetValue() const {
	return GetValueInternal<float>();
}
template <>
double Value::GetValue() const {
	return GetValueInternal<double>();
}
template <>
date_t Value::GetValue() const {
	return GetValueInternal<date_t>();
}
template <>
dtime_t Value::GetValue() const {
	return GetValueInternal<dtime_t>();
}
template <>
timestamp_t Value::GetValue() const {
	return GetValueInternal<timestamp_t>();
}

template <>
DUCKDB_API interval_t Value::GetValue() const {
	return GetValueInternal<interval_t>();
}

template <>
DUCKDB_API Value Value::GetValue() const {
	return Value(*this);
}

uintptr_t Value::GetPointer() const {
	D_ASSERT(type() == LogicalType::POINTER);
	return value_.pointer;
}

Value Value::Numeric(const LogicalType &type, int64_t value) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		D_ASSERT(value == 0 || value == 1);
		return Value::BOOLEAN(value ? 1 : 0);
	case LogicalTypeId::TINYINT:
		D_ASSERT(value >= NumericLimits<int8_t>::Minimum() && value <= NumericLimits<int8_t>::Maximum());
		return Value::TINYINT((int8_t)value);
	case LogicalTypeId::SMALLINT:
		D_ASSERT(value >= NumericLimits<int16_t>::Minimum() && value <= NumericLimits<int16_t>::Maximum());
		return Value::SMALLINT((int16_t)value);
	case LogicalTypeId::INTEGER:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::INTEGER((int32_t)value);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(value);
	case LogicalTypeId::UTINYINT:
		D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
		return Value::UTINYINT((uint8_t)value);
	case LogicalTypeId::USMALLINT:
		D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
		return Value::USMALLINT((uint16_t)value);
	case LogicalTypeId::UINTEGER:
		D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
		return Value::UINTEGER((uint32_t)value);
	case LogicalTypeId::UBIGINT:
		D_ASSERT(value >= 0);
		return Value::UBIGINT(value);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::DECIMAL:
		return Value::DECIMAL(value, DecimalType::GetWidth(type), DecimalType::GetScale(type));
	case LogicalTypeId::FLOAT:
		return Value((float)value);
	case LogicalTypeId::DOUBLE:
		return Value((double)value);
	case LogicalTypeId::POINTER:
		return Value::POINTER(value);
	case LogicalTypeId::DATE:
		D_ASSERT(value >= NumericLimits<int32_t>::Minimum() && value <= NumericLimits<int32_t>::Maximum());
		return Value::DATE(date_t(value));
	case LogicalTypeId::TIME:
		return Value::TIME(dtime_t(value));
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(timestamp_t(value));
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(timestamp_t(value));
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			D_ASSERT(value >= NumericLimits<uint8_t>::Minimum() && value <= NumericLimits<uint8_t>::Maximum());
			return Value::UTINYINT((uint8_t)value);
		case PhysicalType::UINT16:
			D_ASSERT(value >= NumericLimits<uint16_t>::Minimum() && value <= NumericLimits<uint16_t>::Maximum());
			return Value::USMALLINT((uint16_t)value);
		case PhysicalType::UINT32:
			D_ASSERT(value >= NumericLimits<uint32_t>::Minimum() && value <= NumericLimits<uint32_t>::Maximum());
			return Value::UINTEGER((uint32_t)value);
		default:
			throw InternalException("Enum doesn't accept this physical type");
		}
	default:
		throw InvalidTypeException(type, "Numeric requires numeric type");
	}
}

Value Value::Numeric(const LogicalType &type, hugeint_t value) {
#ifdef DEBUG
	// perform a throwing cast to verify that the type fits
	Value::HUGEINT(value).DefaultCastAs(type);
#endif
	switch (type.id()) {
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(value);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(Hugeint::Cast<uint64_t>(value));
	default:
		return Value::Numeric(type, Hugeint::Cast<int64_t>(value));
	}
}

//===--------------------------------------------------------------------===//
// GetValueUnsafe
//===--------------------------------------------------------------------===//
template <>
DUCKDB_API bool Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::BOOL);
	return value_.boolean;
}

template <>
int8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT8 || type_.InternalType() == PhysicalType::BOOL);
	return value_.tinyint;
}

template <>
int16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT16);
	return value_.smallint;
}

template <>
int32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.integer;
}

template <>
int64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.bigint;
}

template <>
hugeint_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT128);
	return value_.hugeint;
}

template <>
uint8_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT8);
	return value_.utinyint;
}

template <>
uint16_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT16);
	return value_.usmallint;
}

template <>
uint32_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT32);
	return value_.uinteger;
}

template <>
uint64_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::UINT64);
	return value_.ubigint;
}

template <>
string Value::GetValueUnsafe() const {
	return StringValue::Get(*this);
}

template <>
DUCKDB_API string_t Value::GetValueUnsafe() const {
	return string_t(StringValue::Get(*this));
}

template <>
float Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::FLOAT);
	return value_.float_;
}

template <>
double Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::DOUBLE);
	return value_.double_;
}

template <>
date_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT32);
	return value_.date;
}

template <>
dtime_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.time;
}

template <>
timestamp_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INT64);
	return value_.timestamp;
}

template <>
interval_t Value::GetValueUnsafe() const {
	D_ASSERT(type_.InternalType() == PhysicalType::INTERVAL);
	return value_.interval;
}

//===--------------------------------------------------------------------===//
// Hash
//===--------------------------------------------------------------------===//
hash_t Value::Hash() const {
	if (IsNull()) {
		return 0;
	}
	Vector input(*this);
	Vector result(LogicalType::HASH);
	VectorOperations::Hash(input, result, 1);

	auto data = FlatVector::GetData<hash_t>(result);
	return data[0];
}

string Value::ToString() const {
	if (IsNull()) {
		return "NULL";
	}
	return StringValue::Get(DefaultCastAs(LogicalType::VARCHAR));
}

string Value::ToSQLString() const {
	if (IsNull()) {
		return ToString();
	}
	switch (type_.id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::BLOB:
		return "'" + ToString() + "'::" + type_.ToString();
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
		return "'" + StringUtil::Replace(ToString(), "'", "''") + "'";
	case LogicalTypeId::STRUCT: {
		string ret = "{";
		auto &child_types = StructType::GetChildTypes(type_);
		auto &struct_values = StructValue::GetChildren(*this);
		for (size_t i = 0; i < struct_values.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_values[i];
			ret += "'" + name + "': " + child.ToSQLString();
			if (i < struct_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += "}";
		return ret;
	}
	case LogicalTypeId::FLOAT:
		if (!FloatIsFinite(FloatValue::Get(*this))) {
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(*this);
		if (!DoubleIsFinite(val)) {
			if (!Value::IsNan(val)) {
				// to infinity and beyond
				return val < 0 ? "-1e1000" : "1e1000";
			}
			return "'" + ToString() + "'::" + type_.ToString();
		}
		return ToString();
	}
	case LogicalTypeId::LIST: {
		string ret = "[";
		auto &list_values = ListValue::GetChildren(*this);
		for (size_t i = 0; i < list_values.size(); i++) {
			auto &child = list_values[i];
			ret += child.ToSQLString();
			if (i < list_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	default:
		return ToString();
	}
}

//===--------------------------------------------------------------------===//
// Type-specific getters
//===--------------------------------------------------------------------===//
bool BooleanValue::Get(const Value &value) {
	return value.GetValueUnsafe<bool>();
}

int8_t TinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int8_t>();
}

int16_t SmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int16_t>();
}

int32_t IntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<int32_t>();
}

int64_t BigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<int64_t>();
}

hugeint_t HugeIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<hugeint_t>();
}

uint8_t UTinyIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint8_t>();
}

uint16_t USmallIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint16_t>();
}

uint32_t UIntegerValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint32_t>();
}

uint64_t UBigIntValue::Get(const Value &value) {
	return value.GetValueUnsafe<uint64_t>();
}

float FloatValue::Get(const Value &value) {
	return value.GetValueUnsafe<float>();
}

double DoubleValue::Get(const Value &value) {
	return value.GetValueUnsafe<double>();
}

const string &StringValue::Get(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling StringValue::Get on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<StringValueInfo>().GetString();
}

date_t DateValue::Get(const Value &value) {
	return value.GetValueUnsafe<date_t>();
}

dtime_t TimeValue::Get(const Value &value) {
	return value.GetValueUnsafe<dtime_t>();
}

timestamp_t TimestampValue::Get(const Value &value) {
	return value.GetValueUnsafe<timestamp_t>();
}

interval_t IntervalValue::Get(const Value &value) {
	return value.GetValueUnsafe<interval_t>();
}

const vector<Value> &StructValue::GetChildren(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling StructValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::STRUCT);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const vector<Value> &ListValue::GetChildren(const Value &value) {
	if (value.is_null) {
		throw InternalException("Calling ListValue::GetChildren on a NULL value");
	}
	D_ASSERT(value.type().InternalType() == PhysicalType::LIST);
	D_ASSERT(value.value_info_);
	return value.value_info_->Get<NestedValueInfo>().GetValues();
}

const Value &UnionValue::GetValue(const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::UNION);
	auto &children = StructValue::GetChildren(value);
	auto tag = children[0].GetValueUnsafe<union_tag_t>();
	D_ASSERT(tag < children.size() - 1);
	return children[tag + 1];
}

union_tag_t UnionValue::GetTag(const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::UNION);
	auto children = StructValue::GetChildren(value);
	auto tag = children[0].GetValueUnsafe<union_tag_t>();
	D_ASSERT(tag < children.size() - 1);
	return tag;
}

const LogicalType &UnionValue::GetType(const Value &value) {
	return UnionType::GetMemberType(value.type(), UnionValue::GetTag(value));
}

hugeint_t IntegralValue::Get(const Value &value) {
	switch (value.type().InternalType()) {
	case PhysicalType::INT8:
		return TinyIntValue::Get(value);
	case PhysicalType::INT16:
		return SmallIntValue::Get(value);
	case PhysicalType::INT32:
		return IntegerValue::Get(value);
	case PhysicalType::INT64:
		return BigIntValue::Get(value);
	case PhysicalType::INT128:
		return HugeIntValue::Get(value);
	case PhysicalType::UINT8:
		return UTinyIntValue::Get(value);
	case PhysicalType::UINT16:
		return USmallIntValue::Get(value);
	case PhysicalType::UINT32:
		return UIntegerValue::Get(value);
	case PhysicalType::UINT64:
		return UBigIntValue::Get(value);
	default:
		throw InternalException("Invalid internal type \"%s\" for IntegralValue::Get", value.type().ToString());
	}
}

//===--------------------------------------------------------------------===//
// Comparison Operators
//===--------------------------------------------------------------------===//
bool Value::operator==(const Value &rhs) const {
	return ValueOperations::Equals(*this, rhs);
}

bool Value::operator!=(const Value &rhs) const {
	return ValueOperations::NotEquals(*this, rhs);
}

bool Value::operator<(const Value &rhs) const {
	return ValueOperations::LessThan(*this, rhs);
}

bool Value::operator>(const Value &rhs) const {
	return ValueOperations::GreaterThan(*this, rhs);
}

bool Value::operator<=(const Value &rhs) const {
	return ValueOperations::LessThanEquals(*this, rhs);
}

bool Value::operator>=(const Value &rhs) const {
	return ValueOperations::GreaterThanEquals(*this, rhs);
}

bool Value::operator==(const int64_t &rhs) const {
	return *this == Value::Numeric(type_, rhs);
}

bool Value::operator!=(const int64_t &rhs) const {
	return *this != Value::Numeric(type_, rhs);
}

bool Value::operator<(const int64_t &rhs) const {
	return *this < Value::Numeric(type_, rhs);
}

bool Value::operator>(const int64_t &rhs) const {
	return *this > Value::Numeric(type_, rhs);
}

bool Value::operator<=(const int64_t &rhs) const {
	return *this <= Value::Numeric(type_, rhs);
}

bool Value::operator>=(const int64_t &rhs) const {
	return *this >= Value::Numeric(type_, rhs);
}

bool Value::TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                      Value &new_value, string *error_message, bool strict) const {
	if (type_ == target_type) {
		new_value = Copy();
		return true;
	}
	Vector input(*this);
	Vector result(target_type);
	if (!VectorOperations::TryCast(set, get_input, input, result, 1, error_message, strict)) {
		return false;
	}
	new_value = result.GetValue(0);
	return true;
}

bool Value::TryCastAs(ClientContext &context, const LogicalType &target_type, Value &new_value, string *error_message,
                      bool strict) const {
	GetCastFunctionInput get_input(context);
	return TryCastAs(CastFunctionSet::Get(context), get_input, target_type, new_value, error_message, strict);
}

bool Value::DefaultTryCastAs(const LogicalType &target_type, Value &new_value, string *error_message,
                             bool strict) const {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return TryCastAs(set, get_input, target_type, new_value, error_message, strict);
}

Value Value::CastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                    bool strict) const {
	Value new_value;
	string error_message;
	if (!TryCastAs(set, get_input, target_type, new_value, &error_message, strict)) {
		throw InvalidInputException("Failed to cast value: %s", error_message);
	}
	return new_value;
}

Value Value::CastAs(ClientContext &context, const LogicalType &target_type, bool strict) const {
	GetCastFunctionInput get_input(context);
	return CastAs(CastFunctionSet::Get(context), get_input, target_type, strict);
}

Value Value::DefaultCastAs(const LogicalType &target_type, bool strict) const {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return CastAs(set, get_input, target_type, strict);
}

bool Value::TryCastAs(CastFunctionSet &set, GetCastFunctionInput &get_input, const LogicalType &target_type,
                      bool strict) {
	Value new_value;
	string error_message;
	if (!TryCastAs(set, get_input, target_type, new_value, &error_message, strict)) {
		return false;
	}
	type_ = target_type;
	is_null = new_value.is_null;
	value_ = new_value.value_;
	value_info_ = std::move(new_value.value_info_);
	return true;
}

bool Value::TryCastAs(ClientContext &context, const LogicalType &target_type, bool strict) {
	GetCastFunctionInput get_input(context);
	return TryCastAs(CastFunctionSet::Get(context), get_input, target_type, strict);
}

bool Value::DefaultTryCastAs(const LogicalType &target_type, bool strict) {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return TryCastAs(set, get_input, target_type, strict);
}

void Value::Reinterpret(LogicalType new_type) {
	this->type_ = std::move(new_type);
}

void Value::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", type_);
	serializer.WriteProperty(101, "is_null", is_null);
	if (!IsNull()) {
		switch (type_.InternalType()) {
		case PhysicalType::BIT:
			throw InternalException("BIT type should not be serialized");
		case PhysicalType::BOOL:
			serializer.WriteProperty(102, "value", value_.boolean);
			break;
		case PhysicalType::INT8:
			serializer.WriteProperty(102, "value", value_.tinyint);
			break;
		case PhysicalType::INT16:
			serializer.WriteProperty(102, "value", value_.smallint);
			break;
		case PhysicalType::INT32:
			serializer.WriteProperty(102, "value", value_.integer);
			break;
		case PhysicalType::INT64:
			serializer.WriteProperty(102, "value", value_.bigint);
			break;
		case PhysicalType::UINT8:
			serializer.WriteProperty(102, "value", value_.utinyint);
			break;
		case PhysicalType::UINT16:
			serializer.WriteProperty(102, "value", value_.usmallint);
			break;
		case PhysicalType::UINT32:
			serializer.WriteProperty(102, "value", value_.uinteger);
			break;
		case PhysicalType::UINT64:
			serializer.WriteProperty(102, "value", value_.ubigint);
			break;
		case PhysicalType::INT128:
			serializer.WriteProperty(102, "value", value_.hugeint);
			break;
		case PhysicalType::FLOAT:
			serializer.WriteProperty(102, "value", value_.float_);
			break;
		case PhysicalType::DOUBLE:
			serializer.WriteProperty(102, "value", value_.double_);
			break;
		case PhysicalType::INTERVAL:
			serializer.WriteProperty(102, "value", value_.interval);
			break;
		case PhysicalType::VARCHAR: {
			if (type_.id() == LogicalTypeId::BLOB) {
				auto blob_str = Blob::ToString(StringValue::Get(*this));
				serializer.WriteProperty(102, "value", blob_str);
			} else {
				serializer.WriteProperty(102, "value", StringValue::Get(*this));
			}
		} break;
		case PhysicalType::LIST: {
			serializer.WriteObject(102, "value", [&](Serializer &serializer) {
				auto &children = ListValue::GetChildren(*this);
				serializer.WriteProperty(100, "children", children);
			});
		} break;
		case PhysicalType::STRUCT: {
			serializer.WriteObject(102, "value", [&](Serializer &serializer) {
				auto &children = StructValue::GetChildren(*this);
				serializer.WriteProperty(100, "children", children);
			});
		} break;
		default:
			throw NotImplementedException("Unimplemented type for Serialize");
		}
	}
}

Value Value::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<LogicalType>(100, "type");
	auto is_null = deserializer.ReadProperty<bool>(101, "is_null");
	Value new_value = Value(type);
	if (is_null) {
		return new_value;
	}
	new_value.is_null = false;
	switch (type.InternalType()) {
	case PhysicalType::BIT:
		throw InternalException("BIT type should not be deserialized");
	case PhysicalType::BOOL:
		new_value.value_.boolean = deserializer.ReadProperty<bool>(102, "value");
		break;
	case PhysicalType::UINT8:
		new_value.value_.utinyint = deserializer.ReadProperty<uint8_t>(102, "value");
		break;
	case PhysicalType::INT8:
		new_value.value_.tinyint = deserializer.ReadProperty<int8_t>(102, "value");
		break;
	case PhysicalType::UINT16:
		new_value.value_.usmallint = deserializer.ReadProperty<uint16_t>(102, "value");
		break;
	case PhysicalType::INT16:
		new_value.value_.smallint = deserializer.ReadProperty<int16_t>(102, "value");
		break;
	case PhysicalType::UINT32:
		new_value.value_.uinteger = deserializer.ReadProperty<uint32_t>(102, "value");
		break;
	case PhysicalType::INT32:
		new_value.value_.integer = deserializer.ReadProperty<int32_t>(102, "value");
		break;
	case PhysicalType::UINT64:
		new_value.value_.ubigint = deserializer.ReadProperty<uint64_t>(102, "value");
		break;
	case PhysicalType::INT64:
		new_value.value_.bigint = deserializer.ReadProperty<int64_t>(102, "value");
		break;
	case PhysicalType::INT128:
		new_value.value_.hugeint = deserializer.ReadProperty<hugeint_t>(102, "value");
		break;
	case PhysicalType::FLOAT:
		new_value.value_.float_ = deserializer.ReadProperty<float>(102, "value");
		break;
	case PhysicalType::DOUBLE:
		new_value.value_.double_ = deserializer.ReadProperty<double>(102, "value");
		break;
	case PhysicalType::INTERVAL:
		new_value.value_.interval = deserializer.ReadProperty<interval_t>(102, "value");
		break;
	case PhysicalType::VARCHAR: {
		auto str = deserializer.ReadProperty<string>(102, "value");
		if (type.id() == LogicalTypeId::BLOB) {
			new_value.value_info_ = make_shared<StringValueInfo>(Blob::ToBlob(str));
		} else {
			new_value.value_info_ = make_shared<StringValueInfo>(str);
		}
	} break;
	case PhysicalType::LIST: {
		deserializer.ReadObject(102, "value", [&](Deserializer &obj) {
			auto children = obj.ReadProperty<vector<Value>>(100, "children");
			new_value.value_info_ = make_shared<NestedValueInfo>(children);
		});
	} break;
	case PhysicalType::STRUCT: {
		deserializer.ReadObject(102, "value", [&](Deserializer &obj) {
			auto children = obj.ReadProperty<vector<Value>>(100, "children");
			new_value.value_info_ = make_shared<NestedValueInfo>(children);
		});
	} break;
	default:
		throw NotImplementedException("Unimplemented type for Deserialize");
	}
	return new_value;
}

void Value::Print() const {
	Printer::Print(ToString());
}

bool Value::NotDistinctFrom(const Value &lvalue, const Value &rvalue) {
	return ValueOperations::NotDistinctFrom(lvalue, rvalue);
}

static string SanitizeValue(string input) {
	// some results might contain padding spaces, e.g. when rendering
	// VARCHAR(10) and the string only has 6 characters, they will be padded
	// with spaces to 10 in the rendering. We don't do that here yet as we
	// are looking at internal structures. So just ignore any extra spaces
	// on the right
	StringUtil::RTrim(input);
	// for result checking code, replace null bytes with their escaped value (\0)
	return StringUtil::Replace(input, string("\0", 1), "\\0");
}

bool Value::ValuesAreEqual(CastFunctionSet &set, GetCastFunctionInput &get_input, const Value &result_value,
                           const Value &value) {
	if (result_value.IsNull() != value.IsNull()) {
		return false;
	}
	if (result_value.IsNull() && value.IsNull()) {
		// NULL = NULL in checking code
		return true;
	}
	switch (value.type_.id()) {
	case LogicalTypeId::FLOAT: {
		auto other = result_value.CastAs(set, get_input, LogicalType::FLOAT);
		float ldecimal = value.value_.float_;
		float rdecimal = other.value_.float_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::DOUBLE: {
		auto other = result_value.CastAs(set, get_input, LogicalType::DOUBLE);
		double ldecimal = value.value_.double_;
		double rdecimal = other.value_.double_;
		return ApproxEqual(ldecimal, rdecimal);
	}
	case LogicalTypeId::VARCHAR: {
		auto other = result_value.CastAs(set, get_input, LogicalType::VARCHAR);
		string left = SanitizeValue(StringValue::Get(other));
		string right = SanitizeValue(StringValue::Get(value));
		return left == right;
	}
	default:
		if (result_value.type_.id() == LogicalTypeId::FLOAT || result_value.type_.id() == LogicalTypeId::DOUBLE) {
			return Value::ValuesAreEqual(set, get_input, value, result_value);
		}
		return value == result_value;
	}
}

bool Value::ValuesAreEqual(ClientContext &context, const Value &result_value, const Value &value) {
	GetCastFunctionInput get_input(context);
	return Value::ValuesAreEqual(CastFunctionSet::Get(context), get_input, result_value, value);
}
bool Value::DefaultValuesAreEqual(const Value &result_value, const Value &value) {
	CastFunctionSet set;
	GetCastFunctionInput get_input;
	return Value::ValuesAreEqual(set, get_input, result_value, value);
}

} // namespace duckdb
























#include <cstring> // strlen() on Solaris

namespace duckdb {

Vector::Vector(LogicalType type_p, bool create_data, bool zero_data, idx_t capacity)
    : vector_type(VectorType::FLAT_VECTOR), type(std::move(type_p)), data(nullptr) {
	if (create_data) {
		Initialize(zero_data, capacity);
	}
}

Vector::Vector(LogicalType type_p, idx_t capacity) : Vector(std::move(type_p), true, false, capacity) {
}

Vector::Vector(LogicalType type_p, data_ptr_t dataptr)
    : vector_type(VectorType::FLAT_VECTOR), type(std::move(type_p)), data(dataptr) {
	if (dataptr && !type.IsValid()) {
		throw InternalException("Cannot create a vector of type INVALID!");
	}
}

Vector::Vector(const VectorCache &cache) : type(cache.GetType()) {
	ResetFromCache(cache);
}

Vector::Vector(Vector &other) : type(other.type) {
	Reference(other);
}

Vector::Vector(Vector &other, const SelectionVector &sel, idx_t count) : type(other.type) {
	Slice(other, sel, count);
}

Vector::Vector(Vector &other, idx_t offset, idx_t end) : type(other.type) {
	Slice(other, offset, end);
}

Vector::Vector(const Value &value) : type(value.type()) {
	Reference(value);
}

Vector::Vector(Vector &&other) noexcept
    : vector_type(other.vector_type), type(std::move(other.type)), data(other.data),
      validity(std::move(other.validity)), buffer(std::move(other.buffer)), auxiliary(std::move(other.auxiliary)) {
}

void Vector::Reference(const Value &value) {
	D_ASSERT(GetType().id() == value.type().id());
	this->vector_type = VectorType::CONSTANT_VECTOR;
	buffer = VectorBuffer::CreateConstantVector(value.type());
	auto internal_type = value.type().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_uniq<VectorStructBuffer>();
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &child_vectors = struct_buffer->GetChildren();
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto vector =
			    make_uniq<Vector>(value.IsNull() ? Value(child_types[i].second) : StructValue::GetChildren(value)[i]);
			child_vectors.push_back(std::move(vector));
		}
		auxiliary = shared_ptr<VectorBuffer>(struct_buffer.release());
		if (value.IsNull()) {
			SetValue(0, value);
		}
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_uniq<VectorListBuffer>(value.type());
		auxiliary = shared_ptr<VectorBuffer>(list_buffer.release());
		data = buffer->GetData();
		SetValue(0, value);
	} else {
		auxiliary.reset();
		data = buffer->GetData();
		SetValue(0, value);
	}
}

void Vector::Reference(const Vector &other) {
	if (other.GetType().id() != GetType().id()) {
		throw InternalException("Vector::Reference used on vector of different type");
	}
	D_ASSERT(other.GetType() == GetType());
	Reinterpret(other);
}

void Vector::ReferenceAndSetType(const Vector &other) {
	type = other.GetType();
	Reference(other);
}

void Vector::Reinterpret(const Vector &other) {
	vector_type = other.vector_type;
	AssignSharedPointer(buffer, other.buffer);
	AssignSharedPointer(auxiliary, other.auxiliary);
	data = other.data;
	validity = other.validity;
}

void Vector::ResetFromCache(const VectorCache &cache) {
	cache.ResetFromCache(*this);
}

void Vector::Slice(Vector &other, idx_t offset, idx_t end) {
	if (other.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		Reference(other);
		return;
	}
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR);

	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		Vector new_vector(GetType());
		auto &entries = StructVector::GetEntries(new_vector);
		auto &other_entries = StructVector::GetEntries(other);
		D_ASSERT(entries.size() == other_entries.size());
		for (idx_t i = 0; i < entries.size(); i++) {
			entries[i]->Slice(*other_entries[i], offset, end);
		}
		new_vector.validity.Slice(other.validity, offset, end - offset);
		Reference(new_vector);
	} else {
		Reference(other);
		if (offset > 0) {
			data = data + GetTypeIdSize(internal_type) * offset;
			validity.Slice(other.validity, offset, end - offset);
		}
	}
}

void Vector::Slice(Vector &other, const SelectionVector &sel, idx_t count) {
	Reference(other);
	Slice(sel, count);
}

void Vector::Slice(const SelectionVector &sel, idx_t count) {
	if (GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// dictionary on a constant is just a constant
		return;
	}
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// already a dictionary, slice the current dictionary
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto sliced_dictionary = current_sel.Slice(sel, count);
		buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary));
		if (GetType().InternalType() == PhysicalType::STRUCT) {
			auto &child_vector = DictionaryVector::Child(*this);

			Vector new_child(child_vector);
			new_child.auxiliary = make_buffer<VectorStructBuffer>(new_child, sel, count);
			auxiliary = make_buffer<VectorChildBuffer>(std::move(new_child));
		}
		return;
	}

	if (GetVectorType() == VectorType::FSST_VECTOR) {
		Flatten(sel, count);
		return;
	}

	Vector child_vector(*this);
	auto internal_type = GetType().InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		child_vector.auxiliary = make_buffer<VectorStructBuffer>(*this, sel, count);
	}
	auto child_ref = make_buffer<VectorChildBuffer>(std::move(child_vector));
	auto dict_buffer = make_buffer<DictionaryBuffer>(sel);
	vector_type = VectorType::DICTIONARY_VECTOR;
	buffer = std::move(dict_buffer);
	auxiliary = std::move(child_ref);
}

void Vector::Slice(const SelectionVector &sel, idx_t count, SelCache &cache) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR && GetType().InternalType() != PhysicalType::STRUCT) {
		// dictionary vector: need to merge dictionaries
		// check if we have a cached entry
		auto &current_sel = DictionaryVector::SelVector(*this);
		auto target_data = current_sel.data();
		auto entry = cache.cache.find(target_data);
		if (entry != cache.cache.end()) {
			// cached entry exists: use that
			this->buffer = make_buffer<DictionaryBuffer>(entry->second->Cast<DictionaryBuffer>().GetSelVector());
			vector_type = VectorType::DICTIONARY_VECTOR;
		} else {
			Slice(sel, count);
			cache.cache[target_data] = this->buffer;
		}
	} else {
		Slice(sel, count);
	}
}

void Vector::Initialize(bool zero_data, idx_t capacity) {
	auxiliary.reset();
	validity.Reset();
	auto &type = GetType();
	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		auto struct_buffer = make_uniq<VectorStructBuffer>(type, capacity);
		auxiliary = shared_ptr<VectorBuffer>(struct_buffer.release());
	} else if (internal_type == PhysicalType::LIST) {
		auto list_buffer = make_uniq<VectorListBuffer>(type, capacity);
		auxiliary = shared_ptr<VectorBuffer>(list_buffer.release());
	}
	auto type_size = GetTypeIdSize(internal_type);
	if (type_size > 0) {
		buffer = VectorBuffer::CreateStandardVector(type, capacity);
		data = buffer->GetData();
		if (zero_data) {
			memset(data, 0, capacity * type_size);
		}
	}
	if (capacity > STANDARD_VECTOR_SIZE) {
		validity.Resize(STANDARD_VECTOR_SIZE, capacity);
	}
}

struct DataArrays {
	Vector &vec;
	data_ptr_t data;
	optional_ptr<VectorBuffer> buffer;
	idx_t type_size;
	bool is_nested;
	DataArrays(Vector &vec, data_ptr_t data, optional_ptr<VectorBuffer> buffer, idx_t type_size, bool is_nested)
	    : vec(vec), data(data), buffer(buffer), type_size(type_size), is_nested(is_nested) {
	}
};

void FindChildren(vector<DataArrays> &to_resize, VectorBuffer &auxiliary) {
	if (auxiliary.GetBufferType() == VectorBufferType::LIST_BUFFER) {
		auto &buffer = auxiliary.Cast<VectorListBuffer>();
		auto &child = buffer.GetChild();
		auto data = child.GetData();
		if (!data) {
			//! Nested type
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  true);
			to_resize.emplace_back(arrays);
			FindChildren(to_resize, *child.GetAuxiliary());
		} else {
			DataArrays arrays(child, data, child.GetBuffer().get(), GetTypeIdSize(child.GetType().InternalType()),
			                  false);
			to_resize.emplace_back(arrays);
		}
	} else if (auxiliary.GetBufferType() == VectorBufferType::STRUCT_BUFFER) {
		auto &buffer = auxiliary.Cast<VectorStructBuffer>();
		auto &children = buffer.GetChildren();
		for (auto &child : children) {
			auto data = child->GetData();
			if (!data) {
				//! Nested type
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), true);
				to_resize.emplace_back(arrays);
				FindChildren(to_resize, *child->GetAuxiliary());
			} else {
				DataArrays arrays(*child, data, child->GetBuffer().get(),
				                  GetTypeIdSize(child->GetType().InternalType()), false);
				to_resize.emplace_back(arrays);
			}
		}
	}
}
void Vector::Resize(idx_t cur_size, idx_t new_size) {
	vector<DataArrays> to_resize;
	if (!buffer) {
		buffer = make_buffer<VectorBuffer>(0);
	}
	if (!data) {
		//! this is a nested structure
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), true);
		to_resize.emplace_back(arrays);
		FindChildren(to_resize, *auxiliary);
	} else {
		DataArrays arrays(*this, data, buffer.get(), GetTypeIdSize(GetType().InternalType()), false);
		to_resize.emplace_back(arrays);
	}
	for (auto &data_to_resize : to_resize) {
		if (!data_to_resize.is_nested) {
			auto new_data = make_unsafe_uniq_array<data_t>(new_size * data_to_resize.type_size);
			memcpy(new_data.get(), data_to_resize.data, cur_size * data_to_resize.type_size * sizeof(data_t));
			data_to_resize.buffer->SetData(std::move(new_data));
			data_to_resize.vec.data = data_to_resize.buffer->GetData();
		}
		data_to_resize.vec.validity.Resize(cur_size, new_size);
	}
}

void Vector::SetValue(idx_t index, const Value &val) {
	if (GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		// dictionary: apply dictionary and forward to child
		auto &sel_vector = DictionaryVector::SelVector(*this);
		auto &child = DictionaryVector::Child(*this);
		return child.SetValue(sel_vector.get_index(index), val);
	}
	if (val.type() != GetType()) {
		SetValue(index, val.DefaultCastAs(GetType()));
		return;
	}
	D_ASSERT(val.type().InternalType() == GetType().InternalType());

	validity.EnsureWritable();
	validity.Set(index, !val.IsNull());
	if (val.IsNull() && GetType().InternalType() != PhysicalType::STRUCT) {
		// for structs we still need to set the child-entries to NULL
		// so we do not bail out yet
		return;
	}

	switch (GetType().InternalType()) {
	case PhysicalType::BOOL:
		reinterpret_cast<bool *>(data)[index] = val.GetValueUnsafe<bool>();
		break;
	case PhysicalType::INT8:
		reinterpret_cast<int8_t *>(data)[index] = val.GetValueUnsafe<int8_t>();
		break;
	case PhysicalType::INT16:
		reinterpret_cast<int16_t *>(data)[index] = val.GetValueUnsafe<int16_t>();
		break;
	case PhysicalType::INT32:
		reinterpret_cast<int32_t *>(data)[index] = val.GetValueUnsafe<int32_t>();
		break;
	case PhysicalType::INT64:
		reinterpret_cast<int64_t *>(data)[index] = val.GetValueUnsafe<int64_t>();
		break;
	case PhysicalType::INT128:
		reinterpret_cast<hugeint_t *>(data)[index] = val.GetValueUnsafe<hugeint_t>();
		break;
	case PhysicalType::UINT8:
		reinterpret_cast<uint8_t *>(data)[index] = val.GetValueUnsafe<uint8_t>();
		break;
	case PhysicalType::UINT16:
		reinterpret_cast<uint16_t *>(data)[index] = val.GetValueUnsafe<uint16_t>();
		break;
	case PhysicalType::UINT32:
		reinterpret_cast<uint32_t *>(data)[index] = val.GetValueUnsafe<uint32_t>();
		break;
	case PhysicalType::UINT64:
		reinterpret_cast<uint64_t *>(data)[index] = val.GetValueUnsafe<uint64_t>();
		break;
	case PhysicalType::FLOAT:
		reinterpret_cast<float *>(data)[index] = val.GetValueUnsafe<float>();
		break;
	case PhysicalType::DOUBLE:
		reinterpret_cast<double *>(data)[index] = val.GetValueUnsafe<double>();
		break;
	case PhysicalType::INTERVAL:
		reinterpret_cast<interval_t *>(data)[index] = val.GetValueUnsafe<interval_t>();
		break;
	case PhysicalType::VARCHAR:
		reinterpret_cast<string_t *>(data)[index] = StringVector::AddStringOrBlob(*this, StringValue::Get(val));
		break;
	case PhysicalType::STRUCT: {
		D_ASSERT(GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR);

		auto &children = StructVector::GetEntries(*this);
		if (val.IsNull()) {
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				vec_child->SetValue(index, Value());
			}
		} else {
			auto &val_children = StructValue::GetChildren(val);
			D_ASSERT(children.size() == val_children.size());
			for (size_t i = 0; i < children.size(); i++) {
				auto &vec_child = children[i];
				auto &struct_child = val_children[i];
				vec_child->SetValue(index, struct_child);
			}
		}
		break;
	}
	case PhysicalType::LIST: {
		auto offset = ListVector::GetListSize(*this);
		auto &val_children = ListValue::GetChildren(val);
		if (!val_children.empty()) {
			for (idx_t i = 0; i < val_children.size(); i++) {
				ListVector::PushBack(*this, val_children[i]);
			}
		}
		//! now set the pointer
		auto &entry = reinterpret_cast<list_entry_t *>(data)[index];
		entry.length = val_children.size();
		entry.offset = offset;
		break;
	}
	default:
		throw InternalException("Unimplemented type for Vector::SetValue");
	}
}

Value Vector::GetValueInternal(const Vector &v_p, idx_t index_p) {
	const Vector *vector = &v_p;
	idx_t index = index_p;
	bool finished = false;
	while (!finished) {
		switch (vector->GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			index = 0;
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		case VectorType::FSST_VECTOR:
			finished = true;
			break;
		// dictionary: apply dictionary and forward to child
		case VectorType::DICTIONARY_VECTOR: {
			auto &sel_vector = DictionaryVector::SelVector(*vector);
			auto &child = DictionaryVector::Child(*vector);
			vector = &child;
			index = sel_vector.get_index(index);
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			SequenceVector::GetSequence(*vector, start, increment);
			return Value::Numeric(vector->GetType(), start + increment * index);
		}
		default:
			throw InternalException("Unimplemented vector type for Vector::GetValue");
		}
	}
	auto data = vector->data;
	auto &validity = vector->validity;
	auto &type = vector->GetType();

	if (!validity.RowIsValid(index)) {
		return Value(vector->GetType());
	}

	if (vector->GetVectorType() == VectorType::FSST_VECTOR) {
		if (vector->GetType().InternalType() != PhysicalType::VARCHAR) {
			throw InternalException("FSST Vector with non-string datatype found!");
		}
		auto str_compressed = reinterpret_cast<string_t *>(data)[index];
		Value result = FSSTPrimitives::DecompressValue(FSSTVector::GetDecoder(const_cast<Vector &>(*vector)),
		                                               str_compressed.GetData(), str_compressed.GetSize());
		return result;
	}

	switch (vector->GetType().id()) {
	case LogicalTypeId::BOOLEAN:
		return Value::BOOLEAN(reinterpret_cast<bool *>(data)[index]);
	case LogicalTypeId::TINYINT:
		return Value::TINYINT(reinterpret_cast<int8_t *>(data)[index]);
	case LogicalTypeId::SMALLINT:
		return Value::SMALLINT(reinterpret_cast<int16_t *>(data)[index]);
	case LogicalTypeId::INTEGER:
		return Value::INTEGER(reinterpret_cast<int32_t *>(data)[index]);
	case LogicalTypeId::DATE:
		return Value::DATE(reinterpret_cast<date_t *>(data)[index]);
	case LogicalTypeId::TIME:
		return Value::TIME(reinterpret_cast<dtime_t *>(data)[index]);
	case LogicalTypeId::TIME_TZ:
		return Value::TIMETZ(reinterpret_cast<dtime_tz_t *>(data)[index]);
	case LogicalTypeId::BIGINT:
		return Value::BIGINT(reinterpret_cast<int64_t *>(data)[index]);
	case LogicalTypeId::UTINYINT:
		return Value::UTINYINT(reinterpret_cast<uint8_t *>(data)[index]);
	case LogicalTypeId::USMALLINT:
		return Value::USMALLINT(reinterpret_cast<uint16_t *>(data)[index]);
	case LogicalTypeId::UINTEGER:
		return Value::UINTEGER(reinterpret_cast<uint32_t *>(data)[index]);
	case LogicalTypeId::UBIGINT:
		return Value::UBIGINT(reinterpret_cast<uint64_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP:
		return Value::TIMESTAMP(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_NS:
		return Value::TIMESTAMPNS(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_MS:
		return Value::TIMESTAMPMS(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::TIMESTAMP_TZ:
		return Value::TIMESTAMPTZ(reinterpret_cast<timestamp_t *>(data)[index]);
	case LogicalTypeId::HUGEINT:
		return Value::HUGEINT(reinterpret_cast<hugeint_t *>(data)[index]);
	case LogicalTypeId::UUID:
		return Value::UUID(reinterpret_cast<hugeint_t *>(data)[index]);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(reinterpret_cast<int16_t *>(data)[index], width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(reinterpret_cast<int32_t *>(data)[index], width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(reinterpret_cast<int64_t *>(data)[index], width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(reinterpret_cast<hugeint_t *>(data)[index], width, scale);
		default:
			throw InternalException("Physical type '%s' has a width bigger than 38, which is not supported",
			                        TypeIdToString(type.InternalType()));
		}
	}
	case LogicalTypeId::ENUM: {
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			return Value::ENUM(reinterpret_cast<uint8_t *>(data)[index], type);
		case PhysicalType::UINT16:
			return Value::ENUM(reinterpret_cast<uint16_t *>(data)[index], type);
		case PhysicalType::UINT32:
			return Value::ENUM(reinterpret_cast<uint32_t *>(data)[index], type);
		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
	}
	case LogicalTypeId::POINTER:
		return Value::POINTER(reinterpret_cast<uintptr_t *>(data)[index]);
	case LogicalTypeId::FLOAT:
		return Value::FLOAT(reinterpret_cast<float *>(data)[index]);
	case LogicalTypeId::DOUBLE:
		return Value::DOUBLE(reinterpret_cast<double *>(data)[index]);
	case LogicalTypeId::INTERVAL:
		return Value::INTERVAL(reinterpret_cast<interval_t *>(data)[index]);
	case LogicalTypeId::VARCHAR: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value(str.GetString());
	}
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::BLOB: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::BIT: {
		auto str = reinterpret_cast<string_t *>(data)[index];
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize());
	}
	case LogicalTypeId::MAP: {
		auto offlen = reinterpret_cast<list_entry_t *>(data)[index];
		auto &child_vec = ListVector::GetEntry(*vector);
		duckdb::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::MAP(ListType::GetChildType(type), std::move(children));
	}
	case LogicalTypeId::UNION: {
		auto tag = UnionVector::GetTag(*vector, index);
		auto value = UnionVector::GetMember(*vector, tag).GetValue(index);
		auto members = UnionType::CopyMemberTypes(type);
		return Value::UNION(members, tag, std::move(value));
	}
	case LogicalTypeId::STRUCT: {
		// we can derive the value schema from the vector schema
		auto &child_entries = StructVector::GetEntries(*vector);
		child_list_t<Value> children;
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			auto &struct_child = child_entries[child_idx];
			children.push_back(make_pair(StructType::GetChildName(type, child_idx), struct_child->GetValue(index_p)));
		}
		return Value::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST: {
		auto offlen = reinterpret_cast<list_entry_t *>(data)[index];
		auto &child_vec = ListVector::GetEntry(*vector);
		duckdb::vector<Value> children;
		for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
			children.push_back(child_vec.GetValue(i));
		}
		return Value::LIST(ListType::GetChildType(type), std::move(children));
	}
	default:
		throw InternalException("Unimplemented type for value access");
	}
}

Value Vector::GetValue(const Vector &v_p, idx_t index_p) {
	auto value = GetValueInternal(v_p, index_p);
	// set the alias of the type to the correct value, if there is a type alias
	if (v_p.GetType().HasAlias()) {
		value.GetTypeMutable().CopyAuxInfo(v_p.GetType());
	}
	if (v_p.GetType().id() != LogicalTypeId::AGGREGATE_STATE && value.type().id() != LogicalTypeId::AGGREGATE_STATE) {

		D_ASSERT(v_p.GetType() == value.type());
	}
	return value;
}

Value Vector::GetValue(idx_t index) const {
	return GetValue(*this, index);
}

// LCOV_EXCL_START
string VectorTypeToString(VectorType type) {
	switch (type) {
	case VectorType::FLAT_VECTOR:
		return "FLAT";
	case VectorType::FSST_VECTOR:
		return "FSST";
	case VectorType::SEQUENCE_VECTOR:
		return "SEQUENCE";
	case VectorType::DICTIONARY_VECTOR:
		return "DICTIONARY";
	case VectorType::CONSTANT_VECTOR:
		return "CONSTANT";
	default:
		return "UNKNOWN";
	}
}

string Vector::ToString(idx_t count) const {
	string retval =
	    VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": " + to_string(count) + " = [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
		break;
	case VectorType::FSST_VECTOR: {
		for (idx_t i = 0; i < count; i++) {
			string_t compressed_string = reinterpret_cast<string_t *>(data)[i];
			Value val = FSSTPrimitives::DecompressValue(FSSTVector::GetDecoder(const_cast<Vector &>(*this)),
			                                            compressed_string.GetData(), compressed_string.GetSize());
			retval += GetValue(i).ToString() + (i == count - 1 ? "" : ", ");
		}
	} break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);
		for (idx_t i = 0; i < count; i++) {
			retval += to_string(start + increment * i) + (i == count - 1 ? "" : ", ");
		}
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print(idx_t count) const {
	Printer::Print(ToString(count));
}

string Vector::ToString() const {
	string retval = VectorTypeToString(GetVectorType()) + " " + GetType().ToString() + ": (UNKNOWN COUNT) [ ";
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
	case VectorType::DICTIONARY_VECTOR:
		break;
	case VectorType::CONSTANT_VECTOR:
		retval += GetValue(0).ToString();
		break;
	case VectorType::SEQUENCE_VECTOR: {
		break;
	}
	default:
		retval += "UNKNOWN VECTOR TYPE";
		break;
	}
	retval += "]";
	return retval;
}

void Vector::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

template <class T>
static void TemplatedFlattenConstantVector(data_ptr_t data, data_ptr_t old_data, idx_t count) {
	auto constant = Load<T>(old_data);
	auto output = (T *)data;
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void Vector::Flatten(idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::FSST_VECTOR: {
		// Even though count may only be a part of the vector, we need to flatten the whole thing due to the way
		// ToUnifiedFormat uses flatten
		idx_t total_count = FSSTVector::GetCount(*this);
		// create vector to decompress into
		Vector other(GetType(), total_count);
		// now copy the data of this vector to the other vector, decompressing the strings in the process
		VectorOperations::Copy(*this, other, total_count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::DICTIONARY_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType(), count);
		// now copy the data of this vector to the other vector, removing the selection vector in the process
		VectorOperations::Copy(*this, other, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::CONSTANT_VECTOR: {
		bool is_null = ConstantVector::IsNull(*this);
		// allocate a new buffer for the vector
		auto old_buffer = std::move(buffer);
		auto old_data = data;
		buffer = VectorBuffer::CreateStandardVector(type, MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count));
		if (old_buffer) {
			D_ASSERT(buffer->GetAuxiliaryData() == nullptr);
			// The old buffer might be relying on the auxiliary data, keep it alive
			buffer->MoveAuxiliaryData(*old_buffer);
		}
		data = buffer->GetData();
		vector_type = VectorType::FLAT_VECTOR;
		if (is_null) {
			// constant NULL, set nullmask
			validity.EnsureWritable();
			validity.SetAllInvalid(count);
			return;
		}
		// non-null constant: have to repeat the constant
		switch (GetType().InternalType()) {
		case PhysicalType::BOOL:
			TemplatedFlattenConstantVector<bool>(data, old_data, count);
			break;
		case PhysicalType::INT8:
			TemplatedFlattenConstantVector<int8_t>(data, old_data, count);
			break;
		case PhysicalType::INT16:
			TemplatedFlattenConstantVector<int16_t>(data, old_data, count);
			break;
		case PhysicalType::INT32:
			TemplatedFlattenConstantVector<int32_t>(data, old_data, count);
			break;
		case PhysicalType::INT64:
			TemplatedFlattenConstantVector<int64_t>(data, old_data, count);
			break;
		case PhysicalType::UINT8:
			TemplatedFlattenConstantVector<uint8_t>(data, old_data, count);
			break;
		case PhysicalType::UINT16:
			TemplatedFlattenConstantVector<uint16_t>(data, old_data, count);
			break;
		case PhysicalType::UINT32:
			TemplatedFlattenConstantVector<uint32_t>(data, old_data, count);
			break;
		case PhysicalType::UINT64:
			TemplatedFlattenConstantVector<uint64_t>(data, old_data, count);
			break;
		case PhysicalType::INT128:
			TemplatedFlattenConstantVector<hugeint_t>(data, old_data, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedFlattenConstantVector<float>(data, old_data, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedFlattenConstantVector<double>(data, old_data, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedFlattenConstantVector<interval_t>(data, old_data, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedFlattenConstantVector<string_t>(data, old_data, count);
			break;
		case PhysicalType::LIST: {
			TemplatedFlattenConstantVector<list_entry_t>(data, old_data, count);
			break;
		}
		case PhysicalType::STRUCT: {
			auto normalified_buffer = make_uniq<VectorStructBuffer>();

			auto &new_children = normalified_buffer->GetChildren();

			auto &child_entries = StructVector::GetEntries(*this);
			for (auto &child : child_entries) {
				D_ASSERT(child->GetVectorType() == VectorType::CONSTANT_VECTOR);
				auto vector = make_uniq<Vector>(*child);
				vector->Flatten(count);
				new_children.push_back(std::move(vector));
			}
			auxiliary = shared_ptr<VectorBuffer>(normalified_buffer.release());
		} break;
		default:
			throw InternalException("Unimplemented type for VectorOperations::Flatten");
		}
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment, sequence_count;
		SequenceVector::GetSequence(*this, start, increment, sequence_count);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, sequence_count, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify");
	}
}

void Vector::Flatten(const SelectionVector &sel, idx_t count) {
	switch (GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		// already a flat vector
		break;
	case VectorType::FSST_VECTOR: {
		// create a new flat vector of this type
		Vector other(GetType());
		// copy the data of this vector to the other vector, removing compression and selection vector in the process
		VectorOperations::Copy(*this, other, sel, count, 0, 0);
		// create a reference to the data in the other vector
		this->Reference(other);
		break;
	}
	case VectorType::SEQUENCE_VECTOR: {
		int64_t start, increment;
		SequenceVector::GetSequence(*this, start, increment);

		buffer = VectorBuffer::CreateStandardVector(GetType());
		data = buffer->GetData();
		VectorOperations::GenerateSequence(*this, count, sel, start, increment);
		break;
	}
	default:
		throw InternalException("Unimplemented type for normalify with selection vector");
	}
}

void Vector::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) {
	switch (GetVectorType()) {
	case VectorType::DICTIONARY_VECTOR: {
		auto &sel = DictionaryVector::SelVector(*this);
		format.owned_sel.Initialize(sel);
		format.sel = &format.owned_sel;

		auto &child = DictionaryVector::Child(*this);
		if (child.GetVectorType() == VectorType::FLAT_VECTOR) {
			format.data = FlatVector::GetData(child);
			format.validity = FlatVector::Validity(child);
		} else {
			// dictionary with non-flat child: create a new reference to the child and flatten it
			Vector child_vector(child);
			child_vector.Flatten(sel, count);
			auto new_aux = make_buffer<VectorChildBuffer>(std::move(child_vector));

			format.data = FlatVector::GetData(new_aux->data);
			format.validity = FlatVector::Validity(new_aux->data);
			this->auxiliary = std::move(new_aux);
		}
		break;
	}
	case VectorType::CONSTANT_VECTOR:
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
		format.data = ConstantVector::GetData(*this);
		format.validity = ConstantVector::Validity(*this);
		break;
	default:
		Flatten(count);
		format.sel = FlatVector::IncrementalSelectionVector();
		format.data = FlatVector::GetData(*this);
		format.validity = FlatVector::Validity(*this);
		break;
	}
}

void Vector::RecursiveToUnifiedFormat(Vector &input, idx_t count, RecursiveUnifiedVectorFormat &data) {

	input.ToUnifiedFormat(count, data.unified);

	if (input.GetType().InternalType() == PhysicalType::LIST) {
		auto &child = ListVector::GetEntry(input);
		auto child_count = ListVector::GetListSize(input);
		data.children.emplace_back();
		Vector::RecursiveToUnifiedFormat(child, child_count, data.children.back());

	} else if (input.GetType().InternalType() == PhysicalType::STRUCT) {
		auto &children = StructVector::GetEntries(input);
		for (idx_t i = 0; i < children.size(); i++) {
			data.children.emplace_back();
		}
		for (idx_t i = 0; i < children.size(); i++) {
			Vector::RecursiveToUnifiedFormat(*children[i], count, data.children[i]);
		}
	}
}

void Vector::Sequence(int64_t start, int64_t increment, idx_t count) {
	this->vector_type = VectorType::SEQUENCE_VECTOR;
	this->buffer = make_buffer<VectorBuffer>(sizeof(int64_t) * 3);
	auto data = reinterpret_cast<int64_t *>(buffer->GetData());
	data[0] = start;
	data[1] = increment;
	data[2] = int64_t(count);
	validity.Reset();
	auxiliary.reset();
}

void Vector::Serialize(Serializer &serializer, idx_t count) {
	auto &logical_type = GetType();

	UnifiedVectorFormat vdata;
	ToUnifiedFormat(count, vdata);

	const bool all_valid = (count > 0) && !vdata.validity.AllValid();
	serializer.WriteProperty(100, "all_valid", all_valid);
	if (all_valid) {
		ValidityMask flat_mask(count);
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = vdata.sel->get_index(i);
			flat_mask.Set(i, vdata.validity.RowIsValid(row_idx));
		}
		serializer.WriteProperty(101, "validity", const_data_ptr_cast(flat_mask.GetData()),
		                         flat_mask.ValidityMaskSize(count));
	}
	if (TypeIsConstantSize(logical_type.InternalType())) {
		// constant size type: simple copy
		idx_t write_size = GetTypeIdSize(logical_type.InternalType()) * count;
		auto ptr = make_unsafe_uniq_array<data_t>(write_size);
		VectorOperations::WriteToStorage(*this, count, ptr.get());
		serializer.WriteProperty(102, "data", ptr.get(), write_size);
	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);

			// Serialize data as a list
			serializer.WriteList(102, "data", count, [&](Serializer::List &list, idx_t i) {
				auto idx = vdata.sel->get_index(i);
				auto str = !vdata.validity.RowIsValid(idx) ? NullValue<string_t>() : strings[idx];
				list.WriteElement(str);
			});
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);

			// Serialize entries as a list
			serializer.WriteList(103, "children", entries.size(), [&](Serializer::List &list, idx_t i) {
				list.WriteObject([&](Serializer &object) { entries[i]->Serialize(object, count); });
			});
			break;
		}
		case PhysicalType::LIST: {
			auto &child = ListVector::GetEntry(*this);
			auto list_size = ListVector::GetListSize(*this);

			// serialize the list entries in a flat array
			auto entries = make_unsafe_uniq_array<list_entry_t>(count);
			auto source_array = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				auto source = source_array[idx];
				entries[i].offset = source.offset;
				entries[i].length = source.length;
			}
			serializer.WriteProperty(104, "list_size", list_size);
			serializer.WriteList(105, "entries", count, [&](Serializer::List &list, idx_t i) {
				list.WriteObject([&](Serializer &object) {
					object.WriteProperty(100, "offset", entries[i].offset);
					object.WriteProperty(101, "length", entries[i].length);
				});
			});
			serializer.WriteObject(106, "child", [&](Serializer &object) { child.Serialize(object, list_size); });
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Serialize!");
		}
	}
}

void Vector::Deserialize(Deserializer &deserializer, idx_t count) {
	auto &logical_type = GetType();

	auto &validity = FlatVector::Validity(*this);
	validity.Reset();
	const auto has_validity = deserializer.ReadProperty<bool>(100, "all_valid");
	if (has_validity) {
		validity.Initialize(count);
		deserializer.ReadProperty(101, "validity", data_ptr_cast(validity.GetData()), validity.ValidityMaskSize(count));
	}

	if (TypeIsConstantSize(logical_type.InternalType())) {
		// constant size type: read fixed amount of data
		auto column_size = GetTypeIdSize(logical_type.InternalType()) * count;
		auto ptr = make_unsafe_uniq_array<data_t>(column_size);
		deserializer.ReadProperty(102, "data", ptr.get(), column_size);

		VectorOperations::ReadFromStorage(ptr.get(), count, *this);
	} else {
		switch (logical_type.InternalType()) {
		case PhysicalType::VARCHAR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			deserializer.ReadList(102, "data", [&](Deserializer::List &list, idx_t i) {
				auto str = list.ReadElement<string>();
				if (validity.RowIsValid(i)) {
					strings[i] = StringVector::AddStringOrBlob(*this, str);
				}
			});
			break;
		}
		case PhysicalType::STRUCT: {
			auto &entries = StructVector::GetEntries(*this);
			// Deserialize entries as a list
			deserializer.ReadList(103, "children", [&](Deserializer::List &list, idx_t i) {
				list.ReadObject([&](Deserializer &obj) { entries[i]->Deserialize(obj, count); });
			});
			break;
		}
		case PhysicalType::LIST: {
			// Read the list size
			auto list_size = deserializer.ReadProperty<uint64_t>(104, "list_size");
			ListVector::Reserve(*this, list_size);
			ListVector::SetListSize(*this, list_size);

			// Read the entries
			auto list_entries = FlatVector::GetData<list_entry_t>(*this);
			deserializer.ReadList(105, "entries", [&](Deserializer::List &list, idx_t i) {
				list.ReadObject([&](Deserializer &obj) {
					list_entries[i].offset = obj.ReadProperty<uint64_t>(100, "offset");
					list_entries[i].length = obj.ReadProperty<uint64_t>(101, "length");
				});
			});

			// Read the child vector
			deserializer.ReadObject(106, "child", [&](Deserializer &obj) {
				auto &child = ListVector::GetEntry(*this);
				child.Deserialize(obj, list_size);
			});
			break;
		}
		default:
			throw InternalException("Unimplemented variable width type for Vector::Deserialize!");
		}
	}
}

void Vector::SetVectorType(VectorType vector_type_p) {
	this->vector_type = vector_type_p;
	if (TypeIsConstantSize(GetType().InternalType()) &&
	    (GetVectorType() == VectorType::CONSTANT_VECTOR || GetVectorType() == VectorType::FLAT_VECTOR)) {
		auxiliary.reset();
	}
	if (vector_type == VectorType::CONSTANT_VECTOR && GetType().InternalType() == PhysicalType::STRUCT) {
		auto &entries = StructVector::GetEntries(*this);
		for (auto &entry : entries) {
			entry->SetVectorType(vector_type);
		}
	}
}

void Vector::UTFVerify(const SelectionVector &sel, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	if (GetType().InternalType() == PhysicalType::VARCHAR) {
		// we just touch all the strings and let the sanitizer figure out if any
		// of them are deallocated/corrupt
		switch (GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			auto string = ConstantVector::GetData<string_t>(*this);
			if (!ConstantVector::IsNull(*this)) {
				string->Verify();
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			auto strings = FlatVector::GetData<string_t>(*this);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel.get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].Verify();
				}
			}
			break;
		}
		default:
			break;
		}
	}
#endif
}

void Vector::UTFVerify(idx_t count) {
	auto flat_sel = FlatVector::IncrementalSelectionVector();

	UTFVerify(*flat_sel, count);
}

void Vector::VerifyMap(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::MAP);
	auto &child = ListType::GetChildType(vector_p.GetType());
	D_ASSERT(StructType::GetChildCount(child) == 2);
	D_ASSERT(StructType::GetChildName(child, 0) == "key");
	D_ASSERT(StructType::GetChildName(child, 1) == "value");

	auto valid_check = MapVector::CheckMapValidity(vector_p, count, sel_p);
	D_ASSERT(valid_check == MapInvalidReason::VALID);
#endif // DEBUG
}

void Vector::VerifyUnion(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	D_ASSERT(vector_p.GetType().id() == LogicalTypeId::UNION);
	auto valid_check = UnionVector::CheckUnionValidity(vector_p, count, sel_p);
	D_ASSERT(valid_check == UnionInvalidReason::VALID);
#endif // DEBUG
}

void Vector::Verify(Vector &vector_p, const SelectionVector &sel_p, idx_t count) {
#ifdef DEBUG
	if (count == 0) {
		return;
	}
	Vector *vector = &vector_p;
	const SelectionVector *sel = &sel_p;
	SelectionVector owned_sel;
	auto &type = vector->GetType();
	auto vtype = vector->GetVectorType();
	if (vector->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(*vector);
		D_ASSERT(child.GetVectorType() != VectorType::DICTIONARY_VECTOR);
		auto &dict_sel = DictionaryVector::SelVector(*vector);
		// merge the selection vectors and verify the child
		auto new_buffer = dict_sel.Slice(*sel, count);
		owned_sel.Initialize(new_buffer);
		sel = &owned_sel;
		vector = &child;
		vtype = vector->GetVectorType();
	}
	if (TypeIsConstantSize(type.InternalType()) &&
	    (vtype == VectorType::CONSTANT_VECTOR || vtype == VectorType::FLAT_VECTOR)) {
		D_ASSERT(!vector->auxiliary);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		// verify that the string is correct unicode
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					strings[oidx].Verify();
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (type.id() == LogicalTypeId::BIT) {
		switch (vtype) {
		case VectorType::FLAT_VECTOR: {
			auto &validity = FlatVector::Validity(*vector);
			auto strings = FlatVector::GetData<string_t>(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto oidx = sel->get_index(i);
				if (validity.RowIsValid(oidx)) {
					auto buf = strings[oidx].GetData();
					D_ASSERT(*buf >= 0 && *buf < 8);
					Bit::Verify(strings[oidx]);
				}
			}
			break;
		}
		default:
			break;
		}
	}

	if (type.InternalType() == PhysicalType::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		D_ASSERT(!child_types.empty());
		// create a selection vector of the non-null entries of the struct vector
		auto &children = StructVector::GetEntries(*vector);
		D_ASSERT(child_types.size() == children.size());
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			D_ASSERT(children[child_idx]->GetType() == child_types[child_idx].second);
			Vector::Verify(*children[child_idx], sel_p, count);
			if (vtype == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR);
				if (ConstantVector::IsNull(*vector)) {
					D_ASSERT(ConstantVector::IsNull(*children[child_idx]));
				}
			}
			if (vtype != VectorType::FLAT_VECTOR) {
				continue;
			}
			optional_ptr<ValidityMask> child_validity;
			SelectionVector owned_child_sel;
			const SelectionVector *child_sel = &owned_child_sel;
			if (children[child_idx]->GetVectorType() == VectorType::FLAT_VECTOR) {
				child_sel = FlatVector::IncrementalSelectionVector();
				child_validity = &FlatVector::Validity(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &child = DictionaryVector::Child(*children[child_idx]);
				if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
					continue;
				}
				child_validity = &FlatVector::Validity(child);
				child_sel = &DictionaryVector::SelVector(*children[child_idx]);
			} else if (children[child_idx]->GetVectorType() == VectorType::CONSTANT_VECTOR) {
				child_sel = ConstantVector::ZeroSelectionVector(count, owned_child_sel);
				child_validity = &ConstantVector::Validity(*children[child_idx]);
			} else {
				continue;
			}
			// for any NULL entry in the struct, the child should be NULL as well
			auto &validity = FlatVector::Validity(*vector);
			for (idx_t i = 0; i < count; i++) {
				auto index = sel->get_index(i);
				if (!validity.RowIsValid(index)) {
					auto child_index = child_sel->get_index(sel_p.get_index(i));
					D_ASSERT(!child_validity->RowIsValid(child_index));
				}
			}
		}

		if (vector->GetType().id() == LogicalTypeId::UNION) {
			VerifyUnion(*vector, *sel, count);
		}
	}

	if (type.InternalType() == PhysicalType::LIST) {
		if (vtype == VectorType::CONSTANT_VECTOR) {
			if (!ConstantVector::IsNull(*vector)) {
				auto &child = ListVector::GetEntry(*vector);
				SelectionVector child_sel(ListVector::GetListSize(*vector));
				idx_t child_count = 0;
				auto le = ConstantVector::GetData<list_entry_t>(*vector);
				D_ASSERT(le->offset + le->length <= ListVector::GetListSize(*vector));
				for (idx_t k = 0; k < le->length; k++) {
					child_sel.set_index(child_count++, le->offset + k);
				}
				Vector::Verify(child, child_sel, child_count);
			}
		} else if (vtype == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(*vector);
			auto &child = ListVector::GetEntry(*vector);
			auto child_size = ListVector::GetListSize(*vector);
			auto list_data = FlatVector::GetData<list_entry_t>(*vector);
			idx_t total_size = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					total_size += le.length;
				}
			}
			SelectionVector child_sel(total_size);
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				auto idx = sel->get_index(i);
				auto &le = list_data[idx];
				if (validity.RowIsValid(idx)) {
					D_ASSERT(le.offset + le.length <= child_size);
					for (idx_t k = 0; k < le.length; k++) {
						child_sel.set_index(child_count++, le.offset + k);
					}
				}
			}
			Vector::Verify(child, child_sel, child_count);
		}

		if (vector->GetType().id() == LogicalTypeId::MAP) {
			VerifyMap(*vector, *sel, count);
		}
	}
#endif
}

void Vector::Verify(idx_t count) {
	auto flat_sel = FlatVector::IncrementalSelectionVector();
	Verify(*this, *flat_sel, count);
}

//===--------------------------------------------------------------------===//
// FlatVector
//===--------------------------------------------------------------------===//
void FlatVector::SetNull(Vector &vector, idx_t idx, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	vector.validity.Set(idx, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			FlatVector::SetNull(*entry, idx, is_null);
		}
	}
}

//===--------------------------------------------------------------------===//
// ConstantVector
//===--------------------------------------------------------------------===//
void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.validity.Set(0, !is_null);
	if (is_null && vector.GetType().InternalType() == PhysicalType::STRUCT) {
		// set all child entries to null as well
		auto &entries = StructVector::GetEntries(vector);
		for (auto &entry : entries) {
			entry->SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(*entry, is_null);
		}
	}
}

const SelectionVector *ConstantVector::ZeroSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return ConstantVector::ZeroSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, 0);
	}
	return &owned_sel;
}

void ConstantVector::Reference(Vector &vector, Vector &source, idx_t position, idx_t count) {
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto list_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(list_index)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
		auto list_entry = list_data[list_index];

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetData<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetEntry(vector);
		child.Reference(ListVector::GetEntry(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	case PhysicalType::STRUCT: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(*target_entries[i], *source_entries[i], position, count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		vector.validity.Set(0, true);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		auto value = source.GetValue(position);
		vector.Reference(value);
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

//===--------------------------------------------------------------------===//
// StringVector
//===--------------------------------------------------------------------===//
string_t StringVector::AddString(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddString(vector, string_t(data, len));
}

string_t StringVector::AddStringOrBlob(Vector &vector, const char *data, idx_t len) {
	return StringVector::AddStringOrBlob(vector, string_t(data, len));
}

string_t StringVector::AddString(Vector &vector, const char *data) {
	return StringVector::AddString(vector, string_t(data, strlen(data)));
}

string_t StringVector::AddString(Vector &vector, const string &data) {
	return StringVector::AddString(vector, string_t(data.c_str(), data.size()));
}

string_t StringVector::AddString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::VARCHAR || vector.GetType().id() == LogicalTypeId::BIT);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = vector.auxiliary->Cast<VectorStringBuffer>();
	return string_buffer.AddString(data);
}

string_t StringVector::AddStringOrBlob(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = vector.auxiliary->Cast<VectorStringBuffer>();
	return string_buffer.AddBlob(data);
}

string_t StringVector::EmptyString(Vector &vector, idx_t len) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (len <= string_t::INLINE_LENGTH) {
		return string_t(len);
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRING_BUFFER);
	auto &string_buffer = vector.auxiliary->Cast<VectorStringBuffer>();
	return string_buffer.EmptyString(len);
}

void StringVector::AddHandle(Vector &vector, BufferHandle handle) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = vector.auxiliary->Cast<VectorStringBuffer>();
	string_buffer.AddHeapReference(make_buffer<ManagedVectorBuffer>(std::move(handle)));
}

void StringVector::AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(buffer.get() != vector.auxiliary.get());
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorStringBuffer>();
	}
	auto &string_buffer = vector.auxiliary->Cast<VectorStringBuffer>();
	string_buffer.AddHeapReference(std::move(buffer));
}

void StringVector::AddHeapReference(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(other.GetType().InternalType() == PhysicalType::VARCHAR);

	if (other.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		StringVector::AddHeapReference(vector, DictionaryVector::Child(other));
		return;
	}
	if (!other.auxiliary) {
		return;
	}
	StringVector::AddBuffer(vector, other.auxiliary);
}

//===--------------------------------------------------------------------===//
// FSSTVector
//===--------------------------------------------------------------------===//
string_t FSSTVector::AddCompressedString(Vector &vector, const char *data, idx_t len) {
	return FSSTVector::AddCompressedString(vector, string_t(data, len));
}

string_t FSSTVector::AddCompressedString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (data.IsInlined()) {
		// string will be inlined: no need to store in string heap
		return data;
	}
	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);
	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.AddBlob(data);
}

void *FSSTVector::GetDecoder(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (!vector.auxiliary) {
		throw InternalException("GetDecoder called on FSST Vector without registered buffer");
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);
	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.GetDecoder();
}

void FSSTVector::RegisterDecoder(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.AddDecoder(duckdb_fsst_decoder);
}

void FSSTVector::SetCount(Vector &vector, idx_t count) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.SetCount(count);
}

idx_t FSSTVector::GetCount(Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);

	if (!vector.auxiliary) {
		vector.auxiliary = make_buffer<VectorFSSTStringBuffer>();
	}
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::FSST_BUFFER);

	auto &fsst_string_buffer = vector.auxiliary->Cast<VectorFSSTStringBuffer>();
	return fsst_string_buffer.GetCount();
}

void FSSTVector::DecompressVector(const Vector &src, Vector &dst, idx_t src_offset, idx_t dst_offset, idx_t copy_count,
                                  const SelectionVector *sel) {
	D_ASSERT(src.GetVectorType() == VectorType::FSST_VECTOR);
	D_ASSERT(dst.GetVectorType() == VectorType::FLAT_VECTOR);
	auto dst_mask = FlatVector::Validity(dst);
	auto ldata = FSSTVector::GetCompressedData<string_t>(src);
	auto tdata = FlatVector::GetData<string_t>(dst);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel->get_index(src_offset + i);
		auto target_idx = dst_offset + i;
		string_t compressed_string = ldata[source_idx];
		if (dst_mask.RowIsValid(target_idx) && compressed_string.GetSize() > 0) {
			tdata[target_idx] = FSSTPrimitives::DecompressValue(
			    FSSTVector::GetDecoder(src), dst, compressed_string.GetData(), compressed_string.GetSize());
		} else {
			tdata[target_idx] = string_t(nullptr, 0);
		}
	}
}

//===--------------------------------------------------------------------===//
// MapVector
//===--------------------------------------------------------------------===//
Vector &MapVector::GetKeys(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return *entries[0];
}
Vector &MapVector::GetValues(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return *entries[1];
}

const Vector &MapVector::GetKeys(const Vector &vector) {
	return GetKeys((Vector &)vector);
}
const Vector &MapVector::GetValues(const Vector &vector) {
	return GetValues((Vector &)vector);
}

MapInvalidReason MapVector::CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel) {
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	UnifiedVectorFormat map_vdata;

	map.ToUnifiedFormat(count, map_vdata);
	auto &map_validity = map_vdata.validity;

	auto list_data = ListVector::GetData(map);
	auto &keys = MapVector::GetKeys(map);
	UnifiedVectorFormat key_vdata;
	keys.ToUnifiedFormat(count, key_vdata);
	auto &key_validity = key_vdata.validity;

	for (idx_t row = 0; row < count; row++) {
		auto mapped_row = sel.get_index(row);
		auto map_idx = map_vdata.sel->get_index(mapped_row);
		// map is allowed to be NULL
		if (!map_validity.RowIsValid(map_idx)) {
			continue;
		}
		value_set_t unique_keys;
		for (idx_t i = 0; i < list_data[map_idx].length; i++) {
			auto index = list_data[map_idx].offset + i;
			index = key_vdata.sel->get_index(index);
			if (!key_validity.RowIsValid(index)) {
				return MapInvalidReason::NULL_KEY;
			}
			auto value = keys.GetValue(index);
			auto result = unique_keys.insert(value);
			if (!result.second) {
				return MapInvalidReason::DUPLICATE_KEY;
			}
		}
	}
	return MapInvalidReason::VALID;
}

void MapVector::MapConversionVerify(Vector &vector, idx_t count) {
	auto valid_check = MapVector::CheckMapValidity(vector, count);
	switch (valid_check) {
	case MapInvalidReason::VALID:
		break;
	case MapInvalidReason::DUPLICATE_KEY: {
		throw InvalidInputException("Map keys have to be unique");
	}
	case MapInvalidReason::NULL_KEY: {
		throw InvalidInputException("Map keys can not be NULL");
	}
	case MapInvalidReason::NULL_KEY_LIST: {
		throw InvalidInputException("The list of map keys is not allowed to be NULL");
	}
	default: {
		throw InternalException("MapInvalidReason not implemented");
	}
	}
}

//===--------------------------------------------------------------------===//
// StructVector
//===--------------------------------------------------------------------===//
vector<unique_ptr<Vector>> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::UNION);

	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return StructVector::GetEntries(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.auxiliary->Cast<VectorStructBuffer>().GetChildren();
}

const vector<unique_ptr<Vector>> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

//===--------------------------------------------------------------------===//
// ListVector
//===--------------------------------------------------------------------===//
const Vector &ListVector::GetEntry(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return vector.auxiliary->Cast<VectorListBuffer>().GetChild();
}

Vector &ListVector::GetEntry(Vector &vector) {
	const Vector &cvector = vector;
	return const_cast<Vector &>(ListVector::GetEntry(cvector));
}

void ListVector::Reserve(Vector &vector, idx_t required_capacity) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = vector.auxiliary->Cast<VectorListBuffer>();
	child_buffer.Reserve(required_capacity);
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetSize();
}

idx_t ListVector::GetListCapacity(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetCapacity();
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = other.auxiliary;
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
	}
	vec.auxiliary->Cast<VectorListBuffer>().SetSize(size);
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.PushBack(insert);
}

idx_t ListVector::GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count) {

	auto info = ListVector::GetConsecutiveChildListInfo(list, offset, count);
	if (info.needs_slicing) {
		SelectionVector sel(info.child_list_info.length);
		ListVector::GetConsecutiveChildSelVector(list, sel, offset, count);

		result.Slice(sel, info.child_list_info.length);
		result.Flatten(info.child_list_info.length);
	}
	return info.child_list_info.length;
}

ConsecutiveChildListInfo ListVector::GetConsecutiveChildListInfo(Vector &list, idx_t offset, idx_t count) {

	ConsecutiveChildListInfo info;
	UnifiedVectorFormat unified_list_data;
	list.ToUnifiedFormat(offset + count, unified_list_data);
	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(unified_list_data);

	// find the first non-NULL entry
	idx_t first_length = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		info.child_list_info.offset = list_data[idx].offset;
		first_length = list_data[idx].length;
		break;
	}

	// small performance improvement for constant vectors
	// avoids iterating over all their (constant) elements
	if (list.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		info.child_list_info.length = first_length;
		return info;
	}

	// now get the child count and determine whether the children are stored consecutively
	// also determine if a flat vector has pseudo constant values (all offsets + length the same)
	// this can happen e.g. for UNNESTs
	bool is_consecutive = true;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		if (list_data[idx].offset != info.child_list_info.offset || list_data[idx].length != first_length) {
			info.is_constant = false;
		}
		if (list_data[idx].offset != info.child_list_info.offset + info.child_list_info.length) {
			is_consecutive = false;
		}
		info.child_list_info.length += list_data[idx].length;
	}

	if (info.is_constant) {
		info.child_list_info.length = first_length;
	}
	if (!info.is_constant && !is_consecutive) {
		info.needs_slicing = true;
	}

	return info;
}

void ListVector::GetConsecutiveChildSelVector(Vector &list, SelectionVector &sel, idx_t offset, idx_t count) {
	UnifiedVectorFormat unified_list_data;
	list.ToUnifiedFormat(offset + count, unified_list_data);
	auto list_data = UnifiedVectorFormat::GetData<list_entry_t>(unified_list_data);

	//	SelectionVector child_sel(info.second.length);
	idx_t entry = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto idx = unified_list_data.sel->get_index(i);
		if (!unified_list_data.validity.RowIsValid(idx)) {
			continue;
		}
		for (idx_t k = 0; k < list_data[idx].length; k++) {
			//			child_sel.set_index(entry++, list_data[idx].offset + k);
			sel.set_index(entry++, list_data[idx].offset + k);
		}
	}
	//
	//	result.Slice(child_sel, info.second.length);
	//	result.Flatten(info.second.length);
	//	info.second.offset = 0;
}

//===--------------------------------------------------------------------===//
// UnionVector
//===--------------------------------------------------------------------===//
const Vector &UnionVector::GetMember(const Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return *entries[member_index + 1]; // skip the "tag" entry
}

Vector &UnionVector::GetMember(Vector &vector, idx_t member_index) {
	D_ASSERT(member_index < UnionType::GetMemberCount(vector.GetType()));
	auto &entries = StructVector::GetEntries(vector);
	return *entries[member_index + 1]; // skip the "tag" entry
}

const Vector &UnionVector::GetTags(const Vector &vector) {
	// the tag vector is always the first struct child.
	return *StructVector::GetEntries(vector)[0];
}

Vector &UnionVector::GetTags(Vector &vector) {
	// the tag vector is always the first struct child.
	return *StructVector::GetEntries(vector)[0];
}

void UnionVector::SetToMember(Vector &union_vector, union_tag_t tag, Vector &member_vector, idx_t count,
                              bool keep_tags_for_null) {
	D_ASSERT(union_vector.GetType().id() == LogicalTypeId::UNION);
	D_ASSERT(tag < UnionType::GetMemberCount(union_vector.GetType()));

	// Set the union member to the specified vector
	UnionVector::GetMember(union_vector, tag).Reference(member_vector);
	auto &tag_vector = UnionVector::GetTags(union_vector);

	if (member_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// if the member vector is constant, we can set the union to constant as well
		union_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<union_tag_t>(tag_vector)[0] = tag;
		ConstantVector::SetNull(union_vector, ConstantVector::IsNull(member_vector));

	} else {
		// otherwise flatten and set to flatvector
		member_vector.Flatten(count);
		union_vector.SetVectorType(VectorType::FLAT_VECTOR);

		if (member_vector.validity.AllValid()) {
			// if the member vector is all valid, we can set the tag to constant
			tag_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			auto tag_data = ConstantVector::GetData<union_tag_t>(tag_vector);
			*tag_data = tag;
		} else {
			tag_vector.SetVectorType(VectorType::FLAT_VECTOR);
			if (keep_tags_for_null) {
				FlatVector::Validity(tag_vector).SetAllValid(count);
				FlatVector::Validity(union_vector).SetAllValid(count);
			} else {
				// ensure the tags have the same validity as the member
				FlatVector::Validity(union_vector) = FlatVector::Validity(member_vector);
				FlatVector::Validity(tag_vector) = FlatVector::Validity(member_vector);
			}

			auto tag_data = FlatVector::GetData<union_tag_t>(tag_vector);
			memset(tag_data, tag, count);
		}
	}

	// Set the non-selected members to constant null vectors
	for (idx_t i = 0; i < UnionType::GetMemberCount(union_vector.GetType()); i++) {
		if (i != tag) {
			auto &member = UnionVector::GetMember(union_vector, i);
			member.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(member, true);
		}
	}
}

union_tag_t UnionVector::GetTag(const Vector &vector, idx_t index) {
	// the tag vector is always the first struct child.
	auto &tag_vector = *StructVector::GetEntries(vector)[0];
	if (tag_vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(tag_vector);
		return FlatVector::GetData<union_tag_t>(child)[index];
	}
	if (tag_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::GetData<union_tag_t>(tag_vector)[0];
	}
	return FlatVector::GetData<union_tag_t>(tag_vector)[index];
}

UnionInvalidReason UnionVector::CheckUnionValidity(Vector &vector, idx_t count, const SelectionVector &sel) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::UNION);
	auto member_count = UnionType::GetMemberCount(vector.GetType());
	if (member_count == 0) {
		return UnionInvalidReason::NO_MEMBERS;
	}

	UnifiedVectorFormat union_vdata;
	vector.ToUnifiedFormat(count, union_vdata);

	UnifiedVectorFormat tags_vdata;
	auto &tag_vector = UnionVector::GetTags(vector);
	tag_vector.ToUnifiedFormat(count, tags_vdata);

	// check that only one member is valid at a time
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto union_mapped_row_idx = sel.get_index(row_idx);
		if (!union_vdata.validity.RowIsValid(union_mapped_row_idx)) {
			continue;
		}

		auto tag_mapped_row_idx = tags_vdata.sel->get_index(row_idx);
		if (!tags_vdata.validity.RowIsValid(tag_mapped_row_idx)) {
			continue;
		}

		auto tag = (UnifiedVectorFormat::GetData<union_tag_t>(tags_vdata))[tag_mapped_row_idx];
		if (tag >= member_count) {
			return UnionInvalidReason::TAG_OUT_OF_RANGE;
		}

		bool found_valid = false;
		for (idx_t member_idx = 0; member_idx < member_count; member_idx++) {

			UnifiedVectorFormat member_vdata;
			auto &member = UnionVector::GetMember(vector, member_idx);
			member.ToUnifiedFormat(count, member_vdata);

			auto mapped_row_idx = member_vdata.sel->get_index(row_idx);
			if (member_vdata.validity.RowIsValid(mapped_row_idx)) {
				if (found_valid) {
					return UnionInvalidReason::VALIDITY_OVERLAP;
				}
				found_valid = true;
				if (tag != static_cast<union_tag_t>(member_idx)) {
					return UnionInvalidReason::TAG_MISMATCH;
				}
			}
		}
	}

	return UnionInvalidReason::VALID;
}

} // namespace duckdb








namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, idx_t capacity) {
	return make_buffer<VectorBuffer>(capacity * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(const LogicalType &type) {
	return VectorBuffer::CreateConstantVector(type.InternalType());
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, idx_t capacity) {
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStringBuffer::VectorStringBuffer(VectorBufferType type) : VectorBuffer(type) {
}

VectorFSSTStringBuffer::VectorFSSTStringBuffer() : VectorStringBuffer(VectorBufferType::FSST_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, idx_t capacity)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		auto vector = make_uniq<Vector>(child_type.second, capacity);
		children.push_back(std::move(vector));
	}
}

VectorStructBuffer::VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
	auto &other_vector = StructVector::GetEntries(other);
	for (auto &child_vector : other_vector) {
		auto vector = make_uniq<Vector>(*child_vector, sel, count);
		children.push_back(std::move(vector));
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer(unique_ptr<Vector> vector, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), child(std::move(vector)), capacity(initial_capacity) {
}

VectorListBuffer::VectorListBuffer(const LogicalType &list_type, idx_t initial_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER),
      child(make_uniq<Vector>(ListType::GetChildType(list_type), initial_capacity)), capacity(initial_capacity) {
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > capacity) {
		idx_t new_capacity = NextPowerOfTwo(to_reserve);
		D_ASSERT(new_capacity >= to_reserve);
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(const Value &insert) {
	while (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

void VectorListBuffer::SetCapacity(idx_t new_capacity) {
	this->capacity = new_capacity;
}

void VectorListBuffer::SetSize(idx_t new_size) {
	this->size = new_size;
}

VectorListBuffer::~VectorListBuffer() {
}

ManagedVectorBuffer::ManagedVectorBuffer(BufferHandle handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(std::move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}

} // namespace duckdb





namespace duckdb {

class VectorCacheBuffer : public VectorBuffer {
public:
	explicit VectorCacheBuffer(Allocator &allocator, const LogicalType &type_p, idx_t capacity_p = STANDARD_VECTOR_SIZE)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), type(type_p), capacity(capacity_p) {
		auto internal_type = type.InternalType();
		switch (internal_type) {
		case PhysicalType::LIST: {
			// memory for the list offsets
			owned_data = allocator.Allocate(capacity * GetTypeIdSize(internal_type));
			// child data of the list
			auto &child_type = ListType::GetChildType(type);
			child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type, capacity));
			auto child_vector = make_uniq<Vector>(child_type, false, false);
			auxiliary = make_shared<VectorListBuffer>(std::move(child_vector));
			break;
		}
		case PhysicalType::STRUCT: {
			auto &child_types = StructType::GetChildTypes(type);
			for (auto &child_type : child_types) {
				child_caches.push_back(make_buffer<VectorCacheBuffer>(allocator, child_type.second, capacity));
			}
			auto struct_buffer = make_shared<VectorStructBuffer>(type);
			auxiliary = std::move(struct_buffer);
			break;
		}
		default:
			owned_data = allocator.Allocate(capacity * GetTypeIdSize(internal_type));
			break;
		}
	}

	void ResetFromCache(Vector &result, const buffer_ptr<VectorBuffer> &buffer) {
		D_ASSERT(type == result.GetType());
		auto internal_type = type.InternalType();
		result.vector_type = VectorType::FLAT_VECTOR;
		AssignSharedPointer(result.buffer, buffer);
		result.validity.Reset();
		switch (internal_type) {
		case PhysicalType::LIST: {
			result.data = owned_data.get();
			// reinitialize the VectorListBuffer
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through child
			auto &child_cache = child_caches[0]->Cast<VectorCacheBuffer>();
			auto &list_buffer = result.auxiliary->Cast<VectorListBuffer>();
			list_buffer.SetCapacity(child_cache.capacity);
			list_buffer.SetSize(0);
			list_buffer.SetAuxiliaryData(nullptr);

			auto &list_child = list_buffer.GetChild();
			child_cache.ResetFromCache(list_child, child_caches[0]);
			break;
		}
		case PhysicalType::STRUCT: {
			// struct does not have data
			result.data = nullptr;
			// reinitialize the VectorStructBuffer
			auxiliary->SetAuxiliaryData(nullptr);
			AssignSharedPointer(result.auxiliary, auxiliary);
			// propagate through children
			auto &children = result.auxiliary->Cast<VectorStructBuffer>().GetChildren();
			for (idx_t i = 0; i < children.size(); i++) {
				auto &child_cache = child_caches[i]->Cast<VectorCacheBuffer>();
				child_cache.ResetFromCache(*children[i], child_caches[i]);
			}
			break;
		}
		default:
			// regular type: no aux data and reset data to cached data
			result.data = owned_data.get();
			result.auxiliary.reset();
			break;
		}
	}

	const LogicalType &GetType() {
		return type;
	}

private:
	//! The type of the vector cache
	LogicalType type;
	//! Owned data
	AllocatedData owned_data;
	//! Child caches (if any). Used for nested types.
	vector<buffer_ptr<VectorBuffer>> child_caches;
	//! Aux data for the vector (if any)
	buffer_ptr<VectorBuffer> auxiliary;
	//! Capacity of the vector
	idx_t capacity;
};

VectorCache::VectorCache(Allocator &allocator, const LogicalType &type_p, idx_t capacity_p) {
	buffer = make_buffer<VectorCacheBuffer>(allocator, type_p, capacity_p);
}

void VectorCache::ResetFromCache(Vector &result) const {
	D_ASSERT(buffer);
	auto &vcache = buffer->Cast<VectorCacheBuffer>();
	vcache.ResetFromCache(result, buffer);
}

const LogicalType &VectorCache::GetType() const {
	auto &vcache = buffer->Cast<VectorCacheBuffer>();
	return vcache.GetType();
}

} // namespace duckdb


namespace duckdb {

const SelectionVector *ConstantVector::ZeroSelectionVector() {
	static const SelectionVector ZERO_SELECTION_VECTOR =
	    SelectionVector(const_cast<sel_t *>(ConstantVector::ZERO_VECTOR)); // NOLINT
	return &ZERO_SELECTION_VECTOR;
}

const SelectionVector *FlatVector::IncrementalSelectionVector() {
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
	return &INCREMENTAL_SELECTION_VECTOR;
}

const sel_t ConstantVector::ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};

} // namespace duckdb




























#include <cmath>

namespace duckdb {

LogicalType::LogicalType() : LogicalType(LogicalTypeId::INVALID) {
}

LogicalType::LogicalType(LogicalTypeId id) : id_(id) {
	physical_type_ = GetInternalType();
}
LogicalType::LogicalType(LogicalTypeId id, shared_ptr<ExtraTypeInfo> type_info_p)
    : id_(id), type_info_(std::move(type_info_p)) {
	physical_type_ = GetInternalType();
}

LogicalType::LogicalType(const LogicalType &other)
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(other.type_info_) {
}

LogicalType::LogicalType(LogicalType &&other) noexcept
    : id_(other.id_), physical_type_(other.physical_type_), type_info_(std::move(other.type_info_)) {
}

hash_t LogicalType::Hash() const {
	return duckdb::Hash<uint8_t>((uint8_t)id_);
}

PhysicalType LogicalType::GetInternalType() {
	switch (id_) {
	case LogicalTypeId::BOOLEAN:
		return PhysicalType::BOOL;
	case LogicalTypeId::TINYINT:
		return PhysicalType::INT8;
	case LogicalTypeId::UTINYINT:
		return PhysicalType::UINT8;
	case LogicalTypeId::SMALLINT:
		return PhysicalType::INT16;
	case LogicalTypeId::USMALLINT:
		return PhysicalType::UINT16;
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return PhysicalType::INT32;
	case LogicalTypeId::UINTEGER:
		return PhysicalType::UINT32;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return PhysicalType::INT64;
	case LogicalTypeId::UBIGINT:
		return PhysicalType::UINT64;
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UUID:
		return PhysicalType::INT128;
	case LogicalTypeId::FLOAT:
		return PhysicalType::FLOAT;
	case LogicalTypeId::DOUBLE:
		return PhysicalType::DOUBLE;
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return PhysicalType::INVALID;
		}
		auto width = DecimalType::GetWidth(*this);
		if (width <= Decimal::MAX_WIDTH_INT16) {
			return PhysicalType::INT16;
		} else if (width <= Decimal::MAX_WIDTH_INT32) {
			return PhysicalType::INT32;
		} else if (width <= Decimal::MAX_WIDTH_INT64) {
			return PhysicalType::INT64;
		} else if (width <= Decimal::MAX_WIDTH_INT128) {
			return PhysicalType::INT128;
		} else {
			throw InternalException("Decimal has a width of %d which is bigger than the maximum supported width of %d",
			                        width, DecimalType::MaxWidth());
		}
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
		return PhysicalType::VARCHAR;
	case LogicalTypeId::INTERVAL:
		return PhysicalType::INTERVAL;
	case LogicalTypeId::UNION:
	case LogicalTypeId::STRUCT:
		return PhysicalType::STRUCT;
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return PhysicalType::LIST;
	case LogicalTypeId::POINTER:
		// LCOV_EXCL_START
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			return PhysicalType::UINT32;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			return PhysicalType::UINT64;
		} else {
			throw InternalException("Unsupported pointer size");
		}
		// LCOV_EXCL_STOP
	case LogicalTypeId::VALIDITY:
		return PhysicalType::BIT;
	case LogicalTypeId::ENUM: {
		if (!type_info_) {
			return PhysicalType::INVALID;
		}
		return EnumType::GetPhysicalType(*this);
	}
	case LogicalTypeId::TABLE:
	case LogicalTypeId::LAMBDA:
	case LogicalTypeId::ANY:
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
		return PhysicalType::INVALID;
	case LogicalTypeId::USER:
		return PhysicalType::UNKNOWN;
	case LogicalTypeId::AGGREGATE_STATE:
		return PhysicalType::VARCHAR;
	default:
		throw InternalException("Invalid LogicalType %s", ToString());
	}
}

// **DEPRECATED**: Use EnumUtil directly instead.
string LogicalTypeIdToString(LogicalTypeId type) {
	return EnumUtil::ToString(type);
}

constexpr const LogicalTypeId LogicalType::INVALID;
constexpr const LogicalTypeId LogicalType::SQLNULL;
constexpr const LogicalTypeId LogicalType::BOOLEAN;
constexpr const LogicalTypeId LogicalType::TINYINT;
constexpr const LogicalTypeId LogicalType::UTINYINT;
constexpr const LogicalTypeId LogicalType::SMALLINT;
constexpr const LogicalTypeId LogicalType::USMALLINT;
constexpr const LogicalTypeId LogicalType::INTEGER;
constexpr const LogicalTypeId LogicalType::UINTEGER;
constexpr const LogicalTypeId LogicalType::BIGINT;
constexpr const LogicalTypeId LogicalType::UBIGINT;
constexpr const LogicalTypeId LogicalType::HUGEINT;
constexpr const LogicalTypeId LogicalType::UUID;
constexpr const LogicalTypeId LogicalType::FLOAT;
constexpr const LogicalTypeId LogicalType::DOUBLE;
constexpr const LogicalTypeId LogicalType::DATE;

constexpr const LogicalTypeId LogicalType::TIMESTAMP;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_MS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_NS;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_S;

constexpr const LogicalTypeId LogicalType::TIME;

constexpr const LogicalTypeId LogicalType::TIME_TZ;
constexpr const LogicalTypeId LogicalType::TIMESTAMP_TZ;

constexpr const LogicalTypeId LogicalType::HASH;
constexpr const LogicalTypeId LogicalType::POINTER;

constexpr const LogicalTypeId LogicalType::VARCHAR;

constexpr const LogicalTypeId LogicalType::BLOB;
constexpr const LogicalTypeId LogicalType::BIT;
constexpr const LogicalTypeId LogicalType::INTERVAL;
constexpr const LogicalTypeId LogicalType::ROW_TYPE;

// TODO these are incomplete and should maybe not exist as such
constexpr const LogicalTypeId LogicalType::TABLE;
constexpr const LogicalTypeId LogicalType::LAMBDA;

constexpr const LogicalTypeId LogicalType::ANY;

const vector<LogicalType> LogicalType::Numeric() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT,  LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,   LogicalType::FLOAT,
	                             LogicalType::DOUBLE,    LogicalTypeId::DECIMAL, LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER,  LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::Integral() {
	vector<LogicalType> types = {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,
	                             LogicalType::BIGINT,    LogicalType::HUGEINT,  LogicalType::UTINYINT,
	                             LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT};
	return types;
}

const vector<LogicalType> LogicalType::AllTypes() {
	vector<LogicalType> types = {
	    LogicalType::BOOLEAN,   LogicalType::TINYINT,  LogicalType::SMALLINT,  LogicalType::INTEGER,
	    LogicalType::BIGINT,    LogicalType::DATE,     LogicalType::TIMESTAMP, LogicalType::DOUBLE,
	    LogicalType::FLOAT,     LogicalType::VARCHAR,  LogicalType::BLOB,      LogicalType::BIT,
	    LogicalType::INTERVAL,  LogicalType::HUGEINT,  LogicalTypeId::DECIMAL, LogicalType::UTINYINT,
	    LogicalType::USMALLINT, LogicalType::UINTEGER, LogicalType::UBIGINT,   LogicalType::TIME,
	    LogicalTypeId::LIST,    LogicalTypeId::STRUCT, LogicalType::TIME_TZ,   LogicalType::TIMESTAMP_TZ,
	    LogicalTypeId::MAP,     LogicalTypeId::UNION,  LogicalType::UUID};
	return types;
}

const PhysicalType ROW_TYPE = PhysicalType::INT64;

// LCOV_EXCL_START
string TypeIdToString(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
		return "BOOL";
	case PhysicalType::INT8:
		return "INT8";
	case PhysicalType::INT16:
		return "INT16";
	case PhysicalType::INT32:
		return "INT32";
	case PhysicalType::INT64:
		return "INT64";
	case PhysicalType::UINT8:
		return "UINT8";
	case PhysicalType::UINT16:
		return "UINT16";
	case PhysicalType::UINT32:
		return "UINT32";
	case PhysicalType::UINT64:
		return "UINT64";
	case PhysicalType::INT128:
		return "INT128";
	case PhysicalType::FLOAT:
		return "FLOAT";
	case PhysicalType::DOUBLE:
		return "DOUBLE";
	case PhysicalType::VARCHAR:
		return "VARCHAR";
	case PhysicalType::INTERVAL:
		return "INTERVAL";
	case PhysicalType::STRUCT:
		return "STRUCT";
	case PhysicalType::LIST:
		return "LIST";
	case PhysicalType::INVALID:
		return "INVALID";
	case PhysicalType::BIT:
		return "BIT";
	case PhysicalType::UNKNOWN:
		return "UNKNOWN";
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

idx_t GetTypeIdSize(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
		return sizeof(int8_t);
	case PhysicalType::INT16:
		return sizeof(int16_t);
	case PhysicalType::INT32:
		return sizeof(int32_t);
	case PhysicalType::INT64:
		return sizeof(int64_t);
	case PhysicalType::UINT8:
		return sizeof(uint8_t);
	case PhysicalType::UINT16:
		return sizeof(uint16_t);
	case PhysicalType::UINT32:
		return sizeof(uint32_t);
	case PhysicalType::UINT64:
		return sizeof(uint64_t);
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	case PhysicalType::VARCHAR:
		return sizeof(string_t);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	case PhysicalType::STRUCT:
	case PhysicalType::UNKNOWN:
		return 0; // no own payload
	case PhysicalType::LIST:
		return sizeof(list_entry_t); // offset + len
	default:
		throw InternalException("Invalid PhysicalType for GetTypeIdSize");
	}
}

bool TypeIsConstantSize(PhysicalType type) {
	return (type >= PhysicalType::BOOL && type <= PhysicalType::DOUBLE) || type == PhysicalType::INTERVAL ||
	       type == PhysicalType::INT128;
}
bool TypeIsIntegral(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}
bool TypeIsNumeric(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::DOUBLE) || type == PhysicalType::INT128;
}
bool TypeIsInteger(PhysicalType type) {
	return (type >= PhysicalType::UINT8 && type <= PhysicalType::INT64) || type == PhysicalType::INT128;
}

string LogicalType::ToString() const {
	auto alias = GetAlias();
	if (!alias.empty()) {
		return alias;
	}
	switch (id_) {
	case LogicalTypeId::STRUCT: {
		if (!type_info_) {
			return "STRUCT";
		}
		auto &child_types = StructType::GetChildTypes(*this);
		string ret = "STRUCT(";
		for (size_t i = 0; i < child_types.size(); i++) {
			ret += StringUtil::Format("%s %s", SQLIdentifier(child_types[i].first), child_types[i].second);
			if (i < child_types.size() - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::LIST: {
		if (!type_info_) {
			return "LIST";
		}
		return ListType::GetChildType(*this).ToString() + "[]";
	}
	case LogicalTypeId::MAP: {
		if (!type_info_) {
			return "MAP";
		}
		auto &key_type = MapType::KeyType(*this);
		auto &value_type = MapType::ValueType(*this);
		return "MAP(" + key_type.ToString() + ", " + value_type.ToString() + ")";
	}
	case LogicalTypeId::UNION: {
		if (!type_info_) {
			return "UNION";
		}
		string ret = "UNION(";
		size_t count = UnionType::GetMemberCount(*this);
		for (size_t i = 0; i < count; i++) {
			ret += UnionType::GetMemberName(*this, i) + " " + UnionType::GetMemberType(*this, i).ToString();
			if (i < count - 1) {
				ret += ", ";
			}
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::DECIMAL: {
		if (!type_info_) {
			return "DECIMAL";
		}
		auto width = DecimalType::GetWidth(*this);
		auto scale = DecimalType::GetScale(*this);
		if (width == 0) {
			return "DECIMAL";
		}
		return StringUtil::Format("DECIMAL(%d,%d)", width, scale);
	}
	case LogicalTypeId::ENUM: {
		string ret = "ENUM(";
		for (idx_t i = 0; i < EnumType::GetSize(*this); i++) {
			if (i > 0) {
				ret += ", ";
			}
			ret += KeywordHelper::WriteQuoted(EnumType::GetString(*this, i).GetString(), '\'');
		}
		ret += ")";
		return ret;
	}
	case LogicalTypeId::USER: {
		return KeywordHelper::WriteOptionallyQuoted(UserType::GetTypeName(*this));
	}
	case LogicalTypeId::AGGREGATE_STATE: {
		return AggregateStateType::GetTypeName(*this);
	}
	default:
		return EnumUtil::ToString(id_);
	}
}
// LCOV_EXCL_STOP

LogicalTypeId TransformStringToLogicalTypeId(const string &str) {
	auto type = DefaultTypeGenerator::GetDefaultType(str);
	if (type == LogicalTypeId::INVALID) {
		// This is a User Type, at this point we don't know if its one of the User Defined Types or an error
		// It is checked in the binder
		type = LogicalTypeId::USER;
	}
	return type;
}

LogicalType TransformStringToLogicalType(const string &str) {
	if (StringUtil::Lower(str) == "null") {
		return LogicalType::SQLNULL;
	}
	return Parser::ParseColumnList("dummy " + str).GetColumn(LogicalIndex(0)).Type();
}

LogicalType GetUserTypeRecursive(const LogicalType &type, ClientContext &context) {
	if (type.id() == LogicalTypeId::USER && type.HasAlias()) {
		return Catalog::GetType(context, INVALID_CATALOG, INVALID_SCHEMA, type.GetAlias());
	}
	// Look for LogicalTypeId::USER in nested types
	if (type.id() == LogicalTypeId::STRUCT) {
		child_list_t<LogicalType> children;
		children.reserve(StructType::GetChildCount(type));
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, GetUserTypeRecursive(child.second, context));
		}
		return LogicalType::STRUCT(children);
	}
	if (type.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(GetUserTypeRecursive(ListType::GetChildType(type), context));
	}
	if (type.id() == LogicalTypeId::MAP) {
		return LogicalType::MAP(GetUserTypeRecursive(MapType::KeyType(type), context),
		                        GetUserTypeRecursive(MapType::ValueType(type), context));
	}
	// Not LogicalTypeId::USER or a nested type
	return type;
}

LogicalType TransformStringToLogicalType(const string &str, ClientContext &context) {
	return GetUserTypeRecursive(TransformStringToLogicalType(str), context);
}

bool LogicalType::IsIntegral() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::IsNumeric() const {
	switch (id_) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return true;
	default:
		return false;
	}
}

bool LogicalType::IsValid() const {
	return id() != LogicalTypeId::INVALID && id() != LogicalTypeId::UNKNOWN;
}

bool LogicalType::GetDecimalProperties(uint8_t &width, uint8_t &scale) const {
	switch (id_) {
	case LogicalTypeId::SQLNULL:
		width = 0;
		scale = 0;
		break;
	case LogicalTypeId::BOOLEAN:
		width = 1;
		scale = 0;
		break;
	case LogicalTypeId::TINYINT:
		// tinyint: [-127, 127] = DECIMAL(3,0)
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::SMALLINT:
		// smallint: [-32767, 32767] = DECIMAL(5,0)
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::INTEGER:
		// integer: [-2147483647, 2147483647] = DECIMAL(10,0)
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::BIGINT:
		// bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
		width = 19;
		scale = 0;
		break;
	case LogicalTypeId::UTINYINT:
		// UInt8 — [0 : 255]
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::USMALLINT:
		// UInt16 — [0 : 65535]
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::UINTEGER:
		// UInt32 — [0 : 4294967295]
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::UBIGINT:
		// UInt64 — [0 : 18446744073709551615]
		width = 20;
		scale = 0;
		break;
	case LogicalTypeId::HUGEINT:
		// hugeint: max size decimal (38, 0)
		// note that a hugeint is not guaranteed to fit in this
		width = 38;
		scale = 0;
		break;
	case LogicalTypeId::DECIMAL:
		width = DecimalType::GetWidth(*this);
		scale = DecimalType::GetScale(*this);
		break;
	default:
		// Nonsense values to ensure initialization
		width = 255u;
		scale = 255u;
		// FIXME(carlo): This should be probably a throw, requires checkign the various call-sites
		return false;
	}
	return true;
}

//! Grows Decimal width/scale when appropriate
static LogicalType DecimalSizeCheck(const LogicalType &left, const LogicalType &right) {
	D_ASSERT(left.id() == LogicalTypeId::DECIMAL || right.id() == LogicalTypeId::DECIMAL);
	D_ASSERT(left.id() != right.id());

	//! Make sure the 'right' is the DECIMAL type
	if (left.id() == LogicalTypeId::DECIMAL) {
		return DecimalSizeCheck(right, left);
	}
	auto width = DecimalType::GetWidth(right);
	auto scale = DecimalType::GetScale(right);

	uint8_t other_width;
	uint8_t other_scale;
	bool success = left.GetDecimalProperties(other_width, other_scale);
	if (!success) {
		throw InternalException("Type provided to DecimalSizeCheck was not a numeric type");
	}
	D_ASSERT(other_scale == 0);
	const auto effective_width = width - scale;
	if (other_width > effective_width) {
		auto new_width = other_width + scale;
		//! Cap the width at max, if an actual value exceeds this, an exception will be thrown later
		if (new_width > DecimalType::MaxWidth()) {
			new_width = DecimalType::MaxWidth();
		}
		return LogicalType::DECIMAL(new_width, scale);
	}
	return right;
}

static LogicalType CombineNumericTypes(const LogicalType &left, const LogicalType &right) {
	D_ASSERT(left.id() != right.id());
	if (left.id() > right.id()) {
		// this method is symmetric
		// arrange it so the left type is smaller to limit the number of options we need to check
		return CombineNumericTypes(right, left);
	}
	if (CastRules::ImplicitCast(left, right) >= 0) {
		// we can implicitly cast left to right, return right
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (right.id() == LogicalTypeId::DECIMAL) {
			return DecimalSizeCheck(left, right);
		}
		return right;
	}
	if (CastRules::ImplicitCast(right, left) >= 0) {
		// we can implicitly cast right to left, return left
		//! Depending on the type, we might need to grow the `width` of the DECIMAL type
		if (left.id() == LogicalTypeId::DECIMAL) {
			return DecimalSizeCheck(right, left);
		}
		return left;
	}
	// we can't cast implicitly either way and types are not equal
	// this happens when left is signed and right is unsigned
	// e.g. INTEGER and UINTEGER
	// in this case we need to upcast to make sure the types fit

	if (left.id() == LogicalTypeId::BIGINT || right.id() == LogicalTypeId::UBIGINT) {
		return LogicalType::HUGEINT;
	}
	if (left.id() == LogicalTypeId::INTEGER || right.id() == LogicalTypeId::UINTEGER) {
		return LogicalType::BIGINT;
	}
	if (left.id() == LogicalTypeId::SMALLINT || right.id() == LogicalTypeId::USMALLINT) {
		return LogicalType::INTEGER;
	}
	if (left.id() == LogicalTypeId::TINYINT || right.id() == LogicalTypeId::UTINYINT) {
		return LogicalType::SMALLINT;
	}
	throw InternalException("Cannot combine these numeric types!?");
}

LogicalType LogicalType::MaxLogicalType(const LogicalType &left, const LogicalType &right) {
	// we always prefer aliased types
	if (!left.GetAlias().empty()) {
		return left;
	}
	if (!right.GetAlias().empty()) {
		return right;
	}
	if (left.id() != right.id() && left.IsNumeric() && right.IsNumeric()) {
		return CombineNumericTypes(left, right);
	} else if (left.id() == LogicalTypeId::UNKNOWN) {
		return right;
	} else if (right.id() == LogicalTypeId::UNKNOWN) {
		return left;
	} else if ((right.id() == LogicalTypeId::ENUM || left.id() == LogicalTypeId::ENUM) && right.id() != left.id()) {
		// if one is an enum and the other is not, compare strings, not enums
		// see https://github.com/duckdb/duckdb/issues/8561
		return LogicalTypeId::VARCHAR;
	} else if (left.id() < right.id()) {
		return right;
	}
	if (right.id() < left.id()) {
		return left;
	}
	// Since both left and right are equal we get the left type as our type_id for checks
	auto type_id = left.id();
	if (type_id == LogicalTypeId::ENUM) {
		// If both types are different ENUMs we do a string comparison.
		return left == right ? left : LogicalType::VARCHAR;
	}
	if (type_id == LogicalTypeId::VARCHAR) {
		// varchar: use type that has collation (if any)
		if (StringType::GetCollation(right).empty()) {
			return left;
		}
		return right;
	}
	if (type_id == LogicalTypeId::DECIMAL) {
		// unify the width/scale so that the resulting decimal always fits
		// "width - scale" gives us the number of digits on the left side of the decimal point
		// "scale" gives us the number of digits allowed on the right of the decimal point
		// using the max of these of the two types gives us the new decimal size
		auto extra_width_left = DecimalType::GetWidth(left) - DecimalType::GetScale(left);
		auto extra_width_right = DecimalType::GetWidth(right) - DecimalType::GetScale(right);
		auto extra_width = MaxValue<uint8_t>(extra_width_left, extra_width_right);
		auto scale = MaxValue<uint8_t>(DecimalType::GetScale(left), DecimalType::GetScale(right));
		auto width = extra_width + scale;
		if (width > DecimalType::MaxWidth()) {
			// if the resulting decimal does not fit, we truncate the scale
			width = DecimalType::MaxWidth();
			scale = width - extra_width;
		}
		return LogicalType::DECIMAL(width, scale);
	}
	if (type_id == LogicalTypeId::LIST) {
		// list: perform max recursively on child type
		auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
		return LogicalType::LIST(new_child);
	}
	if (type_id == LogicalTypeId::MAP) {
		// list: perform max recursively on child type
		auto new_child = MaxLogicalType(ListType::GetChildType(left), ListType::GetChildType(right));
		return LogicalType::MAP(new_child);
	}
	if (type_id == LogicalTypeId::STRUCT) {
		// struct: perform recursively
		auto &left_child_types = StructType::GetChildTypes(left);
		auto &right_child_types = StructType::GetChildTypes(right);
		if (left_child_types.size() != right_child_types.size()) {
			// child types are not of equal size, we can't cast anyway
			// just return the left child
			return left;
		}
		child_list_t<LogicalType> child_types;
		for (idx_t i = 0; i < left_child_types.size(); i++) {
			auto child_type = MaxLogicalType(left_child_types[i].second, right_child_types[i].second);
			child_types.emplace_back(left_child_types[i].first, std::move(child_type));
		}

		return LogicalType::STRUCT(child_types);
	}
	if (type_id == LogicalTypeId::UNION) {
		auto left_member_count = UnionType::GetMemberCount(left);
		auto right_member_count = UnionType::GetMemberCount(right);
		if (left_member_count != right_member_count) {
			// return the "larger" type, with the most members
			return left_member_count > right_member_count ? left : right;
		}
		// otherwise, keep left, don't try to meld the two together.
		return left;
	}
	// types are equal but no extra specifier: just return the type
	return left;
}

void LogicalType::Verify() const {
#ifdef DEBUG
	if (id_ == LogicalTypeId::DECIMAL) {
		D_ASSERT(DecimalType::GetWidth(*this) >= 1 && DecimalType::GetWidth(*this) <= Decimal::MAX_WIDTH_DECIMAL);
		D_ASSERT(DecimalType::GetScale(*this) >= 0 && DecimalType::GetScale(*this) <= DecimalType::GetWidth(*this));
	}
#endif
}

bool ApproxEqual(float ldecimal, float rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::FloatIsFinite(ldecimal) || !Value::FloatIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	float epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

bool ApproxEqual(double ldecimal, double rdecimal) {
	if (Value::IsNan(ldecimal) && Value::IsNan(rdecimal)) {
		return true;
	}
	if (!Value::DoubleIsFinite(ldecimal) || !Value::DoubleIsFinite(rdecimal)) {
		return ldecimal == rdecimal;
	}
	double epsilon = std::fabs(rdecimal) * 0.01 + 0.00000001;
	return std::fabs(ldecimal - rdecimal) <= epsilon;
}

//===--------------------------------------------------------------------===//
// Extra Type Info
//===--------------------------------------------------------------------===//
void LogicalType::SetAlias(string alias) {
	if (!type_info_) {
		type_info_ = make_shared<ExtraTypeInfo>(ExtraTypeInfoType::GENERIC_TYPE_INFO, std::move(alias));
	} else {
		type_info_->alias = std::move(alias);
	}
}

string LogicalType::GetAlias() const {
	if (id() == LogicalTypeId::USER) {
		return UserType::GetTypeName(*this);
	}
	if (type_info_) {
		return type_info_->alias;
	}
	return string();
}

bool LogicalType::HasAlias() const {
	if (id() == LogicalTypeId::USER) {
		return !UserType::GetTypeName(*this).empty();
	}
	if (type_info_ && !type_info_->alias.empty()) {
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Decimal Type
//===--------------------------------------------------------------------===//
uint8_t DecimalType::GetWidth(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<DecimalTypeInfo>().width;
}

uint8_t DecimalType::GetScale(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<DecimalTypeInfo>().scale;
}

uint8_t DecimalType::MaxWidth() {
	return DecimalWidth<hugeint_t>::max;
}

LogicalType LogicalType::DECIMAL(int width, int scale) {
	D_ASSERT(width >= scale);
	auto type_info = make_shared<DecimalTypeInfo>(width, scale);
	return LogicalType(LogicalTypeId::DECIMAL, std::move(type_info));
}

//===--------------------------------------------------------------------===//
// String Type
//===--------------------------------------------------------------------===//
string StringType::GetCollation(const LogicalType &type) {
	if (type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	auto info = type.AuxInfo();
	if (!info) {
		return string();
	}
	if (info->type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
		return string();
	}
	return info->Cast<StringTypeInfo>().collation;
}

LogicalType LogicalType::VARCHAR_COLLATION(string collation) { // NOLINT
	auto string_info = make_shared<StringTypeInfo>(std::move(collation));
	return LogicalType(LogicalTypeId::VARCHAR, std::move(string_info));
}

//===--------------------------------------------------------------------===//
// List Type
//===--------------------------------------------------------------------===//
const LogicalType &ListType::GetChildType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<ListTypeInfo>().child_type;
}

LogicalType LogicalType::LIST(const LogicalType &child) {
	auto info = make_shared<ListTypeInfo>(child);
	return LogicalType(LogicalTypeId::LIST, std::move(info));
}

//===--------------------------------------------------------------------===//
// Aggregate State Type
//===--------------------------------------------------------------------===//
const aggregate_state_t &AggregateStateType::GetStateType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<AggregateStateTypeInfo>().state_type;
}

const string AggregateStateType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::AGGREGATE_STATE);
	auto info = type.AuxInfo();
	if (!info) {
		return "AGGREGATE_STATE<?>";
	}
	auto aggr_state = info->Cast<AggregateStateTypeInfo>().state_type;
	return "AGGREGATE_STATE<" + aggr_state.function_name + "(" +
	       StringUtil::Join(aggr_state.bound_argument_types, aggr_state.bound_argument_types.size(), ", ",
	                        [](const LogicalType &arg_type) { return arg_type.ToString(); }) +
	       ")" + "::" + aggr_state.return_type.ToString() + ">";
}

//===--------------------------------------------------------------------===//
// Struct Type
//===--------------------------------------------------------------------===//
const child_list_t<LogicalType> &StructType::GetChildTypes(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION);

	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<StructTypeInfo>().child_types;
}

const LogicalType &StructType::GetChildType(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].second;
}

const string &StructType::GetChildName(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	return child_types[index].first;
}

idx_t StructType::GetChildCount(const LogicalType &type) {
	return StructType::GetChildTypes(type).size();
}
bool StructType::IsUnnamed(const LogicalType &type) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(child_types.size() > 0);
	return child_types[0].first.empty();
}

LogicalType LogicalType::STRUCT(child_list_t<LogicalType> children) {
	auto info = make_shared<StructTypeInfo>(std::move(children));
	return LogicalType(LogicalTypeId::STRUCT, std::move(info));
}

LogicalType LogicalType::AGGREGATE_STATE(aggregate_state_t state_type) { // NOLINT
	auto info = make_shared<AggregateStateTypeInfo>(std::move(state_type));
	return LogicalType(LogicalTypeId::AGGREGATE_STATE, std::move(info));
}

//===--------------------------------------------------------------------===//
// Map Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::MAP(const LogicalType &child_p) {
	D_ASSERT(child_p.id() == LogicalTypeId::STRUCT);
	auto &children = StructType::GetChildTypes(child_p);
	D_ASSERT(children.size() == 2);

	// We do this to enforce that for every MAP created, the keys are called "key"
	// and the values are called "value"

	// This is done because for Vector the keys of the STRUCT are used in equality checks.
	// Vector::Reference will throw if the types don't match
	child_list_t<LogicalType> new_children(2);
	new_children[0] = children[0];
	new_children[0].first = "key";

	new_children[1] = children[1];
	new_children[1].first = "value";

	auto child = LogicalType::STRUCT(std::move(new_children));
	auto info = make_shared<ListTypeInfo>(child);
	return LogicalType(LogicalTypeId::MAP, std::move(info));
}

LogicalType LogicalType::MAP(LogicalType key, LogicalType value) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("key", std::move(key));
	child_types.emplace_back("value", std::move(value));
	return LogicalType::MAP(LogicalType::STRUCT(child_types));
}

const LogicalType &MapType::KeyType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return StructType::GetChildTypes(ListType::GetChildType(type))[0].second;
}

const LogicalType &MapType::ValueType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::MAP);
	return StructType::GetChildTypes(ListType::GetChildType(type))[1].second;
}

//===--------------------------------------------------------------------===//
// Union Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::UNION(child_list_t<LogicalType> members) {
	D_ASSERT(!members.empty());
	D_ASSERT(members.size() <= UnionType::MAX_UNION_MEMBERS);
	// union types always have a hidden "tag" field in front
	members.insert(members.begin(), {"", LogicalType::UTINYINT});
	auto info = make_shared<StructTypeInfo>(std::move(members));
	return LogicalType(LogicalTypeId::UNION, std::move(info));
}

const LogicalType &UnionType::GetMemberType(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	// skip the "tag" field
	return child_types[index + 1].second;
}

const string &UnionType::GetMemberName(const LogicalType &type, idx_t index) {
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(index < child_types.size());
	// skip the "tag" field
	return child_types[index + 1].first;
}

idx_t UnionType::GetMemberCount(const LogicalType &type) {
	// don't count the "tag" field
	return StructType::GetChildTypes(type).size() - 1;
}
const child_list_t<LogicalType> UnionType::CopyMemberTypes(const LogicalType &type) {
	auto child_types = StructType::GetChildTypes(type);
	child_types.erase(child_types.begin());
	return child_types;
}

//===--------------------------------------------------------------------===//
// User Type
//===--------------------------------------------------------------------===//
const string &UserType::GetTypeName(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::USER);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<UserTypeInfo>().user_type_name;
}

LogicalType LogicalType::USER(const string &user_type_name) {
	auto info = make_shared<UserTypeInfo>(user_type_name);
	return LogicalType(LogicalTypeId::USER, std::move(info));
}

//===--------------------------------------------------------------------===//
// Enum Type
//===--------------------------------------------------------------------===//
LogicalType LogicalType::ENUM(Vector &ordered_data, idx_t size) {
	return EnumTypeInfo::CreateType(ordered_data, size);
}

LogicalType LogicalType::ENUM(const string &enum_name, Vector &ordered_data, idx_t size) {
	return LogicalType::ENUM(ordered_data, size);
}

const string EnumType::GetValue(const Value &val) {
	auto info = val.type().AuxInfo();
	auto &values_insert_order = info->Cast<EnumTypeInfo>().GetValuesInsertOrder();
	return StringValue::Get(values_insert_order.GetValue(val.GetValue<uint32_t>()));
}

const Vector &EnumType::GetValuesInsertOrder(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<EnumTypeInfo>().GetValuesInsertOrder();
}

idx_t EnumType::GetSize(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto info = type.AuxInfo();
	D_ASSERT(info);
	return info->Cast<EnumTypeInfo>().GetDictSize();
}

PhysicalType EnumType::GetPhysicalType(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	auto aux_info = type.AuxInfo();
	D_ASSERT(aux_info);
	auto &info = aux_info->Cast<EnumTypeInfo>();
	D_ASSERT(info.GetEnumDictType() == EnumDictType::VECTOR_DICT);
	return EnumTypeInfo::DictType(info.GetDictSize());
}

//===--------------------------------------------------------------------===//
// Logical Type
//===--------------------------------------------------------------------===//

// the destructor needs to know about the extra type info
LogicalType::~LogicalType() {
}

bool LogicalType::EqualTypeInfo(const LogicalType &rhs) const {
	if (type_info_.get() == rhs.type_info_.get()) {
		return true;
	}
	if (type_info_) {
		return type_info_->Equals(rhs.type_info_.get());
	} else {
		D_ASSERT(rhs.type_info_);
		return rhs.type_info_->Equals(type_info_.get());
	}
}

bool LogicalType::operator==(const LogicalType &rhs) const {
	if (id_ != rhs.id_) {
		return false;
	}
	return EqualTypeInfo(rhs);
}

} // namespace duckdb





namespace duckdb {

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//

struct ValuePositionComparator {
	// Return true if the positional Values definitely match.
	// Default to the same as the final value
	template <typename OP>
	static inline bool Definite(const Value &lhs, const Value &rhs) {
		return Final<OP>(lhs, rhs);
	}

	// Select the positional Values that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static inline bool Possible(const Value &lhs, const Value &rhs) {
		return ValueOperations::NotDistinctFrom(lhs, rhs);
	}

	// Return true if the positional Values definitely match in the final position
	// This needs to be specialised.
	template <typename OP>
	static inline bool Final(const Value &lhs, const Value &rhs) {
		return false;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static inline bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos);
	}
};

// Equals must always check every column
template <>
inline bool ValuePositionComparator::Definite<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return false;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::Equals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// NotEquals must check everything that matched
template <>
inline bool ValuePositionComparator::Possible<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return true;
}

template <>
inline bool ValuePositionComparator::Final<duckdb::NotEquals>(const Value &lhs, const Value &rhs) {
	return ValueOperations::NotDistinctFrom(lhs, rhs);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
bool ValuePositionComparator::Definite<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Definite<duckdb::GreaterThan>(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThan>(const Value &lhs, const Value &rhs) {
	return ValueOperations::DistinctGreaterThan(lhs, rhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::LessThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Final<duckdb::GreaterThan>(lhs, rhs);
}

template <>
bool ValuePositionComparator::Definite<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Definite<duckdb::GreaterThan>(rhs, lhs);
}

template <>
bool ValuePositionComparator::Final<duckdb::GreaterThanEquals>(const Value &lhs, const Value &rhs) {
	return !ValuePositionComparator::Final<duckdb::GreaterThan>(rhs, lhs);
}

// Strict inequalities just use strict for both Definite and Final
template <>
bool ValuePositionComparator::Final<duckdb::LessThan>(const Value &lhs, const Value &rhs) {
	return ValuePositionComparator::Final<duckdb::GreaterThan>(rhs, lhs);
}

template <class OP>
static bool TemplatedBooleanOperation(const Value &left, const Value &right) {
	const auto &left_type = left.type();
	const auto &right_type = right.type();
	if (left_type != right_type) {
		Value left_copy = left;
		Value right_copy = right;

		LogicalType comparison_type = BoundComparisonExpression::BindComparison(left_type, right_type);
		if (!left_copy.DefaultTryCastAs(comparison_type) || !right_copy.DefaultTryCastAs(comparison_type)) {
			return false;
		}
		D_ASSERT(left_copy.type() == right_copy.type());
		return TemplatedBooleanOperation<OP>(left_copy, right_copy);
	}
	switch (left_type.InternalType()) {
	case PhysicalType::BOOL:
		return OP::Operation(left.GetValueUnsafe<bool>(), right.GetValueUnsafe<bool>());
	case PhysicalType::INT8:
		return OP::Operation(left.GetValueUnsafe<int8_t>(), right.GetValueUnsafe<int8_t>());
	case PhysicalType::INT16:
		return OP::Operation(left.GetValueUnsafe<int16_t>(), right.GetValueUnsafe<int16_t>());
	case PhysicalType::INT32:
		return OP::Operation(left.GetValueUnsafe<int32_t>(), right.GetValueUnsafe<int32_t>());
	case PhysicalType::INT64:
		return OP::Operation(left.GetValueUnsafe<int64_t>(), right.GetValueUnsafe<int64_t>());
	case PhysicalType::UINT8:
		return OP::Operation(left.GetValueUnsafe<uint8_t>(), right.GetValueUnsafe<uint8_t>());
	case PhysicalType::UINT16:
		return OP::Operation(left.GetValueUnsafe<uint16_t>(), right.GetValueUnsafe<uint16_t>());
	case PhysicalType::UINT32:
		return OP::Operation(left.GetValueUnsafe<uint32_t>(), right.GetValueUnsafe<uint32_t>());
	case PhysicalType::UINT64:
		return OP::Operation(left.GetValueUnsafe<uint64_t>(), right.GetValueUnsafe<uint64_t>());
	case PhysicalType::INT128:
		return OP::Operation(left.GetValueUnsafe<hugeint_t>(), right.GetValueUnsafe<hugeint_t>());
	case PhysicalType::FLOAT:
		return OP::Operation(left.GetValueUnsafe<float>(), right.GetValueUnsafe<float>());
	case PhysicalType::DOUBLE:
		return OP::Operation(left.GetValueUnsafe<double>(), right.GetValueUnsafe<double>());
	case PhysicalType::INTERVAL:
		return OP::Operation(left.GetValueUnsafe<interval_t>(), right.GetValueUnsafe<interval_t>());
	case PhysicalType::VARCHAR:
		return OP::Operation(StringValue::Get(left), StringValue::Get(right));
	case PhysicalType::STRUCT: {
		auto &left_children = StructValue::GetChildren(left);
		auto &right_children = StructValue::GetChildren(right);
		// this should be enforced by the type
		D_ASSERT(left_children.size() == right_children.size());
		idx_t i = 0;
		for (; i < left_children.size() - 1; ++i) {
			if (ValuePositionComparator::Definite<OP>(left_children[i], right_children[i])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[i], right_children[i])) {
				return false;
			}
		}
		return ValuePositionComparator::Final<OP>(left_children[i], right_children[i]);
	}
	case PhysicalType::LIST: {
		auto &left_children = ListValue::GetChildren(left);
		auto &right_children = ListValue::GetChildren(right);
		for (idx_t pos = 0;; ++pos) {
			if (pos == left_children.size() || pos == right_children.size()) {
				return ValuePositionComparator::TieBreak<OP>(left_children.size(), right_children.size());
			}
			if (ValuePositionComparator::Definite<OP>(left_children[pos], right_children[pos])) {
				return true;
			}
			if (!ValuePositionComparator::Possible<OP>(left_children[pos], right_children[pos])) {
				return false;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unimplemented type for value comparison");
	}
}

bool ValueOperations::Equals(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::NotEquals(const Value &left, const Value &right) {
	return !ValueOperations::Equals(left, right);
}

bool ValueOperations::GreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() || right.IsNull()) {
		throw InternalException("Comparison on NULL values");
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::GreaterThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThan(const Value &left, const Value &right) {
	return ValueOperations::GreaterThan(right, left);
}

bool ValueOperations::LessThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::GreaterThan(left, right);
}

bool ValueOperations::NotDistinctFrom(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return true;
	}
	if (left.IsNull() != right.IsNull()) {
		return false;
	}
	return TemplatedBooleanOperation<duckdb::Equals>(left, right);
}

bool ValueOperations::DistinctFrom(const Value &left, const Value &right) {
	return !ValueOperations::NotDistinctFrom(left, right);
}

bool ValueOperations::DistinctGreaterThan(const Value &left, const Value &right) {
	if (left.IsNull() && right.IsNull()) {
		return false;
	} else if (right.IsNull()) {
		return false;
	} else if (left.IsNull()) {
		return true;
	}
	return TemplatedBooleanOperation<duckdb::GreaterThan>(left, right);
}

bool ValueOperations::DistinctGreaterThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThan(const Value &left, const Value &right) {
	return ValueOperations::DistinctGreaterThan(right, left);
}

bool ValueOperations::DistinctLessThanEquals(const Value &left, const Value &right) {
	return !ValueOperations::DistinctGreaterThan(left, right);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// boolean_operators.cpp
// Description: This file contains the implementation of the boolean
// operations AND OR !
//===--------------------------------------------------------------------===//





namespace duckdb {

//===--------------------------------------------------------------------===//
// AND/OR
//===--------------------------------------------------------------------===//
template <class OP>
static void TemplatedBooleanNullmask(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType().id() == LogicalTypeId::BOOLEAN && right.GetType().id() == LogicalTypeId::BOOLEAN &&
	         result.GetType().id() == LogicalTypeId::BOOLEAN);

	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// operation on two constants, result is constant vector
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto ldata = ConstantVector::GetData<uint8_t>(left);
		auto rdata = ConstantVector::GetData<uint8_t>(right);
		auto result_data = ConstantVector::GetData<bool>(result);

		bool is_null = OP::Operation(*ldata > 0, *rdata > 0, ConstantVector::IsNull(left),
		                             ConstantVector::IsNull(right), *result_data);
		ConstantVector::SetNull(result, is_null);
	} else {
		// perform generic loop
		UnifiedVectorFormat ldata, rdata;
		left.ToUnifiedFormat(count, ldata);
		right.ToUnifiedFormat(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto left_data = UnifiedVectorFormat::GetData<uint8_t>(ldata); // we use uint8 to avoid load of gunk bools
		auto right_data = UnifiedVectorFormat::GetData<uint8_t>(rdata);
		auto result_data = FlatVector::GetData<bool>(result);
		auto &result_mask = FlatVector::Validity(result);
		if (!ldata.validity.AllValid() || !rdata.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				bool is_null =
				    OP::Operation(left_data[lidx] > 0, right_data[ridx] > 0, !ldata.validity.RowIsValid(lidx),
				                  !rdata.validity.RowIsValid(ridx), result_data[i]);
				result_mask.Set(i, !is_null);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto lidx = ldata.sel->get_index(i);
				auto ridx = rdata.sel->get_index(i);
				result_data[i] = OP::SimpleOperation(left_data[lidx], right_data[ridx]);
			}
		}
	}
}

/*
SQL AND Rules:

TRUE  AND TRUE   = TRUE
TRUE  AND FALSE  = FALSE
TRUE  AND NULL   = NULL
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
FALSE AND NULL   = FALSE
NULL  AND TRUE   = NULL
NULL  AND FALSE  = FALSE
NULL  AND NULL   = NULL

Basically:
- Only true if both are true
- False if either is false (regardless of NULLs)
- NULL otherwise
*/
struct TernaryAnd {
	static bool SimpleOperation(bool left, bool right) {
		return left && right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is FALSE if right is false
			// result is NULL if right is true
			result = right;
			return right;
		} else if (right_null) {
			// right is NULL:
			// result is FALSE if left is false
			// result is NULL if left is true
			result = left;
			return left;
		} else {
			// no NULL: perform the AND
			result = left && right;
			return false;
		}
	}
};

void VectorOperations::And(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryAnd>(left, right, result, count);
}

/*
SQL OR Rules:

OR
TRUE  OR TRUE  = TRUE
TRUE  OR FALSE = TRUE
TRUE  OR NULL  = TRUE
FALSE OR TRUE  = TRUE
FALSE OR FALSE = FALSE
FALSE OR NULL  = NULL
NULL  OR TRUE  = TRUE
NULL  OR FALSE = NULL
NULL  OR NULL  = NULL

Basically:
- Only false if both are false
- True if either is true (regardless of NULLs)
- NULL otherwise
*/

struct TernaryOr {
	static bool SimpleOperation(bool left, bool right) {
		return left || right;
	}
	static bool Operation(bool left, bool right, bool left_null, bool right_null, bool &result) {
		if (left_null && right_null) {
			// both NULL:
			// result is NULL
			return true;
		} else if (left_null) {
			// left is NULL:
			// result is TRUE if right is true
			// result is NULL if right is false
			result = right;
			return !right;
		} else if (right_null) {
			// right is NULL:
			// result is TRUE if left is true
			// result is NULL if left is false
			result = left;
			return !left;
		} else {
			// no NULL: perform the OR
			result = left || right;
			return false;
		}
	}
};

void VectorOperations::Or(Vector &left, Vector &right, Vector &result, idx_t count) {
	TemplatedBooleanNullmask<TernaryOr>(left, right, result, count);
}

struct NotOperator {
	template <class TA, class TR>
	static inline TR Operation(TA left) {
		return !left;
	}
};

void VectorOperations::Not(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(input.GetType() == LogicalType::BOOLEAN && result.GetType() == LogicalType::BOOLEAN);
	UnaryExecutor::Execute<bool, bool, NotOperator>(input, result, count);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// comparison_operators.cpp
// Description: This file contains the implementation of the comparison
// operations == != >= <= > <
//===--------------------------------------------------------------------===//








namespace duckdb {

template <class T>
bool EqualsFloat(T left, T right) {
	if (DUCKDB_UNLIKELY(Value::IsNan(left) && Value::IsNan(right))) {
		return true;
	}
	return left == right;
}

template <>
bool Equals::Operation(const float &left, const float &right) {
	return EqualsFloat<float>(left, right);
}

template <>
bool Equals::Operation(const double &left, const double &right) {
	return EqualsFloat<double>(left, right);
}

template <class T>
bool GreaterThanFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no number that is bigger than right
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return false;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left > right;
}

template <>
bool GreaterThan::Operation(const float &left, const float &right) {
	return GreaterThanFloat<float>(left, right);
}

template <>
bool GreaterThan::Operation(const double &left, const double &right) {
	return GreaterThanFloat<double>(left, right);
}

template <class T>
bool GreaterThanEqualsFloat(T left, T right) {
	// handle nans
	// nan is always bigger than everything else
	bool left_is_nan = Value::IsNan(left);
	bool right_is_nan = Value::IsNan(right);
	// if right is nan, there is no bigger number
	// we only return true if left is also nan (in which case the numbers are equal)
	if (DUCKDB_UNLIKELY(right_is_nan)) {
		return left_is_nan;
	}
	// if left is nan, but right is not, left is always bigger
	if (DUCKDB_UNLIKELY(left_is_nan)) {
		return true;
	}
	return left >= right;
}

template <>
bool GreaterThanEquals::Operation(const float &left, const float &right) {
	return GreaterThanEqualsFloat<float>(left, right);
}

template <>
bool GreaterThanEquals::Operation(const double &left, const double &right) {
	return GreaterThanEqualsFloat<double>(left, right);
}

struct ComparisonSelector {
	template <typename OP>
	static idx_t Select(Vector &left, Vector &right, const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                    SelectionVector *false_sel) {
		throw NotImplementedException("Unknown comparison operation!");
	}
};

template <>
inline idx_t ComparisonSelector::Select<duckdb::Equals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                        idx_t count, SelectionVector *true_sel,
                                                        SelectionVector *false_sel) {
	return VectorOperations::Equals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::NotEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                           idx_t count, SelectionVector *true_sel,
                                                           SelectionVector *false_sel) {
	return VectorOperations::NotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel) {
	return VectorOperations::GreaterThan(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::GreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector *sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector *false_sel) {
	return VectorOperations::GreaterThanEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThan>(Vector &left, Vector &right, const SelectionVector *sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel) {
	return VectorOperations::GreaterThan(right, left, sel, count, true_sel, false_sel);
}

template <>
inline idx_t ComparisonSelector::Select<duckdb::LessThanEquals>(Vector &left, Vector &right, const SelectionVector *sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel) {
	return VectorOperations::GreaterThanEquals(right, left, sel, count, true_sel, false_sel);
}

static void ComparesNotNull(UnifiedVectorFormat &ldata, UnifiedVectorFormat &rdata, ValidityMask &vresult,
                            idx_t count) {
	for (idx_t i = 0; i < count; ++i) {
		auto lidx = ldata.sel->get_index(i);
		auto ridx = rdata.sel->get_index(i);
		if (!ldata.validity.RowIsValid(lidx) || !rdata.validity.RowIsValid(ridx)) {
			vresult.SetInvalid(i);
		}
	}
}

template <typename OP>
static void NestedComparisonExecutor(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if ((left_constant && ConstantVector::IsNull(left)) || (right_constant && ConstantVector::IsNull(right))) {
		// either left or right is constant NULL: result is constant NULL
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	if (left_constant && right_constant) {
		// both sides are constant, and neither is NULL so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		SelectionVector true_sel(1);
		auto match_count = ComparisonSelector::Select<OP>(left, right, nullptr, 1, &true_sel, nullptr);
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = match_count > 0;
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	auto &result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat leftv, rightv;
	left.ToUnifiedFormat(count, leftv);
	right.ToUnifiedFormat(count, rightv);
	if (!leftv.validity.AllValid() || !rightv.validity.AllValid()) {
		ComparesNotNull(leftv, rightv, result_validity, count);
	}
	SelectionVector true_sel(count);
	SelectionVector false_sel(count);
	idx_t match_count = ComparisonSelector::Select<OP>(left, right, nullptr, count, &true_sel, &false_sel);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

struct ComparisonExecutor {
private:
	template <class T, class OP>
	static inline void TemplatedExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::Execute<T, T, bool, OP>(left, right, result, count);
	}

public:
	template <class OP>
	static inline void Execute(Vector &left, Vector &right, Vector &result, idx_t count) {
		D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
		// the inplace loops take the result as the last parameter
		switch (left.GetType().InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedExecute<int8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT16:
			TemplatedExecute<int16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT32:
			TemplatedExecute<int32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT64:
			TemplatedExecute<int64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT8:
			TemplatedExecute<uint8_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT16:
			TemplatedExecute<uint16_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT32:
			TemplatedExecute<uint32_t, OP>(left, right, result, count);
			break;
		case PhysicalType::UINT64:
			TemplatedExecute<uint64_t, OP>(left, right, result, count);
			break;
		case PhysicalType::INT128:
			TemplatedExecute<hugeint_t, OP>(left, right, result, count);
			break;
		case PhysicalType::FLOAT:
			TemplatedExecute<float, OP>(left, right, result, count);
			break;
		case PhysicalType::DOUBLE:
			TemplatedExecute<double, OP>(left, right, result, count);
			break;
		case PhysicalType::INTERVAL:
			TemplatedExecute<interval_t, OP>(left, right, result, count);
			break;
		case PhysicalType::VARCHAR:
			TemplatedExecute<string_t, OP>(left, right, result, count);
			break;
		case PhysicalType::LIST:
		case PhysicalType::STRUCT:
			NestedComparisonExecutor<OP>(left, right, result, count);
			break;
		default:
			throw InternalException("Invalid type for comparison");
		}
	}
};

void VectorOperations::Equals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::Equals>(left, right, result, count);
}

void VectorOperations::NotEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::NotEquals>(left, right, result, count);
}

void VectorOperations::GreaterThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(left, right, result, count);
}

void VectorOperations::LessThanEquals(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThanEquals>(right, left, result, count);
}

void VectorOperations::GreaterThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(left, right, result, count);
}

void VectorOperations::LessThan(Vector &left, Vector &right, Vector &result, idx_t count) {
	ComparisonExecutor::Execute<duckdb::GreaterThan>(right, left, result, count);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// generators.cpp
// Description: This file contains the implementation of different generators
//===--------------------------------------------------------------------===//





namespace duckdb {

template <class T>
void TemplatedGenerateSequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	D_ASSERT(result.GetType().IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		if (i > 0) {
			value += increment;
		}
		result_data[i] = value;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, int64_t start, int64_t increment) {
	if (!result.GetType().IsNumeric()) {
		throw InvalidTypeException(result.GetType(), "Can only generate sequences for numeric values!");
	}
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedGenerateSequence<int8_t>(result, count, start, increment);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateSequence<int16_t>(result, count, start, increment);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateSequence<int32_t>(result, count, start, increment);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateSequence<int64_t>(result, count, start, increment);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateSequence<float>(result, count, start, increment);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateSequence<double>(result, count, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

template <class T>
void TemplatedGenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                               int64_t increment) {
	D_ASSERT(result.GetType().IsNumeric());
	if (start > NumericLimits<T>::Maximum() || increment > NumericLimits<T>::Maximum()) {
		throw Exception("Sequence start or increment out of type range");
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<T>(result);
	auto value = (T)start;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		result_data[idx] = value + increment * idx;
	}
}

void VectorOperations::GenerateSequence(Vector &result, idx_t count, const SelectionVector &sel, int64_t start,
                                        int64_t increment) {
	if (!result.GetType().IsNumeric()) {
		throw InvalidTypeException(result.GetType(), "Can only generate sequences for numeric values!");
	}
	switch (result.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedGenerateSequence<int8_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT16:
		TemplatedGenerateSequence<int16_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT32:
		TemplatedGenerateSequence<int32_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::INT64:
		TemplatedGenerateSequence<int64_t>(result, count, sel, start, increment);
		break;
	case PhysicalType::FLOAT:
		TemplatedGenerateSequence<float>(result, count, sel, start, increment);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGenerateSequence<double>(result, count, sel, start, increment);
		break;
	default:
		throw NotImplementedException("Unimplemented type for generate sequence");
	}
}

} // namespace duckdb



namespace duckdb {

struct DistinctBinaryLambdaWrapper {
	template <class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(LEFT_TYPE left, RIGHT_TYPE right, bool is_left_null, bool is_right_null) {
		return OP::template Operation<LEFT_TYPE>(left, right, is_left_null, is_right_null);
	}
};

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                       RESULT_TYPE *__restrict result_data, const SelectionVector *__restrict lsel,
                                       const SelectionVector *__restrict rsel, idx_t count, ValidityMask &lmask,
                                       ValidityMask &rmask, ValidityMask &result_mask) {
	for (idx_t i = 0; i < count; i++) {
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		auto lentry = ldata[lindex];
		auto rentry = rdata[rindex];
		result_data[i] =
		    OP::template Operation<LEFT_TYPE>(lentry, rentry, !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteConstant(Vector &left, Vector &right, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);

	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);
	auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
	*result_data =
	    OP::template Operation<LEFT_TYPE>(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right));
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteGeneric(Vector &left, Vector &right, Vector &result, idx_t count) {
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		DistinctExecuteConstant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result);
	} else {
		UnifiedVectorFormat ldata, rdata;

		left.ToUnifiedFormat(count, ldata);
		right.ToUnifiedFormat(count, rdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
		DistinctExecuteGenericLoop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(
		    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata),
		    result_data, ldata.sel, rdata.sel, count, ldata.validity, rdata.validity, FlatVector::Validity(result));
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecuteSwitch(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteGeneric<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP>
static void DistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecuteSwitch<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP>(left, right, result, count);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t
DistinctSelectGenericLoop(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                          const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                          const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                          ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto result_idx = result_sel->get_index(i);
		auto lindex = lsel->get_index(i);
		auto rindex = rsel->get_index(i);
		if (NO_NULL) {
			if (OP::Operation(ldata[lindex], rdata[rindex], false, false)) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		} else {
			if (OP::Operation(ldata[lindex], rdata[rindex], !lmask.RowIsValid(lindex), !rmask.RowIsValid(rindex))) {
				if (HAS_TRUE_SEL) {
					true_sel->set_index(true_count++, result_idx);
				}
			} else {
				if (HAS_FALSE_SEL) {
					false_sel->set_index(false_count++, result_idx);
				}
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool NO_NULL>
static inline idx_t
DistinctSelectGenericLoopSelSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                   const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                   const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                   ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, true, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectGenericLoop<LEFT_TYPE, RIGHT_TYPE, OP, NO_NULL, false, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static inline idx_t
DistinctSelectGenericLoopSwitch(const LEFT_TYPE *__restrict ldata, const RIGHT_TYPE *__restrict rdata,
                                const SelectionVector *__restrict lsel, const SelectionVector *__restrict rsel,
                                const SelectionVector *__restrict result_sel, idx_t count, ValidityMask &lmask,
                                ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	if (!lmask.AllValid() || !rmask.AllValid()) {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, false>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		return DistinctSelectGenericLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, true>(
		    ldata, rdata, lsel, rsel, result_sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectGeneric(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	UnifiedVectorFormat ldata, rdata;

	left.ToUnifiedFormat(count, ldata);
	right.ToUnifiedFormat(count, rdata);

	return DistinctSelectGenericLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP>(
	    UnifiedVectorFormat::GetData<LEFT_TYPE>(ldata), UnifiedVectorFormat::GetData<RIGHT_TYPE>(rdata), ldata.sel,
	    rdata.sel, sel, count, ldata.validity, rdata.validity, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL,
          bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DistinctSelectFlatLoop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                           const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                           ValidityMask &rmask, SelectionVector *true_sel, SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t result_idx = sel->get_index(i);
		idx_t lidx = LEFT_CONSTANT ? 0 : i;
		idx_t ridx = RIGHT_CONSTANT ? 0 : i;
		const bool lnull = !lmask.RowIsValid(lidx);
		const bool rnull = !rmask.RowIsValid(ridx);
		bool comparison_result = OP::Operation(ldata[lidx], rdata[ridx], lnull, rnull);
		if (HAS_TRUE_SEL) {
			true_sel->set_index(true_count, result_idx);
			true_count += comparison_result;
		}
		if (HAS_FALSE_SEL) {
			false_sel->set_index(false_count, result_idx);
			false_count += !comparison_result;
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT, bool NO_NULL>
static inline idx_t DistinctSelectFlatLoopSelSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                    const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                    ValidityMask &rmask, SelectionVector *true_sel,
                                                    SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else if (true_sel) {
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, true, false>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	} else {
		D_ASSERT(false_sel);
		return DistinctSelectFlatLoop<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, NO_NULL, false, true>(
		    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static inline idx_t DistinctSelectFlatLoopSwitch(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                 const SelectionVector *sel, idx_t count, ValidityMask &lmask,
                                                 ValidityMask &rmask, SelectionVector *true_sel,
                                                 SelectionVector *false_sel) {
	return DistinctSelectFlatLoopSelSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT, true>(
	    ldata, rdata, sel, count, lmask, rmask, true_sel, false_sel);
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static idx_t DistinctSelectFlat(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = FlatVector::GetData<LEFT_TYPE>(left);
	auto rdata = FlatVector::GetData<RIGHT_TYPE>(right);
	if (LEFT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(left)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, validity, FlatVector::Validity(right), true_sel, false_sel);
	} else if (RIGHT_CONSTANT) {
		ValidityMask validity;
		if (ConstantVector::IsNull(right)) {
			validity.SetAllInvalid(1);
		}
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), validity, true_sel, false_sel);
	} else {
		return DistinctSelectFlatLoopSwitch<LEFT_TYPE, RIGHT_TYPE, OP, LEFT_CONSTANT, RIGHT_CONSTANT>(
		    ldata, rdata, sel, count, FlatVector::Validity(left), FlatVector::Validity(right), true_sel, false_sel);
	}
}
template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelectConstant(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	auto ldata = ConstantVector::GetData<LEFT_TYPE>(left);
	auto rdata = ConstantVector::GetData<RIGHT_TYPE>(right);

	// both sides are constant, return either 0 or the count
	// in this case we do not fill in the result selection vector at all
	if (!OP::Operation(*ldata, *rdata, ConstantVector::IsNull(left), ConstantVector::IsNull(right))) {
		if (false_sel) {
			for (idx_t i = 0; i < count; i++) {
				false_sel->set_index(i, sel->get_index(i));
			}
		}
		return 0;
	} else {
		if (true_sel) {
			for (idx_t i = 0; i < count; i++) {
				true_sel->set_index(i, sel->get_index(i));
			}
		}
		return count;
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP>
static idx_t DistinctSelect(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                            SelectionVector *true_sel, SelectionVector *false_sel) {
	if (!sel) {
		sel = FlatVector::IncrementalSelectionVector();
	}
	if (left.GetVectorType() == VectorType::CONSTANT_VECTOR && right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectConstant<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	           right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, true, false>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR &&
	           right.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, true>(left, right, sel, count, true_sel, false_sel);
	} else if (left.GetVectorType() == VectorType::FLAT_VECTOR && right.GetVectorType() == VectorType::FLAT_VECTOR) {
		return DistinctSelectFlat<LEFT_TYPE, RIGHT_TYPE, OP, false, false>(left, right, sel, count, true_sel,
		                                                                   false_sel);
	} else {
		return DistinctSelectGeneric<LEFT_TYPE, RIGHT_TYPE, OP>(left, right, sel, count, true_sel, false_sel);
	}
}

template <class OP>
static idx_t DistinctSelectNotNull(Vector &left, Vector &right, const idx_t count, idx_t &true_count,
                                   const SelectionVector &sel, SelectionVector &maybe_vec, OptionalSelection &true_opt,
                                   OptionalSelection &false_opt) {
	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(count, lvdata);
	right.ToUnifiedFormat(count, rvdata);

	auto &lmask = lvdata.validity;
	auto &rmask = rvdata.validity;

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
	true_count = 0;
	idx_t false_count = 0;
	for (idx_t i = 0; i < count; ++i) {
		const auto result_idx = sel.get_index(i);
		const auto lidx = lvdata.sel->get_index(i);
		const auto ridx = rvdata.sel->get_index(i);
		const auto lnull = !lmask.RowIsValid(lidx);
		const auto rnull = !rmask.RowIsValid(ridx);
		if (lnull || rnull) {
			// If either is NULL then we can major distinguish them
			if (!OP::Operation(false, false, lnull, rnull)) {
				false_opt.Append(false_count, result_idx);
			} else {
				true_opt.Append(true_count, result_idx);
			}
		} else {
			//	Neither is NULL, distinguish values.
			slicer.set_index(remaining, i);
			maybe_vec.set_index(remaining++, result_idx);
		}
	}

	true_opt.Advance(true_count);
	false_opt.Advance(false_count);

	if (remaining && remaining < count) {
		left.Slice(slicer, remaining);
		right.Slice(slicer, remaining);
	}

	return remaining;
}

struct PositionComparator {
	// Select the rows that definitely match.
	// Default to the same as the final row
	template <typename OP>
	static idx_t Definite(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      SelectionVector *true_sel, SelectionVector &false_sel) {
		return Final<OP>(left, right, sel, count, true_sel, &false_sel);
	}

	// Select the possible rows that need further testing.
	// Usually this means Is Not Distinct, as those are the semantics used by Postges
	template <typename OP>
	static idx_t Possible(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
	                      SelectionVector &true_sel, SelectionVector *false_sel) {
		return VectorOperations::NestedEquals(left, right, sel, count, &true_sel, false_sel);
	}

	// Select the matching rows for the final position.
	// This needs to be specialised.
	template <typename OP>
	static idx_t Final(Vector &left, Vector &right, const SelectionVector &sel, idx_t count, SelectionVector *true_sel,
	                   SelectionVector *false_sel) {
		return 0;
	}

	// Tie-break based on length when one of the sides has been exhausted, returning true if the LHS matches.
	// This essentially means that the existing positions compare equal.
	// Default to the same semantics as the OP for idx_t. This works in most cases.
	template <typename OP>
	static bool TieBreak(const idx_t lpos, const idx_t rpos) {
		return OP::Operation(lpos, rpos, false, false);
	}
};

// NotDistinctFrom must always check every column
template <>
idx_t PositionComparator::Definite<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                            idx_t count, SelectionVector *true_sel,
                                                            SelectionVector &false_sel) {
	return 0;
}

template <>
idx_t PositionComparator::Final<duckdb::NotDistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, SelectionVector *true_sel,
                                                         SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

// DistinctFrom must check everything that matched
template <>
idx_t PositionComparator::Possible<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                         idx_t count, SelectionVector &true_sel,
                                                         SelectionVector *false_sel) {
	return count;
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctFrom>(Vector &left, Vector &right, const SelectionVector &sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

// Non-strict inequalities must use strict comparisons for Definite
template <>
idx_t PositionComparator::Definite<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector &false_sel) {
	return VectorOperations::DistinctGreaterThan(right, left, &sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel,
                                                                idx_t count, SelectionVector *true_sel,
                                                                SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(right, left, &sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Definite<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                      const SelectionVector &sel, idx_t count,
                                                                      SelectionVector *true_sel,
                                                                      SelectionVector &false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, &false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThanEquals>(Vector &left, Vector &right,
                                                                   const SelectionVector &sel, idx_t count,
                                                                   SelectionVector *true_sel,
                                                                   SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

// Strict inequalities just use strict for both Definite and Final
template <>
idx_t PositionComparator::Final<duckdb::DistinctLessThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                          idx_t count, SelectionVector *true_sel,
                                                          SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(right, left, &sel, count, true_sel, false_sel);
}

template <>
idx_t PositionComparator::Final<duckdb::DistinctGreaterThan>(Vector &left, Vector &right, const SelectionVector &sel,
                                                             idx_t count, SelectionVector *true_sel,
                                                             SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

using StructEntries = vector<unique_ptr<Vector>>;

static void ExtractNestedSelection(const SelectionVector &slice_sel, const idx_t count, const SelectionVector &sel,
                                   OptionalSelection &opt) {

	for (idx_t i = 0; i < count;) {
		const auto slice_idx = slice_sel.get_index(i);
		const auto result_idx = sel.get_index(slice_idx);
		opt.Append(i, result_idx);
	}
	opt.Advance(count);
}

static void DensifyNestedSelection(const SelectionVector &dense_sel, const idx_t count, SelectionVector &slice_sel) {
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, dense_sel.get_index(i));
	}
}

template <class OP>
static idx_t DistinctSelectStruct(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                  OptionalSelection &true_opt, OptionalSelection &false_opt) {
	if (count == 0) {
		return 0;
	}

	// Avoid allocating in the 99% of the cases where we don't need to.
	StructEntries lsliced, rsliced;
	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);
	D_ASSERT(lchildren.size() == rchildren.size());

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	const auto vcount = count;
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	idx_t match_count = 0;
	for (idx_t col_no = 0; col_no < lchildren.size(); ++col_no) {
		// Slice the children to maintain density
		Vector lchild(*lchildren[col_no]);
		lchild.Flatten(vcount);
		lchild.Slice(slice_sel, count);

		Vector rchild(*rchildren[col_no]);
		rchild.Flatten(vcount);
		rchild.Slice(slice_sel, count);

		// Find everything that definitely matches
		auto true_count = PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel);
		if (true_count > 0) {
			auto false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Remove the definite matches from the slicing vector
			DensifyNestedSelection(false_sel, false_count, slice_sel);

			match_count += true_count;
			count -= true_count;
		}

		if (col_no != lchildren.size() - 1) {
			// Find what might match on the next position
			true_count = PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel);
			auto false_count = count - true_count;

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			// Remove any definite failures from the slicing vector
			if (false_count) {
				DensifyNestedSelection(true_sel, true_count, slice_sel);
			}

			count = true_count;
		} else {
			true_count = PositionComparator::Final<OP>(lchild, rchild, slice_sel, count, &true_sel, &false_sel);
			auto false_count = count - true_count;

			// Extract the definite matches into the true result
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);

			// Extract the definite failures into the false result
			ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

			match_count += true_count;
		}
	}
	return match_count;
}

static void PositionListCursor(SelectionVector &cursor, UnifiedVectorFormat &vdata, const idx_t pos,
                               const SelectionVector &slice_sel, const idx_t count) {
	const auto data = UnifiedVectorFormat::GetData<list_entry_t>(vdata);
	for (idx_t i = 0; i < count; ++i) {
		const auto slice_idx = slice_sel.get_index(i);

		const auto lidx = vdata.sel->get_index(slice_idx);
		const auto &entry = data[lidx];
		cursor.set_index(i, entry.offset + pos);
	}
}

template <class OP>
static idx_t DistinctSelectList(Vector &left, Vector &right, idx_t count, const SelectionVector &sel,
                                OptionalSelection &true_opt, OptionalSelection &false_opt) {
	if (count == 0) {
		return count;
	}

	// Create dictionary views of the children so we can vectorise the positional comparisons.
	SelectionVector lcursor(count);
	SelectionVector rcursor(count);

	Vector lentry_flattened(ListVector::GetEntry(left));
	Vector rentry_flattened(ListVector::GetEntry(right));
	lentry_flattened.Flatten(ListVector::GetListSize(left));
	rentry_flattened.Flatten(ListVector::GetListSize(right));
	Vector lchild(lentry_flattened, lcursor, count);
	Vector rchild(rentry_flattened, rcursor, count);

	// To perform the positional comparison, we use a vectorisation of the following algorithm:
	// bool CompareLists(T *left, idx_t nleft, T *right, nright) {
	// 	for (idx_t pos = 0; ; ++pos) {
	// 		if (nleft == pos || nright == pos)
	// 			return OP::TieBreak(nleft, nright);
	// 		if (OP::Definite(*left, *right))
	// 			return true;
	// 		if (!OP::Maybe(*left, *right))
	// 			return false;
	// 		}
	//	 	++left, ++right;
	// 	}
	// }

	// Get pointers to the list entries
	UnifiedVectorFormat lvdata;
	left.ToUnifiedFormat(count, lvdata);
	const auto ldata = UnifiedVectorFormat::GetData<list_entry_t>(lvdata);

	UnifiedVectorFormat rvdata;
	right.ToUnifiedFormat(count, rvdata);
	const auto rdata = UnifiedVectorFormat::GetData<list_entry_t>(rvdata);

	// In order to reuse the comparators, we have to track what passed and failed internally.
	// To do that, we need local SVs that we then merge back into the real ones after every pass.
	SelectionVector slice_sel(count);
	for (idx_t i = 0; i < count; ++i) {
		slice_sel.set_index(i, i);
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	idx_t match_count = 0;
	for (idx_t pos = 0; count > 0; ++pos) {
		// Set up the cursors for the current position
		PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
		PositionListCursor(rcursor, rvdata, pos, slice_sel, count);

		// Tie-break the pairs where one of the LISTs is exhausted.
		idx_t true_count = 0;
		idx_t false_count = 0;
		idx_t maybe_count = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto slice_idx = slice_sel.get_index(i);
			const auto lidx = lvdata.sel->get_index(slice_idx);
			const auto &lentry = ldata[lidx];
			const auto ridx = rvdata.sel->get_index(slice_idx);
			const auto &rentry = rdata[ridx];
			if (lentry.length == pos || rentry.length == pos) {
				const auto idx = sel.get_index(slice_idx);
				if (PositionComparator::TieBreak<OP>(lentry.length, rentry.length)) {
					true_opt.Append(true_count, idx);
				} else {
					false_opt.Append(false_count, idx);
				}
			} else {
				true_sel.set_index(maybe_count++, slice_idx);
			}
		}
		true_opt.Advance(true_count);
		false_opt.Advance(false_count);
		match_count += true_count;

		// Redensify the list cursors
		if (maybe_count < count) {
			count = maybe_count;
			DensifyNestedSelection(true_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find everything that definitely matches
		true_count = PositionComparator::Definite<OP>(lchild, rchild, slice_sel, count, &true_sel, false_sel);
		if (true_count) {
			false_count = count - true_count;
			ExtractNestedSelection(false_count ? true_sel : slice_sel, true_count, sel, true_opt);
			match_count += true_count;

			// Redensify the list cursors
			count -= true_count;
			DensifyNestedSelection(false_sel, count, slice_sel);
			PositionListCursor(lcursor, lvdata, pos, slice_sel, count);
			PositionListCursor(rcursor, rvdata, pos, slice_sel, count);
		}

		// Find what might match on the next position
		true_count = PositionComparator::Possible<OP>(lchild, rchild, slice_sel, count, true_sel, &false_sel);
		false_count = count - true_count;
		ExtractNestedSelection(true_count ? false_sel : slice_sel, false_count, sel, false_opt);

		if (false_count) {
			DensifyNestedSelection(true_sel, true_count, slice_sel);
		}
		count = true_count;
	}

	return match_count;
}

template <class OP, class OPNESTED>
static idx_t DistinctSelectNested(Vector &left, Vector &right, const SelectionVector *sel, const idx_t count,
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

	idx_t match_count = 0;
	auto unknown =
	    DistinctSelectNotNull<OP>(l_not_null, r_not_null, count, match_count, *sel, maybe_vec, true_opt, false_opt);

	if (PhysicalType::LIST == left.GetType().InternalType()) {
		match_count += DistinctSelectList<OPNESTED>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt);
	} else {
		match_count += DistinctSelectStruct<OPNESTED>(l_not_null, r_not_null, unknown, maybe_vec, true_opt, false_opt);
	}

	// Copy the buffered selections to the output selections
	if (true_sel) {
		DensifyNestedSelection(true_vec, match_count, *true_sel);
	}

	if (false_sel) {
		DensifyNestedSelection(false_vec, count - match_count, *false_sel);
	}

	return match_count;
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count);

template <class T, class OP>
static inline void TemplatedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	DistinctExecute<T, T, bool, OP>(left, right, result, count);
}
template <class OP>
static void ExecuteDistinct(Vector &left, Vector &right, Vector &result, idx_t count) {
	D_ASSERT(left.GetType() == right.GetType() && result.GetType() == LogicalType::BOOLEAN);
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedDistinctExecute<int8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedDistinctExecute<int16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedDistinctExecute<int32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedDistinctExecute<int64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedDistinctExecute<uint8_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedDistinctExecute<uint16_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedDistinctExecute<uint32_t, OP>(left, right, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedDistinctExecute<uint64_t, OP>(left, right, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedDistinctExecute<hugeint_t, OP>(left, right, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedDistinctExecute<float, OP>(left, right, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDistinctExecute<double, OP>(left, right, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDistinctExecute<interval_t, OP>(left, right, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedDistinctExecute<string_t, OP>(left, right, result, count);
		break;
	case PhysicalType::LIST:
	case PhysicalType::STRUCT:
		NestedDistinctExecute<OP>(left, right, result, count);
		break;
	default:
		throw InternalException("Invalid type for distinct comparison");
	}
}

template <class OP, class OPNESTED = OP>
static idx_t TemplatedDistinctSelectOperation(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                              SelectionVector *true_sel, SelectionVector *false_sel) {
	// the inplace loops take the result as the last parameter
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return DistinctSelect<int8_t, int8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT16:
		return DistinctSelect<int16_t, int16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT32:
		return DistinctSelect<int32_t, int32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT64:
		return DistinctSelect<int64_t, int64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT8:
		return DistinctSelect<uint8_t, uint8_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT16:
		return DistinctSelect<uint16_t, uint16_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT32:
		return DistinctSelect<uint32_t, uint32_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::UINT64:
		return DistinctSelect<uint64_t, uint64_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INT128:
		return DistinctSelect<hugeint_t, hugeint_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::FLOAT:
		return DistinctSelect<float, float, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::DOUBLE:
		return DistinctSelect<double, double, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::INTERVAL:
		return DistinctSelect<interval_t, interval_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::VARCHAR:
		return DistinctSelect<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		return DistinctSelectNested<OP, OPNESTED>(left, right, sel, count, true_sel, false_sel);
	default:
		throw InternalException("Invalid type for distinct selection");
	}
}

template <typename OP>
static void NestedDistinctExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
	const auto left_constant = left.GetVectorType() == VectorType::CONSTANT_VECTOR;
	const auto right_constant = right.GetVectorType() == VectorType::CONSTANT_VECTOR;

	if (left_constant && right_constant) {
		// both sides are constant, so just compare one element.
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		SelectionVector true_sel(1);
		auto match_count = TemplatedDistinctSelectOperation<OP>(left, right, nullptr, 1, &true_sel, nullptr);
		result_data[0] = match_count > 0;
		return;
	}

	SelectionVector true_sel(count);
	SelectionVector false_sel(count);

	// DISTINCT is either true or false
	idx_t match_count = TemplatedDistinctSelectOperation<OP>(left, right, nullptr, count, &true_sel, &false_sel);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);

	for (idx_t i = 0; i < match_count; ++i) {
		const auto idx = true_sel.get_index(i);
		result_data[idx] = true;
	}

	const idx_t no_match_count = count - match_count;
	for (idx_t i = 0; i < no_match_count; ++i) {
		const auto idx = false_sel.get_index(i);
		result_data[idx] = false;
	}
}

void VectorOperations::DistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::DistinctFrom>(left, right, result, count);
}

void VectorOperations::NotDistinctFrom(Vector &left, Vector &right, Vector &result, idx_t count) {
	ExecuteDistinct<duckdb::NotDistinctFrom>(left, right, result, count);
}

// true := A != B with nulls being equal
idx_t VectorOperations::DistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                     SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, true_sel, false_sel);
}
// true := A == B with nulls being equal
idx_t VectorOperations::NotDistinctFrom(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	return count - TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, sel, count, false_sel, true_sel);
}

// true := A > B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                            SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(left, right, sel, count, true_sel, false_sel);
}

// true := A > B with nulls being minimal
idx_t VectorOperations::DistinctGreaterThanNullsFirst(Vector &left, Vector &right, const SelectionVector *sel,
                                                      idx_t count, SelectionVector *true_sel,
                                                      SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanNullsFirst, duckdb::DistinctGreaterThan>(
	    left, right, sel, count, true_sel, false_sel);
}
// true := A >= B with nulls being maximal
idx_t VectorOperations::DistinctGreaterThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return count -
	       TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(right, left, sel, count, false_sel, true_sel);
}
// true := A < B with nulls being maximal
idx_t VectorOperations::DistinctLessThan(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                         SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(right, left, sel, count, true_sel, false_sel);
}

// true := A < B with nulls being minimal
idx_t VectorOperations::DistinctLessThanNullsFirst(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                                   SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThanNullsFirst, duckdb::DistinctGreaterThan>(
	    right, left, sel, count, true_sel, false_sel);
}

// true := A <= B with nulls being maximal
idx_t VectorOperations::DistinctLessThanEquals(Vector &left, Vector &right, const SelectionVector *sel, idx_t count,
                                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return count -
	       TemplatedDistinctSelectOperation<duckdb::DistinctGreaterThan>(left, right, sel, count, false_sel, true_sel);
}

// true := A != B with nulls being equal, inputs selected
idx_t VectorOperations::NestedNotEquals(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	return TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, &sel, count, true_sel, false_sel);
}
// true := A == B with nulls being equal, inputs selected
idx_t VectorOperations::NestedEquals(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                     SelectionVector *true_sel, SelectionVector *false_sel) {
	return count -
	       TemplatedDistinctSelectOperation<duckdb::DistinctFrom>(left, right, &sel, count, false_sel, true_sel);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//




namespace duckdb {

template <bool INVERSE>
void IsNullLoop(Vector &input, Vector &result, idx_t count) {
	D_ASSERT(result.GetType() == LogicalType::BOOLEAN);

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto result_data = ConstantVector::GetData<bool>(result);
		*result_data = INVERSE ? !ConstantVector::IsNull(input) : ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto result_data = FlatVector::GetData<bool>(result);
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			result_data[i] = INVERSE ? data.validity.RowIsValid(idx) : !data.validity.RowIsValid(idx);
		}
	}
}

void VectorOperations::IsNotNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<true>(input, result, count);
}

void VectorOperations::IsNull(Vector &input, Vector &result, idx_t count) {
	IsNullLoop<false>(input, result, count);
}

bool VectorOperations::HasNotNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return !ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		if (data.validity.AllValid()) {
			return true;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

bool VectorOperations::HasNull(Vector &input, idx_t count) {
	if (count == 0) {
		return false;
	}
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		return ConstantVector::IsNull(input);
	} else {
		UnifiedVectorFormat data;
		input.ToUnifiedFormat(count, data);

		if (data.validity.AllValid()) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = data.sel->get_index(i);
			if (!data.validity.RowIsValid(idx)) {
				return true;
			}
		}
		return false;
	}
}

idx_t VectorOperations::CountNotNull(Vector &input, const idx_t count) {
	idx_t valid = 0;

	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	if (vdata.validity.AllValid()) {
		return count;
	}
	switch (input.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		valid += vdata.validity.CountValid(count);
		break;
	case VectorType::CONSTANT_VECTOR:
		valid += vdata.validity.CountValid(1) * count;
		break;
	default:
		for (idx_t i = 0; i < count; ++i) {
			const auto row_idx = vdata.sel->get_index(i);
			valid += int(vdata.validity.RowIsValid(row_idx));
		}
		break;
	}

	return valid;
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// numeric_inplace_operators.cpp
// Description: This file contains the implementation of numeric inplace ops
// += *= /= -= %=
//===--------------------------------------------------------------------===//



#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// In-Place Addition
//===--------------------------------------------------------------------===//

void VectorOperations::AddInPlace(Vector &input, int64_t right, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::POINTER);
	if (right == 0) {
		return;
	}
	switch (input.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		D_ASSERT(!ConstantVector::IsNull(input));
		auto data = ConstantVector::GetData<uintptr_t>(input);
		*data += right;
		break;
	}
	default: {
		D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);
		auto data = FlatVector::GetData<uintptr_t>(input);
		for (idx_t i = 0; i < count; i++) {
			data[i] += right;
		}
		break;
	}
	}
}

} // namespace duckdb






namespace duckdb {

bool VectorOperations::TryCast(CastFunctionSet &set, GetCastFunctionInput &input, Vector &source, Vector &result,
                               idx_t count, string *error_message, bool strict) {
	auto cast_function = set.GetCastFunction(source.GetType(), result.GetType(), input);
	unique_ptr<FunctionLocalState> local_state;
	if (cast_function.init_local_state) {
		CastLocalStateParameters lparameters(input.context, cast_function.cast_data);
		local_state = cast_function.init_local_state(lparameters);
	}
	CastParameters parameters(cast_function.cast_data.get(), strict, error_message, local_state.get());
	return cast_function.function(source, result, count, parameters);
}

bool VectorOperations::DefaultTryCast(Vector &source, Vector &result, idx_t count, string *error_message, bool strict) {
	CastFunctionSet set;
	GetCastFunctionInput input;
	return VectorOperations::TryCast(set, input, source, result, count, error_message, strict);
}

void VectorOperations::DefaultCast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::DefaultTryCast(source, result, count, nullptr, strict);
}

bool VectorOperations::TryCast(ClientContext &context, Vector &source, Vector &result, idx_t count,
                               string *error_message, bool strict) {
	auto &config = DBConfig::GetConfig(context);
	auto &set = config.GetCastFunctions();
	GetCastFunctionInput get_input(context);
	return VectorOperations::TryCast(set, get_input, source, result, count, error_message, strict);
}

void VectorOperations::Cast(ClientContext &context, Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(context, source, result, count, nullptr, strict);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// copy.cpp
// Description: This file contains the implementation of the different copy
// functions
//===--------------------------------------------------------------------===//







namespace duckdb {

template <class T>
static void TemplatedCopy(const Vector &source, const SelectionVector &sel, Vector &target, idx_t source_offset,
                          idx_t target_offset, idx_t copy_count) {
	auto ldata = FlatVector::GetData<T>(source);
	auto tdata = FlatVector::GetData<T>(target);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel.get_index(source_offset + i);
		tdata[target_offset + i] = ldata[source_idx];
	}
}

static const ValidityMask &CopyValidityMask(const Vector &v) {
	switch (v.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		return FlatVector::Validity(v);
	case VectorType::FSST_VECTOR:
		return FSSTVector::Validity(v);
	default:
		throw InternalException("Unsupported vector type in vector copy");
	}
}

void VectorOperations::Copy(const Vector &source_p, Vector &target, const SelectionVector &sel_p, idx_t source_count,
                            idx_t source_offset, idx_t target_offset) {
	D_ASSERT(source_offset <= source_count);
	D_ASSERT(source_p.GetType() == target.GetType());
	idx_t copy_count = source_count - source_offset;

	SelectionVector owned_sel;
	const SelectionVector *sel = &sel_p;

	const Vector *source = &source_p;
	bool finished = false;
	while (!finished) {
		switch (source->GetVectorType()) {
		case VectorType::DICTIONARY_VECTOR: {
			// dictionary vector: merge selection vectors
			auto &child = DictionaryVector::Child(*source);
			auto &dict_sel = DictionaryVector::SelVector(*source);
			// merge the selection vectors and verify the child
			auto new_buffer = dict_sel.Slice(*sel, source_count);
			owned_sel.Initialize(new_buffer);
			sel = &owned_sel;
			source = &child;
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			int64_t start, increment;
			Vector seq(source->GetType());
			SequenceVector::GetSequence(*source, start, increment);
			VectorOperations::GenerateSequence(seq, source_count, *sel, start, increment);
			VectorOperations::Copy(seq, target, *sel, source_count, source_offset, target_offset);
			return;
		}
		case VectorType::CONSTANT_VECTOR:
			sel = ConstantVector::ZeroSelectionVector(copy_count, owned_sel);
			finished = true;
			break;
		case VectorType::FSST_VECTOR:
			finished = true;
			break;
		case VectorType::FLAT_VECTOR:
			finished = true;
			break;
		default:
			throw NotImplementedException("FIXME unimplemented vector type for VectorOperations::Copy");
		}
	}

	if (copy_count == 0) {
		return;
	}

	// Allow copying of a single value to constant vectors
	const auto target_vector_type = target.GetVectorType();
	if (copy_count == 1 && target_vector_type == VectorType::CONSTANT_VECTOR) {
		target_offset = 0;
		target.SetVectorType(VectorType::FLAT_VECTOR);
	}
	D_ASSERT(target.GetVectorType() == VectorType::FLAT_VECTOR);

	// first copy the nullmask
	auto &tmask = FlatVector::Validity(target);
	if (source->GetVectorType() == VectorType::CONSTANT_VECTOR) {
		const bool valid = !ConstantVector::IsNull(*source);
		for (idx_t i = 0; i < copy_count; i++) {
			tmask.Set(target_offset + i, valid);
		}
	} else {
		auto &smask = CopyValidityMask(*source);
		if (smask.IsMaskSet()) {
			for (idx_t i = 0; i < copy_count; i++) {
				auto idx = sel->get_index(source_offset + i);

				if (smask.RowIsValid(idx)) {
					// set valid
					if (!tmask.AllValid()) {
						tmask.SetValidUnsafe(target_offset + i);
					}
				} else {
					// set invalid
					if (tmask.AllValid()) {
						auto init_size = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, target_offset + copy_count);
						tmask.Initialize(init_size);
					}
					tmask.SetInvalidUnsafe(target_offset + i);
				}
			}
		}
	}

	D_ASSERT(sel);

	// For FSST Vectors we decompress instead of copying.
	if (source->GetVectorType() == VectorType::FSST_VECTOR) {
		FSSTVector::DecompressVector(*source, target, source_offset, target_offset, copy_count, sel);
		return;
	}

	// now copy over the data
	switch (source->GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCopy<int8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT16:
		TemplatedCopy<int16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT32:
		TemplatedCopy<int32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT64:
		TemplatedCopy<int64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT8:
		TemplatedCopy<uint8_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT16:
		TemplatedCopy<uint16_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT32:
		TemplatedCopy<uint32_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::UINT64:
		TemplatedCopy<uint64_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INT128:
		TemplatedCopy<hugeint_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedCopy<float>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCopy<double>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedCopy<interval_t>(*source, *sel, target, source_offset, target_offset, copy_count);
		break;
	case PhysicalType::VARCHAR: {
		auto ldata = FlatVector::GetData<string_t>(*source);
		auto tdata = FlatVector::GetData<string_t>(target);
		for (idx_t i = 0; i < copy_count; i++) {
			auto source_idx = sel->get_index(source_offset + i);
			auto target_idx = target_offset + i;
			if (tmask.RowIsValid(target_idx)) {
				tdata[target_idx] = StringVector::AddStringOrBlob(target, ldata[source_idx]);
			}
		}
		break;
	}
	case PhysicalType::STRUCT: {
		auto &source_children = StructVector::GetEntries(*source);
		auto &target_children = StructVector::GetEntries(target);
		D_ASSERT(source_children.size() == target_children.size());
		for (idx_t i = 0; i < source_children.size(); i++) {
			VectorOperations::Copy(*source_children[i], *target_children[i], sel_p, source_count, source_offset,
			                       target_offset);
		}
		break;
	}
	case PhysicalType::LIST: {
		D_ASSERT(target.GetType().InternalType() == PhysicalType::LIST);

		auto &source_child = ListVector::GetEntry(*source);
		auto sdata = FlatVector::GetData<list_entry_t>(*source);
		auto tdata = FlatVector::GetData<list_entry_t>(target);

		if (target_vector_type == VectorType::CONSTANT_VECTOR) {
			// If we are only writing one value, then the copied values (if any) are contiguous
			// and we can just Append from the offset position
			if (!tmask.RowIsValid(target_offset)) {
				break;
			}
			auto source_idx = sel->get_index(source_offset);
			auto &source_entry = sdata[source_idx];
			const idx_t source_child_size = source_entry.length + source_entry.offset;

			//! overwrite constant target vectors.
			ListVector::SetListSize(target, 0);
			ListVector::Append(target, source_child, source_child_size, source_entry.offset);

			auto &target_entry = tdata[target_offset];
			target_entry.length = source_entry.length;
			target_entry.offset = 0;
		} else {
			//! if the source has list offsets, we need to append them to the target
			//! build a selection vector for the copied child elements
			vector<sel_t> child_rows;
			for (idx_t i = 0; i < copy_count; ++i) {
				if (tmask.RowIsValid(target_offset + i)) {
					auto source_idx = sel->get_index(source_offset + i);
					auto &source_entry = sdata[source_idx];
					for (idx_t j = 0; j < source_entry.length; ++j) {
						child_rows.emplace_back(source_entry.offset + j);
					}
				}
			}
			idx_t source_child_size = child_rows.size();
			SelectionVector child_sel(child_rows.data());

			idx_t old_target_child_len = ListVector::GetListSize(target);

			//! append to list itself
			ListVector::Append(target, source_child, child_sel, source_child_size);

			//! now write the list offsets
			for (idx_t i = 0; i < copy_count; i++) {
				auto source_idx = sel->get_index(source_offset + i);
				auto &source_entry = sdata[source_idx];
				auto &target_entry = tdata[target_offset + i];

				target_entry.length = source_entry.length;
				target_entry.offset = old_target_child_len;
				if (tmask.RowIsValid(target_offset + i)) {
					old_target_child_len += target_entry.length;
				}
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type '%s' for copy!",
		                              TypeIdToString(source->GetType().InternalType()));
	}

	if (target_vector_type != VectorType::FLAT_VECTOR) {
		target.SetVectorType(target_vector_type);
	}
}

void VectorOperations::Copy(const Vector &source, Vector &target, idx_t source_count, idx_t source_offset,
                            idx_t target_offset) {
	VectorOperations::Copy(source, target, *FlatVector::IncrementalSelectionVector(), source_count, source_offset,
	                       target_offset);
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// hash.cpp
// Description: This file contains the vectorized hash implementations
//===--------------------------------------------------------------------===//







namespace duckdb {

struct HashOp {
	static const hash_t NULL_HASH = 0xbf58476d1ce4e5b9;

	template <class T>
	static inline hash_t Operation(T input, bool is_null) {
		return is_null ? NULL_HASH : duckdb::Hash<T>(input);
	}
};

static inline hash_t CombineHashScalar(hash_t a, hash_t b) {
	return (a * UINT64_C(0xbf58476d1ce4e5b9)) ^ b;
}

template <bool HAS_RSEL, class T>
static inline void TightLoopHash(const T *__restrict ldata, hash_t *__restrict result_data, const SelectionVector *rsel,
                                 idx_t count, const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			result_data[ridx] = duckdb::Hash<T>(ldata[idx]);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void TemplatedLoopHash(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto ldata = ConstantVector::GetData<T>(input);
		auto result_data = ConstantVector::GetData<hash_t>(result);
		*result_data = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);

		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		TightLoopHash<HAS_RSEL, T>(UnifiedVectorFormat::GetData<T>(idata), FlatVector::GetData<hash_t>(result), rsel,
		                           count, idata.sel, idata.validity);
	}
}

template <bool HAS_RSEL, bool FIRST_HASH>
static inline void StructLoopHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	auto &children = StructVector::GetEntries(input);

	D_ASSERT(!children.empty());
	idx_t col_no = 0;
	if (HAS_RSEL) {
		if (FIRST_HASH) {
			VectorOperations::Hash(*children[col_no++], hashes, *rsel, count);
		} else {
			VectorOperations::CombineHash(hashes, *children[col_no++], *rsel, count);
		}
		while (col_no < children.size()) {
			VectorOperations::CombineHash(hashes, *children[col_no++], *rsel, count);
		}
	} else {
		if (FIRST_HASH) {
			VectorOperations::Hash(*children[col_no++], hashes, count);
		} else {
			VectorOperations::CombineHash(hashes, *children[col_no++], count);
		}
		while (col_no < children.size()) {
			VectorOperations::CombineHash(hashes, *children[col_no++], count);
		}
	}
}

template <bool HAS_RSEL, bool FIRST_HASH>
static inline void ListLoopHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	auto hdata = FlatVector::GetData<hash_t>(hashes);

	UnifiedVectorFormat idata;
	input.ToUnifiedFormat(count, idata);
	const auto ldata = UnifiedVectorFormat::GetData<list_entry_t>(idata);

	// Hash the children into a temporary
	auto &child = ListVector::GetEntry(input);
	const auto child_count = ListVector::GetListSize(input);

	Vector child_hashes(LogicalType::HASH, child_count);
	if (child_count > 0) {
		VectorOperations::Hash(child, child_hashes, child_count);
		child_hashes.Flatten(child_count);
	}
	auto chdata = FlatVector::GetData<hash_t>(child_hashes);

	// Reduce the number of entries to check to the non-empty ones
	SelectionVector unprocessed(count);
	SelectionVector cursor(HAS_RSEL ? STANDARD_VECTOR_SIZE : count);
	idx_t remaining = 0;
	for (idx_t i = 0; i < count; ++i) {
		const idx_t ridx = HAS_RSEL ? rsel->get_index(i) : i;
		const auto lidx = idata.sel->get_index(ridx);
		const auto &entry = ldata[lidx];
		if (idata.validity.RowIsValid(lidx) && entry.length > 0) {
			unprocessed.set_index(remaining++, ridx);
			cursor.set_index(ridx, entry.offset);
		} else if (FIRST_HASH) {
			hdata[ridx] = HashOp::NULL_HASH;
		}
		// Empty or NULL non-first elements have no effect.
	}

	count = remaining;
	if (count == 0) {
		return;
	}

	// Merge the first position hash into the main hash
	idx_t position = 1;
	if (FIRST_HASH) {
		remaining = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto ridx = unprocessed.get_index(i);
			const auto cidx = cursor.get_index(ridx);
			hdata[ridx] = chdata[cidx];

			const auto lidx = idata.sel->get_index(ridx);
			const auto &entry = ldata[lidx];
			if (entry.length > position) {
				// Entry still has values to hash
				unprocessed.set_index(remaining++, ridx);
				cursor.set_index(ridx, cidx + 1);
			}
		}
		count = remaining;
		if (count == 0) {
			return;
		}
		++position;
	}

	// Combine the hashes for the remaining positions until there are none left
	for (;; ++position) {
		remaining = 0;
		for (idx_t i = 0; i < count; ++i) {
			const auto ridx = unprocessed.get_index(i);
			const auto cidx = cursor.get_index(ridx);
			hdata[ridx] = CombineHashScalar(hdata[ridx], chdata[cidx]);

			const auto lidx = idata.sel->get_index(ridx);
			const auto &entry = ldata[lidx];
			if (entry.length > position) {
				// Entry still has values to hash
				unprocessed.set_index(remaining++, ridx);
				cursor.set_index(ridx, cidx + 1);
			}
		}

		count = remaining;
		if (count == 0) {
			break;
		}
	}
}

template <bool HAS_RSEL>
static inline void HashTypeSwitch(Vector &input, Vector &result, const SelectionVector *rsel, idx_t count) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedLoopHash<HAS_RSEL, int8_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT16:
		TemplatedLoopHash<HAS_RSEL, int16_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT32:
		TemplatedLoopHash<HAS_RSEL, int32_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT64:
		TemplatedLoopHash<HAS_RSEL, int64_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedLoopHash<HAS_RSEL, uint8_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedLoopHash<HAS_RSEL, uint16_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedLoopHash<HAS_RSEL, uint32_t>(input, result, rsel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedLoopHash<HAS_RSEL, uint64_t>(input, result, rsel, count);
		break;
	case PhysicalType::INT128:
		TemplatedLoopHash<HAS_RSEL, hugeint_t>(input, result, rsel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedLoopHash<HAS_RSEL, float>(input, result, rsel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedLoopHash<HAS_RSEL, double>(input, result, rsel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedLoopHash<HAS_RSEL, interval_t>(input, result, rsel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedLoopHash<HAS_RSEL, string_t>(input, result, rsel, count);
		break;
	case PhysicalType::STRUCT:
		StructLoopHash<HAS_RSEL, true>(input, result, rsel, count);
		break;
	case PhysicalType::LIST:
		ListLoopHash<HAS_RSEL, true>(input, result, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for hash");
	}
}

void VectorOperations::Hash(Vector &input, Vector &result, idx_t count) {
	HashTypeSwitch<false>(input, result, nullptr, count);
}

void VectorOperations::Hash(Vector &input, Vector &result, const SelectionVector &sel, idx_t count) {
	HashTypeSwitch<true>(input, result, &sel, count);
}

template <bool HAS_RSEL, class T>
static inline void TightLoopCombineHashConstant(const T *__restrict ldata, hash_t constant_hash,
                                                hash_t *__restrict hash_data, const SelectionVector *rsel, idx_t count,
                                                const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
			hash_data[ridx] = CombineHashScalar(constant_hash, other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = CombineHashScalar(constant_hash, other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
static inline void TightLoopCombineHash(const T *__restrict ldata, hash_t *__restrict hash_data,
                                        const SelectionVector *rsel, idx_t count,
                                        const SelectionVector *__restrict sel_vector, ValidityMask &mask) {
	if (!mask.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = HashOp::Operation(ldata[idx], !mask.RowIsValid(idx));
			hash_data[ridx] = CombineHashScalar(hash_data[ridx], other_hash);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto ridx = HAS_RSEL ? rsel->get_index(i) : i;
			auto idx = sel_vector->get_index(ridx);
			auto other_hash = duckdb::Hash<T>(ldata[idx]);
			hash_data[ridx] = CombineHashScalar(hash_data[ridx], other_hash);
		}
	}
}

template <bool HAS_RSEL, class T>
void TemplatedLoopCombineHash(Vector &input, Vector &hashes, const SelectionVector *rsel, idx_t count) {
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR && hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto ldata = ConstantVector::GetData<T>(input);
		auto hash_data = ConstantVector::GetData<hash_t>(hashes);

		auto other_hash = HashOp::Operation(*ldata, ConstantVector::IsNull(input));
		*hash_data = CombineHashScalar(*hash_data, other_hash);
	} else {
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// mix constant with non-constant, first get the constant value
			auto constant_hash = *ConstantVector::GetData<hash_t>(hashes);
			// now re-initialize the hashes vector to an empty flat vector
			hashes.SetVectorType(VectorType::FLAT_VECTOR);
			TightLoopCombineHashConstant<HAS_RSEL, T>(UnifiedVectorFormat::GetData<T>(idata), constant_hash,
			                                          FlatVector::GetData<hash_t>(hashes), rsel, count, idata.sel,
			                                          idata.validity);
		} else {
			D_ASSERT(hashes.GetVectorType() == VectorType::FLAT_VECTOR);
			TightLoopCombineHash<HAS_RSEL, T>(UnifiedVectorFormat::GetData<T>(idata),
			                                  FlatVector::GetData<hash_t>(hashes), rsel, count, idata.sel,
			                                  idata.validity);
		}
	}
}

template <bool HAS_RSEL>
static inline void CombineHashTypeSwitch(Vector &hashes, Vector &input, const SelectionVector *rsel, idx_t count) {
	D_ASSERT(hashes.GetType().id() == LogicalType::HASH);
	switch (input.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedLoopCombineHash<HAS_RSEL, int8_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT16:
		TemplatedLoopCombineHash<HAS_RSEL, int16_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT32:
		TemplatedLoopCombineHash<HAS_RSEL, int32_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT64:
		TemplatedLoopCombineHash<HAS_RSEL, int64_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedLoopCombineHash<HAS_RSEL, uint8_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedLoopCombineHash<HAS_RSEL, uint16_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedLoopCombineHash<HAS_RSEL, uint32_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedLoopCombineHash<HAS_RSEL, uint64_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::INT128:
		TemplatedLoopCombineHash<HAS_RSEL, hugeint_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedLoopCombineHash<HAS_RSEL, float>(input, hashes, rsel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedLoopCombineHash<HAS_RSEL, double>(input, hashes, rsel, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedLoopCombineHash<HAS_RSEL, interval_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedLoopCombineHash<HAS_RSEL, string_t>(input, hashes, rsel, count);
		break;
	case PhysicalType::STRUCT:
		StructLoopHash<HAS_RSEL, false>(input, hashes, rsel, count);
		break;
	case PhysicalType::LIST:
		ListLoopHash<HAS_RSEL, false>(input, hashes, rsel, count);
		break;
	default:
		throw InvalidTypeException(input.GetType(), "Invalid type for hash");
	}
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, idx_t count) {
	CombineHashTypeSwitch<false>(hashes, input, nullptr, count);
}

void VectorOperations::CombineHash(Vector &hashes, Vector &input, const SelectionVector &rsel, idx_t count) {
	CombineHashTypeSwitch<true>(hashes, input, &rsel, count);
}

} // namespace duckdb




namespace duckdb {

template <class T>
static void CopyToStorageLoop(UnifiedVectorFormat &vdata, idx_t count, data_ptr_t target) {
	auto ldata = UnifiedVectorFormat::GetData<T>(vdata);
	auto result_data = (T *)target;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			result_data[i] = NullValue<T>();
		} else {
			result_data[i] = ldata[idx];
		}
	}
}

void VectorOperations::WriteToStorage(Vector &source, idx_t count, data_ptr_t target) {
	if (count == 0) {
		return;
	}
	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	switch (source.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		CopyToStorageLoop<int8_t>(vdata, count, target);
		break;
	case PhysicalType::INT16:
		CopyToStorageLoop<int16_t>(vdata, count, target);
		break;
	case PhysicalType::INT32:
		CopyToStorageLoop<int32_t>(vdata, count, target);
		break;
	case PhysicalType::INT64:
		CopyToStorageLoop<int64_t>(vdata, count, target);
		break;
	case PhysicalType::UINT8:
		CopyToStorageLoop<uint8_t>(vdata, count, target);
		break;
	case PhysicalType::UINT16:
		CopyToStorageLoop<uint16_t>(vdata, count, target);
		break;
	case PhysicalType::UINT32:
		CopyToStorageLoop<uint32_t>(vdata, count, target);
		break;
	case PhysicalType::UINT64:
		CopyToStorageLoop<uint64_t>(vdata, count, target);
		break;
	case PhysicalType::INT128:
		CopyToStorageLoop<hugeint_t>(vdata, count, target);
		break;
	case PhysicalType::FLOAT:
		CopyToStorageLoop<float>(vdata, count, target);
		break;
	case PhysicalType::DOUBLE:
		CopyToStorageLoop<double>(vdata, count, target);
		break;
	case PhysicalType::INTERVAL:
		CopyToStorageLoop<interval_t>(vdata, count, target);
		break;
	default:
		throw NotImplementedException("Unimplemented type for WriteToStorage");
	}
}

template <class T>
static void ReadFromStorageLoop(data_ptr_t source, idx_t count, Vector &result) {
	auto ldata = (T *)source;
	auto result_data = FlatVector::GetData<T>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = ldata[i];
	}
}

void VectorOperations::ReadFromStorage(data_ptr_t source, idx_t count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ReadFromStorageLoop<int8_t>(source, count, result);
		break;
	case PhysicalType::INT16:
		ReadFromStorageLoop<int16_t>(source, count, result);
		break;
	case PhysicalType::INT32:
		ReadFromStorageLoop<int32_t>(source, count, result);
		break;
	case PhysicalType::INT64:
		ReadFromStorageLoop<int64_t>(source, count, result);
		break;
	case PhysicalType::UINT8:
		ReadFromStorageLoop<uint8_t>(source, count, result);
		break;
	case PhysicalType::UINT16:
		ReadFromStorageLoop<uint16_t>(source, count, result);
		break;
	case PhysicalType::UINT32:
		ReadFromStorageLoop<uint32_t>(source, count, result);
		break;
	case PhysicalType::UINT64:
		ReadFromStorageLoop<uint64_t>(source, count, result);
		break;
	case PhysicalType::INT128:
		ReadFromStorageLoop<hugeint_t>(source, count, result);
		break;
	case PhysicalType::FLOAT:
		ReadFromStorageLoop<float>(source, count, result);
		break;
	case PhysicalType::DOUBLE:
		ReadFromStorageLoop<double>(source, count, result);
		break;
	case PhysicalType::INTERVAL:
		ReadFromStorageLoop<interval_t>(source, count, result);
		break;
	default:
		throw NotImplementedException("Unimplemented type for ReadFromStorage");
	}
}

} // namespace duckdb






namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : default_fs(FileSystem::CreateLocal()) {
	VirtualFileSystem::RegisterSubSystem(FileCompressionType::GZIP, make_uniq<GZipFileSystem>());
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                                   FileCompressionType compression, FileOpener *opener) {
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto detect compression settings based on file name
		auto lower_path = StringUtil::Lower(path);
		if (StringUtil::EndsWith(lower_path, ".tmp")) {
			// strip .tmp
			lower_path = lower_path.substr(0, lower_path.length() - 4);
		}
		if (StringUtil::EndsWith(lower_path, ".gz")) {
			compression = FileCompressionType::GZIP;
		} else if (StringUtil::EndsWith(lower_path, ".zst")) {
			compression = FileCompressionType::ZSTD;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle
	auto file_handle = FindFileSystem(path).OpenFile(path, flags, lock, FileCompressionType::UNCOMPRESSED, opener);
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(std::move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = compressed_fs.find(compression);
		if (entry == compressed_fs.end()) {
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		file_handle = entry->second->OpenCompressedFile(std::move(file_handle), flags & FileFlags::FILE_FLAGS_WRITE);
	}
	return file_handle;
}

void VirtualFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	handle.file_system.Read(handle, buffer, nr_bytes, location);
}

void VirtualFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	handle.file_system.Write(handle, buffer, nr_bytes, location);
}

int64_t VirtualFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return handle.file_system.Read(handle, buffer, nr_bytes);
}

int64_t VirtualFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return handle.file_system.Write(handle, buffer, nr_bytes);
}

int64_t VirtualFileSystem::GetFileSize(FileHandle &handle) {
	return handle.file_system.GetFileSize(handle);
}
time_t VirtualFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return handle.file_system.GetLastModifiedTime(handle);
}
FileType VirtualFileSystem::GetFileType(FileHandle &handle) {
	return handle.file_system.GetFileType(handle);
}

void VirtualFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	handle.file_system.Truncate(handle, new_size);
}

void VirtualFileSystem::FileSync(FileHandle &handle) {
	handle.file_system.FileSync(handle);
}

// need to look up correct fs for this
bool VirtualFileSystem::DirectoryExists(const string &directory) {
	return FindFileSystem(directory).DirectoryExists(directory);
}
void VirtualFileSystem::CreateDirectory(const string &directory) {
	FindFileSystem(directory).CreateDirectory(directory);
}

void VirtualFileSystem::RemoveDirectory(const string &directory) {
	FindFileSystem(directory).RemoveDirectory(directory);
}

bool VirtualFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                  FileOpener *opener) {
	return FindFileSystem(directory).ListFiles(directory, callback, opener);
}

void VirtualFileSystem::MoveFile(const string &source, const string &target) {
	FindFileSystem(source).MoveFile(source, target);
}

bool VirtualFileSystem::FileExists(const string &filename) {
	return FindFileSystem(filename).FileExists(filename);
}

bool VirtualFileSystem::IsPipe(const string &filename) {
	return FindFileSystem(filename).IsPipe(filename);
}
void VirtualFileSystem::RemoveFile(const string &filename) {
	FindFileSystem(filename).RemoveFile(filename);
}

string VirtualFileSystem::PathSeparator(const string &path) {
	return FindFileSystem(path).PathSeparator(path);
}

vector<string> VirtualFileSystem::Glob(const string &path, FileOpener *opener) {
	return FindFileSystem(path).Glob(path, opener);
}

void VirtualFileSystem::RegisterSubSystem(unique_ptr<FileSystem> fs) {
	sub_systems.push_back(std::move(fs));
}

void VirtualFileSystem::UnregisterSubSystem(const string &name) {
	for (auto sub_system = sub_systems.begin(); sub_system != sub_systems.end(); sub_system++) {
		if (sub_system->get()->GetName() == name) {
			sub_systems.erase(sub_system);
			return;
		}
	}
	throw InvalidInputException("Could not find filesystem with name %s", name);
}

void VirtualFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	compressed_fs[compression_type] = std::move(fs);
}

vector<string> VirtualFileSystem::ListSubSystems() {
	vector<string> names(sub_systems.size());
	for (idx_t i = 0; i < sub_systems.size(); i++) {
		names[i] = sub_systems[i]->GetName();
	}
	return names;
}

std::string VirtualFileSystem::GetName() const {
	return "VirtualFileSystem";
}

void VirtualFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	unordered_set<string> new_disabled_file_systems;
	for (auto &name : names) {
		if (name.empty()) {
			continue;
		}
		if (new_disabled_file_systems.find(name) != new_disabled_file_systems.end()) {
			throw InvalidInputException("Duplicate disabled file system \"%s\"", name);
		}
		new_disabled_file_systems.insert(name);
	}
	for (auto &disabled_fs : disabled_file_systems) {
		if (new_disabled_file_systems.find(disabled_fs) == new_disabled_file_systems.end()) {
			throw InvalidInputException("File system \"%s\" has been disabled previously, it cannot be re-enabled",
			                            disabled_fs);
		}
	}
	disabled_file_systems = std::move(new_disabled_file_systems);
}

FileSystem &VirtualFileSystem::FindFileSystem(const string &path) {
	auto &fs = FindFileSystemInternal(path);
	if (!disabled_file_systems.empty() && disabled_file_systems.find(fs.GetName()) != disabled_file_systems.end()) {
		throw PermissionException("File system %s has been disabled by configuration", fs.GetName());
	}
	return fs;
}

FileSystem &VirtualFileSystem::FindFileSystemInternal(const string &path) {
	for (auto &sub_system : sub_systems) {
		if (sub_system->CanHandleFile(path)) {
			return *sub_system;
		}
	}
	return *default_fs;
}

} // namespace duckdb


namespace duckdb {

#ifdef DUCKDB_WINDOWS

std::wstring WindowsUtil::UTF8ToUnicode(const char *input) {
	idx_t result_size;

	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, nullptr, 0);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	auto buffer = make_unsafe_uniq_array<wchar_t>(result_size);
	result_size = MultiByteToWideChar(CP_UTF8, 0, input, -1, buffer.get(), result_size);
	if (result_size == 0) {
		throw IOException("Failure in MultiByteToWideChar");
	}
	return std::wstring(buffer.get(), result_size);
}

static string WideCharToMultiByteWrapper(LPCWSTR input, uint32_t code_page) {
	idx_t result_size;

	result_size = WideCharToMultiByte(code_page, 0, input, -1, 0, 0, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	auto buffer = make_unsafe_uniq_array<char>(result_size);
	result_size = WideCharToMultiByte(code_page, 0, input, -1, buffer.get(), result_size, 0, 0);
	if (result_size == 0) {
		throw IOException("Failure in WideCharToMultiByte");
	}
	return string(buffer.get(), result_size - 1);
}

string WindowsUtil::UnicodeToUTF8(LPCWSTR input) {
	return WideCharToMultiByteWrapper(input, CP_UTF8);
}

static string WindowsUnicodeToMBCS(LPCWSTR unicode_text, int use_ansi) {
	uint32_t code_page = use_ansi ? CP_ACP : CP_OEMCP;
	return WideCharToMultiByteWrapper(unicode_text, code_page);
}

string WindowsUtil::UTF8ToMBCS(const char *input, bool use_ansi) {
	auto unicode = WindowsUtil::UTF8ToUnicode(input);
	return WindowsUnicodeToMBCS(unicode.c_str(), use_ansi);
}

#endif

} // namespace duckdb







namespace duckdb {

template <class T>
struct AvgState {
	uint64_t count;
	T value;

	void Initialize() {
		this->count = 0;
	}

	void Combine(const AvgState<T> &other) {
		this->count += other.count;
		this->value += other.value;
	}
};

struct KahanAvgState {
	uint64_t count;
	double value;
	double err;

	void Initialize() {
		this->count = 0;
		this->err = 0.0;
	}

	void Combine(const KahanAvgState &other) {
		this->count += other.count;
		KahanAddInternal(other.value, this->value, this->err);
		KahanAddInternal(other.err, this->value, this->err);
	}
};

struct AverageDecimalBindData : public FunctionData {
	explicit AverageDecimalBindData(double scale) : scale(scale) {
	}

	double scale;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<AverageDecimalBindData>(scale);
	};

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<AverageDecimalBindData>();
		return scale == other.scale;
	}
};

struct AverageSetOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.Initialize();
	}
	template <class STATE>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE &state, idx_t count) {
		state.count += count;
	}
};

template <class T>
static T GetAverageDivident(uint64_t count, optional_ptr<FunctionData> bind_data) {
	T divident = T(count);
	if (bind_data) {
		auto &avg_bind_data = bind_data->Cast<AverageDecimalBindData>();
		divident *= avg_bind_data.scale;
	}
	return divident;
}

struct IntegerAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			double divident = GetAverageDivident<double>(state.count, finalize_data.input.bind_data);
			target = double(state.value) / divident;
		}
	}
};

struct IntegerAverageOperationHugeint : public BaseSumOperation<AverageSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			long double divident = GetAverageDivident<long double>(state.count, finalize_data.input.bind_data);
			target = Hugeint::Cast<long double>(state.value) / divident;
		}
	}
};

struct HugeintAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			long double divident = GetAverageDivident<long double>(state.count, finalize_data.input.bind_data);
			target = Hugeint::Cast<long double>(state.value) / divident;
		}
	}
};

struct NumericAverageOperation : public BaseSumOperation<AverageSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.value / state.count;
		}
	}
};

struct KahanAverageOperation : public BaseSumOperation<AverageSetOperation, KahanAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = (state.value / state.count) + (state.err / state.count);
		}
	}
};

AggregateFunction GetAverageAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16: {
		return AggregateFunction::UnaryAggregate<AvgState<int64_t>, int16_t, double, IntegerAverageOperation>(
		    LogicalType::SMALLINT, LogicalType::DOUBLE);
	}
	case PhysicalType::INT32: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int32_t, double, IntegerAverageOperationHugeint>(
		    LogicalType::INTEGER, LogicalType::DOUBLE);
	}
	case PhysicalType::INT64: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, int64_t, double, IntegerAverageOperationHugeint>(
		    LogicalType::BIGINT, LogicalType::DOUBLE);
	}
	case PhysicalType::INT128: {
		return AggregateFunction::UnaryAggregate<AvgState<hugeint_t>, hugeint_t, double, HugeintAverageOperation>(
		    LogicalType::HUGEINT, LogicalType::DOUBLE);
	}
	default:
		throw InternalException("Unimplemented average aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalAvg(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetAverageAggregate(decimal_type.InternalType());
	function.name = "avg";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DOUBLE;
	return make_uniq<AverageDecimalBindData>(
	    Hugeint::Cast<double>(Hugeint::POWERS_OF_TEN[DecimalType::GetScale(decimal_type)]));
}

AggregateFunctionSet AvgFun::GetFunctions() {
	AggregateFunctionSet avg;

	avg.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalAvg));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT16));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT32));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT64));
	avg.AddFunction(GetAverageAggregate(PhysicalType::INT128));
	avg.AddFunction(AggregateFunction::UnaryAggregate<AvgState<double>, double, double, NumericAverageOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	return avg;
}

AggregateFunction FAvgFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KahanAvgState, double, double, KahanAverageOperation>(LogicalType::DOUBLE,
	                                                                                               LogicalType::DOUBLE);
}

} // namespace duckdb






namespace duckdb {

AggregateFunction CorrFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}
} // namespace duckdb




namespace duckdb {

AggregateFunction CovarPopFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

AggregateFunction CovarSampFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CovarState, double, double, double, CovarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb




#include <cmath>

namespace duckdb {

AggregateFunction StdDevSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE);
}

AggregateFunction StdDevPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevPopOperation>(LogicalType::DOUBLE,
	                                                                                          LogicalType::DOUBLE);
}

AggregateFunction VarPopFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarPopOperation>(LogicalType::DOUBLE,
	                                                                                       LogicalType::DOUBLE);
}

AggregateFunction VarSampFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(LogicalType::DOUBLE,
	                                                                                        LogicalType::DOUBLE);
}

AggregateFunction StandardErrorOfTheMeanFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<StddevState, double, double, StandardErrorOfTheMeanOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb







namespace duckdb {

struct ApproxDistinctCountState {
	ApproxDistinctCountState() : log(nullptr) {
	}
	~ApproxDistinctCountState() {
		if (log) {
			delete log;
		}
	}

	HyperLogLog *log;
};

struct ApproxCountDistinctFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.log = nullptr;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.log) {
			return;
		}
		if (!target.log) {
			target.log = new HyperLogLog();
		}
		D_ASSERT(target.log);
		D_ASSERT(source.log);
		auto new_log = target.log->MergePointer(*source.log);
		delete target.log;
		target.log = new_log;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.log) {
			target = state.log->Count();
		} else {
			target = 0;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.log) {
			delete state.log;
			state.log = nullptr;
		}
	}
};

static void ApproxCountDistinctSimpleUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                                    data_ptr_t state, idx_t count) {
	D_ASSERT(input_count == 1);

	auto agg_state = reinterpret_cast<ApproxDistinctCountState *>(state);
	if (!agg_state->log) {
		agg_state->log = new HyperLogLog();
	}

	UnifiedVectorFormat vdata;
	inputs[0].ToUnifiedFormat(count, vdata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];
	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	agg_state->log->AddToLog(vdata, count, indices, counts);
}

static void ApproxCountDistinctUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count,
                                              Vector &state_vector, idx_t count) {
	D_ASSERT(input_count == 1);

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetDataNoConst<ApproxDistinctCountState *>(sdata);

	for (idx_t i = 0; i < count; i++) {
		auto agg_state = states[sdata.sel->get_index(i)];
		if (!agg_state->log) {
			agg_state->log = new HyperLogLog();
		}
	}

	UnifiedVectorFormat vdata;
	inputs[0].ToUnifiedFormat(count, vdata);

	if (count > STANDARD_VECTOR_SIZE) {
		throw InternalException("ApproxCountDistinct - count must be at most vector size");
	}
	uint64_t indices[STANDARD_VECTOR_SIZE];
	uint8_t counts[STANDARD_VECTOR_SIZE];
	HyperLogLog::ProcessEntries(vdata, inputs[0].GetType(), indices, counts, count);
	HyperLogLog::AddToLogs(vdata, count, indices, counts, reinterpret_cast<HyperLogLog ***>(states), sdata.sel);
}

AggregateFunction GetApproxCountDistinctFunction(const LogicalType &input_type) {
	auto fun = AggregateFunction(
	    {input_type}, LogicalTypeId::BIGINT, AggregateFunction::StateSize<ApproxDistinctCountState>,
	    AggregateFunction::StateInitialize<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    ApproxCountDistinctUpdateFunction,
	    AggregateFunction::StateCombine<ApproxDistinctCountState, ApproxCountDistinctFunction>,
	    AggregateFunction::StateFinalize<ApproxDistinctCountState, int64_t, ApproxCountDistinctFunction>,
	    ApproxCountDistinctSimpleUpdateFunction, nullptr,
	    AggregateFunction::StateDestroy<ApproxDistinctCountState, ApproxCountDistinctFunction>);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

AggregateFunctionSet ApproxCountDistinctFun::GetFunctions() {
	AggregateFunctionSet approx_count("approx_count_distinct");
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UTINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::USMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UINTEGER));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::UBIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TINYINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::SMALLINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::BIGINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::HUGEINT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::FLOAT));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::DOUBLE));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::VARCHAR));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::TIMESTAMP_TZ));
	approx_count.AddFunction(GetApproxCountDistinctFunction(LogicalType::BLOB));
	return approx_count;
}

} // namespace duckdb







namespace duckdb {

struct ArgMinMaxStateBase {
	ArgMinMaxStateBase() : is_initialized(false) {
	}

	template <class T>
	static inline void CreateValue(T &value) {
	}

	template <class T>
	static inline void DestroyValue(T &value) {
	}

	template <class T>
	static inline void AssignValue(T &target, T new_value, bool is_initialized) {
		target = new_value;
	}

	template <typename T>
	static inline void ReadValue(Vector &result, T &arg, T &target) {
		target = arg;
	}

	bool is_initialized;
};

// Out-of-line specialisations
template <>
void ArgMinMaxStateBase::CreateValue(Vector *&value) {
	value = nullptr;
}

template <>
void ArgMinMaxStateBase::DestroyValue(string_t &value) {
	if (!value.IsInlined()) {
		delete[] value.GetData();
	}
}

template <>
void ArgMinMaxStateBase::DestroyValue(Vector *&value) {
	delete value;
	value = nullptr;
}

template <>
void ArgMinMaxStateBase::AssignValue(string_t &target, string_t new_value, bool is_initialized) {
	if (is_initialized) {
		DestroyValue(target);
	}
	if (new_value.IsInlined()) {
		target = new_value;
	} else {
		// non-inlined string, need to allocate space for it
		auto len = new_value.GetSize();
		auto ptr = new char[len];
		memcpy(ptr, new_value.GetData(), len);

		target = string_t(ptr, len);
	}
}

template <>
void ArgMinMaxStateBase::ReadValue(Vector &result, string_t &arg, string_t &target) {
	target = StringVector::AddStringOrBlob(result, arg);
}

template <class A, class B>
struct ArgMinMaxState : public ArgMinMaxStateBase {
	using ARG_TYPE = A;
	using BY_TYPE = B;

	ARG_TYPE arg;
	BY_TYPE value;

	ArgMinMaxState() {
		CreateValue(arg);
		CreateValue(value);
	}

	~ArgMinMaxState() {
		if (is_initialized) {
			DestroyValue(arg);
			DestroyValue(value);
			is_initialized = false;
		}
	}
};

template <class COMPARATOR>
struct ArgMinMaxBase {

	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &x, const B_TYPE &y, AggregateBinaryInput &) {
		if (!state.is_initialized) {
			STATE::template AssignValue<A_TYPE>(state.arg, x, false);
			STATE::template AssignValue<B_TYPE>(state.value, y, false);
			state.is_initialized = true;
		} else {
			OP::template Execute<A_TYPE, B_TYPE, STATE>(state, x, y);
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE>
	static void Execute(STATE &state, A_TYPE x_data, B_TYPE y_data) {
		if (COMPARATOR::Operation(y_data, state.value)) {
			STATE::template AssignValue<A_TYPE>(state.arg, x_data, true);
			STATE::template AssignValue<B_TYPE>(state.value, y_data, true);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			STATE::template AssignValue(target.arg, source.arg, target.is_initialized);
			STATE::template AssignValue(target.value, source.value, target.is_initialized);
			target.is_initialized = true;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized) {
			finalize_data.ReturnNull();
		} else {
			STATE::template ReadValue(finalize_data.result, state.arg, target);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <typename COMPARATOR>
struct VectorArgMinMaxBase : ArgMinMaxBase<COMPARATOR> {
	template <class STATE>
	static void AssignVector(STATE &state, Vector &arg, const idx_t idx) {
		if (!state.is_initialized) {
			state.arg = new Vector(arg.GetType());
			state.arg->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		sel_t selv = idx;
		SelectionVector sel(&selv);
		VectorOperations::Copy(arg, *state.arg, sel, 1, 0, 0);
	}

	template <class STATE>
	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &arg = inputs[0];
		UnifiedVectorFormat adata;
		arg.ToUnifiedFormat(count, adata);

		using BY_TYPE = typename STATE::BY_TYPE;
		auto &by = inputs[1];
		UnifiedVectorFormat bdata;
		by.ToUnifiedFormat(count, bdata);
		const auto bys = UnifiedVectorFormat::GetData<BY_TYPE>(bdata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto states = (STATE **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			const auto bidx = bdata.sel->get_index(i);
			if (!bdata.validity.RowIsValid(bidx)) {
				continue;
			}
			const auto bval = bys[bidx];

			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];
			if (!state.is_initialized) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval, false);
				AssignVector(state, arg, i);
				state.is_initialized = true;

			} else if (COMPARATOR::template Operation<BY_TYPE>(bval, state.value)) {
				STATE::template AssignValue<BY_TYPE>(state.value, bval, true);
				AssignVector(state, arg, i);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_initialized) {
			return;
		}
		if (!target.is_initialized || COMPARATOR::Operation(source.value, target.value)) {
			STATE::template AssignValue(target.value, source.value, target.is_initialized);
			AssignVector(target, *source.arg, 0);
			target.is_initialized = true;
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.is_initialized) {
			finalize_data.ReturnNull();
		} else {
			VectorOperations::Copy(*state.arg, finalize_data.result, 1, 0, finalize_data.result_idx);
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	return AggregateFunction(
	    {type, by_type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    OP::template Update<STATE>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, OP::Bind, AggregateFunction::StateDestroy<STATE, OP>);
}

template <class OP, class ARG_TYPE>
AggregateFunction GetVectorArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type);
	case PhysicalType::DOUBLE:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type);
	case PhysicalType::VARCHAR:
		return GetVectorArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class ARG_TYPE>
void AddVectorArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::INTEGER, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BIGINT, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DOUBLE, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::VARCHAR, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DATE, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP_TZ, type));
	fun.AddFunction(GetVectorArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BLOB, type));
}

template <class OP, class ARG_TYPE, class BY_TYPE>
AggregateFunction GetArgMinMaxFunctionInternal(const LogicalType &by_type, const LogicalType &type) {
	using STATE = ArgMinMaxState<ARG_TYPE, BY_TYPE>;
	auto function = AggregateFunction::BinaryAggregate<STATE, ARG_TYPE, BY_TYPE, ARG_TYPE, OP>(type, by_type, type);
	if (type.InternalType() == PhysicalType::VARCHAR || by_type.InternalType() == PhysicalType::VARCHAR) {
		function.destructor = AggregateFunction::StateDestroy<STATE, OP>;
	}
	return function;
}

template <class OP, class ARG_TYPE>
AggregateFunction GetArgMinMaxFunctionBy(const LogicalType &by_type, const LogicalType &type) {
	switch (by_type.InternalType()) {
	case PhysicalType::INT32:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int32_t>(by_type, type);
	case PhysicalType::INT64:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, int64_t>(by_type, type);
	case PhysicalType::DOUBLE:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, double>(by_type, type);
	case PhysicalType::VARCHAR:
		return GetArgMinMaxFunctionInternal<OP, ARG_TYPE, string_t>(by_type, type);
	default:
		throw InternalException("Unimplemented arg_min/arg_max aggregate");
	}
}

template <class OP, class ARG_TYPE>
void AddArgMinMaxFunctionBy(AggregateFunctionSet &fun, const LogicalType &type) {
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::INTEGER, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BIGINT, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DOUBLE, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::VARCHAR, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::DATE, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::TIMESTAMP_TZ, type));
	fun.AddFunction(GetArgMinMaxFunctionBy<OP, ARG_TYPE>(LogicalType::BLOB, type));
}

template <class COMPARATOR>
static void AddArgMinMaxFunctions(AggregateFunctionSet &fun) {
	using OP = ArgMinMaxBase<COMPARATOR>;
	AddArgMinMaxFunctionBy<OP, int32_t>(fun, LogicalType::INTEGER);
	AddArgMinMaxFunctionBy<OP, int64_t>(fun, LogicalType::BIGINT);
	AddArgMinMaxFunctionBy<OP, double>(fun, LogicalType::DOUBLE);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::VARCHAR);
	AddArgMinMaxFunctionBy<OP, date_t>(fun, LogicalType::DATE);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP);
	AddArgMinMaxFunctionBy<OP, timestamp_t>(fun, LogicalType::TIMESTAMP_TZ);
	AddArgMinMaxFunctionBy<OP, string_t>(fun, LogicalType::BLOB);

	using VECTOR_OP = VectorArgMinMaxBase<COMPARATOR>;
	AddVectorArgMinMaxFunctionBy<VECTOR_OP, Vector *>(fun, LogicalType::ANY);
}

AggregateFunctionSet ArgMinFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<LessThan>(fun);
	return fun;
}

AggregateFunctionSet ArgMaxFun::GetFunctions() {
	AggregateFunctionSet fun;
	AddArgMinMaxFunctions<GreaterThan>(fun);
	return fun;
}

} // namespace duckdb








namespace duckdb {

template <class T>
struct BitState {
	bool is_set;
	T value;
};

template <class OP>
static AggregateFunction GetBitfieldUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<BitState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitwiseOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		//  If there are no matching rows, returns a null value.
		state.is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.is_set) {
			OP::template Assign(state, input);
			state.is_set = true;
		} else {
			OP::template Execute(state, input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		state.value = input;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target.is_set) {
			// target is NULL, use source value directly.
			OP::template Assign(target, source.value);
			target.is_set = true;
		} else {
			OP::template Execute(target, source.value);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct BitAndOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value &= input;
	}
};

struct BitOrOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value |= input;
	}
};

struct BitXorOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value ^= input;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

struct BitStringBitwiseOperation : public BitwiseOperation {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.is_set && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		D_ASSERT(state.is_set == false);
		if (input.IsInlined()) {
			state.value = input;
		} else { // non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);

			state.value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = finalize_data.ReturnString(state.value);
		}
	}
};

struct BitStringAndOperation : public BitStringBitwiseOperation {

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseAnd(input, state.value, state.value);
	}
};

struct BitStringOrOperation : public BitStringBitwiseOperation {

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseOr(input, state.value, state.value);
	}
};

struct BitStringXorOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseXor(input, state.value, state.value);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

AggregateFunctionSet BitAndFun::GetFunctions() {
	AggregateFunctionSet bit_and;
	for (auto &type : LogicalType::Integral()) {
		bit_and.AddFunction(GetBitfieldUnaryAggregate<BitAndOperation>(type));
	}

	bit_and.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringAndOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_and;
}

AggregateFunctionSet BitOrFun::GetFunctions() {
	AggregateFunctionSet bit_or;
	for (auto &type : LogicalType::Integral()) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	bit_or.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringOrOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_or;
}

AggregateFunctionSet BitXorFun::GetFunctions() {
	AggregateFunctionSet bit_xor;
	for (auto &type : LogicalType::Integral()) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	bit_xor.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringXorOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_xor;
}

} // namespace duckdb










namespace duckdb {

template <class INPUT_TYPE>
struct BitAggState {
	bool is_set;
	string_t value;
	INPUT_TYPE min;
	INPUT_TYPE max;
};

struct BitstringAggBindData : public FunctionData {
	Value min;
	Value max;

	BitstringAggBindData() {
	}

	BitstringAggBindData(Value min, Value max) : min(std::move(min)), max(std::move(max)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BitstringAggBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BitstringAggBindData>();
		if (min.IsNull() && other.min.IsNull() && max.IsNull() && other.max.IsNull()) {
			return true;
		}
		if (Value::NotDistinctFrom(min, other.min) && Value::NotDistinctFrom(max, other.max)) {
			return true;
		}
		return false;
	}
};

struct BitStringAggOperation {
	static constexpr const idx_t MAX_BIT_RANGE = 1000000000; // for now capped at 1 billion bits

	template <class STATE>
	static void Initialize(STATE &state) {
		state.is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto &bind_agg_data = unary_input.input.bind_data->template Cast<BitstringAggBindData>();
		if (!state.is_set) {
			if (bind_agg_data.min.IsNull() || bind_agg_data.max.IsNull()) {
				throw BinderException(
				    "Could not retrieve required statistics. Alternatively, try by providing the statistics "
				    "explicitly: BITSTRING_AGG(col, min, max) ");
			}
			state.min = bind_agg_data.min.GetValue<INPUT_TYPE>();
			state.max = bind_agg_data.max.GetValue<INPUT_TYPE>();
			idx_t bit_range =
			    GetRange(bind_agg_data.min.GetValue<INPUT_TYPE>(), bind_agg_data.max.GetValue<INPUT_TYPE>());
			if (bit_range > MAX_BIT_RANGE) {
				throw OutOfRangeException(
				    "The range between min and max value (%s <-> %s) is too large for bitstring aggregation",
				    NumericHelper::ToString(state.min), NumericHelper::ToString(state.max));
			}
			idx_t len = Bit::ComputeBitstringLen(bit_range);
			auto target = len > string_t::INLINE_LENGTH ? string_t(new char[len], len) : string_t(len);
			Bit::SetEmptyBitString(target, bit_range);

			state.value = target;
			state.is_set = true;
		}
		if (input >= state.min && input <= state.max) {
			Execute(state, input, bind_agg_data.min.GetValue<INPUT_TYPE>());
		} else {
			throw OutOfRangeException("Value %s is outside of provided min and max range (%s <-> %s)",
			                          NumericHelper::ToString(input), NumericHelper::ToString(state.min),
			                          NumericHelper::ToString(state.max));
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE>
	static idx_t GetRange(INPUT_TYPE min, INPUT_TYPE max) {
		D_ASSERT(max >= min);
		INPUT_TYPE result;
		if (!TrySubtractOperator::Operation(max, min, result)) {
			return NumericLimits<idx_t>::Maximum();
		}
		idx_t val(result);
		if (val == NumericLimits<idx_t>::Maximum()) {
			return val;
		}
		return val + 1;
	}

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, INPUT_TYPE min) {
		Bit::SetBit(state.value, input - min, 1);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			return;
		}
		if (!target.is_set) {
			Assign(target, source.value);
			target.is_set = true;
			target.min = source.min;
			target.max = source.max;
		} else {
			Bit::BitwiseOr(source.value, target.value, target.value);
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		D_ASSERT(state.is_set == false);
		if (input.IsInlined()) {
			state.value = input;
		} else { // non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);
			state.value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.is_set && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <>
void BitStringAggOperation::Execute(BitAggState<hugeint_t> &state, hugeint_t input, hugeint_t min) {
	idx_t val;
	if (Hugeint::TryCast(input - min, val)) {
		Bit::SetBit(state.value, val, 1);
	} else {
		throw OutOfRangeException("Range too large for bitstring aggregation");
	}
}

template <>
idx_t BitStringAggOperation::GetRange(hugeint_t min, hugeint_t max) {
	hugeint_t result;
	if (!TrySubtractOperator::Operation(max, min, result)) {
		return NumericLimits<idx_t>::Maximum();
	}
	idx_t range;
	if (!Hugeint::TryCast(result + 1, range)) {
		return NumericLimits<idx_t>::Maximum();
	}
	return range;
}

unique_ptr<BaseStatistics> BitstringPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                                   AggregateStatisticsInput &input) {

	if (!NumericStats::HasMinMax(input.child_stats[0])) {
		throw BinderException("Could not retrieve required statistics. Alternatively, try by providing the statistics "
		                      "explicitly: BITSTRING_AGG(col, min, max) ");
	}
	auto &bind_agg_data = input.bind_data->Cast<BitstringAggBindData>();
	bind_agg_data.min = NumericStats::Min(input.child_stats[0]);
	bind_agg_data.max = NumericStats::Max(input.child_stats[0]);
	return nullptr;
}

unique_ptr<FunctionData> BindBitstringAgg(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 3) {
		if (!arguments[1]->IsFoldable() || !arguments[2]->IsFoldable()) {
			throw BinderException("bitstring_agg requires a constant min and max argument");
		}
		auto min = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		auto max = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		Function::EraseArgument(function, arguments, 2);
		Function::EraseArgument(function, arguments, 1);
		return make_uniq<BitstringAggBindData>(min, max);
	}
	return make_uniq<BitstringAggBindData>();
}

template <class TYPE>
static void BindBitString(AggregateFunctionSet &bitstring_agg, const LogicalTypeId &type) {
	auto function =
	    AggregateFunction::UnaryAggregateDestructor<BitAggState<TYPE>, TYPE, string_t, BitStringAggOperation>(
	        type, LogicalType::BIT);
	function.bind = BindBitstringAgg;              // create new a 'BitstringAggBindData'
	function.statistics = BitstringPropagateStats; // stores min and max from column stats in BitstringAggBindData
	bitstring_agg.AddFunction(function); // uses the BitstringAggBindData to access statistics for creating bitstring
	function.arguments = {type, type, type};
	function.statistics = nullptr; // min and max are provided as arguments
	bitstring_agg.AddFunction(function);
}

void GetBitStringAggregate(const LogicalType &type, AggregateFunctionSet &bitstring_agg) {
	switch (type.id()) {
	case LogicalType::TINYINT: {
		return BindBitString<int8_t>(bitstring_agg, type.id());
	}
	case LogicalType::SMALLINT: {
		return BindBitString<int16_t>(bitstring_agg, type.id());
	}
	case LogicalType::INTEGER: {
		return BindBitString<int32_t>(bitstring_agg, type.id());
	}
	case LogicalType::BIGINT: {
		return BindBitString<int64_t>(bitstring_agg, type.id());
	}
	case LogicalType::HUGEINT: {
		return BindBitString<hugeint_t>(bitstring_agg, type.id());
	}
	case LogicalType::UTINYINT: {
		return BindBitString<uint8_t>(bitstring_agg, type.id());
	}
	case LogicalType::USMALLINT: {
		return BindBitString<uint16_t>(bitstring_agg, type.id());
	}
	case LogicalType::UINTEGER: {
		return BindBitString<uint32_t>(bitstring_agg, type.id());
	}
	case LogicalType::UBIGINT: {
		return BindBitString<uint64_t>(bitstring_agg, type.id());
	}
	default:
		throw InternalException("Unimplemented bitstring aggregate");
	}
}

AggregateFunctionSet BitstringAggFun::GetFunctions() {
	AggregateFunctionSet bitstring_agg("bitstring_agg");
	for (auto &type : LogicalType::Integral()) {
		GetBitStringAggregate(type, bitstring_agg);
	}
	return bitstring_agg;
}

} // namespace duckdb






namespace duckdb {

struct BoolState {
	bool empty;
	bool val;
};

struct BoolAndFunFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.val = true;
		state.empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.val = target.val && source.val;
		target.empty = target.empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.empty) {
			finalize_data.ReturnNull();
			return;
		}
		target = state.val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		state.empty = false;
		state.val = input && state.val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};

struct BoolOrFunFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.val = false;
		state.empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.val = target.val || source.val;
		target.empty = target.empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.empty) {
			finalize_data.ReturnNull();
			return;
		}
		target = state.val;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		state.empty = false;
		state.val = input || state.val;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction BoolOrFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolOrFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction BoolAndFun::GetFunction() {
	auto fun = AggregateFunction::UnaryAggregate<BoolState, bool, bool, BoolAndFunFunction>(
	    LogicalType(LogicalTypeId::BOOLEAN), LogicalType::BOOLEAN);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

} // namespace duckdb








namespace duckdb {

template <class T>
struct EntropyState {
	using DistinctMap = unordered_map<T, idx_t>;

	idx_t count;
	DistinctMap *distinct;

	EntropyState &operator=(const EntropyState &other) = delete;

	EntropyState &Assign(const EntropyState &other) {
		D_ASSERT(!distinct);
		distinct = new DistinctMap(*other.distinct);
		count = other.count;
		return *this;
	}
};

struct EntropyFunctionBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.distinct = nullptr;
		state.count = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.distinct) {
			return;
		}
		if (!target.distinct) {
			target.Assign(source);
			return;
		}
		for (auto &val : *source.distinct) {
			auto value = val.first;
			(*target.distinct)[value] += val.second;
		}
		target.count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		double count = state.count;
		if (state.distinct) {
			double entropy = 0;
			for (auto &val : *state.distinct) {
				entropy += (val.second / count) * log2(count / val.second);
			}
			target = entropy;
		} else {
			target = 0;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.distinct) {
			delete state.distinct;
		}
	}
};

struct EntropyFunction : EntropyFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (!state.distinct) {
			state.distinct = new unordered_map<INPUT_TYPE, idx_t>();
		}
		(*state.distinct)[input]++;
		state.count++;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

struct EntropyFunctionString : EntropyFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (!state.distinct) {
			state.distinct = new unordered_map<string, idx_t>();
		}
		auto value = input.GetString();
		(*state.distinct)[value]++;
		state.count++;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

template <typename INPUT_TYPE, typename RESULT_TYPE>
AggregateFunction GetEntropyFunction(const LogicalType &input_type, const LogicalType &result_type) {
	auto fun =
	    AggregateFunction::UnaryAggregateDestructor<EntropyState<INPUT_TYPE>, INPUT_TYPE, RESULT_TYPE, EntropyFunction>(
	        input_type, result_type);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

AggregateFunction GetEntropyFunctionInternal(PhysicalType type) {
	switch (type) {
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint16_t>, uint16_t, double, EntropyFunction>(
		    LogicalType::USMALLINT, LogicalType::DOUBLE);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint32_t>, uint32_t, double, EntropyFunction>(
		    LogicalType::UINTEGER, LogicalType::DOUBLE);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<uint64_t>, uint64_t, double, EntropyFunction>(
		    LogicalType::UBIGINT, LogicalType::DOUBLE);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int16_t>, int16_t, double, EntropyFunction>(
		    LogicalType::SMALLINT, LogicalType::DOUBLE);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int32_t>, int32_t, double, EntropyFunction>(
		    LogicalType::INTEGER, LogicalType::DOUBLE);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<int64_t>, int64_t, double, EntropyFunction>(
		    LogicalType::BIGINT, LogicalType::DOUBLE);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<float>, float, double, EntropyFunction>(
		    LogicalType::FLOAT, LogicalType::DOUBLE);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<double>, double, double, EntropyFunction>(
		    LogicalType::DOUBLE, LogicalType::DOUBLE);
	case PhysicalType::VARCHAR:
		return AggregateFunction::UnaryAggregateDestructor<EntropyState<string>, string_t, double,
		                                                   EntropyFunctionString>(LogicalType::VARCHAR,
		                                                                          LogicalType::DOUBLE);

	default:
		throw InternalException("Unimplemented approximate_count aggregate");
	}
}

AggregateFunction GetEntropyFunction(PhysicalType type) {
	auto fun = GetEntropyFunctionInternal(type);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

AggregateFunctionSet EntropyFun::GetFunctions() {
	AggregateFunctionSet entropy("entropy");
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT16));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT32));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::UINT64));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::FLOAT));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT16));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT32));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::INT64));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::DOUBLE));
	entropy.AddFunction(GetEntropyFunction(PhysicalType::VARCHAR));
	entropy.AddFunction(GetEntropyFunction<int64_t, double>(LogicalType::TIMESTAMP, LogicalType::DOUBLE));
	entropy.AddFunction(GetEntropyFunction<int64_t, double>(LogicalType::TIMESTAMP_TZ, LogicalType::DOUBLE));
	return entropy;
}

} // namespace duckdb






namespace duckdb {

struct KurtosisState {
	idx_t n;
	double sum;
	double sum_sqr;
	double sum_cub;
	double sum_four;
};

struct KurtosisOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.n = 0;
		state.sum = state.sum_sqr = state.sum_cub = state.sum_four = 0.0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		state.n++;
		state.sum += input;
		state.sum_sqr += pow(input, 2);
		state.sum_cub += pow(input, 3);
		state.sum_four += pow(input, 4);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.n == 0) {
			return;
		}
		target.n += source.n;
		target.sum += source.sum;
		target.sum_sqr += source.sum_sqr;
		target.sum_cub += source.sum_cub;
		target.sum_four += source.sum_four;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		auto n = (double)state.n;
		if (n <= 3) {
			finalize_data.ReturnNull();
			return;
		}
		double temp = 1 / n;
		//! This is necessary due to linux 32 bits
		long double temp_aux = 1 / n;
		if (state.sum_sqr - state.sum * state.sum * temp == 0 ||
		    state.sum_sqr - state.sum * state.sum * temp_aux == 0) {
			finalize_data.ReturnNull();
			return;
		}
		double m4 =
		    temp * (state.sum_four - 4 * state.sum_cub * state.sum * temp +
		            6 * state.sum_sqr * state.sum * state.sum * temp * temp - 3 * pow(state.sum, 4) * pow(temp, 3));

		double m2 = temp * (state.sum_sqr - state.sum * state.sum * temp);
		if (m2 <= 0 || ((n - 2) * (n - 3)) == 0) { // m2 shouldn't be below 0 but floating points are weird
			finalize_data.ReturnNull();
			return;
		}
		target = (n - 1) * ((n + 1) * m4 / (m2 * m2) - 3 * (n - 1)) / ((n - 2) * (n - 3));
		if (!Value::DoubleIsFinite(target)) {
			throw OutOfRangeException("Kurtosis is out of range!");
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction KurtosisFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KurtosisState, double, double, KurtosisOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE);
}

} // namespace duckdb







namespace duckdb {

template <class T>
struct MinMaxState {
	T value;
	bool isset;
};

template <class OP>
static AggregateFunction GetUnaryAggregate(LogicalType type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregate<MinMaxState<int16_t>, int16_t, int16_t, OP>(type, type);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregate<MinMaxState<int32_t>, int32_t, int32_t, OP>(type, type);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregate<MinMaxState<int64_t>, int64_t, int64_t, OP>(type, type);
	case PhysicalType::UINT8:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case PhysicalType::UINT16:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case PhysicalType::UINT32:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case PhysicalType::UINT64:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregate<MinMaxState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregate<MinMaxState<float>, float, float, OP>(type, type);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregate<MinMaxState<double>, double, double, OP>(type, type);
	case PhysicalType::INTERVAL:
		return AggregateFunction::UnaryAggregate<MinMaxState<interval_t>, interval_t, interval_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented type for min/max aggregate");
	}
}

struct MinMaxBase {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.isset = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		if (!state.isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input, unary_input.input);
			state.isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input, unary_input.input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (!state.isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input, unary_input.input);
			state.isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input, unary_input.input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct NumericMinMaxBase : public MinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		state.value = input;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

struct MinOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			state.value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			target = source;
		} else if (GreaterThan::Operation(target.value, source.value)) {
			target.value = source.value;
		}
	}
};

struct MaxOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state.value)) {
			state.value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			target = source;
		} else if (LessThan::Operation(target.value, source.value)) {
			target.value = source.value;
		}
	}
};

struct StringMinMaxBase : public MinMaxBase {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.isset && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		Destroy(state, input_data);
		if (input.IsInlined()) {
			state.value = input;
		} else {
			// non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);

			state.value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target.isset) {
			// target is NULL, use source value directly
			Assign(target, source.value, input_data);
			target.isset = true;
		} else {
			OP::template Execute<string_t, STATE>(target, source.value, input_data);
		}
	}
};

struct MinOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (LessThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}
};

struct MaxOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input, AggregateInputData &input_data) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state.value)) {
			Assign(state, input, input_data);
		}
	}
};

template <typename T, class OP>
static bool TemplatedOptimumType(Vector &left, idx_t lidx, idx_t lcount, Vector &right, idx_t ridx, idx_t rcount) {
	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(lcount, lvdata);
	right.ToUnifiedFormat(rcount, rvdata);

	lidx = lvdata.sel->get_index(lidx);
	ridx = rvdata.sel->get_index(ridx);

	auto ldata = UnifiedVectorFormat::GetData<T>(lvdata);
	auto rdata = UnifiedVectorFormat::GetData<T>(rvdata);

	auto &lval = ldata[lidx];
	auto &rval = rdata[ridx];

	auto lnull = !lvdata.validity.RowIsValid(lidx);
	auto rnull = !rvdata.validity.RowIsValid(ridx);

	return OP::Operation(lval, rval, lnull, rnull);
}

template <class OP>
static bool TemplatedOptimumList(Vector &left, idx_t lidx, idx_t lcount, Vector &right, idx_t ridx, idx_t rcount);

template <class OP>
static bool TemplatedOptimumStruct(Vector &left, idx_t lidx, idx_t lcount, Vector &right, idx_t ridx, idx_t rcount);

template <class OP>
static bool TemplatedOptimumValue(Vector &left, idx_t lidx, idx_t lcount, Vector &right, idx_t ridx, idx_t rcount) {
	D_ASSERT(left.GetType() == right.GetType());
	switch (left.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedOptimumType<int8_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::INT16:
		return TemplatedOptimumType<int16_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::INT32:
		return TemplatedOptimumType<int32_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::INT64:
		return TemplatedOptimumType<int64_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::UINT8:
		return TemplatedOptimumType<uint8_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::UINT16:
		return TemplatedOptimumType<uint16_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::UINT32:
		return TemplatedOptimumType<uint32_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::UINT64:
		return TemplatedOptimumType<uint64_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::INT128:
		return TemplatedOptimumType<hugeint_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::FLOAT:
		return TemplatedOptimumType<float, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::DOUBLE:
		return TemplatedOptimumType<double, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::INTERVAL:
		return TemplatedOptimumType<interval_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::VARCHAR:
		return TemplatedOptimumType<string_t, OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::LIST:
		return TemplatedOptimumList<OP>(left, lidx, lcount, right, ridx, rcount);
	case PhysicalType::STRUCT:
		return TemplatedOptimumStruct<OP>(left, lidx, lcount, right, ridx, rcount);
	default:
		throw InternalException("Invalid type for distinct comparison");
	}
}

template <class OP>
static bool TemplatedOptimumStruct(Vector &left, idx_t lidx_p, idx_t lcount, Vector &right, idx_t ridx_p,
                                   idx_t rcount) {
	// STRUCT dictionaries apply to all the children
	// so map the indexes first
	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(lcount, lvdata);
	right.ToUnifiedFormat(rcount, rvdata);

	idx_t lidx = lvdata.sel->get_index(lidx_p);
	idx_t ridx = rvdata.sel->get_index(ridx_p);

	// DISTINCT semantics are in effect for nested types
	auto lnull = !lvdata.validity.RowIsValid(lidx);
	auto rnull = !rvdata.validity.RowIsValid(ridx);
	if (lnull || rnull) {
		return OP::Operation(0, 0, lnull, rnull);
	}

	auto &lchildren = StructVector::GetEntries(left);
	auto &rchildren = StructVector::GetEntries(right);

	D_ASSERT(lchildren.size() == rchildren.size());
	for (idx_t col_no = 0; col_no < lchildren.size(); ++col_no) {
		auto &lchild = *lchildren[col_no];
		auto &rchild = *rchildren[col_no];

		// Strict comparisons use the OP for definite
		if (TemplatedOptimumValue<OP>(lchild, lidx_p, lcount, rchild, ridx_p, rcount)) {
			return true;
		}

		if (col_no == lchildren.size() - 1) {
			break;
		}

		// Strict comparisons use IS NOT DISTINCT for possible
		if (!TemplatedOptimumValue<NotDistinctFrom>(lchild, lidx_p, lcount, rchild, ridx_p, rcount)) {
			return false;
		}
	}

	return false;
}

template <class OP>
static bool TemplatedOptimumList(Vector &left, idx_t lidx, idx_t lcount, Vector &right, idx_t ridx, idx_t rcount) {
	UnifiedVectorFormat lvdata, rvdata;
	left.ToUnifiedFormat(lcount, lvdata);
	right.ToUnifiedFormat(rcount, rvdata);

	// Update the indexes and vector sizes for recursion.
	lidx = lvdata.sel->get_index(lidx);
	ridx = rvdata.sel->get_index(ridx);

	lcount = ListVector::GetListSize(left);
	rcount = ListVector::GetListSize(right);

	// DISTINCT semantics are in effect for nested types
	auto lnull = !lvdata.validity.RowIsValid(lidx);
	auto rnull = !rvdata.validity.RowIsValid(ridx);
	if (lnull || rnull) {
		return OP::Operation(0, 0, lnull, rnull);
	}

	auto &lchild = ListVector::GetEntry(left);
	auto &rchild = ListVector::GetEntry(right);

	auto ldata = UnifiedVectorFormat::GetData<list_entry_t>(lvdata);
	auto rdata = UnifiedVectorFormat::GetData<list_entry_t>(rvdata);

	auto &lval = ldata[lidx];
	auto &rval = rdata[ridx];

	for (idx_t pos = 0;; ++pos) {
		// Tie-breaking uses the OP
		if (pos == lval.length || pos == rval.length) {
			return OP::Operation(lval.length, rval.length, false, false);
		}

		// Strict comparisons use the OP for definite
		lidx = lval.offset + pos;
		ridx = rval.offset + pos;
		if (TemplatedOptimumValue<OP>(lchild, lidx, lcount, rchild, ridx, rcount)) {
			return true;
		}

		// Strict comparisons use IS NOT DISTINCT for possible
		if (!TemplatedOptimumValue<NotDistinctFrom>(lchild, lidx, lcount, rchild, ridx, rcount)) {
			return false;
		}
	}

	return false;
}

struct VectorMinMaxState {
	Vector *value;
};

struct VectorMinMaxBase {
	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Initialize(STATE &state) {
		state.value = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.value) {
			delete state.value;
		}
		state.value = nullptr;
	}

	template <class STATE>
	static void Assign(STATE &state, Vector &input, const idx_t idx) {
		if (!state.value) {
			state.value = new Vector(input.GetType());
			state.value->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		sel_t selv = idx;
		SelectionVector sel(&selv);
		VectorOperations::Copy(input, *state.value, sel, 1, 0, 0);
	}

	template <class STATE>
	static void Execute(STATE &state, Vector &input, const idx_t idx, const idx_t count) {
		Assign(state, input, idx);
	}

	template <class STATE, class OP>
	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto states = (STATE **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			if (!idata.validity.RowIsValid(idx)) {
				continue;
			}
			const auto sidx = sdata.sel->get_index(i);
			auto &state = *states[sidx];
			if (!state.value) {
				Assign(state, input, i);
			} else {
				OP::template Execute(state, input, i, count);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.value) {
			return;
		} else if (!target.value) {
			Assign(target, *source.value, 0);
		} else {
			OP::template Execute(target, *source.value, 0, 1);
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.value) {
			finalize_data.ReturnNull();
		} else {
			VectorOperations::Copy(*state.value, finalize_data.result, 1, 0, finalize_data.result_idx);
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

struct MinOperationVector : public VectorMinMaxBase {
	template <class STATE>
	static void Execute(STATE &state, Vector &input, const idx_t idx, const idx_t count) {
		if (TemplatedOptimumValue<DistinctLessThan>(input, idx, count, *state.value, 0, 1)) {
			Assign(state, input, idx);
		}
	}
};

struct MaxOperationVector : public VectorMinMaxBase {
	template <class STATE>
	static void Execute(STATE &state, Vector &input, const idx_t idx, const idx_t count) {
		if (TemplatedOptimumValue<DistinctGreaterThan>(input, idx, count, *state.value, 0, 1)) {
			Assign(state, input, idx);
		}
	}
};

template <class OP>
unique_ptr<FunctionData> BindDecimalMinMax(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	auto name = function.name;
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		function = GetUnaryAggregate<OP>(LogicalType::SMALLINT);
		break;
	case PhysicalType::INT32:
		function = GetUnaryAggregate<OP>(LogicalType::INTEGER);
		break;
	case PhysicalType::INT64:
		function = GetUnaryAggregate<OP>(LogicalType::BIGINT);
		break;
	default:
		function = GetUnaryAggregate<OP>(LogicalType::HUGEINT);
		break;
	}
	function.name = std::move(name);
	function.arguments[0] = decimal_type;
	function.return_type = decimal_type;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return nullptr;
}

template <typename OP, typename STATE>
static AggregateFunction GetMinMaxFunction(const LogicalType &type) {
	return AggregateFunction(
	    {type}, type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    OP::template Update<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateVoidFinalize<STATE, OP>, nullptr, OP::Bind, AggregateFunction::StateDestroy<STATE, OP>);
}

template <class OP, class OP_STRING, class OP_VECTOR>
static AggregateFunction GetMinMaxOperator(const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		return AggregateFunction::UnaryAggregateDestructor<MinMaxState<string_t>, string_t, string_t, OP_STRING>(
		    type.id(), type.id());
	} else if (type.InternalType() == PhysicalType::LIST || type.InternalType() == PhysicalType::STRUCT) {
		return GetMinMaxFunction<OP_VECTOR, VectorMinMaxState>(type);
	} else {
		return GetUnaryAggregate<OP>(type);
	}
}

template <class OP, class OP_STRING, class OP_VECTOR>
unique_ptr<FunctionData> BindMinMax(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	auto input_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetMinMaxOperator<OP, OP_STRING, OP_VECTOR>(input_type);
	function.name = std::move(name);
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	if (function.bind) {
		return function.bind(context, function, arguments);
	} else {
		return nullptr;
	}
}

template <class OP, class OP_STRING, class OP_VECTOR>
static void AddMinMaxOperator(AggregateFunctionSet &set) {
	set.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindDecimalMinMax<OP>));
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, BindMinMax<OP, OP_STRING, OP_VECTOR>));
}

AggregateFunctionSet MinFun::GetFunctions() {
	AggregateFunctionSet min("min");
	AddMinMaxOperator<MinOperation, MinOperationString, MinOperationVector>(min);
	return min;
}

AggregateFunctionSet MaxFun::GetFunctions() {
	AggregateFunctionSet max("max");
	AddMinMaxOperator<MaxOperation, MaxOperationString, MaxOperationVector>(max);
	return max;
}

} // namespace duckdb






namespace duckdb {

struct ProductState {
	bool empty;
	double val;
};

struct ProductFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.val = 1;
		state.empty = true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.val *= source.val;
		target.empty = target.empty && source.empty;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.empty) {
			finalize_data.ReturnNull();
			return;
		}
		target = state.val;
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (state.empty) {
			state.empty = false;
		}
		state.val *= input;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction ProductFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<ProductState, double, double, ProductFunction>(
	    LogicalType(LogicalTypeId::DOUBLE), LogicalType::DOUBLE);
}

} // namespace duckdb






namespace duckdb {

struct SkewState {
	size_t n;
	double sum;
	double sum_sqr;
	double sum_cub;
};

struct SkewnessOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.n = 0;
		state.sum = state.sum_sqr = state.sum_cub = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		state.n++;
		state.sum += input;
		state.sum_sqr += pow(input, 2);
		state.sum_cub += pow(input, 3);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.n == 0) {
			return;
		}

		target.n += source.n;
		target.sum += source.sum;
		target.sum_sqr += source.sum_sqr;
		target.sum_cub += source.sum_cub;
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.n <= 2) {
			finalize_data.ReturnNull();
			return;
		}
		double n = state.n;
		double temp = 1 / n;
		auto p = std::pow(temp * (state.sum_sqr - state.sum * state.sum * temp), 3);
		if (p < 0) {
			p = 0; // Shouldn't be below 0 but floating points are weird
		}
		double div = std::sqrt(p);
		if (div == 0) {
			finalize_data.ReturnNull();
			return;
		}
		double temp1 = std::sqrt(n * (n - 1)) / (n - 2);
		target = temp1 * temp *
		         (state.sum_cub - 3 * state.sum_sqr * state.sum * temp + 2 * pow(state.sum, 3) * temp * temp) / div;
		if (!Value::DoubleIsFinite(target)) {
			throw OutOfRangeException("SKEW is out of range!");
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction SkewnessFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<SkewState, double, double, SkewnessOperation>(LogicalType::DOUBLE,
	                                                                                       LogicalType::DOUBLE);
}

} // namespace duckdb










namespace duckdb {

struct StringAggState {
	idx_t size;
	idx_t alloc_size;
	char *dataptr;
};

struct StringAggBindData : public FunctionData {
	explicit StringAggBindData(string sep_p) : sep(std::move(sep_p)) {
	}

	string sep;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StringAggBindData>(sep);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StringAggBindData>();
		return sep == other.sep;
	}
};

struct StringAggFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.dataptr = nullptr;
		state.alloc_size = 0;
		state.size = 0;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.dataptr) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddString(finalize_data.result, state.dataptr, state.size);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.dataptr) {
			delete[] state.dataptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void PerformOperation(StringAggState &state, const char *str, const char *sep, idx_t str_size,
	                                    idx_t sep_size) {
		if (!state.dataptr) {
			// first iteration: allocate space for the string and copy it into the state
			state.alloc_size = MaxValue<idx_t>(8, NextPowerOfTwo(str_size));
			state.dataptr = new char[state.alloc_size];
			state.size = str_size;
			memcpy(state.dataptr, str, str_size);
		} else {
			// subsequent iteration: first check if we have space to place the string and separator
			idx_t required_size = state.size + str_size + sep_size;
			if (required_size > state.alloc_size) {
				// no space! allocate extra space
				while (state.alloc_size < required_size) {
					state.alloc_size *= 2;
				}
				auto new_data = new char[state.alloc_size];
				memcpy(new_data, state.dataptr, state.size);
				delete[] state.dataptr;
				state.dataptr = new_data;
			}
			// copy the separator
			memcpy(state.dataptr + state.size, sep, sep_size);
			state.size += sep_size;
			// copy the string
			memcpy(state.dataptr + state.size, str, str_size);
			state.size += str_size;
		}
	}

	static inline void PerformOperation(StringAggState &state, string_t str, optional_ptr<FunctionData> data_p) {
		auto &data = data_p->Cast<StringAggBindData>();
		PerformOperation(state, str.GetData(), data.sep.c_str(), str.GetSize(), data.sep.size());
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		PerformOperation(state, input, unary_input.input.bind_data);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (!source.dataptr) {
			// source is not set: skip combining
			return;
		}
		PerformOperation(target, string_t(source.dataptr, source.size), aggr_input_data.bind_data);
	}
};

unique_ptr<FunctionData> StringAggBind(ClientContext &context, AggregateFunction &function,
                                       vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() == 1) {
		// single argument: default to comma
		return make_uniq<StringAggBindData>(",");
	}
	D_ASSERT(arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("Separator argument to StringAgg must be a constant");
	}
	auto separator_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	string separator_string = ",";
	if (separator_val.IsNull()) {
		arguments[0] = make_uniq<BoundConstantExpression>(Value(LogicalType::VARCHAR));
	} else {
		separator_string = separator_val.ToString();
	}
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<StringAggBindData>(std::move(separator_string));
}

static void StringAggSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const AggregateFunction &function) {
	auto bind_data = bind_data_p->Cast<StringAggBindData>();
	serializer.WriteProperty(100, "separator", bind_data.sep);
}

unique_ptr<FunctionData> StringAggDeserialize(Deserializer &deserializer, AggregateFunction &bound_function) {
	auto sep = deserializer.ReadProperty<string>(100, "separator");
	return make_uniq<StringAggBindData>(std::move(sep));
}

AggregateFunctionSet StringAggFun::GetFunctions() {
	AggregateFunctionSet string_agg;
	AggregateFunction string_agg_param(
	    {LogicalType::VARCHAR}, LogicalType::VARCHAR, AggregateFunction::StateSize<StringAggState>,
	    AggregateFunction::StateInitialize<StringAggState, StringAggFunction>,
	    AggregateFunction::UnaryScatterUpdate<StringAggState, string_t, StringAggFunction>,
	    AggregateFunction::StateCombine<StringAggState, StringAggFunction>,
	    AggregateFunction::StateFinalize<StringAggState, string_t, StringAggFunction>,
	    AggregateFunction::UnaryUpdate<StringAggState, string_t, StringAggFunction>, StringAggBind,
	    AggregateFunction::StateDestroy<StringAggState, StringAggFunction>);
	string_agg_param.serialize = StringAggSerialize;
	string_agg_param.deserialize = StringAggDeserialize;
	string_agg.AddFunction(string_agg_param);
	string_agg_param.arguments.emplace_back(LogicalType::VARCHAR);
	string_agg.AddFunction(string_agg_param);
	return string_agg;
}

} // namespace duckdb






namespace duckdb {

struct SumSetOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.Initialize();
	}
	template <class STATE>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.Combine(source);
	}
	template <class STATE>
	static void AddValues(STATE &state, idx_t count) {
		state.isset = true;
	}
};

struct IntegerSumOperation : public BaseSumOperation<SumSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = Hugeint::Convert(state.value);
		}
	}
};

struct SumToHugeintOperation : public BaseSumOperation<SumSetOperation, HugeintAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <class ADD_OPERATOR>
struct DoubleSumOperation : public BaseSumOperation<SumSetOperation, ADD_OPERATOR> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

using NumericSumOperation = DoubleSumOperation<RegularAdd>;
using KahanSumOperation = DoubleSumOperation<KahanAdd>;

struct HugeintSumOperation : public BaseSumOperation<SumSetOperation, RegularAdd> {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.isset) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

AggregateFunction GetSumAggregateNoOverflow(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT32: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int32_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::INTEGER, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	case PhysicalType::INT64: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int64_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::BIGINT, LogicalType::HUGEINT);
		function.name = "sum_no_overflow";
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	default:
		throw BinderException("Unsupported internal type for sum_no_overflow");
	}
}

unique_ptr<BaseStatistics> SumPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                             AggregateStatisticsInput &input) {
	if (input.node_stats && input.node_stats->has_max_cardinality) {
		auto &numeric_stats = input.child_stats[0];
		if (!NumericStats::HasMinMax(numeric_stats)) {
			return nullptr;
		}
		auto internal_type = numeric_stats.GetType().InternalType();
		hugeint_t max_negative;
		hugeint_t max_positive;
		switch (internal_type) {
		case PhysicalType::INT32:
			max_negative = NumericStats::Min(numeric_stats).GetValueUnsafe<int32_t>();
			max_positive = NumericStats::Max(numeric_stats).GetValueUnsafe<int32_t>();
			break;
		case PhysicalType::INT64:
			max_negative = NumericStats::Min(numeric_stats).GetValueUnsafe<int64_t>();
			max_positive = NumericStats::Max(numeric_stats).GetValueUnsafe<int64_t>();
			break;
		default:
			throw InternalException("Unsupported type for propagate sum stats");
		}
		auto max_sum_negative = max_negative * hugeint_t(input.node_stats->max_cardinality);
		auto max_sum_positive = max_positive * hugeint_t(input.node_stats->max_cardinality);
		if (max_sum_positive >= NumericLimits<int64_t>::Maximum() ||
		    max_sum_negative <= NumericLimits<int64_t>::Minimum()) {
			// sum can potentially exceed int64_t bounds: use hugeint sum
			return nullptr;
		}
		// total sum is guaranteed to fit in a single int64: use int64 sum instead of hugeint sum
		expr.function = GetSumAggregateNoOverflow(internal_type);
	}
	return nullptr;
}

AggregateFunction GetSumAggregate(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16: {
		auto function = AggregateFunction::UnaryAggregate<SumState<int64_t>, int16_t, hugeint_t, IntegerSumOperation>(
		    LogicalType::SMALLINT, LogicalType::HUGEINT);
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}

	case PhysicalType::INT32: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int32_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::INTEGER, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	case PhysicalType::INT64: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, int64_t, hugeint_t, SumToHugeintOperation>(
		        LogicalType::BIGINT, LogicalType::HUGEINT);
		function.statistics = SumPropagateStats;
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	case PhysicalType::INT128: {
		auto function =
		    AggregateFunction::UnaryAggregate<SumState<hugeint_t>, hugeint_t, hugeint_t, HugeintSumOperation>(
		        LogicalType::HUGEINT, LogicalType::HUGEINT);
		function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
		return function;
	}
	default:
		throw InternalException("Unimplemented sum aggregate");
	}
}

unique_ptr<FunctionData> BindDecimalSum(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetSumAggregate(decimal_type.InternalType());
	function.name = "sum";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return nullptr;
}

unique_ptr<FunctionData> BindDecimalSumNoOverflow(ClientContext &context, AggregateFunction &function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetSumAggregateNoOverflow(decimal_type.InternalType());
	function.name = "sum_no_overflow";
	function.arguments[0] = decimal_type;
	function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return nullptr;
}

AggregateFunctionSet SumFun::GetFunctions() {
	AggregateFunctionSet sum;
	// decimal
	sum.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
	                                  BindDecimalSum));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT16));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT32));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT64));
	sum.AddFunction(GetSumAggregate(PhysicalType::INT128));
	sum.AddFunction(AggregateFunction::UnaryAggregate<SumState<double>, double, double, NumericSumOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	return sum;
}

AggregateFunctionSet SumNoOverflowFun::GetFunctions() {
	AggregateFunctionSet sum_no_overflow;
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT32));
	sum_no_overflow.AddFunction(GetSumAggregateNoOverflow(PhysicalType::INT64));
	sum_no_overflow.AddFunction(
	    AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalSumNoOverflow));
	return sum_no_overflow;
}

AggregateFunction KahanSumFun::GetFunction() {
	return AggregateFunction::UnaryAggregate<KahanSumState, double, double, KahanSumOperation>(LogicalType::DOUBLE,
	                                                                                           LogicalType::DOUBLE);
}

} // namespace duckdb








#include <algorithm>
#include <cmath>
#include <stdlib.h>

namespace duckdb {

struct ApproxQuantileState {
	duckdb_tdigest::TDigest *h;
	idx_t pos;
};

struct ApproximateQuantileBindData : public FunctionData {
	ApproximateQuantileBindData() {
	}
	explicit ApproximateQuantileBindData(float quantile_p) : quantiles(1, quantile_p) {
	}

	explicit ApproximateQuantileBindData(vector<float> quantiles_p) : quantiles(std::move(quantiles_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ApproximateQuantileBindData>(quantiles);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ApproximateQuantileBindData>();
		//		return quantiles == other.quantiles;
		if (quantiles != other.quantiles) {
			return false;
		}
		return true;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<ApproximateQuantileBindData>();
		serializer.WriteProperty(100, "quantiles", bind_data.quantiles);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<ApproximateQuantileBindData>();
		deserializer.ReadProperty(100, "quantiles", result->quantiles);
		return std::move(result);
	}

	vector<float> quantiles;
};

struct ApproxQuantileOperation {
	using SAVE_TYPE = duckdb_tdigest::Value;

	template <class STATE>
	static void Initialize(STATE &state) {
		state.pos = 0;
		state.h = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto val = Cast::template Operation<INPUT_TYPE, SAVE_TYPE>(input);
		if (!Value::DoubleIsFinite(val)) {
			return;
		}
		if (!state.h) {
			state.h = new duckdb_tdigest::TDigest(100);
		}
		state.h->add(val);
		state.pos++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.pos == 0) {
			return;
		}
		D_ASSERT(source.h);
		if (!target.h) {
			target.h = new duckdb_tdigest::TDigest(100);
		}
		target.h->merge(source.h);
		target.pos += source.pos;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.h) {
			delete state.h;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ApproxQuantileScalarOperation : public ApproxQuantileOperation {
	template <class TARGET_TYPE, class STATE>
	static void Finalize(STATE &state, TARGET_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(state.h);
		D_ASSERT(finalize_data.input.bind_data);
		state.h->compress();
		auto &bind_data = finalize_data.input.bind_data->template Cast<ApproximateQuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		target = Cast::template Operation<SAVE_TYPE, TARGET_TYPE>(state.h->quantile(bind_data.quantiles[0]));
	}
};

AggregateFunction GetApproximateQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int16_t, int16_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::SMALLINT,
		                                                                                  LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int32_t, int32_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::INTEGER,
		                                                                                  LogicalType::INTEGER);
	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int64_t, int64_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::BIGINT,
		                                                                                  LogicalType::BIGINT);
	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, hugeint_t, hugeint_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::HUGEINT,
		                                                                                  LogicalType::HUGEINT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, double, double,
		                                                   ApproxQuantileScalarOperation>(LogicalType::DOUBLE,
		                                                                                  LogicalType::DOUBLE);
	default:
		throw InternalException("Unimplemented quantile aggregate");
	}
}

static float CheckApproxQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("APPROXIMATE QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<float>();
	if (quantile < 0 || quantile > 1) {
		throw BinderException("APPROXIMATE QUANTILE can only take parameters in range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindApproxQuantile(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("APPROXIMATE QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);

	vector<float> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckApproxQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckApproxQuantile(element_val));
		}
	}

	// remove the quantile argument so we can use the unary aggregate
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<ApproximateQuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindApproxQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproximateQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "approx_quantile";
	function.serialize = ApproximateQuantileBindData::Serialize;
	function.deserialize = ApproximateQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetApproximateQuantileAggregate(PhysicalType type) {
	auto fun = GetApproximateQuantileAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::FLOAT);
	return fun;
}

template <class CHILD_TYPE>
struct ApproxQuantileListOperation : public ApproxQuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(STATE &state, RESULT_TYPE &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ApproximateQuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		D_ASSERT(state.h);
		state.h->compress();

		auto &entry = target;
		entry.offset = ridx;
		entry.length = bind_data.quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[ridx + q] = Cast::template Operation<SAVE_TYPE, CHILD_TYPE>(state.h->quantile(quantile));
		}

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction ApproxQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedApproxQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ApproxQuantileState;
	using OP = ApproxQuantileListOperation<INPUT_TYPE>;
	auto fun = ApproxQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	return fun;
}

AggregateFunction GetApproxQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedApproxQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedApproxQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedApproxQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented approximate quantile list aggregate");
		}
	default:
		// TODO: Add quantitative temporal types
		throw NotImplementedException("Unimplemented approximate quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindApproxQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproxQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "approx_quantile";
	function.serialize = ApproximateQuantileBindData::Serialize;
	function.deserialize = ApproximateQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetApproxQuantileListAggregate(const LogicalType &type) {
	auto fun = GetApproxQuantileListAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	fun.serialize = ApproximateQuantileBindData::Serialize;
	fun.deserialize = ApproximateQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_float = LogicalType::LIST(LogicalType::FLOAT);
	fun.arguments.push_back(list_of_float);
	return fun;
}

AggregateFunctionSet ApproxQuantileFun::GetFunctions() {
	AggregateFunctionSet approx_quantile;
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::FLOAT}, LogicalTypeId::DECIMAL,
	                                              nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                              BindApproxQuantileDecimal));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT16));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT32));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT64));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT128));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::DOUBLE));

	// List variants
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::FLOAT)},
	                                              LogicalType::LIST(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr,
	                                              nullptr, nullptr, nullptr, BindApproxQuantileDecimalList));

	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::TINYINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::SMALLINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::INTEGER));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::BIGINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::HUGEINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::FLOAT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::DOUBLE));
	return approx_quantile;
}

} // namespace duckdb
// MODE( <expr1> )
// Returns the most frequent value for the values within expr1.
// NULL values are ignored. If all the values are NULL, or there are 0 rows, then the function returns NULL.








#include <functional>

namespace std {

template <>
struct hash<duckdb::interval_t> {
	inline size_t operator()(const duckdb::interval_t &val) const {
		return hash<int32_t> {}(val.days) ^ hash<int32_t> {}(val.months) ^ hash<int64_t> {}(val.micros);
	}
};

template <>
struct hash<duckdb::hugeint_t> {
	inline size_t operator()(const duckdb::hugeint_t &val) const {
		return hash<int64_t> {}(val.upper) ^ hash<int64_t> {}(val.lower);
	}
};

} // namespace std

namespace duckdb {

template <class KEY_TYPE>
struct ModeState {
	struct ModeAttr {
		ModeAttr() : count(0), first_row(std::numeric_limits<idx_t>::max()) {
		}
		size_t count;
		idx_t first_row;
	};
	using Counts = unordered_map<KEY_TYPE, ModeAttr>;

	Counts *frequency_map;
	KEY_TYPE *mode;
	size_t nonzero;
	bool valid;
	size_t count;

	void Initialize() {
		frequency_map = nullptr;
		mode = nullptr;
		nonzero = 0;
		valid = false;
		count = 0;
	}

	void Destroy() {
		if (frequency_map) {
			delete frequency_map;
		}
		if (mode) {
			delete mode;
		}
	}

	void Reset() {
		Counts empty;
		frequency_map->swap(empty);
		nonzero = 0;
		count = 0;
		valid = false;
	}

	void ModeAdd(const KEY_TYPE &key, idx_t row) {
		auto &attr = (*frequency_map)[key];
		auto new_count = (attr.count += 1);
		if (new_count == 1) {
			++nonzero;
			attr.first_row = row;
		} else {
			attr.first_row = MinValue(row, attr.first_row);
		}
		if (new_count > count) {
			valid = true;
			count = new_count;
			if (mode) {
				*mode = key;
			} else {
				mode = new KEY_TYPE(key);
			}
		}
	}

	void ModeRm(const KEY_TYPE &key, idx_t frame) {
		auto &attr = (*frequency_map)[key];
		auto old_count = attr.count;
		nonzero -= int(old_count == 1);

		attr.count -= 1;
		if (count == old_count && key == *mode) {
			valid = false;
		}
	}

	typename Counts::const_iterator Scan() const {
		//! Initialize control variables to first variable of the frequency map
		auto highest_frequency = frequency_map->begin();
		for (auto i = highest_frequency; i != frequency_map->end(); ++i) {
			// Tie break with the lowest insert position
			if (i->second.count > highest_frequency->second.count ||
			    (i->second.count == highest_frequency->second.count &&
			     i->second.first_row < highest_frequency->second.first_row)) {
				highest_frequency = i;
			}
		}
		return highest_frequency;
	}
};

struct ModeIncluded {
	inline explicit ModeIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p, idx_t bias_p)
	    : fmask(fmask_p), dmask(dmask_p), bias(bias_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx - bias);
	}
	const ValidityMask &fmask;
	const ValidityMask &dmask;
	const idx_t bias;
};

struct ModeAssignmentStandard {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Assign(Vector &result, INPUT_TYPE input) {
		return RESULT_TYPE(input);
	}
};

struct ModeAssignmentString {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Assign(Vector &result, INPUT_TYPE input) {
		return StringVector::AddString(result, input);
	}
};

template <typename KEY_TYPE, typename ASSIGN_OP>
struct ModeFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.Initialize();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.frequency_map) {
			state.frequency_map = new typename STATE::Counts();
		}
		auto key = KEY_TYPE(input);
		auto &i = (*state.frequency_map)[key];
		i.count++;
		i.first_row = MinValue<idx_t>(i.first_row, state.count);
		state.count++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.frequency_map) {
			return;
		}
		if (!target.frequency_map) {
			// Copy - don't destroy! Otherwise windowing will break.
			target.frequency_map = new typename STATE::Counts(*source.frequency_map);
			return;
		}
		for (auto &val : *source.frequency_map) {
			auto &i = (*target.frequency_map)[val.first];
			i.count += val.second.count;
			i.first_row = MinValue(i.first_row, val.second.first_row);
		}
		target.count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.frequency_map) {
			finalize_data.ReturnNull();
			return;
		}
		auto highest_frequency = state.Scan();
		if (highest_frequency != state.frequency_map->end()) {
			target = ASSIGN_OP::template Assign<T, T>(finalize_data.result, highest_frequency->first);
		} else {
			finalize_data.ReturnNull();
		}
	}
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &, idx_t count) {
		if (!state.frequency_map) {
			state.frequency_map = new typename STATE::Counts();
		}
		auto key = KEY_TYPE(input);
		auto &i = (*state.frequency_map)[key];
		i.count += count;
		i.first_row = MinValue<idx_t>(i.first_row, state.count);
		state.count += count;
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &, STATE &state, const FrameBounds &frame, const FrameBounds &prev,
	                   Vector &result, idx_t rid, idx_t bias) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		ModeIncluded included(fmask, dmask, bias);

		if (!state.frequency_map) {
			state.frequency_map = new typename STATE::Counts;
		}
		const double tau = .25;
		if (state.nonzero <= tau * state.frequency_map->size() || prev.end <= frame.start || frame.end <= prev.start) {
			state.Reset();
			// for f ∈ F do
			for (auto f = frame.start; f < frame.end; ++f) {
				if (included(f)) {
					state.ModeAdd(KEY_TYPE(data[f]), f);
				}
			}
		} else {
			// for f ∈ P \ F do
			for (auto p = prev.start; p < frame.start; ++p) {
				if (included(p)) {
					state.ModeRm(KEY_TYPE(data[p]), p);
				}
			}
			for (auto p = frame.end; p < prev.end; ++p) {
				if (included(p)) {
					state.ModeRm(KEY_TYPE(data[p]), p);
				}
			}

			// for f ∈ F \ P do
			for (auto f = frame.start; f < prev.start; ++f) {
				if (included(f)) {
					state.ModeAdd(KEY_TYPE(data[f]), f);
				}
			}
			for (auto f = prev.end; f < frame.end; ++f) {
				if (included(f)) {
					state.ModeAdd(KEY_TYPE(data[f]), f);
				}
			}
		}

		if (!state.valid) {
			// Rescan
			auto highest_frequency = state.Scan();
			if (highest_frequency != state.frequency_map->end()) {
				*(state.mode) = highest_frequency->first;
				state.count = highest_frequency->second.count;
				state.valid = (state.count > 0);
			}
		}

		if (state.valid) {
			rdata[rid] = ASSIGN_OP::template Assign<INPUT_TYPE, RESULT_TYPE>(result, *state.mode);
		} else {
			rmask.Set(rid, false);
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.Destroy();
	}
};

template <typename INPUT_TYPE, typename KEY_TYPE, typename ASSIGN_OP = ModeAssignmentStandard>
AggregateFunction GetTypedModeFunction(const LogicalType &type) {
	using STATE = ModeState<KEY_TYPE>;
	using OP = ModeFunction<KEY_TYPE, ASSIGN_OP>;
	auto func = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	func.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return func;
}

AggregateFunction GetModeAggregate(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return GetTypedModeFunction<int8_t, int8_t>(type);
	case PhysicalType::UINT8:
		return GetTypedModeFunction<uint8_t, uint8_t>(type);
	case PhysicalType::INT16:
		return GetTypedModeFunction<int16_t, int16_t>(type);
	case PhysicalType::UINT16:
		return GetTypedModeFunction<uint16_t, uint16_t>(type);
	case PhysicalType::INT32:
		return GetTypedModeFunction<int32_t, int32_t>(type);
	case PhysicalType::UINT32:
		return GetTypedModeFunction<uint32_t, uint32_t>(type);
	case PhysicalType::INT64:
		return GetTypedModeFunction<int64_t, int64_t>(type);
	case PhysicalType::UINT64:
		return GetTypedModeFunction<uint64_t, uint64_t>(type);
	case PhysicalType::INT128:
		return GetTypedModeFunction<hugeint_t, hugeint_t>(type);

	case PhysicalType::FLOAT:
		return GetTypedModeFunction<float, float>(type);
	case PhysicalType::DOUBLE:
		return GetTypedModeFunction<double, double>(type);

	case PhysicalType::INTERVAL:
		return GetTypedModeFunction<interval_t, interval_t>(type);

	case PhysicalType::VARCHAR:
		return GetTypedModeFunction<string_t, string, ModeAssignmentString>(type);

	default:
		throw NotImplementedException("Unimplemented mode aggregate");
	}
}

unique_ptr<FunctionData> BindModeDecimal(ClientContext &context, AggregateFunction &function,
                                         vector<unique_ptr<Expression>> &arguments) {
	function = GetModeAggregate(arguments[0]->return_type);
	function.name = "mode";
	return nullptr;
}

AggregateFunctionSet ModeFun::GetFunctions() {
	const vector<LogicalType> TEMPORAL = {LogicalType::DATE,         LogicalType::TIMESTAMP, LogicalType::TIME,
	                                      LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,   LogicalType::INTERVAL};

	AggregateFunctionSet mode;
	mode.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                   nullptr, nullptr, nullptr, BindModeDecimal));

	for (const auto &type : LogicalType::Numeric()) {
		if (type.id() != LogicalTypeId::DECIMAL) {
			mode.AddFunction(GetModeAggregate(type));
		}
	}

	for (const auto &type : TEMPORAL) {
		mode.AddFunction(GetModeAggregate(type));
	}

	mode.AddFunction(GetModeAggregate(LogicalType::VARCHAR));
	return mode;
}
} // namespace duckdb












#include <algorithm>
#include <stdlib.h>
#include <utility>

namespace duckdb {

// Hugeint arithmetic
static hugeint_t MultiplyByDouble(const hugeint_t &h, const double &d) {
	D_ASSERT(d >= 0 && d <= 1);
	return Hugeint::Convert(Hugeint::Cast<double>(h) * d);
}

// Interval arithmetic
static interval_t MultiplyByDouble(const interval_t &i, const double &d) { // NOLINT
	D_ASSERT(d >= 0 && d <= 1);
	return Interval::FromMicro(std::llround(Interval::GetMicro(i) * d));
}

inline interval_t operator+(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) + Interval::GetMicro(rhs));
}

inline interval_t operator-(const interval_t &lhs, const interval_t &rhs) {
	return Interval::FromMicro(Interval::GetMicro(lhs) - Interval::GetMicro(rhs));
}

template <typename SAVE_TYPE>
struct QuantileState {
	using SaveType = SAVE_TYPE;

	// Regular aggregation
	vector<SaveType> v;

	// Windowed Quantile indirection
	vector<idx_t> w;
	idx_t pos;

	// Windowed MAD indirection
	vector<idx_t> m;

	QuantileState() : pos(0) {
	}

	~QuantileState() {
	}

	inline void SetPos(size_t pos_p) {
		pos = pos_p;
		if (pos >= w.size()) {
			w.resize(pos);
		}
	}
};

struct QuantileIncluded {
	inline explicit QuantileIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p, idx_t bias_p)
	    : fmask(fmask_p), dmask(dmask_p), bias(bias_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx - bias);
	}

	inline bool AllValid() const {
		return fmask.AllValid() && dmask.AllValid();
	}

	const ValidityMask &fmask;
	const ValidityMask &dmask;
	const idx_t bias;
};

void ReuseIndexes(idx_t *index, const FrameBounds &frame, const FrameBounds &prev) {
	idx_t j = 0;

	//  Copy overlapping indices
	for (idx_t p = 0; p < (prev.end - prev.start); ++p) {
		auto idx = index[p];

		//  Shift down into any hole
		if (j != p) {
			index[j] = idx;
		}

		//  Skip overlapping values
		if (frame.start <= idx && idx < frame.end) {
			++j;
		}
	}

	//  Insert new indices
	if (j > 0) {
		// Overlap: append the new ends
		for (auto f = frame.start; f < prev.start; ++f, ++j) {
			index[j] = f;
		}
		for (auto f = prev.end; f < frame.end; ++f, ++j) {
			index[j] = f;
		}
	} else {
		//  No overlap: overwrite with new values
		for (auto f = frame.start; f < frame.end; ++f, ++j) {
			index[j] = f;
		}
	}
}

static idx_t ReplaceIndex(idx_t *index, const FrameBounds &frame, const FrameBounds &prev) { // NOLINT
	D_ASSERT(index);

	idx_t j = 0;
	for (idx_t p = 0; p < (prev.end - prev.start); ++p) {
		auto idx = index[p];
		if (j != p) {
			break;
		}

		if (frame.start <= idx && idx < frame.end) {
			++j;
		}
	}
	index[j] = frame.end - 1;

	return j;
}

template <class INPUT_TYPE>
static inline int CanReplace(const idx_t *index, const INPUT_TYPE *fdata, const idx_t j, const idx_t k0, const idx_t k1,
                             const QuantileIncluded &validity) {
	D_ASSERT(index);

	// NULLs sort to the end, so if we have inserted a NULL,
	// it must be past the end of the quantile to be replaceable.
	// Note that the quantile values are never NULL.
	const auto ij = index[j];
	if (!validity(ij)) {
		return k1 < j ? 1 : 0;
	}

	auto curr = fdata[ij];
	if (k1 < j) {
		auto hi = fdata[index[k0]];
		return hi < curr ? 1 : 0;
	} else if (j < k0) {
		auto lo = fdata[index[k1]];
		return curr < lo ? -1 : 0;
	}

	return 0;
}

template <class INPUT_TYPE>
struct IndirectLess {
	inline explicit IndirectLess(const INPUT_TYPE *inputs_p) : inputs(inputs_p) {
	}

	inline bool operator()(const idx_t &lhi, const idx_t &rhi) const {
		return inputs[lhi] < inputs[rhi];
	}

	const INPUT_TYPE *inputs;
};

struct CastInterpolation {

	template <class INPUT_TYPE, class TARGET_TYPE>
	static inline TARGET_TYPE Cast(const INPUT_TYPE &src, Vector &result) {
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(src);
	}
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Interpolate(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
		const auto delta = hi - lo;
		return lo + delta * d;
	}
};

template <>
interval_t CastInterpolation::Cast(const dtime_t &src, Vector &result) {
	return {0, 0, src.micros};
}

template <>
double CastInterpolation::Interpolate(const double &lo, const double d, const double &hi) {
	return lo * (1.0 - d) + hi * d;
}

template <>
dtime_t CastInterpolation::Interpolate(const dtime_t &lo, const double d, const dtime_t &hi) {
	return dtime_t(std::llround(lo.micros * (1.0 - d) + hi.micros * d));
}

template <>
timestamp_t CastInterpolation::Interpolate(const timestamp_t &lo, const double d, const timestamp_t &hi) {
	return timestamp_t(std::llround(lo.value * (1.0 - d) + hi.value * d));
}

template <>
hugeint_t CastInterpolation::Interpolate(const hugeint_t &lo, const double d, const hugeint_t &hi) {
	const hugeint_t delta = hi - lo;
	return lo + MultiplyByDouble(delta, d);
}

template <>
interval_t CastInterpolation::Interpolate(const interval_t &lo, const double d, const interval_t &hi) {
	const interval_t delta = hi - lo;
	return lo + MultiplyByDouble(delta, d);
}

template <>
string_t CastInterpolation::Cast(const std::string &src, Vector &result) {
	return StringVector::AddString(result, src);
}

template <>
string_t CastInterpolation::Cast(const string_t &src, Vector &result) {
	return StringVector::AddString(result, src);
}

// Direct access
template <typename T>
struct QuantileDirect {
	using INPUT_TYPE = T;
	using RESULT_TYPE = T;

	inline const INPUT_TYPE &operator()(const INPUT_TYPE &x) const {
		return x;
	}
};

// Indirect access
template <typename T>
struct QuantileIndirect {
	using INPUT_TYPE = idx_t;
	using RESULT_TYPE = T;
	const RESULT_TYPE *data;

	explicit QuantileIndirect(const RESULT_TYPE *data_p) : data(data_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return data[input];
	}
};

// Composed access
template <typename OUTER, typename INNER>
struct QuantileComposed {
	using INPUT_TYPE = typename INNER::INPUT_TYPE;
	using RESULT_TYPE = typename OUTER::RESULT_TYPE;

	const OUTER &outer;
	const INNER &inner;

	explicit QuantileComposed(const OUTER &outer_p, const INNER &inner_p) : outer(outer_p), inner(inner_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return outer(inner(input));
	}
};

// Accessed comparison
template <typename ACCESSOR>
struct QuantileCompare {
	using INPUT_TYPE = typename ACCESSOR::INPUT_TYPE;
	const ACCESSOR &accessor;
	const bool desc;
	explicit QuantileCompare(const ACCESSOR &accessor_p, bool desc_p) : accessor(accessor_p), desc(desc_p) {
	}

	inline bool operator()(const INPUT_TYPE &lhs, const INPUT_TYPE &rhs) const {
		const auto lval = accessor(lhs);
		const auto rval = accessor(rhs);

		return desc ? (rval < lval) : (lval < rval);
	}
};

//	Avoid using naked Values in inner loops...
struct QuantileValue {
	explicit QuantileValue(const Value &v) : val(v), dbl(v.GetValue<double>()) {
		const auto &type = val.type();
		switch (type.id()) {
		case LogicalTypeId::DECIMAL: {
			integral = IntegralValue::Get(v);
			scaling = Hugeint::POWERS_OF_TEN[DecimalType::GetScale(type)];
			break;
		}
		default:
			break;
		}
	}

	Value val;

	//	DOUBLE
	double dbl;

	//	DECIMAL
	hugeint_t integral;
	hugeint_t scaling;
};

bool operator==(const QuantileValue &x, const QuantileValue &y) {
	return x.val == y.val;
}

// Continuous interpolation
template <bool DISCRETE>
struct Interpolator {
	Interpolator(const QuantileValue &q, const idx_t n_p, const bool desc_p)
	    : desc(desc_p), RN((double)(n_p - 1) * q.dbl), FRN(floor(RN)), CRN(ceil(RN)), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		if (CRN == FRN) {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
		} else {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			std::nth_element(v_t + FRN, v_t + CRN, v_t + end, comp);
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[CRN]), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Replace(const INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		if (CRN == FRN) {
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
		} else {
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[CRN]), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	const bool desc;
	const double RN;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

// Discrete "interpolation"
template <>
struct Interpolator<true> {
	static inline idx_t Index(const QuantileValue &q, const idx_t n) {
		idx_t floored;
		switch (q.val.type().id()) {
		case LogicalTypeId::DECIMAL: {
			//	Integer arithmetic for accuracy
			const auto integral = q.integral;
			const auto scaling = q.scaling;
			const auto scaled_q = DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(n, integral);
			const auto scaled_n = DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(n, scaling);
			floored = Cast::Operation<hugeint_t, idx_t>((scaled_n - scaled_q) / scaling);
			break;
		}
		default:
			const auto scaled_q = (double)(n * q.dbl);
			floored = floor(n - scaled_q);
			break;
		}

		return MaxValue<idx_t>(1, n - floored) - 1;
	}

	Interpolator(const QuantileValue &q, const idx_t n_p, bool desc_p)
	    : desc(desc_p), FRN(Index(q, n_p)), CRN(FRN), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Replace(const INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
	}

	const bool desc;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

template <typename T>
static inline T QuantileAbs(const T &t) {
	return AbsOperator::Operation<T, T>(t);
}

template <>
inline Value QuantileAbs(const Value &v) {
	const auto &type = v.type();
	switch (type.id()) {
	case LogicalTypeId::DECIMAL: {
		const auto integral = IntegralValue::Get(v);
		const auto width = DecimalType::GetWidth(type);
		const auto scale = DecimalType::GetScale(type);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return Value::DECIMAL(QuantileAbs<int16_t>(Cast::Operation<hugeint_t, int16_t>(integral)), width, scale);
		case PhysicalType::INT32:
			return Value::DECIMAL(QuantileAbs<int32_t>(Cast::Operation<hugeint_t, int32_t>(integral)), width, scale);
		case PhysicalType::INT64:
			return Value::DECIMAL(QuantileAbs<int64_t>(Cast::Operation<hugeint_t, int64_t>(integral)), width, scale);
		case PhysicalType::INT128:
			return Value::DECIMAL(QuantileAbs<hugeint_t>(integral), width, scale);
		default:
			throw InternalException("Unknown DECIMAL type");
		}
	}
	default:
		return Value::DOUBLE(QuantileAbs<double>(v.GetValue<double>()));
	}
}

struct QuantileBindData : public FunctionData {
	QuantileBindData() {
	}

	explicit QuantileBindData(const Value &quantile_p)
	    : quantiles(1, QuantileValue(QuantileAbs(quantile_p))), order(1, 0), desc(quantile_p < 0) {
	}

	explicit QuantileBindData(const vector<Value> &quantiles_p) {
		vector<Value> normalised;
		size_t pos = 0;
		size_t neg = 0;
		for (idx_t i = 0; i < quantiles_p.size(); ++i) {
			const auto &q = quantiles_p[i];
			pos += (q > 0);
			neg += (q < 0);
			normalised.emplace_back(QuantileAbs(q));
			order.push_back(i);
		}
		if (pos && neg) {
			throw BinderException("QUANTILE parameters must have consistent signs");
		}
		desc = (neg > 0);

		IndirectLess<Value> lt(normalised.data());
		std::sort(order.begin(), order.end(), lt);

		for (const auto &q : normalised) {
			quantiles.emplace_back(QuantileValue(q));
		}
	}

	QuantileBindData(const QuantileBindData &other) : order(other.order), desc(other.desc) {
		for (const auto &q : other.quantiles) {
			quantiles.emplace_back(q);
		}
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuantileBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuantileBindData>();
		return desc == other.desc && quantiles == other.quantiles && order == other.order;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<QuantileBindData>();
		vector<Value> raw;
		for (const auto &q : bind_data.quantiles) {
			raw.emplace_back(q.val);
		}
		serializer.WriteProperty(100, "quantiles", raw);
		serializer.WriteProperty(101, "order", bind_data.order);
		serializer.WriteProperty(102, "desc", bind_data.desc);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<QuantileBindData>();
		vector<Value> raw;
		deserializer.ReadProperty(100, "quantiles", raw);
		deserializer.ReadProperty(101, "order", result->order);
		deserializer.ReadProperty(102, "desc", result->desc);
		for (const auto &r : raw) {
			result->quantiles.emplace_back(QuantileValue(r));
		}
		return std::move(result);
	}

	static void SerializeDecimal(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                             const AggregateFunction &function) {
		throw NotImplementedException("FIXME: serializing quantiles with decimals is not supported right now");
	}

	vector<QuantileValue> quantiles;
	vector<idx_t> order;
	bool desc;
};

struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		state.v.emplace_back(input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.v.empty()) {
			return;
		}
		target.v.insert(target.v.end(), source.v.begin(), source.v.end());
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction QuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) { // NOLINT
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <bool DISCRETE>
struct QuantileScalarOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		Interpolator<DISCRETE> interp(bind_data.quantiles[0], state.v.size(), bind_data.desc);
		target = interp.template Operation<typename STATE::SaveType, T>(state.v.data(), finalize_data.result);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const FrameBounds &frame,
	                   const FrameBounds &prev, Vector &result, idx_t ridx, idx_t bias) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		QuantileIncluded included(fmask, dmask, bias);

		//  Lazily initialise frame state
		auto prev_pos = state.pos;
		state.SetPos(frame.end - frame.start);

		auto index = state.w.data();
		D_ASSERT(index);

		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		// Find the two positions needed
		const auto &q = bind_data.quantiles[0];

		bool replace = false;
		if (frame.start == prev.start + 1 && frame.end == prev.end + 1) {
			//  Fixed frame size
			const auto j = ReplaceIndex(index, frame, prev);
			//	We can only replace if the number of NULLs has not changed
			if (included.AllValid() || included(prev.start) == included(prev.end)) {
				Interpolator<DISCRETE> interp(q, prev_pos, false);
				replace = CanReplace(index, data, j, interp.FRN, interp.CRN, included);
				if (replace) {
					state.pos = prev_pos;
				}
			}
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (!replace && !included.AllValid()) {
			// Remove the NULLs
			state.pos = std::partition(index, index + state.pos, included) - index;
		}
		if (state.pos) {
			Interpolator<DISCRETE> interp(q, state.pos, false);

			using ID = QuantileIndirect<INPUT_TYPE>;
			ID indirect(data);
			rdata[ridx] = replace ? interp.template Replace<idx_t, RESULT_TYPE, ID>(index, result, indirect)
			                      : interp.template Operation<idx_t, RESULT_TYPE, ID>(index, result, indirect);
		} else {
			rmask.Set(ridx, false);
		}
	}
};

template <typename INPUT_TYPE, typename SAVED_TYPE>
AggregateFunction GetTypedDiscreteQuantileAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<SAVED_TYPE>;
	using OP = QuantileScalarOperation<true>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, INPUT_TYPE, OP>(type, type);
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, INPUT_TYPE, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedDiscreteQuantileAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileAggregateFunction<interval_t, interval_t>(type);

	case LogicalTypeId::VARCHAR:
		return GetTypedDiscreteQuantileAggregateFunction<string_t, std::string>(type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile aggregate");
	}
}

template <class CHILD_TYPE, bool DISCRETE>
struct QuantileListOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state.v.data();
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		idx_t lower = 0;
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<DISCRETE> interp(quantile, state.v.size(), bind_data.desc);
			interp.begin = lower;
			rdata[ridx + q] = interp.template Operation<typename STATE::SaveType, CHILD_TYPE>(v_t, result);
			lower = interp.FRN;
		}
		entry.length = bind_data.quantiles.size();

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const FrameBounds &frame,
	                   const FrameBounds &prev, Vector &list, idx_t lidx, idx_t bias) {
		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();

		QuantileIncluded included(fmask, dmask, bias);

		// Result is a constant LIST<RESULT_TYPE> with a fixed length
		auto ldata = FlatVector::GetData<RESULT_TYPE>(list);
		auto &lmask = FlatVector::Validity(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		//  Lazily initialise frame state
		auto prev_pos = state.pos;
		state.SetPos(frame.end - frame.start);

		auto index = state.w.data();

		// We can generalise replacement for quantile lists by observing that when a replacement is
		// valid for a single quantile, it is valid for all quantiles greater/less than that quantile
		// based on whether the insertion is below/above the quantile location.
		// So if a replaced index in an IQR is located between Q25 and Q50, but has a value below Q25,
		// then Q25 must be recomputed, but Q50 and Q75 are unaffected.
		// For a single element list, this reduces to the scalar case.
		std::pair<idx_t, idx_t> replaceable {state.pos, 0};
		if (frame.start == prev.start + 1 && frame.end == prev.end + 1) {
			//  Fixed frame size
			const auto j = ReplaceIndex(index, frame, prev);
			//	We can only replace if the number of NULLs has not changed
			if (included.AllValid() || included(prev.start) == included(prev.end)) {
				for (const auto &q : bind_data.order) {
					const auto &quantile = bind_data.quantiles[q];
					Interpolator<DISCRETE> interp(quantile, prev_pos, false);
					const auto replace = CanReplace(index, data, j, interp.FRN, interp.CRN, included);
					if (replace < 0) {
						//	Replacement is before this quantile, so the rest will be replaceable too.
						replaceable.first = MinValue(replaceable.first, interp.FRN);
						replaceable.second = prev_pos;
						break;
					} else if (replace > 0) {
						//	Replacement is after this quantile, so everything before it is replaceable too.
						replaceable.first = 0;
						replaceable.second = MaxValue(replaceable.second, interp.CRN);
					}
				}
				if (replaceable.first < replaceable.second) {
					state.pos = prev_pos;
				}
			}
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (replaceable.first >= replaceable.second && !included.AllValid()) {
			// Remove the NULLs
			state.pos = std::partition(index, index + state.pos, included) - index;
		}

		if (state.pos) {
			using ID = QuantileIndirect<INPUT_TYPE>;
			ID indirect(data);
			for (const auto &q : bind_data.order) {
				const auto &quantile = bind_data.quantiles[q];
				Interpolator<DISCRETE> interp(quantile, state.pos, false);
				if (replaceable.first <= interp.FRN && interp.CRN <= replaceable.second) {
					rdata[lentry.offset + q] = interp.template Replace<idx_t, CHILD_TYPE, ID>(index, result, indirect);
				} else {
					// Make sure we don't disturb any replacements
					if (replaceable.first < replaceable.second) {
						if (interp.FRN < replaceable.first) {
							interp.end = replaceable.first;
						}
						if (replaceable.second < interp.CRN) {
							interp.begin = replaceable.second;
						}
					}
					rdata[lentry.offset + q] =
					    interp.template Operation<idx_t, CHILD_TYPE, ID>(index, result, indirect);
				}
			}
		} else {
			lmask.Set(lidx, false);
		}
	}
};

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = QuantileState<SAVE_TYPE>;
	using OP = QuantileListOperation<INPUT_TYPE, true>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedDiscreteQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedDiscreteQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedDiscreteQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedDiscreteQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedDiscreteQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedDiscreteQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile list aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedDiscreteQuantileListAggregateFunction<date_t, date_t>(type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedDiscreteQuantileListAggregateFunction<timestamp_t, timestamp_t>(type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedDiscreteQuantileListAggregateFunction<dtime_t, dtime_t>(type);
	case LogicalTypeId::INTERVAL:
		return GetTypedDiscreteQuantileListAggregateFunction<interval_t, interval_t>(type);
	case LogicalTypeId::VARCHAR:
		return GetTypedDiscreteQuantileListAggregateFunction<string_t, std::string>(type);
	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <typename INPUT_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedContinuousQuantileAggregateFunction(const LogicalType &input_type,
                                                              const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = QuantileScalarOperation<false>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented continuous quantile DECIMAL aggregate");
		}
	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedContinuousQuantileAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented continuous quantile aggregate");
	}
}

template <typename INPUT_TYPE, typename CHILD_TYPE>
AggregateFunction GetTypedContinuousQuantileListAggregateFunction(const LogicalType &input_type,
                                                                  const LogicalType &result_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = QuantileListOperation<CHILD_TYPE, false>;
	auto fun = QuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(input_type, result_type);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, list_entry_t, OP>;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedContinuousQuantileListAggregateFunction<int8_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::SMALLINT:
		return GetTypedContinuousQuantileListAggregateFunction<int16_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::INTEGER:
		return GetTypedContinuousQuantileListAggregateFunction<int32_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::BIGINT:
		return GetTypedContinuousQuantileListAggregateFunction<int64_t, double>(type, LogicalType::DOUBLE);
	case LogicalTypeId::HUGEINT:
		return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, double>(type, LogicalType::DOUBLE);

	case LogicalTypeId::FLOAT:
		return GetTypedContinuousQuantileListAggregateFunction<float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedContinuousQuantileListAggregateFunction<double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedContinuousQuantileListAggregateFunction<int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedContinuousQuantileListAggregateFunction<int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedContinuousQuantileListAggregateFunction<int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedContinuousQuantileListAggregateFunction<hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented discrete quantile DECIMAL list aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedContinuousQuantileListAggregateFunction<date_t, timestamp_t>(type, LogicalType::TIMESTAMP);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedContinuousQuantileListAggregateFunction<timestamp_t, timestamp_t>(type, type);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedContinuousQuantileListAggregateFunction<dtime_t, dtime_t>(type, type);

	default:
		throw NotImplementedException("Unimplemented discrete quantile list aggregate");
	}
}

template <typename T, typename R, typename MEDIAN_TYPE>
struct MadAccessor {
	using INPUT_TYPE = T;
	using RESULT_TYPE = R;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}

	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return TryAbsOperator::Operation<RESULT_TYPE, RESULT_TYPE>(delta);
	}
};

// hugeint_t - double => undefined
template <>
struct MadAccessor<hugeint_t, double, double> {
	using INPUT_TYPE = hugeint_t;
	using RESULT_TYPE = double;
	using MEDIAN_TYPE = double;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = Hugeint::Cast<double>(input) - median;
		return TryAbsOperator::Operation<double, double>(delta);
	}
};

// date_t - timestamp_t => interval_t
template <>
struct MadAccessor<date_t, interval_t, timestamp_t> {
	using INPUT_TYPE = date_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = timestamp_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto dt = Cast::Operation<date_t, timestamp_t>(input);
		const auto delta = dt - median;
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
	}
};

// timestamp_t - timestamp_t => int64_t
template <>
struct MadAccessor<timestamp_t, interval_t, timestamp_t> {
	using INPUT_TYPE = timestamp_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = timestamp_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
	}
};

// dtime_t - dtime_t => int64_t
template <>
struct MadAccessor<dtime_t, interval_t, dtime_t> {
	using INPUT_TYPE = dtime_t;
	using RESULT_TYPE = interval_t;
	using MEDIAN_TYPE = dtime_t;
	const MEDIAN_TYPE &median;
	explicit MadAccessor(const MEDIAN_TYPE &median_p) : median(median_p) {
	}
	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
		const auto delta = input - median;
		return Interval::FromMicro(TryAbsOperator::Operation<int64_t, int64_t>(delta));
	}
};

template <typename MEDIAN_TYPE>
struct MedianAbsoluteDeviationOperation : public QuantileOperation {

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.v.empty()) {
			finalize_data.ReturnNull();
			return;
		}
		using SAVE_TYPE = typename STATE::SaveType;
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		const auto &q = bind_data.quantiles[0];
		Interpolator<false> interp(q, state.v.size(), false);
		const auto med = interp.template Operation<SAVE_TYPE, MEDIAN_TYPE>(state.v.data(), finalize_data.result);

		MadAccessor<SAVE_TYPE, T, MEDIAN_TYPE> accessor(med);
		target = interp.template Operation<SAVE_TYPE, T>(state.v.data(), finalize_data.result, accessor);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE>
	static void Window(const INPUT_TYPE *data, const ValidityMask &fmask, const ValidityMask &dmask,
	                   AggregateInputData &aggr_input_data, STATE &state, const FrameBounds &frame,
	                   const FrameBounds &prev, Vector &result, idx_t ridx, idx_t bias) {
		auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
		auto &rmask = FlatVector::Validity(result);

		QuantileIncluded included(fmask, dmask, bias);

		//  Lazily initialise frame state
		auto prev_pos = state.pos;
		state.SetPos(frame.end - frame.start);

		auto index = state.w.data();
		D_ASSERT(index);

		// We need a second index for the second pass.
		if (state.pos > state.m.size()) {
			state.m.resize(state.pos);
		}

		auto index2 = state.m.data();
		D_ASSERT(index2);

		// The replacement trick does not work on the second index because if
		// the median has changed, the previous order is not correct.
		// It is probably close, however, and so reuse is helpful.
		ReuseIndexes(index2, frame, prev);
		std::partition(index2, index2 + state.pos, included);

		// Find the two positions needed for the median
		D_ASSERT(aggr_input_data.bind_data);
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();
		D_ASSERT(bind_data.quantiles.size() == 1);
		const auto &q = bind_data.quantiles[0];

		bool replace = false;
		if (frame.start == prev.start + 1 && frame.end == prev.end + 1) {
			//  Fixed frame size
			const auto j = ReplaceIndex(index, frame, prev);
			//	We can only replace if the number of NULLs has not changed
			if (included.AllValid() || included(prev.start) == included(prev.end)) {
				Interpolator<false> interp(q, prev_pos, false);
				replace = CanReplace(index, data, j, interp.FRN, interp.CRN, included);
				if (replace) {
					state.pos = prev_pos;
				}
			}
		} else {
			ReuseIndexes(index, frame, prev);
		}

		if (!replace && !included.AllValid()) {
			// Remove the NULLs
			state.pos = std::partition(index, index + state.pos, included) - index;
		}

		if (state.pos) {
			Interpolator<false> interp(q, state.pos, false);

			// Compute or replace median from the first index
			using ID = QuantileIndirect<INPUT_TYPE>;
			ID indirect(data);
			const auto med = replace ? interp.template Replace<idx_t, MEDIAN_TYPE, ID>(index, result, indirect)
			                         : interp.template Operation<idx_t, MEDIAN_TYPE, ID>(index, result, indirect);

			// Compute mad from the second index
			using MAD = MadAccessor<INPUT_TYPE, RESULT_TYPE, MEDIAN_TYPE>;
			MAD mad(med);

			using MadIndirect = QuantileComposed<MAD, ID>;
			MadIndirect mad_indirect(mad, indirect);
			rdata[ridx] = interp.template Operation<idx_t, RESULT_TYPE, MadIndirect>(index2, result, mad_indirect);
		} else {
			rmask.Set(ridx, false);
		}
	}
};

unique_ptr<FunctionData> BindMedian(ClientContext &context, AggregateFunction &function,
                                    vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<QuantileBindData>(Value::DECIMAL(int16_t(5), 2, 1));
}

template <typename INPUT_TYPE, typename MEDIAN_TYPE, typename TARGET_TYPE>
AggregateFunction GetTypedMedianAbsoluteDeviationAggregateFunction(const LogicalType &input_type,
                                                                   const LogicalType &target_type) {
	using STATE = QuantileState<INPUT_TYPE>;
	using OP = MedianAbsoluteDeviationOperation<MEDIAN_TYPE>;
	auto fun = AggregateFunction::UnaryAggregateDestructor<STATE, INPUT_TYPE, TARGET_TYPE, OP>(input_type, target_type);
	fun.bind = BindMedian;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = AggregateFunction::UnaryWindow<STATE, INPUT_TYPE, TARGET_TYPE, OP>;
	return fun;
}

AggregateFunction GetMedianAbsoluteDeviationAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::FLOAT:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<float, float, float>(type, type);
	case LogicalTypeId::DOUBLE:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<double, double, double>(type, type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int16_t, int16_t, int16_t>(type, type);
		case PhysicalType::INT32:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int32_t, int32_t, int32_t>(type, type);
		case PhysicalType::INT64:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<int64_t, int64_t, int64_t>(type, type);
		case PhysicalType::INT128:
			return GetTypedMedianAbsoluteDeviationAggregateFunction<hugeint_t, hugeint_t, hugeint_t>(type, type);
		default:
			throw NotImplementedException("Unimplemented Median Absolute Deviation DECIMAL aggregate");
		}
		break;

	case LogicalTypeId::DATE:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<date_t, timestamp_t, interval_t>(type,
		                                                                                         LogicalType::INTERVAL);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<timestamp_t, timestamp_t, interval_t>(
		    type, LogicalType::INTERVAL);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return GetTypedMedianAbsoluteDeviationAggregateFunction<dtime_t, dtime_t, interval_t>(type,
		                                                                                      LogicalType::INTERVAL);

	default:
		throw NotImplementedException("Unimplemented Median Absolute Deviation aggregate");
	}
}

unique_ptr<FunctionData> BindMedianDecimal(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindMedian(context, function, arguments);

	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "median";
	function.serialize = QuantileBindData::SerializeDecimal;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

unique_ptr<FunctionData> BindMedianAbsoluteDeviationDecimal(ClientContext &context, AggregateFunction &function,
                                                            vector<unique_ptr<Expression>> &arguments) {
	function = GetMedianAbsoluteDeviationAggregateFunction(arguments[0]->return_type);
	function.name = "mad";
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return BindMedian(context, function, arguments);
}

static const Value &CheckQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < -1 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in the range [-1, 1]");
	}
	if (Value::IsNan(quantile)) {
		throw BinderException("QUANTILE parameter cannot be NaN");
	}

	return quantile_val;
}

unique_ptr<FunctionData> BindQuantile(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("QUANTILE can only take constant parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	vector<Value> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckQuantile(element_val));
		}
	}

	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<QuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetDiscreteQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_disc";
	function.serialize = QuantileBindData::SerializeDecimal;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

unique_ptr<FunctionData> BindDiscreteQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetDiscreteQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_disc";
	function.serialize = QuantileBindData::SerializeDecimal;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetContinuousQuantileAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_cont";
	function.serialize = QuantileBindData::SerializeDecimal;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

unique_ptr<FunctionData> BindContinuousQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindQuantile(context, function, arguments);
	function = GetContinuousQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "quantile_cont";
	function.serialize = QuantileBindData::SerializeDecimal;
	function.deserialize = QuantileBindData::Deserialize;
	function.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return bind_data;
}

static bool CanInterpolate(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::VARCHAR:
		return false;
	default:
		return true;
	}
}

AggregateFunction GetMedianAggregate(const LogicalType &type) {
	auto fun = CanInterpolate(type) ? GetContinuousQuantileAggregateFunction(type)
	                                : GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindMedian;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	return fun;
}

AggregateFunction GetDiscreteQuantileAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetDiscreteQuantileListAggregate(const LogicalType &type) {
	auto fun = GetDiscreteQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetContinuousQuantileListAggregate(const LogicalType &type) {
	auto fun = GetContinuousQuantileListAggregateFunction(type);
	fun.bind = BindQuantile;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction GetQuantileDecimalAggregate(const vector<LogicalType> &arguments, const LogicalType &return_type,
                                              bind_aggregate_function_t bind) {
	AggregateFunction fun(arguments, return_type, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, bind);
	fun.bind = bind;
	fun.serialize = QuantileBindData::Serialize;
	fun.deserialize = QuantileBindData::Deserialize;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

vector<LogicalType> GetQuantileTypes() {
	return {LogicalType::TINYINT,   LogicalType::SMALLINT, LogicalType::INTEGER,      LogicalType::BIGINT,
	        LogicalType::HUGEINT,   LogicalType::FLOAT,    LogicalType::DOUBLE,       LogicalType::DATE,
	        LogicalType::TIMESTAMP, LogicalType::TIME,     LogicalType::TIMESTAMP_TZ, LogicalType::TIME_TZ,
	        LogicalType::INTERVAL,  LogicalType::VARCHAR};
}

AggregateFunctionSet MedianFun::GetFunctions() {
	AggregateFunctionSet median("median");
	median.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, BindMedianDecimal));
	for (const auto &type : GetQuantileTypes()) {
		median.AddFunction(GetMedianAggregate(type));
	}
	return median;
}

AggregateFunctionSet QuantileDiscFun::GetFunctions() {
	AggregateFunctionSet quantile_disc("quantile_disc");
	quantile_disc.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindDiscreteQuantileDecimal));
	quantile_disc.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindDiscreteQuantileDecimalList));
	for (const auto &type : GetQuantileTypes()) {
		quantile_disc.AddFunction(GetDiscreteQuantileAggregate(type));
		quantile_disc.AddFunction(GetDiscreteQuantileListAggregate(type));
	}
	return quantile_disc;
	// quantile
}

AggregateFunctionSet QuantileContFun::GetFunctions() {
	AggregateFunctionSet quantile_cont("quantile_cont");
	quantile_cont.AddFunction(GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                                      LogicalTypeId::DECIMAL, BindContinuousQuantileDecimal));
	quantile_cont.AddFunction(
	    GetQuantileDecimalAggregate({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                LogicalType::LIST(LogicalTypeId::DECIMAL), BindContinuousQuantileDecimalList));

	for (const auto &type : GetQuantileTypes()) {
		if (CanInterpolate(type)) {
			quantile_cont.AddFunction(GetContinuousQuantileAggregate(type));
			quantile_cont.AddFunction(GetContinuousQuantileListAggregate(type));
		}
	}
	return quantile_cont;
}

AggregateFunctionSet MadFun::GetFunctions() {
	AggregateFunctionSet mad("mad");
	mad.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindMedianAbsoluteDeviationDecimal));

	const vector<LogicalType> MAD_TYPES = {LogicalType::FLOAT,     LogicalType::DOUBLE, LogicalType::DATE,
	                                       LogicalType::TIMESTAMP, LogicalType::TIME,   LogicalType::TIMESTAMP_TZ,
	                                       LogicalType::TIME_TZ};
	for (const auto &type : MAD_TYPES) {
		mad.AddFunction(GetMedianAbsoluteDeviationAggregateFunction(type));
	}
	return mad;
}

} // namespace duckdb








#include <algorithm>
#include <stdlib.h>

namespace duckdb {

template <typename T>
struct ReservoirQuantileState {
	T *v;
	idx_t len;
	idx_t pos;
	BaseReservoirSampling *r_samp;

	void Resize(idx_t new_len) {
		if (new_len <= len) {
			return;
		}
		T *old_v = v;
		v = (T *)realloc(v, new_len * sizeof(T));
		if (!v) {
			free(old_v);
			throw InternalException("Memory allocation failure");
		}
		len = new_len;
	}

	void ReplaceElement(T &input) {
		v[r_samp->min_entry] = input;
		r_samp->ReplaceElement();
	}

	void FillReservoir(idx_t sample_size, T element) {
		if (pos < sample_size) {
			v[pos++] = element;
			r_samp->InitializeReservoir(pos, len);
		} else {
			D_ASSERT(r_samp->next_index >= r_samp->current_count);
			if (r_samp->next_index == r_samp->current_count) {
				ReplaceElement(element);
			}
		}
	}
};

struct ReservoirQuantileBindData : public FunctionData {
	ReservoirQuantileBindData() {
	}
	ReservoirQuantileBindData(double quantile_p, int32_t sample_size_p)
	    : quantiles(1, quantile_p), sample_size(sample_size_p) {
	}

	ReservoirQuantileBindData(vector<double> quantiles_p, int32_t sample_size_p)
	    : quantiles(std::move(quantiles_p)), sample_size(sample_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ReservoirQuantileBindData>(quantiles, sample_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReservoirQuantileBindData>();
		return quantiles == other.quantiles && sample_size == other.sample_size;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<ReservoirQuantileBindData>();
		serializer.WriteProperty(100, "quantiles", bind_data.quantiles);
		serializer.WriteProperty(101, "sample_size", bind_data.sample_size);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<ReservoirQuantileBindData>();
		deserializer.ReadProperty(100, "quantiles", result->quantiles);
		deserializer.ReadProperty(101, "sample_size", result->sample_size);
		return std::move(result);
	}

	vector<double> quantiles;
	int32_t sample_size;
};

struct ReservoirQuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.v = nullptr;
		state.len = 0;
		state.pos = 0;
		state.r_samp = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto &bind_data = unary_input.input.bind_data->template Cast<ReservoirQuantileBindData>();
		if (state.pos == 0) {
			state.Resize(bind_data.sample_size);
		}
		if (!state.r_samp) {
			state.r_samp = new BaseReservoirSampling();
		}
		D_ASSERT(state.v);
		state.FillReservoir(bind_data.sample_size, input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.pos == 0) {
			return;
		}
		if (target.pos == 0) {
			target.Resize(source.len);
		}
		if (!target.r_samp) {
			target.r_samp = new BaseReservoirSampling();
		}
		for (idx_t src_idx = 0; src_idx < source.pos; src_idx++) {
			target.FillReservoir(target.len, source.v[src_idx]);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.v) {
			free(state.v);
			state.v = nullptr;
		}
		if (state.r_samp) {
			delete state.r_samp;
			state.r_samp = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ReservoirQuantileScalarOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(state.v);
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();
		auto v_t = state.v;
		D_ASSERT(bind_data.quantiles.size() == 1);
		auto offset = (idx_t)((double)(state.pos - 1) * bind_data.quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state.pos);
		target = v_t[offset];
	}
};

AggregateFunction GetReservoirQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int8_t>, int8_t, int8_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::TINYINT,
		                                                                                     LogicalType::TINYINT);

	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int16_t>, int16_t, int16_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::SMALLINT,
		                                                                                     LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int32_t>, int32_t, int32_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::INTEGER,
		                                                                                     LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int64_t>, int64_t, int64_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::BIGINT,
		                                                                                     LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<hugeint_t>, hugeint_t, hugeint_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::HUGEINT,
		                                                                                     LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<float>, float, float,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::FLOAT,
		                                                                                     LogicalType::FLOAT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<double>, double, double,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::DOUBLE,
		                                                                                     LogicalType::DOUBLE);
	default:
		throw InternalException("Unimplemented reservoir quantile aggregate");
	}
}

template <class CHILD_TYPE>
struct ReservoirQuantileListOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state.v;
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		entry.length = bind_data.quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data.quantiles[q];
			auto offset = (idx_t)((double)(state.pos - 1) * quantile);
			std::nth_element(v_t, v_t + offset, v_t + state.pos);
			rdata[ridx + q] = v_t[offset];
		}

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction ReservoirQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedReservoirQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ReservoirQuantileState<SAVE_TYPE>;
	using OP = ReservoirQuantileListOperation<INPUT_TYPE>;
	auto fun = ReservoirQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	return fun;
}

AggregateFunction GetReservoirQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedReservoirQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedReservoirQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedReservoirQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
		}
	default:
		// TODO: Add quantitative temporal types
		throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
	}
}

static double CheckReservoirQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("RESERVOIR_QUANTILE QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < 0 || quantile > 1) {
		throw BinderException("RESERVOIR_QUANTILE can only take parameters in the range [0, 1]");
	}
	return quantile;
}

unique_ptr<FunctionData> BindReservoirQuantile(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	vector<double> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckReservoirQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckReservoirQuantile(element_val));
		}
	}

	if (arguments.size() == 2) {
		if (function.arguments.size() == 2) {
			Function::EraseArgument(function, arguments, arguments.size() - 1);
		} else {
			arguments.pop_back();
		}
		return make_uniq<ReservoirQuantileBindData>(quantiles, 8192);
	}
	if (!arguments[2]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant sample size parameters");
	}
	Value sample_size_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	if (sample_size_val.IsNull()) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample cannot be NULL");
	}
	auto sample_size = sample_size_val.GetValue<int32_t>();

	if (sample_size_val.IsNull() || sample_size <= 0) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample must be bigger than 0");
	}

	// remove the quantile argument so we can use the unary aggregate
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	Function::EraseArgument(function, arguments, arguments.size() - 1);
	return make_uniq<ReservoirQuantileBindData>(quantiles, sample_size);
}

unique_ptr<FunctionData> BindReservoirQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	function = GetReservoirQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	auto bind_data = BindReservoirQuantile(context, function, arguments);
	function.name = "reservoir_quantile";
	function.serialize = ReservoirQuantileBindData::Serialize;
	function.deserialize = ReservoirQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetReservoirQuantileAggregate(PhysicalType type) {
	auto fun = GetReservoirQuantileAggregateFunction(type);
	fun.bind = BindReservoirQuantile;
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	return fun;
}

unique_ptr<FunctionData> BindReservoirQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                          vector<unique_ptr<Expression>> &arguments) {
	function = GetReservoirQuantileListAggregateFunction(arguments[0]->return_type);
	auto bind_data = BindReservoirQuantile(context, function, arguments);
	function.serialize = ReservoirQuantileBindData::Serialize;
	function.deserialize = ReservoirQuantileBindData::Deserialize;
	function.name = "reservoir_quantile";
	return bind_data;
}

AggregateFunction GetReservoirQuantileListAggregate(const LogicalType &type) {
	auto fun = GetReservoirQuantileListAggregateFunction(type);
	fun.bind = BindReservoirQuantile;
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	return fun;
}

static void DefineReservoirQuantile(AggregateFunctionSet &set, const LogicalType &type) {
	//	Four versions: type, scalar/list[, count]
	auto fun = GetReservoirQuantileAggregate(type.InternalType());
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);

	// List variants
	fun = GetReservoirQuantileListAggregate(type);
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);
}

static void GetReservoirQuantileDecimalFunction(AggregateFunctionSet &set, const vector<LogicalType> &arguments,
                                                const LogicalType &return_value) {
	AggregateFunction fun(arguments, return_value, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      BindReservoirQuantileDecimal);
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);
}

AggregateFunctionSet ReservoirQuantileFun::GetFunctions() {
	AggregateFunctionSet reservoir_quantile;

	// DECIMAL
	GetReservoirQuantileDecimalFunction(reservoir_quantile, {LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                    LogicalTypeId::DECIMAL);
	GetReservoirQuantileDecimalFunction(reservoir_quantile,
	                                    {LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                    LogicalType::LIST(LogicalTypeId::DECIMAL));

	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::TINYINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::SMALLINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::INTEGER);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::BIGINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::HUGEINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::FLOAT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::DOUBLE);
	return reservoir_quantile;
}

} // namespace duckdb







namespace duckdb {

struct HistogramFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(UnifiedVectorFormat &sdata, UnifiedVectorFormat &input_data, idx_t count) {
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto &state = *states[sdata.sel->get_index(i)];
				if (!state.hist) {
					state.hist = new MAP_TYPE();
				}
				auto value = UnifiedVectorFormat::GetData<T>(input_data);
				(*state.hist)[value[input_data.sel->get_index(i)]]++;
			}
		}
	}

	template <class T>
	static Value HistogramFinalize(T first) {
		return Value::CreateValue(first);
	}
};

struct HistogramStringFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(UnifiedVectorFormat &sdata, UnifiedVectorFormat &input_data, idx_t count) {
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto &state = *states[sdata.sel->get_index(i)];
				if (!state.hist) {
					state.hist = new MAP_TYPE();
				}
				(*state.hist)[input_strings[input_data.sel->get_index(i)].GetString()]++;
			}
		}
	}

	template <class T>
	static Value HistogramFinalize(T first) {
		string_t value = first;
		return Value::CreateValue(value);
	}
};

struct HistogramFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.hist = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.hist) {
			delete state.hist;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class OP, class T, class MAP_TYPE>
static void HistogramUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                                    idx_t count) {

	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	OP::template HistogramUpdate<T, MAP_TYPE>(sdata, input_data, count);
}

template <class T, class MAP_TYPE>
static void HistogramCombineFunction(Vector &state_vector, Vector &combined, AggregateInputData &, idx_t count) {

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states_ptr = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto combined_ptr = FlatVector::GetData<HistogramAggState<T, MAP_TYPE> *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states_ptr[sdata.sel->get_index(i)];
		if (!state.hist) {
			continue;
		}
		if (!combined_ptr[i]->hist) {
			combined_ptr[i]->hist = new MAP_TYPE();
		}
		D_ASSERT(combined_ptr[i]->hist);
		D_ASSERT(state.hist);
		for (auto &entry : *state.hist) {
			(*combined_ptr[i]->hist)[entry.first] += entry.second;
		}
	}
}

template <class OP, class T, class MAP_TYPE>
static void HistogramFinalizeFunction(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                      idx_t offset) {

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto &mask = FlatVector::Validity(result);
	auto old_len = ListVector::GetListSize(result);

	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.hist) {
			mask.SetInvalid(rid);
			continue;
		}

		for (auto &entry : *state.hist) {
			Value bucket_value = OP::template HistogramFinalize<T>(entry.first);
			auto count_value = Value::CreateValue(entry.second);
			auto struct_value =
			    Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
			ListVector::PushBack(result, struct_value);
		}

		auto list_struct_data = ListVector::GetData(result);
		list_struct_data[rid].length = ListVector::GetListSize(result) - old_len;
		list_struct_data[rid].offset = old_len;
		old_len += list_struct_data[rid].length;
	}
	result.Verify(count);
}

unique_ptr<FunctionData> HistogramBindFunction(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::LIST ||
	    arguments[0]->return_type.id() == LogicalTypeId::STRUCT ||
	    arguments[0]->return_type.id() == LogicalTypeId::MAP) {
		throw NotImplementedException("Unimplemented type for histogram %s", arguments[0]->return_type.ToString());
	}

	auto struct_type = LogicalType::MAP(arguments[0]->return_type, LogicalType::UBIGINT);

	function.return_type = struct_type;
	return make_uniq<VariableReturnBindData>(function.return_type);
}

template <class OP, class T, class MAP_TYPE = map<T, idx_t>>
static AggregateFunction GetHistogramFunction(const LogicalType &type) {

	using STATE_TYPE = HistogramAggState<T, MAP_TYPE>;

	return AggregateFunction("histogram", {type}, LogicalTypeId::MAP, AggregateFunction::StateSize<STATE_TYPE>,
	                         AggregateFunction::StateInitialize<STATE_TYPE, HistogramFunction>,
	                         HistogramUpdateFunction<OP, T, MAP_TYPE>, HistogramCombineFunction<T, MAP_TYPE>,
	                         HistogramFinalizeFunction<OP, T, MAP_TYPE>, nullptr, HistogramBindFunction,
	                         AggregateFunction::StateDestroy<STATE_TYPE, HistogramFunction>);
}

template <class OP, class T, bool IS_ORDERED>
AggregateFunction GetMapType(const LogicalType &type) {

	if (IS_ORDERED) {
		return GetHistogramFunction<OP, T>(type);
	}
	return GetHistogramFunction<OP, T, unordered_map<T, idx_t>>(type);
}

template <bool IS_ORDERED = true>
AggregateFunction GetHistogramFunction(const LogicalType &type) {

	switch (type.id()) {
	case LogicalType::BOOLEAN:
		return GetMapType<HistogramFunctor, bool, IS_ORDERED>(type);
	case LogicalType::UTINYINT:
		return GetMapType<HistogramFunctor, uint8_t, IS_ORDERED>(type);
	case LogicalType::USMALLINT:
		return GetMapType<HistogramFunctor, uint16_t, IS_ORDERED>(type);
	case LogicalType::UINTEGER:
		return GetMapType<HistogramFunctor, uint32_t, IS_ORDERED>(type);
	case LogicalType::UBIGINT:
		return GetMapType<HistogramFunctor, uint64_t, IS_ORDERED>(type);
	case LogicalType::TINYINT:
		return GetMapType<HistogramFunctor, int8_t, IS_ORDERED>(type);
	case LogicalType::SMALLINT:
		return GetMapType<HistogramFunctor, int16_t, IS_ORDERED>(type);
	case LogicalType::INTEGER:
		return GetMapType<HistogramFunctor, int32_t, IS_ORDERED>(type);
	case LogicalType::BIGINT:
		return GetMapType<HistogramFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::FLOAT:
		return GetMapType<HistogramFunctor, float, IS_ORDERED>(type);
	case LogicalType::DOUBLE:
		return GetMapType<HistogramFunctor, double, IS_ORDERED>(type);
	case LogicalType::VARCHAR:
		return GetMapType<HistogramStringFunctor, string, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP:
		return GetMapType<HistogramFunctor, timestamp_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_TZ:
		return GetMapType<HistogramFunctor, timestamp_tz_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_S:
		return GetMapType<HistogramFunctor, timestamp_sec_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_MS:
		return GetMapType<HistogramFunctor, timestamp_ms_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_NS:
		return GetMapType<HistogramFunctor, timestamp_ns_t, IS_ORDERED>(type);
	case LogicalType::TIME:
		return GetMapType<HistogramFunctor, dtime_t, IS_ORDERED>(type);
	case LogicalType::TIME_TZ:
		return GetMapType<HistogramFunctor, dtime_tz_t, IS_ORDERED>(type);
	case LogicalType::DATE:
		return GetMapType<HistogramFunctor, date_t, IS_ORDERED>(type);
	default:
		throw InternalException("Unimplemented histogram aggregate");
	}
}

AggregateFunctionSet HistogramFun::GetFunctions() {
	AggregateFunctionSet fun;
	fun.AddFunction(GetHistogramFunction<>(LogicalType::BOOLEAN));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UTINYINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::USMALLINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UINTEGER));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UBIGINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TINYINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::SMALLINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::INTEGER));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::BIGINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::FLOAT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::DOUBLE));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::VARCHAR));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_TZ));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_S));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_MS));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_NS));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIME));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIME_TZ));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::DATE));
	return fun;
}

AggregateFunction HistogramFun::GetHistogramUnorderedMap(LogicalType &type) {
	const auto &const_type = type;
	return GetHistogramFunction<false>(const_type);
}

} // namespace duckdb





namespace duckdb {

struct ListBindData : public FunctionData {
	explicit ListBindData(const LogicalType &stype_p);
	~ListBindData() override;

	LogicalType stype;
	ListSegmentFunctions functions;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ListBindData>(stype);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ListBindData>();
		return stype == other.stype;
	}
};

ListBindData::ListBindData(const LogicalType &stype_p) : stype(stype_p) {
	// always unnest once because the result vector is of type LIST
	auto type = ListType::GetChildType(stype_p);
	GetSegmentDataFunctions(functions, type);
}

ListBindData::~ListBindData() {
}

struct ListAggState {
	LinkedList linked_list;
};

struct ListFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.linked_list.total_capacity = 0;
		state.linked_list.first_segment = nullptr;
		state.linked_list.last_segment = nullptr;
	}
	static bool IgnoreNull() {
		return false;
	}
};

static void ListUpdateFunction(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                               Vector &state_vector, idx_t count) {

	D_ASSERT(input_count == 1);
	auto &input = inputs[0];
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, count, input_data);

	UnifiedVectorFormat states_data;
	state_vector.ToUnifiedFormat(count, states_data);
	auto states = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[states_data.sel->get_index(i)];
		list_bind_data.functions.AppendRow(aggr_input_data.allocator, state.linked_list, input_data, i);
	}
}

static void ListCombineFunction(Vector &states_vector, Vector &combined, AggregateInputData &, idx_t count) {

	UnifiedVectorFormat states_data;
	states_vector.ToUnifiedFormat(count, states_data);
	auto states_ptr = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	auto combined_ptr = FlatVector::GetData<ListAggState *>(combined);
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states_ptr[states_data.sel->get_index(i)];
		if (state.linked_list.total_capacity == 0) {
			// NULL, no need to append
			// this can happen when adding a FILTER to the grouping, e.g.,
			// LIST(i) FILTER (WHERE i <> 3)
			continue;
		}

		if (combined_ptr[i]->linked_list.total_capacity == 0) {
			combined_ptr[i]->linked_list = state.linked_list;
			continue;
		}

		// append the linked list
		combined_ptr[i]->linked_list.last_segment->next = state.linked_list.first_segment;
		combined_ptr[i]->linked_list.last_segment = state.linked_list.last_segment;
		combined_ptr[i]->linked_list.total_capacity += state.linked_list.total_capacity;
	}
}

static void ListFinalize(Vector &states_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                         idx_t offset) {

	UnifiedVectorFormat states_data;
	states_vector.ToUnifiedFormat(count, states_data);
	auto states = UnifiedVectorFormat::GetData<ListAggState *>(states_data);

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto &mask = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();

	// first iterate over all entries and set up the list entries, and get the newly required total length
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states[states_data.sel->get_index(i)];
		const auto rid = i + offset;
		result_data[rid].offset = total_len;
		if (state.linked_list.total_capacity == 0) {
			mask.SetInvalid(rid);
			result_data[rid].length = 0;
			continue;
		}

		// set the length and offset of this list in the result vector
		auto total_capacity = state.linked_list.total_capacity;
		result_data[rid].length = total_capacity;
		total_len += total_capacity;
	}

	// reserve capacity, then iterate over all entries again and copy over the data to the child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetEntry(result);
	for (idx_t i = 0; i < count; i++) {

		auto &state = *states[states_data.sel->get_index(i)];
		const auto rid = i + offset;
		if (state.linked_list.total_capacity == 0) {
			continue;
		}

		idx_t current_offset = result_data[rid].offset;
		list_bind_data.functions.BuildListVector(state.linked_list, result_child, current_offset);
	}

	ListVector::SetListSize(result, total_len);
}

static void ListWindow(Vector inputs[], const ValidityMask &filter_mask, AggregateInputData &aggr_input_data,
                       idx_t input_count, data_ptr_t state, const FrameBounds &frame, const FrameBounds &prev,
                       Vector &result, idx_t rid, idx_t bias) {

	auto &list_bind_data = aggr_input_data.bind_data->Cast<ListBindData>();
	LinkedList linked_list;

	// UPDATE step

	D_ASSERT(input_count == 1);
	auto &input = inputs[0];

	// FIXME: we unify more values than necessary (count is frame.end)
	RecursiveUnifiedVectorFormat input_data;
	Vector::RecursiveToUnifiedFormat(input, frame.end, input_data);

	for (idx_t i = frame.start; i < frame.end; i++) {
		list_bind_data.functions.AppendRow(aggr_input_data.allocator, linked_list, input_data, i);
	}

	// FINALIZE step

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	size_t total_len = ListVector::GetListSize(result);

	// set the length and offset of this list in the result vector
	result_data[rid].offset = total_len;
	result_data[rid].length = linked_list.total_capacity;
	D_ASSERT(linked_list.total_capacity != 0);
	total_len += linked_list.total_capacity;

	// reserve capacity, then copy over the data to the child vector
	ListVector::Reserve(result, total_len);
	auto &result_child = ListVector::GetEntry(result);
	idx_t offset = result_data[rid].offset;
	list_bind_data.functions.BuildListVector(linked_list, result_child, offset);

	ListVector::SetListSize(result, total_len);
}

unique_ptr<FunctionData> ListBindFunction(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 1);
	D_ASSERT(function.arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		function.arguments[0] = LogicalTypeId::UNKNOWN;
		function.return_type = LogicalType::SQLNULL;
		return nullptr;
	}

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return make_uniq<ListBindData>(function.return_type);
}

AggregateFunction ListFun::GetFunction() {
	return AggregateFunction({LogicalType::ANY}, LogicalTypeId::LIST, AggregateFunction::StateSize<ListAggState>,
	                         AggregateFunction::StateInitialize<ListAggState, ListFunction>, ListUpdateFunction,
	                         ListCombineFunction, ListFinalize, nullptr, ListBindFunction, nullptr, nullptr,
	                         ListWindow);
}

} // namespace duckdb






namespace duckdb {
struct RegrState {
	double sum;
	size_t count;
};

struct RegrAvgFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.sum = 0;
		state.count = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target.sum += source.sum;
		target.count += source.count;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
		} else {
			target = state.sum / (double)state.count;
		}
	}
	static bool IgnoreNull() {
		return true;
	}
};
struct RegrAvgXFunction : RegrAvgFunction {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		state.sum += x;
		state.count++;
	}
};

struct RegrAvgYFunction : RegrAvgFunction {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		state.sum += y;
		state.count++;
	}
};

AggregateFunction RegrAvgxFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrState, double, double, double, RegrAvgXFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

AggregateFunction RegrAvgyFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrState, double, double, double, RegrAvgYFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb







namespace duckdb {

AggregateFunction RegrCountFun::GetFunction() {
	auto regr_count = AggregateFunction::BinaryAggregate<size_t, double, double, uint32_t, RegrCountFunction>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::UINTEGER);
	regr_count.name = "regr_count";
	regr_count.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return regr_count;
}

} // namespace duckdb
//! AVG(y)-REGR_SLOPE(y,x)*AVG(x)





namespace duckdb {

struct RegrInterceptState {
	size_t count;
	double sum_x;
	double sum_y;
	RegrSlopeState slope;
};

struct RegrInterceptOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.count = 0;
		state.sum_x = 0;
		state.sum_y = 0;
		RegrSlopeOperation::Initialize<RegrSlopeState>(state.slope);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		state.count++;
		state.sum_x += x;
		state.sum_y += y;
		RegrSlopeOperation::Operation<A_TYPE, B_TYPE, RegrSlopeState, OP>(state.slope, y, x, idata);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		target.count += source.count;
		target.sum_x += source.sum_x;
		target.sum_y += source.sum_y;
		RegrSlopeOperation::Combine<RegrSlopeState, OP>(source.slope, target.slope, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.count == 0) {
			finalize_data.ReturnNull();
			return;
		}
		RegrSlopeOperation::Finalize<T, RegrSlopeState>(state.slope, target, finalize_data);
		auto x_avg = state.sum_x / state.count;
		auto y_avg = state.sum_y / state.count;
		target = y_avg - target * x_avg;
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction RegrInterceptFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrInterceptState, double, double, double, RegrInterceptOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
// REGR_R2(y, x)
// Returns the coefficient of determination for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// null                 if var_pop(x) = 0, else
// 1                    if var_pop(y) = 0 and var_pop(x) <> 0, else
// power(corr(y,x), 2)





namespace duckdb {
struct RegrR2State {
	CorrState corr;
	StddevState var_pop_x;
	StddevState var_pop_y;
};

struct RegrR2Operation {
	template <class STATE>
	static void Initialize(STATE &state) {
		CorrOperation::Initialize<CorrState>(state.corr);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop_x);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop_y);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		CorrOperation::Operation<A_TYPE, B_TYPE, CorrState, OP>(state.corr, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop_x, x);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop_y, y);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		CorrOperation::Combine<CorrState, OP>(source.corr, target.corr, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop_x, target.var_pop_x, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop_y, target.var_pop_y, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		auto var_pop_x = state.var_pop_x.count > 1 ? (state.var_pop_x.dsquared / state.var_pop_x.count) : 0;
		if (!Value::DoubleIsFinite(var_pop_x)) {
			throw OutOfRangeException("VARPOP(X) is out of range!");
		}
		if (var_pop_x == 0) {
			finalize_data.ReturnNull();
			return;
		}
		auto var_pop_y = state.var_pop_y.count > 1 ? (state.var_pop_y.dsquared / state.var_pop_y.count) : 0;
		if (!Value::DoubleIsFinite(var_pop_y)) {
			throw OutOfRangeException("VARPOP(Y) is out of range!");
		}
		if (var_pop_y == 0) {
			target = 1;
			return;
		}
		CorrOperation::Finalize<T, CorrState>(state.corr, target, finalize_data);
		target = pow(target, 2);
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction RegrR2Fun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrR2State, double, double, double, RegrR2Operation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}
} // namespace duckdb
// REGR_SLOPE(y, x)
// Returns the slope of the linear regression line for non-null pairs in a group.
// It is computed for non-null pairs using the following formula:
// COVAR_POP(x,y) / VAR_POP(x)

//! Input : Any numeric type
//! Output : Double





namespace duckdb {

AggregateFunction RegrSlopeFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSlopeState, double, double, double, RegrSlopeOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
// REGR_SXX(y, x)
// Returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs.
// REGR_SYY(y, x)
// Returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs.





namespace duckdb {

struct RegrSState {
	size_t count;
	StddevState var_pop;
};

struct RegrBaseOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		RegrCountFunction::Initialize<size_t>(state.count);
		STDDevBaseOperation::Initialize<StddevState>(state.var_pop);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		RegrCountFunction::Combine<size_t, OP>(source.count, target.count, aggr_input_data);
		STDDevBaseOperation::Combine<StddevState, OP>(source.var_pop, target.var_pop, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.var_pop.count == 0) {
			finalize_data.ReturnNull();
			return;
		}
		auto var_pop = state.var_pop.count > 1 ? (state.var_pop.dsquared / state.var_pop.count) : 0;
		if (!Value::DoubleIsFinite(var_pop)) {
			throw OutOfRangeException("VARPOP is out of range!");
		}
		RegrCountFunction::Finalize<T, size_t>(state.count, target, finalize_data);
		target *= var_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct RegrSXXOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop, x);
	}
};

struct RegrSYYOperation : RegrBaseOperation {
	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		STDDevBaseOperation::Execute<A_TYPE, StddevState>(state.var_pop, y);
	}
};

AggregateFunction RegrSXXFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSXXOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

AggregateFunction RegrSYYFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSState, double, double, double, RegrSYYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb
// REGR_SXY(y, x)
// Returns REGR_COUNT(expr1, expr2) * COVAR_POP(expr1, expr2) for non-null pairs.






namespace duckdb {

struct RegrSXyState {
	size_t count;
	CovarState cov_pop;
};

struct RegrSXYOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		RegrCountFunction::Initialize<size_t>(state.count);
		CovarOperation::Initialize<CovarState>(state.cov_pop);
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &y, const B_TYPE &x, AggregateBinaryInput &idata) {
		RegrCountFunction::Operation<A_TYPE, B_TYPE, size_t, OP>(state.count, y, x, idata);
		CovarOperation::Operation<A_TYPE, B_TYPE, CovarState, OP>(state.cov_pop, y, x, idata);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		CovarOperation::Combine<CovarState, OP>(source.cov_pop, target.cov_pop, aggr_input_data);
		RegrCountFunction::Combine<size_t, OP>(source.count, target.count, aggr_input_data);
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		CovarPopOperation::Finalize<T, CovarState>(state.cov_pop, target, finalize_data);
		auto cov_pop = target;
		RegrCountFunction::Finalize<T, size_t>(state.count, target, finalize_data);
		target *= cov_pop;
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction RegrSXYFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<RegrSXyState, double, double, double, RegrSXYOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}

} // namespace duckdb





namespace duckdb {

template <class T>
void FillExtraInfo(StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	info.description = function.description;
	info.parameter_names = StringUtil::Split(function.parameters, ",");
	info.example = function.example;
}

void CoreFunctions::RegisterFunctions(Catalog &catalog, CatalogTransaction transaction) {
	auto functions = StaticFunctionDefinition::GetFunctionList();
	for (idx_t i = 0; functions[i].name; i++) {
		auto &function = functions[i];
		if (function.get_function || function.get_function_set) {
			// scalar function
			ScalarFunctionSet result;
			if (function.get_function) {
				result.AddFunction(function.get_function());
			} else {
				result = function.get_function_set();
			}
			result.name = function.name;
			CreateScalarFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else if (function.get_aggregate_function || function.get_aggregate_function_set) {
			// aggregate function
			AggregateFunctionSet result;
			if (function.get_aggregate_function) {
				result.AddFunction(function.get_aggregate_function());
			} else {
				result = function.get_aggregate_function_set();
			}
			result.name = function.name;
			CreateAggregateFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

} // namespace duckdb





















namespace duckdb {

// Scalar Function
#define DUCKDB_SCALAR_FUNCTION_BASE(_PARAM, _NAME)                                                                     \
	{ _NAME, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, _PARAM::GetFunction, nullptr, nullptr, nullptr }
#define DUCKDB_SCALAR_FUNCTION(_PARAM)       DUCKDB_SCALAR_FUNCTION_BASE(_PARAM, _PARAM::Name)
#define DUCKDB_SCALAR_FUNCTION_ALIAS(_PARAM) DUCKDB_SCALAR_FUNCTION_BASE(_PARAM::ALIAS, _PARAM::Name)
// Scalar Function Set
#define DUCKDB_SCALAR_FUNCTION_SET_BASE(_PARAM, _NAME)                                                                 \
	{ _NAME, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, nullptr, _PARAM::GetFunctions, nullptr, nullptr }
#define DUCKDB_SCALAR_FUNCTION_SET(_PARAM)       DUCKDB_SCALAR_FUNCTION_SET_BASE(_PARAM, _PARAM::Name)
#define DUCKDB_SCALAR_FUNCTION_SET_ALIAS(_PARAM) DUCKDB_SCALAR_FUNCTION_SET_BASE(_PARAM::ALIAS, _PARAM::Name)
// Aggregate Function
#define DUCKDB_AGGREGATE_FUNCTION_BASE(_PARAM, _NAME)                                                                  \
	{ _NAME, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, nullptr, nullptr, _PARAM::GetFunction, nullptr }
#define DUCKDB_AGGREGATE_FUNCTION(_PARAM)       DUCKDB_AGGREGATE_FUNCTION_BASE(_PARAM, _PARAM::Name)
#define DUCKDB_AGGREGATE_FUNCTION_ALIAS(_PARAM) DUCKDB_AGGREGATE_FUNCTION_BASE(_PARAM::ALIAS, _PARAM::Name)
// Aggregate Function Set
#define DUCKDB_AGGREGATE_FUNCTION_SET_BASE(_PARAM, _NAME)                                                              \
	{ _NAME, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, nullptr, nullptr, nullptr, _PARAM::GetFunctions }
#define DUCKDB_AGGREGATE_FUNCTION_SET(_PARAM)       DUCKDB_AGGREGATE_FUNCTION_SET_BASE(_PARAM, _PARAM::Name)
#define DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(_PARAM) DUCKDB_AGGREGATE_FUNCTION_SET_BASE(_PARAM::ALIAS, _PARAM::Name)
#define FINAL_FUNCTION                                                                                                 \
	{ nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr }

// this list is generated by scripts/generate_functions.py
static StaticFunctionDefinition internal_functions[] = {
	DUCKDB_SCALAR_FUNCTION(FactorialOperatorFun),
	DUCKDB_SCALAR_FUNCTION_SET(BitwiseAndFun),
	DUCKDB_SCALAR_FUNCTION(PowOperatorFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ListInnerProductFunAlias),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ListDistanceFunAlias),
	DUCKDB_SCALAR_FUNCTION_SET(LeftShiftFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ListCosineSimilarityFunAlias),
	DUCKDB_SCALAR_FUNCTION_SET(RightShiftFun),
	DUCKDB_SCALAR_FUNCTION_SET(AbsOperatorFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(PowOperatorFunAlias),
	DUCKDB_SCALAR_FUNCTION(StartsWithOperatorFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(AbsFun),
	DUCKDB_SCALAR_FUNCTION(AcosFun),
	DUCKDB_SCALAR_FUNCTION_SET(AgeFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(AggregateFun),
	DUCKDB_SCALAR_FUNCTION(AliasFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ApplyFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ApproxCountDistinctFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ApproxQuantileFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ArgMaxFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ArgMinFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(ArgmaxFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(ArgminFun),
	DUCKDB_AGGREGATE_FUNCTION_ALIAS(ArrayAggFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayAggrFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayAggregateFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayApplyFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayDistinctFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayFilterFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ArrayReverseSortFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ArraySliceFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ArraySortFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayTransformFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ArrayUniqueFun),
	DUCKDB_SCALAR_FUNCTION(ASCIIFun),
	DUCKDB_SCALAR_FUNCTION(AsinFun),
	DUCKDB_SCALAR_FUNCTION(AtanFun),
	DUCKDB_SCALAR_FUNCTION(Atan2Fun),
	DUCKDB_AGGREGATE_FUNCTION_SET(AvgFun),
	DUCKDB_SCALAR_FUNCTION_SET(BarFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(Base64Fun),
	DUCKDB_SCALAR_FUNCTION_SET(BinFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(BitAndFun),
	DUCKDB_SCALAR_FUNCTION_SET(BitCountFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(BitOrFun),
	DUCKDB_SCALAR_FUNCTION(BitPositionFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(BitXorFun),
	DUCKDB_SCALAR_FUNCTION(BitStringFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(BitstringAggFun),
	DUCKDB_AGGREGATE_FUNCTION(BoolAndFun),
	DUCKDB_AGGREGATE_FUNCTION(BoolOrFun),
	DUCKDB_SCALAR_FUNCTION(CardinalityFun),
	DUCKDB_SCALAR_FUNCTION(CbrtFun),
	DUCKDB_SCALAR_FUNCTION_SET(CeilFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(CeilingFun),
	DUCKDB_SCALAR_FUNCTION_SET(CenturyFun),
	DUCKDB_SCALAR_FUNCTION(ChrFun),
	DUCKDB_AGGREGATE_FUNCTION(CorrFun),
	DUCKDB_SCALAR_FUNCTION(CosFun),
	DUCKDB_SCALAR_FUNCTION(CotFun),
	DUCKDB_AGGREGATE_FUNCTION(CovarPopFun),
	DUCKDB_AGGREGATE_FUNCTION(CovarSampFun),
	DUCKDB_SCALAR_FUNCTION(CurrentDatabaseFun),
	DUCKDB_SCALAR_FUNCTION(CurrentDateFun),
	DUCKDB_SCALAR_FUNCTION(CurrentQueryFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSchemaFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSchemasFun),
	DUCKDB_SCALAR_FUNCTION(CurrentSettingFun),
	DUCKDB_SCALAR_FUNCTION(DamerauLevenshteinFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateDiffFun),
	DUCKDB_SCALAR_FUNCTION_SET(DatePartFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateSubFun),
	DUCKDB_SCALAR_FUNCTION_SET(DateTruncFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(DatediffFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(DatepartFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(DatesubFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(DatetruncFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayNameFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfMonthFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfWeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(DayOfYearFun),
	DUCKDB_SCALAR_FUNCTION_SET(DecadeFun),
	DUCKDB_SCALAR_FUNCTION(DecodeFun),
	DUCKDB_SCALAR_FUNCTION(DegreesFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(Editdist3Fun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ElementAtFun),
	DUCKDB_SCALAR_FUNCTION(EncodeFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(EntropyFun),
	DUCKDB_SCALAR_FUNCTION(EnumCodeFun),
	DUCKDB_SCALAR_FUNCTION(EnumFirstFun),
	DUCKDB_SCALAR_FUNCTION(EnumLastFun),
	DUCKDB_SCALAR_FUNCTION(EnumRangeFun),
	DUCKDB_SCALAR_FUNCTION(EnumRangeBoundaryFun),
	DUCKDB_SCALAR_FUNCTION_SET(EpochFun),
	DUCKDB_SCALAR_FUNCTION_SET(EpochMsFun),
	DUCKDB_SCALAR_FUNCTION_SET(EpochNsFun),
	DUCKDB_SCALAR_FUNCTION_SET(EpochUsFun),
	DUCKDB_SCALAR_FUNCTION_SET(EraFun),
	DUCKDB_SCALAR_FUNCTION(ErrorFun),
	DUCKDB_SCALAR_FUNCTION(EvenFun),
	DUCKDB_SCALAR_FUNCTION(ExpFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FactorialFun),
	DUCKDB_AGGREGATE_FUNCTION(FAvgFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FilterFun),
	DUCKDB_SCALAR_FUNCTION(ListFlattenFun),
	DUCKDB_SCALAR_FUNCTION_SET(FloorFun),
	DUCKDB_SCALAR_FUNCTION(FormatFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FormatreadabledecimalsizeFun),
	DUCKDB_SCALAR_FUNCTION(FormatBytesFun),
	DUCKDB_SCALAR_FUNCTION(FromBase64Fun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FromBinaryFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(FromHexFun),
	DUCKDB_AGGREGATE_FUNCTION_ALIAS(FsumFun),
	DUCKDB_SCALAR_FUNCTION(GammaFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(GcdFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(GenRandomUuidFun),
	DUCKDB_SCALAR_FUNCTION_SET(GenerateSeriesFun),
	DUCKDB_SCALAR_FUNCTION(GetBitFun),
	DUCKDB_SCALAR_FUNCTION(CurrentTimeFun),
	DUCKDB_SCALAR_FUNCTION(GetCurrentTimestampFun),
	DUCKDB_SCALAR_FUNCTION_SET(GreatestFun),
	DUCKDB_SCALAR_FUNCTION_SET(GreatestCommonDivisorFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(GroupConcatFun),
	DUCKDB_SCALAR_FUNCTION(HammingFun),
	DUCKDB_SCALAR_FUNCTION(HashFun),
	DUCKDB_SCALAR_FUNCTION_SET(HexFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(HistogramFun),
	DUCKDB_SCALAR_FUNCTION_SET(HoursFun),
	DUCKDB_SCALAR_FUNCTION(InSearchPathFun),
	DUCKDB_SCALAR_FUNCTION(InstrFun),
	DUCKDB_SCALAR_FUNCTION_SET(IsFiniteFun),
	DUCKDB_SCALAR_FUNCTION_SET(IsInfiniteFun),
	DUCKDB_SCALAR_FUNCTION_SET(IsNanFun),
	DUCKDB_SCALAR_FUNCTION_SET(ISODayOfWeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(ISOYearFun),
	DUCKDB_SCALAR_FUNCTION(JaccardFun),
	DUCKDB_SCALAR_FUNCTION(JaroSimilarityFun),
	DUCKDB_SCALAR_FUNCTION(JaroWinklerSimilarityFun),
	DUCKDB_SCALAR_FUNCTION_SET(JulianDayFun),
	DUCKDB_AGGREGATE_FUNCTION(KahanSumFun),
	DUCKDB_AGGREGATE_FUNCTION(KurtosisFun),
	DUCKDB_SCALAR_FUNCTION_SET(LastDayFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(LcmFun),
	DUCKDB_SCALAR_FUNCTION_SET(LeastFun),
	DUCKDB_SCALAR_FUNCTION_SET(LeastCommonMultipleFun),
	DUCKDB_SCALAR_FUNCTION(LeftFun),
	DUCKDB_SCALAR_FUNCTION(LeftGraphemeFun),
	DUCKDB_SCALAR_FUNCTION(LevenshteinFun),
	DUCKDB_SCALAR_FUNCTION(LogGammaFun),
	DUCKDB_AGGREGATE_FUNCTION(ListFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListAggrFun),
	DUCKDB_SCALAR_FUNCTION(ListAggregateFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListApplyFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListCosineSimilarityFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListDistanceFun),
	DUCKDB_SCALAR_FUNCTION(ListDistinctFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ListDotProductFun),
	DUCKDB_SCALAR_FUNCTION(ListFilterFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListInnerProductFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(ListPackFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListReverseSortFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListSliceFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListSortFun),
	DUCKDB_SCALAR_FUNCTION(ListTransformFun),
	DUCKDB_SCALAR_FUNCTION(ListUniqueFun),
	DUCKDB_SCALAR_FUNCTION(ListValueFun),
	DUCKDB_SCALAR_FUNCTION(LnFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(LogFun),
	DUCKDB_SCALAR_FUNCTION(Log10Fun),
	DUCKDB_SCALAR_FUNCTION(Log2Fun),
	DUCKDB_SCALAR_FUNCTION(LpadFun),
	DUCKDB_SCALAR_FUNCTION_SET(LtrimFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(MadFun),
	DUCKDB_SCALAR_FUNCTION_SET(MakeDateFun),
	DUCKDB_SCALAR_FUNCTION(MakeTimeFun),
	DUCKDB_SCALAR_FUNCTION_SET(MakeTimestampFun),
	DUCKDB_SCALAR_FUNCTION(MapFun),
	DUCKDB_SCALAR_FUNCTION(MapConcatFun),
	DUCKDB_SCALAR_FUNCTION(MapEntriesFun),
	DUCKDB_SCALAR_FUNCTION(MapExtractFun),
	DUCKDB_SCALAR_FUNCTION(MapFromEntriesFun),
	DUCKDB_SCALAR_FUNCTION(MapKeysFun),
	DUCKDB_SCALAR_FUNCTION(MapValuesFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(MaxFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(MaxByFun),
	DUCKDB_SCALAR_FUNCTION(MD5Fun),
	DUCKDB_SCALAR_FUNCTION(MD5NumberFun),
	DUCKDB_SCALAR_FUNCTION(MD5NumberLowerFun),
	DUCKDB_SCALAR_FUNCTION(MD5NumberUpperFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(MeanFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(MedianFun),
	DUCKDB_SCALAR_FUNCTION_SET(MicrosecondsFun),
	DUCKDB_SCALAR_FUNCTION_SET(MillenniumFun),
	DUCKDB_SCALAR_FUNCTION_SET(MillisecondsFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(MinFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(MinByFun),
	DUCKDB_SCALAR_FUNCTION_SET(MinutesFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(MismatchesFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ModeFun),
	DUCKDB_SCALAR_FUNCTION_SET(MonthFun),
	DUCKDB_SCALAR_FUNCTION_SET(MonthNameFun),
	DUCKDB_SCALAR_FUNCTION_SET(NextAfterFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(NowFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(OrdFun),
	DUCKDB_SCALAR_FUNCTION(PiFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(PositionFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(PowFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(PowerFun),
	DUCKDB_SCALAR_FUNCTION(PrintfFun),
	DUCKDB_AGGREGATE_FUNCTION(ProductFun),
	DUCKDB_AGGREGATE_FUNCTION_SET_ALIAS(QuantileFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(QuantileContFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(QuantileDiscFun),
	DUCKDB_SCALAR_FUNCTION_SET(QuarterFun),
	DUCKDB_SCALAR_FUNCTION(RadiansFun),
	DUCKDB_SCALAR_FUNCTION(RandomFun),
	DUCKDB_SCALAR_FUNCTION_SET(ListRangeFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(RegexpSplitToArrayFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrAvgxFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrAvgyFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrCountFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrInterceptFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrR2Fun),
	DUCKDB_AGGREGATE_FUNCTION(RegrSlopeFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrSXXFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrSXYFun),
	DUCKDB_AGGREGATE_FUNCTION(RegrSYYFun),
	DUCKDB_SCALAR_FUNCTION_SET(RepeatFun),
	DUCKDB_SCALAR_FUNCTION(ReplaceFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(ReservoirQuantileFun),
	DUCKDB_SCALAR_FUNCTION(ReverseFun),
	DUCKDB_SCALAR_FUNCTION(RightFun),
	DUCKDB_SCALAR_FUNCTION(RightGraphemeFun),
	DUCKDB_SCALAR_FUNCTION_SET(RoundFun),
	DUCKDB_SCALAR_FUNCTION(RowFun),
	DUCKDB_SCALAR_FUNCTION(RpadFun),
	DUCKDB_SCALAR_FUNCTION_SET(RtrimFun),
	DUCKDB_SCALAR_FUNCTION_SET(SecondsFun),
	DUCKDB_AGGREGATE_FUNCTION(StandardErrorOfTheMeanFun),
	DUCKDB_SCALAR_FUNCTION(SetBitFun),
	DUCKDB_SCALAR_FUNCTION(SetseedFun),
	DUCKDB_SCALAR_FUNCTION(SHA256Fun),
	DUCKDB_SCALAR_FUNCTION_SET(SignFun),
	DUCKDB_SCALAR_FUNCTION_SET(SignBitFun),
	DUCKDB_SCALAR_FUNCTION(SinFun),
	DUCKDB_AGGREGATE_FUNCTION(SkewnessFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(SplitFun),
	DUCKDB_SCALAR_FUNCTION(SqrtFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(StartsWithFun),
	DUCKDB_SCALAR_FUNCTION(StatsFun),
	DUCKDB_AGGREGATE_FUNCTION_ALIAS(StddevFun),
	DUCKDB_AGGREGATE_FUNCTION(StdDevPopFun),
	DUCKDB_AGGREGATE_FUNCTION(StdDevSampFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(StrSplitFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(StrSplitRegexFun),
	DUCKDB_SCALAR_FUNCTION_SET(StrfTimeFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(StringAggFun),
	DUCKDB_SCALAR_FUNCTION(StringSplitFun),
	DUCKDB_SCALAR_FUNCTION_SET(StringSplitRegexFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(StringToArrayFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(StrposFun),
	DUCKDB_SCALAR_FUNCTION_SET(StrpTimeFun),
	DUCKDB_SCALAR_FUNCTION(StructInsertFun),
	DUCKDB_SCALAR_FUNCTION(StructPackFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(SumFun),
	DUCKDB_AGGREGATE_FUNCTION_SET(SumNoOverflowFun),
	DUCKDB_AGGREGATE_FUNCTION_ALIAS(SumkahanFun),
	DUCKDB_SCALAR_FUNCTION(TanFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimeBucketFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneHourFun),
	DUCKDB_SCALAR_FUNCTION_SET(TimezoneMinuteFun),
	DUCKDB_SCALAR_FUNCTION_SET(ToBaseFun),
	DUCKDB_SCALAR_FUNCTION(ToBase64Fun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ToBinaryFun),
	DUCKDB_SCALAR_FUNCTION(ToDaysFun),
	DUCKDB_SCALAR_FUNCTION_SET_ALIAS(ToHexFun),
	DUCKDB_SCALAR_FUNCTION(ToHoursFun),
	DUCKDB_SCALAR_FUNCTION(ToMicrosecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToMillisecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToMinutesFun),
	DUCKDB_SCALAR_FUNCTION(ToMonthsFun),
	DUCKDB_SCALAR_FUNCTION(ToSecondsFun),
	DUCKDB_SCALAR_FUNCTION(ToTimestampFun),
	DUCKDB_SCALAR_FUNCTION(ToYearsFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(TodayFun),
	DUCKDB_SCALAR_FUNCTION_ALIAS(TransactionTimestampFun),
	DUCKDB_SCALAR_FUNCTION(TranslateFun),
	DUCKDB_SCALAR_FUNCTION_SET(TrimFun),
	DUCKDB_SCALAR_FUNCTION_SET(TruncFun),
	DUCKDB_SCALAR_FUNCTION_SET(TryStrpTimeFun),
	DUCKDB_SCALAR_FUNCTION(CurrentTransactionIdFun),
	DUCKDB_SCALAR_FUNCTION(TypeOfFun),
	DUCKDB_SCALAR_FUNCTION(UnbinFun),
	DUCKDB_SCALAR_FUNCTION(UnhexFun),
	DUCKDB_SCALAR_FUNCTION(UnicodeFun),
	DUCKDB_SCALAR_FUNCTION(UnionExtractFun),
	DUCKDB_SCALAR_FUNCTION(UnionTagFun),
	DUCKDB_SCALAR_FUNCTION(UnionValueFun),
	DUCKDB_SCALAR_FUNCTION(UUIDFun),
	DUCKDB_AGGREGATE_FUNCTION(VarPopFun),
	DUCKDB_AGGREGATE_FUNCTION(VarSampFun),
	DUCKDB_AGGREGATE_FUNCTION_ALIAS(VarianceFun),
	DUCKDB_SCALAR_FUNCTION(VectorTypeFun),
	DUCKDB_SCALAR_FUNCTION(VersionFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekDayFun),
	DUCKDB_SCALAR_FUNCTION_SET(WeekOfYearFun),
	DUCKDB_SCALAR_FUNCTION_SET(BitwiseXorFun),
	DUCKDB_SCALAR_FUNCTION_SET(YearFun),
	DUCKDB_SCALAR_FUNCTION_SET(YearWeekFun),
	DUCKDB_SCALAR_FUNCTION_SET(BitwiseOrFun),
	DUCKDB_SCALAR_FUNCTION_SET(BitwiseNotFun),
	FINAL_FUNCTION
};

StaticFunctionDefinition *StaticFunctionDefinition::GetFunctionList() {
	return internal_functions;
}

} // namespace duckdb




namespace duckdb {

//===--------------------------------------------------------------------===//
// BitStringFunction
//===--------------------------------------------------------------------===//
static void BitStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, int32_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t input, int32_t n) {
		    if (n < 0) {
			    throw InvalidInputException("The bitstring length cannot be negative");
		    }
		    if (idx_t(n) < input.GetSize()) {
			    throw InvalidInputException("Length must be equal or larger than input string");
		    }
		    idx_t len;
		    Bit::TryGetBitStringSize(input, len, nullptr); // string verification

		    len = Bit::ComputeBitstringLen(n);
		    string_t target = StringVector::EmptyString(result, len);
		    Bit::BitString(input, n, target);
		    target.Finalize();
		    return target;
	    });
}

ScalarFunction BitStringFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::BIT, BitStringFunction);
}

//===--------------------------------------------------------------------===//
// get_bit
//===--------------------------------------------------------------------===//
struct GetBitOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA input, TB n) {
		if (n < 0 || (idx_t)n > Bit::BitLength(input) - 1) {
			throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                          NumericHelper::ToString(Bit::BitLength(input) - 1));
		}
		return Bit::GetBit(input, n);
	}
};

ScalarFunction GetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::INTEGER}, LogicalType::INTEGER,
	                      ScalarFunction::BinaryFunction<string_t, int32_t, int32_t, GetBitOperator>);
}

//===--------------------------------------------------------------------===//
// set_bit
//===--------------------------------------------------------------------===//
static void SetBitOperation(DataChunk &args, ExpressionState &state, Vector &result) {
	TernaryExecutor::Execute<string_t, int32_t, int32_t, string_t>(
	    args.data[0], args.data[1], args.data[2], result, args.size(),
	    [&](string_t input, int32_t n, int32_t new_value) {
		    if (new_value != 0 && new_value != 1) {
			    throw InvalidInputException("The new bit must be 1 or 0");
		    }
		    if (n < 0 || (idx_t)n > Bit::BitLength(input) - 1) {
			    throw OutOfRangeException("bit index %s out of valid range (0..%s)", NumericHelper::ToString(n),
			                              NumericHelper::ToString(Bit::BitLength(input) - 1));
		    }
		    string_t target = StringVector::EmptyString(result, input.GetSize());
		    memcpy(target.GetDataWriteable(), input.GetData(), input.GetSize());
		    Bit::SetBit(target, n, new_value);
		    return target;
	    });
}

ScalarFunction SetBitFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::INTEGER, LogicalType::INTEGER}, LogicalType::BIT,
	                      SetBitOperation);
}

//===--------------------------------------------------------------------===//
// bit_position
//===--------------------------------------------------------------------===//
struct BitPositionOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA substring, TB input) {
		if (substring.GetSize() > input.GetSize()) {
			return 0;
		}
		return Bit::BitPosition(substring, input);
	}
};

ScalarFunction BitPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::BIT, LogicalType::BIT}, LogicalType::INTEGER,
	                      ScalarFunction::BinaryFunction<string_t, string_t, int32_t, BitPositionOperator>);
}

} // namespace duckdb



namespace duckdb {

struct Base64EncodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto result_str = StringVector::EmptyString(result, Blob::ToBase64Size(input));
		Blob::ToBase64(input, result_str.GetDataWriteable());
		result_str.Finalize();
		return result_str;
	}
};

struct Base64DecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
		auto result_size = Blob::FromBase64Size(input);
		auto result_blob = StringVector::EmptyString(result, result_size);
		Blob::FromBase64(input, data_ptr_cast(result_blob.GetDataWriteable()), result_size);
		result_blob.Finalize();
		return result_blob;
	}
};

static void Base64EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64EncodeOperator>(args.data[0], result, args.size());
}

static void Base64DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::ExecuteString<string_t, string_t, Base64DecodeOperator>(args.data[0], result, args.size());
}

ScalarFunction ToBase64Fun::GetFunction() {
	return ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, Base64EncodeFunction);
}

ScalarFunction FromBase64Fun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, Base64DecodeFunction);
}

} // namespace duckdb



namespace duckdb {

static void EncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// encode is essentially a nop cast from varchar to blob
	// we only need to reinterpret the data using the blob type
	result.Reinterpret(args.data[0]);
}

struct BlobDecodeOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE input) {
		auto input_data = input.GetData();
		auto input_length = input.GetSize();
		if (Utf8Proc::Analyze(input_data, input_length) == UnicodeType::INVALID) {
			throw ConversionException(
			    "Failure in decode: could not convert blob to UTF8 string, the blob contained invalid UTF8 characters");
		}
		return input;
	}
};

static void DecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// decode is also a nop cast, but requires verification if the provided string is actually
	UnaryExecutor::Execute<string_t, string_t, BlobDecodeOperator>(args.data[0], result, args.size());
	StringVector::AddHeapReference(result, args.data[0]);
}

ScalarFunction EncodeFun::GetFunction() {
	return ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB, EncodeFunction);
}

ScalarFunction DecodeFun::GetFunction() {
	return ScalarFunction({LogicalType::BLOB}, LogicalType::VARCHAR, DecodeFunction);
}

} // namespace duckdb








namespace duckdb {

static void AgeFunctionStandard(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	auto current_timestamp = Timestamp::GetCurrentTimestamp();

	UnaryExecutor::ExecuteWithNulls<timestamp_t, interval_t>(input.data[0], result, input.size(),
	                                                         [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		                                                         if (Timestamp::IsFinite(input)) {
			                                                         return Interval::GetAge(current_timestamp, input);
		                                                         } else {
			                                                         mask.SetInvalid(idx);
			                                                         return interval_t();
		                                                         }
	                                                         });
}

static void AgeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 2);

	BinaryExecutor::ExecuteWithNulls<timestamp_t, timestamp_t, interval_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [&](timestamp_t input1, timestamp_t input2, ValidityMask &mask, idx_t idx) {
		    if (Timestamp::IsFinite(input1) && Timestamp::IsFinite(input2)) {
			    return Interval::GetAge(input1, input2);
		    } else {
			    mask.SetInvalid(idx);
			    return interval_t();
		    }
	    });
}

ScalarFunctionSet AgeFun::GetFunctions() {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunctionStandard));
	age.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunction));
	return age;
}

} // namespace duckdb









namespace duckdb {

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return MetaTransaction::Get(state.GetContext()).start_timestamp;
}

static void CurrentTimeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto val = Value::TIME(Timestamp::GetTime(GetTransactionTimestamp(state)));
	result.Reference(val);
}

static void CurrentDateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::DATE(Timestamp::GetDate(GetTransactionTimestamp(state)));
	result.Reference(val);
}

static void CurrentTimestampFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::TIMESTAMPTZ(GetTransactionTimestamp(state));
	result.Reference(val);
}

ScalarFunction CurrentTimeFun::GetFunction() {
	ScalarFunction current_time({}, LogicalType::TIME, CurrentTimeFunction);
	current_time.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_time;
}

ScalarFunction CurrentDateFun::GetFunction() {
	ScalarFunction current_date({}, LogicalType::DATE, CurrentDateFunction);
	current_date.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_date;
}

ScalarFunction GetCurrentTimestampFun::GetFunction() {
	ScalarFunction current_timestamp({}, LogicalType::TIMESTAMP_TZ, CurrentTimestampFunction);
	current_timestamp.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return current_timestamp;
}

} // namespace duckdb












namespace duckdb {

// This function is an implementation of the "period-crossing" date difference function from T-SQL
// https://docs.microsoft.com/en-us/sql/t-sql/functions/datediff-transact-sql?view=sql-server-ver15
struct DateDiff {
	template <class TA, class TB, class TR, class OP>
	static inline void BinaryExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::ExecuteWithNulls<TA, TB, TR>(
		    left, right, result, count, [&](TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
			    if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
				    return OP::template Operation<TA, TB, TR>(startdate, enddate);
			    } else {
				    mask.SetInvalid(idx);
				    return TR();
			    }
		    });
	}

	struct YearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) - Date::ExtractYear(startdate);
		}
	};

	struct MonthOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			int32_t start_year, start_month, start_day;
			Date::Convert(startdate, start_year, start_month, start_day);
			int32_t end_year, end_month, end_day;
			Date::Convert(enddate, end_year, end_month, end_day);

			return (end_year * 12 + end_month - 1) - (start_year * 12 + start_month - 1);
		}
	};

	struct DayOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return TR(Date::EpochDays(enddate)) - TR(Date::EpochDays(startdate));
		}
	};

	struct DecadeOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 10 - Date::ExtractYear(startdate) / 10;
		}
	};

	struct CenturyOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 100 - Date::ExtractYear(startdate) / 100;
		}
	};

	struct MilleniumOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractYear(enddate) / 1000 - Date::ExtractYear(startdate) / 1000;
		}
	};

	struct QuarterOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			int32_t start_year, start_month, start_day;
			Date::Convert(startdate, start_year, start_month, start_day);
			int32_t end_year, end_month, end_day;
			Date::Convert(enddate, end_year, end_month, end_day);

			return (end_year * 12 + end_month - 1) / Interval::MONTHS_PER_QUARTER -
			       (start_year * 12 + start_month - 1) / Interval::MONTHS_PER_QUARTER;
		}
	};

	struct WeekOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(Date::GetMondayOfCurrentWeek(enddate)) / Interval::SECS_PER_WEEK -
			       Date::Epoch(Date::GetMondayOfCurrentWeek(startdate)) / Interval::SECS_PER_WEEK;
		}
	};

	struct ISOYearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::ExtractISOYearNumber(enddate) - Date::ExtractISOYearNumber(startdate);
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::EpochMicroseconds(enddate) - Date::EpochMicroseconds(startdate);
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::EpochMicroseconds(enddate) / Interval::MICROS_PER_MSEC -
			       Date::EpochMicroseconds(startdate) / Interval::MICROS_PER_MSEC;
		}
	};

	struct SecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) - Date::Epoch(startdate);
		}
	};

	struct MinutesOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) / Interval::SECS_PER_MINUTE -
			       Date::Epoch(startdate) / Interval::SECS_PER_MINUTE;
		}
	};

	struct HoursOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return Date::Epoch(enddate) / Interval::SECS_PER_HOUR - Date::Epoch(startdate) / Interval::SECS_PER_HOUR;
		}
	};
};

// TIMESTAMP specialisations
template <>
int64_t DateDiff::YearOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return YearOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MonthOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return MonthOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                         Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::DayOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return DayOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::DecadeOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return DecadeOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                          Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::CenturyOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return CenturyOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MilleniumOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return MilleniumOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                             Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::QuarterOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return QuarterOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::WeekOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return WeekOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate), Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::ISOYearOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return ISOYearOperator::Operation<date_t, date_t, int64_t>(Timestamp::GetDate(startdate),
	                                                           Timestamp::GetDate(enddate));
}

template <>
int64_t DateDiff::MicrosecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	const auto start = Timestamp::GetEpochMicroSeconds(startdate);
	const auto end = Timestamp::GetEpochMicroSeconds(enddate);
	return SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(end, start);
}

template <>
int64_t DateDiff::MillisecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochMs(enddate) - Timestamp::GetEpochMs(startdate);
}

template <>
int64_t DateDiff::SecondsOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) - Timestamp::GetEpochSeconds(startdate);
}

template <>
int64_t DateDiff::MinutesOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) / Interval::SECS_PER_MINUTE -
	       Timestamp::GetEpochSeconds(startdate) / Interval::SECS_PER_MINUTE;
}

template <>
int64_t DateDiff::HoursOperator::Operation(timestamp_t startdate, timestamp_t enddate) {
	return Timestamp::GetEpochSeconds(enddate) / Interval::SECS_PER_HOUR -
	       Timestamp::GetEpochSeconds(startdate) / Interval::SECS_PER_HOUR;
}

// TIME specialisations
template <>
int64_t DateDiff::YearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DateDiff::MonthOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DateDiff::DayOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DateDiff::DecadeOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DateDiff::CenturyOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DateDiff::MilleniumOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DateDiff::QuarterOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DateDiff::WeekOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DateDiff::ISOYearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"isoyear\" not recognized");
}

template <>
int64_t DateDiff::MicrosecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros - startdate.micros;
}

template <>
int64_t DateDiff::MillisecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_MSEC - startdate.micros / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DateDiff::SecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_SEC - startdate.micros / Interval::MICROS_PER_SEC;
}

template <>
int64_t DateDiff::MinutesOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_MINUTE - startdate.micros / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DateDiff::HoursOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros / Interval::MICROS_PER_HOUR - startdate.micros / Interval::MICROS_PER_HOUR;
}

template <typename TA, typename TB, typename TR>
static int64_t DifferenceDates(DatePartSpecifier type, TA startdate, TB enddate) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return DateDiff::YearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MONTH:
		return DateDiff::MonthOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return DateDiff::DayOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DECADE:
		return DateDiff::DecadeOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::CENTURY:
		return DateDiff::CenturyOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLENNIUM:
		return DateDiff::MilleniumOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::QUARTER:
		return DateDiff::QuarterOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateDiff::WeekOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::ISOYEAR:
		return DateDiff::ISOYearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MICROSECONDS:
		return DateDiff::MicrosecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLISECONDS:
		return DateDiff::MillisecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateDiff::SecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MINUTE:
		return DateDiff::MinutesOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::HOUR:
		return DateDiff::HoursOperator::template Operation<TA, TB, TR>(startdate, enddate);
	default:
		throw NotImplementedException("Specifier type not implemented for DATEDIFF");
	}
}

struct DateDiffTernaryOperator {
	template <typename TS, typename TA, typename TB, typename TR>
	static inline TR Operation(TS part, TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
		if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
			return DifferenceDates<TA, TB, TR>(GetDatePartSpecifier(part.GetString()), startdate, enddate);
		} else {
			mask.SetInvalid(idx);
			return TR();
		}
	}
};

template <typename TA, typename TB, typename TR>
static void DateDiffBinaryExecutor(DatePartSpecifier type, Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::YearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MONTH:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::MonthOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::DayOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DECADE:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::DecadeOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::CenturyOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLENNIUM:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::MilleniumOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::QuarterOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::WeekOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::ISOYEAR:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::ISOYearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::MicrosecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::MillisecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::SecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::MinutesOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::HOUR:
		DateDiff::BinaryExecute<TA, TB, TR, DateDiff::HoursOperator>(left, right, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATEDIFF");
	}
}

template <typename T>
static void DateDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	auto &part_arg = args.data[0];
	auto &start_arg = args.data[1];
	auto &end_arg = args.data[2];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateDiffBinaryExecutor<T, T, int64_t>(type, start_arg, end_arg, result, args.size());
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<string_t, T, T, int64_t>(
		    part_arg, start_arg, end_arg, result, args.size(),
		    DateDiffTernaryOperator::Operation<string_t, T, T, int64_t>);
	}
}

ScalarFunctionSet DateDiffFun::GetFunctions() {
	ScalarFunctionSet date_diff("date_diff");
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
	                                     LogicalType::BIGINT, DateDiffFunction<date_t>));
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                     LogicalType::BIGINT, DateDiffFunction<timestamp_t>));
	date_diff.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME, LogicalType::TIME},
	                                     LogicalType::BIGINT, DateDiffFunction<dtime_t>));
	return date_diff;
}

} // namespace duckdb













namespace duckdb {

DatePartSpecifier GetDateTypePartSpecifier(const string &specifier, LogicalType &type) {
	const auto part = GetDatePartSpecifier(specifier);
	switch (type.id()) {
	case LogicalType::TIMESTAMP:
	case LogicalType::TIMESTAMP_TZ:
		return part;
	case LogicalType::DATE:
		switch (part) {
		case DatePartSpecifier::YEAR:
		case DatePartSpecifier::MONTH:
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DECADE:
		case DatePartSpecifier::CENTURY:
		case DatePartSpecifier::MILLENNIUM:
		case DatePartSpecifier::DOW:
		case DatePartSpecifier::ISODOW:
		case DatePartSpecifier::ISOYEAR:
		case DatePartSpecifier::WEEK:
		case DatePartSpecifier::QUARTER:
		case DatePartSpecifier::DOY:
		case DatePartSpecifier::YEARWEEK:
		case DatePartSpecifier::ERA:
		case DatePartSpecifier::EPOCH:
		case DatePartSpecifier::JULIAN_DAY:
			return part;
		default:
			break;
		}
		break;
	case LogicalType::TIME:
		switch (part) {
		case DatePartSpecifier::MICROSECONDS:
		case DatePartSpecifier::MILLISECONDS:
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::MINUTE:
		case DatePartSpecifier::HOUR:
		case DatePartSpecifier::EPOCH:
		case DatePartSpecifier::TIMEZONE:
		case DatePartSpecifier::TIMEZONE_HOUR:
		case DatePartSpecifier::TIMEZONE_MINUTE:
			return part;
		default:
			break;
		}
		break;
	case LogicalType::INTERVAL:
		switch (part) {
		case DatePartSpecifier::YEAR:
		case DatePartSpecifier::MONTH:
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DECADE:
		case DatePartSpecifier::CENTURY:
		case DatePartSpecifier::QUARTER:
		case DatePartSpecifier::MILLENNIUM:
		case DatePartSpecifier::MICROSECONDS:
		case DatePartSpecifier::MILLISECONDS:
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::MINUTE:
		case DatePartSpecifier::HOUR:
		case DatePartSpecifier::EPOCH:
			return part;
		default:
			break;
		}
		break;
	default:
		break;
	}

	throw NotImplementedException("\"%s\" units \"%s\" not recognized", EnumUtil::ToString(type.id()), specifier);
}

template <int64_t MIN, int64_t MAX>
static unique_ptr<BaseStatistics> PropagateSimpleDatePartStatistics(vector<BaseStatistics> &child_stats) {
	// we can always propagate simple date part statistics
	// since the min and max can never exceed these bounds
	auto result = NumericStats::CreateEmpty(LogicalType::BIGINT);
	result.CopyValidity(child_stats[0]);
	NumericStats::SetMin(result, Value::BIGINT(MIN));
	NumericStats::SetMax(result, Value::BIGINT(MAX));
	return result.ToUnique();
}

struct DatePart {
	template <class T, class OP, class TR = int64_t>
	static unique_ptr<BaseStatistics> PropagateDatePartStatistics(vector<BaseStatistics> &child_stats,
	                                                              const LogicalType &stats_type = LogicalType::BIGINT) {
		// we can only propagate complex date part stats if the child has stats
		auto &nstats = child_stats[0];
		if (!NumericStats::HasMinMax(nstats)) {
			return nullptr;
		}
		// run the operator on both the min and the max, this gives us the [min, max] bound
		auto min = NumericStats::GetMin<T>(nstats);
		auto max = NumericStats::GetMax<T>(nstats);
		if (min > max) {
			return nullptr;
		}
		// Infinities prevent us from computing generic ranges
		if (!Value::IsFinite(min) || !Value::IsFinite(max)) {
			return nullptr;
		}
		TR min_part = OP::template Operation<T, TR>(min);
		TR max_part = OP::template Operation<T, TR>(max);
		auto result = NumericStats::CreateEmpty(stats_type);
		NumericStats::SetMin(result, Value(min_part));
		NumericStats::SetMax(result, Value(max_part));
		result.CopyValidity(child_stats[0]);
		return result.ToUnique();
	}

	template <typename OP>
	struct PartOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input, ValidityMask &mask, idx_t idx, void *dataptr) {
			if (Value::IsFinite(input)) {
				return OP::template Operation<TA, TR>(input);
			} else {
				mask.SetInvalid(idx);
				return TR();
			}
		}
	};

	template <class TA, class TR, class OP>
	static void UnaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
		D_ASSERT(input.ColumnCount() >= 1);
		using IOP = PartOperator<OP>;
		UnaryExecutor::GenericExecute<TA, TR, IOP>(input.data[0], result, input.size(), nullptr, true);
	}

	struct YearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractYear(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, YearOperator>(input.child_stats);
		}
	};

	struct MonthOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractMonth(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of month operator is [1, 12]
			return PropagateSimpleDatePartStatistics<1, 12>(input.child_stats);
		}
	};

	struct DayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractDay(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of day operator is [1, 31]
			return PropagateSimpleDatePartStatistics<1, 31>(input.child_stats);
		}
	};

	struct DecadeOperator {
		// From the PG docs: "The year field divided by 10"
		template <typename TR>
		static inline TR DecadeFromYear(TR yyyy) {
			return yyyy / 10;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return DecadeFromYear(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, DecadeOperator>(input.child_stats);
		}
	};

	struct CenturyOperator {
		// From the PG docs:
		// "The first century starts at 0001-01-01 00:00:00 AD, although they did not know it at the time.
		// This definition applies to all Gregorian calendar countries.
		// There is no century number 0, you go from -1 century to 1 century.
		// If you disagree with this, please write your complaint to: Pope, Cathedral Saint-Peter of Roma, Vatican."
		// (To be fair, His Holiness had nothing to do with this -
		// it was the lack of zero in the counting systems of the time...)
		template <typename TR>
		static inline TR CenturyFromYear(TR yyyy) {
			if (yyyy > 0) {
				return ((yyyy - 1) / 100) + 1;
			} else {
				return (yyyy / 100) - 1;
			}
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return CenturyFromYear(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, CenturyOperator>(input.child_stats);
		}
	};

	struct MillenniumOperator {
		// See the century comment
		template <typename TR>
		static inline TR MillenniumFromYear(TR yyyy) {
			if (yyyy > 0) {
				return ((yyyy - 1) / 1000) + 1;
			} else {
				return (yyyy / 1000) - 1;
			}
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return MillenniumFromYear<TR>(YearOperator::Operation<TA, TR>(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, MillenniumOperator>(input.child_stats);
		}
	};

	struct QuarterOperator {
		template <class TR>
		static inline TR QuarterFromMonth(TR mm) {
			return (mm - 1) / Interval::MONTHS_PER_QUARTER + 1;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return QuarterFromMonth(Date::ExtractMonth(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			// min/max of quarter operator is [1, 4]
			return PropagateSimpleDatePartStatistics<1, 4>(input.child_stats);
		}
	};

	struct DayOfWeekOperator {
		template <class TR>
		static inline TR DayOfWeekFromISO(TR isodow) {
			// day of the week (Sunday = 0, Saturday = 6)
			// turn sunday into 0 by doing mod 7
			return isodow % 7;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return DayOfWeekFromISO(Date::ExtractISODayOfTheWeek(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 6>(input.child_stats);
		}
	};

	struct ISODayOfWeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			// isodow (Monday = 1, Sunday = 7)
			return Date::ExtractISODayOfTheWeek(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 7>(input.child_stats);
		}
	};

	struct DayOfYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractDayOfTheYear(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 366>(input.child_stats);
		}
	};

	struct WeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractISOWeekNumber(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<1, 54>(input.child_stats);
		}
	};

	struct ISOYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::ExtractISOYearNumber(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, ISOYearOperator>(input.child_stats);
		}
	};

	struct YearWeekOperator {
		template <class TR>
		static inline TR YearWeekFromParts(TR yyyy, TR ww) {
			return yyyy * 100 + ((yyyy > 0) ? ww : -ww);
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t yyyy, ww;
			Date::ExtractISOYearWeek(input, yyyy, ww);
			return YearWeekFromParts(yyyy, ww);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, YearWeekOperator>(input.child_stats);
		}
	};

	struct EpochNanosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros * Interval::NANOS_PER_MICRO;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochNanosecondsOperator>(input.child_stats);
		}
	};

	struct EpochMicrosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochMicrosecondsOperator>(input.child_stats);
		}
	};

	struct EpochMillisOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input.micros / Interval::MICROS_PER_MSEC;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochMillisOperator>(input.child_stats);
		}

		static void Inverse(DataChunk &input, ExpressionState &state, Vector &result) {
			D_ASSERT(input.ColumnCount() == 1);

			UnaryExecutor::Execute<int64_t, timestamp_t>(input.data[0], result, input.size(),
			                                             [&](int64_t input) { return Timestamp::FromEpochMs(input); });
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60000000>(input.child_stats);
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60000>(input.child_stats);
		}
	};

	struct SecondsOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60>(input.child_stats);
		}
	};

	struct MinutesOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 60>(input.child_stats);
		}
	};

	struct HoursOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 24>(input.child_stats);
		}
	};

	struct EpochOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::Epoch(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, EpochOperator, double>(input.child_stats, LogicalType::DOUBLE);
		}
	};

	struct EraOperator {
		template <class TR>
		static inline TR EraFromYear(TR yyyy) {
			return yyyy > 0 ? 1 : 0;
		}

		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return EraFromYear(Date::ExtractYear(input));
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 1>(input.child_stats);
		}
	};

	struct TimezoneOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			// Regular timestamps are UTC.
			return 0;
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateSimpleDatePartStatistics<0, 0>(input.child_stats);
		}
	};

	struct JulianDayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Timestamp::GetJulianDay(input);
		}

		template <class T>
		static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, FunctionStatisticsInput &input) {
			return PropagateDatePartStatistics<T, JulianDayOperator, double>(input.child_stats, LogicalType::DOUBLE);
		}
	};

	// These are all zero and have the same restrictions
	using TimezoneHourOperator = TimezoneOperator;
	using TimezoneMinuteOperator = TimezoneOperator;

	struct StructOperator {
		using part_codes_t = vector<DatePartSpecifier>;
		using part_mask_t = uint64_t;

		enum MaskBits : uint8_t {
			YMD = 1 << 0,
			DOW = 1 << 1,
			DOY = 1 << 2,
			EPOCH = 1 << 3,
			TIME = 1 << 4,
			ZONE = 1 << 5,
			ISO = 1 << 6,
			JD = 1 << 7
		};

		static part_mask_t GetMask(const part_codes_t &part_codes) {
			part_mask_t mask = 0;
			for (const auto &part_code : part_codes) {
				switch (part_code) {
				case DatePartSpecifier::YEAR:
				case DatePartSpecifier::MONTH:
				case DatePartSpecifier::DAY:
				case DatePartSpecifier::DECADE:
				case DatePartSpecifier::CENTURY:
				case DatePartSpecifier::MILLENNIUM:
				case DatePartSpecifier::QUARTER:
				case DatePartSpecifier::ERA:
					mask |= YMD;
					break;
				case DatePartSpecifier::YEARWEEK:
				case DatePartSpecifier::WEEK:
				case DatePartSpecifier::ISOYEAR:
					mask |= ISO;
					break;
				case DatePartSpecifier::DOW:
				case DatePartSpecifier::ISODOW:
					mask |= DOW;
					break;
				case DatePartSpecifier::DOY:
					mask |= DOY;
					break;
				case DatePartSpecifier::EPOCH:
					mask |= EPOCH;
					break;
				case DatePartSpecifier::JULIAN_DAY:
					mask |= JD;
					break;
				case DatePartSpecifier::MICROSECONDS:
				case DatePartSpecifier::MILLISECONDS:
				case DatePartSpecifier::SECOND:
				case DatePartSpecifier::MINUTE:
				case DatePartSpecifier::HOUR:
					mask |= TIME;
					break;
				case DatePartSpecifier::TIMEZONE:
				case DatePartSpecifier::TIMEZONE_HOUR:
				case DatePartSpecifier::TIMEZONE_MINUTE:
					mask |= ZONE;
					break;
				case DatePartSpecifier::INVALID:
					throw InternalException("Invalid DatePartSpecifier for STRUCT mask!");
				}
			}
			return mask;
		}

		template <typename P>
		static inline P HasPartValue(vector<P> part_values, DatePartSpecifier part) {
			auto idx = size_t(part);
			if (IsBigintDatepart(part)) {
				return part_values[idx - size_t(DatePartSpecifier::BEGIN_BIGINT)];
			} else {
				return part_values[idx - size_t(DatePartSpecifier::BEGIN_DOUBLE)];
			}
		}

		using bigint_vec = vector<int64_t *>;
		using double_vec = vector<double *>;

		template <class TA>
		static inline void Operation(bigint_vec &bigint_values, double_vec &double_values, const TA &input,
		                             const idx_t idx, const part_mask_t mask) {
			int64_t *bigint_data;
			// YMD calculations
			int32_t yyyy = 1970;
			int32_t mm = 0;
			int32_t dd = 1;
			if (mask & YMD) {
				Date::Convert(input, yyyy, mm, dd);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::YEAR);
				if (bigint_data) {
					bigint_data[idx] = yyyy;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::MONTH);
				if (bigint_data) {
					bigint_data[idx] = mm;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DAY);
				if (bigint_data) {
					bigint_data[idx] = dd;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DECADE);
				if (bigint_data) {
					bigint_data[idx] = DecadeOperator::DecadeFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::CENTURY);
				if (bigint_data) {
					bigint_data[idx] = CenturyOperator::CenturyFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::MILLENNIUM);
				if (bigint_data) {
					bigint_data[idx] = MillenniumOperator::MillenniumFromYear(yyyy);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::QUARTER);
				if (bigint_data) {
					bigint_data[idx] = QuarterOperator::QuarterFromMonth(mm);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ERA);
				if (bigint_data) {
					bigint_data[idx] = EraOperator::EraFromYear(yyyy);
				}
			}

			// Week calculations
			if (mask & DOW) {
				auto isodow = Date::ExtractISODayOfTheWeek(input);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DOW);
				if (bigint_data) {
					bigint_data[idx] = DayOfWeekOperator::DayOfWeekFromISO(isodow);
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ISODOW);
				if (bigint_data) {
					bigint_data[idx] = isodow;
				}
			}

			// ISO calculations
			if (mask & ISO) {
				int32_t ww = 0;
				int32_t iyyy = 0;
				Date::ExtractISOYearWeek(input, iyyy, ww);
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::WEEK);
				if (bigint_data) {
					bigint_data[idx] = ww;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::ISOYEAR);
				if (bigint_data) {
					bigint_data[idx] = iyyy;
				}
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::YEARWEEK);
				if (bigint_data) {
					bigint_data[idx] = YearWeekOperator::YearWeekFromParts(iyyy, ww);
				}
			}

			if (mask & EPOCH) {
				auto double_data = HasPartValue(double_values, DatePartSpecifier::EPOCH);
				if (double_data) {
					double_data[idx] = Date::Epoch(input);
				}
			}
			if (mask & DOY) {
				bigint_data = HasPartValue(bigint_values, DatePartSpecifier::DOY);
				if (bigint_data) {
					bigint_data[idx] = Date::ExtractDayOfTheYear(input);
				}
			}
			if (mask & JD) {
				auto double_data = HasPartValue(double_values, DatePartSpecifier::JULIAN_DAY);
				if (double_data) {
					double_data[idx] = Date::ExtractJulianDay(input);
				}
			}
		}
	};
};

template <class T>
static void LastYearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	int32_t last_year = 0;
	UnaryExecutor::ExecuteWithNulls<T, int64_t>(args.data[0], result, args.size(),
	                                            [&](T input, ValidityMask &mask, idx_t idx) {
		                                            if (Value::IsFinite(input)) {
			                                            return Date::ExtractYear(input, &last_year);
		                                            } else {
			                                            mask.SetInvalid(idx);
			                                            return 0;
		                                            }
	                                            });
}

template <>
int64_t DatePart::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::YearOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::YearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DatePart::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::MonthOperator::Operation(interval_t input) {
	return input.months % Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::MonthOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DatePart::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOperator::Operation(interval_t input) {
	return input.days;
}

template <>
int64_t DatePart::DayOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DatePart::DecadeOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_DECADE;
}

template <>
int64_t DatePart::DecadeOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DatePart::CenturyOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_CENTURY;
}

template <>
int64_t DatePart::CenturyOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DatePart::MillenniumOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_MILLENIUM;
}

template <>
int64_t DatePart::MillenniumOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DatePart::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::QuarterOperator::Operation(interval_t input) {
	return MonthOperator::Operation<interval_t, int64_t>(input) / Interval::MONTHS_PER_QUARTER + 1;
}

template <>
int64_t DatePart::QuarterOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"dow\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"dow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isodow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"isodow\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"doy\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"doy\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::WeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"week\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DatePart::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::ISOYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isoyear\" not recognized");
}

template <>
int64_t DatePart::ISOYearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"isoyear\" not recognized");
}

template <>
int64_t DatePart::YearWeekOperator::Operation(timestamp_t input) {
	return YearWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::YearWeekOperator::Operation(interval_t input) {
	const auto yyyy = YearOperator::Operation<interval_t, int64_t>(input);
	const auto ww = WeekOperator::Operation<interval_t, int64_t>(input);
	return YearWeekOperator::YearWeekFromParts<int64_t>(yyyy, ww);
}

template <>
int64_t DatePart::YearWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"yearweek\" not recognized");
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochNanoSeconds(input);
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(date_t input) {
	return Date::EpochNanoseconds(input);
}

template <>
int64_t DatePart::EpochNanosecondsOperator::Operation(interval_t input) {
	return Interval::GetNanoseconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochMicroSeconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(date_t input) {
	return Date::EpochMicroseconds(input);
}

template <>
int64_t DatePart::EpochMicrosecondsOperator::Operation(interval_t input) {
	return Interval::GetMicro(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochMs(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(date_t input) {
	return Date::EpochMilliseconds(input);
}

template <>
int64_t DatePart::EpochMillisOperator::Operation(interval_t input) {
	return Interval::GetMilli(input);
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove everything but the second & microsecond part
	return time.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(interval_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(dtime_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::MinutesOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove the hour part, and truncate to minutes
	return (time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(interval_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(dtime_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetTime(input).micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(interval_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(dtime_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
double DatePart::EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochMicroSeconds(input) / double(Interval::MICROS_PER_SEC);
}

template <>
double DatePart::EpochOperator::Operation(interval_t input) {
	int64_t interval_years = input.months / Interval::MONTHS_PER_YEAR;
	int64_t interval_days;
	interval_days = Interval::DAYS_PER_YEAR * interval_years;
	interval_days += Interval::DAYS_PER_MONTH * (input.months % Interval::MONTHS_PER_YEAR);
	interval_days += input.days;
	int64_t interval_epoch;
	interval_epoch = interval_days * Interval::SECS_PER_DAY;
	// we add 0.25 days per year to sort of account for leap days
	interval_epoch += interval_years * (Interval::SECS_PER_DAY / 4);
	return interval_epoch + input.micros / double(Interval::MICROS_PER_SEC);
}

//	TODO: We can't propagate interval statistics because we can't easily compare interval_t for order.
template <>
unique_ptr<BaseStatistics> DatePart::EpochOperator::PropagateStatistics<interval_t>(ClientContext &context,
                                                                                    FunctionStatisticsInput &input) {
	return nullptr;
}

template <>
double DatePart::EpochOperator::Operation(dtime_t input) {
	return input.micros / double(Interval::MICROS_PER_SEC);
}

template <>
unique_ptr<BaseStatistics> DatePart::EpochOperator::PropagateStatistics<dtime_t>(ClientContext &context,
                                                                                 FunctionStatisticsInput &input) {
	auto result = NumericStats::CreateEmpty(LogicalType::DOUBLE);
	result.CopyValidity(input.child_stats[0]);
	NumericStats::SetMin(result, Value::DOUBLE(0));
	NumericStats::SetMax(result, Value::DOUBLE(Interval::SECS_PER_DAY));
	return result.ToUnique();
}

template <>
int64_t DatePart::EraOperator::Operation(timestamp_t input) {
	return EraOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::EraOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"era\" not recognized");
}

template <>
int64_t DatePart::EraOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"era\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(date_t input) {
	throw NotImplementedException("\"date\" units \"timezone\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(interval_t input) {
	throw NotImplementedException("\"interval\" units \"timezone\" not recognized");
}

template <>
int64_t DatePart::TimezoneOperator::Operation(dtime_t input) {
	return 0;
}

template <>
double DatePart::JulianDayOperator::Operation(date_t input) {
	return Date::ExtractJulianDay(input);
}

template <>
double DatePart::JulianDayOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"julian\" not recognized");
}

template <>
double DatePart::JulianDayOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"julian\" not recognized");
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const dtime_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	int64_t *part_data;
	if (mask & TIME) {
		const auto micros = MicrosecondsOperator::Operation<dtime_t, int64_t>(input);
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MICROSECONDS);
		if (part_data) {
			part_data[idx] = micros;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLISECONDS);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_MSEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::SECOND);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_SEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MINUTE);
		if (part_data) {
			part_data[idx] = MinutesOperator::Operation<dtime_t, int64_t>(input);
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::HOUR);
		if (part_data) {
			part_data[idx] = HoursOperator::Operation<dtime_t, int64_t>(input);
		}
	}

	if (mask & EPOCH) {
		auto part_data = HasPartValue(double_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<dtime_t, double>(input);
			;
		}
	}

	if (mask & ZONE) {
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE);
		if (part_data) {
			part_data[idx] = 0;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE_HOUR);
		if (part_data) {
			part_data[idx] = 0;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::TIMEZONE_MINUTE);
		if (part_data) {
			part_data[idx] = 0;
		}
	}
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const timestamp_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	date_t d;
	dtime_t t;
	Timestamp::Convert(input, d, t);

	// Both define epoch, and the correct value is the sum.
	// So mask it out and compute it separately.
	Operation(bigint_values, double_values, d, idx, mask & ~EPOCH);
	Operation(bigint_values, double_values, t, idx, mask & ~EPOCH);

	if (mask & EPOCH) {
		auto part_data = HasPartValue(double_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<timestamp_t, double>(input);
		}
	}

	if (mask & JD) {
		auto part_data = HasPartValue(double_values, DatePartSpecifier::JULIAN_DAY);
		if (part_data) {
			part_data[idx] = JulianDayOperator::Operation<timestamp_t, double>(input);
		}
	}
}

template <>
void DatePart::StructOperator::Operation(bigint_vec &bigint_values, double_vec &double_values, const interval_t &input,
                                         const idx_t idx, const part_mask_t mask) {
	int64_t *part_data;
	if (mask & YMD) {
		const auto mm = input.months % Interval::MONTHS_PER_YEAR;
		part_data = HasPartValue(bigint_values, DatePartSpecifier::YEAR);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_YEAR;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MONTH);
		if (part_data) {
			part_data[idx] = mm;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::DAY);
		if (part_data) {
			part_data[idx] = input.days;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::DECADE);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_DECADE;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::CENTURY);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_CENTURY;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLENNIUM);
		if (part_data) {
			part_data[idx] = input.months / Interval::MONTHS_PER_MILLENIUM;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::QUARTER);
		if (part_data) {
			part_data[idx] = mm / Interval::MONTHS_PER_QUARTER + 1;
		}
	}

	if (mask & TIME) {
		const auto micros = MicrosecondsOperator::Operation<interval_t, int64_t>(input);
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MICROSECONDS);
		if (part_data) {
			part_data[idx] = micros;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MILLISECONDS);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_MSEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::SECOND);
		if (part_data) {
			part_data[idx] = micros / Interval::MICROS_PER_SEC;
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::MINUTE);
		if (part_data) {
			part_data[idx] = MinutesOperator::Operation<interval_t, int64_t>(input);
		}
		part_data = HasPartValue(bigint_values, DatePartSpecifier::HOUR);
		if (part_data) {
			part_data[idx] = HoursOperator::Operation<interval_t, int64_t>(input);
		}
	}

	if (mask & EPOCH) {
		auto part_data = HasPartValue(double_values, DatePartSpecifier::EPOCH);
		if (part_data) {
			part_data[idx] = EpochOperator::Operation<interval_t, double>(input);
		}
	}
}

template <typename T>
static int64_t ExtractElement(DatePartSpecifier type, T element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return DatePart::YearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MONTH:
		return DatePart::MonthOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DAY:
		return DatePart::DayOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DECADE:
		return DatePart::DecadeOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::CENTURY:
		return DatePart::CenturyOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLENNIUM:
		return DatePart::MillenniumOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::QUARTER:
		return DatePart::QuarterOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOW:
		return DatePart::DayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ISODOW:
		return DatePart::ISODayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOY:
		return DatePart::DayOfYearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::WEEK:
		return DatePart::WeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ISOYEAR:
		return DatePart::ISOYearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::YEARWEEK:
		return DatePart::YearWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MICROSECONDS:
		return DatePart::MicrosecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLISECONDS:
		return DatePart::MillisecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::SECOND:
		return DatePart::SecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MINUTE:
		return DatePart::MinutesOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::HOUR:
		return DatePart::HoursOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ERA:
		return DatePart::EraOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::TIMEZONE:
	case DatePartSpecifier::TIMEZONE_HOUR:
	case DatePartSpecifier::TIMEZONE_MINUTE:
		return DatePart::TimezoneOperator::template Operation<T, int64_t>(element);
	default:
		throw NotImplementedException("Specifier type not implemented for DATEPART");
	}
}

template <typename T>
static void DatePartFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &spec_arg = args.data[0];
	auto &date_arg = args.data[1];

	BinaryExecutor::ExecuteWithNulls<string_t, T, int64_t>(
	    spec_arg, date_arg, result, args.size(), [&](string_t specifier, T date, ValidityMask &mask, idx_t idx) {
		    if (Value::IsFinite(date)) {
			    return ExtractElement<T>(GetDatePartSpecifier(specifier.GetString()), date);
		    } else {
			    mask.SetInvalid(idx);
			    return int64_t(0);
		    }
	    });
}

static unique_ptr<FunctionData> DatePartBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	//	If we are only looking for Julian Days for timestamps,
	//	then return doubles.
	if (arguments[0]->HasParameter() || !arguments[0]->IsFoldable()) {
		return nullptr;
	}

	Value part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	const auto part_name = part_value.ToString();
	switch (GetDatePartSpecifier(part_name)) {
	case DatePartSpecifier::JULIAN_DAY:
		arguments.erase(arguments.begin());
		bound_function.arguments.erase(bound_function.arguments.begin());
		bound_function.name = "julian";
		bound_function.return_type = LogicalType::DOUBLE;
		switch (arguments[0]->return_type.id()) {
		case LogicalType::TIMESTAMP:
			bound_function.function = DatePart::UnaryFunction<timestamp_t, double, DatePart::JulianDayOperator>;
			bound_function.statistics = DatePart::JulianDayOperator::template PropagateStatistics<timestamp_t>;
			break;
		case LogicalType::DATE:
			bound_function.function = DatePart::UnaryFunction<date_t, double, DatePart::JulianDayOperator>;
			bound_function.statistics = DatePart::JulianDayOperator::template PropagateStatistics<date_t>;
			break;
		default:
			throw BinderException("%s can only take DATE or TIMESTAMP arguments", bound_function.name);
		}
		break;
	case DatePartSpecifier::EPOCH:
		arguments.erase(arguments.begin());
		bound_function.arguments.erase(bound_function.arguments.begin());
		bound_function.name = "epoch";
		bound_function.return_type = LogicalType::DOUBLE;
		switch (arguments[0]->return_type.id()) {
		case LogicalType::TIMESTAMP:
			bound_function.function = DatePart::UnaryFunction<timestamp_t, double, DatePart::EpochOperator>;
			bound_function.statistics = DatePart::EpochOperator::template PropagateStatistics<timestamp_t>;
			break;
		case LogicalType::DATE:
			bound_function.function = DatePart::UnaryFunction<date_t, double, DatePart::EpochOperator>;
			bound_function.statistics = DatePart::EpochOperator::template PropagateStatistics<date_t>;
			break;
		case LogicalType::INTERVAL:
			bound_function.function = DatePart::UnaryFunction<interval_t, double, DatePart::EpochOperator>;
			bound_function.statistics = DatePart::EpochOperator::template PropagateStatistics<interval_t>;
			break;
		case LogicalType::TIME:
			bound_function.function = DatePart::UnaryFunction<dtime_t, double, DatePart::EpochOperator>;
			bound_function.statistics = DatePart::EpochOperator::template PropagateStatistics<dtime_t>;
			break;
		default:
			throw BinderException("%s can only take temporal arguments", bound_function.name);
		}
		break;
	default:
		break;
	}

	return nullptr;
}

ScalarFunctionSet GetGenericDatePartFunction(scalar_function_t date_func, scalar_function_t ts_func,
                                             scalar_function_t interval_func, function_statistics_t date_stats,
                                             function_statistics_t ts_stats) {
	ScalarFunctionSet operator_set;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, std::move(date_func), nullptr, nullptr, date_stats));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, std::move(ts_func), nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, LogicalType::BIGINT, std::move(interval_func)));
	return operator_set;
}

template <class OP>
static ScalarFunctionSet GetDatePartFunction() {
	return GetGenericDatePartFunction(
	    DatePart::UnaryFunction<date_t, int64_t, OP>, DatePart::UnaryFunction<timestamp_t, int64_t, OP>,
	    ScalarFunction::UnaryFunction<interval_t, int64_t, OP>, OP::template PropagateStatistics<date_t>,
	    OP::template PropagateStatistics<timestamp_t>);
}

ScalarFunctionSet GetGenericTimePartFunction(const LogicalType &result_type, scalar_function_t date_func,
                                             scalar_function_t ts_func, scalar_function_t interval_func,
                                             scalar_function_t time_func, function_statistics_t date_stats,
                                             function_statistics_t ts_stats, function_statistics_t time_stats) {
	ScalarFunctionSet operator_set;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, result_type, std::move(date_func), nullptr, nullptr, date_stats));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, result_type, std::move(ts_func), nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, result_type, std::move(interval_func)));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIME}, result_type, std::move(time_func), nullptr, nullptr, time_stats));
	return operator_set;
}

template <class OP, class TR = int64_t>
static ScalarFunctionSet GetTimePartFunction(const LogicalType &result_type = LogicalType::BIGINT) {
	return GetGenericTimePartFunction(
	    result_type, DatePart::UnaryFunction<date_t, TR, OP>, DatePart::UnaryFunction<timestamp_t, TR, OP>,
	    ScalarFunction::UnaryFunction<interval_t, TR, OP>, ScalarFunction::UnaryFunction<dtime_t, TR, OP>,
	    OP::template PropagateStatistics<date_t>, OP::template PropagateStatistics<timestamp_t>,
	    OP::template PropagateStatistics<dtime_t>);
}

struct LastDayOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		int32_t yyyy, mm, dd;
		Date::Convert(input, yyyy, mm, dd);
		yyyy += (mm / 12);
		mm %= 12;
		++mm;
		return Date::FromDate(yyyy, mm, 1) - 1;
	}
};

template <>
date_t LastDayOperator::Operation(timestamp_t input) {
	return LastDayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

struct MonthNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::MONTH_NAMES[DatePart::MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

struct DayNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::DAY_NAMES[DatePart::DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

struct StructDatePart {
	using part_codes_t = vector<DatePartSpecifier>;

	struct BindData : public VariableReturnBindData {
		part_codes_t part_codes;

		explicit BindData(const LogicalType &stype, const part_codes_t &part_codes_p)
		    : VariableReturnBindData(stype), part_codes(part_codes_p) {
		}

		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BindData>(stype, part_codes);
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		// collect names and deconflict, construct return type
		if (arguments[0]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[0]->IsFoldable()) {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		case_insensitive_set_t name_collision_set;
		child_list_t<LogicalType> struct_children;
		part_codes_t part_codes;

		Value parts_list = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
		if (parts_list.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(parts_list);
			if (list_children.empty()) {
				throw BinderException("%s requires non-empty lists of part names", bound_function.name);
			}
			for (const auto &part_value : list_children) {
				if (part_value.IsNull()) {
					throw BinderException("NULL struct entry name in %s", bound_function.name);
				}
				const auto part_name = part_value.ToString();
				const auto part_code = GetDateTypePartSpecifier(part_name, arguments[1]->return_type);
				if (name_collision_set.find(part_name) != name_collision_set.end()) {
					throw BinderException("Duplicate struct entry name \"%s\" in %s", part_name, bound_function.name);
				}
				name_collision_set.insert(part_name);
				part_codes.emplace_back(part_code);
				const auto part_type = IsBigintDatepart(part_code) ? LogicalType::BIGINT : LogicalType::DOUBLE;
				struct_children.emplace_back(make_pair(part_name, part_type));
			}
		} else {
			throw BinderException("%s can only take constant lists of part names", bound_function.name);
		}

		Function::EraseArgument(bound_function, arguments, 0);
		bound_function.return_type = LogicalType::STRUCT(struct_children);
		return make_uniq<BindData>(bound_function.return_type, part_codes);
	}

	template <typename INPUT_TYPE>
	static void Function(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		D_ASSERT(args.ColumnCount() == 1);

		const auto count = args.size();
		Vector &input = args.data[0];

		//	Type counts
		const auto BIGINT_COUNT = size_t(DatePartSpecifier::BEGIN_DOUBLE) - size_t(DatePartSpecifier::BEGIN_BIGINT);
		const auto DOUBLE_COUNT = size_t(DatePartSpecifier::BEGIN_INVALID) - size_t(DatePartSpecifier::BEGIN_DOUBLE);
		DatePart::StructOperator::bigint_vec bigint_values(BIGINT_COUNT, nullptr);
		DatePart::StructOperator::double_vec double_values(DOUBLE_COUNT, nullptr);
		const auto part_mask = DatePart::StructOperator::GetMask(info.part_codes);

		auto &child_entries = StructVector::GetEntries(result);

		// The first computer of a part "owns" it
		// and other requestors just reference the owner
		vector<size_t> owners(int(DatePartSpecifier::JULIAN_DAY) + 1, child_entries.size());
		for (size_t col = 0; col < child_entries.size(); ++col) {
			const auto part_index = size_t(info.part_codes[col]);
			if (owners[part_index] == child_entries.size()) {
				owners[part_index] = col;
			}
		}

		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			if (ConstantVector::IsNull(input)) {
				ConstantVector::SetNull(result, true);
			} else {
				ConstantVector::SetNull(result, false);
				for (size_t col = 0; col < child_entries.size(); ++col) {
					auto &child_entry = child_entries[col];
					ConstantVector::SetNull(*child_entry, false);
					const auto part_index = size_t(info.part_codes[col]);
					if (owners[part_index] == col) {
						if (IsBigintDatepart(info.part_codes[col])) {
							bigint_values[part_index - size_t(DatePartSpecifier::BEGIN_BIGINT)] =
							    ConstantVector::GetData<int64_t>(*child_entry);
						} else {
							double_values[part_index - size_t(DatePartSpecifier::BEGIN_DOUBLE)] =
							    ConstantVector::GetData<double>(*child_entry);
						}
					}
				}
				auto tdata = ConstantVector::GetData<INPUT_TYPE>(input);
				if (Value::IsFinite(tdata[0])) {
					DatePart::StructOperator::Operation(bigint_values, double_values, tdata[0], 0, part_mask);
				} else {
					for (auto &child_entry : child_entries) {
						ConstantVector::SetNull(*child_entry, true);
					}
				}
			}
		} else {
			UnifiedVectorFormat rdata;
			input.ToUnifiedFormat(count, rdata);

			const auto &arg_valid = rdata.validity;
			auto tdata = UnifiedVectorFormat::GetData<INPUT_TYPE>(rdata);

			// Start with a valid flat vector
			result.SetVectorType(VectorType::FLAT_VECTOR);
			auto &res_valid = FlatVector::Validity(result);
			if (res_valid.GetData()) {
				res_valid.SetAllValid(count);
			}

			// Start with valid children
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				child_entry->SetVectorType(VectorType::FLAT_VECTOR);
				auto &child_validity = FlatVector::Validity(*child_entry);
				if (child_validity.GetData()) {
					child_validity.SetAllValid(count);
				}

				// Pre-multiplex
				const auto part_index = size_t(info.part_codes[col]);
				if (owners[part_index] == col) {
					if (IsBigintDatepart(info.part_codes[col])) {
						bigint_values[part_index - size_t(DatePartSpecifier::BEGIN_BIGINT)] =
						    FlatVector::GetData<int64_t>(*child_entry);
					} else {
						double_values[part_index - size_t(DatePartSpecifier::BEGIN_DOUBLE)] =
						    FlatVector::GetData<double>(*child_entry);
					}
				}
			}

			for (idx_t i = 0; i < count; ++i) {
				const auto idx = rdata.sel->get_index(i);
				if (arg_valid.RowIsValid(idx)) {
					if (Value::IsFinite(tdata[idx])) {
						DatePart::StructOperator::Operation(bigint_values, double_values, tdata[idx], i, part_mask);
					} else {
						for (auto &child_entry : child_entries) {
							FlatVector::Validity(*child_entry).SetInvalid(i);
						}
					}
				} else {
					res_valid.SetInvalid(i);
					for (auto &child_entry : child_entries) {
						FlatVector::Validity(*child_entry).SetInvalid(i);
					}
				}
			}
		}

		// Reference any duplicate parts
		for (size_t col = 0; col < child_entries.size(); ++col) {
			const auto part_index = size_t(info.part_codes[col]);
			const auto owner = owners[part_index];
			if (owner != col) {
				child_entries[col]->Reference(*child_entries[owner]);
			}
		}

		result.Verify(count);
	}

	static void SerializeFunction(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                              const ScalarFunction &function) {
		D_ASSERT(bind_data_p);
		auto &info = bind_data_p->Cast<BindData>();
		serializer.WriteProperty(100, "stype", info.stype);
		serializer.WriteProperty(101, "part_codes", info.part_codes);
	}

	static unique_ptr<FunctionData> DeserializeFunction(Deserializer &deserializer, ScalarFunction &bound_function) {
		auto stype = deserializer.ReadProperty<LogicalType>(100, "stype");
		auto part_codes = deserializer.ReadProperty<vector<DatePartSpecifier>>(101, "part_codes");
		return make_uniq<BindData>(std::move(stype), std::move(part_codes));
	}

	template <typename INPUT_TYPE>
	static ScalarFunction GetFunction(const LogicalType &temporal_type) {
		auto part_type = LogicalType::LIST(LogicalType::VARCHAR);
		auto result_type = LogicalType::STRUCT({});
		ScalarFunction result({part_type, temporal_type}, result_type, Function<INPUT_TYPE>, Bind);
		result.serialize = SerializeFunction;
		result.deserialize = DeserializeFunction;
		return result;
	}
};

ScalarFunctionSet YearFun::GetFunctions() {
	return GetGenericDatePartFunction(LastYearFunction<date_t>, LastYearFunction<timestamp_t>,
	                                  ScalarFunction::UnaryFunction<interval_t, int64_t, DatePart::YearOperator>,
	                                  DatePart::YearOperator::PropagateStatistics<date_t>,
	                                  DatePart::YearOperator::PropagateStatistics<timestamp_t>);
}

ScalarFunctionSet MonthFun::GetFunctions() {
	return GetDatePartFunction<DatePart::MonthOperator>();
}

ScalarFunctionSet DayFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOperator>();
}

ScalarFunctionSet DecadeFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DecadeOperator>();
}

ScalarFunctionSet CenturyFun::GetFunctions() {
	return GetDatePartFunction<DatePart::CenturyOperator>();
}

ScalarFunctionSet MillenniumFun::GetFunctions() {
	return GetDatePartFunction<DatePart::MillenniumOperator>();
}

ScalarFunctionSet QuarterFun::GetFunctions() {
	return GetDatePartFunction<DatePart::QuarterOperator>();
}

ScalarFunctionSet DayOfWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfWeekOperator>();
}

ScalarFunctionSet ISODayOfWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::ISODayOfWeekOperator>();
}

ScalarFunctionSet DayOfYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfYearOperator>();
}

ScalarFunctionSet WeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::WeekOperator>();
}

ScalarFunctionSet ISOYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::ISOYearOperator>();
}

ScalarFunctionSet EraFun::GetFunctions() {
	return GetDatePartFunction<DatePart::EraOperator>();
}

ScalarFunctionSet TimezoneFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneOperator>();
}

ScalarFunctionSet TimezoneHourFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneHourOperator>();
}

ScalarFunctionSet TimezoneMinuteFun::GetFunctions() {
	return GetDatePartFunction<DatePart::TimezoneMinuteOperator>();
}

ScalarFunctionSet EpochFun::GetFunctions() {
	return GetTimePartFunction<DatePart::EpochOperator, double>(LogicalType::DOUBLE);
}

ScalarFunctionSet EpochNsFun::GetFunctions() {
	using OP = DatePart::EpochNanosecondsOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));
	return operator_set;
}

ScalarFunctionSet EpochUsFun::GetFunctions() {
	using OP = DatePart::EpochMicrosecondsOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));
	return operator_set;
}

ScalarFunctionSet EpochMsFun::GetFunctions() {
	using OP = DatePart::EpochMillisOperator;
	auto operator_set = GetTimePartFunction<OP>();

	//	TIMESTAMP WITH TIME ZONE has the same representation as TIMESTAMP so no need to defer to ICU
	auto tstz_func = DatePart::UnaryFunction<timestamp_t, int64_t, OP>;
	auto tstz_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, tstz_func, nullptr, nullptr, tstz_stats));

	//	Legacy inverse BIGINT => TIMESTAMP
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, DatePart::EpochMillisOperator::Inverse));

	return operator_set;
}

ScalarFunctionSet MicrosecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MicrosecondsOperator>();
}

ScalarFunctionSet MillisecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MillisecondsOperator>();
}

ScalarFunctionSet SecondsFun::GetFunctions() {
	return GetTimePartFunction<DatePart::SecondsOperator>();
}

ScalarFunctionSet MinutesFun::GetFunctions() {
	return GetTimePartFunction<DatePart::MinutesOperator>();
}

ScalarFunctionSet HoursFun::GetFunctions() {
	return GetTimePartFunction<DatePart::HoursOperator>();
}

ScalarFunctionSet YearWeekFun::GetFunctions() {
	return GetDatePartFunction<DatePart::YearWeekOperator>();
}

ScalarFunctionSet DayOfMonthFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOperator>();
}

ScalarFunctionSet WeekDayFun::GetFunctions() {
	return GetDatePartFunction<DatePart::DayOfWeekOperator>();
}

ScalarFunctionSet WeekOfYearFun::GetFunctions() {
	return GetDatePartFunction<DatePart::WeekOperator>();
}

ScalarFunctionSet LastDayFun::GetFunctions() {
	ScalarFunctionSet last_day;
	last_day.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::DATE,
	                                    DatePart::UnaryFunction<date_t, date_t, LastDayOperator>));
	last_day.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DATE,
	                                    DatePart::UnaryFunction<timestamp_t, date_t, LastDayOperator>));
	return last_day;
}

ScalarFunctionSet MonthNameFun::GetFunctions() {
	ScalarFunctionSet monthname;
	monthname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                     DatePart::UnaryFunction<date_t, string_t, MonthNameOperator>));
	monthname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                     DatePart::UnaryFunction<timestamp_t, string_t, MonthNameOperator>));
	return monthname;
}

ScalarFunctionSet DayNameFun::GetFunctions() {
	ScalarFunctionSet dayname;
	dayname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                   DatePart::UnaryFunction<date_t, string_t, DayNameOperator>));
	dayname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                   DatePart::UnaryFunction<timestamp_t, string_t, DayNameOperator>));
	return dayname;
}

ScalarFunctionSet JulianDayFun::GetFunctions() {
	using OP = DatePart::JulianDayOperator;

	ScalarFunctionSet operator_set;
	auto date_func = DatePart::UnaryFunction<date_t, double, OP>;
	auto date_stats = OP::template PropagateStatistics<date_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::DOUBLE, date_func, nullptr, nullptr, date_stats));
	auto ts_func = DatePart::UnaryFunction<timestamp_t, double, OP>;
	auto ts_stats = OP::template PropagateStatistics<timestamp_t>;
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DOUBLE, ts_func, nullptr, nullptr, ts_stats));

	return operator_set;
}

ScalarFunctionSet DatePartFun::GetFunctions() {
	ScalarFunctionSet date_part;
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::BIGINT,
	                                     DatePartFunction<date_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::BIGINT,
	                                     DatePartFunction<timestamp_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME}, LogicalType::BIGINT,
	                                     DatePartFunction<dtime_t>, DatePartBind));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::BIGINT,
	                                     DatePartFunction<interval_t>, DatePartBind));

	// struct variants
	date_part.AddFunction(StructDatePart::GetFunction<date_t>(LogicalType::DATE));
	date_part.AddFunction(StructDatePart::GetFunction<timestamp_t>(LogicalType::TIMESTAMP));
	date_part.AddFunction(StructDatePart::GetFunction<dtime_t>(LogicalType::TIME));
	date_part.AddFunction(StructDatePart::GetFunction<interval_t>(LogicalType::INTERVAL));

	return date_part;
}

} // namespace duckdb












namespace duckdb {

struct DateSub {
	static int64_t SubtractMicros(timestamp_t startdate, timestamp_t enddate) {
		const auto start = Timestamp::GetEpochMicroSeconds(startdate);
		const auto end = Timestamp::GetEpochMicroSeconds(enddate);
		return SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(end, start);
	}

	template <class TA, class TB, class TR, class OP>
	static inline void BinaryExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::ExecuteWithNulls<TA, TB, TR>(
		    left, right, result, count, [&](TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
			    if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
				    return OP::template Operation<TA, TB, TR>(startdate, enddate);
			    } else {
				    mask.SetInvalid(idx);
				    return TR();
			    }
		    });
	}

	struct MonthOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {

			if (start_ts > end_ts) {
				return -MonthOperator::Operation<TA, TB, TR>(end_ts, start_ts);
			}
			// The number of complete months depends on whether end_ts is on the last day of the month.
			date_t end_date;
			dtime_t end_time;
			Timestamp::Convert(end_ts, end_date, end_time);

			int32_t yyyy, mm, dd;
			Date::Convert(end_date, yyyy, mm, dd);
			const auto end_days = Date::MonthDays(yyyy, mm);
			if (end_days == dd) {
				// Now check whether the start day is after the end day
				date_t start_date;
				dtime_t start_time;
				Timestamp::Convert(start_ts, start_date, start_time);
				Date::Convert(start_date, yyyy, mm, dd);
				if (dd > end_days || (dd == end_days && start_time < end_time)) {
					// Move back to the same time on the last day of the (shorter) end month
					start_date = Date::FromDate(yyyy, mm, end_days);
					start_ts = Timestamp::FromDatetime(start_date, start_time);
				}
			}

			// Our interval difference will now give the correct result.
			// Note that PG gives different interval subtraction results,
			// so if we change this we will have to reimplement.
			return Interval::GetAge(end_ts, start_ts).months;
		}
	};

	struct QuarterOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_QUARTER;
		}
	};

	struct YearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_YEAR;
		}
	};

	struct DecadeOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_DECADE;
		}
	};

	struct CenturyOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_CENTURY;
		}
	};

	struct MilleniumOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_MILLENIUM;
		}
	};

	struct DayOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_DAY;
		}
	};

	struct WeekOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_WEEK;
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate);
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_MSEC;
		}
	};

	struct SecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_SEC;
		}
	};

	struct MinutesOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_MINUTE;
		}
	};

	struct HoursOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_HOUR;
		}
	};
};

// DATE specialisations
template <>
int64_t DateSub::YearOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return YearOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                  Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MonthOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MonthOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                   Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::DayOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return DayOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                 Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::DecadeOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return DecadeOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                    Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::CenturyOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return CenturyOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MilleniumOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MilleniumOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                       Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::QuarterOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return QuarterOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::WeekOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return WeekOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                  Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MicrosecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MicrosecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                          Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MillisecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MillisecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                          Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::SecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return SecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MinutesOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MinutesOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::HoursOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return HoursOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                   Timestamp::FromDatetime(enddate, t0));
}

// TIME specialisations
template <>
int64_t DateSub::YearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DateSub::MonthOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DateSub::DayOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DateSub::DecadeOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DateSub::CenturyOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DateSub::MilleniumOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DateSub::QuarterOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DateSub::WeekOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DateSub::MicrosecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros - startdate.micros;
}

template <>
int64_t DateSub::MillisecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DateSub::SecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DateSub::MinutesOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DateSub::HoursOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_HOUR;
}

template <typename TA, typename TB, typename TR>
static int64_t SubtractDateParts(DatePartSpecifier type, TA startdate, TB enddate) {
	switch (type) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::ISOYEAR:
		return DateSub::YearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MONTH:
		return DateSub::MonthOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return DateSub::DayOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DECADE:
		return DateSub::DecadeOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::CENTURY:
		return DateSub::CenturyOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLENNIUM:
		return DateSub::MilleniumOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::QUARTER:
		return DateSub::QuarterOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateSub::WeekOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MICROSECONDS:
		return DateSub::MicrosecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLISECONDS:
		return DateSub::MillisecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateSub::SecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MINUTE:
		return DateSub::MinutesOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::HOUR:
		return DateSub::HoursOperator::template Operation<TA, TB, TR>(startdate, enddate);
	default:
		throw NotImplementedException("Specifier type not implemented for DATESUB");
	}
}

struct DateSubTernaryOperator {
	template <typename TS, typename TA, typename TB, typename TR>
	static inline TR Operation(TS part, TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
		if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
			return SubtractDateParts<TA, TB, TR>(GetDatePartSpecifier(part.GetString()), startdate, enddate);
		} else {
			mask.SetInvalid(idx);
			return TR();
		}
	}
};

template <typename TA, typename TB, typename TR>
static void DateSubBinaryExecutor(DatePartSpecifier type, Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::ISOYEAR:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::YearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MONTH:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MonthOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::DayOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DECADE:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::DecadeOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::CenturyOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLENNIUM:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MilleniumOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::QuarterOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::WeekOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MicrosecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MillisecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::SecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MinutesOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::HOUR:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::HoursOperator>(left, right, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATESUB");
	}
}

template <typename T>
static void DateSubFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	auto &part_arg = args.data[0];
	auto &start_arg = args.data[1];
	auto &end_arg = args.data[2];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateSubBinaryExecutor<T, T, int64_t>(type, start_arg, end_arg, result, args.size());
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<string_t, T, T, int64_t>(
		    part_arg, start_arg, end_arg, result, args.size(),
		    DateSubTernaryOperator::Operation<string_t, T, T, int64_t>);
	}
}

ScalarFunctionSet DateSubFun::GetFunctions() {
	ScalarFunctionSet date_sub("date_sub");
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
	                                    LogicalType::BIGINT, DateSubFunction<date_t>));
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                    LogicalType::BIGINT, DateSubFunction<timestamp_t>));
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME, LogicalType::TIME},
	                                    LogicalType::BIGINT, DateSubFunction<dtime_t>));
	return date_sub;
}

} // namespace duckdb










namespace duckdb {

struct DateTrunc {
	template <class TA, class TR, class OP>
	static inline TR UnaryFunction(TA input) {
		if (Value::IsFinite(input)) {
			return OP::template Operation<TA, TR>(input);
		} else {
			return Cast::template Operation<TA, TR>(input);
		}
	}

	template <class TA, class TR, class OP>
	static inline void UnaryExecute(Vector &left, Vector &result, idx_t count) {
		UnaryExecutor::Execute<TA, TR>(left, result, count, UnaryFunction<TA, TR, OP>);
	}

	struct MillenniumOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 1000) * 1000, 1, 1);
		}
	};

	struct CenturyOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 100) * 100, 1, 1);
		}
	};

	struct DecadeOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 10) * 10, 1, 1);
		}
	};

	struct YearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate(Date::ExtractYear(input), 1, 1);
		}
	};

	struct QuarterOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t yyyy, mm, dd;
			Date::Convert(input, yyyy, mm, dd);
			mm = 1 + (((mm - 1) / 3) * 3);
			return Date::FromDate(yyyy, mm, 1);
		}
	};

	struct MonthOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate(Date::ExtractYear(input), Date::ExtractMonth(input), 1);
		}
	};

	struct WeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::GetMondayOfCurrentWeek(input);
		}
	};

	struct ISOYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Date::GetMondayOfCurrentWeek(input);
			date.days -= (Date::ExtractISOWeekNumber(date) - 1) * Interval::DAYS_PER_WEEK;

			return date;
		}
	};

	struct DayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input;
		}
	};

	struct HourOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, 0, 0, 0));
		}
	};

	struct MinuteOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, 0, 0));
		}
	};

	struct SecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, sec, 0));
		}
	};

	struct MillisecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			micros -= micros % Interval::MICROS_PER_MSEC;
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, sec, micros));
		}
	};

	struct MicrosecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input;
		}
	};
};

// DATE specialisations
template <>
date_t DateTrunc::MillenniumOperator::Operation(timestamp_t input) {
	return MillenniumOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::MillenniumOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(MillenniumOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::MillenniumOperator::Operation(timestamp_t input) {
	return MillenniumOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::CenturyOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(CenturyOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::DecadeOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(DecadeOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::YearOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(YearOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::QuarterOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(QuarterOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::MonthOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(MonthOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::WeekOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(WeekOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::ISOYearOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(ISOYearOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::DayOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(DayOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::HourOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::HourOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::HourOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(HourOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MinuteOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MinuteOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MinuteOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(HourOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::SecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::SecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::SecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(DayOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MillisecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MillisecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MillisecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(MillisecondOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MicrosecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MicrosecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MicrosecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(MicrosecondOperator::Operation<timestamp_t, timestamp_t>(input));
}

// INTERVAL specialisations
template <>
interval_t DateTrunc::MillenniumOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_MILLENIUM) * Interval::MONTHS_PER_MILLENIUM;
	return input;
}

template <>
interval_t DateTrunc::CenturyOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_CENTURY) * Interval::MONTHS_PER_CENTURY;
	return input;
}

template <>
interval_t DateTrunc::DecadeOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_DECADE) * Interval::MONTHS_PER_DECADE;
	return input;
}

template <>
interval_t DateTrunc::YearOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_YEAR) * Interval::MONTHS_PER_YEAR;
	return input;
}

template <>
interval_t DateTrunc::QuarterOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_QUARTER) * Interval::MONTHS_PER_QUARTER;
	return input;
}

template <>
interval_t DateTrunc::MonthOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	return input;
}

template <>
interval_t DateTrunc::WeekOperator::Operation(interval_t input) {
	input.micros = 0;
	input.days = (input.days / Interval::DAYS_PER_WEEK) * Interval::DAYS_PER_WEEK;
	return input;
}

template <>
interval_t DateTrunc::ISOYearOperator::Operation(interval_t input) {
	return YearOperator::Operation<interval_t, interval_t>(input);
}

template <>
interval_t DateTrunc::DayOperator::Operation(interval_t input) {
	input.micros = 0;
	return input;
}

template <>
interval_t DateTrunc::HourOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_HOUR) * Interval::MICROS_PER_HOUR;
	return input;
}

template <>
interval_t DateTrunc::MinuteOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_MINUTE) * Interval::MICROS_PER_MINUTE;
	return input;
}

template <>
interval_t DateTrunc::SecondOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_SEC) * Interval::MICROS_PER_SEC;
	return input;
}

template <>
interval_t DateTrunc::MillisecondOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_MSEC) * Interval::MICROS_PER_MSEC;
	return input;
}

template <>
interval_t DateTrunc::MicrosecondOperator::Operation(interval_t input) {
	return input;
}

template <class TA, class TR>
static TR TruncateElement(DatePartSpecifier type, TA element) {
	if (!Value::IsFinite(element)) {
		return Cast::template Operation<TA, TR>(element);
	}

	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return DateTrunc::MillenniumOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::CENTURY:
		return DateTrunc::CenturyOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DECADE:
		return DateTrunc::DecadeOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::YEAR:
		return DateTrunc::YearOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::QUARTER:
		return DateTrunc::QuarterOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MONTH:
		return DateTrunc::MonthOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateTrunc::WeekOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::ISOYEAR:
		return DateTrunc::ISOYearOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return DateTrunc::DayOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::HOUR:
		return DateTrunc::HourOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MINUTE:
		return DateTrunc::MinuteOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateTrunc::SecondOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MILLISECONDS:
		return DateTrunc::MillisecondOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MICROSECONDS:
		return DateTrunc::MicrosecondOperator::Operation<TA, TR>(element);
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC");
	}
}

struct DateTruncBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA specifier, TB date) {
		return TruncateElement<TB, TR>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

template <typename TA, typename TR>
static void DateTruncUnaryExecutor(DatePartSpecifier type, Vector &left, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MillenniumOperator>(left, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::CenturyOperator>(left, result, count);
		break;
	case DatePartSpecifier::DECADE:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::DecadeOperator>(left, result, count);
		break;
	case DatePartSpecifier::YEAR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::YearOperator>(left, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::QuarterOperator>(left, result, count);
		break;
	case DatePartSpecifier::MONTH:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MonthOperator>(left, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::WeekOperator>(left, result, count);
		break;
	case DatePartSpecifier::ISOYEAR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::ISOYearOperator>(left, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::DayOperator>(left, result, count);
		break;
	case DatePartSpecifier::HOUR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::HourOperator>(left, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MinuteOperator>(left, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::SecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MillisecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MicrosecondOperator>(left, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC");
	}
}

template <typename TA, typename TR>
static void DateTruncFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &part_arg = args.data[0];
	auto &date_arg = args.data[1];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateTruncUnaryExecutor<TA, TR>(type, date_arg, result, args.size());
		}
	} else {
		BinaryExecutor::ExecuteStandard<string_t, TA, TR, DateTruncBinaryOperator>(part_arg, date_arg, result,
		                                                                           args.size());
	}
}

template <class TA, class TR, class OP>
static unique_ptr<BaseStatistics> DateTruncStatistics(vector<BaseStatistics> &child_stats) {
	// we can only propagate date stats if the child has stats
	auto &nstats = child_stats[1];
	if (!NumericStats::HasMinMax(nstats)) {
		return nullptr;
	}
	// run the operator on both the min and the max, this gives us the [min, max] bound
	auto min = NumericStats::GetMin<TA>(nstats);
	auto max = NumericStats::GetMax<TA>(nstats);
	if (min > max) {
		return nullptr;
	}

	// Infinite values are unmodified
	auto min_part = DateTrunc::UnaryFunction<TA, TR, OP>(min);
	auto max_part = DateTrunc::UnaryFunction<TA, TR, OP>(max);

	auto min_value = Value::CreateValue(min_part);
	auto max_value = Value::CreateValue(max_part);
	auto result = NumericStats::CreateEmpty(min_value.type());
	NumericStats::SetMin(result, min_value);
	NumericStats::SetMax(result, max_value);
	result.CopyValidity(child_stats[0]);
	return result.ToUnique();
}

template <class TA, class TR, class OP>
static unique_ptr<BaseStatistics> PropagateDateTruncStatistics(ClientContext &context, FunctionStatisticsInput &input) {
	return DateTruncStatistics<TA, TR, OP>(input.child_stats);
}

template <typename TA, typename TR>
static function_statistics_t DateTruncStats(DatePartSpecifier type) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MillenniumOperator>;
	case DatePartSpecifier::CENTURY:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::CenturyOperator>;
	case DatePartSpecifier::DECADE:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::DecadeOperator>;
	case DatePartSpecifier::YEAR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::YearOperator>;
	case DatePartSpecifier::QUARTER:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::QuarterOperator>;
	case DatePartSpecifier::MONTH:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MonthOperator>;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::WeekOperator>;
	case DatePartSpecifier::ISOYEAR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::ISOYearOperator>;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::DayOperator>;
	case DatePartSpecifier::HOUR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::HourOperator>;
	case DatePartSpecifier::MINUTE:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MinuteOperator>;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::SecondOperator>;
	case DatePartSpecifier::MILLISECONDS:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MillisecondOperator>;
	case DatePartSpecifier::MICROSECONDS:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MicrosecondOperator>;
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC statistics");
	}
}

static unique_ptr<FunctionData> DateTruncBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		return nullptr;
	}

	// Rebind to return a date if we are truncating that far
	Value part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (part_value.IsNull()) {
		return nullptr;
	}
	const auto part_name = part_value.ToString();
	const auto part_code = GetDatePartSpecifier(part_name);
	switch (part_code) {
	case DatePartSpecifier::MILLENNIUM:
	case DatePartSpecifier::CENTURY:
	case DatePartSpecifier::DECADE:
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::QUARTER:
	case DatePartSpecifier::MONTH:
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
	case DatePartSpecifier::ISOYEAR:
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		switch (bound_function.arguments[1].id()) {
		case LogicalType::TIMESTAMP:
			bound_function.function = DateTruncFunction<timestamp_t, date_t>;
			bound_function.statistics = DateTruncStats<timestamp_t, date_t>(part_code);
			break;
		case LogicalType::DATE:
			bound_function.function = DateTruncFunction<date_t, date_t>;
			bound_function.statistics = DateTruncStats<date_t, date_t>(part_code);
			break;
		default:
			throw NotImplementedException("Temporal argument type for DATETRUNC");
		}
		bound_function.return_type = LogicalType::DATE;
		break;
	default:
		switch (bound_function.arguments[1].id()) {
		case LogicalType::TIMESTAMP:
			bound_function.statistics = DateTruncStats<timestamp_t, timestamp_t>(part_code);
			break;
		case LogicalType::DATE:
			bound_function.statistics = DateTruncStats<date_t, timestamp_t>(part_code);
			break;
		default:
			throw NotImplementedException("Temporal argument type for DATETRUNC");
		}
		break;
	}

	return nullptr;
}

ScalarFunctionSet DateTruncFun::GetFunctions() {
	ScalarFunctionSet date_trunc("date_trunc");
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                      DateTruncFunction<timestamp_t, timestamp_t>, DateTruncBind));
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::TIMESTAMP,
	                                      DateTruncFunction<date_t, timestamp_t>, DateTruncBind));
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                                      DateTruncFunction<interval_t, interval_t>));
	return date_trunc;
}

} // namespace duckdb






namespace duckdb {

struct EpochSecOperator {
	template <class INPUT_TYPE, class RESULT_TYPE>
	static RESULT_TYPE Operation(INPUT_TYPE sec) {
		int64_t result;
		if (!TryCast::Operation(sec * Interval::MICROS_PER_SEC, result)) {
			throw ConversionException("Could not convert epoch seconds to TIMESTAMP WITH TIME ZONE");
		}
		return timestamp_t(result);
	}
};

static void EpochSecFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);

	UnaryExecutor::Execute<double, timestamp_t, EpochSecOperator>(input.data[0], result, input.size());
}

ScalarFunction ToTimestampFun::GetFunction() {
	// to_timestamp is an alias from Postgres that converts the time in seconds to a timestamp
	return ScalarFunction({LogicalType::DOUBLE}, LogicalType::TIMESTAMP_TZ, EpochSecFunction);
}

} // namespace duckdb







#include <cmath>

namespace duckdb {

struct MakeDateOperator {
	template <typename YYYY, typename MM, typename DD, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd) {
		return Date::FromDate(yyyy, mm, dd);
	}
};

template <typename T>
static void ExecuteMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(),
	                                          MakeDateOperator::Operation<T, T, T, date_t>);
}

template <typename T>
static void ExecuteStructMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	// this should be guaranteed by the binder
	D_ASSERT(input.ColumnCount() == 1);
	auto &vec = input.data[0];

	auto &children = StructVector::GetEntries(vec);
	D_ASSERT(children.size() == 3);
	auto &yyyy = *children[0];
	auto &mm = *children[1];
	auto &dd = *children[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(), Date::FromDate);
}

struct MakeTimeOperator {
	template <typename HH, typename MM, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(HH hh, MM mm, SS ss) {
		int64_t secs = ss;
		int64_t micros = std::round((ss - secs) * Interval::MICROS_PER_SEC);
		if (!Time::IsValidTime(hh, mm, secs, micros)) {
			throw ConversionException("Time out of range: %d:%d:%d.%d", hh, mm, secs, micros);
		}
		return Time::FromTime(hh, mm, secs, micros);
	}
};

template <typename T>
static void ExecuteMakeTime(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, double, dtime_t>(yyyy, mm, dd, result, input.size(),
	                                                MakeTimeOperator::Operation<T, T, double, dtime_t>);
}

struct MakeTimestampOperator {
	template <typename YYYY, typename MM, typename DD, typename HR, typename MN, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd, HR hr, MN mn, SS ss) {
		const auto d = MakeDateOperator::Operation<YYYY, MM, DD, date_t>(yyyy, mm, dd);
		const auto t = MakeTimeOperator::Operation<HR, MN, SS, dtime_t>(hr, mn, ss);
		return Timestamp::FromDatetime(d, t);
	}

	template <typename T, typename RESULT_TYPE>
	static RESULT_TYPE Operation(T micros) {
		return timestamp_t(micros);
	}
};

template <typename T>
static void ExecuteMakeTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	if (input.ColumnCount() == 1) {
		auto func = MakeTimestampOperator::Operation<T, timestamp_t>;
		UnaryExecutor::Execute<T, timestamp_t>(input.data[0], result, input.size(), func);
		return;
	}

	D_ASSERT(input.ColumnCount() == 6);

	auto func = MakeTimestampOperator::Operation<T, T, T, T, T, double, timestamp_t>;
	SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(input, result, func);
}

ScalarFunctionSet MakeDateFun::GetFunctions() {
	ScalarFunctionSet make_date("make_date");
	make_date.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::DATE, ExecuteMakeDate<int64_t>));

	child_list_t<LogicalType> make_date_children {
	    {"year", LogicalType::BIGINT}, {"month", LogicalType::BIGINT}, {"day", LogicalType::BIGINT}};
	make_date.AddFunction(
	    ScalarFunction({LogicalType::STRUCT(make_date_children)}, LogicalType::DATE, ExecuteStructMakeDate<int64_t>));
	return make_date;
}

ScalarFunction MakeTimeFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE}, LogicalType::TIME,
	                      ExecuteMakeTime<int64_t>);
}

ScalarFunctionSet MakeTimestampFun::GetFunctions() {
	ScalarFunctionSet operator_set("make_timestamp");
	operator_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                         LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                        LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	return operator_set;
}

} // namespace duckdb








#include <cctype>
#include <utility>

namespace duckdb {

struct StrfTimeBindData : public FunctionData {
	explicit StrfTimeBindData(StrfTimeFormat format_p, string format_string_p, bool is_null)
	    : format(std::move(format_p)), format_string(std::move(format_string_p)), is_null(is_null) {
	}

	StrfTimeFormat format;
	string format_string;
	bool is_null;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StrfTimeBindData>(format, format_string, is_null);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StrfTimeBindData>();
		return format_string == other.format_string;
	}
};

template <bool REVERSED>
static unique_ptr<FunctionData> StrfTimeBindFunction(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	auto format_idx = REVERSED ? 0 : 1;
	auto &format_arg = arguments[format_idx];
	if (format_arg->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!format_arg->IsFoldable()) {
		throw InvalidInputException("strftime format must be a constant");
	}
	Value options_str = ExpressionExecutor::EvaluateScalar(context, *format_arg);
	auto format_string = options_str.GetValue<string>();
	StrfTimeFormat format;
	bool is_null = options_str.IsNull();
	if (!is_null) {
		string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
		}
	}
	return make_uniq<StrfTimeBindData>(format, format_string, is_null);
}

template <bool REVERSED>
static void StrfTimeFunctionDate(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StrfTimeBindData>();

	if (info.is_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}
	info.format.ConvertDateVector(args.data[REVERSED ? 1 : 0], result, args.size());
}

template <bool REVERSED>
static void StrfTimeFunctionTimestamp(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StrfTimeBindData>();

	if (info.is_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}
	info.format.ConvertTimestampVector(args.data[REVERSED ? 1 : 0], result, args.size());
}

ScalarFunctionSet StrfTimeFun::GetFunctions() {
	ScalarFunctionSet strftime;

	strftime.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                    StrfTimeFunctionDate<false>, StrfTimeBindFunction<false>));
	strftime.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                    StrfTimeFunctionTimestamp<false>, StrfTimeBindFunction<false>));
	strftime.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::VARCHAR,
	                                    StrfTimeFunctionDate<true>, StrfTimeBindFunction<true>));
	strftime.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                    StrfTimeFunctionTimestamp<true>, StrfTimeBindFunction<true>));
	return strftime;
}

StrpTimeFormat::StrpTimeFormat() {
}

StrpTimeFormat::StrpTimeFormat(const string &format_string) {
	if (format_string.empty()) {
		return;
	}
	StrTimeFormat::ParseFormatSpecifier(format_string, *this);
}

struct StrpTimeBindData : public FunctionData {
	StrpTimeBindData(const StrpTimeFormat &format, const string &format_string)
	    : formats(1, format), format_strings(1, format_string) {
	}

	StrpTimeBindData(vector<StrpTimeFormat> formats_p, vector<string> format_strings_p)
	    : formats(std::move(formats_p)), format_strings(std::move(format_strings_p)) {
	}

	vector<StrpTimeFormat> formats;
	vector<string> format_strings;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StrpTimeBindData>(formats, format_strings);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StrpTimeBindData>();
		return format_strings == other.format_strings;
	}
};

static unique_ptr<FunctionData> StrpTimeBindFunction(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("strptime format must be a constant");
	}
	Value format_value = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	string format_string;
	StrpTimeFormat format;
	if (format_value.IsNull()) {
		return make_uniq<StrpTimeBindData>(format, format_string);
	} else if (format_value.type().id() == LogicalTypeId::VARCHAR) {
		format_string = format_value.ToString();
		format.format_specifier = format_string;
		string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
		if (!error.empty()) {
			throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
		}
		if (format.HasFormatSpecifier(StrTimeSpecifier::UTC_OFFSET)) {
			bound_function.return_type = LogicalType::TIMESTAMP_TZ;
		}
		return make_uniq<StrpTimeBindData>(format, format_string);
	} else if (format_value.type() == LogicalType::LIST(LogicalType::VARCHAR)) {
		const auto &children = ListValue::GetChildren(format_value);
		if (children.empty()) {
			throw InvalidInputException("strptime format list must not be empty");
		}
		vector<string> format_strings;
		vector<StrpTimeFormat> formats;
		for (const auto &child : children) {
			format_string = child.ToString();
			format.format_specifier = format_string;
			string error = StrTimeFormat::ParseFormatSpecifier(format_string, format);
			if (!error.empty()) {
				throw InvalidInputException("Failed to parse format specifier %s: %s", format_string, error);
			}
			// If any format has UTC offsets, then we have to produce TSTZ
			if (format.HasFormatSpecifier(StrTimeSpecifier::UTC_OFFSET)) {
				bound_function.return_type = LogicalType::TIMESTAMP_TZ;
			}
			format_strings.emplace_back(format_string);
			formats.emplace_back(format);
		}
		return make_uniq<StrpTimeBindData>(formats, format_strings);
	} else {
		throw InvalidInputException("strptime format must be a string");
	}
}

struct StrpTimeFunction {

	static void Parse(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<StrpTimeBindData>();

		if (args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR && ConstantVector::IsNull(args.data[1])) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}
		UnaryExecutor::Execute<string_t, timestamp_t>(args.data[0], result, args.size(), [&](string_t input) {
			StrpTimeFormat::ParseResult result;
			for (auto &format : info.formats) {
				if (format.Parse(input, result)) {
					return result.ToTimestamp();
				}
			}
			throw InvalidInputException(result.FormatError(input, info.formats[0].format_specifier));
		});
	}

	static void TryParse(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<StrpTimeBindData>();

		if (args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR && ConstantVector::IsNull(args.data[1])) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return;
		}

		UnaryExecutor::ExecuteWithNulls<string_t, timestamp_t>(
		    args.data[0], result, args.size(), [&](string_t input, ValidityMask &mask, idx_t idx) {
			    timestamp_t result;
			    string error;
			    for (auto &format : info.formats) {
				    if (format.TryParseTimestamp(input, result, error)) {
					    return result;
				    }
			    }

			    mask.SetInvalid(idx);
			    return timestamp_t();
		    });
	}
};

ScalarFunctionSet StrpTimeFun::GetFunctions() {
	ScalarFunctionSet strptime;

	const auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	auto fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::TIMESTAMP,
	                          StrpTimeFunction::Parse, StrpTimeBindFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	strptime.AddFunction(fun);

	fun = ScalarFunction({LogicalType::VARCHAR, list_type}, LogicalType::TIMESTAMP, StrpTimeFunction::Parse,
	                     StrpTimeBindFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	strptime.AddFunction(fun);
	return strptime;
}

ScalarFunctionSet TryStrpTimeFun::GetFunctions() {
	ScalarFunctionSet try_strptime;

	const auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	auto fun = ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::TIMESTAMP,
	                          StrpTimeFunction::TryParse, StrpTimeBindFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	try_strptime.AddFunction(fun);

	fun = ScalarFunction({LogicalType::VARCHAR, list_type}, LogicalType::TIMESTAMP, StrpTimeFunction::TryParse,
	                     StrpTimeBindFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	try_strptime.AddFunction(fun);

	return try_strptime;
}

} // namespace duckdb













namespace duckdb {

struct TimeBucket {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	// There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 360 months between 1970-01-01 and 2000-01-01
	constexpr static const int32_t DEFAULT_ORIGIN_MONTHS = 360;

	enum struct BucketWidthType { CONVERTIBLE_TO_MICROS, CONVERTIBLE_TO_MONTHS, UNCLASSIFIED };

	static inline BucketWidthType ClassifyBucketWidth(const interval_t bucket_width) {
		if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else {
			return BucketWidthType::UNCLASSIFIED;
		}
	}

	static inline BucketWidthType ClassifyBucketWidthErrorThrow(const interval_t bucket_width) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}

	template <typename T>
	static inline int32_t EpochMonths(T ts) {
		date_t ts_date = Cast::template Operation<T, date_t>(ts);
		return (Date::ExtractYear(ts_date) - 1970) * 12 + Date::ExtractMonth(ts_date) - 1;
	}

	static inline timestamp_t WidthConvertibleToMicrosCommon(int64_t bucket_width_micros, int64_t ts_micros,
	                                                         int64_t origin_micros) {
		origin_micros %= bucket_width_micros;
		ts_micros = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(ts_micros, origin_micros);

		int64_t result_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			result_micros =
			    SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(result_micros, bucket_width_micros);
		}
		result_micros += origin_micros;

		return Timestamp::FromEpochMicroSeconds(result_micros);
	}

	static inline date_t WidthConvertibleToMonthsCommon(int32_t bucket_width_months, int32_t ts_months,
	                                                    int32_t origin_months) {
		origin_months %= bucket_width_months;
		ts_months = SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(ts_months, origin_months);

		int32_t result_months = (ts_months / bucket_width_months) * bucket_width_months;
		if (ts_months < 0 && ts_months % bucket_width_months != 0) {
			result_months =
			    SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(result_months, bucket_width_months);
		}
		result_months += origin_months;

		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;

		return Date::FromDate(year, month, 1);
	}

	struct WidthConvertibleToMicrosBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			return Cast::template Operation<timestamp_t, TR>(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS));
		}
	};

	struct WidthConvertibleToMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			return Cast::template Operation<date_t, TR>(
			    WidthConvertibleToMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS));
		}
	};

	struct BinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return WidthConvertibleToMicrosBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return WidthConvertibleToMonthsBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	};

	struct OffsetWidthConvertibleToMicrosTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(
			    Interval::Add(Cast::template Operation<TB, timestamp_t>(ts), Interval::Invert(offset)));
			return Cast::template Operation<timestamp_t, TR>(Interval::Add(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS), offset));
		}
	};

	struct OffsetWidthConvertibleToMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(Interval::Add(ts, Interval::Invert(offset)));
			return Interval::Add(Cast::template Operation<date_t, TR>(WidthConvertibleToMonthsCommon(
			                         bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS)),
			                     offset);
		}
	};

	struct OffsetTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OffsetWidthConvertibleToMicrosTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                offset);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OffsetWidthConvertibleToMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                offset);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	};

	struct OriginWidthConvertibleToMicrosTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			int64_t origin_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(origin));
			return Cast::template Operation<timestamp_t, TR>(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, origin_micros));
		}
	};

	struct OriginWidthConvertibleToMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			int32_t origin_months = EpochMonths(origin);
			return Cast::template Operation<date_t, TR>(
			    WidthConvertibleToMonthsCommon(bucket_width.months, ts_months, origin_months));
		}
	};

	struct OriginTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin, ValidityMask &mask, idx_t idx) {
			if (!Value::IsFinite(origin)) {
				mask.SetInvalid(idx);
				return TR();
			}
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OriginWidthConvertibleToMicrosTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                origin);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OriginWidthConvertibleToMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                origin);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	};
};

template <typename T>
static void TimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthConvertibleToMicrosBinaryOperator::Operation<interval_t, T, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthConvertibleToMonthsBinaryOperator::Operation<interval_t, T, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				BinaryExecutor::Execute<interval_t, T, T>(bucket_width_arg, ts_arg, result, args.size(),
				                                          TimeBucket::BinaryOperator::Operation<interval_t, T, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	} else {
		BinaryExecutor::Execute<interval_t, T, T>(bucket_width_arg, ts_arg, result, args.size(),
		                                          TimeBucket::BinaryOperator::Operation<interval_t, T, T>);
	}
}

template <typename T>
static void TimeBucketOffsetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &offset_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthConvertibleToMicrosTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthConvertibleToMonthsTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, T, interval_t, T>(
		    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
		    TimeBucket::OffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
	}
}

template <typename T>
static void TimeBucketOriginFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &origin_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    origin_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg) || ConstantVector::IsNull(origin_arg) ||
		    !Value::IsFinite(*ConstantVector::GetData<T>(origin_arg))) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthConvertibleToMicrosTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthConvertibleToMonthsTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
		    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
		    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
	}
}

ScalarFunctionSet TimeBucketFun::GetFunctions() {
	ScalarFunctionSet time_bucket;
	time_bucket.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE}, LogicalType::DATE, TimeBucketFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                       TimeBucketFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::INTERVAL},
	                                       LogicalType::DATE, TimeBucketOffsetFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                       LogicalType::TIMESTAMP, TimeBucketOffsetFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::DATE},
	                                       LogicalType::DATE, TimeBucketOriginFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                       LogicalType::TIMESTAMP, TimeBucketOriginFunction<timestamp_t>));
	return time_bucket;
}

} // namespace duckdb




namespace duckdb {

struct ToYearsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.days = 0;
		result.micros = 0;
		if (!TryMultiplyOperator::Operation<int32_t, int32_t, int32_t>(input, Interval::MONTHS_PER_YEAR,
		                                                               result.months)) {
			throw OutOfRangeException("Interval value %d years out of range", input);
		}
		return result;
	}
};

struct ToMonthsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = input;
		result.days = 0;
		result.micros = 0;
		return result;
	}
};

struct ToDaysOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = input;
		result.micros = 0;
		return result;
	}
};

struct ToHoursOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, Interval::MICROS_PER_HOUR,
		                                                               result.micros)) {
			throw OutOfRangeException("Interval value %d hours out of range", input);
		}
		return result;
	}
};

struct ToMinutesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, Interval::MICROS_PER_MINUTE,
		                                                               result.micros)) {
			throw OutOfRangeException("Interval value %d minutes out of range", input);
		}
		return result;
	}
};

struct ToSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, Interval::MICROS_PER_SEC,
		                                                               result.micros)) {
			throw OutOfRangeException("Interval value %d seconds out of range", input);
		}
		return result;
	}
};

struct ToMilliSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(input, Interval::MICROS_PER_MSEC,
		                                                               result.micros)) {
			throw OutOfRangeException("Interval value %d milliseconds out of range", input);
		}
		return result;
	}
};

struct ToMicroSecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		interval_t result;
		result.months = 0;
		result.days = 0;
		result.micros = input;
		return result;
	}
};

ScalarFunction ToYearsFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToYearsOperator>);
}

ScalarFunction ToMonthsFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToMonthsOperator>);
}

ScalarFunction ToDaysFun::GetFunction() {
	return ScalarFunction({LogicalType::INTEGER}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int32_t, interval_t, ToDaysOperator>);
}

ScalarFunction ToHoursFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToHoursOperator>);
}

ScalarFunction ToMinutesFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToMinutesOperator>);
}

ScalarFunction ToSecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToSecondsOperator>);
}

ScalarFunction ToMillisecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToMilliSecondsOperator>);
}

ScalarFunction ToMicrosecondsFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT}, LogicalType::INTERVAL,
	                      ScalarFunction::UnaryFunction<int64_t, interval_t, ToMicroSecondsOperator>);
}

} // namespace duckdb







namespace duckdb {

static void VectorTypeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto data = ConstantVector::GetData<string_t>(result);
	data[0] = StringVector::AddString(result, EnumUtil::ToString(input.data[0].GetVectorType()));
}

ScalarFunction VectorTypeFun::GetFunction() {
	return ScalarFunction("vector_type",        // name of the function
	                      {LogicalType::ANY},   // argument list
	                      LogicalType::VARCHAR, // return type
	                      VectorTypeFunction);
}

} // namespace duckdb


namespace duckdb {

static void EnumFirstFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	auto val = Value(enum_vector.GetValue(0));
	result.Reference(val);
}

static void EnumLastFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto enum_size = EnumType::GetSize(types[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	auto val = Value(enum_vector.GetValue(enum_size - 1));
	result.Reference(val);
}

static void EnumRangeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto enum_size = EnumType::GetSize(types[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	vector<Value> enum_values;
	for (idx_t i = 0; i < enum_size; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	auto val = Value::LIST(enum_values);
	result.Reference(val);
}

static void EnumRangeBoundaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 2);
	idx_t start, end;
	auto first_param = input.GetValue(0, 0);
	auto second_param = input.GetValue(1, 0);

	auto &enum_vector =
	    first_param.IsNull() ? EnumType::GetValuesInsertOrder(types[1]) : EnumType::GetValuesInsertOrder(types[0]);

	if (first_param.IsNull()) {
		start = 0;
	} else {
		start = first_param.GetValue<uint32_t>();
	}
	if (second_param.IsNull()) {
		end = EnumType::GetSize(types[0]);
	} else {
		end = second_param.GetValue<uint32_t>() + 1;
	}
	vector<Value> enum_values;
	for (idx_t i = start; i < end; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	Value val;
	if (enum_values.empty()) {
		val = Value::EMPTYLIST(LogicalType::VARCHAR);
	} else {
		val = Value::LIST(enum_values);
	}
	result.Reference(val);
}

static void EnumCodeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	result.Reinterpret(input.data[0]);
}

static void CheckEnumParameter(const Expression &expr) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
}

unique_ptr<FunctionData> BindEnumFunction(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	return nullptr;
}

unique_ptr<FunctionData> BindEnumCodeFunction(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw BinderException("This function needs an ENUM as an argument");
	}

	auto phy_type = EnumType::GetPhysicalType(arguments[0]->return_type);
	switch (phy_type) {
	case PhysicalType::UINT8:
		bound_function.return_type = LogicalType(LogicalTypeId::UTINYINT);
		break;
	case PhysicalType::UINT16:
		bound_function.return_type = LogicalType(LogicalTypeId::USMALLINT);
		break;
	case PhysicalType::UINT32:
		bound_function.return_type = LogicalType(LogicalTypeId::UINTEGER);
		break;
	case PhysicalType::UINT64:
		bound_function.return_type = LogicalType(LogicalTypeId::UBIGINT);
		break;
	default:
		throw InternalException("Unsupported Enum Internal Type");
	}

	return nullptr;
}

unique_ptr<FunctionData> BindEnumRangeBoundaryFunction(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	CheckEnumParameter(*arguments[1]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM && arguments[0]->return_type != LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[1]->return_type.id() != LogicalTypeId::ENUM && arguments[1]->return_type != LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[0]->return_type == LogicalType::SQLNULL && arguments[1]->return_type == LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[0]->return_type.id() == LogicalTypeId::ENUM &&
	    arguments[1]->return_type.id() == LogicalTypeId::ENUM &&
	    arguments[0]->return_type != arguments[1]->return_type) {
		throw BinderException("The parameters need to link to ONLY one enum OR be NULL ");
	}
	return nullptr;
}

ScalarFunction EnumFirstFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, EnumFirstFunction, BindEnumFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

ScalarFunction EnumLastFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, EnumLastFunction, BindEnumFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

ScalarFunction EnumCodeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::ANY, EnumCodeFunction, BindEnumCodeFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

ScalarFunction EnumRangeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), EnumRangeFunction,
	                          BindEnumFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

ScalarFunction EnumRangeBoundaryFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY, LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR),
	                          EnumRangeBoundaryFunction, BindEnumRangeBoundaryFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb



namespace duckdb {

static void AliasFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	Value v(state.expr.alias.empty() ? func_expr.children[0]->GetName() : state.expr.alias);
	result.Reference(v);
}

ScalarFunction AliasFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, AliasFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb







namespace duckdb {

struct CurrentSettingBindData : public FunctionData {
	explicit CurrentSettingBindData(Value value_p) : value(std::move(value_p)) {
	}

	Value value;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSettingBindData>(value);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSettingBindData>();
		return Value::NotDistinctFrom(value, other.value);
	}
};

static void CurrentSettingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<CurrentSettingBindData>();
	result.Reference(info.value);
}

unique_ptr<FunctionData> CurrentSettingBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {

	auto &key_child = arguments[0];
	if (key_child->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	if (key_child->return_type.id() != LogicalTypeId::VARCHAR ||
	    key_child->return_type.id() != LogicalTypeId::VARCHAR || !key_child->IsFoldable()) {
		throw ParserException("Key name for current_setting needs to be a constant string");
	}
	Value key_val = ExpressionExecutor::EvaluateScalar(context, *key_child);
	D_ASSERT(key_val.type().id() == LogicalTypeId::VARCHAR);
	auto &key_str = StringValue::Get(key_val);
	if (key_val.IsNull() || key_str.empty()) {
		throw ParserException("Key name for current_setting needs to be neither NULL nor empty");
	}

	auto key = StringUtil::Lower(key_str);
	Value val;
	if (!context.TryGetCurrentSetting(key, val)) {
		Catalog::AutoloadExtensionByConfigName(context, key);
		// If autoloader didn't throw, the config is now available
		context.TryGetCurrentSetting(key, val);
	}

	bound_function.return_type = val.type();
	return make_uniq<CurrentSettingBindData>(val);
}

ScalarFunction CurrentSettingFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::ANY, CurrentSettingFunction, CurrentSettingBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb

#include <iostream>

namespace duckdb {

struct ErrorOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		throw Exception(input.GetString());
	}
};

ScalarFunction ErrorFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                          ScalarFunction::UnaryFunction<string_t, bool, ErrorOperator>);
	// Set the function with side effects to avoid the optimization.
	fun.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return fun;
}

} // namespace duckdb


namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction HashFun::GetFunction() {
	auto hash_fun = ScalarFunction({LogicalType::ANY}, LogicalType::HASH, HashFunction);
	hash_fun.varargs = LogicalType::ANY;
	hash_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return hash_fun;
}

} // namespace duckdb



namespace duckdb {

template <class OP>
struct LeastOperator {
	template <class T>
	static T Operation(T left, T right) {
		return OP::Operation(left, right) ? left : right;
	}
};

template <class T, class OP, bool IS_STRING = false>
static void LeastGreatestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (args.ColumnCount() == 1) {
		// single input: nop
		result.Reference(args.data[0]);
		return;
	}
	auto result_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			// non-constant input: result is not a constant vector
			result_type = VectorType::FLAT_VECTOR;
		}
		if (IS_STRING) {
			// for string vectors we add a reference to the heap of the children
			StringVector::AddHeapReference(result, args.data[col_idx]);
		}
	}

	auto result_data = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	// copy over the first column
	bool result_has_value[STANDARD_VECTOR_SIZE];
	{
		UnifiedVectorFormat vdata;
		args.data[0].ToUnifiedFormat(args.size(), vdata);
		auto input_data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < args.size(); i++) {
			auto vindex = vdata.sel->get_index(i);
			if (vdata.validity.RowIsValid(vindex)) {
				result_data[i] = input_data[vindex];
				result_has_value[i] = true;
			} else {
				result_has_value[i] = false;
			}
		}
	}
	// now handle the remainder of the columns
	for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
		if (args.data[col_idx].GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    ConstantVector::IsNull(args.data[col_idx])) {
			// ignore null vector
			continue;
		}

		UnifiedVectorFormat vdata;
		args.data[col_idx].ToUnifiedFormat(args.size(), vdata);

		auto input_data = UnifiedVectorFormat::GetData<T>(vdata);
		if (!vdata.validity.AllValid()) {
			// potential new null entries: have to check the null mask
			for (idx_t i = 0; i < args.size(); i++) {
				auto vindex = vdata.sel->get_index(i);
				if (vdata.validity.RowIsValid(vindex)) {
					// not a null entry: perform the operation and add to new set
					auto ivalue = input_data[vindex];
					if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
						result_has_value[i] = true;
						result_data[i] = ivalue;
					}
				}
			}
		} else {
			// no new null entries: only need to perform the operation
			for (idx_t i = 0; i < args.size(); i++) {
				auto vindex = vdata.sel->get_index(i);

				auto ivalue = input_data[vindex];
				if (!result_has_value[i] || OP::template Operation<T>(ivalue, result_data[i])) {
					result_has_value[i] = true;
					result_data[i] = ivalue;
				}
			}
		}
	}
	for (idx_t i = 0; i < args.size(); i++) {
		if (!result_has_value[i]) {
			result_mask.SetInvalid(i);
		}
	}
	result.SetVectorType(result_type);
}

template <typename T, class OP>
ScalarFunction GetLeastGreatestFunction(const LogicalType &type) {
	return ScalarFunction({type}, type, LeastGreatestFunction<T, OP>, nullptr, nullptr, nullptr, nullptr, type,
	                      FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING);
}

template <class OP>
static ScalarFunctionSet GetLeastGreatestFunctions() {
	ScalarFunctionSet fun_set;
	fun_set.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT, LeastGreatestFunction<int64_t, OP>,
	                                   nullptr, nullptr, nullptr, nullptr, LogicalType::BIGINT,
	                                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction(
	    {LogicalType::HUGEINT}, LogicalType::HUGEINT, LeastGreatestFunction<hugeint_t, OP>, nullptr, nullptr, nullptr,
	    nullptr, LogicalType::HUGEINT, FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE, LeastGreatestFunction<double, OP>,
	                                   nullptr, nullptr, nullptr, nullptr, LogicalType::DOUBLE,
	                                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	fun_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   LeastGreatestFunction<string_t, OP, true>, nullptr, nullptr, nullptr, nullptr,
	                                   LogicalType::VARCHAR, FunctionSideEffects::NO_SIDE_EFFECTS,
	                                   FunctionNullHandling::SPECIAL_HANDLING));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME));
	fun_set.AddFunction(GetLeastGreatestFunction<date_t, OP>(LogicalType::DATE));

	fun_set.AddFunction(GetLeastGreatestFunction<timestamp_t, OP>(LogicalType::TIMESTAMP_TZ));
	fun_set.AddFunction(GetLeastGreatestFunction<time_t, OP>(LogicalType::TIME_TZ));
	return fun_set;
}

ScalarFunctionSet LeastFun::GetFunctions() {
	return GetLeastGreatestFunctions<duckdb::LessThan>();
}

ScalarFunctionSet GreatestFun::GetFunctions() {
	return GetLeastGreatestFunctions<duckdb::GreaterThan>();
}

} // namespace duckdb



namespace duckdb {

struct StatsBindData : public FunctionData {
	explicit StatsBindData(string stats_p = string()) : stats(std::move(stats_p)) {
	}

	string stats;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StatsBindData>(stats);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<StatsBindData>();
		return stats == other.stats;
	}
};

static void StatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<StatsBindData>();
	if (info.stats.empty()) {
		info.stats = "No statistics";
	}
	Value v(info.stats);
	result.Reference(v);
}

unique_ptr<FunctionData> StatsBind(ClientContext &context, ScalarFunction &bound_function,
                                   vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<StatsBindData>();
}

static unique_ptr<BaseStatistics> StatsPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;
	auto &info = bind_data->Cast<StatsBindData>();
	info.stats = child_stats[0].ToString();
	return nullptr;
}

ScalarFunction StatsFun::GetFunction() {
	ScalarFunction stats({LogicalType::ANY}, LogicalType::VARCHAR, StatsFunction, StatsBind, nullptr,
	                     StatsPropagateStats);
	stats.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	stats.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return stats;
}

} // namespace duckdb

#endif
