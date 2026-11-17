#include "pgwire.hpp"

#include "log.hpp"
#include "pgcompat.hpp"

#include "duckdb/main/query_result_stream.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <random>
#include <regex>
#include <stdexcept>
#include <vector>

namespace dgl {

using duckdb::idx_t;
using duckdb::LogicalType;
using duckdb::LogicalTypeId;
using duckdb::Value;

namespace {

// ---------------------------------------------------------------------------
// Socket I/O and message framing

bool ReadExact(int fd, void *buffer, size_t n) {
	auto p = static_cast<char *>(buffer);
	while (n > 0) {
		auto got = ::recv(fd, p, n, 0);
		if (got <= 0) {
			return false;
		}
		p += got;
		n -= static_cast<size_t>(got);
	}
	return true;
}

bool WriteAll(int fd, const char *data, size_t n) {
	while (n > 0) {
#ifdef MSG_NOSIGNAL
		auto sent = ::send(fd, data, n, MSG_NOSIGNAL);
#else
		auto sent = ::send(fd, data, n, 0); // macOS: SO_NOSIGPIPE is set on the socket instead
#endif
		if (sent <= 0) {
			return false;
		}
		data += sent;
		n -= static_cast<size_t>(sent);
	}
	return true;
}

struct ProtocolError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

// Reads the body of one message.
class Reader {
public:
	explicit Reader(const std::string &data) : data_(data) {
	}
	uint8_t Byte() {
		Need(1);
		return static_cast<uint8_t>(data_[pos_++]);
	}
	int16_t Int16() {
		Need(2);
		uint16_t v;
		std::memcpy(&v, data_.data() + pos_, 2);
		pos_ += 2;
		return static_cast<int16_t>(ntohs(v));
	}
	int32_t Int32() {
		Need(4);
		uint32_t v;
		std::memcpy(&v, data_.data() + pos_, 4);
		pos_ += 4;
		return static_cast<int32_t>(ntohl(v));
	}
	std::string CString() {
		auto end = data_.find('\0', pos_);
		if (end == std::string::npos) {
			throw ProtocolError("unterminated string in message");
		}
		auto s = data_.substr(pos_, end - pos_);
		pos_ = end + 1;
		return s;
	}
	std::string Bytes(size_t n) {
		Need(n);
		auto s = data_.substr(pos_, n);
		pos_ += n;
		return s;
	}

private:
	void Need(size_t n) {
		if (pos_ + n > data_.size()) {
			throw ProtocolError("message too short");
		}
	}
	const std::string &data_;
	size_t pos_ = 0;
};

// Builds outgoing messages into one buffer, flushed at protocol sync points.
class Writer {
public:
	void Begin(char type) {
		buf_.push_back(type);
		start_ = buf_.size();
		Int32(0); // the length, filled in by End
	}
	void End() {
		uint32_t len = htonl(static_cast<uint32_t>(buf_.size() - start_));
		std::memcpy(&buf_[start_], &len, 4);
	}
	void Byte(char c) {
		buf_.push_back(c);
	}
	void Int16(int16_t v) {
		uint16_t n = htons(static_cast<uint16_t>(v));
		buf_.append(reinterpret_cast<char *>(&n), 2);
	}
	void Int32(int32_t v) {
		uint32_t n = htonl(static_cast<uint32_t>(v));
		buf_.append(reinterpret_cast<char *>(&n), 4);
	}
	void CString(const std::string &s) {
		buf_.append(s.data(), s.size());
		buf_.push_back('\0');
	}
	void Raw(const std::string &s) {
		buf_ += s;
	}
	size_t Size() const {
		return buf_.size();
	}
	bool Flush(int fd) {
		bool ok = WriteAll(fd, buf_.data(), buf_.size());
		buf_.clear();
		return ok;
	}

private:
	std::string buf_;
	size_t start_ = 0;
};

// ---------------------------------------------------------------------------
// Types

namespace oid {
constexpr int32_t BOOL = 16, BYTEA = 17, INT8 = 20, INT2 = 21, INT4 = 23, TEXT = 25, FLOAT4 = 700, FLOAT8 = 701,
                  VARCHAR = 1043, DATE = 1082, TIME = 1083, TIMESTAMP = 1114, TIMESTAMPTZ = 1184, INTERVAL = 1186,
                  NUMERIC = 1700, UUID = 2950, UNKNOWN = 705;
}

struct PgType {
	int32_t oid;
	int16_t len;
};

PgType ToPg(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return {oid::BOOL, 1};
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::UTINYINT:
		return {oid::INT2, 2};
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::USMALLINT:
		return {oid::INT4, 4};
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return {oid::INT8, 8};
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DECIMAL:
		return {oid::NUMERIC, -1};
	case LogicalTypeId::FLOAT:
		return {oid::FLOAT4, 4};
	case LogicalTypeId::DOUBLE:
		return {oid::FLOAT8, 8};
	case LogicalTypeId::DATE:
		return {oid::DATE, 4};
	case LogicalTypeId::TIME:
		return {oid::TIME, 8};
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_NS:
		return {oid::TIMESTAMP, 8};
	case LogicalTypeId::TIMESTAMP_TZ:
		return {oid::TIMESTAMPTZ, 8};
	case LogicalTypeId::INTERVAL:
		return {oid::INTERVAL, 16};
	case LogicalTypeId::BLOB:
		return {oid::BYTEA, -1};
	case LogicalTypeId::UUID:
		return {oid::UUID, 16};
	case LogicalTypeId::VARCHAR:
		return {oid::VARCHAR, -1};
	default:
		return {oid::TEXT, -1}; // lists, structs, maps, ...: their DuckDB text form
	}
}

// The DuckDB type a client means by a parameter's type OID, or INVALID for "unspecified".
LogicalType FromPg(int32_t type_oid) {
	switch (type_oid) {
	case oid::BOOL:
		return LogicalType::BOOLEAN;
	case oid::INT2:
		return LogicalType::SMALLINT;
	case oid::INT4:
		return LogicalType::INTEGER;
	case oid::INT8:
		return LogicalType::BIGINT;
	case oid::FLOAT4:
		return LogicalType::FLOAT;
	case oid::FLOAT8:
		return LogicalType::DOUBLE;
	case oid::DATE:
		return LogicalType::DATE;
	case oid::TIME:
		return LogicalType::TIME;
	case oid::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case oid::TIMESTAMPTZ:
		return LogicalType::TIMESTAMP_TZ;
	case oid::INTERVAL:
		return LogicalType::INTERVAL;
	case oid::BYTEA:
		return LogicalType::BLOB;
	case oid::UUID:
		return LogicalType::UUID;
	case oid::TEXT:
	case oid::VARCHAR:
		return LogicalType::VARCHAR;
	default:
		return LogicalType::INVALID;
	}
}

bool UsableType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::INVALID:
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::ANY:
	case LogicalTypeId::SQLNULL:
		return false;
	default:
		return true;
	}
}

// Microseconds and days between 1970-01-01 (DuckDB) and 2000-01-01 (PostgreSQL).
constexpr int64_t PG_EPOCH_DAYS = 10957;
constexpr int64_t PG_EPOCH_MICROS = PG_EPOCH_DAYS * 86400LL * 1000000LL;

std::string Int16Bytes(int16_t v) {
	uint16_t n = htons(static_cast<uint16_t>(v));
	return std::string(reinterpret_cast<char *>(&n), 2);
}
std::string Int32Bytes(int32_t v) {
	uint32_t n = htonl(static_cast<uint32_t>(v));
	return std::string(reinterpret_cast<char *>(&n), 4);
}
std::string Int64Bytes(int64_t v) {
	return Int32Bytes(static_cast<int32_t>(static_cast<uint64_t>(v) >> 32)) +
	       Int32Bytes(static_cast<int32_t>(static_cast<uint64_t>(v) & 0xffffffffULL));
}
int64_t ReadInt64(const std::string &s) {
	uint64_t v = 0;
	for (int i = 0; i < 8; i++) {
		v = (v << 8) | static_cast<uint8_t>(s[i]);
	}
	return static_cast<int64_t>(v);
}
int32_t ReadInt32(const std::string &s, size_t at = 0) {
	uint32_t v = 0;
	for (int i = 0; i < 4; i++) {
		v = (v << 8) | static_cast<uint8_t>(s[at + i]);
	}
	return static_cast<int32_t>(v);
}
int16_t ReadInt16(const std::string &s, size_t at) {
	return static_cast<int16_t>((static_cast<uint8_t>(s[at]) << 8) | static_cast<uint8_t>(s[at + 1]));
}

// PostgreSQL's binary numeric: base-10000 digits around the decimal point.
std::string NumericBinary(const std::string &text) {
	std::string s = text;
	bool negative = !s.empty() && s[0] == '-';
	if (negative || (!s.empty() && s[0] == '+')) {
		s = s.substr(1);
	}
	auto dot = s.find('.');
	std::string int_part = dot == std::string::npos ? s : s.substr(0, dot);
	std::string frac_part = dot == std::string::npos ? "" : s.substr(dot + 1);
	auto dscale = static_cast<int16_t>(frac_part.size());
	while (int_part.size() % 4) {
		int_part = "0" + int_part;
	}
	while (frac_part.size() % 4) {
		frac_part += "0";
	}
	std::vector<int16_t> digits;
	for (size_t i = 0; i < int_part.size(); i += 4) {
		digits.push_back(static_cast<int16_t>(std::stoi(int_part.substr(i, 4))));
	}
	int16_t weight = static_cast<int16_t>(int_part.size() / 4) - 1;
	for (size_t i = 0; i < frac_part.size(); i += 4) {
		digits.push_back(static_cast<int16_t>(std::stoi(frac_part.substr(i, 4))));
	}
	while (!digits.empty() && digits.front() == 0) {
		digits.erase(digits.begin());
		weight--;
	}
	while (!digits.empty() && digits.back() == 0) {
		digits.pop_back();
	}
	if (digits.empty()) {
		weight = 0;
		negative = false;
	}
	std::string out = Int16Bytes(static_cast<int16_t>(digits.size())) + Int16Bytes(weight) +
	                  Int16Bytes(negative ? 0x4000 : 0) + Int16Bytes(dscale);
	for (auto d : digits) {
		out += Int16Bytes(d);
	}
	return out;
}

std::string NumericText(const std::string &bin) {
	if (bin.size() < 8) {
		throw ProtocolError("bad binary numeric");
	}
	int ndigits = ReadInt16(bin, 0), weight = ReadInt16(bin, 2), sign = static_cast<uint16_t>(ReadInt16(bin, 4)),
	    dscale = ReadInt16(bin, 6);
	if (sign == 0xC000) {
		return "NaN";
	}
	std::vector<int> digits;
	for (int i = 0; i < ndigits; i++) {
		digits.push_back(ReadInt16(bin, 8 + 2 * i));
	}
	std::string int_part, frac_part;
	for (int i = 0; i <= weight; i++) {
		int d = i < ndigits ? digits[i] : 0;
		char buf[8];
		snprintf(buf, sizeof(buf), int_part.empty() ? "%d" : "%04d", d);
		int_part += buf;
	}
	if (int_part.empty()) {
		int_part = "0";
	}
	for (int i = weight + 1; static_cast<int>(frac_part.size()) < dscale; i++) {
		int d = i >= 0 && i < ndigits ? digits[i] : 0;
		char buf[8];
		snprintf(buf, sizeof(buf), "%04d", d);
		frac_part += buf;
	}
	frac_part = frac_part.substr(0, dscale);
	return std::string(sign == 0x4000 ? "-" : "") + int_part + (frac_part.empty() ? "" : "." + frac_part);
}

std::string HexBytes(const std::string &raw) {
	static const char *hex = "0123456789abcdef";
	std::string out = "\\x";
	for (unsigned char c : raw) {
		out.push_back(hex[c >> 4]);
		out.push_back(hex[c & 15]);
	}
	return out;
}

std::string UuidBytes(const std::string &text) {
	std::string out;
	int hi = -1;
	for (char c : text) {
		int v = std::isdigit(static_cast<unsigned char>(c)) ? c - '0'
		        : (c >= 'a' && c <= 'f')                    ? c - 'a' + 10
		        : (c >= 'A' && c <= 'F')                    ? c - 'A' + 10
		                                                    : -1;
		if (v < 0) {
			continue;
		}
		if (hi < 0) {
			hi = v;
		} else {
			out.push_back(static_cast<char>(hi * 16 + v));
			hi = -1;
		}
	}
	return out;
}

// One value in the format the client asked for; returns false for NULL.
bool Encode(const Value &value, const LogicalType &type, bool binary, std::string &out) {
	if (value.IsNull()) {
		return false;
	}
	auto pg = ToPg(type);
	if (!binary) {
		switch (pg.oid) {
		case oid::BOOL:
			out = value.GetValue<bool>() ? "t" : "f";
			return true;
		case oid::BYTEA:
			out = HexBytes(value.GetValueUnsafe<std::string>());
			return true;
		default:
			out = value.ToString();
			return true;
		}
	}
	switch (pg.oid) {
	case oid::BOOL:
		out = std::string(1, value.GetValue<bool>() ? 1 : 0);
		return true;
	case oid::INT2:
		out = Int16Bytes(value.GetValue<int16_t>());
		return true;
	case oid::INT4:
		out = Int32Bytes(value.GetValue<int32_t>());
		return true;
	case oid::INT8:
		out = Int64Bytes(value.GetValue<int64_t>());
		return true;
	case oid::FLOAT4: {
		float f = value.GetValue<float>();
		uint32_t bits;
		std::memcpy(&bits, &f, 4);
		out = Int32Bytes(static_cast<int32_t>(bits));
		return true;
	}
	case oid::FLOAT8: {
		double d = value.GetValue<double>();
		uint64_t bits;
		std::memcpy(&bits, &d, 8);
		out = Int64Bytes(static_cast<int64_t>(bits));
		return true;
	}
	case oid::NUMERIC:
		out = NumericBinary(value.ToString());
		return true;
	case oid::DATE:
		out = Int32Bytes(static_cast<int32_t>(value.GetValue<duckdb::date_t>().days - PG_EPOCH_DAYS));
		return true;
	case oid::TIME:
		out = Int64Bytes(value.GetValue<duckdb::dtime_t>().value);
		return true;
	case oid::TIMESTAMP:
	case oid::TIMESTAMPTZ: {
		auto ts = value.DefaultCastAs(type.id() == LogicalTypeId::TIMESTAMP_TZ ? LogicalType::TIMESTAMP_TZ
		                                                                       : LogicalType::TIMESTAMP);
		out = Int64Bytes(ts.GetValueUnsafe<int64_t>() - PG_EPOCH_MICROS);
		return true;
	}
	case oid::INTERVAL: {
		auto iv = value.GetValue<duckdb::interval_t>();
		out = Int64Bytes(iv.micros) + Int32Bytes(iv.days) + Int32Bytes(iv.months);
		return true;
	}
	case oid::UUID:
		out = UuidBytes(value.ToString());
		return true;
	case oid::BYTEA:
		out = value.GetValueUnsafe<std::string>();
		return true;
	default:
		out = value.ToString();
		return true;
	}
}

// A parameter value as the client sent it, in the type its OID names (VARCHAR when unspecified).
Value DecodeParam(const std::string &raw, int32_t type_oid, bool binary) {
	if (!binary) {
		if (type_oid == oid::BYTEA && raw.rfind("\\x", 0) == 0) {
			std::string bytes;
			for (size_t i = 2; i + 1 < raw.size(); i += 2) {
				bytes.push_back(static_cast<char>(std::stoi(raw.substr(i, 2), nullptr, 16)));
			}
			return Value::BLOB_RAW(bytes);
		}
		return Value(raw);
	}
	switch (type_oid) {
	case oid::BOOL:
		return Value::BOOLEAN(!raw.empty() && raw[0] != 0);
	case oid::INT2:
		return Value::SMALLINT(ReadInt16(raw, 0));
	case oid::INT4:
		return Value::INTEGER(ReadInt32(raw));
	case oid::INT8:
		return Value::BIGINT(ReadInt64(raw));
	case oid::FLOAT4: {
		auto bits = static_cast<uint32_t>(ReadInt32(raw));
		float f;
		std::memcpy(&f, &bits, 4);
		return Value::FLOAT(f);
	}
	case oid::FLOAT8: {
		auto bits = static_cast<uint64_t>(ReadInt64(raw));
		double d;
		std::memcpy(&d, &bits, 8);
		return Value::DOUBLE(d);
	}
	case oid::NUMERIC:
		return Value(NumericText(raw));
	case oid::DATE:
		return Value::DATE(duckdb::date_t(static_cast<int32_t>(ReadInt32(raw) + PG_EPOCH_DAYS)));
	case oid::TIME:
		return Value::TIME(duckdb::dtime_t(ReadInt64(raw)));
	case oid::TIMESTAMP:
		return Value::TIMESTAMP(duckdb::timestamp_t(ReadInt64(raw) + PG_EPOCH_MICROS));
	case oid::TIMESTAMPTZ:
		return Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(ReadInt64(raw) + PG_EPOCH_MICROS));
	case oid::UUID: {
		static const char *hex = "0123456789abcdef";
		std::string text;
		for (size_t i = 0; i < raw.size(); i++) {
			if (i == 4 || i == 6 || i == 8 || i == 10) {
				text.push_back('-');
			}
			text.push_back(hex[static_cast<uint8_t>(raw[i]) >> 4]);
			text.push_back(hex[static_cast<uint8_t>(raw[i]) & 15]);
		}
		return Value(text).DefaultCastAs(LogicalType::UUID);
	}
	case oid::BYTEA:
		return Value::BLOB_RAW(raw);
	default:
		return Value(raw); // text, varchar, unspecified: binary is the UTF-8 text itself
	}
}

// ---------------------------------------------------------------------------
// SQL text

// Splits a simple-query string into statements on top-level semicolons.
std::vector<std::string> SplitStatements(const std::string &sql) {
	std::vector<std::string> out;
	std::string current;
	size_t i = 0;
	auto flush = [&]() {
		auto b = current.find_first_not_of(" \t\r\n");
		if (b != std::string::npos) {
			out.push_back(current);
		}
		current.clear();
	};
	while (i < sql.size()) {
		char c = sql[i];
		if (c == '\'' || c == '"') {
			auto end = i + 1;
			while (end < sql.size()) {
				if (sql[end] == c) {
					if (end + 1 < sql.size() && sql[end + 1] == c) {
						end += 2;
						continue;
					}
					break;
				}
				end++;
			}
			current += sql.substr(i, end + 1 - i);
			i = end + 1;
		} else if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
			auto end = sql.find('\n', i);
			end = end == std::string::npos ? sql.size() : end;
			current += sql.substr(i, end - i);
			i = end;
		} else if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
			auto end = sql.find("*/", i + 2);
			end = end == std::string::npos ? sql.size() : end + 2;
			current += sql.substr(i, end - i);
			i = end;
		} else if (c == '$') {
			// A dollar-quoted string: $tag$ ... $tag$
			auto close = sql.find('$', i + 1);
			bool tag_ok = close != std::string::npos;
			for (size_t k = i + 1; tag_ok && k < close; k++) {
				tag_ok = std::isalnum(static_cast<unsigned char>(sql[k])) || sql[k] == '_';
			}
			if (tag_ok) {
				auto tag = sql.substr(i, close - i + 1);
				if (tag.size() > 2 && std::isdigit(static_cast<unsigned char>(tag[1]))) {
					current.push_back(c); // $1: a parameter, not a quote
					i++;
					continue;
				}
				auto end = sql.find(tag, close + 1);
				end = end == std::string::npos ? sql.size() : end + tag.size();
				current += sql.substr(i, end - i);
				i = end;
			} else {
				current.push_back(c);
				i++;
			}
		} else if (c == ';') {
			flush();
			i++;
		} else {
			current.push_back(c);
			i++;
		}
	}
	flush();
	return out;
}

// Lower case with runs of whitespace as one space, trimmed, without a trailing semicolon.
std::string Normalize(const std::string &sql) {
	std::string out;
	bool space = false;
	for (char c : sql) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			space = !out.empty();
			continue;
		}
		if (space) {
			out.push_back(' ');
			space = false;
		}
		out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
	}
	while (!out.empty() && (out.back() == ';' || out.back() == ' ')) {
		out.pop_back();
	}
	return out;
}

bool StartsWithWord(const std::string &norm, const std::string &word) {
	return norm == word || norm.rfind(word + " ", 0) == 0;
}

std::string FirstWordUpper(const std::string &norm) {
	auto word = norm.substr(0, norm.find(' '));
	std::transform(word.begin(), word.end(), word.begin(), ::toupper);
	return word;
}

// The run-time parameters PostgreSQL clients ask for, with PostgreSQL's answers.
const std::map<std::string, std::string> &SessionParameters() {
	static const std::map<std::string, std::string> params {
	    {"server_version", "16.0"},
	    {"server_version_num", "160000"},
	    {"server_encoding", "UTF8"},
	    {"client_encoding", "UTF8"},
	    {"datestyle", "ISO, MDY"},
	    {"intervalstyle", "postgres"},
	    {"timezone", "UTC"},
	    {"integer_datetimes", "on"},
	    {"standard_conforming_strings", "on"},
	    {"transaction_isolation", "read committed"},
	    {"transaction isolation level", "read committed"},
	    {"default_transaction_isolation", "read committed"},
	    {"transaction_read_only", "off"},
	    {"default_transaction_read_only", "off"},
	    {"max_identifier_length", "63"},
	    {"lc_collate", "C"},
	    {"lc_ctype", "C"},
	    {"is_superuser", "off"},
	    {"extra_float_digits", "3"},
	    {"application_name", ""},
	    {"statement_timeout", "0"},
	    {"lock_timeout", "0"},
	    {"bytea_output", "hex"},
	};
	return params;
}

// Statements answered by the session itself rather than by DuckDB.
struct Special {
	enum class Kind { NONE, EMPTY, IGNORE, SHOW, BEGIN, COMMIT, ROLLBACK };
	Kind kind = Kind::NONE;
	std::string tag;
	std::string name, value; // SHOW
};

Special Classify(const std::string &sql) {
	Special s;
	auto norm = Normalize(sql);
	// Strip leading comments.
	while (norm.rfind("--", 0) == 0 || norm.rfind("/*", 0) == 0) {
		auto end = norm.rfind("--", 0) == 0 ? norm.find('\n') : norm.find("*/");
		norm = end == std::string::npos ? "" : Normalize(norm.substr(end + (norm[0] == '-' ? 1 : 2)));
	}
	if (norm.empty()) {
		s.kind = Special::Kind::EMPTY;
		return s;
	}
	if (StartsWithWord(norm, "set") || StartsWithWord(norm, "reset") || StartsWithWord(norm, "discard") ||
	    StartsWithWord(norm, "deallocate") || StartsWithWord(norm, "savepoint") || StartsWithWord(norm, "release") ||
	    StartsWithWord(norm, "listen") || StartsWithWord(norm, "unlisten") || norm.rfind("rollback to", 0) == 0) {
		// Session settings: DuckDB's are locked, and a PostgreSQL client's are not DuckDB's anyway.
		s.kind = Special::Kind::IGNORE;
		s.tag = norm.rfind("discard all", 0) == 0 ? "DISCARD ALL" : FirstWordUpper(norm);
		return s;
	}
	if (StartsWithWord(norm, "show")) {
		auto name = norm.substr(5);
		auto &params = SessionParameters();
		auto it = params.find(name);
		if (it != params.end()) {
			s.kind = Special::Kind::SHOW;
			s.name = name == "transaction isolation level" ? "transaction_isolation" : name;
			s.value = it->second;
			s.tag = "SHOW";
		}
		return s;
	}
	if (StartsWithWord(norm, "begin") || norm.rfind("start transaction", 0) == 0) {
		s.kind = Special::Kind::BEGIN;
		s.tag = "BEGIN";
	} else if (StartsWithWord(norm, "commit") || StartsWithWord(norm, "end")) {
		s.kind = Special::Kind::COMMIT;
		s.tag = "COMMIT";
	} else if (StartsWithWord(norm, "rollback") || StartsWithWord(norm, "abort")) {
		s.kind = Special::Kind::ROLLBACK;
		s.tag = "ROLLBACK";
	}
	return s;
}

const char *SqlState(duckdb::ExceptionType type) {
	using duckdb::ExceptionType;
	switch (type) {
	case ExceptionType::PARSER:
		return "42601"; // syntax_error
	case ExceptionType::CATALOG:
		return "42P01"; // undefined_table
	case ExceptionType::BINDER:
		return "42703"; // undefined_column
	case ExceptionType::CONSTRAINT:
		return "23505";
	case ExceptionType::INTERRUPT:
		return "57014"; // query_canceled
	case ExceptionType::PERMISSION:
		return "42501";
	case ExceptionType::CONVERSION:
	case ExceptionType::INVALID_INPUT:
		return "22023";
	case ExceptionType::TRANSACTION:
		return "25P02";
	case ExceptionType::NOT_IMPLEMENTED:
		return "0A000";
	default:
		return "XX000";
	}
}

} // namespace

// ---------------------------------------------------------------------------
// A session: one client connection

class PgSession {
public:
	PgSession(PgServer &server, int fd) : server_(server), fd_(fd) {
	}
	~PgSession() {
		if (pid_) {
			server_.Unregister(pid_);
		}
		::close(fd_);
	}
	void Run();
	void Interrupt() {
		if (con_) {
			con_->Interrupt();
		}
	}

private:
	struct Statement {
		std::string sql;
		Special special;
		std::unique_ptr<duckdb::PreparedStatement> prepared;
		std::vector<int32_t> param_oids;
	};
	struct Portal {
		std::shared_ptr<Statement> statement;
		duckdb::vector<Value> params;
		std::vector<int16_t> result_formats;
		std::unique_ptr<duckdb::QueryResultStream<>> stream;
		std::unique_ptr<duckdb::DataChunk> chunk;
		idx_t offset = 0;
		size_t rows_sent = 0;
		bool started = false;
	};

	bool Startup();
	bool ReadMessage(char &type, std::string &body);
	void SendError(const std::string &code, const std::string &message);
	void SendErrorFrom(const duckdb::ErrorData &error);
	void ReadyForQuery();
	void ParameterStatus(const std::string &name, const std::string &value);

	void SimpleQuery(const std::string &sql);
	std::shared_ptr<Statement> Prepare(const std::string &sql, std::vector<int32_t> param_oids);
	void RowDescription(const Statement &statement, const std::vector<int16_t> &formats);
	bool HasRows(const Statement &statement) const;
	// Runs (or continues) a portal; false on error (already reported).
	bool Execute(Portal &portal, int32_t max_rows);
	bool RunSpecial(const Special &special);
	duckdb::vector<Value> BindParams(const Statement &statement, const std::vector<std::string> &raw,
	                                 const std::vector<bool> &nulls, const std::vector<int16_t> &formats);

	PgServer &server_;
	int fd_;
	int32_t pid_ = 0;
	int32_t secret_ = 0;
	std::string database_; // the database the client asked for, which current_database() reports
	std::unique_ptr<duckdb::Connection> con_;
	Writer out_;
	bool failed_transaction_ = false;
	std::map<std::string, std::shared_ptr<Statement>> statements_;
	std::map<std::string, Portal> portals_;
};

bool PgSession::ReadMessage(char &type, std::string &body) {
	if (!ReadExact(fd_, &type, 1)) {
		return false;
	}
	uint32_t len;
	if (!ReadExact(fd_, &len, 4)) {
		return false;
	}
	len = ntohl(len);
	if (len < 4 || len > (1u << 30)) {
		return false;
	}
	body.resize(len - 4);
	return body.empty() || ReadExact(fd_, &body[0], body.size());
}

void PgSession::SendError(const std::string &code, const std::string &message) {
	out_.Begin('E');
	out_.Byte('S');
	out_.CString("ERROR");
	out_.Byte('V');
	out_.CString("ERROR");
	out_.Byte('C');
	out_.CString(code);
	out_.Byte('M');
	out_.CString(message);
	out_.Byte('\0');
	out_.End();
}

void PgSession::SendErrorFrom(const duckdb::ErrorData &error) {
	if (con_ && con_->HasActiveTransaction()) {
		failed_transaction_ = true;
	}
	auto message = error.RawMessage();
	auto pos = message.find("\n\nLINE ");
	SendError(SqlState(error.Type()), pos == std::string::npos ? message : message.substr(0, pos));
}

void PgSession::ParameterStatus(const std::string &name, const std::string &value) {
	out_.Begin('S');
	out_.CString(name);
	out_.CString(value);
	out_.End();
}

void PgSession::ReadyForQuery() {
	out_.Begin('Z');
	char status = 'I';
	if (con_ && con_->HasActiveTransaction()) {
		status = failed_transaction_ ? 'E' : 'T';
	}
	out_.Byte(status);
	out_.End();
	out_.Flush(fd_);
}

bool PgSession::Startup() {
	for (;;) {
		uint32_t len;
		if (!ReadExact(fd_, &len, 4)) {
			return false;
		}
		len = ntohl(len);
		if (len < 8 || len > 10000) {
			return false;
		}
		std::string body(len - 4, '\0');
		if (!ReadExact(fd_, &body[0], body.size())) {
			return false;
		}
		Reader in(body);
		auto code = in.Int32();
		if (code == 80877103 || code == 80877104) { // SSLRequest, GSSENCRequest: plain only
			if (!WriteAll(fd_, "N", 1)) {
				return false;
			}
			continue;
		}
		if (code == 80877102) { // CancelRequest, on its own connection
			auto pid = in.Int32();
			auto secret = in.Int32();
			server_.Cancel(pid, secret);
			return false;
		}
		if ((code >> 16) != 3) {
			SendError("0A000", "unsupported protocol version");
			out_.Flush(fd_);
			return false;
		}
		std::map<std::string, std::string> params;
		for (;;) {
			auto key = in.CString();
			if (key.empty()) {
				break;
			}
			params[key] = in.CString();
		}
		// Password authentication: the password is the server's token; any user name.
		out_.Begin('R');
		out_.Int32(3); // AuthenticationCleartextPassword
		out_.End();
		out_.Flush(fd_);
		char type;
		std::string reply;
		if (!ReadMessage(type, reply) || type != 'p') {
			return false;
		}
		auto password = reply.substr(0, reply.find('\0'));
		auto &token = server_.Token();
		unsigned char diff = password.size() == token.size() ? 0 : 1;
		for (size_t i = 0; i < token.size(); i++) {
			diff |= static_cast<unsigned char>(token[i] ^ (i < password.size() ? password[i] : 0));
		}
		if (diff != 0) {
			SendError("28P01", "password authentication failed for user \"" + params["user"] + "\"");
			out_.Flush(fd_);
			return false;
		}
		con_ = std::make_unique<duckdb::Connection>(server_.Database());
		// PostgreSQL's default database is the user name. Clients such as DataGrip check that
		// current_database() is the one they connected to.
		database_ = params.count("database") && !params["database"].empty() ? params["database"] : params["user"];
		out_.Begin('R');
		out_.Int32(0); // AuthenticationOk
		out_.End();
		auto &session = SessionParameters();
		for (auto name : {"server_version", "server_encoding", "client_encoding", "DateStyle", "IntervalStyle",
		                  "TimeZone", "integer_datetimes", "standard_conforming_strings", "is_superuser"}) {
			std::string key = name;
			std::transform(key.begin(), key.end(), key.begin(), ::tolower);
			ParameterStatus(name, session.at(key));
		}
		ParameterStatus("application_name", params.count("application_name") ? params["application_name"] : "");
		ParameterStatus("session_authorization", params["user"]);
		std::random_device rd;
		secret_ = static_cast<int32_t>(rd());
		pid_ = server_.Register(this, secret_);
		out_.Begin('K');
		out_.Int32(pid_);
		out_.Int32(secret_);
		out_.End();
		ReadyForQuery();
		return true;
	}
}

std::shared_ptr<PgSession::Statement> PgSession::Prepare(const std::string &sql, std::vector<int32_t> param_oids) {
	static const bool log_sql = std::getenv("DUCKGRAPHLAKE_LOG_SQL") != nullptr;
	if (log_sql) {
		Log("pg sql: " + sql);
	}
	auto statement = std::make_shared<Statement>();
	statement->sql = sql;
	statement->special = Classify(sql);
	statement->param_oids = std::move(param_oids);
	if (statement->special.kind == Special::Kind::NONE) {
		auto session_sql = ReplaceSessionFunctions(sql, database_, pid_);
		auto rewritten = RewriteCatalogQuery(session_sql);
		statement->prepared = con_->Prepare(rewritten ? *rewritten : session_sql);
		if (statement->prepared->HasError()) {
			SendErrorFrom(statement->prepared->GetErrorObject());
			return nullptr;
		}
	}
	return statement;
}

bool PgSession::HasRows(const Statement &statement) const {
	if (statement.special.kind == Special::Kind::SHOW) {
		return true;
	}
	if (!statement.prepared) {
		return false;
	}
	return statement.prepared->GetStatementProperties().return_type == duckdb::StatementReturnType::QUERY_RESULT;
}

void PgSession::RowDescription(const Statement &statement, const std::vector<int16_t> &formats) {
	auto format_of = [&](size_t i) -> int16_t {
		if (formats.empty()) {
			return 0;
		}
		return formats.size() == 1 ? formats[0] : (i < formats.size() ? formats[i] : 0);
	};
	out_.Begin('T');
	if (statement.special.kind == Special::Kind::SHOW) {
		out_.Int16(1);
		out_.CString(statement.special.name);
		out_.Int32(0);
		out_.Int16(0);
		out_.Int32(oid::TEXT);
		out_.Int16(-1);
		out_.Int32(-1);
		out_.Int16(format_of(0));
		out_.End();
		return;
	}
	auto &types = statement.prepared->GetTypes();
	auto &names = statement.prepared->GetNames();
	out_.Int16(static_cast<int16_t>(types.size()));
	for (size_t i = 0; i < types.size(); i++) {
		auto pg = ToPg(types[i]);
		out_.CString(names[i].GetIdentifierName());
		out_.Int32(0); // table OID
		out_.Int16(0); // column number
		out_.Int32(pg.oid);
		out_.Int16(pg.len);
		out_.Int32(-1); // type modifier
		out_.Int16(format_of(i));
	}
	out_.End();
}

bool PgSession::RunSpecial(const Special &special) {
	switch (special.kind) {
	case Special::Kind::BEGIN:
		if (!con_->HasActiveTransaction()) {
			auto r = con_->Query("BEGIN TRANSACTION");
			if (r->HasError()) {
				SendErrorFrom(r->GetErrorObject());
				return false;
			}
		}
		break;
	case Special::Kind::COMMIT:
	case Special::Kind::ROLLBACK: {
		std::string tag = special.tag;
		if (con_->HasActiveTransaction()) {
			// A failed transaction can only be rolled back; PostgreSQL reports COMMIT as ROLLBACK then.
			bool rollback = special.kind == Special::Kind::ROLLBACK || failed_transaction_;
			auto r = con_->Query(rollback ? "ROLLBACK" : "COMMIT");
			if (r->HasError()) {
				SendErrorFrom(r->GetErrorObject());
				return false;
			}
			tag = rollback ? "ROLLBACK" : "COMMIT";
		}
		failed_transaction_ = false;
		out_.Begin('C');
		out_.CString(tag);
		out_.End();
		return true;
	}
	case Special::Kind::SHOW: {
		out_.Begin('D');
		out_.Int16(1);
		out_.Int32(static_cast<int32_t>(special.value.size()));
		out_.Raw(special.value);
		out_.End();
		out_.Begin('C');
		out_.CString("SHOW");
		out_.End();
		return true;
	}
	default:
		break;
	}
	out_.Begin('C');
	out_.CString(special.tag);
	out_.End();
	return true;
}

bool PgSession::Execute(Portal &portal, int32_t max_rows) {
	auto &statement = *portal.statement;
	if (statement.special.kind == Special::Kind::EMPTY) {
		out_.Begin('I'); // EmptyQueryResponse
		out_.End();
		return true;
	}
	if (statement.special.kind != Special::Kind::NONE) {
		return RunSpecial(statement.special);
	}
	auto &types = statement.prepared->GetTypes();
	auto format_of = [&](size_t i) -> bool {
		auto &f = portal.result_formats;
		if (f.empty()) {
			return false;
		}
		return (f.size() == 1 ? f[0] : (i < f.size() ? f[i] : 0)) == 1;
	};
	try {
		if (!portal.started) {
			portal.started = true;
			auto result = statement.prepared->Submit(portal.params);
			if (result->HasError()) {
				SendErrorFrom(result->GetErrorObject());
				return false;
			}
			portal.stream = std::make_unique<duckdb::QueryResultStream<>>(std::move(result));
		}
		auto return_type = statement.prepared->GetStatementProperties().return_type;
		if (return_type != duckdb::StatementReturnType::QUERY_RESULT) {
			int64_t changed = 0;
			while (auto chunk = portal.stream->Fetch()) {
				if (chunk->size() > 0 && chunk->ColumnCount() > 0 &&
				    return_type == duckdb::StatementReturnType::CHANGED_ROWS) {
					changed += chunk->GetValue(0, 0).GetValue<int64_t>();
				}
			}
			if (portal.stream->HasError()) {
				SendErrorFrom(portal.stream->GetErrorObject());
				return false;
			}
			std::string tag;
			switch (statement.prepared->GetStatementType()) {
			case duckdb::StatementType::INSERT_STATEMENT:
				tag = "INSERT 0 " + std::to_string(changed);
				break;
			case duckdb::StatementType::UPDATE_STATEMENT:
				tag = "UPDATE " + std::to_string(changed);
				break;
			case duckdb::StatementType::DELETE_STATEMENT:
				tag = "DELETE " + std::to_string(changed);
				break;
			default:
				tag = FirstWordUpper(Normalize(statement.sql));
			}
			out_.Begin('C');
			out_.CString(tag);
			out_.End();
			return true;
		}
		std::string value;
		size_t sent_now = 0;
		for (;;) {
			if (!portal.chunk || portal.offset >= portal.chunk->size()) {
				portal.chunk = portal.stream->Fetch();
				portal.offset = 0;
				if (!portal.chunk) {
					break;
				}
				continue;
			}
			if (max_rows > 0 && sent_now == static_cast<size_t>(max_rows)) {
				out_.Begin('s'); // PortalSuspended: the client asks for more with another Execute
				out_.End();
				return true;
			}
			auto &chunk = *portal.chunk;
			auto r = portal.offset++;
			out_.Begin('D');
			out_.Int16(static_cast<int16_t>(chunk.ColumnCount()));
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				if (Encode(chunk.GetValue(c, r), types[c], format_of(c), value)) {
					out_.Int32(static_cast<int32_t>(value.size()));
					out_.Raw(value);
				} else {
					out_.Int32(-1);
				}
			}
			out_.End();
			sent_now++;
			portal.rows_sent++;
			if (out_.Size() > (1 << 20)) {
				out_.Flush(fd_);
			}
		}
		if (portal.stream->HasError()) {
			SendErrorFrom(portal.stream->GetErrorObject());
			return false;
		}
		out_.Begin('C');
		out_.CString("SELECT " + std::to_string(portal.rows_sent));
		out_.End();
		return true;
	} catch (std::exception &ex) {
		SendErrorFrom(duckdb::ErrorData(ex));
		return false;
	}
}

duckdb::vector<Value> PgSession::BindParams(const Statement &statement, const std::vector<std::string> &raw,
                                            const std::vector<bool> &nulls, const std::vector<int16_t> &formats) {
	duckdb::vector<Value> values;
	auto expected = statement.prepared ? statement.prepared->GetExpectedParameterTypes()
	                                   : duckdb::case_insensitive_map_t<LogicalType>();
	for (size_t i = 0; i < raw.size(); i++) {
		auto type_oid = i < statement.param_oids.size() ? statement.param_oids[i] : 0;
		bool binary = !formats.empty() && (formats.size() == 1 ? formats[0] : formats[i]) == 1;
		LogicalType target = LogicalType::INVALID;
		auto it = expected.find(std::to_string(i + 1));
		if (it != expected.end() && UsableType(it->second)) {
			target = it->second;
		} else {
			target = FromPg(type_oid);
		}
		if (nulls[i]) {
			values.push_back(UsableType(target) ? Value(target) : Value());
			continue;
		}
		auto text = raw[i];
		if (!binary && (target.id() == LogicalTypeId::DATE || target.id() == LogicalTypeId::TIMESTAMP)) {
			// Drivers send dates and timestamps with the client's UTC offset ("2024-02-28 +01"), which
			// PostgreSQL ignores for these types.
			static const std::regex date_part(R"(^\s*(\d{4,}-\d{2}-\d{2}))");
			static const std::regex offset(R"(\s*[+-]\d{2}(:?\d{2}){0,2}\s*$)");
			std::smatch m;
			if (target.id() == LogicalTypeId::DATE && std::regex_search(text, m, date_part)) {
				text = m[1].str();
			} else if (target.id() == LogicalTypeId::TIMESTAMP && text.size() > 10) {
				text = std::regex_replace(text, offset, "");
			}
		}
		auto v = DecodeParam(text, type_oid, binary);
		if (UsableType(target) && v.type() != target) {
			std::string error;
			auto cast = v.DefaultTryCastAs(target, &error);
			if (!cast) {
				throw duckdb::InvalidInputException("parameter $%d: %s", static_cast<int>(i + 1), error);
			}
			v = *cast;
		}
		values.push_back(v);
	}
	return values;
}

void PgSession::SimpleQuery(const std::string &sql) {
	auto statements = SplitStatements(sql);
	if (statements.empty()) {
		out_.Begin('I');
		out_.End();
	}
	for (auto &text : statements) {
		auto statement = Prepare(text, {});
		if (!statement) {
			break;
		}
		Portal portal;
		portal.statement = statement;
		if (HasRows(*statement)) {
			RowDescription(*statement, {});
		}
		if (!Execute(portal, 0)) {
			break;
		}
	}
	ReadyForQuery();
}

void PgSession::Run() {
	try {
		if (!Startup()) {
			return;
		}
		bool skip_until_sync = false;
		char type;
		std::string body;
		while (ReadMessage(type, body)) {
			if (skip_until_sync && type != 'S') {
				continue;
			}
			Reader in(body);
			switch (type) {
			case 'Q':
				SimpleQuery(in.CString());
				break;
			case 'P': { // Parse
				auto name = in.CString();
				auto sql = in.CString();
				std::vector<int32_t> oids(static_cast<size_t>(in.Int16()));
				for (auto &o : oids) {
					o = in.Int32();
				}
				auto statement = Prepare(sql, oids);
				if (!statement) {
					skip_until_sync = true;
					break;
				}
				statements_[name] = statement;
				out_.Begin('1');
				out_.End();
				break;
			}
			case 'B': { // Bind
				auto portal_name = in.CString();
				auto statement_name = in.CString();
				std::vector<int16_t> param_formats(static_cast<size_t>(in.Int16()));
				for (auto &f : param_formats) {
					f = in.Int16();
				}
				auto n = static_cast<size_t>(in.Int16());
				std::vector<std::string> raw(n);
				std::vector<bool> nulls(n);
				for (size_t i = 0; i < n; i++) {
					auto len = in.Int32();
					nulls[i] = len < 0;
					if (len >= 0) {
						raw[i] = in.Bytes(static_cast<size_t>(len));
					}
				}
				std::vector<int16_t> result_formats(static_cast<size_t>(in.Int16()));
				for (auto &f : result_formats) {
					f = in.Int16();
				}
				auto it = statements_.find(statement_name);
				if (it == statements_.end()) {
					SendError("26000", "prepared statement \"" + statement_name + "\" does not exist");
					skip_until_sync = true;
					break;
				}
				Portal portal;
				portal.statement = it->second;
				portal.result_formats = result_formats;
				try {
					portal.params = BindParams(*it->second, raw, nulls, param_formats);
				} catch (std::exception &ex) {
					SendErrorFrom(duckdb::ErrorData(ex));
					skip_until_sync = true;
					break;
				}
				portals_[portal_name] = std::move(portal);
				out_.Begin('2');
				out_.End();
				break;
			}
			case 'D': { // Describe
				auto what = in.Byte();
				auto name = in.CString();
				if (what == 'S') {
					auto it = statements_.find(name);
					if (it == statements_.end()) {
						SendError("26000", "prepared statement \"" + name + "\" does not exist");
						skip_until_sync = true;
						break;
					}
					auto &statement = *it->second;
					idx_t count = statement.prepared ? statement.prepared->GetParameterCount() : 0;
					auto expected = statement.prepared ? statement.prepared->GetExpectedParameterTypes()
					                                   : duckdb::case_insensitive_map_t<LogicalType>();
					out_.Begin('t');
					out_.Int16(static_cast<int16_t>(count));
					for (idx_t i = 0; i < count; i++) {
						int32_t type_oid = i < statement.param_oids.size() ? statement.param_oids[i] : 0;
						if (type_oid == 0) {
							auto e = expected.find(std::to_string(i + 1));
							type_oid = e != expected.end() && UsableType(e->second) ? ToPg(e->second).oid : oid::TEXT;
						}
						out_.Int32(type_oid);
					}
					out_.End();
					if (HasRows(statement)) {
						RowDescription(statement, {});
					} else {
						out_.Begin('n'); // NoData
						out_.End();
					}
				} else {
					auto it = portals_.find(name);
					if (it == portals_.end()) {
						SendError("34000", "portal \"" + name + "\" does not exist");
						skip_until_sync = true;
						break;
					}
					if (HasRows(*it->second.statement)) {
						RowDescription(*it->second.statement, it->second.result_formats);
					} else {
						out_.Begin('n');
						out_.End();
					}
				}
				break;
			}
			case 'E': { // Execute
				auto name = in.CString();
				auto max_rows = in.Int32();
				auto it = portals_.find(name);
				if (it == portals_.end()) {
					SendError("34000", "portal \"" + name + "\" does not exist");
					skip_until_sync = true;
					break;
				}
				if (!Execute(it->second, max_rows)) {
					skip_until_sync = true;
				}
				break;
			}
			case 'C': { // Close
				auto what = in.Byte();
				auto name = in.CString();
				if (what == 'S') {
					statements_.erase(name);
				} else {
					portals_.erase(name);
				}
				out_.Begin('3');
				out_.End();
				break;
			}
			case 'S': // Sync
				skip_until_sync = false;
				portals_.erase("");
				ReadyForQuery();
				break;
			case 'H': // Flush
				out_.Flush(fd_);
				break;
			case 'X': // Terminate
				return;
			default:
				SendError("08P01", std::string("unsupported message type '") + type + "'");
				out_.Flush(fd_);
				return;
			}
		}
	} catch (std::exception &ex) {
		SendError("08P01", ex.what());
		out_.Flush(fd_);
	}
}

// ---------------------------------------------------------------------------
// Server

PgServer::PgServer(duckdb::DuckDB &db, std::string token) : db_(db), token_(std::move(token)) {
}

PgServer::~PgServer() {
	Stop();
}

void PgServer::Start(const std::string &host, int port) {
	addrinfo hints {};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	addrinfo *found = nullptr;
	if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &found) != 0 || !found) {
		throw std::runtime_error("cannot resolve " + host);
	}
	listen_fd_ = ::socket(found->ai_family, found->ai_socktype, found->ai_protocol);
	int yes = 1;
	setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	bool ok =
	    listen_fd_ >= 0 && ::bind(listen_fd_, found->ai_addr, found->ai_addrlen) == 0 && ::listen(listen_fd_, 64) == 0;
	freeaddrinfo(found);
	if (!ok) {
		throw std::runtime_error("cannot listen on " + host + ":" + std::to_string(port) + ": " + strerror(errno));
	}
	accept_thread_ = std::thread([this]() { AcceptLoop(); });
}

void PgServer::Stop() {
	if (stopping_.exchange(true)) {
		return;
	}
	if (listen_fd_ >= 0) {
		::shutdown(listen_fd_, SHUT_RDWR);
		::close(listen_fd_);
	}
	if (accept_thread_.joinable()) {
		accept_thread_.join();
	}
}

void PgServer::AcceptLoop() {
	while (!stopping_) {
		int fd = ::accept(listen_fd_, nullptr, nullptr);
		if (fd < 0) {
			if (stopping_) {
				return;
			}
			continue;
		}
		int yes = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
#ifdef SO_NOSIGPIPE
		setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof(yes));
#endif
		std::thread([this, fd]() {
			PgSession session(*this, fd);
			session.Run();
		}).detach();
	}
}

int32_t PgServer::Register(PgSession *session, int32_t secret) {
	std::lock_guard<std::mutex> lock(sessions_mutex_);
	auto pid = next_pid_++;
	sessions_[pid] = {session, secret};
	return pid;
}

void PgServer::Unregister(int32_t pid) {
	std::lock_guard<std::mutex> lock(sessions_mutex_);
	sessions_.erase(pid);
}

void PgServer::Cancel(int32_t pid, int32_t secret) {
	std::lock_guard<std::mutex> lock(sessions_mutex_);
	auto it = sessions_.find(pid);
	if (it != sessions_.end() && it->second.second == secret) {
		it->second.first->Interrupt();
	}
}

} // namespace dgl
