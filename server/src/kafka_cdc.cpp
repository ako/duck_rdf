#include "kafka_cdc.hpp"

#include "json.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <stdexcept>

namespace dgl {

const char *CDC_TYPE = "STRUCT(op VARCHAR, ts TIMESTAMP, source_ts TIMESTAMP, tx_id VARCHAR, \"sequence\" BIGINT, "
                       "snapshot BOOLEAN, connector VARCHAR, version VARCHAR)";

namespace {

std::string Base64Decode(const std::string &in) {
	auto value = [](char c) -> int {
		if (c >= 'A' && c <= 'Z') {
			return c - 'A';
		}
		if (c >= 'a' && c <= 'z') {
			return c - 'a' + 26;
		}
		if (c >= '0' && c <= '9') {
			return c - '0' + 52;
		}
		if (c == '+' || c == '-') {
			return 62;
		}
		if (c == '/' || c == '_') {
			return 63;
		}
		return -1;
	};
	std::string out;
	uint32_t buffer = 0;
	int bits = 0;
	for (char c : in) {
		auto v = value(c);
		if (v < 0) {
			if (c == '=') {
				break;
			}
			throw std::runtime_error("not base64");
		}
		buffer = (buffer << 6) | static_cast<uint32_t>(v);
		bits += 6;
		if (bits >= 8) {
			bits -= 8;
			out.push_back(static_cast<char>((buffer >> bits) & 0xFF));
		}
	}
	return out;
}

// Days since 1970-01-01 as y-m-d (H. Hinnant's civil_from_days).
std::string CivilDate(int64_t days) {
	days += 719468;
	int64_t era = (days >= 0 ? days : days - 146096) / 146097;
	auto doe = static_cast<unsigned>(days - era * 146097);
	unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
	int64_t y = static_cast<int64_t>(yoe) + era * 400;
	unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
	unsigned mp = (5 * doy + 2) / 153;
	unsigned d = doy - (153 * mp + 2) / 5 + 1;
	unsigned m = mp < 10 ? mp + 3 : mp - 9;
	y += m <= 2;
	char buf[32];
	snprintf(buf, sizeof(buf), "%04lld-%02u-%02u", static_cast<long long>(y), m, d);
	return buf;
}

// Microseconds since the epoch as an ISO timestamp (UTC, no zone).
std::string IsoTimestamp(int64_t micros) {
	int64_t days = micros / 86400000000LL;
	int64_t rest = micros % 86400000000LL;
	if (rest < 0) {
		rest += 86400000000LL;
		days -= 1;
	}
	char buf[48];
	snprintf(buf, sizeof(buf), "%s %02lld:%02lld:%02lld.%06lld", CivilDate(days).c_str(),
	         static_cast<long long>(rest / 3600000000LL), static_cast<long long>(rest / 60000000LL % 60),
	         static_cast<long long>(rest / 1000000LL % 60), static_cast<long long>(rest % 1000000LL));
	return buf;
}

std::string StringParam(const Json &schema, const std::string &key) {
	auto params = schema.Get("parameters");
	auto v = params ? params->Get(key) : nullptr;
	if (!v) {
		return "";
	}
	return v->IsString() ? v->AsString() : v->Dump();
}

int IntParam(const Json &schema, const std::string &key, int fallback) {
	auto s = StringParam(schema, key);
	try {
		return s.empty() ? fallback : std::stoi(s);
	} catch (std::exception &) {
		return fallback;
	}
}

std::string SchemaName(const Json &schema) {
	auto n = schema.Get("name");
	return n && n->IsString() ? n->AsString() : "";
}

std::string SchemaType(const Json &schema) {
	auto t = schema.Get("type");
	return t && t->IsString() ? t->AsString() : "";
}

std::string Quote(const std::string &name) {
	std::string out = "\"";
	for (char c : name) {
		out += c;
		if (c == '"') {
			out += '"';
		}
	}
	return out + "\"";
}

// A Kafka Connect schema as a DuckDB type.
std::string DuckType(const Json &schema) {
	auto name = SchemaName(schema);
	auto type = SchemaType(schema);
	if (name == "org.apache.kafka.connect.data.Decimal") {
		auto scale = IntParam(schema, "scale", 0);
		auto precision = std::min(IntParam(schema, "connect.decimal.precision", 38), 38);
		precision = std::max(precision, std::max(scale, 1));
		return "DECIMAL(" + std::to_string(precision) + ", " + std::to_string(scale) + ")";
	}
	if (name == "org.apache.kafka.connect.data.Timestamp" || name == "io.debezium.time.Timestamp" ||
	    name == "io.debezium.time.MicroTimestamp" || name == "io.debezium.time.NanoTimestamp") {
		return "TIMESTAMP";
	}
	if (name == "io.debezium.time.ZonedTimestamp") {
		return "TIMESTAMPTZ";
	}
	if (name == "org.apache.kafka.connect.data.Date" || name == "io.debezium.time.Date") {
		return "DATE";
	}
	if (name == "io.debezium.data.Uuid") {
		return "UUID";
	}
	if (type == "int8") {
		return "TINYINT";
	}
	if (type == "int16") {
		return "SMALLINT";
	}
	if (type == "int32") {
		return "INTEGER";
	}
	if (type == "int64") {
		return "BIGINT";
	}
	if (type == "float32" || type == "float") {
		return "FLOAT";
	}
	if (type == "float64" || type == "double") {
		return "DOUBLE";
	}
	if (type == "boolean") {
		return "BOOLEAN";
	}
	if (type == "array") {
		auto items = schema.Get("items");
		return (items ? DuckType(*items) : std::string("JSON")) + "[]";
	}
	if (type == "struct") {
		auto fields = schema.Get("fields");
		if (!fields || !fields->IsArray() || fields->Size() == 0) {
			return "JSON";
		}
		std::string out = "STRUCT(";
		bool first = true;
		for (auto &f : fields->Items()) {
			auto fname = f.Get("field");
			if (!fname || !fname->IsString()) {
				continue;
			}
			out += (first ? "" : ", ") + Quote(fname->AsString()) + " " + DuckType(f);
			first = false;
		}
		return first ? "JSON" : out + ")";
	}
	if (type == "map") {
		return "JSON";
	}
	return "VARCHAR"; // string, bytes (kept as base64 text)
}

// A Kafka Connect value in the form json_transform turns into DuckType(schema).
Json Convert(const Json &value, const Json &schema) {
	if (value.IsNull()) {
		return value;
	}
	auto name = SchemaName(schema);
	auto type = SchemaType(schema);
	try {
		if (name == "org.apache.kafka.connect.data.Decimal" && value.IsString()) {
			return Json(ConnectDecimal(value.AsString(), IntParam(schema, "scale", 0)));
		}
		if (value.IsNumber()) {
			if (name == "org.apache.kafka.connect.data.Timestamp" || name == "io.debezium.time.Timestamp") {
				return Json(IsoTimestamp(value.AsInt() * 1000));
			}
			if (name == "io.debezium.time.MicroTimestamp") {
				return Json(IsoTimestamp(value.AsInt()));
			}
			if (name == "io.debezium.time.NanoTimestamp") {
				return Json(IsoTimestamp(value.AsInt() / 1000));
			}
			if (name == "org.apache.kafka.connect.data.Date" || name == "io.debezium.time.Date") {
				return Json(CivilDate(value.AsInt()));
			}
		}
	} catch (std::exception &) {
		return Json(); // a malformed value: NULL; the raw record keeps it
	}
	if (type == "array" && value.IsArray()) {
		auto items = schema.Get("items");
		if (!items) {
			return value;
		}
		Json out = Json::Array();
		for (auto &v : value.Items()) {
			out.Push(Convert(v, *items));
		}
		return out;
	}
	if (type == "struct" && value.IsObject()) {
		auto fields = schema.Get("fields");
		if (!fields || !fields->IsArray()) {
			return value;
		}
		Json out = Json::Object();
		for (auto &f : fields->Items()) {
			auto fname = f.Get("field");
			if (fname && fname->IsString()) {
				auto v = value.Get(fname->AsString());
				out.Set(fname->AsString(), v ? Convert(*v, f) : Json());
			}
		}
		return out;
	}
	return value;
}

const Json *Field(const Json &struct_schema, const std::string &name) {
	auto fields = struct_schema.Get("fields");
	if (!fields || !fields->IsArray()) {
		return nullptr;
	}
	for (auto &f : fields->Items()) {
		auto n = f.Get("field");
		if (n && n->IsString() && n->AsString() == name) {
			return &f;
		}
	}
	return nullptr;
}

Json OptionalCopy(const Json *v) {
	return v ? *v : Json();
}

Json Micros(const Json *payload_or_source, int64_t scale_to_micros, const char *key) {
	auto v = payload_or_source ? payload_or_source->Get(key) : nullptr;
	if (!v || !v->IsNumber()) {
		return Json();
	}
	return Json(IsoTimestamp(v->AsInt() * scale_to_micros));
}

Json Timestamp(const Json *object) {
	// The most precise of ts_us and ts_ms.
	if (object && object->Get("ts_us") && object->Get("ts_us")->IsNumber()) {
		return Micros(object, 1, "ts_us");
	}
	return Micros(object, 1000, "ts_ms");
}

} // namespace

std::string ConnectDecimal(const std::string &base64, int scale) {
	auto bytes = Base64Decode(base64);
	std::vector<uint8_t> mag(bytes.begin(), bytes.end());
	bool negative = !mag.empty() && (mag[0] & 0x80);
	if (negative) { // two's complement: invert and add one
		for (auto &b : mag) {
			b = static_cast<uint8_t>(~b);
		}
		for (auto it = mag.rbegin(); it != mag.rend(); ++it) {
			if (++*it != 0) {
				break;
			}
		}
	}
	// Repeated division by ten of the big-endian magnitude.
	std::string digits;
	bool nonzero = true;
	while (nonzero) {
		nonzero = false;
		uint32_t rem = 0;
		for (auto &b : mag) {
			uint32_t cur = (rem << 8) | b;
			b = static_cast<uint8_t>(cur / 10);
			rem = cur % 10;
			nonzero = nonzero || b != 0;
		}
		digits.push_back(static_cast<char>('0' + rem));
	}
	while (static_cast<int>(digits.size()) <= scale) {
		digits.push_back('0');
	}
	std::reverse(digits.begin(), digits.end());
	if (scale > 0) {
		digits.insert(digits.size() - static_cast<size_t>(scale), ".");
	}
	return (negative ? "-" : "") + digits;
}

std::optional<ChangeEvent> UnwrapChangeEvent(const std::string &value) {
	if (value.empty() || value[0] != '{') {
		return std::nullopt;
	}
	Json root;
	try {
		root = Json::Parse(value);
	} catch (std::exception &) {
		return std::nullopt;
	}
	auto schema = root.Get("schema");
	auto payload = root.Get("payload");
	if (!schema || !payload || !schema->IsObject() || !payload->IsObject()) {
		return std::nullopt;
	}
	auto op = payload->Get("op");
	if (!op || !op->IsString()) {
		return std::nullopt;
	}
	bool deleted = op->AsString() == "d";
	auto image_schema = Field(*schema, deleted ? "before" : "after");
	if (!image_schema) {
		image_schema = Field(*schema, deleted ? "after" : "before");
	}
	if (!image_schema || SchemaType(*image_schema) != "struct") {
		return std::nullopt;
	}
	auto image = payload->Get(deleted ? "before" : "after");
	if (!image || image->IsNull()) {
		image = payload->Get(deleted ? "after" : "before");
	}
	ChangeEvent event;
	Json row = Json::Object();
	for (auto &f : image_schema->Get("fields")->Items()) {
		auto fname = f.Get("field");
		if (!fname || !fname->IsString() || fname->AsString() == "cdc" || fname->AsString() == "warpstream") {
			continue; // the table's own columns win
		}
		event.columns.emplace_back(fname->AsString(), DuckType(f));
		auto v = image && image->IsObject() ? image->Get(fname->AsString()) : nullptr;
		row.Set(fname->AsString(), v ? Convert(*v, f) : Json());
	}
	auto source = payload->Get("source");
	Json cdc = Json::Object();
	cdc.Set("op", *op);
	cdc.Set("ts", Timestamp(payload));
	cdc.Set("source_ts", Timestamp(source));
	auto tx = source ? source->Get("txId") : nullptr;
	if (!tx && payload->Get("transaction") && payload->Get("transaction")->IsObject()) {
		tx = payload->Get("transaction")->Get("id");
	}
	cdc.Set("tx_id", OptionalCopy(tx));
	cdc.Set("sequence", OptionalCopy(source ? source->Get("sequence") : nullptr));
	auto snapshot = source ? source->Get("snapshot") : nullptr;
	// Debezium's own connectors write snapshot as "true"/"last"/"false"; Mendix's as a boolean.
	if (snapshot && snapshot->IsString()) {
		cdc.Set("snapshot", Json(snapshot->AsString() != "false"));
	} else {
		cdc.Set("snapshot", OptionalCopy(snapshot));
	}
	cdc.Set("connector", OptionalCopy(source ? source->Get("connector") : nullptr));
	cdc.Set("version", OptionalCopy(source ? source->Get("version") : nullptr));
	row.Set("cdc", std::move(cdc));
	event.row = row.Dump();
	return event;
}

} // namespace dgl
