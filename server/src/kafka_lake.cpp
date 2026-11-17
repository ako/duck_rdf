#include "kafka_lake.hpp"

#include "kafka_cdc.hpp"

#include "log.hpp"
#include "setup.hpp"
#include "sparql.hpp"

#include "duckdb/main/extension_helper.hpp"

#include <yaml-cpp/yaml.h>

#include <chrono>
#include <filesystem>
#include <set>
#include <stdexcept>

namespace dgl {

namespace fs = std::filesystem;

namespace {

const char *NAMESPACE = "tableflow";
// Tableflow's warpstream struct, plus the record's headers (text values; a value that is not UTF-8
// is NULL). Tables written before headers were kept have no headers field and get none.
const char *WARPSTREAM_TYPE = "STRUCT(\"partition\" INTEGER, \"offset\" BIGINT, \"key\" BLOB, \"value\" BLOB, "
                              "\"timestamp\" TIMESTAMP, headers MAP(VARCHAR, VARCHAR))";

// A Tableflow type name (field_type_remapping) as a DuckDB type.
std::string TableflowType(const std::string &type) {
	static const std::map<std::string, std::string> types {{"boolean", "BOOLEAN"},
	                                                       {"int", "INTEGER"},
	                                                       {"long", "BIGINT"},
	                                                       {"float", "FLOAT"},
	                                                       {"double", "DOUBLE"},
	                                                       {"date", "DATE"},
	                                                       {"time", "TIME"},
	                                                       {"timestamp", "TIMESTAMP"},
	                                                       {"timestamptz", "TIMESTAMPTZ"},
	                                                       {"string", "VARCHAR"},
	                                                       {"uuid", "UUID"},
	                                                       {"binary", "BLOB"},
	                                                       {"fixed", "BLOB"},
	                                                       {"decimal", "DECIMAL(38, 9)"}};
	auto it = types.find(type);
	if (it == types.end()) {
		throw std::runtime_error("unknown Tableflow type: " + type);
	}
	return it->second;
}

struct Converted {
	std::string type;      // DuckDB type
	std::string structure; // json_transform structure (JSON text)
};

// A JSON Schema node as a DuckDB type and json_transform structure.
Converted FromJsonSchema(const Json &node, const std::string &path, const std::map<std::string, std::string> &remap) {
	auto remapped = remap.find(path);
	if (remapped != remap.end()) {
		auto type = TableflowType(remapped->second);
		std::string structure;
		JsonQuote(type, structure);
		return {type, structure};
	}
	std::string type;
	if (auto t = node.Get("type")) {
		if (t->IsString()) {
			type = t->AsString();
		} else if (t->IsArray()) {
			for (auto &item : t->Items()) {
				if (item.IsString() && item.AsString() != "null") {
					type = item.AsString();
				}
			}
		}
	} else if (node.Get("properties")) {
		type = "object";
	}
	auto scalar = [](const std::string &duck) {
		std::string structure;
		JsonQuote(duck, structure);
		return Converted {duck, structure};
	};
	if (type == "object") {
		auto props = node.Get("properties");
		if ((!props || props->Size() == 0) && node.Get("additionalProperties") &&
		    node.Get("additionalProperties")->IsObject()) {
			auto value = FromJsonSchema(*node.Get("additionalProperties"), path + ".value", remap);
			return scalar("MAP(VARCHAR, " + value.type + ")");
		}
		if (!props || props->Size() == 0) {
			return scalar("JSON");
		}
		std::string duck = "STRUCT(", structure = "{";
		bool first = true;
		for (auto &p : props->Members()) {
			auto child = FromJsonSchema(p.second, path.empty() ? p.first : path + "." + p.first, remap);
			if (!first) {
				duck += ", ";
				structure += ",";
			}
			first = false;
			duck += SqlIdent(p.first) + " " + child.type;
			JsonQuote(p.first, structure);
			structure += ":" + child.structure;
		}
		return {duck + ")", structure + "}"};
	}
	if (type == "array") {
		auto items = node.Get("items");
		auto element = items && items->IsObject() ? FromJsonSchema(*items, path + ".element", remap) : scalar("JSON");
		return {element.type + "[]", "[" + element.structure + "]"};
	}
	if (type == "integer") {
		return scalar("BIGINT");
	}
	if (type == "number") {
		return scalar("DOUBLE");
	}
	if (type == "boolean") {
		return scalar("BOOLEAN");
	}
	if (type == "string") {
		auto format = node.Get("format") && node.Get("format")->IsString() ? node.Get("format")->AsString() : "";
		if (format == "date") {
			return scalar("DATE");
		}
		if (format == "date-time") {
			return scalar("TIMESTAMPTZ");
		}
		if (format == "time") {
			return scalar("TIME");
		}
		if (format == "uuid") {
			return scalar("UUID");
		}
		return scalar("VARCHAR");
	}
	return scalar("JSON"); // no type: keep the JSON text
}

std::string YamlString(const YAML::Node &node, const std::string &key, const std::string &fallback = "") {
	return node[key] && node[key].IsScalar() ? node[key].as<std::string>() : fallback;
}

} // namespace

TableflowConfig LoadTableflowConfig(const std::string &path) {
	YAML::Node root;
	try {
		root = YAML::LoadFile(path);
	} catch (std::exception &ex) {
		throw std::runtime_error(path + ": " + ex.what());
	}
	TableflowConfig config;
	auto tables = root["tables"];
	if (!tables || !tables.IsSequence()) {
		throw std::runtime_error(path + ": no tables list");
	}
	std::set<std::string> noted;
	for (auto table : tables) {
		TableflowTable t;
		t.cluster = YamlString(table, "source_cluster_name", "local");
		t.topic = YamlString(table, "source_topic");
		if (t.topic.empty()) {
			throw std::runtime_error(path + ": a table without source_topic");
		}
		t.name = YamlString(table, "name", t.cluster + "__" + t.topic);
		auto format = YamlString(table, "source_format", "json");
		if (format != "json") {
			throw std::runtime_error(path + ": table " + t.name + ": only source_format json is supported locally");
		}
		auto mode = YamlString(table, "schema_mode", "inline");
		if (mode != "inline") {
			throw std::runtime_error(path + ": table " + t.name + ": only schema_mode inline is supported locally");
		}
		t.skip_raw_values = table["skip_raw_record_values"] && table["skip_raw_record_values"].as<bool>();
		std::map<std::string, std::string> remap;
		if (auto remaps = table["field_type_remapping"]) {
			for (auto r : remaps) {
				remap[YamlString(r, "path")] = YamlString(r, "type");
			}
		}
		auto schema_text = YamlString(table, "input_schema");
		if (!schema_text.empty()) {
			auto schema = Json::Parse(schema_text);
			auto converted = FromJsonSchema(schema, "", remap);
			if (auto props = schema.Get("properties")) {
				for (auto &p : props->Members()) {
					t.columns.emplace_back(p.first, FromJsonSchema(p.second, p.first, remap).type);
				}
				t.json_structure = converted.structure;
			}
		}
		for (auto &key : {"schema", "transforms", "partitioning_scheme", "custom_partitioning", "sorting",
		                  "retention_ttl", "dlq_mode", "compression"}) {
			if (table[key] && !noted.count(key)) {
				noted.insert(key);
				config.notes.push_back(std::string(key) + " is not applied locally");
			}
		}
		config.tables.push_back(t);
	}
	return config;
}

// ---------------------------------------------------------------------------

KafkaLake::KafkaLake(KafkaLakeOptions options, IcebergCatalog &catalog, TableCallback on_table)
    : options_(std::move(options)), catalog_(catalog), on_table_(std::move(on_table)) {
}

KafkaLake::~KafkaLake() {
	Stop();
}

bool KafkaLake::ValidTopicName(const std::string &name) {
	if (name.empty() || name.size() > 249 || name == "." || name == "..") {
		return false;
	}
	for (char c : name) {
		if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '_' || c == '-')) {
			return false;
		}
	}
	return true;
}

const TableflowTable *KafkaLake::Configured(const std::string &topic) const {
	for (auto &t : options_.tableflow.tables) {
		if (t.topic == topic) {
			return &t;
		}
	}
	return nullptr;
}

std::string KafkaLake::TableFor(const std::string &topic) const {
	auto t = Configured(topic);
	return t ? t->name : options_.cluster + "__" + topic;
}

void KafkaLake::Start() {
	// Topics: those configured, and, if this folder was produced to before, those created then.
	for (auto &t : options_.tableflow.tables) {
		topics_.emplace(t.topic, options_.default_partitions);
	}
	if (fs::exists(fs::path(options_.data_dir) / ".duckgraphlake" / "kafka.duckdb")) {
		std::lock_guard<std::mutex> lock(mutex_);
		EnsureOpen(); // also commits what an earlier run left in the buffer
	}
	stopping_ = false;
	flusher_ = std::thread([this]() { FlushLoop(); });
}

void KafkaLake::EnsureOpen() {
	if (open_) {
		return;
	}
	auto state = fs::path(options_.data_dir) / ".duckgraphlake";
	fs::create_directories(state);
	catalog_.EnsureNamespace(NAMESPACE);
	db_ = std::make_unique<duckdb::DuckDB>((state / "kafka.duckdb").string());
	duckdb::ExtensionHelper::LoadAllExtensions(*db_);
	append_con_ = std::make_unique<duckdb::Connection>(*db_);
	Exec(*append_con_, "INSTALL iceberg");
	Exec(*append_con_, "LOAD iceberg");
	Exec(*append_con_, "CREATE TABLE IF NOT EXISTS kafka_topics (name VARCHAR PRIMARY KEY, partitions INTEGER)");
	Exec(*append_con_, "CREATE TABLE IF NOT EXISTS kafka_buffer (seq BIGINT, topic VARCHAR, \"partition\" INTEGER, "
	                   "\"offset\" BIGINT, \"timestamp\" TIMESTAMP, \"key\" BLOB, \"value\" BLOB)");
	// Added later: a buffer file from before them gets the columns, NULL for what it holds.
	Exec(*append_con_, "ALTER TABLE kafka_buffer ADD COLUMN IF NOT EXISTS headers MAP(VARCHAR, BLOB)");
	Exec(*append_con_, "ALTER TABLE kafka_buffer ADD COLUMN IF NOT EXISTS cdc_row VARCHAR");
	Exec(*append_con_, "ALTER TABLE kafka_buffer ADD COLUMN IF NOT EXISTS cdc_columns VARCHAR");
	for (auto &row : Column(*append_con_, "SELECT name || '\x1f' || partitions FROM kafka_topics")) {
		auto sep = row.find('\x1f');
		topics_[row.substr(0, sep)] = std::stoi(row.substr(sep + 1));
	}
	auto seq = Column(*append_con_, "SELECT coalesce(max(seq), 0) FROM kafka_buffer");
	next_seq_ = std::stoll(seq[0]) + 1;
	open_ = true;
}

void KafkaLake::Stop() {
	{
		std::lock_guard<std::mutex> lock(flush_mutex_);
		if (stopping_) {
			return;
		}
		stopping_ = true;
	}
	flush_cv_.notify_all();
	if (flusher_.joinable()) {
		flusher_.join();
	}
	// What is still buffered stays in the buffer file and is committed after the next start.
}

int KafkaLake::Partitions(const std::string &topic) {
	std::lock_guard<std::mutex> lock(mutex_);
	auto it = topics_.find(topic);
	return it == topics_.end() ? 0 : it->second;
}

void KafkaLake::CreateTopic(const std::string &topic, int partitions) {
	std::lock_guard<std::mutex> lock(mutex_);
	if (topics_.count(topic)) {
		return;
	}
	partitions = partitions > 0 ? partitions : options_.default_partitions;
	EnsureOpen();
	topics_[topic] = partitions;
	Exec(*append_con_,
	     "INSERT OR REPLACE INTO kafka_topics VALUES (" + SqlLiteral(topic) + ", " + std::to_string(partitions) + ")");
	Log("kafka: topic " + topic + " (" + std::to_string(partitions) + " partition(s)) -> table " + TableFor(topic));
}

std::vector<std::pair<std::string, int>> KafkaLake::Topics() {
	std::lock_guard<std::mutex> lock(mutex_);
	return {topics_.begin(), topics_.end()};
}

int64_t KafkaLake::NextOffset(const std::string &topic, int32_t partition) {
	auto key = std::make_pair(topic, partition);
	auto it = next_offset_.find(key);
	if (it != next_offset_.end()) {
		return it->second;
	}
	// First use since start-up: after the buffer's and the table's highest offsets. Records the
	// table already holds (committed just before a crash) leave the buffer.
	int64_t committed = -1;
	auto folder = catalog_.TableFolder(NAMESPACE, TableFor(topic));
	if (fs::exists(fs::path(folder) / "metadata" / "version-hint.text")) {
		auto rows = Column(*append_con_, "SELECT coalesce(max(warpstream.\"offset\"), -1) FROM iceberg_scan(" +
		                                     SqlLiteral(folder) +
		                                     ") WHERE warpstream.\"partition\" = " + std::to_string(partition));
		committed = rows.empty() || rows[0].empty() ? -1 : std::stoll(rows[0]);
		Exec(*append_con_, "DELETE FROM kafka_buffer WHERE topic = " + SqlLiteral(topic) + " AND \"partition\" = " +
		                       std::to_string(partition) + " AND \"offset\" <= " + std::to_string(committed));
	}
	auto buffered = Column(*append_con_, "SELECT coalesce(max(\"offset\"), -1) FROM kafka_buffer WHERE topic = " +
	                                         SqlLiteral(topic) + " AND \"partition\" = " + std::to_string(partition));
	auto next = std::max<int64_t>(committed, std::stoll(buffered[0])) + 1;
	next_offset_[key] = next;
	return next;
}

int64_t KafkaLake::Append(const std::string &topic, int32_t partition, const std::vector<KafkaRecord> &records) {
	std::lock_guard<std::mutex> lock(mutex_);
	EnsureOpen();
	auto base = NextOffset(topic, partition);
	duckdb::Appender appender(*append_con_, "kafka_buffer");
	auto offset = base;
	bool unwrap = options_.unwrap_cdc && !Configured(topic);
	for (auto &r : records) {
		duckdb::Value headers(duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB));
		if (!r.headers.empty()) {
			// A map: a key repeated keeps its last value.
			std::map<std::string, std::optional<std::string>> last;
			std::vector<std::string> order;
			for (auto &h : r.headers) {
				if (!last.count(h.first)) {
					order.push_back(h.first);
				}
				last[h.first] = h.second;
			}
			duckdb::vector<duckdb::Value> keys, values;
			for (auto &k : order) {
				keys.push_back(duckdb::Value(k));
				values.push_back(last[k] ? duckdb::Value::BLOB_RAW(*last[k])
				                         : duckdb::Value(duckdb::LogicalType::BLOB));
			}
			headers = duckdb::Value::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BLOB, std::move(keys),
			                             std::move(values));
		}
		std::optional<ChangeEvent> change;
		if (unwrap && r.value) {
			change = UnwrapChangeEvent(*r.value);
		}
		appender.BeginRow();
		appender.Append<int64_t>(next_seq_++);
		appender.Append(duckdb::Value(topic));
		appender.Append<int32_t>(partition);
		appender.Append<int64_t>(offset++);
		appender.Append(duckdb::Value::TIMESTAMP(duckdb::Timestamp::FromEpochMs(r.timestamp_ms)));
		appender.Append(r.key ? duckdb::Value::BLOB_RAW(*r.key) : duckdb::Value(duckdb::LogicalType::BLOB));
		appender.Append(r.value ? duckdb::Value::BLOB_RAW(*r.value) : duckdb::Value(duckdb::LogicalType::BLOB));
		appender.Append(headers);
		if (change) {
			Json columns = Json::Array();
			for (auto &c : change->columns) {
				Json pair = Json::Array();
				pair.Push(Json(c.first));
				pair.Push(Json(c.second));
				columns.Push(std::move(pair));
			}
			appender.Append(duckdb::Value(change->row));
			appender.Append(duckdb::Value(columns.Dump()));
		} else {
			appender.Append(duckdb::Value(duckdb::LogicalType::VARCHAR));
			appender.Append(duckdb::Value(duckdb::LogicalType::VARCHAR));
		}
		appender.EndRow();
	}
	appender.Close(); // committed to the buffer file: safe to acknowledge
	next_offset_[std::make_pair(topic, partition)] = offset;
	flush_cv_.notify_all();
	return base;
}

void KafkaLake::FlushLoop() {
	auto last = std::chrono::steady_clock::now();
	for (;;) {
		{
			std::unique_lock<std::mutex> lock(flush_mutex_);
			flush_cv_.wait_until(lock, last + std::chrono::milliseconds(options_.flush_ms),
			                     [this]() { return stopping_; });
			if (stopping_) {
				return;
			}
		}
		last = std::chrono::steady_clock::now();
		Flush();
	}
}

void KafkaLake::Flush() {
	if (!open_) {
		return; // nothing produced yet
	}
	duckdb::Connection con(*db_);
	try {
		if (!attached_) {
			Exec(con, "ATTACH 'duckgraphlake' AS lake (TYPE iceberg, ENDPOINT " + SqlLiteral(options_.catalog_url) +
			              ", TOKEN " + SqlLiteral(options_.token) + ")");
			attached_ = true;
		}
	} catch (std::exception &ex) {
		return; // the HTTP server is not up yet: next round
	}
	std::vector<std::pair<std::string, int64_t>> pending;
	for (auto &row : Column(con, "SELECT topic || '\x1f' || max(seq) FROM kafka_buffer GROUP BY topic ORDER BY 1")) {
		auto sep = row.find('\x1f');
		pending.emplace_back(row.substr(0, sep), std::stoll(row.substr(sep + 1)));
	}
	for (auto &p : pending) {
		FlushTopic(con, p.first, p.second);
	}
}

KafkaLake::Columns KafkaLake::TableColumns(duckdb::Connection &con, const std::string &qualified) {
	Columns columns;
	for (auto &row : Column(con, "SELECT column_name || '\x1f' || column_type FROM (DESCRIBE " + qualified + ")")) {
		auto sep = row.find('\x1f');
		columns.emplace_back(row.substr(0, sep), row.substr(sep + 1));
	}
	return columns;
}

namespace {

// The fields of the change events buffered for a topic, in the order they first appear.
KafkaLake::Columns BufferedChangeColumns(duckdb::Connection &con, const std::string &topic, int64_t up_to_seq) {
	KafkaLake::Columns columns;
	std::set<std::string> seen;
	for (auto &text : Column(con, "SELECT cdc_columns FROM kafka_buffer WHERE topic = " + SqlLiteral(topic) +
	                                  " AND seq <= " + std::to_string(up_to_seq) +
	                                  " AND cdc_columns IS NOT NULL GROUP BY cdc_columns ORDER BY min(seq)")) {
		auto parsed = Json::Parse(text); // not in the for: a temporary's member would dangle
		for (auto &pair : parsed.Items()) {
			auto name = pair.Items()[0].AsString();
			if (seen.insert(name).second) {
				columns.emplace_back(name, pair.Items()[1].AsString());
			}
		}
	}
	return columns;
}

const std::string *ColumnType(const KafkaLake::Columns &columns, const std::string &name) {
	for (auto &c : columns) {
		if (c.first == name) {
			return &c.second;
		}
	}
	return nullptr;
}

} // namespace

bool KafkaLake::FlushTopic(duckdb::Connection &con, const std::string &topic, int64_t up_to_seq) {
	static std::mutex logged_mutex;
	static std::set<std::string> logged;
	auto log_once = [&](const std::string &message) {
		std::lock_guard<std::mutex> lock(logged_mutex);
		if (logged.insert(message).second) {
			Log(message);
		}
	};
	auto table = TableFor(topic);
	auto configured = Configured(topic);
	auto qualified = std::string("lake.") + NAMESPACE + "." + SqlIdent(table);
	try {
		if (!table_ready_[table]) {
			auto existing = catalog_.Tables(NAMESPACE);
			if (std::find(existing.begin(), existing.end(), table) == existing.end()) {
				Columns columns;
				if (configured) {
					columns = configured->columns;
				} else if (options_.unwrap_cdc) {
					// A topic of change events: a column per field of the entity, and a cdc struct.
					columns = BufferedChangeColumns(con, topic, up_to_seq);
					if (!columns.empty()) {
						columns.emplace_back("cdc", CDC_TYPE);
					}
				}
				std::string list;
				for (auto &c : columns) {
					list += SqlIdent(c.first) + " " + c.second + ", ";
				}
				Exec(con, "CREATE TABLE " + qualified + " (" + list + "warpstream " + WARPSTREAM_TYPE + ")");
			}
			table_columns_[table] = TableColumns(con, qualified);
			table_ready_[table] = true;
		}
		auto columns = table_columns_[table];
		auto warpstream_type = ColumnType(columns, "warpstream");
		bool has_headers = warpstream_type && warpstream_type->find("headers") != std::string::npos;
		bool cdc = !configured && options_.unwrap_cdc && ColumnType(columns, "cdc") != nullptr;
		std::string fields;
		if (cdc) {
			// Fields the entity gained since the table was made (a minor revision of a CDC service
			// adds attributes) become new columns.
			bool altered = false;
			for (auto &c : BufferedChangeColumns(con, topic, up_to_seq)) {
				if (ColumnType(columns, c.first)) {
					continue;
				}
				try {
					Exec(con, "ALTER TABLE " + qualified + " ADD COLUMN " + SqlIdent(c.first) + " " + c.second);
					altered = true;
				} catch (std::exception &ex) {
					log_once("kafka: " + table + ": no column for the new field " + c.first +
					         " (kept in the raw "
					         "value): " +
					         duckdb::ErrorData(ex).RawMessage());
				}
			}
			if (altered) {
				columns = table_columns_[table] = TableColumns(con, qualified);
			}
			std::string structure = "{";
			for (auto &c : columns) {
				if (c.first == "warpstream") {
					continue;
				}
				if (structure.size() > 1) {
					structure += ",";
				}
				JsonQuote(c.first, structure);
				structure += ":";
				JsonQuote(c.second, structure);
			}
			structure += "}";
			fields = "unnest(json_transform(cdc_row, " + SqlLiteral(structure) + ")), ";
		} else if (configured && !configured->columns.empty()) {
			// The record's JSON value, parsed by the table's schema. A value that is not JSON, or not of
			// the schema's shape, leaves the fields NULL; the raw value stays in warpstream.value.
			fields = "unnest(CASE WHEN json_valid(TRY(decode(\"value\"))) THEN json_transform(decode(\"value\"), " +
			         SqlLiteral(configured->json_structure) + ") END), ";
		}
		auto raw_value = configured && configured->skip_raw_values ? "NULL::BLOB" : "\"value\"";
		std::string headers = has_headers ? ", 'headers': map_from_entries(list_transform(map_entries(headers), "
		                                    "lambda e: {'key': e.key, 'value': TRY(decode(e.value))}))"
		                                  : "";
		Exec(con, "INSERT INTO " + qualified + " BY NAME SELECT " + fields +
		              "{'partition': \"partition\", 'offset': \"offset\", 'key': \"key\", 'value': " + raw_value +
		              ", 'timestamp': \"timestamp\"" + headers +
		              "} AS warpstream FROM kafka_buffer WHERE topic = " + SqlLiteral(topic) +
		              " AND seq <= " + std::to_string(up_to_seq) + " ORDER BY \"partition\", \"offset\"");
		Exec(con, "DELETE FROM kafka_buffer WHERE topic = " + SqlLiteral(topic) +
		              " AND seq <= " + std::to_string(up_to_seq));
		if (!table_announced_[table]) {
			table_announced_[table] = true;
			on_table_(table, catalog_.TableFolder(NAMESPACE, table));
		}
		return true;
	} catch (std::exception &ex) {
		duckdb::ErrorData error(ex);
		log_once("kafka: writing " + table + " failed (kept in the buffer, retried): " + error.RawMessage());
		table_ready_[table] = false;
		return false;
	}
}

} // namespace dgl
